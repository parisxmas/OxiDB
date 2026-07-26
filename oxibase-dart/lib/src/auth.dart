import 'dart:async';

import 'transport.dart';
import 'types.dart';

/// End-user sign-in for one project.
///
/// A project's users are the *app's* users, not the developer's. Tokens are
/// ES256, signed with the project's own key, and short-lived: the client renews
/// them, which **rotates the refresh token**.
///
/// That rotation is the thing to get right in an app. Store the session on every
/// [onAuthStateChange] event, not only after sign-in — the token you were handed
/// at sign-in is revoked the moment the first renewal happens, so a stored copy
/// goes stale and the next launch cannot resume with it.
class OxibaseAuth {
  OxibaseAuth(this._transport);

  final Transport _transport;

  String get _base {
    final base = _transport.authBaseUrl;
    if (base == null) {
      throw const OxibaseException('auth needs the authUrl option (the control-plane base)');
    }
    return base;
  }

  String get _ref {
    final ref = _transport.ref;
    if (ref == null) throw const OxibaseException('auth needs a project ref');
    return ref;
  }

  Uri _endpoint(String action) =>
      Uri.parse('$_base/platform/v1/projects/${Uri.encodeComponent(_ref)}/auth/$action');

  /// The session in force, or null when running as the project key.
  Session? get currentSession => _transport.session;

  /// Session changes: sign-in, **every token rotation**, and sign-out.
  ///
  /// Persist on each one:
  /// ```dart
  /// client.auth.onAuthStateChange.listen((change) async {
  ///   final (event, session) = change;
  ///   if (session == null) await store.clear();
  ///   else await store.save(jsonEncode(session.toJson()));
  /// });
  /// ```
  Stream<(AuthChangeEvent, Session?)> get onAuthStateChange => _transport.authChanges;

  Future<Session> signUp({required String email, required String password}) =>
      _authCall('signup', {'email': email, 'password': password});

  Future<Session> signInWithPassword({required String email, required String password}) =>
      _authCall('login', {'email': email, 'password': password});

  Future<Session> _authCall(String action, Map<String, dynamic> body) async {
    final res = await _transport.send('POST', _endpoint(action), body: body, authed: false);
    final decoded = _transport.decodeOrThrow(res);
    if (decoded['verification_required'] == true) {
      throw const OxibaseException(
        'check your inbox: this project requires the emailed link before sign-in',
        statusCode: 403,
        verificationRequired: true,
      );
    }
    final session =
        Session.fromJson({...decoded, 'email': decoded['user']?['email'] ?? body['email']});
    if (session == null) throw const OxibaseException('no session in the response');
    _transport.adopt(session, emit: AuthChangeEvent.signedIn);
    return session;
  }

  /// Resume a stored session. Deliberately does **not** emit a change: the caller
  /// supplied it, so echoing it back would loop a listener that persists on every
  /// event straight into another save.
  void setSession(Session session) => _transport.adopt(session);

  /// Renew now. Rarely needed — a 401 renews automatically — but useful on
  /// resume from background, where the token is likely stale.
  Future<bool> refreshSession() => _transport.refresh();

  void signOut() => _transport.clearSession();

  /// Email a password-reset link. Always succeeds, known address or not, so the
  /// call cannot be used to discover who has an account.
  Future<void> resetPasswordForEmail(String email) => _post('recover', {'email': email});

  Future<void> resendVerification(String email) => _post('resend', {'email': email});

  /// Complete a reset with the token from the email.
  Future<void> resetPassword({required String token, required String password}) =>
      _post('reset', {'token': token, 'password': password});

  /// Email a one-time sign-in link. The link lands on [redirectTo] with the
  /// session in the URL fragment; pass it to [sessionFromRedirect].
  Future<void> signInWithMagicLink({required String email, required String redirectTo}) =>
      _post('magiclink', {'email': email, 'redirect_to': redirectTo});

  /// The provider consent URL to open in a browser or webview.
  ///
  /// [redirectTo] must be listed in the project's allowed redirect URLs — on
  /// mobile that is typically your app's deep link, e.g. `myapp://auth`.
  Uri oauthUrl({required String provider, required String redirectTo}) => Uri.parse(
        '${_endpoint('authorize/${Uri.encodeComponent(provider)}')}'
        '?redirect_to=${Uri.encodeQueryComponent(redirectTo)}',
      );

  /// Sign in with a Google ID token the app already holds (google_sign_in).
  Future<Session> signInWithIdToken({required String provider, required String token}) async {
    final res = await _transport.send(
      'POST',
      _endpoint('oauth/${Uri.encodeComponent(provider)}'),
      body: {'credential': token},
      authed: false,
    );
    final decoded = _transport.decodeOrThrow(res);
    final session = Session.fromJson({...decoded, 'email': decoded['user']?['email']});
    if (session == null) throw const OxibaseException('no session in the response');
    _transport.adopt(session, emit: AuthChangeEvent.signedIn);
    return session;
  }

  /// Pick the session out of the URL a redirect or deep link arrived on, and
  /// adopt it. Returns null when the URL carries no session.
  ///
  /// The tokens are in the **fragment** rather than the query, so they never
  /// reach a server log.
  Session? sessionFromRedirect(Uri url) {
    final fragment = url.fragment;
    if (fragment.isEmpty) return null;
    final params = Uri.splitQueryString(fragment);
    final error = params['error'];
    if (error != null) throw OxibaseException(error, statusCode: 401);
    final access = params['access_token'];
    final refresh = params['refresh_token'];
    if (access == null || refresh == null) return null;
    final session = Session(accessToken: access, refreshToken: refresh);
    _transport.adopt(session, emit: AuthChangeEvent.signedIn);
    return session;
  }

  /// Which sign-in methods this project offers. Public; no key needed.
  Future<AuthSettings> getSettings() async {
    final res = await _transport.send('GET', _endpoint('settings'), authed: false);
    final j = _transport.decodeOrThrow(res);
    return AuthSettings(
      password: j['password'] as bool? ?? true,
      magicLink: j['magic_link'] as bool? ?? false,
      providers: (j['providers'] as List?)?.map((e) => e.toString()).toList() ?? const [],
      googleClientId: j['google_client_id'] as String?,
    );
  }

  Future<void> _post(String action, Map<String, dynamic> body) async {
    final res = await _transport.send('POST', _endpoint(action), body: body, authed: false);
    if (res.statusCode >= 300) throw _transport.exceptionFor(res, null);
  }
}

/// What a project's sign-in screen should offer.
class AuthSettings {
  const AuthSettings({
    required this.password,
    required this.magicLink,
    required this.providers,
    this.googleClientId,
  });

  final bool password;
  final bool magicLink;
  final List<String> providers;
  final String? googleClientId;
}
