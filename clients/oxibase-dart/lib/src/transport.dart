import 'dart:async';
import 'dart:convert';

import 'package:http/http.dart' as http;

import 'types.dart';

/// Sends every request, and owns the one piece of state that is easy to get
/// wrong: which token is current.
///
/// Two behaviours here exist because of bugs found in the JavaScript client, and
/// they are the reason this is a shared object rather than a function:
///
///  * **Refreshes are single-flight.** The refresh token is single-use, so if ten
///    requests each notice a 401 and each POST the same refresh token, one
///    rotates it and the other nine are told "invalid" — with a perfectly good
///    session. They wait on one refresh instead.
///  * **A request whose token was rotated while it was in flight retries rather
///    than refreshing again.** Without the generation counter the second caller
///    starts a pointless second rotation.
class Transport {
  Transport({
    required this.baseUrl,
    required this.key,
    required this.tenantPath,
    required this.dbParam,
    required this.extraHeaders,
    http.Client? httpClient,
  })  : _http = httpClient ?? http.Client(),
        _ownsClient = httpClient == null;

  final String baseUrl;

  /// The project's anon or service_role key: what requests carry until a user
  /// signs in.
  final String key;

  /// `/<ref>` when addressing the project by path, else empty.
  final String tenantPath;

  /// The project ref to send as `?db=`, when not addressing by path.
  final String? dbParam;

  final Map<String, String> extraHeaders;

  final http.Client _http;
  final bool _ownsClient;

  String? _accessToken;
  String? _refreshToken;
  String? _email;

  /// Bumped whenever the access token changes.
  int _tokenGeneration = 0;

  Future<bool>? _refreshInFlight;

  final _authChanges = StreamController<(AuthChangeEvent, Session?)>.broadcast();

  Stream<(AuthChangeEvent, Session?)> get authChanges => _authChanges.stream;

  /// The control-plane base, needed only by auth. Set by the client.
  String? authBaseUrl;

  /// The project ref; auth endpoints are per project.
  String? ref;

  Session? get session => (_accessToken != null && _refreshToken != null)
      ? Session(accessToken: _accessToken!, refreshToken: _refreshToken!, email: _email)
      : null;

  String get currentToken => _accessToken ?? key;

  void adopt(Session s, {AuthChangeEvent? emit}) {
    _accessToken = s.accessToken;
    _refreshToken = s.refreshToken;
    _email = s.email ?? _email;
    _tokenGeneration++;
    if (emit != null) _authChanges.add((emit, session));
  }

  void clearSession() {
    _accessToken = null;
    _refreshToken = null;
    _email = null;
    _tokenGeneration++;
    _authChanges.add((AuthChangeEvent.signedOut, null));
  }

  Uri url(String path, [Map<String, String>? query]) {
    final u = Uri.parse('$baseUrl$tenantPath$path');
    final params = <String, String>{...u.queryParameters, ...?query};
    if (dbParam != null) params['db'] = dbParam!;
    return u.replace(queryParameters: params.isEmpty ? null : params);
  }

  /// Send, and on a 401 renew the session once and send again.
  Future<http.Response> send(
    String method,
    Uri uri, {
    Object? body,
    Map<String, String>? headers,
    bool authed = true,
  }) async {
    Future<http.Response> attempt() {
      final h = <String, String>{...extraHeaders, ...?headers};
      if (authed) h['Authorization'] = 'Bearer $currentToken';
      final request = http.Request(method, uri)..headers.addAll(h);
      if (body is List<int>) {
        request.bodyBytes = body;
      } else if (body is String) {
        request.body = body;
      } else if (body != null) {
        request.headers.putIfAbsent('Content-Type', () => 'application/json');
        request.body = jsonEncode(body);
      }
      return _http.send(request).then(http.Response.fromStream);
    }

    final sentGeneration = _tokenGeneration;
    final res = await attempt();
    if (res.statusCode != 401 || !authed || _refreshToken == null) return res;

    // Someone else rotated while this was in flight: the 401 is about the old
    // token, so retry rather than start another rotation.
    if (_tokenGeneration != sentGeneration) return attempt();
    final renewed = await refresh();
    return renewed ? attempt() : res;
  }

  /// Renew the access token. Concurrent callers share one attempt.
  Future<bool> refresh() {
    final inFlight = _refreshInFlight;
    if (inFlight != null) return inFlight;
    final started = _doRefresh().whenComplete(() => _refreshInFlight = null);
    _refreshInFlight = started;
    return started;
  }

  Future<bool> _doRefresh() async {
    final base = authBaseUrl;
    final projectRef = ref;
    final token = _refreshToken;
    if (base == null || projectRef == null || token == null) return false;

    final uri =
        Uri.parse('$base/platform/v1/projects/${Uri.encodeComponent(projectRef)}/auth/refresh');
    late http.Response res;
    try {
      res = await _http.post(
        uri,
        headers: const {'Content-Type': 'application/json'},
        body: jsonEncode({'refresh_token': token}),
      );
    } catch (_) {
      return false;
    }
    if (res.statusCode >= 300) return false;
    final decoded = _tryDecode(res.body);
    final access = decoded?['token'];
    if (access is! String) return false;
    _accessToken = access;
    final rotated = decoded?['refresh_token'];
    if (rotated is String) _refreshToken = rotated;
    _tokenGeneration++;
    _authChanges.add((AuthChangeEvent.tokenRefreshed, session));
    return true;
  }

  /// Decode a JSON body, or throw [OxibaseException] describing the refusal.
  Map<String, dynamic> decodeOrThrow(http.Response res) {
    final decoded = _tryDecode(res.body);
    if (res.statusCode >= 300) throw exceptionFor(res, decoded);
    return decoded ?? const {};
  }

  List<Map<String, dynamic>> decodeListOrThrow(http.Response res) {
    if (res.statusCode >= 300) throw exceptionFor(res, _tryDecode(res.body));
    if (res.body.trim().isEmpty) return const [];
    final parsed = jsonDecode(res.body);
    if (parsed is List) {
      return parsed
          .whereType<Map<dynamic, dynamic>>()
          .map((e) => e.cast<String, dynamic>())
          .toList();
    }
    if (parsed is Map) return [parsed.cast<String, dynamic>()];
    return const [];
  }

  OxibaseException exceptionFor(http.Response res, Map<String, dynamic>? decoded) {
    final retry = int.tryParse(res.headers['retry-after'] ?? '');
    final message = (decoded?['error'] ?? decoded?['message'] ?? decoded?['hint'])?.toString();
    return OxibaseException(
      message ?? 'HTTP ${res.statusCode}',
      statusCode: res.statusCode,
      retryAfter: retry,
    );
  }

  Map<String, dynamic>? _tryDecode(String body) {
    if (body.trim().isEmpty) return null;
    try {
      final parsed = jsonDecode(body);
      return parsed is Map ? parsed.cast<String, dynamic>() : null;
    } catch (_) {
      return null;
    }
  }

  void dispose() {
    if (_ownsClient) _http.close();
    _authChanges.close();
  }
}
