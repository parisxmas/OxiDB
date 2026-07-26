import 'dart:async';
import 'dart:convert';

import 'package:http/http.dart' as http;

import 'auth.dart';
import 'query.dart';
import 'realtime.dart';
import 'storage.dart';
import 'transport.dart';
import 'types.dart';

/// A client for one OxiBase project.
///
/// ```dart
/// final client = OxibaseClient(
///   url: 'https://oxibase.example.com',
///   key: anonKey,              // public by design; rules decide what it may do
///   ref: 'my-project',
///   authUrl: 'https://oxibase.example.com',
/// );
///
/// final posts = await client.from('posts').select().order('ts', ascending: false).limit(20).get();
/// ```
///
/// One client per app: it owns the HTTP connection pool, the session, and the
/// single realtime socket.
class OxibaseClient {
  OxibaseClient({
    required String url,
    required String key,
    String? ref,
    String? authUrl,
    bool tenantInPath = false,
    String? realtimeUrl,
    Map<String, String> headers = const {},
    http.Client? httpClient,
  }) : _transport = Transport(
          baseUrl: _trimSlash(url),
          key: key,
          tenantPath: tenantInPath && ref != null ? '/${Uri.encodeComponent(ref)}' : '',
          dbParam: tenantInPath ? null : ref,
          extraHeaders: headers,
          httpClient: httpClient,
        ) {
    _transport
      ..ref = ref
      ..authBaseUrl = authUrl == null ? null : _trimSlash(authUrl);

    auth = OxibaseAuth(_transport);
    storage = OxibaseStorage(_transport);
    _realtime = Realtime(
      _transport,
      realtimeUrl ?? '${_trimSlash(url).replaceFirst(RegExp(r'^http'), 'ws')}/ws',
    );

    // A change of identity has to reach the socket: one authenticated as the
    // previous reader would keep streaming rows under the old rules. Rotations
    // are not identity changes, so they are left alone — reconnecting on every
    // hourly refresh would drop every subscription for nothing.
    _authWatch = auth.onAuthStateChange.listen((change) {
      final (event, _) = change;
      if (event != AuthChangeEvent.tokenRefreshed) _realtime.reset();
    });
  }

  final Transport _transport;
  late final Realtime _realtime;
  late final StreamSubscription<(AuthChangeEvent, Session?)> _authWatch;

  /// The data-plane base this client talks to.
  String get url => _transport.baseUrl;

  /// The project this client is bound to, if it was given one.
  String? get ref => _transport.ref;

  /// End-user sign-in for this project.
  late final OxibaseAuth auth;

  /// Per-project file storage.
  late final OxibaseStorage storage;

  static String _trimSlash(String s) => s.replaceAll(RegExp(r'/+$'), '');

  /// Query a collection or a SQL table. Which engine answers is the server's
  /// business: a name that is a SQL table goes to the SQL engine, anything else
  /// to the document engine, and the two can never share a name.
  OxibaseQuery from(String table) => OxibaseQuery(_transport, table);

  /// Query a time-series measurement. Selected by profile rather than by name,
  /// because a measurement exists only once something has been written to it.
  ///
  /// Reads are open to a browser key; **appending needs a service_role key**, as
  /// a series has no per-row rules to adjudicate a write with.
  OxibaseQuery series(String measurement) => OxibaseQuery(
        _transport,
        measurement,
        profile: const {'Accept-Profile': 'tsdb', 'Content-Profile': 'tsdb'},
      );

  /// Run SQL against the project's SQL engine. Values must be bound (`?`), never
  /// interpolated.
  Future<List<SqlResult>> sql(String statement, [List<Object?>? params]) async {
    final res = await _transport.send(
      'POST',
      _transport.url('/api/sql'),
      body: {'sql': statement, if (params != null) 'params': params},
      headers: const {'Content-Type': 'application/json'},
    );
    final decoded = _transport.decodeOrThrow(res);
    return (decoded['results'] as List? ?? const [])
        .whereType<Map<dynamic, dynamic>>()
        .map((e) => SqlResult.fromJson(e.cast<String, dynamic>()))
        .toList();
  }

  /// Ranked full-text search over a collection (BM25), rather than the substring
  /// match `ilike` gives you: results come back best-first, and a document using
  /// a term twice outranks one using it once.
  ///
  /// Needs an index over the fields — [createTextIndex], once, with a
  /// service_role key. Read rules still apply: a closed collection is refused, and
  /// a row-level rule filters the matches, so a filtered search can return fewer
  /// than [limit].
  Future<List<Map<String, dynamic>>> textSearch(
    String collection,
    String query, {
    int? limit,
    bool highlight = false,
  }) async {
    final res = await _transport.send(
      'POST',
      _transport.url('/api/${Uri.encodeComponent(collection)}/text_search'),
      body: {
        'query': query,
        if (limit != null) 'limit': limit,
        if (highlight) 'highlight': true,
      },
      headers: const {'Content-Type': 'application/json'},
    );
    return _transport.decodeListOrThrow(res);
  }

  /// Build the BM25 index [textSearch] needs. Schema work: a service_role key,
  /// once at setup rather than per request.
  Future<void> createTextIndex(String collection, List<String> fields) async {
    final res = await _transport.send(
      'POST',
      _transport.url('/api/${Uri.encodeComponent(collection)}/text_index'),
      body: {'fields': fields},
      headers: const {'Content-Type': 'application/json'},
    );
    if (res.statusCode >= 300) {
      throw _transport.exceptionFor(res, jsonDecode(res.body) as Map<String, dynamic>?);
    }
  }

  /// Subscribe to a collection's changes over the shared WebSocket.
  Future<RealtimeSubscription> subscribe(
    String collection,
    void Function(ChangeEvent) onChange, {
    Map<String, dynamic>? query,
    void Function(String message)? onError,
  }) =>
      _realtime.subscribe(collection, onChange, query: query, onError: onError);

  /// Close the HTTP pool and the realtime socket. Call it when the app is done
  /// with the client — usually never, since there is one for the process.
  void dispose() {
    unawaited(_authWatch.cancel());
    _realtime.dispose();
    _transport.dispose();
  }
}
