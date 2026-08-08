import 'dart:async';
import 'dart:convert';
import 'dart:isolate';
import 'dart:typed_data';

import 'bindings.dart';
import 'oxidb.dart';

/// The same API as [OxiDb], but every call returns a `Future` and runs on a
/// dedicated background isolate — so a synchronous FFI call that contends
/// with the engine's background checkpoint can never block the UI isolate
/// (an ANR waiting to happen on a large database). Open it with
/// [OxiDb.background]; the interface mirrors [OxiDb] method-for-method.
///
/// ```dart
/// final db = await OxiDb.background('${dir.path}/oxidb');
/// await db.insert('users', {'name': 'Alice'});
/// final adults = await db.find('users', query: {'age': {r'$gte': 18}});
/// await db.close();
/// ```
///
/// The database is opened INSIDE the worker isolate, so no native handle
/// crosses isolate boundaries. Calls are serialized through one command
/// channel and answered in order.
class OxiDbAsync {
  OxiDbAsync._(this._toWorker, this._isolate, this._fromWorker);

  final SendPort _toWorker;
  final Isolate _isolate;
  final ReceivePort _fromWorker;
  final _pending = <int, Completer<Object?>>{};
  var _nextId = 0;
  var _closed = false;

  static Future<OxiDbAsync> spawn(String path, Uint8List? encryptionKey) async {
    final fromWorker = ReceivePort();
    final isolate = await Isolate.spawn(
      _entry,
      _Init(fromWorker.sendPort, path, encryptionKey, Bindings.libraryPath),
    );
    final completer = Completer<SendPort>();
    late final OxiDbAsync self;
    fromWorker.listen((msg) {
      if (msg is SendPort) {
        completer.complete(msg);
        return;
      }
      final m = msg as Map;
      final id = m['id'] as int;
      final pending = self._pending.remove(id);
      if (pending == null) return;
      if (m['ok'] == true) {
        pending.complete(m['data']);
      } else {
        pending.completeError(OxiDbException(m['error'] as String? ?? 'error'));
      }
    });
    final toWorker = await completer.future;
    self = OxiDbAsync._(toWorker, isolate, fromWorker);
    // The worker opens the database as its first act; surface a failure.
    await self._call({'op': '_openresult'});
    return self;
  }

  Future<Object?> _call(Map<String, Object?> cmd) {
    if (_closed) {
      return Future.error(OxiDbException('database is closed'));
    }
    final id = _nextId++;
    final completer = Completer<Object?>();
    _pending[id] = completer;
    _toWorker.send({'id': id, 'cmd': cmd});
    return completer.future;
  }

  Future<Map<String, dynamic>> _obj(Map<String, Object?> cmd) async {
    final data = await _call(cmd);
    return data is Map<String, dynamic> ? data : <String, dynamic>{};
  }

  Future<List<Map<String, dynamic>>> _docs(Map<String, Object?> cmd) async {
    final data = await _call(cmd);
    return data is List
        ? data.whereType<Map<String, dynamic>>().toList()
        : <Map<String, dynamic>>[];
  }

  /// Close the database and stop the worker isolate.
  Future<void> close() async {
    if (_closed) return;
    _closed = true;
    try {
      await _call({'op': '_close'});
    } catch (_) {}
    _isolate.kill(priority: Isolate.immediate);
    _fromWorker.close();
  }

  // ── The OxiDb surface, async ──────────────────────────────────────────

  Future<void> createCollection(String name) =>
      _call({'cmd': 'create_collection', 'collection': name});

  Future<List<String>> listCollections() async {
    final data = await _call({'cmd': 'list_collections'});
    return data is List ? data.whereType<String>().toList() : <String>[];
  }

  Future<void> dropCollection(String name) =>
      _call({'cmd': 'drop_collection', 'collection': name});

  Future<int> insert(String collection, Map<String, Object?> doc) async {
    final r = await _obj({'cmd': 'insert', 'collection': collection, 'doc': doc});
    return (r['id'] as num?)?.toInt() ?? 0;
  }

  Future<List<int>> insertMany(
      String collection, List<Map<String, Object?>> docs) async {
    final data =
        await _call({'cmd': 'insert_many', 'collection': collection, 'docs': docs});
    return data is List ? data.whereType<num>().map((n) => n.toInt()).toList() : <int>[];
  }

  Future<List<Map<String, dynamic>>> find(
    String collection, {
    Map<String, Object?> query = const {},
    Map<String, Object?>? sort,
    int? skip,
    int? limit,
  }) =>
      _docs({
        'cmd': 'find',
        'collection': collection,
        'query': query,
        if (sort != null) 'sort': sort,
        if (skip != null) 'skip': skip,
        if (limit != null) 'limit': limit,
      });

  Future<Map<String, dynamic>?> findOne(
      String collection, Map<String, Object?> query) async {
    final data =
        await _call({'cmd': 'find_one', 'collection': collection, 'query': query});
    return data is Map<String, dynamic> ? data : null;
  }

  Future<Map<String, dynamic>> update(
    String collection,
    Map<String, Object?> query,
    Map<String, Object?> update, {
    bool upsert = false,
  }) =>
      _obj({
        'cmd': 'update',
        'collection': collection,
        'query': query,
        'update': update,
        if (upsert) 'upsert': true,
      });

  Future<Map<String, dynamic>> updateOne(
    String collection,
    Map<String, Object?> query,
    Map<String, Object?> update, {
    bool upsert = false,
  }) =>
      _obj({
        'cmd': 'update_one',
        'collection': collection,
        'query': query,
        'update': update,
        if (upsert) 'upsert': true,
      });

  Future<int> delete(String collection, Map<String, Object?> query) async {
    final r = await _obj({'cmd': 'delete', 'collection': collection, 'query': query});
    return (r['deleted'] as num?)?.toInt() ?? 0;
  }

  Future<int> deleteOne(String collection, Map<String, Object?> query) async {
    final r =
        await _obj({'cmd': 'delete_one', 'collection': collection, 'query': query});
    return (r['deleted'] as num?)?.toInt() ?? 0;
  }

  Future<int> count(String collection, {Map<String, Object?> query = const {}}) async {
    final r = await _obj({'cmd': 'count', 'collection': collection, 'query': query});
    return (r['count'] as num?)?.toInt() ?? 0;
  }

  Future<void> createIndex(String collection, String field) =>
      _call({'cmd': 'create_index', 'collection': collection, 'field': field});

  Future<void> createUniqueIndex(String collection, String field) =>
      _call({'cmd': 'create_unique_index', 'collection': collection, 'field': field});

  Future<void> createCompositeIndex(String collection, List<String> fields) =>
      _call({'cmd': 'create_composite_index', 'collection': collection, 'fields': fields});

  Future<void> createTextIndex(String collection, List<String> fields) =>
      _call({'cmd': 'create_text_index', 'collection': collection, 'fields': fields});

  Future<void> createGeoIndex(String collection, String field) =>
      _call({'cmd': 'create_geo_index', 'collection': collection, 'field': field});

  Future<void> createTtlIndex(String collection, String field, Duration expireAfter) =>
      _call({
        'cmd': 'create_ttl_index',
        'collection': collection,
        'field': field,
        'expire_after_seconds': expireAfter.inSeconds,
      });

  Future<List<Map<String, dynamic>>> listIndexes(String collection) =>
      _docs({'cmd': 'list_indexes', 'collection': collection});

  Future<void> dropIndex(String collection, String name) =>
      _call({'cmd': 'drop_index', 'collection': collection, 'field': name});

  Future<List<Map<String, dynamic>>> aggregate(
          String collection, List<Map<String, Object?>> pipeline) =>
      _docs({'cmd': 'aggregate', 'collection': collection, 'pipeline': pipeline});

  Future<List<Map<String, dynamic>>> textSearch(String collection, String query,
          {int limit = 10}) =>
      _docs({'cmd': 'text_search', 'collection': collection, 'query': query, 'limit': limit});

  Future<Object?> sql(String statement, [List<Object?> params = const []]) =>
      _call({
        'engine': 'sql',
        'cmd': 'sql',
        'sql': statement,
        if (params.isNotEmpty) 'params': params,
      });

  // Blob storage (base64 crosses the isolate boundary as a plain string).

  Future<void> createBucket(String name) =>
      _call({'cmd': 'create_bucket', 'bucket': name});

  Future<void> putObject(String bucket, String key, Uint8List data,
          {String contentType = 'application/octet-stream'}) =>
      _call({
        'cmd': 'put_object',
        'bucket': bucket,
        'key': key,
        'data': base64Encode(data),
        'content_type': contentType,
      });

  Future<Uint8List> getObject(String bucket, String key) async {
    final r = await _obj({'cmd': 'get_object', 'bucket': bucket, 'key': key});
    return base64Decode(r['content'] as String? ?? '');
  }

  Future<List<Map<String, dynamic>>> listObjects(String bucket,
          {String? prefix, int? limit}) =>
      _docs({
        'cmd': 'list_objects',
        'bucket': bucket,
        if (prefix != null) 'prefix': prefix,
        if (limit != null) 'limit': limit,
      });

  Future<void> deleteObject(String bucket, String key) =>
      _call({'cmd': 'delete_object', 'bucket': bucket, 'key': key});
}

/// Spawn payload — everything the worker needs before it can open the db.
/// The library path is forwarded because `Bindings.libraryPath` is per-
/// isolate static state and does not cross the spawn.
class _Init {
  _Init(this.reply, this.path, this.key, this.libPath);
  final SendPort reply;
  final String path;
  final Uint8List? key;
  final String? libPath;
}

void _entry(_Init init) {
  if (init.libPath != null) {
    Bindings.libraryPath = init.libPath;
  }
  final commands = ReceivePort();
  init.reply.send(commands.sendPort);

  OxiDb? db;
  Object? openError;
  try {
    db = OxiDb.open(init.path, encryptionKey: init.key);
  } catch (e) {
    openError = e;
  }

  commands.listen((message) {
    final m = (message as Map).cast<String, Object?>();
    final id = m['id'] as int;
    final cmd = (m['cmd'] as Map).cast<String, Object?>();
    final op = cmd['op'];
    if (op == '_openresult') {
      if (openError != null) {
        init.reply.send({'id': id, 'ok': false, 'error': '$openError'});
      } else {
        init.reply.send({'id': id, 'ok': true, 'data': null});
      }
      return;
    }
    if (op == '_close') {
      db?.close();
      init.reply.send({'id': id, 'ok': true, 'data': null});
      commands.close();
      return;
    }
    final d = db;
    if (d == null) {
      init.reply.send({'id': id, 'ok': false, 'error': 'database failed to open'});
      return;
    }
    try {
      final data = d.execute(cmd);
      init.reply.send({'id': id, 'ok': true, 'data': data});
    } on OxiDbException catch (e) {
      init.reply.send({'id': id, 'ok': false, 'error': e.message});
    } catch (e) {
      init.reply.send({'id': id, 'ok': false, 'error': '$e'});
    }
  });
}
