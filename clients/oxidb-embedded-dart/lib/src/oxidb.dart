import 'dart:convert';
import 'dart:ffi';
import 'dart:typed_data';

import 'package:ffi/ffi.dart';

import 'bindings.dart';

/// Thrown when the engine refuses a command.
class OxiDbException implements Exception {
  OxiDbException(this.message);
  final String message;
  @override
  String toString() => 'OxiDbException: $message';
}

/// An embedded (in-process) OxiDB database — the full engine, no server:
/// document CRUD with MongoDB-style queries and upsert, field/unique/
/// composite/text/geo/TTL indexes, aggregation pipelines, BM25 full-text
/// search, ACID transactions, blob storage, the SQL engine, and AES-256-GCM
/// encryption at rest.
///
/// ```dart
/// final db = OxiDb.open('${dir.path}/oxidb');
/// db.insert('users', {'name': 'Alice', 'age': 30});
/// final adults = db.find('users', query: {'age': {r'$gte': 18}});
/// db.close();
/// ```
///
/// Storage is disk-first by default: documents live in an mmap'd file and
/// resident memory stays bounded, which is exactly what a phone wants.
/// Every write is WAL-backed — durable when the call returns.
class OxiDb {
  OxiDb._(this._handle);

  Pointer<Void> _handle;
  final Bindings _b = Bindings.instance();

  /// Open (or create) a database at [path] (a directory).
  ///
  /// [encryptionKey] — exactly 32 bytes — turns on transparent AES-256-GCM
  /// at the storage layer (data files and WAL both). Keep the key in the
  /// platform keystore (Keychain / Android Keystore), hand it here at open.
  static OxiDb open(String path, {Uint8List? encryptionKey}) {
    final b = Bindings.instance();
    final cPath = path.toNativeUtf8();
    try {
      Pointer<Void> h;
      if (encryptionKey == null) {
        h = b.open(cPath);
      } else {
        if (encryptionKey.length != 32) {
          throw ArgumentError('encryptionKey must be exactly 32 bytes');
        }
        final cKey = malloc<Uint8>(32);
        cKey.asTypedList(32).setAll(0, encryptionKey);
        try {
          h = b.openEncryptedBytes(cPath, cKey, 32);
        } finally {
          malloc.free(cKey);
        }
      }
      if (h == nullptr) {
        throw OxiDbException('failed to open OxiDB at $path');
      }
      return OxiDb._(h);
    } finally {
      malloc.free(cPath);
    }
  }

  /// Close the database (folds pending work; a cleanly closed directory is
  /// snapshot-only). Safe to call more than once.
  void close() {
    if (_handle != nullptr) {
      _b.close(_handle);
      _handle = nullptr;
    }
  }

  bool get isOpen => _handle != nullptr;

  // ── Low level ─────────────────────────────────────────────────────────

  /// Execute a raw JSON command (the OxiDB server protocol, in-process)
  /// and return the `data` value of the `{ok, data|error}` envelope.
  Object? execute(Map<String, Object?> command) {
    if (_handle == nullptr) {
      throw StateError('OxiDb is closed');
    }
    final cCmd = jsonEncode(command).toNativeUtf8();
    Pointer<Utf8> out = nullptr;
    try {
      out = _b.execute(_handle, cCmd);
      if (out == nullptr) {
        throw OxiDbException('execute returned null');
      }
      final decoded = jsonDecode(out.toDartString());
      if (decoded is! Map<String, dynamic>) {
        throw OxiDbException('malformed response: $decoded');
      }
      if (decoded['ok'] != true) {
        throw OxiDbException(decoded['error']?.toString() ?? 'unknown error');
      }
      return decoded['data'];
    } finally {
      malloc.free(cCmd);
      if (out != nullptr) {
        _b.freeString(out);
      }
    }
  }

  Map<String, dynamic> _obj(Object? data) =>
      data is Map<String, dynamic> ? data : <String, dynamic>{};

  List<Map<String, dynamic>> _docs(Object? data) => data is List
      ? data.whereType<Map<String, dynamic>>().toList()
      : <Map<String, dynamic>>[];

  // ── Collections ───────────────────────────────────────────────────────

  void createCollection(String name) =>
      execute({'cmd': 'create_collection', 'collection': name});

  List<String> listCollections() {
    final data = execute({'cmd': 'list_collections'});
    return data is List ? data.whereType<String>().toList() : <String>[];
  }

  void dropCollection(String name) =>
      execute({'cmd': 'drop_collection', 'collection': name});

  // ── CRUD ──────────────────────────────────────────────────────────────

  /// Insert a document; returns its id.
  int insert(String collection, Map<String, Object?> doc) {
    final r = _obj(execute({'cmd': 'insert', 'collection': collection, 'doc': doc}));
    return (r['id'] as num?)?.toInt() ?? 0;
  }

  /// Insert many; returns the new ids.
  List<int> insertMany(String collection, List<Map<String, Object?>> docs) {
    final data = execute({'cmd': 'insert_many', 'collection': collection, 'docs': docs});
    return data is List ? data.whereType<num>().map((n) => n.toInt()).toList() : <int>[];
  }

  /// Find documents. The full query language: operators (`$gt`, `$in`,
  /// `$regex`, …), logic (`$and`/`$or`), geo (`$near`, `$geoWithin` — box,
  /// circle, polygon), plus `sort`/`skip`/`limit`.
  List<Map<String, dynamic>> find(
    String collection, {
    Map<String, Object?> query = const {},
    Map<String, Object?>? sort,
    int? skip,
    int? limit,
  }) {
    final cmd = <String, Object?>{
      'cmd': 'find',
      'collection': collection,
      'query': query,
      if (sort != null) 'sort': sort,
      if (skip != null) 'skip': skip,
      if (limit != null) 'limit': limit,
    };
    return _docs(execute(cmd));
  }

  Map<String, dynamic>? findOne(String collection, Map<String, Object?> query) {
    final data = execute({'cmd': 'find_one', 'collection': collection, 'query': query});
    return data is Map<String, dynamic> ? data : null;
  }

  /// Update matching documents with `$set`/`$inc`/… operators. With
  /// [upsert], a query matching nothing inserts a document synthesized
  /// from the query's equality conditions with the update applied; the
  /// result then carries `upserted: <id>`.
  Map<String, dynamic> update(
    String collection,
    Map<String, Object?> query,
    Map<String, Object?> update, {
    bool upsert = false,
  }) {
    return _obj(execute({
      'cmd': 'update',
      'collection': collection,
      'query': query,
      'update': update,
      if (upsert) 'upsert': true,
    }));
  }

  /// Update at most one document (upsert as in [update]).
  Map<String, dynamic> updateOne(
    String collection,
    Map<String, Object?> query,
    Map<String, Object?> update, {
    bool upsert = false,
  }) {
    return _obj(execute({
      'cmd': 'update_one',
      'collection': collection,
      'query': query,
      'update': update,
      if (upsert) 'upsert': true,
    }));
  }

  /// Delete matching documents; returns how many.
  int delete(String collection, Map<String, Object?> query) {
    final r = _obj(execute({'cmd': 'delete', 'collection': collection, 'query': query}));
    return (r['deleted'] as num?)?.toInt() ?? 0;
  }

  int deleteOne(String collection, Map<String, Object?> query) {
    final r = _obj(execute({'cmd': 'delete_one', 'collection': collection, 'query': query}));
    return (r['deleted'] as num?)?.toInt() ?? 0;
  }

  int count(String collection, {Map<String, Object?> query = const {}}) {
    final r = _obj(execute({'cmd': 'count', 'collection': collection, 'query': query}));
    return (r['count'] as num?)?.toInt() ?? 0;
  }

  // ── Indexes ───────────────────────────────────────────────────────────

  void createIndex(String collection, String field) =>
      execute({'cmd': 'create_index', 'collection': collection, 'field': field});

  void createUniqueIndex(String collection, String field) =>
      execute({'cmd': 'create_unique_index', 'collection': collection, 'field': field});

  void createCompositeIndex(String collection, List<String> fields) =>
      execute({'cmd': 'create_composite_index', 'collection': collection, 'fields': fields});

  void createTextIndex(String collection, List<String> fields) =>
      execute({'cmd': 'create_text_index', 'collection': collection, 'fields': fields});

  /// Geohash index for `$near` / `$geoWithin` — the live-map queries.
  void createGeoIndex(String collection, String field) =>
      execute({'cmd': 'create_geo_index', 'collection': collection, 'field': field});

  /// Documents expire [expireAfter] after their [field] timestamp — offline
  /// caches that clean themselves.
  void createTtlIndex(String collection, String field, Duration expireAfter) =>
      execute({
        'cmd': 'create_ttl_index',
        'collection': collection,
        'field': field,
        'expire_after_seconds': expireAfter.inSeconds,
      });

  List<Map<String, dynamic>> listIndexes(String collection) =>
      _docs(execute({'cmd': 'list_indexes', 'collection': collection}));

  void dropIndex(String collection, String name) =>
      execute({'cmd': 'drop_index', 'collection': collection, 'field': name});

  // ── Aggregation & search ──────────────────────────────────────────────

  /// Aggregation pipeline: `$match`, `$group`, `$sort`, `$lookup`,
  /// `$geoNear`, `$unwind`, window functions, … — same stages as the server.
  List<Map<String, dynamic>> aggregate(
          String collection, List<Map<String, Object?>> pipeline) =>
      _docs(execute({'cmd': 'aggregate', 'collection': collection, 'pipeline': pipeline}));

  /// BM25 full-text search over a text index.
  List<Map<String, dynamic>> textSearch(String collection, String query,
          {int limit = 10}) =>
      _docs(execute({
        'cmd': 'text_search',
        'collection': collection,
        'query': query,
        'limit': limit,
      }));

  // ── SQL engine ────────────────────────────────────────────────────────

  /// Run a SQL statement on the embedded relational engine (`?`/`$N`
  /// placeholders bound from [params]). Separate storage from the document
  /// collections, same directory, same encryption.
  Object? sql(String statement, [List<Object?> params = const []]) =>
      execute({
        'engine': 'sql',
        'cmd': 'sql',
        'sql': statement,
        if (params.isNotEmpty) 'params': params,
      });

  // ── Transactions ──────────────────────────────────────────────────────

  void beginTransaction() => execute({'cmd': 'begin_tx'});
  void commitTransaction() => execute({'cmd': 'commit_tx'});
  void rollbackTransaction() => execute({'cmd': 'rollback_tx'});

  /// Run [body] inside a transaction: commit on return, rollback on throw.
  T transaction<T>(T Function() body) {
    beginTransaction();
    try {
      final result = body();
      commitTransaction();
      return result;
    } catch (_) {
      try {
        rollbackTransaction();
      } catch (_) {
        // The original error is the one worth surfacing.
      }
      rethrow;
    }
  }

  // ── Blob storage ──────────────────────────────────────────────────────

  void createBucket(String name) => execute({'cmd': 'create_bucket', 'bucket': name});

  void putObject(String bucket, String key, Uint8List data,
          {String contentType = 'application/octet-stream'}) =>
      execute({
        'cmd': 'put_object',
        'bucket': bucket,
        'key': key,
        'data': base64Encode(data),
        'content_type': contentType,
      });

  Uint8List getObject(String bucket, String key) {
    final r = _obj(execute({'cmd': 'get_object', 'bucket': bucket, 'key': key}));
    return base64Decode(r['content'] as String? ?? '');
  }

  List<Map<String, dynamic>> listObjects(String bucket, {String? prefix, int? limit}) =>
      _docs(execute({
        'cmd': 'list_objects',
        'bucket': bucket,
        if (prefix != null) 'prefix': prefix,
        if (limit != null) 'limit': limit,
      }));

  void deleteObject(String bucket, String key) =>
      execute({'cmd': 'delete_object', 'bucket': bucket, 'key': key});
}
