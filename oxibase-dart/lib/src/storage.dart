import 'dart:convert';
import 'dart:typed_data';

import 'transport.dart';
import 'types.dart';

/// Per-project file storage.
///
/// Reads are open to the project's browser key unless a rule closes the bucket;
/// **writes need a service_role key**, because storage has no per-row policy for
/// rules to adjudicate with. In a mobile app that means uploads go through your
/// own server, exactly as they would on the web — the key that ships in the app
/// is the anon one.
class OxibaseStorage {
  OxibaseStorage(this._transport);

  final Transport _transport;

  Uri _url(String path) => _transport.url('/api/storage$path');

  /// Buckets in the project, and the bytes stored (the quota usage).
  Future<({List<String> buckets, int totalBytes})> listBuckets() async {
    final res = await _transport.send('GET', _url(''));
    final j = _transport.decodeOrThrow(res);
    return (
      buckets: (j['buckets'] as List?)?.map((e) => e.toString()).toList() ?? const [],
      totalBytes: (j['total_bytes'] as num?)?.toInt() ?? 0,
    );
  }

  Future<void> createBucket(String name) async {
    final res = await _transport.send('POST', _url('/${Uri.encodeComponent(name)}'));
    if (res.statusCode >= 300) throw _transport.exceptionFor(res, null);
  }

  /// Delete an empty bucket. A bucket with objects in it answers 409 rather than
  /// taking them with it.
  Future<void> deleteBucket(String name) async {
    final res = await _transport.send('DELETE', _url('/${Uri.encodeComponent(name)}'));
    if (res.statusCode >= 300) throw _transport.exceptionFor(res, null);
  }

  OxibaseBucket from(String bucket) => OxibaseBucket(_transport, bucket);
}

/// One bucket.
class OxibaseBucket {
  OxibaseBucket(this._transport, this.bucket);

  final Transport _transport;
  final String bucket;

  String get _b => Uri.encodeComponent(bucket);
  String _keyPath(String key) => key.split('/').map(Uri.encodeComponent).join('/');

  Uri _url(String path) => _transport.url('/api/storage$path');

  /// Upload (or replace) an object. Needs a service_role key.
  Future<StorageObject> upload(
    String key,
    Uint8List bytes, {
    String contentType = 'application/octet-stream',
  }) async {
    final res = await _transport.send(
      'PUT',
      _url('/$_b/${_keyPath(key)}'),
      body: bytes,
      headers: {'Content-Type': contentType},
    );
    final j = _transport.decodeOrThrow(res);
    return StorageObject.fromJson({'bucket': bucket, 'key': key, ...j});
  }

  /// The bytes of an object.
  Future<Uint8List> download(String key) async {
    final res = await _transport.send('GET', _url('/$_b/${_keyPath(key)}'));
    if (res.statusCode >= 300) throw _transport.exceptionFor(res, null);
    return res.bodyBytes;
  }

  Future<List<StorageObject>> list({String? prefix, int? limit}) async {
    final res = await _transport.send(
      'GET',
      _url('/$_b').replace(queryParameters: {
        ..._url('/$_b').queryParameters,
        if (prefix != null) 'prefix': prefix,
        if (limit != null) 'limit': '$limit',
      }),
    );
    final j = _transport.decodeOrThrow(res);
    return (j['objects'] as List? ?? const [])
        .whereType<Map<dynamic, dynamic>>()
        .map((e) => StorageObject.fromJson(e.cast<String, dynamic>()))
        .toList();
  }

  Future<void> remove(String key) async {
    final res = await _transport.send('DELETE', _url('/$_b/${_keyPath(key)}'));
    if (res.statusCode >= 300) throw _transport.exceptionFor(res, null);
  }

  /// The object's URL. Requests to it still carry the Authorization header, so
  /// this is not a link you can hand to an `Image.network` without one — see the
  /// README for the pattern.
  Uri publicUrl(String key) => _url('/$_b/${_keyPath(key)}');

  /// Ranked full-text search over the *contents* of this bucket's files: HTML,
  /// XML, JSON, PDF, DOCX, XLSX, and images when the server has OCR. Text is
  /// extracted on upload, so there is no index to build first.
  ///
  /// Per bucket by design — a bucket's read rule is what keeps its files private,
  /// so a search across buckets could report keys from one this key cannot read.
  Future<List<StorageSearchHit>> search(
    String query, {
    int? limit,
    bool highlight = false,
    int? snippetChars,
    int? maxSnippets,
  }) async {
    final res = await _transport.send(
      'POST',
      _url('/_search'),
      body: {
        'bucket': bucket,
        'query': query,
        if (limit != null) 'limit': limit,
        if (highlight && snippetChars == null && maxSnippets == null)
          'highlight': true
        else if (highlight)
          'highlight': {
            if (snippetChars != null) 'snippet_chars': snippetChars,
            if (maxSnippets != null) 'max_snippets': maxSnippets,
          },
      },
      headers: {'Content-Type': 'application/json'},
    );
    if (res.statusCode >= 300) {
      throw _transport.exceptionFor(res, jsonDecode(res.body) as Map<String, dynamic>?);
    }
    final parsed = jsonDecode(res.body);
    if (parsed is! List) return const [];
    return parsed
        .whereType<Map<dynamic, dynamic>>()
        .map((e) => StorageSearchHit.fromJson(e.cast<String, dynamic>()))
        .toList();
  }
}
