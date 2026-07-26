/// Values the client hands back, and the errors it throws.
library;

/// A signed-in end user's session.
///
/// [refreshToken] is **single-use**: the moment [accessToken] is renewed the old
/// refresh token is revoked server-side. An app that stores a session must store
/// the new one on every [AuthChangeEvent.tokenRefreshed] — see
/// `OxibaseAuth.onAuthStateChange`. Keeping only what sign-in returned leaves a
/// spent token in storage and the next launch cannot resume.
class Session {
  const Session({required this.accessToken, required this.refreshToken, this.email});

  final String accessToken;
  final String refreshToken;

  /// The address the session belongs to, when the server told us.
  final String? email;

  Map<String, dynamic> toJson() => {
        'access_token': accessToken,
        'refresh_token': refreshToken,
        if (email != null) 'email': email,
      };

  static Session? fromJson(Map<String, dynamic>? json) {
    if (json == null) return null;
    final access = json['access_token'] ?? json['token'];
    final refresh = json['refresh_token'] ?? json['refreshToken'];
    if (access is! String || refresh is! String) return null;
    return Session(
      accessToken: access,
      refreshToken: refresh,
      email: json['email'] as String?,
    );
  }

  @override
  String toString() => 'Session(email: $email, accessToken: <redacted>, refreshToken: <redacted>)';
}

/// What moved the session.
enum AuthChangeEvent {
  /// A sign-in, or a session adopted from a redirect.
  signedIn,

  /// The access token was renewed **and the refresh token rotated**. Persist this.
  tokenRefreshed,

  /// Signed out; the session in the change is null.
  signedOut,
}

/// One realtime change.
class ChangeEvent {
  const ChangeEvent({required this.op, required this.collection, this.docId, this.doc});

  /// `insert`, `update` or `delete`.
  final String op;
  final String collection;
  final Object? docId;

  /// The changed document. Absent on deletes.
  final Map<String, dynamic>? doc;

  @override
  String toString() => 'ChangeEvent($op $collection${docId == null ? '' : ' #$docId'})';
}

/// One statement's result from `.sql()`.
class SqlResult {
  const SqlResult({this.columns, this.rows, this.affected, this.lastInsertId, this.ddl});

  final List<String>? columns;
  final List<List<dynamic>>? rows;
  final int? affected;
  final int? lastInsertId;
  final bool? ddl;

  /// Rows as maps, which is usually what a UI wants.
  List<Map<String, dynamic>> get asMaps {
    final cols = columns;
    final data = rows;
    if (cols == null || data == null) return const [];
    return data
        .map((r) =>
            <String, dynamic>{for (var i = 0; i < cols.length && i < r.length; i++) cols[i]: r[i]})
        .toList();
  }

  static SqlResult fromJson(Map<String, dynamic> j) => SqlResult(
        columns: (j['columns'] as List?)?.map((e) => e.toString()).toList(),
        rows: (j['rows'] as List?)?.map((r) => (r as List).toList()).toList(),
        affected: j['affected'] as int?,
        lastInsertId: j['last_insert_id'] as int?,
        ddl: j['ddl'] as bool?,
      );
}

/// Metadata of one stored object.
class StorageObject {
  const StorageObject({
    required this.key,
    required this.bucket,
    required this.size,
    required this.contentType,
    this.etag,
    this.createdAt,
  });

  final String key;
  final String bucket;
  final int size;
  final String contentType;
  final String? etag;
  final String? createdAt;

  static StorageObject fromJson(Map<String, dynamic> j) => StorageObject(
        key: (j['key'] ?? '').toString(),
        bucket: (j['bucket'] ?? '').toString(),
        size: (j['size'] as num?)?.toInt() ?? 0,
        contentType: (j['content_type'] ?? 'application/octet-stream').toString(),
        etag: j['etag'] as String?,
        createdAt: j['created_at'] as String?,
      );
}

/// One hit from a full-text search over stored files.
class StorageSearchHit {
  const StorageSearchHit({
    required this.bucket,
    required this.key,
    required this.score,
    this.highlights,
  });

  final String bucket;
  final String key;
  final double score;

  /// Present only when highlights were asked for; snippets carry `<mark>`.
  final List<String>? highlights;

  static StorageSearchHit fromJson(Map<String, dynamic> j) => StorageSearchHit(
        bucket: (j['bucket'] ?? '').toString(),
        key: (j['key'] ?? '').toString(),
        score: (j['score'] as num?)?.toDouble() ?? 0,
        highlights: (j['highlights'] as List?)?.map((e) => e.toString()).toList(),
      );
}

/// Anything the server refused, with the status it refused with.
///
/// The status matters here rather than being noise: OxiBase distinguishes them
/// deliberately, and an app should too.
class OxibaseException implements Exception {
  const OxibaseException(this.message, {this.statusCode, this.retryAfter});

  final String message;
  final int? statusCode;

  /// Seconds to wait, from `Retry-After`, when [isRateLimited].
  final int? retryAfter;

  /// The caller is not allowed — a security rule said no, or the key is wrong
  /// for the operation (a browser key writing a SQL table, say).
  bool get isDenied => statusCode == 401 || statusCode == 403;

  /// Too fast: a rule's per-identity rate limit, or the project's request cap.
  /// [retryAfter] says how long to wait. Distinct from [isDenied] on purpose —
  /// "not yet" is not "no".
  bool get isRateLimited => statusCode == 429;

  /// A unique index already holds this value: the row is already there. What
  /// deduplicating by key looks like when it works.
  bool get isConflict => statusCode == 409;

  @override
  String toString() =>
      'OxibaseException(${statusCode ?? '-'}): $message${retryAfter == null ? '' : ' (retry after ${retryAfter}s)'}';
}
