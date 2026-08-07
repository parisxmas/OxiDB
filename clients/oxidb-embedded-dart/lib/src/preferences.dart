import 'oxidb.dart';

/// SharedPreferences-shaped sugar over a collection: one document per key
/// (`{k, v}`, unique index on `k`), each write a single upsert, each read
/// an indexed point lookup. Values are anything JSON representable —
/// including whole maps and lists, which SharedPreferences cannot hold.
///
/// This is a convenience view, not the library: the same [OxiDb] handle
/// serves full document collections, geo queries, SQL and blobs alongside.
class Preferences {
  Preferences(this._db, {String collection = '_prefs'}) : _collection = collection {
    try {
      _db.createUniqueIndex(_collection, 'k');
    } on OxiDbException {
      // Index already exists — correctness does not depend on it; every
      // write goes through a keyed upsert, the index only speeds reads.
    }
  }

  final OxiDb _db;
  final String _collection;

  /// Insert-or-replace, one upsert — no read-modify-write race.
  void put(String key, Object? value) {
    _db.updateOne(
      _collection,
      {'k': key},
      {
        r'$set': {'v': value}
      },
      upsert: true,
    );
  }

  Object? get(String key) => _db.findOne(_collection, {'k': key})?['v'];

  String? getString(String key) => get(key) is String ? get(key) as String : null;

  int? getInt(String key) {
    final v = get(key);
    return v is int ? v : (v is double ? v.toInt() : null);
  }

  double? getDouble(String key) {
    final v = get(key);
    return v is double ? v : (v is int ? v.toDouble() : null);
  }

  bool? getBool(String key) => get(key) is bool ? get(key) as bool : null;

  bool contains(String key) => _db.findOne(_collection, {'k': key}) != null;

  /// Removes [key]; true when something was removed.
  bool remove(String key) => _db.deleteOne(_collection, {'k': key}) > 0;

  List<String> keys() => _db
      .find(_collection)
      .map((d) => d['k'])
      .whereType<String>()
      .toList();

  /// Remove every key in this namespace.
  void clear() => _db.delete(_collection, {});
}
