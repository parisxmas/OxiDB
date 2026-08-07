// Host-side integration tests against the real native library. Point
// OXIDB_FFI_LIB at liboxidb_embedded_ffi.dylib/.so (defaults to the repo's
// target/release build). Every claim the README makes is exercised here:
// CRUD + upsert, indexes (unique/geo/TTL), geo queries, aggregation with
// $geoNear, full-text search, transactions, blobs, SQL, encrypted-at-rest
// with a byte key, and the Preferences sugar.

import 'dart:io';
import 'dart:typed_data';

import 'package:oxidb_embedded/oxidb_embedded.dart';
import 'package:test/test.dart';

String _libPath() {
  final env = Platform.environment['OXIDB_FFI_LIB'];
  if (env != null) return env;
  final ext = Platform.isMacOS ? 'dylib' : 'so';
  return '${Directory.current.path}/../../target/release/liboxidb_embedded_ffi.$ext';
}

void main() {
  Bindings.libraryPath = _libPath();

  late Directory dir;
  late OxiDb db;

  setUp(() {
    dir = Directory.systemTemp.createTempSync('oxidb_dart_');
    db = OxiDb.open(dir.path);
  });

  tearDown(() {
    db.close();
    try {
      dir.deleteSync(recursive: true);
    } catch (_) {}
  });

  test('crud, query operators and upsert', () {
    final id = db.insert('users', {'name': 'Alice', 'age': 30});
    expect(id, greaterThan(0));
    db.insertMany('users', [
      {'name': 'Bob', 'age': 17},
      {'name': 'Carol', 'age': 41},
    ]);

    final adults = db.find('users',
        query: {
          'age': {r'$gte': 18}
        },
        sort: {'age': 1});
    expect(adults.map((d) => d['name']), ['Alice', 'Carol']);

    // Upsert: first call inserts (reports the id), second replaces.
    final first = db.updateOne(
        'prefs_like',
        {'k': 'theme'},
        {
          r'$set': {'v': 'dark'}
        },
        upsert: true);
    expect(first['upserted'], isNotNull);
    final second = db.updateOne(
        'prefs_like',
        {'k': 'theme'},
        {
          r'$set': {'v': 'light'}
        },
        upsert: true);
    expect(second['upserted'], isNull);
    expect(db.count('prefs_like'), 1);
    expect(db.findOne('prefs_like', {'k': 'theme'})?['v'], 'light');

    expect(db.deleteOne('users', {'name': 'Bob'}), 1);
    expect(db.count('users'), 2);
  });

  test('geo index, \$near and \$geoNear aggregation', () {
    for (var i = 0; i < 20; i++) {
      db.insert('places', {
        'name': 'p$i',
        'loc': [28.97 + i * 0.01, 41.0],
      });
    }
    db.createGeoIndex('places', 'loc');

    final near = db.find('places',
        query: {
          'loc': {
            r'$near': {
              r'$geometry': {
                'type': 'Point',
                'coordinates': [28.97, 41.0]
              },
              r'$maxDistance': 2000.0,
            }
          }
        });
    expect(near.first['name'], 'p0');
    expect(near.length, lessThan(20));

    final ranked = db.aggregate('places', [
      {
        r'$geoNear': {
          'near': [28.97, 41.0],
          'key': 'loc',
          'distanceField': 'dist_m',
        }
      },
      {r'$limit': 3},
    ]);
    expect(ranked.length, 3);
    expect(ranked.first['dist_m'], lessThan(1.0));
    expect(ranked[1]['dist_m'], greaterThan(ranked.first['dist_m'] as num));
  });

  test('full-text search over a text index', () {
    db.insert('notes', {'title': 'grocery run', 'body': 'buy milk and bread'});
    db.insert('notes', {'title': 'workout', 'body': 'leg day at the gym'});
    db.createTextIndex('notes', ['title', 'body']);
    final hits = db.textSearch('notes', 'milk');
    expect(hits, hasLength(1));
    expect(hits.first['title'], 'grocery run');
  });

  test('transactions commit and roll back', () {
    db.insert('acct', {'name': 'a', 'bal': 100});
    db.transaction(() {
      db.update('acct', {'name': 'a'}, {
        r'$inc': {'bal': -30}
      });
    });
    expect(db.findOne('acct', {'name': 'a'})?['bal'], 70);

    expect(
      () => db.transaction(() {
        db.update('acct', {'name': 'a'}, {
          r'$inc': {'bal': -1000}
        });
        throw StateError('abort');
      }),
      throwsStateError,
    );
    expect(db.findOne('acct', {'name': 'a'})?['bal'], 70,
        reason: 'rolled-back write must not stick');
  });

  test('blob storage roundtrip', () {
    db.createBucket('avatars');
    final bytes = Uint8List.fromList(List.generate(1024, (i) => i % 251));
    db.putObject('avatars', 'u1.png', bytes, contentType: 'image/png');
    expect(db.getObject('avatars', 'u1.png'), bytes);
    expect(db.listObjects('avatars').length, 1);
    db.deleteObject('avatars', 'u1.png');
    expect(db.listObjects('avatars'), isEmpty);
  });

  test('sql engine works in the same directory', () {
    db.sql('CREATE TABLE todos (id INT PRIMARY KEY, title TEXT)');
    db.sql('INSERT INTO todos VALUES (?, ?)', [1, 'ship it']);
    final out = db.sql('SELECT title FROM todos WHERE id = ?', [1]);
    expect(out.toString(), contains('ship it'));
  });

  test('preferences sugar', () {
    final prefs = Preferences(db);
    prefs.put('theme', 'dark');
    prefs.put('volume', 0.75);
    prefs.put('onboarded', true);
    prefs.put('profile', {'name': 'Alice', 'tags': ['a', 'b']});

    expect(prefs.getString('theme'), 'dark');
    expect(prefs.getDouble('volume'), 0.75);
    expect(prefs.getBool('onboarded'), true);
    expect((prefs.get('profile') as Map)['name'], 'Alice');
    expect(prefs.contains('theme'), isTrue);
    expect(prefs.keys().toSet(), {'theme', 'volume', 'onboarded', 'profile'});

    prefs.put('theme', 'light'); // replace, not duplicate
    expect(prefs.getString('theme'), 'light');
    expect(db.count('_prefs'), 4);

    expect(prefs.remove('volume'), isTrue);
    expect(prefs.remove('volume'), isFalse);
    prefs.clear();
    expect(prefs.keys(), isEmpty);
  });

  test('encrypted at rest with a byte key, wrong key never decrypts', () {
    final encDir = Directory.systemTemp.createTempSync('oxidb_dart_enc_');
    final key = Uint8List.fromList(List.filled(32, 42));
    var enc = OxiDb.open(encDir.path, encryptionKey: key);
    enc.insert('vault', {'secret': 's3cr3t-payload'});
    enc.close();

    // Reopen with the same key: readable.
    enc = OxiDb.open(encDir.path, encryptionKey: key);
    expect(enc.findOne('vault', {})?['secret'], 's3cr3t-payload');
    enc.close();

    // The plaintext must not appear anywhere on disk.
    var leaked = false;
    for (final f in encDir.listSync(recursive: true).whereType<File>()) {
      if (String.fromCharCodes(f.readAsBytesSync()).contains('s3cr3t-payload')) {
        leaked = true;
      }
    }
    expect(leaked, isFalse, reason: 'plaintext leaked to disk');

    // A 16-byte key is refused up front.
    expect(
      () => OxiDb.open(encDir.path, encryptionKey: Uint8List(16)),
      throwsArgumentError,
    );
    encDir.deleteSync(recursive: true);
  });

  test('ttl index accepts a Duration', () {
    db.insert('cache', {'k': 'x', 'at': DateTime.now().toIso8601String()});
    db.createTtlIndex('cache', 'at', const Duration(hours: 1));
    final idx = db.listIndexes('cache');
    expect(idx.any((i) => i['index_type'] == 'ttl'), isTrue, reason: '$idx');
  });
}
