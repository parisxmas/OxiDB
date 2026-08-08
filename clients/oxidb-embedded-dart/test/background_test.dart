// OxiDb.background(): the async facade over a worker isolate. Same real
// dylib as the sync tests (OXIDB_FFI_LIB, or target/release), driven
// entirely through Futures — proving the isolate command channel carries
// the full surface and that concurrent awaits are answered correctly.

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
  // The worker isolate reads Bindings.libraryPath forwarded from here.
  Bindings.libraryPath = _libPath();

  late Directory dir;
  late OxiDbAsync db;

  setUp(() async {
    dir = Directory.systemTemp.createTempSync('oxidb_bg_');
    db = await OxiDb.background(dir.path);
  });

  tearDown(() async {
    await db.close();
    try {
      dir.deleteSync(recursive: true);
    } catch (_) {}
  });

  test('crud, query, upsert over the worker', () async {
    final id = await db.insert('users', {'name': 'Alice', 'age': 30});
    expect(id, greaterThan(0));
    await db.insertMany('users', [
      {'name': 'Bob', 'age': 17},
      {'name': 'Carol', 'age': 41},
    ]);
    final adults = await db.find('users',
        query: {
          'age': {r'$gte': 18}
        },
        sort: {'age': 1});
    expect(adults.map((d) => d['name']), ['Alice', 'Carol']);

    final first = await db.updateOne('prefs', {'k': 'theme'},
        {r'$set': {'v': 'dark'}}, upsert: true);
    expect(first['upserted'], isNotNull);
    final second = await db.updateOne('prefs', {'k': 'theme'},
        {r'$set': {'v': 'light'}}, upsert: true);
    expect(second['upserted'], isNull);
    expect(await db.count('prefs'), 1);
    expect((await db.findOne('prefs', {'k': 'theme'}))?['v'], 'light');
  });

  test('concurrent awaits are matched to the right reply', () async {
    // Fire 200 inserts without awaiting between them, then await all.
    // If the command channel mismatched ids, the count would be wrong.
    final futures = [
      for (var i = 0; i < 200; i++) db.insert('c', {'i': i})
    ];
    final ids = await Future.wait(futures);
    expect(ids.toSet().length, 200, reason: 'ids must be distinct');
    expect(await db.count('c'), 200);

    // Interleave reads and writes concurrently.
    final results = await Future.wait([
      db.count('c'),
      db.insert('c', {'i': 999}),
      db.find('c', query: {'i': 5}),
    ]);
    expect(results[0], 200);
    expect((results[2] as List).length, 1);
  });

  test('errors surface as exceptions, not hangs', () async {
    // A real engine refusal (unique violation) must reject the Future,
    // not hang the channel.
    await db.createUniqueIndex('u', 'k');
    await db.insert('u', {'k': 'x'});
    await expectLater(
      db.insert('u', {'k': 'x'}),
      throwsA(isA<OxiDbException>()),
    );
    // The channel is still usable after an error.
    expect(await db.insert('u', {'k': 'y'}), greaterThan(0));
  });

  test('geo, aggregation, fts, sql, blobs all work async', () async {
    for (var i = 0; i < 12; i++) {
      await db.insert('places', {
        'name': 'p$i',
        'loc': [28.9 + i * 0.01, 41.0]
      });
    }
    await db.createGeoIndex('places', 'loc');
    final ranked = await db.aggregate('places', [
      {
        r'$geoNear': {
          'near': [28.9, 41.0],
          'key': 'loc',
          'distanceField': 'd'
        }
      },
      {r'$limit': 3},
    ]);
    expect(ranked.length, 3);
    expect(ranked.first['d'], lessThan(ranked[1]['d'] as num));

    await db.insert('notes', {'text': 'buy milk today'});
    await db.createTextIndex('notes', ['text']);
    expect((await db.textSearch('notes', 'milk')).length, 1);

    await db.sql('CREATE TABLE t (id INT PRIMARY KEY, v TEXT)');
    await db.sql('INSERT INTO t VALUES (?, ?)', [1, 'hi']);
    expect((await db.sql('SELECT v FROM t')).toString(), contains('hi'));

    await db.createBucket('b');
    final bytes = Uint8List.fromList(List.generate(64, (i) => i));
    await db.putObject('b', 'k', bytes);
    expect(await db.getObject('b', 'k'), bytes);
  });

  test('encrypted background open with a byte key', () async {
    final encDir = Directory.systemTemp.createTempSync('oxidb_bg_enc_');
    final key = Uint8List.fromList(List.filled(32, 9));
    var enc = await OxiDb.background(encDir.path, encryptionKey: key);
    await enc.insert('v', {'secret': 'hunter2'});
    await enc.close();

    enc = await OxiDb.background(encDir.path, encryptionKey: key);
    expect((await enc.findOne('v', {}))?['secret'], 'hunter2');
    await enc.close();

    var leaked = false;
    for (final f in encDir.listSync(recursive: true).whereType<File>()) {
      if (String.fromCharCodes(f.readAsBytesSync()).contains('hunter2')) {
        leaked = true;
      }
    }
    expect(leaked, isFalse);
    encDir.deleteSync(recursive: true);
  });

  test('calling after close rejects', () async {
    final d = await OxiDb.background(
        Directory.systemTemp.createTempSync('oxidb_bg_c_').path);
    await d.insert('x', {'a': 1});
    await d.close();
    await expectLater(db2Call(d), throwsA(isA<OxiDbException>()));
  });
}

Future<void> db2Call(OxiDbAsync d) => d.insert('x', {'a': 2});
