// Opt-in integration test: talks to a running OxiDB/OxiBase.
//
//   OXIBASE_URL=http://127.0.0.1:8098 OXIBASE_KEY=<a JWT with role=admin> \
//     dart test test/live_test.dart
//
// Skipped entirely without those, so `dart test` stays hermetic.
import 'dart:convert';
import 'dart:io' show Platform;
import 'dart:typed_data';

import 'package:oxibase/oxibase.dart';
import 'package:test/test.dart';

void main() {
  final envUrl = Platform.environment['OXIBASE_URL'] ?? '';
  final envKey = Platform.environment['OXIBASE_KEY'] ?? '';

  if (envUrl.isEmpty || envKey.isEmpty) {
    test('live', () {}, skip: 'set OXIBASE_URL and OXIBASE_KEY to run');
    return;
  }

  late OxibaseClient client;
  final collection = 'dart_probe_${DateTime.now().millisecondsSinceEpoch}';

  setUpAll(() => client = OxibaseClient(url: envUrl, key: envKey));
  tearDownAll(() => client.dispose());

  test('documents: insert, filter, order, update, delete', () async {
    await client.from(collection).insert([
      {'owner': 'ada', 'n': 1, 'body': 'the storage engine, storage twice'},
      {'owner': 'ada', 'n': 2, 'body': 'a single mention of storage'},
      {'owner': 'grace', 'n': 3, 'body': 'avocados'},
    ]);

    final ada = await client
        .from(collection)
        .select()
        .eq('owner', 'ada')
        .order('n', ascending: false)
        .get();
    expect(ada.length, 2);
    expect(ada.first['n'], 2);

    await client.from(collection).eq('n', 1).update({'body': 'rewritten'});
    final one = await client.from(collection).select().eq('n', 1).maybeSingle();
    expect(one?['body'], 'rewritten');

    await client.from(collection).eq('owner', 'grace').delete();
    expect((await client.from(collection).select().get()).length, 2);
  });

  test('full-text search ranks, once an index exists', () async {
    // Its own collection: the document test above rewrites and deletes rows, and
    // a search asserting on that would be testing the order the tests ran in.
    final corpus = '${collection}_fts';
    await client.from(corpus).insert([
      {'body': 'the storage engine writes storage pages'},
      {'body': 'the planner picks an index for storage'},
      {'body': 'nothing about avocados here'},
    ]);
    await client.createTextIndex(corpus, ['body']);

    final hits = await client.textSearch(corpus, 'storage', limit: 5);
    expect(hits.length, 2, reason: 'only the matching documents');
    expect(hits.first['body'].toString(), contains('writes storage pages'),
        reason: 'the one saying it twice ranks first');
    expect(hits.first['_score'], isNotNull);

    final none = await client.textSearch(corpus, 'kangaroo');
    expect(none, isEmpty);
  });

  test('a duplicate on a unique index is a conflict, not a fault', () async {
    // What deduplicating by key looks like when it works: the second insert is
    // refused with 409, which an app can act on. It used to be a 500.
    final guard = '${collection}_guard';
    await client.from(guard).insert({'key': 'k1'});
    await expectLater(
      client.from(guard).insert({'key': 'k1'}),
      throwsA(isA<OxibaseException>().having((e) => e.isConflict, 'isConflict', isTrue)),
      skip: 'needs a unique index on `key`, created out of band',
    );
  }, skip: 'enable once the probe collection is provisioned with a unique index');

  test('storage: upload, download, search its text', () async {
    final bucket = 'dartprobe${DateTime.now().millisecondsSinceEpoch}';
    final b = client.storage.from(bucket);
    await b.upload('notes.txt', Uint8List.fromList(utf8.encode('retention policy, retention')),
        contentType: 'text/plain');
    final bytes = await b.download('notes.txt');
    expect(utf8.decode(bytes), contains('retention'));

    List<StorageSearchHit> hits = const [];
    for (var i = 0; i < 40 && hits.isEmpty; i++) {
      hits = await b.search('retention', limit: 5, highlight: true);
      if (hits.isEmpty) await Future<void>.delayed(const Duration(milliseconds: 250));
    }
    expect(hits.single.key, 'notes.txt');
    expect(hits.single.highlights?.first, contains('<mark>'));

    await b.remove('notes.txt');
    await client.storage.deleteBucket(bucket);
  });

  tearDownAll(() async {
    await client.from(collection).gte('n', 0).delete().catchError((_) => <Map<String, dynamic>>[]);
  });
}
