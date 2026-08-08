// The database lives in its own isolate. Every OxiDB call is synchronous
// FFI; running it on the UI isolate means a call that contends with the
// background checkpoint (the engine rewriting its data file) blocks the
// frame — measured at 450 ms for a single insert during a fold, enough to
// ANR when the fold is large. Here the UI isolate only ever sends a
// command and awaits a message, so the interface never blocks.

import 'dart:async';
import 'dart:io';
import 'dart:isolate';
import 'dart:math';

import 'package:oxidb_embedded/oxidb_embedded.dart';

const int kTarget = 2000000;
const int kBatch = 5000;
const cities = [
  'Istanbul', 'Ankara', 'Izmir', 'Bursa', 'Antalya',
  'Adana', 'Konya', 'Gaziantep', 'Mersin', 'Kayseri',
];

/// Handle to the worker: spawn once, then `send` commands and listen to
/// [events] for `{type, ...}` replies.
class DbWorker {
  DbWorker._(this._toWorker, this.events);
  final SendPort _toWorker;

  /// Broadcast of worker → UI messages: progress ticks, query results,
  /// insert acks, and the initial `ready`.
  final Stream<Map<String, Object?>> events;

  static Future<DbWorker> spawn(String dbPath) async {
    final fromWorker = ReceivePort();
    await Isolate.spawn(_entry, fromWorker.sendPort);
    final broadcast = fromWorker.asBroadcastStream();
    // First message is the worker's command port.
    final toWorker = await broadcast.first as SendPort;
    final worker = DbWorker._(
      toWorker,
      broadcast
          .where((m) => m is Map)
          .map((m) => (m as Map).cast<String, Object?>()),
    );
    worker.send({'op': 'open', 'path': dbPath});
    return worker;
  }

  void send(Map<String, Object?> cmd) => _toWorker.send(cmd);
}

void _entry(SendPort toMain) {
  final commands = ReceivePort();
  toMain.send(commands.sendPort);
  OxiDb? db;
  String dbPath = '';
  final rand = Random(7);

  commands.listen((message) {
    final cmd = (message as Map).cast<String, Object?>();
    switch (cmd['op']) {
      case 'open':
        dbPath = cmd['path'] as String;
        final d = OxiDb.open(dbPath);
        _seedNotes(d);
        db = d;
        toMain.send({'type': 'ready', 'total': d.count('people')});
        if (d.count('people') < kTarget) {
          _seed(d, toMain);
        }

      case 'insert':
        final d = db;
        if (d == null) break;
        final sw = Stopwatch()..start();
        final id = d.insert('people', {
          'name': 'yeni_${DateTime.now().millisecondsSinceEpoch}',
          'city': cities[rand.nextInt(cities.length)],
          'age': 18 + rand.nextInt(62),
          'score': rand.nextInt(1000),
        });
        toMain.send({
          'type': 'result',
          'label': 'insert → id $id, ${sw.elapsedMilliseconds} ms (WAL-durable)',
          'ms': sw.elapsedMilliseconds,
          'total': d.count('people'),
          'rows': const <Map<String, Object?>>[],
        });

      case 'query':
        final d = db;
        if (d == null) break;
        final kind = cmd['kind'] as String;
        final sw = Stopwatch()..start();
        List<Map<String, dynamic>> rows;
        String label;
        switch (kind) {
          case 'izmir':
            rows = d.find('people', query: {'city': 'Izmir'}, limit: 20);
            label = 'city=Izmir limit 20';
          case 'ankara':
            rows = d.find('people',
                query: {
                  'city': 'Ankara',
                  'age': {r'$gte': 60}
                },
                limit: 20);
            label = 'city=Ankara ∧ age≥60';
          case 'count':
            final n = d.count('people', query: {'city': 'Istanbul'});
            toMain.send({
              'type': 'result',
              'label':
                  'count(city=Istanbul) → $n, ${sw.elapsedMilliseconds} ms (index-only)',
              'ms': sw.elapsedMilliseconds,
              'rows': const <Map<String, Object?>>[],
            });
            return;
          default:
            return;
        }
        toMain.send({
          'type': 'result',
          'label': '$label → ${rows.length} satır, ${sw.elapsedMilliseconds} ms',
          'ms': sw.elapsedMilliseconds,
          'rows': rows.take(20).toList(),
        });

      case 'fts':
        final d = db;
        if (d == null) break;
        final q = (cmd['q'] as String).trim();
        if (q.isEmpty) break;
        final sw = Stopwatch()..start();
        final hits = d.textSearch('notes', q);
        toMain.send({
          'type': 'result',
          'label': 'FTS "$q" → ${hits.length} sonuç, ${sw.elapsedMilliseconds} ms (BM25)',
          'ms': sw.elapsedMilliseconds,
          'rows': hits
              .map((h) => <String, Object?>{
                    'name': h['text'],
                    'city': 'skor ${(h['_score'] as num?)?.toStringAsFixed(2)}',
                    'age': '-',
                    'score': '-',
                    '_id': h['_id'],
                  })
              .toList(),
        });

      case 'dbsize':
        var bytes = 0;
        try {
          for (final f in Directory(dbPath).listSync(recursive: true)) {
            if (f is File) bytes += f.lengthSync();
          }
        } catch (_) {}
        toMain.send({'type': 'dbsize', 'mb': bytes / (1024 * 1024)});
    }
  });
}

void _seedNotes(OxiDb d) {
  if (d.count('notes') > 0) return;
  for (final t in const [
    'Pazartesi süt ve ekmek almayı unutma',
    'Ankara toplantısı için sunum hazırla',
    'Koşu antrenmanı: sahilde 5 km tempo',
    'Yeni telefon için ekran koruyucu sipariş et',
    'Annemi ara, doğum günü hediyesi konuş',
    'Sunum dosyasını ekibe mail at',
    'Süt kuzusuna masal kitabı al',
    'Salı akşamı basketbol maçı — salon 2',
  ]) {
    d.insert('notes', {'text': t});
  }
  d.createTextIndex('notes', ['text']);
}

void _seed(OxiDb d, SendPort toMain) {
  d.createIndex('people', 'city');
  d.createIndex('people', 'age');
  final sw = Stopwatch()..start();
  var done = d.count('people');
  final startCount = done;
  while (done < kTarget) {
    final n = min(kBatch, kTarget - done);
    final batch = List.generate(n, (i) {
      final id = done + i;
      return <String, Object?>{
        'name': 'user_$id',
        'city': cities[id % cities.length],
        'age': 18 + (id * 7) % 62,
        'score': (id * 13) % 1000,
      };
    });
    d.insertMany('people', batch);
    done += n;
    final rate = ((done - startCount) * 1000 / max(sw.elapsedMilliseconds, 1)).round();
    toMain.send({'type': 'progress', 'done': done, 'rate': rate});
  }
  toMain.send({'type': 'seed_done', 'total': done});
}
