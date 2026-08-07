// OxiDB embedded demo: a 2,000,000-document collection living INSIDE the
// app, queried live, with the app's resident memory on screen — the
// disk-first story made visible: documents stay in an mmap'd file, RSS
// stays flat no matter how many records exist.

import 'dart:async';
import 'dart:io';
import 'dart:math';

import 'package:flutter/material.dart';
import 'package:oxidb_embedded/oxidb_embedded.dart';
import 'package:path_provider/path_provider.dart';

const int kTarget = 2000000;
const int kBatch = 5000;
const cities = [
  'Istanbul', 'Ankara', 'Izmir', 'Bursa', 'Antalya',
  'Adana', 'Konya', 'Gaziantep', 'Mersin', 'Kayseri',
];

void main() {
  runApp(const DemoApp());
}

class DemoApp extends StatelessWidget {
  const DemoApp({super.key});
  @override
  Widget build(BuildContext context) => MaterialApp(
        title: 'OxiDB 2M Demo',
        theme: ThemeData(colorSchemeSeed: Colors.deepOrange, useMaterial3: true),
        home: const HomePage(),
      );
}

class HomePage extends StatefulWidget {
  const HomePage({super.key});
  @override
  State<HomePage> createState() => _HomePageState();
}

class _HomePageState extends State<HomePage> {
  OxiDb? db;
  int total = 0;
  bool seeding = false;
  double seedProgress = 0;
  String seedRate = '';

  int rssMb = 0;
  int peakRssMb = 0;
  int anonMb = 0;
  int fileMb = 0;
  double dbSizeMb = 0;
  String dbPath = '';

  String queryResult = 'Henüz sorgu yok';
  int queryMs = 0;
  List<Map<String, dynamic>> rows = const [];

  final rand = Random(42);
  Timer? memTimer;
  final searchCtrl = TextEditingController();

  @override
  void initState() {
    super.initState();
    var tick = 0;
    memTimer = Timer.periodic(const Duration(milliseconds: 500), (_) {
      final rss = ProcessInfo.currentRss ~/ (1024 * 1024);
      // The honest split: RssAnon is the binding heap; RssFile is mmap'd
      // file pages (the database file the last scan touched) — clean,
      // evictable page cache the kernel reclaims under pressure. Without
      // this split, "RSS ≈ database size" reads as a leak when it is not.
      var anon = 0, file = 0;
      try {
        for (final line in File('/proc/self/status').readAsLinesSync()) {
          if (line.startsWith('RssAnon:')) {
            anon = int.parse(line.replaceAll(RegExp(r'[^0-9]'), '')) ~/ 1024;
          } else if (line.startsWith('RssFile:')) {
            file = int.parse(line.replaceAll(RegExp(r'[^0-9]'), '')) ~/ 1024;
          }
        }
      } catch (_) {}
      // The on-disk size every 2 s — a directory walk, not free.
      if (tick % 4 == 0) _updateDbSize();
      tick++;
      setState(() {
        rssMb = rss;
        anonMb = anon;
        fileMb = file;
        if (rss > peakRssMb) peakRssMb = rss;
      });
    });
    _openDb();
  }

  @override
  void dispose() {
    memTimer?.cancel();
    searchCtrl.dispose();
    db?.close();
    super.dispose();
  }

  Future<void> _openDb() async {
    final dir = await getApplicationDocumentsDirectory();
    dbPath = '${dir.path}/oxidb_2m';
    final d = OxiDb.open(dbPath);
    _seedNotes(d);
    // One-shot: the on-disk layout, into logcat (flutter's print lands
    // there) — which file actually holds the bytes.
    try {
      final files = Directory(dbPath)
          .listSync(recursive: true)
          .whereType<File>()
          .map((f) => MapEntry(f.path.replaceFirst('$dbPath/', ''), f.lengthSync()))
          .toList()
        ..sort((a, b) => b.value.compareTo(a.value));
      for (final e in files.take(8)) {
        // ignore: avoid_print
        print('OXIDB-FILE ${(e.value / 1048576).toStringAsFixed(1)} MB  ${e.key}');
      }
    } catch (_) {}
    setState(() {
      db = d;
      total = d.count('people');
    });
    if (total < kTarget) {
      unawaited(_seed());
    }
  }

  /// Total bytes under the database directory — what 2M records actually
  /// cost on flash.
  void _updateDbSize() {
    if (dbPath.isEmpty) return;
    var bytes = 0;
    try {
      for (final f in Directory(dbPath).listSync(recursive: true)) {
        if (f is File) bytes += f.lengthSync();
      }
    } catch (_) {}
    dbSizeMb = bytes / (1024 * 1024);
  }

  /// A small notes collection with a BM25 text index — the FTS half of
  /// the demo. Seeded once.
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

  void _searchNotes() {
    final d = db;
    final q = searchCtrl.text.trim();
    if (d == null || q.isEmpty) return;
    final sw = Stopwatch()..start();
    final hits = d.textSearch('notes', q);
    sw.stop();
    setState(() {
      rows = hits
          .map((h) => <String, dynamic>{
                'name': h['text'],
                'city': 'skor ${(h['_score'] as num?)?.toStringAsFixed(2)}',
                'age': '-',
                'score': '-',
                '_id': h['_id'],
              })
          .toList();
      queryResult = 'FTS "$q" → ${hits.length} sonuç, ${sw.elapsedMilliseconds} ms (BM25)';
    });
  }

  /// Seed up to 2M documents in batches, yielding to the UI between
  /// batches so the memory ticker and progress bar stay live.
  Future<void> _seed() async {
    final d = db!;
    setState(() => seeding = true);
    // Index BEFORE the data: writes maintain it incrementally.
    d.createIndex('people', 'city');
    d.createIndex('people', 'age');
    final sw = Stopwatch()..start();
    var done = total;
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
      final rate = done - total == 0
          ? 0
          : ((done - total) * 1000 / max(sw.elapsedMilliseconds, 1)).round();
      setState(() {
        seedProgress = done / kTarget;
        seedRate = '$rate doc/s';
        total = done;
      });
      // Let the frame render.
      await Future<void>.delayed(Duration.zero);
    }
    setState(() => seeding = false);
  }

  void _run(String label, List<Map<String, dynamic>> Function(OxiDb) q) {
    final d = db;
    if (d == null) return;
    final sw = Stopwatch()..start();
    final out = q(d);
    sw.stop();
    setState(() {
      rows = out.take(20).toList();
      queryMs = sw.elapsedMilliseconds;
      queryResult = '$label → ${out.length} satır, ${sw.elapsedMilliseconds} ms';
    });
  }

  void _insertOne() {
    final d = db;
    if (d == null) return;
    final sw = Stopwatch()..start();
    final id = d.insert('people', {
      'name': 'yeni_${DateTime.now().millisecondsSinceEpoch}',
      'city': cities[rand.nextInt(cities.length)],
      'age': 18 + rand.nextInt(62),
      'score': rand.nextInt(1000),
    });
    sw.stop();
    setState(() {
      total = d.count('people');
      queryResult = 'insert → id $id, ${sw.elapsedMilliseconds} ms (WAL-durable)';
      rows = const [];
    });
  }

  @override
  Widget build(BuildContext context) {
    final fmt = NumberFormatTr();
    return Scaffold(
      appBar: AppBar(title: const Text('OxiDB — 2M kayıt, cihaz içinde')),
      body: Padding(
        padding: const EdgeInsets.all(12),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            Card(
              child: Padding(
                padding: const EdgeInsets.all(12),
                child: Row(
                  children: [
                    Expanded(
                      child: Column(
                          crossAxisAlignment: CrossAxisAlignment.start,
                          children: [
                            Text('Kayıt: ${fmt.f(total)}',
                                style: Theme.of(context).textTheme.titleMedium),
                            const Text('oxidb_embedded (lite, disk-first)',
                                style: TextStyle(fontSize: 12)),
                            Text('Disk: ${dbSizeMb.toStringAsFixed(1)} MB',
                                style: Theme.of(context).textTheme.titleMedium),
                          ]),
                    ),
                    Column(crossAxisAlignment: CrossAxisAlignment.end, children: [
                      Text('RSS: $rssMb MB',
                          style: Theme.of(context)
                              .textTheme
                              .titleMedium
                              ?.copyWith(
                                  color: Colors.deepOrange,
                                  fontWeight: FontWeight.bold)),
                      Text('heap $anonMb + dosya $fileMb',
                          style: const TextStyle(fontSize: 12)),
                      Text('tepe: $peakRssMb MB',
                          style: const TextStyle(fontSize: 12)),
                    ]),
                  ],
                ),
              ),
            ),
            if (seeding) ...[
              const SizedBox(height: 8),
              LinearProgressIndicator(value: seedProgress),
              Padding(
                padding: const EdgeInsets.all(4),
                child: Text(
                    'Tohumlanıyor: ${fmt.f(total)} / ${fmt.f(kTarget)}  ($seedRate)'),
              ),
            ],
            const SizedBox(height: 8),
            Wrap(spacing: 8, runSpacing: 8, children: [
              FilledButton(
                onPressed: seeding || db == null
                    ? null
                    : () => _run(
                        'city=Izmir limit 20',
                        (d) => d.find('people',
                            query: {'city': 'Izmir'}, limit: 20)),
                child: const Text('Izmir (index)'),
              ),
              FilledButton(
                onPressed: seeding || db == null
                    ? null
                    : () => _run(
                        'city=Ankara ∧ age≥60',
                        (d) => d.find('people',
                            query: {
                              'city': 'Ankara',
                              'age': {r'$gte': 60}
                            },
                            limit: 20)),
                child: const Text('Ankara ∧ yaş≥60'),
              ),
              FilledButton.tonal(
                onPressed: seeding || db == null
                    ? null
                    : () {
                        final d = db!;
                        final sw = Stopwatch()..start();
                        final n = d.count('people', query: {'city': 'Istanbul'});
                        sw.stop();
                        setState(() {
                          queryMs = sw.elapsedMilliseconds;
                          queryResult =
                              'count(city=Istanbul) → ${fmt.f(n)}, ${sw.elapsedMilliseconds} ms (index-only)';
                          rows = const [];
                        });
                      },
                child: const Text('İstanbul say'),
              ),
              FilledButton.icon(
                onPressed: seeding || db == null ? null : _insertOne,
                icon: const Icon(Icons.add),
                label: const Text('Insert'),
              ),
            ]),
            const SizedBox(height: 8),
            Row(children: [
              Expanded(
                child: TextField(
                  controller: searchCtrl,
                  decoration: const InputDecoration(
                    isDense: true,
                    border: OutlineInputBorder(),
                    hintText: 'Not ara (FTS/BM25)… ör: süt',
                  ),
                  onSubmitted: (_) => _searchNotes(),
                ),
              ),
              const SizedBox(width: 8),
              FilledButton(
                onPressed: seeding || db == null ? null : _searchNotes,
                child: const Text('Ara'),
              ),
            ]),
            const SizedBox(height: 8),
            Text(queryResult, style: Theme.of(context).textTheme.bodyLarge),
            const Divider(),
            Expanded(
              child: ListView.builder(
                itemCount: rows.length,
                itemBuilder: (c, i) {
                  final r = rows[i];
                  return ListTile(
                    dense: true,
                    title: Text('${r['name']}  —  ${r['city']}'),
                    subtitle: Text('yaş ${r['age']}, skor ${r['score']}'),
                    trailing: Text('#${r['_id']}'),
                  );
                },
              ),
            ),
          ],
        ),
      ),
    );
  }
}

/// 2000000 → "2.000.000" without pulling intl in.
class NumberFormatTr {
  String f(int n) {
    final s = n.toString();
    final out = StringBuffer();
    for (var i = 0; i < s.length; i++) {
      if (i > 0 && (s.length - i) % 3 == 0) out.write('.');
      out.write(s[i]);
    }
    return out.toString();
  }
}
