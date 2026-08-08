// OxiDB embedded demo: a 2,000,000-document collection living INSIDE the
// app, queried live, with the app's resident memory on screen — the
// disk-first story made visible: documents stay in an mmap'd file, RSS
// stays flat no matter how many records exist.
//
// The database is opened with OxiDb.background(), which runs the whole
// engine on a worker isolate. OxiDB's FFI is synchronous, so a call made
// on the UI isolate that contends with the background checkpoint can jank
// a frame or ANR on a large database — background() makes every call a
// Future the UI simply awaits, so the interface never blocks.

import 'dart:async';
import 'dart:io';
import 'dart:isolate';
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

void main() => runApp(const DemoApp());

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
  OxiDbAsync? db;
  String dbPath = '';
  int total = 0;
  bool seeding = false;
  bool busy = false;
  double seedProgress = 0;
  String seedRate = '';

  int rssMb = 0;
  int peakRssMb = 0;
  int anonMb = 0;
  int fileMb = 0;
  double dbSizeMb = 0;

  String queryResult = 'Başlatılıyor…';
  List<Map<String, dynamic>> rows = const [];

  final rand = Random(42);
  Timer? memTimer;
  final searchCtrl = TextEditingController();

  @override
  void initState() {
    super.initState();
    memTimer = Timer.periodic(const Duration(milliseconds: 500), (t) {
      final rss = ProcessInfo.currentRss ~/ (1024 * 1024);
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
      if (t.tick % 4 == 0) _updateDbSize();
      setState(() {
        rssMb = rss;
        anonMb = anon;
        fileMb = file;
        if (rss > peakRssMb) peakRssMb = rss;
      });
    });
    _start();
  }

  @override
  void dispose() {
    memTimer?.cancel();
    searchCtrl.dispose();
    db?.close();
    super.dispose();
  }

  Future<void> _start() async {
    final dir = await getApplicationDocumentsDirectory();
    dbPath = '${dir.path}/oxidb_2m';
    final d = await OxiDb.background(dbPath);
    await _seedNotes(d);
    final n = await d.count('people');
    setState(() {
      db = d;
      total = n;
      queryResult = n > 0 ? 'Hazır' : 'Tohumlama başlıyor…';
    });
    if (n < kTarget) unawaited(_seed(d));
  }

  Future<void> _seedNotes(OxiDbAsync d) async {
    if (await d.count('notes') > 0) return;
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
      await d.insert('notes', {'text': t});
    }
    await d.createTextIndex('notes', ['text']);
  }

  Future<void> _seed(OxiDbAsync d) async {
    setState(() => seeding = true);
    await d.createIndex('people', 'city');
    await d.createIndex('people', 'age');
    final sw = Stopwatch()..start();
    var done = total;
    final start = done;
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
      // Each await hands the frame back to the UI — the work runs on the
      // worker isolate, so seeding 2M rows never freezes the interface.
      await d.insertMany('people', batch);
      done += n;
      final rate = ((done - start) * 1000 / max(sw.elapsedMilliseconds, 1)).round();
      setState(() {
        total = done;
        seedProgress = done / kTarget;
        seedRate = '$rate doc/s';
      });
    }
    setState(() => seeding = false);
  }

  /// The on-disk size — a directory walk, off the UI isolate.
  Future<void> _updateDbSize() async {
    if (dbPath.isEmpty) return;
    final path = dbPath;
    final mb = await Isolate.run(() {
      var bytes = 0;
      try {
        for (final f in Directory(path).listSync(recursive: true)) {
          if (f is File) bytes += f.lengthSync();
        }
      } catch (_) {}
      return bytes / (1024 * 1024);
    });
    if (mounted) setState(() => dbSizeMb = mb);
  }

  Future<void> _run(
      String label, Future<List<Map<String, dynamic>>> Function(OxiDbAsync) q) async {
    final d = db;
    if (d == null || busy) return;
    setState(() => busy = true);
    final sw = Stopwatch()..start();
    final out = await q(d);
    setState(() {
      busy = false;
      rows = out.take(20).toList();
      queryResult = '$label → ${out.length} satır, ${sw.elapsedMilliseconds} ms';
    });
  }

  Future<void> _insertOne() async {
    final d = db;
    if (d == null || busy) return;
    setState(() => busy = true);
    final sw = Stopwatch()..start();
    final id = await d.insert('people', {
      'name': 'yeni_${DateTime.now().millisecondsSinceEpoch}',
      'city': cities[rand.nextInt(cities.length)],
      'age': 18 + rand.nextInt(62),
      'score': rand.nextInt(1000),
    });
    final n = await d.count('people');
    setState(() {
      busy = false;
      total = n;
      queryResult = 'insert → id $id, ${sw.elapsedMilliseconds} ms (WAL-durable)';
      rows = const [];
    });
  }

  Future<void> _search() async {
    final d = db;
    final q = searchCtrl.text.trim();
    if (d == null || busy || q.isEmpty) return;
    setState(() => busy = true);
    final sw = Stopwatch()..start();
    final hits = await d.textSearch('notes', q);
    setState(() {
      busy = false;
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

  @override
  Widget build(BuildContext context) {
    final fmt = NumberFormatTr();
    final ready = db != null && !seeding && !busy;
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
                          style: Theme.of(context).textTheme.titleMedium?.copyWith(
                              color: Colors.deepOrange, fontWeight: FontWeight.bold)),
                      Text('heap $anonMb + dosya $fileMb',
                          style: const TextStyle(fontSize: 12)),
                      Text('tepe: $peakRssMb MB', style: const TextStyle(fontSize: 12)),
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
                onPressed: ready
                    ? () => _run('city=Izmir limit 20',
                        (d) => d.find('people', query: {'city': 'Izmir'}, limit: 20))
                    : null,
                child: const Text('Izmir (index)'),
              ),
              FilledButton(
                onPressed: ready
                    ? () => _run(
                        'city=Ankara ∧ age≥60',
                        (d) => d.find('people',
                            query: {
                              'city': 'Ankara',
                              'age': {r'$gte': 60}
                            },
                            limit: 20))
                    : null,
                child: const Text('Ankara ∧ yaş≥60'),
              ),
              FilledButton.tonal(
                onPressed: ready
                    ? () async {
                        final d = db!;
                        setState(() => busy = true);
                        final sw = Stopwatch()..start();
                        final n = await d.count('people', query: {'city': 'Istanbul'});
                        setState(() {
                          busy = false;
                          queryResult =
                              'count(city=Istanbul) → ${fmt.f(n)}, ${sw.elapsedMilliseconds} ms (index-only)';
                          rows = const [];
                        });
                      }
                    : null,
                child: const Text('İstanbul say'),
              ),
              FilledButton.icon(
                onPressed: ready ? _insertOne : null,
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
                  onSubmitted: (_) => _search(),
                ),
              ),
              const SizedBox(width: 8),
              FilledButton(onPressed: ready ? _search : null, child: const Text('Ara')),
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
