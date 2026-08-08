// OxiDB embedded demo: a 2,000,000-document collection living INSIDE the
// app, queried live, with the app's resident memory on screen — the
// disk-first story made visible: documents stay in an mmap'd file, RSS
// stays flat no matter how many records exist.
//
// The database runs in a background isolate (db_worker.dart): OxiDB calls
// are synchronous FFI, so keeping them off the UI isolate is what keeps
// the interface responsive even while the engine checkpoints in the
// background. The UI here only sends commands and renders replies.

import 'dart:async';
import 'dart:io';

import 'package:flutter/material.dart';
import 'package:path_provider/path_provider.dart';

import 'db_worker.dart';

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
  DbWorker? worker;
  int total = 0;
  bool seeding = false;
  double seedProgress = 0;
  String seedRate = '';

  int rssMb = 0;
  int peakRssMb = 0;
  int anonMb = 0;
  int fileMb = 0;
  double dbSizeMb = 0;

  String queryResult = 'Başlatılıyor…';
  List<Map<String, dynamic>> rows = const [];

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
      // Ask the worker for the on-disk size every 2 s (a directory walk —
      // also off the UI isolate).
      if (t.tick % 4 == 0) worker?.send({'op': 'dbsize'});
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
    super.dispose();
  }

  Future<void> _start() async {
    final dir = await getApplicationDocumentsDirectory();
    final w = await DbWorker.spawn('${dir.path}/oxidb_2m');
    w.events.listen(_onWorkerMessage);
    setState(() => worker = w);
  }

  void _onWorkerMessage(Map<String, Object?> m) {
    switch (m['type']) {
      case 'ready':
        setState(() {
          total = m['total'] as int;
          queryResult = total > 0 ? 'Hazır' : 'Tohumlama başlıyor…';
          if (total < kTarget) seeding = true;
        });
      case 'progress':
        setState(() {
          total = m['done'] as int;
          seedProgress = total / kTarget;
          seedRate = '${m['rate']} doc/s';
          seeding = true;
        });
      case 'seed_done':
        setState(() {
          total = m['total'] as int;
          seeding = false;
        });
      case 'result':
        setState(() {
          queryResult = m['label'] as String;
          rows = (m['rows'] as List).cast<Map<String, dynamic>>();
          if (m['total'] != null) total = m['total'] as int;
        });
      case 'dbsize':
        setState(() => dbSizeMb = m['mb'] as double);
    }
  }

  @override
  Widget build(BuildContext context) {
    final fmt = NumberFormatTr();
    final ready = worker != null && !seeding;
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
                onPressed:
                    ready ? () => worker!.send({'op': 'query', 'kind': 'izmir'}) : null,
                child: const Text('Izmir (index)'),
              ),
              FilledButton(
                onPressed:
                    ready ? () => worker!.send({'op': 'query', 'kind': 'ankara'}) : null,
                child: const Text('Ankara ∧ yaş≥60'),
              ),
              FilledButton.tonal(
                onPressed:
                    ready ? () => worker!.send({'op': 'query', 'kind': 'count'}) : null,
                child: const Text('İstanbul say'),
              ),
              FilledButton.icon(
                onPressed: ready ? () => worker!.send({'op': 'insert'}) : null,
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
                  onSubmitted: (q) => worker?.send({'op': 'fts', 'q': q}),
                ),
              ),
              const SizedBox(width: 8),
              FilledButton(
                onPressed: ready
                    ? () => worker!.send({'op': 'fts', 'q': searchCtrl.text})
                    : null,
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
