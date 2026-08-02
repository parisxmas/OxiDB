// A tour of the client, as a plain Dart program.
//
//   dart run example/main.dart
//
// Reads OXIBASE_URL / OXIBASE_KEY / OXIBASE_REF from the environment.
import 'dart:io';

import 'package:oxibase/oxibase.dart';

Future<void> main() async {
  final env = Platform.environment;
  final client = OxibaseClient(
    url: env['OXIBASE_URL'] ?? 'http://127.0.0.1:8087',
    key: env['OXIBASE_KEY'] ?? '',
    ref: env['OXIBASE_REF'],
    authUrl: env['OXIBASE_AUTH_URL'] ?? env['OXIBASE_URL'],
  );

  // Persist on every change, not only at sign-in: refresh tokens rotate.
  client.auth.onAuthStateChange.listen((change) {
    final (event, session) = change;
    stdout.writeln('auth: $event (session: ${session == null ? 'none' : 'held'})');
  });

  final posts = await client
      .from('posts')
      .select('ts,handle,body')
      .isNull('reply_to')
      .order('ts', ascending: false)
      .limit(5)
      .get();
  stdout.writeln('${posts.length} recent posts');
  for (final p in posts) {
    stdout.writeln('  @${p['handle']}: ${p['body']}');
  }

  final sub = await client.subscribe('posts', (event) {
    stdout.writeln('live: ${event.op} ${event.doc?['body'] ?? ''}');
  }, onError: stdout.writeln);

  await Future<void>.delayed(const Duration(seconds: 5));
  sub.unsubscribe();
  client.dispose();
}
