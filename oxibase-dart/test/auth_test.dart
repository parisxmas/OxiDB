import 'dart:async';
import 'dart:convert';

import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import 'package:oxibase/oxibase.dart';
import 'package:test/test.dart';

/// A stand-in for the server that behaves the way the real one does in the two
/// respects that matter here: an access token expires, and a refresh token is
/// **single-use** — presenting it revokes it and hands back a new one.
class FakeServer {
  FakeServer({this.refreshDelay = const Duration(milliseconds: 40)});

  final Duration refreshDelay;
  final Set<String> liveAccess = {'A1'};
  final Set<String> liveRefresh = {'R0'};
  int refreshCalls = 0;
  int dataCalls = 0;
  int _n = 1;

  http.Client get client => MockClient((req) async {
        if (req.url.path.endsWith('/auth/refresh')) {
          refreshCalls++;
          final presented = (jsonDecode(req.body) as Map)['refresh_token'];
          await Future<void>.delayed(refreshDelay);
          if (!liveRefresh.remove(presented)) {
            return http.Response(jsonEncode({'message': 'invalid refresh token'}), 401);
          }
          _n++;
          liveAccess.add('A$_n');
          liveRefresh.add('R$_n');
          return http.Response(jsonEncode({'token': 'A$_n', 'refresh_token': 'R$_n'}), 200);
        }
        dataCalls++;
        final bearer = (req.headers['Authorization'] ?? '').replaceFirst('Bearer ', '');
        if (!liveAccess.contains(bearer)) {
          return http.Response(jsonEncode({'message': 'expired'}), 401);
        }
        return http.Response(
            jsonEncode([
              {'id': 1}
            ]),
            200);
      });
}

OxibaseClient clientFor(FakeServer server) => OxibaseClient(
      url: 'https://data.test',
      key: 'anon-key',
      ref: 'proj',
      authUrl: 'https://cp.test',
      httpClient: server.client,
    );

void main() {
  test('concurrent 401s cause exactly one refresh, and every request survives', () async {
    // The bug this guards against: eight requests each noticing a 401 and each
    // POSTing the same single-use refresh token — one rotates it, seven are told
    // "invalid" while the session was perfectly good.
    final server = FakeServer();
    final client = clientFor(server);
    client.auth.setSession(const Session(accessToken: 'A0-expired', refreshToken: 'R0'));

    final results = await Future.wait(
      List.generate(8, (_) => client.from('posts').select().get()),
    );

    expect(results.every((rows) => rows.length == 1), isTrue);
    expect(server.refreshCalls, 1, reason: 'refresh must be single-flight');
    expect(client.auth.currentSession?.accessToken, 'A2');
    client.dispose();
  });

  test('every rotation is reported, so a stored session can keep up', () async {
    final server = FakeServer();
    final client = clientFor(server);
    final events = <(AuthChangeEvent, Session?)>[];
    final sub = client.auth.onAuthStateChange.listen(events.add);

    client.auth.setSession(const Session(accessToken: 'A0-expired', refreshToken: 'R0'));
    await Future<void>.delayed(Duration.zero);
    expect(events, isEmpty, reason: 'setSession must not echo back to the listener');

    await client.from('posts').select().get();
    await Future<void>.delayed(Duration.zero);

    expect(events.length, 1);
    expect(events.first.$1, AuthChangeEvent.tokenRefreshed);
    expect(events.first.$2?.refreshToken, 'R2', reason: 'the event carries the NEW refresh token');

    await sub.cancel();
    client.dispose();
  });

  test('the token handed out at sign-in is spent once it has been used', () async {
    // Why the callback exists at all: resuming with the sign-in copy fails, and
    // resuming with the rotated one works.
    final server = FakeServer();
    final first = clientFor(server);
    const atSignIn = Session(accessToken: 'A0-expired', refreshToken: 'R0');
    first.auth.setSession(atSignIn);
    await first.from('posts').select().get(); // rotates R0 -> R2
    final rotated = first.auth.currentSession!;
    first.dispose();

    final stale = clientFor(server);
    stale.auth.setSession(atSignIn);
    await expectLater(
      stale.from('posts').select().get(),
      throwsA(isA<OxibaseException>().having((e) => e.statusCode, 'status', 401)),
    );
    stale.dispose();

    final resumed = clientFor(server);
    resumed.auth.setSession(rotated);
    server.liveAccess.clear(); // the stored access token has expired too
    final rows = await resumed.from('posts').select().get();
    expect(rows.length, 1);
    resumed.dispose();
  });

  test('signOut reverts to the project key and reports it', () async {
    final server = FakeServer();
    final client = clientFor(server);
    final events = <(AuthChangeEvent, Session?)>[];
    final sub = client.auth.onAuthStateChange.listen(events.add);

    client.auth.setSession(const Session(accessToken: 'A1', refreshToken: 'R0'));
    client.auth.signOut();
    await Future<void>.delayed(Duration.zero);

    expect(client.auth.currentSession, isNull);
    expect(events.single.$1, AuthChangeEvent.signedOut);
    expect(events.single.$2, isNull);
    await sub.cancel();
    client.dispose();
  });

  test('a 429 arrives as a rate-limit error with its Retry-After', () async {
    final client = OxibaseClient(
      url: 'https://data.test',
      key: 'anon',
      ref: 'proj',
      httpClient: MockClient((_) async => http.Response(
            jsonEncode({'error': 'slow down'}),
            429,
            headers: {'retry-after': '12'},
          )),
    );
    await expectLater(
      client.from('posts').select().get(),
      throwsA(isA<OxibaseException>()
          .having((e) => e.isRateLimited, 'isRateLimited', isTrue)
          .having((e) => e.retryAfter, 'retryAfter', 12)),
    );
    client.dispose();
  });
}
