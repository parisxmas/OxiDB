// The socket dies routinely on a phone — backgrounding, a lost signal, Wi-Fi to
// cellular. What matters is that subscriptions come back by themselves, so this
// test kills the connection from the server side and waits.
import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:oxibase/oxibase.dart';
import 'package:test/test.dart';

/// The smallest server that speaks enough of the protocol: acks `auth` and
/// `subscribe`, pushes a change on demand, and can hang up.
class FakeRealtime {
  FakeRealtime(this._server) {
    _server.listen((req) async {
      final socket = await WebSocketTransformer.upgrade(req);
      connections++;
      _sockets.add(socket);
      socket.listen((dynamic raw) {
        final msg = jsonDecode(raw as String) as Map<String, dynamic>;
        if (msg['cmd'] == 'auth' || msg['cmd'] == 'subscribe') {
          if (msg['cmd'] == 'subscribe') subscribeIds.add('${msg['id']}');
          _say(socket, {'ok': true});
        }
      }, onError: (Object _) {}, cancelOnError: true);
    });
  }

  final HttpServer _server;
  final List<WebSocket> _sockets = [];
  int connections = 0;
  final List<String> subscribeIds = [];

  int get port => _server.port;

  /// A frame can race the close the test just did; that is the harness, not the
  /// client, so it must not fail the test.
  void _say(WebSocket socket, Map<String, dynamic> frame) {
    if (socket.readyState != WebSocket.open) return;
    try {
      socket.add(jsonEncode(frame));
    } on StateError {
      /* closed between the check and the write */
    }
  }

  void pushChange(String subscriptionId, Map<String, dynamic> doc) {
    for (final s in List.of(_sockets)) {
      _say(s, {
        'event': 'change',
        'subscription': subscriptionId,
        'op': 'insert',
        'collection': 'posts',
        'doc': doc,
      });
    }
  }

  Future<void> dropConnections() async {
    for (final s in List.of(_sockets)) {
      await s.close();
    }
    _sockets.clear();
  }

  Future<void> close() async {
    await dropConnections();
    await _server.close(force: true);
  }

  static Future<FakeRealtime> start() async =>
      FakeRealtime(await HttpServer.bind(InternetAddress.loopbackIPv4, 0));
}

void main() {
  test('a dropped socket reconnects and the subscription still delivers', () async {
    final server = await FakeRealtime.start();
    final client = OxibaseClient(
      url: 'http://127.0.0.1:${server.port}',
      key: 'anon',
      realtimeUrl: 'ws://127.0.0.1:${server.port}/ws',
    );

    final received = <ChangeEvent>[];
    await client.subscribe('posts', received.add);
    await _until(() => server.subscribeIds.isNotEmpty);
    expect(server.connections, 1);

    server.pushChange(server.subscribeIds.first, {'body': 'before'});
    await _until(() => received.length == 1);

    // The network goes away.
    await server.dropConnections();

    // It should come back on its own, and re-arm the subscription.
    await _until(() => server.connections == 2, timeout: const Duration(seconds: 6));
    await _until(() => server.subscribeIds.length == 2, timeout: const Duration(seconds: 6));

    server.pushChange(server.subscribeIds.last, {'body': 'after'});
    await _until(() => received.length == 2, timeout: const Duration(seconds: 6));
    expect(received.last.doc?['body'], 'after',
        reason: 'events flow again without the app doing anything');

    client.dispose();
    await server.close();
  }, timeout: const Timeout(Duration(seconds: 30)));

  test('an idle client does not hold or re-dial a socket', () async {
    final server = await FakeRealtime.start();
    final client = OxibaseClient(
      url: 'http://127.0.0.1:${server.port}',
      key: 'anon',
      realtimeUrl: 'ws://127.0.0.1:${server.port}/ws',
    );

    final sub = await client.subscribe('posts', (_) {});
    await _until(() => server.connections == 1);
    sub.unsubscribe();

    await server.dropConnections();
    await Future<void>.delayed(const Duration(seconds: 2));
    expect(server.connections, 1, reason: 'nothing subscribed, so nothing to reconnect for');

    client.dispose();
    await server.close();
  }, timeout: const Timeout(Duration(seconds: 30)));
}

Future<void> _until(bool Function() done, {Duration timeout = const Duration(seconds: 3)}) async {
  final deadline = DateTime.now().add(timeout);
  while (!done()) {
    if (DateTime.now().isAfter(deadline)) fail('timed out waiting');
    await Future<void>.delayed(const Duration(milliseconds: 30));
  }
}
