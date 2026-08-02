import 'dart:async';
import 'dart:convert';

import 'package:web_socket_channel/web_socket_channel.dart';

import 'transport.dart';
import 'types.dart';

/// Live changes over one shared WebSocket.
///
/// One socket for the whole client, whatever the number of subscriptions: the
/// realtime endpoint is per deployment and the project is chosen *inside* the
/// connection by the auth frame, so the ref never appears in the path.
///
/// The server applies read rules to the stream as it does to a query, so a
/// subscriber only ever receives rows it would have been allowed to fetch — and a
/// delete it may not see is dropped rather than leaking an id.
class Realtime {
  Realtime(this._transport, this._wsUrl);

  final Transport _transport;
  final String _wsUrl;

  WebSocketChannel? _socket;
  bool _ready = false;
  int _nextId = 1;
  bool _disposed = false;

  /// Backoff for re-dialling, from half a second to fifteen — the same shape the
  /// JavaScript client uses. On a phone the socket dies routinely: backgrounding,
  /// a lost signal, Wi-Fi to cellular. Without this the subscriptions simply
  /// stopped and nothing said so.
  static const _retryFloor = Duration(milliseconds: 500);
  static const _retryCeiling = Duration(seconds: 15);
  Duration _retry = _retryFloor;
  Timer? _retryTimer;

  /// Commands are answered in order on a connection, so a plain FIFO matches an
  /// ack to the command that caused it.
  final List<void Function(bool ok, String? error)> _acks = [];
  final Map<String, _Subscription> _subs = {};

  /// Subscribe to a collection's changes.
  ///
  /// [query] is a server-side equality filter: only changes whose document
  /// matches are sent.
  Future<RealtimeSubscription> subscribe(
    String collection,
    void Function(ChangeEvent) onChange, {
    Map<String, dynamic>? query,
    void Function(String message)? onError,
  }) async {
    final id = '${_nextId++}';
    final sub = _Subscription(collection, onChange, query, onError);
    _subs[id] = sub;
    await _ensureSocket();
    _send({
      'cmd': 'subscribe',
      'id': id,
      'collection': collection,
      if (query != null) 'query': query,
      if (_transport.ref != null) 'db': _transport.ref,
    }, (ok, error) {
      if (!ok) {
        sub.active = false;
        onError?.call(error ?? 'subscribe failed');
      }
    });
    return RealtimeSubscription._(() {
      _subs.remove(id);
      if (_ready) _send({'cmd': 'unsubscribe', 'id': id}, null);
    });
  }

  /// Drop the connection so it reopens with the current identity. Called when the
  /// session changes: a socket authenticated as the previous reader would keep
  /// streaming rows under the old rules.
  void reset() {
    final socket = _socket;
    _socket = null;
    _ready = false;
    _retry = _retryFloor;
    _retryTimer?.cancel();
    _retryTimer = null;
    _acks.clear();
    socket?.sink.close();
    for (final sub in _subs.values) {
      sub.active = true;
    }
    if (_subs.isNotEmpty) unawaited(_ensureSocket());
  }

  Future<void> _ensureSocket() async {
    if (_socket != null) return;
    final socket = WebSocketChannel.connect(Uri.parse(_wsUrl));
    _socket = socket;
    socket.stream.listen(
      _onMessage,
      onDone: () {
        if (_socket != socket) return; // superseded by a reset
        _socket = null;
        _ready = false;
        _scheduleReconnect();
      },
      onError: (Object e) {
        if (_socket != socket) return;
        for (final sub in _subs.values) {
          sub.onError?.call('realtime connection failed: $e');
        }
        _socket = null;
        _ready = false;
        _scheduleReconnect();
      },
      cancelOnError: true,
    );
    await socket.ready;
    // Connected: the next drop starts from the floor again, so a long-lived
    // connection does not inherit the backoff of an outage hours ago.
    _retry = _retryFloor;

    // Authenticate first: the frame carries the project, which pins the
    // connection to it — a socket cannot then be pointed at another database.
    final completer = Completer<void>();
    _sendRaw(socket, {
      'cmd': 'auth',
      'token': _transport.currentToken,
      if (_transport.ref != null) 'db': _transport.ref,
    }, (ok, error) {
      _ready = ok;
      if (!ok) {
        for (final sub in _subs.values) {
          sub.onError?.call(error ?? 'realtime auth failed');
        }
      }
      if (!completer.isCompleted) completer.complete();
    });
    await completer.future;

    // Re-arm anything subscribed before the socket existed.
    for (final entry in _subs.entries) {
      _send({
        'cmd': 'subscribe',
        'id': entry.key,
        'collection': entry.value.collection,
        if (entry.value.query != null) 'query': entry.value.query,
        if (_transport.ref != null) 'db': _transport.ref,
      }, null);
    }
  }

  /// Re-dial after a drop, but only while something is subscribed — an idle
  /// client should not hold a socket open, or keep waking the radio to reopen one.
  void _scheduleReconnect() {
    if (_disposed || _subs.isEmpty || _retryTimer != null) return;
    final delay = _retry;
    _retry = Duration(
      milliseconds: (_retry.inMilliseconds * 2).clamp(0, _retryCeiling.inMilliseconds),
    );
    _retryTimer = Timer(delay, () {
      _retryTimer = null;
      if (_disposed || _socket != null || _subs.isEmpty) return;
      unawaited(_ensureSocket().catchError((Object _) {
        // Still down: the onError/onDone above schedules the next attempt.
      }));
    });
  }

  void _send(Map<String, dynamic> frame, void Function(bool, String?)? ack) {
    final socket = _socket;
    if (socket == null) return;
    _sendRaw(socket, frame, ack);
  }

  void _sendRaw(
      WebSocketChannel socket, Map<String, dynamic> frame, void Function(bool, String?)? ack) {
    _acks.add(ack ?? (_, __) {});
    socket.sink.add(jsonEncode(frame));
  }

  void _onMessage(dynamic raw) {
    final decoded = jsonDecode(raw as String);
    if (decoded is! Map) return;
    final msg = decoded.cast<String, dynamic>();

    if (msg['event'] == 'change') {
      final sub = _subs['${msg['subscription']}'];
      if (sub == null || !sub.active) return;
      sub.onChange(ChangeEvent(
        op: (msg['op'] ?? '').toString(),
        collection: (msg['collection'] ?? '').toString(),
        docId: msg['doc_id'],
        doc: (msg['doc'] as Map?)?.cast<String, dynamic>(),
      ));
      return;
    }

    if (_acks.isEmpty) return;
    final handler = _acks.removeAt(0);
    handler(msg['ok'] == true, msg['error'] as String?);
  }

  void dispose() {
    _disposed = true;
    _retryTimer?.cancel();
    _retryTimer = null;
    _subs.clear();
    _socket?.sink.close();
    _socket = null;
  }
}

class _Subscription {
  _Subscription(this.collection, this.onChange, this.query, this.onError);

  final String collection;
  final void Function(ChangeEvent) onChange;
  final Map<String, dynamic>? query;
  final void Function(String)? onError;
  bool active = true;
}

/// Cancel with [unsubscribe].
class RealtimeSubscription {
  RealtimeSubscription._(this._cancel);

  final void Function() _cancel;

  void unsubscribe() => _cancel();
}
