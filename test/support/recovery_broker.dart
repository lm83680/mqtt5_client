import 'dart:async';
import 'dart:convert';
import 'dart:io';

/// Loopback MQTT 5 broker for socket and acknowledgement fault injection.
final class ScriptedBroker {
  ScriptedBroker._(this.server);

  final ServerSocket server;
  final List<BrokerPeer> peers = [];
  final List<BrokerPublish> publications = [];
  final List<(BrokerPeer, String)> subscriptions = [];
  void Function(BrokerPeer)? onConnect;
  void Function(BrokerPeer, int, String)? onSubscribe;
  void Function(BrokerPeer, BrokerPublish)? onPublish;
  bool answerPing = true;
  bool resetOnPublish = false;
  final List<Timer> _timers = [];
  final List<Socket> _rawSockets = [];
  bool _secure = false;

  void after(Duration delay, void Function() action) =>
      _timers.add(Timer(delay, action));
  int Function(BrokerPeer)? pubcompReason;
  Duration pubcompDelay = Duration.zero;

  String get url => '${_secure ? 'mqtts' : 'mqtt'}://127.0.0.1:${server.port}';

  static Future<ScriptedBroker> start({
    SecurityContext? securityContext,
    Duration handshakeDelay = Duration.zero,
  }) async {
    final broker = ScriptedBroker._(
      await ServerSocket.bind(InternetAddress.loopbackIPv4, 0),
    );
    broker._secure = securityContext != null;
    broker.server.listen((socket) async {
      broker._rawSockets.add(socket);
      try {
        if (handshakeDelay > Duration.zero) {
          await Future<void>.delayed(handshakeDelay);
        }
        final connected = securityContext == null
            ? socket
            : await SecureSocket.secureServer(socket, securityContext);
        final peer = BrokerPeer(connected, broker, broker.peers.length);
        broker.peers.add(peer);
      } catch (_) {
        socket.destroy();
      }
    });
    return broker;
  }

  Future<void> close() async {
    for (final timer in _timers) {
      timer.cancel();
    }
    await server.close();
    for (final socket in _rawSockets) {
      socket.destroy();
    }
    for (final peer in peers) {
      peer.socket.destroy();
    }
  }
}

final class BrokerPeer {
  BrokerPeer(this.socket, this.broker, this.index) {
    unawaited(
      socket.done.then<void>(
        (_) {},
        onError: (Object error, StackTrace stackTrace) {},
      ),
    );
    socket.listen(
      _receive,
      onError: (Object _) {},
      onDone: () {
        closed = true;
        socket.destroy();
      },
    );
  }

  final Socket socket;
  final ScriptedBroker broker;
  final int index;
  final List<int> _buffer = [];
  final Set<String> topics = {};
  final List<int> pubrelIds = [];
  bool closed = false;
  int connectPackets = 0;
  int pings = 0;

  void send(List<int> bytes) => socket.add(bytes);
  void accept() => send([0x20, 3, 0, 0, 0]);
  void disconnect() => send([0xe0, 2, 0x8e, 0]);
  void suback(int id, {int reason = 1}) =>
      send([0x90, 4, id >> 8, id & 255, 0, reason]);
  void acknowledge(BrokerPublish publication) {
    if (publication.qos == 0) return;
    send([
      publication.qos == 1 ? 0x40 : 0x50,
      4,
      publication.id >> 8,
      publication.id & 255,
      0,
      0,
    ]);
  }

  void publish(String topic, String payload) {
    final topicBytes = utf8.encode(topic);
    final body = [
      topicBytes.length >> 8,
      topicBytes.length & 255,
      ...topicBytes,
      0,
      ...utf8.encode(payload),
    ];
    send([0x30, ..._variableInteger(body.length), ...body]);
  }

  void _receive(List<int> bytes) {
    _buffer.addAll(bytes);
    while (_buffer.length >= 2) {
      if (broker.resetOnPublish && _buffer.first >> 4 == 3) {
        socket.destroy();
        return;
      }
      var cursor = 1;
      var size = 0;
      var multiplier = 1;
      var complete = false;
      while (cursor < _buffer.length) {
        final byte = _buffer[cursor++];
        size += (byte & 127) * multiplier;
        multiplier *= 128;
        if (byte & 128 == 0) {
          complete = true;
          break;
        }
      }
      if (!complete || _buffer.length < cursor + size) return;
      final header = _buffer.first;
      final body = _buffer.sublist(cursor, cursor + size);
      _buffer.removeRange(0, cursor + size);
      _packet(header, body);
    }
  }

  void _packet(int header, List<int> body) {
    switch (header >> 4) {
      case 1:
        connectPackets++;
        if (broker.onConnect case final handler?) {
          handler(this);
        } else {
          accept();
        }
      case 8:
        final id = body[0] * 256 + body[1];
        final cursor = _skipProperties(body, 2);
        final topicLength = body[cursor] * 256 + body[cursor + 1];
        final topic = utf8.decode(
          body.sublist(cursor + 2, cursor + 2 + topicLength),
        );
        topics.add(topic);
        broker.subscriptions.add((this, topic));
        if (broker.onSubscribe case final handler?) {
          handler(this, id, topic);
        } else {
          suback(id);
        }
      case 3:
        final length = body[0] * 256 + body[1];
        final topic = utf8.decode(body.sublist(2, 2 + length));
        final qos = (header >> 1) & 3;
        var cursor = 2 + length;
        final id = qos == 0 ? 0 : body[cursor] * 256 + body[cursor + 1];
        if (qos > 0) cursor += 2;
        cursor = _skipProperties(body, cursor);
        final publication = BrokerPublish(
          topic,
          utf8.decode(body.sublist(cursor)),
          id,
          qos,
        );
        broker.publications.add(publication);
        if (broker.onPublish case final handler?) {
          handler(this, publication);
        } else {
          acknowledge(publication);
        }
      case 6:
        pubrelIds.add(body[0] * 256 + body[1]);
        final packet = [
          0x70,
          4,
          body[0],
          body[1],
          broker.pubcompReason?.call(this) ?? 0,
          0,
        ];
        broker.after(broker.pubcompDelay, () => send(packet));
      case 12:
        pings++;
        if (broker.answerPing) send([0xd0, 0]);
      case 14:
        socket.destroy();
    }
  }

  static int _skipProperties(List<int> body, int cursor) {
    var length = 0;
    var multiplier = 1;
    while (true) {
      final byte = body[cursor++];
      length += (byte & 127) * multiplier;
      multiplier *= 128;
      if (byte & 128 == 0) return cursor + length;
    }
  }

  static List<int> _variableInteger(int value) {
    final result = <int>[];
    do {
      var byte = value % 128;
      value ~/= 128;
      if (value > 0) byte |= 128;
      result.add(byte);
    } while (value > 0);
    return result;
  }
}

final class BrokerPublish {
  const BrokerPublish(this.topic, this.payload, this.id, this.qos);
  final String topic;
  final String payload;
  final int id;
  final int qos;
}

Future<void> eventually(bool Function() predicate) async {
  final deadline = DateTime.now().add(const Duration(seconds: 5));
  while (!predicate()) {
    if (DateTime.now().isAfter(deadline)) {
      throw StateError('Condition did not become true');
    }
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
}
