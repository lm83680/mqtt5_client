@TestOn('vm')
library;

import 'dart:async';
import 'dart:io';

import 'package:mqtt5_client/mqtt5_client.dart';
import 'package:mqtt5_client/mqtt5_server_client.dart';
import 'package:test/test.dart';

import 'support/recovery_broker.dart';

class ObservedClient extends MqttServerClient {
  ObservedClient(int port)
    : super.withPort('127.0.0.1', 'recovery', port, maxConnectionAttempts: 1);

  MqttConnectionKeepAlive? get keepAliveForTest => keepAlive;
}

void main() {
  late ScriptedBroker broker;
  late MqttServerClient client;

  setUp(() async {
    broker = await ScriptedBroker.start();
    client = ObservedClient(broker.server.port)..socketTimeout = 5000;
    client.logging(on: false);
  });

  tearDown(() async {
    client.disconnect();
    await broker.close();
  });

  Future<void> tls({Duration handshakeDelay = Duration.zero}) async {
    client.disconnect();
    await broker.close();
    final context = SecurityContext()
      ..useCertificateChain('test/pem/localhost.cert')
      ..usePrivateKey('test/pem/localhost.key');
    broker = await ScriptedBroker.start(
      securityContext: context,
      handshakeDelay: handshakeDelay,
    );
    client = ObservedClient(broker.server.port)
      ..secure = true
      ..socketTimeout = 5000
      ..onBadCertificate = (_) => true;
  }

  int publish({MqttQos qos = MqttQos.exactlyOnce, String payload = 'command'}) {
    final builder = MqttPayloadBuilder()..addString(payload);
    return client.publishMessage('down/cmd', qos, builder.payload!);
  }

  for (final milliseconds in [1500, 4000]) {
    test(
      'socket timeout does not reduce CONNACK wait: ${milliseconds}ms',
      () async {
        broker.onConnect = (peer) =>
            broker.after(Duration(milliseconds: milliseconds), peer.accept);
        final status = await client.connect();
        expect(status?.state, MqttConnectionState.connected);
        expect(broker.peers, hasLength(1));
        expect(broker.peers.single.connectPackets, 1);
      },
    );
  }

  test('independent CONNACK timeout closes a failed socket', () async {
    client.connectAckTimeout = 100;
    broker.onConnect = (_) {};
    final clock = Stopwatch()..start();
    await expectLater(
      client.connect(),
      throwsA(isA<MqttNoConnectionException>()),
    );
    expect(clock.elapsedMilliseconds, lessThan(1000));
    await eventually(
      () => broker.peers.isNotEmpty && broker.peers.single.closed,
    );
    expect(broker.peers, hasLength(1));
  });

  test('initial retries retire each previous socket before success', () async {
    client =
        MqttServerClient.withPort('127.0.0.1', 'retries', broker.server.port)
          ..socketTimeout = 5000
          ..connectAckTimeout = 80;
    broker.onConnect = (peer) {
      if (peer.index == 2) peer.accept();
    };
    expect((await client.connect())?.state, MqttConnectionState.connected);
    expect(broker.peers, hasLength(3));
    await eventually(() => broker.peers.take(2).every((peer) => peer.closed));
    expect(broker.peers.last.closed, isFalse);
  });

  test(
    'disconnect retires a TCP dial that completes late without sending CONNECT',
    () async {
      final release = Completer<void>();
      await IOOverrides.runZoned(
        () async {
          final connecting = client.connect();
          final failed = expectLater(
            connecting,
            throwsA(isA<MqttNoConnectionException>()),
          );
          try {
            await eventually(() => broker.peers.isNotEmpty);
            client.disconnect();
            release.complete();
            await failed;
            await eventually(
              () => broker.peers.isNotEmpty && broker.peers.single.closed,
            );
            expect(broker.peers.single.connectPackets, 0);
          } finally {
            if (!release.isCompleted) release.complete();
          }
        },
        socketConnect:
            (host, port, {sourceAddress, sourcePort = 0, timeout}) async {
              final task = await Socket.startConnect(host, port);
              final socket = await task.socket;
              await release.future;
              return socket;
            },
      );
    },
  );

  test('disconnect cancels an in-flight CONNACK wait promptly', () async {
    broker.onConnect = (_) {};
    final connecting = client.connect();
    final failed = expectLater(
      connecting,
      throwsA(isA<MqttNoConnectionException>()),
    );
    await eventually(
      () => broker.peers.isNotEmpty && broker.peers.single.connectPackets == 1,
    );
    client.disconnect();
    await failed.timeout(const Duration(milliseconds: 500));
    await eventually(
      () => broker.peers.isNotEmpty && broker.peers.single.closed,
    );
  });

  test(
    'broker DISCONNECT closes the native socket and preserves its reason',
    () async {
      await client.connect();
      broker.peers.single.disconnect();
      await eventually(
        () =>
            client.connectionStatus?.state == MqttConnectionState.disconnected,
      );
      await eventually(
        () => broker.peers.isNotEmpty && broker.peers.single.closed,
      );
      expect(
        client.connectionStatus?.disconnectionOrigin,
        MqttDisconnectionOrigin.brokerSolicited,
      );
      expect(
        client.connectionStatus?.disconnectMessage.reasonCode,
        MqttDisconnectReasonCode.sessionTakenOver,
      );
    },
  );

  test(
    'automatic reconnection still works with socket ownership cleanup',
    () async {
      client.autoReconnect = true;
      final reconnected = Completer<void>();
      client.onAutoReconnected = () => reconnected.complete();
      await client.connect();
      broker.peers.single.socket.destroy();
      await reconnected.future.timeout(const Duration(seconds: 3));
      expect(client.connectionStatus?.state, MqttConnectionState.connected);
      expect(broker.peers, hasLength(2));
    },
  );

  test(
    'stop cancels both keepalive timers without a second disconnect callback',
    () async {
      final observed = client as ObservedClient;
      client.keepAlivePeriod = 1;
      client.disconnectOnNoResponsePeriod = 1;
      var disconnected = 0;
      client.onDisconnected = () => disconnected++;
      broker.answerPing = false;
      await client.connect();
      final keepAlive = observed.keepAliveForTest!;
      await eventually(() => keepAlive.disconnectTimer?.isActive == true);
      client.disconnect();
      expect(keepAlive.pingTimer?.isActive, isFalse);
      expect(keepAlive.disconnectTimer?.isActive, isFalse);
      await Future<void>.delayed(const Duration(milliseconds: 1100));
      expect(disconnected, 1);
    },
  );

  test('TLS also waits for delayed CONNACK on a single socket', () async {
    await tls();
    broker.onConnect = (peer) =>
        broker.after(const Duration(milliseconds: 1500), peer.accept);
    expect((await client.connect())?.state, MqttConnectionState.connected);
    final published = client.published!.first;
    publish();
    await published;
    expect(broker.peers, hasLength(1));
  });

  test(
    'late TLS handshake after disconnect is closed without CONNECT',
    () async {
      await tls(handshakeDelay: const Duration(milliseconds: 250));
      final connecting = client.connect();
      final failed = expectLater(
        connecting,
        throwsA(isA<MqttNoConnectionException>()),
      );
      await Future<void>.delayed(const Duration(milliseconds: 50));
      client.disconnect();
      await failed;
      await eventually(
        () => broker.peers.isNotEmpty && broker.peers.single.closed,
      );
      expect(broker.peers.single.connectPackets, 0);
    },
  );

  test(
    'socket timeout bounds TLS negotiation and retires its late result',
    () async {
      await tls(handshakeDelay: const Duration(milliseconds: 1400));
      client.socketTimeout = 1000;
      await expectLater(client.connect(), throwsA(isA<TimeoutException>()));
      await eventually(
        () => broker.peers.isNotEmpty && broker.peers.single.closed,
      );
      expect(broker.peers.single.connectPackets, 0);
    },
  );

  test(
    'TLS reset during a write does not escape through IOSink.done',
    () async {
      await tls();
      await client.connect();
      broker.resetOnPublish = true;
      publish(payload: 'x' * (1024 * 1024));
      await eventually(
        () =>
            client.connectionStatus?.state == MqttConnectionState.disconnected,
      );
    },
  );

  for (final stage in [2, 3]) {
    test(
      'QoS 2 rejection at stage $stage reports publishFail and preserves the connection',
      () async {
        var denied = true;
        broker.onPublish = (peer, publication) {
          if (denied && stage == 2) {
            peer.send([
              0x50,
              4,
              publication.id >> 8,
              publication.id & 255,
              0x87,
              0,
            ]);
          } else {
            peer.acknowledge(publication);
          }
        };
        broker.pubcompReason = (_) => stage == 3 && denied ? 0x92 : 0;
        await client.connect();
        final failed = client.publishFail!.first;
        final id = publish();
        final ack = await failed.timeout(const Duration(seconds: 2));
        expect(ack.messageIdentifier, id);
        expect(client.connectionStatus?.state, MqttConnectionState.connected);
        denied = false;
        final published = client.published!.first;
        publish();
        await published.timeout(const Duration(seconds: 2));
        expect(broker.peers, hasLength(1));
      },
    );
  }

  test(
    'duplicate PUBCOMP is ignored without poisoning the next request',
    () async {
      await client.connect();
      final published = client.published!.first;
      final id = publish();
      await published;
      broker.peers.single.send([0x70, 4, id >> 8, id & 255, 0, 0]);
      final next = client.published!.first;
      publish();
      await next.timeout(const Duration(seconds: 2));
      expect(client.connectionStatus?.state, MqttConnectionState.connected);
    },
  );

  test('unknown PUBREC reply retains the incoming packet identifier', () async {
    await client.connect();
    broker.peers.single.send([0x50, 4, 0x03, 0xDB, 0, 0]);
    await eventually(() => broker.peers.single.pubrelIds.isNotEmpty);
    expect(broker.peers.single.pubrelIds.single, 987);
    expect(client.connectionStatus?.state, MqttConnectionState.connected);
  });
}
