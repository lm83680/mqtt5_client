/*
 * Package : mqtt5_client
 * Author : S. Hamblett <steve.hamblett@linux.com>
 * Date   : 10/05/2020
 * Copyright :  S.Hamblett
 */

part of '../../../mqtt5_server_client.dart';

/// The MQTT normal(insecure TCP) server connection class
class MqttServerNormalConnection extends MqttServerConnection {
  /// Default constructor
  MqttServerNormalConnection(
    super.eventBus,
    super.socketOptions,
    super.socketTimeout,
  );

  /// Initializes a new instance of the MqttConnection class.
  MqttServerNormalConnection.fromConnect(
    String server,
    int port,
    events.EventBus eventBus,
    List<RawSocketOption> socketOptions,
    Duration? socketTimeout,
  ) : super(eventBus, socketOptions, socketTimeout) {
    connect(server, port);
  }

  /// Connect, with bounded TCP/TLS establishment and late socket cleanup.
  @override
  Future<MqttConnectionStatus?> connect(String server, int port) =>
      _connectSocket(Socket.connect(server, port, timeout: socketTimeout));

  /// Automatic recovery obeys the same socket timeout and ownership rules.
  @override
  Future<MqttConnectionStatus?> connectAuto(String server, int port) =>
      connect(server, port);
}
