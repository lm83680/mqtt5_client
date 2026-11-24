/*
 * Package : mqtt5_client
 * Author : S. Hamblett <steve.hamblett@linux.com>
 * Date   : 10/05/2020
 * Copyright :  S.Hamblett
 */

part of '../../../mqtt5_server_client.dart';

/// The MQTT server secure connection class
class MqttServerSecureConnection extends MqttServerConnection {
  /// The security context for secure usage
  SecurityContext? context;

  /// Callback function to handle bad certificate. if true, ignore the error.
  bool Function(X509Certificate certificate)? onBadCertificate;

  /// Default constructor
  MqttServerSecureConnection(
    this.context,
    events.EventBus? eventBus,
    this.onBadCertificate,
    List<RawSocketOption> socketOptions,
    Duration? socketTimeout,
  ) : super(eventBus, socketOptions, socketTimeout);

  /// Initializes a new instance of the MqttServerSecureConnection class.
  MqttServerSecureConnection.fromConnect(
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
      _connectSocket(
        SecureSocket.connect(
          server,
          port,
          context: context,
          onBadCertificate: onBadCertificate,
          timeout: socketTimeout,
        ),
      );

  /// Automatic recovery obeys the same socket timeout and ownership rules.
  @override
  Future<MqttConnectionStatus?> connectAuto(String server, int port) =>
      connect(server, port);
}
