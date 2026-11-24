# PetTa MQTT reliability changes

## 基线与提交位置

- 官方仓库：<https://github.com/shamblett/mqtt5_client>
- 基线：官方 tag `4.17.1`，commit `bcb544dc701e655f04456e7f6820c7f338112fca`
- 本地分支：`codex/mqtt-connection-reliability`
- 保留官方包名和版本 `mqtt5_client: 4.17.1`，便于后续通过 Git 依赖接入。
- 上游 remote 名为 `upstream`；自己的 GitHub 仓库可添加为 `origin`。
- 当前改动在工作区中，尚未创建提交或推送。

## 修复内容

1. **TCP/TLS 与 CONNACK 超时分离。** 新增公开配置
   `MqttServerClient.connectAckTimeout`（毫秒，默认 5000）。设置 socketTimeout
   不再意外把 CONNACK 等待缩短为 1000ms。
2. **单个 client 的连接尝试不留下并行旧 socket。** 首次连接重试前清理上一条
   native 连接；取消会终止 CONNACK 等待，迟到的 TCP/TLS 结果在发送 CONNECT
   前被销毁。默认重试次数仍保持上游的 3 次，调用方可设为 1 次。
3. **完整关闭连接。** 主动关闭、broker DISCONNECT 和 socket 异常均释放底层
   socket；断开回调在清理前解除，避免递归。初始连接和自动重连共用清理规则。
4. **TLS 整体建连有时限。** socketTimeout 同时约束 TCP 及 TLS 协商，超时后
   即使系统异步握手迟到，也会关闭其结果。Socket 异常/超时会如实抛出，不再
   被误当作连接成功继续等待 CONNACK。
5. **关闭写端异步错误和计时器泄漏。** 同时观察 Socket 的读错误与 IOSink.done，
   避免写入中 reset 形成未处理异步异常；停用保活时同时取消 PING 和响应计时器。
6. **QoS 2 结果正确性。** 被拒绝的 PUBREC/PUBCOMP 通过 publishFail 报告，
   不再误报发布成功；重复/迟到 PUBCOMP 安全忽略。未知 PUBREC 的 PUBREL
   使用收到的 packetId，而不是分配一个无关的新 ID。

证书校验规则保持上游默认行为。测试中的自签名证书仅由本地测试客户端显式接受。

## 验证

在项目根目录运行：

```sh
dart pub get
dart test
dart analyze lib test/mqtt_server_recovery_test.dart test/support/recovery_broker.dart test/support/mqtt_client_mock_socket.dart
```

本次结果：374 项测试全部通过，静态分析无问题。其中新增 17 项使用真实本地
TCP/TLS broker 的故障测试，直接运行库原生 client，不依赖 PetTaApp 的适配层。
涵盖 1.5 秒/4 秒慢 CONNACK、独立握手超时、重试清理、迟到 TCP/TLS、
broker 踢线、自动重连、保活清理、TLS 写入 reset 及 QoS 2 异常 ACK。

## PetTaApp 后续接入

提交到自己的 GitHub 仓库并提供地址/commit 后，再修改 App 的 Git 依赖。
目前 PetTaApp 仍使用 pub.dev 官方版及其现有适配，不依赖本机此目录。
接入此 fork 后可移除 App 中预先建立 socket、IOOverrides 交付和 QoS 2
ACK 修补代码，改用库的原生连接管理：

```dart
final client = MqttServerClient.withPort(
  host,
  clientId,
  port,
  maxConnectionAttempts: 1,
)
  ..autoReconnect = false
  ..socketTimeout = 5000
  ..connectAckTimeout = 5000
  ..keepAlivePeriod = 20
  ..disconnectOnNoResponsePeriod = 20;
```

App 仍需保留外层总超时、连接恢复退避、单主题 SUBACK 补偿、请求有效期、
前台健康探测及网络恢复唤醒。这些是业务恢复策略，不适合移入通用 MQTT 库。
