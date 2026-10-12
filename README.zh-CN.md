# @cnhongwei/react-native-mqtt

面向 React Native Android/iOS 的 MQTT 3.1.1 与 MQTT 5 原生客户端，使用新架构 TurboModule，不支持 Web。

Android/iOS 均**不支持 MQTT 3.1（`protocolVersion: 3`）**。
协议级别 `4` 对应 MQTT 3.1.1，`5` 对应 MQTT 5.0；`3` 不是 MQTT 3.0。

## 安装

```sh
npm install @cnhongwei/react-native-mqtt
```

按普通 React Native 流程安装原生依赖并重新构建应用；Metro 或 OTA 不会交付原生变更。

## 基本用法

```ts
import { createMqttClient, decodeUtf8 } from '@cnhongwei/react-native-mqtt';

const client = createMqttClient({
  host: 'test.mosquitto.org',
  port: 1883,
  protocolVersion: 4,
});

const listener = client.onMessage((message) => {
  console.log(message.topic, decodeUtf8(message.bytes));
});

try {
  await client.connect();
  const subscription = await client.subscribe('demo/status');
  await client.publishText('demo/status', 'hello');
  await client.ready();
  await subscription.unsubscribe();
} finally {
  listener.remove();
  await client.dispose();
}
```

`createMqttClient` 同步返回且不执行 Native I/O；`connect()` 惰性执行，同一轮调用会合并。`ready()` 不会隐式连接，只等待调用时捕获的订阅意图。请在 `finally` 中调用 `dispose()`。

## 选项

`protocolVersion` 为 `4`（MQTT 3.1.1，默认）或 `5`。
传入 `3` 会在 Native I/O 前抛出 `ERR_INVALID_OPTIONS`。Paho Java 本身支持
MQTT 3.1，但本库未开放；锁定的 CocoaMQTT 2.4.0 实现仅支持 MQTT 3.1.1/5。

RN/Expo 示例支持选择 MQTT 3.1.1（4）或 MQTT 5.0（5），示例默认 5，SDK 默认 4。
断开后可切换版本；连接后运行 `Test QoS 0/1/2` 检查实际消息回环，选择 5 时还可运行
`Test v5 properties` 检查 Content Type 和重复 key 的有序 User Properties。
结果显示 PASS/FAIL（含超时），测试会取消其独立 topic 的订阅。这是交互冒烟测试，
不代表完整协议一致性验收；完整操作与剩余测试范围见 [示例说明](example/README.md)。

- `host`、`port`：必填，端口为 `1..65535` 整数。
- `transport`：`tcp`、`tls`、`ws`、`wss`；WS/WSS 的 `path` 默认 `/mqtt`。
- `clientId`、`username`、`password`：可选 MQTT UTF-8 字符串；password 必须配合 username。
- `keepAliveSeconds`：`0..65535`，默认 `60`。
- `connectTimeoutMs` / `operationTimeoutMs` / `disconnectTimeoutMs`：默认分别为 `30000`、`30000`、`5000`。
- `reconnect`：默认启用，初始延迟 `1000`、最大延迟 `30000`、抖动比例 `0.2`。
- `tls`：可选 CA、PKCS#12 Base64 与显式 `insecure`；仅 TLS/WSS 可用，默认校验证书链和主机名。
- MQTT 3.1.1 使用 `cleanSession`；MQTT 5 使用 `cleanStart`、会话过期、`receiveMaximum` 和有序（允许重复 key）`userProperties`。
- `delivery` 默认队列为 `1024` 条/`8 MiB`、单条 `1 MiB`；batch 默认 `64` 条/`256 KiB`，ACK watchdog `15000 ms`，溢出策略 `disconnect`。

订阅 filter 支持 `+` 和末尾独占层级 `#`，保留空层级，不支持 `$share/`。发布 topic name 不允许通配符。二进制请使用 `publishBytes` 或严格 RFC 4648 的 `publishBase64`。

## API、错误与限制

客户端提供 `connect`、`disconnect`、`dispose`、`ready`、`getSnapshot`、`subscribe`、`publishText`、`publishBytes`、`publishBase64`，以及 `onStateChange`、`onMessage`、`onDiagnostic`。订阅 handle 是引用计数的；相同选项共享 Native 订阅，冲突会拒绝。

### Android MQTT 后端与 QoS 2

Android 使用直连的 Eclipse Paho MQTT Java 1.2.5：MQTT 3.1.1 使用
`org.eclipse.paho.client.mqttv3`，MQTT 5 使用
`org.eclipse.paho:org.eclipse.paho.mqttv5.client`，不使用 Paho Android
Service。MQTT 5 QoS 2 发布必须完成到 `PUBCOMP`；Android 对
`PUBREC` → `PUBCOMP` 的成功、超时、断开、重复包和负路径已通过仓库受控
Paho 测试验证。但这不代表 RN/Expo 公共矩阵已经全部完成。

错误通过 `MqttError` 返回，固定 code 包括：`ERR_INVALID_OPTIONS`、`ERR_INVALID_PAYLOAD`、`ERR_NATIVE_UNAVAILABLE`、`ERR_CONTRACT_MISMATCH`、`ERR_NOT_CONNECTED`、`ERR_DISPOSED`、`ERR_OPERATION_IN_PROGRESS`、`ERR_OPERATION_SUPERSEDED`、`ERR_CONNECT_TIMEOUT`、`ERR_OPERATION_TIMEOUT`、`ERR_CONNECTION_REFUSED`、`ERR_CONNECTION_LOST`、`ERR_SUBSCRIPTION_CONFLICT`、`ERR_SUBSCRIBE_FAILED`、`ERR_UNSUBSCRIBE_FAILED`、`ERR_PUBLISH_FAILED`、`ERR_TLS`、`ERR_CLEANUP_FAILED`、`ERR_UNKNOWN`。

当前为前台、进程内存操作，不保证磁盘离线 outbox、后台持续运行、Doze/进程终止后的保活，也不支持 Expo Go。Expo 需要 development/release build。

Android 使用 `ws://` 时，宿主应用必须为实际使用该传输的构建变体显式允许明文流量。示例应用因公共 smoke 矩阵包含明文 WebSocket 监听器，已为 Release 打开该配置；`wss://` 不需要此明文许可。

## 开发验证

```sh
yarn typecheck
yarn lint
yarn test
yarn codegen
yarn build
yarn test:package
yarn verify
```

不编写旧 API 迁移指南；本项目没有旧 API 兼容目标。
