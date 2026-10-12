# @cnhongwei/react-native-mqtt

Native MQTT 3.1.1 and MQTT 5 client for React Native on Android and iOS.
The package uses the New Architecture TurboModule and does not support Web.

MQTT 3.1 (`protocolVersion: 3`) is **not supported** on either platform.
Protocol levels `4` and `5` mean MQTT 3.1.1 and MQTT 5.0, respectively.

## Installation

```sh
npm install @cnhongwei/react-native-mqtt
```

Install native dependencies with the normal React Native workflow, then
rebuild the application. Native changes are not delivered by Metro or OTA.

## Basic usage

```ts
import { createMqttClient, decodeUtf8 } from '@cnhongwei/react-native-mqtt';

const client = createMqttClient({
  host: 'test.mosquitto.org',
  port: 1883,
  protocolVersion: 4,
  clientId: 'example-client',
});

const listener = client.onMessage((message) => {
  console.log(message.topic, decodeUtf8(message.bytes));
});

try {
  const ack = await client.connect();
  console.log(ack.sessionPresent);
  const subscription = await client.subscribe('demo/status');
  await client.publishText('demo/status', 'hello');
  await client.ready();
  await subscription.unsubscribe();
} finally {
  listener.remove();
  await client.dispose();
}
```

`createMqttClient` is synchronous and performs no Native I/O. `connect()` is
lazy and deduplicated for same-turn callers. `ready()` waits for the captured
subscription intent and does not connect implicitly. Always call `dispose()` in
a `finally` block; `Symbol.asyncDispose` is installed when the runtime provides
it.

## Options

`protocolVersion` is `4` (MQTT 3.1.1, the default) or `5`.
Passing `3` (MQTT 3.1, not MQTT 3.0) throws `ERR_INVALID_OPTIONS` before native I/O.
Paho Java can support MQTT 3.1, but this wrapper does not expose it; the locked
CocoaMQTT 2.4.0 backend implements MQTT 3.1.1/5, not MQTT 3.1.

### Testing protocols in the example

Both RN and Expo examples offer MQTT 3.1.1 (4) / MQTT 5.0 (5) selection
(example default: 5; SDK default: 4). Disconnect before switching versions.
For each connection profile and version, connect, run **Test QoS 0/1/2**, then
disconnect. The test checks actual payload/QoS round trips and unsubscribes its
unique test topic. In version 5, **Test v5 properties** also checks Content Type
and ordered, duplicate-key User Properties received from the broker.
The UI reports PASS/FAIL, including timeouts, and disables the v5 test in version 4.
These checks are interactive smoke tests, not full protocol conformance tests;
see [example testing instructions](example/README.md) for the remaining matrix.

| Option                                                                           | Meaning and default                                                                                                                                        |
| -------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `host`, `port`                                                                   | Required host and integer port `1..65535`.                                                                                                                 |
| `transport`, `path`                                                              | `tcp`, `tls`, `ws`, or `wss`; `path` applies only to WS/WSS and defaults to `/mqtt`.                                                                       |
| `clientId`                                                                       | Optional MQTT UTF-8 string; an ID is generated when omitted.                                                                                               |
| `username`, `password`                                                           | Optional UTF-8 credentials; password requires username.                                                                                                    |
| `keepAliveSeconds`                                                               | `0..65535`, default `60`; `0` disables protocol heartbeat.                                                                                                 |
| `connectTimeoutMs`                                                               | Positive safe integer, default `30000`.                                                                                                                    |
| `operationTimeoutMs`                                                             | Positive safe integer, default `30000`.                                                                                                                    |
| `disconnectTimeoutMs`                                                            | Positive safe integer, default `5000`.                                                                                                                     |
| `reconnect`                                                                      | `enabled: true`, `initialDelayMs: 1000`, `maxDelayMs: 30000`, `jitterRatio: 0.2`.                                                                          |
| `tls`                                                                            | Optional DER CA / PKCS#12 Base64 and explicit `insecure`; only TLS/WSS. Default verifies system trust and hostname.                                        |
| `will`                                                                           | MQTT will `{ topic, payloadBase64, qos?, retained? }`.                                                                                                     |
| `cleanSession`                                                                   | MQTT 3.1.1 only.                                                                                                                                           |
| `cleanStart`, `sessionExpiryIntervalSeconds`, `receiveMaximum`, `userProperties` | MQTT 5 only; user properties preserve order and duplicate keys.                                                                                            |
| `delivery`                                                                       | Queue defaults `1024` messages / `8 MiB` / `1 MiB` per message; batches default `64` messages / `256 KiB`, ACK watchdog `15000 ms`, overflow `disconnect`. |

Subscribe filters support `+` and a terminal `#`, preserve empty levels, and
do not support `$share/`. Publish topics are topic names and reject wildcards.
Use `publishBytes` for arbitrary bytes and `publishBase64` for canonical RFC
4648 Base64; neither converts binary payloads to text.

## Public surface

- `connect`, `disconnect`, `dispose`, `ready`, and `getSnapshot` manage one
  in-memory session.
- `subscribe(filter, options)` returns a reference-counted handle. Equal
  options share one broker subscription; conflicting options are rejected.
- `publishText`, `publishBytes`, and `publishBase64` publish QoS `0..2`.
- `onStateChange`, `onMessage`, and `onDiagnostic` return removable listeners.
- `encodeUtf8`/`decodeUtf8` and `encodeBase64`/`decodeBase64` are strict helpers.

`Message.bytes` and `ConnAck.userProperties` are the lossless wire-facing
forms. `onMessage` is delivery acknowledgement to the bridge, not MQTT or
business acknowledgement.

### Android MQTT backend and QoS 2

Android uses the direct Eclipse Paho MQTT Java 1.2.5 clients for MQTT 3.1.1
(`org.eclipse.paho.client.mqttv3`) and MQTT 5
(`org.eclipse.paho:org.eclipse.paho.mqttv5.client`); Paho Android Service is
not used. MQTT 5 QoS 2 publish completion must reach `PUBCOMP`. The dedicated
Android `PUBREC` → `PUBCOMP` success, timeout, disconnect, duplicate, and
negative-path behavior has been verified with the repository-controlled Paho
tests. This does not imply that the incomplete public RN/Expo matrix is
complete.

## Errors and platform limits

Operations reject with `MqttError` containing `code` and `message`. The fixed
codes are `ERR_INVALID_OPTIONS`, `ERR_INVALID_PAYLOAD`,
`ERR_NATIVE_UNAVAILABLE`, `ERR_CONTRACT_MISMATCH`, `ERR_NOT_CONNECTED`,
`ERR_DISPOSED`, `ERR_OPERATION_IN_PROGRESS`, `ERR_OPERATION_SUPERSEDED`,
`ERR_CONNECT_TIMEOUT`, `ERR_OPERATION_TIMEOUT`, `ERR_CONNECTION_REFUSED`,
`ERR_CONNECTION_LOST`, `ERR_SUBSCRIPTION_CONFLICT`, `ERR_SUBSCRIBE_FAILED`,
`ERR_UNSUBSCRIBE_FAILED`, `ERR_PUBLISH_FAILED`, `ERR_TLS`,
`ERR_CLEANUP_FAILED`, and `ERR_UNKNOWN`. `outcome` distinguishes
`not-sent`, `rejected`, and `unknown` where known.

This release is foreground, in-memory operation. It does not promise a
persistent offline outbox, guaranteed background execution, Doze/process-death
keep-alive, or Expo Go support. Expo requires a development or release build;
native changes require a new build.

For Android `ws://` connections, the consuming application must explicitly
allow cleartext traffic for the build variant that uses them. The example
application does this for Release because its public smoke matrix includes
plaintext WebSocket listeners; `wss://` does not require this opt-in.

## Development verification

```sh
yarn typecheck
yarn lint
yarn test
yarn codegen
yarn build
yarn test:package
yarn verify
```

Android and iOS native commands require their respective SDK/toolchain; see
`example/android` and `example/ios`. The public Mosquitto matrix is configured
under `test/mosquitto`; public service availability is not a substitute for
fault-injection or performance evidence.

## Contributing

- [Development workflow](CONTRIBUTING.md#development-workflow)
- [Sending a pull request](CONTRIBUTING.md#sending-a-pull-request)
- [Code of conduct](CODE_OF_CONDUCT.md)

## License

MIT

---

Made with [create-react-native-library](https://github.com/callstack/react-native-builder-bob)
