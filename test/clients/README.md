# Android MQTT client probes

These are independent Android apps for separating the MQTT client library
from the React Native bridge and the package Session implementation.

- `paho-android`: Eclipse Paho MQTT 3.1.1 client `1.2.5` with `MemoryPersistence`.
- `rn-paho-v5`: direct Eclipse Paho MQTT 5 client `1.2.5`; Paho Android Service is not used.

Both apps run MQTT 5 QoS 1 connect/publish/disconnect against the repository
Mosquitto fixture through the emulator host alias `10.0.2.2`, using TCP 1883
and WebSocket `/mqtt` on 8080. Results are emitted with the `MQTT_PROBE` tag.

```sh
docker compose -f test/broker/compose.yaml up -d mosquitto
cd test/clients/paho-android && ./gradlew :app:assembleDebug :app:assembleRelease
adb -s emulator-5554 logcat -s MQTT_PROBE:I MQTT_PROBE:E
```

`rn-paho` is the RN-layer A/B probe. It keeps the same React Native shell but
uses a manually registered Paho native module and a separate application id.
Its build artifacts are valid Debug and R8 Release APKs. The probe uses
`org.eclipse.paho.client.mqttv3:1.2.5` (MQTT 3.1.1), not Paho MQTT 5.
Its logger/resource diagnostics and local TCP/WS runtime both pass after the
probe's explicit logger constructor keep rule is applied.

`rn-paho-v5` is the MQTT 5 counterpart. It uses the direct Paho Java MQTT 5
client `org.eclipse.paho:org.eclipse.paho.mqttv5.client:1.2.5` inside the same
React Native shell; Paho Android Service is not used. Its API 24-compatible
R8 Release APK passed the class/constructor diagnostic, the Paho logcat
resource-bundle diagnostic (`keys=187`), and logger-factory initialization.
The automatic local MQTT 5 smoke passed TCP 1883 and WebSocket 8080 on
emulators `emulator-5554`, `emulator-5556`, and `emulator-5558`.

This is a library and local-transport comparison only. It does not claim the
public `test.mosquitto.org` 11-listener matrix, TLS/WSS/mTLS, authentication,
or production Paho backend replacement.
