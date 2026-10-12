# Expo MQTT example

仅支持 MQTT 3.1.1（`4`）和 MQTT 5.0（`5`），**不支持 MQTT 3.1（`3`）**。
顶部可选择协议，默认 5；切换前先断开。连接后可运行 `Test QoS 0/1/2`，版本 5 还可运行
`Test v5 properties` 验证 Content Type 和重复 key 的有序 User Properties 回传。
结果以 PASS/FAIL 显示；完整步骤与测试范围见 [RN 示例说明](../../example/README.md)。

从 `examples/expo` 启动 development build 或构建 release build。此示例复用
`examples/shared/ExampleScreen.js`，使用内置 profile 连接
[test.mosquitto.org](https://test.mosquitto.org/)，不支持 Expo Go。

所有端口使用 topic `cnhongwei/react-native-mqtt`。连接成功后自动订阅，发送内容是本地时间纯文本，例如
`hello 2026-09-12 02:17:44`。Logs 和 Messages tab 会分别显示生命周期日志和真实收到的消息。

公开账号为 `rw/readwrite`、`ro/readonly`、`wo/writeonly`；mTLS profile 使用测试证书 `client.p12`，密码为
`12345678`，客户端证书有效至 `2026-12-09 23:28:31 UTC`。8887 的过期证书失败属于预期行为。

`JS block demo (3 sec)` 只演示 JavaScript 线程阻塞与 32 次发布结果，不代表 Native 阻塞期间的投递或溢出能力。
