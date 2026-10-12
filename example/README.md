# React Native MQTT example

## 协议选择与测试

支持 MQTT 3.1.1（协议级别 4）和 MQTT 5.0（5）；**不支持 MQTT 3.1（3）**。
示例默认选择 5（SDK 默认 4）。页面顶部可切换版本，连接期间禁用切换；先 Disconnect
再选择另一版本，旧客户端会被释放，之后重新 Connect and subscribe。

1. 对每个版本和连接 profile，连接并确认订阅 ready。
2. 点击 `Test QoS 0/1/2`，验证三种 QoS 的发布完成及收到的 payload、QoS、retain 标记。
3. 版本 5 再点击 `Test v5 properties`，验证 `contentType: text/plain` 和两个相同 key、
   不同 value 的 User Properties 按顺序回传。版本 4 禁用该按钮。
4. 查看 PASS/FAIL 和详细 Logs；回环等待上限 15 秒，连接/订阅/取消订阅使用 SDK 超时。
   测试 topic 为 `cnhongwei/react-native-mqtt/checks/<unique-id>`，结束时移除监听并取消订阅。
5. Disconnect，确认取消主订阅和断开日志；切换版本/profile 后重复。

RN 与 Expo 复用同一界面。发布测试仅使用公开示例数据，不设置 retained。
公共 8887 端口应证书失败；公共服务不可用或属性被改变会显示失败，不视为协议不支持。
这覆盖交互冒烟流程，不等于完整验收。完整回归还需在 Android/iOS 上分别覆盖两种协议的
TCP/TLS/WS/WSS、认证/证书负例、遗嘱、保留消息、会话恢复及重连，使用可控本地 Broker。

这个示例使用内置的连接 profile，通过页面顶部的选择面板连接
[test.mosquitto.org](https://test.mosquitto.org/)，不需要手动输入用户名、密码或证书。

所有 profile 使用同一个 topic：`cnhongwei/react-native-mqtt`。点击 Publish timestamp 会发送本地设备时间形成的纯文本，例如
`hello 2026-09-12 02:17:44`；连接成功后示例会自动订阅该 topic，因此消息会从真实 MQTT 回调进入 Messages 列表。

Logs 和 Messages 是两个独立的列表 tab，连接、订阅、发布、收到消息、断开、销毁和诊断事件都会记录日志。列表有数量和内容上限，避免示例自身无限增长。

公开测试账号为 `rw/readwrite`、`ro/readonly`、`wo/writeonly`。`client.p12` 是公开测试证书，密码为 `12345678`；当前客户端证书有效至 `2026-12-09 23:28:31 UTC`。8887 使用过期证书，连接失败是预期结果。

`JS block demo (3 sec)` 会在 JavaScript 线程同步阻塞约 3 秒，同时发起 32 次发布并统计 Promise 成功/失败；它用于观察 JS 阻塞期间的行为，不是 Native 阻塞或消息溢出测试。压力测试应使用本地 broker 和独立发布端。

示例不支持 Expo Go；Expo 版本需要 development build 或 release build。

## Getting started

The project is bootstrapped using [`@react-native-community/cli`](https://github.com/react-native-community/cli).

# Getting Started

> **Note**: Make sure you have completed the [Set Up Your Environment](https://reactnative.dev/docs/set-up-your-environment) guide before proceeding.

## Step 1: Start Metro

First, you will need to run **Metro**, the JavaScript build tool for React Native.

To start the Metro dev server, run the following command from the root of your React Native project:

```sh
# Using npm
npm start

# OR using Yarn
yarn start
```

## Step 2: Build and run your app

With Metro running, open a new terminal window/pane from the root of your React Native project, and use one of the following commands to build and run your Android or iOS app:

### Android

```sh
# Using npm
npm run android

# OR using Yarn
yarn android
```

### iOS

For iOS, remember to install CocoaPods dependencies (this only needs to be run on first clone or after updating native deps).

The first time you create a new project, run the Ruby bundler to install CocoaPods itself:

```sh
bundle install
```

Then, and every time you update your native dependencies, run:

```sh
bundle exec pod install
```

For more information, please visit [CocoaPods Getting Started guide](https://guides.cocoapods.org/using/getting-started.html).

```sh
# Using npm
npm run ios

# OR using Yarn
yarn ios
```

If everything is set up correctly, you should see your new app running in the Android Emulator, iOS Simulator, or your connected device.

This is one way to run your app — you can also build it directly from Android Studio or Xcode.

## Step 3: Modify your app

Now that you have successfully run the app, let's make changes!

Open `App.tsx` in your text editor of choice and make some changes. When you save, your app will automatically update and reflect these changes — this is powered by [Fast Refresh](https://reactnative.dev/docs/fast-refresh).

When you want to forcefully reload, for example to reset the state of your app, you can perform a full reload:

- **Android**: Press the <kbd>R</kbd> key twice or select **"Reload"** from the **Dev Menu**, accessed via <kbd>Ctrl</kbd> + <kbd>M</kbd> (Windows/Linux) or <kbd>Cmd ⌘</kbd> + <kbd>M</kbd> (macOS).
- **iOS**: Press <kbd>R</kbd> in iOS Simulator.

## Congratulations! :tada:

You've successfully run and modified your React Native App. :partying_face:

### Now what?

- If you want to add this new React Native code to an existing application, check out the [Integration guide](https://reactnative.dev/docs/integration-with-existing-apps).
- If you're curious to learn more about React Native, check out the [docs](https://reactnative.dev/docs/getting-started).

# Troubleshooting

If you're having issues getting the above steps to work, see the [Troubleshooting](https://reactnative.dev/docs/troubleshooting) page.

# Learn More

To learn more about React Native, take a look at the following resources:

- [React Native Website](https://reactnative.dev) - learn more about React Native.
- [Getting Started](https://reactnative.dev/docs/environment-setup) - an **overview** of React Native and how setup your environment.
- [Learn the Basics](https://reactnative.dev/docs/getting-started) - a **guided tour** of the React Native **basics**.
- [Blog](https://reactnative.dev/blog) - read the latest official React Native **Blog** posts.
- [`@facebook/react-native`](https://github.com/facebook/react-native) - the Open Source; GitHub **repository** for React Native.
