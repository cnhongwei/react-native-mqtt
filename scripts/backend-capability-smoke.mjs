import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';

const root = process.cwd();
const podLock = readFileSync(join(root, 'example/ios/Podfile.lock'), 'utf8');
const podspec = readFileSync(join(root, 'ReactNativeMqtt.podspec'), 'utf8');
const gradle = readFileSync(join(root, 'android/build.gradle'), 'utf8');
const swift = readFileSync(join(root, 'ios/ReactNativeMqttCocoaTransport.swift'), 'utf8');
const rules = readFileSync(join(root, 'android/consumer-rules.pro'), 'utf8');

const requiredPodEntries = [
  'CocoaMQTT/Core (2.4.0)',
  'MqttCocoaAsyncSocket (1.0.8)',
  'Starscream (4.0.8)',
]; 
for (const entry of requiredPodEntries) {
  if (!podLock.includes(entry)) throw new Error(`missing locked iOS dependency: ${entry}`);
}
if (!podspec.includes('s.platforms    = { :ios => min_ios_version_supported }')) {
  throw new Error('iOS podspec does not inherit React Native minimum iOS version');
}
if (!podspec.includes('s.swift_version = "5.7"')) {
  throw new Error('iOS podspec Swift tools version is not pinned to 5.7');
}

for (const dependency of [
  'org.eclipse.paho:org.eclipse.paho.client.mqttv3:1.2.5',
  'org.eclipse.paho:org.eclipse.paho.mqttv5.client:1.2.5',
]) {
  if (!gradle.includes(dependency)) throw new Error(`Paho dependency is not locked in android/build.gradle: ${dependency}`);
}
if (!swift.includes('mqtt.autoReconnect = false')) {
  throw new Error('iOS CocoaMQTT automatic reconnect is not disabled');
}
for (const marker of ['sessionPresent', 'userPropertyList', 'serverDisconnectReasonCode']) {
  if (!swift.includes(marker)) {
    throw new Error(`CocoaMQTT adapter capability marker is missing: ${marker}`);
  }
}
for (const marker of [
  'com.cnhongwei.reactnativemqtt.PahoLogger',
  'org.eclipse.paho.client.mqttv3.logging',
  'org.eclipse.paho.mqttv5.client.logging',
]) {
  if (!rules.includes(marker)) throw new Error(`Paho consumer rule is missing: ${marker}`);
}

const gradleUserHome = process.env.GRADLE_USER_HOME ?? join(process.env.HOME ?? '', '.gradle');
const moduleCandidates = [
  join(gradleUserHome, 'caches/modules-2/files-2.1/org.eclipse.paho/org.eclipse.paho.client.mqttv3/1.2.5'),
  join(gradleUserHome, 'caches/modules-2/files-2.1/org.eclipse.paho/org.eclipse.paho.mqttv5.client/1.2.5'),
];
const moduleRoots = moduleCandidates.filter((candidate) => existsSync(candidate));
if (moduleRoots.length !== moduleCandidates.length) {
  console.log('Backend capability smoke skipped: one or more Paho Gradle module caches are unavailable.');
  process.exit(0);
}
for (const rootPath of moduleRoots) {
  const jar = spawnSync('find', [rootPath, '-name', '*.jar', '!', '-name', '*-sources.jar', '-print'], { encoding: 'utf8' }).stdout.trim().split('\n')[0];
  if (!jar) throw new Error(`Paho client jar is unavailable under ${rootPath}`);
  const classList = spawnSync('jar', ['tf', jar], { encoding: 'utf8' });
  if (classList.status !== 0 || !classList.stdout.includes('MqttAsyncClient.class')) {
    throw new Error(`unable to inspect Paho client jar: ${jar}`);
  }
}

console.log('Backend capability smoke passed: locked CocoaMQTT dependencies, Paho MQTT 3.1.1/5 artifacts, Paho logger rules, and single reconnect ownership are present.');
