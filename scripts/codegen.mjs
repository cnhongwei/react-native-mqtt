import { existsSync } from 'node:fs';
import { spawnSync } from 'node:child_process';

const checks = ['src/NativeReactNativeMqtt.ts', 'package.json'];
for (const file of checks) {
  if (!existsSync(file)) throw new Error(`Codegen input missing: ${file}`);
}

const result = spawnSync('yarn', ['react-native', 'codegen'], { stdio: 'inherit' });
if (result.error || result.status !== 0) {
  throw result.error ?? new Error(`React Native Codegen exited with ${result.status}`);
}

const generatedOutputs = [
  'android/app/build/generated/source/codegen/java/com/facebook/fbreact/specs/NativeReactNativeMqttSpec.java',
  'android/app/build/generated/source/codegen/jni/ReactNativeMqttSpec.h',
  'build/generated/ios/ReactCodegen/ReactNativeMqttSpec/ReactNativeMqttSpec.h',
  'build/generated/ios/ReactCodegen/ReactNativeMqttSpec/ReactNativeMqttSpec-generated.mm',
];
for (const file of generatedOutputs) {
  if (!existsSync(file)) throw new Error(`Codegen output missing: ${file}`);
}
console.error('Android and iOS Codegen outputs are present.');
