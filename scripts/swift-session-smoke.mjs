import { execFileSync } from 'node:child_process';
import { resolve } from 'node:path';

const output = '/tmp/react-native-mqtt-swift-session-smoke';
execFileSync('swiftc', [
  '-swift-version', '5',
  resolve('scripts/swift-session-smoke.swift'),
  resolve('ios/ReactNativeMqttSession.swift'),
  '-o', output,
], { stdio: 'inherit' });
execFileSync(output, { stdio: 'inherit' });
