import { packageName, packageVersion } from '../index';

describe('package scaffold', () => {
  it('exposes the fixed package identity without loading NativeModules', () => {
    expect(packageName).toBe('@cnhongwei/react-native-mqtt');
    expect(packageVersion).toBe('0.1.0');
  });
});
