import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { spawnSync } from 'node:child_process';

const root = process.cwd();
const archive = join(tmpdir(), 'react-native-mqtt-consumer.tgz');
const consumer = await mkdtemp(join(tmpdir(), 'react-native-mqtt-consumer.'));

try {
  const build = spawnSync('yarn', ['build'], { cwd: root, stdio: 'inherit' });
  if (build.status !== 0) throw new Error('yarn build failed');

  const pack = spawnSync('yarn', ['pack', '--out', archive], {
    cwd: root,
    stdio: 'inherit',
  });
  if (pack.status !== 0) throw new Error('yarn pack failed');

  await writeFile(
    join(consumer, 'package.json'),
    JSON.stringify(
      {
        name: 'react-native-mqtt-clean-consumer',
        private: true,
        dependencies: {
          '@cnhongwei/react-native-mqtt': `file:${archive}`,
          'react': '19.2.3',
          'react-native': '0.86.2',
        },
      },
      null,
      2
    )
  );

  const install = spawnSync(
    'npm',
    ['install', '--ignore-scripts', '--no-audit', '--no-fund'],
    { cwd: consumer, stdio: 'inherit' }
  );
  if (install.status !== 0)
    throw new Error('clean consumer npm install failed');

  const packageJson = JSON.parse(
    await readFile(
      join(consumer, 'node_modules/@cnhongwei/react-native-mqtt/package.json'),
      'utf8'
    )
  );
  if (
    packageJson.name !== '@cnhongwei/react-native-mqtt' ||
    packageJson.version !== '0.1.0'
  ) {
    throw new Error('installed package identity/version is incorrect');
  }

  for (const path of [
    'android/build.gradle',
    'android/consumer-rules.pro',
    'ios/ReactNativeMqtt.mm',
    'ReactNativeMqtt.podspec',
  ]) {
    if (
      !existsSync(
        join(consumer, 'node_modules/@cnhongwei/react-native-mqtt', path)
      )
    ) {
      throw new Error(`installed package is missing ${path}`);
    }
  }
  console.log('Clean consumer package smoke passed.');
} finally {
  await rm(consumer, { recursive: true, force: true });
  await rm(archive, { force: true });
}
