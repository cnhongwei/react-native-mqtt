import { readFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { resolve } from 'node:path';

const matrix = JSON.parse(
  readFileSync('test/mosquitto/public-listeners.json', 'utf8')
);
if (matrix.listeners.length !== 11) {
  throw new Error('Mosquitto listener matrix is incomplete');
}

const composeFile = 'test/broker/compose.yaml';
const composePath = resolve(process.cwd(), composeFile);
const gradleArgs = [
  ':cnhongwei_react-native-mqtt:testDebugUnitTest',
  '--tests',
  '*PahoMqttTransportIntegrationTest',
  '--no-daemon',
  '--rerun-tasks',
  '--console=plain',
];
const run = (command, args, env = {}, cwd = process.cwd()) => {
  const result = spawnSync(command, args, {
    stdio: 'inherit',
    cwd,
    env: { ...process.env, ...env },
  });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    throw new Error(`${command} ${args.join(' ')} exited with ${result.status}`);
  }
};

// The default is a deterministic local broker run. Public listeners are an
// explicit opt-in because they are external, mutable infrastructure and cannot
// provide fault injection or performance evidence.
const publicRun = process.env.RUN_MQTT_PUBLIC_TESTS === '1';
const fixtureStarted = !publicRun;
if (fixtureStarted) run('docker', ['compose', '-f', composePath, 'up', '-d']);
try {
  run(
    './gradlew',
    gradleArgs,
    publicRun
      ? { RUN_MQTT_PUBLIC_TESTS: '1' }
      : {
          RUN_MQTT_LOCAL_TESTS: '1',
          MQTT_TEST_HOST: '127.0.0.1',
          MQTT_TEST_PORT: '1883',
          MQTT_COMPOSE_FILE: composePath,
      },
    'example/android'
  );
} finally {
  if (fixtureStarted) {
    const cleanup = spawnSync(
      'docker',
      ['compose', '-f', composePath, 'down', '-v'],
      { stdio: 'inherit', env: process.env }
    );
    if (cleanup.error || cleanup.status !== 0) {
      process.exitCode = 1;
    }
  }
}

console.log(
  publicRun
    ? 'Public transport integration command completed; inspect each listener result before claiming matrix coverage.'
    : 'Local Mosquitto integration passed and its fixture was cleaned up.'
);
