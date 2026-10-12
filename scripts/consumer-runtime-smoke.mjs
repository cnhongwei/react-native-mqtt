import {
  cp,
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  rm,
  writeFile,
} from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import { spawn, spawnSync } from 'node:child_process';

const root = process.cwd();
const example = resolve(root, 'example');
const archive = join(tmpdir(), 'react-native-mqtt-consumer-runtime.tgz');
const consumer = await mkdtemp(
  join(tmpdir(), 'react-native-mqtt-consumer-runtime.')
);
const androidPackage = 'cnhongwei.reactnativemqtt.example';
const iosBundle = 'cnhongwei.reactnativemqtt.example';
const androidDevice = process.env.CONSUMER_ANDROID_DEVICE || 'emulator-5554';
const androidInstalledDevices = new Set();
let iosInstalled = false;
let iosDevice;
let metroProcess;
const runtimeErrorPort = Number(process.env.CONSUMER_RUNTIME_ERROR_PORT || 0);
const runtimeErrorMissingUserInfo =
  process.env.CONSUMER_RUNTIME_ERROR_MISSING_USERINFO === '1';
const runtimePort = Number(process.env.CONSUMER_RUNTIME_PORT || 0);
const runtimeTlsInsecure = process.env.CONSUMER_RUNTIME_TLS_INSECURE === '1';
const runtimeTlsCaBase64 = process.env.CONSUMER_RUNTIME_TLS_CA_BASE64 || '';
const runtimeTlsP12Base64 = process.env.CONSUMER_RUNTIME_TLS_P12_BASE64 || '';
const runtimeTlsP12Password =
  process.env.CONSUMER_RUNTIME_TLS_P12_PASSWORD || '';
const runtimeUsername = process.env.CONSUMER_RUNTIME_USERNAME || '';
const runtimePassword = process.env.CONSUMER_RUNTIME_PASSWORD || '';
const runtimeTransport = process.env.CONSUMER_RUNTIME_TRANSPORT || '';
const runtimePath = process.env.CONSUMER_RUNTIME_PATH || '';
const runtimeReceiveMaximum = Number(
  process.env.CONSUMER_RUNTIME_RECEIVE_MAXIMUM || 0
);
const runtimeCleanStart =
  process.env.CONSUMER_RUNTIME_CLEAN_START === '0' ? false : null;
const runtimeSessionExpiry = Number(
  process.env.CONSUMER_RUNTIME_SESSION_EXPIRY || 0
);
const runtimeAssignedClientId =
  process.env.CONSUMER_RUNTIME_ASSIGNED_CLIENT_ID === '1';
const runtimeWill = process.env.CONSUMER_RUNTIME_WILL === '1';
const runtimeDuplicateUserProperties =
  process.env.CONSUMER_RUNTIME_DUPLICATE_USER_PROPERTIES === '1';
const runtimeAndroidOnly = process.env.CONSUMER_RUNTIME_ANDROID_ONLY === '1';
const runtimeIosOnly = process.env.CONSUMER_RUNTIME_IOS_ONLY === '1';
const runtimeMatrix = process.env.CONSUMER_RUNTIME_MATRIX === '1';
const runtimeMatrixPorts = process.env.CONSUMER_RUNTIME_MATRIX_PORTS
  ? process.env.CONSUMER_RUNTIME_MATRIX_PORTS.split(',').map((value) =>
      Number(value.trim())
    )
  : null;
const runtimeWaitAfterIosLaunchMs = Number(
  process.env.CONSUMER_RUNTIME_WAIT_AFTER_IOS_LAUNCH_MS || 0
);
const runtimeDoze = process.env.CONSUMER_RUNTIME_DOZE === '1';
const runtimeReconnect = process.env.CONSUMER_RUNTIME_RECONNECT === '1';
const runtimeDispose = process.env.CONSUMER_RUNTIME_DISPOSE === '1';
const runtimeOverflow = process.env.CONSUMER_RUNTIME_OVERFLOW === '1';
const runtimeIndependentPublisher =
  process.env.CONSUMER_RUNTIME_INDEPENDENT_PUBLISHER === '1';
const runtimeIndependentPublisherCount = Number(
  process.env.CONSUMER_RUNTIME_INDEPENDENT_PUBLISHER_COUNT || 32
);
const runtimeKeepTemp = process.env.CONSUMER_RUNTIME_KEEP_TEMP === '1';
const runtimeConnectOnly = process.env.CONSUMER_RUNTIME_CONNECT_ONLY === '1';
const runtimeHost = process.env.CONSUMER_RUNTIME_HOST || '';
const runtimeProtocolVersion = Number(
  process.env.CONSUMER_RUNTIME_PROTOCOL_VERSION || 5
);
const runtimeMatrixProtocols = process.env.CONSUMER_RUNTIME_MATRIX_PROTOCOLS
  ? process.env.CONSUMER_RUNTIME_MATRIX_PROTOCOLS.split(',').map((value) =>
      Number(value.trim())
    )
  : [4, 5];
if (runtimeErrorPort > 0 && runtimePort > 0) {
  throw new Error(
    'Choose only one of CONSUMER_RUNTIME_ERROR_PORT or CONSUMER_RUNTIME_PORT'
  );
}
if (runtimeErrorPort > 0 && runtimeErrorMissingUserInfo) {
  throw new Error('Choose only one native error smoke mode');
}
if (runtimeDoze && runtimeErrorPort > 0) {
  throw new Error('CONSUMER_RUNTIME_DOZE requires a successful runtime path');
}
if (runtimeReconnect && runtimeErrorPort > 0) {
  throw new Error(
    'CONSUMER_RUNTIME_RECONNECT requires a successful runtime path'
  );
}
if (runtimeDispose && runtimeErrorPort > 0) {
  throw new Error(
    'CONSUMER_RUNTIME_DISPOSE requires a successful runtime path'
  );
}
if (runtimeOverflow && runtimeErrorPort > 0) {
  throw new Error(
    'CONSUMER_RUNTIME_OVERFLOW requires a successful runtime path'
  );
}
if (runtimeIndependentPublisher && !runtimeOverflow) {
  throw new Error(
    'CONSUMER_RUNTIME_INDEPENDENT_PUBLISHER requires CONSUMER_RUNTIME_OVERFLOW'
  );
}
if (
  runtimeIndependentPublisher &&
  (!Number.isInteger(runtimeIndependentPublisherCount) ||
    runtimeIndependentPublisherCount < 1 ||
    runtimeIndependentPublisherCount > 64)
) {
  throw new Error(
    'CONSUMER_RUNTIME_INDEPENDENT_PUBLISHER_COUNT must be an integer from 1 to 64'
  );
}
if (runtimeReconnect && !runtimeHost) {
  throw new Error('CONSUMER_RUNTIME_RECONNECT requires CONSUMER_RUNTIME_HOST');
}
if (
  runtimeReceiveMaximum !== 0 &&
  (!Number.isInteger(runtimeReceiveMaximum) ||
    runtimeReceiveMaximum < 1 ||
    runtimeReceiveMaximum > 65535)
) {
  throw new Error(
    'CONSUMER_RUNTIME_RECEIVE_MAXIMUM must be an integer in 1..65535'
  );
}
if (
  !Number.isInteger(runtimeSessionExpiry) ||
  runtimeSessionExpiry < 0 ||
  runtimeSessionExpiry > 4294967295
) {
  throw new Error(
    'CONSUMER_RUNTIME_SESSION_EXPIRY must be an integer in 0..4294967295'
  );
}
if (runtimeAssignedClientId && runtimeCleanStart === false) {
  throw new Error(
    'CONSUMER_RUNTIME_ASSIGNED_CLIENT_ID requires cleanStart=true'
  );
}
if (
  runtimeTransport &&
  !['tcp', 'tls', 'ws', 'wss'].includes(runtimeTransport)
) {
  throw new Error('CONSUMER_RUNTIME_TRANSPORT must be tcp, tls, ws, or wss');
}
if (
  !Number.isInteger(runtimeWaitAfterIosLaunchMs) ||
  runtimeWaitAfterIosLaunchMs < 0
) {
  throw new Error(
    'CONSUMER_RUNTIME_WAIT_AFTER_IOS_LAUNCH_MS must be a non-negative integer'
  );
}
if (runtimeAndroidOnly && runtimeIosOnly) {
  throw new Error(
    'Choose only one of CONSUMER_RUNTIME_ANDROID_ONLY or CONSUMER_RUNTIME_IOS_ONLY'
  );
}
if (runtimeMatrix && !runtimeAndroidOnly) {
  throw new Error(
    'CONSUMER_RUNTIME_MATRIX currently requires Android-only mode'
  );
}
if (
  runtimeMatrixPorts &&
  runtimeMatrixPorts.some(
    (port) => !Number.isInteger(port) || port < 1 || port > 65535
  )
) {
  throw new Error(
    'CONSUMER_RUNTIME_MATRIX_PORTS must be comma-separated valid ports'
  );
}
if (![4, 5].includes(runtimeProtocolVersion)) {
  throw new Error('CONSUMER_RUNTIME_PROTOCOL_VERSION must be 4 or 5');
}
if (
  runtimeMatrixProtocols.length === 0 ||
  runtimeMatrixProtocols.some((version) => ![4, 5].includes(version))
) {
  throw new Error(
    'CONSUMER_RUNTIME_MATRIX_PROTOCOLS must be a comma-separated list of 4 and/or 5'
  );
}
const fixtureComposeFile = resolve(root, 'test/broker/compose.yaml');
let fixtureStarted = false;
const selectedRuntimePort =
  runtimeErrorPort || runtimePort || (runtimeOverflow ? 1883 : 0);
const publicMatrixPorts = new Set([
  1883, 1884, 8883, 8884, 8885, 8886, 8887, 8080, 8081, 8090, 8091,
]);
const inferredRuntimeTransport =
  runtimeTransport ||
  ([8883, 8884, 8885, 8886, 8887].includes(selectedRuntimePort)
    ? 'tls'
    : [8081, 8091].includes(selectedRuntimePort)
      ? 'wss'
      : [8080, 8090].includes(selectedRuntimePort)
        ? 'ws'
        : '');

const run = (command, args, cwd, env = {}) => {
  const result = spawnSync(command, args, {
    cwd,
    stdio: 'inherit',
    env: { ...process.env, CI: '1', ...env },
  });
  if (result.error) throw result.error;
  if (result.status !== 0)
    throw new Error(
      `${command} ${args.join(' ')} exited with ${result.status}`
    );
};

const capture = (command, args, cwd, env = {}) => {
  const result = spawnSync(command, args, {
    cwd,
    encoding: 'utf8',
    env: { ...process.env, CI: '1', ...env },
  });
  if (result.error) throw result.error;
  if (result.status !== 0)
    throw new Error(
      `${command} ${args.join(' ')} exited with ${result.status}: ${result.stderr}`
    );
  return result.stdout.trim();
};

const wait = (milliseconds) =>
  new Promise((resolveWait) => setTimeout(resolveWait, milliseconds));

const publishIndependentMessages = async (messages) => {
  for (const message of messages) {
    await new Promise((resolvePublish, rejectPublish) => {
      const publisher = spawn(
        'mosquitto_pub',
        [
          '-h',
          '127.0.0.1',
          '-p',
          '1883',
          '-t',
          'cnhongwei/react-native-mqtt',
          '-q',
          '1',
          '-m',
          message,
        ],
        { stdio: ['ignore', 'ignore', 'pipe'] }
      );
      let stderr = '';
      publisher.stderr.on('data', (chunk) => {
        stderr += chunk.toString();
      });
      publisher.once('error', rejectPublish);
      publisher.once('close', (code, signal) => {
        if (code === 0) resolvePublish();
        else
          rejectPublish(
            new Error(
              `independent publisher exited with ${code ?? signal}: ${stderr}`
            )
          );
      });
    });
  }
};

const waitForMetro = async (port, timeoutMs = 30000) => {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    try {
      const response = await fetch(`http://127.0.0.1:${port}/status`);
      if (response.ok) return;
    } catch {}
    await wait(250);
  }
  throw new Error(`Metro did not become ready on port ${port}`);
};

const assertMetroPortFree = async (port) => {
  try {
    const response = await fetch(`http://127.0.0.1:${port}/status`);
    if (response.ok)
      throw new Error(
        `Metro port ${port} is already occupied by an existing server`
      );
  } catch (error) {
    if (error instanceof Error && error.message.startsWith('Metro port '))
      throw error;
  }
};

const uiXml = (device, cwd) => {
  let lastError;
  for (let attempt = 0; attempt < 3; attempt += 1) {
    try {
      run(
        'adb',
        [
          '-s',
          device,
          'shell',
          'uiautomator',
          'dump',
          '/sdcard/consumer-window.xml',
        ],
        cwd
      );
      lastError = undefined;
      break;
    } catch (error) {
      lastError = error;
      // A short immediate retry avoids losing the whole matrix to transient
      // Android uiautomator process eviction (exit 137).
    }
  }
  if (lastError) throw lastError;
  return capture(
    'adb',
    ['-s', device, 'shell', 'cat', '/sdcard/consumer-window.xml'],
    cwd
  );
};

const tapText = (device, text, cwd) => {
  const escaped = text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const match = uiXml(device, cwd).match(
    new RegExp(
      `<node(?=[^>]*(?:text|content-desc)="[^"]*${escaped}[^"]*")(?=[^>]*bounds="\\[(\\d+),(\\d+)\\]\\[(\\d+),(\\d+)\\]")[^>]*>`
    )
  );
  if (!match) throw new Error(`could not find Android UI text: ${text}`);
  const x = Math.round((Number(match[1]) + Number(match[3])) / 2);
  const y = Math.round((Number(match[2]) + Number(match[4])) / 2);
  run(
    'adb',
    ['-s', device, 'shell', 'input', 'tap', String(x), String(y)],
    cwd
  );
};

const tapId = (device, resourceId, cwd) => {
  const escaped = resourceId.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const match = uiXml(device, cwd).match(
    new RegExp(
      `resource-id="[^"]*${escaped}"[^>]*bounds="\\[(\\d+),(\\d+)\\]\\[(\\d+),(\\d+)\\]"`
    )
  );
  if (!match) throw new Error(`could not find Android UI id: ${resourceId}`);
  const x = Math.round((Number(match[1]) + Number(match[3])) / 2);
  const y = Math.round((Number(match[2]) + Number(match[4])) / 2);
  run('adb', ['-s', device, 'shell', 'input', 'tap', String(x), String(y)], cwd);
};

const tapListener = async (device, port, cwd) => {
  const escaped = String(port).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const match = uiXml(device, cwd).match(
    new RegExp(
      `<node(?=[^>]*content-desc="listener-${escaped}")(?=[^>]*bounds="\\[(\\d+),(\\d+)\\]\\[(\\d+),(\\d+)\\]")[^>]*>`
    )
  );
  if (match) {
    const x = Math.round((Number(match[1]) + Number(match[3])) / 2);
    const y = Math.round((Number(match[2]) + Number(match[4])) / 2);
    run(
      'adb',
      ['-s', device, 'shell', 'input', 'tap', String(x), String(y)],
      cwd
    );
  } else {
    tapText(device, String(port), cwd);
  }
  await waitForUiText(device, String(port), cwd, 5000);
  await waitForUiTextToDisappear(device, 'Choose connection', cwd, 5000);
};

const waitForUiText = async (device, text, cwd, timeoutMs = 45000) => {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    if (uiXml(device, cwd).includes(text)) return;
    await wait(1000);
  }
  const finalUi = uiXml(device, cwd);
  const stateText =
    finalUi.match(/text="State: [^"]*"/)?.[0] || 'State text not found';
  const resultText =
    finalUi.match(/text="(?:.*event.*|.*ERR_)[^"]*"/)?.[0] ||
    'structured result text not found';
  throw new Error(
    `Android consumer UI did not show: ${text}; ${stateText}; ${resultText}`
  );
};

const waitForUiTextToDisappear = async (
  device,
  text,
  cwd,
  timeoutMs = 5000
) => {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    if (!uiXml(device, cwd).includes(text)) return;
    await wait(250);
  }
  throw new Error(`Android consumer UI did not dismiss: ${text}`);
};

const waitForUiErrorCode = async (device, cwd, timeoutMs = 45000) => {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    const xml = uiXml(device, cwd);
    const eventError = xml.includes('event') && xml.includes('error');
    const errorCode = /ERR_[A-Z_]+/.test(xml);
    if (eventError && errorCode) return;
    run(
      'adb',
      [
        '-s',
        device,
        'shell',
        'input',
        'swipe',
        '540',
        '1000',
        '540',
        '300',
        '250',
      ],
      cwd
    );
    await wait(1000);
  }
  const finalUi = uiXml(device, cwd);
  throw new Error(
    `Android consumer UI did not show a structured ERR_* code; final UI: ${finalUi.slice(0, 4000)}`
  );
};

const waitForUiExpiredTlsFailure = async (device, cwd, timeoutMs = 45000) => {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    const xml = uiXml(device, cwd);
    if (/expired|CertificateExpiredException/i.test(xml)) return;
    await wait(1000);
  }
  throw new Error(
    'Android consumer UI did not show the expected expired TLS certificate failure'
  );
};

const scrollUntilUiText = async (device, text, cwd, attempts = 12) => {
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    const xml = uiXml(device, cwd);
    if (xml.includes(text)) return;
    const listBounds = xml.match(
      /resource-id="mqtt-(?:messages|logs)"[^>]*bounds="\[(\d+),(\d+)\]\[(\d+),(\d+)\]"/
    );
    const x = listBounds
      ? Math.round((Number(listBounds[1]) + Number(listBounds[3])) / 2)
      : 540;
    const fromY = listBounds ? Number(listBounds[4]) - 10 : 2100;
    const toY = listBounds ? Number(listBounds[2]) + 10 : 500;
    run(
      'adb',
      [
        '-s',
        device,
        'shell',
        'input',
        'swipe',
        String(x),
        String(fromY),
        String(x),
        String(toY),
        '300',
      ],
      cwd
    );
    await wait(250);
  }
  throw new Error(
    `Android consumer UI did not expose scrollable text: ${text}`
  );
};

const scrollToTop = async (device, cwd) => {
  for (let attempt = 0; attempt < 12; attempt += 1) {
    run(
      'adb',
      [
        '-s',
        device,
        'shell',
        'input',
        'swipe',
        '540',
        '500',
        '540',
        '1100',
        '300',
      ],
      cwd
    );
    await wait(100);
  }
};

try {
  if (runtimeReconnect || runtimeOverflow) {
    run('docker', ['compose', '-f', fixtureComposeFile, 'up', '-d'], root);
    fixtureStarted = true;
  }
  run('yarn', ['build'], root);
  run('yarn', ['pack', '--out', archive], root);

  await cp(example, consumer, {
    recursive: true,
    filter: (source) => {
      const relative = source.slice(example.length + 1);
      return !(
        relative === 'node_modules' ||
        relative.startsWith('node_modules/') ||
        relative === 'android/.gradle' ||
        relative.startsWith('android/.gradle/') ||
        relative === 'android/build' ||
        relative.startsWith('android/build/') ||
        relative === 'ios/Pods' ||
        relative.startsWith('ios/Pods/') ||
        relative === 'ios/build' ||
        relative.startsWith('ios/build/') ||
        relative === 'ios/Podfile.lock'
      );
    },
  });

  const packagePath = join(consumer, 'package.json');
  const packageJson = JSON.parse(await readFile(packagePath, 'utf8'));
  packageJson.dependencies['@cnhongwei/react-native-mqtt'] = `file:${archive}`;
  await writeFile(packagePath, `${JSON.stringify(packageJson, null, 2)}\n`);
  await writeFile(
    join(consumer, 'metro.config.js'),
    `const { getDefaultConfig } = require('@react-native/metro-config');\nmodule.exports = getDefaultConfig(__dirname);\n`
  );
  await writeFile(
    join(consumer, 'babel.config.js'),
    `module.exports = { presets: ['module:@react-native/babel-preset'] };\n`
  );
  await mkdir(join(consumer, 'examples'), { recursive: true });
  await cp(
    join(root, 'examples/public-listeners.ts'),
    join(consumer, 'examples/public-listeners.ts')
  );
  await cp(join(root, 'examples/shared'), join(consumer, 'examples/shared'), {
    recursive: true,
  });
  const appSourcePath = join(consumer, 'src/App.tsx');
  const runtimeTls =
    inferredRuntimeTransport === 'tls' || inferredRuntimeTransport === 'wss'
      ? {
          insecure: runtimeTlsInsecure,
          ...(runtimeTlsCaBase64 ? { caBase64: runtimeTlsCaBase64 } : {}),
          ...(runtimeTlsP12Base64
            ? {
                p12Base64: runtimeTlsP12Base64,
                p12Password: runtimeTlsP12Password,
              }
            : {}),
        }
      : undefined;
  const runtimeConfig =
    selectedRuntimePort > 0
      ? {
          profiles: [
            {
              port: selectedRuntimePort,
              transport: inferredRuntimeTransport || 'tcp',
              path: runtimePath || undefined,
              label: `runtime ${selectedRuntimePort}`,
              auth: runtimeUsername ? 'runtime credentials' : 'anonymous',
            },
          ],
          connectionOptions: {
            host: runtimeHost || undefined,
            port: selectedRuntimePort,
            transport: inferredRuntimeTransport || 'tcp',
            path: runtimePath || undefined,
            protocolVersion: runtimeProtocolVersion,
            username: runtimeUsername || undefined,
            password: runtimePassword || undefined,
            tls: runtimeTls,
            clientId: runtimeAssignedClientId ? '' : undefined,
            receiveMaximum: runtimeReceiveMaximum || undefined,
            cleanStart:
              runtimeCleanStart === null ? undefined : runtimeCleanStart,
            sessionExpiryIntervalSeconds: runtimeSessionExpiry || undefined,
            will: runtimeWill
              ? { topic: 'demo/will', payloadBase64: 'd2lsbA==', qos: 1 }
              : undefined,
            userProperties: runtimeDuplicateUserProperties
              ? [
                  { key: 'trace', value: 'one' },
                  { key: 'trace', value: 'two' },
                ]
              : undefined,
            delivery: runtimeOverflow
              ? {
                  maxQueuedMessages: runtimeIndependentPublisher ? 64 : 2,
                  maxQueuedBytes: 4096,
                  maxMessageBytes: 1024,
                  overflowPolicy: runtimeIndependentPublisher
                    ? 'drop-newest'
                    : 'disconnect',
                }
              : undefined,
          },
          publishOptions: [2888, 2890].includes(runtimeErrorPort)
            ? { qos: 2 }
            : { qos: 1 },
          ...(runtimeIndependentPublisher ? { jsBlockPublishCount: 0 } : {}),
        }
      : undefined;
  await writeFile(
    appSourcePath,
    `import ExampleScreen from '../examples/shared/ExampleScreen';\n\nexport default function App() {\n  return <ExampleScreen title="runtime consumer" clientIdPrefix="rn-runtime" runtimeConfig={${runtimeConfig ? JSON.stringify(runtimeConfig) : 'undefined'}} />;\n}\n`
  );

  run(
    'npm',
    ['install', '--ignore-scripts', '--no-audit', '--no-fund'],
    consumer
  );
  if (!runtimeAndroidOnly && runtimeWaitAfterIosLaunchMs > 0) {
    await assertMetroPortFree(8081);
    metroProcess = spawn(
      'npx',
      ['react-native', 'start', '--port', '8081', '--reset-cache'],
      {
        cwd: consumer,
        stdio: 'inherit',
        env: { ...process.env, CI: '1' },
      }
    );
    await waitForMetro(8081);
  }
  if (!runtimeIosOnly) {
    run(
      './gradlew',
      [':app:assembleDebug', '--no-daemon', '--console=plain'],
      join(consumer, 'android')
    );
    run(
      './gradlew',
      [
        ':app:assembleRelease',
        '-Pandroid.enableR8.fullMode=true',
        '--no-daemon',
        '--console=plain',
      ],
      join(consumer, 'android')
    );
  }
  if (!runtimeAndroidOnly) {
    run('pod', ['install', '--repo-update'], join(consumer, 'ios'));

    const workspaces = await readdir(join(consumer, 'ios'));
    const workspace = workspaces.find((name) => name.endsWith('.xcworkspace'));
    if (!workspace)
      throw new Error('isolated consumer iOS workspace was not generated');
    const scheme = workspace.replace(/\.xcworkspace$/, '');
    run(
      'xcodebuild',
      [
        '-quiet',
        '-workspace',
        workspace,
        '-scheme',
        scheme,
        '-destination',
        'platform=iOS Simulator,name=iPhone 17 Pro',
        '-configuration',
        'Debug',
        '-derivedDataPath',
        'build/ios-consumer-debug',
        'build',
        'CODE_SIGNING_ALLOWED=NO',
      ],
      join(consumer, 'ios')
    );
  }

  const releaseApk = join(
    consumer,
    'android/app/build/outputs/apk/release/app-release.apk'
  );
  if (!runtimeIosOnly) {
    if (
      !existsSync(
        join(consumer, 'android/app/build/outputs/apk/debug/app-debug.apk')
      )
    )
      throw new Error('isolated consumer Android APK was not produced');
    const releaseConfig = join(
      consumer,
      'android/app/build/outputs/mapping/release/configuration.txt'
    );
    if (!existsSync(releaseApk) || !existsSync(releaseConfig))
      throw new Error(
        'isolated consumer Android R8 release artifacts were not produced'
      );
    const configuration = await readFile(releaseConfig, 'utf8');
    if (!configuration.includes('InsecureTrustManagerFactorySpi'))
      throw new Error(
        'isolated consumer R8 configuration does not include the packaged consumer rules'
      );
  }
  if (
    !runtimeAndroidOnly &&
    !existsSync(
      join(
        consumer,
        'ios/build/ios-consumer-debug/Build/Products/Debug-iphonesimulator'
      )
    )
  )
    throw new Error('isolated consumer iOS Debug product was not produced');

  if (!runtimeIosOnly) {
    const apk = join(
      consumer,
      'android/app/build/outputs/apk/debug/app-debug.apk'
    );
    run('adb', ['-s', androidDevice, 'install', '-r', apk], consumer);
    androidInstalledDevices.add(androidDevice);
    // The Debug APK is installed to verify the debug artifact, but it has no
    // embedded JS bundle when Metro is not explicitly running. Runtime smoke
    // uses the packaged Release APK below so Android-only runs are deterministic.

    const releaseDevice =
      process.env.CONSUMER_ANDROID_RELEASE_DEVICE || androidDevice;
    run('adb', ['-s', releaseDevice, 'install', '-r', releaseApk], consumer);
    androidInstalledDevices.add(releaseDevice);
    run(
      'adb',
      ['-s', releaseDevice, 'shell', 'am', 'force-stop', androidPackage],
      consumer
    );
    run(
      'adb',
      [
        '-s',
        releaseDevice,
        'shell',
        'am',
        'start',
        '-n',
        `${androidPackage}/.MainActivity`,
      ],
      consumer
    );
    await wait(1500);
    const releasePid = capture(
      'adb',
      ['-s', releaseDevice, 'shell', 'pidof', androidPackage],
      consumer
    );
    if (!releasePid)
      throw new Error(
        'isolated consumer Android R8 release process did not start'
      );
    const launchedUi = uiXml(releaseDevice, consumer);
    if (!launchedUi.includes(`package="${androidPackage}"`)) {
      throw new Error(
        `isolated consumer launched an unexpected Android package; expected ${androidPackage}`
      );
    }
    if (selectedRuntimePort === 0) {
      tapText(releaseDevice, 'TCP anonymous', consumer);
      await waitForUiText(releaseDevice, 'Choose connection', consumer);
      tapText(releaseDevice, 'Close', consumer);
      await waitForUiTextToDisappear(
        releaseDevice,
        'Choose connection',
        consumer
      );
      tapText(releaseDevice, 'TCP anonymous', consumer);
      await waitForUiText(releaseDevice, 'Choose connection', consumer);
      run(
        'adb',
        ['-s', releaseDevice, 'shell', 'input', 'keyevent', 'KEYCODE_BACK'],
        consumer
      );
      await waitForUiTextToDisappear(
        releaseDevice,
        'Choose connection',
        consumer
      );
    }
    if (runtimeMatrix) {
      const matrixPorts = runtimeMatrixPorts || [
        1883, 1884, 8883, 8884, 8885, 8886, 8887, 8080, 8081, 8090, 8091,
      ];
      const matrixResults = [];
      let currentPort = 1883;
      for (const protocolVersion of runtimeMatrixProtocols) {
      tapId(releaseDevice, `mqtt-protocol-${protocolVersion}`, consumer);
        await waitForUiText(
          releaseDevice,
          protocolVersion === 4 ? 'MQTT 3.1.1 (4)' : 'MQTT 5.0 (5)',
          consumer
        );
        for (const port of matrixPorts) {
          console.log(`[public-matrix] testing protocol=${protocolVersion} port=${port}`);
          try {
          if (port !== 1883) {
            tapText(releaseDevice, String(currentPort), consumer);
            await waitForUiText(releaseDevice, 'Choose connection', consumer);
            await scrollUntilUiText(releaseDevice, String(port), consumer);
            await tapListener(releaseDevice, port, consumer);
            currentPort = port;
          }
          await scrollToTop(releaseDevice, consumer);
          tapId(releaseDevice, 'mqtt-connect', consumer);
          if (port === 8887) {
            await waitForUiExpiredTlsFailure(releaseDevice, consumer);
            matrixResults.push({ protocolVersion, port, result: 'expected-failure', reason: 'expired TLS certificate' });
            continue;
          }
          if (port === 8885) {
            await waitForUiText(
              releaseDevice,
              'Not authorized',
              consumer
            );
            matrixResults.push({ protocolVersion, port, result: 'expected-failure', reason: 'broker authentication rejected' });
            continue;
          }
          await waitForUiText(releaseDevice, 'State: connected', consumer);
          tapId(releaseDevice, 'mqtt-test-roundtrip', consumer);
          await waitForUiText(
            releaseDevice,
            `PASS · MQTT ${protocolVersion === 4 ? '3.1.1 (4)' : '5.0 (5)'} · QoS 0/1/2 · unsubscribed`,
            consumer
          );
          if (protocolVersion === 5) {
            tapId(releaseDevice, 'mqtt-test-v5', consumer);
            await waitForUiText(
              releaseDevice,
              'PASS · MQTT 5.0 (5) · User Properties + Content Type · unsubscribed',
              consumer
            );
          }
          tapId(releaseDevice, 'mqtt-disconnect', consumer);
          await waitForUiText(releaseDevice, 'State: disconnected', consumer);
          tapId(releaseDevice, 'mqtt-dispose', consumer);
          await waitForUiText(releaseDevice, 'State: idle', consumer);
          matrixResults.push({ protocolVersion, port, result: 'passed' });
        } catch (error) {
          const reason = String(error?.message || error);
          matrixResults.push({ protocolVersion, port, result: 'blocked/unavailable', reason });
          console.log(`[public-matrix] blocked/unavailable protocol=${protocolVersion} port=${port}: ${reason}`);
          try {
            if (uiXml(releaseDevice, consumer).includes('Choose connection')) {
              run(
                'adb',
                ['-s', releaseDevice, 'shell', 'input', 'keyevent', 'KEYCODE_BACK'],
                consumer
              );
              await waitForUiTextToDisappear(
                releaseDevice,
                'Choose connection',
                consumer,
                10000
              );
            }
            const cleanupUi = uiXml(releaseDevice, consumer);
            if (/State: (?:connected|connecting|disconnecting)/.test(cleanupUi)) {
              tapText(releaseDevice, 'Disconnect', consumer);
              await waitForUiText(releaseDevice, 'State: disconnected', consumer, 10000);
            }
            if (uiXml(releaseDevice, consumer).includes('text="Dispose client"')) {
              tapText(releaseDevice, 'Dispose client', consumer);
              await waitForUiText(releaseDevice, 'State: idle', consumer, 10000);
            }
          } catch (cleanupError) {
            console.log(`[public-matrix] cleanup failed: ${String(cleanupError?.message || cleanupError)}`);
          }
          await wait(1000);
          }
        }
      }
      console.log(`[public-matrix] results ${JSON.stringify(matrixResults)}`);
      const blocked = matrixResults.filter(
        (entry) => entry.result === 'blocked/unavailable'
      );
      if (blocked.length > 0)
        throw new Error(
          `public matrix incomplete; blocked/unavailable listeners: ${blocked.map((entry) => entry.port).join(', ')}`
        );
    } else if (selectedRuntimePort > 0) {
      if (
        publicMatrixPorts.has(selectedRuntimePort) &&
        selectedRuntimePort !== 1883
      ) {
        tapText(releaseDevice, String(selectedRuntimePort), consumer);
        await waitForUiText(releaseDevice, 'Choose connection', consumer);
        await scrollUntilUiText(
          releaseDevice,
          String(selectedRuntimePort),
          consumer
        );
        await tapListener(releaseDevice, selectedRuntimePort, consumer);
      }
      await scrollToTop(releaseDevice, consumer);
      tapText(releaseDevice, 'Connect and subscribe', consumer);
      if (runtimeErrorPort > 0) {
        if (runtimeErrorPort === 2889) {
          await waitForUiText(releaseDevice, 'State: connected', consumer);
          await waitForUiText(
            releaseDevice,
            'SERVER_DISCONNECT',
            consumer,
            45000
          );
        } else if ([2886, 2887, 2888, 2890].includes(runtimeErrorPort)) {
          await waitForUiText(releaseDevice, 'State: connected', consumer);
          tapText(
            releaseDevice,
            runtimeErrorPort === 2887 ? 'Disconnect' : 'Publish timestamp',
            consumer
          );
          await waitForUiErrorCode(releaseDevice, consumer);
        } else {
          await waitForUiErrorCode(releaseDevice, consumer);
        }
        if ([2885, 2886, 2887, 2888, 2890].includes(runtimeErrorPort)) {
          const errorResult = uiXml(releaseDevice, consumer);
          const expectedCode =
            runtimeErrorPort === 2885
              ? 'ERR_SUBSCRIBE_FAILED'
              : runtimeErrorPort === 2886 ||
                  runtimeErrorPort === 2888 ||
                  runtimeErrorPort === 2890
                ? 'ERR_PUBLISH_FAILED'
                : 'ERR_UNSUBSCRIBE_FAILED';
          const expectedReasonCode =
            runtimeErrorPort === 2890 ? 'reasonCode":146' : 'reasonCode":135';
          if (
            !errorResult.includes(expectedCode) ||
            !errorResult.includes(expectedReasonCode)
          )
            throw new Error(
              `negative ACK consumer smoke did not preserve ${expectedCode} and ${expectedReasonCode}`
            );
        }
      } else {
        if (selectedRuntimePort === 8887) {
          await waitForUiErrorCode(releaseDevice, consumer);
        } else {
          await waitForUiText(releaseDevice, 'State: connected', consumer);
        }
        if (selectedRuntimePort === 8887) {
          // 8887 is the OpenSpec-defined expired-certificate negative case.
        } else if (runtimeConnectOnly) {
          tapText(releaseDevice, 'Disconnect', consumer);
          await waitForUiText(releaseDevice, 'State: disconnected', consumer);
        } else {
          tapText(releaseDevice, 'Publish timestamp', consumer);
          tapText(releaseDevice, 'Messages (', consumer);
          await waitForUiText(releaseDevice, 'hello ', consumer);
          if (runtimeDoze) {
            run(
              'adb',
              ['-s', releaseDevice, 'shell', 'dumpsys', 'deviceidle', 'enable'],
              consumer
            );
            run(
              'adb',
              [
                '-s',
                releaseDevice,
                'shell',
                'dumpsys',
                'deviceidle',
                'force-idle',
              ],
              consumer
            );
            await wait(5000);
            run(
              'adb',
              [
                '-s',
                releaseDevice,
                'shell',
                'dumpsys',
                'deviceidle',
                'unforce',
              ],
              consumer
            );
            run(
              'adb',
              [
                '-s',
                releaseDevice,
                'shell',
                'dumpsys',
                'deviceidle',
                'disable',
              ],
              consumer
            );
            await wait(3000);
            await waitForUiText(releaseDevice, 'State: connected', consumer);
            tapText(releaseDevice, 'Publish timestamp', consumer);
            tapText(releaseDevice, 'Messages (', consumer);
            await waitForUiText(releaseDevice, 'hello ', consumer);
          }
          if (runtimeReconnect) {
            run(
              'docker',
              ['compose', '-f', fixtureComposeFile, 'stop', 'mosquitto'],
              root
            );
            await wait(3000);
            run(
              'docker',
              ['compose', '-f', fixtureComposeFile, 'up', '-d', 'mosquitto'],
              root
            );
            await waitForUiText(
              releaseDevice,
              'State: connected',
              consumer,
              45000
            );
            tapText(releaseDevice, 'Publish timestamp', consumer);
            tapText(releaseDevice, 'Messages (', consumer);
            await waitForUiText(releaseDevice, 'hello ', consumer, 45000);
          }
          if (runtimeOverflow) {
            if (runtimeIndependentPublisher) {
              await wait(250);
            }
            tapText(releaseDevice, 'JS block demo (3 sec)', consumer);
            if (runtimeIndependentPublisher) {
              const sent = Array.from(
                { length: runtimeIndependentPublisherCount },
                (_, index) =>
                  `external-burst-${String(index + 1).padStart(2, '0')}`
              );
              await publishIndependentMessages(sent);
              console.log(
                `[independent-publisher] sent=${sent.length} topic=cnhongwei/react-native-mqtt`
              );
              await waitForUiText(releaseDevice, 'State: connected', consumer);
              tapText(releaseDevice, 'Messages (', consumer);
              await scrollUntilUiText(
                releaseDevice,
                `external-burst-${String(runtimeIndependentPublisherCount).padStart(2, '0')}`,
                consumer,
                60
              );
              const burstUi = uiXml(releaseDevice, consumer);
              const received = (burstUi.match(/external-burst-/g) || []).length;
              console.log(
                `[independent-publisher] visible-received=${received} newest=external-burst-32`
              );
            } else {
              await waitForUiText(
                releaseDevice,
                'State: disconnected',
                consumer,
                45000
              );
              await wait(3000);
              if (uiXml(releaseDevice, consumer).includes('State: connected'))
                throw new Error(
                  'overflow disconnect unexpectedly reconnected automatically'
                );
            }
          }
          if (runtimeDispose) {
            tapText(releaseDevice, 'Dispose client', consumer);
            await waitForUiText(releaseDevice, 'State: idle', consumer);
          } else {
            tapText(releaseDevice, 'Disconnect', consumer);
            await waitForUiText(releaseDevice, 'State: disconnected', consumer);
          }
        }
      }
    } else if (runtimeErrorMissingUserInfo) {
      tapText(releaseDevice, 'Publish timestamp', consumer);
      await waitForUiErrorCode(releaseDevice, consumer);
      const result = uiXml(releaseDevice, consumer);
      if (
        !result.includes('ERR_NOT_CONNECTED') ||
        result.includes('reasonCode')
      )
        throw new Error(
          'native missing-userInfo rejection did not preserve code/message shape'
        );
    } else {
      tapText(releaseDevice, 'Connect and subscribe', consumer);
      await waitForUiText(releaseDevice, 'State: connected', consumer);
      tapText(releaseDevice, 'Publish timestamp', consumer);
      tapText(releaseDevice, 'Messages (', consumer);
      await waitForUiText(releaseDevice, 'hello ', consumer);
      if (runtimeDispose) {
        tapText(releaseDevice, 'Dispose client', consumer);
        await waitForUiText(releaseDevice, 'State: idle', consumer);
      } else {
        tapText(releaseDevice, 'Disconnect', consumer);
        await waitForUiText(releaseDevice, 'State: disconnected', consumer);
      }
    }
  }

  if (!runtimeAndroidOnly) {
    iosDevice = capture(
      'xcrun',
      ['simctl', 'list', 'devices', 'booted', '-j'],
      consumer
    );
    const booted = JSON.parse(iosDevice);
    const bootedRuntime = Object.values(booted.devices)
      .flat()
      .find((device) => device.state === 'Booted');
    if (!bootedRuntime)
      throw new Error(
        'no booted iOS Simulator is available for consumer runtime smoke'
      );
    iosDevice = bootedRuntime.udid;
    const productDir = join(
      consumer,
      'ios/build/ios-consumer-debug/Build/Products/Debug-iphonesimulator'
    );
    const products = await readdir(productDir);
    const app = products.find((name) => name.endsWith('.app'));
    if (!app) throw new Error('isolated consumer iOS app was not produced');
    run(
      'xcrun',
      ['simctl', 'install', iosDevice, join(productDir, app)],
      consumer
    );
    iosInstalled = true;
    run('xcrun', ['simctl', 'launch', iosDevice, iosBundle], consumer);
    if (runtimeWaitAfterIosLaunchMs > 0) {
      console.log(
        `Waiting ${runtimeWaitAfterIosLaunchMs}ms after iOS launch for external UI inspection.`
      );
      await wait(runtimeWaitAfterIosLaunchMs);
    }
  }

  console.log(
    runtimeAndroidOnly
      ? 'Clean consumer Android Debug/R8 Release build, install, and runtime smoke passed.'
      : 'Clean consumer Android Debug/R8 Release and iOS build, install, and launch smoke passed.'
  );
} finally {
  if (runtimeKeepTemp) {
    console.log(`Keeping runtime consumer for inspection: ${consumer}`);
  } else {
    if (metroProcess) {
      metroProcess.kill('SIGTERM');
      metroProcess = undefined;
    }
    for (const device of androidInstalledDevices) {
      spawnSync('adb', ['-s', device, 'uninstall', androidPackage], {
        stdio: 'ignore',
      });
    }
    if (iosInstalled && iosDevice) {
      spawnSync('xcrun', ['simctl', 'terminate', iosDevice, iosBundle], {
        stdio: 'ignore',
      });
      spawnSync('xcrun', ['simctl', 'uninstall', iosDevice, iosBundle], {
        stdio: 'ignore',
      });
    }
    if (fixtureStarted) {
      spawnSync('docker', ['compose', '-f', fixtureComposeFile, 'down', '-v'], {
        stdio: 'ignore',
      });
    }
    await rm(consumer, { recursive: true, force: true });
    await rm(archive, { force: true });
  }
}
