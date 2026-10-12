import {
  cp,
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

const sourceRoot = resolve('examples/expo');
const archive = join(tmpdir(), 'react-native-mqtt-expo-consumer.tgz');
const root = await mkdtemp(join(tmpdir(), 'react-native-mqtt-expo-consumer.'));
const runtimeAndroidDevice = process.env.EXPO_RUNTIME_ANDROID_DEVICE || '';
const runtimeHost = process.env.EXPO_RUNTIME_HOST || '';
const runtimePort = Number(process.env.EXPO_RUNTIME_PORT || 0);
const runtimeTransport = process.env.EXPO_RUNTIME_TRANSPORT || '';
const runtimePath = process.env.EXPO_RUNTIME_PATH || '';
const runtimeUsername = process.env.EXPO_RUNTIME_USERNAME || '';
const runtimePassword = process.env.EXPO_RUNTIME_PASSWORD || '';
const runtimeProtocolVersion = Number(
  process.env.EXPO_RUNTIME_PROTOCOL_VERSION || 5
);
const runtimeTlsInsecure = process.env.EXPO_RUNTIME_TLS_INSECURE === '1';
const runtimeTlsCaBase64 = process.env.EXPO_RUNTIME_TLS_CA_BASE64 || '';
const runtimeTlsP12Base64 = process.env.EXPO_RUNTIME_TLS_P12_BASE64 || '';
const runtimeTlsP12Password = process.env.EXPO_RUNTIME_TLS_P12_PASSWORD || '';
const runtimeDevelopment = process.env.EXPO_RUNTIME_DEVELOPMENT === '1';
const skipIosBuild = process.env.EXPO_SKIP_IOS_BUILD === '1';
const runtimeFixture = runtimeAndroidDevice !== '';
if (
  runtimeFixture &&
  (!runtimeHost ||
    !Number.isInteger(runtimePort) ||
    runtimePort < 1 ||
    runtimePort > 65535)
)
  throw new Error(
    'EXPO_RUNTIME_ANDROID_DEVICE requires EXPO_RUNTIME_HOST and EXPO_RUNTIME_PORT'
  );
if (runtimeDevelopment && !runtimeFixture)
  throw new Error(
    'EXPO_RUNTIME_DEVELOPMENT requires EXPO_RUNTIME_ANDROID_DEVICE, EXPO_RUNTIME_HOST, and EXPO_RUNTIME_PORT'
  );
if (![4, 5].includes(runtimeProtocolVersion))
  throw new Error('EXPO_RUNTIME_PROTOCOL_VERSION must be 4 or 5');
if (runtimeTransport && !['tcp', 'tls', 'ws', 'wss'].includes(runtimeTransport))
  throw new Error('EXPO_RUNTIME_TRANSPORT must be tcp, tls, ws, or wss');

if (!existsSync(resolve(sourceRoot, 'package.json')))
  throw new Error('Expo example is missing');

const ignored = new Set([
  'node_modules',
  'android/build',
  'ios/Pods',
  'ios/build',
  'ios/Podfile.lock',
]);
await cp(sourceRoot, root, {
  recursive: true,
  filter: (source) => {
    const relative = source.slice(sourceRoot.length + 1);
    return ![...ignored].some(
      (path) => relative === path || relative.startsWith(`${path}/`)
    );
  },
});
await cp(
  resolve('examples/public-listeners.ts'),
  join(root, 'public-listeners.ts')
);
await cp(resolve('examples/shared'), join(root, 'shared'), { recursive: true });
const appPath = join(root, 'App.js');
const inferredRuntimeTransport =
  runtimeTransport ||
  ([8883, 8884, 8885, 8886, 8887].includes(runtimePort)
    ? 'tls'
    : [8081, 8091].includes(runtimePort)
      ? 'wss'
      : [8080, 8090].includes(runtimePort)
        ? 'ws'
        : 'tcp');
const runtimeConfig = runtimePort
  ? {
      profiles: [
        {
          port: runtimePort,
          transport: inferredRuntimeTransport,
          path:
            runtimePath ||
            (inferredRuntimeTransport === 'ws' || inferredRuntimeTransport === 'wss'
              ? '/mqtt'
              : undefined),
          label: `runtime ${runtimePort}`,
          auth: runtimeUsername ? 'runtime credentials' : 'anonymous',
        },
      ],
      connectionOptions: {
        host: runtimeHost || undefined,
        port: runtimePort,
        transport: inferredRuntimeTransport,
        path:
          runtimePath ||
          (inferredRuntimeTransport === 'ws' || inferredRuntimeTransport === 'wss'
            ? '/mqtt'
            : undefined),
        protocolVersion: runtimeProtocolVersion,
        username: runtimeUsername || undefined,
        password: runtimePassword || undefined,
        tls:
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
            : undefined,
      },
    }
  : undefined;
await writeFile(
  appPath,
  `import ExampleScreen from './shared/ExampleScreen';\n\nexport default function App() {\n  return <ExampleScreen title="react-native-mqtt Expo" clientIdPrefix="expo-example" runtimeConfig={${runtimeConfig ? JSON.stringify(runtimeConfig) : 'undefined'}} />;\n}\n`
);

const packagePath = join(root, 'package.json');
const packageJson = JSON.parse(await readFile(packagePath, 'utf8'));
packageJson.dependencies['@cnhongwei/react-native-mqtt'] = `file:${archive}`;
await writeFile(packagePath, `${JSON.stringify(packageJson, null, 2)}\n`);

const expo = resolve(root, 'node_modules/.bin/expo');
let metroProcess;

const cleanup = async () => {
  if (metroProcess) {
    metroProcess.kill('SIGTERM');
    metroProcess = undefined;
  }
  await rm(root, { recursive: true, force: true });
  await rm(archive, { force: true });
};

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

const waitForMetro = async (port, timeoutMs = 45000) => {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    try {
      const response = await fetch(`http://127.0.0.1:${port}/status`);
      if (response.ok) return;
    } catch {}
    await new Promise((resolveWait) => setTimeout(resolveWait, 500));
  }
  throw new Error(`Expo Metro did not become ready on port ${port}`);
};

try {
  run('yarn', ['build'], resolve('.'));
  run('yarn', ['pack', '--out', archive], resolve('.'));
  run('npm', ['install', '--ignore-scripts', '--no-audit', '--no-fund'], root);
  if (!existsSync(expo))
    throw new Error(
      'Expo CLI is missing from the isolated consumer dependencies'
    );

  run(expo, ['prebuild', '--clean'], root);

  const generatedAndroidBuild = resolve(root, 'android/app/build.gradle');
  let generatedAndroidBuildText = await readFile(generatedAndroidBuild, 'utf8');
  if (!generatedAndroidBuildText.includes('bundle.properties')) {
    generatedAndroidBuildText = generatedAndroidBuildText.replace(
      'android {\n',
      'android {\n    packagingOptions {\n        resources.excludes += ["META-INF/INDEX.LIST", "bundle.properties"]\n    }\n\n'
    );
  }
  if (
    !generatedAndroidBuildText.includes('coreLibraryDesugaringEnabled true')
  ) {
    generatedAndroidBuildText = generatedAndroidBuildText.replace(
      'android {\n',
      'android {\n    compileOptions {\n        coreLibraryDesugaringEnabled true\n    }\n\n'
    );
    generatedAndroidBuildText = generatedAndroidBuildText.replace(
      'dependencies {\n',
      'dependencies {\n    coreLibraryDesugaring("com.android.tools:desugar_jdk_libs:2.1.5")\n\n'
    );
    await writeFile(generatedAndroidBuild, generatedAndroidBuildText);
  }

  run(
    './gradlew',
    [':app:assembleDebug', '--no-daemon', '--console=plain'],
    resolve(root, 'android')
  );
  run(
    './gradlew',
    [':app:assembleRelease', '--no-daemon', '--console=plain'],
    resolve(root, 'android'),
    {
      NODE_ENV: 'production',
    }
  );

  if (!skipIosBuild) {
    const workspaces = (await readdir(resolve(root, 'ios'))).filter((name) =>
      name.endsWith('.xcworkspace')
    );
    if (workspaces.length !== 1)
      throw new Error('Expo iOS prebuild must produce exactly one workspace');

    run('pod', ['install', '--repo-update'], resolve(root, 'ios'));
    const workspace = workspaces[0];
    const scheme = workspace.replace(/\.xcworkspace$/, '');
    for (const configuration of ['Debug', 'Release']) {
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
          configuration,
          '-derivedDataPath',
          `build/ios-${configuration.toLowerCase()}`,
          'build',
          'CODE_SIGNING_ALLOWED=NO',
        ],
        resolve(root, 'ios')
      );
    }
  }

  if (
    !existsSync(
      resolve(
        root,
        'node_modules/@cnhongwei/react-native-mqtt/android/build.gradle'
      )
    )
  )
    throw new Error(
      'isolated Expo consumer did not install the packaged Android library'
    );
  if (
    !existsSync(
      resolve(root, 'android/app/build/outputs/apk/debug/app-debug.apk')
    )
  )
    throw new Error('isolated Expo Android Debug APK was not produced');
  if (
    !existsSync(
      resolve(root, 'android/app/build/outputs/apk/release/app-release.apk')
    )
  )
    throw new Error('isolated Expo Android Release APK was not produced');
  if (!skipIosBuild) {
    if (
      !existsSync(
        resolve(
          root,
          'ios/build/ios-debug/Build/Products/Debug-iphonesimulator'
        )
      )
    )
      throw new Error('isolated Expo iOS Debug product was not produced');
    if (
      !existsSync(
        resolve(
          root,
          'ios/build/ios-release/Build/Products/Release-iphonesimulator'
        )
      )
    )
      throw new Error('isolated Expo iOS Release product was not produced');
  }

  if (runtimeFixture) {
    const compose = resolve('test/broker/compose.yaml');
    run('docker', ['compose', '-f', compose, 'up', '-d'], resolve('.'));
    try {
      if (runtimeDevelopment) {
        metroProcess = spawn(
          expo,
          [
            'start',
            '--dev-client',
            '--host',
            'lan',
            '--port',
            '8081',
            '--non-interactive',
          ],
          {
            cwd: root,
            stdio: 'inherit',
            env: { ...process.env, CI: '1', BROWSER: 'none' },
          }
        );
        await waitForMetro(8081);
        run(
          'adb',
          ['-s', runtimeAndroidDevice, 'reverse', 'tcp:8081', 'tcp:8081'],
          root
        );
      }
      const runtimeApk = resolve(
        root,
        `android/app/build/outputs/apk/${runtimeDevelopment ? 'debug' : 'release'}/app-${runtimeDevelopment ? 'debug' : 'release'}.apk`
      );
      run(
        'adb',
        ['-s', runtimeAndroidDevice, 'install', '-r', runtimeApk],
        root
      );
      run(
        'adb',
        [
          '-s',
          runtimeAndroidDevice,
          'shell',
          'am',
          'force-stop',
          'com.cnhongwei.mqttexample.expo',
        ],
        root
      );
      if (runtimeDevelopment) {
        run(
          'adb',
          [
            '-s',
            runtimeAndroidDevice,
            'shell',
            'am',
            'start',
            '-a',
            'android.intent.action.VIEW',
            '-d',
            'exp+react-native-mqtt-expo-example://expo-development-client/?url=http%3A%2F%2F127.0.0.1%3A8081',
          ],
          root
        );
      } else {
        run(
          'adb',
          [
            '-s',
            runtimeAndroidDevice,
            'shell',
            'am',
            'start',
            '-n',
            'com.cnhongwei.mqttexample.expo/.MainActivity',
          ],
          root
        );
      }
      const adbOutput = (args) =>
        spawnSync('adb', ['-s', runtimeAndroidDevice, ...args], {
          cwd: root,
          encoding: 'utf8',
          stdio: ['ignore', 'pipe', 'ignore'],
        }).stdout || '';
      const uiXml = () => {
        const dump = spawnSync(
          'adb',
          [
            '-s',
            runtimeAndroidDevice,
            'shell',
            'uiautomator',
            'dump',
            '/sdcard/expo-window.xml',
          ],
          {
            cwd: root,
            encoding: 'utf8',
            stdio: ['ignore', 'pipe', 'ignore'],
          }
        );
        if (dump.status !== 0) return '';
        return adbOutput(['shell', 'cat', '/sdcard/expo-window.xml']);
      };
      const foregroundActivity = () =>
        adbOutput(['shell', 'dumpsys', 'activity', 'activities'])
          .split('\n')
          .find((line) => line.includes('mResumedActivity')) || '';
      const waitFor = async (text, timeoutMs = 45000) => {
        const startedAt = Date.now();
        while (Date.now() - startedAt < timeoutMs) {
          if (uiXml().includes(text)) return;
          await new Promise((resolveWait) => setTimeout(resolveWait, 1000));
        }
        const ui = uiXml().replace(/></g, '>\n<').slice(-12000);
        const logcat = adbOutput(['logcat', '-d', '-t', '300'])
          .split('\n')
          .filter((line) =>
            /cnhongwei|Paho|Mqtt|SSL|WebSocket|ReactNative/i.test(line)
          )
          .slice(-120)
          .join('\n');
        throw new Error(
          `Expo Android runtime did not show: ${text}\nUI:\n${ui}\nRelevant logcat:\n${logcat}`
        );
      };
      const waitForStructuredError = async (timeoutMs = 45000) => {
        const startedAt = Date.now();
        while (Date.now() - startedAt < timeoutMs) {
          const xml = uiXml();
          if (/ERR_[A-Z_]+/.test(xml) && /event|diagnostic/i.test(xml)) return;
          run(
            'adb',
            [
              '-s',
              runtimeAndroidDevice,
              'shell',
              'input',
              'swipe',
              '540',
              '1000',
              '540',
              '300',
              '250',
            ],
            root
          );
          await new Promise((resolveWait) => setTimeout(resolveWait, 1000));
        }
        throw new Error(
          `Expo Android runtime did not show a structured ERR_* result for port ${runtimePort}`
        );
      };
      const tap = (text) => {
        const match = uiXml().match(
          new RegExp(
            `text="[^"]*${text.replace(/[.*+?^${}()|[\\]\\]/g, '\\$&')}[^"]*"[^>]*bounds="\\[(\\d+),(\\d+)\\]\\[(\\d+),(\\d+)\\]"`
          )
        );
        if (!match)
          throw new Error(`Expo Android runtime could not find: ${text}`);
        const x = Math.round((Number(match[1]) + Number(match[3])) / 2);
        const y = Math.round((Number(match[2]) + Number(match[4])) / 2);
        run(
          'adb',
          [
            '-s',
            runtimeAndroidDevice,
            'shell',
            'input',
            'tap',
            String(x),
            String(y),
          ],
          root
        );
      };
      const runtimeScreenStartedAt = Date.now();
      let devLauncherBackSent = false;
      while (Date.now() - runtimeScreenStartedAt < 45000) {
        const xml = uiXml();
        if (xml.includes('Connect and subscribe')) break;
        if (xml.includes('Continue')) {
          tap('Continue');
          await new Promise((resolveWait) => setTimeout(resolveWait, 750));
          run(
            'adb',
            ['-s', runtimeAndroidDevice, 'shell', 'input', 'keyevent', '4'],
            root
          );
          devLauncherBackSent = true;
          await new Promise((resolveWait) => setTimeout(resolveWait, 1000));
          continue;
        }
        // Expo Dev Launcher can remain on its menu after the deep link.  On API24
        // the menu may expose no stable text to UiAutomator, so use the resumed
        // activity as the bounded signal for one Back action.
        if (
          !devLauncherBackSent &&
          Date.now() - runtimeScreenStartedAt > 5000 &&
          foregroundActivity().includes('DevLauncherActivity')
        ) {
          run(
            'adb',
            ['-s', runtimeAndroidDevice, 'shell', 'input', 'keyevent', '4'],
            root
          );
          devLauncherBackSent = true;
          await new Promise((resolveWait) => setTimeout(resolveWait, 1000));
          continue;
        }
        await new Promise((resolveWait) => setTimeout(resolveWait, 750));
      }
      await waitFor('Connect and subscribe');
      tap('Connect and subscribe');
      if (runtimePort === 8887) {
        await waitForStructuredError();
      } else {
        await waitFor('State: connected');
        tap('Publish timestamp');
        tap('Messages (');
        await waitFor('hello ');
        tap('Disconnect');
        await waitFor('State: disconnected');
      }
    } finally {
      spawnSync(
        'adb',
        [
          '-s',
          runtimeAndroidDevice,
          'uninstall',
          'com.cnhongwei.mqttexample.expo',
        ],
        { stdio: 'ignore' }
      );
      if (metroProcess) {
        metroProcess.kill('SIGTERM');
        metroProcess = undefined;
      }
      spawnSync('docker', ['compose', '-f', compose, 'down', '-v'], {
        stdio: 'ignore',
      });
    }
  }

  console.log(
    runtimeFixture
      ? `Isolated Expo tarball install, clean prebuild, ${skipIosBuild ? 'Android-only' : 'iOS/Android'} Debug/Release builds, and Android ${runtimeDevelopment ? 'development-client' : 'bundled-release'} runtime smoke passed.`
      : `Isolated Expo tarball install, clean prebuild, and ${skipIosBuild ? 'Android-only' : 'iOS/Android'} Debug/Release builds passed.`
  );
} finally {
  await cleanup();
}
