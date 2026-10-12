import { readFileSync } from 'node:fs';

const examples = [
  ['example/src/App.tsx', "from '../../examples/shared/ExampleScreen'"],
  ['examples/expo/App.js', "from '../shared/ExampleScreen'"],
  ['examples/shared/ExampleScreen.js', 'testID="mqtt-profile"'],
];
const required = [
  'testID="mqtt-profile"',
  'testID="mqtt-connect"',
  'testID="mqtt-publish"',
  'testID={`mqtt-protocol-${version}`}',
  'testID="mqtt-test-roundtrip"',
  'testID="mqtt-test-v5"',
  'testID={`mqtt-test-result-',
  'testID="mqtt-disconnect"',
  'testID="mqtt-profile-close"',
  "renderList('logs'",
  "renderList('messages'",
  'cnhongwei/react-native-mqtt',
  'JS block demo',
  'appendBounded',
  '16 * 1024',
  '4096',
];

for (const [file, entryMarker] of examples) {
  const source = readFileSync(file, 'utf8');
  if (source.includes('<ScrollView'))
    throw new Error(
      `${file} must use virtualized lists from the shared screen`
    );
  const markers = file.includes('shared/ExampleScreen')
    ? required
    : [entryMarker];
  for (const marker of markers) {
    if (!source.includes(marker)) {
      throw new Error(
        `${file} is missing example verification marker: ${marker}`
      );
    }
  }
}

const shared = readFileSync('examples/shared/ExampleScreen.js', 'utf8');
for (const port of [
  1883, 1884, 8883, 8884, 8885, 8886, 8887, 8080, 8081, 8090, 8091,
]) {
  if (!shared.includes(`port: ${port}`))
    throw new Error(`shared example is missing fixed profile ${port}`);
}
if ((shared.match(/port: \d+/g) || []).length !== 11)
  throw new Error('shared example must contain exactly 11 fixed profiles');
for (const marker of [
  "username: 'rw'",
  "password: 'readwrite'",
  'p12Password: profile.p12 ? CLIENT_P12_PASSWORD : undefined',
  'Promise.allSettled(promises)',
  'if (clientRef.current) {',
]) {
  if (!shared.includes(marker))
    throw new Error(`shared example is missing fixed behavior: ${marker}`);
}
if (shared.includes('<TextInput'))
  throw new Error('shared example must not require manual connection input');
if (shared.includes('<Text style={s.clear}>`Clear ${tab}`</Text>'))
  throw new Error('clear action must render the selected tab name');
for (const marker of ['useColorScheme', 'barStyle={dark ?', 'overlay:']) {
  if (!shared.includes(marker))
    throw new Error(`shared example is missing theme behavior: ${marker}`);
}

console.log('Example surface smoke passed for RN and Expo entry points.');
