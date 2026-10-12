import { readFile } from 'node:fs/promises';

const files = [
  'android/src/main/java/com/cnhongwei/reactnativemqtt/ReactNativeMqttModule.kt',
  'ios/ReactNativeMqtt.mm',
];
const sensitive = /username|password|(?:payload|payloadBase64)|privateKey|p12Password/i;

for (const path of files) {
  const source = await readFile(path, 'utf8');
  const lines = source.split('\n');
  lines.forEach((line, index) => {
    if (line.includes('emitOnDiagnostic') && sensitive.test(line)) {
      throw new Error(`${path}:${index + 1} diagnostic contains a sensitive field`);
    }
  });
}

console.log('Diagnostic safety smoke passed: native diagnostics contain no credential, private-key, or payload fields.');
