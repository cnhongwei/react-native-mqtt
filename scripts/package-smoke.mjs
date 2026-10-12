import { existsSync } from 'node:fs';
import { spawnSync } from 'node:child_process';

if (!existsSync('lib/module/index.js') || !existsSync('lib/typescript/src/index.d.ts')) {
  throw new Error('Build output missing; run yarn build first');
}
const result = spawnSync('yarn', ['pack', '--out', '/tmp/react-native-mqtt-0.1.0.tgz'], { stdio: 'inherit' });
if (result.error || result.status !== 0) throw result.error ?? new Error('yarn pack failed');

const archive = '/tmp/react-native-mqtt-0.1.0.tgz';
const listing = spawnSync('tar', ['-tzf', archive], { encoding: 'utf8' });
if (listing.error || listing.status !== 0) {
  throw listing.error ?? new Error('tar listing failed');
}
const forbidden = [
  /^package\/android\/src\/(test|androidTest)\//,
  /^package\/ios\/Tests\//,
  /^package\/(test|examples)\//,
];
const developmentFiles = listing.stdout
  .split('\n')
  .filter((entry) => forbidden.some((pattern) => pattern.test(entry)));
if (developmentFiles.length > 0) {
  throw new Error(`Development files are present in tarball:\n${developmentFiles.join('\n')}`);
}
console.log('Package smoke passed: development tests and example paths are excluded.');
