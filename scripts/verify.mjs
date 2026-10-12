import { spawnSync } from 'node:child_process';

const commands = [
  ['typecheck'],
  ['lint'],
  ['test:diagnostics'],
  ['test:scenarios'],
  ['test:delivery-benchmark'],
  ['test:examples'],
  ['test', '--runInBand', '--detectOpenHandles'],
  ['test:package'],
];
for (const command of commands) {
  const result = spawnSync('yarn', command, { stdio: 'inherit' });
  if (result.status !== 0) {
    console.error(`Verification stopped at yarn ${command.join(' ')}`);
    process.exit(result.status ?? 1);
  }
}
console.log('JS and package scaffold verification passed. Native verification remains environment-gated.');
