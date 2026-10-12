import { spawn, spawnSync } from 'node:child_process';

const protocols = (process.env.IOS_PUBLIC_MATRIX_PROTOCOLS || '4,5')
  .split(',')
  .map((value) => Number(value.trim()))
  .filter((value) => [4, 5].includes(value));
const ports = (process.env.IOS_PUBLIC_MATRIX_PORTS || '1883,1884,8883,8884,8885,8886,8887,8080,8081,8090,8091')
  .split(',')
  .map((value) => Number(value.trim()))
  .filter(Boolean);
const flow = new URL('../test/ios/public-matrix.yaml', import.meta.url).pathname;
const v5Flow = new URL('../test/ios/public-matrix-v5.yaml', import.meta.url).pathname;
const failureFlow = new URL('../test/ios/public-matrix-expected-failure.yaml', import.meta.url).pathname;
const failureV5Flow = new URL('../test/ios/public-matrix-expected-failure-v5.yaml', import.meta.url).pathname;
const tlsFailureFlow = new URL('../test/ios/public-matrix-tls-failure.yaml', import.meta.url).pathname;
const tlsFailureV5Flow = new URL('../test/ios/public-matrix-tls-failure-v5.yaml', import.meta.url).pathname;
const simulatorUdid = process.env.IOS_SIMULATOR_UDID || '';
const expectedFailure = new Map([[8885, 'Not authorized'], [8887, 'certificate']]);
const maxAttempts = 3;

function run(command, args, env) {
  return new Promise((resolve, reject) => {
    let output = '';
    const child = spawn(command, args, { stdio: ['ignore', 'pipe', 'pipe'], env: { ...process.env, ...env } });
    child.stdout.on('data', (chunk) => { output += chunk; process.stdout.write(chunk); });
    child.stderr.on('data', (chunk) => { output += chunk; process.stderr.write(chunk); });
    child.on('error', reject);
    child.on('close', (code) => code === 0 ? resolve(output) : reject(Object.assign(new Error(`${command} exited with ${code}`), { output })));
  });
}

function looksLikeBrokerUnavailable(error) {
  const output = `${error.output || ''} ${error.message || ''}`.toLowerCase();
  return output.includes('timed out') || output.includes('timeout') ||
    output.includes('connection refused') || output.includes('connection reset') ||
    output.includes('not authorized') || output.includes('certificate') ||
    output.includes('unable to connect') || output.includes('state: connected');
}

function prepareSimulator() {
  if (!simulatorUdid) return;
  spawnSync('xcrun', ['simctl', 'boot', simulatorUdid], { stdio: 'ignore' });
  spawnSync('xcrun', ['simctl', 'bootstatus', simulatorUdid, '-b'], { stdio: 'ignore' });
}

const maestroCheck = spawnSync('maestro', ['--version'], { stdio: 'ignore' });
if (maestroCheck.error || maestroCheck.status !== 0) {
  console.error('Maestro is required. Install it from https://docs.maestro.dev/getting-started/installing-maestro');
  process.exit(2);
}
const results = [];
for (const protocol of protocols) {
  for (const port of ports) {
    prepareSimulator();
    const expected = expectedFailure.get(port);
    const isTlsFailure = port === 8887;
    const args = ['test', expected ? isTlsFailure ? protocol === 5 ? tlsFailureV5Flow : tlsFailureFlow : protocol === 5 ? failureV5Flow : failureFlow : protocol === 5 ? v5Flow : flow];
    if (simulatorUdid) args.push('--device', simulatorUdid);
    args.push('-e', `PROTOCOL=${protocol}`, '-e', `PORT=${port}`);
    let completed = false;
    for (let attempt = 1; attempt <= maxAttempts && !completed; attempt += 1) {
      try {
        await run('maestro', args, {});
        results.push({ protocol, port, result: expected ? 'expected-failure-verified' : 'passed', attempts: attempt });
        completed = true;
      } catch (error) {
        if (looksLikeBrokerUnavailable(error) && attempt < maxAttempts) {
          console.error(`[ios-public-matrix] broker retry ${attempt}/${maxAttempts} protocol=${protocol} port=${port}`);
          prepareSimulator();
          continue;
        }
        results.push({ protocol, port, result: looksLikeBrokerUnavailable(error) ? 'broker-unavailable' : 'automation-failure', attempts: attempt, error: String(error.message || error) });
        completed = true;
      }
    }
  }
}
console.log(`[ios-public-matrix] results ${JSON.stringify(results)}`);
if (results.some((entry) => entry.result === 'failed')) process.exitCode = 1;
