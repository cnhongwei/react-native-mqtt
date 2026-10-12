import { readFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { spawnSync } from 'node:child_process';

const root = process.cwd();
const projects = [
  { name: 'RN example', directory: 'example/android', buildFile: 'app/build.gradle' },
  { name: 'Expo example', directory: 'examples/expo/android', buildFile: 'app/build.gradle' },
];

for (const project of projects) {
  const directory = resolve(root, project.directory);
  const buildFile = readFileSync(join(directory, project.buildFile), 'utf8');
  if (!buildFile.includes('coreLibraryDesugaringEnabled true')) {
    throw new Error(`${project.name} does not enable core library desugaring`);
  }
  if (!buildFile.includes('com.android.tools:desugar_jdk_libs:2.1.5')) {
    throw new Error(`${project.name} does not pin desugar_jdk_libs 2.1.5`);
  }

  const result = spawnSync('./gradlew', [':app:assembleDebug', '--no-daemon', '--console=plain'], {
    cwd: directory,
    encoding: 'utf8',
    env: { ...process.env, CI: '1' },
  });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    throw new Error(`${project.name} desugaring build failed:\n${result.stdout}\n${result.stderr}`);
  }
  if (!`${result.stdout}\n${result.stderr}`.includes('l8DexDesugarLibDebug')) {
    throw new Error(`${project.name} did not execute the L8 desugaring task`);
  }
  console.log(`${project.name} coreLibraryDesugaring smoke passed`);
}
