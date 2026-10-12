import { readFile } from 'node:fs/promises';
import { existsSync } from 'node:fs';

const libraryManifestPath = 'android/src/main/AndroidManifest.xml';
const mergedManifestPath =
  'example/android/app/build/intermediates/merged_manifests/release/processReleaseManifest/AndroidManifest.xml';
const forbiddenLibraryElements = /<(?:service|receiver|provider)\b|android\.permission\.(?:FOREGROUND_SERVICE|SCHEDULE_EXACT_ALARM|WAKE_LOCK)\b/i;
const forbiddenMergedPermissions = /android\.permission\.(?:FOREGROUND_SERVICE|SCHEDULE_EXACT_ALARM|WAKE_LOCK)\b/i;

const libraryManifest = await readFile(libraryManifestPath, 'utf8');
if (forbiddenLibraryElements.test(libraryManifest)) {
  throw new Error(`${libraryManifestPath} declares a forbidden background component or permission`);
}

if (!existsSync(mergedManifestPath)) {
  throw new Error(`missing ${mergedManifestPath}; build the Android release consumer first`);
}
const mergedManifest = await readFile(mergedManifestPath, 'utf8');
if (forbiddenMergedPermissions.test(mergedManifest)) {
  throw new Error(`${mergedManifestPath} contains a forbidden background permission`);
}

console.log('Manifest audit passed: library and merged release manifest add no service, foreground-service, exact-alarm, or wake-lock capability.');
