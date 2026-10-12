const { withDangerousMod } = require('expo/config-plugins');
const fs = require('node:fs');
const path = require('node:path');

module.exports = function withMqttPodfile(config) {
  config = withDangerousMod(config, [
    'ios',
    async (modConfig) => {
      const podfile = path.join(
        modConfig.modRequest.platformProjectRoot,
        'Podfile'
      );
      let contents = fs.readFileSync(podfile, 'utf8');
      const modularDependency =
        "pod 'MqttCocoaAsyncSocket', :modular_headers => true";
      if (!contents.includes(modularDependency)) {
        const marker =
          'require File.join(File.dirname(`node --print "require.resolve(\'expo/package.json\')"`), "scripts/autolinking")';
        if (!contents.includes(marker)) {
          throw new Error(
            'Expo Podfile marker for modular headers was not found'
          );
        }
        contents = contents.replace(marker, `${marker}\n${modularDependency}`);
        fs.writeFileSync(podfile, contents);
      }
      return modConfig;
    },
  ]);
  return withDangerousMod(config, [
    'android',
    async (modConfig) => {
      const gradle = path.join(
        modConfig.modRequest.platformProjectRoot,
        'app',
        'build.gradle'
      );
      let contents = fs.readFileSync(gradle, 'utf8');
      const block = `\n    compileOptions {\n        coreLibraryDesugaringEnabled true\n    }\n\n    packagingOptions {\n        resources {\n            excludes += ["META-INF/INDEX.LIST", "bundle.properties"]\n        }\n    }`;
      if (
        !contents.includes(
          'excludes += ["META-INF/INDEX.LIST", "bundle.properties"]'
        )
      ) {
        const marker = 'android {';
        if (!contents.includes(marker)) {
          throw new Error('Expo app Gradle android block was not found');
        }
        contents = contents.replace(marker, `${marker}${block}`);
        fs.writeFileSync(gradle, contents);
      }
      return modConfig;
    },
  ]);
};
