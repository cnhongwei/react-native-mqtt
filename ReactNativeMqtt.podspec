require "json"

package = JSON.parse(File.read(File.join(__dir__, "package.json")))

Pod::Spec.new do |s|
  s.name         = "ReactNativeMqtt"
  s.version      = package["version"]
  s.summary      = package["description"]
  s.homepage     = package["homepage"]
  s.license      = package["license"]
  s.authors      = package["author"]

  s.platforms    = { :ios => min_ios_version_supported }
  s.swift_version = "5.7"
  s.source       = { :git => "https://github.com/cnhongwei/react-native-mqtt.git", :tag => "#{s.version}" }

  s.source_files = "ios/**/*.{h,m,mm,swift,cpp}"
  s.exclude_files = "ios/Tests/**/*"
  s.private_header_files = "ios/**/*.h"

  install_modules_dependencies(s)
  s.dependency "CocoaMQTT/WebSockets", "2.4.0"

  s.test_spec "NativeSessionScenarios" do |test_spec|
    test_spec.source_files = "ios/Tests/**/*.swift"
    test_spec.requires_app_host = false
  end
end
