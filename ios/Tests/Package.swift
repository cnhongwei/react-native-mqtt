// swift-tools-version: 5.7

// Test-only package manifest. The directory is excluded from the production Pod.
// It gives CI and local validation a real executable XCTest entry point without
// adding an application target to the consumer example.

import PackageDescription

let package = Package(
  name: "ReactNativeMqttNativeSessionScenarios",
  platforms: [.iOS(.v15)],
  targets: [
    .testTarget(
      name: "NativeSessionScenarioTests",
      path: ".",
      exclude: ["Package.swift"]
    ),
  ]
)
