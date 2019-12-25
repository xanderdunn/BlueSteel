// swift-tools-version:5.1
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "BlueSteel",
    platforms: [.iOS("11.3"), .macOS("10.13"), .watchOS("4.3")],
    products: [
        .library(
            name: "BlueSteel",
            targets: ["BlueSteel"]),
    ],
    targets: [
        .target(
            name: "BlueSteel",
            dependencies: []),
        .testTarget(
            name: "BlueSteelTests",
            dependencies: ["BlueSteel"]),
    ]
)
