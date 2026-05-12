// swift-tools-version:5.9
import PackageDescription

let package = Package(
    name: "OxiDB",
    platforms: [
        .macOS(.v13),
        .iOS(.v16),
    ],
    products: [
        .library(name: "OxiDB", targets: ["OxiDB"]),
    ],
    targets: [
        .binaryTarget(
            name: "OxiDBEmbeddedBinary",
            url: "https://github.com/parisxmas/OxiDB/releases/download/v0.25.21/OxiDBEmbedded.xcframework.zip",
            checksum: "b1354b047bd0fde9b438439e3304ae9ee6d16de2ba5e6d9fabed22d4581a0d3c"
        ),
        .target(
            name: "COxiDBEmbedded",
            dependencies: ["OxiDBEmbeddedBinary"],
            path: "swift/COxiDBEmbedded",
            publicHeadersPath: "include"
        ),
        .target(
            name: "OxiDB",
            dependencies: ["COxiDBEmbedded"],
            path: "swift/OxiDB/Sources/OxiDB",
            exclude: ["include"]
        ),
    ]
)
