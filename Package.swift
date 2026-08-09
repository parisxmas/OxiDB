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
            url: "https://github.com/parisxmas/OxiDB/releases/download/v0.43.0/OxiDBEmbedded.xcframework.zip",
            checksum: "32f0c3324e16bc3edee9a530bad0fdb832b4a8d9ee83223046c39471c280a966"
        ),
        .target(
            name: "COxiDBEmbedded",
            dependencies: ["OxiDBEmbeddedBinary"],
            path: "clients/swift/COxiDBEmbedded",
            publicHeadersPath: "include"
        ),
        .target(
            name: "OxiDB",
            dependencies: ["COxiDBEmbedded"],
            path: "clients/swift/OxiDB/Sources/OxiDB",
            exclude: ["include"]
        ),
    ]
)
