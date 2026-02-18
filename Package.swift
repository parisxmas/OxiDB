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
            name: "COxiDBEmbedded",
            url: "https://github.com/parisxmas/OxiDB/releases/download/v0.7.0/OxiDBEmbedded.xcframework.zip",
            checksum: "cdba5f1d4305173d85bae6df05bfebb41337f74af7702e55914e1fe7e23e54a5"
        ),
        .target(
            name: "OxiDB",
            dependencies: ["COxiDBEmbedded"],
            path: "swift/OxiDB/Sources/OxiDB",
            exclude: ["include"]
        ),
    ]
)
