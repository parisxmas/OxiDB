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
            url: "https://github.com/parisxmas/OxiDB/releases/download/v0.13.0/OxiDBEmbedded.xcframework.zip",
            checksum: "9ad71735539cdd85f75b125fc5ec3c71577b74213ba500486811f12d1a0ba9d1"
        ),
        .target(
            name: "OxiDB",
            dependencies: ["COxiDBEmbedded"],
            path: "swift/OxiDB/Sources/OxiDB",
            exclude: ["include"]
        ),
    ]
)
