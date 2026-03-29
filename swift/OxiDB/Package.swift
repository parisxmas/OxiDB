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
            url: "https://github.com/parisxmas/OxiDB/releases/download/v0.22.0/OxiDBEmbedded.xcframework.zip",
            checksum: "6979e8314d7c928d11a287a72d93ec8b3250d0ed6ab8d248745e18cf2c9c4c6c"
        ),
        .target(
            name: "OxiDB",
            dependencies: ["COxiDBEmbedded"],
            path: "Sources/OxiDB",
            exclude: ["include"]
        ),
    ]
)
