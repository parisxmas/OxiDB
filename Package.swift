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
            url: "https://github.com/parisxmas/OxiDB/releases/download/v0.7.1/OxiDBEmbedded.xcframework.zip",
            checksum: "29f9e2bc973ec0fc67df9f74a7d40863c17b9bc2b3f51882a819910e02df2930"
        ),
        .target(
            name: "OxiDB",
            dependencies: ["COxiDBEmbedded"],
            path: "swift/OxiDB/Sources/OxiDB",
            exclude: ["include"]
        ),
    ]
)
