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
            url: "https://github.com/parisxmas/OxiDB/releases/download/v0.9.0/OxiDBEmbedded.xcframework.zip",
            checksum: "5ef8ed5e4f2ae0fe82933fd0527fc09a5c18d43eb981a02bf27a2f0c0070bb8d"
        ),
        .target(
            name: "OxiDB",
            dependencies: ["COxiDBEmbedded"],
            path: "swift/OxiDB/Sources/OxiDB",
            exclude: ["include"]
        ),
    ]
)
