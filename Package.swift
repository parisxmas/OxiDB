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
            url: "https://github.com/parisxmas/OxiDB/releases/download/v0.42.12/OxiDBEmbedded.xcframework.zip",
            checksum: "8a56f3ceecb880c73014514bf8761f7a9b1ed0b3f2849d9f7e3211339a89542a"
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
