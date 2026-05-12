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
            checksum: "e27cb910510a3444978f5d682e47084ffcabc0bcf9f375773a0fc74625b880e0"
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
