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
            url: "https://github.com/parisxmas/OxiDB/releases/download/v0.22.0/OxiDBEmbedded.xcframework.zip",
            checksum: "900537ec6badaf6649580dca947be11cb3e78ec84d7bded08a26890a618dd698"
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
