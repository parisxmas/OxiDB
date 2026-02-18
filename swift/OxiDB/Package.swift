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
        .systemLibrary(
            name: "COxiDB",
            path: "Sources/OxiDB/include",
            pkgConfig: nil,
            providers: nil
        ),
        .target(
            name: "OxiDB",
            dependencies: ["COxiDB"],
            path: "Sources/OxiDB",
            exclude: ["include"],
            linkerSettings: [
                .linkedLibrary("oxidb_client_ffi"),
                .linkedLibrary("oxidb_embedded_ffi"),
            ]
        ),
    ]
)
