# OxiDB iOS Demo

A SwiftUI demo app that connects to an OxiDB server using the C FFI client library.

## Prerequisites

- Xcode 15+
- Rust toolchain with iOS target: `rustup target add aarch64-apple-ios`
- A running OxiDB server (see main project README)

## Build Steps

### 1. Build the FFI Library for iOS

```bash
# From the project root:
cargo build --release -p oxidb-client-ffi --target aarch64-apple-ios
```

This produces a static library at:
```
target/aarch64-apple-ios/release/liboxidb_client_ffi.a
```

For the iOS Simulator (Apple Silicon Mac):
```bash
cargo build --release -p oxidb-client-ffi --target aarch64-apple-ios-sim
```

### 2. Set Up the Xcode Project

1. Open Xcode and create a new iOS App project in this directory, or open the existing one.

2. **Add the static library:**
   - Go to your target's Build Settings
   - Under "Library Search Paths", add the path to the built library:
     ```
     $(PROJECT_DIR)/../../../../target/aarch64-apple-ios/release
     ```
   - Under "Other Linker Flags", add:
     ```
     -loxidb_client_ffi
     ```

3. **Add the bridging header or C module:**
   - Copy `oxidb-client-ffi/include/oxidb.h` into your project
   - Either create a bridging header that includes it:
     ```c
     #include "oxidb.h"
     ```
   - Or add the Swift package `swift/OxiDB` as a local dependency

4. **Add the OxiDB Swift package** (recommended):
   - In Xcode: File > Add Package Dependencies > Add Local
   - Navigate to `swift/OxiDB/` and add it
   - This provides the `OxiDB` module with the full Swift wrapper

### 3. Build and Run

1. Start an OxiDB server on your development machine:
   ```bash
   cargo run --release -p oxidb-server
   ```

2. Build and run the iOS app in the simulator or on a device.

3. In the app, enter your server's host and port (default: `127.0.0.1:4444`) and tap Connect.

> **Note:** When running on a physical device, use your Mac's local IP address instead of `127.0.0.1`. When running on the simulator, `127.0.0.1` connects to the host Mac.

## Features Demonstrated

- **Connect/Disconnect** to an OxiDB server
- **Insert** sample documents into a collection
- **Query** documents with filter criteria
- **Count** documents in a collection
- **Create indexes** on fields
- **Aggregation** pipeline execution
- **Transactions** with auto-rollback
- **List collections** on the server

## Creating a Universal (Fat) Library

To support both device and simulator:

```bash
# Build for both targets
cargo build --release -p oxidb-client-ffi --target aarch64-apple-ios
cargo build --release -p oxidb-client-ffi --target aarch64-apple-ios-sim

# Create a universal library with lipo
lipo -create \
  target/aarch64-apple-ios/release/liboxidb_client_ffi.a \
  target/aarch64-apple-ios-sim/release/liboxidb_client_ffi.a \
  -output liboxidb_client_ffi_universal.a
```

Or create an XCFramework:

```bash
xcodebuild -create-xcframework \
  -library target/aarch64-apple-ios/release/liboxidb_client_ffi.a \
  -headers oxidb-client-ffi/include/ \
  -library target/aarch64-apple-ios-sim/release/liboxidb_client_ffi.a \
  -headers oxidb-client-ffi/include/ \
  -output OxiDBClientFFI.xcframework
```
