# OxiDB iOS Demo

A SwiftUI demo app that uses OxiDB. Supports two modes:

- **Embedded mode** — database runs in-process, no server needed (recommended for mobile)
- **Client mode** — connects to a remote OxiDB server over TCP

## Prerequisites

- Xcode 15+
- Rust toolchain with iOS target: `rustup target add aarch64-apple-ios`
- For client mode: a running OxiDB server (see main project README)

## Build Steps

### 1. Build the FFI Libraries for iOS

```bash
# From the project root:

# Embedded FFI (recommended — no server needed)
cargo build --release -p oxidb-embedded-ffi --target aarch64-apple-ios

# Client FFI (for TCP server mode)
cargo build --release -p oxidb-client-ffi --target aarch64-apple-ios
```

This produces static libraries at:
```
target/aarch64-apple-ios/release/liboxidb_embedded_ffi.a
target/aarch64-apple-ios/release/liboxidb_client_ffi.a
```

For the iOS Simulator (Apple Silicon Mac):
```bash
cargo build --release -p oxidb-embedded-ffi --target aarch64-apple-ios-sim
cargo build --release -p oxidb-client-ffi --target aarch64-apple-ios-sim
```

### 2. Set Up the Xcode Project

1. Open Xcode and create a new iOS App project in this directory, or open the existing one.

2. **Add the static libraries:**
   - Go to your target's Build Settings
   - Under "Library Search Paths", add the path to the built libraries:
     ```
     $(PROJECT_DIR)/../../../../target/aarch64-apple-ios/release
     ```
   - Under "Other Linker Flags", add:
     ```
     -loxidb_embedded_ffi -loxidb_client_ffi
     ```

3. **Add the bridging header or C module:**
   - Copy the headers into your project:
     - `oxidb-embedded-ffi/include/oxidb_embedded.h` (for embedded mode)
     - `oxidb-client-ffi/include/oxidb.h` (for client mode)
   - Either create a bridging header that includes them:
     ```c
     #include "oxidb_embedded.h"
     #include "oxidb.h"
     ```
   - Or add the Swift package `swift/OxiDB` as a local dependency

4. **Add the OxiDB Swift package** (recommended):
   - In Xcode: File > Add Package Dependencies > Add Local
   - Navigate to `swift/OxiDB/` and add it
   - This provides the `OxiDB` module with both `OxiDBDatabase` and `OxiDBClient`

### 3. Build and Run

#### Embedded mode (no server needed):

Build and run the iOS app. Use `OxiDBDatabase.open(path:)` with a path in the app's documents directory:

```swift
import OxiDB

let docsDir = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask).first!
let dbPath = docsDir.appendingPathComponent("mydb").path
let db = try OxiDBDatabase.open(path: dbPath)
```

#### Client mode (TCP server):

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
cargo build --release -p oxidb-embedded-ffi --target aarch64-apple-ios
cargo build --release -p oxidb-embedded-ffi --target aarch64-apple-ios-sim
cargo build --release -p oxidb-client-ffi --target aarch64-apple-ios
cargo build --release -p oxidb-client-ffi --target aarch64-apple-ios-sim

# Create universal libraries with lipo
lipo -create \
  target/aarch64-apple-ios/release/liboxidb_embedded_ffi.a \
  target/aarch64-apple-ios-sim/release/liboxidb_embedded_ffi.a \
  -output liboxidb_embedded_ffi_universal.a

lipo -create \
  target/aarch64-apple-ios/release/liboxidb_client_ffi.a \
  target/aarch64-apple-ios-sim/release/liboxidb_client_ffi.a \
  -output liboxidb_client_ffi_universal.a
```

Or create XCFrameworks:

```bash
xcodebuild -create-xcframework \
  -library target/aarch64-apple-ios/release/liboxidb_embedded_ffi.a \
  -headers oxidb-embedded-ffi/include/ \
  -library target/aarch64-apple-ios-sim/release/liboxidb_embedded_ffi.a \
  -headers oxidb-embedded-ffi/include/ \
  -output OxiDBEmbeddedFFI.xcframework

xcodebuild -create-xcframework \
  -library target/aarch64-apple-ios/release/liboxidb_client_ffi.a \
  -headers oxidb-client-ffi/include/ \
  -library target/aarch64-apple-ios-sim/release/liboxidb_client_ffi.a \
  -headers oxidb-client-ffi/include/ \
  -output OxiDBClientFFI.xcframework
```
