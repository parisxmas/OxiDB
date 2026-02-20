# OxiDB Swift Client

A Swift wrapper for OxiDB. Supports two modes:

- **`OxiDBClient`** — connects to an OxiDB server over TCP (requires a running server)
- **`OxiDBDatabase`** — embedded, in-process database (no server needed)

Both classes share the same API surface and error types.

## Requirements

- Swift 5.9+
- macOS 13+ / iOS 16+

For `OxiDBClient` (server mode):
- The `liboxidb_client_ffi` shared library (`.dylib` on macOS, `.a` for iOS)

For `OxiDBDatabase` (embedded mode):
- The `liboxidb_embedded_ffi` library (`.dylib`/`.a`)

## Getting the FFI Libraries

### Prebuilt Binaries (no Rust needed)

Download from the [latest release](https://github.com/parisxmas/OxiDB/releases/latest):

**Embedded FFI** (`OxiDBDatabase` — recommended for mobile):

```bash
# macOS arm64 (Apple Silicon)
curl -LO https://github.com/parisxmas/OxiDB/releases/download/v0.13.0/oxidb-embedded-ffi-macos-arm64.tar.gz
tar xzf oxidb-embedded-ffi-macos-arm64.tar.gz
sudo cp liboxidb_embedded_ffi.dylib liboxidb_embedded_ffi.a /usr/local/lib/
sudo cp oxidb_embedded.h /usr/local/include/

# iOS device (arm64)
curl -LO https://github.com/parisxmas/OxiDB/releases/download/v0.13.0/oxidb-embedded-ffi-ios-arm64.tar.gz
tar xzf oxidb-embedded-ffi-ios-arm64.tar.gz

# iOS simulator (arm64, Apple Silicon Mac)
curl -LO https://github.com/parisxmas/OxiDB/releases/download/v0.13.0/oxidb-embedded-ffi-ios-sim-arm64.tar.gz
tar xzf oxidb-embedded-ffi-ios-sim-arm64.tar.gz
```

**Client FFI** (`OxiDBClient` — TCP server mode):

```bash
# macOS arm64 (Apple Silicon)
curl -LO https://github.com/parisxmas/OxiDB/releases/download/v0.13.0/oxidb-client-ffi-macos-arm64.tar.gz
tar xzf oxidb-client-ffi-macos-arm64.tar.gz
sudo cp liboxidb_client_ffi.dylib /usr/local/lib/
sudo cp oxidb.h /usr/local/include/
```

### Build from Source

```bash
# From the project root:

# Client FFI (for OxiDBClient — TCP mode)
cargo build --release -p oxidb-client-ffi

# Embedded FFI (for OxiDBDatabase — in-process mode)
cargo build --release -p oxidb-embedded-ffi

# The libraries will be at:
# target/release/liboxidb_client_ffi.dylib   (macOS)
# target/release/liboxidb_embedded_ffi.dylib (macOS)

# For iOS:
cargo build --release -p oxidb-embedded-ffi --target aarch64-apple-ios
# -> target/aarch64-apple-ios/release/liboxidb_embedded_ffi.a
```

## Installation

### Swift Package Manager (recommended)

**From Xcode:** File → Add Package Dependencies → Enter the URL:

```
https://github.com/parisxmas/OxiDB.git
```

Xcode will automatically download the prebuilt embedded library (XCFramework). No Rust toolchain or manual library setup needed.

**From `Package.swift`:**

```swift
dependencies: [
    .package(url: "https://github.com/parisxmas/OxiDB.git", from: "0.13.0")
]
```

Then add `"OxiDB"` to your target's dependencies.

### Local Development (from source)

If you prefer to build from source or need the TCP client mode:

```swift
dependencies: [
    .package(path: "../swift/OxiDB")  // adjust path as needed
]
```

Make sure the FFI libraries are findable by the linker. Either:
- Install them to `/usr/local/lib/`
- Or pass the library search path: `swift build -Xlinker -L/path/to/target/release`

## Usage

### Embedded Mode (no server needed)

```swift
import OxiDB

// Open a database at a directory path
let db = try OxiDBDatabase.open(path: "/path/to/mydb")

// Or with encryption
let db = try OxiDBDatabase.open(path: "/path/to/mydb", encryptionKeyPath: "/path/to/key")

// Insert a document
try db.insert(collection: "users", document: [
    "name": "Alice",
    "age": 30,
    "email": "alice@example.com"
])

// Query
let users = try db.find(collection: "users", query: ["age": ["$gte": 25]])
print("Found \(users.count) users")

// Update
try db.update(
    collection: "users",
    query: ["name": "Alice"],
    update: ["$set": ["age": 31]]
)

// Delete
try db.delete(collection: "users", query: ["name": "Alice"])

// Indexes
try db.createIndex(collection: "users", field: "email")
try db.createCompositeIndex(collection: "users", fields: ["city", "age"])

// Full-text search on documents
try db.createTextIndex(collection: "articles", fields: ["title", "body"])
let results = try db.textSearch(collection: "articles", query: "rust programming", limit: 10)

// Aggregation
let result = try db.aggregate(collection: "users", pipeline: [
    ["$group": ["_id": "city", "count": ["$count": true]]],
    ["$sort": ["count": -1]]
])

// Transactions (auto-rollback on error)
try db.transaction {
    try db.insert(collection: "ledger", document: ["from": "A", "to": "B", "amount": 100])
    try db.insert(collection: "ledger", document: ["from": "B", "to": "C", "amount": 50])
}

// Blob storage
try db.createBucket("files")
let data = Data("Hello, World!".utf8).base64EncodedString()
try db.putObject(bucket: "files", key: "greeting.txt", dataBase64: data, contentType: "text/plain")
let obj = try db.getObject(bucket: "files", key: "greeting.txt")

// Full-text search
let results = try db.search(query: "hello world", limit: 10)

// Close (also called automatically in deinit)
db.close()
```

### Client Mode (TCP server)

```swift
import OxiDB

// Connect
let client = try OxiDBClient.connect(host: "127.0.0.1", port: 4444)

// Insert a document
try client.insert(collection: "users", document: [
    "name": "Alice",
    "age": 30,
    "email": "alice@example.com"
])

// Query
let users = try client.find(collection: "users", query: ["age": ["$gte": 25]])
print("Found \(users.count) users")

// Update
try client.update(
    collection: "users",
    query: ["name": "Alice"],
    update: ["$set": ["age": 31]]
)

// Delete
try client.delete(collection: "users", query: ["name": "Alice"])

// Indexes
try client.createIndex(collection: "users", field: "email")
try client.createCompositeIndex(collection: "users", fields: ["city", "age"])

// Aggregation
let result = try client.aggregate(collection: "users", pipeline: [
    ["$group": ["_id": "city", "count": ["$count": true]]],
    ["$sort": ["count": -1]]
])

// Transactions (auto-rollback on error)
try client.transaction {
    try client.insert(collection: "ledger", document: ["from": "A", "to": "B", "amount": 100])
    try client.insert(collection: "ledger", document: ["from": "B", "to": "C", "amount": 50])
}

// Blob storage
try client.createBucket("files")
let data = Data("Hello, World!".utf8).base64EncodedString()
try client.putObject(bucket: "files", key: "greeting.txt", dataBase64: data, contentType: "text/plain")
let obj = try client.getObject(bucket: "files", key: "greeting.txt")

// Full-text search
let results = try client.search(query: "hello world", limit: 10)

// Disconnect (also called automatically in deinit)
client.disconnect()
```

## Watching (Mutation Events)

The Swift SDK supports **local mutation watching** on both `OxiDBClient` and `OxiDBDatabase`.

- Local means events are emitted for operations executed through that Swift instance.
- Event shape:
  - `operation: OxiDBMutationOperation` (`insert`, `insert_many`, `update`, `delete`, `commit_tx`)
  - `collection: String?`
  - `timestamp: Date`
  - `metadata: [String: Any]`

### Option 1: Callback Observer

```swift
import OxiDB

let db = try OxiDBDatabase.open(path: "/path/to/mydb")

let observerID = db.addMutationObserver { event in
    guard event.collection == "users" else { return }
    print("WATCH \(event.operation.rawValue) collection=\(event.collection ?? "-")")
    print("metadata=\(event.metadata)")
}

// ... run writes (insert/update/delete) ...

db.removeMutationObserver(observerID)
```

### Option 2: AsyncStream

```swift
import OxiDB

let db = try OxiDBDatabase.open(path: "/path/to/mydb")

let task = Task {
    for await event in db.mutationEvents() {
        if event.collection == "users" {
            print("async WATCH -> \(event.operation.rawValue)")
        }
    }
}

// later
task.cancel()
```

### Rule-Based Watch Example (path changes)

The SDK provides mutation events; you can build higher-level watch rules on top.
Example: trigger when `users.city` is updated.

```swift
import OxiDB

func pathWasUpdated(_ path: String, metadata: [String: Any]) -> Bool {
    guard let update = metadata["update"] as? [String: Any] else { return false }
    for (_, payload) in update {
        guard let fields = payload as? [String: Any] else { continue }
        for changedPath in fields.keys {
            if changedPath == path || changedPath.hasPrefix(path + ".") || path.hasPrefix(changedPath + ".") {
                return true
            }
        }
    }
    return false
}

let db = try OxiDBDatabase.open(path: "/path/to/mydb")
let id = db.addMutationObserver { event in
    guard event.collection == "users", event.operation == .update else { return }
    if pathWasUpdated("city", metadata: event.metadata) {
        print("WATCH TRIGGER: users.city changed")
    }
}

// ... use db ...
db.removeMutationObserver(id)
```

Notes:
- In embedded mode (`OxiDBDatabase`), events are emitted from successful `execute(_:)` commands.
- In client mode (`OxiDBClient`), events are emitted from successful SDK write calls.
- For app-level persistent watch rules (e.g. stored in `_config`), persist your rule docs and evaluate them inside your observer callback.

## Error Handling

```swift
do {
    let result = try db.find(collection: "users", query: [:])
} catch OxiDBError.databaseOpenFailed {
    print("Failed to open database")
} catch OxiDBError.connectionFailed {
    print("Not connected (client mode)")
} catch OxiDBError.operationFailed(let msg) {
    print("Operation failed: \(msg)")
} catch OxiDBError.transactionConflict(let msg) {
    print("Transaction conflict: \(msg)")
}
```

## API Reference

### Embedded Database (`OxiDBDatabase`)
- `OxiDBDatabase.open(path:)` - Open embedded database
- `OxiDBDatabase.open(path:encryptionKeyPath:)` - Open with encryption
- `close()` - Close database
- `execute(_:)` - Execute a raw JSON command dictionary

### Client Connection (`OxiDBClient`)
- `OxiDBClient.connect(host:port:)` - Connect to server
- `disconnect()` - Close connection
- `ping()` - Ping server

### CRUD (both classes)
- `insert(collection:document:)` - Insert one document
- `insertMany(collection:documents:)` - Insert multiple documents
- `find(collection:query:)` - Find documents
- `findOne(collection:query:)` - Find one document
- `update(collection:query:update:)` - Update documents
- `updateOne(collection:query:update:)` - Update one document (embedded only)
- `delete(collection:query:)` - Delete documents
- `deleteOne(collection:query:)` - Delete one document (embedded only)
- `count(collection:)` - Count documents

### Indexes (both classes)
- `createIndex(collection:field:)` - Single-field index
- `createUniqueIndex(collection:field:)` - Unique index (embedded only)
- `createCompositeIndex(collection:fields:)` - Multi-field index
- `createTextIndex(collection:fields:)` - Full-text search index (embedded only)
- `textSearch(collection:query:limit:)` - Search documents by text (embedded only)

### Collections (both classes)
- `createCollection(_:)` - Create collection explicitly (embedded only)
- `listCollections()` - List all collections
- `dropCollection(_:)` - Drop a collection
- `compact(collection:)` - Compact a collection (embedded only)

### Aggregation (both classes)
- `aggregate(collection:pipeline:)` - Run aggregation pipeline

### Transactions (both classes)
- `beginTransaction()` / `commitTransaction()` / `rollbackTransaction()` - Manual control
- `transaction(_:)` - Auto-commit/rollback block

### Blob Storage (both classes)
- `createBucket(_:)` / `listBuckets()` / `deleteBucket(_:)` - Bucket management
- `putObject(bucket:key:dataBase64:contentType:metadata:)` - Upload
- `getObject(bucket:key:)` / `headObject(bucket:key:)` - Download / metadata
- `deleteObject(bucket:key:)` / `listObjects(bucket:prefix:limit:)` - Delete / list

### Full-Text Search (both classes)
- `search(query:bucket:limit:)` - Search indexed content

### Mutation Watching (both classes)
- `addMutationObserver(_:)` - Register callback observer
- `removeMutationObserver(_:)` - Remove observer by UUID
- `mutationEvents()` - Async stream of mutation events
