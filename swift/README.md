# OxiDB Swift Client

A Swift wrapper for OxiDB using the C FFI client library. Connects to an OxiDB server over TCP.

## Requirements

- Swift 5.9+
- macOS 13+ / iOS 16+
- The `liboxidb_client_ffi` shared library (`.dylib` on macOS, `.a` for iOS)

## Getting the FFI Library

### Prebuilt Binary (no Rust needed)

Download the prebuilt library from the [latest release](https://github.com/parisxmas/OxiDB/releases/latest):

```bash
# macOS arm64 (Apple Silicon)
curl -LO https://github.com/parisxmas/OxiDB/releases/download/v0.6.0/oxidb-client-ffi-macos-arm64.tar.gz
tar xzf oxidb-client-ffi-macos-arm64.tar.gz

# Install system-wide
sudo cp liboxidb_client_ffi.dylib /usr/local/lib/
sudo cp oxidb.h /usr/local/include/
```

### Build from Source

```bash
# From the project root:
cargo build --release -p oxidb-client-ffi

# The library will be at:
# target/release/liboxidb_client_ffi.dylib (macOS)
```

## Installation

### Swift Package Manager

Add to your `Package.swift`:

```swift
dependencies: [
    .package(path: "../swift/OxiDB")  // adjust path as needed
]
```

Make sure the FFI library is findable by the linker. Either:
- Install it to `/usr/local/lib/`
- Or pass the library search path: `swift build -Xlinker -L/path/to/target/release`

## Usage

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
    ["$group": ["_by": "city", "count": ["$count": true]]],
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

## Error Handling

```swift
do {
    let result = try client.find(collection: "users", query: [:])
} catch OxiDBError.connectionFailed {
    print("Not connected")
} catch OxiDBError.operationFailed(let msg) {
    print("Operation failed: \(msg)")
} catch OxiDBError.transactionConflict(let msg) {
    print("Transaction conflict: \(msg)")
}
```

## API Reference

### Connection
- `OxiDBClient.connect(host:port:)` - Connect to server
- `disconnect()` - Close connection
- `ping()` - Ping server

### CRUD
- `insert(collection:document:)` - Insert one document
- `insertMany(collection:documents:)` - Insert multiple documents
- `find(collection:query:)` - Find documents
- `findOne(collection:query:)` - Find one document
- `update(collection:query:update:)` - Update documents
- `delete(collection:query:)` - Delete documents
- `count(collection:)` - Count documents

### Indexes
- `createIndex(collection:field:)` - Single-field index
- `createCompositeIndex(collection:fields:)` - Multi-field index

### Collections
- `listCollections()` - List all collections
- `dropCollection(_:)` - Drop a collection

### Aggregation
- `aggregate(collection:pipeline:)` - Run aggregation pipeline

### Transactions
- `beginTransaction()` / `commitTransaction()` / `rollbackTransaction()` - Manual control
- `transaction(_:)` - Auto-commit/rollback block

### Blob Storage
- `createBucket(_:)` / `listBuckets()` / `deleteBucket(_:)` - Bucket management
- `putObject(bucket:key:dataBase64:contentType:metadata:)` - Upload
- `getObject(bucket:key:)` / `headObject(bucket:key:)` - Download / metadata
- `deleteObject(bucket:key:)` / `listObjects(bucket:prefix:limit:)` - Delete / list

### Full-Text Search
- `search(query:bucket:limit:)` - Search indexed content
