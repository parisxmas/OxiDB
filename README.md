# OxiDB

A fast, embeddable document database engine written in Rust.

## Features

- **Append-only storage** with single-byte soft deletes
- **Write-ahead log (WAL)** with CRC32 checksums for crash safety
- **Field, unique, and composite indexes** backed by BTreeMap for range scans
- **Query operators**: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$exists`, `$and`, `$or`
- **Sort, skip, and limit** via `FindOptions`
- **Automatic date detection** — date strings are stored as `i64` millis for fast comparison
- **Compaction** to reclaim space from deleted documents
- **TCP server** with a length-prefixed JSON protocol
- **C FFI library** and **.NET client**
- **Thread-safe** — `RwLock` per collection, concurrent readers never block each other

## Quick Start

Add the dependency:

```toml
[dependencies]
oxidb = { path = "." }
serde_json = "1"
```

```rust
use oxidb::OxiDb;
use serde_json::json;

fn main() -> oxidb::Result<()> {
    let db = OxiDb::open(std::path::Path::new("./my_data"))?;

    // Insert
    let id = db.insert("users", json!({"name": "Alice", "age": 30}))?;

    // Find
    let docs = db.find("users", &json!({"age": {"$gte": 18}}))?;

    // Update
    let modified = db.update(
        "users",
        &json!({"name": "Alice"}),
        &json!({"$set": {"age": 31}}),
    )?;

    // Delete
    let deleted = db.delete("users", &json!({"age": {"$lt": 18}}))?;

    Ok(())
}
```

Collections are created implicitly on first insert.

## Query Operators

| Operator   | Example                                  | Description                |
|------------|------------------------------------------|----------------------------|
| `$eq`      | `{"status": "active"}`                   | Equality (implicit)        |
| `$ne`      | `{"status": {"$ne": "banned"}}`          | Not equal                  |
| `$gt`      | `{"age": {"$gt": 21}}`                   | Greater than               |
| `$gte`     | `{"age": {"$gte": 18}}`                  | Greater than or equal      |
| `$lt`      | `{"age": {"$lt": 65}}`                   | Less than                  |
| `$lte`     | `{"age": {"$lte": 100}}`                | Less than or equal         |
| `$in`      | `{"cat": {"$in": ["a", "b"]}}`           | Value in array             |
| `$exists`  | `{"email": {"$exists": true}}`           | Field exists / does not    |
| `$and`     | `{"$and": [{"a": 1}, {"b": 2}]}`        | Logical AND (explicit)     |
| `$or`      | `{"$or": [{"a": 1}, {"b": 2}]}`         | Logical OR                 |

Multiple conditions on different fields are implicitly ANDed:

```json
{"status": "active", "age": {"$gte": 18}}
```

## Sort, Skip, Limit

```rust
use oxidb::query::{FindOptions, SortOrder};

let opts = FindOptions {
    sort: Some(vec![("age".to_string(), SortOrder::Desc)]),
    skip: Some(10),
    limit: Some(5),
};
let page = db.find_with_options("users", &json!({}), &opts)?;
```

## Indexes

```rust
// Field index — speeds up equality and range queries
db.create_index("events", "status")?;

// Unique index — also enforces a uniqueness constraint
db.create_unique_index("users", "email")?;

// Composite index — multi-field, supports prefix scanning
db.create_composite_index("events", vec!["type".into(), "created_at".into()])?;
```

Indexes are automatically backfilled from existing documents and kept in sync on every insert, update, and delete.

## Compaction

Deleted documents are soft-deleted (status byte flipped) and remain on disk until compaction:

```rust
let stats = db.compact("users")?;
println!(
    "Reclaimed {} bytes, kept {} docs",
    stats.old_size - stats.new_size,
    stats.docs_kept,
);
```

Compaction rewrites the data file atomically (write to temp, fsync, rename) and rebuilds all indexes.

## TCP Server

### Running

```bash
# Defaults: 127.0.0.1:4444, data dir ./oxidb_data
cargo run --bin oxidb-server

# Custom address and data directory
OXIDB_ADDR=0.0.0.0:4444 OXIDB_DATA=/var/lib/oxidb cargo run --bin oxidb-server
```

### Protocol

Messages are length-prefixed JSON over TCP:

```
[u32 LE length][JSON bytes]
```

Max message size is 16 MiB.

**Request:**

```json
{"cmd": "find", "collection": "users", "query": {"age": {"$gte": 18}}, "limit": 10}
```

**Response:**

```json
{"ok": true, "data": [{"_id": 1, "name": "Alice", "age": 30}]}
```

### Commands

| Command                  | Fields                                             |
|--------------------------|----------------------------------------------------|
| `ping`                   | —                                                  |
| `insert`                 | `collection`, `doc`                                |
| `insert_many`            | `collection`, `docs`                               |
| `find`                   | `collection`, `query`, `sort?`, `skip?`, `limit?`  |
| `find_one`               | `collection`, `query`                              |
| `update`                 | `collection`, `query`, `update`                    |
| `delete`                 | `collection`, `query`                              |
| `count`                  | `collection`                                       |
| `create_index`           | `collection`, `field`                              |
| `create_unique_index`    | `collection`, `field`                              |
| `create_composite_index` | `collection`, `fields`                             |
| `create_collection`      | `collection`                                       |
| `list_collections`       | —                                                  |
| `drop_collection`        | `collection`                                       |
| `compact`                | `collection`                                       |

Sort values: `1` for ascending, `-1` for descending.

## C FFI

Build the shared library:

```bash
cargo build --release -p oxidb-client-ffi
# → liboxidb_client_ffi.dylib / .so / .dll
```

Key functions:

```c
OxiDbConn* oxidb_connect(const char* host, uint16_t port);
char*      oxidb_insert(OxiDbConn* conn, const char* collection, const char* doc_json);
char*      oxidb_find(OxiDbConn* conn, const char* collection, const char* query_json);
void       oxidb_free_string(char* ptr);
void       oxidb_disconnect(OxiDbConn* conn);
```

All operation functions return a JSON-encoded response string. The caller must free it with `oxidb_free_string`.

## .NET Client

The .NET client (`dotnet/OxiDb.Client`) wraps the C FFI library via P/Invoke:

```csharp
using OxiDb.Client;

using var db = OxiDbClient.Connect("127.0.0.1", 4444);

db.Insert("users", "{\"name\":\"Alice\",\"age\":30}");
var result = db.Find("users", "{\"age\":{\"$gte\":18}}");
```

## Architecture

### Storage (.dat files)

Each collection is stored as an append-only file of records:

```
[status: u8][length: u32 LE][JSON bytes]
```

`status` is `0` (active) or `1` (deleted). Deletes flip the byte in place — no rewrite needed.

### Write-Ahead Log

Every mutation is logged to a WAL before touching the data file:

```
[crc32: u32 LE][payload_len: u32 LE][op_type: u8][doc_id: u64 LE][doc bytes]
```

Batch operations (`insert_many`, `update`, `delete`) use a 3-fsync protocol: WAL write + fsync → data mutations + fsync → WAL checkpoint + fsync. On startup the WAL is replayed idempotently and then truncated.

### IndexValue Type Ordering

Values in indexes follow a deterministic order:

```
Null < Boolean < Integer/Float < DateTime < String
```

Date strings (ISO 8601, RFC 3339, `YYYY-MM-DD`) are automatically detected and stored as epoch milliseconds, enabling fast integer comparison for date range queries.

## License

See [LICENSE](LICENSE) for details.
