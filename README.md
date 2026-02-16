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
- **Aggregation pipeline** — 10 MongoDB-style stages: `$match`, `$group`, `$sort`, `$skip`, `$limit`, `$project`, `$count`, `$unwind`, `$addFields`, `$lookup`
- **S3-like blob storage** — buckets, put/get/head/delete/list objects with metadata and CRC32 etags
- **Full-text search** — automatic text extraction, inverted index with TF-IDF ranked search
- **TCP server** with a length-prefixed JSON protocol and thread pool
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

## Aggregation Pipeline

Run multi-stage data processing pipelines, MongoDB-style:

```rust
use serde_json::json;

let results = db.aggregate("orders", &json!([
    {"$match": {"status": "completed"}},
    {"$group": {"_id": "$category", "total": {"$sum": "$amount"}, "count": {"$sum": 1}}},
    {"$sort": {"total": -1}},
    {"$limit": 10}
]))?;
```

### Stages

| Stage         | Description                                        |
|---------------|----------------------------------------------------|
| `$match`      | Filter documents (uses index if leading stage)     |
| `$group`      | Group by key with accumulators                     |
| `$sort`       | Sort by fields (1 = asc, -1 = desc)               |
| `$skip`       | Skip N documents                                   |
| `$limit`      | Limit to N documents                               |
| `$project`    | Include, exclude, or compute fields                |
| `$count`      | Replace docs with a single count document          |
| `$unwind`     | Expand array fields into one document per element  |
| `$addFields`  | Add computed fields while preserving existing ones |
| `$lookup`     | Left outer join with another collection            |

### Accumulators (for `$group`)

`$sum`, `$avg`, `$min`, `$max`, `$count`, `$first`, `$last`, `$push`

### Expressions

Field references (`"$fieldName"`), literals, and arithmetic operators (`$add`, `$subtract`, `$multiply`, `$divide`). Dot-notation is supported for nested fields (`"$user.address.city"`).

### Examples

**Group with null key (aggregate all documents):**

```json
[{"$group": {"_id": null, "avgPrice": {"$avg": "$price"}, "maxPrice": {"$max": "$price"}}}]
```

**Unwind + group (tag frequency):**

```json
[
    {"$unwind": "$tags"},
    {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}}
]
```

**Lookup (cross-collection join):**

```json
[{"$lookup": {"from": "products", "localField": "productId", "foreignField": "_id", "as": "product"}}]
```

**Project with computed fields:**

```json
[
    {"$addFields": {"total": {"$multiply": ["$price", "$qty"]}}},
    {"$project": {"name": 1, "total": 1, "_id": 0}}
]
```

## Blob Storage

Store and retrieve binary objects (files, images, PDFs, etc.) in S3-style buckets:

```rust
use std::collections::HashMap;

// Create a bucket
db.create_bucket("docs")?;

// Upload an object
let data = b"Hello World";
let meta = db.put_object("docs", "hello.txt", data, "text/plain", HashMap::new())?;

// Retrieve an object
let (data, meta) = db.get_object("docs", "hello.txt")?;

// Get metadata only (no data read)
let meta = db.head_object("docs", "hello.txt")?;

// List objects with optional prefix filter
let objects = db.list_objects("docs", Some("reports/"), None)?;

// Delete an object
db.delete_object("docs", "hello.txt")?;

// List and delete buckets
let buckets = db.list_buckets();
db.delete_bucket("docs")?;
```

Objects are stored on disk as `_blobs/<bucket>/<id>.data` with a JSON metadata sidecar (`<id>.meta`). Each object gets a CRC32 etag and supports user-defined metadata.

## Full-Text Search

Text content from uploaded objects is automatically indexed. Supported content types:

| Content Type | Extraction |
|---|---|
| `text/*` | UTF-8 decode (HTML tags stripped for `text/html`) |
| `application/json` | Recursive string value extraction |
| Other | Not indexed (blob stored only) |

Search uses TF-IDF scoring to rank results:

```rust
// Upload text objects (automatically indexed)
db.put_object("docs", "report.txt", b"database performance tuning guide", "text/plain", HashMap::new())?;
db.put_object("docs", "notes.md", b"quick notes about database queries", "text/plain", HashMap::new())?;

// Search across all buckets
let results = db.search(None, "database performance", 10)?;

// Search within a specific bucket
let results = db.search(Some("docs"), "database", 10)?;
```

Each result includes `bucket`, `key`, and `score` (TF-IDF relevance).

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
# Defaults: 127.0.0.1:4444, data dir ./oxidb_data, 4 worker threads
cargo run --bin oxidb-server

# Custom settings
OXIDB_ADDR=0.0.0.0:4444 OXIDB_DATA=/var/lib/oxidb OXIDB_POOL_SIZE=8 cargo run --bin oxidb-server
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
| `aggregate`              | `collection`, `pipeline`                           |
| `compact`                | `collection`                                       |
| `create_bucket`          | `bucket`                                           |
| `list_buckets`           | --                                                 |
| `delete_bucket`          | `bucket`                                           |
| `put_object`             | `bucket`, `key`, `data` (base64), `content_type?`, `metadata?` |
| `get_object`             | `bucket`, `key`                                    |
| `head_object`            | `bucket`, `key`                                    |
| `delete_object`          | `bucket`, `key`                                    |
| `list_objects`           | `bucket`, `prefix?`, `limit?`                      |
| `search`                 | `query`, `bucket?`, `limit?`                       |

Sort values: `1` for ascending, `-1` for descending.

### Blob & Search Examples

**Put an object (binary data is base64-encoded):**

```json
{"cmd": "put_object", "bucket": "docs", "key": "report.txt",
 "data": "SGVsbG8gV29ybGQ=", "content_type": "text/plain",
 "metadata": {"author": "Alice"}}
```

**Get an object:**

```json
{"cmd": "get_object", "bucket": "docs", "key": "report.txt"}
```
```json
{"ok": true, "data": {"content": "SGVsbG8gV29ybGQ=", "metadata": {"key": "report.txt", "bucket": "docs", "size": 11, ...}}}
```

**Search:**

```json
{"cmd": "search", "query": "database performance", "bucket": "docs", "limit": 10}
```
```json
{"ok": true, "data": [{"bucket": "docs", "key": "report.txt", "score": 2.45}]}
```

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
char*      oxidb_aggregate(OxiDbConn* conn, const char* collection, const char* pipeline_json);
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

// Aggregation pipeline
var stats = db.Aggregate("orders", """
    [
        {"$match": {"status": "completed"}},
        {"$group": {"_id": "$category", "total": {"$sum": "$amount"}}},
        {"$sort": {"total": -1}}
    ]
""");
```

## Architecture

### Storage (.dat files)

Each collection is stored as an append-only file of records:

```
[status: u8][length: u32 LE][JSON bytes]
```

`status` is `0` (active) or `1` (deleted). Deletes flip the byte in place — no rewrite needed.

### Blob & FTS Storage

```
<data_dir>/
├── _blobs/<bucket>/<id>.data     # binary content
├── _blobs/<bucket>/<id>.meta     # JSON metadata (key, size, etag, content_type, ...)
├── _fts/index.json               # persisted inverted index
├── users.dat / users.wal         # existing collections (unchanged)
```

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
