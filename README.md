<p align="center">
  <img src="logo.png" alt="OxiDB" width="500">
</p>

<p align="center">A fast, embeddable document database written in Rust. Works like MongoDB but runs as a single binary with zero configuration.</p>

**Client libraries:** Python, Go, Ruby, Java/Spring Boot, Julia, PHP, .NET, Swift/iOS, C FFI

## Installation

### Option 1: Download a pre-built binary (easiest)

Download the latest release for your platform from [GitHub Releases](https://github.com/parisxmas/OxiDB/releases):

| Platform | Download |
|----------|----------|
| macOS Apple Silicon (M1/M2/M3/M4) | `oxidb-server-macos-arm64.tar.gz` |
| macOS Intel | `oxidb-server-macos-x86_64.tar.gz` |
| Linux x86_64 | `oxidb-server-linux-x86_64.tar.gz` |
| Windows x86_64 | `oxidb-server-windows-x86_64.zip` |

```bash
# macOS / Linux
tar xzf oxidb-server-*.tar.gz
./oxidb-server
```

```powershell
# Windows
Expand-Archive oxidb-server-windows-x86_64.zip
.\oxidb-server.exe
```

The server starts on `127.0.0.1:4444` by default. Data is stored in `./oxidb_data/`.

### Option 2: Build from source

Requires [Rust](https://rustup.rs/) (1.70+):

```bash
git clone https://github.com/parisxmas/OxiDB.git
cd OxiDB
cargo run --release --package oxidb-server
```

### Option 3: Run with Docker

No Rust toolchain needed — just Docker:

```bash
git clone https://github.com/parisxmas/OxiDB.git
cd OxiDB
docker compose up -d
```

This builds the server from source inside a container and starts it on port `4444`. Data is persisted in a Docker volume.

To rebuild after pulling updates:

```bash
docker compose up -d --build
```

To stop:

```bash
docker compose down
```

You can also run the image directly:

```bash
docker build -t oxidb .
docker run -d --name oxidb-server -p 4444:4444 -v oxidb_data:/data oxidb
```

### Configuration

Configure via environment variables (works with binary, source, and Docker):

```bash
# Binary or source
OXIDB_ADDR=0.0.0.0:4444 OXIDB_DATA=/var/lib/oxidb OXIDB_POOL_SIZE=8 ./oxidb-server

# Docker (edit docker-compose.yml environment section, or pass via docker run)
docker run -d -p 4444:4444 -e OXIDB_POOL_SIZE=8 -v oxidb_data:/data oxidb
```

| Variable | Default | Description |
|----------|---------|-------------|
| `OXIDB_ADDR` | `127.0.0.1:4444` | Listen address and port |
| `OXIDB_DATA` | `./oxidb_data` | Data directory |
| `OXIDB_POOL_SIZE` | `4` | Worker thread count |
| `OXIDB_IDLE_TIMEOUT` | `30` | Idle connection timeout in seconds (0 = no timeout) |
| `OXIDB_ENCRYPTION_KEY` | — | Path to 32-byte AES-256 key file for encryption at rest |
| `OXIDB_TLS_CERT` | — | Path to TLS certificate PEM file |
| `OXIDB_TLS_KEY` | — | Path to TLS private key PEM file |
| `OXIDB_AUTH` | `false` | Enable SCRAM-SHA-256 authentication (`true`/`1`) |
| `OXIDB_AUDIT` | `false` | Enable audit logging (`true`/`1`) |

### Verify it works

```bash
# In another terminal, test with a raw TCP command:
echo -ne '\x11\x00\x00\x00{"cmd":"ping"}' | nc localhost 4444
# You should see a response containing "pong"
```

## Using OxiDB from your language

Once the server is running, connect to it from any supported language. Every client uses the same TCP protocol — just pick your language:

| Language | Location | Install |
|----------|----------|---------|
| [Python](#python) | `python/oxidb.py` | Copy file, no dependencies |
| [Go](#go) | `go/oxidb/` | `go get github.com/parisxmas/OxiDB/go/oxidb` |
| [Ruby](#ruby) | `ruby/lib/oxidb.rb` | Copy file or use gemspec, no dependencies |
| [Java / Spring Boot](#java--spring-boot) | `oxidb-spring-boot-starter/` | `mvn install`, then add Maven dependency |
| [Julia](#julia) | `julia/OxiDb/` | TCP client or embedded (no server needed) |
| [Swift/iOS](#swiftios) | `swift/OxiDB/` | Embedded (no server) or TCP client |
| [PHP](#php) | `php/src/OxiDbClient.php` | Copy files, no dependencies |
| [.NET](#net-client) | `dotnet/OxiDb.Client/` | Uses C FFI via P/Invoke |
| [Rust (embedded)](#rust-embedded-library) | crate root | `oxidb = { path = "." }` |

### Quick example (Python)

```bash
# 1. Start the server
./oxidb-server

# 2. In another terminal
cp python/oxidb.py my_project/
python3
```

```python
from oxidb import OxiDbClient

db = OxiDbClient("127.0.0.1", 4444)
db.insert("users", {"name": "Alice", "age": 30})
print(db.find("users", {"name": "Alice"}))
# [{'_id': 1, '_version': 1, 'name': 'Alice', 'age': 30}]
db.close()
```

## Features

- **Document database** — JSON documents, no schema required, collections auto-created on insert
- **MongoDB-style queries** — `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$exists`, `$and`, `$or`
- **12 update operators** — `$set`, `$unset`, `$inc`, `$mul`, `$min`, `$max`, `$rename`, `$currentDate`, `$push`, `$pull`, `$addToSet`, `$pop`
- **Aggregation pipeline** — 10 stages: `$match`, `$group`, `$sort`, `$skip`, `$limit`, `$project`, `$count`, `$unwind`, `$addFields`, `$lookup`
- **Indexes** — field, unique, and composite indexes with automatic backfill
- **Transactions** — OCC (optimistic concurrency control) with begin/commit/rollback
- **Blob storage** — S3-style buckets with put/get/head/delete/list and CRC32 etags
- **Full-text search** — automatic text extraction from 10+ formats, TF-IDF ranked search
- **Crash-safe** — write-ahead log with CRC32 checksums, verified by SIGKILL recovery tests
- **Encryption at rest** — AES-256-GCM with per-record nonces, optional via `OXIDB_ENCRYPTION_KEY`
- **Security** — TLS transport, SCRAM-SHA-256 authentication, role-based access control, audit logging
- **Compaction** — reclaim space from deleted documents
- **Thread-safe** — `RwLock` per collection, concurrent readers never block
- **Tested** — 17 concurrency/crash/encryption tests with 100 simultaneous connections

## Performance

OxiDB is designed for low-latency operations. All benchmarks run on the same Linux server (Debian 6.1, x86_64, 8 GB RAM) with both databases in Docker containers. OxiDB vs MongoDB 6.0, best of 3 runs per test.

### Benchmark 1: 1 Million Documents

1,000,000 employee records with 8 indexes (created_at, updated_at, last_login, department, status, country, salary, level). Tests cover inserts, queries, counts, aggregation, and updates.

**Result: OxiDB wins 16 of 38 tests**

#### Count Operations (1M docs) — OxiDB dominates

| Operation | OxiDB | MongoDB | Winner |
|-----------|-------|---------|--------|
| Count all docs | 0.39ms | 382.74ms | **OxiDB 987x** |
| Count by department | 0.35ms | 26.91ms | **OxiDB 76x** |
| Count by salary range | 19.55ms | 200.05ms | **OxiDB 10x** |
| Count by date range (2023) | 6.81ms | 52.31ms | **OxiDB 8x** |

#### Sorted Queries with Limit (1M docs) — OxiDB dominates

| Operation | OxiDB | MongoDB | Winner |
|-----------|-------|---------|--------|
| Sort by created_at desc, limit 10 | 0.22ms | 0.52ms | **OxiDB 2.4x** |
| Sort by updated_at desc, skip 50, limit 20 | 0.23ms | 0.55ms | **OxiDB 2.4x** |
| Sort by salary desc, limit 10 | 0.24ms | 0.50ms | **OxiDB 2.1x** |
| Sort by created_at asc, limit 100 | 0.43ms | 0.85ms | **OxiDB 2.0x** |
| Find limit only (no sort, limit 100) | 0.50ms | 0.66ms | **OxiDB 1.3x** |

#### Indexed Field Queries (1M docs)

| Operation | OxiDB | MongoDB | Winner |
|-----------|-------|---------|--------|
| Find by status (indexed, ~250K results) | 1429ms | 2552ms | **OxiDB 1.8x** |
| Find by level (indexed, ~83K results) | 490ms | 505ms | **OxiDB 1.03x** |
| Find by country (indexed, ~100K results) | 640ms | 654ms | **OxiDB 1.02x** |
| Wide date range (1 year, ~200K results) | 1818ms | 2242ms | **OxiDB 1.2x** |

#### Filtered Aggregation (1M docs)

| Operation | OxiDB | MongoDB | Winner |
|-----------|-------|---------|--------|
| match(country=US) + group by level | 101ms | 192ms | **OxiDB 1.9x** |
| match(dept=eng) + group by country | 138ms | 191ms | **OxiDB 1.4x** |

#### Where MongoDB Wins (1M docs)

| Operation | OxiDB | MongoDB | Winner |
|-----------|-------|---------|--------|
| Bulk insert 1M docs (5K batches) | 33.4s | 15.4s | **MongoDB 2.2x** |
| Update single doc | 1367ms | 1.2ms | **MongoDB 1169x** |
| Update by indexed field | 898ms | 292ms | **MongoDB 3.1x** |
| Full-scan aggregation (group by dept) | 1665ms | 924ms | **MongoDB 1.8x** |
| Full-scan aggregation (group by country) | 1608ms | 791ms | **MongoDB 2.0x** |
| Index creation (1M docs, avg) | 1930ms | 1610ms | **MongoDB 1.2x** |
| Narrow date range (1 week) | 398ms | 26ms | **MongoDB 15x** |
| FindOne unindexed (full scan) | 1154ms | 142ms | **MongoDB 8x** |

### Benchmark 2: Feature-Focused (Small-Medium Collections)

Mixed workloads on 5-1000 doc collections: single inserts, batch inserts, indexes, finds, counts, updates, aggregation, deletes.

**Result: OxiDB wins 25 of 30 tests (1.73x overall faster)**

| Category | OxiDB Wins | Highlights |
|----------|-----------|------------|
| Index creation | 3/3 | **34x-66x** faster |
| Find queries | 7/7 | **2x-3.5x** faster |
| Count | 2/2 | **2.7x-6.4x** faster |
| Aggregation | 5/5 | **2.4x-3.9x** faster |
| Delete | 1/1 | **3.1x** faster |
| Inserts | 2/5 | Wins small batches |

| Operation | OxiDB | MongoDB | Speedup |
|-----------|-------|---------|---------|
| Create unique index | 0.25ms | 16.6ms | **66x** |
| Create index | 0.30ms | 17.0ms | **57x** |
| Count (1K docs) | 0.10ms | 0.63ms | **6.4x** |
| Aggregate: group + sort + limit | 0.10ms | 0.38ms | **3.9x** |
| Count all users | 0.09ms | 0.31ms | **3.6x** |
| Aggregate: match + count | 0.10ms | 0.34ms | **3.5x** |
| Find with range ($gte) | 0.09ms | 0.32ms | **3.5x** |
| Find with $in | 0.10ms | 0.33ms | **3.4x** |
| Find all users | 0.10ms | 0.32ms | **3.3x** |
| Delete by query | 0.09ms | 0.28ms | **3.1x** |
| Aggregate 1K docs (group by 10) | 0.41ms | 1.23ms | **3.0x** |
| Find all 1000 docs | 1.09ms | 1.94ms | **1.8x** |
| Insert 5 users (single) | 25.7ms | 42.9ms | **1.7x** |

### Key Optimizations

- **Full document cache** — all documents held in memory after startup, eliminating disk I/O for reads
- **Index-backed sort** — `find()` with sort + limit on an indexed field iterates the BTreeMap index directly, turning O(n) into O(limit)
- **Index-only count** — `count()` with a simple equality filter on an indexed field returns the index set size without touching documents
- **Single-pass updates/deletes** — mutations scan once using cached documents instead of double-reading from disk

### Summary

| Workload | OxiDB vs MongoDB |
|----------|-----------------|
| Count operations (1M docs) | **8x-987x** faster |
| Index creation (small collections) | **34x-66x** faster |
| Sorted find + limit (1M docs) | **2x-2.4x** faster |
| Filtered aggregation ($match + $group) | **1.4x-1.9x** faster |
| Indexed field queries (large result sets) | **1x-1.8x** faster |
| Small collection reads/aggregations | **2x-4x** faster |
| Bulk inserts (1M docs) | MongoDB **2.2x** faster |
| Single-doc updates | MongoDB significantly faster |
| Full-table aggregation (no $match) | MongoDB **1.8x-2x** faster |
| Unindexed full scans | MongoDB **8x-15x** faster |

OxiDB excels at **read-heavy workloads** — counts, sorted pagination, filtered aggregations, indexed lookups. MongoDB wins on **write-heavy operations** — bulk inserts, in-place updates, full-collection scans. For typical web application patterns (index, query, count, paginate), OxiDB delivers consistently lower latency.

## Testing

OxiDB includes 5 Python test suites (17 tests total) that verify correctness under heavy concurrency, crash scenarios, and encryption. All tests use 100 simultaneous connections unless noted.

```bash
# Start the server with enough threads for 100 connections
env OXIDB_POOL_SIZE=110 OXIDB_IDLE_TIMEOUT=0 ./oxidb-server

# Run individual test suites
python3 examples/python/test_occ_and_integrity.py
python3 examples/python/test_crash_recovery.py        # manages its own server
python3 examples/python/test_collection_lifecycle.py
python3 examples/python/test_encryption.py             # manages its own server
python3 examples/python/memory_stress_test.py
```

### OCC & Data Integrity (`test_occ_and_integrity.py`)

| Test | What it verifies |
|------|-----------------|
| **OCC Conflict Storm** | 100 connections x 50 rounds of read-modify-write transactions on a single document. Verifies final balance = 1000 + successful commits. 97.6% conflict rate, zero corruption. |
| **Multi-Account Transfer** | 10 accounts, 100 connections doing random transfers via transactions. Verifies total sum is preserved exactly (conservation of money). |
| **Data Integrity** | 100 workers each insert 50 docs with SHA-256 checksums, update 20, delete 10. Every surviving document verified byte-perfect. |
| **Concurrent $inc Counter** | 100 connections x 100 atomic increments. Final value must equal exactly 10,000. |

### Crash Recovery & WAL Replay (`test_crash_recovery.py`)

| Test | What it verifies |
|------|-----------------|
| **Committed Data Survives** | Insert 500 docs, SIGKILL the server, restart — all documents intact with valid checksums. |
| **Uncommitted Tx Lost** | Begin transaction, insert 100 docs, SIGKILL without committing — zero uncommitted docs after recovery. |
| **Crash During Heavy Writes** | 50 concurrent writers, SIGKILL mid-flight — every confirmed insert survives, zero corruption. |

### Collection Lifecycle (`test_collection_lifecycle.py`)

| Test | What it verifies |
|------|-----------------|
| **Drop While Writing** | 50 writers + 1 dropper for 5s — all errors are clean, no panics or corruption. |
| **Drop While Reading** | 50 readers + 1 dropper/reseeder for 5s — zero corrupt documents in any read. |
| **Rapid Collection Churn** | 50 threads each create → write 30 docs → verify checksums → drop, all concurrently. |
| **Cross-Collection Isolation** | Thrash one collection (drop/recreate) while verifying another is completely unaffected (9,800+ reads, zero corruption). |

### Encryption at Rest (`test_encryption.py`)

| Test | What it verifies |
|------|-----------------|
| **No Plaintext on Disk** | Write distinctive strings with encryption, scan raw `.dat`/`.wal` files — plaintext never appears. |
| **Readable After Restart** | 200 docs byte-perfect after encrypted server restart (WAL replay with decryption). |
| **Wrong Key Fails** | Write with key A, restart with key B — data access correctly rejected (AES-GCM auth tag mismatch). |
| **Survives Crash** | 300 encrypted docs survive SIGKILL, plaintext never on disk, sample values verified after recovery. |
| **Encryption Overhead** | Encrypted files are exactly 28 bytes/record larger (12B nonce + 16B AES-GCM auth tag). |

### Memory Stress (`memory_stress_test.py`)

100 concurrent connections performing mixed inserts, updates, queries, and deletes across multiple iterations with `drop_collection` between rounds. Monitors RSS growth to detect memory leaks.

## Rust (embedded library)

Use OxiDB directly as a Rust library without the TCP server:

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

## Update Operators

Updates support 12 MongoDB-style operators. Multiple operators can be combined in a single update. All operators support dot-notation for nested fields.

### Field Operators

| Operator       | Example                                          | Description                            |
|----------------|--------------------------------------------------|----------------------------------------|
| `$set`         | `{"$set": {"age": 31}}`                         | Set field value                        |
| `$unset`       | `{"$unset": {"temp": ""}}`                      | Remove field                           |
| `$inc`         | `{"$inc": {"count": 1}}`                        | Increment by value (creates if missing)|
| `$mul`         | `{"$mul": {"price": 1.1}}`                      | Multiply by value (0 if missing)       |
| `$min`         | `{"$min": {"low": 50}}`                         | Set to value if less than current      |
| `$max`         | `{"$max": {"high": 100}}`                       | Set to value if greater than current   |
| `$rename`      | `{"$rename": {"old": "new"}}`                   | Rename field                           |
| `$currentDate` | `{"$currentDate": {"updated_at": true}}`        | Set to current ISO 8601 datetime       |

### Array Operators

| Operator     | Example                                   | Description                              |
|--------------|-------------------------------------------|------------------------------------------|
| `$push`      | `{"$push": {"tags": "new"}}`             | Append to array (creates if missing)     |
| `$pull`      | `{"$pull": {"tags": "old"}}`             | Remove all matching elements             |
| `$addToSet`  | `{"$addToSet": {"tags": "unique"}}`      | Append only if not already present       |
| `$pop`       | `{"$pop": {"arr": 1}}`                   | Remove last (1) or first (-1) element    |

### Examples

**Combine multiple operators:**

```rust
let modified = db.update(
    "users",
    &json!({"name": "Alice"}),
    &json!({
        "$set": {"status": "active"},
        "$inc": {"login_count": 1},
        "$currentDate": {"last_login": true}
    }),
)?;
```

**Nested field updates with dot-notation:**

```rust
db.update(
    "users",
    &json!({"name": "Alice"}),
    &json!({"$set": {"address.city": "NYC", "address.zip": "10001"}}),
)?;
```

**Array manipulation:**

```rust
db.update("posts", &json!({"_id": 1}), &json!({"$push": {"tags": "rust"}}))?;
db.update("posts", &json!({"_id": 1}), &json!({"$pull": {"tags": "draft"}}))?;
db.update("posts", &json!({"_id": 1}), &json!({"$addToSet": {"tags": "database"}}))?;
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

Text content from uploaded objects is automatically indexed in a background thread so `put_object` returns immediately. Supported content types:

| Content Type | Extensions | Extraction |
|---|---|---|
| `text/html` | .html | Strip HTML tags |
| `text/xml`, `application/xml` | .xml | Strip XML tags |
| `text/*` | .txt, .md, .csv, .tsv, .log | UTF-8 decode |
| `application/json` | .json | Recursive string value extraction |
| `application/pdf` | .pdf | PDF text extraction (`pdf-extract`) |
| `application/vnd.openxmlformats-officedocument.wordprocessingml.document` | .docx | Unzip → extract `word/document.xml` → strip tags |
| `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet` | .xlsx | Unzip → extract shared strings |
| `image/png`, `image/jpeg`, `image/tiff`, `image/bmp` | .png, .jpg, .tiff, .bmp | OCR via Tesseract (requires `ocr` feature) |
| Other | * | Not indexed (blob stored only) |

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

### OCR / ICR (Image Text Extraction)

With the `ocr` feature enabled, uploaded images (PNG, JPEG, TIFF, BMP) are automatically processed with Tesseract OCR to extract printed and handwritten text, which is then indexed for full-text search.

**Install system dependencies:**

```bash
# macOS
brew install tesseract leptonica

# Ubuntu / Debian
sudo apt-get install libtesseract-dev libleptonica-dev tesseract-ocr-eng
```

**Build with OCR support:**

```bash
cargo build --workspace --features ocr
```

Without the `ocr` feature (the default), the project compiles and runs normally — image blobs are stored but not text-indexed.

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

## TCP Protocol

### Wire Format

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

## Python

Zero dependencies — uses only the Python standard library. Python 3.7+.

**Install:** Copy the single file into your project:

```bash
cp python/oxidb.py your_project/
```

**Connect:**

```python
from oxidb import OxiDbClient

client = OxiDbClient("127.0.0.1", 4444)
# or as a context manager:
# with OxiDbClient("127.0.0.1", 4444) as client:
```

### CRUD

```python
# Insert
client.insert("users", {"name": "Alice", "age": 30})
client.insert_many("users", [
    {"name": "Bob", "age": 25},
    {"name": "Charlie", "age": 35},
])

# Find with options
docs = client.find("users", {"name": "Alice"})
docs = client.find("users", {}, sort={"age": 1}, skip=0, limit=10)
doc  = client.find_one("users", {"name": "Alice"})

# Update
client.update("users", {"name": "Alice"}, {"$set": {"age": 31}})

# Delete
client.delete("users", {"name": "Charlie"})

# Count
n = client.count("users")
```

### Collections & Indexes

```python
client.create_collection("orders")
cols = client.list_collections()
client.drop_collection("orders")

client.create_index("users", "name")
client.create_unique_index("users", "email")
client.create_composite_index("users", ["name", "age"])
```

### Aggregation

```python
results = client.aggregate("users", [
    {"$match": {"age": {"$gte": 18}}},
    {"$group": {"_id": "$category", "total": {"$sum": "$amount"}}},
    {"$sort": {"total": -1}},
])
```

### Transactions

```python
# Auto-commit on success, auto-rollback on exception
with client.transaction():
    client.insert("ledger", {"action": "debit",  "amount": 100})
    client.insert("ledger", {"action": "credit", "amount": 100})

# Manual control
client.begin_tx()
client.insert("ledger", {"action": "refund", "amount": 50})
client.commit_tx()   # or client.rollback_tx()
```

### Blob Storage

```python
client.create_bucket("files")
client.list_buckets()

client.put_object("files", "hello.txt", b"Hello from Python!",
                  content_type="text/plain", metadata={"author": "py"})
data, meta = client.get_object("files", "hello.txt")
head = client.head_object("files", "hello.txt")
objs = client.list_objects("files", prefix="hello", limit=10)

client.delete_object("files", "hello.txt")
client.delete_bucket("files")
```

### Full-Text Search

```python
results = client.search("hello world", bucket="files", limit=10)
```

### Compaction

```python
stats = client.compact("users")  # returns {old_size, new_size, docs_kept}
```

```python
client.close()
```

## .NET Client

Wraps the C FFI library via P/Invoke. Requires .NET 8+.

**Install:** Build the FFI library first, then reference the project:

```bash
cargo build --release -p oxidb-client-ffi
# Then add dotnet/OxiDb.Client as a project reference
```

**Connect:**

```csharp
using OxiDb.Client;

using var db = OxiDbClient.Connect("127.0.0.1", 4444);
```

### CRUD

```csharp
// Insert
db.Insert("users", "{\"name\":\"Alice\",\"age\":30}");

// Find
var docs = db.Find("users", "{\"name\":\"Alice\"}");
var doc  = db.FindOne("users", "{\"name\":\"Alice\"}");

// With Filter/UpdateDef builders
var docs2 = db.Find("users", Filter.Gte("age", 18));
db.Update("users", Filter.Eq("name", "Alice"), UpdateDef.Set("age", 31));
db.Delete("users", Filter.Eq("name", "Charlie"));

// Count
var count = db.Count("users");
```

### Collections & Indexes

```csharp
db.ListCollections();
db.DropCollection("orders");

db.CreateIndex("users", "name");
db.CreateCompositeIndex("users", "[\"name\", \"age\"]");
```

### Aggregation

```csharp
var stats = db.Aggregate("orders", """
    [
        {"$match": {"status": "completed"}},
        {"$group": {"_id": "$category", "total": {"$sum": "$amount"}}},
        {"$sort": {"total": -1}}
    ]
""");
```

### Transactions

```csharp
db.BeginTransaction();
db.Insert("ledger", "{\"action\":\"debit\",\"amount\":100}");
db.Insert("ledger", "{\"action\":\"credit\",\"amount\":100}");
db.CommitTransaction();   // or db.RollbackTransaction()
```

### Blob Storage

```csharp
db.CreateBucket("files");
db.ListBuckets();

db.PutObject("files", "hello.txt", Convert.ToBase64String(data), "text/plain");
var obj = db.GetObject("files", "hello.txt");
var head = db.HeadObject("files", "hello.txt");
var objs = db.ListObjects("files", prefix: "hello", limit: 10);

db.DeleteObject("files", "hello.txt");
db.DeleteBucket("files");
```

### Full-Text Search

```csharp
var results = db.Search("hello world", bucket: "files", limit: 10);
```

## Java / Spring Boot

Spring Boot 3.x auto-configuration starter. Java 17+.

**Install:** Build and install the starter to your local Maven repository, then add it to your project:

```bash
cd oxidb-spring-boot-starter && mvn clean install
```

Add the starter to your `pom.xml`:

```xml
<dependency>
    <groupId>com.oxidb</groupId>
    <artifactId>oxidb-spring-boot-starter</artifactId>
    <version>0.1.0</version>
</dependency>
```

Configure in `application.properties`:

```properties
oxidb.host=127.0.0.1
oxidb.port=4444
oxidb.timeout-ms=5000
```

Inject the auto-configured client:

```java
@Autowired
private OxiDbClient db;
```

### CRUD

```java
// Insert
db.insert("users", Map.of("name", "Alice", "age", 30));
db.insertMany("users", List.of(
    Map.of("name", "Bob", "age", 25),
    Map.of("name", "Charlie", "age", 35)
));

// Find with options
JsonNode docs = db.find("users", Map.of("name", "Alice"));
JsonNode docs2 = db.find("users", Map.of(), Map.of("age", 1), 0, 10); // sort, skip, limit
JsonNode doc = db.findOne("users", Map.of("name", "Alice"));

// Also accepts JSON strings
JsonNode docs3 = db.find("users", "{\"age\":{\"$gte\":18}}");

// Update
db.update("users", Map.of("name", "Alice"), Map.of("$set", Map.of("age", 31)));

// Delete
db.delete("users", Map.of("name", "Charlie"));

// Count
int n = db.count("users");
```

### Collections & Indexes

```java
db.createCollection("orders");
db.listCollections();
db.dropCollection("orders");

db.createIndex("users", "name");
db.createUniqueIndex("users", "email");
db.createCompositeIndex("users", List.of("name", "age"));
```

### Aggregation

```java
JsonNode results = db.aggregate("users", """
    [
        {"$match": {"age": {"$gte": 18}}},
        {"$group": {"_id": "$category", "total": {"$sum": "$amount"}}},
        {"$sort": {"total": -1}}
    ]
""");
```

### Transactions

```java
// Auto-commit on success, auto-rollback on exception
db.withTransaction(() -> {
    db.insert("ledger", Map.of("action", "debit",  "amount", 100));
    db.insert("ledger", Map.of("action", "credit", "amount", 100));
});

// Manual control
db.beginTx();
db.insert("ledger", Map.of("action", "refund", "amount", 50));
db.commitTx();   // or db.rollbackTx()
```

### Blob Storage

```java
db.createBucket("files");
db.listBuckets();

db.putObject("files", "hello.txt", "Hello!".getBytes(), "text/plain", Map.of("author", "java"));
JsonNode obj = db.getObject("files", "hello.txt");
byte[] content = db.decodeObjectContent(obj);
JsonNode head = db.headObject("files", "hello.txt");
JsonNode objs = db.listObjects("files", "hello", 10);

db.deleteObject("files", "hello.txt");
db.deleteBucket("files");
```

### Full-Text Search

```java
JsonNode results = db.search("hello world", "files", 10);
```

### Compaction

```java
JsonNode stats = db.compact("users"); // old_size, new_size, docs_kept
```

See `examples/spring-boot` for a full working REST app.

## PHP

Zero dependencies — uses only built-in PHP sockets and json. PHP 8.1+.

**Install:** Copy the `php/src/` files into your project:

```bash
cp php/src/*.php your_project/
```

**Connect:**

```php
require_once 'src/OxiDbException.php';
require_once 'src/TransactionConflictException.php';
require_once 'src/OxiDbClient.php';

$db = new \OxiDb\OxiDbClient('127.0.0.1', 4444);
```

### CRUD

```php
// Insert
$db->insert('users', ['name' => 'Alice', 'age' => 30]);
$db->insertMany('users', [
    ['name' => 'Bob', 'age' => 25],
    ['name' => 'Charlie', 'age' => 35],
]);

// Find with options
$docs = $db->find('users', ['name' => 'Alice']);
$docs = $db->find('users', [], ['age' => 1], 0, 10); // sort, skip, limit
$doc  = $db->findOne('users', ['name' => 'Alice']);

// Update
$db->update('users', ['name' => 'Alice'], ['$set' => ['age' => 31]]);

// Delete
$db->delete('users', ['name' => 'Charlie']);

// Count
$n = $db->count('users');
```

### Collections & Indexes

```php
$db->createCollection('orders');
$db->listCollections();
$db->dropCollection('orders');

$db->createIndex('users', 'name');
$db->createUniqueIndex('users', 'email');
$db->createCompositeIndex('users', ['name', 'age']);
```

### Aggregation

```php
$results = $db->aggregate('users', [
    ['$match' => ['age' => ['$gte' => 18]]],
    ['$group' => ['_id' => '$category', 'total' => ['$sum' => '$amount']]],
    ['$sort'  => ['total' => -1]],
]);
```

### Transactions

```php
// Auto-commit on success, auto-rollback on exception
$db->transaction(function () use ($db) {
    $db->insert('ledger', ['action' => 'debit',  'amount' => 100]);
    $db->insert('ledger', ['action' => 'credit', 'amount' => 100]);
});

// Manual control
$db->beginTx();
$db->insert('ledger', ['action' => 'refund', 'amount' => 50]);
$db->commitTx();   // or $db->rollbackTx()
```

### Blob Storage

```php
$db->createBucket('files');
$db->listBuckets();

$db->putObject('files', 'hello.txt', 'Hello from PHP!', 'text/plain', ['author' => 'php']);
[$data, $meta] = $db->getObject('files', 'hello.txt');
$head = $db->headObject('files', 'hello.txt');
$objs = $db->listObjects('files', 'hello', 10);

$db->deleteObject('files', 'hello.txt');
$db->deleteBucket('files');
```

### Full-Text Search

```php
$results = $db->search('hello world', 'files', 10);
```

### Compaction

```php
$stats = $db->compact('users'); // old_size, new_size, docs_kept
```

```php
$db->close();
```

## Ruby

Zero dependencies — uses only the Ruby standard library. Ruby 3.0+.

**Install:** Copy the single file into your project:

```bash
cp ruby/lib/oxidb.rb your_project/
```

**Connect:**

```ruby
require_relative "oxidb"

db = OxiDb::Client.new("127.0.0.1", 4444)
# or with a block:
# OxiDb::Client.open("127.0.0.1", 4444) { |db| ... }
```

### CRUD

```ruby
# Insert
db.insert("users", { "name" => "Alice", "age" => 30 })
db.insert_many("users", [
  { "name" => "Bob", "age" => 25 },
  { "name" => "Charlie", "age" => 35 }
])

# Find with options
docs = db.find("users", { "name" => "Alice" })
docs = db.find("users", {}, sort: { "age" => 1 }, skip: 0, limit: 10)
doc  = db.find_one("users", { "name" => "Alice" })

# Update
db.update("users", { "name" => "Alice" }, { "$set" => { "age" => 31 } })

# Delete
db.delete("users", { "name" => "Charlie" })

# Count
n = db.count("users")
```

### Collections & Indexes

```ruby
db.create_collection("orders")
db.list_collections
db.drop_collection("orders")

db.create_index("users", "name")
db.create_unique_index("users", "email")
db.create_composite_index("users", ["name", "age"])
```

### Aggregation

```ruby
results = db.aggregate("users", [
  { "$match" => { "age" => { "$gte" => 18 } } },
  { "$group" => { "_id" => "$category", "total" => { "$sum" => "$amount" } } },
  { "$sort"  => { "total" => -1 } }
])
```

### Transactions

```ruby
# Auto-commit on success, auto-rollback on exception
db.transaction do
  db.insert("ledger", { "action" => "debit",  "amount" => 100 })
  db.insert("ledger", { "action" => "credit", "amount" => 100 })
end

# Manual control
db.begin_tx
db.insert("ledger", { "action" => "refund", "amount" => 50 })
db.commit_tx   # or db.rollback_tx
```

### Blob Storage

```ruby
db.create_bucket("files")
db.list_buckets

db.put_object("files", "hello.txt", "Hello from Ruby!",
              content_type: "text/plain", metadata: { "author" => "ruby" })
data, meta = db.get_object("files", "hello.txt")
head = db.head_object("files", "hello.txt")
objs = db.list_objects("files", prefix: "hello", limit: 10)

db.delete_object("files", "hello.txt")
db.delete_bucket("files")
```

### Full-Text Search

```ruby
results = db.search("hello world", bucket: "files", limit: 10)
```

### Compaction

```ruby
stats = db.compact("users") # returns { "old_size" => ..., "new_size" => ..., "docs_kept" => ... }
```

```ruby
db.close
```

## Go

Zero dependencies — uses only the Go standard library. Go 1.21+.

**Install:**

```bash
go get github.com/parisxmas/OxiDB/go/oxidb
```

**Connect:**

```go
import "github.com/parisxmas/OxiDB/go/oxidb"

client, _ := oxidb.ConnectDefault() // 127.0.0.1:4444
defer client.Close()
```

### CRUD

```go
// Insert
client.Insert("users", map[string]any{"name": "Alice", "age": 30})
client.InsertMany("users", []map[string]any{
    {"name": "Bob", "age": 25},
    {"name": "Charlie", "age": 35},
})

// Find with options
docs, _ := client.Find("users", map[string]any{"name": "Alice"}, nil)
limit := 10
docs, _ = client.Find("users", map[string]any{}, &oxidb.FindOptions{
    Sort: map[string]any{"age": 1}, Limit: &limit,
})
doc, _ := client.FindOne("users", map[string]any{"name": "Alice"})

// Update
client.Update("users", map[string]any{"name": "Alice"}, map[string]any{"$set": map[string]any{"age": 31}})

// Delete
client.Delete("users", map[string]any{"name": "Charlie"})

// Count
n, _ := client.Count("users", map[string]any{})
```

### Collections & Indexes

```go
client.CreateCollection("orders")
cols, _ := client.ListCollections()
client.DropCollection("orders")

client.CreateIndex("users", "name")
client.CreateUniqueIndex("users", "email")
client.CreateCompositeIndex("users", []string{"name", "age"})
```

### Aggregation

```go
results, _ := client.Aggregate("users", []map[string]any{
    {"$match": map[string]any{"age": map[string]any{"$gte": 18}}},
    {"$group": map[string]any{"_id": "$category", "total": map[string]any{"$sum": "$amount"}}},
    {"$sort": map[string]any{"total": -1}},
})
```

### Transactions

```go
// Auto-commit on success, auto-rollback on error
client.WithTransaction(func() error {
    client.Insert("ledger", map[string]any{"action": "debit", "amount": 100})
    client.Insert("ledger", map[string]any{"action": "credit", "amount": 100})
    return nil
})

// Manual control
client.BeginTx()
client.Insert("ledger", map[string]any{"action": "refund", "amount": 50})
client.CommitTx()   // or client.RollbackTx()
```

### Blob Storage

```go
client.CreateBucket("files")
client.ListBuckets()

client.PutObject("files", "hello.txt", []byte("Hello from Go!"), "text/plain", map[string]string{"author": "go"})
data, meta, _ := client.GetObject("files", "hello.txt")
head, _ := client.HeadObject("files", "hello.txt")
prefix := "hello"
objs, _ := client.ListObjects("files", &prefix, &limit)

client.DeleteObject("files", "hello.txt")
client.DeleteBucket("files")
```

### Full-Text Search

```go
results, _ := client.Search("hello world", nil, 10)
// or filter by bucket:
bucket := "files"
results, _ = client.Search("hello world", &bucket, 10)
```

### Compaction

```go
stats, _ := client.Compact("users") // map with old_size, new_size, docs_kept
```

## Julia

Julia 1.6+. Two modes available:

### Embedded Mode (no server needed)

Uses the `OxiDbEmbedded` package — prebuilt native library downloaded automatically:

```julia
using Pkg
Pkg.develop(path="julia/OxiDbEmbedded")
```

```julia
using OxiDbEmbedded

db = open_db("/tmp/mydb")
insert(db, "users", Dict("name" => "Alice", "age" => 30))
docs = find(db, "users", Dict("name" => "Alice"))
close(db)
```

Or run the full demo: `julia examples/julia/embedded_example.jl`

### TCP Client Mode

Requires a running OxiDB server. Only dependency is `JSON3`.

**Install:**

```julia
using Pkg
Pkg.develop(path="julia/OxiDb")
```

**Connect:**

```julia
using OxiDb

client = connect_oxidb("127.0.0.1", 4444)
```

### CRUD

```julia
# Insert
insert(client, "users", Dict("name" => "Alice", "age" => 30))
insert_many(client, "users", [
    Dict("name" => "Bob", "age" => 25),
    Dict("name" => "Charlie", "age" => 35)
])

# Find with options
docs = find(client, "users", Dict("name" => "Alice"))
docs = find(client, "users", Dict(); sort=Dict("age" => 1), limit=10, skip=0)
doc  = find_one(client, "users", Dict("name" => "Alice"))

# Update
update(client, "users", Dict("name" => "Alice"), Dict("\$set" => Dict("age" => 31)))

# Delete
delete(client, "users", Dict("name" => "Charlie"))

# Count
n = count_docs(client, "users")
```

### Collections & Indexes

```julia
create_collection(client, "orders")
cols = list_collections(client)
drop_collection(client, "orders")

create_index(client, "users", "name")
create_unique_index(client, "users", "email")
create_composite_index(client, "users", ["name", "age"])
```

### Aggregation

```julia
results = aggregate(client, "users", [
    Dict("\$match" => Dict("age" => Dict("\$gte" => 18))),
    Dict("\$group" => Dict("_id" => nothing, "avg_age" => Dict("\$avg" => "\$age"))),
    Dict("\$sort"  => Dict("avg_age" => -1))
])
```

### Transactions

```julia
# Auto-commit on success, auto-rollback on exception
transaction(client) do
    insert(client, "ledger", Dict("action" => "debit",  "amount" => 100))
    insert(client, "ledger", Dict("action" => "credit", "amount" => 100))
end

# Manual control
begin_tx(client)
insert(client, "ledger", Dict("action" => "refund", "amount" => 50))
commit_tx(client)   # or rollback_tx(client)
```

### Blob Storage

```julia
create_bucket(client, "files")
list_buckets(client)

put_object(client, "files", "hello.txt", Vector{UInt8}("Hello from Julia!");
           content_type="text/plain", metadata=Dict("author" => "julia"))
data, meta = get_object(client, "files", "hello.txt")
head = head_object(client, "files", "hello.txt")
objs = list_objects(client, "files"; prefix="hello", limit=10)

delete_object(client, "files", "hello.txt")
delete_bucket(client, "files")
```

### Full-Text Search

```julia
results = search(client, "hello world"; bucket="files", limit=10)
```

### Compaction

```julia
stats = compact(client, "users")  # returns Dict with old_size, new_size, docs_kept
```

```julia
close(client)
```

## Swift/iOS

Swift 5.9+, macOS 13+, iOS 16+. Two modes: **embedded** (no server, recommended for mobile) and **TCP client**.

### Getting the FFI Libraries

Download prebuilt binaries from [GitHub Releases](https://github.com/parisxmas/OxiDB/releases/latest):

```bash
# Embedded FFI — macOS arm64
curl -LO https://github.com/parisxmas/OxiDB/releases/download/v0.7.0/oxidb-embedded-ffi-macos-arm64.tar.gz
tar xzf oxidb-embedded-ffi-macos-arm64.tar.gz
sudo cp liboxidb_embedded_ffi.dylib liboxidb_embedded_ffi.a /usr/local/lib/
sudo cp oxidb_embedded.h /usr/local/include/

# Embedded FFI — iOS device (arm64)
curl -LO https://github.com/parisxmas/OxiDB/releases/download/v0.7.0/oxidb-embedded-ffi-ios-arm64.tar.gz

# Embedded FFI — iOS simulator (arm64, Apple Silicon)
curl -LO https://github.com/parisxmas/OxiDB/releases/download/v0.7.0/oxidb-embedded-ffi-ios-sim-arm64.tar.gz
```

Or build from source:

```bash
cargo build --release -p oxidb-embedded-ffi                              # macOS
cargo build --release -p oxidb-embedded-ffi --target aarch64-apple-ios   # iOS
```

### Installation (Swift Package Manager)

Add to your `Package.swift`:

```swift
dependencies: [
    .package(path: "../swift/OxiDB")  // adjust path as needed
]
```

### Embedded Mode (no server needed)

```swift
import OxiDB

// Open database
let db = try OxiDBDatabase.open(path: "/path/to/mydb")

// Or with encryption
let db = try OxiDBDatabase.open(path: "/path/to/mydb", encryptionKeyPath: "/path/to/key")

// Insert
try db.insert(collection: "users", document: [
    "name": "Alice", "age": 30, "city": "New York"
])
try db.insertMany(collection: "users", documents: [
    ["name": "Bob", "age": 25],
    ["name": "Charlie", "age": 35]
])

// Query
let users = try db.find(collection: "users", query: ["city": "New York"])
let alice = try db.findOne(collection: "users", query: ["name": "Alice"])

// Update
try db.update(
    collection: "users",
    query: ["name": "Alice"],
    update: ["$set": ["age": 31]]
)

// Delete
try db.delete(collection: "users", query: ["name": "Charlie"])

// Count
let n = try db.count(collection: "users")
```

### Indexes

```swift
try db.createIndex(collection: "users", field: "email")
try db.createUniqueIndex(collection: "users", field: "email")
try db.createCompositeIndex(collection: "users", fields: ["city", "age"])
```

### Full-Text Search

```swift
// Create text index on document fields
try db.createTextIndex(collection: "articles", fields: ["title", "body"])

// Search with TF-IDF ranking
let results = try db.textSearch(collection: "articles", query: "rust programming", limit: 10)
// Each result includes _score field
```

### Aggregation

```swift
let result = try db.aggregate(collection: "users", pipeline: [
    ["$group": ["_id": "city", "count": ["$count": true]]],
    ["$sort": ["count": -1]]
])
```

### Transactions

```swift
// Auto-commit on success, auto-rollback on error
try db.transaction {
    try db.insert(collection: "ledger", document: ["from": "A", "to": "B", "amount": 100])
    try db.insert(collection: "ledger", document: ["from": "B", "to": "C", "amount": 50])
}
```

### Blob Storage

```swift
try db.createBucket("files")
let data = Data("Hello, World!".utf8).base64EncodedString()
try db.putObject(bucket: "files", key: "greeting.txt", dataBase64: data, contentType: "text/plain")
let obj = try db.getObject(bucket: "files", key: "greeting.txt")
```

### Collections

```swift
try db.createCollection("orders")
let cols = try db.listCollections()
try db.dropCollection("orders")
try db.compact(collection: "users")
```

### Client Mode (TCP server)

```swift
import OxiDB

let client = try OxiDBClient.connect(host: "127.0.0.1", port: 4444)
try client.insert(collection: "users", document: ["name": "Alice", "age": 30])
let users = try client.find(collection: "users", query: ["age": ["$gte": 25]])
client.disconnect()
```

### Error Handling

```swift
do {
    let result = try db.find(collection: "users", query: [:])
} catch OxiDBError.databaseOpenFailed {
    print("Failed to open database")
} catch OxiDBError.operationFailed(let msg) {
    print("Operation failed: \(msg)")
} catch OxiDBError.transactionConflict(let msg) {
    print("Transaction conflict: \(msg)")
}
```

See [`swift/README.md`](swift/README.md) for full API reference and [`examples/ios/`](examples/ios/) for a working iOS demo app.

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
