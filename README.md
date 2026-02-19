<p align="center">
  <img src="logo.png" alt="OxiDB" width="500">
</p>

<p align="center">A fast, embeddable document database written in Rust with Raft replication. Works like MongoDB but runs as a single binary with zero configuration.</p>

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

```bash
git clone https://github.com/parisxmas/OxiDB.git
cd OxiDB
docker compose up -d
```

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `OXIDB_ADDR` | `127.0.0.1:4444` | Listen address and port |
| `OXIDB_DATA` | `./oxidb_data` | Data directory |
| `OXIDB_POOL_SIZE` | `4` | Worker thread count |
| `OXIDB_IDLE_TIMEOUT` | `30` | Idle connection timeout in seconds (0 = no timeout) |
| `OXIDB_ENCRYPTION_KEY` | — | Path to 32-byte AES-256 key file for encryption at rest |
| `OXIDB_TLS_CERT` | — | Path to TLS certificate PEM file |
| `OXIDB_TLS_KEY` | — | Path to TLS private key PEM file |
| `OXIDB_AUTH` | `false` | Enable SCRAM-SHA-256 authentication |
| `OXIDB_AUDIT` | `false` | Enable audit logging |
| `OXIDB_NODE_ID` | — | Numeric node ID to enable Raft replication |
| `OXIDB_RAFT_ADDR` | `127.0.0.1:4445` | Raft inter-node communication address |
| `OXIDB_RAFT_PEERS` | — | Comma-separated peer list: `"2=host2:4445,3=host3:4445"` |

## Features

- **Document database** — JSON documents, no schema required, collections auto-created on insert
- **MongoDB-style queries** — `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$exists`, `$and`, `$or`
- **12 update operators** — `$set`, `$unset`, `$inc`, `$mul`, `$min`, `$max`, `$rename`, `$currentDate`, `$push`, `$pull`, `$addToSet`, `$pop`
- **Aggregation pipeline** — 10 stages: `$match`, `$group`, `$sort`, `$skip`, `$limit`, `$project`, `$count`, `$unwind`, `$addFields`, `$lookup`
- **Indexes** — field, unique, composite, and full-text indexes with automatic backfill; list and drop support
- **Persistent index cache** — index data (BTreeMap contents) persisted to binary `.fidx`/`.cidx` files; on restart, indexes load from cache in seconds instead of rebuilding from documents (16M docs: ~3s vs ~30min)
- **Transactions** — OCC (optimistic concurrency control) with begin/commit/rollback
- **Blob storage** — S3-style buckets with put/get/head/delete/list and CRC32 etags
- **Full-text search** — automatic text extraction from 10+ formats (HTML, XML, PDF, DOCX, XLSX, images via OCR), TF-IDF ranked search
- **Raft replication** — multi-node cluster via OpenRaft with automatic leader election, HAProxy-compatible health checks, and sub-second failover
- **JSONB binary storage** — compact binary format for faster serialization; backward-compatible with existing JSON data files
- **Crash-safe** — write-ahead log with CRC32 checksums, verified by SIGKILL recovery tests
- **Encryption at rest** — AES-256-GCM with per-record nonces
- **Security** — TLS transport, SCRAM-SHA-256 authentication, role-based access control (Admin/ReadWrite/Read), audit logging
- **Compaction** — reclaim space from deleted documents with atomic file swap
- **Thread-safe** — `RwLock` per collection, concurrent readers never block
- **CLI tool** — interactive shell with MongoDB-style syntax, embedded and client modes
- **Multi-language clients** — Python, Go, Ruby, Java/Spring Boot, Julia, PHP, .NET, Swift/iOS — all zero or minimal dependencies

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

Multiple conditions on different fields are implicitly ANDed.

## Update Operators

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

All operators support dot-notation for nested fields.

## Aggregation Pipeline

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

Field references (`"$fieldName"`), literals, and arithmetic operators (`$add`, `$subtract`, `$multiply`, `$divide`). Dot-notation supported for nested fields.

## TCP Protocol

### Wire Format

Messages are length-prefixed JSON over TCP:

```
[u32 LE length][JSON bytes]
```

Max message size is 16 MiB.

### Commands

| Command                  | Fields                                             |
|--------------------------|----------------------------------------------------|
| `ping`                   | —                                                  |
| `insert`                 | `collection`, `doc`                                |
| `insert_many`            | `collection`, `docs`                               |
| `find`                   | `collection`, `query`, `sort?`, `skip?`, `limit?`  |
| `find_one`               | `collection`, `query`                              |
| `update`                 | `collection`, `query`, `update`                    |
| `update_one`             | `collection`, `query`, `update`                    |
| `delete`                 | `collection`, `query`                              |
| `delete_one`             | `collection`, `query`                              |
| `count`                  | `collection`, `query?`                             |
| `create_index`           | `collection`, `field`                              |
| `create_unique_index`    | `collection`, `field`                              |
| `create_composite_index` | `collection`, `fields`                             |
| `create_text_index`      | `collection`, `fields`                             |
| `list_indexes`           | `collection`                                       |
| `drop_index`             | `collection`, `index`                              |
| `text_search`            | `collection`, `query`, `limit?`                    |
| `create_collection`      | `collection`                                       |
| `list_collections`       | —                                                  |
| `drop_collection`        | `collection`                                       |
| `aggregate`              | `collection`, `pipeline`                           |
| `compact`                | `collection`                                       |
| `create_bucket`          | `bucket`                                           |
| `list_buckets`           | —                                                  |
| `delete_bucket`          | `bucket`                                           |
| `put_object`             | `bucket`, `key`, `data` (base64), `content_type?`, `metadata?` |
| `get_object`             | `bucket`, `key`                                    |
| `head_object`            | `bucket`, `key`                                    |
| `delete_object`          | `bucket`, `key`                                    |
| `list_objects`           | `bucket`, `prefix?`, `limit?`                      |
| `search`                 | `query`, `bucket?`, `limit?`                       |

## Raft Cluster

Multi-node replication via [OpenRaft](https://github.com/databendlabs/openraft). All writes go through Raft consensus; reads execute locally.

```bash
# Node 1 (initial leader)
OXIDB_NODE_ID=1 OXIDB_RAFT_ADDR=0.0.0.0:4445 OXIDB_ADDR=0.0.0.0:4444 ./oxidb-server

# Node 2
OXIDB_NODE_ID=2 OXIDB_RAFT_ADDR=0.0.0.0:4445 OXIDB_ADDR=0.0.0.0:4444 ./oxidb-server

# Node 3
OXIDB_NODE_ID=3 OXIDB_RAFT_ADDR=0.0.0.0:4445 OXIDB_ADDR=0.0.0.0:4444 ./oxidb-server
```

A ready-to-use 3-node cluster with HAProxy is included in `tests/cluster/`.

| Raft Command | Description |
|---------|-------------|
| `raft_init` | Initialize single-node cluster |
| `raft_add_learner` | Add a node as learner (`node_id`, `addr`) |
| `raft_change_membership` | Promote learners to voters (`members` array) |
| `raft_metrics` | Get node state, term, leader ID, log indices |

## Performance

All benchmarks on the same Linux server (Debian 6.1, x86_64, 8 GB RAM), both databases in Docker. OxiDB vs MongoDB 6.0, best of 3 runs.

### 1 Million Documents (8 indexes)

| Category | OxiDB vs MongoDB |
|----------|-----------------|
| Count operations | **8x-987x** faster |
| Sorted find + limit | **2x-2.4x** faster |
| Filtered aggregation ($match + $group) | **1.4x-1.9x** faster |
| Indexed field queries (large result sets) | **1x-1.8x** faster |
| Bulk inserts (1M docs) | MongoDB **2.2x** faster |
| Single-doc updates | MongoDB significantly faster |
| Full-table aggregation (no $match) | MongoDB **1.8x-2x** faster |
| Unindexed full scans | MongoDB **8x-15x** faster |

### Small-Medium Collections (5-1K docs)

**OxiDB wins 25 of 30 tests (1.73x overall faster)**

| Category | Highlights |
|----------|-----------|
| Index creation | **34x-66x** faster |
| Find queries | **2x-3.5x** faster |
| Count | **2.7x-6.4x** faster |
| Aggregation | **2.4x-3.9x** faster |

OxiDB excels at **read-heavy workloads** — counts, sorted pagination, filtered aggregations, indexed lookups. MongoDB wins on **write-heavy operations** — bulk inserts, in-place updates, full-collection scans.

## Architecture

### Storage (.dat files)

Each collection is an append-only file: `[status: u8][length: u32 LE][JSONB bytes]`. Deletes flip the status byte in place.

### Index Persistence (.fidx / .cidx files)

Field and composite index data is cached to binary files with a validated header:

```
[MAGIC: "OXIX"][VERSION: u32][DOC_COUNT: u64][NEXT_ID: u64][BODY_CRC32: u32][BODY_LEN: u64][BODY]
```

On startup, if the cache matches the current doc_count and next_id, indexes load from the binary cache in seconds. If the cache is stale or corrupt, indexes are rebuilt from the `.dat` file transparently. Writes use atomic tmp+rename to prevent corruption.

### Write-Ahead Log (.wal files)

Every mutation is logged before touching the data file. Batch operations use a 3-fsync protocol: WAL write + fsync, data mutations + fsync, WAL checkpoint + fsync. On startup the WAL is replayed idempotently and then truncated. WAL recovery also updates loaded index caches.

### IndexValue Type Ordering

```
Null < Boolean < Integer/Float < DateTime < String
```

Date strings (ISO 8601, RFC 3339, `YYYY-MM-DD`) are automatically stored as epoch milliseconds for fast integer comparison.

## License

This project is licensed under the [MIT License](LICENSE).
