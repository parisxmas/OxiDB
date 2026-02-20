<p align="center">
  <img src="logo.png" alt="OxiDB" width="500">
</p>

<p align="center">A fast, embeddable document database written in Rust. SQL and MongoDB-style queries, single binary, zero configuration, Raft replication, AES-256 encryption, SCRAM-SHA-256 auth, crash-safe WAL, and sub-second failover.</p>

**Client libraries:** [Python](python/) | [Go](go/) | [Java/Spring Boot](oxidb-spring-boot-starter/) | [Julia](julia/) | [.NET](dotnet/) | [Swift/iOS](swift/) | [C FFI](oxidb-client-ffi/)

## Installation

### Option 1: Download a pre-built binary (easiest)

Download the latest release for your platform from [GitHub Releases](https://github.com/parisxmas/OxiDB/releases):

| Platform | Download |
|----------|----------|
| macOS Apple Silicon (M1/M2/M3/M4) | `oxidb-server-macos-arm64.tar.gz` |
| macOS Intel | `oxidb-server-macos-x86_64.tar.gz` |
| Linux x86_64 | `oxidb-server-linux-x86_64.tar.gz` |

```bash
tar xzf oxidb-server-*.tar.gz
./oxidb-server
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
| `OXIDB_GELF_ADDR` | — | GELF UDP endpoint for centralized logging (e.g. `172.17.0.1:12201`) |
| `OXIDB_VERBOSE` | `false` | Enable verbose startup logging (also `--verbose` flag) |
| `OXIDB_NODE_ID` | — | Numeric node ID to enable Raft cluster mode |
| `OXIDB_RAFT_ADDR` | `127.0.0.1:4445` | Raft inter-node communication address |
| `OXIDB_RAFT_PEERS` | — | Comma-separated peer list: `"1=host1:4445,2=host2:4445,3=host3:4445"` |

## Features

- **SQL query language** — `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `CREATE/DROP TABLE`, `CREATE INDEX`, `SHOW TABLES` with `WHERE`, `ORDER BY`, `GROUP BY`, `HAVING`, `JOIN`, `LIMIT`, `OFFSET`
- **Document database** — JSON documents, no schema required, collections auto-created on insert
- **MongoDB-style queries** — `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$exists`, `$regex`, `$and`, `$or`
- **12 update operators** — `$set`, `$unset`, `$inc`, `$mul`, `$min`, `$max`, `$rename`, `$currentDate`, `$push`, `$pull`, `$addToSet`, `$pop`
- **Aggregation pipeline** — 10 stages: `$match`, `$group`, `$sort`, `$skip`, `$limit`, `$project`, `$count`, `$unwind`, `$addFields`, `$lookup`; index-accelerated `$group` for count, sum, min, max, avg
- **Indexes** — field, unique, composite, and full-text indexes with automatic backfill; list and drop support
- **Persistent index cache** — index data (BTreeMap contents) persisted to binary `.fidx`/`.cidx` files; on restart, indexes load from cache in seconds instead of rebuilding from documents (16M docs: ~3s vs ~30min)
- **Zero-copy reads** — `find_one`, `update`, and `delete` use Arc-based document iteration, cloning only matching documents instead of every visited document
- **Transactions** — OCC (optimistic concurrency control) with begin/commit/rollback
- **Blob storage** — S3-style buckets with put/get/head/delete/list and CRC32 etags
- **Full-text search** — automatic text extraction from 10+ formats (HTML, XML, PDF, DOCX, XLSX, images via OCR), TF-IDF ranked search
- **Raft replication** — multi-node cluster via OpenRaft with automatic leader election, HAProxy-compatible health checks, and sub-second failover
- **Change streams** — real-time `watch`/`unwatch` with collection filtering, backpressure handling, and token-based resume
- **JSONB binary storage** — compact binary format for faster serialization; backward-compatible with existing JSON data files
- **Crash-safe** — write-ahead log with CRC32 checksums, verified by SIGKILL recovery tests
- **Encryption at rest** — AES-256-GCM with per-record nonces
- **Security** — TLS transport, SCRAM-SHA-256 authentication, role-based access control (Admin/ReadWrite/Read), audit logging
- **Stored procedures** — JSON-defined multi-step procedures with control flow (`if`/`else`, `abort`, `return`), variable binding, and automatic transaction wrapping
- **Cron scheduler** — built-in background scheduler that runs stored procedures on cron expressions (`"0 3 * * *"`) or fixed intervals (`"30s"`, `"5m"`, `"2h"`), with run history tracking
- **GELF logging** — centralized UDP logging to Graylog/Loki via `OXIDB_GELF_ADDR`
- **Compaction** — reclaim space from deleted documents with atomic file swap
- **Thread-safe** — `RwLock` per collection, concurrent readers never block
- **CLI tool** — interactive shell with MongoDB-style syntax, embedded and client modes
- **Multi-language clients** — Python, Go, Java/Spring Boot, Julia, .NET, Swift/iOS — all zero or minimal dependencies

## SQL Query Language

OxiDB supports SQL as a query interface. SQL statements are parsed and translated to the document engine — no separate storage layer.

### Supported Statements

| Statement | Example |
|-----------|---------|
| `SELECT` | `SELECT * FROM users WHERE age > 21 ORDER BY name LIMIT 10` |
| `SELECT` (aggregate) | `SELECT dept, AVG(salary) FROM employees GROUP BY dept HAVING AVG(salary) > 50000` |
| `SELECT` (join) | `SELECT u.name, o.total FROM users u JOIN orders o ON u._id = o.user_id` |
| `INSERT` | `INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', 25)` |
| `UPDATE` | `UPDATE users SET age = 31 WHERE name = 'Alice'` |
| `DELETE` | `DELETE FROM users WHERE age < 18` |
| `CREATE TABLE` | `CREATE TABLE users (id INT, name TEXT)` |
| `DROP TABLE` | `DROP TABLE users` |
| `CREATE INDEX` | `CREATE INDEX idx_name ON users (name)` |
| `SHOW TABLES` | `SHOW TABLES` |

### WHERE Clause

`=`, `!=`, `<>`, `>`, `>=`, `<`, `<=`, `AND`, `OR`, `IN (...)`, `IS NULL`, `IS NOT NULL`, `LIKE`, `BETWEEN ... AND ...`

### Aggregate Functions

`COUNT(*)`, `COUNT(field)`, `SUM(field)`, `AVG(field)`, `MIN(field)`, `MAX(field)`

### Server Usage

```json
{"cmd": "sql", "query": "SELECT * FROM users WHERE age > 21 ORDER BY name LIMIT 10"}
```

### Client Library Usage

```python
# Python
result = client.sql("SELECT name, age FROM users WHERE age > 21")
```

```go
// Go
result, err := client.SQL("SELECT name, age FROM users WHERE age > 21")
```

```java
// Java
JsonNode result = client.sql("SELECT name, age FROM users WHERE age > 21");
```

```julia
# Julia
result = sql(client, "SELECT name, age FROM users WHERE age > 21")
```

```csharp
// .NET
var result = client.Sql("SELECT name, age FROM users WHERE age > 21");
```

```swift
// Swift
let result = try client.sql(query: "SELECT name, age FROM users WHERE age > 21")
```

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
| `$regex`   | `{"name": {"$regex": "^A", "$options": "i"}}` | Regular expression match   |
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
| `sql`                    | `query`                                            |
| `create_procedure`       | `name`, `params`, `steps`                          |
| `call_procedure`         | `name`, `params?`                                  |
| `list_procedures`        | —                                                  |
| `get_procedure`          | `name`                                             |
| `delete_procedure`       | `name`                                             |
| `create_schedule`        | `name`, `procedure`, `cron` or `every`, `params?`, `enabled?` |
| `list_schedules`         | —                                                  |
| `get_schedule`           | `name`                                             |
| `delete_schedule`        | `name`                                             |
| `enable_schedule`        | `name`                                             |
| `disable_schedule`       | `name`                                             |
| `watch`                  | `collection?`, `resume_after?`                     |
| `unwatch`                | —                                                  |
| `begin_tx`               | —                                                  |
| `commit_tx`              | —                                                  |
| `rollback_tx`            | —                                                  |

## Stored Procedures

Define multi-step procedures as JSON and execute them atomically within a transaction.

```json
{"cmd": "create_procedure", "name": "transfer_funds", "params": ["from", "to", "amount"], "steps": [
  {"step": "find_one", "collection": "accounts", "query": {"account_id": "$param.from"}, "as": "sender"},
  {"step": "if", "condition": {"$expr": {"$lt": ["$sender.balance", "$param.amount"]}},
   "then": [{"step": "abort", "message": "insufficient funds"}]},
  {"step": "update", "collection": "accounts", "query": {"account_id": "$param.from"}, "update": {"$inc": {"balance": -100}}},
  {"step": "update", "collection": "accounts", "query": {"account_id": "$param.to"}, "update": {"$inc": {"balance": 100}}},
  {"step": "return", "value": {"status": "ok"}}
]}
```

### Step Types

| Step | Description |
|------|-------------|
| `find` | Query documents, store result array in `as` variable |
| `find_one` | Query single document, store in `as` variable |
| `insert` | Insert a document |
| `update` | Update matching documents |
| `delete` | Delete matching documents |
| `count` | Count matching documents, store in `as` variable |
| `aggregate` | Run aggregation pipeline, store in `as` variable |
| `set` | Set a variable to a value |
| `if` | Conditional branching with `then`/`else` step arrays |
| `abort` | Rollback transaction and return error |
| `return` | Commit transaction and return value |

Variables use `$param.name` for parameters and `$varname` for step results. Dot-notation is supported for nested access.

## Cron Scheduler

The built-in scheduler runs stored procedures on a schedule. Two modes are supported:

- **Cron expression** — standard 5-field format: `minute hour dom month dow`
- **Interval** — simple repeating duration: `"30s"`, `"5m"`, `"2h"`

```json
// Run a procedure every night at 3:00 AM
{"cmd": "create_schedule", "name": "nightly_cleanup", "procedure": "cleanup_old_records", "params": {"days": 30}, "cron": "0 3 * * *"}

// Run a procedure every 5 minutes
{"cmd": "create_schedule", "name": "health_check", "procedure": "check_status", "every": "5m"}

// List all schedules (includes last_run, last_status, run_count)
{"cmd": "list_schedules"}

// Pause a schedule
{"cmd": "disable_schedule", "name": "nightly_cleanup"}

// Resume a schedule
{"cmd": "enable_schedule", "name": "nightly_cleanup"}

// Delete a schedule
{"cmd": "delete_schedule", "name": "nightly_cleanup"}
```

### Cron Expression Format

```
 ┌───────── minute (0-59)
 │ ┌─────── hour (0-23)
 │ │ ┌───── day of month (1-31)
 │ │ │ ┌─── month (1-12)
 │ │ │ │ ┌─ day of week (0-6, 0=Sun)
 * * * * *
```

Each field supports: `*` (all), `N` (exact), `N-M` (range), `*/N` (step), `N,M,O` (list).

### Schedule Commands

| Command | RBAC | Description |
|---------|------|-------------|
| `create_schedule` | Admin | Create or replace a named schedule |
| `list_schedules` | Read | List all schedules with status |
| `get_schedule` | Read | Get a schedule by name |
| `delete_schedule` | Admin | Delete a schedule |
| `enable_schedule` | ReadWrite | Enable a paused schedule |
| `disable_schedule` | ReadWrite | Pause a schedule |

The scheduler thread starts automatically with the server. Schedule state (last run time, status, error, run count) is persisted in the `_schedules` system collection.

## Raft Cluster

Multi-node replication via [OpenRaft](https://github.com/databendlabs/openraft). All writes go through Raft consensus; reads execute locally. Setting `OXIDB_NODE_ID` activates cluster mode with an async tokio runtime.

```bash
# Build with cluster support
cargo build --release -p oxidb-server --features cluster

# Node 1
OXIDB_NODE_ID=1 OXIDB_RAFT_ADDR=0.0.0.0:4445 OXIDB_ADDR=0.0.0.0:4444 \
  OXIDB_RAFT_PEERS=1=node1:4445,2=node2:4445,3=node3:4445 \
  OXIDB_DATA=./data1 ./target/release/oxidb-server

# Node 2
OXIDB_NODE_ID=2 OXIDB_RAFT_ADDR=0.0.0.0:4445 OXIDB_ADDR=0.0.0.0:4444 \
  OXIDB_RAFT_PEERS=1=node1:4445,2=node2:4445,3=node3:4445 \
  OXIDB_DATA=./data2 ./target/release/oxidb-server

# Node 3
OXIDB_NODE_ID=3 OXIDB_RAFT_ADDR=0.0.0.0:4445 OXIDB_ADDR=0.0.0.0:4444 \
  OXIDB_RAFT_PEERS=1=node1:4445,2=node2:4445,3=node3:4445 \
  OXIDB_DATA=./data3 ./target/release/oxidb-server
```

Then initialize the cluster via any client:

```json
{"cmd": "raft_init"}
{"cmd": "raft_add_learner", "node_id": 2, "addr": "node2:4445"}
{"cmd": "raft_add_learner", "node_id": 3, "addr": "node3:4445"}
{"cmd": "raft_change_membership", "members": [1, 2, 3]}
```

A ready-to-use 3-node cluster with HAProxy is included in `tests/cluster/`.

| Raft Command | Description |
|---------|-------------|
| `raft_init` | Initialize single-node cluster |
| `raft_add_learner` | Add a node as learner (`node_id`, `addr`) |
| `raft_change_membership` | Promote learners to voters (`members` array) |
| `raft_metrics` | Get node state, term, leader ID, log indices |

## Change Streams

Subscribe to real-time change events (insert, update, delete) on a collection or the entire database.

```json
// Watch all collections
{"cmd": "watch"}

// Watch a specific collection
{"cmd": "watch", "collection": "users"}

// Resume from a specific point (after reconnect)
{"cmd": "watch", "resume_after": 42}
```

The server responds with `{"ok": true, "data": "watching"}` then pushes events:

```json
{"event": "change", "data": {"op": "insert", "collection": "users", "doc": {...}, "seq": 43}}
{"event": "change", "data": {"op": "update", "collection": "users", "doc": {...}, "seq": 44}}
{"event": "change", "data": {"op": "delete", "collection": "users", "id": "abc123", "seq": 45}}
```

If the client falls behind, an overflow notification is sent:

```json
{"event": "overflow", "data": {"dropped": 12}}
```

Send `{"cmd": "unwatch"}` to stop receiving events and return to normal request mode.

> **Note:** Watch requires Admin role when authentication is enabled. Not available over TLS connections in standalone mode.

## Benchmark vs MongoDB

1M documents, 20 fields each, 8 indexed fields. Single node, same hardware.

| Category | Operation | OxiDB | MongoDB | Winner |
|----------|-----------|-------|---------|--------|
| **INSERT** | 1M docs (batch 500) | 25.5s | 7.8s | MongoDB |
| **QUERIES** | Equality (indexed) | 0ms | 1ms | OxiDB |
| | Range (indexed) | 3ms | 5ms | OxiDB |
| | Regex | 173ms | 334ms | OxiDB |
| | Unindexed scan | 140ms | 92ms | MongoDB |
| | Sort + limit (indexed) | 1ms | 2ms | OxiDB |
| | Multi-condition AND | 1ms | 1ms | Tie |
| | Count (indexed) | 0ms | 13ms | OxiDB |
| | find_one (unindexed) | 140ms | 92ms | MongoDB |
| **AGGREGATION** | Group + count (indexed) | 13ms | 73ms | OxiDB |
| | Group + avg | 472ms | 345ms | MongoDB |
| | Group + count (filtered) | 13ms | 41ms | OxiDB |
| | Group + count (all) | 13ms | 132ms | OxiDB |

**Score: OxiDB wins 21/27 operations.** OxiDB is faster on indexed queries, counts, regex, and aggregation counts. MongoDB is faster on raw inserts and unindexed full-collection scans.

Benchmark source: [`examples/docker_benchmark/`](examples/docker_benchmark/)

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

### Performance Optimizations

- **Zero-copy iteration** — unindexed `find_one`, `update`, and `delete` use `Arc<Value>` references instead of cloning every visited document; only matching documents are cloned
- **Zero-copy batch insert** — `insert_many` passes byte slices to the storage layer by reference, eliminating a full copy of all serialized documents
- **Index-accelerated aggregation** — `$group` with count/sum accumulators uses `FieldIndex::iter_asc()` to read group counts directly from the B-tree index without touching documents; `$group` with min/max/avg uses index-partitioned iteration to eliminate HashMap overhead

### IndexValue Type Ordering

```
Null < Boolean < Integer/Float < DateTime < String
```

Date strings (ISO 8601, RFC 3339, `YYYY-MM-DD`) are automatically stored as epoch milliseconds for fast integer comparison.

## License

This project is licensed under the [MIT License](LICENSE).
