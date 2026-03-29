<p align="center">
  <img src="logo.png" alt="OxiDB" width="500">
</p>

<p align="center">A fast, embeddable document database written in Rust. SQL and JSON queries, S3-compatible API, Redis-compatible in-memory store, MQTT messaging, hash sharding, Raft replication, AES-256 encryption, crash-safe WAL, single binary, zero configuration.</p>

**Client libraries:** [Python](python/) | [Go](go/) | [Java/Spring Boot](oxidb-spring-boot-starter/) | [Julia](julia/) | [.NET](dotnet/) | [Swift/iOS](swift/) | [C FFI](oxidb-client-ffi/) | [VS Code Extension](oxidb-vscode/)

## Installation

### Option 1: Download a pre-built binary (easiest)

Download the latest release for your platform from [GitHub Releases](https://github.com/parisxmas/OxiDB/releases):

| Platform | Download |
|----------|----------|
| macOS Apple Silicon (M1/M2/M3/M4) | [`oxidb-server-macos-arm64.tar.gz`](https://github.com/parisxmas/OxiDB/releases/latest/download/oxidb-server-macos-arm64.tar.gz) |
| macOS Intel | [`oxidb-server-macos-x86_64.tar.gz`](https://github.com/parisxmas/OxiDB/releases/latest/download/oxidb-server-macos-x86_64.tar.gz) |
| Linux x86_64 | [`oxidb-server-linux-x86_64.tar.gz`](https://github.com/parisxmas/OxiDB/releases/latest/download/oxidb-server-linux-x86_64.tar.gz) |

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
| `OXIDB_GELF_ADDR` | — | GELF UDP endpoint for centralized logging (e.g. `192.0.2.100:12201`) |
| `OXIDB_VERBOSE` | `false` | Enable verbose startup logging (also `--verbose` flag) |
| `OXIDB_NODE_ID` | — | Numeric node ID to enable Raft cluster mode |
| `OXIDB_RAFT_ADDR` | `127.0.0.1:4445` | Raft inter-node communication address |
| `OXIDB_RAFT_PEERS` | — | Comma-separated peer list: `"1=host1:4445,2=host2:4445,3=host3:4445"` |
| `OXIDB_OXIMEM_PORT` | — | Enable OxiMem (Redis-compatible) RESP listener on this port |
| `OXIDB_OXIMEM_SQL` | `false` | Mirror OxiMem data to OxiDB collections for SQL querying |
| `OXIDB_MQTT_PORT` | — | Enable MQTT v3.1.1 listener on this port |
| `OXIDB_S3_PORT` | — | Enable S3-compatible HTTP API on this port |
| `OXIDB_S3_ACCESS_KEY` | — | S3 access key for AWS SigV4 authentication |
| `OXIDB_S3_SECRET_KEY` | — | S3 secret key for AWS SigV4 authentication |
| `OXIDB_S3_CREDENTIALS` | — | Path to S3 credentials file |
| `OXIDB_S3_ENCRYPTION_KEY` | — | Hex-encoded 32-byte AES-256 key for S3 SSE |
| `OXIDB_S3_DEFAULT_ENCRYPTION` | `false` | Encrypt all S3 objects by default |
| `OXIDB_BTREE` | `true` | Use B-tree storage engine (set `false` for legacy append-only) |
| `OXIDB_LOG_COMMANDS` | `false` | Log OxiMem/MQTT commands |

## Features

- **SQL query language** — `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `CREATE/DROP TABLE`, `CREATE INDEX`, `SHOW TABLES` with `WHERE`, `ORDER BY`, `GROUP BY`, `HAVING`, `JOIN`, `LIMIT`, `OFFSET`
- **Document database** — JSON documents, no schema required, collections auto-created on insert
- **JSON-based queries** — `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$exists`, `$regex`, `$and`, `$or`
- **12 update operators** — `$set`, `$unset`, `$inc`, `$mul`, `$min`, `$max`, `$rename`, `$currentDate`, `$push`, `$pull`, `$addToSet`, `$pop`
- **Aggregation pipeline** — 11 stages: `$match`, `$group`, `$sort`, `$skip`, `$limit`, `$project`, `$count`, `$unwind`, `$addFields`, `$lookup`, `$out`; index-accelerated `$group` for count, sum, min, max, avg
- **Indexes** — field, unique, composite, full-text, and vector indexes with automatic backfill; list and drop support
- **Vector search** — k-nearest-neighbor similarity search with cosine, Euclidean, and dot product metrics; flat (exact) for small collections, HNSW (approximate) for large; zero external dependencies
- **B-tree storage engine** (default) — DashMap-based concurrent document storage with interior mutability; reads never block reads, writes to different documents proceed in parallel; PagedFieldIndex (sorted Vec with binary search) for cache-friendly index access
- **Memory-mapped indexes** — primary index (`.pidx`) and field indexes (`.fidx2`) use mmap for zero-startup-cost loading; OS pages in data on demand, no RAM preloading (append-only engine)
- **Zero-copy reads** — `find_one`, `update`, and `delete` use Arc-based document iteration, cloning only matching documents instead of every visited document
- **Transactions** — OCC (optimistic concurrency control) with begin/commit/rollback
- **S3-compatible API** — full HTTP REST API with path-style requests, multipart upload, range reads, object tagging, copy, conditional requests, SSE-S3/SSE-C encryption; compatible with AWS CLI and boto3
- **OxiMem (Redis-compatible)** — in-memory key-value store with RESP wire protocol; 50+ commands (strings, hashes, lists, sets, sorted sets, pub/sub); optional SQL mirroring to OxiDB collections
- **MQTT v3.1.1** — publish/subscribe messaging with cross-protocol bridging to OxiMem pub/sub channels
- **Hash sharding** — OxiPool proxy with CRC32 hash routing, scatter-gather queries, per-collection shard keys, and cross-shard transaction detection
- **Blob storage** — S3-style buckets with put/get/head/delete/list and CRC32 etags
- **Full-text search** — automatic text extraction from 10+ formats (HTML, XML, PDF, DOCX, XLSX, images via OCR), TF-IDF ranked search
- **Raft replication** — multi-node cluster via OpenRaft with automatic leader election, HAProxy-compatible health checks, and sub-second failover
- **Change streams** — real-time `watch`/`unwatch` with collection filtering, backpressure handling, and token-based resume
- **JSONB binary storage** — compact binary format for faster serialization; backward-compatible with existing JSON data files
- **Crash-safe** — write-ahead log with CRC32 checksums, verified by SIGKILL recovery tests
- **Encryption at rest** — AES-256-GCM with per-record nonces
- **Security** — TLS transport, SCRAM-SHA-256 authentication, role-based access control (Admin/ReadWrite/Read), audit logging
- **OxiScript** — lightweight stored procedure language; `proc transfer(from, to, amount) { ... }` compiles to JSON steps; supports if/else, variable binding, field access, all DB operations, and procedure-calling-procedure
- **Stored procedures** — JSON-defined or OxiScript multi-step procedures with control flow (`if`/`else`, `abort`, `return`), variable binding, and automatic transaction wrapping
- **Cron scheduler** — built-in background scheduler that runs stored procedures on cron expressions (`"0 3 * * *"`) or fixed intervals (`"30s"`, `"5m"`, `"2h"`), with run history tracking
- **GELF logging** — centralized UDP logging to Graylog/Loki via `OXIDB_GELF_ADDR`
- **Compaction** — reclaim space from deleted documents with atomic file swap
- **Concurrent access** — DashMap storage + RwLock-per-index interior mutability allows lock-free document reads and fine-grained write concurrency; 72K mixed ops/sec with 10 concurrent workers
- **Stripe-level locking** — 16 internal stripes per collection in append-only engine; parallel index building with rayon uses all CPU cores
- **VS Code extension** — collection browser, MongoDB-style query editor, OxiScript syntax highlighting
- **CLI tool** — interactive shell with JSON-based syntax, embedded and client modes
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
| `$nin`     | `{"cat": {"$nin": ["a", "b"]}}`          | Value not in array         |
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
| `$out`        | Write pipeline results to a target collection      |

### Accumulators (for `$group`)

`$sum`, `$avg`, `$min`, `$max`, `$count`, `$first`, `$last`, `$push`, `$addToSet`

### Expressions

Field references (`"$fieldName"`), literals, and operators:

- **Arithmetic:** `$add`, `$subtract`, `$multiply`, `$divide`, `$mod`
- **String:** `$concat`, `$toLower`, `$toUpper`, `$substr`, `$trim`, `$split`
- **Date:** `$year`, `$month`, `$dayOfMonth`, `$hour`, `$minute`, `$second`, `$dayOfWeek`
- **Conditional:** `$cond`, `$ifNull`
- **Array:** `$size`

Dot-notation supported for nested fields.

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

## OxiMem (Redis-Compatible In-Memory Store)

OxiMem is a built-in in-memory key-value store with full RESP wire protocol compatibility. Connect with `redis-cli`, any Redis client library, or the Redis SDK.

```bash
# Enable OxiMem on port 6379
OXIDB_OXIMEM_PORT=6379 ./oxidb-server

# Connect with redis-cli
redis-cli -p 6379
> SET user:1 '{"name":"Alice"}'
> GET user:1
```

### Supported Commands

| Category | Commands |
|----------|----------|
| **String** | `SET`, `GET`, `GETSET`, `SETNX`, `SETEX`, `PSETEX`, `MSET`, `MGET`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `INCRBYFLOAT`, `APPEND`, `STRLEN`, `GETRANGE` |
| **Key** | `DEL`, `EXISTS`, `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PERSIST`, `TTL`, `PTTL`, `TYPE`, `KEYS`, `RENAME`, `RANDOMKEY`, `DBSIZE`, `FLUSHDB`, `FLUSHALL`, `SCAN` |
| **Hash** | `HSET`, `HMSET`, `HGET`, `HMGET`, `HDEL`, `HEXISTS`, `HGETALL`, `HKEYS`, `HVALS`, `HLEN`, `HINCRBY`, `HSETNX` |
| **List** | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`, `LINDEX` |
| **Set** | `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SCARD` |
| **Sorted Set** | `ZADD`, `ZREM`, `ZSCORE`, `ZRANK`, `ZREVRANK`, `ZRANGE`, `ZREVRANGE`, `ZRANGEBYSCORE`, `ZREVRANGEBYSCORE`, `ZCARD`, `ZCOUNT`, `ZINCRBY`, `ZPOPMIN`, `ZPOPMAX` |
| **Pub/Sub** | `PUBLISH`, `SUBSCRIBE`, `UNSUBSCRIBE` |
| **Server** | `PING`, `ECHO`, `QUIT`, `SELECT`, `COMMAND`, `CLIENT`, `AUTH`, `INFO`, `CONFIG` |

Set `OXIDB_OXIMEM_SQL=true` to mirror all OxiMem data to OxiDB collections (`_kv`, `_hash`, `_list`, `_set`), making it queryable via SQL.

## MQTT v3.1.1

Built-in MQTT message broker with cross-protocol bridging to OxiMem pub/sub channels.

```bash
# Enable MQTT on port 1883
OXIDB_MQTT_PORT=1883 ./oxidb-server

# Publish from MQTT, receive in redis-cli (or vice versa)
mosquitto_pub -t "sensors/temp" -m '{"value": 22.5}'
```

Supports CONNECT, PUBLISH (QoS 0-1), SUBSCRIBE, UNSUBSCRIBE, PINGREQ/PINGRESP, and DISCONNECT. MQTT and OxiMem RESP share the same pub/sub infrastructure — a message published via MQTT is delivered to OxiMem SUBSCRIBE listeners and vice versa.

## S3-Compatible API

Full S3-compatible HTTP REST API. Works with AWS CLI, boto3, and any S3-compatible client.

```bash
# Enable S3 API on port 9000 with authentication
OXIDB_S3_PORT=9000 OXIDB_S3_ACCESS_KEY=mykey OXIDB_S3_SECRET_KEY=mysecret ./oxidb-server

# Use with AWS CLI
aws --endpoint-url http://localhost:9000 s3 mb s3://mybucket
aws --endpoint-url http://localhost:9000 s3 cp file.txt s3://mybucket/file.txt
aws --endpoint-url http://localhost:9000 s3 ls s3://mybucket
```

### S3 Features

- **Bucket operations** — create, delete, list
- **Object operations** — put, get, head, delete, list (with prefix filtering)
- **Multipart upload** — initiate, upload parts, complete, abort, list parts
- **Object copy** — server-side copy via `x-amz-copy-source` header
- **Range requests** — partial reads with `Range` header
- **Conditional requests** — `If-Match`, `If-None-Match`, `If-Modified-Since`, `If-Unmodified-Since`
- **Object tagging** — put, get, delete tags
- **Batch delete** — delete multiple objects in a single request
- **Authentication** — AWS Signature V4
- **Server-side encryption** — SSE-S3 (server-managed key) and SSE-C (customer-provided key), both AES-256-GCM

## Sharding (OxiPool)

Hash-based sharding proxy that distributes data across multiple OxiDB nodes.

```bash
# Start shards
OXIDB_ADDR=0.0.0.0:4444 OXIDB_SHARD_ID=0 OXIDB_DATA=./shard0 ./oxidb-server &
OXIDB_ADDR=0.0.0.0:4445 OXIDB_SHARD_ID=1 OXIDB_DATA=./shard1 ./oxidb-server &

# Start OxiPool proxy
OXIPOOL_SHARDS=localhost:4444,localhost:4445 \
  OXIPOOL_SHARD_KEYS=orders:customer_id,users:region \
  ./oxipool
```

### Sharding Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `OXIPOOL_SHARDS` | — | Comma-separated shard endpoints |
| `OXIPOOL_SHARD_CONFIG` | — | JSON config file path (overrides `OXIPOOL_SHARDS`) |
| `OXIPOOL_NUM_CHUNKS` | `256` | Virtual chunk count (must be power of 2) |
| `OXIPOOL_SHARD_KEYS` | — | Per-collection shard keys (e.g. `orders:customer_id,users:region`) |
| `OXIDB_SHARD_ID` | — | Shard numeric ID for ID range prefixing |

### Routing

- **Targeted** — operations with shard key in query/document route to a single shard
- **Scatter-gather** — queries without shard key fan out to all shards and merge results
- **Broadcast** — DDL operations (create/drop collection, index management)
- **Primary-only** — admin commands (`list_collections`, etc.)

Cross-shard transactions are detected and rejected.

## Benchmark: OxiDB vs MongoDB 8

100K documents, 14 fields each, 8 indexed fields. Native Apple Silicon, B-tree engine. OxiDB wins 17/18 tests.

| Category | Operation | OxiDB | MongoDB | Winner |
|----------|-----------|-------|---------|--------|
| **INSERT** | 100K docs (batch 5000) | 2.3s | 2.9s | OxiDB 1.2x |
| **INDEX** | 8 indexes | 101ms | 2.7s | OxiDB 27x |
| **QUERY** | Exact match (indexed) | 240µs | 1ms | OxiDB 7x |
| | Equality (indexed) | 1ms | 54ms | OxiDB 31x |
| | Range (indexed) | 4ms | 120ms | OxiDB 28x |
| | Range + equality | 17ms | 45ms | OxiDB 2.6x |
| | Multi-condition AND | 4ms | 25ms | OxiDB 6x |
| | Unindexed scan | 60ms | 59ms | Tied |
| | find_one (indexed) | 156µs | 331µs | OxiDB 2x |
| | Count (indexed) | 70µs | 1ms | OxiDB 16x |
| | Sort + limit 10 (indexed) | 140µs | 496µs | OxiDB 3.5x |
| **UPDATE** | UpdateOne (indexed) | 84µs | 333µs | OxiDB 4x |
| | UpdateMany (bulk) | 1ms | 31ms | OxiDB 20x |
| **AGGREGATE** | Group by dept + avg salary | 19ms | 26ms | OxiDB 1.4x |
| | Match region + group dept | 17ms | 18ms | OxiDB 1.1x |
| | Group by city + full stats | 20ms | 33ms | OxiDB 1.6x |
| **CONCURRENT** | find_one (10 workers) | 11ms | 14ms | OxiDB 1.4x |
| **DELETE** | DeleteMany | 4ms | 970ms | OxiDB 215x |

**Concurrent mixed workload** (10 workers, 70% read / 20% update / 10% insert): OxiDB 72K ops/sec vs MongoDB 43K ops/sec.

Benchmark source: [`tests/benchmark-1m/`](tests/benchmark-1m/)

## Architecture

### Storage Engines

OxiDB provides two storage engines, selected via `OXIDB_BTREE` (default: `true`):

**B-tree engine** (default) — `DashMap<doc_id, JSONB bytes>` with concurrent access. Documents are stored in a sharded hash map that allows lock-free reads and parallel writes. Indexes use `PagedFieldIndex` (sorted `Vec` with binary search) wrapped in per-index `RwLock` for fine-grained concurrency. Persistence via periodic serialization to `.btree` files.

**Append-only engine** — each collection is a `.dat` file: `[status: u8][length: u32 LE][JSONB bytes]`. Deletes flip the status byte in place. Uses memory-mapped primary index (`.pidx`) and field indexes (`.fidx2`) for zero-startup-cost loading. Write-ahead log with CRC32 checksums and 3-fsync protocol for crash safety. 16-stripe locking for write concurrency.

### Concurrency Model

The B-tree engine uses **interior mutability** — each component handles its own locking:
- **Storage** — `DashMap` (sharded concurrent hash map): reads and writes to different documents proceed in parallel
- **Indexes** — `RwLock` per index group: queries (read lock) never block other queries; writes (write lock) only block during index mutation
- **Document cache** — 16-shard LRU with per-shard `Mutex`: near-zero contention for cached reads
- **ID generation** — `AtomicU64`: lock-free sequential ID allocation

This eliminates the collection-level write lock that previously serialized all operations.

### Write-Ahead Log (.wal files)

Every mutation in the append-only engine is logged before touching the data file. Batch operations use a 3-fsync protocol: WAL write + fsync, data mutations + fsync, WAL checkpoint + fsync. On startup the WAL is replayed idempotently and then truncated.

### Performance Optimizations

- **Concurrent document access** — DashMap storage allows parallel reads/writes without collection-level locking
- **Index-level sort+limit** — sort queries with limit use index iteration with cross-index membership checks, avoiding full document loading
- **Dirty-flag persistence** — background sync only writes to disk when data has changed, eliminating redundant I/O
- **Cache-accelerated aggregation** — streaming aggregation checks the doc cache before decoding JSONB bytes; unordered iteration for grouping avoids unnecessary key sorting
- **Parallel JSONB encoding** — `insert_many` encodes documents in parallel using rayon before acquiring any locks
- **Zero-copy iteration** — `find_one`, `update`, and `delete` use `Arc<Value>` references, cloning only matching documents
- **Index-accelerated aggregation** — `$group` with count/sum accumulators reads group counts directly from the index without touching documents

### IndexValue Type Ordering

```
Null < Boolean < Integer/Float < DateTime < String
```

Date strings (ISO 8601, RFC 3339, `YYYY-MM-DD`) are automatically stored as epoch milliseconds for fast integer comparison.

## License

This project is licensed under the [MIT License](LICENSE).
