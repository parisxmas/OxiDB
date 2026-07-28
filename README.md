<p align="center">
  <img src="logo.png" alt="OxiDB" width="500">
</p>

<p align="center">A fast, embeddable document database written in Rust. JSON queries, S3-compatible API, Redis-compatible in-memory store, MQTT messaging, GELF log ingestion with auto-indexing, alerting, retention policies, GPU-accelerated vector search, hash sharding, Raft replication, AES-256 encryption, crash-safe WAL, single binary, zero configuration.</p>

<p align="center"><strong>Client libraries:</strong> <a href="python/">Python</a> · <a href="go/">Go</a> · <a href="julia/">Julia</a> · <a href="dotnet/">.NET</a> · <a href="swift/">Swift/iOS</a> · <a href="oxidb-js/">JS/TS</a> · <a href="oxidb-client-ffi/">C FFI</a> · <a href="oxidb-vscode/">VS Code</a></p>

---

> ⚠️ **WARNING — pre-1.0 stability.** OxiDB is under active pre-1.0 development. The on-disk data format, the wire/server protocol, the client SDK surface, and the JSON query language are all subject to **breaking changes between releases** with no migration path or backward-compatibility guarantee. Pin a specific version, expect to dump-and-reload on upgrade, and treat any production-like use as experimental until a `1.0` release explicitly commits to stability. The test suite protects against regressions *within* a version, not breaking changes *across* versions.

**Adoption status:** pre-1.0. Path to 1.0 stability is in [ADR-0003](docs/decisions/0003-1.0-stability-scope.md); release policy in [ADR-0004](docs/decisions/0004-phase-0-answers.md). All architectural decisions: [`docs/decisions/`](docs/decisions/).

## Installation

### Option 1: Download a pre-built binary (easiest)

Download the latest release for your platform from [GitHub Releases](https://github.com/parisxmas/OxiDB/releases):

| Platform | Download |
|----------|----------|
| macOS Apple Silicon (M1/M2/M3/M4) | [`oxidb-server-macos-arm64.tar.gz`](https://github.com/parisxmas/OxiDB/releases/latest/download/oxidb-server-macos-arm64.tar.gz) |
| macOS Intel | [`oxidb-server-macos-x86_64.tar.gz`](https://github.com/parisxmas/OxiDB/releases/latest/download/oxidb-server-macos-x86_64.tar.gz) |
| Linux x86_64 | [`oxidb-server-linux-x86_64.tar.gz`](https://github.com/parisxmas/OxiDB/releases/latest/download/oxidb-server-linux-x86_64.tar.gz) |
| Linux ARM64 | [`oxidb-server-linux-arm64.tar.gz`](https://github.com/parisxmas/OxiDB/releases/latest/download/oxidb-server-linux-arm64.tar.gz) |
| Windows x86_64 | [`oxidb-server-windows-x86_64.zip`](https://github.com/parisxmas/OxiDB/releases/latest/download/oxidb-server-windows-x86_64.zip) |

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
| `OXIDB_BLOB_SYNC` | `false` | fsync each blob `put` and `delete` before returning, so a successful write is durable on disk instead of waiting for the 1 Hz background flush. `put` fsyncs payload, meta, and the bucket dir; `delete` fsyncs the bucket dir after unlinking the meta. Directory fsyncs are group-committed across concurrent writers, so throughput stays high. Enable when a caller treats a successful write as a commit |
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
| `OXIDB_MQTT_PORT` | — | Enable MQTT v3.1.1 listener on this port |
| `OXIDB_S3_PORT` | — | Enable S3-compatible HTTP API on this port |
| `OXIDB_S3_ACCESS_KEY` | — | S3 access key for AWS SigV4 authentication |
| `OXIDB_S3_SECRET_KEY` | — | S3 secret key for AWS SigV4 authentication |
| `OXIDB_S3_CREDENTIALS` | — | Path to S3 credentials file |
| `OXIDB_S3_ENCRYPTION_KEY` | — | Hex-encoded 32-byte AES-256 key for S3 SSE |
| `OXIDB_S3_DEFAULT_ENCRYPTION` | `false` | Encrypt all S3 objects by default |
| `OXIDB_HTTP_PORT` | — | Enable REST HTTP API on this port |
| `OXIDB_UDP_PORT` | — | Enable UDP GELF/JSON log ingestion on this port |
| `OXIDB_UDP_COLLECTION` | `_udp_logs` | Collection name for UDP log ingestion |
| `OXIDB_GELF_PORT` | — | Enable GELF UDP ingestion with auto-indexing (e.g. `12201`) |
| `OXIDB_GELF_COLLECTION` | `_gelf_logs` | Collection name for GELF log ingestion |
| `OXIDB_ALERT_INTERVAL` | `15` | Alert evaluator check interval in seconds |
| `OXIDB_LOG_COMMANDS` | `false` | Log OxiMem/MQTT commands |
| `OXIDB_FTS_LANG` | `english` | Snowball stemmer language: `english`, `turkish`/`tr`, `german`, `french`, `spanish`, `italian`, `portuguese`, `russian`, `dutch`, `danish`, `finnish`, `hungarian`, `norwegian`, `romanian`, `greek`, `arabic`, `swedish`, `tamil` |
| `OXIDB_FTS_K1` | `1.2` | BM25 term-frequency saturation (>0). Matches Lucene/Elasticsearch default |
| `OXIDB_FTS_B` | `0.75` | BM25 length normalization (0–1). 0 = ignore document length, 1 = full normalization |
| `OXIDB_FTS_WORKERS` | `1` | Number of FTS extract+index worker threads (one per core for heavy PDF/DOCX ingestion) |
| `OXIDB_FTS_FLUSH_INTERVAL_MS` | `1000` | How often the FTS index file is fsynced; batches per-document writes to amortize disk I/O |
| `OXIDB_PITR` | `false` | Enable Point-In-Time Recovery: stamp WAL records with a global sequence number, rotate sealed WAL segments, and run the background archiver. Off = zero cost |
| `OXIDB_ARCHIVE_DIR` | `<data>/_archive` | Where the archiver deposits sealed WAL segments and the manifest |
| `OXIDB_ARCHIVE_INTERVAL` | `10` | Archiver poll cadence in seconds — how often sealed segments are copied to the archive |
| `OXIDB_WAL_SEGMENT_BYTES` | `16777216` | Live-WAL size at which a collection seals its current segment and rotates (PITR only) |
| `OXIDB_ARCHIVE_RETENTION_HOURS` | `0` | Prune archived segments older than this many hours. `0` = never prune. Age-based — keep a base backup at least as old as the window |

## Features

- **Document database** — JSON documents, no schema required, collections auto-created on insert
- **JSON-based queries** — `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$exists`, `$regex`, `$elemMatch`, `$all`, `$size`, `$not`, `$type`, `$mod`, `$and`, `$or`, `$nor`, `$expr`
- **12 update operators** — `$set`, `$unset`, `$inc`, `$mul`, `$min`, `$max`, `$rename`, `$currentDate`, `$push`, `$pull`, `$addToSet`, `$pop`
- **Aggregation pipeline** — 12 stages: `$match`, `$group`, `$sort`, `$skip`, `$limit`, `$project`, `$count`, `$unwind`, `$addFields`, `$lookup`, `$out`, `$dateHistogram`; index-accelerated `$group` for count, sum, min, max, avg; accumulators include `$sum`, `$avg`, `$min`, `$max`, `$count`, `$first`, `$last`, `$push`, `$addToSet`, `$percentile`
- **Indexes** — field, unique, composite, full-text, vector, and TTL indexes with automatic backfill; list and drop support
- **TTL indexes** — automatic document expiration on any datetime field; `create_ttl_index` with configurable `expireAfterSeconds`; index-accelerated eviction via background thread
- **Vector search** — k-nearest-neighbor similarity search with cosine, Euclidean, and dot product metrics; flat (exact) for small collections, HNSW (approximate) for large; optional GPU acceleration via wgpu compute shaders (Metal/Vulkan/DX12, `--features gpu`)
- **B-tree storage engine** — `scc::HashMap` concurrent document storage with interior mutability; reads never block reads, writes to different documents proceed in parallel; WAL for crash safety; persisted field indexes for instant startup
- **Zero-copy reads** — `find_one`, `update`, and `delete` use Arc-based document iteration, cloning only matching documents instead of every visited document
- **Transactions** — OCC (optimistic concurrency control) with begin/commit/rollback
- **REST HTTP API** — JSON-over-HTTP interface for all document operations (CRUD, aggregation, indexes, procedures); works with `curl`, Postman, or any HTTP client; CORS enabled
- **S3-compatible API** — full HTTP REST API with path-style requests, multipart upload, range reads, object tagging, copy, conditional requests, SSE-S3/SSE-C encryption; compatible with AWS CLI and boto3
- **OxiMem (Redis-compatible)** — in-memory key-value store with RESP wire protocol; 50+ commands (strings, hashes, lists, sets, sorted sets, pub/sub)
- **MQTT v3.1.1** — publish/subscribe messaging with cross-protocol bridging to OxiMem pub/sub channels
- **Hash sharding** — OxiPool proxy with CRC32 hash routing, scatter-gather queries, per-collection shard keys, and cross-shard transaction detection
- **Blob storage** — S3-style buckets with put/get/head/delete/list and CRC32 etags
- **Full-text search** — automatic text extraction from 10+ formats (HTML, XML, PDF, DOCX, XLSX, images via OCR), **BM25 ranking** (Lucene/Elasticsearch-compatible, tunable `k1`/`b`), Snowball stemming for **18 languages** (English, Turkish, German, French, Spanish, Italian, Portuguese, Russian, Dutch, Danish, Finnish, Hungarian, Norwegian, Romanian, Greek, Arabic, Swedish, Tamil), Turkish/English stop words, Unicode accent normalization, **`<mark>` highlighted snippets** (per-field for collections, on-demand re-extract for blobs), **async multi-worker extract** (`OXIDB_FTS_WORKERS`) for high-volume PDF/DOCX ingestion, and **batched index persist** (`OXIDB_FTS_FLUSH_INTERVAL_MS`) to amortize disk writes
- **Raft replication** — multi-node cluster via OpenRaft with automatic leader election, HAProxy-compatible health checks, and sub-second failover
- **Change streams** — real-time `watch`/`unwatch` with collection filtering, backpressure handling, and token-based resume
- **JSONB binary storage** — compact binary format for faster serialization; backward-compatible with existing JSON data files
- **Crash-safe** — write-ahead log with CRC32 checksums, verified by SIGKILL recovery tests
- **Point-In-Time Recovery** — opt-in (`OXIDB_PITR`): every WAL record is stamped with a global sequence number + wall-clock, sealed WAL segments are archived crash-safely with a self-healing manifest, and `restore_to_point` rebuilds the database to any `Gsn` / `Timestamp` / `Latest` target on top of a base backup — with transactionally-consistent cuts (a transaction straddling the cut is excluded whole, never half-applied). v1 limits: blobs restore to the base-backup point only, the FTS index is rebuilt, and index DDL between base and target is not replayed
- **Encryption at rest** — AES-256-GCM on storage records and B-tree persistence files; per-record nonces
- **Security** — TLS transport, SCRAM-SHA-256 authentication, role-based access control (Admin/ReadWrite/Read), audit logging
- **OxiScript** — lightweight stored procedure language; `proc transfer(from, to, amount) { ... }` compiles to JSON steps; supports if/else, variable binding, field access, all DB operations, and procedure-calling-procedure
- **Stored procedures** — JSON-defined or OxiScript multi-step procedures with control flow (`if`/`else`, `abort`, `return`), variable binding, and automatic transaction wrapping
- **Cron scheduler** — built-in background scheduler that runs stored procedures on cron expressions (`"0 3 * * *"`) or fixed intervals (`"30s"`, `"5m"`, `"2h"`), with run history tracking
- **GELF logging** — centralized UDP logging to Graylog/Loki via `OXIDB_GELF_ADDR`
- **GELF ingestion** — receive GELF v1.1 structured logs via UDP, auto-index every field (Elasticsearch-style dynamic mapping), chunked message reassembly, simd-json parsing, crossbeam lock-free channels, batch insert; 138K msg/sec sustained throughput
- **Retention policies** — collection-level `set_retention` with automatic TTL-based cleanup; `{"cmd": "set_retention", "collection": "_gelf_logs", "days": 30}`
- **Alerting** — background alert evaluator with count/aggregation thresholds, webhook actions, cooldown, and `_alert_history` logging; `{"cmd": "create_alert", "name": "high_errors", "collection": "_gelf_logs", "condition": {...}, "actions": [...]}`
- **Compaction** — reclaim space from deleted documents with atomic file swap
- **Concurrent access** — `scc::HashMap` storage + RwLock-per-index interior mutability; lock-free document reads, fine-grained write concurrency; thread-per-connection model with unbounded concurrency
- **Query optimizer** — selectivity-based index selection; picks the most selective condition in AND queries using index cardinality estimates
- **JSONB partial extraction** — aggregation extracts only needed fields from binary docs, skipping nested arrays; 1M × 3KB docs aggregated in 300-700ms
- **UDP log ingestion** — high-throughput fire-and-forget GELF/JSON receiver; SO_REUSEPORT multi-thread listeners
- **VS Code extension** — collection browser, MongoDB-style query editor, OxiScript syntax highlighting
- **CLI tool** — interactive shell with JSON-based syntax, embedded and client modes
- **Multi-language clients** — Python, Go, Julia, .NET, Swift/iOS — all zero or minimal dependencies

## Query Operators

### Comparison

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

### Logical

| Operator   | Example                                  | Description                |
|------------|------------------------------------------|----------------------------|
| `$and`     | `{"$and": [{"a": 1}, {"b": 2}]}`        | All conditions must match  |
| `$or`      | `{"$or": [{"a": 1}, {"b": 2}]}`         | Any condition matches      |
| `$nor`     | `{"$nor": [{"status": "deleted"}, {"banned": true}]}` | None of the conditions match |
| `$not`     | `{"age": {"$not": {"$gt": 30}}}`         | Negate an operator expression |

### Element

| Operator   | Example                                  | Description                |
|------------|------------------------------------------|----------------------------|
| `$exists`  | `{"email": {"$exists": true}}`           | Field exists / does not    |
| `$type`    | `{"age": {"$type": "number"}}`           | Field is a specific JSON type (`string`, `number`, `bool`, `array`, `object`, `null`, `int`) |

### Evaluation

| Operator   | Example                                  | Description                |
|------------|------------------------------------------|----------------------------|
| `$regex`   | `{"name": {"$regex": "^A", "$options": "i"}}` | Regular expression match   |
| `$mod`     | `{"qty": {"$mod": [4, 0]}}`             | Modulo: `field % divisor == remainder` |
| `$expr`    | `{"$expr": {"$gt": ["$sold", "$stock"]}}` | Cross-field comparison using `$field` references |

### Array

| Operator     | Example                                          | Description                |
|--------------|--------------------------------------------------|----------------------------|
| `$elemMatch` | `{"items": {"$elemMatch": {"price": {"$gt": 100}}}}` | At least one array element matches all conditions |
| `$all`       | `{"tags": {"$all": ["rust", "fast"]}}`           | Array contains all specified values |
| `$size`      | `{"tags": {"$size": 3}}`                        | Array has exact length     |

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
| `create_ttl_index`       | `collection`, `field`, `expireAfterSeconds`        |
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
| `set_retention`          | `collection`, `days`                               |
| `get_retention`          | `collection`                                       |
| `delete_retention`       | `collection`                                       |
| `list_retentions`        | —                                                  |
| `create_alert`           | `name`, `collection`, `condition`, `actions`, `cooldown_seconds?` |
| `delete_alert`           | `name`                                              |
| `list_alerts`            | —                                                  |
| `get_alert`              | `name`                                              |
| `test_alert`             | `name`                                              |
| `list_alert_history`     | —                                                  |
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

## REST HTTP API

JSON-over-HTTP interface for all document operations. No client library needed — works with `curl`, Postman, browser JavaScript, or any HTTP client.

```bash
# Enable REST API on port 8080
OXIDB_HTTP_PORT=8080 ./oxidb-server

# Insert
curl -X POST http://localhost:8080/api/users/documents \
  -H "Content-Type: application/json" \
  -d '{"doc": {"name": "Alice", "age": 30}}'

# Find
curl 'http://localhost:8080/api/users/documents?q={"age":{"$gt":21}}&sort={"age":-1}&limit=10'
```

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/ping` | Health check |
| `GET` | `/api/collections` | List collections |
| `POST` | `/api/collections` | Create collection (`{"name": "..."}`) |
| `DELETE` | `/api/collections/{name}` | Drop collection |
| `POST` | `/api/{collection}/documents` | Insert (`{"doc": {...}}` or `{"docs": [...]}`) |
| `GET` | `/api/{collection}/documents` | Find (`?q={}&sort={}&skip=N&limit=N`) |
| `PATCH` | `/api/{collection}/documents` | Update (`{"query": {}, "update": {}}`) |
| `DELETE` | `/api/{collection}/documents` | Delete (`{"query": {}}`) |
| `GET` | `/api/{collection}/count` | Count (`?q={}`) |
| `POST` | `/api/{collection}/aggregate` | Aggregation (`{"pipeline": [...]}`) |
| `POST` | `/api/{collection}/indexes` | Create index (`{"field": "...", "type": "field\|unique\|ttl"}`) |
| `GET` | `/api/{collection}/indexes` | List indexes |
| `DELETE` | `/api/{collection}/indexes/{name}` | Drop index |
| `POST` | `/api/procedures` | Create procedure (`{"script": "proc ..."}`) |
| `POST` | `/api/procedures/{name}/call` | Call procedure |
| `GET` | `/api/procedures` | List procedures |
| `DELETE` | `/api/procedures/{name}` | Delete procedure |

CORS enabled. JSON request and response bodies. HTTP keep-alive supported.

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

## Architecture

### Storage Engine

`scc::HashMap<doc_id, JSONB bytes>` with interior mutability. Documents stored in a lock-free concurrent hash map. Indexes use `PagedFieldIndex` (sorted `Vec` with binary search) wrapped in per-index `RwLock`. Persistence via WAL + periodic `.btree` file serialization. Persisted field indexes (`.fidx`) for instant startup.

### Concurrency Model

Interior mutability — no collection-level lock:
- **Storage** — `scc::HashMap`: lock-free reads, parallel writes to different documents
- **Indexes** — `RwLock` per index group: queries never block other queries
- **Document cache** — 16-shard LRU with per-shard `Mutex`
- **ID generation** — `AtomicU64`

### Write-Ahead Log

Every insert/update/delete is appended to a per-collection `.wal` file **before** the in-memory B-tree is mutated. Each record carries a CRC32 checksum and a transaction id, so recovery can skip aborted transactions and ignore a partially-written tail.

By default each commit fsyncs the WAL (strict ACID-D — when a write returns success the bytes are on disk; matches `synchronous_commit=on` / MongoDB `j:true`). `OXIDB_LAZY_SYNC=true` instead batches fsyncs into the background snapshot thread, trading up to `OXIDB_SYNC_INTERVAL_MS` of data loss on crash for higher write throughput. Default cadence is **1000 ms in strict mode, 10 ms in lazy mode**.

The same background thread also writes periodic `.btree` snapshots but deliberately does **not** truncate the WAL afterward: a concurrent insert can land between the snapshot and the truncation and would otherwise be lost. The WAL therefore grows between snapshots and is replayed idempotently on top of the last `.btree` image at startup. Truncation happens only in the final checkpoint on graceful shutdown, when no writers are active.

### Performance Optimizations

- **`scc::HashMap`** — write-optimized concurrent hash map with finer-grained bucket locks than DashMap
- **Selectivity-based query optimizer** — AND queries pick the most selective index condition first using `count_eq`/`count_range` cardinality estimates
- **JSONB partial extraction** — aggregation uses `feed_raw()` to extract only group key + accumulator fields from binary docs, skipping nested arrays (17-50x faster on large nested documents)
- **Parallel cache-based scan** — unindexed queries use rayon parallel filter with partial-JSONB pre-filter (skip docs that obviously fail before paying full decode)
- **Index-level sort+limit** — sort queries with limit use index iteration with cross-index membership checks
- **Deferred index compaction** — bulk delete skips per-entry Vec shifts, compacts once at end (34x faster DeleteMany)
- **Dirty-flag persistence** — background sync only writes to disk when data has changed
- **Parallel JSONB encoding** — `insert_many` encodes documents in parallel using rayon
- **Index persistence** — field/composite indexes saved to `.fidx`/`.cidx` files; loaded on startup if doc_count matches

### IndexValue Type Ordering

```
Null < Boolean < Integer/Float < DateTime < String
```

Date strings (ISO 8601, RFC 3339, `YYYY-MM-DD`) are automatically stored as epoch milliseconds for fast integer comparison.

## Book (Turkish)

A full-length Turkish book, **_OxiDB Doküman Veritabanı_** — a ground-up,
code-free explanation of document databases and then how OxiDB works, step by
step — lives in [`books/belge-veritabanlari/`](books/belge-veritabanlari/).
~116 pages, cover + 27 hand-drawn diagrams.

📖 **[Download the PDF](https://github.com/parisxmas/OxiDB/raw/master/books/belge-veritabanlari/belge-veritabanlari.pdf)**
(GitHub does not preview PDFs in the file list; use this link or open the file
and click **Download**.)

## License

As of **v0.40.0**, OxiDB is **source-available** — see
[LICENSE](LICENSE). Read it, modify it, and run it in production for
your own applications and business, free, at any scale.

Two things need a commercial license: **offering OxiDB as a service** to
third parties, and **distributing it** — on its own or embedded in your
product. See [COMMERCIAL-LICENSE.md](COMMERCIAL-LICENSE.md) for terms
and contact.

The thin TCP client libraries are MIT-licensed, redistribution included
— shipping one inside your application needs no license.

Each prior release keeps the license it was published with: early
releases `MIT OR Apache-2.0`, through v0.32.x `AGPL-3.0-only`, and
v0.33.0–v0.39.x proprietary.

### Contribution

By submitting a contribution you agree it may be distributed under the
source-available and commercial licenses above — see
[COMMERCIAL-LICENSE.md](COMMERCIAL-LICENSE.md#6-contributions).
