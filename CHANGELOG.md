# Changelog

## v0.25.3

### Cluster mode — Raft persistence: O(1) per mutation

- Rewrite `oxidb-server/src/raft/log_store.rs` persistence layer to scale
  - Split single `raft_state.json` into `raft_meta.json` (small, vote/committed/sm_data — rewritten on metadata changes) and `raft_log.jsonl` (append-only, one Entry per line)
  - `append_to_log` is now O(1) per entry instead of O(n) — single line append
  - `delete_conflict_logs_since` / `purge_logs_upto` rewrite the log file (rare events)
  - On startup, `raft_meta.json` + `raft_log.jsonl` are loaded line-by-line into the in-memory BTreeMap
  - Transparent migration from the v0.25.2 single-file format
- Unblocked 1M-record load tests under failover: 22.4 s end-to-end, 44,701 rec/s avg, 0 records lost (previously stalled at ~52% complete due to 14 MB-per-mutation rewrites)

## v0.25.2

### Cluster mode — Raft state persistence

- Add disk persistence for Raft storage in `oxidb-server/src/raft/log_store.rs`
  - `OxiDbStore` was previously in-memory only; nodes that restarted came back as `Learner term=0` and lost cluster membership, breaking failover scenarios
  - New `OxiDbStore::open(db, &data_dir)` constructor loads existing Raft state on startup
  - Atomic write-through (write-then-rename) on every mutation: `save_vote`, `save_committed`, `append_to_log`, `delete_conflict_logs_since`, `purge_logs_upto`, `apply_to_state_machine`, `install_snapshot`
  - Wired up from `oxidb-server/src/main.rs` cluster-mode startup
- `OxiDbStore::new(db)` retained as in-memory variant (used by tests)

### Tests

- Add `ShardReplicaRealWorldTest/` — full sharded + replicated cluster harness
  - 14-service `docker-compose.yml`: 9 oxidb-server nodes (3 Raft groups), 3 per-shard oxipool master/replica routers, 1 top-level shard-routing oxipool, 1 Go API tier, 1 cluster-init bootstrapper, 1 opt-in smoke harness
  - `cluster-init/` — one-shot Go tool that runs `raft_init` + `raft_add_learner` + `raft_change_membership` on each shard's leader candidate
  - `api/` — Go HTTP API with endpoints for browse, cart, checkout (TX-pinned), order history, scatter-gather queries, raft metrics
  - `smoke/` — 5-assertion Go smoke test covering health, sharding, replication, TX pinning, scatter-gather
  - `tests/test_cluster.py` — 8 Python integration tests (CRUD + sharding + aggregation)
  - `tests/test_failover.py` — 5 Python failover scenarios (network partition, follower down, recovery catch-up, two followers down, leader down)
  - `tests/test_load_failover.py` — load test with mid-stream failover (parameterized by `TOTAL`/`BATCH`/`FAILOVER_AT`)
  - Validated against 10K, 100K, and 1M record loads

## v0.25.1

### Query Engine

- Add `$not` operator — negate any field condition; missing fields return true (MongoDB-compatible)
- Add `$nor` top-level operator — match documents where none of the conditions are true
- Add `$all` operator — array must contain all specified values
- Add `$size` operator — match arrays with exact length
- Add `$type` operator — match by JSON type (string, number, bool, array, object, null, int)
- Add `$mod` operator — modulo arithmetic on numeric fields (`[divisor, remainder]`)
- Add `$expr` top-level operator — cross-field comparisons (`{"$expr": {"$gt": ["$sold", "$stock"]}}`)
- Add `$elemMatch` operator — match array elements against sub-queries with AND semantics
- Refactor `matches_doc` and `matches_value` into shared `eval_field_op` helper

### Go Client

- Add stored procedure methods: `CreateProcedure`, `CreateProcedureFromScript`, `CallProcedure`, `ListProcedures`, `GetProcedure`, `DeleteProcedure`, `CompileOxiScript`
- Add `CreateTTLIndex` for automatic document expiration
- Add retention policy methods: `SetRetention`, `GetRetention`, `DeleteRetention`, `ListRetentions`
- Add alerting methods: `CreateAlert`, `GetAlert`, `DeleteAlert`, `ListAlerts`, `TestAlert`, `ListAlertHistory`
- Add `ExtractText` for blob text extraction (PDF, DOCX, HTML)
- Add `Backup` and `Restore` for full database backup/restore
- Add `SetDialect` for SQL dialect switching (mysql, postgresql, mssql, generic)

### Tests

- 107 query engine tests (53 new), including 15 real-world scenario tests covering fraud detection, loan eligibility, matchmaking, property search, supply chain, content management, and more

## v0.25.0

- Bump all workspace crates to v0.25.0
- Add Python client retention, alerting, and TTL methods
- Add `oxidb-tail` TUI log viewer with table columns, stats toggle, and keyboard shortcuts

## v0.24.0

- Add WebAssembly support — OxiDB runs in the browser
- Add JavaScript/TypeScript SDK (`oxidb` npm package) — zero dependencies, REST + WebSocket
- Add JWT authentication for REST and WebSocket APIs
- Add WebSocket server with real-time subscriptions
- Add per-document security rules (Firebase-style access control)
- Add TTL indexes with automatic document expiration
- Add REST HTTP API with CORS and 64-thread pool

## v0.23.1

- Add TTL indexes, REST HTTP API, OxiScript tests, Julia client updates

## v0.23.0

- Add stored procedures (OxiScript and JavaScript)
- Add cron scheduling for procedures
- Add multi-database support (create, drop, use, list)
- Add SQL dialect support (MySQL, PostgreSQL, MSSQL, Generic)

## v0.22.2

- Add GELF ingestion, retention policies, and alerting system
- Add GELF chunked reassembly and FTS stemming with accent normalization
- Add GPU-accelerated vector search via wgpu compute shaders

## v0.22.0

- Add OxiMem in-memory key-value layer (RESP protocol, redis-cli compatible)
- Add MQTT v3.1.1 protocol with cross-protocol pub/sub
- Add sorted sets (ZADD, ZRANGE, etc.)

## v0.20.4

- Add vector similarity search (cosine, euclidean, dot product)
- Add pipeline command batching

## v0.20.0

- Add S3-compatible blob storage with full-text search
- Add backup and restore commands

## v0.19.0

- Add SCRAM-SHA-256 authentication
- Add RBAC (Admin, ReadWrite, Read roles)
- Add TLS support
- Add audit logging

## v0.18.0

- Initial public release
- Core document engine with append-only storage, WAL, field indexes
- SQL and JSON query support
- ACID transactions with OCC
- Aggregation pipeline
- Full-text search
- Python, Go, .NET, PHP, Swift, Julia client libraries
