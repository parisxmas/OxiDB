# Changelog

## v0.25.9

### Full-text search — BM25 ranking + multi-language stemmers + highlighting

- **BM25 ranking** replaces TF-IDF (`src/fts.rs`)
  - Length-normalized: long documents no longer outrank short ones on identical TF
  - Saturating: 10× term frequency does not yield 10× score (k1 saturation)
  - Lucene/Elasticsearch defaults `k1=1.2`, `b=0.75`; tunable via `OXIDB_FTS_K1` / `OXIDB_FTS_B`
  - Lazy migration: `_fts/index.json` files written before BM25 are auto-backfilled with `total_term_count` on first open — no rebuild required
  - Both `FtsIndex` (blob FTS) and `CollectionTextIndex` (per-collection FTS) on the new path
- **Snowball stemmers, 18 languages** via `OXIDB_FTS_LANG`
  - English (default), Turkish/`tr`, German, French, Spanish, Italian, Portuguese, Russian, Dutch, Danish, Finnish, Hungarian, Norwegian, Romanian, Greek, Arabic, Swedish, Tamil
  - Cached per-process via `OnceLock` — no overhead in the hot path
  - Verified in tests: `kitap` ↔ `kitaplar` ↔ `kitaplarda` collapse to a common stem under `OXIDB_FTS_LANG=tr`
- **Highlighted snippets** — `fts::highlight(text, query, snippet_chars, max_snippets)` returns `<mark>matched</mark>` snippets with offsets and matched-term counts
  - Same tokenization pipeline as the index, so a `running` query highlights `runs` (and `kitaplar` highlights `kitabı` under Turkish)
  - Custom tags via `highlight_with_tags(...)`
  - Multi-byte (UTF-8) safe: char-boundary snapping prevents panics on Turkish/CJK text
  - Wired through to:
    - `Collection::text_search_highlighted` / `BTreeCollection::text_search_highlighted`
    - `OxiDb::text_search_highlighted`, `OxiDb::search_highlighted`
    - Server `text_search` and `search` ops via optional `"highlight": true` or `"highlight": {"snippet_chars": N, "max_snippets": M}`
- **Multi-worker FTS pipeline** — `OXIDB_FTS_WORKERS` (default 1)
  - New `FtsDispatcher` round-robins jobs across N worker channels — CPU-bound `extract_text` (PDF/DOCX/OCR) parallelizes across cores
  - Round-robin with `try_send` fallback to blocking `send` so one slow worker doesn't backpressure the whole pool
- **Batched FTS persist** — `OXIDB_FTS_FLUSH_INTERVAL_MS` (default 1000 ms)
  - `FtsIndex` gains `set_batched(true)` / `flush()` — per-document mutations only mark the index dirty
  - Background flusher thread persists at most once per interval
  - Eliminates the previous N² disk write amplification on bulk ingestion (every doc previously rewrote the entire `_fts/index.json`)
  - Existing test path keeps `batched=false` so synchronous-persist guarantees still hold
- **Startup config dump** — `oxidb-server` now logs `FTS: lang=... k1=... b=... (BM25)` after the alert-evaluator line
- 38 new FTS unit tests + 3 new BTreeCollection integration tests; total lib suite 678 → 714

### Aggregation pipeline

- **`$dateHistogram`** stage (`src/pipeline.rs`)
  - Buckets a date field by `interval` and runs accumulators per bucket
  - Intervals: `Ns`/`Nm`/`Nh`/`Nd`/`Nw` (fixed-width) or `1M`/`1y` (calendar) plus long forms `minute`/`hour`/`day`/`week`/`month`/`year`
  - `min_doc_count: 0` fills empty buckets between observed min and max with `count: 0` — emitted as a synthetic `Stage::DateBucketFill` chained after the underlying `$group`
  - Accepts ISO 8601 / RFC 3339 strings or numeric epoch ms; output `_id` is always an ISO string in UTC
  - Implementation: new `Expression::DateBucket(expr, DateInterval)` reuses the standard `$group` execution path; index-accelerated group remains compatible
- **`$percentile`** accumulator
  - Exact percentile with linear interpolation between nearest ranks
  - Syntax: `{ "$percentile": { "input": "$score", "p": [0.5, 0.95, 0.99] } }` → returns array of values matching `p` order
  - Validates `p` is non-empty and each entry is in `[0, 1]`

### Server / handlers

- `text_search` op gains optional `"highlight"` field — collection FTS returns `<mark>` snippets per indexed string field plus `_score`
- `search` op gains optional `"highlight"` field — blob FTS re-extracts each hit's blob and emits snippet array (cost is paid only when requested)
- `oxidb-server/Cargo.toml` bumped: 0.25.3 → 0.25.9

### Tests / demo

- New `ftstests/` directory — end-to-end FTS smoke + live demo
  - `01_generate.py` — fetches three Project Gutenberg books (Alice / Pride and Prejudice / Sherlock Holmes) and splits each into ~33 minimal `.docx` chunks (uses `zipfile` only, no `python-docx` dependency)
  - `02_upload.py` — uploads via the in-tree Python TCP client
  - `03_search.py` — 15 ranking / stop-word / highlight assertions; 15/15 pass against a fresh server
  - `web.py` — stdlib HTTP proxy: `/api/search`, `/api/blob/<key>`, `/api/text/<key>`, `/api/stats`, `/api/upload/<filename>`, `/healthz`
  - `index.html` — single-page UI: search box, query chips, drag-and-drop upload, document viewer (PDF iframe / mammoth.js DOCX / SheetJS XLSX / sandboxed HTML / `<pre>` for text), find-in-doc toolbar with prev/next match navigation, auto-highlight of the search query inside the viewer, download button, live document count
  - `run.sh` — one-shot: build server + spawn + generate + upload + run search suite
  - `serve.sh` — long-running: persistent data dir + auto-seed + web client (default `http://127.0.0.1:8765/`)
  - `deploy/` — image-based deploy (`docker buildx --platform=linux/amd64` → `docker save` → scp tar → `docker load`); ships only image binaries to the remote, no source tree

## v0.25.3

### `oxipool/src/scatter.rs` — partial-shard errors no longer silent

- New `first_partial_error` helper — checks every shard response before merging.
- `merge_counts`, `merge_doc_arrays`, `merge_modified` now fail fast with `ok:false` and the failing shard's error message instead of silently summing/concatenating only the responding shards.
- Surfaced by the 100K load test: a stale-pool conn to a freshly-restarted follower returned an error → router silently dropped that shard → `count` returned ~2/3 of the actual rows. With the fix the client sees a real error and can retry, instead of getting an under-count.
- `oxipool` crate version bumped: 0.25.0 → 0.25.3.

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
