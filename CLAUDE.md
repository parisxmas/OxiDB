# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

OxiDB is a fast, embeddable document database engine written in Rust. It supports both SQL and JSON-based queries. It can run as an embedded library, a standalone TCP server, or be accessed via client libraries (Python, Go, Julia, .NET, Swift/iOS via C FFI).

## Build Commands

```bash
cargo build --release                    # Build core library
cargo build --release -p oxidb-server    # Build TCP server
cargo build --release -p oxidb-client-ffi # Build C FFI shared library (.dylib/.so/.dll)
cargo build --workspace --features ocr   # Build with OCR support
```

## Testing

```bash
cargo test                               # All tests (workspace)
cargo test -p oxidb                      # Core library tests only
cargo test -p oxidb-server               # Server tests only
cargo test <test_name>                   # Single test by name
cargo test -- --nocapture                # With stdout output
```

Unit tests are inline (`#[cfg(test)]` modules in source files). Integration tests for the server are in `oxidb-server/tests/` (ACID and security tests).

## Linting / Formatting

```bash
cargo fmt
cargo clippy
```

## Workspace Structure

Cargo workspace with three crates:
- **Root (`oxidb`)** — core database library
- **`oxidb-server/`** — TCP server with SCRAM-SHA-256 auth, RBAC, TLS, audit logging
- **`oxidb-client-ffi/`** — C-compatible FFI (`cdylib`) for language bindings

Client libraries: `python/`, `go/`, `julia/`, `dotnet/`, `swift/`

## Architecture

### Core Engine (`src/engine.rs`)
`OxiDb` owns a `RwLock<HashMap<name, Arc<RwLock<Collection>>>>`. Per-collection locking enables concurrent reads. Collections are auto-created on first insert.

### Multiple Databases (`src/database_manager.rs`, ADR-0012)
`DatabaseManager` hosts many isolated `OxiDb` instances, one per subdirectory of `OXIDB_DATA`: default database `oxidb` (alias `postgres`), server-global `_auth`/`_audit` at the top level, flat legacy layouts auto-migrated into `oxidb/` on open (never overwriting). Wire: optional `db` field per request, `use_db` sets the session default, plus `create_database`/`drop_database`/`list_databases` — and the SQL-text equivalents `CREATE/DROP DATABASE`, `SHOW DATABASES`, `USE` (both surfaces parse into one `DbIntent` in `oxidb-server/src/db_admin.rs`; create/drop are Admin-only). RBAC supports per-database roles (`db_roles`, `grant_db_role`/`revoke_db_role`). The SQL engine is per-database too: default db at `OXIDB_SQL_DATA` (`${OXIDB_DATA}/sql`), others at `${OXIDB_DATA}/<name>/sql` (`sql_bridge` registry). Cluster mode replicates database DDL and scopes writes via `OxiDbRequest::Scoped`; TTL/alert threads run per database; REST takes `?db=<name>`, WS a per-message `"db"` field; transactions are bound to the database they began on. OxiMem keyspace and S3 buckets are intentionally global.

### Collection (`src/collection.rs`)
Each collection owns:
- **Storage** (`storage.rs`) — append-only file with `[status:u8][length:u32 LE][payload]` records; soft-delete flips status byte in-place
- **WAL** (`wal.rs`) — write-ahead log with CRC32 checksums; entries tagged with transaction IDs; 3-fsync protocol (WAL → data → checkpoint)
- **In-memory document cache** — `HashMap<DocumentId, Arc<Value>>` (JSON deserialized once, then refcounted)
- **Field indexes** — `BTreeMap<IndexValue, BTreeSet<DocumentId>>` per indexed field
- **Composite indexes** — multi-field B-tree for prefix scans
- **Version map** — per-document version counters for OCC

### Query Engine (`src/query.rs`, `src/pipeline.rs`)
Query AST with field conditions and logical operators ($and, $or, $nor). Operators: $eq, $ne, $gt/$gte/$lt/$lte, $in, $nin, $exists, $regex, $elemMatch, $not, $all, $size, $type, $mod. Top-level $expr for cross-field comparisons. Key optimizations:
- Index-backed sort: BTreeMap iteration is O(limit) instead of O(n log n)
- Index-only count: returns set size without touching documents
- Early termination: `update_one`/`delete_one` stop after first match

Aggregation pipeline: $match, $group, $sort, $skip, $limit, $project, $count, $unwind, $addFields, $lookup, $facet, $setWindowFields (window functions — document `{documents: [lo,hi]}` AND value/time `{range: [lo,hi], unit?}` frames), $dateHistogram (time-bucketed group + empty-bucket fill), $ohlcv (tick→candle OHLCV with symbol partitioning and LOCF gap fill), $densify (gap-generating docs on a numeric/date axis; fixed units only), $fill (locf / linear / constant null-filling per partition).

### Updates (`src/update.rs`)
Field operators ($set, $unset, $inc, $mul, $min, $max, $rename, $currentDate) and array operators ($push, $pull, $addToSet, $pop). Supports dot-notation for nested fields.

### Indexes (`src/index.rs`, `src/value.rs`)
`IndexValue` enforces cross-type ordering: Null < Bool < Num < DateTime < String. Dates are auto-detected from ISO 8601/RFC 3339/YYYY-MM-DD strings and stored as epoch ms.

### Transactions (`src/transaction.rs`, `src/tx_log.rs`)
OCC with 3-phase commit: prepare → validate versions → commit. Writes are buffered until commit. Deadlock-free via sorted collection locking (BTreeSet). Recovery uses transaction log + WAL replay on startup.

Isolation: backward-validating OCC over item read-sets — committed transactions are serializable w.r.t. the items they read/wrote; phantoms and torn reads for non-tx observers are admitted. The exact guarantee, anomaly scorecard, and application rules live in `docs/isolation.md`, pinned by `tests/isolation_characterization.rs`.

### Full-Text Search (`src/fts.rs`)
Background worker thread receives indexing jobs via `sync_channel(256)`. Supports HTML, XML, JSON, PDF, DOCX, XLSX, images (OCR with `ocr` feature). TF-IDF ranking. Persisted as `_fts/index.json`.

### Blob Storage (`src/blob.rs`)
S3-style bucket interface. Objects stored as `_blobs/<bucket>/<id>.data` + `<id>.meta`. CRC32 etags.

### Encryption (`src/crypto.rs`)
Transparent AES-GCM encryption at the storage layer. Optional—enabled by passing an encryption key to the engine.

### Point-In-Time Recovery (`src/pitr.rs`, `src/archive.rs`)
Opt-in via `OXIDB_PITR`. `ArchiveSequencer` (`pitr.rs`) hands every durable WAL write a global, monotonic, wall-clock-stamped GSN (persisted in leases via `_gsn`); WAL records carry it in the **v2** record format (`wal.rs`). The WAL rotates: `Wal::seal()` atomically renames the live `.wal` to a numbered sealed segment under the WAL lock. A background archiver (`archive.rs`, spawned by the engine) copies sealed segments into `_archive/segments/*.seg` (verbatim bytes + trailer) with a self-healing `manifest.json`. `backup()` embeds a `base.meta` GSN watermark. `OxiDb::restore_to_point` extracts a base backup then `archive::replay_into` advances it to a `Gsn`/`Timestamp`/`Latest` target with transactionally-consistent cuts. Off = zero cost.

### Server Protocol (`oxidb-server/`)
Length-prefixed JSON over TCP (max 16 MiB). Auth via SCRAM-SHA-256. RBAC roles: Admin, ReadWrite, Read. Configurable via env vars:
- `OXIDB_ADDR` (default `127.0.0.1:4444`)
- `OXIDB_DATA` (default `./oxidb_data`)
- `OXIDB_POOL_SIZE` (default 4 worker threads)
- `OXIDB_IDLE_TIMEOUT` (default 30s, 0 = never)
- `OXIDB_AUDIT` (default off; set to `true`/`1` to enable audit log)
- `OXIDB_AUDIT_MAX_BYTES` (optional; rotates audit log when live file reaches this many bytes)
- `OXIDB_AUDIT_MAX_AGE_SECS` (optional; rotates after this many elapsed seconds since file became active)
- `OXIDB_AUDIT_CALENDAR` (optional; `hourly` / `daily` / `none` — UTC calendar boundary)
- `OXIDB_AUDIT_COMPRESS` (optional; `true`/`1`/`yes`/`on` to gzip rotated audit files; default off)
- `OXIDB_SLOW_QUERY_MS` (optional; record wire commands slower than this many ms into `_profile` with a TTL index; default off)
- `OXIDB_PROFILE_TTL_SECS` (optional; retention of `_profile` records, default 86400)

Diagnostics: `{"cmd": "explain", "inner": {...find/count/aggregate...}}` returns the query plan (strategy, index used, examined/returned, post-filter operators) plus real run timing. Prometheus exposition at `GET /metrics` on the REST listener.

### Second engine — SQL (`oxidb-sql/`, ADR-0010)
A standalone relational SQL engine (its own crate) can be mounted alongside the document engine in the same server. It owns **entirely separate files** and shares no state — a collection name and a SQL table name never collide. Off by default.
- `OXIDB_SQL` (set to `1`/`true`/`yes`/`on` to enable; default off — zero cost when unused)
- `OXIDB_SQL_DATA` (SQL engine data dir; default `${OXIDB_DATA}/sql`)
- `OXIDB_SQL_DISK_FIRST` (rows served from the mmap'd last-checkpoint `.rdat` snapshot, only post-checkpoint changes in RAM; default off = all rows resident)
- `OXIDB_SQL_CHECKPOINT_BYTES` (auto-checkpoint the SQL WAL past this size; default 64 MiB, `0` = manual only)

Wire routing (`handler.rs`): a request with `engine: "sql"` — or the reserved `sql` command — is served by the SQL engine; a missing/`"doc"` engine keeps the document path byte-for-byte (full backward compatibility). Request shape: `{ "engine": "sql", "cmd": "sql", "sql": "SELECT ...", "params": [ ... ] }` (`params` binds `?`/`$N`). RBAC gates `sql` at the `ReadWrite` role. SQL supports DDL, DML, single-table + INNER/LEFT/RIGHT/FULL-join SELECT with GROUP BY/HAVING aggregates, secondary indexes, parameterized queries, and per-engine transactions (`BEGIN`/`COMMIT`/`ROLLBACK`). Node-local in v1 (not Raft-replicated). Stored procedures come in two languages (ADR-0014): SQL-text bodies (`CREATE PROCEDURE ... AS BEGIN dml; END` — zero-toolchain, re-parsed per CALL) and compiled **Cobra** bytecode (`CREATE PROCEDURE ... LANGUAGE COBRA AS '<base64 .cobrac>'`) executed by the `oxidb-cobra/` VM crate: the file defines `def run(db, ...params)`, `db.query`/`db.execute` join the CALL's transaction, print → notices, 100M-instruction fuel cap, determinism validated at CREATE (async/import/IO rejected) so CALL replicates safely.

SQL analytics surface additions: `DISTINCT ON` (argmax), `mode() WITHIN GROUP` (ordered-set aggregate), non-recursive `WITH`/CTE (parse-time desugared to derived tables), `LEAST`/`GREATEST`, set operations `UNION`/`EXCEPT`/`INTERSECT` (each with `ALL` bag semantics; standard precedence — INTERSECT binds tighter), FROM-less `SELECT` (`SELECT 1` — one implicit zero-column row; WHERE/aggregates/set-op arms all work), aggregate `DISTINCT` (`COUNT/SUM/AVG(DISTINCT x)`), `CROSS JOIN` (desugars to INNER ON TRUE), and **LATERAL joins** (`[LEFT] JOIN LATERAL (SELECT ...) x ON ...` — left-referencing derived tables re-executed per left row via the correlated-subquery machinery; RIGHT/FULL LATERAL rejected). Date/time: `NOW()`/`CURRENT_TIMESTAMP`, `EXTRACT(part FROM ts)` + `date_part('part', ts)`, `date_trunc('part', ts)` (UTC calendar math, ISO weeks, PG DOW numbering), `INTERVAL` literals folded to ms integers (fixed units only — month/year rejected), timestamp ± ms(/double) arithmetic, `ts - ts` → ms. Scalars: `FLOOR`/`CEILING` (exact for DECIMAL), `POWER`, `SQRT`, `%`/`MOD`, `POSITION`/`STRPOS`, `LPAD`/`RPAD`. The EF Core provider translates `DateTime` members/`AddX` methods, `Math.*`, `IndexOf`/`PadLeft/Right`, and renders EF's CROSS/OUTER APPLY as `[LEFT] JOIN LATERAL` (`OxiDbTranslators.cs`, `OxiDbQueryAndUpdate.cs`). Index usage: PRIMARY KEY point lookups and in-transaction reads consult indexes; **index-nested-loop join** (small outer ⋈ indexed base table) prunes the scan.

### Third engine — TSDB (`oxidb-tsdb/`)
A standalone time-series engine (InfluxDB-style), mounted like SQL. Off by default. Storage differentiator: each series (measurement × tag-set × field) is a **Gorilla-compressed** columnar stream (delta-of-delta timestamps + XOR floats; ~0.3 bytes/point on regular data), partitioned into sealed blocks so retention drops whole expired blocks.
- `OXIDB_TSDB` (`1`/`true`/`yes`/`on` to enable; default off — zero cost when unused). Per-database, like SQL.
- `OXIDB_TSDB_DATA` (default `${OXIDB_DATA}/tsdb` for the default db; named dbs at `${OXIDB_DATA}/<name>/tsdb`).

Persistence (`persist.rs`): a **versioned block snapshot + per-generation WAL**, with a `MANIFEST` as the atomic commit point (`blocks.<N>.tsb`, `wal.<N>.log`, `MANIFEST`). Every point is WAL-appended; a checkpoint seals active buffers, writes a fresh `blocks.<N+1>.tsb` snapshot (+fsync), atomically replaces the MANIFEST (temp+rename), then rotates the WAL. Recovery reads the MANIFEST's generation → snapshot + its WAL, so a crash on either side of the rename never double-counts; retention is durable (dropped blocks just aren't in the next snapshot). Auto-checkpoint past 8 MiB of WAL.

Fields are typed (float/**integer**/**boolean**/**string**); numeric values are stored as `f64` (ints exact to 2^53, bools 0/1), text in a separate `(ts, string)` path; the type is remembered per series and reported in query results. Numeric aggregations: mean/sum/min/max/count/first/last/**distinct** plus **`rate`** (per-second change, for counters) and **`percentile`** (`agg:"percentile"`+`p`, or shorthand `p95`; linear interpolation). Text-field aggregations: first/last/count/distinct.

**Continuous-aggregate rollups**: `add_rollup(measurement, interval, aggs)` materializes completed time buckets of every numeric series of a measurement into a derived measurement `<m>@<label>` with fields `<field>_<agg>` (e.g. `cpu@1m` `usage_mean`). Incremental via a persisted per-series watermark (crash-safe, no double-count on restart); the rule set persists in `rollups.json`. `refresh_rollups(now)` processes only fully-closed buckets. Roll from raw (1s→1m, 1s→1h); chaining is a caller concern.

Wire routing (`tsdb_bridge.rs`): `engine: "tsdb"` (or reserved `tsdb` command), `cmd: "tsdb"` with an `op`: `write` (points: measurement/tags/fields/ts — JSON bool→boolean, integer→integer, else float), **`write_lp`** (InfluxDB line protocol in an `lp` string; ms timestamps, `now()` when absent — `line_protocol.rs`), `query` (measurement + field + tag filters + `[start,end)` + optional `GROUP BY time(interval)` downsample + `group_by` tags + `agg`), `stats`, `retention` (cutoff), `checkpoint`, `rollup_add`/`rollup_refresh`/`rollups`. RBAC gates `tsdb` at `ReadWrite`; Read role is query-only. Node-local.
