# Changelog

## Unreleased

### Observability: Prometheus `/metrics` endpoint

- `GET /metrics` on the REST listener (`OXIDB_HTTP_PORT`) serves the
  Prometheus text exposition (format 0.0.4), zero new dependencies.
  Deliberately unauthenticated (standard scraper practice) — bind the
  HTTP port privately if the API is private.
- **Counters** (lock-free atomics on the hot paths):
  `oxidb_commands_total{class=insert|find|update|delete|count|aggregate|sql|tx|blob|other}`
  (every wire command, counted at the session-handler chokepoint — both
  sync and cluster TCP paths), `oxidb_errors_total` (every error
  response built by the server), `oxidb_http_requests_total`,
  `oxidb_tx_commits_total` / `oxidb_tx_conflicts_total` (OCC conflicts
  distinguished — the hot-account saturation signal).
- **Gauges** (computed per scrape): `oxidb_up`, `oxidb_uptime_seconds`,
  `oxidb_process_resident_memory_bytes` / `_cpu_percent` / `_threads`
  (via the existing `/proc/self` reader; zeros on non-Linux),
  `oxidb_collections`, `oxidb_documents{collection}` (index-backed
  count) and `oxidb_documents_total` — default database.
- PROC_STATS is now anchored at process start, so uptime measures the
  server, not "since first scrape".
### Time-series: range/time windows, `$densify`, `$fill`

- **`$setWindowFields` range windows** — in addition to document frames,
  `window: {range: [lo, hi], unit: "minute"}` frames by the value of the
  (single, ascending) `sortBy` field: with `unit` the field is a date and
  bounds are `value × unit`; without it the field is numeric. Enables
  true time-based moving averages ("last 5 minutes", not "last 5 docs").
  Bounds accept numbers / `"current"` / `"unbounded"`. Fixed units only
  (millisecond…week). Docs with an unparseable sort value get `null`.
- **`$densify`** — generate documents at stepped values of a numeric or
  date field where none exist: `{field, partitionByFields?, range:
  {step, unit?, bounds: "full" | "partition" | [lo, hi)}}`. Synthetic
  docs carry only the field + partition fields; explicit bounds are
  hi-exclusive (MongoDB semantics); docs with missing field pass
  through.
- **`$fill`** — fill null/missing fields per partition in sort order:
  `output: {f: {method: "locf"}}` (carry last value forward),
  `{method: "linear"}` (interpolate over the single sortBy axis — numeric
  or date), or `{value: const}`. Linear leaves leading/trailing nulls
  (MongoDB semantics); LOCF does not leak across partitions.
- The canonical gapless-series recipe now composes end-to-end:
  `$ohlcv` / `$dateHistogram` → `$densify` → `$fill`.
### Time-series: `$ohlcv` aggregation stage (tick → candle)

- New pipeline stage collapsing tick/trade documents into time-bucketed
  OHLCV candles:
  `{"$ohlcv": {"time": "ts", "interval": "1m", "price": "price",
  "volume": "qty", "symbol": "sym", "fill": "previous"}}` →
  `{symbol, time, open, high, low, close, volume, count}` per bucket.
  - Intervals share the `$dateHistogram` grammar (`1s`…`1y`, month/year
    calendar-aware); time field accepts ISO 8601 strings or epoch ms.
  - Input is time-sorted internally — open/close are correct without a
    preceding `$sort`; same-timestamp ticks keep input order.
  - `symbol` partitions candles per instrument in one pass.
  - `fill: "previous"` synthesizes flat candles for empty buckets
    carrying the previous close (o=h=l=c, volume 0, count 0) — the
    standard charting convention (LOCF).
  - Docs with unparseable time or non-numeric price are skipped.
  - Composes with the rest of the pipeline (`$match` before,
    `$sort`/`$limit`/`$project` after) and works through every surface
    that runs aggregation (doc engine, REST, WS, all clients).
### Cluster: Jepsen-style network-partition test (+ documented liveness gap)

- **`oxidb-server/tests/raft_partition_test.rs`** — deterministic
  network-partition testing for the Raft cluster, in-process (no docker
  / iptables). Partitions are injected at the transport by a
  `PartitionedFactory` that wraps the real `OxiDbNetworkFactory` and
  returns `Unreachable` for cut directed edges from a shared matrix —
  openraft sees exactly what a dropped-packet partition looks like.
  Two tests, both `--features cluster`:
  - `raft_survives_follower_partition` — strands two followers (leader
    stays in the majority). All four invariants hold across 3 rounds:
    no lost acked writes, no split-brain, majority stays available,
    full 5-node convergence on heal.
  - `raft_leader_partition_safety` — the hard case: the current leader
    is isolated into the minority. **Safety holds** — minority cannot
    commit, majority re-elects and stays available, the whole majority
    quorum converges and holds every acked write, and the stranded
    ex-leader never surfaces uncommitted/phantom data.
- **Documented liveness gap (openraft 0.9 has no PreVote):** an
  isolated leader inflates its term while partitioned and, on heal,
  returns as a disruptive stale-log candidate the quorum correctly
  ignores — so it does **not** rejoin/catch up without a restart. Data
  safety is unaffected (this is why the leader-partition test asserts
  majority-quorum convergence, not full-cluster). Fix path: upgrade
  openraft to a PreVote-capable release, or force an isolated leader to
  step down to follower. Tracked for the cluster hardening pass.
### Durability fix: commit log survives crash-during-persist (found by new Jepsen-style test)

- **`tests/jepsen_bank_crash.rs`** — Jepsen-style bank workload with
  crash faults: concurrent transfer transactions (journal doc + three
  balance `$inc`s, atomic; mixed OCC / `tx_find_for_update` paths), a
  parent process records ACKs, SIGKILLs the victim at a random moment,
  reopens the data dir and checks the history: every ACKed transfer
  present (durability), every account's balance derivable from the
  journal (atomicity — no half-applied transactions), journal uids
  unique (no double replay), global sum conserved. Rounds accumulate on
  one dir, so recovery-after-recovery is exercised too.
- **The test found a real pre-existing durability bug in 3 rounds:**
  `_tx_commit_log` was persisted by truncate-and-rewrite in place; a
  crash landing inside the persist left the file empty/torn, and
  recovery then discarded EVERY transactional WAL entry not yet in a
  snapshot — acked commits vanished wholesale. Fixed with atomic
  replace (tmp + fsync + rename + dir fsync); stale tmp cleaned on
  open. All transactional server versions carried this window.
- In-memory engines now use a unique temp dir per instance (was
  per-process): concurrent in-memory engines shared one commit log,
  cross-contaminating committed sets.
- Fixed `procedure.rs` test helper dropping its `TempDir` while the
  engine was live — tests ran on a deleted directory (masked by the old
  in-place persist writing to unlinked fds).
- Verified: 15 SIGKILL rounds on macOS + 15 on Linux (13.7k transfers),
  zero lost acks, zero partial applies; full lib/ACID/crash/sigkill
  suites green.

### Engine: group commit + pessimistic document locks (hot-account contention)

- **Group commit.** `commit_transaction` is now a two-phase pipeline:
  validate → WAL append (no fsync) → apply runs under the commit lock;
  the WAL fsync (`Wal::sync_shared`, leader-elected shared flush) and
  the tx-commit-log mark (submitted async, in ticket order) run outside
  it, so concurrent commits share physical fsyncs instead of paying one
  each. Ack still comes only after both fsyncs — durability semantics
  unchanged. Commit marks are submitted in apply order (turnstile), so
  a commit can never become durable without the commits whose writes it
  read; snapshot persistence waits for the turnstile to settle before
  writing (`wait_marks_settled`).
- **`tx_find_for_update`** — pessimistic per-document write locks
  (`SELECT ... FOR UPDATE`): matched docs are locked (sorted, re-read
  under the lock) until commit/rollback; waiting is bounded by a lock
  timeout (`Error::LockTimeout`). Hot documents (the exchange
  fee-account pattern) queue instead of burning OCC retries.
- New bench `tests/hot_account_bench.rs` (hot-ratio sweep × occ /
  for-update). On an M-series Mac, 8 workers: throughput 130 → ~300
  tx/s (2.3×), p99 at full contention 502ms → 48ms (10×), max 1.5s →
  58ms; for-update mode eliminates conflicts entirely (0.00/commit at
  every hot ratio). Money-conservation invariant holds in all modes.
  On Linux (4-core VPS, virtio): occ 1.5–2.2k tx/s with p99 ≤ 8.3ms,
  for-update 1.1–1.3k tx/s with p99 ≈ 10ms — both at full durability,
  every hot ratio. `tx_find_for_update`'s post-lock re-read uses the
  direct by-id cache lookup (`load_doc_arc`); the initial `find`-based
  re-read was a full scan per doc and cost for-update ~5–10× on fast
  disks.

### License change — proprietary as of v0.33.0

- OxiDB v0.33.0 and later is **proprietary, commercially licensed**
  software (`LICENSE` replaced; see `COMMERCIAL-LICENSE.md`). Engine
  crates now declare `license-file` instead of `AGPL-3.0-only`; the
  engine-bundling packages (`oxidb-embedded` on PyPI,
  `OxiDb.Client.Embedded` on NuGet) now ship the proprietary license
  instead of MIT. Thin TCP client libraries remain MIT. Versions up to
  and including v0.32.x stay available under their original licenses
  (early releases `MIT OR Apache-2.0`, later `AGPL-3.0-only`).

### SQL engine: stored procedures

- `CREATE [OR ALTER] PROCEDURE name(p TYPE, ...) AS BEGIN ...; END`,
  `CALL name(args...)`, `DROP PROCEDURE [IF EXISTS]`, `SHOW PROCEDURES`.
  Bodies are DML/SELECT batches with **named parameters** (rewritten to
  `$N` at creation — expression positions only, so INSERT column lists are
  safe; parameters shadow same-named columns). A top-level `CALL` is
  atomic via an implicit transaction; inside an open transaction it joins
  it. The CALL's result is the last statement's result set. Procedures are
  WAL-logged, checkpointed with the catalog, and replicate in cluster mode
  like other writes. v1 body restrictions: no DDL / transaction control /
  nested CALL. Stress-verified with a 1000+ line join- and math-heavy
  procedure (`oxidb-sql/tests/data/complex_procedure.sql`).

### .NET: OxiDb.EntityFrameworkCore — EF Core provider (ADR-0013 Phase E)

- New `OxiDb.EntityFrameworkCore` package (EF Core 9 / net10.0):
  `UseOxiDb("Host=...;Port=...;Database=...")`. Covers the relational
  basics end-to-end against a live server: `EnsureCreated` (migrations
  SQL with table-level `CONSTRAINT ... PRIMARY KEY` and `AUTO_INCREMENT`
  on store-generated integer keys), LINQ queries (Where/OrderBy/joins/
  GroupBy aggregates/`Contains`→`LIKE`/Skip/Take), `SaveChanges` with
  generated keys via `RETURNING`, optimistic-concurrency affected-row
  checks, and explicit transactions with rollback
  (`tests/efcore-oxidb-test/`). Minimal provider: no migrations
  scaffolding, no value converters beyond the built-in type mappings; not
  validated against the EF Core specification test suites.
- String translators: `Contains`/`StartsWith`/`EndsWith` (LIKE over
  CONCAT), `ToUpper`/`ToLower`/`Trim`/`Replace`/`Substring`, and
  `string.Length` → `LENGTH()`.

### SQL engine: derived tables, parameterized LIMIT/OFFSET, UPDATE/DELETE RETURNING

Engine gaps EF Core's query pipeline hits, useful to every client:

- **Derived tables**: `SELECT ... FROM (SELECT ...) AS x`, in FROM and in
  JOINs, with bind parameters inside the subquery (inline-view execution,
  same machinery as views).
- **Parameterized LIMIT/OFFSET**: `LIMIT $1 OFFSET $2` (EF parameterizes
  Skip/Take); literal counts unchanged.
- **`UPDATE ... RETURNING` / `DELETE ... RETURNING`**: project the
  updated (post-assignment) or deleted rows back as a result set — how EF
  counts affected rows (`RETURNING 1`); works inside transactions.
- **Table-level constraints**: `CONSTRAINT name PRIMARY KEY (col)` /
  `UNIQUE (col)` in `CREATE TABLE` (the shape EF migrations and pg_dump
  emit); single-column for now.

### SQL engine: Phase D DDL/types + cluster interactive commits (ADR-0013)

- `ALTER TABLE ADD/DROP/RENAME COLUMN`, column `DEFAULT`s, `DECIMAL`
  (stored as DOUBLE), a real `BLOB` type (base64 on the JSON wire),
  column-level `UNIQUE` now **enforced** (was silently ignored), FK syntax
  tolerated, and `INSERT ... RETURNING`.
- Cluster mode now supports interactive SQL transactions: statements run
  on the leader, a lone `COMMIT` replicates the buffered ops through Raft
  as one atomic `SqlTxnCommit` entry applied on every node.

### .NET: OxiDb.Data ADO.NET provider — Dapper works (ADR-0013 Phase C)

- New `OxiDb.Data` package: `OxiDbConnection` / `OxiDbCommand` /
  `OxiDbDataReader` / `OxiDbParameter` / `OxiDbTransaction` /
  `OxiDbFactory` over the TCP wire client. Named `@parameters` are
  rewritten to positional placeholders; column CLR types come from the
  wire's `types` metadata (INT→long, TIMESTAMP→DateTime, ...);
  `DbTransaction` rides the interactive session transactions from Phase B,
  including `Save`/`Rollback(name)`/`Release` savepoints. Connection
  string: `Host=...;Port=...;Database=...` (database via session
  `use_db`). **Dapper runs end-to-end** — typed queries, named params,
  multi-command transactions (`tests/adonet-dapper-test/`).

### SQL engine: interactive transactions + savepoints (ADR-0013 Phase B)

- `BEGIN` now opens a transaction that spans requests on the same
  connection: statements across round-trips share it (read-your-writes,
  invisible to other sessions), `COMMIT` flushes one atomic WAL batch.
  `SAVEPOINT` / `ROLLBACK TO SAVEPOINT` / `RELEASE SAVEPOINT` give partial
  rollback. A statement error aborts the transaction; disconnect rolls it
  back; it stays bound to the database it began on. Engine API:
  `execute_params_in_session` (the old `execute*` entry points keep their
  batch-scoped auto-rollback contract).
- Cluster mode initially rejected cross-request SQL transactions
  (self-contained `BEGIN..COMMIT` batches still replicated whole); the
  Phase D entry above lifted this via `SqlTxnCommit`.

### SQL engine: EF-oriented expression surface (ADR-0013 Phase A)

- `CASE WHEN` (searched + simple form, short-circuiting), `[NOT] LIKE`
  (`%`/`_`, `ESCAPE`), `CAST`, `SELECT DISTINCT`, `EXISTS`/`NOT EXISTS`
  (correlated included — rewrites onto the correlated-IN machinery), string
  scalars `UPPER`/`LOWER`/`LENGTH`/`SUBSTRING`/`CONCAT`/`TRIM`/`LTRIM`/
  `RTRIM`/`REPLACE`/`ABS` and the `||` operator.
- SELECT results now carry per-column type metadata: wire shape gains a
  `"types"` array (`"INT"`... or `null` when unknown) — groundwork for an
  ADO.NET `DbDataReader`. ADR-0013 documents the full EF Core roadmap
  (interactive transactions, ADO.NET provider, Migrations DDL, EF provider).

### SQL engine: COALESCE / IFNULL / NULLIF

- `COALESCE(a, b, ...)` (first non-NULL, short-circuiting), `IFNULL(a, b)`,
  and `NULLIF(a, b)` work everywhere expressions do — SELECT lists, WHERE,
  JOIN conditions, GROUP BY/HAVING, and over aggregates
  (`COALESCE(SUM(x), 0)` with LEFT-JOIN NULL padding).

## v0.33.1

### SQL engine: AUTO_INCREMENT primary keys

- `INT PRIMARY KEY AUTO_INCREMENT` (also `AUTOINCREMENT` and
  `GENERATED ... AS IDENTITY`): omitted or `NULL` insert values draw
  sequential ids from a per-table counter; the INSERT result reports the
  last assigned value as `last_insert_id`. Explicit values push the counter
  past themselves; the counter survives restarts (seeded `max+1`), works
  inside transactions, and `DESCRIBE` gains an `auto_increment` column.

## v0.33.0

### SQL-text user management (server 0.32.3)

- `CREATE USER name WITH PASSWORD '...' [ROLE r]`, `ALTER USER` (password
  and/or role), `DROP USER [IF EXISTS]`, `SHOW USERS`, and per-database
  grants — `GRANT role ON DATABASE db TO user` / `REVOKE [ALL] ON DATABASE
  db FROM user` — now work as SQL statements, mirroring the wire commands
  (`create_user` etc.) against the same SCRAM user store with the same
  Admin-only gate. Single-statement only; wire-protocol only (REST rejects
  them clearly — its JWT auth manages a different user system).

### Multi-database: remaining limitations closed (ADR-0012, server 0.32.2)

- **Raft-replicated database DDL** — in cluster mode `create_database` /
  `drop_database` (either surface) go through Raft and apply on every node;
  writes and SQL targeting a named database replicate wrapped in a `Scoped`
  envelope (old log entries replay unchanged against the default database).
  Transaction commits replicate against the database the transaction began
  on.
- **SQL-text database DDL** — `CREATE DATABASE [IF NOT EXISTS]`,
  `DROP DATABASE [IF EXISTS]`, `SHOW DATABASES`, `USE <db>` now work as SQL
  statements (single-statement only). Wire commands and SQL text parse into
  one shared intent with one permission gate: create/drop require the admin
  role — the SQL form cannot slip through the `sql` command's ReadWrite
  gate.
- **Background threads on every database** — TTL eviction and alert
  evaluation now run per database (previously default-only; cluster nodes
  ran *no* TTL eviction at all). `drop_database` shuts the engine down
  explicitly — previously its TTL thread held the engine alive forever
  (leak), and `OxiDb::shutdown` never signalled the TTL thread.
- **REST/WS database targeting** — REST accepts `?db=<name>` on every
  route (including `POST /api/sql`); WebSocket messages accept a `"db"`
  field, and subscriptions watch (and clean up on) the database they were
  opened against.
- **Transactions bound to their database** — a session's open transaction
  now rejects requests targeting a different database (previously they'd
  silently hit the wrong engine).

Still intentionally global: OxiMem/RESP keyspace (Redis-style numbered
databases are an orthogonal protocol concept) and S3 buckets (one global
bucket namespace, matching S3 semantics).

### Multi-database SQL + database-aware cluster path (ADR-0012, server 0.32.1)

The document engine has had first-class databases since v0.19
(`create_database`/`drop_database`/`list_databases`/`use_db`, per-request
`db` field, per-database RBAC). This release closes the gaps around it:

- **Per-database SQL engines** — SQL requests now land in the database the
  session resolved, not one global catalog. Default database keeps its
  historical `OXIDB_SQL_DATA` directory (existing data untouched); every
  other database gets `${OXIDB_DATA}/<name>/sql`, dropped together with the
  database. Same tables in different databases are fully isolated.
- **Async/cluster server is database-aware** — it now routes by
  `db`/session database, implements the four database commands, and opens
  the same managed data layout as standalone mode (previously it served
  only the default database and used an incompatible flat layout).
- **Flat-layout auto-migration fixed** — it now recognizes the B-tree
  engine's files (`.btree`/`.worm`/`.bopts`/`.bdat`; previously only the
  original `.dat`-era extensions, leaving modern flat layouts orphaned),
  moves `_archive`/`_gsn` along, and never overwrites an existing file
  (collides → warn and leave the flat file in place).

Known limitations (documented in ADR-0012): database DDL is node-local in
cluster mode (not Raft-replicated); SQL-text `CREATE DATABASE`/`USE` are not
parsed (wire commands cover it); TTL/alert threads run on the default
database only; REST/WS/OxiMem/S3 address the default database.

### SQL engine: auto-checkpoint + disk-first row storage

- **Auto-checkpoint** — the engine now folds the WAL into per-table `.rdat`
  snapshots automatically once the live WAL exceeds
  `OXIDB_SQL_CHECKPOINT_BYTES` (default 64 MiB; `0` = manual only).
  Previously checkpoints never ran in production: the WAL grew without bound
  and every restart replayed all of it.
- **Disk-first mode** (`OXIDB_SQL_DISK_FIRST`) — rows are served from the
  mmap'd last-checkpoint snapshot; RAM holds only the changes made since
  that checkpoint (auto-checkpointing bounds that overlay). At 1M rows this
  roughly halves resident memory (272 → 143 MB) and speeds up open, at a
  decode cost on full scans (11 → 43 ms). Indexes and the PRIMARY KEY map
  stay in RAM. Both modes share the same on-disk format, so a database can
  be reopened in either mode. New `SqlOptions` /
  `SqlEngine::open_with_options` on the crate API; snapshot CRCs are
  verified once at map time.

### SQL engine: catalog introspection

- New statements `SHOW TABLES` (with row counts), `SHOW VIEWS`,
  `SHOW INDEXES [FROM table]`, and `DESCRIBE table` /
  `SHOW COLUMNS FROM table` — answered from the catalog as ordinary result
  sets. Read-only: allowed for the `read` role, never Raft-replicated, and
  inside a transaction they see the transaction's own uncommitted DDL.

### VS Code extension 0.2.0: SQL engine support

- New **SQL** explorer view: tables (with row counts), columns, indexes, and
  views, introspected live from the server; refresh, drop-table/view and
  "Select Top 100" context actions.
- **New SQL Query** command opens a `.sql` editor; **Run SQL** (`Cmd/Ctrl+Enter`
  or `F5`) executes the selection or whole file — with `?` / `$N` parameter
  support — and renders result grids per statement.
- Document-engine improvements: connection status-bar item, pipelined TCP
  client (requests no longer race), collection view kept usable.

## v0.32.0

### SQL engine: insert benchmark + WAL sync mode

- New differential write benchmark (`oxidb-sql/examples/insert_bench.rs` +
  `insert_bench_postgres.sh`): identical INSERTs into a table with a PK and
  four secondary indexes (one composite), measured at both durability
  levels on both engines. At full durability OxiDB bulk-loads 2.1× faster
  than PostgreSQL 15 and ties on single-row inserts (fsync-bound); at
  PostgreSQL's own default (cache-level) durability OxiDB is 16× faster in
  bulk and 5.5× faster per single insert. Parity checks byte-identical.
- New `OXIDB_SQL_SYNC` = `full` (default) | `data`: WAL sync mode for the
  SQL engine, the same trade PostgreSQL exposes as `wal_sync_method`
  (`data` = OS-cache-level sync; on macOS an explicit `fsync(2)`, since
  Rust's `sync_data` still issues `F_FULLFSYNC` there).

### SQL engine: join reordering + parallel hash joins

- **Greedy join reordering** — all-INNER join chains execute
  smallest-table-first among the joins whose `ON` is already fully
  resolvable (and still equi-joins) against the tables placed so far;
  written order is kept for outer joins, view sources, or `ON` clauses
  with unqualified columns.
- **Parallel probe/build (rayon)** — above 32k rows the hash-join probe
  runs over left-tuple chunks in parallel (chunk-ordered concat keeps the
  emitted rows identical to the sequential loop; right-matched bitmaps
  OR-merge for outer joins), and build-side key evaluation parallelizes
  the same way. Small queries stay sequential and unaffected.
- Hard-join benchmark at 20× scale: Q1 21.2 ms / Q2 20.7 ms vs
  PostgreSQL 15's 45.1 / 47.9 — the lead grows to 2.1–4.8× across all
  four queries (`oxidb-sql/BENCHMARKS.md`).

### SQL engine: cluster replication, embedded surface, correlated subqueries, views, window functions

- **Raft replication (server 0.31.5)** — in cluster mode, SQL writes (any
  non-SELECT statement) replicate through Raft and re-execute on every node;
  SELECTs run node-locally. All nodes must set `OXIDB_SQL=1`.
- **Embedded FFI SQL** — `oxidb-embedded-ffi` routes `engine:"sql"` requests
  to a per-handle SQL engine at `<data dir>/sql`, opened lazily on first use
  (no env var in embedded mode). Python `oxidb-embedded` and .NET
  `OxiDb.Client.Embedded` gain the same `sql()` surface as the TCP clients.
  The JSON bridging moved into `oxidb_sql::json`, shared by server and FFI.
- **Correlated subqueries** — subqueries may reference the enclosing query's
  tables (one level up; inner names shadow outer, per SQL scoping) and
  re-execute per outer row; works in SELECT/UPDATE/DELETE. Rejected inside
  aggregated queries and window functions.
- **Views** — `CREATE [OR REPLACE] VIEW` / `DROP VIEW [IF EXISTS]`; the body
  (a single SELECT) is trial-run at creation, stored in the catalog, and
  re-executed fresh whenever the view is selected from or joined.
- **Window functions** — `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()` and
  `COUNT/SUM/AVG/MIN/MAX(...) OVER (PARTITION BY ... ORDER BY ...)`;
  whole-partition without ORDER BY, standard running-with-peers frame with
  it. SELECT-list only; explicit frames unsupported.
- **ADR-0011** — proposed design for cross-engine (document + SQL)
  transactions via a shared GSN commit clock and a two-log 2PC; not
  implemented.

### SQL engine: v1 gap closure

The standalone SQL engine (below) gained the features its v1 said were
missing:

- **PRIMARY KEY uniqueness is enforced** — duplicate key values are rejected
  on INSERT (multi-row batches are checked whole, all-or-nothing), UPDATE,
  and inside transactions; enforcement survives restart/recovery. A table
  can declare at most one PK column.
- **Implicit numeric coercion** — integer values widen into `DOUBLE` columns
  and into `TIMESTAMP` columns (epoch ms), for literals and parameters.
- **Timestamp literals** — `TIMESTAMP '2026-01-02 03:04:05'`
  (also `YYYY-MM-DD`, `T` separator, fractional seconds, `Z`/`±HH:MM`).
- **OFFSET** — `LIMIT n OFFSET m` and bare `OFFSET`.
- **UNION / UNION ALL** — with outer ORDER BY (output names or 1-based
  positions), LIMIT and OFFSET applying to the combined result.
- **`[NOT] IN` lists** with SQL three-valued NULL semantics.
- **Uncorrelated subqueries** — scalar `(SELECT ...)` in any expression
  (0 rows = NULL, >1 row errors) and `[NOT] IN (SELECT ...)`, usable in
  SELECT/UPDATE/DELETE/INSERT and inside transactions.
- **Multi-column secondary indexes** — `CREATE INDEX i ON t (a, b)`; used
  when the WHERE clause has equality conjuncts for all index columns
  (widest qualifying index wins). Old single-column catalogs/WALs load
  unchanged.
- **Read-role SQL (server 0.31.4)** — the `sql` command is now allowed for
  the `read` role, restricted to SELECT statements; the flag is decided at
  the session layer (TCP RBAC and REST JWT paths) and enforced per
  statement by the SQL bridge.

### Second engine: standalone SQL (ADR-0010)

OxiDB can now mount a real relational SQL engine alongside the document engine
in the same server. It is a separate crate (`oxidb-sql`) with its own typed
tables, catalog, row storage, WAL + crash recovery, secondary indexes, and
transactions — it shares **no state or files** with document collections and
is **off by default** (zero cost when unused).

- **Enable** with `OXIDB_SQL=1`; data lives under `${OXIDB_DATA}/sql`
  (override: `OXIDB_SQL_DATA`).
- **Wire**: `{ "engine": "sql", "cmd": "sql", "sql": "SELECT ...", "params": [...] }`
  → one result per statement (`{columns,rows}` / `{affected}` / `{ddl}` /
  `{transaction}`). Requests without `engine` keep the document path
  byte-for-byte. RBAC: `sql` requires ReadWrite. Node-local in v1 (not
  Raft-replicated).
- **REST**: `POST /api/sql` with `{"sql": "...", "params": [...]}` (the old
  410 stub from the removed doc-engine SQL translator is gone).
- **SQL surface**: CREATE/DROP TABLE + single-column indexes (IF [NOT]
  EXISTS), INSERT (multi-row = one fsync, statement-atomic), UPDATE, DELETE,
  SELECT with WHERE / INNER-LEFT-RIGHT-FULL joins / GROUP BY / HAVING /
  aggregates / ORDER BY (incl. projection aliases) / LIMIT, `?`/`$N`
  parameters, and BEGIN/COMMIT/ROLLBACK transactions.
- **Performance**: late-materialization executor (column-pruned flat scans,
  u32 index-tuple joins, direct-address dense-int join index, streaming
  group/aggregate). On the hard multi-join differential benchmark
  (`oxidb-sql/BENCHMARKS.md`) it beats PostgreSQL 15 on all four queries at
  20× scale (e.g. 5-way join + GROUP BY over 300k items: 26.8 ms vs 45.1 ms)
  with byte-identical results.
- **Clients**: Python `db.sql(sql, params)` (0.27.0), Go
  `client.Sql(sql, params...)` → `[]SqlResult`, .NET
  `IOxiDbClient.SqlAsync(sql, params)` (TCP; embedded throws
  `NotSupportedException`), JS `db.sql(sql, params)` via REST (0.25.0).
- **Docs**: `docs/sql.md` rewritten for the new engine;
  `docs/protocol-reference.md` and `docs/server.md` updated.

## v0.31.1

### `$ne` / `$nin` now match documents that lack the field (server 0.31.1)

OxiDB was **excluding** documents that don't contain the queried field from
`$ne`/`$nin` results — the opposite of MongoDB, where an absent field is "not
equal to" any concrete value and therefore matches. The practical impact was a
data-loss-shaped bug: a filter like `{owner_banned: {$ne: true}}` over a
collection where most documents simply omit `owner_banned` returned almost
nothing instead of every non-banned document.

The wrong behavior lived in four places in `src/query.rs`:

- **Post-filter paths** (`eval_field_op` in-memory, `matches_raw_inner`
  byte-level/disk-first) treated a missing field as a non-match for every
  comparison operator, including `$ne`/`$nin`.
- **Index-backed paths** (`execute_field_op`, `execute_indexed_lazy`) served
  `$ne` from `find_ne`/`for_each_ne`, which only iterate documents that *have*
  the field — so missing-field documents could never be returned.

The fix:

- A missing field now **matches** `$ne`/`$nin`, **unless** the operand is
  `null` — MongoDB treats an absent field as `null` for these comparisons, so
  `{f: {$ne: null}}` and `{f: {$nin: [null]}}` still exclude it.
- `$ne` is no longer served by the field index (which can't enumerate
  missing-field documents); it falls through to a full scan + corrected
  post-filter, matching how `count_indexed` already handled it. `estimate_rows`
  no longer produces an index estimate for `$ne`.

Regression coverage: `collection::tests::ne_and_nin_include_missing_field`
exercises indexed and non-indexed collections plus the `$ne: null` edge case.

## v0.31.0

### Go connection pool: no slot leak on failed reconnect (server 0.31.0)

`Pool.checkout` closed a stale connection and dialed a replacement, but on a
failed dial it dropped the slot on the floor. During a backend outage every
failed checkout permanently shrank the pool until all slots were gone and
`Get()` blocked forever — even after the backend recovered. The closed conn is
now returned to the channel, so capacity is preserved and the pool self-heals as
soon as the backend is reachable again (`go/oxidb/pool.go`).

### Engine audit, round 2 — concurrency, `$group` identity, PITR, Mongo-compat operators (server 0.30.8)

Closes the remaining findings from the full-engine audit (commit `95e50670`).

**Concurrency / transactions**
- `update()` merges its prepare phase into the write-locked apply: each matched
  doc is re-read and the update recomputed against current content, closing the
  read→apply gap that left stale index entries, resurrected concurrently-deleted
  docs, and lost concurrent updates. The `_version` bump is now an atomic RMW,
  with an intra-batch unique check.
- `commit_lock` is now an `RwLock`: tx commits take the write lock, direct
  `update`/`delete`/`find_and_modify` take the read lock — a direct write can no
  longer slip into a commit's validate→apply window and be blindly overwritten
  (OCC now observes every writer), while direct writes stay concurrent with each
  other.

**Aggregation correctness**
- `$group` keys are no longer identified by their 64-bit hash alone: groups live
  in an insertion-ordered vec with hash→index buckets and every hit verifies the
  materialized key, so SipHash collisions can no longer silently merge distinct
  groups (`exec_group`, `StreamingGroup`).
- `try_index_group` is gated on an explicit full-scan flag from the engine
  instead of a docs-count heuristic — a `$match`-filtered subset can no longer
  fall through to index-read groups that ignore the filter.

**Lifecycle / PITR**
- `drop_collection` now removes `.bdat`/`.bopts`/`.worm`, sealed `.wal.<seq>`
  segments (previously replayed into a re-created collection, resurrecting
  dropped docs), and `.mfidx` files.
- `Wal::checkpoint` resets the header so the next append rewrites the `OXWA`
  header instead of degrading to legacy format; a truncated `_gsn` lease file is
  now a hard error instead of silently restarting GSNs at 1 (duplicate GSNs
  corrupted replay dedup); `prune_archive` deletes the data-dir original with the
  `.seg`; `replay_into` warns when the earliest replayable GSN is above the base
  watermark.

**MongoDB-compatible update operators**
- `$pull` with an operator/condition object actually pulls (was a literal
  no-op); `$push`/`$addToSet` support `{$each: [..]}`; `$inc`/`$mul` do checked
  integer arithmetic (overflow errors, exact above 2^53, never write null from a
  non-finite f64); dotted `$project` inclusion/exclusion builds/removes nested
  structure; `$unset` on an array element nulls in place; `$rename` moves
  explicit-null fields; `$pop` validates its operand on empty/missing arrays.

**Performance**
- `insert()` logs+fsyncs the WAL before taking the index write lock when there
  are no unique indexes (readers no longer stall behind every insert's fsync);
  full scans no longer flood the LRU; `find({})` decodes straight into the
  returned `Arc`; `$setWindowFields` running totals and whole-partition frames
  fold once instead of O(n²); `archive_pass` skips the manifest rebuild on idle
  ticks.

### Engine audit, round 1 — durability, correctness, perf (server 0.30.7)

Durability, correctness and performance fixes from the full-engine audit
(commit `2fa7b7d4`).

**Durability (silent data loss)**
- `storage`: torn tail truncated at open — post-crash WAL replay used to
  re-append acknowledged writes *behind* the garbage, where the next open's scan
  never reached them (lost once the WAL was checkpointed). `read_exact_at`
  replaces `read_at` (short reads had returned zero-filled buffers); failed
  appends roll the file back to the last record boundary so an ENOSPC mid-write
  no longer desyncs every later `DocLocation`.
- WAL replay filters transactional entries through the tx commit log — a tx that
  died between its WAL fsync and `mark_committed` is no longer resurrected or
  half-applied across collections; the engine eagerly recovers collections with
  pending WAL at startup.
- `btree_storage`: compaction aborts on a read error instead of silently
  dropping the live document from the rewritten file, with a dir-fsync after the
  rename.

**Wrong results**
- Index-backed sort no longer drops non-eq predicates (`matches_value` was
  skipped whenever one indexed eq existed) — creating an index no longer changes
  result sets; `skip`+`limit` uses saturating arithmetic (skip with no limit had
  wrapped and returned `[]`).
- `query`: AND range merge keeps the **tightest** bounds instead of last-wins
  (`{a:{$gt:10,$gte:5}}` had returned the unfiltered superset); contradictory
  ranges count 0.
- TTL eviction range-scans from `DateTime(i64::MIN)`, not `Unbounded` — numeric/
  null/bool values in a TTL field are no longer evicted regardless of expiry.

**Concurrency**
- Per-name open locks (concurrent first-touch opens of the same collection could
  persist a stale snapshot and truncate the winner's WAL); `insert_many` reserves
  its id block with `fetch_add`; the intra-batch unique map is hoisted out of the
  per-doc loop (it was recreated empty per doc, so duplicates sailed through);
  encode errors fail the batch instead of persisting empty documents.

**Resilience at open**
- `fts` index written via tmp+fsync+rename, corrupt index quarantined and
  rebuilt instead of bricking `open`, extractor panics no longer kill the worker;
  `blob` rejects bucket `"."` and quarantines torn/undecryptable `.meta`;
  `mmap_field_index` bounds-checks an untrusted `entry_count`; compressed
  collections account `total_bytes` in on-disk units so auto-compaction can
  trigger.

**Performance**
- `$lookup` is now a hash join (one `$in` query instead of one query per input
  doc); `$sort` decorates once per doc; `$in`/`$nin` operands are sorted+deduped
  at parse and binary-searched; per-doc FTS term lists make re-index O(doc terms)
  instead of a full inverted-index sweep.

### OxiPool: routing, fan-out and merge correctness + availability (server 0.30.8)

Hardening pass on the sharding proxy (commit `14fddd14`): shard-key resolution
only hashes scalars (operator objects scatter instead of targeting one wrong
shard), `update_one`/`delete_one` probe serially and stop at the first match,
`find` applies sort/skip/limit globally, split-aggregation passthrough is a
fail-closed whitelist, backend write failures `spawn_replace` instead of leaking
connections, and `OXIPOOL_REQUEST_TIMEOUT`/`OXIPOOL_IDLE_TIMEOUT` bound hung and
idle shards. Command classification now parses the real `cmd` field instead of
substring-scanning the payload.

### Aggregation: window functions (`$setWindowFields`) (server 0.30.5)

Adds SQL-style window functions — compute a value for each document from a
window of neighbouring documents **without collapsing rows** (the way `$group`
does). Partition by an expression, order each partition by `sortBy`, then add
`output` fields:

```json
[{ "$setWindowFields": {
     "partitionBy": "$region",
     "sortBy": { "date": 1 },
     "output": {
       "runningTotal": { "$sum": "$amount", "window": { "documents": ["unbounded", "current"] } },
       "movingAvg7":   { "$avg": "$amount", "window": { "documents": [-6, 0] } },
       "rank":         { "$rank": {} },
       "prevDay":      { "$shift": { "output": "$amount", "by": -1, "default": 0 } }
     }
} }]
```

Supported `output` operators:
- **Accumulators over a window**: `$sum`, `$avg`, `$min`, `$max`, `$count`,
  `$first`, `$last`, `$push`, `$addToSet`, `$percentile` (reuses the `$group`
  accumulator machinery). Document-based windows `window: { documents: [lo, hi] }`
  where each bound is an integer offset, `"unbounded"`, or `"current"`; the
  default window (no `window`) is the whole partition.
- **Positional/ranking**: `$rank` (ties share a rank, gaps after),
  `$denseRank` (no gaps), `$documentNumber`, and `$shift` (lag/lead with a
  default). These require `sortBy`.

Implementation (`src/pipeline.rs`): partitions by the `partitionBy` expression
(preserving first-seen order), stable-sorts each partition by `sortBy`, then for
each row computes outputs from the immutable partition (so window outputs never
feed into each other) and writes them back. Works on every aggregation path (the
Arc executor delegates to the owned-value executor). Range/time windows are not
yet supported (clear error). Tested at the pipeline level (running total, moving
average, `$shift`, rank/denseRank/documentNumber with ties, validation) and
end-to-end over the wire.

### Aggregation: `$facet` stage (server 0.30.4)

New `$facet` aggregation stage — runs several independent sub-pipelines over the
**same** input documents in one pass and emits a single document whose fields
are each sub-pipeline's result array. The one-pass primitive behind faceted
search and dashboards (e.g. results + per-category counts + price buckets +
top-N from a single query, instead of N separate scans):

```json
[{ "$match": { "inStock": true } },
 { "$facet": {
     "byCategory": [{ "$group": { "_id": "$category", "n": { "$sum": 1 } } }],
     "top5":       [{ "$sort": { "rating": -1 } }, { "$limit": 5 } ],
     "total":      [{ "$count": "n" }]
 } }]
```

Each sub-pipeline is parsed/executed by the existing pipeline engine over a copy
of the (already-buffered, in-memory) input — no extra storage scans; the input
is moved into the last sub-pipeline to avoid one clone. Nested `$facet` and
`$out` inside a sub-pipeline are rejected (as in MongoDB), as are empty/non-array
specs. Works on every aggregation path (the Arc executor delegates `$facet` to
the owned-value executor). Tested at the pipeline level and end-to-end over the
wire (`$match` → `$facet` with group/count/sort+limit sub-pipelines).

### `create_collection_with_options` in the .NET and JS clients (server 0.30.3)

Mirrors the Python/Go helpers in the remaining first-party clients.

- **.NET** (`OxiDb.Client.Tcp` + `OxiDb.Client.Embedded`, both via the shared
  `IOxiDbClient`): `CreateCollectionWithOptionsAsync(name, StorageOptions, ct)`.
  New `StorageOptions` class with nullable properties (`DiskFirst`, `Compress`,
  `AutoCompact`, `CompactMinBytes`, `CompactDeadRatio`); `ToWire()` emits a
  string-keyed map omitting unset fields (so it serializes correctly over both
  JSON and OxiWire, and the server fills defaults via `#[serde(default)]`).
  .NET packages → 0.29.0.
- **JS** (`oxidb-js`, REST SDK): `createCollection(name, options?)` gains an
  optional storage-options object (`{disk_first, compress, ...}`); `StorageOptions`
  added to the TypeScript types. Backward compatible — `createCollection(name)`
  is unchanged. npm package → 0.24.0.
- **Server (REST)**: `POST /api/collections` now accepts an optional `options`
  object and routes to `create_collection_with_options` (invalid options → 400),
  which is what the JS client uses. The TCP-based clients (.NET) use the existing
  wire command unchanged.

Validated end-to-end: the JS client over REST and a server-side check both
confirm the on-disk shape (`.bdat` + persisted `.bopts` with the exact options;
plain collections stay in-RAM). Full server suite green.

### Raft replication for `create_collection_with_options` (server 0.30.2)

Completes the cluster story for the per-collection-options wire command (0.30.1
noted it executed only on the receiving node). In the `cluster` build it now
replicates through Raft like every other write:

- New `OxiDbRequest::CreateCollectionWithOptions { name, options:
  StorageOptions }` variant; applied by the state machine
  (`raft::log_store::apply_request`) via `db.create_collection_with_options`, so
  every node creates the collection with the same storage shape.
- `is_write_command` includes the command, and `build_raft_request` parses the
  `options` object **on the leader** before proposing — invalid options return
  `None`, falling through to local execution which reports the error to the
  client instead of replicating a doomed entry.

Tests: `async_server` unit tests for the build path (options parsed, defaults
when absent, invalid-options → fall-through). Full server suite green with
`--features cluster` (raft suites included) and in the default build.

### `create_collection_with_options` over the wire (server 0.30.1)

The per-collection `StorageOptions` API (0.30.0) is now reachable over the
length-prefixed JSON wire protocol, so remote clients — not just embedded
callers — can choose a collection's storage shape:

```json
{"cmd": "create_collection_with_options",
 "collection": "events",
 "options": {"disk_first": true, "compress": false}}
```

The `options` object maps to `StorageOptions`; omitted fields fall back to the
defaults (in-RAM, compressed, auto-compaction on) — `StorageOptions` is now
`#[serde(default)]`, so partial option objects (and forward-compatible `.bopts`
files) deserialize cleanly. The command requires the **ReadWrite** role (same as
`create_collection`) and is handled by `handler::handle_request`, the universal
execution path for both the standalone server and the cluster build. Tested in
`oxidb-server/tests/handler_test.rs` (create disk-first+uncompressed over the
wire, insert/read back, assert the `.bdat`/`.bopts` exist and no `.btree`).

Note: in a Raft cluster this initially executed only on the receiving node;
0.30.2 (above) adds the `OxiDbRequest` variant so it replicates through Raft.

### Per-collection storage options (server 0.30.0)

The disk-first storage knobs — disk-first vs in-RAM, `.bdat` compression, and
the compaction policy (auto/min-bytes/dead-ratio) — were process-wide
`OXIDB_*` env vars: every collection in an engine shared one setting. They are
now **per-collection**, via a new `StorageOptions` struct and
`OxiDb::create_collection_with_options(name, opts)`. One engine can now host,
say, a disk-first *uncompressed* analytics collection next to a default in-RAM
one.

- `StorageOptions { disk_first, compress, auto_compact, compact_min_bytes,
  compact_dead_ratio }` — `Default` is in-RAM/compressed; `StorageOptions::
  from_env()` reproduces the old env-var behavior and remains the default for
  collections created without explicit options. The env vars are unchanged and
  still act as the default, so existing deployments and the env-var workflow
  behave exactly as before.
- For disk-first collections the resolved options are **persisted** next to the
  data as `<name>.bopts`. On reopen they are read back, so a collection's
  on-disk format is consistent regardless of the current environment —
  previously, flipping `OXIDB_DISK_FIRST` between runs could mismatch an
  existing collection. When no `.bopts` is present (collections created before
  this change), the format is detected from the on-disk files (`.bdat` →
  disk-first, `.btree` → in-RAM), so old data dirs open correctly.
- `BTreeStorage` now carries a resolved `StorageOptions` instead of reading the
  env helpers ad hoc; `should_compact` and `compact` consult it.

Tests: `tests/per_collection_options.rs` (a disk-first-uncompressed collection
alongside an in-RAM one in one engine; env-independent reopen; compressed vs
uncompressed `.bdat` sizes). Full lib (801) + soak suites pass in all configs
(default / disk-first / disk-first-uncompressed); server suite green.

### Uncompressed `.bdat` mode for disk-first storage (server 0.29.3)

New opt-in `OXIDB_DISK_UNCOMPRESSED=1` (only meaningful with
`OXIDB_DISK_FIRST=1`): disk-first stores write the `.bdat` **without** zstd
compression. This trades a little disk for a lot of CPU — uncompressed records
skip the per-record compress on write *and* the decompress on read, and (when
unencrypted) are read **zero-copy** straight from the mmap via the
`Storage::for_each_payload` scan path added in 0.29.1. Reads stay adaptive
(records are decoded by their magic bytes), so the flag needs no migration and
mixed compressed/uncompressed files read back correctly; compaction rewrites
records in whichever mode is active.

This closes nearly the entire disk-first performance gap on the 1M-doc
benchmark — full-collection scans/aggregations and index builds were
decompression-bound (every record carried a zstd frame, defeating the zero-copy
path; see the 0.29.1 caveat):

| 1M-doc, disk-first        | compressed | uncompressed | in-RAM | MongoDB |
|---------------------------|-----------:|-------------:|-------:|--------:|
| Insert                    |      6.24s |        5.45s |  5.26s |   ~5.0s |
| Build 8 indexes           |     18.2s  |        2.43s |  1.53s |    4.6s |
| Group by dept + avg       |      2.54s |        248ms |  280ms |   360ms |
| Group by city + stats     |      2.36s |        378ms |  391ms |   305ms |
| Unindexed scan            |      714ms |        582ms |  429ms |   586ms |
| **Wins vs MongoDB**       |     5 / 18 |     11 / 18  | 12/18  |       — |
| Disk footprint            |       689M |         727M |   612M |    323M |

Uncompressed disk-first now wins 11/18 (vs in-RAM's 12/18) — aggregations went
~10× faster and now beat/tie MongoDB, index build ~7×, all while keeping the
low resident-memory benefit. Disk grew only ~5% (689M→727M): zstd was buying
almost nothing on these small structured docs while costing ~10× on every scan.
Implemented as a `compress: bool` on `Storage` (`open_with_options`); the in-RAM
store and the default compressed disk-first mode are unchanged. Full lib (801)
suite passes in all three configs (default / disk-first / disk-first
uncompressed).

### Batched parallel-compress append for bulk insert (server 0.29.2)

Disk-first bulk insert was ~2.4× slower than MongoDB (and the in-RAM store)
because `insert_many_prepared` wrote the data file **one document at a time**:
each `storage.insert` was a `Storage::append_no_sync` that zstd-compressed the
single doc, took the storage mutex, `lseek(SEEK_END)`'d, and issued three
`write_all`s — ×1,000,000. The codebase already had a batched append that
compresses the whole batch **in parallel** and writes it with a single lock +
seek + `write_all` (`append_batch_no_sync_buffered`), but nothing used it.

`insert_many_prepared` now splits the prepared batch into its encoded bytes
(one `storage.insert_batch` call — buffered parallel-compress append on disk, a
parallel tree fill in-RAM) and the `(id, Value)` pairs the index/cache pass
needs, instead of interleaving a per-document storage write. The WAL path is
unchanged (still one buffered write + one fsync per batch), so durability is
identical.

Result on the 1M-doc benchmark, disk-first: **insert 12.1s → 6.24s** (~1.9×;
from 2.4× down to 1.3× vs MongoDB, approaching the in-RAM 5.26s). In-RAM and
MongoDB-parity inserts are unchanged. Full lib + soak suites pass in both modes.

### Disk-first aggregation/scan/sort fixes (server 0.29.1)

Profiling the 1M-doc benchmark in disk-first mode (`OXIDB_DISK_FIRST=1`) found
three issues, all rooted in the same point — neither documents nor the
index-only shortcut are resident, so each full scan re-materializes every doc
from the mmap and the index-iteration fast paths were disabled.

- **Index-only count `$group` restored on disk** (`pipeline::try_index_only_count`,
  `PagedFieldIndex::for_each_entry_asc`). A count-only group-by on an indexed
  field is near-instant in-RAM (reads posting-list sizes, zero doc reads) but
  disk-backed indexes bailed (`if fi.is_disk() { return None }`) to a full
  document scan. The fast path now iterates the mmap index via a
  backend-agnostic callback. Measured: count-only group-by on a 200K disk-first
  collection **1.56s → 86ms (~18×)**, matching the in-RAM path.

- **Index-backed sort fixed on disk** (`btree_collection::find_with_options`).
  `iter_asc`/`iter_desc` yield nothing for a disk-backed `PagedFieldIndex` (the
  in-RAM `entries` Vec is empty), so `sort + limit` on an indexed field
  **silently returned an empty result set** in disk-first mode. It now uses the
  `for_each_entry_asc`/`for_each_entry_desc` callbacks (which read the mmap),
  preserving early termination. Regression test:
  `disk_first_soak::disk_first_indexed_sort_and_count_group` (runs in both modes).

- **Zero-copy + sequential full-collection scan** (`Storage::for_each_payload`,
  used by `BTreeStorage::for_each_value`). The scan now locks the read mmap once
  and reads records in **data-file (offset) order** — a sequential,
  readahead-friendly sweep instead of random index-order page faults — and for
  records needing no decode (no encryption *and* not compressed) hands the
  callback a slice **borrowed straight from the mmap** (no per-record `Vec`
  allocation or memcpy). This benefits incompressible/large-doc and cold-cache
  workloads; for collections of small *compressible* documents the scan stays
  decompression-bound (every record carries a zstd frame, so the zero-copy path
  doesn't apply) — closing that gap (uncompressed `.bdat` or a decoded-bytes
  cache) is a tracked follow-up in ADR-0009.

### Automatic compaction trigger for disk-first storage (server 0.29.0)

Compaction no longer has to be invoked by hand. The periodic maintenance path
(`sync_writes`, right after `persist`) now calls `BTreeStorage::should_compact`
— a cheap check (one file-size read + one atomic load) that compacts when the
`.bdat` is both at least `OXIDB_COMPACT_MIN_BYTES` (default 4 MiB) **and** at
least `OXIDB_COMPACT_DEAD_RATIO` (default 0.5, i.e. ≥50%) dead, where
`dead_ratio = 1 − live_bytes/file_size`. That maintenance point doesn't hold the
data lock, so it can safely take compaction's exclusive write lock; a compaction
resets dead space, so the trigger self-rate-limits. Set `OXIDB_AUTO_COMPACT=0`
to disable. No-op in in-RAM mode. Field indexes are doc_id-keyed, so the store
rewrite leaves them valid — no re-index.

Soak-verified (`tests/disk_first_soak.rs::auto_compaction_bounds_file_size`,
`#[ignore]`d, timing-based): with the periodic sync thread running, 2400 updates
to 800 docs carrying an *incompressible* ~2 KiB payload settle the `.bdat` at
~2.8 MiB versus ~6.4 MiB uncompacted, with live count + point reads intact. Run:
`OXIDB_DISK_FIRST=1 cargo test --test disk_first_soak auto_compaction -- --ignored --nocapture`.

### Compaction for disk-first storage (server 0.28.28)

The disk-first store is append-only, so updates/deletes leave dead records that
grow the `.bdat` with the number of writes, not the live size (the soak suite
measured ~17× bloat under heavy update churn). `compact()` now reclaims it:
`BTreeStorage::compact` rewrites the data file keeping only live records and
atomically swaps it in. The `data` handle is an `RwLock<Arc<Storage>>` — normal
ops hold the read lock spanning their index-lookup + data-read (so a
`DocLocation` is never used against a swapped file), and compaction holds the
write lock, rebuilding + remapping under exclusivity. Field indexes are
doc_id-keyed (unaffected by the store rewrite); their `.mfidx` is rewritten
cleanly by the overlay-merge on persist.

Soak-verified (`tests/disk_first_soak.rs::compaction_reclaims_space_and_preserves_data`):
167 KB → 34 KB (~5×) after heavy update churn, with all live values + indexed
queries intact through compaction *and* a clean reopen. In-RAM `compact()` is
unchanged (persists the snapshot — no dead space to reclaim). (An automatic
dead-space trigger landed next — see 0.28.29 above.)

### Byte-level post-filter find — no Value materialization (server 0.28.27)

A `find` whose query an index couldn't satisfy materialized the entire result
as `Vec<Arc<Value>>` (~7× heavier than encoded), so a large unindexed result
(e.g. `verified=true` ≈ 250K matches) spiked the server into OOM territory. The
server now has a byte-level post-filter path: it filters each stored document
with `matches_raw_jsonb` — non-matches are **skipped with no decode** — and
transcodes matches JSONB→OxiWire directly into one buffer (no `Value`).

A 500K-doc A/B (query `verified=true`, 250K matches) shows it is both **lower
memory and faster** than the Value path: ~90 MiB encoded buffer vs ~875 MiB of
`Value`s, **321 ms vs 358 ms** (and the byte path's time *includes* encoding,
which the server adds on top of the Value path). Results are identical to the
Value path (parity test across eq/range/$in/compound/missing-field). Sort/skip/
limit still use the Value path (it owns ordering). This is the correct version
of the earlier reverted attempt, which regressed 2–11× by decoding every doc;
this one decodes only undecidable cases.

### Disk-first field indexes (opt-in, server 0.28.26)

Extends disk-first mode (`OXIDB_DISK_FIRST=1`) to single-field indexes: they're
now backed by the mmap'd `MmapFieldIndex` (`.mfidx`, paged in on demand, small
in-memory write overlay) instead of the fully-resident `PagedFieldIndex`.
`PagedFieldIndex` gained an additive `Option<MmapFieldIndex>` and delegates
every method to it in disk mode, so the query layer is unchanged; the
count-only `$group` fast paths (which use `iter_asc`) detect a disk-backed
index and fall back to the hashing path. On reopen the index is mmap-loaded
(instant), not rebuilt.

- **Memory:** a fresh process opening a 500K-doc collection with 5 indexes sits
  at **~7 MiB resident** — store *and* indexes are off the resident heap, so
  memory no longer scales with the dataset. (Index *build* still spikes
  transiently; a large query result still materializes — separate concerns.)
- **Correctness:** full core suite passes in both modes (in-RAM default
  unchanged at 799/799). See [ADR-0009](docs/decisions/0009-disk-first-storage.md).

### Disk-first storage mode (opt-in, server 0.28.25)

The default `BTreeStorage` keeps every document's bytes resident in RAM
(`scc::HashMap<u64, Vec<u8>>`), so memory scales with the dataset (~370 B/doc
of payload + overhead). A new **opt-in disk-first mode** (`OXIDB_DISK_FIRST=1`)
keeps only a compact `doc_id → DocLocation{offset,len}` index resident (~24
B/doc) and stores document bytes in an mmap'd append-only `{collection}.bdat`
file, read on demand — reusing the hardened `src/storage.rs` backend (mmap
reads, soft-delete, CRC, encryption, torn-tail recovery). See
[ADR-0009](docs/decisions/0009-disk-first-storage.md).

- **Memory:** a 1M-doc probe shows **RSS after insert: 161 MiB (disk-first) vs
  812 MiB (in-RAM)** — the resident store drops ~80% (162 vs 844 B/doc).
- **Writes** use un-synced/batched appends; durability comes from the WAL
  (fsynced per commit), with the data file flushed at checkpoint — so writes
  aren't serialized on a per-append fsync.
- **Correctness:** the full core suite passes in **both** modes (CRUD, queries,
  transactions, recovery, backup/restore, TTL, indexes). The in-RAM path is
  byte-identical — disk mode is an additive branch, so the default is
  unaffected.
- **Opt-in for now:** it's the core durability path, so it needs soak-testing
  (auto-compaction, lazy cursors, crash-injection) before it can become the
  default. ADR-0009 lists the follow-ups.

### Bound the in-memory caches by a fixed budget (server 0.28.21)

The per-collection document caches previously used entry-count defaults that
*scaled with the dataset*: the encoded-bytes cache defaulted to **1,000,000
entries** (enough to cache an entire 1M-doc collection, ~500–768 MiB) and the
deserialized-`Value` cache to **100,000 entries** (~400 MiB). On the 1M-doc
MongoDB benchmark these dominated RSS (OxiDB 1.71 GiB vs MongoDB's 0.5 GiB
hard-capped page cache).

Both caches are now bounded by a fixed **memory budget (~128 MiB each, ~256 MiB
total)** that does not grow with the dataset:

- `OXIDB_DOC_BYTES_CACHE_SIZE` default: 1,000,000 → **~175,000** entries.
- `OXIDB_DOC_CACHE_SIZE` default: 100,000 → **~32,000** entries.

These caches sit on top of the primary store, which already holds every
document resident in RAM, so a miss costs only a re-decode/transcode (CPU),
never I/O — making the bound safe. A 1M-doc in-process probe confirms the
`Value`-cache change alone cuts ~280 MiB of RSS; the bytes-cache ceiling drops
from ~500+ MiB to ~134 MiB. Both remain tunable via their env vars for
workloads that benefit from a larger hot set.

Also adds `OxiDb::memory_report(collection)` — an allocator-independent
resident-memory breakdown (primary store + indexes) computed from the live
structures, for introspection and capacity planning.

### Correct cross-shard aggregation in OxiPool (server 0.28.20)

OxiPool previously merged scatter-gather `aggregate` results by **concatenating**
each shard's output, which is only correct for per-document pipelines —
cross-shard `$group`/`$sort`/`$limit`/`$count` returned duplicated, unordered,
or over-limited results. Aggregations now split into a shard-local half and a
merge half (MongoDB-style), so results match a single-node run. See
[ADR-0008](docs/decisions/0008-cross-shard-aggregation-merge.md).

- **New `oxidb-agg-merge` crate** — `split_pipeline()` decomposes a pipeline
  into shard + merge halves. Handles `$sum`, `$count`, `$min`, `$max`, `$avg`
  group accumulators and `$sort`/`$limit`/`$skip`/`$count` blockers; `$avg`
  ships `{sum,count}` partials and finalizes `Σsum/Σcount` at the merge.
- **New `aggregate_docs` command** (+ `OxiDb::aggregate_docs`) — runs a
  pipeline over a supplied document array using the real executor, so the merge
  pass has identical semantics to single-node aggregation. Read-level RBAC.
- **OxiPool** runs the shard pipeline on every shard, concatenates the
  partials, and runs the merge pipeline on one shard via `aggregate_docs`.
  Per-document pipelines still concat directly.
- **Honest errors instead of wrong answers:** pipelines that can't be merged
  correctly across shards — `$push`/`$addToSet`/`$percentile`/`$first`/`$last`
  group accumulators, `$facet`/`$bucket`/`$sortByCount`/`$dateHistogram`,
  `$lookup` — now return a clear error suggesting a shard-key `$match` or a
  single-node run. (Previously these were silently wrong.)
- Tests prove split→shard→merge equals the single-node baseline across 1–5
  shards for sum/count/min/max/avg/mixed/sort-limit/count/passthrough.

### Code-review pass — security, correctness, and storage hardening (server 0.28.19)

A review across the server, query engine, and storage layers fixed 13 bugs
with regression tests, and scoped the one deferred item as ADR-0007.

**Security (REST/WS — only relevant when `OXIDB_HTTP_PORT`/`OXIDB_WS_PORT` +
`OXIDB_JWT_SECRET` are enabled):**

- Public `POST /api/auth/signup` no longer honors a client-supplied `admin`
  role — self-assigning `admin` is rejected (403).
- The JWT role is now **enforced** on every protected REST endpoint and on
  mutating WebSocket commands. Previously any valid token (even `read`) could
  drop collections, create stored procedures, or rewrite a collection's
  security rules. Mirrors the TCP RBAC default-deny posture.
- Cluster (async) dispatch now honors per-database role overrides
  (`effective_role`), matching the standalone path; a `grant_db_role`
  downgrade is no longer bypassed in cluster mode.

**Transactions:**

- Added a commit lock serializing the OCC validate→apply window, closing a
  concurrent lost-update race where two commits on the same document could
  both validate and both write.

**Query / update engine:**

- `$type:"double"` no longer matches integers; `"number"` is the numeric union.
- `$all: []` matches nothing (was: every array-valued document).
- `$substr` is code-point based — no more panic on multibyte UTF-8 input.
- `$mod` guards against a zero divisor and out-of-range float casts.
- `$inc`/`$mul`/`$min`/`$max` distinguish a present `null` from a missing
  field (e.g. `$mul` on `{"price": null}` errors instead of yielding `0`).
- `$project` keeps present-but-null nested fields instead of dropping them.
- `$set`/`$inc` on an out-of-bounds array index pad with `null`s (MongoDB
  semantics) instead of silently dropping the write.
- Index-only `$group` count falls back to the hashing path when a document is
  indexed under multiple keys, fixing a balanced miscount of the null group.

**Storage robustness:**

- WAL `Update` replay materializes orphan updates into storage/`primary_index`
  so field/composite indexes and the doc cache never reference a missing doc.
- zstd decompression buffer is clamped to a 1 GiB ceiling so a corrupt frame
  header can't drive an OOM allocation on read.
- Data-file scanners treat a torn final record as a clean end-of-data boundary
  instead of erroring or allocating from a bogus length.
- mmap read offsets use checked `u64` math (no 32-bit truncation/wrap).

**Deferred (see [ADR-0007](docs/decisions/0007-wal-commit-record-atomic-recovery.md)):**

- True crash-atomic transaction recovery needs an in-WAL commit record; the
  naive committed-set replay filter was reverted because it conflicts with the
  lazy-checkpoint + `remove_committed` design (it discarded committed data).
  Recovery currently favors durability over atomicity; ADR-0007 scopes the fix.

### SQL surface removed — OxiDB is a document database

The engine SQL surface (`oxidb::sql`, `SqlDialect`, `SqlResult`, `execute_sql`),
the PostgreSQL wire listener (`oxidb-server::pg_wire`), the JDBC driver
(`oxidb-jdbc/`), and the EF Core provider (`dotnet/OxiDb.EntityFrameworkCore/`)
have all been deleted. The `cmd: "sql"` over TCP, REST `/sql`, and WebSocket
`sql` endpoints now return errors. NuGet `OxiDb.EntityFrameworkCore` is
deprecated; use `OxiDb.Linq` for typed query syntax. SQL benchmark/comparison
directories (`tests/comparison-sqlite`, `tests/comparison-efcore`,
`tests/efcore-*`, `examples/bench_vs_sqlite.rs`) removed alongside.

## v0.28.18

### .NET clients — developer-friendly rework

Three published NuGet packages at v0.28.18 (`OxiDb.Client.Tcp`,
`OxiDb.Client.Embedded`, and **NEW: `OxiDb.Linq`** — previously
source-only).

New on the .NET TCP client (all backward-compatible):

- **Exception hierarchy:** `OxiDbException` base with subclasses for
  `DuplicateKey`, `TransactionConflict`, `Authentication`, `NotFound`,
  `Immutable` (WORM), `Connection`, `Protocol`. Server error strings
  routed to the right subclass via `FromServerMessage`.
  `OxiDbTcpException` retained as `[Obsolete]` alias (removed in 2.0).
- **`HelloAsync` + `HelloResponse` record** — wire-protocol HELLO
  surfaced to .NET consumers, returns server version, supported wire
  versions, stable + experimental feature sets, auth methods.
- **Typed CRUD:** `FindAsync<T>`, `FindOneAsync<T>`,
  `InsertReturningIdAsync` (`long`), `InsertManyReturningIdsAsync`
  (`long[]`). Eliminates the `JsonElement`→parse dance.
- **`IAsyncEnumerable<T>` streaming** — `StreamAsync<T>` over
  paginated LIMIT/SKIP batches for million-row result sets.
- **DI integration:** `services.AddOxiDbTcp(opts => opts.Host(...))`.
- **Type-safe query builder:** `Query.Eq/Gte/Lt/In/And/Or/Range/...`
  for runtime-constructed queries.

### `$or` + dot-paths in the partial-JSONB matcher

Final two MongoDB-winning tests in `tests/comparison-mongodb` (1M docs)
flip with this. `matches_query_partial_jsonb`:

- **`Query::Or`** — short-circuits on the first `Some(true)`; returns
  `Some(false)` when all definite subs miss; returns `None` when any
  sub is undecidable so the caller can fall back to full decode.
- **Dot-paths** — new `extract_field_path` walks the first segment via
  `codec::extract_field` (JSONB-bytes fast path) then chains `Value`
  navigation for nested segments. `address.zip` and friends now match
  partially.

Bench (vs v0.28.17):
- `$or city=Tokyo OR Paris`:   3.16s → 1.48s (−53%; flips to OxiDB 2.1× win)
- Nested `address.zip` range: 2.71s → 1.47s (−46%; flips to OxiDB 1.3× win)

Win count: **OxiDB 24 / MongoDB 0** at 1M docs.

### Versions

- `oxidb-server`: 0.28.17 → 0.28.18

## v0.28.17

### Partial-JSONB pre-filter on the find full-scan path

`find_with_options_arcs`'s no-index branch used to call `load_doc_arc`
on every doc in the collection — a full JSONB → `serde_json::Value`
decode at ~20 µs/doc just to evaluate the filter on the rejected
majority. The rayon `par_iter` (and wasm sequential) loop now routes
through `matches_query_partial_jsonb` first: ~5 µs partial extract on
raw JSONB bytes, with the full decode reserved for the docs that pass.

Same closure logic as v0.28.16's aggregate_streaming path, reused via
a local `filter_one` closure: `Some(false)` → skip,
`Some(true)` → materialise Arc (no redundant `matches_value` re-check),
`None` → full decode + `matches_value`.

Bench (vs v0.28.16, 1M docs, full-scan queries):
- Compound (dept=Sales AND status=active):  3.25s → 515 ms (−84%; FLIP)
- `$in` country:                            4.48s → 2.21s (−51%; FLIP)
- Exact match dept=Engineering:             4.31s → 2.18s (−49%; FLIP)
- Range (salary 50K-100K):                  3.63s → 2.06s (−43%)
- Range (age ≥ 50):                         6.62s → 4.37s (−34%)
- Boolean (verified=true):                  4.92s → 3.72s (−24%)

### Versions

- `oxidb-server`: 0.28.16 → 0.28.17

## v0.28.16

### Partial-JSONB post-filter in `aggregate_streaming`

When the leading `$match` of an aggregation pipeline can't be fully
satisfied by an index (e.g. `department` has a field index but
`status` doesn't), the streaming path historically called
`load_doc_arc` on every over-approximation candidate to run the
residual filter — a full JSONB → `serde_json::Value` decode at
~20 µs/doc.

Now the slow path tries a partial-JSONB matcher first:
```rust
matches_query_partial_jsonb(query, raw_jsonb_bytes) -> Option<bool>
```
Handles top-level `$eq`, `$ne`, `$gt`/`$gte`/`$lt`/`$lte`, `$in` with
`codec::extract_field` (single-key extract straight off JSONB, no
full decode). Anything outside that set (dot-paths, `$regex`,
`$elemMatch`, `$expr`, `$or`, `$nor`, `$not`, ...) returns `None` and
the caller falls back to full decode.

Bench (vs v0.28.15, 1M docs, `Match + Group` active engineers):
- OxiDB:    1.58 s → 213 ms   (−86%; 7.4× faster)
- Verdict:  4.9× MongoDB win → 1.6× OxiDB win (FLIP).

### Versions

- `oxidb-server`: 0.28.15 → 0.28.16

## v0.28.15

### Bytes-first composite-index path

Extends `find_oxiwire_bytes` to recognise queries that are exactly
covered by a composite index's fields, and routes them through
`CompositeIndex::find_prefix` for direct ID resolution. The previous
single-field-only path missed this case: a multi-field equality query
fell through to the Value-based fallback even when a composite index
could satisfy it natively.

New helper `try_composite_lookup`:
- Verifies query is pure AND-of-`$eq` via `extract_eq_conditions` +
  `is_eq_only_on` (no `$gt`/`$in`/regex/etc.).
- For each registered composite index, checks that its `fields`
  exactly cover the query's eq-field set (order-independent).
- Builds the prefix vec in index field order and calls `find_prefix`.

Bench (vs v0.28.14, composite-indexed `dept=Sales AND status=active`):
- OxiDB:   2.07 s → 157 ms   (−92%; 13× faster)
- Verdict: 2.8× MongoDB win → 4.1× OxiDB win (FLIP).

### Versions

- `oxidb-server`: 0.28.14 → 0.28.15

## v0.28.14

### Bytes-first find path for OxiWire responses

Closes the JSONB → `Value` materialisation bottleneck on the find →
wire pipeline. For queries fully satisfied by an index (no post-filter,
no sort/skip/limit), the engine now streams pre-encoded OxiWire bytes
directly from storage instead of materializing a `serde_json::Value`
tree per row.

New modules:
- `src/jsonb_oxiwire.rs`: direct JSONB → OxiWire byte converter built
  on a custom serde Visitor + DeserializeSeed. Walks JSONB once and
  emits OxiWire tags inline; never constructs Value. 7 roundtrip tests.
- `src/doc_bytes_cache.rs`: per-collection sharded LRU of `Arc<[u8]>`;
  env-tunable capacity (`OXIDB_DOC_BYTES_CACHE_SIZE`, default 1M).
- `src/wire_oxiwire.rs`: engine-local copy of the OxiWire encoder
  (byte-compatible with `oxidb-server/src/oxiwire.rs`) for the
  `doc_cache.peek` warm path that re-encodes from Value.

`BTreeCollection::load_doc_oxiwire_bytes` — 3-tier lookup:
- `bytes_cache` hit       → return cached `Arc<[u8]>`
- `doc_cache.peek` hit    → encode Value, cache, return
- cold                    → pread JSONB, jsonb→oxiwire, cache, return

`BTreeCollection::find_oxiwire_bytes` — returns `Some(Ok(_))` only when
`query::is_fully_indexed` is true AND `execute_indexed` yields a
candidate ID set; returns `None` to fall through to the Value path.
Bytes-cache invalidation hooks added at every `doc_cache` mutation site.

`doc_cache::DEFAULT_CAPACITY` const → env-tunable `default_capacity()`
function (`OXIDB_DOC_CACHE_SIZE`). Const kept as a legacy alias.

Bench (vs v0.28.13, 1M docs):
- Indexed: dept=Engineering   2.60s → 1.18s   (−55%)
- Indexed: age ≥ 60           3.87s → 1.52s   (−61%)
- Indexed: city=Tokyo         0.99s → 0.49s   (−50%)
- Indexed: salary range       2.67s → 1.06s   (−61%)
- Range query 10K windows  128.91s → 31.26s   (−76%, flips: now beats Mongo)

Net win count: OxiDB 17 / MongoDB 7 (was 16/8).

### Versions

- `oxidb-server`: 0.28.13 → 0.28.14

## v0.28.13

### 1.0 prep: Phases 2 + 3 + 5 + Phase 4 carryover

A focused tranche of 1.0 prep landings — does not change the engine
surface, adds versioning + discovery hooks across the wire protocols
and the client SDK story.

**Phase 2 — Wire handshake** ([ADR-0003](docs/decisions/0003-1.0-stability-scope.md)):
- `oxidb-server/src/hello.rs` (new): OxiWire `HELLO` handler. `cmd: "hello"`
  returns server-info + features + auth methods, picks the highest
  mutually-supported wire version, pre-auth + idempotent.
- `session.rs`: `wire_version` field.
- `async_server.rs`: dispatch `HELLO` before the auth check.
- `rest/mod.rs`: `/v1/` URL prefix + `GET /v1/hello` discovery endpoint;
  legacy bare paths still route during the deprecation window.
- `ws.rs`: RFC 6455 `Sec-WebSocket-Protocol: oxidb.v1` negotiation;
  clients without the header still connect (backward-compat).
- [`docs/format/compat-matrix.md`](docs/format/compat-matrix.md):
  cross-version compat matrix + per-protocol negotiation rules.

**Phase 3 — Client SDK freeze** (scaffold; Python as the template):
- `python/scripts/generate_api_snapshot.py`: `inspect`-based introspection.
- `python/scripts/check_api_snapshot.py`: CI gate; unified-diff on mismatch.
- `python/api/v1.json`: ~68 public symbols captured (1008 lines).
- [`docs/PHASE3-SDK-FREEZE.md`](docs/PHASE3-SDK-FREEZE.md): pattern doc
  + per-client introspection-mechanism table for the remaining 9
  Tier-A clients.

**Phase 4 — `oxidb migrate` CLI scaffold**:
- `oxidb-cli/src/migrate.rs` (new, ~340 LOC) — magic-byte sniffer for
  OXWA/OXTX/OXBT/OXIX + blob `.meta` JSON. Walks data dir, classifies
  each file as `Current(v)` / `Older(v)` / `Newer(v)` / `Legacy` /
  `Unreadable`. `run()` validates, refuses on `Newer`, errors on
  `Older` (no v1→v2 paths registered yet).
- New subcommands: `oxidb migrate inspect --data <PATH>` and
  `oxidb migrate run --data <PATH>` (with `--dry-run`, `--no-backup`,
  `--in-place`, `--out` flags).
- Existing REPL/eval entrypoint preserved via clap's optional-subcommand
  pattern.

**Phase 5 — Policy docs** (operationalises [ADR-0004](docs/decisions/0004-phase-0-answers.md)):
- [`docs/SEMVER.md`](docs/SEMVER.md): patch/minor/major rules + the
  additive vs breaking change list.
- [`docs/STABILITY.md`](docs/STABILITY.md): the 1.0 stable surface +
  experimental subsystems + Tier-A/B clients + 5-criterion promotion
  bar.
- [`docs/DEPRECATION.md`](docs/DEPRECATION.md): notice-period table +
  4-announcement requirements + reserved-name special case.
- [`docs/SECURITY.md`](docs/SECURITY.md): GitHub Security Advisories
  channel + 3-day ack / 90-day disclosure + supported-versions
  backport matrix.
- `docs/README.md` indexed all four under "1.0 release policy".

### Versions

- `oxidb-server`: 0.28.12 → 0.28.13

<!-- Gap: v0.25.27 → v0.28.12 entries are not in this file. Major work
     in that range: PRs #42–#76 (CERN-grade testing program, 9 fuzz
     targets, 7 DoS bugs found+fixed, audit rotation by size/age/
     calendar-aligned UTC/gzip). Captured in the GitHub release notes
     for v0.28.12 (auto-generated). -->

## v0.25.26

### `find_and_modify` — atomic single-document read-modify-write

`update` + `$inc` was never safe for counters. `update` finds matching
documents under a read lock, releases it, applies the operators, then
writes under a write lock — a read-modify-write with a gap — so two
concurrent `$inc` calls on the same document both read the old value
and one increment is lost. And `update` only ever returns a count,
never the resulting value.

- **`BTreeCollection::find_and_modify` / `OxiDb::find_and_modify`** —
  finds one document, applies the update operators, writes it back, and
  returns the *modified* document, all while the collection's index
  write locks are held. It is therefore atomic against any other
  `find_and_modify` (and against `update`'s write phase) on that
  collection — the safe primitive for counters such as a mailbox's
  IMAP `UIDNEXT`.
- Server command `find_and_modify` (ReadWrite role) returns the modified
  document, not a count; `Client.FindAndModify` added to the Go client.
- A concurrency test fires 8 writers × 200 `$inc` at one counter
  document and asserts no increment is lost and every returned value is
  distinct (1..=1600).

### Versions

- `oxidb-server`: 0.25.25 → 0.25.26

## v0.25.25

### Relicensed — AGPL-3.0 + commercial (dual-license)

OxiDB moves from `MIT OR Apache-2.0` to a **dual license**: the public,
open-source license is now **AGPL-3.0-only** (see [`LICENSE`](LICENSE)),
and a separate **commercial license** is available for closed-source /
proprietary use that the AGPL's copyleft does not permit — see
[`COMMERCIAL-LICENSE.md`](COMMERCIAL-LICENSE.md).

- `LICENSE-MIT` / `LICENSE-APACHE` removed; `LICENSE` is now the full
  AGPL-3.0 text. `license = "AGPL-3.0-only"` across every workspace
  crate's `Cargo.toml`.
- Prior releases remain under `MIT OR Apache-2.0` — that grant on those
  specific versions cannot be revoked. This and every future version is
  AGPL-3.0 / commercial.
- Contributions are accepted under **both** licenses, so the whole of
  OxiDB — contributed code included — can still be offered commercially.

### Versions

- `oxidb-server`: 0.25.24 → 0.25.25

## v0.25.24

### Blob store — opt-in durable writes with group-committed fsync

The blob store acknowledged a `put_object` as soon as the temp files
were renamed into place, leaving durability to a 1 Hz background fsync
thread — so a successful put could still be lost to a power cut for up
to a second. The document WAL has a real 3-fsync protocol; the blob
store, where large payloads (e.g. mail bodies behind the 16 MiB wire
cap) actually land, did not. This adds an opt-in durable path.

- **`OXIDB_BLOB_SYNC` / `BlobStore::with_sync_writes`** (`src/blob.rs`)
  — when set, `put_object` fsyncs the payload and meta temp files, then
  fsyncs the bucket directory between the `.data` and `.meta` renames
  and again after. The ordering is load-bearing: `scan_bucket` treats
  `.meta` as the source of truth, so a `.meta` made durable ahead of
  its `.data` would be a ghost read on recovery. The second dir fsync
  is the commit point — once it returns the put is durable and a
  caller (e.g. an SMTP server) may treat it as committed.

- **Durable `delete_object`** (`src/blob.rs`) — symmetric with `put`:
  fsyncs the bucket directory after unlinking the `.meta`, so an ack'd
  delete cannot be resurrected by a crash. In durable mode a real I/O
  error from the unlink is now propagated instead of swallowed;
  non-durable mode keeps its prior best-effort behavior.

- **Group-commit directory syncer** (`src/blob.rs`) — a naive per-put
  implementation would fsync the bucket dir twice per put, fully
  serial. `DirSyncer` mirrors the `tx_log` committer: a dedicated
  thread owns the fsync, callers `rename` then enqueue and block, and
  the thread coalesces every request waiting in its queue into one
  `fsync` per distinct directory (`MAX_BATCH = 512`). Because a caller
  always renames before it enqueues, any queued request corresponds to
  a completed rename — so N concurrent durable puts to one bucket cost
  ~2 directory fsyncs, not 4N. Single-put latency is unchanged (a
  channel round-trip is ~µs against an ~ms fsync).

- **Background `blob-sync` thread fix** (`src/blob.rs`) — the periodic
  fsync thread fsynced the `_blobs` root, but object renames/unlinks
  happen one level down in the per-bucket directories, and fsyncing a
  parent does not flush its children. It now enumerates the bucket
  dirs each tick and fsyncs every one. This is the best-effort path
  for deployments that leave `OXIDB_BLOB_SYNC` off.

- **Crash-consistency harness** (`tests/blob-crash-recovery-go/`) — a
  Go harness in the `crash-recovery-go` mould: put N objects, delete a
  subset, churn a second bucket so the SIGKILL lands mid-`put_object`,
  cold-boot the same data dir, and assert every ack'd put survived
  intact, every ack'd delete held, no stray `.tmp` files remain, and
  every listed object is readable (no ghost `.meta`). Scope is stated
  honestly in the file header: SIGKILL does not drop the page cache,
  so this proves crash *consistency*, not fsync *durability* — the
  latter needs block-layer fault injection (dm-log-writes), tracked
  separately. 3 new `blob::tests` unit tests cover the sync and
  group-commit paths plus reopen (9 → 12).

### Versions

- `oxidb-server`: 0.25.23 → 0.25.24

## v0.25.23

### Point-In-Time Recovery

OxiDB had only full-snapshot `backup`/`restore` — no way to recover to an
arbitrary moment (e.g. "just before the bad bulk delete"). The hard part:
OxiDB has **no global write ordering** — each collection's `.wal` is an
independent byte stream, `_tx_commit_log` is an unordered set, and most
writes carry `tx_id = 0`. PITR introduces that ordering and the machinery
to archive and replay against it. Opt-in via `OXIDB_PITR`; **zero cost
when off** — every record stays byte-identical v1, no extra threads.

- **WAL v2 record format** (`src/wal.rs`) — records gain v2 op-types
  (high bit) carrying an optional `[gsn][wall_clock_micros]` header.
  `read_entries` replays mixed v1/v2 files; v1 is still emitted unless a
  sequencer is attached.

- **Archive sequencer** (`src/pitr.rs`) — `ArchiveSequencer` hands every
  durable WAL write a global, monotonic, wall-clock-stamped GSN. It
  survives restarts via a leased `_gsn` file — one fsync per 10k GSNs,
  never reusing a number. GSN allocation happens **under the WAL lock**,
  so "the counter passed N" implies "N's record is in the file" — the
  invariant the base-backup watermark relies on.

- **WAL segment rotation** (`src/wal.rs`) — `Wal::seal()` atomically
  renames the live WAL to a numbered sealed segment and opens a fresh
  one, entirely under the WAL lock so it is atomic against every
  concurrent `log*` (closing the documented "lost acks across
  truncation" race). `log*` auto-seals past `OXIDB_WAL_SEGMENT_BYTES`
  (default 16 MiB). `replay_wal` now replays sealed segments + the live
  WAL, so a rotated WAL still recovers every acknowledged write. A
  6-writer-plus-sealer concurrency stress asserts zero loss/dup.

- **Archiver** (`src/archive.rs`) — a background `oxidb-archiver` thread
  copies sealed segments into `OXIDB_ARCHIVE_DIR/segments/*.seg` —
  verbatim WAL bytes (at-rest encryption preserved) plus a trailer with
  the GSN/time range + CRC. Crash-safe (`tmp → fsync → rename →
  fsync-dir`), idempotent (a segment is archived iff `<name>.seg`
  exists), with a `manifest.json` rebuilt from the `.seg` trailers so a
  torn manifest self-heals. Best-effort — sealed segments are immutable,
  read with no locking, never blocking a foreground write.

- **Base-backup watermark** (`src/engine.rs`, `src/pitr.rs`) — `backup()`
  reads the GSN counter, barriers every collection's WAL, and embeds a
  `base.meta` watermark in the tarball; the base is then guaranteed to
  contain every write below it.

- **`restore_to_point`** (`src/engine.rs`, `src/archive.rs`) — extracts a
  base backup, then `replay_into` advances it to a `Gsn` / `Timestamp` /
  `Latest` target: gathers every WAL record per collection, resolves the
  target GSN, applies a **two-pass transactionally-consistent cut** (a
  transaction is admitted only if its whole footprint — `max(gsn)` over
  all its records, across collections — fits under the target; one
  straddling the cut is excluded whole, never half-applied), rewrites
  each WAL with exactly the admitted records, and drops the stale index
  caches / FTS index / tx commit log. Idempotent and offline.

- **Server + retention** (`oxidb-server/src/handler.rs`,
  `src/archive.rs`) — admin commands `restore_to_point` and
  `archive_status`. `OXIDB_ARCHIVE_RETENTION_HOURS` prunes archived
  segments older than the window (`0` = disabled).

v1 limitations: blob objects restore to the base-backup point only (the
document set restores to the target); the FTS index is dropped and must
be rebuilt; create/drop-index DDL between the base and the target is not
replayed. SIGKILL preserves the page cache, so the included tests prove
crash *consistency* and idempotency, not fsync durability under power
loss — block-layer fault injection is tracked separately.

- 25 new unit tests across `wal`, `pitr`, and `archive`; the full lib
  suite is green except `wal_checkpoint_clears_wal` and
  `restore_from_backup`, which already fail on `master`.

### Versions

- `oxidb-server`: 0.25.22 → 0.25.23

## v0.25.22

### Engine — group-commit tx_log + lazy DocCache shards

- **Group commit on the transaction log** (`src/tx_log.rs`) —
  `mark_committed` was a global `Mutex<File>` + unconditional
  `sync_data()` per commit. Every transaction on every collection
  serialised through that single fsync, capping throughput at
  ~85 doc/s on a single-tenant DMS upload workload regardless of
  `OXIDB_LAZY_SYNC`. Moved the file behind a dedicated
  `oxidb-tx-commit` thread that owns an in-memory
  `HashSet<TransactionId>` + the file handle. Callers submit
  `Cmd::Mark / Remove / Clear / Read` over `mpsc` and block on a
  per-call `sync_channel(1)` reply. The committer drains the queue
  non-blockingly up to **`MAX_BATCH = 512`**, applies all mutations
  against the in-memory set, and emits **one** `persist + sync_data`
  per batch. Notifies every waiter only after the fsync, so
  durability semantics are unchanged. Reads are deferred until after
  the same batch's fsync — recovery invariant preserved.
  - File format unchanged: still a sequence of `[tx_id: u64 LE]`.
    Old logs are parsed once at `open()` into the in-memory set;
    the file is rewritten in full (sorted ids) at each batch
    boundary.
  - Bench (dms-bench, lazy_sync, Turkish FTS, real PDF + metadata
    uploads, pool=128):
    - single-tenant: **85 → 850 doc/s** @ 256 workers (~10×)
    - multitenant 10t × 10w: **73 → 527 doc/s** (~7×)
  - At 800+ doc/s the new bottleneck is per-collection
    `btree_storage::persist_mu` — expected, out of scope.
  - 9/9 tx_log unit tests pass, plus a new
    `concurrent_marks_all_durable` that fires 64 threads × 32 marks
    and verifies the post-run set holds all 2048 ids and survives
    close + reopen.

- **Lazy-allocate DocCache shards** (`src/doc_cache.rs`) — each
  `BTreeCollection` eagerly built a 16-shard LRU sized for 100K
  entries on creation. At scale that compounded to ~400 KiB of
  preallocated hashtable buckets per collection regardless of use:
  at 10K collections the dms-bench scale-load test hit **3.9 GiB
  RSS** with **161K anonymous mmap regions**. Switched the shard
  slots to `Mutex<Option<LruCache<…>>>` with a stored per-shard cap
  target (`AtomicUsize`). First `put()` to a shard materialises the
  inner `LruCache` at the cap; `clear()` drops it back to `None` to
  reclaim. Capacity ceiling unchanged — active collections still
  grow to the full 100K slots.
  - 10K populated collections: **3.9 GiB → 269 MiB** RSS (−93%),
    **161,348 → 6,316** anonymous mmap regions (−96%), insert p99
    unchanged.
  - 9/9 doc_cache unit tests pass, including a new
    `lazy_alloc_until_first_put` assertion.

### Combined effect

Unlocks the **collection-prefix multi-tenancy** primitive started
in v0.25.19 (`create_collection` 1260× speedup): with both
parallel-create AND the new memory + commit-throughput patches,
10K-collection deployments are now realistic — RSS, fsync
contention, and create-cost all in their right place.

### Versions

- `oxidb-server`: 0.25.21 → 0.25.22

## v0.25.21

### Decode — 5× faster cold-path wire response

- **`codec::decode_doc_to_text`** (`src/codec.rs`) — JSONB → JSON text via `RawJsonb::to_string`, skipping the `serde_json::Value` intermediate. One walk, no allocated Value tree. Legacy JSON-text bytes pass through unchanged.
- **`BTreeCollection::load_doc_text`** (`src/btree_collection.rs`) — cache-hit serializes the cached `Arc<Value>` (unchanged hot path); cache-miss reads raw bytes and calls `decode_doc_to_text` directly. Does NOT populate the doc cache on miss — decoding to Value just to cache it would undo the speedup, and the wire payload by itself doesn't carry the structured Value that filter paths need.
- Driven by measurement (`examples/measure_cache_hitrate.rs`): Zipfian sweep showed DocCache hit ratios of 60-92% for typical OLTP-shaped workloads (Zipf s ~ 1.0-1.2, 10-100× cache cap) and 12-35% for low-skew / big-working-set configs. Cold path is mixed but the absolute hit is biggest there.

  | Doc shape | Cold A (now) | Cold B (this) | Speedup | Hot A | Hot B |
  |---|---:|---:|---:|---:|---:|
  | small (4 fields) | 1 503 ns | 305 ns | 4.93× | 131 | 122 |
  | medium nested | 2 110 ns | 435 ns | 4.85× | 248 | 254 |
  | LARGE (50 events) | 31 001 ns | 5 752 ns | 5.39× | 4 001 | 3 875 |

- Next step (separate change): wire the server's find/find_one response handler to call `load_doc_text`. The engine-level building block is the prerequisite.

### Observability — DocCache hit/miss counters

- `DocCache` now tracks cumulative hits and misses (atomic Relaxed; cheap). `DocCache::stats()` returns a `CacheStats { hits, misses, hit_ratio() }` snapshot; `reset_stats()` clears the window.
- Exposed on the collection via `BTreeCollection::doc_cache_stats` / `doc_cache_stats_reset` / `doc_cache_clear`.

### Benches

- **`examples/measure_cache_hitrate.rs`** — Zipfian sweep that produced the hit-ratio data above.
- **`examples/profile_decode_wire.rs`** — micro-bench isolating the find→wire decode step (JSONB → Value → text vs `RawJsonb::to_string`).
- **`examples/profile_load_doc_text.rs`** — end-to-end load on a populated `BTreeCollection`, verifies the patch doesn't regress the hot path.

### Versions

- `oxidb-server`: 0.25.20 → 0.25.21

## v0.25.20

### Codec — 3-4× faster JSONB encode, ~40% smaller on-disk image

- **`codec::encode_doc` rewrite** (`src/codec.rs`) — route `Value` → `serde_json::to_writer` → `jsonb::parse_owned_jsonb_standard_mode` instead of `jsonb::to_owned_jsonb` (which uses the serde Serialize path).
  - The serde encoder in `jsonb` 0.5 allocates a fresh `Serializer` (with its own `Vec<u8>`) for every map value, materializes each child as an intermediate `OwnedJsonb`, then concatenates them in `ObjectBuilder::build → to_vec → buffer.append`. That was the dominant per-encode cost.
  - It also wrapped every scalar inside a container with a 4-byte `SCALAR_CONTAINER_TAG`, inflating the on-disk image by 30-50%.
  - The new path produces output bytes that are still a valid `OwnedJsonb` and decode through the same `jsonb::from_raw_jsonb` / `RawJsonb::get_by_*` calls. Legacy fat-format images from older writers continue to round-trip — `decode_doc` is unchanged.
- **Bench results** (median of 5, `--release`, M2):

  | Doc shape | Before | After | Speedup | Bytes before / after |
  |---|---:|---:|---:|---:|
  | flat scalars (5 fields) | 1 177 ns | 259 ns | 4.5× | 198 B / 83 B |
  | nested objects (2-level) | 2 913 ns | 742 ns | 3.9× | 474 B / 240 B |
  | array of 50 small objects | 48 348 ns | 12 336 ns | 3.9× | 8 682 B / 4 575 B |
  | LARGE realistic doc | 61 256 ns | 20 092 ns | 3.0× | 13 571 B / 9 306 B |

- **End-to-end smoke** — existing `wal_checkpoint` test logs the WAL size after 20 inserts: **3 821 B → 1 459 B (-62%)** with no change other than the encoder. Disk space, memory residency, and WAL replay cost all benefit.
- Standard mode is used (not extended) because `serde_json::to_writer` always emits strict JSON — no NaN/Infinity, no leading plus signs, no empty array elements to accommodate.

### Benches

- **`examples/profile_decode.rs`** — decomposes the decode hot path (full decode vs partial extract vs custom IndexValue extractor) so we can spot regressions on the next `jsonb` bump. Findings: partial extract is doc-size-independent (~90 ns); writing a custom IndexValue deserializer that bypasses serde turned out to be marginally *slower* — `get_by_keypath` is the dominant cost, not the scalar dispatch.
- **`examples/profile_encode.rs`** — decomposes encode by document shape; the basis for the `encode_doc` rewrite. Both run in a few seconds with `cargo run --release --example <name>`; no `criterion` / external harness.

### Versions

- `oxidb-server`: 0.25.19 → 0.25.20

## v0.25.19

### Engine — ACID hardening

- **Durability: "ack means on disk"** (`src/btree_storage.rs`, `src/btree_collection.rs`, `src/wal.rs`)
  - `persist` switched from non-atomic `fs::write` to tmp + `sync_data` + atomic rename + parent fsync; per-collection mutex prevents concurrent commits from trampling a shared `{name}.btree.tmp`
  - `OXIDB_LAZY_SYNC` default flipped from `true` → `false`; strict mode fsyncs every commit. Lazy mode still available opt-in.
  - `set_lazy_sync` wired through every write path (single + batch insert, update, delete, sync_writes) so the env flag actually selects fsync vs no_sync
  - tx-commit `log_wal_batch` on btree no longer a no-op — durability via WAL fsync at commit, not a synchronous full-file persist
  - `sync_writes` persists without truncating WAL (truncate was racy with concurrent writers and lost ~3/2000 acks under load); WAL truncation moved to a new `final_checkpoint` that runs only at shutdown
  - New `enable_periodic_snapshot` (strict mode default 1s cadence) so WAL doesn't grow unbounded between commits
- **Boot tolerance** — `btree_storage::open` tolerates partial / truncated images and leaves recovery to WAL replay instead of refusing to boot
- **Graceful shutdown** — `oxidb-server` SIGTERM/SIGINT handler flushes engine via `OxiDb::shutdown` then `process::exit` cleanly; SIGPIPE ignored
- **Multi-collection atomicity** (`src/transaction.rs`)
  - `tx_insert` reserves the doc id at buffering time and returns it to the client, so callers can wire the assigned id into sibling writes inside the same transaction
  - `WriteOp::Insert` carries the pre-allocated id; `prepare_tx_insert` consumes it instead of double-allocating
  - Wire-protocol `insert`/`insert_many` in tx mode now return `{"id": N}` matching the non-tx response shape (was `"buffered"` with no id)
  - `find_one` routes through `tx_find` when in a transaction so the read version is recorded for OCC validation — without this a read-then-write inside a tx would skip the conflict check
- **Crash-test harnesses**
  - `tests/crash-recovery-go`: 2000 inserts → SIGKILL → restart, every ack'd write must survive; no `.btree.tmp` leftovers; payload spot-check
  - `tests/atomicity-go`: 3 scenarios — pre-commit SIGKILL, post-commit SIGKILL, mid-tx SIGTERM (graceful) — across two collections with foreign-key linkage, asserting all-or-nothing recovery
- **Legacy `.dat` data-loss guard** — `OxiDb::open_internal` scans the data dir at startup and refuses to open non-empty `.dat` collection files without a matching `.btree`. Without this, upgrading a pre-BTree binary in place silently shadowed real records with empty collections. `OXIDB_ALLOW_LEGACY_DAT=1` keeps the old behavior for callers that explicitly accept the loss.

### Engine — concurrency

- **Parallel `create_collection`** (`src/engine.rs`) — hold the global write lock only to insert into the collections map, not while opening the `BTreeCollection` from disk. Mirrors `get_or_create_collection`: read-check, lock-free open, then short write lock with a race-loser re-check.
  - Bench (1000 collections, 8 workers): Phase 1 wall **4m18s → 205ms**, `CreateCollection` p99 **6.25s → 1ms** (~1260× speedup). At 10K collections: p99=11ms, RSS=1.24 GiB — viable as a per-tenant collection-prefix multi-tenancy primitive.
- **Lock-free blob `put` rename** (`src/blob.rs`) — `fs::rename` no longer held under the bucket write lock; split into: brief lock for id allocation, lock-free renames, brief lock for hashmap commit. Same-key races are safe for content-addressed callers (identical bytes → identical result). Brings 32-way concurrent put p50 down from ~900ms.

### Full-text search

- **Parallel indexing worker pool + introspection** (`src/fts.rs`)
  - `FtsRuntime` tracks queue depth, per-worker in-flight job, and a ring of recently completed/failed/skipped jobs
  - Engine: `bucket_fts_size` accessor (powers per-tenant FTS quota accounting); `fts_status` returns the runtime snapshot as JSON
  - Server: new `fts_status`, `bucket_fts_size`, `proc_status` commands; admin + reader RBAC roles get `fts_status` + `proc_status`

### S3

- **`aws-chunked` request body decoding** (`oxidb-server/src/s3/`) — AWS CLI / boto3 send streaming PUT bodies with `content-encoding: aws-chunked` or `x-amz-content-sha256: STREAMING-*`. Strip the chunk-size framing back to the original payload in both single PUT and multipart upload paths. Tolerates missing trailers and partial reads.

### Blobs

- **Skip zstd for already-compressed mime types** (`src/blob.rs`) — re-compressing image/video/audio buys ~nothing while costing CPU on every Put and Get. Detect the content-type prefix and store raw bytes when compression is futile; decode path stays forward-compatible with legacy zstd-stored blobs.

### OCR

- **Image crate + dockerized tesseract toolchain** — `ocr` feature now pulls `image` (png/jpeg/tiff/bmp/gif/webp) so the pipeline can decode the source file before handing pixels to leptess. Dockerfile installs `libtesseract-dev`/`libleptonica-dev` + libclang for leptess bindgen, plus runtime libs and `eng`/`tur` traineddata in the slim image. Build uses `--features cluster,ocr`.

### Process metrics

- **macOS dev-box `proc_status`** (`oxidb-server/src/proc_stats.rs`) — `getrusage(RUSAGE_SELF)`-backed `read_cpu_ticks` + `read_vm_rss_kb` so dashboards report real CPU% / RSS on Darwin. Linux prod path (`/proc/self/{stat,status}`) unchanged.
- **Real macOS thread count via Mach `task_threads`** — replaces the placeholder `0` with the live thread count (matches Activity Monitor); releases per-thread send rights and the returned array to avoid port-name leaks under the 5s admin probe.

### Go client

- **`Client.ProcStatus()` / `Client.FtsStatus()`** — typed wrappers so callers don't assemble raw `{"cmd": ...}` maps. `BucketFTSSize` added.
- **Bounded ping check on pooled connection checkout** (`go/oxidb/pool.go`) — without a deadline, `Ping` on a server-reaped TCP conn that didn't get a clean FIN could hang for the OS keepalive interval (~2h on macOS/Linux). 2s `pingTimeout`; checkout transparently dials a fresh replacement when the pooled conn fails Ping.

### Python client

- `fts_status`, `proc_status` added.

### Build / repo

- `oxidb-wasm`: drop member-level `[profile.release]` — Cargo only honors profile sections at the workspace root; the per-crate config was silently ignored and emitted a warning every build.
- Drop tracked `target/` symlink — external SSD disconnect left rustc processes hanging in `U` state during build. Cargo creates a fresh local `target/` now.

### Versions

- `oxidb-server`: 0.25.9 → 0.25.19

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
