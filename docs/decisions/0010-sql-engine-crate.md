# ADR-0010: Second engine — standalone SQL engine crate (option 2)

**Status:** Proposed — 2026-07-01
**Branch:** `feat/sql-engine-crate`
**Supersedes:** —
**Related:** [`oxidb-server/src/handler.rs`](../../oxidb-server/src/handler.rs),
[`oxidb-server/src/oxiwire.rs`](../../oxidb-server/src/oxiwire.rs),
[`oxidb-server/src/rbac.rs`](../../oxidb-server/src/rbac.rs),
[`src/engine.rs`](../../src/engine.rs)

## Context

OxiDB is a document database. A prior SQL surface was **deliberately removed**
(commit `c7be1ca5`, "OxiDB is a document db") because it was a thin SQL *dialect*
layered onto the document engine — it shared the document storage, query AST, and
transaction machinery, and dragged relational semantics into a store that isn't
relational.

The new requirement is different and explicit:

> Two engines in one instance. One is SQL. They work on **different files**.
> SQL will **not** work on document-db files.

This is a genuine second engine, not a dialect. The two engines share only the
process, the server port/auth, and the outer lifecycle. They share **no** file,
no lock map, no storage format, and (initially) no transaction.

We pick **option 2** from the design discussion: a separate `oxidb-sql` crate that
owns its own storage, WAL, recovery, catalog, planner, and executor, mounted as a
sibling of the document engine behind one server.

## Decision

### 1. Crate layout

Add a new workspace crate `oxidb-sql/` (sibling to `oxidb`, the document core).

```
oxidb-sql/
  Cargo.toml            # own deps; may depend on `oxidb` ONLY for shared low-level
                        # utilities (wal framing, crc, codec) if we choose to reuse
                        # them — no dependency on the document engine's collection/
                        # query/storage types.
  src/
    lib.rs              # pub struct SqlEngine — the public handle
    catalog.rs          # schema catalog: tables, columns, types, constraints, indexes
    types.rs            # SQL type system + value repr (row-oriented, typed cells)
    storage.rs          # row-oriented, fixed-schema file format (.rdat + catalog)
    wal.rs              # SQL engine's OWN write-ahead log + recovery
    parser.rs           # SQL parse (reuse `sqlparser` crate — already a dep of oxidb)
    planner.rs          # logical plan -> physical plan (scan, filter, join, agg, sort)
    executor.rs         # volcano/iterator executor over the physical plan
    txn.rs              # transaction scoped to THIS engine only
    index.rs            # B-tree secondary indexes over typed columns
  tests/
    sql_smoke.rs        # DDL + DML + SELECT round-trips
    recovery.rs         # crash/replay independent of the doc engine
```

Register in root `Cargo.toml` `[workspace] members`. The document core (`oxidb`)
does **not** depend on `oxidb-sql`; the server depends on both.

**Reuse boundary (explicit):** `oxidb-sql` may reuse *stateless* low-level helpers
from `oxidb` — WAL record framing/CRC (`wal.rs` patterns), `codec.rs`,
`IndexValue` ordering — by lifting them into a shared spot or depending on `oxidb`
for those symbols only. It must **not** touch `Collection`, `OxiDb`, `storage.rs`
(document storage), `query.rs`, or `pipeline.rs`. If reuse creates coupling risk,
copy the ~100 lines instead. The whole point of option 2 is independence.

### 2. On-disk layout — fully separate files

```
oxidb_data/                     # OXIDB_DATA root (unchanged default)
  <document collections>        # today's .bdat/.bopts/.btree/WAL — UNTOUCHED
  _fts/ _blobs/ _archive/ ...   # today's doc-engine subsystems — UNTOUCHED
  sql/                          # NEW — the SQL engine's private root
    catalog.json                # schema catalog (tables, columns, indexes)
    <table>.rdat                # row-oriented data file per table
    <table>.idx/                # secondary index files
    wal/                        # SQL engine's own WAL + sealed segments
```

Hard rule: **the SQL engine never reads or writes outside `oxidb_data/sql/`**, and
the document engine never enters `sql/`. Collection names and table names live in
disjoint namespaces and cannot collide because they never share a directory.

### 3. Wire routing — additive `engine` discriminator

OxiWire is a **transport encoding only** (`oxiwire.rs`: `MAGIC` byte + generic
self-describing value decoder; no per-command opcodes). JSON, MsgPack, and OxiWire
all decode to the same `serde_json::Value` request with a `"cmd"` key. Therefore
engine selection is a field in the decoded request, not a wire-format concern — no
protocol version bump, no codec change.

Add one optional envelope field, defaulted so old clients are unaffected:

```json
{ "engine": "sql", "cmd": "exec", "sql": "SELECT * FROM users WHERE id = ?", "params": [7] }
{ "cmd": "insert", "collection": "users", "doc": { ... } }   // no engine -> doc
```

Router change in `handler.rs`, **before** the existing `match cmd`:

```rust
let engine = request.get("engine").and_then(|v| v.as_str()).unwrap_or("doc");
match engine {
    "sql" => return sql_handler::handle(&sql_engine, &request, format),
    "doc" | _ => { /* existing code path, byte-for-byte unchanged */ }
}
```

- `#[serde(default)]` / `unwrap_or("doc")` ⇒ **any request without `engine` hits the
  document engine exactly as today.** Full backward compatibility (see §8).
- SQL commands (proposed): `exec` (DDL/DML), `query` (SELECT), `prepare`,
  `begin`/`commit`/`rollback` — all under `engine: "sql"`, so no collision with the
  document verbs of the same name.

### 4. SQL engine internals (the real work)

This is the bulk of the effort; the coexistence is the easy part.

- **Catalog** (`catalog.rs`): persistent `CREATE TABLE` metadata — column names,
  SQL types, nullability, PK/unique/FK constraints, index definitions. Loaded on
  open, mutated under DDL.
- **Type system** (`types.rs`): a real typed value (INT/BIGINT/DOUBLE/TEXT/BOOL/
  TIMESTAMP/…), typed rows, NULL handling, coercion rules. Distinct from the
  document engine's dynamic `serde_json::Value`.
- **Storage** (`storage.rs`): row-oriented, fixed-schema pages. Fixed schema means
  we can pack rows densely (unlike the document JSON blobs). Own free-list, own
  page format.
- **WAL + recovery** (`wal.rs`): the SQL engine replays **its own** log on startup,
  fully independent of the document WAL. No cross-log reconciliation because no
  transaction spans both (see §5).
- **Parser** (`parser.rs`): reuse the `sqlparser` crate (already in `oxidb`'s deps
  at 0.59). Parse into our own logical AST — do not reuse the old removed dialect.
- **Planner** (`planner.rs`): logical → physical plan. Scan / index-scan / filter /
  project / hash-join / sort / aggregate. This is genuine relational algebra the
  document engine does not provide.
- **Executor** (`executor.rs`): volcano/iterator model pulling typed rows.
- **Indexes** (`index.rs`): B-tree secondary indexes over typed columns; can reuse
  `IndexValue` cross-type ordering from the document core.

Initial SQL scope (MVP): `CREATE TABLE`, `CREATE INDEX`, `DROP`, `INSERT`,
`UPDATE`, `DELETE`, `SELECT` with `WHERE` / `ORDER BY` / `LIMIT` / single-table +
inner `JOIN` + basic aggregates (`COUNT/SUM/AVG/MIN/MAX` + `GROUP BY`). Parameterized
queries (`?` / `$1`). No views, no subqueries, no window functions in v1 — additive
later.

### 5. Transactions — scoped per engine (no cross-engine atomicity in v1)

Because the two engines share no files, the simplest **correct** boundary is: a
transaction belongs to exactly one engine.

- `engine:"sql"` + `begin/commit/rollback` → SQL engine's own OCC/2PL + WAL.
- Document transactions → today's OCC + 3-phase commit, unchanged.
- **There is no atomic write spanning a document collection and a SQL table in v1.**

This is a deliberate, documented limitation. Achieving cross-engine atomicity would
require a shared WAL/GSN ordering layer — reintroducing exactly the coupling option
2 exists to avoid. Note: `ArchiveSequencer` already mints a global monotonic GSN;
if cross-engine transactions are ever wanted, that is the seam to build on — but it
is explicitly **out of scope** here.

### 6. Server wiring

- `async_server.rs` / `main.rs`: construct `SqlEngine` alongside `OxiDb` at startup,
  gated by an env var so it is **off by default** and zero-cost when unused:
  `OXIDB_SQL=1` (and optional `OXIDB_SQL_DATA`, default `${OXIDB_DATA}/sql`).
- `handler.rs`: the `engine` router in §3, plus a `sql_handler` module translating
  the JSON request to SQL-engine calls and the typed result rows back to
  `serde_json::Value` for the existing response serializers (JSON/MsgPack/OxiWire
  all keep working via `serialize_response`).
- `rbac.rs`: register the new SQL commands. Mapping: `query`/`prepare` → Read;
  `exec`/`begin`/`commit`/`rollback` → ReadWrite; DDL (`CREATE`/`DROP`) → Admin
  (decision point — see §9). Additive; existing command permissions unchanged.
- **Cluster/Raft:** SQL writes are **not** replicated in v1 (single-node SQL engine).
  Documented limitation; Raft integration is a follow-up (would add a
  `SqlExec` variant to `raft/types.rs` mirroring the doc path). Guard: if
  `OXIDB_SQL=1` and cluster mode is on, log a clear "SQL engine is node-local, not
  replicated" warning.

### 7. Client libraries (additive)

Each client gets an optional SQL surface; existing methods send no `engine` and are
untouched:

- **Python** (`oxidb`): `conn.sql("SELECT ...", params=[...])`.
- **Go** (`go/oxidb`): `client.SQL(ctx, query, args...)`.
- **.NET**: `IOxiDbClient.SqlAsync(...)` (Tcp + Embedded).
- **JS** (`oxidb-js`): `db.sql(query, params)` over REST/WS.

All simply add `"engine":"sql"` to the request envelope. Ship after the server MVP.

### 8. Backward compatibility guarantee

- **Wire:** unchanged. OxiWire/JSON/MsgPack carry a new optional key through the
  existing generic codec; no frame/version change (`oxiwire.rs` verified — generic
  value decoder, no per-command opcodes).
- **Old clients:** never send `engine` ⇒ always routed to the document engine ⇒
  identical behavior. No recompile, no config.
- **On disk:** existing `.bdat`/`.bopts`/`.btree`/WAL/`_fts`/`_blobs`/`_archive`
  untouched; SQL lives only under `sql/`.
- **Off by default:** `OXIDB_SQL` unset ⇒ SQL engine not constructed ⇒ zero cost,
  and `engine:"sql"` returns a clean "SQL engine not enabled" error.
- The only "break" is a *new* client sending `engine:"sql"` to an *old* server,
  which is a client-must-match-server concern, not a wire regression (old server
  ignores the unknown key and routes to doc).

### 9. Open decisions (resolve during implementation)

1. **RBAC granularity for DDL** — dedicated `SqlDdl` capability vs. reuse `Admin`.
2. **Storage reuse vs. copy** — depend on `oxidb` for WAL/codec helpers, or vendor
   a copy to keep `oxidb-sql` fully standalone. Lean: thin dependency for framing
   helpers, independent storage/format.
3. **Value bridging** — how SQL `TIMESTAMP`/`DECIMAL`/`NULL` map to JSON in
   responses (JSON has no native decimal/timestamp). Likely: ISO-8601 strings +
   documented conventions, mirroring the document engine's date handling.
4. **Embedded FFI** — expose SQL engine through `oxidb-embedded-ffi` too, or
   server-only for v1? Lean: server-only first.

## Phasing

- **Phase 0 — crate skeleton.** `oxidb-sql` crate, `SqlEngine` handle, catalog +
  typed value + row storage + own WAL/recovery. `tests/recovery.rs` proves
  independent crash-replay. No server wiring yet.
- **Phase 1 — SQL MVP.** parser (sqlparser) → logical AST → planner → executor for
  DDL + INSERT/UPDATE/DELETE + single-table SELECT (WHERE/ORDER BY/LIMIT).
  `tests/sql_smoke.rs`.
- **Phase 2 — relational depth.** inner JOIN, GROUP BY + aggregates, secondary
  indexes + index scans, parameterized queries, per-engine transactions.
- **Phase 3 — server integration.** `OXIDB_SQL` gate, `engine` router in
  `handler.rs`, `sql_handler`, RBAC entries, response bridging. Backward-compat
  tests: old requests still hit doc engine byte-for-byte.
- **Phase 4 — clients + docs.** Python/Go/.NET/JS SQL methods; update
  `docs/sql.md`, `docs/protocol-reference.md`, `docs/server.md`; changelog.
- **Deferred (own ADRs):** cross-engine transactions (shared GSN), Raft replication
  of SQL writes, embedded FFI SQL surface, views/subqueries/window functions.

## Consequences

**Positive**
- Genuine two-engine instance; SQL and document data are physically and
  transactionally isolated exactly as requested.
- Zero risk to the document engine — no shared code on the hot path, off by default.
- Full wire and on-disk backward compatibility.
- Clean crate boundary makes the SQL engine independently testable and evolvable.

**Negative / cost**
- Writing a real SQL engine (planner + executor + typed storage + recovery) is the
  large majority of the work; the coexistence is trivial by comparison.
- No cross-engine atomic transactions in v1 (documented limitation).
- SQL engine is node-local in v1 (not Raft-replicated).
- Second WAL/recovery path to maintain and test.
- Re-introduces a SQL surface after it was deliberately removed — but as an
  *isolated opt-in engine on its own files*, not a dialect over documents, which is
  the distinction that motivated the removal.
