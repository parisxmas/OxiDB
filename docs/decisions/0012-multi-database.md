# ADR-0012: Multiple databases, shared by both engines

**Status:** Accepted — 2026-07-03 (revised; first draft 2026-07-03 was written
against a wrong premise, see "Correction" below)
**Related:** [ADR-0010](0010-sql-engine-crate.md) (SQL engine crate),
[ADR-0011](0011-cross-engine-transactions.md) (cross-engine transactions),
[`src/database_manager.rs`](../../src/database_manager.rs),
[`oxidb-server/src/sql_bridge.rs`](../../oxidb-server/src/sql_bridge.rs)

## Correction

The first draft of this ADR claimed "neither engine has a database concept
today" and proposed a new registry with a `_dbs/<name>/` layout. That premise
was wrong: the **document engine has had first-class databases since v0.19**
— `DatabaseManager` (`src/database_manager.rs`) with this layout:

```text
OXIDB_DATA/
├── _auth/  _audit/       # server-global
├── oxidb/                # default database (postgres = alias)
│   ├── *.btree/.wal/...  _blobs/ _fts/ _archive/
├── myapp/                # user database
└── sql/                  # default database's SQL engine (OXIDB_SQL_DATA)
```

with wire commands `create_database` / `drop_database` / `list_databases` /
`use_db`, a per-request `db` field, session-scoped current database, name
validation, flat-layout auto-migration, and per-database RBAC
(`db_roles` on users, `grant_db_role`/`revoke_db_role`, effective-role
resolution). This revision documents what was actually missing and what was
done about it.

## Context — the real gaps (as of v0.32.0)

1. **The SQL engine ignored databases entirely.** `sql_bridge` held one
   process-global `SqlEngine` at `OXIDB_SQL_DATA`; a request's `db` field or
   session database changed which *document* namespace it hit, but SQL always
   landed in the same single catalog.
2. **The async/cluster server ignored databases.** `async_server::dispatch`
   routed every request to `state.db` (the default database) and had no
   `create/drop/list/use_db` command handling — only the standalone
   dispatcher was database-aware. Cluster mode didn't even construct a
   `DatabaseManager`; it opened the data root as one flat database, an
   incompatible layout.
3. **Flat-layout auto-migration was stale.** `COLLECTION_EXTENSIONS` listed
   only the original engine's extensions (`.dat` era), so modern
   `.btree`/`.worm`/`.bopts`/`.bdat` files at the data root were never
   migrated (left orphaned/invisible), `_archive`/`_gsn` were not moved, and
   a name collision would silently overwrite the destination.

## Decision (implemented, server 0.32.1)

**One database = one document namespace + one SQL namespace**, addressed by
the same name everywhere.

1. **Per-database SQL engines** — `sql_bridge` keeps a registry
   `name → Arc<SqlEngine>`, lazily opened:
   - default database (`oxidb`, alias `postgres`): `OXIDB_SQL_DATA`
     (historically `${OXIDB_DATA}/sql`) — existing SQL data untouched;
   - every other database: `${OXIDB_DATA}/<name>/sql` — inside the
     database's own directory, so `drop_database` removes it with the rest.
   The handler routes SQL by the database the session layer already resolved
   (`handle_request_in_db`); `drop_database` also evicts the registry entry
   so a recreated database starts with a fresh catalog.
2. **Async/cluster server is database-aware** — `ServerState` carries the
   `DatabaseManager`; dispatch resolves `request.db` / session current
   database for every command and implements the four database commands
   (node-local; see limitations). Cluster mode now opens the same managed
   layout as standalone.
3. **Migration fixed** — extension list covers the B-tree engine files,
   `_archive`/`_gsn` move with the database, and migration never overwrites:
   on a name collision the flat file stays put with a warning.

## Completion (server 0.32.2)

The v1 limitations were closed in a follow-up round:

- **Raft**: `CreateDatabase`/`DropDatabase` request variants replicate
  database DDL; a `Scoped { db, inner }` envelope replicates writes, SQL,
  and transaction commits against named databases (unwrapped requests —
  including every pre-0.32.2 log entry — apply to the default database, so
  old logs replay unchanged).
- **SQL text**: `CREATE DATABASE [IF NOT EXISTS]` / `DROP DATABASE
  [IF EXISTS]` / `SHOW DATABASES` / `USE <db>` parse via
  `oxidb_sql::parse_database_statement` (single-statement only). Both
  surfaces funnel through one `DbIntent` (`oxidb-server/src/db_admin.rs`)
  with one permission gate: create/drop are Admin-only on either surface.
- **Background threads**: `DatabaseManager::enable_background_threads`
  starts TTL eviction + alert evaluation on every opened/created database;
  `drop_database` shuts the engine down (the TTL thread holds a strong Arc —
  without the shutdown signal the dropped engine leaked forever).
- **REST**: `?db=<name>` on every route, including `POST /api/sql`.
  **WebSocket**: per-message `"db"` field; subscriptions remember the
  engine they watch and unwatch it on cleanup.
- **Transactions**: a session's open transaction is bound to the database
  it began on (`Session::tx_db`); requests targeting another database are
  rejected until commit/rollback, and replicated commits extract buffered
  writes from — and apply to — the bound database.

## Intentionally out of scope

- **OxiMem/RESP keyspace stays global** — Redis-style `SELECT <n>` numbered
  databases are an orthogonal protocol concept.
- **S3 buckets stay global** — one bucket namespace, matching S3 semantics.
- Cross-database queries/transactions remain non-goals (ADR-0011 scopes
  cross-*engine* transactions to a single database).
