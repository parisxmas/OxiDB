# ADR-0012: Multiple databases, shared by both engines

**Status:** Proposed — 2026-07-03 (design only; no implementation)
**Related:** [ADR-0010](0010-sql-engine-crate.md) (SQL engine crate),
[ADR-0011](0011-cross-engine-transactions.md) (cross-engine transactions),
[`src/engine.rs`](../../src/engine.rs),
[`oxidb-server/src/handler.rs`](../../oxidb-server/src/handler.rs),
[`oxidb-server/src/sql_bridge.rs`](../../oxidb-server/src/sql_bridge.rs)

## Context

Neither engine has a database concept today:

- The **document engine** is one `OxiDb` over one data directory — a flat
  collection namespace. Collections live as files directly in `OXIDB_DATA`
  (`<name>.btree` / `.wal` / `.worm`), beside system directories (`_blobs/`,
  `_fts/`, `_archive/`).
- The **SQL engine** is one `SqlEngine` over one directory
  (`OXIDB_SQL_DATA`, default `${OXIDB_DATA}/sql`) — a flat table/view/index
  namespace in a single `catalog.json`.

The only isolation available is name-prefixing (`crm.users`) or running a
second server instance. Users coming from MongoDB (`use crm`) or PostgreSQL
(`CREATE DATABASE`) expect first-class databases: independent namespaces,
per-database access control, and cheap create/drop.

A key design question is whether "database" is a per-engine concept (a SQL
database and an unrelated document database that happen to share a name) or a
**server-level concept spanning both engines**. Per-engine would be simpler
to bolt on, but it duplicates DDL, duplicates RBAC scoping, and makes the
eventual ADR-0011 cross-engine transaction story incoherent (a transaction
would span "two databases"). This ADR proposes the shared model.

## Decision (proposed design)

One **server-level database registry**. A database is a named unit that
contains *both* a document namespace and a SQL namespace. Creating database
`crm` makes `crm` addressable from document commands and from SQL; dropping
it removes both.

```
              ┌────────────── database "default" ──────────────┐
OXIDB_DATA/   │ *.btree/*.wal/*.worm   _blobs/ _fts/   sql/    │   ← today's layout, untouched
              └────────────────────────────────────────────────┘
OXIDB_DATA/_dbs/crm/      ← database "crm"
              ├── *.btree/*.wal/*.worm  _blobs/ _fts/          (document engine)
              └── sql/                                         (SQL engine)
OXIDB_DATA/_dbs/analytics/ ...
```

### 1. Directory layout & backward compatibility

- The **existing data-dir root *is* database `default`**. Upgrading moves no
  files; a server that never issues a database command behaves byte-for-byte
  as today.
- New databases live under `OXIDB_DATA/_dbs/<name>/`, each an ordinary
  engine root (collections + `_blobs` + `_fts` + `sql/`). The `_dbs/` prefix
  cannot collide with collection files or system dirs at the root.
- Database names: `[A-Za-z0-9_-]{1,64}`, must not start with `_`;
  `default` is reserved (always exists, cannot be dropped).

### 2. Registry

A `Databases` struct owned by the server (and by the embedded FFI handle):

```rust
struct Databases {
    root: PathBuf,
    doc:  RwLock<HashMap<String, Arc<OxiDb>>>,      // lazily opened
    sql:  RwLock<HashMap<String, Arc<SqlEngine>>>,  // lazily opened, gated on OXIDB_SQL
}
```

- Engines open **lazily on first use** (like the SQL engine today) and stay
  open; v1 does not evict idle databases. Cost of an open-but-idle document
  engine is its caches — acceptable for the expected tens of databases, and
  an LRU close policy can come later without wire changes.
- `create_database` = validate name + `mkdir` + registry entry (cheap).
  `drop_database` = close engines, delete the directory. Admin-only,
  audited, and refused for `default` and for databases with an active
  transaction.
- `list_databases` = registry ∪ `_dbs/*` directory scan (so databases
  created before a restart are found without a manifest file).

### 3. Wire protocol

- Every request gains an **optional `"db"` field**; absent means
  `"default"`. This keeps requests stateless — connection pools, OxiPool
  fan-out, and retries need no session affinity. Existing clients are
  untouched (full backward compatibility, same rule as the `engine` field).

  ```json
  { "db": "crm", "cmd": "find", "collection": "users", "query": {} }
  { "db": "crm", "engine": "sql", "cmd": "sql", "sql": "SELECT ..." }
  ```

- New commands, document-engine style: `create_database`, `drop_database`,
  `list_databases`.
- **Convenience `use`** (optional, phase 2+): a session may set a default
  db (`{"cmd": "use", "db": "crm"}`) applied when a request has no `db`
  field. Pure session state in the connection handler; the wire stays
  stateless underneath. Clients that pool connections should keep sending
  explicit `db` instead.

### 4. SQL surface

The same registry, reachable from SQL — routed by the server's SQL bridge,
not by `oxidb-sql` itself (the crate keeps owning exactly one directory;
multiplexing is the host's job, mirroring how the embedded FFI already opens
per-handle engines):

```sql
CREATE DATABASE crm;          -- registry create (both engines)
DROP DATABASE crm;            -- registry drop (both engines)
SHOW DATABASES;               -- introspection, read-only (like ADR SHOW TABLES)
USE crm;                      -- session default, server-side state
```

`sqlparser` 0.59 already parses all four (`Statement::CreateDatabase`,
`Statement::Drop` with `ObjectType::Database`, `Statement::ShowDatabases`,
`Statement::Use`); the bridge intercepts them before `oxidb_sql::execute`
and serves them from the registry.
Qualified names (`crm.users`) stay unsupported inside SQL text in v1 — the
`db` field / `USE` selects the database for the whole statement batch.

### 5. RBAC

Users gain an optional per-database role map on top of the global role:

```json
{ "user": "reporting", "role": "read",
  "db_roles": { "crm": "readwrite", "analytics": "read" } }
```

- Effective role for a request = `db_roles[db]` if present, else the global
  role. Empty `db_roles` ⇒ exactly today's semantics.
- `create_database` / `drop_database` / `list_databases` (full list) require
  Admin; non-admins see only databases they have a role for.

### 6. Cluster (Raft)

Database DDL is a replicated write: `OxiDbRequest` gains
`CreateDatabase { name }` / `DropDatabase { name }`, and every existing
write variant gains an optional `db` field (defaulting to `default`, so old
log entries replay unchanged). SQL writes already replicate as SQL text;
they additionally carry the resolved `db`. Snapshot/restore transfers the
whole `OXIDB_DATA` tree, so `_dbs/` rides along for free.

### 7. Embedded FFI & clients

- Embedded: the handle's directory is the registry root; the FFI request
  JSON accepts the same `db` field. Python/.NET embedded wrappers expose
  `db=` / `Database=` options.
- TCP clients (Python, Go, .NET, JS, Julia, PHP): constructor gains an
  optional `db` (default `"default"`), stamped on every request. One client
  object per database — mirrors MongoDB driver ergonomics.
- REST: `POST /api/db/{db}/...` alongside the existing routes (which serve
  `default`); WebSocket subscriptions gain a `db` field.
- VS Code extension: a database picker above the trees; `USE` in the SQL
  editor works via the session default.

### 8. Interaction with other subsystems

- **Blobs, FTS, TTL, alerts, schedules, security rules**: all are owned by
  the per-database `OxiDb` instance and scope naturally. System collections
  (`_auth_users` etc.) that are *server-global* stay in `default` — auth is
  server-level, not per-database.
- **PITR / backup**: v1 scopes `backup()` / `restore_to_point` to one
  database (each `OxiDb` already owns its own GSN sequencer + archive).
  A server-wide consistent multi-db snapshot needs the ADR-0011 shared GSN
  clock — explicitly deferred to that work.
- **ADR-0011**: cross-engine transactions coordinate the two engines *of one
  database*. Cross-database transactions are a non-goal.
- **OxiMem / MQTT**: keyspace stays global in v1 (`SELECT <n>`-style RESP
  databases are an independent, orthogonal feature).

## Non-goals (v1)

- Cross-database queries, joins, `$lookup`, or transactions.
- Qualified `db.table` names inside SQL text.
- Per-database encryption keys, quotas, or resource limits.
- Idle-database eviction (lazy open only).
- Renaming databases.

## Phasing

1. **Registry + document engine** — `Databases`, lazy open, `db` field in
   the handler, `create/drop/list_databases` commands, tests. No SQL, no
   RBAC changes. Ships usable multi-db.
2. **SQL surface** — `db` routing in the SQL bridge; `CREATE/DROP DATABASE`,
   `SHOW DATABASES`, `USE` intercepted in the bridge; session default.
3. **RBAC** — `db_roles`, effective-role resolution, admin gating, audit
   fields.
4. **Cluster** — Raft request variants + `db` on write variants, replay
   compatibility tests.
5. **Clients & tooling** — 6 client libraries, REST routes, embedded FFI,
   VS Code extension picker, docs.

## Consequences

- One database concept for the whole product: one DDL surface, one RBAC
  scope, one directory convention — and ADR-0011 composes with it cleanly.
- Existing deployments upgrade with zero migration and zero behavior change
  until the first database command arrives.
- The per-database engine instances multiply memory for caches and WAL
  handles; acceptable at tens of databases, revisit (eviction) beyond that.
- `oxidb-sql` stays single-directory and host-multiplexed, so the crate's
  complexity does not grow with this feature.
