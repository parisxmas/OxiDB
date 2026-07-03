# ADR-0013: Full .NET EF Core support for the SQL engine

**Status:** Accepted — 2026-07-03 (Phases A–D shipped incl. B's cluster support; Phase E minimal provider shipped)
**Related:** [ADR-0010](0010-sql-engine-crate.md) (SQL engine),
[ADR-0012](0012-multi-database.md) (multi-database),
`dotnet/` (.NET packages; the pre-ADR-0010 EF Core provider was removed in
`c7be1ca5` and is recoverable from git history as a skeleton)

## Context

Entity Framework Core is the default data-access layer in .NET; "works with
EF" is the practical bar for the SQL engine being a first-class citizen in
that ecosystem. Today `OxiDb.Client.Tcp`/`Embedded` expose `SqlAsync`, but EF
sits on the ADO.NET abstraction (`DbConnection`/`DbCommand`/`DbDataReader`)
plus a provider layer — neither exists — and a capability probe against the
engine (2026-07-03, live) found every one of the following missing:
`CASE WHEN`, `LIKE`, `SELECT DISTINCT`, `EXISTS`, `CAST`, string scalar
functions, named parameters, `ALTER TABLE`, column `DEFAULT`s, `DECIMAL`,
`BLOB`, foreign-key syntax — and, architecturally, any transaction that
spans requests (`BEGIN…COMMIT` is scoped to one execute batch; an open
transaction at batch end auto-rolls back).

What the engine already has that EF needs: real relational storage with PK
uniqueness and secondary indexes; INNER/LEFT/RIGHT/FULL joins; GROUP
BY/HAVING; scalar/IN/correlated subqueries; UNION; LIMIT/OFFSET; views;
window functions; positional parameters; `AUTO_INCREMENT` with
`last_insert_id` (key retrieval); `COALESCE`/`NULLIF`; SHOW/DESCRIBE
introspection (scaffolding-ready).

## Decision

Close the gap in five phases, each independently shippable:

- **Phase A — engine expression surface** (this ADR's first deliverable):
  `CASE WHEN` (searched + simple), `LIKE`/`NOT LIKE` (`%`/`_`, `ESCAPE`),
  `SELECT DISTINCT`, `EXISTS`/`NOT EXISTS` (including correlated, reusing
  the correlated-IN machinery), `CAST`, string scalars
  (`UPPER`/`LOWER`/`LENGTH`/`SUBSTRING`/`CONCAT`/`TRIM`/`REPLACE`) and the
  `||` operator, and per-column **type metadata** on SELECT results (wire:
  `"types"` array) so a future `DbDataReader.GetFieldType` doesn't have to
  guess from values.
- **Phase B — interactive transactions** *(shipped, standalone mode)*:
  connection-scoped SQL transactions (`BEGIN` → many requests with reads →
  `COMMIT`) and savepoints. `TxnState` became owned/id-keyed: it parks in
  the engine's session map between requests and is resumed per batch; the
  session layer carries the id (`Session::sql_tx`), disconnect rolls back,
  errors abort the transaction. Old `execute*` entry points keep the
  batch-scoped auto-rollback contract. **Cluster**: statements execute
  locally on the leader (writes buffer in the parked transaction); a lone
  `COMMIT` is intercepted by the dispatcher, the buffered ops replicate
  through Raft as one `SqlTxnCommit` entry (deterministic — ops carry final
  row ids/cells), and every node applies them as one atomic WAL batch.
  `BEGIN` must be its own request in cluster mode.
- **Phase C — ADO.NET provider** (`OxiDb.Data`) *(shipped)*:
  `DbConnection` (connection string `Host/Port/Database`, database via
  session `use_db`), `DbCommand` with named-`@p`-to-positional rewrite,
  `DbDataReader` over the wire result + `types` metadata,
  `DbTransaction`/savepoints over Phase B, `DbProviderFactory`.
  Milestone hit: **Dapper runs end-to-end** (typed mapping incl.
  TIMESTAMP→DateTime, named params, multi-command transactions) —
  `tests/adonet-dapper-test/`.
- **Phase D — DDL & types for Migrations** *(shipped)*: `ALTER TABLE`
  ADD/DROP/RENAME COLUMN (WAL-logged, rows rewritten, disk-first folds into
  a fresh snapshot), column `DEFAULT` literals, `DECIMAL`→DOUBLE storage
  (documented), a real `BLOB` type (base64 on the JSON wire), column
  `UNIQUE` **enforced** (engine + transactions; NULLs exempt), FK syntax
  parsed and ignored (documented), and `INSERT ... RETURNING` (how
  ADO.NET/EF read generated keys).
- **Phase E — EF Core provider** *(shipped, minimal)*
  (`OxiDb.EntityFrameworkCore`, EF Core 9 / net10.0): relational service
  registrations (convention set, type mappings, SQL generation helper),
  QuerySqlGenerator (engine-form LIMIT/OFFSET), UpdateSqlGenerator (keys
  read back via `RETURNING`), MigrationsSqlGenerator (`AUTO_INCREMENT` on
  store-generated integer keys), string method/member translators
  (Contains/StartsWith/EndsWith → LIKE, Upper/Lower/Trim/Replace/
  Substring/Length), `EnsureCreated`-based schema creation, and a history
  repository stub for `Database.Migrate()`. Engine gaps closed for EF's
  query shapes: derived tables (`FROM (SELECT ...) AS x`), parameterized
  `LIMIT $1 OFFSET $2`, `UPDATE`/`DELETE ... RETURNING`, table-level
  `CONSTRAINT ... PRIMARY KEY/UNIQUE`. Verified by a live end-to-end
  suite (`tests/efcore-oxidb-test/`): EnsureCreated, generated keys,
  Where/OrderBy/join/GroupBy-Sum/Contains/Skip+Take LINQ, change-tracked
  UPDATE/DELETE with concurrency checks, explicit transactions +
  rollback. **Not** validated against EF's relational specification test
  suites; no design-time scaffolding (`dotnet ef dbcontext scaffold`),
  no migration-operation coverage beyond CreateTable, no value
  converters beyond the built-in mappings.

## Notes / constraints

- Scalar additions ride the `Expr::Func { ScalarFunc, args }` node
  introduced with COALESCE — new functions add **no** expression-traversal
  arms. `CASE` and `LIKE` are encoded as ScalarFunc variants with lazy
  argument evaluation (short-circuit for free).
- `EXISTS (SELECT …)` rewrites to `1 IN (SELECT 1 …)` at translation,
  inheriting correlation support; aggregated EXISTS subqueries are rejected
  (EF only generates `SELECT 1` bodies).
- `DISTINCT` dedups after projection and ordering, before LIMIT/OFFSET;
  `DISTINCT` + ORDER BY on a non-projected column is rejected-by-behavior
  (PostgreSQL rejects it outright), documented.
- Type metadata is static-first (column refs, casts, literals, function
  return types), falling back to value scanning; absent types are `null` in
  the wire array.
- Interim milestone worth naming: after Phases A+C, **Dapper-class usage is
  fully supported** without waiting for EF.

## Out of scope

- `DECIMAL` exactness beyond storage (no fixed-point arithmetic engine).
- Distributed EF transactions across databases (ADR-0011/0012 boundaries).
- EF6 (non-Core).
