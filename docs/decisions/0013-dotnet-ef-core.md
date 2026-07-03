# ADR-0013: Full .NET EF Core support for the SQL engine

**Status:** Accepted — 2026-07-03 (Phase A in progress)
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
- **Phase B — interactive transactions** (largest architectural item):
  connection-scoped SQL transactions (`BEGIN` → many requests with reads →
  `COMMIT`), savepoints, and the cluster story (buffered ops commit as one
  Raft batch). Requires re-shaping `Transaction<'a>` (engine borrow) into an
  id-keyed owned transaction like the document engine's `active_tx`.
  Design first (own ADR section or ADR-0014).
- **Phase C — ADO.NET provider** (`OxiDb.Data`): `DbConnection`,
  `DbCommand` (named `@p` → positional rewrite), `DbDataReader` over the
  wire result + type metadata, `DbTransaction` over Phase B. Milestone:
  **Dapper works** — a meaningful ecosystem unlock before EF.
- **Phase D — DDL & types for Migrations**: `ALTER TABLE`
  ADD/DROP/RENAME COLUMN, column `DEFAULT`s, `DECIMAL`, `BLOB`/binary,
  UNIQUE **enforcement** (today parsed and silently ignored — an integrity
  trap), FK syntax tolerance (parse + document non-enforcement, or enforce).
- **Phase E — EF Core provider** (`OxiDb.EntityFrameworkCore`):
  QuerySqlGenerator (LINQ → OxiDB dialect), TypeMappingSource,
  Migrations/Update SQL generators (keys via `last_insert_id`; batched
  inserts derive per-row ids from the contiguous block), scaffolding over
  SHOW/DESCRIBE; validated against EF's relational specification tests.

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
