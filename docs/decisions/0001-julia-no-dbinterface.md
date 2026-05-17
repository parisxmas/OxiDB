# ADR 0001 — Julia clients do not implement `DBInterface.jl`

**Status:** Accepted
**Date:** 2026-05-18
**Related:** PR #26 (Tables.jl interop), PR #27 (drop SQL from the Julia surface)

## Context

OxiDB ships two Julia packages — `julia/OxiDb` (TCP client) and
`julia/OxiDbEmbedded` (in-process via FFI). The natural question for
ecosystem fit is whether they should implement
[`DBInterface.jl`](https://github.com/JuliaDatabases/DBInterface.jl),
the standard Julia database interface implemented by SQLite.jl,
LibPQ.jl, MySQL.jl, DuckDB.jl, ODBC.jl, ClickHouse.jl, etc., and
consumed by tooling such as Migrator.jl, DataFrames extensions, and
MLJ adapters.

`DBInterface`'s public surface is **fundamentally SQL-shaped**. Every
overload of `execute` and `prepare` requires a SQL `AbstractString` —
verified directly against the installed source:

```julia
execute(conn::Connection, sql::AbstractString, params)
execute(conn::Connection, sql::AbstractString; kwargs...)
prepare(conn, sql::AbstractString)
```

There is no document-friendly subset. A driver that implements it must
accept SQL strings as the primary query input.

## Decision

The Julia clients **do not** implement `DBInterface.jl`. They retain
their document-native API — `find(db, coll, query::Dict)`,
`update(db, …, Dict("\$inc" => …))`, etc. — and integrate with the
Julia data ecosystem via `Tables.jl` instead.

## Rationale

Implementing `DBInterface` would force one of two unacceptable choices:

1. **Reintroduce SQL on the Julia surface.** OxiDB is a document
   database; SQL was deliberately removed from the Julia surface in
   PR #27 (no `sql()` export, no `sql_dashboard` example, no SQL row
   in the helper docs). Adding SQL back through `DBInterface` directly
   contradicts that direction.

2. **Ship a non-conformant adapter** — only `connect` / `close!` /
   `transaction`, no `execute` / `prepare`. This wouldn't actually help
   the consumers `DBInterface` exists for: Migrator.jl, DataFrames
   extensions, and MLJ adapters all go through `execute(conn, "…")`,
   so they'd see a broken driver. The only deliverable would be a
   "implements DBInterface" badge that lies.

The Julia data-ecosystem story is solved through a different door:

- **Data-flow side** (`DataFrame(query_result)`, `CSV.write`, `MLJ`,
  `Plots`, `GLM`, …): handled by `Tables.jl` interop, added in PR #26.
  `find` / `aggregate` return an `OxiDbResult <: AbstractVector` that
  satisfies the `Tables.jl` row-access interface. Heterogeneous-schema
  rows are merged with `missing` filled in for absent fields, just
  like `Tables.dictrowtable`.
- **Query side** stays **document-native**. Idiomatic Julia callers
  build query / update / aggregation pipeline `Dict`s directly — no
  string parsing, no `?` parameter binding, no `prepare` ceremony.

## Consequences

**Positive:**

- The Julia surface stays internally consistent with OxiDB's
  document-first positioning. There is exactly one way to query, and
  it isn't a SQL string in a function called `execute`.
- `Tables.jl` already handles the part of the ecosystem that 95% of
  Julia data work actually uses — `DataFrame(rows)` works, `CSV.write`
  works, `MLJ` accepts results.
- No half-baked compatibility surface to maintain.

**Negative / accepted trade-offs:**

- Tooling that takes `DBInterface.Connection` as input (e.g. Migrator,
  some testing helpers) won't accept an `OxiDbClient` /
  `OxiDatabase`. Users of those tools will need to either bypass them
  or wrap the OxiDB call themselves.
- The Julia clients won't appear on the JuliaDatabases.org
  driver-compat matrix, which costs some discoverability.

## Revisiting

This decision is worth revisiting if `DBInterface.jl` grows a
first-class non-SQL execute path (e.g., `execute(conn, query::Any,
params)` where drivers pick the query type), or if the project
direction changes such that OxiDB exposes a first-class Julia SQL
surface again.
