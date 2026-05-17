# OxiDb.jl examples

Two sets of self-contained scripts:

- **`embedded/`** — the [`OxiDbEmbedded`](../OxiDbEmbedded) package (v0.5.0):
  the engine runs **in your process**, no server.
- **`*.jl` (numbered)** — the [`OxiDb`](../OxiDb) TCP client (v0.6.0) against a
  local `oxidb-server`.

New to OxiDB? Start with `embedded/` — there's nothing to install or run first.

## Run them all

```bash
julia/examples/run_all.sh
```

`run_all.sh` instantiates the example environment, runs every embedded script
(nothing needed), then runs the TCP scripts — reusing an `oxidb-server`
already on `127.0.0.1:4444`, or building one from source and starting a
throwaway instance (temp data dir, torn down on exit). Reports pass/fail.

## Run one

```bash
# Embedded — no server, just run it:
julia --project=julia/examples julia/examples/embedded/01_hello.jl

# TCP — needs a server on 127.0.0.1:4444:
cargo run --release -p oxidb-server          # ...in another terminal
julia --project=julia/examples julia/examples/01_hello.jl
```

`julia/examples/Project.toml` resolves both `OxiDb` and `OxiDbEmbedded`
straight from the sibling `../OxiDb` / `../OxiDbEmbedded` checkouts (via
`[sources]`) — no registry, no manual `Pkg.develop`. First run:
`julia --project=julia/examples -e 'using Pkg; Pkg.instantiate()'`
(`run_all.sh` does this for you).

**Requirements:** Julia 1.11+ (for `[sources]`); Rust toolchain only if you
want `run_all.sh` to build the server for you.

## Embedded examples — `embedded/`

In-process, no server. The full helper API is exported directly.

| # | File | What it shows |
|---|---|---|
| 01 | `embedded/01_hello.jl`             | Smallest possible program — `open_db`, insert, find |
| 02 | `embedded/02_persistence.jl`       | Close + reopen the same path; data persists, like SQLite |
| 03 | `embedded/03_transactions.jl`      | `transaction(db) do … end` — auto-commit, auto-rollback on throw |
| 04 | `embedded/04_indexes.jl`           | `create_index` / `create_unique_index`, indexed lookup |
| 05 | `embedded/05_aggregation.jl`       | `$match` → `$group` → `$sort` pipeline |
| 06 | `embedded/06_encryption_at_rest.jl`| `open_db(path; encryption_key_path=…)` — AES-encrypted on disk |
| 07 | `embedded/07_tables_interop.jl`    | Tables.jl interop — `DataFrame(rows)` / `CSV.write` / MLJ work out of the box |

## TCP-client examples — numbered scripts

Need a running `oxidb-server`.

| # | File | What it shows |
|---|---|---|
| 01 | `01_hello.jl`                  | Smallest possible program — connect, insert, find |
| 02 | `02_bulk_load_and_index.jl`    | `insert_many` 10k rows, `create_index`, range query |
| 03 | `03_aggregation_top_n.jl`      | `$match` → `$group` → `$sort` → `$limit` pipeline |
| 04 | `04_atomic_counter_and_push.jl`| Combined `$inc` + `$push` + `$addToSet` in one call |
| 05 | `05_transaction_transfer.jl`   | `begin_tx` / `commit_tx` / `rollback_tx`, OCC retry-safe |
| 06 | `06_full_text_search.jl`       | `create_text_index` + TF-IDF ranked `text_search` |
| 07 | `07_vector_search.jl`          | `create_vector_index` + cosine `vector_search` |
| 08 | `08_oxiscript_procedure.jl`    | Server-side OxiScript proc, one-round-trip workflow |
| 09 | `09_ttl_sessions.jl`           | TTL index that auto-expires session rows |
| 10 | `10_sql_dashboard.jl`          | Same data, queried with `SELECT … GROUP BY` |

## A note on the two client APIs

`OxiDbEmbedded` exports the **full** helper surface — every operation is a
named function (see [its README](../OxiDbEmbedded/README.md)).

The TCP `OxiDb` client is intentionally **minimal**: `connect`, `exec`, and a
dozen CRUD/query helpers (`insert`, `find`, `update`, `delete`, `count_docs`,
`aggregate`, `sql`, …). Anything without a dedicated helper — index creation,
transactions, TTL, vector/text indexes, OxiScript procedures — is reached
through the generic escape hatch:

```julia
exec(db, "create_ttl_index"; collection = "sessions",
     field = "created_at", expireAfterSeconds = 3600)
```

Read each set top-to-bottom as a tour.
