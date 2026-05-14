# OxiDb.jl examples

Ten self-contained scripts exercising the [`OxiDb`](../OxiDb) TCP client
(v0.5.0) against a local `oxidb-server`.

## Run them all

```bash
julia/examples/run_all.sh
```

`run_all.sh` reuses an `oxidb-server` already listening on `127.0.0.1:4444`,
or builds one from source and starts a throwaway instance (on a temp data
directory, torn down on exit). It instantiates the example environment and
runs every script, reporting pass/fail.

## Run one

```bash
# 1. have an oxidb-server running on 127.0.0.1:4444
cargo run --release -p oxidb-server          # ...in another terminal

# 2. run any script against the bundled example environment
julia --project=julia/examples julia/examples/01_hello.jl
```

The `julia/examples/Project.toml` resolves `OxiDb` straight from the sibling
`../OxiDb` checkout (via `[sources]`) — no registry, no manual `Pkg.develop`.
First run: `julia --project=julia/examples -e 'using Pkg; Pkg.instantiate()'`
(`run_all.sh` does this for you).

**Requirements:** Julia 1.11+ (for `[sources]`); Rust toolchain only if you
want `run_all.sh` to build the server for you.

## What each script shows

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

## A note on the client API

The TCP `OxiDb` client is intentionally minimal: `connect`, `exec`, and a
dozen CRUD/query helpers (`insert`, `find`, `update`, `delete`, `count_docs`,
`aggregate`, `sql`, …). Anything without a dedicated helper — index creation,
transactions, TTL, vector/text indexes, OxiScript procedures — is reached
through the generic escape hatch:

```julia
exec(db, "create_ttl_index"; collection = "sessions",
     field = "created_at", expireAfterSeconds = 3600)
```

The scripts above use exactly this mix; read them top-to-bottom as a tour.
