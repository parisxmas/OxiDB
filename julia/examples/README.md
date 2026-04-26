# OxiDb.jl examples

Ten self-contained scripts. Each one runs in isolation against a local
`oxidb-server` on `127.0.0.1:4444`:

```bash
oxidb-server &                              # start the server (any v0.25.x build)
cd julia/examples
julia --project=../OxiDb 01_hello.jl
```

`--project=../OxiDb` points Julia at the OxiDb package's `Project.toml`
so `using OxiDb` resolves without a separate install.

| # | File | What it shows |
|---|---|---|
| 01 | `01_hello.jl`                  | Smallest possible program — connect, insert, find |
| 02 | `02_bulk_load_and_index.jl`    | `insert_many` 10 k rows, `create_index`, range query |
| 03 | `03_aggregation_top_n.jl`      | `$match` → `$group` → `$sort` → `$limit` pipeline |
| 04 | `04_atomic_counter_and_push.jl`| Combined `$inc` + `$push` + `$addToSet` in one call |
| 05 | `05_transaction_transfer.jl`   | `begin_tx` / `commit_tx` / `rollback_tx`, OCC retry-safe |
| 06 | `06_full_text_search.jl`       | `create_text_index` + TF-IDF ranked `text_search` |
| 07 | `07_vector_search.jl`          | `create_vector_index` + cosine `vector_search` |
| 08 | `08_oxiscript_procedure.jl`    | Server-side OxiScript proc, one-round-trip workflow |
| 09 | `09_ttl_sessions.jl`           | TTL index that auto-expires session rows |
| 10 | `10_sql_dashboard.jl`          | Same data, queried with `SELECT … GROUP BY` |

All scripts use the minimal client (`OxiDb` v0.4.0) — `connect`, `exec`,
and 12 convenience helpers. Anything not in the helper list is reachable
via the generic `exec(db, "command_name"; field = value)`.
