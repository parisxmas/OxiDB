# OxiDbEmbedded.jl

In-process [OxiDB](https://github.com/parisxmas/OxiDB) for Julia — the database
engine runs **inside your Julia process** via a native FFI library. No server,
no socket, no separate process to manage.

| | |
|---|---|
| **Mode** | Embedded (in-process, FFI) |
| **Server needed?** | No |
| **Version** | 0.5.0 |
| **Julia** | 1.6+ |

For the client/server (TCP) variant, see [`../OxiDb`](../OxiDb).

## Install

```julia
using Pkg
Pkg.develop(path="julia/OxiDbEmbedded")
```

## Native library

`OxiDbEmbedded` calls into `liboxidb_embedded_ffi` (`.dylib` / `.so` / `.dll`),
which lives in `lib/` (git-ignored — it is a build artifact, not source).

It is resolved in this order:

1. **Already in `lib/`** — used as-is.
2. **Otherwise auto-downloaded** from GitHub Releases on first `using`.
3. **Build it yourself** from the engine source — recommended when working
   against an unreleased engine:

   ```bash
   cargo build --release -p oxidb-embedded-ffi
   cp target/release/liboxidb_embedded_ffi.* julia/OxiDbEmbedded/lib/
   ```

   (On macOS the file is `liboxidb_embedded_ffi.dylib`; Linux `.so`; Windows
   `oxidb_embedded_ffi.dll`.)

## Quick start

```julia
using OxiDbEmbedded

db = open_db(mktempdir())            # or open_db("/path/to/data")

insert(db, "users", Dict("name" => "Alice", "age" => 30))
insert(db, "users", Dict("name" => "Bob",   "age" => 17))

adults = find(db, "users", Dict("age" => Dict("\$gte" => 18)))
@show adults

update(db, "users", Dict("name" => "Alice"), Dict("\$inc" => Dict("age" => 1)))
@show count_docs(db, "users")

close(db)
```

Encrypted at rest:

```julia
db = open_db("/path/to/data"; encryption_key_path = "/path/to/key")
```

## API

`OxiDbEmbedded` exports the **full** helper surface — CRUD, indexes,
aggregation, transactions, blob storage, full-text search, OxiScript
procedures, SQL:

| Group | Functions |
|-------|-----------|
| Lifecycle | `open_db`, `close`, `ping` |
| Collections | `create_collection`, `list_collections`, `drop_collection` |
| CRUD | `insert`, `insert_many`, `find`, `find_one`, `update`, `update_one`, `delete`, `delete_one`, `count_docs` |
| Indexes | `create_index`, `create_unique_index`, `create_composite_index`, `create_text_index`, `create_ttl_index`, `list_indexes`, `drop_index` |
| Aggregation | `aggregate` |
| Transactions | `transaction` (do-block), `begin_tx`, `commit_tx`, `rollback_tx` |
| Full-text search | `text_search` (documents), `search` (blobs) |
| Blob storage | `create_bucket`, `list_buckets`, `delete_bucket`, `put_object`, `get_object`, `head_object`, `delete_object`, `list_objects` |
| OxiScript | `compile_oxiscript`, `create_procedure`, `call_procedure`, `list_procedures`, `get_procedure`, `delete_procedure` |
| SQL | `sql` |
| Maintenance | `compact` |

Detailed usage for each group — with the same call shapes used here — is in the
[shared API reference](../OxiDb/README.md#api-reference).

```julia
transaction(db) do
    insert(db, "ledger", Dict("action" => "debit",  "amount" => 100))
    insert(db, "ledger", Dict("action" => "credit", "amount" => 100))
end   # auto-commits; auto-rolls back if the block throws
```

## Tables.jl / DataFrames interop

`find` and `aggregate` return an `OxiDbResult` that walks like a `Vector` of
row `Dict`s *and* satisfies the [Tables.jl] row-access interface. So it flows
straight into DataFrames, CSV, MLJ, Plots — anything that consumes a
Tables.jl table. Heterogeneous-schema documents are handled automatically:
missing fields become `missing` in the materialized column.

```julia
using OxiDbEmbedded, DataFrames

rows = find(db, "users", Dict("age" => Dict("\$gte" => 18)))

length(rows)        # walks like a Vector{Dict}…
rows[1]["name"]

DataFrame(rows)     # …and like a Tables.jl table — no manual conversion
```

[Tables.jl]: https://github.com/JuliaData/Tables.jl

## Errors

```julia
try
    insert(db, "users", Dict("email" => "dup@test.com"))
catch e
    e isa OxiDbError                 && println("database error: ", e.msg)
    e isa TransactionConflictError   && println("OCC conflict: ", e.msg)
end
```

## License

See [LICENSE](../../LICENSE).
