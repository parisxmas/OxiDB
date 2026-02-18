# OxiDB Julia Examples

Two ways to use OxiDB from Julia:

| Example | Mode | Server needed? |
|---------|------|----------------|
| `embedded_example.jl` | Embedded (in-process via `OxiDbEmbedded`) | No |
| `example.jl` | TCP client (via `OxiDb`) | Yes |

## Embedded Mode (recommended)

Uses the `OxiDbEmbedded` package — no server, no compilation needed.

```bash
julia embedded_example.jl
```

The package auto-downloads the prebuilt native library from [GitHub Releases](https://github.com/parisxmas/OxiDB/releases/latest) on first run.

### Use in your own project

```julia
using Pkg
Pkg.develop(path="julia/OxiDbEmbedded")
```

```julia
using OxiDbEmbedded

db = open_db("/tmp/mydb")
insert(db, "users", Dict("name" => "Alice", "age" => 30))
docs = find(db, "users", Dict("name" => "Alice"))
close(db)
```

### Supported platforms (prebuilt)

| Platform | Architecture | Status |
|----------|-------------|--------|
| macOS | arm64 (Apple Silicon) | Prebuilt available |
| macOS | x86_64 | Build from source |
| Linux | x86_64 | Build from source |

To build from source: `cargo build --release -p oxidb-embedded-ffi`

## TCP Client Mode

Requires a running OxiDB server. Download from [GitHub Releases](https://github.com/parisxmas/OxiDB/releases/latest) or build with `cargo build --release -p oxidb-server`.

```bash
# Start the server
./oxidb-server

# Install dependencies and run
julia --project=. -e 'using Pkg; Pkg.instantiate()'
julia --project=. example.jl
```

## Features Demonstrated

Both examples exercise the full feature set:

- Ping
- Collection management (create, list, drop)
- CRUD (insert, insert_many, find, find_one, update, delete, count)
- Update operators ($set, $inc, $unset, $push, $pull, $addToSet, $rename, $currentDate, $mul, $min, $max)
- Indexes (single-field, unique, composite)
- Aggregation pipeline ($match, $group, $sort, $skip, $limit, $count, $project, $addFields, $lookup, $unwind)
- Transactions (auto-commit and manual rollback)
- Blob storage (buckets, put/get/head/list/delete objects)
- Full-text search
- Compaction

The embedded example additionally demonstrates `update_one` and `delete_one` (embedded-only operations).
