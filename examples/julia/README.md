# OxiDB Julia Examples

Two ways to use OxiDB from Julia:

| Example | Mode | Server needed? |
|---------|------|----------------|
| `embedded_example.jl` | Embedded (in-process via FFI) | No |
| `example.jl` | TCP client | Yes |

## Embedded Mode (recommended)

Uses `ccall` to the prebuilt `liboxidb_embedded_ffi` library directly — no server needed. The library is **automatically downloaded** on first run.

```bash
julia embedded_example.jl
```

No setup required. The script:
1. Auto-installs the `JSON3` package if missing
2. Auto-downloads the prebuilt native library from [GitHub Releases](https://github.com/parisxmas/OxiDB/releases/latest) into `lib/` on first run

Subsequent runs use the cached library.

### Supported platforms

| Platform | Architecture | Status |
|----------|-------------|--------|
| macOS | arm64 (Apple Silicon) | Prebuilt available |
| macOS | x86_64 | Build from source |
| Linux | x86_64 | Build from source |

To build from source instead:

```bash
cargo build --release -p oxidb-embedded-ffi
```

Then set `LIB_PATH` in the script to point to `target/release/liboxidb_embedded_ffi`.

## TCP Client Mode

Requires a running OxiDB server. Download the server binary from [GitHub Releases](https://github.com/parisxmas/OxiDB/releases/latest) or build with `cargo build --release -p oxidb-server`.

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
