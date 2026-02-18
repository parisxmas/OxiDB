# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

OxiDB is a fast, embeddable document database engine written in Rust. It can run as an embedded library, a standalone TCP server, or be accessed via client libraries (Python, Go, Ruby, Java/Spring Boot, PHP, Julia, .NET via C FFI).

## Build Commands

```bash
cargo build --release                    # Build core library
cargo build --release -p oxidb-server    # Build TCP server
cargo build --release -p oxidb-client-ffi # Build C FFI shared library (.dylib/.so/.dll)
cargo build --workspace --features ocr   # Build with OCR support
```

## Testing

```bash
cargo test                               # All tests (workspace)
cargo test -p oxidb                      # Core library tests only
cargo test -p oxidb-server               # Server tests only
cargo test <test_name>                   # Single test by name
cargo test -- --nocapture                # With stdout output
```

Unit tests are inline (`#[cfg(test)]` modules in source files). Integration tests for the server are in `oxidb-server/tests/` (ACID and security tests).

## Linting / Formatting

```bash
cargo fmt
cargo clippy
```

## Workspace Structure

Cargo workspace with three crates:
- **Root (`oxidb`)** — core database library
- **`oxidb-server/`** — TCP server with SCRAM-SHA-256 auth, RBAC, TLS, audit logging
- **`oxidb-client-ffi/`** — C-compatible FFI (`cdylib`) for language bindings

Client libraries: `python/`, `go/`, `ruby/`, `php/`, `julia/`, `dotnet/`

## Architecture

### Core Engine (`src/engine.rs`)
`OxiDb` owns a `RwLock<HashMap<name, Arc<RwLock<Collection>>>>`. Per-collection locking enables concurrent reads. Collections are auto-created on first insert.

### Collection (`src/collection.rs`)
Each collection owns:
- **Storage** (`storage.rs`) — append-only file with `[status:u8][length:u32 LE][payload]` records; soft-delete flips status byte in-place
- **WAL** (`wal.rs`) — write-ahead log with CRC32 checksums; entries tagged with transaction IDs; 3-fsync protocol (WAL → data → checkpoint)
- **In-memory document cache** — `HashMap<DocumentId, Arc<Value>>` (JSON deserialized once, then refcounted)
- **Field indexes** — `BTreeMap<IndexValue, BTreeSet<DocumentId>>` per indexed field
- **Composite indexes** — multi-field B-tree for prefix scans
- **Version map** — per-document version counters for OCC

### Query Engine (`src/query.rs`, `src/pipeline.rs`)
Query AST with field conditions and logical operators ($and, $or). Operators: $eq, $ne, $gt/$gte/$lt/$lte, $in, $exists. Key optimizations:
- Index-backed sort: BTreeMap iteration is O(limit) instead of O(n log n)
- Index-only count: returns set size without touching documents
- Early termination: `update_one`/`delete_one` stop after first match

Aggregation pipeline: $match, $group, $sort, $skip, $limit, $project, $count, $unwind, $addFields, $lookup.

### Updates (`src/update.rs`)
Field operators ($set, $unset, $inc, $mul, $min, $max, $rename, $currentDate) and array operators ($push, $pull, $addToSet, $pop). Supports dot-notation for nested fields.

### Indexes (`src/index.rs`, `src/value.rs`)
`IndexValue` enforces cross-type ordering: Null < Bool < Num < DateTime < String. Dates are auto-detected from ISO 8601/RFC 3339/YYYY-MM-DD strings and stored as epoch ms.

### Transactions (`src/transaction.rs`, `src/tx_log.rs`)
OCC with 3-phase commit: prepare → validate versions → commit. Writes are buffered until commit. Deadlock-free via sorted collection locking (BTreeSet). Recovery uses transaction log + WAL replay on startup.

### Full-Text Search (`src/fts.rs`)
Background worker thread receives indexing jobs via `sync_channel(256)`. Supports HTML, XML, JSON, PDF, DOCX, XLSX, images (OCR with `ocr` feature). TF-IDF ranking. Persisted as `_fts/index.json`.

### Blob Storage (`src/blob.rs`)
S3-style bucket interface. Objects stored as `_blobs/<bucket>/<id>.data` + `<id>.meta`. CRC32 etags.

### Encryption (`src/crypto.rs`)
Transparent AES-GCM encryption at the storage layer. Optional—enabled by passing an encryption key to the engine.

### Server Protocol (`oxidb-server/`)
Length-prefixed JSON over TCP (max 16 MiB). Auth via SCRAM-SHA-256. RBAC roles: Admin, ReadWrite, Read. Configurable via env vars:
- `OXIDB_ADDR` (default `127.0.0.1:4444`)
- `OXIDB_DATA` (default `./oxidb_data`)
- `OXIDB_POOL_SIZE` (default 4 worker threads)
- `OXIDB_IDLE_TIMEOUT` (default 30s, 0 = never)
