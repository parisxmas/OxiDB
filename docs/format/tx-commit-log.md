# `_tx_commit_log` — Transaction commit log

**1.0 status:** Stable (per [ADR-0003](../decisions/0003-1.0-stability-scope.md))
**Written by:** `src/tx_log.rs` — `committer_loop` (background thread,
group-commit batched), `persist`
**Read by:** `src/tx_log.rs` — `parse_log` (on open),
`read_committed`

## Purpose

A single file at `<data_dir>/_tx_commit_log` that records the set of
**committed transaction IDs**. WAL replay during recovery consults
this set to decide which transactional `tx_id != 0` records to apply
and which to drop (uncommitted / rolled-back transactions, see
[`wal.md`](wal.md#payload-format)).

## File layout

A concatenated array of `u64 LE` transaction IDs — nothing else:

```
┌─────────────┬─────────────┬─────────────┬─── ... ────┐
│ tx_id 0 u64 │ tx_id 1 u64 │ tx_id 2 u64 │            │
│ LE          │ LE          │ LE          │            │
└─────────────┴─────────────┴─────────────┴────────────┘
   8 bytes      8 bytes       8 bytes
```

- File size is always a multiple of 8.
- IDs are written **sorted** (`ids.sort_unstable()` in `persist`) for
  reproducible on-disk content — keeps tests deterministic and makes
  hex-diffing post-recovery state across runs cheap.
- The order in the file does NOT match commit order; the file is a
  *set*, not a log of commit events. The single canonical
  representation is "sorted u64s".

There is **no file-level header** in the current format. Adding one
is the Phase 1b task for this file type.

## Write protocol — group commit

The committer is a dedicated thread (`oxidb-tx-commit`) consuming a
channel of commands (`Mark` / `Remove` / `Read`). It batches commands
(`MAX_BATCH = 1024`) and performs **one `sync_data()` per batch**, so
N concurrent commit-or-rollback calls share a single fsync.

`persist` rewrites the whole file every batch:

1. Collect all live IDs from the in-memory `HashSet`.
2. Sort them.
3. `file.set_len(0)` + `seek(0)` + `write_all(buf)` + `sync_data()`.

This is intentionally simple — the file stays small (a few KB at most
for typical workloads, since IDs are removed on commit completion in
most paths), and rewriting it on every batch is cheaper than
append-with-occasional-compaction logic.

Callers block on `mark_committed` / `remove_committed` until the
enclosing batch has fsynced — durability semantics are identical to
"every call fsyncs", with the fsync shared across the batch. See the
batched-fsync rationale in the [WAL section of the
README](../../README.md#write-ahead-log).

## Encryption

The file is **not encrypted** in the current code path. Transaction
IDs are opaque numeric tokens with no document content — their value
is operationally low-sensitivity. If the data directory's host
filesystem is encrypted at rest (the recommended deployment for
sensitive datasets), the file inherits that protection.

If application-level encryption of this file becomes a requirement,
the Phase 1b header (see "Versioning" below) leaves room for an
`encrypted` flag bit.

## Versioning

The current format has no header. Phase 1b will introduce one:

```
┌──────────────┬──────────────┬──────────────┐
│  magic (4B)  │  version u16 │  flags u16   │   <- new 8-byte file header
│  "OXTX"      │  LE = 1      │  LE = 0      │
└──────────────┴──────────────┴──────────────┘
... then the sorted u64-LE id stream as above ...
```

- Engine reads version `1` AND legacy header-less files (detected by
  the absence of `OXTX` magic at offset 0).
- Legacy detection is unambiguous: a legacy file starts with the
  first 4 bytes of a `tx_id`. Transaction IDs are allocated from a
  counter starting at 1, so the first 4 bytes will be small (`01 00
  00 00`, `02 00 00 00`, …) — `b"OXTX"` (`4F 58 54 58`) is far outside
  that range, so the discrimination is solid until tx_id exceeds
  ~`0x4F580000`. By that point the engine has rewritten the file
  many times and the header is there.
- On next `persist()`, a legacy file is rewritten with a v1 header.
- Engine refuses versions it doesn't know.
- `flags` reserved at 0 for now.

## Compatibility rules

**Additive (1.X minor allowed):**
- New flag bits (defaulted to 0). Older readers ignore unknown bits
  *unless* the bit is in a future-reserved "must-understand" range
  documented per bit.
- Adding an *adjacent* file (e.g. `_tx_commit_log.aux` for a future
  audit trail) is fine — different file, different spec.

**Breaking (requires 2.0):**
- Changing the entry size or encoding from `u64 LE`.
- Repurposing `OXTX` magic, the version byte, or the file's name.
- Changing the "set, sorted on disk" invariant — code already
  depends on it (tests, reproducibility).

## Isolation level (observed)

OxiDB's OCC validates the **write set** at commit time; reads
inside a tx see the latest committed data rather than a
`begin_transaction`-time snapshot. Empirically (pinned in
[`tests/cern_acid_isolation.rs`](../../tests/cern_acid_isolation.rs)):

| Anomaly | Occurs? | Why |
|---|---|---|
| Dirty read | ❌ | Writes are buffered until commit; uncommitted data never reaches the visible state |
| Lost update | ❌ | Write-set conflict detection — second commit gets `Error::TransactionConflict` |
| Phantom read | ✅ | Reads see committed data, not a snapshot — a concurrent committed insert appears in a later predicate read inside the same tx |
| Write skew (A5B) | ✅ | Two txs writing to *different* docs with a cross-row constraint both commit — OCC sees no write-write conflict |

In ANSI SQL terms this is **read committed** with OCC-mediated lost-
update prevention — between read-committed and snapshot isolation.
Equivalent to PostgreSQL's `READ COMMITTED` plus serializable-update
protection.

If/when serializable snapshot isolation (SSI) lands, the phantom-
read and write-skew assertions in `cern_acid_isolation.rs` flip and
those flips are the intentional documentation that the engine has
been promoted.

## Code refs

- `src/tx_log.rs:75` — `pub fn open(...)`
- `src/tx_log.rs:97` — `pub fn mark_committed(...)`
- `src/tx_log.rs:122` — `pub fn remove_committed(...)`
- `src/tx_log.rs:146` — `pub fn read_committed(...)`
- `src/tx_log.rs:201` — `fn parse_log(...)` (the reader)
- `src/tx_log.rs:222` — `fn persist(...)` (the writer)
- `src/tx_log.rs:233` — `fn committer_loop(...)` (group-commit thread)
