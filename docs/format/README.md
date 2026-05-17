# On-disk format specs

This folder documents every file type OxiDB writes to disk: byte
layout, versioning, and the rules for what can change without breaking
1.0 backward-compatibility.

## Why this folder exists

Phase 1 of [ADR-0003](../decisions/0003-1.0-stability-scope.md)
requires that every on-disk file type used by the 1.0 **stable
surface** has a written spec — both so the engine can grow versioned
headers + "refuse-newer / read-older" logic, and so downstream tools
(backup, recovery, forensics) can be written without reverse-engineering
Rust code.

These specs are descriptive, not prescriptive: they record the format
the engine currently writes. When the format changes, the spec (and a
matching format-version increment) changes with it.

## Index

| File type | 1.0 status | Spec |
|---|---|---|
| `.btree` (B-tree storage) | **Stable** | [btree.md](btree.md) |
| `.wal` (write-ahead log) | **Stable** | [wal.md](wal.md) |
| `_blobs/<bucket>/<id>.{data,meta}` (blob storage) | **Stable** | [blob-object.md](blob-object.md) |
| `.fidx` (field index) | Stable | _spec pending — see `src/index_persist.rs`_ |
| `.cidx` (composite index) | Stable | _spec pending — see `src/index_persist.rs`_ |
| `_tx_commit_log` (transaction commit log) | Stable | _spec pending — see `src/tx_log.rs`_ |
| `_fts/index.json` (document FTS) | Stable | _spec pending — see `src/fts.rs`_ |
| `.vidx` (vector index) | Experimental | _intentionally unspecced — see ADR-0003_ |
| `_archive/segments/*.seg` + `manifest.json` (PITR) | Experimental | _intentionally unspecced — see ADR-0003_ |

(See [ADR-0003 §"1.0 covers" / §"NOT covered by 1.0"](../decisions/0003-1.0-stability-scope.md#decision)
for what "stable" vs "experimental" buys you here.)

## Versioning convention (in force after Phase 1 closes)

Every stable file type carries:

1. **Magic** — a fixed 4-byte ASCII prefix unique to the file type
   (e.g. `"OXBT"` for `.btree`). Lets a tool tell file types apart and
   rejects "I pointed it at a JPEG" mistakes early.
2. **Format-version u16 LE** — increments on every format change to
   that file type.
3. **Reserved 2-byte padding** for future flag bits without bumping
   the version (e.g. `compressed`, `encrypted-at-rest`).

Total file header: 8 bytes (`magic + version + flags`). Files
predating Phase 1 don't have this header and are detected by the
absence of magic; the engine reads them in "legacy mode" and rewrites
them with a header on the next `persist()`.

## Compatibility rules

The rules for *what kind of format change requires what kind of
release* come from [ADR-0004 decision 5](../decisions/0004-phase-0-answers.md)
applied to file formats:

**Additive (allowed in any 1.X minor — engine reads older formats):**
- Adding new optional records / fields whose absence preserves prior
  read behavior
- Adding new flag bits in the reserved padding, defaulting to 0
- Tightening write-time validation that older writers would have
  passed (no on-disk change)

**Breaking (requires a 2.0 release + a `oxidb migrate` step):**
- Changing the byte layout of an existing record type
- Removing or repurposing an existing magic / version / record-type byte
- Changing the semantics of an existing field on values older writers
  would have produced

**Greyish, with explicit rules:**
- Compression / encryption flags can be added per-file (recorded in
  the flag bits); engine MUST read uncompressed/unencrypted files
  forever.
- A new file type within an existing collection's data dir is
  additive; an existing file type changing on-disk *shape* is breaking.

## How to add a new spec

1. Pick the file type and confirm it's 1.0-stable per ADR-0003.
2. Write `docs/format/<name>.md` following the section structure of an
   existing spec (Status, File layout, Versioning, Compatibility, Code refs).
3. Update the index in this README.
4. Open a PR. If the spec exposes a previously-undocumented
   inconsistency, file a follow-up issue rather than fixing it in the
   spec PR.
