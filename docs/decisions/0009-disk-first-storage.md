# ADR-0009: Disk-first storage mode (opt-in)

**Status:** Accepted (opt-in; default remains in-RAM) — 2026-05-30
**Supersedes:** —
**Related:** [`src/btree_storage.rs`](../../src/btree_storage.rs),
[`src/storage.rs`](../../src/storage.rs),
[`tests/mem_probe.rs`](../../tests/mem_probe.rs)

## Context

The default `BTreeStorage` keeps every document's bytes resident in RAM in an
`scc::HashMap<u64, Vec<u8>>` (the `.btree` file is a periodic snapshot, reloaded
in full on open). So OxiDB is effectively an in-memory database with disk
persistence: resident memory scales linearly with the dataset (~370 B/doc of
payload + container overhead → ~400 MiB for 1M docs, before indexes/caches).
The 1M-doc MongoDB benchmark quantified this — on a memory-tight host the
resident dataset plus indexes plus transient query buffers can OOM, whereas
MongoDB/WiredTiger is disk-first with a bounded page cache and stays within its
configured budget (see the 1M-doc memory probe analysis).

Earlier, cheaper levers were tried first and are in tree: bounding the LRU
caches by a fixed budget (ADR-era cache work) cut the cache layer from ~900 MiB
to ~256 MiB. But the **primary store being fully RAM-resident** is the
structural floor those levers can't move.

## Decision

Add an **opt-in disk-first storage mode** to `BTreeStorage`, enabled by
`OXIDB_DISK_FIRST=1`. In this mode the in-memory map holds only a compact
`doc_id → DocLocation{offset:u64, len:u32}` index (~24 B/doc), and document
bytes live in an **mmap'd append-only data file** read on demand.

Crucially, it **reuses the hardened append-only `src/storage.rs` `Storage`**
backend rather than writing a new one — that component already provides append,
mmap reads (`read_lockfree`), soft-delete, CRC, encryption, and torn-tail
recovery (the latter hardened in the earlier storage-robustness pass). The
disk-first data file is named `{collection}.bdat` (deliberately **not** `.dat`,
which the engine reserves for the genuinely-legacy `Collection` format and
refuses to open).

The in-RAM path is left **byte-identical** — disk mode is an additive
`Option<DiskBackend>` branch on every `BTreeStorage` method, so the default is
unchanged and unaffected.

### Mechanics

- **insert** → append bytes to the data file, record the returned location in
  the index; on update, soft-delete the old location.
- **get** → index lookup → `read_lockfree` from the mmap.
- **remove** → soft-delete + drop from the index.
- **scans / cursors** → iterate the index (sorted where required), reading each
  value from the data file. Cursors materialize their snapshot like the in-RAM
  cursor does (a transient per-query cost); the steady-state win is that the
  resident index holds only locations.
- **persist** → fsync the data file (no separate snapshot to write).
- **open / recovery** → scan the data file's live records, extract each `_id`,
  and rebuild the index. WAL replay continues to reconcile recent writes.

## Consequences

- **Memory:** resident store drops from ~370 B/doc to ~24 B/doc — roughly
  **~400 MiB → ~24 MiB for 1M docs**. The data file's pages are mmap'd and
  faulted in on read; they count as (reclaimable, file-backed) RSS only for the
  working set actually touched, and the OS evicts them under pressure — the
  WiredTiger-style behavior the benchmark wanted.
- **Reads:** a point read costs an mmap lookup instead of a RAM clone — cheap
  for resident pages, a page fault otherwise. The doc/bytes caches (now budget-
  bounded) sit on top as the hot tier.
- **Writes:** append-only means updates/deletes leave dead space reclaimed by
  compaction (the `Storage` machinery); not yet auto-triggered in this mode.
- **Correctness:** the **entire core test suite passes in both modes**
  (`OXIDB_DISK_FIRST=1 cargo test -p oxidb --lib` — CRUD, queries, transactions,
  recovery, backup/restore, TTL, indexes). Backup tars the whole data dir, so
  `.bdat` is included; restore and discovery recognize it.

## Why opt-in (not the new default)

This is the core durability path of a database running real production
workloads. Even with a green test suite, a storage-engine change needs
**soak-testing** (sustained writes, crash injection, compaction under churn)
before it can be trusted as the default. Shipping it opt-in lets it be exercised
and benchmarked in the field with zero risk to existing deployments; flipping
the default is a separate, later decision once it has miles on it.

## Update: disk-first field indexes (2026-05-30)

Under the same `OXIDB_DISK_FIRST` flag, single-field indexes are now disk-backed
too, via the existing `MmapFieldIndex` (mmap'd `.mfidx`, paged in on demand,
with a small in-memory write overlay). `PagedFieldIndex` gained an additive
`Option<MmapFieldIndex>` and delegates every method to it in disk mode — so the
query layer and its ~90 call sites are unchanged. The one exception is
`iter_asc`/`iter_desc` (borrow-vs-owned item types can't be unified): the
count-only `$group` fast paths detect a disk-backed index (`is_disk()`) and
fall back to the hashing path. On reopen the index is **mmap-loaded** (instant,
empty overlay), not rebuilt.

Result: a fresh process opening a 500K-doc collection with 5 indexes sits at
**~7 MiB resident** — both the document store and the indexes are off the
resident heap (faulted in lazily, reclaimable). Full core suite passes in both
modes. Caveats: index *build* still materializes the overlay (a transient
spike, not steady state); the composite index remains in-RAM; and a query that
returns a large result set still materializes it (the read-path transient,
tracked separately).

## Follow-ups before this can be the default

- **Compaction — implemented** (`BTreeStorage::compact` → `OxiDb::compact`):
  rewrites the `.bdat` keeping only live records and atomically swaps it in (the
  `data` handle is an `RwLock<Arc<Storage>>`; normal ops take the read lock
  spanning index-lookup+data-read, compaction takes the write lock, so a
  `DocLocation` is never used against the swapped file). Field indexes are
  doc_id-keyed so the store rewrite doesn't touch them; their `.mfidx` is
  rewritten cleanly by the overlay merge on persist. Soak-verified: 167 KB →
  34 KB (~5×) after heavy update churn, with all live data + indexed queries
  intact through compact and a reopen.
- **Automatic compaction trigger — implemented**
  (`BTreeStorage::should_compact`): the periodic maintenance path
  (`sync_writes`, after `persist`) checks a cheap dead-space heuristic and
  compacts when the `.bdat` is both at least `OXIDB_COMPACT_MIN_BYTES`
  (default 4 MiB) **and** at least `OXIDB_COMPACT_DEAD_RATIO` (default 0.5, i.e.
  ≥50%) dead — `dead_ratio = 1 − live_bytes/file_size`. That maintenance point
  doesn't hold the data lock, so it can safely take compaction's exclusive write
  lock; a compaction resets dead space, so the trigger self-rate-limits. Set
  `OXIDB_AUTO_COMPACT=0` to disable. Field indexes are doc_id-keyed, so the store
  rewrite leaves them valid (no re-index). Soak-verified
  (`auto_compaction_bounds_file_size`, `#[ignore]`d): under 2400 updates to 800
  docs with an *incompressible* ~2 KiB payload, the `.bdat` settles at ~2.8 MiB
  versus ~6.4 MiB uncompacted, with live count + point reads intact.
- Lazy cursors (stream values instead of materializing the snapshot) to also cut
  the transient per-sorted-query spike.
- A 1M-doc bench run in disk mode on a quiet box to confirm the RSS win and
  measure the read-latency trade-off end to end.

### Bulk-insert append batching (2026-05-30)

Disk-first bulk insert (`insert_many_prepared`) appended to the `.bdat` **one
document at a time** — each per-doc `Storage::append_no_sync` compressed a
single record, took the storage mutex, `lseek`'d to the end, and wrote, ×N.
That per-doc compress + unbatched append made disk-first insert ~2.4× slower
than the in-RAM store (and MongoDB), even though the WAL path (one buffered
write + one fsync per batch) was already amortized. The fix routes the storage
write through `BTreeStorage::insert_batch` →
`Storage::append_batch_no_sync_buffered`, which compresses the whole batch in
parallel and writes it with a single lock + seek + `write_all` (in-RAM:
parallel tree fill). 1M-doc benchmark, disk-first: insert **12.1s → 6.24s**
(~1.9×; 2.4× → 1.3× vs MongoDB). Durability unchanged. Server 0.29.2.

### Aggregation / scan / sort fixes (2026-05-30)

A 1M-doc benchmark in disk-first mode surfaced three issues — fixed in server
0.29.1:

- **Index-only count `$group`** bailed for disk-backed indexes
  (`try_index_only_count` returned `None` on `is_disk()`), turning a zero-doc
  index read into a full document scan. `PagedFieldIndex` gained backend-
  agnostic `for_each_entry_asc`/`for_each_entry_desc` callbacks (the borrowed
  `iter_asc`/`iter_desc` are in-RAM-only — a disk-backed index yields nothing
  from them), and the fast path now uses them. Count-only group-by on a 200K
  disk-first collection: **1.56s → 86ms (~18×)**.
- **Index-backed sort silently returned empty in disk-first mode** — it used
  `iter_asc`/`iter_desc`, which yield nothing for a disk-backed index, with no
  fallback. Now uses the `for_each_entry_*` callbacks (mmap-reading,
  early-terminating). Regressioned by
  `disk_first_soak::disk_first_indexed_sort_and_count_group`.
- **Full-collection scans** (`Storage::for_each_payload`) now lock the read mmap
  once, read in **offset order** (sequential sweep, not random index-order
  faults), and **borrow mmap bytes zero-copy** for records needing no decode.
  Caveat: small *compressible* documents stay decompression-bound (each record
  carries a zstd frame, so the zero-copy borrow doesn't trigger and every scan
  re-decompresses) — the in-RAM store keeps raw bytes resident and pays none of
  this. **Resolved** by the uncompressed `.bdat` mode below.

### Per-collection storage options (2026-05-30)

The disk-first knobs started as process-wide env vars (`OXIDB_DISK_FIRST`,
`OXIDB_DISK_UNCOMPRESSED`, `OXIDB_AUTO_COMPACT`/`OXIDB_COMPACT_*`). They are now
per-collection: a `StorageOptions { disk_first, compress, auto_compact,
compact_min_bytes, compact_dead_ratio }` carried on `BTreeStorage`, set via
`OxiDb::create_collection_with_options`. `should_compact`/`compact` and the
`.bdat` compress flag read it instead of the env helpers. `StorageOptions::
from_env()` reproduces the env-var behavior and stays the default for
collections opened/created without explicit options, so nothing about the
existing workflow changes.

For disk-first collections the resolved options are persisted as `<name>.bopts`
and read back on open — a collection's on-disk format is now authoritative
across reopens regardless of the environment (flipping `OXIDB_DISK_FIRST`
between runs can no longer mismatch an existing collection). Collections with no
`.bopts` (created before this change) resolve by detecting the on-disk format
(`.bdat` → disk-first, `.btree` → in-RAM), so old data dirs open unchanged.
This lets one engine mix storage shapes — e.g. a disk-first *uncompressed*
scan/aggregation-heavy collection next to a default in-RAM one. Tested in
`tests/per_collection_options.rs`.

### Uncompressed `.bdat` mode — closes the scan/insert gap (2026-05-30)

`OXIDB_DISK_UNCOMPRESSED=1` (with `OXIDB_DISK_FIRST=1`) writes the data file
without zstd compression — a `compress: bool` on `Storage`
(`open_with_options`), default on; the in-RAM store and the default compressed
disk-first mode are untouched. Uncompressed records skip the per-record
compress on write and decompress on read, and (unencrypted) are read zero-copy
from the mmap by `for_each_payload`. Reads stay adaptive (magic-byte decode), so
no migration is needed and mixed files read back fine; compaction rewrites in
whichever mode is active.

This was the lever the two caveats above pointed at — full scans/aggregations
and index builds were decompression-bound. 1M-doc benchmark, disk-first:

| Operation             | compressed | uncompressed | in-RAM | MongoDB |
|-----------------------|-----------:|-------------:|-------:|--------:|
| Insert                |      6.24s |        5.45s |  5.26s |   ~5.0s |
| Build 8 indexes       |     18.2s  |        2.43s |  1.53s |    4.6s |
| Group by dept + avg   |      2.54s |        248ms |  280ms |   360ms |
| Group by city + stats |      2.36s |        378ms |  391ms |   305ms |
| Unindexed scan        |      714ms |        582ms |  429ms |   586ms |
| Wins vs MongoDB       |     5 / 18 |     11 / 18  | 12/18  |       — |
| Disk footprint        |       689M |         727M |   612M |    323M |

Uncompressed disk-first wins 11/18 (≈ in-RAM's 12/18) — aggregations ~10×
faster (now beating/tying MongoDB), index build ~7× — while keeping the
low-resident-memory property. Disk grew only ~5% (689M→727M): zstd bought almost
nothing on these small structured docs while costing ~10× on every scan. For
genuinely compressible corpora the default compressed mode is still preferable;
uncompressed is the choice for scan/aggregation-heavy workloads where disk is
cheap relative to memory and CPU.

### Soak tests (done)

`tests/disk_first_soak.rs` runs in both modes (set `OXIDB_DISK_FIRST=1` to soak
the disk engine; `SOAK_ROUNDS=N` to lengthen):

- `churn_integrity_model_checked` — sustained insert/update/delete vs an
  in-test reference model.
- `index_consistency_under_churn` — an indexed field repeatedly updated;
  indexed counts must match the model (verifies old index entries are removed
  and new ones added).
- `churn_then_clean_reopen` — churn → `shutdown()` → reopen; all data + indexed
  queries survive.
- `crash_recovery_committed_survives` — drop **without** shutdown (simulated
  crash) → reopen; committed writes recovered via WAL replay.
- `bdat_growth_under_update_churn` — correctness under heavy update churn +
  reports `.bdat` growth (the compaction motivation above).
- `compaction_reclaims_space_and_preserves_data` — explicit `compact()` shrinks
  the `.bdat` and keeps all data + indexed queries intact through a reopen.
- `auto_compaction_bounds_file_size` (`#[ignore]`d, timing-based) — with the
  periodic sync thread running, the `.bdat` stays bounded near the live size
  under sustained update churn instead of growing with the write count. Run:
  `OXIDB_DISK_FIRST=1 cargo test --test disk_first_soak auto_compaction -- --ignored --nocapture`.
