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
  intact through compact and a reopen. **Still TODO:** an *automatic* trigger
  (run compaction on a dead-space ratio/size threshold) — today it's an explicit
  `compact` call.
- Lazy cursors (stream values instead of materializing the snapshot) to also cut
  the transient per-sorted-query spike.
- A 1M-doc bench run in disk mode on a quiet box to confirm the RSS win and
  measure the read-latency trade-off end to end.

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
