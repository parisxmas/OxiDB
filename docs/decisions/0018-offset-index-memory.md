# ADR-0018: Shrinking the disk-first offset index (packed DocLocation + fenced mmap index)

**Status:** Accepted for Phase 1 (packed `DocLocation` + fenced `MmapPrimaryIndex`, landed & tested) — 2026-07-21. Phase 2 (wiring the fenced index into the live `btree_storage` path) **Proposed**, deferred.
**Supersedes:** —
**Related:** [ADR-0009](0009-disk-first-storage.md) (disk-first storage),
[ADR-0017](0017-mvcc-lite-read-snapshots.md) (snapshot gate — a Phase-2 interaction),
[`src/storage.rs`](../../src/storage.rs) (`DocLocation`),
[`src/mmap_index.rs`](../../src/mmap_index.rs) (`MmapPrimaryIndex`, fence),
[`src/btree_storage.rs`](../../src/btree_storage.rs) (`DiskBackend`).

## Context

ADR-0009 moved document **bytes** out of RAM (mmap'd `.bdat`), but disk-first
still keeps one structure fully resident that scales linearly with the document
count: the primary offset index `doc_id → DocLocation`, an
`scc::HashMap<u64, DocLocation>` in `DiskBackend`. This index can never be paged
out — it records *that each document exists and where* — so it is the memory
floor disk-first cannot lower.

The cost, per document:
- `DocLocation` was `{ offset: u64, length: u32 }` = 16 B (padded from 12).
- plus the `u64` key and `scc::HashMap` bucket overhead → ~40–48 B/doc all-in.

At scale that floor bites: ~40–50 MB at 1 M docs (fine), but **~4–5 GB at 100 M
docs** — and unlike the LRU caches (bounded) or the mmap page cache
(reclaimable), it is not recoverable under memory pressure. MongoDB/WiredTiger
has no equivalent resident floor: its `_id` index is a B-tree on disk, paged
into a bounded cache, so its resident cost is the *working set*, not the *doc
count*.

The question this ADR answers: **how do we keep disk-first's fast, no-disk
primary lookup while removing (or bounding) the per-document resident floor?**

## Options considered

Cheap (shrink the resident entry, keep it fully resident):
1. **Drop `length` from the index** — it is redundant with the `.bdat` record
   header (`[status:1][len:4][payload]`). Read it from the header; keep a
   running live-bytes counter for compaction. Saves ~⅓, changes accounting hot
   paths.
2. **Bit-pack `offset`+`length` into one `u64`.** Keeps `length` available,
   minimal churn, but imposes bit-width limits.
3. Compact map (open-addressing / sorted `Vec`) instead of `scc` — less
   overhead, loses lock-free concurrency.

Structural (remove/bound the floor):
4. **Sparse/block ("fence") index** — one resident offset per N-doc block +
   an intra-block scan on mmap. 64–256× smaller resident, ~1 extra page touch,
   but still linear (smaller constant) and needs an overlay for records an
   update moved to a later block.
5. **Move the index itself to disk (mmap'd, like `.mfidx`/`.mcidx`).**
   Eliminates the resident floor entirely (only hot pages resident,
   OS-reclaimable); zero-startup; O(log n) lookup + cold page faults. This is
   the MongoDB/WiredTiger model — and OxiDB already had dormant machinery for
   it: `MmapPrimaryIndex` / the `.pidx` format, built earlier and set aside in
   favour of the resident `SccMap` for O(1)-no-fault lookups.
6. Dense `Vec<u64>` keyed by `id − base` for monotonic ids — 8 B/doc, no hash
   overhead, but delete-gaps + id-range assumptions.
7. Shard (OxiPool) — bounds per node, not total.
8. TTL/retention — bounds the *live* doc count so 100 M is never reached (the
   coldchain telemetry case), sidestepping rather than solving.

## Decision

Two complementary changes.

### A. Pack `DocLocation` into a single `u64` (options #1 + #2)

`DocLocation` becomes a newtype `struct DocLocation(u64)`: the byte offset in
the **high 40 bits** (≤ 1 TiB per `.bdat`) and the payload length in the **low
24 bits** (≤ 16 MiB per document — already the wire message ceiling). Accessors
`offset()` / `length()`; `new(offset, length)` packs; `fits(offset, length)`
guards.

- Offset-index entry: **24 B → 16 B** (u64 key + 8 B value), halving the linear
  floor: ~4–5 GB → **~2.7–3.3 GB** at 100 M docs.
- Reads are unaffected (`length` still available for `read_lockfree`'s mmap
  slice); accounting is unchanged (accessor rename only).
- The append paths (`Storage::write_record`, the in-memory batch appends)
  **reject** a record that exceeds the packed limits (`DocLocation::fits`)
  rather than truncating it into a wrong read.

The 1 TiB / 16 MiB ceilings are far above realistic single-collection usage
(100 M × 250 B ≈ 25 GB), and an over-limit write fails loudly instead of
corrupting. This is the pragmatic bound-and-guard the packing inherently
requires.

### B. Hybrid fenced mmap primary index (#4 fence over #5 mmap)

The structural fix is a **hybrid**: the full `doc_id → location` index lives on
disk (mmap'd — resident cost is only hot pages, not the doc count), with a small
resident **fence** on top so a lookup does not fault a page per binary-search
probe.

Implemented on the existing `MmapPrimaryIndex` (`.pidx`), bumped v2 → v3:
- A **fence section** at the file tail — `FENCE_STRIDE` (128), `FENCE_COUNT`,
  then every 128th entry's `doc_id`. Written by every `.pidx` writer.
- On open, the fence is loaded into a resident `Vec<u64>` with **one sequential
  read** (startup stays O(fence), not O(entries)).
- A lookup binary-searches the resident fence in RAM to a single ~1-page block,
  then binary-searches that block on the mmap → **~1 cold page fault instead of
  O(log n)**. An absent/old fence section falls back to a full binary search.

Resident cost at 100 M docs: **~6 MB** (fence) with the full ~2.8 GB index on
disk, vs. ~1.6–3.3 GB fully resident. This keeps disk-first's "primary lookup
never blocks on disk" identity largely intact (fence-locate is resident and
O(1)-ish; only the block probe touches the mmap).

Why hybrid rather than plain #5: OxiDB deliberately chose the resident `SccMap`
over the plain mmap `.pidx` earlier, for O(1) no-fault lookups. The fence buys
back most of that speed (bounded faults) while getting #5's flat memory — so the
hybrid, not a straight revert to the mmap index, is the right point on the
curve. Plain #4 was rejected as the *sole* answer because it stays linear and
needs an overlay for update-moved records; layering the fence over the on-disk
index gets both flat memory and fast locate.

## Phasing

- **Phase 1 (done, this ADR's accepted scope):** the packed `DocLocation` and
  the fenced `MmapPrimaryIndex` — additive and independently tested. The fenced
  index is **not yet wired** into the live disk-first path (`DiskBackend` still
  uses the resident `SccMap`), so there is no behaviour change yet; the memory
  win from B lands in Phase 2.
- **Phase 2 (proposed, deferred):** wire the fenced `MmapPrimaryIndex` into
  `btree_storage`'s `DiskBackend` behind a flag, default unchanged. This is
  crash-consistency-critical (it is the primary index) and touches:
  - the lookup/mutation surface (`get`/`insert`/`remove`/`contains`/`len`/
    `clear`) and the **7 full-scan `iter_sync` sites** — which need a **streaming
    `for_each_entry`** over mmap+overlay so a scan never materialises all entries
    (2.8 GB at 100 M);
  - open (load `.pidx` vs. scan the `.bdat`), checkpoint (persist `.pidx`),
    recovery (WAL replay → overlay; a crash between the `.bdat` append and the
    `.pidx` persist must rescan the `.bdat` tail);
  - compaction (rewrites the `.bdat` → every offset changes → rebuild `.pidx`);
  - the ADR-0017 snapshot gate (displaced-bytes recording interacts with the
    index).
  Landed flag-gated with crash tests and a memory/latency benchmark before the
  default flips.

## Consequences

- **Memory:** offset index 24 B → 16 B/entry now (A); in Phase 2, the resident
  floor drops to the fence (~6 MB/100 M docs) with the index on disk (B).
- **New hard limits (A):** a single `.bdat` ≤ 1 TiB and a document ≤ 16 MiB.
  Above either, the write errors rather than corrupting. Both are well beyond
  current usage; a collection that needs more should shard (option #7).
- **Lookup latency (Phase 2, B):** resident O(1) hash → fence-locate + ~1 cold
  page fault. Acceptable for the target scale; the fence bounds the regression.
- **Backward compatibility:** `.pidx` v2 files read as "absent" (full rebuild on
  next persist), as before; the packed `DocLocation` is an in-memory
  representation only — no `.bdat`/`.pidx` on-disk record layout changed.
- Verified: 867 lib tests green, incl. `doclocation_packs_into_one_word`,
  `fenced_lookup_across_blocks_and_gaps`, and a disk-first mmap-backed TTL
  eviction regression test. Landed in commit `a9acb677`.
