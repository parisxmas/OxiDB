# Memory: OxiDB vs PostgreSQL over the same 1,200,000 rows

Both engines get the same schema, the same rows, and the same client. OxiDB
speaks the PostgreSQL v3 wire ([ADR-0023](decisions/0023-postgres-wire-protocol.md)),
so `psql` loads both from one file over one protocol — nothing about the load
path differs.

```bash
bench/pg-memory/run.sh [workdir]     # process memory, both engines, all phases
bench/pg-memory/fair.sh [workdir]    # total physical, incl. page cache
```

## The dataset

1,200,000 rows across five related tables, chosen so every index shape that
costs an engine memory is present ([`schema.sql`](../bench/pg-memory/schema.sql),
[`gen.py`](../bench/pg-memory/gen.py), fixed seed):

| Table | Rows | Keys and indexes |
|---|---:|---|
| `customers` | 200,000 | surrogate PK, `UNIQUE` email, 1 single + 1 multi-column index |
| `products` | 50,000 | surrogate PK, `UNIQUE` sku, 1 single + 1 multi-column index |
| `orders` | 400,000 | surrogate PK, **FK** → customers, 1 single + 1 multi-column index |
| `order_items` | 300,000 | **composite PK** `(order_id, line_no)`, **FK** → orders, 1 index |
| `inventory` | 250,000 | **composite PK** `(product_id, warehouse)`, **FK** → products, 1 index |

Five primary keys (two composite), three enforced foreign keys, two unique
columns, eight secondary indexes (three multi-column).

## How memory is measured — and why one number is not enough

Three quantities matter, and no single tool reports all of them:

| | What it is | Tool |
|---|---|---|
| **Process** | anonymous memory charged to the engine's processes — malloc, and PostgreSQL's `shared_buffers` | macOS `phys_footprint`, summed over the process family |
| **Page cache** | the engine's data files resident in kernel memory | [`cached.rs`](../bench/pg-memory/cached.rs), `mincore(2)` over the data directory |
| **Total** | the two added: what the machine is actually holding | — |

**`phys_footprint` alone is not a fair comparison, and an earlier version of
this document made that mistake.** It deliberately excludes clean file-backed
pages, because the OS may evict them. That is defensible on its own terms, but
it means an engine that mmaps its data has that data go uncounted, while one
that copies data into anonymous shared memory is charged in full. Moving
OxiDB's indexes into mmap'd files therefore *looked* like a 3× win on that
metric when much of it was a change of accounting column.

`ps rss` is not the answer either: it counts a shared page once per process
that maps it, so summing it over PostgreSQL's nine processes counts
`shared_buffers` nine times — 131 MB reported against a true 106.

PostgreSQL 18.4 runs stock: `shared_buffers=128MB`, `max_connections=100`.

## Results

Warm — every table read and every index used once:

| | PostgreSQL 18.4 | OxiDB (disk-first) |
|---|---:|---:|
| Process (anonymous) | 108 MB | **35 MB** |
| Data files in page cache | 189 MB | **168 MB** |
| **Total physical** | **297 MB** | **203 MB** |
| Of which evictable | 189 MB (64%) | **168 MB (83%)** |
| Data directory on disk | 511 MB | **169 MB** |
| Processes | 9 | **1** |

Process-only figures, for the record and *not* as a headline:

| | PostgreSQL 18.4 | OxiDB (resident) | OxiDB (disk-first) |
|---|---:|---:|---:|
| Boot, empty database | 40 MB | 4 MB | 4 MB |
| After loading | 69 MB | 522 MB | 68 MB |
| Warm — every table read | 106 MB | 370 MB | 36 MB |
| + every index used once | 106 MB | 520 MB | 36 MB |
| Load time | **5.7 s** | 21 s | 22 s |

### What the honest comparison says

**On total physical memory OxiDB uses about a third less** — 203 MB against
297 — not the 3× the process column suggests. Most of that advantage is simply
that its data directory is a third the size, so there is less to cache.

**The real structural difference is what can be given back.** OxiDB's 35 MB is
its whole non-evictable footprint; the other 168 MB is clean file-backed pages
the kernel can drop under pressure. PostgreSQL's 108 MB *is* `shared_buffers`
and a per-backend allocation — anonymous memory it will not return whatever the
machine needs. So under memory pressure OxiDB shrinks to ~35 MB and keeps
working from disk, while PostgreSQL's floor stays near its configured cache.

That is a genuine property, and it is the one the earlier "36 MB against
106 MB" line was accidentally describing. Stated properly: **OxiDB's
non-evictable floor is about a third of PostgreSQL's; its total is about
two-thirds.**

### What changed, and what it cost

The first run of this benchmark measured 423 MB of process memory warm in
disk-first mode and 611 MB after the load. Nine changes, each measured on the
same dataset, took those to 36 MB and 68 MB — and the total-physical figure to
203 MB:

| Change | Effect |
|---|---|
| Sorted inline posting lists instead of a `BTreeSet<u64>` per index key | a key matching one row cost ~150 bytes to say so |
| `SmallVec` key tuples instead of a heap `Vec` per key | one allocation per key in every index and PK map |
| Unboxed `i64` primary-key map for single-column integer keys | 103 → 34 bytes per row |
| Fold the replayed WAL tail at open (disk-first) | a 55 MB WAL tail cost 60 MB of resident overlay |
| Build secondary indexes on first use, not at open | 318 MB for three indexes, paid before answering anything |
| Stop caching statements that carry their values inline | **~250 MB** of parsed bulk `INSERT`s, for a hit rate of zero |
| Disk-backed secondary indexes (`.sidx`) | 318 MB resident → 41 MB on disk |
| Disk-backed primary keys (`$pk.sidx`) | 163 → 58 MB |
| Disk-backed `UNIQUE` columns (`$uq<pos>.sidx`) | 58 → 36 MB |

The last three moved memory from anonymous to file-backed as well as shrinking
it — which is exactly why the total-physical row above exists.

## Read this before quoting the startup row

**OxiDB's 4 MB after a restart is not efficiency; it is not having opened the
database yet.** The SQL engine is created on the first statement that needs it,
so a freshly restarted server that has been asked nothing is holding a listener
and nothing else. One `SELECT 1` — which touches no table — opens the engine and
jumps it to the warm figure:

```
after boot (no query):            4368 KB
after 'SELECT 1' (engine open):     36 MB
```

This is what made the number worth chasing rather than quoting. Before the
changes below the same `SELECT 1` reached **620 MB**, because opening the engine
replayed the WAL tail into RAM and rebuilt every index and key map before
answering anything.

PostgreSQL's 26 MB at the same point is a server that has already opened its
cluster and is ready to serve. Comparing the two is comparing a shop before it
unlocks with one that is open. The **warm** row is the comparison that means
something.

## What the warm rows say

**PostgreSQL's memory is a configured cap; OxiDB's used to be a function of the
data.** Closing that meant copying the design rather than tuning around it. From
the source:

- **Everything is page-granular.** `src/backend/storage/buffer/README`: buffers
  are found through a partitioned hash from page identifier to buffer, pinned
  by refcount, and evicted by a clock sweep over usage counters. Pins and locks
  protect *whole pages*, never keys or tuples.
- **A unique insert reads the index off disk.** `_bt_doinsert`
  (`src/backend/access/nbtree/nbtinsert.c`) descends the tree via
  `_bt_search_insert`, then `_bt_check_unique` binary-searches the *locked leaf
  page* and follows `_bt_relandgetbuf` to the sibling when duplicates span
  pages. There is no key map to consult — which is exactly what OxiDB's
  `pk_map` was.
- **An index entry is ~20 bytes, on disk.** `IndexTupleData`
  (`src/include/access/itup.h`) is an 8-byte header — a 6-byte heap TID plus
  2 bytes of flags — with the key at a `MAXALIGN` boundary, plus the page's
  4-byte line pointer.

OxiDB now does the same thing in its own idiom. Secondary indexes and primary
keys are written into the checkpoint's generation as sorted `.sidx` files and
served by mmap; only writes since that checkpoint are resident. The file is a
**hint**, not the truth — a candidate is verified against the live row, so a
deleted or re-keyed row corrects itself without any tombstone bookkeeping.

What is left in *anonymous* memory, and why it is small: the row-offset index of
the mmap'd `.rdat`, the catalog, and the post-checkpoint overlays. **No key set
of any kind is held there** — primary keys, `UNIQUE` columns and secondary
indexes are all mapped files.

The pages of those files are still physical memory while they are hot; the
difference is that they are the kernel's to reclaim, not the engine's to hold.
That is the same bargain PostgreSQL makes for its *heap* — it is `shared_buffers`
that is not evictable, and OxiDB no longer has an equivalent.

## What OxiDB does win

- **Startup floor**: 4 MB against 40 MB for an empty database, and one process
  against nine. For many small databases per host — the OxiBase multi-tenant
  case — that per-instance floor matters more than the warm figure.
- **Disk**: 169 MB against 511 MB for the whole data directory. The `.sidx`
  files are part of that — the memory did not vanish, it moved somewhere the OS
  can reclaim.
- **Non-evictable memory**: 35 MB against 108 MB. Under pressure OxiDB gives
  back 83% of what it holds and keeps serving from disk; PostgreSQL's
  `shared_buffers` is not returnable.
- **Total physical**: 203 MB against 297 MB — a third less, largely because
  there is a third as much data directory to cache.

## What this benchmark does not measure

Query speed (see [the wire benchmark](wire-benchmark.md)), concurrency, larger
datasets, or a tuned PostgreSQL. Nor does it measure behaviour *under* memory
pressure, which is where the evictable/non-evictable split above would actually
be tested — that needs a constrained cgroup or container and is the obvious next
experiment. It also flatters neither engine on load time:
5.7 s against 22 s is real, and materializing indexes and key sets at every
checkpoint is part of why OxiDB's is slower. Both went through `psql` with multi-row `INSERT`s —
PostgreSQL's `COPY` would be far faster and OxiDB has no equivalent.

The honest summary is that OxiDB in disk-first mode holds no per-row structure
in anonymous memory at all: rows, secondary indexes, primary keys and `UNIQUE`
columns are files it maps. Its non-evictable floor is about a third of
PostgreSQL's and does not grow with row count; its total physical use is about
two-thirds, mostly because its files are smaller. It is not three times
lighter — that reading came from a metric that did not count the pages it had
moved into files.

Resident mode still trades memory for speed on purpose, and is the right default
for databases that fit comfortably in RAM.
