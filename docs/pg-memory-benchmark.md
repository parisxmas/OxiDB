# Memory: OxiDB vs PostgreSQL over the same 1,200,000 rows

Both engines get the same schema, the same rows, and the same client. OxiDB
speaks the PostgreSQL v3 wire ([ADR-0023](decisions/0023-postgres-wire-protocol.md)),
so `psql` loads both from one file over one protocol — nothing about the load
path differs.

```bash
bench/pg-memory/run.sh [workdir]
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

## How memory is measured

macOS `phys_footprint` (what Activity Monitor calls Memory), summed over each
engine's process family. **Not `ps rss`**: a shared page is counted once per
process that maps it, so summing RSS over PostgreSQL's nine processes counts
`shared_buffers` nine times. The script prints both, and the gap is the reason
to distrust the RSS column — at boot it reads 64 MB against a true 40 MB.

PostgreSQL 18.4 runs stock: `shared_buffers=128MB`, `max_connections=100`.

## Results

| | PostgreSQL 18.4 | OxiDB (resident) | OxiDB (disk-first) |
|---|---:|---:|---:|
| Boot, empty database | 40 MB | **4 MB** | **4 MB** |
| After loading | 69 MB | 522 MB | **68 MB** |
| Restart, before any query | 26 MB | *4 MB** | *4 MB** |
| **Warm — every table read** | 106 MB | 370 MB | **36 MB** |
| **+ every index used once** | 106 MB | 520 MB | **36 MB** |
| Processes | 9 | **1** | **1** |
| Load time | **5.7 s** | 21 s | 22 s |
| Data directory on disk | 511 MB | 173 MB | **169 MB** |

\** Lazy open — see below. This number is real but does not mean what it looks
like, and should not be quoted on its own.

**In disk-first mode OxiDB now uses about a third of PostgreSQL's memory** —
36 MB against 106 — and, like PostgreSQL, exercising every index does not move
it.
Resident mode is unchanged by design: it keeps rows, indexes and keys in RAM
because that is what it is for.

### What changed, and what it cost

The first run of this benchmark measured 423 MB warm in disk-first mode and
611 MB after the load. Nine changes, each measured on the same dataset, took
those to **36 MB** and 68 MB:

| Change | Effect |
|---|---|
| Sorted inline posting lists instead of a `BTreeSet<u64>` per index key | a key matching one row cost ~150 bytes to say so |
| `SmallVec` key tuples instead of a heap `Vec` per key | one allocation per key in every index and PK map |
| Unboxed `i64` primary-key map for single-column integer keys | 103 → 34 bytes per row |
| Fold the replayed WAL tail at open (disk-first) | a 55 MB WAL tail cost 60 MB of resident overlay |
| Build secondary indexes on first use, not at open | 318 MB for three indexes, paid before answering anything |
| Stop caching statements that carry their values inline | **~250 MB** of parsed bulk `INSERT`s, for a hit rate of zero |
| **Disk-backed secondary indexes** (`.sidx`) | 318 MB resident became 41 MB on disk; using every index became free |
| **Disk-backed primary keys** (`$pk.sidx`) | 163 → 58 MB |
| **Disk-backed `UNIQUE` columns** (`$uq<pos>.sidx`) | the last per-row resident structure: **58 → 36 MB** |

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

What is left resident, and why it is small: the row-offset index of the mmap'd
`.rdat`, the catalog, and the post-checkpoint overlays. **No key set of any kind
is held in memory** — primary keys, `UNIQUE` columns and secondary indexes are
all mapped files.

## What OxiDB does win

- **Startup floor**: 4 MB against 40 MB for an empty database, and one process
  against nine. For many small databases per host — the OxiBase multi-tenant
  case — that per-instance floor matters more than the warm figure.
- **Disk**: 169 MB against 511 MB for the whole data directory. The `.sidx`
  files are part of that — the memory did not vanish, it moved somewhere the OS
  can reclaim.
- **Memory, now**: 36 MB against 106 MB, on the same data, with every index
  used. It used to be 4× the other way.

## What this benchmark does not measure

Query speed (see [the wire benchmark](wire-benchmark.md)), concurrency, larger
datasets, or a tuned PostgreSQL. It also flatters neither engine on load time:
5.7 s against 22 s is real, and materializing indexes and key sets at every
checkpoint is part of why OxiDB's is slower. Both went through `psql` with multi-row `INSERT`s —
PostgreSQL's `COPY` would be far faster and OxiDB has no equivalent.

The honest summary is that OxiDB in disk-first mode now holds no per-row
structure in memory at all: rows, secondary indexes and primary keys are files
it maps, and what stays resident is bounded by the checkpoint interval rather
than by row count. That is the same property PostgreSQL gets from its buffer
pool, reached a different way — whole-file rewrites at checkpoint instead of
in-place pages — and it is why the two warm rows are now flat for both engines.

Resident mode still trades memory for speed on purpose, and is the right default
for databases that fit comfortably in RAM.
