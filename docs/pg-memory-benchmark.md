# Memory: OxiDB vs PostgreSQL over the same 1,000,000 rows

Both engines get the same schema, the same rows, and the same client. OxiDB
speaks the PostgreSQL v3 wire ([ADR-0023](decisions/0023-postgres-wire-protocol.md)),
so `psql` loads both from one file over one protocol — nothing about the load
path differs.

```bash
bench/pg-memory/run.sh [workdir]
```

## The dataset

1,000,000 rows across five related tables, chosen so every index shape that
costs an engine memory is present ([`schema.sql`](../bench/pg-memory/schema.sql),
[`gen.py`](../bench/pg-memory/gen.py), fixed seed):

| Table | Rows | Keys and indexes |
|---|---:|---|
| `customers` | 200,000 | surrogate PK, `UNIQUE` email, 1 single + 1 multi-column index |
| `products` | 50,000 | surrogate PK, `UNIQUE` sku, 1 single + 1 multi-column index |
| `orders` | 400,000 | surrogate PK, **FK** → customers, 1 single + 1 multi-column index |
| `order_items` | 300,000 | **composite PK** `(order_id, line_no)`, **FK** → orders, 1 index |
| `inventory` | 50,000 | **composite PK** `(product_id, warehouse)`, **FK** → products, 1 index |

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
| After loading 1M rows | 70 MB | 770 MB | 611 MB |
| Restart, before any query | 26 MB | *4 MB** | *4 MB** |
| **Warm — every table read** | 105 MB | 370 MB | **162 MB** |
| **+ every index used once** | **106 MB** | 520 MB | 310 MB |
| Processes | 9 | **1** | **1** |
| Load time | **7.2 s** | 23 s | 24 s |
| Data directory on disk | 504 MB | 81 MB | **74 MB** |

\** Lazy open — see below. This number is real but does not mean what it looks
like, and should not be quoted on its own.

The two warm rows are both needed. OxiDB builds a secondary index when a query
first wants it, so a workload that scans and never seeks pays for none of them;
PostgreSQL reads index pages on demand too, but its cache is capped, so the same
step barely moves it. Quoting only the first row would flatter OxiDB by choosing
its favourable workload.

### What changed, and what it cost

The first run of this benchmark measured 423 MB warm in disk-first mode. Five
changes, each measured on the same dataset, took it to 162 MB (310 MB with every
index exercised):

| Change | Effect |
|---|---|
| Sorted inline posting lists instead of a `BTreeSet<u64>` per index key | a key matching one row cost ~150 bytes to say so |
| `SmallVec` key tuples instead of a heap `Vec` per key | one allocation per key in every index and PK map |
| Unboxed `i64` primary-key map for single-column integer keys | **103 → 34 bytes per row** |
| Fold the replayed WAL tail at open (disk-first) | a 55 MB WAL tail cost 60 MB of resident overlay |
| Build secondary indexes on first use, not at open | 318 MB for three indexes on 1M rows, paid before answering anything |

## Read this before quoting the startup row

**OxiDB's 4 MB after a restart is not efficiency; it is not having opened the
database yet.** The SQL engine is created on the first statement that needs it,
so a freshly restarted server that has been asked nothing is holding a listener
and nothing else. One `SELECT 1` — which touches no table — opens the engine and
jumps it to the warm figure:

```
after boot (no query):            4368 KB
after 'SELECT 1' (engine open):    160 MB
```

This is what made the number worth chasing rather than quoting. Before the
changes below the same `SELECT 1` reached **620 MB**, because opening the engine
replayed the WAL tail into RAM and rebuilt every index before answering
anything.

PostgreSQL's 26 MB at the same point is a server that has already opened its
cluster and is ready to serve. Comparing the two is comparing a shop before it
unlocks with one that is open. The **warm** row is the comparison that means
something.

## What the warm rows say

**PostgreSQL's memory is a configured cap; OxiDB's is a function of the data.**
That is the whole difference, and it survives every optimisation above.

It is worth being precise about how PostgreSQL achieves that, because it is a
design OxiDB can borrow rather than a constant to admire. From the source:

- **Everything is page-granular.** `src/backend/storage/buffer/README`: buffers
  are found through a partitioned hash from page identifier to buffer, pinned
  by refcount, and evicted by a clock sweep over usage counters. Pins and locks
  protect *whole pages*, never keys or tuples. The pool is `shared_buffers` and
  does not grow past it.
- **A unique insert reads the index off disk.** `_bt_doinsert`
  (`src/backend/access/nbtree/nbtinsert.c`) descends the tree via
  `_bt_search_insert`, then `_bt_check_unique` binary-searches the *locked leaf
  page* and follows `_bt_relandgetbuf` to the sibling when duplicates span
  pages. There is no key map to consult — which is exactly what OxiDB's
  `pk_map` is.
- **An index entry is ~20 bytes, on disk.** `IndexTupleData`
  (`src/include/access/itup.h`) is an 8-byte header — a 6-byte heap TID plus
  2 bytes of flags — with the key following at a `MAXALIGN` boundary, plus the
  page's 4-byte line pointer. For a 4-byte integer key that is 16 + 4 = 20
  bytes **in a page that may be evicted**.

Against that, OxiDB spends ~106 bytes per index entry, in RAM, for the life of
the process. The gap is not tuning — it is that one design stores keys in pages
it can drop and the other stores them in a `BTreeMap` it cannot.

OxiDB holds every index key and primary key in RAM. `OXIDB_SQL_DISK_FIRST` moves
*row data* to an mmap'd snapshot; keys stay resident by design. So the gap is
now small when a workload does not use its indexes (162 MB against 105 MB) and
still ~3× when it uses all of them (310 MB against 106 MB).

Measured cost of what remains, on 1M rows in disk-first mode:

| Component | Memory |
|---|---:|
| Base — row offsets, catalog, mmap metadata | 29 MB |
| Single-column `INT PRIMARY KEY` map | 34 MB |
| Three secondary indexes (one integer, one text, one composite) | 318 MB |

The index figure is the one worth attacking next: ~106 bytes per row entry,
against PostgreSQL's ~20 on disk. Most of it is the `Value` discriminant, a
separate small heap allocation per `Text` key, `BTreeMap` node slack, and — for
composite keys — a `SmallVec` that spills.

Closing it means doing what the source above describes, and what OxiDB's
*document* engine already does with `.mcidx` (which took a 3-index million-row
collection from ~430 MB resident to 6.8 MB): pack index entries into fixed-size
pages on disk, cache those pages with a bound and an eviction policy, and answer
a primary-key or uniqueness check by descending the pages instead of consulting a
resident map. That deletes `pk_map` and the secondary index maps outright, and
makes memory a configured number rather than a function of row count.

## What OxiDB does win

- **Startup floor**: 4 MB against 40 MB for an empty database, and one process
  against nine. For many small databases per host — the OxiBase multi-tenant
  case — that per-instance floor matters more than the warm figure.
- **Disk**: 74 MB against 504 MB for the whole data directory, and against
  153 MB counting only tables and indexes. Roughly half the bytes for the same
  million rows, before counting PostgreSQL's WAL and free-space maps.
- **Scan-heavy workloads**: 162 MB against 105 MB is a gap; it used to be 4×.

## What this benchmark does not measure

Query speed (see [the wire benchmark](wire-benchmark.md)), concurrency, larger
datasets, or a tuned PostgreSQL. It also flatters neither engine on load time:
7.2 s against 23 s is real, but both went through `psql` with multi-row
`INSERT`s — PostgreSQL's `COPY` would be far faster and OxiDB has no equivalent.

The honest summary is that OxiDB now starts small and *stays* small until a
query asks for an index, at which point it pays for that index in RAM and keeps
paying. PostgreSQL never pays more than its buffer cap. Below a few hundred
thousand rows, across many small databases, or on workloads that scan more than
they seek, OxiDB trades well. On an index-heavy million-row table it still does
not, and a disk-backed index is what would change that.
