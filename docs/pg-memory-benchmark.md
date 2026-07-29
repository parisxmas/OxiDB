# Memory: OxiDB vs PostgreSQL over the same 1,200,000 rows

Both engines get the same schema, the same rows, and the same client. OxiDB
speaks the PostgreSQL v3 wire ([ADR-0023](decisions/0023-postgres-wire-protocol.md)),
so `psql` loads both from one file over one protocol — nothing about the load
path differs.

```bash
bench/pg-memory/run.sh [workdir]      # process memory, both engines, all phases
bench/pg-memory/fair.sh [workdir]     # total physical, incl. page cache
bench/pg-memory/pressure.sh [workdir] # cgroup memory limits: where each stops
bench/pg-memory/tenants.sh [workdir]  # many small tenants — the OxiBase shape
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
machine needs. The obvious inference is that OxiDB should therefore survive in a smaller box.
**It does not** — see the pressure test below, which measures rather than infers
and finds OxiDB's floor at ~96 MB against a tuned PostgreSQL's ~64 MB.

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

## Under actual memory pressure

The `fair.sh` numbers say 83% of OxiDB's footprint is evictable against 64% of
PostgreSQL's, which predicts that OxiDB should keep working in a tighter box.
[`pressure.sh`](../bench/pg-memory/pressure.sh) tests that instead of inferring
it: both engines in Linux containers with a hard `--memory` limit, same data,
same workload, cgroup v2 — where page cache is charged to the limit and
reclaimed under pressure, and anonymous memory is not.

**The prediction was wrong.**

| Memory limit | OxiDB | PostgreSQL (stock, `shared_buffers=128MB`) | PostgreSQL (`shared_buffers=32MB`) |
|---|---|---|---|
| 128 MB | ok | ok | ok |
| 112 MB | ok | ok | ok |
| 96 MB | ok | OOM-killed | ok |
| 88 MB | **OOM-killed** | — | ok |
| 80 MB | — | did not start | ok |
| 64 MB | — | did not start | ok |
| 48 MB | — | — | OOM-killed |

**OxiDB's floor is ~96 MB; a tuned PostgreSQL's is ~64 MB.** OxiDB beats
PostgreSQL *at its defaults* — it survives 96 MB where stock PostgreSQL is
killed — but loses to a PostgreSQL told to use less, which is a one-line
configuration change. Peak usage inside the cgroup at the floor is 94 MB.

The reason the evictable share did not translate into a lower floor: the
cgroup's own accounting shows **`anon 72 MB, file 0 MB`** for a warm OxiDB.
What remains anonymous still scales with the data — the `.rdat` row-offset
index is 24 bytes per row (27 MB at 1.2M rows) — on top of a 37 MB floor for an
*empty* database. Evictable pages do not help when the non-evictable part is
already most of what you need.

### The worse finding: opening costs more than running

A database opened with an unflushed WAL tail — the state a bulk load leaves —
peaks well above the ~94 MB it then runs in. Opening replays the tail and
checkpoints, and both halves of that were holding whole-table structures:

| | Peak opening |
|---|---:|
| Originally | 415 MB |
| Index files built in bounded chunks (external sort) | 365 MB |
| WAL records freed as they are applied | **328 MB** |
| Folding mid-replay + decoding the tail lazily | 337 MB (no better) |

The steady-state floor is unchanged at ~96 MB, so neither change cost anything.

**What was fixed.** A checkpoint used to collect every index, primary key and
`UNIQUE` column into a `BTreeMap` before writing it, making its peak
proportional to the table. `IndexBuilder` is an external sort instead: pairs
accumulate to a bounded buffer, get sorted and spilled to a run file, and the
runs are merged k-way straight into the output. The three file sections go to
temp files and are concatenated, so nothing holds a copy of the finished index
either. Separately, `Wal::open_since` hands back the parsed tail as a `Vec` and
the replay loop *borrowed* it, keeping every record alive beside the overlay it
was building; consuming it frees each record as it is applied.

**Two more things were tried, and neither moved it.** Replaying now folds
part-way through, so the overlay is bounded rather than holding the whole tail
(`checkpoint_upto` records the last *applied* sequence as the watermark and
leaves the log intact — getting either wrong deletes records that were never
replayed, which two tests pin by failing when the old behaviour is restored).
And `Wal::open_since` no longer parses the tail into a `Vec` up front; it hands
back locations and decodes one record at a time. Both are right on their own
terms. Neither reduced the peak: it went 328 → 337 MB.

**What the peak actually is, measured rather than assumed.** The cgroup's
breakdown on a *clean* open is `anon 71 MB, file 0 MB` with a peak of 83 MB. On
the tail open it is 337 MB and still anonymous. So it is not one live structure
being held — bounding the overlay proved that — it is **allocator retention
across repeated large allocations**: each fold rebuilds every table's row-offset
index (24 bytes a row) and musl's allocator does not return the freed pages.
Fixing that is an allocator-behaviour problem, not a data-structure one, and it
is where this stops for now.

The practical shape of the trap is unchanged: a server sized for its steady
state cannot restart immediately after a bulk load. The margin needed is now
about 3.5× steady state rather than 4.5×.

## The document engine, measured separately

Everything above is the **SQL** engine. `OXIDB_DISK_FIRST` (document engine,
default **on**) and `OXIDB_SQL_DISK_FIRST` (SQL engine, default **off**) are
different switches over different storage, and none of the tuning above touched
the document side. Since OxiBase serves documents, storage and realtime from
that engine, it is worth its own numbers.

One million documents of the same shape as the `customers` table, loaded over
OxiWire:

| | Document engine |
|---|---:|
| Empty | 8 MB |
| After loading 1M documents | 84 MB |
| After restart, before any query | **52 MB** |
| After a full scan + filter | **92 MB** |
| On disk | 141 MB |

**It is already bounded, which is the property the SQL engine had to be taught.**
Five times the data costs 1.7× the memory, not 5×: both document caches are
sized by a fixed 128 MiB budget rather than an entry count that tracks the
collection (`doc_cache.rs`, `doc_bytes_cache.rs`), so a scan fills them and
stops. That is the same shape as PostgreSQL's `shared_buffers`, reached
deliberately — the comments there record an earlier default that cached a whole
1M-document collection and dominated RSS.

What still scales with the dataset is the per-document offset index, the
document-side counterpart of the `.rdat` row-offset index that sets the SQL
engine's floor.

## Many small tenants: the shape OxiBase actually runs

Everything above compares one large database against one PostgreSQL. That is
the wrong shape for OxiBase, where a project *is* a database and every project
on a host shares one engine process — while Supabase's model gives each project
its own Postgres instance, so its fixed costs are paid per tenant rather than
per host.

[`tenants.sh`](../bench/pg-memory/tenants.sh) measures that: 10 tenants, each a
small SaaS schema (5k customers, 20k orders with a foreign key, 40k order lines
under a composite key, four indexes), same data both sides, loaded through the
same client. Memory is each container's own `memory.current`, which charges
page cache to whoever faulted it, summed across every container the engine
needs. PostgreSQL is tuned to `shared_buffers=32MB` — the setting that won the
pressure test, not the stock 128MB.

| | OxiDB | PostgreSQL |
|---|---:|---:|
| Processes / containers | **1** | 10 |
| Total, 10 tenants warmed | **201 MB** | 960 MB |
| Per tenant | **20 MB** | 96 MB |
| Marginal, per extra tenant | **~16 MB** | 96 MB |

**4.8× less memory for the same ten tenants**, and the gap widens with the
count: PostgreSQL's 96 MB was identical for every instance, because a postmaster
plus eight background processes plus `shared_buffers` is a fixed cost paid once
per project. OxiDB pays its ~37 MB baseline once for the host and about 16 MB
per additional tenant. At 50 tenants that projects to ~0.8 GB against ~4.8 GB.

This is the comparison that favours OxiDB most, and it is also the one that
matches the deployment — which is exactly why it is worth stating separately
from the single-database numbers rather than instead of them. On one big
database with every index used, OxiDB is *not* three times lighter (see above);
across many small ones it genuinely is.

**A gap this found:** `OXIDB_DOC=0` implies a single database today.
Provisioning a tenant goes through the document manager, which is in-memory when
documents are off, so it never creates the on-disk directory the SQL registry
looks for. A SQL-only multi-tenant host is therefore not yet possible; this
benchmark runs with the document engine on, as OxiBase does.

## What OxiDB does win

- **Startup floor**: 4 MB against 40 MB for an empty database, and one process
  against nine. For many small databases per host — the OxiBase multi-tenant
  case — that per-instance floor matters more than the warm figure.
- **Disk**: 169 MB against 511 MB for the whole data directory. The `.sidx`
  files are part of that — the memory did not vanish, it moved somewhere the OS
  can reclaim.
- **Non-evictable memory**: 35 MB against 108 MB, as measured on macOS — but
  see the pressure test below, where this does *not* translate into a lower
  operating floor.
- **Total physical**: 203 MB against 297 MB — a third less, largely because
  there is a third as much data directory to cache.

## What this benchmark does not measure

Query speed (see [the wire benchmark](wire-benchmark.md)), concurrency, or
larger datasets. It does now measure behaviour under memory pressure, and that
section is the one to read before believing any of the others. It also flatters neither engine on load time:
5.7 s against 22 s is real, and materializing indexes and key sets at every
checkpoint is part of why OxiDB's is slower. Both went through `psql` with multi-row `INSERT`s —
PostgreSQL's `COPY` would be far faster and OxiDB has no equivalent.

The honest summary: OxiDB in disk-first mode holds no *key set* in anonymous
memory — rows, secondary indexes, primary keys and `UNIQUE` columns are files it
maps — and that took its process memory from 423 MB to 36 MB and its total
physical use to about two-thirds of PostgreSQL's. But it is not three times
lighter (that came from a metric which stopped counting the pages it had moved
into files), and it does not run in a smaller box: its floor is ~96 MB against a
tuned PostgreSQL's ~64 MB, because what is left anonymous — a 37 MB baseline
plus 24 bytes per row of offset index — is still most of what it needs. And
opening after a bulk load still peaks at 328 MB against a 94 MB steady state,
which is the sharpest edge in the design.

Resident mode still trades memory for speed on purpose, and is the right default
for databases that fit comfortably in RAM.
