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

At that point this document concluded the remainder was **allocator retention** —
freed pages musl was not returning — and that no data-structure change would
help. **That was wrong, and it was wrong because it was inferred rather than
measured.** Process-level metrics cannot distinguish live bytes from freed-but-
retained ones, so the conclusion was reached by elimination, from the fact that
two structural fixes had not paid.

### Measuring the peak instead of arguing about it

`cargo run -p oxidb-sql --example open_mem <dir>` installs a counting global
allocator and reports live and peak *allocated* bytes across an open. That
distinguishes exactly what process metrics cannot: if peak-live tracks the peak
footprint, something really is that big.

It does. Opening the 1.2M-row database with a 204 MB tail: **peak live 128.7 MB,
process peak 116 MiB, steady state 37.5 MB.** The peak is live memory, and the
same open with an already-folded WAL peaks at 42 MB — so *all* of it is the WAL
replay, none of it retention.

Sweeping how often the replay folds gives a straight line, reproducible to a
tenth of a megabyte:

| Fold every | Peak | Open time |
|---|---:|---:|
| 25k row ops | 71 MB | 42.6 s |
| 50k | 78 MB | 22.8 s |
| 200k (the default then) | 129 MB | 8.0 s |
| never | 356 MB | 2.1 s |

So the peak was ≈ 55 MB + ~370 bytes per pending row operation, and the "356 MB"
row is the number this document had been calling irreducible. It was reducible by
one constant — except that folding four times as often cost five times the open
time, which is why the trade had looked like a dead end.

**Why folding was so expensive.** A fold wrote every table's snapshot and rebuilt
every index, walking the table once per consumer: a table with a primary key, a
`UNIQUE` column and one secondary index was read four times, and in disk-first
mode each read decodes every row out of the mmap. Timing the phases put ~40% in
the walks and ~58% in sorting and writing the index files — and, decisively, the
cost was the same whether the fold had 200k pending operations to absorb or 2.

Two changes, in the order the measurements asked for them:

1. **One walk per table feeds every consumer.** The snapshot writer became a sink
   (`storage::SnapshotWriter`) so the primary key, `UNIQUE` and secondary index
   builders can be driven from the same pass. Worth ~10% — the walk was not the
   problem — and it *cost* 20 MB, because several spill buffers are now alive at
   once. On its own this was not a win.
2. **A table nothing has touched is not written at all.** Its files in the
   generation it is based on are already correct, so they are hard-linked into the
   new generation. Reclaiming the old generation then just drops a directory
   entry; the MANIFEST rename is still the only commit point. This is what changed
   the trade: a fold became proportional to what changed rather than to the
   database.

With folds cheap, the buffer regression from (1) was worth paying back:
`SPILL_ENTRIES` dropped from 131_072 to 32_768, which bought 22 MB for 10% time
and stops helping below that. Then the fold threshold moved to the new knee, 50k.

| | Peak live | Open time |
|---|---:|---:|
| Before | 129 MB | 8.0 s |
| Untouched tables reused, fold every 50k | **63 MB** | **6.1 s** |

Both axes improved: half the peak, and faster than the setting it replaced.

### The same thing measured end to end, in a cgroup

The allocator counts live bytes; an operator sizes a container. `openpeak.sh`
measures the second: load 1.2M rows at the **default** settings (so the tail is
whatever the engine leaves — 12 MB here, since auto-checkpointing bounds it),
`SIGKILL`, restart, and read the kernel's own `memory.peak`. Same box, same data,
the two builds differing only in this change:

| | Peak opening | Settled after | Clean reopen |
|---|---:|---:|---:|
| Before | 333 MB | 258 MB | 82 / 73 MB |
| After | **180 MB** | **108 MB** | 83 / 73 MB |

These are larger than the allocator's figures because a cgroup charges the page
cache the open faults, and they confirm the earlier ad-hoc number this document
reported (333 MB) on a harness that can be re-run.

**But the peak is not what a memory limit binds on, and that must not be
oversold.** Under a hard limit the kernel reclaims the page-cache share of that
peak, so what decides whether the server starts is the anonymous part. Opening
the same bulk-loaded database inside decreasing `--memory` limits:

| Limit | Before | After |
|---|---|---|
| 256 MB | opens | opens |
| 192 MB | opens | opens |
| 160 MB | OOM | **opens** |
| 128 MB | OOM | OOM |

One step, not the 1.9× the peak figure suggests. Both numbers are real and they
answer different questions: the peak is what the process transiently needs, the
limit sweep is what it survives.

### Does it hold at 10M rows? Two answers, and only one is comfortable

Everything above is 1.2M rows. `gen.py --scale 8` gives the same schema at 9.6M
(1.6M customers, 3.2M orders, 2.4M order items, 2M inventory, 400k products),
loaded the same way, which separates the costs that are bounded from the ones
that are per-row.

**The open transient is bounded, and stays bounded.** It is set by how much of
the WAL tail is materialized at once, and the tail is itself bounded by
`checkpoint_bytes` — so it does not grow with the table. At 9.6M rows the
default settings left a 56 MB tail, and the peak sat 189 MB above the steady
state, in the same range as at 1.2M. That is the part this work fixed, and it
scales.

**The steady state is not bounded at all.** Live bytes after opening:

| Rows | Steady state |
|---:|---:|
| 1.2M | 37.5 MB |
| 9.6M | 300.0 MB |

That is **32.8 bytes per row with no meaningful fixed part** — the fit passes
through the origin. Extrapolating: 10M rows ≈ 310 MB, 50M ≈ 1.5 GB, 100M ≈
3.1 GB, all of it anonymous and none of it evictable. Almost all of it is the
`.rdat` row-offset index, `Vec<(u64, u64, u32)>` = 24 bytes per row, built at
open for every table.

So the honest answer to "is there an OOM risk past 10M rows" was **yes, and it is
not the open peak** — a per-row resident cost that a mode called *disk-first*
should not have, survivable at 10M (310 MB) and decisive at 100M (3.1 GB), where
the data is on disk and the index to it is not.

### Making the row index sparse

The records are fixed-header and laid out in ascending row-id order, so the file
is already its own index: one entry per 32 records is enough, and reaching a
specific record from there is a walk of 16-byte headers. No format change — the
`.rdat` layout already supported this; only what is built at open changed.

The catch is that the scan path reads records **positionally and sequentially**.
Asking a sparse index for "record *i*" would re-walk its block every row and make
a scan quadratic in the block size, so sequential readers now take a
`SnapshotCursor` that carries its own byte offset and advances in constant time.
Only lookup-by-id walks.

| | 1.2M before | after | 9.6M before | after |
|---|---:|---:|---:|---:|
| steady state | 37.5 MB | **0.8 MB** | 300.0 MB | **6.3 MB** |
| open peak | 62.5 MB | 37.1 MB | 489.3 MB | 137.0 MB |
| open time | 6101 ms | 6119 ms | 3954 ms | 3963 ms |
| full scan | 16 ms | 17 ms | 132 ms | 135 ms |
| point lookup by PK | 6.8 µs | 7.0 µs | 7.2 µs | 7.3 µs |

**32.8 bytes per row became 0.69** — 47× less, for about 2% on reads and nothing
on open time. 100M rows now costs ~69 MB where it would have cost ~3.1 GB, and
disk-first mode holds no per-row structure of any kind: rows, secondary indexes,
primary keys, `UNIQUE` columns and now the row index are all files.

Re-measured end to end in the cgroup harness after the sparse index (and after
the ten query-speed changes that followed, which — verified — moved memory not
at all): opening the bulk-loaded database now peaks at **137 MB and settles at
72**, and a clean reopen runs at **39 MB flat**, against the 180/108 and 83/73
this document recorded before the index went sparse. One wrinkle stated rather
than hidden: the restart *ratio* looks worse (3.5× against 2.5×) precisely
because the floor fell by more than the peak — the replay transient is now
measured against a much smaller base. Every absolute number improved.

Two notes on how the size was chosen and what it cost. A sweep at 9.6M showed the
walk barely registers (7.1 µs at 16 records per block, 7.4 µs at 128) while memory
falls 8×, which makes big blocks look free — but that run has the file in page
cache, and a walk skips over payloads, so a 128-record block spans ~14 KB: four
pages faulted to read one row against one page at 32. Every size already turns
gigabytes into megabytes, so the remaining megabytes were not worth buying with
page faults this benchmark cannot charge for. And the dense index used to sort
itself defensively at open; a sparse one cannot, since it does not hold the rows,
so ascending order is now an enforced invariant and a file violating it fails to
open rather than answering lookups wrongly for the rest of its life.

### The fold interval has to scale too

Measuring at 9.6M caught a default this work had just got wrong. The trade
folding makes is between a transient that costs ~370 bytes per pending row
operation and a steady state that costs ~33 bytes per row — both linear in the
same unit, so a *fixed* interval means a different ratio at every scale:

| | fold every 50k | fold every `rows/24` |
|---|---|---|
| 1.2M rows | 6.1 s, 63 MB (1.67× steady) | same — 50k is the floor |
| 9.6M rows | 13.0 s, 387 MB (1.29×) | **4.0 s, 489 MB (1.63×)** |

At 1.2M, 50 000 is right: it halves a 3.2× margin for 2.6 seconds. At 9.6M the
same value spends 9 extra seconds to shave 46 MB off a 300 MB steady state,
which is not a trade any operator asked for. Scaling the interval with the row
count fixes the *ratio* instead — restarting costs about 1.65× running at either
size — and it means a large database folds once or not at all, because the
interval only bites while it is shorter than the tail. `OXIDB_SQL_REPLAY_FOLD_OPS`
still overrides it.

Reuse is the kind of optimization that loses data quietly, so it is deliberately
conservative — a table qualifies only if its row store *is* its base snapshot
(derived from the store, not tracked as a dirty flag that a new write path could
forget to set), and only if every file the new generation needs already exists in
the old one. That second condition is not redundant: an index created since the
last checkpoint has no file yet, and linking "whatever exists" would commit a
generation declaring an index it holds no file for — which answers no query
wrongly, it just silently stops being an index. `checkpoint_reuse_tests` pins
each case, and the index one fails when the check is removed.

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
5.7 s against 22 s is real, and rewriting every table's snapshot and indexes at
every checkpoint is part of why OxiDB's is slower — a checkpoint now skips the
tables that have not changed, which a load of one table at a time benefits from,
but the table being loaded is still rewritten each time. Both went through `psql`
with multi-row `INSERT`s — PostgreSQL's `COPY` would be far faster and OxiDB has
no equivalent.

The honest summary: OxiDB in disk-first mode holds no *key set* in anonymous
memory — rows, secondary indexes, primary keys and `UNIQUE` columns are files it
maps — and that took its process memory from 423 MB to 36 MB and its total
physical use to about two-thirds of PostgreSQL's. But it is not three times
lighter (that came from a metric which stopped counting the pages it had moved
into files), and it does not run in a smaller box: its floor is ~96 MB against a
tuned PostgreSQL's ~64 MB, because what is left anonymous — a 37 MB baseline
plus 24 bytes per row of offset index — is still most of what it needs. Opening
after a bulk load used to be the sharpest edge in the design, peaking at 4.6×
the steady state; measuring where those bytes actually were — rather than
inferring it, which is how this document previously got the answer wrong — took
that to 2.4×, and the smallest limit it can restart a bulk-loaded database in
from 192 MB to 160 MB.

Resident mode still trades memory for speed on purpose, and is the right default
for databases that fit comfortably in RAM.
