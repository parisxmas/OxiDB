# Query performance: OxiDB vs PostgreSQL

Every other benchmark in this tree measures memory. This one measures the thing
a database is usually judged on — and the one OxiDB had never been compared
against PostgreSQL on at all. The
[wire benchmark](wire-benchmark.md) compared two *protocols* over OxiDB's own
engine; PostgreSQL's planner and executor were never in it.

```bash
cargo run --release -p oxidb-server --example query_bench -- \
    --oxidb 127.0.0.1:5600/oxidb --postgres 127.0.0.1:5601/bench --secs 3
```

## Method

Because OxiDB speaks the PostgreSQL v3 wire, **one hand-rolled client drives
both servers** ([`query_bench.rs`](../oxidb-server/examples/query_bench.rs)) —
no driver difference, no language difference, no library caching one side's
results. It connects once, sends a simple-query message, reads every backend
message to `ReadyForQuery`, and **decodes every cell**, so neither side is
credited for skipping work the other does.

Same dataset as the memory benchmark (`bench/pg-memory/`): 1.2M rows over five
related tables with foreign keys, composite primary keys and eight indexes.
Each workload substitutes a different value per iteration, so neither side can
sit on a single cached plan. PostgreSQL runs stock, after `ANALYZE`. Warm on
both sides; 3 s per workload; ratios are the result, absolutes are hardware.

Answers were checked to be identical before timing anything.

## Results

Ratio > 1 means OxiDB is faster. OxiDB in its **default** (resident) mode:

| Workload | OxiDB /s | p50 | PostgreSQL /s | p50 | Ratio |
|---|---:|---:|---:|---:|---:|
| Point `SELECT` by primary key | 37.0k | 27µs | 38.7k | 26µs | **0.96×** |
| Composite primary key lookup | 35.8k | 28µs | 37.4k | 27µs | **0.96×** |
| Secondary index equality | 42.6k | 23µs | 37.4k | 27µs | **1.14×** |
| Range scan + `ORDER BY` + `LIMIT` | 35.8k | 27µs | 33.2k | 30µs | **1.08×** |
| **Full-scan aggregate** | 135 | 7.4ms | 132 | 7.6ms | **1.02×** |
| `GROUP BY` | 54 | 18.4ms | 84 | 12.0ms | **0.65×** |
| Join + filter | 86 | 11.6ms | 98 | 10.1ms | **0.87×** |
| Index, low selectivity | 462 | 2.1ms | 606 | 1.6ms | **0.76×** |

### What changed

The first run of this benchmark had every scanning workload at 0.24-0.48×.
Aggregates are now folded **during** the scan rather than after it
(`streamed_aggregate` in `executor.rs`):

| Workload | before | after |
|---|---:|---:|
| Full-scan aggregate | 0.48× | **1.02×** |
| `GROUP BY` | 0.42× | **0.66×** |
| Index, low selectivity | 0.29× | **0.76×** |
| Join + filter | 0.56× | **0.87×** |
| Secondary index equality | 1.08× | **1.14×** |

The general path builds every source row into a `Chunk` and then indexes into
it per group — two passes over 400,000 rows and a 400,000-element vector, to
produce one row. Folding accumulators as the scan produces rows makes it one
pass and no vector.

Two things that mattered more than the idea:

- **It must not bypass the index.** The first version intercepted before the
  index probe, so `count(*) WHERE customer_id = ?` went from a 25µs lookup to a
  4ms full scan — 0.01×. The benchmark caught it immediately. Folding now runs
  *over* the index result when one applies.
- **It must not fold DECIMAL through `f64`.** The general path adds decimals
  exactly. A fast path that quietly rounded them would be a wrong answer rather
  than a slow one, so `SUM`/`AVG` stream only over types where the fold is
  exact and the type is known.

### What still loses, and why

**Joins (0.87×)**, improved from 0.56×. The hash join materialized the whole
right table and then discarded most of it: a left side matching 40,000 of
400,000 rows still paid to build all 400,000. A semi-join pre-filter now skips a
right row before building it, when its key is not one the left holds.

**The structure was the whole difficulty, and two measured failures found it.**
Raising the index-nested-loop threshold so 19,823 left rows probed instead of
scanning made it *worse* — 0.56× → 0.29× — which validated the existing 8,192
cap rather than replacing it. Then the obvious `BTreeSet<IndexKey>` membership
test was worse still, 0.22×: about fourteen `Value` comparisons per right row
cost more than the row build it was avoiding. A direct-addressed bitmap over the
integer key range makes the test one shift and one mask, and that is finally
cheaper than building a row. It applies only to integer keys over a range dense
enough to be worth the bitmap; anything else takes the old path.

**Low-selectivity index scans (0.76×)**, from 0.29×. Three costs per candidate
row were removed in turn. `index_lookup_eq` built a `Vec<(u64, Vec<Value>)>` of
every match before the caller saw any of them — rows now stream through
`index_visit_eq`. Each row was materialized *twice*, once physically to verify
the index key and once logically for the result — they are the same row unless a
column has been dropped. And `RowStore::raw` cloned unconditionally, so every
candidate was copied out of a `Vec` sitting in memory; `physical_ref` borrows it
instead, leaving only the disk-first base to materialize, because its rows live
encoded in the mmap.

The remaining gap is the per-row predicate evaluation itself, not row access.

## What this does not measure

Concurrency (both were driven by one connection), larger datasets, a tuned
PostgreSQL, write-heavy workloads beyond the load, or query planning on shapes
more complex than these. Nor does it compare plans: PostgreSQL's advantage on
the scanning workloads may partly be *choosing* a better one, which is a
different problem from executing a chosen one faster.

One cosmetic difference worth knowing: floating-point sums agree to about 10
significant digits but not exactly, because the two engines add in different
orders.
