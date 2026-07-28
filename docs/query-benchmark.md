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
| Point `SELECT` by primary key | 37.2k | 26µs | 38.4k | 26µs | **0.97×** |
| Composite primary key lookup | 36.3k | 27µs | 37.4k | 27µs | **0.97×** |
| Secondary index equality | 40.4k | 25µs | 37.3k | 27µs | **1.08×** |
| Range scan + `ORDER BY` + `LIMIT` | 35.1k | 28µs | 31.8k | 30µs | **1.10×** |
| Index, low selectivity | 176 | 5.6ms | 605 | 1.6ms | **0.29×** |
| Full-scan aggregate | 61 | 16.4ms | 128 | 7.8ms | **0.48×** |
| `GROUP BY` | 34 | 29.1ms | 82 | 12.1ms | **0.42×** |
| Join + filter | 53 | 18.9ms | 95 | 10.5ms | **0.56×** |

## What it says

**Point access is a dead heat.** Primary key, composite key, secondary index
equality and a small indexed range with `ORDER BY … LIMIT` all land within 10%
either way, some ahead and some behind. For the request-per-key workload most
applications actually run, OxiDB is competitive with PostgreSQL.

**Anything that scans is 2-3.5× slower.** Aggregates, `GROUP BY` and joins are
where PostgreSQL's executor shows its decades: tight per-tuple loops, a planner
that picks between scan strategies, and aggregate paths that avoid materializing
rows. OxiDB's disadvantage grows with the number of rows a query has to walk.

**Disk-first mode costs another chunk of scan speed**, because rows are decoded
out of the mmap'd snapshot instead of being read as live values:

| Workload | resident | disk-first |
|---|---:|---:|
| Full-scan aggregate | 0.48× | 0.26× |
| `GROUP BY` | 0.42× | 0.29× |
| Join + filter | 0.56× | 0.24× |

Point lookups are unaffected — they touch one row either way. So the memory
benchmarks' disk-first figures and this one's resident figures describe two
different trades, and a deployment picks one: disk-first for footprint, resident
for scan throughput.

**Loading is slower too**: 21 s against 5.5 s for the same 1.2M rows through the
same client.

## What this does not measure

Concurrency (both were driven by one connection), larger datasets, a tuned
PostgreSQL, write-heavy workloads beyond the load, or query planning on shapes
more complex than these. Nor does it compare plans: PostgreSQL's advantage on
the scanning workloads may partly be *choosing* a better one, which is a
different problem from executing a chosen one faster.

One cosmetic difference worth knowing: floating-point sums agree to about 10
significant digits but not exactly, because the two engines add in different
orders.
