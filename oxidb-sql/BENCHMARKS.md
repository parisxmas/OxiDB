# oxidb-sql — hard-join benchmark vs PostgreSQL

Compares the standalone SQL engine against PostgreSQL 15 on deep multi-way
join queries. Both engines run the **identical dataset** (same row values,
generated from the same formulas) and the **identical SQL**, so the run is both
a correctness cross-check and a speed comparison.

- oxidb-sql: `cargo run --release --example join_bench -p oxidb-sql` (in-process, embedded)
- Postgres: `psql -d oxidb_sql_bench -f examples/join_bench_postgres.sql` (PG 15, local socket)
- Both harnesses take a scale factor: `SCALE=20 cargo run ...` / `psql -v k=20 -f ...`

Dataset at scale k: regions=10, suppliers=10, customers=1000k, products=300k,
orders=5000k, items=15000k. Neither side has secondary indexes on the join
keys (only primary keys), so both rely on their engine's join strategy.

## Correctness — identical results

All four queries return byte-identical results on both engines at every scale
tested (e.g. scale 20: Q1 `R1|116502, R10|582585, …`; Q2 correctly omits
suppliers S1/S6 which the data formula never assigns; Q4 = 100000). ✅

## Speed — scale 20 (100k orders, 300k items), best of 5

| Query | Shape | v2 (row-materializing) | v3 (late materialization) | PostgreSQL | Speedup |
|-------|-------|-----------------------:|--------------------------:|-----------:|--------:|
| Q1 | 5-way INNER, revenue/region   | 163.3 ms | **26.8 ms** | 45.1 ms | 1.7× |
| Q2 | 6-way INNER, revenue/supplier | 180.0 ms | **28.3 ms** | 47.9 ms | 1.7× |
| Q3 | LEFT chain, orders/region     |  27.5 ms |  **2.8 ms** | 14.8 ms | 5.3× |
| Q4 | FULL join, row count          |  15.5 ms |  **1.3 ms** |  7.3 ms | 5.7× |

## Speed — scale 1 (5k orders, 15k items), best of 5

| Query | v2 | v3 | PostgreSQL |
|-------|---:|---:|-----------:|
| Q1 | 7.0 ms | **1.04 ms** | 7.8 ms |
| Q2 | 6.5 ms | **1.06 ms** | 7.3 ms |
| Q3 | 1.1 ms | **0.27 ms** | 1.4 ms |
| Q4 | 0.7 ms | **0.17 ms** | 0.68 ms |

**oxidb-sql beats PostgreSQL 15 on all four queries at both scales** — at
scale 20 the v2 engine had *lost* all four (up to 3.8× slower); v3 wins them
back by 1.7–5.7×. (Postgres timings are client `\timing` best-of-5 over a
local socket; oxidb-sql is in-process. Both engines parse + plan + execute per
call; neither uses secondary indexes on join keys.)

## The v3 execution model (late materialization)

The v2 engine materialized every intermediate join row as a fresh
`Vec<Value>` — a 5-way join at scale 20 performed ~560k row allocations per
query, re-copying every carried cell (including heap-cloned strings) at each
join stage. v3 never copies cell values through joins:

**1. Flat pruned scans (`Store::scan_pruned` → `Chunk`).** Each referenced
table is scanned once into a single flat `Vec<Value>` carrying only the
columns the query mentions (projection push-down over SELECT / WHERE / ON /
GROUP BY / HAVING / ORDER BY). One allocation per table instead of one per row.

**2. Index-tuple joins.** The working set is a flat `Vec<u32>` of per-table
row indices (`u32::MAX` = the NULL side of an outer join). A join extends
tuples by one index — a few bytes of memcpy — regardless of how many columns
the tables carry. Expressions read cells through a `View` that maps a bound
column position to (chunk, column); values are only touched at final
projection/aggregation.

**3. Direct-address dense-int join index.** For a single-component integer
equi-key whose value range is comparable to the row count (the typical
`fk = pk` case), the build side is a plain array indexed by `key - min` with
bucket chains in a flat `next` array — no hashing, no per-key `Vec`. Falls
back to a hash map (fxhash-style, with a bit-mixing finalizer) keyed by a
type-normalized `JoinKey` for everything else. A bucket hit already proves the
equi-key conjuncts, so only a residual (non-equi) `ON` remainder is
re-evaluated per candidate pair; NULL and NaN keys never match.

**4. Streaming grouping + aggregation.** GROUP BY streams tuple indices into
first-seen-ordered groups via hash(key) → candidate-group comparison (group
keys are cloned once per *group*, not per row — bare-column keys borrow the
cell). Aggregates (`COUNT/SUM/AVG/MIN/MAX`) fold streamingly over the group's
tuple indices with no per-group value buffering. ORDER BY keys are evaluated
once per row/group instead of once per comparison.

**5. Batched multi-row INSERT.** A multi-row `INSERT` is validated up front
and logged as one WAL `Batch` record — a single fsync for the whole statement
(previously one fsync *per row*), making it statement-atomic as well.

Correctness is unchanged throughout — all 166 crate tests plus this
differential check against PostgreSQL produce identical results.

### History

| Version | Q1 @ scale 1 | What changed |
|---------|-------------:|--------------|
| v0 | 9425 ms | naïve nested-loop (full right-table scan per left row) |
| v1 | 10.3 ms | hash join + per-join equi-key planner |
| v2 | 7.0 ms | expression binding (positional `Col`), move-not-clone grouping |
| v3 | 1.04 ms | late materialization: pruned flat chunks, u32 index-tuple joins, dense-int direct-address index, streaming group/agg, residual-only ON recheck |

## Remaining headroom

Not yet done (would widen the lead further): join reordering / cost-based
planning, building the index on the smaller join side, parallel probe/build,
and SIMD-friendly column-major chunks.
