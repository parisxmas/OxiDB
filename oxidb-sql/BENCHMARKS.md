# oxidb-sql — hard-join benchmark vs PostgreSQL

Compares the standalone SQL engine against PostgreSQL 15 on deep multi-way
join queries. Both engines run the **identical dataset** (same row values,
generated from the same formulas) and the **identical SQL**, so the run is both
a correctness cross-check and a speed comparison.

- oxidb-sql: `cargo run --release --example join_bench -p oxidb-sql` (in-process, embedded)
- Postgres: `psql -d oxidb_sql_bench -f examples/join_bench_postgres.sql` (PG 15, local socket)

Dataset: regions=10, suppliers=10, customers=1000, products=300, orders=5000,
items=15000. Neither side has secondary indexes on the join keys (only primary
keys), so both rely on their engine's join strategy.

## Correctness — identical results

All four queries return byte-identical results on both engines (e.g. Q1
`R1|5493, R10|27555, …`; Q2 correctly omits suppliers S1/S6 which the data
formula never assigns; Q4 = 5000). ✅

## Speed (best of 5)

| Query | Shape | oxidb-sql (nested-loop) | oxidb-sql (hash join) | PostgreSQL |
|-------|-------|------------------------:|----------------------:|-----------:|
| Q1 | 5-way INNER, revenue/region   | 9425 ms | **10.3 ms** | 7.8 ms |
| Q2 | 6-way INNER, revenue/supplier | 9594 ms | **12.0 ms** | 7.3 ms |
| Q3 | LEFT chain, orders/region     |  615 ms |  **1.8 ms** | 1.4 ms |
| Q4 | FULL join, row count          |  584 ms |  **1.2 ms** | 2.1 ms |

The hash join (below) closed the gap from ~275-1300× to **~1.3-1.6×**, and
oxidb-sql now beats Postgres on Q4. (Postgres `EXPLAIN ANALYZE` pure execution
time: Q1 12.3 ms, Q2 10.7 ms; the table uses client `\timing` best-of-5.)

## The optimization: hash join + a simple planner

`executor.rs::join_into` now has a per-join planner:

- It scans the `ON` predicate for **equi-join keys** (`left_col = right_col`
  conjuncts split by which side each column belongs to).
- If any are found it builds a **hash join**: a `HashMap` keyed by the join key
  is built over the right rows, then probed with the left rows — O(N+M) instead
  of O(N·M). NULL keys never match (SQL semantics); the full `ON` is
  re-evaluated on candidate pairs so residual (non-equi) conditions and any hash
  collisions are handled, keeping results identical to the nested-loop path.
- If there is no equi key (e.g. `ON a.id < b.id`), it falls back to the
  nested-loop join.

Correctness is unchanged — all 163 crate tests plus this differential check
against PostgreSQL still pass.

### Original naïve baseline (for reference)

Before the hash join, each join step full-scanned the entire right table per
left row; Q1/Q2's `orders(≈5000) ⋈ items(15000)` ≈ 75M comparisons per run
dominated, giving the 9.4 s figures above.
