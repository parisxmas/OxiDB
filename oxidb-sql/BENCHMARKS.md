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

| Query | Shape | oxidb-sql | PostgreSQL | PG faster by |
|-------|-------|----------:|-----------:|-------------:|
| Q1 | 5-way INNER, revenue/region   | 9425 ms | 7.8 ms | ~1200× |
| Q2 | 6-way INNER, revenue/supplier | 9594 ms | 7.3 ms | ~1300× |
| Q3 | LEFT chain, orders/region     |  615 ms | 1.4 ms |  ~430× |
| Q4 | FULL join, row count          |  584 ms | 2.1 ms |  ~275× |

(Postgres `EXPLAIN ANALYZE` pure execution time: Q1 12.3 ms, Q2 10.7 ms —
similar order of magnitude; the table uses client `\timing` best-of-5.)

## Why the gap

This is expected and it points at exactly one missing piece: **oxidb-sql uses a
naïve nested-loop join with no planner.** For each accumulated left row it
full-scans the entire right table (`executor.rs::join_into`), so a join step
costs O(|left| × |right|). Q1/Q2's heavy step is `orders (≈5000) ⋈ items
(15000)` ≈ 75M comparisons per run, repeated for the next joins.

PostgreSQL's cost-based optimizer builds **hash joins** (confirmed via
`EXPLAIN ANALYZE`: `Hash Join` at every level), which are O(|left| + |right|),
plus `HashAggregate` for the GROUP BY.

The engine is *correct* on all of these (identical output); it is *slow*
because it has no hash-join / merge-join / join-reordering / statistics. Those
are the natural contents of a future "query planner" phase — this benchmark is
the baseline to measure that work against. The gap shrinks for the outer-join
queries (Q3/Q4) because they touch fewer tables / smaller intermediate results.
