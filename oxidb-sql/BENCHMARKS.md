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

| Query | Shape | v0 nested-loop | v1 hash join | v2 +bind +group-move | PostgreSQL |
|-------|-------|---------------:|-------------:|---------------------:|-----------:|
| Q1 | 5-way INNER, revenue/region   | 9425 ms | 10.3 ms | **7.0 ms** | 7.8 ms |
| Q2 | 6-way INNER, revenue/supplier | 9594 ms | 12.0 ms | **6.5 ms** | 7.3 ms |
| Q3 | LEFT chain, orders/region     |  615 ms |  1.8 ms | **1.1 ms** | 1.4 ms |
| Q4 | FULL join, row count          |  584 ms |  1.2 ms | **0.7 ms** | 2.1 ms |

**oxidb-sql now beats PostgreSQL 15 on all four queries** — after starting
~275-1300× slower. (Postgres `EXPLAIN ANALYZE` pure execution time: Q1 12.3 ms,
Q2 10.7 ms; the table uses client `\timing` best-of-5. Both engines parse +
plan + execute per call; neither uses secondary indexes on join keys.)

## The optimizations

**1. Hash join + a simple per-join planner** (`executor.rs::join_into`).
The `ON` predicate is scanned for **equi-join keys** (`left_col = right_col`
conjuncts, split by which side each column belongs to). If any exist, a
`HashMap` keyed by the join key is built over the right rows and probed with the
left — O(N+M) instead of O(N·M). NULL keys never match (SQL semantics); the full
`ON` is re-checked on candidate pairs, so residual (non-equi) conditions and any
hash collisions stay correct. With no equi key (e.g. `ON a.id < b.id`) it falls
back to the nested-loop join. This alone took ~1000× → ~1.3×.

**2. Expression binding** (compile once, evaluate O(1)). Before evaluating any
per-row expression (join `ON`, `WHERE`, projection, `GROUP BY`, `HAVING`,
`ORDER BY`), every `Column` reference is resolved to a positional index
(`Expr::Col(i)`) against the row schema. This removes the per-row, per-column
linear name search (`resolve_col`) that dominated once the join was O(N+M).

**3. Move-not-clone grouping.** `GROUP BY` moves each (wide) join row into its
group bucket instead of cloning it — a big saving on the 5-/6-way joins whose
intermediate rows carry ~15 columns.

Correctness is unchanged throughout — all 166 crate tests plus this differential
check against PostgreSQL still produce identical results.

### Original naïve baseline (for reference)

The v0 join full-scanned the entire right table per left row; Q1/Q2's
`orders(≈5000) ⋈ items(15000)` ≈ 75M comparisons per run gave the 9.4 s figures.

## Remaining headroom

Not yet done (would widen the lead further): join reordering / cost-based
planning, projection push-down (carry only needed columns through joins),
building the hash table on the smaller side, and parallel execution.
