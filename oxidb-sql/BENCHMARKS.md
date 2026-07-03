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

| Query | Shape | v2 (row-materializing) | v3 (late materialization) | v4 (+parallel join) | PostgreSQL | Speedup |
|-------|-------|-----------------------:|--------------------------:|--------------------:|-----------:|--------:|
| Q1 | 5-way INNER, revenue/region   | 163.3 ms | 26.8 ms | **21.2 ms** | 45.1 ms | 2.1× |
| Q2 | 6-way INNER, revenue/supplier | 180.0 ms | 28.3 ms | **20.7 ms** | 47.9 ms | 2.3× |
| Q3 | LEFT chain, orders/region     |  27.5 ms |  2.8 ms |  **3.4 ms** | 14.8 ms | 4.4× |
| Q4 | FULL join, row count          |  15.5 ms |  1.3 ms |  **1.5 ms** |  7.3 ms | 4.8× |

(v3→v4 also absorbed the correlated-subquery / view / window-function
evaluation plumbing added between the two measurements — Q3/Q4's small deltas
are that, not the parallelism, which only engages above 32k rows.)

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

## v4: join reordering + parallel probe/build

**Greedy join reordering.** An all-INNER join chain is reordered before
execution: at each step, among the joins whose `ON` is fully resolvable
against the tables placed so far (and still equi-connects — stays a hash
join), the one with the smallest right-table row count goes first, shrinking
intermediate results early. Written order is kept whenever reordering is
unsafe or unknowable: any outer join, a view source (no cardinality hint), or
an `ON` with unqualified column references.

**Parallel probe/build (rayon).** Above 32k rows, the hash-join probe runs on
chunks of left tuples in parallel — chunk outputs concatenate in chunk order,
so emitted rows are byte-identical to the sequential loop; per-chunk
right-matched bitmaps are OR-merged for outer joins. The build side's key
evaluation parallelizes the same way. Below the threshold everything stays
sequential, so small queries (scale 1: Q1 ≈ 1.06 ms) are unaffected.

## Insert benchmark — PK + 4 secondary indexes

`examples/insert_bench.rs` vs `examples/insert_bench_postgres.sh`: identical
statements (100 batches x 1,000 rows, then 2,000 autocommit single-row
INSERTs) into `events (id BIGINT PRIMARY KEY, user_id, kind, ts, amount)`
with indexes on `user_id`, `kind`, `amount`, and `(user_id, kind)`, plus a
bare (no PK, no index) table as contrast. Parity checks (COUNT, SUM, indexed
lookups) are byte-identical on both engines.

Durability matters more than anything else here, so both engines were
measured at **both** durability levels (macOS, Apple SSD; a raw
`F_FULLFSYNC` costs ~3.6 ms on this drive):

| Workload | OxiDB `full`¹ | OxiDB `data`² | PG `fsync_writethrough`¹ | PG `open_datasync`² (default) |
|----------|--------------:|--------------:|-------------------------:|------------------------------:|
| bulk indexed (rows/s)  | **131,660** | **316,948** | 62,483 | 19,241 |
| bulk bare (rows/s)     | **110,232** | **451,835** | 87,266 | 45,491 |
| single insert (ms/ins) | 4.07 | **0.030** | **3.74** | 0.165 |

¹ true storage flush (`F_FULLFSYNC`) — survives power loss.
² OS-cache-level sync — PostgreSQL's macOS default; not power-loss-proof.

Like-for-like: at full durability OxiDB loads **2.1× faster** in bulk and
ties on single inserts (both are fsync-bound: 4.07 vs 3.74 ms against a
3.6 ms physical flush). At PostgreSQL's own default durability class
(`OXIDB_SQL_SYNC=data`), OxiDB is **16× faster** in bulk and **5.5× faster**
per single insert. The bare-table delta shows index maintenance costs OxiDB
~30% and PostgreSQL ~2-3× in bulk.

`OXIDB_SQL_SYNC` = `full` (default) | `data` selects the WAL sync mode —
the same trade PostgreSQL exposes as `wal_sync_method`.

## v5 — disk-first row storage (2026-07-03)

Same hard-join benchmark at scale 20, with `OXIDB_SQL_DISK_FIRST=1`: rows
served from the mmap'd last-checkpoint `.rdat` snapshot, only
post-checkpoint changes resident. Fresh process per run (seeded directory
reused via `JOIN_BENCH_DIR`), current RSS via `ps`, PG 15 best-of-5 client
`\timing` on the same machine:

| Query | resident | disk-first | PostgreSQL 15 |
|-------|---------:|-----------:|--------------:|
| Q1 5-way INNER  | **23.0 ms** | 27.2 ms | 45.5 ms |
| Q2 6-way INNER  | **23.4 ms** | 26.4 ms | 47.1 ms |
| Q3 LEFT chain   | **3.2 ms**  | 5.3 ms  | 14.0 ms |
| Q4 FULL join    | **1.7 ms**  | 3.8 ms  | 6.9 ms |

Disk-first stays **1.7–2.6× ahead of PostgreSQL on every query** while
giving up 15–120% to resident mode (mmap decode on base rows).

Memory (same runs): RSS after open **109 MB resident vs 61 MB disk-first**;
after the query workload 191 vs 187 MB (join intermediates dominate; the
touched mmap pages are clean file pages the OS can evict under pressure).
On a plainer 1M-row/4-col table (`examples/disk_first_rss.rs`, seed and
measure in separate processes): **272 → 143 MB** RSS, open 226 → 170 ms,
full scan 11 → 43 ms. The PG backend peaked at 47 MB RSS during the join
queries (128 MB `shared_buffers` configured; PG additionally leans on the
kernel page cache, which never shows up in its RSS — mmap'd disk-first is
OxiDB's equivalent of that architecture).

Write path is unaffected: the insert benchmark measures 127k vs 122k rows/s
bulk (noise) and identical 3.92 ms single inserts (fsync-bound) across the
two modes. Auto-checkpointing (`OXIDB_SQL_CHECKPOINT_BYTES`, default
64 MiB) bounds both the WAL replay time and the disk-first RAM overlay.

## Remaining headroom

Not yet done (would widen the lead further): cost-based planning beyond the
greedy reorder, building the index on the smaller join side, parallel
grouping, and SIMD-friendly column-major chunks.
