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
| `GROUP BY` | 92 | 10.9ms | 85 | 11.7ms | **1.07×** |
| Join + filter | 86 | 11.6ms | 98 | 10.1ms | **0.87×** |
| Index, low selectivity | 462 | 2.1ms | 606 | 1.6ms | **0.76×** |

### What changed

The first run of this benchmark had every scanning workload at 0.24-0.48×.
Aggregates are now folded **during** the scan rather than after it
(`streamed_aggregate` in `executor.rs`):

| Workload | before | after |
|---|---:|---:|
| Full-scan aggregate | 0.48× | **1.02×** |
| Index, low selectivity | 0.29× | **0.76×** |
| `GROUP BY` | 0.42× | **1.07×** |
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

**`GROUP BY` (1.10×)**, from 0.42×. `eval_scalar` returns an *owned* `Value`, so
grouping by a text column heap-allocated and copied a string for every row —
400,000 allocations to find a group that already existed. When the key is a
plain column it is now compared straight out of the row and nothing is
allocated until a genuinely new group appears.

Above 32 groups a hash index over the same in-place comparison takes over, so
high cardinality does not turn the walk quadratic: 173,186 groups over 400,000
rows went from 153 ms to 93 ms, against PostgreSQL's 72 ms.

**Both paths are needed, and measurement set the boundary.** Using the hash
unconditionally made the five-group case *slower* — 1.10× → 0.95× — because
hashing a text key reads every byte where a comparison usually stops at the
first. Keeping the walk below the threshold and hashing above it is what gets
both.

The key columns are restricted to types whose equality is unambiguous (integer,
text, boolean, timestamp). `Value::total_order`, which the general path uses,
compares numerics *across* types (`Int(1)` equals `Double(1.0)`) and treats NaN
as equal to everything numeric — a relation no hash can reproduce and one plain
`==` disagrees with. Rather than let two paths group differently, float and
decimal keys stay on the ordered path.

## Disk-first mode, and where its scan cost actually is

Everything above is resident mode, the SQL engine's default. Disk-first
(`OXIDB_SQL_DISK_FIRST=1`) keeps rows in a mapped file and decodes them as it
reads, which costs scan speed — and since the same executor runs in both modes,
the difference *is* that decode. Measured against the same native PostgreSQL:

| workload | disk-first | resident |
|---|---|---|
| point SELECT by PK | 0.95× | 0.95× |
| composite PK lookup | 0.94× | 0.97× |
| secondary index eq | 1.06× | 1.19× |
| range scan + ORDER BY | 1.08× | 1.09× |
| index, low selectivity | 0.45× | 0.77× |
| GROUP BY | 0.51× | 1.07× |
| full scan aggregate | 0.59× | 1.00× |
| join + filter | 0.46× | 0.87× |

Point and indexed work is unaffected — those read a row or two. Scanning work is
where the decode is paid per row.

### Decoding only the columns a query reads

A scan used to decode the whole row. `sum(total)` reads one of `orders`' five
columns, so four were rebuilt and dropped. Skipping a cell costs reading its
length and advancing, so the executor now passes the column set it will read
(`collect_needed`, which already walks the projection, filter, joins, GROUP BY,
HAVING and ORDER BY, so it is a superset by construction) and the decoder skips
the rest, leaving `Value::Null` placeholders so positions — and therefore every
caller — are unchanged.

| workload | before | after |
|---|---|---|
| full scan aggregate | 0.37× | **0.59×** |
| join + filter | 0.27× | **0.46×** |
| GROUP BY | 0.53× | 0.51× |
| index, low selectivity | 0.46× | 0.45× |

The two that did not move say something precise about the remaining cost.
`GROUP BY status` needs the one *text* column and skips three integers, and the
low-selectivity query is served through an index rather than a scan.

### What a scanned cell costs (`examples/decode_bench.rs`)

Two attempts to reason about this from query timings were wrong — subtracting one
query from another conflates a cell with the predicate term that reads it — so
the decoder is timed on its own, over 400k rows of the benchmark's `orders`
shape:

```text
  full decode (5 cells)                  40.0 ns/row
  masked: id + total (two numerics)      13.3 ns/row
  masked: id + status (one text)         32.6 ns/row
  masked: id only (one numeric)          12.4 ns/row

  of which, measured separately:
  Box<str> alloc + copy + free           13.2 ns
  UTF-8 validation                        3.4 ns
```

**A numeric cell costs about 2 ns; a text cell costs about 20 ns**, and two
thirds of that is the allocation. There is no broad per-cell inefficiency to
trim — the fixed-width path is already close to the cost of reading eight bytes.
What is left is materializing variable-length cells, which is what a borrowed
cell type (`&str` into the mapping, copied only when a value is kept) would
remove: roughly 16 ns a row per text column, or about 6 ms on a 400k-row scan.

That is the ceiling, and it is worth being clear about where it is: a scan that
reads *no* text and allocates nothing is still 0.65×, so allocation is not what
separates disk-first from resident mode — decoding at all is. Resident mode does
not decode, which is why it reaches parity and why disk-first cannot be made to
match it by removing allocations.

### Borrowing the cells a scan only compares

`ValueRef<'a>` is a cell pointing into the mapping — `&str` rather than a copied
`Box<str>`. It is deliberately not what `Value` becomes: a lifetime on `Value`
would spread through the catalog, the WAL records and their serde derives to
benefit one code path.

It is used where a cell is *compared* far more often than it is kept: a group key.
Grouping 400k rows by a text column read that column 400k times and copied it
400k times, to find a group that already existed. Now it is copied once per group.

| workload | before | after |
|---|---|---|
| GROUP BY (text key) | 0.51× | **0.63×** |

p50 fell from 23.0 ms to 18.6 ms — about 11 ns a row against the ~16 the
allocation costs, the difference being the borrowed row's own bookkeeping. The
other workloads are unchanged, which is the expected result rather than a
disappointment: they have no text group key, and this changes nothing else.

Three things are declined rather than emulated, each because the answer could
otherwise differ: **resident tables** (their cells are already `Value`s, so
borrowing only converts and compares twice — and leaving them alone keeps the
mode that is at parity untouched), **a `DECIMAL` column** (a decimal cannot be
borrowed out of an owned value, so it would read as NULL), and **a dropped
column** (stored positions no longer match visible ones).

The grouping logic itself has one implementation, reading key cells through a
`KeyCells` trait with an impl per source. A second copy for borrowed rows is
precisely how this engine previously ended up with two paths that grouped
differently — one comparing with `==` while the other used `total_order`, which
disagree on `Int(1)` versus `Double(1.0)`.

### The predicate, evaluated once instead of per row

With the decode work done, the remaining gap had a shape worth isolating. Varying
only the *number of `AND`ed terms* in a filter — same column, so the decoded cells
are identical and only evaluation changes:

| | OxiDB | PostgreSQL |
|---|---:|---:|
| `sum(total)`, no filter | 8.5 ms | 7.0 ms |
| per extra predicate term | **+5.1 ms** | **+0.35 ms** |

With no predicate at all, disk-first is within 1.2× of PostgreSQL. The entire
remaining gap on filtered scans was predicate evaluation, at **12.8 ns per term
per row against PostgreSQL's ~1 ns**. Since comparing two `f64`s is about 1 ns,
nearly all of it was `eval_scalar` re-deciding *what to do* on every row: try both
operands as borrowed strings, then as borrowed values, then dispatch the operator
— a fixed sequence of decisions repeated 400,000 times for an expression that
never changes.

`SimpleFilter` makes those decisions once. A conjunction of `column <cmp>
literal` — the dominant filter shape — reduces to a list of `(column index,
operator, value)`, and anything else is declined so the general path handles it
unchanged. Per term: **12.8 ns → 2.7 ns**.

| workload | before | after |
|---|---|---|
| full scan aggregate | 0.60× | **0.71×** |
| GROUP BY | 0.63× | **0.75×** |
| join + filter | 0.46× | **0.49×** |
| 5-term predicate | 0.26× | **0.62×** |

This is *not* the expression compiler this engine already tried and reverted
(`4398402e` → `e7d0da69`, "net-negative, wrong lever"). That linearized
expressions into a bytecode program, and a Rust interpreter's dispatch loop —
without computed goto — costs more than the inlined recursive match it replaced.
This removes the per-row work rather than relocating it, and leaves the general
evaluator in place for everything it does not cover.

The obvious next step — specializing each term on the column's static type, so an
integer comparison becomes a direct `i64` compare instead of a `cmp_values`
dispatch — **was built and measured worse**: 3.5 ns a term against 2.7. Storing
terms as an enum meant the column and operator accessors each became a match over
four variants, and the compare a two-level match on `(term, cell)`; that dispatch
costs more per row than the call it removes. A micro-benchmark had predicted a
2.7x gain, because `black_box`ing the row inflates what both variants share and
makes the differing part look dominant — the absolute numbers were known to be
contaminated, and assuming the *ratio* transferred was the error. The ladder above
is what caught it.

A second evaluator is only correct while it agrees with the first, so it is
checked against `eval_scalar` over every pairing of values and operators. That
caught a real divergence on its first run: for `c0 > 0 AND c1 < 'm'` with a NULL
`c0`, the reduction short-circuited on the NULL and returned "no rows", while SQL's
three-valued `AND` short-circuits only on a definite FALSE — so the general path
continues to the second term and *errors*. The reduction was quietly answering a
query that should fail. It now tracks unknown separately from false, and the test
compares error-reachability rather than only the boolean.

### Streaming the join instead of materializing it

With evaluation fixed, the join workload's remaining gap had a precise shape.
Phase-timing the general path on the benchmark join (400k orders ⋈ 200k
customers, one count) in disk-first mode:

| phase | disk-first | resident |
|---|---:|---:|
| materialize the left side | 7.0 ms | 1.6 ms |
| semi-join scan of the right | 10.7 ms | 4.6 ms |
| probe + emit | 1.1 ms | 1.1 ms |

The join itself is 1.1 ms in both modes; everything else is building chunks
whose rows are read once and thrown away — and in disk-first mode every
materialized row is a decode. PostgreSQL does not materialize the probe side at
all: it hashes the small filtered side and streams the big one.

`streamed_join_aggregate` is that execution shape. When an aggregate query has
exactly one INNER equi-join (no residual ON predicate, no ORDER BY/LIMIT/
DISTINCT, each WHERE conjunct binding one side alone), the right side is built
small — through an index when one serves its equality conjuncts, 20k fetches
instead of a 200k scan — and the left side is *streamed*, each match folded as
it is found. Nothing left is materialized, no tuples, no emit.

| workload | before | after |
|---|---|---|
| join + filter | 0.49× | **0.79×** (20.4 ms → 12.8 ms) |

Two gates keep it from regressing what already works. A *selective* equality is
the general path's home turf (index probe → small driver → index-nested-loop,
microseconds); selectivity cannot be read off the query, so it is bounded by a
capped index walk — more than `INL_MAX_LEFT` matches (or no index) means the
general path would scan too. And INNER being symmetric, the sides are oriented
by measurement: an equality-filtered side becomes the indexed side, otherwise
the smaller table does.

The differential test (each query run bare and with a `LIMIT` big enough to
change nothing, which the streamed path declines) had to be shaped twice before
it tested anything: with the indexed side keyed uniquely every bucket chain had
length one, so a fold that stopped after a chain's first match still passed —
and a fixture smaller than `INL_MAX_LEFT` declined every equality query before
the code under test ran. The current fixture puts duplicate keys on the indexed
side and an equality past the cap, and fails under three separate sabotages
(chain walk, scratch fill, residual-filter re-check).

One pre-existing slowness surfaced while guarding this: `… JOIN … WHERE c.id =
42` — a point equality on the join's far side — takes ~13 ms on the general
path in disk-first mode, because `choose_driver` does not swap toward it and
the left side is scanned in full. The streamed path correctly declines it (the
index-nested-loop *should* serve it); making the general path actually do so is
separate work.

### Masking the index-driven fetch too

The low-selectivity workload (`country = 'TR' AND created > … AND id > …`) is
served from the `country` index, not a scan — so none of the scan-side masking
applied, and each of the ~20,000 candidates was materialized whole: five
columns, two of them text cells (`email`, `name`) that neither the key
verification nor the fold ever reads.

`index_visit_eq_cols` carries the same wanted-column set the scan path uses
into the index fetch; the mask always includes the index's own columns, since
the base-is-a-hint contract requires every candidate verified against the live
row. A table with a dropped column falls back to the full fetch, for the same
positional reason the scan path declines there.

| workload | before | after |
|---|---|---|
| index, low selectivity | 0.52× | **0.65×** (3.17 ms → 2.54 ms) |

The streamed join's right-side build fetches through the same mask.

### Walking the candidates instead of searching for each

What remained after masking was the locate cost: every candidate found its row
with a fresh binary search over the sparse block index plus a walk of up to a
block's records. PostgreSQL does not pay this — its bitmap heap scan sorts the
candidates and visits pages sequentially.

The OxiDB shape of that trick turns out to be almost free, because both halves
are already sorted: `candidates()` returns ascending row ids, and `.rdat`
records are laid out in ascending row-id order. So a dense candidate set is
fetched by **one cursor walking forward**, skipping records by header length —
no searching at all (`visit_ids_masked`). Overlay rows are served from the
overlay exactly as the per-id path resolves them; a sparse candidate set (less
than one candidate per 16 base rows, where skipping costs more than searching)
declines to the per-id path, as do resident tables and tables with a dropped
column.

| workload | before | after |
|---|---|---|
| index, low selectivity | 0.65× | **0.99×** (2.54 ms → 1.64 ms, parity) |

Two notes on the estimate this replaced. The prediction was ~0.8×, from a
decomposition putting the locate at ~45–60 ns of a 128 ns candidate — low
again: the walk beat it to parity, so the search (and what it evicted from
cache) was worth more like 70 ns. And the earlier claim that resident mode's
0.77× bounded fetch-side work was wrong for this change — resident pays its own
per-candidate map descent, which a sequential walk undercuts; disk-first now
*beats* resident on this workload.

## What this does not measure

Concurrency (both were driven by one connection), larger datasets, a tuned
PostgreSQL, write-heavy workloads beyond the load, or query planning on shapes
more complex than these. Nor does it compare plans: PostgreSQL's advantage on
the scanning workloads may partly be *choosing* a better one, which is a
different problem from executing a chosen one faster.

One cosmetic difference worth knowing: floating-point sums agree to about 10
significant digits but not exactly, because the two engines add in different
orders.
