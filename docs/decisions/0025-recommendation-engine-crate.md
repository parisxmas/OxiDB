# ADR-0025: Fourth engine — a co-occurrence recommendation engine crate (`oxidb-rec`)

**Status:** Accepted (v1 implemented, phases 1–5) — 2026-08-09
**Branch:** `feat/rec-engine-crate`
**Supersedes:** —
**Related:** [ADR-0010](0010-sql-engine-crate.md) (the second-engine pattern this
follows exactly), [ADR-0012](0012-multi-database.md) (per-database engine
instances), [ADR-0011](0011-cross-engine-transactions.md) (why this engine takes
part in no transaction),
[`oxidb-server/src/handler.rs`](../../oxidb-server/src/handler.rs) (the `engine`
discriminator, lines 209–225),
[`oxidb-server/src/tsdb_bridge.rs`](../../oxidb-server/src/tsdb_bridge.rs) (the
bridge this one mirrors),
[`src/vector.rs`](../../src/vector.rs) (the adjacent capability, and what this is
*not*).

## Context

"Customers who bought this also bought" is the most requested query a shop puts
to a database, and today OxiDB answers it the way everyone answers it: not at
all. The application is expected to self-join its order lines in the SQL engine,
score the pairs, and write the result somewhere — a batch job, run nightly,
whose output is stale by construction.

That answer is unsatisfying for a reason worth stating precisely: **the work is
not shaped like a query.** It is shaped like an index.

- The **unit** is a basket, not a document or a row. The datum is a set, and its
  meaning lives entirely in which other members it appears beside.
- The **update** is a pair-count increment: for a basket B, `|B|²/2` increments,
  independent of catalogue size. Nothing about it wants a planner.
- The **read** is a partial sort over one sparse row, which the document engine
  can only express as a full collection scan.
- The **scoring** is domain mathematics (co-occurrence significance) that has a
  right answer and a great many wrong ones. Left to the caller, every caller
  gets it wrong the same way — see §4.

Forcing this into the document engine means storing an N×N matrix as documents
(at 10k items, 100M cells, nearly all zero), or recomputing from order history
per request. Forcing it into the SQL engine means the nightly batch job we are
trying to escape. Neither is a modelling failure by the caller; it is a missing
access method.

Two clarifications about what this is not:

**Not vector search.** `src/vector.rs` already ships HNSW with three metrics and
is wired to the protocol (`create_vector_index`, `vector_search`). That answers
*"which items resemble this one"* from an embedding somebody else computed. This
answers *"which items are actually bought beside this one"* from observed
behaviour, needs no model, no embedding and no training, and is right on the
first order rather than after a fit. They are complementary and neither
substitutes for the other.

**Not a graph engine.** The obvious modelling — `(customer)-[bought]->(item)`,
traverse two hops — is a trap at this workload. From a bestseller, hop one
reaches 100k customers and hop two reaches millions of paths, per query. Graph
traversal wins on sparse neighbourhoods and path-finding; this is aggregation
over a dense one, and the right structure is an adjacency counter, not a
traversal.

### Prior art

There is no famous database engine in this slot, which is itself the finding.
The algorithm is Amazon's item-to-item collaborative filtering (Linden, Smith &
York, *IEEE Internet Computing*, 2003); the scoring is Dunning's log-likelihood
ratio (*Computational Linguistics* 19(1), 1993), as used by Apache Mahout's
`ItemSimilarityJob` and its later Correlated Cross-Occurrence work. In
production this is normally assembled as Spark → indicator matrix →
Elasticsearch (the retired Apache PredictionIO "Universal Recommender"), or
bought as a service (Amazon Personalize, Recombee), or self-hosted as a system
on top of other databases (Gorse).

Every one of those puts a refresh cycle between the order and the
recommendation. The reason to build this as an engine rather than a pipeline is
to delete that cycle, not to add another implementation of the mathematics.

## Decision

Add **`oxidb-rec`**, a fourth engine crate mounted as a sibling behind the same
server, following ADR-0010's pattern without deviation: its own files, its own
WAL, its own recovery, reached by an additive `engine: "rec"` discriminator, and
sharing with the other engines only the process, the port, auth and RBAC.

### 1. Crate layout

```
oxidb-rec/
  src/lib.rs        Rec facade: open/new, track, related, for_basket, stats,
                    checkpoint, retention, backup_begin/write/end, restore
  src/model.rs      item interner, Scoring enum, config, basket types
  src/store.rs      per-model counters, bucket roll, scoring, top-K queries
  src/persist.rs    MANIFEST + generation snapshot + per-generation WAL
  tests/            e2e, persistence/recovery, scoring, retention
```

No dependency on `oxidb` (the document crate) and none on `oxidb-sql`. The crate
is usable standalone, as `oxidb-tsdb` is.

### 2. Data model — interned items, bucketed sparse counters

Items arrive as strings (a slug, a SKU) and are interned to `u32`. One interner
per database, shared by all models: the catalogue is the same catalogue.

A **model** is a named event space — `purchase`, `view`, `cart`. Viewing and
buying are different signals with different base rates; they are counted
separately and blended at query time, never pooled. Each model holds:

```rust
baskets:     [u32; BUCKETS]                    // total baskets per period
item_counts: HashMap<u32, Row>                 // baskets containing item x
pair_counts: HashMap<u32, HashMap<u32, Row>>   // baskets containing both

struct Row {
    counts: [u32; BUCKETS],
    epoch:  u32,   // the bucket period counts[0] belongs to — see §3
}
```

Fixed-size `[u32; BUCKETS]` rather than `Vec`: at a few million pairs, `Vec`'s
24-byte header alone outweighs the counts it points at.

Pair rows are stored **in both directions**. This doubles the counter memory and
makes `related(x)` a single row lookup, which is the hot path; the alternative
(canonical `x < y` ordering) halves memory and turns every query into a scan.
Budget ≈ `distinct_pairs × 2 × 36 bytes` plus map overhead — about 72 MB at one
million distinct pairs.

`track` is **idempotent on basket id**: the same order submitted twice counts
once. This is not only correctness hygiene — it is what makes WAL replay after a
crash safe without a commit protocol (§6).

The seen-set that backs idempotence is **bucketed like everything else**: basket
ids are kept per period, and when a period falls off the window (§3) its ids
fall with it. The idempotence window therefore equals the counting window —
which is the only coherent choice, since a replay of a basket older than the
window would increment counters no live bucket holds — and the set's size is
bounded by one window of baskets instead of growing with the model's lifetime.
An unbounded seen-set would have been this engine's own resident-index mistake.

Two ingest guards:

- **Basket size cap** (default 50 items). A 500-line order is `125_000`
  increments and is almost always a bulk import or a test fixture; it carries no
  preference signal and would dominate the counters. Skipped baskets are counted
  and reported by `stats`, never silently dropped.
- **Intra-basket dedup.** The same item in two variants is one occurrence.

### 3. Time — rolling buckets, not a retention sweep

Counters are kept per time bucket: `BUCKETS = 8` slots of a configurable width
(default 30 days), so the default window is 240 days. Conceptually, when a
write arrives in a period newer than slot 0, every counter shifts one slot left
and the oldest falls off.

**The shift is lazy, never a global sweep.** Each row carries the bucket period
its slot 0 belongs to (`Row.epoch`); a row is shifted **when it is next
touched** — by a write or a read — by however many periods it is behind, which
is a fixed-size array rotation, O(BUCKETS), per touch. An earlier draft shifted
the whole pair map eagerly at the period boundary: a full scan under the write
lock, monthly at the default width — rare, but an arbitrary request would have
carried a multi-second stall at millions of pairs, the same shape as a
foreground index build. Lazy shifting deletes the pause outright; the cost is
one `u32` comparison per row access, and rows nobody touches simply stay stale
until the checkpoint walks them anyway.

That checkpoint walk doubles as garbage collection: a row whose counts are all
zero after shifting — untouched for a full window — is dropped from the
snapshot rather than carried forever. Either way, the design still removes the
alternative it was chosen against: keeping every basket's contents in memory
forever so that expiring events can be decremented one at a time.

Scores weight the buckets by an exponential half-life (a query parameter,
default 2 buckets). Last season's fashion therefore stops governing this
season's recommendations without any rebuild, and "rising together right now"
becomes a query rather than a separate pipeline.

### 4. Scoring — in the engine, and LLR by default

Counters are stored raw; scores are derived at query time from the decayed
`co(x,y)`, `n(x)`, `n(y)` and `N`. Changing the scoring mode is therefore a
parameter, never a rebuild.

Supported modes: `llr` (default), `cosine`, `jaccard`, `lift`, `count`.

**Why LLR is the default.** Take two rare items bought together exactly once.
Cosine scores that `1 / sqrt(1 × 1) = 1.0` — a perfect score, the best any pair
can get — because the denominator is as small as the numerator. Raw counts have
the opposite failure: everything looks related to the bestseller. A
recommendation list built on either fills with coincidences at one end and
batteries at the other. Dunning's G² asks instead how surprised we should be by
the co-occurrence given the base rates, and answers "not at all" for the single
coincidence. On sparse retail data this is not a matter of taste; the lists are
visibly different.

Over the 2×2 contingency table `k11 = co`, `k12 = n(x) − co`,
`k21 = n(y) − co`, `k22 = N − n(x) − n(y) + co`:

```
H(v...)  = Σ xlogx(vᵢ) subtracted from xlogx(Σ vᵢ)      [xlogx(0) = 0]
G²       = 2 · (H(rows) + H(cols) − H(cells)),  clamped at 0
```

Computed in `f64` throughout, because decay makes the counts fractional.

A `min_support` parameter (default 1) additionally floors the raw co-count.

### 5. Query surface

| Op | Meaning |
|---|---|
| `track` | ingest one basket into a model (idempotent on basket id) |
| `related` | items most associated with one item |
| `for_basket` | items most associated with a *set* — the cart page |
| `stats` | models, items, pairs, baskets, skipped, memory |
| `checkpoint` / `backup` / `restore` | as TSDB |

`for_basket` sums each candidate's score across the basket's members, excluding
the basket itself and any caller-supplied `exclude` list. It is deliberately
part of v1 rather than a later addition: item→item alone is the weakest useful
form, and the cart page is where recommendations convert.

**Cold start returns empty.** An item with no observed co-occurrence yields no
rows; it does not fall back to bestsellers. The engine should not disguise "I
have no evidence" as "here is a recommendation" — the caller decides the
fallback, and may request one explicitly with `fallback: "popular"`.

### 6. On-disk layout — fully separate files

Per database, under `<db_dir>/rec/`, mirroring TSDB's discipline:

```
MANIFEST            {"generation": N} — the authoritative generation
snap.<N>.rec        binary snapshot: interner + all model counters
wal.<N>.log         events appended since that snapshot
```

Checkpoint writes `snap.<N+1>`, atomically renames MANIFEST (temp+rename — the
single commit point), then starts `wal.<N+1>` and drops generation N. A crash
before the rename recovers from N. Backup pins a generation and archives it with
the engine lock released, as `Tsdb::backup_begin/write/end` does.

Recovery is snapshot load + WAL replay. Because `track` is idempotent on basket
id and the seen-set is inside the snapshot (bucketed, §2 — so the snapshot's
size is bounded by one window), replaying records that the snapshot already
contains is a no-op — the WAL needs no commit records and no truncation
discipline. Row epochs (§3) are snapshotted with their rows: a lazy shift owed
before the crash is still owed after it, and falls due on the same next touch.

### 7. Wire routing — additive, exactly as ADR-0010

Requests carrying `engine: "rec"`, or the reserved `cmd: "rec"`, route to the
new bridge at the existing junction in
[`handler.rs`](../../oxidb-server/src/handler.rs) (lines 209–225), beside
`"sql"` and `"tsdb"`. A missing or `"doc"` engine leaves the document path
byte-for-byte unchanged. Gated by `OXIDB_REC=1`, like `OXIDB_TSDB=1`.

RBAC gains two entries mirroring TSDB's (`rbac.rs:51`, `:82`): the read role may
run `related`, `for_basket` and `stats`; `track`, `retention` and `checkpoint`
require write.

### 8. Not replicated in v1 — inherited, and stated plainly

In cluster mode, `is_write_command` (`async_server.rs:663`) enumerates document
writes and `sql_is_write` covers SQL. **Neither matches `tsdb`** — the third
engine's writes are already not replicated through Raft; they are served by
`dispatch_local` and stay on the node that received them.

`rec` inherits that gap rather than fixing it. `track` is a write and will not
be replicated in v1, so in a cluster each node accumulates only the baskets it
personally received, and `related` answers differ per node.

This is acceptable for v1 and unacceptable for GA, and it is recorded here so
that it is a decision rather than a discovery. Two properties make the eventual
fix cheap: `track` is idempotent, so at-least-once delivery is sufficient and no
consensus on ordering is required; and the state is a commutative counter, so
replicas converge under any interleaving. A follow-up ADR should close this for
`tsdb` and `rec` together, since they have the same shape and the same gap.

### 9. Open decisions (resolve during implementation)

1. **Bucket width default.** 30 days suits fashion; groceries want a week and
   B2B wants a quarter. Per-model or per-database configuration?
2. **Cross-model blending.** v1 queries one model. Should `for_basket` accept
   weighted model mixing (`purchase: 1.0, view: 0.3`), or does the caller issue
   two queries and merge?

Two earlier open points are resolved into the design above rather than left
open: `BUCKETS = 8` is **fixed** (the stack-array row is most of the memory
argument in §2, and the lazy-shift epoch in §3 assumes a fixed rotation);
counters **saturate** rather than wrap on `u32` overflow, and `stats` reports
any saturation observed — a count pinned at the ceiling skews a score, but a
wrapped one inverts it.

## Phasing

| Phase | Content | Rough size |
|---|---|---|
| 1 | Crate: model, store, scoring, in-memory `track`/`related`/`for_basket`/`stats` + tests | ~900 lines |
| 2 | Persistence: MANIFEST/snapshot/WAL, recovery, backup/restore + tests | ~600 lines |
| 3 | Server bridge, `handler.rs` routing, RBAC, `db_admin` teardown, `disk_usage`, Dockerfile | ~400 lines |
| 4 | Cobra plugin bindings (`rec_*`; and the missing `vector_*`, which today has no binding at all) | ~150 lines |
| 5 | Validation against a real catalogue: LLR vs. cosine vs. count on live orders | — |

TSDB's total footprint is ~4270 lines (2348 crate + 575 tests + 660 bridge + 431
REST + 256 ADR). `rec` should land smaller: there is no Gorilla codec, no bit
packing and no line protocol to write. The PostgREST surface
([ADR-0019](0019-postgrest-rest-surface.md)) is **out of scope for v1**.

Phase 5 is not optional garnish. The claim this engine makes is that scoring
choice changes the lists visibly, and that claim should be demonstrated on real
orders before the default is fixed in a release.

### Phase 5 results (UCI Online Retail, 541k order lines, 2010–2011)

Run via `oxidb-rec/examples/scoring_validation.rs` (the dataset is fetched, not
committed). Findings, with top-10 overlap against LLR as the number:

- **Count vs LLR: visibly different, worst where it matters.** Overlap ranges
  10/10 (lunch bags — a family so tight every mode agrees) down to **2/10 on a
  rare probe**, where count's top item is the shop's global bestseller at a
  co-occurrence of 2 — the base-rate contamination §4 predicts, in the flesh.
  On the mid-popularity cakestand probe, 5/10: count's list admits bestsellers
  the LLR list correctly omits.
- **Cosine vs LLR: converges on head items (9–10/10), diverges exactly where
  §4 predicts.** For a probe in only 5 baskets, cosine's podium is a
  three-way 0.41 tie of perfectly-exclusive one-off pairings, while LLR ranks
  the evidenced items first — including the probe's sister product (the other
  bookcover tape) and demoting the coincidences. Cosine also *excludes* LLR's
  top item because its larger margin dilutes the cosine denominator: the
  exclusivity bias, observed.
- **LLR's lists read right on inspection**: the Regency cakestand pulls the
  Regency teacups/teapot/sugar-bowl family; the red lunch bag pulls the other
  lunch bags; the tape pulls the other tape.

The LLR default stands validated; the claim survives contact with real
orders, with one calibration: cosine's failure needs a small-margin probe to
manifest, so a head-only evaluation would wrongly conclude the modes agree.

## Consequences

**Good**

- The recommendation is current the instant an order is written. There is no
  refresh cycle to schedule, miss, or explain — which is the entire reason to
  make this an engine instead of a nightly job.
- Update cost is independent of catalogue size, so the engine's behaviour does
  not change as the shop grows.
- The scoring mistake everyone makes (§4) is made once, in one place, correctly,
  with the alternatives available as a parameter.
- No batch infrastructure, no Spark, no model training, no GPU, and no embedding
  provider — the capability is reachable by any application that can already
  write an order.

**Bad**

- A fourth engine is a fourth thing to back up, monitor, size, document and
  break. The counter is that it reuses ADR-0010's routing, TSDB's persistence
  discipline and the existing auth/RBAC wholesale.
- Memory is proportional to distinct co-occurring pairs, which grows with
  catalogue *diversity* rather than order count and is harder to capacity-plan
  than a row count. `stats` must report it prominently.
- Not replicated in v1 (§8). In a cluster, recommendations are per-node until a
  follow-up closes this.
- Co-occurrence is deliberately not personalisation. It answers "what goes with
  this", not "what should *this person* see next". Blurring those two in the
  documentation would set an expectation the engine does not meet.

**Neutral**

- No transaction participation, by design and consistent with
  [ADR-0011](0011-cross-engine-transactions.md). A recommendation counter that
  is a few seconds behind an order is not an inconsistency anyone can observe;
  paying for cross-engine atomicity here would buy nothing.
