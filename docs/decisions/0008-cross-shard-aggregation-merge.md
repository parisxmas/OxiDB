# ADR-0008: Correct cross-shard aggregation merge in OxiPool

**Status:** Accepted (implemented 2026-05-29)
**Date:** 2026-05-29
**Supersedes:** —
**Related:** [`oxipool/src/scatter.rs`](../../oxipool/src/scatter.rs),
[`oxipool/src/shard.rs`](../../oxipool/src/shard.rs),
[`src/pipeline.rs`](../../src/pipeline.rs),
[`docs/aggregation.md`](../aggregation.md)

## Context

OxiPool shards data across nodes and fans queries out scatter-gather
(`oxipool/src/scatter.rs`). For `aggregate`, the merge strategy is
`MergeStrategy::ConcatDocs` (`scatter.rs:79`), and `merge_doc_arrays`
(`scatter.rs:269`) simply **concatenates** each shard's result array. Each
shard runs the *entire* pipeline on its local subset and the router glues the
outputs together with no second-stage reduction.

That is correct only for **per-document** pipelines (`$match`, `$project`,
`$addFields`, `$unwind`) and for the **shard-key-targeted** case
(`shard.rs:339` routes an aggregate to a single shard *only* when the first
stage is a `$match` carrying the shard key). For every other cross-shard
pipeline the result is silently wrong:

| Stage | Cross-shard result today |
|---|---|
| `$group` | each shard groups its own docs → a key present on N shards yields N separate group docs, never merged/re-summed |
| `$sort` | per-shard sorted runs concatenated → not globally ordered |
| `$limit` / `$skip` | applied per shard → up to `limit × num_shards` returned; skip is per-shard |
| `$count` (stage) | one count doc per shard, not summed |

Example: `aggregate([{$group:{_id:"$city",total:{$sum:"$amt"}}}])` over 3
shards can return `{Tokyo:100},{Tokyo:250},{Tokyo:80}` instead of
`{Tokyo:430}`. A *silently incomplete/duplicated* result is worse than a
clear error.

Constraints that shape the fix:

- **OxiPool is intentionally lean** — its only deps are tokio, serde,
  serde_json, crc32fast (`oxipool/Cargo.toml`). It does **not** depend on the
  `oxidb` core engine, and we'd like to keep the proxy small.
- The core aggregation executor (`src/pipeline.rs`) has a rich model:
  stages `Match, Group, Sort, Skip, Limit, Project, Count, Unwind, AddFields,
  Lookup` (+ `DateHistogram`/`DateBucketFill`) and accumulators `Sum, Avg,
  Min, Max, Count, First, Last, Push, AddToSet, Percentile`.

## The core idea: split the pipeline

The standard solution (MongoDB's `splitPipelineForSharding`) is to split a
pipeline at the first *blocking/reducing* stage into:

- a **shard-local pipeline** — runs on each shard, and
- a **merge pipeline** — runs once on the concatenated shard outputs.

For a `$group`, the shard-local part must emit **partial group state**, and
the merge part re-groups by `_id` **combining** those partials. Accumulator
mergeability is the crux:

| Accumulator | Decomposable? | Shard emits | Merge step |
|---|---|---|---|
| `Sum`, `Count` | ✅ | partial sum / count | sum of partials |
| `Min`, `Max` | ✅ | partial min / max | min / max of partials |
| `Push` | ✅ | partial array | concat (order across shards is undefined anyway) |
| `AddToSet` | ✅ | partial set | union |
| `Avg` | ⚠️ rewrite | `{sum, count}` partial | `Σsum / Σcount` at finalize |
| `First`, `Last` | ⚠️ order-dependent | requires a preceding global `$sort`; shard emits candidate + sort key | re-sort, then first/last |
| `Percentile` | ❌ | needs all values | ship raw values (expensive) or approximate (t-digest) |

So a correct merge is **not** "re-run the same pipeline at the router" — for
`Avg`/`Percentile`/`First`/`Last` the shard-local pipeline must be *rewritten*
to emit partial state and the merge pipeline rewritten to finalize.

`$sort` + `$limit`: each shard does a local top-k (sort + limit N), and the
merge does a k-way merge + global limit/skip. `$count`: shard → count, merge →
sum.

## Decision (proposed), in phases

### Phase 0 — Stop being silently wrong (small, do first)

In `shard.rs`, when an `aggregate` is routed `ScatterGather` (not shard-key
targeted) **and** its pipeline contains any reducing stage (`$group`, `$sort`,
`$limit`, `$skip`, `$count`, `$sortByCount`, `$bucket`, `$facet`, …), return a
**clear error** from OxiPool instead of concatenating:

> "cross-shard aggregation with $group/$sort/$limit is not yet supported; add
> a `$match` on the shard key to target a single shard, or run against a single
> node."

This is a few lines and converts a correctness bug into an honest limitation.
Per-document pipelines and shard-key-targeted aggregates keep working.

### Phase 1 — Decomposable merge (the 80% case)

Implement the pipeline split for the **decomposable** accumulators
(`Sum, Min, Max, Count, Push, AddToSet`) plus `$sort`/`$limit`/`$skip`/`$count`.
Pipelines using only these get a real cross-shard merge; anything else still
hits the Phase 0 error.

Three ways to run the merge pass at the router — pick one:

- **Option A — shared `oxidb-aggregate` crate.** Extract the stage/accumulator
  model + a pure in-memory executor (`Vec<Value> → Vec<Value>`, no storage) from
  `src/pipeline.rs` into a small crate that both `oxidb` and `oxipool` depend
  on. Router builds the merge pipeline and runs it. **Pros:** single source of
  truth, no logic divergence. **Cons:** requires carving a storage-free
  executor entry point out of `pipeline.rs` (today it's coupled to
  collection/index/doc-cache lookups); some refactor.
- **Option B — reimplement a minimal reducer in OxiPool.** Hand-write the
  merge for the decomposable set in `oxipool`. **Pros:** keeps the proxy
  dependency-free. **Cons:** duplicates semantics (number coercion, index-value
  ordering, null handling) — high risk of subtle divergence from the core
  engine; explicitly discouraged.
- **Option C — server-side `merge_aggregate` command.** Add a command to
  `oxidb-server` that takes the concatenated partial results + the merge
  pipeline and finalizes using the *real* core executor; OxiPool ships the
  partials to one node and returns its answer. **Pros:** reuses the core
  executor verbatim, OxiPool stays thin. **Cons:** one extra round trip and one
  shard does the merge work; needs a "run pipeline over a supplied doc array"
  server entry point (which is independently useful).

**Recommendation:** **Option A** if we expect cross-shard analytics to be hot
(executor reuse + no round trip), otherwise **Option C** as the lower-risk
first cut (reuses core semantics with minimal OxiPool change). Avoid B.

Mechanics regardless of option:
- OxiPool already parses the pipeline JSON for routing (`shard.rs:341`); extend
  that to compute `(shard_pipeline, merge_pipeline)`.
- Forward `shard_pipeline` to each shard (replacing the original `pipeline` in
  the payload), collect partials, run `merge_pipeline` over the concatenation.
- `scatter.rs` gains a `MergeStrategy::Aggregate { merge_pipeline }` arm
  instead of `ConcatDocs` for reducing pipelines; `ConcatDocs` stays for
  per-document ones.

### Phase 2 — Non-decomposable accumulators

Handle `Avg` (ship `{sum,count}` partials), `First`/`Last` (require/inject a
global sort key), and `Percentile` (ship raw values for exact, or adopt
t-digest for approximate). `$facet`/`$bucket`/`$lookup` across shards each need
their own treatment (`$lookup` against a sharded foreign collection is its own
hard problem — likely Phase 3 or "unsupported, error").

## Consequences

- **Correctness:** cross-shard `$group`/`$sort`/`$limit`/`$count` become
  correct (Phase 1) or honestly rejected (Phase 0) instead of silently wrong.
- **Coupling:** Options A/C reuse core semantics (no divergence); A adds an
  `oxipool → oxidb-aggregate` dep, C adds a server command + a round trip.
- **Compat:** Phase 0 changes some currently-"succeeding" (wrong) queries into
  errors — call it out in the changelog; it is a bug fix, not a regression.
- **Testing:** add a sharded aggregation suite (extend
  `tests/cluster/` / `ShardReplicaRealWorldTest/`) asserting cross-shard
  `$group`+`$sum`/`$avg`, `$sort`+`$limit` top-k, and `$count` match a
  single-node baseline over the same data.

## Effort estimate

| Scope | Rough effort |
|---|---|
| Phase 0 — reject reducing cross-shard pipelines with a clear error + tests | ~0.5 day |
| Phase 1, Option C — `merge_aggregate` server command + pipeline split in OxiPool + tests | ~3–4 days |
| Phase 1, Option A — extract storage-free `oxidb-aggregate` executor crate + wire OxiPool + tests | ~1 week |
| Phase 2 — Avg/First/Last/Percentile partial-state + facet/bucket | +3–5 days |

## Implementation status (2026-05-29)

Implemented via **Option C** (server-side merge executor reused over the
network), with the pipeline split in a shared crate so OxiPool stays lean:

- **`oxidb-agg-merge`** (new workspace crate, `serde_json` only) —
  `split_pipeline()` returns `Passthrough` / `Split { shard, merge }` /
  `Unsupported(reason)`. Decomposes `$sum`, `$count`, `$min`, `$max`, `$avg`
  group accumulators and `$sort`/`$limit`/`$skip`/`$count` blockers.
- **`OxiDb::aggregate_docs(pipeline, docs)`** + server command
  `aggregate_docs` — runs a pipeline over a supplied doc array via the real
  `Pipeline::execute_from`, so the merge has identical semantics to a
  single-node run (no logic duplicated in the proxy). Read-level RBAC.
- **OxiPool** `scatter_aggregate()` — splits, fans the shard pipeline out,
  concatenates partials, runs the merge pipeline on one shard via
  `aggregate_docs`. `Passthrough` → plain concat; `Unsupported` → clear error
  (Phase 0).
- **Tests** — `tests/cross_shard_agg.rs` proves split→shard→merge equals the
  single-node baseline across 1–5 shards for sum/count/min/max/avg/mixed/
  match-group/sort-limit/count/passthrough; plus split-shape unit tests in the
  crate.

**Delivered:** Phases 0 + 1 in full, and Phase 2's `$avg`. **Deferred to a
follow-up** (honest `Unsupported` error today, not silent-wrong): `$push`,
`$addToSet`, `$percentile`/`$median`/`$stdDev`, `$first`/`$last` (the core
lacks an array-merge expression; `$first`/`$last` are order-undefined across
shards), and `$facet`/`$bucket`/`$sortByCount`/`$dateHistogram`/`$lookup`.
