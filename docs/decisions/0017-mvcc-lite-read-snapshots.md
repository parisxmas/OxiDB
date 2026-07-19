# ADR-0017: MVCC-lite — read snapshots for the document engine

**Status:** Proposed — 2026-07-19 (design only; do not build until a trigger
below fires)
**Related:** [`docs/isolation.md`](../isolation.md),
[`src/engine.rs`](../../src/engine.rs) (OCC),
[`src/btree_storage.rs`](../../src/btree_storage.rs) (disk-first `.bdat`),
[`src/pitr.rs`](../../src/pitr.rs) (GSN),
[ADR-0011](0011-cross-engine-transactions.md) (the precedent for writing a
design down and then not building it)

## Context

The document engine's concurrency model is OCC over item read-sets, and
`docs/isolation.md` states its guarantee precisely: committed transactions
are serializable with respect to the items they read and wrote; **phantoms
and torn reads for non-transactional observers are admitted**. Readers never
block writers and writers never block readers — which is most of what MVCC
is usually bought for, and we already have it without version chains.

What we do **not** have is a consistent multi-document snapshot:

1. **Torn observers.** A non-transactional `find`/`aggregate` scanning while
   a transaction commits can see document A after the commit and document B
   before it — half a transfer. A balance-sum report run against a busy
   collection can produce a total that was never true at any moment.
2. **Read-only transactions can abort.** An OCC transaction that only reads
   still validates its read-set at commit; a long read concurrent with
   writes retries, possibly repeatedly, despite writing nothing.
3. **No `AS OF` queries.** History exists only through PITR restore, not as
   something you can query in place.

Nobody has hit these hard enough to ask. This ADR exists so that when
someone does, the answer is a plan instead of a shrug — and so that the
*scope* of the answer is already decided to be small.

## Decision (proposed)

If triggered, build **MVCC-lite: snapshot visibility for the read path
only.** The write path stays exactly what it is — OCC three-phase commit,
`find_for_update` pessimistic locks, group-commit fsync. No full MVCC, no
writer-side snapshot isolation, no first-committer-wins rewrite.

### Design sketch

**Commit sequence.** A per-database monotonic commit counter stamps every
committed write batch (the PITR GSN already proves the pattern; this one is
in-memory-cheap and does not require PITR to be on). Each document's entry
in the existing version map gains the commit seq that last touched it.

**Recent-versions side map.** The key economy: documents that have not
changed recently have exactly one version and pay **nothing**. On update or
delete of a document, the *previous* value's location is pushed into a
bounded per-collection side map `doc_id → [(commit_seq, prior_location)]`.
Under the disk-first default this is nearly free storage-wise: the
append-only `.bdat` already retains old record bytes until compaction, so a
"prior version" is an offset, not a copy. (In-RAM collections keep a copy;
that cost is one reason this stays read-path-only.)

**Snapshot reads.** A snapshot read (an `aggregate`, a `find` that opts in,
a read-only transaction) captures the commit counter once. Resolving a
document: if its current commit seq ≤ snapshot, use it as-is — the common,
zero-overhead case; otherwise walk the side map for the newest version ≤
snapshot. Deletes resolve through tombstone versions the same way.

**Indexes: additive until the horizon.** Index entries are *added*
immediately but their *removal* is deferred past the snapshot horizon, so an
index scan yields a superset of what any active snapshot can see; the reader
re-verifies the predicate against the snapshot-resolved document (the
post-filter machinery already exists). Two published costs follow:
index-only `count` and pure index-backed paths are **latest-only**
optimizations — under a snapshot they degrade to resolve-and-check; and
index memory carries deferred removals until the horizon passes.

**Horizon and GC.** The horizon is the minimum commit seq across active
snapshots (absent any, the current counter). A background sweep — the TTL
thread's cadence is fine — prunes side-map versions and deferred index
removals behind the horizon. Disk-first compaction respects the horizon
before reclaiming old record bytes. A snapshot held open forever is a bloat
bug by definition, so snapshots carry a max age (default minutes, not
hours); exceeding it fails the snapshot, never the writers.

### Phases

- **Phase 1 — snapshot aggregation.** Commit counter, side map, horizon GC;
  `aggregate` runs under a snapshot by default (it is the torn-sum victim).
  Fixes problem 1 where it bites.
- **Phase 2 — read-only snapshot transactions.** `begin(snapshot: true)`:
  no read-set, no validation, cannot abort, cannot write. Fixes problem 2.
- **Phase 3 (optional) — `AS OF`.** A configured retention window widens the
  horizon by wall-clock time; reads may name a timestamp. Only if asked.

## Alternatives considered

- **Full MVCC** (writer snapshots, first-committer-wins): rejected. It
  replaces OCC rather than complementing it, imports vacuum-class bloat
  failure modes, and pays the index-versioning price everywhere instead of
  only under active snapshots. The engine's most valuable property is that
  its guarantees are small enough to crash-test exhaustively.
- **Per-collection read latch for consistent scans**: cheap, but it blocks
  writers for the scan's duration — surrendering the one MVCC property we
  already have. Rejected.
- **"Use PITR restore for consistent reads"**: technically true, practically
  an insult to whoever needed a correct sum.
- **Status quo**: the current answer, and the right one until a trigger
  fires. `docs/isolation.md` documents the gap honestly.

## Triggers — do not build until one of these is real

1. A user reports incorrect multi-document read results (torn sums, reports,
   reconciliation) against a concurrently-written collection, and
   `find_for_update` / single-document reads cannot express their need.
2. A user needs long read-only transactions that must not abort or retry.
3. A concrete `AS OF` / time-travel query requirement.

Absent these, this ADR stays Proposed, like ADR-0011 — a design is not a
mandate.

## Consequences

**Positive.** Torn observers become impossible where it matters
(aggregation first); read-only work stops retrying; the disk-first layout's
append-only file makes old versions cheap; the write path — the part every
durability test pins — does not change at all.

**Negative / cost.** A new background GC with a bloat failure mode (bounded
by the snapshot max-age); index memory grows with write rate × snapshot
age; index-only count and index-backed sort lose their shortcuts under
snapshots (kept for latest reads); the SIGKILL crash matrix gains a
dimension (a crash must never resurrect a pruned version or lose a deferred
index removal — recovery rebuilds both from the WAL replay it already does).

## Verification (when built)

The discipline is inherited, not invented: a characterization suite extends
`docs/isolation.md` with the new snapshot guarantees stated exactly;
crash tests SIGKILL mid-commit-under-snapshot and mid-GC; a torn-sum test
runs transfers against a summing aggregation and fails without snapshots
(red-first, like the WAL sentinel's crash-replay test); and a bloat test
holds a snapshot past max-age and asserts writers never stall and the
horizon advances.
