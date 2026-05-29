# ADR-0007: In-WAL commit record for atomic transaction recovery

**Status:** Proposed
**Date:** 2026-05-29
**Supersedes:** —
**Related:** [`docs/format/wal.md`](../format/wal.md),
[`docs/format/tx-commit-log.md`](../format/tx-commit-log.md),
[`docs/transactions.md`](../transactions.md)

## Context

A code-review pass (branch `bugfix/code-review-20260529`) flagged a
crash-atomicity gap in transaction recovery on the default
`BTreeCollection` backend, and an attempted fix had to be reverted
because it was incompatible with the current durability design. This ADR
records the gap, why the naive fix fails, and scopes the correct fix so it
can be done deliberately rather than rushed.

### The gap

`OxiDb::commit_transaction` (`src/engine.rs`) commits in this order:

1. Remove the tx from the active set.
2. Resolve involved collections.
3. OCC validation (versions match).
4. Prepare per-collection mutations.
5. **`col.log_wal_batch(entries)`** — write each collection's WAL entries,
   each tagged with the real `tx_id`, with an fsync (strict mode).
6. **`tx_log.mark_committed(tx_id)`** — the intended commit point.
7. Collect change events.
8. `col.apply_prepared(...)` — apply to in-memory storage.
9. `col.checkpoint_wal()` — **only sets `dirty = true`**; the per-collection
   `.btree` snapshot is written later by the periodic sync thread / on
   shutdown.
10. **`tx_log.remove_committed(tx_id)`** — drop the marker now that the data
    is in in-memory storage.

The `TxCommitLog` (`src/tx_log.rs`) is a *global, separate* file
(`_tx_commit_log`) holding the set of "committed but maybe not yet
checkpointed" tx ids. Recovery (`OxiDb::open_internal`) reads it, and the
default backend replays the WAL via
`BTreeCollection::replay_wal → replay_entries`, which currently applies
**every** entry unconditionally (ignores `tx_id`).

Two crash windows expose the problem:

- **Crash between step 5 and step 6** — WAL entries tagged `tx_id=N` are
  durable, but `N` was never marked committed. On restart the unconditional
  replay applies them anyway → **a transaction that never committed becomes
  durable** (atomicity violation).
- **Normal operation** — because step 9 only marks dirty and step 10 removes
  the marker, committed data routinely lives **only in the WAL**, tagged
  `tx_id=N`, with `N` absent from `_tx_commit_log`. Recovery *must* replay
  these entries.

### Why the naive fix was reverted

The obvious fix — "replay a transactional entry only if its `tx_id` is in the
committed set" — was implemented and reverted. It breaks the normal case
above: after `remove_committed` (step 10), the committed set no longer
contains `N`, yet `N`'s entries are still the only durable copy of the data.
Filtering by the committed set therefore **discards legitimately-committed
data**, which the `acid_test::test_durability_tx_committed_survives_reopen`
integration test caught immediately.

The root issue is that the global `_tx_commit_log` marker lifecycle
(`mark` before apply, `remove` after in-memory apply) cannot, on its own,
distinguish at the WAL level between:

- "tagged `N`, uncommitted (crashed 5→6)" → must discard, vs.
- "tagged `N`, committed + applied + marker removed, not yet checkpointed"
  → must replay.

Both look identical to the WAL scanner (entries tagged `N`, `N` not in the
set). The current code resolves the ambiguity by **always replaying**, which
favors not-losing-committed-data over not-applying-uncommitted-data. That is
the safer of the two one-sided choices, but it is not atomic.

## Decision (proposed)

Make the WAL **self-describing** about commit status by writing an explicit
**commit record** as the last entry of a transaction, and replay a
transaction's entries during recovery **iff** its commit record is present.
This removes the dependency on the out-of-band `_tx_commit_log` for
crash-atomicity and is the textbook ARIES-style approach.

### Design sketch

1. **New WAL entry variant** `WalEntry::Commit { tx_id }` (and the symmetric
   v2 form). Add opcodes alongside the existing ones in `src/wal.rs`:
   `OP_COMMIT = 4` / `OP_COMMIT_V2 = 0x84`. The record carries only `tx_id`
   (plus the v2 GSN/wall-clock when PITR is on). Bump nothing else — the
   framing, CRC, and v1/v2 split already accommodate a new op byte. Update
   `encode_record`/`decode_record`/`read_entries`/`recover` to round-trip it.
   This is a forward-compatible WAL format change (old readers would not
   understand `OP_COMMIT`; gate behind a `WAL_VERSION` bump to 2 and keep
   reading v1 for back-compat — see "Format/compat" below).

2. **Commit protocol change** (`commit_transaction`): after step 5 writes the
   data entries to each involved collection's WAL, write a `Commit { tx_id }`
   record to each involved collection's WAL **and fsync** — *that* fsync is
   the durable commit point, replacing `tx_log.mark_committed` as the
   linearization point. Steps 8–9 (apply + dirty) stay. Step 10
   (`remove_committed`) and the whole `mark_committed` dance become
   unnecessary for crash-atomicity (see "TxCommitLog" below).

3. **Recovery change** (`replay_wal`/`replay_entries` in
   `src/btree_collection.rs`): two-pass per WAL stream.
   - Pass 1: scan and collect the set of `tx_id`s that have a `Commit` record.
   - Pass 2: apply `Insert`/`Update`/`Delete` entries where `tx_id == 0`
     (non-transactional, always durable) **or** `tx_id ∈ committed-set`.
     Buffer or two-pass; a single `read_entries()` already materializes the
     vector, so this is just a pre-scan of the same vector. Drop entries for
     transactions with no commit record (crashed 5→6).

### The cross-collection atomicity wrinkle (the hard part)

A transaction can span multiple collections, and **each collection has its
own WAL**. Writing a `Commit` record to collection A's WAL and then crashing
before writing it to collection B's WAL leaves the transaction half-committed
(A replays, B doesn't). The per-collection commit record makes each
collection's slice atomic, but **not the transaction as a whole**.

Options, in increasing order of cost/correctness:

- **(a) Document the boundary.** Keep per-collection commit records; declare
  that multi-collection transactions are atomic *per collection* but the
  inter-collection commit is best-effort across a crash. Cheapest; honest;
  probably unacceptable if we advertise ACID multi-collection txns.
- **(b) Two-phase commit across the collection WALs.** Write a `Prepare`
  record to every involved WAL (fsync all), then a `Commit` record to every
  involved WAL (fsync all). Recovery applies a tx only if **every** involved
  WAL has its `Commit` (or, with a prepared-but-not-committed tx, consult a
  decision). Requires recording the participant set somewhere durable (e.g.
  the commit record lists the collections, or a tiny global decision log).
- **(c) Single shared transaction journal.** Route all transactional WAL
  writes for a commit through one append-only journal (a real redo log)
  rather than N per-collection WALs, with one commit record and one fsync.
  This is the cleanest ARIES design and also collapses N fsyncs → 1 (a perf
  win, see below), but it is the largest change: collections would replay
  from the shared journal up to their last checkpoint LSN, and the
  per-collection WAL becomes a non-transactional fast path only.

**Recommendation:** (c) is the right long-term design; (b) is a defensible
interim that reuses the per-collection WALs. (a) is only acceptable if we
re-scope the ACID claim. Pick during the implementation PR.

### TxCommitLog fate

With an in-WAL commit record, `_tx_commit_log` is no longer load-bearing for
crash-atomicity. It can either be **removed** (simplest) or **kept** purely as
a group-commit fsync coalescer for the commit-record write (its existing
strength — one fsync for N concurrent commits). If kept, its semantics change
from "source of truth for recovery" to "batching optimization," and
`read_committed`/`clear` at startup go away.

## Consequences

- **Correctness:** recovery becomes truly atomic — uncommitted (crashed-mid-
  commit) transactions are dropped, committed ones are always replayed, with
  no reliance on a separate marker file whose lifecycle races the data.
- **Performance:** option (c) reduces a multi-collection commit from N WAL
  fsyncs to 1. Options (a)/(b) are fsync-neutral or add one fsync per
  participant.
- **Format:** `WAL_VERSION` bumps to 2; v1 WALs still read (no commit records
  → every entry treated as committed, matching today's behavior, so existing
  data files recover unchanged). Document in
  [`docs/format/wal.md`](../format/wal.md) and the compat matrix.
- **Testing:** add crash-injection tests for both windows — kill between data-
  write and commit-record (must NOT recover the tx) and kill between commit-
  record and checkpoint (MUST recover the tx) — plus a multi-collection
  partial-commit crash for whichever cross-collection option is chosen. The
  existing `acid_test` suite is the natural home.

## Effort estimate

| Scope | Rough effort |
|---|---|
| Commit record + single-collection atomicity (opt a) + tests | ~1–2 days |
| + Cross-collection 2PC over per-collection WALs (opt b) | +2–3 days |
| + Shared transaction journal (opt c, supersedes per-collection WAL for txns) | +1–2 weeks |

## Interim state (today)

Until this lands, recovery **favors durability over atomicity**: committed
transactions always survive a crash; a transaction that crashes in the narrow
window between its WAL fsync and the (now-removed) commit marker may be
partially applied on restart. The OCC commit lock added in the same review
(`commit_lock` in `src/engine.rs`) is independent and remains in force.
