# ADR-0011: Cross-engine transactions (document + SQL)

**Status:** Proposed — 2026-07-02 (design only; no implementation)
**Related:** [ADR-0010](0010-sql-engine-crate.md),
[`src/pitr.rs`](../../src/pitr.rs), [`src/transaction.rs`](../../src/transaction.rs),
[`oxidb-sql/src/transaction.rs`](../../oxidb-sql/src/transaction.rs)

## Context

Since ADR-0010, one server process can host two engines: the document engine
and the standalone SQL engine. By design they share **no state and no files** —
which also means there is no way to update a document collection and a SQL
table atomically. Today a caller doing both must accept that a crash between
the two commits leaves one applied and the other not.

Each engine already has single-engine atomicity:

- **Document engine**: OCC transactions with a 3-phase commit (prepare →
  validate versions → commit), a transaction log, and WAL replay recovery.
  With PITR enabled, every durable WAL write carries a global, monotonic,
  wall-clock-stamped **GSN** issued by the `ArchiveSequencer`.
- **SQL engine**: buffered transactions flushed as one WAL `Batch` record
  (single fsync, all-or-nothing on replay).

The missing piece is a commit protocol spanning both WALs.

## Decision (proposed design)

A lightweight two-phase commit coordinated by the server session, with the
document engine's **GSN sequencer promoted to a process-wide commit clock**
shared by both engines.

### 1. Shared GSN

Lift the `ArchiveSequencer` (or a thin process-wide wrapper over it) out of
the document engine so the SQL engine's WAL records can also carry GSNs.
The SQL WAL frame gains an optional GSN field (v2 records, mirroring the
document WAL's format bump). Off when cross-engine transactions are unused —
zero cost, like PITR.

### 2. Commit protocol

A cross-engine transaction `XTX` buffers writes in both engines exactly as
their native transactions do today. On commit, the session layer runs:

1. **Prepare** — both engines validate (document OCC version checks; SQL PK /
   schema checks) and stage their batch *without* applying:
   the document engine writes `XtxPrepare { xid, gsn }` + payload to its WAL;
   the SQL engine writes `XtxPrepare { xid, gsn }` + its `Batch` to its WAL.
   Both fsync. Either failure → both roll back (nothing was applied).
2. **Decide** — a single `XtxCommit { xid, gsn }` record is appended + fsynced
   to the **document engine's WAL** (the designated decision log; it is the
   engine with the sequencer).
3. **Apply** — both engines apply their staged batches in memory and append
   `XtxApplied { xid }` markers lazily (no extra fsync needed for
   correctness).

### 3. Recovery

On startup each engine replays its WAL as today, but a staged `XtxPrepare`
without a matching local `XtxApplied` is held back until the engines
cross-check: the document WAL is the decision log, so

- decision record present → both apply the staged batch (idempotent);
- absent → both discard it (the transaction never committed).

This gives all-or-nothing across engines with **three fsyncs per cross-engine
commit** (two prepares + one decision) — the same count as the document
engine's existing 3-fsync protocol.

### 4. Wire surface

```json
{ "cmd": "xtx_begin" }                          → { "xid": ... }
{ "cmd": "insert", "collection": "c", "xid": ... }
{ "engine": "sql", "cmd": "sql", "sql": "...", "xid": ... }
{ "cmd": "xtx_commit", "xid": ... }             → atomic across both engines
```

RBAC: `xtx_*` at ReadWrite. Cluster mode: out of scope for v1 of this ADR
(requires Raft-coordinating the decision record; noted as future work).

## Consequences

- Both WALs learn two new record types (`XtxPrepare`, marker); format bumps
  are backward compatible (old records replay unchanged).
- The GSN sequencer becomes a small shared component; the engines otherwise
  stay fully separate — no shared locks, no shared files, no shared caches.
- Recovery becomes a two-log protocol *only* for prepared-but-undecided
  transactions; the common path (no XTX in the tail) is unchanged.
- Latency: 3 fsyncs per cross-engine commit vs 1 (SQL) / 3 (document) today.

## Why not…

- **One shared WAL for both engines** — recreates exactly the coupling
  ADR-0010 rejected; a corrupt shared log takes down both engines.
- **Best-effort dual commit (no prepare)** — the status quo; loses atomicity
  on crash between the two commits.
- **Full XA/external coordinator** — heavyweight for an embedded/in-process
  pair of engines that already share a process lifetime.

## Phasing

1. Shared sequencer extraction (no behavior change).
2. SQL WAL v2 records with GSN.
3. Staged-batch (`XtxPrepare`) support + recovery holdback in both engines.
4. Session-layer commit protocol + wire commands + tests (crash-point matrix).
5. (Later, own ADR) Raft-replicated cross-engine transactions.
