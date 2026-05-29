# Architecture Decision Records (ADRs)

This folder is **append-only**. Each file records a deliberate
architectural choice — the context that led to it, the decision, and
its consequences — so the reasoning isn't lost six months later when
someone asks "why don't we…?"

## Adding a new ADR

1. Copy the format of an existing entry.
2. Number it sequentially: `NNNN-short-kebab-case-title.md`.
3. Open a PR. The discussion lives in the PR; the merged ADR records
   the outcome.

## Superseding a decision

Don't rewrite an old ADR — write a new one that references it
("Supersedes ADR-0001"). The old file stays as the historical record.

## Status values

| Status | Meaning |
|---|---|
| **Proposed**  | Open PR, decision still under discussion |
| **Accepted**  | Merged; in force |
| **Superseded by ADR-NNNN** | Replaced by a later decision |
| **Deprecated** | No longer applicable, not yet replaced |

## Index

| # | Title | Status |
|---|---|---|
| [0001](0001-julia-no-dbinterface.md) | Julia clients do not implement `DBInterface.jl` | Accepted |
| [0002](0002-mongodb-bench-in-network.md) | MongoDB comparison benchmark runs in-network, not from the host | Accepted |
| [0003](0003-1.0-stability-scope.md) | 1.0 stability surface and scope | Accepted |
| [0004](0004-phase-0-answers.md) | Release policy: Phase 0 answers (promotion criteria, LTS, cadence, client tiering, DSL stability) | Accepted |
| [0005](0005-raft-fault-injection-scope.md) | Raft / HA fault-injection — what exists, what's missing for full Jepsen-grade | Accepted |
| [0006](0006-cern-testing-gap-estimates.md) | Effort estimates for remaining CERN-grade testing gaps across categories 1, 2, 3, 5, 6, 7, 8 | Accepted |
| [0007](0007-wal-commit-record-atomic-recovery.md) | In-WAL commit record for atomic transaction recovery | Proposed |
| [0008](0008-cross-shard-aggregation-merge.md) | Correct cross-shard aggregation merge in OxiPool | Accepted |
