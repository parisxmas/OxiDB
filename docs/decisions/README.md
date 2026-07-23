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
| [0009](0009-disk-first-storage.md) | Disk-first storage mode (opt-in) | Accepted |
| [0010](0010-sql-engine-crate.md) | Standalone SQL engine crate mounted as a second engine | Accepted |
| [0011](0011-cross-engine-transactions.md) | Cross-engine transactions (document + SQL) | Deferred |
| [0012](0012-multi-database.md) | Multiple databases, shared by both engines | Accepted |
| [0013](0013-dotnet-ef-core.md) | Full .NET EF Core support for the SQL engine | Accepted |
| [0014](0014-cobra-stored-procedures.md) | Cobra as the compiled stored-procedure language | Accepted |
| [0015](0015-durable-mqtt-qos.md) | Durable MQTT — persistent sessions and honest QoS 1 | Accepted |
| [0016](0016-amqp-protocol.md) | AMQP 0-9-1 (RabbitMQ protocol) on the shared broker substrate | Accepted |
| [0017](0017-mvcc-lite-read-snapshots.md) | MVCC-lite — read snapshots for the document engine | Accepted |
| [0018](0018-offset-index-memory.md) | Shrinking the disk-first offset index (packed DocLocation + fenced mmap index) | Accepted (Phase 1); Phase 2 Proposed |
| [0019](0019-postgrest-rest-surface.md) | PostgREST-compatible auto-REST surface (document + SQL + TSDB engines) | Accepted (all phases landed) |
| [0020](0020-oxibase-control-plane.md) | OxiBase — a control plane for multi-tenant provisioning | Proposed |
