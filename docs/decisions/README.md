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
