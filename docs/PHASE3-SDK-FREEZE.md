# Phase 3 — Client SDK Freeze Pattern

**Status:** In progress. Python TCP client is the template; the other
9 Tier-A clients follow the same pattern.

**Source:** [ADR-0003 Phase 3](decisions/0003-1.0-stability-scope.md)
and [ADR-0004 §4](decisions/0004-phase-0-answers.md) (Tier-A list of
10 clients covered by the 1.0 backward-compat promise).

## What "SDK freeze" means

Each Tier-A client commits a structural snapshot of its public surface
to `<client>/api/v1.json`. The snapshot captures classes, methods,
signatures, and exception types — everything that a 1.0 user could
write code against. A CI test re-generates the snapshot on every PR
and fails if the result diverges from the committed file.

This does NOT prevent surface changes — it requires them to be
**intentional and visible**. A maintainer who genuinely wants to evolve
the surface regenerates the snapshot, commits it as part of their PR,
and adds the `intentional-v1-bump` label so the diff is reviewed
deliberately.

## Reference implementation: Python TCP client

```
python/
├── oxidb.py                          # the client
├── api/
│   └── v1.json                       # committed snapshot, 1008 lines, ~68 public symbols
└── scripts/
    ├── generate_api_snapshot.py      # regenerate by introspecting `oxidb`
    └── check_api_snapshot.py         # CI gate; exits non-zero on diff
```

### How the snapshot looks

```json
{
  "module": "oxidb",
  "schema_version": 1,
  "classes": {
    "OxiDbClient": {
      "bases": [],
      "methods": {
        "insert": {
          "params": [
            {"name": "collection", "kind": "POSITIONAL_OR_KEYWORD", "annotation": "str", "default": "<no-default>"},
            {"name": "doc", "kind": "POSITIONAL_OR_KEYWORD", "annotation": "dict", "default": "<no-default>"}
          ],
          "return": "<no-annotation>"
        },
        ...
      }
    },
    "OxiDbError": { ... },
    "TransactionConflictError": { ... }
  },
  "functions": {}
}
```

### Local workflow

```bash
# Verify current code matches the committed snapshot
cd python && python3 scripts/check_api_snapshot.py

# Intentionally regenerate after a surface change
cd python && python3 scripts/generate_api_snapshot.py > api/v1.json
git add api/v1.json
```

### CI workflow

CI runs `python3 python/scripts/check_api_snapshot.py` per push. A failure
prints a unified diff of what changed and the instructions to regenerate.
Reviewers see the diff in the PR and decide whether the change is
intentional.

## Adapting the pattern to other clients

The pattern translates to each language's idioms:

| Client                                    | Introspection mechanism                              | Snapshot path                                    |
|-------------------------------------------|------------------------------------------------------|--------------------------------------------------|
| `python/` (TCP)                           | `inspect` module (done — see above)                 | `python/api/v1.json`                             |
| `python-embedded/` (FFI)                  | `inspect` over the published `oxidb_embedded` module| `python-embedded/api/v1.json`                    |
| `go/` (OxiWire)                           | `go/ast` walk of the public `oxidb` package         | `go/api/v1.json`                                 |
| `julia/OxiDb` (TCP)                       | `methods(OxiDb)` + `propertynames` over the module  | `julia/OxiDb/api/v1.json`                        |
| `julia/OxiDbEmbedded` (FFI)               | same                                                | `julia/OxiDbEmbedded/api/v1.json`                |
| `dotnet/OxiDb.Client.Tcp`                 | `Microsoft.CodeAnalysis` over the assembly         | `dotnet/OxiDb.Client.Tcp/api/v1.json`            |
| `dotnet/OxiDb.Client.Embedded`            | same                                                | `dotnet/OxiDb.Client.Embedded/api/v1.json`       |
| `dotnet/OxiDb.EntityFrameworkCore`        | same                                                | `dotnet/OxiDb.EntityFrameworkCore/api/v1.json`   |
| `oxidb-js/` (REST + WebSocket)            | `typescript` compiler API + `.d.ts` parsing        | `oxidb-js/api/v1.json`                           |
| `oxidb-jdbc/`                             | Reflection over the JAR                             | `oxidb-jdbc/api/v1.json`                         |

Every snapshot has the same top-level shape (`module`, `schema_version`,
`classes`, `functions`) so cross-language diffs are visually consistent and
reviewers don't have to learn 10 separate formats.

## Stable vs experimental within a client

A client's `api/v1.json` captures the **public** surface — but not every
public method is necessarily 1.0-stable. ADR-0004 §1 promotes via a
separate ADR; meanwhile, methods that touch experimental subsystems
(see [STABILITY.md §Experimental](STABILITY.md)) should be marked in
the client docstring/JSDoc/XML doc-comment so the user is aware.

The snapshot does not currently track stable-vs-experimental — that's a
future enhancement (likely a `"stability": "stable"|"experimental"` field
per method). For 1.0 GA the rule is: if it's in the snapshot, it's
covered by the 1.0 promise unless the client's docs explicitly mark
it experimental.

## Tier-B clients

`php/` and `swift/` ship with explicit `1.0-experimental` markers per
[STABILITY.md §Tier-B](STABILITY.md). They do **not** maintain `api/v1.json`
snapshots — their surface is allowed to change in any 1.x minor of the
engine. They graduate to Tier A (and adopt this pattern) via a promotion
ADR per [ADR-0004 §1](decisions/0004-phase-0-answers.md).

## Acceptance for Phase 3 closure

ADR-0003 Phase 3 is considered closed when:

- All 10 Tier-A clients have `<client>/api/v1.json` committed
- All 10 have a check-snapshot script wired into CI
- The CI matrix (Phase 6 RC deliverable, per [compat-matrix.md](format/compat-matrix.md))
  runs `{client v1.0} × {server v0.x, v1.0}` per push and stays green
