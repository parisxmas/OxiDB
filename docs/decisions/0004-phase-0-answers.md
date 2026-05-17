# ADR 0004 — Release policy: Phase 0 answers

**Status:** Accepted
**Date:** 2026-05-18
**Related:** [ADR-0003](0003-1.0-stability-scope.md) (raised these five
questions in its "Open questions" section); Phase 5 of ADR-0003 will
operationalise these decisions into `docs/SEMVER.md`,
`docs/STABILITY.md`, `docs/DEPRECATION.md`, and `docs/SECURITY.md`.

## Context

[ADR-0003](0003-1.0-stability-scope.md) defined which surfaces are in
and out of OxiDB 1.0 but deferred five release-policy decisions to a
Phase 0 follow-up:

1. Promotion criteria for an experimental subsystem → stable
2. LTS length for the 1.0 series
3. Major-version cadence
4. Per-client 1.0 scope (all 9 clients or some shipped as experimental)
5. What counts as a "breaking change" for the JSON query/update DSL

This ADR answers all five. Future Phase 5 documents (`SEMVER.md`,
`STABILITY.md`, etc.) translate them into operational policy text;
this ADR is the *decision* with rationale.

## Decisions

### 1. Promotion criteria: experimental → stable

A subsystem currently outside the 1.0 stable surface (per ADR-0003 —
PITR, Raft, FDW, OxiScript, vector search, security rules + JWT, the
alternate wire protocols, change streams, scheduler) moves into the
stable surface only when **all** of the following hold:

| # | Criterion |
|---|-----------|
| 1 | The subsystem's surface (API + on-disk format + wire shape) has been on master for **≥ 6 months without breaking changes** |
| 2 | A written spec exists in `docs/` — for on-disk format, wire shape, or API as appropriate |
| 3 | Line coverage for the subsystem's code is **≥ 70%** in CI, with at least one integration test exercising the full request → durable-state → readback cycle |
| 4 | At least **one external user** is running it in production (a documented case study, not a CI fixture) |
| 5 | A dedicated **promotion ADR** is merged ("ADR-00NN: promote X to stable in 1.Y"), which records the version it lands in and the version's deprecation window for any prior experimental shape |

**Rationale.** "On master without breakage" measures shape stability;
the spec is for future readers; the coverage threshold catches the
common "we shipped a feature with only happy-path tests" trap; the
external user is the only check that the surface is actually fit for
purpose outside its author's head; the promotion ADR is the
recorded act of taking on the multi-year commitment.

### 2. LTS length

The **1.0 series is supported for at least 24 months** from 1.0.0 GA.
"Supported" means:

- No breaking changes to the 1.0 stable surface land in any 1.X release
  during the window.
- Security fixes are issued for any 1.X minor still inside the support
  window.
- Critical bug fixes (data loss, corruption, security) are backported to
  **the current minor and the previous one**.

Each 1.X minor stops receiving non-security fixes **12 months after the
next minor releases**, so users always have at least a year to upgrade.

After 24 months from 1.0.0 GA, the 1.0 series moves into "security
only" mode for an additional 12 months and then becomes EOL.

**Rationale.** 24 months is the floor that unlocks CERN-grade and
enterprise procurement conversations (see
[`docs/cern-compatibility.md`](../cern-compatibility.md), Layer 1).
Shorter is operationally easier but excludes the adoption audience 1.0
is being shipped for in the first place. The 12-month backport overlap
inside the window is the standard "users can plan an upgrade on their
own cadence" guarantee.

### 3. Major-version cadence

**No fixed cadence. 2.0 ships only when a breaking change to the 1.0
stable surface becomes necessary AND a migration path exists.** The
target is "1.x for years, not months" — SQLite-style stability, not
PostgreSQL-style yearly major bumps.

Concretely:

- A proposed breaking change to a 1.0 stable surface requires its own
  ADR with explicit justification (the additive-extension path was
  considered and rejected, why), and lands no sooner than the next
  major release.
- Until 2.0 is necessary, new functionality lands as **additive**
  changes in 1.X minors (within the 1.0 stable surface's
  forward-compatibility rules — see decision 5) or as
  **experimental** features (outside the stable surface).
- When 2.0 is announced, the 1.0 series enters its EOL countdown (see
  decision 2) — the announcement starts a clock, the GA is at least 6
  months later, and `oxidb migrate` covers every 1.x → 2.0 transition.

**Rationale.** A yearly-major cadence (PostgreSQL-style) defeats the
purpose of LTS for the audience that needs LTS. Open-ended-1.x
(SQLite-style) defers cost to a hypothetical future where the cost is
better understood. We can revisit this if 1.0 hits hard architectural
limits — but that's an ADR, not a calendar.

### 4. Per-client 1.0 scope

The 9-ish official clients ship at two tiers under 1.0:

**Tier A — covered by the 1.0 backward-compat promise:**

- `python/` — TCP client (`oxidb`)
- `python-embedded/` — embedded FFI (`oxidb-embedded`)
- `go/` — Go client (`oxidb-go`)
- `julia/OxiDb` — TCP client (already document-only and Tables.jl-aligned per ADR-0001)
- `julia/OxiDbEmbedded` — embedded FFI
- `dotnet/OxiDb.Client.Tcp`
- `dotnet/OxiDb.Client.Embedded`
- `dotnet/OxiDb.EntityFrameworkCore`
- `oxidb-js/` — JS/TS (REST + WebSocket)
- `oxidb-jdbc/` — JDBC driver

Each tier-A client must, before 1.0 GA: tag a `1.0` package version,
audit its public surface, mark experimental APIs as such, and have an
`api/v1.json` snapshot in CI (Phase 3 of ADR-0003).

**Tier B — shipped but explicitly Experimental, NOT covered by 1.0
backward-compat:**

- `php/` (PHP TCP + FFI bindings) — thinner surface, less production use
- `swift/` — iOS C-FFI bindings — thinner surface, less production use

Tier-B clients ship with a `1.0-experimental` (or `0.x` rolling) version
suffix and a header note in their README explicitly stating that breaking
changes can occur in any 1.X minor of the *engine*. They graduate to
Tier A via the same promotion-ADR mechanism in decision 1.

**Rationale.** Honest tiering beats a one-size-fits-all promise that
gets quietly broken on the thinner client lines. Splitting the
maintenance burden by current bandwidth lets 1.0 actually ship without
blocking on PHP/Swift work that may take longer.

### 5. What counts as a "breaking change" for the JSON query/update DSL

**Additive (allowed in any 1.X minor, NOT a breaking change):**

- New top-level operators (`$newoperator`)
- New update operators (`$newupdate`)
- New aggregation stages or accumulators
- New optional fields on existing operators, **provided that absence of
  the field preserves prior behavior exactly**
- Loosening a validation error into success — when the loosening is
  documented (e.g. accepting a previously-rejected input shape)

**Breaking (requires a 2.0 release):**

- Changing the semantics of an existing operator on inputs that were
  previously accepted (e.g. `$gte` no longer doing numeric-string
  coercion)
- Changing operator precedence within a query expression
- Changing the result shape of a query or aggregation stage
  (column names, nesting, presence/absence of `_id` etc.)
- Changing implicit type coercion rules in queries (e.g. `"42"` ceasing
  to match `42` under `$eq`)
- Removing or renaming any operator that has appeared in a 1.X release
- Changing the *type* of an exception thrown for a given input (the
  exception class hierarchy is part of the contract; see below)

**Greyish, with explicit rules:**

- **Exception messages** (the human-readable string) are **not** stable
  across 1.X minors. Code that pattern-matches on error message text
  is on its own. The exception **class / kind** (`OxiDbError`,
  `TransactionConflictError`, etc.) IS stable.
- **Performance characteristics** are **not** part of the contract.
  An operator may get faster, slower, or change query-plan strategy in
  any 1.X minor. Regression CI exists to catch unintentional slowdowns
  but does not promise any specific latency or throughput.
- **New reserved operator names** (`$something_new`) — adding one is
  potentially breaking for users who happened to have `$something_new`
  as a literal document field name. New reserved names require a
  **one-minor deprecation announcement**: the proposed new operator
  is added as a no-op-on-literal-collision in 1.X, becomes the
  documented operator in 1.X+1.

**Rationale.** Drawing this line in writing is the only way the JSON
DSL stays usable across the 1.x series without surprise breakages. The
implicit-coercion rule in particular has bitten every NoSQL database
that didn't pin it early. Performance carve-out is realistic; pinning
specific latency numbers would either be unenforceable or paint the
optimizer into a corner.

## Consequences

**Positive:**

- ADR-0003's Phase 0 closes — Phase 1 (format freeze) can start.
- The promotion-criteria rule (decision 1) gives experimental subsystems
  a concrete, achievable path into stability instead of being
  permanently "experimental" by default.
- 24-month LTS + open-ended major-version cadence is the combination
  CERN-grade and enterprise audiences require (see
  [`docs/cern-compatibility.md`](../cern-compatibility.md), Layer 1).
- The DSL-breakage rule in particular spares the project the perpetual
  "is this a bug or a feature?" debate every NoSQL DB has fought.

**Negative / accepted trade-offs:**

- 24-month LTS is a real maintenance burden — security backports to
  N-1 minor for a multi-year window requires sustained capacity.
  Without external maintainers (see ADR-0003 Layer 5), this falls
  entirely on the current maintainer.
- The per-client Tier-A/Tier-B split visibly demotes PHP and Swift
  surface, which is honest but may disappoint users of those clients.
- The "no fixed major cadence" stance means *some* future user pain
  is deferred — features that would benefit from a clean break get
  shoehorned in as additive surface instead.

## Revisiting

- If a Tier-B client gains active maintenance + use, promote it to
  Tier A via decision 1's mechanism.
- If maintainer capacity meaningfully exceeds the 24-month LTS burden,
  consider extending — but only after at least one full LTS cycle has
  been delivered.
- If a breaking-change pressure builds up across multiple operators or
  shapes, that's the trigger for the 2.0 ADR; this ADR's "open-ended
  cadence" stance is itself revisitable.
