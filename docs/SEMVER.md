# Semantic Versioning Policy

**Status:** Effective once OxiDB 1.0.0 ships. The 0.x series does not commit to
backward compatibility (see the README's "WARNING — not production-ready" block
and [ADR-0003](decisions/0003-1.0-stability-scope.md)).

**Source decisions:** [ADR-0003](decisions/0003-1.0-stability-scope.md) (scope)
and [ADR-0004](decisions/0004-phase-0-answers.md) (release-policy answers).
This document operationalises those decisions. If this file and the ADRs
disagree, the ADRs win — open a PR fixing this file.

## Scope of the promise

The semver promise covers OxiDB's **1.0 stable surface** as defined in
[STABILITY.md](STABILITY.md). Features explicitly listed as experimental in
that document are NOT covered by this policy and may change in any release.

## Version components

OxiDB 1.x version numbers are `MAJOR.MINOR.PATCH`.

### PATCH (`1.X.Y` → `1.X.Y+1`)

- Bug fixes
- Documentation corrections
- Performance improvements that preserve observable behavior
- Internal refactors with no surface impact
- Security fixes (may also trigger backports per [SECURITY.md](SECURITY.md))

**Not allowed in PATCH:** new operators, new endpoints, new fields, behavior
changes — even ones the maintainer considers "obviously additive."

### MINOR (`1.X.Y` → `1.X+1.0`)

Additive changes only. Specifically allowed:

- New top-level JSON query operators (`$newoperator`)
- New JSON update operators (`$newupdate`)
- New aggregation pipeline stages or accumulators
- New optional fields on existing operators, **provided absence of the field
  preserves prior behavior exactly**
- New endpoints, new RPC methods, new wire-protocol message types behind
  the negotiated feature set (see [STABILITY.md §Wire](STABILITY.md))
- New client SDK methods
- Loosening a validation error into success when documented (e.g. accepting
  a previously-rejected input shape)
- Promotion of an experimental subsystem into the stable surface via a
  promotion ADR (see [STABILITY.md §Promotion](STABILITY.md))

### MAJOR (`1.X.Y` → `2.0.0`)

Required for any of the following — they are the breaking-change list from
[ADR-0004 §5](decisions/0004-phase-0-answers.md):

- Changing the semantics of an existing operator on inputs that were
  previously accepted (e.g. `$gte` no longer doing numeric-string coercion)
- Changing operator precedence within a query expression
- Changing the result shape of a query or aggregation stage (column names,
  nesting, presence/absence of `_id`, etc.)
- Changing implicit type coercion rules in queries (`"42"` ceasing to match
  `42` under `$eq`)
- Removing or renaming any operator that has appeared in a 1.x release
- Changing the **type** (class hierarchy) of an exception thrown for a given
  input — the exception kind is part of the contract; the human-readable
  message is not (see "Greyish" below)
- On-disk format changes that the migration tool (`oxidb migrate`) cannot
  handle automatically
- Removing or breaking-changing any client SDK method covered by 1.0

Major releases require their own ADR justifying why the additive path was
considered and rejected. See [DEPRECATION.md](DEPRECATION.md) for the timeline
between announcement and GA.

## Greyish, with explicit rules

**Exception messages** — the human-readable string is **not stable** across
1.x minors. Code that pattern-matches on error message text is on its own.
The exception **class / kind** (`OxiDbError`, `TransactionConflictError`,
etc.) IS stable.

**Performance characteristics** are **not part of the contract.** An operator
may get faster, slower, or change query-plan strategy in any 1.x minor.
Regression CI exists to catch unintentional slowdowns but does not promise
any specific latency or throughput.

**New reserved operator names** (`$something_new`) — adding one is
potentially breaking for users who happen to have `$something_new` as a
literal document field name. New reserved names require a **one-minor
deprecation announcement**: the proposed operator is added as a
no-op-on-literal-collision in 1.X, becomes the documented operator in 1.X+1.
See [DEPRECATION.md](DEPRECATION.md).

**Optional config defaults** — changing the default value of an
environment-variable-tunable knob (e.g. `OXIDB_POOL_SIZE`) is allowed in a
MINOR if the new default is strictly better for the common case AND the old
value is still accepted. Document it in the changelog under "Behavior changes."

## Release cadence

No fixed cadence. OxiDB aims for "1.x for years, not months" — SQLite-style
stability, not PostgreSQL-style yearly majors. A 2.0 ships only when a
breaking change to the 1.0 stable surface becomes necessary AND `oxidb migrate`
covers the transition. See [ADR-0004 §3](decisions/0004-phase-0-answers.md).

## Pre-release versions

- `1.0.0-rc1`, `1.0.0-rc2`, … — release candidates during the GA soak window.
  RC versions are not covered by the semver promise; the version they
  stabilise toward is.
- No alpha / beta tags are planned for the 1.0 cycle. Experimental subsystems
  are gated by feature flags inside stable releases instead (see
  [STABILITY.md](STABILITY.md)).

## When in doubt

Ask in a PR or open an issue tagged `semver-question` *before* merging the
change. A small clarification thread is cheaper than a broken contract.
