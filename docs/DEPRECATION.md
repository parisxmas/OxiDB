# Deprecation Policy

**Status:** Effective once OxiDB 1.0.0 ships.

**Source decisions:** [ADR-0003](decisions/0003-1.0-stability-scope.md)
("deprecation window 2+ minor releases" in Consequences) and
[ADR-0004 §3](decisions/0004-phase-0-answers.md) (major-version cadence with
6-month minimum announcement → GA window). This document operationalises
those decisions.

## Why this exists

Removing or changing a feature that users depend on is the most expensive
operation in a database project's lifetime. The rules below exist so that
removal is **never a surprise** to anyone running a 1.x release.

## What can be deprecated

Anything in the [1.0 stable surface](STABILITY.md). Deprecation does not
*remove* the feature — it just marks it as scheduled for removal in a
future major release. The feature continues to work, unchanged, for the
entire deprecation window.

Experimental features (see [STABILITY.md §Experimental](STABILITY.md)) do
not require deprecation announcements — they may be reshaped or removed in
any 1.x release. They get a changelog note, not a deprecation cycle.

## Notice period

| Item being deprecated                          | Minimum notice |
|------------------------------------------------|----------------|
| A stable-surface operator, stage, or accumulator | 2 minor releases AND announcement of the 2.0 release it will be removed in |
| A stable client SDK method                     | 2 minor releases of that client AND the same 2.0 announcement |
| An env-var name (when replaced)                | 2 minor releases; both names accepted during the window |
| A wire-protocol message (when replaced)        | 2 minor releases; the OxiWire `HELLO` handshake negotiates both during the window |
| An on-disk format version                      | 1 minor release where `oxidb migrate` upgrades it AND announcement of the 2.0 it will be refused in |

"2 minor releases" means: if removal lands in 2.0.0, the deprecation notice
must appear no later than the 1.X-2 minor (i.e. two minors before the
removal-bearing major). Combined with the 6-month-from-announcement-to-GA
rule in [SEMVER.md](SEMVER.md), the minimum real-world deprecation horizon
is **9–12 months**.

## How deprecation is announced

Every deprecation requires all four of the following:

1. **Changelog entry** under a dedicated `### Deprecated` heading in the
   release that adds the deprecation marker. The entry names the feature,
   the replacement (if any), and the target removal version.
2. **Code marker** —
   - Rust: `#[deprecated(since = "1.X.0", note = "use Y; will be removed in 2.0")]`
   - Other clients: the language's idiomatic equivalent (Python
     `DeprecationWarning`, .NET `[Obsolete]`, JS console warning on first
     use per process, Go doc comment `// Deprecated:` prefix per
     [Go documentation conventions](https://go.dev/wiki/Deprecated))
3. **Documentation update** — the feature's docs page in `docs/` gets a
   header banner pointing to the replacement, and `STABILITY.md` notes the
   pending move.
4. **Runtime warning** when feasible — for engine-level deprecations, emit
   a one-shot log warning on first use per process (gated by
   `OXIDB_SUPPRESS_DEPRECATION_WARNINGS=1` for users who can't fix the
   call sites yet).

## During the deprecation window

- The deprecated feature **must continue to work, unchanged**. Bug fixes
  still apply; performance work is best-effort.
- Releases inside the window may not silently change the deprecated
  feature's behavior. A behavior change before removal counts as a separate
  breaking change and requires its own ADR.
- New documentation should not introduce the deprecated feature except in
  migration-from-X sections.

## Removal

Removal lands in a major release (`2.0.0`, `3.0.0`, …). The release notes
must:

- List every deprecation removed in this major
- Link to the deprecation announcement releases for each
- Include the `oxidb migrate` command or steps that move users off the
  removed feature, where applicable

After removal, the deprecation marker is gone — there is no "removed in
2.0.0" stub left behind in 2.x code.

## Reserved-name introductions

A special case from [SEMVER.md §Greyish](SEMVER.md): introducing a new
reserved JSON operator name (e.g. `$something_new`) is potentially breaking
for users who happen to have `$something_new` as a literal document field.
The rollout is two minors:

- **1.X**: the proposed operator is added as a *no-op-on-literal-collision*.
  If a document literally has a field `$something_new`, the engine treats
  the query as it always did (literal match). Changelog: "Reserved
  `$something_new` for use in 1.X+1 as a query operator."
- **1.X+1**: `$something_new` becomes the documented operator. Documents
  with a literal `$something_new` field name need to either escape the key
  or migrate.

This is the only case where a "deprecation" runs only one minor — because
the deprecated *thing* (a literal field name beginning with `$`) was always
discouraged.

## Exception: security removal

If a feature is found to have an unfixable security vulnerability, it may
be removed in a PATCH release without going through the deprecation window.
Such removals MUST be documented in [SECURITY.md](SECURITY.md) and the
release notes, with a clear "remove or replace by upgrading to ≥ 1.X.Y"
instruction. This exception is rare and not a backdoor for routine cleanups.
