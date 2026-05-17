# Upgrade-chain fixtures

CERN-grade testing roadmap category 6 (upgrade / migration). See
[`docs/testing-roadmap.md`](../../../docs/testing-roadmap.md) and the
test that consumes these: [`tests/cern_upgrade_chain.rs`](../../cern_upgrade_chain.rs).

## What's in this directory

One `<version>.tar.gz` file per shipped server version, each
containing a known-shape OxiDB data directory:

| Field | What it tests |
|---|---|
| `events/` collection with 10 docs (`id`, `name`, `n`) | Storage + WAL replay |
| `events.n` index | `.fidx` OXIX-headed file readability |
| `meta/` collection populated transactionally | `_tx_commit_log` (OXTX) gating |
| `_blobs/audit/handover.txt` blob | `.meta` JSON `format_version` (v1+) |

The contract: **any committed fixture in this directory MUST open
cleanly with the current engine and pass the assertions in
`cern_upgrade_chain.rs`.** A backward-incompat to the on-disk
formats has to either:

1. Land a documented migration step AND a fresh fixture for the
   new version, OR
2. Bump the relevant format `format_version` field so the OLD
   fixture is refused with a clean `Error::IncompatibleFormat`
   rather than silently mis-read (see `src/blob.rs::open()` for
   the pattern).

## How to add a new version's fixture

When shipping `vX.Y.Z`:

1. Run the generator helper (only runs when explicitly invoked):
   ```bash
   cargo test --test cern_upgrade_chain \
       generate_fixture_for_current_version \
       -- --ignored --nocapture
   ```
2. This writes `tests/fixtures/upgrade/vX.Y.Z.tar.gz`.
3. Commit it. PR description should note "adds upgrade-chain
   fixture for vX.Y.Z".
4. Subsequent engine versions will read it via
   `read_all_committed_fixtures` automatically — no test edits
   needed.

## What this is NOT

- Not a **forward-compat** test (we never claim newer-version
  fixtures can be opened by older engines; that's only the
  `Error::IncompatibleFormat` tripwire's domain, see
  [`docs/format/blob-object.md`](../../../docs/format/blob-object.md)).
- Not a **byte-stable** test (we don't assert the on-disk bytes
  match across versions; only that reads + queries + aggregations
  return correct results).
- Not a **migration step** test (those land in separate test
  files per migration, named `tests/migration_<from>_to_<to>.rs`).
