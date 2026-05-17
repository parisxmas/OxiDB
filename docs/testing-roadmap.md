# Testing roadmap

**Status of this document:** roadmap, not a commitment.
**See also:** [`docs/cern-compatibility.md`](cern-compatibility.md) (the
"Layer 2 — production hardening" row is what this roadmap
operationalises).

## TL;DR

A real CERN-grade testing program is a multi-month engineering project
with eight broad categories. This document lists them with realistic
estimates and what's in vs out of the first concrete slice that
landed alongside this doc.

| # | Category | First-slice status | What "complete" looks like |
|---|---|---|---|
| 1 | **ACID & isolation** | ✅ partial — first concurrent-transfers test in [`tests/cern_acid_transfers.rs`](../tests/cern_acid_transfers.rs) | Reference anomaly suite (A5A/A5B, write skew, lost update, dirty read, phantom) |
| 2 | **Crash recovery** | ✅ partial — soft-crash WAL replay test in [`tests/cern_crash_recovery.rs`](../tests/cern_crash_recovery.rs) | Byte-offset SIGKILL matrix via a victim subprocess; ENOSPC/EIO/cosmic-bit-flip injection |
| 3 | **Performance & long-tail** | ✅ partial — bounded soak in [`tests/cern_soak.rs`](../tests/cern_soak.rs) | Multi-day soak, RSS / fd / WAL-size leak detection, HEP-shaped workload (bursty ingest + long-range scans + high-fanout reads) |
| 4 | **HA / Raft fault injection** | ❌ not started | **Jepsen-style suite** (split-brain, partition, clock skew, slow disk) — biggest single gap |
| 5 | **Security** | ❌ not started | Wire-protocol fuzzing (`cargo-fuzz`), external pentest (Cure53 / Trail of Bits), authn/authz bypass attempts |
| 6 | **Upgrade / migration** | ❌ not started | Byte-identical fixture corpus per release; N → N+1 → N+2 round-trip; downgrade where allowed |
| 7 | **Scale** | ❌ not started | 10⁹+ doc dataset, 24-hour sustained insert + scan, multi-TB on-disk |
| 8 | **Disaster recovery / drills** | ❌ not started | Power-loss VM drill, primary-site-down 24h, restore-from-cold-backup time-to-recover SLA |

(Categories #4–#8 each warrant their own multi-PR effort; ADRs to scope
them individually will come as the engine matures.)

## What "first slice" delivers

Three Rust integration tests under `tests/`, opt-in via `cargo test
-- --ignored` so a normal `cargo test` run isn't slowed by them:

### `cern_acid_transfers.rs`

A classic money-transfer atomicity test. N threads × M transfers
between two accounts, each transfer wrapped in
`begin_transaction` / `tx_update` × 2 / `commit_transaction`, OCC
retry loop on conflict. After the storm settles, the invariant
`sum(balances) == initial_total` MUST still hold — money can't be
created or destroyed.

Catches: lost update, partial commit, isolation breakage on the
critical-section path.

Does NOT yet cover: write skew, phantom, dirty read, snapshot vs
serializable distinctions. Those land in follow-up tests as the
isolation contract gets explicit names.

### `cern_crash_recovery.rs`

Two soft-crash tests using `std::mem::forget(db)` to bypass `Drop`
(which would otherwise call the graceful shutdown path):

- **committed-survives** — insert N records via auto-commit, "crash"
  (drop without shutdown), reopen, assert all N replay from the WAL.
- **uncommitted-doesn't** — start a tx, do some `tx_insert`s, "crash"
  before `commit_transaction`, reopen, assert NONE of the uncommitted
  inserts replay.

Catches: WAL fsync ordering bugs, transaction-id replay-set bugs in
`_tx_commit_log`.

Does NOT yet cover the real-world hard cases: SIGKILL at every byte
offset of a write, fsync-returns-EIO mid-batch, power-loss
simulation. Those need a subprocess + fault-injection harness — a
follow-up.

### `cern_soak.rs`

Runs an insert / read / update / delete loop for
`OXIDB_SOAK_SECS` seconds (default **30** for CI; set to e.g. `3600`
for an hourly soak). Asserts at the end:

- Engine still responsive (a final `find` returns).
- The document count is within a small drift of expected — no runaway
  bloat from update-without-cleanup, no silent doc loss.

Does NOT yet cover: process RSS leak detection (needs platform
syscall via `procfs` or `mach_task_basic_info`), WAL-size unbounded
growth check, fd-leak check. Each is a small follow-up; deliberately
out of scope here to keep the harness portable.

## How to run

```bash
# Skipped by default to keep `cargo test` fast — opt in with --ignored:
cargo test -- --ignored

# Or one at a time:
cargo test --test cern_acid_transfers -- --ignored
cargo test --test cern_crash_recovery -- --ignored
cargo test --test cern_soak           -- --ignored

# Longer soak (default 30s):
OXIDB_SOAK_SECS=600 cargo test --test cern_soak -- --ignored
```

CI integration should run the `--ignored` slice on a separate stage
that's allowed to take minutes rather than seconds.

## What's NOT in this first slice (and why)

| Out of scope | Reason | What it needs |
|---|---|---|
| Jepsen-style Raft | Multi-week engineering effort, separate harness | Cluster lifecycle controller + nemesis + linearizability checker; either Clojure Jepsen or a Rust equivalent like `madsim` |
| External pentest | Done by an external firm | Engagement with Cure53 / Trail of Bits / similar — budget + scheduling, not engineering |
| 30-day soak | Literally 30 days | A long-running CI agent with snapshot / alert hooks |
| HEP-scale 10⁹ doc | TB of disk, hours per run | Beefy CI node + storage; the test code itself is achievable, the run isn't a per-commit cost |
| Cosmic-ray simulation | Bit-flip injection at the page-cache level | A custom syscall-interposer or a controlled VM where memory bits are randomly perturbed |
| Upgrade chain (N → N+1) | Need multiple compiled engine versions co-existing | Fixture corpus snapshotted per release + a script that builds prior tag + reads with current binary |

These appear as separate rows in [`docs/cern-compatibility.md`](cern-compatibility.md)
Layer 2 / Layer 5 and will each get their own scoping ADR when their
time comes.

## Update triggers

This document should be edited (not appended-to) whenever:

- A new test category lands in this repo — bump its row to ✅ or ✅
  partial with a link.
- One of the "out of scope" items becomes scoped (e.g. ADR-00NN
  proposes the Jepsen suite) — move it from the bottom table into the
  main table.
- ADR-0003 graduates an experimental feature into the stable surface —
  the test categories that exercise that feature need to extend
  coverage.
