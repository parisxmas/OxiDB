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
| 1 | **ACID & isolation** | ✅ partial — concurrent transfers ([`tests/cern_acid_transfers.rs`](../tests/cern_acid_transfers.rs)) + 4-anomaly suite ([`tests/cern_acid_isolation.rs`](../tests/cern_acid_isolation.rs): dirty / phantom / write-skew / read-skew, all pinned, isolation level documented in [`docs/format/tx-commit-log.md`](format/tx-commit-log.md#isolation-level-observed)) | Explicit snapshot vs serializable distinction, multi-version concurrency check, reference Adya/Berenson exhaustive suite — see [ADR-0006 §1](decisions/0006-cern-testing-gap-estimates.md#category-1--acid--isolation) for effort estimates |
| 2 | **Crash recovery** | ✅ partial — soft-crash ([`tests/cern_crash_recovery.rs`](../tests/cern_crash_recovery.rs)) + hard-SIGKILL ([`tests/cern_sigkill_drill.rs`](../tests/cern_sigkill_drill.rs)) + byte-offset SIGKILL matrix ([`tests/cern_sigkill_byte_offset.rs`](../tests/cern_sigkill_byte_offset.rs)) | ENOSPC/EIO injection; cosmic-bit-flip simulation; deeper init-time kill cases — see [ADR-0006 §2](decisions/0006-cern-testing-gap-estimates.md#category-2--crash-recovery) for effort estimates |
| 3 | **Performance & long-tail** | ✅ partial — bounded soak in [`tests/cern_soak.rs`](../tests/cern_soak.rs) | Multi-day soak, RSS / fd / WAL-size leak detection, HEP-shaped workload — see [ADR-0006 §3](decisions/0006-cern-testing-gap-estimates.md#category-3--performance--long-tail) for effort estimates |
| 4 | **HA / Raft fault injection** | ✅ partial — 7 tests in [`oxidb-server/tests/raft_test.rs`](../oxidb-server/tests/raft_test.rs) (cluster formation, leader-kill failover, data consistency after failover, multi-node kill, minority-can't-elect, write replication) | Linearizability checker (Knossos / Elle), network-partition nemesis (vs node kill), randomised chaos ordering, clock skew, slow/asymmetric network, slow disk — each effort-estimated in [ADR-0005](decisions/0005-raft-fault-injection-scope.md) |
| 5 | **Security** | ✅ partial — wire-protocol fuzz harness in [`fuzz/`](../fuzz/) (4 mutation + 3 structure-aware roundtrip + 2 cross-impl differential) + [OSS-Fuzz integration infra](../infra/oss-fuzz/) (upstream PR pending) + authn/authz bypass corpus ([JWT/RBAC](../oxidb-server/tests/security_authn_authz.rs) + [SCRAM stateful](../oxidb-server/tests/security_scram_stateful.rs) + [handler canonicalisation](../oxidb-server/tests/security_handler_canonicalisation.rs), 32 attack patterns total, all rejected). 7 DoS + 1 correctness bug found+fixed via the harness | OSS-Fuzz upstream submission to `google/oss-fuzz`; external pentest (Cure53 / Trail of Bits) — see [ADR-0006 §5](decisions/0006-cern-testing-gap-estimates.md#category-5--security) for effort estimates |
| 6 | **Upgrade / migration** | ✅ partial — per-version fixture corpus in [`tests/fixtures/upgrade/`](../tests/fixtures/upgrade/), reader in [`tests/cern_upgrade_chain.rs`](../tests/cern_upgrade_chain.rs). First fixture: `v0.28.4.tar.gz` (866 bytes) covering Storage, WAL, index, tx-commit-log, blob. | N → N+1 → N+2 chain with multiple committed fixtures; documented migration steps per release; downgrade-where-allowed test pattern — see [ADR-0006 §6](decisions/0006-cern-testing-gap-estimates.md#category-6--upgrade--migration) for effort estimates |
| 7 | **Scale** | ✅ partial — HEP-shaped workload at 100K docs in [`tests/cern_scale.rs`](../tests/cern_scale.rs) (insert throughput floor, index sub-linearity, aggregation correctness, delete-by-predicate). `OXIDB_SCALE_DOCS` env var to crank up | 10⁹+ doc dataset, 24-hour sustained insert + scan, multi-TB on-disk, dedicated CI runner — see [ADR-0006 §7](decisions/0006-cern-testing-gap-estimates.md#category-7--scale) for effort estimates |
| 8 | **Disaster recovery / drills** | ✅ partial — backup→wipe→restore drill in [`tests/cern_dr_drill.rs`](../tests/cern_dr_drill.rs) (1500 docs + indexes + blobs, RTO ~30ms on dev hardware) | Power-loss VM drill (kvm + qemu), primary-site-down 24h, RTO SLA over realistic dataset sizes, restore-from-encrypted-backup — see [ADR-0006 §8](decisions/0006-cern-testing-gap-estimates.md#category-8--disaster-recovery) for effort estimates |

**Aggregate budget** to close every gap above (per [ADR-0006](decisions/0006-cern-testing-gap-estimates.md#aggregate-budget)):
~6–9 months of one engineer's focused time, broken down by tag: 11×xs +
9×s + 11×m + 3×l + 1×xl + 3×xxl (where xxl items are usually
blocked on external dependencies).

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

Does NOT yet cover: A5A (read skew, monotonic-reads), explicit
snapshot vs serializable distinctions. Those land in follow-up
tests as the isolation contract gets explicit names.

### `cern_acid_isolation.rs`

Three isolation-anomaly tests that empirically **pin** OxiDB's
observed behaviour:

| Anomaly | Test | OxiDB result |
|---|---|---|
| Dirty read | `dirty_read_does_not_occur` | ❌ NEVER occurs |
| Phantom read | `phantom_read_pinned_behaviour` | ✅ Occurs (read-committed) |
| Write skew (A5B) | `write_skew_pinned_behaviour` | ✅ Occurs (OCC ≠ SSI) |

The phantom and write-skew tests **assert that the anomaly DOES
happen** — so any future isolation upgrade (SSI / serializable
snapshot) flips them, and that flip is the deliberate documentation
that the engine got stronger. Pinning observed behaviour in tests
is the only way to make isolation-level changes visible at PR-
review time.

ANSI SQL classification: OxiDB sits at **read committed + OCC
lost-update protection** — equivalent to PostgreSQL's `READ
COMMITTED` with serializable-update guards. See
[`docs/format/tx-commit-log.md`](format/tx-commit-log.md#isolation-level-observed)
for the per-anomaly classification table.

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

### `cern_sigkill_drill.rs`

The real thing: a victim subprocess (self-spawned via
`env::current_exe()` + a role env var, no extra `[[bin]]` target)
opens OxiDb and inserts forever, ack'ing each `insert()` over its
stdout pipe. The parent reads N acks, then sends **SIGKILL** via
`Child::kill()`, then reopens the same data dir and asserts:

- **Every ACKed record is recovered.** The victim only writes the
  ack AFTER `insert()` returns, by which time `insert`'s WAL fsync
  has completed. A missing record here would be a "lost
  acknowledged write" — the textbook database durability bug.
- **No phantom data.** Recovered ids form a contiguous prefix of
  the insert sequence — replay never fabricates documents.

Unlike `mem::forget`, this exercises the OS-process boundary:
SIGKILL is uncatchable, lands anywhere in the victim's execution
including mid-fsync, mid-syscall. That's the real crash shape.

Empirically the parent observes 0–1 "extra durable" records beyond
the last ack on each run — this is correct (the child may have
completed the next insert's fsync between the ack write and the
SIGKILL landing) and demonstrates the test is genuinely racing the
kernel.

Extends to byte-offset matrix via `cern_sigkill_byte_offset.rs`
(below). Does NOT yet cover: fsync-returns-EIO mid-batch,
power-loss simulation, ENOSPC injection. Those need a syscall
interposer or LD_PRELOAD harness — a follow-up.

### `fuzz/` — wire-protocol fuzz harness

`cargo-fuzz` + `libfuzzer-sys` targets in `fuzz/fuzz_targets/`,
excluded from the main workspace (the harness needs sanitizer-
friendly compile flags the normal `cargo build` shouldn't pay
for). Four targets:

- `wire_deserialize` — the top-level message dispatcher
  (`{`/`[` → JSON, `0xDB` → OxiWire, else → MsgPack)
- `wire_oxiwire` — hand-rolled OxiWire binary decoder
- `wire_resp` — RESP (Redis-compatible) parser used by OxiMem
- `wire_pg` — PostgreSQL frontend-message decoder

Each target is a thin wrapper: take `&[u8]`, call the parser, drop
the `Result`. libfuzzer counts any panic / abort / sanitizer
finding as a crash. Run with `cargo +nightly fuzz run <target>` —
see [`fuzz/README.md`](../fuzz/README.md) for the playbook.

The harness is the deliverable; what it finds is follow-up. The
first smoke run found multiple crashers across all four targets —
each gets its own fix PR with a pinned regression test.

**Update:** structure-aware fuzzing landed for OxiWire — see
`oxiwire_roundtrip` target. Generates an `Arbitrary` value tree,
encodes through `oxiwire::encode_value`, decodes via
`oxiwire::decode_request`, asserts JSON-canonical equality. Runs
~6× faster than bit-flipping (~18k iter/s vs ~3k iter/s in 30s
smoke runs) because every input is valid-by-construction and
libfuzzer mutates *meaningfully* rather than throwing megabytes of
garbage that bounce off the decoder's first byte check.

Does NOT yet cover: structure-aware roundtrip for the remaining 3
wire formats (RESP, MsgPack, pg_wire); differential fuzz against a
reference impl (RESP vs real Redis, pg_wire vs real Postgres);
OSS-Fuzz continuous-integration; coverage reporting.

### `cern_sigkill_byte_offset.rs`

Extends the SIGKILL drill to a **matrix of kill delays** (100µs,
500µs, 1ms, 5ms, 10ms, 50ms, 200ms). For each delay: spawn victim
→ sleep → SIGKILL → drain pipe → reopen → check invariants. Same
durability + no-phantom invariants as the basic drill, asserted
at every delay.

Across the grid, the kill lands at a different point in the
engine's write trajectory:
- **100µs–1ms** → engine still inside `DB::open` initialisation
- **5ms** → first insert mid-fsync (kernel completes the syscall
  before death; `+1 extra durable` shows up here)
- **10ms+** → first ACKs reach parent, then steady-state kills

Also asserts **reopen succeeds at every delay** — a kill mid-init
that leaves the data dir unrecoverable would be a real bug.

Across 5 consecutive runs the matrix is highly reproducible:
the `+1 extra-durable` count is stable at every non-zero delay,
indicating the WAL-fsync-to-ack race is well-bounded.

Does NOT yet cover: SIGKILL at offset 1, 2, ..., K bytes *within*
a single fsync syscall (needs syscall interposition — fsync is
uninterruptible from userspace, so we can't pause it mid-byte).

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
cargo test --test cern_sigkill_drill  -- --ignored
cargo test --test cern_sigkill_byte_offset -- --ignored --nocapture
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
