# ADR-0006: Effort estimates for the remaining CERN-grade testing gaps

**Status:** Accepted
**Date:** 2026-05-18
**Supersedes:** —
**Related:** [`docs/testing-roadmap.md`](../testing-roadmap.md),
[ADR-0005](0005-raft-fault-injection-scope.md) (Raft category, done
separately)

## Context

After PRs #42 / #46-49 / #50-55 every row in `docs/testing-roadmap.md`
sits at ✅ partial with an explicit gap list. ADR-0005 effort-
estimated the gap for category 4 (Raft / HA) at item-level. This
ADR does the same exercise for the other 7 categories so the
roadmap claim "every gap is explicit *and* effort-estimated" is
literally true and someone planning capacity has concrete numbers
to budget against.

## Effort vocabulary

| Tag | Wall-clock budget (single experienced engineer) |
|---|---|
| **xs** | < 1 day |
| **s**  | 1–3 days |
| **m**  | 1–2 weeks |
| **l**  | 2–4 weeks |
| **xl** | 1–3 months |
| **xxl**| ≥ 3 months (or unbounded — needs external dependency) |

These are honest "person actually does the work" numbers, not
hopium. They include design, code, tests, docs, and review.

## Category 1 — ACID & isolation

Existing: 4 tests (transfers + dirty + phantom + write-skew). See
[`tests/cern_acid_isolation.rs`](../../tests/cern_acid_isolation.rs).

| Gap | Effort | Notes / unblockers |
|---|---|---|
| A5A (read skew / monotonic reads) test | ✅ landed — [`tests/cern_acid_isolation.rs::read_skew_pinned_behaviour`](../../tests/cern_acid_isolation.rs). Pinned dual finding: read-skew IS visible during the tx (X stale, Y fresh), AND OCC catches it at commit with `TransactionConflict`. Stronger than pure read-committed. |
| Explicit snapshot-isolation API (`OXIDB_SNAPSHOT_ISO` mode) | **m** | Engine change, not test change. Pre-req for the "is this *actually* snapshot now?" test. Needs its own ADR. |
| Multi-version concurrency control (MVCC) | **xl** | Major engine work, not a test. Replaces or augments OCC. ADR-worthy. |
| Reference anomaly suite (Adya / Berenson exhaustive) | **m** | A5A/B done; remaining are P0, P1, P2, P3, P4 — each ~50 lines. |

**Suggested order:** A5A first (xs), reference suite next (m). Don't
build SI / MVCC tests until the engine actually grows the surface.

## Category 2 — Crash recovery

Existing: 3 tests (soft `mem::forget`, hard SIGKILL after acks,
byte-offset SIGKILL matrix). See [`tests/cern_sigkill_byte_offset.rs`](../../tests/cern_sigkill_byte_offset.rs).

| Gap | Effort | Notes / unblockers |
|---|---|---|
| ENOSPC injection mid-write | **s** | LD_PRELOAD shim or syscall interposer; test harness shape exists. |
| fsync returns EIO mid-batch | **s** | Same shim; different errno. |
| Cosmic-bit-flip simulation (page-cache memory poisoning) | **l** | Either kernel module or a controlled VM; the test itself is small but the infra to run it is heavy. |
| SIGKILL at offset 1, 2, ..., K bytes WITHIN a single fsync | **m** | fsync is uninterruptible from userspace → needs syscall interposition (LD_PRELOAD that returns early after N bytes) or kernel-level fault injection. |
| Deeper init-time kill cases | **xs** | Already partially covered by `cern_sigkill_byte_offset.rs` at 100µs-1ms delays. Adding more samples is trivial. |

**Suggested order:** ENOSPC + EIO first (both s, same shim), then
deeper init-time (xs), then the in-fsync byte-offset matrix (m).
Cosmic-ray simulation is over-engineering until something motivates it.

## Category 3 — Performance & long-tail

Existing: `cern_soak.rs` (bounded soak, default 30s, configurable
via env var).

| Gap | Effort | Notes / unblockers |
|---|---|---|
| RSS leak detection | **s** | macOS: `mach_task_basic_info`; Linux: `/proc/self/status`. Conditional compilation. |
| fd leak detection | **s** | Same per-platform syscalls. |
| WAL-size unbounded growth check | ✅ landed — `cern_soak.rs` now asserts `<dir>/soak.wal` stays ≤ 64 MiB after the steady-state insert/update/delete loop. Empirical: 1.73 MiB after 30s / 21,950 mixed ops on dev hw. Catches "rotation disabled" regressions. |
| Multi-day soak | **m** ops, **xxl** wall-clock | Test code is one env-var tweak; the *running* is a 30-day commitment of a CI agent. |
| HEP-shaped workload (bursty + scans + high-fanout reads) | **s** | Mostly composition of patterns already in `cern_scale.rs`. |

**Suggested order:** WAL-size check first (xs), RSS + fd leak (s),
HEP-shape composition (s), multi-day soak last (needs dedicated
runner allocation).

## Category 5 — Security

Existing: 4 mutation fuzz targets + 1 structure-aware (OxiWire
roundtrip). See [`fuzz/`](../../fuzz/).

| Gap | Effort | Notes / unblockers |
|---|---|---|
| Structure-aware roundtrip for RESP | ✅ landed — [`fuzz/fuzz_targets/resp_roundtrip.rs`](../../fuzz/fuzz_targets/resp_roundtrip.rs) (558k iter/30s clean) |
| Structure-aware roundtrip for MsgPack | ✅ landed — [`fuzz/fuzz_targets/msgpack_roundtrip.rs`](../../fuzz/fuzz_targets/msgpack_roundtrip.rs) (570k iter/30s clean, cross-impl diff vs `rmp_serde`) |
| Differential fuzz vs real Redis | ✅ landed (first slice) — [`fuzz/fuzz_targets/resp_diff_redis.rs`](../../fuzz/fuzz_targets/resp_diff_redis.rs) uses `redis::parse_redis_value` (canonical redis-rs parser) as the reference rather than a subprocess. Scoped to `:` / `$` framing where RESP2 spec is unambiguous; found 1 OOM + 1 correctness bug on first run (both fixed). Subprocess-against-real-redis-server is a follow-up. |
| Differential fuzz vs real Postgres | ✅ landed (first slice) — [`fuzz/fuzz_targets/pg_diff_pgwire.rs`](../../fuzz/fuzz_targets/pg_diff_pgwire.rs) uses the `pgwire` crate as a separately-authored reference Postgres-wire decoder (rather than a `libpq` subprocess — same trade-off rationale as `resp_diff_redis`). Scoped to `Q P D H C X` tagged messages; found 2 real OxiDB panic bugs on first runs (signed-i16 overflow, empty-body decode), both fixed. |
| OSS-Fuzz continuous integration | ⏳ **infra landed** ([`infra/oss-fuzz/`](../../infra/oss-fuzz/)) — upstream PR pending. The Dockerfile / build.sh / project.yaml are committed; submitting to `google/oss-fuzz` is a manual step documented in that directory's README. |
| External pentest (Cure53 / Trail of Bits) | **xxl** wall-clock, **0** internal eng | Contracted engagement. Cost + scheduling, not engineering effort. |
| Authn/authz bypass test corpus | ✅ landed — JWT + RBAC ([`security_authn_authz.rs`](../../oxidb-server/tests/security_authn_authz.rs), 12 patterns) + SCRAM stateful ([`security_scram_stateful.rs`](../../oxidb-server/tests/security_scram_stateful.rs), 11 patterns) + handler-level canonicalisation ([`security_handler_canonicalisation.rs`](../../oxidb-server/tests/security_handler_canonicalisation.rs), 9 patterns) + per-vector audit-log evidence shape ([`security_audit_evidence.rs`](../../oxidb-server/tests/security_audit_evidence.rs), 8 patterns) + audit-log size-based rotation ([`security_audit_rotation.rs`](../../oxidb-server/tests/security_audit_rotation.rs), 5 patterns) + audit-log **time-based** rotation ([`security_audit_time_rotation.rs`](../../oxidb-server/tests/security_audit_time_rotation.rs), 8 patterns covering age trigger, age-clock reset, independent size+age triggers, backwards-compat shims, concurrent-safety under age load). 53 total tests, all currently pass. Rotation primitive now exposes `RotationPolicy { max_bytes, max_age }` covering both triggers independently. |
| Coverage reporting (`cargo +nightly fuzz coverage`) | ✅ landed — [`fuzz/coverage.sh`](../../fuzz/coverage.sh) wraps `cargo fuzz coverage` + `llvm-cov report`, filters to files the target actually touched, supports HTML output. CI step still pending (no nightly fuzz CI yet). |

**Suggested order:** RESP + MsgPack roundtrip (xs each, same day),
coverage reporting (xs), OSS-Fuzz (m, highest leverage), then
diff-fuzz against Redis. External pentest is its own initiative.

## Category 6 — Upgrade / migration

Existing: per-version fixture corpus pattern + first fixture
(`v0.28.4`). See [`tests/cern_upgrade_chain.rs`](../../tests/cern_upgrade_chain.rs).

| Gap | Effort | Notes / unblockers |
|---|---|---|
| N → N+1 → N+2 chain (multiple committed fixtures) | **xs per fixture, accumulates** | The pattern is in place; needs N+1 fixture committed when v0.29.0 ships, etc. Recurring overhead per release. |
| Documented migration steps per release | **xs per release, accumulates** | CHANGELOG entry under "Format changes"; no migration code yet because nothing's broken backwards. |
| Downgrade-where-allowed test pattern | **s** | The reverse direction: new fixture must open with prior engine if `format_version` is compatible. Needs at least one prior binary to test against. |
| Forward-compat tripwire coverage | **xs** | Already proven for blob `.meta`; same shape for `.btree` (OXBT), `.wal` (OXWA), `_tx_commit_log` (OXTX) when those grow new versions. |

**Suggested order:** Tripwire coverage for the other 3 format kinds
when we bump them (xs each). N+1 fixture lands with v0.29.0 by
convention. Downgrade is best deferred to a real downgrade need.

## Category 7 — Scale

Existing: `cern_scale.rs` (HEP-shaped at 100K docs, all invariants
hold).

| Gap | Effort | Notes / unblockers |
|---|---|---|
| 10⁹+ doc dataset run | **xs** code, **m** wall-clock per run, **l** to procure runner | The harness already takes `OXIDB_SCALE_DOCS`. Needs CI node with TB disk + hours of budget. |
| 24-hour sustained insert + scan | **xs** code, **xl** wall-clock | One env var. The cost is the running. |
| Multi-TB on-disk | **xs** code, **l** infra | Same; needs the storage. |
| Dedicated nightly CI runner | **m** | Capacity planning + cost approval, not engineering. |

**Suggested order:** Procure the runner first; everything else is
already implemented behind env-var knobs.

## Category 8 — Disaster recovery

Existing: `cern_dr_drill.rs` (backup → wipe → restore, RTO ~30ms on
1500 docs).

| Gap | Effort | Notes / unblockers |
|---|---|---|
| Power-loss VM drill (kvm + qemu) | **m** | Suspend the engine VM mid-fsync, snapshot, restore, verify. Real cost is the VM tooling setup. |
| Primary-site-down 24h drill | **s** code, **xl** wall-clock | A live cluster running 24h on snapshot+restore cycle. Coordination, not engineering. |
| RTO SLA over realistic dataset sizes | **xs** | Scale up `cern_dr_drill.rs` via env var, time each phase. |
| Restore-from-encrypted-backup | ✅ landed — `cern_dr_drill.rs::encrypted_backup_wipe_restore_round_trip` covers all three sub-cases: correct key (50 docs recovered), wrong key (0 docs, no plaintext leak), no key (0 docs, no plaintext leak). Engine's AEAD authentication tag catches wrong keys at decryption time. |

**Suggested order:** RTO scale-up (xs) and encrypted variant (s)
first. Power-loss VM is a separate harness; 24h drill is operational.

## Aggregate budget

| Tag | Items |
|---|---|
| **xs** (< 1 day each) | 11 |
| **s**  (1–3 days each) | 9 |
| **m**  (1–2 weeks each) | 11 |
| **l**  (2–4 weeks each) | 3 |
| **xl** (1–3 months each) | 1 |
| **xxl** (≥ 3 months, often blocked on external) | 3 |

Rough total internal engineering: **~6–9 months of one engineer's
focused time** to close every gap above to "done." That is *not*
the budget to ship 1.0 — per ADR-0003 the 1.0 stable surface is a
specific subset; this ADR is the full-coverage maximum.

## Decision

Adopt the effort tags above. Update `docs/testing-roadmap.md` so
each row's gap column links to this ADR's relevant section and is
not just a list of words.

When a gap-item is started, open a follow-up ADR if it's >= m and
the design has non-obvious choices; otherwise the PR description
suffices.

## Consequences

- The "every gap is explicit AND effort-estimated" claim in the
  testing-roadmap is now literally true (was only true for category 4
  before this PR).
- Capacity planning has numbers, not adjectives.
- Future PRs that close a gap-item should update this ADR's row to
  ✅ when they merge.

## Alternatives considered

- **One ADR per category.** Rejected — six more ADR files for one
  shared analysis adds friction without clarity.
- **Inline estimates in `docs/testing-roadmap.md` cells.** Rejected
  — the roadmap rows would balloon to multi-line markdown cells
  that don't render well; the ADR is the right home for prose.
- **Don't estimate, just enumerate.** Rejected — estimates without
  numbers are wishes. The numbers are mediocre rather than precise
  on purpose: known-mediocre is honest, missing-entirely is not.
