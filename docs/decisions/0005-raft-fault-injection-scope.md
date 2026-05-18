# ADR-0005: Raft / HA fault-injection — what exists, what's missing

**Status:** Accepted
**Date:** 2026-05-18
**Supersedes:** —
**Related:** [`docs/testing-roadmap.md`](../testing-roadmap.md) (category 4),
[ADR-0003](0003-1.0-stability-scope.md) (1.0 surface), 
[ADR-0004](0004-phase-0-answers.md) (release policy)

## Context

The CERN-grade testing roadmap originally listed category 4 (HA / Raft
fault injection) as `❌ not started`, with the placeholder "**Jepsen-
style suite** (split-brain, partition, clock skew, slow disk) — biggest
single gap". That was inaccurate. A look at `oxidb-server/tests/raft_test.rs`
(behind the `cluster` feature) shows **6 substantial Raft fault-injection
tests** already in tree. This ADR pins what's there and scopes what
remains for a full Jepsen-style program.

**Errata (2026-05-18):** the original version of this ADR (PR #55)
miscounted as 7 by including a `test_split_brain_prevention` entry
that doesn't exist in `raft_test.rs` — that was a hallucination from
a careless `grep -c` that picked up the `test_openraft_config` helper
fn. The actual list is the 6 below. Split-brain prevention is a
property the existing tests collectively assert (via leader-kill
failover + minority-can't-elect), not a standalone test fn.

## What already exists (`oxidb-server/tests/raft_test.rs`, `cluster` feature)

| Test | Scenario | Property checked |
|---|---|---|
| `test_cluster_formation_and_leader_election` | 3-node cluster startup | A leader is elected within a bounded time |
| `test_write_replication` | Single-leader write | Followers apply the write |
| `test_leader_kill_and_failover` | `leader.kill()` | A new leader is elected; cluster keeps making progress |
| `test_data_consistency_after_failover` | Kill leader after writes | All survivors agree on the same final state |
| `test_kill_two_nodes` | 5-node cluster, kill 2 followers | Majority (3 of 5) still makes progress |
| `test_minority_cannot_elect_leader` | 4-node cluster, kill 3 | Survivor cannot elect itself leader (no quorum) |

These are **not** "compiles and runs once" smoke tests — they exercise the
real openraft state machine across multiple processes' worth of
`OxiDbStore`, `OxiDbNetwork`, and `OxiRaft` instances driven by tokio
multi-threaded runtimes. They catch the canonical Raft bugs: lost-update
on failover, divergence after partition, premature leader claim on
quorum loss.

## What's missing for full Jepsen-grade

Jepsen-grade testing is meaningfully more than "good fault-injection
test suite". The remaining gap, in priority order:

### 1. Linearizability checker (e.g. Knossos / Elle)

The existing tests assert "final state matches expected" after a
specific scenario. Jepsen records the **full history of client
observations** during a chaotic run, then **checks** that the history
is linearizable / serializable / sequential against an abstract model.
That catches anomalies the "settled state matches" check can miss —
e.g., a brief stale read during failover that goes uncorroborated by
later state.

**Effort:** medium. Either port `Knossos` (Clojure) to Rust or
integrate with the Knossos JAR via subprocess. ~2 weeks of dedicated
work.

### 2. Network partition nemesis (vs node kill)

Today's tests partition by killing nodes. A real partition leaves both
sides ALIVE but isolated; the minority side keeps trying to elect a
leader; the majority side continues; on heal, the two sides reconcile.
That's a different bug surface than "node is gone."

**Effort:** medium. Needs either iptables-style filtering at the OS
level (root, platform-specific) or a TCP-proxy interposer in the
network layer (Rust-only, portable). Estimated ~1 week.

### 3. Randomized chaos / nemesis ordering

Existing tests are specific scenarios. Jepsen runs randomised
nemeses for hours, picking random failure shapes from a catalogue
(`kill-random-node`, `partition-halves`, `slow-network-50ms`, `slow-
disk`, `clock-skew-1s`, `heal-everything`). The 8th scenario is the
one that breaks because nobody wrote a specific test for it.

**Effort:** medium-large. The nemesis driver itself is a few hundred
lines; the time investment is *running* it (chaos finds bugs slowly,
needs hours per generation).

### 4. Clock skew

Raft elections depend on timers. Skewing one node's clock by ±1s
relative to others can either elect a leader too aggressively or
miss heartbeats. Rare in well-managed datacentres but observed in
practice (VM live-migration, NTP failure).

**Effort:** small (~3 days). Needs a clock-mock layer in `OxiRaft`
or willingness to use `libc::settimeofday`-via-`unshare(CLONE_NEWTIME)`
on Linux only.

### 5. Slow / asymmetric network

Today's network failure is binary (works / doesn't). Real production
sees 200ms latency spikes, 5% packet loss, asymmetric loss (A→B works,
B→A drops). Each shape produces different Raft behaviour.

**Effort:** small (~3 days). TC `netem` on Linux, or in-process
proxy with configurable latency / loss.

### 6. Slow disk

Raft commits depend on log-store sync latency. Slow-disk nemesis
(simulate fsync taking 100ms) stresses commit ordering and timeout
tuning. fsync-EIO is the harder variant.

**Effort:** small-medium (~1 week). LD_PRELOAD shim or syscall
interposer.

## Decision

**Promote category 4 to ✅ partial.** The roadmap row gets updated to
reflect the 7 existing tests, with the 6-item gap list above as the
explicit remaining work.

**Don't bulk-land "more chaos tests" right now.** The existing tests
are tight, specific, and pass — adding more of the same shape buys
marginal coverage. The next meaningful unit of work is item #1
(linearizability checker), which is a separate multi-week PR and
should be scoped in its own ADR when started.

**Don't promise Jepsen integration as a 1.0 blocker.** Per ADR-0003
the 1.0 stable surface does NOT include the Raft / cluster mode
(it's marked experimental). Jepsen-grade testing graduates with the
cluster mode itself.

## Consequences

- `docs/testing-roadmap.md` row 4 flips ❌ → ✅ partial, with link to
  the existing tests + this ADR for the gap list.
- Any future "Raft is harder than X" claim or comparison needs to
  point at this ADR so we're honest about where we stand.
- Item #1 (linearizability checker) is the natural next ADR (ADR-0006
  presumably) when someone budgets the work.

## Alternatives considered

- **Add yet another chaos test now.** Rejected — duplicates existing
  coverage without addressing the real gap (linearizability check).
- **Wait until Jepsen integration to flip the row.** Rejected — sets
  a false zero baseline for the 6 tests that already exist and do
  real work.
- **Block 1.0 on Jepsen.** Rejected per ADR-0003 — cluster mode is
  not in the 1.0 stable surface, so its testing program doesn't
  have to graduate with the 1.0 cut.
