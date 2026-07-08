# OxiDB as an Exchange Data Layer — readiness & evidence

What a real crypto/securities exchange needs from its database, and how
each guarantee is verified in this repository. Scope note up front: an
exchange's **matching engine** is deliberately out of scope — real
exchanges run matching in a single-threaded in-memory event loop (the
LMAX model), not in a database; no SQL/NoSQL engine does or should do
order matching. OxiDB is the durable, consistent data layer *behind* the
matching engine: order/trade/journal persistence, the balance ledger,
market data, and idempotent request handling.

Every row below maps a guarantee to an executable test. Fault-injection
tests use `--ignored` (run explicitly); correctness unit tests run in the
normal suite.

## ACID & isolation

| Guarantee | Evidence | Result |
|---|---|---|
| Isolation level is precisely characterized | `tests/isolation_characterization.rs`, `tests/cern_acid_isolation.rs`, `docs/isolation.md` | Backward-validating OCC over item read-sets: **serializable w.r.t. items read/wrote** |
| No dirty write / lost update / item-read write skew | `isolation_characterization.rs` (G0, P4, A5B) | Prevented |
| A writer acting on read-skew data cannot commit | `isolation_characterization.rs` (A5A) | Prevented |
| History is serializable (not just "money conserved") | `tests/linearizability.rs` — Elle/Adya ww/wr/rw cycle detection | Acyclic over 2000 txns / ~5.4k edges (no G0/G1c/G2) |
| Exactly-once under retry (no double-spend) | `tests/exactly_once.rs` | Applied exactly once under sequential retry, 8-way concurrent storm, and SIGKILL+retry (50 rounds) |
| Multi-collection order fill is all-or-nothing | `tests/multi_collection_crash.rs` | 5 collections/order atomic across SIGKILL; journal balances, balances reconcile |

Admitted (documented, not bugs): predicate **phantoms** and torn
multi-doc reads for **non-transactional observers** — mitigations and the
full scorecard are in `docs/isolation.md`.

## Durability & crash recovery

| Guarantee | Evidence | Result |
|---|---|---|
| Committed writes survive process crash (SIGKILL anywhere) | `tests/jepsen_bank_crash.rs`, `tests/cern_sigkill_drill.rs` | Every acked write recovered; found & fixed a commit-log torn-write bug (atomic replace) |
| A failed fsync is never reported as success (fsyncgate) | `tests/fsync_fault.rs` | Commit returns Err; found & fixed a group-commit leak (fsync failure now poisons in-memory state) |
| Recovery-of-recovery (repeated crashes) | jepsen / multi-collection (rounds accumulate on one dir) | Holds across 15–50 rounds |
| Full strict ACID-D durability by default | every commit fsyncs WAL + commit-log before ack | `OXIDB_SQL_SYNC` etc. tune, default = strict |

## Performance under contention (not matching, ledger writes)

| Concern | Evidence | Result |
|---|---|---|
| Hot-account contention (fee account, shared position) | `tests/hot_account_bench.rs` | Group commit + `find_for_update`: ~2.3× tx/s, ~10× lower p99; Linux VPS 1.5–2.2k tx/s, p99 ≤ 8 ms at full durability |
| p50/p99/max latency reported, not just throughput | same bench | Measured per run |

## Cluster (Raft replication)

| Guarantee | Evidence | Result |
|---|---|---|
| No split-brain / no lost committed writes under network partition | `oxidb-server/tests/raft_partition_test.rs` | Safety holds; minority cannot commit; majority quorum converges |
| Sequence/trade-ID monotonicity across leader failover | `oxidb-server/tests/raft_sequence_monotonic.rs` | Gapless, unique, monotonic; every acked seq survives 2 failovers |

## Known gaps (honest disclosure)

1. **Matching engine** — out of scope by design (belongs in-memory, not
   in the DB).
2. **Cross-engine 2PC (document + SQL in one transaction)** — ADR-0011
   is *Proposed*. Within a single engine, multi-collection transactions
   are atomic (proven above); spanning both engines atomically is not
   yet implemented.
3. **Raft liveness on ex-leader rejoin** — openraft 0.9 has no PreVote,
   so a leader isolated by a partition inflates its term and, on heal,
   rejoins as a disruptive stale-log candidate the quorum ignores; it
   does not catch up without a restart. **Safety is unaffected**
   (documented in the partition test). Fix path: PreVote-capable
   openraft, or force an isolated leader to step down.
4. **Observer isolation** — non-transactional reads can see a commit's
   partial apply window (≈ MongoDB "local" read concern). Transactions
   acting on such a view abort; snapshot reads would close this.

## How to run the evidence

```
# fast correctness (normal suite)
cargo test --release
cargo test -p oxidb-server --features cluster

# fault-injection / ACID drills (explicit)
cargo test --release --test isolation_characterization -- --ignored
cargo test --release --test linearizability            -- --ignored
cargo test --release --test exactly_once               -- --ignored
cargo test --release --test multi_collection_crash     -- --ignored
cargo test --release --test jepsen_bank_crash          -- --ignored
cargo test --release --test fsync_fault                -- --ignored
cargo test --release --test hot_account_bench          -- --ignored
cargo test -p oxidb-server --features cluster --test raft_partition_test
cargo test -p oxidb-server --features cluster --test raft_sequence_monotonic
```
