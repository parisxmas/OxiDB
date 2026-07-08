# OxiDB Transaction Isolation — the exact guarantee

Empirically characterized and pinned by
`tests/isolation_characterization.rs` (plus the older
`tests/cern_acid_isolation.rs`). If engine behavior changes, those tests
fail loudly and this document must be updated in the same commit.

## The model

OxiDB transactions are **backward-validating OCC over item read-sets**:

1. **Reads inside a transaction** (`tx_find`, and the match phase of
   `tx_update`/`tx_delete`) return the latest **committed** state.
   There is no snapshot, and a transaction does **not** see its own
   buffered writes until commit.
2. Every document a transaction reads is recorded in its read-set with
   the version it had at read time.
3. **At commit**, the entire read-set is validated: if any document the
   transaction read has changed since, the commit aborts with
   `TransactionConflict`. Writes are then applied atomically under the
   commit lock and become durable (WAL + commit-log fsync, group
   committed) before the client is acknowledged.
4. `find_for_update` / `tx_find_for_update` additionally takes
   pessimistic per-document locks (released at commit/rollback), so hot
   documents queue instead of burning conflict retries. Locks only
   exclude other `for_update` callers; OCC validation remains the
   correctness backstop for every path.

## Anomaly scorecard

| Anomaly (Adya/Berenson) | Status | Pinned by |
|---|---|---|
| G1a dirty read (uncommitted writes visible) | **Prevented** | `dirty_read_does_not_occur` |
| G0 dirty write (concurrent blind writes interleave) | **Prevented** | `g0_dirty_write_prevented` |
| P4 lost update (read-modify-write race) | **Prevented** | `p4_lost_update_prevented` |
| A5B write skew over **item reads** (read both docs, write different ones) | **Prevented** — read-set validation | `a5b_write_skew_item_reads_prevented`, `write_skew_pinned_behaviour` |
| A5A read skew — *observation* mid-tx | **Admitted** (reads are read-committed, no snapshot) | `a5a_read_skew_observable_but_not_actionable` |
| A5A read skew — *acting on it* | **Prevented** — a writer that observed stale data fails validation | same test |
| P3 **phantom** (predicate re-read sees new rows) | **Admitted** | `phantom_read_pinned_behaviour` |
| Phantom **write skew** (predicate-based constraint violated by concurrent inserts) | **Admitted** — read-sets hold returned docs only, not predicates | `phantom_write_skew_admitted` |
| G1b intermediate read, **read-only observers** (torn multi-doc visibility during a commit's apply window) | **Admitted** (≈ MongoDB "local" read concern) | `g1b_intermediate_reads_admitted_for_observers` |
| G1b for **transactions** (acting on a torn view) | **Prevented** — stale read-set aborts at validation | `a5a_…` |
| Read-your-own-writes inside a tx | **Not provided** mid-tx (writes compose at commit — see 838a6730) | `read_your_own_writes_pinned_not_visible_mid_tx` |

**Summary: committed transactions are serializable with respect to the
items they read and wrote.** The two deliberate gaps are (a) predicate
reads (phantoms) and (b) what read-only, non-transactional observers can
see mid-commit.

## Rules for application developers (exchange-grade usage)

1. **Read what you depend on, inside the transaction.** The guarantee
   covers your read-set. A balance check done outside the transaction
   (or not at all) is not protected.
2. **Materialize predicate constraints.** "At most N open orders",
   position limits, risk caps — anything checked by a *query* can be
   violated by phantoms. Keep the constraint in a document every writer
   read-modify-writes (a counter/limit doc); OCC then serializes on it.
   Pinned working pattern: `phantom_write_skew_mitigated_by_counter_doc`.
3. **Use `find_for_update` on hot documents** (fee account, shared
   position) to replace conflict-retry storms with orderly queueing.
4. **Don't read back your own uncommitted writes** — compute the value
   you wrote instead of re-querying it mid-transaction.
5. **Reports/monitors that need cross-document consistency** should
   read inside a transaction and re-read (validation via a no-op write
   commit), or tolerate torn views — plain reads can observe a commit's
   partial apply window.

## Exactly-once / idempotency (retries, timeouts, crashes)

The client-visible failure mode isolation can't solve alone: a request

- **Snapshot reads / MVCC** would close A5A-observation, P3-for-readers
  and G1b-for-observers in one move.
- **Predicate validation or SSI** would close phantom write skew
  without the counter-doc pattern.
- Both are performance/complexity trade-offs; today's model is honest,
  fast, and sufficient when the rules above are followed.
