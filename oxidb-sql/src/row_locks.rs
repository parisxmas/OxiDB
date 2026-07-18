//! Pessimistic row locks for `SELECT ... FOR UPDATE` and writer-writer
//! exclusion — the SQL engine's counterpart of the document engine's
//! `doc_locks` (same Condvar lock-table pattern, same timeout discipline).
//!
//! Ownership is a plain `u64`: an interactive transaction's lock owner id, or
//! an ephemeral per-statement id for autocommit DML. A lock is held from
//! acquisition until `release_all` — commit/rollback for a transaction,
//! statement end for autocommit. Waiting happens on a Condvar with a
//! deadline, and NEVER while the engine's `inner` mutex is held: the holder
//! needs that mutex to commit and release, so waiting under it would
//! deadlock the engine.
//!
//! Deadlocks between two owners (A holds x wants y, B holds y wants x) are
//! not detected — they resolve as a `LockTimeout` on one side, which aborts
//! that statement/transaction. Acquisition is in sorted row-id order per
//! statement to make that case rare.

use std::collections::HashMap;
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::error::{Result, SqlError};

#[derive(Default)]
struct State {
    /// (table, row id) -> owner.
    owners: HashMap<(String, u64), u64>,
    /// owner -> keys it holds (for O(held) release).
    held: HashMap<u64, Vec<(String, u64)>>,
}

#[derive(Default)]
pub(crate) struct RowLocks {
    state: Mutex<State>,
    cv: Condvar,
}

impl RowLocks {
    /// Acquire `row_ids` (pre-sorted by the caller) for `owner`, blocking up
    /// to `timeout` PER CALL for contended rows. Re-entrant: rows already
    /// held by `owner` are free. On timeout, rows already acquired by this
    /// call stay held — the owner's statement fails and its release path
    /// (rollback / statement end) returns them.
    pub(crate) fn lock_many(
        &self,
        table: &str,
        row_ids: &[u64],
        owner: u64,
        timeout: Duration,
    ) -> Result<()> {
        let deadline = Instant::now() + timeout;
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        for &id in row_ids {
            let key = (table.to_string(), id);
            loop {
                match st.owners.get(&key) {
                    Some(&o) if o == owner => break,
                    Some(_) => {
                        let now = Instant::now();
                        if now >= deadline {
                            return Err(SqlError::LockTimeout {
                                table: table.to_string(),
                                row_id: id,
                            });
                        }
                        let (guard, _) = self
                            .cv
                            .wait_timeout(st, deadline - now)
                            .unwrap_or_else(|e| e.into_inner());
                        st = guard;
                    }
                    None => {
                        st.owners.insert(key.clone(), owner);
                        st.held.entry(owner).or_default().push(key);
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    /// Release every lock `owner` holds and wake all waiters. Idempotent.
    pub(crate) fn release_all(&self, owner: u64) {
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(keys) = st.held.remove(&owner) {
            for key in keys {
                st.owners.remove(&key);
            }
            drop(st);
            self.cv.notify_all();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn reentrant_for_the_same_owner_exclusive_across_owners() {
        let l = RowLocks::default();
        l.lock_many("t", &[1, 2], 7, Duration::from_millis(10))
            .unwrap();
        l.lock_many("t", &[2], 7, Duration::from_millis(10))
            .unwrap();
        let err = l
            .lock_many("t", &[2], 8, Duration::from_millis(10))
            .unwrap_err();
        assert!(matches!(err, SqlError::LockTimeout { row_id: 2, .. }));
        l.release_all(7);
        l.lock_many("t", &[2], 8, Duration::from_millis(10))
            .unwrap();
    }

    #[test]
    fn a_waiter_proceeds_the_moment_the_holder_releases() {
        let l = Arc::new(RowLocks::default());
        l.lock_many("t", &[5], 1, Duration::from_millis(10))
            .unwrap();
        let l2 = Arc::clone(&l);
        let waiter = std::thread::spawn(move || {
            let t0 = Instant::now();
            l2.lock_many("t", &[5], 2, Duration::from_secs(5)).unwrap();
            t0.elapsed()
        });
        std::thread::sleep(Duration::from_millis(50));
        l.release_all(1);
        let waited = waiter.join().unwrap();
        assert!(
            waited >= Duration::from_millis(40) && waited < Duration::from_secs(1),
            "the waiter must block until release, then proceed at once ({waited:?})"
        );
    }
}
