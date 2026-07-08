//! Pessimistic per-document write locks — the engine's `SELECT ... FOR
//! UPDATE` primitive (`OxiDb::tx_find_for_update`).
//!
//! OCC alone melts down on hot documents: every transaction that read a
//! hot doc and lost the commit race aborts with `TransactionConflict`
//! and redoes all its work, so at high contention most CPU goes to
//! retries and tail latency explodes. Transactions that instead lock the
//! hot document up front serialize on it — each waiter parks here
//! instead of burning a failed round trip through validate/prepare.
//!
//! Scope: these locks only exclude other `tx_find_for_update` callers,
//! exactly like row locks among `SELECT FOR UPDATE` statements in SQL
//! databases. Plain writers bypass them — OCC version validation at
//! commit remains the correctness backstop for every path.
//!
//! Deadlock policy: none detected; acquisition blocks with a timeout and
//! returns [`Error::LockTimeout`] so a cycle resolves itself instead of
//! hanging. Callers that lock several documents should acquire them in a
//! globally consistent order (`tx_find_for_update` sorts each call's
//! matches by doc id; multi-call lock order is the caller's job).
//! Locks are memory-only and vanish on restart — a crashed holder can't
//! wedge anything.

use std::collections::HashMap;
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::error::{Error, Result};
use crate::tx_log::TransactionId;

#[derive(Default)]
struct State {
    /// (collection, doc_id) → transaction currently holding the lock.
    owners: HashMap<(String, u64), TransactionId>,
    /// Reverse index for O(1) release of everything a tx holds.
    held: HashMap<TransactionId, Vec<(String, u64)>>,
}

#[derive(Default)]
pub(crate) struct DocLockManager {
    state: Mutex<State>,
    cv: Condvar,
}

impl DocLockManager {
    /// Acquire the write lock on one document for `tx_id`, waiting up to
    /// `timeout` for the current holder to release. Re-entrant per
    /// transaction.
    pub fn lock(
        &self,
        collection: &str,
        doc_id: u64,
        tx_id: TransactionId,
        timeout: Duration,
    ) -> Result<()> {
        let key = (collection.to_string(), doc_id);
        let deadline = Instant::now() + timeout;
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            match st.owners.get(&key) {
                Some(&owner) if owner == tx_id => return Ok(()),
                Some(_) => {
                    let now = Instant::now();
                    if now >= deadline {
                        return Err(Error::LockTimeout {
                            collection: collection.to_string(),
                            doc_id,
                        });
                    }
                    let (guard, _) = self
                        .cv
                        .wait_timeout(st, deadline - now)
                        .unwrap_or_else(|e| e.into_inner());
                    st = guard;
                }
                None => {
                    st.owners.insert(key.clone(), tx_id);
                    st.held.entry(tx_id).or_default().push(key);
                    return Ok(());
                }
            }
        }
    }

    /// Release every lock `tx_id` holds. Idempotent; called on commit
    /// (as soon as the in-memory apply is done — waiters may proceed
    /// while the releaser waits for its group fsync) and on rollback.
    pub fn release_all(&self, tx_id: TransactionId) {
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(keys) = st.held.remove(&tx_id) {
            for key in keys {
                st.owners.remove(&key);
            }
            drop(st);
            self.cv.notify_all();
        }
    }
}
