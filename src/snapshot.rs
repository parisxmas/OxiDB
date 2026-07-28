//! MVCC-lite: read snapshots for the document engine (ADR-0017).
//!
//! Snapshot visibility for the READ path only. The write path — OCC,
//! `find_for_update`, group commit — is untouched; this module only
//! remembers, while at least one snapshot is open, what documents looked
//! like *before* each change, so a reader pinned to commit-sequence `S` can
//! roll any later change back.
//!
//! The economics the ADR promised, kept:
//! - **No snapshot open ⇒ no cost.** Writers check one relaxed atomic and
//!   move on; the version map is empty; the commit counter does not tick.
//! - **Unchanged documents pay nothing.** A document is consulted against
//!   the map only while `total_entries > 0`, and the probe is one hash
//!   lookup that misses.
//! - Priors are stored as the encoded bytes the storage layer already had
//!   in hand (its `insert`/`remove` return the old value), decoded lazily
//!   if a snapshot actually needs them.
//!
//! Correctness hinges on one linearization point owned by the ENGINE, not
//! this module: a snapshot begins under `commit_lock.write()`, and every
//! logical write records under `commit_lock.read()` (direct writes and OCC
//! commits already hold it; the engine adds it where it was missing). So
//! "every change with `seq > S`" is exactly "every change that became
//! visible after the snapshot began" — no torn boundary.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::error::{Error, Result};

/// One remembered change: the state the document had *before* the change
/// stamped `seq` (`None` = it did not exist — the change was an insert).
struct VersionEntry {
    seq: u64,
    prior: Option<Arc<Vec<u8>>>,
}

#[derive(Default)]
struct GateState {
    /// Commit-sequence counter. Ticks only while snapshots are active.
    counter: u64,
    /// Active snapshots: S → (refcount, hard deadline).
    snaps: BTreeMap<u64, (usize, Instant)>,
    /// collection → doc id → changes since the oldest active snapshot,
    /// ascending by seq.
    map: HashMap<String, HashMap<u64, Vec<VersionEntry>>>,
}

/// What a snapshot read should use for a document it found in live storage.
pub enum Resolved {
    /// The live value is already correct for this snapshot.
    Current,
    /// The document had this earlier state at the snapshot (decoded lazily
    /// by the caller); `None` = it did not exist yet — skip it.
    Prior(Option<Arc<Vec<u8>>>),
}

pub struct SnapGate {
    /// Fast writer gate: number of active snapshots. Zero = record() is a
    /// single relaxed load and out.
    active: AtomicU64,
    /// Fast reader gate: total remembered entries. Zero = resolve() is a
    /// single relaxed load and out — the common case even under a snapshot,
    /// when nothing is being written concurrently.
    total_entries: AtomicU64,
    state: crate::locks::Mutex<GateState>,
    /// Hard ceiling on snapshot lifetime. A snapshot held open forever is a
    /// bloat bug by definition (the map can only grow while one is active),
    /// so an expired snapshot dies — its reads fail, writers never do.
    max_age: Duration,
}

fn max_age_from_env() -> Duration {
    let secs = std::env::var("OXIDB_SNAPSHOT_MAX_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(300);
    Duration::from_secs(secs.max(1))
}

impl Default for SnapGate {
    fn default() -> Self {
        Self::new()
    }
}

impl SnapGate {
    pub fn new() -> Self {
        SnapGate {
            active: AtomicU64::new(0),
            total_entries: AtomicU64::new(0),
            state: crate::locks::Mutex::new(GateState::default()),
            max_age: max_age_from_env(),
        }
    }

    /// At least one snapshot open? The storage layer's one-load gate.
    #[inline]
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed) > 0
    }

    /// Register a snapshot and return its sequence `S`. The caller MUST
    /// hold the engine's `commit_lock` write side across this call — that
    /// exclusivity is what makes `S` a clean boundary between "visible to
    /// this snapshot" and "rolled back by it".
    pub fn begin(&self) -> u64 {
        let mut st = self.state.lock();
        let s = st.counter;
        let deadline = Instant::now() + self.max_age;
        let e = st.snaps.entry(s).or_insert((0, deadline));
        e.0 += 1;
        if e.1 < deadline {
            e.1 = deadline;
        }
        self.active.fetch_add(1, Ordering::SeqCst);
        s
    }

    /// Release a snapshot. Idempotence is the caller's problem; the engine
    /// wraps this in a handle that ends exactly once.
    pub fn end(&self, s: u64) {
        let mut st = self.state.lock();
        if let Some(e) = st.snaps.get_mut(&s) {
            e.0 -= 1;
            if e.0 == 0 {
                st.snaps.remove(&s);
            }
            self.active.fetch_sub(1, Ordering::SeqCst);
        }
        Self::prune(&mut st, &self.total_entries);
    }

    /// Is `s` still a live snapshot? Reads through a dead one must error,
    /// not silently degrade to latest.
    pub fn check(&self, s: u64) -> Result<()> {
        if self.state.lock().snaps.contains_key(&s) {
            Ok(())
        } else {
            Err(Error::SnapshotExpired(s))
        }
    }

    /// Remember a document's prior state. Called by the storage layer with
    /// whatever `insert`/`remove` displaced, under the writer's
    /// `commit_lock.read()` scope. Free when no snapshot is open.
    pub fn record(&self, collection: &str, doc_id: u64, prior: Option<&[u8]>) {
        if self.active.load(Ordering::Relaxed) == 0 {
            return;
        }
        let mut st = self.state.lock();
        if st.snaps.is_empty() {
            return; // raced an end(); nothing left to serve
        }
        st.counter += 1;
        let seq = st.counter;
        st.map
            .entry(collection.to_string())
            .or_default()
            .entry(doc_id)
            .or_default()
            .push(VersionEntry {
                seq,
                prior: prior.map(|b| Arc::new(b.to_vec())),
            });
        self.total_entries.fetch_add(1, Ordering::Relaxed);
    }

    /// Has ANYTHING been remembered for `collection` with seq > `s`? The
    /// optimistic fast paths run against live state and then ask this: a
    /// `false` answer proves no write raced the read, so live state WAS the
    /// snapshot. One relaxed load in the common case.
    pub fn changed_since(&self, collection: &str, s: u64) -> bool {
        if self.total_entries.load(Ordering::Relaxed) == 0 {
            return false;
        }
        let st = self.state.lock();
        st.map
            .get(collection)
            .is_some_and(|m| m.values().any(|v| v.iter().any(|e| e.seq > s)))
    }

    /// Resolve one live document for snapshot `s`: the oldest remembered
    /// change with `seq > s` holds the state the document had at `s`.
    pub fn resolve(&self, collection: &str, doc_id: u64, s: u64) -> Resolved {
        if self.total_entries.load(Ordering::Relaxed) == 0 {
            return Resolved::Current;
        }
        let st = self.state.lock();
        let Some(chain) = st.map.get(collection).and_then(|m| m.get(&doc_id)) else {
            return Resolved::Current;
        };
        match chain.iter().find(|e| e.seq > s) {
            None => Resolved::Current,
            Some(e) => Resolved::Prior(e.prior.clone()),
        }
    }

    /// Documents deleted after `s` do not appear in live storage at all;
    /// this returns every remembered doc id of `collection` so a snapshot
    /// scan can resurrect the ones that existed at `s`. Empty in the
    /// no-concurrent-writes case.
    pub fn remembered_ids(&self, collection: &str) -> Vec<u64> {
        if self.total_entries.load(Ordering::Relaxed) == 0 {
            return Vec::new();
        }
        let st = self.state.lock();
        st.map
            .get(collection)
            .map(|m| m.keys().copied().collect())
            .unwrap_or_default()
    }

    /// Drop snapshots that outlived `max_age` (their readers start failing;
    /// writers were never waiting on them) and prune the map. Driven by the
    /// engine's existing TTL tick.
    pub fn expire_tick(&self) {
        let mut st = self.state.lock();
        if st.snaps.is_empty() {
            return;
        }
        let now = Instant::now();
        let expired: Vec<u64> = st
            .snaps
            .iter()
            .filter(|(_, (_, dl))| *dl <= now)
            .map(|(s, _)| *s)
            .collect();
        for s in expired {
            if let Some((refs, _)) = st.snaps.remove(&s) {
                self.active.fetch_sub(refs as u64, Ordering::SeqCst);
            }
        }
        Self::prune(&mut st, &self.total_entries);
    }

    /// Drop every entry no live snapshot can still need: an entry rolls a
    /// snapshot back only when `entry.seq > S`, so anything at or below the
    /// oldest active S is dead weight. No snapshots ⇒ the whole map goes.
    fn prune(st: &mut GateState, total: &AtomicU64) {
        let Some(&min_s) = st.snaps.keys().next() else {
            let dropped: u64 = st
                .map
                .values()
                .map(|m| m.values().map(Vec::len).sum::<usize>() as u64)
                .sum();
            if dropped > 0 {
                total.fetch_sub(dropped, Ordering::Relaxed);
            }
            st.map.clear();
            return;
        };
        let mut dropped = 0u64;
        st.map.retain(|_, docs| {
            docs.retain(|_, chain| {
                let before = chain.len();
                chain.retain(|e| e.seq > min_s);
                dropped += (before - chain.len()) as u64;
                !chain.is_empty()
            });
            !docs.is_empty()
        });
        if dropped > 0 {
            total.fetch_sub(dropped, Ordering::Relaxed);
        }
    }
}

/// A snapshot-resolved document set for one collection: live docs rolled
/// back to `s`, plus docs deleted after `s` resurrected. This is the
/// always-correct slow path the optimistic fast paths fall back to. Doc ids
/// come from the `_id` every stored document carries.
pub fn snapshot_docs(
    gate: &SnapGate,
    collection: &str,
    s: u64,
    live: Vec<Arc<Value>>,
) -> Result<Vec<Arc<Value>>> {
    let mut out = Vec::with_capacity(live.len());
    let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::new();
    for doc in live {
        let Some(id) = doc.get("_id").and_then(|v| v.as_u64()) else {
            out.push(doc);
            continue;
        };
        seen.insert(id);
        match gate.resolve(collection, id, s) {
            Resolved::Current => out.push(doc),
            Resolved::Prior(Some(bytes)) => out.push(Arc::new(crate::codec::decode_doc(&bytes)?)),
            Resolved::Prior(None) => {} // inserted after the snapshot
        }
    }
    // Deleted after `s`: not in live storage, remembered in the gate.
    for id in gate.remembered_ids(collection) {
        if seen.contains(&id) {
            continue;
        }
        if let Resolved::Prior(Some(bytes)) = gate.resolve(collection, id, s) {
            out.push(Arc::new(crate::codec::decode_doc(&bytes)?));
        }
    }
    Ok(out)
}
