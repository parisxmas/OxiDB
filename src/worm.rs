//! WORM phase 2 — engine-level immutability.
//!
//! Closes the rollback-safety + direct-engine-bypass gaps from DMS's
//! application-level WORM phase 1 (see DMS's
//! `docs/operations/worm-roadmap.md`). The application-level gate in
//! DMS's `Document.CheckMutation` is still useful as a fast
//! pre-check + error-shaping layer; this module is the load-bearing
//! enforcement underneath. Even a direct-OxiDB-shell connection or a
//! rolled-back-to-pre-WORM DMS binary cannot bypass the lock.
//!
//! Storage shape: one append-only file per collection,
//! `<collection>.worm`, holding records of the form
//!
//!   `[op: u8 = 0x01 lock | 0x02 release][doc_id: u64 LE][locked_until_micros: u64 LE]`
//!
//! Replayed at open-time into an in-memory `HashMap<DocumentId, u64>`.
//! A release record overwrites the in-memory entry with 0, which
//! `is_locked` treats as unlocked. The on-disk log is the source of
//! truth so the in-memory map can be reconstructed after a crash
//! without needing a separate snapshot.
//!
//! The bitmap key is `locked_until_micros = u64::MAX` for indefinite
//! retention (matches DMS's `RetentionDays: 0` semantics). Finite
//! retention writes a wall-clock expiry; `is_locked` compares against
//! `now`. Time travel via clock skew is acknowledged; the calling
//! side (DMS) handles legal-hold semantics separately.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::locks::Mutex;

/// Op code: a lock entry that pins `doc_id` until `locked_until_micros`.
const OP_LOCK: u8 = 0x01;
/// Op code: a release entry that clears the lock. The on-disk log
/// stays append-only; recovery resolves the latest entry per doc.
const OP_RELEASE: u8 = 0x02;

const RECORD_SIZE: usize = 1 + 8 + 8;

/// `u64::MAX` is the sentinel for "indefinite retention" — locks
/// with this value are never time-expired and can only be removed
/// via an explicit Release (admin-only, audited at the wire layer).
pub const INDEFINITE: u64 = u64::MAX;

/// WormSet is the per-collection lock state. Wired into
/// `BTreeCollection` so the mutation paths can consult it before
/// touching the btree.
pub struct WormSet {
    /// In-memory truth: doc_id → locked_until_micros. Entries with
    /// value 0 are "released" — they survive in the map only because
    /// the on-disk log replayed them; future writes upsert this map.
    /// We never grow without bound: a release writes the entry to
    /// 0 in memory AND appends a release record on disk.
    locks: Mutex<HashMap<u64, u64>>,
    /// Append-only log handle. Held under the mutex to keep ordering
    /// strict with the in-memory state — concurrent locks/releases
    /// serialize through here.
    log: Mutex<File>,
    /// Path on disk for the log file. Held for tooling (backup,
    /// archive) that needs to know where the file is.
    path: PathBuf,
}

impl WormSet {
    /// Open or create the WORM log for a collection. Empty file →
    /// empty set. Existing file is replayed into the in-memory map.
    /// Truncated / corrupt tail is tolerated; recovery stops at the
    /// last fully-readable record (same approach as `wal.rs`).
    pub fn open(coll_path: &Path) -> Result<Self> {
        let path = worm_path(coll_path);
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)?;

        let mut locks: HashMap<u64, u64> = HashMap::new();
        let mut buf = [0u8; RECORD_SIZE];
        loop {
            match file.read(&mut buf) {
                Ok(0) => break,                 // EOF, clean.
                Ok(RECORD_SIZE) => {}           // full record, fall through.
                Ok(_) => break,                 // truncated tail, tolerate.
                Err(e) => return Err(e.into()), // real I/O error.
            }
            let op = buf[0];
            let doc_id = u64::from_le_bytes(buf[1..9].try_into().unwrap());
            let until = u64::from_le_bytes(buf[9..17].try_into().unwrap());
            match op {
                OP_LOCK => {
                    // Lock entry: upserts. Later lock with smaller
                    // until still wins (operator intent — clamping
                    // retention down is allowed). Larger until also
                    // wins. Operators don't accidentally lower locks
                    // because the wire command refuses to lower
                    // existing values; this replay path is just
                    // applying what was already accepted.
                    locks.insert(doc_id, until);
                }
                OP_RELEASE => {
                    // Release: in-memory value 0 = unlocked. Keep
                    // the entry around so `is_locked` shortcuts via
                    // map lookup; map growth bounded by the number
                    // of distinct doc_ids ever locked, which is the
                    // operator's cost anyway.
                    locks.insert(doc_id, 0);
                }
                _ => {
                    // Unknown op: tolerate as a tail-corruption
                    // boundary, same as wal.rs.
                    break;
                }
            }
        }

        Ok(Self {
            locks: Mutex::new(locks),
            log: Mutex::new(file),
            path,
        })
    }

    /// Lock `doc_id` until `locked_until_micros`. Wire-side admin
    /// gate runs before this is reached.
    ///
    /// Refuses to LOWER an existing lock — once a doc is locked
    /// until time T, the engine won't let a subsequent call reduce
    /// it to T' < T. Raising is allowed (extending retention).
    /// Release is the only way to clear; it goes through `release()`.
    pub fn lock(&self, doc_id: u64, locked_until_micros: u64) -> Result<()> {
        let mut locks = self.locks.lock();
        if let Some(&existing) = locks.get(&doc_id) {
            if existing > 0 && locked_until_micros < existing {
                return Err(Error::InvalidQuery(format!(
                    "worm_lock: refuse to lower existing lock on doc {} \
                     (existing locked_until={} requested={})",
                    doc_id, existing, locked_until_micros
                )));
            }
        }
        let mut log = self.log.lock();
        write_record(&mut log, OP_LOCK, doc_id, locked_until_micros)?;
        locks.insert(doc_id, locked_until_micros);
        Ok(())
    }

    /// Release `doc_id`. Admin-only at the wire layer. Audited there;
    /// this method is the storage mechanism only.
    pub fn release(&self, doc_id: u64) -> Result<()> {
        let mut locks = self.locks.lock();
        let mut log = self.log.lock();
        write_record(&mut log, OP_RELEASE, doc_id, 0)?;
        locks.insert(doc_id, 0);
        Ok(())
    }

    /// is_locked reports whether `doc_id` is currently locked
    /// against mutation. `now_micros` is the caller's wall-clock
    /// reading (typically `crate::pitr::now_micros()`); separating
    /// the parameter keeps this function pure for tests.
    pub fn is_locked(&self, doc_id: u64, now_micros: u64) -> bool {
        let locks = self.locks.lock();
        match locks.get(&doc_id) {
            Some(&until) => until > 0 && (until == INDEFINITE || until > now_micros),
            None => false,
        }
    }

    /// locked_until returns the current `locked_until_micros` for a
    /// doc, or `None` if not locked. Surfaced for admin tooling
    /// (status checks, audit views).
    pub fn locked_until(&self, doc_id: u64) -> Option<u64> {
        let locks = self.locks.lock();
        locks.get(&doc_id).copied().filter(|&v| v > 0)
    }

    /// path on disk (for backup / archive tooling).
    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn worm_path(coll_path: &Path) -> PathBuf {
    let mut name = coll_path
        .file_name()
        .map(|n| n.to_os_string())
        .unwrap_or_default();
    name.push(".worm");
    coll_path.with_file_name(name)
}

fn write_record(file: &mut File, op: u8, doc_id: u64, until: u64) -> Result<()> {
    let mut rec = [0u8; RECORD_SIZE];
    rec[0] = op;
    rec[1..9].copy_from_slice(&doc_id.to_le_bytes());
    rec[9..17].copy_from_slice(&until.to_le_bytes());
    file.write_all(&rec)?;
    // fsync the lock state immediately — a lost lock is a real
    // regulatory problem (write-once invariant broken). The WAL's
    // group-commit path doesn't help here because locks aren't on
    // the main write hot-path; one fsync per lock op is acceptable.
    file.sync_data()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn tmp_path() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("oxidb-worm-test-{}", rand_u64()));
        fs::create_dir_all(&dir).unwrap();
        dir.join("test_coll")
    }

    fn rand_u64() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }

    #[test]
    fn lock_release_roundtrip() {
        let p = tmp_path();
        let w = WormSet::open(&p).unwrap();
        assert!(!w.is_locked(42, 1000));
        w.lock(42, INDEFINITE).unwrap();
        assert!(w.is_locked(42, 1000));
        assert!(w.is_locked(42, u64::MAX - 1));
        w.release(42).unwrap();
        assert!(!w.is_locked(42, 1000));
        let _ = fs::remove_dir_all(p.parent().unwrap());
    }

    #[test]
    fn finite_lock_expires_at_until() {
        let p = tmp_path();
        let w = WormSet::open(&p).unwrap();
        w.lock(7, 1000).unwrap();
        assert!(w.is_locked(7, 500));
        assert!(w.is_locked(7, 999));
        assert!(!w.is_locked(7, 1000)); // boundary: now == until → unlocked
        assert!(!w.is_locked(7, 5000));
        let _ = fs::remove_dir_all(p.parent().unwrap());
    }

    #[test]
    fn refuses_to_lower_lock() {
        let p = tmp_path();
        let w = WormSet::open(&p).unwrap();
        w.lock(1, 1000).unwrap();
        let err = w.lock(1, 500).unwrap_err();
        match err {
            Error::InvalidQuery(msg) => assert!(msg.contains("refuse to lower")),
            other => panic!("expected InvalidQuery, got {:?}", other),
        }
        // Raising still works.
        w.lock(1, 2000).unwrap();
        assert!(w.is_locked(1, 1500));
        let _ = fs::remove_dir_all(p.parent().unwrap());
    }

    #[test]
    fn replay_after_reopen() {
        let p = tmp_path();
        {
            let w = WormSet::open(&p).unwrap();
            w.lock(10, INDEFINITE).unwrap();
            w.lock(20, 9999).unwrap();
            w.release(10).unwrap();
        }
        // Reopen — state should be:
        //   10: released
        //   20: locked until 9999
        let w2 = WormSet::open(&p).unwrap();
        assert!(!w2.is_locked(10, 1));
        assert!(w2.is_locked(20, 100));
        assert!(!w2.is_locked(20, 99999));
        let _ = fs::remove_dir_all(p.parent().unwrap());
    }
}
