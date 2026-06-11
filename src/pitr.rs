//! Point-In-Time Recovery — the archive sequencer.
//!
//! PITR needs a single global ordering across every collection's WAL, but
//! OxiDB has none: each collection's `.wal` is an independent byte stream,
//! `_tx_commit_log` is an unordered set, and most writes carry `tx_id = 0`.
//! The [`ArchiveSequencer`] supplies that ordering — a Global Sequence
//! Number (GSN), monotonic across all collections, plus a wall-clock stamp,
//! handed to every durable WAL write (see [`crate::wal::Wal`]).
//!
//! ## Why the GSN is leased, not bumped-per-write
//!
//! The GSN must stay monotonic *across process restarts* — otherwise GSN 5
//! from Monday and GSN 5 from Tuesday collide in the archive. But the live
//! WAL is truncated on clean shutdown, so it can't be the source of the
//! high-water mark. Instead the sequencer persists its ceiling to a tiny
//! `_gsn` file in **leases**: on `open` it reserves a block of
//! [`GSN_LEASE`] numbers with one fsync, hands them out from memory, and
//! only touches the file again when the block is exhausted. A crash wastes
//! at most one lease worth of GSNs — a harmless gap, since replay tolerates
//! missing numbers. Cost: one fsync per `GSN_LEASE` writes, not per write.

#![cfg(not(target_arch = "wasm32"))]

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::locks::Mutex;
use crate::wal::WalMeta;

/// GSNs reserved per on-disk lease. A crash wastes at most this many as a
/// harmless gap in the sequence; replay skips missing numbers.
const GSN_LEASE: u64 = 10_000;

/// Name of the lease file under the data directory.
pub const GSN_FILE: &str = "_gsn";

/// Default live-WAL size at which a collection seals its current segment
/// and starts a fresh one. Overridable via `OXIDB_WAL_SEGMENT_BYTES`.
pub const DEFAULT_WAL_SEGMENT_BYTES: u64 = 16 * 1024 * 1024;

/// Read the WAL segment threshold from `OXIDB_WAL_SEGMENT_BYTES`, falling
/// back to [`DEFAULT_WAL_SEGMENT_BYTES`]. A zero / unparseable value is
/// ignored — sealing must always have a positive threshold.
fn env_segment_threshold() -> u64 {
    std::env::var("OXIDB_WAL_SEGMENT_BYTES")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_WAL_SEGMENT_BYTES)
}

/// Hands out globally-ordered, wall-clock-stamped sequence numbers for
/// PITR. One instance per `OxiDb`, shared (`Arc`) by every collection's WAL.
pub struct ArchiveSequencer {
    /// Next GSN to hand out. Always `<= leased_through` once a caller is
    /// inside `next()`.
    next_gsn: AtomicU64,
    /// Highest GSN durably reserved in the `_gsn` file.
    leased_through: AtomicU64,
    /// The `_gsn` file: a single `[u64 LE]` holding `leased_through`.
    /// The mutex serializes lease extensions (rare — once per `GSN_LEASE`).
    state: Mutex<File>,
    /// Live-WAL size past which a collection seals its current segment.
    /// Shared by every collection's WAL — PITR config lives on the
    /// sequencer since it is already the one object every WAL holds.
    segment_threshold_bytes: u64,
}

impl ArchiveSequencer {
    /// Open (or create) the `_gsn` lease file under `data_dir` and start
    /// the sequencer strictly above every GSN that may have been handed
    /// out before — then immediately reserve the first lease, so even a
    /// crash before the first write cannot reuse a number.
    pub fn open(data_dir: &Path) -> Result<Self> {
        let path = data_dir.join(GSN_FILE);
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)?;

        // Distinguish "fresh database" (empty file → start at 1) from a
        // SHORT or unreadable file (torn in-place write, I/O error). The
        // latter must fail loudly: silently restarting the sequence at 1
        // would hand out duplicate GSNs, breaking the archive's global
        // ordering and the `(gsn, doc_id)` dedup in `replay_into`.
        let file_len = file.metadata()?.len();
        let prev_ceiling = if file_len == 0 {
            0
        } else if file_len < 8 {
            return Err(crate::error::Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "{} is truncated ({file_len} bytes); refusing to restart the \
                     GSN sequence — restore the file or delete it ONLY if no \
                     archive segments exist",
                    path.display()
                ),
            )));
        } else {
            let mut buf = [0u8; 8];
            file.read_exact(&mut buf)?;
            u64::from_le_bytes(buf)
        };

        let start = prev_ceiling + 1;
        let new_ceiling = start + GSN_LEASE;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&new_ceiling.to_le_bytes())?;
        file.sync_data()?;

        Ok(Self {
            next_gsn: AtomicU64::new(start),
            leased_through: AtomicU64::new(new_ceiling),
            state: Mutex::new(file),
            segment_threshold_bytes: env_segment_threshold(),
        })
    }

    /// Live-WAL size threshold past which a collection seals its current
    /// segment and rotates to a fresh one.
    pub fn segment_threshold_bytes(&self) -> u64 {
        self.segment_threshold_bytes
    }

    /// Override the segment threshold, ignoring `OXIDB_WAL_SEGMENT_BYTES`.
    /// Builder-style — for tests and for embedders that configure rotation
    /// in code rather than through the environment.
    pub fn with_segment_threshold(mut self, bytes: u64) -> Self {
        self.segment_threshold_bytes = bytes;
        self
    }

    /// Allocate the next GSN and stamp it with the current wall-clock.
    /// The returned GSN is guaranteed to sit within the durably-reserved
    /// range before it can appear in a WAL record.
    pub fn next(&self) -> Result<WalMeta> {
        let gsn = self.next_gsn.fetch_add(1, Ordering::SeqCst);
        // If we've run past the durably-reserved ceiling, extend the lease
        // before letting this GSN escape into a WAL record. The loop covers
        // many writers racing past the threshold at once.
        while gsn >= self.leased_through.load(Ordering::Acquire) {
            self.extend_lease()?;
        }
        Ok(WalMeta {
            gsn,
            wall_clock_micros: now_micros(),
        })
    }

    /// Persist a higher ceiling. Serialized by `state`; idempotent — a
    /// thread that finds the ceiling already far enough ahead does nothing.
    fn extend_lease(&self) -> Result<()> {
        let mut file = self.state.lock();
        let target = self.next_gsn.load(Ordering::Acquire) + GSN_LEASE;
        if target <= self.leased_through.load(Ordering::Acquire) {
            return Ok(()); // another writer already extended far enough
        }
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&target.to_le_bytes())?;
        file.sync_data()?;
        self.leased_through.store(target, Ordering::Release);
        Ok(())
    }

    /// The next GSN that will be handed out — for watermarks and diagnostics.
    pub fn current_gsn(&self) -> u64 {
        self.next_gsn.load(Ordering::SeqCst)
    }
}

/// Wall-clock now, microseconds since the Unix epoch. Saturates to 0 if the
/// clock is before the epoch (it never is in practice).
pub fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

/// Where to restore the database to in [`crate::archive::replay_into`] /
/// `OxiDb::restore_to_point`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PitrTarget {
    /// Restore to the largest GSN whose record's wall-clock is `<=` this
    /// timestamp (micros since the Unix epoch). Tolerates non-monotonic
    /// wall-clocks — it resolves by value, not by position.
    Timestamp(u64),
    /// Restore to exactly this GSN (inclusive).
    Gsn(u64),
    /// Restore to the newest record present in the archive.
    Latest,
}

/// Format version for `base.meta`.
pub const BASE_META_VERSION: u32 = 1;
/// Name of the base-backup watermark file. It is written into the data
/// directory so it travels inside the backup tarball.
pub const BASE_META_FILE: &str = "base.meta";

/// Watermark embedded in a base backup. The PITR replay tool reads it to
/// learn where to resume from the archive: the base is guaranteed to
/// contain every write with `gsn < base_gsn` (the backup barriers every
/// collection's WAL after reading the counter — see `OxiDb::backup`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaseMeta {
    pub format_version: u32,
    /// GSN watermark. `0` means PITR was disabled when the backup ran —
    /// there is no archive to replay against this base.
    pub base_gsn: u64,
    /// Wall-clock (micros since the Unix epoch) the watermark was taken.
    pub base_wall_clock: u64,
}

impl BaseMeta {
    /// A fresh watermark for `base_gsn`, stamped with the current wall-clock.
    pub fn new(base_gsn: u64) -> Self {
        Self {
            format_version: BASE_META_VERSION,
            base_gsn,
            base_wall_clock: now_micros(),
        }
    }

    /// Write `base.meta` into `dir`, atomically (`tmp -> fsync -> rename
    /// -> fsync dir`) so a crash never leaves a half-written watermark.
    pub fn write_to(&self, dir: &Path) -> Result<()> {
        let json = serde_json::to_vec_pretty(self)
            .map_err(|e| crate::error::Error::Io(std::io::Error::other(e)))?;
        let tmp = dir.join(format!("{BASE_META_FILE}.tmp"));
        {
            let mut f = File::create(&tmp)?;
            f.write_all(&json)?;
            f.sync_data()?;
        }
        std::fs::rename(&tmp, dir.join(BASE_META_FILE))?;
        if let Ok(d) = File::open(dir) {
            let _ = d.sync_all();
        }
        Ok(())
    }

    /// Read `base.meta` from `dir`, or `None` if it is not there (e.g. a
    /// backup taken before PITR existed, or with PITR disabled).
    pub fn read_from(dir: &Path) -> Result<Option<BaseMeta>> {
        match std::fs::read(dir.join(BASE_META_FILE)) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|e| crate::error::Error::Io(std::io::Error::other(e))),
            Err(_) => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn hands_out_monotonic_gsns() {
        let dir = TempDir::new().unwrap();
        let seq = ArchiveSequencer::open(dir.path()).unwrap();
        let a = seq.next().unwrap().gsn;
        let b = seq.next().unwrap().gsn;
        let c = seq.next().unwrap().gsn;
        assert_eq!((a, b, c), (1, 2, 3));
        assert_eq!(seq.current_gsn(), 4);
    }

    #[test]
    fn stamps_a_plausible_wall_clock() {
        let dir = TempDir::new().unwrap();
        let seq = ArchiveSequencer::open(dir.path()).unwrap();
        let m = seq.next().unwrap();
        // After 2020-01-01 in micros — i.e. the clock was actually read.
        assert!(m.wall_clock_micros > 1_577_836_800_000_000);
    }

    #[test]
    fn reopen_never_reuses_a_gsn() {
        let dir = TempDir::new().unwrap();
        {
            let seq = ArchiveSequencer::open(dir.path()).unwrap();
            seq.next().unwrap();
            seq.next().unwrap(); // handed out 1, 2
        }
        // A fresh sequencer must resume strictly above the persisted
        // ceiling — never reissuing 1 or 2 (the lease reserved a whole
        // block, so the next run starts past it).
        let seq2 = ArchiveSequencer::open(dir.path()).unwrap();
        let g = seq2.next().unwrap().gsn;
        assert!(
            g > GSN_LEASE,
            "expected resume past the reserved lease, got {g}"
        );
    }

    #[test]
    fn lease_extends_transparently_past_the_block() {
        let dir = TempDir::new().unwrap();
        let seq = ArchiveSequencer::open(dir.path()).unwrap();
        // Cross the first lease boundary; GSNs stay unique and monotonic.
        let mut last = 0;
        for _ in 0..(GSN_LEASE + 50) {
            let g = seq.next().unwrap().gsn;
            assert!(g > last);
            last = g;
        }
    }

    #[test]
    fn concurrent_writers_get_unique_gsns() {
        let dir = TempDir::new().unwrap();
        let seq = Arc::new(ArchiveSequencer::open(dir.path()).unwrap());
        let mut handles = Vec::new();
        for _ in 0..8 {
            let seq = Arc::clone(&seq);
            handles.push(std::thread::spawn(move || {
                (0..500)
                    .map(|_| seq.next().unwrap().gsn)
                    .collect::<Vec<_>>()
            }));
        }
        let mut all: Vec<u64> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        all.sort_unstable();
        let before = all.len();
        all.dedup();
        assert_eq!(all.len(), before, "GSNs must be unique across writers");
        assert_eq!(all.len(), 8 * 500);
    }

    // ── PITR Phase 4: base-backup watermark ─────────────────────────────

    #[test]
    fn base_meta_roundtrips() {
        let dir = TempDir::new().unwrap();
        let meta = BaseMeta::new(4242);
        assert_eq!(meta.format_version, BASE_META_VERSION);
        assert_eq!(meta.base_gsn, 4242);
        assert!(meta.base_wall_clock > 1_577_836_800_000_000);

        meta.write_to(dir.path()).unwrap();
        let read = BaseMeta::read_from(dir.path()).unwrap().unwrap();
        assert_eq!(read.base_gsn, 4242);
        assert_eq!(read.base_wall_clock, meta.base_wall_clock);
        assert_eq!(read.format_version, BASE_META_VERSION);
    }

    #[test]
    fn base_meta_read_missing_is_none() {
        let dir = TempDir::new().unwrap();
        assert!(BaseMeta::read_from(dir.path()).unwrap().is_none());
    }
}
