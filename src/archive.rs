//! Point-In-Time Recovery — the WAL segment archiver.
//!
//! Phase 2 rotates each collection's WAL into numbered sealed segments
//! (`<collection>.wal.<seq>`) that pile up in the data directory. This
//! module copies them — crash-safely — into the archive directory so the
//! PITR replay tool (Phase 5) has a durable, ordered history to replay.
//!
//! ## Layout
//!
//! ```text
//! _archive/
//!   manifest.json                 versioned index, rebuilt from the .seg trailers
//!   segments/
//!     <collection>.wal.<seq>.seg  verbatim sealed WAL bytes + a fixed trailer
//! ```
//!
//! A `.seg` file is the sealed segment's bytes copied **verbatim** (so
//! at-rest encryption is preserved untouched) followed by a fixed 56-byte
//! trailer carrying the GSN / wall-clock range, record count, and a CRC
//! of the copied bytes.
//!
//! ## Crash safety & idempotency
//!
//! Each `.seg` is written `tmp -> fsync -> rename -> fsync(dir)`. A
//! segment is archived iff `<name>.seg` exists, so [`archive_pass`] re-run
//! after a crash simply skips what is already there and finishes what is
//! not — every sealed segment ends up archived exactly once. The manifest
//! is a pure derived index: it is rebuilt by scanning the `.seg` trailers,
//! so a torn or stale manifest self-heals on the next pass.
//!
//! Sealed segments are immutable once renamed (the collection only ever
//! writes to the fresh live `.wal`), so the archiver reads them with no
//! locking and never touches the foreground write path — it is strictly
//! best-effort.
//!
//! ## What this phase does NOT do
//!
//! It does not delete the original `<collection>.wal.<seq>` from the data
//! directory (empty ones aside — those carry nothing). Those segments are
//! still needed by crash recovery until the btree snapshot covers them;
//! safe removal is coupled to the snapshot point and is left to a later
//! change. Until then a sealed segment is both archived here and replayed
//! from the data dir on open (idempotently).

#![cfg(not(target_arch = "wasm32"))]

use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::crypto::EncryptionKey;
use crate::error::{Error, Result};
use crate::wal::{Wal, WalRecord};

/// Manifest format version — bumped on any breaking manifest change.
pub const MANIFEST_VERSION: u32 = 1;
/// Manifest filename under the archive directory.
pub const MANIFEST_FILE: &str = "manifest.json";
/// Subdirectory holding the archived `.seg` files.
pub const SEGMENTS_DIR: &str = "segments";
/// Default archive directory name under the data directory.
pub const DEFAULT_ARCHIVE_SUBDIR: &str = "_archive";

/// Marks the end of a `.seg` trailer so a reader can validate it ("OXAS").
const TRAILER_MAGIC: u32 = 0x4F58_4153;
/// Fixed `.seg` trailer size: 6×u64 + crc(u32) + magic(u32).
const TRAILER_SIZE: u64 = 6 * 8 + 4 + 4;

/// One archived segment's metadata, as recorded in the manifest. Mirrors
/// the on-disk `.seg` trailer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    /// Archived filename, e.g. `users.wal.3.seg`.
    pub segment: String,
    /// Original sealed-segment name in the data dir, e.g. `users.wal.3`.
    pub original: String,
    /// Smallest / largest GSN among the segment's records (0 if none).
    pub start_gsn: u64,
    pub end_gsn: u64,
    /// Smallest / largest wall-clock (micros since epoch) among the records.
    pub start_wall_clock: u64,
    pub end_wall_clock: u64,
    /// Number of WAL records in the segment.
    pub record_count: u64,
    /// Length of the verbatim WAL bytes — the trailer starts at this offset.
    pub wal_byte_len: u64,
    /// CRC32 of the verbatim WAL bytes.
    pub content_crc: u32,
}

/// The archive manifest — a derived index over the `.seg` files, sorted
/// by `start_gsn`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub format_version: u32,
    pub segments: Vec<ManifestEntry>,
}

/// Outcome of one [`archive_pass`].
#[derive(Debug, Default, Clone, Copy)]
pub struct ArchiveStats {
    /// Sealed segments newly copied into the archive this pass.
    pub archived: usize,
    /// Sealed segments already archived (skipped).
    pub skipped: usize,
    /// Empty sealed segments removed from the data dir (they carry nothing).
    pub empty_removed: usize,
}

/// Run one archiving pass over `data_dir`, depositing into `archive_dir`.
/// Idempotent and crash-safe — safe to call on a schedule and after a crash.
pub fn archive_pass(
    data_dir: &Path,
    archive_dir: &Path,
    encryption: Option<&Arc<EncryptionKey>>,
) -> Result<ArchiveStats> {
    let segments_dir = archive_dir.join(SEGMENTS_DIR);
    fs::create_dir_all(&segments_dir)?;

    // Sweep stray temp files left by a crash mid-write.
    if let Ok(rd) = fs::read_dir(&segments_dir) {
        for entry in rd.flatten() {
            if entry.file_name().to_string_lossy().ends_with(".seg.tmp") {
                let _ = fs::remove_file(entry.path());
            }
        }
    }

    let mut stats = ArchiveStats::default();

    // Archive every not-yet-archived sealed segment in the data dir.
    for (name, path) in list_sealed_segments(data_dir) {
        let len = fs::metadata(&path)?.len();
        if len == 0 {
            // An empty sealed segment carries nothing — drop it.
            let _ = fs::remove_file(&path);
            stats.empty_removed += 1;
            continue;
        }
        let archived_path = segments_dir.join(format!("{name}.seg"));
        if archived_path.exists() {
            stats.skipped += 1;
            continue;
        }
        archive_one(&path, &name, &archived_path, &segments_dir, encryption)?;
        stats.archived += 1;
    }

    // Rebuild the manifest from the .seg trailers (self-healing index).
    rebuild_manifest(archive_dir, &segments_dir)?;
    Ok(stats)
}

/// Load the archive manifest, or an empty one if it does not exist yet.
pub fn load_manifest(archive_dir: &Path) -> Result<Manifest> {
    match fs::read(archive_dir.join(MANIFEST_FILE)) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map_err(|e| Error::Io(std::io::Error::other(e))),
        Err(_) => Ok(Manifest { format_version: MANIFEST_VERSION, segments: Vec::new() }),
    }
}

/// Resolve the archive directory: `OXIDB_ARCHIVE_DIR` if set, else
/// `<data_dir>/_archive`.
pub fn archive_dir_for(data_dir: &Path) -> PathBuf {
    match std::env::var("OXIDB_ARCHIVE_DIR") {
        Ok(d) if !d.is_empty() => PathBuf::from(d),
        _ => data_dir.join(DEFAULT_ARCHIVE_SUBDIR),
    }
}

// -----------------------------------------------------------------------
// Internal helpers
// -----------------------------------------------------------------------

/// Copy one sealed segment into the archive, crash-safely. The verbatim
/// WAL bytes are copied untouched; the records are read (decrypting if a
/// key is supplied) only to derive the trailer's GSN / wall-clock range.
fn archive_one(
    sealed_path: &Path,
    name: &str,
    archived_path: &Path,
    segments_dir: &Path,
    encryption: Option<&Arc<EncryptionKey>>,
) -> Result<()> {
    let wal_bytes = fs::read(sealed_path)?;
    let records = Wal::open_with_encryption(sealed_path, encryption.cloned())?.read_records()?;
    let (start_gsn, end_gsn, start_wc, end_wc) = gsn_time_range(&records);
    let content_crc = crc32(&wal_bytes);
    let trailer = build_trailer(
        start_gsn, end_gsn, start_wc, end_wc,
        records.len() as u64, wal_bytes.len() as u64, content_crc,
    );

    let tmp_path = segments_dir.join(format!("{name}.seg.tmp"));
    {
        let mut f = File::create(&tmp_path)?;
        f.write_all(&wal_bytes)?;
        f.write_all(&trailer)?;
        f.sync_data()?;
    }
    fs::rename(&tmp_path, archived_path)?;
    // Make the rename durable before the segment is considered archived.
    if let Ok(dir) = File::open(segments_dir) {
        let _ = dir.sync_all();
    }
    Ok(())
}

/// Rebuild `manifest.json` by scanning every `.seg` trailer. Atomic:
/// `tmp -> fsync -> rename -> fsync(dir)`. The manifest is a pure index —
/// the `.seg` files are the source of truth — so this fully self-heals a
/// torn or stale manifest.
fn rebuild_manifest(archive_dir: &Path, segments_dir: &Path) -> Result<()> {
    let mut segments = Vec::new();
    if let Ok(rd) = fs::read_dir(segments_dir) {
        for entry in rd.flatten() {
            let p = entry.path();
            if p.extension().and_then(|e| e.to_str()) == Some("seg") {
                if let Some(e) = read_segment_entry(&p)? {
                    segments.push(e);
                }
            }
        }
    }
    segments.sort_by(|a, b| a.start_gsn.cmp(&b.start_gsn).then(a.original.cmp(&b.original)));

    let manifest = Manifest { format_version: MANIFEST_VERSION, segments };
    let json = serde_json::to_vec_pretty(&manifest)
        .map_err(|e| Error::Io(std::io::Error::other(e)))?;

    let tmp = archive_dir.join(format!("{MANIFEST_FILE}.tmp"));
    {
        let mut f = File::create(&tmp)?;
        f.write_all(&json)?;
        f.sync_data()?;
    }
    fs::rename(&tmp, archive_dir.join(MANIFEST_FILE))?;
    if let Ok(dir) = File::open(archive_dir) {
        let _ = dir.sync_all();
    }
    Ok(())
}

/// Read and validate the trailer of an archived `.seg` file. Returns
/// `Ok(None)` for a file too short or with a bad magic (a torn or
/// non-`.seg` file) so the caller can skip it instead of failing.
fn read_segment_entry(seg_path: &Path) -> Result<Option<ManifestEntry>> {
    let segment = match seg_path.file_name() {
        Some(n) => n.to_string_lossy().into_owned(),
        None => return Ok(None),
    };
    let original = match segment.strip_suffix(".seg") {
        Some(o) => o.to_string(),
        None => return Ok(None),
    };
    let mut f = File::open(seg_path)?;
    let total = f.metadata()?.len();
    if total < TRAILER_SIZE {
        return Ok(None);
    }
    f.seek(SeekFrom::Start(total - TRAILER_SIZE))?;
    let mut buf = [0u8; TRAILER_SIZE as usize];
    f.read_exact(&mut buf)?;
    if u32::from_le_bytes(buf[52..56].try_into().unwrap()) != TRAILER_MAGIC {
        return Ok(None);
    }
    let u = |i: usize| u64::from_le_bytes(buf[i * 8..i * 8 + 8].try_into().unwrap());
    Ok(Some(ManifestEntry {
        segment,
        original,
        start_gsn: u(0),
        end_gsn: u(1),
        start_wall_clock: u(2),
        end_wall_clock: u(3),
        record_count: u(4),
        wal_byte_len: u(5),
        content_crc: u32::from_le_bytes(buf[48..52].try_into().unwrap()),
    }))
}

/// Every sealed segment in `data_dir` as `(name, path)` pairs. A sealed
/// segment is a `<something>.wal.<digits>` file.
fn list_sealed_segments(data_dir: &Path) -> Vec<(String, PathBuf)> {
    let mut out = Vec::new();
    if let Ok(rd) = fs::read_dir(data_dir) {
        for entry in rd.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if sealed_seq(&name).is_some() {
                out.push((name, entry.path()));
            }
        }
    }
    out
}

/// The seal sequence number if `name` is a sealed segment, else `None`.
fn sealed_seq(name: &str) -> Option<u64> {
    let (head, tail) = name.rsplit_once('.')?;
    let seq = tail.parse::<u64>().ok()?;
    head.ends_with(".wal").then_some(seq)
}

/// Min/max GSN and wall-clock over a segment's records. Zeroed metadata
/// (v1 records) is ignored; an all-v1 / empty segment yields all zeroes.
fn gsn_time_range(records: &[WalRecord]) -> (u64, u64, u64, u64) {
    let mut start_gsn = u64::MAX;
    let mut end_gsn = 0u64;
    let mut start_wc = u64::MAX;
    let mut end_wc = 0u64;
    for r in records {
        if r.meta.gsn != 0 {
            start_gsn = start_gsn.min(r.meta.gsn);
            end_gsn = end_gsn.max(r.meta.gsn);
        }
        if r.meta.wall_clock_micros != 0 {
            start_wc = start_wc.min(r.meta.wall_clock_micros);
            end_wc = end_wc.max(r.meta.wall_clock_micros);
        }
    }
    (
        if start_gsn == u64::MAX { 0 } else { start_gsn },
        end_gsn,
        if start_wc == u64::MAX { 0 } else { start_wc },
        end_wc,
    )
}

fn build_trailer(
    start_gsn: u64,
    end_gsn: u64,
    start_wc: u64,
    end_wc: u64,
    record_count: u64,
    wal_byte_len: u64,
    content_crc: u32,
) -> Vec<u8> {
    let mut t = Vec::with_capacity(TRAILER_SIZE as usize);
    for v in [start_gsn, end_gsn, start_wc, end_wc, record_count, wal_byte_len] {
        t.extend_from_slice(&v.to_le_bytes());
    }
    t.extend_from_slice(&content_crc.to_le_bytes());
    t.extend_from_slice(&TRAILER_MAGIC.to_le_bytes());
    t
}

fn crc32(data: &[u8]) -> u32 {
    let mut h = crc32fast::Hasher::new();
    h.update(data);
    h.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pitr::ArchiveSequencer;
    use crate::wal::WalEntry;
    use tempfile::TempDir;

    /// Build a sealed segment `<name>.wal.<seq>` in `dir` carrying `n`
    /// GSN-stamped inserts, returning its path.
    fn make_sealed_segment(dir: &Path, name: &str, n: u64) -> PathBuf {
        let seq = Arc::new(ArchiveSequencer::open(dir).unwrap());
        let wal = Wal::open(&dir.join(format!("{name}.wal")))
            .unwrap()
            .with_sequencer(Some(seq));
        for i in 0..n {
            wal.log(&WalEntry::insert(i, b"payload".to_vec())).unwrap();
        }
        wal.seal().unwrap();
        wal.list_sealed_segments().pop().unwrap()
    }

    #[test]
    fn archives_sealed_segments_and_builds_manifest() {
        let data = TempDir::new().unwrap();
        let archive = TempDir::new().unwrap();
        make_sealed_segment(data.path(), "users", 5);
        make_sealed_segment(data.path(), "posts", 3);

        let stats = archive_pass(data.path(), archive.path(), None).unwrap();
        assert_eq!(stats.archived, 2);
        assert_eq!(stats.skipped, 0);

        let manifest = load_manifest(archive.path()).unwrap();
        assert_eq!(manifest.format_version, MANIFEST_VERSION);
        assert_eq!(manifest.segments.len(), 2);
        let total: u64 = manifest.segments.iter().map(|s| s.record_count).sum();
        assert_eq!(total, 8);
        // Every entry carries a real GSN range from the stamped records.
        for s in &manifest.segments {
            assert!(s.start_gsn >= 1 && s.end_gsn >= s.start_gsn);
            assert!(s.content_crc != 0);
        }
    }

    #[test]
    fn archive_pass_is_idempotent() {
        let data = TempDir::new().unwrap();
        let archive = TempDir::new().unwrap();
        make_sealed_segment(data.path(), "c", 4);

        let first = archive_pass(data.path(), archive.path(), None).unwrap();
        assert_eq!(first.archived, 1);
        // Re-run: the segment is already archived, so it's skipped — archived
        // exactly once.
        let second = archive_pass(data.path(), archive.path(), None).unwrap();
        assert_eq!(second.archived, 0);
        assert_eq!(second.skipped, 1);
        assert_eq!(load_manifest(archive.path()).unwrap().segments.len(), 1);
    }

    #[test]
    fn manifest_self_heals_from_seg_trailers() {
        let data = TempDir::new().unwrap();
        let archive = TempDir::new().unwrap();
        make_sealed_segment(data.path(), "c", 6);
        archive_pass(data.path(), archive.path(), None).unwrap();

        // Corrupt the manifest entirely — the .seg files are the truth.
        fs::write(archive.path().join(MANIFEST_FILE), b"garbage not json").unwrap();
        // A fresh pass (nothing new to archive) must still rebuild it.
        let stats = archive_pass(data.path(), archive.path(), None).unwrap();
        assert_eq!(stats.archived, 0);
        let manifest = load_manifest(archive.path()).unwrap();
        assert_eq!(manifest.segments.len(), 1);
        assert_eq!(manifest.segments[0].record_count, 6);
    }

    #[test]
    fn reconcile_finishes_after_a_crash_mid_write() {
        let data = TempDir::new().unwrap();
        let archive = TempDir::new().unwrap();
        make_sealed_segment(data.path(), "c", 3);
        let segments_dir = archive.path().join(SEGMENTS_DIR);
        fs::create_dir_all(&segments_dir).unwrap();
        // Simulate a crash partway through a previous archive_one: a stray
        // .seg.tmp with no matching .seg.
        fs::write(segments_dir.join("c.wal.0.seg.tmp"), b"partial").unwrap();

        let stats = archive_pass(data.path(), archive.path(), None).unwrap();
        // The stray tmp is swept and the segment is archived cleanly.
        assert!(!segments_dir.join("c.wal.0.seg.tmp").exists());
        assert!(segments_dir.join("c.wal.0.seg").exists());
        assert_eq!(stats.archived, 1);
    }

    #[test]
    fn empty_sealed_segments_are_dropped_not_archived() {
        let data = TempDir::new().unwrap();
        let archive = TempDir::new().unwrap();
        // An explicit seal on an empty WAL leaves an empty sealed segment.
        let wal = Wal::open(&data.path().join("e.wal")).unwrap();
        wal.seal().unwrap();
        assert!(data.path().join("e.wal.0").exists());

        let stats = archive_pass(data.path(), archive.path(), None).unwrap();
        assert_eq!(stats.empty_removed, 1);
        assert_eq!(stats.archived, 0);
        assert!(!data.path().join("e.wal.0").exists());
        assert!(load_manifest(archive.path()).unwrap().segments.is_empty());
    }

    #[test]
    fn archived_segment_preserves_wal_bytes_verbatim() {
        let data = TempDir::new().unwrap();
        let archive = TempDir::new().unwrap();
        let seg_path = make_sealed_segment(data.path(), "c", 4);
        let original_bytes = fs::read(&seg_path).unwrap();

        archive_pass(data.path(), archive.path(), None).unwrap();
        let archived = fs::read(archive.path().join(SEGMENTS_DIR).join("c.wal.0.seg")).unwrap();

        // The archived file is the verbatim WAL bytes + a 56-byte trailer.
        assert_eq!(&archived[..original_bytes.len()], &original_bytes[..]);
        assert_eq!(archived.len() as u64, original_bytes.len() as u64 + TRAILER_SIZE);
        // ...and the trailer's wal_byte_len points exactly at the boundary.
        let entry = load_manifest(archive.path()).unwrap().segments.pop().unwrap();
        assert_eq!(entry.wal_byte_len, original_bytes.len() as u64);
    }
}
