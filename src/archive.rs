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

use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::crypto::EncryptionKey;
use crate::error::{Error, Result};
use crate::pitr::{BaseMeta, PitrTarget};
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
            // An empty sealed segment carries nothing. `.0` stays as the
            // scanner's sentinel (see `Wal::retire_segment`); the rest go.
            if sealed_seq(&name) != Some(0) {
                let _ = fs::remove_file(&path);
                stats.empty_removed += 1;
            }
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

    // Rebuild the manifest from the .seg trailers (self-healing index) —
    // but only when this pass changed something or the existing manifest
    // is missing/unreadable. The rebuild re-opens every archived segment
    // and rewrites + fsyncs manifest.json; doing that on every idle tick
    // made the archiver's steady-state cost O(archive size) forever.
    let manifest_ok = fs::read(archive_dir.join(MANIFEST_FILE))
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Manifest>(&bytes).ok())
        .is_some();
    if stats.archived > 0 || stats.empty_removed > 0 || !manifest_ok {
        rebuild_manifest(archive_dir, &segments_dir)?;
    }
    Ok(stats)
}

/// Load the archive manifest, or an empty one if it does not exist yet.
pub fn load_manifest(archive_dir: &Path) -> Result<Manifest> {
    match fs::read(archive_dir.join(MANIFEST_FILE)) {
        Ok(bytes) => {
            serde_json::from_slice(&bytes).map_err(|e| Error::Io(std::io::Error::other(e)))
        }
        Err(_) => Ok(Manifest {
            format_version: MANIFEST_VERSION,
            segments: Vec::new(),
        }),
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

/// Delete archived segments whose newest record is older than
/// `retention_hours`, then rebuild the manifest. A no-op when
/// `retention_hours` is 0 — retention is disabled by default.
///
/// Age-based only: the operator must keep a base backup at least as old
/// as the retention window, or PITR cannot restore through the pruned
/// range (the standard base-backup + WAL-retention operational contract).
///
/// When `data_dir` is given, the pruned segment's ORIGINAL sealed file
/// (`<collection>.wal.<seq>`) is deleted with it. Without this, the next
/// `archive_pass` finds the original, sees the `.seg` is gone, and
/// re-archives it — retention never freed space, it just set up an
/// archive→prune→re-archive I/O ping-pong every tick. Deleting the
/// original is safe at retention age: its records have long since been
/// persisted into the collection's snapshot (every open and background
/// sync does so), so the segment is not needed for crash recovery.
///
/// Returns the number of segments pruned.
pub fn prune_archive(
    archive_dir: &Path,
    data_dir: Option<&Path>,
    retention_hours: u64,
) -> Result<usize> {
    if retention_hours == 0 {
        return Ok(0);
    }
    let segments_dir = archive_dir.join(SEGMENTS_DIR);
    let cutoff = crate::pitr::now_micros().saturating_sub(
        retention_hours
            .saturating_mul(3600)
            .saturating_mul(1_000_000),
    );
    let mut pruned = 0;
    if let Ok(rd) = fs::read_dir(&segments_dir) {
        for entry in rd.flatten() {
            let p = entry.path();
            if p.extension().and_then(|e| e.to_str()) != Some("seg") {
                continue;
            }
            if let Some(meta) = read_segment_entry(&p)? {
                // end_wall_clock 0 means the segment carried no v2 records
                // (no timestamp to age it against) — keep it, to be safe.
                if meta.end_wall_clock != 0 && meta.end_wall_clock < cutoff {
                    let _ = fs::remove_file(&p);
                    // Drop the data-dir original too (see doc comment).
                    if let (Some(dd), Some(stem)) =
                        (data_dir, p.file_stem().and_then(|s| s.to_str()))
                    {
                        let original = dd.join(stem);
                        if original.exists() {
                            // `.0` is the segment scanner's sentinel:
                            // truncated, never removed (`Wal::retire_segment`).
                            let seq = sealed_seq(stem).unwrap_or(u64::MAX);
                            let _ = crate::wal::Wal::retire_segment(&original, seq);
                        }
                    }
                    pruned += 1;
                }
            }
        }
    }
    if pruned > 0 {
        rebuild_manifest(archive_dir, &segments_dir)?;
    }
    Ok(pruned)
}

/// A summary of the archive — for the `archive_status` server command.
#[derive(Debug, Clone, Serialize)]
pub struct ArchiveStatus {
    pub segment_count: usize,
    pub total_records: u64,
    /// Smallest / largest GSN covered by the archive (0 if empty).
    pub min_gsn: u64,
    pub max_gsn: u64,
    /// Smallest / largest wall-clock (micros) covered by the archive.
    pub min_wall_clock: u64,
    pub max_wall_clock: u64,
}

/// Summarize the archive at `archive_dir` from its manifest. Returns a
/// zeroed summary when there is no archive yet.
pub fn archive_status(archive_dir: &Path) -> Result<ArchiveStatus> {
    let manifest = load_manifest(archive_dir)?;
    let mut s = ArchiveStatus {
        segment_count: manifest.segments.len(),
        total_records: 0,
        min_gsn: u64::MAX,
        max_gsn: 0,
        min_wall_clock: u64::MAX,
        max_wall_clock: 0,
    };
    for e in &manifest.segments {
        s.total_records += e.record_count;
        if e.start_gsn != 0 {
            s.min_gsn = s.min_gsn.min(e.start_gsn);
        }
        s.max_gsn = s.max_gsn.max(e.end_gsn);
        if e.start_wall_clock != 0 {
            s.min_wall_clock = s.min_wall_clock.min(e.start_wall_clock);
        }
        s.max_wall_clock = s.max_wall_clock.max(e.end_wall_clock);
    }
    if s.min_gsn == u64::MAX {
        s.min_gsn = 0;
    }
    if s.min_wall_clock == u64::MAX {
        s.min_wall_clock = 0;
    }
    Ok(s)
}

/// What [`replay_into`] drove the restore to.
#[derive(Debug, Clone, Copy)]
pub struct ReplayOutcome {
    /// GSN the restore was driven to (inclusive).
    pub target_gsn: u64,
    /// Records applied to the restored WALs.
    pub records_applied: u64,
    /// Records skipped — beyond `target_gsn`, or excluded because their
    /// transaction straddled the cut.
    pub records_skipped: u64,
    /// Collections materialized.
    pub collections: usize,
}

/// Advance an already-extracted base backup in `target_dir` up to
/// `target` by replaying the archive on top of it.
///
/// The base backup, once extracted, holds `<collection>.btree` snapshots
/// plus `<collection>.wal` / `.wal.<seq>` files; the archive holds the
/// sealed segments since. This rewrites each collection's live WAL to
/// contain exactly the records that belong in the restored state — every
/// record with `gsn <= target_gsn`, minus any whose transaction straddles
/// the cut — and drops the stale index caches, the FTS index and the tx
/// commit log, so a plain `OxiDb::open(target_dir)` afterward yields the
/// point-in-time state. The `.btree` snapshots are taken as-is: they
/// predate the base watermark, so they are always within `target_gsn`.
///
/// Idempotent and offline — it never touches the source database.
pub fn replay_into(
    target_dir: &Path,
    archive_dir: &Path,
    target: PitrTarget,
    encryption: Option<&Arc<EncryptionKey>>,
) -> Result<ReplayOutcome> {
    // The base's GSN watermark — 0 / absent means the base predates PITR;
    // the restore then degrades to whatever the base + archive hold.
    let base_gsn = BaseMeta::read_from(target_dir)?
        .map(|m| m.base_gsn)
        .unwrap_or(0);

    // 1. Gather every WAL record per collection: from the base's own WAL
    //    files in target_dir, and from the archived sealed segments.
    let mut by_collection: HashMap<String, Vec<WalRecord>> = HashMap::new();

    // 1a. Base WAL files already in target_dir (live `.wal` + sealed `.wal.<seq>`).
    for (collection, path) in base_wal_files(target_dir) {
        let recs = Wal::open_with_encryption(&path, encryption.cloned())?.read_records()?;
        by_collection.entry(collection).or_default().extend(recs);
    }
    // Collections present only as a `.btree` snapshot (no WAL yet) still count.
    for collection in base_collection_names(target_dir) {
        by_collection.entry(collection).or_default();
    }
    // 1b. Archived sealed segments.
    let manifest = load_manifest(archive_dir)?;
    let segments_dir = archive_dir.join(SEGMENTS_DIR);
    for entry in &manifest.segments {
        let collection = match collection_of(&entry.original) {
            Some(c) => c,
            None => continue,
        };
        let seg_path = segments_dir.join(&entry.segment);
        let recs = Wal::open_with_encryption(&seg_path, encryption.cloned())?
            .read_records_prefix(entry.wal_byte_len)?;
        by_collection.entry(collection).or_default().extend(recs);
    }

    // 2. Resolve target_gsn from the full record set.
    let target_gsn = resolve_target_gsn(&by_collection, target);

    // Coverage check against the base watermark: the earliest stamped
    // record we can replay should not sit far above the base's GSN —
    // a gap means history between the base backup and the surviving
    // archive was pruned (or segments are missing), and the restore
    // would silently skip it. Advisory, not fatal: GSN sequences have
    // legitimate holes (the lease jumps ahead on every restart), so a
    // gap cannot be proven to contain records.
    if base_gsn > 0 {
        let min_stamped = by_collection
            .values()
            .flatten()
            .map(|r| r.meta.gsn)
            .filter(|&g| g != 0)
            .min();
        if let Some(min_gsn) = min_stamped
            && min_gsn > base_gsn + 1
            && target_gsn >= min_gsn
        {
            eprintln!(
                "[pitr] WARNING: base backup watermark is GSN {base_gsn} but the \
                     earliest replayable record is GSN {min_gsn} — history in between \
                     (if any) is not in the archive (pruned or missing segments); the \
                     restored state may silently skip it"
            );
        }
    }

    // 3. Two-pass transaction filter across ALL collections (a tx may span
    //    collections). Pass 1: the max GSN each tx touches. Pass 2 admits
    //    a tx only if its whole footprint fits under target_gsn — so a
    //    transaction straddling the cut is excluded entirely, never half-
    //    applied.
    let mut tx_max: HashMap<u64, u64> = HashMap::new();
    for recs in by_collection.values() {
        for r in recs {
            let tx = r.entry.tx_id();
            if tx != 0 {
                let e = tx_max.entry(tx).or_insert(0);
                *e = (*e).max(r.meta.gsn);
            }
        }
    }
    let admitted = |r: &WalRecord| -> bool {
        if r.meta.gsn > target_gsn {
            return false;
        }
        let tx = r.entry.tx_id();
        tx == 0 || tx_max.get(&tx).map(|&m| m <= target_gsn).unwrap_or(true)
    };

    // 4. Rewrite each collection's WAL with exactly the admitted records,
    //    in GSN order. The base's WAL files are removed first.
    let mut records_applied = 0u64;
    let mut records_skipped = 0u64;
    let collections = by_collection.len();
    for (collection, mut recs) in by_collection {
        recs.sort_by_key(|r| r.meta.gsn);
        let mut seen = HashSet::new();
        let mut included = Vec::new();
        for r in recs {
            if !admitted(&r) {
                records_skipped += 1;
                continue;
            }
            // The same record can appear in both a base WAL and an
            // archived segment — `(gsn, doc_id)` dedups them. GSN 0
            // (pre-PITR base records) cannot be deduped this way, so
            // those are kept unconditionally.
            if r.meta.gsn != 0 && !seen.insert((r.meta.gsn, r.entry.doc_id())) {
                continue;
            }
            included.push(r);
        }
        records_applied += included.len() as u64;

        remove_collection_wals(target_dir, &collection);
        let wal_path = target_dir.join(format!("{collection}.wal"));
        Wal::open_with_encryption(&wal_path, encryption.cloned())?.log_records(&included)?;
    }

    // 5. Drop derived/stale state so `OxiDb::open` rebuilds it cleanly.
    drop_derived_state(target_dir);

    Ok(ReplayOutcome {
        target_gsn,
        records_applied,
        records_skipped,
        collections,
    })
}

// -----------------------------------------------------------------------
// Internal helpers
// -----------------------------------------------------------------------

/// `(collection, path)` for every base WAL file in `dir` — the live
/// `<C>.wal` and any sealed `<C>.wal.<seq>` the base backup carried.
fn base_wal_files(dir: &Path) -> Vec<(String, PathBuf)> {
    let mut out = Vec::new();
    if let Ok(rd) = fs::read_dir(dir) {
        for entry in rd.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if let Some(c) = name.strip_suffix(".wal") {
                out.push((c.to_string(), entry.path()));
            } else if let Some(c) = collection_of(&name) {
                out.push((c, entry.path()));
            }
        }
    }
    out
}

/// Collection names that have a `.btree` snapshot in `dir`.
fn base_collection_names(dir: &Path) -> Vec<String> {
    let mut out = Vec::new();
    if let Ok(rd) = fs::read_dir(dir) {
        for entry in rd.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if let Some(c) = name.strip_suffix(".btree") {
                out.push(c.to_string());
            }
        }
    }
    out
}

/// The collection name from a sealed-segment name `<C>.wal.<seq>`.
fn collection_of(sealed_name: &str) -> Option<String> {
    let (head, tail) = sealed_name.rsplit_once('.')?;
    tail.parse::<u64>().ok()?; // `<seq>` must be numeric
    Some(head.strip_suffix(".wal")?.to_string())
}

/// Resolve a [`PitrTarget`] to a concrete GSN against the full record set.
fn resolve_target_gsn(by_collection: &HashMap<String, Vec<WalRecord>>, target: PitrTarget) -> u64 {
    match target {
        PitrTarget::Gsn(g) => g,
        PitrTarget::Latest => by_collection
            .values()
            .flatten()
            .map(|r| r.meta.gsn)
            .max()
            .unwrap_or(0),
        // Largest GSN whose record's wall-clock is at or before `t`.
        // Resolving by value (not position) tolerates a non-monotonic
        // wall-clock; records with no wall-clock (v1) never qualify.
        PitrTarget::Timestamp(t) => by_collection
            .values()
            .flatten()
            .filter(|r| r.meta.wall_clock_micros != 0 && r.meta.wall_clock_micros <= t)
            .map(|r| r.meta.gsn)
            .max()
            .unwrap_or(0),
    }
}

/// Remove `<collection>.wal` and every `<collection>.wal.<seq>` from `dir`.
fn remove_collection_wals(dir: &Path, collection: &str) {
    let _ = fs::remove_file(dir.join(format!("{collection}.wal")));
    let prefix = format!("{collection}.wal.");
    if let Ok(rd) = fs::read_dir(dir) {
        for entry in rd.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name
                .strip_prefix(&prefix)
                .and_then(|s| s.parse::<u64>().ok())
                .is_some()
            {
                let _ = fs::remove_file(entry.path());
            }
        }
    }
}

/// Drop the derived state the base brought along that no longer matches
/// the restored document set: index caches (`.fidx/.cidx/.vidx` — the
/// `.idx` *metadata* is kept, as it is the schema), the FTS index, the tx
/// commit log, and the stale copy of the source's archive. All of these
/// rebuild (or stay empty) on the next `OxiDb::open`.
fn drop_derived_state(dir: &Path) {
    if let Ok(rd) = fs::read_dir(dir) {
        for entry in rd.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.ends_with(".fidx") || name.ends_with(".cidx") || name.ends_with(".vidx") {
                let _ = fs::remove_file(entry.path());
            }
        }
    }
    let _ = fs::remove_dir_all(dir.join("_fts"));
    let _ = fs::remove_file(dir.join("_tx_commit_log"));
    let _ = fs::remove_dir_all(dir.join(DEFAULT_ARCHIVE_SUBDIR));
}

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
        start_gsn,
        end_gsn,
        start_wc,
        end_wc,
        records.len() as u64,
        wal_bytes.len() as u64,
        content_crc,
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
            if p.extension().and_then(|e| e.to_str()) == Some("seg")
                && let Some(e) = read_segment_entry(&p)?
            {
                segments.push(e);
            }
        }
    }
    segments.sort_by(|a, b| {
        a.start_gsn
            .cmp(&b.start_gsn)
            .then(a.original.cmp(&b.original))
    });

    let manifest = Manifest {
        format_version: MANIFEST_VERSION,
        segments,
    };
    let json =
        serde_json::to_vec_pretty(&manifest).map_err(|e| Error::Io(std::io::Error::other(e)))?;

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
    for v in [
        start_gsn,
        end_gsn,
        start_wc,
        end_wc,
        record_count,
        wal_byte_len,
    ] {
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
    use crate::wal::{WalEntry, WalMeta};
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
        // Two explicit seals on an empty WAL leave two empty sealed segments.
        let wal = Wal::open(&data.path().join("e.wal")).unwrap();
        wal.seal().unwrap();
        wal.seal().unwrap();
        assert!(data.path().join("e.wal.0").exists());
        assert!(data.path().join("e.wal.1").exists());

        let stats = archive_pass(data.path(), archive.path(), None).unwrap();
        // `.1` is dropped; `.0` survives EMPTY — it is the segment scanner's
        // fast-path sentinel (`Wal::retire_segment`), and an empty `.0` is
        // never archived, so it cannot ping-pong back into the manifest.
        assert_eq!(stats.empty_removed, 1);
        assert_eq!(stats.archived, 0);
        assert!(data.path().join("e.wal.0").exists());
        assert_eq!(
            std::fs::metadata(data.path().join("e.wal.0"))
                .unwrap()
                .len(),
            0
        );
        assert!(!data.path().join("e.wal.1").exists());
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
        assert_eq!(
            archived.len() as u64,
            original_bytes.len() as u64 + TRAILER_SIZE
        );
        // ...and the trailer's wal_byte_len points exactly at the boundary.
        let entry = load_manifest(archive.path())
            .unwrap()
            .segments
            .pop()
            .unwrap();
        assert_eq!(entry.wal_byte_len, original_bytes.len() as u64);
    }

    // ── PITR Phase 5: replay_into (restore_to_point core) ───────────────

    /// GSNs in the rewritten `c.wal` of `target_dir`, sorted.
    fn restored_gsns(target_dir: &Path) -> Vec<u64> {
        let mut g: Vec<u64> = Wal::open(&target_dir.join("c.wal"))
            .unwrap()
            .read_records()
            .unwrap()
            .iter()
            .map(|r| r.meta.gsn)
            .collect();
        g.sort_unstable();
        g
    }

    /// A `target_dir` standing in for an extracted base backup: a
    /// `base.meta` watermark and an (empty) `c.wal` so the collection is
    /// discovered.
    fn fake_extracted_base(target_dir: &Path, base_gsn: u64) {
        BaseMeta::new(base_gsn).write_to(target_dir).unwrap();
        Wal::open(&target_dir.join("c.wal")).unwrap();
    }

    #[test]
    fn replay_to_gsn_filters_the_record_set() {
        let target = TempDir::new().unwrap();
        let src = TempDir::new().unwrap();
        let archive = TempDir::new().unwrap();

        // Archive: one sealed segment, GSNs 1..=10 for collection "c".
        let seq = Arc::new(ArchiveSequencer::open(src.path()).unwrap());
        let wal = Wal::open(&src.path().join("c.wal"))
            .unwrap()
            .with_sequencer(Some(seq));
        for i in 1..=10u64 {
            wal.log(&WalEntry::insert(i, b"x".to_vec())).unwrap();
        }
        wal.seal().unwrap();
        archive_pass(src.path(), archive.path(), None).unwrap();

        fake_extracted_base(target.path(), 1);
        let outcome = replay_into(target.path(), archive.path(), PitrTarget::Gsn(6), None).unwrap();
        assert_eq!(outcome.target_gsn, 6);
        assert_eq!(outcome.records_applied, 6);
        assert_eq!(restored_gsns(target.path()), vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn replay_latest_includes_every_archived_record() {
        let target = TempDir::new().unwrap();
        let src = TempDir::new().unwrap();
        let archive = TempDir::new().unwrap();

        let seq = Arc::new(ArchiveSequencer::open(src.path()).unwrap());
        let wal = Wal::open(&src.path().join("c.wal"))
            .unwrap()
            .with_sequencer(Some(seq));
        for i in 1..=7u64 {
            wal.log(&WalEntry::insert(i, b"x".to_vec())).unwrap();
        }
        wal.seal().unwrap();
        archive_pass(src.path(), archive.path(), None).unwrap();

        fake_extracted_base(target.path(), 1);
        let outcome = replay_into(target.path(), archive.path(), PitrTarget::Latest, None).unwrap();
        assert_eq!(outcome.target_gsn, 7);
        assert_eq!(restored_gsns(target.path()), vec![1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn replay_excludes_a_transaction_straddling_the_cut() {
        let target = TempDir::new().unwrap();
        let src = TempDir::new().unwrap();
        let archive = TempDir::new().unwrap();

        // GSN 1-5 non-transactional, 6 & 7 belong to tx 99, 8 non-transactional.
        let seq = Arc::new(ArchiveSequencer::open(src.path()).unwrap());
        let wal = Wal::open(&src.path().join("c.wal"))
            .unwrap()
            .with_sequencer(Some(seq));
        for i in 1..=5u64 {
            wal.log(&WalEntry::insert(i, b"x".to_vec())).unwrap();
        }
        wal.log(&WalEntry::Insert {
            doc_id: 6,
            doc_bytes: b"x".to_vec(),
            tx_id: 99,
        })
        .unwrap();
        wal.log(&WalEntry::Insert {
            doc_id: 7,
            doc_bytes: b"x".to_vec(),
            tx_id: 99,
        })
        .unwrap();
        wal.log(&WalEntry::insert(8, b"x".to_vec())).unwrap();
        wal.seal().unwrap();
        archive_pass(src.path(), archive.path(), None).unwrap();

        fake_extracted_base(target.path(), 1);
        // Cut at GSN 6: tx 99 reaches GSN 7 > 6, so the whole tx is dropped —
        // GSN 6 is excluded even though 6 <= 6.
        let outcome = replay_into(target.path(), archive.path(), PitrTarget::Gsn(6), None).unwrap();
        assert_eq!(restored_gsns(target.path()), vec![1, 2, 3, 4, 5]);
        assert!(outcome.records_skipped >= 1);
    }

    #[test]
    fn replay_timestamp_resolves_by_wall_clock_and_drops_derived_state() {
        let target = TempDir::new().unwrap();
        let src = TempDir::new().unwrap();
        let archive = TempDir::new().unwrap();

        // Explicit wall-clocks: GSN i carries wall-clock i seconds.
        let wal = Wal::open(&src.path().join("c.wal")).unwrap();
        for i in 1..=5u64 {
            wal.log_with_meta(
                &WalEntry::insert(i, b"x".to_vec()),
                WalMeta {
                    gsn: i,
                    wall_clock_micros: i * 1_000_000,
                },
            )
            .unwrap();
        }
        wal.seal().unwrap();
        archive_pass(src.path(), archive.path(), None).unwrap();

        fake_extracted_base(target.path(), 1);
        // Stale derived state the base would have carried along.
        fs::write(target.path().join("c.fidx"), b"stale index cache").unwrap();
        fs::write(target.path().join("_tx_commit_log"), b"stale").unwrap();

        let outcome = replay_into(
            target.path(),
            archive.path(),
            PitrTarget::Timestamp(3_000_000),
            None,
        )
        .unwrap();
        assert_eq!(outcome.target_gsn, 3);
        assert_eq!(restored_gsns(target.path()), vec![1, 2, 3]);
        // Derived state is dropped so OxiDb::open rebuilds it cleanly.
        assert!(!target.path().join("c.fidx").exists());
        assert!(!target.path().join("_tx_commit_log").exists());
    }

    // ── PITR Phase 6: retention + status ────────────────────────────────

    #[test]
    fn prune_archive_drops_old_segments_by_age() {
        let data = TempDir::new().unwrap();
        let archive = TempDir::new().unwrap();
        // One segment dated to 1970, one dated to now.
        let old = Wal::open(&data.path().join("old.wal")).unwrap();
        old.log_with_meta(
            &WalEntry::insert(1, b"x".to_vec()),
            WalMeta {
                gsn: 1,
                wall_clock_micros: 1_000_000,
            },
        )
        .unwrap();
        old.seal().unwrap();
        let recent = Wal::open(&data.path().join("recent.wal")).unwrap();
        recent
            .log_with_meta(
                &WalEntry::insert(2, b"x".to_vec()),
                WalMeta {
                    gsn: 2,
                    wall_clock_micros: crate::pitr::now_micros(),
                },
            )
            .unwrap();
        recent.seal().unwrap();
        archive_pass(data.path(), archive.path(), None).unwrap();
        assert_eq!(load_manifest(archive.path()).unwrap().segments.len(), 2);

        // Retention 0 is disabled — a no-op.
        assert_eq!(
            prune_archive(archive.path(), Some(data.path()), 0).unwrap(),
            0
        );
        // Retention 1h prunes the 1970 segment, keeps the recent one.
        assert_eq!(
            prune_archive(archive.path(), Some(data.path()), 1).unwrap(),
            1
        );
        let m = load_manifest(archive.path()).unwrap();
        assert_eq!(m.segments.len(), 1);
        assert_eq!(m.segments[0].original, "recent.wal.0");

        // The pruned segment's data-dir original must not be re-archivable —
        // otherwise the next archive_pass would undo the prune every tick.
        // Seq 0 is truncated to the empty scanner sentinel rather than
        // removed (`Wal::retire_segment`); empty segments never archive.
        assert_eq!(
            std::fs::metadata(data.path().join("old.wal.0"))
                .unwrap()
                .len(),
            0,
            "the pruned original must be the empty sentinel"
        );
        archive_pass(data.path(), archive.path(), None).unwrap();
        assert_eq!(
            load_manifest(archive.path()).unwrap().segments.len(),
            1,
            "pruned segment must not be resurrected by the next pass"
        );
    }

    #[test]
    fn archive_status_summarizes_the_manifest() {
        let data = TempDir::new().unwrap();
        let archive = TempDir::new().unwrap();
        make_sealed_segment(data.path(), "c", 5);
        archive_pass(data.path(), archive.path(), None).unwrap();

        let s = archive_status(archive.path()).unwrap();
        assert_eq!(s.segment_count, 1);
        assert_eq!(s.total_records, 5);
        assert!(s.min_gsn >= 1 && s.max_gsn >= s.min_gsn);

        // A never-archived directory yields a zeroed summary.
        let empty = TempDir::new().unwrap();
        let z = archive_status(empty.path()).unwrap();
        assert_eq!(z.segment_count, 0);
        assert_eq!(z.total_records, 0);
    }
}
