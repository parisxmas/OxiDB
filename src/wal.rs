use crate::document::DocumentId;

/// Durability fault injection for tests (fsyncgate-class coverage).
/// A single relaxed atomic load per fsync when disarmed — negligible,
/// and OFF by default. Lets a test force the WAL's next fsync to return
/// EIO so we can prove a commit whose fsync failed is NOT acknowledged.
pub mod fault {
    use std::sync::atomic::{AtomicU64, Ordering};

    // 0 = disarmed. N>0 = fail the Nth fsync counted from arming, then
    // auto-disarm.
    static FAIL_AT: AtomicU64 = AtomicU64::new(0);
    static COUNT: AtomicU64 = AtomicU64::new(0);

    /// Arm: the `n`-th fsync from now returns an injected error, then the
    /// fault disarms itself. `n == 1` = the very next fsync.
    pub fn fail_fsync_at(n: u64) {
        COUNT.store(0, Ordering::SeqCst);
        FAIL_AT.store(n, Ordering::SeqCst);
    }

    /// Disarm any pending fault.
    pub fn disarm() {
        FAIL_AT.store(0, Ordering::SeqCst);
    }

    /// Returns true if THIS fsync should fail (and disarms if so).
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn should_fail() -> bool {
        let target = FAIL_AT.load(Ordering::SeqCst);
        if target == 0 {
            return false;
        }
        let n = COUNT.fetch_add(1, Ordering::SeqCst) + 1;
        if n >= target {
            FAIL_AT.store(0, Ordering::SeqCst);
            true
        } else {
            false
        }
    }
}

/// fsync a WAL file, honoring an armed durability fault (see `fault`).
#[cfg(not(target_arch = "wasm32"))]
fn fsync_file(file: &File) -> std::io::Result<()> {
    if fault::should_fail() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "injected fsync failure (EIO)",
        ));
    }
    file.sync_data()
}

#[cfg(not(target_arch = "wasm32"))]
use crate::locks::Mutex;
#[cfg(not(target_arch = "wasm32"))]
use std::borrow::Cow;
#[cfg(not(target_arch = "wasm32"))]
use std::collections::{HashMap, HashSet};
#[cfg(not(target_arch = "wasm32"))]
use std::fs::{self, File, OpenOptions};
#[cfg(not(target_arch = "wasm32"))]
use std::io::{Read, Seek, SeekFrom, Write};
#[cfg(not(target_arch = "wasm32"))]
use std::path::{Path, PathBuf};
#[cfg(not(target_arch = "wasm32"))]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;

#[cfg(not(target_arch = "wasm32"))]
use crc32fast::Hasher;

#[cfg(not(target_arch = "wasm32"))]
use crate::crypto::EncryptionKey;
#[cfg(not(target_arch = "wasm32"))]
use crate::doc_cache::DocCache;
#[cfg(not(target_arch = "wasm32"))]
use crate::engine::LogCallback;
#[cfg(not(target_arch = "wasm32"))]
use crate::error::{Error, Result};
#[cfg(not(target_arch = "wasm32"))]
use crate::index::CompositeIndex;
#[cfg(not(target_arch = "wasm32"))]
use crate::paged_field_index::PagedFieldIndex;
#[cfg(not(target_arch = "wasm32"))]
use crate::pitr::ArchiveSequencer;
#[cfg(not(target_arch = "wasm32"))]
use crate::storage::{DocLocation, Storage};

const OP_INSERT: u8 = 1;
const OP_UPDATE: u8 = 2;
const OP_DELETE: u8 = 3;

// v2 (PITR) record op-types. Same layout as v1 but with an extended
// header — `[gsn: u64 LE][wall_clock_micros: u64 LE]` — inserted
// between `doc_id` and `doc_bytes`. The high bit distinguishes them so
// `read_entries` can replay a file containing a mix of v1 and v2
// records (e.g. written across an upgrade) with no separate format
// flag. v1 records are still emitted unless an archive sequencer is
// attached, so a PITR-disabled database is byte-identical to before.
const OP_INSERT_V2: u8 = 0x81;
const OP_UPDATE_V2: u8 = 0x82;
const OP_DELETE_V2: u8 = 0x83;

// ── File-level header (Phase 1b of ADR-0003 / docs/format/wal.md) ──
//
// Layout (8 bytes, little-endian):
//   [b"OXWA" (4)][version u16][flags u16]
//
// Sits at the very start of a WAL file written by a current engine. The
// reader accepts both v1 and legacy header-less files (detected by the
// absence of `OXWA` magic at offset 0). The writer writes the header
// only when starting from an empty file — legacy files are never
// retro-prepended; on the next `seal()` rotation the new live WAL
// starts empty and the next append writes the header. Recognising a
// version we don't know is a hard error, not a silent misread.

#[cfg(not(target_arch = "wasm32"))]
const WAL_MAGIC: &[u8; 4] = b"OXWA";
#[cfg(not(target_arch = "wasm32"))]
const WAL_VERSION: u16 = 1;
#[cfg(not(target_arch = "wasm32"))]
const WAL_HEADER_SIZE: usize = 8;

#[cfg(not(target_arch = "wasm32"))]
mod header_state {
    /// File is currently empty — the header writes on the first append.
    pub const NEEDED: u8 = 0;
    /// File already starts with `OXWA`; records start at offset 8.
    pub const PRESENT: u8 = 1;
    /// File is a pre-Phase-1b legacy file (no magic). Records start at
    /// offset 0; the engine never retro-prepends a header.
    pub const LEGACY: u8 = 2;
}

/// A WAL entry representing a pending mutation.
pub enum WalEntry {
    Insert {
        doc_id: DocumentId,
        doc_bytes: Vec<u8>,
        tx_id: u64,
    },
    Update {
        doc_id: DocumentId,
        doc_bytes: Vec<u8>,
        tx_id: u64,
    },
    Delete {
        doc_id: DocumentId,
        tx_id: u64,
    },
}

impl WalEntry {
    /// Create an Insert entry with tx_id=0 (non-transactional).
    pub fn insert(doc_id: DocumentId, doc_bytes: Vec<u8>) -> Self {
        WalEntry::Insert {
            doc_id,
            doc_bytes,
            tx_id: 0,
        }
    }

    /// Create an Update entry with tx_id=0 (non-transactional).
    pub fn update(doc_id: DocumentId, doc_bytes: Vec<u8>) -> Self {
        WalEntry::Update {
            doc_id,
            doc_bytes,
            tx_id: 0,
        }
    }

    /// Create a Delete entry with tx_id=0 (non-transactional).
    pub fn delete(doc_id: DocumentId) -> Self {
        WalEntry::Delete { doc_id, tx_id: 0 }
    }

    pub fn tx_id(&self) -> u64 {
        match self {
            WalEntry::Insert { tx_id, .. } => *tx_id,
            WalEntry::Update { tx_id, .. } => *tx_id,
            WalEntry::Delete { tx_id, .. } => *tx_id,
        }
    }

    /// The document this entry mutates.
    pub fn doc_id(&self) -> DocumentId {
        match self {
            WalEntry::Insert { doc_id, .. } => *doc_id,
            WalEntry::Update { doc_id, .. } => *doc_id,
            WalEntry::Delete { doc_id, .. } => *doc_id,
        }
    }
}

/// Per-record metadata carried only by v2 (PITR) WAL records. v1 records
/// parse back as `WalMeta::default()` (all zeroes), so `gsn == 0` means
/// "this record predates PITR / was written with PITR disabled".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WalMeta {
    /// Global Sequence Number — monotonic across every collection's WAL.
    /// Assigned by the archive sequencer (Phase 1). 0 in v1 records.
    pub gsn: u64,
    /// Wall-clock of the write, microseconds since the Unix epoch.
    /// 0 in v1 records.
    pub wall_clock_micros: u64,
}

/// A WAL entry together with its v2 metadata. Returned by `read_records`
/// for callers that need the GSN/timestamp (the archiver and the PITR
/// replay tool); `read_entries` drops the meta for the existing replay
/// paths that don't need it.
pub struct WalRecord {
    pub entry: WalEntry,
    pub meta: WalMeta,
}

#[cfg(not(target_arch = "wasm32"))]
pub struct Wal {
    inner: Mutex<File>,
    path: PathBuf,
    encryption: Option<Arc<EncryptionKey>>,
    /// Archive sequencer for PITR. When `Some`, every `log*` call stamps
    /// its records with a GSN + wall-clock and emits the v2 format;
    /// when `None` (PITR disabled — the default) records stay v1.
    sequencer: Option<Arc<ArchiveSequencer>>,
    /// Next sequence number for a sealed segment (`<path>.<seq>`). Seeded
    /// past the highest sealed segment already on disk so seals never
    /// collide across restarts. Only advanced under the `inner` lock.
    next_seal_seq: AtomicU64,
    /// One of `header_state::{NEEDED,PRESENT,LEGACY}`. Decides whether
    /// the next append prepends the OXWA header and whether the scanner
    /// skips the first 8 bytes. Only mutated under the `inner` lock,
    /// but exposed as an atomic so read-only paths (like `scan`) can
    /// observe it without taking the lock twice.
    header_state: std::sync::atomic::AtomicU8,
    /// Monotonic count of completed append calls, bumped under the
    /// `inner` lock. Together with `synced_seq` this powers
    /// [`Wal::sync_shared`]: an append is durable iff its sequence is
    /// ≤ `synced_seq`.
    append_seq: AtomicU64,
    /// Highest `append_seq` known to be covered by an fsync.
    synced_seq: AtomicU64,
    /// Leadership lock for [`Wal::sync_shared`] — the holder performs one
    /// fsync on behalf of every waiter whose appends predate it (group
    /// commit). Deliberately separate from `inner` so appends proceed
    /// while an fsync is in flight.
    sync_lock: Mutex<()>,
}

#[cfg(not(target_arch = "wasm32"))]
impl Wal {
    /// Open or create a WAL file.
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_with_encryption(path, None)
    }

    pub fn open_with_encryption(
        path: &Path,
        encryption: Option<Arc<EncryptionKey>>,
    ) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)?;

        let next_seal_seq = Self::scan_max_seal_seq(path);
        let header_state = Self::detect_header_state(&mut file, path)?;

        Ok(Self {
            inner: Mutex::new(file),
            path: path.to_path_buf(),
            encryption,
            sequencer: None,
            next_seal_seq: AtomicU64::new(next_seal_seq),
            header_state: std::sync::atomic::AtomicU8::new(header_state),
            append_seq: AtomicU64::new(0),
            synced_seq: AtomicU64::new(0),
            sync_lock: Mutex::new(()),
        })
    }

    /// Decide whether the file has an OXWA header, is legacy, or is empty
    /// (header pending). Called once at open time; the result is stored
    /// in `self.header_state` and updated under the `inner` lock when an
    /// empty file gets its first append or `seal` rotates a fresh empty
    /// file in.
    fn detect_header_state(file: &mut File, path: &Path) -> Result<u8> {
        let file_len = file.metadata()?.len();
        if file_len == 0 {
            return Ok(header_state::NEEDED);
        }
        if file_len < WAL_HEADER_SIZE as u64 {
            // 1-7 bytes — corrupt / truncated header. Treat as legacy so the
            // scanner's per-record CRC catches the torn tail.
            return Ok(header_state::LEGACY);
        }
        let mut probe = [0u8; WAL_HEADER_SIZE];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut probe)?;
        if &probe[0..4] != WAL_MAGIC {
            return Ok(header_state::LEGACY);
        }
        let version = u16::from_le_bytes([probe[4], probe[5]]);
        if version != WAL_VERSION {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "unsupported WAL format version {} at {}; this binary understands version {}",
                    version,
                    path.display(),
                    WAL_VERSION
                ),
            )));
        }
        // probe[6..8] = flags; reserved at 0 — ignored until a bit is
        // documented + assigned.
        Ok(header_state::PRESENT)
    }

    /// Write the OXWA header at offset 0 if the current state is `NEEDED`.
    /// Assumes the caller already holds the `inner` lock and that `file`
    /// is the live WAL file (not a sealed segment). After this returns,
    /// `header_state` is `PRESENT` and the file cursor is left at offset 8.
    fn ensure_header_locked(&self, file: &mut File) -> Result<()> {
        if self.header_state.load(std::sync::atomic::Ordering::Acquire) != header_state::NEEDED {
            return Ok(());
        }
        let mut header = Vec::with_capacity(WAL_HEADER_SIZE);
        header.extend_from_slice(WAL_MAGIC);
        header.extend_from_slice(&WAL_VERSION.to_le_bytes());
        header.extend_from_slice(&0u16.to_le_bytes()); // flags reserved
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&header)?;
        self.header_state
            .store(header_state::PRESENT, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// Byte offset of the first record in the live file — 8 for OXWA-
    /// headed files, 0 for legacy / brand-new files. Used by the scanner
    /// to skip the header when present without re-probing the bytes.
    fn records_start_offset(&self) -> u64 {
        if self.header_state.load(std::sync::atomic::Ordering::Acquire) == header_state::PRESENT {
            WAL_HEADER_SIZE as u64
        } else {
            0
        }
    }

    /// Attach (or detach) the PITR archive sequencer. Builder-style so the
    /// `open*` signatures stay stable. With a sequencer attached every
    /// subsequent `log*` call emits v2 records stamped with a GSN +
    /// wall-clock; without one, records stay v1 (the default).
    pub fn with_sequencer(mut self, sequencer: Option<Arc<ArchiveSequencer>>) -> Self {
        self.sequencer = sequencer;
        self
    }

    /// Serialize and append a WAL entry, then fsync. Emits a v2 record
    /// (GSN + wall-clock stamped) when an archive sequencer is attached.
    pub fn log(&self, entry: &WalEntry) -> Result<()> {
        self.append_entries_locked(std::slice::from_ref(entry), true)
    }

    /// Append a single record stamped with an explicit `meta` (v2 format),
    /// then fsync — bypassing the attached sequencer. Mainly for tests and
    /// for callers that supply their own GSN.
    pub fn log_with_meta(&self, entry: &WalEntry, meta: WalMeta) -> Result<()> {
        let rec = Self::frame(self.serialize_entry(entry, Some(meta))?);
        let mut file = self.inner.lock();
        self.ensure_header_locked(&mut file)?;
        file.seek(SeekFrom::End(0))?;
        file.write_all(&rec)?;
        fsync_file(&file)?;
        self.maybe_seal_locked(&mut file)?;
        Ok(())
    }

    /// Serialize and append a WAL entry without fsync.
    pub fn log_no_sync(&self, entry: &WalEntry) -> Result<()> {
        self.append_entries_locked(std::slice::from_ref(entry), false)
    }

    /// Write multiple WAL entries with a single fsync.
    pub fn log_batch(&self, entries: &[WalEntry]) -> Result<()> {
        self.append_entries_locked(entries, true)
    }

    /// Write multiple WAL entries without fsync.
    pub fn log_batch_no_sync(&self, entries: &[WalEntry]) -> Result<()> {
        self.append_entries_locked(entries, false)
    }

    /// Write multiple insert entries without fsync, avoiding doc_bytes clones.
    pub fn log_batch_inserts_no_sync(&self, entries: &[(u64, &[u8])]) -> Result<()> {
        self.log_batch_inserts_no_sync_buffered(entries)
    }

    /// Write multiple insert entries without fsync using a single write_all call.
    /// Builds the entire batch into one buffer, reducing syscalls from 3*N to 1.
    pub fn log_batch_inserts_no_sync_buffered(&self, entries: &[(u64, &[u8])]) -> Result<()> {
        self.append_inserts_locked(entries, false)
    }

    /// Acquire the WAL lock, then for each entry allocate its GSN (when a
    /// sequencer is attached), encode it, and append — optionally fsync,
    /// then seal if the segment passed its size threshold.
    ///
    /// Allocating the GSN *inside* the lock is what makes the base-backup
    /// watermark sound: once `ArchiveSequencer::current_gsn()` has moved
    /// past `N`, GSN `N`'s record is already in this file (or being
    /// written by the thread holding this lock). So a [`Wal::barrier`]
    /// after reading the counter guarantees a subsequent tar sees every
    /// record below the watermark — no encode/append gap to race.
    fn append_entries_locked(&self, entries: &[WalEntry], sync: bool) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut file = self.inner.lock();
        self.ensure_header_locked(&mut file)?;
        let mut buf = Vec::new();
        for entry in entries {
            let meta = match &self.sequencer {
                Some(seq) => Some(seq.next()?),
                None => None,
            };
            buf.extend_from_slice(&Self::frame(self.serialize_entry(entry, meta)?));
        }
        file.seek(SeekFrom::End(0))?;
        file.write_all(&buf)?;
        let seq = self.append_seq.fetch_add(1, Ordering::SeqCst) + 1;
        if sync {
            fsync_file(&file)?;
            self.synced_seq.fetch_max(seq, Ordering::SeqCst);
        }
        self.maybe_seal_locked(&mut file)?;
        Ok(())
    }

    /// Like [`Wal::append_entries_locked`] but for the no-clone bulk-insert
    /// path — GSN allocation still happens inside the lock.
    fn append_inserts_locked(&self, entries: &[(u64, &[u8])], sync: bool) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut file = self.inner.lock();
        self.ensure_header_locked(&mut file)?;
        let mut buf = Vec::new();
        for &(doc_id, doc_bytes) in entries {
            buf.extend_from_slice(&self.encode_insert_record(doc_id, doc_bytes)?);
        }
        file.seek(SeekFrom::End(0))?;
        file.write_all(&buf)?;
        let seq = self.append_seq.fetch_add(1, Ordering::SeqCst) + 1;
        if sync {
            fsync_file(&file)?;
            self.synced_seq.fetch_max(seq, Ordering::SeqCst);
        }
        self.maybe_seal_locked(&mut file)?;
        Ok(())
    }

    /// Acquire and immediately release the WAL lock — a write barrier.
    /// Once this returns, every writer that had allocated a GSN before
    /// the call has finished appending its record to the file. Base
    /// backup relies on this; see [`Wal::append_entries_locked`].
    pub fn barrier(&self) {
        let _guard = self.inner.lock();
    }

    /// Seal the current segment now, regardless of size: atomically rename
    /// the live WAL to `<path>.<seq>` and start a fresh empty one. Holds
    /// the `inner` lock for the whole rotation, so no acknowledged write
    /// can fall between the sealed segment and the new live WAL.
    /// Bytes in the live WAL. What the online checkpoint watches: this is the
    /// number that used to only ever go up.
    pub fn size_bytes(&self) -> u64 {
        let file = self.inner.lock();
        file.metadata().map(|m| m.len()).unwrap_or(0)
    }

    /// Whether an archive sequencer is attached (PITR on). When it is, sealed
    /// segments belong to the archiver — it copies them to `_archive/` and
    /// removes them — so nobody else may delete them.
    pub fn pitr_enabled(&self) -> bool {
        self.sequencer.is_some()
    }

    pub fn seal(&self) -> Result<()> {
        let mut file = self.inner.lock();
        self.seal_locked(&mut file)
    }

    /// Every sealed segment for this WAL (`<path>.<seq>`), sorted oldest
    /// first. The archiver (Phase 3) consumes these; the live WAL and any
    /// non-numeric sibling files are excluded.
    pub fn list_sealed_segments(&self) -> Vec<PathBuf> {
        let mut segs = Self::scan_sealed_segments(&self.path);
        segs.sort_by_key(|(seq, _)| *seq);
        segs.into_iter().map(|(_, p)| p).collect()
    }

    /// fsync the WAL file without writing anything else.
    /// Used after the *_no_sync batch paths when the caller wants to
    /// finalize durability for a group of writes.
    pub fn sync(&self) -> Result<()> {
        let seq = self.append_seq.load(Ordering::SeqCst);
        let file = self.inner.lock();
        fsync_file(&file)?;
        self.synced_seq.fetch_max(seq, Ordering::SeqCst);
        Ok(())
    }

    /// Group-commit fsync: make every append that completed before this
    /// call durable, sharing the physical fsync with every concurrent
    /// caller. The fast path returns without any syscall when a
    /// concurrent leader's fsync already covered this caller's appends.
    ///
    /// The leader clones the file handle under the `inner` lock (so the
    /// covered-sequence snapshot is consistent and a concurrent `seal`
    /// can't swap the file mid-snapshot) but performs the fsync *outside*
    /// it — appends keep flowing while the disk flush is in flight, which
    /// is what lets a batch build up for the next round.
    pub fn sync_shared(&self) -> Result<()> {
        let target = self.append_seq.load(Ordering::SeqCst);
        if self.synced_seq.load(Ordering::SeqCst) >= target {
            return Ok(());
        }
        let _lead = self.sync_lock.lock();
        if self.synced_seq.load(Ordering::SeqCst) >= target {
            // A previous leader's fsync covered us while we waited.
            return Ok(());
        }
        let (dup, covered) = {
            let file = self.inner.lock();
            (file.try_clone()?, self.append_seq.load(Ordering::SeqCst))
        };
        fsync_file(&dup)?;
        self.synced_seq.fetch_max(covered, Ordering::SeqCst);
        Ok(())
    }

    /// Truncate the WAL to 0 (checkpoint), then fsync.
    pub fn checkpoint(&self) -> Result<()> {
        let file = self.inner.lock();
        file.set_len(0)?;
        fsync_file(&file)?;
        // Truncation discards every pending append — nothing left to sync.
        self.synced_seq
            .fetch_max(self.append_seq.load(Ordering::SeqCst), Ordering::SeqCst);
        // The truncate removed the OXWA header along with the records; the
        // next append must rewrite it. Leaving the state PRESENT made the
        // first post-checkpoint record land at offset 0 while in-process
        // readers still skipped 8 header bytes — and silently degraded the
        // file to the legacy header-less format on every checkpoint+append
        // cycle. Store while still holding the inner lock so it can't race
        // a concurrent append's `ensure_header_locked`.
        self.header_state
            .store(header_state::NEEDED, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// Truncate the WAL to 0 without fsync.
    pub fn checkpoint_no_sync(&self) -> Result<()> {
        let file = self.inner.lock();
        file.set_len(0)?;
        // Truncation discards every pending append — nothing left to sync.
        self.synced_seq
            .fetch_max(self.append_seq.load(Ordering::SeqCst), Ordering::SeqCst);
        // See `checkpoint` — the header must be rewritten on next append.
        self.header_state
            .store(header_state::NEEDED, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// Read all valid entries from the WAL and replay them idempotently.
    /// When field_indexes and composite_indexes are provided, WAL replay also
    /// updates those indexes so that a cached index load remains consistent.
    pub fn recover(
        &self,
        storage: &Storage,
        primary_index: &mut HashMap<DocumentId, DocLocation>,
        doc_cache: &DocCache,
        next_id: &mut DocumentId,
        committed_tx_ids: &HashSet<u64>,
        version_index: &mut HashMap<DocumentId, u64>,
        field_indexes: &mut HashMap<String, PagedFieldIndex>,
        composite_indexes: &mut Vec<CompositeIndex>,
        verbose: bool,
        log_callback: &Option<LogCallback>,
    ) -> Result<()> {
        let vlog = |msg: &str| {
            eprintln!("{msg}");
            if let Some(cb) = log_callback {
                cb(msg);
            }
        };

        let entries = self.read_entries()?;

        if verbose && !entries.is_empty() {
            vlog(&format!(
                "[verbose] WAL: {} entries to replay",
                entries.len()
            ));
        }

        let mut inserts = 0u64;
        let mut updates = 0u64;
        let mut deletes = 0u64;
        let mut skipped = 0u64;

        for entry in entries {
            // Skip uncommitted transactional entries
            let tx_id = entry.tx_id();
            if tx_id != 0 && !committed_tx_ids.contains(&tx_id) {
                skipped += 1;
                continue;
            }

            match entry {
                WalEntry::Insert {
                    doc_id, doc_bytes, ..
                } => {
                    // Skip if already present in primary_index
                    if primary_index.contains_key(&doc_id) {
                        skipped += 1;
                        continue;
                    }
                    // Read _version from the doc bytes
                    if let Ok(doc) = crate::codec::decode_doc(&doc_bytes) {
                        let ver = doc.get("_version").and_then(|v| v.as_u64()).unwrap_or(0);
                        version_index.insert(doc_id, ver);
                        // Update field and composite indexes
                        for idx in field_indexes.values_mut() {
                            idx.insert_value(doc_id, &doc);
                        }
                        for idx in composite_indexes.iter_mut() {
                            idx.insert_value(doc_id, &doc);
                        }
                        doc_cache.put(doc_id, Arc::new(doc));
                    }
                    // _no_sync: one storage.sync() covers the whole replay
                    // (see below) — a per-entry fsync made recovering large
                    // WALs take minutes. Crash-safety is unchanged: the WAL
                    // holds every entry until the checkpoint at the end.
                    let loc = storage.append_no_sync(&doc_bytes)?;
                    primary_index.insert(doc_id, loc);
                    if doc_id >= *next_id {
                        *next_id = doc_id + 1;
                    }
                    inserts += 1;
                }
                WalEntry::Update {
                    doc_id, doc_bytes, ..
                } => {
                    if let Some(&old_loc) = primary_index.get(&doc_id) {
                        // Existing doc — remove old index values, then replace
                        // storage if the bytes actually changed. One read
                        // serves both the index removal and the comparison.
                        let current_bytes = storage.read(old_loc)?;
                        if let Ok(old_doc) = crate::codec::decode_doc(&current_bytes) {
                            for idx in field_indexes.values_mut() {
                                idx.remove_value(doc_id, &old_doc);
                            }
                            for idx in composite_indexes.iter_mut() {
                                idx.remove_value(doc_id, &old_doc);
                            }
                        }
                        if current_bytes != doc_bytes {
                            let new_loc = storage.append_no_sync(&doc_bytes)?;
                            storage.mark_deleted_no_sync(old_loc)?;
                            primary_index.insert(doc_id, new_loc);
                        }
                    } else {
                        // Orphan update: there is no prior record for this
                        // doc_id in the replay (e.g. its Insert lived in an
                        // earlier, already-checkpointed segment, or this is a
                        // committed full-document replace). Materialize it as an
                        // insert so storage and primary_index stay consistent
                        // with the field/composite indexes and doc cache updated
                        // below — otherwise those would reference a doc with no
                        // storage location or primary-index entry.
                        let loc = storage.append_no_sync(&doc_bytes)?;
                        primary_index.insert(doc_id, loc);
                        if doc_id >= *next_id {
                            *next_id = doc_id + 1;
                        }
                    }
                    // Update version_index and indexes from the new doc bytes.
                    // Safe to run unconditionally now: the branch above
                    // guarantees `doc_id` is present in storage/primary_index.
                    if let Ok(doc) = crate::codec::decode_doc(&doc_bytes) {
                        let ver = doc.get("_version").and_then(|v| v.as_u64()).unwrap_or(0);
                        version_index.insert(doc_id, ver);
                        for idx in field_indexes.values_mut() {
                            idx.insert_value(doc_id, &doc);
                        }
                        for idx in composite_indexes.iter_mut() {
                            idx.insert_value(doc_id, &doc);
                        }
                        doc_cache.put(doc_id, Arc::new(doc));
                    }
                    updates += 1;
                }
                WalEntry::Delete { doc_id, .. } => {
                    // Remove from indexes before removing from primary
                    if let Some(&loc) = primary_index.get(&doc_id) {
                        if let Ok(old_doc) = crate::codec::decode_doc(&storage.read(loc)?) {
                            for idx in field_indexes.values_mut() {
                                idx.remove_value(doc_id, &old_doc);
                            }
                            for idx in composite_indexes.iter_mut() {
                                idx.remove_value(doc_id, &old_doc);
                            }
                        }
                        storage.mark_deleted_no_sync(loc)?;
                        primary_index.remove(&doc_id);
                    }
                    doc_cache.remove(doc_id);
                    version_index.remove(&doc_id);
                    deletes += 1;
                }
            }
        }

        if verbose && (inserts > 0 || updates > 0 || deletes > 0 || skipped > 0) {
            vlog(&format!(
                "[verbose] WAL: replayed {} inserts, {} updates, {} deletes, {} skipped",
                inserts, updates, deletes, skipped
            ));
        }

        // Single fsync for the whole replay — the data file must be durable
        // BEFORE the checkpoint truncates the WAL that could re-create it.
        if inserts > 0 || updates > 0 || deletes > 0 {
            storage.sync()?;
        }

        self.checkpoint()?;
        Ok(())
    }

    /// Delete the WAL file from disk.
    pub fn remove_file(&self) -> Result<()> {
        if self.path.exists() {
            fs::remove_file(&self.path)?;
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Seal the segment if a sequencer is attached and the live WAL has
    /// grown past its size threshold. No-op when PITR is disabled — with
    /// no archiver to consume them, sealed segments would only pile up.
    /// Caller holds the `inner` lock.
    fn maybe_seal_locked(&self, file: &mut File) -> Result<()> {
        let threshold = match &self.sequencer {
            Some(seq) => seq.segment_threshold_bytes(),
            None => return Ok(()),
        };
        if file.metadata()?.len() < threshold {
            return Ok(());
        }
        self.seal_locked(file)
    }

    /// Rotate the live WAL: fsync it, atomically rename it to the next
    /// numbered sealed segment, then open a fresh empty live WAL in its
    /// place. Caller holds the `inner` lock; `file` is swapped in-place
    /// for the fresh handle before the lock is released, so the next
    /// writer always sees the new segment.
    ///
    /// Crash safety: a crash between the rename and the dir fsync leaves
    /// either the old `.wal` intact (rename not durable → replay it) or
    /// the sealed `<path>.<seq>` present with no `.wal` (`open` recreates
    /// an empty one, recovery replays the sealed segment). Both states
    /// are recoverable; no acknowledged write is lost.
    fn seal_locked(&self, file: &mut File) -> Result<()> {
        // Flush any not-yet-synced writes into the segment being sealed.
        fsync_file(&file)?;
        let seq = self.next_seal_seq.fetch_add(1, Ordering::SeqCst);
        let sealed_path = Self::sealed_segment_path(&self.path, seq);
        fs::rename(&self.path, &sealed_path)?;
        // Fresh, empty live WAL at the original path.
        *file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&self.path)?;
        // New file is empty — the next append writes the OXWA header.
        self.header_state
            .store(header_state::NEEDED, std::sync::atomic::Ordering::Release);
        // Make the rename (and the new file's dir entry) durable.
        if let Some(parent) = self.path.parent() {
            if let Ok(dir) = File::open(parent) {
                let _ = dir.sync_all();
            }
        }
        Ok(())
    }

    /// Every sealed segment for `wal_path` as `(seq, path)` pairs. Matches
    /// `<file_name>.<n>` siblings where `<n>` parses as a `u64`; the live
    /// WAL itself and `.tmp`-style siblings are skipped.
    fn scan_sealed_segments(wal_path: &Path) -> Vec<(u64, PathBuf)> {
        // Fast path: when no `.0` sentinel exists, no segments exist and the
        // parent-directory scan can be skipped. This collapses two read_dir
        // calls per `BTreeCollection::open` to a couple of `stat` calls —
        // at 10K collections in one data dir the dropped work is O(N²)
        // read_dir entries (~456s observed in the collection-scale bench).
        //
        // The sentinel is an INVARIANT, not an accident: `retire_segment`
        // truncates `.0` to empty instead of removing it, precisely so this
        // probe stays truthful. The first version of this fast path assumed
        // `.0` would always survive while the checkpoint deleted it — after
        // which every later segment was invisible: never retired (unbounded
        // growth) and, far worse, never REPLAYED at recovery, losing acked
        // writes to a crash between a seal and its persist. `.1` is probed
        // as well for data dirs that bug left behind, whose first surviving
        // segment is `.1` with no `.0` beside it.
        if !Self::sealed_segment_path(wal_path, 0).exists()
            && !Self::sealed_segment_path(wal_path, 1).exists()
        {
            return Vec::new();
        }
        let (parent, prefix) = match (wal_path.parent(), wal_path.file_name()) {
            (Some(p), Some(f)) => (p, format!("{}.", f.to_string_lossy())),
            _ => return Vec::new(),
        };
        let mut out = Vec::new();
        if let Ok(rd) = fs::read_dir(parent) {
            for entry in rd.flatten() {
                let fname = entry.file_name();
                let fname = fname.to_string_lossy();
                if let Some(suffix) = fname.strip_prefix(&prefix) {
                    if let Ok(seq) = suffix.parse::<u64>() {
                        out.push((seq, entry.path()));
                    }
                }
            }
        }
        out
    }

    /// The next seal sequence number — one past the highest sealed segment
    /// already on disk, so seals never collide across restarts.
    fn scan_max_seal_seq(wal_path: &Path) -> u64 {
        Self::scan_sealed_segments(wal_path)
            .iter()
            .map(|(seq, _)| *seq + 1)
            .max()
            .unwrap_or(0)
    }

    /// Retire one sealed segment whose contents are known to be covered by a
    /// persisted snapshot (or an archive copy). Sequence 0 is TRUNCATED to
    /// empty instead of removed: the empty `.0` is the sentinel
    /// `scan_sealed_segments`' fast path probes, and removing it once made
    /// the scanner blind to every later segment — see the comment there.
    pub(crate) fn retire_segment(path: &Path, seq: u64) -> std::io::Result<()> {
        if seq == 0 {
            OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)?;
            Ok(())
        } else {
            fs::remove_file(path)
        }
    }

    /// Retire EVERY sealed segment of this WAL. Only correct when the caller
    /// has just persisted a snapshot taken after a seal barrier: every
    /// segment on disk was sealed before that barrier, so the snapshot
    /// covers them all — including orphans an earlier crash (or the
    /// scanner-blindness bug) left behind, which recovery replayed into the
    /// tree at open.
    pub fn retire_covered_segments(&self) -> Result<()> {
        let segs = Self::scan_sealed_segments(&self.path);
        if segs.is_empty() {
            return Ok(());
        }
        for (seq, path) in segs {
            Self::retire_segment(&path, seq)?;
        }
        // The sentinel must survive every retire — including on a legacy dir
        // (from before the sentinel rule) whose first segment was `.1`, where
        // nothing above ever produced a `.0` to truncate.
        let sentinel = Self::sealed_segment_path(&self.path, 0);
        if !sentinel.exists() {
            OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&sentinel)?;
        }
        Ok(())
    }

    /// Path of the sealed segment with sequence `seq`: `<wal_path>.<seq>`.
    fn sealed_segment_path(wal_path: &Path, seq: u64) -> PathBuf {
        let mut name = wal_path
            .file_name()
            .map(|n| n.to_os_string())
            .unwrap_or_default();
        name.push(format!(".{seq}"));
        wal_path.with_file_name(name)
    }

    /// Build one on-disk INSERT record without owning `doc_bytes` — the
    /// no-clone fast path for the bulk-insert log methods. Stamps a v2
    /// header when a sequencer is attached, mirroring `serialize_entry`.
    fn encode_insert_record(&self, doc_id: u64, doc_bytes: &[u8]) -> Result<Vec<u8>> {
        let meta = match &self.sequencer {
            Some(seq) => Some(seq.next()?),
            None => None,
        };
        let encrypted = self.maybe_encrypt(doc_bytes)?;
        let extra = if meta.is_some() { 16 } else { 0 };
        let mut payload = Vec::with_capacity(1 + 8 + 8 + extra + encrypted.len());
        payload.push(if meta.is_some() {
            OP_INSERT_V2
        } else {
            OP_INSERT
        });
        payload.extend_from_slice(&0u64.to_le_bytes()); // tx_id = 0
        payload.extend_from_slice(&doc_id.to_le_bytes());
        if let Some(m) = meta {
            payload.extend_from_slice(&m.gsn.to_le_bytes());
            payload.extend_from_slice(&m.wall_clock_micros.to_le_bytes());
        }
        payload.extend_from_slice(&*encrypted);
        Ok(Self::frame(payload))
    }

    /// Wrap a payload in its on-disk frame: `[crc: u32 LE][len: u32 LE][payload]`.
    fn frame(payload: Vec<u8>) -> Vec<u8> {
        let crc = Self::compute_crc(&payload);
        let mut rec = Vec::with_capacity(8 + payload.len());
        rec.extend_from_slice(&crc.to_le_bytes());
        rec.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        rec.extend_from_slice(&payload);
        rec
    }

    /// Serialize an entry's payload.
    ///
    /// v1 (`meta` is `None`): `[op: u8][tx_id: u64 LE][doc_id: u64 LE][doc_bytes...]`
    /// v2 (`meta` is `Some`): `[op|0x80][tx_id: u64 LE][doc_id: u64 LE][gsn: u64 LE][wall_clock: u64 LE][doc_bytes...]`
    ///
    /// When encryption is enabled, doc_bytes are encrypted before inclusion in
    /// the payload. CRC is computed by the caller over the final payload.
    fn serialize_entry(&self, entry: &WalEntry, meta: Option<WalMeta>) -> Result<Vec<u8>> {
        // Extra header bytes for v2: gsn (8) + wall_clock (8).
        let extra = if meta.is_some() { 16 } else { 0 };
        let payload = match entry {
            WalEntry::Insert {
                doc_id,
                doc_bytes,
                tx_id,
            }
            | WalEntry::Update {
                doc_id,
                doc_bytes,
                tx_id,
            } => {
                let is_insert = matches!(entry, WalEntry::Insert { .. });
                let encrypted = self.maybe_encrypt(doc_bytes)?;
                let mut payload = Vec::with_capacity(1 + 8 + 8 + extra + encrypted.len());
                let (v1_op, v2_op) = if is_insert {
                    (OP_INSERT, OP_INSERT_V2)
                } else {
                    (OP_UPDATE, OP_UPDATE_V2)
                };
                payload.push(if meta.is_some() { v2_op } else { v1_op });
                payload.extend_from_slice(&tx_id.to_le_bytes());
                payload.extend_from_slice(&doc_id.to_le_bytes());
                if let Some(m) = meta {
                    payload.extend_from_slice(&m.gsn.to_le_bytes());
                    payload.extend_from_slice(&m.wall_clock_micros.to_le_bytes());
                }
                payload.extend_from_slice(&*encrypted);
                payload
            }
            WalEntry::Delete { doc_id, tx_id } => {
                let mut payload = Vec::with_capacity(1 + 8 + 8 + extra);
                payload.push(if meta.is_some() {
                    OP_DELETE_V2
                } else {
                    OP_DELETE
                });
                payload.extend_from_slice(&tx_id.to_le_bytes());
                payload.extend_from_slice(&doc_id.to_le_bytes());
                if let Some(m) = meta {
                    payload.extend_from_slice(&m.gsn.to_le_bytes());
                    payload.extend_from_slice(&m.wall_clock_micros.to_le_bytes());
                }
                payload
            }
        };
        Ok(payload)
    }

    fn maybe_encrypt<'a>(&self, data: &'a [u8]) -> Result<Cow<'a, [u8]>> {
        match &self.encryption {
            Some(key) => Ok(Cow::Owned(key.encrypt(data)?)),
            None => Ok(Cow::Borrowed(data)),
        }
    }

    fn maybe_decrypt(&self, data: &[u8]) -> Result<Vec<u8>> {
        match &self.encryption {
            Some(key) => key.decrypt(data),
            None => Ok(data.to_vec()),
        }
    }

    fn compute_crc(data: &[u8]) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(data);
        hasher.finalize()
    }

    /// Read all valid entries from the WAL. v2 metadata (GSN/timestamp) is
    /// dropped here — this is the path used by the existing crash-recovery
    /// replay, which only needs the mutations. Use `read_records` when the
    /// metadata matters (the archiver and the PITR replay tool).
    pub fn read_entries(&self) -> Result<Vec<WalEntry>> {
        Ok(self.scan(u64::MAX)?.into_iter().map(|r| r.entry).collect())
    }

    /// Read all valid records from the WAL, preserving v2 metadata.
    pub fn read_records(&self) -> Result<Vec<WalRecord>> {
        self.scan(u64::MAX)
    }

    /// Read records from only the first `max_bytes` of the file. Used to
    /// read the WAL records out of an archived `.seg` file — that file is
    /// the verbatim WAL bytes followed by a fixed trailer, so `max_bytes`
    /// is the trailer offset and the trailer is never mis-parsed.
    pub fn read_records_prefix(&self, max_bytes: u64) -> Result<Vec<WalRecord>> {
        self.scan(max_bytes)
    }

    /// Append a batch of records with their explicit v2 metadata, with a
    /// single fsync. Used by the PITR replay tool to materialize a
    /// filtered WAL during `restore_to_point` — no auto-seal, since the
    /// target database is being built, not run.
    pub fn log_records(&self, records: &[WalRecord]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let mut file = self.inner.lock();
        self.ensure_header_locked(&mut file)?;
        let mut buf = Vec::new();
        for r in records {
            buf.extend_from_slice(&Self::frame(self.serialize_entry(&r.entry, Some(r.meta))?));
        }
        file.seek(SeekFrom::End(0))?;
        file.write_all(&buf)?;
        fsync_file(&file)?;
        Ok(())
    }

    /// Scan the WAL front-to-back over the first `limit` bytes,
    /// CRC-verifying each record and stopping at the first torn/corrupt
    /// one (treating it as the crash boundary).
    fn scan(&self, limit: u64) -> Result<Vec<WalRecord>> {
        let mut file = self.inner.lock();
        let file_len = file.metadata()?.len().min(limit);
        // Skip the OXWA file header when it's present; legacy / brand-new
        // files start the record stream at offset 0.
        let start = self.records_start_offset().min(file_len);
        file.seek(SeekFrom::Start(start))?;
        let mut records = Vec::new();
        let mut pos = start;

        while pos + 8 <= file_len {
            // Read header: crc32 (4) + payload_len (4)
            let mut header = [0u8; 8];
            if file.read_exact(&mut header).is_err() {
                if pos > 0 {
                    eprintln!(
                        "[wal] truncated header at offset {pos}, stopping replay ({} entries recovered)",
                        records.len()
                    );
                }
                break;
            }

            let stored_crc = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
            let payload_len =
                u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as usize;

            if pos + 8 + payload_len as u64 > file_len {
                eprintln!(
                    "[wal] truncated payload at offset {pos} (need {} bytes, file has {}), stopping replay",
                    payload_len,
                    file_len - pos - 8
                );
                break;
            }

            let mut payload = vec![0u8; payload_len];
            if file.read_exact(&mut payload).is_err() {
                break; // Read error, stop
            }

            // Verify CRC
            let computed_crc = Self::compute_crc(&payload);
            if stored_crc != computed_crc {
                eprintln!(
                    "[wal] CRC mismatch at offset {pos}: stored={stored_crc:#010x} computed={computed_crc:#010x}, stopping replay ({} entries recovered)",
                    records.len()
                );
                break;
            }

            // Parse payload (handles both v1 and v2 records)
            if let Some((entry, meta)) = self.parse_payload(&payload) {
                records.push(WalRecord { entry, meta });
            } else {
                break; // Malformed payload
            }

            pos += 8 + payload_len as u64;
        }

        Ok(records)
    }

    /// Parse one record payload. Handles both v1 records and v2 (PITR)
    /// records carrying a `[gsn][wall_clock]` header — see `serialize_entry`.
    /// Returns the entry plus its metadata (`WalMeta::default()` for v1).
    fn parse_payload(&self, payload: &[u8]) -> Option<(WalEntry, WalMeta)> {
        // Every record begins with [op: u8][tx_id: u64 LE][doc_id: u64 LE].
        if payload.len() < 17 {
            return None;
        }
        let op_type = payload[0];
        let tx_id = u64::from_le_bytes(payload[1..9].try_into().ok()?);
        let doc_id = u64::from_le_bytes(payload[9..17].try_into().ok()?);

        // v2 records carry [gsn: u64 LE][wall_clock: u64 LE] after doc_id;
        // the high bit of the op byte marks them.
        let is_v2 = op_type & 0x80 != 0;
        let (meta, body_start) = if is_v2 {
            if payload.len() < 33 {
                return None; // 1 + 8 + 8 + 8 + 8
            }
            let gsn = u64::from_le_bytes(payload[17..25].try_into().ok()?);
            let wall_clock_micros = u64::from_le_bytes(payload[25..33].try_into().ok()?);
            (
                WalMeta {
                    gsn,
                    wall_clock_micros,
                },
                33,
            )
        } else {
            (WalMeta::default(), 17)
        };

        let entry = match op_type {
            OP_INSERT | OP_INSERT_V2 => {
                let doc_bytes = self.maybe_decrypt(&payload[body_start..]).ok()?;
                WalEntry::Insert {
                    doc_id,
                    doc_bytes,
                    tx_id,
                }
            }
            OP_UPDATE | OP_UPDATE_V2 => {
                let doc_bytes = self.maybe_decrypt(&payload[body_start..]).ok()?;
                WalEntry::Update {
                    doc_id,
                    doc_bytes,
                    tx_id,
                }
            }
            OP_DELETE | OP_DELETE_V2 => WalEntry::Delete { doc_id, tx_id },
            _ => return None,
        };
        Some((entry, meta))
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_wal(dir: &TempDir) -> Wal {
        Wal::open(&dir.path().join("test.wal")).unwrap()
    }

    #[test]
    fn log_and_read_insert() {
        let dir = TempDir::new().unwrap();
        let wal = test_wal(&dir);

        let entry = WalEntry::insert(1, b"doc_data".to_vec());
        wal.log(&entry).unwrap();

        let entries = wal.read_entries().unwrap();
        assert_eq!(entries.len(), 1);
        match &entries[0] {
            WalEntry::Insert {
                doc_id,
                doc_bytes,
                tx_id,
            } => {
                assert_eq!(*doc_id, 1);
                assert_eq!(doc_bytes, b"doc_data");
                assert_eq!(*tx_id, 0);
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn log_and_read_update() {
        let dir = TempDir::new().unwrap();
        let wal = test_wal(&dir);

        let entry = WalEntry::update(5, b"updated_data".to_vec());
        wal.log(&entry).unwrap();

        let entries = wal.read_entries().unwrap();
        assert_eq!(entries.len(), 1);
        match &entries[0] {
            WalEntry::Update {
                doc_id,
                doc_bytes,
                tx_id,
            } => {
                assert_eq!(*doc_id, 5);
                assert_eq!(doc_bytes, b"updated_data");
                assert_eq!(*tx_id, 0);
            }
            _ => panic!("expected Update"),
        }
    }

    #[test]
    fn log_and_read_delete() {
        let dir = TempDir::new().unwrap();
        let wal = test_wal(&dir);

        let entry = WalEntry::delete(10);
        wal.log(&entry).unwrap();

        let entries = wal.read_entries().unwrap();
        assert_eq!(entries.len(), 1);
        match &entries[0] {
            WalEntry::Delete { doc_id, tx_id } => {
                assert_eq!(*doc_id, 10);
                assert_eq!(*tx_id, 0);
            }
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn log_batch() {
        let dir = TempDir::new().unwrap();
        let wal = test_wal(&dir);

        let entries = vec![
            WalEntry::insert(1, b"a".to_vec()),
            WalEntry::insert(2, b"b".to_vec()),
            WalEntry::delete(1),
        ];
        wal.log_batch(&entries).unwrap();

        let read = wal.read_entries().unwrap();
        assert_eq!(read.len(), 3);
    }

    #[test]
    fn checkpoint_clears_wal() {
        let dir = TempDir::new().unwrap();
        let wal = test_wal(&dir);

        wal.log(&WalEntry::insert(1, b"data".to_vec())).unwrap();
        assert!(!wal.read_entries().unwrap().is_empty());

        wal.checkpoint().unwrap();
        assert!(wal.read_entries().unwrap().is_empty());
    }

    #[test]
    fn crc_corruption_stops_replay() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("corrupt.wal");
        let wal = Wal::open(&wal_path).unwrap();

        wal.log(&WalEntry::insert(1, b"good".to_vec())).unwrap();
        wal.log(&WalEntry::insert(2, b"will_corrupt".to_vec()))
            .unwrap();
        wal.log(&WalEntry::insert(3, b"after_corrupt".to_vec()))
            .unwrap();

        // Corrupt the CRC of the second entry. File layout post-Phase-1b:
        //   [OXWA header 8B][rec0 crc 4B][rec0 len 4B][rec0 payload …]
        //   [rec1 crc 4B][rec1 len 4B][rec1 payload …]
        let mut file_data = std::fs::read(&wal_path).unwrap();
        let first_payload_off = WAL_HEADER_SIZE + 8; // header + first record's framing
        let first_payload_len = u32::from_le_bytes([
            file_data[WAL_HEADER_SIZE + 4],
            file_data[WAL_HEADER_SIZE + 5],
            file_data[WAL_HEADER_SIZE + 6],
            file_data[WAL_HEADER_SIZE + 7],
        ]) as usize;
        let second_offset = first_payload_off + first_payload_len; // start of rec1 framing
        file_data[second_offset] ^= 0xFF; // flip a CRC byte of rec1
        std::fs::write(&wal_path, &file_data).unwrap();

        // Reopen and read — should stop at corrupt entry
        let wal2 = Wal::open(&wal_path).unwrap();
        let entries = wal2.read_entries().unwrap();
        assert_eq!(entries.len(), 1); // Only first entry survived
    }

    #[test]
    fn transactional_entries_with_tx_id() {
        let dir = TempDir::new().unwrap();
        let wal = test_wal(&dir);

        let entry = WalEntry::Insert {
            doc_id: 1,
            doc_bytes: b"tx_data".to_vec(),
            tx_id: 42,
        };
        wal.log(&entry).unwrap();

        let entries = wal.read_entries().unwrap();
        assert_eq!(entries[0].tx_id(), 42);
    }

    #[test]
    fn recover_replays_insert() {
        let dir = TempDir::new().unwrap();
        let wal = test_wal(&dir);
        let storage = Storage::open(&dir.path().join("data.dat")).unwrap();

        // Log an insert
        let doc_bytes = br#"{"name":"test","_version":1}"#;
        wal.log(&WalEntry::insert(0, doc_bytes.to_vec())).unwrap();

        let mut primary_index = HashMap::new();
        let mut next_id = 0u64;
        let committed = HashSet::new();
        let mut version_index = HashMap::new();

        let mut fi = HashMap::new();
        let mut ci = Vec::new();
        let dc = DocCache::new(1000);
        wal.recover(
            &storage,
            &mut primary_index,
            &dc,
            &mut next_id,
            &committed,
            &mut version_index,
            &mut fi,
            &mut ci,
            false,
            &None,
        )
        .unwrap();

        assert_eq!(primary_index.len(), 1);
        assert!(primary_index.contains_key(&0));
        assert_eq!(next_id, 1);
    }

    #[test]
    fn recover_skips_uncommitted_tx() {
        let dir = TempDir::new().unwrap();
        let wal = test_wal(&dir);
        let storage = Storage::open(&dir.path().join("data.dat")).unwrap();

        // Log a transactional insert with tx_id=99 (not committed)
        let entry = WalEntry::Insert {
            doc_id: 0,
            doc_bytes: br#"{"x":1}"#.to_vec(),
            tx_id: 99,
        };
        wal.log(&entry).unwrap();

        let mut primary_index = HashMap::new();
        let mut next_id = 0u64;
        let committed = HashSet::new(); // tx 99 not committed
        let mut version_index = HashMap::new();
        let mut fi = HashMap::new();
        let mut ci = Vec::new();
        let dc = DocCache::new(1000);

        wal.recover(
            &storage,
            &mut primary_index,
            &dc,
            &mut next_id,
            &committed,
            &mut version_index,
            &mut fi,
            &mut ci,
            false,
            &None,
        )
        .unwrap();

        assert!(primary_index.is_empty()); // Should be skipped
    }

    #[test]
    fn recover_applies_committed_tx() {
        let dir = TempDir::new().unwrap();
        let wal = test_wal(&dir);
        let storage = Storage::open(&dir.path().join("data.dat")).unwrap();

        let entry = WalEntry::Insert {
            doc_id: 0,
            doc_bytes: br#"{"x":1,"_version":1}"#.to_vec(),
            tx_id: 99,
        };
        wal.log(&entry).unwrap();

        let mut primary_index = HashMap::new();
        let mut next_id = 0u64;
        let mut committed = HashSet::new();
        committed.insert(99u64); // Mark tx 99 as committed
        let mut version_index = HashMap::new();
        let mut fi = HashMap::new();
        let mut ci = Vec::new();
        let dc = DocCache::new(1000);

        wal.recover(
            &storage,
            &mut primary_index,
            &dc,
            &mut next_id,
            &committed,
            &mut version_index,
            &mut fi,
            &mut ci,
            false,
            &None,
        )
        .unwrap();

        assert_eq!(primary_index.len(), 1);
    }

    #[test]
    fn recover_delete_removes_from_index() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");
        let storage = Storage::open(&dir.path().join("data.dat")).unwrap();

        // First, add a record directly to storage and primary_index
        let doc_bytes = br#"{"x":1}"#;
        let loc = storage.append(doc_bytes).unwrap();
        let mut primary_index = HashMap::new();
        primary_index.insert(0u64, loc);
        let mut next_id = 1u64;
        let committed = HashSet::new();
        let mut version_index = HashMap::new();
        let mut fi = HashMap::new();
        let mut ci = Vec::new();
        let dc = DocCache::new(1000);

        // Now log a delete in WAL
        let wal = Wal::open(&wal_path).unwrap();
        wal.log(&WalEntry::delete(0)).unwrap();

        wal.recover(
            &storage,
            &mut primary_index,
            &dc,
            &mut next_id,
            &committed,
            &mut version_index,
            &mut fi,
            &mut ci,
            false,
            &None,
        )
        .unwrap();

        assert!(primary_index.is_empty());
    }

    #[test]
    fn encrypted_wal_roundtrip() {
        let dir = TempDir::new().unwrap();
        let key_path = dir.path().join("test.key");
        std::fs::write(&key_path, &[0x42u8; 32]).unwrap();
        let enc_key = crate::crypto::EncryptionKey::load_from_file(&key_path).unwrap();

        let wal =
            Wal::open_with_encryption(&dir.path().join("encrypted.wal"), Some(enc_key)).unwrap();

        let data = b"secret_doc_content";
        wal.log(&WalEntry::insert(1, data.to_vec())).unwrap();

        let entries = wal.read_entries().unwrap();
        assert_eq!(entries.len(), 1);
        match &entries[0] {
            WalEntry::Insert { doc_bytes, .. } => assert_eq!(doc_bytes, data),
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn remove_file_deletes_wal() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("remove_me.wal");
        let wal = Wal::open(&wal_path).unwrap();
        wal.log(&WalEntry::insert(1, b"x".to_vec())).unwrap();
        assert!(wal_path.exists());

        wal.remove_file().unwrap();
        assert!(!wal_path.exists());
    }

    #[test]
    fn log_no_sync_and_batch_no_sync() {
        let dir = TempDir::new().unwrap();
        let wal = test_wal(&dir);

        wal.log_no_sync(&WalEntry::insert(1, b"a".to_vec()))
            .unwrap();
        wal.log_batch_no_sync(&[
            WalEntry::insert(2, b"b".to_vec()),
            WalEntry::insert(3, b"c".to_vec()),
        ])
        .unwrap();

        let entries = wal.read_entries().unwrap();
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn empty_wal_reads_nothing() {
        let dir = TempDir::new().unwrap();
        let wal = test_wal(&dir);
        let entries = wal.read_entries().unwrap();
        assert!(entries.is_empty());
    }

    // ── PITR Phase 0: v2 (extended-header) record format ────────────────

    #[test]
    fn v2_record_roundtrips_with_meta() {
        let dir = TempDir::new().unwrap();
        let wal = test_wal(&dir);

        let meta = WalMeta {
            gsn: 7,
            wall_clock_micros: 1_700_000_000_000_000,
        };
        wal.log_with_meta(&WalEntry::insert(3, b"v2_doc".to_vec()), meta)
            .unwrap();
        wal.log_with_meta(
            &WalEntry::delete(3),
            WalMeta {
                gsn: 8,
                wall_clock_micros: 1_700_000_000_000_001,
            },
        )
        .unwrap();

        let records = wal.read_records().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].meta, meta);
        match &records[0].entry {
            WalEntry::Insert {
                doc_id,
                doc_bytes,
                tx_id,
            } => {
                assert_eq!(*doc_id, 3);
                assert_eq!(doc_bytes, b"v2_doc");
                assert_eq!(*tx_id, 0);
            }
            _ => panic!("expected Insert"),
        }
        assert_eq!(records[1].meta.gsn, 8);
        assert!(matches!(
            records[1].entry,
            WalEntry::Delete { doc_id: 3, .. }
        ));
    }

    #[test]
    fn read_entries_drops_v2_meta_but_keeps_entry() {
        let dir = TempDir::new().unwrap();
        let wal = test_wal(&dir);

        wal.log_with_meta(
            &WalEntry::Update {
                doc_id: 9,
                doc_bytes: b"x".to_vec(),
                tx_id: 42,
            },
            WalMeta {
                gsn: 100,
                wall_clock_micros: 5,
            },
        )
        .unwrap();

        // The legacy replay path still sees the entry, just without meta.
        let entries = wal.read_entries().unwrap();
        assert_eq!(entries.len(), 1);
        match &entries[0] {
            WalEntry::Update {
                doc_id,
                doc_bytes,
                tx_id,
            } => {
                assert_eq!(*doc_id, 9);
                assert_eq!(doc_bytes, b"x");
                assert_eq!(*tx_id, 42);
            }
            _ => panic!("expected Update"),
        }
    }

    #[test]
    fn mixed_v1_and_v2_records_in_one_file() {
        let dir = TempDir::new().unwrap();
        let wal = test_wal(&dir);

        // Simulates a WAL written across an upgrade: v1 records, then v2.
        wal.log(&WalEntry::insert(1, b"old".to_vec())).unwrap();
        wal.log_with_meta(
            &WalEntry::insert(2, b"new".to_vec()),
            WalMeta {
                gsn: 1,
                wall_clock_micros: 11,
            },
        )
        .unwrap();
        wal.log(&WalEntry::delete(1)).unwrap();
        wal.log_with_meta(
            &WalEntry::delete(2),
            WalMeta {
                gsn: 2,
                wall_clock_micros: 22,
            },
        )
        .unwrap();

        let records = wal.read_records().unwrap();
        assert_eq!(records.len(), 4);
        // v1 records parse back with zeroed meta...
        assert_eq!(records[0].meta, WalMeta::default());
        assert_eq!(records[2].meta, WalMeta::default());
        // ...v2 records keep theirs.
        assert_eq!(records[1].meta.gsn, 1);
        assert_eq!(records[3].meta.gsn, 2);

        // And the legacy entry-only view still returns all four.
        assert_eq!(wal.read_entries().unwrap().len(), 4);
    }

    #[test]
    fn v2_record_with_encryption_roundtrips() {
        let dir = TempDir::new().unwrap();
        let key_path = dir.path().join("v2.key");
        std::fs::write(&key_path, &[0x17u8; 32]).unwrap();
        let enc_key = crate::crypto::EncryptionKey::load_from_file(&key_path).unwrap();
        let wal = Wal::open_with_encryption(&dir.path().join("v2_enc.wal"), Some(enc_key)).unwrap();

        let meta = WalMeta {
            gsn: 55,
            wall_clock_micros: 999,
        };
        wal.log_with_meta(&WalEntry::insert(1, b"secret_v2".to_vec()), meta)
            .unwrap();

        let records = wal.read_records().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].meta, meta);
        match &records[0].entry {
            WalEntry::Insert { doc_bytes, .. } => assert_eq!(doc_bytes, b"secret_v2"),
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn v2_crc_corruption_stops_replay() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("v2_corrupt.wal");
        let wal = Wal::open(&wal_path).unwrap();

        wal.log_with_meta(
            &WalEntry::insert(1, b"good".to_vec()),
            WalMeta {
                gsn: 1,
                wall_clock_micros: 1,
            },
        )
        .unwrap();
        wal.log_with_meta(
            &WalEntry::insert(2, b"corrupt_me".to_vec()),
            WalMeta {
                gsn: 2,
                wall_clock_micros: 2,
            },
        )
        .unwrap();

        // Corrupt the CRC of the second record. File layout post-Phase-1b:
        //   [OXWA header 8B][rec0 framing 8B][rec0 payload][rec1 framing 8B][rec1 payload]
        let mut bytes = std::fs::read(&wal_path).unwrap();
        let first_len = u32::from_le_bytes([
            bytes[WAL_HEADER_SIZE + 4],
            bytes[WAL_HEADER_SIZE + 5],
            bytes[WAL_HEADER_SIZE + 6],
            bytes[WAL_HEADER_SIZE + 7],
        ]) as usize;
        let second_off = WAL_HEADER_SIZE + 8 + first_len;
        bytes[second_off] ^= 0xFF;
        std::fs::write(&wal_path, &bytes).unwrap();

        let wal2 = Wal::open(&wal_path).unwrap();
        assert_eq!(wal2.read_records().unwrap().len(), 1);
    }

    // ── PITR Phase 1: sequencer-attached WAL auto-stamps records ────────

    #[test]
    fn attached_sequencer_auto_stamps_every_log_path() {
        use crate::pitr::ArchiveSequencer;
        let dir = TempDir::new().unwrap();
        let seq = Arc::new(ArchiveSequencer::open(dir.path()).unwrap());
        let wal = Wal::open(&dir.path().join("seq.wal"))
            .unwrap()
            .with_sequencer(Some(seq));

        // Exercise the single, batch, and no-clone-bulk-insert log paths.
        wal.log(&WalEntry::insert(1, b"a".to_vec())).unwrap();
        wal.log_batch(&[WalEntry::update(1, b"b".to_vec()), WalEntry::delete(1)])
            .unwrap();
        wal.log_batch_inserts_no_sync_buffered(&[(2, b"c".as_slice())])
            .unwrap();

        let records = wal.read_records().unwrap();
        assert_eq!(records.len(), 4);
        // Every record carries a unique, strictly increasing, non-zero GSN.
        assert_eq!(
            records.iter().map(|r| r.meta.gsn).collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
        for r in &records {
            assert!(
                r.meta.wall_clock_micros > 0,
                "v2 records must carry a wall-clock"
            );
        }
    }

    #[test]
    fn no_sequencer_still_emits_v1() {
        let dir = TempDir::new().unwrap();
        let wal = test_wal(&dir);
        wal.log(&WalEntry::insert(1, b"x".to_vec())).unwrap();
        let records = wal.read_records().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].meta, WalMeta::default()); // v1 → zeroed meta
    }

    // ── PITR Phase 2: WAL segment rotation (seal) ───────────────────────

    #[test]
    fn seal_creates_numbered_segment_and_fresh_live_wal() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("c.wal");
        let wal = Wal::open(&wal_path).unwrap();

        wal.log(&WalEntry::insert(1, b"a".to_vec())).unwrap();
        wal.log(&WalEntry::insert(2, b"b".to_vec())).unwrap();
        wal.seal().unwrap();

        // The sealed segment exists and holds the two records...
        let sealed = wal.list_sealed_segments();
        assert_eq!(sealed.len(), 1);
        assert_eq!(
            Wal::open(&sealed[0]).unwrap().read_records().unwrap().len(),
            2
        );
        // ...and the live WAL is fresh and empty, ready for new writes.
        assert!(wal.read_records().unwrap().is_empty());
        wal.log(&WalEntry::insert(3, b"c".to_vec())).unwrap();
        assert_eq!(wal.read_records().unwrap().len(), 1);
    }

    #[test]
    fn union_of_sealed_and_live_holds_all_records() {
        let dir = TempDir::new().unwrap();
        let wal = Wal::open(&dir.path().join("c.wal")).unwrap();

        // Spread 9 records across 3 segments (2 sealed + 1 live).
        for i in 0..3 {
            wal.log(&WalEntry::insert(i, b"x".to_vec())).unwrap();
        }
        wal.seal().unwrap();
        for i in 3..6 {
            wal.log(&WalEntry::insert(i, b"x".to_vec())).unwrap();
        }
        wal.seal().unwrap();
        for i in 6..9 {
            wal.log(&WalEntry::insert(i, b"x".to_vec())).unwrap();
        }

        let mut ids: Vec<u64> = Vec::new();
        for seg in wal.list_sealed_segments() {
            for r in Wal::open(&seg).unwrap().read_records().unwrap() {
                ids.push(r.entry.doc_id());
            }
        }
        for r in wal.read_records().unwrap() {
            ids.push(r.entry.doc_id());
        }
        ids.sort_unstable();
        assert_eq!(ids, (0..9).collect::<Vec<_>>());
    }

    #[test]
    fn seal_seq_resumes_after_reopen() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("c.wal");
        {
            let wal = Wal::open(&wal_path).unwrap();
            wal.log(&WalEntry::insert(1, b"a".to_vec())).unwrap();
            wal.seal().unwrap(); // → c.wal.0
            wal.log(&WalEntry::insert(2, b"b".to_vec())).unwrap();
            wal.seal().unwrap(); // → c.wal.1
        }
        // A reopened WAL must seal to c.wal.2, not collide with c.wal.0.
        let wal = Wal::open(&wal_path).unwrap();
        wal.log(&WalEntry::insert(3, b"c".to_vec())).unwrap();
        wal.seal().unwrap();
        let sealed = wal.list_sealed_segments();
        assert_eq!(sealed.len(), 3);
        assert!(sealed[2].to_string_lossy().ends_with("c.wal.2"));
    }

    #[test]
    fn auto_seal_on_threshold() {
        use crate::pitr::ArchiveSequencer;
        let dir = TempDir::new().unwrap();
        // Tiny threshold so a handful of records trips a rotation.
        let seq = Arc::new(
            ArchiveSequencer::open(dir.path())
                .unwrap()
                .with_segment_threshold(256),
        );
        let wal = Wal::open(&dir.path().join("c.wal"))
            .unwrap()
            .with_sequencer(Some(seq));

        for i in 0..40 {
            wal.log(&WalEntry::insert(i, b"a-reasonably-sized-payload".to_vec()))
                .unwrap();
        }
        // Crossing the threshold sealed at least one segment automatically,
        // and every record is still present across the union.
        let sealed = wal.list_sealed_segments();
        assert!(
            !sealed.is_empty(),
            "threshold should have triggered an auto-seal"
        );
        let mut total = wal.read_records().unwrap().len();
        for seg in sealed {
            total += Wal::open(&seg).unwrap().read_records().unwrap().len();
        }
        assert_eq!(total, 40);
    }

    #[test]
    fn seal_is_atomic_under_concurrent_writers() {
        // The scar this guards against: a seal racing concurrent log()
        // calls dropping an acknowledged write (see btree_collection's
        // documented "lost 3/2000 acks" note).
        let dir = TempDir::new().unwrap();
        let wal = Arc::new(Wal::open(&dir.path().join("race.wal")).unwrap());
        let writers = 6usize;
        let per_writer = 400usize;

        let mut handles = Vec::new();
        for w in 0..writers {
            let wal = Arc::clone(&wal);
            handles.push(std::thread::spawn(move || {
                for i in 0..per_writer {
                    let doc_id = (w * per_writer + i) as u64;
                    wal.log(&WalEntry::insert(doc_id, b"x".to_vec())).unwrap();
                }
            }));
        }
        // A sealer rotating the segment underneath the writers.
        {
            let wal = Arc::clone(&wal);
            handles.push(std::thread::spawn(move || {
                for _ in 0..60 {
                    wal.seal().unwrap();
                    std::thread::yield_now();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        // Every acknowledged write must appear exactly once across the
        // union of sealed segments + the live WAL — no loss, no dup.
        let mut seen = std::collections::HashSet::new();
        for seg in wal.list_sealed_segments() {
            for r in Wal::open(&seg).unwrap().read_records().unwrap() {
                assert!(
                    seen.insert(r.entry.doc_id()),
                    "record duplicated across segments"
                );
            }
        }
        for r in wal.read_records().unwrap() {
            assert!(
                seen.insert(r.entry.doc_id()),
                "record duplicated in live WAL"
            );
        }
        assert_eq!(
            seen.len(),
            writers * per_writer,
            "lost an acknowledged write across rotation"
        );
    }

    // ── PITR Phase 4: GSN allocated under the lock + barrier ────────────

    #[test]
    fn barrier_flushes_in_flight_writes() {
        // The base-backup invariant: after `barrier()`, every record below
        // the GSN counter is present in the file. GSNs are allocated under
        // the WAL lock, so there is no encode/append gap to race.
        use crate::pitr::ArchiveSequencer;
        let dir = TempDir::new().unwrap();
        let seq = Arc::new(ArchiveSequencer::open(dir.path()).unwrap());
        let wal = Arc::new(
            Wal::open(&dir.path().join("b.wal"))
                .unwrap()
                .with_sequencer(Some(Arc::clone(&seq))),
        );

        let mut handles = Vec::new();
        for w in 0..4usize {
            let wal = Arc::clone(&wal);
            handles.push(std::thread::spawn(move || {
                for i in 0..250usize {
                    wal.log(&WalEntry::insert((w * 250 + i) as u64, b"x".to_vec()))
                        .unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let watermark = seq.current_gsn();
        wal.barrier();
        let records = wal.read_records().unwrap();
        assert_eq!(
            records.len(),
            1000,
            "every acknowledged write must be in the file"
        );
        let max_gsn = records.iter().map(|r| r.meta.gsn).max().unwrap_or(0);
        assert!(
            max_gsn < watermark,
            "the GSN counter must sit above every written record"
        );
    }

    // ── Phase 1b header (docs/format/wal.md) ─────────────────────────────

    /// A fresh WAL writes the OXWA / version=1 / flags=0 header at the
    /// very start of the file, just before the first record.
    #[test]
    fn first_append_writes_oxwa_header() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("a.wal");
        let wal = Wal::open(&path).unwrap();
        wal.log(&WalEntry::insert(1, b"x".to_vec())).unwrap();
        drop(wal);

        let raw = fs::read(&path).unwrap();
        assert!(
            raw.len() >= WAL_HEADER_SIZE + 8,
            "header + at least one framed record"
        );
        assert_eq!(&raw[0..4], WAL_MAGIC, "OXWA magic at offset 0");
        assert_eq!(u16::from_le_bytes([raw[4], raw[5]]), WAL_VERSION);
        assert_eq!(u16::from_le_bytes([raw[6], raw[7]]), 0, "flags reserved 0");
    }

    /// A pre-Phase-1b WAL file (no OXWA magic at offset 0) reads correctly,
    /// and subsequent appends extend it as-is — we never retro-prepend a
    /// header onto an existing file.
    #[test]
    fn reads_and_extends_legacy_wal() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("legacy.wal");

        // Hand-craft a legacy WAL by writing one record via the engine, then
        // stripping the 8-byte header we now produce. That gives us a byte
        // sequence identical to what a pre-Phase-1b engine would have written.
        {
            let wal = Wal::open(&path).unwrap();
            wal.log(&WalEntry::insert(7, b"hello".to_vec())).unwrap();
        }
        let with_header = fs::read(&path).unwrap();
        assert_eq!(&with_header[0..4], WAL_MAGIC);
        fs::write(&path, &with_header[WAL_HEADER_SIZE..]).unwrap();

        // Reopen + read: legacy bytes parse correctly.
        let wal = Wal::open(&path).unwrap();
        let entries = wal.read_entries().unwrap();
        assert_eq!(entries.len(), 1);

        // Append another record. File is still legacy (no header retro-prepend);
        // both entries are recoverable on reopen.
        wal.log(&WalEntry::insert(8, b"world".to_vec())).unwrap();
        drop(wal);

        let raw = fs::read(&path).unwrap();
        assert_ne!(
            &raw[0..4],
            WAL_MAGIC,
            "legacy file must NOT get a retro header"
        );

        let wal = Wal::open(&path).unwrap();
        assert_eq!(wal.read_entries().unwrap().len(), 2);
    }

    /// A WAL file with a newer format version we don't recognise must be
    /// refused at open time rather than silently misinterpreted.
    #[test]
    fn refuses_newer_wal_version() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("future.wal");

        // Hand-craft a v2 WAL: OXWA + version=2 + flags=0, no records.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(WAL_MAGIC);
        bytes.extend_from_slice(&2u16.to_le_bytes());
        bytes.extend_from_slice(&0u16.to_le_bytes());
        fs::write(&path, &bytes).unwrap();

        let err = Wal::open(&path)
            .err()
            .expect("open should reject a newer format");
        let msg = format!("{err}");
        assert!(
            msg.contains("unsupported WAL format version 2"),
            "error should name the unsupported version: {msg}"
        );
    }

    /// After `seal()` rotates a fresh empty file into place, the next append
    /// writes a new OXWA header — so each new live segment starts with one.
    #[test]
    fn seal_then_append_writes_fresh_header() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("rot.wal");
        let wal = Wal::open(&path).unwrap();
        wal.log(&WalEntry::insert(1, b"a".to_vec())).unwrap();
        wal.seal().unwrap();
        wal.log(&WalEntry::insert(2, b"b".to_vec())).unwrap();

        // Live file (post-seal) starts with OXWA.
        let live = fs::read(&path).unwrap();
        assert_eq!(
            &live[0..4],
            WAL_MAGIC,
            "new live segment after seal must carry a header"
        );

        // The sealed segment (the previous .wal renamed to .0) ALSO starts
        // with a header — it was written by this same engine before sealing.
        let sealed_path = path.with_file_name("rot.wal.0");
        let sealed = fs::read(&sealed_path).unwrap();
        assert_eq!(
            &sealed[0..4],
            WAL_MAGIC,
            "sealed segment retains its header"
        );
    }
}
