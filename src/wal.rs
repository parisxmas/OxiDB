use crate::document::DocumentId;

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
use std::sync::Arc;
#[cfg(not(target_arch = "wasm32"))]
use crate::locks::Mutex;

#[cfg(not(target_arch = "wasm32"))]
use crc32fast::Hasher;

#[cfg(not(target_arch = "wasm32"))]
use crate::crypto::EncryptionKey;
#[cfg(not(target_arch = "wasm32"))]
use crate::doc_cache::DocCache;
#[cfg(not(target_arch = "wasm32"))]
use crate::engine::LogCallback;
#[cfg(not(target_arch = "wasm32"))]
use crate::error::Result;
#[cfg(not(target_arch = "wasm32"))]
use crate::index::CompositeIndex;
#[cfg(not(target_arch = "wasm32"))]
use crate::pitr::ArchiveSequencer;
#[cfg(not(target_arch = "wasm32"))]
use crate::paged_field_index::PagedFieldIndex;
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

/// A WAL entry representing a pending mutation.
pub enum WalEntry {
    Insert { doc_id: DocumentId, doc_bytes: Vec<u8>, tx_id: u64 },
    Update { doc_id: DocumentId, doc_bytes: Vec<u8>, tx_id: u64 },
    Delete { doc_id: DocumentId, tx_id: u64 },
}

impl WalEntry {
    /// Create an Insert entry with tx_id=0 (non-transactional).
    pub fn insert(doc_id: DocumentId, doc_bytes: Vec<u8>) -> Self {
        WalEntry::Insert { doc_id, doc_bytes, tx_id: 0 }
    }

    /// Create an Update entry with tx_id=0 (non-transactional).
    pub fn update(doc_id: DocumentId, doc_bytes: Vec<u8>) -> Self {
        WalEntry::Update { doc_id, doc_bytes, tx_id: 0 }
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
}

#[cfg(not(target_arch = "wasm32"))]
impl Wal {
    /// Open or create a WAL file.
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_with_encryption(path, None)
    }

    pub fn open_with_encryption(path: &Path, encryption: Option<Arc<EncryptionKey>>) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)?;

        Ok(Self {
            inner: Mutex::new(file),
            path: path.to_path_buf(),
            encryption,
            sequencer: None,
        })
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
        let rec = self.encode_record(entry)?;
        let mut file = self.inner.lock();
        file.seek(SeekFrom::End(0))?;
        file.write_all(&rec)?;
        file.sync_data()?;
        Ok(())
    }

    /// Append a single record stamped with an explicit `meta` (v2 format),
    /// then fsync — bypassing the attached sequencer. Mainly for tests and
    /// for callers that supply their own GSN.
    pub fn log_with_meta(&self, entry: &WalEntry, meta: WalMeta) -> Result<()> {
        let rec = Self::frame(self.serialize_entry(entry, Some(meta))?);
        let mut file = self.inner.lock();
        file.seek(SeekFrom::End(0))?;
        file.write_all(&rec)?;
        file.sync_data()?;
        Ok(())
    }

    /// Serialize and append a WAL entry without fsync.
    pub fn log_no_sync(&self, entry: &WalEntry) -> Result<()> {
        let rec = self.encode_record(entry)?;
        let mut file = self.inner.lock();
        file.seek(SeekFrom::End(0))?;
        file.write_all(&rec)?;
        Ok(())
    }

    /// Write multiple WAL entries with a single fsync.
    pub fn log_batch(&self, entries: &[WalEntry]) -> Result<()> {
        let mut buf = Vec::new();
        for entry in entries {
            buf.extend_from_slice(&self.encode_record(entry)?);
        }
        let mut file = self.inner.lock();
        file.seek(SeekFrom::End(0))?;
        file.write_all(&buf)?;
        file.sync_data()?;
        Ok(())
    }

    /// Write multiple WAL entries without fsync.
    pub fn log_batch_no_sync(&self, entries: &[WalEntry]) -> Result<()> {
        let mut buf = Vec::new();
        for entry in entries {
            buf.extend_from_slice(&self.encode_record(entry)?);
        }
        let mut file = self.inner.lock();
        file.seek(SeekFrom::End(0))?;
        file.write_all(&buf)?;
        Ok(())
    }

    /// Write multiple insert entries without fsync, avoiding doc_bytes clones.
    pub fn log_batch_inserts_no_sync(&self, entries: &[(u64, &[u8])]) -> Result<()> {
        self.log_batch_inserts_no_sync_buffered(entries)
    }

    /// Write multiple insert entries without fsync using a single write_all call.
    /// Builds the entire batch into one buffer, reducing syscalls from 3*N to 1.
    pub fn log_batch_inserts_no_sync_buffered(&self, entries: &[(u64, &[u8])]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut buf = Vec::new();
        for &(doc_id, doc_bytes) in entries {
            buf.extend_from_slice(&self.encode_insert_record(doc_id, doc_bytes)?);
        }
        let mut file = self.inner.lock();
        file.seek(SeekFrom::End(0))?;
        file.write_all(&buf)?;
        Ok(())
    }

    /// fsync the WAL file without writing anything else.
    /// Used after the *_no_sync batch paths when the caller wants to
    /// finalize durability for a group of writes.
    pub fn sync(&self) -> Result<()> {
        let file = self.inner.lock();
        file.sync_data()?;
        Ok(())
    }

    /// Truncate the WAL to 0 (checkpoint), then fsync.
    pub fn checkpoint(&self) -> Result<()> {
        let file = self.inner.lock();
        file.set_len(0)?;
        file.sync_data()?;
        Ok(())
    }

    /// Truncate the WAL to 0 without fsync.
    pub fn checkpoint_no_sync(&self) -> Result<()> {
        let file = self.inner.lock();
        file.set_len(0)?;
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
            vlog(&format!("[verbose] WAL: {} entries to replay", entries.len()));
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
                WalEntry::Insert { doc_id, doc_bytes, .. } => {
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
                    let loc = storage.append(&doc_bytes)?;
                    primary_index.insert(doc_id, loc);
                    if doc_id >= *next_id {
                        *next_id = doc_id + 1;
                    }
                    inserts += 1;
                }
                WalEntry::Update { doc_id, doc_bytes, .. } => {
                    // Remove old values from indexes before updating
                    if let Some(&old_loc) = primary_index.get(&doc_id) {
                        if let Ok(old_doc) = crate::codec::decode_doc(&storage.read(old_loc)?) {
                            for idx in field_indexes.values_mut() {
                                idx.remove_value(doc_id, &old_doc);
                            }
                            for idx in composite_indexes.iter_mut() {
                                idx.remove_value(doc_id, &old_doc);
                            }
                        }
                        // Read current doc bytes; if different, apply update
                        let current_bytes = storage.read(old_loc)?;
                        if current_bytes != doc_bytes {
                            let new_loc = storage.append(&doc_bytes)?;
                            storage.mark_deleted(old_loc)?;
                            primary_index.insert(doc_id, new_loc);
                        }
                    }
                    // Update version_index and indexes from the new doc bytes
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
                        storage.mark_deleted(loc)?;
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

    /// Build one on-disk record — `[crc: u32 LE][len: u32 LE][payload]` —
    /// for `entry`. When an archive sequencer is attached this allocates a
    /// GSN + wall-clock and emits the v2 payload; otherwise v1.
    fn encode_record(&self, entry: &WalEntry) -> Result<Vec<u8>> {
        let meta = match &self.sequencer {
            Some(seq) => Some(seq.next()?),
            None => None,
        };
        Ok(Self::frame(self.serialize_entry(entry, meta)?))
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
        payload.push(if meta.is_some() { OP_INSERT_V2 } else { OP_INSERT });
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
            WalEntry::Insert { doc_id, doc_bytes, tx_id }
            | WalEntry::Update { doc_id, doc_bytes, tx_id } => {
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
                payload.push(if meta.is_some() { OP_DELETE_V2 } else { OP_DELETE });
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
        Ok(self.scan()?.into_iter().map(|r| r.entry).collect())
    }

    /// Read all valid records from the WAL, preserving v2 metadata.
    pub fn read_records(&self) -> Result<Vec<WalRecord>> {
        self.scan()
    }

    /// Scan the WAL front-to-back, CRC-verifying each record and stopping
    /// at the first torn/corrupt one (treating it as the crash boundary).
    fn scan(&self) -> Result<Vec<WalRecord>> {
        let mut file = self.inner.lock();
        file.seek(SeekFrom::Start(0))?;
        let file_len = file.metadata()?.len();
        let mut records = Vec::new();
        let mut pos = 0u64;

        while pos + 8 <= file_len {
            // Read header: crc32 (4) + payload_len (4)
            let mut header = [0u8; 8];
            if file.read_exact(&mut header).is_err() {
                if pos > 0 {
                    eprintln!("[wal] truncated header at offset {pos}, stopping replay ({} entries recovered)", records.len());
                }
                break;
            }

            let stored_crc = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
            let payload_len =
                u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as usize;

            if pos + 8 + payload_len as u64 > file_len {
                eprintln!("[wal] truncated payload at offset {pos} (need {} bytes, file has {}), stopping replay", payload_len, file_len - pos - 8);
                break;
            }

            let mut payload = vec![0u8; payload_len];
            if file.read_exact(&mut payload).is_err() {
                break; // Read error, stop
            }

            // Verify CRC
            let computed_crc = Self::compute_crc(&payload);
            if stored_crc != computed_crc {
                eprintln!("[wal] CRC mismatch at offset {pos}: stored={stored_crc:#010x} computed={computed_crc:#010x}, stopping replay ({} entries recovered)", records.len());
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
            (WalMeta { gsn, wall_clock_micros }, 33)
        } else {
            (WalMeta::default(), 17)
        };

        let entry = match op_type {
            OP_INSERT | OP_INSERT_V2 => {
                let doc_bytes = self.maybe_decrypt(&payload[body_start..]).ok()?;
                WalEntry::Insert { doc_id, doc_bytes, tx_id }
            }
            OP_UPDATE | OP_UPDATE_V2 => {
                let doc_bytes = self.maybe_decrypt(&payload[body_start..]).ok()?;
                WalEntry::Update { doc_id, doc_bytes, tx_id }
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
            WalEntry::Insert { doc_id, doc_bytes, tx_id } => {
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
            WalEntry::Update { doc_id, doc_bytes, tx_id } => {
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
        wal.log(&WalEntry::insert(2, b"will_corrupt".to_vec())).unwrap();
        wal.log(&WalEntry::insert(3, b"after_corrupt".to_vec())).unwrap();

        // Corrupt the CRC of the second entry
        let mut file_data = std::fs::read(&wal_path).unwrap();
        // First entry: 8 header + payload, then second starts
        // Find the second entry's offset: parse first entry length
        let first_payload_len = u32::from_le_bytes([
            file_data[4], file_data[5], file_data[6], file_data[7],
        ]) as usize;
        let second_offset = 8 + first_payload_len;
        // Corrupt the CRC bytes of the second entry
        file_data[second_offset] ^= 0xFF;
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
        wal.recover(&storage, &mut primary_index, &dc, &mut next_id, &committed, &mut version_index, &mut fi, &mut ci, false, &None)
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

        wal.recover(&storage, &mut primary_index, &dc, &mut next_id, &committed, &mut version_index, &mut fi, &mut ci, false, &None)
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

        wal.recover(&storage, &mut primary_index, &dc, &mut next_id, &committed, &mut version_index, &mut fi, &mut ci, false, &None)
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

        wal.recover(&storage, &mut primary_index, &dc, &mut next_id, &committed, &mut version_index, &mut fi, &mut ci, false, &None)
            .unwrap();

        assert!(primary_index.is_empty());
    }

    #[test]
    fn encrypted_wal_roundtrip() {
        let dir = TempDir::new().unwrap();
        let key_path = dir.path().join("test.key");
        std::fs::write(&key_path, &[0x42u8; 32]).unwrap();
        let enc_key = crate::crypto::EncryptionKey::load_from_file(&key_path).unwrap();

        let wal = Wal::open_with_encryption(
            &dir.path().join("encrypted.wal"),
            Some(enc_key),
        )
        .unwrap();

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

        wal.log_no_sync(&WalEntry::insert(1, b"a".to_vec())).unwrap();
        wal.log_batch_no_sync(&[
            WalEntry::insert(2, b"b".to_vec()),
            WalEntry::insert(3, b"c".to_vec()),
        ]).unwrap();

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

        let meta = WalMeta { gsn: 7, wall_clock_micros: 1_700_000_000_000_000 };
        wal.log_with_meta(&WalEntry::insert(3, b"v2_doc".to_vec()), meta).unwrap();
        wal.log_with_meta(&WalEntry::delete(3), WalMeta { gsn: 8, wall_clock_micros: 1_700_000_000_000_001 }).unwrap();

        let records = wal.read_records().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].meta, meta);
        match &records[0].entry {
            WalEntry::Insert { doc_id, doc_bytes, tx_id } => {
                assert_eq!(*doc_id, 3);
                assert_eq!(doc_bytes, b"v2_doc");
                assert_eq!(*tx_id, 0);
            }
            _ => panic!("expected Insert"),
        }
        assert_eq!(records[1].meta.gsn, 8);
        assert!(matches!(records[1].entry, WalEntry::Delete { doc_id: 3, .. }));
    }

    #[test]
    fn read_entries_drops_v2_meta_but_keeps_entry() {
        let dir = TempDir::new().unwrap();
        let wal = test_wal(&dir);

        wal.log_with_meta(
            &WalEntry::Update { doc_id: 9, doc_bytes: b"x".to_vec(), tx_id: 42 },
            WalMeta { gsn: 100, wall_clock_micros: 5 },
        ).unwrap();

        // The legacy replay path still sees the entry, just without meta.
        let entries = wal.read_entries().unwrap();
        assert_eq!(entries.len(), 1);
        match &entries[0] {
            WalEntry::Update { doc_id, doc_bytes, tx_id } => {
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
        wal.log_with_meta(&WalEntry::insert(2, b"new".to_vec()), WalMeta { gsn: 1, wall_clock_micros: 11 }).unwrap();
        wal.log(&WalEntry::delete(1)).unwrap();
        wal.log_with_meta(&WalEntry::delete(2), WalMeta { gsn: 2, wall_clock_micros: 22 }).unwrap();

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

        let meta = WalMeta { gsn: 55, wall_clock_micros: 999 };
        wal.log_with_meta(&WalEntry::insert(1, b"secret_v2".to_vec()), meta).unwrap();

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

        wal.log_with_meta(&WalEntry::insert(1, b"good".to_vec()), WalMeta { gsn: 1, wall_clock_micros: 1 }).unwrap();
        wal.log_with_meta(&WalEntry::insert(2, b"corrupt_me".to_vec()), WalMeta { gsn: 2, wall_clock_micros: 2 }).unwrap();

        // Corrupt the CRC of the second record.
        let mut bytes = std::fs::read(&wal_path).unwrap();
        let first_len = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]) as usize;
        let second_off = 8 + first_len;
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
        let wal = Wal::open(&dir.path().join("seq.wal")).unwrap().with_sequencer(Some(seq));

        // Exercise the single, batch, and no-clone-bulk-insert log paths.
        wal.log(&WalEntry::insert(1, b"a".to_vec())).unwrap();
        wal.log_batch(&[WalEntry::update(1, b"b".to_vec()), WalEntry::delete(1)]).unwrap();
        wal.log_batch_inserts_no_sync_buffered(&[(2, b"c".as_slice())]).unwrap();

        let records = wal.read_records().unwrap();
        assert_eq!(records.len(), 4);
        // Every record carries a unique, strictly increasing, non-zero GSN.
        assert_eq!(records.iter().map(|r| r.meta.gsn).collect::<Vec<_>>(), vec![1, 2, 3, 4]);
        for r in &records {
            assert!(r.meta.wall_clock_micros > 0, "v2 records must carry a wall-clock");
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
}
