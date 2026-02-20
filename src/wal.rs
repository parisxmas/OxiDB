use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crc32fast::Hasher;

use crate::crypto::EncryptionKey;
use crate::document::DocumentId;
use crate::engine::LogCallback;
use crate::error::Result;
use crate::index::{CompositeIndex, FieldIndex};
use crate::storage::{DocLocation, Storage};

const OP_INSERT: u8 = 1;
const OP_UPDATE: u8 = 2;
const OP_DELETE: u8 = 3;

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

/// Write-ahead log for crash-safe mutations.
///
/// Thread-safe: all file operations are serialized via an internal Mutex.
pub struct Wal {
    inner: Mutex<File>,
    path: PathBuf,
    encryption: Option<Arc<EncryptionKey>>,
}

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
        })
    }

    /// Serialize and append a WAL entry, then fsync.
    pub fn log(&self, entry: &WalEntry) -> Result<()> {
        let payload = self.serialize_entry(entry)?;
        let crc = Self::compute_crc(&payload);

        let mut file = self.inner.lock().unwrap();
        file.seek(SeekFrom::End(0))?;
        file.write_all(&crc.to_le_bytes())?;
        file.write_all(&(payload.len() as u32).to_le_bytes())?;
        file.write_all(&payload)?;
        file.sync_data()?;

        Ok(())
    }

    /// Serialize and append a WAL entry without fsync.
    pub fn log_no_sync(&self, entry: &WalEntry) -> Result<()> {
        let payload = self.serialize_entry(entry)?;
        let crc = Self::compute_crc(&payload);

        let mut file = self.inner.lock().unwrap();
        file.seek(SeekFrom::End(0))?;
        file.write_all(&crc.to_le_bytes())?;
        file.write_all(&(payload.len() as u32).to_le_bytes())?;
        file.write_all(&payload)?;

        Ok(())
    }

    /// Write multiple WAL entries with a single fsync.
    pub fn log_batch(&self, entries: &[WalEntry]) -> Result<()> {
        let mut file = self.inner.lock().unwrap();
        file.seek(SeekFrom::End(0))?;
        for entry in entries {
            let payload = self.serialize_entry(entry)?;
            let crc = Self::compute_crc(&payload);
            file.write_all(&crc.to_le_bytes())?;
            file.write_all(&(payload.len() as u32).to_le_bytes())?;
            file.write_all(&payload)?;
        }
        file.sync_data()?;
        Ok(())
    }

    /// Write multiple WAL entries without fsync.
    pub fn log_batch_no_sync(&self, entries: &[WalEntry]) -> Result<()> {
        let mut file = self.inner.lock().unwrap();
        file.seek(SeekFrom::End(0))?;
        for entry in entries {
            let payload = self.serialize_entry(entry)?;
            let crc = Self::compute_crc(&payload);
            file.write_all(&crc.to_le_bytes())?;
            file.write_all(&(payload.len() as u32).to_le_bytes())?;
            file.write_all(&payload)?;
        }
        Ok(())
    }

    /// Truncate the WAL to 0 (checkpoint), then fsync.
    pub fn checkpoint(&self) -> Result<()> {
        let file = self.inner.lock().unwrap();
        file.set_len(0)?;
        file.sync_data()?;
        Ok(())
    }

    /// Truncate the WAL to 0 without fsync.
    pub fn checkpoint_no_sync(&self) -> Result<()> {
        let file = self.inner.lock().unwrap();
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
        doc_cache: &mut HashMap<DocumentId, Arc<serde_json::Value>>,
        next_id: &mut DocumentId,
        committed_tx_ids: &HashSet<u64>,
        version_index: &mut HashMap<DocumentId, u64>,
        field_indexes: &mut HashMap<String, FieldIndex>,
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
                        doc_cache.insert(doc_id, Arc::new(doc));
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
                        doc_cache.insert(doc_id, Arc::new(doc));
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
                    doc_cache.remove(&doc_id);
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

    /// Payload format: [op_type: u8][tx_id: u64 LE][doc_id: u64 LE][doc_bytes...]
    /// When encryption is enabled, doc_bytes are encrypted before inclusion in payload.
    /// CRC is computed over the final (possibly encrypted) payload.
    fn serialize_entry(&self, entry: &WalEntry) -> Result<Vec<u8>> {
        match entry {
            WalEntry::Insert { doc_id, doc_bytes, tx_id } => {
                let encrypted = self.maybe_encrypt(doc_bytes)?;
                let mut payload = Vec::with_capacity(1 + 8 + 8 + encrypted.len());
                payload.push(OP_INSERT);
                payload.extend_from_slice(&tx_id.to_le_bytes());
                payload.extend_from_slice(&doc_id.to_le_bytes());
                payload.extend_from_slice(&encrypted);
                Ok(payload)
            }
            WalEntry::Update { doc_id, doc_bytes, tx_id } => {
                let encrypted = self.maybe_encrypt(doc_bytes)?;
                let mut payload = Vec::with_capacity(1 + 8 + 8 + encrypted.len());
                payload.push(OP_UPDATE);
                payload.extend_from_slice(&tx_id.to_le_bytes());
                payload.extend_from_slice(&doc_id.to_le_bytes());
                payload.extend_from_slice(&encrypted);
                Ok(payload)
            }
            WalEntry::Delete { doc_id, tx_id } => {
                let mut payload = Vec::with_capacity(1 + 8 + 8);
                payload.push(OP_DELETE);
                payload.extend_from_slice(&tx_id.to_le_bytes());
                payload.extend_from_slice(&doc_id.to_le_bytes());
                Ok(payload)
            }
        }
    }

    fn maybe_encrypt(&self, data: &[u8]) -> Result<Vec<u8>> {
        match &self.encryption {
            Some(key) => key.encrypt(data),
            None => Ok(data.to_vec()),
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

    fn read_entries(&self) -> Result<Vec<WalEntry>> {
        let mut file = self.inner.lock().unwrap();
        file.seek(SeekFrom::Start(0))?;
        let file_len = file.metadata()?.len();
        let mut entries = Vec::new();
        let mut pos = 0u64;

        while pos + 8 <= file_len {
            // Read header: crc32 (4) + payload_len (4)
            let mut header = [0u8; 8];
            if file.read_exact(&mut header).is_err() {
                break; // Truncated header, stop
            }

            let stored_crc = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
            let payload_len =
                u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as usize;

            if pos + 8 + payload_len as u64 > file_len {
                break; // Truncated payload, stop
            }

            let mut payload = vec![0u8; payload_len];
            if file.read_exact(&mut payload).is_err() {
                break; // Read error, stop
            }

            // Verify CRC
            let computed_crc = Self::compute_crc(&payload);
            if stored_crc != computed_crc {
                break; // Corrupt entry, stop replay
            }

            // Parse payload
            if let Some(entry) = self.parse_payload(&payload) {
                entries.push(entry);
            } else {
                break; // Malformed payload
            }

            pos += 8 + payload_len as u64;
        }

        Ok(entries)
    }

    /// Payload format: [op_type: u8][tx_id: u64 LE][doc_id: u64 LE][encrypted_doc_bytes...]
    fn parse_payload(&self, payload: &[u8]) -> Option<WalEntry> {
        if payload.len() < 17 {
            return None; // minimum: 1 (op) + 8 (tx_id) + 8 (doc_id)
        }

        let op_type = payload[0];
        let tx_id = u64::from_le_bytes(payload[1..9].try_into().ok()?);
        let doc_id = u64::from_le_bytes(payload[9..17].try_into().ok()?);

        match op_type {
            OP_INSERT => {
                let doc_bytes = self.maybe_decrypt(&payload[17..]).ok()?;
                Some(WalEntry::Insert { doc_id, doc_bytes, tx_id })
            }
            OP_UPDATE => {
                let doc_bytes = self.maybe_decrypt(&payload[17..]).ok()?;
                Some(WalEntry::Update { doc_id, doc_bytes, tx_id })
            }
            OP_DELETE => {
                Some(WalEntry::Delete { doc_id, tx_id })
            }
            _ => None,
        }
    }
}

#[cfg(test)]
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
        let mut dc = HashMap::new();
        wal.recover(&storage, &mut primary_index, &mut dc, &mut next_id, &committed, &mut version_index, &mut fi, &mut ci, false, &None)
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
        let mut dc = HashMap::new();

        wal.recover(&storage, &mut primary_index, &mut dc, &mut next_id, &committed, &mut version_index, &mut fi, &mut ci, false, &None)
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
        let mut dc = HashMap::new();

        wal.recover(&storage, &mut primary_index, &mut dc, &mut next_id, &committed, &mut version_index, &mut fi, &mut ci, false, &None)
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
        let mut dc = HashMap::new();

        // Now log a delete in WAL
        let wal = Wal::open(&wal_path).unwrap();
        wal.log(&WalEntry::delete(0)).unwrap();

        wal.recover(&storage, &mut primary_index, &mut dc, &mut next_id, &committed, &mut version_index, &mut fi, &mut ci, false, &None)
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
}
