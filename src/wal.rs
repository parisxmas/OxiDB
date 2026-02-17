use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crc32fast::Hasher;

use crate::crypto::EncryptionKey;
use crate::document::DocumentId;
use crate::error::Result;
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
    pub fn recover(
        &self,
        storage: &Storage,
        primary_index: &mut HashMap<DocumentId, DocLocation>,
        next_id: &mut DocumentId,
        committed_tx_ids: &HashSet<u64>,
        version_index: &mut HashMap<DocumentId, u64>,
    ) -> Result<()> {
        let entries = self.read_entries()?;

        for entry in entries {
            // Skip uncommitted transactional entries
            let tx_id = entry.tx_id();
            if tx_id != 0 && !committed_tx_ids.contains(&tx_id) {
                continue;
            }

            match entry {
                WalEntry::Insert { doc_id, doc_bytes, .. } => {
                    // Skip if already present in primary_index
                    if primary_index.contains_key(&doc_id) {
                        continue;
                    }
                    // Read _version from the doc bytes
                    if let Ok(doc) = serde_json::from_slice::<serde_json::Value>(&doc_bytes) {
                        let ver = doc.get("_version").and_then(|v| v.as_u64()).unwrap_or(0);
                        version_index.insert(doc_id, ver);
                    }
                    let loc = storage.append(&doc_bytes)?;
                    primary_index.insert(doc_id, loc);
                    if doc_id >= *next_id {
                        *next_id = doc_id + 1;
                    }
                }
                WalEntry::Update { doc_id, doc_bytes, .. } => {
                    if let Some(&old_loc) = primary_index.get(&doc_id) {
                        // Read current doc bytes; if different, apply update
                        let current_bytes = storage.read(old_loc)?;
                        if current_bytes != doc_bytes {
                            let new_loc = storage.append(&doc_bytes)?;
                            storage.mark_deleted(old_loc)?;
                            primary_index.insert(doc_id, new_loc);
                        }
                    }
                    // Update version_index from the new doc bytes
                    if let Ok(doc) = serde_json::from_slice::<serde_json::Value>(&doc_bytes) {
                        let ver = doc.get("_version").and_then(|v| v.as_u64()).unwrap_or(0);
                        version_index.insert(doc_id, ver);
                    }
                }
                WalEntry::Delete { doc_id, .. } => {
                    if let Some(&loc) = primary_index.get(&doc_id) {
                        storage.mark_deleted(loc)?;
                        primary_index.remove(&doc_id);
                    }
                    version_index.remove(&doc_id);
                }
            }
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
