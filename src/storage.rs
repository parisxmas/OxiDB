use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::crypto::EncryptionKey;
use crate::error::Result;

const RECORD_ACTIVE: u8 = 0;
const RECORD_DELETED: u8 = 1;

/// Location of a document in the data file.
#[derive(Debug, Clone, Copy)]
pub struct DocLocation {
    pub offset: u64,
    pub length: u32,
}

struct StorageInner {
    file: File,
    current_offset: u64,
}

/// Append-only file storage for documents.
///
/// Record format: [status: u8][length: u32 LE][payload]
/// - status 0 = active, 1 = deleted (soft delete)
/// - payload is either raw json_bytes or encrypted bytes
///
/// Thread-safe: all file operations are serialized via an internal Mutex.
pub struct Storage {
    _path: PathBuf,
    inner: Mutex<StorageInner>,
    encryption: Option<Arc<EncryptionKey>>,
}

impl Storage {
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

        let current_offset = file.metadata()?.len();

        Ok(Self {
            _path: path.to_path_buf(),
            inner: Mutex::new(StorageInner {
                file,
                current_offset,
            }),
            encryption,
        })
    }

    /// Encrypt doc_bytes if encryption is enabled.
    fn maybe_encrypt(&self, doc_bytes: &[u8]) -> Result<Vec<u8>> {
        match &self.encryption {
            Some(key) => key.encrypt(doc_bytes),
            None => Ok(doc_bytes.to_vec()),
        }
    }

    /// Decrypt payload if encryption is enabled.
    fn maybe_decrypt(&self, payload: &[u8]) -> Result<Vec<u8>> {
        match &self.encryption {
            Some(key) => key.decrypt(payload),
            None => Ok(payload.to_vec()),
        }
    }

    /// Append a document to the data file, returns its location.
    pub fn append(&self, doc_bytes: &[u8]) -> Result<DocLocation> {
        let payload = self.maybe_encrypt(doc_bytes)?;
        let mut inner = self.inner.lock().unwrap();
        let offset = inner.current_offset;
        let length = payload.len() as u32;

        inner.file.seek(SeekFrom::End(0))?;
        inner.file.write_all(&[RECORD_ACTIVE])?;
        inner.file.write_all(&length.to_le_bytes())?;
        inner.file.write_all(&payload)?;
        inner.file.sync_data()?;

        inner.current_offset += 1 + 4 + length as u64;

        Ok(DocLocation { offset, length })
    }

    /// Read a document's bytes from the data file.
    pub fn read(&self, loc: DocLocation) -> Result<Vec<u8>> {
        let mut inner = self.inner.lock().unwrap();
        inner.file.seek(SeekFrom::Start(loc.offset + 5))?;
        let mut buf = vec![0u8; loc.length as usize];
        inner.file.read_exact(&mut buf)?;
        drop(inner);
        self.maybe_decrypt(&buf)
    }

    /// Soft-delete a record by flipping its status byte.
    pub fn mark_deleted(&self, loc: DocLocation) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.file.seek(SeekFrom::Start(loc.offset))?;
        inner.file.write_all(&[RECORD_DELETED])?;
        inner.file.sync_data()?;
        Ok(())
    }

    /// Append a document without fsync (caller must call `sync()` after batch).
    pub fn append_no_sync(&self, doc_bytes: &[u8]) -> Result<DocLocation> {
        let payload = self.maybe_encrypt(doc_bytes)?;
        let mut inner = self.inner.lock().unwrap();
        let offset = inner.current_offset;
        let length = payload.len() as u32;

        inner.file.seek(SeekFrom::End(0))?;
        inner.file.write_all(&[RECORD_ACTIVE])?;
        inner.file.write_all(&length.to_le_bytes())?;
        inner.file.write_all(&payload)?;

        inner.current_offset += 1 + 4 + length as u64;

        Ok(DocLocation { offset, length })
    }

    /// Soft-delete without fsync (caller must call `sync()` after batch).
    pub fn mark_deleted_no_sync(&self, loc: DocLocation) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.file.seek(SeekFrom::Start(loc.offset))?;
        inner.file.write_all(&[RECORD_DELETED])?;
        Ok(())
    }

    /// Flush and fsync the data file.
    pub fn sync(&self) -> Result<()> {
        let inner = self.inner.lock().unwrap();
        inner.file.sync_data()?;
        Ok(())
    }

    /// Returns the total file size in bytes.
    pub fn file_size(&self) -> u64 {
        let inner = self.inner.lock().unwrap();
        inner.current_offset
    }

    /// Returns the path this storage was opened with.
    pub fn path(&self) -> &Path {
        &self._path
    }

    /// Iterate all active records. Returns (DocLocation, plaintext_bytes) pairs.
    /// The DocLocation contains the correct on-disk payload length (which may
    /// differ from plaintext length when encryption is enabled).
    pub fn iter_active(&self) -> Result<Vec<(DocLocation, Vec<u8>)>> {
        let mut inner = self.inner.lock().unwrap();
        inner.file.seek(SeekFrom::Start(0))?;
        let file_len = inner.file.metadata()?.len();
        let mut results = Vec::new();
        let mut pos = 0u64;

        while pos < file_len {
            let mut header = [0u8; 5];
            inner.file.read_exact(&mut header)?;

            let status = header[0];
            let length = u32::from_le_bytes([header[1], header[2], header[3], header[4]]);

            if status == RECORD_ACTIVE {
                let mut data = vec![0u8; length as usize];
                inner.file.read_exact(&mut data)?;
                let plaintext = match &self.encryption {
                    Some(key) => key.decrypt(&data)?,
                    None => data,
                };
                results.push((DocLocation { offset: pos, length }, plaintext));
            } else {
                inner.file.seek(SeekFrom::Current(length as i64))?;
            }

            pos += 5 + length as u64;
        }

        Ok(results)
    }

    /// Stream active records one at a time via callback, avoiding the large
    /// Vec allocation of `iter_active`.
    pub fn for_each_active<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(DocLocation, Vec<u8>) -> Result<()>,
    {
        let mut inner = self.inner.lock().unwrap();
        inner.file.seek(SeekFrom::Start(0))?;
        let file_len = inner.file.metadata()?.len();
        let mut pos = 0u64;

        while pos < file_len {
            let mut header = [0u8; 5];
            inner.file.read_exact(&mut header)?;

            let status = header[0];
            let length = u32::from_le_bytes([header[1], header[2], header[3], header[4]]);

            if status == RECORD_ACTIVE {
                let mut data = vec![0u8; length as usize];
                inner.file.read_exact(&mut data)?;
                let plaintext = match &self.encryption {
                    Some(key) => key.decrypt(&data)?,
                    None => data,
                };
                // Drop inner lock before callback (callback may need to read storage)
                drop(inner);
                f(DocLocation { offset: pos, length }, plaintext)?;
                inner = self.inner.lock().unwrap();
                // Re-seek to continue after this record
                inner.file.seek(SeekFrom::Start(pos + 5 + length as u64))?;
            } else {
                inner.file.seek(SeekFrom::Current(length as i64))?;
            }

            pos += 5 + length as u64;
        }

        Ok(())
    }
    /// Sequential scan using a separate read-only file handle.
    /// Does NOT hold the main mutex — other reads/writes can proceed concurrently.
    /// Uses BufReader for efficient sequential I/O (OS read-ahead).
    /// The callback receives raw (decrypted) bytes and returns Ok(true) to continue
    /// or Ok(false) to stop early.
    pub fn scan_readonly_while<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8]) -> Result<bool>,
    {
        use std::io::BufReader;

        let file = File::open(&self._path)?;
        let file_len = file.metadata()?.len();
        let mut reader = BufReader::with_capacity(256 * 1024, file);
        let mut pos = 0u64;
        let mut buf = Vec::with_capacity(4096);
        let mut decrypt_buf: Vec<u8>;

        while pos < file_len {
            let mut header = [0u8; 5];
            reader.read_exact(&mut header)?;
            let status = header[0];
            let length =
                u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;

            if status == RECORD_ACTIVE {
                buf.resize(length, 0);
                reader.read_exact(&mut buf)?;
                let bytes: &[u8] = match &self.encryption {
                    Some(key) => {
                        decrypt_buf = key.decrypt(&buf)?;
                        &decrypt_buf
                    }
                    None => &buf,
                };
                if !f(bytes)? {
                    break;
                }
            } else {
                reader.seek(SeekFrom::Current(length as i64))?;
            }

            pos += 5 + length as u64;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_storage(dir: &TempDir) -> Storage {
        Storage::open(&dir.path().join("test.dat")).unwrap()
    }

    #[test]
    fn append_and_read_roundtrip() {
        let dir = TempDir::new().unwrap();
        let storage = test_storage(&dir);
        let data = b"hello world";
        let loc = storage.append(data).unwrap();
        let read_back = storage.read(loc).unwrap();
        assert_eq!(read_back, data);
    }

    #[test]
    fn append_multiple_records() {
        let dir = TempDir::new().unwrap();
        let storage = test_storage(&dir);
        let loc1 = storage.append(b"first").unwrap();
        let loc2 = storage.append(b"second").unwrap();
        let loc3 = storage.append(b"third").unwrap();

        assert_eq!(storage.read(loc1).unwrap(), b"first");
        assert_eq!(storage.read(loc2).unwrap(), b"second");
        assert_eq!(storage.read(loc3).unwrap(), b"third");
        assert_ne!(loc1.offset, loc2.offset);
        assert_ne!(loc2.offset, loc3.offset);
    }

    #[test]
    fn soft_delete_hides_record() {
        let dir = TempDir::new().unwrap();
        let storage = test_storage(&dir);
        let loc1 = storage.append(b"keep").unwrap();
        let loc2 = storage.append(b"delete_me").unwrap();
        let loc3 = storage.append(b"also_keep").unwrap();

        storage.mark_deleted(loc2).unwrap();

        let active = storage.iter_active().unwrap();
        assert_eq!(active.len(), 2);
        assert_eq!(active[0].1, b"keep");
        assert_eq!(active[1].1, b"also_keep");

        // Deleted record's bytes are still readable by direct offset
        let raw = storage.read(loc2).unwrap();
        assert_eq!(raw, b"delete_me");

        // But loc1 and loc3 are still fine
        assert_eq!(storage.read(loc1).unwrap(), b"keep");
        assert_eq!(storage.read(loc3).unwrap(), b"also_keep");
    }

    #[test]
    fn file_size_grows_correctly() {
        let dir = TempDir::new().unwrap();
        let storage = test_storage(&dir);
        assert_eq!(storage.file_size(), 0);

        let data = b"test";
        storage.append(data).unwrap();
        // header (1 status + 4 length) + payload
        assert_eq!(storage.file_size(), 5 + data.len() as u64);
    }

    #[test]
    fn iter_active_on_empty_file() {
        let dir = TempDir::new().unwrap();
        let storage = test_storage(&dir);
        let active = storage.iter_active().unwrap();
        assert!(active.is_empty());
    }

    #[test]
    fn append_no_sync_and_manual_sync() {
        let dir = TempDir::new().unwrap();
        let storage = test_storage(&dir);

        let loc1 = storage.append_no_sync(b"batch1").unwrap();
        let loc2 = storage.append_no_sync(b"batch2").unwrap();
        storage.sync().unwrap();

        assert_eq!(storage.read(loc1).unwrap(), b"batch1");
        assert_eq!(storage.read(loc2).unwrap(), b"batch2");
    }

    #[test]
    fn mark_deleted_no_sync() {
        let dir = TempDir::new().unwrap();
        let storage = test_storage(&dir);

        let loc = storage.append(b"will_delete").unwrap();
        storage.mark_deleted_no_sync(loc).unwrap();
        storage.sync().unwrap();

        let active = storage.iter_active().unwrap();
        assert!(active.is_empty());
    }

    #[test]
    fn encrypted_storage_roundtrip() {
        let dir = TempDir::new().unwrap();
        let key_path = dir.path().join("test.key");
        std::fs::write(&key_path, &[0x42u8; 32]).unwrap();
        let enc_key = EncryptionKey::load_from_file(&key_path).unwrap();

        let storage = Storage::open_with_encryption(
            &dir.path().join("encrypted.dat"),
            Some(enc_key),
        )
        .unwrap();

        let data = b"secret document payload";
        let loc = storage.append(data).unwrap();
        let read_back = storage.read(loc).unwrap();
        assert_eq!(read_back, data);
    }

    #[test]
    fn encrypted_data_not_plaintext() {
        let dir = TempDir::new().unwrap();
        let key_path = dir.path().join("test.key");
        std::fs::write(&key_path, &[0x42u8; 32]).unwrap();
        let enc_key = EncryptionKey::load_from_file(&key_path).unwrap();

        let data_path = dir.path().join("encrypted.dat");
        let storage = Storage::open_with_encryption(&data_path, Some(enc_key)).unwrap();

        let data = b"secret document payload";
        storage.append(data).unwrap();

        // Read raw file and verify plaintext is not visible
        let raw = std::fs::read(&data_path).unwrap();
        assert!(!raw.windows(data.len()).any(|w| w == data));
    }

    #[test]
    fn encrypted_iter_active() {
        let dir = TempDir::new().unwrap();
        let key_path = dir.path().join("test.key");
        std::fs::write(&key_path, &[0x42u8; 32]).unwrap();
        let enc_key = EncryptionKey::load_from_file(&key_path).unwrap();

        let storage = Storage::open_with_encryption(
            &dir.path().join("encrypted.dat"),
            Some(enc_key),
        )
        .unwrap();

        storage.append(b"doc_a").unwrap();
        let loc_b = storage.append(b"doc_b").unwrap();
        storage.append(b"doc_c").unwrap();

        storage.mark_deleted(loc_b).unwrap();

        let active = storage.iter_active().unwrap();
        assert_eq!(active.len(), 2);
        assert_eq!(active[0].1, b"doc_a");
        assert_eq!(active[1].1, b"doc_c");
    }

    #[test]
    fn reopen_preserves_data() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("persist.dat");

        let loc;
        {
            let storage = Storage::open(&path).unwrap();
            loc = storage.append(b"persistent").unwrap();
        }

        // Reopen
        let storage = Storage::open(&path).unwrap();
        let data = storage.read(loc).unwrap();
        assert_eq!(data, b"persistent");
        let active = storage.iter_active().unwrap();
        assert_eq!(active.len(), 1);
    }

    #[test]
    fn large_payload() {
        let dir = TempDir::new().unwrap();
        let storage = test_storage(&dir);
        let data = vec![0xABu8; 100_000];
        let loc = storage.append(&data).unwrap();
        assert_eq!(storage.read(loc).unwrap(), data);
    }
}
