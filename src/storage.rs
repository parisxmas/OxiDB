use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

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
/// Record format: [status: u8][length: u32 LE][json_bytes]
/// - status 0 = active, 1 = deleted (soft delete)
///
/// Thread-safe: all file operations are serialized via an internal Mutex.
pub struct Storage {
    _path: PathBuf,
    inner: Mutex<StorageInner>,
}

impl Storage {
    pub fn open(path: &Path) -> Result<Self> {
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
        })
    }

    /// Append a document to the data file, returns its location.
    pub fn append(&self, doc_bytes: &[u8]) -> Result<DocLocation> {
        let mut inner = self.inner.lock().unwrap();
        let offset = inner.current_offset;
        let length = doc_bytes.len() as u32;

        inner.file.seek(SeekFrom::End(0))?;
        inner.file.write_all(&[RECORD_ACTIVE])?;
        inner.file.write_all(&length.to_le_bytes())?;
        inner.file.write_all(doc_bytes)?;
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
        Ok(buf)
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
        let mut inner = self.inner.lock().unwrap();
        let offset = inner.current_offset;
        let length = doc_bytes.len() as u32;

        inner.file.seek(SeekFrom::End(0))?;
        inner.file.write_all(&[RECORD_ACTIVE])?;
        inner.file.write_all(&length.to_le_bytes())?;
        inner.file.write_all(doc_bytes)?;

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

    /// Iterate all active records. Returns (offset, json_bytes) pairs.
    /// Used for rebuilding indexes on startup.
    pub fn iter_active(&self) -> Result<Vec<(u64, Vec<u8>)>> {
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
                results.push((pos, data));
            } else {
                inner.file.seek(SeekFrom::Current(length as i64))?;
            }

            pos += 5 + length as u64;
        }

        Ok(results)
    }
}
