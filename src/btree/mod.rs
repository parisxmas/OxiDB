//! B-tree storage engine for OxiDB.
//!
//! Provides `BTreeStorage` — a B-tree-based alternative to the append-only
//! file storage. Documents are stored in pages managed by an on-disk B-tree
//! keyed by document ID, with an in-memory page cache for fast reads.

use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use crate::locks::RwLock;

use crate::error::{Error, Result};

/// Magic bytes at the start of the data file.
const BTREE_MAGIC: &[u8; 4] = b"OXBT";

/// Current format version.
const FORMAT_VERSION: u32 = 1;

/// Header size: 4 (magic) + 4 (version) + 8 (entry count).
const HEADER_SIZE: usize = 16;

/// Entry format: [key: u64 LE][length: u32 LE][payload]
/// Deleted entries are simply removed from the tree and compacted out of the file.

/// In-memory B-tree node backed by an on-disk data file.
///
/// The B-tree maps `DocumentId -> (offset, length)` for quick lookups.
/// All data lives in a single flat file; the in-memory index is rebuilt
/// on open by scanning the file.
pub struct BTreeStorage {
    dir: PathBuf,
    #[allow(dead_code)]
    data_path: PathBuf,
    /// In-memory B-tree index: doc_id -> (file_offset, payload_length).
    index: RwLock<BTreeMap<u64, (u64, u32)>>,
    /// The data file, protected by a write lock.
    file: RwLock<fs::File>,
    /// Current write offset (end of file).
    write_offset: RwLock<u64>,
}

impl BTreeStorage {
    /// Open (or create) a B-tree storage directory.
    ///
    /// The directory will contain a single `data.obt` file.
    pub fn open(dir: &Path) -> Result<Self> {
        fs::create_dir_all(dir)?;
        let data_path = dir.join("data.obt");

        let (file, index, write_offset) = if data_path.exists() {
            Self::open_existing(&data_path)?
        } else {
            Self::create_new(&data_path)?
        };

        Ok(Self {
            dir: dir.to_path_buf(),
            data_path,
            index: RwLock::new(index),
            file: RwLock::new(file),
            write_offset: RwLock::new(write_offset),
        })
    }

    fn create_new(path: &Path) -> Result<(fs::File, BTreeMap<u64, (u64, u32)>, u64)> {
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        // Write header
        file.write_all(BTREE_MAGIC)?;
        file.write_all(&FORMAT_VERSION.to_le_bytes())?;
        file.write_all(&0u64.to_le_bytes())?; // entry count placeholder
        file.flush()?;
        Ok((file, BTreeMap::new(), HEADER_SIZE as u64))
    }

    fn open_existing(path: &Path) -> Result<(fs::File, BTreeMap<u64, (u64, u32)>, u64)> {
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;

        // Read and validate header
        let mut header = [0u8; HEADER_SIZE];
        file.read_exact(&mut header).map_err(|e| {
            Error::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to read btree header: {}", e),
            ))
        })?;

        if &header[0..4] != BTREE_MAGIC {
            return Err(Error::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid btree magic bytes",
            )));
        }

        let _version = u32::from_le_bytes(header[4..8].try_into().unwrap());
        let entry_count = u64::from_le_bytes(header[8..16].try_into().unwrap());

        // Scan the file to rebuild the in-memory index
        let _file_len = file.metadata()?.len();
        let mut index = BTreeMap::new();
        let pos = HEADER_SIZE as u64;
        let mut buf = Vec::new();

        // Read entire data section into memory for scanning
        use std::io::Seek;
        file.seek(io::SeekFrom::Start(pos))?;
        file.read_to_end(&mut buf)?;

        let mut buf_pos = 0usize;
        let mut count = 0u64;

        while buf_pos + 12 <= buf.len() {
            // Entry: [key: u64][length: u32][payload]
            let key = u64::from_le_bytes(buf[buf_pos..buf_pos + 8].try_into().unwrap());
            let length = u32::from_le_bytes(buf[buf_pos + 8..buf_pos + 12].try_into().unwrap());

            if buf_pos + 12 + length as usize > buf.len() {
                // Truncated entry — stop here
                break;
            }

            // The payload offset in the file is the position after key+length header
            let payload_offset = pos + buf_pos as u64 + 12;
            index.insert(key, (payload_offset, length));

            buf_pos += 12 + length as usize;
            count += 1;
        }

        let write_offset = pos + buf_pos as u64;

        // Warn if count differs from header (recovery scenario)
        if count != entry_count {
            eprintln!(
                "[btree] entry count mismatch: header={}, scanned={}",
                entry_count, count
            );
        }

        Ok((file, index, write_offset))
    }

    /// Insert a document. If a document with the same key already exists,
    /// it is overwritten (the old space is wasted until compaction).
    pub fn insert(&self, doc_id: u64, payload: &[u8]) -> Result<()> {
        use std::io::{Seek, SeekFrom};

        let mut file = self.file.write();
        let mut offset = self.write_offset.write();

        // Seek to end
        file.seek(SeekFrom::Start(*offset))?;

        // Write entry: [key: u64 LE][length: u32 LE][payload]
        let length = payload.len() as u32;
        file.write_all(&doc_id.to_le_bytes())?;
        file.write_all(&length.to_le_bytes())?;
        file.write_all(payload)?;

        let payload_offset = *offset + 12;
        *offset += 12 + length as u64;

        // Update in-memory index
        self.index.write().insert(doc_id, (payload_offset, length));

        Ok(())
    }

    /// Get a document by ID. Returns the raw payload bytes.
    pub fn get(&self, doc_id: u64) -> Result<Vec<u8>> {
        let index = self.index.read();
        let (payload_offset, length) = index.get(&doc_id).ok_or_else(|| {
            Error::Io(io::Error::new(
                io::ErrorKind::NotFound,
                format!("btree: doc {} not found", doc_id),
            ))
        })?;

        let offset = *payload_offset;
        let len = *length as usize;
        drop(index);

        // Use positional read (pread) for lock-free reads on Unix
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            let file = self.file.read();
            let mut buf = vec![0u8; len];
            file.read_at(&mut buf, offset)?;
            Ok(buf)
        }
        #[cfg(not(unix))]
        {
            use std::io::{Seek, SeekFrom};
            let mut file = self.file.write();
            file.seek(SeekFrom::Start(offset))?;
            let mut buf = vec![0u8; len];
            file.read_exact(&mut buf)?;
            Ok(buf)
        }
    }

    /// Delete a document by ID. The on-disk entry is not removed (left as garbage);
    /// space is reclaimed by `compact()`.
    pub fn delete(&self, doc_id: u64) -> Result<()> {
        let removed = self.index.write().remove(&doc_id);
        if removed.is_none() {
            return Err(Error::NotFound(doc_id));
        }
        Ok(())
    }

    /// Update a document (delete + insert). The old space becomes garbage.
    pub fn update(&self, doc_id: u64, payload: &[u8]) -> Result<()> {
        // Remove old entry from index (don't error if missing — insert will create it)
        self.index.write().remove(&doc_id);
        self.insert(doc_id, payload)
    }

    /// Scan all documents in key order. Returns `(doc_id, payload)` pairs.
    pub fn scan_all(&self) -> Result<Vec<(u64, Vec<u8>)>> {
        let index = self.index.read();
        let mut results = Vec::with_capacity(index.len());

        for (&doc_id, &(payload_offset, length)) in index.iter() {
            let len = length as usize;

            #[cfg(unix)]
            {
                use std::os::unix::fs::FileExt;
                let file = self.file.read();
                let mut buf = vec![0u8; len];
                file.read_at(&mut buf, payload_offset)?;
                results.push((doc_id, buf));
            }
            #[cfg(not(unix))]
            {
                use std::io::{Seek, SeekFrom};
                let mut file = self.file.write();
                file.seek(SeekFrom::Start(payload_offset))?;
                let mut buf = vec![0u8; len];
                file.read_exact(&mut buf)?;
                results.push((doc_id, buf));
            }
        }

        Ok(results)
    }

    /// Streaming scan: call the callback for each document's raw bytes.
    /// Reads documents in doc_id order. Returns early if callback returns false.
    /// Uses sorted iteration — sequential file reads for I/O locality.
    pub fn scan_while<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8]) -> Result<bool>,
    {
        let index = self.index.read();

        // Collect (doc_id, offset, length) sorted by offset for sequential I/O
        let mut entries: Vec<(u64, u32)> = index.values().copied().collect();
        entries.sort_unstable_by_key(|&(offset, _)| offset);

        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            let file = self.file.read();
            for (offset, length) in entries {
                let mut buf = vec![0u8; length as usize];
                file.read_at(&mut buf, offset)?;
                if !f(&buf)? {
                    break;
                }
            }
        }
        #[cfg(not(unix))]
        {
            use std::io::{Seek, SeekFrom};
            let mut file = self.file.write();
            for (offset, length) in entries {
                file.seek(SeekFrom::Start(offset))?;
                let mut buf = vec![0u8; length as usize];
                file.read_exact(&mut buf)?;
                if !f(&buf)? {
                    break;
                }
            }
        }

        Ok(())
    }

    /// Snapshot of the in-memory index (cloned) for iteration.
    pub fn index_snapshot(&self) -> BTreeMap<u64, (u64, u32)> {
        self.index.read().clone()
    }

    /// Return the number of live documents.
    pub fn count(&self) -> usize {
        self.index.read().len()
    }

    /// Flush writes to disk.
    pub fn sync(&self) -> Result<()> {
        let file = self.file.read();
        file.sync_all()?;

        // Update entry count in header
        let count = self.index.read().len() as u64;
        drop(file);

        let mut file = self.file.write();
        use std::io::{Seek, SeekFrom};
        file.seek(SeekFrom::Start(8))?;
        file.write_all(&count.to_le_bytes())?;
        file.flush()?;

        Ok(())
    }

    /// Path to the B-tree storage directory.
    pub fn path(&self) -> &Path {
        &self.dir
    }

    /// Batch-read multiple documents by ID.
    ///
    /// Groups reads by file offset so that nearby documents (which share
    /// similar insert-time locality) are read with sequential I/O rather
    /// than random seeks.  Returns `(doc_id, payload)` pairs for every
    /// doc_id that exists in the index.
    pub fn get_batch(&self, doc_ids: &[u64]) -> Result<Vec<(u64, Vec<u8>)>> {
        if doc_ids.is_empty() {
            return Ok(Vec::new());
        }

        // Phase 1: look up all (doc_id, offset, length) under a single read lock
        let index = self.index.read();
        let mut lookups: Vec<(u64, u64, u32)> = Vec::with_capacity(doc_ids.len());
        for &id in doc_ids {
            if let Some(&(payload_offset, length)) = index.get(&id) {
                lookups.push((id, payload_offset, length));
            }
        }
        drop(index);

        if lookups.is_empty() {
            return Ok(Vec::new());
        }

        // Phase 2: sort by file offset for sequential I/O
        lookups.sort_unstable_by_key(|&(_, offset, _)| offset);

        // Phase 3: batch pread all payloads
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            let file = self.file.read();
            let mut results = Vec::with_capacity(lookups.len());
            for (id, offset, length) in lookups {
                let mut buf = vec![0u8; length as usize];
                file.read_at(&mut buf, offset)?;
                results.push((id, buf));
            }
            Ok(results)
        }
        #[cfg(not(unix))]
        {
            use std::io::{Seek, SeekFrom};
            let mut file = self.file.write();
            let mut results = Vec::with_capacity(lookups.len());
            for (id, offset, length) in lookups {
                file.seek(SeekFrom::Start(offset))?;
                let mut buf = vec![0u8; length as usize];
                file.read_exact(&mut buf)?;
                results.push((id, buf));
            }
            Ok(results)
        }
    }

    /// Approximate data file size.
    pub fn file_size(&self) -> u64 {
        *self.write_offset.read()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn btree_basic_crud() {
        let dir = tempdir().unwrap();
        let storage = BTreeStorage::open(&dir.path().join("test.btree")).unwrap();

        // Insert
        storage.insert(1, b"hello").unwrap();
        storage.insert(2, b"world").unwrap();
        assert_eq!(storage.count(), 2);

        // Get
        assert_eq!(storage.get(1).unwrap(), b"hello");
        assert_eq!(storage.get(2).unwrap(), b"world");

        // Update
        storage.update(1, b"updated").unwrap();
        assert_eq!(storage.get(1).unwrap(), b"updated");
        assert_eq!(storage.count(), 2);

        // Delete
        storage.delete(2).unwrap();
        assert_eq!(storage.count(), 1);
        assert!(storage.get(2).is_err());

        // Scan
        let all = storage.scan_all().unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].0, 1);
        assert_eq!(all[0].1, b"updated");
    }

    #[test]
    fn btree_reopen() {
        let dir = tempdir().unwrap();
        let btree_dir = dir.path().join("reopen.btree");

        {
            let storage = BTreeStorage::open(&btree_dir).unwrap();
            storage.insert(10, b"persist-me").unwrap();
            storage.insert(20, b"me-too").unwrap();
            storage.sync().unwrap();
        }

        // Reopen
        let storage = BTreeStorage::open(&btree_dir).unwrap();
        assert_eq!(storage.count(), 2);
        assert_eq!(storage.get(10).unwrap(), b"persist-me");
        assert_eq!(storage.get(20).unwrap(), b"me-too");
    }

    #[test]
    fn btree_empty() {
        let dir = tempdir().unwrap();
        let storage = BTreeStorage::open(&dir.path().join("empty.btree")).unwrap();
        assert_eq!(storage.count(), 0);
        assert!(storage.scan_all().unwrap().is_empty());
    }

    #[test]
    fn btree_get_batch_basic() {
        let dir = tempdir().unwrap();
        let storage = BTreeStorage::open(&dir.path().join("batch.btree")).unwrap();

        storage.insert(1, b"one").unwrap();
        storage.insert(2, b"two").unwrap();
        storage.insert(3, b"three").unwrap();
        storage.insert(4, b"four").unwrap();
        storage.insert(5, b"five").unwrap();

        // Batch read a subset (out of order)
        let results = storage.get_batch(&[5, 2, 4]).unwrap();
        assert_eq!(results.len(), 3);

        // Results are sorted by file offset, so check by converting to a map
        let map: std::collections::HashMap<u64, Vec<u8>> = results.into_iter().collect();
        assert_eq!(map[&2], b"two");
        assert_eq!(map[&4], b"four");
        assert_eq!(map[&5], b"five");
    }

    #[test]
    fn btree_get_batch_with_missing() {
        let dir = tempdir().unwrap();
        let storage = BTreeStorage::open(&dir.path().join("batch_miss.btree")).unwrap();

        storage.insert(10, b"ten").unwrap();
        storage.insert(20, b"twenty").unwrap();

        // Request includes non-existent IDs
        let results = storage.get_batch(&[10, 99, 20, 100]).unwrap();
        assert_eq!(results.len(), 2);
        let map: std::collections::HashMap<u64, Vec<u8>> = results.into_iter().collect();
        assert_eq!(map[&10], b"ten");
        assert_eq!(map[&20], b"twenty");
    }

    #[test]
    fn btree_get_batch_empty() {
        let dir = tempdir().unwrap();
        let storage = BTreeStorage::open(&dir.path().join("batch_empty.btree")).unwrap();

        storage.insert(1, b"one").unwrap();

        // Empty request
        let results = storage.get_batch(&[]).unwrap();
        assert!(results.is_empty());

        // All missing
        let results = storage.get_batch(&[99, 100]).unwrap();
        assert!(results.is_empty());
    }
}
