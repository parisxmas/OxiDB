//! In-memory storage backends for OxiDB.
//!
//! Provides `InMemStorage` (a `Vec<u8>`-backed alternative to file-based `Storage`)
//! and backend enums (`StorageBackend`, `WalBackend`) that allow `Collection` to
//! operate transparently in either file or memory mode.

use std::sync::Mutex;

use crate::error::Result;
use crate::storage::DocLocation;
use crate::wal::WalEntry;

use std::path::{Path, PathBuf};

#[cfg(not(target_arch = "wasm32"))]
use std::collections::{HashMap, HashSet};

#[cfg(not(target_arch = "wasm32"))]
use crate::doc_cache::DocCache;
#[cfg(not(target_arch = "wasm32"))]
use crate::document::DocumentId;
#[cfg(not(target_arch = "wasm32"))]
use crate::engine::LogCallback;
#[cfg(not(target_arch = "wasm32"))]
use crate::index::CompositeIndex;
#[cfg(not(target_arch = "wasm32"))]
use crate::paged_field_index::PagedFieldIndex;
#[cfg(not(target_arch = "wasm32"))]
use crate::storage::Storage;
#[cfg(not(target_arch = "wasm32"))]
use crate::wal::Wal;

const RECORD_ACTIVE: u8 = 0;
const RECORD_DELETED: u8 = 1;

// ---------------------------------------------------------------------------
// InMemStorage
// ---------------------------------------------------------------------------

/// In-memory storage backend using a `Vec<u8>` buffer.
///
/// Uses the same record format as file-based `Storage`:
///   `[status: u8][length: u32 LE][payload]`
///
/// No compression or encryption — pure speed for volatile data.
pub struct InMemStorage {
    data: Mutex<Vec<u8>>,
    path: PathBuf, // empty; exists only for API compat with `path()`
}

impl InMemStorage {
    pub fn new() -> Self {
        Self {
            data: Mutex::new(Vec::with_capacity(1024 * 1024)), // 1 MB initial
            path: PathBuf::new(),
        }
    }

    pub fn append(&self, doc_bytes: &[u8]) -> Result<DocLocation> {
        let mut data = self.data.lock().unwrap();
        let offset = data.len() as u64;
        let length = doc_bytes.len() as u32;
        data.push(RECORD_ACTIVE);
        data.extend_from_slice(&length.to_le_bytes());
        data.extend_from_slice(doc_bytes);
        Ok(DocLocation { offset, length })
    }

    pub fn read(&self, loc: DocLocation) -> Result<Vec<u8>> {
        let data = self.data.lock().unwrap();
        let start = (loc.offset + 5) as usize;
        let end = start + loc.length as usize;
        if end > data.len() {
            return Err(crate::error::Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "read past end of memory storage",
            )));
        }
        Ok(data[start..end].to_vec())
    }

    pub fn read_lockfree(&self, loc: DocLocation) -> Result<Vec<u8>> {
        self.read(loc)
    }

    pub fn read_batch_lockfree(
        &self,
        locs: &mut [(usize, DocLocation)],
    ) -> Result<Vec<(usize, Vec<u8>)>> {
        locs.sort_unstable_by_key(|&(_, loc)| loc.offset);
        let data = self.data.lock().unwrap();
        let mut results = Vec::with_capacity(locs.len());
        for &(idx, loc) in locs.iter() {
            let start = (loc.offset + 5) as usize;
            let end = start + loc.length as usize;
            results.push((idx, data[start..end].to_vec()));
        }
        Ok(results)
    }

    pub fn mark_deleted(&self, loc: DocLocation) -> Result<()> {
        let mut data = self.data.lock().unwrap();
        data[loc.offset as usize] = RECORD_DELETED;
        Ok(())
    }

    pub fn append_no_sync(&self, doc_bytes: &[u8]) -> Result<DocLocation> {
        self.append(doc_bytes) // no sync distinction in memory
    }

    pub fn append_batch_no_sync(&self, items: &[&[u8]]) -> Result<Vec<DocLocation>> {
        let mut data = self.data.lock().unwrap();
        let mut locations = Vec::with_capacity(items.len());
        for &item in items {
            let offset = data.len() as u64;
            let length = item.len() as u32;
            data.push(RECORD_ACTIVE);
            data.extend_from_slice(&length.to_le_bytes());
            data.extend_from_slice(item);
            locations.push(DocLocation { offset, length });
        }
        Ok(locations)
    }

    pub fn append_batch_no_sync_buffered(&self, items: &[&[u8]]) -> Result<Vec<DocLocation>> {
        if items.is_empty() {
            return Ok(vec![]);
        }
        let total: usize = items.iter().map(|i| 5 + i.len()).sum();
        let mut data = self.data.lock().unwrap();
        data.reserve(total);
        let mut locations = Vec::with_capacity(items.len());
        for &item in items {
            let offset = data.len() as u64;
            let length = item.len() as u32;
            data.push(RECORD_ACTIVE);
            data.extend_from_slice(&length.to_le_bytes());
            data.extend_from_slice(item);
            locations.push(DocLocation { offset, length });
        }
        Ok(locations)
    }

    pub fn mark_deleted_no_sync(&self, loc: DocLocation) -> Result<()> {
        self.mark_deleted(loc)
    }

    pub fn sync(&self) -> Result<()> {
        Ok(()) // no-op
    }

    pub fn file_size(&self) -> u64 {
        self.data.lock().unwrap().len() as u64
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn iter_active(&self) -> Result<Vec<(DocLocation, Vec<u8>)>> {
        let data = self.data.lock().unwrap();
        let mut results = Vec::new();
        let mut pos = 0usize;
        let data_len = data.len();

        while pos + 5 <= data_len {
            let status = data[pos];
            let length =
                u32::from_le_bytes([data[pos + 1], data[pos + 2], data[pos + 3], data[pos + 4]])
                    as usize;

            if pos + 5 + length > data_len {
                break;
            }

            if status == RECORD_ACTIVE {
                let bytes = data[pos + 5..pos + 5 + length].to_vec();
                results.push((
                    DocLocation {
                        offset: pos as u64,
                        length: length as u32,
                    },
                    bytes,
                ));
            }

            pos += 5 + length;
        }

        Ok(results)
    }

    pub fn for_each_active<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(DocLocation, Vec<u8>) -> Result<()>,
    {
        // Snapshot the data to avoid holding the lock during callbacks.
        let snapshot = self.data.lock().unwrap().clone();
        let mut pos = 0usize;
        let data_len = snapshot.len();

        while pos + 5 <= data_len {
            let status = snapshot[pos];
            let length = u32::from_le_bytes([
                snapshot[pos + 1],
                snapshot[pos + 2],
                snapshot[pos + 3],
                snapshot[pos + 4],
            ]) as usize;

            if pos + 5 + length > data_len {
                break;
            }

            if status == RECORD_ACTIVE {
                let bytes = snapshot[pos + 5..pos + 5 + length].to_vec();
                f(
                    DocLocation {
                        offset: pos as u64,
                        length: length as u32,
                    },
                    bytes,
                )?;
            }

            pos += 5 + length;
        }

        Ok(())
    }

    pub fn scan_readonly_while<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8]) -> Result<bool>,
    {
        let snapshot = self.data.lock().unwrap().clone();
        let mut pos = 0usize;
        let data_len = snapshot.len();

        while pos + 5 <= data_len {
            let status = snapshot[pos];
            let length = u32::from_le_bytes([
                snapshot[pos + 1],
                snapshot[pos + 2],
                snapshot[pos + 3],
                snapshot[pos + 4],
            ]) as usize;

            if pos + 5 + length > data_len {
                break;
            }

            if status == RECORD_ACTIVE {
                if !f(&snapshot[pos + 5..pos + 5 + length])? {
                    break;
                }
            }

            pos += 5 + length;
        }

        Ok(())
    }

    pub fn scan_segment_readonly_while<F>(
        &self,
        start_offset: u64,
        end_offset: u64,
        mut f: F,
    ) -> Result<()>
    where
        F: FnMut(&[u8]) -> Result<bool>,
    {
        let snapshot = self.data.lock().unwrap().clone();
        let mut pos = start_offset as usize;
        let end = end_offset as usize;

        while pos + 5 <= end && pos + 5 <= snapshot.len() {
            let status = snapshot[pos];
            let length = u32::from_le_bytes([
                snapshot[pos + 1],
                snapshot[pos + 2],
                snapshot[pos + 3],
                snapshot[pos + 4],
            ]) as usize;

            if pos + 5 + length > snapshot.len() {
                break;
            }

            if status == RECORD_ACTIVE {
                if !f(&snapshot[pos + 5..pos + 5 + length])? {
                    break;
                }
            }

            pos += 5 + length;
        }

        Ok(())
    }

    /// Compact in-memory: rebuild data buffer keeping only active records.
    pub fn compact_into_new(&self) -> Result<InMemStorage> {
        let new = InMemStorage::new();
        let active = self.iter_active()?;
        let mut data = new.data.lock().unwrap();
        for (_loc, bytes) in active {
            let length = bytes.len() as u32;
            data.push(RECORD_ACTIVE);
            data.extend_from_slice(&length.to_le_bytes());
            data.extend_from_slice(&bytes);
        }
        drop(data);
        Ok(new)
    }
}

// ---------------------------------------------------------------------------
// StorageBackend
// ---------------------------------------------------------------------------

/// Unified storage backend: either file-based or in-memory.
pub enum StorageBackend {
    #[cfg(not(target_arch = "wasm32"))]
    File(Storage),
    Memory(InMemStorage),
}

impl StorageBackend {
    pub fn append(&self, doc_bytes: &[u8]) -> Result<DocLocation> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::File(s) => s.append(doc_bytes),
            Self::Memory(m) => m.append(doc_bytes),
        }
    }

    pub fn read(&self, loc: DocLocation) -> Result<Vec<u8>> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::File(s) => s.read(loc),
            Self::Memory(m) => m.read(loc),
        }
    }

    pub fn read_lockfree(&self, loc: DocLocation) -> Result<Vec<u8>> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::File(s) => s.read_lockfree(loc),
            Self::Memory(m) => m.read_lockfree(loc),
        }
    }

    pub fn read_batch_lockfree(
        &self,
        locs: &mut [(usize, DocLocation)],
    ) -> Result<Vec<(usize, Vec<u8>)>> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::File(s) => s.read_batch_lockfree(locs),
            Self::Memory(m) => m.read_batch_lockfree(locs),
        }
    }

    pub fn mark_deleted(&self, loc: DocLocation) -> Result<()> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::File(s) => s.mark_deleted(loc),
            Self::Memory(m) => m.mark_deleted(loc),
        }
    }

    pub fn append_no_sync(&self, doc_bytes: &[u8]) -> Result<DocLocation> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::File(s) => s.append_no_sync(doc_bytes),
            Self::Memory(m) => m.append_no_sync(doc_bytes),
        }
    }

    pub fn append_batch_no_sync(&self, items: &[&[u8]]) -> Result<Vec<DocLocation>> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::File(s) => s.append_batch_no_sync(items),
            Self::Memory(m) => m.append_batch_no_sync(items),
        }
    }

    pub fn append_batch_no_sync_buffered(&self, items: &[&[u8]]) -> Result<Vec<DocLocation>> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::File(s) => s.append_batch_no_sync_buffered(items),
            Self::Memory(m) => m.append_batch_no_sync_buffered(items),
        }
    }

    pub fn mark_deleted_no_sync(&self, loc: DocLocation) -> Result<()> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::File(s) => s.mark_deleted_no_sync(loc),
            Self::Memory(m) => m.mark_deleted_no_sync(loc),
        }
    }

    pub fn sync(&self) -> Result<()> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::File(s) => s.sync(),
            Self::Memory(m) => m.sync(),
        }
    }

    pub fn file_size(&self) -> u64 {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::File(s) => s.file_size(),
            Self::Memory(m) => m.file_size(),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn path(&self) -> &Path {
        match self {
            Self::File(s) => s.path(),
            Self::Memory(m) => m.path(),
        }
    }

    pub fn iter_active(&self) -> Result<Vec<(DocLocation, Vec<u8>)>> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::File(s) => s.iter_active(),
            Self::Memory(m) => m.iter_active(),
        }
    }

    pub fn for_each_active<F>(&self, f: F) -> Result<()>
    where
        F: FnMut(DocLocation, Vec<u8>) -> Result<()>,
    {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::File(s) => s.for_each_active(f),
            Self::Memory(m) => m.for_each_active(f),
        }
    }

    pub fn scan_readonly_while<F>(&self, f: F) -> Result<()>
    where
        F: FnMut(&[u8]) -> Result<bool>,
    {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::File(s) => s.scan_readonly_while(f),
            Self::Memory(m) => m.scan_readonly_while(f),
        }
    }

    pub fn scan_segment_readonly_while<F>(
        &self,
        start_offset: u64,
        end_offset: u64,
        f: F,
    ) -> Result<()>
    where
        F: FnMut(&[u8]) -> Result<bool>,
    {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::File(s) => s.scan_segment_readonly_while(start_offset, end_offset, f),
            Self::Memory(m) => m.scan_segment_readonly_while(start_offset, end_offset, f),
        }
    }
}

// ---------------------------------------------------------------------------
// WalBackend
// ---------------------------------------------------------------------------

/// Unified WAL backend: either file-based or no-op (in-memory mode).
pub enum WalBackend {
    #[cfg(not(target_arch = "wasm32"))]
    File(Wal),
    Memory, // all operations are no-ops
}

impl WalBackend {
    pub fn log(&self, entry: &WalEntry) -> Result<()> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::File(w) => w.log(entry),
            Self::Memory => Ok(()),
        }
    }

    pub fn log_no_sync(&self, entry: &WalEntry) -> Result<()> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::File(w) => w.log_no_sync(entry),
            Self::Memory => Ok(()),
        }
    }

    pub fn log_batch(&self, entries: &[WalEntry]) -> Result<()> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::File(w) => w.log_batch(entries),
            Self::Memory => Ok(()),
        }
    }

    pub fn log_batch_no_sync(&self, entries: &[WalEntry]) -> Result<()> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::File(w) => w.log_batch_no_sync(entries),
            Self::Memory => Ok(()),
        }
    }

    pub fn log_batch_inserts_no_sync(&self, entries: &[(u64, &[u8])]) -> Result<()> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::File(w) => w.log_batch_inserts_no_sync(entries),
            Self::Memory => Ok(()),
        }
    }

    pub fn log_batch_inserts_no_sync_buffered(&self, entries: &[(u64, &[u8])]) -> Result<()> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::File(w) => w.log_batch_inserts_no_sync_buffered(entries),
            Self::Memory => Ok(()),
        }
    }

    pub fn sync(&self) -> Result<()> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::File(w) => w.sync(),
            Self::Memory => Ok(()),
        }
    }

    /// Group-commit fsync — see [`Wal::sync_shared`].
    pub fn sync_shared(&self) -> Result<()> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::File(w) => w.sync_shared(),
            Self::Memory => Ok(()),
        }
    }

    pub fn checkpoint(&self) -> Result<()> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::File(w) => w.checkpoint(),
            Self::Memory => Ok(()),
        }
    }

    pub fn checkpoint_no_sync(&self) -> Result<()> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::File(w) => w.checkpoint_no_sync(),
            Self::Memory => Ok(()),
        }
    }

    /// Seal the live WAL segment (PITR rotation), turning the current
    /// tail into a numbered sealed segment. No-op in memory mode.
    pub fn seal(&self) -> Result<()> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::File(w) => w.seal(),
            Self::Memory => Ok(()),
        }
    }

    /// Write barrier: once this returns, every writer that had allocated
    /// a GSN before the call has finished appending. No-op in memory mode.
    pub fn barrier(&self) {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::File(w) => w.barrier(),
            Self::Memory => {}
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn recover(
        &self,
        storage: &StorageBackend,
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
        match self {
            Self::File(wal) => match storage {
                StorageBackend::File(s) => wal.recover(
                    s,
                    primary_index,
                    doc_cache,
                    next_id,
                    committed_tx_ids,
                    version_index,
                    field_indexes,
                    composite_indexes,
                    verbose,
                    log_callback,
                ),
                _ => Ok(()),
            },
            Self::Memory => Ok(()),
        }
    }

    pub fn remove_file(&self) -> Result<()> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::File(w) => w.remove_file(),
            Self::Memory => Ok(()),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mem_storage_append_and_read() {
        let storage = InMemStorage::new();
        let loc = storage.append(b"hello").unwrap();
        let data = storage.read(loc).unwrap();
        assert_eq!(data, b"hello");
    }

    #[test]
    fn mem_storage_multiple_records() {
        let storage = InMemStorage::new();
        let loc1 = storage.append(b"first").unwrap();
        let loc2 = storage.append(b"second").unwrap();
        let loc3 = storage.append(b"third").unwrap();

        assert_eq!(storage.read(loc1).unwrap(), b"first");
        assert_eq!(storage.read(loc2).unwrap(), b"second");
        assert_eq!(storage.read(loc3).unwrap(), b"third");
    }

    #[test]
    fn mem_storage_delete() {
        let storage = InMemStorage::new();
        let loc1 = storage.append(b"keep").unwrap();
        let loc2 = storage.append(b"delete_me").unwrap();
        let _loc3 = storage.append(b"also_keep").unwrap();

        storage.mark_deleted(loc2).unwrap();

        let active = storage.iter_active().unwrap();
        assert_eq!(active.len(), 2);
        assert_eq!(active[0].1, b"keep");
        assert_eq!(active[1].1, b"also_keep");

        // Deleted record still readable by direct offset
        assert_eq!(storage.read(loc1).unwrap(), b"keep");
    }

    #[test]
    fn mem_storage_batch_append() {
        let storage = InMemStorage::new();
        let items: Vec<&[u8]> = vec![b"a", b"b", b"c"];
        let locs = storage.append_batch_no_sync_buffered(&items).unwrap();
        assert_eq!(locs.len(), 3);
        assert_eq!(storage.read(locs[0]).unwrap(), b"a");
        assert_eq!(storage.read(locs[1]).unwrap(), b"b");
        assert_eq!(storage.read(locs[2]).unwrap(), b"c");
    }

    #[test]
    fn mem_storage_scan_readonly() {
        let storage = InMemStorage::new();
        storage.append(b"one").unwrap();
        storage.append(b"two").unwrap();
        storage.append(b"three").unwrap();

        let mut collected = Vec::new();
        storage
            .scan_readonly_while(|bytes| {
                collected.push(bytes.to_vec());
                Ok(true)
            })
            .unwrap();
        assert_eq!(collected.len(), 3);
    }

    #[test]
    fn mem_storage_file_size() {
        let storage = InMemStorage::new();
        assert_eq!(storage.file_size(), 0);
        storage.append(b"test").unwrap();
        // 1 (status) + 4 (length) + 4 (payload) = 9
        assert_eq!(storage.file_size(), 9);
    }

    #[test]
    fn mem_storage_compact() {
        let storage = InMemStorage::new();
        storage.append(b"keep").unwrap();
        let loc = storage.append(b"delete").unwrap();
        storage.append(b"also_keep").unwrap();
        storage.mark_deleted(loc).unwrap();

        let compacted = storage.compact_into_new().unwrap();
        let active = compacted.iter_active().unwrap();
        assert_eq!(active.len(), 2);
        assert_eq!(active[0].1, b"keep");
        assert_eq!(active[1].1, b"also_keep");
        // Compacted storage should be smaller (no deleted record)
        assert!(compacted.file_size() < storage.file_size());
    }

    #[test]
    fn storage_backend_memory_roundtrip() {
        let backend = StorageBackend::Memory(InMemStorage::new());
        let loc = backend.append(b"hello").unwrap();
        assert_eq!(backend.read(loc).unwrap(), b"hello");
        assert_eq!(backend.read_lockfree(loc).unwrap(), b"hello");
    }

    #[test]
    fn wal_backend_memory_noop() {
        let wal = WalBackend::Memory;
        wal.log(&WalEntry::insert(1, b"data".to_vec())).unwrap();
        wal.log_no_sync(&WalEntry::insert(2, b"data".to_vec()))
            .unwrap();
        wal.checkpoint().unwrap();
        wal.remove_file().unwrap();
    }
}
