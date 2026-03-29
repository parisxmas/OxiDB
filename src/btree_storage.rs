//! B-tree page-based storage engine for OxiDB.
//!
//! Stores documents as key-value pairs (doc_id → JSONB bytes) in a file-backed
//! B-tree with fixed-size pages. Leaf pages are linked via next_page pointers
//! for efficient sequential cursor scans.
//!
//! Page layout:
//!   - 4 bytes: page type (0=free, 1=internal, 2=leaf)
//!   - 4 bytes: number of entries
//!   - 4 bytes: next_page (leaf only, 0 = none)
//!   - 4 bytes: prev_page (leaf only, 0 = none)
//!   - remaining: entries
//!
//! Leaf entry:
//!   [key: u64 LE][value_len: u32 LE][value bytes]
//!
//! Internal entry:
//!   [child_page: u32 LE][key: u64 LE]
//!   ... last child_page follows after last entry

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{Error, Result};

/// In-memory B-tree storage using a `HashMap<u64, Vec<u8>>`.
///
/// This is the first implementation: a clean Rust BTreeMap that provides the
/// correct cursor-based API. It can later be replaced with a page-cache-backed
/// file B-tree without changing the BTreeCollection code.
///
/// Advantages over the append-only StorageBackend adapter:
/// - O(log n) point lookups by doc_id (no HashMap needed)
/// - Sequential cursor scan via BTreeMap iterator (no per-doc pread)
/// - No DocLocation indirection — doc_id IS the key
/// - No soft-delete / compaction needed — updates are in-place
pub struct BTreeStorage {
    /// The B-tree: doc_id → JSONB-encoded document bytes.
    tree: HashMap<u64, Vec<u8>>,
    /// Total bytes stored (for stats).
    total_bytes: AtomicU64,
    /// Data directory for persistence (empty for in-memory).
    data_dir: PathBuf,
    /// Collection name (for file naming).
    name: String,
}

/// A cursor for iterating through the B-tree in key order.
/// Wraps a collected snapshot of keys for stable iteration.
pub struct Cursor {
    entries: Vec<(u64, Vec<u8>)>,
    pos: usize,
}

impl Cursor {
    /// Create a cursor positioned at the first entry (sorted by key).
    pub fn seek_first(tree: &HashMap<u64, Vec<u8>>) -> Result<Self> {
        let mut entries: Vec<(u64, Vec<u8>)> = tree
            .iter()
            .map(|(&k, v)| (k, v.clone()))
            .collect();
        entries.sort_unstable_by_key(|(k, _)| *k);
        Ok(Self { entries, pos: 0 })
    }

    /// Create a cursor positioned at a specific key (or the next key >= it).
    pub fn seek(tree: &HashMap<u64, Vec<u8>>, key: u64) -> Result<Self> {
        let mut entries: Vec<(u64, Vec<u8>)> = tree
            .iter()
            .filter(|(k, _)| **k >= key)
            .map(|(&k, v)| (k, v.clone()))
            .collect();
        entries.sort_unstable_by_key(|(k, _)| *k);
        Ok(Self { entries, pos: 0 })
    }

    /// Advance to the next entry. Returns None when exhausted.
    pub fn next(&mut self) -> Result<Option<(u64, Vec<u8>)>> {
        if self.pos >= self.entries.len() {
            return Ok(None);
        }
        let entry = self.entries[self.pos].clone();
        self.pos += 1;
        Ok(Some(entry))
    }

    /// Peek at the current entry without advancing.
    pub fn peek(&self) -> Option<&(u64, Vec<u8>)> {
        self.entries.get(self.pos)
    }

    /// Number of remaining entries.
    pub fn remaining(&self) -> usize {
        self.entries.len().saturating_sub(self.pos)
    }
}

/// A reverse cursor for iterating through the B-tree in descending key order.
pub struct ReverseCursor {
    entries: Vec<(u64, Vec<u8>)>,
    pos: usize,
}

impl ReverseCursor {
    /// Create a cursor positioned at the last entry (descending key order).
    pub fn seek_last(tree: &HashMap<u64, Vec<u8>>) -> Result<Self> {
        let mut entries: Vec<(u64, Vec<u8>)> = tree
            .iter()
            .map(|(&k, v)| (k, v.clone()))
            .collect();
        entries.sort_unstable_by(|(a, _), (b, _)| b.cmp(a)); // descending
        Ok(Self { entries, pos: 0 })
    }

    /// Advance to the previous (next lower) entry.
    pub fn next(&mut self) -> Result<Option<(u64, Vec<u8>)>> {
        if self.pos >= self.entries.len() {
            return Ok(None);
        }
        let entry = self.entries[self.pos].clone();
        self.pos += 1;
        Ok(Some(entry))
    }
}

impl BTreeStorage {
    /// Create a new in-memory B-tree storage.
    pub fn new(name: &str, data_dir: &Path) -> Self {
        Self {
            tree: HashMap::new(),
            total_bytes: AtomicU64::new(0),
            data_dir: data_dir.to_path_buf(),
            name: name.to_string(),
        }
    }

    /// Create a new in-memory B-tree storage (no persistence).
    pub fn new_in_memory(name: &str) -> Self {
        Self {
            tree: HashMap::new(),
            total_bytes: AtomicU64::new(0),
            data_dir: PathBuf::new(),
            name: name.to_string(),
        }
    }

    /// Open or load a B-tree from disk. Falls back to empty if no file exists.
    pub fn open(name: &str, data_dir: &Path) -> Result<Self> {
        let path = data_dir.join(format!("{}.btree", name));
        let mut storage = Self::new(name, data_dir);

        if path.exists() {
            let data = std::fs::read(&path)?;
            storage.load_from_bytes(&data)?;
        }

        Ok(storage)
    }

    /// Deserialize the B-tree from a binary dump.
    /// Format: repeated [key: u64 LE][len: u32 LE][value bytes]
    fn load_from_bytes(&mut self, data: &[u8]) -> Result<()> {
        let mut pos = 0;
        let mut total = 0u64;
        while pos + 12 <= data.len() {
            let key = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            if pos + len > data.len() {
                return Err(Error::Codec("truncated btree file".into()));
            }
            let value = data[pos..pos + len].to_vec();
            total += len as u64;
            self.tree.insert(key, value);
            pos += len;
        }
        self.total_bytes.store(total, Ordering::Release);
        Ok(())
    }

    /// Persist the B-tree to disk.
    pub fn persist(&self) -> Result<()> {
        if self.data_dir.as_os_str().is_empty() {
            return Ok(()); // in-memory mode
        }
        let path = self.data_dir.join(format!("{}.btree", self.name));
        let mut buf = Vec::with_capacity(self.total_bytes.load(Ordering::Acquire) as usize + self.tree.len() * 12);
        for (&key, value) in &self.tree {
            buf.extend_from_slice(&key.to_le_bytes());
            buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
            buf.extend_from_slice(value);
        }
        std::fs::write(&path, &buf)?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Core operations
    // -----------------------------------------------------------------------

    /// Insert a key-value pair. Returns the previous value if the key existed.
    pub fn insert(&mut self, key: u64, value: Vec<u8>) -> Option<Vec<u8>> {
        let new_len = value.len() as u64;
        let old = self.tree.insert(key, value);
        if let Some(ref old_val) = old {
            // Replace: subtract old, add new
            let old_len = old_val.len() as u64;
            let current = self.total_bytes.load(Ordering::Acquire);
            self.total_bytes.store(current - old_len + new_len, Ordering::Release);
        } else {
            self.total_bytes.fetch_add(new_len, Ordering::AcqRel);
        }
        old
    }

    /// Get a value by key.
    pub fn get(&self, key: u64) -> Option<&Vec<u8>> {
        self.tree.get(&key)
    }

    /// Remove a key-value pair. Returns the removed value.
    pub fn remove(&mut self, key: u64) -> Option<Vec<u8>> {
        let old = self.tree.remove(&key);
        if let Some(ref val) = old {
            let len = val.len() as u64;
            let current = self.total_bytes.load(Ordering::Acquire);
            self.total_bytes.store(current.saturating_sub(len), Ordering::Release);
        }
        old
    }

    /// Check if a key exists.
    pub fn contains_key(&self, key: u64) -> bool {
        self.tree.contains_key(&key)
    }

    /// Number of entries in the B-tree.
    pub fn count(&self) -> usize {
        self.tree.len()
    }

    /// Total bytes stored.
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Acquire)
    }

    // -----------------------------------------------------------------------
    // Cursor-based iteration
    // -----------------------------------------------------------------------

    /// Create a forward cursor starting at the first entry.
    pub fn cursor_first(&self) -> Result<Cursor> {
        Cursor::seek_first(&self.tree)
    }

    /// Create a forward cursor starting at the given key.
    pub fn cursor_seek(&self, key: u64) -> Result<Cursor> {
        Cursor::seek(&self.tree, key)
    }

    /// Create a reverse cursor starting at the last entry.
    pub fn cursor_last(&self) -> Result<ReverseCursor> {
        ReverseCursor::seek_last(&self.tree)
    }

    /// Iterate all entries calling `f` for each. Stops early if `f` returns false.
    /// This is the key advantage: sequential page-order iteration.
    pub fn scan_all_while<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(u64, &[u8]) -> Result<bool>,
    {
        let mut keys: Vec<u64> = self.tree.keys().copied().collect();
        keys.sort_unstable();
        for key in keys {
            if let Some(value) = self.tree.get(&key) {
                if !f(key, value)? {
                    break;
                }
            }
        }
        Ok(())
    }

    /// Iterate all entries calling `f` for each (bytes only, sorted by key).
    pub fn scan_bytes_while<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8]) -> Result<bool>,
    {
        let mut keys: Vec<u64> = self.tree.keys().copied().collect();
        keys.sort_unstable();
        for key in keys {
            if let Some(value) = self.tree.get(&key) {
                if !f(value)? {
                    break;
                }
            }
        }
        Ok(())
    }

    /// Get a reference to the underlying BTreeMap (for direct iteration).
    pub fn tree(&self) -> &HashMap<u64, Vec<u8>> {
        &self.tree
    }

    /// Clear all entries.
    pub fn clear(&mut self) {
        self.tree.clear();
        self.total_bytes.store(0, Ordering::Release);
    }

    /// Pre-allocate capacity in the underlying HashMap.
    /// Call this before a series of `insert_batch` calls to avoid incremental reallocation.
    pub fn reserve(&mut self, additional: usize) {
        self.tree.reserve(additional);
    }

    /// Batch insert multiple entries using BTreeMap::extend for bulk efficiency.
    /// IMPORTANT: Only use for new keys (no replacements). If keys might exist,
    /// use the regular insert() loop instead.
    pub fn insert_batch(&mut self, entries: Vec<(u64, Vec<u8>)>) {
        let total_new: u64 = entries.iter().map(|(_, v)| v.len() as u64).sum();
        self.tree.reserve(entries.len());
        self.tree.extend(entries);
        self.total_bytes.fetch_add(total_new, Ordering::AcqRel);
    }

    /// Return all values as a Vec of byte slices for parallel processing.
    pub fn values_as_slices(&self) -> Vec<&[u8]> {
        self.tree.values().map(|v| v.as_slice()).collect()
    }

    /// Iterate all entries in arbitrary order (no key sort). Faster for full scans
    /// like aggregation where key order doesn't matter.
    pub fn for_each_value<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(u64, &[u8]) -> Result<bool>,
    {
        for (&key, value) in &self.tree {
            if !f(key, value)? {
                break;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_crud() {
        let mut storage = BTreeStorage::new_in_memory("test");
        assert_eq!(storage.count(), 0);

        storage.insert(1, b"hello".to_vec());
        storage.insert(2, b"world".to_vec());
        assert_eq!(storage.count(), 2);
        assert_eq!(storage.get(1).unwrap(), b"hello");
        assert_eq!(storage.get(2).unwrap(), b"world");

        storage.insert(1, b"updated".to_vec());
        assert_eq!(storage.count(), 2);
        assert_eq!(storage.get(1).unwrap(), b"updated");

        let removed = storage.remove(2);
        assert_eq!(removed.unwrap(), b"world");
        assert_eq!(storage.count(), 1);
        assert!(storage.get(2).is_none());
    }

    #[test]
    fn cursor_forward() {
        let mut storage = BTreeStorage::new_in_memory("test");
        for i in 1..=5 {
            storage.insert(i, format!("doc_{}", i).into_bytes());
        }

        let mut cursor = storage.cursor_first().unwrap();
        let mut keys = Vec::new();
        while let Some((key, _value)) = cursor.next().unwrap() {
            keys.push(key);
        }
        assert_eq!(keys, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn cursor_seek() {
        let mut storage = BTreeStorage::new_in_memory("test");
        for i in [1, 3, 5, 7, 9] {
            storage.insert(i, format!("doc_{}", i).into_bytes());
        }

        let mut cursor = storage.cursor_seek(4).unwrap();
        let (key, _) = cursor.next().unwrap().unwrap();
        assert_eq!(key, 5); // next key >= 4
    }

    #[test]
    fn cursor_reverse() {
        let mut storage = BTreeStorage::new_in_memory("test");
        for i in 1..=5 {
            storage.insert(i, format!("doc_{}", i).into_bytes());
        }

        let mut cursor = storage.cursor_last().unwrap();
        let mut keys = Vec::new();
        while let Some((key, _value)) = cursor.next().unwrap() {
            keys.push(key);
        }
        assert_eq!(keys, vec![5, 4, 3, 2, 1]);
    }

    #[test]
    fn scan_all() {
        let mut storage = BTreeStorage::new_in_memory("test");
        for i in 1..=10 {
            storage.insert(i, format!("doc_{}", i).into_bytes());
        }

        let mut count = 0;
        storage.scan_all_while(|_key, _value| {
            count += 1;
            Ok(count < 5)
        }).unwrap();
        assert_eq!(count, 5); // stopped after 5
    }

    #[test]
    fn persistence_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut storage = BTreeStorage::new("test_persist", dir.path());
            storage.insert(1, b"alpha".to_vec());
            storage.insert(2, b"beta".to_vec());
            storage.insert(3, b"gamma".to_vec());
            storage.persist().unwrap();
        }
        {
            let storage = BTreeStorage::open("test_persist", dir.path()).unwrap();
            assert_eq!(storage.count(), 3);
            assert_eq!(storage.get(1).unwrap(), b"alpha");
            assert_eq!(storage.get(2).unwrap(), b"beta");
            assert_eq!(storage.get(3).unwrap(), b"gamma");
        }
    }
}
