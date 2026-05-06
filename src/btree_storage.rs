//! B-tree page-based storage engine for OxiDB.
//!
//! Stores documents as key-value pairs (doc_id → JSONB bytes) in an `scc::HashMap`
//! for lock-free concurrent access. Methods take `&self` instead of `&mut self`
//! because `scc::HashMap` provides interior mutability with per-bucket locking.
//!
//! Page layout (on-disk persistence format):
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

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use scc::HashMap as SccMap;

use crate::error::{Error, Result};

/// Concurrent B-tree storage using an `scc::HashMap<u64, Vec<u8>>`.
///
/// All mutating methods take `&self` — `scc::HashMap` handles internal locking
/// at bucket granularity, so multiple threads can insert/remove concurrently
/// without an external `RwLock`.
///
/// Advantages over the append-only StorageBackend adapter:
/// - O(1) amortized point lookups by doc_id (hash map)
/// - Sequential cursor scan via sorted snapshot (no per-doc pread)
/// - No DocLocation indirection — doc_id IS the key
/// - No soft-delete / compaction needed — updates are in-place
/// - Lock-free concurrent reads and writes via scc bucket-level locking
pub struct BTreeStorage {
    /// The concurrent map: doc_id → JSONB-encoded document bytes.
    tree: SccMap<u64, Vec<u8>>,
    /// Total bytes stored (for stats). Updated atomically.
    total_bytes: AtomicU64,
    /// Data directory for persistence (empty for in-memory).
    data_dir: PathBuf,
    /// Collection name (for file naming).
    name: String,
    /// Optional encryption key for at-rest encryption of the .btree file.
    encryption: Option<Arc<crate::EncryptionKey>>,
    /// Serializes concurrent persist() calls. Without this, two
    /// concurrent commits both write to the same `{name}.btree.tmp`
    /// file, mangle each other's bytes, and the surviving rename
    /// produces a corrupt on-disk image — the source of the
    /// "io error: No such file or directory" / truncated-file errors
    /// observed under load. Mutex is per-collection so writes to
    /// different collections still proceed in parallel.
    persist_mu: crate::locks::Mutex<()>,
}

/// A cursor for iterating through the storage in ascending key order.
/// Wraps a collected snapshot of entries for stable iteration.
pub struct Cursor {
    entries: Vec<(u64, Vec<u8>)>,
    pos: usize,
}

impl Cursor {
    /// Create a cursor positioned at the first entry (sorted by key).
    pub fn seek_first(tree: &SccMap<u64, Vec<u8>>) -> Result<Self> {
        let mut entries: Vec<(u64, Vec<u8>)> = Vec::new();
        tree.iter_sync(|k, v| {
            entries.push((*k, v.clone()));
            true
        });
        entries.sort_unstable_by_key(|(k, _)| *k);
        Ok(Self { entries, pos: 0 })
    }

    /// Create a cursor positioned at a specific key (or the next key >= it).
    pub fn seek(tree: &SccMap<u64, Vec<u8>>, key: u64) -> Result<Self> {
        let mut entries: Vec<(u64, Vec<u8>)> = Vec::new();
        tree.iter_sync(|k, v| {
            if *k >= key {
                entries.push((*k, v.clone()));
            }
            true
        });
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

/// A reverse cursor for iterating through the storage in descending key order.
pub struct ReverseCursor {
    entries: Vec<(u64, Vec<u8>)>,
    pos: usize,
}

impl ReverseCursor {
    /// Create a cursor positioned at the last entry (descending key order).
    pub fn seek_last(tree: &SccMap<u64, Vec<u8>>) -> Result<Self> {
        let mut entries: Vec<(u64, Vec<u8>)> = Vec::new();
        tree.iter_sync(|k, v| {
            entries.push((*k, v.clone()));
            true
        });
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
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new(name: &str, data_dir: &Path, encryption: Option<Arc<crate::EncryptionKey>>) -> Self {
        Self {
            tree: SccMap::new(),
            total_bytes: AtomicU64::new(0),
            data_dir: data_dir.to_path_buf(),
            name: name.to_string(),
            encryption,
            persist_mu: crate::locks::Mutex::new(()),
        }
    }

    /// Create a new in-memory B-tree storage (no persistence).
    pub fn new_in_memory(name: &str) -> Self {
        Self {
            tree: SccMap::new(),
            total_bytes: AtomicU64::new(0),
            data_dir: PathBuf::new(),
            name: name.to_string(),
            encryption: None,
            persist_mu: crate::locks::Mutex::new(()),
        }
    }

    /// Open or load a B-tree from disk. Falls back to empty if no file
    /// exists. Tolerates truncation: if the on-disk image got partially
    /// written before we landed `persist()`'s atomic-rename fix (or in
    /// any future bug), load every fully-decoded record and let the WAL
    /// replay catch up with the rest. Without this fallback the engine
    /// refuses to boot — bricked database for one bad shutdown.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open(name: &str, data_dir: &Path, encryption: Option<Arc<crate::EncryptionKey>>) -> Result<Self> {
        let path = data_dir.join(format!("{}.btree", name));
        let tmp = data_dir.join(format!("{}.btree.tmp", name));
        let storage = Self::new(name, data_dir, encryption);

        // Stale tmp from a crash mid-persist: drop it. The real file
        // (if present) is either intact (rename completed first) or
        // partially the old version (rename never ran) — both are
        // valid starting points; the leftover tmp is just garbage.
        if tmp.exists() {
            let _ = std::fs::remove_file(&tmp);
        }

        if path.exists() {
            let mut data = std::fs::read(&path)?;
            if let Some(ref key) = storage.encryption {
                // Decryption failure on the whole file is fatal — we
                // can't partially-decrypt to recover. WAL replay alone
                // (without the BTree base) would still rebuild state
                // for whatever WAL has retained, so we soldier on.
                match key.decrypt(&data) {
                    Ok(d) => data = d,
                    Err(e) => {
                        eprintln!(
                            "[btree] {}.btree decryption failed: {} — \
                             starting from empty store, WAL replay will rebuild",
                            name, e
                        );
                        return Ok(storage);
                    }
                }
            }
            if let Err(e) = storage.load_from_bytes(&data) {
                eprintln!(
                    "[btree] {}.btree partially loaded ({} records, {} bytes total) \
                     before error: {} — WAL replay will reconcile",
                    name,
                    storage.tree.len(),
                    data.len(),
                    e
                );
                // Fall through with whatever was loaded — load_from_bytes
                // upserts records as it parses them, so any prefix that
                // decoded cleanly is already in `tree`.
            }
        }

        Ok(storage)
    }

    /// Deserialize the B-tree from a binary dump.
    /// Format: repeated [key: u64 LE][len: u32 LE][value bytes]
    fn load_from_bytes(&self, data: &[u8]) -> Result<()> {
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
            // upsert_sync behaves like DashMap::insert — inserts or replaces
            let _ = self.tree.upsert_sync(key, value);
            pos += len;
        }
        self.total_bytes.store(total, Ordering::Release);
        Ok(())
    }

    /// Persist the B-tree to disk atomically.
    ///
    /// Durability protocol: write to a sibling tmp path, fsync the file
    /// data, atomically rename onto the target, then fsync the parent
    /// directory so the rename itself is durable across power loss.
    ///
    /// Why each step matters:
    ///   - tmp + rename: a process crash mid-write leaves the original
    ///     `{name}.btree` untouched. Without this, a partial overwrite
    ///     produces the "truncated btree file" startup error we hit.
    ///   - fsync(tmp): ensures the tmp file's bytes hit disk before we
    ///     rename. Skipping it lets rename succeed while data is still
    ///     in OS page cache; a power loss between rename and natural
    ///     flush re-creates the truncation problem on a "valid-named"
    ///     file.
    ///   - fsync(dir): the rename itself is a directory metadata
    ///     change. POSIX requires fsync on the directory to make the
    ///     entry durable.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn persist(&self) -> Result<()> {
        if self.data_dir.as_os_str().is_empty() {
            return Ok(()); // in-memory mode
        }
        // Serialize concurrent persists for THIS collection. Two
        // concurrent commits that both reach checkpoint_wal would
        // otherwise both open `{name}.btree.tmp` with truncate(true),
        // mangle each other's bytes, and the surviving rename leaves
        // the on-disk image either truncated or unrelated to either
        // committer's view of state. We hold the lock for the entire
        // write+rename+fsync sequence so the on-disk image is always
        // a snapshot of the tree at some single point in time.
        //
        // This serializes commits that target the same collection
        // but doesn't block commits on other collections (each has
        // its own BTreeStorage with its own mutex).
        let _persist_guard = self.persist_mu.lock();

        let path = self.data_dir.join(format!("{}.btree", self.name));
        let tmp = self.data_dir.join(format!("{}.btree.tmp", self.name));

        let mut buf = Vec::with_capacity(self.total_bytes.load(Ordering::Acquire) as usize + self.tree.len() * 12);
        self.tree.iter_sync(|key, value| {
            buf.extend_from_slice(&key.to_le_bytes());
            buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
            buf.extend_from_slice(value);
            true
        });
        let write_buf = if let Some(ref key) = self.encryption {
            key.encrypt(&buf)?
        } else {
            buf
        };

        // 1) Write + fsync the tmp file.
        {
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)?;
            std::io::Write::write_all(&mut f, &write_buf)?;
            f.sync_data()?;
        }

        // 2) Atomic rename. POSIX guarantees this is observed by other
        // processes as either the old file or the new one — never a
        // mix.
        std::fs::rename(&tmp, &path)?;

        // 3) Fsync the parent directory so the rename hits disk too.
        // On macOS APFS this is a no-op as far as durability goes (FS
        // crash-consistency is stronger), but it costs nothing and is
        // required on ext4 / xfs / etc.
        if let Some(parent) = path.parent() {
            if let Ok(dir) = std::fs::File::open(parent) {
                let _ = dir.sync_data();
            }
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Core operations
    // -----------------------------------------------------------------------

    /// Insert a key-value pair. Returns the previous value if the key existed.
    ///
    /// Uses `scc::HashMap::upsert_sync` which returns the old value on replace
    /// (same semantics as the former `DashMap::insert`).
    ///
    /// Uses atomic `fetch_add` / `fetch_sub` for `total_bytes` to avoid
    /// load+store races under concurrent inserts.
    pub fn insert(&self, key: u64, value: Vec<u8>) -> Option<Vec<u8>> {
        let new_len = value.len() as u64;
        let old = self.tree.upsert_sync(key, value);
        if let Some(ref old_val) = old {
            // Replace: subtract old size, add new size (two atomic ops, no race).
            let old_len = old_val.len() as u64;
            self.total_bytes.fetch_sub(old_len, Ordering::AcqRel);
            self.total_bytes.fetch_add(new_len, Ordering::AcqRel);
        } else {
            self.total_bytes.fetch_add(new_len, Ordering::AcqRel);
        }
        old
    }

    /// Get a value by key. Returns a cloned `Vec<u8>` because `scc::HashMap`
    /// entry guards cannot outlive the bucket lock scope.
    pub fn get(&self, key: u64) -> Option<Vec<u8>> {
        self.tree.read_sync(&key, |_, v| v.clone())
    }

    /// Remove a key-value pair. Returns the removed value.
    pub fn remove(&self, key: u64) -> Option<Vec<u8>> {
        let old = self.tree.remove_sync(&key).map(|(_, v)| v);
        if let Some(ref val) = old {
            let len = val.len() as u64;
            self.total_bytes.fetch_sub(len, Ordering::AcqRel);
        }
        old
    }

    /// Check if a key exists.
    pub fn contains_key(&self, key: u64) -> bool {
        self.tree.contains_sync(&key)
    }

    /// Number of entries in the storage.
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

    /// Iterate all entries calling `f` for each (sorted by key).
    /// Stops early if `f` returns false.
    /// Collects keys first, then looks up each value, so concurrent
    /// mutations between collect and lookup are safe (missing keys skipped).
    pub fn scan_all_while<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(u64, &[u8]) -> Result<bool>,
    {
        let mut keys: Vec<u64> = Vec::new();
        self.tree.iter_sync(|k, _| {
            keys.push(*k);
            true
        });
        keys.sort_unstable();
        for key in keys {
            if let Some(value) = self.tree.read_sync(&key, |_, v| v.clone()) {
                if !f(key, &value)? {
                    break;
                }
            }
        }
        Ok(())
    }

    /// Iterate all entries calling `f` for each (bytes only, sorted by key).
    /// Collects keys first, then looks up each value.
    pub fn scan_bytes_while<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8]) -> Result<bool>,
    {
        let mut keys: Vec<u64> = Vec::new();
        self.tree.iter_sync(|k, _| {
            keys.push(*k);
            true
        });
        keys.sort_unstable();
        for key in keys {
            if let Some(value) = self.tree.read_sync(&key, |_, v| v.clone()) {
                if !f(&value)? {
                    break;
                }
            }
        }
        Ok(())
    }

    /// Clear all entries.
    pub fn clear(&self) {
        self.tree.clear_sync();
        self.total_bytes.store(0, Ordering::Release);
    }

    /// No-op. `scc::HashMap` manages its own capacity internally.
    /// Kept for API compatibility.
    pub fn reserve(&self, _additional: usize) {
        // scc::HashMap handles its own capacity.
    }

    /// Batch insert multiple entries. Uses `upsert_sync` for each entry.
    /// Uses atomic `fetch_add` for the total byte count.
    ///
    /// IMPORTANT: Only use for new keys (no replacements). If keys might exist,
    /// use the regular `insert()` loop instead.
    pub fn insert_batch(&self, entries: Vec<(u64, Vec<u8>)>) {
        let total_new: u64 = entries.iter().map(|(_, v)| v.len() as u64).sum();
        #[cfg(not(target_arch = "wasm32"))]
        if entries.len() > 1000 {
            use rayon::prelude::*;
            entries.into_par_iter().for_each(|(key, value)| {
                let _ = self.tree.insert_sync(key, value);
            });
        } else {
            for (key, value) in entries {
                let _ = self.tree.insert_sync(key, value);
            }
        }
        #[cfg(target_arch = "wasm32")]
        for (key, value) in entries {
            let _ = self.tree.insert_sync(key, value);
        }
        self.total_bytes.fetch_add(total_new, Ordering::AcqRel);
    }

    /// Return all values as owned `Vec<u8>` copies for parallel processing.
    /// Values are cloned because `scc::HashMap` bucket guards cannot outlive iteration.
    pub fn values_as_slices(&self) -> Vec<Vec<u8>> {
        let mut values = Vec::new();
        self.tree.iter_sync(|_, v| {
            values.push(v.clone());
            true
        });
        values
    }

    /// Collect all keys (lightweight — no value cloning).
    pub fn scan_keys<F>(&self, mut f: F)
    where
        F: FnMut(u64),
    {
        self.tree.iter_sync(|key, _| {
            f(*key);
            true
        });
    }

    /// Iterate all entries in arbitrary order (no key sort). Faster for full scans
    /// like aggregation where key order doesn't matter.
    pub fn for_each_value<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(u64, &[u8]) -> Result<bool>,
    {
        let mut err: Option<Error> = None;
        self.tree.iter_sync(|key, value| {
            match f(*key, value) {
                Ok(true) => true,
                Ok(false) => false,
                Err(e) => {
                    err = Some(e);
                    false
                }
            }
        });
        if let Some(e) = err {
            return Err(e);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_crud() {
        let storage = BTreeStorage::new_in_memory("test");
        assert_eq!(storage.count(), 0);

        storage.insert(1, b"hello".to_vec());
        storage.insert(2, b"world".to_vec());
        assert_eq!(storage.count(), 2);
        assert_eq!(storage.get(1).unwrap().as_slice(), b"hello");
        assert_eq!(storage.get(2).unwrap().as_slice(), b"world");

        storage.insert(1, b"updated".to_vec());
        assert_eq!(storage.count(), 2);
        assert_eq!(storage.get(1).unwrap().as_slice(), b"updated");

        let removed = storage.remove(2);
        assert_eq!(removed.unwrap(), b"world");
        assert_eq!(storage.count(), 1);
        assert!(storage.get(2).is_none());
    }

    #[test]
    fn cursor_forward() {
        let storage = BTreeStorage::new_in_memory("test");
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
        let storage = BTreeStorage::new_in_memory("test");
        for i in [1, 3, 5, 7, 9] {
            storage.insert(i, format!("doc_{}", i).into_bytes());
        }

        let mut cursor = storage.cursor_seek(4).unwrap();
        let (key, _) = cursor.next().unwrap().unwrap();
        assert_eq!(key, 5); // next key >= 4
    }

    #[test]
    fn cursor_reverse() {
        let storage = BTreeStorage::new_in_memory("test");
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
        let storage = BTreeStorage::new_in_memory("test");
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
            let storage = BTreeStorage::new("test_persist", dir.path(), None);
            storage.insert(1, b"alpha".to_vec());
            storage.insert(2, b"beta".to_vec());
            storage.insert(3, b"gamma".to_vec());
            storage.persist().unwrap();
        }
        {
            let storage = BTreeStorage::open("test_persist", dir.path(), None).unwrap();
            assert_eq!(storage.count(), 3);
            assert_eq!(storage.get(1).unwrap().as_slice(), b"alpha");
            assert_eq!(storage.get(2).unwrap().as_slice(), b"beta");
            assert_eq!(storage.get(3).unwrap().as_slice(), b"gamma");
        }
    }
}
