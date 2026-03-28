/// B-tree page-based storage engine for OxiDB.
///
/// Public API: `BTreeStorage` provides a StorageBackend-like interface
/// backed by a page-based B-tree with LZ4-compressed pages on disk,
/// a sharded LRU page cache, and a physical-page WAL.

pub mod page;
pub mod node;
pub mod pager;
pub mod tree;
pub mod cursor;
pub mod wal;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use self::cursor::Cursor;
use self::page::{Page, PageType};
use self::pager::Pager;
use self::tree::BTree;
use self::wal::BTreeWal;

/// Top-level B-tree storage handle.
pub struct BTreeStorage {
    tree: BTree,
    #[allow(dead_code)]
    wal: BTreeWal,
    #[allow(dead_code)]
    path: PathBuf,
}

impl BTreeStorage {
    /// Open (or create) a B-tree database at the given directory path.
    /// Files created: `<path>/btree.db` and `<path>/btree.wal`.
    pub fn open(path: &Path) -> std::io::Result<Self> {
        std::fs::create_dir_all(path)?;

        let db_path = path.join("btree.db");
        let wal_path = path.join("btree.wal");

        let pager = Arc::new(Pager::open(&db_path)?);
        let wal = BTreeWal::open(&wal_path)?;

        // Recover from WAL if needed.
        if wal.has_frames()? {
            let recovered = wal.recover(&pager)?;
            if recovered > 0 {
                pager.flush_dirty()?;
            }
            wal.checkpoint()?;
        }

        // Initialize fresh database if needed.
        let (root_id, doc_count) = if pager.total_pages() == 0 {
            // Allocate header page (0) and root leaf (1).
            let hdr_id = pager.allocate_page(); // 0
            let root_id = pager.allocate_page(); // 1
            let header = Page::new(hdr_id, PageType::Header);
            let root = Page::new(root_id, PageType::Leaf);
            pager.write_page(&header)?;
            pager.write_page(&root)?;
            pager.flush_dirty()?;
            (root_id, 0u64)
        } else {
            // Read the header page to find root page id and doc count.
            // We store root_page_id at bytes 64..68 and doc_count at bytes 68..76
            // in the header page body.
            let header = pager.read_page(0)?;
            let root_id = u32::from_le_bytes(
                header.data[64..68].try_into().unwrap()
            );
            let doc_count = u64::from_le_bytes(
                header.data[68..76].try_into().unwrap()
            );
            // If root_id is 0 (uninitialized), use page 1.
            let root_id = if root_id == 0 { 1 } else { root_id };
            (root_id, doc_count)
        };

        let tree = BTree::new(pager, root_id, doc_count);

        Ok(Self {
            tree,
            wal,
            path: path.to_path_buf(),
        })
    }

    /// Insert a document.
    pub fn insert(&self, doc_id: u64, doc_bytes: &[u8]) -> std::io::Result<()> {
        self.tree.insert(doc_id, doc_bytes)
    }

    /// Get a document by id.
    pub fn get(&self, doc_id: u64) -> std::io::Result<Option<Vec<u8>>> {
        self.tree.find(doc_id)
    }

    /// Delete a document. Returns true if found and deleted.
    pub fn delete(&self, doc_id: u64) -> std::io::Result<bool> {
        self.tree.delete(doc_id)
    }

    /// Update a document (delete + insert).
    pub fn update(&self, doc_id: u64, doc_bytes: &[u8]) -> std::io::Result<()> {
        self.tree.update(doc_id, doc_bytes)
    }

    /// Full scan of all documents via cursor.
    pub fn scan_all(&self) -> std::io::Result<Vec<(u64, Vec<u8>)>> {
        let mut result = Vec::new();
        let mut cursor = Cursor::seek_first(&self.tree)?;
        while let Some(entry) = cursor.next()? {
            result.push(entry);
        }
        Ok(result)
    }

    /// Document count.
    pub fn count(&self) -> u64 {
        self.tree.count()
    }

    /// Flush dirty pages and persist metadata to the header page, then fsync.
    pub fn sync(&self) -> std::io::Result<()> {
        // Write metadata to header page.
        let mut header = self.tree.pager.read_page(0)?;
        let root_id = self.tree.root_page_id();
        let doc_count = self.tree.count();
        header.data[64..68].copy_from_slice(&root_id.to_le_bytes());
        header.data[68..76].copy_from_slice(&doc_count.to_le_bytes());
        header.dirty = true;
        self.tree.pager.write_page(&header)?;

        self.tree.pager.sync()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn btree_storage_full_crud() {
        let dir = TempDir::new().unwrap();
        let storage = BTreeStorage::open(dir.path()).unwrap();

        // Insert.
        storage.insert(1, b"hello").unwrap();
        storage.insert(2, b"world").unwrap();
        storage.insert(3, b"rust").unwrap();

        // Get.
        assert_eq!(storage.get(1).unwrap(), Some(b"hello".to_vec()));
        assert_eq!(storage.get(2).unwrap(), Some(b"world".to_vec()));
        assert_eq!(storage.get(3).unwrap(), Some(b"rust".to_vec()));
        assert_eq!(storage.get(99).unwrap(), None);

        // Update.
        storage.update(2, b"WORLD").unwrap();
        assert_eq!(storage.get(2).unwrap(), Some(b"WORLD".to_vec()));

        // Delete.
        assert!(storage.delete(1).unwrap());
        assert_eq!(storage.get(1).unwrap(), None);
        assert!(!storage.delete(1).unwrap());

        // Count.
        assert_eq!(storage.count(), 2);

        // Scan.
        let all = storage.scan_all().unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].0, 2);
        assert_eq!(all[1].0, 3);
    }

    #[test]
    fn btree_storage_persistence() {
        let dir = TempDir::new().unwrap();

        // Write and sync.
        {
            let storage = BTreeStorage::open(dir.path()).unwrap();
            for i in 1..=50u64 {
                storage.insert(i, format!("val{}", i).as_bytes()).unwrap();
            }
            storage.sync().unwrap();
        }

        // Re-open and verify.
        {
            let storage = BTreeStorage::open(dir.path()).unwrap();
            assert_eq!(storage.count(), 50);
            for i in 1..=50u64 {
                let expected = format!("val{}", i);
                assert_eq!(
                    storage.get(i).unwrap(),
                    Some(expected.into_bytes()),
                    "key {} missing after reopen",
                    i
                );
            }
        }
    }

    #[test]
    fn btree_storage_large_scan() {
        let dir = TempDir::new().unwrap();
        let storage = BTreeStorage::open(dir.path()).unwrap();

        let n = 1000u64;
        for i in 1..=n {
            storage.insert(i, b"data").unwrap();
        }

        let all = storage.scan_all().unwrap();
        assert_eq!(all.len(), n as usize);
        // Verify sorted order.
        for i in 0..all.len() {
            assert_eq!(all[i].0, (i + 1) as u64);
        }
    }

    #[test]
    fn btree_storage_empty() {
        let dir = TempDir::new().unwrap();
        let storage = BTreeStorage::open(dir.path()).unwrap();
        assert_eq!(storage.count(), 0);
        assert_eq!(storage.get(1).unwrap(), None);
        assert!(!storage.delete(1).unwrap());
        assert!(storage.scan_all().unwrap().is_empty());
    }
}
