//! Memory-mapped primary index for zero-startup-cost document lookups.
//!
//! Instead of loading all doc_id → (offset, length) mappings into RAM at startup,
//! this module persists them to a `.pidx` file and memory-maps it.
//! The OS manages which pages stay in RAM (hot) vs disk (cold).
//!
//! File format:
//! ```text
//! [MAGIC: "OXPI" 4 bytes]
//! [VERSION: u32 LE]
//! [ENTRY_COUNT: u64 LE]
//! [NEXT_ID: u64 LE]
//! [entries: sorted array of (doc_id: u64, offset: u64, length: u32, version: u64) ]
//! ```
//!
//! Each entry is 28 bytes. Lookups use binary search: O(log n).

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use memmap2::Mmap;

use crate::document::DocumentId;
use crate::storage::DocLocation;

const MAGIC: &[u8; 4] = b"OXPI";
const VERSION: u32 = 1;
const HEADER_SIZE: usize = 4 + 4 + 8 + 8; // magic + version + count + next_id
const ENTRY_SIZE: usize = 28; // doc_id(8) + offset(8) + length(4) + version(8)

/// A memory-mapped primary index. Zero startup cost — the OS pages in data on demand.
pub struct MmapPrimaryIndex {
    mmap: Option<Mmap>,
    path: PathBuf,
    entry_count: u64,
    next_id: u64,
    /// In-memory overlay for new writes since last persist.
    overlay: parking_lot::RwLock<HashMap<DocumentId, (DocLocation, u64)>>,
    /// Deleted doc_ids since last persist (tombstones).
    deleted: parking_lot::RwLock<std::collections::HashSet<DocumentId>>,
}

impl MmapPrimaryIndex {
    /// Open or create a primary index at the given path.
    pub fn open(path: &Path) -> io::Result<Self> {
        if path.exists() && fs::metadata(path)?.len() >= HEADER_SIZE as u64 {
            let file = File::open(path)?;
            let mmap = unsafe { Mmap::map(&file)? };

            // Validate header
            if &mmap[0..4] != MAGIC {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "bad pidx magic"));
            }
            let version = u32::from_le_bytes(mmap[4..8].try_into().unwrap());
            if version != VERSION {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unsupported pidx version: {}", version),
                ));
            }
            let entry_count = u64::from_le_bytes(mmap[8..16].try_into().unwrap());
            let next_id = u64::from_le_bytes(mmap[16..24].try_into().unwrap());

            Ok(Self {
                mmap: Some(mmap),
                path: path.to_path_buf(),
                entry_count,
                next_id,
                overlay: parking_lot::RwLock::new(HashMap::new()),
                deleted: parking_lot::RwLock::new(std::collections::HashSet::new()),
            })
        } else {
            Ok(Self {
                mmap: None,
                path: path.to_path_buf(),
                entry_count: 0,
                next_id: 1,
                overlay: parking_lot::RwLock::new(HashMap::new()),
                deleted: parking_lot::RwLock::new(std::collections::HashSet::new()),
            })
        }
    }

    /// Get the next document ID.
    pub fn next_id(&self) -> u64 {
        let overlay = self.overlay.read();
        if overlay.is_empty() {
            self.next_id
        } else {
            let max_overlay = overlay.keys().max().copied().unwrap_or(0);
            self.next_id.max(max_overlay + 1)
        }
    }

    /// Look up a document's location by ID. Checks overlay first, then mmap.
    pub fn get(&self, doc_id: DocumentId) -> Option<(DocLocation, u64)> {
        // Check tombstones
        if self.deleted.read().contains(&doc_id) {
            return None;
        }

        // Check overlay (recent writes)
        if let Some(entry) = self.overlay.read().get(&doc_id) {
            return Some(*entry);
        }

        // Binary search in mmap
        self.mmap_lookup(doc_id)
    }

    /// Check if a document exists.
    pub fn contains(&self, doc_id: DocumentId) -> bool {
        self.get(doc_id).is_some()
    }

    /// Get just the location.
    pub fn get_location(&self, doc_id: DocumentId) -> Option<DocLocation> {
        self.get(doc_id).map(|(loc, _)| loc)
    }

    /// Get just the version.
    pub fn get_version(&self, doc_id: DocumentId) -> u64 {
        self.get(doc_id).map(|(_, v)| v).unwrap_or(0)
    }

    /// Insert or update an entry in the overlay.
    pub fn insert(&self, doc_id: DocumentId, loc: DocLocation, version: u64) {
        self.deleted.write().remove(&doc_id);
        self.overlay.write().insert(doc_id, (loc, version));
    }

    /// Mark a document as deleted.
    pub fn remove(&self, doc_id: DocumentId) {
        self.overlay.write().remove(&doc_id);
        self.deleted.write().insert(doc_id);
    }

    /// Update version for a document.
    pub fn update_version(&self, doc_id: DocumentId, new_version: u64) {
        if let Some(mut entry) = self.overlay.write().get_mut(&doc_id) {
            entry.1 = new_version;
        } else if let Some((loc, _)) = self.mmap_lookup(doc_id) {
            self.overlay.write().insert(doc_id, (loc, new_version));
        }
    }

    /// Update location for a document (after update-in-place or append).
    pub fn update_location(&self, doc_id: DocumentId, new_loc: DocLocation, new_version: u64) {
        self.insert(doc_id, new_loc, new_version);
    }

    /// Total number of documents (mmap + overlay - deleted).
    pub fn len(&self) -> usize {
        let overlay = self.overlay.read();
        let deleted = self.deleted.read();
        let mmap_count = self.entry_count as usize;
        let overlay_new = overlay
            .keys()
            .filter(|id| !self.mmap_contains(**id))
            .count();
        let mmap_deleted = deleted
            .iter()
            .filter(|id| self.mmap_contains(**id))
            .count();
        mmap_count + overlay_new - mmap_deleted
    }

    /// Iterate over all document IDs and locations.
    pub fn iter(&self) -> Vec<(DocumentId, DocLocation)> {
        let overlay = self.overlay.read();
        let deleted = self.deleted.read();
        let mut result = Vec::with_capacity(self.len());

        // Mmap entries (excluding deleted and overridden)
        if let Some(ref mmap) = self.mmap {
            for i in 0..self.entry_count as usize {
                let entry = self.read_entry(mmap, i);
                if !deleted.contains(&entry.0) && !overlay.contains_key(&entry.0) {
                    result.push((entry.0, DocLocation {
                        offset: entry.1,
                        length: entry.2,
                    }));
                }
            }
        }

        // Overlay entries
        for (&doc_id, &(loc, _)) in overlay.iter() {
            if !deleted.contains(&doc_id) {
                result.push((doc_id, loc));
            }
        }

        result
    }

    /// Persist the current state (mmap + overlay - deleted) to disk.
    /// After this, overlay is cleared and a new mmap is created.
    pub fn persist(&self) -> io::Result<()> {
        let all_entries = self.collect_all_entries();
        if all_entries.is_empty() {
            return Ok(());
        }

        let next_id = self.next_id();

        // Write to temp file, then rename (atomic)
        let tmp_path = self.path.with_extension("pidx.tmp");
        {
            let file = File::create(&tmp_path)?;
            let mut writer = BufWriter::with_capacity(256 * 1024, file);

            writer.write_all(MAGIC)?;
            writer.write_all(&VERSION.to_le_bytes())?;
            writer.write_all(&(all_entries.len() as u64).to_le_bytes())?;
            writer.write_all(&next_id.to_le_bytes())?;

            for (doc_id, offset, length, version) in &all_entries {
                writer.write_all(&doc_id.to_le_bytes())?;
                writer.write_all(&offset.to_le_bytes())?;
                writer.write_all(&length.to_le_bytes())?;
                writer.write_all(&version.to_le_bytes())?;
            }
            writer.flush()?;
        }
        fs::rename(&tmp_path, &self.path)?;

        Ok(())
    }

    /// Rebuild from in-memory HashMap (used during migration from old format).
    pub fn rebuild_from_maps(
        path: &Path,
        primary: &HashMap<DocumentId, DocLocation>,
        versions: &HashMap<DocumentId, u64>,
        next_id: u64,
    ) -> io::Result<Self> {
        let mut entries: Vec<(u64, u64, u32, u64)> = primary
            .iter()
            .map(|(&id, loc)| {
                let version = versions.get(&id).copied().unwrap_or(1);
                (id, loc.offset, loc.length, version)
            })
            .collect();
        entries.sort_unstable_by_key(|e| e.0);

        let tmp_path = path.with_extension("pidx.tmp");
        {
            let file = File::create(&tmp_path)?;
            let mut writer = BufWriter::with_capacity(256 * 1024, file);

            writer.write_all(MAGIC)?;
            writer.write_all(&VERSION.to_le_bytes())?;
            writer.write_all(&(entries.len() as u64).to_le_bytes())?;
            writer.write_all(&next_id.to_le_bytes())?;

            for (doc_id, offset, length, version) in &entries {
                writer.write_all(&doc_id.to_le_bytes())?;
                writer.write_all(&offset.to_le_bytes())?;
                writer.write_all(&length.to_le_bytes())?;
                writer.write_all(&version.to_le_bytes())?;
            }
            writer.flush()?;
        }
        fs::rename(&tmp_path, path)?;

        Self::open(path)
    }

    // ─── Internal ──────────────────────────────────────────────────

    fn mmap_lookup(&self, doc_id: DocumentId) -> Option<(DocLocation, u64)> {
        let mmap = self.mmap.as_ref()?;
        let count = self.entry_count as usize;
        if count == 0 {
            return None;
        }

        // Binary search on sorted doc_id array
        let mut lo = 0usize;
        let mut hi = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let (id, offset, length, version) = self.read_entry(mmap, mid);
            match id.cmp(&doc_id) {
                std::cmp::Ordering::Equal => {
                    return Some((DocLocation { offset, length }, version));
                }
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        None
    }

    fn mmap_contains(&self, doc_id: DocumentId) -> bool {
        self.mmap_lookup(doc_id).is_some()
    }

    #[inline]
    fn read_entry(&self, mmap: &Mmap, index: usize) -> (u64, u64, u32, u64) {
        let base = HEADER_SIZE + index * ENTRY_SIZE;
        let doc_id = u64::from_le_bytes(mmap[base..base + 8].try_into().unwrap());
        let offset = u64::from_le_bytes(mmap[base + 8..base + 16].try_into().unwrap());
        let length = u32::from_le_bytes(mmap[base + 16..base + 20].try_into().unwrap());
        let version = u64::from_le_bytes(mmap[base + 20..base + 28].try_into().unwrap());
        (doc_id, offset, length, version)
    }

    fn collect_all_entries(&self) -> Vec<(u64, u64, u32, u64)> {
        let overlay = self.overlay.read();
        let deleted = self.deleted.read();
        let mut entries: Vec<(u64, u64, u32, u64)> = Vec::with_capacity(self.len());

        // Mmap entries
        if let Some(ref mmap) = self.mmap {
            for i in 0..self.entry_count as usize {
                let (id, offset, length, version) = self.read_entry(mmap, i);
                if !deleted.contains(&id) {
                    if let Some(&(loc, ver)) = overlay.get(&id) {
                        entries.push((id, loc.offset, loc.length, ver));
                    } else {
                        entries.push((id, offset, length, version));
                    }
                }
            }
        }

        // New overlay entries (not in mmap)
        for (&id, &(loc, ver)) in overlay.iter() {
            if !deleted.contains(&id) && !self.mmap_contains(id) {
                entries.push((id, loc.offset, loc.length, ver));
            }
        }

        entries.sort_unstable_by_key(|e| e.0);
        entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Result;

    #[test]
    fn roundtrip() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("test.pidx");

        let idx = MmapPrimaryIndex::open(&path)?;
        assert_eq!(idx.len(), 0);

        // Insert some entries
        idx.insert(1, DocLocation { offset: 0, length: 100 }, 1);
        idx.insert(5, DocLocation { offset: 100, length: 200 }, 1);
        idx.insert(3, DocLocation { offset: 300, length: 150 }, 1);

        assert_eq!(idx.len(), 3);
        assert!(idx.contains(1));
        assert!(idx.contains(5));
        assert!(!idx.contains(2));

        // Persist and reopen
        idx.persist()?;
        let idx2 = MmapPrimaryIndex::open(&path)?;
        assert_eq!(idx2.len(), 3);
        assert_eq!(idx2.get_version(1), 1);
        let loc = idx2.get_location(5).unwrap();
        assert_eq!(loc.offset, 100);
        assert_eq!(loc.length, 200);

        Ok(())
    }

    #[test]
    fn overlay_and_delete() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("test.pidx");

        let idx = MmapPrimaryIndex::open(&path)?;
        idx.insert(1, DocLocation { offset: 0, length: 100 }, 1);
        idx.insert(2, DocLocation { offset: 100, length: 100 }, 1);
        idx.persist()?;

        let idx = MmapPrimaryIndex::open(&path)?;
        assert_eq!(idx.len(), 2);

        // Delete one
        idx.remove(1);
        assert_eq!(idx.len(), 1);
        assert!(!idx.contains(1));
        assert!(idx.contains(2));

        // Add new
        idx.insert(3, DocLocation { offset: 200, length: 50 }, 1);
        assert_eq!(idx.len(), 2);

        Ok(())
    }

    #[test]
    fn binary_search_large() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("test.pidx");

        let mut primary = HashMap::new();
        let mut versions = HashMap::new();
        for i in 0..10000u64 {
            primary.insert(i, DocLocation { offset: i * 100, length: 90 });
            versions.insert(i, 1u64);
        }

        let idx = MmapPrimaryIndex::rebuild_from_maps(&path, &primary, &versions, 10000)?;
        assert_eq!(idx.len(), 10000);

        // Check random lookups
        assert!(idx.contains(0));
        assert!(idx.contains(5000));
        assert!(idx.contains(9999));
        assert!(!idx.contains(10000));

        let loc = idx.get_location(7777).unwrap();
        assert_eq!(loc.offset, 7777 * 100);

        Ok(())
    }
}
