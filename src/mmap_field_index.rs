//! Mmap-based field index — zero-startup-cost alternative to `FieldIndex`.
//!
//! The on-disk format (`.fidx` v2) is designed so the OS can page in only the
//! portions actually touched by a query.  New writes go into an in-memory
//! overlay (`BTreeMap`) and are merged back to disk on `persist()`.
//!
//! ## File layout
//!
//! ```text
//! [MAGIC: "OXFI" 4 bytes]
//! [VERSION: u32 LE = 2]
//! [FLAGS: u32 LE]           // bit 0 = unique
//! [FIELD_NAME_LEN: u16 LE]
//! [FIELD_NAME: UTF-8 bytes]
//! [ENTRY_COUNT: u64 LE]     // unique IndexValue count
//! [DOC_COUNT: u64 LE]       // total documents indexed
//!
//! -- Entry table (sorted by IndexValue for binary search) --
//! [entries: array of EntryHeader]
//!   EntryHeader = {
//!     value_type: u8
//!     value_i64: i64 LE      // numeric payload / string-table offset
//!     value_len: u32 LE      // string length (0 for non-string)
//!     docid_offset: u64 LE   // byte offset into docid section
//!     docid_count: u32 LE    // number of doc_ids
//!   }  // 25 bytes each
//!
//! -- String table --
//! [strings: concatenated UTF-8, referenced by offset+len]
//!
//! -- DocID section --
//! [docid_lists: arrays of u64 LE, sorted ascending per entry]
//! ```

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::io::{self, Write};
use std::ops::Bound;
use std::path::{Path, PathBuf};

use memmap2::Mmap;
use serde_json::Value;

use crate::document::DocumentId;
use crate::index::{DocIdSet, FieldIndex};
use crate::value::IndexValue;

// ── Constants ────────────────────────────────────────────────────────────────

const MAGIC: &[u8; 4] = b"OXFI";
const FORMAT_VERSION: u32 = 2;
const ENTRY_HEADER_SIZE: usize = 25;

// value_type tags (match IndexValue::TAG_*)
const TAG_NULL: u8 = 0;
const TAG_BOOL: u8 = 1;
const TAG_INT: u8 = 2;
const TAG_FLOAT: u8 = 3;
const TAG_DATETIME: u8 = 4;
const TAG_STRING: u8 = 5;

// ── Fixed-size header ────────────────────────────────────────────────────────

/// Size of everything before the entry table:
///   MAGIC(4) + VERSION(4) + FLAGS(4) + FIELD_NAME_LEN(2) = 14 bytes
/// followed by FIELD_NAME (variable) then ENTRY_COUNT(8) + DOC_COUNT(8) = 16
const HEADER_FIXED: usize = 14;

// ── Helpers to read LE values from a byte slice ─────────────────────────────

fn read_u16(buf: &[u8], off: usize) -> u16 {
    u16::from_le_bytes([buf[off], buf[off + 1]])
}

fn read_u32(buf: &[u8], off: usize) -> u32 {
    u32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]])
}

fn read_u64(buf: &[u8], off: usize) -> u64 {
    u64::from_le_bytes([
        buf[off],
        buf[off + 1],
        buf[off + 2],
        buf[off + 3],
        buf[off + 4],
        buf[off + 5],
        buf[off + 6],
        buf[off + 7],
    ])
}

fn read_i64(buf: &[u8], off: usize) -> i64 {
    i64::from_le_bytes([
        buf[off],
        buf[off + 1],
        buf[off + 2],
        buf[off + 3],
        buf[off + 4],
        buf[off + 5],
        buf[off + 6],
        buf[off + 7],
    ])
}

// ── Parsed header (offsets into the mmap) ────────────────────────────────────

#[derive(Debug, Clone)]
struct MmapLayout {
    entry_table_offset: usize,
    string_table_offset: usize,
    docid_section_offset: usize,
    entry_count: u64,
    #[allow(dead_code)]
    doc_count: u64,
}

// ── MmapFieldIndex ──────────────────────────────────────────────────────────

/// A field index backed by an mmap'd `.fidx` v2 file.
///
/// Reads are served from the mmap (binary search) merged with an in-memory
/// overlay.  Writes go into the overlay.  `persist()` rewrites the file
/// from scratch (overlay + mmap merged).
#[derive(Debug)]
pub struct MmapFieldIndex {
    mmap: Option<Mmap>,
    layout: Option<MmapLayout>,
    path: PathBuf,
    pub field: String,
    pub unique: bool,
    /// Overlay for new writes since last persist.
    overlay: BTreeMap<IndexValue, DocIdSet>,
    /// Tombstones: doc_id → old IndexValue removed since last persist.
    removed: HashMap<DocumentId, Vec<IndexValue>>,
}

// ── resolve_value_field (duplicated to avoid circular dep) ───────────────────

fn resolve_value_field<'a>(data: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = data;
    for part in path.split('.') {
        current = current.as_object()?.get(part)?;
    }
    Some(current)
}

// ── Construction / opening ──────────────────────────────────────────────────

impl MmapFieldIndex {
    /// Create a new (empty) mmap field index.
    pub fn new(field: String) -> Self {
        Self {
            mmap: None,
            layout: None,
            path: PathBuf::new(),
            field,
            unique: false,
            overlay: BTreeMap::new(),
            removed: HashMap::new(),
        }
    }

    /// Create a new (empty) unique mmap field index.
    pub fn new_unique(field: String) -> Self {
        Self {
            mmap: None,
            layout: None,
            path: PathBuf::new(),
            field,
            unique: true,
            overlay: BTreeMap::new(),
            removed: HashMap::new(),
        }
    }

    /// Open an existing `.fidx` v2 file via mmap. Instant — no deserialization.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = fs::File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let buf = &mmap[..];

        // Validate header
        if buf.len() < HEADER_FIXED + 16 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "fidx v2 file too small",
            ));
        }
        if &buf[0..4] != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad magic in fidx v2",
            ));
        }
        let version = read_u32(buf, 4);
        if version != FORMAT_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported fidx version {}", version),
            ));
        }
        let flags = read_u32(buf, 8);
        let unique = (flags & 1) != 0;
        let field_name_len = read_u16(buf, 12) as usize;

        if buf.len() < HEADER_FIXED + field_name_len + 16 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "fidx v2 truncated field name",
            ));
        }
        let field = String::from_utf8(buf[HEADER_FIXED..HEADER_FIXED + field_name_len].to_vec())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let after_name = HEADER_FIXED + field_name_len;
        let entry_count = read_u64(buf, after_name);
        let doc_count = read_u64(buf, after_name + 8);

        let entry_table_offset = after_name + 16;
        let entry_table_size = entry_count as usize * ENTRY_HEADER_SIZE;

        // Compute string table offset: after entry table.
        // We need to scan entries to find total string bytes for the string table,
        // but we stored it implicitly. The string table starts right after the entry table,
        // and the docid section starts after the string table.
        // To find the string table size, we look at the max (offset + len) in entries.
        let string_table_offset = entry_table_offset + entry_table_size;

        // Find the end of the string table by scanning entries for max string offset+len
        let mut string_table_end: usize = 0;
        for i in 0..entry_count as usize {
            let eoff = entry_table_offset + i * ENTRY_HEADER_SIZE;
            let vtype = buf[eoff];
            if vtype == TAG_STRING {
                let str_offset = read_i64(buf, eoff + 1) as usize;
                let str_len = read_u32(buf, eoff + 9) as usize;
                let end = str_offset + str_len;
                if end > string_table_end {
                    string_table_end = end;
                }
            }
        }

        let docid_section_offset = string_table_offset + string_table_end;

        let layout = MmapLayout {
            entry_table_offset,
            string_table_offset,
            docid_section_offset,
            entry_count,
            doc_count,
        };

        Ok(Self {
            mmap: Some(mmap),
            layout: Some(layout),
            path: path.to_path_buf(),
            field,
            unique,
            overlay: BTreeMap::new(),
            removed: HashMap::new(),
        })
    }

    /// Set the path for persist().
    pub fn set_path(&mut self, path: PathBuf) {
        self.path = path;
    }

    // ── Mmap entry reading ──────────────────────────────────────────────────

    /// Read the IndexValue from entry `idx` in the mmap entry table.
    fn mmap_entry_value(&self, idx: usize) -> Option<IndexValue> {
        let mmap = self.mmap.as_ref()?;
        let layout = self.layout.as_ref()?;
        let buf = &mmap[..];
        let eoff = layout.entry_table_offset + idx * ENTRY_HEADER_SIZE;
        if eoff + ENTRY_HEADER_SIZE > buf.len() {
            return None;
        }
        let vtype = buf[eoff];
        let value_i64 = read_i64(buf, eoff + 1);
        let value_len = read_u32(buf, eoff + 9) as usize;

        Some(match vtype {
            TAG_NULL => IndexValue::Null,
            TAG_BOOL => IndexValue::Boolean(value_i64 != 0),
            TAG_INT => IndexValue::Integer(value_i64),
            TAG_FLOAT => IndexValue::Float(f64::from_bits(value_i64 as u64)),
            TAG_DATETIME => IndexValue::DateTime(value_i64),
            TAG_STRING => {
                let str_start = layout.string_table_offset + value_i64 as usize;
                let str_end = str_start + value_len;
                if str_end > buf.len() {
                    return None;
                }
                let s = std::str::from_utf8(&buf[str_start..str_end]).ok()?;
                IndexValue::String(s.to_string())
            }
            _ => return None,
        })
    }

    /// Read the doc_id list for entry `idx` from the mmap.
    fn mmap_entry_docids(&self, idx: usize) -> DocIdSet {
        let Some(mmap) = self.mmap.as_ref() else {
            return DocIdSet::Empty;
        };
        let Some(layout) = self.layout.as_ref() else {
            return DocIdSet::Empty;
        };
        let buf = &mmap[..];
        let eoff = layout.entry_table_offset + idx * ENTRY_HEADER_SIZE;
        if eoff + ENTRY_HEADER_SIZE > buf.len() {
            return DocIdSet::Empty;
        }
        let docid_offset = read_u64(buf, eoff + 13) as usize;
        let docid_count = read_u32(buf, eoff + 21) as usize;

        let abs_offset = layout.docid_section_offset + docid_offset;
        let needed = abs_offset + docid_count * 8;
        if needed > buf.len() {
            return DocIdSet::Empty;
        }

        if docid_count == 0 {
            DocIdSet::Empty
        } else if docid_count == 1 {
            DocIdSet::One(read_u64(buf, abs_offset))
        } else {
            let mut set = BTreeSet::new();
            for j in 0..docid_count {
                set.insert(read_u64(buf, abs_offset + j * 8));
            }
            DocIdSet::Set(set)
        }
    }

    /// Check if a doc_id exists in a mmap entry's docid list using binary search.
    /// O(log n) — does NOT load all doc_ids into memory.
    fn mmap_entry_contains_docid(&self, idx: usize, doc_id: DocumentId) -> bool {
        let Some(mmap) = self.mmap.as_ref() else { return false };
        let Some(layout) = self.layout.as_ref() else { return false };
        let buf = &mmap[..];
        let eoff = layout.entry_table_offset + idx * ENTRY_HEADER_SIZE;
        if eoff + ENTRY_HEADER_SIZE > buf.len() { return false }
        let docid_offset = read_u64(buf, eoff + 13) as usize;
        let docid_count = read_u32(buf, eoff + 21) as usize;
        if docid_count == 0 { return false }
        let abs_offset = layout.docid_section_offset + docid_offset;
        if abs_offset + docid_count * 8 > buf.len() { return false }

        // Binary search on sorted u64 array in mmap
        let mut lo = 0usize;
        let mut hi = docid_count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let v = read_u64(buf, abs_offset + mid * 8);
            if v == doc_id { return true }
            if v < doc_id { lo = mid + 1 } else { hi = mid }
        }
        false
    }

    /// Binary-search the mmap entry table for an exact IndexValue match.
    /// Returns the entry index if found.
    fn mmap_find_entry(&self, target: &IndexValue) -> Option<usize> {
        let layout = self.layout.as_ref()?;
        let count = layout.entry_count as usize;
        if count == 0 {
            return None;
        }
        let mut lo: usize = 0;
        let mut hi: usize = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let val = self.mmap_entry_value(mid)?;
            match val.cmp(target) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Equal => return Some(mid),
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        None
    }

    /// Find the first entry index >= `target` (lower bound).
    fn mmap_lower_bound(&self, target: &IndexValue) -> usize {
        let Some(layout) = self.layout.as_ref() else {
            return 0;
        };
        let count = layout.entry_count as usize;
        if count == 0 {
            return 0;
        }
        let mut lo: usize = 0;
        let mut hi: usize = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let val = match self.mmap_entry_value(mid) {
                Some(v) => v,
                None => break,
            };
            if val < *target {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// Find the first entry index > `target` (upper bound).
    fn mmap_upper_bound(&self, target: &IndexValue) -> usize {
        let Some(layout) = self.layout.as_ref() else {
            return 0;
        };
        let count = layout.entry_count as usize;
        if count == 0 {
            return 0;
        }
        let mut lo: usize = 0;
        let mut hi: usize = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let val = match self.mmap_entry_value(mid) {
                Some(v) => v,
                None => break,
            };
            if val <= *target {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// Get doc IDs from mmap for a given IndexValue, filtering out tombstones.
    fn mmap_get(&self, value: &IndexValue) -> DocIdSet {
        let Some(idx) = self.mmap_find_entry(value) else {
            return DocIdSet::Empty;
        };
        let mut set = self.mmap_entry_docids(idx);
        // Remove tombstoned doc IDs
        for (doc_id, removed_vals) in &self.removed {
            if removed_vals.iter().any(|v| v == value) {
                set.remove(doc_id);
            }
        }
        set
    }

    /// Iterate mmap entries in a range [start_idx..end_idx), filtering tombstones.
    fn mmap_range_entries(
        &self,
        start_idx: usize,
        end_idx: usize,
    ) -> Vec<(IndexValue, DocIdSet)> {
        let mut result = Vec::new();
        for i in start_idx..end_idx {
            let Some(val) = self.mmap_entry_value(i) else {
                continue;
            };
            let mut ids = self.mmap_entry_docids(i);
            // Apply tombstones
            for (doc_id, removed_vals) in &self.removed {
                if removed_vals.iter().any(|v| v == &val) {
                    ids.remove(doc_id);
                }
            }
            if !ids.is_empty() {
                result.push((val, ids));
            }
        }
        result
    }

    // ── Public API (same as FieldIndex) ─────────────────────────────────────

    /// Insert a document value into the overlay.
    pub fn insert_value(&mut self, id: DocumentId, data: &Value) {
        if let Some(value) = resolve_value_field(data, &self.field) {
            let key = IndexValue::from_json(value);
            self.overlay.entry(key).or_default().insert(id);
        }
    }

    /// Insert a pre-computed IndexValue directly into the overlay.
    pub fn insert_raw(&mut self, id: DocumentId, key: IndexValue) {
        self.overlay.entry(key).or_default().insert(id);
    }

    /// Insert a Document into the overlay.
    pub fn insert(&mut self, doc: &crate::document::Document) {
        if let Some(value) = doc.get_field(&self.field) {
            let key = IndexValue::from_json(value);
            self.overlay.entry(key).or_default().insert(doc.id);
        }
    }

    /// Remove a document value. Records a tombstone so mmap lookups skip it.
    pub fn remove_value(&mut self, id: DocumentId, data: &Value) {
        if let Some(value) = resolve_value_field(data, &self.field) {
            let key = IndexValue::from_json(value);
            // Try overlay first
            if let Some(set) = self.overlay.get_mut(&key) {
                if set.remove(&id) {
                    if set.is_empty() {
                        self.overlay.remove(&key);
                    }
                    return;
                }
            }
            // Record tombstone for mmap layer
            self.removed.entry(id).or_default().push(key);
        }
    }

    /// Remove a Document.
    pub fn remove(&mut self, doc: &crate::document::Document) {
        if let Some(value) = doc.get_field(&self.field) {
            let key = IndexValue::from_json(value);
            if let Some(set) = self.overlay.get_mut(&key) {
                if set.remove(&doc.id) {
                    if set.is_empty() {
                        self.overlay.remove(&key);
                    }
                    return;
                }
            }
            self.removed.entry(doc.id).or_default().push(key);
        }
    }

    /// Check if a value already exists for a different document (uniqueness check).
    pub fn check_unique(&self, value: &IndexValue, exclude_id: Option<DocumentId>) -> bool {
        // Check overlay
        if let Some(ids) = self.overlay.get(value) {
            let found = match exclude_id {
                Some(eid) => ids.iter().any(|&id| id != eid),
                None => !ids.is_empty(),
            };
            if found {
                return true;
            }
        }
        // Check mmap
        let mmap_ids = self.mmap_get(value);
        match exclude_id {
            Some(eid) => mmap_ids.iter().any(|&id| id != eid),
            None => !mmap_ids.is_empty(),
        }
    }

    // ── Query helpers ───────────────────────────────────────────────────────

    /// Find all doc IDs matching an exact value. Merges mmap + overlay.
    pub fn find_eq(&self, value: &IndexValue) -> BTreeSet<DocumentId> {
        let mut result = self.mmap_get(value).to_btreeset();
        if let Some(ids) = self.overlay.get(value) {
            result.extend(ids);
        }
        result
    }

    /// Check if a doc_id exists for a given value without materializing a BTreeSet.
    /// O(log n) in mmap layer + O(log n) in overlay.
    pub fn contains_doc_id(&self, value: &IndexValue, doc_id: DocumentId) -> bool {
        // Check overlay first (recent writes)
        if let Some(ids) = self.overlay.get(value) {
            if ids.contains(&doc_id) {
                return true;
            }
        }
        // Check tombstones
        if let Some(removed_vals) = self.removed.get(&doc_id) {
            if removed_vals.iter().any(|v| v == value) {
                return false;
            }
        }
        // Check mmap via binary search — O(log n), no allocation
        if let Some(idx) = self.mmap_find_entry(value) {
            return self.mmap_entry_contains_docid(idx, doc_id);
        }
        false
    }

    /// Count matching docs for exact value.
    pub fn count_eq(&self, value: &IndexValue) -> usize {
        let mmap_count = self.mmap_get(value).len();
        let overlay_count = self.overlay.get(value).map_or(0, |s| s.len());
        mmap_count + overlay_count
    }

    /// Find all doc IDs NOT matching a value.
    pub fn find_ne(&self, value: &IndexValue) -> BTreeSet<DocumentId> {
        let mut result = BTreeSet::new();
        // Mmap entries
        if let Some(layout) = &self.layout {
            for i in 0..layout.entry_count as usize {
                if let Some(val) = self.mmap_entry_value(i) {
                    if &val != value {
                        let mut ids = self.mmap_entry_docids(i);
                        // Apply tombstones
                        for (doc_id, removed_vals) in &self.removed {
                            if removed_vals.iter().any(|v| v == &val) {
                                ids.remove(doc_id);
                            }
                        }
                        result.extend(&ids);
                    }
                }
            }
        }
        // Overlay entries
        for (k, ids) in &self.overlay {
            if k != value {
                result.extend(ids);
            }
        }
        result
    }

    /// Range query. Merges mmap range scan with overlay range.
    pub fn find_range(
        &self,
        start: Bound<&IndexValue>,
        end: Bound<&IndexValue>,
    ) -> BTreeSet<DocumentId> {
        let mut result = BTreeSet::new();

        // Mmap range scan
        if let Some(layout) = &self.layout {
            let count = layout.entry_count as usize;
            let start_idx = match start {
                Bound::Unbounded => 0,
                Bound::Included(v) => self.mmap_lower_bound(v),
                Bound::Excluded(v) => self.mmap_upper_bound(v),
            };
            let end_idx = match end {
                Bound::Unbounded => count,
                Bound::Included(v) => self.mmap_upper_bound(v),
                Bound::Excluded(v) => self.mmap_lower_bound(v),
            };
            for (_, ids) in self.mmap_range_entries(start_idx, end_idx) {
                result.extend(&ids);
            }
        }

        // Overlay range scan
        for (_, ids) in self.overlay.range((start, end)) {
            result.extend(ids);
        }

        result
    }

    /// Count docs in a range.
    pub fn count_range(&self, start: Bound<&IndexValue>, end: Bound<&IndexValue>) -> usize {
        let mut count = 0;

        // Mmap range
        if let Some(layout) = &self.layout {
            let total = layout.entry_count as usize;
            let start_idx = match start {
                Bound::Unbounded => 0,
                Bound::Included(v) => self.mmap_lower_bound(v),
                Bound::Excluded(v) => self.mmap_upper_bound(v),
            };
            let end_idx = match end {
                Bound::Unbounded => total,
                Bound::Included(v) => self.mmap_upper_bound(v),
                Bound::Excluded(v) => self.mmap_lower_bound(v),
            };
            for (_, ids) in self.mmap_range_entries(start_idx, end_idx) {
                count += ids.len();
            }
        }

        // Overlay range
        for (_, ids) in self.overlay.range((start, end)) {
            count += ids.len();
        }

        count
    }

    /// Count docs matching any of the given values.
    pub fn count_in(&self, values: &[IndexValue]) -> usize {
        let mut count = 0;
        for v in values {
            count += self.count_eq(v);
        }
        count
    }

    /// Total count of all indexed docs.
    pub fn count_all(&self) -> usize {
        let mut count = 0;

        // Mmap entries
        if let Some(layout) = &self.layout {
            for (_, ids) in self.mmap_range_entries(0, layout.entry_count as usize) {
                count += ids.len();
            }
        }

        // Overlay entries
        for ids in self.overlay.values() {
            count += ids.len();
        }

        count
    }

    /// Find docs matching any of the given values.
    pub fn find_in(&self, values: &[IndexValue]) -> BTreeSet<DocumentId> {
        let mut result = BTreeSet::new();
        for v in values {
            result.extend(self.find_eq(v));
        }
        result
    }

    /// All doc IDs in the index.
    pub fn all_ids(&self) -> BTreeSet<DocumentId> {
        let mut result = BTreeSet::new();

        // Mmap
        if let Some(layout) = &self.layout {
            for (_, ids) in self.mmap_range_entries(0, layout.entry_count as usize) {
                result.extend(&ids);
            }
        }

        // Overlay
        for ids in self.overlay.values() {
            result.extend(ids);
        }

        result
    }

    // ── Lazy iteration (callback-based with early termination) ──────────────

    /// Iterate IDs matching `value`, calling `f` per ID. Stops when `f` returns false.
    pub fn for_each_eq<F>(&self, value: &IndexValue, mut f: F)
    where
        F: FnMut(DocumentId) -> bool,
    {
        // Mmap
        let mmap_ids = self.mmap_get(value);
        for &id in &mmap_ids {
            if !f(id) {
                return;
            }
        }
        // Overlay
        if let Some(ids) = self.overlay.get(value) {
            for &id in ids {
                if !f(id) {
                    return;
                }
            }
        }
    }

    /// Iterate IDs NOT matching `value`, calling `f` per ID. Stops when `f` returns false.
    pub fn for_each_ne<F>(&self, value: &IndexValue, mut f: F)
    where
        F: FnMut(DocumentId) -> bool,
    {
        // Mmap
        if let Some(layout) = &self.layout {
            for i in 0..layout.entry_count as usize {
                if let Some(val) = self.mmap_entry_value(i) {
                    if &val != value {
                        let mut ids = self.mmap_entry_docids(i);
                        for (doc_id, removed_vals) in &self.removed {
                            if removed_vals.iter().any(|v| v == &val) {
                                ids.remove(doc_id);
                            }
                        }
                        for &id in &ids {
                            if !f(id) {
                                return;
                            }
                        }
                    }
                }
            }
        }
        // Overlay
        for (k, ids) in &self.overlay {
            if k != value {
                for &id in ids {
                    if !f(id) {
                        return;
                    }
                }
            }
        }
    }

    /// Iterate IDs in a range, calling `f` per ID. Stops when `f` returns false.
    pub fn for_each_in_range<F>(
        &self,
        start: Bound<&IndexValue>,
        end: Bound<&IndexValue>,
        mut f: F,
    ) where
        F: FnMut(DocumentId) -> bool,
    {
        // Collect all entries from both sources, merged and sorted
        let merged = self.merged_range(start, end);
        for (_, ids) in &merged {
            for &id in ids {
                if !f(id) {
                    return;
                }
            }
        }
    }

    /// Iterate IDs matching any of the given values, calling `f` per ID.
    pub fn for_each_in<F>(&self, values: &[IndexValue], mut f: F)
    where
        F: FnMut(DocumentId) -> bool,
    {
        for v in values {
            let mut cont = true;
            self.for_each_eq(v, |id| {
                cont = f(id);
                cont
            });
            if !cont {
                return;
            }
        }
    }

    /// Iterate (value, doc_ids) in ascending order — merges mmap + overlay.
    pub fn iter_asc(&self) -> MmapFieldIndexIter<'_> {
        MmapFieldIndexIter::new(self)
    }

    /// Iterate (value, doc_ids) in descending order — merges mmap + overlay.
    pub fn iter_desc(&self) -> MmapFieldIndexIterDesc<'_> {
        MmapFieldIndexIterDesc::new(self)
    }

    /// Number of unique values in the index (mmap + overlay, approximate).
    pub fn len(&self) -> usize {
        // This is approximate because overlay values may overlap with mmap
        let mmap_count = self
            .layout
            .as_ref()
            .map_or(0, |l| l.entry_count as usize);
        mmap_count + self.overlay.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Remove all entries from the index while keeping field/unique metadata.
    pub fn clear(&mut self) {
        self.overlay.clear();
        self.removed.clear();
        self.mmap = None;
        self.layout = None;
    }

    // ── Merged range helper (for sorted iteration) ──────────────────────────

    /// Produce a merged, sorted list of (IndexValue, DocIdSet) from both mmap
    /// and overlay for a given range. Used by iter_asc/desc and for_each_in_range.
    fn merged_range(
        &self,
        start: Bound<&IndexValue>,
        end: Bound<&IndexValue>,
    ) -> Vec<(IndexValue, DocIdSet)> {
        // Collect mmap entries in range
        let mut mmap_entries: Vec<(IndexValue, DocIdSet)> = Vec::new();
        if let Some(layout) = &self.layout {
            let count = layout.entry_count as usize;
            let start_idx = match start {
                Bound::Unbounded => 0,
                Bound::Included(v) => self.mmap_lower_bound(v),
                Bound::Excluded(v) => self.mmap_upper_bound(v),
            };
            let end_idx = match end {
                Bound::Unbounded => count,
                Bound::Included(v) => self.mmap_upper_bound(v),
                Bound::Excluded(v) => self.mmap_lower_bound(v),
            };
            mmap_entries = self.mmap_range_entries(start_idx, end_idx);
        }

        // Collect overlay entries in range
        let overlay_entries: Vec<(IndexValue, DocIdSet)> = self
            .overlay
            .range((start, end))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        // Merge the two sorted lists
        merge_sorted_entries(mmap_entries, overlay_entries)
    }

    /// Produce all merged entries (for full iteration).
    fn merged_all(&self) -> Vec<(IndexValue, DocIdSet)> {
        self.merged_range(Bound::Unbounded, Bound::Unbounded)
    }

    // ── Persistence ─────────────────────────────────────────────────────────

    /// Write the complete index (mmap + overlay merged) to a `.fidx` v2 file.
    pub fn persist(&mut self) -> io::Result<()> {
        if self.path.as_os_str().is_empty() {
            return Ok(());
        }

        // Collect all entries merged
        let entries = self.merged_all();

        write_fidx_v2(&self.path, &self.field, self.unique, &entries)?;

        // After persisting, reopen the mmap and clear overlay
        self.overlay.clear();
        self.removed.clear();

        // Reopen mmap
        if self.path.exists() {
            let file = fs::File::open(&self.path)?;
            let mmap = unsafe { Mmap::map(&file)? };
            let reopened = Self::parse_mmap_layout(&mmap, &self.path)?;
            self.mmap = Some(mmap);
            self.layout = Some(reopened);
        }

        Ok(())
    }

    fn parse_mmap_layout(mmap: &Mmap, _path: &Path) -> io::Result<MmapLayout> {
        let buf = &mmap[..];
        if buf.len() < HEADER_FIXED + 16 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "fidx v2 file too small",
            ));
        }
        let field_name_len = read_u16(buf, 12) as usize;
        let after_name = HEADER_FIXED + field_name_len;
        let entry_count = read_u64(buf, after_name);
        let doc_count = read_u64(buf, after_name + 8);
        let entry_table_offset = after_name + 16;
        let entry_table_size = entry_count as usize * ENTRY_HEADER_SIZE;
        let string_table_offset = entry_table_offset + entry_table_size;

        let mut string_table_end: usize = 0;
        for i in 0..entry_count as usize {
            let eoff = entry_table_offset + i * ENTRY_HEADER_SIZE;
            if eoff + ENTRY_HEADER_SIZE > buf.len() {
                break;
            }
            let vtype = buf[eoff];
            if vtype == TAG_STRING {
                let str_offset = read_i64(buf, eoff + 1) as usize;
                let str_len = read_u32(buf, eoff + 9) as usize;
                let end = str_offset + str_len;
                if end > string_table_end {
                    string_table_end = end;
                }
            }
        }

        let docid_section_offset = string_table_offset + string_table_end;

        Ok(MmapLayout {
            entry_table_offset,
            string_table_offset,
            docid_section_offset,
            entry_count,
            doc_count,
        })
    }

    /// Convert an existing in-memory FieldIndex into a MmapFieldIndex.
    /// Writes the v2 file and opens it via mmap.
    pub fn rebuild_from(old: &FieldIndex, path: &Path) -> io::Result<Self> {
        // Collect all entries from the old index
        let entries: Vec<(IndexValue, DocIdSet)> = old
            .iter_asc()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        write_fidx_v2(path, &old.field, old.unique, &entries)?;

        // Open the newly written file
        Self::open(path)
    }

    /// Create a MmapFieldIndex from an in-memory FieldIndex (overlay-only, no disk file).
    pub fn from_field_index(old: &FieldIndex) -> Self {
        let mut idx = if old.unique {
            Self::new_unique(old.field.clone())
        } else {
            Self::new(old.field.clone())
        };
        for (val, ids) in old.iter_asc() {
            idx.overlay.insert(val.clone(), ids.clone());
        }
        idx
    }

    /// Convert this MmapFieldIndex to an in-memory FieldIndex (for persistence/compatibility).
    pub fn to_field_index(&self) -> FieldIndex {
        let mut fi = if self.unique {
            FieldIndex::new_unique(self.field.clone())
        } else {
            FieldIndex::new(self.field.clone())
        };
        for (val, ids) in self.merged_all() {
            for &id in &ids {
                fi.insert_raw(id, val.clone());
            }
        }
        fi
    }
}

// ── Write fidx v2 file ──────────────────────────────────────────────────────

fn write_fidx_v2(
    path: &Path,
    field: &str,
    unique: bool,
    entries: &[(IndexValue, DocIdSet)],
) -> io::Result<()> {
    let mut buf = Vec::new();

    // Header
    buf.write_all(MAGIC)?;
    buf.write_all(&FORMAT_VERSION.to_le_bytes())?;
    let flags: u32 = if unique { 1 } else { 0 };
    buf.write_all(&flags.to_le_bytes())?;
    let field_bytes = field.as_bytes();
    buf.write_all(&(field_bytes.len() as u16).to_le_bytes())?;
    buf.write_all(field_bytes)?;

    // Entry count + doc count
    let entry_count = entries.len() as u64;
    let doc_count: u64 = entries.iter().map(|(_, ids)| ids.len() as u64).sum();
    buf.write_all(&entry_count.to_le_bytes())?;
    buf.write_all(&doc_count.to_le_bytes())?;

    // Build string table and entry headers
    let mut string_table = Vec::new();
    let mut entry_headers: Vec<[u8; ENTRY_HEADER_SIZE]> = Vec::with_capacity(entries.len());

    // Also collect docid data
    let mut docid_data: Vec<u8> = Vec::new();

    for (val, ids) in entries {
        let mut header = [0u8; ENTRY_HEADER_SIZE];

        match val {
            IndexValue::Null => {
                header[0] = TAG_NULL;
                // value_i64 = 0, value_len = 0
            }
            IndexValue::Boolean(b) => {
                header[0] = TAG_BOOL;
                let v: i64 = if *b { 1 } else { 0 };
                header[1..9].copy_from_slice(&v.to_le_bytes());
            }
            IndexValue::Integer(i) => {
                header[0] = TAG_INT;
                header[1..9].copy_from_slice(&i.to_le_bytes());
            }
            IndexValue::Float(f) => {
                header[0] = TAG_FLOAT;
                header[1..9].copy_from_slice(&(f.to_bits() as i64).to_le_bytes());
            }
            IndexValue::DateTime(ms) => {
                header[0] = TAG_DATETIME;
                header[1..9].copy_from_slice(&ms.to_le_bytes());
            }
            IndexValue::String(s) => {
                header[0] = TAG_STRING;
                let offset = string_table.len() as i64;
                let s_bytes = s.as_bytes();
                string_table.extend_from_slice(s_bytes);
                header[1..9].copy_from_slice(&offset.to_le_bytes());
                header[9..13].copy_from_slice(&(s_bytes.len() as u32).to_le_bytes());
            }
        }

        // docid_offset (relative to docid section start)
        let docid_offset = docid_data.len() as u64;
        header[13..21].copy_from_slice(&docid_offset.to_le_bytes());

        // docid_count
        let docid_count = ids.len() as u32;
        header[21..25].copy_from_slice(&docid_count.to_le_bytes());

        // Write doc IDs (sorted ascending)
        let mut sorted_ids: Vec<DocumentId> = ids.iter().copied().collect();
        sorted_ids.sort_unstable();
        for id in sorted_ids {
            docid_data.extend_from_slice(&id.to_le_bytes());
        }

        entry_headers.push(header);
    }

    // Write entry table
    for h in &entry_headers {
        buf.write_all(h)?;
    }

    // Write string table
    buf.write_all(&string_table)?;

    // Write docid section
    buf.write_all(&docid_data)?;

    // Atomic write: tmp + rename
    let tmp_path = path.with_extension("fidx2.tmp");
    let mut file = fs::File::create(&tmp_path)?;
    file.write_all(&buf)?;
    file.sync_data()?;
    drop(file);
    fs::rename(&tmp_path, path)?;

    Ok(())
}

// ── Merge two sorted entry lists ─────────────────────────────────────────────

fn merge_sorted_entries(
    mut mmap_entries: Vec<(IndexValue, DocIdSet)>,
    overlay_entries: Vec<(IndexValue, DocIdSet)>,
) -> Vec<(IndexValue, DocIdSet)> {
    if overlay_entries.is_empty() {
        return mmap_entries;
    }
    if mmap_entries.is_empty() {
        return overlay_entries;
    }

    let mut result = Vec::with_capacity(mmap_entries.len() + overlay_entries.len());
    let mut mi = 0;
    let mut oi = 0;

    // Sort overlay_entries (they come from BTreeMap so already sorted)
    while mi < mmap_entries.len() && oi < overlay_entries.len() {
        match mmap_entries[mi].0.cmp(&overlay_entries[oi].0) {
            std::cmp::Ordering::Less => {
                result.push(std::mem::replace(
                    &mut mmap_entries[mi],
                    (IndexValue::Null, DocIdSet::Empty),
                ));
                mi += 1;
            }
            std::cmp::Ordering::Equal => {
                // Merge doc ID sets
                let (val, mut mmap_ids) = std::mem::replace(
                    &mut mmap_entries[mi],
                    (IndexValue::Null, DocIdSet::Empty),
                );
                for &id in &overlay_entries[oi].1 {
                    mmap_ids.insert(id);
                }
                if !mmap_ids.is_empty() {
                    result.push((val, mmap_ids));
                }
                mi += 1;
                oi += 1;
            }
            std::cmp::Ordering::Greater => {
                result.push(overlay_entries[oi].clone());
                oi += 1;
            }
        }
    }
    while mi < mmap_entries.len() {
        result.push(std::mem::replace(
            &mut mmap_entries[mi],
            (IndexValue::Null, DocIdSet::Empty),
        ));
        mi += 1;
    }
    while oi < overlay_entries.len() {
        result.push(overlay_entries[oi].clone());
        oi += 1;
    }

    result
}

// ── Iterators ───────────────────────────────────────────────────────────────

/// Ascending iterator over merged (mmap + overlay) entries.
pub struct MmapFieldIndexIter<'a> {
    entries: Vec<(IndexValue, DocIdSet)>,
    pos: usize,
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> MmapFieldIndexIter<'a> {
    fn new(index: &'a MmapFieldIndex) -> Self {
        let entries = index.merged_all();
        Self {
            entries,
            pos: 0,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<'a> Iterator for MmapFieldIndexIter<'a> {
    type Item = (IndexValue, DocIdSet);

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos < self.entries.len() {
            let entry = std::mem::replace(
                &mut self.entries[self.pos],
                (IndexValue::Null, DocIdSet::Empty),
            );
            self.pos += 1;
            Some(entry)
        } else {
            None
        }
    }
}

/// Descending iterator over merged (mmap + overlay) entries.
pub struct MmapFieldIndexIterDesc<'a> {
    index: &'a MmapFieldIndex,
    mmap_pos: usize,
    overlay_rev: Vec<(IndexValue, DocIdSet)>,
    overlay_pos: usize,
}

impl<'a> MmapFieldIndexIterDesc<'a> {
    fn new(index: &'a MmapFieldIndex) -> Self {
        let mmap_count = index.layout.as_ref().map_or(0, |l| l.entry_count as usize);
        // Overlay is typically small (recent writes only) — safe to clone + reverse
        let overlay_rev: Vec<(IndexValue, DocIdSet)> = index.overlay.clone().into_iter().collect();
        let overlay_pos = overlay_rev.len();
        Self { index, mmap_pos: mmap_count, overlay_rev, overlay_pos }
    }
}

impl<'a> Iterator for MmapFieldIndexIterDesc<'a> {
    type Item = (IndexValue, DocIdSet);

    fn next(&mut self) -> Option<Self::Item> {
        let mm_val = if self.mmap_pos > 0 {
            self.index.mmap_entry_value(self.mmap_pos - 1)
        } else {
            None
        };
        let ov_val = if self.overlay_pos > 0 {
            Some(&self.overlay_rev[self.overlay_pos - 1].0)
        } else {
            None
        };

        match (mm_val, ov_val) {
            (Some(mv), Some(ov)) => {
                if ov >= &mv {
                    self.overlay_pos -= 1;
                    let (v, ids) = self.overlay_rev[self.overlay_pos].clone();
                    if ov == &mv { self.mmap_pos -= 1; } // skip dup
                    Some((v, ids))
                } else {
                    self.mmap_pos -= 1;
                    let ids = self.index.mmap_entry_docids(self.mmap_pos);
                    Some((mv, ids))
                }
            }
            (Some(mv), None) => {
                self.mmap_pos -= 1;
                let ids = self.index.mmap_entry_docids(self.mmap_pos);
                Some((mv, ids))
            }
            (None, Some(_)) => {
                self.overlay_pos -= 1;
                Some(self.overlay_rev[self.overlay_pos].clone())
            }
            (None, None) => None,
        }
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::Document;
    use crate::index::FieldIndex;
    use serde_json::json;
    use tempfile::tempdir;

    fn make_doc(id: u64, data: serde_json::Value) -> Document {
        Document::new(id, data).unwrap()
    }

    #[test]
    fn basic_insert_and_find() {
        let mut idx = MmapFieldIndex::new("status".into());
        let d1 = json!({"status": "active"});
        let d2 = json!({"status": "inactive"});
        let d3 = json!({"status": "active"});
        idx.insert_value(1, &d1);
        idx.insert_value(2, &d2);
        idx.insert_value(3, &d3);

        let result = idx.find_eq(&IndexValue::String("active".into()));
        assert_eq!(result, BTreeSet::from([1, 3]));

        let result = idx.find_eq(&IndexValue::String("inactive".into()));
        assert_eq!(result, BTreeSet::from([2]));
    }

    #[test]
    fn persist_and_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.fidx2");

        let mut idx = MmapFieldIndex::new("status".into());
        idx.set_path(path.clone());
        idx.insert_value(1, &json!({"status": "active"}));
        idx.insert_value(2, &json!({"status": "inactive"}));
        idx.insert_value(3, &json!({"status": "active"}));
        idx.persist().unwrap();

        // Reopen
        let reopened = MmapFieldIndex::open(&path).unwrap();
        assert_eq!(reopened.field, "status");
        assert!(!reopened.unique);
        let result = reopened.find_eq(&IndexValue::String("active".into()));
        assert_eq!(result, BTreeSet::from([1, 3]));
    }

    #[test]
    fn rebuild_from_field_index() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("rebuild.fidx2");

        let mut fi = FieldIndex::new("score".into());
        fi.insert(&make_doc(1, json!({"score": 100})));
        fi.insert(&make_doc(2, json!({"score": 200})));
        fi.insert(&make_doc(3, json!({"score": 100})));

        let mmap_idx = MmapFieldIndex::rebuild_from(&fi, &path).unwrap();
        assert_eq!(mmap_idx.find_eq(&IndexValue::Integer(100)), BTreeSet::from([1, 3]));
        assert_eq!(mmap_idx.find_eq(&IndexValue::Integer(200)), BTreeSet::from([2]));
    }

    #[test]
    fn overlay_and_tombstones() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("overlay.fidx2");

        let mut idx = MmapFieldIndex::new("x".into());
        idx.set_path(path.clone());
        idx.insert_value(1, &json!({"x": 10}));
        idx.insert_value(2, &json!({"x": 20}));
        idx.persist().unwrap();

        // Reopen and add overlay + tombstone
        let mut idx2 = MmapFieldIndex::open(&path).unwrap();
        idx2.set_path(path.clone());
        idx2.insert_value(3, &json!({"x": 10})); // overlay add
        idx2.remove_value(1, &json!({"x": 10})); // tombstone

        assert_eq!(idx2.find_eq(&IndexValue::Integer(10)), BTreeSet::from([3]));
        assert_eq!(idx2.find_eq(&IndexValue::Integer(20)), BTreeSet::from([2]));
    }

    #[test]
    fn range_query() {
        let mut idx = MmapFieldIndex::new("score".into());
        idx.insert_value(1, &json!({"score": 10}));
        idx.insert_value(2, &json!({"score": 20}));
        idx.insert_value(3, &json!({"score": 30}));
        idx.insert_value(4, &json!({"score": 40}));

        let result = idx.find_range(
            Bound::Included(&IndexValue::Integer(20)),
            Bound::Excluded(&IndexValue::Integer(40)),
        );
        assert_eq!(result, BTreeSet::from([2, 3]));
    }

    #[test]
    fn unique_check() {
        let mut idx = MmapFieldIndex::new_unique("email".into());
        idx.insert_value(1, &json!({"email": "a@b.c"}));
        assert!(idx.check_unique(&IndexValue::String("a@b.c".into()), None));
        assert!(!idx.check_unique(&IndexValue::String("a@b.c".into()), Some(1)));
        assert!(!idx.check_unique(&IndexValue::String("d@e.f".into()), None));
    }

    #[test]
    fn iter_asc_order() {
        let mut idx = MmapFieldIndex::new("n".into());
        idx.insert_value(3, &json!({"n": 30}));
        idx.insert_value(1, &json!({"n": 10}));
        idx.insert_value(2, &json!({"n": 20}));

        let vals: Vec<IndexValue> = idx.iter_asc().map(|(v, _)| v).collect();
        assert_eq!(vals, vec![
            IndexValue::Integer(10),
            IndexValue::Integer(20),
            IndexValue::Integer(30),
        ]);
    }

    #[test]
    fn count_all() {
        let mut idx = MmapFieldIndex::new("x".into());
        idx.insert_value(1, &json!({"x": "a"}));
        idx.insert_value(2, &json!({"x": "b"}));
        idx.insert_value(3, &json!({"x": "a"}));
        assert_eq!(idx.count_all(), 3);
    }

    #[test]
    fn mixed_types_persist_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("mixed.fidx2");

        let mut idx = MmapFieldIndex::new("val".into());
        idx.set_path(path.clone());
        idx.insert_raw(1, IndexValue::Null);
        idx.insert_raw(2, IndexValue::Boolean(true));
        idx.insert_raw(3, IndexValue::Integer(42));
        idx.insert_raw(4, IndexValue::Float(3.14));
        idx.insert_raw(5, IndexValue::DateTime(1_700_000_000_000));
        idx.insert_raw(6, IndexValue::String("hello".into()));
        idx.persist().unwrap();

        let reopened = MmapFieldIndex::open(&path).unwrap();
        assert_eq!(reopened.count_all(), 6);
        assert_eq!(reopened.find_eq(&IndexValue::Integer(42)), BTreeSet::from([3]));
        assert_eq!(reopened.find_eq(&IndexValue::String("hello".into())), BTreeSet::from([6]));
        assert_eq!(reopened.find_eq(&IndexValue::Null), BTreeSet::from([1]));
    }
}
