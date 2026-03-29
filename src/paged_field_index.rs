//! Cache-friendly field index using sorted Vec with binary search.
//!
//! Replaces `BTreeMap<IndexValue, DocIdSet>` (poor cache locality at 1M entries
//! due to pointer chasing) with a contiguous `Vec<(IndexValue, DocIdSet)>`
//! where binary search touches ~20 cache lines for 1M entries (log2(1M) = 20),
//! compared to ~20 random memory locations for BTreeMap.
//!
//! Write buffer absorbs recent inserts and merges them periodically into the
//! sorted main array, giving amortized O(1) inserts with O(log n) lookups.

use std::collections::BTreeSet;
use std::io::{self, Read, Write};
use std::ops::Bound;

use serde_json::Value;

use crate::document::DocumentId;
use crate::index::DocIdSet;
use crate::value::IndexValue;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Resolve a field path (with dot notation) directly on a &Value.
fn resolve_value_field<'a>(data: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = data;
    for part in path.split('.') {
        current = current.as_object()?.get(part)?;
    }
    Some(current)
}

// ---------------------------------------------------------------------------
// Write buffer entry
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum WriteOp {
    Insert(IndexValue, DocumentId),
    Remove(IndexValue, DocumentId),
}

// ---------------------------------------------------------------------------
// PagedFieldIndex
// ---------------------------------------------------------------------------

/// A cache-friendly field index using a sorted Vec with binary search.
///
/// The main `entries` Vec is contiguous memory. Binary search on 1M entries
/// touches ~20 cache lines (log2(1M) = 20), compared to ~20 random memory
/// locations for BTreeMap's pointer-chasing tree traversal.
///
/// Recent writes go into a `write_buffer` that is periodically merged into
/// the sorted entries array.
#[derive(Debug)]
pub struct PagedFieldIndex {
    pub field: String,
    pub unique: bool,
    /// Main sorted array — contiguous memory, cache-friendly binary search.
    entries: Vec<(IndexValue, DocIdSet)>,
    /// Write buffer for recent mutations — merged when full.
    write_buffer: Vec<WriteOp>,
    /// Maximum write buffer size before automatic merge.
    write_buffer_limit: usize,
}

impl PagedFieldIndex {
    pub fn new(field: String) -> Self {
        Self {
            field,
            unique: false,
            entries: Vec::new(),
            write_buffer: Vec::new(),
            write_buffer_limit: 1000,
        }
    }

    pub fn new_unique(field: String) -> Self {
        Self {
            field,
            unique: true,
            entries: Vec::new(),
            write_buffer: Vec::new(),
            write_buffer_limit: 1000,
        }
    }

    // -- Write operations ----------------------------------------------------

    /// Insert a document into the index by extracting the field value from the document.
    pub fn insert_value(&mut self, id: DocumentId, data: &Value) {
        if let Some(value) = resolve_value_field(data, &self.field) {
            let key = IndexValue::from_json(value);
            self.insert_raw(id, key);
        }
    }

    /// Insert a pre-computed IndexValue directly.
    pub fn insert_raw(&mut self, id: DocumentId, key: IndexValue) {
        // Try direct insertion into entries for better performance on single ops
        if self.write_buffer.is_empty() {
            match self.entries.binary_search_by(|e| e.0.cmp(&key)) {
                Ok(pos) => {
                    self.entries[pos].1.insert(id);
                    return;
                }
                Err(pos) => {
                    self.entries.insert(pos, (key, DocIdSet::One(id)));
                    return;
                }
            }
        }

        self.write_buffer.push(WriteOp::Insert(key, id));
        if self.write_buffer.len() >= self.write_buffer_limit {
            self.flush_write_buffer();
        }
    }

    /// Remove a document from the index by extracting the field value.
    pub fn remove_value(&mut self, id: DocumentId, data: &Value) {
        if let Some(value) = resolve_value_field(data, &self.field) {
            let key = IndexValue::from_json(value);
            self.remove_raw(id, key);
        }
    }

    /// Remove a document with a pre-computed IndexValue.
    fn remove_raw(&mut self, id: DocumentId, key: IndexValue) {
        // Try direct removal from entries
        if self.write_buffer.is_empty() {
            if let Ok(pos) = self.entries.binary_search_by(|e| e.0.cmp(&key)) {
                let removed = self.entries[pos].1.remove(&id);
                if removed && self.entries[pos].1.is_empty() {
                    self.entries.remove(pos);
                }
            }
            return;
        }

        self.write_buffer.push(WriteOp::Remove(key, id));
        if self.write_buffer.len() >= self.write_buffer_limit {
            self.flush_write_buffer();
        }
    }

    /// Flush the write buffer, merging all pending operations into the sorted entries.
    pub fn flush_write_buffer(&mut self) {
        if self.write_buffer.is_empty() {
            return;
        }

        let ops = std::mem::take(&mut self.write_buffer);

        for op in ops {
            match op {
                WriteOp::Insert(key, id) => {
                    match self.entries.binary_search_by(|e| e.0.cmp(&key)) {
                        Ok(pos) => {
                            self.entries[pos].1.insert(id);
                        }
                        Err(pos) => {
                            self.entries.insert(pos, (key, DocIdSet::One(id)));
                        }
                    }
                }
                WriteOp::Remove(key, id) => {
                    if let Ok(pos) = self.entries.binary_search_by(|e| e.0.cmp(&key)) {
                        let removed = self.entries[pos].1.remove(&id);
                        if removed && self.entries[pos].1.is_empty() {
                            self.entries.remove(pos);
                        }
                    }
                }
            }
        }
    }

    // -- Binary search helpers -----------------------------------------------

    /// Binary search for a key in the sorted entries.
    #[inline]
    fn find_pos(&self, key: &IndexValue) -> Result<usize, usize> {
        self.entries.binary_search_by(|e| e.0.cmp(key))
    }

    /// Find the range of entry indices matching the given bounds.
    fn range_positions(
        &self,
        start: Bound<&IndexValue>,
        end: Bound<&IndexValue>,
    ) -> (usize, usize) {
        let lo = match start {
            Bound::Included(v) => {
                match self.find_pos(v) {
                    Ok(pos) => pos,
                    Err(pos) => pos,
                }
            }
            Bound::Excluded(v) => {
                match self.find_pos(v) {
                    Ok(pos) => pos + 1,
                    Err(pos) => pos,
                }
            }
            Bound::Unbounded => 0,
        };

        let hi = match end {
            Bound::Included(v) => {
                match self.find_pos(v) {
                    Ok(pos) => pos + 1,
                    Err(pos) => pos,
                }
            }
            Bound::Excluded(v) => {
                match self.find_pos(v) {
                    Ok(pos) => pos,
                    Err(pos) => pos,
                }
            }
            Bound::Unbounded => self.entries.len(),
        };

        (lo, hi.min(self.entries.len()))
    }

    // -- Query helpers -------------------------------------------------------

    pub fn find_eq(&self, value: &IndexValue) -> BTreeSet<DocumentId> {
        // Check write buffer
        if !self.write_buffer.is_empty() {
            let mut result = BTreeSet::new();
            if let Ok(pos) = self.find_pos(value) {
                result = self.entries[pos].1.to_btreeset();
            }
            for op in &self.write_buffer {
                match op {
                    WriteOp::Insert(k, id) if k == value => { result.insert(*id); }
                    WriteOp::Remove(k, id) if k == value => { result.remove(id); }
                    _ => {}
                }
            }
            return result;
        }

        match self.find_pos(value) {
            Ok(pos) => self.entries[pos].1.to_btreeset(),
            Err(_) => BTreeSet::new(),
        }
    }

    pub fn count_eq(&self, value: &IndexValue) -> usize {
        if !self.write_buffer.is_empty() {
            return self.find_eq(value).len();
        }
        match self.find_pos(value) {
            Ok(pos) => self.entries[pos].1.len(),
            Err(_) => 0,
        }
    }

    pub fn count_range(
        &self,
        start: Bound<&IndexValue>,
        end: Bound<&IndexValue>,
    ) -> usize {
        if !self.write_buffer.is_empty() {
            return self.find_range(start, end).len();
        }
        let (lo, hi) = self.range_positions(start, end);
        let mut count = 0;
        for i in lo..hi {
            count += self.entries[i].1.len();
        }
        count
    }

    pub fn count_in(&self, values: &[IndexValue]) -> usize {
        if !self.write_buffer.is_empty() {
            return self.find_in(values).len();
        }
        let mut count = 0;
        for v in values {
            if let Ok(pos) = self.find_pos(v) {
                count += self.entries[pos].1.len();
            }
        }
        count
    }

    pub fn count_all(&self) -> usize {
        if !self.write_buffer.is_empty() {
            return self.all_ids().len();
        }
        let mut count = 0;
        for (_, ids) in &self.entries {
            count += ids.len();
        }
        count
    }

    pub fn find_ne(&self, value: &IndexValue) -> BTreeSet<DocumentId> {
        if !self.write_buffer.is_empty() {
            let mut result = BTreeSet::new();
            for (k, ids) in &self.entries {
                if k != value {
                    for &id in ids { result.insert(id); }
                }
            }
            for op in &self.write_buffer {
                match op {
                    WriteOp::Insert(k, id) if k != value => { result.insert(*id); }
                    WriteOp::Remove(k, id) if k != value => { result.remove(id); }
                    _ => {}
                }
            }
            return result;
        }

        let mut result = BTreeSet::new();
        for (k, ids) in &self.entries {
            if k != value {
                for &id in ids { result.insert(id); }
            }
        }
        result
    }

    pub fn find_range(
        &self,
        start: Bound<&IndexValue>,
        end: Bound<&IndexValue>,
    ) -> BTreeSet<DocumentId> {
        if !self.write_buffer.is_empty() {
            let mut result = BTreeSet::new();
            let (lo, hi) = self.range_positions(start, end);
            for i in lo..hi {
                for &id in &self.entries[i].1 { result.insert(id); }
            }
            for op in &self.write_buffer {
                match op {
                    WriteOp::Insert(k, id) => {
                        if in_bounds(k, start, end) { result.insert(*id); }
                    }
                    WriteOp::Remove(k, id) => {
                        if in_bounds(k, start, end) { result.remove(id); }
                    }
                }
            }
            return result;
        }

        let (lo, hi) = self.range_positions(start, end);
        let mut result = BTreeSet::new();
        for i in lo..hi {
            for &id in &self.entries[i].1 { result.insert(id); }
        }
        result
    }

    pub fn find_in(&self, values: &[IndexValue]) -> BTreeSet<DocumentId> {
        if !self.write_buffer.is_empty() {
            let mut result = BTreeSet::new();
            for v in values {
                if let Ok(pos) = self.find_pos(v) {
                    for &id in &self.entries[pos].1 { result.insert(id); }
                }
            }
            for op in &self.write_buffer {
                match op {
                    WriteOp::Insert(k, id) if values.contains(k) => { result.insert(*id); }
                    WriteOp::Remove(k, id) if values.contains(k) => { result.remove(id); }
                    _ => {}
                }
            }
            return result;
        }

        let mut result = BTreeSet::new();
        for v in values {
            if let Ok(pos) = self.find_pos(v) {
                for &id in &self.entries[pos].1 { result.insert(id); }
            }
        }
        result
    }

    pub fn all_ids(&self) -> BTreeSet<DocumentId> {
        let mut result = BTreeSet::new();
        for (_, ids) in &self.entries {
            for &id in ids { result.insert(id); }
        }
        for op in &self.write_buffer {
            match op {
                WriteOp::Insert(_, id) => { result.insert(*id); }
                WriteOp::Remove(_, id) => { result.remove(id); }
            }
        }
        result
    }

    // -- Lazy iteration helpers (early-termination via callback) ---------------

    pub fn for_each_eq<F>(&self, value: &IndexValue, mut f: F)
    where
        F: FnMut(DocumentId) -> bool,
    {
        if !self.write_buffer.is_empty() {
            for id in self.find_eq(value) {
                if !f(id) { return; }
            }
            return;
        }
        if let Ok(pos) = self.find_pos(value) {
            for &id in &self.entries[pos].1 {
                if !f(id) { return; }
            }
        }
    }

    pub fn for_each_ne<F>(&self, value: &IndexValue, mut f: F)
    where
        F: FnMut(DocumentId) -> bool,
    {
        if !self.write_buffer.is_empty() {
            for id in self.find_ne(value) {
                if !f(id) { return; }
            }
            return;
        }
        for (k, ids) in &self.entries {
            if k != value {
                for &id in ids {
                    if !f(id) { return; }
                }
            }
        }
    }

    pub fn for_each_in_range<F>(
        &self,
        start: Bound<&IndexValue>,
        end: Bound<&IndexValue>,
        mut f: F,
    ) where
        F: FnMut(DocumentId) -> bool,
    {
        if !self.write_buffer.is_empty() {
            for id in self.find_range(start, end) {
                if !f(id) { return; }
            }
            return;
        }
        let (lo, hi) = self.range_positions(start, end);
        for i in lo..hi {
            for &id in &self.entries[i].1 {
                if !f(id) { return; }
            }
        }
    }

    pub fn for_each_in<F>(&self, values: &[IndexValue], mut f: F)
    where
        F: FnMut(DocumentId) -> bool,
    {
        if !self.write_buffer.is_empty() {
            for id in self.find_in(values) {
                if !f(id) { return; }
            }
            return;
        }
        for v in values {
            if let Ok(pos) = self.find_pos(v) {
                for &id in &self.entries[pos].1 {
                    if !f(id) { return; }
                }
            }
        }
    }

    // -- Sorted iteration -----------------------------------------------------

    /// Iterate (value, doc_ids) in ascending order.
    pub fn iter_asc(&self) -> impl Iterator<Item = (&IndexValue, &DocIdSet)> {
        self.entries.iter().map(|(k, v)| (k, v))
    }

    /// Iterate (value, doc_ids) in descending order.
    pub fn iter_desc(&self) -> impl Iterator<Item = (&IndexValue, &DocIdSet)> {
        self.entries.iter().rev().map(|(k, v)| (k, v))
    }

    // -- Unique constraint check ---------------------------------------------

    pub fn check_unique(&self, value: &IndexValue, exclude_id: Option<DocumentId>) -> bool {
        if !self.write_buffer.is_empty() {
            let ids = self.find_eq(value);
            return match exclude_id {
                Some(eid) => ids.iter().any(|&id| id != eid),
                None => !ids.is_empty(),
            };
        }
        if let Ok(pos) = self.find_pos(value) {
            let ids = &self.entries[pos].1;
            match exclude_id {
                Some(eid) => ids.iter().any(|&id| id != eid),
                None => !ids.is_empty(),
            }
        } else {
            false
        }
    }

    // -- Misc ----------------------------------------------------------------

    pub fn clear(&mut self) {
        self.entries.clear();
        self.write_buffer.clear();
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.write_buffer.is_empty()
    }

    /// Build the index from pre-sorted (IndexValue, DocumentId) pairs.
    /// Much faster than per-item `insert_raw` for bulk index building because
    /// it avoids O(n) Vec::insert shifts — pairs are already in order so we
    /// just append or extend the last entry.
    ///
    /// IMPORTANT: `pairs` MUST be sorted by IndexValue (ascending).
    pub fn build_from_sorted(&mut self, pairs: Vec<(IndexValue, DocumentId)>) {
        self.entries.clear();
        self.write_buffer.clear();
        if pairs.is_empty() {
            return;
        }
        // Pre-allocate: at most pairs.len() unique values
        self.entries.reserve(pairs.len() / 2);
        for (val, id) in pairs {
            if let Some(last) = self.entries.last_mut() {
                if last.0 == val {
                    last.1.insert(id);
                    continue;
                }
            }
            self.entries.push((val, DocIdSet::One(id)));
        }
    }

    /// Merge another PagedFieldIndex into this one.
    /// Used for parallel index building.
    pub fn merge(&mut self, mut other: Self) {
        self.flush_write_buffer();
        other.flush_write_buffer();
        for (key, ids) in other.entries {
            match self.entries.binary_search_by(|e| e.0.cmp(&key)) {
                Ok(pos) => {
                    for &id in &ids {
                        self.entries[pos].1.insert(id);
                    }
                }
                Err(pos) => {
                    self.entries.insert(pos, (key, ids));
                }
            }
        }
    }

    // -- Binary serialization (same format as FieldIndex) --------------------

    /// Serialize the entire field index to a binary writer.
    /// Format: [field_name_len:u32][field_name][unique:u8][entry_count:u32]
    ///   per entry: [IndexValue][doc_count:u32][doc_ids as u64 LE...]
    pub fn write_to<W: Write>(&self, w: &mut W) -> io::Result<()> {
        let name_bytes = self.field.as_bytes();
        w.write_all(&(name_bytes.len() as u32).to_le_bytes())?;
        w.write_all(name_bytes)?;
        w.write_all(&[self.unique as u8])?;
        // Count entries (skip empties from write buffer)
        let entry_count = self.entries.iter().filter(|(_, ids)| !ids.is_empty()).count();
        w.write_all(&(entry_count as u32).to_le_bytes())?;
        for (key, ids) in &self.entries {
            if ids.is_empty() { continue; }
            key.write_to(w)?;
            w.write_all(&(ids.len() as u32).to_le_bytes())?;
            for &id in ids {
                w.write_all(&id.to_le_bytes())?;
            }
        }
        Ok(())
    }

    /// Deserialize a field index from a binary reader.
    /// Compatible with FieldIndex binary format.
    pub fn read_from<R: Read>(r: &mut R) -> io::Result<Self> {
        let mut len_buf = [0u8; 4];
        r.read_exact(&mut len_buf)?;
        let name_len = u32::from_le_bytes(len_buf) as usize;
        let mut name_buf = vec![0u8; name_len];
        r.read_exact(&mut name_buf)?;
        let field = String::from_utf8(name_buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let mut unique_buf = [0u8; 1];
        r.read_exact(&mut unique_buf)?;
        let unique = unique_buf[0] != 0;
        r.read_exact(&mut len_buf)?;
        let entry_count = u32::from_le_bytes(len_buf) as usize;
        let mut entries = Vec::with_capacity(entry_count);
        let mut id_buf = [0u8; 8];
        for _ in 0..entry_count {
            let key = IndexValue::read_from(r)?;
            r.read_exact(&mut len_buf)?;
            let doc_count = u32::from_le_bytes(len_buf) as usize;
            let ids = if doc_count == 0 {
                DocIdSet::Empty
            } else if doc_count == 1 {
                r.read_exact(&mut id_buf)?;
                DocIdSet::One(u64::from_le_bytes(id_buf))
            } else {
                let mut set = std::collections::BTreeSet::new();
                for _ in 0..doc_count {
                    r.read_exact(&mut id_buf)?;
                    set.insert(u64::from_le_bytes(id_buf));
                }
                DocIdSet::Set(set)
            };
            entries.push((key, ids));
        }
        Ok(Self {
            field,
            unique,
            entries,
            write_buffer: Vec::new(),
            write_buffer_limit: 1000,
        })
    }
}

// ---------------------------------------------------------------------------
// Helper: check if key is within bounds
// ---------------------------------------------------------------------------

fn in_bounds(key: &IndexValue, start: Bound<&IndexValue>, end: Bound<&IndexValue>) -> bool {
    let lo_ok = match start {
        Bound::Included(v) => key >= v,
        Bound::Excluded(v) => key > v,
        Bound::Unbounded => true,
    };
    let hi_ok = match end {
        Bound::Included(v) => key <= v,
        Bound::Excluded(v) => key < v,
        Bound::Unbounded => true,
    };
    lo_ok && hi_ok
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn basic_insert_and_find() {
        let mut idx = PagedFieldIndex::new("status".into());
        idx.insert_value(1, &json!({"status": "active"}));
        idx.insert_value(2, &json!({"status": "inactive"}));
        idx.insert_value(3, &json!({"status": "active"}));

        let result = idx.find_eq(&IndexValue::String("active".into()));
        assert_eq!(result, BTreeSet::from([1, 3]));
    }

    #[test]
    fn insert_raw_and_find() {
        let mut idx = PagedFieldIndex::new("x".into());
        idx.insert_raw(1, IndexValue::Integer(10));
        idx.insert_raw(2, IndexValue::Integer(20));
        idx.insert_raw(3, IndexValue::Integer(10));

        assert_eq!(idx.find_eq(&IndexValue::Integer(10)), BTreeSet::from([1, 3]));
        assert_eq!(idx.find_eq(&IndexValue::Integer(20)), BTreeSet::from([2]));
        assert_eq!(idx.find_eq(&IndexValue::Integer(30)), BTreeSet::new());
    }

    #[test]
    fn remove_value() {
        let mut idx = PagedFieldIndex::new("x".into());
        idx.insert_value(1, &json!({"x": 10}));
        idx.insert_value(2, &json!({"x": 10}));

        idx.remove_value(1, &json!({"x": 10}));

        assert_eq!(idx.find_eq(&IndexValue::Integer(10)), BTreeSet::from([2]));
    }

    #[test]
    fn remove_last_entry_removes_key() {
        let mut idx = PagedFieldIndex::new("x".into());
        idx.insert_raw(1, IndexValue::Integer(10));
        idx.remove_raw(1, IndexValue::Integer(10));

        assert_eq!(idx.len(), 0);
        assert_eq!(idx.find_eq(&IndexValue::Integer(10)), BTreeSet::new());
    }

    #[test]
    fn find_range() {
        let mut idx = PagedFieldIndex::new("age".into());
        for i in 0..100 {
            idx.insert_raw(i, IndexValue::Integer(i as i64));
        }

        let result = idx.find_range(
            Bound::Included(&IndexValue::Integer(10)),
            Bound::Excluded(&IndexValue::Integer(20)),
        );
        let expected: BTreeSet<u64> = (10..20).collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn find_ne() {
        let mut idx = PagedFieldIndex::new("x".into());
        idx.insert_raw(1, IndexValue::Integer(10));
        idx.insert_raw(2, IndexValue::Integer(20));
        idx.insert_raw(3, IndexValue::Integer(10));

        let result = idx.find_ne(&IndexValue::Integer(10));
        assert_eq!(result, BTreeSet::from([2]));
    }

    #[test]
    fn find_in() {
        let mut idx = PagedFieldIndex::new("x".into());
        idx.insert_raw(1, IndexValue::Integer(10));
        idx.insert_raw(2, IndexValue::Integer(20));
        idx.insert_raw(3, IndexValue::Integer(30));

        let result = idx.find_in(&[IndexValue::Integer(10), IndexValue::Integer(30)]);
        assert_eq!(result, BTreeSet::from([1, 3]));
    }

    #[test]
    fn count_operations() {
        let mut idx = PagedFieldIndex::new("x".into());
        idx.insert_raw(1, IndexValue::Integer(10));
        idx.insert_raw(2, IndexValue::Integer(10));
        idx.insert_raw(3, IndexValue::Integer(20));

        assert_eq!(idx.count_eq(&IndexValue::Integer(10)), 2);
        assert_eq!(idx.count_eq(&IndexValue::Integer(20)), 1);
        assert_eq!(idx.count_eq(&IndexValue::Integer(30)), 0);
        assert_eq!(idx.count_all(), 3);
    }

    #[test]
    fn count_range() {
        let mut idx = PagedFieldIndex::new("x".into());
        for i in 0..100 {
            idx.insert_raw(i, IndexValue::Integer(i as i64));
        }
        let count = idx.count_range(
            Bound::Included(&IndexValue::Integer(10)),
            Bound::Excluded(&IndexValue::Integer(20)),
        );
        assert_eq!(count, 10);
    }

    #[test]
    fn all_ids() {
        let mut idx = PagedFieldIndex::new("x".into());
        idx.insert_raw(1, IndexValue::Integer(10));
        idx.insert_raw(2, IndexValue::Integer(20));

        assert_eq!(idx.all_ids(), BTreeSet::from([1, 2]));
    }

    #[test]
    fn unique_constraint() {
        let mut idx = PagedFieldIndex::new_unique("email".into());
        idx.insert_raw(1, IndexValue::String("a@b.c".into()));

        assert!(idx.check_unique(&IndexValue::String("a@b.c".into()), None));
        assert!(!idx.check_unique(&IndexValue::String("a@b.c".into()), Some(1)));
        assert!(!idx.check_unique(&IndexValue::String("new@b.c".into()), None));
    }

    #[test]
    fn write_buffer_flush() {
        let mut idx = PagedFieldIndex::new("x".into());
        idx.insert_raw(1, IndexValue::Integer(10));
        // Manually push to write_buffer
        idx.write_buffer.push(WriteOp::Insert(IndexValue::Integer(20), 2));
        idx.write_buffer.push(WriteOp::Insert(IndexValue::Integer(30), 3));

        // Before flush, find_eq should still work (handles write buffer)
        assert_eq!(idx.find_eq(&IndexValue::Integer(20)), BTreeSet::from([2]));

        idx.flush_write_buffer();
        assert_eq!(idx.find_eq(&IndexValue::Integer(20)), BTreeSet::from([2]));
        assert_eq!(idx.find_eq(&IndexValue::Integer(30)), BTreeSet::from([3]));
    }

    #[test]
    fn clear() {
        let mut idx = PagedFieldIndex::new("x".into());
        idx.insert_raw(1, IndexValue::Integer(10));
        idx.clear();
        assert_eq!(idx.find_eq(&IndexValue::Integer(10)), BTreeSet::new());
    }

    #[test]
    fn iter_asc_desc() {
        let mut idx = PagedFieldIndex::new("x".into());
        idx.insert_raw(1, IndexValue::Integer(30));
        idx.insert_raw(2, IndexValue::Integer(10));
        idx.insert_raw(3, IndexValue::Integer(20));

        let asc: Vec<i64> = idx.iter_asc()
            .filter_map(|(k, _)| if let IndexValue::Integer(i) = k { Some(*i) } else { None })
            .collect();
        assert_eq!(asc, vec![10, 20, 30]);

        let desc: Vec<i64> = idx.iter_desc()
            .filter_map(|(k, _)| if let IndexValue::Integer(i) = k { Some(*i) } else { None })
            .collect();
        assert_eq!(desc, vec![30, 20, 10]);
    }

    #[test]
    fn for_each_eq_early_termination() {
        let mut idx = PagedFieldIndex::new("x".into());
        idx.insert_raw(1, IndexValue::Integer(10));
        idx.insert_raw(2, IndexValue::Integer(10));
        idx.insert_raw(3, IndexValue::Integer(10));

        let mut collected = Vec::new();
        idx.for_each_eq(&IndexValue::Integer(10), |id| {
            collected.push(id);
            collected.len() < 2 // stop after 2
        });
        assert_eq!(collected.len(), 2);
    }

    #[test]
    fn merge_indexes() {
        let mut idx1 = PagedFieldIndex::new("x".into());
        idx1.insert_raw(1, IndexValue::Integer(10));
        idx1.insert_raw(2, IndexValue::Integer(20));

        let mut idx2 = PagedFieldIndex::new("x".into());
        idx2.insert_raw(3, IndexValue::Integer(10));
        idx2.insert_raw(4, IndexValue::Integer(30));

        idx1.merge(idx2);

        assert_eq!(idx1.find_eq(&IndexValue::Integer(10)), BTreeSet::from([1, 3]));
        assert_eq!(idx1.find_eq(&IndexValue::Integer(20)), BTreeSet::from([2]));
        assert_eq!(idx1.find_eq(&IndexValue::Integer(30)), BTreeSet::from([4]));
    }

    #[test]
    fn binary_roundtrip() {
        let mut idx = PagedFieldIndex::new("status".into());
        idx.insert_raw(1, IndexValue::String("active".into()));
        idx.insert_raw(2, IndexValue::String("inactive".into()));
        idx.insert_raw(3, IndexValue::String("active".into()));

        let mut buf = Vec::new();
        idx.write_to(&mut buf).unwrap();
        let decoded = PagedFieldIndex::read_from(&mut &buf[..]).unwrap();

        assert_eq!(decoded.field, "status");
        assert_eq!(decoded.find_eq(&IndexValue::String("active".into())), BTreeSet::from([1, 3]));
        assert_eq!(decoded.find_eq(&IndexValue::String("inactive".into())), BTreeSet::from([2]));
    }

    #[test]
    fn binary_compat_with_field_index() {
        // Write with FieldIndex, read with PagedFieldIndex
        let mut fi = crate::index::FieldIndex::new("age".into());
        fi.insert_raw(10, IndexValue::Integer(25));
        fi.insert_raw(20, IndexValue::Integer(30));
        fi.insert_raw(30, IndexValue::Integer(25));

        let mut buf = Vec::new();
        fi.write_to(&mut buf).unwrap();
        let pfi = PagedFieldIndex::read_from(&mut &buf[..]).unwrap();

        assert_eq!(pfi.field, "age");
        assert_eq!(pfi.find_eq(&IndexValue::Integer(25)), BTreeSet::from([10, 30]));
        assert_eq!(pfi.find_eq(&IndexValue::Integer(30)), BTreeSet::from([20]));
    }
}
