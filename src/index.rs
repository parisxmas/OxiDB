use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Read, Write};
use std::ops::Bound;

use serde_json::Value;

use crate::document::{Document, DocumentId};
use crate::value::IndexValue;

// ---------------------------------------------------------------------------
// DocIdSet — compact storage for document ID sets in indexes
// ---------------------------------------------------------------------------

/// Compact storage for document ID sets in field/composite indexes.
///
/// Single-element sets (common for high-cardinality fields like order_id
/// or amount) are stored inline without heap allocation, saving ~80 bytes
/// per entry compared to `BTreeSet`.
#[derive(Debug, Clone)]
pub enum DocIdSet {
    Empty,
    One(DocumentId),
    Set(BTreeSet<DocumentId>),
}

impl Default for DocIdSet {
    fn default() -> Self {
        DocIdSet::Empty
    }
}

impl DocIdSet {
    pub fn insert(&mut self, id: DocumentId) -> bool {
        match self {
            DocIdSet::Empty => {
                *self = DocIdSet::One(id);
                true
            }
            DocIdSet::One(existing) => {
                if *existing == id {
                    false
                } else {
                    let mut set = BTreeSet::new();
                    set.insert(*existing);
                    set.insert(id);
                    *self = DocIdSet::Set(set);
                    true
                }
            }
            DocIdSet::Set(set) => set.insert(id),
        }
    }

    pub fn remove(&mut self, id: &DocumentId) -> bool {
        match self {
            DocIdSet::Empty => false,
            DocIdSet::One(existing) => {
                if existing == id {
                    *self = DocIdSet::Empty;
                    true
                } else {
                    false
                }
            }
            DocIdSet::Set(set) => {
                let removed = set.remove(id);
                if removed && set.len() == 1 {
                    let remaining = *set.iter().next().unwrap();
                    *self = DocIdSet::One(remaining);
                }
                removed
            }
        }
    }

    pub fn contains(&self, id: &DocumentId) -> bool {
        match self {
            DocIdSet::Empty => false,
            DocIdSet::One(existing) => existing == id,
            DocIdSet::Set(set) => set.contains(id),
        }
    }

    pub fn is_empty(&self) -> bool {
        matches!(self, DocIdSet::Empty)
    }

    pub fn len(&self) -> usize {
        match self {
            DocIdSet::Empty => 0,
            DocIdSet::One(_) => 1,
            DocIdSet::Set(set) => set.len(),
        }
    }

    pub fn iter(&self) -> DocIdSetIter<'_> {
        match self {
            DocIdSet::Empty => DocIdSetIter::Empty,
            DocIdSet::One(id) => DocIdSetIter::One(Some(id)),
            DocIdSet::Set(set) => DocIdSetIter::Set(set.iter()),
        }
    }

    /// Approximate heap bytes owned beyond the inline enum size. For `Set`,
    /// counts the BTreeSet's node storage (~each `u64` plus B-tree node
    /// bookkeeping). Used for memory accounting.
    pub fn heap_bytes(&self) -> usize {
        match self {
            DocIdSet::Set(s) => s.len() * (std::mem::size_of::<DocumentId>() + 8),
            _ => 0,
        }
    }

    /// Convert to a BTreeSet (for query result APIs that return BTreeSet).
    pub fn to_btreeset(&self) -> BTreeSet<DocumentId> {
        match self {
            DocIdSet::Empty => BTreeSet::new(),
            DocIdSet::One(id) => BTreeSet::from([*id]),
            DocIdSet::Set(set) => set.clone(),
        }
    }
}

/// Iterator over DocIdSet elements in ascending order.
pub enum DocIdSetIter<'a> {
    Empty,
    One(Option<&'a DocumentId>),
    Set(std::collections::btree_set::Iter<'a, DocumentId>),
}

impl<'a> Iterator for DocIdSetIter<'a> {
    type Item = &'a DocumentId;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            DocIdSetIter::Empty => None,
            DocIdSetIter::One(id) => id.take(),
            DocIdSetIter::Set(iter) => iter.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            DocIdSetIter::Empty => (0, Some(0)),
            DocIdSetIter::One(Some(_)) => (1, Some(1)),
            DocIdSetIter::One(None) => (0, Some(0)),
            DocIdSetIter::Set(iter) => iter.size_hint(),
        }
    }
}

impl<'a> DoubleEndedIterator for DocIdSetIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            DocIdSetIter::Empty => None,
            DocIdSetIter::One(id) => id.take(),
            DocIdSetIter::Set(iter) => iter.next_back(),
        }
    }
}

impl<'a> IntoIterator for &'a DocIdSet {
    type Item = &'a DocumentId;
    type IntoIter = DocIdSetIter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

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
// Single-field index
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct FieldIndex {
    pub field: String,
    pub unique: bool,
    tree: BTreeMap<IndexValue, DocIdSet>,
}

impl FieldIndex {
    pub fn new(field: String) -> Self {
        Self {
            field,
            unique: false,
            tree: BTreeMap::new(),
        }
    }

    pub fn new_unique(field: String) -> Self {
        Self {
            field,
            unique: true,
            tree: BTreeMap::new(),
        }
    }

    /// Check if a value already exists in the index for a different document.
    pub fn check_unique(&self, value: &IndexValue, exclude_id: Option<DocumentId>) -> bool {
        if let Some(ids) = self.tree.get(value) {
            match exclude_id {
                Some(eid) => ids.iter().any(|&id| id != eid),
                None => !ids.is_empty(),
            }
        } else {
            false
        }
    }

    pub fn insert(&mut self, doc: &Document) {
        if let Some(value) = doc.get_field(&self.field) {
            let key = IndexValue::from_json(value);
            self.tree.entry(key).or_default().insert(doc.id);
        }
    }

    /// Insert using a &Value directly — avoids constructing a Document.
    pub fn insert_value(&mut self, id: DocumentId, data: &Value) {
        if let Some(value) = resolve_value_field(data, &self.field) {
            let key = IndexValue::from_json(value);
            self.tree.entry(key).or_default().insert(id);
        }
    }

    /// Insert a pre-computed IndexValue directly, bypassing field resolution.
    pub fn insert_raw(&mut self, id: DocumentId, key: IndexValue) {
        self.tree.entry(key).or_default().insert(id);
    }

    /// Merge another FieldIndex into this one. Used for parallel index building.
    pub fn merge(&mut self, other: FieldIndex) {
        for (key, ids) in other.tree {
            let entry = self.tree.entry(key).or_default();
            for id in ids.iter() {
                entry.insert(*id);
            }
        }
    }

    pub fn remove(&mut self, doc: &Document) {
        if let Some(value) = doc.get_field(&self.field) {
            let key = IndexValue::from_json(value);
            if let Some(set) = self.tree.get_mut(&key) {
                set.remove(&doc.id);
                if set.is_empty() {
                    self.tree.remove(&key);
                }
            }
        }
    }

    /// Remove using a &Value directly — avoids constructing a Document.
    pub fn remove_value(&mut self, id: DocumentId, data: &Value) {
        if let Some(value) = resolve_value_field(data, &self.field) {
            let key = IndexValue::from_json(value);
            if let Some(set) = self.tree.get_mut(&key) {
                set.remove(&id);
                if set.is_empty() {
                    self.tree.remove(&key);
                }
            }
        }
    }

    // -- Query helpers -------------------------------------------------------

    pub fn find_eq(&self, value: &IndexValue) -> BTreeSet<DocumentId> {
        self.tree
            .get(value)
            .map(|ids| ids.to_btreeset())
            .unwrap_or_default()
    }

    /// Count matching docs without building a BTreeSet.
    pub fn count_eq(&self, value: &IndexValue) -> usize {
        self.tree.get(value).map_or(0, |ids| ids.len())
    }

    /// Count docs in a range without building a BTreeSet.
    pub fn count_range(&self, start: Bound<&IndexValue>, end: Bound<&IndexValue>) -> usize {
        let mut count = 0;
        for (_key, ids) in self.tree.range((start, end)) {
            count += ids.len();
        }
        count
    }

    /// Count docs matching any of the given values.
    pub fn count_in(&self, values: &[IndexValue]) -> usize {
        let mut count = 0;
        for v in values {
            if let Some(ids) = self.tree.get(v) {
                count += ids.len();
            }
        }
        count
    }

    /// Total count of all indexed docs.
    pub fn count_all(&self) -> usize {
        let mut count = 0;
        for ids in self.tree.values() {
            count += ids.len();
        }
        count
    }

    pub fn find_ne(&self, value: &IndexValue) -> BTreeSet<DocumentId> {
        let mut result = BTreeSet::new();
        for (k, ids) in &self.tree {
            if k != value {
                result.extend(ids);
            }
        }
        result
    }

    pub fn find_range(
        &self,
        start: Bound<&IndexValue>,
        end: Bound<&IndexValue>,
    ) -> BTreeSet<DocumentId> {
        let mut result = BTreeSet::new();
        for (_key, ids) in self.tree.range((start, end)) {
            result.extend(ids);
        }
        result
    }

    pub fn find_in(&self, values: &[IndexValue]) -> BTreeSet<DocumentId> {
        let mut result = BTreeSet::new();
        for v in values {
            if let Some(ids) = self.tree.get(v) {
                result.extend(ids);
            }
        }
        result
    }

    pub fn all_ids(&self) -> BTreeSet<DocumentId> {
        let mut result = BTreeSet::new();
        for ids in self.tree.values() {
            result.extend(ids);
        }
        result
    }

    // -- Lazy iteration helpers (early-termination via callback) ---------------

    /// Iterate IDs matching `value`, calling `f` per ID. Stops when `f` returns false.
    pub fn for_each_eq<F>(&self, value: &IndexValue, mut f: F)
    where
        F: FnMut(DocumentId) -> bool,
    {
        if let Some(ids) = self.tree.get(value) {
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
        for (k, ids) in &self.tree {
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
    pub fn for_each_in_range<F>(&self, start: Bound<&IndexValue>, end: Bound<&IndexValue>, mut f: F)
    where
        F: FnMut(DocumentId) -> bool,
    {
        for (_key, ids) in self.tree.range((start, end)) {
            for &id in ids {
                if !f(id) {
                    return;
                }
            }
        }
    }

    /// Iterate IDs matching any of the given values, calling `f` per ID.
    /// Stops when `f` returns false.
    pub fn for_each_in<F>(&self, values: &[IndexValue], mut f: F)
    where
        F: FnMut(DocumentId) -> bool,
    {
        for v in values {
            if let Some(ids) = self.tree.get(v) {
                for &id in ids {
                    if !f(id) {
                        return;
                    }
                }
            }
        }
    }

    /// Iterate (value, doc_ids) in ascending order.
    pub fn iter_asc(&self) -> impl Iterator<Item = (&IndexValue, &DocIdSet)> {
        self.tree.iter()
    }

    /// Iterate (value, doc_ids) in descending order.
    pub fn iter_desc(&self) -> impl Iterator<Item = (&IndexValue, &DocIdSet)> {
        self.tree.iter().rev()
    }

    /// Remove all entries from the index while keeping field/unique metadata.
    pub fn clear(&mut self) {
        self.tree.clear();
    }

    // -- Binary serialization -------------------------------------------------

    /// Serialize the entire field index to a binary writer.
    /// Format: [field_name_len:u32][field_name][unique:u8][entry_count:u32]
    ///   per entry: [IndexValue][doc_count:u32][doc_ids as u64 LE...]
    pub fn write_to<W: Write>(&self, w: &mut W) -> io::Result<()> {
        // Field name
        let name_bytes = self.field.as_bytes();
        w.write_all(&(name_bytes.len() as u32).to_le_bytes())?;
        w.write_all(name_bytes)?;
        // Unique flag
        w.write_all(&[self.unique as u8])?;
        // Entry count
        w.write_all(&(self.tree.len() as u32).to_le_bytes())?;
        for (key, ids) in &self.tree {
            key.write_to(w)?;
            w.write_all(&(ids.len() as u32).to_le_bytes())?;
            for &id in ids {
                w.write_all(&id.to_le_bytes())?;
            }
        }
        Ok(())
    }

    /// Deserialize a field index from a binary reader.
    pub fn read_from<R: Read>(r: &mut R) -> io::Result<Self> {
        // Field name
        let mut len_buf = [0u8; 4];
        r.read_exact(&mut len_buf)?;
        let name_len = u32::from_le_bytes(len_buf) as usize;
        let mut name_buf = vec![0u8; name_len];
        r.read_exact(&mut name_buf)?;
        let field = String::from_utf8(name_buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        // Unique flag
        let mut unique_buf = [0u8; 1];
        r.read_exact(&mut unique_buf)?;
        let unique = unique_buf[0] != 0;
        // Entry count
        r.read_exact(&mut len_buf)?;
        let entry_count = u32::from_le_bytes(len_buf) as usize;
        let mut tree = BTreeMap::new();
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
                let mut set = BTreeSet::new();
                for _ in 0..doc_count {
                    r.read_exact(&mut id_buf)?;
                    set.insert(u64::from_le_bytes(id_buf));
                }
                DocIdSet::Set(set)
            };
            tree.insert(key, ids);
        }
        Ok(Self {
            field,
            unique,
            tree,
        })
    }
}

// ---------------------------------------------------------------------------
// Composite (multi-field) index
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct CompositeKey(pub Vec<IndexValue>);

#[derive(Debug)]
pub struct CompositeIndex {
    pub fields: Vec<String>,
    tree: BTreeMap<CompositeKey, DocIdSet>,
}

impl CompositeIndex {
    pub fn new(fields: Vec<String>) -> Self {
        Self {
            fields,
            tree: BTreeMap::new(),
        }
    }

    pub fn name(&self) -> String {
        self.fields.join("_")
    }

    /// Approximate resident heap bytes of this composite index: per-entry
    /// B-tree node storage plus the heap owned by string key components and
    /// large doc-id sets.
    pub fn memory_bytes(&self) -> usize {
        // Per-entry B-tree overhead: key + value inline sizes + node bookkeeping.
        let per_entry = std::mem::size_of::<CompositeKey>() + std::mem::size_of::<DocIdSet>() + 24;
        let mut total = self.tree.len() * per_entry;
        for (key, ids) in &self.tree {
            total += key.0.capacity() * std::mem::size_of::<IndexValue>();
            for iv in &key.0 {
                total += iv.heap_bytes();
            }
            total += ids.heap_bytes();
        }
        total
    }

    fn extract_key_from_value(&self, data: &Value) -> CompositeKey {
        let values = self
            .fields
            .iter()
            .map(|f| {
                resolve_value_field(data, f)
                    .map(IndexValue::from_json)
                    .unwrap_or(IndexValue::Null)
            })
            .collect();
        CompositeKey(values)
    }

    fn extract_key(&self, doc: &Document) -> CompositeKey {
        let values = self
            .fields
            .iter()
            .map(|f| {
                doc.get_field(f)
                    .map(IndexValue::from_json)
                    .unwrap_or(IndexValue::Null)
            })
            .collect();
        CompositeKey(values)
    }

    /// Iterate every (key values, doc_ids) entry in ascending key order —
    /// entries sharing a first slot value are contiguous, so a single pass
    /// yields group runs. The callback returns `false` to stop early. Used by
    /// the covered-aggregation fast path.
    pub fn for_each_entry<F: FnMut(&[IndexValue], &DocIdSet) -> bool>(&self, mut f: F) {
        for (key, ids) in &self.tree {
            if !f(&key.0, ids) {
                return;
            }
        }
    }

    pub fn insert(&mut self, doc: &Document) {
        let key = self.extract_key(doc);
        self.tree.entry(key).or_default().insert(doc.id);
    }

    /// Insert using a &Value directly — avoids constructing a Document.
    pub fn insert_value(&mut self, id: DocumentId, data: &Value) {
        let key = self.extract_key_from_value(data);
        self.tree.entry(key).or_default().insert(id);
    }

    pub fn remove(&mut self, doc: &Document) {
        let key = self.extract_key(doc);
        if let Some(set) = self.tree.get_mut(&key) {
            set.remove(&doc.id);
            if set.is_empty() {
                self.tree.remove(&key);
            }
        }
    }

    /// Remove using a &Value directly — avoids constructing a Document.
    pub fn remove_value(&mut self, id: DocumentId, data: &Value) {
        let key = self.extract_key_from_value(data);
        if let Some(set) = self.tree.get_mut(&key) {
            set.remove(&id);
            if set.is_empty() {
                self.tree.remove(&key);
            }
        }
    }

    /// Remove all entries from the index.
    pub fn clear(&mut self) {
        self.tree.clear();
    }

    // -- Binary serialization -------------------------------------------------

    /// Serialize the entire composite index to a binary writer.
    /// Format: [field_count:u32][field_names...][entry_count:u32]
    ///   per entry: [key_values...][doc_count:u32][doc_ids as u64 LE...]
    pub fn write_to<W: Write>(&self, w: &mut W) -> io::Result<()> {
        // Field names
        w.write_all(&(self.fields.len() as u32).to_le_bytes())?;
        for f in &self.fields {
            let bytes = f.as_bytes();
            w.write_all(&(bytes.len() as u32).to_le_bytes())?;
            w.write_all(bytes)?;
        }
        // Entry count
        w.write_all(&(self.tree.len() as u32).to_le_bytes())?;
        for (CompositeKey(values), ids) in &self.tree {
            for v in values {
                v.write_to(w)?;
            }
            w.write_all(&(ids.len() as u32).to_le_bytes())?;
            for &id in ids {
                w.write_all(&id.to_le_bytes())?;
            }
        }
        Ok(())
    }

    /// Deserialize a composite index from a binary reader.
    pub fn read_from<R: Read>(r: &mut R) -> io::Result<Self> {
        let mut len_buf = [0u8; 4];
        // Field names
        r.read_exact(&mut len_buf)?;
        let field_count = u32::from_le_bytes(len_buf) as usize;
        let mut fields = Vec::with_capacity(field_count);
        for _ in 0..field_count {
            r.read_exact(&mut len_buf)?;
            let name_len = u32::from_le_bytes(len_buf) as usize;
            let mut name_buf = vec![0u8; name_len];
            r.read_exact(&mut name_buf)?;
            let name = String::from_utf8(name_buf)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            fields.push(name);
        }
        // Entry count
        r.read_exact(&mut len_buf)?;
        let entry_count = u32::from_le_bytes(len_buf) as usize;
        let mut tree = BTreeMap::new();
        let mut id_buf = [0u8; 8];
        for _ in 0..entry_count {
            let mut key_values = Vec::with_capacity(field_count);
            for _ in 0..field_count {
                key_values.push(IndexValue::read_from(r)?);
            }
            r.read_exact(&mut len_buf)?;
            let doc_count = u32::from_le_bytes(len_buf) as usize;
            let ids = if doc_count == 0 {
                DocIdSet::Empty
            } else if doc_count == 1 {
                r.read_exact(&mut id_buf)?;
                DocIdSet::One(u64::from_le_bytes(id_buf))
            } else {
                let mut set = BTreeSet::new();
                for _ in 0..doc_count {
                    r.read_exact(&mut id_buf)?;
                    set.insert(u64::from_le_bytes(id_buf));
                }
                DocIdSet::Set(set)
            };
            tree.insert(CompositeKey(key_values), ids);
        }
        Ok(Self { fields, tree })
    }

    // -- Query helpers -------------------------------------------------------

    pub fn find_exact(&self, key: &CompositeKey) -> BTreeSet<DocumentId> {
        self.tree
            .get(key)
            .map(|ids| ids.to_btreeset())
            .unwrap_or_default()
    }

    /// Prefix scan — e.g. for composite index [A, B, C], query on A only.
    /// Works because Vec ordering is lexicographic: keys sharing the prefix
    /// are contiguous in the BTreeMap.
    pub fn find_prefix(&self, prefix: &[IndexValue]) -> BTreeSet<DocumentId> {
        let mut result = BTreeSet::new();
        let start = CompositeKey(prefix.to_vec());

        for (key, ids) in self.tree.range(start..) {
            if key.0.len() >= prefix.len() && key.0[..prefix.len()] == *prefix {
                result.extend(ids);
            } else {
                break;
            }
        }

        result
    }

    /// Iterate prefix-matching entries in ascending order, calling `f` for each DocumentId.
    /// Entries are yielded in the natural BTreeMap order of the remaining (non-prefix) fields.
    /// Return `false` from `f` to stop early.
    pub fn for_each_prefix_asc<F>(&self, prefix: &[IndexValue], mut f: F)
    where
        F: FnMut(DocumentId) -> bool,
    {
        let start = CompositeKey(prefix.to_vec());
        for (key, ids) in self.tree.range(start..) {
            if key.0.len() < prefix.len() || key.0[..prefix.len()] != *prefix {
                break;
            }
            for &id in ids {
                if !f(id) {
                    return;
                }
            }
        }
    }

    /// Iterate prefix-matching entries in descending order, calling `f` for each DocumentId.
    /// Return `false` from `f` to stop early.
    pub fn for_each_prefix_desc<F>(&self, prefix: &[IndexValue], mut f: F)
    where
        F: FnMut(DocumentId) -> bool,
    {
        if prefix.is_empty() {
            return;
        }

        let lower = CompositeKey(prefix.to_vec());

        // Try to compute tight upper bound for O(limit) reverse iteration.
        if let Some(successor) = prefix.last().unwrap().try_successor() {
            let mut upper_vec = prefix.to_vec();
            *upper_vec.last_mut().unwrap() = successor;
            let upper = CompositeKey(upper_vec);

            for (_, ids) in self.tree.range(lower..upper).rev() {
                for &id in ids.iter().rev() {
                    if !f(id) {
                        return;
                    }
                }
            }
        } else {
            // Fallback: collect forward, iterate reverse.
            let entries: Vec<_> = self
                .tree
                .range(lower..)
                .take_while(|(key, _)| {
                    key.0.len() >= prefix.len() && key.0[..prefix.len()] == *prefix
                })
                .collect();

            for (_, ids) in entries.iter().rev() {
                for &id in ids.iter().rev() {
                    if !f(id) {
                        return;
                    }
                }
            }
        }
    }

    /// Prefix + range on the next field.
    /// Example: index [status, created_at], query status="active" AND created_at > X
    pub fn find_prefix_range(
        &self,
        prefix: &[IndexValue],
        range_start: Bound<&IndexValue>,
        range_end: Bound<&IndexValue>,
    ) -> BTreeSet<DocumentId> {
        let mut result = BTreeSet::new();
        let prefix_len = prefix.len();
        let range_idx = prefix_len;

        let scan_start = CompositeKey(prefix.to_vec());

        for (key, ids) in self.tree.range(scan_start..) {
            // Must still share the prefix
            if key.0.len() <= range_idx || key.0[..prefix_len] != *prefix {
                break;
            }

            let val = &key.0[range_idx];
            let in_range = match (&range_start, &range_end) {
                (Bound::Unbounded, Bound::Unbounded) => true,
                (Bound::Included(s), Bound::Unbounded) => val >= s,
                (Bound::Excluded(s), Bound::Unbounded) => val > s,
                (Bound::Unbounded, Bound::Included(e)) => val <= e,
                (Bound::Unbounded, Bound::Excluded(e)) => val < e,
                (Bound::Included(s), Bound::Included(e)) => val >= s && val <= e,
                (Bound::Included(s), Bound::Excluded(e)) => val >= s && val < e,
                (Bound::Excluded(s), Bound::Included(e)) => val > s && val <= e,
                (Bound::Excluded(s), Bound::Excluded(e)) => val > s && val < e,
            };

            if in_range {
                result.extend(ids);
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_doc(id: u64, data: serde_json::Value) -> Document {
        Document::new(id, data).unwrap()
    }

    #[test]
    fn field_index_eq() {
        let mut idx = FieldIndex::new("status".into());
        idx.insert(&make_doc(1, json!({"status": "active"})));
        idx.insert(&make_doc(2, json!({"status": "inactive"})));
        idx.insert(&make_doc(3, json!({"status": "active"})));

        let result = idx.find_eq(&IndexValue::String("active".into()));
        assert_eq!(result, BTreeSet::from([1, 3]));
    }

    #[test]
    fn field_index_range_dates() {
        let mut idx = FieldIndex::new("created_at".into());
        idx.insert(&make_doc(1, json!({"created_at": "2024-01-01"})));
        idx.insert(&make_doc(2, json!({"created_at": "2024-06-15"})));
        idx.insert(&make_doc(3, json!({"created_at": "2025-01-01"})));

        let start = IndexValue::from_json(&serde_json::Value::String("2024-03-01".into()));
        let end = IndexValue::from_json(&serde_json::Value::String("2024-12-31".into()));

        let result = idx.find_range(Bound::Included(&start), Bound::Included(&end));
        assert_eq!(result, BTreeSet::from([2]));
    }

    #[test]
    fn composite_index_prefix() {
        let mut idx = CompositeIndex::new(vec!["status".into(), "priority".into()]);
        idx.insert(&make_doc(1, json!({"status": "active", "priority": 1})));
        idx.insert(&make_doc(2, json!({"status": "active", "priority": 5})));
        idx.insert(&make_doc(3, json!({"status": "closed", "priority": 1})));

        let result = idx.find_prefix(&[IndexValue::String("active".into())]);
        assert_eq!(result, BTreeSet::from([1, 2]));
    }

    #[test]
    fn field_index_ne() {
        let mut idx = FieldIndex::new("status".into());
        idx.insert(&make_doc(1, json!({"status": "active"})));
        idx.insert(&make_doc(2, json!({"status": "inactive"})));
        idx.insert(&make_doc(3, json!({"status": "active"})));

        let result = idx.find_ne(&IndexValue::String("active".into()));
        assert_eq!(result, BTreeSet::from([2]));
    }

    #[test]
    fn field_index_range_numeric() {
        let mut idx = FieldIndex::new("score".into());
        idx.insert(&make_doc(1, json!({"score": 10})));
        idx.insert(&make_doc(2, json!({"score": 50})));
        idx.insert(&make_doc(3, json!({"score": 90})));

        let low = IndexValue::Integer(20);
        let high = IndexValue::Integer(80);
        let result = idx.find_range(Bound::Included(&low), Bound::Excluded(&high));
        assert_eq!(result, BTreeSet::from([2]));
    }

    #[test]
    fn field_index_in() {
        let mut idx = FieldIndex::new("tag".into());
        idx.insert(&make_doc(1, json!({"tag": "a"})));
        idx.insert(&make_doc(2, json!({"tag": "b"})));
        idx.insert(&make_doc(3, json!({"tag": "c"})));
        idx.insert(&make_doc(4, json!({"tag": "a"})));

        let result = idx.find_in(&[
            IndexValue::String("a".into()),
            IndexValue::String("c".into()),
        ]);
        assert_eq!(result, BTreeSet::from([1, 3, 4]));
    }

    #[test]
    fn field_index_remove() {
        let mut idx = FieldIndex::new("x".into());
        let doc = make_doc(1, json!({"x": 42}));
        idx.insert(&doc);
        assert_eq!(idx.find_eq(&IndexValue::Integer(42)), BTreeSet::from([1]));

        idx.remove(&doc);
        assert!(idx.find_eq(&IndexValue::Integer(42)).is_empty());
    }

    #[test]
    fn field_index_all_ids() {
        let mut idx = FieldIndex::new("x".into());
        idx.insert(&make_doc(1, json!({"x": "a"})));
        idx.insert(&make_doc(2, json!({"x": "b"})));
        idx.insert(&make_doc(3, json!({"y": "no_x"}))); // missing field, not indexed

        assert_eq!(idx.all_ids(), BTreeSet::from([1, 2]));
    }

    #[test]
    fn field_index_count_eq() {
        let mut idx = FieldIndex::new("x".into());
        idx.insert(&make_doc(1, json!({"x": 1})));
        idx.insert(&make_doc(2, json!({"x": 1})));
        idx.insert(&make_doc(3, json!({"x": 2})));

        assert_eq!(idx.count_eq(&IndexValue::Integer(1)), 2);
        assert_eq!(idx.count_eq(&IndexValue::Integer(2)), 1);
        assert_eq!(idx.count_eq(&IndexValue::Integer(99)), 0);
    }

    #[test]
    fn field_index_count_range() {
        let mut idx = FieldIndex::new("x".into());
        idx.insert(&make_doc(1, json!({"x": 10})));
        idx.insert(&make_doc(2, json!({"x": 20})));
        idx.insert(&make_doc(3, json!({"x": 30})));

        let lo = IndexValue::Integer(15);
        let hi = IndexValue::Integer(25);
        assert_eq!(
            idx.count_range(Bound::Included(&lo), Bound::Included(&hi)),
            1
        );
    }

    #[test]
    fn field_index_count_all() {
        let mut idx = FieldIndex::new("x".into());
        idx.insert(&make_doc(1, json!({"x": 1})));
        idx.insert(&make_doc(2, json!({"x": 2})));
        assert_eq!(idx.count_all(), 2);
    }

    #[test]
    fn field_index_count_in() {
        let mut idx = FieldIndex::new("x".into());
        idx.insert(&make_doc(1, json!({"x": "a"})));
        idx.insert(&make_doc(2, json!({"x": "b"})));
        idx.insert(&make_doc(3, json!({"x": "c"})));

        assert_eq!(
            idx.count_in(&[
                IndexValue::String("a".into()),
                IndexValue::String("c".into())
            ]),
            2
        );
    }

    #[test]
    fn unique_index_check() {
        let mut idx = FieldIndex::new_unique("email".into());
        assert!(idx.unique);

        idx.insert(&make_doc(1, json!({"email": "a@b.c"})));
        assert!(idx.check_unique(&IndexValue::String("a@b.c".into()), None));
        assert!(!idx.check_unique(&IndexValue::String("a@b.c".into()), Some(1))); // exclude self
        assert!(!idx.check_unique(&IndexValue::String("new@b.c".into()), None));
    }

    #[test]
    fn field_index_clear() {
        let mut idx = FieldIndex::new("x".into());
        idx.insert(&make_doc(1, json!({"x": 1})));
        assert_eq!(idx.count_all(), 1);

        idx.clear();
        assert_eq!(idx.count_all(), 0);
    }

    #[test]
    fn field_index_insert_value() {
        let mut idx = FieldIndex::new("name".into());
        idx.insert_value(1, &json!({"name": "Alice"}));
        idx.insert_value(2, &json!({"name": "Bob"}));

        assert_eq!(
            idx.find_eq(&IndexValue::String("Alice".into())),
            BTreeSet::from([1])
        );
    }

    #[test]
    fn field_index_remove_value() {
        let mut idx = FieldIndex::new("name".into());
        idx.insert_value(1, &json!({"name": "Alice"}));
        idx.remove_value(1, &json!({"name": "Alice"}));
        assert!(idx.find_eq(&IndexValue::String("Alice".into())).is_empty());
    }

    #[test]
    fn field_index_iter_asc_desc() {
        let mut idx = FieldIndex::new("x".into());
        idx.insert(&make_doc(1, json!({"x": 3})));
        idx.insert(&make_doc(2, json!({"x": 1})));
        idx.insert(&make_doc(3, json!({"x": 2})));

        let asc: Vec<_> = idx.iter_asc().map(|(v, _)| v.clone()).collect();
        assert_eq!(
            asc,
            vec![
                IndexValue::Integer(1),
                IndexValue::Integer(2),
                IndexValue::Integer(3)
            ]
        );

        let desc: Vec<_> = idx.iter_desc().map(|(v, _)| v.clone()).collect();
        assert_eq!(
            desc,
            vec![
                IndexValue::Integer(3),
                IndexValue::Integer(2),
                IndexValue::Integer(1)
            ]
        );
    }

    #[test]
    fn composite_index_exact() {
        let mut idx = CompositeIndex::new(vec!["a".into(), "b".into()]);
        idx.insert(&make_doc(1, json!({"a": "x", "b": 1})));
        idx.insert(&make_doc(2, json!({"a": "x", "b": 2})));
        idx.insert(&make_doc(3, json!({"a": "y", "b": 1})));

        let key = CompositeKey(vec![IndexValue::String("x".into()), IndexValue::Integer(1)]);
        assert_eq!(idx.find_exact(&key), BTreeSet::from([1]));
    }

    #[test]
    fn composite_index_prefix_range() {
        let mut idx = CompositeIndex::new(vec!["status".into(), "score".into()]);
        idx.insert(&make_doc(1, json!({"status": "active", "score": 10})));
        idx.insert(&make_doc(2, json!({"status": "active", "score": 50})));
        idx.insert(&make_doc(3, json!({"status": "active", "score": 90})));
        idx.insert(&make_doc(4, json!({"status": "closed", "score": 50})));

        let prefix = &[IndexValue::String("active".into())];
        let lo = IndexValue::Integer(20);
        let hi = IndexValue::Integer(80);
        let result = idx.find_prefix_range(prefix, Bound::Included(&lo), Bound::Included(&hi));
        assert_eq!(result, BTreeSet::from([2]));
    }

    #[test]
    fn composite_index_name() {
        let idx = CompositeIndex::new(vec!["a".into(), "b".into(), "c".into()]);
        assert_eq!(idx.name(), "a_b_c");
    }

    #[test]
    fn composite_index_remove() {
        let mut idx = CompositeIndex::new(vec!["a".into(), "b".into()]);
        let doc = make_doc(1, json!({"a": 1, "b": 2}));
        idx.insert(&doc);
        let key = CompositeKey(vec![IndexValue::Integer(1), IndexValue::Integer(2)]);
        assert_eq!(idx.find_exact(&key), BTreeSet::from([1]));

        idx.remove(&doc);
        assert!(idx.find_exact(&key).is_empty());
    }

    #[test]
    fn composite_index_missing_field_uses_null() {
        let mut idx = CompositeIndex::new(vec!["a".into(), "b".into()]);
        idx.insert(&make_doc(1, json!({"a": 1}))); // b is missing → Null

        let key = CompositeKey(vec![IndexValue::Integer(1), IndexValue::Null]);
        assert_eq!(idx.find_exact(&key), BTreeSet::from([1]));
    }

    #[test]
    fn composite_insert_remove_value() {
        let mut idx = CompositeIndex::new(vec!["x".into(), "y".into()]);
        idx.insert_value(1, &json!({"x": "a", "y": 1}));
        let key = CompositeKey(vec![IndexValue::String("a".into()), IndexValue::Integer(1)]);
        assert_eq!(idx.find_exact(&key), BTreeSet::from([1]));

        idx.remove_value(1, &json!({"x": "a", "y": 1}));
        assert!(idx.find_exact(&key).is_empty());
    }

    #[test]
    fn dot_notation_in_index() {
        let mut idx = FieldIndex::new("address.city".into());
        idx.insert_value(1, &json!({"address": {"city": "NYC"}}));
        idx.insert_value(2, &json!({"address": {"city": "LA"}}));

        assert_eq!(
            idx.find_eq(&IndexValue::String("NYC".into())),
            BTreeSet::from([1])
        );
    }

    #[test]
    fn field_index_binary_roundtrip() {
        let mut idx = FieldIndex::new_unique("email".into());
        idx.insert(&make_doc(1, json!({"email": "alice@test.com"})));
        idx.insert(&make_doc(2, json!({"email": "bob@test.com"})));
        idx.insert(&make_doc(3, json!({"email": "charlie@test.com"})));

        let mut buf = Vec::new();
        idx.write_to(&mut buf).unwrap();
        let decoded = FieldIndex::read_from(&mut &buf[..]).unwrap();

        assert_eq!(decoded.field, "email");
        assert!(decoded.unique);
        assert_eq!(
            decoded.find_eq(&IndexValue::String("alice@test.com".into())),
            BTreeSet::from([1])
        );
        assert_eq!(
            decoded.find_eq(&IndexValue::String("bob@test.com".into())),
            BTreeSet::from([2])
        );
        assert_eq!(decoded.count_all(), 3);
    }

    #[test]
    fn field_index_binary_roundtrip_mixed_types() {
        let mut idx = FieldIndex::new("val".into());
        idx.insert(&make_doc(1, json!({"val": null})));
        idx.insert(&make_doc(2, json!({"val": true})));
        idx.insert(&make_doc(3, json!({"val": 42})));
        idx.insert(&make_doc(4, json!({"val": 3.14})));
        idx.insert(&make_doc(5, json!({"val": "2024-01-01"})));
        idx.insert(&make_doc(6, json!({"val": "hello"})));

        let mut buf = Vec::new();
        idx.write_to(&mut buf).unwrap();
        let decoded = FieldIndex::read_from(&mut &buf[..]).unwrap();
        assert_eq!(decoded.count_all(), 6);
        assert_eq!(
            decoded.find_eq(&IndexValue::Integer(42)),
            BTreeSet::from([3])
        );
    }

    #[test]
    fn composite_index_binary_roundtrip() {
        let mut idx = CompositeIndex::new(vec!["status".into(), "priority".into()]);
        idx.insert(&make_doc(1, json!({"status": "active", "priority": 1})));
        idx.insert(&make_doc(2, json!({"status": "active", "priority": 5})));
        idx.insert(&make_doc(3, json!({"status": "closed", "priority": 1})));

        let mut buf = Vec::new();
        idx.write_to(&mut buf).unwrap();
        let decoded = CompositeIndex::read_from(&mut &buf[..]).unwrap();

        assert_eq!(decoded.fields, vec!["status", "priority"]);
        let result = decoded.find_prefix(&[IndexValue::String("active".into())]);
        assert_eq!(result, BTreeSet::from([1, 2]));
    }
}
