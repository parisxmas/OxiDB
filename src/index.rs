use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

use serde_json::Value;

use crate::document::{Document, DocumentId};
use crate::value::IndexValue;

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
    tree: BTreeMap<IndexValue, BTreeSet<DocumentId>>,
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
                Some(eid) => ids.iter().any(|id| *id != eid),
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
        self.tree.get(value).cloned().unwrap_or_default()
    }

    /// Count matching docs without building a BTreeSet.
    pub fn count_eq(&self, value: &IndexValue) -> usize {
        self.tree.get(value).map_or(0, |ids| ids.len())
    }

    /// Count docs in a range without building a BTreeSet.
    pub fn count_range(
        &self,
        start: Bound<&IndexValue>,
        end: Bound<&IndexValue>,
    ) -> usize {
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

    /// Iterate (value, doc_ids) in ascending order.
    pub fn iter_asc(&self) -> impl Iterator<Item = (&IndexValue, &BTreeSet<DocumentId>)> {
        self.tree.iter()
    }

    /// Iterate (value, doc_ids) in descending order.
    pub fn iter_desc(&self) -> impl Iterator<Item = (&IndexValue, &BTreeSet<DocumentId>)> {
        self.tree.iter().rev()
    }

    /// Remove all entries from the index while keeping field/unique metadata.
    pub fn clear(&mut self) {
        self.tree.clear();
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
    tree: BTreeMap<CompositeKey, BTreeSet<DocumentId>>,
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

    // -- Query helpers -------------------------------------------------------

    pub fn find_exact(&self, key: &CompositeKey) -> BTreeSet<DocumentId> {
        self.tree.get(key).cloned().unwrap_or_default()
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
}
