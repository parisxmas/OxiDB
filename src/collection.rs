use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use serde_json::Value;

use crate::document::{Document, DocumentId};
use crate::error::{Error, Result};
use crate::index::{CompositeIndex, FieldIndex};
use crate::query::{self, FindOptions, Query, SortOrder};
use crate::storage::{DocLocation, Storage};
use crate::value::IndexValue;
use crate::wal::{Wal, WalEntry};

/// Statistics returned after a compaction run.
#[derive(Debug, Clone)]
pub struct CompactStats {
    pub old_size: u64,
    pub new_size: u64,
    pub docs_kept: usize,
}

pub struct Collection {
    name: String,
    data_dir: PathBuf,
    storage: Storage,
    wal: Wal,
    primary_index: HashMap<DocumentId, DocLocation>,
    field_indexes: HashMap<String, FieldIndex>,
    composite_indexes: Vec<CompositeIndex>,
    next_id: DocumentId,
}

impl Collection {
    /// Create or open a collection backed by a data file.
    pub fn open(name: &str, data_dir: &Path) -> Result<Self> {
        let data_path = data_dir.join(format!("{}.dat", name));
        let wal_path = data_dir.join(format!("{}.wal", name));
        let storage = Storage::open(&data_path)?;
        let wal = Wal::open(&wal_path)?;

        let mut primary_index = HashMap::new();
        let mut next_id: DocumentId = 1;

        // Rebuild primary index from existing data
        for (offset, bytes) in storage.iter_active()? {
            let doc: Value = serde_json::from_slice(&bytes)?;
            if let Some(id) = doc.get("_id").and_then(|v| v.as_u64()) {
                let length = bytes.len() as u32;
                primary_index.insert(id, DocLocation { offset, length });
                if id >= next_id {
                    next_id = id + 1;
                }
            }
        }

        // WAL recovery: replay any pending entries
        wal.recover(&storage, &mut primary_index, &mut next_id)?;

        Ok(Self {
            name: name.to_string(),
            data_dir: data_dir.to_path_buf(),
            storage,
            wal,
            primary_index,
            field_indexes: HashMap::new(),
            composite_indexes: Vec::new(),
            next_id,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    // -----------------------------------------------------------------------
    // Index management
    // -----------------------------------------------------------------------

    /// Create a single-field index. Rebuilds from existing documents.
    pub fn create_index(&mut self, field: &str) -> Result<()> {
        if self.field_indexes.contains_key(field) {
            return Err(Error::IndexAlreadyExists(field.to_string()));
        }

        let mut idx = FieldIndex::new(field.to_string());

        // Backfill from existing documents
        for (&id, &loc) in &self.primary_index.clone() {
            let bytes = self.storage.read(loc)?;
            let data: Value = serde_json::from_slice(&bytes)?;
            let doc = Document::new(id, data)?;
            idx.insert(&doc);
        }

        self.field_indexes.insert(field.to_string(), idx);
        Ok(())
    }

    /// Create a unique single-field index. Rebuilds from existing documents.
    /// Returns error if existing data violates uniqueness.
    pub fn create_unique_index(&mut self, field: &str) -> Result<()> {
        if self.field_indexes.contains_key(field) {
            return Err(Error::IndexAlreadyExists(field.to_string()));
        }

        let mut idx = FieldIndex::new_unique(field.to_string());

        // Backfill and check for uniqueness violations
        for (&id, &loc) in &self.primary_index.clone() {
            let bytes = self.storage.read(loc)?;
            let data: Value = serde_json::from_slice(&bytes)?;
            let doc = Document::new(id, data)?;

            if let Some(value) = doc.get_field(field) {
                let iv = IndexValue::from_json(value);
                if idx.check_unique(&iv, None) {
                    return Err(Error::UniqueViolation {
                        field: field.to_string(),
                    });
                }
            }

            idx.insert(&doc);
        }

        self.field_indexes.insert(field.to_string(), idx);
        Ok(())
    }

    /// Create a composite (multi-field) index. Rebuilds from existing documents.
    pub fn create_composite_index(&mut self, fields: Vec<String>) -> Result<String> {
        let name = fields.join("_");
        if self.composite_indexes.iter().any(|i| i.name() == name) {
            return Err(Error::IndexAlreadyExists(name));
        }

        let mut idx = CompositeIndex::new(fields);

        // Backfill
        for (&id, &loc) in &self.primary_index.clone() {
            let bytes = self.storage.read(loc)?;
            let data: Value = serde_json::from_slice(&bytes)?;
            let doc = Document::new(id, data)?;
            idx.insert(&doc);
        }

        let idx_name = idx.name();
        self.composite_indexes.push(idx);
        Ok(idx_name)
    }

    // -----------------------------------------------------------------------
    // Unique constraint checks
    // -----------------------------------------------------------------------

    /// Check unique constraints for a document about to be inserted.
    fn check_unique_constraints(
        &self,
        data: &Value,
        exclude_id: Option<DocumentId>,
    ) -> Result<()> {
        let tmp_doc = Document::new(exclude_id.unwrap_or(0), data.clone())?;
        for idx in self.field_indexes.values() {
            if !idx.unique {
                continue;
            }
            if let Some(value) = tmp_doc.get_field(&idx.field) {
                let iv = IndexValue::from_json(value);
                if idx.check_unique(&iv, exclude_id) {
                    return Err(Error::UniqueViolation {
                        field: idx.field.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // CRUD operations
    // -----------------------------------------------------------------------

    /// Insert a document. Returns the assigned _id.
    pub fn insert(&mut self, mut data: Value) -> Result<DocumentId> {
        if !data.is_object() {
            return Err(Error::NotAnObject);
        }

        let id = self.next_id;

        // Inject _id
        data.as_object_mut()
            .unwrap()
            .insert("_id".to_string(), Value::Number(id.into()));

        // Check unique constraints BEFORE any disk writes
        self.check_unique_constraints(&data, None)?;

        self.next_id += 1;

        let bytes = serde_json::to_vec(&data)?;

        // WAL: log before mutating .dat
        self.wal.log(&WalEntry::Insert {
            doc_id: id,
            doc_bytes: bytes.clone(),
        })?;

        let loc = self.storage.append(&bytes)?;

        // WAL: checkpoint after .dat is durable
        self.wal.checkpoint()?;

        let doc = Document::new(id, data)?;
        self.primary_index.insert(id, loc);

        // Update all field indexes
        for idx in self.field_indexes.values_mut() {
            idx.insert(&doc);
        }
        // Update all composite indexes
        for idx in &mut self.composite_indexes {
            idx.insert(&doc);
        }

        Ok(id)
    }

    /// Insert multiple documents in a single atomic batch (3 fsyncs total).
    /// Either all documents are inserted or none (on constraint violation).
    pub fn insert_many(&mut self, docs: Vec<Value>) -> Result<Vec<DocumentId>> {
        if docs.is_empty() {
            return Ok(vec![]);
        }

        // Phase 1: assign IDs, serialize, and validate ALL constraints upfront
        let mut prepared = Vec::with_capacity(docs.len());
        // Track values we're about to insert for intra-batch uniqueness checks
        let mut pending_unique: HashMap<String, HashMap<IndexValue, DocumentId>> = HashMap::new();

        for mut data in docs {
            if !data.is_object() {
                return Err(Error::NotAnObject);
            }
            let id = self.next_id + prepared.len() as u64;
            data.as_object_mut()
                .unwrap()
                .insert("_id".to_string(), Value::Number(id.into()));

            // Check against existing index
            self.check_unique_constraints(&data, None)?;

            // Check intra-batch uniqueness
            let tmp_doc = Document::new(id, data.clone())?;
            for idx in self.field_indexes.values() {
                if !idx.unique {
                    continue;
                }
                if let Some(value) = tmp_doc.get_field(&idx.field) {
                    let iv = IndexValue::from_json(value);
                    let field_map = pending_unique.entry(idx.field.clone()).or_default();
                    if field_map.contains_key(&iv) {
                        return Err(Error::UniqueViolation {
                            field: idx.field.clone(),
                        });
                    }
                    field_map.insert(iv, id);
                }
            }

            let bytes = serde_json::to_vec(&data)?;
            prepared.push((id, data, bytes));
        }

        // Phase 2: WAL log all entries → single fsync
        let wal_entries: Vec<WalEntry> = prepared
            .iter()
            .map(|(id, _, bytes)| WalEntry::Insert {
                doc_id: *id,
                doc_bytes: bytes.clone(),
            })
            .collect();
        self.wal.log_batch(&wal_entries)?;

        // Phase 3: append all to .dat without per-record fsync → single fsync
        let mut ids = Vec::with_capacity(prepared.len());
        let mut locs = Vec::with_capacity(prepared.len());
        for (id, _, bytes) in &prepared {
            let loc = self.storage.append_no_sync(bytes)?;
            ids.push(*id);
            locs.push((*id, loc));
        }
        self.storage.sync()?;

        // Phase 4: checkpoint WAL → single fsync
        self.wal.checkpoint()?;

        // Phase 5: update in-memory indexes (all constraints passed, safe to commit)
        self.next_id += prepared.len() as u64;
        for ((id, data, _), (_, loc)) in prepared.into_iter().zip(locs.iter()) {
            self.primary_index.insert(id, *loc);
            let doc = Document::new(id, data)?;
            for idx in self.field_indexes.values_mut() {
                idx.insert(&doc);
            }
            for idx in &mut self.composite_indexes {
                idx.insert(&doc);
            }
        }

        Ok(ids)
    }

    /// Find documents matching a query.
    pub fn find(&self, query_json: &Value) -> Result<Vec<Value>> {
        self.find_with_options(query_json, &FindOptions::default())
    }

    /// Find documents matching a query with sort/skip/limit options.
    pub fn find_with_options(
        &self,
        query_json: &Value,
        opts: &FindOptions,
    ) -> Result<Vec<Value>> {
        let query = query::parse_query(query_json)?;
        let all_ids: BTreeSet<DocumentId> = self.primary_index.keys().copied().collect();

        // Try index-accelerated lookup
        let candidate_ids = query::execute_indexed(
            &query,
            &self.field_indexes,
            &self.composite_indexes,
            &all_ids,
        );

        let ids_to_scan = candidate_ids.as_ref().unwrap_or(&all_ids);

        let mut results = Vec::new();
        for &id in ids_to_scan {
            if let Some(&loc) = self.primary_index.get(&id) {
                let bytes = self.storage.read(loc)?;
                let data: Value = serde_json::from_slice(&bytes)?;
                let doc = Document::new(id, data.clone())?;
                if matches_with_post_filter(&query, &doc, candidate_ids.is_some()) {
                    results.push(data);
                }
            }
        }

        // Apply sort → skip → limit pipeline
        if let Some(sort_fields) = &opts.sort {
            results.sort_by(|a, b| {
                for (field, order) in sort_fields {
                    let av = a.pointer(&format!("/{}", field.replace('.', "/")));
                    let bv = b.pointer(&format!("/{}", field.replace('.', "/")));
                    let aiv = av.map(IndexValue::from_json).unwrap_or(IndexValue::Null);
                    let biv = bv.map(IndexValue::from_json).unwrap_or(IndexValue::Null);
                    let cmp = aiv.cmp(&biv);
                    let cmp = match order {
                        SortOrder::Asc => cmp,
                        SortOrder::Desc => cmp.reverse(),
                    };
                    if cmp != std::cmp::Ordering::Equal {
                        return cmp;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        if let Some(skip) = opts.skip {
            let skip = skip as usize;
            if skip >= results.len() {
                results.clear();
            } else {
                results = results.into_iter().skip(skip).collect();
            }
        }

        if let Some(limit) = opts.limit {
            results.truncate(limit as usize);
        }

        Ok(results)
    }

    /// Find a single document matching a query.
    pub fn find_one(&self, query_json: &Value) -> Result<Option<Value>> {
        let query = query::parse_query(query_json)?;
        let all_ids: BTreeSet<DocumentId> = self.primary_index.keys().copied().collect();

        let candidate_ids = query::execute_indexed(
            &query,
            &self.field_indexes,
            &self.composite_indexes,
            &all_ids,
        );

        let ids_to_scan = candidate_ids.as_ref().unwrap_or(&all_ids);

        for &id in ids_to_scan {
            if let Some(&loc) = self.primary_index.get(&id) {
                let bytes = self.storage.read(loc)?;
                let data: Value = serde_json::from_slice(&bytes)?;
                let doc = Document::new(id, data.clone())?;
                if matches_with_post_filter(&query, &doc, candidate_ids.is_some()) {
                    return Ok(Some(data));
                }
            }
        }

        Ok(None)
    }

    /// Get a document by its _id directly.
    pub fn get(&self, id: DocumentId) -> Result<Option<Value>> {
        if let Some(&loc) = self.primary_index.get(&id) {
            let bytes = self.storage.read(loc)?;
            let data: Value = serde_json::from_slice(&bytes)?;
            Ok(Some(data))
        } else {
            Ok(None)
        }
    }

    /// Update documents matching a query atomically. Returns number of updated documents.
    /// If any unique constraint is violated, no documents are modified.
    pub fn update(&mut self, query_json: &Value, update_json: &Value) -> Result<u64> {
        // Validate update document has at least one operator
        let update_obj = update_json
            .as_object()
            .ok_or_else(|| Error::InvalidQuery("update must be an object".into()))?;
        if update_obj.is_empty() {
            return Err(Error::InvalidQuery(
                "update must contain at least one operator".into(),
            ));
        }

        // Find matching doc IDs first
        let matching = self.find(query_json)?;
        if matching.is_empty() {
            return Ok(0);
        }

        // Phase 1: prepare all updates and validate constraints upfront
        struct UpdateOp {
            id: DocumentId,
            old_loc: DocLocation,
            old_doc: Document,
            new_data: Value,
            new_bytes: Vec<u8>,
        }
        let mut ops = Vec::with_capacity(matching.len());

        for doc_data in &matching {
            let id = doc_data
                .get("_id")
                .and_then(|v| v.as_u64())
                .ok_or_else(|| Error::InvalidQuery("document missing _id".into()))?;

            if let Some(&old_loc) = self.primary_index.get(&id) {
                let bytes = self.storage.read(old_loc)?;
                let mut data: Value = serde_json::from_slice(&bytes)?;
                let old_doc = Document::new(id, data.clone())?;

                // Apply ALL update operators
                crate::update::apply_update(&mut data, update_json)?;

                // Check unique constraints (exclude self)
                self.check_unique_constraints(&data, Some(id))?;

                let new_bytes = serde_json::to_vec(&data)?;
                ops.push(UpdateOp {
                    id,
                    old_loc,
                    old_doc,
                    new_data: data,
                    new_bytes,
                });
            }
        }

        if ops.is_empty() {
            return Ok(0);
        }

        // Phase 2: WAL log all updates → single fsync
        let wal_entries: Vec<WalEntry> = ops
            .iter()
            .map(|op| WalEntry::Update {
                doc_id: op.id,
                doc_bytes: op.new_bytes.clone(),
            })
            .collect();
        self.wal.log_batch(&wal_entries)?;

        // Phase 3: apply all mutations to .dat → single fsync
        let mut new_locs = Vec::with_capacity(ops.len());
        for op in &ops {
            let new_loc = self.storage.append_no_sync(&op.new_bytes)?;
            self.storage.mark_deleted_no_sync(op.old_loc)?;
            new_locs.push(new_loc);
        }
        self.storage.sync()?;

        // Phase 4: checkpoint WAL → single fsync
        self.wal.checkpoint()?;

        // Phase 5: update in-memory state
        let count = ops.len() as u64;
        for (op, new_loc) in ops.into_iter().zip(new_locs) {
            self.primary_index.insert(op.id, new_loc);
            let new_doc = Document::new(op.id, op.new_data)?;
            for idx in self.field_indexes.values_mut() {
                idx.remove(&op.old_doc);
                idx.insert(&new_doc);
            }
            for idx in &mut self.composite_indexes {
                idx.remove(&op.old_doc);
                idx.insert(&new_doc);
            }
        }

        Ok(count)
    }

    /// Delete documents matching a query atomically. Returns number deleted.
    pub fn delete(&mut self, query_json: &Value) -> Result<u64> {
        let matching = self.find(query_json)?;
        if matching.is_empty() {
            return Ok(0);
        }

        // Phase 1: collect all deletions
        struct DeleteOp {
            id: DocumentId,
            loc: DocLocation,
            doc: Document,
        }
        let mut ops = Vec::with_capacity(matching.len());

        for doc_data in &matching {
            let id = doc_data
                .get("_id")
                .and_then(|v| v.as_u64())
                .ok_or_else(|| Error::InvalidQuery("document missing _id".into()))?;

            if let Some(&loc) = self.primary_index.get(&id) {
                let doc = Document::new(id, doc_data.clone())?;
                ops.push(DeleteOp { id, loc, doc });
            }
        }

        if ops.is_empty() {
            return Ok(0);
        }

        // Phase 2: WAL log all deletes → single fsync
        let wal_entries: Vec<WalEntry> = ops
            .iter()
            .map(|op| WalEntry::Delete { doc_id: op.id })
            .collect();
        self.wal.log_batch(&wal_entries)?;

        // Phase 3: mark all deleted in .dat → single fsync
        for op in &ops {
            self.storage.mark_deleted_no_sync(op.loc)?;
        }
        self.storage.sync()?;

        // Phase 4: checkpoint WAL → single fsync
        self.wal.checkpoint()?;

        // Phase 5: update in-memory state
        let count = ops.len() as u64;
        for op in ops {
            self.primary_index.remove(&op.id);
            for idx in self.field_indexes.values_mut() {
                idx.remove(&op.doc);
            }
            for idx in &mut self.composite_indexes {
                idx.remove(&op.doc);
            }
        }

        Ok(count)
    }

    /// Returns the number of documents in the collection.
    pub fn count(&self) -> usize {
        self.primary_index.len()
    }

    /// Compact the data file by rewriting only active records.
    /// Reclaims space from deleted documents and rebuilds all indexes.
    pub fn compact(&mut self) -> Result<CompactStats> {
        // Ensure WAL is clean
        self.wal.checkpoint()?;

        let old_size = self.storage.file_size();

        // Create temp storage
        let tmp_path = self.data_dir.join(format!("{}.dat.tmp", self.name));
        let new_storage = Storage::open(&tmp_path)?;

        // Copy active records to new file
        let active_records = self.storage.iter_active()?;
        let mut new_primary_index = HashMap::new();
        let mut next_id: DocumentId = 1;

        for (_old_offset, bytes) in &active_records {
            let doc: Value = serde_json::from_slice(bytes)?;
            let id = doc.get("_id").and_then(|v| v.as_u64()).ok_or_else(|| {
                Error::InvalidQuery("document missing _id during compaction".into())
            })?;

            let loc = new_storage.append_no_sync(bytes)?;
            new_primary_index.insert(id, loc);
            if id >= next_id {
                next_id = id + 1;
            }
        }
        new_storage.sync()?;

        let docs_kept = new_primary_index.len();
        let new_size = new_storage.file_size();

        // Atomic swap: rename tmp → original
        let dat_path = self.data_dir.join(format!("{}.dat", self.name));
        std::fs::rename(&tmp_path, &dat_path)?;

        // Replace storage with new instance pointing to the renamed file
        self.storage = Storage::open(&dat_path)?;
        self.primary_index = new_primary_index;
        self.next_id = next_id;

        // Rebuild all indexes
        for idx in self.field_indexes.values_mut() {
            idx.clear();
        }
        for idx in &mut self.composite_indexes {
            idx.clear();
        }
        for (&id, &loc) in &self.primary_index {
            let bytes = self.storage.read(loc)?;
            let data: Value = serde_json::from_slice(&bytes)?;
            let doc = Document::new(id, data)?;
            for idx in self.field_indexes.values_mut() {
                idx.insert(&doc);
            }
            for idx in &mut self.composite_indexes {
                idx.insert(&doc);
            }
        }

        Ok(CompactStats {
            old_size,
            new_size,
            docs_kept,
        })
    }
}

/// If we used an index, we still need to post-filter for conditions
/// the index didn't fully cover. If no index was used, always filter.
fn matches_with_post_filter(query: &Query, doc: &Document, _used_index: bool) -> bool {
    // Always apply the full query as a post-filter for correctness.
    // Index results are candidates; the post-filter ensures precision.
    query::matches_doc(query, doc)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::tempdir;

    fn temp_collection(name: &str) -> Collection {
        let dir = tempdir().unwrap();
        Collection::open(name, dir.path()).unwrap()
    }

    #[test]
    fn insert_and_get() {
        let mut col = temp_collection("test");
        let id = col.insert(json!({"name": "Alice", "age": 30})).unwrap();
        let doc = col.get(id).unwrap().unwrap();
        assert_eq!(doc["name"], "Alice");
        assert_eq!(doc["_id"], id);
    }

    #[test]
    fn find_with_index() {
        let mut col = temp_collection("test");
        col.create_index("status").unwrap();
        col.insert(json!({"status": "active", "name": "Alice"}))
            .unwrap();
        col.insert(json!({"status": "inactive", "name": "Bob"}))
            .unwrap();
        col.insert(json!({"status": "active", "name": "Charlie"}))
            .unwrap();

        let results = col.find(&json!({"status": "active"})).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn date_range_query() {
        let mut col = temp_collection("test");
        col.create_index("created_at").unwrap();

        col.insert(json!({"created_at": "2024-01-15T10:00:00Z", "name": "old"}))
            .unwrap();
        col.insert(json!({"created_at": "2024-06-15T10:00:00Z", "name": "mid"}))
            .unwrap();
        col.insert(json!({"created_at": "2025-01-15T10:00:00Z", "name": "new"}))
            .unwrap();

        let results = col
            .find(&json!({
                "created_at": {"$gte": "2024-03-01", "$lt": "2025-01-01"}
            }))
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["name"], "mid");
    }

    #[test]
    fn update_doc() {
        let mut col = temp_collection("test");
        let id = col.insert(json!({"name": "Alice", "age": 30})).unwrap();

        let count = col
            .update(&json!({"name": "Alice"}), &json!({"$set": {"age": 31}}))
            .unwrap();
        assert_eq!(count, 1);

        let doc = col.get(id).unwrap().unwrap();
        assert_eq!(doc["age"], 31);
    }

    #[test]
    fn delete_doc() {
        let mut col = temp_collection("test");
        col.insert(json!({"name": "Alice"})).unwrap();
        col.insert(json!({"name": "Bob"})).unwrap();

        let count = col.delete(&json!({"name": "Alice"})).unwrap();
        assert_eq!(count, 1);
        assert_eq!(col.count(), 1);
    }

    #[test]
    fn unique_index_enforced() {
        let mut col = temp_collection("test");
        col.create_unique_index("email").unwrap();
        col.insert(json!({"email": "alice@test.com", "name": "Alice"}))
            .unwrap();

        // Duplicate should fail
        let result = col.insert(json!({"email": "alice@test.com", "name": "Bob"}));
        assert!(result.is_err());
        assert_eq!(col.count(), 1); // No partial write
    }

    #[test]
    fn unique_index_allows_different_values() {
        let mut col = temp_collection("test");
        col.create_unique_index("email").unwrap();
        col.insert(json!({"email": "alice@test.com"})).unwrap();
        col.insert(json!({"email": "bob@test.com"})).unwrap();
        assert_eq!(col.count(), 2);
    }

    #[test]
    fn unique_index_update_same_doc_ok() {
        let mut col = temp_collection("test");
        col.create_unique_index("email").unwrap();
        col.insert(json!({"email": "alice@test.com", "name": "Alice"}))
            .unwrap();

        // Updating other fields on same doc should work (email unchanged)
        let count = col
            .update(
                &json!({"email": "alice@test.com"}),
                &json!({"$set": {"name": "Alicia"}}),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn unique_index_update_conflict() {
        let mut col = temp_collection("test");
        col.create_unique_index("email").unwrap();
        col.insert(json!({"email": "alice@test.com", "name": "Alice"}))
            .unwrap();
        col.insert(json!({"email": "bob@test.com", "name": "Bob"}))
            .unwrap();

        // Trying to set Bob's email to Alice's should fail
        let result = col.update(
            &json!({"name": "Bob"}),
            &json!({"$set": {"email": "alice@test.com"}}),
        );
        assert!(result.is_err());

        // Bob's email should be unchanged
        let bob = col.find_one(&json!({"name": "Bob"})).unwrap().unwrap();
        assert_eq!(bob["email"], "bob@test.com");
    }

    #[test]
    fn insert_many_unique_violation_rolls_back() {
        let mut col = temp_collection("test");
        col.create_unique_index("email").unwrap();
        col.insert(json!({"email": "alice@test.com"})).unwrap();

        // Batch with one duplicate — entire batch should fail
        let result = col.insert_many(vec![
            json!({"email": "charlie@test.com"}),
            json!({"email": "alice@test.com"}), // conflict
            json!({"email": "dave@test.com"}),
        ]);
        assert!(result.is_err());
        assert_eq!(col.count(), 1); // None from batch were inserted
    }

    #[test]
    fn insert_many_intra_batch_uniqueness() {
        let mut col = temp_collection("test");
        col.create_unique_index("email").unwrap();

        // Two docs in same batch with same email
        let result = col.insert_many(vec![
            json!({"email": "same@test.com"}),
            json!({"email": "same@test.com"}),
        ]);
        assert!(result.is_err());
        assert_eq!(col.count(), 0);
    }

    #[test]
    fn atomic_multi_doc_update() {
        let mut col = temp_collection("test");
        col.insert(json!({"status": "draft", "title": "A"}))
            .unwrap();
        col.insert(json!({"status": "draft", "title": "B"}))
            .unwrap();

        let count = col
            .update(
                &json!({"status": "draft"}),
                &json!({"$set": {"status": "published"}}),
            )
            .unwrap();
        assert_eq!(count, 2);

        let published = col.find(&json!({"status": "published"})).unwrap();
        assert_eq!(published.len(), 2);
        let drafts = col.find(&json!({"status": "draft"})).unwrap();
        assert_eq!(drafts.len(), 0);
    }

    #[test]
    fn atomic_multi_doc_delete() {
        let mut col = temp_collection("test");
        col.insert(json!({"status": "old", "title": "A"}))
            .unwrap();
        col.insert(json!({"status": "old", "title": "B"}))
            .unwrap();
        col.insert(json!({"status": "new", "title": "C"}))
            .unwrap();

        let count = col.delete(&json!({"status": "old"})).unwrap();
        assert_eq!(count, 2);
        assert_eq!(col.count(), 1);
    }

    // -----------------------------------------------------------------------
    // Sort / Skip / Limit tests
    // -----------------------------------------------------------------------

    #[test]
    fn sort_ascending() {
        let mut col = temp_collection("test");
        col.insert(json!({"name": "Charlie", "age": 35})).unwrap();
        col.insert(json!({"name": "Alice", "age": 25})).unwrap();
        col.insert(json!({"name": "Bob", "age": 30})).unwrap();

        let opts = FindOptions {
            sort: Some(vec![("age".to_string(), SortOrder::Asc)]),
            skip: None,
            limit: None,
        };
        let results = col.find_with_options(&json!({}), &opts).unwrap();
        assert_eq!(results[0]["name"], "Alice");
        assert_eq!(results[1]["name"], "Bob");
        assert_eq!(results[2]["name"], "Charlie");
    }

    #[test]
    fn sort_descending() {
        let mut col = temp_collection("test");
        col.insert(json!({"name": "Charlie", "age": 35})).unwrap();
        col.insert(json!({"name": "Alice", "age": 25})).unwrap();
        col.insert(json!({"name": "Bob", "age": 30})).unwrap();

        let opts = FindOptions {
            sort: Some(vec![("age".to_string(), SortOrder::Desc)]),
            skip: None,
            limit: None,
        };
        let results = col.find_with_options(&json!({}), &opts).unwrap();
        assert_eq!(results[0]["name"], "Charlie");
        assert_eq!(results[1]["name"], "Bob");
        assert_eq!(results[2]["name"], "Alice");
    }

    #[test]
    fn sort_multi_field() {
        let mut col = temp_collection("test");
        col.insert(json!({"dept": "eng", "age": 30, "name": "Bob"})).unwrap();
        col.insert(json!({"dept": "eng", "age": 25, "name": "Alice"})).unwrap();
        col.insert(json!({"dept": "sales", "age": 28, "name": "Charlie"})).unwrap();
        col.insert(json!({"dept": "eng", "age": 35, "name": "Dave"})).unwrap();

        let opts = FindOptions {
            sort: Some(vec![
                ("dept".to_string(), SortOrder::Asc),
                ("age".to_string(), SortOrder::Asc),
            ]),
            skip: None,
            limit: None,
        };
        let results = col.find_with_options(&json!({}), &opts).unwrap();
        // eng group sorted by age: Alice(25), Bob(30), Dave(35)
        // then sales: Charlie(28)
        assert_eq!(results[0]["name"], "Alice");
        assert_eq!(results[1]["name"], "Bob");
        assert_eq!(results[2]["name"], "Dave");
        assert_eq!(results[3]["name"], "Charlie");
    }

    #[test]
    fn skip_and_limit() {
        let mut col = temp_collection("test");
        for i in 0..10 {
            col.insert(json!({"n": i})).unwrap();
        }

        let opts = FindOptions {
            sort: Some(vec![("n".to_string(), SortOrder::Asc)]),
            skip: Some(3),
            limit: Some(4),
        };
        let results = col.find_with_options(&json!({}), &opts).unwrap();
        assert_eq!(results.len(), 4);
        assert_eq!(results[0]["n"], 3);
        assert_eq!(results[1]["n"], 4);
        assert_eq!(results[2]["n"], 5);
        assert_eq!(results[3]["n"], 6);
    }

    #[test]
    fn limit_only() {
        let mut col = temp_collection("test");
        for i in 0..10 {
            col.insert(json!({"n": i})).unwrap();
        }

        let opts = FindOptions {
            sort: Some(vec![("n".to_string(), SortOrder::Asc)]),
            skip: None,
            limit: Some(3),
        };
        let results = col.find_with_options(&json!({}), &opts).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0]["n"], 0);
        assert_eq!(results[2]["n"], 2);
    }

    #[test]
    fn skip_only() {
        let mut col = temp_collection("test");
        for i in 0..5 {
            col.insert(json!({"n": i})).unwrap();
        }

        let opts = FindOptions {
            sort: Some(vec![("n".to_string(), SortOrder::Asc)]),
            skip: Some(3),
            limit: None,
        };
        let results = col.find_with_options(&json!({}), &opts).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0]["n"], 3);
        assert_eq!(results[1]["n"], 4);
    }

    // -----------------------------------------------------------------------
    // Compaction tests
    // -----------------------------------------------------------------------

    #[test]
    fn compact_reclaims_space() {
        let dir = tempdir().unwrap();
        let mut col = Collection::open("compact_test", dir.path()).unwrap();

        // Insert 10 docs
        for i in 0..10 {
            col.insert(json!({"n": i, "payload": "x".repeat(100)})).unwrap();
        }

        let size_before = col.storage.file_size();

        // Delete 7 of them
        col.delete(&json!({"n": {"$lt": 7}})).unwrap();
        assert_eq!(col.count(), 3);

        // File size is unchanged after delete (soft delete)
        let size_after_delete = col.storage.file_size();
        assert!(size_after_delete >= size_before);

        // Compact
        let stats = col.compact().unwrap();
        assert_eq!(stats.docs_kept, 3);
        assert!(stats.new_size < stats.old_size);

        // Verify remaining docs are accessible
        let results = col.find(&json!({})).unwrap();
        assert_eq!(results.len(), 3);
        for doc in &results {
            let n = doc["n"].as_i64().unwrap();
            assert!(n >= 7 && n < 10);
        }
    }
}
