use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde_json::Value;

use crate::crypto::EncryptionKey;
use crate::document::DocumentId;
use crate::error::{Error, Result};
use crate::fts::CollectionTextIndex;
use crate::index::{CompositeIndex, FieldIndex};
use crate::query::{self, FindOptions, Query, SortOrder};
use crate::storage::{DocLocation, Storage};
use crate::value::IndexValue;
use crate::wal::{Wal, WalEntry};

/// Resolve a field path (with dot notation) directly on a &Value.
fn resolve_field_in_value<'a>(data: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = data;
    for part in path.split('.') {
        current = current.as_object()?.get(part)?;
    }
    Some(current)
}

/// Metadata about an index on a collection.
#[derive(Debug, Clone, serde::Serialize)]
pub struct IndexInfo {
    pub name: String,
    pub index_type: String,
    pub fields: Vec<String>,
    pub unique: bool,
}

/// Statistics returned after a compaction run.
#[derive(Debug, Clone)]
pub struct CompactStats {
    pub old_size: u64,
    pub new_size: u64,
    pub docs_kept: usize,
}

/// A prepared mutation from transactional prepare_tx_* methods.
pub struct PreparedMutation {
    pub wal_entry: WalEntry,
    pub doc_id: DocumentId,
    pub new_bytes: Vec<u8>,
    pub old_loc: Option<DocLocation>,
    pub old_data: Option<Value>,
    pub new_data: Value,
    pub is_delete: bool,
}

pub struct Collection {
    name: String,
    data_dir: PathBuf,
    storage: Storage,
    wal: Wal,
    primary_index: HashMap<DocumentId, DocLocation>,
    field_indexes: HashMap<String, FieldIndex>,
    composite_indexes: Vec<CompositeIndex>,
    text_index: Option<CollectionTextIndex>,
    version_index: HashMap<DocumentId, u64>,
    next_id: DocumentId,
    /// In-memory document cache: doc_id → parsed JSON Value.
    /// Eliminates JSON deserialization on read paths (refcount bump only).
    doc_cache: HashMap<DocumentId, Arc<Value>>,
    encryption: Option<Arc<EncryptionKey>>,
}

impl Collection {
    /// Create or open a collection backed by a data file.
    pub fn open(name: &str, data_dir: &Path) -> Result<Self> {
        Self::open_with_options(name, data_dir, &HashSet::new(), None)
    }

    /// Create or open a collection, filtering WAL recovery by committed tx_ids.
    pub fn open_with_committed_txs(
        name: &str,
        data_dir: &Path,
        committed_tx_ids: &HashSet<u64>,
    ) -> Result<Self> {
        Self::open_with_options(name, data_dir, committed_tx_ids, None)
    }

    /// Create or open a collection with optional encryption and tx recovery.
    pub fn open_with_options(
        name: &str,
        data_dir: &Path,
        committed_tx_ids: &HashSet<u64>,
        encryption: Option<Arc<EncryptionKey>>,
    ) -> Result<Self> {
        let data_path = data_dir.join(format!("{}.dat", name));
        let wal_path = data_dir.join(format!("{}.wal", name));
        let storage = Storage::open_with_encryption(&data_path, encryption.clone())?;
        let wal = Wal::open_with_encryption(&wal_path, encryption.clone())?;

        let mut primary_index = HashMap::new();
        let mut version_index = HashMap::new();
        let mut doc_cache = HashMap::new();
        let mut next_id: DocumentId = 1;

        // Rebuild primary index, version index, and doc cache from existing data
        for (offset, bytes) in storage.iter_active()? {
            let doc: Value = crate::codec::decode_doc(&bytes)?;
            if let Some(id) = doc.get("_id").and_then(|v| v.as_u64()) {
                let length = bytes.len() as u32;
                primary_index.insert(id, DocLocation { offset, length });
                let ver = doc.get("_version").and_then(|v| v.as_u64()).unwrap_or(0);
                version_index.insert(id, ver);
                doc_cache.insert(id, Arc::new(doc));
                if id >= next_id {
                    next_id = id + 1;
                }
            }
        }

        // WAL recovery: replay any pending entries
        wal.recover(
            &storage,
            &mut primary_index,
            &mut next_id,
            committed_tx_ids,
            &mut version_index,
        )?;

        Ok(Self {
            name: name.to_string(),
            data_dir: data_dir.to_path_buf(),
            storage,
            wal,
            primary_index,
            field_indexes: HashMap::new(),
            composite_indexes: Vec::new(),
            text_index: None,
            version_index,
            next_id,
            doc_cache,
            encryption,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Read document value by id, using cache if available, falling back to disk+parse on miss.
    fn read_doc_value(&self, id: DocumentId, loc: DocLocation) -> Result<Arc<Value>> {
        if let Some(val) = self.doc_cache.get(&id) {
            return Ok(Arc::clone(val));
        }
        let bytes = self.storage.read(loc)?;
        let data: Value = crate::codec::decode_doc(&bytes)?;
        Ok(Arc::new(data))
    }

    /// Insert a parsed Value into the cache (no capacity limit — all docs are cached).
    fn cache_put(&mut self, id: DocumentId, val: Arc<Value>) {
        self.doc_cache.insert(id, val);
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

        // Backfill from cache — no disk reads, no JSON parsing, no Value clones
        for (&id, data) in &self.doc_cache {
            idx.insert_value(id, data);
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

        // Backfill from cache — no disk reads, no JSON parsing, no Value clones
        for (&id, data) in &self.doc_cache {
            if let Some(value) = resolve_field_in_value(data, field) {
                let iv = IndexValue::from_json(value);
                if idx.check_unique(&iv, None) {
                    return Err(Error::UniqueViolation {
                        field: field.to_string(),
                    });
                }
            }

            idx.insert_value(id, data);
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

        // Backfill from cache — no disk reads, no JSON parsing, no Value clones
        for (&id, data) in &self.doc_cache {
            idx.insert_value(id, data);
        }

        let idx_name = idx.name();
        self.composite_indexes.push(idx);
        Ok(idx_name)
    }

    /// Create a full-text search index on the specified fields.
    /// Rebuilds from existing documents in cache.
    pub fn create_text_index(&mut self, fields: Vec<String>) -> Result<()> {
        if self.text_index.is_some() {
            return Err(Error::IndexAlreadyExists("_text".to_string()));
        }

        let mut idx = CollectionTextIndex::new(fields);

        // Backfill from cache
        for (&id, data) in &self.doc_cache {
            idx.index_doc(id, data);
        }

        self.text_index = Some(idx);
        Ok(())
    }

    /// List all indexes on this collection.
    pub fn list_indexes(&self) -> Vec<IndexInfo> {
        let mut indexes = Vec::new();
        for idx in self.field_indexes.values() {
            indexes.push(IndexInfo {
                name: idx.field.clone(),
                index_type: if idx.unique { "unique".to_string() } else { "field".to_string() },
                fields: vec![idx.field.clone()],
                unique: idx.unique,
            });
        }
        for idx in &self.composite_indexes {
            indexes.push(IndexInfo {
                name: idx.name(),
                index_type: "composite".to_string(),
                fields: idx.fields.clone(),
                unique: false,
            });
        }
        if let Some(ref text_idx) = self.text_index {
            indexes.push(IndexInfo {
                name: "_text".to_string(),
                index_type: "text".to_string(),
                fields: text_idx.fields().to_vec(),
                unique: false,
            });
        }
        indexes
    }

    /// Drop an index by name.
    pub fn drop_index(&mut self, name: &str) -> Result<()> {
        if self.field_indexes.remove(name).is_some() {
            return Ok(());
        }
        if let Some(pos) = self.composite_indexes.iter().position(|i| i.name() == name) {
            self.composite_indexes.remove(pos);
            return Ok(());
        }
        if name == "_text" && self.text_index.is_some() {
            self.text_index = None;
            return Ok(());
        }
        Err(Error::IndexNotFound(name.to_string()))
    }

    /// Full-text search on collection documents. Returns matching documents with `_score` field.
    pub fn text_search(&self, query: &str, limit: usize) -> Result<Vec<Value>> {
        let idx = self.text_index.as_ref().ok_or_else(|| {
            Error::InvalidQuery("no text index on this collection; create one with create_text_index".into())
        })?;

        let search_results = idx.search(query, limit);
        let mut docs = Vec::with_capacity(search_results.len());
        for result in search_results {
            if let Some(data) = self.doc_cache.get(&result.doc_id) {
                let mut doc = (**data).clone();
                if let Some(obj) = doc.as_object_mut() {
                    obj.insert("_score".to_string(), serde_json::json!(result.score));
                }
                docs.push(doc);
            }
        }
        Ok(docs)
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
        for idx in self.field_indexes.values() {
            if !idx.unique {
                continue;
            }
            if let Some(value) = resolve_field_in_value(data, &idx.field) {
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

        // Inject _id and _version
        let obj = data.as_object_mut().unwrap();
        obj.insert("_id".to_string(), Value::Number(id.into()));
        obj.insert("_version".to_string(), Value::Number(1.into()));

        // Check unique constraints BEFORE any disk writes
        self.check_unique_constraints(&data, None)?;

        self.next_id += 1;

        let bytes = crate::codec::encode_doc(&data)?;

        // WAL: log before mutating .dat (no fsync — storage.append will fsync)
        self.wal.log_no_sync(&WalEntry::insert(id, bytes.clone()))?;

        let loc = self.storage.append(&bytes)?;

        // WAL: lazy checkpoint (no fsync — stale entries replay idempotently)
        self.wal.checkpoint_no_sync()?;

        let cached = Arc::new(data);
        self.primary_index.insert(id, loc);
        self.version_index.insert(id, 1);
        self.cache_put(id, Arc::clone(&cached));

        // Update all field indexes (value-based, no Document clone)
        for idx in self.field_indexes.values_mut() {
            idx.insert_value(id, &cached);
        }
        for idx in &mut self.composite_indexes {
            idx.insert_value(id, &cached);
        }
        if let Some(ref mut text_idx) = self.text_index {
            text_idx.index_doc(id, &cached);
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
            let obj = data.as_object_mut().unwrap();
            obj.insert("_id".to_string(), Value::Number(id.into()));
            obj.insert("_version".to_string(), Value::Number(1.into()));

            // Check against existing index
            self.check_unique_constraints(&data, None)?;

            // Check intra-batch uniqueness (no Document clone needed)
            for idx in self.field_indexes.values() {
                if !idx.unique {
                    continue;
                }
                if let Some(value) = resolve_field_in_value(&data, &idx.field) {
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

            let bytes = crate::codec::encode_doc(&data)?;
            prepared.push((id, data, bytes));
        }

        // Phase 2: WAL log all entries (no fsync — storage fsync provides durability)
        let wal_entries: Vec<WalEntry> = prepared
            .iter()
            .map(|(id, _, bytes)| WalEntry::insert(*id, bytes.clone()))
            .collect();
        self.wal.log_batch_no_sync(&wal_entries)?;

        // Phase 3: append all to .dat → single fsync (the only fsync in this method)
        let mut ids = Vec::with_capacity(prepared.len());
        let mut locs = Vec::with_capacity(prepared.len());
        for (id, _, bytes) in &prepared {
            let loc = self.storage.append_no_sync(bytes)?;
            ids.push(*id);
            locs.push((*id, loc));
        }
        self.storage.sync()?;

        // Phase 4: lazy WAL checkpoint (no fsync)
        self.wal.checkpoint_no_sync()?;

        // Phase 5: update in-memory indexes (all constraints passed, safe to commit)
        self.next_id += prepared.len() as u64;
        for ((id, data, _bytes), (_, loc)) in prepared.into_iter().zip(locs.iter()) {
            self.primary_index.insert(id, *loc);
            self.version_index.insert(id, 1);
            let cached = Arc::new(data);
            self.cache_put(id, Arc::clone(&cached));
            for idx in self.field_indexes.values_mut() {
                idx.insert_value(id, &cached);
            }
            for idx in &mut self.composite_indexes {
                idx.insert_value(id, &cached);
            }
            if let Some(ref mut text_idx) = self.text_index {
                text_idx.index_doc(id, &cached);
            }
        }

        Ok(ids)
    }

    /// Find documents matching a query.
    pub fn find(&self, query_json: &Value) -> Result<Vec<Value>> {
        self.find_with_options(query_json, &FindOptions::default())
    }

    /// Find documents returning Arc references — avoids Value::clone.
    /// Used by the aggregation pipeline which only needs to read fields.
    pub fn find_arcs(&self, query_json: &Value) -> Result<Vec<Arc<Value>>> {
        self.find_with_options_arcs(query_json, &FindOptions::default())
    }

    /// Find documents matching a query with sort/skip/limit options.
    pub fn find_with_options(
        &self,
        query_json: &Value,
        opts: &FindOptions,
    ) -> Result<Vec<Value>> {
        let arcs = self.find_with_options_arcs(query_json, opts)?;
        Ok(arcs.into_iter().map(|a| Arc::try_unwrap(a).unwrap_or_else(|a| (*a).clone())).collect())
    }

    /// Find documents matching a query with sort/skip/limit options,
    /// returning Arc references. Avoids Value::clone — results are
    /// zero-copy references into the cache.
    pub fn find_with_options_arcs(
        &self,
        query_json: &Value,
        opts: &FindOptions,
    ) -> Result<Vec<Arc<Value>>> {
        let query = query::parse_query(query_json)?;

        // Fast path: Query::All with no sort — iterate cache directly.
        if matches!(query, Query::All) && opts.sort.is_none() {
            let skip = opts.skip.unwrap_or(0) as usize;
            let limit = opts.limit.map(|l| l as usize).unwrap_or(usize::MAX);
            let mut results = Vec::new();
            let mut skipped = 0;
            for (_id, data) in &self.doc_cache {
                if skipped < skip {
                    skipped += 1;
                    continue;
                }
                if results.len() >= limit {
                    break;
                }
                results.push(Arc::clone(data));
            }
            return Ok(results);
        }

        // Fast path: index-backed sort with early termination.
        if let Some(sort_fields) = &opts.sort {
            if sort_fields.len() == 1 {
                let (sort_field, sort_order) = &sort_fields[0];
                if let Some(field_idx) = self.field_indexes.get(sort_field) {
                    let need = opts.skip.unwrap_or(0) as usize + opts.limit.unwrap_or(u64::MAX) as usize;
                    let mut results = Vec::new();

                    match sort_order {
                        SortOrder::Asc => {
                            'outer_asc: for (_value, doc_ids) in field_idx.iter_asc() {
                                for &id in doc_ids {
                                    if let Some(data) = self.doc_cache.get(&id) {
                                        if query::matches_value(&query, data) {
                                            results.push(Arc::clone(data));
                                            if results.len() >= need {
                                                break 'outer_asc;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        SortOrder::Desc => {
                            'outer_desc: for (_value, doc_ids) in field_idx.iter_desc() {
                                for &id in doc_ids.iter().rev() {
                                    if let Some(data) = self.doc_cache.get(&id) {
                                        if query::matches_value(&query, data) {
                                            results.push(Arc::clone(data));
                                            if results.len() >= need {
                                                break 'outer_desc;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Apply skip
                    if let Some(skip) = opts.skip {
                        let skip = skip as usize;
                        if skip >= results.len() {
                            results.clear();
                        } else {
                            results = results.into_iter().skip(skip).collect();
                        }
                    }

                    // Apply limit
                    if let Some(limit) = opts.limit {
                        results.truncate(limit as usize);
                    }

                    return Ok(results);
                }
            }
        }

        // Standard path: try index-accelerated lookup
        let candidate_ids = query::execute_indexed(
            &query,
            &self.field_indexes,
            &self.composite_indexes,
        );

        let skip_post_filter = query::is_fully_indexed(&query, &self.field_indexes);

        let early_limit: Option<usize> = if opts.sort.is_none() && opts.skip.is_none() {
            opts.limit.map(|l| l as usize)
        } else {
            None
        };

        let mut results = Vec::new();

        if let Some(ref indexed_ids) = candidate_ids {
            for &id in indexed_ids {
                if let Some(data) = self.doc_cache.get(&id) {
                    if skip_post_filter || query::matches_value(&query, data) {
                        results.push(Arc::clone(data));
                        if let Some(limit) = early_limit {
                            if results.len() >= limit {
                                break;
                            }
                        }
                    }
                }
            }
        } else {
            for (_id, data) in &self.doc_cache {
                if query::matches_value(&query, data) {
                    results.push(Arc::clone(data));
                    if let Some(limit) = early_limit {
                        if results.len() >= limit {
                            break;
                        }
                    }
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

        // Try index-accelerated lookup
        let candidate_ids = if !matches!(query, Query::All) {
            query::execute_indexed(
                &query,
                &self.field_indexes,
                &self.composite_indexes,
            )
        } else {
            None
        };

        let skip_post_filter = query::is_fully_indexed(&query, &self.field_indexes);

        if let Some(ref indexed_ids) = candidate_ids {
            for &id in indexed_ids {
                if let Some(&loc) = self.primary_index.get(&id) {
                    let data = self.read_doc_value(id, loc)?;
                    if skip_post_filter || query::matches_value(&query, &data) {
                        return Ok(Some((*data).clone()));
                    }
                }
            }
        } else {
            // No index — iterate cache directly (no BTreeSet allocation)
            for (_id, data) in &self.doc_cache {
                if query::matches_value(&query, &data) {
                    return Ok(Some((**data).clone()));
                }
            }
        }

        Ok(None)
    }

    /// Get a document by its _id directly.
    pub fn get(&self, id: DocumentId) -> Result<Option<Value>> {
        if let Some(&loc) = self.primary_index.get(&id) {
            let data = self.read_doc_value(id, loc)?;
            Ok(Some((*data).clone()))
        } else {
            Ok(None)
        }
    }

    /// Update documents matching a query atomically. Returns number of updated documents.
    /// If any unique constraint is violated, no documents are modified.
    /// `limit` caps the number of documents to update (e.g. `Some(1)` for update_one).
    pub fn update(&mut self, query_json: &Value, update_json: &Value, limit: Option<usize>) -> Result<u64> {
        // Validate update document has at least one operator
        let update_obj = update_json
            .as_object()
            .ok_or_else(|| Error::InvalidQuery("update must be an object".into()))?;
        if update_obj.is_empty() {
            return Err(Error::InvalidQuery(
                "update must contain at least one operator".into(),
            ));
        }

        let query = query::parse_query(query_json)?;
        let candidate_ids = query::execute_indexed(
            &query,
            &self.field_indexes,
            &self.composite_indexes,
        );

        // Phase 1: Find matching docs (with early termination via limit)
        let mut matches: Vec<(DocumentId, Arc<Value>, DocLocation)> = Vec::new();

        if let Some(ref indexed_ids) = candidate_ids {
            for &id in indexed_ids {
                if let Some(&old_loc) = self.primary_index.get(&id) {
                    let data = self.read_doc_value(id, old_loc)?;
                    if query::matches_value(&query, &data) {
                        matches.push((id, data, old_loc));
                        if limit.is_some_and(|l| matches.len() >= l) { break; }
                    }
                }
            }
        } else {
            // No index — iterate cache directly, NO .collect()
            for (&id, data) in &self.doc_cache {
                if query::matches_value(&query, data) {
                    if let Some(&old_loc) = self.primary_index.get(&id) {
                        matches.push((id, Arc::clone(data), old_loc));
                        if limit.is_some_and(|l| matches.len() >= l) { break; }
                    }
                }
            }
        }

        if matches.is_empty() {
            return Ok(0);
        }

        // Phase 2: Prepare all updates and validate constraints upfront
        struct UpdateOp {
            id: DocumentId,
            old_loc: DocLocation,
            old_data: Arc<Value>,
            new_data: Value,
            new_bytes: Vec<u8>,
        }
        let mut ops = Vec::with_capacity(matches.len());

        for (id, data, old_loc) in &matches {
            let old_data = Arc::clone(data);
            let mut mutable_data = (**data).clone();

            crate::update::apply_update(&mut mutable_data, update_json)?;

            let old_version = mutable_data.get("_version").and_then(|v| v.as_u64()).unwrap_or(0);
            let new_version = old_version + 1;
            mutable_data.as_object_mut()
                .unwrap()
                .insert("_version".to_string(), Value::Number(new_version.into()));

            self.check_unique_constraints(&mutable_data, Some(*id))?;

            let new_bytes = crate::codec::encode_doc(&mutable_data)?;
            ops.push(UpdateOp {
                id: *id,
                old_loc: *old_loc,
                old_data,
                new_data: mutable_data,
                new_bytes,
            });
        }

        if ops.is_empty() {
            return Ok(0);
        }

        // Phase 2: WAL log all updates → single fsync
        let wal_entries: Vec<WalEntry> = ops
            .iter()
            .map(|op| WalEntry::update(op.id, op.new_bytes.clone()))
            .collect();
        self.wal.log_batch_no_sync(&wal_entries)?;

        // Phase 3: apply all mutations to .dat → single fsync (the only fsync)
        let mut new_locs = Vec::with_capacity(ops.len());
        for op in &ops {
            let new_loc = self.storage.append_no_sync(&op.new_bytes)?;
            self.storage.mark_deleted_no_sync(op.old_loc)?;
            new_locs.push(new_loc);
        }
        self.storage.sync()?;

        // Phase 4: lazy WAL checkpoint (no fsync)
        self.wal.checkpoint_no_sync()?;

        // Phase 5: update in-memory state
        let count = ops.len() as u64;
        for (op, new_loc) in ops.into_iter().zip(new_locs) {
            self.primary_index.insert(op.id, new_loc);
            let new_version = op.new_data.get("_version").and_then(|v| v.as_u64()).unwrap_or(1);
            self.version_index.insert(op.id, new_version);
            let cached = Arc::new(op.new_data);
            self.cache_put(op.id, Arc::clone(&cached));
            for idx in self.field_indexes.values_mut() {
                idx.remove_value(op.id, &op.old_data);
                idx.insert_value(op.id, &cached);
            }
            for idx in &mut self.composite_indexes {
                idx.remove_value(op.id, &op.old_data);
                idx.insert_value(op.id, &cached);
            }
            if let Some(ref mut text_idx) = self.text_index {
                text_idx.index_doc(op.id, &cached);
            }
        }

        Ok(count)
    }

    /// Delete documents matching a query atomically. Returns number deleted.
    /// `limit` caps the number of documents to delete (e.g. `Some(1)` for delete_one).
    pub fn delete(&mut self, query_json: &Value, limit: Option<usize>) -> Result<u64> {
        let query = query::parse_query(query_json)?;
        let candidate_ids = query::execute_indexed(
            &query,
            &self.field_indexes,
            &self.composite_indexes,
        );

        // Phase 1: Find matching docs (with early termination via limit)
        struct DeleteOp {
            id: DocumentId,
            loc: DocLocation,
            data: Arc<Value>,
        }
        let mut ops = Vec::new();

        if let Some(ref indexed_ids) = candidate_ids {
            for &id in indexed_ids {
                if let Some(&loc) = self.primary_index.get(&id) {
                    let data = self.read_doc_value(id, loc)?;
                    if query::matches_value(&query, &data) {
                        ops.push(DeleteOp { id, loc, data });
                        if limit.is_some_and(|l| ops.len() >= l) { break; }
                    }
                }
            }
        } else {
            // No index — iterate cache directly, NO .collect()
            for (&id, data) in &self.doc_cache {
                if query::matches_value(&query, data) {
                    if let Some(&loc) = self.primary_index.get(&id) {
                        ops.push(DeleteOp { id, loc, data: Arc::clone(data) });
                        if limit.is_some_and(|l| ops.len() >= l) { break; }
                    }
                }
            }
        }

        if ops.is_empty() {
            return Ok(0);
        }

        // Phase 2: WAL log all deletes (no fsync — storage fsync provides durability)
        let wal_entries: Vec<WalEntry> = ops
            .iter()
            .map(|op| WalEntry::delete(op.id))
            .collect();
        self.wal.log_batch_no_sync(&wal_entries)?;

        // Phase 3: mark all deleted in .dat → single fsync (the only fsync)
        for op in &ops {
            self.storage.mark_deleted_no_sync(op.loc)?;
        }
        self.storage.sync()?;

        // Phase 4: lazy WAL checkpoint (no fsync)
        self.wal.checkpoint_no_sync()?;

        // Phase 5: update in-memory state
        let count = ops.len() as u64;
        for op in ops {
            self.primary_index.remove(&op.id);
            self.version_index.remove(&op.id);
            self.doc_cache.remove(&op.id);
            for idx in self.field_indexes.values_mut() {
                idx.remove_value(op.id, &op.data);
            }
            for idx in &mut self.composite_indexes {
                idx.remove_value(op.id, &op.data);
            }
            if let Some(ref mut text_idx) = self.text_index {
                text_idx.remove_doc(op.id);
            }
        }

        Ok(count)
    }

    /// Returns the number of documents in the collection.
    pub fn count(&self) -> usize {
        self.primary_index.len()
    }

    /// Count documents matching a query without building a Vec<Value>.
    pub fn count_matching(&self, query_json: &Value) -> Result<usize> {
        let query = query::parse_query(query_json)?;

        // Fast path: count directly from index (no BTreeSet, no doc reads)
        if let Some(count) = query::count_indexed(&query, &self.field_indexes) {
            return Ok(count);
        }

        // Slow path: need to scan docs
        let candidate_ids = query::execute_indexed(
            &query,
            &self.field_indexes,
            &self.composite_indexes,
        );

        let skip_post_filter = query::is_fully_indexed(&query, &self.field_indexes);

        let mut count = 0;
        if let Some(ref indexed_ids) = candidate_ids {
            if skip_post_filter {
                return Ok(indexed_ids.len());
            }
            for &id in indexed_ids {
                if let Some(&loc) = self.primary_index.get(&id) {
                    let data = self.read_doc_value(id, loc)?;
                    if query::matches_value(&query, &data) {
                        count += 1;
                    }
                }
            }
        } else {
            // No index — iterate cache directly, zero clones
            for (_id, data) in &self.doc_cache {
                if query::matches_value(&query, &data) {
                    count += 1;
                }
            }
        }
        Ok(count)
    }

    /// Compact the data file by rewriting only active records.
    /// Reclaims space from deleted documents and rebuilds all indexes.
    pub fn compact(&mut self) -> Result<CompactStats> {
        // Ensure WAL is clean
        self.wal.checkpoint()?;

        let old_size = self.storage.file_size();

        // Create temp storage (with same encryption key if present)
        let tmp_path = self.data_dir.join(format!("{}.dat.tmp", self.name));
        let new_storage = Storage::open_with_encryption(&tmp_path, self.encryption.clone())?;

        // Copy active records to new file
        let active_records = self.storage.iter_active()?;
        let mut new_primary_index = HashMap::new();
        let mut next_id: DocumentId = 1;

        for (_old_offset, bytes) in &active_records {
            let doc: Value = crate::codec::decode_doc(bytes)?;
            let id = doc.get("_id").and_then(|v| v.as_u64()).ok_or_else(|| {
                Error::InvalidQuery("document missing _id during compaction".into())
            })?;

            // Re-encode as JSONB (converts legacy JSON records on compact)
            let new_bytes = crate::codec::encode_doc(&doc)?;
            let loc = new_storage.append_no_sync(&new_bytes)?;
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
        self.storage = Storage::open_with_encryption(&dat_path, self.encryption.clone())?;
        self.primary_index = new_primary_index;
        self.next_id = next_id;

        // Rebuild all indexes, version_index, and doc_cache
        self.version_index.clear();
        self.doc_cache.clear();
        for idx in self.field_indexes.values_mut() {
            idx.clear();
        }
        for idx in &mut self.composite_indexes {
            idx.clear();
        }
        if let Some(ref mut text_idx) = self.text_index {
            text_idx.clear();
        }
        for (&id, &loc) in &self.primary_index.clone() {
            let bytes = self.storage.read(loc)?;
            let data: Value = crate::codec::decode_doc(&bytes)?;
            let ver = data.get("_version").and_then(|v| v.as_u64()).unwrap_or(0);
            self.version_index.insert(id, ver);
            let cached = Arc::new(data);
            self.doc_cache.insert(id, Arc::clone(&cached));
            for idx in self.field_indexes.values_mut() {
                idx.insert_value(id, &cached);
            }
            for idx in &mut self.composite_indexes {
                idx.insert_value(id, &cached);
            }
            if let Some(ref mut text_idx) = self.text_index {
                text_idx.index_doc(id, &cached);
            }
        }

        Ok(CompactStats {
            old_size,
            new_size,
            docs_kept,
        })
    }

    // -----------------------------------------------------------------------
    // Version tracking
    // -----------------------------------------------------------------------

    /// Get the current version of a document (0 if not found).
    pub fn get_version(&self, doc_id: DocumentId) -> u64 {
        self.version_index.get(&doc_id).copied().unwrap_or(0)
    }

    /// Log a batch of WAL entries (used by the engine during transactional commit).
    pub fn log_wal_batch(&self, entries: &[WalEntry]) -> Result<()> {
        self.wal.log_batch(entries)
    }

    /// Checkpoint the WAL (used by the engine after transactional apply).
    pub fn checkpoint_wal(&self) -> Result<()> {
        self.wal.checkpoint()
    }

    // -----------------------------------------------------------------------
    // Transactional prepare helpers (called by engine with write lock held)
    // -----------------------------------------------------------------------

    /// Prepare a transactional insert. Returns (doc_id, PreparedMutation).
    /// Does NOT touch WAL or storage -- caller orchestrates.
    pub fn prepare_tx_insert(&mut self, mut data: Value, tx_id: u64) -> Result<PreparedMutation> {
        if !data.is_object() {
            return Err(Error::NotAnObject);
        }

        let id = self.next_id;
        let obj = data.as_object_mut().unwrap();
        obj.insert("_id".to_string(), Value::Number(id.into()));
        obj.insert("_version".to_string(), Value::Number(1.into()));

        self.check_unique_constraints(&data, None)?;

        self.next_id += 1;

        let bytes = crate::codec::encode_doc(&data)?;

        Ok(PreparedMutation {
            wal_entry: WalEntry::Insert { doc_id: id, doc_bytes: bytes.clone(), tx_id },
            doc_id: id,
            new_bytes: bytes,
            old_loc: None,
            old_data: None,
            new_data: data,
            is_delete: false,
        })
    }

    /// Prepare transactional updates. Returns Vec<PreparedMutation>.
    pub fn prepare_tx_update(
        &mut self,
        query_json: &Value,
        update_json: &Value,
        tx_id: u64,
    ) -> Result<Vec<PreparedMutation>> {
        let update_obj = update_json
            .as_object()
            .ok_or_else(|| Error::InvalidQuery("update must be an object".into()))?;
        if update_obj.is_empty() {
            return Err(Error::InvalidQuery(
                "update must contain at least one operator".into(),
            ));
        }

        // Single-pass scan with cache
        let query = query::parse_query(query_json)?;
        let candidate_ids = query::execute_indexed(
            &query,
            &self.field_indexes,
            &self.composite_indexes,
        );

        let mut mutations = Vec::new();

        let mut process_candidate = |id: DocumentId, cached: &Arc<Value>, old_loc: DocLocation| -> Result<()> {
            if !query::matches_value(&query, &cached) {
                return Ok(());
            }
            let old_data = (**cached).clone();
            let mut data = (**cached).clone();

            crate::update::apply_update(&mut data, update_json)?;

            let old_version = data.get("_version").and_then(|v| v.as_u64()).unwrap_or(0);
            let new_version = old_version + 1;
            data.as_object_mut()
                .unwrap()
                .insert("_version".to_string(), Value::Number(new_version.into()));

            self.check_unique_constraints(&data, Some(id))?;

            let new_bytes = crate::codec::encode_doc(&data)?;
            mutations.push(PreparedMutation {
                wal_entry: WalEntry::Update { doc_id: id, doc_bytes: new_bytes.clone(), tx_id },
                doc_id: id,
                new_bytes,
                old_loc: Some(old_loc),
                old_data: Some(old_data),
                new_data: data,
                is_delete: false,
            });
            Ok(())
        };

        if let Some(ref indexed_ids) = candidate_ids {
            for &id in indexed_ids {
                if let Some(&old_loc) = self.primary_index.get(&id) {
                    let data = self.read_doc_value(id, old_loc)?;
                    process_candidate(id, &data, old_loc)?;
                }
            }
        } else {
            let cache_snapshot: Vec<(DocumentId, Arc<Value>, DocLocation)> = self.doc_cache.iter()
                .filter_map(|(&id, data)| {
                    self.primary_index.get(&id).map(|&loc| (id, Arc::clone(data), loc))
                })
                .collect();
            for (id, data, old_loc) in cache_snapshot {
                process_candidate(id, &data, old_loc)?;
            }
        }

        Ok(mutations)
    }

    /// Prepare transactional deletes. Returns Vec<PreparedMutation>.
    pub fn prepare_tx_delete(
        &mut self,
        query_json: &Value,
        tx_id: u64,
    ) -> Result<Vec<PreparedMutation>> {
        // Single-pass scan with cache
        let query = query::parse_query(query_json)?;
        let candidate_ids = query::execute_indexed(
            &query,
            &self.field_indexes,
            &self.composite_indexes,
        );

        let mut mutations = Vec::new();

        let mut process_candidate = |id: DocumentId, cached: &Arc<Value>, loc: DocLocation| -> Result<()> {
            if !query::matches_value(&query, &cached) {
                return Ok(());
            }
            mutations.push(PreparedMutation {
                wal_entry: WalEntry::Delete { doc_id: id, tx_id },
                doc_id: id,
                new_bytes: vec![],
                old_loc: Some(loc),
                old_data: Some((**cached).clone()),
                new_data: Value::Null,
                is_delete: true,
            });
            Ok(())
        };

        if let Some(ref indexed_ids) = candidate_ids {
            for &id in indexed_ids {
                if let Some(&loc) = self.primary_index.get(&id) {
                    let data = self.read_doc_value(id, loc)?;
                    process_candidate(id, &data, loc)?;
                }
            }
        } else {
            let cache_snapshot: Vec<(DocumentId, Arc<Value>, DocLocation)> = self.doc_cache.iter()
                .filter_map(|(&id, data)| {
                    self.primary_index.get(&id).map(|&loc| (id, Arc::clone(data), loc))
                })
                .collect();
            for (id, data, loc) in cache_snapshot {
                process_candidate(id, &data, loc)?;
            }
        }

        Ok(mutations)
    }

    /// Apply a batch of prepared mutations to storage and update indexes.
    /// WAL should already have been logged by the caller.
    pub fn apply_prepared(&mut self, mutations: &mut Vec<PreparedMutation>) -> Result<()> {
        // Apply to storage
        for m in mutations.iter() {
            if m.is_delete {
                if let Some(loc) = m.old_loc {
                    self.storage.mark_deleted_no_sync(loc)?;
                }
            } else if let Some(old_loc) = m.old_loc {
                // Update
                self.storage.mark_deleted_no_sync(old_loc)?;
            }
        }

        // Inserts and updates: append new bytes
        let mut new_locs = Vec::with_capacity(mutations.len());
        for m in mutations.iter() {
            if m.is_delete {
                new_locs.push(None);
            } else {
                let loc = self.storage.append_no_sync(&m.new_bytes)?;
                new_locs.push(Some(loc));
            }
        }
        self.storage.sync()?;

        // Update in-memory indexes
        for (i, m) in mutations.iter().enumerate() {
            if m.is_delete {
                self.primary_index.remove(&m.doc_id);
                self.version_index.remove(&m.doc_id);
                self.doc_cache.remove(&m.doc_id);
                if let Some(ref old_data) = m.old_data {
                    for idx in self.field_indexes.values_mut() {
                        idx.remove_value(m.doc_id, old_data);
                    }
                    for idx in &mut self.composite_indexes {
                        idx.remove_value(m.doc_id, old_data);
                    }
                }
                if let Some(ref mut text_idx) = self.text_index {
                    text_idx.remove_doc(m.doc_id);
                }
            } else if let Some(loc) = new_locs[i] {
                self.primary_index.insert(m.doc_id, loc);
                let ver = m.new_data.get("_version").and_then(|v| v.as_u64()).unwrap_or(1);
                self.version_index.insert(m.doc_id, ver);
                let cached = Arc::new(m.new_data.clone());
                self.cache_put(m.doc_id, Arc::clone(&cached));

                if let Some(ref old_data) = m.old_data {
                    for idx in self.field_indexes.values_mut() {
                        idx.remove_value(m.doc_id, old_data);
                    }
                    for idx in &mut self.composite_indexes {
                        idx.remove_value(m.doc_id, old_data);
                    }
                }
                for idx in self.field_indexes.values_mut() {
                    idx.insert_value(m.doc_id, &cached);
                }
                for idx in &mut self.composite_indexes {
                    idx.insert_value(m.doc_id, &cached);
                }
                if let Some(ref mut text_idx) = self.text_index {
                    text_idx.index_doc(m.doc_id, &cached);
                }
            }
        }

        Ok(())
    }
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
    fn insert_assigns_version_1() {
        let mut col = temp_collection("test");
        let id = col.insert(json!({"name": "Alice"})).unwrap();
        let doc = col.get(id).unwrap().unwrap();
        assert_eq!(doc["_version"], 1);
        assert_eq!(col.get_version(id), 1);
    }

    #[test]
    fn update_increments_version() {
        let mut col = temp_collection("test");
        let id = col.insert(json!({"name": "Alice"})).unwrap();
        assert_eq!(col.get_version(id), 1);
        col.update(&json!({"_id": id}), &json!({"$set": {"name": "Bob"}}), None).unwrap();
        let doc = col.get(id).unwrap().unwrap();
        assert_eq!(doc["_version"], 2);
        assert_eq!(col.get_version(id), 2);
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
            .update(&json!({"name": "Alice"}), &json!({"$set": {"age": 31}}), None)
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

        let count = col.delete(&json!({"name": "Alice"}), None).unwrap();
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
                None,
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
            None,
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
                None,
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

        let count = col.delete(&json!({"status": "old"}), None).unwrap();
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
        col.delete(&json!({"n": {"$lt": 7}}), None).unwrap();
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
