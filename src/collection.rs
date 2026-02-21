use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde_json::Value;

use crate::crypto::EncryptionKey;
use crate::document::DocumentId;
use crate::engine::LogCallback;
use crate::error::{Error, Result};
use crate::fts::CollectionTextIndex;
use crate::index::{CompositeIndex, FieldIndex};
use crate::index_persist;
use crate::vector::{DistanceMetric, VectorIndex};
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
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IndexInfo {
    pub name: String,
    pub index_type: String,
    pub fields: Vec<String>,
    pub unique: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dimension: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metric: Option<String>,
}

/// Persisted index metadata (written to .idx files).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct IndexMetadata {
    version: u32,
    indexes: Vec<IndexInfo>,
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
    doc_cache: HashMap<DocumentId, Arc<Value>>,
    field_indexes: HashMap<String, FieldIndex>,
    composite_indexes: Vec<CompositeIndex>,
    text_index: Option<CollectionTextIndex>,
    vector_indexes: HashMap<String, VectorIndex>,
    version_index: HashMap<DocumentId, u64>,
    next_id: DocumentId,
    encryption: Option<Arc<EncryptionKey>>,
    verbose: bool,
    log_callback: Option<LogCallback>,
}

impl Collection {
    /// Write a verbose message to stderr and forward to the GELF log callback if set.
    fn vlog(&self, msg: &str) {
        eprintln!("{msg}");
        if let Some(cb) = &self.log_callback {
            cb(msg);
        }
    }
}

/// Load persisted index definitions from a .idx file.
fn load_index_metadata(path: &Path) -> Result<Vec<IndexInfo>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let bytes = std::fs::read(path)?;
    let meta: IndexMetadata = serde_json::from_slice(&bytes)
        .map_err(|e| Error::InvalidQuery(format!("corrupt .idx file: {}", e)))?;
    Ok(meta.indexes)
}

impl Collection {
    /// Persist current index definitions to a .idx file alongside the .dat file.
    fn save_index_metadata(&self) -> Result<()> {
        let indexes = self.list_indexes();
        let meta = IndexMetadata {
            version: 1,
            indexes,
        };
        let path = self.data_dir.join(format!("{}.idx", self.name));
        let json = serde_json::to_vec_pretty(&meta)?;
        std::fs::write(&path, json)?;
        Ok(())
    }

    /// Persist current index data (BTreeMap contents) to binary cache files.
    /// Called after create_index, create_unique_index, create_composite_index, and compact.
    pub fn save_index_data(&self) {
        let doc_count = self.primary_index.len() as u64;
        let next_id = self.next_id;

        // Save field indexes (.fidx)
        let fidx_path = self.data_dir.join(format!("{}.fidx", self.name));
        let field_refs: Vec<&FieldIndex> = self.field_indexes.values().collect();
        if let Err(e) = index_persist::save_field_indexes(&fidx_path, &field_refs, doc_count, next_id) {
            eprintln!("[warn] {}: failed to save field index cache: {}", self.name, e);
        }

        // Save composite indexes (.cidx)
        let cidx_path = self.data_dir.join(format!("{}.cidx", self.name));
        let comp_refs: Vec<&CompositeIndex> = self.composite_indexes.iter().collect();
        if let Err(e) = index_persist::save_composite_indexes(&cidx_path, &comp_refs, doc_count, next_id) {
            eprintln!("[warn] {}: failed to save composite index cache: {}", self.name, e);
        }

        // Save vector indexes (.vidx)
        let vidx_path = self.data_dir.join(format!("{}.vidx", self.name));
        let vec_refs: Vec<&VectorIndex> = self.vector_indexes.values().collect();
        if let Err(e) = index_persist::save_vector_indexes(&vidx_path, &vec_refs, doc_count, next_id) {
            eprintln!("[warn] {}: failed to save vector index cache: {}", self.name, e);
        }
    }

    /// Create or open a collection backed by a data file.
    pub fn open(name: &str, data_dir: &Path) -> Result<Self> {
        Self::open_with_options(name, data_dir, &HashSet::new(), None, false, None)
    }

    /// Create or open a collection, filtering WAL recovery by committed tx_ids.
    pub fn open_with_committed_txs(
        name: &str,
        data_dir: &Path,
        committed_tx_ids: &HashSet<u64>,
    ) -> Result<Self> {
        Self::open_with_options(name, data_dir, committed_tx_ids, None, false, None)
    }

    /// Create or open a collection with optional encryption and tx recovery.
    pub fn open_with_options(
        name: &str,
        data_dir: &Path,
        committed_tx_ids: &HashSet<u64>,
        encryption: Option<Arc<EncryptionKey>>,
        verbose: bool,
        log_callback: Option<LogCallback>,
    ) -> Result<Self> {
        let vlog = |msg: &str| {
            eprintln!("{msg}");
            if let Some(cb) = &log_callback {
                cb(msg);
            }
        };

        let data_path = data_dir.join(format!("{}.dat", name));
        let wal_path = data_dir.join(format!("{}.wal", name));
        let storage = Storage::open_with_encryption(&data_path, encryption.clone())?;
        let wal = Wal::open_with_encryption(&wal_path, encryption.clone())?;

        if verbose {
            let file_size = storage.file_size();
            vlog(&format!("[verbose] {}: storage file {} bytes", name, file_size));
        }

        // Load persisted index definitions (if any)
        let idx_path = data_dir.join(format!("{}.idx", name));
        let persisted_indexes = load_index_metadata(&idx_path)?;
        let has_persisted_indexes = !persisted_indexes.is_empty();

        // Pre-create empty index structures from metadata
        let mut field_indexes: HashMap<String, FieldIndex> = HashMap::new();
        let mut composite_indexes: Vec<CompositeIndex> = Vec::new();
        let mut text_index: Option<CollectionTextIndex> = None;
        let mut vector_indexes: HashMap<String, VectorIndex> = HashMap::new();

        for info in &persisted_indexes {
            match info.index_type.as_str() {
                "field" => {
                    field_indexes.insert(
                        info.name.clone(),
                        FieldIndex::new(info.name.clone()),
                    );
                }
                "unique" => {
                    field_indexes.insert(
                        info.name.clone(),
                        FieldIndex::new_unique(info.name.clone()),
                    );
                }
                "composite" => {
                    composite_indexes.push(CompositeIndex::new(info.fields.clone()));
                }
                "text" => {
                    text_index = Some(CollectionTextIndex::new(info.fields.clone()));
                }
                "vector" => {
                    if let (Some(dim), Some(metric_str)) = (info.dimension, info.metric.as_deref()) {
                        let field = info.fields.first().cloned().unwrap_or_default();
                        let metric = VectorIndex::parse_metric(metric_str);
                        vector_indexes.insert(
                            field.clone(),
                            VectorIndex::new(field, dim, metric),
                        );
                    }
                }
                _ => {}
            }
        }

        if verbose && has_persisted_indexes {
            vlog(&format!(
                "[verbose] {}: loaded {} index definitions from .idx (will rebuild during load)",
                name,
                persisted_indexes.len()
            ));
        }

        let mut primary_index = HashMap::new();
        let mut doc_cache: HashMap<DocumentId, Arc<Value>> = HashMap::new();
        let mut version_index = HashMap::new();
        let mut next_id: DocumentId = 1;

        let load_start = std::time::Instant::now();
        let mut doc_count: u64 = 0;

        // Clone callback for use inside the closure
        let inner_cb = log_callback.clone();

        // Phase 1: Scan .dat for primary_index, doc_cache, version_index, next_id.
        // Also rebuild text index (always from docs — not cached).
        // Field/composite indexes are NOT rebuilt here; we try the cache first.
        storage.for_each_active(|loc, bytes| {
            let doc: Value = crate::codec::decode_doc(&bytes)?;
            if let Some(id) = doc.get("_id").and_then(|v| v.as_u64()) {
                primary_index.insert(id, loc);
                let ver = doc.get("_version").and_then(|v| v.as_u64()).unwrap_or(0);
                version_index.insert(id, ver);
                if id >= next_id {
                    next_id = id + 1;
                }

                let doc_arc = Arc::new(doc);

                // Text index is always rebuilt from docs (not cached)
                if let Some(ref mut ti) = text_index {
                    ti.index_doc(id, &doc_arc);
                }

                doc_cache.insert(id, doc_arc);
            }
            doc_count += 1;
            if verbose && doc_count % 500_000 == 0 {
                let msg = format!(
                    "[verbose] {}: loaded {} documents... ({:.1}s)",
                    name,
                    doc_count,
                    load_start.elapsed().as_secs_f64()
                );
                eprintln!("{msg}");
                if let Some(cb) = &inner_cb {
                    cb(&msg);
                }
            }
            Ok(())
        })?;

        if verbose {
            vlog(&format!(
                "[verbose] {}: {} documents loaded in {:.2}s (primary scan)",
                name,
                doc_count,
                load_start.elapsed().as_secs_f64(),
            ));
        }

        // Phase 2: Try loading cached index data (.fidx / .cidx / .vidx)
        let mut indexes_from_cache = false;
        if has_persisted_indexes {
            let fidx_path = data_dir.join(format!("{}.fidx", name));
            let cidx_path = data_dir.join(format!("{}.cidx", name));
            let vidx_path = data_dir.join(format!("{}.vidx", name));

            let cached_field = index_persist::load_field_indexes(
                &fidx_path,
                doc_count,
                next_id,
            );
            let cached_composite = index_persist::load_composite_indexes(
                &cidx_path,
                doc_count,
                next_id,
            );
            let cached_vector = index_persist::load_vector_indexes(
                &vidx_path,
                doc_count,
                next_id,
            );

            // Both must succeed for the cache to be valid
            let field_ok = cached_field.is_some() || field_indexes.is_empty();
            let comp_ok = cached_composite.is_some() || composite_indexes.is_empty();
            let vec_ok = cached_vector.is_some() || vector_indexes.is_empty();

            if field_ok && comp_ok && vec_ok {
                let cache_start = std::time::Instant::now();
                if let Some(cached) = cached_field {
                    // Replace empty index structures with cached ones
                    field_indexes.clear();
                    for idx in cached {
                        field_indexes.insert(idx.field.clone(), idx);
                    }
                }
                if let Some(cached) = cached_composite {
                    composite_indexes = cached;
                }
                if let Some(cached) = cached_vector {
                    vector_indexes.clear();
                    for idx in cached {
                        vector_indexes.insert(idx.field.clone(), idx);
                    }
                }
                indexes_from_cache = true;
                if verbose {
                    vlog(&format!(
                        "[verbose] {}: loaded index data from cache in {:.3}s",
                        name,
                        cache_start.elapsed().as_secs_f64(),
                    ));
                }
            }
        }

        // Phase 2b: If cache was invalid, rebuild indexes from doc_cache (zero disk I/O)
        if has_persisted_indexes && !indexes_from_cache {
            if verbose {
                vlog(&format!(
                    "[verbose] {}: index cache invalid, rebuilding {} indexes from doc_cache...",
                    name,
                    persisted_indexes.len(),
                ));
            }
            let rebuild_start = std::time::Instant::now();
            let mut rebuild_count = 0u64;

            for (&id, arc) in &doc_cache {
                for idx in field_indexes.values_mut() {
                    idx.insert_value(id, arc);
                }
                for idx in &mut composite_indexes {
                    idx.insert_value(id, arc);
                }
                for idx in vector_indexes.values_mut() {
                    let _ = idx.insert(id, arc);
                }
                rebuild_count += 1;
                if verbose && rebuild_count % 500_000 == 0 {
                    let msg = format!(
                        "[verbose] {}: index rebuild {} / {} docs ({:.1}s)",
                        name,
                        rebuild_count,
                        doc_count,
                        rebuild_start.elapsed().as_secs_f64()
                    );
                    vlog(&msg);
                }
            }

            if verbose {
                vlog(&format!(
                    "[verbose] {}: rebuilt {} indexes in {:.2}s",
                    name,
                    persisted_indexes.len(),
                    rebuild_start.elapsed().as_secs_f64(),
                ));
            }
        }

        // Phase 3: WAL recovery (updates indexes and doc_cache too)
        wal.recover(
            &storage,
            &mut primary_index,
            &mut doc_cache,
            &mut next_id,
            committed_tx_ids,
            &mut version_index,
            &mut field_indexes,
            &mut composite_indexes,
            verbose,
            &log_callback,
        )?;

        if verbose {
            vlog(&format!("[verbose] {}: collection ready", name));
        }

        let collection = Self {
            name: name.to_string(),
            data_dir: data_dir.to_path_buf(),
            storage,
            wal,
            primary_index,
            doc_cache,
            field_indexes,
            composite_indexes,
            text_index,
            vector_indexes,
            version_index,
            next_id,
            encryption,
            verbose,
            log_callback,
        };

        // Save index cache after rebuild so next restart loads from cache
        if has_persisted_indexes && !indexes_from_cache {
            collection.save_index_data();
        }

        Ok(collection)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Access the field indexes for index-accelerated aggregation.
    pub fn field_indexes(&self) -> &HashMap<String, FieldIndex> {
        &self.field_indexes
    }

    /// Access the document cache for index-accelerated aggregation.
    pub fn doc_cache(&self) -> &HashMap<DocumentId, Arc<Value>> {
        &self.doc_cache
    }

    /// Read a document by its ID from the in-memory cache.
    fn read_doc(&self, id: DocumentId) -> Result<Option<Value>> {
        Ok(self.doc_cache.get(&id).map(|arc| (**arc).clone()))
    }

    /// Read a document by its ID, returning an Arc (zero-copy from cache).
    fn read_doc_arc(&self, id: DocumentId) -> Option<Arc<Value>> {
        self.doc_cache.get(&id).map(Arc::clone)
    }

    /// Iterate all documents, calling `f` for each one.
    fn for_each_doc<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(DocumentId, Value) -> Result<()>,
    {
        for (&id, arc) in &self.doc_cache {
            f(id, (**arc).clone())?;
        }
        Ok(())
    }

    /// Iterate all documents as Arc references. Zero-clone.
    /// Stops early when `f` returns `Ok(false)`.
    fn for_each_doc_arc_while<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(DocumentId, &Arc<Value>) -> Result<bool>,
    {
        for (&id, arc) in &self.doc_cache {
            if !f(id, arc)? {
                break;
            }
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Index management
    // -----------------------------------------------------------------------

    /// Create a single-field index. Rebuilds from existing documents.
    /// If the index already exists (e.g. rebuilt from persisted metadata on load),
    /// returns Ok immediately — making this call idempotent.
    pub fn create_index(&mut self, field: &str) -> Result<()> {
        if self.field_indexes.contains_key(field) {
            return Ok(());
        }

        let total = self.primary_index.len();
        if self.verbose {
            self.vlog(&format!(
                "[verbose] {}: creating index on '{}' ({} docs to scan)",
                self.name, field, total
            ));
        }
        let start = std::time::Instant::now();
        let mut count = 0u64;
        let mut idx = FieldIndex::new(field.to_string());

        // Backfill from doc cache (zero disk I/O)
        for (&id, arc) in &self.doc_cache {
            idx.insert_value(id, arc);
            count += 1;
            if self.verbose && count % 500_000 == 0 {
                self.vlog(&format!(
                    "[verbose] {}: index '{}' scanned {} / {} docs ({:.1}s)",
                    self.name, field, count, total, start.elapsed().as_secs_f64()
                ));
            }
        }

        if self.verbose {
            self.vlog(&format!(
                "[verbose] {}: index '{}' ready ({} docs in {:.2}s)",
                self.name, field, count, start.elapsed().as_secs_f64()
            ));
        }
        self.field_indexes.insert(field.to_string(), idx);
        self.save_index_metadata()?;
        self.save_index_data();
        Ok(())
    }

    /// Create a unique single-field index. Rebuilds from existing documents.
    /// Returns error if existing data violates uniqueness.
    /// If the index already exists, returns Ok immediately (idempotent).
    pub fn create_unique_index(&mut self, field: &str) -> Result<()> {
        if self.field_indexes.contains_key(field) {
            return Ok(());
        }

        let total = self.primary_index.len();
        if self.verbose {
            self.vlog(&format!(
                "[verbose] {}: creating unique index on '{}' ({} docs to scan)",
                self.name, field, total
            ));
        }
        let start = std::time::Instant::now();
        let mut count = 0u64;
        let mut idx = FieldIndex::new_unique(field.to_string());
        let field_owned = field.to_string();

        // Backfill from doc cache (zero disk I/O)
        for (&id, arc) in &self.doc_cache {
            if let Some(value) = resolve_field_in_value(arc, &field_owned) {
                let iv = IndexValue::from_json(value);
                if idx.check_unique(&iv, None) {
                    return Err(Error::UniqueViolation {
                        field: field_owned.clone(),
                    });
                }
            }
            idx.insert_value(id, arc);
            count += 1;
            if self.verbose && count % 500_000 == 0 {
                self.vlog(&format!(
                    "[verbose] {}: unique index '{}' scanned {} / {} docs ({:.1}s)",
                    self.name, field, count, total, start.elapsed().as_secs_f64()
                ));
            }
        }

        if self.verbose {
            self.vlog(&format!(
                "[verbose] {}: unique index '{}' ready ({} docs in {:.2}s)",
                self.name, field, count, start.elapsed().as_secs_f64()
            ));
        }
        self.field_indexes.insert(field.to_string(), idx);
        self.save_index_metadata()?;
        self.save_index_data();
        Ok(())
    }

    /// Create a composite (multi-field) index. Rebuilds from existing documents.
    /// If the index already exists, returns Ok with the name (idempotent).
    pub fn create_composite_index(&mut self, fields: Vec<String>) -> Result<String> {
        let name = fields.join("_");
        if self.composite_indexes.iter().any(|i| i.name() == name) {
            return Ok(name);
        }

        let total = self.primary_index.len();
        if self.verbose {
            self.vlog(&format!(
                "[verbose] {}: creating composite index '{}' ({} docs to scan)",
                self.name, name, total
            ));
        }
        let start = std::time::Instant::now();
        let mut count = 0u64;
        let mut idx = CompositeIndex::new(fields);

        // Backfill from doc cache (zero disk I/O)
        for (&id, arc) in &self.doc_cache {
            idx.insert_value(id, arc);
            count += 1;
            if self.verbose && count % 500_000 == 0 {
                self.vlog(&format!(
                    "[verbose] {}: composite index '{}' scanned {} / {} docs ({:.1}s)",
                    self.name, name, count, total, start.elapsed().as_secs_f64()
                ));
            }
        }

        if self.verbose {
            self.vlog(&format!(
                "[verbose] {}: composite index '{}' ready ({} docs in {:.2}s)",
                self.name, name, count, start.elapsed().as_secs_f64()
            ));
        }
        let idx_name = idx.name();
        self.composite_indexes.push(idx);
        self.save_index_metadata()?;
        self.save_index_data();
        Ok(idx_name)
    }

    /// Create a full-text search index on the specified fields.
    /// Rebuilds from existing documents in doc store.
    /// If the index already exists, returns Ok immediately (idempotent).
    pub fn create_text_index(&mut self, fields: Vec<String>) -> Result<()> {
        if self.text_index.is_some() {
            return Ok(());
        }

        let total = self.primary_index.len();
        if self.verbose {
            self.vlog(&format!(
                "[verbose] {}: creating text index on {:?} ({} docs to scan)",
                self.name, fields, total
            ));
        }
        let start = std::time::Instant::now();
        let mut count = 0u64;
        let mut idx = CollectionTextIndex::new(fields);

        // Backfill from doc cache (zero disk I/O)
        for (&id, arc) in &self.doc_cache {
            idx.index_doc(id, arc);
            count += 1;
            if self.verbose && count % 500_000 == 0 {
                self.vlog(&format!(
                    "[verbose] {}: text index scanned {} / {} docs ({:.1}s)",
                    self.name, count, total, start.elapsed().as_secs_f64()
                ));
            }
        }

        if self.verbose {
            self.vlog(&format!(
                "[verbose] {}: text index ready ({} docs in {:.2}s)",
                self.name, count, start.elapsed().as_secs_f64()
            ));
        }
        self.text_index = Some(idx);
        self.save_index_metadata()?;
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
                dimension: None,
                metric: None,
            });
        }
        for idx in &self.composite_indexes {
            indexes.push(IndexInfo {
                name: idx.name(),
                index_type: "composite".to_string(),
                fields: idx.fields.clone(),
                unique: false,
                dimension: None,
                metric: None,
            });
        }
        if let Some(ref text_idx) = self.text_index {
            indexes.push(IndexInfo {
                name: "_text".to_string(),
                index_type: "text".to_string(),
                fields: text_idx.fields().to_vec(),
                unique: false,
                dimension: None,
                metric: None,
            });
        }
        for idx in self.vector_indexes.values() {
            indexes.push(IndexInfo {
                name: format!("_vec_{}", idx.field),
                index_type: "vector".to_string(),
                fields: vec![idx.field.clone()],
                unique: false,
                dimension: Some(idx.dimension),
                metric: Some(idx.metric_str().to_string()),
            });
        }
        indexes
    }

    /// Drop an index by name and update persisted metadata.
    pub fn drop_index(&mut self, name: &str) -> Result<()> {
        if self.field_indexes.remove(name).is_some() {
            self.save_index_metadata()?;
            return Ok(());
        }
        if let Some(pos) = self.composite_indexes.iter().position(|i| i.name() == name) {
            self.composite_indexes.remove(pos);
            self.save_index_metadata()?;
            return Ok(());
        }
        if name == "_text" && self.text_index.is_some() {
            self.text_index = None;
            self.save_index_metadata()?;
            return Ok(());
        }
        if let Some(field) = name.strip_prefix("_vec_")
            && self.vector_indexes.remove(field).is_some()
        {
            self.save_index_metadata()?;
            self.save_index_data();
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
            if let Some(mut doc) = self.read_doc(result.doc_id)? {
                if let Some(obj) = doc.as_object_mut() {
                    obj.insert("_score".to_string(), serde_json::json!(result.score));
                }
                docs.push(doc);
            }
        }
        Ok(docs)
    }

    // -----------------------------------------------------------------------
    // Vector index methods
    // -----------------------------------------------------------------------

    /// Create a vector index on the specified field.
    /// Rebuilds from existing documents in doc cache.
    /// If an index already exists on this field, returns Ok immediately (idempotent).
    pub fn create_vector_index(&mut self, field: &str, dimension: usize, metric: DistanceMetric) -> Result<()> {
        if self.vector_indexes.contains_key(field) {
            return Ok(());
        }

        let total = self.primary_index.len();
        if self.verbose {
            self.vlog(&format!(
                "[verbose] {}: creating vector index on '{}' (dim={}, metric={}, {} docs to scan)",
                self.name, field, dimension, metric.as_str(), total
            ));
        }
        let start = std::time::Instant::now();
        let mut count = 0u64;
        let mut idx = VectorIndex::new(field.to_string(), dimension, metric);

        // Backfill from doc cache (zero disk I/O)
        for (&id, arc) in &self.doc_cache {
            if let Err(e) = idx.insert(id, arc) {
                if self.verbose {
                    self.vlog(&format!(
                        "[verbose] {}: vector index skip doc {}: {}",
                        self.name, id, e
                    ));
                }
            }
            count += 1;
            if self.verbose && count % 500_000 == 0 {
                self.vlog(&format!(
                    "[verbose] {}: vector index '{}' scanned {} / {} docs ({:.1}s)",
                    self.name, field, count, total, start.elapsed().as_secs_f64()
                ));
            }
        }

        if self.verbose {
            self.vlog(&format!(
                "[verbose] {}: vector index '{}' ready ({} vectors from {} docs in {:.2}s)",
                self.name, field, idx.len(), count, start.elapsed().as_secs_f64()
            ));
        }
        self.vector_indexes.insert(field.to_string(), idx);
        self.save_index_metadata()?;
        self.save_index_data();
        Ok(())
    }

    /// Perform vector similarity search. Returns matching documents with `_similarity` score.
    pub fn vector_search(&self, field: &str, query_vector: &[f32], limit: usize, ef_search: Option<usize>) -> Result<Vec<Value>> {
        let idx = self.vector_indexes.get(field).ok_or_else(|| {
            Error::InvalidQuery(format!(
                "no vector index on field '{}'; create one with create_vector_index",
                field
            ))
        })?;

        let search_results = idx.search(query_vector, limit, ef_search)
            .map_err(|e| Error::InvalidQuery(e))?;

        let mut docs = Vec::with_capacity(search_results.len());
        for result in search_results {
            if let Some(mut doc) = self.read_doc(result.doc_id)? {
                if let Some(obj) = doc.as_object_mut() {
                    obj.insert("_similarity".to_string(), serde_json::json!(result.similarity));
                    obj.insert("_distance".to_string(), serde_json::json!(result.distance));
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

        self.primary_index.insert(id, loc);
        self.version_index.insert(id, 1);

        let data_arc = Arc::new(data);

        // Update all field indexes
        for idx in self.field_indexes.values_mut() {
            idx.insert_value(id, &data_arc);
        }
        for idx in &mut self.composite_indexes {
            idx.insert_value(id, &data_arc);
        }
        if let Some(ref mut text_idx) = self.text_index {
            text_idx.index_doc(id, &data_arc);
        }
        for idx in self.vector_indexes.values_mut() {
            let _ = idx.insert(id, &data_arc);
        }

        self.doc_cache.insert(id, data_arc);

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
        let byte_slices: Vec<&[u8]> = prepared.iter().map(|(_, _, bytes)| bytes.as_slice()).collect();
        let batch_locs = self.storage.append_batch_no_sync(&byte_slices)?;
        self.storage.sync()?;

        let mut ids = Vec::with_capacity(prepared.len());
        let mut locs = Vec::with_capacity(prepared.len());
        for ((id, _, _), loc) in prepared.iter().zip(batch_locs) {
            ids.push(*id);
            locs.push((*id, loc));
        }

        // Phase 4: lazy WAL checkpoint (no fsync)
        self.wal.checkpoint_no_sync()?;

        // Phase 5: update in-memory indexes
        self.next_id += prepared.len() as u64;

        for ((id, data, _bytes), (_, loc)) in prepared.into_iter().zip(locs.iter()) {
            self.primary_index.insert(id, *loc);
            self.version_index.insert(id, 1);
            let data_arc = Arc::new(data);
            for idx in self.field_indexes.values_mut() {
                idx.insert_value(id, &data_arc);
            }
            for idx in &mut self.composite_indexes {
                idx.insert_value(id, &data_arc);
            }
            if let Some(ref mut text_idx) = self.text_index {
                text_idx.index_doc(id, &data_arc);
            }
            for idx in self.vector_indexes.values_mut() {
                let _ = idx.insert(id, &data_arc);
            }
            self.doc_cache.insert(id, data_arc);
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

        // Fast path: Query::All with no sort — iterate doc cache directly.
        if matches!(query, Query::All) && opts.sort.is_none() {
            let skip = opts.skip.unwrap_or(0) as usize;
            let limit = opts.limit.map(|l| l as usize).unwrap_or(usize::MAX);
            let mut results = Vec::new();
            let mut skipped = 0;
            self.for_each_doc_arc_while(|_id, arc| {
                if skipped < skip {
                    skipped += 1;
                    return Ok(true);
                }
                if results.len() >= limit {
                    return Ok(false);
                }
                results.push(Arc::clone(arc));
                Ok(true)
            })?;
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
                                    if let Some(arc) = self.read_doc_arc(id) {
                                        if query::matches_value(&query, &arc) {
                                            results.push(arc);
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
                                    if let Some(arc) = self.read_doc_arc(id) {
                                        if query::matches_value(&query, &arc) {
                                            results.push(arc);
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

        // Composite index-backed sort: if a composite index has the query's
        // equality fields as prefix and the sort field as last field, iterate
        // the composite BTreeMap directly in sort order with early termination.
        // Supports post-filtering on additional query conditions beyond the
        // composite prefix (e.g., query on formId + data.x, sort by createdAt,
        // composite index on (formId, createdAt)).
        if let Some(sort_fields) = &opts.sort {
            if sort_fields.len() == 1 {
                let (sort_field, sort_order) = &sort_fields[0];
                if let Some(eq_conds) = query::extract_eq_conditions(&query) {
                    for comp_idx in &self.composite_indexes {
                        let fields = &comp_idx.fields;
                        let n = fields.len();
                        if n >= 2
                            && fields[n - 1] == *sort_field
                            && fields[..n - 1]
                                .iter()
                                .all(|f| eq_conds.contains_key(f.as_str()))
                        {
                            // Build prefix from equality condition values
                            let prefix: Vec<IndexValue> = fields[..n - 1]
                                .iter()
                                .map(|f| eq_conds[f.as_str()].clone())
                                .collect();

                            let need = opts.skip.unwrap_or(0) as usize
                                + opts.limit.unwrap_or(u64::MAX) as usize;

                            // Read + filter docs inline during composite index iteration.
                            let doc_cache = &self.doc_cache;
                            let mut results: Vec<Arc<Value>> = Vec::new();

                            let mut handler = |id: DocumentId| -> bool {
                                if let Some(arc) = doc_cache.get(&id) {
                                    if query::matches_value(&query, arc) {
                                        results.push(Arc::clone(arc));
                                        return results.len() < need;
                                    }
                                }
                                true
                            };

                            match sort_order {
                                SortOrder::Asc => {
                                    comp_idx.for_each_prefix_asc(&prefix, &mut handler);
                                }
                                SortOrder::Desc => {
                                    comp_idx.for_each_prefix_desc(&prefix, &mut handler);
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
            }
        }

        // Standard path: try index-accelerated lookup

        let skip_post_filter = query::is_fully_indexed(&query, &self.field_indexes);

        let early_limit: Option<usize> = if opts.sort.is_none() && opts.skip.is_none() {
            opts.limit.map(|l| l as usize)
        } else {
            None
        };

        let mut results = Vec::new();

        // Fast path: lazy index iteration for limit queries without sort/skip.
        // Avoids materializing full BTreeSet of IDs.
        if let Some(limit) = early_limit {
            let doc_cache = &self.doc_cache;
            let lazy_result = query::execute_indexed_lazy(
                &query,
                &self.field_indexes,
                &mut |id| {
                    if let Some(arc) = doc_cache.get(&id) {
                        if skip_post_filter || query::matches_value(&query, arc) {
                            results.push(Arc::clone(arc));
                            if results.len() >= limit {
                                return false;
                            }
                        }
                    }
                    true
                },
            );
            if lazy_result.is_some() {
                return Ok(results);
            }
        }

        let candidate_ids = query::execute_indexed(
            &query,
            &self.field_indexes,
            &self.composite_indexes,
        );

        if let Some(ref indexed_ids) = candidate_ids {
            for &id in indexed_ids {
                if let Some(arc) = self.read_doc_arc(id) {
                    if skip_post_filter || query::matches_value(&query, &arc) {
                        results.push(arc);
                        if let Some(limit) = early_limit {
                            if results.len() >= limit {
                                break;
                            }
                        }
                    }
                }
            }
        } else {
            self.for_each_doc_arc_while(|_id, arc| {
                if query::matches_value(&query, arc) {
                    results.push(Arc::clone(arc));
                    if let Some(limit) = early_limit {
                        if results.len() >= limit {
                            return Ok(false);
                        }
                    }
                }
                Ok(true)
            })?;
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

        let skip_post_filter = query::is_fully_indexed(&query, &self.field_indexes);

        // Try lazy index path first — avoids materializing full BTreeSet
        if !matches!(query, Query::All) {
            let mut found: Option<Value> = None;
            let doc_cache = &self.doc_cache;
            let lazy_result = query::execute_indexed_lazy(
                &query,
                &self.field_indexes,
                &mut |id| {
                    if let Some(arc) = doc_cache.get(&id) {
                        if skip_post_filter || query::matches_value(&query, arc) {
                            found = Some((**arc).clone());
                            return false;
                        }
                    }
                    true
                },
            );
            if lazy_result.is_some() {
                return Ok(found);
            }
        }

        // Fallback: full materialization path
        let candidate_ids = if !matches!(query, Query::All) {
            query::execute_indexed(
                &query,
                &self.field_indexes,
                &self.composite_indexes,
            )
        } else {
            None
        };

        if let Some(ref indexed_ids) = candidate_ids {
            for &id in indexed_ids {
                if self.primary_index.contains_key(&id) {
                    if let Some(data) = self.read_doc(id)? {
                        if skip_post_filter || query::matches_value(&query, &data) {
                            return Ok(Some(data));
                        }
                    }
                }
            }
        } else {
            // No index — iterate doc store (zero-copy: clone only the match)
            let mut found: Option<Value> = None;
            self.for_each_doc_arc_while(|_id, arc| {
                if query::matches_value(&query, arc) {
                    found = Some((**arc).clone());
                    return Ok(false);
                }
                Ok(true)
            })?;
            return Ok(found);
        }

        Ok(None)
    }

    /// Get a document by its _id directly.
    pub fn get(&self, id: DocumentId) -> Result<Option<Value>> {
        if self.primary_index.contains_key(&id) {
            self.read_doc(id)
        } else {
            Ok(None)
        }
    }

    /// Update documents matching a query atomically. Returns IDs of updated documents.
    /// If any unique constraint is violated, no documents are modified.
    /// `limit` caps the number of documents to update (e.g. `Some(1)` for update_one).
    pub fn update(&mut self, query_json: &Value, update_json: &Value, limit: Option<usize>) -> Result<Vec<DocumentId>> {
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

        // Phase 1: Find matching docs (with early termination via limit)
        let mut matches: Vec<(DocumentId, Value, DocLocation)> = Vec::new();

        // Try lazy index path first for limited updates
        let mut lazy_handled = false;
        if limit.is_some() {
            let doc_cache = &self.doc_cache;
            let primary_index = &self.primary_index;
            let skip_post_filter = query::is_fully_indexed(&query, &self.field_indexes);
            let lim = limit.unwrap();
            let lazy_result = query::execute_indexed_lazy(
                &query,
                &self.field_indexes,
                &mut |id| {
                    if let Some(arc) = doc_cache.get(&id) {
                        if skip_post_filter || query::matches_value(&query, arc) {
                            if let Some(&old_loc) = primary_index.get(&id) {
                                matches.push((id, (**arc).clone(), old_loc));
                                if matches.len() >= lim {
                                    return false;
                                }
                            }
                        }
                    }
                    true
                },
            );
            if lazy_result.is_some() {
                lazy_handled = true;
            }
        }

        if !lazy_handled {
            let candidate_ids = query::execute_indexed(
                &query,
                &self.field_indexes,
                &self.composite_indexes,
            );

            if let Some(ref indexed_ids) = candidate_ids {
                for &id in indexed_ids {
                    if let Some(&old_loc) = self.primary_index.get(&id) {
                        if let Some(data) = self.read_doc(id)? {
                            if query::matches_value(&query, &data) {
                                matches.push((id, data, old_loc));
                                if limit.is_some_and(|l| matches.len() >= l) { break; }
                            }
                        }
                    }
                }
            } else {
                // No index — iterate doc store (zero-copy: clone only matches)
                self.for_each_doc_arc_while(|id, arc| {
                    if query::matches_value(&query, arc) {
                        if let Some(&old_loc) = self.primary_index.get(&id) {
                            matches.push((id, (**arc).clone(), old_loc));
                            if limit.is_some_and(|l| matches.len() >= l) { return Ok(false); }
                        }
                    }
                    Ok(true)
                })?;
            }
        }

        if matches.is_empty() {
            return Ok(Vec::new());
        }

        // Phase 2: Prepare all updates and validate constraints upfront
        struct UpdateOp {
            id: DocumentId,
            old_loc: DocLocation,
            old_data: Value,
            new_data: Value,
            new_bytes: Vec<u8>,
        }
        let mut ops = Vec::with_capacity(matches.len());

        for (id, data, old_loc) in matches {
            let mut mutable_data = data.clone();

            crate::update::apply_update(&mut mutable_data, update_json)?;

            let old_version = mutable_data.get("_version").and_then(|v| v.as_u64()).unwrap_or(0);
            let new_version = old_version + 1;
            mutable_data.as_object_mut()
                .unwrap()
                .insert("_version".to_string(), Value::Number(new_version.into()));

            self.check_unique_constraints(&mutable_data, Some(id))?;

            let new_bytes = crate::codec::encode_doc(&mutable_data)?;
            ops.push(UpdateOp {
                id,
                old_loc,
                old_data: data,
                new_data: mutable_data,
                new_bytes,
            });
        }

        if ops.is_empty() {
            return Ok(Vec::new());
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
        let mut updated_ids = Vec::with_capacity(ops.len());
        for (op, new_loc) in ops.into_iter().zip(new_locs) {
            updated_ids.push(op.id);
            self.primary_index.insert(op.id, new_loc);
            let new_version = op.new_data.get("_version").and_then(|v| v.as_u64()).unwrap_or(1);
            self.version_index.insert(op.id, new_version);
            for idx in self.field_indexes.values_mut() {
                idx.remove_value(op.id, &op.old_data);
                idx.insert_value(op.id, &op.new_data);
            }
            for idx in &mut self.composite_indexes {
                idx.remove_value(op.id, &op.old_data);
                idx.insert_value(op.id, &op.new_data);
            }
            if let Some(ref mut text_idx) = self.text_index {
                text_idx.index_doc(op.id, &op.new_data);
            }
            for idx in self.vector_indexes.values_mut() {
                idx.remove(op.id);
                let _ = idx.insert(op.id, &op.new_data);
            }
            self.doc_cache.insert(op.id, Arc::new(op.new_data));
        }

        Ok(updated_ids)
    }

    /// Delete documents matching a query atomically. Returns IDs of deleted documents.
    /// `limit` caps the number of documents to delete (e.g. `Some(1)` for delete_one).
    pub fn delete(&mut self, query_json: &Value, limit: Option<usize>) -> Result<Vec<DocumentId>> {
        let query = query::parse_query(query_json)?;

        // Phase 1: Find matching docs (with early termination via limit)
        struct DeleteOp {
            id: DocumentId,
            loc: DocLocation,
            data: Value,
        }
        let mut ops = Vec::new();

        // Try lazy index path first for limited deletes
        let mut lazy_handled = false;
        if limit.is_some() {
            let doc_cache = &self.doc_cache;
            let primary_index = &self.primary_index;
            let skip_post_filter = query::is_fully_indexed(&query, &self.field_indexes);
            let lim = limit.unwrap();
            let lazy_result = query::execute_indexed_lazy(
                &query,
                &self.field_indexes,
                &mut |id| {
                    if let Some(arc) = doc_cache.get(&id) {
                        if skip_post_filter || query::matches_value(&query, arc) {
                            if let Some(&loc) = primary_index.get(&id) {
                                ops.push(DeleteOp { id, loc, data: (**arc).clone() });
                                if ops.len() >= lim {
                                    return false;
                                }
                            }
                        }
                    }
                    true
                },
            );
            if lazy_result.is_some() {
                lazy_handled = true;
            }
        }

        if !lazy_handled {
            let candidate_ids = query::execute_indexed(
                &query,
                &self.field_indexes,
                &self.composite_indexes,
            );

            if let Some(ref indexed_ids) = candidate_ids {
                for &id in indexed_ids {
                    if let Some(&loc) = self.primary_index.get(&id) {
                        if let Some(data) = self.read_doc(id)? {
                            if query::matches_value(&query, &data) {
                                ops.push(DeleteOp { id, loc, data });
                                if limit.is_some_and(|l| ops.len() >= l) { break; }
                            }
                        }
                    }
                }
            } else {
                // No index — iterate doc store (zero-copy: clone only matches)
                self.for_each_doc_arc_while(|id, arc| {
                    if query::matches_value(&query, arc) {
                        if let Some(&loc) = self.primary_index.get(&id) {
                            ops.push(DeleteOp { id, loc, data: (**arc).clone() });
                            if limit.is_some_and(|l| ops.len() >= l) { return Ok(false); }
                        }
                    }
                    Ok(true)
                })?;
            }
        }

        if ops.is_empty() {
            return Ok(Vec::new());
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
        let mut deleted_ids = Vec::with_capacity(ops.len());
        for op in ops {
            deleted_ids.push(op.id);
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
            for idx in self.vector_indexes.values_mut() {
                idx.remove(op.id);
            }
        }

        Ok(deleted_ids)
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
            // For large candidate sets (>50% of collection), use sequential scan
            // with BufReader (one file handle, no mutex contention) + raw JSONB
            // field extraction when records are in JSONB binary format.
            if indexed_ids.len() > self.primary_index.len() / 2 {
                return self.count_with_scan(&query);
            }
            // Small candidate set — random access via doc cache
            for &id in indexed_ids {
                if let Some(arc) = self.doc_cache.get(&id) {
                    if query::matches_value(&query, arc) {
                        count += 1;
                    }
                }
            }
        } else {
            // No index — sequential scan
            return self.count_with_scan(&query);
        }
        Ok(count)
    }

    /// Count using sequential file scan with raw JSONB field extraction.
    /// Opens a separate read-only file handle (no mutex contention with concurrent
    /// reads) and uses BufReader for efficient sequential I/O. For each record,
    /// extracts only the fields referenced by the query from raw JSONB instead of
    /// deserializing the entire document.
    fn count_with_scan(&self, query: &query::Query) -> Result<usize> {
        let mut count = 0;
        self.storage.scan_readonly_while(|bytes| {
            if let Some(matched) = query::matches_raw_jsonb(query, bytes) {
                if matched {
                    count += 1;
                }
            } else {
                // Fallback for legacy JSON text or complex value types
                let data = crate::codec::decode_doc(bytes)?;
                if query::matches_value(query, &data) {
                    count += 1;
                }
            }
            Ok(true)
        })?;
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

        for (_old_loc, bytes) in &active_records {
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
        for idx in self.vector_indexes.values_mut() {
            idx.clear();
        }
        for (&id, &loc) in &self.primary_index.clone() {
            let bytes = self.storage.read(loc)?;
            let data: Value = crate::codec::decode_doc(&bytes)?;
            let ver = data.get("_version").and_then(|v| v.as_u64()).unwrap_or(0);
            self.version_index.insert(id, ver);
            let data_arc = Arc::new(data);
            for idx in self.field_indexes.values_mut() {
                idx.insert_value(id, &data_arc);
            }
            for idx in &mut self.composite_indexes {
                idx.insert_value(id, &data_arc);
            }
            if let Some(ref mut text_idx) = self.text_index {
                text_idx.index_doc(id, &data_arc);
            }
            for idx in self.vector_indexes.values_mut() {
                let _ = idx.insert(id, &data_arc);
            }
            self.doc_cache.insert(id, data_arc);
        }

        // Save index data cache after compaction (indexes are fresh)
        self.save_index_data();

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

        let mut process_candidate = |id: DocumentId, cached: &Value, old_loc: DocLocation| -> Result<()> {
            if !query::matches_value(&query, cached) {
                return Ok(());
            }
            let old_data = cached.clone();
            let mut data = cached.clone();

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
                    if let Some(data) = self.read_doc(id)? {
                        process_candidate(id, &data, old_loc)?;
                    }
                }
            }
        } else {
            // Collect snapshot from doc store then process
            let mut snapshot: Vec<(DocumentId, Value, DocLocation)> = Vec::new();
            self.for_each_doc(|id, data| {
                if let Some(&loc) = self.primary_index.get(&id) {
                    snapshot.push((id, data, loc));
                }
                Ok(())
            })?;
            for (id, data, old_loc) in &snapshot {
                process_candidate(*id, data, *old_loc)?;
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

        let mut process_candidate = |id: DocumentId, cached: &Value, loc: DocLocation| -> Result<()> {
            if !query::matches_value(&query, cached) {
                return Ok(());
            }
            mutations.push(PreparedMutation {
                wal_entry: WalEntry::Delete { doc_id: id, tx_id },
                doc_id: id,
                new_bytes: vec![],
                old_loc: Some(loc),
                old_data: Some(cached.clone()),
                new_data: Value::Null,
                is_delete: true,
            });
            Ok(())
        };

        if let Some(ref indexed_ids) = candidate_ids {
            for &id in indexed_ids {
                if let Some(&loc) = self.primary_index.get(&id) {
                    if let Some(data) = self.read_doc(id)? {
                        process_candidate(id, &data, loc)?;
                    }
                }
            }
        } else {
            // Collect snapshot from doc store then process
            let mut snapshot: Vec<(DocumentId, Value, DocLocation)> = Vec::new();
            self.for_each_doc(|id, data| {
                if let Some(&loc) = self.primary_index.get(&id) {
                    snapshot.push((id, data, loc));
                }
                Ok(())
            })?;
            for (id, data, loc) in &snapshot {
                process_candidate(*id, data, *loc)?;
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

        // Update in-memory indexes, doc_cache, and doc store
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
                for idx in self.vector_indexes.values_mut() {
                    idx.remove(m.doc_id);
                }
            } else if let Some(loc) = new_locs[i] {
                self.primary_index.insert(m.doc_id, loc);
                let ver = m.new_data.get("_version").and_then(|v| v.as_u64()).unwrap_or(1);
                self.version_index.insert(m.doc_id, ver);
                if let Some(ref old_data) = m.old_data {
                    for idx in self.field_indexes.values_mut() {
                        idx.remove_value(m.doc_id, old_data);
                    }
                    for idx in &mut self.composite_indexes {
                        idx.remove_value(m.doc_id, old_data);
                    }
                }
                for idx in self.field_indexes.values_mut() {
                    idx.insert_value(m.doc_id, &m.new_data);
                }
                for idx in &mut self.composite_indexes {
                    idx.insert_value(m.doc_id, &m.new_data);
                }
                if let Some(ref mut text_idx) = self.text_index {
                    text_idx.index_doc(m.doc_id, &m.new_data);
                }
                for idx in self.vector_indexes.values_mut() {
                    idx.remove(m.doc_id);
                    let _ = idx.insert(m.doc_id, &m.new_data);
                }
                self.doc_cache.insert(m.doc_id, Arc::new(m.new_data.clone()));
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

    fn temp_collection(name: &str) -> (tempfile::TempDir, Collection) {
        let dir = tempdir().unwrap();
        let col = Collection::open(name, dir.path()).unwrap();
        (dir, col)
    }

    #[test]
    fn insert_and_get() {
        let (_dir, mut col) = temp_collection("test");
        let id = col.insert(json!({"name": "Alice", "age": 30})).unwrap();
        let doc = col.get(id).unwrap().unwrap();
        assert_eq!(doc["name"], "Alice");
        assert_eq!(doc["_id"], id);
    }

    #[test]
    fn insert_assigns_version_1() {
        let (_dir, mut col) = temp_collection("test");
        let id = col.insert(json!({"name": "Alice"})).unwrap();
        let doc = col.get(id).unwrap().unwrap();
        assert_eq!(doc["_version"], 1);
        assert_eq!(col.get_version(id), 1);
    }

    #[test]
    fn update_increments_version() {
        let (_dir, mut col) = temp_collection("test");
        let id = col.insert(json!({"name": "Alice"})).unwrap();
        assert_eq!(col.get_version(id), 1);
        col.update(&json!({"_id": id}), &json!({"$set": {"name": "Bob"}}), None).unwrap();
        let doc = col.get(id).unwrap().unwrap();
        assert_eq!(doc["_version"], 2);
        assert_eq!(col.get_version(id), 2);
    }

    #[test]
    fn find_with_index() {
        let (_dir, mut col) = temp_collection("test");
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
        let (_dir, mut col) = temp_collection("test");
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
        let (_dir, mut col) = temp_collection("test");
        let id = col.insert(json!({"name": "Alice", "age": 30})).unwrap();

        let ids = col
            .update(&json!({"name": "Alice"}), &json!({"$set": {"age": 31}}), None)
            .unwrap();
        assert_eq!(ids.len(), 1);

        let doc = col.get(id).unwrap().unwrap();
        assert_eq!(doc["age"], 31);
    }

    #[test]
    fn delete_doc() {
        let (_dir, mut col) = temp_collection("test");
        col.insert(json!({"name": "Alice"})).unwrap();
        col.insert(json!({"name": "Bob"})).unwrap();

        let ids = col.delete(&json!({"name": "Alice"}), None).unwrap();
        assert_eq!(ids.len(), 1);
        assert_eq!(col.count(), 1);
    }

    #[test]
    fn unique_index_enforced() {
        let (_dir, mut col) = temp_collection("test");
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
        let (_dir, mut col) = temp_collection("test");
        col.create_unique_index("email").unwrap();
        col.insert(json!({"email": "alice@test.com"})).unwrap();
        col.insert(json!({"email": "bob@test.com"})).unwrap();
        assert_eq!(col.count(), 2);
    }

    #[test]
    fn unique_index_update_same_doc_ok() {
        let (_dir, mut col) = temp_collection("test");
        col.create_unique_index("email").unwrap();
        col.insert(json!({"email": "alice@test.com", "name": "Alice"}))
            .unwrap();

        // Updating other fields on same doc should work (email unchanged)
        let ids = col
            .update(
                &json!({"email": "alice@test.com"}),
                &json!({"$set": {"name": "Alicia"}}),
                None,
            )
            .unwrap();
        assert_eq!(ids.len(), 1);
    }

    #[test]
    fn unique_index_update_conflict() {
        let (_dir, mut col) = temp_collection("test");
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
        let (_dir, mut col) = temp_collection("test");
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
        let (_dir, mut col) = temp_collection("test");
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
        let (_dir, mut col) = temp_collection("test");
        col.insert(json!({"status": "draft", "title": "A"}))
            .unwrap();
        col.insert(json!({"status": "draft", "title": "B"}))
            .unwrap();

        let ids = col
            .update(
                &json!({"status": "draft"}),
                &json!({"$set": {"status": "published"}}),
                None,
            )
            .unwrap();
        assert_eq!(ids.len(), 2);

        let published = col.find(&json!({"status": "published"})).unwrap();
        assert_eq!(published.len(), 2);
        let drafts = col.find(&json!({"status": "draft"})).unwrap();
        assert_eq!(drafts.len(), 0);
    }

    #[test]
    fn atomic_multi_doc_delete() {
        let (_dir, mut col) = temp_collection("test");
        col.insert(json!({"status": "old", "title": "A"}))
            .unwrap();
        col.insert(json!({"status": "old", "title": "B"}))
            .unwrap();
        col.insert(json!({"status": "new", "title": "C"}))
            .unwrap();

        let ids = col.delete(&json!({"status": "old"}), None).unwrap();
        assert_eq!(ids.len(), 2);
        assert_eq!(col.count(), 1);
    }

    // -----------------------------------------------------------------------
    // Sort / Skip / Limit tests
    // -----------------------------------------------------------------------

    #[test]
    fn sort_ascending() {
        let (_dir, mut col) = temp_collection("test");
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
        let (_dir, mut col) = temp_collection("test");
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
        let (_dir, mut col) = temp_collection("test");
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
        let (_dir, mut col) = temp_collection("test");
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
        let (_dir, mut col) = temp_collection("test");
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
        let (_dir, mut col) = temp_collection("test");
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

    #[test]
    fn composite_index_backed_sort_desc() {
        let (_dir, mut col) = temp_collection("comp_sort");
        col.create_index("formId").unwrap();
        col.create_composite_index(vec!["formId".into(), "createdAt".into()])
            .unwrap();

        // Insert docs with different formIds and createdAt dates
        col.insert(json!({"formId": "1", "createdAt": "2024-01-01T00:00:00Z", "name": "a"})).unwrap();
        col.insert(json!({"formId": "1", "createdAt": "2024-06-01T00:00:00Z", "name": "b"})).unwrap();
        col.insert(json!({"formId": "1", "createdAt": "2025-01-01T00:00:00Z", "name": "c"})).unwrap();
        col.insert(json!({"formId": "2", "createdAt": "2024-03-01T00:00:00Z", "name": "d"})).unwrap();
        col.insert(json!({"formId": "1", "createdAt": "2024-03-01T00:00:00Z", "name": "e"})).unwrap();

        // Sort DESC by createdAt for formId=1, limit 2
        let opts = FindOptions {
            sort: Some(vec![("createdAt".into(), SortOrder::Desc)]),
            skip: None,
            limit: Some(2),
        };
        let results = col.find_with_options(&json!({"formId": "1"}), &opts).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0]["name"], "c"); // 2025-01-01 (newest)
        assert_eq!(results[1]["name"], "b"); // 2024-06-01

        // Sort ASC by createdAt for formId=1, limit 2
        let opts_asc = FindOptions {
            sort: Some(vec![("createdAt".into(), SortOrder::Asc)]),
            skip: None,
            limit: Some(2),
        };
        let results_asc = col.find_with_options(&json!({"formId": "1"}), &opts_asc).unwrap();
        assert_eq!(results_asc.len(), 2);
        assert_eq!(results_asc[0]["name"], "a"); // 2024-01-01 (oldest)
        assert_eq!(results_asc[1]["name"], "e"); // 2024-03-01

        // Skip + limit on DESC
        let opts_skip = FindOptions {
            sort: Some(vec![("createdAt".into(), SortOrder::Desc)]),
            skip: Some(1),
            limit: Some(2),
        };
        let results_skip = col.find_with_options(&json!({"formId": "1"}), &opts_skip).unwrap();
        assert_eq!(results_skip.len(), 2);
        assert_eq!(results_skip[0]["name"], "b"); // skipped "c", so start from "b"
        assert_eq!(results_skip[1]["name"], "e"); // 2024-03-01

        // formId=2 should only return its own doc
        let results_f2 = col.find_with_options(&json!({"formId": "2"}), &opts).unwrap();
        assert_eq!(results_f2.len(), 1);
        assert_eq!(results_f2[0]["name"], "d");
    }

    #[test]
    fn composite_index_backed_sort_asc() {
        let (_dir, mut col) = temp_collection("comp_sort_asc");
        col.create_composite_index(vec!["status".into(), "score".into()])
            .unwrap();

        col.insert(json!({"status": "active", "score": 50, "name": "mid"})).unwrap();
        col.insert(json!({"status": "active", "score": 10, "name": "low"})).unwrap();
        col.insert(json!({"status": "active", "score": 90, "name": "high"})).unwrap();
        col.insert(json!({"status": "closed", "score": 30, "name": "other"})).unwrap();

        let opts = FindOptions {
            sort: Some(vec![("score".into(), SortOrder::Asc)]),
            skip: None,
            limit: Some(2),
        };
        let results = col.find_with_options(&json!({"status": "active"}), &opts).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0]["name"], "low");  // score 10
        assert_eq!(results[1]["name"], "mid");  // score 50
    }

    #[test]
    fn composite_index_sort_with_post_filter() {
        // Test that composite index sort works with extra query conditions
        // beyond the composite prefix fields (post-filtering).
        let (_dir, mut col) = temp_collection("comp_sort_postfilter");
        col.create_composite_index(vec!["formId".into(), "createdAt".into()])
            .unwrap();

        col.insert(json!({"formId": "1", "createdAt": "2024-01-01", "data": {"level": "Junior"}})).unwrap();
        col.insert(json!({"formId": "1", "createdAt": "2024-01-02", "data": {"level": "Senior"}})).unwrap();
        col.insert(json!({"formId": "1", "createdAt": "2024-01-03", "data": {"level": "Junior"}})).unwrap();
        col.insert(json!({"formId": "1", "createdAt": "2024-01-04", "data": {"level": "Senior"}})).unwrap();
        col.insert(json!({"formId": "1", "createdAt": "2024-01-05", "data": {"level": "Junior"}})).unwrap();
        col.insert(json!({"formId": "2", "createdAt": "2024-01-06", "data": {"level": "Junior"}})).unwrap();

        // Query: formId="1" AND data.level="Junior", sort by createdAt DESC, limit 2
        let opts = FindOptions {
            sort: Some(vec![("createdAt".into(), SortOrder::Desc)]),
            skip: None,
            limit: Some(2),
        };
        let query = json!({"$and": [{"formId": "1"}, {"data.level": "Junior"}]});
        let results = col.find_with_options(&query, &opts).unwrap();
        assert_eq!(results.len(), 2);
        // Should get the two most recent Junior entries from formId "1"
        assert_eq!(results[0]["createdAt"], "2024-01-05");
        assert_eq!(results[1]["createdAt"], "2024-01-03");
    }
}
