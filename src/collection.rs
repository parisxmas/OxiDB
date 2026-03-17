use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde_json::Value;

use crate::crypto::EncryptionKey;
use crate::doc_cache::{self, DocCache};
use crate::document::DocumentId;
use crate::engine::LogCallback;
use crate::error::{Error, Result};
use crate::fts::CollectionTextIndex;
use crate::in_memory::{InMemStorage, StorageBackend, WalBackend};
use crate::index::{CompositeIndex, FieldIndex};
use crate::index_persist;
use crate::vector::{DistanceMetric, VectorIndex};
use crate::query::{self, FindOptions, Query, SortOrder};
use crate::storage::{DocLocation, Storage};
use crate::value::IndexValue;
use crate::wal::{Wal, WalEntry};

/// Resolve a field path (with dot notation) directly on a &Value.
pub fn resolve_field_in_value<'a>(data: &'a Value, path: &str) -> Option<&'a Value> {
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
    storage: StorageBackend,
    wal: WalBackend,
    primary_index: HashMap<DocumentId, DocLocation>,
    doc_cache: DocCache,
    field_indexes: HashMap<String, FieldIndex>,
    composite_indexes: Vec<CompositeIndex>,
    text_index: Option<CollectionTextIndex>,
    vector_indexes: HashMap<String, VectorIndex>,
    version_index: HashMap<DocumentId, u64>,
    next_id: DocumentId,
    encryption: Option<Arc<EncryptionKey>>,
    verbose: bool,
    log_callback: Option<LogCallback>,
    /// When true, write operations skip per-operation fsync.
    /// A background thread periodically calls `sync_writes()` to flush to disk.
    /// This matches MongoDB's default durability (journal flushed every ~10ms).
    lazy_sync: bool,
    /// When true, this collection is in-memory only (no disk I/O).
    in_memory: bool,
    /// TTL index: maps expiry timestamp (ms since epoch) to document IDs.
    ttl_index: std::collections::BTreeMap<u64, Vec<DocumentId>>,
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
        if self.in_memory {
            return Ok(());
        }
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
        if self.in_memory {
            return;
        }
        let doc_count = self.primary_index.len() as u64;
        let next_id = self.next_id;

        // Save field indexes (.fidx)
        let enc = self.encryption.as_ref();
        let fidx_path = self.data_dir.join(format!("{}.fidx", self.name));
        let field_refs: Vec<&FieldIndex> = self.field_indexes.values().collect();
        if let Err(e) = index_persist::save_field_indexes(&fidx_path, &field_refs, doc_count, next_id, enc) {
            eprintln!("[warn] {}: failed to save field index cache: {}", self.name, e);
        }

        // Save composite indexes (.cidx)
        let cidx_path = self.data_dir.join(format!("{}.cidx", self.name));
        let comp_refs: Vec<&CompositeIndex> = self.composite_indexes.iter().collect();
        if let Err(e) = index_persist::save_composite_indexes(&cidx_path, &comp_refs, doc_count, next_id, enc) {
            eprintln!("[warn] {}: failed to save composite index cache: {}", self.name, e);
        }

        // Save vector indexes (.vidx)
        let vidx_path = self.data_dir.join(format!("{}.vidx", self.name));
        let vec_refs: Vec<&VectorIndex> = self.vector_indexes.values().collect();
        if let Err(e) = index_persist::save_vector_indexes(&vidx_path, &vec_refs, doc_count, next_id, enc) {
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
        let storage = StorageBackend::File(Storage::open_with_encryption(&data_path, encryption.clone())?);
        let wal = WalBackend::File(Wal::open_with_encryption(&wal_path, encryption.clone())?);

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
        let doc_cache = DocCache::new(doc_cache::DEFAULT_CAPACITY);
        let mut version_index = HashMap::new();
        let shard_id_offset = Self::shard_id_offset();
        let mut next_id: DocumentId = 1 + shard_id_offset;

        let load_start = std::time::Instant::now();
        let mut doc_count: u64 = 0;

        // Clone callback for use inside the closure
        let inner_cb = log_callback.clone();

        // Phase 1: Scan .dat for primary_index, version_index, next_id.
        // Also rebuild text index (always from docs — not cached).
        // Field/composite indexes are NOT rebuilt here; we try the cache first.
        // Documents are NOT cached — the LRU cache is populated on demand.
        storage.for_each_active(|loc, bytes| {
            let doc: Value = crate::codec::decode_doc(&bytes)?;
            if let Some(id) = doc.get("_id").and_then(|v| v.as_u64()) {
                primary_index.insert(id, loc);
                let ver = doc.get("_version").and_then(|v| v.as_u64()).unwrap_or(0);
                version_index.insert(id, ver);
                if id >= next_id {
                    next_id = id + 1;
                }

                // Text index is always rebuilt from docs (not cached)
                if let Some(ref mut ti) = text_index {
                    let doc_arc = Arc::new(doc);
                    ti.index_doc(id, &doc_arc);
                }
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

            let enc = encryption.as_ref();
            let cached_field = index_persist::load_field_indexes(
                &fidx_path,
                doc_count,
                next_id,
                enc,
            );
            let cached_composite = index_persist::load_composite_indexes(
                &cidx_path,
                doc_count,
                next_id,
                enc,
            );
            let cached_vector = index_persist::load_vector_indexes(
                &vidx_path,
                doc_count,
                next_id,
                enc,
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

        // Phase 2b: If cache was invalid, rebuild indexes by scanning storage
        if has_persisted_indexes && !indexes_from_cache {
            if verbose {
                vlog(&format!(
                    "[verbose] {}: index cache invalid, rebuilding {} indexes from storage...",
                    name,
                    persisted_indexes.len(),
                ));
            }
            let rebuild_start = std::time::Instant::now();
            let mut rebuild_count = 0u64;

            // Use sequential streaming scan instead of per-doc random reads
            storage.scan_readonly_while(|bytes| {
                let doc: Value = crate::codec::decode_doc(bytes)?;
                if let Some(id) = doc.get("_id").and_then(|v| v.as_u64()) {
                    let doc_arc = Arc::new(doc);
                    for idx in field_indexes.values_mut() {
                        idx.insert_value(id, &doc_arc);
                    }
                    for idx in &mut composite_indexes {
                        idx.insert_value(id, &doc_arc);
                    }
                    for idx in vector_indexes.values_mut() {
                        let _ = idx.insert(id, &doc_arc);
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
                        eprintln!("{msg}");
                        if let Some(cb) = &log_callback {
                            cb(&msg);
                        }
                    }
                }
                Ok(true)
            })?;

            if verbose {
                vlog(&format!(
                    "[verbose] {}: rebuilt {} indexes in {:.2}s",
                    name,
                    persisted_indexes.len(),
                    rebuild_start.elapsed().as_secs_f64(),
                ));
            }
        }

        // Phase 3: WAL recovery (updates indexes and LRU cache too)
        wal.recover(
            &storage,
            &mut primary_index,
            &doc_cache,
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
            lazy_sync: false,
            in_memory: false,
            ttl_index: std::collections::BTreeMap::new(),
        };

        // Save index cache after rebuild so next restart loads from cache
        if has_persisted_indexes && !indexes_from_cache {
            collection.save_index_data();
        }

        Ok(collection)
    }

    /// Create a new in-memory collection (no disk I/O).
    pub fn open_in_memory(name: &str) -> Self {
        Self {
            name: name.to_string(),
            data_dir: PathBuf::new(),
            storage: StorageBackend::Memory(InMemStorage::new()),
            wal: WalBackend::Memory,
            primary_index: HashMap::new(),
            doc_cache: DocCache::new(doc_cache::DEFAULT_CAPACITY),
            field_indexes: HashMap::new(),
            composite_indexes: Vec::new(),
            text_index: None,
            vector_indexes: HashMap::new(),
            version_index: HashMap::new(),
            next_id: 1 + Self::shard_id_offset(),
            encryption: None,
            verbose: false,
            log_callback: None,
            lazy_sync: false,
            in_memory: true,
            ttl_index: std::collections::BTreeMap::new(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the ID offset for this shard based on the `OXIDB_SHARD_ID` env var.
    /// Each shard gets a 2^48 range within u64, allowing up to 65535 shards.
    /// Shard 0 (or unset): offset 0, Shard 1: offset 2^48, Shard 2: offset 2*2^48, etc.
    fn shard_id_offset() -> DocumentId {
        match std::env::var("OXIDB_SHARD_ID") {
            Ok(val) => {
                let shard_id: u64 = val.parse().unwrap_or(0);
                shard_id * (1u64 << 48)
            }
            Err(_) => 0,
        }
    }

    /// Enable or disable lazy sync mode.
    /// When enabled, write operations skip per-operation fsync for higher throughput.
    pub fn set_lazy_sync(&mut self, enabled: bool) {
        self.lazy_sync = enabled;
    }

    /// Set the LRU document cache capacity. Excess entries are evicted immediately.
    pub fn set_cache_capacity(&self, capacity: usize) {
        self.doc_cache.resize(capacity);
    }

    /// Current LRU cache capacity.
    pub fn cache_capacity(&self) -> usize {
        self.doc_cache.capacity()
    }

    /// Flush pending writes to disk: sync storage file, then checkpoint WAL.
    /// Called periodically by the engine's background sync thread.
    pub fn sync_writes(&self) -> Result<()> {
        self.storage.sync()?;
        self.wal.checkpoint_no_sync()?;
        Ok(())
    }

    /// Access the field indexes for index-accelerated aggregation.
    pub fn field_indexes(&self) -> &HashMap<String, FieldIndex> {
        &self.field_indexes
    }

    /// Access the composite indexes.
    pub fn composite_indexes(&self) -> &[CompositeIndex] {
        &self.composite_indexes
    }

    /// Check if a text index exists.
    pub fn has_text_index(&self) -> bool {
        self.text_index.is_some()
    }

    /// Access the vector indexes.
    pub fn vector_indexes(&self) -> &HashMap<String, VectorIndex> {
        &self.vector_indexes
    }

    /// Look up a document by ID. Checks LRU cache first, falls back to storage.
    pub fn load_doc_arc(&self, id: DocumentId) -> Option<Arc<Value>> {
        // Fast path: LRU cache hit
        if let Some(arc) = self.doc_cache.get(id) {
            return Some(arc);
        }
        // Slow path: lockfree pread from storage, decode, populate cache
        let loc = self.primary_index.get(&id)?;
        let bytes = self.storage.read_lockfree(*loc).ok()?;
        let doc = crate::codec::decode_doc(&bytes).ok()?;
        let arc = Arc::new(doc);
        self.doc_cache.put(id, Arc::clone(&arc));
        Some(arc)
    }

    /// Read a document by its ID (cloned Value).
    fn read_doc(&self, id: DocumentId) -> Result<Option<Value>> {
        Ok(self.load_doc_arc(id).map(|arc| (*arc).clone()))
    }

    /// Read a document by its ID, returning an Arc.
    fn read_doc_arc(&self, id: DocumentId) -> Option<Arc<Value>> {
        self.load_doc_arc(id)
    }

    /// Iterate all documents, calling `f` for each one.
    /// Reads from storage sequentially for efficiency.
    fn for_each_doc<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(DocumentId, Value) -> Result<()>,
    {
        for (&id, &loc) in &self.primary_index {
            let bytes = self.storage.read(loc)?;
            let doc = crate::codec::decode_doc(&bytes)?;
            f(id, doc)?;
        }
        Ok(())
    }

    /// Iterate all documents as Arc references.
    /// Stops early when `f` returns `Ok(false)`.
    /// Uses sequential storage scan for efficiency.
    fn for_each_doc_arc_while<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(DocumentId, &Arc<Value>) -> Result<bool>,
    {
        for (&id, &loc) in &self.primary_index {
            let bytes = self.storage.read(loc)?;
            let doc = crate::codec::decode_doc(&bytes)?;
            let arc = Arc::new(doc);
            self.doc_cache.put(id, Arc::clone(&arc));
            if !f(id, &arc)? {
                break;
            }
        }
        Ok(())
    }

    /// Stream all documents sequentially using a BufReader-backed scan.
    /// Much faster than `for_each_doc_arc_while` for full-collection scans
    /// because it avoids per-doc Mutex acquisitions and random seeks.
    /// Does NOT populate the LRU cache (would thrash for large scans).
    /// Callback receives decoded `Value`; returns `Ok(true)` to continue.
    fn for_each_doc_streaming<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&Value) -> Result<bool>,
    {
        self.storage.scan_readonly_while(|bytes| {
            let doc: Value = crate::codec::decode_doc(bytes)?;
            f(&doc)
        })
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
        let name = self.name.clone();
        let verbose = self.verbose;

        // Backfill from storage using sequential streaming scan.
        // Zero-decode path: extract only _id + indexed field from raw JSONB.
        let field_owned = field.to_string();
        self.storage.scan_readonly_while(|bytes| {
            if !bytes.is_empty() && bytes[0] != b'{' && bytes[0] != b'[' {
                // JSONB binary — extract only the two fields we need
                let raw = jsonb::RawJsonb::new(bytes);
                if let Some(id) = extract_raw_u64(&raw, "_id") {
                    if let Some(iv) = extract_raw_index_value(&raw, &field_owned) {
                        idx.insert_raw(id, iv);
                    }
                    count += 1;
                }
            } else {
                // Legacy JSON text — full decode fallback
                let doc: Value = crate::codec::decode_doc(bytes)?;
                if let Some(id) = doc.get("_id").and_then(|v| v.as_u64()) {
                    let arc = Arc::new(doc);
                    idx.insert_value(id, &arc);
                    count += 1;
                }
            }
            if verbose && count % 500_000 == 0 {
                eprintln!(
                    "[verbose] {}: index '{}' scanned {} / {} docs ({:.1}s)",
                    name, field, count, total, start.elapsed().as_secs_f64()
                );
            }
            Ok(true)
        })?;

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
        let name = self.name.clone();
        let verbose = self.verbose;
        let mut unique_err: Option<Error> = None;

        // Backfill from storage using sequential streaming scan.
        // Zero-decode path: extract only _id + indexed field from raw JSONB.
        self.storage.scan_readonly_while(|bytes| {
            if !bytes.is_empty() && bytes[0] != b'{' && bytes[0] != b'[' {
                // JSONB binary — extract only the two fields we need
                let raw = jsonb::RawJsonb::new(bytes);
                if let Some(id) = extract_raw_u64(&raw, "_id") {
                    if let Some(iv) = extract_raw_index_value(&raw, &field_owned) {
                        if idx.check_unique(&iv, None) {
                            unique_err = Some(Error::UniqueViolation {
                                field: field_owned.clone(),
                            });
                            return Ok(false);
                        }
                        idx.insert_raw(id, iv);
                    }
                    count += 1;
                }
            } else {
                // Legacy JSON text — full decode fallback
                let doc: Value = crate::codec::decode_doc(bytes)?;
                if let Some(id) = doc.get("_id").and_then(|v| v.as_u64()) {
                    let arc = Arc::new(doc);
                    if let Some(value) = resolve_field_in_value(&arc, &field_owned) {
                        let iv = IndexValue::from_json(value);
                        if idx.check_unique(&iv, None) {
                            unique_err = Some(Error::UniqueViolation {
                                field: field_owned.clone(),
                            });
                            return Ok(false);
                        }
                    }
                    idx.insert_value(id, &arc);
                    count += 1;
                }
            }
            if verbose && count % 500_000 == 0 {
                eprintln!(
                    "[verbose] {}: unique index '{}' scanned {} / {} docs ({:.1}s)",
                    name, field_owned, count, total, start.elapsed().as_secs_f64()
                );
            }
            Ok(true)
        })?;

        if let Some(err) = unique_err {
            return Err(err);
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
        let col_name = self.name.clone();
        let verbose = self.verbose;

        // Backfill from storage using sequential streaming scan
        self.storage.scan_readonly_while(|bytes| {
            let doc: Value = crate::codec::decode_doc(bytes)?;
            if let Some(id) = doc.get("_id").and_then(|v| v.as_u64()) {
                let arc = Arc::new(doc);
                idx.insert_value(id, &arc);
                count += 1;
                if verbose && count % 500_000 == 0 {
                    eprintln!(
                        "[verbose] {}: composite index '{}' scanned {} / {} docs ({:.1}s)",
                        col_name, name, count, total, start.elapsed().as_secs_f64()
                    );
                }
            }
            Ok(true)
        })?;

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
        let name = self.name.clone();
        let verbose = self.verbose;

        // Backfill from storage using sequential streaming scan
        self.storage.scan_readonly_while(|bytes| {
            let doc: Value = crate::codec::decode_doc(bytes)?;
            if let Some(id) = doc.get("_id").and_then(|v| v.as_u64()) {
                let arc = Arc::new(doc);
                idx.index_doc(id, &arc);
                count += 1;
                if verbose && count % 500_000 == 0 {
                    eprintln!(
                        "[verbose] {}: text index scanned {} / {} docs ({:.1}s)",
                        name, count, total, start.elapsed().as_secs_f64()
                    );
                }
            }
            Ok(true)
        })?;

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
        let name = self.name.clone();
        let verbose = self.verbose;
        let field_owned = field.to_string();

        // Backfill from storage using sequential streaming scan
        self.storage.scan_readonly_while(|bytes| {
            let doc: Value = crate::codec::decode_doc(bytes)?;
            if let Some(id) = doc.get("_id").and_then(|v| v.as_u64()) {
                let arc = Arc::new(doc);
                if let Err(e) = idx.insert(id, &arc) {
                    if verbose {
                        eprintln!(
                            "[verbose] {}: vector index skip doc {}: {}",
                            name, id, e
                        );
                    }
                }
                count += 1;
                if verbose && count % 500_000 == 0 {
                    eprintln!(
                        "[verbose] {}: vector index '{}' scanned {} / {} docs ({:.1}s)",
                        name, field_owned, count, total, start.elapsed().as_secs_f64()
                    );
                }
            }
            Ok(true)
        })?;

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
    pub fn check_unique_constraints(
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

        // WAL: log before mutating .dat
        self.wal.log_no_sync(&WalEntry::insert(id, bytes.clone()))?;

        let loc = if self.lazy_sync {
            // Deferred sync: skip fsync, background thread will flush
            self.storage.append_no_sync(&bytes)?
        } else {
            self.storage.append(&bytes)?
        };

        if !self.lazy_sync {
            // Eager checkpoint when fsync already happened
            self.wal.checkpoint_no_sync()?;
        }

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

        // TTL: if the document has a _ttl field (seconds), register expiry
        self.register_ttl(id, &data_arc);

        self.doc_cache.put(id, data_arc);

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

        // Phase 2: WAL log all entries (skip entirely when lazy_sync — WAL provides
        // no additional crash safety when neither WAL nor storage is fsynced)
        if !self.lazy_sync {
            let wal_refs: Vec<(u64, &[u8])> = prepared
                .iter()
                .map(|(id, _, bytes)| (*id, bytes.as_slice()))
                .collect();
            self.wal.log_batch_inserts_no_sync_buffered(&wal_refs)?;
        }

        // Phase 3: append all to .dat (single write_all via buffered method)
        let byte_slices: Vec<&[u8]> = prepared.iter().map(|(_, _, bytes)| bytes.as_slice()).collect();
        let batch_locs = self.storage.append_batch_no_sync_buffered(&byte_slices)?;
        if !self.lazy_sync {
            self.storage.sync()?;
        }

        let mut ids = Vec::with_capacity(prepared.len());
        let mut locs = Vec::with_capacity(prepared.len());
        for ((id, _, _), loc) in prepared.iter().zip(batch_locs) {
            ids.push(*id);
            locs.push((*id, loc));
        }

        if !self.lazy_sync {
            self.wal.checkpoint_no_sync()?;
        }

        // Phase 5: update in-memory indexes
        self.next_id += prepared.len() as u64;

        // Skip cache population for large batches — bulk inserts thrash
        // the LRU cache (1M inserts into 100K cache = 900K wasted evictions
        // that fragment the allocator). Cache is populated lazily on first read.
        let skip_cache = prepared.len() > 1000;
        let has_indexes = !self.field_indexes.is_empty()
            || !self.composite_indexes.is_empty()
            || self.text_index.is_some()
            || !self.vector_indexes.is_empty();

        for ((id, data, _bytes), (_, loc)) in prepared.into_iter().zip(locs.iter()) {
            self.primary_index.insert(id, *loc);
            self.version_index.insert(id, 1);
            if has_indexes || !skip_cache {
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
                if !skip_cache {
                    self.doc_cache.put(id, data_arc);
                }
            }
        }

        Ok(ids)
    }

    /// Reserve a contiguous block of document IDs. Returns the first ID in the range.
    /// Caller gets IDs `first_id .. first_id + count`.
    pub fn reserve_ids(&mut self, count: u64) -> DocumentId {
        let first = self.next_id;
        self.next_id += count;
        first
    }

    /// Insert pre-serialized documents. Each tuple is (doc_id, parsed_value, encoded_bytes).
    /// IDs must have been reserved via `reserve_ids`. Unique constraints must have been
    /// checked by the caller (the engine layer handles this).
    pub fn insert_many_prepared(
        &mut self,
        prepared: Vec<(DocumentId, Value, Vec<u8>)>,
    ) -> Result<Vec<DocumentId>> {
        if prepared.is_empty() {
            return Ok(vec![]);
        }

        // WAL (skip when lazy_sync)
        if !self.lazy_sync {
            let wal_refs: Vec<(u64, &[u8])> = prepared
                .iter()
                .map(|(id, _, bytes)| (*id, bytes.as_slice()))
                .collect();
            self.wal.log_batch_inserts_no_sync_buffered(&wal_refs)?;
        }

        // Append to storage (single write_all)
        let byte_slices: Vec<&[u8]> = prepared.iter().map(|(_, _, bytes)| bytes.as_slice()).collect();
        let batch_locs = self.storage.append_batch_no_sync_buffered(&byte_slices)?;
        if !self.lazy_sync {
            self.storage.sync()?;
            self.wal.checkpoint_no_sync()?;
        }

        // Update in-memory indexes
        let mut ids = Vec::with_capacity(prepared.len());
        for ((id, data, _bytes), loc) in prepared.into_iter().zip(batch_locs) {
            self.primary_index.insert(id, loc);
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
            self.doc_cache.put(id, data_arc);
            ids.push(id);
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

        // Fast path: Query::All with no sort — use streaming sequential scan.
        if matches!(query, Query::All) && opts.sort.is_none() {
            let skip = opts.skip.unwrap_or(0) as usize;
            let limit = opts.limit.map(|l| l as usize).unwrap_or(usize::MAX);
            let mut results = Vec::new();
            let mut skipped = 0;
            self.for_each_doc_streaming(|doc| {
                if skipped < skip {
                    skipped += 1;
                    return Ok(true);
                }
                if results.len() >= limit {
                    return Ok(false);
                }
                results.push(Arc::new(doc.clone()));
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
                    // In index-backed sort we iterate the SORT field's index,
                    // NOT the query fields' indexes — so we can only skip filtering
                    // when there is no query filter at all.
                    let skip_filter = matches!(query, Query::All);

                    match sort_order {
                        SortOrder::Asc => {
                            'outer_asc: for (_value, doc_ids) in field_idx.iter_asc() {
                                for &id in doc_ids {
                                    if let Some(arc) = self.read_doc_arc(id) {
                                        if skip_filter || query::matches_value(&query, &arc) {
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
                                        if skip_filter || query::matches_value(&query, &arc) {
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
                            let mut results: Vec<Arc<Value>> = Vec::new();

                            let mut handler = |id: DocumentId| -> bool {
                                if let Some(arc) = self.load_doc_arc(id) {
                                    if query::matches_value(&query, &arc) {
                                        results.push(arc);
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
            let lazy_result = query::execute_indexed_lazy(
                &query,
                &self.field_indexes,
                &mut |id| {
                    if let Some(arc) = self.load_doc_arc(id) {
                        if skip_post_filter || query::matches_value(&query, &arc) {
                            results.push(arc);
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
            const BATCH_THRESHOLD: usize = 1024;

            if indexed_ids.len() >= BATCH_THRESHOLD && early_limit.is_none() {
                // Batch path: probe cache once, batch-read misses with sorted
                // offsets for I/O locality, then batch-populate cache.
                let ids: Vec<u64> = indexed_ids.iter().copied().collect();

                // Phase 1: single-lock cache probe
                let mut all_docs = self.doc_cache.get_many(&ids);

                // Phase 2: collect cache misses with storage locations
                let mut miss_locs: Vec<(usize, crate::storage::DocLocation)> = Vec::new();
                for (i, opt) in all_docs.iter().enumerate() {
                    if opt.is_none() {
                        if let Some(&loc) = self.primary_index.get(&ids[i]) {
                            miss_locs.push((i, loc));
                        }
                    }
                }

                if !miss_locs.is_empty() {
                    // Phase 3: batch pread sorted by offset
                    let batch = self.storage.read_batch_lockfree(&mut miss_locs)?;

                    // Phase 4: decode and batch-populate cache
                    let mut cache_entries: Vec<(u64, Arc<Value>)> =
                        Vec::with_capacity(batch.len());
                    for (i, bytes) in batch {
                        let doc = crate::codec::decode_doc(&bytes)?;
                        let arc = Arc::new(doc);
                        cache_entries.push((ids[i], Arc::clone(&arc)));
                        all_docs[i] = Some(arc);
                    }
                    self.doc_cache.put_many(cache_entries);
                }

                // Phase 5: build results
                for opt in all_docs {
                    if let Some(arc) = opt {
                        if skip_post_filter || query::matches_value(&query, &arc) {
                            results.push(arc);
                        }
                    }
                }
            } else {
                // Per-doc path: good for small result sets or queries with limit
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
            }
        } else {
            // No index — iterate all documents via parallel cache-first path.
            // Probes LRU cache first (zero-cost for warm cache), falls
            // back to disk + decompress only for cache misses.
            let ids: Vec<DocumentId> = self.primary_index.keys().copied().collect();
            let cached = self.doc_cache.get_many(&ids);

            let num_threads = std::thread::available_parallelism()
                .map(|n| n.get().min(8))
                .unwrap_or(4);

            // Phase 1: parallel query matching on cached documents
            let mut miss_indices: Vec<usize> = Vec::new();
            if cached.len() >= 10_000 && num_threads > 1 && early_limit.is_none() {
                let chunk_size = (cached.len() + num_threads - 1) / num_threads;
                let (par_results, par_misses) = std::thread::scope(|s| {
                    let handles: Vec<_> = (0..num_threads)
                        .map(|t| {
                            let start = t * chunk_size;
                            let end = ((t + 1) * chunk_size).min(cached.len());
                            let slice = &cached[start..end];
                            let q = &query;
                            s.spawn(move || {
                                let mut local_hits: Vec<Arc<Value>> = Vec::new();
                                let mut local_miss: Vec<usize> = Vec::new();
                                for (j, opt) in slice.iter().enumerate() {
                                    if let Some(arc) = opt {
                                        if query::matches_value(q, arc) {
                                            local_hits.push(Arc::clone(arc));
                                        }
                                    } else {
                                        local_miss.push(start + j);
                                    }
                                }
                                (local_hits, local_miss)
                            })
                        })
                        .collect();
                    let mut all_results: Vec<Arc<Value>> = Vec::new();
                    let mut all_misses: Vec<usize> = Vec::new();
                    for h in handles {
                        let (r, m) = h.join().unwrap();
                        all_results.extend(r);
                        all_misses.extend(m);
                    }
                    (all_results, all_misses)
                });
                results.extend(par_results);
                miss_indices = par_misses;
            } else {
                // Sequential path: small collections or queries with limit
                for (i, opt) in cached.into_iter().enumerate() {
                    if let Some(arc) = opt {
                        if query::matches_value(&query, &arc) {
                            results.push(arc);
                            if let Some(limit) = early_limit {
                                if results.len() >= limit {
                                    break;
                                }
                            }
                        }
                    } else {
                        miss_indices.push(i);
                    }
                }
            }

            // Phase 2: batch-read cache misses from disk (if any and not already at limit)
            let at_limit = early_limit.map_or(false, |l| results.len() >= l);
            if !miss_indices.is_empty() && !at_limit {
                let mut miss_locs: Vec<(usize, crate::storage::DocLocation)> = miss_indices
                    .iter()
                    .filter_map(|&i| {
                        self.primary_index.get(&ids[i]).map(|&loc| (i, loc))
                    })
                    .collect();

                if !miss_locs.is_empty() {
                    let batch = self.storage.read_batch_lockfree(&mut miss_locs)?;
                    let mut cache_entries: Vec<(DocumentId, Arc<Value>)> =
                        Vec::with_capacity(batch.len());

                    for (i, bytes) in batch {
                        let doc = crate::codec::decode_doc(&bytes)?;
                        let arc = Arc::new(doc);
                        cache_entries.push((ids[i], Arc::clone(&arc)));
                        if query::matches_value(&query, &arc) {
                            results.push(arc);
                            if let Some(limit) = early_limit {
                                if results.len() >= limit {
                                    break;
                                }
                            }
                        }
                    }
                    self.doc_cache.put_many(cache_entries);
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

        let skip_post_filter = query::is_fully_indexed(&query, &self.field_indexes);

        // Try lazy index path first — avoids materializing full BTreeSet
        if !matches!(query, Query::All) {
            let mut found: Option<Value> = None;
            let lazy_result = query::execute_indexed_lazy(
                &query,
                &self.field_indexes,
                &mut |id| {
                    if let Some(arc) = self.load_doc_arc(id) {
                        if skip_post_filter || query::matches_value(&query, &arc) {
                            found = Some((*arc).clone());
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

    /// Execute a streaming `$group` aggregation without materializing all docs.
    /// Compute segment boundaries for parallel scanning.
    /// Returns a list of (start_offset, end_offset) pairs covering the whole file.
    fn compute_scan_segments(&self, num_threads: usize) -> Vec<(u64, u64)> {
        let file_size = self.storage.file_size();
        if file_size == 0 || num_threads <= 1 {
            return vec![(0, file_size)];
        }
        let mut offsets: Vec<u64> = self.primary_index.values().map(|loc| loc.offset).collect();
        offsets.sort_unstable();
        let n = offsets.len();
        let mut boundaries = Vec::with_capacity(num_threads + 1);
        boundaries.push(offsets[0]);
        for i in 1..num_threads {
            let idx = i * n / num_threads;
            boundaries.push(offsets[idx]);
        }
        boundaries.push(file_size);
        // Deduplicate adjacent boundaries
        boundaries.dedup();
        boundaries.windows(2).map(|w| (w[0], w[1])).collect()
    }

    /// Streams through storage sequentially, decoding each doc and feeding it
    /// to the group accumulators inline.
    ///
    /// When `match_query` is `Query::All`, scans the entire data file.
    /// When `match_query` is an indexed query, reads only candidate docs.
    ///
    /// For large collections (>= 100K docs) with raw-eligible pipelines,
    /// uses parallel segmented scanning for significant speedup.
    pub(crate) fn aggregate_streaming(
        &self,
        match_query_json: Option<&Value>,
        group_key: &crate::pipeline::GroupKey,
        accumulators: &[(String, crate::pipeline::Accumulator)],
    ) -> Result<Vec<Value>> {
        let use_raw = crate::pipeline::is_raw_eligible(group_key, accumulators);
        let doc_count = self.primary_index.len();

        // ── Parallel path: large collection + raw-eligible ───────────────
        // Decide whether to use parallel full scan even for $match queries
        // when the indexed candidate set is large (> 25% of collection).
        let use_parallel = use_raw && doc_count >= 100_000;

        if use_parallel {
            // Determine if we have a $match filter
            let match_query = match match_query_json {
                Some(match_val) => {
                    let q = query::parse_query(match_val)?;
                    if matches!(q, Query::All) { None } else { Some(q) }
                }
                None => None,
            };

            // For $match with index, check if parallel full scan is better
            // than indexed lookup. With warm LRU cache, random reads via
            // cached doc pointers are nearly free, so prefer the indexed path
            // whenever candidates < 50% of collection. This avoids scanning
            // the entire collection when an index can skip most documents.
            if let Some(ref query) = match_query {
                let candidate_ids = query::execute_indexed(
                    query,
                    &self.field_indexes,
                    &self.composite_indexes,
                );
                if let Some(ref ids) = candidate_ids {
                    if ids.len() <= doc_count / 2 {
                        // Small candidate set — indexed random-read is faster
                        return self.aggregate_streaming_indexed(
                            query, ids, group_key, accumulators, use_raw,
                        );
                    }
                }
            }

            // Parallel segmented scan
            let num_threads = std::thread::available_parallelism()
                .map(|n| n.get().min(8))
                .unwrap_or(4);
            let segments = self.compute_scan_segments(num_threads);

            let match_query_ref = &match_query;
            let result: Result<Vec<Value>> = std::thread::scope(|s| {
                let handles: Vec<_> = segments
                    .iter()
                    .map(|&(start, end)| {
                        s.spawn(move || {
                            let mut local_group =
                                crate::pipeline::StreamingGroup::new(group_key, accumulators);
                            if let Some(ref query) = *match_query_ref {
                                self.storage.scan_segment_readonly_while(start, end, |bytes| {
                                    match query::matches_raw_jsonb(query, bytes) {
                                        Some(true) => local_group.feed_raw(bytes),
                                        Some(false) => {}
                                        None => {
                                            let doc = crate::codec::decode_doc(bytes)?;
                                            if query::matches_value(query, &doc) {
                                                local_group.feed(&doc);
                                            }
                                        }
                                    }
                                    Ok(true)
                                })?;
                            } else {
                                self.storage.scan_segment_readonly_while(start, end, |bytes| {
                                    local_group.feed_raw(bytes);
                                    Ok(true)
                                })?;
                            }
                            Ok::<_, crate::error::Error>(local_group)
                        })
                    })
                    .collect();

                let mut merged: Option<crate::pipeline::StreamingGroup> = None;
                for handle in handles {
                    let local_group: crate::pipeline::StreamingGroup = handle.join().unwrap()?;
                    match merged {
                        None => merged = Some(local_group),
                        Some(ref mut m) => m.merge(local_group),
                    }
                }
                Ok(merged
                    .map(|g| g.finalize())
                    .unwrap_or_default())
            });
            return result;
        }

        // ── Sequential path (small collection or non-raw) ───────────────
        let mut group =
            crate::pipeline::StreamingGroup::new(group_key, accumulators);

        match match_query_json {
            None => {
                if use_raw {
                    self.storage.scan_readonly_while(|bytes| {
                        group.feed_raw(bytes);
                        Ok(true)
                    })?;
                } else {
                    self.for_each_doc_streaming(|doc| {
                        group.feed(doc);
                        Ok(true)
                    })?;
                }
            }
            Some(match_val) => {
                let query = query::parse_query(match_val)?;
                if matches!(query, Query::All) {
                    if use_raw {
                        self.storage.scan_readonly_while(|bytes| {
                            group.feed_raw(bytes);
                            Ok(true)
                        })?;
                    } else {
                        self.for_each_doc_streaming(|doc| {
                            group.feed(doc);
                            Ok(true)
                        })?;
                    }
                } else {
                    // Try index-accelerated candidate lookup
                    let candidate_ids = query::execute_indexed(
                        &query,
                        &self.field_indexes,
                        &self.composite_indexes,
                    );
                    let skip_post_filter =
                        query::is_fully_indexed(&query, &self.field_indexes);
                    if let Some(ref ids) = candidate_ids {
                        // Indexed match — batch pread + raw JSONB path
                        let id_vec: Vec<u64> = ids.iter().copied().collect();

                        // Phase 1: single-lock cache probe
                        let cached = self.doc_cache.get_many(&id_vec);

                        // Phase 2: collect cache misses with storage locations
                        let mut miss_locs: Vec<(usize, crate::storage::DocLocation)> =
                            Vec::new();
                        for (i, opt) in cached.iter().enumerate() {
                            if opt.is_none() {
                                if let Some(&loc) = self.primary_index.get(&id_vec[i]) {
                                    miss_locs.push((i, loc));
                                }
                            }
                        }

                        // Phase 3: batch pread sorted by offset for I/O locality
                        let batch_raw = if !miss_locs.is_empty() {
                            self.storage.read_batch_lockfree(&mut miss_locs)?
                        } else {
                            Vec::new()
                        };

                        // Phase 4: feed cache hits
                        for opt in cached.iter() {
                            if let Some(arc) = opt {
                                if skip_post_filter
                                    || query::matches_value(&query, arc)
                                {
                                    group.feed(arc);
                                }
                            }
                        }

                        // Phase 5: feed cache misses from raw bytes
                        if use_raw && skip_post_filter {
                            for (_i, bytes) in &batch_raw {
                                group.feed_raw(bytes);
                            }
                        } else {
                            let mut cache_entries: Vec<(u64, Arc<Value>)> =
                                Vec::with_capacity(batch_raw.len());
                            for (i, bytes) in batch_raw {
                                let doc = crate::codec::decode_doc(&bytes)?;
                                let arc = Arc::new(doc);
                                if skip_post_filter
                                    || query::matches_value(&query, &arc)
                                {
                                    group.feed(&arc);
                                }
                                cache_entries.push((id_vec[i], arc));
                            }
                            self.doc_cache.put_many(cache_entries);
                        }
                    } else if use_raw {
                        self.storage.scan_readonly_while(|bytes| {
                            match query::matches_raw_jsonb(&query, bytes) {
                                Some(true) => group.feed_raw(bytes),
                                Some(false) => {}
                                None => {
                                    let doc = crate::codec::decode_doc(bytes)?;
                                    if query::matches_value(&query, &doc) {
                                        group.feed(&doc);
                                    }
                                }
                            }
                            Ok(true)
                        })?;
                    } else {
                        self.for_each_doc_streaming(|doc| {
                            if query::matches_value(&query, doc) {
                                group.feed(doc);
                            }
                            Ok(true)
                        })?;
                    }
                }
            }
        }

        Ok(group.finalize())
    }

    /// Indexed match path — batch pread + raw JSONB path (extracted for reuse).
    fn aggregate_streaming_indexed(
        &self,
        query: &Query,
        ids: &std::collections::BTreeSet<crate::document::DocumentId>,
        group_key: &crate::pipeline::GroupKey,
        accumulators: &[(String, crate::pipeline::Accumulator)],
        use_raw: bool,
    ) -> Result<Vec<Value>> {
        let mut group = crate::pipeline::StreamingGroup::new(group_key, accumulators);
        let skip_post_filter = query::is_fully_indexed(query, &self.field_indexes);
        let id_vec: Vec<u64> = ids.iter().copied().collect();

        let cached = self.doc_cache.get_many(&id_vec);

        let mut miss_locs: Vec<(usize, crate::storage::DocLocation)> = Vec::new();
        for (i, opt) in cached.iter().enumerate() {
            if opt.is_none() {
                if let Some(&loc) = self.primary_index.get(&id_vec[i]) {
                    miss_locs.push((i, loc));
                }
            }
        }

        let batch_raw = if !miss_locs.is_empty() {
            self.storage.read_batch_lockfree(&mut miss_locs)?
        } else {
            Vec::new()
        };

        for opt in cached.iter() {
            if let Some(arc) = opt {
                if skip_post_filter || query::matches_value(query, arc) {
                    group.feed(arc);
                }
            }
        }

        if use_raw && skip_post_filter {
            for (_i, bytes) in &batch_raw {
                group.feed_raw(bytes);
            }
        } else {
            let mut cache_entries: Vec<(u64, Arc<Value>)> =
                Vec::with_capacity(batch_raw.len());
            for (i, bytes) in batch_raw {
                let doc = crate::codec::decode_doc(&bytes)?;
                let arc = Arc::new(doc);
                if skip_post_filter || query::matches_value(query, &arc) {
                    group.feed(&arc);
                }
                cache_entries.push((id_vec[i], arc));
            }
            self.doc_cache.put_many(cache_entries);
        }

        Ok(group.finalize())
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
            let primary_index = &self.primary_index;
            let skip_post_filter = query::is_fully_indexed(&query, &self.field_indexes);
            let lim = limit.unwrap();
            let lazy_result = query::execute_indexed_lazy(
                &query,
                &self.field_indexes,
                &mut |id| {
                    if let Some(arc) = self.load_doc_arc(id) {
                        if skip_post_filter || query::matches_value(&query, &arc) {
                            if let Some(&old_loc) = primary_index.get(&id) {
                                matches.push((id, (*arc).clone(), old_loc));
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

        // Phase 3: apply all mutations to .dat
        let mut new_locs = Vec::with_capacity(ops.len());
        for op in &ops {
            let new_loc = self.storage.append_no_sync(&op.new_bytes)?;
            self.storage.mark_deleted_no_sync(op.old_loc)?;
            new_locs.push(new_loc);
        }
        if !self.lazy_sync {
            self.storage.sync()?;
        }

        if !self.lazy_sync {
            self.wal.checkpoint_no_sync()?;
        }

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
            self.doc_cache.put(op.id, Arc::new(op.new_data));
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
            let primary_index = &self.primary_index;
            let skip_post_filter = query::is_fully_indexed(&query, &self.field_indexes);
            let lim = limit.unwrap();
            let lazy_result = query::execute_indexed_lazy(
                &query,
                &self.field_indexes,
                &mut |id| {
                    if let Some(arc) = self.load_doc_arc(id) {
                        if skip_post_filter || query::matches_value(&query, &arc) {
                            if let Some(&loc) = primary_index.get(&id) {
                                ops.push(DeleteOp { id, loc, data: (*arc).clone() });
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

        // Phase 3: mark all deleted in .dat
        for op in &ops {
            self.storage.mark_deleted_no_sync(op.loc)?;
        }
        if !self.lazy_sync {
            self.storage.sync()?;
        }

        if !self.lazy_sync {
            self.wal.checkpoint_no_sync()?;
        }

        // Phase 5: update in-memory state
        let mut deleted_ids = Vec::with_capacity(ops.len());
        for op in ops {
            deleted_ids.push(op.id);
            self.primary_index.remove(&op.id);
            self.version_index.remove(&op.id);
            self.doc_cache.remove(op.id);
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
            // Small candidate set — random access via LRU cache + storage fallback
            for &id in indexed_ids {
                if let Some(arc) = self.load_doc_arc(id) {
                    if query::matches_value(&query, &arc) {
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

        // In-memory mode: compact by rebuilding the Vec<u8> buffer
        if self.in_memory {
            let active_records = self.storage.iter_active()?;
            let new_mem = InMemStorage::new();
            let mut new_primary_index = HashMap::new();
            let mut next_id: DocumentId = 1;

            for (_old_loc, bytes) in &active_records {
                let doc: Value = crate::codec::decode_doc(bytes)?;
                let id = doc.get("_id").and_then(|v| v.as_u64()).ok_or_else(|| {
                    Error::InvalidQuery("document missing _id during compaction".into())
                })?;
                let new_bytes = crate::codec::encode_doc(&doc)?;
                let loc = new_mem.append(&new_bytes)?;
                new_primary_index.insert(id, loc);
                if id >= next_id {
                    next_id = id + 1;
                }
            }

            let docs_kept = new_primary_index.len();
            let new_size = new_mem.file_size();

            self.storage = StorageBackend::Memory(new_mem);
            self.primary_index = new_primary_index;
            self.next_id = next_id;

            // Fall through to index rebuild below
            self.version_index.clear();
            self.doc_cache.clear();
            for idx in self.field_indexes.values_mut() { idx.clear(); }
            for idx in &mut self.composite_indexes { idx.clear(); }
            if let Some(ref mut text_idx) = self.text_index { text_idx.clear(); }
            for idx in self.vector_indexes.values_mut() { idx.clear(); }
            for (&id, &loc) in &self.primary_index.clone() {
                let bytes = self.storage.read(loc)?;
                let data: Value = crate::codec::decode_doc(&bytes)?;
                let ver = data.get("_version").and_then(|v| v.as_u64()).unwrap_or(0);
                self.version_index.insert(id, ver);
                let data_arc = Arc::new(data);
                for idx in self.field_indexes.values_mut() { idx.insert_value(id, &data_arc); }
                for idx in &mut self.composite_indexes { idx.insert_value(id, &data_arc); }
                if let Some(ref mut text_idx) = self.text_index { text_idx.index_doc(id, &data_arc); }
                for idx in self.vector_indexes.values_mut() { let _ = idx.insert(id, &data_arc); }
                self.doc_cache.put(id, data_arc);
            }

            return Ok(CompactStats { old_size, new_size, docs_kept });
        }

        // File mode: create temp storage, copy, rename
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
        self.storage = StorageBackend::File(Storage::open_with_encryption(&dat_path, self.encryption.clone())?);
        self.primary_index = new_primary_index;
        self.next_id = next_id;

        // Rebuild all indexes and version_index; clear LRU cache
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
            self.doc_cache.put(id, data_arc);
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
    // TTL (time-to-live) support
    // -----------------------------------------------------------------------

    /// If the document has a `_ttl` field (seconds), compute the expiry time
    /// and register it in the TTL index.
    fn register_ttl(&mut self, doc_id: DocumentId, data: &Value) {
        let ttl_secs = data
            .get("_ttl")
            .and_then(|v| v.as_u64().or_else(|| v.as_f64().map(|f| f as u64)));
        if let Some(secs) = ttl_secs {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let expires_at = now_ms + secs * 1000;
            self.ttl_index
                .entry(expires_at)
                .or_insert_with(Vec::new)
                .push(doc_id);
        }
    }

    /// Remove a document from the TTL index (called on update/delete).
    #[allow(dead_code)]
    fn unregister_ttl(&mut self, doc_id: DocumentId) {
        self.ttl_index.retain(|_, ids| {
            ids.retain(|&id| id != doc_id);
            !ids.is_empty()
        });
    }

    /// Evict all expired documents. Returns the number of evicted documents.
    pub fn evict_expired(&mut self) -> usize {
        if self.ttl_index.is_empty() {
            return 0;
        }

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // Collect expired doc IDs
        let expired_keys: Vec<u64> = self
            .ttl_index
            .range(..=now_ms)
            .map(|(k, _)| *k)
            .collect();

        if expired_keys.is_empty() {
            return 0;
        }

        let mut expired_ids: Vec<DocumentId> = Vec::new();
        for key in &expired_keys {
            if let Some(ids) = self.ttl_index.get(key) {
                expired_ids.extend(ids);
            }
        }

        let count = expired_ids.len();

        // Delete each expired document
        for doc_id in &expired_ids {
            if let Some(loc) = self.primary_index.remove(doc_id) {
                let _ = self.storage.mark_deleted_no_sync(loc);

                // Remove from indexes
                if let Ok(bytes) = self.storage.read(loc) {
                    if let Ok(doc) = crate::codec::decode_doc(&bytes) {
                        for idx in self.field_indexes.values_mut() {
                            idx.remove_value(*doc_id, &doc);
                        }
                        for idx in &mut self.composite_indexes {
                            idx.remove_value(*doc_id, &doc);
                        }
                    }
                }
            }
            self.version_index.remove(doc_id);
            self.doc_cache.remove(*doc_id);
        }

        // Remove expired TTL entries
        for key in expired_keys {
            self.ttl_index.remove(&key);
        }

        if count > 0 {
            let _ = self.storage.sync();
        }

        count
    }

    /// Returns true if this collection is in-memory only.
    pub fn is_in_memory(&self) -> bool {
        self.in_memory
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
                self.doc_cache.remove(m.doc_id);
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
                self.doc_cache.put(m.doc_id, Arc::new(m.new_data.clone()));
            }
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Raw JSONB helpers for zero-decode index creation
// ---------------------------------------------------------------------------

/// Extract a u64 value from a raw JSONB field (used for _id).
fn extract_raw_u64(raw: &jsonb::RawJsonb, field: &str) -> Option<u64> {
    use jsonb::keypath::KeyPath;
    use std::borrow::Cow;
    let keypath = [KeyPath::Name(Cow::Borrowed(field))];
    let owned = raw.get_by_keypath(keypath.iter()).ok()??;
    let val: Value = jsonb::from_raw_jsonb(&owned.as_raw()).ok()?;
    val.as_u64()
}

/// Extract an IndexValue from a raw JSONB field (used for indexed field).
fn extract_raw_index_value(raw: &jsonb::RawJsonb, field: &str) -> Option<IndexValue> {
    use jsonb::keypath::KeyPath;
    use std::borrow::Cow;
    let parts: Vec<&str> = field.split('.').collect();
    let keypath: Vec<KeyPath> = parts
        .iter()
        .map(|p| KeyPath::Name(Cow::Borrowed(p)))
        .collect();
    let owned = raw.get_by_keypath(keypath.iter()).ok()??;
    let val: Value = jsonb::from_raw_jsonb(&owned.as_raw()).ok()?;
    Some(IndexValue::from_json(&val))
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
