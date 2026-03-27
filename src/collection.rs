use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use parking_lot::RwLock;
use serde_json::Value;

use crate::crypto::EncryptionKey;
use crate::doc_cache::{self, DocCache};
use crate::document::DocumentId;
use crate::engine::LogCallback;
use crate::error::{Error, Result};
use crate::fts::CollectionTextIndex;
use crate::in_memory::{InMemStorage, StorageBackend, WalBackend};
use crate::index::{CompositeIndex, FieldIndex};
use crate::index_bundle::IndexBundle;
use crate::index_persist;
use crate::stripe::{Stripe, NUM_STRIPES};
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

/// A collection with stripe-level locking.
///
/// Document data is distributed across N stripes (each independently lockable)
/// and indexes have their own RwLock. This allows concurrent writes to different
/// documents and concurrent reads to proceed without global contention.
pub struct Collection {
    name: String,
    data_dir: PathBuf,
    storage: std::cell::UnsafeCell<StorageBackend>,
    wal: WalBackend,
    stripes: Vec<RwLock<Stripe>>,
    indexes: RwLock<IndexBundle>,
    next_id: AtomicU64,
    encryption: Option<Arc<EncryptionKey>>,
    verbose: bool,
    log_callback: Option<LogCallback>,
    lazy_sync: AtomicBool,
    in_memory: bool,
}

// SAFETY: StorageBackend (both File and Memory variants) uses internal Mutex
// for all mutations. The UnsafeCell is only needed for compact() which replaces
// the entire storage backend. compact() is serialized by nature (single caller).
unsafe impl Send for Collection {}
unsafe impl Sync for Collection {}

impl Collection {
    fn vlog(&self, msg: &str) {
        eprintln!("{msg}");
        if let Some(cb) = &self.log_callback {
            cb(msg);
        }
    }

    /// SAFETY: StorageBackend uses internal Mutex for all operations.
    /// This accessor is safe for concurrent reads/appends. The UnsafeCell
    /// is only needed so compact() can replace the entire backend.
    #[inline]
    fn storage(&self) -> &StorageBackend {
        unsafe { &*self.storage.get() }
    }

    #[inline]
    fn lazy_sync(&self) -> bool {
        self.lazy_sync.load(Ordering::Acquire)
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

// -----------------------------------------------------------------------
// Helper: distribute a flat HashMap into stripes
// -----------------------------------------------------------------------

fn distribute_primary_index(
    flat: HashMap<DocumentId, DocLocation>,
) -> Vec<HashMap<DocumentId, DocLocation>> {
    let mut per_stripe: Vec<HashMap<DocumentId, DocLocation>> = (0..NUM_STRIPES)
        .map(|_| HashMap::new())
        .collect();
    for (id, loc) in flat {
        per_stripe[Stripe::stripe_for(id)].insert(id, loc);
    }
    per_stripe
}

fn distribute_version_index(
    flat: HashMap<DocumentId, u64>,
) -> Vec<HashMap<DocumentId, u64>> {
    let mut per_stripe: Vec<HashMap<DocumentId, u64>> = (0..NUM_STRIPES)
        .map(|_| HashMap::new())
        .collect();
    for (id, ver) in flat {
        per_stripe[Stripe::stripe_for(id)].insert(id, ver);
    }
    per_stripe
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
    pub fn save_index_data(&self) {
        if self.in_memory {
            return;
        }
        let doc_count = self.count() as u64;
        let next_id = self.next_id.load(Ordering::Acquire);

        let idx = self.indexes.read();
        let enc = self.encryption.as_ref();
        let fidx_path = self.data_dir.join(format!("{}.fidx", self.name));
        let field_refs: Vec<&FieldIndex> = idx.field_indexes.values().collect();
        if let Err(e) = index_persist::save_field_indexes(&fidx_path, &field_refs, doc_count, next_id, enc) {
            eprintln!("[warn] {}: failed to save field index cache: {}", self.name, e);
        }

        let cidx_path = self.data_dir.join(format!("{}.cidx", self.name));
        let comp_refs: Vec<&CompositeIndex> = idx.composite_indexes.iter().collect();
        if let Err(e) = index_persist::save_composite_indexes(&cidx_path, &comp_refs, doc_count, next_id, enc) {
            eprintln!("[warn] {}: failed to save composite index cache: {}", self.name, e);
        }

        let vidx_path = self.data_dir.join(format!("{}.vidx", self.name));
        let vec_refs: Vec<&VectorIndex> = idx.vector_indexes.values().collect();
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

        let idx_path = data_dir.join(format!("{}.idx", name));
        let persisted_indexes = load_index_metadata(&idx_path)?;
        let has_persisted_indexes = !persisted_indexes.is_empty();

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
        let mut version_index = HashMap::new();
        let shard_id_offset = Self::shard_id_offset();
        let mut next_id: DocumentId = 1 + shard_id_offset;

        let load_start = std::time::Instant::now();
        let mut doc_count: u64 = 0;

        let inner_cb = log_callback.clone();

        // Phase 1: Scan .dat for primary_index, version_index, next_id.
        storage.for_each_active(|loc, bytes| {
            let doc: Value = crate::codec::decode_doc(&bytes)?;
            if let Some(id) = doc.get("_id").and_then(|v| v.as_u64()) {
                primary_index.insert(id, loc);
                let ver = doc.get("_version").and_then(|v| v.as_u64()).unwrap_or(0);
                version_index.insert(id, ver);
                if id >= next_id {
                    next_id = id + 1;
                }

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

            let field_ok = cached_field.is_some() || field_indexes.is_empty();
            let comp_ok = cached_composite.is_some() || composite_indexes.is_empty();
            let vec_ok = cached_vector.is_some() || vector_indexes.is_empty();

            if field_ok && comp_ok && vec_ok {
                let cache_start = std::time::Instant::now();
                if let Some(cached) = cached_field {
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
        // We use a temporary DocCache for WAL recovery
        let doc_cache = DocCache::new(doc_cache::DEFAULT_CAPACITY);
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

        // Distribute flat maps into stripes
        let per_stripe_primary = distribute_primary_index(primary_index);
        let per_stripe_version = distribute_version_index(version_index);

        let cache_per_stripe = doc_cache::DEFAULT_CAPACITY / NUM_STRIPES;
        let stripes: Vec<RwLock<Stripe>> = per_stripe_primary
            .into_iter()
            .zip(per_stripe_version.into_iter())
            .map(|(pi, vi)| {
                RwLock::new(Stripe {
                    primary_index: pi,
                    version_index: vi,
                    doc_cache: DocCache::new(cache_per_stripe.max(1)),
                })
            })
            .collect();

        let indexes = RwLock::new(IndexBundle {
            field_indexes,
            composite_indexes,
            text_index,
            vector_indexes,
            ttl_index: std::collections::BTreeMap::new(),
        });

        let collection = Self {
            name: name.to_string(),
            data_dir: data_dir.to_path_buf(),
            storage: std::cell::UnsafeCell::new(storage),
            wal,
            stripes,
            indexes,
            next_id: AtomicU64::new(next_id),
            encryption,
            verbose,
            log_callback,
            lazy_sync: AtomicBool::new(false),
            in_memory: false,
        };

        // Save index cache after rebuild so next restart loads from cache
        if has_persisted_indexes && !indexes_from_cache {
            collection.save_index_data();
        }

        Ok(collection)
    }

    /// Create a new in-memory collection (no disk I/O).
    pub fn open_in_memory(name: &str) -> Self {
        let cache_per_stripe = doc_cache::DEFAULT_CAPACITY / NUM_STRIPES;
        let stripes: Vec<RwLock<Stripe>> = (0..NUM_STRIPES)
            .map(|_| RwLock::new(Stripe::new(cache_per_stripe.max(1))))
            .collect();

        Self {
            name: name.to_string(),
            data_dir: PathBuf::new(),
            storage: std::cell::UnsafeCell::new(StorageBackend::Memory(InMemStorage::new())),
            wal: WalBackend::Memory,
            stripes,
            indexes: RwLock::new(IndexBundle::new()),
            next_id: AtomicU64::new(1 + Self::shard_id_offset()),
            encryption: None,
            verbose: false,
            log_callback: None,
            lazy_sync: AtomicBool::new(false),
            in_memory: true,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    fn shard_id_offset() -> DocumentId {
        match std::env::var("OXIDB_SHARD_ID") {
            Ok(val) => {
                let shard_id: u64 = val.parse().unwrap_or(0);
                shard_id * (1u64 << 48)
            }
            Err(_) => 0,
        }
    }

    pub fn set_lazy_sync(&self, enabled: bool) {
        self.lazy_sync.store(enabled, Ordering::Release);
    }

    pub fn set_cache_capacity(&self, capacity: usize) {
        let per_stripe = capacity / NUM_STRIPES;
        for stripe_lock in &self.stripes {
            let stripe = stripe_lock.read();
            stripe.doc_cache.resize(per_stripe.max(1));
        }
    }

    pub fn cache_capacity(&self) -> usize {
        // Sum up all stripe capacities
        self.stripes.iter().map(|s| s.read().doc_cache.capacity()).sum()
    }

    /// Flush pending writes to disk: sync storage file, then checkpoint WAL.
    pub fn sync_writes(&self) -> Result<()> {
        self.storage().sync()?;
        self.wal.checkpoint_no_sync()?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Index accessors (take index read lock)
    // -----------------------------------------------------------------------

    /// Access the field indexes for index-accelerated aggregation.
    /// NOTE: Returns a read guard. Callers must not hold this across I/O.
    pub fn field_indexes(&self) -> parking_lot::RwLockReadGuard<'_, IndexBundle> {
        self.indexes.read()
    }

    /// Check if any composite indexes exist.
    pub fn has_composite_indexes(&self) -> bool {
        !self.indexes.read().composite_indexes.is_empty()
    }

    pub fn has_text_index(&self) -> bool {
        self.indexes.read().text_index.is_some()
    }

    /// Look up a document by ID. Checks LRU cache first, falls back to storage.
    pub fn load_doc_arc(&self, id: DocumentId) -> Option<Arc<Value>> {
        let stripe_idx = Stripe::stripe_for(id);
        let stripe = self.stripes[stripe_idx].read();

        // Fast path: LRU cache hit
        if let Some(arc) = stripe.doc_cache.get(id) {
            return Some(arc);
        }

        // Slow path: read from storage
        let loc = stripe.primary_index.get(&id)?;
        let bytes = self.storage().read_lockfree(*loc).ok()?;
        let doc = crate::codec::decode_doc(&bytes).ok()?;
        let arc = Arc::new(doc);
        stripe.doc_cache.put(id, Arc::clone(&arc));
        Some(arc)
    }

    fn read_doc(&self, id: DocumentId) -> Result<Option<Value>> {
        Ok(self.load_doc_arc(id).map(|arc| (*arc).clone()))
    }

    fn read_doc_arc(&self, id: DocumentId) -> Option<Arc<Value>> {
        self.load_doc_arc(id)
    }

    /// Check if a document exists in the primary index.
    fn primary_contains(&self, id: DocumentId) -> bool {
        let stripe_idx = Stripe::stripe_for(id);
        let stripe = self.stripes[stripe_idx].read();
        stripe.primary_index.contains_key(&id)
    }

    /// Get a document location from the primary index.
    fn primary_get(&self, id: DocumentId) -> Option<DocLocation> {
        let stripe_idx = Stripe::stripe_for(id);
        let stripe = self.stripes[stripe_idx].read();
        stripe.primary_index.get(&id).copied()
    }

    /// Iterate all documents, calling `f` for each one.
    fn for_each_doc<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(DocumentId, Value) -> Result<()>,
    {
        // Collect all (id, loc) pairs from all stripes
        let mut all_entries: Vec<(DocumentId, DocLocation)> = Vec::new();
        for stripe_lock in &self.stripes {
            let stripe = stripe_lock.read();
            for (&id, &loc) in &stripe.primary_index {
                all_entries.push((id, loc));
            }
        }
        for (id, loc) in all_entries {
            let bytes = self.storage().read(loc)?;
            let doc = crate::codec::decode_doc(&bytes)?;
            f(id, doc)?;
        }
        Ok(())
    }

    fn for_each_doc_arc_while<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(DocumentId, &Arc<Value>) -> Result<bool>,
    {
        let mut all_entries: Vec<(DocumentId, DocLocation)> = Vec::new();
        for stripe_lock in &self.stripes {
            let stripe = stripe_lock.read();
            for (&id, &loc) in &stripe.primary_index {
                all_entries.push((id, loc));
            }
        }
        for (id, loc) in all_entries {
            let bytes = self.storage().read(loc)?;
            let doc = crate::codec::decode_doc(&bytes)?;
            let arc = Arc::new(doc);
            let stripe_idx = Stripe::stripe_for(id);
            self.stripes[stripe_idx].read().doc_cache.put(id, Arc::clone(&arc));
            if !f(id, &arc)? {
                break;
            }
        }
        Ok(())
    }

    fn for_each_doc_streaming<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&Value) -> Result<bool>,
    {
        self.storage().scan_readonly_while(|bytes| {
            let doc: Value = crate::codec::decode_doc(bytes)?;
            f(&doc)
        })
    }

    // -----------------------------------------------------------------------
    // Index management
    // -----------------------------------------------------------------------

    pub fn create_index(&self, field: &str) -> Result<()> {
        {
            let idx = self.indexes.read();
            if idx.field_indexes.contains_key(field) {
                return Ok(());
            }
        }

        let total = self.count();
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

        let field_owned = field.to_string();
        self.storage().scan_readonly_while(|bytes| {
            if !bytes.is_empty() && bytes[0] != b'{' && bytes[0] != b'[' {
                let raw = jsonb::RawJsonb::new(bytes);
                if let Some(id) = extract_raw_u64(&raw, "_id") {
                    if let Some(iv) = extract_raw_index_value(&raw, &field_owned) {
                        idx.insert_raw(id, iv);
                    }
                    count += 1;
                }
            } else {
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
        {
            let mut indexes = self.indexes.write();
            indexes.field_indexes.insert(field.to_string(), idx);
        }
        self.save_index_metadata()?;
        self.save_index_data();
        Ok(())
    }

    pub fn create_unique_index(&self, field: &str) -> Result<()> {
        {
            let idx = self.indexes.read();
            if idx.field_indexes.contains_key(field) {
                return Ok(());
            }
        }

        let total = self.count();
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

        self.storage().scan_readonly_while(|bytes| {
            if !bytes.is_empty() && bytes[0] != b'{' && bytes[0] != b'[' {
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
        {
            let mut indexes = self.indexes.write();
            indexes.field_indexes.insert(field.to_string(), idx);
        }
        self.save_index_metadata()?;
        self.save_index_data();
        Ok(())
    }

    pub fn create_composite_index(&self, fields: Vec<String>) -> Result<String> {
        let name = fields.join("_");
        {
            let idx = self.indexes.read();
            if idx.composite_indexes.iter().any(|i| i.name() == name) {
                return Ok(name);
            }
        }

        let total = self.count();
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

        self.storage().scan_readonly_while(|bytes| {
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
        {
            let mut indexes = self.indexes.write();
            indexes.composite_indexes.push(idx);
        }
        self.save_index_metadata()?;
        self.save_index_data();
        Ok(idx_name)
    }

    pub fn create_text_index(&self, fields: Vec<String>) -> Result<()> {
        {
            let idx = self.indexes.read();
            if idx.text_index.is_some() {
                return Ok(());
            }
        }

        let total = self.count();
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

        self.storage().scan_readonly_while(|bytes| {
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
        {
            let mut indexes = self.indexes.write();
            indexes.text_index = Some(idx);
        }
        self.save_index_metadata()?;
        Ok(())
    }

    pub fn list_indexes(&self) -> Vec<IndexInfo> {
        let idx = self.indexes.read();
        let mut indexes = Vec::new();
        for idx in idx.field_indexes.values() {
            indexes.push(IndexInfo {
                name: idx.field.clone(),
                index_type: if idx.unique { "unique".to_string() } else { "field".to_string() },
                fields: vec![idx.field.clone()],
                unique: idx.unique,
                dimension: None,
                metric: None,
            });
        }
        for idx in &idx.composite_indexes {
            indexes.push(IndexInfo {
                name: idx.name(),
                index_type: "composite".to_string(),
                fields: idx.fields.clone(),
                unique: false,
                dimension: None,
                metric: None,
            });
        }
        if let Some(ref text_idx) = idx.text_index {
            indexes.push(IndexInfo {
                name: "_text".to_string(),
                index_type: "text".to_string(),
                fields: text_idx.fields().to_vec(),
                unique: false,
                dimension: None,
                metric: None,
            });
        }
        for vidx in idx.vector_indexes.values() {
            indexes.push(IndexInfo {
                name: format!("_vec_{}", vidx.field),
                index_type: "vector".to_string(),
                fields: vec![vidx.field.clone()],
                unique: false,
                dimension: Some(vidx.dimension),
                metric: Some(vidx.metric_str().to_string()),
            });
        }
        indexes
    }

    pub fn drop_index(&self, name: &str) -> Result<()> {
        let mut idx = self.indexes.write();
        if idx.field_indexes.remove(name).is_some() {
            drop(idx);
            self.save_index_metadata()?;
            return Ok(());
        }
        if let Some(pos) = idx.composite_indexes.iter().position(|i| i.name() == name) {
            idx.composite_indexes.remove(pos);
            drop(idx);
            self.save_index_metadata()?;
            return Ok(());
        }
        if name == "_text" && idx.text_index.is_some() {
            idx.text_index = None;
            drop(idx);
            self.save_index_metadata()?;
            return Ok(());
        }
        if let Some(field) = name.strip_prefix("_vec_")
            && idx.vector_indexes.remove(field).is_some()
        {
            drop(idx);
            self.save_index_metadata()?;
            self.save_index_data();
            return Ok(());
        }
        Err(Error::IndexNotFound(name.to_string()))
    }

    pub fn text_search(&self, query: &str, limit: usize) -> Result<Vec<Value>> {
        let idx_guard = self.indexes.read();
        let idx = idx_guard.text_index.as_ref().ok_or_else(|| {
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

    pub fn create_vector_index(&self, field: &str, dimension: usize, metric: DistanceMetric) -> Result<()> {
        {
            let idx = self.indexes.read();
            if idx.vector_indexes.contains_key(field) {
                return Ok(());
            }
        }

        let total = self.count();
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

        self.storage().scan_readonly_while(|bytes| {
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
        {
            let mut indexes = self.indexes.write();
            indexes.vector_indexes.insert(field.to_string(), idx);
        }
        self.save_index_metadata()?;
        self.save_index_data();
        Ok(())
    }

    pub fn vector_search(&self, field: &str, query_vector: &[f32], limit: usize, ef_search: Option<usize>) -> Result<Vec<Value>> {
        let idx_guard = self.indexes.read();
        let idx = idx_guard.vector_indexes.get(field).ok_or_else(|| {
            Error::InvalidQuery(format!(
                "no vector index on field '{}'; create one with create_vector_index",
                field
            ))
        })?;

        let search_results = idx.search(query_vector, limit, ef_search)
            .map_err(|e| Error::InvalidQuery(e))?;

        drop(idx_guard); // Release index lock before reading docs

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

    pub fn check_unique_constraints(
        &self,
        data: &Value,
        exclude_id: Option<DocumentId>,
    ) -> Result<()> {
        let idx_guard = self.indexes.read();
        for idx in idx_guard.field_indexes.values() {
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
    pub fn insert(&self, mut data: Value) -> Result<DocumentId> {
        if !data.is_object() {
            return Err(Error::NotAnObject);
        }

        let id = self.next_id.fetch_add(1, Ordering::SeqCst);

        let obj = data.as_object_mut().unwrap();
        obj.insert("_id".to_string(), Value::Number(id.into()));
        obj.insert("_version".to_string(), Value::Number(1.into()));

        // Check unique constraints (needs index read lock)
        self.check_unique_constraints(&data, None)?;

        let bytes = crate::codec::encode_doc(&data)?;

        // WAL: log before mutating .dat
        self.wal.log_no_sync(&WalEntry::insert(id, bytes.clone()))?;

        let loc = if self.lazy_sync() {
            self.storage().append_no_sync(&bytes)?
        } else {
            self.storage().append(&bytes)?
        };

        if !self.lazy_sync() {
            self.wal.checkpoint_no_sync()?;
        }

        // Update stripe
        let stripe_idx = Stripe::stripe_for(id);
        {
            let mut stripe = self.stripes[stripe_idx].write();
            stripe.primary_index.insert(id, loc);
            stripe.version_index.insert(id, 1);
        }

        let data_arc = Arc::new(data);

        // Update indexes
        {
            let mut indexes = self.indexes.write();
            for idx in indexes.field_indexes.values_mut() {
                idx.insert_value(id, &data_arc);
            }
            for idx in &mut indexes.composite_indexes {
                idx.insert_value(id, &data_arc);
            }
            if let Some(ref mut text_idx) = indexes.text_index {
                text_idx.index_doc(id, &data_arc);
            }
            for idx in indexes.vector_indexes.values_mut() {
                let _ = idx.insert(id, &data_arc);
            }
            // TTL
            Self::register_ttl_inner(&mut indexes.ttl_index, id, &data_arc);
        }

        self.stripes[stripe_idx].read().doc_cache.put(id, data_arc);

        Ok(id)
    }

    /// Insert multiple documents in a single atomic batch.
    pub fn insert_many(&self, docs: Vec<Value>) -> Result<Vec<DocumentId>> {
        if docs.is_empty() {
            return Ok(vec![]);
        }

        // Phase 1: assign IDs, serialize, and validate ALL constraints upfront
        let first_id = self.next_id.fetch_add(docs.len() as u64, Ordering::SeqCst);
        let mut prepared = Vec::with_capacity(docs.len());
        let mut pending_unique: HashMap<String, HashMap<IndexValue, DocumentId>> = HashMap::new();

        // Get unique field names under a read lock
        let unique_fields: Vec<String> = {
            let idx = self.indexes.read();
            idx.field_indexes
                .values()
                .filter(|idx| idx.unique)
                .map(|idx| idx.field.clone())
                .collect()
        };
        let has_unique_indexes = !unique_fields.is_empty();

        for (i, mut data) in docs.into_iter().enumerate() {
            if !data.is_object() {
                return Err(Error::NotAnObject);
            }
            let id = first_id + i as u64;
            let obj = data.as_object_mut().unwrap();
            obj.insert("_id".to_string(), Value::Number(id.into()));
            obj.insert("_version".to_string(), Value::Number(1.into()));

            // Check against existing index
            self.check_unique_constraints(&data, None)?;

            // Check intra-batch uniqueness
            if has_unique_indexes {
                for field in &unique_fields {
                    if let Some(value) = resolve_field_in_value(&data, field) {
                        let iv = IndexValue::from_json(value);
                        let field_map = pending_unique.entry(field.clone()).or_default();
                        if field_map.contains_key(&iv) {
                            return Err(Error::UniqueViolation {
                                field: field.clone(),
                            });
                        }
                        field_map.insert(iv, id);
                    }
                }
            }

            let bytes = crate::codec::encode_doc(&data)?;
            prepared.push((id, data, bytes));
        }

        // Phase 2: WAL log all entries
        if !self.lazy_sync() {
            let wal_refs: Vec<(u64, &[u8])> = prepared
                .iter()
                .map(|(id, _, bytes)| (*id, bytes.as_slice()))
                .collect();
            self.wal.log_batch_inserts_no_sync_buffered(&wal_refs)?;
        }

        // Phase 3: append all to .dat
        let byte_slices: Vec<&[u8]> = prepared.iter().map(|(_, _, bytes)| bytes.as_slice()).collect();
        let batch_locs = self.storage().append_batch_no_sync_buffered(&byte_slices)?;
        if !self.lazy_sync() {
            self.storage().sync()?;
        }

        if !self.lazy_sync() {
            self.wal.checkpoint_no_sync()?;
        }

        // Phase 4: update stripes and indexes
        let skip_cache = prepared.len() > 1000;
        let has_indexes = {
            let idx = self.indexes.read();
            !idx.field_indexes.is_empty()
                || !idx.composite_indexes.is_empty()
                || idx.text_index.is_some()
                || !idx.vector_indexes.is_empty()
        };

        let mut ids = Vec::with_capacity(prepared.len());

        // Group by stripe for efficient locking
        let mut per_stripe: Vec<Vec<(DocumentId, Value, DocLocation)>> = (0..NUM_STRIPES)
            .map(|_| Vec::new())
            .collect();
        for ((id, data, _bytes), loc) in prepared.into_iter().zip(batch_locs) {
            let s = Stripe::stripe_for(id);
            per_stripe[s].push((id, data, loc));
            ids.push(id);
        }

        // Lock stripes in order (deadlock prevention)
        for (s, entries) in per_stripe.iter().enumerate() {
            if entries.is_empty() { continue; }
            let mut stripe = self.stripes[s].write();
            for (id, _, loc) in entries {
                stripe.primary_index.insert(*id, *loc);
                stripe.version_index.insert(*id, 1);
            }
        }

        // Update indexes
        if has_indexes || !skip_cache {
            let mut indexes = self.indexes.write();
            for entries in &per_stripe {
                for (id, data, _loc) in entries {
                    if has_indexes || !skip_cache {
                        let data_arc = Arc::new(data.clone());
                        for idx in indexes.field_indexes.values_mut() {
                            idx.insert_value(*id, &data_arc);
                        }
                        for idx in &mut indexes.composite_indexes {
                            idx.insert_value(*id, &data_arc);
                        }
                        if let Some(ref mut text_idx) = indexes.text_index {
                            text_idx.index_doc(*id, &data_arc);
                        }
                        for idx in indexes.vector_indexes.values_mut() {
                            let _ = idx.insert(*id, &data_arc);
                        }
                        if !skip_cache {
                            let s = Stripe::stripe_for(*id);
                            self.stripes[s].read().doc_cache.put(*id, data_arc);
                        }
                    }
                }
            }
        }

        Ok(ids)
    }

    /// Reserve a contiguous block of document IDs.
    pub fn reserve_ids(&self, count: u64) -> DocumentId {
        self.next_id.fetch_add(count, Ordering::SeqCst)
    }

    /// Insert pre-serialized documents.
    pub fn insert_many_prepared(
        &self,
        prepared: Vec<(DocumentId, Value, Vec<u8>)>,
    ) -> Result<Vec<DocumentId>> {
        if prepared.is_empty() {
            return Ok(vec![]);
        }

        // WAL (skip when lazy_sync)
        if !self.lazy_sync() {
            let wal_refs: Vec<(u64, &[u8])> = prepared
                .iter()
                .map(|(id, _, bytes)| (*id, bytes.as_slice()))
                .collect();
            self.wal.log_batch_inserts_no_sync_buffered(&wal_refs)?;
        }

        // Append to storage
        let byte_slices: Vec<&[u8]> = prepared.iter().map(|(_, _, bytes)| bytes.as_slice()).collect();
        let batch_locs = self.storage().append_batch_no_sync_buffered(&byte_slices)?;
        if !self.lazy_sync() {
            self.storage().sync()?;
            self.wal.checkpoint_no_sync()?;
        }

        let mut ids = Vec::with_capacity(prepared.len());

        // Group by stripe
        let mut per_stripe: Vec<Vec<(DocumentId, Value, DocLocation)>> = (0..NUM_STRIPES)
            .map(|_| Vec::new())
            .collect();
        for ((id, data, _bytes), loc) in prepared.into_iter().zip(batch_locs) {
            let s = Stripe::stripe_for(id);
            per_stripe[s].push((id, data, loc));
            ids.push(id);
        }

        // Update stripes in order
        for (s, entries) in per_stripe.iter().enumerate() {
            if entries.is_empty() { continue; }
            let mut stripe = self.stripes[s].write();
            for (id, _, loc) in entries {
                stripe.primary_index.insert(*id, *loc);
                stripe.version_index.insert(*id, 1);
            }
        }

        // Update indexes
        {
            let mut indexes = self.indexes.write();
            for entries in &per_stripe {
                for (id, data, _) in entries {
                    let data_arc = Arc::new(data.clone());
                    for idx in indexes.field_indexes.values_mut() {
                        idx.insert_value(*id, &data_arc);
                    }
                    for idx in &mut indexes.composite_indexes {
                        idx.insert_value(*id, &data_arc);
                    }
                    if let Some(ref mut text_idx) = indexes.text_index {
                        text_idx.index_doc(*id, &data_arc);
                    }
                    for idx in indexes.vector_indexes.values_mut() {
                        let _ = idx.insert(*id, &data_arc);
                    }
                    let s = Stripe::stripe_for(*id);
                    self.stripes[s].read().doc_cache.put(*id, data_arc);
                }
            }
        }

        Ok(ids)
    }

    // -----------------------------------------------------------------------
    // Find / Query
    // -----------------------------------------------------------------------

    pub fn find(&self, query_json: &Value) -> Result<Vec<Value>> {
        self.find_with_options(query_json, &FindOptions::default())
    }

    pub fn find_arcs(&self, query_json: &Value) -> Result<Vec<Arc<Value>>> {
        self.find_with_options_arcs(query_json, &FindOptions::default())
    }

    pub fn find_with_options(
        &self,
        query_json: &Value,
        opts: &FindOptions,
    ) -> Result<Vec<Value>> {
        let arcs = self.find_with_options_arcs(query_json, opts)?;
        Ok(arcs.into_iter().map(|a| Arc::try_unwrap(a).unwrap_or_else(|a| (*a).clone())).collect())
    }

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

        let idx_guard = self.indexes.read();

        // Fast path: index-backed sort with early termination.
        if let Some(sort_fields) = &opts.sort {
            if sort_fields.len() == 1 {
                let (sort_field, sort_order) = &sort_fields[0];
                if let Some(field_idx) = idx_guard.field_indexes.get(sort_field) {
                    let need = opts.skip.unwrap_or(0) as usize + opts.limit.unwrap_or(u64::MAX) as usize;
                    let mut results = Vec::new();
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
                    return Ok(results);
                }
            }
        }

        // Composite index-backed sort
        if let Some(sort_fields) = &opts.sort {
            if sort_fields.len() == 1 {
                let (sort_field, sort_order) = &sort_fields[0];
                if let Some(eq_conds) = query::extract_eq_conditions(&query) {
                    for comp_idx in &idx_guard.composite_indexes {
                        let fields = &comp_idx.fields;
                        let n = fields.len();
                        if n >= 2
                            && fields[n - 1] == *sort_field
                            && fields[..n - 1]
                                .iter()
                                .all(|f| eq_conds.contains_key(f.as_str()))
                        {
                            let prefix: Vec<IndexValue> = fields[..n - 1]
                                .iter()
                                .map(|f| eq_conds[f.as_str()].clone())
                                .collect();

                            let need = opts.skip.unwrap_or(0) as usize
                                + opts.limit.unwrap_or(u64::MAX) as usize;

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
                            return Ok(results);
                        }
                    }
                }
            }
        }

        // Standard path: try index-accelerated lookup
        let skip_post_filter = query::is_fully_indexed(&query, &idx_guard.field_indexes);

        let early_limit: Option<usize> = if opts.sort.is_none() && opts.skip.is_none() {
            opts.limit.map(|l| l as usize)
        } else {
            None
        };

        let mut results = Vec::new();

        // Fast path: lazy index iteration for limit queries without sort/skip.
        if let Some(limit) = early_limit {
            let lazy_result = query::execute_indexed_lazy(
                &query,
                &idx_guard.field_indexes,
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
            &idx_guard.field_indexes,
            &idx_guard.composite_indexes,
        );

        if let Some(ref indexed_ids) = candidate_ids {
            const BATCH_THRESHOLD: usize = 1024;

            if indexed_ids.len() >= BATCH_THRESHOLD && early_limit.is_none() {
                let ids: Vec<u64> = indexed_ids.iter().copied().collect();

                // Phase 1: cache probe across all stripes
                let mut all_docs: Vec<Option<Arc<Value>>> = Vec::with_capacity(ids.len());
                for &id in &ids {
                    let s = Stripe::stripe_for(id);
                    let stripe = self.stripes[s].read();
                    all_docs.push(stripe.doc_cache.get(id));
                }

                // Phase 2: collect cache misses with storage locations
                let mut miss_locs: Vec<(usize, crate::storage::DocLocation)> = Vec::new();
                for (i, opt) in all_docs.iter().enumerate() {
                    if opt.is_none() {
                        if let Some(loc) = self.primary_get(ids[i]) {
                            miss_locs.push((i, loc));
                        }
                    }
                }

                if !miss_locs.is_empty() {
                    let batch = self.storage().read_batch_lockfree(&mut miss_locs)?;

                    for (i, bytes) in batch {
                        let doc = crate::codec::decode_doc(&bytes)?;
                        let arc = Arc::new(doc);
                        let s = Stripe::stripe_for(ids[i]);
                        self.stripes[s].read().doc_cache.put(ids[i], Arc::clone(&arc));
                        all_docs[i] = Some(arc);
                    }
                }

                for opt in all_docs {
                    if let Some(arc) = opt {
                        if skip_post_filter || query::matches_value(&query, &arc) {
                            results.push(arc);
                        }
                    }
                }
            } else {
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
            // No index — iterate all documents
            drop(idx_guard); // Release index lock before full scan

            let ids: Vec<DocumentId> = {
                let mut all_ids = Vec::new();
                for stripe_lock in &self.stripes {
                    let stripe = stripe_lock.read();
                    all_ids.extend(stripe.primary_index.keys().copied());
                }
                all_ids
            };

            // Batch cache probe
            let mut cached: Vec<Option<Arc<Value>>> = Vec::with_capacity(ids.len());
            for &id in &ids {
                let s = Stripe::stripe_for(id);
                let stripe = self.stripes[s].read();
                cached.push(stripe.doc_cache.get(id));
            }

            let num_threads = std::thread::available_parallelism()
                .map(|n| n.get().min(8))
                .unwrap_or(4);

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

            // Phase 2: batch-read cache misses from disk
            let at_limit = early_limit.map_or(false, |l| results.len() >= l);
            if !miss_indices.is_empty() && !at_limit {
                let mut miss_locs: Vec<(usize, crate::storage::DocLocation)> = miss_indices
                    .iter()
                    .filter_map(|&i| {
                        self.primary_get(ids[i]).map(|loc| (i, loc))
                    })
                    .collect();

                if !miss_locs.is_empty() {
                    let batch = self.storage().read_batch_lockfree(&mut miss_locs)?;

                    for (i, bytes) in batch {
                        let doc = crate::codec::decode_doc(&bytes)?;
                        let arc = Arc::new(doc);
                        let s = Stripe::stripe_for(ids[i]);
                        self.stripes[s].read().doc_cache.put(ids[i], Arc::clone(&arc));
                        if query::matches_value(&query, &arc) {
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
        }

        // Apply sort -> skip -> limit pipeline
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

    pub fn find_one(&self, query_json: &Value) -> Result<Option<Value>> {
        let query = query::parse_query(query_json)?;

        let idx_guard = self.indexes.read();
        let skip_post_filter = query::is_fully_indexed(&query, &idx_guard.field_indexes);

        // Try lazy index path first
        if !matches!(query, Query::All) {
            let mut found: Option<Value> = None;
            let lazy_result = query::execute_indexed_lazy(
                &query,
                &idx_guard.field_indexes,
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
                &idx_guard.field_indexes,
                &idx_guard.composite_indexes,
            )
        } else {
            None
        };

        drop(idx_guard);

        if let Some(ref indexed_ids) = candidate_ids {
            for &id in indexed_ids {
                if self.primary_contains(id) {
                    if let Some(data) = self.read_doc(id)? {
                        if skip_post_filter || query::matches_value(&query, &data) {
                            return Ok(Some(data));
                        }
                    }
                }
            }
        } else {
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

    // -----------------------------------------------------------------------
    // Streaming aggregation
    // -----------------------------------------------------------------------

    fn compute_scan_segments(&self, num_threads: usize) -> Vec<(u64, u64)> {
        let file_size = self.storage().file_size();
        if file_size == 0 || num_threads <= 1 {
            return vec![(0, file_size)];
        }
        let mut offsets: Vec<u64> = Vec::new();
        for stripe_lock in &self.stripes {
            let stripe = stripe_lock.read();
            offsets.extend(stripe.primary_index.values().map(|loc| loc.offset));
        }
        offsets.sort_unstable();
        let n = offsets.len();
        if n == 0 {
            return vec![(0, file_size)];
        }
        let mut boundaries = Vec::with_capacity(num_threads + 1);
        boundaries.push(offsets[0]);
        for i in 1..num_threads {
            let idx = i * n / num_threads;
            boundaries.push(offsets[idx]);
        }
        boundaries.push(file_size);
        boundaries.dedup();
        boundaries.windows(2).map(|w| (w[0], w[1])).collect()
    }

    pub(crate) fn aggregate_streaming(
        &self,
        match_query_json: Option<&Value>,
        group_key: &crate::pipeline::GroupKey,
        accumulators: &[(String, crate::pipeline::Accumulator)],
    ) -> Result<Vec<Value>> {
        let use_raw = crate::pipeline::is_raw_eligible(group_key, accumulators);
        let doc_count = self.count();

        let use_parallel = use_raw && doc_count >= 100_000;

        if use_parallel {
            let match_query = match match_query_json {
                Some(match_val) => {
                    let q = query::parse_query(match_val)?;
                    if matches!(q, Query::All) { None } else { Some(q) }
                }
                None => None,
            };

            if let Some(ref query) = match_query {
                let idx_guard = self.indexes.read();
                let candidate_ids = query::execute_indexed(
                    query,
                    &idx_guard.field_indexes,
                    &idx_guard.composite_indexes,
                );
                if let Some(ref ids) = candidate_ids {
                    if ids.len() <= doc_count / 2 {
                        return self.aggregate_streaming_indexed(
                            query, ids, group_key, accumulators, use_raw,
                        );
                    }
                }
            }

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
                                self.storage().scan_segment_readonly_while(start, end, |bytes| {
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
                                self.storage().scan_segment_readonly_while(start, end, |bytes| {
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

        // Sequential path
        let mut group =
            crate::pipeline::StreamingGroup::new(group_key, accumulators);

        match match_query_json {
            None => {
                if use_raw {
                    self.storage().scan_readonly_while(|bytes| {
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
                        self.storage().scan_readonly_while(|bytes| {
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
                    let idx_guard = self.indexes.read();
                    let candidate_ids = query::execute_indexed(
                        &query,
                        &idx_guard.field_indexes,
                        &idx_guard.composite_indexes,
                    );
                    let skip_post_filter =
                        query::is_fully_indexed(&query, &idx_guard.field_indexes);
                    drop(idx_guard);

                    if let Some(ref ids) = candidate_ids {
                        let id_vec: Vec<u64> = ids.iter().copied().collect();

                        // Cache probe
                        let mut cached: Vec<Option<Arc<Value>>> = Vec::with_capacity(id_vec.len());
                        for &id in &id_vec {
                            let s = Stripe::stripe_for(id);
                            cached.push(self.stripes[s].read().doc_cache.get(id));
                        }

                        let mut miss_locs: Vec<(usize, crate::storage::DocLocation)> = Vec::new();
                        for (i, opt) in cached.iter().enumerate() {
                            if opt.is_none() {
                                if let Some(loc) = self.primary_get(id_vec[i]) {
                                    miss_locs.push((i, loc));
                                }
                            }
                        }

                        let batch_raw = if !miss_locs.is_empty() {
                            self.storage().read_batch_lockfree(&mut miss_locs)?
                        } else {
                            Vec::new()
                        };

                        for opt in cached.iter() {
                            if let Some(arc) = opt {
                                if skip_post_filter || query::matches_value(&query, arc) {
                                    group.feed(arc);
                                }
                            }
                        }

                        if use_raw && skip_post_filter {
                            for (_i, bytes) in &batch_raw {
                                group.feed_raw(bytes);
                            }
                        } else {
                            for (i, bytes) in batch_raw {
                                let doc = crate::codec::decode_doc(&bytes)?;
                                let arc = Arc::new(doc);
                                if skip_post_filter || query::matches_value(&query, &arc) {
                                    group.feed(&arc);
                                }
                                let s = Stripe::stripe_for(id_vec[i]);
                                self.stripes[s].read().doc_cache.put(id_vec[i], arc);
                            }
                        }
                    } else if use_raw {
                        self.storage().scan_readonly_while(|bytes| {
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

    fn aggregate_streaming_indexed(
        &self,
        query: &Query,
        ids: &std::collections::BTreeSet<crate::document::DocumentId>,
        group_key: &crate::pipeline::GroupKey,
        accumulators: &[(String, crate::pipeline::Accumulator)],
        use_raw: bool,
    ) -> Result<Vec<Value>> {
        let idx_guard = self.indexes.read();
        let mut group = crate::pipeline::StreamingGroup::new(group_key, accumulators);
        let skip_post_filter = query::is_fully_indexed(query, &idx_guard.field_indexes);
        drop(idx_guard);

        let id_vec: Vec<u64> = ids.iter().copied().collect();

        let mut cached: Vec<Option<Arc<Value>>> = Vec::with_capacity(id_vec.len());
        for &id in &id_vec {
            let s = Stripe::stripe_for(id);
            cached.push(self.stripes[s].read().doc_cache.get(id));
        }

        let mut miss_locs: Vec<(usize, crate::storage::DocLocation)> = Vec::new();
        for (i, opt) in cached.iter().enumerate() {
            if opt.is_none() {
                if let Some(loc) = self.primary_get(id_vec[i]) {
                    miss_locs.push((i, loc));
                }
            }
        }

        let batch_raw = if !miss_locs.is_empty() {
            self.storage().read_batch_lockfree(&mut miss_locs)?
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
            for (i, bytes) in batch_raw {
                let doc = crate::codec::decode_doc(&bytes)?;
                let arc = Arc::new(doc);
                if skip_post_filter || query::matches_value(query, &arc) {
                    group.feed(&arc);
                }
                let s = Stripe::stripe_for(id_vec[i]);
                self.stripes[s].read().doc_cache.put(id_vec[i], arc);
            }
        }

        Ok(group.finalize())
    }

    // -----------------------------------------------------------------------
    // Get by ID
    // -----------------------------------------------------------------------

    pub fn get(&self, id: DocumentId) -> Result<Option<Value>> {
        if self.primary_contains(id) {
            self.read_doc(id)
        } else {
            Ok(None)
        }
    }

    // -----------------------------------------------------------------------
    // Update
    // -----------------------------------------------------------------------

    pub fn update(&self, query_json: &Value, update_json: &Value, limit: Option<usize>) -> Result<Vec<DocumentId>> {
        let update_obj = update_json
            .as_object()
            .ok_or_else(|| Error::InvalidQuery("update must be an object".into()))?;
        if update_obj.is_empty() {
            return Err(Error::InvalidQuery(
                "update must contain at least one operator".into(),
            ));
        }

        let query = query::parse_query(query_json)?;

        // Phase 1: Find matching docs
        let mut matches: Vec<(DocumentId, Value, DocLocation)> = Vec::new();

        {
            let idx_guard = self.indexes.read();

            let mut lazy_handled = false;
            if limit.is_some() {
                let skip_post_filter = query::is_fully_indexed(&query, &idx_guard.field_indexes);
                let lim = limit.unwrap();
                let lazy_result = query::execute_indexed_lazy(
                    &query,
                    &idx_guard.field_indexes,
                    &mut |id| {
                        if let Some(arc) = self.load_doc_arc(id) {
                            if skip_post_filter || query::matches_value(&query, &arc) {
                                if let Some(old_loc) = self.primary_get(id) {
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
                    &idx_guard.field_indexes,
                    &idx_guard.composite_indexes,
                );

                if let Some(ref indexed_ids) = candidate_ids {
                    for &id in indexed_ids {
                        if let Some(old_loc) = self.primary_get(id) {
                            if let Some(data) = self.read_doc(id)? {
                                if query::matches_value(&query, &data) {
                                    matches.push((id, data, old_loc));
                                    if limit.is_some_and(|l| matches.len() >= l) { break; }
                                }
                            }
                        }
                    }
                } else {
                    drop(idx_guard);
                    self.for_each_doc_arc_while(|id, arc| {
                        if query::matches_value(&query, arc) {
                            if let Some(old_loc) = self.primary_get(id) {
                                matches.push((id, (**arc).clone(), old_loc));
                                if limit.is_some_and(|l| matches.len() >= l) { return Ok(false); }
                            }
                        }
                        Ok(true)
                    })?;
                }
            }
        }

        if matches.is_empty() {
            return Ok(Vec::new());
        }

        // Phase 2: Prepare all updates and validate constraints
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

        // Phase 3: WAL log all updates
        let wal_entries: Vec<WalEntry> = ops
            .iter()
            .map(|op| WalEntry::update(op.id, op.new_bytes.clone()))
            .collect();
        self.wal.log_batch_no_sync(&wal_entries)?;

        // Phase 4: apply all mutations to .dat
        let mut new_locs = Vec::with_capacity(ops.len());
        for op in &ops {
            let new_loc = self.storage().append_no_sync(&op.new_bytes)?;
            self.storage().mark_deleted_no_sync(op.old_loc)?;
            new_locs.push(new_loc);
        }
        if !self.lazy_sync() {
            self.storage().sync()?;
        }

        if !self.lazy_sync() {
            self.wal.checkpoint_no_sync()?;
        }

        // Phase 5: update stripes and indexes
        let mut updated_ids = Vec::with_capacity(ops.len());
        {
            let mut indexes = self.indexes.write();
            for (op, new_loc) in ops.into_iter().zip(new_locs) {
                updated_ids.push(op.id);
                let stripe_idx = Stripe::stripe_for(op.id);
                {
                    let mut stripe = self.stripes[stripe_idx].write();
                    stripe.primary_index.insert(op.id, new_loc);
                    let new_version = op.new_data.get("_version").and_then(|v| v.as_u64()).unwrap_or(1);
                    stripe.version_index.insert(op.id, new_version);
                }
                for idx in indexes.field_indexes.values_mut() {
                    idx.remove_value(op.id, &op.old_data);
                    idx.insert_value(op.id, &op.new_data);
                }
                for idx in &mut indexes.composite_indexes {
                    idx.remove_value(op.id, &op.old_data);
                    idx.insert_value(op.id, &op.new_data);
                }
                if let Some(ref mut text_idx) = indexes.text_index {
                    text_idx.index_doc(op.id, &op.new_data);
                }
                for idx in indexes.vector_indexes.values_mut() {
                    idx.remove(op.id);
                    let _ = idx.insert(op.id, &op.new_data);
                }
                self.stripes[Stripe::stripe_for(op.id)].read().doc_cache.put(op.id, Arc::new(op.new_data));
            }
        }

        Ok(updated_ids)
    }

    // -----------------------------------------------------------------------
    // Delete
    // -----------------------------------------------------------------------

    pub fn delete(&self, query_json: &Value, limit: Option<usize>) -> Result<Vec<DocumentId>> {
        let query = query::parse_query(query_json)?;

        struct DeleteOp {
            id: DocumentId,
            loc: DocLocation,
            data: Value,
        }
        let mut ops = Vec::new();

        {
            let idx_guard = self.indexes.read();

            let mut lazy_handled = false;
            if limit.is_some() {
                let skip_post_filter = query::is_fully_indexed(&query, &idx_guard.field_indexes);
                let lim = limit.unwrap();
                let lazy_result = query::execute_indexed_lazy(
                    &query,
                    &idx_guard.field_indexes,
                    &mut |id| {
                        if let Some(arc) = self.load_doc_arc(id) {
                            if skip_post_filter || query::matches_value(&query, &arc) {
                                if let Some(loc) = self.primary_get(id) {
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
                    &idx_guard.field_indexes,
                    &idx_guard.composite_indexes,
                );

                if let Some(ref indexed_ids) = candidate_ids {
                    for &id in indexed_ids {
                        if let Some(loc) = self.primary_get(id) {
                            if let Some(data) = self.read_doc(id)? {
                                if query::matches_value(&query, &data) {
                                    ops.push(DeleteOp { id, loc, data });
                                    if limit.is_some_and(|l| ops.len() >= l) { break; }
                                }
                            }
                        }
                    }
                } else {
                    drop(idx_guard);
                    self.for_each_doc_arc_while(|id, arc| {
                        if query::matches_value(&query, arc) {
                            if let Some(loc) = self.primary_get(id) {
                                ops.push(DeleteOp { id, loc, data: (**arc).clone() });
                                if limit.is_some_and(|l| ops.len() >= l) { return Ok(false); }
                            }
                        }
                        Ok(true)
                    })?;
                }
            }
        }

        if ops.is_empty() {
            return Ok(Vec::new());
        }

        // Phase 2: WAL log
        let wal_entries: Vec<WalEntry> = ops
            .iter()
            .map(|op| WalEntry::delete(op.id))
            .collect();
        self.wal.log_batch_no_sync(&wal_entries)?;

        // Phase 3: mark deleted
        for op in &ops {
            self.storage().mark_deleted_no_sync(op.loc)?;
        }
        if !self.lazy_sync() {
            self.storage().sync()?;
        }

        if !self.lazy_sync() {
            self.wal.checkpoint_no_sync()?;
        }

        // Phase 4: update stripes and indexes
        let mut deleted_ids = Vec::with_capacity(ops.len());
        {
            let mut indexes = self.indexes.write();
            for op in ops {
                deleted_ids.push(op.id);
                let stripe_idx = Stripe::stripe_for(op.id);
                {
                    let mut stripe = self.stripes[stripe_idx].write();
                    stripe.primary_index.remove(&op.id);
                    stripe.version_index.remove(&op.id);
                    stripe.doc_cache.remove(op.id);
                }
                for idx in indexes.field_indexes.values_mut() {
                    idx.remove_value(op.id, &op.data);
                }
                for idx in &mut indexes.composite_indexes {
                    idx.remove_value(op.id, &op.data);
                }
                if let Some(ref mut text_idx) = indexes.text_index {
                    text_idx.remove_doc(op.id);
                }
                for idx in indexes.vector_indexes.values_mut() {
                    idx.remove(op.id);
                }
            }
        }

        Ok(deleted_ids)
    }

    // -----------------------------------------------------------------------
    // Count
    // -----------------------------------------------------------------------

    pub fn count(&self) -> usize {
        self.stripes.iter().map(|s| s.read().primary_index.len()).sum()
    }

    pub fn count_matching(&self, query_json: &Value) -> Result<usize> {
        let query = query::parse_query(query_json)?;

        let idx_guard = self.indexes.read();

        if let Some(count) = query::count_indexed(&query, &idx_guard.field_indexes) {
            return Ok(count);
        }

        let candidate_ids = query::execute_indexed(
            &query,
            &idx_guard.field_indexes,
            &idx_guard.composite_indexes,
        );

        let skip_post_filter = query::is_fully_indexed(&query, &idx_guard.field_indexes);

        drop(idx_guard);

        let mut count = 0;
        if let Some(ref indexed_ids) = candidate_ids {
            if skip_post_filter {
                return Ok(indexed_ids.len());
            }
            let total_docs = self.count();
            if indexed_ids.len() > total_docs / 2 {
                return self.count_with_scan(&query);
            }
            for &id in indexed_ids {
                if let Some(arc) = self.load_doc_arc(id) {
                    if query::matches_value(&query, &arc) {
                        count += 1;
                    }
                }
            }
        } else {
            return self.count_with_scan(&query);
        }
        Ok(count)
    }

    fn count_with_scan(&self, query: &query::Query) -> Result<usize> {
        let mut count = 0;
        self.storage().scan_readonly_while(|bytes| {
            if let Some(matched) = query::matches_raw_jsonb(query, bytes) {
                if matched {
                    count += 1;
                }
            } else {
                let data = crate::codec::decode_doc(bytes)?;
                if query::matches_value(query, &data) {
                    count += 1;
                }
            }
            Ok(true)
        })?;
        Ok(count)
    }

    // -----------------------------------------------------------------------
    // Compact
    // -----------------------------------------------------------------------

    pub fn compact(&self) -> Result<CompactStats> {
        // Ensure WAL is clean
        self.wal.checkpoint()?;

        let old_size = self.storage().file_size();

        // In-memory mode
        if self.in_memory {
            let active_records = self.storage().iter_active()?;
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

            // We need to update storage - this requires unsafe mutation since we have &self
            // For the in-memory case, we use the same pattern as set_lazy_sync
            unsafe { *self.storage.get() = StorageBackend::Memory(new_mem); }

            // Distribute into stripes
            let per_stripe = distribute_primary_index(new_primary_index);
            for (s, pi) in per_stripe.into_iter().enumerate() {
                let mut stripe = self.stripes[s].write();
                stripe.primary_index = pi;
                stripe.version_index.clear();
                stripe.doc_cache.clear();
            }

            self.next_id.store(next_id, Ordering::SeqCst);

            // Rebuild indexes
            {
                let mut indexes = self.indexes.write();
                for idx in indexes.field_indexes.values_mut() { idx.clear(); }
                for idx in &mut indexes.composite_indexes { idx.clear(); }
                if let Some(ref mut text_idx) = indexes.text_index { text_idx.clear(); }
                for idx in indexes.vector_indexes.values_mut() { idx.clear(); }

                // Re-scan for version + indexes
                let mut all_entries: Vec<(DocumentId, DocLocation)> = Vec::new();
                for stripe_lock in &self.stripes {
                    let stripe = stripe_lock.read();
                    for (&id, &loc) in &stripe.primary_index {
                        all_entries.push((id, loc));
                    }
                }
                for (id, loc) in all_entries {
                    let bytes = self.storage().read(loc)?;
                    let data: Value = crate::codec::decode_doc(&bytes)?;
                    let ver = data.get("_version").and_then(|v| v.as_u64()).unwrap_or(0);
                    let s = Stripe::stripe_for(id);
                    {
                        let mut stripe = self.stripes[s].write();
                        stripe.version_index.insert(id, ver);
                    }
                    let data_arc = Arc::new(data);
                    for idx in indexes.field_indexes.values_mut() { idx.insert_value(id, &data_arc); }
                    for idx in &mut indexes.composite_indexes { idx.insert_value(id, &data_arc); }
                    if let Some(ref mut text_idx) = indexes.text_index { text_idx.index_doc(id, &data_arc); }
                    for idx in indexes.vector_indexes.values_mut() { let _ = idx.insert(id, &data_arc); }
                    self.stripes[s].read().doc_cache.put(id, data_arc);
                }
            }

            return Ok(CompactStats { old_size, new_size, docs_kept });
        }

        // File mode
        let tmp_path = self.data_dir.join(format!("{}.dat.tmp", self.name));
        let new_storage = Storage::open_with_encryption(&tmp_path, self.encryption.clone())?;

        let active_records = self.storage().iter_active()?;
        let mut new_primary_index = HashMap::new();
        let mut next_id: DocumentId = 1;

        for (_old_loc, bytes) in &active_records {
            let doc: Value = crate::codec::decode_doc(bytes)?;
            let id = doc.get("_id").and_then(|v| v.as_u64()).ok_or_else(|| {
                Error::InvalidQuery("document missing _id during compaction".into())
            })?;
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

        // Atomic swap
        let dat_path = self.data_dir.join(format!("{}.dat", self.name));
        std::fs::rename(&tmp_path, &dat_path)?;

        // Replace storage
        unsafe { *self.storage.get() = StorageBackend::File(Storage::open_with_encryption(&dat_path, self.encryption.clone())?); }

        // Distribute into stripes
        let per_stripe = distribute_primary_index(new_primary_index);
        for (s, pi) in per_stripe.into_iter().enumerate() {
            let mut stripe = self.stripes[s].write();
            stripe.primary_index = pi;
            stripe.version_index.clear();
            stripe.doc_cache.clear();
        }
        self.next_id.store(next_id, Ordering::SeqCst);

        // Rebuild indexes
        {
            let mut indexes = self.indexes.write();
            for idx in indexes.field_indexes.values_mut() { idx.clear(); }
            for idx in &mut indexes.composite_indexes { idx.clear(); }
            if let Some(ref mut text_idx) = indexes.text_index { text_idx.clear(); }
            for idx in indexes.vector_indexes.values_mut() { idx.clear(); }

            let mut all_entries: Vec<(DocumentId, DocLocation)> = Vec::new();
            for stripe_lock in &self.stripes {
                let stripe = stripe_lock.read();
                for (&id, &loc) in &stripe.primary_index {
                    all_entries.push((id, loc));
                }
            }
            for (id, loc) in all_entries {
                let bytes = self.storage().read(loc)?;
                let data: Value = crate::codec::decode_doc(&bytes)?;
                let ver = data.get("_version").and_then(|v| v.as_u64()).unwrap_or(0);
                let s = Stripe::stripe_for(id);
                {
                    let mut stripe = self.stripes[s].write();
                    stripe.version_index.insert(id, ver);
                }
                let data_arc = Arc::new(data);
                for idx in indexes.field_indexes.values_mut() { idx.insert_value(id, &data_arc); }
                for idx in &mut indexes.composite_indexes { idx.insert_value(id, &data_arc); }
                if let Some(ref mut text_idx) = indexes.text_index { text_idx.index_doc(id, &data_arc); }
                for idx in indexes.vector_indexes.values_mut() { let _ = idx.insert(id, &data_arc); }
                self.stripes[s].read().doc_cache.put(id, data_arc);
            }
        }

        self.save_index_data();

        Ok(CompactStats {
            old_size,
            new_size,
            docs_kept,
        })
    }

    // -----------------------------------------------------------------------
    // TTL support
    // -----------------------------------------------------------------------

    fn register_ttl_inner(ttl_index: &mut std::collections::BTreeMap<u64, Vec<DocumentId>>, doc_id: DocumentId, data: &Value) {
        let ttl_secs = data
            .get("_ttl")
            .and_then(|v| v.as_u64().or_else(|| v.as_f64().map(|f| f as u64)));
        if let Some(secs) = ttl_secs {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let expires_at = now_ms + secs * 1000;
            ttl_index
                .entry(expires_at)
                .or_insert_with(Vec::new)
                .push(doc_id);
        }
    }

    pub fn evict_expired(&self) -> usize {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let mut indexes = self.indexes.write();
        if indexes.ttl_index.is_empty() {
            return 0;
        }

        let expired_keys: Vec<u64> = indexes
            .ttl_index
            .range(..=now_ms)
            .map(|(k, _)| *k)
            .collect();

        if expired_keys.is_empty() {
            return 0;
        }

        let mut expired_ids: Vec<DocumentId> = Vec::new();
        for key in &expired_keys {
            if let Some(ids) = indexes.ttl_index.get(key) {
                expired_ids.extend(ids);
            }
        }

        let count = expired_ids.len();

        for doc_id in &expired_ids {
            let stripe_idx = Stripe::stripe_for(*doc_id);
            let mut stripe = self.stripes[stripe_idx].write();
            if let Some(loc) = stripe.primary_index.remove(doc_id) {
                let _ = self.storage().mark_deleted_no_sync(loc);

                if let Ok(bytes) = self.storage().read(loc) {
                    if let Ok(doc) = crate::codec::decode_doc(&bytes) {
                        for idx in indexes.field_indexes.values_mut() {
                            idx.remove_value(*doc_id, &doc);
                        }
                        for idx in &mut indexes.composite_indexes {
                            idx.remove_value(*doc_id, &doc);
                        }
                    }
                }
            }
            stripe.version_index.remove(doc_id);
            stripe.doc_cache.remove(*doc_id);
        }

        for key in expired_keys {
            indexes.ttl_index.remove(&key);
        }

        if count > 0 {
            let _ = self.storage().sync();
        }

        count
    }

    pub fn is_in_memory(&self) -> bool {
        self.in_memory
    }

    // -----------------------------------------------------------------------
    // Version tracking
    // -----------------------------------------------------------------------

    pub fn get_version(&self, doc_id: DocumentId) -> u64 {
        let stripe_idx = Stripe::stripe_for(doc_id);
        let stripe = self.stripes[stripe_idx].read();
        stripe.version_index.get(&doc_id).copied().unwrap_or(0)
    }

    pub fn log_wal_batch(&self, entries: &[WalEntry]) -> Result<()> {
        self.wal.log_batch(entries)
    }

    pub fn checkpoint_wal(&self) -> Result<()> {
        self.wal.checkpoint()
    }

    // -----------------------------------------------------------------------
    // Transaction prepare helpers
    // -----------------------------------------------------------------------

    pub fn prepare_tx_insert(&self, mut data: Value, tx_id: u64) -> Result<PreparedMutation> {
        if !data.is_object() {
            return Err(Error::NotAnObject);
        }

        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let obj = data.as_object_mut().unwrap();
        obj.insert("_id".to_string(), Value::Number(id.into()));
        obj.insert("_version".to_string(), Value::Number(1.into()));

        self.check_unique_constraints(&data, None)?;

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

    pub fn prepare_tx_update(
        &self,
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

        let query = query::parse_query(query_json)?;
        let idx_guard = self.indexes.read();
        let candidate_ids = query::execute_indexed(
            &query,
            &idx_guard.field_indexes,
            &idx_guard.composite_indexes,
        );
        drop(idx_guard);

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
                if let Some(old_loc) = self.primary_get(id) {
                    if let Some(data) = self.read_doc(id)? {
                        process_candidate(id, &data, old_loc)?;
                    }
                }
            }
        } else {
            let mut snapshot: Vec<(DocumentId, Value, DocLocation)> = Vec::new();
            self.for_each_doc(|id, data| {
                if let Some(loc) = self.primary_get(id) {
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

    pub fn prepare_tx_delete(
        &self,
        query_json: &Value,
        tx_id: u64,
    ) -> Result<Vec<PreparedMutation>> {
        let query = query::parse_query(query_json)?;
        let idx_guard = self.indexes.read();
        let candidate_ids = query::execute_indexed(
            &query,
            &idx_guard.field_indexes,
            &idx_guard.composite_indexes,
        );
        drop(idx_guard);

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
                if let Some(loc) = self.primary_get(id) {
                    if let Some(data) = self.read_doc(id)? {
                        process_candidate(id, &data, loc)?;
                    }
                }
            }
        } else {
            let mut snapshot: Vec<(DocumentId, Value, DocLocation)> = Vec::new();
            self.for_each_doc(|id, data| {
                if let Some(loc) = self.primary_get(id) {
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

    pub fn apply_prepared(&self, mutations: &mut Vec<PreparedMutation>) -> Result<()> {
        // Apply to storage
        for m in mutations.iter() {
            if m.is_delete {
                if let Some(loc) = m.old_loc {
                    self.storage().mark_deleted_no_sync(loc)?;
                }
            } else if let Some(old_loc) = m.old_loc {
                self.storage().mark_deleted_no_sync(old_loc)?;
            }
        }

        // Inserts and updates: append new bytes
        let mut new_locs = Vec::with_capacity(mutations.len());
        for m in mutations.iter() {
            if m.is_delete {
                new_locs.push(None);
            } else {
                let loc = self.storage().append_no_sync(&m.new_bytes)?;
                new_locs.push(Some(loc));
            }
        }
        self.storage().sync()?;

        // Update stripes and indexes
        let mut indexes = self.indexes.write();
        for (i, m) in mutations.iter().enumerate() {
            let stripe_idx = Stripe::stripe_for(m.doc_id);
            if m.is_delete {
                {
                    let mut stripe = self.stripes[stripe_idx].write();
                    stripe.primary_index.remove(&m.doc_id);
                    stripe.version_index.remove(&m.doc_id);
                    stripe.doc_cache.remove(m.doc_id);
                }
                if let Some(ref old_data) = m.old_data {
                    for idx in indexes.field_indexes.values_mut() {
                        idx.remove_value(m.doc_id, old_data);
                    }
                    for idx in &mut indexes.composite_indexes {
                        idx.remove_value(m.doc_id, old_data);
                    }
                }
                if let Some(ref mut text_idx) = indexes.text_index {
                    text_idx.remove_doc(m.doc_id);
                }
                for idx in indexes.vector_indexes.values_mut() {
                    idx.remove(m.doc_id);
                }
            } else if let Some(loc) = new_locs[i] {
                {
                    let mut stripe = self.stripes[stripe_idx].write();
                    stripe.primary_index.insert(m.doc_id, loc);
                    let ver = m.new_data.get("_version").and_then(|v| v.as_u64()).unwrap_or(1);
                    stripe.version_index.insert(m.doc_id, ver);
                }
                if let Some(ref old_data) = m.old_data {
                    for idx in indexes.field_indexes.values_mut() {
                        idx.remove_value(m.doc_id, old_data);
                    }
                    for idx in &mut indexes.composite_indexes {
                        idx.remove_value(m.doc_id, old_data);
                    }
                }
                for idx in indexes.field_indexes.values_mut() {
                    idx.insert_value(m.doc_id, &m.new_data);
                }
                for idx in &mut indexes.composite_indexes {
                    idx.insert_value(m.doc_id, &m.new_data);
                }
                if let Some(ref mut text_idx) = indexes.text_index {
                    text_idx.index_doc(m.doc_id, &m.new_data);
                }
                for idx in indexes.vector_indexes.values_mut() {
                    idx.remove(m.doc_id);
                    let _ = idx.insert(m.doc_id, &m.new_data);
                }
                self.stripes[stripe_idx].read().doc_cache.put(m.doc_id, Arc::new(m.new_data.clone()));
            }
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Raw JSONB helpers for zero-decode index creation
// ---------------------------------------------------------------------------

fn extract_raw_u64(raw: &jsonb::RawJsonb, field: &str) -> Option<u64> {
    use jsonb::keypath::KeyPath;
    use std::borrow::Cow;
    let keypath = [KeyPath::Name(Cow::Borrowed(field))];
    let owned = raw.get_by_keypath(keypath.iter()).ok()??;
    let val: Value = jsonb::from_raw_jsonb(&owned.as_raw()).ok()?;
    val.as_u64()
}

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
        let (_dir, col) = temp_collection("test");
        let id = col.insert(json!({"name": "Alice", "age": 30})).unwrap();
        let doc = col.get(id).unwrap().unwrap();
        assert_eq!(doc["name"], "Alice");
        assert_eq!(doc["_id"], id);
    }

    #[test]
    fn insert_assigns_version_1() {
        let (_dir, col) = temp_collection("test");
        let id = col.insert(json!({"name": "Alice"})).unwrap();
        let doc = col.get(id).unwrap().unwrap();
        assert_eq!(doc["_version"], 1);
        assert_eq!(col.get_version(id), 1);
    }

    #[test]
    fn update_increments_version() {
        let (_dir, col) = temp_collection("test");
        let id = col.insert(json!({"name": "Alice"})).unwrap();
        assert_eq!(col.get_version(id), 1);
        col.update(&json!({"_id": id}), &json!({"$set": {"name": "Bob"}}), None).unwrap();
        let doc = col.get(id).unwrap().unwrap();
        assert_eq!(doc["_version"], 2);
        assert_eq!(col.get_version(id), 2);
    }

    #[test]
    fn find_with_index() {
        let (_dir, col) = temp_collection("test");
        col.create_index("status").unwrap();
        col.insert(json!({"status": "active", "name": "Alice"})).unwrap();
        col.insert(json!({"status": "inactive", "name": "Bob"})).unwrap();
        col.insert(json!({"status": "active", "name": "Charlie"})).unwrap();

        let results = col.find(&json!({"status": "active"})).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn date_range_query() {
        let (_dir, col) = temp_collection("test");
        col.create_index("created_at").unwrap();

        col.insert(json!({"created_at": "2024-01-15T10:00:00Z", "name": "old"})).unwrap();
        col.insert(json!({"created_at": "2024-06-15T10:00:00Z", "name": "mid"})).unwrap();
        col.insert(json!({"created_at": "2025-01-15T10:00:00Z", "name": "new"})).unwrap();

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
        let (_dir, col) = temp_collection("test");
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
        let (_dir, col) = temp_collection("test");
        col.insert(json!({"name": "Alice"})).unwrap();
        col.insert(json!({"name": "Bob"})).unwrap();

        let ids = col.delete(&json!({"name": "Alice"}), None).unwrap();
        assert_eq!(ids.len(), 1);
        assert_eq!(col.count(), 1);
    }

    #[test]
    fn unique_index_enforced() {
        let (_dir, col) = temp_collection("test");
        col.create_unique_index("email").unwrap();
        col.insert(json!({"email": "alice@test.com", "name": "Alice"})).unwrap();

        let result = col.insert(json!({"email": "alice@test.com", "name": "Bob"}));
        assert!(result.is_err());
        assert_eq!(col.count(), 1);
    }

    #[test]
    fn unique_index_allows_different_values() {
        let (_dir, col) = temp_collection("test");
        col.create_unique_index("email").unwrap();
        col.insert(json!({"email": "alice@test.com"})).unwrap();
        col.insert(json!({"email": "bob@test.com"})).unwrap();
        assert_eq!(col.count(), 2);
    }

    #[test]
    fn unique_index_update_same_doc_ok() {
        let (_dir, col) = temp_collection("test");
        col.create_unique_index("email").unwrap();
        col.insert(json!({"email": "alice@test.com", "name": "Alice"})).unwrap();

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
        let (_dir, col) = temp_collection("test");
        col.create_unique_index("email").unwrap();
        col.insert(json!({"email": "alice@test.com", "name": "Alice"})).unwrap();
        col.insert(json!({"email": "bob@test.com", "name": "Bob"})).unwrap();

        let result = col.update(
            &json!({"name": "Bob"}),
            &json!({"$set": {"email": "alice@test.com"}}),
            None,
        );
        assert!(result.is_err());

        let bob = col.find_one(&json!({"name": "Bob"})).unwrap().unwrap();
        assert_eq!(bob["email"], "bob@test.com");
    }

    #[test]
    fn insert_many_unique_violation_rolls_back() {
        let (_dir, col) = temp_collection("test");
        col.create_unique_index("email").unwrap();
        col.insert(json!({"email": "alice@test.com"})).unwrap();

        let result = col.insert_many(vec![
            json!({"email": "charlie@test.com"}),
            json!({"email": "alice@test.com"}),
            json!({"email": "dave@test.com"}),
        ]);
        assert!(result.is_err());
        assert_eq!(col.count(), 1);
    }

    #[test]
    fn insert_many_intra_batch_uniqueness() {
        let (_dir, col) = temp_collection("test");
        col.create_unique_index("email").unwrap();

        let result = col.insert_many(vec![
            json!({"email": "same@test.com"}),
            json!({"email": "same@test.com"}),
        ]);
        assert!(result.is_err());
        assert_eq!(col.count(), 0);
    }

    #[test]
    fn atomic_multi_doc_update() {
        let (_dir, col) = temp_collection("test");
        col.insert(json!({"status": "draft", "title": "A"})).unwrap();
        col.insert(json!({"status": "draft", "title": "B"})).unwrap();

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
        let (_dir, col) = temp_collection("test");
        col.insert(json!({"status": "old", "title": "A"})).unwrap();
        col.insert(json!({"status": "old", "title": "B"})).unwrap();
        col.insert(json!({"status": "new", "title": "C"})).unwrap();

        let ids = col.delete(&json!({"status": "old"}), None).unwrap();
        assert_eq!(ids.len(), 2);
        assert_eq!(col.count(), 1);
    }

    #[test]
    fn sort_ascending() {
        let (_dir, col) = temp_collection("test");
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
        let (_dir, col) = temp_collection("test");
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
        let (_dir, col) = temp_collection("test");
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
        assert_eq!(results[0]["name"], "Alice");
        assert_eq!(results[1]["name"], "Bob");
        assert_eq!(results[2]["name"], "Dave");
        assert_eq!(results[3]["name"], "Charlie");
    }

    #[test]
    fn skip_and_limit() {
        let (_dir, col) = temp_collection("test");
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
        let (_dir, col) = temp_collection("test");
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
        let (_dir, col) = temp_collection("test");
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

    #[test]
    fn compact_reclaims_space() {
        let dir = tempdir().unwrap();
        let col = Collection::open("compact_test", dir.path()).unwrap();

        for i in 0..10 {
            col.insert(json!({"n": i, "payload": "x".repeat(100)})).unwrap();
        }

        let size_before = col.storage().file_size();

        col.delete(&json!({"n": {"$lt": 7}}), None).unwrap();
        assert_eq!(col.count(), 3);

        let size_after_delete = col.storage().file_size();
        assert!(size_after_delete >= size_before);

        let stats = col.compact().unwrap();
        assert_eq!(stats.docs_kept, 3);
        assert!(stats.new_size < stats.old_size);

        let results = col.find(&json!({})).unwrap();
        assert_eq!(results.len(), 3);
        for doc in &results {
            let n = doc["n"].as_i64().unwrap();
            assert!(n >= 7 && n < 10);
        }
    }

    #[test]
    fn composite_index_backed_sort_desc() {
        let (_dir, col) = temp_collection("comp_sort");
        col.create_index("formId").unwrap();
        col.create_composite_index(vec!["formId".into(), "createdAt".into()])
            .unwrap();

        col.insert(json!({"formId": "1", "createdAt": "2024-01-01T00:00:00Z", "name": "a"})).unwrap();
        col.insert(json!({"formId": "1", "createdAt": "2024-06-01T00:00:00Z", "name": "b"})).unwrap();
        col.insert(json!({"formId": "1", "createdAt": "2025-01-01T00:00:00Z", "name": "c"})).unwrap();
        col.insert(json!({"formId": "2", "createdAt": "2024-03-01T00:00:00Z", "name": "d"})).unwrap();
        col.insert(json!({"formId": "1", "createdAt": "2024-03-01T00:00:00Z", "name": "e"})).unwrap();

        let opts = FindOptions {
            sort: Some(vec![("createdAt".into(), SortOrder::Desc)]),
            skip: None,
            limit: Some(2),
        };
        let results = col.find_with_options(&json!({"formId": "1"}), &opts).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0]["name"], "c");
        assert_eq!(results[1]["name"], "b");

        let opts_asc = FindOptions {
            sort: Some(vec![("createdAt".into(), SortOrder::Asc)]),
            skip: None,
            limit: Some(2),
        };
        let results_asc = col.find_with_options(&json!({"formId": "1"}), &opts_asc).unwrap();
        assert_eq!(results_asc.len(), 2);
        assert_eq!(results_asc[0]["name"], "a");
        assert_eq!(results_asc[1]["name"], "e");

        let opts_skip = FindOptions {
            sort: Some(vec![("createdAt".into(), SortOrder::Desc)]),
            skip: Some(1),
            limit: Some(2),
        };
        let results_skip = col.find_with_options(&json!({"formId": "1"}), &opts_skip).unwrap();
        assert_eq!(results_skip.len(), 2);
        assert_eq!(results_skip[0]["name"], "b");
        assert_eq!(results_skip[1]["name"], "e");

        let results_f2 = col.find_with_options(&json!({"formId": "2"}), &opts).unwrap();
        assert_eq!(results_f2.len(), 1);
        assert_eq!(results_f2[0]["name"], "d");
    }

    #[test]
    fn composite_index_backed_sort_asc() {
        let (_dir, col) = temp_collection("comp_sort_asc");
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
        assert_eq!(results[0]["name"], "low");
        assert_eq!(results[1]["name"], "mid");
    }

    #[test]
    fn composite_index_sort_with_post_filter() {
        let (_dir, col) = temp_collection("comp_sort_postfilter");
        col.create_composite_index(vec!["formId".into(), "createdAt".into()])
            .unwrap();

        col.insert(json!({"formId": "1", "createdAt": "2024-01-01", "data": {"level": "Junior"}})).unwrap();
        col.insert(json!({"formId": "1", "createdAt": "2024-01-02", "data": {"level": "Senior"}})).unwrap();
        col.insert(json!({"formId": "1", "createdAt": "2024-01-03", "data": {"level": "Junior"}})).unwrap();
        col.insert(json!({"formId": "1", "createdAt": "2024-01-04", "data": {"level": "Senior"}})).unwrap();
        col.insert(json!({"formId": "1", "createdAt": "2024-01-05", "data": {"level": "Junior"}})).unwrap();
        col.insert(json!({"formId": "2", "createdAt": "2024-01-06", "data": {"level": "Junior"}})).unwrap();

        let opts = FindOptions {
            sort: Some(vec![("createdAt".into(), SortOrder::Desc)]),
            skip: None,
            limit: Some(2),
        };
        let query = json!({"$and": [{"formId": "1"}, {"data.level": "Junior"}]});
        let results = col.find_with_options(&query, &opts).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0]["createdAt"], "2024-01-05");
        assert_eq!(results[1]["createdAt"], "2024-01-03");
    }
}
