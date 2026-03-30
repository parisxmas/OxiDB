//! B-tree-native Collection implementation for OxiDB.
//!
//! Uses `BTreeStorage` directly as the primary document store, bypassing the
//! append-only `StorageBackend` adapter pattern. Key advantages:
//!
//! - **No pidx/primary_index** — the B-tree IS the primary index (doc_id → bytes)
//! - **No WAL** — B-tree updates are atomic; persistence via `persist()`
//! - **Cursor-based scan** — `scan_all_while` walks the B-tree in key order,
//!   yielding page-sequential access (optimal I/O pattern)
//! - **No DocLocation** — doc_id is the key; B-tree finds it in O(log n)
//! - **LRU doc cache** — decoded `Arc<Value>` cache avoids repeated deserialization

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use parking_lot::{Mutex, RwLock};
use rayon::prelude::*;
use serde_json::Value;

use crate::btree_storage::BTreeStorage;
use crate::codec;
use crate::collection::{CompactStats, IndexInfo, IndexMetadata, load_index_metadata, resolve_field_in_value};
use crate::index_persist;
use crate::in_memory::WalBackend;
use crate::wal::{Wal, WalEntry};
use crate::doc_cache::DocCache;
use crate::document::DocumentId;
use crate::error::{Error, Result};
use crate::fts::CollectionTextIndex;
use crate::index::CompositeIndex;
use crate::paged_field_index::PagedFieldIndex;
use crate::query::{self, FindOptions, Query, SortOrder};
use crate::value::IndexValue;
use crate::vector::{DistanceMetric, VectorIndex};

/// A B-tree-native collection that stores documents directly in a `BTreeStorage`.
///
/// Unlike `Collection` which uses append-only storage + primary index HashMap,
/// this struct uses the B-tree as both storage AND primary index.
///
/// All fields with interior mutability — callers use `&self` for everything.
pub struct BTreeCollection {
    #[allow(dead_code)]
    name: String,
    #[allow(dead_code)]
    data_dir: PathBuf,
    storage: BTreeStorage,                                     // Already thread-safe (scc::HashMap)
    field_indexes: RwLock<HashMap<String, PagedFieldIndex>>,
    composite_indexes: RwLock<Vec<CompositeIndex>>,
    text_index: RwLock<Option<CollectionTextIndex>>,
    vector_indexes: RwLock<HashMap<String, VectorIndex>>,
    doc_cache: DocCache,                                       // Already thread-safe
    next_id: AtomicU64,                                        // Already atomic
    #[allow(dead_code)]
    in_memory: bool,
    ttl_index: Mutex<std::collections::BTreeMap<u64, Vec<DocumentId>>>,
    /// Dirty flag: set on write, cleared after persist.
    dirty: AtomicBool,
    wal: WalBackend,
}

impl BTreeCollection {
    /// Create a new B-tree-backed collection (file mode).
    pub fn open(name: &str, data_dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        let storage = BTreeStorage::open(name, data_dir)?;

        let wal_path = data_dir.join(format!("{}.wal", name));
        let wal = Wal::open(&wal_path)?;
        let replayed = Self::replay_wal(&wal, &storage)?;
        let wal_backend = WalBackend::File(wal);
        if replayed > 0 {
            storage.persist()?;
            if let WalBackend::File(ref w) = wal_backend { w.checkpoint_no_sync()?; }
        }

        // Determine next_id by scanning the tree for the max key
        let mut max_id: u64 = 0;
        storage.scan_all_while(|key, _bytes| {
            if key > max_id {
                max_id = key;
            }
            Ok(true)
        })?;

        // Load persisted index definitions
        let idx_path = data_dir.join(format!("{}.idx", name));
        let persisted_indexes = load_index_metadata(&idx_path).unwrap_or_default();

        // Try loading cached index data
        let doc_count = storage.count() as u64;
        let fidx_path = data_dir.join(format!("{}.fidx", name));
        let loaded_field_indexes = if !persisted_indexes.is_empty() {
            index_persist::load_field_indexes(&fidx_path, doc_count, max_id + 1, None)
        } else {
            None
        };

        let field_indexes = if let Some(loaded) = loaded_field_indexes {
            // Loaded from cache — build HashMap from the loaded vec
            let mut map = HashMap::new();
            for idx in loaded {
                map.insert(idx.field.clone(), idx);
            }
            map
        } else if !persisted_indexes.is_empty() {
            // Metadata exists but cache stale — rebuild from B-tree
            let mut map = HashMap::new();
            for info in &persisted_indexes {
                if info.index_type == "field" || info.index_type == "unique" {
                    let field = &info.fields[0];
                    let mut idx = PagedFieldIndex::new(field.clone());
                    if info.unique { idx.unique = true; }
                    // Build index by scanning all docs
                    let mut pairs: Vec<(IndexValue, DocumentId)> = Vec::new();
                    storage.scan_all_while(|id, bytes| {
                        if let Ok(doc) = crate::codec::decode_doc(bytes) {
                            if let Some(val) = resolve_field_in_value(&doc, field) {
                                pairs.push((IndexValue::from_json(val), id));
                            }
                        }
                        Ok(true)
                    })?;
                    pairs.sort_by(|(a, _), (b, _)| a.cmp(b));
                    idx.build_from_sorted(pairs);
                    map.insert(field.clone(), idx);
                }
            }
            map
        } else {
            HashMap::new()
        };

        let cidx_path = data_dir.join(format!("{}.cidx", name));
        let composite_indexes = if !persisted_indexes.is_empty() {
            let loaded = index_persist::load_composite_indexes(&cidx_path, doc_count, max_id + 1, None);
            loaded.unwrap_or_default()
        } else {
            Vec::new()
        };

        Ok(Self {
            name: name.to_string(),
            data_dir: data_dir.to_path_buf(),
            storage,
            field_indexes: RwLock::new(field_indexes),
            composite_indexes: RwLock::new(composite_indexes),
            text_index: RwLock::new(None),
            vector_indexes: RwLock::new(HashMap::new()),
            doc_cache: DocCache::new(crate::doc_cache::DEFAULT_CAPACITY),
            next_id: AtomicU64::new(max_id + 1),
            in_memory: false,
            ttl_index: Mutex::new(std::collections::BTreeMap::new()),
            dirty: AtomicBool::new(false),
            wal: wal_backend,
        })
    }

    /// Create a pure in-memory B-tree collection.
    pub fn open_in_memory(name: &str) -> Self {
        Self {
            name: name.to_string(),
            data_dir: PathBuf::new(),
            storage: BTreeStorage::new_in_memory(name),
            field_indexes: RwLock::new(HashMap::new()),
            composite_indexes: RwLock::new(Vec::new()),
            text_index: RwLock::new(None),
            vector_indexes: RwLock::new(HashMap::new()),
            doc_cache: DocCache::new(crate::doc_cache::DEFAULT_CAPACITY),
            next_id: AtomicU64::new(1),
            in_memory: true,
            ttl_index: Mutex::new(std::collections::BTreeMap::new()),
            dirty: AtomicBool::new(false),
            wal: WalBackend::Memory,
        }
    }

    fn replay_wal(wal: &Wal, storage: &BTreeStorage) -> Result<usize> {
        let entries = wal.read_entries()?;
        if entries.is_empty() { return Ok(0); }
        let mut count = 0usize;
        for entry in &entries {
            match entry {
                WalEntry::Insert { doc_id, doc_bytes, .. }
                | WalEntry::Update { doc_id, doc_bytes, .. } => {
                    storage.insert(*doc_id, doc_bytes.clone());
                    count += 1;
                }
                WalEntry::Delete { doc_id, .. } => {
                    storage.remove(*doc_id);
                    count += 1;
                }
            }
        }
        Ok(count)
    }

    // -----------------------------------------------------------------------
    // Configuration (matching Collection API)
    // -----------------------------------------------------------------------

    pub fn set_lazy_sync(&self, _enabled: bool) {
        // B-tree handles its own durability; this is a no-op.
    }

    pub fn set_cache_capacity(&self, capacity: usize) {
        self.doc_cache.resize(capacity);
    }

    pub fn sync_writes(&self) -> Result<()> {
        if !self.dirty.swap(false, Ordering::AcqRel) {
            return Ok(()); // nothing changed since last persist
        }
        self.storage.persist()?;
        self.wal.checkpoint_no_sync()?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Accessor methods (matching Collection API)
    // -----------------------------------------------------------------------

    pub fn field_indexes(&self) -> parking_lot::RwLockReadGuard<'_, HashMap<String, PagedFieldIndex>> {
        self.field_indexes.read()
    }

    pub fn composite_indexes(&self) -> parking_lot::RwLockReadGuard<'_, Vec<CompositeIndex>> {
        self.composite_indexes.read()
    }

    pub fn has_text_index(&self) -> bool {
        self.text_index.read().is_some()
    }

    pub fn vector_indexes(&self) -> parking_lot::RwLockReadGuard<'_, HashMap<String, VectorIndex>> {
        self.vector_indexes.read()
    }

    pub fn save_index_data(&self) {
        if self.in_memory || self.data_dir.as_os_str().is_empty() {
            return;
        }
        let doc_count = self.storage.count() as u64;
        let next_id = self.next_id.load(Ordering::Acquire);

        // Flush write buffers and save field indexes
        {
            let mut fi = self.field_indexes.write();
            for idx in fi.values_mut() {
                idx.flush_write_buffer();
            }
            let fidx_path = self.data_dir.join(format!("{}.fidx", self.name));
            let field_refs: Vec<&PagedFieldIndex> = fi.values().collect();
            let _ = index_persist::save_field_indexes(&fidx_path, &field_refs, doc_count, next_id, None);
        }

        // Save composite indexes
        {
            let ci = self.composite_indexes.read();
            if !ci.is_empty() {
                let cidx_path = self.data_dir.join(format!("{}.cidx", self.name));
                let comp_refs: Vec<&CompositeIndex> = ci.iter().collect();
                let _ = index_persist::save_composite_indexes(&cidx_path, &comp_refs, doc_count, next_id, None);
            }
        }
    }

    fn save_index_metadata(&self) -> Result<()> {
        if self.in_memory || self.data_dir.as_os_str().is_empty() {
            return Ok(());
        }
        let meta = IndexMetadata {
            version: 1,
            indexes: self.list_indexes(),
        };
        let bytes = serde_json::to_vec(&meta)?;
        let path = self.data_dir.join(format!("{}.idx", self.name));
        std::fs::write(&path, &bytes)?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Document loading helpers
    // -----------------------------------------------------------------------

    /// Load a document by ID, using the LRU cache.
    pub fn load_doc_arc(&self, id: DocumentId) -> Option<Arc<Value>> {
        // Fast path: cache hit
        if let Some(arc) = self.doc_cache.get(id) {
            return Some(arc);
        }
        // Slow path: B-tree lookup + decode
        let bytes = self.storage.get(id)?;
        let doc = codec::decode_doc(&bytes).ok()?;
        let arc = Arc::new(doc);
        self.doc_cache.put(id, Arc::clone(&arc));
        Some(arc)
    }

    /// Read a document by ID (cloned Value).
    fn read_doc(&self, id: DocumentId) -> Result<Option<Value>> {
        Ok(self.load_doc_arc(id).map(|arc| (*arc).clone()))
    }

    /// Read a document by ID, returning an Arc reference.
    fn read_doc_arc(&self, id: DocumentId) -> Option<Arc<Value>> {
        self.load_doc_arc(id)
    }

    // -----------------------------------------------------------------------
    // Scanning helpers — the KEY advantage of B-tree collection
    // -----------------------------------------------------------------------

    /// Iterate all documents via B-tree cursor (sequential page scan).
    /// Each call reads from the page cache. Stops if `f` returns `Ok(false)`.
    fn for_each_doc_arc_while<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(DocumentId, &Arc<Value>) -> Result<bool>,
    {
        self.storage.scan_all_while(|key, bytes| {
            let doc = codec::decode_doc(bytes)?;
            let arc = Arc::new(doc);
            self.doc_cache.put(key, Arc::clone(&arc));
            f(key, &arc)
        })
    }

    /// Stream all documents sequentially via B-tree cursor.
    /// Does NOT populate the LRU cache (avoids thrashing for large scans).
    /// Collect all doc IDs from storage (lightweight, no value cloning).
    fn collect_doc_ids(&self) -> Vec<u64> {
        let mut ids = Vec::new();
        self.storage.scan_keys(|k| ids.push(k));
        ids
    }

    fn for_each_doc_streaming<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&Value) -> Result<bool>,
    {
        // Sorted iteration (preserves key order for find queries)
        self.storage.scan_all_while(|key, bytes| {
            if let Some(arc) = self.doc_cache.get(key) {
                return f(&arc);
            }
            let doc = codec::decode_doc(bytes)?;
            f(&doc)
        })
    }

    /// Unsorted doc iteration — faster for aggregation where order doesn't matter.
    fn for_each_doc_unordered<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&Value) -> Result<bool>,
    {
        self.storage.for_each_value(|key, bytes| {
            if let Some(arc) = self.doc_cache.get(key) {
                return f(&arc);
            }
            let doc = codec::decode_doc(bytes)?;
            f(&doc)
        })
    }

    // -----------------------------------------------------------------------
    // Unique constraint checking
    // -----------------------------------------------------------------------

    /// Check unique constraints. Caller must hold (or pass) a read guard on field_indexes.
    fn check_unique_constraints_with(
        fi: &HashMap<String, PagedFieldIndex>,
        data: &Value,
        exclude_id: Option<DocumentId>,
    ) -> Result<()> {
        for idx in fi.values() {
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

    pub fn check_unique_constraints(
        &self,
        data: &Value,
        exclude_id: Option<DocumentId>,
    ) -> Result<()> {
        let fi = self.field_indexes.read();
        Self::check_unique_constraints_with(&fi, data, exclude_id)
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

        // Inject _id and _version
        let obj = data.as_object_mut().unwrap();
        obj.insert("_id".to_string(), Value::Number(id.into()));
        obj.insert("_version".to_string(), Value::Number(1.into()));

        // Check unique constraints BEFORE any mutation
        let mut fi = self.field_indexes.write();
        Self::check_unique_constraints_with(&fi, &data, None)?;

        let bytes = codec::encode_doc(&data)?;

        // WAL log before mutation
        self.wal.log_no_sync(&WalEntry::Insert { doc_id: id, doc_bytes: bytes.clone(), tx_id: 0 })?;

        // Insert directly into B-tree (the B-tree IS the storage)
        self.storage.insert(id, bytes);

        let data_arc = Arc::new(data);

        // Update field indexes
        for idx in fi.values_mut() {
            idx.insert_value(id, &data_arc);
        }
        {
            let mut ci = self.composite_indexes.write();
            for idx in ci.iter_mut() {
                idx.insert_value(id, &data_arc);
            }
        }
        {
            let mut ti = self.text_index.write();
            if let Some(ref mut text_idx) = *ti {
                text_idx.index_doc(id, &data_arc);
            }
        }
        {
            let mut vi = self.vector_indexes.write();
            for idx in vi.values_mut() {
                let _ = idx.insert(id, &data_arc);
            }
        }

        // TTL
        self.register_ttl(id, &data_arc);

        self.doc_cache.put(id, data_arc);
        self.dirty.store(true, Ordering::Release);

        Ok(id)
    }

    /// Insert multiple documents atomically.
    pub fn insert_many(&self, docs: Vec<Value>) -> Result<Vec<DocumentId>> {
        if docs.is_empty() {
            return Ok(vec![]);
        }

        let mut fi = self.field_indexes.write();
        let has_unique = fi.values().any(|idx| idx.unique);
        let has_field_indexes = !fi.is_empty();

        let mut ci = self.composite_indexes.write();
        let has_composite = !ci.is_empty();

        let mut ti = self.text_index.write();
        let has_text = ti.is_some();

        let mut vi = self.vector_indexes.write();
        let has_vector = !vi.is_empty();

        let has_indexes = has_field_indexes || has_composite || has_text || has_vector;

        // Phase 1: assign IDs and prepare docs (parallel JSONB encode)
        let first_id = self.next_id.load(Ordering::SeqCst);
        let doc_count = docs.len();

        // Assign IDs first
        let mut docs_with_ids: Vec<(u64, Value)> = Vec::with_capacity(doc_count);
        for (i, mut data) in docs.into_iter().enumerate() {
            if !data.is_object() {
                return Err(Error::NotAnObject);
            }
            let id = first_id + i as u64;
            let obj = data.as_object_mut().unwrap();
            obj.insert("_id".to_string(), Value::Number(id.into()));
            obj.insert("_version".to_string(), Value::Number(1.into()));
            docs_with_ids.push((id, data));
        }

        // Unique constraint check (only when needed)
        if has_unique {
            let mut pending_unique: HashMap<String, HashMap<IndexValue, DocumentId>> = HashMap::new();
            for (id, data) in &docs_with_ids {
                Self::check_unique_constraints_with(&fi, data, None)?;
                for idx in fi.values() {
                    if !idx.unique { continue; }
                    if let Some(value) = resolve_field_in_value(data, &idx.field) {
                        let iv = IndexValue::from_json(value);
                        let field_map = pending_unique.entry(idx.field.clone()).or_default();
                        if field_map.contains_key(&iv) {
                            return Err(Error::UniqueViolation { field: idx.field.clone() });
                        }
                        field_map.insert(iv, *id);
                    }
                }
            }
        }

        // Parallel JSONB encode
        let btree_entries: Vec<(u64, Vec<u8>)> = if doc_count > 500 {
            docs_with_ids.par_iter()
                .map(|(id, data)| (*id, codec::encode_doc(data).unwrap_or_default()))
                .collect()
        } else {
            docs_with_ids.iter()
                .map(|(id, data)| (*id, codec::encode_doc(data).unwrap_or_default()))
                .collect()
        };

        // Phase 2: bulk insert into B-tree
        // Pre-reserve full capacity to avoid incremental reallocation across batches
        if self.storage.count() == 0 {
            self.storage.reserve(doc_count);
        }
        let ids: Vec<DocumentId> = btree_entries.iter().map(|(id, _)| *id).collect();
        {
            let wal_refs: Vec<(u64, &[u8])> = btree_entries.iter().map(|(id, b)| (*id, b.as_slice())).collect();
            self.wal.log_batch_inserts_no_sync_buffered(&wal_refs)?;
        }
        self.storage.insert_batch(btree_entries);

        // Phase 3: update indexes and cache
        // Skip cache population during bulk insert when no indexes need it
        let skip_cache = doc_count > 5_000 || !has_indexes;

        // Keep data values for index updates and caching
        let data_values: Vec<(u64, Value)> = if has_indexes || !skip_cache {
            docs_with_ids
        } else {
            Vec::new()
        };

        if has_indexes || !skip_cache {
            for (id, data) in data_values {
                let data_arc = Arc::new(data);
                for idx in fi.values_mut() {
                    idx.insert_value(id, &data_arc);
                }
                for idx in ci.iter_mut() {
                    idx.insert_value(id, &data_arc);
                }
                if let Some(ref mut text_idx) = *ti {
                    text_idx.index_doc(id, &data_arc);
                }
                for idx in vi.values_mut() {
                    let _ = idx.insert(id, &data_arc);
                }
                if !skip_cache {
                    self.doc_cache.put(id, data_arc);
                }
            }
        }

        self.next_id.store(first_id + doc_count as u64, Ordering::SeqCst);

        Ok(ids)
    }

    /// Reserve a contiguous block of document IDs.
    pub fn reserve_ids(&self, count: u64) -> DocumentId {
        self.next_id.fetch_add(count, Ordering::SeqCst)
    }

    /// Insert pre-serialized documents (used by engine's insert_many path).
    pub fn insert_many_prepared(
        &self,
        prepared: Vec<(DocumentId, Value, Vec<u8>)>,
    ) -> Result<Vec<DocumentId>> {
        if prepared.is_empty() {
            return Ok(vec![]);
        }

        let mut fi = self.field_indexes.write();
        let mut ci = self.composite_indexes.write();
        let mut ti = self.text_index.write();
        let mut vi = self.vector_indexes.write();

        let mut ids = Vec::with_capacity(prepared.len());
        {
            let wal_refs: Vec<(u64, &[u8])> = prepared.iter().map(|(id, _, b)| (*id, b.as_slice())).collect();
            self.wal.log_batch_inserts_no_sync_buffered(&wal_refs)?;
        }
        let has_indexes = !fi.is_empty() || !ci.is_empty() || ti.is_some() || !vi.is_empty();
        let skip_cache = prepared.len() > 1000;
        for (id, data, bytes) in prepared {
            self.storage.insert(id, bytes);

            if has_indexes || !skip_cache {
                let data_arc = Arc::new(data);
                for idx in fi.values_mut() {
                    idx.insert_value(id, &data_arc);
                }
                for idx in ci.iter_mut() {
                    idx.insert_value(id, &data_arc);
                }
                if let Some(ref mut text_idx) = *ti {
                    text_idx.index_doc(id, &data_arc);
                }
                for idx in vi.values_mut() {
                    let _ = idx.insert(id, &data_arc);
                }
                if !skip_cache {
                    self.doc_cache.put(id, data_arc);
                }
            }
            ids.push(id);
        }
        if !ids.is_empty() {
            self.dirty.store(true, Ordering::Release);
        }

        Ok(ids)
    }

    // -----------------------------------------------------------------------
    // Find operations
    // -----------------------------------------------------------------------

    /// Find documents matching a query.
    pub fn find(&self, query_json: &Value) -> Result<Vec<Value>> {
        self.find_with_options(query_json, &FindOptions::default())
    }

    /// Find documents returning Arc references.
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
        Ok(arcs
            .into_iter()
            .map(|a| Arc::try_unwrap(a).unwrap_or_else(|a| (*a).clone()))
            .collect())
    }

    /// Find documents with options, returning Arc references.
    pub fn find_with_options_arcs(
        &self,
        query_json: &Value,
        opts: &FindOptions,
    ) -> Result<Vec<Arc<Value>>> {
        let query = query::parse_query(query_json)?;

        // Fast path: Query::All with no sort — use B-tree cursor scan
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

        // Acquire read lock on field_indexes for the duration of index-accelerated paths
        let fi = self.field_indexes.read();

        // Fast path: index-backed sort with early termination
        if let Some(sort_fields) = &opts.sort {
            if sort_fields.len() == 1 {
                let (sort_field, sort_order) = &sort_fields[0];
                if let Some(field_idx) = fi.get(sort_field) {
                    let need = opts.skip.unwrap_or(0) as usize
                        + opts.limit.unwrap_or(u64::MAX) as usize;
                    let mut results = Vec::new();
                    let skip_filter = matches!(query, Query::All);

                    // Extract eq conditions for index-level containment check
                    let eq_checks: Vec<(String, IndexValue)> = if !skip_filter {
                        query::extract_eq_conditions(&query)
                            .map(|m| m.into_iter()
                                .filter(|(f, _)| fi.contains_key(f) && f != sort_field)
                                .collect())
                            .unwrap_or_default()
                    } else {
                        Vec::new()
                    };
                    let use_index_check = !eq_checks.is_empty();

                    let check_id = |id: DocumentId| -> bool {
                        for (field, value) in &eq_checks {
                            if let Some(idx) = fi.get(field.as_str()) {
                                if !idx.contains_doc_id(value, id) {
                                    return false;
                                }
                            }
                        }
                        true
                    };

                    match sort_order {
                        SortOrder::Asc => {
                            'outer_asc: for (_value, doc_ids) in field_idx.iter_asc() {
                                for &id in doc_ids {
                                    if use_index_check && !check_id(id) { continue; }
                                    if let Some(arc) = self.read_doc_arc(id) {
                                        if skip_filter || use_index_check || query::matches_value(&query, &arc) {
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
                                    if use_index_check && !check_id(id) { continue; }
                                    if let Some(arc) = self.read_doc_arc(id) {
                                        if skip_filter || use_index_check || query::matches_value(&query, &arc) {
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

        // Composite index-backed sort
        if let Some(sort_fields) = &opts.sort {
            if sort_fields.len() == 1 {
                let (sort_field, sort_order) = &sort_fields[0];
                if let Some(eq_conds) = query::extract_eq_conditions(&query) {
                    let ci = self.composite_indexes.read();
                    for comp_idx in ci.iter() {
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

        // Standard path: index-accelerated or full B-tree cursor scan
        let skip_post_filter = query::is_fully_indexed(&query, &fi);

        let early_limit: Option<usize> = if opts.sort.is_none() && opts.skip.is_none() {
            opts.limit.map(|l| l as usize)
        } else {
            None
        };

        let mut results = Vec::new();

        // Try lazy index iteration for limit queries
        if let Some(limit) = early_limit {
            let lazy_result = query::execute_indexed_lazy(
                &query,
                &fi,
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

        let ci = self.composite_indexes.read();
        let candidate_ids = query::execute_indexed(
            &query,
            &fi,
            &ci,
        );
        drop(ci);

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
            // Drop the field_indexes read lock before full scan
            drop(fi);

            // No index — parallel B-tree scan using rayon
            if early_limit.is_some() {
                // With early limit (no sort + limit) — keep sequential for early termination
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
            } else {
                // Full scan — collect doc_ids then parallel filter
                let doc_ids: Vec<u64> = {
                    let mut ids = Vec::new();
                    self.storage.scan_keys(|k| { ids.push(k); });
                    ids
                };
                let matched: Vec<Arc<Value>> = doc_ids
                    .par_iter()
                    .filter_map(|&id| {
                        let arc = self.load_doc_arc(id)?;
                        if query::matches_value(&query, &arc) {
                            Some(arc)
                        } else {
                            None
                        }
                    })
                    .collect();
                results = matched;
            }

            // Re-acquire for the sort phase below (but only if we need sort and don't have fi)
            // Actually, sort doesn't use field_indexes, so no need to re-acquire.
        }

        // Apply sort -> skip -> limit
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

        let fi = self.field_indexes.read();
        let skip_post_filter = query::is_fully_indexed(&query, &fi);

        // Try lazy index path first
        if !matches!(query, Query::All) {
            let mut found: Option<Value> = None;
            let lazy_result = query::execute_indexed_lazy(
                &query,
                &fi,
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

        // Fallback: index or cursor scan
        let candidate_ids = if !matches!(query, Query::All) {
            let ci = self.composite_indexes.read();
            query::execute_indexed(
                &query,
                &fi,
                &ci,
            )
        } else {
            None
        };

        // Drop read locks before potential full scan
        drop(fi);

        if let Some(ref indexed_ids) = candidate_ids {
            for &id in indexed_ids {
                if self.storage.contains_key(id) {
                    if let Some(data) = self.read_doc(id)? {
                        if skip_post_filter || query::matches_value(&query, &data) {
                            return Ok(Some(data));
                        }
                    }
                }
            }
        } else {
            // No index — B-tree cursor scan
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
    // Update
    // -----------------------------------------------------------------------

    /// Update documents matching a query. Returns IDs of updated documents.
    pub fn update(
        &self,
        query_json: &Value,
        update_json: &Value,
        limit: Option<usize>,
    ) -> Result<Vec<DocumentId>> {
        let update_obj = update_json
            .as_object()
            .ok_or_else(|| Error::InvalidQuery("update must be an object".into()))?;
        if update_obj.is_empty() {
            return Err(Error::InvalidQuery(
                "update must contain at least one operator".into(),
            ));
        }

        let query = query::parse_query(query_json)?;

        // Phase 1: Find matching docs (read lock on indexes)
        let mut matches: Vec<(DocumentId, Value)> = Vec::new();
        {
            let fi = self.field_indexes.read();
            let mut lazy_handled = false;
            if limit.is_some() {
                let skip_post_filter = query::is_fully_indexed(&query, &fi);
                let lim = limit.unwrap();
                let lazy_result = query::execute_indexed_lazy(
                    &query,
                    &fi,
                    &mut |id| {
                        if self.storage.contains_key(id) {
                            if let Some(arc) = self.load_doc_arc(id) {
                                if skip_post_filter || query::matches_value(&query, &arc) {
                                    matches.push((id, (*arc).clone()));
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
                let ci = self.composite_indexes.read();
                let candidate_ids = query::execute_indexed(
                    &query,
                    &fi,
                    &ci,
                );
                drop(ci);

                if let Some(ref indexed_ids) = candidate_ids {
                    for &id in indexed_ids {
                        if self.storage.contains_key(id) {
                            if let Some(data) = self.read_doc(id)? {
                                if query::matches_value(&query, &data) {
                                    matches.push((id, data));
                                    if limit.is_some_and(|l| matches.len() >= l) {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                } else {
                    // Drop fi before full scan to avoid holding lock during I/O
                    drop(fi);
                    // B-tree cursor scan
                    self.for_each_doc_arc_while(|id, arc| {
                        if query::matches_value(&query, arc) {
                            matches.push((id, (**arc).clone()));
                            if limit.is_some_and(|l| matches.len() >= l) {
                                return Ok(false);
                            }
                        }
                        Ok(true)
                    })?;
                }
            }
        } // fi read lock released (if not already dropped)

        if matches.is_empty() {
            return Ok(Vec::new());
        }

        // Phase 2: Prepare and validate updates (no lock needed)
        struct UpdateOp {
            id: DocumentId,
            old_data: Value,
            new_data: Value,
            new_bytes: Vec<u8>,
        }

        let mut ops = Vec::with_capacity(matches.len());

        // We need to check if we have indexes for old_data tracking — peek under a brief read lock
        let need_old_data = {
            let fi = self.field_indexes.read();
            let ci = self.composite_indexes.read();
            !fi.is_empty() || !ci.is_empty()
        };

        for (id, mut data) in matches {
            // Save old data BEFORE mutation for index removal (avoids a second load_doc_arc call)
            let old_data = if need_old_data {
                data.clone()
            } else {
                Value::Null
            };

            crate::update::apply_update(&mut data, update_json)?;

            let old_version = data
                .get("_version")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let new_version = old_version + 1;
            if let Some(obj) = data.as_object_mut() {
                obj.insert("_version".to_string(), Value::Number(new_version.into()));
            }

            let new_bytes = codec::encode_doc(&data)?;

            ops.push(UpdateOp {
                id,
                old_data,
                new_data: data,
                new_bytes,
            });
        }

        if ops.is_empty() {
            return Ok(Vec::new());
        }

        // Phase 3: Apply to B-tree and update indexes (write locks)
        let mut fi = self.field_indexes.write();
        let mut ci = self.composite_indexes.write();
        let mut ti = self.text_index.write();
        let mut vi = self.vector_indexes.write();

        // Unique constraint check under write lock
        let has_unique = fi.values().any(|idx| idx.unique);
        if has_unique {
            for op in &ops {
                Self::check_unique_constraints_with(&fi, &op.new_data, Some(op.id))?;
            }
        }

        {
            let wal_entries: Vec<WalEntry> = ops.iter().map(|op| WalEntry::Update { doc_id: op.id, doc_bytes: op.new_bytes.clone(), tx_id: 0 }).collect();
            self.wal.log_batch_no_sync(&wal_entries)?;
        }

        let mut updated_ids = Vec::with_capacity(ops.len());
        for op in ops {
            // Update B-tree in-place (replace value)
            self.storage.insert(op.id, op.new_bytes);

            // Update field indexes — only for fields whose value changed
            for idx in fi.values_mut() {
                let old_val = crate::collection::resolve_field_in_value(&op.old_data, &idx.field);
                let new_val = crate::collection::resolve_field_in_value(&op.new_data, &idx.field);
                if old_val != new_val {
                    idx.remove_value(op.id, &op.old_data);
                    idx.insert_value(op.id, &op.new_data);
                }
            }
            for idx in ci.iter_mut() {
                idx.remove_value(op.id, &op.old_data);
                idx.insert_value(op.id, &op.new_data);
            }
            if let Some(ref mut text_idx) = *ti {
                text_idx.index_doc(op.id, &op.new_data);
            }
            for idx in vi.values_mut() {
                idx.remove(op.id);
                let _ = idx.insert(op.id, &op.new_data);
            }

            self.doc_cache.put(op.id, Arc::new(op.new_data));
            updated_ids.push(op.id);
        }
        if !updated_ids.is_empty() {
            self.dirty.store(true, Ordering::Release);
        }

        Ok(updated_ids)
    }

    // -----------------------------------------------------------------------
    // Delete
    // -----------------------------------------------------------------------

    /// Delete documents matching a query. Returns IDs of deleted documents.
    pub fn delete(
        &self,
        query_json: &Value,
        limit: Option<usize>,
    ) -> Result<Vec<DocumentId>> {
        let query = query::parse_query(query_json)?;

        // Phase 1: Find matching docs
        struct DeleteOp {
            id: DocumentId,
            data: Value,
        }
        let mut ops = Vec::new();

        {
            let fi = self.field_indexes.read();
            let mut lazy_handled = false;
            if limit.is_some() {
                let skip_post_filter = query::is_fully_indexed(&query, &fi);
                let lim = limit.unwrap();
                let lazy_result = query::execute_indexed_lazy(
                    &query,
                    &fi,
                    &mut |id| {
                        if let Some(arc) = self.load_doc_arc(id) {
                            if skip_post_filter || query::matches_value(&query, &arc) {
                                if self.storage.contains_key(id) {
                                    ops.push(DeleteOp {
                                        id,
                                        data: (*arc).clone(),
                                    });
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
                let ci = self.composite_indexes.read();
                let candidate_ids = query::execute_indexed(
                    &query,
                    &fi,
                    &ci,
                );
                drop(ci);

                if let Some(ref indexed_ids) = candidate_ids {
                    for &id in indexed_ids {
                        if self.storage.contains_key(id) {
                            if let Some(data) = self.read_doc(id)? {
                                if query::matches_value(&query, &data) {
                                    ops.push(DeleteOp { id, data });
                                    if limit.is_some_and(|l| ops.len() >= l) {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                } else {
                    // Drop fi before full scan
                    drop(fi);
                    // B-tree cursor scan
                    self.for_each_doc_arc_while(|id, arc| {
                        if query::matches_value(&query, arc) {
                            ops.push(DeleteOp {
                                id,
                                data: (**arc).clone(),
                            });
                            if limit.is_some_and(|l| ops.len() >= l) {
                                return Ok(false);
                            }
                        }
                        Ok(true)
                    })?;
                }
            }
        } // fi read lock released (if not already dropped)

        if ops.is_empty() {
            return Ok(Vec::new());
        }

        // Phase 2: Remove from B-tree and update indexes (write locks)
        {
            let wal_entries: Vec<WalEntry> = ops.iter().map(|op| WalEntry::Delete { doc_id: op.id, tx_id: 0 }).collect();
            self.wal.log_batch_no_sync(&wal_entries)?;
        }

        let mut fi = self.field_indexes.write();
        let mut ci = self.composite_indexes.write();
        let mut ti = self.text_index.write();
        let mut vi = self.vector_indexes.write();

        let mut deleted_ids = Vec::with_capacity(ops.len());
        for op in ops {
            // Remove from B-tree (no soft-delete — immediate reclaim)
            self.storage.remove(op.id);
            self.doc_cache.remove(op.id);

            for idx in fi.values_mut() {
                idx.remove_value(op.id, &op.data);
            }
            for idx in ci.iter_mut() {
                idx.remove_value(op.id, &op.data);
            }
            if let Some(ref mut text_idx) = *ti {
                text_idx.remove_doc(op.id);
            }
            for idx in vi.values_mut() {
                idx.remove(op.id);
            }

            deleted_ids.push(op.id);
        }
        if !deleted_ids.is_empty() {
            self.dirty.store(true, Ordering::Release);
        }

        Ok(deleted_ids)
    }

    // -----------------------------------------------------------------------
    // Count
    // -----------------------------------------------------------------------

    /// Returns the total number of documents in the collection.
    pub fn count(&self) -> usize {
        self.storage.count()
    }

    /// Count documents matching a query.
    pub fn count_matching(&self, query_json: &Value) -> Result<usize> {
        let query = query::parse_query(query_json)?;

        let fi = self.field_indexes.read();

        // Fast path: count from index
        if let Some(count) = query::count_indexed(&query, &fi) {
            return Ok(count);
        }

        let ci = self.composite_indexes.read();
        let candidate_ids = query::execute_indexed(
            &query,
            &fi,
            &ci,
        );
        drop(ci);

        let skip_post_filter = query::is_fully_indexed(&query, &fi);

        let mut count = 0;
        if let Some(ref indexed_ids) = candidate_ids {
            if skip_post_filter {
                return Ok(indexed_ids.len());
            }
            for &id in indexed_ids {
                if let Some(arc) = self.load_doc_arc(id) {
                    if query::matches_value(&query, &arc) {
                        count += 1;
                    }
                }
            }
        } else {
            // Drop fi before full scan
            drop(fi);

            // Parallel scan using doc_cache (avoids cloning raw bytes)
            let doc_ids: Vec<u64> = {
                let mut ids = Vec::new();
                self.storage.scan_keys(|k| { ids.push(k); });
                ids
            };
            count = doc_ids
                .par_iter()
                .filter(|&&id| {
                    if let Some(arc) = self.load_doc_arc(id) {
                        query::matches_value(&query, &arc)
                    } else {
                        false
                    }
                })
                .count();
        }

        Ok(count)
    }

    // -----------------------------------------------------------------------
    // Compact
    // -----------------------------------------------------------------------

    /// Compact the B-tree storage.
    /// Since B-tree has no soft-deletes, this is mainly a rebuild/persistence operation.
    pub fn compact(&self) -> Result<CompactStats> {
        let old_size = self.storage.total_bytes();

        // Persist current state to disk
        self.storage.persist()?;

        let new_size = self.storage.total_bytes();
        let docs_kept = self.storage.count();

        Ok(CompactStats {
            old_size,
            new_size,
            docs_kept,
        })
    }

    // -----------------------------------------------------------------------
    // Version
    // -----------------------------------------------------------------------

    pub fn get_version(&self, doc_id: DocumentId) -> u64 {
        // Read version from the document payload in the B-tree (no separate HashMap)
        if let Some(arc) = self.doc_cache.get(doc_id) {
            return arc.get("_version").and_then(|v| v.as_u64()).unwrap_or(0);
        }
        if let Some(bytes) = self.storage.get(doc_id) {
            if let Ok(doc) = codec::decode_doc(&bytes) {
                return doc.get("_version").and_then(|v| v.as_u64()).unwrap_or(0);
            }
        }
        0
    }

    // -----------------------------------------------------------------------
    // Index management
    // -----------------------------------------------------------------------

    /// Create a single-field index. Rebuilds from B-tree cursor scan.
    pub fn create_index(&self, field: &str) -> Result<()> {
        {
            let fi = self.field_indexes.read();
            if fi.contains_key(field) {
                return Ok(());
            }
        }

        // Parallel index build: extract (id, IndexValue) pairs from all docs
        let slices = self.storage.values_as_slices();
        let field_owned = field.to_string();

        let mut pairs: Vec<(IndexValue, u64)> = slices.par_iter()
            .filter_map(|bytes| {
                if !bytes.is_empty() && bytes[0] != b'{' && bytes[0] != b'[' {
                    let raw = jsonb::RawJsonb::new(bytes);
                    let id = extract_raw_u64(&raw, "_id")?;
                    let iv = extract_raw_index_value(&raw, &field_owned)?;
                    Some((iv, id))
                } else {
                    let doc: Value = codec::decode_doc(bytes).ok()?;
                    let id = doc.get("_id")?.as_u64()?;
                    let val = resolve_field_in_value(&doc, &field_owned)?;
                    Some((IndexValue::from_json(val), id))
                }
            })
            .collect();

        // Sort by IndexValue so build_from_sorted can append in order (no O(n) shifts)
        pairs.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));

        let mut idx = PagedFieldIndex::new(field.to_string());
        let sorted_pairs: Vec<(IndexValue, u64)> = pairs;
        idx.build_from_sorted(sorted_pairs.into_iter().map(|(iv, id)| (iv, id)).collect());

        let mut fi = self.field_indexes.write();
        fi.insert(field.to_string(), idx);
        drop(fi);
        let _ = self.save_index_metadata();
        Ok(())
    }

    /// Create a unique single-field index.
    pub fn create_unique_index(&self, field: &str) -> Result<()> {
        {
            let fi = self.field_indexes.read();
            if fi.contains_key(field) {
                return Ok(());
            }
        }

        let mut idx = PagedFieldIndex::new_unique(field.to_string());

        self.storage.scan_bytes_while(|bytes| {
            if !bytes.is_empty() && bytes[0] != b'{' && bytes[0] != b'[' {
                let raw = jsonb::RawJsonb::new(bytes);
                if let Some(id) = extract_raw_u64(&raw, "_id") {
                    if let Some(iv) = extract_raw_index_value(&raw, field) {
                        // Check uniqueness during build
                        if idx.check_unique(&iv, None) {
                            return Err(Error::UniqueViolation {
                                field: field.to_string(),
                            });
                        }
                        idx.insert_raw(id, iv);
                    }
                }
            } else {
                let doc: Value = codec::decode_doc(bytes)?;
                if let Some(id) = doc.get("_id").and_then(|v| v.as_u64()) {
                    if let Some(value) = resolve_field_in_value(&doc, field) {
                        let iv = IndexValue::from_json(value);
                        if idx.check_unique(&iv, None) {
                            return Err(Error::UniqueViolation {
                                field: field.to_string(),
                            });
                        }
                        idx.insert_raw(id, iv);
                    }
                }
            }
            Ok(true)
        })?;

        let mut fi = self.field_indexes.write();
        fi.insert(field.to_string(), idx);
        drop(fi);
        let _ = self.save_index_metadata();
        Ok(())
    }

    /// Create a composite (multi-field) index.
    pub fn create_composite_index(&self, fields: Vec<String>) -> Result<String> {
        let name = fields.join("_");
        {
            let ci = self.composite_indexes.read();
            if ci.iter().any(|i| i.name() == name) {
                return Ok(name);
            }
        }

        let mut idx = CompositeIndex::new(fields);

        // Backfill
        self.storage.scan_bytes_while(|bytes| {
            let doc: Value = codec::decode_doc(bytes)?;
            if let Some(id) = doc.get("_id").and_then(|v| v.as_u64()) {
                idx.insert_value(id, &doc);
            }
            Ok(true)
        })?;

        let mut ci = self.composite_indexes.write();
        ci.push(idx);
        drop(ci);
        let _ = self.save_index_metadata();
        Ok(name)
    }

    /// List all indexes.
    pub fn list_indexes(&self) -> Vec<IndexInfo> {
        let mut indexes = Vec::new();
        let fi = self.field_indexes.read();
        for idx in fi.values() {
            indexes.push(IndexInfo {
                name: idx.field.clone(),
                index_type: if idx.unique {
                    "unique".to_string()
                } else {
                    "field".to_string()
                },
                fields: vec![idx.field.clone()],
                unique: idx.unique,
                dimension: None,
                metric: None,
            });
        }
        let ci = self.composite_indexes.read();
        for idx in ci.iter() {
            indexes.push(IndexInfo {
                name: idx.name(),
                index_type: "composite".to_string(),
                fields: idx.fields.clone(),
                unique: false,
                dimension: None,
                metric: None,
            });
        }
        let ti = self.text_index.read();
        if let Some(ref text_idx) = *ti {
            indexes.push(IndexInfo {
                name: "_text".to_string(),
                index_type: "text".to_string(),
                fields: text_idx.fields().to_vec(),
                unique: false,
                dimension: None,
                metric: None,
            });
        }
        let vi = self.vector_indexes.read();
        for idx in vi.values() {
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

    /// Drop an index by name.
    pub fn drop_index(&self, name: &str) -> Result<()> {
        {
            let mut fi = self.field_indexes.write();
            if fi.remove(name).is_some() {
                drop(fi);
                let _ = self.save_index_metadata();
                return Ok(());
            }
        }
        {
            let mut ci = self.composite_indexes.write();
            if let Some(pos) = ci.iter().position(|i| i.name() == name) {
                ci.remove(pos);
                drop(ci);
                let _ = self.save_index_metadata();
                return Ok(());
            }
        }
        if name == "_text" {
            let mut ti = self.text_index.write();
            if ti.is_some() {
                *ti = None;
                let _ = self.save_index_metadata();
                return Ok(());
            }
        }
        if let Some(field) = name.strip_prefix("_vec_") {
            let mut vi = self.vector_indexes.write();
            if vi.remove(field).is_some() {
                let _ = self.save_index_metadata();
                return Ok(());
            }
        }
        Err(Error::IndexNotFound(name.to_string()))
    }

    // -----------------------------------------------------------------------
    // Text search
    // -----------------------------------------------------------------------

    pub fn create_text_index(&self, fields: Vec<String>) -> Result<()> {
        {
            let ti = self.text_index.read();
            if ti.is_some() {
                return Ok(());
            }
        }

        let mut idx = CollectionTextIndex::new(fields);

        self.storage.scan_bytes_while(|bytes| {
            let doc: Value = codec::decode_doc(bytes)?;
            if let Some(id) = doc.get("_id").and_then(|v| v.as_u64()) {
                let arc = Arc::new(doc);
                idx.index_doc(id, &arc);
            }
            Ok(true)
        })?;

        let mut ti = self.text_index.write();
        *ti = Some(idx);
        Ok(())
    }

    pub fn text_search(&self, query: &str, limit: usize) -> Result<Vec<Value>> {
        let ti = self.text_index.read();
        let idx = ti.as_ref().ok_or_else(|| {
            Error::InvalidQuery(
                "no text index on this collection; create one with create_text_index".into(),
            )
        })?;

        let search_results = idx.search(query, limit);
        let mut docs = Vec::with_capacity(search_results.len());
        for result in search_results {
            if let Some(mut doc) = self.read_doc(result.doc_id)? {
                if let Some(obj) = doc.as_object_mut() {
                    obj.insert(
                        "_score".to_string(),
                        serde_json::json!(result.score),
                    );
                }
                docs.push(doc);
            }
        }
        Ok(docs)
    }

    // -----------------------------------------------------------------------
    // Vector search
    // -----------------------------------------------------------------------

    pub fn create_vector_index(
        &self,
        field: &str,
        dimension: usize,
        metric: DistanceMetric,
    ) -> Result<()> {
        {
            let vi = self.vector_indexes.read();
            if vi.contains_key(field) {
                return Ok(());
            }
        }

        let mut idx = VectorIndex::new(field.to_string(), dimension, metric);

        self.storage.scan_bytes_while(|bytes| {
            let doc: Value = codec::decode_doc(bytes)?;
            if let Some(id) = doc.get("_id").and_then(|v| v.as_u64()) {
                let arc = Arc::new(doc);
                let _ = idx.insert(id, &arc);
            }
            Ok(true)
        })?;

        let mut vi = self.vector_indexes.write();
        vi.insert(field.to_string(), idx);
        Ok(())
    }

    pub fn vector_search(
        &self,
        field: &str,
        query_vector: &[f32],
        limit: usize,
        ef_search: Option<usize>,
    ) -> Result<Vec<Value>> {
        let vi = self.vector_indexes.read();
        let idx = vi.get(field).ok_or_else(|| {
            Error::InvalidQuery(format!(
                "no vector index on field '{}'; create one with create_vector_index",
                field
            ))
        })?;

        let search_results = idx
            .search(query_vector, limit, ef_search)
            .map_err(Error::InvalidQuery)?;

        let mut docs = Vec::with_capacity(search_results.len());
        for result in search_results {
            if let Some(mut doc) = self.read_doc(result.doc_id)? {
                if let Some(obj) = doc.as_object_mut() {
                    obj.insert(
                        "_similarity".to_string(),
                        serde_json::json!(result.similarity),
                    );
                    obj.insert(
                        "_distance".to_string(),
                        serde_json::json!(result.distance),
                    );
                }
                docs.push(doc);
            }
        }
        Ok(docs)
    }

    // -----------------------------------------------------------------------
    // Aggregation streaming (matching Collection API)
    // -----------------------------------------------------------------------

    /// Execute a streaming group aggregation using B-tree cursor scan.
    #[allow(dead_code)]
    pub(crate) fn aggregate_streaming(
        &self,
        match_query_json: Option<&Value>,
        group_key: &crate::pipeline::GroupKey,
        accumulators: &[(String, crate::pipeline::Accumulator)],
    ) -> Result<Vec<Value>> {
        let mut group =
            crate::pipeline::StreamingGroup::new(group_key, accumulators);

        match match_query_json {
            None => {
                // Sequential streaming — use feed_raw for JSONB partial extraction
                // (extracts only group key + accumulator fields, skips nested arrays)
                self.storage.for_each_value(|key, bytes| {
                    if let Some(arc) = self.doc_cache.get(key) {
                        group.feed(&arc);
                    } else {
                        group.feed_raw(bytes);
                    }
                    Ok(true)
                })?;
            }
            Some(match_val) => {
                let query = query::parse_query(match_val)?;
                if matches!(query, Query::All) {
                    self.storage.for_each_value(|key, bytes| {
                        if let Some(arc) = self.doc_cache.get(key) {
                            group.feed(&arc);
                        } else {
                            group.feed_raw(bytes);
                        }
                        Ok(true)
                    })?;
                } else {
                    // Try index-accelerated path
                    let fi = self.field_indexes.read();
                    let ci = self.composite_indexes.read();
                    let candidate_ids = query::execute_indexed(
                        &query,
                        &fi,
                        &ci,
                    );
                    let skip_post_filter =
                        query::is_fully_indexed(&query, &fi);
                    drop(ci);

                    if let Some(ref ids) = candidate_ids {
                        for &id in ids {
                            if skip_post_filter {
                                // Index guarantees match — use feed_raw (no full decode)
                                if let Some(arc) = self.doc_cache.get(id) {
                                    group.feed(&arc);
                                } else if let Some(bytes) = self.storage.get(id) {
                                    group.feed_raw(&bytes);
                                }
                            } else if let Some(arc) = self.load_doc_arc(id) {
                                if query::matches_value(&query, &arc) {
                                    group.feed(&arc);
                                }
                            }
                        }
                    } else {
                        drop(fi);
                        // Full scan with filter — must decode for match check
                        self.storage.for_each_value(|key, bytes| {
                            if let Some(arc) = self.doc_cache.get(key) {
                                if query::matches_value(&query, &arc) {
                                    group.feed(&arc);
                                }
                            } else if let Ok(doc) = codec::decode_doc(bytes) {
                                if query::matches_value(&query, &doc) {
                                    group.feed(&doc);
                                }
                            }
                            Ok(true)
                        })?;
                    }
                }
            }
        }

        Ok(group.finalize())
    }

    // -----------------------------------------------------------------------
    // Transaction support (matching Collection API)
    // -----------------------------------------------------------------------

    pub fn log_wal_batch(&self, _entries: &[crate::wal::WalEntry]) -> Result<()> {
        // B-tree collection doesn't use WAL — this is a no-op.
        // Durability is provided by B-tree persistence.
        Ok(())
    }

    pub fn checkpoint_wal(&self) -> Result<()> {
        // No WAL to checkpoint — persist the B-tree instead.
        self.storage.persist()
    }

    pub fn prepare_tx_insert(
        &self,
        mut data: Value,
        tx_id: u64,
    ) -> Result<crate::collection::PreparedMutation> {
        if !data.is_object() {
            return Err(Error::NotAnObject);
        }

        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let obj = data.as_object_mut().unwrap();
        obj.insert("_id".to_string(), Value::Number(id.into()));
        obj.insert("_version".to_string(), Value::Number(1.into()));

        self.check_unique_constraints(&data, None)?;

        let bytes = codec::encode_doc(&data)?;

        Ok(crate::collection::PreparedMutation {
            wal_entry: crate::wal::WalEntry::Insert {
                doc_id: id,
                doc_bytes: bytes.clone(),
                tx_id,
            },
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
    ) -> Result<Vec<crate::collection::PreparedMutation>> {
        let update_obj = update_json
            .as_object()
            .ok_or_else(|| Error::InvalidQuery("update must be an object".into()))?;
        if update_obj.is_empty() {
            return Err(Error::InvalidQuery(
                "update must contain at least one operator".into(),
            ));
        }

        let query = query::parse_query(query_json)?;
        let fi = self.field_indexes.read();
        let ci = self.composite_indexes.read();
        let candidate_ids = query::execute_indexed(
            &query,
            &fi,
            &ci,
        );

        let mut mutations = Vec::new();

        let process_candidate =
            |id: DocumentId,
             cached: &Value,
             mutations: &mut Vec<crate::collection::PreparedMutation>|
             -> Result<()> {
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

                Self::check_unique_constraints_with(&fi, &data, Some(id))?;

                let new_bytes = codec::encode_doc(&data)?;
                mutations.push(crate::collection::PreparedMutation {
                    wal_entry: crate::wal::WalEntry::Update {
                        doc_id: id,
                        doc_bytes: new_bytes.clone(),
                        tx_id,
                    },
                    doc_id: id,
                    new_bytes,
                    old_loc: None, // No DocLocation in B-tree mode
                    old_data: Some(old_data),
                    new_data: data,
                    is_delete: false,
                });
                Ok(())
            };

        if let Some(ref indexed_ids) = candidate_ids {
            for &id in indexed_ids {
                if let Some(data) = self.read_doc(id)? {
                    process_candidate(id, &data, &mut mutations)?;
                }
            }
        } else {
            drop(ci);
            drop(fi);
            let mut snapshot: Vec<(DocumentId, Value)> = Vec::new();
            self.for_each_doc_arc_while(|id, arc| {
                snapshot.push((id, (**arc).clone()));
                Ok(true)
            })?;
            // Re-acquire for unique checks in process_candidate
            let fi = self.field_indexes.read();
            let _ci = self.composite_indexes.read();
            let process_candidate_reborrowed =
                |id: DocumentId,
                 cached: &Value,
                 mutations: &mut Vec<crate::collection::PreparedMutation>|
                 -> Result<()> {
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

                    Self::check_unique_constraints_with(&fi, &data, Some(id))?;

                    let new_bytes = codec::encode_doc(&data)?;
                    mutations.push(crate::collection::PreparedMutation {
                        wal_entry: crate::wal::WalEntry::Update {
                            doc_id: id,
                            doc_bytes: new_bytes.clone(),
                            tx_id,
                        },
                        doc_id: id,
                        new_bytes,
                        old_loc: None,
                        old_data: Some(old_data),
                        new_data: data,
                        is_delete: false,
                    });
                    Ok(())
                };
            for (id, data) in &snapshot {
                process_candidate_reborrowed(*id, data, &mut mutations)?;
            }
        }

        Ok(mutations)
    }

    pub fn prepare_tx_delete(
        &self,
        query_json: &Value,
        tx_id: u64,
    ) -> Result<Vec<crate::collection::PreparedMutation>> {
        let query = query::parse_query(query_json)?;
        let fi = self.field_indexes.read();
        let ci = self.composite_indexes.read();
        let candidate_ids = query::execute_indexed(
            &query,
            &fi,
            &ci,
        );
        drop(ci);
        drop(fi);

        let mut mutations = Vec::new();

        let process_candidate =
            |id: DocumentId,
             cached: &Value,
             mutations: &mut Vec<crate::collection::PreparedMutation>|
             -> Result<()> {
                if !query::matches_value(&query, cached) {
                    return Ok(());
                }
                mutations.push(crate::collection::PreparedMutation {
                    wal_entry: crate::wal::WalEntry::Delete { doc_id: id, tx_id },
                    doc_id: id,
                    new_bytes: vec![],
                    old_loc: None,
                    old_data: Some(cached.clone()),
                    new_data: Value::Null,
                    is_delete: true,
                });
                Ok(())
            };

        if let Some(ref indexed_ids) = candidate_ids {
            for &id in indexed_ids {
                if let Some(data) = self.read_doc(id)? {
                    process_candidate(id, &data, &mut mutations)?;
                }
            }
        } else {
            let mut snapshot: Vec<(DocumentId, Value)> = Vec::new();
            self.for_each_doc_arc_while(|id, arc| {
                snapshot.push((id, (**arc).clone()));
                Ok(true)
            })?;
            for (id, data) in &snapshot {
                process_candidate(*id, data, &mut mutations)?;
            }
        }

        Ok(mutations)
    }

    /// Apply prepared mutations to the B-tree and update indexes.
    pub fn apply_prepared(
        &self,
        mutations: &mut Vec<crate::collection::PreparedMutation>,
    ) -> Result<()> {
        let mut fi = self.field_indexes.write();
        let mut ci = self.composite_indexes.write();
        let mut ti = self.text_index.write();
        let mut vi = self.vector_indexes.write();

        for m in mutations.iter() {
            if m.is_delete {
                // Remove from B-tree
                self.storage.remove(m.doc_id);
                self.doc_cache.remove(m.doc_id);
                if let Some(ref old_data) = m.old_data {
                    for idx in fi.values_mut() {
                        idx.remove_value(m.doc_id, old_data);
                    }
                    for idx in ci.iter_mut() {
                        idx.remove_value(m.doc_id, old_data);
                    }
                }
                if let Some(ref mut text_idx) = *ti {
                    text_idx.remove_doc(m.doc_id);
                }
                for idx in vi.values_mut() {
                    idx.remove(m.doc_id);
                }
            } else {
                // Insert or update in B-tree
                self.storage.insert(m.doc_id, m.new_bytes.clone());

                if let Some(ref old_data) = m.old_data {
                    for idx in fi.values_mut() {
                        idx.remove_value(m.doc_id, old_data);
                    }
                    for idx in ci.iter_mut() {
                        idx.remove_value(m.doc_id, old_data);
                    }
                }
                for idx in fi.values_mut() {
                    idx.insert_value(m.doc_id, &m.new_data);
                }
                for idx in ci.iter_mut() {
                    idx.insert_value(m.doc_id, &m.new_data);
                }
                if let Some(ref mut text_idx) = *ti {
                    text_idx.index_doc(m.doc_id, &m.new_data);
                }
                for idx in vi.values_mut() {
                    idx.remove(m.doc_id);
                    let _ = idx.insert(m.doc_id, &m.new_data);
                }
                self.doc_cache.put(m.doc_id, Arc::new(m.new_data.clone()));
            }
        }
        if !mutations.is_empty() {
            self.dirty.store(true, Ordering::Release);
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // TTL support
    // -----------------------------------------------------------------------

    fn register_ttl(&self, doc_id: DocumentId, data: &Value) {
        let ttl_secs = data
            .get("_ttl")
            .and_then(|v| v.as_u64().or_else(|| v.as_f64().map(|f| f as u64)));
        if let Some(secs) = ttl_secs {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let expires_at = now_ms + secs * 1000;
            let mut ttl = self.ttl_index.lock();
            ttl.entry(expires_at)
                .or_insert_with(Vec::new)
                .push(doc_id);
        }
    }

    /// Evict expired documents. Returns the number evicted.
    pub fn evict_expired(&self) -> usize {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // Collect expired entries under the ttl lock
        let expired: Vec<(u64, Vec<DocumentId>)> = {
            let ttl = self.ttl_index.lock();
            if ttl.is_empty() {
                return 0;
            }
            ttl.range(..=now_ms)
                .map(|(&k, v)| (k, v.clone()))
                .collect()
        };

        if expired.is_empty() {
            return 0;
        }

        // Acquire write locks for index cleanup
        let mut fi = self.field_indexes.write();
        let mut ci = self.composite_indexes.write();
        let mut ti = self.text_index.write();

        let mut evicted = 0;
        for (_ts, ids) in &expired {
            for &id in ids {
                if self.storage.contains_key(id) {
                    if let Some(bytes) = self.storage.get(id) {
                        if let Ok(data) = codec::decode_doc(&bytes) {
                            self.storage.remove(id);
                            self.doc_cache.remove(id);
                            for idx in fi.values_mut() {
                                idx.remove_value(id, &data);
                            }
                            for idx in ci.iter_mut() {
                                idx.remove_value(id, &data);
                            }
                            if let Some(ref mut text_idx) = *ti {
                                text_idx.remove_doc(id);
                            }
                            evicted += 1;
                        }
                    }
                }
            }
        }

        // Remove expired entries from ttl_index
        {
            let mut ttl = self.ttl_index.lock();
            for (ts, _) in &expired {
                ttl.remove(ts);
            }
        }

        evicted
    }
}

// ---------------------------------------------------------------------------
// JSONB helpers (same as collection.rs)
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn new_btree_collection(name: &str) -> BTreeCollection {
        BTreeCollection::open_in_memory(name)
    }

    #[test]
    fn insert_and_find() {
        let col = new_btree_collection("test");

        let id1 = col.insert(json!({"name": "Alice", "age": 30})).unwrap();
        let id2 = col.insert(json!({"name": "Bob", "age": 25})).unwrap();
        let id3 = col.insert(json!({"name": "Charlie", "age": 35})).unwrap();

        assert_eq!(col.count(), 3);

        // Find all
        let all = col.find(&json!({})).unwrap();
        assert_eq!(all.len(), 3);

        // Find by field
        let results = col.find(&json!({"name": "Bob"})).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("_id").unwrap().as_u64().unwrap(), id2);

        // Find one
        let one = col.find_one(&json!({"name": "Alice"})).unwrap();
        assert!(one.is_some());
        assert_eq!(one.unwrap().get("_id").unwrap().as_u64().unwrap(), id1);

        // Find non-existent
        let none = col.find_one(&json!({"name": "Dave"})).unwrap();
        assert!(none.is_none());

        let _ = id3; // used to create the doc
    }

    #[test]
    fn insert_many_and_count() {
        let col = new_btree_collection("test");

        let docs: Vec<Value> = (0..1000)
            .map(|i| json!({"seq": i, "value": format!("doc_{}", i)}))
            .collect();

        let ids = col.insert_many(docs).unwrap();
        assert_eq!(ids.len(), 1000);
        assert_eq!(col.count(), 1000);

        // Verify first and last
        let first = col.find_one(&json!({"seq": 0})).unwrap().unwrap();
        assert_eq!(first.get("value").unwrap().as_str().unwrap(), "doc_0");

        let last = col.find_one(&json!({"seq": 999})).unwrap().unwrap();
        assert_eq!(last.get("value").unwrap().as_str().unwrap(), "doc_999");
    }

    #[test]
    fn create_index_and_find() {
        let col = new_btree_collection("test");

        // Insert docs
        for i in 0..100 {
            col.insert(json!({"category": format!("cat_{}", i % 5), "value": i}))
                .unwrap();
        }

        // Create index on category
        col.create_index("category").unwrap();

        // Find with index
        let results = col.find(&json!({"category": "cat_2"})).unwrap();
        assert_eq!(results.len(), 20);

        // Count matching
        let count = col.count_matching(&json!({"category": "cat_0"})).unwrap();
        assert_eq!(count, 20);
    }

    #[test]
    fn find_with_sort_and_limit() {
        let col = new_btree_collection("test");

        for i in 0..50 {
            col.insert(json!({"score": 50 - i, "name": format!("player_{}", i)}))
                .unwrap();
        }

        col.create_index("score").unwrap();

        // Sort ascending, limit 5
        let opts = FindOptions {
            sort: Some(vec![("score".to_string(), SortOrder::Asc)]),
            skip: None,
            limit: Some(5),
        };
        let results = col.find_with_options(&json!({}), &opts).unwrap();
        assert_eq!(results.len(), 5);
        assert_eq!(results[0].get("score").unwrap().as_i64().unwrap(), 1);
        assert_eq!(results[4].get("score").unwrap().as_i64().unwrap(), 5);

        // Sort descending, skip 2, limit 3
        let opts = FindOptions {
            sort: Some(vec![("score".to_string(), SortOrder::Desc)]),
            skip: Some(2),
            limit: Some(3),
        };
        let results = col.find_with_options(&json!({}), &opts).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].get("score").unwrap().as_i64().unwrap(), 48);
    }

    #[test]
    fn update_documents() {
        let col = new_btree_collection("test");

        col.insert(json!({"name": "Alice", "age": 30})).unwrap();
        col.insert(json!({"name": "Bob", "age": 25})).unwrap();

        // Update Alice's age
        let updated = col
            .update(&json!({"name": "Alice"}), &json!({"$set": {"age": 31}}), None)
            .unwrap();
        assert_eq!(updated.len(), 1);

        let alice = col.find_one(&json!({"name": "Alice"})).unwrap().unwrap();
        assert_eq!(alice.get("age").unwrap().as_i64().unwrap(), 31);
        assert_eq!(alice.get("_version").unwrap().as_u64().unwrap(), 2);

        // Update with limit
        col.insert(json!({"name": "Charlie", "age": 30})).unwrap();
        let updated = col
            .update(
                &json!({"age": 30}),
                &json!({"$set": {"status": "active"}}),
                Some(1),
            )
            .unwrap();
        // Should update at most 1
        assert!(updated.len() <= 1);
    }

    #[test]
    fn delete_documents() {
        let col = new_btree_collection("test");

        for i in 0..10 {
            col.insert(json!({"group": i % 3, "value": i})).unwrap();
        }

        assert_eq!(col.count(), 10);

        // Delete group 0 docs
        let deleted = col.delete(&json!({"group": 0}), None).unwrap();
        assert_eq!(deleted.len(), 4); // 0, 3, 6, 9
        assert_eq!(col.count(), 6);

        // Delete one
        let deleted = col.delete(&json!({"group": 1}), Some(1)).unwrap();
        assert_eq!(deleted.len(), 1);
        assert_eq!(col.count(), 5);
    }

    #[test]
    fn unique_index_enforcement() {
        let col = new_btree_collection("test");

        col.insert(json!({"email": "alice@test.com"})).unwrap();
        col.create_unique_index("email").unwrap();

        // Should fail: duplicate email
        let result = col.insert(json!({"email": "alice@test.com"}));
        assert!(result.is_err());

        // Should succeed: different email
        col.insert(json!({"email": "bob@test.com"})).unwrap();
    }

    #[test]
    fn version_tracking() {
        let col = new_btree_collection("test");

        let id = col.insert(json!({"name": "test"})).unwrap();
        assert_eq!(col.get_version(id), 1);

        col.update(
            &json!({"_id": id}),
            &json!({"$set": {"name": "updated"}}),
            None,
        )
        .unwrap();
        assert_eq!(col.get_version(id), 2);
    }

    #[test]
    fn compact_operation() {
        let col = new_btree_collection("test");

        for i in 0..100 {
            col.insert(json!({"value": i})).unwrap();
        }

        col.delete(&json!({"value": {"$lt": 50}}), None).unwrap();
        assert_eq!(col.count(), 50);

        let stats = col.compact().unwrap();
        assert_eq!(stats.docs_kept, 50);
    }

    #[test]
    fn btree_cursor_scan_order() {
        let col = new_btree_collection("test");

        // Insert in random order
        for &i in &[5, 3, 1, 4, 2] {
            col.insert(json!({"seq": i})).unwrap();
        }

        // B-tree cursor scan should yield docs in key (doc_id) order
        let all = col.find(&json!({})).unwrap();
        assert_eq!(all.len(), 5);

        // All docs should be present
        let ids: Vec<u64> = all
            .iter()
            .map(|d| d.get("_id").unwrap().as_u64().unwrap())
            .collect();
        // IDs are assigned sequentially regardless of insert order
        assert_eq!(ids, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn insert_1000_create_index_find_sort_count_update_delete() {
        let col = new_btree_collection("stress_test");

        // Insert 1000 docs
        let docs: Vec<Value> = (0..1000)
            .map(|i| {
                json!({
                    "category": format!("cat_{}", i % 10),
                    "priority": i % 5,
                    "value": i,
                    "name": format!("item_{}", i)
                })
            })
            .collect();

        let ids = col.insert_many(docs).unwrap();
        assert_eq!(ids.len(), 1000);
        assert_eq!(col.count(), 1000);

        // Create indexes
        col.create_index("category").unwrap();
        col.create_index("priority").unwrap();
        col.create_index("value").unwrap();

        // Find with index
        let cat_2 = col.find(&json!({"category": "cat_2"})).unwrap();
        assert_eq!(cat_2.len(), 100);

        // Find with sort + limit
        let opts = FindOptions {
            sort: Some(vec![("value".to_string(), SortOrder::Desc)]),
            skip: None,
            limit: Some(10),
        };
        let top_10 = col.find_with_options(&json!({}), &opts).unwrap();
        assert_eq!(top_10.len(), 10);
        assert_eq!(top_10[0].get("value").unwrap().as_i64().unwrap(), 999);
        assert_eq!(top_10[9].get("value").unwrap().as_i64().unwrap(), 990);

        // Count matching
        let count = col.count_matching(&json!({"priority": 0})).unwrap();
        assert_eq!(count, 200);

        // Update
        let updated = col
            .update(
                &json!({"category": "cat_0"}),
                &json!({"$set": {"status": "processed"}}),
                None,
            )
            .unwrap();
        assert_eq!(updated.len(), 100);

        // Verify update
        let cat_0 = col.find(&json!({"category": "cat_0"})).unwrap();
        for doc in &cat_0 {
            assert_eq!(doc.get("status").unwrap().as_str().unwrap(), "processed");
        }

        // Delete
        let deleted = col.delete(&json!({"priority": 4}), None).unwrap();
        assert_eq!(deleted.len(), 200);
        assert_eq!(col.count(), 800);

        // Verify count after delete
        let count = col.count_matching(&json!({"priority": 4})).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn file_persistence_roundtrip() {
        let dir = tempfile::tempdir().unwrap();

        // Create and populate
        {
            let col = BTreeCollection::open("persist_test", dir.path()).unwrap();
            for i in 0..100 {
                col.insert(json!({"seq": i, "name": format!("doc_{}", i)}))
                    .unwrap();
            }
            col.sync_writes().unwrap();
        }

        // Reopen and verify
        {
            let col = BTreeCollection::open("persist_test", dir.path()).unwrap();
            assert_eq!(col.count(), 100);

            let doc = col.find_one(&json!({"seq": 50})).unwrap().unwrap();
            assert_eq!(doc.get("name").unwrap().as_str().unwrap(), "doc_50");
        }
    }

    #[test]
    fn wal_recovery_after_crash() {
        let dir = tempfile::tempdir().unwrap();

        // Insert docs WITHOUT calling sync_writes (simulates crash)
        {
            let col = BTreeCollection::open("wal_test", dir.path()).unwrap();
            for i in 0..50 {
                col.insert(json!({"seq": i, "data": format!("val_{}", i)})).unwrap();
            }
            // NO sync_writes — WAL has entries but .btree file is empty
        }

        // Reopen — WAL replay should recover all 50 docs
        {
            let col = BTreeCollection::open("wal_test", dir.path()).unwrap();
            assert_eq!(col.count(), 50);
            let doc = col.find_one(&json!({"seq": 25})).unwrap().unwrap();
            assert_eq!(doc.get("data").unwrap().as_str().unwrap(), "val_25");
        }
    }

    #[test]
    fn wal_recovery_update_and_delete() {
        let dir = tempfile::tempdir().unwrap();

        // Phase 1: insert + persist
        {
            let col = BTreeCollection::open("wal_ud", dir.path()).unwrap();
            for i in 0..10 {
                col.insert(json!({"key": i, "value": 0})).unwrap();
            }
            col.sync_writes().unwrap(); // persist + checkpoint
        }

        // Phase 2: update + delete WITHOUT persist (crash)
        {
            let col = BTreeCollection::open("wal_ud", dir.path()).unwrap();
            assert_eq!(col.count(), 10);
            // Update key=5 → value=999
            col.update(&json!({"key": 5}), &json!({"$set": {"value": 999}}), None).unwrap();
            // Delete key=0
            col.delete(&json!({"key": 0}), None).unwrap();
            // NO sync_writes — crash
        }

        // Phase 3: recover — should have 9 docs, key=5 value=999
        {
            let col = BTreeCollection::open("wal_ud", dir.path()).unwrap();
            assert_eq!(col.count(), 9);
            let doc = col.find_one(&json!({"key": 5})).unwrap().unwrap();
            let val = doc.get("value").unwrap().as_i64().unwrap();
            assert_eq!(val, 999);
            assert!(col.find_one(&json!({"key": 0})).unwrap().is_none());
        }
    }

    #[test]
    fn wal_checkpoint_clears_wal() {
        let dir = tempfile::tempdir().unwrap();

        {
            let col = BTreeCollection::open("wal_cp", dir.path()).unwrap();
            for i in 0..20 {
                col.insert(json!({"seq": i})).unwrap();
            }
            col.sync_writes().unwrap(); // persist + checkpoint WAL
        }

        // WAL file should be empty after checkpoint
        let wal_path = dir.path().join("wal_cp.wal");
        let wal_size = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
        assert_eq!(wal_size, 0, "WAL should be empty after checkpoint");

        // Reopen — no replay needed, all data from .btree file
        {
            let col = BTreeCollection::open("wal_cp", dir.path()).unwrap();
            assert_eq!(col.count(), 20);
        }
    }
}
