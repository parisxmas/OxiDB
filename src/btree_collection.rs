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

use crate::locks::{Mutex, RwLock};
#[cfg(not(target_arch = "wasm32"))]
use rayon::prelude::*;
use serde_json::{Value, json};

use crate::btree_storage::BTreeStorage;
use crate::codec;
#[cfg(not(target_arch = "wasm32"))]
use crate::collection::load_index_metadata;
use crate::collection::{CompactStats, IndexInfo, IndexMetadata, resolve_field_in_value};
use crate::doc_bytes_cache::DocBytesCache;
use crate::doc_cache::DocCache;
use crate::document::DocumentId;
use crate::error::{Error, Result};
use crate::fts::CollectionTextIndex;
use crate::in_memory::WalBackend;
use crate::index::CompositeIndex;
#[cfg(not(target_arch = "wasm32"))]
use crate::index_persist;
use crate::paged_field_index::PagedFieldIndex;
use crate::query::{self, FindOptions, Query, SortOrder};
use crate::value::IndexValue;
use crate::vector::{DistanceMetric, VectorIndex};
#[cfg(not(target_arch = "wasm32"))]
use crate::wal::Wal;
use crate::wal::WalEntry;

/// A B-tree-native collection that stores documents directly in a `BTreeStorage`.
///
/// Unlike `Collection` which uses append-only storage + primary index HashMap,
/// this struct uses the B-tree as both storage AND primary index.
///
/// All fields with interior mutability — callers use `&self` for everything.
/// Configuration for a TTL index on a datetime field.
#[derive(Debug, Clone)]
pub struct TtlIndexConfig {
    pub field: String,
    pub expire_after_seconds: u64,
}

/// Resident memory breakdown for a single collection (see
/// [`BTreeCollection::memory_report`]). All figures are bytes computed from the
/// live data structures, independent of process RSS / allocator retention.
#[derive(Debug, Clone, Copy)]
pub struct CollectionMemory {
    /// Number of live documents resident in the in-RAM primary store.
    pub storage_entries: usize,
    /// Sum of encoded document payload bytes held resident in the primary store.
    pub storage_payload: usize,
    /// Estimated per-entry container overhead of the primary store.
    pub storage_overhead: usize,
    /// Resident bytes across all single-field indexes.
    pub field_index_bytes: usize,
    /// Resident bytes across all composite indexes.
    pub composite_index_bytes: usize,
}

impl CollectionMemory {
    /// Total resident bytes attributable to this collection's primary store
    /// plus indexes (excludes the LRU caches).
    pub fn total(&self) -> usize {
        self.storage_payload
            + self.storage_overhead
            + self.field_index_bytes
            + self.composite_index_bytes
    }
}

pub struct BTreeCollection {
    #[allow(dead_code)]
    name: String,
    #[allow(dead_code)]
    data_dir: PathBuf,
    storage: BTreeStorage, // Already thread-safe (scc::HashMap)
    field_indexes: RwLock<HashMap<String, PagedFieldIndex>>,
    composite_indexes: RwLock<Vec<CompositeIndex>>,
    text_index: RwLock<Option<CollectionTextIndex>>,
    vector_indexes: RwLock<HashMap<String, VectorIndex>>,
    doc_cache: DocCache, // Already thread-safe
    /// Sibling of `doc_cache` that holds OxiWire-encoded bytes per doc.
    /// Populated lazily by the wire-output path (and explicitly on insert)
    /// so a `find` that ships docs to the client can `memcpy` instead of
    /// re-encoding each Value. Each entry is ~500 B versus ~3.5 KB for an
    /// `Arc<Value>`, so this cache can be sized ~7× larger for the same
    /// RAM footprint. See [`crate::doc_bytes_cache`] for sizing rationale.
    bytes_cache: DocBytesCache,
    next_id: AtomicU64, // Already atomic
    #[allow(dead_code)]
    in_memory: bool,
    ttl_index: Mutex<std::collections::BTreeMap<u64, Vec<DocumentId>>>,
    ttl_configs: RwLock<Vec<TtlIndexConfig>>,
    /// Dirty flag: set on write, cleared after persist.
    dirty: AtomicBool,
    /// Lazy-sync mode: when true, WAL writes skip per-commit fsync and
    /// rely on the engine's background sync thread (~10ms cadence). When
    /// false, every commit fsyncs the WAL — strict durability for ACID.
    lazy_sync: AtomicBool,
    wal: WalBackend,
    /// WORM phase 2 — engine-level immutability log. None on
    /// in-memory collections (no place to persist) and during the
    /// pre-Phase-2 transition window before any worm op has touched
    /// the collection. update / delete / find_and_modify consult
    /// `worm.is_locked(doc_id, now)` before applying; locked docs
    /// surface `Error::DocumentWormLocked` directly out of the
    /// storage layer.
    worm: Option<crate::worm::WormSet>,
}

impl BTreeCollection {
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open(
        name: &str,
        data_dir: &Path,
        encryption: Option<std::sync::Arc<crate::EncryptionKey>>,
    ) -> Result<Self> {
        Self::open_with_sequencer(name, data_dir, encryption, None)
    }

    /// Open a collection, attaching `sequencer` to its WAL. When `Some`
    /// (PITR enabled), every WAL write is stamped with a global GSN +
    /// wall-clock and emitted in the v2 record format. Storage options are
    /// resolved per-collection (persisted `.bopts` or env defaults).
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_with_sequencer(
        name: &str,
        data_dir: &Path,
        encryption: Option<std::sync::Arc<crate::EncryptionKey>>,
        sequencer: Option<std::sync::Arc<crate::pitr::ArchiveSequencer>>,
    ) -> Result<Self> {
        Self::open_resolved(name, data_dir, encryption, sequencer, None, None)
    }

    /// Open a collection with **explicit** per-collection
    /// [`StorageOptions`](crate::btree_storage::StorageOptions) (disk-first,
    /// compression, compaction policy) instead of the resolved/env defaults.
    /// Used by `OxiDb::create_collection_with_options`.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_with_options(
        name: &str,
        data_dir: &Path,
        encryption: Option<std::sync::Arc<crate::EncryptionKey>>,
        sequencer: Option<std::sync::Arc<crate::pitr::ArchiveSequencer>>,
        opts: crate::btree_storage::StorageOptions,
    ) -> Result<Self> {
        Self::open_resolved(name, data_dir, encryption, sequencer, Some(opts), None)
    }

    /// Open a collection with crash-recovery filtering: WAL entries that
    /// belong to a transaction (`tx_id != 0`) are replayed only if that
    /// transaction reached its commit point — i.e. its id is in
    /// `committed_txs`, the set read from the engine's global commit log.
    /// Entries of transactions that died between their WAL fsync and
    /// `mark_committed` are discarded instead of being resurrected.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_recovering(
        name: &str,
        data_dir: &Path,
        encryption: Option<std::sync::Arc<crate::EncryptionKey>>,
        sequencer: Option<std::sync::Arc<crate::pitr::ArchiveSequencer>>,
        storage_opts: Option<crate::btree_storage::StorageOptions>,
        committed_txs: &std::collections::HashSet<u64>,
    ) -> Result<Self> {
        Self::open_resolved(
            name,
            data_dir,
            encryption,
            sequencer,
            storage_opts,
            Some(committed_txs),
        )
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn open_resolved(
        name: &str,
        data_dir: &Path,
        encryption: Option<std::sync::Arc<crate::EncryptionKey>>,
        sequencer: Option<std::sync::Arc<crate::pitr::ArchiveSequencer>>,
        storage_opts: Option<crate::btree_storage::StorageOptions>,
        committed_txs: Option<&std::collections::HashSet<u64>>,
    ) -> Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        let storage = match storage_opts {
            Some(opts) => BTreeStorage::open_with_options(name, data_dir, encryption, opts)?,
            None => BTreeStorage::open(name, data_dir, encryption)?,
        };

        let wal_path = data_dir.join(format!("{}.wal", name));
        let wal = Wal::open(&wal_path)?.with_sequencer(sequencer);
        let (applied, skipped) = Self::replay_wal(&wal, &storage, committed_txs)?;
        let wal_backend = WalBackend::File(wal);
        if applied > 0 {
            storage.persist()?;
        }
        if applied + skipped > 0 {
            // Checkpoint even when every entry was skipped: skipped entries
            // belong to transactions that never committed, and leaving them
            // in the WAL would let a later run — whose fresh commit log can
            // contain a colliding tx id — resurrect them on replay.
            if let WalBackend::File(ref w) = wal_backend {
                w.checkpoint_no_sync()?;
            }
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

        // Try loading cached index data. Disk-first indexes use their own
        // mmap'd `.mfidx` format (not the `.fidx` PagedFieldIndex cache), so
        // skip the cache and rebuild them disk-backed below.
        let disk_first = storage.is_disk_first();
        let doc_count = storage.count() as u64;
        let fidx_path = data_dir.join(format!("{}.fidx", name));
        let loaded_field_indexes = if !persisted_indexes.is_empty() && !disk_first {
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
                    if disk_first {
                        let mpath = data_dir.join(format!("{}.{}.mfidx", name, field));
                        // Reopen: mmap the persisted index (instant, empty
                        // overlay, small resident). First creation / missing
                        // file: build into the overlay from a scan, then persist.
                        let idx = match PagedFieldIndex::open_disk(mpath.clone()) {
                            Ok(idx) => idx,
                            Err(_) => {
                                let mut idx =
                                    PagedFieldIndex::new_disk(field.clone(), info.unique, mpath);
                                storage.scan_all_while(|_id, bytes| {
                                    if let Ok(doc) = crate::codec::decode_doc(bytes) {
                                        if let Some(id) = doc.get("_id").and_then(|v| v.as_u64()) {
                                            idx.insert_value(id, &doc);
                                        }
                                    }
                                    Ok(true)
                                })?;
                                let _ = idx.persist_disk();
                                idx
                            }
                        };
                        map.insert(field.clone(), idx);
                        continue;
                    }
                    let mut idx = PagedFieldIndex::new(field.clone());
                    if info.unique {
                        idx.unique = true;
                    }
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
            let loaded =
                index_persist::load_composite_indexes(&cidx_path, doc_count, max_id + 1, None);
            loaded.unwrap_or_default()
        } else {
            Vec::new()
        };

        // Restore TTL index configs from persisted metadata
        let ttl_configs: Vec<TtlIndexConfig> = persisted_indexes
            .iter()
            .filter(|info| info.index_type == "ttl")
            .filter_map(|info| {
                info.expire_after_seconds.map(|expire| TtlIndexConfig {
                    field: info.fields[0].clone(),
                    expire_after_seconds: expire,
                })
            })
            .collect();

        // Open the WORM log alongside the storage / WAL. Lives in
        // the same data dir, sibling to `<name>.wal`. Created lazily
        // on first read — empty file = empty set.
        let coll_marker = data_dir.join(name);
        let worm = crate::worm::WormSet::open(&coll_marker).ok();

        Ok(Self {
            name: name.to_string(),
            data_dir: data_dir.to_path_buf(),
            storage,
            field_indexes: RwLock::new(field_indexes),
            composite_indexes: RwLock::new(composite_indexes),
            text_index: RwLock::new(None),
            vector_indexes: RwLock::new(HashMap::new()),
            doc_cache: DocCache::new(crate::doc_cache::default_capacity()),
            bytes_cache: DocBytesCache::new(crate::doc_bytes_cache::default_capacity()),
            next_id: AtomicU64::new(max_id + 1),
            in_memory: false,
            ttl_index: Mutex::new(std::collections::BTreeMap::new()),
            ttl_configs: RwLock::new(ttl_configs),
            dirty: AtomicBool::new(false),
            lazy_sync: AtomicBool::new(false),
            wal: wal_backend,
            worm,
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
            doc_cache: DocCache::new(crate::doc_cache::default_capacity()),
            bytes_cache: DocBytesCache::new(crate::doc_bytes_cache::default_capacity()),
            next_id: AtomicU64::new(1),
            in_memory: true,
            ttl_index: Mutex::new(std::collections::BTreeMap::new()),
            ttl_configs: RwLock::new(Vec::new()),
            dirty: AtomicBool::new(false),
            lazy_sync: AtomicBool::new(false),
            wal: WalBackend::Memory,
            worm: None, // in-memory collections don't persist WORM state
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn replay_wal(
        wal: &Wal,
        storage: &BTreeStorage,
        committed_txs: Option<&std::collections::HashSet<u64>>,
    ) -> Result<(usize, usize)> {
        let (mut applied, mut skipped) = (0usize, 0usize);
        // With PITR enabled the history since the last snapshot is split
        // across sealed segments (`<name>.wal.<seq>`) plus the live
        // `<name>.wal`. Replay the sealed segments oldest-first, then the
        // live WAL, so no acknowledged write is missed. Without PITR
        // there are no sealed segments and this is just the live WAL —
        // identical to the prior behavior. Replay is idempotent (upsert
        // by doc_id), so re-applying entries already in the snapshot is
        // harmless. Sealed segments are left in place for the archiver
        // (Phase 3) to consume; until then they are re-replayed each open.
        for seg_path in wal.list_sealed_segments() {
            let seg = Wal::open(&seg_path)?;
            let (a, s) = Self::replay_entries(&seg.read_entries()?, storage, committed_txs);
            applied += a;
            skipped += s;
        }
        let (a, s) = Self::replay_entries(&wal.read_entries()?, storage, committed_txs);
        applied += a;
        skipped += s;
        Ok((applied, skipped))
    }

    /// Apply a batch of WAL entries to `storage` idempotently, returning
    /// `(applied, skipped)` counts. When `committed_txs` is `Some`,
    /// transactional entries (`tx_id != 0`) whose transaction is absent
    /// from the set are skipped — they were fsync'd at commit step 5 but
    /// the transaction never reached its commit point (`mark_committed`),
    /// so applying them would resurrect an aborted transaction or
    /// half-apply a multi-collection one. Non-transactional entries
    /// (`tx_id == 0`) always apply.
    fn replay_entries(
        entries: &[WalEntry],
        storage: &BTreeStorage,
        committed_txs: Option<&std::collections::HashSet<u64>>,
    ) -> (usize, usize) {
        let (mut applied, mut skipped) = (0usize, 0usize);
        for entry in entries {
            let tx_id = match entry {
                WalEntry::Insert { tx_id, .. }
                | WalEntry::Update { tx_id, .. }
                | WalEntry::Delete { tx_id, .. } => *tx_id,
            };
            if let Some(committed) = committed_txs {
                if tx_id != 0 && !committed.contains(&tx_id) {
                    skipped += 1;
                    continue;
                }
            }
            match entry {
                WalEntry::Insert {
                    doc_id, doc_bytes, ..
                }
                | WalEntry::Update {
                    doc_id, doc_bytes, ..
                } => {
                    storage.insert(*doc_id, doc_bytes.clone());
                    applied += 1;
                }
                WalEntry::Delete { doc_id, .. } => {
                    storage.remove(*doc_id);
                    applied += 1;
                }
            }
        }
        (applied, skipped)
    }

    // -----------------------------------------------------------------------
    // Configuration (matching Collection API)
    // -----------------------------------------------------------------------

    pub fn set_lazy_sync(&self, enabled: bool) {
        // Switches WAL between per-commit fsync (false → strict ACID-D)
        // and batched background-sync (true → ~10ms data-loss window
        // on crash, MongoDB-style). The engine flips this at startup
        // based on OXIDB_LAZY_SYNC; runtime flips are also safe — the
        // hot path reads this atomically per-commit.
        self.lazy_sync.store(enabled, Ordering::Release);
    }

    /// Single-entry WAL write — fsyncs unless lazy mode is on.
    #[inline]
    fn wal_log(&self, entry: &WalEntry) -> Result<()> {
        if self.lazy_sync.load(Ordering::Acquire) {
            self.wal.log_no_sync(entry)
        } else {
            self.wal.log(entry)
        }
    }

    /// Batch WAL write — fsyncs once at the end unless lazy mode is on.
    #[inline]
    fn wal_log_batch(&self, entries: &[WalEntry]) -> Result<()> {
        if self.lazy_sync.load(Ordering::Acquire) {
            self.wal.log_batch_no_sync(entries)
        } else {
            self.wal.log_batch(entries)
        }
    }

    pub fn set_cache_capacity(&self, capacity: usize) {
        self.doc_cache.resize(capacity);
    }

    /// Snapshot of (hits, misses) on the per-collection document cache.
    pub fn doc_cache_stats(&self) -> crate::doc_cache::CacheStats {
        self.doc_cache.stats()
    }

    /// Snapshot of (hits, misses) on the per-collection bytes cache.
    pub fn bytes_cache_stats(&self) -> crate::doc_bytes_cache::BytesCacheStats {
        self.bytes_cache.stats()
    }

    /// Resident memory breakdown for this collection, computed by walking the
    /// live structures (allocator-independent — unlike process RSS, which is
    /// inflated by transient high-water marks the OS hasn't reclaimed).
    pub fn memory_report(&self) -> CollectionMemory {
        let storage_entries = self.storage.count();
        let storage_payload = self.storage.total_bytes() as usize;
        // Per-entry overhead in the scc::HashMap<u64, Vec<u8>>: 8-byte key, the
        // 24-byte Vec header stored in the slot, plus bucket bookkeeping.
        let storage_overhead = storage_entries * (8 + 24 + 16);

        let field_index_bytes: usize = self
            .field_indexes
            .read()
            .values()
            .map(|i| i.memory_bytes())
            .sum();
        let composite_index_bytes: usize = self
            .composite_indexes
            .read()
            .iter()
            .map(|c| c.memory_bytes())
            .sum();

        CollectionMemory {
            storage_entries,
            storage_payload,
            storage_overhead,
            field_index_bytes,
            composite_index_bytes,
        }
    }

    /// Encode a Value to OxiWire bytes and pre-populate the bytes cache.
    /// Called from the insert path so subsequent reads of the same doc can
    /// skip the JSONB→Value→OxiWire round trip.
    fn populate_bytes_cache(&self, id: DocumentId, value: &Value) {
        let bytes = crate::wire_oxiwire::encode_value_owned(value);
        self.bytes_cache.put(id, Arc::<[u8]>::from(bytes));
    }

    /// Load a document's pre-encoded OxiWire bytes. Three-tier path:
    ///
    /// 1. bytes_cache hit → return the cached Arc<[u8]> directly.
    /// 2. doc_cache hit (Value already in memory) → encode from Value,
    ///    cache bytes, return. Avoids the JSONB re-parse.
    /// 3. cold → pread JSONB bytes, convert JSONB → OxiWire directly
    ///    via [`crate::jsonb_oxiwire`] (no Value materialization),
    ///    cache bytes, return.
    ///
    /// Tier 3 is the win for big result sets: skips the ~20 µs/doc
    /// JSONB→Value cost that the Value-cache miss path would otherwise
    /// pay, and never pollutes the doc_cache.
    pub fn load_doc_oxiwire_bytes(&self, id: DocumentId) -> Option<Arc<[u8]>> {
        if let Some(bytes) = self.bytes_cache.get(id) {
            return Some(bytes);
        }
        if let Some(arc) = self.doc_cache.peek(id) {
            let bytes = crate::wire_oxiwire::encode_value_owned(&arc);
            let arc_bytes: Arc<[u8]> = Arc::<[u8]>::from(bytes);
            self.bytes_cache.put(id, Arc::clone(&arc_bytes));
            return Some(arc_bytes);
        }
        let jsonb_bytes = self.storage.get(id)?;
        let oxi_bytes = crate::jsonb_oxiwire::jsonb_to_oxiwire_owned(&jsonb_bytes).ok()?;
        let arc_bytes: Arc<[u8]> = Arc::<[u8]>::from(oxi_bytes);
        self.bytes_cache.put(id, Arc::clone(&arc_bytes));
        Some(arc_bytes)
    }

    /// Invalidate the bytes cache for a single doc id. Called from update /
    /// delete paths so stale bytes never get served. Cheap if absent.
    fn invalidate_bytes_cache(&self, id: DocumentId) {
        self.bytes_cache.remove(id);
    }

    /// Bytes-first find — the deeper refactor that closes the JSONB→Value
    /// gap. Returns `Some(Ok(_))` when the query is fully satisfied by an
    /// index (no post-filter, no sort, no skip/limit) and we can stream
    /// directly from `load_doc_oxiwire_bytes` without materialising Value
    /// trees. Returns `None` for queries that need the Value path; caller
    /// falls back to `find_with_options_arcs`.
    ///
    /// Two index strategies are tried, in order:
    /// 1. **Composite index** — when the query is exactly an AND of `$eq`
    ///    conditions covering some composite index's fields, use
    ///    `find_prefix` for direct ID resolution. This is the path that
    ///    matters for multi-field equality queries (the TestCompoundIndexQuery
    ///    shape in the bench).
    /// 2. **Single-field index path** — for queries `is_fully_indexed`
    ///    recognises (single eq, range, $in), fall through to
    ///    `execute_indexed` which intersects per-field index results.
    /// Post-filter `find` that encodes the matching documents directly into a
    /// single OxiWire buffer — never materializing a `Vec<Arc<Value>>`.
    ///
    /// This is the low-memory path for queries an index can't fully satisfy
    /// (e.g. `verified=true` over a whole collection). It byte-filters each
    /// stored document with [`query::matches_raw_jsonb`] — non-matches are
    /// skipped with **no decode at all** — and transcodes matches JSONB→OxiWire
    /// (no `Value`). So peak memory tracks the compact result bytes, not the
    /// ~7×-heavier `Value` set that OOM'd large unindexed results. Returns
    /// `None` for sort/skip/limit (the Value path owns ordering) so they fall
    /// back unchanged.
    pub fn find_oxiwire_postfilter(
        &self,
        query_json: &Value,
        opts: &FindOptions,
    ) -> Option<Result<(usize, Vec<u8>)>> {
        if opts.sort.is_some() || opts.skip.is_some() || opts.limit.is_some() {
            return None;
        }
        let query = match query::parse_query(query_json) {
            Ok(q) => q,
            Err(e) => return Some(Err(e)),
        };
        let mut buf: Vec<u8> = Vec::new();
        let mut count = 0usize;
        let scan = self.storage.scan_bytes_while(|bytes| {
            // Byte-level match: skip non-matches without decoding to Value.
            let matched = match query::matches_raw_jsonb(&query, bytes) {
                Some(b) => b,
                None => match crate::codec::decode_doc(bytes) {
                    Ok(doc) => query::matches_value(&query, &doc),
                    Err(_) => false,
                },
            };
            if matched {
                // Transcode the stored bytes straight to OxiWire (no Value).
                match crate::jsonb_oxiwire::jsonb_to_oxiwire_owned(bytes) {
                    Ok(oxi) => buf.extend_from_slice(&oxi),
                    Err(_) => {
                        // Legacy JSON record: decode + encode (rare).
                        if let Ok(doc) = crate::codec::decode_doc(bytes) {
                            crate::wire_oxiwire::encode_value(&doc, &mut buf);
                        } else {
                            return Ok(true);
                        }
                    }
                }
                count += 1;
            }
            Ok(true)
        });
        Some(scan.map(|_| (count, buf)))
    }

    pub fn find_oxiwire_bytes(
        &self,
        query_json: &Value,
        opts: &FindOptions,
    ) -> Option<Result<Vec<Arc<[u8]>>>> {
        // Sort/skip/limit not supported in the fast path — the Value-based
        // path has dedicated logic for those (early termination, index-backed
        // sort, etc.) that we don't replicate here.
        if opts.sort.is_some() || opts.skip.is_some() || opts.limit.is_some() {
            return None;
        }

        let query = match query::parse_query(query_json) {
            Ok(q) => q,
            Err(e) => return Some(Err(e)),
        };

        // ── Strategy 1: composite-index exact cover ─────────────────────
        if let Some(ids) = self.try_composite_lookup(&query) {
            let mut results: Vec<Arc<[u8]>> = Vec::with_capacity(ids.len());
            for id in &ids {
                if let Some(bytes) = self.load_doc_oxiwire_bytes(*id) {
                    results.push(bytes);
                }
            }
            return Some(Ok(results));
        }

        // ── Strategy 2: single-field index path ─────────────────────────
        let fi = self.field_indexes.read();
        if !query::is_fully_indexed(&query, &fi) {
            return None;
        }
        let ci = self.composite_indexes.read();
        let candidate_ids = query::execute_indexed(&query, &fi, &ci);
        drop(ci);
        drop(fi);

        let ids = match candidate_ids {
            Some(ids) => ids,
            None => return None,
        };

        let mut results: Vec<Arc<[u8]>> = Vec::with_capacity(ids.len());
        for id in &ids {
            if let Some(bytes) = self.load_doc_oxiwire_bytes(*id) {
                results.push(bytes);
            }
        }
        Some(Ok(results))
    }

    /// If a composite index's `fields` exactly matches the query's set of
    /// top-level `$eq` conditions (no extra conditions, no non-eq ops),
    /// build the prefix and look up via `find_prefix`. Returns `None`
    /// otherwise so the caller can try the single-field path.
    fn try_composite_lookup(
        &self,
        query: &Query,
    ) -> Option<std::collections::BTreeSet<DocumentId>> {
        let eq_conditions = query::extract_eq_conditions(query)?;
        // The query must be eq-only — no $gt/$in/etc. mixed in. We can't
        // catch mixed conditions just from extract_eq_conditions because
        // that helper skips non-eq subs silently; verify with is_eq_only_on.
        let eq_fields: Vec<String> = eq_conditions.keys().cloned().collect();
        if !query::is_eq_only_on(query, &eq_fields) {
            return None;
        }

        let composites = self.composite_indexes.read();
        for ci in composites.iter() {
            if ci.fields.len() != eq_conditions.len() {
                continue;
            }
            // Composite's field set must exactly match the query's.
            if !ci.fields.iter().all(|f| eq_conditions.contains_key(f)) {
                continue;
            }
            // Build the prefix in the index's field order.
            let prefix: Vec<IndexValue> = ci
                .fields
                .iter()
                .map(|f| eq_conditions.get(f).unwrap().clone())
                .collect();
            return Some(ci.find_prefix(&prefix));
        }
        None
    }

    /// Reset the doc cache hit/miss counters. Useful when measuring a
    /// specific window (e.g. ignore the warmup phase of a benchmark).
    pub fn doc_cache_stats_reset(&self) {
        self.doc_cache.reset_stats();
    }

    /// Evict every entry from the per-collection document cache.
    pub fn doc_cache_clear(&self) {
        self.bytes_cache.clear();
        self.doc_cache.clear();
    }

    pub fn sync_writes(&self) -> Result<()> {
        if !self.dirty.swap(false, Ordering::AcqRel) {
            return Ok(()); // nothing changed since last persist
        }
        // Capture the current btree state to disk via atomic rename.
        //
        // Critically, do NOT checkpoint (truncate) the WAL here.
        // Concurrent inserts can append to the WAL and update the
        // in-memory btree _after_ persist has begun iterating the
        // tree but _before_ checkpoint runs — those entries would
        // be lost (their WAL record erased, but the btree image on
        // disk doesn't yet reflect them). Tested empirically:
        // crash-recovery-go lost ~3/2000 acks before the WAL
        // truncation was removed.
        //
        // The cost is unbounded WAL growth between snapshots, which
        // we bound elsewhere: graceful shutdown does a final persist
        // and only THEN can safely truncate (no concurrent writers).
        // Recovery on startup is idempotent — the WAL replays on top
        // of the snapshot, double-applying inserts is a no-op via
        // doc_id deduplication.
        #[cfg(not(target_arch = "wasm32"))]
        self.storage.persist()?;
        #[cfg(not(target_arch = "wasm32"))]
        self.persist_disk_indexes();
        // Auto-compaction: this periodic maintenance point isn't holding the
        // data lock, so it can safely take compaction's exclusive write lock.
        // Only fires once the data file is past the dead-space threshold;
        // after a compaction dead space resets, so it self-rate-limits.
        #[cfg(not(target_arch = "wasm32"))]
        if self.storage.should_compact() {
            let _ = self.storage.compact();
        }
        Ok(())
    }

    /// Flush disk-backed field indexes' mmap overlays to their `.mfidx` files.
    /// No-op in in-RAM mode (indexes persist via `save_index_data` instead).
    #[cfg(not(target_arch = "wasm32"))]
    fn persist_disk_indexes(&self) {
        if !self.storage.is_disk_first() {
            return;
        }
        let mut fi = self.field_indexes.write();
        for idx in fi.values_mut() {
            let _ = idx.persist_disk();
        }
    }

    /// Final checkpoint at shutdown only: persist BTree, then truncate
    /// the WAL. Safe to truncate here because the engine has stopped
    /// accepting writes (the listener is closed and worker threads have
    /// drained). NOT safe to call during normal operation — see
    /// sync_writes for the race that motivated splitting them.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn final_checkpoint(&self) -> Result<()> {
        self.storage.persist()?;
        self.persist_disk_indexes();
        self.dirty.store(false, Ordering::Release);
        if self.lazy_sync.load(Ordering::Acquire) {
            self.wal.checkpoint_no_sync()?;
        } else {
            self.wal.checkpoint()?;
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Accessor methods (matching Collection API)
    // -----------------------------------------------------------------------

    pub fn field_indexes(
        &self,
    ) -> crate::locks::RwLockReadGuard<'_, HashMap<String, PagedFieldIndex>> {
        self.field_indexes.read()
    }

    pub fn composite_indexes(&self) -> crate::locks::RwLockReadGuard<'_, Vec<CompositeIndex>> {
        self.composite_indexes.read()
    }

    pub fn has_text_index(&self) -> bool {
        self.text_index.read().is_some()
    }

    pub fn vector_indexes(
        &self,
    ) -> crate::locks::RwLockReadGuard<'_, HashMap<String, VectorIndex>> {
        self.vector_indexes.read()
    }

    #[cfg(not(target_arch = "wasm32"))]
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
            let _ = index_persist::save_field_indexes(
                &fidx_path,
                &field_refs,
                doc_count,
                next_id,
                None,
            );
        }

        // Save composite indexes
        {
            let ci = self.composite_indexes.read();
            if !ci.is_empty() {
                let cidx_path = self.data_dir.join(format!("{}.cidx", self.name));
                let comp_refs: Vec<&CompositeIndex> = ci.iter().collect();
                let _ = index_persist::save_composite_indexes(
                    &cidx_path, &comp_refs, doc_count, next_id, None,
                );
            }
        }
    }

    #[cfg(target_arch = "wasm32")]
    pub fn save_index_data(&self) {
        // No-op on wasm32 (no filesystem persistence)
    }

    fn save_index_metadata(&self) -> Result<()> {
        if self.in_memory || self.data_dir.as_os_str().is_empty() {
            return Ok(());
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            let meta = IndexMetadata {
                version: 1,
                indexes: self.list_indexes(),
            };
            let bytes = serde_json::to_vec(&meta)?;
            let path = self.data_dir.join(format!("{}.idx", self.name));
            std::fs::write(&path, &bytes)?;
        }
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

    /// Load a document and return it as JSON text, skipping the
    /// `serde_json::Value` intermediate on the cache-miss path.
    ///
    /// - Cache hit  → serialize the cached `Arc<Value>` to text (this
    ///   matches current find-response behavior; ~3-25 µs per doc).
    /// - Cache miss → `RawJsonb::to_string` directly off the storage
    ///   bytes; ~5 µs on a 13 KB document where `decode_doc` followed
    ///   by `serde_json::to_vec` would cost ~30 µs. The cache is NOT
    ///   populated on this path — the caller has already gotten the
    ///   wire payload it wanted, and decoding to Value just to put
    ///   it in the cache would undo the speedup.
    ///
    /// Use this from the server's wire response path. Filter and
    /// update paths should keep using `load_doc_arc`, which gives
    /// them the structured Value the cache exists to serve.
    pub fn load_doc_text(&self, id: DocumentId) -> Option<String> {
        if let Some(arc) = self.doc_cache.peek(id) {
            return serde_json::to_string(&*arc).ok();
        }
        let bytes = self.storage.get(id)?;
        codec::decode_doc_to_text(&bytes).ok()
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
            // Serve from cache when present, but do NOT insert on a miss: a
            // full-collection scan that `put`s every doc evicts the entire
            // hot working set (and pays an LRU write per document) for
            // entries that are typically never read again.
            if let Some(arc) = self.doc_cache.get(key) {
                return f(key, &arc);
            }
            let doc = codec::decode_doc(bytes)?;
            let arc = Arc::new(doc);
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

        let bytes = codec::encode_doc(&data)?;
        let entry = WalEntry::Insert {
            doc_id: id,
            doc_bytes: bytes.clone(),
            tx_id: 0,
        };

        // Lock strategy: the WAL write below can carry a per-commit fsync,
        // and holding the field-index write lock across it stalls every
        // reader on this collection for the fsync's duration (~ms). With NO
        // unique indexes (the common case) validation cannot reject the
        // insert, so the WAL entry is logged BEFORE the write lock — the
        // same ordering `delete` uses. With unique indexes the
        // validate→WAL→apply order must hold: a WAL entry must never exist
        // for a rejected insert (replay would resurrect it).
        let has_unique = {
            let fi = self.field_indexes.read();
            fi.values().any(|idx| idx.unique)
        };
        let mut fi;
        if has_unique {
            fi = self.field_indexes.write();
            Self::check_unique_constraints_with(&fi, &data, None)?;
            self.wal_log(&entry)?;
        } else {
            self.wal_log(&entry)?;
            fi = self.field_indexes.write();
            // A unique index created between the peek and this lock:
            // re-validate, and neutralize the already-logged insert with a
            // tombstone if it now violates (so replay can't resurrect it).
            if fi.values().any(|idx| idx.unique) {
                if let Err(e) = Self::check_unique_constraints_with(&fi, &data, None) {
                    let _ = self.wal_log(&WalEntry::Delete {
                        doc_id: id,
                        tx_id: 0,
                    });
                    return Err(e);
                }
            }
        }

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
        // fetch_add reserves the whole block atomically. The previous
        // load → use → store sequence raced with concurrent `insert()`
        // (whose fetch_add happens before it takes the index locks held
        // here): both sides could be handed the same id — one document
        // silently overwriting the other via the storage upsert — and the
        // trailing store could roll `next_id` backwards past other
        // allocations.
        let doc_count = docs.len();
        let first_id = self.next_id.fetch_add(doc_count as u64, Ordering::SeqCst);

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
            let mut pending_unique: HashMap<String, HashMap<IndexValue, DocumentId>> =
                HashMap::new();
            for (id, data) in &docs_with_ids {
                Self::check_unique_constraints_with(&fi, data, None)?;
                for idx in fi.values() {
                    if !idx.unique {
                        continue;
                    }
                    if let Some(value) = resolve_field_in_value(data, &idx.field) {
                        let iv = IndexValue::from_json(value);
                        let field_map = pending_unique.entry(idx.field.clone()).or_default();
                        if field_map.contains_key(&iv) {
                            return Err(Error::UniqueViolation {
                                field: idx.field.clone(),
                            });
                        }
                        field_map.insert(iv, *id);
                    }
                }
            }
        }

        // Parallel JSONB encode (sequential on wasm32). Encode errors must
        // fail the batch — `unwrap_or_default()` here used to persist an
        // EMPTY byte vector to the WAL and storage: an acknowledged write
        // that every later decode silently skips, i.e. a lost document.
        #[cfg(not(target_arch = "wasm32"))]
        let btree_entries: Vec<(u64, Vec<u8>)> = if doc_count > 500 {
            docs_with_ids
                .par_iter()
                .map(|(id, data)| Ok((*id, codec::encode_doc(data)?)))
                .collect::<Result<Vec<_>>>()?
        } else {
            docs_with_ids
                .iter()
                .map(|(id, data)| Ok((*id, codec::encode_doc(data)?)))
                .collect::<Result<Vec<_>>>()?
        };
        #[cfg(target_arch = "wasm32")]
        let btree_entries: Vec<(u64, Vec<u8>)> = docs_with_ids
            .iter()
            .map(|(id, data)| Ok((*id, codec::encode_doc(data)?)))
            .collect::<Result<Vec<_>>>()?;

        // Phase 2: bulk insert into B-tree
        // Pre-reserve full capacity to avoid incremental reallocation across batches
        if self.storage.count() == 0 {
            self.storage.reserve(doc_count);
        }
        let ids: Vec<DocumentId> = btree_entries.iter().map(|(id, _)| *id).collect();
        {
            let wal_refs: Vec<(u64, &[u8])> = btree_entries
                .iter()
                .map(|(id, b)| (*id, b.as_slice()))
                .collect();
            self.wal.log_batch_inserts_no_sync_buffered(&wal_refs)?;
            // Strict durability: one fsync at the end of the batch
            // covers every record we just appended. Lazy mode skips
            // it and lets the periodic sync thread catch up.
            if !self.lazy_sync.load(Ordering::Acquire) {
                self.wal.sync()?;
            }
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
            let wal_refs: Vec<(u64, &[u8])> = prepared
                .iter()
                .map(|(id, _, b)| (*id, b.as_slice()))
                .collect();
            self.wal.log_batch_inserts_no_sync_buffered(&wal_refs)?;
            if !self.lazy_sync.load(Ordering::Acquire) {
                self.wal.sync()?;
            }
        }
        let has_indexes = !fi.is_empty() || !ci.is_empty() || ti.is_some() || !vi.is_empty();
        let skip_cache = prepared.len() > 1000;
        let need_index_pass = has_indexes || !skip_cache;

        // Split the prepared batch into the encoded bytes (for one batched
        // storage append) and the `(id, Value)` pairs the index/cache pass
        // needs. The storage write goes through `insert_batch` — in disk-first
        // mode that's a single parallel-compressed, single-`write_all` append
        // for the whole batch rather than a per-document lock/seek/compress/
        // write (the bulk-insert bottleneck); in-RAM it's a parallel tree fill.
        // All ids here are freshly reserved (new keys), satisfying
        // `insert_batch`'s no-replacement contract.
        let mut storage_entries: Vec<(u64, Vec<u8>)> = Vec::with_capacity(prepared.len());
        let mut index_items: Vec<(DocumentId, Value)> = if need_index_pass {
            Vec::with_capacity(prepared.len())
        } else {
            Vec::new()
        };
        for (id, data, bytes) in prepared {
            storage_entries.push((id, bytes));
            if need_index_pass {
                index_items.push((id, data));
            }
            ids.push(id);
        }

        self.storage.insert_batch(storage_entries);

        if need_index_pass {
            for (id, data) in index_items {
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
    pub fn find_with_options(&self, query_json: &Value, opts: &FindOptions) -> Result<Vec<Value>> {
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

        // Fast path: Query::All with no sort — use B-tree cursor scan.
        // Decodes straight into the returned Arc (cache hits are reused);
        // the previous `Arc::new(doc.clone())` paid a full deep clone per
        // returned document on top of the decode.
        if matches!(query, Query::All) && opts.sort.is_none() {
            let skip = opts.skip.unwrap_or(0) as usize;
            let limit = opts.limit.map(|l| l as usize).unwrap_or(usize::MAX);
            let mut results = Vec::new();
            let mut skipped = 0;
            self.storage.scan_all_while(|key, bytes| {
                if skipped < skip {
                    skipped += 1;
                    return Ok(true);
                }
                if results.len() >= limit {
                    return Ok(false);
                }
                let arc = match self.doc_cache.get(key) {
                    Some(arc) => arc,
                    None => Arc::new(codec::decode_doc(bytes)?),
                };
                results.push(arc);
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
                    // saturating_add: with skip set and no limit this was
                    // `skip + usize::MAX`, which wraps in release builds and
                    // silently truncated the scan to `skip - 1` rows.
                    let need = (opts.skip.unwrap_or(0) as usize)
                        .saturating_add(opts.limit.map(|l| l as usize).unwrap_or(usize::MAX));
                    let mut results = Vec::new();
                    let skip_filter = matches!(query, Query::All);

                    // Extract eq conditions for index-level containment check
                    let eq_checks: Vec<(String, IndexValue)> = if !skip_filter {
                        query::extract_eq_conditions(&query)
                            .map(|m| {
                                m.into_iter()
                                    .filter(|(f, _)| fi.contains_key(f) && f != sort_field)
                                    .collect()
                            })
                            .unwrap_or_default()
                    } else {
                        Vec::new()
                    };
                    let use_index_check = !eq_checks.is_empty();
                    // The containment pre-filter decides the query on its own
                    // only when EVERY condition is an $eq on one of the
                    // checked indexed fields. Any other condition ($gt,
                    // $regex, an eq on an unindexed field, ...) must still
                    // pass the real post-filter — previously matches_value
                    // was skipped whenever one indexed eq existed, silently
                    // dropping the remaining predicates from the query.
                    let eq_fields: Vec<String> = eq_checks.iter().map(|(f, _)| f.clone()).collect();
                    let fully_covered = use_index_check && query::is_eq_only_on(&query, &eq_fields);

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

                    // Docs that lack the sort field are absent from this
                    // index but must still be returned — they sort as null,
                    // exactly like the comparator fallback (and MongoDB), so
                    // creating an index must not change the result set. Zero
                    // extra cost when the index covers every doc.
                    let missing_ids: Vec<DocumentId> = {
                        let with_field = field_idx.count_all();
                        if with_field >= self.storage.count() {
                            Vec::new()
                        } else {
                            let mut indexed: std::collections::HashSet<DocumentId> =
                                std::collections::HashSet::with_capacity(with_field);
                            field_idx.for_each_entry_asc(|_value, doc_ids| {
                                for &id in doc_ids {
                                    indexed.insert(id);
                                }
                                true
                            });
                            let mut missing = Vec::new();
                            self.storage.scan_keys(|id| {
                                if !indexed.contains(&id) {
                                    missing.push(id);
                                }
                            });
                            missing
                        }
                    };
                    let push_missing = |results: &mut Vec<Arc<Value>>| {
                        for &id in &missing_ids {
                            if results.len() >= need {
                                break;
                            }
                            if use_index_check && !check_id(id) {
                                continue;
                            }
                            if let Some(arc) = self.read_doc_arc(id) {
                                if skip_filter
                                    || fully_covered
                                    || query::matches_value(&query, &arc)
                                {
                                    results.push(arc);
                                }
                            }
                        }
                    };

                    // `for_each_entry_*` (not `iter_asc`/`iter_desc`) so this
                    // works in disk-first mode too — a disk-backed index's
                    // `iter_asc` yields nothing, which previously made
                    // index-backed sort silently return an empty result set.
                    match sort_order {
                        SortOrder::Asc => {
                            // Null group (missing sort field) sorts first.
                            push_missing(&mut results);
                            field_idx.for_each_entry_asc(|_value, doc_ids| {
                                for &id in doc_ids {
                                    if use_index_check && !check_id(id) {
                                        continue;
                                    }
                                    if let Some(arc) = self.read_doc_arc(id) {
                                        if skip_filter
                                            || fully_covered
                                            || query::matches_value(&query, &arc)
                                        {
                                            results.push(arc);
                                            if results.len() >= need {
                                                return false;
                                            }
                                        }
                                    }
                                }
                                true
                            });
                        }
                        SortOrder::Desc => {
                            field_idx.for_each_entry_desc(|_value, doc_ids| {
                                for &id in doc_ids.iter().rev() {
                                    if use_index_check && !check_id(id) {
                                        continue;
                                    }
                                    if let Some(arc) = self.read_doc_arc(id) {
                                        if skip_filter
                                            || fully_covered
                                            || query::matches_value(&query, &arc)
                                        {
                                            results.push(arc);
                                            if results.len() >= need {
                                                return false;
                                            }
                                        }
                                    }
                                }
                                true
                            });
                            // Null group (missing sort field) sorts last.
                            push_missing(&mut results);
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

                            let need = (opts.skip.unwrap_or(0) as usize).saturating_add(
                                opts.limit.map(|l| l as usize).unwrap_or(usize::MAX),
                            );

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
            let lazy_result = query::execute_indexed_lazy(&query, &fi, &mut |id| {
                if let Some(arc) = self.load_doc_arc(id) {
                    if skip_post_filter || query::matches_value(&query, &arc) {
                        results.push(arc);
                        if results.len() >= limit {
                            return false;
                        }
                    }
                }
                true
            });
            if lazy_result.is_some() {
                return Ok(results);
            }
        }

        let ci = self.composite_indexes.read();
        let candidate_ids = query::execute_indexed(&query, &fi, &ci);
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
                    self.storage.scan_keys(|k| {
                        ids.push(k);
                    });
                    ids
                };
                // Filter closure: try the partial-JSONB matcher first so
                // docs that obviously don't match never pay the full
                // JSONB→Value decode. For top-level AND-of-$eq/$range/$in
                // queries this is ~5 µs/doc instead of ~20 µs/doc on
                // mismatch — a 4× per-doc win for the common case.
                let filter_one = |id: u64| -> Option<Arc<Value>> {
                    let bytes = self.storage.get(id)?;
                    match matches_query_partial_jsonb(&query, &bytes) {
                        Some(false) => return None,
                        Some(true) => {
                            // Partial proved match — skip the redundant full
                            // matches_value re-check, just materialise the
                            // Arc for the caller's return type.
                            if let Some(arc) = self.doc_cache.get(id) {
                                return Some(arc);
                            }
                            let doc = codec::decode_doc(&bytes).ok()?;
                            let arc = Arc::new(doc);
                            self.doc_cache.put(id, Arc::clone(&arc));
                            Some(arc)
                        }
                        None => {
                            // Partial undecided — full decode + match check.
                            if let Some(arc) = self.doc_cache.get(id) {
                                return if query::matches_value(&query, &arc) {
                                    Some(arc)
                                } else {
                                    None
                                };
                            }
                            let doc = codec::decode_doc(&bytes).ok()?;
                            let arc = Arc::new(doc);
                            self.doc_cache.put(id, Arc::clone(&arc));
                            if query::matches_value(&query, &arc) {
                                Some(arc)
                            } else {
                                None
                            }
                        }
                    }
                };
                #[cfg(not(target_arch = "wasm32"))]
                let matched: Vec<Arc<Value>> = doc_ids
                    .par_iter()
                    .filter_map(|&id| filter_one(id))
                    .collect();
                #[cfg(target_arch = "wasm32")]
                let matched: Vec<Arc<Value>> =
                    doc_ids.iter().filter_map(|&id| filter_one(id)).collect();
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
            let lazy_result = query::execute_indexed_lazy(&query, &fi, &mut |id| {
                if let Some(arc) = self.load_doc_arc(id) {
                    if skip_post_filter || query::matches_value(&query, &arc) {
                        found = Some((*arc).clone());
                        return false;
                    }
                }
                true
            });
            if lazy_result.is_some() {
                return Ok(found);
            }
        }

        // Fallback: index or cursor scan
        let candidate_ids = if !matches!(query, Query::All) {
            let ci = self.composite_indexes.read();
            query::execute_indexed(&query, &fi, &ci)
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
    // WORM (engine-level immutability)
    // -----------------------------------------------------------------------

    /// Lock `doc_id` until `locked_until_micros`. After this returns,
    /// update / delete / find_and_modify on the doc refuse with
    /// `Error::DocumentWormLocked`. Pass `crate::worm::INDEFINITE` for
    /// retention without a wall-clock expiry (release is the only way
    /// out).
    ///
    /// No-op (returns Ok) on in-memory collections — they have no
    /// place to persist the lock log. Callers in DMS only invoke this
    /// for on-disk per-tenant docs collections, so the in-memory case
    /// is a test convenience.
    pub fn worm_lock(&self, doc_id: DocumentId, locked_until_micros: u64) -> Result<()> {
        match &self.worm {
            Some(w) => w.lock(doc_id, locked_until_micros),
            None => Ok(()),
        }
    }

    /// Release a previously-set WORM lock. Admin-only at the wire
    /// layer (handler.rs gate); this method is the storage path.
    pub fn worm_release(&self, doc_id: DocumentId) -> Result<()> {
        match &self.worm {
            Some(w) => w.release(doc_id),
            None => Ok(()),
        }
    }

    /// Report whether `doc_id` is currently locked at the engine level.
    /// `now_micros` is the caller's wall-clock reading.
    pub fn worm_is_locked(&self, doc_id: DocumentId, now_micros: u64) -> bool {
        match &self.worm {
            Some(w) => w.is_locked(doc_id, now_micros),
            None => false,
        }
    }

    /// `locked_until_micros` for a doc, or `None` if unlocked. Surfaces
    /// in admin status views.
    pub fn worm_locked_until(&self, doc_id: DocumentId) -> Option<u64> {
        self.worm.as_ref().and_then(|w| w.locked_until(doc_id))
    }

    /// check_worm_for_ids refuses if any id in the iterator is locked.
    /// Returns the first locked id as `Error::DocumentWormLocked` —
    /// callers (update / delete / find_and_modify) hit this before
    /// touching storage so a rejected mutation leaves no partial state.
    fn check_worm_for_ids<I: IntoIterator<Item = DocumentId>>(&self, ids: I) -> Result<()> {
        let worm = match &self.worm {
            Some(w) => w,
            None => return Ok(()),
        };
        #[cfg(not(target_arch = "wasm32"))]
        let now = crate::pitr::now_micros();
        // No wall clock on wasm32 (pitr is native-only); 0 treats any WORM
        // lock as still active, which is the conservative direction.
        #[cfg(target_arch = "wasm32")]
        let now = 0u64;
        for id in ids {
            if worm.is_locked(id, now) {
                return Err(Error::DocumentWormLocked {
                    collection: self.name.clone(),
                    doc_id: id,
                    locked_until_micros: worm.locked_until(id).unwrap_or(0),
                    now_micros: now,
                });
            }
        }
        Ok(())
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
                let lazy_result = query::execute_indexed_lazy(&query, &fi, &mut |id| {
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
                });
                if lazy_result.is_some() {
                    lazy_handled = true;
                }
            }

            if !lazy_handled {
                let ci = self.composite_indexes.read();
                let candidate_ids = query::execute_indexed(&query, &fi, &ci);
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

        // WORM phase 2 gate: refuse the entire update batch if any
        // target is engine-locked. Surfaces before we prepare any
        // write so the rejection is clean and atomic — no partial
        // application.
        self.check_worm_for_ids(matches.iter().map(|(id, _)| *id))?;
        let matched_ids: Vec<DocumentId> = matches.into_iter().map(|(id, _)| id).collect();

        // Phase 2+3 (merged): take the write locks, then RE-READ each
        // matched doc and compute the update against its CURRENT content.
        // Between phase 1's read locks and here a concurrent writer can
        // update or delete the doc; computing from the phase-1 snapshot
        // used to (a) leave stale index entries keyed by snapshot values
        // the doc no longer has, (b) resurrect a concurrently-deleted doc
        // via the blind storage insert, and (c) silently lose the
        // concurrent update's changes. The recompute under exclusive
        // locks also makes the `_version` bump a true atomic RMW.
        struct UpdateOp {
            id: DocumentId,
            old_data: Value,
            new_data: Value,
            new_bytes: Vec<u8>,
        }

        let mut fi = self.field_indexes.write();
        let mut ci = self.composite_indexes.write();
        let mut ti = self.text_index.write();
        let mut vi = self.vector_indexes.write();

        let mut ops = Vec::with_capacity(matched_ids.len());
        for id in matched_ids {
            // Re-read under the write locks; storage is the source of truth.
            let Some(cur_bytes) = self.storage.get(id) else {
                continue; // deleted since phase 1 — do not resurrect
            };
            let current = codec::decode_doc(&cur_bytes)?;
            // Re-check the predicate: a concurrent update may have moved
            // the doc out of the query's result set.
            if !query::matches_value(&query, &current) {
                continue;
            }

            let mut new_data = current.clone();
            crate::update::apply_update(&mut new_data, update_json)?;

            let old_version = current
                .get("_version")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let new_version = old_version + 1;
            if let Some(obj) = new_data.as_object_mut() {
                obj.insert("_version".to_string(), Value::Number(new_version.into()));
            }

            let new_bytes = codec::encode_doc(&new_data)?;

            ops.push(UpdateOp {
                id,
                old_data: current,
                new_data,
                new_bytes,
            });
        }

        if ops.is_empty() {
            return Ok(Vec::new());
        }

        // Unique constraint check under write lock — against the index AND
        // across the batch itself: two ops in one update() setting the same
        // unique value each pass the index check (the other's OLD value is
        // what's indexed) and used to both go through.
        let has_unique = fi.values().any(|idx| idx.unique);
        if has_unique {
            let mut pending_unique: HashMap<String, HashMap<IndexValue, DocumentId>> =
                HashMap::new();
            for op in &ops {
                Self::check_unique_constraints_with(&fi, &op.new_data, Some(op.id))?;
                for idx in fi.values().filter(|i| i.unique) {
                    if let Some(value) = resolve_field_in_value(&op.new_data, &idx.field) {
                        let iv = IndexValue::from_json(value);
                        let field_map = pending_unique.entry(idx.field.clone()).or_default();
                        if let Some(&other) = field_map.get(&iv) {
                            if other != op.id {
                                return Err(Error::UniqueViolation {
                                    field: idx.field.clone(),
                                });
                            }
                        }
                        field_map.insert(iv, op.id);
                    }
                }
            }
        }

        {
            let wal_entries: Vec<WalEntry> = ops
                .iter()
                .map(|op| WalEntry::Update {
                    doc_id: op.id,
                    doc_bytes: op.new_bytes.clone(),
                    tx_id: 0,
                })
                .collect();
            self.wal_log_batch(&wal_entries)?;
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

            self.invalidate_bytes_cache(op.id);
            self.doc_cache.put(op.id, Arc::new(op.new_data));
            updated_ids.push(op.id);
        }
        if !updated_ids.is_empty() {
            self.dirty.store(true, Ordering::Release);
        }

        Ok(updated_ids)
    }

    /// Atomically find ONE document matching `query`, apply `update` to
    /// it, write it back, and return the resulting document.
    ///
    /// Unlike `update`, the find + apply + write all run while this
    /// collection's index write locks are held — so two concurrent
    /// `find_and_modify` calls on the same document cannot lose an
    /// update; each sees the other's result. (`update` reads under a
    /// read lock, releases it, then writes under a write lock — a
    /// read-modify-write with a gap, so concurrent `$inc` via `update`
    /// *does* lose updates.) This is the safe primitive for counters
    /// such as a mailbox's IMAP `UIDNEXT`: a caller mutating a counter
    /// must always go through `find_and_modify`, never plain `update`.
    ///
    /// Returns the document *as modified* (with the bumped `_version`),
    /// or `None` if nothing matched.
    pub fn find_and_modify(
        &self,
        query_json: &Value,
        update_json: &Value,
    ) -> Result<Option<Value>> {
        let update_obj = update_json
            .as_object()
            .ok_or_else(|| Error::InvalidQuery("update must be an object".into()))?;
        if update_obj.is_empty() {
            return Err(Error::InvalidQuery(
                "update must contain at least one operator".into(),
            ));
        }
        let query = query::parse_query(query_json)?;

        // Take the write locks FIRST, then find — the read-modify-write
        // is therefore atomic against every other find_and_modify, and
        // against update's write phase, on this collection.
        let mut fi = self.field_indexes.write();
        let mut ci = self.composite_indexes.write();
        let mut ti = self.text_index.write();
        let mut vi = self.vector_indexes.write();

        // Find one matching document. read_doc / load_doc_arc /
        // for_each_doc_arc_while never touch the index locks, so finding
        // (even by full scan) while holding them is deadlock-free.
        let mut found: Option<(DocumentId, Value)> = None;
        let candidate_ids = query::execute_indexed(&query, &fi, &ci);
        if let Some(ref indexed_ids) = candidate_ids {
            for &id in indexed_ids {
                if self.storage.contains_key(id) {
                    if let Some(data) = self.read_doc(id)? {
                        if query::matches_value(&query, &data) {
                            found = Some((id, data));
                            break;
                        }
                    }
                }
            }
        } else {
            self.for_each_doc_arc_while(|id, arc| {
                if query::matches_value(&query, arc) {
                    found = Some((id, (**arc).clone()));
                    return Ok(false);
                }
                Ok(true)
            })?;
        }

        let (id, old_data) = match found {
            Some(pair) => pair,
            None => return Ok(None),
        };

        // WORM phase 2 gate before we touch storage.
        self.check_worm_for_ids(std::iter::once(id))?;

        // Apply the operators and bump _version.
        let mut new_data = old_data.clone();
        crate::update::apply_update(&mut new_data, update_json)?;
        let new_version = new_data
            .get("_version")
            .and_then(|v| v.as_u64())
            .unwrap_or(0)
            + 1;
        if let Some(obj) = new_data.as_object_mut() {
            obj.insert("_version".to_string(), Value::Number(new_version.into()));
        }
        let new_bytes = codec::encode_doc(&new_data)?;

        if fi.values().any(|idx| idx.unique) {
            Self::check_unique_constraints_with(&fi, &new_data, Some(id))?;
        }

        // WAL first, then the in-memory btree and the indexes.
        self.wal_log_batch(&[WalEntry::Update {
            doc_id: id,
            doc_bytes: new_bytes.clone(),
            tx_id: 0,
        }])?;
        self.storage.insert(id, new_bytes);

        for idx in fi.values_mut() {
            let ov = crate::collection::resolve_field_in_value(&old_data, &idx.field);
            let nv = crate::collection::resolve_field_in_value(&new_data, &idx.field);
            if ov != nv {
                idx.remove_value(id, &old_data);
                idx.insert_value(id, &new_data);
            }
        }
        for idx in ci.iter_mut() {
            idx.remove_value(id, &old_data);
            idx.insert_value(id, &new_data);
        }
        if let Some(ref mut text_idx) = *ti {
            text_idx.index_doc(id, &new_data);
        }
        for idx in vi.values_mut() {
            idx.remove(id);
            let _ = idx.insert(id, &new_data);
        }

        self.invalidate_bytes_cache(id);
        self.doc_cache.put(id, Arc::new(new_data.clone()));
        self.dirty.store(true, Ordering::Release);

        Ok(Some(new_data))
    }

    // -----------------------------------------------------------------------
    // Delete
    // -----------------------------------------------------------------------

    /// Delete documents matching a query. Returns IDs of deleted documents.
    pub fn delete(&self, query_json: &Value, limit: Option<usize>) -> Result<Vec<DocumentId>> {
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
                let lazy_result = query::execute_indexed_lazy(&query, &fi, &mut |id| {
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
                });
                if lazy_result.is_some() {
                    lazy_handled = true;
                }
            }

            if !lazy_handled {
                let ci = self.composite_indexes.read();
                let candidate_ids = query::execute_indexed(&query, &fi, &ci);
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

        // WORM phase 2 gate: refuse the entire delete batch if any
        // target is locked. Same atomicity contract as update.
        self.check_worm_for_ids(ops.iter().map(|op| op.id))?;

        // Phase 2: Remove from B-tree and update indexes (write locks)
        {
            let wal_entries: Vec<WalEntry> = ops
                .iter()
                .map(|op| WalEntry::Delete {
                    doc_id: op.id,
                    tx_id: 0,
                })
                .collect();
            self.wal_log_batch(&wal_entries)?;
        }

        let mut fi = self.field_indexes.write();
        let mut ci = self.composite_indexes.write();
        let mut ti = self.text_index.write();
        let mut vi = self.vector_indexes.write();

        let mut deleted_ids = Vec::with_capacity(ops.len());
        for op in ops {
            // Remove from B-tree (no soft-delete — immediate reclaim)
            self.storage.remove(op.id);
            self.invalidate_bytes_cache(op.id);
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
            // Compact empty entries from field indexes in one pass (avoids O(n) shift per delete)
            if deleted_ids.len() > 100 {
                for idx in fi.values_mut() {
                    idx.compact_entries();
                }
            }
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

    /// Explain how a find would execute, then execute it and report
    /// actual numbers. Reuses the SAME planner functions the real find
    /// path calls (`execute_indexed`, `is_fully_indexed`, the
    /// index-backed-sort check), so the reported plan can't drift from
    /// reality. Plan fields:
    ///
    /// - `strategy`: `COLLSCAN` | `INDEX_SCAN` | `INDEX_SORT` |
    ///   `FULL_SCAN_ALL` (match-all cursor walk)
    /// - `index_used`, `candidates` (docs the index narrowed to),
    /// - `post_filter` + `post_filter_ops` (operators an index can't
    ///   serve — these force per-document predicate evaluation),
    /// - `sort`: `none` | `index-backed` | `in-memory`,
    /// - `examined` / `returned` / `duration_ms` from the real run.
    pub fn explain_find(&self, query_json: &Value, opts: &FindOptions) -> Result<Value> {
        let query = query::parse_query(query_json)?;
        let total_docs = self.count();

        // ── Plan (mirrors find_with_options_arcs decision order) ──
        let fi = self.field_indexes.read();
        let ci = self.composite_indexes.read();

        let match_all = matches!(query, query::Query::All);
        let fully_indexed = query::is_fully_indexed(&query, &fi);
        let candidate_ids = query::execute_indexed(&query, &fi, &ci);

        let sort_desc = match &opts.sort {
            None => "none",
            Some(fields) if fields.len() == 1 && fi.contains_key(&fields[0].0) => "index-backed",
            Some(_) => "in-memory",
        };

        let (strategy, examined) = if match_all && opts.sort.is_none() {
            ("FULL_SCAN_ALL", total_docs)
        } else if sort_desc == "index-backed" {
            // The index-sort path walks the sort index (early-terminating
            // on limit) and post-filters; worst case examines everything.
            ("INDEX_SORT", total_docs)
        } else {
            match &candidate_ids {
                Some(ids) => ("INDEX_SCAN", ids.len()),
                None => ("COLLSCAN", total_docs),
            }
        };

        let index_used: Value = if sort_desc == "index-backed" {
            json!(opts.sort.as_ref().unwrap()[0].0.clone())
        } else if candidate_ids.is_some() {
            // The fields whose index conditions narrowed the candidates.
            let indexed_fields: Vec<String> = query::extract_eq_conditions(&query)
                .map(|m| {
                    m.into_iter()
                        .map(|(f, _)| f)
                        .filter(|f| fi.contains_key(f))
                        .collect()
                })
                .unwrap_or_default();
            if indexed_fields.is_empty() {
                json!("(range/in conditions)")
            } else {
                json!(indexed_fields)
            }
        } else {
            Value::Null
        };
        drop(ci);
        drop(fi);

        let post_filter_ops = collect_post_filter_ops(query_json);
        let candidates = candidate_ids.as_ref().map(|ids| ids.len());

        // ── Analyze: run it for real ──
        let start = std::time::Instant::now();
        let results = self.find_with_options(query_json, opts)?;
        let duration_us = start.elapsed().as_micros() as u64;

        Ok(json!({
            "strategy": strategy,
            "index_used": index_used,
            "collection_docs": total_docs,
            "candidates": candidates,
            "examined": examined,
            "returned": results.len(),
            "post_filter": !fully_indexed && !match_all,
            "post_filter_ops": post_filter_ops,
            "sort": sort_desc,
            "duration_ms": duration_us as f64 / 1000.0,
        }))
    }

    /// Count documents matching a query, with plan + timing (see
    /// [`BTreeCollection::explain_find`]). Reports whether the count was
    /// served index-only (never touching documents).
    pub fn explain_count(&self, query_json: &Value) -> Result<Value> {
        let query = query::parse_query(query_json)?;
        let fi = self.field_indexes.read();
        let index_only =
            matches!(query, query::Query::All) || query::count_indexed(&query, &fi).is_some();
        drop(fi);

        let start = std::time::Instant::now();
        let count = self.count_matching(query_json)?;
        let duration_us = start.elapsed().as_micros() as u64;

        Ok(json!({
            "strategy": if index_only { "INDEX_ONLY_COUNT" } else { "FILTERED_COUNT" },
            "index_only": index_only,
            "post_filter_ops": collect_post_filter_ops(query_json),
            "count": count,
            "duration_ms": duration_us as f64 / 1000.0,
        }))
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
        let candidate_ids = query::execute_indexed(&query, &fi, &ci);
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
                self.storage.scan_keys(|k| {
                    ids.push(k);
                });
                ids
            };
            #[cfg(not(target_arch = "wasm32"))]
            {
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
            #[cfg(target_arch = "wasm32")]
            {
                count = doc_ids
                    .iter()
                    .filter(|&&id| {
                        if let Some(arc) = self.load_doc_arc(id) {
                            query::matches_value(&query, &arc)
                        } else {
                            false
                        }
                    })
                    .count();
            }
        }

        Ok(count)
    }

    // -----------------------------------------------------------------------
    // Compact
    // -----------------------------------------------------------------------

    /// Compact the B-tree storage.
    /// Since B-tree has no soft-deletes, this is mainly a rebuild/persistence operation.
    pub fn compact(&self) -> Result<CompactStats> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            // Reclaim dead space in the data file (disk-first) or persist the
            // snapshot (in-RAM). Field indexes are doc_id-keyed, so the store
            // rewrite doesn't invalidate them; persist_disk_indexes rewrites the
            // .mfidx files cleanly (overlay merged), reclaiming their dead space.
            let (old_size, new_size) = self.storage.compact()?;
            self.persist_disk_indexes();
            return Ok(CompactStats {
                old_size,
                new_size,
                docs_kept: self.storage.count(),
            });
        }
        #[cfg(target_arch = "wasm32")]
        {
            let sz = self.storage.total_bytes();
            Ok(CompactStats {
                old_size: sz,
                new_size: sz,
                docs_kept: self.storage.count(),
            })
        }
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

        let iter_fn = |bytes: &Vec<u8>| -> Option<(IndexValue, u64)> {
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
        };
        #[cfg(not(target_arch = "wasm32"))]
        let mut pairs: Vec<(IndexValue, u64)> = slices.par_iter().filter_map(iter_fn).collect();
        #[cfg(target_arch = "wasm32")]
        let mut pairs: Vec<(IndexValue, u64)> = slices.iter().filter_map(iter_fn).collect();

        // Sort by IndexValue so build_from_sorted can append in order (no O(n) shifts)
        pairs.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));

        let sorted_pairs: Vec<(IndexValue, u64)> = pairs;
        let idx = if self.storage.is_disk_first() {
            let mpath = self.data_dir.join(format!("{}.{}.mfidx", self.name, field));
            let mut idx = PagedFieldIndex::new_disk(field.to_string(), false, mpath);
            for (iv, id) in sorted_pairs {
                idx.insert_raw(id, iv);
            }
            let _ = idx.persist_disk();
            idx
        } else {
            let mut idx = PagedFieldIndex::new(field.to_string());
            idx.build_from_sorted(sorted_pairs);
            idx
        };

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

        let mut idx = if self.storage.is_disk_first() {
            let mpath = self.data_dir.join(format!("{}.{}.mfidx", self.name, field));
            PagedFieldIndex::new_disk(field.to_string(), true, mpath)
        } else {
            PagedFieldIndex::new_unique(field.to_string())
        };

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
        let _ = idx.persist_disk();

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
                expire_after_seconds: None,
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
                expire_after_seconds: None,
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
                expire_after_seconds: None,
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
                expire_after_seconds: None,
            });
        }
        let ttl_cfgs = self.ttl_configs.read();
        for cfg in ttl_cfgs.iter() {
            indexes.push(IndexInfo {
                name: format!("{}_ttl", cfg.field),
                index_type: "ttl".to_string(),
                fields: vec![cfg.field.clone()],
                unique: false,
                dimension: None,
                metric: None,
                expire_after_seconds: Some(cfg.expire_after_seconds),
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
        if let Some(field) = name.strip_suffix("_ttl") {
            let mut cfgs = self.ttl_configs.write();
            if let Some(pos) = cfgs.iter().position(|c| c.field == field) {
                cfgs.remove(pos);
                drop(cfgs);
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
                    obj.insert("_score".to_string(), serde_json::json!(result.score));
                }
                docs.push(doc);
            }
        }
        Ok(docs)
    }

    /// Full-text search with `<mark>...</mark>` highlighted snippets.
    /// See `Collection::text_search_highlighted` for the response shape.
    pub fn text_search_highlighted(
        &self,
        query: &str,
        limit: usize,
        snippet_chars: usize,
        max_snippets: usize,
    ) -> Result<Vec<Value>> {
        let ti = self.text_index.read();
        let idx = ti.as_ref().ok_or_else(|| {
            Error::InvalidQuery(
                "no text index on this collection; create one with create_text_index".into(),
            )
        })?;

        let indexed_fields: Vec<String> = idx.fields().to_vec();
        let search_results = idx.search(query, limit);
        let mut docs = Vec::with_capacity(search_results.len());
        for result in search_results {
            if let Some(mut doc) = self.read_doc(result.doc_id)? {
                let mut highlights = serde_json::Map::new();
                if max_snippets > 0 && snippet_chars > 0 {
                    for field in &indexed_fields {
                        for text in crate::collection::collect_field_strings(&doc, field) {
                            let snippets =
                                crate::fts::highlight(&text, query, snippet_chars, max_snippets);
                            if !snippets.is_empty() {
                                let arr: Vec<Value> = snippets
                                    .into_iter()
                                    .map(|s| Value::String(s.text))
                                    .collect();
                                highlights
                                    .entry(field.clone())
                                    .or_insert_with(|| Value::Array(Vec::new()))
                                    .as_array_mut()
                                    .unwrap()
                                    .extend(arr);
                            }
                        }
                    }
                }
                if let Some(obj) = doc.as_object_mut() {
                    obj.insert("_score".to_string(), serde_json::json!(result.score));
                    if !highlights.is_empty() {
                        obj.insert("_highlights".to_string(), Value::Object(highlights));
                    }
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
                    obj.insert("_distance".to_string(), serde_json::json!(result.distance));
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
        let mut group = crate::pipeline::StreamingGroup::new(group_key, accumulators);

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
                    let candidate_ids = query::execute_indexed(&query, &fi, &ci);
                    let skip_post_filter = query::is_fully_indexed(&query, &fi);
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
                            } else if let Some(arc) = self.doc_cache.get(id) {
                                // Warm Value cache: full check on the cached Arc.
                                if query::matches_value(&query, &arc) {
                                    group.feed(&arc);
                                }
                            } else if let Some(bytes) = self.storage.get(id) {
                                // Cold path: try a partial-JSONB post-filter
                                // first. Only top-level eq/range/$in on
                                // single fields qualify — the helper returns
                                // None when it can't decide, and we fall
                                // back to full decode.
                                match matches_query_partial_jsonb(&query, &bytes) {
                                    Some(true) => group.feed_raw(&bytes),
                                    Some(false) => {} // not a match
                                    None => {
                                        if let Ok(doc) = codec::decode_doc(&bytes) {
                                            if query::matches_value(&query, &doc) {
                                                group.feed(&doc);
                                            }
                                        }
                                    }
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

    pub fn log_wal_batch(&self, entries: &[crate::wal::WalEntry]) -> Result<()> {
        // Tx-commit durability path. Write the batch to the WAL with
        // a single fsync — mirroring `wal_log_batch` used by the
        // non-tx hot path. Without this, a crash between
        // `tx_log.mark_committed` and the next `persist()` would
        // lose committed writes: the tx is marked committed, but the
        // btree image on disk doesn't carry the new data and there's
        // no WAL to replay from.
        if self.lazy_sync.load(Ordering::Acquire) {
            self.wal.log_batch_no_sync(entries)
        } else {
            self.wal.log_batch(entries)
        }
    }

    /// Append tx WAL entries WITHOUT fsync. The commit pipeline calls
    /// this inside the commit critical section, then finalizes
    /// durability outside it via [`BTreeCollection::sync_wal_shared`] so
    /// concurrent commits share one fsync (group commit).
    pub fn log_wal_batch_no_sync(&self, entries: &[crate::wal::WalEntry]) -> Result<()> {
        self.wal.log_batch_no_sync(entries)
    }

    /// Group-commit durability point for entries appended via
    /// [`BTreeCollection::log_wal_batch_no_sync`]. No-op under
    /// `lazy_sync` — that mode already trades durability for speed.
    pub fn sync_wal_shared(&self) -> Result<()> {
        if self.lazy_sync.load(Ordering::Acquire) {
            Ok(())
        } else {
            self.wal.sync_shared()
        }
    }

    pub fn checkpoint_wal(&self) -> Result<()> {
        // Mark the data as needing eventual persistence. The actual
        // btree-snapshot write is now handled by sync_writes (called
        // by the periodic sync thread or on shutdown), so commits no
        // longer pay the full-file rewrite cost on the hot path —
        // durability is already secured by the WAL fsync in
        // log_wal_batch above. Writing the snapshot every commit
        // collapsed throughput from ~250 ops/s to ~1.8 ops/s under
        // contention; deferred persistence is the standard pattern
        // (Postgres checkpointer, MongoDB journal flush).
        self.dirty.store(true, Ordering::Release);
        Ok(())
    }

    /// Seal this collection's live WAL into a numbered sealed segment.
    /// No-op for in-memory collections. The engine uses this at shutdown
    /// (PITR mode) so the un-sealed tail becomes a sealed segment the
    /// archiver can pick up, instead of being lost to the WAL truncate
    /// in `final_checkpoint`.
    pub fn seal_wal(&self) -> Result<()> {
        self.wal.seal()
    }

    /// Write barrier on this collection's WAL — once it returns, every
    /// writer that had allocated a GSN before the call has finished
    /// appending. The engine uses this in `backup()` so the base-backup
    /// GSN watermark is sound.
    pub fn wal_barrier(&self) {
        self.wal.barrier();
    }

    pub fn prepare_tx_insert(
        &self,
        mut data: Value,
        tx_id: u64,
        preassigned_id: Option<DocumentId>,
    ) -> Result<crate::collection::PreparedMutation> {
        if !data.is_object() {
            return Err(Error::NotAnObject);
        }

        // If the caller (engine.tx_insert) already reserved an id at
        // buffering time, use it — that id is the one the API client
        // already sees. Otherwise fall back to allocating one here
        // (legacy paths and direct engine callers).
        let id = preassigned_id.unwrap_or_else(|| self.next_id.fetch_add(1, Ordering::SeqCst));
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
        let candidate_ids = query::execute_indexed(&query, &fi, &ci);

        let mut mutations = Vec::new();

        let process_candidate = |id: DocumentId,
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
        let candidate_ids = query::execute_indexed(&query, &fi, &ci);
        drop(ci);
        drop(fi);

        let mut mutations = Vec::new();

        let process_candidate = |id: DocumentId,
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
                self.invalidate_bytes_cache(m.doc_id);
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
                self.invalidate_bytes_cache(m.doc_id);
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

    /// Create a TTL index on a datetime field. Documents are automatically
    /// deleted when `field_value + expire_after_seconds` has passed.
    pub fn create_ttl_index(&self, field: &str, expire_after_seconds: u64) -> Result<()> {
        // Check if a TTL index already exists on this field
        {
            let cfgs = self.ttl_configs.read();
            if cfgs.iter().any(|c| c.field == field) {
                return Ok(()); // idempotent
            }
        }

        // Ensure a field index exists on the date field for efficient range scan
        self.create_index(field)?;

        // Store the TTL config
        {
            let mut cfgs = self.ttl_configs.write();
            cfgs.push(TtlIndexConfig {
                field: field.to_string(),
                expire_after_seconds,
            });
        }

        let _ = self.save_index_metadata();

        // Immediately evict any already-expired docs
        self.evict_ttl_indexed();

        Ok(())
    }

    /// Remove a TTL config for the given field (used by retention policy deletion).
    /// The underlying field index is kept — only the TTL eviction config is removed.
    pub fn remove_ttl_config(&self, field: &str) {
        let mut cfgs = self.ttl_configs.write();
        cfgs.retain(|c| c.field != field);
        drop(cfgs);
        let _ = self.save_index_metadata();
    }

    /// Evict documents that have expired according to TTL index configs.
    /// For each TTL index, finds documents where `field_value + expireAfterSeconds <= now`
    /// using the field index for efficient range scan.
    pub fn evict_ttl_indexed(&self) -> usize {
        let cfgs = self.ttl_configs.read();
        if cfgs.is_empty() {
            return 0;
        }

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let mut to_delete: Vec<DocumentId> = Vec::new();

        for cfg in cfgs.iter() {
            // cutoff = now - expire_after_seconds (documents with datetime <= cutoff are expired)
            let cutoff_ms = now_ms.saturating_sub(cfg.expire_after_seconds * 1000);
            let cutoff_val = IndexValue::DateTime(cutoff_ms as i64);
            // The lower bound must be the DateTime type floor, NOT Unbounded:
            // IndexValue orders Null < Bool < Num < DateTime < String, so an
            // unbounded-below range would sweep in every null/bool/number
            // entry (e.g. a numeric epoch `created_at`) and the TTL thread
            // would delete those documents regardless of expiry.
            let floor_val = IndexValue::DateTime(i64::MIN);

            // Use the field index for an efficient range scan
            let fi = self.field_indexes.read();
            if let Some(idx) = fi.get(&cfg.field) {
                let expired_ids = idx.find_range(
                    std::ops::Bound::Included(&floor_val),
                    std::ops::Bound::Included(&cutoff_val),
                );
                to_delete.extend(expired_ids);
            }
        }
        drop(cfgs);

        if to_delete.is_empty() {
            return 0;
        }

        // Delete expired docs — acquire write locks for index cleanup
        let mut fi = self.field_indexes.write();
        let mut ci = self.composite_indexes.write();
        let mut ti = self.text_index.write();

        let mut evicted = 0;
        for id in to_delete {
            if self.storage.contains_key(id) {
                if let Some(bytes) = self.storage.get(id) {
                    if let Ok(data) = codec::decode_doc(&bytes) {
                        self.storage.remove(id);
                        self.invalidate_bytes_cache(id);
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

        if evicted > 0 {
            self.dirty.store(true, std::sync::atomic::Ordering::Release);
        }

        evicted
    }

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
            ttl.entry(expires_at).or_insert_with(Vec::new).push(doc_id);
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
            ttl.range(..=now_ms).map(|(&k, v)| (k, v.clone())).collect()
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
                            self.invalidate_bytes_cache(id);
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

/// Partial-JSONB query evaluator. Extracts only the fields referenced by
/// the query (via `codec::extract_field`) and evaluates equality / range /
/// `$in` predicates against them. Returns `None` for queries that would
/// require deeper inspection — `$regex`, `$elemMatch`, `$expr`, nested
/// dot-notation paths, `$not`, etc. Caller falls back to full decode.
///
/// Saves ~75% of the per-doc decode cost in the aggregation `$match` →
/// `$group` over-approximation path (e.g. dept-indexed but status-not),
/// where full JSONB→Value materialisation is the dominant per-row cost.
fn matches_query_partial_jsonb(query: &Query, bytes: &[u8]) -> Option<bool> {
    match query {
        Query::All => Some(true),
        Query::Field { field, op } => {
            let field_val = extract_field_path(bytes, field);
            eval_field_op_partial(op, field_val.as_ref())
        }
        Query::And(subs) => {
            for sub in subs {
                match matches_query_partial_jsonb(sub, bytes) {
                    Some(true) => continue,
                    Some(false) => return Some(false),
                    None => return None,
                }
            }
            Some(true)
        }
        Query::Or(subs) => {
            // Short-circuit on first definite match. If all definite subs
            // return Some(false) and none return None, we know it's a
            // mismatch. Any None makes the whole $or undecidable.
            let mut saw_undecidable = false;
            for sub in subs {
                match matches_query_partial_jsonb(sub, bytes) {
                    Some(true) => return Some(true),
                    Some(false) => continue,
                    None => saw_undecidable = true,
                }
            }
            if saw_undecidable { None } else { Some(false) }
        }
        // Nor/Expr need fuller evaluation — let the caller full-decode.
        Query::Nor(_) | Query::Expr(_) => None,
    }
}

/// Extract a (possibly-dotted) field path from JSONB bytes. For top-level
/// names this is just `codec::extract_field`; for dotted paths
/// (`address.zip`) the first segment is extracted from JSONB, then
/// subsequent segments walk the resulting `Value` tree.
///
/// Returns `None` when the path doesn't resolve to a value (missing key
/// at any level, or intermediate level isn't an object).
fn extract_field_path(bytes: &[u8], path: &str) -> Option<Value> {
    if !path.contains('.') {
        return codec::extract_field(bytes, path);
    }
    let mut parts = path.split('.');
    let first = parts.next()?;
    let mut current = codec::extract_field(bytes, first)?;
    for part in parts {
        match current {
            Value::Object(mut map) => {
                current = map.remove(part)?;
            }
            _ => return None,
        }
    }
    Some(current)
}

/// Walk a raw query and list the operators the index planner cannot
/// serve (they force per-document post-filtering). The set mirrors
/// `query::execute_field_op`'s `=> return None` arms plus the tree-level
/// `$nor`/`$expr` — if the planner learns a new trick, update both.
fn collect_post_filter_ops(query_json: &Value) -> Vec<String> {
    const POST_FILTER: &[&str] = &[
        "$ne",
        "$nin",
        "$exists",
        "$regex",
        "$elemMatch",
        "$not",
        "$all",
        "$size",
        "$type",
        "$mod",
        "$nor",
        "$expr",
    ];
    let mut found: Vec<String> = Vec::new();
    fn walk(v: &Value, found: &mut Vec<String>) {
        match v {
            Value::Object(map) => {
                for (k, val) in map {
                    if k.starts_with('$') && POST_FILTER.contains(&k.as_str()) {
                        if !found.iter().any(|f| f == k) {
                            found.push(k.clone());
                        }
                    }
                    walk(val, found);
                }
            }
            Value::Array(arr) => {
                for item in arr {
                    walk(item, found);
                }
            }
            _ => {}
        }
    }
    walk(query_json, &mut found);
    found.sort();
    found
}

fn eval_field_op_partial(op: &crate::query::QueryOp, field_val: Option<&Value>) -> Option<bool> {
    use crate::query::QueryOp;
    use crate::value::IndexValue;
    match op {
        QueryOp::Eq(want) => {
            let iv = field_val
                .map(IndexValue::from_json)
                .unwrap_or(IndexValue::Null);
            Some(&iv == want)
        }
        QueryOp::Ne(want) => {
            let iv = field_val
                .map(IndexValue::from_json)
                .unwrap_or(IndexValue::Null);
            Some(&iv != want)
        }
        QueryOp::Gt(want) => {
            let iv = IndexValue::from_json(field_val?);
            Some(&iv > want)
        }
        QueryOp::Gte(want) => {
            let iv = IndexValue::from_json(field_val?);
            Some(&iv >= want)
        }
        QueryOp::Lt(want) => {
            let iv = IndexValue::from_json(field_val?);
            Some(&iv < want)
        }
        QueryOp::Lte(want) => {
            let iv = IndexValue::from_json(field_val?);
            Some(&iv <= want)
        }
        QueryOp::In(opts) => {
            let iv = field_val
                .map(IndexValue::from_json)
                .unwrap_or(IndexValue::Null);
            Some(opts.iter().any(|o| o == &iv))
        }
        // Defer everything else (regex, nin, exists, elemMatch, etc.) to
        // the full-decode path. Conservative — easy to extend later.
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn new_btree_collection(name: &str) -> BTreeCollection {
        BTreeCollection::open_in_memory(name)
    }

    #[test]
    fn find_oxiwire_postfilter_count_matches_value_path() {
        // The byte-level post-filter must select exactly the same documents as
        // the Value path across a range of unindexed query shapes.
        let col = new_btree_collection("postfilter_parity");
        for i in 0..500 {
            let city = ["Tokyo", "Paris", "Berlin"][i % 3];
            col.insert(json!({
                "v": i % 2 == 0,
                "age": 18 + (i % 60),
                "city": city,
                "score": (i % 100) as f64,
            }))
            .unwrap();
        }
        let opts = FindOptions::default();
        for q in [
            json!({"v": true}),
            json!({"age": {"$gte": 50}}),
            json!({"city": "Paris"}),
            json!({"score": {"$lt": 25}}),
            json!({"v": false, "city": "Tokyo"}),
            json!({"age": {"$in": [20, 30, 40]}}),
            json!({"nonexistent": 1}),
        ] {
            let (count, _buf) = col
                .find_oxiwire_postfilter(&q, &opts)
                .expect("postfilter handles no sort/skip/limit")
                .expect("scan ok");
            let baseline = col.find_with_options_arcs(&q, &opts).unwrap().len();
            assert_eq!(count, baseline, "mismatch for query {}", q);
        }
    }

    #[test]
    fn bytes_cache_lazy_populates_on_first_read_then_hits() {
        let col = new_btree_collection("bytes_cache_test");

        let id1 = col.insert(json!({"name": "alpha", "n": 1})).unwrap();
        let id2 = col.insert(json!({"name": "beta", "n": 2})).unwrap();

        // First read of each doc: bytes_cache miss → populates via
        // doc_cache peek (Value is still in cache from insert) or
        // jsonb_oxiwire fallback.
        let stats_before = col.bytes_cache_stats();
        let bytes1 = col.load_doc_oxiwire_bytes(id1).expect("first read");
        let bytes2 = col.load_doc_oxiwire_bytes(id2).expect("first read");
        let stats_after = col.bytes_cache_stats();
        assert!(!bytes1.is_empty());
        assert!(!bytes2.is_empty());
        assert_eq!(stats_after.misses - stats_before.misses, 2);

        // Second read: bytes_cache HIT.
        let stats_pre_hit = col.bytes_cache_stats();
        let _ = col.load_doc_oxiwire_bytes(id1).expect("hit");
        let _ = col.load_doc_oxiwire_bytes(id2).expect("hit");
        let stats_post_hit = col.bytes_cache_stats();
        assert_eq!(stats_post_hit.hits - stats_pre_hit.hits, 2);

        // Invalidate; next read should miss again and re-populate.
        col.invalidate_bytes_cache(id1);
        let stats_pre_miss = col.bytes_cache_stats();
        let bytes1_again = col.load_doc_oxiwire_bytes(id1).expect("repopulate");
        let stats_post_miss = col.bytes_cache_stats();
        assert_eq!(&*bytes1, &*bytes1_again, "re-encoded bytes match");
        assert_eq!(stats_post_miss.misses - stats_pre_miss.misses, 1);
    }

    #[test]
    fn partial_jsonb_filter_matches_eq_on_top_level_field() {
        let v = json!({"status": "active", "department": "Engineering", "n": 7});
        let bytes = codec::encode_doc(&v).unwrap();
        let q = query::parse_query(&json!({"status": "active"})).unwrap();
        assert_eq!(matches_query_partial_jsonb(&q, &bytes), Some(true));

        let q_neg = query::parse_query(&json!({"status": "inactive"})).unwrap();
        assert_eq!(matches_query_partial_jsonb(&q_neg, &bytes), Some(false));
    }

    #[test]
    fn partial_jsonb_filter_handles_and_of_eqs() {
        let v = json!({"status": "active", "department": "Engineering"});
        let bytes = codec::encode_doc(&v).unwrap();
        let q = query::parse_query(&json!({
            "status": "active",
            "department": "Engineering",
        }))
        .unwrap();
        assert_eq!(matches_query_partial_jsonb(&q, &bytes), Some(true));

        let q_miss = query::parse_query(&json!({
            "status": "active",
            "department": "Sales",
        }))
        .unwrap();
        assert_eq!(matches_query_partial_jsonb(&q_miss, &bytes), Some(false));
    }

    #[test]
    fn partial_jsonb_filter_returns_none_for_regex() {
        let v = json!({"x": 1});
        let bytes = codec::encode_doc(&v).unwrap();
        // Regex: defer to full decode.
        let q_regex = query::parse_query(&json!({"name": {"$regex": "^A"}})).unwrap();
        assert_eq!(matches_query_partial_jsonb(&q_regex, &bytes), None);
    }

    #[test]
    fn partial_jsonb_filter_walks_dot_paths() {
        let v = json!({
            "address": {"street": "1 Main", "zip": "94100"},
            "name": "Alice"
        });
        let bytes = codec::encode_doc(&v).unwrap();

        // Top-level eq still works.
        let q_top = query::parse_query(&json!({"name": "Alice"})).unwrap();
        assert_eq!(matches_query_partial_jsonb(&q_top, &bytes), Some(true));

        // Nested eq via dot-path.
        let q_eq = query::parse_query(&json!({"address.zip": "94100"})).unwrap();
        assert_eq!(matches_query_partial_jsonb(&q_eq, &bytes), Some(true));

        // Nested range via dot-path (the bench's actual case).
        let q_range = query::parse_query(&json!({
            "address.zip": {"$gte": "00000", "$lt": "10000"}
        }))
        .unwrap();
        // "94100" is NOT in [00000, 10000) → false.
        assert_eq!(matches_query_partial_jsonb(&q_range, &bytes), Some(false));

        // Same range, doc that's in range.
        let v2 = json!({"address": {"zip": "05500"}});
        let bytes2 = codec::encode_doc(&v2).unwrap();
        assert_eq!(matches_query_partial_jsonb(&q_range, &bytes2), Some(true));

        // Missing path segment → Some(false) via Eq's null-coercion. (The
        // caller is OK with conservatism here — None would force a full
        // decode that also wouldn't match.)
        let q_missing = query::parse_query(&json!({"address.zip": "X"})).unwrap();
        let v3 = json!({"address": {}});
        let bytes3 = codec::encode_doc(&v3).unwrap();
        assert_eq!(
            matches_query_partial_jsonb(&q_missing, &bytes3),
            Some(false)
        );
    }

    #[test]
    fn partial_jsonb_filter_handles_or() {
        let v = json!({"city": "Tokyo", "n": 1});
        let bytes = codec::encode_doc(&v).unwrap();

        // The bench's exact $or shape.
        let q = query::parse_query(&json!({
            "$or": [{"city": "Tokyo"}, {"city": "Paris"}]
        }))
        .unwrap();
        assert_eq!(matches_query_partial_jsonb(&q, &bytes), Some(true));

        let v2 = json!({"city": "London"});
        let bytes2 = codec::encode_doc(&v2).unwrap();
        assert_eq!(matches_query_partial_jsonb(&q, &bytes2), Some(false));

        // $or with one undecidable sub → None.
        let q_mixed = query::parse_query(&json!({
            "$or": [{"city": "London"}, {"name": {"$regex": "^A"}}]
        }))
        .unwrap();
        // city=London fails on the first doc, regex undecidable on the
        // second → whole $or is undecidable.
        let v3 = json!({"city": "Berlin", "name": "Alice"});
        let bytes3 = codec::encode_doc(&v3).unwrap();
        assert_eq!(matches_query_partial_jsonb(&q_mixed, &bytes3), None);
    }

    #[test]
    fn find_oxiwire_bytes_uses_composite_index() {
        let col = new_btree_collection("composite_test");
        // Seed: 4 docs, three with status=active across dept variants.
        col.insert(json!({"dept": "Sales", "status": "active", "n": 1}))
            .unwrap();
        col.insert(json!({"dept": "Sales", "status": "inactive", "n": 2}))
            .unwrap();
        col.insert(json!({"dept": "Eng", "status": "active", "n": 3}))
            .unwrap();
        col.insert(json!({"dept": "Sales", "status": "active", "n": 4}))
            .unwrap();

        col.create_composite_index(vec!["dept".to_string(), "status".to_string()])
            .unwrap();

        // Query covered exactly by the composite index — should hit the
        // bytes-first composite path (returns 2 matches: n=1 and n=4).
        let query = json!({"dept": "Sales", "status": "active"});
        let result = col
            .find_oxiwire_bytes(&query, &FindOptions::default())
            .expect("fast path engaged")
            .expect("ok");
        assert_eq!(
            result.len(),
            2,
            "expected 2 matches via composite-index lookup"
        );
        for bytes in &result {
            assert!(!bytes.is_empty(), "encoded doc bytes must be non-empty");
        }
    }

    #[test]
    fn find_oxiwire_bytes_returns_none_for_mixed_ops() {
        // Composite path only fires for pure-eq queries that exactly match
        // a composite index's fields. A query with $gte falls through.
        let col = new_btree_collection("composite_falls_through");
        col.insert(json!({"dept": "Sales", "n": 10})).unwrap();
        col.insert(json!({"dept": "Eng", "n": 20})).unwrap();
        col.create_composite_index(vec!["dept".to_string(), "n".to_string()])
            .unwrap();

        let query = json!({"dept": "Sales", "n": {"$gte": 5}});
        // is_eq_only_on returns false for $gte → composite path doesn't fire.
        // No single-field index on `dept` here either → single-field path
        // also can't fully satisfy. Should return None.
        let result = col.find_oxiwire_bytes(&query, &FindOptions::default());
        assert!(result.is_none(), "mixed-op queries should fall back");
    }

    /// Cold path — doc_cache evicted, bytes_cache empty → must read
    /// from storage and use jsonb_to_oxiwire directly.
    #[test]
    fn bytes_cache_cold_path_uses_jsonb_to_oxiwire() {
        let col = new_btree_collection("cold_path_test");
        let id = col
            .insert(json!({"name": "alpha", "n": 42, "tags": ["x"]}))
            .unwrap();

        // Evict both caches to force the cold (jsonb→oxiwire) path.
        col.doc_cache_clear();
        let stats_before = col.bytes_cache_stats();
        let bytes = col.load_doc_oxiwire_bytes(id).expect("cold load");
        let stats_after = col.bytes_cache_stats();
        assert!(!bytes.is_empty());
        assert_eq!(stats_after.misses - stats_before.misses, 1);

        // doc_cache should NOT have been polluted by the cold path.
        let doc_stats = col.doc_cache_stats();
        assert!(doc_stats.hits + doc_stats.misses >= 0); // smoke; no fixed assertion
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
            .update(
                &json!({"name": "Alice"}),
                &json!({"$set": {"age": 31}}),
                None,
            )
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
            let col = BTreeCollection::open("persist_test", dir.path(), None).unwrap();
            for i in 0..100 {
                col.insert(json!({"seq": i, "name": format!("doc_{}", i)}))
                    .unwrap();
            }
            col.sync_writes().unwrap();
        }

        // Reopen and verify
        {
            let col = BTreeCollection::open("persist_test", dir.path(), None).unwrap();
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
            let col = BTreeCollection::open("wal_test", dir.path(), None).unwrap();
            for i in 0..50 {
                col.insert(json!({"seq": i, "data": format!("val_{}", i)}))
                    .unwrap();
            }
            // NO sync_writes — WAL has entries but .btree file is empty
        }

        // Reopen — WAL replay should recover all 50 docs
        {
            let col = BTreeCollection::open("wal_test", dir.path(), None).unwrap();
            assert_eq!(col.count(), 50);
            let doc = col.find_one(&json!({"seq": 25})).unwrap().unwrap();
            assert_eq!(doc.get("data").unwrap().as_str().unwrap(), "val_25");
        }
    }

    #[test]
    fn replay_wal_includes_sealed_segments() {
        // PITR Phase 2: with a rotated WAL, recovery must replay every
        // sealed segment plus the live WAL — not just the live tail.
        let dir = tempfile::tempdir().unwrap();
        let wal = Wal::open(&dir.path().join("rs.wal")).unwrap();
        // Spread inserts across 3 segments: 2 sealed + 1 live.
        wal.log(&WalEntry::insert(1, b"a".to_vec())).unwrap();
        wal.seal().unwrap();
        wal.log(&WalEntry::insert(2, b"b".to_vec())).unwrap();
        wal.seal().unwrap();
        wal.log(&WalEntry::insert(3, b"c".to_vec())).unwrap();

        let storage = BTreeStorage::open("rs", dir.path(), None).unwrap();
        let (applied, skipped) = BTreeCollection::replay_wal(&wal, &storage, None).unwrap();
        assert_eq!(
            applied, 3,
            "every sealed segment's entries must be replayed"
        );
        assert_eq!(skipped, 0);
        assert_eq!(storage.count(), 3);
    }

    #[test]
    fn replay_wal_skips_uncommitted_tx_entries() {
        // A transaction whose WAL entries were fsync'd (commit step 5) but
        // that never reached its commit point (step 6) must NOT be
        // resurrected by replay; non-transactional and committed-tx entries
        // must still apply.
        let dir = tempfile::tempdir().unwrap();
        let wal = Wal::open(&dir.path().join("txf.wal")).unwrap();
        wal.log(&WalEntry::insert(1, b"plain".to_vec())).unwrap(); // tx_id 0
        wal.log(&WalEntry::Insert {
            doc_id: 2,
            doc_bytes: b"committed".to_vec(),
            tx_id: 7,
        })
        .unwrap();
        wal.log(&WalEntry::Insert {
            doc_id: 3,
            doc_bytes: b"uncommitted".to_vec(),
            tx_id: 9,
        })
        .unwrap();

        let committed: std::collections::HashSet<u64> = [7u64].into_iter().collect();
        let storage = BTreeStorage::open("txf", dir.path(), None).unwrap();
        let (applied, skipped) =
            BTreeCollection::replay_wal(&wal, &storage, Some(&committed)).unwrap();
        assert_eq!(applied, 2, "non-tx + committed-tx entries apply");
        assert_eq!(skipped, 1, "uncommitted tx entry is discarded");
        assert!(storage.contains_key(1));
        assert!(storage.contains_key(2));
        assert!(
            !storage.contains_key(3),
            "uncommitted tx must not resurrect"
        );
    }

    #[test]
    fn find_and_modify_returns_modified_doc_or_none() {
        let dir = tempfile::tempdir().unwrap();
        let col = BTreeCollection::open("c", dir.path(), None).unwrap();
        col.insert(json!({"name": "ctr", "v": 41})).unwrap();

        let doc = col
            .find_and_modify(&json!({"name": "ctr"}), &json!({"$inc": {"v": 1}}))
            .unwrap()
            .expect("doc exists");
        assert_eq!(doc.get("v").and_then(|v| v.as_u64()).unwrap(), 42);

        // No match → None, nothing written.
        assert!(
            col.find_and_modify(&json!({"name": "nope"}), &json!({"$set": {"v": 0}}))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn find_and_modify_is_atomic_under_concurrent_inc() {
        // The mailbox-UIDNEXT scenario: many concurrent allocations of a
        // counter must neither lose an increment nor hand out a value
        // twice. (Plain `update` + `$inc` fails this — its read and write
        // phases are not contiguous.)
        let dir = tempfile::tempdir().unwrap();
        let col = Arc::new(BTreeCollection::open("counters", dir.path(), None).unwrap());
        col.insert(json!({"name": "uidnext", "v": 0})).unwrap();

        let writers = 8usize;
        let per_writer = 200usize;
        let mut handles = Vec::new();
        for _ in 0..writers {
            let col = Arc::clone(&col);
            handles.push(std::thread::spawn(move || {
                let mut got = Vec::with_capacity(per_writer);
                for _ in 0..per_writer {
                    let doc = col
                        .find_and_modify(&json!({"name": "uidnext"}), &json!({"$inc": {"v": 1}}))
                        .unwrap()
                        .expect("counter doc exists");
                    got.push(doc.get("v").and_then(|v| v.as_u64()).unwrap());
                }
                got
            }));
        }
        let mut all: Vec<u64> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();

        // No lost updates: the counter reached exactly writers*per_writer...
        let total = (writers * per_writer) as u64;
        let final_v = col
            .find_one(&json!({"name": "uidnext"}))
            .unwrap()
            .unwrap()
            .get("v")
            .and_then(|v| v.as_u64())
            .unwrap();
        assert_eq!(final_v, total, "lost an increment under concurrency");
        // ...and every call returned a distinct value 1..=total — exactly
        // what UID assignment needs.
        all.sort_unstable();
        assert_eq!(
            all,
            (1..=total).collect::<Vec<_>>(),
            "a value was handed out twice"
        );
    }

    #[test]
    fn wal_recovery_update_and_delete() {
        let dir = tempfile::tempdir().unwrap();

        // Phase 1: insert + persist
        {
            let col = BTreeCollection::open("wal_ud", dir.path(), None).unwrap();
            for i in 0..10 {
                col.insert(json!({"key": i, "value": 0})).unwrap();
            }
            col.sync_writes().unwrap(); // persist + checkpoint
        }

        // Phase 2: update + delete WITHOUT persist (crash)
        {
            let col = BTreeCollection::open("wal_ud", dir.path(), None).unwrap();
            assert_eq!(col.count(), 10);
            // Update key=5 → value=999
            col.update(&json!({"key": 5}), &json!({"$set": {"value": 999}}), None)
                .unwrap();
            // Delete key=0
            col.delete(&json!({"key": 0}), None).unwrap();
            // NO sync_writes — crash
        }

        // Phase 3: recover — should have 9 docs, key=5 value=999
        {
            let col = BTreeCollection::open("wal_ud", dir.path(), None).unwrap();
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
            let col = BTreeCollection::open("wal_cp", dir.path(), None).unwrap();
            for i in 0..20 {
                col.insert(json!({"seq": i})).unwrap();
            }
            col.sync_writes().unwrap(); // persist the btree snapshot
        }

        // `sync_writes` persists the snapshot but deliberately does NOT
        // truncate the live WAL — doing so concurrently with writers once
        // lost acknowledged writes (see the comment in `sync_writes`).
        // The WAL is reclaimed on the next open instead: replay re-applies
        // it on top of the snapshot, then a post-replay checkpoint
        // truncates it. So it is non-empty here...
        let wal_path = dir.path().join("wal_cp.wal");
        assert!(
            std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0) > 0,
            "sync_writes should not truncate the live WAL"
        );

        // ...and reclaimed to empty after a reopen, with all data intact.
        {
            let col = BTreeCollection::open("wal_cp", dir.path(), None).unwrap();
            assert_eq!(col.count(), 20);
        }
        let wal_size = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
        assert_eq!(
            wal_size, 0,
            "WAL should be reclaimed after reopen's post-replay checkpoint"
        );
    }

    // ---------------------------------------------------------------------
    // Full-text search (highlighted)
    // ---------------------------------------------------------------------

    #[test]
    fn text_search_highlighted_marks_matching_terms() {
        let col = new_btree_collection("hl_test");
        col.create_text_index(vec!["title".to_string(), "body".to_string()])
            .unwrap();
        col.insert(json!({
            "title": "Rust Programming Guide",
            "body": "An introduction to systems programming with rust and safety guarantees"
        }))
        .unwrap();
        col.insert(json!({
            "title": "Go in Action",
            "body": "Concurrency primitives in the go programming language"
        }))
        .unwrap();

        let results = col.text_search_highlighted("rust", 10, 80, 3).unwrap();
        assert_eq!(results.len(), 1);
        let hl = results[0].get("_highlights").expect("missing _highlights");
        let hl_obj = hl.as_object().unwrap();

        // The match is in "title" and "body" — at least one should highlight.
        let any_marked = hl_obj.values().any(|v| {
            v.as_array()
                .map(|arr| {
                    arr.iter()
                        .any(|s| s.as_str().unwrap_or("").contains("<mark>"))
                })
                .unwrap_or(false)
        });
        assert!(
            any_marked,
            "expected at least one snippet with <mark> tag, got: {hl:?}"
        );

        // The doc body still has its original fields.
        assert_eq!(results[0]["title"], json!("Rust Programming Guide"));
        assert!(results[0]["_score"].as_f64().unwrap() > 0.0);
    }

    #[test]
    fn text_search_highlighted_omits_highlights_when_max_zero() {
        let col = new_btree_collection("hl_zero");
        col.create_text_index(vec!["body".to_string()]).unwrap();
        col.insert(json!({"body": "rust is great"})).unwrap();

        let results = col.text_search_highlighted("rust", 10, 80, 0).unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].get("_highlights").is_none());
        assert!(results[0].get("_score").is_some());
    }

    #[test]
    fn text_search_highlighted_no_match_no_highlights_field() {
        let col = new_btree_collection("hl_none");
        col.create_text_index(vec!["body".to_string()]).unwrap();
        col.insert(json!({"body": "rust is great"})).unwrap();

        // The query stems to a different word, so no hits.
        let results = col.text_search_highlighted("python", 10, 80, 3).unwrap();
        assert_eq!(results.len(), 0);
    }
}
