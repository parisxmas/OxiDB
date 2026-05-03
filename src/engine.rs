use std::collections::HashMap;
#[cfg(not(target_arch = "wasm32"))]
use std::path::{Path, PathBuf};
#[cfg(target_arch = "wasm32")]
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
#[cfg(not(target_arch = "wasm32"))]
use std::sync::mpsc;
use std::sync::Arc;
use crate::locks::{Mutex, RwLock};

#[cfg(not(target_arch = "wasm32"))]
use flate2::Compression;
#[cfg(not(target_arch = "wasm32"))]
use flate2::read::GzDecoder;
#[cfg(not(target_arch = "wasm32"))]
use flate2::write::GzEncoder;
use serde_json::{json, Value};

#[cfg(not(target_arch = "wasm32"))]
use crate::blob::BlobStore;
use crate::btree_collection::BTreeCollection;
use crate::change_stream::{ChangeEvent, ChangeStreamBroker, OperationType, ResumeError, SubscriberId, WatchFilter, WatchHandle};
use crate::collection::{CompactStats, IndexInfo, resolve_field_in_value};
use crate::value::IndexValue;
use crate::crypto::EncryptionKey;
use crate::document::DocumentId;
use crate::error::{Error, Result};
#[cfg(not(target_arch = "wasm32"))]
use crate::fts::{self, FtsIndex};
use crate::pipeline::Pipeline;
use crate::query::FindOptions;
use crate::transaction::{ReadRecord, Transaction, WriteOp};
#[cfg(not(target_arch = "wasm32"))]
use crate::tx_log::{TransactionId, TxCommitLog};
#[cfg(target_arch = "wasm32")]
type TransactionId = u64;

/// Callback type for forwarding engine log messages to an external sink.
pub type LogCallback = Arc<dyn Fn(&str) + Send + Sync>;

/// Helper: compute index metadata from a `BTreeCollection`.
/// Returns (unique_fields, need_values, has_text_index).
fn index_metadata(col: &BTreeCollection) -> (Vec<String>, bool, bool) {
    let fi = col.field_indexes();
    let ci = col.composite_indexes();
    let vi = col.vector_indexes();
    let unique_fields: Vec<String> = fi.values()
        .filter(|idx| idx.unique).map(|idx| idx.field.clone()).collect();
    let need = !fi.is_empty() || !ci.is_empty()
        || col.has_text_index() || !vi.is_empty();
    (unique_fields, need, col.has_text_index())
}

/// Information about a completed backup operation.
#[derive(Debug)]
pub struct BackupInfo {
    pub path: String,
    pub size_bytes: u64,
    pub collections: usize,
}

/// Information about a completed restore operation.
#[derive(Debug)]
pub struct RestoreInfo {
    pub path: String,
    pub collections: usize,
}

#[cfg(not(target_arch = "wasm32"))]
enum FtsJob {
    Index {
        data: Vec<u8>,
        content_type: String,
        bucket: String,
        key: String,
    },
    Remove {
        bucket: String,
        key: String,
    },
}

/// Round-robin dispatcher that fans FTS jobs out to N worker threads.
/// Each worker has its own bounded channel; when one is full the next
/// send tries the next worker. This parallelizes CPU-bound extract_text
/// across cores.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct FtsDispatcher {
    senders: Vec<mpsc::SyncSender<FtsJob>>,
    counter: AtomicUsize,
}

#[cfg(not(target_arch = "wasm32"))]
impl FtsDispatcher {
    fn new(senders: Vec<mpsc::SyncSender<FtsJob>>) -> Self {
        Self {
            senders,
            counter: AtomicUsize::new(0),
        }
    }

    fn send(&self, job: FtsJob) -> std::result::Result<(), mpsc::SendError<FtsJob>> {
        let n = self.senders.len();
        // Try the next worker round-robin. If its channel is full,
        // fall back to a blocking send to even out load.
        let start = self.counter.fetch_add(1, Ordering::Relaxed) % n;
        let mut current_job = job;
        for i in 0..n {
            let idx = (start + i) % n;
            match self.senders[idx].try_send(current_job) {
                Ok(()) => return Ok(()),
                Err(mpsc::TrySendError::Full(j)) => current_job = j,
                Err(mpsc::TrySendError::Disconnected(j)) => current_job = j,
            }
        }
        // All workers backed up — block on the round-robin pick.
        self.senders[start].send(current_job)
    }
}

/// Read OXIDB_FTS_WORKERS / OXIDB_FTS_FLUSH_INTERVAL_MS env vars and
/// spin up the worker pool + periodic flusher thread. Switches the
/// FTS index into batched-persist mode so high-volume ingestion does
/// not rewrite the whole index file on every document.
#[cfg(not(target_arch = "wasm32"))]
fn setup_fts_workers(fts_index: &Arc<RwLock<FtsIndex>>) -> FtsDispatcher {
    let n_workers = std::env::var("OXIDB_FTS_WORKERS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1)
        .max(1);
    let flush_interval_ms = std::env::var("OXIDB_FTS_FLUSH_INTERVAL_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(1000);

    // Switch into batched mode so individual index_document calls
    // don't each rewrite the whole _fts/index.json file.
    fts_index.write().set_batched(true);

    let mut senders = Vec::with_capacity(n_workers);
    for _ in 0..n_workers {
        let (tx, rx) = mpsc::sync_channel::<FtsJob>(256);
        let fts_worker = Arc::clone(fts_index);
        std::thread::spawn(move || {
            while let Ok(job) = rx.recv() {
                match job {
                    FtsJob::Index {
                        data,
                        content_type,
                        bucket,
                        key,
                    } => {
                        // CPU-bound extraction happens BEFORE the lock so
                        // workers parallelize across cores.
                        if let Some(text) = fts::extract_text(&data, &content_type) {
                            let _ = fts_worker.write().index_document(&bucket, &key, &text);
                        }
                    }
                    FtsJob::Remove { bucket, key } => {
                        let _ = fts_worker.write().remove_document(&bucket, &key);
                    }
                }
            }
        });
        senders.push(tx);
    }

    // Periodic flusher: writes the FTS index file at most once every
    // flush_interval_ms regardless of how many docs were indexed.
    if flush_interval_ms > 0 {
        let flush_target = Arc::clone(fts_index);
        std::thread::spawn(move || loop {
            std::thread::sleep(std::time::Duration::from_millis(flush_interval_ms));
            let _ = flush_target.write().flush();
        });
    }

    FtsDispatcher::new(senders)
}

/// The main OxiDB engine. Manages multiple collections.
pub struct OxiDb {
    data_dir: PathBuf,
    collections: RwLock<HashMap<String, Arc<BTreeCollection>>>,
    #[cfg(not(target_arch = "wasm32"))]
    blob_store: BlobStore,
    #[cfg(not(target_arch = "wasm32"))]
    fts_index: Arc<RwLock<FtsIndex>>,
    #[cfg(not(target_arch = "wasm32"))]
    fts_tx: FtsDispatcher,
    #[cfg(not(target_arch = "wasm32"))]
    tx_log: TxCommitLog,
    next_tx_id: AtomicU64,
    active_transactions: RwLock<HashMap<TransactionId, Mutex<Transaction>>>,
    encryption: Option<Arc<EncryptionKey>>,
    verbose: bool,
    log_callback: Option<LogCallback>,
    change_broker: ChangeStreamBroker,
    #[cfg(not(target_arch = "wasm32"))]
    scheduler_shutdown: Mutex<Option<mpsc::SyncSender<()>>>,
    lazy_sync: AtomicBool,
    #[cfg(not(target_arch = "wasm32"))]
    sync_shutdown: Mutex<Option<mpsc::SyncSender<()>>>,
    cache_capacity: AtomicUsize,
    in_memory: bool,
    #[cfg(not(target_arch = "wasm32"))]
    ttl_shutdown: Mutex<Option<mpsc::SyncSender<()>>>,
    #[cfg(not(target_arch = "wasm32"))]
    alert_shutdown: Mutex<Option<mpsc::SyncSender<()>>>,
    #[cfg(feature = "gpu")]
    gpu: Mutex<Option<Arc<crate::gpu::GpuCompute>>>,
}

impl OxiDb {
    /// Open or create a database at the given directory.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open(data_dir: &Path) -> Result<Self> {
        Self::open_internal(data_dir, None, false, None)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_with_options(data_dir: &Path, encryption: Option<Arc<EncryptionKey>>) -> Result<Self> {
        Self::open_internal(data_dir, encryption, false, None)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_verbose(data_dir: &Path, encryption: Option<Arc<EncryptionKey>>, verbose: bool) -> Result<Self> {
        Self::open_internal(data_dir, encryption, verbose, None)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_with_log(
        data_dir: &Path,
        encryption: Option<Arc<EncryptionKey>>,
        verbose: bool,
        log_callback: LogCallback,
    ) -> Result<Self> {
        Self::open_internal(data_dir, encryption, verbose, Some(log_callback))
    }

    /// Create a pure in-memory database (no disk I/O at all).
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_in_memory() -> Result<Self> {
        let tmp = std::env::temp_dir().join(format!("oxidb_mem_{}", std::process::id()));
        std::fs::create_dir_all(&tmp)?;

        let blob_store = BlobStore::open_with_encryption(&tmp, None)?;
        let fts_index = Arc::new(RwLock::new(FtsIndex::open(&tmp)?));
        let tx_log = TxCommitLog::open(&tmp)?;

        let fts_tx = setup_fts_workers(&fts_index);

        Ok(Self {
            data_dir: tmp,
            collections: RwLock::new(HashMap::new()),
            blob_store,
            fts_index,
            fts_tx,
            tx_log,
            next_tx_id: AtomicU64::new(1),
            active_transactions: RwLock::new(HashMap::new()),
            encryption: None,
            verbose: false,
            log_callback: None,
            change_broker: ChangeStreamBroker::new(),
            scheduler_shutdown: Mutex::new(None),
            lazy_sync: AtomicBool::new(false),
            sync_shutdown: Mutex::new(None),
            cache_capacity: AtomicUsize::new(crate::doc_cache::DEFAULT_CAPACITY),
            in_memory: true,
            ttl_shutdown: Mutex::new(None),
            alert_shutdown: Mutex::new(None),
            #[cfg(feature = "gpu")]
            gpu: Mutex::new(None),
})
    }

    /// Create a pure in-memory database for WebAssembly.
    /// No filesystem, no threads, no blob/FTS subsystems.
    #[cfg(target_arch = "wasm32")]
    pub fn open_in_memory() -> Result<Self> {
        Ok(Self {
            data_dir: PathBuf::new(),
            collections: RwLock::new(HashMap::new()),
            next_tx_id: AtomicU64::new(1),
            active_transactions: RwLock::new(HashMap::new()),
            encryption: None,
            verbose: false,
            log_callback: None,
            change_broker: ChangeStreamBroker::new(),
            lazy_sync: AtomicBool::new(false),
            cache_capacity: AtomicUsize::new(crate::doc_cache::DEFAULT_CAPACITY),
            in_memory: true,
        })
    }

    /// Returns true if this database is running in pure in-memory mode.
    pub fn is_in_memory(&self) -> bool {
        self.in_memory
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// Start the background TTL eviction thread.
    /// Runs every `interval` and evicts expired documents from all collections.
    pub fn start_ttl_thread(self: &Arc<Self>, interval: std::time::Duration) {
        let (tx, rx) = mpsc::sync_channel::<()>(0);
        let db = Arc::clone(self);
        std::thread::Builder::new()
            .name("oxidb-ttl".into())
            .spawn(move || {
                loop {
                    match rx.recv_timeout(interval) {
                        Err(mpsc::RecvTimeoutError::Timeout) => {
                            // Time to check for expired documents
                            let cols = db.collections.read();
                            for col_arc in cols.values() {
                                col_arc.evict_expired();
                                col_arc.evict_ttl_indexed();
                            }
                        }
                        _ => break, // Shutdown signal or channel closed
                    }
                }
            })
            .expect("failed to spawn TTL thread");
        *self.ttl_shutdown.lock() = Some(tx);
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn open_internal(
        data_dir: &Path,
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

        if verbose {
            vlog(&format!("[verbose] opening database at {}", data_dir.display()));
        }

        std::fs::create_dir_all(data_dir)?;
        let blob_store = BlobStore::open_with_encryption(data_dir, encryption.clone())?;

        if verbose {
            vlog("[verbose] blob store opened");
        }

        let fts_index = Arc::new(RwLock::new(FtsIndex::open(data_dir)?));

        if verbose {
            vlog("[verbose] FTS index loaded");
        }

        // Open transaction commit log and read committed tx_ids for recovery
        let tx_log = TxCommitLog::open(data_dir)?;
        let committed_tx_ids = tx_log.read_committed()?;

        if verbose && !committed_tx_ids.is_empty() {
            vlog(&format!(
                "[verbose] tx commit log: {} committed transactions to recover",
                committed_tx_ids.len()
            ));
        }

        let fts_tx = setup_fts_workers(&fts_index);

        if verbose {
            vlog("[verbose] FTS worker threads started");
        }

        // After recovery, clear the commit log (all committed txns are now applied)
        if !committed_tx_ids.is_empty() {
            tx_log.clear()?;
        }

        Ok(Self {
            data_dir: data_dir.to_path_buf(),
            collections: RwLock::new(HashMap::new()),
            blob_store,
            fts_index,
            fts_tx,
            tx_log,
            next_tx_id: AtomicU64::new(1),
            active_transactions: RwLock::new(HashMap::new()),
            encryption,
            verbose,
            log_callback,
            change_broker: ChangeStreamBroker::new(),
            scheduler_shutdown: Mutex::new(None),
            lazy_sync: AtomicBool::new(false),
            sync_shutdown: Mutex::new(None),
            cache_capacity: AtomicUsize::new(crate::doc_cache::DEFAULT_CAPACITY),
            in_memory: false,
            ttl_shutdown: Mutex::new(None),
            alert_shutdown: Mutex::new(None),
            #[cfg(feature = "gpu")]
            gpu: Mutex::new(None),
})
    }

    /// Return an Arc to a collection, auto-creating if needed.
    fn get_or_create_collection(&self, name: &str) -> Result<Arc<BTreeCollection>> {
        {
            let cols = self.collections.read();
            if let Some(col) = cols.get(name) {
                return Ok(Arc::clone(col));
            }
        }
        #[cfg(not(target_arch = "wasm32"))]
        let col = if self.in_memory {
            BTreeCollection::open_in_memory(name)
        } else {
            BTreeCollection::open(name, &self.data_dir, self.encryption.clone())?
        };
        #[cfg(target_arch = "wasm32")]
        let col = BTreeCollection::open_in_memory(name);
        if self.lazy_sync.load(Ordering::Acquire) {
            col.set_lazy_sync(true);
        }
        col.set_cache_capacity(self.cache_capacity.load(Ordering::Acquire));
        let arc = Arc::new(col);
        let mut cols = self.collections.write();
        if let Some(existing) = cols.get(name) {
            return Ok(Arc::clone(existing));
        }
        cols.insert(name.to_string(), Arc::clone(&arc));
        Ok(arc)
    }

    /// Create a new collection.
    ///
    /// Opens the collection (disk I/O) without holding the global write lock,
    /// then takes the lock only to insert. Mirrors `get_or_create_collection`'s
    /// concurrency pattern so parallel `create_collection` calls don't serialize.
    pub fn create_collection(&self, name: &str) -> Result<()> {
        // Fast path: already exists.
        {
            let cols = self.collections.read();
            if cols.contains_key(name) {
                return Err(Error::CollectionAlreadyExists(name.to_string()));
            }
        }

        // Open without holding the write lock — concurrent creates of distinct
        // collections proceed in parallel here.
        #[cfg(not(target_arch = "wasm32"))]
        let col = if self.in_memory {
            BTreeCollection::open_in_memory(name)
        } else {
            BTreeCollection::open(name, &self.data_dir, self.encryption.clone())?
        };
        #[cfg(target_arch = "wasm32")]
        let col = BTreeCollection::open_in_memory(name);
        if self.lazy_sync.load(Ordering::Acquire) {
            col.set_lazy_sync(true);
        }
        col.set_cache_capacity(self.cache_capacity.load(Ordering::Acquire));

        // Acquire the write lock only to insert. Re-check existence to
        // handle the race with another writer that won.
        let mut cols = self.collections.write();
        if cols.contains_key(name) {
            return Err(Error::CollectionAlreadyExists(name.to_string()));
        }
        cols.insert(name.to_string(), Arc::new(col));
        Ok(())
    }

    /// List all collection names (both in-memory and on disk).
    pub fn list_collections(&self) -> Vec<String> {
        let mut names: std::collections::HashSet<String> = {
            let cols = self.collections.read();
            cols.keys().cloned().collect()
        };
        #[cfg(not(target_arch = "wasm32"))]
        if !self.in_memory {
            if let Ok(disk_names) = Self::discover_collection_names_on_disk(&self.data_dir) {
                for name in disk_names {
                    names.insert(name);
                }
            }
        }
        let mut result: Vec<String> = names.into_iter().collect();
        result.sort();
        result
    }

    /// Flush all index data to disk for every loaded collection.
    pub fn flush_indexes(&self) {
        let cols = self.collections.read();
        for col_arc in cols.values() {
            col_arc.save_index_data();
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// Enable lazy sync mode: write operations skip per-operation fsync,
    /// and a background thread flushes all collections every `interval`.
    /// This matches MongoDB's default durability (journal flushed periodically).
    pub fn enable_lazy_sync(self: &Arc<Self>, interval: std::time::Duration) {
        self.lazy_sync.store(true, Ordering::Release);

        // Enable lazy_sync on all currently loaded collections
        {
            let cols = self.collections.read();
            for col_arc in cols.values() {
                col_arc.set_lazy_sync(true);
            }
        }

        // Spawn background sync thread
        let (tx, rx) = mpsc::sync_channel::<()>(0);
        let db = Arc::clone(self);
        std::thread::Builder::new()
            .name("oxidb-sync".into())
            .spawn(move || {
                let mut flush_counter = 0u64;
                loop {
                    match rx.recv_timeout(interval) {
                        Err(mpsc::RecvTimeoutError::Timeout) => {
                            // Periodic flush
                            let cols = db.collections.read();
                            for col_arc in cols.values() {
                                let _ = col_arc.sync_writes();
                            }
                            drop(cols);

                            // Persist indexes every ~10s (1000 * 10ms interval)
                            flush_counter += 1;
                            if flush_counter % 1000 == 0 {
                                db.flush_indexes();
                            }
                        }
                        _ => break, // Shutdown signal or sender dropped
                    }
                }
                // Final flush on shutdown: sync writes + persist indexes
                let cols = db.collections.read();
                for col_arc in cols.values() {
                    let _ = col_arc.sync_writes();
                }
                drop(cols);
                db.flush_indexes();
            })
            .expect("failed to spawn sync thread");

        *self.sync_shutdown.lock() = Some(tx);
    }

    /// Returns whether lazy sync mode is enabled.
    pub fn is_lazy_sync(&self) -> bool {
        self.lazy_sync.load(Ordering::Acquire)
    }

    /// Set the per-collection LRU document cache capacity.
    /// Applies immediately to all loaded collections and to future collections.
    pub fn set_cache_capacity(&self, capacity: usize) {
        self.cache_capacity.store(capacity, Ordering::Release);
        let cols = self.collections.read();
        for col_arc in cols.values() {
            col_arc.set_cache_capacity(capacity);
        }
    }

    /// Returns the current per-collection cache capacity.
    pub fn cache_capacity(&self) -> usize {
        self.cache_capacity.load(Ordering::Acquire)
    }

    /// Drop a collection and its data.
    pub fn drop_collection(&self, name: &str) -> Result<()> {
        let mut cols = self.collections.write();
        cols.remove(name);
        #[cfg(not(target_arch = "wasm32"))]
        if !self.in_memory {
            for ext in &["dat", "wal", "idx", "fidx", "cidx", "vidx"] {
                let path = self.data_dir.join(format!("{}.{}", name, ext));
                if path.exists() {
                    std::fs::remove_file(path)?;
                }
            }
            let btree_dir = self.data_dir.join(format!("{}.btree", name));
            if btree_dir.exists() {
                std::fs::remove_dir_all(btree_dir)?;
            }
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Change stream methods
    // -----------------------------------------------------------------------

    /// Subscribe to change events. Returns a `WatchHandle` with the subscriber
    /// ID, event receiver, and backpressure tracking.
    ///
    /// If `resume_after` is `Some(token)`, missed events are replayed from an
    /// internal ring buffer. Returns `Err(ResumeError::TokenTooOld)` if the
    /// token has been evicted.
    pub fn watch(
        &self,
        filter: WatchFilter,
        resume_after: Option<u64>,
    ) -> std::result::Result<WatchHandle, ResumeError> {
        self.change_broker.subscribe(filter, 256, resume_after)
    }

    /// Unsubscribe from change events.
    pub fn unwatch(&self, id: SubscriberId) {
        self.change_broker.unsubscribe(id);
    }

    // -----------------------------------------------------------------------
    // Convenience methods that delegate to collections
    // -----------------------------------------------------------------------

    pub fn insert(&self, collection: &str, doc: Value) -> Result<DocumentId> {
        let col = self.get_or_create_collection(collection)?;
        let emit = self.change_broker.has_subscribers();
        let doc_clone = if emit { Some(doc.clone()) } else { None };
        let id = col.insert(doc)?;
        if let Some(mut d) = doc_clone {
            if let Some(obj) = d.as_object_mut() {
                obj.insert("_id".to_string(), Value::Number(id.into()));
                obj.insert("_version".to_string(), Value::Number(1.into()));
            }
            self.change_broker.emit(ChangeEvent {
                token: 0,
                operation: OperationType::Insert,
                collection: collection.to_string(),
                doc_id: id,
                document: Some(d),
                tx_id: None,
            });
        }
        Ok(id)
    }

    pub fn insert_many(&self, collection: &str, docs: Vec<Value>) -> Result<Vec<DocumentId>> {
        if docs.is_empty() {
            return Ok(vec![]);
        }

        let col = self.get_or_create_collection(collection)?;
        let emit = self.change_broker.has_subscribers();

        // Phase 1: Reserve IDs and check unique constraints
        let (first_id, has_unique_indexes, unique_fields, need_values) = {
            let count = docs.len() as u64;

            let (unique_fields, need_values, _) = index_metadata(&col);

            let first_id = col.reserve_ids(count);

            // Check existing unique constraints only when unique indexes exist
            if !unique_fields.is_empty() {
                for (i, doc) in docs.iter().enumerate() {
                    let id = first_id + i as u64;
                    let mut check_doc = doc.clone();
                    if let Some(obj) = check_doc.as_object_mut() {
                        obj.insert("_id".to_string(), Value::Number(id.into()));
                    }
                    col.check_unique_constraints(&check_doc, None)?;
                }
            }

            (first_id, !unique_fields.is_empty(), unique_fields, need_values)
        };

        // Phase 2: Pre-serialize all documents (no lock held — other threads can work)

        // When no indexes need the Value and batch is large, drop it after
        // encoding to reduce allocator churn.
        let keep_values = need_values || emit || docs.len() <= 1000;

        // Assign IDs and prepare docs
        let mut docs_with_ids: Vec<(u64, Value)> = Vec::with_capacity(docs.len());
        for (i, mut data) in docs.into_iter().enumerate() {
            if !data.is_object() {
                return Err(Error::NotAnObject);
            }
            let id = first_id + i as u64;
            let obj = data.as_object_mut().unwrap();
            obj.insert("_id".to_string(), Value::Number(id.into()));
            obj.insert("_version".to_string(), Value::Number(1.into()));

            // Intra-batch uniqueness check
            if has_unique_indexes {
                let mut pending_unique: HashMap<String, HashMap<IndexValue, DocumentId>> = HashMap::new();
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
            docs_with_ids.push((id, data));
        }

        // Parallel JSONB encode for large batches
        #[cfg(not(target_arch = "wasm32"))]
        let prepared: Vec<(DocumentId, Value, Vec<u8>)> = if docs_with_ids.len() > 500 && !has_unique_indexes {
            use rayon::prelude::*;
            docs_with_ids.into_par_iter().map(|(id, data)| {
                let bytes = crate::codec::encode_doc(&data).unwrap_or_default();
                if keep_values { (id, data, bytes) } else { (id, Value::Null, bytes) }
            }).collect()
        } else {
            let mut prepared = Vec::with_capacity(docs_with_ids.len());
            for (id, data) in docs_with_ids {
                let bytes = crate::codec::encode_doc(&data)?;
                if keep_values {
                    prepared.push((id, data, bytes));
                } else {
                    prepared.push((id, Value::Null, bytes));
                }
            }
            prepared
        };
        #[cfg(target_arch = "wasm32")]
        let prepared: Vec<(DocumentId, Value, Vec<u8>)> = {
            let mut prepared = Vec::with_capacity(docs_with_ids.len());
            for (id, data) in docs_with_ids {
                let bytes = crate::codec::encode_doc(&data)?;
                if keep_values {
                    prepared.push((id, data, bytes));
                } else {
                    prepared.push((id, Value::Null, bytes));
                }
            }
            prepared
        };

        // Phase 3: Insert pre-serialized docs (fast: no serialization)
        let ids = col.insert_many_prepared(prepared)?;

        // Emit change events if needed
        if emit {
            for &id in &ids {
                self.change_broker.emit(ChangeEvent {
                    token: 0,
                    operation: OperationType::Insert,
                    collection: collection.to_string(),
                    doc_id: id,
                    document: None,
                    tx_id: None,
                });
            }
        }

        Ok(ids)
    }

    pub fn find(&self, collection: &str, query: &Value) -> Result<Vec<Value>> {
        let col = self.get_or_create_collection(collection)?;
        col.find(query)
    }

    pub fn find_with_options(
        &self,
        collection: &str,
        query: &Value,
        opts: &FindOptions,
    ) -> Result<Vec<Value>> {
        let col = self.get_or_create_collection(collection)?;
        col.find_with_options(query, opts)
    }

    pub fn find_with_options_arcs(
        &self,
        collection: &str,
        query: &Value,
        opts: &FindOptions,
    ) -> Result<Vec<Arc<Value>>> {
        let col = self.get_or_create_collection(collection)?;
        col.find_with_options_arcs(query, opts)
    }

    pub fn find_one(&self, collection: &str, query: &Value) -> Result<Option<Value>> {
        let col = self.get_or_create_collection(collection)?;
        col.find_one(query)
    }

    pub fn update(&self, collection: &str, query: &Value, update: &Value) -> Result<u64> {
        let col = self.get_or_create_collection(collection)?;
        let ids = col.update(query, update, None)?;
        if self.change_broker.has_subscribers() {
            for &id in &ids {
                self.change_broker.emit(ChangeEvent {
                    token: 0,
                    operation: OperationType::Update,
                    collection: collection.to_string(),
                    doc_id: id,
                    document: None,
                    tx_id: None,
                });
            }
        }
        Ok(ids.len() as u64)
    }

    pub fn update_one(&self, collection: &str, query: &Value, update: &Value) -> Result<u64> {
        let col = self.get_or_create_collection(collection)?;
        let ids = col.update(query, update, Some(1))?;
        if self.change_broker.has_subscribers() {
            for &id in &ids {
                self.change_broker.emit(ChangeEvent {
                    token: 0,
                    operation: OperationType::Update,
                    collection: collection.to_string(),
                    doc_id: id,
                    document: None,
                    tx_id: None,
                });
            }
        }
        Ok(ids.len() as u64)
    }

    pub fn delete(&self, collection: &str, query: &Value) -> Result<u64> {
        let col = self.get_or_create_collection(collection)?;
        let ids = col.delete(query, None)?;
        if self.change_broker.has_subscribers() {
            for &id in &ids {
                self.change_broker.emit(ChangeEvent {
                    token: 0,
                    operation: OperationType::Delete,
                    collection: collection.to_string(),
                    doc_id: id,
                    document: None,
                    tx_id: None,
                });
            }
        }
        Ok(ids.len() as u64)
    }

    pub fn delete_one(&self, collection: &str, query: &Value) -> Result<u64> {
        let col = self.get_or_create_collection(collection)?;
        let ids = col.delete(query, Some(1))?;
        if self.change_broker.has_subscribers() {
            for &id in &ids {
                self.change_broker.emit(ChangeEvent {
                    token: 0,
                    operation: OperationType::Delete,
                    collection: collection.to_string(),
                    doc_id: id,
                    document: None,
                    tx_id: None,
                });
            }
        }
        Ok(ids.len() as u64)
    }

    pub fn create_index(&self, collection: &str, field: &str) -> Result<()> {
        let col = self.get_or_create_collection(collection)?;
        col.create_index(field)
    }

    pub fn create_unique_index(&self, collection: &str, field: &str) -> Result<()> {
        let col = self.get_or_create_collection(collection)?;
        col.create_unique_index(field)
    }

    pub fn create_composite_index(
        &self,
        collection: &str,
        fields: Vec<String>,
    ) -> Result<String> {
        let col = self.get_or_create_collection(collection)?;
        col.create_composite_index(fields)
    }

    pub fn create_ttl_index(
        &self,
        collection: &str,
        field: &str,
        expire_after_seconds: u64,
    ) -> Result<()> {
        let col = self.get_or_create_collection(collection)?;
        col.create_ttl_index(field, expire_after_seconds)
    }

    pub fn list_indexes(&self, collection: &str) -> Result<Vec<IndexInfo>> {
        let col = self.get_or_create_collection(collection)?;
        Ok(col.list_indexes())
    }

    pub fn drop_index(&self, collection: &str, index_name: &str) -> Result<()> {
        let col = self.get_or_create_collection(collection)?;
        col.drop_index(index_name)
    }

    pub fn count(&self, collection: &str, query: &Value) -> Result<usize> {
        let col = self.get_or_create_collection(collection)?;
        if query.as_object().is_some_and(|m| m.is_empty()) {
            Ok(col.count())
        } else {
            col.count_matching(query)
        }
    }

    pub fn compact(&self, collection: &str) -> Result<CompactStats> {
        let col = self.get_or_create_collection(collection)?;
        col.compact()
    }

    pub fn create_text_index(&self, collection: &str, fields: Vec<String>) -> Result<()> {
        let col = self.get_or_create_collection(collection)?;
        col.create_text_index(fields)
    }

    pub fn text_search(
        &self,
        collection: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<Value>> {
        let col = self.get_or_create_collection(collection)?;
        col.text_search(query, limit)
    }

    pub fn text_search_highlighted(
        &self,
        collection: &str,
        query: &str,
        limit: usize,
        snippet_chars: usize,
        max_snippets: usize,
    ) -> Result<Vec<Value>> {
        let col = self.get_or_create_collection(collection)?;
        col.text_search_highlighted(query, limit, snippet_chars, max_snippets)
    }

    /// Set the GPU compute backend for accelerated vector search.
    #[cfg(feature = "gpu")]
    pub fn set_gpu(&self, gpu: Arc<crate::gpu::GpuCompute>) {
        *self.gpu.lock() = Some(gpu);
    }

    pub fn create_vector_index(
        &self,
        collection: &str,
        field: &str,
        dimension: usize,
        metric: crate::vector::DistanceMetric,
    ) -> Result<()> {
        let col = self.get_or_create_collection(collection)?;
        col.create_vector_index(field, dimension, metric)
    }

    pub fn vector_search(
        &self,
        collection: &str,
        field: &str,
        query_vector: &[f32],
        limit: usize,
        ef_search: Option<usize>,
    ) -> Result<Vec<Value>> {
        let col = self.get_or_create_collection(collection)?;
        col.vector_search(field, query_vector, limit, ef_search)
    }

    pub fn aggregate(&self, collection: &str, pipeline_json: &Value) -> Result<Vec<Value>> {
        let pipeline = Pipeline::parse(pipeline_json)?;
        let (leading_match, start_idx) = pipeline.take_leading_match();
        let out_collection = pipeline.out_collection().map(|s| s.to_string());

        let lookup_fn = |foreign: &str, query: &Value| -> Result<Vec<Value>> {
            self.find(foreign, query)
        };

        // Streaming fast path: when pipeline is [$match?] -> $group -> [rest],
        // stream docs through storage sequentially instead of materializing
        // the full Vec<Arc<Value>>. This is 5-10x faster for large collections.
        let result = if let Some((group_key, accumulators, next_idx)) =
            pipeline.try_streaming_group(start_idx)
        {
            let col = self.get_or_create_collection(collection)?;

            // Index-only count: if group key has an index and all accumulators
            // are count-only, read counts directly from the index (zero I/O).
            let fi = col.field_indexes();
            if let Some(index_result) = crate::pipeline::try_index_only_count(
                group_key,
                accumulators,
                &fi,
                col.count(),
                leading_match,
            ) {
                pipeline.execute_from(next_idx, index_result, &lookup_fn)?
            } else {
                drop(fi);
                let group_result =
                    col.aggregate_streaming(leading_match, group_key, accumulators)?;
                // Continue with remaining pipeline stages on the small group result
                pipeline.execute_from(next_idx, group_result, &lookup_fn)?
            }
        } else {
            let query = match leading_match {
                Some(q) => q.clone(),
                None => json!({}),
            };

            // Standard path: use Arc-based pipeline to avoid cloning all initial docs.
            // This is critical for aggregation over large datasets (200K+ docs).
            let col = self.get_or_create_collection(collection)?;
            let arcs = col.find_arcs(&query)?;
            let doc_lookup = |id: DocumentId| col.load_doc_arc(id);
            let fi = col.field_indexes();
            pipeline.execute_from_arcs(start_idx, arcs, &lookup_fn, Some(&fi), Some(&doc_lookup))?
        };

        // Handle $out: write results to the target collection
        if let Some(target) = out_collection {
            let target_col = self.get_or_create_collection(&target)?;
            for doc in &result {
                // Strip _id and _version so the target collection assigns new ones
                let mut clean = doc.clone();
                if let Some(obj) = clean.as_object_mut() {
                    obj.remove("_id");
                    obj.remove("_version");
                }
                target_col.insert(clean)?;
            }
        }

        Ok(result)
    }

    // -----------------------------------------------------------------------
    // Transaction methods
    // -----------------------------------------------------------------------

    /// Begin a new transaction. Returns the transaction ID.
    pub fn begin_transaction(&self) -> TransactionId {
        let tx_id = self.next_tx_id.fetch_add(1, Ordering::SeqCst);
        let tx = Transaction::new(tx_id);
        self.active_transactions.write().insert(tx_id, Mutex::new(tx));
        tx_id
    }

    /// Extract buffered write ops from a transaction (for Raft replication).
    /// Removes the transaction from the active set.
    pub fn extract_transaction_writes(&self, tx_id: TransactionId) -> Result<Vec<WriteOp>> {
        let mut txs = self.active_transactions.write();
        let tx_mutex = txs.remove(&tx_id).ok_or(Error::TransactionNotFound(tx_id))?;
        let tx = tx_mutex.into_inner();
        Ok(tx.write_ops)
    }

    /// Buffer an insert within a transaction.
    pub fn tx_insert(&self, tx_id: TransactionId, collection: &str, doc: Value) -> Result<()> {
        let txs = self.active_transactions.read();
        let tx_mutex = txs.get(&tx_id).ok_or(Error::TransactionNotFound(tx_id))?;
        let mut tx = tx_mutex.lock();
        tx.collections_involved.insert(collection.to_string());
        tx.write_ops.push(WriteOp::Insert {
            collection: collection.to_string(),
            data: doc,
        });
        Ok(())
    }

    /// Execute a read within a transaction, recording versions for OCC.
    pub fn tx_find(&self, tx_id: TransactionId, collection: &str, query: &Value) -> Result<Vec<Value>> {
        let col = self.get_or_create_collection(collection)?;
        let results = col.find(query)?;

        // Record read versions
        let txs = self.active_transactions.read();
        let tx_mutex = txs.get(&tx_id).ok_or(Error::TransactionNotFound(tx_id))?;
        let mut tx = tx_mutex.lock();
        tx.collections_involved.insert(collection.to_string());

        for doc in &results {
            if let Some(doc_id) = doc.get("_id").and_then(|v| v.as_u64()) {
                let version = col.get_version(doc_id);
                tx.read_set.push(ReadRecord {
                    collection: collection.to_string(),
                    doc_id,
                    version,
                });
            }
        }

        Ok(results)
    }

    /// Buffer an update within a transaction, recording read versions.
    pub fn tx_update(
        &self,
        tx_id: TransactionId,
        collection: &str,
        query: &Value,
        update: &Value,
    ) -> Result<()> {
        // Read to find matching docs and record their versions
        let col = self.get_or_create_collection(collection)?;
        let matching = col.find(query)?;

        let txs = self.active_transactions.read();
        let tx_mutex = txs.get(&tx_id).ok_or(Error::TransactionNotFound(tx_id))?;
        let mut tx = tx_mutex.lock();
        tx.collections_involved.insert(collection.to_string());

        for doc in &matching {
            if let Some(doc_id) = doc.get("_id").and_then(|v| v.as_u64()) {
                let version = col.get_version(doc_id);
                tx.read_set.push(ReadRecord {
                    collection: collection.to_string(),
                    doc_id,
                    version,
                });
            }
        }

        tx.write_ops.push(WriteOp::Update {
            collection: collection.to_string(),
            query: query.clone(),
            update: update.clone(),
        });
        Ok(())
    }

    /// Buffer a delete within a transaction, recording read versions.
    pub fn tx_delete(
        &self,
        tx_id: TransactionId,
        collection: &str,
        query: &Value,
    ) -> Result<()> {
        let col = self.get_or_create_collection(collection)?;
        let matching = col.find(query)?;

        let txs = self.active_transactions.read();
        let tx_mutex = txs.get(&tx_id).ok_or(Error::TransactionNotFound(tx_id))?;
        let mut tx = tx_mutex.lock();
        tx.collections_involved.insert(collection.to_string());

        for doc in &matching {
            if let Some(doc_id) = doc.get("_id").and_then(|v| v.as_u64()) {
                let version = col.get_version(doc_id);
                tx.read_set.push(ReadRecord {
                    collection: collection.to_string(),
                    doc_id,
                    version,
                });
            }
        }

        tx.write_ops.push(WriteOp::Delete {
            collection: collection.to_string(),
            query: query.clone(),
        });
        Ok(())
    }

    /// Commit a transaction using OCC validation.
    pub fn commit_transaction(&self, tx_id: TransactionId) -> Result<()> {
        // 1. Remove transaction from active set
        let tx = {
            let mut txs = self.active_transactions.write();
            txs.remove(&tx_id)
                .ok_or(Error::TransactionNotFound(tx_id))?
        };
        let tx = tx.into_inner();

        // 2. Resolve all involved collections
        let mut locked_collections: Vec<(String, Arc<BTreeCollection>)> = Vec::new();
        for col_name in &tx.collections_involved {
            let col = self.get_or_create_collection(col_name)?;
            locked_collections.push((col_name.clone(), col));
        }

        // 3. OCC validation: verify all recorded versions match current versions
        for record in &tx.read_set {
            if let Some((_, col)) = locked_collections.iter().find(|(n, _)| n == &record.collection) {
                let current_version = col.get_version(record.doc_id);
                if current_version != record.version {
                    return Err(Error::TransactionConflict {
                        collection: record.collection.clone(),
                        doc_id: record.doc_id,
                        expected_version: record.version,
                        actual_version: current_version,
                    });
                }
            }
        }

        // Build a name→Arc map for quick lookup
        let col_map: HashMap<String, Arc<BTreeCollection>> = locked_collections.iter()
            .map(|(n, c)| (n.clone(), Arc::clone(c)))
            .collect();

        // 4. Prepare: execute each WriteOp against the collection
        //    Collect WAL entries and mutations per collection
        let mut all_mutations: HashMap<String, Vec<crate::collection::PreparedMutation>> = HashMap::new();

        for op in tx.write_ops {
            match op {
                WriteOp::Insert { collection, data } => {
                    let col = col_map.get(&collection).unwrap();
                    let mutation = col.prepare_tx_insert(data, tx_id)?;
                    all_mutations.entry(collection).or_default().push(mutation);
                }
                WriteOp::Update { collection, query, update } => {
                    let col = col_map.get(&collection).unwrap();
                    let mutations = col.prepare_tx_update(&query, &update, tx_id)?;
                    all_mutations.entry(collection).or_default().extend(mutations);
                }
                WriteOp::Delete { collection, query } => {
                    let col = col_map.get(&collection).unwrap();
                    let mutations = col.prepare_tx_delete(&query, tx_id)?;
                    all_mutations.entry(collection).or_default().extend(mutations);
                }
            }
        }

        // 5. WAL log: for each collection, log WAL entries with single fsync each
        for (col_name, mutations) in &all_mutations {
            let col = col_map.get(col_name).unwrap();
            let entries: Vec<crate::wal::WalEntry> = mutations
                .iter()
                .map(|m| match &m.wal_entry {
                    crate::wal::WalEntry::Insert { doc_id, doc_bytes, tx_id } => {
                        crate::wal::WalEntry::Insert {
                            doc_id: *doc_id,
                            doc_bytes: doc_bytes.clone(),
                            tx_id: *tx_id,
                        }
                    }
                    crate::wal::WalEntry::Update { doc_id, doc_bytes, tx_id } => {
                        crate::wal::WalEntry::Update {
                            doc_id: *doc_id,
                            doc_bytes: doc_bytes.clone(),
                            tx_id: *tx_id,
                        }
                    }
                    crate::wal::WalEntry::Delete { doc_id, tx_id } => {
                        crate::wal::WalEntry::Delete {
                            doc_id: *doc_id,
                            tx_id: *tx_id,
                        }
                    }
                })
                .collect();
            col.log_wal_batch(&entries)?;
        }

        // 6. COMMIT POINT: mark transaction as committed in the global log
        #[cfg(not(target_arch = "wasm32"))]
        self.tx_log.mark_committed(tx_id)?;

        // 7. Collect event data before consuming mutations
        let emit = self.change_broker.has_subscribers();
        let pending_events: Vec<ChangeEvent> = if emit {
            all_mutations
                .iter()
                .flat_map(|(col_name, mutations)| {
                    mutations.iter().map(move |m| {
                        if m.is_delete {
                            ChangeEvent {
                                token: 0,
                                operation: OperationType::Delete,
                                collection: col_name.clone(),
                                doc_id: m.doc_id,
                                document: None,
                                tx_id: Some(tx_id),
                            }
                        } else if m.old_loc.is_some() {
                            ChangeEvent {
                                token: 0,
                                operation: OperationType::Update,
                                collection: col_name.clone(),
                                doc_id: m.doc_id,
                                document: None,
                                tx_id: Some(tx_id),
                            }
                        } else {
                            ChangeEvent {
                                token: 0,
                                operation: OperationType::Insert,
                                collection: col_name.clone(),
                                doc_id: m.doc_id,
                                document: Some(m.new_data.clone()),
                                tx_id: Some(tx_id),
                            }
                        }
                    })
                })
                .collect()
        } else {
            Vec::new()
        };

        // 8. Apply: for each collection, apply mutations to storage
        for (col_name, mut mutations) in all_mutations {
            let col = col_map.get(&col_name).unwrap();
            col.apply_prepared(&mut mutations)?;
        }

        // 9. Checkpoint: for each collection, checkpoint WAL
        for (col_name, _) in &locked_collections {
            let col = col_map.get(col_name).unwrap();
            col.checkpoint_wal()?;
        }

        // 10. Cleanup: remove tx_id from commit log
        #[cfg(not(target_arch = "wasm32"))]
        self.tx_log.remove_committed(tx_id)?;

        // 11. Emit change events after successful commit
        for event in pending_events {
            self.change_broker.emit(event);
        }

        // 12. Collection Arcs drop automatically when scope ends
        Ok(())
    }

    /// Rollback a transaction, discarding all buffered operations.
    pub fn rollback_transaction(&self, tx_id: TransactionId) -> Result<()> {
        let mut txs = self.active_transactions.write();
        txs.remove(&tx_id);
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Blob storage methods (native only)
    // -----------------------------------------------------------------------

    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_bucket(&self, name: &str) -> Result<()> {
        self.blob_store.create_bucket(name)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn list_buckets(&self) -> Vec<String> {
        self.blob_store.list_buckets()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn delete_bucket(&self, name: &str) -> Result<()> {
        self.blob_store.delete_bucket(name)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: &[u8],
        content_type: &str,
        metadata: HashMap<String, String>,
    ) -> Result<Value> {
        let meta = self
            .blob_store
            .put_object(bucket, key, data, content_type, metadata)?;

        if let Err(e) = self.fts_tx.send(FtsJob::Index {
            data: data.to_vec(),
            content_type: content_type.to_string(),
            bucket: bucket.to_string(),
            key: key.to_string(),
        }) {
            eprintln!("[fts] failed to queue index job: {e}");
        }

        Ok(serde_json::to_value(&meta)?)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn get_object(&self, bucket: &str, key: &str) -> Result<(Vec<u8>, Value)> {
        let (data, meta) = self.blob_store.get_object(bucket, key)?;
        Ok((data, serde_json::to_value(&meta)?))
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn head_object(&self, bucket: &str, key: &str) -> Result<Value> {
        let meta = self.blob_store.head_object(bucket, key)?;
        Ok(serde_json::to_value(&meta)?)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        self.blob_store.delete_object(bucket, key)?;

        if let Err(e) = self.fts_tx.send(FtsJob::Remove {
            bucket: bucket.to_string(),
            key: key.to_string(),
        }) {
            eprintln!("[fts] failed to queue remove job: {e}");
        }

        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<Value>> {
        let metas = self.blob_store.list_objects(bucket, prefix, limit)?;
        metas
            .into_iter()
            .map(|m| serde_json::to_value(&m).map_err(Error::from))
            .collect()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn search(
        &self,
        bucket: Option<&str>,
        query: &str,
        limit: usize,
    ) -> Result<Vec<Value>> {
        let results = self.fts_index.read().search(bucket, query, limit);
        Ok(results
            .into_iter()
            .map(|r| {
                json!({
                    "bucket": r.bucket,
                    "key": r.key,
                    "score": r.score,
                })
            })
            .collect())
    }

    /// Blob FTS search with `<mark>...</mark>` highlighted snippets.
    ///
    /// For each hit, fetches the blob, re-extracts text via
    /// `fts::extract_text()`, and produces snippets through `fts::highlight()`.
    /// This is more expensive than `search()` because it re-extracts
    /// each matched document — pass it only when the caller actually
    /// wants snippets, and keep `limit` modest.
    ///
    /// Output shape per hit:
    ///   { "bucket": "...", "key": "...", "score": ...,
    ///     "highlights": ["...<mark>term</mark>...", ...] }
    /// `highlights` is omitted when extraction fails or there are no matches.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn search_highlighted(
        &self,
        bucket: Option<&str>,
        query: &str,
        limit: usize,
        snippet_chars: usize,
        max_snippets: usize,
    ) -> Result<Vec<Value>> {
        let results = self.fts_index.read().search(bucket, query, limit);
        let mut out = Vec::with_capacity(results.len());
        for r in results {
            let mut entry = json!({
                "bucket": r.bucket,
                "key": r.key,
                "score": r.score,
            });
            if max_snippets > 0 && snippet_chars > 0 {
                if let Ok((data, meta)) = self.blob_store.get_object(&r.bucket, &r.key) {
                    if let Some(text) = crate::fts::extract_text(&data, &meta.content_type) {
                        let snippets = crate::fts::highlight(
                            &text,
                            query,
                            snippet_chars,
                            max_snippets,
                        );
                        if !snippets.is_empty() {
                            let arr: Vec<Value> = snippets
                                .into_iter()
                                .map(|s| Value::String(s.text))
                                .collect();
                            if let Some(obj) = entry.as_object_mut() {
                                obj.insert("highlights".to_string(), Value::Array(arr));
                            }
                        }
                    }
                }
            }
            out.push(entry);
        }
        Ok(out)
    }

    // -----------------------------------------------------------------------
    // Stored procedures
    // -----------------------------------------------------------------------

    /// Create or replace a stored procedure.
    pub fn create_procedure(&self, name: &str, body: Value) -> Result<()> {
        // Validate the procedure definition
        crate::procedure::parse_procedure(&body)?;

        let col = self.get_or_create_collection("_procedures")?;

        // Ensure unique index on name
        let _ = col.create_unique_index("name");

        // Delete existing procedure with same name (upsert semantics)
        let _ = col.delete(&json!({"name": name}), None);

        // Store the full definition with the name
        let mut doc = body;
        if let Some(obj) = doc.as_object_mut() {
            obj.insert("name".to_string(), Value::String(name.to_string()));
        }
        col.insert(doc)?;
        Ok(())
    }

    /// Execute a stored procedure by name with the given parameters.
    pub fn call_procedure(&self, name: &str, params: Value) -> Result<Value> {
        let proc_def = {
            let col = self.get_or_create_collection("_procedures")?;
            col.find_one(&json!({"name": name}))?
                .ok_or_else(|| Error::ProcedureNotFound(name.to_string()))?
        };
        crate::procedure::execute_procedure(self, &proc_def, &params)
    }

    /// List all stored procedure names.
    pub fn list_procedures(&self) -> Result<Vec<String>> {
        let col = self.get_or_create_collection("_procedures")?;
        let docs = col.find(&json!({}))?;
        Ok(docs
            .iter()
            .filter_map(|d| d.get("name").and_then(|v| v.as_str()).map(String::from))
            .collect())
    }

    /// Get a stored procedure definition by name.
    pub fn get_procedure(&self, name: &str) -> Result<Value> {
        let col = self.get_or_create_collection("_procedures")?;
        col.find_one(&json!({"name": name}))?
            .ok_or_else(|| Error::ProcedureNotFound(name.to_string()))
    }

    /// Delete a stored procedure by name.
    pub fn delete_procedure(&self, name: &str) -> Result<()> {
        let col = self.get_or_create_collection("_procedures")?;
        let deleted = col.delete(&json!({"name": name}), None)?;
        if deleted.is_empty() {
            return Err(Error::ProcedureNotFound(name.to_string()));
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Cron Scheduler
    // -----------------------------------------------------------------------

    #[cfg(not(target_arch = "wasm32"))]
    pub fn start_scheduler(self: &Arc<Self>) {
        let (tx, rx) = mpsc::sync_channel::<()>(0);
        let db = Arc::clone(self);
        std::thread::spawn(move || {
            crate::scheduler::scheduler_loop(db, rx);
        });
        *self.scheduler_shutdown.lock() = Some(tx);
    }

    /// Create or replace a named schedule.
    pub fn create_schedule(&self, name: &str, mut def: Value) -> Result<()> {
        // Validate: must have either "cron" or "every"
        let has_cron = def.get("cron").and_then(|v| v.as_str()).is_some();
        let has_every = def.get("every").and_then(|v| v.as_str()).is_some();
        if !has_cron && !has_every {
            return Err(Error::ScheduleError(
                "schedule must have 'cron' or 'every' field".into(),
            ));
        }
        if has_cron && has_every {
            return Err(Error::ScheduleError(
                "schedule cannot have both 'cron' and 'every'".into(),
            ));
        }

        // Validate cron expression if present
        if let Some(cron_str) = def.get("cron").and_then(|v| v.as_str()) {
            crate::scheduler::parse_cron(cron_str)?;
        }
        // Validate interval if present
        if let Some(every_str) = def.get("every").and_then(|v| v.as_str()) {
            crate::scheduler::parse_interval(every_str)?;
        }

        // Validate procedure exists
        let procedure = def
            .get("procedure")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::ScheduleError("missing 'procedure' field".into()))?
            .to_string();
        // Check procedure exists
        self.get_procedure(&procedure)?;

        // Ensure required fields
        if let Some(obj) = def.as_object_mut() {
            obj.insert("name".to_string(), Value::String(name.to_string()));
            obj.entry("enabled".to_string())
                .or_insert(Value::Bool(true));
            obj.entry("last_run".to_string())
                .or_insert(Value::Null);
            obj.entry("last_run_epoch".to_string())
                .or_insert(json!(0));
            obj.entry("last_status".to_string())
                .or_insert(Value::Null);
            obj.entry("last_error".to_string())
                .or_insert(Value::Null);
            obj.entry("run_count".to_string())
                .or_insert(json!(0));
        }

        let col = self.get_or_create_collection("_schedules")?;
        let _ = col.create_unique_index("name");
        // Upsert: delete existing, then insert
        let _ = col.delete(&json!({"name": name}), None);
        col.insert(def)?;
        Ok(())
    }

    /// List all schedules.
    pub fn list_schedules(&self) -> Result<Vec<Value>> {
        let col = self.get_or_create_collection("_schedules")?;
        col.find(&json!({}))
    }

    /// Get a schedule by name.
    pub fn get_schedule(&self, name: &str) -> Result<Value> {
        let col = self.get_or_create_collection("_schedules")?;
        col.find_one(&json!({"name": name}))?
            .ok_or_else(|| Error::ScheduleError(format!("schedule not found: {name}")))
    }

    /// Delete a schedule by name.
    pub fn delete_schedule(&self, name: &str) -> Result<()> {
        let col = self.get_or_create_collection("_schedules")?;
        let deleted = col.delete(&json!({"name": name}), None)?;
        if deleted.is_empty() {
            return Err(Error::ScheduleError(format!("schedule not found: {name}")));
        }
        Ok(())
    }

    /// Enable a schedule.
    pub fn enable_schedule(&self, name: &str) -> Result<()> {
        // Verify it exists
        self.get_schedule(name)?;
        self.update(
            "_schedules",
            &json!({"name": name}),
            &json!({"$set": {"enabled": true}}),
        )?;
        Ok(())
    }

    /// Disable (pause) a schedule.
    pub fn disable_schedule(&self, name: &str) -> Result<()> {
        // Verify it exists
        self.get_schedule(name)?;
        self.update(
            "_schedules",
            &json!({"name": name}),
            &json!({"$set": {"enabled": false}}),
        )?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Retention Policies
    // -----------------------------------------------------------------------

    /// Set a retention policy on a collection.
    /// Documents older than `retain_days` (based on `_ts` field) are automatically deleted.
    /// Internally creates a TTL index on `_ts` — the existing TTL eviction thread handles cleanup.
    pub fn set_retention(&self, collection: &str, retain_days: u64) -> Result<()> {
        if retain_days == 0 {
            return Err(Error::InvalidQuery("retain_days must be > 0".to_string()));
        }
        let expire_after_seconds = retain_days * 86400;

        // Create TTL index on _ts (idempotent — skips if already exists)
        self.create_ttl_index(collection, "_ts", expire_after_seconds)?;

        // Upsert policy record in _retention_policies
        let policy_col = self.get_or_create_collection("_retention_policies")?;
        let _ = policy_col.create_index("collection");
        let _ = policy_col.delete(&json!({"collection": collection}), None);
        policy_col.insert(json!({
            "collection": collection,
            "retain_days": retain_days,
            "expire_after_seconds": expire_after_seconds,
            "_ts": chrono::Utc::now().to_rfc3339(),
        }))?;

        Ok(())
    }

    /// Get the retention policy for a collection.
    pub fn get_retention(&self, collection: &str) -> Result<Value> {
        let policy_col = self.get_or_create_collection("_retention_policies")?;
        policy_col
            .find_one(&json!({"collection": collection}))?
            .ok_or_else(|| Error::InvalidQuery(format!("no retention policy for '{collection}'")))
    }

    /// Delete the retention policy for a collection.
    /// Removes the TTL config so documents are no longer auto-evicted.
    pub fn delete_retention(&self, collection: &str) -> Result<()> {
        // Remove TTL config from the target collection
        let col = self.get_or_create_collection(collection)?;
        col.remove_ttl_config("_ts");

        // Remove the policy record
        let policy_col = self.get_or_create_collection("_retention_policies")?;
        let deleted = policy_col.delete(&json!({"collection": collection}), None)?;
        if deleted.is_empty() {
            return Err(Error::InvalidQuery(format!("no retention policy for '{collection}'")));
        }
        Ok(())
    }

    /// List all retention policies.
    pub fn list_retentions(&self) -> Result<Vec<Value>> {
        let policy_col = self.get_or_create_collection("_retention_policies")?;
        policy_col.find(&json!({}))
    }

    // -----------------------------------------------------------------------
    // Alerting
    // -----------------------------------------------------------------------

    /// Start the background alert evaluator thread.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn start_alert_evaluator(self: &Arc<Self>, interval: std::time::Duration) {
        let (tx, rx) = mpsc::sync_channel::<()>(0);
        let db = Arc::clone(self);
        std::thread::Builder::new()
            .name("oxidb-alerts".into())
            .spawn(move || {
                crate::alerting::alert_loop(db, rx, interval);
            })
            .expect("failed to spawn alert evaluator thread");
        *self.alert_shutdown.lock() = Some(tx);
    }

    /// Create a new alert rule.
    pub fn create_alert(&self, name: &str, mut def: Value) -> Result<()> {
        // Validate required fields
        let obj = def.as_object_mut().ok_or_else(|| {
            Error::InvalidQuery("alert definition must be a JSON object".to_string())
        })?;

        if !obj.contains_key("collection") {
            return Err(Error::InvalidQuery("missing 'collection'".to_string()));
        }
        if !obj.contains_key("condition") {
            return Err(Error::InvalidQuery("missing 'condition'".to_string()));
        }
        if !obj.contains_key("actions") {
            return Err(Error::InvalidQuery("missing 'actions'".to_string()));
        }

        // Validate condition has required fields
        if let Some(cond) = obj.get("condition").and_then(|v| v.as_object()) {
            if !cond.contains_key("type") {
                return Err(Error::InvalidQuery("condition missing 'type'".to_string()));
            }
            if !cond.contains_key("threshold") {
                return Err(Error::InvalidQuery("condition missing 'threshold'".to_string()));
            }
        }

        // Set defaults
        obj.entry("name").or_insert(json!(name));
        obj.entry("enabled").or_insert(json!(true));
        obj.entry("cooldown_seconds").or_insert(json!(300));
        obj.entry("last_fired").or_insert(json!(null));
        obj.entry("last_fired_epoch").or_insert(json!(0));
        obj.entry("fire_count").or_insert(json!(0));
        obj.entry("_ts").or_insert(json!(chrono::Utc::now().to_rfc3339()));

        // Check for duplicate
        let alert_col = self.get_or_create_collection("_alerts")?;
        let _ = alert_col.create_index("name");
        if alert_col.find_one(&json!({"name": name}))?.is_some() {
            // Update existing alert
            let _ = alert_col.delete(&json!({"name": name}), None);
        }

        alert_col.insert(Value::Object(obj.clone()))?;
        Ok(())
    }

    /// Delete an alert by name.
    pub fn delete_alert(&self, name: &str) -> Result<()> {
        let alert_col = self.get_or_create_collection("_alerts")?;
        let deleted = alert_col.delete(&json!({"name": name}), None)?;
        if deleted.is_empty() {
            return Err(Error::InvalidQuery(format!("alert not found: {name}")));
        }
        Ok(())
    }

    /// List all alerts.
    pub fn list_alerts(&self) -> Result<Vec<Value>> {
        let alert_col = self.get_or_create_collection("_alerts")?;
        alert_col.find(&json!({}))
    }

    /// Get a single alert by name.
    pub fn get_alert(&self, name: &str) -> Result<Value> {
        let alert_col = self.get_or_create_collection("_alerts")?;
        alert_col
            .find_one(&json!({"name": name}))?
            .ok_or_else(|| Error::InvalidQuery(format!("alert not found: {name}")))
    }

    /// Test an alert by evaluating its condition immediately (without cooldown or actions).
    pub fn test_alert(&self, name: &str) -> Result<Value> {
        let alert = self.get_alert(name)?;
        let now = crate::scheduler::epoch_now();

        let collection = alert.get("collection").and_then(|v| v.as_str())
            .ok_or_else(|| Error::InvalidQuery("alert missing 'collection'".to_string()))?;
        let condition = alert.get("condition")
            .ok_or_else(|| Error::InvalidQuery("alert missing 'condition'".to_string()))?;
        let cond_type = condition.get("type").and_then(|v| v.as_str()).unwrap_or("");

        let result = match cond_type {
            "count_threshold" => {
                let query = crate::alerting::build_windowed_query_pub(condition, now);
                let count = match query {
                    Some(q) => self.count(collection, &q)? as i64,
                    None => 0,
                };
                let threshold = condition.get("threshold").and_then(|v| v.as_i64()).unwrap_or(0);
                let operator = condition.get("operator").and_then(|v| v.as_str()).unwrap_or("gte");
                json!({
                    "alert": name,
                    "type": "count_threshold",
                    "current_value": count,
                    "threshold": threshold,
                    "operator": operator,
                    "would_fire": crate::alerting::compare_pub(count, threshold, operator),
                })
            }
            _ => json!({"alert": name, "error": format!("unsupported condition type: {cond_type}")}),
        };

        Ok(result)
    }

    /// List alert history.
    pub fn list_alert_history(&self) -> Result<Vec<Value>> {
        let hist_col = self.get_or_create_collection("_alert_history")?;
        hist_col.find(&json!({}))
    }

    // -----------------------------------------------------------------------
    // Backup & Restore
    // -----------------------------------------------------------------------

    /// Create a compressed tar.gz backup of the entire data directory.
    ///
    /// The backup flushes all indexes and WAL checkpoints before archiving,
    /// then holds read locks on all collections to ensure a consistent snapshot.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn backup(&self, output_path: &Path) -> Result<BackupInfo> {
        // 1. Validate output path doesn't already exist
        if output_path.exists() {
            return Err(Error::Backup(format!(
                "output path already exists: {}",
                output_path.display()
            )));
        }

        // Ensure parent directory exists
        if let Some(parent) = output_path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)?;
            }
        }

        // 2. Discover any collections on disk that haven't been loaded yet
        let disk_names = Self::discover_collection_names_on_disk(&self.data_dir)?;

        // 3. Ensure all collections are loaded
        for name in &disk_names {
            let _ = self.get_or_create_collection(name)?;
        }

        // 4. Flush indexes and checkpoint WALs for each collection
        let num_collections = {
            let cols = self.collections.read();
            for col_arc in cols.values() {
                col_arc.save_index_data();
                let _ = col_arc.checkpoint_wal();
            }
            cols.len()
        };

        // 5. Hold collections map read lock for consistent snapshot
        let _cols = self.collections.read();

        // 6. Create tar.gz archive
        let file = std::fs::File::create(output_path)?;
        let enc = GzEncoder::new(file, Compression::default());
        let mut archive = tar::Builder::new(enc);

        Self::add_dir_to_tar(&mut archive, &self.data_dir, &self.data_dir)?;

        let enc = archive.into_inner().map_err(|e| Error::Backup(e.to_string()))?;
        enc.finish().map_err(|e| Error::Backup(e.to_string()))?;

        // 7. Return info
        let metadata = std::fs::metadata(output_path)?;
        Ok(BackupInfo {
            path: output_path.to_string_lossy().into_owned(),
            size_bytes: metadata.len(),
            collections: num_collections,
        })
    }

    /// Restore a tar.gz backup archive to a target directory.
    ///
    /// This is a static method — the caller should open a new `OxiDb` instance
    /// on the target directory after restoration.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn restore(archive_path: &Path, target_dir: &Path) -> Result<RestoreInfo> {
        // 1. Validate archive exists
        if !archive_path.exists() {
            return Err(Error::Backup(format!(
                "archive not found: {}",
                archive_path.display()
            )));
        }

        // 2. Validate target directory is empty or doesn't exist
        if target_dir.exists() {
            let has_entries = std::fs::read_dir(target_dir)?
                .next()
                .is_some();
            if has_entries {
                return Err(Error::Backup(format!(
                    "target directory is not empty: {}",
                    target_dir.display()
                )));
            }
        } else {
            std::fs::create_dir_all(target_dir)?;
        }

        // 3. Extract tar.gz into target directory
        let file = std::fs::File::open(archive_path)?;
        let dec = GzDecoder::new(file);
        let mut archive = tar::Archive::new(dec);
        archive.unpack(target_dir)?;

        // 4. Count .dat files
        let collections = Self::discover_collection_names_on_disk(target_dir)?;

        Ok(RestoreInfo {
            path: target_dir.to_string_lossy().into_owned(),
            collections: collections.len(),
        })
    }

    /// Scan a directory for `*.dat` files and `*.btree` files/directories and return collection names.
    #[cfg(not(target_arch = "wasm32"))]
    fn discover_collection_names_on_disk(dir: &Path) -> Result<Vec<String>> {
        let mut names = Vec::new();
        if !dir.exists() {
            return Ok(names);
        }
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let ext = path.extension().and_then(|e| e.to_str());
            if ext == Some("dat") || ext == Some("btree") {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    names.push(stem.to_string());
                }
            }
        }
        Ok(names)
    }

    /// Recursively add directory contents to a tar archive, skipping `.tmp` files.
    #[cfg(not(target_arch = "wasm32"))]
    fn add_dir_to_tar<W: std::io::Write>(
        archive: &mut tar::Builder<W>,
        dir: &std::path::Path,
        base: &std::path::Path,
    ) -> Result<()> {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();

            // Skip .tmp files
            if path.extension().and_then(|e| e.to_str()) == Some("tmp") {
                continue;
            }

            let rel = path.strip_prefix(base).unwrap_or(&path);

            if path.is_dir() {
                Self::add_dir_to_tar(archive, &path, base)?;
            } else if path.is_file() {
                archive
                    .append_path_with_name(&path, rel)
                    .map_err(|e| Error::Backup(e.to_string()))?;
            }
        }
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for OxiDb {
    fn drop(&mut self) {
        let _ = self.scheduler_shutdown.lock().take();
        let _ = self.sync_shutdown.lock().take();
        let _ = self.alert_shutdown.lock().take();
        {
            let cols = self.collections.read();
            for col_arc in cols.values() {
                let _ = col_arc.sync_writes();
            }
        }
        self.flush_indexes();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::tempdir;

    fn temp_db() -> OxiDb {
        let dir = tempdir().unwrap();
        OxiDb::open(dir.path()).unwrap()
    }

    #[test]
    fn tx_insert_commit() {
        let db = temp_db();
        let tx_id = db.begin_transaction();
        db.tx_insert(tx_id, "users", json!({"name": "Alice"})).unwrap();
        db.commit_transaction(tx_id).unwrap();

        let docs = db.find("users", &json!({})).unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["name"], "Alice");
    }

    #[test]
    fn tx_insert_rollback() {
        let db = temp_db();
        let tx_id = db.begin_transaction();
        db.tx_insert(tx_id, "users", json!({"name": "Alice"})).unwrap();
        db.rollback_transaction(tx_id).unwrap();

        let docs = db.find("users", &json!({})).unwrap();
        assert_eq!(docs.len(), 0);
    }

    #[test]
    fn tx_multi_collection_commit() {
        let db = temp_db();
        let tx_id = db.begin_transaction();
        db.tx_insert(tx_id, "users", json!({"name": "Alice"})).unwrap();
        db.tx_insert(tx_id, "orders", json!({"item": "Widget"})).unwrap();
        db.commit_transaction(tx_id).unwrap();

        let users = db.find("users", &json!({})).unwrap();
        let orders = db.find("orders", &json!({})).unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(orders.len(), 1);
    }

    #[test]
    fn tx_multi_collection_rollback() {
        let db = temp_db();
        let tx_id = db.begin_transaction();
        db.tx_insert(tx_id, "users", json!({"name": "Alice"})).unwrap();
        db.tx_insert(tx_id, "orders", json!({"item": "Widget"})).unwrap();
        db.rollback_transaction(tx_id).unwrap();

        let users = db.find("users", &json!({})).unwrap();
        let orders = db.find("orders", &json!({})).unwrap();
        assert_eq!(users.len(), 0);
        assert_eq!(orders.len(), 0);
    }

    #[test]
    fn tx_occ_conflict() {
        let db = temp_db();
        // Insert a doc outside of a transaction
        db.insert("users", json!({"name": "Alice", "age": 30})).unwrap();

        // TX1 reads the doc
        let tx1 = db.begin_transaction();
        let docs = db.tx_find(tx1, "users", &json!({"name": "Alice"})).unwrap();
        assert_eq!(docs.len(), 1);

        // TX2 updates the doc and commits
        let tx2 = db.begin_transaction();
        db.tx_update(tx2, "users", &json!({"name": "Alice"}), &json!({"$set": {"age": 31}})).unwrap();
        db.commit_transaction(tx2).unwrap();

        // TX1 tries to update -- should get a conflict since the version changed
        db.tx_update(tx1, "users", &json!({"name": "Alice"}), &json!({"$set": {"age": 32}})).unwrap();
        let result = db.commit_transaction(tx1);
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::TransactionConflict { .. } => {}
            other => panic!("Expected TransactionConflict, got: {other}"),
        }
    }

    #[test]
    fn concurrent_no_conflict() {
        let db = temp_db();
        db.insert("users", json!({"name": "Alice", "age": 30})).unwrap();
        db.insert("users", json!({"name": "Bob", "age": 25})).unwrap();

        // TX1 reads and updates Alice
        let tx1 = db.begin_transaction();
        db.tx_find(tx1, "users", &json!({"name": "Alice"})).unwrap();
        db.tx_update(tx1, "users", &json!({"name": "Alice"}), &json!({"$set": {"age": 31}})).unwrap();

        // TX2 reads and updates Bob (different doc)
        let tx2 = db.begin_transaction();
        db.tx_find(tx2, "users", &json!({"name": "Bob"})).unwrap();
        db.tx_update(tx2, "users", &json!({"name": "Bob"}), &json!({"$set": {"age": 26}})).unwrap();

        // Both should succeed
        db.commit_transaction(tx1).unwrap();
        db.commit_transaction(tx2).unwrap();

        let alice = db.find_one("users", &json!({"name": "Alice"})).unwrap().unwrap();
        let bob = db.find_one("users", &json!({"name": "Bob"})).unwrap().unwrap();
        assert_eq!(alice["age"], 31);
        assert_eq!(bob["age"], 26);
    }

    #[test]
    fn auto_rollback_on_drop() {
        let db = temp_db();
        let tx_id = db.begin_transaction();
        db.tx_insert(tx_id, "users", json!({"name": "Ghost"})).unwrap();
        // Simulate disconnect: just rollback without commit
        db.rollback_transaction(tx_id).unwrap();

        let docs = db.find("users", &json!({})).unwrap();
        assert_eq!(docs.len(), 0);
    }

    #[test]
    fn backup_creates_archive() {
        let dir = tempdir().unwrap();
        let db = OxiDb::open(dir.path()).unwrap();
        db.insert("users", json!({"name": "Alice"})).unwrap();
        db.insert("orders", json!({"item": "Widget"})).unwrap();

        let backup_path = dir.path().join("backup.tar.gz");
        let info = db.backup(&backup_path).unwrap();

        assert!(backup_path.exists());
        assert!(info.size_bytes > 0);
        assert_eq!(info.collections, 2);
    }

    #[test]
    fn backup_fails_if_output_exists() {
        let dir = tempdir().unwrap();
        let db = OxiDb::open(dir.path()).unwrap();
        db.insert("users", json!({"name": "Alice"})).unwrap();

        let backup_path = dir.path().join("backup.tar.gz");
        std::fs::write(&backup_path, b"existing").unwrap();

        let result = db.backup(&backup_path);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already exists"));
    }

    #[test]
    fn restore_from_backup() {
        let dir = tempdir().unwrap();
        let db = OxiDb::open(dir.path()).unwrap();
        db.insert("users", json!({"name": "Alice"})).unwrap();
        db.insert("users", json!({"name": "Bob"})).unwrap();
        db.insert("orders", json!({"item": "Widget"})).unwrap();

        let backup_path = dir.path().join("backup.tar.gz");
        db.backup(&backup_path).unwrap();
        drop(db);

        let restore_dir = dir.path().join("restored");
        let info = OxiDb::restore(&backup_path, &restore_dir).unwrap();
        assert_eq!(info.collections, 2);

        let db2 = OxiDb::open(&restore_dir).unwrap();
        let users = db2.find("users", &json!({})).unwrap();
        assert_eq!(users.len(), 2);
        let orders = db2.find("orders", &json!({})).unwrap();
        assert_eq!(orders.len(), 1);
    }

    #[test]
    fn restore_fails_if_target_not_empty() {
        let dir = tempdir().unwrap();
        let db = OxiDb::open(dir.path()).unwrap();
        db.insert("users", json!({"name": "Alice"})).unwrap();

        let backup_path = dir.path().join("backup.tar.gz");
        db.backup(&backup_path).unwrap();

        let restore_dir = dir.path().join("notempty");
        std::fs::create_dir_all(&restore_dir).unwrap();
        std::fs::write(restore_dir.join("file.txt"), b"data").unwrap();

        let result = OxiDb::restore(&backup_path, &restore_dir);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not empty"));
    }

    // -----------------------------------------------------------------------
    // Change stream tests
    // -----------------------------------------------------------------------

    #[test]
    fn watch_insert_emits_event() {
        let db = temp_db();
        let handle = db.watch(WatchFilter::All, None).unwrap();

        let id = db.insert("users", json!({"name": "Alice"})).unwrap();

        let event = handle.rx.recv_timeout(std::time::Duration::from_secs(1)).unwrap();
        assert_eq!(event.operation, OperationType::Insert);
        assert_eq!(event.collection, "users");
        assert_eq!(event.doc_id, id);
        assert!(event.document.is_some());
        assert!(event.token > 0);
        let doc = event.document.unwrap();
        assert_eq!(doc["name"], "Alice");
        assert_eq!(doc["_id"], id);
    }

    #[test]
    fn watch_update_emits_event() {
        let db = temp_db();
        let id = db.insert("users", json!({"name": "Alice", "age": 30})).unwrap();

        let handle = db.watch(WatchFilter::All, None).unwrap();
        db.update("users", &json!({"name": "Alice"}), &json!({"$set": {"age": 31}})).unwrap();

        let event = handle.rx.recv_timeout(std::time::Duration::from_secs(1)).unwrap();
        assert_eq!(event.operation, OperationType::Update);
        assert_eq!(event.collection, "users");
        assert_eq!(event.doc_id, id);
        assert!(event.document.is_none());
    }

    #[test]
    fn watch_delete_emits_event() {
        let db = temp_db();
        let id = db.insert("users", json!({"name": "Alice"})).unwrap();

        let handle = db.watch(WatchFilter::All, None).unwrap();
        db.delete("users", &json!({"name": "Alice"})).unwrap();

        let event = handle.rx.recv_timeout(std::time::Duration::from_secs(1)).unwrap();
        assert_eq!(event.operation, OperationType::Delete);
        assert_eq!(event.collection, "users");
        assert_eq!(event.doc_id, id);
        assert!(event.document.is_none());
    }

    #[test]
    fn watch_tx_commit_emits_events() {
        let db = temp_db();
        let handle = db.watch(WatchFilter::All, None).unwrap();

        let tx_id = db.begin_transaction();
        db.tx_insert(tx_id, "users", json!({"name": "Alice"})).unwrap();
        db.tx_insert(tx_id, "users", json!({"name": "Bob"})).unwrap();
        db.commit_transaction(tx_id).unwrap();

        let e1 = handle.rx.recv_timeout(std::time::Duration::from_secs(1)).unwrap();
        let e2 = handle.rx.recv_timeout(std::time::Duration::from_secs(1)).unwrap();
        assert_eq!(e1.operation, OperationType::Insert);
        assert_eq!(e2.operation, OperationType::Insert);
        assert!(e1.tx_id.is_some());
        assert!(e2.tx_id.is_some());
    }

    #[test]
    fn unwatch_stops_events() {
        let db = temp_db();
        let handle = db.watch(WatchFilter::All, None).unwrap();

        db.unwatch(handle.id);
        db.insert("users", json!({"name": "Alice"})).unwrap();

        assert!(handle.rx.recv_timeout(std::time::Duration::from_millis(50)).is_err());
    }

    #[test]
    fn watch_filters_by_collection() {
        let db = temp_db();
        let handle = db.watch(WatchFilter::Collection("orders".to_string()), None).unwrap();

        db.insert("users", json!({"name": "Alice"})).unwrap();
        let order_id = db.insert("orders", json!({"item": "Widget"})).unwrap();

        let event = handle.rx.recv_timeout(std::time::Duration::from_secs(1)).unwrap();
        assert_eq!(event.collection, "orders");
        assert_eq!(event.doc_id, order_id);

        // No more events (the users insert was filtered out)
        assert!(handle.rx.recv_timeout(std::time::Duration::from_millis(50)).is_err());
    }

    #[test]
    fn test_sp_demo() {
        let db = temp_db();

        // ── Seed data ──────────────────────────────────────────────
        db.insert("accounts", json!({
            "account_id": "ACC001", "owner": "Alice", "balance": 500
        })).unwrap();
        db.insert("accounts", json!({
            "account_id": "ACC002", "owner": "Bob", "balance": 200
        })).unwrap();

        // ── 1. Create a stored procedure ───────────────────────────
        db.create_procedure("transfer_funds", json!({
            "name": "transfer_funds",
            "params": ["from_account", "to_account", "amount"],
            "steps": [
                {
                    "step": "find_one",
                    "collection": "accounts",
                    "query": { "account_id": "$param.from_account" },
                    "as": "sender"
                },
                {
                    "step": "find_one",
                    "collection": "accounts",
                    "query": { "account_id": "$param.to_account" },
                    "as": "receiver"
                },
                {
                    "step": "if",
                    "condition": { "$expr": { "$lt": ["$sender.balance", "$param.amount"] } },
                    "then": [
                        { "step": "abort", "message": "insufficient funds" }
                    ]
                },
                {
                    "step": "update",
                    "collection": "accounts",
                    "query": { "account_id": "$param.from_account" },
                    "update": { "$inc": { "balance": -150 } }
                },
                {
                    "step": "update",
                    "collection": "accounts",
                    "query": { "account_id": "$param.to_account" },
                    "update": { "$inc": { "balance": 150 } }
                },
                {
                    "step": "return",
                    "value": {
                        "status": "ok",
                        "from": "$param.from_account",
                        "to": "$param.to_account",
                        "amount": "$param.amount",
                        "sender_old_balance": "$sender.balance",
                        "receiver_old_balance": "$receiver.balance"
                    }
                }
            ]
        })).unwrap();

        // ── 2. List procedures ─────────────────────────────────────
        let procs = db.list_procedures().unwrap();
        println!("\n=== Stored procedures: {:?}", procs);
        assert_eq!(procs, vec!["transfer_funds"]);

        // ── 3. Get procedure definition ────────────────────────────
        let def = db.get_procedure("transfer_funds").unwrap();
        println!("\n=== Procedure definition:\n{}", serde_json::to_string_pretty(&def).unwrap());

        // ── 4. Call the procedure (success) ────────────────────────
        let result = db.call_procedure("transfer_funds", json!({
            "from_account": "ACC001",
            "to_account": "ACC002",
            "amount": 150
        })).unwrap();
        println!("\n=== Transfer result:\n{}", serde_json::to_string_pretty(&result).unwrap());
        assert_eq!(result["status"], "ok");

        // Verify balances after transfer
        let alice = db.find_one("accounts", &json!({"account_id": "ACC001"})).unwrap().unwrap();
        let bob = db.find_one("accounts", &json!({"account_id": "ACC002"})).unwrap().unwrap();
        println!("\n=== After transfer:");
        println!("  Alice: {}", alice["balance"]);
        println!("  Bob:   {}", bob["balance"]);
        assert_eq!(alice["balance"], 350);
        assert_eq!(bob["balance"], 350);

        // ── 5. Call the procedure (insufficient funds → abort) ─────
        let err = db.call_procedure("transfer_funds", json!({
            "from_account": "ACC001",
            "to_account": "ACC002",
            "amount": 9999
        }));
        println!("\n=== Insufficient funds error: {}", err.unwrap_err());

        // Verify balances unchanged after abort
        let alice = db.find_one("accounts", &json!({"account_id": "ACC001"})).unwrap().unwrap();
        let bob = db.find_one("accounts", &json!({"account_id": "ACC002"})).unwrap().unwrap();
        println!("  Alice still: {}", alice["balance"]);
        println!("  Bob still:   {}", bob["balance"]);
        assert_eq!(alice["balance"], 350);
        assert_eq!(bob["balance"], 350);

        // ── 6. Delete the procedure ────────────────────────────────
        db.delete_procedure("transfer_funds").unwrap();
        let procs = db.list_procedures().unwrap();
        println!("\n=== After delete, procedures: {:?}", procs);
        assert!(procs.is_empty());
    }

    #[test]
    fn test_sp_nested_if() {
        let db = temp_db();

        // Seed: users with different ages and membership tiers
        db.insert("users", json!({
            "name": "Alice", "age": 25, "tier": "gold", "balance": 1000
        })).unwrap();
        db.insert("users", json!({
            "name": "Bob", "age": 16, "tier": "silver", "balance": 500
        })).unwrap();
        db.insert("users", json!({
            "name": "Charlie", "age": 30, "tier": "bronze", "balance": 50
        })).unwrap();

        // A procedure that classifies a user's discount based on nested rules:
        //   - if age < 18 → "minor_not_eligible"
        //   - else →
        //       - if tier == "gold" →
        //           - if balance >= 500 → 30% discount
        //           - else → 20% discount
        //       - else →
        //           - if balance >= 200 → 10% discount
        //           - else → 5% discount
        db.create_procedure("calc_discount", json!({
            "name": "calc_discount",
            "params": ["username"],
            "steps": [
                {
                    "step": "find_one",
                    "collection": "users",
                    "query": { "name": "$param.username" },
                    "as": "user"
                },
                {
                    "step": "if",
                    "condition": { "$expr": { "$lt": ["$user.age", 18] } },
                    "then": [
                        { "step": "return", "value": {
                            "user": "$param.username",
                            "discount": 0,
                            "reason": "minor_not_eligible"
                        }}
                    ],
                    "else": [
                        {
                            "step": "if",
                            "condition": { "$expr": { "$eq": ["$user.tier", "gold"] } },
                            "then": [
                                {
                                    "step": "if",
                                    "condition": { "$expr": { "$gte": ["$user.balance", 500] } },
                                    "then": [
                                        { "step": "return", "value": {
                                            "user": "$param.username",
                                            "discount": 30,
                                            "reason": "gold_high_balance"
                                        }}
                                    ],
                                    "else": [
                                        { "step": "return", "value": {
                                            "user": "$param.username",
                                            "discount": 20,
                                            "reason": "gold_low_balance"
                                        }}
                                    ]
                                }
                            ],
                            "else": [
                                {
                                    "step": "if",
                                    "condition": { "$expr": { "$gte": ["$user.balance", 200] } },
                                    "then": [
                                        { "step": "return", "value": {
                                            "user": "$param.username",
                                            "discount": 10,
                                            "reason": "standard_high_balance"
                                        }}
                                    ],
                                    "else": [
                                        { "step": "return", "value": {
                                            "user": "$param.username",
                                            "discount": 5,
                                            "reason": "standard_low_balance"
                                        }}
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        })).unwrap();

        // Alice: age 25, gold, balance 1000 → gold_high_balance (30%)
        let r = db.call_procedure("calc_discount", json!({"username": "Alice"})).unwrap();
        println!("\nAlice: {}", serde_json::to_string_pretty(&r).unwrap());
        assert_eq!(r["discount"], 30);
        assert_eq!(r["reason"], "gold_high_balance");

        // Bob: age 16 → minor_not_eligible (0%)
        let r = db.call_procedure("calc_discount", json!({"username": "Bob"})).unwrap();
        println!("\nBob: {}", serde_json::to_string_pretty(&r).unwrap());
        assert_eq!(r["discount"], 0);
        assert_eq!(r["reason"], "minor_not_eligible");

        // Charlie: age 30, bronze, balance 50 → standard_low_balance (5%)
        let r = db.call_procedure("calc_discount", json!({"username": "Charlie"})).unwrap();
        println!("\nCharlie: {}", serde_json::to_string_pretty(&r).unwrap());
        assert_eq!(r["discount"], 5);
        assert_eq!(r["reason"], "standard_low_balance");
    }

    // -----------------------------------------------------------------------
    // In-memory engine tests
    // -----------------------------------------------------------------------

    fn mem_db() -> OxiDb {
        OxiDb::open_in_memory().unwrap()
    }

    #[test]
    fn in_memory_insert_find() {
        let db = mem_db();
        db.insert("users", json!({"name": "Alice", "age": 30})).unwrap();
        db.insert("users", json!({"name": "Bob", "age": 25})).unwrap();

        let docs = db.find("users", &json!({})).unwrap();
        assert_eq!(docs.len(), 2);

        let alice = db.find("users", &json!({"name": "Alice"})).unwrap();
        assert_eq!(alice.len(), 1);
        assert_eq!(alice[0]["age"], 30);
    }

    #[test]
    fn in_memory_update_delete() {
        let db = mem_db();
        db.insert("users", json!({"name": "Alice", "age": 30})).unwrap();

        db.update("users", &json!({"name": "Alice"}), &json!({"$set": {"age": 31}})).unwrap();
        let docs = db.find("users", &json!({"name": "Alice"})).unwrap();
        assert_eq!(docs[0]["age"], 31);

        db.delete("users", &json!({"name": "Alice"})).unwrap();
        let docs = db.find("users", &json!({})).unwrap();
        assert_eq!(docs.len(), 0);
    }

    #[test]
    fn in_memory_collections() {
        let db = mem_db();
        db.insert("users", json!({"name": "Alice"})).unwrap();
        db.insert("orders", json!({"item": "Widget"})).unwrap();

        let cols = db.list_collections();
        assert!(cols.contains(&"users".to_string()));
        assert!(cols.contains(&"orders".to_string()));

        db.drop_collection("orders").unwrap();
        let cols = db.list_collections();
        assert!(!cols.contains(&"orders".to_string()));
    }

    #[test]
    fn in_memory_indexes() {
        let db = mem_db();
        db.create_index("users", "email").unwrap();
        db.insert("users", json!({"name": "Alice", "email": "alice@test.com"})).unwrap();
        db.insert("users", json!({"name": "Bob", "email": "bob@test.com"})).unwrap();

        let docs = db.find("users", &json!({"email": "alice@test.com"})).unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["name"], "Alice");
    }

    #[test]
    fn in_memory_transactions() {
        let db = mem_db();
        let tx = db.begin_transaction();
        db.tx_insert(tx, "users", json!({"name": "Alice"})).unwrap();
        db.tx_insert(tx, "users", json!({"name": "Bob"})).unwrap();
        db.commit_transaction(tx).unwrap();

        let docs = db.find("users", &json!({})).unwrap();
        assert_eq!(docs.len(), 2);
    }

    #[test]
    fn in_memory_transaction_rollback() {
        let db = mem_db();
        db.insert("users", json!({"name": "Existing"})).unwrap();

        let tx = db.begin_transaction();
        db.tx_insert(tx, "users", json!({"name": "Temporary"})).unwrap();
        db.rollback_transaction(tx).unwrap();

        let docs = db.find("users", &json!({})).unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["name"], "Existing");
    }

    #[test]
    fn in_memory_is_in_memory() {
        let db = mem_db();
        assert!(db.is_in_memory());

        let disk_db = temp_db();
        assert!(!disk_db.is_in_memory());
    }

    #[test]
    fn in_memory_ttl_eviction() {
        let db = mem_db();
        // Insert a doc with 0-second TTL (expires immediately)
        db.insert("cache", json!({"key": "session", "_ttl": 0})).unwrap();
        // Insert a doc without TTL
        db.insert("cache", json!({"key": "permanent"})).unwrap();

        // Before eviction, both docs exist
        let docs = db.find("cache", &json!({})).unwrap();
        assert_eq!(docs.len(), 2);

        // Wait briefly then evict
        std::thread::sleep(std::time::Duration::from_millis(10));
        {
            let col = db.get_or_create_collection("cache").unwrap();
            let evicted = col.evict_expired();
            assert_eq!(evicted, 1);
        }

        let docs = db.find("cache", &json!({})).unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["key"], "permanent");
    }

    #[test]
    fn ttl_index_create_and_list() {
        let db = mem_db();
        db.create_ttl_index("sessions", "created_at", 3600).unwrap();

        let indexes = db.list_indexes("sessions").unwrap();
        // Should have both a field index on "created_at" and a TTL index
        let ttl_idx = indexes.iter().find(|i| i.index_type == "ttl").unwrap();
        assert_eq!(ttl_idx.name, "created_at_ttl");
        assert_eq!(ttl_idx.fields, vec!["created_at".to_string()]);
        assert_eq!(ttl_idx.expire_after_seconds, Some(3600));

        let field_idx = indexes.iter().find(|i| i.index_type == "field").unwrap();
        assert_eq!(field_idx.name, "created_at");
    }

    #[test]
    fn ttl_index_evicts_expired_docs() {
        let db = mem_db();
        // TTL of 1 second
        db.create_ttl_index("sessions", "created_at", 1).unwrap();

        // Insert a doc with a timestamp 5 seconds in the past (should be expired)
        let past = chrono::Utc::now() - chrono::Duration::seconds(5);
        let past_str = past.to_rfc3339();
        db.insert("sessions", json!({"user": "expired", "created_at": past_str})).unwrap();

        // Insert a doc with current timestamp (should survive)
        let now_str = chrono::Utc::now().to_rfc3339();
        db.insert("sessions", json!({"user": "active", "created_at": now_str})).unwrap();

        // Both exist before eviction
        let docs = db.find("sessions", &json!({})).unwrap();
        assert_eq!(docs.len(), 2);

        // Run eviction
        {
            let col = db.get_or_create_collection("sessions").unwrap();
            let evicted = col.evict_ttl_indexed();
            assert_eq!(evicted, 1);
        }

        // Only the active doc remains
        let docs = db.find("sessions", &json!({})).unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["user"], "active");
    }

    #[test]
    fn ttl_index_no_eviction_before_expiry() {
        let db = mem_db();
        // TTL of 1 hour
        db.create_ttl_index("cache", "ts", 3600).unwrap();

        // Insert a doc with current timestamp — should NOT be evicted
        let now_str = chrono::Utc::now().to_rfc3339();
        db.insert("cache", json!({"key": "fresh", "ts": now_str})).unwrap();

        {
            let col = db.get_or_create_collection("cache").unwrap();
            let evicted = col.evict_ttl_indexed();
            assert_eq!(evicted, 0);
        }

        let docs = db.find("cache", &json!({})).unwrap();
        assert_eq!(docs.len(), 1);
    }

    #[test]
    fn ttl_index_idempotent_create() {
        let db = mem_db();
        db.create_ttl_index("logs", "timestamp", 86400).unwrap();
        db.create_ttl_index("logs", "timestamp", 86400).unwrap(); // should not error

        let indexes = db.list_indexes("logs").unwrap();
        let ttl_count = indexes.iter().filter(|i| i.index_type == "ttl").count();
        assert_eq!(ttl_count, 1);
    }

    #[test]
    fn ttl_index_drop() {
        let db = mem_db();
        db.create_ttl_index("sessions", "created_at", 3600).unwrap();

        // Verify it exists
        let indexes = db.list_indexes("sessions").unwrap();
        assert!(indexes.iter().any(|i| i.name == "created_at_ttl"));

        // Drop it
        db.drop_index("sessions", "created_at_ttl").unwrap();

        // Verify it's gone
        let indexes = db.list_indexes("sessions").unwrap();
        assert!(!indexes.iter().any(|i| i.name == "created_at_ttl"));
    }

    #[test]
    fn ttl_index_persists_across_restart() {
        let dir = tempdir().unwrap();

        // Phase 1: create TTL index and insert docs
        {
            let db = OxiDb::open(dir.path()).unwrap();
            db.create_ttl_index("sessions", "created_at", 2).unwrap();

            // Insert an already-expired doc (5s ago, TTL=2s)
            let past = chrono::Utc::now() - chrono::Duration::seconds(5);
            db.insert("sessions", json!({"user": "old", "created_at": past.to_rfc3339()})).unwrap();

            // Insert a fresh doc
            db.insert("sessions", json!({"user": "new", "created_at": chrono::Utc::now().to_rfc3339()})).unwrap();

            // Flush to disk
            let col = db.get_or_create_collection("sessions").unwrap();
            let _ = col.sync_writes();
        }
        // db dropped — simulates shutdown

        // Phase 2: reopen and verify TTL index was restored
        {
            let db = OxiDb::open(dir.path()).unwrap();

            // TTL index should be in list_indexes
            let indexes = db.list_indexes("sessions").unwrap();
            let ttl_idx = indexes.iter().find(|i| i.index_type == "ttl");
            assert!(ttl_idx.is_some(), "TTL index not restored after restart");
            let ttl_idx = ttl_idx.unwrap();
            assert_eq!(ttl_idx.name, "created_at_ttl");
            assert_eq!(ttl_idx.expire_after_seconds, Some(2));

            // Evict — the old doc should be removed
            let col = db.get_or_create_collection("sessions").unwrap();
            let evicted = col.evict_ttl_indexed();
            assert_eq!(evicted, 1);

            // Only the fresh doc remains
            let docs = db.find("sessions", &json!({})).unwrap();
            assert_eq!(docs.len(), 1);
            assert_eq!(docs[0]["user"], "new");
        }
    }

    #[test]
    fn ttl_index_evicts_on_create() {
        let db = mem_db();
        // Insert already-expired docs BEFORE creating the TTL index
        let past = chrono::Utc::now() - chrono::Duration::seconds(10);
        db.insert("events", json!({"type": "old", "at": past.to_rfc3339()})).unwrap();
        db.insert("events", json!({"type": "current", "at": chrono::Utc::now().to_rfc3339()})).unwrap();

        // Creating the TTL index should immediately evict the old doc
        db.create_ttl_index("events", "at", 5).unwrap();

        let docs = db.find("events", &json!({})).unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["type"], "current");
    }

    #[test]
    fn in_memory_sql() {
        let db = Arc::new(mem_db());
        db.insert("products", json!({"name": "Widget", "price": 9.99})).unwrap();
        db.insert("products", json!({"name": "Gadget", "price": 19.99})).unwrap();

        let result = crate::sql::execute_sql(&db, "SELECT * FROM products WHERE price > 10");
        assert!(result.is_ok());
        match result.unwrap() {
            crate::sql::SqlResult::Select(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0]["name"], "Gadget");
            }
            _ => panic!("expected Select"),
        }
    }
}
