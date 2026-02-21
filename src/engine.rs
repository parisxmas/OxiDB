use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex, RwLock};

use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use serde_json::{json, Value};

use crate::blob::BlobStore;
use crate::change_stream::{ChangeEvent, ChangeStreamBroker, OperationType, ResumeError, SubscriberId, WatchFilter, WatchHandle};
use crate::collection::{Collection, CompactStats, IndexInfo};
use crate::crypto::EncryptionKey;
use crate::document::DocumentId;
use crate::error::{Error, Result};
use crate::fts::{self, FtsIndex};
use crate::pipeline::Pipeline;
use crate::query::FindOptions;
use crate::transaction::{ReadRecord, Transaction, WriteOp};
use crate::tx_log::{TransactionId, TxCommitLog};

/// Callback type for forwarding engine log messages to an external sink.
pub type LogCallback = Arc<dyn Fn(&str) + Send + Sync>;

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

/// The main OxiDB engine. Manages multiple collections.
///
/// Thread-safe: uses a `RwLock` on the collections map and per-collection
/// `RwLock`s so that reads on different collections never block each other,
/// and reads on the *same* collection can proceed concurrently.
pub struct OxiDb {
    data_dir: PathBuf,
    collections: RwLock<HashMap<String, Arc<RwLock<Collection>>>>,
    blob_store: BlobStore,
    fts_index: Arc<RwLock<FtsIndex>>,
    fts_tx: mpsc::SyncSender<FtsJob>,
    tx_log: TxCommitLog,
    next_tx_id: AtomicU64,
    active_transactions: RwLock<HashMap<TransactionId, Mutex<Transaction>>>,
    encryption: Option<Arc<EncryptionKey>>,
    verbose: bool,
    log_callback: Option<LogCallback>,
    change_broker: ChangeStreamBroker,
    scheduler_shutdown: Mutex<Option<mpsc::SyncSender<()>>>,
}

impl OxiDb {
    /// Open or create a database at the given directory.
    pub fn open(data_dir: &Path) -> Result<Self> {
        Self::open_internal(data_dir, None, false, None)
    }

    /// Open or create a database with optional encryption key.
    pub fn open_with_options(data_dir: &Path, encryption: Option<Arc<EncryptionKey>>) -> Result<Self> {
        Self::open_internal(data_dir, encryption, false, None)
    }

    /// Open or create a database with verbose logging enabled.
    pub fn open_verbose(data_dir: &Path, encryption: Option<Arc<EncryptionKey>>, verbose: bool) -> Result<Self> {
        Self::open_internal(data_dir, encryption, verbose, None)
    }

    /// Open with verbose logging and an external log callback (e.g. GELF).
    pub fn open_with_log(
        data_dir: &Path,
        encryption: Option<Arc<EncryptionKey>>,
        verbose: bool,
        log_callback: LogCallback,
    ) -> Result<Self> {
        Self::open_internal(data_dir, encryption, verbose, Some(log_callback))
    }

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

        let (fts_tx, fts_rx) = mpsc::sync_channel::<FtsJob>(256);
        let fts_worker = Arc::clone(&fts_index);
        std::thread::spawn(move || {
            while let Ok(job) = fts_rx.recv() {
                match job {
                    FtsJob::Index { data, content_type, bucket, key } => {
                        if let Some(text) = fts::extract_text(&data, &content_type) {
                            let _ = fts_worker.write().unwrap().index_document(&bucket, &key, &text);
                        }
                    }
                    FtsJob::Remove { bucket, key } => {
                        let _ = fts_worker.write().unwrap().remove_document(&bucket, &key);
                    }
                }
            }
        });

        if verbose {
            vlog("[verbose] FTS worker thread started");
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
        })
    }

    /// Return an Arc to a collection's RwLock, auto-creating if needed.
    fn get_or_create_collection(&self, name: &str) -> Result<Arc<RwLock<Collection>>> {
        // Fast path: read lock only
        {
            let cols = self.collections.read().unwrap();
            if let Some(col) = cols.get(name) {
                return Ok(Arc::clone(col));
            }
        }
        // Slow path: load the collection OUTSIDE the write lock so that other
        // collections remain accessible while a large collection is loading.
        let col = Collection::open_with_options(
            name,
            &self.data_dir,
            &std::collections::HashSet::new(),
            self.encryption.clone(),
            self.verbose,
            self.log_callback.clone(),
        )?;
        let arc = Arc::new(RwLock::new(col));
        // Briefly acquire write lock to insert
        let mut cols = self.collections.write().unwrap();
        // Double-check: another thread may have loaded the same collection
        if let Some(existing) = cols.get(name) {
            return Ok(Arc::clone(existing));
        }
        cols.insert(name.to_string(), Arc::clone(&arc));
        Ok(arc)
    }

    /// Create a new collection.
    pub fn create_collection(&self, name: &str) -> Result<()> {
        let mut cols = self.collections.write().unwrap();
        if cols.contains_key(name) {
            return Err(Error::CollectionAlreadyExists(name.to_string()));
        }
        let col = Collection::open_with_options(
            name,
            &self.data_dir,
            &std::collections::HashSet::new(),
            self.encryption.clone(),
            self.verbose,
            self.log_callback.clone(),
        )?;
        cols.insert(name.to_string(), Arc::new(RwLock::new(col)));
        Ok(())
    }

    /// List all collection names.
    pub fn list_collections(&self) -> Vec<String> {
        let cols = self.collections.read().unwrap();
        cols.keys().cloned().collect()
    }

    /// Flush all index data to disk for every loaded collection.
    pub fn flush_indexes(&self) {
        let cols = self.collections.read().unwrap();
        for (_name, col_arc) in cols.iter() {
            if let Ok(col) = col_arc.read() {
                col.save_index_data();
            }
        }
    }

    /// Drop a collection and its data.
    pub fn drop_collection(&self, name: &str) -> Result<()> {
        let mut cols = self.collections.write().unwrap();
        cols.remove(name);
        for ext in &["dat", "wal", "idx", "fidx", "cidx", "vidx"] {
            let path = self.data_dir.join(format!("{}.{}", name, ext));
            if path.exists() {
                std::fs::remove_file(path)?;
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
        let id = col.write().unwrap().insert(doc)?;
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
        let col = self.get_or_create_collection(collection)?;
        let emit = self.change_broker.has_subscribers();
        let doc_clones: Option<Vec<Value>> = if emit { Some(docs.iter().cloned().collect()) } else { None };
        let ids = col.write().unwrap().insert_many(docs)?;
        if let Some(clones) = doc_clones {
            for (mut d, &id) in clones.into_iter().zip(ids.iter()) {
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
        }
        Ok(ids)
    }

    pub fn find(&self, collection: &str, query: &Value) -> Result<Vec<Value>> {
        let col = self.get_or_create_collection(collection)?;
        col.read().unwrap().find(query)
    }

    pub fn find_with_options(
        &self,
        collection: &str,
        query: &Value,
        opts: &FindOptions,
    ) -> Result<Vec<Value>> {
        let col = self.get_or_create_collection(collection)?;
        col.read().unwrap().find_with_options(query, opts)
    }

    pub fn find_with_options_arcs(
        &self,
        collection: &str,
        query: &Value,
        opts: &FindOptions,
    ) -> Result<Vec<Arc<Value>>> {
        let col = self.get_or_create_collection(collection)?;
        col.read().unwrap().find_with_options_arcs(query, opts)
    }

    pub fn find_one(&self, collection: &str, query: &Value) -> Result<Option<Value>> {
        let col = self.get_or_create_collection(collection)?;
        col.read().unwrap().find_one(query)
    }

    pub fn update(&self, collection: &str, query: &Value, update: &Value) -> Result<u64> {
        let col = self.get_or_create_collection(collection)?;
        let ids = col.write().unwrap().update(query, update, None)?;
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
        let ids = col.write().unwrap().update(query, update, Some(1))?;
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
        let ids = col.write().unwrap().delete(query, None)?;
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
        let ids = col.write().unwrap().delete(query, Some(1))?;
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
        col.write().unwrap().create_index(field)
    }

    pub fn create_unique_index(&self, collection: &str, field: &str) -> Result<()> {
        let col = self.get_or_create_collection(collection)?;
        col.write().unwrap().create_unique_index(field)
    }

    pub fn create_composite_index(
        &self,
        collection: &str,
        fields: Vec<String>,
    ) -> Result<String> {
        let col = self.get_or_create_collection(collection)?;
        col.write().unwrap().create_composite_index(fields)
    }

    pub fn list_indexes(&self, collection: &str) -> Result<Vec<IndexInfo>> {
        let col = self.get_or_create_collection(collection)?;
        Ok(col.read().unwrap().list_indexes())
    }

    pub fn drop_index(&self, collection: &str, index_name: &str) -> Result<()> {
        let col = self.get_or_create_collection(collection)?;
        col.write().unwrap().drop_index(index_name)
    }

    pub fn count(&self, collection: &str, query: &Value) -> Result<usize> {
        let col = self.get_or_create_collection(collection)?;
        let col = col.read().unwrap();
        if query.as_object().is_some_and(|m| m.is_empty()) {
            Ok(col.count())
        } else {
            col.count_matching(query)
        }
    }

    pub fn compact(&self, collection: &str) -> Result<CompactStats> {
        let col = self.get_or_create_collection(collection)?;
        col.write().unwrap().compact()
    }

    pub fn create_text_index(&self, collection: &str, fields: Vec<String>) -> Result<()> {
        let col = self.get_or_create_collection(collection)?;
        col.write().unwrap().create_text_index(fields)
    }

    pub fn text_search(
        &self,
        collection: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<Value>> {
        let col = self.get_or_create_collection(collection)?;
        col.read().unwrap().text_search(query, limit)
    }

    pub fn create_vector_index(
        &self,
        collection: &str,
        field: &str,
        dimension: usize,
        metric: crate::vector::DistanceMetric,
    ) -> Result<()> {
        let col = self.get_or_create_collection(collection)?;
        col.write().unwrap().create_vector_index(field, dimension, metric)
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
        col.read().unwrap().vector_search(field, query_vector, limit, ef_search)
    }

    pub fn aggregate(&self, collection: &str, pipeline_json: &Value) -> Result<Vec<Value>> {
        let pipeline = Pipeline::parse(pipeline_json)?;
        let (leading_match, start_idx) = pipeline.take_leading_match();

        let query = match leading_match {
            Some(q) => q.clone(),
            None => json!({}),
        };

        let lookup_fn = |foreign: &str, query: &Value| -> Result<Vec<Value>> {
            self.find(foreign, query)
        };

        // Fast path: use Arc-based pipeline to avoid cloning all initial docs.
        // This is critical for aggregation over large datasets (200K+ docs).
        let col = self.get_or_create_collection(collection)?;
        let col_guard = col.read().unwrap();
        let arcs = col_guard.find_arcs(&query)?;
        let field_indexes = col_guard.field_indexes();
        let doc_cache = col_guard.doc_cache();
        pipeline.execute_from_arcs(start_idx, arcs, &lookup_fn, Some(field_indexes), Some(doc_cache))
    }

    // -----------------------------------------------------------------------
    // Transaction methods
    // -----------------------------------------------------------------------

    /// Begin a new transaction. Returns the transaction ID.
    pub fn begin_transaction(&self) -> TransactionId {
        let tx_id = self.next_tx_id.fetch_add(1, Ordering::SeqCst);
        let tx = Transaction::new(tx_id);
        self.active_transactions.write().unwrap().insert(tx_id, Mutex::new(tx));
        tx_id
    }

    /// Buffer an insert within a transaction.
    pub fn tx_insert(&self, tx_id: TransactionId, collection: &str, doc: Value) -> Result<()> {
        let txs = self.active_transactions.read().unwrap();
        let tx_mutex = txs.get(&tx_id).ok_or(Error::TransactionNotFound(tx_id))?;
        let mut tx = tx_mutex.lock().unwrap();
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
        let col_guard = col.read().unwrap();
        let results = col_guard.find(query)?;

        // Record read versions
        let txs = self.active_transactions.read().unwrap();
        let tx_mutex = txs.get(&tx_id).ok_or(Error::TransactionNotFound(tx_id))?;
        let mut tx = tx_mutex.lock().unwrap();
        tx.collections_involved.insert(collection.to_string());

        for doc in &results {
            if let Some(doc_id) = doc.get("_id").and_then(|v| v.as_u64()) {
                let version = col_guard.get_version(doc_id);
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
        let col_guard = col.read().unwrap();
        let matching = col_guard.find(query)?;

        let txs = self.active_transactions.read().unwrap();
        let tx_mutex = txs.get(&tx_id).ok_or(Error::TransactionNotFound(tx_id))?;
        let mut tx = tx_mutex.lock().unwrap();
        tx.collections_involved.insert(collection.to_string());

        for doc in &matching {
            if let Some(doc_id) = doc.get("_id").and_then(|v| v.as_u64()) {
                let version = col_guard.get_version(doc_id);
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
        let col_guard = col.read().unwrap();
        let matching = col_guard.find(query)?;

        let txs = self.active_transactions.read().unwrap();
        let tx_mutex = txs.get(&tx_id).ok_or(Error::TransactionNotFound(tx_id))?;
        let mut tx = tx_mutex.lock().unwrap();
        tx.collections_involved.insert(collection.to_string());

        for doc in &matching {
            if let Some(doc_id) = doc.get("_id").and_then(|v| v.as_u64()) {
                let version = col_guard.get_version(doc_id);
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
            let mut txs = self.active_transactions.write().unwrap();
            txs.remove(&tx_id)
                .ok_or(Error::TransactionNotFound(tx_id))?
        };
        let tx = tx.into_inner().unwrap();

        // 2. Acquire write locks on all involved collections in BTreeSet order (deadlock-free)
        let mut locked_collections: Vec<(String, Arc<RwLock<Collection>>)> = Vec::new();
        for col_name in &tx.collections_involved {
            let col = self.get_or_create_collection(col_name)?;
            locked_collections.push((col_name.clone(), col));
        }

        // Acquire write guards -- we hold them for the duration of commit
        let mut write_guards: HashMap<String, std::sync::RwLockWriteGuard<Collection>> = HashMap::new();
        for (name, col_arc) in &locked_collections {
            write_guards.insert(name.clone(), col_arc.write().unwrap());
        }

        // 3. OCC validation: verify all recorded versions match current versions
        for record in &tx.read_set {
            if let Some(col) = write_guards.get(&record.collection) {
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

        // 4. Prepare: execute each WriteOp against the locked collection
        //    Collect WAL entries and mutations per collection
        let mut all_mutations: HashMap<String, Vec<crate::collection::PreparedMutation>> = HashMap::new();

        for op in tx.write_ops {
            match op {
                WriteOp::Insert { collection, data } => {
                    let col = write_guards.get_mut(&collection).unwrap();
                    let mutation = col.prepare_tx_insert(data, tx_id)?;
                    all_mutations.entry(collection).or_default().push(mutation);
                }
                WriteOp::Update { collection, query, update } => {
                    let col = write_guards.get_mut(&collection).unwrap();
                    let mutations = col.prepare_tx_update(&query, &update, tx_id)?;
                    all_mutations.entry(collection).or_default().extend(mutations);
                }
                WriteOp::Delete { collection, query } => {
                    let col = write_guards.get_mut(&collection).unwrap();
                    let mutations = col.prepare_tx_delete(&query, tx_id)?;
                    all_mutations.entry(collection).or_default().extend(mutations);
                }
            }
        }

        // 5. WAL log: for each collection, log WAL entries with single fsync each
        for (col_name, mutations) in &all_mutations {
            let col = write_guards.get(col_name).unwrap();
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
            let col = write_guards.get_mut(&col_name).unwrap();
            col.apply_prepared(&mut mutations)?;
        }

        // 9. Checkpoint: for each collection, checkpoint WAL
        for (col_name, _) in &locked_collections {
            let col = write_guards.get(col_name).unwrap();
            col.checkpoint_wal()?;
        }

        // 10. Cleanup: remove tx_id from commit log
        self.tx_log.remove_committed(tx_id)?;

        // 11. Emit change events after successful commit
        for event in pending_events {
            self.change_broker.emit(event);
        }

        // 12. Write locks drop automatically when write_guards goes out of scope
        Ok(())
    }

    /// Rollback a transaction, discarding all buffered operations.
    pub fn rollback_transaction(&self, tx_id: TransactionId) -> Result<()> {
        let mut txs = self.active_transactions.write().unwrap();
        txs.remove(&tx_id);
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Blob storage methods
    // -----------------------------------------------------------------------

    pub fn create_bucket(&self, name: &str) -> Result<()> {
        self.blob_store.create_bucket(name)
    }

    pub fn list_buckets(&self) -> Vec<String> {
        self.blob_store.list_buckets()
    }

    pub fn delete_bucket(&self, name: &str) -> Result<()> {
        self.blob_store.delete_bucket(name)
    }

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

        let _ = self.fts_tx.send(FtsJob::Index {
            data: data.to_vec(),
            content_type: content_type.to_string(),
            bucket: bucket.to_string(),
            key: key.to_string(),
        });

        Ok(serde_json::to_value(&meta)?)
    }

    pub fn get_object(&self, bucket: &str, key: &str) -> Result<(Vec<u8>, Value)> {
        let (data, meta) = self.blob_store.get_object(bucket, key)?;
        Ok((data, serde_json::to_value(&meta)?))
    }

    pub fn head_object(&self, bucket: &str, key: &str) -> Result<Value> {
        let meta = self.blob_store.head_object(bucket, key)?;
        Ok(serde_json::to_value(&meta)?)
    }

    pub fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        self.blob_store.delete_object(bucket, key)?;

        let _ = self.fts_tx.send(FtsJob::Remove {
            bucket: bucket.to_string(),
            key: key.to_string(),
        });

        Ok(())
    }

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

    pub fn search(
        &self,
        bucket: Option<&str>,
        query: &str,
        limit: usize,
    ) -> Result<Vec<Value>> {
        let results = self.fts_index.read().unwrap().search(bucket, query, limit);
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

    // -----------------------------------------------------------------------
    // Stored procedures
    // -----------------------------------------------------------------------

    /// Create or replace a stored procedure.
    pub fn create_procedure(&self, name: &str, body: Value) -> Result<()> {
        // Validate the procedure definition
        crate::procedure::parse_procedure(&body)?;

        let col = self.get_or_create_collection("_procedures")?;
        let mut col_guard = col.write().unwrap();

        // Ensure unique index on name
        let _ = col_guard.create_unique_index("name");

        // Delete existing procedure with same name (upsert semantics)
        let _ = col_guard.delete(&json!({"name": name}), None);

        // Store the full definition with the name
        let mut doc = body;
        if let Some(obj) = doc.as_object_mut() {
            obj.insert("name".to_string(), Value::String(name.to_string()));
        }
        col_guard.insert(doc)?;
        Ok(())
    }

    /// Execute a stored procedure by name with the given parameters.
    pub fn call_procedure(&self, name: &str, params: Value) -> Result<Value> {
        let proc_def = {
            let col = self.get_or_create_collection("_procedures")?;
            let col_guard = col.read().unwrap();
            col_guard
                .find_one(&json!({"name": name}))?
                .ok_or_else(|| Error::ProcedureNotFound(name.to_string()))?
        };
        crate::procedure::execute_procedure(self, &proc_def, &params)
    }

    /// List all stored procedure names.
    pub fn list_procedures(&self) -> Result<Vec<String>> {
        let col = self.get_or_create_collection("_procedures")?;
        let col_guard = col.read().unwrap();
        let docs = col_guard.find(&json!({}))?;
        Ok(docs
            .iter()
            .filter_map(|d| d.get("name").and_then(|v| v.as_str()).map(String::from))
            .collect())
    }

    /// Get a stored procedure definition by name.
    pub fn get_procedure(&self, name: &str) -> Result<Value> {
        let col = self.get_or_create_collection("_procedures")?;
        let col_guard = col.read().unwrap();
        col_guard
            .find_one(&json!({"name": name}))?
            .ok_or_else(|| Error::ProcedureNotFound(name.to_string()))
    }

    /// Delete a stored procedure by name.
    pub fn delete_procedure(&self, name: &str) -> Result<()> {
        let col = self.get_or_create_collection("_procedures")?;
        let mut col_guard = col.write().unwrap();
        let deleted = col_guard.delete(&json!({"name": name}), None)?;
        if deleted.is_empty() {
            return Err(Error::ProcedureNotFound(name.to_string()));
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Cron Scheduler
    // -----------------------------------------------------------------------

    /// Spawn the scheduler background thread that periodically runs due schedules.
    pub fn start_scheduler(self: &Arc<Self>) {
        let (tx, rx) = mpsc::sync_channel::<()>(0);
        let db = Arc::clone(self);
        std::thread::spawn(move || {
            crate::scheduler::scheduler_loop(db, rx);
        });
        *self.scheduler_shutdown.lock().unwrap() = Some(tx);
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
        let mut col_guard = col.write().unwrap();
        let _ = col_guard.create_unique_index("name");
        // Upsert: delete existing, then insert
        let _ = col_guard.delete(&json!({"name": name}), None);
        col_guard.insert(def)?;
        Ok(())
    }

    /// List all schedules.
    pub fn list_schedules(&self) -> Result<Vec<Value>> {
        let col = self.get_or_create_collection("_schedules")?;
        let col_guard = col.read().unwrap();
        col_guard.find(&json!({}))
    }

    /// Get a schedule by name.
    pub fn get_schedule(&self, name: &str) -> Result<Value> {
        let col = self.get_or_create_collection("_schedules")?;
        let col_guard = col.read().unwrap();
        col_guard
            .find_one(&json!({"name": name}))?
            .ok_or_else(|| Error::ScheduleError(format!("schedule not found: {name}")))
    }

    /// Delete a schedule by name.
    pub fn delete_schedule(&self, name: &str) -> Result<()> {
        let col = self.get_or_create_collection("_schedules")?;
        let mut col_guard = col.write().unwrap();
        let deleted = col_guard.delete(&json!({"name": name}), None)?;
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
    // Backup & Restore
    // -----------------------------------------------------------------------

    /// Create a compressed tar.gz backup of the entire data directory.
    ///
    /// The backup flushes all indexes and WAL checkpoints before archiving,
    /// then holds read locks on all collections to ensure a consistent snapshot.
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

        // 2. Discover all collection names from .dat files on disk
        let disk_names = Self::discover_collection_names_on_disk(&self.data_dir)?;

        // 3. Ensure all collections are loaded
        for name in &disk_names {
            let _ = self.get_or_create_collection(name)?;
        }

        // 4. Flush indexes and checkpoint WALs for each collection
        {
            let cols = self.collections.read().unwrap();
            for col_arc in cols.values() {
                let col = col_arc.write().unwrap();
                col.save_index_data();
                let _ = col.checkpoint_wal();
            }
        }

        // 5. Acquire read locks on all collections for consistent snapshot
        let cols = self.collections.read().unwrap();
        let _read_guards: Vec<_> = cols.values()
            .map(|c| c.read().unwrap())
            .collect();

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
            collections: disk_names.len(),
        })
    }

    /// Restore a tar.gz backup archive to a target directory.
    ///
    /// This is a static method — the caller should open a new `OxiDb` instance
    /// on the target directory after restoration.
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

    /// Scan a directory for `*.dat` files and return collection names.
    fn discover_collection_names_on_disk(dir: &Path) -> Result<Vec<String>> {
        let mut names = Vec::new();
        if !dir.exists() {
            return Ok(names);
        }
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("dat") {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    names.push(stem.to_string());
                }
            }
        }
        Ok(names)
    }

    /// Recursively add directory contents to a tar archive, skipping `.tmp` files.
    fn add_dir_to_tar<W: std::io::Write>(
        archive: &mut tar::Builder<W>,
        dir: &Path,
        base: &Path,
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

impl Drop for OxiDb {
    fn drop(&mut self) {
        // Shut down the scheduler thread (dropping the sender causes it to exit)
        let _ = self.scheduler_shutdown.lock().unwrap().take();
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
}
