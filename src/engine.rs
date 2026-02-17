use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex, RwLock};

use serde_json::{json, Value};

use crate::blob::BlobStore;
use crate::collection::{Collection, CompactStats};
use crate::crypto::EncryptionKey;
use crate::document::DocumentId;
use crate::error::{Error, Result};
use crate::fts::{self, FtsIndex};
use crate::pipeline::Pipeline;
use crate::query::FindOptions;
use crate::transaction::{ReadRecord, Transaction, WriteOp};
use crate::tx_log::{TransactionId, TxCommitLog};

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
}

impl OxiDb {
    /// Open or create a database at the given directory.
    pub fn open(data_dir: &Path) -> Result<Self> {
        Self::open_with_options(data_dir, None)
    }

    /// Open or create a database with optional encryption key.
    pub fn open_with_options(data_dir: &Path, encryption: Option<Arc<EncryptionKey>>) -> Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        let blob_store = BlobStore::open_with_encryption(data_dir, encryption.clone())?;
        let fts_index = Arc::new(RwLock::new(FtsIndex::open(data_dir)?));

        // Open transaction commit log and read committed tx_ids for recovery
        let tx_log = TxCommitLog::open(data_dir)?;
        let committed_tx_ids = tx_log.read_committed()?;

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
        // Slow path: write lock to create
        let mut cols = self.collections.write().unwrap();
        // Double-check after acquiring write lock
        if let Some(col) = cols.get(name) {
            return Ok(Arc::clone(col));
        }
        let col = Collection::open_with_options(
            name,
            &self.data_dir,
            &std::collections::HashSet::new(),
            self.encryption.clone(),
        )?;
        let arc = Arc::new(RwLock::new(col));
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
        )?;
        cols.insert(name.to_string(), Arc::new(RwLock::new(col)));
        Ok(())
    }

    /// List all collection names.
    pub fn list_collections(&self) -> Vec<String> {
        let cols = self.collections.read().unwrap();
        cols.keys().cloned().collect()
    }

    /// Drop a collection and its data.
    pub fn drop_collection(&self, name: &str) -> Result<()> {
        let mut cols = self.collections.write().unwrap();
        cols.remove(name);
        let data_path = self.data_dir.join(format!("{}.dat", name));
        if data_path.exists() {
            std::fs::remove_file(data_path)?;
        }
        let wal_path = self.data_dir.join(format!("{}.wal", name));
        if wal_path.exists() {
            std::fs::remove_file(wal_path)?;
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Convenience methods that delegate to collections
    // -----------------------------------------------------------------------

    pub fn insert(&self, collection: &str, doc: Value) -> Result<DocumentId> {
        let col = self.get_or_create_collection(collection)?;
        col.write().unwrap().insert(doc)
    }

    pub fn insert_many(&self, collection: &str, docs: Vec<Value>) -> Result<Vec<DocumentId>> {
        let col = self.get_or_create_collection(collection)?;
        col.write().unwrap().insert_many(docs)
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
        col.write().unwrap().update(query, update)
    }

    pub fn delete(&self, collection: &str, query: &Value) -> Result<u64> {
        let col = self.get_or_create_collection(collection)?;
        col.write().unwrap().delete(query)
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
        let arcs = col.read().unwrap().find_arcs(&query)?;
        pipeline.execute_from_arcs(start_idx, arcs, &lookup_fn)
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

        // 7. Apply: for each collection, apply mutations to storage
        for (col_name, mut mutations) in all_mutations {
            let col = write_guards.get_mut(&col_name).unwrap();
            col.apply_prepared(&mut mutations)?;
        }

        // 8. Checkpoint: for each collection, checkpoint WAL
        for (col_name, _) in &locked_collections {
            let col = write_guards.get(col_name).unwrap();
            col.checkpoint_wal()?;
        }

        // 9. Cleanup: remove tx_id from commit log
        self.tx_log.remove_committed(tx_id)?;

        // 10. Write locks drop automatically when write_guards goes out of scope
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
}
