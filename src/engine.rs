use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use serde_json::{json, Value};

use crate::blob::BlobStore;
use crate::collection::{Collection, CompactStats};
use crate::document::DocumentId;
use crate::error::{Error, Result};
use crate::fts::{self, FtsIndex};
use crate::pipeline::Pipeline;
use crate::query::FindOptions;

/// The main OxiDB engine. Manages multiple collections.
///
/// Thread-safe: uses a `RwLock` on the collections map and per-collection
/// `RwLock`s so that reads on different collections never block each other,
/// and reads on the *same* collection can proceed concurrently.
pub struct OxiDb {
    data_dir: PathBuf,
    collections: RwLock<HashMap<String, Arc<RwLock<Collection>>>>,
    blob_store: BlobStore,
    fts_index: RwLock<FtsIndex>,
}

impl OxiDb {
    /// Open or create a database at the given directory.
    pub fn open(data_dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        let blob_store = BlobStore::open(data_dir)?;
        let fts_index = FtsIndex::open(data_dir)?;
        Ok(Self {
            data_dir: data_dir.to_path_buf(),
            collections: RwLock::new(HashMap::new()),
            blob_store,
            fts_index: RwLock::new(fts_index),
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
        let col = Collection::open(name, &self.data_dir)?;
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
        let col = Collection::open(name, &self.data_dir)?;
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
            Ok(col.find(query)?.len())
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
        let initial_docs = self.find(collection, &query)?;

        let lookup_fn = |foreign: &str, query: &Value| -> Result<Vec<Value>> {
            self.find(foreign, query)
        };

        pipeline.execute_from(start_idx, initial_docs, &lookup_fn)
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

        if let Some(text) = fts::extract_text(data, content_type) {
            self.fts_index
                .write()
                .unwrap()
                .index_document(bucket, key, &text)?;
        }

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
        self.fts_index
            .write()
            .unwrap()
            .remove_document(bucket, key)?;
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
