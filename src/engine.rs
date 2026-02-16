use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use serde_json::Value;

use crate::collection::{Collection, CompactStats};
use crate::document::DocumentId;
use crate::error::{Error, Result};
use crate::query::FindOptions;

/// The main OxiDB engine. Manages multiple collections.
///
/// Thread-safe: uses a `RwLock` on the collections map and per-collection
/// `RwLock`s so that reads on different collections never block each other,
/// and reads on the *same* collection can proceed concurrently.
pub struct OxiDb {
    data_dir: PathBuf,
    collections: RwLock<HashMap<String, Arc<RwLock<Collection>>>>,
}

impl OxiDb {
    /// Open or create a database at the given directory.
    pub fn open(data_dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        Ok(Self {
            data_dir: data_dir.to_path_buf(),
            collections: RwLock::new(HashMap::new()),
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

    pub fn compact(&self, collection: &str) -> Result<CompactStats> {
        let col = self.get_or_create_collection(collection)?;
        col.write().unwrap().compact()
    }
}
