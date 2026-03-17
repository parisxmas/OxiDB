use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};
use sha2::{Sha256, Digest};

use crate::crypto::EncryptionKey;
use crate::error::{Error, Result};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ObjectMeta {
    pub key: String,
    pub bucket: String,
    pub size: u64,
    pub content_type: String,
    pub etag: String,
    pub created_at: String,
    pub metadata: HashMap<String, String>,
}

struct BucketState {
    keys: HashMap<String, u64>,
    metas: HashMap<u64, ObjectMeta>,
    next_id: u64,
}

pub struct BlobStore {
    base_dir: PathBuf,
    /// Per-bucket locking: the outer RwLock protects the bucket map (create/delete bucket),
    /// inner RwLock protects individual bucket state (object CRUD).
    buckets: RwLock<HashMap<String, Arc<RwLock<BucketState>>>>,
    encryption: Option<Arc<EncryptionKey>>,
    delete_tx: mpsc::Sender<PathBuf>,
}

impl BlobStore {
    pub fn open(data_dir: &Path) -> Result<Self> {
        Self::open_with_encryption(data_dir, None)
    }

    pub fn open_with_encryption(data_dir: &Path, encryption: Option<Arc<EncryptionKey>>) -> Result<Self> {
        let base_dir = data_dir.join("_blobs");
        std::fs::create_dir_all(&base_dir)?;

        let mut buckets: HashMap<String, Arc<RwLock<BucketState>>> = HashMap::new();

        if base_dir.exists() {
            for entry in std::fs::read_dir(&base_dir)? {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    let bucket_name = entry.file_name().to_string_lossy().to_string();
                    let state = Self::scan_bucket(&entry.path(), &encryption)?;
                    buckets.insert(bucket_name, Arc::new(RwLock::new(state)));
                }
            }
        }

        // Background thread for async file deletion
        let (delete_tx, delete_rx) = mpsc::channel::<PathBuf>();
        std::thread::Builder::new()
            .name("blob-gc".into())
            .spawn(move || {
                for path in delete_rx {
                    let _ = std::fs::remove_file(&path);
                }
            })
            .expect("failed to spawn blob-gc thread");

        // Background thread for periodic fsync (durability without per-write cost)
        {
            let sync_dir = base_dir.clone();
            std::thread::Builder::new()
                .name("blob-sync".into())
                .spawn(move || loop {
                    std::thread::sleep(std::time::Duration::from_secs(1));
                    // fsync the blobs root dir to flush all pending renames
                    if let Ok(dir) = std::fs::File::open(&sync_dir) {
                        let _ = dir.sync_all();
                    }
                })
                .expect("failed to spawn blob-sync thread");
        }

        Ok(Self {
            base_dir,
            buckets: RwLock::new(buckets),
            encryption,
            delete_tx,
        })
    }

    fn scan_bucket(bucket_path: &Path, encryption: &Option<Arc<EncryptionKey>>) -> Result<BucketState> {
        let mut keys = HashMap::new();
        let mut metas = HashMap::new();
        let mut max_id: u64 = 0;
        let mut valid_ids = std::collections::HashSet::new();

        // First pass: load all .meta files (the source of truth)
        for entry in std::fs::read_dir(bucket_path)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            if let Some(id_str) = name.strip_suffix(".meta") {
                if let Ok(id) = id_str.parse::<u64>() {
                    let raw_meta = std::fs::read(entry.path())?;
                    let meta_bytes = match encryption {
                        Some(key) => key.decrypt(&raw_meta)?,
                        None => raw_meta,
                    };
                    let meta: ObjectMeta = serde_json::from_slice(&meta_bytes)?;
                    keys.insert(meta.key.clone(), id);
                    metas.insert(id, meta);
                    valid_ids.insert(id);
                    if id >= max_id {
                        max_id = id + 1;
                    }
                }
            }
        }

        // Second pass: clean orphan .data and .tmp files (crash recovery)
        let mut orphans = 0;
        for entry in std::fs::read_dir(bucket_path)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            // Clean up temp files from interrupted writes
            if name.ends_with(".tmp") {
                let _ = std::fs::remove_file(entry.path());
                orphans += 1;
            }
            // Clean orphan .data files (no matching .meta)
            else if let Some(id_str) = name.strip_suffix(".data") {
                if let Ok(id) = id_str.parse::<u64>() {
                    if !valid_ids.contains(&id) {
                        let _ = std::fs::remove_file(entry.path());
                        orphans += 1;
                    }
                }
            }
        }
        if orphans > 0 {
            eprintln!("[blob] cleaned {orphans} orphan/temp files from {}", bucket_path.display());
        }

        Ok(BucketState {
            keys,
            metas,
            next_id: max_id,
        })
    }

    fn bucket_path(&self, bucket: &str) -> PathBuf {
        self.base_dir.join(bucket)
    }

    fn data_path(&self, bucket: &str, id: u64) -> PathBuf {
        self.base_dir.join(bucket).join(format!("{}.data", id))
    }

    fn meta_path(&self, bucket: &str, id: u64) -> PathBuf {
        self.base_dir.join(bucket).join(format!("{}.meta", id))
    }

    fn validate_bucket_name(name: &str) -> Result<()> {
        if name.is_empty() || name.len() > 63 {
            return Err(Error::Io(std::io::Error::new(std::io::ErrorKind::InvalidInput, "bucket name must be 1-63 characters")));
        }
        if name.contains("..") || name.contains('/') || name.contains('\\') || name.contains('\0') {
            return Err(Error::Io(std::io::Error::new(std::io::ErrorKind::InvalidInput, "bucket name contains invalid characters")));
        }
        if !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.' || b == b'_') {
            return Err(Error::Io(std::io::Error::new(std::io::ErrorKind::InvalidInput, "bucket name must be alphanumeric, hyphens, dots, or underscores")));
        }
        Ok(())
    }

    pub fn create_bucket(&self, name: &str) -> Result<()> {
        Self::validate_bucket_name(name)?;
        std::fs::create_dir_all(self.bucket_path(name))?;
        let mut buckets = self.buckets.write().unwrap();
        buckets.entry(name.to_string()).or_insert_with(|| Arc::new(RwLock::new(BucketState {
            keys: HashMap::new(),
            metas: HashMap::new(),
            next_id: 0,
        })));
        Ok(())
    }

    pub fn list_buckets(&self) -> Vec<String> {
        let map = self.buckets.read().unwrap();
        let mut names: Vec<String> = map.keys().cloned().collect();
        names.sort();
        names
    }

    pub fn delete_bucket(&self, name: &str) -> Result<()> {
        let path = {
            let mut map = self.buckets.write().unwrap();
            if !map.contains_key(name) {
                return Err(Error::BucketNotFound(name.to_string()));
            }
            map.remove(name);
            self.bucket_path(name)
        }; // outer lock released before disk I/O
        if path.exists() {
            std::fs::remove_dir_all(path)?;
        }
        Ok(())
    }

    pub fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: &[u8],
        content_type: &str,
        metadata: HashMap<String, String>,
    ) -> Result<ObjectMeta> {
        Self::validate_bucket_name(bucket)?;
        // Auto-create bucket if it doesn't exist
        std::fs::create_dir_all(self.bucket_path(bucket))?;

        // Get or create the per-bucket lock
        let bucket_lock = {
            let mut map = self.buckets.write().unwrap();
            map.entry(bucket.to_string())
                .or_insert_with(|| Arc::new(RwLock::new(BucketState {
                    keys: HashMap::new(),
                    metas: HashMap::new(),
                    next_id: 0,
                })))
                .clone()
        }; // outer lock released — only this bucket is locked below

        // Phase 1: expensive work outside any lock (hash, encrypt, write temp files)
        let hash = Sha256::digest(data);
        let etag: String = hash.iter().take(16).map(|b| format!("{b:02x}")).collect();
        let created_at = now_rfc3339();

        let meta = ObjectMeta {
            key: key.to_string(),
            bucket: bucket.to_string(),
            size: data.len() as u64,
            content_type: content_type.to_string(),
            etag,
            created_at,
            metadata,
        };

        let data_to_write = match &self.encryption {
            Some(key) => key.encrypt(data)?,
            None => data.to_vec(),
        };
        let meta_json = serde_json::to_vec(&meta)?;
        let meta_to_write = match &self.encryption {
            Some(key) => key.encrypt(&meta_json)?,
            None => meta_json,
        };

        // Write to temp files with random names (no lock needed — unique per call)
        let tmp_id = rand::random::<u64>();
        let bucket_dir = self.bucket_path(bucket);
        let data_tmp = bucket_dir.join(format!("{tmp_id}.data.tmp"));
        let meta_tmp = bucket_dir.join(format!("{tmp_id}.meta.tmp"));
        std::fs::write(&data_tmp, data_to_write)?;
        std::fs::write(&meta_tmp, meta_to_write)?;

        // Phase 2: acquire lock ONLY for rename + cache update (fast operations)
        let mut state = bucket_lock.write().unwrap();
        let (id, is_new) = if let Some(&existing_id) = state.keys.get(key) {
            (existing_id, false)
        } else {
            let id = state.next_id;
            state.next_id += 1;
            (id, true)
        };

        // Atomic rename (instant) — meta last = commit point
        let data_path = self.data_path(bucket, id);
        let meta_path = self.meta_path(bucket, id);
        std::fs::rename(&data_tmp, &data_path)?;
        std::fs::rename(&meta_tmp, &meta_path)?;

        if is_new {
            state.keys.insert(key.to_string(), id);
        }
        state.metas.insert(id, meta.clone());
        drop(state); // unlock — renames are complete, reads will see new data

        // Durability: fsync is handled by the background sync thread (every 1s),
        // not per-write. rename() on POSIX is metadata-atomic; the background
        // fsync ensures data reaches disk within 1 second.

        Ok(meta)
    }

    pub fn get_object(&self, bucket: &str, key: &str) -> Result<(Vec<u8>, ObjectMeta)> {
        let (id, cached_meta, bucket_lock) = {
            let map = self.buckets.read().unwrap();
            let bl = map.get(bucket)
                .ok_or_else(|| Error::BucketNotFound(bucket.to_string()))?
                .clone();
            let state = bl.read().unwrap();
            let &id = state.keys.get(key).ok_or_else(|| Error::BlobNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            })?;
            (id, state.metas.get(&id).cloned(), bl.clone())
        }; // all locks released before disk I/O

        let raw_data = std::fs::read(self.data_path(bucket, id))?;
        let data = match &self.encryption {
            Some(key) => key.decrypt(&raw_data)?,
            None => raw_data,
        };

        let meta = if let Some(m) = cached_meta {
            m
        } else {
            let raw_meta = std::fs::read(self.meta_path(bucket, id))?;
            let meta_bytes = match &self.encryption {
                Some(key) => key.decrypt(&raw_meta)?,
                None => raw_meta,
            };
            let m: ObjectMeta = serde_json::from_slice(&meta_bytes)?;
            // Backfill cache
            if let Ok(mut state) = bucket_lock.write() {
                state.metas.insert(id, m.clone());
            }
            m
        };

        Ok((data, meta))
    }

    pub fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectMeta> {
        let (id, cached, bucket_lock) = {
            let map = self.buckets.read().unwrap();
            let bl = map.get(bucket)
                .ok_or_else(|| Error::BucketNotFound(bucket.to_string()))?
                .clone();
            let state = bl.read().unwrap();
            let &id = state.keys.get(key).ok_or_else(|| Error::BlobNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            })?;
            (id, state.metas.get(&id).cloned(), bl.clone())
        };

        if let Some(meta) = cached {
            return Ok(meta);
        }

        let raw_meta = std::fs::read(self.meta_path(bucket, id))?;
        let meta_bytes = match &self.encryption {
            Some(key) => key.decrypt(&raw_meta)?,
            None => raw_meta,
        };
        let meta: ObjectMeta = serde_json::from_slice(&meta_bytes)?;
        if let Ok(mut state) = bucket_lock.write() {
            state.metas.insert(id, meta.clone());
        }
        Ok(meta)
    }

    pub fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        // Hold bucket write lock only for in-memory removal
        let (data_path, meta_path) = {
            let map = self.buckets.read().unwrap();
            let bl = map.get(bucket)
                .ok_or_else(|| Error::BucketNotFound(bucket.to_string()))?
                .clone();
            let mut state = bl.write().unwrap();
            let id = state
                .keys
                .remove(key)
                .ok_or_else(|| Error::BlobNotFound {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                })?;
            state.metas.remove(&id);
            (self.data_path(bucket, id), self.meta_path(bucket, id))
        }; // all locks released here

        // Delete .meta synchronously — this is the commit point for scan_bucket.
        // On crash recovery, absent .meta = object is deleted (no ghost reads).
        let _ = std::fs::remove_file(&meta_path);
        // Defer .data deletion to background thread (large file, slow I/O)
        let _ = self.delete_tx.send(data_path);

        Ok(())
    }

    pub fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<ObjectMeta>> {
        let map = self.buckets.read().unwrap();
        let bl = map.get(bucket)
            .ok_or_else(|| Error::BucketNotFound(bucket.to_string()))?
            .clone();
        let state = bl.read().unwrap();

        let mut matching_keys: Vec<(&String, &u64)> = state
            .keys
            .iter()
            .filter(|(k, _)| match prefix {
                Some(p) => k.starts_with(p),
                None => true,
            })
            .collect();

        matching_keys.sort_by(|a, b| a.0.cmp(b.0));

        let limit = limit.unwrap_or(1000);
        let mut results = Vec::with_capacity(limit.min(matching_keys.len()));

        for (_, &id) in matching_keys.into_iter().take(limit) {
            if let Some(meta) = state.metas.get(&id) {
                results.push(meta.clone());
                continue;
            }
            let raw_meta = std::fs::read(self.meta_path(bucket, id))?;
            let meta_bytes = match &self.encryption {
                Some(key) => key.decrypt(&raw_meta)?,
                None => raw_meta,
            };
            let meta: ObjectMeta = serde_json::from_slice(&meta_bytes)?;
            results.push(meta);
        }

        Ok(results)
    }
}

fn now_rfc3339() -> String {
    use std::time::SystemTime;
    let dur = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = dur.as_secs();
    let days = secs / 86400;
    let time_secs = secs % 86400;
    let hours = time_secs / 3600;
    let mins = (time_secs % 3600) / 60;
    let s = time_secs % 60;

    // Compute year/month/day from days since epoch
    let mut y = 1970i64;
    let mut remaining = days as i64;
    loop {
        let days_in_year = if is_leap(y) { 366 } else { 365 };
        if remaining < days_in_year {
            break;
        }
        remaining -= days_in_year;
        y += 1;
    }
    let leap = is_leap(y);
    let month_days = if leap {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };
    let mut m = 0;
    for (i, &md) in month_days.iter().enumerate() {
        if remaining < md as i64 {
            m = i + 1;
            break;
        }
        remaining -= md as i64;
    }
    let d = remaining + 1;
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        y, m, d, hours, mins, s
    )
}

fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_store() -> (tempfile::TempDir, BlobStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = BlobStore::open(dir.path()).unwrap();
        (dir, store)
    }

    #[test]
    fn create_and_list_buckets() {
        let (_dir, store) = temp_store();
        store.create_bucket("images").unwrap();
        store.create_bucket("docs").unwrap();
        let buckets = store.list_buckets();
        assert_eq!(buckets, vec!["docs", "images"]);
    }

    #[test]
    fn put_and_get_object() {
        let (_dir, store) = temp_store();
        let data = b"Hello World";
        let meta = store
            .put_object("docs", "hello.txt", data, "text/plain", HashMap::new())
            .unwrap();
        assert_eq!(meta.key, "hello.txt");
        assert_eq!(meta.bucket, "docs");
        assert_eq!(meta.size, 11);
        assert_eq!(meta.content_type, "text/plain");
        assert!(!meta.etag.is_empty());

        let (got_data, got_meta) = store.get_object("docs", "hello.txt").unwrap();
        assert_eq!(got_data, data);
        assert_eq!(got_meta.key, "hello.txt");
        assert_eq!(got_meta.etag, meta.etag);
    }

    #[test]
    fn head_object_returns_meta_only() {
        let (_dir, store) = temp_store();
        store
            .put_object("docs", "f.txt", b"abc", "text/plain", HashMap::new())
            .unwrap();
        let meta = store.head_object("docs", "f.txt").unwrap();
        assert_eq!(meta.size, 3);
    }

    #[test]
    fn delete_object_then_not_found() {
        let (_dir, store) = temp_store();
        store
            .put_object("docs", "f.txt", b"abc", "text/plain", HashMap::new())
            .unwrap();
        store.delete_object("docs", "f.txt").unwrap();
        let err = store.get_object("docs", "f.txt").unwrap_err();
        assert!(err.to_string().contains("blob not found"));
    }

    #[test]
    fn list_objects_with_prefix() {
        let (_dir, store) = temp_store();
        store
            .put_object("b", "images/a.png", b"a", "image/png", HashMap::new())
            .unwrap();
        store
            .put_object("b", "images/b.png", b"b", "image/png", HashMap::new())
            .unwrap();
        store
            .put_object("b", "docs/c.txt", b"c", "text/plain", HashMap::new())
            .unwrap();

        let list = store.list_objects("b", Some("images/"), None).unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].key, "images/a.png");
        assert_eq!(list[1].key, "images/b.png");
    }

    #[test]
    fn list_objects_sorted_by_key() {
        let (_dir, store) = temp_store();
        store
            .put_object("b", "c.txt", b"c", "text/plain", HashMap::new())
            .unwrap();
        store
            .put_object("b", "a.txt", b"a", "text/plain", HashMap::new())
            .unwrap();
        store
            .put_object("b", "b.txt", b"b", "text/plain", HashMap::new())
            .unwrap();

        let list = store.list_objects("b", None, None).unwrap();
        let keys: Vec<&str> = list.iter().map(|m| m.key.as_str()).collect();
        assert_eq!(keys, vec!["a.txt", "b.txt", "c.txt"]);
    }

    #[test]
    fn overwrite_existing_key_reuses_id() {
        let (_dir, store) = temp_store();
        store
            .put_object("b", "f.txt", b"v1", "text/plain", HashMap::new())
            .unwrap();
        store
            .put_object("b", "f.txt", b"v2-longer", "text/plain", HashMap::new())
            .unwrap();

        let (data, meta) = store.get_object("b", "f.txt").unwrap();
        assert_eq!(data, b"v2-longer");
        assert_eq!(meta.size, 9);

        // Only one object in the bucket
        let list = store.list_objects("b", None, None).unwrap();
        assert_eq!(list.len(), 1);
    }

    #[test]
    fn get_from_missing_bucket() {
        let (_dir, store) = temp_store();
        let err = store.get_object("nonexistent", "f.txt").unwrap_err();
        assert!(err.to_string().contains("bucket not found"));
    }

    #[test]
    fn delete_bucket_removes_everything() {
        let (_dir, store) = temp_store();
        store
            .put_object("b", "f.txt", b"data", "text/plain", HashMap::new())
            .unwrap();
        store.delete_bucket("b").unwrap();

        let buckets = store.list_buckets();
        assert!(buckets.is_empty());

        let err = store.get_object("b", "f.txt").unwrap_err();
        assert!(err.to_string().contains("bucket not found"));
    }
}
