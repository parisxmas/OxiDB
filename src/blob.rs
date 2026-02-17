use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

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
    next_id: u64,
}

pub struct BlobStore {
    base_dir: PathBuf,
    buckets: RwLock<HashMap<String, BucketState>>,
    encryption: Option<Arc<EncryptionKey>>,
}

impl BlobStore {
    pub fn open(data_dir: &Path) -> Result<Self> {
        Self::open_with_encryption(data_dir, None)
    }

    pub fn open_with_encryption(data_dir: &Path, encryption: Option<Arc<EncryptionKey>>) -> Result<Self> {
        let base_dir = data_dir.join("_blobs");
        std::fs::create_dir_all(&base_dir)?;

        let mut buckets = HashMap::new();

        if base_dir.exists() {
            for entry in std::fs::read_dir(&base_dir)? {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    let bucket_name = entry.file_name().to_string_lossy().to_string();
                    let state = Self::scan_bucket(&entry.path(), &encryption)?;
                    buckets.insert(bucket_name, state);
                }
            }
        }

        Ok(Self {
            base_dir,
            buckets: RwLock::new(buckets),
            encryption,
        })
    }

    fn scan_bucket(bucket_path: &Path, encryption: &Option<Arc<EncryptionKey>>) -> Result<BucketState> {
        let mut keys = HashMap::new();
        let mut max_id: u64 = 0;

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
                    if id >= max_id {
                        max_id = id + 1;
                    }
                }
            }
        }

        Ok(BucketState {
            keys,
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

    pub fn create_bucket(&self, name: &str) -> Result<()> {
        std::fs::create_dir_all(self.bucket_path(name))?;
        let mut buckets = self.buckets.write().unwrap();
        buckets.entry(name.to_string()).or_insert(BucketState {
            keys: HashMap::new(),
            next_id: 0,
        });
        Ok(())
    }

    pub fn list_buckets(&self) -> Vec<String> {
        let buckets = self.buckets.read().unwrap();
        let mut names: Vec<String> = buckets.keys().cloned().collect();
        names.sort();
        names
    }

    pub fn delete_bucket(&self, name: &str) -> Result<()> {
        let mut buckets = self.buckets.write().unwrap();
        if !buckets.contains_key(name) {
            return Err(Error::BucketNotFound(name.to_string()));
        }
        let path = self.bucket_path(name);
        if path.exists() {
            std::fs::remove_dir_all(path)?;
        }
        buckets.remove(name);
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
        // Auto-create bucket if it doesn't exist
        std::fs::create_dir_all(self.bucket_path(bucket))?;

        let mut buckets = self.buckets.write().unwrap();
        let state = buckets
            .entry(bucket.to_string())
            .or_insert(BucketState {
                keys: HashMap::new(),
                next_id: 0,
            });

        // Reuse existing ID if key already exists, otherwise allocate new
        let id = if let Some(&existing_id) = state.keys.get(key) {
            existing_id
        } else {
            let id = state.next_id;
            state.next_id += 1;
            state.keys.insert(key.to_string(), id);
            id
        };

        let etag = format!("{:08x}", crc32fast::hash(data));
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
        std::fs::write(self.data_path(bucket, id), data_to_write)?;
        let meta_json = serde_json::to_vec(&meta)?;
        let meta_to_write = match &self.encryption {
            Some(key) => key.encrypt(&meta_json)?,
            None => meta_json,
        };
        std::fs::write(self.meta_path(bucket, id), meta_to_write)?;

        Ok(meta)
    }

    pub fn get_object(&self, bucket: &str, key: &str) -> Result<(Vec<u8>, ObjectMeta)> {
        let buckets = self.buckets.read().unwrap();
        let state = buckets
            .get(bucket)
            .ok_or_else(|| Error::BucketNotFound(bucket.to_string()))?;
        let &id = state.keys.get(key).ok_or_else(|| Error::BlobNotFound {
            bucket: bucket.to_string(),
            key: key.to_string(),
        })?;

        let raw_data = std::fs::read(self.data_path(bucket, id))?;
        let data = match &self.encryption {
            Some(key) => key.decrypt(&raw_data)?,
            None => raw_data,
        };
        let raw_meta = std::fs::read(self.meta_path(bucket, id))?;
        let meta_bytes = match &self.encryption {
            Some(key) => key.decrypt(&raw_meta)?,
            None => raw_meta,
        };
        let meta: ObjectMeta = serde_json::from_slice(&meta_bytes)?;

        Ok((data, meta))
    }

    pub fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectMeta> {
        let buckets = self.buckets.read().unwrap();
        let state = buckets
            .get(bucket)
            .ok_or_else(|| Error::BucketNotFound(bucket.to_string()))?;
        let &id = state.keys.get(key).ok_or_else(|| Error::BlobNotFound {
            bucket: bucket.to_string(),
            key: key.to_string(),
        })?;

        let raw_meta = std::fs::read(self.meta_path(bucket, id))?;
        let meta_bytes = match &self.encryption {
            Some(key) => key.decrypt(&raw_meta)?,
            None => raw_meta,
        };
        let meta: ObjectMeta = serde_json::from_slice(&meta_bytes)?;
        Ok(meta)
    }

    pub fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        let mut buckets = self.buckets.write().unwrap();
        let state = buckets
            .get_mut(bucket)
            .ok_or_else(|| Error::BucketNotFound(bucket.to_string()))?;
        let id = state
            .keys
            .remove(key)
            .ok_or_else(|| Error::BlobNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            })?;

        let data_path = self.data_path(bucket, id);
        if data_path.exists() {
            std::fs::remove_file(data_path)?;
        }
        let meta_path = self.meta_path(bucket, id);
        if meta_path.exists() {
            std::fs::remove_file(meta_path)?;
        }

        Ok(())
    }

    pub fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<ObjectMeta>> {
        let buckets = self.buckets.read().unwrap();
        let state = buckets
            .get(bucket)
            .ok_or_else(|| Error::BucketNotFound(bucket.to_string()))?;

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
        let mut results = Vec::new();

        for (_, &id) in matching_keys.into_iter().take(limit) {
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
