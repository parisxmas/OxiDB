use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use lru::LruCache;
use serde_json::Value;

use crate::document::DocumentId;

/// Default maximum number of documents held in the LRU cache.
pub const DEFAULT_CAPACITY: usize = 100_000;

/// A bounded LRU document cache with interior mutability.
///
/// Wraps `lru::LruCache` in a `Mutex` so that cache hits can update
/// access order even through a shared `&self` reference. The mutex is
/// held only for the duration of a hash-map lookup + Arc clone, so
/// contention is minimal.
pub struct DocCache {
    inner: Mutex<LruCache<DocumentId, Arc<Value>>>,
}

impl DocCache {
    /// Create a new cache with the given maximum capacity.
    pub fn new(capacity: usize) -> Self {
        let cap = NonZeroUsize::new(capacity.max(1)).unwrap();
        Self {
            inner: Mutex::new(LruCache::new(cap)),
        }
    }

    /// Look up a document by ID, promoting it to most-recently-used.
    /// Returns `None` on cache miss.
    pub fn get(&self, id: DocumentId) -> Option<Arc<Value>> {
        let mut cache = self.inner.lock().unwrap();
        cache.get(&id).map(Arc::clone)
    }

    /// Look up without promoting (peek). Useful during iteration
    /// when we don't want to disturb eviction order.
    pub fn peek(&self, id: DocumentId) -> Option<Arc<Value>> {
        let cache = self.inner.lock().unwrap();
        cache.peek(&id).map(Arc::clone)
    }

    /// Insert or update a document in the cache. May evict the
    /// least-recently-used entry if the cache is full.
    pub fn put(&self, id: DocumentId, doc: Arc<Value>) {
        let mut cache = self.inner.lock().unwrap();
        cache.put(id, doc);
    }

    /// Remove a document from the cache.
    pub fn remove(&self, id: DocumentId) {
        let mut cache = self.inner.lock().unwrap();
        cache.pop(&id);
    }

    /// Remove all entries.
    pub fn clear(&self) {
        let mut cache = self.inner.lock().unwrap();
        cache.clear();
    }

    /// Number of entries currently in the cache.
    pub fn len(&self) -> usize {
        let cache = self.inner.lock().unwrap();
        cache.len()
    }

    /// Resize the cache capacity. Entries beyond the new capacity
    /// are evicted immediately (LRU order).
    pub fn resize(&self, capacity: usize) {
        let cap = NonZeroUsize::new(capacity.max(1)).unwrap();
        let mut cache = self.inner.lock().unwrap();
        cache.resize(cap);
    }

    /// Current maximum capacity.
    pub fn capacity(&self) -> usize {
        let cache = self.inner.lock().unwrap();
        cache.cap().get()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn basic_put_and_get() {
        let cache = DocCache::new(10);
        let doc = Arc::new(json!({"name": "test"}));
        cache.put(1, doc.clone());
        assert_eq!(cache.get(1).unwrap(), doc);
    }

    #[test]
    fn miss_returns_none() {
        let cache = DocCache::new(10);
        assert!(cache.get(42).is_none());
    }

    #[test]
    fn eviction_at_capacity() {
        let cache = DocCache::new(2);
        cache.put(1, Arc::new(json!(1)));
        cache.put(2, Arc::new(json!(2)));
        cache.put(3, Arc::new(json!(3))); // evicts 1
        assert!(cache.get(1).is_none());
        assert!(cache.get(2).is_some());
        assert!(cache.get(3).is_some());
    }

    #[test]
    fn lru_order_respected() {
        let cache = DocCache::new(2);
        cache.put(1, Arc::new(json!(1)));
        cache.put(2, Arc::new(json!(2)));
        // Access 1 to make it recently used
        cache.get(1);
        // Insert 3 — should evict 2 (least recently used)
        cache.put(3, Arc::new(json!(3)));
        assert!(cache.get(1).is_some());
        assert!(cache.get(2).is_none());
        assert!(cache.get(3).is_some());
    }

    #[test]
    fn remove_and_clear() {
        let cache = DocCache::new(10);
        cache.put(1, Arc::new(json!(1)));
        cache.put(2, Arc::new(json!(2)));
        cache.remove(1);
        assert!(cache.get(1).is_none());
        assert_eq!(cache.len(), 1);
        cache.clear();
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn resize_evicts() {
        let cache = DocCache::new(10);
        for i in 0..10 {
            cache.put(i, Arc::new(json!(i)));
        }
        assert_eq!(cache.len(), 10);
        cache.resize(3);
        assert_eq!(cache.len(), 3);
    }
}
