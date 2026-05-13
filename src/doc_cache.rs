use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use crate::locks::Mutex;

use lru::LruCache;
use serde_json::Value;

use crate::document::DocumentId;

/// Default maximum number of documents held in the LRU cache.
pub const DEFAULT_CAPACITY: usize = 100_000;

/// Number of shards to reduce lock contention under concurrent access.
/// Must be a power of two for fast modulo via bitmask.
const NUM_SHARDS: usize = 16;
const SHARD_MASK: u64 = (NUM_SHARDS as u64) - 1;

/// A bounded, sharded LRU document cache with interior mutability.
///
/// Splits entries across `NUM_SHARDS` independent LRU caches, each with
/// its own `Mutex`. Concurrent readers hitting different shards never
/// contend. This reduces p99 latency from ~8x to near-parity with
/// MongoDB for concurrent `find_one` workloads.
///
/// Shards are lazily allocated: each `Mutex<Option<LruCache<…>>>`
/// starts at `None`, and the inner `LruCache` is only materialised on
/// the first `put` to that shard. A 100 K-slot LRU preallocates ~50 KB
/// of hashtable buckets per shard regardless of use — at 10 K
/// collections × 16 shards that compounded to several GiB of dead
/// preallocation in the collection-scale bench. Idle / write-rarely
/// collections now hold near-zero RSS.
pub struct DocCache {
    /// Per-shard cap target. Used by lazy materialisations and
    /// updated atomically by `resize`. `AtomicUsize` (not stored as
    /// NonZeroUsize) so resize is lock-free; constructor + readers
    /// enforce the `>= 1` invariant.
    per_shard_cap: AtomicUsize,
    shards: Vec<Mutex<Option<LruCache<DocumentId, Arc<Value>>>>>,
    /// Cumulative cache hits (Relaxed — counter is observational, not
    /// load-bearing; missing a few under contention is fine).
    hits: AtomicU64,
    /// Cumulative cache misses.
    misses: AtomicU64,
}

/// Snapshot of cache hit/miss counters, returned by `DocCache::stats`.
#[derive(Debug, Clone, Copy)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
}

impl CacheStats {
    pub fn total(&self) -> u64 {
        self.hits + self.misses
    }
    pub fn hit_ratio(&self) -> f64 {
        let total = self.total();
        if total == 0 { 0.0 } else { self.hits as f64 / total as f64 }
    }
}

#[inline]
fn shard_for(id: DocumentId) -> usize {
    (id & SHARD_MASK) as usize
}

impl DocCache {
    /// Create a new cache with the given maximum capacity.
    /// Capacity is distributed evenly across shards. Shards are
    /// allocated lazily on first `put`; until then each holds `None`.
    pub fn new(capacity: usize) -> Self {
        let per_shard = (capacity / NUM_SHARDS).max(1);
        let shards = (0..NUM_SHARDS).map(|_| Mutex::new(None)).collect();
        Self {
            per_shard_cap: AtomicUsize::new(per_shard),
            shards,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    fn per_shard_cap(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.per_shard_cap.load(Ordering::Acquire).max(1)).unwrap()
    }

    /// Look up a document by ID, promoting it to most-recently-used.
    /// Returns `None` on cache miss. Updates hit/miss counters.
    pub fn get(&self, id: DocumentId) -> Option<Arc<Value>> {
        let mut shard = self.shards[shard_for(id)].lock();
        let found = match shard.as_mut() {
            Some(cache) => cache.get(&id).map(Arc::clone),
            None => None,
        };
        drop(shard);
        if found.is_some() {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
        found
    }

    /// Snapshot the hit/miss counters. Used for observability and the
    /// adaptive-sizing decision in tuning workflows.
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
        }
    }

    /// Reset the hit/miss counters. Useful when measuring a specific
    /// window (e.g. ignore warmup phase).
    pub fn reset_stats(&self) {
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
    }

    /// Look up without promoting (peek). Useful during iteration
    /// when we don't want to disturb eviction order.
    pub fn peek(&self, id: DocumentId) -> Option<Arc<Value>> {
        let shard = self.shards[shard_for(id)].lock();
        shard.as_ref().and_then(|c| c.peek(&id).map(Arc::clone))
    }

    /// Insert or update a document in the cache. Materialises the
    /// underlying shard if this is the first write to land on it.
    /// May evict the least-recently-used entry if the shard is full.
    pub fn put(&self, id: DocumentId, doc: Arc<Value>) {
        let cap = self.per_shard_cap();
        let mut shard = self.shards[shard_for(id)].lock();
        let cache = shard.get_or_insert_with(|| LruCache::new(cap));
        cache.put(id, doc);
    }

    /// Remove a document from the cache.
    pub fn remove(&self, id: DocumentId) {
        let mut shard = self.shards[shard_for(id)].lock();
        if let Some(cache) = shard.as_mut() {
            cache.pop(&id);
        }
    }

    /// Drop every shard's contents *and* its backing allocation. Next
    /// `put` re-materialises the affected shard at the current
    /// per-shard cap.
    pub fn clear(&self) {
        for shard in &self.shards {
            *shard.lock() = None;
        }
    }

    /// Number of entries currently in the cache.
    pub fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.lock().as_ref().map(|c| c.len()).unwrap_or(0))
            .sum()
    }

    /// Resize the cache capacity. Updates the lazy-alloc target for
    /// untouched shards and resizes any already-allocated shards in
    /// place; entries beyond the new per-shard cap are evicted in LRU
    /// order.
    pub fn resize(&self, capacity: usize) {
        let per_shard = (capacity / NUM_SHARDS).max(1);
        self.per_shard_cap.store(per_shard, Ordering::Release);
        let new_cap = NonZeroUsize::new(per_shard).unwrap();
        for shard in &self.shards {
            let mut s = shard.lock();
            if let Some(cache) = s.as_mut() {
                cache.resize(new_cap);
            }
        }
    }

    /// Current maximum capacity (per-shard target × shard count).
    pub fn capacity(&self) -> usize {
        self.per_shard_cap.load(Ordering::Acquire) * NUM_SHARDS
    }

    /// Probe the cache for multiple IDs in a single pass.
    /// Groups IDs by shard, acquires each shard lock once,
    /// then returns results in the original order.
    pub fn get_many(&self, ids: &[DocumentId]) -> Vec<Option<Arc<Value>>> {
        let mut result: Vec<Option<Arc<Value>>> = vec![None; ids.len()];

        // Group indices by shard to minimize lock acquisitions
        let mut shard_groups: Vec<Vec<usize>> = vec![Vec::new(); NUM_SHARDS];
        for (i, &id) in ids.iter().enumerate() {
            shard_groups[shard_for(id)].push(i);
        }

        for (shard_idx, indices) in shard_groups.iter().enumerate() {
            if indices.is_empty() {
                continue;
            }
            let mut shard = self.shards[shard_idx].lock();
            if let Some(cache) = shard.as_mut() {
                for &i in indices {
                    if let Some(arc) = cache.get(&ids[i]) {
                        result[i] = Some(Arc::clone(arc));
                    }
                }
            }
        }

        result
    }

    /// Insert multiple entries, grouping by shard for efficiency.
    /// Materialises only the shards that receive at least one entry.
    pub fn put_many(&self, entries: impl IntoIterator<Item = (DocumentId, Arc<Value>)>) {
        // Collect first to group by shard
        let entries: Vec<_> = entries.into_iter().collect();

        let mut shard_groups: Vec<Vec<(DocumentId, Arc<Value>)>> =
            (0..NUM_SHARDS).map(|_| Vec::new()).collect();
        for (id, doc) in entries {
            shard_groups[shard_for(id)].push((id, doc));
        }

        let cap = self.per_shard_cap();
        for (shard_idx, group) in shard_groups.into_iter().enumerate() {
            if group.is_empty() {
                continue;
            }
            let mut shard = self.shards[shard_idx].lock();
            let cache = shard.get_or_insert_with(|| LruCache::new(cap));
            for (id, doc) in group {
                cache.put(id, doc);
            }
        }
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
        // Use IDs that map to the same shard to test eviction
        // shard_for(id) = id & 0xF, so IDs 0, 16, 32 all map to shard 0
        let cache = DocCache::new(NUM_SHARDS * 2); // 2 per shard
        cache.put(0, Arc::new(json!(0)));
        cache.put(16, Arc::new(json!(16)));
        cache.put(32, Arc::new(json!(32))); // evicts 0 in shard 0
        assert!(cache.get(0).is_none());
        assert!(cache.get(16).is_some());
        assert!(cache.get(32).is_some());
    }

    #[test]
    fn lru_order_respected() {
        // Use IDs in the same shard
        let cache = DocCache::new(NUM_SHARDS * 2); // 2 per shard
        cache.put(0, Arc::new(json!(0)));
        cache.put(16, Arc::new(json!(16)));
        // Access 0 to make it recently used
        cache.get(0);
        // Insert 32 — should evict 16 (least recently used in shard 0)
        cache.put(32, Arc::new(json!(32)));
        assert!(cache.get(0).is_some());
        assert!(cache.get(16).is_none());
        assert!(cache.get(32).is_some());
    }

    #[test]
    fn remove_and_clear() {
        let cache = DocCache::new(160);
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
        let cache = DocCache::new(160);
        for i in 0..160u64 {
            cache.put(i, Arc::new(json!(i)));
        }
        assert_eq!(cache.len(), 160);
        // Resize to 3 per shard = 48 total
        cache.resize(NUM_SHARDS * 3);
        assert!(cache.len() <= NUM_SHARDS * 3);
    }

    #[test]
    fn get_many_works() {
        let cache = DocCache::new(160);
        cache.put(1, Arc::new(json!(1)));
        cache.put(2, Arc::new(json!(2)));
        cache.put(3, Arc::new(json!(3)));
        let results = cache.get_many(&[1, 42, 3]);
        assert!(results[0].is_some());
        assert!(results[1].is_none());
        assert!(results[2].is_some());
    }

    #[test]
    fn lazy_alloc_until_first_put() {
        let cache = DocCache::new(160);
        // Freshly constructed: every shard is None.
        let allocated = cache
            .shards
            .iter()
            .filter(|s| s.lock().is_some())
            .count();
        assert_eq!(allocated, 0, "no shard should be allocated yet");
        cache.put(0, Arc::new(json!(0)));
        let after_put = cache
            .shards
            .iter()
            .filter(|s| s.lock().is_some())
            .count();
        assert_eq!(after_put, 1, "only the touched shard should allocate");
        cache.clear();
        let after_clear = cache
            .shards
            .iter()
            .filter(|s| s.lock().is_some())
            .count();
        assert_eq!(after_clear, 0, "clear must reclaim every shard");
    }

    #[test]
    fn put_many_works() {
        let cache = DocCache::new(160);
        let entries = vec![
            (1u64, Arc::new(json!(1))),
            (2, Arc::new(json!(2))),
            (3, Arc::new(json!(3))),
        ];
        cache.put_many(entries);
        assert!(cache.get(1).is_some());
        assert!(cache.get(2).is_some());
        assert!(cache.get(3).is_some());
    }
}
