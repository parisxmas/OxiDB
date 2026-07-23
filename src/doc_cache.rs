use crate::locks::Mutex;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use lru::LruCache;
use serde_json::Value;

use crate::document::DocumentId;

/// Memory budget for the deserialized-`Value` cache, and the per-entry size
/// used to derive a bounded entry count from it.
///
/// This cache is a *speed* layer over the primary store, which already holds
/// every document's encoded bytes resident in RAM — a miss here costs only a
/// re-decode (CPU), never I/O. So the cache is bounded by a fixed memory
/// budget that does **not** scale with the dataset, rather than an entry count
/// that could grow to cache an entire collection. A `serde_json::Value` for a
/// typical document is ~4 KiB on the heap (nested maps, per-`String` boxes),
/// so the default budget yields ~32K entries. The old default (100K entries
/// ≈ ~400 MiB) sized the cache to large collections and dominated RSS; see the
/// 1M-doc memory probe in `tests/mem_probe.rs`. Ops can still raise it via
/// `OXIDB_DOC_CACHE_SIZE` when a larger hot Value set pays off.
const VALUE_CACHE_BUDGET_BYTES: usize = 128 * 1024 * 1024; // 128 MiB
const APPROX_VALUE_HEAP_BYTES: usize = 4096;
const DEFAULT_CAPACITY_FALLBACK: usize = VALUE_CACHE_BUDGET_BYTES / APPROX_VALUE_HEAP_BYTES;

/// Read the doc cache capacity from `OXIDB_DOC_CACHE_SIZE`, cached after the
/// first call. Returns `DEFAULT_CAPACITY_FALLBACK` if the env var is absent
/// or doesn't parse.
pub fn default_capacity() -> usize {
    use std::sync::OnceLock;
    static CACHED: OnceLock<usize> = OnceLock::new();
    *CACHED.get_or_init(|| {
        std::env::var("OXIDB_DOC_CACHE_SIZE")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n >= NUM_SHARDS) // need at least 1 entry per shard
            .unwrap_or(DEFAULT_CAPACITY_FALLBACK)
    })
}

/// Legacy alias for code that hard-coded the constant. New callers should
/// use `default_capacity()` to pick up the env override.
pub const DEFAULT_CAPACITY: usize = DEFAULT_CAPACITY_FALLBACK;

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
pub struct Shared {
    /// Per-shard cap target. Used by lazy materialisations and
    /// updated atomically by `resize`. `AtomicUsize` (not stored as
    /// NonZeroUsize) so resize is lock-free; constructor + readers
    /// enforce the `>= 1` invariant.
    per_shard_cap: AtomicUsize,
    shards: Vec<Mutex<Option<LruCache<CacheKey, Arc<Value>>>>>,
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
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

/// A process-wide cache key: a per-collection namespace in the high 64 bits and
/// the document id in the low 64 bits. This lets one shared cache hold every
/// collection's documents under a single memory budget (instead of a separate
/// budget per collection, which made total RSS scale with the collection count).
pub type CacheKey = u128;

#[inline]
pub fn compose(ns: u64, id: DocumentId) -> CacheKey {
    ((ns as u128) << 64) | (id as u128)
}

#[inline]
fn shard_for(key: CacheKey) -> usize {
    // Shard by the document id (low 64 bits) so a collection's docs still spread
    // across shards.
    ((key as u64) & SHARD_MASK) as usize
}

/// Monotonic source of collection namespaces. Every `BTreeCollection` open (and
/// every cache clear) takes a fresh one, so a re-opened or cleared collection's
/// stale entries are simply orphaned and evicted by the LRU — no scan needed.
static NEXT_NS: AtomicU64 = AtomicU64::new(1);

/// Take a fresh, process-unique collection namespace.
pub fn next_ns() -> u64 {
    NEXT_NS.fetch_add(1, Ordering::Relaxed)
}

/// The single process-global deserialized-`Value` cache, shared by every
/// collection in every database. Sized once from `OXIDB_DOC_CACHE_SIZE`
/// (default 128 MiB budget) — that budget now bounds the whole process, not
/// each collection.
pub fn global() -> std::sync::Arc<Shared> {
    use std::sync::OnceLock;
    static G: OnceLock<std::sync::Arc<Shared>> = OnceLock::new();
    std::sync::Arc::clone(G.get_or_init(|| std::sync::Arc::new(Shared::new(default_capacity()))))
}

impl Shared {
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
    pub fn get(&self, key: CacheKey) -> Option<Arc<Value>> {
        let mut shard = self.shards[shard_for(key)].lock();
        let found = match shard.as_mut() {
            Some(cache) => cache.get(&key).map(Arc::clone),
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
    pub fn peek(&self, key: CacheKey) -> Option<Arc<Value>> {
        let shard = self.shards[shard_for(key)].lock();
        shard.as_ref().and_then(|c| c.peek(&key).map(Arc::clone))
    }

    /// Insert or update a document in the cache. Materialises the
    /// underlying shard if this is the first write to land on it.
    /// May evict the least-recently-used entry if the shard is full.
    pub fn put(&self, key: CacheKey, doc: Arc<Value>) {
        let cap = self.per_shard_cap();
        let mut shard = self.shards[shard_for(key)].lock();
        let cache = shard.get_or_insert_with(|| LruCache::new(cap));
        cache.put(key, doc);
    }

    /// Remove a document from the cache.
    pub fn remove(&self, key: CacheKey) {
        let mut shard = self.shards[shard_for(key)].lock();
        if let Some(cache) = shard.as_mut() {
            cache.pop(&key);
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
    pub fn get_many(&self, keys: &[CacheKey]) -> Vec<Option<Arc<Value>>> {
        let mut result: Vec<Option<Arc<Value>>> = vec![None; keys.len()];

        // Group indices by shard to minimize lock acquisitions
        let mut shard_groups: Vec<Vec<usize>> = vec![Vec::new(); NUM_SHARDS];
        for (i, &key) in keys.iter().enumerate() {
            shard_groups[shard_for(key)].push(i);
        }

        for (shard_idx, indices) in shard_groups.iter().enumerate() {
            if indices.is_empty() {
                continue;
            }
            let mut shard = self.shards[shard_idx].lock();
            if let Some(cache) = shard.as_mut() {
                for &i in indices {
                    if let Some(arc) = cache.get(&keys[i]) {
                        result[i] = Some(Arc::clone(arc));
                    }
                }
            }
        }

        result
    }

    /// Insert multiple entries, grouping by shard for efficiency.
    /// Materialises only the shards that receive at least one entry.
    pub fn put_many(&self, entries: impl IntoIterator<Item = (CacheKey, Arc<Value>)>) {
        // Collect first to group by shard
        let entries: Vec<_> = entries.into_iter().collect();

        let mut shard_groups: Vec<Vec<(CacheKey, Arc<Value>)>> =
            (0..NUM_SHARDS).map(|_| Vec::new()).collect();
        for (key, doc) in entries {
            shard_groups[shard_for(key)].push((key, doc));
        }

        let cap = self.per_shard_cap();
        for (shard_idx, group) in shard_groups.into_iter().enumerate() {
            if group.is_empty() {
                continue;
            }
            let mut shard = self.shards[shard_idx].lock();
            let cache = shard.get_or_insert_with(|| LruCache::new(cap));
            for (key, doc) in group {
                cache.put(key, doc);
            }
        }
    }
}

/// A per-collection facade over the shared global [`DocCache`]. It namespaces
/// every key with the collection's `ns` so the same document id in different
/// collections stays distinct, while all collections share one memory budget.
/// The `BTreeCollection`-facing API is unchanged (still takes `DocumentId`), so
/// call sites don't change.
pub struct DocCache {
    inner: std::sync::Arc<Shared>,
    ns: AtomicU64,
}

impl DocCache {
    /// Wrap the shared cache with a fresh collection namespace.
    pub fn new(_capacity: usize) -> Self {
        Self {
            inner: global(),
            ns: AtomicU64::new(next_ns()),
        }
    }

    #[inline]
    fn key(&self, id: DocumentId) -> CacheKey {
        compose(self.ns.load(Ordering::Relaxed), id)
    }

    pub fn get(&self, id: DocumentId) -> Option<Arc<Value>> {
        self.inner.get(self.key(id))
    }
    pub fn peek(&self, id: DocumentId) -> Option<Arc<Value>> {
        self.inner.peek(self.key(id))
    }
    pub fn put(&self, id: DocumentId, doc: Arc<Value>) {
        self.inner.put(self.key(id), doc);
    }
    pub fn remove(&self, id: DocumentId) {
        self.inner.remove(self.key(id));
    }
    pub fn get_many(&self, ids: &[DocumentId]) -> Vec<Option<Arc<Value>>> {
        let keys: Vec<CacheKey> = ids.iter().map(|&id| self.key(id)).collect();
        self.inner.get_many(&keys)
    }
    pub fn put_many(&self, entries: impl IntoIterator<Item = (DocumentId, Arc<Value>)>) {
        let ns = self.ns.load(Ordering::Relaxed);
        self.inner
            .put_many(entries.into_iter().map(|(id, doc)| (compose(ns, id), doc)));
    }
    /// Invalidate this collection's entries by taking a fresh namespace — the old
    /// keys become unreachable and the LRU evicts them (no global scan).
    pub fn clear(&self) {
        self.ns.store(next_ns(), Ordering::Relaxed);
    }
    /// Resize the shared budget (all collections share it).
    pub fn resize(&self, capacity: usize) {
        self.inner.resize(capacity);
    }
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }
    pub fn len(&self) -> usize {
        self.inner.len()
    }
    pub fn is_empty(&self) -> bool {
        self.inner.len() == 0
    }
    pub fn stats(&self) -> CacheStats {
        self.inner.stats()
    }
    pub fn reset_stats(&self) {
        self.inner.reset_stats();
    }
}

impl Default for DocCache {
    fn default() -> Self {
        Self::new(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // These exercise the shared storage (`Shared`) directly with composite keys
    // so each test gets an isolated instance (the facade `DocCache` shares one
    // process-global cache). `shard_for` uses the low 64 bits, so a `u128` key
    // whose value equals a small id shards exactly like the old id did.

    #[test]
    fn basic_put_and_get() {
        let cache = Shared::new(10);
        let doc = Arc::new(json!({"name": "test"}));
        cache.put(1, doc.clone());
        assert_eq!(cache.get(1).unwrap(), doc);
    }

    #[test]
    fn miss_returns_none() {
        let cache = Shared::new(10);
        assert!(cache.get(42).is_none());
    }

    #[test]
    fn eviction_at_capacity() {
        let cache = Shared::new(NUM_SHARDS * 2); // 2 per shard
        cache.put(0, Arc::new(json!(0)));
        cache.put(16, Arc::new(json!(16)));
        cache.put(32, Arc::new(json!(32))); // evicts 0 in shard 0
        assert!(cache.get(0).is_none());
        assert!(cache.get(16).is_some());
        assert!(cache.get(32).is_some());
    }

    #[test]
    fn lru_order_respected() {
        let cache = Shared::new(NUM_SHARDS * 2); // 2 per shard
        cache.put(0, Arc::new(json!(0)));
        cache.put(16, Arc::new(json!(16)));
        cache.get(0); // make 0 recently used
        cache.put(32, Arc::new(json!(32))); // evicts 16 (LRU in shard 0)
        assert!(cache.get(0).is_some());
        assert!(cache.get(16).is_none());
        assert!(cache.get(32).is_some());
    }

    #[test]
    fn remove_and_clear() {
        let cache = Shared::new(160);
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
        let cache = Shared::new(160);
        for i in 0..160u128 {
            cache.put(i, Arc::new(json!(i)));
        }
        assert_eq!(cache.len(), 160);
        cache.resize(NUM_SHARDS * 3);
        assert!(cache.len() <= NUM_SHARDS * 3);
    }

    #[test]
    fn get_many_works() {
        let cache = Shared::new(160);
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
        let cache = Shared::new(160);
        let allocated = cache.shards.iter().filter(|s| s.lock().is_some()).count();
        assert_eq!(allocated, 0, "no shard should be allocated yet");
        cache.put(0, Arc::new(json!(0)));
        let after_put = cache.shards.iter().filter(|s| s.lock().is_some()).count();
        assert_eq!(after_put, 1, "only the touched shard should allocate");
    }

    #[test]
    fn put_many_works() {
        let cache = Shared::new(160);
        let entries = vec![
            (1u128, Arc::new(json!(1))),
            (2, Arc::new(json!(2))),
            (3, Arc::new(json!(3))),
        ];
        cache.put_many(entries);
        assert!(cache.get(1).is_some());
        assert!(cache.get(2).is_some());
        assert!(cache.get(3).is_some());
    }

    #[test]
    fn namespacing_keeps_collections_distinct() {
        // Two facades over the shared global: the same document id must not
        // collide across collections, and clearing one bumps its namespace.
        let a = DocCache::new(0);
        let b = DocCache::new(0);
        a.put(7, Arc::new(json!("a")));
        b.put(7, Arc::new(json!("b")));
        assert_eq!(*a.get(7).unwrap(), json!("a"));
        assert_eq!(*b.get(7).unwrap(), json!("b"));
        a.clear(); // fresh namespace → a's old entry is now unreachable
        assert!(a.get(7).is_none());
        assert_eq!(*b.get(7).unwrap(), json!("b")); // b untouched
    }
}
