//! Per-collection cache of **pre-encoded OxiWire/MsgPack bytes** keyed by
//! `DocumentId`. Sibling of [`DocCache`](crate::doc_cache::DocCache) — same
//! shard-based LRU machinery, but stores `Arc<[u8]>` (the wire-output bytes
//! for one document) instead of `Arc<Value>`.
//!
//! Why a second cache: a `serde_json::Value` allocation for a typical
//! ~500-byte document is ~3.5 KB on the heap (HashMap nesting, per-String
//! boxes, etc.). Pre-encoded bytes are ~500 B — 7× smaller. For workloads
//! that ship docs to the wire (the common case), the engine can skip the
//! JSONB→Value→encode round-trip on every cache miss.
//!
//! Populated lazily: a doc enters this cache when (a) it is encoded for a
//! wire response, or (b) it is freshly inserted (encode-once-at-write).
//! The first call site avoids cold-start regression; the second amortises
//! the cost across one of the writer's batches.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use lru::LruCache;

use crate::document::DocumentId;
use crate::locks::Mutex;

/// Memory budget for the encoded-bytes (wire) cache, and the per-entry size
/// used to derive a bounded entry count from it.
///
/// Like the `Value` cache, this is a speed layer over an already-resident
/// store: a miss costs a JSONB→Value→OxiWire transcode (CPU), not I/O. The old
/// default of 1,000,000 entries was sized to cache an *entire* 1M-doc
/// collection (~500 MB) — it scaled with the dataset and was the single
/// largest tunable contributor to RSS in the 1M-doc benchmark. We instead
/// bound it to a fixed budget. Each entry is the OxiWire encoding of one doc
/// (~500 B payload) plus `Arc<[u8]>` + LRU node overhead (~768 B all-in), so
/// the default budget yields ~170K entries. Tunable via
/// `OXIDB_DOC_BYTES_CACHE_SIZE`.
const BYTES_CACHE_BUDGET_BYTES: usize = 128 * 1024 * 1024; // 128 MiB
const APPROX_BYTES_ENTRY: usize = 768;
const DEFAULT_BYTES_CAPACITY_FALLBACK: usize = BYTES_CACHE_BUDGET_BYTES / APPROX_BYTES_ENTRY;

const NUM_SHARDS: usize = 16;
const SHARD_MASK: u64 = (NUM_SHARDS as u64) - 1;

/// Read the bytes cache capacity from `OXIDB_DOC_BYTES_CACHE_SIZE`, cached
/// on the first call. Returns the fallback if absent or unparseable.
pub fn default_capacity() -> usize {
    use std::sync::OnceLock;
    static CACHED: OnceLock<usize> = OnceLock::new();
    *CACHED.get_or_init(|| {
        std::env::var("OXIDB_DOC_BYTES_CACHE_SIZE")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n >= NUM_SHARDS)
            .unwrap_or(DEFAULT_BYTES_CAPACITY_FALLBACK)
    })
}

use crate::doc_cache::{CacheKey, compose, next_ns};

#[inline]
fn shard_for(key: CacheKey) -> usize {
    ((key as u64) & SHARD_MASK) as usize
}

/// The single process-global encoded-bytes cache, shared by every collection
/// under one memory budget (was per-collection, so total RSS scaled with the
/// collection count).
pub fn global() -> Arc<SharedBytes> {
    use std::sync::OnceLock;
    static G: OnceLock<Arc<SharedBytes>> = OnceLock::new();
    Arc::clone(G.get_or_init(|| Arc::new(SharedBytes::new(default_capacity()))))
}

pub struct SharedBytes {
    per_shard_cap: AtomicUsize,
    shards: Vec<Mutex<Option<LruCache<CacheKey, Arc<[u8]>>>>>,
    hits: AtomicU64,
    misses: AtomicU64,
}

#[derive(Debug, Clone, Copy)]
pub struct BytesCacheStats {
    pub hits: u64,
    pub misses: u64,
}

impl BytesCacheStats {
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

impl SharedBytes {
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

    pub fn get(&self, key: CacheKey) -> Option<Arc<[u8]>> {
        let mut shard = self.shards[shard_for(key)].lock();
        let found = match shard.as_mut() {
            Some(cache) => cache.get(&key).cloned(),
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

    pub fn put(&self, key: CacheKey, bytes: Arc<[u8]>) {
        let cap = self.per_shard_cap();
        let mut shard = self.shards[shard_for(key)].lock();
        match shard.as_mut() {
            Some(cache) => {
                cache.put(key, bytes);
            }
            None => {
                let mut cache = LruCache::new(cap);
                cache.put(key, bytes);
                *shard = Some(cache);
            }
        }
    }

    pub fn remove(&self, key: CacheKey) {
        let mut shard = self.shards[shard_for(key)].lock();
        if let Some(cache) = shard.as_mut() {
            cache.pop(&key);
        }
    }

    pub fn clear(&self) {
        for shard in &self.shards {
            let mut s = shard.lock();
            *s = None;
        }
    }

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

    pub fn stats(&self) -> BytesCacheStats {
        BytesCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
        }
    }
}

/// Per-collection facade over the shared global [`DocBytesCache`] — namespaces
/// keys so all collections share one budget. `BTreeCollection`-facing API is
/// unchanged (`DocumentId`-based).
pub struct DocBytesCache {
    inner: Arc<SharedBytes>,
    ns: AtomicU64,
}

impl DocBytesCache {
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
    pub fn get(&self, id: DocumentId) -> Option<Arc<[u8]>> {
        self.inner.get(self.key(id))
    }
    pub fn put(&self, id: DocumentId, bytes: Arc<[u8]>) {
        self.inner.put(self.key(id), bytes);
    }
    pub fn remove(&self, id: DocumentId) {
        self.inner.remove(self.key(id));
    }
    /// Invalidate this collection's entries via a fresh namespace (LRU-evicted).
    pub fn clear(&self) {
        self.ns.store(next_ns(), Ordering::Relaxed);
    }
    pub fn resize(&self, capacity: usize) {
        self.inner.resize(capacity);
    }
    pub fn stats(&self) -> BytesCacheStats {
        self.inner.stats()
    }
}

impl Default for DocBytesCache {
    fn default() -> Self {
        Self::new(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get_roundtrip() {
        let cache = DocBytesCache::new(32);
        let bytes: Arc<[u8]> = Arc::from(vec![1u8, 2, 3]);
        cache.put(42, Arc::clone(&bytes));
        let got = cache.get(42).expect("hit");
        assert_eq!(&*got, &[1u8, 2, 3]);
    }

    #[test]
    fn miss_returns_none() {
        let cache = DocBytesCache::new(32);
        assert!(cache.get(9999).is_none());
    }

    #[test]
    fn stats_track_hits_and_misses() {
        // Use the isolated shared storage directly — the facade's stats() reads
        // the process-global cache, which other tests share.
        let cache = SharedBytes::new(32);
        let bytes: Arc<[u8]> = Arc::from(vec![1u8]);
        cache.put(1u128, Arc::clone(&bytes));
        let _ = cache.get(1u128); // hit
        let _ = cache.get(2u128); // miss
        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
    }

    #[test]
    fn env_var_capacity_falls_back_when_unset() {
        // default_capacity() uses OnceLock so first call wins for the
        // process; this test just checks the fallback path exists.
        assert!(default_capacity() >= NUM_SHARDS);
    }
}
