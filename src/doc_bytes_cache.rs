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
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use lru::LruCache;

use crate::document::DocumentId;
use crate::locks::Mutex;

/// Hard-coded fallback. Larger than `DocCache::DEFAULT_CAPACITY` because
/// each entry is 7× smaller — 1M entries fit in ~500 MB at typical doc
/// sizes, comfortably less than the Value-cache footprint at its 100K cap.
const DEFAULT_BYTES_CAPACITY_FALLBACK: usize = 1_000_000;

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

#[inline]
fn shard_for(id: DocumentId) -> usize {
    (id & SHARD_MASK) as usize
}

pub struct DocBytesCache {
    per_shard_cap: AtomicUsize,
    shards: Vec<Mutex<Option<LruCache<DocumentId, Arc<[u8]>>>>>,
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

impl DocBytesCache {
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

    pub fn get(&self, id: DocumentId) -> Option<Arc<[u8]>> {
        let mut shard = self.shards[shard_for(id)].lock();
        let found = match shard.as_mut() {
            Some(cache) => cache.get(&id).cloned(),
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

    pub fn put(&self, id: DocumentId, bytes: Arc<[u8]>) {
        let cap = self.per_shard_cap();
        let mut shard = self.shards[shard_for(id)].lock();
        match shard.as_mut() {
            Some(cache) => {
                cache.put(id, bytes);
            }
            None => {
                let mut cache = LruCache::new(cap);
                cache.put(id, bytes);
                *shard = Some(cache);
            }
        }
    }

    pub fn remove(&self, id: DocumentId) {
        let mut shard = self.shards[shard_for(id)].lock();
        if let Some(cache) = shard.as_mut() {
            cache.pop(&id);
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
        let cache = DocBytesCache::new(32);
        let bytes: Arc<[u8]> = Arc::from(vec![1u8]);
        cache.put(1, Arc::clone(&bytes));
        let _ = cache.get(1); // hit
        let _ = cache.get(2); // miss
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
