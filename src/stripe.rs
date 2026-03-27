//! Stripe-level locking for document data.
//!
//! Documents are distributed across N stripes by `doc_id % NUM_STRIPES`.
//! Each stripe independently holds a primary index, version index, and doc cache
//! segment. This allows concurrent writes to different documents without contention.

use std::collections::HashMap;

use crate::doc_cache::DocCache;
use crate::document::DocumentId;
use crate::storage::DocLocation;

/// Number of stripes. Must be a power of 2 for fast modulo.
pub const NUM_STRIPES: usize = 16;

/// A single stripe holding a subset of the document data.
pub struct Stripe {
    pub primary_index: HashMap<DocumentId, DocLocation>,
    pub version_index: HashMap<DocumentId, u64>,
    pub doc_cache: DocCache,
}

impl Stripe {
    /// Create a new empty stripe with the given per-stripe cache capacity.
    pub fn new(cache_capacity: usize) -> Self {
        Self {
            primary_index: HashMap::new(),
            version_index: HashMap::new(),
            doc_cache: DocCache::new(cache_capacity.max(1)),
        }
    }

    /// Determine which stripe a document belongs to.
    #[inline]
    pub fn stripe_for(doc_id: DocumentId) -> usize {
        doc_id as usize % NUM_STRIPES
    }
}
