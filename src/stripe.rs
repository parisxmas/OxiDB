//! Stripe-level locking for document cache segments.
//!
//! Documents are distributed across N stripes by `doc_id % NUM_STRIPES`.
//! Each stripe holds a doc cache segment. The primary index and version index
//! are handled by `MmapPrimaryIndex` at the collection level.

use crate::doc_cache::DocCache;
use crate::document::DocumentId;

/// Number of stripes. Must be a power of 2 for fast modulo.
pub const NUM_STRIPES: usize = 16;

/// A single stripe holding a doc cache segment.
pub struct Stripe {
    pub doc_cache: DocCache,
}

impl Stripe {
    /// Create a new empty stripe with the given per-stripe cache capacity.
    pub fn new(cache_capacity: usize) -> Self {
        Self {
            doc_cache: DocCache::new(cache_capacity.max(1)),
        }
    }

    /// Determine which stripe a document belongs to.
    #[inline]
    pub fn stripe_for(doc_id: DocumentId) -> usize {
        doc_id as usize % NUM_STRIPES
    }
}
