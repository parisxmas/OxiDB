//! Index bundle: all indexes for a collection, behind a single RwLock.
//!
//! Separating indexes from document stripes allows reads to query indexes
//! (shared lock) while a write is updating a stripe, and vice versa.

use std::collections::HashMap;

use crate::document::DocumentId;
use crate::fts::CollectionTextIndex;
use crate::index::{CompositeIndex, FieldIndex};
use crate::vector::VectorIndex;

/// All secondary indexes for a collection.
pub struct IndexBundle {
    pub field_indexes: HashMap<String, FieldIndex>,
    pub composite_indexes: Vec<CompositeIndex>,
    pub text_index: Option<CollectionTextIndex>,
    pub vector_indexes: HashMap<String, VectorIndex>,
    pub ttl_index: std::collections::BTreeMap<u64, Vec<DocumentId>>,
}

impl IndexBundle {
    /// Create an empty index bundle.
    pub fn new() -> Self {
        Self {
            field_indexes: HashMap::new(),
            composite_indexes: Vec::new(),
            text_index: None,
            vector_indexes: HashMap::new(),
            ttl_index: std::collections::BTreeMap::new(),
        }
    }
}
