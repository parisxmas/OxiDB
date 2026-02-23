use std::collections::BTreeSet;

use serde_json::Value;

use crate::document::DocumentId;
use crate::tx_log::TransactionId;

/// A record of a document read during a transaction, used for OCC validation.
pub struct ReadRecord {
    pub collection: String,
    pub doc_id: DocumentId,
    pub version: u64,
}

/// A buffered write operation within a transaction.
pub enum WriteOp {
    Insert { collection: String, data: Value },
    Update { collection: String, query: Value, update: Value },
    Delete { collection: String, query: Value },
}

/// An active transaction holding its read set, write set, and involved collections.
pub struct Transaction {
    pub id: TransactionId,
    pub read_set: Vec<ReadRecord>,
    pub write_ops: Vec<WriteOp>,
    /// BTreeSet for sorted lock acquisition (deadlock-free ordering).
    pub collections_involved: BTreeSet<String>,
}

impl Transaction {
    pub fn new(id: TransactionId) -> Self {
        Self {
            id,
            read_set: Vec::new(),
            write_ops: Vec::new(),
            collections_involved: BTreeSet::new(),
        }
    }
}
