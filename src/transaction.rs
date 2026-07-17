use std::collections::BTreeSet;

use serde_json::Value;

use crate::document::DocumentId;

/// Identifies a transaction. It lives here rather than in [`crate::tx_log`]
/// because that module is native-only (files, threads) while transactions
/// themselves are not: on wasm32 there is no commit log, but there are still
/// transactions with ids. `tx_log` re-exports it, so `tx_log::TransactionId`
/// keeps working.
pub type TransactionId = u64;

/// A record of a document read during a transaction, used for OCC validation.
pub struct ReadRecord {
    pub collection: String,
    pub doc_id: DocumentId,
    pub version: u64,
}

/// A buffered write operation within a transaction.
pub enum WriteOp {
    /// `id` is pre-allocated at `tx_insert` time so the caller knows the
    /// assigned _id before commit (needed for cross-collection foreign
    /// keys: e.g. inserting a parent doc and its child rows in one tx
    /// where the child carries the parent's id). Falls back to None for
    /// legacy callers; commit-time prepare assigns one then.
    Insert {
        collection: String,
        data: Value,
        id: Option<DocumentId>,
    },
    Update {
        collection: String,
        query: Value,
        update: Value,
    },
    Delete {
        collection: String,
        query: Value,
    },
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
