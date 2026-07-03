use std::io::Cursor;

use openraft::BasicNode;
use serde::{Deserialize, Serialize};
use serde_json::Value;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = OxiDbRequest,
        R = OxiDbResponse,
        NodeId = u64,
        Node = BasicNode,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
);

/// Type alias for the Raft instance used throughout the server.
pub type OxiRaft = openraft::Raft<TypeConfig>;

/// Write requests replicated through Raft consensus.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OxiDbRequest {
    Insert {
        collection: String,
        document: Value,
    },
    InsertMany {
        collection: String,
        documents: Vec<Value>,
    },
    Update {
        collection: String,
        query: Value,
        update: Value,
    },
    UpdateOne {
        collection: String,
        query: Value,
        update: Value,
    },
    Delete {
        collection: String,
        query: Value,
    },
    DeleteOne {
        collection: String,
        query: Value,
    },
    CreateCollection {
        name: String,
    },
    CreateCollectionWithOptions {
        name: String,
        options: oxidb::StorageOptions,
    },
    /// A write statement (or batch) for the standalone SQL engine (ADR-0010).
    /// Applied by re-executing the SQL string with its params on every node —
    /// deterministic because the engine has no non-deterministic functions and
    /// row ids are assigned sequentially. Requires `OXIDB_SQL=1` on all nodes.
    Sql {
        sql: String,
        /// JSON array of bind parameters, or `Null` when none were given.
        params: Value,
    },
    DropCollection {
        name: String,
    },
    Compact {
        collection: String,
    },
    CreateIndex {
        collection: String,
        field: String,
    },
    CreateUniqueIndex {
        collection: String,
        field: String,
    },
    CreateCompositeIndex {
        collection: String,
        fields: Vec<String>,
    },
    CreateTextIndex {
        collection: String,
        fields: Vec<String>,
    },
    DropIndex {
        collection: String,
        index: String,
    },
    CreateBucket {
        bucket: String,
    },
    DeleteBucket {
        bucket: String,
    },
    PutObject {
        bucket: String,
        key: String,
        data_b64: String,
        content_type: String,
        metadata: Value,
    },
    DeleteObject {
        bucket: String,
        key: String,
    },
    /// Atomic transaction commit — all buffered writes applied as one Raft entry.
    CommitTransaction {
        write_ops: Vec<TransactionWriteOp>,
    },
    /// Create a database on every node (ADR-0012).
    CreateDatabase {
        name: String,
        /// `IF NOT EXISTS`: an already-existing database is success.
        #[serde(default)]
        if_not_exists: bool,
    },
    /// Drop a database on every node (ADR-0012).
    DropDatabase {
        name: String,
        /// `IF EXISTS`: a missing database is success.
        #[serde(default)]
        if_exists: bool,
    },
    /// A write scoped to a named database (ADR-0012). Requests without this
    /// wrapper — including every pre-0.32.2 log entry — apply to the default
    /// database, so old logs replay unchanged.
    Scoped {
        db: String,
        inner: Box<OxiDbRequest>,
    },
}

/// A single write operation from a committed transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransactionWriteOp {
    Insert {
        collection: String,
        document: Value,
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

/// Response from applying a write request through the state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OxiDbResponse {
    Ok { data: Value },
    Error { message: String },
}
