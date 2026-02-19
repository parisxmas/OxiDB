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
}

/// Response from applying a write request through the state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OxiDbResponse {
    Ok { data: Value },
    Error { message: String },
}
