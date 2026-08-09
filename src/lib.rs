pub mod locks;

#[cfg(not(target_arch = "wasm32"))]
pub mod archive;
#[cfg(not(target_arch = "wasm32"))]
pub mod blob;
#[cfg(not(target_arch = "wasm32"))]
pub mod btree;
pub mod btree_collection;
pub mod btree_storage;
pub mod change_stream;
pub mod codec;
pub mod collection;
pub mod crypto;
pub mod database_manager;
pub mod doc_bytes_cache;
pub mod doc_cache;
pub(crate) mod doc_locks;
pub mod jsonb_oxiwire;
pub mod wire_oxiwire;

pub mod alerting;
pub mod document;
pub mod engine;
pub mod error;
pub mod fts;
pub mod mmap_text_index;
pub mod geo;
#[cfg(feature = "gpu")]
pub mod gpu;
pub mod in_memory;
pub mod index;
#[cfg(not(target_arch = "wasm32"))]
pub mod index_bundle;
#[cfg(not(target_arch = "wasm32"))]
pub mod index_persist;
pub mod links;
#[cfg(not(target_arch = "wasm32"))]
pub mod mmap_composite_index;
#[cfg(not(target_arch = "wasm32"))]
pub mod mmap_field_index;
#[cfg(not(target_arch = "wasm32"))]
pub mod mmap_index;
pub mod oxiscript;
pub mod paged_field_index;
pub mod pipeline;
#[cfg(not(target_arch = "wasm32"))]
pub mod pitr;
pub mod procedure;
pub mod query;
pub mod scheduler;
pub mod snapshot;
pub mod storage;
#[cfg(not(target_arch = "wasm32"))]
pub mod stripe;
pub mod transaction;
#[cfg(not(target_arch = "wasm32"))]
pub mod tx_log;
pub mod update;
pub mod value;
pub mod vector;
pub mod wal;
pub mod worm;

pub use btree_storage::StorageOptions;
pub use change_stream::{
    ChangeEvent, ChangeStreamBroker, OperationType, ResumeError, SubscriberId, WatchFilter,
    WatchHandle,
};
pub use collection::{Collection, CompactStats, IndexInfo};
pub use crypto::EncryptionKey;
pub use database_manager::DatabaseManager;
pub use document::DocumentId;
pub use engine::{BackupInfo, LogCallback, OxiDb, PitrRestoreInfo, RestoreInfo};
pub use error::{Error, Result};
#[cfg(not(target_arch = "wasm32"))]
pub use pitr::PitrTarget;
pub use transaction::TransactionId;
pub use vector::DistanceMetric;
