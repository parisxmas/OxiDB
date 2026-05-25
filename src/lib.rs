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
pub mod doc_cache;
pub mod doc_bytes_cache;
pub mod jsonb_oxiwire;
pub mod wire_oxiwire;
pub mod crypto;
pub mod database_manager;

#[cfg(feature = "gpu")]
pub mod gpu;
pub mod document;
pub mod engine;
pub mod error;
pub mod fts;
pub mod index;
pub mod links;
#[cfg(not(target_arch = "wasm32"))]
pub mod index_bundle;
#[cfg(not(target_arch = "wasm32"))]
pub mod index_persist;
#[cfg(not(target_arch = "wasm32"))]
pub mod mmap_field_index;
#[cfg(not(target_arch = "wasm32"))]
pub mod mmap_index;
pub mod oxiscript;
pub mod paged_field_index;
#[cfg(not(target_arch = "wasm32"))]
pub mod pitr;
pub mod alerting;
pub mod pipeline;
pub mod procedure;
pub mod query;
pub mod scheduler;
pub mod sql;
#[cfg(not(target_arch = "wasm32"))]
pub mod stripe;
pub mod transaction;
#[cfg(not(target_arch = "wasm32"))]
pub mod tx_log;
pub mod update;
pub mod in_memory;
pub mod storage;
pub mod value;
pub mod vector;
pub mod wal;
pub mod worm;

pub use change_stream::{ChangeEvent, ChangeStreamBroker, OperationType, ResumeError, SubscriberId, WatchFilter, WatchHandle};
pub use collection::{Collection, CompactStats, IndexInfo};
pub use crypto::EncryptionKey;
pub use database_manager::DatabaseManager;
pub use document::DocumentId;
pub use engine::{BackupInfo, LogCallback, OxiDb, PitrRestoreInfo, RestoreInfo};
#[cfg(not(target_arch = "wasm32"))]
pub use pitr::PitrTarget;
pub use error::{Error, Result};
pub use sql::{execute_sql, execute_sql_with_db_manager, execute_sql_with_dialect, SqlDialect, SqlResult};
#[cfg(not(target_arch = "wasm32"))]
pub use tx_log::TransactionId;
#[cfg(target_arch = "wasm32")]
pub type TransactionId = u64;
pub use vector::DistanceMetric;
