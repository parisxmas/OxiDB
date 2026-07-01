//! Error type for the standalone SQL engine.

use std::io;

/// Errors surfaced by the SQL engine.
#[derive(Debug, thiserror::Error)]
pub enum SqlError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    /// The on-disk catalog or a data/WAL file is structurally invalid.
    #[error("corrupt storage: {0}")]
    Corrupt(String),

    /// A referenced table does not exist.
    #[error("no such table: {0}")]
    NoSuchTable(String),

    /// A table with this name already exists.
    #[error("table already exists: {0}")]
    TableExists(String),

    /// A row's cell count or a cell's type does not match the table schema.
    #[error("schema mismatch: {0}")]
    SchemaMismatch(String),
}

/// Convenience result alias for the SQL engine.
pub type Result<T> = std::result::Result<T, SqlError>;
