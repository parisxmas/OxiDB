//! Error type for the standalone SQL engine.

use std::io;

/// Errors surfaced by the SQL engine.
#[derive(Debug, thiserror::Error)]
pub enum SqlError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    /// A `SELECT ... FOR UPDATE` / UPDATE / DELETE waited on a row lock held
    /// by another transaction past the lock timeout
    /// (`OXIDB_SQL_LOCK_TIMEOUT_MS`). Also how a deadlock between two
    /// transactions resolves: one side times out and aborts.
    #[error("lock timeout: row {row_id} of '{table}' is locked by another transaction")]
    LockTimeout { table: String, row_id: u64 },

    /// The on-disk catalog or a data/WAL file is structurally invalid.
    #[error("corrupt storage: {0}")]
    Corrupt(String),

    /// A referenced table does not exist.
    #[error("no such table: {0}")]
    NoSuchTable(String),

    /// A table with this name already exists.
    #[error("table already exists: {0}")]
    TableExists(String),

    /// The database has reached its configured table cap (OxiBase per-project
    /// quota). `0` = unlimited, so this is only ever raised when a cap is set.
    #[error("table limit reached ({0}) — cannot create another table")]
    TableLimitExceeded(usize),

    /// An index with this name already exists.
    #[error("index already exists: {0}")]
    IndexExists(String),

    /// A referenced index does not exist.
    #[error("no such index: {0}")]
    NoSuchIndex(String),

    /// A row's cell count or a cell's type does not match the table schema.
    #[error("schema mismatch: {0}")]
    SchemaMismatch(String),

    /// A string value is longer than its column's declared `VARCHAR(n)` /
    /// `CHAR(n)` length (Postgres SQLSTATE 22001).
    #[error(
        "value too long for column {column:?}: {got} characters exceeds the {max}-character limit"
    )]
    ValueTooLong {
        column: String,
        max: u32,
        got: usize,
    },

    /// An integer is outside the range its column's declared width can hold —
    /// `SMALLINT`, `INT` (Postgres SQLSTATE 22003). Every integer is stored as
    /// an i64; this is the declared width being enforced rather than the value
    /// being silently widened.
    #[error(
        "value {value} is out of range for column {column:?} declared {type_name} ({min}..={max})"
    )]
    IntegerOutOfRange {
        column: String,
        type_name: &'static str,
        value: i64,
        min: i64,
        max: i64,
    },

    /// The SQL text could not be parsed.
    #[error("sql parse error: {0}")]
    Parse(String),

    /// The SQL is valid but uses a feature this engine does not (yet) support.
    #[error("unsupported sql: {0}")]
    Unsupported(String),

    /// A referenced column does not exist in the table.
    #[error("no such column: {0}")]
    NoSuchColumn(String),

    /// An expression could not be evaluated (type error, etc.).
    #[error("evaluation error: {0}")]
    Eval(String),

    /// A write would violate PRIMARY KEY uniqueness.
    #[error("duplicate key: {0}")]
    DuplicateKey(String),

    /// A write would violate a FOREIGN KEY constraint (a child row with no
    /// parent, or deleting/updating a parent that children still reference).
    #[error("foreign key violation: {0}")]
    ForeignKeyViolation(String),

    /// A referenced view does not exist.
    #[error("no such view: {0}")]
    NoSuchView(String),

    /// A called (or dropped) stored procedure does not exist.
    #[error("no such procedure: {0}")]
    NoSuchProcedure(String),
}

/// Convenience result alias for the SQL engine.
pub type Result<T> = std::result::Result<T, SqlError>;
