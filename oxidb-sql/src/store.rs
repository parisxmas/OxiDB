//! The storage abstraction the executor runs against.
//!
//! Two things implement [`Store`]: the [`SqlEngine`](crate::SqlEngine) itself
//! (autocommit — every call is applied and logged immediately) and a
//! [`Transaction`](crate::transaction::Transaction) (buffered — calls accumulate
//! in an overlay and are flushed atomically on commit). Making the executor
//! generic over `Store` means the exact same query/DML code runs in both modes.

use crate::catalog::Table;
use crate::error::Result;
use crate::types::Value;

/// A set of rows as `(row_id, cells)` pairs.
pub(crate) type Rows = Vec<(u64, Vec<Value>)>;

/// Read/write operations the executor needs. All methods take `&self`; buffered
/// implementations use interior mutability.
pub(crate) trait Store {
    fn table_def(&self, name: &str) -> Option<Table>;
    fn scan(&self, table: &str) -> Result<Vec<(u64, Vec<Value>)>>;
    fn insert(&self, table: &str, cells: Vec<Value>) -> Result<u64>;
    fn update_row(&self, table: &str, row_id: u64, cells: Vec<Value>) -> Result<()>;
    fn delete(&self, table: &str, row_id: u64) -> Result<bool>;
    fn create_table(&self, table: Table) -> Result<()>;
    fn drop_table(&self, name: &str) -> Result<()>;
    fn create_index(&self, name: &str, table: &str, column: &str) -> Result<()>;
    fn drop_index(&self, name: &str) -> Result<()>;

    /// Look up rows where an indexed `column` equals `value`.
    ///
    /// Returns `Ok(None)` when no index covers `(table, column)` (the caller
    /// should fall back to a full scan); `Ok(Some(rows))` — possibly empty —
    /// when an index served the lookup.
    fn index_lookup_eq(&self, table: &str, column: &str, value: &Value) -> Result<Option<Rows>>;
}
