//! The storage abstraction the executor runs against.
//!
//! Two things implement [`Store`]: the [`SqlEngine`](crate::SqlEngine) itself
//! (autocommit — every call is applied and logged immediately) and a
//! [`Transaction`](crate::transaction::Transaction) (buffered — calls accumulate
//! in an overlay and are flushed atomically on commit). Making the executor
//! generic over `Store` means the exact same query/DML code runs in both modes.

use crate::catalog::{IndexDef, Table};
use crate::error::Result;
use crate::types::Value;

/// A set of rows as `(row_id, cells)` pairs.
pub(crate) type Rows = Vec<(u64, Vec<Value>)>;

/// A column-pruned table scan in one flat allocation: row `i` occupies
/// `cells[i*width .. (i+1)*width]`. Avoids the per-row `Vec` of a
/// [`Rows`]-shaped scan, which dominates large-join query time.
pub(crate) struct Chunk {
    pub width: usize,
    pub n: usize,
    pub cells: Vec<Value>,
}

impl Chunk {
    /// Row `i` as a cell slice.
    #[inline]
    pub fn row(&self, i: usize) -> &[Value] {
        &self.cells[i * self.width..(i + 1) * self.width]
    }

    /// Build from full rows, keeping only the columns in `keep` (moving the
    /// kept cells out of each row).
    pub fn from_rows<I: IntoIterator<Item = Vec<Value>>>(rows: I, keep: &[usize]) -> Chunk {
        let width = keep.len();
        let mut n = 0;
        let mut cells = Vec::new();
        for mut row in rows {
            n += 1;
            for &k in keep {
                cells.push(std::mem::replace(&mut row[k], Value::Null));
            }
        }
        Chunk { width, n, cells }
    }
}

/// Read/write operations the executor needs. All methods take `&self`; buffered
/// implementations use interior mutability.
pub(crate) trait Store {
    fn table_def(&self, name: &str) -> Option<Table>;
    fn scan(&self, table: &str) -> Result<Vec<(u64, Vec<Value>)>>;
    /// Scan a table into a flat [`Chunk`], keeping only the columns in `keep`
    /// (indices into the table's column order). Implementations can override
    /// this to skip the per-row allocations of [`scan`](Store::scan).
    fn scan_pruned(&self, table: &str, keep: &[usize]) -> Result<Chunk> {
        Ok(Chunk::from_rows(
            self.scan(table)?.into_iter().map(|(_, c)| c),
            keep,
        ))
    }
    fn insert(&self, table: &str, cells: Vec<Value>) -> Result<u64>;
    /// Insert many rows as one durable unit (a single WAL fsync where the
    /// implementation supports it). Returns the number of rows inserted.
    fn insert_many(&self, table: &str, rows: Vec<Vec<Value>>) -> Result<u64> {
        let mut n = 0;
        for cells in rows {
            self.insert(table, cells)?;
            n += 1;
        }
        Ok(n)
    }
    fn update_row(&self, table: &str, row_id: u64, cells: Vec<Value>) -> Result<()>;
    fn delete(&self, table: &str, row_id: u64) -> Result<bool>;
    fn create_table(&self, table: Table) -> Result<()>;
    fn drop_table(&self, name: &str) -> Result<()>;
    /// `ALTER TABLE` — one operation (autocommit only in v1).
    fn alter_table(&self, table: &str, op: &crate::ast::AlterOp) -> Result<()>;
    fn create_index(&self, name: &str, table: &str, columns: &[String]) -> Result<()>;
    fn drop_index(&self, name: &str) -> Result<()>;
    fn create_view(&self, name: &str, query_sql: &str, or_replace: bool) -> Result<()>;
    fn drop_view(&self, name: &str) -> Result<()>;
    /// The stored SQL text of a view, if one with this name exists.
    fn view_sql(&self, name: &str) -> Option<String>;
    fn create_procedure(
        &self,
        name: &str,
        def: crate::catalog::ProcedureDef,
        or_alter: bool,
    ) -> Result<()>;
    fn drop_procedure(&self, name: &str) -> Result<()>;
    /// A stored procedure's definition, if one with this name exists.
    fn procedure_def(&self, name: &str) -> Option<crate::catalog::ProcedureDef>;

    /// Atomically reserve `n` auto-increment values for `table`, returning
    /// the first. Only called for tables with an AUTO_INCREMENT column.
    fn next_auto_block(&self, table: &str, n: i64) -> Result<i64>;

    /// Introspection (`SHOW TABLES` / `DESCRIBE`): all table definitions,
    /// sorted by name.
    fn list_tables(&self) -> Vec<Table>;
    /// Introspection (`SHOW VIEWS`): all `(name, body SQL)` pairs, sorted by
    /// name.
    fn list_views(&self) -> Vec<(String, String)>;
    /// Introspection (`SHOW PROCEDURES`): all `(name, def)` pairs, sorted by
    /// name.
    fn list_procedures(&self) -> Vec<(String, crate::catalog::ProcedureDef)>;
    /// Introspection (`SHOW INDEXES`): all secondary index definitions,
    /// sorted by index name.
    fn list_indexes(&self) -> Vec<IndexDef>;

    /// A cheap cardinality estimate for join planning. `None` when unknown
    /// (views, buffered transactions) — the planner then keeps written order.
    fn row_count_hint(&self, _table: &str) -> Option<usize> {
        None
    }

    /// Look up rows using a secondary index, given the available
    /// `column = value` equality pairs from the WHERE clause.
    ///
    /// Returns `Ok(None)` when no index has *all* of its columns among `eqs`
    /// (the caller should fall back to a full scan); `Ok(Some(rows))` —
    /// possibly empty — when an index served the lookup.
    fn index_lookup_eq(&self, table: &str, eqs: &[(String, Value)]) -> Result<Option<Rows>>;
}
