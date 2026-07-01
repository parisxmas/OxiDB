//! # oxidb-sql
//!
//! A standalone SQL engine mounted alongside the OxiDB document engine
//! (see ADR-0010). It owns its own files, catalog, storage format, WAL, and
//! recovery, and shares no state with the document engine.
//!
//! The engine exposes both a programmatic row API (`create_table`, `insert`,
//! `scan`, …) and a SQL surface via [`SqlEngine::execute`] /
//! [`SqlEngine::execute_params`]: DDL, DML, single-table and inner-join SELECT
//! with aggregation, secondary indexes, parameterized queries, and per-engine
//! transactions (`BEGIN`/`COMMIT`/`ROLLBACK`).

mod ast;
mod catalog;
mod error;
mod executor;
mod parser;
mod storage;
mod store;
mod transaction;
mod types;
mod wal;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

pub use ast::QueryResult;
pub use catalog::{Column, Table};
pub use error::{Result, SqlError};
pub use types::{SqlType, Value};

use catalog::{Catalog, IndexDef};
use store::Store;
use transaction::Transaction;
use types::IndexKey;
use wal::{Wal, WalRecord};

/// An in-memory single-column secondary index: value -> set of row ids.
struct SecondaryIndex {
    col_pos: usize,
    map: BTreeMap<IndexKey, BTreeSet<u64>>,
}

/// Runtime state for one table: its definition, its live rows keyed by a dense
/// engine-assigned `row_id`, and any secondary indexes.
struct TableState {
    def: Table,
    rows: BTreeMap<u64, Vec<Value>>,
    next_row_id: u64,
    /// Secondary indexes keyed by index name.
    indexes: BTreeMap<String, SecondaryIndex>,
}

impl TableState {
    fn empty(def: Table) -> Self {
        TableState {
            def,
            rows: BTreeMap::new(),
            next_row_id: 1,
            indexes: BTreeMap::new(),
        }
    }

    fn observe_row_id(&mut self, row_id: u64) {
        if row_id >= self.next_row_id {
            self.next_row_id = row_id + 1;
        }
    }

    /// (Re)build a secondary index over `column` from the current rows.
    fn build_index(&mut self, index_name: &str, column: &str) -> Result<()> {
        let col_pos = self
            .def
            .columns
            .iter()
            .position(|c| c.name == column)
            .ok_or_else(|| SqlError::NoSuchColumn(column.to_string()))?;
        let mut map: BTreeMap<IndexKey, BTreeSet<u64>> = BTreeMap::new();
        for (rid, cells) in &self.rows {
            map.entry(IndexKey(cells[col_pos].clone()))
                .or_default()
                .insert(*rid);
        }
        self.indexes
            .insert(index_name.to_string(), SecondaryIndex { col_pos, map });
        Ok(())
    }

    fn index_insert(&mut self, row_id: u64, cells: &[Value]) {
        for idx in self.indexes.values_mut() {
            idx.map
                .entry(IndexKey(cells[idx.col_pos].clone()))
                .or_default()
                .insert(row_id);
        }
    }

    fn index_remove(&mut self, row_id: u64, cells: &[Value]) {
        for idx in self.indexes.values_mut() {
            let key = IndexKey(cells[idx.col_pos].clone());
            if let Some(set) = idx.map.get_mut(&key) {
                set.remove(&row_id);
                if set.is_empty() {
                    idx.map.remove(&key);
                }
            }
        }
    }
}

struct Inner {
    dir: PathBuf,
    catalog: Catalog,
    tables: BTreeMap<String, TableState>,
    wal: Wal,
}

/// The public SQL engine handle. Cheap to share behind an `Arc`.
pub struct SqlEngine {
    inner: Mutex<Inner>,
}

impl SqlEngine {
    /// Open (creating if needed) a SQL engine rooted at `dir` (e.g.
    /// `oxidb_data/sql`). Loads the catalog and `.rdat` snapshots, replays the
    /// WAL tail, then (re)builds secondary indexes.
    pub fn open(dir: impl AsRef<Path>) -> Result<SqlEngine> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        // 1. Durable schema snapshot.
        let mut catalog = Catalog::load(&dir)?;

        // 2. Load each table's row snapshot.
        let mut tables: BTreeMap<String, TableState> = BTreeMap::new();
        for (name, def) in &catalog.tables {
            let mut state = TableState::empty(def.clone());
            for (row_id, cells) in storage::read_snapshot(&dir, name, def.arity())? {
                state.observe_row_id(row_id);
                state.rows.insert(row_id, cells);
            }
            tables.insert(name.clone(), state);
        }

        // 3. Replay the WAL tail over the snapshots (idempotent).
        let (wal, records) = Wal::open(&dir)?;
        for rec in &records {
            Self::apply_live(&mut catalog, &mut tables, rec);
        }

        // 4. Build any indexes that existed before the last checkpoint (they are
        //    in the loaded catalog but not in the truncated WAL, so replay didn't
        //    reconstruct them).
        let defs: Vec<IndexDef> = catalog.indexes.values().cloned().collect();
        for def in defs {
            if let Some(state) = tables.get_mut(&def.table)
                && !state.indexes.contains_key(&def.name)
            {
                let _ = state.build_index(&def.name, &def.column);
            }
        }

        Ok(SqlEngine {
            inner: Mutex::new(Inner {
                dir,
                catalog,
                tables,
                wal,
            }),
        })
    }

    /// Apply a WAL record to the in-memory catalog + tables, maintaining
    /// secondary indexes and `next_row_id`. Idempotent by (table, row_id) so
    /// replay over a snapshot always converges. `Batch` is applied whole.
    fn apply_live(
        catalog: &mut Catalog,
        tables: &mut BTreeMap<String, TableState>,
        rec: &WalRecord,
    ) {
        match rec {
            WalRecord::CreateTable(def) => {
                catalog.tables.insert(def.name.clone(), def.clone());
                tables
                    .entry(def.name.clone())
                    .or_insert_with(|| TableState::empty(def.clone()));
            }
            WalRecord::DropTable(name) => {
                catalog.tables.remove(name);
                tables.remove(name);
                // Drop any indexes that belonged to the table.
                catalog.indexes.retain(|_, d| &d.table != name);
            }
            WalRecord::CreateIndex(def) => {
                catalog.indexes.insert(def.name.clone(), def.clone());
                if let Some(state) = tables.get_mut(&def.table) {
                    let _ = state.build_index(&def.name, &def.column);
                }
            }
            WalRecord::DropIndex(name) => {
                if let Some(def) = catalog.indexes.remove(name)
                    && let Some(state) = tables.get_mut(&def.table)
                {
                    state.indexes.remove(name);
                }
            }
            WalRecord::Insert {
                table,
                row_id,
                cells,
            } => {
                if let Some(state) = tables.get_mut(table) {
                    if let Some(old) = state.rows.get(row_id).cloned() {
                        state.index_remove(*row_id, &old);
                    }
                    state.observe_row_id(*row_id);
                    state.rows.insert(*row_id, cells.clone());
                    state.index_insert(*row_id, cells);
                }
            }
            WalRecord::Delete { table, row_id } => {
                if let Some(state) = tables.get_mut(table)
                    && let Some(old) = state.rows.remove(row_id)
                {
                    state.index_remove(*row_id, &old);
                }
            }
            WalRecord::Batch(ops) => {
                for op in ops {
                    Self::apply_live(catalog, tables, op);
                }
            }
        }
    }

    /// Create a new table. Errors if a table of the same name already exists.
    pub fn create_table(&self, def: Table) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        if inner.catalog.contains(&def.name) {
            return Err(SqlError::TableExists(def.name));
        }
        let rec = WalRecord::CreateTable(def);
        let inner = &mut *inner;
        inner.wal.append(&rec)?;
        Self::apply_live(&mut inner.catalog, &mut inner.tables, &rec);
        Ok(())
    }

    /// Drop a table and its rows. Errors if it does not exist.
    pub fn drop_table(&self, name: &str) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        if !inner.catalog.contains(name) {
            return Err(SqlError::NoSuchTable(name.to_string()));
        }
        let rec = WalRecord::DropTable(name.to_string());
        let inner = &mut *inner;
        inner.wal.append(&rec)?;
        Self::apply_live(&mut inner.catalog, &mut inner.tables, &rec);
        Ok(())
    }

    /// Create a single-column secondary index. Errors if the index name is
    /// taken or the table/column does not exist.
    pub fn create_index(&self, name: &str, table: &str, column: &str) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        if inner.catalog.indexes.contains_key(name) {
            return Err(SqlError::IndexExists(name.to_string()));
        }
        let def = inner
            .tables
            .get(table)
            .ok_or_else(|| SqlError::NoSuchTable(table.to_string()))?
            .def
            .clone();
        if !def.columns.iter().any(|c| c.name == column) {
            return Err(SqlError::NoSuchColumn(column.to_string()));
        }
        let rec = WalRecord::CreateIndex(IndexDef {
            name: name.to_string(),
            table: table.to_string(),
            column: column.to_string(),
        });
        let inner = &mut *inner;
        inner.wal.append(&rec)?;
        Self::apply_live(&mut inner.catalog, &mut inner.tables, &rec);
        Ok(())
    }

    /// Drop a secondary index by name. Errors if it does not exist.
    pub fn drop_index(&self, name: &str) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        if !inner.catalog.indexes.contains_key(name) {
            return Err(SqlError::NoSuchIndex(name.to_string()));
        }
        let rec = WalRecord::DropIndex(name.to_string());
        let inner = &mut *inner;
        inner.wal.append(&rec)?;
        Self::apply_live(&mut inner.catalog, &mut inner.tables, &rec);
        Ok(())
    }

    /// Insert a row, returning its assigned `row_id`. Validated against the
    /// schema (arity, types, nullability) before it is logged.
    pub fn insert(&self, table: &str, cells: Vec<Value>) -> Result<u64> {
        let mut inner = self.inner.lock().unwrap();
        let def = inner
            .tables
            .get(table)
            .ok_or_else(|| SqlError::NoSuchTable(table.to_string()))?
            .def
            .clone();
        def.validate_row(&cells)?;

        let row_id = inner.tables.get(table).expect("present").next_row_id;
        let rec = WalRecord::Insert {
            table: table.to_string(),
            row_id,
            cells,
        };
        let inner = &mut *inner;
        inner.wal.append(&rec)?;
        Self::apply_live(&mut inner.catalog, &mut inner.tables, &rec);
        Ok(row_id)
    }

    /// Insert many rows in one durable step: all rows are validated, logged as
    /// a single WAL `Batch` record (one fsync), then applied. Returns the
    /// number of rows inserted.
    pub fn insert_many(&self, table: &str, rows: Vec<Vec<Value>>) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }
        let mut inner = self.inner.lock().unwrap();
        let def = inner
            .tables
            .get(table)
            .ok_or_else(|| SqlError::NoSuchTable(table.to_string()))?
            .def
            .clone();
        for cells in &rows {
            def.validate_row(cells)?;
        }

        let first_id = inner.tables.get(table).expect("present").next_row_id;
        let n = rows.len() as u64;
        let ops: Vec<WalRecord> = rows
            .into_iter()
            .enumerate()
            .map(|(i, cells)| WalRecord::Insert {
                table: table.to_string(),
                row_id: first_id + i as u64,
                cells,
            })
            .collect();
        let rec = WalRecord::Batch(ops);
        let inner = &mut *inner;
        inner.wal.append(&rec)?;
        Self::apply_live(&mut inner.catalog, &mut inner.tables, &rec);
        Ok(n)
    }

    /// Overwrite the cells of an existing row, keeping its `row_id`. Logged as
    /// an idempotent `Insert` record for `row_id`.
    pub fn update_row(&self, table: &str, row_id: u64, cells: Vec<Value>) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let def = inner
            .tables
            .get(table)
            .ok_or_else(|| SqlError::NoSuchTable(table.to_string()))?
            .def
            .clone();
        def.validate_row(&cells)?;
        if !inner
            .tables
            .get(table)
            .expect("present")
            .rows
            .contains_key(&row_id)
        {
            return Err(SqlError::SchemaMismatch(format!(
                "row {row_id} does not exist in {table:?}"
            )));
        }
        let rec = WalRecord::Insert {
            table: table.to_string(),
            row_id,
            cells,
        };
        let inner = &mut *inner;
        inner.wal.append(&rec)?;
        Self::apply_live(&mut inner.catalog, &mut inner.tables, &rec);
        Ok(())
    }

    /// Delete a row by `row_id`. Returns whether a row was removed.
    pub fn delete(&self, table: &str, row_id: u64) -> Result<bool> {
        let mut inner = self.inner.lock().unwrap();
        if !inner.catalog.contains(table) {
            return Err(SqlError::NoSuchTable(table.to_string()));
        }
        let present = inner
            .tables
            .get(table)
            .map(|s| s.rows.contains_key(&row_id))
            .unwrap_or(false);
        if !present {
            return Ok(false);
        }
        let rec = WalRecord::Delete {
            table: table.to_string(),
            row_id,
        };
        let inner = &mut *inner;
        inner.wal.append(&rec)?;
        Self::apply_live(&mut inner.catalog, &mut inner.tables, &rec);
        Ok(true)
    }

    /// Return all live rows of a table as `(row_id, cells)`, ordered by `row_id`.
    pub fn scan(&self, table: &str) -> Result<Vec<(u64, Vec<Value>)>> {
        let inner = self.inner.lock().unwrap();
        let state = inner
            .tables
            .get(table)
            .ok_or_else(|| SqlError::NoSuchTable(table.to_string()))?;
        Ok(state.rows.iter().map(|(id, c)| (*id, c.clone())).collect())
    }

    /// Look up rows where an indexed `column` equals `value`. `Ok(None)` when no
    /// index covers `(table, column)`.
    fn index_lookup_eq(
        &self,
        table: &str,
        column: &str,
        value: &Value,
    ) -> Result<Option<store::Rows>> {
        let inner = self.inner.lock().unwrap();
        let Some(state) = inner.tables.get(table) else {
            return Err(SqlError::NoSuchTable(table.to_string()));
        };
        // Find an index whose column matches.
        let Some((_name, def)) = inner
            .catalog
            .indexes
            .iter()
            .find(|(_, d)| d.table == table && d.column == column)
        else {
            return Ok(None);
        };
        let Some(idx) = state.indexes.get(&def.name) else {
            return Ok(None);
        };
        let key = IndexKey(value.clone());
        let rows = match idx.map.get(&key) {
            Some(ids) => ids
                .iter()
                .filter_map(|id| state.rows.get(id).map(|c| (*id, c.clone())))
                .collect(),
            None => Vec::new(),
        };
        Ok(Some(rows))
    }

    /// Number of live rows in a table.
    pub fn row_count(&self, table: &str) -> Result<usize> {
        let inner = self.inner.lock().unwrap();
        inner
            .tables
            .get(table)
            .map(|s| s.rows.len())
            .ok_or_else(|| SqlError::NoSuchTable(table.to_string()))
    }

    /// The table definition, if it exists.
    pub fn table_def(&self, name: &str) -> Option<Table> {
        let inner = self.inner.lock().unwrap();
        inner.catalog.get(name).cloned()
    }

    /// All table names, sorted.
    pub fn table_names(&self) -> Vec<String> {
        let inner = self.inner.lock().unwrap();
        inner.catalog.tables.keys().cloned().collect()
    }

    /// The next `row_id` a table would assign (for transaction id seeding).
    pub(crate) fn peek_next_row_id(&self, table: &str) -> Option<u64> {
        let inner = self.inner.lock().unwrap();
        inner.tables.get(table).map(|s| s.next_row_id)
    }

    /// Atomically apply a group of records produced by a committing transaction:
    /// one `Batch` WAL record (a single fsync), then applied to live state.
    pub(crate) fn commit_batch(&self, ops: Vec<WalRecord>) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        let mut inner = self.inner.lock().unwrap();
        let rec = WalRecord::Batch(ops);
        let inner = &mut *inner;
        inner.wal.append(&rec)?;
        Self::apply_live(&mut inner.catalog, &mut inner.tables, &rec);
        Ok(())
    }

    /// Durably capture current state into `.rdat` snapshots + `catalog.json`,
    /// then truncate the WAL.
    pub fn checkpoint(&self) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let Inner {
            dir,
            catalog,
            tables,
            wal,
        } = &mut *inner;

        for (name, state) in tables.iter() {
            let rows = state.rows.iter().map(|(id, c)| (*id, c.as_slice()));
            storage::write_snapshot(dir, name, rows)?;
        }
        for entry in std::fs::read_dir(&*dir)? {
            let entry = entry?;
            let file_name = entry.file_name();
            let name = file_name.to_string_lossy();
            if let Some(table) = name.strip_suffix(".rdat")
                && !catalog.contains(table)
            {
                storage::remove_snapshot(dir, table)?;
            }
        }
        catalog.save(dir)?;
        wal.truncate()?;
        Ok(())
    }

    /// Parse and execute a SQL string, returning one [`QueryResult`] per
    /// statement.
    pub fn execute(&self, sql: &str) -> Result<Vec<QueryResult>> {
        self.execute_params(sql, &[])
    }

    /// Like [`execute`](Self::execute) but binds `?`/`$N` placeholders to
    /// `params` (left-to-right for `?`, `N-1` for `$N`).
    ///
    /// `BEGIN`/`COMMIT`/`ROLLBACK` within the string open and close a
    /// transaction whose writes are buffered and, on `COMMIT`, flushed
    /// atomically as one WAL batch. An unmatched `BEGIN` at end of string is
    /// rolled back (its buffered writes are discarded).
    pub fn execute_params(&self, sql: &str, params: &[Value]) -> Result<Vec<QueryResult>> {
        use ast::Statement;
        let statements = parser::parse(sql)?;
        let mut results = Vec::with_capacity(statements.len());
        let mut txn: Option<Transaction<'_>> = None;

        for stmt in statements {
            match stmt {
                Statement::Begin => {
                    if txn.is_some() {
                        return Err(SqlError::Unsupported("nested transaction".into()));
                    }
                    txn = Some(Transaction::new(self));
                    results.push(QueryResult::Transaction);
                }
                Statement::Commit => {
                    let t = txn
                        .take()
                        .ok_or_else(|| SqlError::Unsupported("COMMIT without BEGIN".into()))?;
                    t.commit()?;
                    results.push(QueryResult::Transaction);
                }
                Statement::Rollback => {
                    txn.take()
                        .ok_or_else(|| SqlError::Unsupported("ROLLBACK without BEGIN".into()))?;
                    results.push(QueryResult::Transaction);
                }
                other => {
                    let r = match &txn {
                        Some(t) => executor::execute(t, other, params)?,
                        None => executor::execute(self, other, params)?,
                    };
                    results.push(r);
                }
            }
        }
        // An open transaction at end of the batch is discarded (auto-rollback).
        Ok(results)
    }

    /// Path to the SQL root directory.
    pub fn dir(&self) -> PathBuf {
        self.inner.lock().unwrap().dir.clone()
    }
}

/// Autocommit `Store`: every operation is applied and logged immediately.
impl Store for SqlEngine {
    fn table_def(&self, name: &str) -> Option<Table> {
        SqlEngine::table_def(self, name)
    }
    fn scan(&self, table: &str) -> Result<Vec<(u64, Vec<Value>)>> {
        SqlEngine::scan(self, table)
    }
    fn scan_pruned(&self, table: &str, keep: &[usize]) -> Result<store::Chunk> {
        let inner = self.inner.lock().unwrap();
        let state = inner
            .tables
            .get(table)
            .ok_or_else(|| SqlError::NoSuchTable(table.to_string()))?;
        let n = state.rows.len();
        let mut cells = Vec::with_capacity(n * keep.len());
        for row in state.rows.values() {
            for &k in keep {
                cells.push(row[k].clone());
            }
        }
        Ok(store::Chunk {
            width: keep.len(),
            n,
            cells,
        })
    }
    fn insert(&self, table: &str, cells: Vec<Value>) -> Result<u64> {
        SqlEngine::insert(self, table, cells)
    }
    fn insert_many(&self, table: &str, rows: Vec<Vec<Value>>) -> Result<u64> {
        SqlEngine::insert_many(self, table, rows)
    }
    fn update_row(&self, table: &str, row_id: u64, cells: Vec<Value>) -> Result<()> {
        SqlEngine::update_row(self, table, row_id, cells)
    }
    fn delete(&self, table: &str, row_id: u64) -> Result<bool> {
        SqlEngine::delete(self, table, row_id)
    }
    fn create_table(&self, table: Table) -> Result<()> {
        SqlEngine::create_table(self, table)
    }
    fn drop_table(&self, name: &str) -> Result<()> {
        SqlEngine::drop_table(self, name)
    }
    fn create_index(&self, name: &str, table: &str, column: &str) -> Result<()> {
        SqlEngine::create_index(self, name, table, column)
    }
    fn drop_index(&self, name: &str) -> Result<()> {
        SqlEngine::drop_index(self, name)
    }
    fn index_lookup_eq(
        &self,
        table: &str,
        column: &str,
        value: &Value,
    ) -> Result<Option<store::Rows>> {
        SqlEngine::index_lookup_eq(self, table, column, value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn users() -> Table {
        Table::new(
            "users",
            vec![
                Column::new("id", SqlType::Int).primary_key(),
                Column::new("name", SqlType::Text).not_null(),
            ],
        )
    }

    #[test]
    fn basic_crud() {
        let dir = tempfile::tempdir().unwrap();
        let db = SqlEngine::open(dir.path()).unwrap();
        db.create_table(users()).unwrap();

        let a = db
            .insert("users", vec![Value::Int(1), Value::Text("ada".into())])
            .unwrap();
        let b = db
            .insert("users", vec![Value::Int(2), Value::Text("bob".into())])
            .unwrap();
        assert_ne!(a, b);
        assert_eq!(db.row_count("users").unwrap(), 2);

        assert!(db.delete("users", a).unwrap());
        assert!(!db.delete("users", a).unwrap());
        assert_eq!(db.row_count("users").unwrap(), 1);
    }

    #[test]
    fn create_table_rejects_duplicate() {
        let dir = tempfile::tempdir().unwrap();
        let db = SqlEngine::open(dir.path()).unwrap();
        db.create_table(users()).unwrap();
        assert!(matches!(
            db.create_table(users()),
            Err(SqlError::TableExists(_))
        ));
    }

    #[test]
    fn insert_validates_schema() {
        let dir = tempfile::tempdir().unwrap();
        let db = SqlEngine::open(dir.path()).unwrap();
        db.create_table(users()).unwrap();
        assert!(
            db.insert("users", vec![Value::Int(1), Value::Null])
                .is_err()
        );
        assert!(db.insert("users", vec![Value::Int(1)]).is_err());
    }
}
