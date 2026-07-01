//! # oxidb-sql
//!
//! A standalone SQL engine mounted alongside the OxiDB document engine
//! (see ADR-0010). It owns its own files, catalog, storage format, WAL, and
//! recovery, and shares no state with the document engine.
//!
//! **Phase 0 scope (this crate revision):** the durable foundation only —
//! catalog, typed rows, row-oriented `.rdat` snapshots, an independent WAL, and
//! crash recovery. The SQL parser, planner, and executor arrive in later phases;
//! for now the engine exposes a small programmatic API (`create_table`,
//! `insert`, `delete`, `scan`, `checkpoint`) sufficient to prove durability and
//! independent crash-replay.

mod ast;
mod catalog;
mod error;
mod executor;
mod parser;
mod storage;
mod types;
mod wal;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

pub use ast::QueryResult;
pub use catalog::{Column, Table};
pub use error::{Result, SqlError};
pub use types::{SqlType, Value};

use catalog::Catalog;
use wal::{Wal, WalRecord};

/// Runtime state for one table: its definition plus its live rows keyed by a
/// dense, engine-assigned `row_id`.
struct TableState {
    def: Table,
    rows: BTreeMap<u64, Vec<Value>>,
    next_row_id: u64,
}

impl TableState {
    fn empty(def: Table) -> Self {
        TableState {
            def,
            rows: BTreeMap::new(),
            next_row_id: 1,
        }
    }

    fn observe_row_id(&mut self, row_id: u64) {
        if row_id >= self.next_row_id {
            self.next_row_id = row_id + 1;
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
    /// `oxidb_data/sql`). Loads the catalog and `.rdat` snapshots, then replays
    /// the WAL tail to recover any mutations made since the last checkpoint.
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
        for rec in records {
            Self::apply(&mut catalog, &mut tables, rec);
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

    /// Apply a WAL record to the in-memory catalog + tables. Idempotent by
    /// (table, row_id) so replay over a snapshot always converges.
    fn apply(catalog: &mut Catalog, tables: &mut BTreeMap<String, TableState>, rec: WalRecord) {
        match rec {
            WalRecord::CreateTable(def) => {
                catalog.tables.insert(def.name.clone(), def.clone());
                tables
                    .entry(def.name.clone())
                    .or_insert_with(|| TableState::empty(def));
            }
            WalRecord::DropTable(name) => {
                catalog.tables.remove(&name);
                tables.remove(&name);
            }
            WalRecord::Insert {
                table,
                row_id,
                cells,
            } => {
                if let Some(state) = tables.get_mut(&table) {
                    state.observe_row_id(row_id);
                    state.rows.insert(row_id, cells);
                }
            }
            WalRecord::Delete { table, row_id } => {
                if let Some(state) = tables.get_mut(&table) {
                    state.rows.remove(&row_id);
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
        inner.wal.append(&WalRecord::CreateTable(def.clone()))?;
        inner.catalog.tables.insert(def.name.clone(), def.clone());
        inner
            .tables
            .insert(def.name.clone(), TableState::empty(def));
        Ok(())
    }

    /// Drop a table and its rows. Errors if it does not exist.
    pub fn drop_table(&self, name: &str) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        if !inner.catalog.contains(name) {
            return Err(SqlError::NoSuchTable(name.to_string()));
        }
        inner.wal.append(&WalRecord::DropTable(name.to_string()))?;
        inner.catalog.tables.remove(name);
        inner.tables.remove(name);
        Ok(())
    }

    /// Insert a row, returning its assigned `row_id`. The row is validated
    /// against the table schema (arity, types, nullability) before it is logged.
    pub fn insert(&self, table: &str, cells: Vec<Value>) -> Result<u64> {
        let mut inner = self.inner.lock().unwrap();
        let def = inner
            .tables
            .get(table)
            .ok_or_else(|| SqlError::NoSuchTable(table.to_string()))?
            .def
            .clone();
        def.validate_row(&cells)?;

        let row_id = {
            let state = inner.tables.get_mut(table).expect("table state present");
            let id = state.next_row_id;
            state.next_row_id += 1;
            id
        };

        inner.wal.append(&WalRecord::Insert {
            table: table.to_string(),
            row_id,
            cells: cells.clone(),
        })?;
        inner
            .tables
            .get_mut(table)
            .expect("table state present")
            .rows
            .insert(row_id, cells);
        Ok(row_id)
    }

    /// Overwrite the cells of an existing row, keeping its `row_id`. The new
    /// cells are validated against the schema before being logged. Errors if the
    /// table or the row does not exist.
    ///
    /// This is logged as an idempotent `Insert` record for `row_id` — replaying
    /// it re-establishes exactly this row image, which is why an update never
    /// needs a distinct WAL op.
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
            .expect("table state present")
            .rows
            .contains_key(&row_id)
        {
            return Err(SqlError::SchemaMismatch(format!(
                "row {row_id} does not exist in {table:?}"
            )));
        }
        inner.wal.append(&WalRecord::Insert {
            table: table.to_string(),
            row_id,
            cells: cells.clone(),
        })?;
        inner
            .tables
            .get_mut(table)
            .expect("table state present")
            .rows
            .insert(row_id, cells);
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
        inner.wal.append(&WalRecord::Delete {
            table: table.to_string(),
            row_id,
        })?;
        inner
            .tables
            .get_mut(table)
            .expect("table state present")
            .rows
            .remove(&row_id);
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

    /// Durably capture current state into `.rdat` snapshots + `catalog.json`,
    /// then truncate the WAL.
    ///
    /// Ordering is crash-safe: snapshots and catalog are fsynced *before* the
    /// WAL is truncated, so a crash mid-checkpoint recovers to the same state by
    /// loading the snapshots and replaying the (not-yet-truncated) WAL.
    pub fn checkpoint(&self) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let Inner {
            dir,
            catalog,
            tables,
            wal,
        } = &mut *inner;

        // 1. Write every live table's snapshot.
        for (name, state) in tables.iter() {
            let rows = state.rows.iter().map(|(id, c)| (*id, c.as_slice()));
            storage::write_snapshot(dir, name, rows)?;
        }
        // 2. Remove snapshots for tables that no longer exist.
        //    (Any `.rdat` whose table is absent from the catalog is stale.)
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
        // 3. Persist the schema, then truncate the WAL.
        catalog.save(dir)?;
        wal.truncate()?;
        Ok(())
    }

    /// Parse and execute a SQL string, returning one [`QueryResult`] per
    /// statement (a single string may contain several `;`-separated statements).
    pub fn execute(&self, sql: &str) -> Result<Vec<QueryResult>> {
        let statements = parser::parse(sql)?;
        let mut results = Vec::with_capacity(statements.len());
        for stmt in statements {
            results.push(executor::execute(self, stmt)?);
        }
        Ok(results)
    }

    /// Path to the SQL root directory.
    pub fn dir(&self) -> PathBuf {
        self.inner.lock().unwrap().dir.clone()
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
        // NOT NULL name violated
        assert!(
            db.insert("users", vec![Value::Int(1), Value::Null])
                .is_err()
        );
        // wrong arity
        assert!(db.insert("users", vec![Value::Int(1)]).is_err());
    }
}
