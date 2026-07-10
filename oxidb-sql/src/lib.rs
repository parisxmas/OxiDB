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
mod cobra;
mod decimal;
mod error;
mod executor;
pub mod json;
mod parser;
mod rows;
mod storage;
mod store;
mod transaction;
mod types;
mod wal;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

pub use ast::QueryResult;
pub use catalog::{Column, IndexDef, Table};
pub use decimal::Decimal;
pub use error::{Result, SqlError};
pub use parser::{
    DatabaseStatement, UserStatement, parse_database_statement, parse_user_statement,
};
pub use types::{SqlType, Value};

use catalog::Catalog;
use rows::RowStore;
use store::Store;
use transaction::Transaction;
use types::IndexKey;
use wal::{Wal, WalRecord};

/// Engine-level options, normally read from the environment by
/// [`SqlEngine::open`]. Both row-store modes share the same on-disk format,
/// so a database can be reopened in either mode.
#[derive(Debug, Clone)]
pub struct SqlOptions {
    /// Keep the bulk of each table on disk (mmap'd last-checkpoint snapshot)
    /// with only post-checkpoint changes in RAM, instead of holding every row
    /// resident. Env: `OXIDB_SQL_DISK_FIRST`.
    pub disk_first: bool,
    /// Auto-checkpoint when the live WAL exceeds this many bytes (folds the
    /// WAL into `.rdat` snapshots and truncates it; also bounds the RAM
    /// overlay in disk-first mode). `0` disables auto-checkpointing.
    /// Env: `OXIDB_SQL_CHECKPOINT_BYTES`.
    pub checkpoint_bytes: u64,
}

impl Default for SqlOptions {
    fn default() -> Self {
        SqlOptions {
            disk_first: false,
            checkpoint_bytes: 64 << 20, // 64 MiB
        }
    }
}

impl SqlOptions {
    pub fn from_env() -> Self {
        let mut opts = SqlOptions::default();
        if let Ok(v) = std::env::var("OXIDB_SQL_DISK_FIRST") {
            opts.disk_first =
                matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on");
        }
        if let Ok(v) = std::env::var("OXIDB_SQL_CHECKPOINT_BYTES")
            && let Ok(n) = v.trim().parse::<u64>()
        {
            opts.checkpoint_bytes = n;
        }
        opts
    }
}

/// An in-memory secondary index over one or more columns:
/// key tuple -> set of row ids.
struct SecondaryIndex {
    col_pos: Vec<usize>,
    map: BTreeMap<Vec<IndexKey>, BTreeSet<u64>>,
}

impl SecondaryIndex {
    fn key_of(&self, cells: &[Value]) -> Vec<IndexKey> {
        self.col_pos
            .iter()
            .map(|&p| IndexKey(cells[p].clone()))
            .collect()
    }
}

/// Runtime state for one table: its definition, its live rows keyed by a dense
/// engine-assigned `row_id`, and any secondary indexes.
struct TableState {
    def: Table,
    rows: RowStore,
    next_row_id: u64,
    /// Position of the AUTO_INCREMENT column (if any) and the next value its
    /// counter would assign. Seeded from existing data at open (max + 1), so
    /// restart semantics match SQLite's default rowid behavior.
    auto_pos: Option<usize>,
    next_auto: i64,
    /// Secondary indexes keyed by index name.
    indexes: BTreeMap<String, SecondaryIndex>,
    /// PRIMARY KEY column position (if any) and its value -> row_id map,
    /// used to enforce uniqueness on writes.
    pk_pos: Option<usize>,
    pk_map: BTreeMap<IndexKey, u64>,
    /// Column-level `UNIQUE` constraints: `(column position, value -> row_id)`.
    /// NULLs are exempt (per SQL).
    uniques: Vec<(usize, BTreeMap<IndexKey, u64>)>,
}

impl TableState {
    fn empty(def: Table, disk_first: bool) -> Self {
        let pk_pos = def.pk_pos();
        let auto_pos = def.columns.iter().position(|c| c.auto_increment);
        let uniques = def
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.unique && !c.primary_key)
            .map(|(i, _)| (i, BTreeMap::new()))
            .collect();
        TableState {
            def,
            rows: RowStore::new(disk_first),
            next_row_id: 1,
            auto_pos,
            next_auto: 1,
            indexes: BTreeMap::new(),
            pk_pos,
            pk_map: BTreeMap::new(),
            uniques,
        }
    }

    /// Recompute the schema-derived positions and reseed every constraint
    /// map from the current rows (used after `ALTER TABLE`).
    fn rebuild_meta(&mut self) {
        self.pk_pos = self.def.pk_pos();
        self.auto_pos = self.def.columns.iter().position(|c| c.auto_increment);
        self.uniques = self
            .def
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.unique && !c.primary_key)
            .map(|(i, _)| (i, BTreeMap::new()))
            .collect();
        self.pk_map.clear();
        self.next_auto = 1;
        let mut seeds: Vec<(u64, Vec<Value>)> = Vec::new();
        for (rid, cells) in self.rows.iter() {
            seeds.push((rid, cells.into_owned()));
        }
        for (rid, cells) in seeds {
            if let Some(p) = self.pk_pos {
                self.pk_map.insert(IndexKey(cells[p].clone()), rid);
            }
            for (pos, map) in self.uniques.iter_mut() {
                if !matches!(cells[*pos], Value::Null) {
                    map.insert(IndexKey(cells[*pos].clone()), rid);
                }
            }
            self.observe_auto(&cells);
        }
    }

    fn observe_row_id(&mut self, row_id: u64) {
        if row_id >= self.next_row_id {
            self.next_row_id = row_id + 1;
        }
    }

    /// Keep the auto-increment counter ahead of every value that reaches the
    /// table — explicit inserts, WAL replay, snapshot load.
    fn observe_auto(&mut self, cells: &[Value]) {
        if let Some(p) = self.auto_pos
            && let Some(Value::Int(v)) = cells.get(p)
            && *v >= self.next_auto
        {
            self.next_auto = v + 1;
        }
    }

    /// (Re)build a secondary index over `columns` from the current rows.
    fn build_index(&mut self, index_name: &str, columns: &[String]) -> Result<()> {
        let col_pos: Vec<usize> = columns
            .iter()
            .map(|column| {
                self.def
                    .columns
                    .iter()
                    .position(|c| &c.name == column)
                    .ok_or_else(|| SqlError::NoSuchColumn(column.to_string()))
            })
            .collect::<Result<_>>()?;
        let mut idx = SecondaryIndex {
            col_pos,
            map: BTreeMap::new(),
        };
        for (rid, cells) in self.rows.iter() {
            let key = idx.key_of(&cells);
            idx.map.entry(key).or_default().insert(rid);
        }
        self.indexes.insert(index_name.to_string(), idx);
        Ok(())
    }

    fn index_insert(&mut self, row_id: u64, cells: &[Value]) {
        for idx in self.indexes.values_mut() {
            let key = idx.key_of(cells);
            idx.map.entry(key).or_default().insert(row_id);
        }
        if let Some(p) = self.pk_pos {
            self.pk_map.insert(IndexKey(cells[p].clone()), row_id);
        }
        for (pos, map) in self.uniques.iter_mut() {
            if !matches!(cells[*pos], Value::Null) {
                map.insert(IndexKey(cells[*pos].clone()), row_id);
            }
        }
    }

    fn index_remove(&mut self, row_id: u64, cells: &[Value]) {
        for idx in self.indexes.values_mut() {
            let key = idx.key_of(cells);
            if let Some(set) = idx.map.get_mut(&key) {
                set.remove(&row_id);
                if set.is_empty() {
                    idx.map.remove(&key);
                }
            }
        }
        if let Some(p) = self.pk_pos {
            let key = IndexKey(cells[p].clone());
            // Only remove the mapping if it still points at this row (an
            // idempotent WAL replay can re-insert before the old delete).
            if self.pk_map.get(&key) == Some(&row_id) {
                self.pk_map.remove(&key);
            }
        }
        for (pos, map) in self.uniques.iter_mut() {
            let key = IndexKey(cells[*pos].clone());
            if map.get(&key) == Some(&row_id) {
                map.remove(&key);
            }
        }
    }

    /// Error if `cells`' PRIMARY KEY value already belongs to a row other
    /// than `exclude_row`.
    fn check_pk(&self, cells: &[Value], exclude_row: Option<u64>) -> Result<()> {
        if let Some(p) = self.pk_pos {
            let key = IndexKey(cells[p].clone());
            if let Some(&existing) = self.pk_map.get(&key)
                && Some(existing) != exclude_row
            {
                return Err(SqlError::DuplicateKey(format!(
                    "PRIMARY KEY value {:?} already exists in {:?}",
                    cells[p], self.def.name
                )));
            }
        }
        for (pos, map) in &self.uniques {
            if matches!(cells[*pos], Value::Null) {
                continue; // SQL: NULLs never collide under UNIQUE
            }
            let key = IndexKey(cells[*pos].clone());
            if let Some(&existing) = map.get(&key)
                && Some(existing) != exclude_row
            {
                return Err(SqlError::DuplicateKey(format!(
                    "UNIQUE value {:?} already exists in {:?}.{:?}",
                    cells[*pos], self.def.name, self.def.columns[*pos].name
                )));
            }
        }
        Ok(())
    }
}

struct Inner {
    dir: PathBuf,
    catalog: Catalog,
    tables: BTreeMap<String, TableState>,
    wal: Wal,
    disk_first: bool,
    /// Auto-checkpoint threshold in WAL bytes (0 = manual only).
    checkpoint_bytes: u64,
}

/// The public SQL engine handle. Cheap to share behind an `Arc`.
pub struct SqlEngine {
    inner: Mutex<Inner>,
    /// Interactive (session) transactions parked between calls, keyed by id
    /// (ADR-0013 Phase B). A transaction lives here while its connection is
    /// between requests; execution takes it out and puts it back.
    session_txns: Mutex<std::collections::HashMap<u64, transaction::TxnState>>,
    next_session_txn: std::sync::atomic::AtomicU64,
}

impl SqlEngine {
    /// Open (creating if needed) a SQL engine rooted at `dir` (e.g.
    /// `oxidb_data/sql`), with options from the environment. Loads the catalog
    /// and `.rdat` snapshots, replays the WAL tail, then (re)builds secondary
    /// indexes.
    pub fn open(dir: impl AsRef<Path>) -> Result<SqlEngine> {
        Self::open_with_options(dir, SqlOptions::from_env())
    }

    /// [`open`](SqlEngine::open) with explicit options.
    pub fn open_with_options(dir: impl AsRef<Path>, opts: SqlOptions) -> Result<SqlEngine> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        // 1. Durable schema snapshot.
        let mut catalog = Catalog::load(&dir)?;

        // 2. Load each table's row snapshot. Resident mode materializes every
        //    row; disk-first maps the snapshot and makes one decoding pass to
        //    seed `next_row_id` and the PK map without retaining the rows.
        let mut tables: BTreeMap<String, TableState> = BTreeMap::new();
        for (name, def) in &catalog.tables {
            let mut state = TableState::empty(def.clone(), opts.disk_first);
            if opts.disk_first {
                if let Some(snap) = storage::MappedSnapshot::open(&dir, name, def.arity())? {
                    for (row_id, cells) in snap.entries() {
                        state.observe_row_id(row_id);
                        state.observe_auto(&cells);
                        if let Some(p) = state.pk_pos {
                            state.pk_map.insert(IndexKey(cells[p].clone()), row_id);
                        }
                        for (pos, map) in state.uniques.iter_mut() {
                            if !matches!(cells[*pos], Value::Null) {
                                map.insert(IndexKey(cells[*pos].clone()), row_id);
                            }
                        }
                    }
                    state.rows.attach_base(snap);
                }
            } else {
                for (row_id, cells) in storage::read_snapshot(&dir, name, def.arity())? {
                    state.observe_row_id(row_id);
                    state.observe_auto(&cells);
                    if let Some(p) = state.pk_pos {
                        state.pk_map.insert(IndexKey(cells[p].clone()), row_id);
                    }
                    for (pos, map) in state.uniques.iter_mut() {
                        if !matches!(cells[*pos], Value::Null) {
                            map.insert(IndexKey(cells[*pos].clone()), row_id);
                        }
                    }
                    state.rows.insert(row_id, cells);
                }
            }
            tables.insert(name.clone(), state);
        }

        // 3. Replay the WAL tail over the snapshots (idempotent).
        let (wal, records) = Wal::open(&dir)?;
        for rec in &records {
            Self::apply_live(&mut catalog, &mut tables, rec, opts.disk_first);
        }

        // 4. Build any indexes that existed before the last checkpoint (they are
        //    in the loaded catalog but not in the truncated WAL, so replay didn't
        //    reconstruct them).
        let defs: Vec<IndexDef> = catalog.indexes.values().cloned().collect();
        for def in defs {
            if let Some(state) = tables.get_mut(&def.table)
                && !state.indexes.contains_key(&def.name)
            {
                let _ = state.build_index(&def.name, &def.columns);
            }
        }

        Ok(SqlEngine {
            session_txns: Mutex::new(std::collections::HashMap::new()),
            next_session_txn: std::sync::atomic::AtomicU64::new(1),
            inner: Mutex::new(Inner {
                dir,
                catalog,
                tables,
                wal,
                disk_first: opts.disk_first,
                checkpoint_bytes: opts.checkpoint_bytes,
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
        disk_first: bool,
    ) {
        match rec {
            WalRecord::AlterTable { table, op } => {
                let Some(def) = catalog.tables.get_mut(table) else {
                    return;
                };
                use ast::AlterOp;
                match op {
                    AlterOp::AddColumn(col) => {
                        def.columns.push(col.clone());
                        if let Some(state) = tables.get_mut(table) {
                            state.def = def.clone();
                            let fill = col.default_value.clone().unwrap_or(Value::Null);
                            state.rows.rewrite_all(|cells| cells.push(fill.clone()));
                        }
                    }
                    AlterOp::DropColumn(name) => {
                        let Some(pos) = def.columns.iter().position(|c| &c.name == name) else {
                            return;
                        };
                        def.columns.remove(pos);
                        // Indexes over the column go with it (replay-lenient;
                        // the engine-level command validates first).
                        catalog
                            .indexes
                            .retain(|_, d| !(&d.table == table && d.columns.contains(name)));
                        if let Some(state) = tables.get_mut(table) {
                            state.def = def.clone();
                            state.rows.rewrite_all(|cells| {
                                cells.remove(pos);
                            });
                        }
                    }
                    AlterOp::RenameColumn { old, new } => {
                        if let Some(c) = def.columns.iter_mut().find(|c| &c.name == old) {
                            c.name = new.clone();
                        }
                        for d in catalog.indexes.values_mut() {
                            if &d.table == table {
                                for c in d.columns.iter_mut() {
                                    if c == old {
                                        *c = new.clone();
                                    }
                                }
                            }
                        }
                        if let Some(state) = tables.get_mut(table) {
                            state.def = def.clone();
                        }
                    }
                }
                // Positions may have shifted: rebuild constraint maps and the
                // table's secondary indexes from the (possibly rewritten) rows.
                let defs: Vec<IndexDef> = catalog
                    .indexes
                    .values()
                    .filter(|d| &d.table == table)
                    .cloned()
                    .collect();
                if let Some(state) = tables.get_mut(table) {
                    state.rebuild_meta();
                    state.indexes.clear();
                    for d in defs {
                        let _ = state.build_index(&d.name, &d.columns);
                    }
                }
            }
            WalRecord::CreateTable(def) => {
                catalog.tables.insert(def.name.clone(), def.clone());
                tables
                    .entry(def.name.clone())
                    .or_insert_with(|| TableState::empty(def.clone(), disk_first));
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
                    let _ = state.build_index(&def.name, &def.columns);
                }
            }
            WalRecord::DropIndex(name) => {
                if let Some(def) = catalog.indexes.remove(name)
                    && let Some(state) = tables.get_mut(&def.table)
                {
                    state.indexes.remove(name);
                }
            }
            WalRecord::CreateView { name, sql } => {
                catalog.views.insert(name.clone(), sql.clone());
            }
            WalRecord::DropView(name) => {
                catalog.views.remove(name);
            }
            WalRecord::CreateProcedure { name, def } => {
                catalog.procedures.insert(name.clone(), def.clone());
            }
            WalRecord::DropProcedure(name) => {
                catalog.procedures.remove(name);
            }
            WalRecord::Insert {
                table,
                row_id,
                cells,
            } => {
                if let Some(state) = tables.get_mut(table) {
                    if let Some(old) = state.rows.get(*row_id) {
                        state.index_remove(*row_id, &old);
                    }
                    state.observe_row_id(*row_id);
                    state.observe_auto(cells);
                    state.rows.insert(*row_id, cells.clone());
                    state.index_insert(*row_id, cells);
                }
            }
            WalRecord::Delete { table, row_id } => {
                if let Some(state) = tables.get_mut(table)
                    && let Some(old) = state.rows.remove(*row_id)
                {
                    state.index_remove(*row_id, &old);
                }
            }
            WalRecord::Batch(ops) => {
                for op in ops {
                    Self::apply_live(catalog, tables, op, disk_first);
                }
            }
        }
    }

    /// Create a new table. Errors if a table or view of the same name exists.
    pub fn create_table(&self, def: Table) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        if inner.catalog.contains(&def.name) || inner.catalog.views.contains_key(&def.name) {
            return Err(SqlError::TableExists(def.name));
        }
        let rec = WalRecord::CreateTable(def);
        let inner = &mut *inner;
        inner.wal.append(&rec)?;
        Self::apply_live(
            &mut inner.catalog,
            &mut inner.tables,
            &rec,
            inner.disk_first,
        );
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
        Self::apply_live(
            &mut inner.catalog,
            &mut inner.tables,
            &rec,
            inner.disk_first,
        );
        Ok(())
    }

    /// Create a secondary index over one or more columns. Errors if the index
    /// name is taken or the table / any column does not exist.
    pub fn create_index(&self, name: &str, table: &str, columns: &[String]) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        if inner.catalog.indexes.contains_key(name) {
            return Err(SqlError::IndexExists(name.to_string()));
        }
        if columns.is_empty() {
            return Err(SqlError::Unsupported("index without columns".into()));
        }
        let def = inner
            .tables
            .get(table)
            .ok_or_else(|| SqlError::NoSuchTable(table.to_string()))?
            .def
            .clone();
        for column in columns {
            if !def.columns.iter().any(|c| &c.name == column) {
                return Err(SqlError::NoSuchColumn(column.to_string()));
            }
        }
        let rec = WalRecord::CreateIndex(IndexDef {
            name: name.to_string(),
            table: table.to_string(),
            columns: columns.to_vec(),
        });
        let inner = &mut *inner;
        inner.wal.append(&rec)?;
        Self::apply_live(
            &mut inner.catalog,
            &mut inner.tables,
            &rec,
            inner.disk_first,
        );
        Ok(())
    }

    /// Create (or with `or_replace`, overwrite) a view. The body must be a
    /// single SELECT; it is trial-executed so errors surface at creation.
    pub fn create_view(&self, name: &str, query_sql: &str, or_replace: bool) -> Result<()> {
        // Validate before taking the engine lock: must parse to exactly one
        // SELECT, and a trial run proves the referenced tables/columns exist.
        let stmts = parser::parse(query_sql)?;
        let is_single_select =
            stmts.len() == 1 && matches!(stmts.first(), Some(ast::Statement::Select(_)));
        if !is_single_select {
            return Err(SqlError::Unsupported(
                "a view body must be a single SELECT".into(),
            ));
        }
        executor::execute(self, stmts.into_iter().next().expect("checked"), &[])?;

        let mut inner = self.inner.lock().unwrap();
        if inner.catalog.contains(name) {
            return Err(SqlError::TableExists(name.to_string()));
        }
        if inner.catalog.views.contains_key(name) && !or_replace {
            return Err(SqlError::TableExists(format!("{name} (view)")));
        }
        let rec = WalRecord::CreateView {
            name: name.to_string(),
            sql: query_sql.to_string(),
        };
        let inner = &mut *inner;
        inner.wal.append(&rec)?;
        Self::apply_live(
            &mut inner.catalog,
            &mut inner.tables,
            &rec,
            inner.disk_first,
        );
        Ok(())
    }

    /// Drop a view by name. Errors if it does not exist.
    pub fn drop_view(&self, name: &str) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        if !inner.catalog.views.contains_key(name) {
            return Err(SqlError::NoSuchView(name.to_string()));
        }
        let rec = WalRecord::DropView(name.to_string());
        let inner = &mut *inner;
        inner.wal.append(&rec)?;
        Self::apply_live(
            &mut inner.catalog,
            &mut inner.tables,
            &rec,
            inner.disk_first,
        );
        Ok(())
    }

    /// The stored SQL text of a view, if it exists.
    pub fn view_sql(&self, name: &str) -> Option<String> {
        self.inner.lock().unwrap().catalog.views.get(name).cloned()
    }

    /// Create (or with `or_alter`, overwrite) a stored procedure. A SQL body
    /// was validated to shape (DML/SELECT only, params rewritten to `$N`) at
    /// parse; here it is trial-parsed so unsupported constructs surface at
    /// creation, not first call. A COBRA body was already decoded and
    /// validated by the executor (its `body` is a display placeholder, not
    /// SQL).
    pub fn create_procedure(
        &self,
        name: &str,
        def: catalog::ProcedureDef,
        or_alter: bool,
    ) -> Result<()> {
        if def.language == catalog::ProcLanguage::Sql {
            parser::parse(&def.body)?;
        }
        let mut inner = self.inner.lock().unwrap();
        if inner.catalog.procedures.contains_key(name) && !or_alter {
            return Err(SqlError::TableExists(format!("{name} (procedure)")));
        }
        let rec = WalRecord::CreateProcedure {
            name: name.to_string(),
            def,
        };
        let inner = &mut *inner;
        inner.wal.append(&rec)?;
        Self::apply_live(
            &mut inner.catalog,
            &mut inner.tables,
            &rec,
            inner.disk_first,
        );
        Ok(())
    }

    /// Drop a stored procedure by name. Errors if it does not exist.
    pub fn drop_procedure(&self, name: &str) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        if !inner.catalog.procedures.contains_key(name) {
            return Err(SqlError::NoSuchProcedure(name.to_string()));
        }
        let rec = WalRecord::DropProcedure(name.to_string());
        let inner = &mut *inner;
        inner.wal.append(&rec)?;
        Self::apply_live(
            &mut inner.catalog,
            &mut inner.tables,
            &rec,
            inner.disk_first,
        );
        Ok(())
    }

    /// A stored procedure's definition, if it exists.
    pub fn procedure_def(&self, name: &str) -> Option<catalog::ProcedureDef> {
        self.inner
            .lock()
            .unwrap()
            .catalog
            .procedures
            .get(name)
            .cloned()
    }

    /// All stored procedures as `(name, def)` pairs, sorted by name.
    pub fn list_procedures(&self) -> Vec<(String, catalog::ProcedureDef)> {
        let inner = self.inner.lock().unwrap();
        inner
            .catalog
            .procedures
            .iter()
            .map(|(n, d)| (n.clone(), d.clone()))
            .collect()
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
        Self::apply_live(
            &mut inner.catalog,
            &mut inner.tables,
            &rec,
            inner.disk_first,
        );
        Ok(())
    }

    /// Insert a row, returning its assigned `row_id`. Validated against the
    /// schema (arity, types, nullability, PRIMARY KEY uniqueness) before it
    /// is logged; integer values widen into DOUBLE/TIMESTAMP columns.
    pub fn insert(&self, table: &str, cells: Vec<Value>) -> Result<u64> {
        let mut cells = cells;
        let mut inner = self.inner.lock().unwrap();
        let state = inner
            .tables
            .get(table)
            .ok_or_else(|| SqlError::NoSuchTable(table.to_string()))?;
        state.def.coerce_row(&mut cells);
        state.def.validate_row(&cells)?;
        state.check_pk(&cells, None)?;

        let row_id = inner.tables.get(table).expect("present").next_row_id;
        let rec = WalRecord::Insert {
            table: table.to_string(),
            row_id,
            cells,
        };
        let inner = &mut *inner;
        inner.wal.append(&rec)?;
        Self::apply_live(
            &mut inner.catalog,
            &mut inner.tables,
            &rec,
            inner.disk_first,
        );
        Ok(row_id)
    }

    /// Insert many rows in one durable step: all rows are validated (schema +
    /// PRIMARY KEY uniqueness, including duplicates *within* the batch),
    /// logged as a single WAL `Batch` record (one fsync), then applied.
    /// Returns the number of rows inserted.
    pub fn insert_many(&self, table: &str, rows: Vec<Vec<Value>>) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }
        let mut rows = rows;
        let mut inner = self.inner.lock().unwrap();
        let state = inner
            .tables
            .get(table)
            .ok_or_else(|| SqlError::NoSuchTable(table.to_string()))?;
        let mut batch_keys: BTreeSet<IndexKey> = BTreeSet::new();
        for cells in &mut rows {
            state.def.coerce_row(cells);
            state.def.validate_row(cells)?;
            state.check_pk(cells, None)?;
            if let Some(p) = state.pk_pos
                && !batch_keys.insert(IndexKey(cells[p].clone()))
            {
                return Err(SqlError::DuplicateKey(format!(
                    "PRIMARY KEY value {:?} appears twice in the same INSERT",
                    cells[p]
                )));
            }
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
        Self::apply_live(
            &mut inner.catalog,
            &mut inner.tables,
            &rec,
            inner.disk_first,
        );
        Ok(n)
    }

    /// Overwrite the cells of an existing row, keeping its `row_id`. Logged as
    /// an idempotent `Insert` record for `row_id`.
    pub fn update_row(&self, table: &str, row_id: u64, cells: Vec<Value>) -> Result<()> {
        let mut cells = cells;
        let mut inner = self.inner.lock().unwrap();
        let state = inner
            .tables
            .get(table)
            .ok_or_else(|| SqlError::NoSuchTable(table.to_string()))?;
        state.def.coerce_row(&mut cells);
        state.def.validate_row(&cells)?;
        state.check_pk(&cells, Some(row_id))?;
        if !inner
            .tables
            .get(table)
            .expect("present")
            .rows
            .contains(row_id)
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
        Self::apply_live(
            &mut inner.catalog,
            &mut inner.tables,
            &rec,
            inner.disk_first,
        );
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
            .map(|s| s.rows.contains(row_id))
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
        Self::apply_live(
            &mut inner.catalog,
            &mut inner.tables,
            &rec,
            inner.disk_first,
        );
        Ok(true)
    }

    /// Return all live rows of a table as `(row_id, cells)`, ordered by `row_id`.
    pub fn scan(&self, table: &str) -> Result<Vec<(u64, Vec<Value>)>> {
        let inner = self.inner.lock().unwrap();
        let state = inner
            .tables
            .get(table)
            .ok_or_else(|| SqlError::NoSuchTable(table.to_string()))?;
        Ok(state
            .rows
            .iter()
            .map(|(id, c)| (id, c.into_owned()))
            .collect())
    }

    /// Look up rows using a secondary index whose columns are all present in
    /// the `column = value` pairs `eqs`. `Ok(None)` when no index qualifies.
    fn index_lookup_eq(&self, table: &str, eqs: &[(String, Value)]) -> Result<Option<store::Rows>> {
        let inner = self.inner.lock().unwrap();
        let Some(state) = inner.tables.get(table) else {
            return Err(SqlError::NoSuchTable(table.to_string()));
        };
        // Find an index all of whose columns have an equality pair. Prefer
        // wider indexes (more matched columns = more selective).
        let mut best: Option<&IndexDef> = None;
        for def in inner.catalog.indexes.values() {
            if def.table == table
                && def
                    .columns
                    .iter()
                    .all(|c| eqs.iter().any(|(col, _)| col == c))
                && best.is_none_or(|b| def.columns.len() > b.columns.len())
            {
                best = Some(def);
            }
        }
        let Some(def) = best else {
            return Ok(None);
        };
        let Some(idx) = state.indexes.get(&def.name) else {
            return Ok(None);
        };
        let key: Vec<IndexKey> = def
            .columns
            .iter()
            .map(|c| {
                let (_, v) = eqs.iter().find(|(col, _)| col == c).expect("checked");
                IndexKey(v.clone())
            })
            .collect();
        let rows = match idx.map.get(&key) {
            Some(ids) => ids
                .iter()
                .filter_map(|id| state.rows.get(*id).map(|c| (*id, c)))
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

    /// All table definitions, sorted by name.
    pub fn list_tables(&self) -> Vec<Table> {
        let inner = self.inner.lock().unwrap();
        inner.catalog.tables.values().cloned().collect()
    }

    /// All views as `(name, body SQL)` pairs, sorted by name.
    pub fn list_views(&self) -> Vec<(String, String)> {
        let inner = self.inner.lock().unwrap();
        inner
            .catalog
            .views
            .iter()
            .map(|(n, s)| (n.clone(), s.clone()))
            .collect()
    }

    /// All secondary index definitions, sorted by index name.
    pub fn list_indexes(&self) -> Vec<IndexDef> {
        let inner = self.inner.lock().unwrap();
        inner.catalog.indexes.values().cloned().collect()
    }

    /// Atomically reserve `n` auto-increment values for `table`, returning
    /// the first. Values are handed out even if the insert later fails —
    /// gaps are normal auto-increment behavior.
    pub(crate) fn next_auto_block(&self, table: &str, n: i64) -> Result<i64> {
        let mut inner = self.inner.lock().unwrap();
        let state = inner
            .tables
            .get_mut(table)
            .ok_or_else(|| SqlError::NoSuchTable(table.to_string()))?;
        let start = state.next_auto;
        state.next_auto += n;
        Ok(start)
    }

    /// The next auto-increment value a table would assign (for transaction
    /// counter seeding).
    pub(crate) fn peek_next_auto(&self, table: &str) -> Option<i64> {
        let inner = self.inner.lock().unwrap();
        inner.tables.get(table).map(|s| s.next_auto)
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
        Self::apply_live(
            &mut inner.catalog,
            &mut inner.tables,
            &rec,
            inner.disk_first,
        );
        Ok(())
    }

    /// Durably capture current state into `.rdat` snapshots + `catalog.json`,
    /// then truncate the WAL.
    pub fn checkpoint(&self) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        Self::checkpoint_locked(&mut inner)
    }

    fn checkpoint_locked(inner: &mut Inner) -> Result<()> {
        let Inner {
            dir,
            catalog,
            tables,
            wal,
            disk_first,
            ..
        } = inner;

        for (name, state) in tables.iter() {
            storage::write_snapshot(dir, name, state.rows.iter())?;
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

        // Disk-first: adopt the freshly written snapshots as the new bases and
        // drop the RAM overlays they absorbed. On failure the old base +
        // overlay stay live — still correct, just not yet compacted.
        if *disk_first {
            for (name, state) in tables.iter_mut() {
                if let Some(snap) = storage::MappedSnapshot::open(dir, name, state.def.arity())? {
                    state.rows.attach_base(snap);
                }
            }
        }
        Ok(())
    }

    /// Checkpoint if the live WAL has grown past the configured threshold.
    /// Best-effort: the data an auto-checkpoint would compact is already
    /// durable in the WAL, so a failure here only defers compaction.
    fn maybe_checkpoint(&self) {
        let mut inner = self.inner.lock().unwrap();
        if inner.checkpoint_bytes > 0
            && inner.wal.bytes() >= inner.checkpoint_bytes
            && let Err(e) = Self::checkpoint_locked(&mut inner)
        {
            eprintln!("[oxidb-sql] auto-checkpoint failed (will retry): {e}");
        }
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
        // Batch-scoped semantics: a transaction left open at the end of the
        // batch is discarded (auto-rollback), exactly as before interactive
        // transactions existed.
        let mut session_tx = None;
        let result = self.execute_params_in_session(sql, params, &mut session_tx);
        if let Some(id) = session_tx {
            self.rollback_session_txn(id);
        }
        result
    }

    /// Like [`execute_params`](Self::execute_params), but `BEGIN`/`COMMIT`
    /// may span calls: a transaction left open at the end of the batch is
    /// parked in the engine and `*session_tx` carries its id for the next
    /// call (ADR-0013 Phase B — interactive transactions). `SAVEPOINT name`,
    /// `ROLLBACK TO SAVEPOINT name`, and `RELEASE SAVEPOINT name` operate on
    /// the open transaction. A statement error rolls the open transaction
    /// back and clears `*session_tx`.
    pub fn execute_params_in_session(
        &self,
        sql: &str,
        params: &[Value],
        session_tx: &mut Option<u64>,
    ) -> Result<Vec<QueryResult>> {
        // Resume a parked transaction, if the session has one.
        let mut txn: Option<Transaction<'_>> = match *session_tx {
            Some(id) => {
                let state = self
                    .session_txns
                    .lock()
                    .unwrap()
                    .remove(&id)
                    .ok_or_else(|| {
                        SqlError::Unsupported(format!(
                            "no such transaction: {id} (rolled back, committed, or busy)"
                        ))
                    })?;
                Some(Transaction::from_state(self, state))
            }
            None => None,
        };

        let result = self.run_session_batch(sql, params, &mut txn);

        match result {
            Ok(results) => {
                // Park a still-open transaction for the next call.
                match txn {
                    Some(t) => {
                        let id = session_tx.unwrap_or_else(|| {
                            self.next_session_txn
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                        });
                        self.session_txns.lock().unwrap().insert(id, t.into_state());
                        *session_tx = Some(id);
                    }
                    None => *session_tx = None,
                }
                self.maybe_checkpoint();
                Ok(results)
            }
            Err(e) => {
                // A failed statement aborts the transaction (dropped = rolled
                // back); the session starts clean.
                drop(txn);
                *session_tx = None;
                Err(e)
            }
        }
    }

    /// Execute one statement batch against an optional open transaction.
    fn run_session_batch<'a>(
        &'a self,
        sql: &str,
        params: &[Value],
        txn: &mut Option<Transaction<'a>>,
    ) -> Result<Vec<QueryResult>> {
        use ast::Statement;
        let statements = parser::parse(sql)?;
        let mut results = Vec::with_capacity(statements.len());

        for stmt in statements {
            match stmt {
                Statement::Begin => {
                    if txn.is_some() {
                        return Err(SqlError::Unsupported("nested transaction".into()));
                    }
                    *txn = Some(Transaction::new(self));
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
                Statement::Savepoint(name) => {
                    txn.as_ref()
                        .ok_or_else(|| {
                            SqlError::Unsupported("SAVEPOINT outside a transaction".into())
                        })?
                        .savepoint(&name);
                    results.push(QueryResult::Transaction);
                }
                Statement::RollbackToSavepoint(name) => {
                    txn.as_ref()
                        .ok_or_else(|| {
                            SqlError::Unsupported(
                                "ROLLBACK TO SAVEPOINT outside a transaction".into(),
                            )
                        })?
                        .rollback_to_savepoint(&name)?;
                    results.push(QueryResult::Transaction);
                }
                Statement::ReleaseSavepoint(name) => {
                    txn.as_ref()
                        .ok_or_else(|| {
                            SqlError::Unsupported("RELEASE SAVEPOINT outside a transaction".into())
                        })?
                        .release_savepoint(&name)?;
                    results.push(QueryResult::Transaction);
                }
                Statement::Call { name, args } => {
                    // A CALL is atomic: inside an open transaction it simply
                    // joins it; at top level it runs in an implicit one.
                    let r = match &txn {
                        Some(t) => executor::exec_call(t, &name, &args, params)?,
                        None => {
                            let t = Transaction::new(self);
                            let r = executor::exec_call(&t, &name, &args, params)?;
                            t.commit()?;
                            r
                        }
                    };
                    results.push(r);
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
        Ok(results)
    }

    /// `ALTER TABLE` — validate, log, apply, and checkpoint (the checkpoint
    /// rewrites `.rdat` snapshots at the new arity, which disk-first mode's
    /// mmap'd bases depend on).
    pub fn alter_table(&self, table: &str, op: &ast::AlterOp) -> Result<()> {
        use ast::AlterOp;
        let mut inner = self.inner.lock().unwrap();
        let Some(def) = inner.catalog.tables.get(table) else {
            return Err(SqlError::NoSuchTable(table.to_string()));
        };
        match op {
            AlterOp::AddColumn(col) => {
                if def.columns.iter().any(|c| c.name == col.name) {
                    return Err(SqlError::SchemaMismatch(format!(
                        "column {:?} already exists in {table:?}",
                        col.name
                    )));
                }
                if col.primary_key || col.auto_increment {
                    return Err(SqlError::Unsupported(
                        "adding a PRIMARY KEY / AUTO_INCREMENT column".into(),
                    ));
                }
                let has_rows = inner
                    .tables
                    .get(table)
                    .map(|s| s.rows.len() > 0)
                    .unwrap_or(false);
                if !col.nullable && col.default_value.is_none() && has_rows {
                    return Err(SqlError::SchemaMismatch(format!(
                        "cannot add NOT NULL column {:?} without a DEFAULT to a non-empty table",
                        col.name
                    )));
                }
            }
            AlterOp::DropColumn(name) => {
                let Some(pos) = def.columns.iter().position(|c| &c.name == name) else {
                    return Err(SqlError::NoSuchColumn(name.clone()));
                };
                if def.columns[pos].primary_key {
                    return Err(SqlError::Unsupported(
                        "dropping the PRIMARY KEY column".into(),
                    ));
                }
                if let Some(d) = inner
                    .catalog
                    .indexes
                    .values()
                    .find(|d| d.table == table && d.columns.contains(name))
                {
                    return Err(SqlError::SchemaMismatch(format!(
                        "index {:?} depends on column {:?} (drop the index first)",
                        d.name, name
                    )));
                }
            }
            AlterOp::RenameColumn { old, new } => {
                if !def.columns.iter().any(|c| &c.name == old) {
                    return Err(SqlError::NoSuchColumn(old.clone()));
                }
                if def.columns.iter().any(|c| &c.name == new) {
                    return Err(SqlError::SchemaMismatch(format!(
                        "column {new:?} already exists in {table:?}"
                    )));
                }
            }
        }
        let rec = WalRecord::AlterTable {
            table: table.to_string(),
            op: op.clone(),
        };
        let inner = &mut *inner;
        inner.wal.append(&rec)?;
        Self::apply_live(
            &mut inner.catalog,
            &mut inner.tables,
            &rec,
            inner.disk_first,
        );
        Self::checkpoint_locked(inner)
    }

    /// Take a parked session transaction's buffered operations as JSON,
    /// removing the transaction — for a host that replicates the commit
    /// (cluster mode) instead of applying it locally. Applying the returned
    /// ops via [`apply_replicated_txn_ops`](Self::apply_replicated_txn_ops)
    /// on every node (including this one) completes the commit.
    pub fn take_session_txn_ops(&self, id: u64) -> Result<serde_json::Value> {
        let state = self
            .session_txns
            .lock()
            .unwrap()
            .remove(&id)
            .ok_or_else(|| {
                SqlError::Unsupported(format!(
                    "no such transaction: {id} (rolled back, committed, or busy)"
                ))
            })?;
        Ok(serde_json::to_value(state.take_ops())?)
    }

    /// Apply a replicated buffered commit (the ops from
    /// [`take_session_txn_ops`](Self::take_session_txn_ops)) as one atomic
    /// WAL batch. Deterministic: the ops carry final row ids and cells.
    pub fn apply_replicated_txn_ops(&self, ops: &serde_json::Value) -> Result<()> {
        let ops: Vec<WalRecord> = serde_json::from_value(ops.clone())?;
        self.commit_batch(ops)
    }

    /// Roll back (discard) a parked session transaction. Safe to call for
    /// ids that no longer exist.
    pub fn rollback_session_txn(&self, id: u64) {
        self.session_txns.lock().unwrap().remove(&id);
    }

    /// Path to the SQL root directory.
    pub fn dir(&self) -> PathBuf {
        self.inner.lock().unwrap().dir.clone()
    }
}

/// Whether every statement in `sql` is a read (a SELECT — possibly with set
/// operations — or a SHOW/DESCRIBE introspection statement). Callers that
/// gate write access per statement (e.g. a read-only server role) check this
/// before executing.
/// Whether executing `sql` would leave a transaction open at the end of the
/// batch (a `BEGIN` without a matching `COMMIT`/`ROLLBACK`). Used by the
/// cluster session layer, which cannot replicate open-ended transactions.
/// Whether `sql` is exactly one `BEGIN` statement (how a cluster session
/// starts an interactive transaction).
pub fn is_lone_begin(sql: &str) -> bool {
    matches!(parser::parse(sql).as_deref(), Ok([ast::Statement::Begin]))
}

/// Whether `sql` is exactly one `COMMIT` statement (a cluster session's
/// buffered commit is intercepted and replicated).
pub fn is_lone_commit(sql: &str) -> bool {
    matches!(parser::parse(sql).as_deref(), Ok([ast::Statement::Commit]))
}

pub fn leaves_transaction_open(sql: &str) -> Result<bool> {
    let mut open = false;
    for stmt in parser::parse(sql)? {
        match stmt {
            ast::Statement::Begin => open = true,
            ast::Statement::Commit | ast::Statement::Rollback => open = false,
            _ => {}
        }
    }
    Ok(open)
}

pub fn is_read_only(sql: &str) -> Result<bool> {
    Ok(parser::parse(sql)?
        .iter()
        .all(|s| matches!(s, ast::Statement::Select(_) | ast::Statement::Show(_))))
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
        for (_, row) in state.rows.iter() {
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
    fn create_index(&self, name: &str, table: &str, columns: &[String]) -> Result<()> {
        SqlEngine::create_index(self, name, table, columns)
    }
    fn drop_index(&self, name: &str) -> Result<()> {
        SqlEngine::drop_index(self, name)
    }
    fn create_view(&self, name: &str, query_sql: &str, or_replace: bool) -> Result<()> {
        SqlEngine::create_view(self, name, query_sql, or_replace)
    }
    fn drop_view(&self, name: &str) -> Result<()> {
        SqlEngine::drop_view(self, name)
    }
    fn view_sql(&self, name: &str) -> Option<String> {
        SqlEngine::view_sql(self, name)
    }
    fn create_procedure(
        &self,
        name: &str,
        def: catalog::ProcedureDef,
        or_alter: bool,
    ) -> Result<()> {
        SqlEngine::create_procedure(self, name, def, or_alter)
    }
    fn drop_procedure(&self, name: &str) -> Result<()> {
        SqlEngine::drop_procedure(self, name)
    }
    fn procedure_def(&self, name: &str) -> Option<catalog::ProcedureDef> {
        SqlEngine::procedure_def(self, name)
    }
    fn list_procedures(&self) -> Vec<(String, catalog::ProcedureDef)> {
        SqlEngine::list_procedures(self)
    }
    fn next_auto_block(&self, table: &str, n: i64) -> Result<i64> {
        SqlEngine::next_auto_block(self, table, n)
    }
    fn alter_table(&self, table: &str, op: &ast::AlterOp) -> Result<()> {
        SqlEngine::alter_table(self, table, op)
    }
    fn list_tables(&self) -> Vec<Table> {
        SqlEngine::list_tables(self)
    }
    fn list_views(&self) -> Vec<(String, String)> {
        SqlEngine::list_views(self)
    }
    fn list_indexes(&self) -> Vec<IndexDef> {
        SqlEngine::list_indexes(self)
    }
    fn row_count_hint(&self, table: &str) -> Option<usize> {
        SqlEngine::row_count(self, table).ok()
    }
    fn index_lookup_eq(&self, table: &str, eqs: &[(String, Value)]) -> Result<Option<store::Rows>> {
        SqlEngine::index_lookup_eq(self, table, eqs)
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

    fn select_rows(db: &SqlEngine, sql: &str) -> Vec<Vec<Value>> {
        match db.execute(sql).unwrap().pop().unwrap() {
            QueryResult::Select { rows, .. } => rows,
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn least_greatest_ignore_nulls() {
        let dir = tempfile::tempdir().unwrap();
        let db = SqlEngine::open(dir.path()).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, c INT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1,3,9,5),(2,7,NULL,2)")
            .unwrap();
        let rows = select_rows(
            &db,
            "SELECT LEAST(a,b,c) AS lo, GREATEST(a,b,c) AS hi FROM t ORDER BY id",
        );
        assert_eq!(
            rows,
            vec![
                vec![Value::Int(3), Value::Int(9)],
                // row 2: b is NULL and is ignored (not treated as smallest).
                vec![Value::Int(2), Value::Int(7)],
            ]
        );
        // All-NULL → NULL.
        let r = select_rows(&db, "SELECT LEAST(NULL, NULL) AS x FROM t WHERE id=1");
        assert_eq!(r, vec![vec![Value::Null]]);
    }

    #[test]
    fn cte_basic_and_chained() {
        let dir = tempfile::tempdir().unwrap();
        let db = SqlEngine::open(dir.path()).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40)")
            .unwrap();

        // Single CTE.
        let r = select_rows(
            &db,
            "WITH big AS (SELECT id, v FROM t WHERE v >= 30) \
                                  SELECT id FROM big ORDER BY id",
        );
        assert_eq!(r, vec![vec![Value::Int(3)], vec![Value::Int(4)]]);

        // Chained CTE (b references a) + aggregate in the body.
        let r = select_rows(
            &db,
            "WITH a AS (SELECT id, v FROM t WHERE v >= 20), \
                  b AS (SELECT v FROM a WHERE v <= 30) \
             SELECT SUM(v) AS s FROM b",
        );
        assert_eq!(r, vec![vec![Value::Int(50)]]); // 20 + 30
    }

    #[test]
    fn cte_referenced_twice_and_joined() {
        let dir = tempfile::tempdir().unwrap();
        let db = SqlEngine::open(dir.path()).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)")
            .unwrap();
        // `nums` used twice: once for the avg, once joined — the classic
        // "compare each row to the group average" shape.
        let r = select_rows(
            &db,
            "WITH nums AS (SELECT id, v FROM t) \
             SELECT n.id, n.v FROM nums n \
             JOIN (SELECT AVG(v) AS av FROM nums) g ON 1=1 \
             WHERE n.v > g.av ORDER BY n.id",
        );
        assert_eq!(r, vec![vec![Value::Int(3), Value::Int(30)]]); // avg=20, only 30 > 20
    }

    #[test]
    fn distinct_on_keeps_first_per_group() {
        let dir = tempfile::tempdir().unwrap();
        let db = SqlEngine::open(dir.path()).unwrap();
        db.execute("CREATE TABLE sales (id INT PRIMARY KEY, cust INT, amount INT)")
            .unwrap();
        db.execute(
            "INSERT INTO sales VALUES (1,10,5),(2,10,9),(3,10,3),(4,20,7),(5,20,7),(6,30,1)",
        )
        .unwrap();
        // Highest-amount row per customer (argmax) — DISTINCT ON + ORDER BY.
        let rows = select_rows(
            &db,
            "SELECT DISTINCT ON (cust) cust, amount FROM sales \
             ORDER BY cust, amount DESC",
        );
        assert_eq!(
            rows,
            vec![
                vec![Value::Int(10), Value::Int(9)],
                vec![Value::Int(20), Value::Int(7)],
                vec![Value::Int(30), Value::Int(1)],
            ]
        );
    }

    #[test]
    fn distinct_on_over_group_by_is_argmax() {
        let dir = tempfile::tempdir().unwrap();
        let db = SqlEngine::open(dir.path()).unwrap();
        db.execute("CREATE TABLE s (id INT PRIMARY KEY, cust INT, cat TEXT, spend INT)")
            .unwrap();
        // cust 1: books=10, toys=25 → toys.  cust 2: books=8, toys=3 → books.
        db.execute(
            "INSERT INTO s VALUES \
             (1,1,'books',6),(2,1,'books',4),(3,1,'toys',25),\
             (4,2,'books',8),(5,2,'toys',3)",
        )
        .unwrap();
        // Dominant category per customer: GROUP BY (cust,cat) sums, then
        // DISTINCT ON (cust) picks the top by summed spend.
        let rows = select_rows(
            &db,
            "SELECT DISTINCT ON (cust) cust, cat, SUM(spend) AS s \
             FROM s GROUP BY cust, cat ORDER BY cust, SUM(spend) DESC",
        );
        assert_eq!(
            rows,
            vec![
                vec![Value::Int(1), Value::Text("toys".into()), Value::Int(25)],
                vec![Value::Int(2), Value::Text("books".into()), Value::Int(8)],
            ]
        );
    }

    #[test]
    fn mode_within_group_returns_most_frequent() {
        let dir = tempfile::tempdir().unwrap();
        let db = SqlEngine::open(dir.path()).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, g INT, c TEXT)")
            .unwrap();
        db.execute(
            "INSERT INTO t VALUES \
             (1,1,'a'),(2,1,'a'),(3,1,'b'),(4,2,'x'),(5,2,'y'),(6,2,'x')",
        )
        .unwrap();
        let rows = select_rows(
            &db,
            "SELECT g, mode() WITHIN GROUP (ORDER BY c) FROM t GROUP BY g ORDER BY g",
        );
        assert_eq!(
            rows,
            vec![
                vec![Value::Int(1), Value::Text("a".into())],
                vec![Value::Int(2), Value::Text("x".into())],
            ]
        );
    }

    #[test]
    fn mode_ties_break_to_smallest() {
        let dir = tempfile::tempdir().unwrap();
        let db = SqlEngine::open(dir.path()).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
            .unwrap();
        // 5 and 9 each appear twice; the tie breaks to the smaller value.
        db.execute("INSERT INTO t VALUES (1,9),(2,5),(3,9),(4,5),(5,7)")
            .unwrap();
        let rows = select_rows(&db, "SELECT mode() WITHIN GROUP (ORDER BY v) FROM t");
        assert_eq!(rows, vec![vec![Value::Int(5)]]);
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
