//! Per-engine transactions.
//!
//! A [`Transaction`] borrows the engine and buffers all its writes in an
//! in-memory overlay, so reads within the transaction see its own uncommitted
//! changes (read-your-writes) while the engine's committed state is untouched.
//! On [`commit`](Transaction::commit) the buffered operations are handed to the
//! engine as a single atomic WAL batch (one fsync, all-or-nothing on recovery).
//! Dropping without committing discards the overlay — a rollback.
//!
//! Scope note: Phase 2 transactions assume a single writer for their lifetime
//! (row-id allocation is seeded from the engine at first insert). Concurrent-
//! writer coordination (locking / OCC) is deferred to the server/session layer.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

use crate::SqlEngine;
use crate::catalog::{IndexDef, Table};
use crate::error::{Result, SqlError};
use crate::store::Store;
use crate::types::{IndexKey, Value};
use crate::wal::WalRecord;

/// A row-level change in the overlay: `Some(cells)` upserts, `None` deletes.
type RowChange = Option<Vec<Value>>;

/// Per-table uniqueness maps: `(column position, value -> row_id)` for the
/// PRIMARY KEY and each UNIQUE column.
type UniqueMaps = Vec<(usize, BTreeMap<IndexKey, u64>)>;

#[derive(Default, Clone)]
pub(crate) struct TxnState {
    /// Tables created within the transaction (name -> definition).
    created: BTreeMap<String, Table>,
    /// Tables dropped within the transaction.
    dropped: BTreeSet<String>,
    /// Row overlays per table.
    rows: BTreeMap<String, BTreeMap<u64, RowChange>>,
    /// Per-table row-id allocator, seeded lazily from the engine.
    next_row_id: BTreeMap<String, u64>,
    /// Per-table auto-increment allocator, seeded lazily from the engine.
    /// (Single-writer transactions, like row ids.)
    next_auto: BTreeMap<String, i64>,
    /// Indexes created / dropped within the transaction.
    indexes_created: BTreeMap<String, IndexDef>,
    indexes_dropped: BTreeSet<String>,
    /// Uniqueness state per table, seeded lazily from the visible rows on
    /// first write, then maintained by the overlay: one `(column position,
    /// value -> row_id)` map for the PRIMARY KEY and each UNIQUE column.
    pk_seen: BTreeMap<String, UniqueMaps>,
    /// The ordered operations to flush on commit.
    ops: Vec<WalRecord>,
    /// Savepoint stack: `(name, data snapshot)`. Snapshots exclude the stack
    /// itself, so rolling back to a savepoint keeps earlier savepoints.
    savepoints: Vec<(String, Box<TxnState>)>,
}

impl TxnState {
    /// The transaction's data without its savepoint stack.
    fn data_snapshot(&self) -> TxnState {
        let mut c = self.clone();
        c.savepoints = Vec::new();
        c
    }

    /// Replace the data (not the stack) with a snapshot's.
    fn restore_data(&mut self, snap: &TxnState) {
        let stack = std::mem::take(&mut self.savepoints);
        *self = snap.clone();
        self.savepoints = stack;
    }
}

/// A buffered transaction over a [`SqlEngine`].
pub(crate) struct Transaction<'a> {
    engine: &'a SqlEngine,
    state: RefCell<TxnState>,
}

impl<'a> Transaction<'a> {
    pub(crate) fn new(engine: &'a SqlEngine) -> Self {
        Transaction {
            engine,
            state: RefCell::new(TxnState::default()),
        }
    }

    /// Wrap a previously-suspended session transaction's state.
    pub(crate) fn from_state(engine: &'a SqlEngine, state: TxnState) -> Self {
        Transaction {
            engine,
            state: RefCell::new(state),
        }
    }

    /// Suspend: hand the buffered state back (to be parked in the engine's
    /// session-transaction map between requests).
    pub(crate) fn into_state(self) -> TxnState {
        self.state.into_inner()
    }

    /// `SAVEPOINT name` — snapshot the current buffered data.
    pub(crate) fn savepoint(&self, name: &str) {
        let mut st = self.state.borrow_mut();
        let snap = Box::new(st.data_snapshot());
        st.savepoints.push((name.to_string(), snap));
    }

    /// `ROLLBACK TO SAVEPOINT name` — restore that snapshot's data; the
    /// savepoint itself (and earlier ones) survive, later ones are dropped.
    pub(crate) fn rollback_to_savepoint(&self, name: &str) -> Result<()> {
        let mut st = self.state.borrow_mut();
        let Some(pos) = st.savepoints.iter().rposition(|(n, _)| n == name) else {
            return Err(SqlError::Unsupported(format!("no such savepoint: {name}")));
        };
        let snap = st.savepoints[pos].1.clone();
        st.restore_data(&snap);
        st.savepoints.truncate(pos + 1);
        Ok(())
    }

    /// `RELEASE SAVEPOINT name` — forget the savepoint (and any later ones);
    /// buffered data is untouched.
    pub(crate) fn release_savepoint(&self, name: &str) -> Result<()> {
        let mut st = self.state.borrow_mut();
        let Some(pos) = st.savepoints.iter().rposition(|(n, _)| n == name) else {
            return Err(SqlError::Unsupported(format!("no such savepoint: {name}")));
        };
        st.savepoints.truncate(pos);
        Ok(())
    }

    /// Flush all buffered operations to the engine as one atomic batch.
    pub(crate) fn commit(self) -> Result<()> {
        let ops = self.state.into_inner().ops;
        self.engine.commit_batch(ops)
    }

    /// The definition currently visible to the transaction.
    fn visible_def(&self, name: &str) -> Option<Table> {
        let st = self.state.borrow();
        if st.dropped.contains(name) {
            return None;
        }
        if let Some(def) = st.created.get(name) {
            return Some(def.clone());
        }
        drop(st);
        self.engine.table_def(name)
    }

    /// Seed and bump the row-id allocator for `table`.
    fn alloc_row_id(&self, table: &str) -> u64 {
        let mut st = self.state.borrow_mut();
        let next = match st.next_row_id.get(table) {
            Some(n) => *n,
            None => {
                // Created-in-txn tables start at 1; otherwise continue the
                // engine's sequence.
                if st.created.contains_key(table) {
                    1
                } else {
                    self.engine.peek_next_row_id(table).unwrap_or(1)
                }
            }
        };
        st.next_row_id.insert(table.to_string(), next + 1);
        next
    }

    /// Enforce PRIMARY KEY uniqueness for a candidate row against the rows
    /// visible to this transaction. `exclude` is the row being replaced (for
    /// updates). The per-table key map is seeded from a scan on first use.
    fn check_pk(
        &self,
        table: &str,
        def: &Table,
        cells: &[Value],
        exclude: Option<u64>,
    ) -> Result<()> {
        // Constrained positions: the PRIMARY KEY plus every UNIQUE column.
        let positions: Vec<usize> = def
            .columns
            .iter()
            .enumerate()
            .filter(|(i, c)| c.primary_key || (c.unique && def.pk_pos() != Some(*i)))
            .map(|(i, _)| i)
            .collect();
        if positions.is_empty() {
            return Ok(());
        }
        if !self.state.borrow().pk_seen.contains_key(table) {
            let rows = self.scan(table)?;
            let mut maps: UniqueMaps = positions.iter().map(|&p| (p, BTreeMap::new())).collect();
            for (rid, row) in rows {
                for (p, map) in maps.iter_mut() {
                    if !matches!(row[*p], Value::Null) {
                        map.insert(IndexKey(row[*p].clone()), rid);
                    }
                }
            }
            self.state
                .borrow_mut()
                .pk_seen
                .insert(table.to_string(), maps);
        }
        let st = self.state.borrow();
        let maps = st.pk_seen.get(table).expect("seeded");
        for (p, map) in maps {
            if matches!(cells[*p], Value::Null) {
                continue; // NULLs never collide (and the PK is NOT NULL anyway)
            }
            if let Some(&rid) = map.get(&IndexKey(cells[*p].clone()))
                && Some(rid) != exclude
            {
                let is_pk = def.pk_pos() == Some(*p);
                return Err(SqlError::DuplicateKey(format!(
                    "{} value {:?} already exists in {table:?}",
                    if is_pk { "PRIMARY KEY" } else { "UNIQUE" },
                    cells[*p]
                )));
            }
        }
        Ok(())
    }

    /// Record a write's effect on the seeded PK map (no-op when the table has
    /// no PK or the map was never seeded).
    fn pk_apply(&self, table: &str, _def: &Table, row_id: u64, cells: Option<&[Value]>) {
        let mut st = self.state.borrow_mut();
        let Some(maps) = st.pk_seen.get_mut(table) else {
            return;
        };
        for (p, map) in maps.iter_mut() {
            // Any prior key owned by this row is gone (update changes the
            // key, delete removes the row).
            map.retain(|_, rid| *rid != row_id);
            if let Some(cells) = cells
                && !matches!(cells[*p], Value::Null)
            {
                map.insert(IndexKey(cells[*p].clone()), row_id);
            }
        }
    }
}

impl Store for Transaction<'_> {
    fn table_def(&self, name: &str) -> Option<Table> {
        self.visible_def(name)
    }

    fn scan(&self, table: &str) -> Result<Vec<(u64, Vec<Value>)>> {
        if self.state.borrow().dropped.contains(table) {
            return Err(SqlError::NoSuchTable(table.to_string()));
        }
        // Base rows: empty for tables created in this transaction.
        let created = self.state.borrow().created.contains_key(table);
        let base = if created {
            Vec::new()
        } else {
            self.engine.scan(table)?
        };
        let mut map: BTreeMap<u64, Vec<Value>> = base.into_iter().collect();
        if let Some(overlay) = self.state.borrow().rows.get(table) {
            for (rid, change) in overlay {
                match change {
                    Some(cells) => {
                        map.insert(*rid, cells.clone());
                    }
                    None => {
                        map.remove(rid);
                    }
                }
            }
        }
        Ok(map.into_iter().collect())
    }

    fn insert(&self, table: &str, cells: Vec<Value>) -> Result<u64> {
        let mut cells = cells;
        let def = self
            .visible_def(table)
            .ok_or_else(|| SqlError::NoSuchTable(table.to_string()))?;
        def.coerce_row(&mut cells);
        def.validate_row(&cells)?;
        self.check_pk(table, &def, &cells, None)?;
        let row_id = self.alloc_row_id(table);
        self.pk_apply(table, &def, row_id, Some(&cells));
        let mut st = self.state.borrow_mut();
        st.rows
            .entry(table.to_string())
            .or_default()
            .insert(row_id, Some(cells.clone()));
        st.ops.push(WalRecord::Insert {
            table: table.to_string(),
            row_id,
            cells,
        });
        Ok(row_id)
    }

    fn update_row(&self, table: &str, row_id: u64, cells: Vec<Value>) -> Result<()> {
        let mut cells = cells;
        let def = self
            .visible_def(table)
            .ok_or_else(|| SqlError::NoSuchTable(table.to_string()))?;
        def.coerce_row(&mut cells);
        def.validate_row(&cells)?;
        self.check_pk(table, &def, &cells, Some(row_id))?;
        self.pk_apply(table, &def, row_id, Some(&cells));
        let mut st = self.state.borrow_mut();
        st.rows
            .entry(table.to_string())
            .or_default()
            .insert(row_id, Some(cells.clone()));
        st.ops.push(WalRecord::Insert {
            table: table.to_string(),
            row_id,
            cells,
        });
        Ok(())
    }

    fn delete(&self, table: &str, row_id: u64) -> Result<bool> {
        let Some(def) = self.visible_def(table) else {
            return Err(SqlError::NoSuchTable(table.to_string()));
        };
        self.pk_apply(table, &def, row_id, None);
        let mut st = self.state.borrow_mut();
        let overlay = st.rows.entry(table.to_string()).or_default();
        let existed = !matches!(overlay.get(&row_id), Some(None));
        overlay.insert(row_id, None);
        if existed {
            st.ops.push(WalRecord::Delete {
                table: table.to_string(),
                row_id,
            });
        }
        Ok(existed)
    }

    fn create_table(&self, table: Table) -> Result<()> {
        if self.visible_def(&table.name).is_some() {
            return Err(SqlError::TableExists(table.name));
        }
        let mut st = self.state.borrow_mut();
        st.dropped.remove(&table.name);
        st.created.insert(table.name.clone(), table.clone());
        st.ops.push(WalRecord::CreateTable(table));
        Ok(())
    }

    fn drop_table(&self, name: &str) -> Result<()> {
        if self.visible_def(name).is_none() {
            return Err(SqlError::NoSuchTable(name.to_string()));
        }
        let mut st = self.state.borrow_mut();
        st.created.remove(name);
        st.rows.remove(name);
        st.dropped.insert(name.to_string());
        st.ops.push(WalRecord::DropTable(name.to_string()));
        Ok(())
    }

    fn create_index(&self, name: &str, table: &str, columns: &[String]) -> Result<()> {
        let def = self
            .visible_def(table)
            .ok_or_else(|| SqlError::NoSuchTable(table.to_string()))?;
        for column in columns {
            if !def.columns.iter().any(|c| &c.name == column) {
                return Err(SqlError::NoSuchColumn(column.to_string()));
            }
        }
        let mut st = self.state.borrow_mut();
        if st.indexes_created.contains_key(name) {
            return Err(SqlError::IndexExists(name.to_string()));
        }
        let idx = IndexDef {
            name: name.to_string(),
            table: table.to_string(),
            columns: columns.to_vec(),
        };
        st.indexes_dropped.remove(name);
        st.indexes_created.insert(name.to_string(), idx.clone());
        st.ops.push(WalRecord::CreateIndex(idx));
        Ok(())
    }

    fn drop_index(&self, name: &str) -> Result<()> {
        let mut st = self.state.borrow_mut();
        st.indexes_created.remove(name);
        st.indexes_dropped.insert(name.to_string());
        st.ops.push(WalRecord::DropIndex(name.to_string()));
        Ok(())
    }

    fn alter_table(&self, _table: &str, _op: &crate::ast::AlterOp) -> Result<()> {
        Err(SqlError::Unsupported(
            "ALTER TABLE inside a transaction".into(),
        ))
    }

    fn create_view(&self, _name: &str, _query_sql: &str, _or_replace: bool) -> Result<()> {
        Err(SqlError::Unsupported(
            "CREATE VIEW inside a transaction".into(),
        ))
    }

    fn drop_view(&self, _name: &str) -> Result<()> {
        Err(SqlError::Unsupported(
            "DROP VIEW inside a transaction".into(),
        ))
    }

    fn view_sql(&self, name: &str) -> Option<String> {
        self.engine.view_sql(name)
    }

    fn list_tables(&self) -> Vec<Table> {
        let st = self.state.borrow();
        let mut tables: BTreeMap<String, Table> = self
            .engine
            .list_tables()
            .into_iter()
            .filter(|t| !st.dropped.contains(&t.name))
            .map(|t| (t.name.clone(), t))
            .collect();
        for (name, def) in &st.created {
            tables.insert(name.clone(), def.clone());
        }
        tables.into_values().collect()
    }

    fn next_auto_block(&self, table: &str, n: i64) -> Result<i64> {
        let mut st = self.state.borrow_mut();
        let created = st.created.contains_key(table);
        let next = st.next_auto.entry(table.to_string()).or_insert_with(|| {
            if created {
                1
            } else {
                self.engine.peek_next_auto(table).unwrap_or(1)
            }
        });
        let start = *next;
        *next += n;
        Ok(start)
    }

    fn list_views(&self) -> Vec<(String, String)> {
        // Views can't be created or dropped inside a transaction.
        self.engine.list_views()
    }

    fn list_indexes(&self) -> Vec<IndexDef> {
        let st = self.state.borrow();
        let mut indexes: BTreeMap<String, IndexDef> = self
            .engine
            .list_indexes()
            .into_iter()
            .filter(|d| !st.indexes_dropped.contains(&d.name) && !st.dropped.contains(&d.table))
            .map(|d| (d.name.clone(), d))
            .collect();
        for (name, def) in &st.indexes_created {
            indexes.insert(name.clone(), def.clone());
        }
        indexes.into_values().collect()
    }

    fn index_lookup_eq(
        &self,
        _table: &str,
        _eqs: &[(String, Value)],
    ) -> Result<Option<crate::store::Rows>> {
        // Transactions always full-scan (correct, just not index-accelerated);
        // the overlay makes index reuse across engine+overlay not worth it here.
        Ok(None)
    }
}
