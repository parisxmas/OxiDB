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
use crate::types::Value;
use crate::wal::WalRecord;

/// A row-level change in the overlay: `Some(cells)` upserts, `None` deletes.
type RowChange = Option<Vec<Value>>;

#[derive(Default)]
struct TxnState {
    /// Tables created within the transaction (name -> definition).
    created: BTreeMap<String, Table>,
    /// Tables dropped within the transaction.
    dropped: BTreeSet<String>,
    /// Row overlays per table.
    rows: BTreeMap<String, BTreeMap<u64, RowChange>>,
    /// Per-table row-id allocator, seeded lazily from the engine.
    next_row_id: BTreeMap<String, u64>,
    /// Indexes created / dropped within the transaction.
    indexes_created: BTreeMap<String, IndexDef>,
    indexes_dropped: BTreeSet<String>,
    /// The ordered operations to flush on commit.
    ops: Vec<WalRecord>,
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
        let def = self
            .visible_def(table)
            .ok_or_else(|| SqlError::NoSuchTable(table.to_string()))?;
        def.validate_row(&cells)?;
        let row_id = self.alloc_row_id(table);
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
        let def = self
            .visible_def(table)
            .ok_or_else(|| SqlError::NoSuchTable(table.to_string()))?;
        def.validate_row(&cells)?;
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
        if self.visible_def(table).is_none() {
            return Err(SqlError::NoSuchTable(table.to_string()));
        }
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

    fn create_index(&self, name: &str, table: &str, column: &str) -> Result<()> {
        let def = self
            .visible_def(table)
            .ok_or_else(|| SqlError::NoSuchTable(table.to_string()))?;
        if !def.columns.iter().any(|c| c.name == column) {
            return Err(SqlError::NoSuchColumn(column.to_string()));
        }
        let mut st = self.state.borrow_mut();
        if st.indexes_created.contains_key(name) {
            return Err(SqlError::IndexExists(name.to_string()));
        }
        let idx = IndexDef {
            name: name.to_string(),
            table: table.to_string(),
            column: column.to_string(),
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

    fn index_lookup_eq(
        &self,
        _table: &str,
        _column: &str,
        _value: &Value,
    ) -> Result<Option<crate::store::Rows>> {
        // Transactions always full-scan (correct, just not index-accelerated);
        // the overlay makes index reuse across engine+overlay not worth it here.
        Ok(None)
    }
}
