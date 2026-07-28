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

use crate::catalog::{IndexDef, Table};
use crate::error::{Result, SqlError};
use crate::store::Store;
use crate::types::{IndexKey, Value};
use crate::wal::WalRecord;
use crate::SqlEngine;

/// A row-level change in the overlay: `Some(cells)` upserts, `None` deletes.
type RowChange = Option<Vec<Value>>;

/// One uniqueness constraint's in-transaction key map: the PRIMARY KEY (its
/// member columns in key order — one for a simple key, several for a composite
/// one) or a single UNIQUE column.
#[derive(Clone)]
struct UniqueMap {
    cols: Vec<usize>,
    is_pk: bool,
    map: BTreeMap<Vec<IndexKey>, u64>,
}

/// Per-table uniqueness maps: the PRIMARY KEY (when there is one) followed by
/// each UNIQUE column.
type UniqueMaps = Vec<UniqueMap>;

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
    /// Uniqueness state per table: one `key tuple -> row_id` map for the
    /// PRIMARY KEY and each UNIQUE column, holding ONLY keys
    /// written by this transaction. Committed base rows are probed through
    /// the engine's persistent maps at check time (with this overlay
    /// superseding base ownership), so cost scales with the transaction's
    /// writes — never with table size.
    pk_seen: BTreeMap<String, UniqueMaps>,
    /// The ordered operations to flush on commit.
    ops: Vec<WalRecord>,
    /// Savepoint stack: `(name, data snapshot)`. Snapshots exclude the stack
    /// itself, so rolling back to a savepoint keeps earlier savepoints.
    savepoints: Vec<(String, Box<TxnState>)>,
    /// This transaction's row-lock owner id (`SELECT ... FOR UPDATE`,
    /// writer-writer exclusion). Allocated at BEGIN, survives parking;
    /// locks release at commit/rollback via `release_all(lock_owner)`.
    lock_owner: u64,
}

impl TxnState {
    /// Consume the state, yielding the ordered operations a commit would
    /// flush (for replicating a buffered commit as one unit).
    pub(crate) fn take_ops(self) -> Vec<WalRecord> {
        self.ops
    }

    pub(crate) fn lock_owner(&self) -> u64 {
        self.lock_owner
    }

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
        let mut state = TxnState::default();
        state.lock_owner = engine.alloc_lock_owner();
        Transaction {
            engine,
            state: RefCell::new(state),
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
        let engine = self.engine;
        let state = self.state.into_inner();
        let owner = state.lock_owner;
        let r = engine.commit_batch_checked(state.ops);
        // Either way the transaction is over: success committed, failure
        // aborted. The locks go with it.
        engine.row_locks_release(owner);
        r
    }

    /// Explicit rollback: discard the overlay, release the row locks. Every
    /// abandonment path must come here (or through the engine's
    /// `rollback_session_txn`) — a bare drop would leak the locks until the
    /// timeout bites someone else.
    pub(crate) fn rollback(self) {
        let owner = self.state.borrow().lock_owner;
        self.engine.row_locks_release(owner);
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

    /// The physical layout `(live_slots, physical_arity)` for `table`, or
    /// `None` when it stores logical == physical (a table created in this
    /// transaction, or one with no dropped columns). Used to translate the
    /// transaction's logical rows/positions into the physical space the
    /// engine's WAL and constraint maps use.
    fn phys_layout(&self, table: &str) -> Option<(Vec<usize>, usize)> {
        if self.state.borrow().created.contains_key(table) {
            return None; // in-txn tables can't have been altered
        }
        match self.engine.table_layout(table) {
            Some((live, arity)) if live.len() != arity => Some((live, arity)),
            _ => None,
        }
    }

    /// Expand a logical row (live columns) into the physical layout the engine
    /// stores — tombstoned slots get a `NULL` placeholder. Identity when the
    /// table has no dropped columns.
    fn to_physical(&self, table: &str, logical: Vec<Value>) -> Vec<Value> {
        match self.phys_layout(table) {
            Some((live, arity)) => {
                let mut phys = vec![Value::Null; arity];
                for (val, &slot) in logical.into_iter().zip(live.iter()) {
                    phys[slot] = val;
                }
                phys
            }
            None => logical,
        }
    }

    /// Allocate a row id for `table`.
    ///
    /// For a table that already exists, the id is **reserved from the engine**
    /// (one lock acquisition, alongside the one the uniqueness check already
    /// takes) rather than counted locally from a peeked value. Reading the
    /// counter and allocating locally handed the same id to any writer that
    /// inserted before this transaction committed, and the commit then
    /// overwrote their row — a silent lost write.
    ///
    /// A table created inside this transaction doesn't exist in the engine
    /// yet, so its ids are private to the overlay and start at 1.
    fn alloc_row_id(&self, table: &str) -> u64 {
        if !self.state.borrow().created.contains_key(table)
            && let Some(id) = self.engine.reserve_row_ids(table, 1)
        {
            return id;
        }
        let mut st = self.state.borrow_mut();
        let next = st.next_row_id.entry(table.to_string()).or_insert(1);
        let id = *next;
        *next += 1;
        id
    }

    /// Enforce PRIMARY KEY / UNIQUE uniqueness for a candidate row against
    /// the rows visible to this transaction. `exclude` is the row being
    /// replaced (for updates). Keys written by this transaction live in the
    /// per-table map; committed base ownership is probed through the engine's
    /// persistent maps, with any overlay entry (rewrite or delete) for the
    /// owning row superseding it — a rewrite that kept the key is caught by
    /// the transaction map.
    fn check_pk(
        &self,
        table: &str,
        def: &Table,
        cells: &[Value],
        exclude: Option<u64>,
    ) -> Result<()> {
        // Constrained keys: the PRIMARY KEY (one entry, however many columns)
        // plus every UNIQUE column.
        let constraints = Self::constraints_of(def);
        if constraints.is_empty() {
            return Ok(());
        }
        self.state
            .borrow_mut()
            .pk_seen
            .entry(table.to_string())
            .or_insert(constraints);

        let dup = |c: &UniqueMap| {
            Err(SqlError::DuplicateKey(format!(
                "{} value {} already exists in {table:?}",
                if c.is_pk { "PRIMARY KEY" } else { "UNIQUE" },
                crate::types::render_key(&c.cols, cells)
            )))
        };
        // The engine's constraint maps key on physical positions; `cols` are
        // logical positions. They differ only when a column has been dropped.
        let layout = self.phys_layout(table);
        let st = self.state.borrow();
        let maps = st.pk_seen.get(table).expect("initialized above");
        let overlay = st.rows.get(table);
        let base_exists = !st.created.contains_key(table);
        for c in maps {
            // NULLs never collide (and PK columns are NOT NULL anyway). For a
            // composite key, a NULL in any member exempts the whole row — SQL's
            // rule for a multi-column UNIQUE.
            if c.cols.iter().any(|&p| matches!(cells[p], Value::Null)) {
                continue;
            }
            let key: Vec<IndexKey> = c.cols.iter().map(|&p| IndexKey(cells[p].clone())).collect();
            // Owned by a row this transaction wrote?
            if let Some(&rid) = c.map.get(&key)
                && Some(rid) != exclude
            {
                return dup(c);
            }
            // Owned by a committed base row this transaction hasn't touched?
            // The engine's constraint maps key on physical positions.
            let owner = if !base_exists {
                None
            } else if c.is_pk {
                // The key is *values*, so no logical->physical translation is
                // needed: a dropped column can never be a PK member, so both
                // sides list the same columns in the same relative order.
                self.engine.pk_owner(table, &key)
            } else {
                let phys_p = layout
                    .as_ref()
                    .map(|(live, _)| live[c.cols[0]])
                    .unwrap_or(c.cols[0]);
                self.engine.unique_owner(table, phys_p, &key[0])
            };
            if let Some(rid) = owner
                && Some(rid) != exclude
                && overlay.is_none_or(|o| !o.contains_key(&rid))
            {
                return dup(c);
            }
        }
        Ok(())
    }

    /// The uniqueness constraints of `def`, as empty in-transaction key maps:
    /// the PRIMARY KEY (all its columns as one key) then each UNIQUE column
    /// that isn't already part of the primary key.
    fn constraints_of(def: &Table) -> UniqueMaps {
        let pk = def.pk_cols();
        let mut out: UniqueMaps = Vec::new();
        if !pk.is_empty() {
            out.push(UniqueMap {
                cols: pk,
                is_pk: true,
                map: BTreeMap::new(),
            });
        }
        for (i, col) in def.columns.iter().enumerate() {
            if col.unique && !col.primary_key {
                out.push(UniqueMap {
                    cols: vec![i],
                    is_pk: false,
                    map: BTreeMap::new(),
                });
            }
        }
        out
    }

    /// Record a write's effect on the transaction's key maps (no-op when the
    /// table has no PK/UNIQUE or nothing constrained was written yet — a
    /// first-op delete needs no map: the overlay entry itself supersedes the
    /// base row's ownership in `check_pk`).
    fn pk_apply(&self, table: &str, _def: &Table, row_id: u64, cells: Option<&[Value]>) {
        let mut st = self.state.borrow_mut();
        let Some(maps) = st.pk_seen.get_mut(table) else {
            return;
        };
        for c in maps.iter_mut() {
            // Any prior key owned by this row is gone (update changes the
            // key, delete removes the row). The map holds only this
            // transaction's writes, so the sweep is small.
            c.map.retain(|_, rid| *rid != row_id);
            if let Some(cells) = cells
                && !c.cols.iter().any(|&p| matches!(cells[p], Value::Null))
            {
                let key: Vec<IndexKey> =
                    c.cols.iter().map(|&p| IndexKey(cells[p].clone())).collect();
                c.map.insert(key, row_id);
            }
        }
    }
}

impl Store for Transaction<'_> {
    fn table_def(&self, name: &str) -> Option<Table> {
        self.visible_def(name)
    }
    fn lock_rows(&self, table: &str, row_ids: &[u64]) -> Result<()> {
        // Held until commit/rollback. Blocks on contention — the engine's
        // `inner` mutex is NOT held here.
        self.engine
            .row_locks_lock(table, row_ids, self.state.borrow().lock_owner)
    }

    // Savepoints operate on this transaction's buffered overlay (see the
    // inherent methods above); exposing them through Store lets `execute`
    // and the Cobra `db` handle reach them uniformly.
    fn savepoint(&self, name: &str) -> Result<()> {
        Transaction::savepoint(self, name);
        Ok(())
    }
    fn rollback_to_savepoint(&self, name: &str) -> Result<()> {
        Transaction::rollback_to_savepoint(self, name)
    }
    fn release_savepoint(&self, name: &str) -> Result<()> {
        Transaction::release_savepoint(self, name)
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
        // The overlay (read back within the txn) holds logical rows; the WAL op
        // (applied at commit) holds the physical layout the engine stores.
        let phys = self.to_physical(table, cells.clone());
        let mut st = self.state.borrow_mut();
        st.rows
            .entry(table.to_string())
            .or_default()
            .insert(row_id, Some(cells));
        st.ops.push(WalRecord::Insert {
            table: table.to_string(),
            row_id,
            cells: phys,
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
        let phys = self.to_physical(table, cells.clone());
        let mut st = self.state.borrow_mut();
        st.rows
            .entry(table.to_string())
            .or_default()
            .insert(row_id, Some(cells));
        st.ops.push(WalRecord::Insert {
            table: table.to_string(),
            row_id,
            cells: phys,
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
        // Constraint keys die with the table — a re-created table must not
        // collide with them.
        st.pk_seen.remove(name);
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

    fn create_procedure(
        &self,
        _name: &str,
        _def: crate::catalog::ProcedureDef,
        _or_alter: bool,
    ) -> Result<()> {
        Err(SqlError::Unsupported(
            "CREATE PROCEDURE inside a transaction".into(),
        ))
    }

    fn drop_procedure(&self, _name: &str) -> Result<()> {
        Err(SqlError::Unsupported(
            "DROP PROCEDURE inside a transaction".into(),
        ))
    }

    fn procedure_def(&self, name: &str) -> Option<crate::catalog::ProcedureDef> {
        self.engine.procedure_def(name)
    }

    fn list_procedures(&self) -> Vec<(String, crate::catalog::ProcedureDef)> {
        self.engine.list_procedures()
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
        // Reserved from the engine, for the same reason as row ids: a peeked
        // counter hands a concurrent writer the same values, and two rows then
        // claim one auto-increment key.
        if !self.state.borrow().created.contains_key(table)
            && let Ok(start) = self.engine.next_auto_block(table, n)
        {
            return Ok(start);
        }
        let mut st = self.state.borrow_mut();
        let next = st.next_auto.entry(table.to_string()).or_insert(1);
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
        table: &str,
        eqs: &[(String, Value)],
    ) -> Result<Option<crate::store::Rows>> {
        let st = self.state.borrow();
        // A table dropped or created in this transaction has no usable base
        // index — fall back to a full (overlay-aware) scan.
        if st.dropped.contains(table) || st.created.contains_key(table) {
            return Ok(None);
        }
        // Candidate rows from the committed base index. `None` means no index
        // covers `eqs`, so let the caller full-scan.
        let Some(base) = self.engine.index_lookup_eq(table, eqs)? else {
            return Ok(None);
        };
        // Merge the overlay. The executor re-applies the WHERE predicate to
        // whatever we return, so a superset is safe: we must not MISS a match,
        // but harmless extras are filtered out. Base candidates touched by the
        // overlay are dropped here and re-added (fresh) from the overlay pass,
        // which also covers inserts and rows an update moved *into* the result.
        let overlay = st.rows.get(table);
        let mut out: crate::store::Rows = Vec::with_capacity(base.len());
        for (rid, cells) in base {
            match overlay.and_then(|o| o.get(&rid)) {
                Some(_) => {} // updated or deleted: handled by the overlay pass
                None => out.push((rid, cells)),
            }
        }
        if let Some(o) = overlay {
            for (rid, change) in o {
                if let Some(cells) = change {
                    out.push((*rid, cells.clone()));
                }
            }
        }
        Ok(Some(out))
    }
}
