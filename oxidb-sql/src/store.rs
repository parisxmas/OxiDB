//! The storage abstraction the executor runs against.
//!
//! Two things implement [`Store`]: the [`SqlEngine`](crate::SqlEngine) itself
//! (autocommit — every call is applied and logged immediately) and a
//! [`Transaction`](crate::transaction::Transaction) (buffered — calls accumulate
//! in an overlay and are flushed atomically on commit). Making the executor
//! generic over `Store` means the exact same query/DML code runs in both modes.

use crate::catalog::{IndexDef, Table};
use crate::error::Result;
use crate::types::{Value, ValueRef};

/// A set of rows as `(row_id, cells)` pairs.
pub(crate) type Rows = Vec<(u64, Vec<Value>)>;

/// One end of a single-column index range.
///
/// Deliberately not `std::ops::Bound<Value>`: the open end has to survive being
/// compared against a stored cell, and `total_order` (not `PartialOrd`) is what
/// this engine orders index keys by — see `IndexKey`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RangeBound {
    /// No bound on this side.
    Unbounded,
    /// `col >= v` (low side) or `col <= v` (high side).
    Included(Value),
    /// `col > v` (low side) or `col < v` (high side).
    Excluded(Value),
}

impl RangeBound {
    /// Is `v` inside this bound, treated as the **low** end?
    pub fn allows_low(&self, v: &Value) -> bool {
        match self {
            RangeBound::Unbounded => true,
            RangeBound::Included(b) => crate::types::Value::total_order(v, b).is_ge(),
            RangeBound::Excluded(b) => crate::types::Value::total_order(v, b).is_gt(),
        }
    }
    /// Is `v` inside this bound, treated as the **high** end?
    pub fn allows_high(&self, v: &Value) -> bool {
        match self {
            RangeBound::Unbounded => true,
            RangeBound::Included(b) => crate::types::Value::total_order(v, b).is_le(),
            RangeBound::Excluded(b) => crate::types::Value::total_order(v, b).is_lt(),
        }
    }
    pub fn value(&self) -> Option<&Value> {
        match self {
            RangeBound::Unbounded => None,
            RangeBound::Included(v) | RangeBound::Excluded(v) => Some(v),
        }
    }
}

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
        // Clone only the kept columns straight out of the borrowed rows
        // (scan_visit hands them out under the lock without cloning), instead of
        // scan()'s full-row clone followed by a projection that throws the rest
        // away. A wide table read for a few columns (a grouped aggregate over 2
        // of N columns) then clones only what it uses.
        let mut cells = Vec::new();
        let mut n = 0usize;
        self.scan_visit(table, &mut |row| {
            for &k in keep {
                cells.push(row[k].clone());
            }
            n += 1;
            Ok(true)
        })?;
        Ok(Chunk {
            width: keep.len(),
            n,
            cells,
        })
    }
    /// Stream a table's live rows through `visit` (full row cells, in
    /// `row_id` order; return `false` to stop early). The engine
    /// implementation hands out rows **borrowed in place under its lock**,
    /// so a streamed scan clones nothing the visitor doesn't keep — but the
    /// visitor MUST NOT call back into the store (the executor only streams
    /// expressions proven free of subqueries/correlation).
    fn scan_visit(
        &self,
        table: &str,
        visit: &mut dyn FnMut(&[Value]) -> Result<bool>,
    ) -> Result<()> {
        for (_, cells) in self.scan(table)? {
            if !visit(&cells)? {
                break;
            }
        }
        Ok(())
    }
    /// [`scan_visit`](Store::scan_visit) where the caller will only read the
    /// columns in `want`.
    ///
    /// Rows arrive at **full arity with columns in their usual positions**, so
    /// this is a drop-in for `scan_visit`; what changes is that an
    /// implementation may leave the other cells as `Value::Null` instead of
    /// materializing them. In disk-first mode that is the difference between
    /// decoding one integer and decoding five cells with two string
    /// allocations, per row.
    ///
    /// **`want` must cover every column the visitor reads** — including ones it
    /// only reads to evaluate a predicate, not just the ones it keeps. Callers
    /// derive it from `collect_needed`, which walks the projection, filter, join
    /// conditions, GROUP BY, HAVING and ORDER BY, so the set is a superset by
    /// construction. The default implementation ignores `want` entirely, which
    /// is always correct.
    fn scan_visit_cols(
        &self,
        table: &str,
        want: &[usize],
        visit: &mut dyn FnMut(&[Value]) -> Result<bool>,
    ) -> Result<()> {
        let _ = want;
        self.scan_visit(table, visit)
    }

    /// [`scan_visit_cols`](Store::scan_visit_cols) handing the visitor cells
    /// **borrowed** from storage rather than copied out of it, or `false` if this
    /// store cannot (the caller then takes the owned path and gets the same
    /// answers, more slowly).
    ///
    /// Worth using where a scan only *compares* a variable-length cell — grouping
    /// by a text column reads it 400k times and keeps one copy per group — since
    /// materializing text is about 20 ns a cell against 2 ns for a fixed-width
    /// one, two thirds of it the copy.
    ///
    /// Declined rather than emulated when the answer could differ: a table with a
    /// dropped column (stored positions no longer match visible ones) or a
    /// `DECIMAL` column (a decimal cannot be borrowed from an owned `Value`, so
    /// it would read as NULL). Both are decided by the implementation, which is
    /// the only place that knows the layout.
    fn scan_visit_refs(
        &self,
        table: &str,
        want: &[usize],
        visit: &mut dyn FnMut(&[ValueRef<'_>]) -> Result<bool>,
    ) -> Result<bool> {
        let _ = (table, want, visit);
        Ok(false)
    }

    /// Stream a table's live rows as `(row_id, cells)`, in `row_id` order,
    /// stopping when `visit` returns `false`.
    ///
    /// This is [`scan_visit_cols`](Store::scan_visit_cols) for DML, which needs
    /// the row id it is about to write. It exists because the collecting
    /// [`scan`](Store::scan) materializes **the whole table** into an owned
    /// `Vec<(u64, Vec<Value>)>` before the caller can look at a single row — on
    /// a table larger than memory that is not slow, it is fatal, and it happened
    /// for any `DELETE`/`UPDATE` whose predicate was not pure equality.
    ///
    /// Rows are handed over **borrowed under the store's lock**, so the visitor
    /// MUST NOT call back into the store. The executor only streams predicates
    /// it has proven free of subqueries and correlation (`calls_store`).
    ///
    /// `want` names the columns the visitor reads (as in `scan_visit_cols`);
    /// `None` decodes everything.
    fn scan_visit_ids(
        &self,
        table: &str,
        want: Option<&[usize]>,
        visit: &mut dyn FnMut(u64, &[Value]) -> Result<bool>,
    ) -> Result<()> {
        let _ = want;
        for (id, cells) in self.scan(table)? {
            if !visit(id, &cells)? {
                break;
            }
        }
        Ok(())
    }

    /// [`index_visit_eq`](Store::index_visit_eq) handing the visitor the row id
    /// too. `Ok(None)` when no index qualifies.
    fn index_visit_eq_ids(
        &self,
        table: &str,
        eqs: &[(String, Value)],
        visit: &mut dyn FnMut(u64, &[Value]) -> Result<bool>,
    ) -> Result<Option<()>> {
        let Some(rows) = self.index_lookup_eq(table, eqs)? else {
            return Ok(None);
        };
        for (id, cells) in rows {
            if !visit(id, &cells)? {
                break;
            }
        }
        Ok(Some(()))
    }

    /// Stream the rows an index selects for a **range** on one column, as
    /// `(row_id, cells)`. `Ok(None)` when no index can serve it — the caller
    /// then scans, exactly as with the equality form.
    ///
    /// The `.sidx` base is ordered by decoded key tuple and the overlay is a
    /// `BTreeMap`, so both sides answer a range directly; what this cannot do is
    /// promise the rows arrive in key order (base and overlay are merged by row
    /// id), which is why it serves DML and not `ORDER BY`.
    ///
    /// Candidates are verified against the live row before being handed over —
    /// the base is a hint, as everywhere else in this engine.
    fn index_visit_range_ids(
        &self,
        table: &str,
        col: &str,
        lo: &RangeBound,
        hi: &RangeBound,
        visit: &mut dyn FnMut(u64, &[Value]) -> Result<bool>,
    ) -> Result<Option<()>> {
        let _ = (table, col, lo, hi, visit);
        Ok(None)
    }

    /// Record how many rows the last DML statement examined to find its
    /// matches. Diagnostic only — it is what makes "the index served this" and
    /// "this walked the table" distinguishable from outside, including in
    /// tests. Default: ignored.
    fn note_dml_examined(&self, _rows: u64) {}

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
    // Overridden by the engine to batch into one fsync; the default is the contract.
    #[allow(dead_code)]
    /// Delete many rows of one table as a single durable unit (one WAL fsync
    /// where the implementation supports it). Returns rows deleted. The default
    /// falls back to per-row `delete`; the engine overrides it to batch — this
    /// is what makes `ON DELETE CASCADE` one fsync instead of one per child.
    fn delete_many(&self, table: &str, row_ids: &[u64]) -> Result<usize> {
        let mut n = 0;
        for &id in row_ids {
            if self.delete(table, id)? {
                n += 1;
            }
        }
        Ok(n)
    }
    /// Delete `(table, row_id)` pairs that may span several tables as a single
    /// durable unit (one WAL fsync where supported). This is what lets a DELETE
    /// and its whole ON DELETE CASCADE closure — parent and children across
    /// tables — commit in one fsync. The default falls back to per-row delete.
    fn delete_multi(&self, items: &[(String, u64)]) -> Result<usize> {
        let mut n = 0;
        for (table, id) in items {
            if self.delete(table, *id)? {
                n += 1;
            }
        }
        Ok(n)
    }
    /// Pessimistically lock `row_ids` of `table` for this store's lock owner
    /// (the enclosing transaction, or the autocommit statement), blocking up
    /// to the engine's lock timeout on contention. Held until commit/rollback
    /// (transaction) or statement end (autocommit). No default — silently not
    /// locking is the failure mode this feature exists to kill.
    fn lock_rows(&self, table: &str, row_ids: &[u64]) -> Result<()>;

    // Savepoints — meaningful only inside a transaction. The autocommit engine
    // uses the default impls (an error); [`Transaction`] overrides them. This
    // lets a statement or a stored procedure roll back part of its work without
    // aborting the whole transaction. Deterministic (identical per node), so
    // safe on the Raft-replicated CALL path.
    fn savepoint(&self, _name: &str) -> Result<()> {
        Err(crate::error::SqlError::Unsupported(
            "SAVEPOINT requires an open transaction".into(),
        ))
    }
    fn rollback_to_savepoint(&self, _name: &str) -> Result<()> {
        Err(crate::error::SqlError::Unsupported(
            "ROLLBACK TO SAVEPOINT requires an open transaction".into(),
        ))
    }
    fn release_savepoint(&self, _name: &str) -> Result<()> {
        Err(crate::error::SqlError::Unsupported(
            "RELEASE SAVEPOINT requires an open transaction".into(),
        ))
    }

    fn create_table(&self, table: Table) -> Result<()>;
    fn drop_table(&self, name: &str) -> Result<()>;
    /// `ALTER TABLE` — one operation (autocommit only in v1).
    fn alter_table(&self, table: &str, op: &crate::ast::AlterOp) -> Result<()>;
    fn create_index(&self, name: &str, table: &str, columns: &[String], unique: bool)
    -> Result<()>;
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

    /// [`index_lookup_eq`](Store::index_lookup_eq) without building the result.
    ///
    /// `visit` is called with each matching row and returns `false` to stop
    /// early; the outer `Option` is `None` when no index qualifies, exactly as
    /// the collecting form. This exists because a predicate matching 20,000
    /// rows should not cost a 20,000-element vector to answer `count(*)` — the
    /// caller usually folds each row and drops it.
    ///
    /// The default collects and replays, so an implementation that has nothing
    /// better to offer is still correct.
    /// [`index_visit_eq`](Store::index_visit_eq) where the visitor will only
    /// read the columns in `want` — an implementation may hand rows whose other
    /// cells are `Value::Null` placeholders. The default ignores `want`, which
    /// is always correct.
    fn index_visit_eq_cols(
        &self,
        table: &str,
        eqs: &[(String, Value)],
        want: &[usize],
        visit: &mut dyn FnMut(&[Value]) -> Result<bool>,
    ) -> Result<Option<()>> {
        let _ = want;
        self.index_visit_eq(table, eqs, visit)
    }

    fn index_visit_eq(
        &self,
        table: &str,
        eqs: &[(String, Value)],
        visit: &mut dyn FnMut(&[Value]) -> Result<bool>,
    ) -> Result<Option<()>> {
        let Some(rows) = self.index_lookup_eq(table, eqs)? else {
            return Ok(None);
        };
        for (_, cells) in rows {
            if !visit(&cells)? {
                break;
            }
        }
        Ok(Some(()))
    }
}
