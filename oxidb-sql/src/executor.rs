//! Execute logical [`Statement`]s against a [`Store`].
//!
//! A tree-walking interpreter over typed rows. SELECT uses **late
//! materialization**: each scanned table becomes a flat, column-pruned
//! [`Chunk`], and joins combine *row indices* (u32 tuples) instead of copying
//! cell values. Expressions read cells through a [`View`] that maps a bound
//! column position to (table, column) — values are only touched at final
//! projection/aggregation. It is generic over [`Store`], so the identical code
//! runs in autocommit mode (against the engine) and inside a transaction
//! (against the buffered overlay).

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hash, Hasher};

use crate::ast::{
    AggFunc, BinOp, DatePart, Expr, Join, JoinKind, LazyCorrAgg, LimitExpr, QueryBody, QueryResult,
    RecursiveCte, ScalarFunc, SelectItem, SelectQuery, SelectStmt, SetOpKind, ShowKind, Statement,
    TableRef, UnOp, WindowFunc,
};
use crate::decimal::Decimal;
use crate::error::{Result, SqlError};
use crate::store::{Chunk, Store};
use crate::types::{IndexKey, SqlType, Value};

/// A column in a working row set, qualified by its (aliased) table name.
#[derive(Debug, Clone)]
struct ColRef {
    table: String,
    name: String,
    /// The column's declared type when known (table columns); `None` for
    /// view outputs and other untyped sources.
    ty: Option<SqlType>,
}

/// Execute one data/DDL statement. (Transaction control is handled by the
/// engine's execute loop, not here.)
pub(crate) fn execute<S: Store>(
    store: &S,
    stmt: Statement,
    params: &[Value],
) -> Result<QueryResult> {
    // Resolve uncorrelated subqueries to literal values up front, so the rest
    // of the executor only ever sees plain expressions.
    let mut stmt = stmt;
    resolve_subqueries_stmt(store, &mut stmt, params)?;
    match stmt {
        Statement::CreateTable {
            table,
            if_not_exists,
        } => {
            match store.create_table(table) {
                Ok(()) => {}
                Err(SqlError::TableExists(_)) if if_not_exists => {}
                Err(e) => return Err(e),
            }
            Ok(QueryResult::Ddl)
        }
        Statement::DropTable { name, if_exists } => {
            match store.drop_table(&name) {
                Ok(()) => {}
                Err(SqlError::NoSuchTable(_)) if if_exists => {}
                Err(e) => return Err(e),
            }
            Ok(QueryResult::Ddl)
        }
        Statement::CreateIndex {
            name,
            table,
            columns,
            if_not_exists,
        } => {
            match store.create_index(&name, &table, &columns) {
                Ok(()) => {}
                Err(SqlError::IndexExists(_)) if if_not_exists => {}
                Err(e) => return Err(e),
            }
            Ok(QueryResult::Ddl)
        }
        Statement::DropIndex { name, if_exists } => {
            match store.drop_index(&name) {
                Ok(()) => {}
                Err(SqlError::NoSuchIndex(_)) if if_exists => {}
                Err(e) => return Err(e),
            }
            Ok(QueryResult::Ddl)
        }
        Statement::CreateView {
            name,
            query_sql,
            or_replace,
        } => {
            store.create_view(&name, &query_sql, or_replace)?;
            Ok(QueryResult::Ddl)
        }
        Statement::DropView { name, if_exists } => {
            match store.drop_view(&name) {
                Ok(()) => {}
                Err(SqlError::NoSuchView(_)) if if_exists => {}
                Err(e) => return Err(e),
            }
            Ok(QueryResult::Ddl)
        }
        Statement::Insert {
            table,
            columns,
            rows,
            returning,
        } => exec_insert(store, &table, columns, rows, returning, params),
        Statement::AlterTable { table, op } => {
            store.alter_table(&table, &op)?;
            Ok(QueryResult::Ddl)
        }
        Statement::Select(query) => exec_query(store, query, params),
        Statement::Update {
            table,
            alias,
            assignments,
            filter,
            returning,
        } => exec_update(
            store,
            &table,
            alias.as_deref(),
            assignments,
            filter,
            returning,
            params,
        ),
        Statement::Delete {
            table,
            alias,
            filter,
            returning,
        } => exec_delete(store, &table, alias.as_deref(), filter, returning, params),
        Statement::CreateProcedure {
            name,
            mut def,
            or_alter,
        } => {
            // COBRA (ADR-0014): decode + validate the bytecode NOW, so a bad
            // payload can never reach the catalog (or a mid-call failure).
            if def.language == crate::catalog::ProcLanguage::Cobra {
                crate::cobra::validate_cobra_def(&mut def)?;
            }
            store.create_procedure(&name, def, or_alter)?;
            Ok(QueryResult::Ddl)
        }
        Statement::DropProcedure { name, if_exists } => {
            match store.drop_procedure(&name) {
                Ok(()) => {}
                Err(SqlError::NoSuchProcedure(_)) if if_exists => {}
                Err(e) => return Err(e),
            }
            Ok(QueryResult::Ddl)
        }
        Statement::Call { name, args } => exec_call(store, &name, &args, params),
        // BEGIN/COMMIT/ROLLBACK change the transaction lifecycle and stay
        // session-level. Savepoints operate *within* the current transaction,
        // so they dispatch to the store — which makes them work both at the
        // top level and inside a stored-procedure body (the store is the
        // active transaction there).
        Statement::Begin | Statement::Commit | Statement::Rollback => Err(SqlError::Unsupported(
            "transaction control must be a top-level statement".into(),
        )),
        Statement::Savepoint(name) => {
            store.savepoint(&name)?;
            Ok(QueryResult::Transaction)
        }
        Statement::RollbackToSavepoint(name) => {
            store.rollback_to_savepoint(&name)?;
            Ok(QueryResult::Transaction)
        }
        Statement::ReleaseSavepoint(name) => {
            store.release_savepoint(&name)?;
            Ok(QueryResult::Transaction)
        }
        Statement::Show(kind) => exec_show(store, kind),
    }
}

/// Answer a `SHOW ...` / `DESCRIBE ...` introspection statement from the
/// catalog as an ordinary result set.
fn exec_show<S: Store>(store: &S, kind: ShowKind) -> Result<QueryResult> {
    let text = |s: &str| Value::Text((s.to_string()).into());
    match kind {
        ShowKind::Tables => Ok(QueryResult::Select {
            columns: vec!["table".into(), "rows".into()],
            types: vec![Some(SqlType::Text), Some(SqlType::Int)],
            rows: store
                .list_tables()
                .into_iter()
                .map(|t| {
                    let rows = store
                        .row_count_hint(&t.name)
                        .map_or(Value::Null, |n| Value::Int(n as i64));
                    vec![text(&t.name), rows]
                })
                .collect(),
        }),
        ShowKind::Views => Ok(QueryResult::Select {
            columns: vec!["view".into(), "definition".into()],
            types: vec![Some(SqlType::Text), Some(SqlType::Text)],
            rows: store
                .list_views()
                .into_iter()
                .map(|(name, sql)| vec![Value::Text((name).into()), Value::Text((sql).into())])
                .collect(),
        }),
        ShowKind::Procedures => Ok(QueryResult::Select {
            columns: vec![
                "procedure".into(),
                "params".into(),
                "language".into(),
                "definition".into(),
            ],
            types: vec![Some(SqlType::Text); 4],
            rows: store
                .list_procedures()
                .into_iter()
                .map(|(name, def)| {
                    let params = def
                        .params
                        .iter()
                        .map(|(n, t)| format!("{n} {}", format!("{t:?}").to_uppercase()))
                        .collect::<Vec<_>>()
                        .join(", ");
                    // For cobra: the stored `.cobra` source if it was
                    // supplied (so tooling can show/edit it), otherwise the
                    // `<cobra bytecode, N bytes>` placeholder. SQL bodies use
                    // their text as before.
                    let definition = if def.language == crate::catalog::ProcLanguage::Cobra
                        && !def.source.is_empty()
                    {
                        def.source
                    } else {
                        def.body
                    };
                    vec![
                        Value::Text((name).into()),
                        Value::Text((params).into()),
                        text(def.language.as_str()),
                        Value::Text((definition).into()),
                    ]
                })
                .collect(),
        }),
        ShowKind::Indexes(table) => {
            if let Some(t) = &table
                && store.table_def(t).is_none()
            {
                return Err(SqlError::NoSuchTable(t.clone()));
            }
            Ok(QueryResult::Select {
                columns: vec!["index".into(), "table".into(), "columns".into()],
                types: vec![Some(SqlType::Text); 3],
                rows: store
                    .list_indexes()
                    .into_iter()
                    .filter(|d| table.as_ref().is_none_or(|t| &d.table == t))
                    .map(|d| vec![text(&d.name), text(&d.table), text(&d.columns.join(", "))])
                    .collect(),
            })
        }
        ShowKind::Columns(table) => {
            let def = store
                .table_def(&table)
                .ok_or_else(|| SqlError::NoSuchTable(table.clone()))?;
            Ok(QueryResult::Select {
                columns: vec![
                    "column".into(),
                    "type".into(),
                    "nullable".into(),
                    "primary_key".into(),
                    "auto_increment".into(),
                ],
                types: vec![
                    Some(SqlType::Text),
                    Some(SqlType::Text),
                    Some(SqlType::Bool),
                    Some(SqlType::Bool),
                    Some(SqlType::Bool),
                ],
                rows: def
                    .columns
                    .iter()
                    .map(|c| {
                        // Report what was declared, so DESCRIBE round-trips
                        // `VARCHAR(n)` and `SMALLINT` rather than the storage
                        // type underneath them.
                        let ty = match c.max_len {
                            Some(n) => format!("VARCHAR({n})"),
                            None => c.type_name().to_string(),
                        };
                        vec![
                            text(&c.name),
                            text(&ty),
                            Value::Bool(c.nullable),
                            Value::Bool(c.primary_key),
                            Value::Bool(c.auto_increment),
                        ]
                    })
                    .collect(),
            })
        }
    }
}

/// The empty input row VALUES expressions are evaluated against.
const NO_ROW: &[Value] = &[];

/// A fast, non-cryptographic hasher (fxhash-style multiply-rotate) for the
/// executor's internal hash maps. Join/group hashing is on the per-row hot
/// path, where SipHash's per-write cost dominates; DoS-resistance is not
/// needed for these transient, query-local tables.
#[derive(Default)]
struct FxHasher(u64);

impl FxHasher {
    #[inline]
    fn add(&mut self, n: u64) {
        self.0 = (self.0.rotate_left(5) ^ n).wrapping_mul(0x51_7c_c1_b7_27_22_0a_95);
    }
}

impl Hasher for FxHasher {
    #[inline]
    fn finish(&self) -> u64 {
        // xor-shift-multiply finalizer: the accumulator's entropy sits in the
        // high bits, but the hash table picks buckets from the low bits (and
        // f64-bit keys of small integers have dozens of trailing zeros) — mix
        // high bits down before handing the hash out.
        let mut x = self.0;
        x ^= x >> 32;
        x = x.wrapping_mul(0xd6e8_feb8_6659_fd93);
        x ^= x >> 32;
        x
    }
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.add(b as u64);
        }
    }
    #[inline]
    fn write_u8(&mut self, n: u8) {
        self.add(n as u64);
    }
    #[inline]
    fn write_u32(&mut self, n: u32) {
        self.add(n as u64);
    }
    #[inline]
    fn write_u64(&mut self, n: u64) {
        self.add(n);
    }
    #[inline]
    fn write_usize(&mut self, n: usize) {
        self.add(n as u64);
    }
}

type FxMap<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher>>;

/// Run a stored procedure: evaluate the arguments (literals or the caller's
/// bind parameters — column references are rejected by binding against an
/// empty schema), coerce them to the declared parameter types, and execute
/// the stored body with the arguments as its `$N` parameters. The result is
/// the last statement's result. Atomicity is the caller's job: `lib.rs`
/// wraps a top-level CALL in an implicit transaction.
pub(crate) fn exec_call<S: Store>(
    store: &S,
    name: &str,
    args: &[Expr],
    params: &[Value],
) -> Result<QueryResult> {
    // Evaluate the argument expressions against the caller's parameters (a
    // nested CALL's args may reference the outer procedure's $N). Coercion to
    // the declared types happens in exec_call_values, once the def is known.
    let empty: &[ColRef] = &[];
    let no_row: &[Value] = &[];
    let mut values = Vec::with_capacity(args.len());
    for arg in args {
        let bound = bind_expr(arg, empty)?;
        values.push(eval_scalar(&bound, empty, no_row, params)?);
    }
    exec_call_values(store, name, values)
}

/// Nested-CALL recursion guard. Both the SQL-text body path (a `CALL` in the
/// body flows through `execute` → `exec_call`) and the Cobra `db.call` handle
/// go through [`exec_call_values`], so one thread-local depth counter bounds
/// every recursion shape. A procedure and everything it calls share the
/// caller's transaction.
const MAX_CALL_DEPTH: u32 = 64;

thread_local! {
    static CALL_DEPTH: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

struct DepthGuard;
impl DepthGuard {
    fn enter() -> Result<DepthGuard> {
        let d = CALL_DEPTH.with(|c| {
            let d = c.get() + 1;
            c.set(d);
            d
        });
        if d > MAX_CALL_DEPTH {
            CALL_DEPTH.with(|c| c.set(c.get().saturating_sub(1)));
            return Err(SqlError::Eval(format!(
                "maximum procedure call depth ({MAX_CALL_DEPTH}) exceeded (infinite recursion?)"
            )));
        }
        Ok(DepthGuard)
    }
}
impl Drop for DepthGuard {
    fn drop(&mut self) {
        CALL_DEPTH.with(|c| c.set(c.get().saturating_sub(1)));
    }
}

/// Run a procedure with pre-evaluated argument values (the shared core of a
/// top-level/nested `CALL` and the Cobra `db.call` handle). Coerces the values
/// to the declared parameter types, then dispatches to the SQL-text body or
/// the Cobra VM.
pub(crate) fn exec_call_values<S: Store>(
    store: &S,
    name: &str,
    values: Vec<Value>,
) -> Result<QueryResult> {
    let _guard = DepthGuard::enter()?;

    let def = store
        .procedure_def(name)
        .ok_or_else(|| SqlError::NoSuchProcedure(name.to_string()))?;
    if values.len() != def.params.len() {
        return Err(SqlError::Eval(format!(
            "procedure {name:?} takes {} argument(s), got {}",
            def.params.len(),
            values.len()
        )));
    }
    let mut coerced = Vec::with_capacity(values.len());
    for (v, (pname, ty)) in values.into_iter().zip(&def.params) {
        coerced.push(coerce_call_arg(v, *ty, pname)?);
    }

    // A COBRA procedure executes its stored bytecode (ADR-0014); the SQL
    // path below re-parses the stored text as before.
    if def.language == crate::catalog::ProcLanguage::Cobra {
        return crate::cobra::exec_call_cobra(store, name, &def, coerced);
    }

    let statements = crate::parser::parse(&def.body)?;
    let mut last = QueryResult::Ddl;
    for stmt in statements {
        // Body statements were restricted at creation; keep the check as a
        // defensive backstop. A nested CALL recurses through execute →
        // exec_call (bounded by the depth guard above).
        match &stmt {
            Statement::Insert { .. }
            | Statement::Update { .. }
            | Statement::Delete { .. }
            | Statement::Select(_)
            | Statement::Savepoint(_)
            | Statement::RollbackToSavepoint(_)
            | Statement::ReleaseSavepoint(_)
            | Statement::Call { .. } => {}
            _ => {
                return Err(SqlError::Corrupt(format!(
                    "procedure {name:?} body contains a disallowed statement"
                )));
            }
        }
        last = execute(store, stmt, &coerced)?;
    }
    Ok(last)
}

/// Coerce a CALL argument to the declared parameter type (the same implicit
/// widenings column writes get); reject clear mismatches early so the error
/// names the parameter instead of failing mid-body.
fn coerce_call_arg(v: Value, ty: SqlType, pname: &str) -> Result<Value> {
    let coerced = match (ty, v) {
        (_, Value::Null) => Value::Null,
        (SqlType::Double, Value::Int(i)) => Value::Double(i as f64),
        (SqlType::Timestamp, Value::Int(i)) => Value::Timestamp(i),
        (SqlType::Int, v @ Value::Int(_))
        | (SqlType::Double, v @ Value::Double(_))
        | (SqlType::Text, v @ Value::Text(_))
        | (SqlType::Bool, v @ Value::Bool(_))
        | (SqlType::Timestamp, v @ Value::Timestamp(_))
        | (SqlType::Blob, v @ Value::Bytes(_))
        | (SqlType::Decimal, v @ Value::Decimal(_)) => v,
        // DECIMAL parameters accept exact widenings (Int/Text) and a
        // best-effort Double conversion; a Decimal into a DOUBLE drops to float.
        (SqlType::Decimal, Value::Int(i)) => Value::Decimal(Box::new(Decimal::from_i64(i))),
        (SqlType::Decimal, Value::Text(t)) => match Decimal::parse(&t) {
            Some(d) => Value::Decimal(Box::new(d)),
            None => {
                return Err(SqlError::Eval(format!(
                    "parameter {pname:?}: invalid DECIMAL {t:?}"
                )));
            }
        },
        (SqlType::Decimal, Value::Double(f)) => match Decimal::parse(&format!("{f}")) {
            Some(d) => Value::Decimal(Box::new(d)),
            None => {
                return Err(SqlError::Eval(format!(
                    "parameter {pname:?}: cannot convert {f} to DECIMAL"
                )));
            }
        },
        (SqlType::Double, Value::Decimal(d)) => Value::Double(d.to_f64()),
        (SqlType::Blob, Value::Text(t)) => match crate::catalog::base64_decode(&t) {
            Ok(b) => Value::Bytes((b).into()),
            Err(()) => {
                return Err(SqlError::Eval(format!(
                    "parameter {pname:?}: invalid base64 for BLOB"
                )));
            }
        },
        (ty, v) => {
            return Err(SqlError::Eval(format!(
                "parameter {pname:?} expects {ty:?}, got {v:?}"
            )));
        }
    };
    Ok(coerced)
}

// ── FOREIGN KEY enforcement ─────────────────────────────────────────────────
//
// Basic single-column referential integrity. Child side (INSERT/UPDATE): a
// non-NULL foreign-key value must find its parent row. Parent side (DELETE):
// honour ON DELETE NO ACTION/RESTRICT (reject), CASCADE (delete children), or
// SET NULL (null children). Parent side (UPDATE of a referenced key): reject
// while children still point at the old value. Multi-column FKs are parsed but
// not represented, so not enforced (documented).

/// The parent column an FK references — explicit, or the parent's PRIMARY KEY
/// when the FK wrote `REFERENCES parent` without naming a column.
fn fk_parent_column<S: Store>(store: &S, fk: &crate::catalog::ForeignKey) -> Result<String> {
    if !fk.parent_column.is_empty() {
        return Ok(fk.parent_column.clone());
    }
    let pdef = store
        .table_def(&fk.parent_table)
        .ok_or_else(|| SqlError::NoSuchTable(fk.parent_table.clone()))?;
    match pdef.pk_cols().as_slice() {
        [p] => Ok(pdef.columns[*p].name.clone()),
        [] => Err(SqlError::Unsupported(format!(
            "FOREIGN KEY references {} which has no PRIMARY KEY",
            fk.parent_table
        ))),
        // A single-column FK can't stand in for a composite key; there is no
        // one parent column to resolve to.
        _ => Err(SqlError::Unsupported(format!(
            "FOREIGN KEY references {} whose PRIMARY KEY is composite \
             (name the parent column explicitly)",
            fk.parent_table
        ))),
    }
}

fn fk_col_pos(def: &crate::catalog::Table, col: &str) -> Result<usize> {
    def.columns
        .iter()
        .position(|c| c.name == col)
        .ok_or_else(|| SqlError::NoSuchColumn(col.to_string()))
}

/// Rows of `table` whose `column` equals `value` — index-accelerated when an
/// index on `column` exists, else a scan.
fn fk_rows_where_eq<S: Store>(
    store: &S,
    table: &str,
    column: &str,
    value: &Value,
) -> Result<crate::store::Rows> {
    if let Some(rows) = store.index_lookup_eq(table, &[(column.to_string(), value.clone())])? {
        return Ok(rows);
    }
    let def = store
        .table_def(table)
        .ok_or_else(|| SqlError::NoSuchTable(table.to_string()))?;
    let pos = fk_col_pos(&def, column)?;
    Ok(store
        .scan(table)?
        .into_iter()
        .filter(|(_, cells)| cells.get(pos).is_some_and(|v| v == value))
        .collect())
}

/// Child side: every non-NULL foreign-key value in `cells` must find its
/// parent row. Called before an INSERT or UPDATE writes the row.
fn check_fk_children<S: Store>(
    store: &S,
    def: &crate::catalog::Table,
    cells: &[Value],
) -> Result<()> {
    for fk in &def.foreign_keys {
        let Ok(pos) = fk_col_pos(def, &fk.column) else {
            continue;
        };
        let val = &cells[pos];
        if matches!(val, Value::Null) {
            continue; // a NULL foreign key references nothing (standard SQL)
        }
        let pcol = fk_parent_column(store, fk)?;
        if fk_rows_where_eq(store, &fk.parent_table, &pcol, val)?.is_empty() {
            return Err(SqlError::ForeignKeyViolation(format!(
                "{}.{} = {:?} has no matching row in {}.{}",
                def.name, fk.column, val, fk.parent_table, pcol
            )));
        }
    }
    Ok(())
}

/// (child table def, fk) for every FK in the catalog referencing `parent`.
fn referencing_fks<S: Store>(
    store: &S,
    parent: &str,
) -> Vec<(crate::catalog::Table, crate::catalog::ForeignKey)> {
    let mut out = Vec::new();
    for t in store.list_tables() {
        for fk in &t.foreign_keys {
            if fk.parent_table.eq_ignore_ascii_case(parent) {
                out.push((t.clone(), fk.clone()));
            }
        }
    }
    out
}

/// Parent side on DELETE: honour each referencing FK's ON DELETE action for a
/// parent row being removed. Recurses so a CASCADE chain is fully applied.
/// Parent side on DELETE. NO ACTION/RESTRICT errors if referenced. CASCADE
/// *collects* the referencing child rows (and, recursively, their descendants)
/// into `dels` — the caller deletes the whole closure in one batch — so a
/// cascade costs one fsync, not one per row. SET NULL nulls the child FK inline
/// (rare; not part of the delete batch).
fn enforce_fk_on_delete<S: Store>(
    store: &S,
    parent_def: &crate::catalog::Table,
    parent_cells: &[Value],
    dels: &mut Vec<(String, u64)>,
) -> Result<()> {
    for (child_def, fk) in referencing_fks(store, &parent_def.name) {
        let pcol = fk_parent_column(store, &fk)?;
        let refval = &parent_cells[fk_col_pos(parent_def, &pcol)?];
        if matches!(refval, Value::Null) {
            continue;
        }
        let children = fk_rows_where_eq(store, &child_def.name, &fk.column, refval)?;
        if children.is_empty() {
            continue;
        }
        match fk.on_delete {
            crate::catalog::FkAction::NoAction => {
                return Err(SqlError::ForeignKeyViolation(format!(
                    "{} row(s) in {} still reference {}.{} = {:?}",
                    children.len(),
                    child_def.name,
                    parent_def.name,
                    pcol,
                    refval
                )));
            }
            crate::catalog::FkAction::Cascade => {
                for (rid, cells) in &children {
                    enforce_fk_on_delete(store, &child_def, cells, dels)?; // grandchildren
                    dels.push((child_def.name.clone(), *rid));
                }
            }
            crate::catalog::FkAction::SetNull => {
                let cpos = fk_col_pos(&child_def, &fk.column)?;
                for (rid, mut cells) in children {
                    cells[cpos] = Value::Null;
                    store.update_row(&child_def.name, rid, cells)?;
                }
            }
        }
    }
    Ok(())
}

/// Parent side on UPDATE: restrict changing a referenced key while children
/// still point at the old value (basic: NO ACTION/RESTRICT for updates).
fn enforce_fk_on_parent_update<S: Store>(
    store: &S,
    parent_def: &crate::catalog::Table,
    old_cells: &[Value],
    new_cells: &[Value],
) -> Result<()> {
    for (child_def, fk) in referencing_fks(store, &parent_def.name) {
        let pcol = fk_parent_column(store, &fk)?;
        let ppos = fk_col_pos(parent_def, &pcol)?;
        let (oldv, newv) = (&old_cells[ppos], &new_cells[ppos]);
        if oldv == newv || matches!(oldv, Value::Null) {
            continue; // referenced value unchanged (or was NULL)
        }
        if !fk_rows_where_eq(store, &child_def.name, &fk.column, oldv)?.is_empty() {
            return Err(SqlError::ForeignKeyViolation(format!(
                "cannot change {}.{} from {:?} — row(s) in {} reference it",
                parent_def.name, pcol, oldv, child_def.name
            )));
        }
    }
    Ok(())
}

fn exec_insert<S: Store>(
    store: &S,
    table: &str,
    columns: Option<Vec<String>>,
    rows: Vec<Vec<Expr>>,
    returning: Option<Vec<SelectItem>>,
    params: &[Value],
) -> Result<QueryResult> {
    let def = store
        .table_def(table)
        .ok_or_else(|| SqlError::NoSuchTable(table.to_string()))?;

    // Resolve the (optional) column list to cell positions once.
    let col_indexes: Option<Vec<usize>> = match &columns {
        Some(cols) => Some(
            cols.iter()
                .map(|name| {
                    def.columns
                        .iter()
                        .position(|c| &c.name == name)
                        .ok_or_else(|| SqlError::NoSuchColumn(name.clone()))
                })
                .collect::<Result<_>>()?,
        ),
        None => None,
    };

    let mut all_cells = Vec::with_capacity(rows.len());
    for row_exprs in rows {
        // VALUES cannot reference columns; evaluate against an empty row.
        let values: Vec<Value> = row_exprs
            .iter()
            .map(|e| eval_scalar(e, &[], NO_ROW, params))
            .collect::<Result<_>>()?;

        let cells = match &col_indexes {
            Some(idxs) => {
                if idxs.len() != values.len() {
                    return Err(SqlError::SchemaMismatch(format!(
                        "INSERT has {} columns but {} values",
                        idxs.len(),
                        values.len()
                    )));
                }
                // Omitted columns get their DEFAULT (NULL when none).
                let mut cells: Vec<Value> = def
                    .columns
                    .iter()
                    .map(|c| c.default_value.clone().unwrap_or(Value::Null))
                    .collect();
                for (idx, val) in idxs.iter().zip(values) {
                    cells[*idx] = val;
                }
                cells
            }
            None => values,
        };
        all_cells.push(cells);
    }

    // AUTO_INCREMENT: rows that omit the column (or pass NULL) get values
    // from the table's counter, reserved as one atomic block.
    let mut last_insert_id = None;
    if let Some(p) = def.columns.iter().position(|c| c.auto_increment) {
        let missing = all_cells.iter().filter(|c| c[p] == Value::Null).count();
        if missing > 0 {
            let mut next = store.next_auto_block(table, missing as i64)?;
            for cells in &mut all_cells {
                if cells[p] == Value::Null {
                    cells[p] = Value::Int(next);
                    next += 1;
                }
            }
            last_insert_id = Some(next - 1);
        }
    }

    // FOREIGN KEY: every row's parents must exist before any row is written,
    // so a rejected row leaves the whole INSERT unapplied.
    if !def.foreign_keys.is_empty() {
        for cells in &all_cells {
            check_fk_children(store, &def, cells)?;
        }
    }

    // One durable batch: all rows of a multi-row INSERT share a single fsync.
    let affected = store.insert_many(table, all_cells.clone())? as usize;

    // `RETURNING`: project the inserted rows back as a result set. This is
    // how ADO.NET/EF read generated keys.
    if let Some(items) = returning {
        let schema = table_schema(table, &def);
        return returning_result(&items, &def, &schema, all_cells, params);
    }

    Ok(QueryResult::Mutation {
        affected,
        last_insert_id,
    })
}

/// Project `RETURNING <items>` over the rows an INSERT/UPDATE/DELETE touched.
fn returning_result(
    items: &[SelectItem],
    def: &crate::catalog::Table,
    schema: &[ColRef],
    touched: Vec<Vec<Value>>,
    params: &[Value],
) -> Result<QueryResult> {
    let proj = expand_projection(items, schema)?;
    let columns: Vec<String> = proj.iter().map(|(n, _)| n.clone()).collect();
    let types: Vec<Option<SqlType>> = proj.iter().map(|(_, e)| expr_type(e, schema)).collect();
    let bound: Vec<(String, Expr)> = proj
        .into_iter()
        .map(|(n, e)| Ok::<_, SqlError>((n, bind_expr(&e, schema)?)))
        .collect::<Result<_>>()?;
    let mut out = Vec::with_capacity(touched.len());
    for mut cells in touched {
        // Re-apply coercions the store performed (INT->DOUBLE etc.).
        def.coerce_row(&mut cells);
        let row: Vec<Value> = bound
            .iter()
            .map(|(_, e)| eval_scalar(e, schema, cells.as_slice(), params))
            .collect::<Result<_>>()?;
        out.push(row);
    }
    Ok(QueryResult::Select {
        columns,
        types,
        rows: out,
    })
}

#[allow(clippy::too_many_arguments)]
fn exec_update<S: Store>(
    store: &S,
    table: &str,
    alias: Option<&str>,
    assignments: Vec<(String, Expr)>,
    filter: Option<Expr>,
    returning: Option<Vec<SelectItem>>,
    params: &[Value],
) -> Result<QueryResult> {
    let def = store
        .table_def(table)
        .ok_or_else(|| SqlError::NoSuchTable(table.to_string()))?;
    // With `UPDATE t AS a`, qualified references use the alias.
    let schema = table_schema(alias.unwrap_or(table), &def);
    let targets: Vec<(usize, Expr)> = assignments
        .into_iter()
        .map(|(col, expr)| {
            let idx = def
                .columns
                .iter()
                .position(|c| c.name == col)
                .ok_or_else(|| SqlError::NoSuchColumn(col.clone()))?;
            Ok((idx, expr))
        })
        .collect::<Result<_>>()?;

    // Writer-writer exclusion: every row this UPDATE matches is locked
    // before any assignment is evaluated, so a concurrent FOR UPDATE holder
    // or UPDATE serializes against us instead of losing its write.
    lock_matching_rows(
        store,
        table,
        alias.unwrap_or(table),
        &filter,
        &schema,
        params,
    )?;

    let filter_corr = filter.as_ref().is_some_and(has_corr);
    let targets_corr = targets.iter().any(|(_, e)| has_corr(e));
    let mut affected = 0;
    let mut touched: Vec<Vec<Value>> = Vec::new();
    for (row_id, cells) in dml_candidates(store, table, alias.unwrap_or(table), &filter, params)? {
        if let Some(pred) = &filter
            && !truthy(&eval_scalar_corr(
                store,
                filter_corr,
                pred,
                &schema,
                cells.as_slice(),
                params,
            )?)
        {
            continue;
        }
        let mut new_cells = cells.clone();
        for (idx, expr) in &targets {
            new_cells[*idx] =
                eval_scalar_corr(store, targets_corr, expr, &schema, cells.as_slice(), params)?;
        }
        if returning.is_some() {
            touched.push(new_cells.clone());
        }
        // FOREIGN KEY: the updated row's parents must exist (child side), and a
        // referenced key can't change while children point at the old value.
        if !def.foreign_keys.is_empty() {
            check_fk_children(store, &def, &new_cells)?;
        }
        enforce_fk_on_parent_update(store, &def, &cells, &new_cells)?;
        store.update_row(table, row_id, new_cells)?;
        affected += 1;
    }
    if let Some(items) = returning {
        return returning_result(&items, &def, &schema, touched, params);
    }
    Ok(QueryResult::Mutation {
        affected,
        last_insert_id: None,
    })
}

/// Candidate rows for a DML statement's WHERE: a PK / secondary-index probe
/// when the filter carries a usable equality (`UPDATE t SET ... WHERE id = ?`
/// must not scan the table), else a full scan. A probe may return a superset —
/// the caller re-applies the filter to every candidate row.
/// `SELECT ... FOR UPDATE`: validate the shape, then lock every matched
/// row of the base table until the enclosing transaction commits or rolls
/// back (statement end when autocommit). Only a plain single-table SELECT
/// qualifies — anything whose "rows" are not base-table rows (joins,
/// aggregates, DISTINCT, set operations, views, derived tables) is refused
/// by name, because locking would silently degrade to theater there. With
/// LIMIT/OFFSET, every MATCHING row is locked, not only the returned page —
/// an over-approximation, never an under-approximation.
fn lock_select_for_update<S: Store>(
    store: &S,
    query: &SelectQuery,
    params: &[Value],
) -> Result<()> {
    let unsupported = |what: &str| {
        Err(SqlError::Unsupported(format!(
            "FOR UPDATE on {what} (only a plain single-table SELECT can lock rows)"
        )))
    };
    if !query.ctes.is_empty() {
        return unsupported("a recursive CTE query");
    }
    let QueryBody::Select(stmt) = &query.body else {
        return unsupported("a set operation or VALUES");
    };
    if !stmt.joins.is_empty() {
        return unsupported("a join");
    }
    if !stmt.group_by.is_empty() || stmt.having.is_some() {
        return unsupported("a grouped query");
    }
    if stmt.distinct || !stmt.distinct_on.is_empty() {
        return unsupported("SELECT DISTINCT");
    }
    if stmt.projection.iter().any(|item| match item {
        SelectItem::Expr { expr, .. } => has_aggregate(expr),
        _ => false,
    }) {
        return unsupported("an aggregate query");
    }
    let Some(from) = &stmt.from else {
        return unsupported("a FROM-less SELECT");
    };
    if from.subquery.is_some() {
        return unsupported("a derived table");
    }
    if store.view_sql(&from.name).is_some() {
        return unsupported("a view");
    }
    let def = store
        .table_def(&from.name)
        .ok_or_else(|| SqlError::NoSuchTable(from.name.clone()))?;
    let key = from.alias.as_deref().unwrap_or(&from.name);
    let schema = table_schema(key, &def);
    lock_matching_rows(store, &from.name, key, &stmt.filter, &schema, params)
}

/// Lock every row of `table` the filter currently matches, re-evaluating
/// after each acquisition round until the matched set stabilizes: a row can
/// start (or stop) matching while we wait on a contended lock, so one pass
/// is not enough. Rows locked in an earlier round stay locked even if they
/// no longer match — over-locking is safe, under-locking is the lost-update
/// hole this exists to close. Bounded at 10 rounds: every row matched in
/// the final round is locked either way, because a round only ends by
/// finding nothing new to lock or by locking everything it found.
fn lock_matching_rows<S: Store>(
    store: &S,
    table: &str,
    key: &str,
    filter: &Option<Expr>,
    schema: &[ColRef],
    params: &[Value],
) -> Result<()> {
    let filter_corr = filter.as_ref().is_some_and(has_corr);
    let mut locked: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
    for _ in 0..10 {
        let mut fresh: Vec<u64> = Vec::new();
        for (row_id, cells) in dml_candidates(store, table, key, filter, params)? {
            if let Some(pred) = filter
                && !truthy(&eval_scalar_corr(
                    store,
                    filter_corr,
                    pred,
                    schema,
                    cells.as_slice(),
                    params,
                )?)
            {
                continue;
            }
            if !locked.contains(&row_id) {
                fresh.push(row_id);
            }
        }
        if fresh.is_empty() {
            return Ok(());
        }
        fresh.sort_unstable();
        store.lock_rows(table, &fresh)?;
        locked.extend(fresh);
    }
    Ok(())
}

fn dml_candidates<S: Store>(
    store: &S,
    table: &str,
    key: &str,
    filter: &Option<Expr>,
    params: &[Value],
) -> Result<Vec<(u64, Vec<Value>)>> {
    if let Some(expr) = filter {
        let eqs = eq_conjuncts(expr, key, params);
        if !eqs.is_empty()
            && let Some(rows) = store.index_lookup_eq(table, &eqs)?
        {
            return Ok(rows);
        }
    }
    store.scan(table)
}

fn exec_delete<S: Store>(
    store: &S,
    table: &str,
    alias: Option<&str>,
    filter: Option<Expr>,
    returning: Option<Vec<SelectItem>>,
    params: &[Value],
) -> Result<QueryResult> {
    let def = store
        .table_def(table)
        .ok_or_else(|| SqlError::NoSuchTable(table.to_string()))?;
    // With `DELETE FROM t AS a`, qualified references use the alias.
    let schema = table_schema(alias.unwrap_or(table), &def);

    // Writer-writer exclusion — see exec_update.
    lock_matching_rows(
        store,
        table,
        alias.unwrap_or(table),
        &filter,
        &schema,
        params,
    )?;

    let filter_corr = filter.as_ref().is_some_and(has_corr);
    let mut to_delete = Vec::new();
    for (row_id, cells) in dml_candidates(store, table, alias.unwrap_or(table), &filter, params)? {
        let matches = match &filter {
            Some(pred) => truthy(&eval_scalar_corr(
                store,
                filter_corr,
                pred,
                &schema,
                cells.as_slice(),
                params,
            )?),
            None => true,
        };
        if matches {
            to_delete.push((row_id, cells));
        }
    }
    let mut affected = 0;
    let mut touched: Vec<Vec<Value>> = Vec::new();
    // Collect every row to remove — the matched rows plus each one's ON DELETE
    // CASCADE closure — so the whole DELETE commits in a single WAL fsync.
    let mut dels: Vec<(String, u64)> = Vec::new();
    for (row_id, cells) in to_delete {
        // FK (parent side): RESTRICT errors here; CASCADE collects children
        // into `dels`; SET NULL is applied inline.
        enforce_fk_on_delete(store, &def, &cells, &mut dels)?;
        dels.push((table.to_string(), row_id));
        affected += 1;
        if returning.is_some() {
            touched.push(cells);
        }
    }
    store.delete_multi(&dels)?;
    if let Some(items) = returning {
        return returning_result(&items, &def, &schema, touched, params);
    }
    Ok(QueryResult::Mutation {
        affected,
        last_insert_id: None,
    })
}

// ── SELECT ────────────────────────────────────────────────────────────────

/// Per-table pruned scan chunks plus the layout mapping a combined-schema
/// position to its (table, column) location.
#[derive(Default)]
struct Sources {
    chunks: Vec<Chunk>,
    /// For each combined-schema position: which chunk it lives in.
    table_of: Vec<usize>,
    /// For each combined-schema position: the column inside that chunk.
    col_of: Vec<usize>,
}

impl Sources {
    fn push_table(&mut self, chunk: Chunk) {
        let t = self.chunks.len();
        for c in 0..chunk.width {
            self.table_of.push(t);
            self.col_of.push(c);
        }
        self.chunks.push(chunk);
    }
}

/// The working set of a SELECT: each logical row is a tuple of `stride` row
/// indices (one per joined table), stored flat. `u32::MAX` marks an outer
/// join's NULL side.
struct Tuples {
    stride: usize,
    data: Vec<u32>,
}

impl Tuples {
    #[inline]
    fn n(&self) -> usize {
        self.data.len() / self.stride
    }
    #[inline]
    fn row(&self, i: usize) -> &[u32] {
        &self.data[i * self.stride..(i + 1) * self.stride]
    }
}

/// NULL-row sentinel in a tuple (the padded side of an outer join).
const NULL_ROW: u32 = u32::MAX;

static NULL_VALUE: Value = Value::Null;

/// A logical row: a tuple of per-table row indices viewed through [`Sources`].
#[derive(Clone, Copy)]
struct View<'a> {
    src: &'a Sources,
    tuple: &'a [u32],
}

impl<'a> View<'a> {
    /// Cell at combined-schema position `p`, with the underlying data's
    /// lifetime (outlives this `View` value).
    #[inline]
    fn val_ref(&self, p: usize) -> Option<&'a Value> {
        let t = *self.src.table_of.get(p)?;
        let r = self.tuple[t];
        if r == NULL_ROW {
            return Some(&NULL_VALUE);
        }
        let ch = &self.src.chunks[t];
        Some(&ch.cells[r as usize * ch.width + self.src.col_of[p]])
    }
}

/// Anything an expression can be evaluated over: a plain cell slice or a
/// join-tuple [`View`].
trait RowLike {
    fn val(&self, i: usize) -> Option<&Value>;
}

impl RowLike for [Value] {
    #[inline]
    fn val(&self, i: usize) -> Option<&Value> {
        self.get(i)
    }
}

impl RowLike for View<'_> {
    #[inline]
    fn val(&self, i: usize) -> Option<&Value> {
        self.val_ref(i)
    }
}

/// Resolve a LIMIT/OFFSET operand to a count: literal, or a bound parameter
/// that must be a non-negative integer.
fn resolve_limit(l: Option<LimitExpr>, params: &[Value], what: &str) -> Result<Option<usize>> {
    match l {
        None => Ok(None),
        Some(LimitExpr::Count(n)) => Ok(Some(n)),
        Some(LimitExpr::Param(i)) => match params.get(i) {
            Some(Value::Int(n)) if *n >= 0 => Ok(Some(*n as usize)),
            Some(other) => Err(SqlError::Eval(format!(
                "{what} parameter must be a non-negative integer, got {other:?}"
            ))),
            None => Err(SqlError::Eval(format!(
                "{what} parameter ${} not bound",
                i + 1
            ))),
        },
    }
}

/// Execute a full query: the single-select fast path, or a set-operation tree
/// with outer ORDER BY / LIMIT / OFFSET applied to the combined result.
fn exec_query<S: Store>(store: &S, query: SelectQuery, params: &[Value]) -> Result<QueryResult> {
    if query.for_update {
        lock_select_for_update(store, &query, params)?;
    }
    match query.body {
        QueryBody::Select(s)
            if query.order_by.is_empty() && query.limit.is_none() && query.offset.is_none() =>
        {
            exec_select(store, *s, params)
        }
        body => {
            let (columns, mut rows) = exec_body(store, body, params)?;
            if !query.order_by.is_empty() {
                let keys: Vec<(usize, bool)> = query
                    .order_by
                    .iter()
                    .map(|(e, asc)| Ok::<_, SqlError>((outer_sort_pos(e, &columns)?, *asc)))
                    .collect::<Result<_>>()?;
                rows.sort_by(|a, b| {
                    for &(i, asc) in &keys {
                        let ord = Value::total_order(&a[i], &b[i]);
                        let ord = if asc { ord } else { ord.reverse() };
                        if ord != Ordering::Equal {
                            return ord;
                        }
                    }
                    Ordering::Equal
                });
            }
            let rows: Vec<Vec<Value>> = rows
                .into_iter()
                .skip(resolve_limit(query.offset, params, "OFFSET")?.unwrap_or(0))
                .take(resolve_limit(query.limit, params, "LIMIT")?.unwrap_or(usize::MAX))
                .collect();
            let types = vec![None; columns.len()];
            Ok(QueryResult::Select {
                columns,
                types,
                rows,
            })
        }
    }
}

/// An outer (set-operation) ORDER BY key: a bare output-column name or a
/// 1-based output position.
fn outer_sort_pos(e: &Expr, columns: &[String]) -> Result<usize> {
    match e {
        Expr::Column { table: None, name } => columns
            .iter()
            .position(|c| c == name)
            .ok_or_else(|| SqlError::NoSuchColumn(name.clone())),
        Expr::Literal(Value::Int(n)) if *n >= 1 && (*n as usize) <= columns.len() => {
            Ok(*n as usize - 1)
        }
        _ => Err(SqlError::Unsupported(
            "set-operation ORDER BY must be an output column name or 1-based position".into(),
        )),
    }
}

/// Execute a query body, returning `(columns, rows)`. Set operations take the
/// column names from the left arm; arms must agree on column count.
fn exec_body<S: Store>(
    store: &S,
    body: QueryBody,
    params: &[Value],
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    match body {
        QueryBody::Select(s) => match exec_select(store, *s, params)? {
            QueryResult::Select { columns, rows, .. } => Ok((columns, rows)),
            _ => unreachable!("SELECT produced a non-select result"),
        },
        QueryBody::Values(rows) => {
            // Rows of literal/parameter expressions; PostgreSQL column names.
            let width = rows.first().map(|r| r.len()).unwrap_or(0);
            let columns: Vec<String> = (1..=width).map(|i| format!("column{i}")).collect();
            let empty = Sources::default();
            let view = View {
                src: &empty,
                tuple: &[],
            };
            let out: Vec<Vec<Value>> = rows
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|e| eval_scalar(e, &[], &view, params))
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<_>>()?;
            Ok((columns, out))
        }
        QueryBody::SetOp {
            op,
            all,
            left,
            right,
        } => {
            use std::collections::{BTreeMap, BTreeSet};
            let (columns, mut rows) = exec_body(store, *left, params)?;
            let (rcols, rrows) = exec_body(store, *right, params)?;
            if columns.len() != rcols.len() {
                return Err(SqlError::SchemaMismatch(format!(
                    "{} arms have {} and {} columns",
                    op.name(),
                    columns.len(),
                    rcols.len()
                )));
            }
            // A one-column arm (the common `... EXCEPT SELECT city` shape)
            // keys on the scalar inline — a 1-elem SmallVec stays on the
            // stack, so no per-row heap allocation; wider arms spill to heap
            // exactly like the old Vec key.
            type SetKey = smallvec::SmallVec<[IndexKey; 1]>;
            let key = |row: &[Value]| row.iter().cloned().map(IndexKey).collect::<SetKey>();
            // Bag (ALL) variants count right-arm rows; each count cancels or
            // admits one matching left row. Distinct variants keep the first
            // occurrence, membership-tested against the right arm as a set.
            let mut right_counts: BTreeMap<SetKey, usize> = BTreeMap::new();
            if op != SetOpKind::Union && all {
                for r in &rrows {
                    *right_counts.entry(key(r)).or_insert(0) += 1;
                }
            }
            match (op, all) {
                (SetOpKind::Union, true) => rows.extend(rrows),
                (SetOpKind::Union, false) => {
                    rows.extend(rrows);
                    let mut seen = BTreeSet::new();
                    rows.retain(|row| seen.insert(key(row)));
                }
                (SetOpKind::Except, true) => {
                    rows.retain(|row| match right_counts.get_mut(&key(row)) {
                        Some(n) if *n > 0 => {
                            *n -= 1;
                            false
                        }
                        _ => true,
                    });
                }
                (SetOpKind::Except, false) => {
                    let right: BTreeSet<SetKey> = rrows.iter().map(|r| key(r)).collect();
                    let mut seen = BTreeSet::new();
                    rows.retain(|row| {
                        let k = key(row);
                        !right.contains(&k) && seen.insert(k)
                    });
                }
                (SetOpKind::Intersect, true) => {
                    rows.retain(|row| match right_counts.get_mut(&key(row)) {
                        Some(n) if *n > 0 => {
                            *n -= 1;
                            true
                        }
                        _ => false,
                    });
                }
                (SetOpKind::Intersect, false) => {
                    let right: BTreeSet<SetKey> = rrows.iter().map(|r| key(r)).collect();
                    let mut seen = BTreeSet::new();
                    rows.retain(|row| {
                        let k = key(row);
                        right.contains(&k) && seen.insert(k)
                    });
                }
            }
            Ok((columns, rows))
        }
    }
}

// ── recursive CTEs ──────────────────────────────────────────────────────────

/// Guards against a non-terminating recursion: fail with a clear error
/// instead of hanging or exhausting memory.
const RECURSIVE_MAX_ITERATIONS: usize = 1_000_000;
const RECURSIVE_MAX_ROWS: usize = 10_000_000;

/// Materialize every recursive CTE of `q` by fixpoint iteration and inline
/// the results into the query as VALUES-derived tables, so the rest of the
/// executor only ever sees plain derived tables. No-op when `q.ctes` is
/// empty. A later CTE may reference any earlier one.
fn materialize_recursive_ctes<S: Store>(
    store: &S,
    q: &mut SelectQuery,
    params: &[Value],
) -> Result<()> {
    if q.ctes.is_empty() {
        return Ok(());
    }
    let mut resolved: Vec<(String, SelectQuery, Vec<String>)> = Vec::new();
    for cte in std::mem::take(&mut q.ctes) {
        let name = cte.name.clone();
        let (columns, rows) = fixpoint_cte(store, cte, &resolved, params)?;
        let width = columns.len();
        resolved.push((name, values_query(width, &rows), columns));
    }
    crate::parser::inline_ctes_query(q, &resolved);
    Ok(())
}

/// Evaluate one recursive CTE to a fixpoint: run the anchor, then re-run the
/// step arm with the CTE name bound to the previous iteration's rows until an
/// iteration produces nothing new. UNION (distinct) drops rows already seen —
/// the standard termination device for cyclic data; UNION ALL keeps every row
/// and relies on the step's own termination condition.
fn fixpoint_cte<S: Store>(
    store: &S,
    cte: RecursiveCte,
    earlier: &[(String, SelectQuery, Vec<String>)],
    params: &[Value],
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    use std::collections::BTreeSet;
    let RecursiveCte {
        name,
        columns,
        anchor,
        step,
        union_all,
    } = cte;
    let run = |body: QueryBody,
               binds: &[(String, SelectQuery, Vec<String>)]|
     -> Result<(Vec<String>, Vec<Vec<Value>>)> {
        let mut q = SelectQuery {
            body,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            ctes: Vec::new(),
            for_update: false,
        };
        crate::parser::inline_ctes_query(&mut q, binds);
        match execute(store, Statement::Select(q), params)? {
            QueryResult::Select { columns, rows, .. } => Ok((columns, rows)),
            _ => unreachable!("SELECT produced a non-select result"),
        }
    };

    let (anchor_cols, anchor_rows) = run(anchor, earlier)?;
    let columns = if columns.is_empty() {
        anchor_cols
    } else {
        if columns.len() != anchor_cols.len() {
            return Err(SqlError::SchemaMismatch(format!(
                "recursive CTE {name} declares {} columns but its anchor produces {}",
                columns.len(),
                anchor_cols.len()
            )));
        }
        columns
    };

    let key = |row: &[Value]| row.iter().cloned().map(IndexKey).collect::<Vec<_>>();
    let mut seen: BTreeSet<Vec<IndexKey>> = BTreeSet::new();
    let mut all: Vec<Vec<Value>> = Vec::new();
    let mut working: Vec<Vec<Value>> = Vec::new();
    for row in anchor_rows {
        if union_all || seen.insert(key(&row)) {
            all.push(row.clone());
            working.push(row);
        }
    }

    let mut iterations = 0usize;
    while !working.is_empty() {
        iterations += 1;
        if iterations > RECURSIVE_MAX_ITERATIONS {
            return Err(SqlError::Eval(format!(
                "recursive CTE {name} exceeded {RECURSIVE_MAX_ITERATIONS} iterations — check its termination condition"
            )));
        }
        let mut binds = earlier.to_vec();
        binds.push((
            name.clone(),
            values_query(columns.len(), &working),
            columns.clone(),
        ));
        let (step_cols, step_rows) = run(step.clone(), &binds)?;
        if step_cols.len() != columns.len() {
            return Err(SqlError::SchemaMismatch(format!(
                "recursive CTE {name}: arms have {} and {} columns",
                columns.len(),
                step_cols.len()
            )));
        }
        working = Vec::new();
        for row in step_rows {
            if union_all || seen.insert(key(&row)) {
                all.push(row.clone());
                working.push(row);
            }
        }
        if all.len() > RECURSIVE_MAX_ROWS {
            return Err(SqlError::Eval(format!(
                "recursive CTE {name} exceeded {RECURSIVE_MAX_ROWS} rows"
            )));
        }
    }
    Ok((columns, all))
}

/// A query producing exactly `rows` as literals. Materialized recursive CTEs
/// are substituted into their referencing queries in this form. An empty row
/// set still needs the right width (the derived table's column renames must
/// line up), so it becomes a one-NULL-row VALUES filtered by FALSE.
fn values_query(width: usize, rows: &[Vec<Value>]) -> SelectQuery {
    let plain = |body: QueryBody| SelectQuery {
        body,
        order_by: Vec::new(),
        limit: None,
        offset: None,
        ctes: Vec::new(),
        for_update: false,
    };
    if rows.is_empty() {
        let nulls: Vec<Expr> = (0..width).map(|_| Expr::Literal(Value::Null)).collect();
        return plain(QueryBody::Select(Box::new(SelectStmt {
            distinct: false,
            distinct_on: Vec::new(),
            from: Some(TableRef {
                name: "__empty".into(),
                alias: Some("__empty".into()),
                subquery: Some(Box::new(plain(QueryBody::Values(vec![nulls])))),
                lateral: false,
                alias_columns: Vec::new(),
            }),
            joins: Vec::new(),
            projection: vec![SelectItem::Wildcard],
            filter: Some(Expr::Literal(Value::Bool(false))),
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
        })));
    }
    plain(QueryBody::Values(
        rows.iter()
            .map(|r| r.iter().map(|v| Expr::Literal(v.clone())).collect())
            .collect(),
    ))
}

// ── subquery resolution ─────────────────────────────────────────────────────
//
// Subqueries are handled before row evaluation. An **uncorrelated** subquery
// is executed once and replaced with literals: a scalar subquery becomes
// `Literal` (NULL when it returns no row), `IN (SELECT ...)` becomes a
// literal `In` list. A **correlated** subquery — one whose columns resolve
// against the *enclosing* select's tables rather than its own — has those
// outer references rewritten to synthetic `Param` slots and becomes a
// `CorrScalar` / `CorrIn` node, re-executed per outer row at evaluation time.
// Correlation reaches one level up (a subquery may reference its immediate
// enclosing query only).

/// The tables (key + column names) a column reference can resolve against in
/// one query scope.
struct Scope {
    tables: Vec<(String, Vec<String>)>,
}

impl Scope {
    fn resolves(&self, table: &Option<String>, name: &str) -> bool {
        match table {
            Some(t) => self
                .tables
                .iter()
                .any(|(k, cols)| k == t && cols.iter().any(|c| c == name)),
            None => self
                .tables
                .iter()
                .any(|(_, cols)| cols.iter().any(|c| c == name)),
        }
    }

    /// A scope containing both sides' tables (used when descending through
    /// nested subqueries: intervening scopes shadow the outermost one).
    fn merged(&self, other: &Scope) -> Scope {
        let mut tables = self.tables.clone();
        tables.extend(other.tables.iter().cloned());
        Scope { tables }
    }

    /// A scope over an already-materialized combined schema (used for the
    /// left side of a LATERAL join, which may include derived tables).
    fn from_schema(schema: &[ColRef]) -> Scope {
        let mut tables: Vec<(String, Vec<String>)> = Vec::new();
        for c in schema {
            match tables.iter_mut().find(|(k, _)| *k == c.table) {
                Some((_, cols)) => cols.push(c.name.clone()),
                None => tables.push((c.table.clone(), vec![c.name.clone()])),
            }
        }
        Scope { tables }
    }
}

fn scope_of<S: Store>(store: &S, s: &SelectStmt) -> Scope {
    let mut tables = Vec::new();
    let mut add = |r: &TableRef| {
        // Derived tables resolve to their (aliased) projection columns;
        // base tables to the catalog definition.
        let cols = table_columns(store, r);
        if !cols.is_empty() {
            tables.push((r.key().to_string(), cols));
        }
    };
    if let Some(from) = &s.from {
        add(from);
    }
    for j in &s.joins {
        add(&j.table);
    }
    Scope { tables }
}

/// The output column names of a query body, derived statically from the AST
/// (for name resolution before anything executes). Mirrors the executor's
/// naming: alias > bare column name > the function's default name.
fn derived_columns<S: Store>(store: &S, body: &QueryBody) -> Vec<String> {
    match body {
        QueryBody::Select(s) => {
            let mut cols = Vec::new();
            for item in &s.projection {
                match item {
                    SelectItem::Expr { alias: Some(a), .. } => cols.push(a.clone()),
                    SelectItem::Expr { expr, .. } => cols.push(default_name(expr)),
                    SelectItem::Wildcard => {
                        if let Some(from) = &s.from {
                            cols.extend(table_columns(store, from));
                        }
                        for j in &s.joins {
                            cols.extend(table_columns(store, &j.table));
                        }
                    }
                    SelectItem::QualifiedWildcard(t) => {
                        let matching = s
                            .from
                            .iter()
                            .chain(s.joins.iter().map(|j| &j.table))
                            .find(|r| r.key() == t);
                        if let Some(r) = matching {
                            cols.extend(table_columns(store, r));
                        }
                    }
                }
            }
            cols
        }
        QueryBody::SetOp { left, .. } => derived_columns(store, left),
        QueryBody::Values(rows) => {
            let width = rows.first().map(|r| r.len()).unwrap_or(0);
            (1..=width).map(|i| format!("column{i}")).collect()
        }
    }
}

/// A table reference's column names (catalog for base tables, projection for
/// derived tables).
fn table_columns<S: Store>(store: &S, r: &TableRef) -> Vec<String> {
    if let Some(sub) = &r.subquery {
        if r.alias_columns.is_empty() {
            derived_columns(store, &sub.body)
        } else {
            r.alias_columns.clone()
        }
    } else if let Some(def) = store.table_def(&r.name) {
        def.columns.iter().map(|c| c.name.clone()).collect()
    } else {
        Vec::new()
    }
}

/// The scope of a single-table DML statement: the table's columns, qualified
/// by the alias when one is present (`UPDATE t AS a ... WHERE a.col`).
fn scope_of_dml<S: Store>(store: &S, table: &str, alias: Option<&str>) -> Scope {
    let mut tables = Vec::new();
    if let Some(def) = store.table_def(table) {
        let cols = def.columns.iter().map(|c| c.name.clone()).collect();
        tables.push((alias.unwrap_or(table).to_string(), cols));
    }
    Scope { tables }
}

fn resolve_subqueries_stmt<S: Store>(
    store: &S,
    stmt: &mut Statement,
    params: &[Value],
) -> Result<()> {
    // Synthetic params for correlated outer values are allocated after the
    // user's bind parameters.
    let floor = params.len();
    match stmt {
        Statement::Insert { rows, .. } => {
            // VALUES has no row scope — subqueries here must be uncorrelated.
            for row in rows {
                for e in row {
                    resolve_expr(store, e, params, None, floor)?;
                }
            }
            Ok(())
        }
        Statement::Select(q) => resolve_query(store, q, params, floor),
        Statement::Update {
            table,
            alias,
            assignments,
            filter,
            ..
        } => {
            let scope = scope_of_dml(store, table, alias.as_deref());
            for (_, e) in assignments {
                resolve_expr(store, e, params, Some(&scope), floor)?;
            }
            if let Some(f) = filter {
                resolve_expr(store, f, params, Some(&scope), floor)?;
            }
            Ok(())
        }
        Statement::Delete {
            table,
            alias,
            filter,
            ..
        } => {
            let scope = scope_of_dml(store, table, alias.as_deref());
            if let Some(f) = filter {
                resolve_expr(store, f, params, Some(&scope), floor)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// True when the query contains a bind-parameter slot at or above `floor` —
/// i.e. a synthetic slot for an enclosing scope's value. Such a query must
/// not be materialized at resolve time (the slot is only bound per outer
/// row), even when it has no direct correlation of its own.
fn query_has_param_ge(q: &SelectQuery, floor: usize) -> bool {
    fn expr_has(e: &Expr, floor: usize) -> bool {
        match e {
            Expr::Param(i) => *i >= floor,
            Expr::Binary { left, right, .. } => expr_has(left, floor) || expr_has(right, floor),
            Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } => expr_has(expr, floor),
            Expr::Aggregate { arg, .. } => arg.as_deref().is_some_and(|a| expr_has(a, floor)),
            Expr::Func { args, .. } => args.iter().any(|a| expr_has(a, floor)),
            Expr::In { expr, list, .. } => {
                expr_has(expr, floor) || list.iter().any(|i| expr_has(i, floor))
            }
            Expr::InSet { expr, .. } => expr_has(expr, floor),
            Expr::Subquery(q) => query_has_param_ge(q, floor),
            Expr::InSubquery { expr, query, .. } => {
                expr_has(expr, floor) || query_has_param_ge(query, floor)
            }
            Expr::CorrScalar { query, outer, .. } => {
                outer.iter().any(|o| expr_has(o, floor)) || query_has_param_ge(query, floor)
            }
            Expr::CorrIn {
                expr, query, outer, ..
            } => {
                expr_has(expr, floor)
                    || outer.iter().any(|o| expr_has(o, floor))
                    || query_has_param_ge(query, floor)
            }
            Expr::Window {
                func,
                partition_by,
                order_by,
            } => {
                partition_by.iter().any(|e| expr_has(e, floor))
                    || order_by.iter().any(|(e, _)| expr_has(e, floor))
                    || matches!(func, WindowFunc::Agg(_, Some(a)) if expr_has(a, floor))
            }
            _ => false,
        }
    }
    fn body_has(b: &QueryBody, floor: usize) -> bool {
        match b {
            QueryBody::Select(s) => {
                s.projection.iter().any(
                    |item| matches!(item, SelectItem::Expr { expr, .. } if expr_has(expr, floor)),
                ) || s.filter.as_ref().is_some_and(|e| expr_has(e, floor))
                    || s.joins.iter().any(|j| {
                        expr_has(&j.on, floor)
                            || j.table
                                .subquery
                                .as_deref()
                                .is_some_and(|q| query_has_param_ge(q, floor))
                    })
                    || s.from
                        .as_ref()
                        .and_then(|f| f.subquery.as_deref())
                        .is_some_and(|q| query_has_param_ge(q, floor))
                    || s.group_by.iter().any(|e| expr_has(e, floor))
                    || s.having.as_ref().is_some_and(|e| expr_has(e, floor))
                    || s.order_by.iter().any(|(e, _)| expr_has(e, floor))
            }
            QueryBody::SetOp { left, right, .. } => body_has(left, floor) || body_has(right, floor),
            QueryBody::Values(rows) => rows.iter().flatten().any(|e| expr_has(e, floor)),
        }
    }
    body_has(&q.body, floor)
}

fn resolve_query<S: Store>(
    store: &S,
    q: &mut SelectQuery,
    params: &[Value],
    floor: usize,
) -> Result<()> {
    // A nested WITH RECURSIVE (expression subquery, derived table) is
    // materialized before its body's names are resolved: the CTE arms only
    // ever bind user params, so this is safe at any resolve depth.
    materialize_recursive_ctes(store, q, params)?;
    resolve_body(store, &mut q.body, params, floor)
}

fn resolve_body<S: Store>(
    store: &S,
    body: &mut QueryBody,
    params: &[Value],
    floor: usize,
) -> Result<()> {
    match body {
        QueryBody::Select(s) => resolve_select(store, s, params, floor),
        QueryBody::SetOp { left, right, .. } => {
            resolve_body(store, left, params, floor)?;
            resolve_body(store, right, params, floor)
        }
        QueryBody::Values(rows) => {
            for row in rows {
                for e in row {
                    resolve_expr(store, e, params, None, floor)?;
                }
            }
            Ok(())
        }
    }
}

fn resolve_select<S: Store>(
    store: &S,
    s: &mut SelectStmt,
    params: &[Value],
    floor: usize,
) -> Result<()> {
    // This select's own tables are the outer scope for subqueries in its
    // expressions.
    let scope = scope_of(store, s);
    for item in &mut s.projection {
        if let SelectItem::Expr { expr, .. } = item {
            resolve_expr(store, expr, params, Some(&scope), floor)?;
        }
    }
    if let Some(f) = &mut s.filter {
        resolve_expr(store, f, params, Some(&scope), floor)?;
    }
    for j in &mut s.joins {
        resolve_expr(store, &mut j.on, params, Some(&scope), floor)?;
    }
    for e in &mut s.group_by {
        resolve_expr(store, e, params, Some(&scope), floor)?;
    }
    if let Some(h) = &mut s.having {
        resolve_expr(store, h, params, Some(&scope), floor)?;
    }
    for (e, _) in &mut s.order_by {
        resolve_expr(store, e, params, Some(&scope), floor)?;
    }
    Ok(())
}

fn resolve_expr<S: Store>(
    store: &S,
    e: &mut Expr,
    params: &[Value],
    scope: Option<&Scope>,
    floor: usize,
) -> Result<()> {
    match e {
        Expr::Subquery(q) => {
            let outer = match scope {
                Some(sc) => extract_outer(store, q, sc, floor),
                None => Vec::new(),
            };
            // Nested subqueries inside `q` resolve with q's own selects as
            // their outer scope; their synthetic params start above ours.
            resolve_query(store, q, params, floor + outer.len())?;
            if outer.is_empty() && !query_has_param_ge(q, params.len()) {
                let (columns, mut rows) = run_query_rows(store, (**q).clone(), params)?;
                if columns.len() != 1 {
                    return Err(SqlError::Unsupported(
                        "scalar subquery must return exactly one column".into(),
                    ));
                }
                if rows.len() > 1 {
                    return Err(SqlError::Eval(
                        "scalar subquery returned more than one row".into(),
                    ));
                }
                let v = rows.pop().map(|mut r| r.remove(0)).unwrap_or(Value::Null);
                *e = Expr::Literal(v);
            } else {
                // A single-equality-correlated scalar aggregate decorrelates
                // into one grouped pass + a per-row map lookup, instead of
                // re-running the subquery for every outer row.
                let agg_map = decorrelate_corr_agg(store, q, &outer, floor, params)?;
                *e = Expr::CorrScalar {
                    query: q.clone(),
                    outer,
                    base: floor,
                    agg_map,
                };
            }
            Ok(())
        }
        Expr::InSubquery {
            expr,
            query,
            negated,
        } => {
            resolve_expr(store, expr, params, scope, floor)?;
            let outer = match scope {
                Some(sc) => extract_outer(store, query, sc, floor),
                None => Vec::new(),
            };
            resolve_query(store, query, params, floor + outer.len())?;
            if outer.is_empty() && !query_has_param_ge(query, params.len()) {
                let (columns, rows) = run_query_rows(store, (**query).clone(), params)?;
                if columns.len() != 1 {
                    return Err(SqlError::Unsupported(
                        "IN subquery must return exactly one column".into(),
                    ));
                }
                let list = rows
                    .into_iter()
                    .map(|mut r| Expr::Literal(r.remove(0)))
                    .collect();
                *e = Expr::In {
                    expr: expr.clone(),
                    list,
                    negated: *negated,
                };
            } else {
                // EXISTS-shaped semi-join decorrelation: the parser encodes
                // `EXISTS(...)` as `1 IN (SELECT 1 ... WHERE $slot = key AND
                // residual)`. Lifting the correlation equality out leaves an
                // UNCORRELATED set query — materialized once instead of
                // re-executed per outer row.
                if let Some(rewritten) =
                    decorrelate_exists(store, expr, query, &outer, floor, *negated, params)?
                {
                    *e = rewritten;
                    return Ok(());
                }
                *e = Expr::CorrIn {
                    expr: expr.clone(),
                    query: query.clone(),
                    outer,
                    base: floor,
                    negated: *negated,
                };
            }
            Ok(())
        }
        Expr::In { expr, list, .. } => {
            resolve_expr(store, expr, params, scope, floor)?;
            for item in list {
                resolve_expr(store, item, params, scope, floor)?;
            }
            Ok(())
        }
        Expr::InSet { expr, .. } => resolve_expr(store, expr, params, scope, floor),
        Expr::Binary { left, right, .. } => {
            resolve_expr(store, left, params, scope, floor)?;
            resolve_expr(store, right, params, scope, floor)
        }
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } => {
            resolve_expr(store, expr, params, scope, floor)
        }
        Expr::Aggregate { arg, .. } => match arg {
            Some(a) => resolve_expr(store, a, params, scope, floor),
            None => Ok(()),
        },
        Expr::Func { args, .. } => {
            for a in args {
                resolve_expr(store, a, params, scope, floor)?;
            }
            Ok(())
        }
        Expr::Window {
            func,
            partition_by,
            order_by,
        } => {
            for e in partition_by {
                resolve_expr(store, e, params, scope, floor)?;
            }
            for (e, _) in order_by {
                resolve_expr(store, e, params, scope, floor)?;
            }
            if let WindowFunc::Agg(_, Some(a)) = func {
                resolve_expr(store, a, params, scope, floor)?;
            }
            Ok(())
        }
        Expr::Column { .. }
        | Expr::Col(_)
        | Expr::Literal(_)
        | Expr::Param(_)
        | Expr::CorrScalar { .. }
        | Expr::CorrIn { .. } => Ok(()),
    }
}

/// Find column references inside `q` that do not resolve against the
/// subquery's own tables but do resolve against the enclosing `outer` scope,
/// and rewrite them to `Param(base + k)`. Returns the outer column
/// expressions in slot order (deduplicated).
fn extract_outer<S: Store>(
    store: &S,
    q: &mut SelectQuery,
    outer: &Scope,
    base: usize,
) -> Vec<Expr> {
    let mut out: Vec<Expr> = Vec::new();
    let empty = Scope { tables: Vec::new() };
    extract_outer_body(store, &mut q.body, outer, base, &mut out, &empty);
    out
}

/// `inherited` carries the scopes of every select between the extraction
/// root and `body` — a reference resolving against ANY intervening scope is
/// that level's correlation, not the root's (SQL shadowing across levels).
fn extract_outer_body<S: Store>(
    store: &S,
    body: &mut QueryBody,
    outer: &Scope,
    base: usize,
    out: &mut Vec<Expr>,
    inherited: &Scope,
) {
    match body {
        QueryBody::Select(s) => {
            let inner = scope_of(store, s).merged(inherited);
            // A bare outer-column projection keeps its output name after the
            // rewrite turns it into a parameter (`SELECT "c"."City"` must
            // still surface as column `City`).
            for item in &mut s.projection {
                if let SelectItem::Expr {
                    expr: Expr::Column { name, .. },
                    alias: alias @ None,
                } = item
                {
                    *alias = Some(name.clone());
                }
            }
            let mut rewrite = |e: &mut Expr| rewrite_outer_refs(store, e, &inner, outer, base, out);
            for item in &mut s.projection {
                if let SelectItem::Expr { expr, .. } = item {
                    rewrite(expr);
                }
            }
            if let Some(f) = &mut s.filter {
                rewrite(f);
            }
            for j in &mut s.joins {
                rewrite(&mut j.on);
            }
            for e in &mut s.group_by {
                rewrite(e);
            }
            if let Some(h) = &mut s.having {
                rewrite(h);
            }
            for (e, _) in &mut s.order_by {
                rewrite(e);
            }
            // Outer references may sit inside derived tables (EF nests a
            // LIMIT'd correlated collection in a FROM subquery). Descend with
            // the same outer scope; names resolving in the derived body's own
            // scope are left alone by its recursive `inner` check.
            if let Some(from) = &mut s.from
                && let Some(sub) = &mut from.subquery
            {
                extract_outer_body(store, &mut sub.body, outer, base, out, &inner);
            }
            for j in &mut s.joins {
                if let Some(sub) = &mut j.table.subquery {
                    extract_outer_body(store, &mut sub.body, outer, base, out, &inner);
                }
            }
        }
        QueryBody::SetOp { left, right, .. } => {
            extract_outer_body(store, left, outer, base, out, inherited);
            extract_outer_body(store, right, outer, base, out, inherited);
        }
        // VALUES rows may carry correlated expressions (EF inlines
        // navigation subqueries into collection literals).
        QueryBody::Values(rows) => {
            for row in rows {
                for e in row {
                    rewrite_outer_refs(store, e, inherited, outer, base, out);
                }
            }
        }
    }
}

fn rewrite_outer_refs<S: Store>(
    store: &S,
    e: &mut Expr,
    inner: &Scope,
    outer: &Scope,
    base: usize,
    out: &mut Vec<Expr>,
) {
    match e {
        Expr::Column { table, name } => {
            // Inner scope wins (SQL name shadowing); only a reference that
            // cannot resolve inside but can outside is correlated.
            if inner.resolves(table, name) || !outer.resolves(table, name) {
                return;
            }
            let k = out
                .iter()
                .position(|o| {
                    matches!(o, Expr::Column { table: t2, name: n2 } if t2 == table && n2 == name)
                })
                .unwrap_or_else(|| {
                    out.push(e.clone());
                    out.len() - 1
                });
            *e = Expr::Param(base + k);
        }
        Expr::Binary { left, right, .. } => {
            rewrite_outer_refs(store, left, inner, outer, base, out);
            rewrite_outer_refs(store, right, inner, outer, base, out);
        }
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } => {
            rewrite_outer_refs(store, expr, inner, outer, base, out);
        }
        Expr::Aggregate { arg, .. } => {
            if let Some(a) = arg {
                rewrite_outer_refs(store, a, inner, outer, base, out);
            }
        }
        Expr::Func { args, .. } => {
            for a in args {
                rewrite_outer_refs(store, a, inner, outer, base, out);
            }
        }
        Expr::InSet { expr, .. } => {
            rewrite_outer_refs(store, expr, inner, outer, base, out);
        }
        Expr::In { expr, list, .. } => {
            rewrite_outer_refs(store, expr, inner, outer, base, out);
            for item in list {
                rewrite_outer_refs(store, item, inner, outer, base, out);
            }
        }
        // The probe of a nested IN-subquery belongs to *this* scope. The
        // nested BODY is descended too: a reference resolving against no
        // intervening scope but against the extraction root's outer scope is
        // multi-level correlation and must be captured here (the nested
        // query's own resolution only sees one level up).
        Expr::InSubquery { expr, query, .. } => {
            rewrite_outer_refs(store, expr, inner, outer, base, out);
            extract_outer_body(store, &mut query.body, outer, base, out, inner);
        }
        Expr::Subquery(query) => {
            extract_outer_body(store, &mut query.body, outer, base, out, inner);
        }
        Expr::Window {
            func,
            partition_by,
            order_by,
        } => {
            for e in partition_by {
                rewrite_outer_refs(store, e, inner, outer, base, out);
            }
            for (e, _) in order_by {
                rewrite_outer_refs(store, e, inner, outer, base, out);
            }
            if let WindowFunc::Agg(_, Some(a)) = func {
                rewrite_outer_refs(store, a, inner, outer, base, out);
            }
        }
        Expr::CorrScalar { .. }
        | Expr::CorrIn { .. }
        | Expr::Col(_)
        | Expr::Literal(_)
        | Expr::Param(_) => {}
    }
}

/// Execute a fully-resolved query and return its columns + rows.
fn run_query_rows<S: Store>(
    store: &S,
    q: SelectQuery,
    params: &[Value],
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    match exec_query(store, q, params)? {
        QueryResult::Select { columns, rows, .. } => Ok((columns, rows)),
        _ => unreachable!("subquery produced a non-select result"),
    }
}

// ── correlated evaluation ───────────────────────────────────────────────────

/// Whether an expression contains a correlated subquery node.
fn has_corr(e: &Expr) -> bool {
    match e {
        Expr::CorrScalar { .. } | Expr::CorrIn { .. } => true,
        Expr::Binary { left, right, .. } => has_corr(left) || has_corr(right),
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } => has_corr(expr),
        Expr::Aggregate { arg, .. } => arg.as_deref().is_some_and(has_corr),
        Expr::Func { args, .. } => args.iter().any(has_corr),
        Expr::In { expr, list, .. } => has_corr(expr) || list.iter().any(has_corr),
        Expr::InSet { expr, .. } => has_corr(expr),
        Expr::Window {
            func,
            partition_by,
            order_by,
        } => {
            partition_by.iter().any(has_corr)
                || order_by.iter().any(|(e, _)| has_corr(e))
                || matches!(func, WindowFunc::Agg(_, Some(a)) if has_corr(a))
        }
        _ => false,
    }
}

/// Try to decorrelate a scalar-aggregate subquery — `(SELECT agg(...) FROM
/// ... WHERE key = $slot AND residual)` with exactly one outer reference and
/// no grouping — into a one-shot `SELECT key, agg(...) ... WHERE residual
/// GROUP BY key`, returning that `key -> value` map plus the default for an
/// absent key. `None` when the shape does not qualify (the caller keeps the
/// row-by-row correlated form).
///
/// The projection must be a single aggregate, optionally wrapped in
/// `COALESCE(agg, literal)` (EF Core's `SUM` → `COALESCE(SUM(x), 0)`); the
/// default is that literal, or `0` for a bare `COUNT`, else NULL — matching
/// the value the correlated subquery yields over the empty group.
fn decorrelate_corr_agg<S: Store>(
    _store: &S,
    query: &SelectQuery,
    outer: &[Expr],
    base: usize,
    params: &[Value],
) -> Result<Option<std::sync::Arc<LazyCorrAgg>>> {
    if outer.len() != 1
        || !query.order_by.is_empty()
        || query.limit.is_some()
        || query.offset.is_some()
        || !query.ctes.is_empty()
    {
        return Ok(None);
    }
    let QueryBody::Select(sel) = &query.body else {
        return Ok(None);
    };
    if !sel.group_by.is_empty()
        || sel.having.is_some()
        || sel.distinct
        || !sel.distinct_on.is_empty()
        || sel.limit.is_some()
        || sel.offset.is_some()
        || !sel.order_by.is_empty()
        // Non-lateral joins are fine: the grouped rewrite (map_query clones the
        // whole subquery, joins included, and only re-projects/groups) runs them
        // once. A LATERAL join re-executes per left row and may itself be
        // correlated, so keep those on the row-by-row path.
        || sel.joins.iter().any(|j| j.table.lateral)
    {
        return Ok(None);
    }
    // An expensive (joined) subquery must not pay N per-row re-executions before
    // the grouped map kicks in — each re-execution is a full join+scan.
    let threshold = if sel.joins.is_empty() {
        LazyCorrAgg::THRESHOLD
    } else {
        LazyCorrAgg::THRESHOLD_JOINED
    };
    // The single projection must be an aggregate (optionally COALESCE-wrapped).
    let [SelectItem::Expr { expr: proj, .. }] = sel.projection.as_slice() else {
        return Ok(None);
    };
    let (is_count, default) = match proj {
        Expr::Aggregate {
            func: AggFunc::Count,
            ..
        } => (true, Value::Null),
        Expr::Aggregate { .. } => (false, Value::Null),
        Expr::Func {
            func: ScalarFunc::Coalesce,
            args,
        } if args.len() == 2
            && matches!(args[0], Expr::Aggregate { .. })
            && matches!(args[1], Expr::Literal(_)) =>
        {
            let Expr::Literal(d) = &args[1] else {
                unreachable!()
            };
            (false, d.clone())
        }
        _ => return Ok(None),
    };
    let default = if is_count { Value::Int(0) } else { default };

    // Find the single correlation equality `key = $base` among the top-level
    // WHERE conjuncts; the rest is residual (must not touch the outer slot).
    let Some(filter) = &sel.filter else {
        return Ok(None);
    };
    let mut conjuncts = Vec::new();
    collect_conjuncts(filter, &mut conjuncts);
    let mut key: Option<&Expr> = None;
    let mut residual: Vec<&Expr> = Vec::new();
    for c in conjuncts {
        if key.is_none()
            && let Expr::Binary {
                op: BinOp::Eq,
                left,
                right,
            } = c
        {
            match (left.as_ref(), right.as_ref()) {
                (Expr::Param(p), k) | (k, Expr::Param(p)) if *p == base => {
                    key = Some(k);
                    continue;
                }
                _ => {}
            }
        }
        residual.push(c);
    }
    let Some(key) = key else {
        return Ok(None);
    };

    // Build `SELECT key AS __k, <proj> AS __v FROM ... WHERE residual GROUP BY key`.
    let mut map_query = query.clone();
    let QueryBody::Select(sel2) = &mut map_query.body else {
        unreachable!("shape checked above");
    };
    sel2.projection = vec![
        SelectItem::Expr {
            expr: key.clone(),
            alias: Some("__k".into()),
        },
        SelectItem::Expr {
            expr: proj.clone(),
            alias: Some("__v".into()),
        },
    ];
    sel2.filter = residual.into_iter().cloned().reduce(|l, r| Expr::Binary {
        op: BinOp::And,
        left: Box::new(l),
        right: Box::new(r),
    });
    sel2.group_by = vec![key.clone()];
    // A remaining synthetic slot means the correlation is more than the one
    // equality — keep the row-by-row form.
    if query_has_param_ge(&map_query, params.len()) {
        return Ok(None);
    }
    // Prepared, not run: the map materializes only if enough rows hit it (see
    // LazyCorrAgg). Small/indexed outer sides never pay for the grouped pass.
    Ok(Some(std::sync::Arc::new(LazyCorrAgg {
        map_query,
        params: params.to_vec(),
        default,
        calls: std::sync::atomic::AtomicUsize::new(0),
        map: std::sync::OnceLock::new(),
        threshold,
    })))
}

/// Build (or fetch the cached) decorrelated map for a `LazyCorrAgg`.
fn lazy_corr_map<'a, S: Store>(
    store: &S,
    lazy: &'a LazyCorrAgg,
) -> Result<&'a std::collections::BTreeMap<IndexKey, Value>> {
    if let Some(m) = lazy.map.get() {
        return Ok(m);
    }
    let (columns, rows) = run_query_rows(store, lazy.map_query.clone(), &lazy.params)?;
    let mut map = std::collections::BTreeMap::new();
    if columns.len() == 2 {
        for mut r in rows {
            let v = r.pop().unwrap_or(Value::Null);
            let k = r.pop().unwrap_or(Value::Null);
            if !matches!(k, Value::Null) {
                map.insert(IndexKey(k), v);
            }
        }
    }
    // Another evaluation of the same Arc may have raced us; keep the winner.
    let _ = lazy.map.set(map);
    Ok(lazy.map.get().expect("just set"))
}

/// Try to decorrelate the parser's EXISTS encoding — `1 IN (SELECT 1 FROM
/// ... WHERE $slot = key AND residual)` with exactly one outer reference —
/// into `COALESCE(outer IN <set>, FALSE)` over the one-shot materialized
/// `SELECT key FROM ... WHERE residual` (`NOT` wrapped for NOT EXISTS).
///
/// COALESCE pins the NULL cases to what the correlated form produces: a
/// NULL outer value (`NULL = key` matches nothing → EXISTS is FALSE) and
/// NULL keys in the set (never equal to anything) both surface as a NULL
/// `IN`, which COALESCE folds back to FALSE — making the rewrite exact,
/// including under negation and in projections.
fn decorrelate_exists<S: Store>(
    store: &S,
    probe: &Expr,
    query: &SelectQuery,
    outer: &[Expr],
    base: usize,
    negated: bool,
    params: &[Value],
) -> Result<Option<Expr>> {
    if outer.len() != 1
        || !matches!(probe, Expr::Literal(Value::Int(1)))
        || !query.order_by.is_empty()
        || query.limit.is_some()
        || query.offset.is_some()
        || !query.ctes.is_empty()
    {
        return Ok(None);
    }
    let QueryBody::Select(sel) = &query.body else {
        return Ok(None);
    };
    // The EXISTS projection marker (`SELECT 1`), un-aggregated, un-deduped,
    // and — crucially — without a row-slicing LIMIT/OFFSET: those apply per
    // outer row in the correlated form (EF's ElementAt renders EXISTS with
    // an OFFSET), but would apply once globally after decorrelation.
    if !sel.group_by.is_empty()
        || sel.having.is_some()
        || sel.distinct
        || !sel.distinct_on.is_empty()
        || sel.limit.is_some()
        || sel.offset.is_some()
        || !sel.order_by.is_empty()
    {
        return Ok(None);
    }
    match sel.projection.as_slice() {
        [
            SelectItem::Expr {
                expr: Expr::Literal(Value::Int(1)),
                ..
            },
        ] => {}
        _ => return Ok(None),
    }
    // Find the correlation equality among the WHERE's top-level conjuncts.
    let Some(filter) = &sel.filter else {
        return Ok(None);
    };
    let mut conjuncts = Vec::new();
    collect_conjuncts(filter, &mut conjuncts);
    let mut key: Option<&Expr> = None;
    let mut residual: Vec<&Expr> = Vec::new();
    for c in conjuncts {
        if key.is_none()
            && let Expr::Binary {
                op: BinOp::Eq,
                left,
                right,
            } = c
        {
            match (left.as_ref(), right.as_ref()) {
                (Expr::Param(p), k) | (k, Expr::Param(p)) if *p == base => {
                    key = Some(k);
                    continue;
                }
                _ => {}
            }
        }
        residual.push(c);
    }
    let Some(key) = key else {
        return Ok(None);
    };
    let mut set_query = query.clone();
    let QueryBody::Select(sel2) = &mut set_query.body else {
        unreachable!("shape checked above");
    };
    sel2.projection = vec![SelectItem::Expr {
        expr: key.clone(),
        alias: None,
    }];
    sel2.filter = residual.into_iter().cloned().reduce(|l, r| Expr::Binary {
        op: BinOp::And,
        left: Box::new(l),
        right: Box::new(r),
    });
    // Any remaining synthetic slot means the correlation ran deeper than the
    // one equality (or the slot was used twice) — stay correlated.
    if query_has_param_ge(&set_query, params.len()) {
        return Ok(None);
    }
    let (columns, rows) = run_query_rows(store, set_query, params)?;
    if columns.len() != 1 {
        return Ok(None);
    }
    let mut set = std::collections::BTreeSet::new();
    let mut has_null = false;
    for mut r in rows {
        match r.remove(0) {
            Value::Null => has_null = true,
            v => {
                set.insert(IndexKey(v));
            }
        }
    }
    let membership = Expr::Func {
        func: ScalarFunc::Coalesce,
        args: vec![
            Expr::InSet {
                expr: Box::new(outer[0].clone()),
                set: std::sync::Arc::new(set),
                has_null,
                negated: false,
            },
            Expr::Literal(Value::Bool(false)),
        ],
    };
    Ok(Some(if negated {
        Expr::Unary {
            op: UnOp::Not,
            expr: Box::new(membership),
        }
    } else {
        membership
    }))
}

/// Replace every correlated subquery node in `e` with its result for this
/// row: outer column values are evaluated against the row, appended to the
/// params after slot `base`, and the subquery re-executed.
fn resolve_corr_row<S: Store, R: RowLike + ?Sized>(
    store: &S,
    e: &Expr,
    schema: &[ColRef],
    row: &R,
    params: &[Value],
) -> Result<Expr> {
    Ok(match e {
        Expr::CorrScalar {
            query,
            outer,
            base,
            agg_map,
        } => {
            // Decorrelatable aggregate: run the subquery row-by-row for the
            // first THRESHOLD rows (cheap for a small/indexed outer side);
            // past that, build the grouped map once and look keys up in it.
            if let Some(lazy) = agg_map {
                let n = lazy
                    .calls
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if n >= lazy.threshold || lazy.map.get().is_some() {
                    let aug = corr_params(schema, row, params, outer, *base)?;
                    let key = &aug[*base];
                    let map = lazy_corr_map(store, lazy)?;
                    let v = if matches!(key, Value::Null) {
                        lazy.default.clone()
                    } else {
                        map.get(&IndexKey(key.clone()))
                            .cloned()
                            .unwrap_or_else(|| lazy.default.clone())
                    };
                    return Ok(Expr::Literal(v));
                }
            }
            let aug = corr_params(schema, row, params, outer, *base)?;
            let (columns, mut rows) = run_query_rows(store, (**query).clone(), &aug)?;
            if columns.len() != 1 {
                return Err(SqlError::Unsupported(
                    "scalar subquery must return exactly one column".into(),
                ));
            }
            if rows.len() > 1 {
                return Err(SqlError::Eval(
                    "scalar subquery returned more than one row".into(),
                ));
            }
            Expr::Literal(rows.pop().map(|mut r| r.remove(0)).unwrap_or(Value::Null))
        }
        Expr::CorrIn {
            expr,
            query,
            outer,
            base,
            negated,
        } => {
            let probe = resolve_corr_row(store, expr, schema, row, params)?;
            let aug = corr_params(schema, row, params, outer, *base)?;
            let (columns, rows) = run_query_rows(store, (**query).clone(), &aug)?;
            if columns.len() != 1 {
                return Err(SqlError::Unsupported(
                    "IN subquery must return exactly one column".into(),
                ));
            }
            Expr::In {
                expr: Box::new(probe),
                list: rows
                    .into_iter()
                    .map(|mut r| Expr::Literal(r.remove(0)))
                    .collect(),
                negated: *negated,
            }
        }
        Expr::Binary { op, left, right } => Expr::Binary {
            op: *op,
            left: Box::new(resolve_corr_row(store, left, schema, row, params)?),
            right: Box::new(resolve_corr_row(store, right, schema, row, params)?),
        },
        Expr::Unary { op, expr } => Expr::Unary {
            op: *op,
            expr: Box::new(resolve_corr_row(store, expr, schema, row, params)?),
        },
        Expr::IsNull { expr, negated } => Expr::IsNull {
            expr: Box::new(resolve_corr_row(store, expr, schema, row, params)?),
            negated: *negated,
        },
        Expr::InSet {
            expr,
            set,
            has_null,
            negated,
        } => Expr::InSet {
            expr: Box::new(resolve_corr_row(store, expr, schema, row, params)?),
            set: set.clone(),
            has_null: *has_null,
            negated: *negated,
        },
        Expr::In {
            expr,
            list,
            negated,
        } => Expr::In {
            expr: Box::new(resolve_corr_row(store, expr, schema, row, params)?),
            list: list
                .iter()
                .map(|i| resolve_corr_row(store, i, schema, row, params))
                .collect::<Result<_>>()?,
            negated: *negated,
        },
        Expr::Aggregate {
            func,
            arg,
            distinct,
        } => Expr::Aggregate {
            func: *func,
            arg: match arg {
                Some(a) => Some(Box::new(resolve_corr_row(store, a, schema, row, params)?)),
                None => None,
            },
            distinct: *distinct,
        },
        Expr::Func { func, args } => Expr::Func {
            func: *func,
            args: args
                .iter()
                .map(|a| resolve_corr_row(store, a, schema, row, params))
                .collect::<Result<_>>()?,
        },
        other => other.clone(),
    })
}

/// Build the augmented parameter list for one correlated execution: the
/// user's params (padded to `base`), then the outer values for this row.
fn corr_params<R: RowLike + ?Sized>(
    schema: &[ColRef],
    row: &R,
    params: &[Value],
    outer: &[Expr],
    base: usize,
) -> Result<Vec<Value>> {
    // The subquery sees the user params (padded) and its outer values at
    // exactly `base..` — never any *later* synthetic slots (e.g. window
    // values) the enclosing evaluation may have appended.
    let mut aug: Vec<Value> = params.iter().take(base).cloned().collect();
    if aug.len() < base {
        aug.resize(base, Value::Null);
    }
    for oe in outer {
        aug.push(eval_scalar(oe, schema, row, params)?);
    }
    Ok(aug)
}

/// Evaluate an expression that may contain correlated subqueries over one row.
fn eval_scalar_corr<S: Store, R: RowLike + ?Sized>(
    store: &S,
    corr: bool,
    e: &Expr,
    schema: &[ColRef],
    row: &R,
    params: &[Value],
) -> Result<Value> {
    if corr {
        let resolved = resolve_corr_row(store, e, schema, row, params)?;
        eval_scalar(&resolved, schema, row, params)
    } else {
        eval_scalar(e, schema, row, params)
    }
}

// ── window functions ────────────────────────────────────────────────────────
//
// Window expressions in the SELECT list are computed per input row *before*
// projection: each distinct `Window` node is replaced by a synthetic
// `Param(win_base + k)` and its per-row values are appended to the params for
// that row's evaluation. Partitioning reuses the streaming grouper; with
// ORDER BY, aggregates run cumulatively and peer rows (equal sort keys) share
// the value (the standard RANGE UNBOUNDED PRECEDING .. CURRENT ROW frame).

/// Whether an expression contains a window function node.
fn has_window(e: &Expr) -> bool {
    match e {
        Expr::Window { .. } => true,
        Expr::Binary { left, right, .. } => has_window(left) || has_window(right),
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } => has_window(expr),
        Expr::Aggregate { arg, .. } => arg.as_deref().is_some_and(has_window),
        Expr::Func { args, .. } => args.iter().any(has_window),
        Expr::In { expr, list, .. } => has_window(expr) || list.iter().any(has_window),
        Expr::InSet { expr, .. } => has_window(expr),
        _ => false,
    }
}

/// The lowest params index safely usable for extra synthetic slots: one past
/// every `Param` slot (user or correlated) the expression can touch.
fn max_param_slot(e: &Expr) -> usize {
    match e {
        Expr::Param(i) => i + 1,
        Expr::Binary { left, right, .. } => max_param_slot(left).max(max_param_slot(right)),
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } => max_param_slot(expr),
        Expr::Aggregate { arg, .. } => arg.as_deref().map(max_param_slot).unwrap_or(0),
        Expr::Func { args, .. } => args.iter().map(max_param_slot).max().unwrap_or(0),
        Expr::In { expr, list, .. } => list
            .iter()
            .map(max_param_slot)
            .fold(max_param_slot(expr), usize::max),
        Expr::InSet { expr, .. } => max_param_slot(expr),
        Expr::CorrScalar { outer, base, .. } => base + outer.len(),
        Expr::CorrIn {
            expr, outer, base, ..
        } => (base + outer.len()).max(max_param_slot(expr)),
        Expr::Window {
            func,
            partition_by,
            order_by,
        } => {
            let mut m = partition_by.iter().map(max_param_slot).max().unwrap_or(0);
            m = order_by
                .iter()
                .map(|(e, _)| max_param_slot(e))
                .fold(m, usize::max);
            if let WindowFunc::Agg(_, Some(a)) = func {
                m = m.max(max_param_slot(a));
            }
            m
        }
        _ => 0,
    }
}

/// Replace every `Window` node in `e` with `Param(win_base + k)`, collecting
/// the distinct window nodes into `windows`.
fn replace_windows(e: &mut Expr, windows: &mut Vec<Expr>, win_base: usize) {
    match e {
        Expr::Window { .. } => {
            let k = windows.iter().position(|w| w == e).unwrap_or_else(|| {
                windows.push(e.clone());
                windows.len() - 1
            });
            *e = Expr::Param(win_base + k);
        }
        Expr::Binary { left, right, .. } => {
            replace_windows(left, windows, win_base);
            replace_windows(right, windows, win_base);
        }
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } => {
            replace_windows(expr, windows, win_base);
        }
        Expr::Aggregate { arg: Some(a), .. } => {
            replace_windows(a, windows, win_base);
        }
        Expr::Func { args, .. } => {
            for a in args {
                replace_windows(a, windows, win_base);
            }
        }
        Expr::InSet { expr, .. } => replace_windows(expr, windows, win_base),
        Expr::In { expr, list, .. } => {
            replace_windows(expr, windows, win_base);
            for item in list {
                replace_windows(item, windows, win_base);
            }
        }
        _ => {}
    }
}

/// Compute one window expression's value for every tuple (aligned with tuple
/// order).
fn compute_window<S: Store>(
    store: &S,
    schema: &[ColRef],
    src: &Sources,
    tuples: &Tuples,
    window: &Expr,
    params: &[Value],
) -> Result<Vec<Value>> {
    let Expr::Window {
        func,
        partition_by,
        order_by,
    } = window
    else {
        unreachable!("compute_window on a non-window expression");
    };
    let n = tuples.n();
    let mut out = vec![Value::Null; n];

    let partitions = group_tuples(store, schema, src, tuples, partition_by, params)?;
    for mut part in partitions {
        // Sort the partition by the window ORDER BY (stable).
        let mut keys: Vec<Vec<Value>> = Vec::new();
        if !order_by.is_empty() {
            keys = part
                .iter()
                .map(|&i| {
                    let view = View {
                        src,
                        tuple: tuples.row(i as usize),
                    };
                    order_by
                        .iter()
                        .map(|(e, _)| {
                            eval_scalar_corr(store, has_corr(e), e, schema, &view, params)
                        })
                        .collect::<Result<Vec<Value>>>()
                })
                .collect::<Result<_>>()?;
            let mut order: Vec<usize> = (0..part.len()).collect();
            order.sort_by(|&a, &b| cmp_keys(order_by, &keys[a], &keys[b]));
            part = order.iter().map(|&j| part[j]).collect();
            keys = order
                .into_iter()
                .map(|j| std::mem::take(&mut keys[j]))
                .collect();
        }

        // Peer groups: runs of rows whose sort keys all compare Equal (with
        // no ORDER BY the whole partition is one peer group).
        let peer_end = |start: usize| -> usize {
            if order_by.is_empty() {
                return part.len();
            }
            let mut end = start + 1;
            while end < part.len()
                && keys[start]
                    .iter()
                    .zip(keys[end].iter())
                    .all(|(a, b)| Value::total_order(a, b) == Ordering::Equal)
            {
                end += 1;
            }
            end
        };

        match func {
            WindowFunc::RowNumber => {
                for (j, &i) in part.iter().enumerate() {
                    out[i as usize] = Value::Int(j as i64 + 1);
                }
            }
            WindowFunc::Rank | WindowFunc::DenseRank => {
                let dense = matches!(func, WindowFunc::DenseRank);
                let mut start = 0;
                let mut dense_rank = 0i64;
                while start < part.len() {
                    let end = peer_end(start);
                    dense_rank += 1;
                    let rank = if dense { dense_rank } else { start as i64 + 1 };
                    for &i in &part[start..end] {
                        out[i as usize] = Value::Int(rank);
                    }
                    start = end;
                }
            }
            WindowFunc::Agg(agg, arg) => {
                if order_by.is_empty() {
                    let v = eval_aggregate(
                        store,
                        *agg,
                        arg.as_deref(),
                        false,
                        schema,
                        src,
                        tuples,
                        &part,
                        params,
                    )?;
                    for &i in &part {
                        out[i as usize] = v.clone();
                    }
                } else {
                    // Running aggregate; peers share the value at the end of
                    // their peer group.
                    let mut count: i64 = 0;
                    let mut sum = SumAcc::Empty;
                    let mut best: Option<Value> = None;
                    let mut start = 0;
                    while start < part.len() {
                        let end = peer_end(start);
                        for &i in &part[start..end] {
                            let view = View {
                                src,
                                tuple: tuples.row(i as usize),
                            };
                            let v = match arg {
                                Some(a) => {
                                    eval_scalar_corr(store, has_corr(a), a, schema, &view, params)?
                                }
                                // COUNT(*): every row counts.
                                None => Value::Int(1),
                            };
                            if matches!(v, Value::Null) {
                                continue;
                            }
                            count += 1;
                            match agg {
                                AggFunc::Sum | AggFunc::Avg => sum.add(&v)?,
                                AggFunc::Min => {
                                    if best
                                        .as_ref()
                                        .is_none_or(|b| Value::total_order(&v, b) == Ordering::Less)
                                    {
                                        best = Some(v);
                                    }
                                }
                                AggFunc::Max => {
                                    if best.as_ref().is_none_or(|b| {
                                        Value::total_order(&v, b) == Ordering::Greater
                                    }) {
                                        best = Some(v);
                                    }
                                }
                                AggFunc::Count => {}
                                // mode() OVER is rejected at parse; unreachable here.
                                AggFunc::Mode => {}
                            }
                        }
                        let v = match agg {
                            AggFunc::Count => Value::Int(count),
                            AggFunc::Sum => sum.clone().into_sum(),
                            AggFunc::Avg => sum.clone().into_avg(count),
                            AggFunc::Min | AggFunc::Max => best.clone().unwrap_or(Value::Null),
                            AggFunc::Mode => Value::Null,
                        };
                        for &i in &part[start..end] {
                            out[i as usize] = v.clone();
                        }
                        start = end;
                    }
                }
            }
        }
    }
    Ok(out)
}

fn exec_select<S: Store>(store: &S, select: SelectStmt, params: &[Value]) -> Result<QueryResult> {
    let mut select = select;
    // 0. Pick the smallest base table as the driver (FROM): a query written
    //    `FROM big JOIN small` (EF Core routinely emits this) should start
    //    from `small` so the big table is reached by an index probe, not a
    //    full materialization. Safe only across all-INNER base-table joins.
    choose_driver(store, &mut select, params);
    // 0b. Aggregates over a single table are folded during the scan, without
    //     materializing the rows at all. Returns `None` for anything it does
    //     not handle, which then takes the general path below unchanged.
    if let Some(result) = streamed_aggregate(store, &select, params)? {
        return Ok(result);
    }
    // 1. Build the source: base table, then joins (the join ON is bound inside).
    let (schema, src, mut tuples) = build_source(store, &select, params)?;

    // 2. Expand the projection into (output name, expr) pairs (still unbound).
    let proj_unbound = expand_projection(&select.projection, &schema)?;

    // Resolve ORDER BY references to projection aliases: a bare column that is
    // not an input column but matches an output name (`... AS spend ... ORDER BY
    // spend`) is rewritten to that projection's expression. Real input columns
    // keep their meaning, so this is backward compatible.
    for (expr, _) in select.order_by.iter_mut() {
        if let Expr::Column { table: None, name } = expr
            && resolve_col(&schema, &None, name).is_err()
            && let Some((_, pe)) = proj_unbound.iter().find(|(n, _)| n == name)
        {
            *expr = pe.clone();
        }
    }

    let aggregating = !select.group_by.is_empty()
        || proj_unbound.iter().any(|(_, e)| has_aggregate(e))
        || select.having.as_ref().is_some_and(has_aggregate);
    let columns: Vec<String> = proj_unbound.iter().map(|(n, _)| n.clone()).collect();
    let types: Vec<Option<SqlType>> = proj_unbound
        .iter()
        .map(|(_, e)| expr_type(e, &schema))
        .collect();

    // 3. Bind every expression's columns to positional indices. This both
    //    validates columns (unknown/ambiguous -> error, even over empty rows)
    //    and makes per-row evaluation O(1).
    let bound_filter = select
        .filter
        .as_ref()
        .map(|f| bind_expr(f, &schema))
        .transpose()?;
    let proj: Vec<(String, Expr)> = proj_unbound
        .into_iter()
        .map(|(n, e)| Ok::<_, SqlError>((n, bind_expr(&e, &schema)?)))
        .collect::<Result<_>>()?;
    select.group_by = select
        .group_by
        .iter()
        .map(|e| bind_expr(e, &schema))
        .collect::<Result<_>>()?;
    select.having = select
        .having
        .as_ref()
        .map(|h| bind_expr(h, &schema))
        .transpose()?;
    select.order_by = select
        .order_by
        .iter()
        .map(|(e, a)| Ok::<_, SqlError>((bind_expr(e, &schema)?, *a)))
        .collect::<Result<_>>()?;

    // 4. WHERE: keep only matching tuples.
    if let Some(pred) = &bound_filter {
        if has_window(pred) {
            return Err(SqlError::Unsupported(
                "window function in WHERE (use a view or an outer query)".into(),
            ));
        }
        let corr = has_corr(pred);
        let mut kept = Vec::with_capacity(tuples.data.len());
        for i in 0..tuples.n() {
            let tuple = tuples.row(i);
            let view = View { src: &src, tuple };
            if truthy(&eval_scalar_corr(
                store, corr, pred, &schema, &view, params,
            )?) {
                kept.extend_from_slice(tuple);
            }
        }
        tuples.data = kept;
    }

    let out_rows = if aggregating {
        if proj.iter().any(|(_, e)| has_window(e))
            || select.group_by.iter().any(has_window)
            || select.having.as_ref().is_some_and(has_window)
            || select.order_by.iter().any(|(e, _)| has_window(e))
        {
            return Err(SqlError::Unsupported(
                "window function in an aggregated query (use a view or an outer query)".into(),
            ));
        }
        select_aggregated(store, &schema, &src, &tuples, &select, &proj, params)?
    } else {
        select_simple(store, &schema, &src, &tuples, &select, &proj, params)?
    };

    // DISTINCT: dedup after projection + ordering, before OFFSET/LIMIT. A
    // single output column (the common `SELECT DISTINCT city` / set-op arm)
    // keys on the scalar directly — no per-row Vec allocation.
    let out_rows = if select.distinct {
        if columns.len() == 1 {
            let mut seen: std::collections::BTreeSet<IndexKey> = std::collections::BTreeSet::new();
            out_rows
                .into_iter()
                .filter(|row| seen.insert(IndexKey(row[0].clone())))
                .collect()
        } else {
            let mut seen: std::collections::BTreeSet<Vec<IndexKey>> =
                std::collections::BTreeSet::new();
            out_rows
                .into_iter()
                .filter(|row| seen.insert(row.iter().cloned().map(IndexKey).collect()))
                .collect()
        }
    } else {
        out_rows
    };

    // OFFSET / LIMIT.
    let out_rows: Vec<Vec<Value>> = out_rows
        .into_iter()
        .skip(resolve_limit(select.offset, params, "OFFSET")?.unwrap_or(0))
        .take(resolve_limit(select.limit, params, "LIMIT")?.unwrap_or(usize::MAX))
        .collect();

    Ok(QueryResult::Select {
        columns,
        types,
        rows: out_rows,
    })
}

/// The set of columns a query actually references, used to prune scanned
/// tables before joining (projection push-down). Wide intermediate rows are
/// the dominant cost of multi-way joins, so carrying only referenced columns
/// is a large win.
struct Needed {
    /// `SELECT *` — keep everything.
    all: bool,
    /// `SELECT t.*` — keep all columns of these table keys.
    tables_all: std::collections::HashSet<String>,
    /// Individual references. `None` table = unqualified: keep the name in
    /// every table (binding resolves/ambiguity-checks exactly as before).
    cols: std::collections::HashSet<(Option<String>, String)>,
}

impl Needed {
    fn keep(&self, table_key: &str, col: &str) -> bool {
        self.all
            || self.tables_all.contains(table_key)
            || self
                .cols
                .contains(&(Some(table_key.to_string()), col.to_string()))
            || self.cols.contains(&(None, col.to_string()))
    }
}

fn collect_needed(select: &SelectStmt) -> Needed {
    let mut needed = Needed {
        all: false,
        tables_all: std::collections::HashSet::new(),
        cols: std::collections::HashSet::new(),
    };
    let mut refs: Vec<(&Option<String>, &str)> = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::Wildcard => needed.all = true,
            SelectItem::QualifiedWildcard(t) => {
                needed.tables_all.insert(t.clone());
            }
            SelectItem::Expr { expr, .. } => collect_col_refs(expr, &mut refs),
        }
    }
    if let Some(f) = &select.filter {
        collect_col_refs(f, &mut refs);
    }
    for j in &select.joins {
        collect_col_refs(&j.on, &mut refs);
        // A LATERAL body may reference left-side columns; keep every column
        // it names (references to its own tables are harmless extras here).
        if j.table.lateral
            && let Some(sub) = &j.table.subquery
        {
            collect_body_col_refs(&sub.body, &mut refs);
        }
    }
    for e in &select.group_by {
        collect_col_refs(e, &mut refs);
    }
    if let Some(h) = &select.having {
        collect_col_refs(h, &mut refs);
    }
    for (e, _) in &select.order_by {
        collect_col_refs(e, &mut refs);
    }
    for (t, n) in refs {
        needed.cols.insert((t.clone(), n.to_string()));
    }
    needed
}

/// Column indices of `full` kept by `needed` for table `key`.
fn keep_indices(full: &[ColRef], key: &str, needed: &Needed) -> Vec<usize> {
    (0..full.len())
        .filter(|&i| needed.keep(key, &full[i].name))
        .collect()
}

/// Execute a view's stored SELECT and materialize it as a pruned source.
/// Materialize a derived table (`FROM (SELECT ...) AS alias`): run the
/// subquery with the outer statement's params and package the result like a
/// base-table chunk, columns qualified by the alias.
fn derived_source<S: Store>(
    store: &S,
    r: &TableRef,
    sub: &SelectQuery,
    needed: &Needed,
    params: &[Value],
) -> Result<(Vec<ColRef>, Chunk)> {
    let (columns, types, rows) = match execute(store, Statement::Select(sub.clone()), params)? {
        QueryResult::Select {
            columns,
            types,
            rows,
        } => (columns, types, rows),
        _ => unreachable!("subquery produced a non-select result"),
    };
    // `AS alias(c1, c2)` renames the output columns.
    let columns = rename_columns(columns, &r.alias_columns, r.key())?;
    let full: Vec<ColRef> = columns
        .iter()
        .zip(&types)
        .map(|(c, ty)| ColRef {
            table: r.key().to_string(),
            name: c.clone(),
            ty: *ty,
        })
        .collect();
    let keep = keep_indices(&full, r.key(), needed);
    let schema: Vec<ColRef> = keep.iter().map(|&i| full[i].clone()).collect();
    let chunk = Chunk::from_rows(rows, &keep);
    Ok((schema, chunk))
}

/// Apply a derived table's `AS alias(c1, c2)` column renames.
fn rename_columns(
    columns: Vec<String>,
    alias_columns: &[String],
    what: &str,
) -> Result<Vec<String>> {
    if alias_columns.is_empty() {
        return Ok(columns);
    }
    if alias_columns.len() != columns.len() {
        return Err(SqlError::SchemaMismatch(format!(
            "{what}: alias names {} columns, the subquery returns {}",
            alias_columns.len(),
            columns.len()
        )));
    }
    Ok(alias_columns.to_vec())
}

/// `Ok(None)` when no view with this name exists.
fn view_source<S: Store>(
    store: &S,
    r: &TableRef,
    needed: &Needed,
) -> Result<Option<(Vec<ColRef>, Chunk)>> {
    let Some(sql) = store.view_sql(&r.name) else {
        return Ok(None);
    };
    // Guard against reference cycles formed via CREATE OR REPLACE.
    thread_local! {
        static VIEW_DEPTH: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
    }
    struct DepthGuard;
    impl Drop for DepthGuard {
        fn drop(&mut self) {
            VIEW_DEPTH.with(|d| d.set(d.get() - 1));
        }
    }
    let depth = VIEW_DEPTH.with(|d| {
        d.set(d.get() + 1);
        d.get()
    });
    let _guard = DepthGuard;
    if depth > 32 {
        return Err(SqlError::Eval(format!(
            "view {:?}: reference chain too deep (cycle?)",
            r.name
        )));
    }

    let stmts = crate::parser::parse(&sql)?;
    let stmt = match (stmts.len(), stmts.into_iter().next()) {
        (1, Some(s @ Statement::Select(_))) => s,
        _ => {
            return Err(SqlError::Corrupt(format!(
                "view {:?} body is not a single SELECT",
                r.name
            )));
        }
    };
    let (columns, types, rows) = match execute(store, stmt, &[])? {
        QueryResult::Select {
            columns,
            types,
            rows,
        } => (columns, types, rows),
        _ => unreachable!("view body produced a non-select result"),
    };
    let full: Vec<ColRef> = columns
        .iter()
        .zip(&types)
        .map(|(c, ty)| ColRef {
            table: r.key().to_string(),
            name: c.clone(),
            ty: *ty,
        })
        .collect();
    let keep = keep_indices(&full, r.key(), needed);
    let schema: Vec<ColRef> = keep.iter().map(|&i| full[i].clone()).collect();
    let chunk = Chunk::from_rows(rows, &keep);
    Ok(Some((schema, chunk)))
}

/// Which side of an equi conjunct a (fully qualified) expression belongs to.
enum ConnSide {
    Avail,
    Right,
}

/// Classify an expression for join connectivity: `Some(Avail)` if every
/// column is table-qualified and every qualifier is an already-available
/// table, `Some(Right)` if every qualifier is the joining table. `None` for
/// anything else (unqualified columns, mixed sides, no columns).
fn conn_side(
    e: &Expr,
    avail: &std::collections::HashSet<String>,
    right_key: &str,
) -> Option<ConnSide> {
    let mut refs = Vec::new();
    collect_col_refs(e, &mut refs);
    if refs.is_empty() {
        return None;
    }
    let mut side: Option<ConnSide> = None;
    for (table, _) in refs {
        let t = table.as_deref()?; // unqualified -> unknown
        let s = if t == right_key {
            ConnSide::Right
        } else if avail.contains(t) {
            ConnSide::Avail
        } else {
            return None;
        };
        match (&side, &s) {
            (None, _) => side = Some(s),
            (Some(ConnSide::Avail), ConnSide::Avail) => {}
            (Some(ConnSide::Right), ConnSide::Right) => {}
            _ => return None,
        }
    }
    side
}

/// Whether every column reference in `on` is table-qualified and resolves to
/// an available table or the joining table itself (so executing the join now
/// cannot break binding), and at least one `a = b` conjunct connects the two
/// (so it stays a hash join).
fn join_placeable(on: &Expr, avail: &std::collections::HashSet<String>, right_key: &str) -> bool {
    let mut refs = Vec::new();
    collect_col_refs(on, &mut refs);
    for (table, _) in &refs {
        match table.as_deref() {
            Some(t) if t == right_key || avail.contains(t) => {}
            _ => return false,
        }
    }
    fn connects(e: &Expr, avail: &std::collections::HashSet<String>, right_key: &str) -> bool {
        match e {
            Expr::Binary {
                op: BinOp::And,
                left,
                right,
            } => connects(left, avail, right_key) || connects(right, avail, right_key),
            Expr::Binary {
                op: BinOp::Eq,
                left,
                right,
            } => matches!(
                (
                    conn_side(left, avail, right_key),
                    conn_side(right, avail, right_key)
                ),
                (Some(ConnSide::Avail), Some(ConnSide::Right))
                    | (Some(ConnSide::Right), Some(ConnSide::Avail))
            ),
            _ => false,
        }
    }
    connects(on, avail, right_key)
}

/// Greedily reorder an all-INNER join chain: at each step, among the pending
/// joins that are placeable against the tables joined so far, pick the one
/// with the smallest right-table cardinality (build the smallest hash/index
/// inputs first, shrinking intermediate results early). Written order is kept
/// whenever anything makes reordering unsafe or unknowable: an outer join, a
/// view (no cardinality hint), or an ON with unqualified columns. When no
/// pending join is placeable, the earliest remaining one is taken — its
/// written-order predecessors have all been placed, so it is always valid.
fn reorder_joins<'a, S: Store>(store: &S, from: &TableRef, joins: &'a [Join]) -> Vec<&'a Join> {
    let original: Vec<&Join> = joins.iter().collect();
    if joins.len() < 2 || joins.iter().any(|j| j.kind != JoinKind::Inner) {
        return original;
    }
    let mut sizes = Vec::with_capacity(joins.len());
    for j in joins {
        match store.row_count_hint(&j.table.name) {
            Some(n) => sizes.push(n),
            None => return original,
        }
    }

    let mut avail: std::collections::HashSet<String> =
        std::iter::once(from.key().to_string()).collect();
    let mut pending: Vec<usize> = (0..joins.len()).collect();
    let mut order: Vec<&Join> = Vec::with_capacity(joins.len());
    while !pending.is_empty() {
        let mut best: Option<(usize, usize)> = None; // (position in pending, size)
        for (pi, &ji) in pending.iter().enumerate() {
            if join_placeable(&joins[ji].on, &avail, joins[ji].table.key())
                && best.is_none_or(|(_, bs)| sizes[ji] < bs)
            {
                best = Some((pi, sizes[ji]));
            }
        }
        let pi = best.map(|(pi, _)| pi).unwrap_or(0);
        let ji = pending.remove(pi);
        avail.insert(joins[ji].table.key().to_string());
        order.push(&joins[ji]);
    }
    order
}

/// Make the smallest base table the driver (FROM), swapping it with a join
/// entry when that shrinks the starting side. Only fires when every join is a
/// plain INNER join over a base table (no subqueries/LATERAL/outer joins),
/// where the swap can never change the result — the join graph and its ON
/// predicates are unchanged, only which table the planner scans first. The
/// swapped-in join keeps the same ON clause (it still relates the same two
/// tables), and `reorder_joins` re-sequences the rest.
fn choose_driver<S: Store>(store: &S, select: &mut SelectStmt, params: &[Value]) {
    let Some(from) = &select.from else {
        return;
    };
    if select.joins.is_empty() || from.subquery.is_some() {
        return;
    }
    if select
        .joins
        .iter()
        .any(|j| j.kind != JoinKind::Inner || j.table.subquery.is_some() || j.table.lateral)
    {
        return;
    }
    let Some(from_size) = store.row_count_hint(&from.name) else {
        return;
    };
    // Only worth swapping toward a table the WHERE actually shrinks: a raw
    // row-count is misleading (a 500-row table with no equality filter is a
    // worse driver than a 150k table, because it feeds 500 index probes that
    // fan back out). Require a `col = const` filter on the candidate — and
    // none on the current FROM (else FROM is already a fine driver).
    let has_eq = |key: &str| {
        select
            .filter
            .as_ref()
            .is_some_and(|f| !eq_conjuncts(f, key, params).is_empty())
    };
    if has_eq(from.key()) {
        return;
    }
    // Smallest *selectively-filtered* join table by row-count estimate.
    let mut best: Option<(usize, usize)> = None;
    for (i, j) in select.joins.iter().enumerate() {
        if has_eq(j.table.key())
            && let Some(sz) = store.row_count_hint(&j.table.name)
            && best.is_none_or(|(_, bs)| sz < bs)
        {
            best = Some((i, sz));
        }
    }
    if let Some((ji, jsize)) = best
        && jsize < from_size
    {
        // Swap FROM with joins[ji].table; the join's ON still relates the two.
        let old_from = select.from.take().expect("from present");
        let new_from = std::mem::replace(&mut select.joins[ji].table, old_from);
        let new_key = new_from.key().to_string();
        select.from = Some(new_from);
        // A swap is only safe if every join is still reachable from the new
        // driver by its ON predicates — otherwise the chosen table sat mid-
        // chain and the swap orphaned a table (its columns become unresolvable).
        // Undo when that happens.
        if !joins_reachable(&new_key, &select.joins) {
            let restored = select.from.take().expect("from present");
            let orig = std::mem::replace(&mut select.joins[ji].table, restored);
            select.from = Some(orig);
        }
    }
}

/// Whether every join can be sequenced starting from `driver_key`, each
/// becoming placeable (its ON references only already-available tables) as
/// the ones before it are added. This is exactly `reorder_joins`' loop, used
/// here only to validate a candidate driver swap.
fn joins_reachable(driver_key: &str, joins: &[Join]) -> bool {
    let mut avail: std::collections::HashSet<String> =
        std::iter::once(driver_key.to_string()).collect();
    let mut pending: Vec<usize> = (0..joins.len()).collect();
    while !pending.is_empty() {
        let Some(pos) = pending
            .iter()
            .position(|&ji| join_placeable(&joins[ji].on, &avail, joins[ji].table.key()))
        else {
            return false;
        };
        let ji = pending.remove(pos);
        avail.insert(joins[ji].table.key().to_string());
    }
    true
}

/// Build the combined (schema, sources, tuples) for FROM + joins.
fn build_source<S: Store>(
    store: &S,
    select: &SelectStmt,
    params: &[Value],
) -> Result<(Vec<ColRef>, Sources, Tuples)> {
    let needed = collect_needed(select);
    let Some(from) = &select.from else {
        // FROM-less SELECT: one implicit row with no columns. Expressions are
        // evaluated once against it; WHERE can still drop it. (The parser
        // guarantees there are no joins without a FROM.)
        let mut src = Sources::default();
        src.push_table(Chunk {
            width: 0,
            n: 1,
            cells: Vec::new(),
        });
        let tuples = Tuples {
            stride: 1,
            data: vec![0],
        };
        return Ok((Vec::new(), src, tuples));
    };
    let (mut schema, chunk) = if let Some(sub) = &from.subquery {
        derived_source(store, from, sub, &needed, params)?
    } else {
        match store.table_def(&from.name) {
            Some(base_def) => {
                let full = qualified_schema(from.key(), &base_def);
                let keep = keep_indices(&full, from.key(), &needed);
                let schema: Vec<ColRef> = keep.iter().map(|&i| full[i].clone()).collect();
                // Base rows: with no joins, an index probe when WHERE has a
                // usable equality, else a streamed scan pushing WHERE (and a
                // LIMIT cutoff) down. With joins, push this table's own WHERE
                // conjuncts into its scan — unless an outer join can NULL-pad
                // it (a RIGHT/FULL join pads the FROM side).
                let chunk = if select.joins.is_empty() {
                    base_chunk(store, from, select, params, &keep, &full)?
                } else {
                    let paddable = select
                        .joins
                        .iter()
                        .any(|j| matches!(j.kind, JoinKind::Right | JoinKind::Full));
                    let pushed = if paddable {
                        None
                    } else {
                        pushdown_filter(select.filter.as_ref(), &full, params)
                    };
                    match pushed {
                        Some(f) => {
                            filtered_scan(store, &from.name, Some(&f), &full, &keep, params, None)?
                        }
                        None => store.scan_pruned(&from.name, &keep)?,
                    }
                };
                (schema, chunk)
            }
            None => view_source(store, from, &needed)?
                .ok_or_else(|| SqlError::NoSuchTable(from.name.clone()))?,
        }
    };

    let mut src = Sources::default();
    let n = chunk.n;
    src.push_table(chunk);
    let mut tuples = Tuples {
        stride: 1,
        data: (0..n as u32).collect(),
    };

    // A LATERAL right side must run after every table it references, so keep
    // the written order when any join is lateral.
    let ordered: Vec<&Join> = if select.joins.iter().any(|j| j.table.lateral) {
        select.joins.iter().collect()
    } else {
        reorder_joins(store, from, &select.joins)
    };
    // WHERE conjuncts may be pushed into an INNER join's table scan only
    // when no outer join anywhere can NULL-pad rows (conservative; the full
    // WHERE is re-applied after joining either way).
    let any_outer = select
        .joins
        .iter()
        .any(|j| !matches!(j.kind, JoinKind::Inner));
    for join in ordered {
        if join.table.lateral && join.table.subquery.is_some() {
            lateral_join_into(store, join, &mut schema, &mut src, &mut tuples, params)?;
        } else {
            join_into(
                store,
                join,
                &mut schema,
                &mut src,
                &mut tuples,
                params,
                &needed,
                select.filter.as_ref(),
                !any_outer,
            )?;
        }
    }
    Ok((schema, src, tuples))
}

/// Join the next table into the accumulated working set.
///
/// Simple planner: if the `ON` predicate contains at least one equi-join key
/// (`left_col = right_col`), use a **hash join** (O(N+M)); otherwise fall back
/// to a nested-loop join (O(N·M)). A hash-bucket hit already proves the
/// equi-key conjuncts (HashMap key equality), so only the residual (non-equi)
/// part of the ON — if any — is re-evaluated on candidate pairs.
/// The largest left (outer) side for which an index-nested-loop join is
/// attempted. Above this, one full scan of the right table plus a hash join
/// beats many index probes.
const INL_MAX_LEFT: usize = 8_192;

/// Try an index-nested-loop build of the right chunk: instead of scanning the
/// whole right base table, probe its index with each distinct left-side key
/// value and gather only the matching rows. Returns `Ok(None)` (caller
/// full-scans) unless every precondition holds:
/// - INNER or LEFT join (RIGHT/FULL must see unmatched right rows),
/// - exactly one equi-key whose right side is a plain column,
/// - a small left side, and
/// - the right column is actually indexed (probe returns `Some`).
#[allow(clippy::too_many_arguments)]
fn index_nested_loop_chunk<S: Store>(
    store: &S,
    join: &Join,
    keys: &[(Expr, Expr)],
    left_schema: &[ColRef],
    src: &Sources,
    tuples: &Tuples,
    keep: &[usize],
    params: &[Value],
) -> Result<Option<Chunk>> {
    if matches!(join.kind, JoinKind::Right | JoinKind::Full) || keys.len() != 1 {
        return Ok(None);
    }
    let n_left = if tuples.stride == 0 {
        0
    } else {
        tuples.data.len() / tuples.stride
    };
    if n_left == 0 || n_left > INL_MAX_LEFT {
        return Ok(None);
    }
    // The right key must be a plain column reference (its unqualified name is
    // what the index is keyed on).
    let Expr::Column {
        name: right_col, ..
    } = &keys[0].1
    else {
        return Ok(None);
    };
    let right_col = right_col.clone();

    // Distinct left key values (NULLs never equi-match, so skip them).
    let left_key = bind_expr(&keys[0].0, left_schema)?;
    let mut distinct: std::collections::BTreeSet<IndexKey> = std::collections::BTreeSet::new();
    for lt in tuples.data.chunks_exact(tuples.stride) {
        let view = View { src, tuple: lt };
        let v = eval_scalar(&left_key, left_schema, &view, params)?;
        if !matches!(v, Value::Null) {
            distinct.insert(IndexKey(v));
        }
    }
    if distinct.is_empty() {
        return Ok(Some(Chunk::from_rows(std::iter::empty(), keep)));
    }

    // Probe the index per distinct value, deduping right rows by row_id. The
    // first probe decides whether the column is indexed at all.
    let mut rows: std::collections::BTreeMap<u64, Vec<Value>> = std::collections::BTreeMap::new();
    for (i, key) in distinct.into_iter().enumerate() {
        let hit = store.index_lookup_eq(&join.table.name, &[(right_col.clone(), key.0)])?;
        match hit {
            Some(matched) => {
                for (rid, cells) in matched {
                    rows.entry(rid).or_insert(cells);
                }
            }
            // No index on the right column — abandon on the very first probe.
            None if i == 0 => return Ok(None),
            None => return Ok(None),
        }
    }
    Ok(Some(Chunk::from_rows(rows.into_values(), keep)))
}

/// A materialized subquery result: (columns, types, rows).
type SubRows = (Vec<String>, Vec<Option<SqlType>>, Vec<Vec<Value>>);

/// Join a LATERAL derived table into the working set: rewrite its references
/// to left-side columns into synthetic parameter slots (the same machinery as
/// correlated subqueries), then re-execute the subquery once per left tuple
/// with those slots bound to the tuple's values.
fn lateral_join_into<S: Store>(
    store: &S,
    join: &Join,
    schema: &mut Vec<ColRef>,
    src: &mut Sources,
    tuples: &mut Tuples,
    params: &[Value],
) -> Result<()> {
    let sub = join
        .table
        .subquery
        .as_deref()
        .expect("lateral without a subquery");
    let mut sub = sub.clone();
    let base = params.len();
    let outer = extract_outer(store, &mut sub, &Scope::from_schema(schema), base);
    let sub = Statement::Select(sub);

    // Run the subquery with the outer slots bound; returns (columns, types,
    // rows). The schema comes from the first run (or a NULL-bound probe when
    // there are no left tuples at all).
    let run = |vals: Vec<Value>| -> Result<SubRows> {
        let mut p2 = Vec::with_capacity(base + vals.len());
        p2.extend_from_slice(params);
        p2.extend(vals);
        match execute(store, sub.clone(), &p2)? {
            QueryResult::Select {
                columns,
                types,
                rows,
            } => Ok((columns, types, rows)),
            _ => unreachable!("subquery produced a non-select result"),
        }
    };

    let stride = tuples.stride;
    let n_left = if stride == 0 {
        0
    } else {
        tuples.data.len() / stride
    };
    let mut header: Option<(Vec<String>, Vec<Option<SqlType>>)> = None;
    let mut right_rows: Vec<Vec<Value>> = Vec::new();
    // Right-row span per left tuple.
    let mut ranges: Vec<(usize, usize)> = Vec::with_capacity(n_left);
    for lt in tuples.data.chunks_exact(stride.max(1)).take(n_left) {
        let view = View { src, tuple: lt };
        let vals = outer
            .iter()
            .map(|o| eval_scalar(o, schema, &view, params))
            .collect::<Result<Vec<_>>>()?;
        let (columns, types, rows) = run(vals)?;
        if header.is_none() {
            header = Some((columns, types));
        }
        let start = right_rows.len();
        right_rows.extend(rows);
        ranges.push((start, right_rows.len()));
    }
    let (columns, types) = match header {
        Some(h) => h,
        // No left tuples: probe with NULL slots only to learn the columns.
        None => {
            let (columns, types, _) = run(vec![Value::Null; outer.len()])?;
            (columns, types)
        }
    };
    let columns = rename_columns(columns, &join.table.alias_columns, join.table.key())?;

    let right_schema: Vec<ColRef> = columns
        .iter()
        .zip(&types)
        .map(|(c, ty)| ColRef {
            table: join.table.key().to_string(),
            name: c.clone(),
            ty: *ty,
        })
        .collect();
    let width = right_schema.len();
    let mut combined = schema.clone();
    combined.extend(right_schema);

    let on_trivial = matches!(join.on, Expr::Literal(Value::Bool(true)));
    let on = bind_expr(&join.on, &combined)?;
    let want_left = matches!(join.kind, JoinKind::Left);

    let keep: Vec<usize> = (0..width).collect();
    src.push_table(Chunk::from_rows(right_rows, &keep));
    let mut out: Vec<u32> = Vec::new();
    let mut cand: Vec<u32> = vec![0; stride + 1];
    for (li, lt) in tuples
        .data
        .chunks_exact(stride.max(1))
        .take(n_left)
        .enumerate()
    {
        cand[..stride].copy_from_slice(lt);
        let (start, end) = ranges[li];
        let mut matched = false;
        for ri in start..end {
            cand[stride] = ri as u32;
            let ok = on_trivial || {
                let view = View { src, tuple: &cand };
                truthy(&eval_scalar(&on, &combined, &view, params)?)
            };
            if ok {
                matched = true;
                out.extend_from_slice(&cand);
            }
        }
        if want_left && !matched {
            cand[stride] = NULL_ROW;
            out.extend_from_slice(&cand);
        }
    }
    *schema = combined;
    *tuples = Tuples {
        stride: stride + 1,
        data: out,
    };
    Ok(())
}

// A join-execution step; bundling these into a struct would only move the
// list somewhere else.
#[allow(clippy::too_many_arguments)]
fn join_into<S: Store>(
    store: &S,
    join: &Join,
    schema: &mut Vec<ColRef>,
    src: &mut Sources,
    tuples: &mut Tuples,
    params: &[Value],
    needed: &Needed,
    where_filter: Option<&Expr>,
    filter_pushable: bool,
) -> Result<()> {
    // Resolve the right schema up front, but defer materializing its rows for a
    // base table: an index-nested-loop join can then prune the scan to only the
    // rows the (small) left side needs.
    enum RightSrc {
        Ready(Chunk),
        Base { keep: Vec<usize>, full: Vec<ColRef> },
    }
    let (right_schema, right_src) = if let Some(sub) = &join.table.subquery {
        let (rs, chunk) = derived_source(store, &join.table, sub, needed, params)?;
        (rs, RightSrc::Ready(chunk))
    } else {
        match store.table_def(&join.table.name) {
            Some(def) => {
                let full = qualified_schema(join.table.key(), &def);
                let keep = keep_indices(&full, join.table.key(), needed);
                let right_schema: Vec<ColRef> = keep.iter().map(|&i| full[i].clone()).collect();
                (right_schema, RightSrc::Base { keep, full })
            }
            None => {
                let (rs, chunk) = view_source(store, &join.table, needed)?
                    .ok_or_else(|| SqlError::NoSuchTable(join.table.name.clone()))?;
                (rs, RightSrc::Ready(chunk))
            }
        }
    };

    let left_len = schema.len();
    let mut combined = schema.clone();
    combined.extend(right_schema.iter().cloned());

    // Split the raw (named) ON into equi-key pairs and a residual predicate.
    let (keys, residual) = split_on(&join.on, left_len, &combined);

    // Materialize the right rows. For a base table, try an index-nested-loop
    // prune first (below); otherwise fall back to a full scan.
    let chunk = match right_src {
        RightSrc::Ready(chunk) => chunk,
        RightSrc::Base { keep, full } => {
            match index_nested_loop_chunk(store, join, &keys, schema, src, tuples, &keep, params)? {
                Some(chunk) => chunk,
                None => {
                    // Push this table's own WHERE conjuncts into the scan
                    // (INNER side only; the caller re-applies the full WHERE).
                    let pushed = if filter_pushable {
                        pushdown_filter(where_filter, &full, params)
                    } else {
                        None
                    };
                    match pushed {
                        Some(f) => filtered_scan(
                            store,
                            &join.table.name,
                            Some(&f),
                            &full,
                            &keep,
                            params,
                            None,
                        )?,
                        None => store.scan_pruned(&join.table.name, &keep)?,
                    }
                }
            }
        }
    };
    let nright = chunk.n;
    let residual = residual
        .as_ref()
        .map(|r| bind_expr(r, &combined))
        .transpose()?;

    let want_left = matches!(join.kind, JoinKind::Left | JoinKind::Full);
    let want_right = matches!(join.kind, JoinKind::Right | JoinKind::Full);

    let stride = tuples.stride;
    src.push_table(chunk);
    let right = src.chunks.last().expect("just pushed");

    let mut right_matched = vec![false; nright];
    let mut out: Vec<u32> = Vec::new();
    let mut cand: Vec<u32> = vec![0; stride + 1];

    if keys.is_empty() {
        // ── Nested-loop join ──
        let on = bind_expr(&join.on, &combined)?;
        for lt in tuples.data.chunks_exact(stride) {
            cand[..stride].copy_from_slice(lt);
            let mut left_matched = false;
            for (ri, matched) in right_matched.iter_mut().enumerate() {
                cand[stride] = ri as u32;
                let view = View { src, tuple: &cand };
                if truthy(&eval_scalar(&on, &combined, &view, params)?) {
                    left_matched = true;
                    *matched = true;
                    out.extend_from_slice(&cand);
                }
            }
            if want_left && !left_matched {
                cand[stride] = NULL_ROW;
                out.extend_from_slice(&cand);
            }
        }
    } else {
        // ── Hash join ──
        // Build an index over the right rows keyed by the right-side keys.
        // Rows with a NULL (or NaN) in any key component never equi-match.
        let left_keys: Vec<Expr> = keys
            .iter()
            .map(|(l, _)| bind_expr(l, schema))
            .collect::<Result<_>>()?;
        let right_keys: Vec<Expr> = keys
            .iter()
            .map(|(_, r)| bind_expr(r, &right_schema))
            .collect::<Result<_>>()?;

        let index = RightIndex::build(&right_keys, &right_schema, right, params)?;

        let probe = ProbeCtx {
            stride,
            src,
            index: &index,
            left_keys: &left_keys,
            left_schema: schema,
            combined: &combined,
            residual: &residual,
            params,
            want_left,
            track_right: want_right,
            nright,
        };
        let n_left = tuples.data.len() / stride;
        if n_left >= PAR_THRESHOLD {
            // Chunked parallel probe. Chunk outputs concatenate in chunk
            // order, so the emitted rows are identical to the sequential
            // loop; per-chunk right-matched bitmaps are OR-merged.
            use rayon::prelude::*;
            const TUPLES_PER_CHUNK: usize = 8_192;
            let parts: Vec<(Vec<u32>, Vec<bool>)> = tuples
                .data
                .par_chunks(TUPLES_PER_CHUNK * stride)
                .map(|lchunk| probe.run(lchunk))
                .collect::<Result<_>>()?;
            for (part_out, part_matched) in parts {
                out.extend_from_slice(&part_out);
                for (ri, m) in part_matched.into_iter().enumerate() {
                    if m {
                        right_matched[ri] = true;
                    }
                }
            }
        } else {
            let (seq_out, seq_matched) = probe.run(&tuples.data)?;
            out = seq_out;
            right_matched = seq_matched;
        }
    }

    // RIGHT/FULL: emit unmatched right rows padded with NULLs on the left.
    if want_right {
        for (ri, matched) in right_matched.iter().enumerate() {
            if !matched {
                out.extend(std::iter::repeat_n(NULL_ROW, stride));
                out.push(ri as u32);
            }
        }
    }

    *schema = combined;
    tuples.stride = stride + 1;
    tuples.data = out;
    Ok(())
}

/// A hashable, equality-consistent projection of a join-key [`Value`].
/// Numeric kinds collapse to one space (so `Int(5)` and `Double(5.0)` hash
/// equal, matching the value comparison); NULL keys are handled by the caller.
#[derive(Clone, PartialEq, Eq, Hash)]
enum HashKey {
    Num(u64),
    Bool(bool),
    Text(String),
    Bytes(Vec<u8>),
}

/// A join key of one or more components. The common 1–2 component cases avoid
/// a per-row `Vec` allocation.
#[derive(PartialEq, Eq, Hash)]
enum JoinKey {
    One(HashKey),
    Two(HashKey, HashKey),
    Many(Vec<HashKey>),
}

fn hash_key_component(v: &Value) -> Option<HashKey> {
    let norm = |f: f64| (if f == 0.0 { 0.0 } else { f }).to_bits();
    match v {
        Value::Null => None,
        Value::Bytes(b) => Some(HashKey::Bytes(b.to_vec())),
        Value::Int(n) => Some(HashKey::Num(norm(*n as f64))),
        // NaN = NaN is not true in SQL, so a NaN key can never equi-match —
        // exclude it (like NULL) rather than let bit-equality pair two NaNs.
        Value::Double(f) if f.is_nan() => None,
        Value::Double(f) => Some(HashKey::Num(norm(*f))),
        Value::Timestamp(t) => Some(HashKey::Num(norm(*t as f64))),
        // Decimal joins on its numeric value (consistent with total_order,
        // which treats 2 and 2.00 as equal — both hash to the same f64 bits).
        Value::Decimal(d) => Some(HashKey::Num(norm(d.to_f64()))),
        Value::Bool(b) => Some(HashKey::Bool(*b)),
        Value::Text(s) => Some(HashKey::Text(s.to_string())),
    }
}

/// Evaluate the key expressions over `row`; returns `None` if any component is
/// NULL (a NULL key never equi-matches).
fn join_key<R: RowLike + ?Sized>(
    exprs: &[Expr],
    schema: &[ColRef],
    row: &R,
    params: &[Value],
) -> Result<Option<JoinKey>> {
    match exprs {
        [a] => Ok(hash_key_component(&eval_scalar(a, schema, row, params)?).map(JoinKey::One)),
        [a, b] => {
            let ka = hash_key_component(&eval_scalar(a, schema, row, params)?);
            let kb = hash_key_component(&eval_scalar(b, schema, row, params)?);
            Ok(match (ka, kb) {
                (Some(x), Some(y)) => Some(JoinKey::Two(x, y)),
                _ => None,
            })
        }
        _ => {
            let mut key = Vec::with_capacity(exprs.len());
            for e in exprs {
                match hash_key_component(&eval_scalar(e, schema, row, params)?) {
                    Some(k) => key.push(k),
                    None => return Ok(None),
                }
            }
            Ok(Some(JoinKey::Many(key)))
        }
    }
}

/// End-of-chain sentinel in a [`RightIndex`] bucket chain.
const CHAIN_END: u32 = u32::MAX;

/// Row-count threshold above which join build/probe loops run on the rayon
/// pool (below it, thread fan-out costs more than it saves).
const PAR_THRESHOLD: usize = 32_768;

/// The numeric key of a value if it is exactly an integer (`Int`, `Timestamp`,
/// or an integral `Double` — the numeric kinds compare equal across types).
#[inline]
fn int_key(v: &Value) -> Option<i64> {
    const I64_MAX_F: f64 = 9_223_372_036_854_775_807.0;
    match v {
        Value::Int(i) => Some(*i),
        Value::Timestamp(t) => Some(*t),
        Value::Double(f) if f.fract() == 0.0 && *f >= -I64_MAX_F && *f <= I64_MAX_F => {
            Some(*f as i64)
        }
        _ => None,
    }
}

/// The build side of a hash join. Bucket chains live in a flat `next` array
/// (no per-key `Vec`), preserving right-row order within a key. When the key
/// is a single, densely-packed integer column (the typical `fk = pk` case),
/// the buckets are a direct-address array — no hashing at all.
enum RightIndex {
    Dense {
        min: i64,
        heads: Vec<u32>,
        next: Vec<u32>,
    },
    Map {
        map: FxMap<JoinKey, (u32, u32)>, // key -> (chain head, chain tail)
        next: Vec<u32>,
    },
}

/// Everything one hash-join probe pass needs, bundled so the sequential and
/// parallel paths share the exact same body.
struct ProbeCtx<'a> {
    stride: usize,
    src: &'a Sources,
    index: &'a RightIndex,
    left_keys: &'a [Expr],
    left_schema: &'a [ColRef],
    combined: &'a [ColRef],
    residual: &'a Option<Expr>,
    params: &'a [Value],
    want_left: bool,
    track_right: bool,
    nright: usize,
}

impl ProbeCtx<'_> {
    /// Probe a slice of left tuples, returning the emitted output tuples and
    /// (when tracking) which right rows matched.
    fn run(&self, lchunk: &[u32]) -> Result<(Vec<u32>, Vec<bool>)> {
        let stride = self.stride;
        let mut out: Vec<u32> = Vec::new();
        let mut matched: Vec<bool> = if self.track_right {
            vec![false; self.nright]
        } else {
            Vec::new()
        };
        let mut cand: Vec<u32> = vec![0; stride + 1];
        for lt in lchunk.chunks_exact(stride) {
            cand[..stride].copy_from_slice(lt);
            let mut left_matched = false;
            let lview = View {
                src: self.src,
                tuple: &cand[..stride],
            };
            let mut chain =
                self.index
                    .probe(self.left_keys, self.left_schema, &lview, self.params)?;
            while chain != CHAIN_END {
                let ri = chain;
                chain = self.index.next(ri);
                cand[stride] = ri;
                let keep = match self.residual {
                    None => true,
                    Some(res) => {
                        let view = View {
                            src: self.src,
                            tuple: &cand,
                        };
                        truthy(&eval_scalar(res, self.combined, &view, self.params)?)
                    }
                };
                if keep {
                    left_matched = true;
                    if self.track_right {
                        matched[ri as usize] = true;
                    }
                    out.extend_from_slice(&cand);
                }
            }
            if self.want_left && !left_matched {
                cand[stride] = NULL_ROW;
                out.extend_from_slice(&cand);
            }
        }
        Ok((out, matched))
    }
}

impl RightIndex {
    fn build(
        right_keys: &[Expr],
        right_schema: &[ColRef],
        right: &Chunk,
        params: &[Value],
    ) -> Result<RightIndex> {
        let nright = right.n;

        // Single-component key: check for the dense-int case first. The key
        // evaluation is embarrassingly parallel; the fold stays sequential.
        if let [key_expr] = right_keys {
            let keyvals: Vec<Value> = if nright >= PAR_THRESHOLD {
                use rayon::prelude::*;
                (0..nright)
                    .into_par_iter()
                    .map(|ri| eval_scalar(key_expr, right_schema, right.row(ri), params))
                    .collect::<Result<_>>()?
            } else {
                (0..nright)
                    .map(|ri| eval_scalar(key_expr, right_schema, right.row(ri), params))
                    .collect::<Result<_>>()?
            };
            let mut vals: Vec<Option<i64>> = Vec::with_capacity(nright);
            let mut all_int = true;
            let (mut min, mut max) = (i64::MAX, i64::MIN);
            for v in &keyvals {
                if matches!(v, Value::Null) {
                    vals.push(None);
                    continue;
                }
                match int_key(v) {
                    Some(k) => {
                        min = min.min(k);
                        max = max.max(k);
                        vals.push(Some(k));
                    }
                    None => {
                        all_int = false;
                        break;
                    }
                }
            }
            if all_int {
                let range = if min > max {
                    0 // no non-null keys
                } else {
                    (max as i128 - min as i128) + 1
                };
                // Only direct-address when the key space is comparably sized
                // to the row count (the two build arrays cost 8 bytes/slot,
                // so this caps the memory overhead at ~128 bytes/row).
                if range <= (nright as i128) * 16 + 65_536 {
                    let mut heads = vec![CHAIN_END; range as usize];
                    let mut tails = vec![CHAIN_END; range as usize];
                    let mut next = vec![CHAIN_END; nright];
                    for (ri, k) in vals.iter().enumerate() {
                        let Some(k) = k else { continue };
                        let slot = (k - min) as usize;
                        if heads[slot] == CHAIN_END {
                            heads[slot] = ri as u32;
                        } else {
                            next[tails[slot] as usize] = ri as u32;
                        }
                        tails[slot] = ri as u32;
                    }
                    return Ok(RightIndex::Dense { min, heads, next });
                }
            }
        }

        // General case: hash map from key to chain head/tail.
        let mut map: FxMap<JoinKey, (u32, u32)> =
            FxMap::with_capacity_and_hasher(nright, Default::default());
        let mut next = vec![CHAIN_END; nright];
        for ri in 0..nright {
            if let Some(key) = join_key(right_keys, right_schema, right.row(ri), params)? {
                match map.entry(key) {
                    std::collections::hash_map::Entry::Vacant(e) => {
                        e.insert((ri as u32, ri as u32));
                    }
                    std::collections::hash_map::Entry::Occupied(mut e) => {
                        let (_, tail) = *e.get();
                        next[tail as usize] = ri as u32;
                        e.get_mut().1 = ri as u32;
                    }
                }
            }
        }
        Ok(RightIndex::Map { map, next })
    }

    /// Head of the bucket chain matching the left row's key ([`CHAIN_END`] if
    /// none).
    fn probe<R: RowLike + ?Sized>(
        &self,
        left_keys: &[Expr],
        schema: &[ColRef],
        row: &R,
        params: &[Value],
    ) -> Result<u32> {
        match self {
            RightIndex::Dense { min, heads, .. } => {
                let v = eval_scalar(&left_keys[0], schema, row, params)?;
                // Non-integer keys (Text/Bool/fractional/NULL/NaN) can never
                // equal an integer right key — no match, as in the hash path.
                Ok(match int_key(&v) {
                    Some(k) => {
                        let off = k as i128 - *min as i128;
                        if off >= 0 && off < heads.len() as i128 {
                            heads[off as usize]
                        } else {
                            CHAIN_END
                        }
                    }
                    _ => CHAIN_END,
                })
            }
            RightIndex::Map { map, .. } => Ok(match join_key(left_keys, schema, row, params)? {
                Some(key) => map.get(&key).map(|(head, _)| *head).unwrap_or(CHAIN_END),
                None => CHAIN_END,
            }),
        }
    }

    /// Next right row in the same bucket chain.
    #[inline]
    fn next(&self, ri: u32) -> u32 {
        match self {
            RightIndex::Dense { next, .. } | RightIndex::Map { next, .. } => next[ri as usize],
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Side {
    Left,
    Right,
}

/// Split an `ON` predicate into equi-join key pairs `(left_expr, right_expr)`
/// — top-level `AND` conjuncts of the form `E1 = E2` where one side references
/// only left-schema columns and the other only right-schema columns — plus the
/// residual predicate (the AND of every conjunct that is *not* an equi-key).
/// `residual == None` means the ON is exactly the equi-keys, so a hash-table
/// bucket match alone proves the ON holds.
#[allow(clippy::type_complexity)]
fn split_on(on: &Expr, left_len: usize, combined: &[ColRef]) -> (Vec<(Expr, Expr)>, Option<Expr>) {
    let mut keys = Vec::new();
    let mut residual: Option<Expr> = None;
    collect_equi(on, left_len, combined, &mut keys, &mut residual);
    (keys, residual)
}

fn collect_equi(
    e: &Expr,
    left_len: usize,
    combined: &[ColRef],
    keys: &mut Vec<(Expr, Expr)>,
    residual: &mut Option<Expr>,
) {
    let leftover = match e {
        Expr::Binary {
            op: BinOp::And,
            left,
            right,
        } => {
            collect_equi(left, left_len, combined, keys, residual);
            collect_equi(right, left_len, combined, keys, residual);
            return;
        }
        Expr::Binary {
            op: BinOp::Eq,
            left,
            right,
        } => match (
            expr_side(left, left_len, combined),
            expr_side(right, left_len, combined),
        ) {
            (Some(Side::Left), Some(Side::Right)) => {
                keys.push(((**left).clone(), (**right).clone()));
                return;
            }
            (Some(Side::Right), Some(Side::Left)) => {
                keys.push(((**right).clone(), (**left).clone()));
                return;
            }
            _ => e.clone(),
        },
        _ => e.clone(),
    };
    *residual = Some(match residual.take() {
        None => leftover,
        Some(prev) => Expr::Binary {
            op: BinOp::And,
            left: Box::new(prev),
            right: Box::new(leftover),
        },
    });
}

/// Which side of the join an expression's columns belong to: `Some(Left)` /
/// `Some(Right)` if all its columns are on one side, `None` if it references
/// both sides, no columns, or a column that doesn't resolve.
fn expr_side(e: &Expr, left_len: usize, combined: &[ColRef]) -> Option<Side> {
    let mut cols = Vec::new();
    collect_col_refs(e, &mut cols);
    let mut side: Option<Side> = None;
    for (table, name) in cols {
        let idx = resolve_col(combined, table, name).ok()?;
        let s = if idx < left_len {
            Side::Left
        } else {
            Side::Right
        };
        match side {
            None => side = Some(s),
            Some(prev) if prev != s => return None,
            _ => {}
        }
    }
    side
}

/// Collect every column reference in a query body's own expressions (used to
/// keep the left-side columns a LATERAL subquery names).
fn collect_body_col_refs<'a>(body: &'a QueryBody, refs: &mut Vec<(&'a Option<String>, &'a str)>) {
    match body {
        QueryBody::Select(s) => {
            for item in &s.projection {
                if let SelectItem::Expr { expr, .. } = item {
                    collect_col_refs(expr, refs);
                }
            }
            if let Some(f) = &s.filter {
                collect_col_refs(f, refs);
            }
            if let Some(from) = &s.from
                && let Some(sub) = &from.subquery
            {
                collect_body_col_refs(&sub.body, refs);
            }
            for j in &s.joins {
                collect_col_refs(&j.on, refs);
                if let Some(sub) = &j.table.subquery {
                    collect_body_col_refs(&sub.body, refs);
                }
            }
            for e in &s.group_by {
                collect_col_refs(e, refs);
            }
            if let Some(h) = &s.having {
                collect_col_refs(h, refs);
            }
            for (e, _) in &s.order_by {
                collect_col_refs(e, refs);
            }
        }
        QueryBody::SetOp { left, right, .. } => {
            collect_body_col_refs(left, refs);
            collect_body_col_refs(right, refs);
        }
        QueryBody::Values(_) => {}
    }
}

fn collect_col_refs<'a>(e: &'a Expr, out: &mut Vec<(&'a Option<String>, &'a str)>) {
    match e {
        Expr::Column { table, name } => out.push((table, name)),
        Expr::Binary { left, right, .. } => {
            collect_col_refs(left, out);
            collect_col_refs(right, out);
        }
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } => collect_col_refs(expr, out),
        Expr::Aggregate { arg, .. } => {
            if let Some(a) = arg {
                collect_col_refs(a, out);
            }
        }
        Expr::Func { args, .. } => {
            for a in args {
                collect_col_refs(a, out);
            }
        }
        Expr::InSet { expr, .. } => collect_col_refs(expr, out),
        Expr::In { expr, list, .. } => {
            collect_col_refs(expr, out);
            for item in list {
                collect_col_refs(item, out);
            }
        }
        // Subqueries resolve to literals before any column analysis; their
        // inner column references belong to the subquery's own scope. A
        // correlated node's outer refs and probe DO belong to this scope.
        Expr::CorrScalar { outer, .. } => {
            for o in outer {
                collect_col_refs(o, out);
            }
        }
        Expr::CorrIn { expr, outer, .. } => {
            collect_col_refs(expr, out);
            for o in outer {
                collect_col_refs(o, out);
            }
        }
        Expr::Subquery(_) | Expr::InSubquery { .. } => {}
        Expr::Window {
            func,
            partition_by,
            order_by,
        } => {
            for e in partition_by {
                collect_col_refs(e, out);
            }
            for (e, _) in order_by {
                collect_col_refs(e, out);
            }
            if let WindowFunc::Agg(_, Some(a)) = func {
                collect_col_refs(a, out);
            }
        }
        // `Col` only appears after binding; equi-key detection runs before that.
        Expr::Col(_) | Expr::Literal(_) | Expr::Param(_) => {}
    }
}

/// Fetch base rows for a single (join-free) table as a pruned chunk, using an
/// index when possible.
fn base_chunk<S: Store>(
    store: &S,
    from: &TableRef,
    select: &SelectStmt,
    params: &[Value],
    keep: &[usize],
    full: &[ColRef],
) -> Result<Chunk> {
    if let Some(expr) = &select.filter {
        let eqs = eq_conjuncts(expr, from.key(), params);
        if !eqs.is_empty()
            && let Some(rows) = store.index_lookup_eq(&from.name, &eqs)?
        {
            return Ok(Chunk::from_rows(rows.into_iter().map(|(_, c)| c), keep));
        }
    }
    if let Some(chunk) = streamed_chunk(store, from, select, params, keep, full)? {
        return Ok(chunk);
    }
    store.scan_pruned(&from.name, keep)
}

/// One accumulator, folding values as a scan produces them.
enum Acc {
    /// `COUNT(*)` — no argument to evaluate.
    CountStar(i64),
    Count(i64),
    Sum(Option<f64>, bool),
    Avg(f64, i64),
    Min(Option<Value>),
    Max(Option<Value>),
}

impl Acc {
    /// `func` is one of the shapes `streamed_aggregate`'s guard admits;
    /// anything else (`MODE`, ordered-set aggregates) is rejected there so this
    /// never sees it.
    fn new(func: AggFunc, star: bool) -> Acc {
        match (func, star) {
            (AggFunc::Mode, _) => unreachable!("rejected by the guard"),
            (AggFunc::Count, true) => Acc::CountStar(0),
            (AggFunc::Count, false) => Acc::Count(0),
            // `is_int` tracks whether every folded value was an integer, so
            // SUM over an integer column returns an integer as it does on the
            // general path rather than silently becoming a float.
            (AggFunc::Sum, _) => Acc::Sum(None, true),
            (AggFunc::Avg, _) => Acc::Avg(0.0, 0),
            (AggFunc::Min, _) => Acc::Min(None),
            (AggFunc::Max, _) => Acc::Max(None),
        }
    }

    fn fold(&mut self, v: &Value) {
        if let Acc::CountStar(n) = self {
            *n += 1;
            return;
        }
        if matches!(v, Value::Null) {
            return; // SQL: aggregates other than COUNT(*) skip NULLs
        }
        match self {
            Acc::CountStar(_) => unreachable!("handled above"),
            Acc::Count(n) => *n += 1,
            Acc::Sum(acc, is_int) => {
                if !matches!(v, Value::Int(_)) {
                    *is_int = false;
                }
                if let Some(f) = num(v) {
                    *acc = Some(acc.unwrap_or(0.0) + f);
                }
            }
            Acc::Avg(total, n) => {
                if let Some(f) = num(v) {
                    *total += f;
                    *n += 1;
                }
            }
            Acc::Min(best) => {
                if best
                    .as_ref()
                    .is_none_or(|c| Value::total_order(v, c) == Ordering::Less)
                {
                    *best = Some(v.clone());
                }
            }
            Acc::Max(best) => {
                if best
                    .as_ref()
                    .is_none_or(|c| Value::total_order(v, c) == Ordering::Greater)
                {
                    *best = Some(v.clone());
                }
            }
        }
    }

    fn finish(self) -> Value {
        match self {
            Acc::CountStar(n) | Acc::Count(n) => Value::Int(n),
            Acc::Sum(None, _) => Value::Null,
            Acc::Sum(Some(f), true) => Value::Int(f as i64),
            Acc::Sum(Some(f), false) => Value::Double(f),
            Acc::Avg(_, 0) => Value::Null,
            Acc::Avg(total, n) => Value::Double(total / n as f64),
            Acc::Min(v) | Acc::Max(v) => v.unwrap_or(Value::Null),
        }
    }
}

fn num(v: &Value) -> Option<f64> {
    match v {
        Value::Int(n) => Some(*n as f64),
        Value::Double(f) => Some(*f),
        Value::Decimal(d) => Some(d.to_f64()),
        Value::Timestamp(t) => Some(*t as f64),
        _ => None,
    }
}

/// Fold a single-table aggregate during the scan instead of materializing the
/// rows and aggregating over them.
///
/// The general path builds every source row into a `Chunk`, then indexes into
/// it per group. For `SELECT sum(total) FROM orders WHERE id > 100` that is two
/// passes over 400,000 rows and a 400,000-element vector, to produce one row.
/// This makes it one pass and no vector, which is what closes most of the gap
/// to PostgreSQL on aggregates.
///
/// `Ok(None)` for anything outside the narrow shape it handles, so the general
/// path stays authoritative — the guard below is deliberately conservative and
/// every rejection is a query that still runs, just the old way.
fn streamed_aggregate<S: Store>(
    store: &S,
    select: &SelectStmt,
    params: &[Value],
) -> Result<Option<QueryResult>> {
    // Shape: one base table, no joins, no ordering/limiting/distinct, no
    // windows. HAVING is allowed — it is applied to finished groups.
    let Some(from) = &select.from else {
        return Ok(None);
    };
    if from.subquery.is_some()
        || !select.joins.is_empty()
        || !select.order_by.is_empty()
        || select.limit.is_some()
        || select.offset.is_some()
        || select.distinct
    {
        return Ok(None);
    }
    let Some(base_def) = store.table_def(&from.name) else {
        return Ok(None);
    };
    let full = qualified_schema(from.key(), &base_def);

    // Every projection item must be a group key or an aggregate over a
    // streamable expression; nothing nested (`sum(x) + 1`) and no DISTINCT.
    let proj = expand_projection(&select.projection, &full)?;
    if proj.is_empty() {
        return Ok(None);
    }
    let group: Vec<Expr> = select
        .group_by
        .iter()
        .map(|e| bind_expr(e, &full))
        .collect::<Result<_>>()?;
    // Per projection item: fold an aggregate, or echo group key `i`.
    enum Item {
        Agg(AggFunc, Option<Expr>),
        Key(usize),
    }
    let mut plan: Vec<Item> = Vec::new();
    for (_, e) in &proj {
        let bound = bind_expr(e, &full)?;
        match &bound {
            Expr::Aggregate {
                func,
                arg,
                distinct: false,
            } => {
                // MODE is an ordered-set aggregate; it needs the values, not
                // a fold, so it stays on the general path.
                if *func == AggFunc::Mode {
                    return Ok(None);
                }
                let arg = match arg.as_deref() {
                    None => None,
                    Some(a) if streamable(a, params) && !has_aggregate(a) => Some(a.clone()),
                    Some(_) => return Ok(None),
                };
                // SUM and AVG fold through `f64` here, which is exact for
                // integers and doubles and *not* for DECIMAL — the general
                // path adds decimals exactly, and a fast path that quietly
                // rounded them would be a wrong answer, not a slower one. So
                // only stream arithmetic over types where the fold is exact,
                // and only when the type is actually known.
                if matches!(func, AggFunc::Sum | AggFunc::Avg) {
                    let ty = arg.as_ref().and_then(|a| expr_type(a, &full));
                    if !matches!(
                        ty,
                        Some(SqlType::Int) | Some(SqlType::Double) | Some(SqlType::Timestamp)
                    ) {
                        return Ok(None);
                    }
                }
                plan.push(Item::Agg(*func, arg));
            }
            other if !has_aggregate(other) => {
                // A plain expression is only legal here if it is a group key.
                match group.iter().position(|g| g == other) {
                    Some(i) => plan.push(Item::Key(i)),
                    None => return Ok(None),
                }
            }
            _ => return Ok(None),
        }
    }
    if group.iter().any(|g| !streamable(g, params)) {
        return Ok(None);
    }
    // Nothing to stream if there are no aggregates at all.
    if !plan.iter().any(|p| matches!(p, Item::Agg(..))) {
        return Ok(None);
    }
    let filter = match &select.filter {
        None => None,
        Some(f) => {
            let b = bind_expr(f, &full)?;
            if !streamable(&b, params) || has_aggregate(&b) {
                return Ok(None);
            }
            Some(b)
        }
    };
    let having = match &select.having {
        None => None,
        Some(h) => Some(bind_expr(h, &full)?),
    };

    // Fold. One entry per group; `None` key for the no-GROUP-BY case, which
    // still produces exactly one row even over an empty table.
    let mut groups: Vec<(Vec<Value>, Vec<Acc>)> = Vec::new();
    let mut index: std::collections::BTreeMap<Vec<IndexKey>, usize> =
        std::collections::BTreeMap::new();
    let fresh = || -> Vec<Acc> {
        plan.iter()
            .map(|p| match p {
                Item::Agg(f, a) => Acc::new(*f, a.is_none()),
                Item::Key(_) => Acc::CountStar(0), // unused for key positions
            })
            .collect()
    };
    if group.is_empty() {
        groups.push((Vec::new(), fresh()));
    }
    // An equality filter an index can serve must still go through the index —
    // folding is about avoiding materialization, not about giving up index
    // access. Bypassing this turned `count(*) WHERE customer_id = ?` from a
    // 25µs probe into a 4ms full scan, which the benchmark caught immediately.
    let index_eqs = match &select.filter {
        Some(e) => {
            let eqs = eq_conjuncts(e, from.key(), params);
            match eqs.is_empty() {
                true => None,
                false => Some(eqs),
            }
        }
        None => None,
    };

    let mut key: Vec<IndexKey> = Vec::with_capacity(group.len());
    let mut fold_row = |row: &[Value]| -> Result<()> {
        if let Some(f) = &filter
            && !truthy(&eval_scalar(f, &full, row, params)?)
        {
            return Ok(());
        }
        let at = if group.is_empty() {
            0
        } else {
            key.clear();
            for g in &group {
                key.push(IndexKey(eval_scalar(g, &full, row, params)?));
            }
            // `IndexKey` orders values but does not hash them, so groups live
            // in a BTreeMap. The lookup borrows the reused buffer — `Vec<K>`
            // borrows as `[K]` — so a row that lands in an existing group
            // allocates nothing. Building a fresh key vector per row cost more
            // than the grouping itself on a table with few distinct keys.
            match index.get(key.as_slice()) {
                Some(i) => *i,
                None => {
                    groups.push((key.iter().map(|k| k.0.clone()).collect(), fresh()));
                    index.insert(key.clone(), groups.len() - 1);
                    groups.len() - 1
                }
            }
        };
        for (i, p) in plan.iter().enumerate() {
            if let Item::Agg(_, arg) = p {
                match arg {
                    None => groups[at].1[i].fold(&Value::Null), // COUNT(*)
                    Some(a) => {
                        let v = eval_scalar(a, &full, row, params)?;
                        groups[at].1[i].fold(&v);
                    }
                }
            }
        }
        Ok(())
    };
    // Narrowed by an index where one applies, else the whole table — either
    // way the rows are folded as they arrive and never collected.
    let served = match &index_eqs {
        Some(eqs) => store.index_visit_eq(&from.name, eqs, &mut |cells| {
            fold_row(cells)?;
            Ok(true)
        })?,
        None => None,
    };
    if served.is_none() {
        store.scan_visit(&from.name, &mut |row| {
            fold_row(row)?;
            Ok(true)
        })?;
    }

    // Emit.
    let columns: Vec<String> = proj.iter().map(|(n, _)| n.clone()).collect();
    let types: Vec<Option<SqlType>> = proj.iter().map(|(_, e)| expr_type(e, &full)).collect();
    let mut rows = Vec::with_capacity(groups.len());
    for (key, accs) in groups {
        let out: Vec<Value> = accs
            .into_iter()
            .zip(&plan)
            .map(|(acc, p)| match p {
                Item::Agg(..) => acc.finish(),
                Item::Key(i) => key[*i].clone(),
            })
            .collect();
        if let Some(h) = &having {
            // HAVING sees the finished group: its columns are the projection.
            let names: Vec<ColRef> = columns
                .iter()
                .zip(&types)
                .map(|(c, t)| ColRef {
                    table: String::new(),
                    name: c.clone(),
                    ty: *t,
                })
                .collect();
            let bound = match bind_expr(h, &names) {
                Ok(b) => b,
                Err(_) => return Ok(None), // HAVING over something not projected
            };
            if !truthy(&eval_scalar(&bound, &names, out.as_slice(), params)?) {
                continue;
            }
        }
        rows.push(out);
    }
    Ok(Some(QueryResult::Select {
        columns,
        types,
        rows,
    }))
}

/// Whether an expression may be evaluated inside a streamed scan: no
/// correlation and no windows (both would re-enter machinery a borrowed row
/// can't serve — correlated subqueries would deadlock on the engine's table
/// lock), every param slot user-bound (a higher slot is a synthetic
/// correlation slot), and no volatile functions (a streamed predicate is
/// re-evaluated by the normal pipeline, and `random()` must not be sampled
/// twice per row).
fn streamable(e: &Expr, params: &[Value]) -> bool {
    !has_corr(e) && !has_window(e) && max_param_slot(e) <= params.len() && !has_volatile(e)
}

/// Whether an expression contains a function whose value can differ between
/// two evaluations on the same row (`random()`, `NOW()`).
fn has_volatile(e: &Expr) -> bool {
    match e {
        Expr::Func { func, args } => {
            matches!(func, ScalarFunc::Random | ScalarFunc::Now) || args.iter().any(has_volatile)
        }
        Expr::Binary { left, right, .. } => has_volatile(left) || has_volatile(right),
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } => has_volatile(expr),
        Expr::Aggregate { arg, .. } => arg.as_deref().is_some_and(has_volatile),
        Expr::In { expr, list, .. } => has_volatile(expr) || list.iter().any(has_volatile),
        Expr::InSet { expr, .. } => has_volatile(expr),
        Expr::Subquery(_)
        | Expr::InSubquery { .. }
        | Expr::CorrScalar { .. }
        | Expr::CorrIn { .. } => {
            true // subqueries are opaque here; the caller must not stream them
        }
        Expr::Window {
            func,
            partition_by,
            order_by,
        } => {
            partition_by.iter().any(has_volatile)
                || order_by.iter().any(|(e, _)| has_volatile(e))
                || matches!(func, WindowFunc::Agg(_, Some(a)) if has_volatile(a))
        }
        Expr::Column { .. } | Expr::Col(_) | Expr::Literal(_) | Expr::Param(_) => false,
    }
}

/// Whether the SELECT's output rows are its input rows one-for-one (no
/// grouping, aggregates, windows, or DISTINCT) — the precondition for
/// pushing LIMIT-derived cutoffs into the scan.
fn select_is_plain_rows(select: &SelectStmt) -> bool {
    select.group_by.is_empty()
        && select.having.is_none()
        && !select.distinct
        && select.distinct_on.is_empty()
        && !select.projection.iter().any(|item| match item {
            SelectItem::Expr { expr, .. } => has_aggregate(expr) || has_window(expr),
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => false,
        })
        && !select
            .order_by
            .iter()
            .any(|(e, _)| has_aggregate(e) || has_window(e))
}

/// Ordered cutoffs larger than this fall back to a full scan + sort: the
/// bounded buffer pays an O(cutoff) insert per surviving row, which loses to
/// one big sort well before this size.
const STREAM_TOP_N_CAP: usize = 1024;

/// A streamed single-table scan: evaluate the WHERE on rows **borrowed from
/// the store** and clone only what qualifies — plus, when the query is a
/// plain row stream with a LIMIT, stop early (unordered) or keep only the
/// top `OFFSET + LIMIT` rows under the ORDER BY (a bounded buffer, exactly
/// like `select_simple`'s top-N but before any materialization).
///
/// Returns a chunk the ordinary pipeline treats like any scan: the WHERE is
/// re-applied (cheap — only matching rows remain), the stable sort re-orders
/// the top-N rows (they are emitted in scan order), and OFFSET/LIMIT slice
/// as usual. `None` means nothing could be pushed down; the caller does a
/// plain pruned scan.
fn streamed_chunk<S: Store>(
    store: &S,
    from: &TableRef,
    select: &SelectStmt,
    params: &[Value],
    keep: &[usize],
    full: &[ColRef],
) -> Result<Option<Chunk>> {
    let filter = match &select.filter {
        Some(f) if streamable(f, params) => match bind_expr(f, full) {
            Ok(b) => Some(b),
            // An unbindable column will error identically in the normal
            // pipeline; let it surface there.
            Err(_) => return Ok(None),
        },
        // Nothing may be pushed below a WHERE we can't evaluate in-scan.
        Some(_) => return Ok(None),
        None => None,
    };

    let plain = select_is_plain_rows(select);
    let cap = if plain && let Some(l) = resolve_limit(select.limit, params, "LIMIT")? {
        Some(l.saturating_add(resolve_limit(select.offset, params, "OFFSET")?.unwrap_or(0)))
    } else {
        None
    };

    // Bound ORDER BY keys for an in-scan top-N cutoff.
    let ordered = !select.order_by.is_empty();
    let keys: Option<Vec<(Expr, bool)>> = if ordered
        && cap.is_some_and(|c| c <= STREAM_TOP_N_CAP)
        && select.order_by.iter().all(|(e, _)| streamable(e, params))
    {
        select
            .order_by
            .iter()
            .map(|(e, asc)| bind_expr(e, full).map(|b| (b, *asc)))
            .collect::<Result<Vec<_>>>()
            .ok() // e.g. ORDER BY <projection alias>: resolved later, not here
    } else {
        None
    };
    let early_stop = if ordered { None } else { cap };

    if filter.is_none() && keys.is_none() && early_stop.is_none() {
        return Ok(None); // a plain pruned scan is already optimal
    }

    let width = keep.len();
    if let Some(keys) = keys {
        let k = cap.expect("keys imply a resolved cutoff");
        // Sorted ascending by (sort keys, scan sequence); the last element is
        // the cutoff. Rows are cloned only when they enter the buffer.
        let mut best: Vec<(Vec<Value>, usize, Vec<Value>)> = Vec::with_capacity(k.min(64) + 1);
        let mut kbuf: Vec<Value> = Vec::with_capacity(keys.len());
        let mut seq = 0usize;
        store.scan_visit(&from.name, &mut |row| {
            if let Some(f) = &filter
                && !truthy(&eval_scalar(f, full, row, params)?)
            {
                return Ok(true);
            }
            let i = seq;
            seq += 1;
            kbuf.clear();
            for (e, _) in &keys {
                kbuf.push(eval_scalar(e, full, row, params)?);
            }
            if best.len() >= k {
                match best.last() {
                    Some(last) if cmp_keys(&keys, &kbuf, &last.0) == Ordering::Less => {}
                    _ => return Ok(true), // not better than the cutoff (or k == 0)
                }
            }
            let pos = best.partition_point(|(bk, bi, _)| match cmp_keys(&keys, bk, &kbuf) {
                Ordering::Less => true,
                Ordering::Equal => *bi < i,
                Ordering::Greater => false,
            });
            let kept: Vec<Value> = keep.iter().map(|&c| row[c].clone()).collect();
            best.insert(pos, (std::mem::take(&mut kbuf), i, kept));
            if best.len() > k {
                kbuf = best.pop().expect("longer than k").0; // recycle the buffer
            }
            Ok(true)
        })?;
        // Emit survivors in scan order: the pipeline's stable sort then
        // reproduces exactly what a full scan would have produced.
        best.sort_by_key(|&(_, i, _)| i);
        let n = best.len();
        let mut cells = Vec::with_capacity(n * width);
        for (_, _, kept) in best {
            cells.extend(kept);
        }
        Ok(Some(Chunk { width, n, cells }))
    } else {
        Ok(Some(filtered_scan(
            store,
            &from.name,
            filter.as_ref(),
            full,
            keep,
            params,
            early_stop,
        )?))
    }
}

/// Scan `table` keeping only rows that pass `filter` (already bound against
/// the table's full schema), cloning just the `keep` columns. Rows are
/// evaluated borrowed via [`Store::scan_visit`]; `stop_after` ends the scan
/// once that many rows matched.
fn filtered_scan<S: Store>(
    store: &S,
    table: &str,
    filter: Option<&Expr>,
    full: &[ColRef],
    keep: &[usize],
    params: &[Value],
    stop_after: Option<usize>,
) -> Result<Chunk> {
    let mut cells = Vec::new();
    let mut n = 0usize;
    store.scan_visit(table, &mut |row| {
        if let Some(f) = filter
            && !truthy(&eval_scalar(f, full, row, params)?)
        {
            return Ok(true);
        }
        for &c in keep {
            cells.push(row[c].clone());
        }
        n += 1;
        Ok(stop_after.is_none_or(|c| n < c))
    })?;
    Ok(Chunk {
        width: keep.len(),
        n,
        cells,
    })
}

/// The AND-conjuncts of the WHERE that can be pushed into one table's scan:
/// each must be streamable (see [`streamable`]) and reference only columns
/// that bind against that table's schema alone. Returns the conjunction,
/// bound. The caller must re-apply the full WHERE after joining (pushdown is
/// a superset-safe row reduction, never a replacement) and must not push
/// into any table that an outer join can NULL-pad — a pushed predicate
/// would manufacture padded rows the real WHERE (e.g. `x IS NULL`) then
/// treats differently.
fn pushdown_filter(filter: Option<&Expr>, full: &[ColRef], params: &[Value]) -> Option<Expr> {
    let mut conjuncts = Vec::new();
    collect_conjuncts(filter?, &mut conjuncts);
    conjuncts
        .into_iter()
        .filter(|c| streamable(c, params))
        .filter_map(|c| bind_expr(c, full).ok())
        .reduce(|l, r| Expr::Binary {
            op: BinOp::And,
            left: Box::new(l),
            right: Box::new(r),
        })
}

fn collect_conjuncts<'e>(e: &'e Expr, out: &mut Vec<&'e Expr>) {
    match e {
        Expr::Binary {
            op: BinOp::And,
            left,
            right,
        } => {
            collect_conjuncts(left, out);
            collect_conjuncts(right, out);
        }
        _ => out.push(e),
    }
}

/// Collect `column = constant` conjuncts (split on AND) usable for an index seek.
fn eq_conjuncts(expr: &Expr, table_key: &str, params: &[Value]) -> Vec<(String, Value)> {
    let mut out = Vec::new();
    collect_eq(expr, table_key, params, &mut out);
    out
}

fn collect_eq(expr: &Expr, key: &str, params: &[Value], out: &mut Vec<(String, Value)>) {
    match expr {
        Expr::Binary {
            op: BinOp::And,
            left,
            right,
        } => {
            collect_eq(left, key, params, out);
            collect_eq(right, key, params, out);
        }
        Expr::Binary {
            op: BinOp::Eq,
            left,
            right,
        } => {
            if let Some(pair) =
                eq_pair(left, right, key, params).or_else(|| eq_pair(right, left, key, params))
            {
                out.push(pair);
            }
        }
        _ => {}
    }
}

fn eq_pair(a: &Expr, b: &Expr, key: &str, params: &[Value]) -> Option<(String, Value)> {
    let name = match a {
        Expr::Column { table, name } if table.as_deref().map(|t| t == key).unwrap_or(true) => {
            name.clone()
        }
        _ => return None,
    };
    Some((name, const_value(b, params)?))
}

fn const_value(e: &Expr, params: &[Value]) -> Option<Value> {
    match e {
        Expr::Literal(v) => Some(v.clone()),
        Expr::Param(i) => params.get(*i).cloned(),
        // A negative number literal parses as Neg(Literal): `WHERE id = -1`
        // must still be probe-able.
        Expr::Unary {
            op: UnOp::Neg,
            expr,
        } => match const_value(expr, params)? {
            Value::Int(n) => Some(Value::Int(-n)),
            Value::Double(f) => Some(Value::Double(-f)),
            _ => None,
        },
        _ => None,
    }
}

/// Non-aggregated projection: one output row per input tuple.
fn select_simple<S: Store>(
    store: &S,
    schema: &[ColRef],
    src: &Sources,
    tuples: &Tuples,
    select: &SelectStmt,
    proj: &[(String, Expr)],
    params: &[Value],
) -> Result<Vec<Vec<Value>>> {
    let n = tuples.n();

    // Extract window functions: each distinct Window node becomes a synthetic
    // Param slot whose per-row values are computed up front.
    let mut proj: Vec<(String, Expr)> = proj.to_vec();
    let mut order_by: Vec<(Expr, bool)> = select.order_by.clone();
    let win_base = proj
        .iter()
        .map(|(_, e)| max_param_slot(e))
        .chain(order_by.iter().map(|(e, _)| max_param_slot(e)))
        .fold(params.len(), usize::max);
    let mut windows: Vec<Expr> = Vec::new();
    for (_, e) in proj.iter_mut() {
        replace_windows(e, &mut windows, win_base);
    }
    for (e, _) in order_by.iter_mut() {
        replace_windows(e, &mut windows, win_base);
    }
    // Correlated subqueries inside a window's PARTITION BY / ORDER BY / agg
    // argument are resolved per row: group_tuples and compute_window both go
    // through eval_scalar_corr, so the outer row is in scope (this is how EF
    // renders argmax-per-group, `GroupBy(...).Select(g => g.OrderBy(...).First())`,
    // as ROW_NUMBER() OVER(ORDER BY (correlated subquery))).
    let win_vals: Vec<Vec<Value>> = windows
        .iter()
        .map(|w| compute_window(store, schema, src, tuples, w, params))
        .collect::<Result<_>>()?;

    // Per-row parameter list: the user params (padded), then window values.
    let mut pbuf: Vec<Value> = Vec::new();
    let row_params = |i: usize, pbuf: &mut Vec<Value>| {
        if win_vals.is_empty() {
            return;
        }
        pbuf.clear();
        pbuf.extend_from_slice(params);
        pbuf.resize(win_base, Value::Null);
        for vals in &win_vals {
            pbuf.push(vals[i].clone());
        }
    };

    let proj_corr = proj.iter().any(|(_, e)| has_corr(e));
    // DISTINCT ON (exprs): keep the first row per key group after ordering.
    let don = &select.distinct_on;
    let don_corr = don.iter().any(has_corr);
    let dkey = |i: usize, p: &[Value]| -> Result<Vec<IndexKey>> {
        let view = View {
            src,
            tuple: tuples.row(i),
        };
        don.iter()
            .map(|e| eval_scalar_corr(store, don_corr, e, schema, &view, p).map(IndexKey))
            .collect()
    };

    if order_by.is_empty() {
        let mut out = Vec::with_capacity(n);
        let mut seen: std::collections::BTreeSet<Vec<IndexKey>> = std::collections::BTreeSet::new();
        for i in 0..n {
            row_params(i, &mut pbuf);
            let p: &[Value] = if win_vals.is_empty() { params } else { &pbuf };
            if !don.is_empty() && !seen.insert(dkey(i, p)?) {
                continue;
            }
            let view = View {
                src,
                tuple: tuples.row(i),
            };
            let row: Vec<Value> = proj
                .iter()
                .map(|(_, e)| eval_scalar_corr(store, proj_corr, e, schema, &view, p))
                .collect::<Result<_>>()?;
            out.push(row);
        }
        return Ok(out);
    }

    // Evaluate the sort keys once per row (not per comparison), then sort.
    let keys_corr = order_by.iter().any(|(e, _)| has_corr(e));

    // ORDER BY + LIMIT without DISTINCT / DISTINCT ON: top-N. Keep a bounded
    // buffer of the best `offset + limit` (sort keys, tuple index) pairs and
    // project only the survivors — a 20-row LIMIT over 50k rows must not
    // sort (or project) 50k rows. Ties keep the earlier tuple, matching the
    // stable full sort below; the caller still applies OFFSET/LIMIT.
    let top = if don.is_empty()
        && !select.distinct
        && let Some(l) = resolve_limit(select.limit, params, "LIMIT")?
    {
        Some(l.saturating_add(resolve_limit(select.offset, params, "OFFSET")?.unwrap_or(0)))
    } else {
        None
    };
    if let Some(k) = top {
        // Sorted ascending by (keys, tuple index); the last element is the
        // current cutoff. Keys are evaluated into a reused buffer and only
        // cloned for rows that actually enter the buffer — on large scans
        // almost every row fails the cutoff test.
        let mut best: Vec<(Vec<Value>, usize)> = Vec::with_capacity(k.min(n) + 1);
        let mut kbuf: Vec<Value> = Vec::with_capacity(order_by.len());
        for i in 0..n {
            row_params(i, &mut pbuf);
            let p: &[Value] = if win_vals.is_empty() { params } else { &pbuf };
            let view = View {
                src,
                tuple: tuples.row(i),
            };
            kbuf.clear();
            for (e, _) in &order_by {
                kbuf.push(eval_scalar_corr(store, keys_corr, e, schema, &view, p)?);
            }
            if best.len() >= k {
                match best.last() {
                    Some(last) if cmp_keys(&order_by, &kbuf, &last.0) == Ordering::Less => {}
                    _ => continue, // not better than the cutoff (or k == 0)
                }
            }
            let pos = best.partition_point(|(bk, bi)| match cmp_keys(&order_by, bk, &kbuf) {
                Ordering::Less => true,
                Ordering::Equal => *bi < i,
                Ordering::Greater => false,
            });
            best.insert(pos, (std::mem::take(&mut kbuf), i));
            if best.len() > k {
                best.pop();
            }
        }
        let mut out = Vec::with_capacity(best.len());
        for (_, i) in best {
            row_params(i, &mut pbuf);
            let p: &[Value] = if win_vals.is_empty() { params } else { &pbuf };
            let view = View {
                src,
                tuple: tuples.row(i),
            };
            let row: Vec<Value> = proj
                .iter()
                .map(|(_, e)| eval_scalar_corr(store, proj_corr, e, schema, &view, p))
                .collect::<Result<_>>()?;
            out.push(row);
        }
        return Ok(out);
    }

    let mut keyed: Vec<(Vec<Value>, Vec<IndexKey>, Vec<Value>)> = Vec::with_capacity(n);
    for i in 0..n {
        row_params(i, &mut pbuf);
        let p: &[Value] = if win_vals.is_empty() { params } else { &pbuf };
        let dk = if don.is_empty() {
            Vec::new()
        } else {
            dkey(i, p)?
        };
        let view = View {
            src,
            tuple: tuples.row(i),
        };
        let keys: Vec<Value> = order_by
            .iter()
            .map(|(e, _)| eval_scalar_corr(store, keys_corr, e, schema, &view, p))
            .collect::<Result<_>>()?;
        let row: Vec<Value> = proj
            .iter()
            .map(|(_, e)| eval_scalar_corr(store, proj_corr, e, schema, &view, p))
            .collect::<Result<_>>()?;
        keyed.push((keys, dk, row));
    }
    keyed.sort_by(|a, b| cmp_keys(&order_by, &a.0, &b.0));
    if don.is_empty() {
        Ok(keyed.into_iter().map(|(_, _, row)| row).collect())
    } else {
        let mut seen: std::collections::BTreeSet<Vec<IndexKey>> = std::collections::BTreeSet::new();
        Ok(keyed
            .into_iter()
            .filter(|(_, dk, _)| seen.insert(dk.clone()))
            .map(|(_, _, row)| row)
            .collect())
    }
}

/// Aggregated projection: group tuples, compute aggregates per group.
/// Collect the aggregates that can be computed in a single group pass: plain
/// COUNT/SUM/AVG/MIN/MAX with no DISTINCT and no correlated argument (mode and
/// distinct/correlated aggregates keep the per-aggregate path). Deduped by
/// structural equality so `COUNT(*)` written twice folds one accumulator.
fn collect_streamable_aggs(e: &Expr, out: &mut Vec<Expr>) {
    match e {
        Expr::Aggregate {
            func,
            arg,
            distinct,
        } if !*distinct
            && !matches!(func, AggFunc::Mode)
            && arg.as_deref().is_none_or(|a| !has_corr(a)) =>
        {
            if !out.contains(e) {
                out.push(e.clone());
            }
        }
        Expr::Binary { left, right, .. } => {
            collect_streamable_aggs(left, out);
            collect_streamable_aggs(right, out);
        }
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } => {
            collect_streamable_aggs(expr, out);
        }
        Expr::Func { args, .. } => args.iter().for_each(|a| collect_streamable_aggs(a, out)),
        Expr::In { expr, list, .. } => {
            collect_streamable_aggs(expr, out);
            list.iter().for_each(|i| collect_streamable_aggs(i, out));
        }
        Expr::InSet { expr, .. } => collect_streamable_aggs(expr, out),
        _ => {}
    }
}

/// One accumulator per streamable aggregate.
enum AggAcc {
    CountStar(i64),
    Count(i64),
    Sum(SumAcc),
    Avg(SumAcc, i64),
    MinMax(Option<Value>, Ordering),
}

/// Compute all `aggs` for one group in a single pass over its rows. Values
/// line up with `aggs` positionally.
fn compute_streamable_aggs<S: Store>(
    _store: &S,
    aggs: &[Expr],
    schema: &[ColRef],
    src: &Sources,
    tuples: &Tuples,
    group: &[u32],
    params: &[Value],
) -> Result<Vec<Value>> {
    let mut accs: Vec<AggAcc> = aggs
        .iter()
        .map(|a| match a {
            Expr::Aggregate { func, arg, .. } => match func {
                AggFunc::Count if arg.is_none() => AggAcc::CountStar(0),
                AggFunc::Count => AggAcc::Count(0),
                AggFunc::Sum => AggAcc::Sum(SumAcc::Empty),
                AggFunc::Avg => AggAcc::Avg(SumAcc::Empty, 0),
                AggFunc::Min => AggAcc::MinMax(None, Ordering::Less),
                AggFunc::Max => AggAcc::MinMax(None, Ordering::Greater),
                AggFunc::Mode => unreachable!("mode is not streamable"),
            },
            _ => unreachable!("non-aggregate collected as streamable"),
        })
        .collect();
    for &ri in group {
        let view = View {
            src,
            tuple: tuples.row(ri as usize),
        };
        for (acc, agg) in accs.iter_mut().zip(aggs) {
            let Expr::Aggregate { arg, .. } = agg else {
                unreachable!()
            };
            if let AggAcc::CountStar(n) = acc {
                *n += 1;
                continue;
            }
            let v = eval_scalar(
                arg.as_deref().expect("non-* has arg"),
                schema,
                &view,
                params,
            )?;
            if matches!(v, Value::Null) {
                continue;
            }
            match acc {
                AggAcc::CountStar(_) => unreachable!(),
                AggAcc::Count(n) => *n += 1,
                AggAcc::Sum(s) => s.add(&v)?,
                AggAcc::Avg(s, n) => {
                    s.add(&v)?;
                    *n += 1;
                }
                AggAcc::MinMax(best, want) => match best {
                    None => *best = Some(v),
                    Some(cur) if Value::total_order(&v, cur) == *want => *best = Some(v),
                    _ => {}
                },
            }
        }
    }
    Ok(accs
        .into_iter()
        .map(|acc| match acc {
            AggAcc::CountStar(n) | AggAcc::Count(n) => Value::Int(n),
            AggAcc::Sum(s) => s.into_sum(),
            AggAcc::Avg(s, n) => {
                if n == 0 {
                    Value::Null
                } else {
                    s.into_avg(n)
                }
            }
            AggAcc::MinMax(best, _) => best.unwrap_or(Value::Null),
        })
        .collect())
}

/// Replace each streamable aggregate node in `e` with its precomputed value.
fn substitute_aggs(e: &mut Expr, precomp: &[(Expr, Value)]) {
    if matches!(e, Expr::Aggregate { .. })
        && let Some((_, v)) = precomp.iter().find(|(a, _)| a == e)
    {
        *e = Expr::Literal(v.clone());
        return;
    }
    match e {
        Expr::Binary { left, right, .. } => {
            substitute_aggs(left, precomp);
            substitute_aggs(right, precomp);
        }
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } => substitute_aggs(expr, precomp),
        Expr::Func { args, .. } => args.iter_mut().for_each(|a| substitute_aggs(a, precomp)),
        Expr::In { expr, list, .. } => {
            substitute_aggs(expr, precomp);
            list.iter_mut().for_each(|i| substitute_aggs(i, precomp));
        }
        Expr::InSet { expr, .. } => substitute_aggs(expr, precomp),
        _ => {}
    }
}

fn select_aggregated<S: Store>(
    store: &S,
    schema: &[ColRef],
    src: &Sources,
    tuples: &Tuples,
    select: &SelectStmt,
    proj: &[(String, Expr)],
    params: &[Value],
) -> Result<Vec<Vec<Value>>> {
    // Group by the group-by key (empty group-by => single group over all rows).
    let groups = group_tuples(store, schema, src, tuples, &select.group_by, params)?;

    // DISTINCT ON over aggregated output: the ON expressions are evaluated per
    // group (like the projection). This is the argmax idiom — GROUP BY (a, b),
    // ORDER BY a, agg DESC, DISTINCT ON (a) keeps the top b per a.
    let don = &select.distinct_on;
    // Reuse a projection's value for an ORDER BY key that is the identical
    // expression: EF Core emits the very same (often heavy, correlated)
    // subquery in both SELECT and ORDER BY, and computing it twice per group
    // dominates some analytic queries.
    let key_from_proj: Vec<Option<usize>> = select
        .order_by
        .iter()
        .map(|(e, _)| proj.iter().position(|(_, pe)| pe == e))
        .collect();

    // Streamable aggregates across the whole projection/having/order-by: these
    // are computed in ONE pass per group (not one pass per aggregate), then
    // substituted as literals before evaluating the surrounding expressions.
    let mut streamable: Vec<Expr> = Vec::new();
    for (_, e) in proj {
        collect_streamable_aggs(e, &mut streamable);
    }
    if let Some(h) = &select.having {
        collect_streamable_aggs(h, &mut streamable);
    }
    for (e, _) in &select.order_by {
        collect_streamable_aggs(e, &mut streamable);
    }

    let mut prepared: Vec<(Vec<Value>, Vec<IndexKey>, Vec<Value>)> =
        Vec::with_capacity(groups.len());
    for group in &groups {
        // One-pass values for the streamable aggregates of this group.
        let precomp: Vec<(Expr, Value)> = if streamable.is_empty() {
            Vec::new()
        } else {
            let vals =
                compute_streamable_aggs(store, &streamable, schema, src, tuples, group, params)?;
            streamable.iter().cloned().zip(vals).collect()
        };
        let eval_grp = |e: &Expr| -> Result<Value> {
            if precomp.is_empty() {
                eval_agg(store, e, schema, src, tuples, group, params)
            } else {
                let mut e2 = e.clone();
                substitute_aggs(&mut e2, &precomp);
                eval_agg(store, &e2, schema, src, tuples, group, params)
            }
        };

        if let Some(having) = &select.having
            && !truthy(&eval_grp(having)?)
        {
            continue;
        }
        let out: Vec<Value> = proj
            .iter()
            .map(|(_, e)| eval_grp(e))
            .collect::<Result<_>>()?;
        // Evaluate the sort keys once per group (not per comparison); a key
        // that duplicates a projection reuses that already-computed value.
        let keys: Vec<Value> = select
            .order_by
            .iter()
            .enumerate()
            .map(|(k, (e, _))| match key_from_proj[k] {
                Some(j) => Ok(out[j].clone()),
                None => eval_grp(e),
            })
            .collect::<Result<_>>()?;
        let dk: Vec<IndexKey> = don
            .iter()
            .map(|e| eval_grp(e).map(IndexKey))
            .collect::<Result<_>>()?;
        prepared.push((keys, dk, out));
    }

    if !select.order_by.is_empty() {
        prepared.sort_by(|a, b| cmp_keys(&select.order_by, &a.0, &b.0));
    }
    if don.is_empty() {
        Ok(prepared.into_iter().map(|(_, _, out)| out).collect())
    } else {
        let mut seen: std::collections::BTreeSet<Vec<IndexKey>> = std::collections::BTreeSet::new();
        Ok(prepared
            .into_iter()
            .filter(|(_, dk, _)| seen.insert(dk.clone()))
            .map(|(_, _, out)| out)
            .collect())
    }
}

/// Group tuple indices by the evaluated group-by key, preserving first-seen
/// group order. Streams over the tuples once; group keys are only cloned when
/// a new group is created.
fn group_tuples<S: Store>(
    store: &S,
    schema: &[ColRef],
    src: &Sources,
    tuples: &Tuples,
    group_by: &[Expr],
    params: &[Value],
) -> Result<Vec<Vec<u32>>> {
    let n = tuples.n();
    if group_by.is_empty() {
        // One group over everything (present even when there are no rows).
        return Ok(vec![(0..n as u32).collect()]);
    }

    // hash(key) -> group ids with that hash; collisions resolved by comparing
    // the evaluated key against the group's stored key values.
    let mut by_hash: FxMap<u64, Vec<usize>> = FxMap::default();
    let mut groups: Vec<Vec<u32>> = Vec::new();
    let mut group_keys: Vec<Vec<Value>> = Vec::new();
    let mut scratch: Vec<Cow<'_, Value>> = Vec::with_capacity(group_by.len());

    for i in 0..n {
        let view = View {
            src,
            tuple: tuples.row(i),
        };
        scratch.clear();
        for e in group_by {
            // Bare-column keys (the common case) borrow the cell; anything
            // else evaluates to an owned value.
            match e {
                Expr::Col(p) => scratch
                    .push(Cow::Borrowed(view.val_ref(*p).ok_or_else(|| {
                        SqlError::Eval(format!("bound column {p} out of range"))
                    })?)),
                _ => scratch.push(Cow::Owned(eval_scalar_corr(
                    store,
                    has_corr(e),
                    e,
                    schema,
                    &view,
                    params,
                )?)),
            }
        }

        let mut hasher = FxHasher::default();
        for c in scratch.iter() {
            hash_value_norm(c, &mut hasher);
        }
        let h = hasher.finish();

        let ids = by_hash.entry(h).or_default();
        let found = ids.iter().copied().find(|&g| {
            group_keys[g]
                .iter()
                .zip(scratch.iter())
                .all(|(a, b)| Value::total_order(a, b) == Ordering::Equal)
        });
        match found {
            Some(g) => groups[g].push(i as u32),
            None => {
                let g = groups.len();
                ids.push(g);
                groups.push(vec![i as u32]);
                group_keys.push(scratch.iter().map(|c| c.as_ref().clone()).collect());
            }
        }
    }
    Ok(groups)
}

/// Hash a value consistently with [`Value::total_order`] equality: the numeric
/// kinds share one space, `0.0`/`-0.0` collapse, and NaNs collapse (NaN is
/// *equal* under the total order used for grouping).
fn hash_value_norm(v: &Value, h: &mut impl Hasher) {
    match v {
        Value::Null => h.write_u8(0),
        Value::Bool(b) => {
            h.write_u8(1);
            h.write_u8(*b as u8);
        }
        Value::Int(_) | Value::Double(_) | Value::Timestamp(_) | Value::Decimal(_) => {
            let f = as_f64(v).expect("numeric");
            let bits = if f.is_nan() {
                f64::NAN.to_bits()
            } else if f == 0.0 {
                0f64.to_bits()
            } else {
                f.to_bits()
            };
            h.write_u8(2);
            h.write_u64(bits);
        }
        Value::Text(s) => {
            h.write_u8(3);
            h.write(s.as_bytes());
        }
        Value::Bytes(b) => {
            h.write_u8(4);
            h.write(b);
        }
    }
}

// ── projection helpers ──────────────────────────────────────────────────────

fn expand_projection(items: &[SelectItem], schema: &[ColRef]) -> Result<Vec<(String, Expr)>> {
    let mut out = Vec::new();
    for item in items {
        match item {
            SelectItem::Wildcard => {
                for c in schema {
                    out.push((
                        c.name.clone(),
                        Expr::Column {
                            table: Some(c.table.clone()),
                            name: c.name.clone(),
                        },
                    ));
                }
            }
            SelectItem::QualifiedWildcard(t) => {
                let mut any = false;
                for c in schema.iter().filter(|c| &c.table == t) {
                    any = true;
                    out.push((
                        c.name.clone(),
                        Expr::Column {
                            table: Some(c.table.clone()),
                            name: c.name.clone(),
                        },
                    ));
                }
                if !any {
                    return Err(SqlError::NoSuchTable(t.clone()));
                }
            }
            SelectItem::Expr { expr, alias } => {
                let name = alias.clone().unwrap_or_else(|| default_name(expr));
                out.push((name, expr.clone()));
            }
        }
    }
    Ok(out)
}

/// Best-effort static type of a projection expression (`None` = unknown).
/// Used only for result metadata — never for evaluation.
fn expr_type(e: &Expr, schema: &[ColRef]) -> Option<SqlType> {
    match e {
        Expr::Column { table, name } => {
            let i = resolve_col(schema, table, name).ok()?;
            schema.get(i)?.ty
        }
        Expr::Col(i) => schema.get(*i)?.ty,
        Expr::Literal(v) => match v {
            Value::Int(_) => Some(SqlType::Int),
            Value::Double(_) => Some(SqlType::Double),
            Value::Text(_) => Some(SqlType::Text),
            Value::Bool(_) => Some(SqlType::Bool),
            Value::Timestamp(_) => Some(SqlType::Timestamp),
            Value::Bytes(_) => Some(SqlType::Blob),
            Value::Decimal(_) => Some(SqlType::Decimal),
            Value::Null => None,
        },
        Expr::Binary { op, left, right } => match op {
            BinOp::Eq
            | BinOp::Ne
            | BinOp::Lt
            | BinOp::Le
            | BinOp::Gt
            | BinOp::Ge
            | BinOp::And
            | BinOp::Or => Some(SqlType::Bool),
            BinOp::Concat => Some(SqlType::Text),
            BinOp::BitXor | BinOp::BitAnd | BinOp::BitOr => match expr_type(left, schema) {
                Some(SqlType::Bool) => Some(SqlType::Bool),
                _ => Some(SqlType::Int),
            },
            BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod => {
                match (expr_type(left, schema), expr_type(right, schema)) {
                    // `text + text` concatenates (see `arithmetic`).
                    (Some(SqlType::Text), Some(SqlType::Text)) if matches!(op, BinOp::Add) => {
                        Some(SqlType::Text)
                    }
                    // ts ± ms → ts; ts - ts → ms (see `arithmetic`).
                    (Some(SqlType::Timestamp), Some(SqlType::Timestamp))
                        if matches!(op, BinOp::Sub) =>
                    {
                        Some(SqlType::Int)
                    }
                    (Some(SqlType::Timestamp), Some(SqlType::Int | SqlType::Double))
                    | (Some(SqlType::Int | SqlType::Double), Some(SqlType::Timestamp))
                        if matches!(op, BinOp::Add | BinOp::Sub) =>
                    {
                        Some(SqlType::Timestamp)
                    }
                    (Some(SqlType::Double), _) | (_, Some(SqlType::Double)) => {
                        Some(SqlType::Double)
                    }
                    (Some(SqlType::Int), Some(SqlType::Int)) => Some(SqlType::Int),
                    _ => None,
                }
            }
        },
        Expr::Unary { op, expr } => match op {
            UnOp::Not => Some(SqlType::Bool),
            UnOp::Neg | UnOp::BitNot => expr_type(expr, schema),
        },
        Expr::IsNull { .. }
        | Expr::In { .. }
        | Expr::InSet { .. }
        | Expr::InSubquery { .. }
        | Expr::CorrIn { .. } => Some(SqlType::Bool),
        Expr::Aggregate { func, arg, .. } => match func {
            AggFunc::Count => Some(SqlType::Int),
            AggFunc::Avg => Some(SqlType::Double),
            AggFunc::Sum | AggFunc::Min | AggFunc::Max | AggFunc::Mode => {
                arg.as_deref().and_then(|a| expr_type(a, schema))
            }
        },
        Expr::Window { func, .. } => match func {
            WindowFunc::RowNumber | WindowFunc::Rank | WindowFunc::DenseRank => Some(SqlType::Int),
            WindowFunc::Agg(AggFunc::Count, _) => Some(SqlType::Int),
            WindowFunc::Agg(AggFunc::Avg, _) => Some(SqlType::Double),
            WindowFunc::Agg(_, arg) => arg.as_deref().and_then(|a| expr_type(a, schema)),
        },
        Expr::Func { func, args } => match func {
            ScalarFunc::Upper
            | ScalarFunc::Lower
            | ScalarFunc::Substring
            | ScalarFunc::Concat
            | ScalarFunc::Trim
            | ScalarFunc::Ltrim
            | ScalarFunc::Rtrim
            | ScalarFunc::Replace => Some(SqlType::Text),
            ScalarFunc::Length => Some(SqlType::Int),
            ScalarFunc::Like { .. } => Some(SqlType::Bool),
            ScalarFunc::StartsWith | ScalarFunc::EndsWith => Some(SqlType::Bool),
            ScalarFunc::Cast(t) => Some(*t),
            ScalarFunc::Abs | ScalarFunc::Round | ScalarFunc::NullIf => {
                args.first().and_then(|a| expr_type(a, schema))
            }
            ScalarFunc::Coalesce | ScalarFunc::Least | ScalarFunc::Greatest => {
                args.iter().find_map(|a| expr_type(a, schema))
            }
            ScalarFunc::Case { has_else } => {
                // Value slots are the odd positions (+ trailing ELSE).
                let mut it: Vec<&Expr> = args.iter().skip(1).step_by(2).collect();
                if *has_else {
                    it.push(args.last().unwrap());
                }
                it.into_iter().find_map(|a| expr_type(a, schema))
            }
            ScalarFunc::Now | ScalarFunc::DateTrunc(_) | ScalarFunc::AddMonths => {
                Some(SqlType::Timestamp)
            }
            ScalarFunc::Extract(part) => Some(if matches!(part, DatePart::Epoch) {
                SqlType::Double
            } else {
                SqlType::Int
            }),
            ScalarFunc::Floor | ScalarFunc::Ceil | ScalarFunc::Trunc => {
                args.first().and_then(|a| expr_type(a, schema))
            }
            ScalarFunc::Power
            | ScalarFunc::Sqrt
            | ScalarFunc::Sin
            | ScalarFunc::Cos
            | ScalarFunc::Tan
            | ScalarFunc::Asin
            | ScalarFunc::Acos
            | ScalarFunc::Atan
            | ScalarFunc::Atan2
            | ScalarFunc::Exp
            | ScalarFunc::Ln
            | ScalarFunc::Log10
            | ScalarFunc::Log
            | ScalarFunc::Degrees
            | ScalarFunc::Radians => Some(SqlType::Double),
            ScalarFunc::Sign => Some(SqlType::Int),
            ScalarFunc::Random => Some(SqlType::Double),
            ScalarFunc::Collate { .. } => Some(SqlType::Text),
            ScalarFunc::RegexpLike => Some(SqlType::Bool),
            ScalarFunc::Position => Some(SqlType::Int),
            ScalarFunc::Lpad | ScalarFunc::Rpad => Some(SqlType::Text),
        },
        Expr::Param(_) | Expr::Subquery(_) | Expr::CorrScalar { .. } => None,
    }
}

fn default_name(expr: &Expr) -> String {
    fn agg_name(func: &AggFunc) -> &'static str {
        match func {
            AggFunc::Count => "count",
            AggFunc::Sum => "sum",
            AggFunc::Avg => "avg",
            AggFunc::Min => "min",
            AggFunc::Max => "max",
            AggFunc::Mode => "mode",
        }
    }
    match expr {
        Expr::Column { name, .. } => name.clone(),
        Expr::Aggregate { func, .. } => agg_name(func).to_string(),
        Expr::Func { func, .. } => match func {
            ScalarFunc::Coalesce => "coalesce".to_string(),
            ScalarFunc::Least => "least".to_string(),
            ScalarFunc::Greatest => "greatest".to_string(),
            ScalarFunc::NullIf => "nullif".to_string(),
            ScalarFunc::Upper => "upper".to_string(),
            ScalarFunc::Lower => "lower".to_string(),
            ScalarFunc::Length => "length".to_string(),
            ScalarFunc::Substring => "substring".to_string(),
            ScalarFunc::Concat => "concat".to_string(),
            ScalarFunc::Trim => "trim".to_string(),
            ScalarFunc::Ltrim => "ltrim".to_string(),
            ScalarFunc::Rtrim => "rtrim".to_string(),
            ScalarFunc::Replace => "replace".to_string(),
            ScalarFunc::Abs => "abs".to_string(),
            ScalarFunc::Round => "round".to_string(),
            ScalarFunc::Cast(_) => "cast".to_string(),
            ScalarFunc::Like { .. } => "like".to_string(),
            ScalarFunc::Case { .. } => "case".to_string(),
            ScalarFunc::Now => "now".to_string(),
            ScalarFunc::Extract(_) => "extract".to_string(),
            ScalarFunc::DateTrunc(_) => "date_trunc".to_string(),
            ScalarFunc::Floor => "floor".to_string(),
            ScalarFunc::Ceil => "ceiling".to_string(),
            ScalarFunc::Power => "power".to_string(),
            ScalarFunc::Sqrt => "sqrt".to_string(),
            ScalarFunc::StartsWith => "starts_with".to_string(),
            ScalarFunc::EndsWith => "ends_with".to_string(),
            ScalarFunc::Position => "position".to_string(),
            ScalarFunc::Lpad => "lpad".to_string(),
            ScalarFunc::Rpad => "rpad".to_string(),
            ScalarFunc::AddMonths => "add_months".to_string(),
            ScalarFunc::Sin => "sin".to_string(),
            ScalarFunc::Cos => "cos".to_string(),
            ScalarFunc::Tan => "tan".to_string(),
            ScalarFunc::Asin => "asin".to_string(),
            ScalarFunc::Acos => "acos".to_string(),
            ScalarFunc::Atan => "atan".to_string(),
            ScalarFunc::Atan2 => "atan2".to_string(),
            ScalarFunc::Exp => "exp".to_string(),
            ScalarFunc::Ln => "ln".to_string(),
            ScalarFunc::Log10 => "log10".to_string(),
            ScalarFunc::Log => "log".to_string(),
            ScalarFunc::Degrees => "degrees".to_string(),
            ScalarFunc::Radians => "radians".to_string(),
            ScalarFunc::Sign => "sign".to_string(),
            ScalarFunc::Trunc => "trunc".to_string(),
            ScalarFunc::RegexpLike => "regexp_like".to_string(),
            ScalarFunc::Random => "random".to_string(),
            ScalarFunc::Collate { .. } => "collate".to_string(),
        },
        Expr::Window { func, .. } => match func {
            WindowFunc::RowNumber => "row_number".to_string(),
            WindowFunc::Rank => "rank".to_string(),
            WindowFunc::DenseRank => "dense_rank".to_string(),
            WindowFunc::Agg(f, _) => agg_name(f).to_string(),
        },
        _ => "expr".to_string(),
    }
}

// ── ORDER BY helpers ────────────────────────────────────────────────────────

/// Compare two precomputed sort-key rows under the ORDER BY directions.
fn cmp_keys(order: &[(Expr, bool)], a: &[Value], b: &[Value]) -> Ordering {
    for (i, (_, asc)) in order.iter().enumerate() {
        let ord = Value::total_order(&a[i], &b[i]);
        let ord = if *asc { ord } else { ord.reverse() };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

// ── schema helpers ──────────────────────────────────────────────────────────

fn table_schema(table: &str, def: &crate::catalog::Table) -> Vec<ColRef> {
    qualified_schema(table, def)
}

fn qualified_schema(table_key: &str, def: &crate::catalog::Table) -> Vec<ColRef> {
    def.columns
        .iter()
        .map(|c| ColRef {
            table: table_key.to_string(),
            name: c.name.clone(),
            ty: Some(c.ty),
        })
        .collect()
}

fn resolve_col(schema: &[ColRef], table: &Option<String>, name: &str) -> Result<usize> {
    let mut found = None;
    for (i, c) in schema.iter().enumerate() {
        let table_ok = table.as_deref().map(|t| c.table == t).unwrap_or(true);
        if table_ok && c.name == name {
            if found.is_some() {
                return Err(SqlError::Eval(format!("ambiguous column {name:?}")));
            }
            found = Some(i);
        }
    }
    found.ok_or_else(|| SqlError::NoSuchColumn(name.to_string()))
}

// ── expression evaluation ───────────────────────────────────────────────────

fn truthy(v: &Value) -> bool {
    matches!(v, Value::Bool(true))
}

/// Evaluate a scalar (non-aggregate) expression over a single row (a cell
/// slice or a join-tuple view).
fn eval_scalar<R: RowLike + ?Sized>(
    expr: &Expr,
    schema: &[ColRef],
    row: &R,
    params: &[Value],
) -> Result<Value> {
    match expr {
        Expr::Col(i) => row
            .val(*i)
            .cloned()
            .ok_or_else(|| SqlError::Eval(format!("bound column {i} out of range"))),
        Expr::Literal(v) => Ok(v.clone()),
        Expr::Param(i) => params
            .get(*i)
            .cloned()
            .ok_or_else(|| SqlError::Eval(format!("missing bind parameter ${}", i + 1))),
        Expr::Column { table, name } => {
            let idx = resolve_col(schema, table, name)?;
            row.val(idx)
                .cloned()
                .ok_or_else(|| SqlError::Eval(format!("column {name:?} out of range")))
        }
        Expr::IsNull { expr, negated } => {
            let v = eval_scalar(expr, schema, row, params)?;
            Ok(Value::Bool(matches!(v, Value::Null) != *negated))
        }
        Expr::Unary { op, expr } => {
            let v = eval_scalar(expr, schema, row, params)?;
            apply_unary(*op, v)
        }
        Expr::Binary { op, left, right } => {
            // AND/OR short-circuit: once the left operand decides the result,
            // the right is never evaluated. Only a definitive FALSE (AND) or
            // TRUE (OR) decides — SQL's three-valued logic makes
            // `NULL AND FALSE` FALSE and `NULL OR TRUE` TRUE, so a NULL (or
            // non-boolean, which `three_valued` also treats as unknown) left
            // still has to evaluate the right. Those two cases mirror
            // `three_valued`'s `(Some(false), _)` / `(Some(true), _)` arms
            // exactly, so this only skips work — it never changes a result.
            if matches!(op, BinOp::And | BinOp::Or) {
                let l = eval_scalar(left, schema, row, params)?;
                match (op, &l) {
                    (BinOp::And, Value::Bool(false)) => return Ok(Value::Bool(false)),
                    (BinOp::Or, Value::Bool(true)) => return Ok(Value::Bool(true)),
                    _ => {}
                }
                let r = eval_scalar(right, schema, row, params)?;
                return eval_binary(*op, l, r);
            }
            // Comparisons of borrowable operands compare in place. The owned
            // path below clones both sides per row — for a TEXT column that
            // is a heap allocation just to run a comparison, and for
            // SUBSTRING-of-a-column (EF's StartsWith/EndsWith) an extra
            // intermediate string on top.
            if matches!(
                op,
                BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge
            ) {
                let map_ord = |ord: Ordering| {
                    Value::Bool(match op {
                        BinOp::Eq => ord == Ordering::Equal,
                        BinOp::Ne => ord != Ordering::Equal,
                        BinOp::Lt => ord == Ordering::Less,
                        BinOp::Le => ord != Ordering::Greater,
                        BinOp::Gt => ord == Ordering::Greater,
                        BinOp::Ge => ord != Ordering::Less,
                        _ => unreachable!(),
                    })
                };
                if let (Some(l), Some(r)) = (
                    str_ref(left, schema, row, params),
                    str_ref(right, schema, row, params),
                ) {
                    return Ok(match (l?, r?) {
                        (Some(l), Some(r)) => map_ord(l.cmp(r)),
                        _ => Value::Null,
                    });
                }
                if let (Some(l), Some(r)) =
                    (val_ref(left, row, params), val_ref(right, row, params))
                {
                    if matches!(l, Value::Null) || matches!(r, Value::Null) {
                        return Ok(Value::Null);
                    }
                    return match cmp_values(l, r) {
                        Some(ord) => Ok(map_ord(ord)),
                        None => Err(SqlError::Eval(format!("cannot compare {l:?} and {r:?}"))),
                    };
                }
            }
            let l = eval_scalar(left, schema, row, params)?;
            let r = eval_scalar(right, schema, row, params)?;
            eval_binary(*op, l, r)
        }
        Expr::In {
            expr,
            list,
            negated,
        } => {
            // Probe value borrowed when possible (IN lists loop per row).
            if let Some(v) = val_ref(expr, row, params) {
                return eval_in(v, list, *negated, |item| {
                    eval_scalar(item, schema, row, params)
                });
            }
            let v = eval_scalar(expr, schema, row, params)?;
            eval_in(&v, list, *negated, |item| {
                eval_scalar(item, schema, row, params)
            })
        }
        Expr::InSet {
            expr,
            set,
            has_null,
            negated,
        } => {
            let owned;
            let v: &Value = match val_ref(expr, row, params) {
                Some(v) => v,
                None => {
                    owned = eval_scalar(expr, schema, row, params)?;
                    &owned
                }
            };
            if matches!(v, Value::Null) {
                return Ok(Value::Null);
            }
            Ok(if set.contains(&IndexKey(v.clone())) {
                Value::Bool(!*negated)
            } else if *has_null {
                Value::Null
            } else {
                Value::Bool(*negated)
            })
        }
        Expr::Func { func, args } => {
            if let Some(v) = eval_func_ref(func, args, row, params) {
                return v;
            }
            eval_scalar_func(*func, args, |e| eval_scalar(e, schema, row, params))
        }
        Expr::Aggregate { .. } => Err(SqlError::Eval(
            "aggregate function used outside an aggregated query".into(),
        )),
        Expr::Subquery(_) | Expr::InSubquery { .. } => Err(SqlError::Eval(
            "internal: unresolved subquery reached evaluation".into(),
        )),
        Expr::CorrScalar { .. } | Expr::CorrIn { .. } => Err(SqlError::Eval(
            "internal: correlated subquery reached direct evaluation".into(),
        )),
        Expr::Window { .. } => Err(SqlError::Unsupported(
            "window function only allowed in the SELECT list".into(),
        )),
    }
}

/// Borrow a ref-transparent expression's value (a bound column, literal, or
/// parameter) without cloning; `None` = computed expression (the caller
/// falls back to owned evaluation).
#[inline]
fn val_ref<'a, R: RowLike + ?Sized>(
    e: &'a Expr,
    row: &'a R,
    params: &'a [Value],
) -> Option<&'a Value> {
    match e {
        Expr::Col(i) => row.val(*i),
        Expr::Literal(v) => Some(v),
        Expr::Param(i) => params.get(*i),
        _ => None,
    }
}

/// A borrowed string view of an expression, produced without allocating: a
/// ref-transparent TEXT value, or SUBSTRING over one (the result is a slice
/// of the original). Outer `None` = not borrowable this way (the caller
/// falls back to owned evaluation — e.g. non-text inputs that the generic
/// path coerces); `Some(Ok(None))` = SQL NULL.
fn str_ref<'a, R: RowLike + ?Sized>(
    e: &'a Expr,
    schema: &[ColRef],
    row: &'a R,
    params: &'a [Value],
) -> Option<Result<Option<&'a str>>> {
    match e {
        Expr::Func {
            func: ScalarFunc::Substring,
            args,
        } if args.len() >= 2 => {
            let s = match str_ref(&args[0], schema, row, params)? {
                Ok(Some(s)) => s,
                other => return Some(other), // NULL (or error) propagates
            };
            let start = match eval_scalar(&args[1], schema, row, params) {
                Ok(Value::Null) => return Some(Ok(None)),
                Ok(Value::Int(n)) => n,
                Ok(other) => {
                    return Some(Err(SqlError::Eval(format!("SUBSTRING start {other:?}"))));
                }
                Err(e) => return Some(Err(e)),
            };
            let len = match args.get(2) {
                None => None,
                Some(le) => match eval_scalar(le, schema, row, params) {
                    Ok(Value::Null) => return Some(Ok(None)),
                    Ok(Value::Int(n)) if n >= 0 => Some(n as usize),
                    Ok(Value::Int(_)) => {
                        return Some(Err(SqlError::Eval("SUBSTRING with negative length".into())));
                    }
                    Ok(other) => {
                        return Some(Err(SqlError::Eval(format!("SUBSTRING length {other:?}"))));
                    }
                    Err(e) => return Some(Err(e)),
                },
            };
            Some(Ok(Some(substr_slice(s, start, len))))
        }
        _ => match val_ref(e, row, params)? {
            Value::Text(s) => Some(Ok(Some(&**s))),
            Value::Null => Some(Ok(None)),
            _ => None,
        },
    }
}

/// `SUBSTRING` as a borrowed slice — mirrors the owned implementation
/// exactly (1-based, character-based, out-of-range clamps to empty, a start
/// below 1 consumes length before the string begins).
fn substr_slice(s: &str, start: i64, len: Option<usize>) -> &str {
    let skip = (start.max(1) - 1) as usize;
    // ASCII fast path: byte index == char index, so slice in O(1) instead of
    // walking char boundaries — the hot path for EF's StartsWith/EndsWith,
    // which are SUBSTRING-of-an-ASCII-column comparisons.
    if s.is_ascii() {
        let begin = skip.min(s.len());
        return match len {
            None => &s[begin..],
            Some(l) => {
                let consumed = (1 - start.min(1)) as usize;
                let end = (begin + l.saturating_sub(consumed)).min(s.len());
                &s[begin..end]
            }
        };
    }
    let begin = s.char_indices().nth(skip).map_or(s.len(), |(b, _)| b);
    match len {
        None => &s[begin..],
        Some(l) => {
            let consumed = (1 - start.min(1)) as usize;
            let take = l.saturating_sub(consumed);
            let end = s[begin..]
                .char_indices()
                .nth(take)
                .map_or(s.len(), |(b, _)| begin + b);
            &s[begin..end]
        }
    }
}

/// Zero-clone fast paths for the hot string predicates when every argument
/// is a ref-transparent TEXT (or NULL) value. `None` falls back to the
/// generic implementation (which owns, coerces non-text, and must match
/// these semantics exactly for TEXT inputs).
fn eval_func_ref<R: RowLike + ?Sized>(
    func: &ScalarFunc,
    args: &[Expr],
    row: &R,
    params: &[Value],
) -> Option<Result<Value>> {
    // Outer None = not a fast-path shape; inner None = SQL NULL.
    fn text<'a, R: RowLike + ?Sized>(
        e: &'a Expr,
        row: &'a R,
        params: &'a [Value],
    ) -> Option<Option<&'a str>> {
        match val_ref(e, row, params)? {
            Value::Text(s) => Some(Some(&**s)),
            Value::Null => Some(None),
            _ => None,
        }
    }
    match func {
        ScalarFunc::Position if args.len() == 2 => {
            let (s, sub) = (text(&args[0], row, params)?, text(&args[1], row, params)?);
            Some(Ok(match (s, sub) {
                // 1-based character index; 0 = not found; empty needle → 1.
                (Some(s), Some(sub)) => Value::Int(match s.find(sub) {
                    Some(byte) => s[..byte].chars().count() as i64 + 1,
                    None => 0,
                }),
                _ => Value::Null,
            }))
        }
        ScalarFunc::Like { negated, escape } if args.len() == 2 => {
            let (s, p) = (text(&args[0], row, params)?, text(&args[1], row, params)?);
            Some(Ok(match (s, p) {
                (Some(s), Some(p)) => Value::Bool(like_match(s, p, *escape) != *negated),
                _ => Value::Null,
            }))
        }
        ScalarFunc::Length if args.len() == 1 => {
            let s = text(&args[0], row, params)?;
            Some(Ok(match s {
                Some(s) => Value::Int(s.chars().count() as i64),
                None => Value::Null,
            }))
        }
        // Affix tests compare borrowed bytes in place: no substring is
        // materialised and the string is never scanned to count characters,
        // which is what a `SUBSTRING(s, LENGTH(s) - n + 1, n) = affix`
        // rendering costs per row.
        ScalarFunc::StartsWith | ScalarFunc::EndsWith if args.len() == 2 => {
            let (s, affix) = (text(&args[0], row, params)?, text(&args[1], row, params)?);
            Some(Ok(match (s, affix) {
                (Some(s), Some(a)) => Value::Bool(match func {
                    ScalarFunc::StartsWith => s.starts_with(a),
                    _ => s.ends_with(a),
                }),
                _ => Value::Null,
            }))
        }
        _ => None,
    }
}

/// Evaluate a row-scalar function over lazily-evaluated arguments.
/// COALESCE and CASE short-circuit.
fn eval_scalar_func<F>(func: ScalarFunc, args: &[Expr], mut eval: F) -> Result<Value>
where
    F: FnMut(&Expr) -> Result<Value>,
{
    // NULL-propagating string extraction.
    fn as_text(v: Value, what: &str) -> Result<Option<String>> {
        match v {
            Value::Null => Ok(None),
            Value::Text(s) => Ok(Some(s.into())),
            Value::Int(n) => Ok(Some(n.to_string())),
            Value::Double(f) => Ok(Some(f.to_string())),
            Value::Bool(b) => Ok(Some(b.to_string())),
            Value::Decimal(d) => Ok(Some(d.to_string())),
            other => Err(SqlError::Eval(format!("{what} of {other:?}"))),
        }
    }

    match func {
        ScalarFunc::Coalesce => {
            for a in args {
                let v = eval(a)?;
                if !matches!(v, Value::Null) {
                    return Ok(v);
                }
            }
            Ok(Value::Null)
        }
        ScalarFunc::Least | ScalarFunc::Greatest => {
            // Smallest/largest non-NULL argument (NULLs ignored, like Postgres;
            // all-NULL → NULL). Cross-type ordering matches MIN/MAX.
            let want = if matches!(func, ScalarFunc::Least) {
                Ordering::Less
            } else {
                Ordering::Greater
            };
            let mut best: Option<Value> = None;
            for a in args {
                let v = eval(a)?;
                if matches!(v, Value::Null) {
                    continue;
                }
                match &best {
                    None => best = Some(v),
                    Some(cur) => {
                        if Value::total_order(&v, cur) == want {
                            best = Some(v);
                        }
                    }
                }
            }
            Ok(best.unwrap_or(Value::Null))
        }
        ScalarFunc::NullIf => {
            let a = eval(&args[0])?;
            let b = eval(&args[1])?;
            if cmp_values(&a, &b) == Some(Ordering::Equal) {
                Ok(Value::Null)
            } else {
                Ok(a)
            }
        }
        ScalarFunc::Case { has_else } => {
            let pairs = if has_else { args.len() - 1 } else { args.len() } / 2;
            for k in 0..pairs {
                if truthy(&eval(&args[k * 2])?) {
                    return eval(&args[k * 2 + 1]);
                }
            }
            if has_else {
                eval(&args[args.len() - 1])
            } else {
                Ok(Value::Null)
            }
        }
        ScalarFunc::Upper => Ok(match as_text(eval(&args[0])?, "UPPER")? {
            Some(s) => Value::Text((s.to_uppercase()).into()),
            None => Value::Null,
        }),
        ScalarFunc::Lower => Ok(match as_text(eval(&args[0])?, "LOWER")? {
            Some(s) => Value::Text((s.to_lowercase()).into()),
            None => Value::Null,
        }),
        ScalarFunc::Length => Ok(match as_text(eval(&args[0])?, "LENGTH")? {
            Some(s) => Value::Int(s.chars().count() as i64),
            None => Value::Null,
        }),
        ScalarFunc::Trim | ScalarFunc::Ltrim | ScalarFunc::Rtrim => {
            let Some(s) = as_text(eval(&args[0])?, "TRIM")? else {
                return Ok(Value::Null);
            };
            // Optional second argument: the set of characters to strip
            // (default: whitespace).
            let charset: Option<Vec<char>> = match args.get(1) {
                None => None,
                Some(e) => match as_text(eval(e)?, "TRIM")? {
                    Some(cs) => Some(cs.chars().collect()),
                    None => return Ok(Value::Null),
                },
            };
            let pred = |c: char| match &charset {
                Some(set) => set.contains(&c),
                None => c.is_whitespace(),
            };
            Ok(Value::Text(
                match func {
                    ScalarFunc::Ltrim => s.trim_start_matches(pred),
                    ScalarFunc::Rtrim => s.trim_end_matches(pred),
                    _ => s.trim_matches(pred),
                }
                .into(),
            ))
        }
        ScalarFunc::Concat => {
            let mut out = String::new();
            for a in args {
                match as_text(eval(a)?, "CONCAT")? {
                    Some(s) => out.push_str(&s),
                    None => return Ok(Value::Null),
                }
            }
            Ok(Value::Text((out).into()))
        }
        ScalarFunc::Replace => {
            let (s, from, to) = (
                as_text(eval(&args[0])?, "REPLACE")?,
                as_text(eval(&args[1])?, "REPLACE")?,
                as_text(eval(&args[2])?, "REPLACE")?,
            );
            Ok(match (s, from, to) {
                (Some(s), Some(f), Some(t)) if !f.is_empty() => {
                    Value::Text((s.replace(&f, &t)).into())
                }
                (Some(s), Some(_), Some(_)) => Value::Text((s).into()),
                _ => Value::Null,
            })
        }
        ScalarFunc::Substring => {
            let Some(s) = as_text(eval(&args[0])?, "SUBSTRING")? else {
                return Ok(Value::Null);
            };
            let start = match eval(&args[1])? {
                Value::Null => return Ok(Value::Null),
                Value::Int(n) => n,
                other => return Err(SqlError::Eval(format!("SUBSTRING start {other:?}"))),
            };
            let len = match args.get(2) {
                None => None,
                Some(e) => match eval(e)? {
                    Value::Null => return Ok(Value::Null),
                    Value::Int(n) if n >= 0 => Some(n as usize),
                    Value::Int(_) => {
                        return Err(SqlError::Eval("SUBSTRING with negative length".into()));
                    }
                    other => return Err(SqlError::Eval(format!("SUBSTRING length {other:?}"))),
                },
            };
            // 1-based, character-based; out-of-range clamps to empty.
            let skip = (start.max(1) - 1) as usize;
            let it = s.chars().skip(skip);
            let out: String = match len {
                // A start below 1 consumes length before the string begins.
                Some(l) => {
                    let consumed = (1 - start.min(1)) as usize;
                    it.take(l.saturating_sub(consumed)).collect()
                }
                None => it.collect(),
            };
            Ok(Value::Text((out).into()))
        }
        ScalarFunc::Abs => Ok(match eval(&args[0])? {
            Value::Null => Value::Null,
            Value::Int(n) => Value::Int(n.abs()),
            Value::Double(f) => Value::Double(f.abs()),
            Value::Decimal(d) => Value::Decimal(if d.mantissa() < 0 {
                Box::new(d.neg())
            } else {
                d
            }),
            other => return Err(SqlError::Eval(format!("ABS of {other:?}"))),
        }),
        ScalarFunc::Round => {
            let x = eval(&args[0])?;
            if matches!(x, Value::Null) {
                return Ok(Value::Null);
            }
            // Optional second argument: the number of fractional digits.
            let places = match args.get(1) {
                None => 0u32,
                Some(e) => match eval(e)? {
                    Value::Null => return Ok(Value::Null),
                    Value::Int(n) if n >= 0 => n as u32,
                    _ => {
                        return Err(SqlError::Eval(
                            "ROUND scale must be a non-negative integer".into(),
                        ));
                    }
                },
            };
            Ok(match x {
                Value::Decimal(d) => Value::Decimal(Box::new(d.round(places))),
                Value::Double(f) => {
                    let m = 10f64.powi(places as i32);
                    Value::Double((f * m).round() / m)
                }
                // Integers are already whole; the scale is irrelevant.
                Value::Int(n) => Value::Int(n),
                other => return Err(SqlError::Eval(format!("ROUND of {other:?}"))),
            })
        }
        ScalarFunc::Cast(ty) => cast_value(eval(&args[0])?, ty),
        ScalarFunc::Like { negated, escape } => {
            let s = eval(&args[0])?;
            let pat = eval(&args[1])?;
            match (s, pat) {
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                (Value::Text(s), Value::Text(p)) => {
                    Ok(Value::Bool(like_match(&s, &p, escape) != negated))
                }
                (a, b) => Err(SqlError::Eval(format!("LIKE on {a:?} / {b:?}"))),
            }
        }
        // Case-sensitive, literal affix tests. Non-TEXT arguments coerce the
        // same way the other string functions do; the borrowed fast path in
        // `eval_func_ref` must agree with this for TEXT inputs.
        ScalarFunc::StartsWith | ScalarFunc::EndsWith => {
            let s = as_text(eval(&args[0])?, "starts_with/ends_with")?;
            let affix = as_text(eval(&args[1])?, "starts_with/ends_with")?;
            Ok(match (s, affix) {
                (Some(s), Some(a)) => Value::Bool(match func {
                    ScalarFunc::StartsWith => s.starts_with(&a),
                    _ => s.ends_with(&a),
                }),
                _ => Value::Null,
            })
        }
        ScalarFunc::Now => {
            let ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            Ok(Value::Timestamp(ms))
        }
        ScalarFunc::Extract(part) => match eval(&args[0])? {
            Value::Null => Ok(Value::Null),
            Value::Timestamp(ms) => Ok(extract_part(part, ms)),
            other => Err(SqlError::Eval(format!(
                "EXTRACT from {other:?} (TIMESTAMP required)"
            ))),
        },
        ScalarFunc::DateTrunc(part) => match eval(&args[0])? {
            Value::Null => Ok(Value::Null),
            Value::Timestamp(ms) => Ok(Value::Timestamp(date_trunc_ms(part, ms)?)),
            other => Err(SqlError::Eval(format!(
                "DATE_TRUNC of {other:?} (TIMESTAMP required)"
            ))),
        },
        ScalarFunc::Floor | ScalarFunc::Ceil => {
            let up = matches!(func, ScalarFunc::Ceil);
            Ok(match eval(&args[0])? {
                Value::Null => Value::Null,
                Value::Int(n) => Value::Int(n),
                Value::Double(f) => Value::Double(if up { f.ceil() } else { f.floor() }),
                Value::Decimal(d) => {
                    // Integer floor/ceil of mantissa/10^scale, exactly.
                    let unit = 10i128.pow(d.scale());
                    let m = d.mantissa();
                    let q = if up {
                        -(-m).div_euclid(unit)
                    } else {
                        m.div_euclid(unit)
                    };
                    Value::Decimal(Box::new(Decimal::new(q, 0)))
                }
                other => return Err(SqlError::Eval(format!("FLOOR/CEILING of {other:?}"))),
            })
        }
        ScalarFunc::Power => {
            let (a, b) = (eval(&args[0])?, eval(&args[1])?);
            if matches!(a, Value::Null) || matches!(b, Value::Null) {
                return Ok(Value::Null);
            }
            let x = as_f64(&a).ok_or_else(|| SqlError::Eval(format!("POWER of {a:?}")))?;
            let y = as_f64(&b).ok_or_else(|| SqlError::Eval(format!("POWER of {b:?}")))?;
            Ok(Value::Double(x.powf(y)))
        }
        ScalarFunc::Sqrt => {
            let a = eval(&args[0])?;
            if matches!(a, Value::Null) {
                return Ok(Value::Null);
            }
            let x = as_f64(&a).ok_or_else(|| SqlError::Eval(format!("SQRT of {a:?}")))?;
            if x < 0.0 {
                return Err(SqlError::Eval("SQRT of a negative number".into()));
            }
            Ok(Value::Double(x.sqrt()))
        }
        ScalarFunc::Position => {
            let (s, sub) = (
                as_text(eval(&args[0])?, "POSITION")?,
                as_text(eval(&args[1])?, "POSITION")?,
            );
            Ok(match (s, sub) {
                // 1-based character index; 0 = not found; empty needle → 1.
                (Some(s), Some(sub)) => Value::Int(match s.find(&sub) {
                    Some(byte) => s[..byte].chars().count() as i64 + 1,
                    None => 0,
                }),
                _ => Value::Null,
            })
        }
        ScalarFunc::Lpad | ScalarFunc::Rpad => {
            let Some(s) = as_text(eval(&args[0])?, "LPAD")? else {
                return Ok(Value::Null);
            };
            let len = match eval(&args[1])? {
                Value::Null => return Ok(Value::Null),
                Value::Int(n) => n.max(0) as usize,
                other => return Err(SqlError::Eval(format!("LPAD length {other:?}"))),
            };
            let fill = match args.get(2) {
                None => " ".to_string(),
                Some(e) => match as_text(eval(e)?, "LPAD")? {
                    Some(f) => f,
                    None => return Ok(Value::Null),
                },
            };
            let n = s.chars().count();
            Ok(Value::Text(
                if n >= len {
                    // Longer input truncates to `len` (PostgreSQL semantics).
                    s.chars().take(len).collect()
                } else if fill.is_empty() {
                    s
                } else {
                    let pad: String = fill.chars().cycle().take(len - n).collect();
                    if matches!(func, ScalarFunc::Lpad) {
                        format!("{pad}{s}")
                    } else {
                        format!("{s}{pad}")
                    }
                }
                .into(),
            ))
        }
        ScalarFunc::Sin
        | ScalarFunc::Cos
        | ScalarFunc::Tan
        | ScalarFunc::Asin
        | ScalarFunc::Acos
        | ScalarFunc::Atan
        | ScalarFunc::Exp
        | ScalarFunc::Ln
        | ScalarFunc::Log10
        | ScalarFunc::Degrees
        | ScalarFunc::Radians => {
            let v = eval(&args[0])?;
            if matches!(v, Value::Null) {
                return Ok(Value::Null);
            }
            let x = as_f64(&v).ok_or_else(|| SqlError::Eval(format!("math function of {v:?}")))?;
            let y = match func {
                ScalarFunc::Sin => x.sin(),
                ScalarFunc::Cos => x.cos(),
                ScalarFunc::Tan => x.tan(),
                ScalarFunc::Asin => x.asin(),
                ScalarFunc::Acos => x.acos(),
                ScalarFunc::Atan => x.atan(),
                ScalarFunc::Exp => x.exp(),
                // Out-of-domain logs yield NULL (SQLite semantics — WHERE
                // predicates over mixed rows must not abort the scan).
                ScalarFunc::Ln => {
                    if x <= 0.0 {
                        return Ok(Value::Null);
                    }
                    x.ln()
                }
                ScalarFunc::Log10 => {
                    if x <= 0.0 {
                        return Ok(Value::Null);
                    }
                    x.log10()
                }
                ScalarFunc::Degrees => x.to_degrees(),
                _ => x.to_radians(),
            };
            Ok(Value::Double(y))
        }
        ScalarFunc::Atan2 | ScalarFunc::Log => {
            let (a, b) = (eval(&args[0])?, eval(&args[1])?);
            if matches!(a, Value::Null) || matches!(b, Value::Null) {
                return Ok(Value::Null);
            }
            let x = as_f64(&a).ok_or_else(|| SqlError::Eval(format!("math function of {a:?}")))?;
            let y = as_f64(&b).ok_or_else(|| SqlError::Eval(format!("math function of {b:?}")))?;
            Ok(Value::Double(if matches!(func, ScalarFunc::Atan2) {
                // ATAN2(y, x).
                x.atan2(y)
            } else {
                // LOG(base, x) — PostgreSQL argument order; NULL out of domain.
                if x <= 0.0 || x == 1.0 || y <= 0.0 {
                    return Ok(Value::Null);
                }
                y.log(x)
            }))
        }
        ScalarFunc::Sign => Ok(match eval(&args[0])? {
            Value::Null => Value::Null,
            Value::Int(n) => Value::Int(n.signum()),
            Value::Double(f) => Value::Int(if f > 0.0 {
                1
            } else if f < 0.0 {
                -1
            } else {
                0
            }),
            Value::Decimal(d) => Value::Int(d.mantissa().signum() as i64),
            other => return Err(SqlError::Eval(format!("SIGN of {other:?}"))),
        }),
        ScalarFunc::Trunc => Ok(match eval(&args[0])? {
            Value::Null => Value::Null,
            Value::Int(n) => Value::Int(n),
            Value::Double(f) => Value::Double(f.trunc()),
            Value::Decimal(d) => {
                // Integer part toward zero, exactly.
                let unit = 10i128.pow(d.scale());
                Value::Decimal(Box::new(Decimal::new(d.mantissa() / unit, 0)))
            }
            other => return Err(SqlError::Eval(format!("TRUNC of {other:?}"))),
        }),
        ScalarFunc::Collate { case_insensitive } => Ok(match eval(&args[0])? {
            Value::Null => Value::Null,
            Value::Text(s) => Value::Text(if case_insensitive {
                s.to_lowercase().into()
            } else {
                s
            }),
            other => return Err(SqlError::Eval(format!("COLLATE on {other:?}"))),
        }),
        ScalarFunc::Random => {
            use std::sync::atomic::{AtomicU64, Ordering as AOrd};
            static STATE: AtomicU64 = AtomicU64::new(0);
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.subsec_nanos() as u64 | 1)
                .unwrap_or(1);
            let mut x = STATE.fetch_add(0x9E37_79B9_7F4A_7C15, AOrd::Relaxed) ^ nanos;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            Ok(Value::Double((x >> 11) as f64 / (1u64 << 53) as f64))
        }
        ScalarFunc::RegexpLike => {
            let (s, pat) = (
                as_text(eval(&args[0])?, "REGEXP_LIKE")?,
                as_text(eval(&args[1])?, "REGEXP_LIKE")?,
            );
            match (s, pat) {
                (Some(s), Some(p)) => {
                    let re = regex::Regex::new(&p)
                        .map_err(|e| SqlError::Eval(format!("bad regex: {e}")))?;
                    Ok(Value::Bool(re.is_match(&s)))
                }
                _ => Ok(Value::Null),
            }
        }
        ScalarFunc::AddMonths => {
            let (ts, n) = (eval(&args[0])?, eval(&args[1])?);
            match (ts, n) {
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                (Value::Timestamp(ms), Value::Int(n)) => {
                    let (days, in_day) = ms_to_days(ms);
                    let (y, m, d) = civil_from_days(days);
                    let total = y * 12 + (m - 1) + n;
                    let (ny, nm) = (total.div_euclid(12), total.rem_euclid(12) + 1);
                    // Clamp the day to the target month (Jan 31 + 1mo = Feb 28).
                    let nd = d.min(days_in_month(ny, nm));
                    Ok(Value::Timestamp(
                        days_from_civil(ny, nm, nd) * 86_400_000 + in_day,
                    ))
                }
                (a, b) => Err(SqlError::Eval(format!(
                    "ADD_MONTHS of {a:?}, {b:?} (TIMESTAMP, INT required)"
                ))),
            }
        }
    }
}

/// `CAST(v AS ty)` — NULL passes through; failed text parses are errors.
/// Also the cast `ALTER COLUMN TYPE` applies to existing rows (`lib.rs`).
pub(crate) fn cast_value(v: Value, ty: SqlType) -> Result<Value> {
    use SqlType as T;
    let fail = |v: &Value| SqlError::Eval(format!("cannot cast {v:?} to {ty:?}"));
    Ok(match (v, ty) {
        (Value::Null, _) => Value::Null,
        (v @ Value::Int(_), T::Int)
        | (v @ Value::Double(_), T::Double)
        | (v @ Value::Text(_), T::Text)
        | (v @ Value::Bool(_), T::Bool)
        | (v @ Value::Timestamp(_), T::Timestamp)
        | (v @ Value::Decimal(_), T::Decimal) => v,
        // → DECIMAL (exact from Int/Text; best-effort from a Double via its
        // shortest decimal string — the float's origin caveat carries through).
        (Value::Int(n), T::Decimal) => Value::Decimal(Box::new(Decimal::from_i64(n))),
        (Value::Text(s), T::Decimal) => Value::Decimal(Box::new(
            Decimal::parse(s.trim()).ok_or_else(|| fail(&Value::Text(s.clone())))?,
        )),
        (Value::Double(f), T::Decimal) => Value::Decimal(Box::new(
            Decimal::parse(&format!("{f}")).ok_or_else(|| fail(&Value::Double(f)))?,
        )),
        // DECIMAL → other numeric / text.
        (Value::Decimal(d), T::Int) => Value::Int(d.to_i64()),
        (Value::Decimal(d), T::Double) => Value::Double(d.to_f64()),
        (Value::Decimal(d), T::Text) => Value::Text((d.to_string()).into()),
        (Value::Int(n), T::Double) => Value::Double(n as f64),
        (Value::Int(n), T::Bool) => Value::Bool(n != 0),
        (Value::Int(n), T::Timestamp) => Value::Timestamp(n),
        (Value::Double(f), T::Int) => Value::Int(f.trunc() as i64),
        (Value::Bool(b), T::Int) => Value::Int(b as i64),
        (Value::Timestamp(t), T::Int) => Value::Int(t),
        (Value::Int(n), T::Text) => Value::Text((n.to_string()).into()),
        (Value::Double(f), T::Text) => Value::Text((f.to_string()).into()),
        (Value::Bool(b), T::Text) => Value::Text((b.to_string()).into()),
        (Value::Timestamp(t), T::Text) => Value::Text((t.to_string()).into()),
        (Value::Text(s), T::Int) => Value::Int(
            s.trim()
                .parse()
                .map_err(|_| fail(&Value::Text(s.clone())))?,
        ),
        (Value::Text(s), T::Double) => Value::Double(
            s.trim()
                .parse()
                .map_err(|_| fail(&Value::Text(s.clone())))?,
        ),
        (Value::Text(s), T::Bool) => match s.trim().to_ascii_lowercase().as_str() {
            "true" | "t" | "1" => Value::Bool(true),
            "false" | "f" | "0" => Value::Bool(false),
            _ => return Err(fail(&Value::Text(s))),
        },
        (v, _) => return Err(fail(&v)),
    })
}

/// SQL LIKE: `%` matches any sequence, `_` any single character; `escape`
/// makes the following character literal. ASCII case-insensitive
/// (SQLite/SQL-Server-style — what EF and its providers expect),
/// character-based.
fn like_match(s: &str, pattern: &str, escape: Option<char>) -> bool {
    // Fast path for the wildcard-free / single-boundary-`%` patterns EF emits
    // for StartsWith/EndsWith/Contains, over ASCII: byte-level
    // `eq_ignore_ascii_case` (matching the general path's ASCII case-fold) with
    // NO per-call Vec<char> allocation and no backtracking. This is the hot
    // path for LIKE-heavy scans (e.g. `Name LIKE 'x%' AND Name LIKE '%y'`).
    if escape.is_none() && s.is_ascii() && pattern.is_ascii() && !pattern.contains('_') {
        let pct = pattern.bytes().filter(|&b| b == b'%').count();
        let sb = s.as_bytes();
        match pct {
            0 => return s.eq_ignore_ascii_case(pattern),
            1 if pattern.ends_with('%') => {
                let pre = &pattern.as_bytes()[..pattern.len() - 1];
                return sb.len() >= pre.len() && sb[..pre.len()].eq_ignore_ascii_case(pre);
            }
            1 if pattern.starts_with('%') => {
                let suf = &pattern.as_bytes()[1..];
                return sb.len() >= suf.len()
                    && sb[sb.len() - suf.len()..].eq_ignore_ascii_case(suf);
            }
            2 if pattern.starts_with('%') && pattern.ends_with('%') => {
                let mid = pattern[1..pattern.len() - 1].to_ascii_lowercase();
                return s.to_ascii_lowercase().contains(&mid);
            }
            _ => {}
        }
    }
    fn rec(s: &[char], p: &[char], escape: Option<char>) -> bool {
        match p.split_first() {
            None => s.is_empty(),
            Some((&c, rest)) if Some(c) == escape => match rest.split_first() {
                Some((&lit, rest2)) => s
                    .split_first()
                    .is_some_and(|(&sc, srest)| sc == lit && rec(srest, rest2, escape)),
                None => s.len() == 1 && s[0] == c, // trailing escape = literal
            },
            Some(('%', rest)) => (0..=s.len()).any(|k| rec(&s[k..], rest, escape)),
            Some(('_', rest)) => s
                .split_first()
                .is_some_and(|(_, srest)| rec(srest, rest, escape)),
            Some((&c, rest)) => s
                .split_first()
                .is_some_and(|(&sc, srest)| sc == c && rec(srest, rest, escape)),
        }
    }
    let sc: Vec<char> = s.chars().map(|c| c.to_ascii_lowercase()).collect();
    let pc: Vec<char> = pattern.chars().map(|c| c.to_ascii_lowercase()).collect();
    rec(&sc, &pc, escape.map(|c| c.to_ascii_lowercase()))
}

/// SQL `IN` with three-valued logic: true if any element equals `v`; NULL if
/// nothing matched but `v` or an element is NULL; false otherwise. Values of
/// incomparable types simply don't match.
fn eval_in<F>(v: &Value, list: &[Expr], negated: bool, mut eval_item: F) -> Result<Value>
where
    F: FnMut(&Expr) -> Result<Value>,
{
    let mut saw_null = matches!(v, Value::Null);
    let mut found = false;
    if !saw_null {
        for item in list {
            let iv = eval_item(item)?;
            if matches!(iv, Value::Null) {
                saw_null = true;
                continue;
            }
            if cmp_values(v, &iv) == Some(Ordering::Equal) {
                found = true;
                break;
            }
        }
    }
    Ok(if found {
        Value::Bool(!negated)
    } else if saw_null {
        Value::Null
    } else {
        Value::Bool(negated)
    })
}

/// Resolve every `Column` reference in `expr` to a positional [`Expr::Col`]
/// against `schema`, so later per-row evaluation is O(1). Also validates that
/// all columns exist / are unambiguous (replacing the old check_columns pass).
fn bind_expr(expr: &Expr, schema: &[ColRef]) -> Result<Expr> {
    let bound = match expr {
        Expr::Column { table, name } => Expr::Col(resolve_col(schema, table, name)?),
        Expr::Col(i) => Expr::Col(*i),
        Expr::Literal(v) => Expr::Literal(v.clone()),
        Expr::Param(i) => Expr::Param(*i),
        Expr::Binary { op, left, right } => Expr::Binary {
            op: *op,
            left: Box::new(bind_expr(left, schema)?),
            right: Box::new(bind_expr(right, schema)?),
        },
        Expr::Unary { op, expr } => Expr::Unary {
            op: *op,
            expr: Box::new(bind_expr(expr, schema)?),
        },
        Expr::IsNull { expr, negated } => Expr::IsNull {
            expr: Box::new(bind_expr(expr, schema)?),
            negated: *negated,
        },
        Expr::Aggregate {
            func,
            arg,
            distinct,
        } => Expr::Aggregate {
            func: *func,
            arg: match arg {
                Some(a) => Some(Box::new(bind_expr(a, schema)?)),
                None => None,
            },
            distinct: *distinct,
        },
        Expr::InSet {
            expr,
            set,
            has_null,
            negated,
        } => Expr::InSet {
            expr: Box::new(bind_expr(expr, schema)?),
            set: set.clone(),
            has_null: *has_null,
            negated: *negated,
        },
        Expr::In {
            expr,
            list,
            negated,
        } => {
            let bexpr = bind_expr(expr, schema)?;
            let blist: Vec<Expr> = list
                .iter()
                .map(|e| bind_expr(e, schema))
                .collect::<Result<_>>()?;
            // An all-literal IN list (EF's `Contains` over a constant array)
            // becomes a set: one O(log n) membership test per row instead of
            // a list walk that clones every element every row. Same
            // three-valued semantics (`has_null` tracks NULL members).
            if !blist.is_empty() && blist.iter().all(|e| matches!(e, Expr::Literal(_))) {
                let mut set = std::collections::BTreeSet::new();
                let mut has_null = false;
                for e in &blist {
                    if let Expr::Literal(v) = e {
                        if matches!(v, Value::Null) {
                            has_null = true;
                        } else {
                            set.insert(IndexKey(v.clone()));
                        }
                    }
                }
                Expr::InSet {
                    expr: Box::new(bexpr),
                    set: std::sync::Arc::new(set),
                    has_null,
                    negated: *negated,
                }
            } else {
                Expr::In {
                    expr: Box::new(bexpr),
                    list: blist,
                    negated: *negated,
                }
            }
        }
        Expr::Func { func, args } => Expr::Func {
            func: *func,
            args: args
                .iter()
                .map(|e| bind_expr(e, schema))
                .collect::<Result<_>>()?,
        },
        // A correlated node's outer refs and probe belong to the outer schema
        // and bind here; the inner query binds against its own tables when it
        // executes per row.
        Expr::CorrScalar {
            query,
            outer,
            base,
            agg_map,
        } => Expr::CorrScalar {
            query: query.clone(),
            outer: outer
                .iter()
                .map(|o| bind_expr(o, schema))
                .collect::<Result<_>>()?,
            base: *base,
            agg_map: agg_map.clone(),
        },
        Expr::CorrIn {
            expr,
            query,
            outer,
            base,
            negated,
        } => Expr::CorrIn {
            expr: Box::new(bind_expr(expr, schema)?),
            query: query.clone(),
            outer: outer
                .iter()
                .map(|o| bind_expr(o, schema))
                .collect::<Result<_>>()?,
            base: *base,
            negated: *negated,
        },
        Expr::Window {
            func,
            partition_by,
            order_by,
        } => Expr::Window {
            func: match func {
                WindowFunc::Agg(f, Some(a)) => {
                    WindowFunc::Agg(*f, Some(Box::new(bind_expr(a, schema)?)))
                }
                other => other.clone(),
            },
            partition_by: partition_by
                .iter()
                .map(|e| bind_expr(e, schema))
                .collect::<Result<_>>()?,
            order_by: order_by
                .iter()
                .map(|(e, a)| Ok::<_, SqlError>((bind_expr(e, schema)?, *a)))
                .collect::<Result<_>>()?,
        },
        Expr::Subquery(_) | Expr::InSubquery { .. } => {
            return Err(SqlError::Eval(
                "internal: unresolved subquery reached binding".into(),
            ));
        }
    };
    Ok(fold_constant(bound))
}

/// Whether an expression depends only on literals — no columns, params,
/// aggregates, windows, subqueries or correlation. Such a subtree evaluates
/// to the same value on every row.
fn is_const_expr(e: &Expr) -> bool {
    match e {
        Expr::Literal(_) => true,
        Expr::Col(_)
        | Expr::Column { .. }
        | Expr::Param(_)
        | Expr::Aggregate { .. }
        | Expr::Window { .. }
        | Expr::Subquery(_)
        | Expr::InSubquery { .. }
        | Expr::CorrScalar { .. }
        | Expr::CorrIn { .. }
        | Expr::InSet { .. } => false,
        Expr::Binary { left, right, .. } => is_const_expr(left) && is_const_expr(right),
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } => is_const_expr(expr),
        Expr::Func { args, .. } => args.iter().all(is_const_expr),
        Expr::In { expr, list, .. } => is_const_expr(expr) && list.iter().all(is_const_expr),
    }
}

/// Fold a fully-constant subexpression to its literal value. EF Core inlines
/// call arguments as literals (`LENGTH('Customer 00')`, `x * 60000`), and
/// evaluating those once at bind time instead of per row is a real win on
/// large scans. Volatile calls (`NOW()`, `random()`) are never folded, and a
/// fold that would error (e.g. a domain error) is left for per-row handling.
fn fold_constant(e: Expr) -> Expr {
    if matches!(e, Expr::Literal(_)) || !is_const_expr(&e) || has_volatile(&e) {
        return e;
    }
    let empty: &[Value] = &[];
    match eval_scalar(&e, &[], empty, empty) {
        Ok(v) => Expr::Literal(v),
        Err(_) => e,
    }
}

/// Evaluate an expression that may contain aggregates, over a group of tuple
/// indices. Non-aggregate leaves are evaluated on the group's first row (as
/// SQL requires them to be group keys).
#[allow(clippy::too_many_arguments)]
fn eval_agg<S: Store>(
    store: &S,
    expr: &Expr,
    schema: &[ColRef],
    src: &Sources,
    tuples: &Tuples,
    group: &[u32],
    params: &[Value],
) -> Result<Value> {
    match expr {
        Expr::Aggregate {
            func,
            arg,
            distinct,
        } => eval_aggregate(
            store,
            *func,
            arg.as_deref(),
            *distinct,
            schema,
            src,
            tuples,
            group,
            params,
        ),
        Expr::Literal(v) => Ok(v.clone()),
        Expr::Param(i) => params
            .get(*i)
            .cloned()
            .ok_or_else(|| SqlError::Eval(format!("missing bind parameter ${}", i + 1))),
        Expr::Col(_) | Expr::Column { .. } => {
            // A grouped column: same across the group; read from the first row.
            match group.first() {
                Some(&i) => {
                    let view = View {
                        src,
                        tuple: tuples.row(i as usize),
                    };
                    eval_scalar(expr, schema, &view, params)
                }
                None => Ok(Value::Null),
            }
        }
        Expr::IsNull { expr, negated } => {
            let v = eval_agg(store, expr, schema, src, tuples, group, params)?;
            Ok(Value::Bool(matches!(v, Value::Null) != *negated))
        }
        Expr::Unary { op, expr } => {
            let v = eval_agg(store, expr, schema, src, tuples, group, params)?;
            apply_unary(*op, v)
        }
        Expr::Binary { op, left, right } => {
            let l = eval_agg(store, left, schema, src, tuples, group, params)?;
            let r = eval_agg(store, right, schema, src, tuples, group, params)?;
            eval_binary(*op, l, r)
        }
        Expr::InSet {
            expr,
            set,
            has_null,
            negated,
        } => {
            let v = eval_agg(store, expr, schema, src, tuples, group, params)?;
            if matches!(v, Value::Null) {
                return Ok(Value::Null);
            }
            Ok(if set.contains(&IndexKey(v.clone())) {
                Value::Bool(!*negated)
            } else if *has_null {
                Value::Null
            } else {
                Value::Bool(*negated)
            })
        }
        Expr::In {
            expr,
            list,
            negated,
        } => {
            let v = eval_agg(store, expr, schema, src, tuples, group, params)?;
            eval_in(&v, list, *negated, |item| {
                eval_agg(store, item, schema, src, tuples, group, params)
            })
        }
        Expr::Func { func, args } => eval_scalar_func(*func, args, |e| {
            eval_agg(store, e, schema, src, tuples, group, params)
        }),
        Expr::Subquery(_) | Expr::InSubquery { .. } => Err(SqlError::Eval(
            "internal: unresolved subquery reached evaluation".into(),
        )),
        // A correlated subquery outside an aggregate's argument correlates
        // against the group keys, which are constant across the group:
        // evaluate it on the group's first row.
        Expr::CorrScalar { .. } | Expr::CorrIn { .. } => match group.first() {
            Some(&i) => {
                let view = View {
                    src,
                    tuple: tuples.row(i as usize),
                };
                eval_scalar_corr(store, true, expr, schema, &view, params)
            }
            None => Ok(Value::Null),
        },
        Expr::Window { .. } => Err(SqlError::Unsupported(
            "window function in an aggregated query (use a view or an outer query)".into(),
        )),
    }
}

// ── calendar helpers (UTC, epoch-millisecond timestamps) ────────────────────

/// Split epoch ms into (days since 1970-01-01, ms within that day), flooring.
fn ms_to_days(ms: i64) -> (i64, i64) {
    (ms.div_euclid(86_400_000), ms.rem_euclid(86_400_000))
}

/// Civil date from days since epoch (Howard Hinnant's civil-from-days).
fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Days since epoch for a civil date (the inverse of [`civil_from_days`];
/// mirrors the math in `parser::parse_timestamp`).
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Days in a civil month (Gregorian leap rules).
fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 {
                29
            } else {
                28
            }
        }
    }
}

/// ISO 8601 weekday: Monday = 1 .. Sunday = 7 (1970-01-01 was a Thursday).
fn iso_weekday(days: i64) -> i64 {
    (days + 3).rem_euclid(7) + 1
}

/// ISO 8601 week number (1..=53).
fn iso_week(days: i64) -> i64 {
    let weeks_in = |year: i64| -> i64 {
        let jan1 = iso_weekday(days_from_civil(year, 1, 1));
        let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
        // 53-week years start on Thursday, or on Wednesday when leap.
        if jan1 == 4 || (leap && jan1 == 3) {
            53
        } else {
            52
        }
    };
    let (y, _, _) = civil_from_days(days);
    let doy = days - days_from_civil(y, 1, 1) + 1;
    let w = (doy - iso_weekday(days) + 10) / 7;
    if w < 1 {
        weeks_in(y - 1)
    } else if w > weeks_in(y) {
        1
    } else {
        w
    }
}

/// `EXTRACT(part FROM ts)` over epoch ms, in UTC. PostgreSQL numbering:
/// DOW Sunday = 0; MILLISECONDS includes the seconds field; EPOCH is
/// fractional seconds (DOUBLE); everything else is an integer.
fn extract_part(part: DatePart, ms: i64) -> Value {
    let (days, in_day) = ms_to_days(ms);
    match part {
        DatePart::Epoch => Value::Double(ms as f64 / 1_000.0),
        DatePart::Year => Value::Int(civil_from_days(days).0),
        DatePart::Month => Value::Int(civil_from_days(days).1),
        DatePart::Day => Value::Int(civil_from_days(days).2),
        DatePart::Hour => Value::Int(in_day / 3_600_000),
        DatePart::Minute => Value::Int(in_day / 60_000 % 60),
        DatePart::Second => Value::Int(in_day / 1_000 % 60),
        DatePart::Millisecond => Value::Int(in_day % 60_000),
        DatePart::Dow => Value::Int(iso_weekday(days) % 7),
        DatePart::Doy => {
            let (y, _, _) = civil_from_days(days);
            Value::Int(days - days_from_civil(y, 1, 1) + 1)
        }
        DatePart::Week => Value::Int(iso_week(days)),
    }
}

/// `DATE_TRUNC('part', ts)` over epoch ms, in UTC. Weeks start on Monday
/// (ISO 8601, like PostgreSQL).
fn date_trunc_ms(part: DatePart, ms: i64) -> Result<i64> {
    let (days, in_day) = ms_to_days(ms);
    Ok(match part {
        DatePart::Millisecond => ms,
        DatePart::Second => days * 86_400_000 + in_day / 1_000 * 1_000,
        DatePart::Minute => days * 86_400_000 + in_day / 60_000 * 60_000,
        DatePart::Hour => days * 86_400_000 + in_day / 3_600_000 * 3_600_000,
        DatePart::Day => days * 86_400_000,
        DatePart::Week => (days - (iso_weekday(days) - 1)) * 86_400_000,
        DatePart::Month => {
            let (y, m, _) = civil_from_days(days);
            days_from_civil(y, m, 1) * 86_400_000
        }
        DatePart::Year => {
            let (y, _, _) = civil_from_days(days);
            days_from_civil(y, 1, 1) * 86_400_000
        }
        DatePart::Dow | DatePart::Doy | DatePart::Epoch => {
            return Err(SqlError::Eval(format!("date_trunc part {part:?}")));
        }
    })
}

#[allow(clippy::too_many_arguments)]
fn eval_aggregate<S: Store>(
    store: &S,
    func: AggFunc,
    arg: Option<&Expr>,
    distinct: bool,
    schema: &[ColRef],
    src: &Sources,
    tuples: &Tuples,
    group: &[u32],
    params: &[Value],
) -> Result<Value> {
    // COUNT(*) counts all rows; COUNT(expr) counts non-null; others fold
    // values. All folds stream — no per-group buffering of evaluated values.
    // DISTINCT folds (COUNT/SUM/AVG) additionally keep a set of seen values;
    // it is a no-op for MIN/MAX.
    if func == AggFunc::Count && arg.is_none() {
        return Ok(Value::Int(group.len() as i64));
    }
    let arg = arg.ok_or_else(|| SqlError::Eval("aggregate requires an argument".into()))?;
    // Correlated subqueries inside the argument re-execute per source row.
    let corr = has_corr(arg);
    let view_of = |i: &u32| View {
        src,
        tuple: tuples.row(*i as usize),
    };
    let mut seen: std::collections::BTreeSet<IndexKey> = std::collections::BTreeSet::new();
    // True when this non-null value should be folded (always without
    // DISTINCT; first occurrence only with it).
    let mut admit = |v: &Value| !distinct || seen.insert(IndexKey(v.clone()));
    match func {
        AggFunc::Count => {
            let mut n: i64 = 0;
            for i in group {
                let v = eval_scalar_corr(store, corr, arg, schema, &view_of(i), params)?;
                if !matches!(v, Value::Null) && admit(&v) {
                    n += 1;
                }
            }
            Ok(Value::Int(n))
        }
        AggFunc::Min | AggFunc::Max => {
            let want = if func == AggFunc::Min {
                Ordering::Less
            } else {
                Ordering::Greater
            };
            let mut best: Option<Value> = None;
            for i in group {
                let v = eval_scalar_corr(store, corr, arg, schema, &view_of(i), params)?;
                if matches!(v, Value::Null) {
                    continue;
                }
                match &best {
                    None => best = Some(v),
                    Some(cur) => {
                        if Value::total_order(&v, cur) == want {
                            best = Some(v);
                        }
                    }
                }
            }
            Ok(best.unwrap_or(Value::Null))
        }
        AggFunc::Sum | AggFunc::Avg => {
            let mut sum = SumAcc::Empty;
            let mut n: i64 = 0;
            for i in group {
                let v = eval_scalar_corr(store, corr, arg, schema, &view_of(i), params)?;
                if matches!(v, Value::Null) || !admit(&v) {
                    continue;
                }
                sum.add(&v)?;
                n += 1;
            }
            if func == AggFunc::Sum {
                Ok(sum.into_sum())
            } else {
                Ok(sum.into_avg(n))
            }
        }
        AggFunc::Mode => {
            // Most frequent non-null value; ties broken by the smallest value
            // (SQL-standard `mode() WITHIN GROUP`). Tally by total order.
            let mut counts: Vec<(Value, i64)> = Vec::new();
            for i in group {
                let v = eval_scalar_corr(store, corr, arg, schema, &view_of(i), params)?;
                if matches!(v, Value::Null) {
                    continue;
                }
                match counts
                    .iter_mut()
                    .find(|(k, _)| Value::total_order(k, &v) == Ordering::Equal)
                {
                    Some((_, c)) => *c += 1,
                    None => counts.push((v, 1)),
                }
            }
            let best = counts.into_iter().reduce(|a, b| {
                match a.1.cmp(&b.1) {
                    Ordering::Greater => a,
                    Ordering::Less => b,
                    // tie → smaller value wins
                    Ordering::Equal => {
                        if Value::total_order(&a.0, &b.0) == Ordering::Greater {
                            b
                        } else {
                            a
                        }
                    }
                }
            });
            Ok(best.map(|(v, _)| v).unwrap_or(Value::Null))
        }
    }
}

/// Streaming SUM accumulator. Starts integer, promotes to exact Decimal when a
/// Decimal is summed (so DECIMAL columns stay lossless), or to float when a
/// Double is involved. A Double always wins over Decimal (float is contagious).
#[derive(Clone)]
enum SumAcc {
    Empty,
    Int(i64),
    Float(f64),
    Dec(Decimal),
}

impl SumAcc {
    fn add(&mut self, v: &Value) -> Result<()> {
        match v {
            Value::Int(i) | Value::Timestamp(i) => {
                let i = *i;
                match self {
                    SumAcc::Empty => *self = SumAcc::Int(i),
                    SumAcc::Int(acc) => *acc = acc.wrapping_add(i),
                    SumAcc::Float(acc) => *acc += i as f64,
                    SumAcc::Dec(acc) => *acc = acc.add(&Decimal::from_i64(i)),
                }
                Ok(())
            }
            Value::Decimal(dv) => {
                match self {
                    SumAcc::Empty => *self = SumAcc::Dec((**dv).clone()),
                    SumAcc::Int(acc) => *self = SumAcc::Dec(Decimal::from_i64(*acc).add(dv)),
                    SumAcc::Dec(acc) => *acc = acc.add(dv),
                    SumAcc::Float(acc) => *acc += dv.to_f64(),
                }
                Ok(())
            }
            Value::Double(d) => {
                let d = *d;
                match self {
                    SumAcc::Empty => *self = SumAcc::Float(d),
                    SumAcc::Int(acc) => *self = SumAcc::Float(*acc as f64 + d),
                    SumAcc::Dec(acc) => *self = SumAcc::Float(acc.to_f64() + d),
                    SumAcc::Float(acc) => *acc += d,
                }
                Ok(())
            }
            other => Err(SqlError::Eval(format!(
                "SUM over non-numeric value {other:?}"
            ))),
        }
    }

    /// The SUM result value for this accumulator (`NULL` when empty).
    fn into_sum(self) -> Value {
        match self {
            SumAcc::Empty => Value::Null,
            SumAcc::Int(i) => Value::Int(i),
            SumAcc::Float(f) => Value::Double(f),
            SumAcc::Dec(d) => Value::Decimal(Box::new(d)),
        }
    }

    /// The AVG result value: sum / `count`. Decimal sums stay Decimal (division
    /// to scale-6, half-up); Int/Double sums yield Double as before.
    fn into_avg(self, count: i64) -> Value {
        match self {
            SumAcc::Empty => Value::Null,
            SumAcc::Int(i) => Value::Double(i as f64 / count as f64),
            SumAcc::Float(f) => Value::Double(f / count as f64),
            SumAcc::Dec(d) => {
                let cnt = Decimal::from_i64(count);
                Value::Decimal(Box::new(
                    d.div(&cnt, d.div_scale(&cnt)).unwrap_or_else(|| d.clone()),
                ))
            }
        }
    }
}

fn has_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Aggregate { .. } => true,
        Expr::Binary { left, right, .. } => has_aggregate(left) || has_aggregate(right),
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } => has_aggregate(expr),
        Expr::In { expr, list, .. } => has_aggregate(expr) || list.iter().any(has_aggregate),
        Expr::InSet { expr, .. } => has_aggregate(expr),
        Expr::Func { args, .. } => args.iter().any(has_aggregate),
        Expr::CorrIn { expr, .. } => has_aggregate(expr),
        _ => false,
    }
}

fn apply_unary(op: UnOp, v: Value) -> Result<Value> {
    match op {
        UnOp::Not => match v {
            Value::Null => Ok(Value::Null),
            Value::Bool(b) => Ok(Value::Bool(!b)),
            other => Err(SqlError::Eval(format!("NOT of non-boolean {other:?}"))),
        },
        UnOp::Neg => match v {
            Value::Null => Ok(Value::Null),
            Value::Int(n) => Ok(Value::Int(-n)),
            Value::Double(f) => Ok(Value::Double(-f)),
            Value::Decimal(d) => Ok(Value::Decimal(Box::new(d.neg()))),
            other => Err(SqlError::Eval(format!("negation of {other:?}"))),
        },
        UnOp::BitNot => match v {
            Value::Null => Ok(Value::Null),
            Value::Int(n) => Ok(Value::Int(!n)),
            Value::Bool(b) => Ok(Value::Bool(!b)),
            other => Err(SqlError::Eval(format!("bitwise NOT of {other:?}"))),
        },
    }
}

fn eval_binary(op: BinOp, l: Value, r: Value) -> Result<Value> {
    match op {
        BinOp::Concat => Ok(match (l, r) {
            (Value::Null, _) | (_, Value::Null) => Value::Null,
            (l, r) => {
                let to_s = |v: Value| -> Result<String> {
                    Ok(match v {
                        Value::Text(s) => s.into(),
                        Value::Int(n) => n.to_string(),
                        Value::Double(f) => f.to_string(),
                        Value::Bool(b) => b.to_string(),
                        Value::Decimal(d) => d.to_string(),
                        other => return Err(SqlError::Eval(format!("|| of {other:?}"))),
                    })
                };
                Value::Text((format!("{}{}", to_s(l)?, to_s(r)?)).into())
            }
        }),
        BinOp::And => Ok(three_valued(l, r, false)),
        BinOp::Or => Ok(three_valued(l, r, true)),
        BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge => {
            if matches!(l, Value::Null) || matches!(r, Value::Null) {
                return Ok(Value::Null);
            }
            match cmp_values(&l, &r) {
                Some(ord) => Ok(Value::Bool(match op {
                    BinOp::Eq => ord == Ordering::Equal,
                    BinOp::Ne => ord != Ordering::Equal,
                    BinOp::Lt => ord == Ordering::Less,
                    BinOp::Le => ord != Ordering::Greater,
                    BinOp::Gt => ord == Ordering::Greater,
                    BinOp::Ge => ord != Ordering::Less,
                    _ => unreachable!(),
                })),
                None => Err(SqlError::Eval(format!("cannot compare {l:?} and {r:?}"))),
            }
        }
        BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod => arithmetic(op, l, r),
        BinOp::BitXor | BinOp::BitAnd | BinOp::BitOr => match (l, r) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Int(a), Value::Int(b)) => Ok(Value::Int(match op {
                BinOp::BitXor => a ^ b,
                BinOp::BitAnd => a & b,
                _ => a | b,
            })),
            (Value::Bool(a), Value::Bool(b)) => Ok(Value::Bool(match op {
                BinOp::BitXor => a != b,
                BinOp::BitAnd => a && b,
                _ => a || b,
            })),
            (a, b) => Err(SqlError::Eval(format!("bitwise op on {a:?} / {b:?}"))),
        },
    }
}

fn three_valued(l: Value, r: Value, is_or: bool) -> Value {
    let lb = as_bool(&l);
    let rb = as_bool(&r);
    if is_or {
        match (lb, rb) {
            (Some(true), _) | (_, Some(true)) => Value::Bool(true),
            (Some(false), Some(false)) => Value::Bool(false),
            _ => Value::Null,
        }
    } else {
        match (lb, rb) {
            (Some(false), _) | (_, Some(false)) => Value::Bool(false),
            (Some(true), Some(true)) => Value::Bool(true),
            _ => Value::Null,
        }
    }
}

fn as_bool(v: &Value) -> Option<bool> {
    match v {
        Value::Bool(b) => Some(*b),
        _ => None,
    }
}

/// A value that participates in exact Decimal arithmetic: a Decimal, or an
/// Int (promoted). Doubles are excluded — they force the float path.
fn as_decimal_operand(v: &Value) -> Option<Decimal> {
    match v {
        Value::Decimal(d) => Some((**d).clone()),
        Value::Int(n) => Some(Decimal::from_i64(*n)),
        _ => None,
    }
}

fn arithmetic(op: BinOp, l: Value, r: Value) -> Result<Value> {
    if matches!(l, Value::Null) || matches!(r, Value::Null) {
        return Ok(Value::Null);
    }
    // `text + text` is concatenation (SQL Server style; EF's default string
    // Add renders as `+`).
    if let (Value::Text(a), Value::Text(b), BinOp::Add) = (&l, &r, op) {
        return Ok(Value::Text((format!("{a}{b}")).into()));
    }
    // Timestamp arithmetic: ts ± <ms> stays a timestamp; ts - ts is the
    // difference in milliseconds. (INTERVAL literals fold to ms integers.)
    match (&l, &r, op) {
        (Value::Timestamp(t), Value::Int(ms), BinOp::Add) => {
            return Ok(Value::Timestamp(t.wrapping_add(*ms)));
        }
        (Value::Timestamp(t), Value::Int(ms), BinOp::Sub) => {
            return Ok(Value::Timestamp(t.wrapping_sub(*ms)));
        }
        (Value::Int(ms), Value::Timestamp(t), BinOp::Add) => {
            return Ok(Value::Timestamp(t.wrapping_add(*ms)));
        }
        (Value::Timestamp(a), Value::Timestamp(b), BinOp::Sub) => {
            return Ok(Value::Int(a.wrapping_sub(*b)));
        }
        // Fractional ms (e.g. an EF `AddDays(0.5)` → `ts + days*86400000.0`)
        // round to the nearest millisecond.
        (Value::Timestamp(t), Value::Double(ms), BinOp::Add) => {
            return Ok(Value::Timestamp(t.wrapping_add(ms.round() as i64)));
        }
        (Value::Timestamp(t), Value::Double(ms), BinOp::Sub) => {
            return Ok(Value::Timestamp(t.wrapping_sub(ms.round() as i64)));
        }
        (Value::Double(ms), Value::Timestamp(t), BinOp::Add) => {
            return Ok(Value::Timestamp(t.wrapping_add(ms.round() as i64)));
        }
        _ => {}
    }
    if let (Value::Int(a), Value::Int(b)) = (&l, &r) {
        let (a, b) = (*a, *b);
        return Ok(Value::Int(match op {
            BinOp::Add => a.wrapping_add(b),
            BinOp::Sub => a.wrapping_sub(b),
            BinOp::Mul => a.wrapping_mul(b),
            BinOp::Div => {
                if b == 0 {
                    return Err(SqlError::Eval("division by zero".into()));
                }
                a / b
            }
            BinOp::Mod => {
                if b == 0 {
                    return Err(SqlError::Eval("division by zero".into()));
                }
                a % b
            }
            _ => unreachable!(),
        }));
    }
    // Exact Decimal path: either operand is a Decimal and neither is a Double
    // (a Double drags the whole expression onto the lossy float path below).
    if (matches!(l, Value::Decimal(_)) || matches!(r, Value::Decimal(_)))
        && let (Some(a), Some(b)) = (as_decimal_operand(&l), as_decimal_operand(&r))
    {
        return Ok(Value::Decimal(Box::new(match op {
            BinOp::Add => a.add(&b),
            BinOp::Sub => a.sub(&b),
            BinOp::Mul => a.mul(&b),
            BinOp::Div => a
                .div(&b, a.div_scale(&b))
                .ok_or_else(|| SqlError::Eval("division by zero".into()))?,
            BinOp::Mod => a
                .rem(&b)
                .ok_or_else(|| SqlError::Eval("division by zero".into()))?,
            _ => unreachable!(),
        })));
    }
    let a = as_f64(&l).ok_or_else(|| SqlError::Eval(format!("non-numeric operand {l:?}")))?;
    let b = as_f64(&r).ok_or_else(|| SqlError::Eval(format!("non-numeric operand {r:?}")))?;
    Ok(Value::Double(match op {
        BinOp::Add => a + b,
        BinOp::Sub => a - b,
        BinOp::Mul => a * b,
        BinOp::Div => {
            if b == 0.0 {
                return Err(SqlError::Eval("division by zero".into()));
            }
            a / b
        }
        BinOp::Mod => {
            if b == 0.0 {
                return Err(SqlError::Eval("division by zero".into()));
            }
            a % b
        }
        _ => unreachable!(),
    }))
}

fn as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Int(n) => Some(*n as f64),
        Value::Double(f) => Some(*f),
        Value::Timestamp(t) => Some(*t as f64),
        Value::Decimal(d) => Some(d.to_f64()),
        _ => None,
    }
}

fn cmp_values(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::Text(x), Value::Text(y)) => Some(x.cmp(y)),
        (Value::Bool(x), Value::Bool(y)) => Some(x.cmp(y)),
        (Value::Bytes(x), Value::Bytes(y)) => Some(x.cmp(y)),
        // Exact Decimal vs Decimal/Int (a Double drops to the f64 path below).
        (Value::Decimal(x), Value::Decimal(y)) => Some(x.cmp(y)),
        (Value::Decimal(x), Value::Int(y)) => Some(x.cmp(&Decimal::from_i64(*y))),
        (Value::Int(x), Value::Decimal(y)) => Some(Decimal::from_i64(*x).cmp(y)),
        _ => {
            let (x, y) = (as_f64(a)?, as_f64(b)?);
            x.partial_cmp(&y)
        }
    }
}
