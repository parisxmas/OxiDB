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
    AggFunc, BinOp, Expr, Join, JoinKind, QueryBody, QueryResult, SelectItem, SelectQuery,
    SelectStmt, Statement, TableRef, UnOp,
};
use crate::error::{Result, SqlError};
use crate::store::{Chunk, Store};
use crate::types::{IndexKey, Value};

/// A column in a working row set, qualified by its (aliased) table name.
#[derive(Debug, Clone)]
struct ColRef {
    table: String,
    name: String,
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
        Statement::Insert {
            table,
            columns,
            rows,
        } => exec_insert(store, &table, columns, rows, params),
        Statement::Select(query) => exec_query(store, query, params),
        Statement::Update {
            table,
            assignments,
            filter,
        } => exec_update(store, &table, assignments, filter, params),
        Statement::Delete { table, filter } => exec_delete(store, &table, filter, params),
        Statement::Begin | Statement::Commit | Statement::Rollback => Err(SqlError::Unsupported(
            "transaction control must be a top-level statement".into(),
        )),
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

fn exec_insert<S: Store>(
    store: &S,
    table: &str,
    columns: Option<Vec<String>>,
    rows: Vec<Vec<Expr>>,
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
                let mut cells = vec![Value::Null; def.arity()];
                for (idx, val) in idxs.iter().zip(values) {
                    cells[*idx] = val;
                }
                cells
            }
            None => values,
        };
        all_cells.push(cells);
    }

    // One durable batch: all rows of a multi-row INSERT share a single fsync.
    let affected = store.insert_many(table, all_cells)? as usize;
    Ok(QueryResult::Mutation { affected })
}

fn exec_update<S: Store>(
    store: &S,
    table: &str,
    assignments: Vec<(String, Expr)>,
    filter: Option<Expr>,
    params: &[Value],
) -> Result<QueryResult> {
    let def = store
        .table_def(table)
        .ok_or_else(|| SqlError::NoSuchTable(table.to_string()))?;
    let schema = table_schema(table, &def);
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

    let mut affected = 0;
    for (row_id, cells) in store.scan(table)? {
        if let Some(pred) = &filter
            && !truthy(&eval_scalar(pred, &schema, cells.as_slice(), params)?)
        {
            continue;
        }
        let mut new_cells = cells.clone();
        for (idx, expr) in &targets {
            new_cells[*idx] = eval_scalar(expr, &schema, cells.as_slice(), params)?;
        }
        store.update_row(table, row_id, new_cells)?;
        affected += 1;
    }
    Ok(QueryResult::Mutation { affected })
}

fn exec_delete<S: Store>(
    store: &S,
    table: &str,
    filter: Option<Expr>,
    params: &[Value],
) -> Result<QueryResult> {
    let def = store
        .table_def(table)
        .ok_or_else(|| SqlError::NoSuchTable(table.to_string()))?;
    let schema = table_schema(table, &def);

    let mut to_delete = Vec::new();
    for (row_id, cells) in store.scan(table)? {
        let matches = match &filter {
            Some(pred) => truthy(&eval_scalar(pred, &schema, cells.as_slice(), params)?),
            None => true,
        };
        if matches {
            to_delete.push(row_id);
        }
    }
    let mut affected = 0;
    for row_id in to_delete {
        if store.delete(table, row_id)? {
            affected += 1;
        }
    }
    Ok(QueryResult::Mutation { affected })
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

/// Execute a full query: the single-select fast path, or a set-operation tree
/// with outer ORDER BY / LIMIT / OFFSET applied to the combined result.
fn exec_query<S: Store>(store: &S, query: SelectQuery, params: &[Value]) -> Result<QueryResult> {
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
                .skip(query.offset.unwrap_or(0))
                .take(query.limit.unwrap_or(usize::MAX))
                .collect();
            Ok(QueryResult::Select { columns, rows })
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
            "UNION ORDER BY must be an output column name or 1-based position".into(),
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
            QueryResult::Select { columns, rows } => Ok((columns, rows)),
            _ => unreachable!("SELECT produced a non-select result"),
        },
        QueryBody::SetOp { all, left, right } => {
            let (columns, mut rows) = exec_body(store, *left, params)?;
            let (rcols, rrows) = exec_body(store, *right, params)?;
            if columns.len() != rcols.len() {
                return Err(SqlError::SchemaMismatch(format!(
                    "UNION arms have {} and {} columns",
                    columns.len(),
                    rcols.len()
                )));
            }
            rows.extend(rrows);
            if !all {
                // UNION (distinct): keep the first occurrence of each row.
                let mut seen: std::collections::BTreeSet<Vec<IndexKey>> =
                    std::collections::BTreeSet::new();
                rows.retain(|row| {
                    seen.insert(row.iter().cloned().map(IndexKey).collect::<Vec<_>>())
                });
            }
            Ok((columns, rows))
        }
    }
}

// ── subquery resolution ─────────────────────────────────────────────────────
//
// Uncorrelated subqueries are executed once, before row evaluation, and
// replaced with literals: a scalar subquery becomes `Literal` (NULL when it
// returns no row), `IN (SELECT ...)` becomes a literal `In` list. Column
// references inside a subquery resolve only against the subquery's own tables
// (correlated subqueries are not supported and fail binding there).

fn resolve_subqueries_stmt<S: Store>(
    store: &S,
    stmt: &mut Statement,
    params: &[Value],
) -> Result<()> {
    match stmt {
        Statement::Insert { rows, .. } => {
            for row in rows {
                for e in row {
                    resolve_expr(store, e, params)?;
                }
            }
            Ok(())
        }
        Statement::Select(q) => resolve_query(store, q, params),
        Statement::Update {
            assignments,
            filter,
            ..
        } => {
            for (_, e) in assignments {
                resolve_expr(store, e, params)?;
            }
            if let Some(f) = filter {
                resolve_expr(store, f, params)?;
            }
            Ok(())
        }
        Statement::Delete { filter, .. } => {
            if let Some(f) = filter {
                resolve_expr(store, f, params)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn resolve_query<S: Store>(store: &S, q: &mut SelectQuery, params: &[Value]) -> Result<()> {
    resolve_body(store, &mut q.body, params)
}

fn resolve_body<S: Store>(store: &S, body: &mut QueryBody, params: &[Value]) -> Result<()> {
    match body {
        QueryBody::Select(s) => resolve_select(store, s, params),
        QueryBody::SetOp { left, right, .. } => {
            resolve_body(store, left, params)?;
            resolve_body(store, right, params)
        }
    }
}

fn resolve_select<S: Store>(store: &S, s: &mut SelectStmt, params: &[Value]) -> Result<()> {
    for item in &mut s.projection {
        if let SelectItem::Expr { expr, .. } = item {
            resolve_expr(store, expr, params)?;
        }
    }
    if let Some(f) = &mut s.filter {
        resolve_expr(store, f, params)?;
    }
    for j in &mut s.joins {
        resolve_expr(store, &mut j.on, params)?;
    }
    for e in &mut s.group_by {
        resolve_expr(store, e, params)?;
    }
    if let Some(h) = &mut s.having {
        resolve_expr(store, h, params)?;
    }
    for (e, _) in &mut s.order_by {
        resolve_expr(store, e, params)?;
    }
    Ok(())
}

fn resolve_expr<S: Store>(store: &S, e: &mut Expr, params: &[Value]) -> Result<()> {
    match e {
        Expr::Subquery(q) => {
            let (columns, mut rows) = exec_subquery(store, q, params)?;
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
            Ok(())
        }
        Expr::InSubquery {
            expr,
            query,
            negated,
        } => {
            resolve_expr(store, expr, params)?;
            let (columns, rows) = exec_subquery(store, query, params)?;
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
            Ok(())
        }
        Expr::In { expr, list, .. } => {
            resolve_expr(store, expr, params)?;
            for item in list {
                resolve_expr(store, item, params)?;
            }
            Ok(())
        }
        Expr::Binary { left, right, .. } => {
            resolve_expr(store, left, params)?;
            resolve_expr(store, right, params)
        }
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } => resolve_expr(store, expr, params),
        Expr::Aggregate { arg, .. } => match arg {
            Some(a) => resolve_expr(store, a, params),
            None => Ok(()),
        },
        Expr::Column { .. } | Expr::Col(_) | Expr::Literal(_) | Expr::Param(_) => Ok(()),
    }
}

/// Execute a subquery (resolving its own nested subqueries first) and return
/// its columns + rows.
fn exec_subquery<S: Store>(
    store: &S,
    q: &mut SelectQuery,
    params: &[Value],
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    resolve_query(store, q, params)?;
    match exec_query(store, q.clone(), params)? {
        QueryResult::Select { columns, rows } => Ok((columns, rows)),
        _ => unreachable!("subquery produced a non-select result"),
    }
}

fn exec_select<S: Store>(store: &S, select: SelectStmt, params: &[Value]) -> Result<QueryResult> {
    let mut select = select;
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
        let mut kept = Vec::with_capacity(tuples.data.len());
        for i in 0..tuples.n() {
            let tuple = tuples.row(i);
            let view = View { src: &src, tuple };
            if truthy(&eval_scalar(pred, &schema, &view, params)?) {
                kept.extend_from_slice(tuple);
            }
        }
        tuples.data = kept;
    }

    let out_rows = if aggregating {
        select_aggregated(&schema, &src, &tuples, &select, &proj, params)?
    } else {
        select_simple(&schema, &src, &tuples, &select, &proj, params)?
    };

    // OFFSET / LIMIT.
    let out_rows: Vec<Vec<Value>> = out_rows
        .into_iter()
        .skip(select.offset.unwrap_or(0))
        .take(select.limit.unwrap_or(usize::MAX))
        .collect();

    Ok(QueryResult::Select {
        columns,
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

/// Build the combined (schema, sources, tuples) for FROM + joins.
fn build_source<S: Store>(
    store: &S,
    select: &SelectStmt,
    params: &[Value],
) -> Result<(Vec<ColRef>, Sources, Tuples)> {
    let base_def = store
        .table_def(&select.from.name)
        .ok_or_else(|| SqlError::NoSuchTable(select.from.name.clone()))?;
    let needed = collect_needed(select);
    let full = qualified_schema(select.from.key(), &base_def);
    let keep = keep_indices(&full, select.from.key(), &needed);
    let mut schema: Vec<ColRef> = keep.iter().map(|&i| full[i].clone()).collect();

    // Base rows: use an index when there are no joins and WHERE has a usable
    // equality on an indexed column; otherwise full scan.
    let chunk = if select.joins.is_empty() {
        base_chunk(store, &select.from, &select.filter, params, &keep)?
    } else {
        store.scan_pruned(&select.from.name, &keep)?
    };

    let mut src = Sources::default();
    let n = chunk.n;
    src.push_table(chunk);
    let mut tuples = Tuples {
        stride: 1,
        data: (0..n as u32).collect(),
    };

    for join in &select.joins {
        join_into(
            store,
            join,
            &mut schema,
            &mut src,
            &mut tuples,
            params,
            &needed,
        )?;
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
fn join_into<S: Store>(
    store: &S,
    join: &Join,
    schema: &mut Vec<ColRef>,
    src: &mut Sources,
    tuples: &mut Tuples,
    params: &[Value],
    needed: &Needed,
) -> Result<()> {
    let def = store
        .table_def(&join.table.name)
        .ok_or_else(|| SqlError::NoSuchTable(join.table.name.clone()))?;
    let full = qualified_schema(join.table.key(), &def);
    let keep = keep_indices(&full, join.table.key(), needed);
    let right_schema: Vec<ColRef> = keep.iter().map(|&i| full[i].clone()).collect();
    let chunk = store.scan_pruned(&join.table.name, &keep)?;
    let nright = chunk.n;

    let left_len = schema.len();
    let mut combined = schema.clone();
    combined.extend(right_schema.iter().cloned());

    // Split the raw (named) ON into equi-key pairs and a residual predicate.
    let (keys, residual) = split_on(&join.on, left_len, &combined);
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

        for lt in tuples.data.chunks_exact(stride) {
            cand[..stride].copy_from_slice(lt);
            let mut left_matched = false;
            let lview = View {
                src,
                tuple: &cand[..stride],
            };
            let mut chain = index.probe(&left_keys, schema, &lview, params)?;
            while chain != CHAIN_END {
                let ri = chain;
                chain = index.next(ri);
                cand[stride] = ri;
                let keep = match &residual {
                    None => true,
                    Some(res) => {
                        let view = View { src, tuple: &cand };
                        truthy(&eval_scalar(res, &combined, &view, params)?)
                    }
                };
                if keep {
                    left_matched = true;
                    right_matched[ri as usize] = true;
                    out.extend_from_slice(&cand);
                }
            }
            if want_left && !left_matched {
                cand[stride] = NULL_ROW;
                out.extend_from_slice(&cand);
            }
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
        Value::Int(n) => Some(HashKey::Num(norm(*n as f64))),
        // NaN = NaN is not true in SQL, so a NaN key can never equi-match —
        // exclude it (like NULL) rather than let bit-equality pair two NaNs.
        Value::Double(f) if f.is_nan() => None,
        Value::Double(f) => Some(HashKey::Num(norm(*f))),
        Value::Timestamp(t) => Some(HashKey::Num(norm(*t as f64))),
        Value::Bool(b) => Some(HashKey::Bool(*b)),
        Value::Text(s) => Some(HashKey::Text(s.clone())),
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

impl RightIndex {
    fn build(
        right_keys: &[Expr],
        right_schema: &[ColRef],
        right: &Chunk,
        params: &[Value],
    ) -> Result<RightIndex> {
        let nright = right.n;

        // Single-component key: check for the dense-int case first.
        if let [key_expr] = right_keys {
            let mut vals: Vec<Option<i64>> = Vec::with_capacity(nright);
            let mut all_int = true;
            let (mut min, mut max) = (i64::MAX, i64::MIN);
            for ri in 0..nright {
                let v = eval_scalar(key_expr, right_schema, right.row(ri), params)?;
                if matches!(v, Value::Null) {
                    vals.push(None);
                    continue;
                }
                match int_key(&v) {
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
        Expr::In { expr, list, .. } => {
            collect_col_refs(expr, out);
            for item in list {
                collect_col_refs(item, out);
            }
        }
        // Subqueries resolve to literals before any column analysis; their
        // inner column references belong to the subquery's own scope.
        Expr::Subquery(_) | Expr::InSubquery { .. } => {}
        // `Col` only appears after binding; equi-key detection runs before that.
        Expr::Col(_) | Expr::Literal(_) | Expr::Param(_) => {}
    }
}

/// Fetch base rows for a single (join-free) table as a pruned chunk, using an
/// index when possible.
fn base_chunk<S: Store>(
    store: &S,
    from: &TableRef,
    filter: &Option<Expr>,
    params: &[Value],
    keep: &[usize],
) -> Result<Chunk> {
    if let Some(expr) = filter {
        let eqs = eq_conjuncts(expr, from.key(), params);
        if !eqs.is_empty()
            && let Some(rows) = store.index_lookup_eq(&from.name, &eqs)?
        {
            return Ok(Chunk::from_rows(rows.into_iter().map(|(_, c)| c), keep));
        }
    }
    store.scan_pruned(&from.name, keep)
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
        _ => None,
    }
}

/// Non-aggregated projection: one output row per input tuple.
fn select_simple(
    schema: &[ColRef],
    src: &Sources,
    tuples: &Tuples,
    select: &SelectStmt,
    proj: &[(String, Expr)],
    params: &[Value],
) -> Result<Vec<Vec<Value>>> {
    let n = tuples.n();
    if select.order_by.is_empty() {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let view = View {
                src,
                tuple: tuples.row(i),
            };
            let row: Vec<Value> = proj
                .iter()
                .map(|(_, e)| eval_scalar(e, schema, &view, params))
                .collect::<Result<_>>()?;
            out.push(row);
        }
        return Ok(out);
    }

    // Evaluate the sort keys once per row (not per comparison), then sort.
    let mut keyed: Vec<(Vec<Value>, Vec<Value>)> = Vec::with_capacity(n);
    for i in 0..n {
        let view = View {
            src,
            tuple: tuples.row(i),
        };
        let keys: Vec<Value> = select
            .order_by
            .iter()
            .map(|(e, _)| eval_scalar(e, schema, &view, params))
            .collect::<Result<_>>()?;
        let row: Vec<Value> = proj
            .iter()
            .map(|(_, e)| eval_scalar(e, schema, &view, params))
            .collect::<Result<_>>()?;
        keyed.push((keys, row));
    }
    keyed.sort_by(|a, b| cmp_keys(&select.order_by, &a.0, &b.0));
    Ok(keyed.into_iter().map(|(_, row)| row).collect())
}

/// Aggregated projection: group tuples, compute aggregates per group.
fn select_aggregated(
    schema: &[ColRef],
    src: &Sources,
    tuples: &Tuples,
    select: &SelectStmt,
    proj: &[(String, Expr)],
    params: &[Value],
) -> Result<Vec<Vec<Value>>> {
    // Group by the group-by key (empty group-by => single group over all rows).
    let groups = group_tuples(schema, src, tuples, &select.group_by, params)?;

    let mut prepared: Vec<(Vec<Value>, Vec<Value>)> = Vec::with_capacity(groups.len());
    for group in &groups {
        if let Some(having) = &select.having
            && !truthy(&eval_agg(having, schema, src, tuples, group, params)?)
        {
            continue;
        }
        let out: Vec<Value> = proj
            .iter()
            .map(|(_, e)| eval_agg(e, schema, src, tuples, group, params))
            .collect::<Result<_>>()?;
        // Evaluate the sort keys once per group (not per comparison).
        let keys: Vec<Value> = select
            .order_by
            .iter()
            .map(|(e, _)| eval_agg(e, schema, src, tuples, group, params))
            .collect::<Result<_>>()?;
        prepared.push((keys, out));
    }

    if !select.order_by.is_empty() {
        prepared.sort_by(|a, b| cmp_keys(&select.order_by, &a.0, &b.0));
    }
    Ok(prepared.into_iter().map(|(_, out)| out).collect())
}

/// Group tuple indices by the evaluated group-by key, preserving first-seen
/// group order. Streams over the tuples once; group keys are only cloned when
/// a new group is created.
fn group_tuples(
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
                _ => scratch.push(Cow::Owned(eval_scalar(e, schema, &view, params)?)),
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
        Value::Int(_) | Value::Double(_) | Value::Timestamp(_) => {
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

fn default_name(expr: &Expr) -> String {
    match expr {
        Expr::Column { name, .. } => name.clone(),
        Expr::Aggregate { func, .. } => match func {
            AggFunc::Count => "count",
            AggFunc::Sum => "sum",
            AggFunc::Avg => "avg",
            AggFunc::Min => "min",
            AggFunc::Max => "max",
        }
        .to_string(),
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
            let l = eval_scalar(left, schema, row, params)?;
            let r = eval_scalar(right, schema, row, params)?;
            eval_binary(*op, l, r)
        }
        Expr::In {
            expr,
            list,
            negated,
        } => {
            let v = eval_scalar(expr, schema, row, params)?;
            eval_in(&v, list, *negated, |item| {
                eval_scalar(item, schema, row, params)
            })
        }
        Expr::Aggregate { .. } => Err(SqlError::Eval(
            "aggregate function used outside an aggregated query".into(),
        )),
        Expr::Subquery(_) | Expr::InSubquery { .. } => Err(SqlError::Eval(
            "internal: unresolved subquery reached evaluation".into(),
        )),
    }
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
    Ok(match expr {
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
        Expr::Aggregate { func, arg } => Expr::Aggregate {
            func: *func,
            arg: match arg {
                Some(a) => Some(Box::new(bind_expr(a, schema)?)),
                None => None,
            },
        },
        Expr::In {
            expr,
            list,
            negated,
        } => Expr::In {
            expr: Box::new(bind_expr(expr, schema)?),
            list: list
                .iter()
                .map(|e| bind_expr(e, schema))
                .collect::<Result<_>>()?,
            negated: *negated,
        },
        Expr::Subquery(_) | Expr::InSubquery { .. } => {
            return Err(SqlError::Eval(
                "internal: unresolved subquery reached binding".into(),
            ));
        }
    })
}

/// Evaluate an expression that may contain aggregates, over a group of tuple
/// indices. Non-aggregate leaves are evaluated on the group's first row (as
/// SQL requires them to be group keys).
fn eval_agg(
    expr: &Expr,
    schema: &[ColRef],
    src: &Sources,
    tuples: &Tuples,
    group: &[u32],
    params: &[Value],
) -> Result<Value> {
    match expr {
        Expr::Aggregate { func, arg } => {
            eval_aggregate(*func, arg.as_deref(), schema, src, tuples, group, params)
        }
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
            let v = eval_agg(expr, schema, src, tuples, group, params)?;
            Ok(Value::Bool(matches!(v, Value::Null) != *negated))
        }
        Expr::Unary { op, expr } => {
            let v = eval_agg(expr, schema, src, tuples, group, params)?;
            apply_unary(*op, v)
        }
        Expr::Binary { op, left, right } => {
            let l = eval_agg(left, schema, src, tuples, group, params)?;
            let r = eval_agg(right, schema, src, tuples, group, params)?;
            eval_binary(*op, l, r)
        }
        Expr::In {
            expr,
            list,
            negated,
        } => {
            let v = eval_agg(expr, schema, src, tuples, group, params)?;
            eval_in(&v, list, *negated, |item| {
                eval_agg(item, schema, src, tuples, group, params)
            })
        }
        Expr::Subquery(_) | Expr::InSubquery { .. } => Err(SqlError::Eval(
            "internal: unresolved subquery reached evaluation".into(),
        )),
    }
}

fn eval_aggregate(
    func: AggFunc,
    arg: Option<&Expr>,
    schema: &[ColRef],
    src: &Sources,
    tuples: &Tuples,
    group: &[u32],
    params: &[Value],
) -> Result<Value> {
    // COUNT(*) counts all rows; COUNT(expr) counts non-null; others fold
    // values. All folds stream — no per-group buffering of evaluated values.
    if func == AggFunc::Count && arg.is_none() {
        return Ok(Value::Int(group.len() as i64));
    }
    let arg = arg.ok_or_else(|| SqlError::Eval("aggregate requires an argument".into()))?;
    let view_of = |i: &u32| View {
        src,
        tuple: tuples.row(*i as usize),
    };
    match func {
        AggFunc::Count => {
            let mut n: i64 = 0;
            for i in group {
                if !matches!(eval_scalar(arg, schema, &view_of(i), params)?, Value::Null) {
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
                let v = eval_scalar(arg, schema, &view_of(i), params)?;
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
                let v = eval_scalar(arg, schema, &view_of(i), params)?;
                if matches!(v, Value::Null) {
                    continue;
                }
                sum.add(&v)?;
                n += 1;
            }
            let sum = match sum {
                SumAcc::Empty => return Ok(Value::Null),
                SumAcc::Int(i) => Value::Int(i),
                SumAcc::Float(f) => Value::Double(f),
            };
            if func == AggFunc::Sum {
                return Ok(sum);
            }
            let s = match sum {
                Value::Int(i) => i as f64,
                Value::Double(d) => d,
                _ => unreachable!(),
            };
            Ok(Value::Double(s / n as f64))
        }
    }
}

/// Streaming SUM accumulator: integer until the first Double, then float
/// (matching SQL's numeric widening; Int sums use wrapping arithmetic as
/// before).
enum SumAcc {
    Empty,
    Int(i64),
    Float(f64),
}

impl SumAcc {
    fn add(&mut self, v: &Value) -> Result<()> {
        let vf = match v {
            Value::Int(i) => {
                match self {
                    SumAcc::Empty => *self = SumAcc::Int(*i),
                    SumAcc::Int(acc) => *acc = acc.wrapping_add(*i),
                    SumAcc::Float(acc) => *acc += *i as f64,
                }
                return Ok(());
            }
            Value::Timestamp(t) => {
                match self {
                    SumAcc::Empty => *self = SumAcc::Int(*t),
                    SumAcc::Int(acc) => *acc = acc.wrapping_add(*t),
                    SumAcc::Float(acc) => *acc += *t as f64,
                }
                return Ok(());
            }
            Value::Double(d) => *d,
            other => {
                return Err(SqlError::Eval(format!(
                    "SUM over non-numeric value {other:?}"
                )));
            }
        };
        match self {
            SumAcc::Empty => *self = SumAcc::Float(vf),
            SumAcc::Int(acc) => *self = SumAcc::Float(*acc as f64 + vf),
            SumAcc::Float(acc) => *acc += vf,
        }
        Ok(())
    }
}

fn has_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Aggregate { .. } => true,
        Expr::Binary { left, right, .. } => has_aggregate(left) || has_aggregate(right),
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } => has_aggregate(expr),
        Expr::In { expr, list, .. } => has_aggregate(expr) || list.iter().any(has_aggregate),
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
            other => Err(SqlError::Eval(format!("negation of {other:?}"))),
        },
    }
}

fn eval_binary(op: BinOp, l: Value, r: Value) -> Result<Value> {
    match op {
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
        BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div => arithmetic(op, l, r),
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

fn arithmetic(op: BinOp, l: Value, r: Value) -> Result<Value> {
    if matches!(l, Value::Null) || matches!(r, Value::Null) {
        return Ok(Value::Null);
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
            _ => unreachable!(),
        }));
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
        _ => unreachable!(),
    }))
}

fn as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Int(n) => Some(*n as f64),
        Value::Double(f) => Some(*f),
        Value::Timestamp(t) => Some(*t as f64),
        _ => None,
    }
}

fn cmp_values(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::Text(x), Value::Text(y)) => Some(x.cmp(y)),
        (Value::Bool(x), Value::Bool(y)) => Some(x.cmp(y)),
        _ => {
            let (x, y) = (as_f64(a)?, as_f64(b)?);
            x.partial_cmp(&y)
        }
    }
}
