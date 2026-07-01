//! Execute logical [`Statement`]s against a [`Store`].
//!
//! A tree-walking interpreter over typed rows. SELECT builds a combined row set
//! (base table + nested-loop inner joins), filters it, optionally groups and
//! aggregates, then projects / orders / limits. It is generic over [`Store`], so
//! the identical code runs in autocommit mode (against the engine) and inside a
//! transaction (against the buffered overlay).

use std::cmp::Ordering;
use std::collections::BTreeMap;

use crate::ast::{
    AggFunc, BinOp, Expr, Join, JoinKind, QueryResult, SelectItem, SelectStmt, Statement, TableRef,
    UnOp,
};
use crate::error::{Result, SqlError};
use crate::store::Store;
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
            column,
            if_not_exists,
        } => {
            match store.create_index(&name, &table, &column) {
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
        Statement::Select(select) => exec_select(store, select, params),
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

    let mut affected = 0;
    for row_exprs in rows {
        // VALUES cannot reference columns; evaluate against an empty row.
        let values: Vec<Value> = row_exprs
            .iter()
            .map(|e| eval_scalar(e, &[], &[], params))
            .collect::<Result<_>>()?;

        let cells = match &columns {
            Some(cols) => {
                if cols.len() != values.len() {
                    return Err(SqlError::SchemaMismatch(format!(
                        "INSERT has {} columns but {} values",
                        cols.len(),
                        values.len()
                    )));
                }
                let mut cells = vec![Value::Null; def.arity()];
                for (name, val) in cols.iter().zip(values) {
                    let idx = def
                        .columns
                        .iter()
                        .position(|c| &c.name == name)
                        .ok_or_else(|| SqlError::NoSuchColumn(name.clone()))?;
                    cells[idx] = val;
                }
                cells
            }
            None => values,
        };

        store.insert(table, cells)?;
        affected += 1;
    }
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
            && !truthy(&eval_scalar(pred, &schema, &cells, params)?)
        {
            continue;
        }
        let mut new_cells = cells.clone();
        for (idx, expr) in &targets {
            new_cells[*idx] = eval_scalar(expr, &schema, &cells, params)?;
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
            Some(pred) => truthy(&eval_scalar(pred, &schema, &cells, params)?),
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

fn exec_select<S: Store>(store: &S, select: SelectStmt, params: &[Value]) -> Result<QueryResult> {
    let mut select = select;
    // 1. Build the source: base table, then nested-loop inner joins.
    let (schema, mut rows) = build_source(store, &select, params)?;

    // 2. WHERE.
    if let Some(pred) = &select.filter {
        let mut kept = Vec::with_capacity(rows.len());
        for row in rows {
            if truthy(&eval_scalar(pred, &schema, &row, params)?) {
                kept.push(row);
            }
        }
        rows = kept;
    }

    // 3. Expand the projection into (output name, expr) pairs.
    let proj = expand_projection(&select.projection, &schema)?;

    // Resolve ORDER BY references to projection aliases: a bare column that is
    // not an input column but matches an output name (`... AS spend ... ORDER BY
    // spend`) is rewritten to that projection's expression. Real input columns
    // keep their meaning, so this is backward compatible.
    for (expr, _) in select.order_by.iter_mut() {
        if let Expr::Column { table: None, name } = expr
            && resolve_col(&schema, &None, name).is_err()
            && let Some((_, pe)) = proj.iter().find(|(n, _)| n == name)
        {
            *expr = pe.clone();
        }
    }

    // Bind all column references up front, so unknown/ambiguous columns are
    // caught even when the (post-filter) row set is empty.
    if let Some(f) = &select.filter {
        check_columns(f, &schema)?;
    }
    for (_, e) in &proj {
        check_columns(e, &schema)?;
    }
    for e in &select.group_by {
        check_columns(e, &schema)?;
    }
    if let Some(h) = &select.having {
        check_columns(h, &schema)?;
    }
    for (e, _) in &select.order_by {
        check_columns(e, &schema)?;
    }

    let aggregating = !select.group_by.is_empty()
        || proj.iter().any(|(_, e)| has_aggregate(e))
        || select.having.as_ref().is_some_and(has_aggregate);

    let columns: Vec<String> = proj.iter().map(|(n, _)| n.clone()).collect();

    let out_rows = if aggregating {
        select_aggregated(&schema, rows, &select, &proj, params)?
    } else {
        select_simple(&schema, rows, &select, &proj, params)?
    };

    // LIMIT.
    let out_rows = match select.limit {
        Some(n) => out_rows.into_iter().take(n).collect(),
        None => out_rows,
    };

    Ok(QueryResult::Select {
        columns,
        rows: out_rows,
    })
}

/// Build the combined (schema, rows) for FROM + inner joins.
fn build_source<S: Store>(
    store: &S,
    select: &SelectStmt,
    params: &[Value],
) -> Result<(Vec<ColRef>, Vec<Vec<Value>>)> {
    let base_def = store
        .table_def(&select.from.name)
        .ok_or_else(|| SqlError::NoSuchTable(select.from.name.clone()))?;
    let mut schema = qualified_schema(select.from.key(), &base_def);

    // Base rows: use an index when there are no joins and WHERE has a usable
    // equality on an indexed column; otherwise full scan.
    let mut rows: Vec<Vec<Value>> = if select.joins.is_empty() {
        base_rows(store, &select.from, &select.filter, params)?
    } else {
        store
            .scan(&select.from.name)?
            .into_iter()
            .map(|(_, c)| c)
            .collect()
    };

    for join in &select.joins {
        let (schema, rows_ref) = (&mut schema, &mut rows);
        join_into(store, join, schema, rows_ref, params)?;
    }
    Ok((schema, rows))
}

fn join_into<S: Store>(
    store: &S,
    join: &Join,
    schema: &mut Vec<ColRef>,
    rows: &mut Vec<Vec<Value>>,
    params: &[Value],
) -> Result<()> {
    let def = store
        .table_def(&join.table.name)
        .ok_or_else(|| SqlError::NoSuchTable(join.table.name.clone()))?;
    let right_schema = qualified_schema(join.table.key(), &def);
    let right_rows: Vec<Vec<Value>> = store
        .scan(&join.table.name)?
        .into_iter()
        .map(|(_, c)| c)
        .collect();

    let left_width = schema.len();
    let right_width = right_schema.len();
    let mut combined_schema = schema.clone();
    combined_schema.extend(right_schema);

    let want_left = matches!(join.kind, JoinKind::Left | JoinKind::Full);
    let want_right = matches!(join.kind, JoinKind::Right | JoinKind::Full);

    let mut out = Vec::new();
    // Track which right rows matched at least once (for RIGHT/FULL padding).
    let mut right_matched = vec![false; right_rows.len()];

    for left in rows.iter() {
        let mut left_matched = false;
        for (ri, right) in right_rows.iter().enumerate() {
            let mut combined = left.clone();
            combined.extend(right.clone());
            if truthy(&eval_scalar(&join.on, &combined_schema, &combined, params)?) {
                left_matched = true;
                right_matched[ri] = true;
                out.push(combined);
            }
        }
        // LEFT/FULL: emit an unmatched left row padded with NULLs on the right.
        if want_left && !left_matched {
            let mut combined = left.clone();
            combined.extend(std::iter::repeat_n(Value::Null, right_width));
            out.push(combined);
        }
    }
    // RIGHT/FULL: emit unmatched right rows padded with NULLs on the left.
    if want_right {
        for (ri, right) in right_rows.iter().enumerate() {
            if !right_matched[ri] {
                let mut combined = vec![Value::Null; left_width];
                combined.extend(right.clone());
                out.push(combined);
            }
        }
    }

    *schema = combined_schema;
    *rows = out;
    Ok(())
}

/// Fetch base rows for a single (join-free) table, using an index when possible.
fn base_rows<S: Store>(
    store: &S,
    from: &TableRef,
    filter: &Option<Expr>,
    params: &[Value],
) -> Result<Vec<Vec<Value>>> {
    if let Some(expr) = filter {
        for (col, val) in eq_conjuncts(expr, from.key(), params) {
            if let Some(rows) = store.index_lookup_eq(&from.name, &col, &val)? {
                return Ok(rows.into_iter().map(|(_, c)| c).collect());
            }
        }
    }
    Ok(store
        .scan(&from.name)?
        .into_iter()
        .map(|(_, c)| c)
        .collect())
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

/// Non-aggregated projection: one output row per input row.
fn select_simple(
    schema: &[ColRef],
    rows: Vec<Vec<Value>>,
    select: &SelectStmt,
    proj: &[(String, Expr)],
    params: &[Value],
) -> Result<Vec<Vec<Value>>> {
    // Keep each input row alongside its output for ORDER BY.
    let mut prepared: Vec<(Vec<Value>, Vec<Value>)> = Vec::with_capacity(rows.len());
    for row in rows {
        let out: Vec<Value> = proj
            .iter()
            .map(|(_, e)| eval_scalar(e, schema, &row, params))
            .collect::<Result<_>>()?;
        prepared.push((out, row));
    }

    if !select.order_by.is_empty() {
        let keys = &select.order_by;
        let mut err = None;
        prepared.sort_by(|a, b| cmp_by_order(keys, schema, &a.1, &b.1, params, &mut err));
        if let Some(e) = err {
            return Err(e);
        }
    }

    Ok(prepared.into_iter().map(|(out, _)| out).collect())
}

/// Aggregated projection: group rows, compute aggregates per group.
fn select_aggregated(
    schema: &[ColRef],
    rows: Vec<Vec<Value>>,
    select: &SelectStmt,
    proj: &[(String, Expr)],
    params: &[Value],
) -> Result<Vec<Vec<Value>>> {
    // Group by the group-by key (empty group-by => single group over all rows).
    let groups = group_rows(schema, &rows, &select.group_by, params)?;

    let mut prepared: Vec<(Vec<Value>, Vec<Vec<Value>>)> = Vec::new();
    for (_key, group) in groups {
        if let Some(having) = &select.having
            && !truthy(&eval_agg(having, schema, &group, params)?)
        {
            continue;
        }
        let out: Vec<Value> = proj
            .iter()
            .map(|(_, e)| eval_agg(e, schema, &group, params))
            .collect::<Result<_>>()?;
        prepared.push((out, group));
    }

    if !select.order_by.is_empty() {
        let keys = &select.order_by;
        let mut err = None;
        prepared.sort_by(|a, b| cmp_by_order_agg(keys, schema, &a.1, &b.1, params, &mut err));
        if let Some(e) = err {
            return Err(e);
        }
    }

    Ok(prepared.into_iter().map(|(out, _)| out).collect())
}

/// Group input rows by the evaluated group-by key.
#[allow(clippy::type_complexity)]
fn group_rows(
    schema: &[ColRef],
    rows: &[Vec<Value>],
    group_by: &[Expr],
    params: &[Value],
) -> Result<Vec<(Vec<Value>, Vec<Vec<Value>>)>> {
    if group_by.is_empty() {
        // One group over everything (present even when there are no rows).
        return Ok(vec![(Vec::new(), rows.to_vec())]);
    }
    // Preserve first-seen group order via an index map.
    let mut order: Vec<Vec<IndexKey>> = Vec::new();
    let mut map: BTreeMap<Vec<IndexKey>, Vec<Vec<Value>>> = BTreeMap::new();
    for row in rows {
        let key: Vec<IndexKey> = group_by
            .iter()
            .map(|e| Ok(IndexKey(eval_scalar(e, schema, row, params)?)))
            .collect::<Result<_>>()?;
        if !map.contains_key(&key) {
            order.push(key.clone());
        }
        map.entry(key).or_default().push(row.clone());
    }
    Ok(order
        .into_iter()
        .map(|k| {
            let rows = map.remove(&k).unwrap();
            let key = k.into_iter().map(|ik| ik.0).collect();
            (key, rows)
        })
        .collect())
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

fn cmp_by_order(
    keys: &[(Expr, bool)],
    schema: &[ColRef],
    a: &[Value],
    b: &[Value],
    params: &[Value],
    err: &mut Option<SqlError>,
) -> Ordering {
    for (expr, asc) in keys {
        let (va, vb) = match (
            eval_scalar(expr, schema, a, params),
            eval_scalar(expr, schema, b, params),
        ) {
            (Ok(va), Ok(vb)) => (va, vb),
            (Err(e), _) | (_, Err(e)) => {
                *err = Some(e);
                return Ordering::Equal;
            }
        };
        let ord = Value::total_order(&va, &vb);
        let ord = if *asc { ord } else { ord.reverse() };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

fn cmp_by_order_agg(
    keys: &[(Expr, bool)],
    schema: &[ColRef],
    a: &[Vec<Value>],
    b: &[Vec<Value>],
    params: &[Value],
    err: &mut Option<SqlError>,
) -> Ordering {
    for (expr, asc) in keys {
        let (va, vb) = match (
            eval_agg(expr, schema, a, params),
            eval_agg(expr, schema, b, params),
        ) {
            (Ok(va), Ok(vb)) => (va, vb),
            (Err(e), _) | (_, Err(e)) => {
                *err = Some(e);
                return Ordering::Equal;
            }
        };
        let ord = Value::total_order(&va, &vb);
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

/// Verify that every column reference in `expr` resolves against `schema`.
fn check_columns(expr: &Expr, schema: &[ColRef]) -> Result<()> {
    match expr {
        Expr::Column { table, name } => {
            resolve_col(schema, table, name)?;
            Ok(())
        }
        Expr::Binary { left, right, .. } => {
            check_columns(left, schema)?;
            check_columns(right, schema)
        }
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } => check_columns(expr, schema),
        Expr::Aggregate { arg, .. } => {
            if let Some(a) = arg {
                check_columns(a, schema)?;
            }
            Ok(())
        }
        Expr::Literal(_) | Expr::Param(_) => Ok(()),
    }
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

/// Evaluate a scalar (non-aggregate) expression over a single row.
fn eval_scalar(expr: &Expr, schema: &[ColRef], row: &[Value], params: &[Value]) -> Result<Value> {
    match expr {
        Expr::Literal(v) => Ok(v.clone()),
        Expr::Param(i) => params
            .get(*i)
            .cloned()
            .ok_or_else(|| SqlError::Eval(format!("missing bind parameter ${}", i + 1))),
        Expr::Column { table, name } => {
            let idx = resolve_col(schema, table, name)?;
            row.get(idx)
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
        Expr::Aggregate { .. } => Err(SqlError::Eval(
            "aggregate function used outside an aggregated query".into(),
        )),
    }
}

/// Evaluate an expression that may contain aggregates, over a group of rows.
/// Non-aggregate leaves are evaluated on the group's first row (as SQL requires
/// them to be group keys).
fn eval_agg(
    expr: &Expr,
    schema: &[ColRef],
    group: &[Vec<Value>],
    params: &[Value],
) -> Result<Value> {
    match expr {
        Expr::Aggregate { func, arg } => {
            eval_aggregate(*func, arg.as_deref(), schema, group, params)
        }
        Expr::Literal(v) => Ok(v.clone()),
        Expr::Param(i) => params
            .get(*i)
            .cloned()
            .ok_or_else(|| SqlError::Eval(format!("missing bind parameter ${}", i + 1))),
        Expr::Column { table, name } => {
            // A grouped column: same across the group; read from the first row.
            let idx = resolve_col(schema, table, name)?;
            match group.first() {
                Some(row) => row
                    .get(idx)
                    .cloned()
                    .ok_or_else(|| SqlError::Eval(format!("column {name:?} out of range"))),
                None => Ok(Value::Null),
            }
        }
        Expr::IsNull { expr, negated } => {
            let v = eval_agg(expr, schema, group, params)?;
            Ok(Value::Bool(matches!(v, Value::Null) != *negated))
        }
        Expr::Unary { op, expr } => {
            let v = eval_agg(expr, schema, group, params)?;
            apply_unary(*op, v)
        }
        Expr::Binary { op, left, right } => {
            let l = eval_agg(left, schema, group, params)?;
            let r = eval_agg(right, schema, group, params)?;
            eval_binary(*op, l, r)
        }
    }
}

fn eval_aggregate(
    func: AggFunc,
    arg: Option<&Expr>,
    schema: &[ColRef],
    group: &[Vec<Value>],
    params: &[Value],
) -> Result<Value> {
    // COUNT(*) counts all rows; COUNT(expr) counts non-null; others fold values.
    if func == AggFunc::Count && arg.is_none() {
        return Ok(Value::Int(group.len() as i64));
    }
    let arg = arg.ok_or_else(|| SqlError::Eval("aggregate requires an argument".into()))?;
    let mut values = Vec::new();
    for row in group {
        let v = eval_scalar(arg, schema, row, params)?;
        if !matches!(v, Value::Null) {
            values.push(v);
        }
    }
    match func {
        AggFunc::Count => Ok(Value::Int(values.len() as i64)),
        AggFunc::Min => Ok(fold_extreme(&values, Ordering::Less)),
        AggFunc::Max => Ok(fold_extreme(&values, Ordering::Greater)),
        AggFunc::Sum => Ok(fold_sum(&values)?),
        AggFunc::Avg => {
            if values.is_empty() {
                return Ok(Value::Null);
            }
            let sum = fold_sum(&values)?;
            let n = values.len() as f64;
            let s = match sum {
                Value::Int(i) => i as f64,
                Value::Double(d) => d,
                _ => return Err(SqlError::Eval("AVG over non-numeric values".into())),
            };
            Ok(Value::Double(s / n))
        }
    }
}

fn fold_extreme(values: &[Value], want: Ordering) -> Value {
    let mut best: Option<&Value> = None;
    for v in values {
        match best {
            None => best = Some(v),
            Some(cur) => {
                if Value::total_order(v, cur) == want {
                    best = Some(v);
                }
            }
        }
    }
    best.cloned().unwrap_or(Value::Null)
}

fn fold_sum(values: &[Value]) -> Result<Value> {
    if values.is_empty() {
        return Ok(Value::Null);
    }
    let any_float = values.iter().any(|v| matches!(v, Value::Double(_)));
    if any_float {
        let mut acc = 0.0;
        for v in values {
            acc += match v {
                Value::Int(i) => *i as f64,
                Value::Double(d) => *d,
                Value::Timestamp(t) => *t as f64,
                _ => return Err(SqlError::Eval("SUM over non-numeric values".into())),
            };
        }
        Ok(Value::Double(acc))
    } else {
        let mut acc: i64 = 0;
        for v in values {
            acc = acc.wrapping_add(match v {
                Value::Int(i) => *i,
                Value::Timestamp(t) => *t,
                _ => return Err(SqlError::Eval("SUM over non-numeric values".into())),
            });
        }
        Ok(Value::Int(acc))
    }
}

fn has_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Aggregate { .. } => true,
        Expr::Binary { left, right, .. } => has_aggregate(left) || has_aggregate(right),
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } => has_aggregate(expr),
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
