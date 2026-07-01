//! Execute logical [`Statement`]s against a [`SqlEngine`].
//!
//! The executor is a thin tree-walking interpreter: it drives the engine's
//! public row API (`scan`/`insert`/`delete`/`update_row`) and evaluates
//! expressions over typed rows. It never holds the engine lock itself, so every
//! call goes through the engine's normal, individually-locked operations.

use std::cmp::Ordering;

use crate::SqlEngine;
use crate::ast::{BinOp, Expr, Projection, QueryResult, SelectStmt, Statement, UnOp};
use crate::catalog::Table;
use crate::error::{Result, SqlError};
use crate::types::Value;

/// Execute one statement, returning its result.
pub fn execute(engine: &SqlEngine, stmt: Statement) -> Result<QueryResult> {
    match stmt {
        Statement::CreateTable {
            table,
            if_not_exists,
        } => {
            match engine.create_table(table) {
                Ok(()) => {}
                Err(SqlError::TableExists(_)) if if_not_exists => {}
                Err(e) => return Err(e),
            }
            Ok(QueryResult::Ddl)
        }
        Statement::DropTable { name, if_exists } => {
            match engine.drop_table(&name) {
                Ok(()) => {}
                Err(SqlError::NoSuchTable(_)) if if_exists => {}
                Err(e) => return Err(e),
            }
            Ok(QueryResult::Ddl)
        }
        Statement::Insert {
            table,
            columns,
            rows,
        } => exec_insert(engine, &table, columns, rows),
        Statement::Select(select) => exec_select(engine, select),
        Statement::Update {
            table,
            assignments,
            filter,
        } => exec_update(engine, &table, assignments, filter),
        Statement::Delete { table, filter } => exec_delete(engine, &table, filter),
    }
}

fn exec_insert(
    engine: &SqlEngine,
    table: &str,
    columns: Option<Vec<String>>,
    rows: Vec<Vec<Expr>>,
) -> Result<QueryResult> {
    let def = engine
        .table_def(table)
        .ok_or_else(|| SqlError::NoSuchTable(table.to_string()))?;

    let mut affected = 0;
    for row_exprs in rows {
        // VALUES cannot reference columns, so evaluate against an empty row.
        let values: Vec<Value> = row_exprs
            .iter()
            .map(|e| eval(e, &def, &[]))
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
                    let idx = column_index(&def, name)?;
                    cells[idx] = val;
                }
                cells
            }
            None => values,
        };

        engine.insert(table, cells)?;
        affected += 1;
    }
    Ok(QueryResult::Mutation { affected })
}

fn exec_select(engine: &SqlEngine, select: SelectStmt) -> Result<QueryResult> {
    let def = engine
        .table_def(&select.table)
        .ok_or_else(|| SqlError::NoSuchTable(select.table.clone()))?;

    // 1. Scan + filter.
    let mut rows: Vec<Vec<Value>> = Vec::new();
    for (_id, cells) in engine.scan(&select.table)? {
        if let Some(pred) = &select.filter
            && !truthy(&eval(pred, &def, &cells)?)
        {
            continue;
        }
        rows.push(cells);
    }

    // 2. ORDER BY (on full rows, before projection).
    if !select.order_by.is_empty() {
        let keys: Vec<(usize, bool)> = select
            .order_by
            .iter()
            .map(|(col, asc)| Ok((column_index(&def, col)?, *asc)))
            .collect::<Result<_>>()?;
        rows.sort_by(|a, b| {
            for (idx, asc) in &keys {
                let ord = order_cmp(&a[*idx], &b[*idx]);
                let ord = if *asc { ord } else { ord.reverse() };
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            Ordering::Equal
        });
    }

    // 3. Projection.
    let (columns, projected): (Vec<String>, Vec<Vec<Value>>) = match &select.projection {
        Projection::All => {
            let names = def.columns.iter().map(|c| c.name.clone()).collect();
            (names, rows)
        }
        Projection::Columns(cols) => {
            let idxs: Vec<usize> = cols
                .iter()
                .map(|c| column_index(&def, c))
                .collect::<Result<_>>()?;
            let projected = rows
                .into_iter()
                .map(|row| idxs.iter().map(|i| row[*i].clone()).collect())
                .collect();
            (cols.clone(), projected)
        }
    };

    // 4. LIMIT.
    let projected = match select.limit {
        Some(n) => projected.into_iter().take(n).collect(),
        None => projected,
    };

    Ok(QueryResult::Select {
        columns,
        rows: projected,
    })
}

fn exec_update(
    engine: &SqlEngine,
    table: &str,
    assignments: Vec<(String, Expr)>,
    filter: Option<Expr>,
) -> Result<QueryResult> {
    let def = engine
        .table_def(table)
        .ok_or_else(|| SqlError::NoSuchTable(table.to_string()))?;
    let targets: Vec<(usize, Expr)> = assignments
        .into_iter()
        .map(|(col, expr)| Ok((column_index(&def, &col)?, expr)))
        .collect::<Result<_>>()?;

    let mut affected = 0;
    for (row_id, cells) in engine.scan(table)? {
        if let Some(pred) = &filter
            && !truthy(&eval(pred, &def, &cells)?)
        {
            continue;
        }
        let mut new_cells = cells.clone();
        for (idx, expr) in &targets {
            new_cells[*idx] = eval(expr, &def, &cells)?;
        }
        engine.update_row(table, row_id, new_cells)?;
        affected += 1;
    }
    Ok(QueryResult::Mutation { affected })
}

fn exec_delete(engine: &SqlEngine, table: &str, filter: Option<Expr>) -> Result<QueryResult> {
    let def = engine
        .table_def(table)
        .ok_or_else(|| SqlError::NoSuchTable(table.to_string()))?;

    let mut to_delete = Vec::new();
    for (row_id, cells) in engine.scan(table)? {
        let matches = match &filter {
            Some(pred) => truthy(&eval(pred, &def, &cells)?),
            None => true,
        };
        if matches {
            to_delete.push(row_id);
        }
    }
    let mut affected = 0;
    for row_id in to_delete {
        if engine.delete(table, row_id)? {
            affected += 1;
        }
    }
    Ok(QueryResult::Mutation { affected })
}

// ── expression evaluation ─────────────────────────────────────────────────

fn column_index(def: &Table, name: &str) -> Result<usize> {
    def.columns
        .iter()
        .position(|c| c.name == name)
        .ok_or_else(|| SqlError::NoSuchColumn(name.to_string()))
}

/// A value is "true" for WHERE purposes only if it is exactly `Bool(true)`
/// (SQL three-valued logic: NULL and non-booleans are not true).
fn truthy(v: &Value) -> bool {
    matches!(v, Value::Bool(true))
}

fn eval(expr: &Expr, def: &Table, row: &[Value]) -> Result<Value> {
    match expr {
        Expr::Literal(v) => Ok(v.clone()),
        Expr::Column(name) => {
            let idx = column_index(def, name)?;
            row.get(idx)
                .cloned()
                .ok_or_else(|| SqlError::Eval(format!("column {name:?} out of range")))
        }
        Expr::IsNull { expr, negated } => {
            let v = eval(expr, def, row)?;
            let is_null = matches!(v, Value::Null);
            Ok(Value::Bool(is_null != *negated))
        }
        Expr::Unary { op, expr } => {
            let v = eval(expr, def, row)?;
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
        Expr::Binary { op, left, right } => {
            let l = eval(left, def, row)?;
            let r = eval(right, def, row)?;
            eval_binary(*op, l, r)
        }
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

/// SQL three-valued AND (`is_or = false`) / OR (`is_or = true`).
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
    // Integer arithmetic when both sides are integers; otherwise promote to f64.
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

/// SQL comparison: returns `None` when the two values are not comparable
/// (e.g. a number vs a string). Numeric kinds (Int/Double/Timestamp) compare
/// across each other.
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

/// A **total** order over values for ORDER BY. Establishes a cross-type ranking
/// (Null < Bool < numeric < Text) and orders within a kind; NULLs sort first.
fn order_cmp(a: &Value, b: &Value) -> Ordering {
    fn rank(v: &Value) -> u8 {
        match v {
            Value::Null => 0,
            Value::Bool(_) => 1,
            Value::Int(_) | Value::Double(_) | Value::Timestamp(_) => 2,
            Value::Text(_) => 3,
        }
    }
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Text(x), Value::Text(y)) => x.cmp(y),
        _ if rank(a) == 2 && rank(b) == 2 => as_f64(a)
            .unwrap()
            .partial_cmp(&as_f64(b).unwrap())
            .unwrap_or(Ordering::Equal),
        _ => rank(a).cmp(&rank(b)),
    }
}
