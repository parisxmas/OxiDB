//! Translate `sqlparser` syntax trees into our logical [`ast`](crate::ast).
//!
//! Only the Phase 1 subset is accepted; anything outside it becomes
//! [`SqlError::Unsupported`] rather than being silently mis-handled.

use sqlparser::ast as sp;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::ast::{BinOp, Expr, Projection, SelectStmt, Statement, UnOp};
use crate::catalog::{Column, Table};
use crate::error::{Result, SqlError};
use crate::types::{SqlType, Value};

/// Parse a SQL string into zero or more logical statements.
pub fn parse(sql: &str) -> Result<Vec<Statement>> {
    let dialect = GenericDialect {};
    let statements =
        Parser::parse_sql(&dialect, sql).map_err(|e| SqlError::Parse(e.to_string()))?;
    statements.into_iter().map(translate).collect()
}

fn translate(stmt: sp::Statement) -> Result<Statement> {
    match stmt {
        sp::Statement::CreateTable(ct) => translate_create_table(ct),
        sp::Statement::Drop {
            object_type,
            if_exists,
            names,
            ..
        } => {
            if object_type != sp::ObjectType::Table {
                return Err(SqlError::Unsupported(format!("DROP {object_type:?}")));
            }
            let name = single_object_name(&names)?;
            Ok(Statement::DropTable { name, if_exists })
        }
        sp::Statement::Insert(insert) => translate_insert(insert),
        sp::Statement::Query(query) => translate_select(*query),
        sp::Statement::Update {
            table,
            assignments,
            from,
            selection,
            ..
        } => {
            if from.is_some() {
                return Err(SqlError::Unsupported("UPDATE ... FROM".into()));
            }
            let table = table_name_from_twj(&table)?;
            let assignments = assignments
                .into_iter()
                .map(translate_assignment)
                .collect::<Result<Vec<_>>>()?;
            let filter = selection.map(translate_expr).transpose()?;
            Ok(Statement::Update {
                table,
                assignments,
                filter,
            })
        }
        sp::Statement::Delete(del) => translate_delete(del),
        other => Err(SqlError::Unsupported(format!("statement: {other}"))),
    }
}

fn translate_create_table(ct: sp::CreateTable) -> Result<Statement> {
    if !ct.constraints.is_empty() {
        return Err(SqlError::Unsupported("table-level constraints".into()));
    }
    let name = object_name_to_string(&ct.name)?;
    let mut columns = Vec::with_capacity(ct.columns.len());
    for col in &ct.columns {
        columns.push(translate_column(col)?);
    }
    Ok(Statement::CreateTable {
        table: Table::new(name, columns),
        if_not_exists: ct.if_not_exists,
    })
}

fn translate_column(col: &sp::ColumnDef) -> Result<Column> {
    let ty = map_data_type(&col.data_type)?;
    let mut column = Column::new(col.name.value.clone(), ty);
    for opt in &col.options {
        match &opt.option {
            sp::ColumnOption::NotNull => column = column.not_null(),
            sp::ColumnOption::Null => {}
            sp::ColumnOption::Unique { is_primary, .. } if *is_primary => {
                column = column.primary_key();
            }
            sp::ColumnOption::Unique { .. } => {
                // Plain UNIQUE has no enforcement yet; accept the type, ignore
                // the constraint (documented Phase 1 limitation).
            }
            other => {
                return Err(SqlError::Unsupported(format!(
                    "column option {other:?} on {:?}",
                    col.name.value
                )));
            }
        }
    }
    Ok(column)
}

fn map_data_type(dt: &sp::DataType) -> Result<SqlType> {
    use sp::DataType as D;
    let ty = match dt {
        D::Int(_) | D::Integer(_) | D::BigInt(_) | D::SmallInt(_) | D::TinyInt(_) => SqlType::Int,
        D::Double(_) | D::DoublePrecision | D::Float(_) | D::Real => SqlType::Double,
        D::Text
        | D::Varchar(_)
        | D::Char(_)
        | D::CharVarying(_)
        | D::String(_)
        | D::Nvarchar(_) => SqlType::Text,
        D::Bool | D::Boolean => SqlType::Bool,
        D::Timestamp(_, _) | D::Datetime(_) => SqlType::Timestamp,
        other => return Err(SqlError::Unsupported(format!("data type {other:?}"))),
    };
    Ok(ty)
}

fn translate_insert(insert: sp::Insert) -> Result<Statement> {
    let table = match &insert.table {
        sp::TableObject::TableName(name) => object_name_to_string(name)?,
        other => return Err(SqlError::Unsupported(format!("INSERT target {other:?}"))),
    };
    let columns = if insert.columns.is_empty() {
        None
    } else {
        Some(insert.columns.iter().map(|i| i.value.clone()).collect())
    };
    let source = insert
        .source
        .ok_or_else(|| SqlError::Unsupported("INSERT without VALUES".into()))?;
    let rows = match *source.body {
        sp::SetExpr::Values(values) => values
            .rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(translate_expr)
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?,
        _ => return Err(SqlError::Unsupported("INSERT ... SELECT".into())),
    };
    Ok(Statement::Insert {
        table,
        columns,
        rows,
    })
}

fn translate_delete(del: sp::Delete) -> Result<Statement> {
    let twjs = match &del.from {
        sp::FromTable::WithFromKeyword(t) | sp::FromTable::WithoutKeyword(t) => t,
    };
    if twjs.len() != 1 {
        return Err(SqlError::Unsupported("DELETE from multiple tables".into()));
    }
    let table = table_name_from_twj(&twjs[0])?;
    let filter = del.selection.map(translate_expr).transpose()?;
    Ok(Statement::Delete { table, filter })
}

fn translate_select(query: sp::Query) -> Result<Statement> {
    let select = match *query.body {
        sp::SetExpr::Select(s) => s,
        _ => return Err(SqlError::Unsupported("compound / set query".into())),
    };
    if select.from.len() != 1 {
        return Err(SqlError::Unsupported(
            "SELECT must reference exactly one table".into(),
        ));
    }
    if !select.from[0].joins.is_empty() {
        return Err(SqlError::Unsupported("JOIN (Phase 2)".into()));
    }
    if select.distinct.is_some() {
        return Err(SqlError::Unsupported("SELECT DISTINCT".into()));
    }
    if select.having.is_some() {
        return Err(SqlError::Unsupported("HAVING (Phase 2)".into()));
    }
    if has_group_by(&select.group_by) {
        return Err(SqlError::Unsupported("GROUP BY (Phase 2)".into()));
    }

    let table = table_name_from_twj(&select.from[0])?;

    let projection = translate_projection(&select.projection)?;
    let filter = select.selection.map(translate_expr).transpose()?;
    let order_by = translate_order_by(&query.order_by)?;
    let limit = translate_limit(&query.limit_clause)?;

    Ok(Statement::Select(SelectStmt {
        table,
        projection,
        filter,
        order_by,
        limit,
    }))
}

fn translate_projection(items: &[sp::SelectItem]) -> Result<Projection> {
    if items
        .iter()
        .any(|i| matches!(i, sp::SelectItem::Wildcard(_)))
    {
        if items.len() == 1 {
            return Ok(Projection::All);
        }
        return Err(SqlError::Unsupported("`*` mixed with columns".into()));
    }
    let mut cols = Vec::with_capacity(items.len());
    for item in items {
        match item {
            sp::SelectItem::UnnamedExpr(sp::Expr::Identifier(id)) => cols.push(id.value.clone()),
            sp::SelectItem::ExprWithAlias {
                expr: sp::Expr::Identifier(id),
                ..
            } => cols.push(id.value.clone()),
            other => {
                return Err(SqlError::Unsupported(format!(
                    "projection item {other:?} (only bare columns / * in Phase 1)"
                )));
            }
        }
    }
    Ok(Projection::Columns(cols))
}

fn translate_order_by(order_by: &Option<sp::OrderBy>) -> Result<Vec<(String, bool)>> {
    let Some(ob) = order_by else {
        return Ok(Vec::new());
    };
    let exprs = match &ob.kind {
        sp::OrderByKind::Expressions(e) => e,
        sp::OrderByKind::All(_) => {
            return Err(SqlError::Unsupported("ORDER BY ALL".into()));
        }
    };
    let mut keys = Vec::with_capacity(exprs.len());
    for e in exprs {
        let col = match &e.expr {
            sp::Expr::Identifier(id) => id.value.clone(),
            other => {
                return Err(SqlError::Unsupported(format!(
                    "ORDER BY expression {other:?} (columns only in Phase 1)"
                )));
            }
        };
        let asc = e.options.asc.unwrap_or(true);
        keys.push((col, asc));
    }
    Ok(keys)
}

fn translate_limit(limit: &Option<sp::LimitClause>) -> Result<Option<usize>> {
    let Some(clause) = limit else {
        return Ok(None);
    };
    match clause {
        sp::LimitClause::LimitOffset {
            limit: Some(expr),
            offset,
            limit_by,
        } => {
            if offset.is_some() {
                return Err(SqlError::Unsupported("OFFSET (Phase 2)".into()));
            }
            if !limit_by.is_empty() {
                return Err(SqlError::Unsupported("LIMIT BY".into()));
            }
            let n = expr_to_u64(expr)?;
            Ok(Some(n as usize))
        }
        sp::LimitClause::LimitOffset { limit: None, .. } => Ok(None),
        sp::LimitClause::OffsetCommaLimit { .. } => {
            Err(SqlError::Unsupported("MySQL LIMIT offset,count".into()))
        }
    }
}

fn translate_assignment(a: sp::Assignment) -> Result<(String, Expr)> {
    let col = match a.target {
        sp::AssignmentTarget::ColumnName(name) => object_name_to_string(&name)?,
        sp::AssignmentTarget::Tuple(_) => {
            return Err(SqlError::Unsupported("tuple assignment".into()));
        }
    };
    Ok((col, translate_expr(a.value)?))
}

fn translate_expr(expr: sp::Expr) -> Result<Expr> {
    match expr {
        sp::Expr::Identifier(id) => Ok(Expr::Column(id.value)),
        sp::Expr::CompoundIdentifier(parts) => {
            // `t.col` -> use the final segment.
            let last = parts
                .last()
                .ok_or_else(|| SqlError::Parse("empty compound identifier".into()))?;
            Ok(Expr::Column(last.value.clone()))
        }
        sp::Expr::Value(v) => Ok(Expr::Literal(literal_from_value(&v.value)?)),
        sp::Expr::Nested(inner) => translate_expr(*inner),
        sp::Expr::IsNull(inner) => Ok(Expr::IsNull {
            expr: Box::new(translate_expr(*inner)?),
            negated: false,
        }),
        sp::Expr::IsNotNull(inner) => Ok(Expr::IsNull {
            expr: Box::new(translate_expr(*inner)?),
            negated: true,
        }),
        sp::Expr::UnaryOp { op, expr } => {
            let un = match op {
                sp::UnaryOperator::Not => UnOp::Not,
                sp::UnaryOperator::Minus => UnOp::Neg,
                sp::UnaryOperator::Plus => return translate_expr(*expr),
                other => return Err(SqlError::Unsupported(format!("unary operator {other:?}"))),
            };
            Ok(Expr::Unary {
                op: un,
                expr: Box::new(translate_expr(*expr)?),
            })
        }
        sp::Expr::BinaryOp { left, op, right } => {
            let bin = map_binary_op(&op)?;
            Ok(Expr::Binary {
                op: bin,
                left: Box::new(translate_expr(*left)?),
                right: Box::new(translate_expr(*right)?),
            })
        }
        other => Err(SqlError::Unsupported(format!("expression {other:?}"))),
    }
}

fn map_binary_op(op: &sp::BinaryOperator) -> Result<BinOp> {
    use sp::BinaryOperator as B;
    let mapped = match op {
        B::Eq => BinOp::Eq,
        B::NotEq => BinOp::Ne,
        B::Lt => BinOp::Lt,
        B::LtEq => BinOp::Le,
        B::Gt => BinOp::Gt,
        B::GtEq => BinOp::Ge,
        B::And => BinOp::And,
        B::Or => BinOp::Or,
        B::Plus => BinOp::Add,
        B::Minus => BinOp::Sub,
        B::Multiply => BinOp::Mul,
        B::Divide => BinOp::Div,
        other => return Err(SqlError::Unsupported(format!("binary operator {other:?}"))),
    };
    Ok(mapped)
}

fn literal_from_value(v: &sp::Value) -> Result<Value> {
    match v {
        sp::Value::Number(s, _) => parse_number(s),
        sp::Value::SingleQuotedString(s)
        | sp::Value::DoubleQuotedString(s)
        | sp::Value::EscapedStringLiteral(s) => Ok(Value::Text(s.clone())),
        sp::Value::Boolean(b) => Ok(Value::Bool(*b)),
        sp::Value::Null => Ok(Value::Null),
        other => Err(SqlError::Unsupported(format!("literal {other:?}"))),
    }
}

fn parse_number(s: &str) -> Result<Value> {
    if s.contains('.') || s.contains('e') || s.contains('E') {
        s.parse::<f64>()
            .map(Value::Double)
            .map_err(|_| SqlError::Parse(format!("bad number literal {s:?}")))
    } else {
        s.parse::<i64>()
            .map(Value::Int)
            .map_err(|_| SqlError::Parse(format!("bad integer literal {s:?}")))
    }
}

fn expr_to_u64(expr: &sp::Expr) -> Result<u64> {
    match expr {
        sp::Expr::Value(v) => match &v.value {
            sp::Value::Number(s, _) => s
                .parse::<u64>()
                .map_err(|_| SqlError::Parse(format!("bad LIMIT value {s:?}"))),
            other => Err(SqlError::Unsupported(format!("LIMIT value {other:?}"))),
        },
        other => Err(SqlError::Unsupported(format!("LIMIT expression {other:?}"))),
    }
}

// ── name helpers ──────────────────────────────────────────────────────────

fn object_name_to_string(name: &sp::ObjectName) -> Result<String> {
    let ident = name
        .0
        .last()
        .and_then(|p| p.as_ident())
        .ok_or_else(|| SqlError::Parse("empty object name".into()))?;
    Ok(ident.value.clone())
}

fn single_object_name(names: &[sp::ObjectName]) -> Result<String> {
    if names.len() != 1 {
        return Err(SqlError::Unsupported("DROP of multiple objects".into()));
    }
    object_name_to_string(&names[0])
}

fn table_name_from_twj(twj: &sp::TableWithJoins) -> Result<String> {
    if !twj.joins.is_empty() {
        return Err(SqlError::Unsupported("JOIN (Phase 2)".into()));
    }
    match &twj.relation {
        sp::TableFactor::Table { name, .. } => object_name_to_string(name),
        other => Err(SqlError::Unsupported(format!("table factor {other:?}"))),
    }
}

fn has_group_by(gb: &sp::GroupByExpr) -> bool {
    match gb {
        sp::GroupByExpr::Expressions(exprs, _) => !exprs.is_empty(),
        sp::GroupByExpr::All(_) => true,
    }
}
