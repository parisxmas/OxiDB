//! Translate `sqlparser` syntax trees into our logical [`ast`](crate::ast).
//!
//! Only the supported subset is accepted; anything outside it becomes
//! [`SqlError::Unsupported`] rather than being silently mis-handled.

use sqlparser::ast as sp;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::ast::{
    AggFunc, BinOp, Expr, Join, JoinKind, SelectItem, SelectStmt, Statement, TableRef, UnOp,
};
use crate::catalog::{Column, Table};
use crate::error::{Result, SqlError};
use crate::types::{SqlType, Value};

/// Parse a SQL string into zero or more logical statements.
///
/// `?` placeholders are numbered left-to-right across the whole string; `$N`
/// placeholders use `N-1` directly. Both resolve against the params slice at
/// execution time.
pub fn parse(sql: &str) -> Result<Vec<Statement>> {
    let dialect = GenericDialect {};
    let statements =
        Parser::parse_sql(&dialect, sql).map_err(|e| SqlError::Parse(e.to_string()))?;
    let mut next_param = 0usize;
    statements
        .into_iter()
        .map(|s| translate(s, &mut next_param))
        .collect()
}

fn translate(stmt: sp::Statement, p: &mut usize) -> Result<Statement> {
    match stmt {
        sp::Statement::CreateTable(ct) => translate_create_table(ct),
        sp::Statement::CreateIndex(ci) => translate_create_index(ci),
        sp::Statement::Drop {
            object_type,
            if_exists,
            names,
            ..
        } => match object_type {
            sp::ObjectType::Table => Ok(Statement::DropTable {
                name: single_object_name(&names)?,
                if_exists,
            }),
            sp::ObjectType::Index => Ok(Statement::DropIndex {
                name: single_object_name(&names)?,
                if_exists,
            }),
            other => Err(SqlError::Unsupported(format!("DROP {other:?}"))),
        },
        sp::Statement::Insert(insert) => translate_insert(insert, p),
        sp::Statement::Query(query) => translate_select(*query, p),
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
                .map(|a| translate_assignment(a, p))
                .collect::<Result<Vec<_>>>()?;
            let filter = selection.map(|e| translate_expr(e, p)).transpose()?;
            Ok(Statement::Update {
                table,
                assignments,
                filter,
            })
        }
        sp::Statement::Delete(del) => translate_delete(del, p),
        sp::Statement::StartTransaction { .. } => Ok(Statement::Begin),
        sp::Statement::Commit { .. } => Ok(Statement::Commit),
        sp::Statement::Rollback { .. } => Ok(Statement::Rollback),
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

fn translate_create_index(ci: sp::CreateIndex) -> Result<Statement> {
    let table = object_name_to_string(&ci.table_name)?;
    let name = match &ci.name {
        Some(n) => object_name_to_string(n)?,
        None => return Err(SqlError::Unsupported("CREATE INDEX without a name".into())),
    };
    if ci.columns.len() != 1 {
        return Err(SqlError::Unsupported(
            "multi-column index (Phase 2 supports single-column)".into(),
        ));
    }
    let column = match &ci.columns[0].column.expr {
        sp::Expr::Identifier(id) => id.value.clone(),
        other => {
            return Err(SqlError::Unsupported(format!(
                "index on expression {other:?} (columns only)"
            )));
        }
    };
    Ok(Statement::CreateIndex {
        name,
        table,
        column,
        if_not_exists: ci.if_not_exists,
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
                // the constraint (documented limitation).
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

fn translate_insert(insert: sp::Insert, p: &mut usize) -> Result<Statement> {
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
                    .map(|e| translate_expr(e, p))
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

fn translate_delete(del: sp::Delete, p: &mut usize) -> Result<Statement> {
    let twjs = match &del.from {
        sp::FromTable::WithFromKeyword(t) | sp::FromTable::WithoutKeyword(t) => t,
    };
    if twjs.len() != 1 {
        return Err(SqlError::Unsupported("DELETE from multiple tables".into()));
    }
    let table = table_name_from_twj(&twjs[0])?;
    let filter = del.selection.map(|e| translate_expr(e, p)).transpose()?;
    Ok(Statement::Delete { table, filter })
}

fn translate_select(query: sp::Query, p: &mut usize) -> Result<Statement> {
    let select = match *query.body {
        sp::SetExpr::Select(s) => s,
        _ => return Err(SqlError::Unsupported("compound / set query".into())),
    };
    if select.from.len() != 1 {
        return Err(SqlError::Unsupported(
            "SELECT must reference exactly one table in FROM (use JOIN)".into(),
        ));
    }
    if select.distinct.is_some() {
        return Err(SqlError::Unsupported("SELECT DISTINCT".into()));
    }

    let from = table_ref_from_factor(&select.from[0].relation)?;

    // JOINs: INNER / LEFT / RIGHT / FULL, all with an ON predicate.
    let mut joins = Vec::new();
    for j in &select.from[0].joins {
        let table = table_ref_from_factor(&j.relation)?;
        let (kind, constraint) = match &j.join_operator {
            sp::JoinOperator::Inner(c) | sp::JoinOperator::Join(c) => (JoinKind::Inner, c),
            sp::JoinOperator::Left(c) | sp::JoinOperator::LeftOuter(c) => (JoinKind::Left, c),
            sp::JoinOperator::Right(c) | sp::JoinOperator::RightOuter(c) => (JoinKind::Right, c),
            sp::JoinOperator::FullOuter(c) => (JoinKind::Full, c),
            other => {
                return Err(SqlError::Unsupported(format!(
                    "join type {other:?} (INNER/LEFT/RIGHT/FULL ... ON only)"
                )));
            }
        };
        let on = match constraint {
            sp::JoinConstraint::On(expr) => translate_expr(expr.clone(), p)?,
            other => {
                return Err(SqlError::Unsupported(format!(
                    "join constraint {other:?} (only ON <expr> is supported)"
                )));
            }
        };
        joins.push(Join { table, kind, on });
    }

    let projection = select
        .projection
        .iter()
        .map(|item| translate_select_item(item, p))
        .collect::<Result<Vec<_>>>()?;

    let filter = select.selection.map(|e| translate_expr(e, p)).transpose()?;

    let group_by = match &select.group_by {
        sp::GroupByExpr::Expressions(exprs, _) => exprs
            .iter()
            .map(|e| translate_expr(e.clone(), p))
            .collect::<Result<Vec<_>>>()?,
        sp::GroupByExpr::All(_) => return Err(SqlError::Unsupported("GROUP BY ALL".into())),
    };

    let having = select.having.map(|e| translate_expr(e, p)).transpose()?;
    let order_by = translate_order_by(query.order_by, p)?;
    let limit = translate_limit(&query.limit_clause, p)?;

    Ok(Statement::Select(SelectStmt {
        from,
        joins,
        projection,
        filter,
        group_by,
        having,
        order_by,
        limit,
    }))
}

fn translate_select_item(item: &sp::SelectItem, p: &mut usize) -> Result<SelectItem> {
    match item {
        sp::SelectItem::Wildcard(_) => Ok(SelectItem::Wildcard),
        sp::SelectItem::QualifiedWildcard(kind, _) => match kind {
            sp::SelectItemQualifiedWildcardKind::ObjectName(name) => {
                Ok(SelectItem::QualifiedWildcard(object_name_to_string(name)?))
            }
            sp::SelectItemQualifiedWildcardKind::Expr(_) => {
                Err(SqlError::Unsupported("expr.* wildcard".into()))
            }
        },
        sp::SelectItem::UnnamedExpr(expr) => Ok(SelectItem::Expr {
            expr: translate_expr(expr.clone(), p)?,
            alias: None,
        }),
        sp::SelectItem::ExprWithAlias { expr, alias } => Ok(SelectItem::Expr {
            expr: translate_expr(expr.clone(), p)?,
            alias: Some(alias.value.clone()),
        }),
    }
}

fn translate_order_by(order_by: Option<sp::OrderBy>, p: &mut usize) -> Result<Vec<(Expr, bool)>> {
    let Some(ob) = order_by else {
        return Ok(Vec::new());
    };
    let exprs = match ob.kind {
        sp::OrderByKind::Expressions(e) => e,
        sp::OrderByKind::All(_) => return Err(SqlError::Unsupported("ORDER BY ALL".into())),
    };
    let mut keys = Vec::with_capacity(exprs.len());
    for e in exprs {
        let asc = e.options.asc.unwrap_or(true);
        keys.push((translate_expr(e.expr, p)?, asc));
    }
    Ok(keys)
}

fn translate_limit(limit: &Option<sp::LimitClause>, _p: &mut usize) -> Result<Option<usize>> {
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
                return Err(SqlError::Unsupported("OFFSET (later phase)".into()));
            }
            if !limit_by.is_empty() {
                return Err(SqlError::Unsupported("LIMIT BY".into()));
            }
            Ok(Some(expr_to_u64(expr)? as usize))
        }
        sp::LimitClause::LimitOffset { limit: None, .. } => Ok(None),
        sp::LimitClause::OffsetCommaLimit { .. } => {
            Err(SqlError::Unsupported("MySQL LIMIT offset,count".into()))
        }
    }
}

fn translate_assignment(a: sp::Assignment, p: &mut usize) -> Result<(String, Expr)> {
    let col = match a.target {
        sp::AssignmentTarget::ColumnName(name) => object_name_to_string(&name)?,
        sp::AssignmentTarget::Tuple(_) => {
            return Err(SqlError::Unsupported("tuple assignment".into()));
        }
    };
    Ok((col, translate_expr(a.value, p)?))
}

fn translate_expr(expr: sp::Expr, p: &mut usize) -> Result<Expr> {
    match expr {
        sp::Expr::Identifier(id) => Ok(Expr::Column {
            table: None,
            name: id.value,
        }),
        sp::Expr::CompoundIdentifier(parts) => {
            // `t.col` -> qualifier + final segment. Deeper paths (a.b.c) are
            // reduced to (b, c) which is enough for single-schema qualification.
            if parts.len() < 2 {
                let name = parts
                    .into_iter()
                    .next()
                    .ok_or_else(|| SqlError::Parse("empty compound identifier".into()))?
                    .value;
                return Ok(Expr::Column { table: None, name });
            }
            let name = parts.last().unwrap().value.clone();
            let table = parts[parts.len() - 2].value.clone();
            Ok(Expr::Column {
                table: Some(table),
                name,
            })
        }
        sp::Expr::Value(v) => translate_value(&v.value, p),
        sp::Expr::Nested(inner) => translate_expr(*inner, p),
        sp::Expr::IsNull(inner) => Ok(Expr::IsNull {
            expr: Box::new(translate_expr(*inner, p)?),
            negated: false,
        }),
        sp::Expr::IsNotNull(inner) => Ok(Expr::IsNull {
            expr: Box::new(translate_expr(*inner, p)?),
            negated: true,
        }),
        sp::Expr::UnaryOp { op, expr } => {
            let un = match op {
                sp::UnaryOperator::Not => UnOp::Not,
                sp::UnaryOperator::Minus => UnOp::Neg,
                sp::UnaryOperator::Plus => return translate_expr(*expr, p),
                other => return Err(SqlError::Unsupported(format!("unary operator {other:?}"))),
            };
            Ok(Expr::Unary {
                op: un,
                expr: Box::new(translate_expr(*expr, p)?),
            })
        }
        sp::Expr::BinaryOp { left, op, right } => Ok(Expr::Binary {
            op: map_binary_op(&op)?,
            left: Box::new(translate_expr(*left, p)?),
            right: Box::new(translate_expr(*right, p)?),
        }),
        sp::Expr::Function(f) => translate_function(f, p),
        other => Err(SqlError::Unsupported(format!("expression {other:?}"))),
    }
}

fn translate_function(f: sp::Function, p: &mut usize) -> Result<Expr> {
    let fname = object_name_to_string(&f.name)?.to_ascii_lowercase();
    let func = match fname.as_str() {
        "count" => AggFunc::Count,
        "sum" => AggFunc::Sum,
        "avg" => AggFunc::Avg,
        "min" => AggFunc::Min,
        "max" => AggFunc::Max,
        other => return Err(SqlError::Unsupported(format!("function {other}()"))),
    };
    let args = match f.args {
        sp::FunctionArguments::List(list) => {
            if list.duplicate_treatment == Some(sp::DuplicateTreatment::Distinct) {
                return Err(SqlError::Unsupported("aggregate DISTINCT".into()));
            }
            list.args
        }
        sp::FunctionArguments::None => Vec::new(),
        sp::FunctionArguments::Subquery(_) => {
            return Err(SqlError::Unsupported("aggregate over subquery".into()));
        }
    };
    if args.len() > 1 {
        return Err(SqlError::Unsupported(
            "aggregate with multiple arguments".into(),
        ));
    }
    let arg = match args.into_iter().next() {
        None => None,
        Some(sp::FunctionArg::Unnamed(sp::FunctionArgExpr::Wildcard)) => None, // COUNT(*)
        Some(sp::FunctionArg::Unnamed(sp::FunctionArgExpr::Expr(e))) => {
            Some(Box::new(translate_expr(e, p)?))
        }
        Some(other) => {
            return Err(SqlError::Unsupported(format!(
                "aggregate argument {other:?}"
            )));
        }
    };
    Ok(Expr::Aggregate { func, arg })
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

fn translate_value(v: &sp::Value, p: &mut usize) -> Result<Expr> {
    match v {
        sp::Value::Placeholder(s) => {
            let idx = if s == "?" {
                let i = *p;
                *p += 1;
                i
            } else if let Some(n) = s.strip_prefix('$') {
                n.parse::<usize>()
                    .map_err(|_| SqlError::Parse(format!("bad placeholder {s:?}")))?
                    .checked_sub(1)
                    .ok_or_else(|| SqlError::Parse("placeholder index must be >= 1".into()))?
            } else {
                return Err(SqlError::Unsupported(format!("placeholder {s:?}")));
            };
            Ok(Expr::Param(idx))
        }
        other => Ok(Expr::Literal(literal_from_value(other)?)),
    }
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

fn table_ref_from_factor(factor: &sp::TableFactor) -> Result<TableRef> {
    match factor {
        sp::TableFactor::Table { name, alias, .. } => Ok(TableRef {
            name: object_name_to_string(name)?,
            alias: alias.as_ref().map(|a| a.name.value.clone()),
        }),
        other => Err(SqlError::Unsupported(format!("table factor {other:?}"))),
    }
}

fn table_name_from_twj(twj: &sp::TableWithJoins) -> Result<String> {
    if !twj.joins.is_empty() {
        return Err(SqlError::Unsupported("JOIN not allowed here".into()));
    }
    Ok(table_ref_from_factor(&twj.relation)?.name)
}
