use serde_json::{json, Map, Value};
use sqlparser::ast::{
    self, AssignmentTarget, BinaryOperator, CreateIndex, Expr, FromTable, FunctionArg,
    FunctionArgExpr, GroupByExpr, JoinConstraint, JoinOperator, LimitClause, ObjectName,
    ObjectType, OrderByExpr, OrderByKind, Query, SelectItem, SetExpr, Statement, TableFactor,
    TableObject, TableWithJoins,
};
use sqlparser::dialect::{
    Dialect, GenericDialect, MsSqlDialect, MySqlDialect, PostgreSqlDialect,
};
use sqlparser::parser::Parser;

use crate::database_manager::DatabaseManager;
use crate::engine::OxiDb;
use crate::error::{Error, Result};
use crate::query::FindOptions;

/// Supported SQL dialects.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SqlDialect {
    #[default]
    Generic,
    MySQL,
    PostgreSQL,
    MsSQL,
}

impl SqlDialect {
    /// Parse a dialect name string (case-insensitive).
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "mysql" | "mariadb" => Self::MySQL,
            "postgres" | "postgresql" | "pg" => Self::PostgreSQL,
            "mssql" | "sqlserver" | "tsql" => Self::MsSQL,
            _ => Self::Generic,
        }
    }

    fn to_parser_dialect(&self) -> Box<dyn Dialect> {
        match self {
            Self::Generic => Box::new(GenericDialect {}),
            Self::MySQL => Box::new(MySqlDialect {}),
            Self::PostgreSQL => Box::new(PostgreSqlDialect {}),
            Self::MsSQL => Box::new(MsSqlDialect {}),
        }
    }
}

/// Result of executing a SQL statement against the engine.
#[derive(Debug)]
pub enum SqlResult {
    Select(Vec<Value>),
    Insert(Vec<u64>),
    Update(u64),
    Delete(u64),
    Ddl(String),
    /// Switch current database context; the handler should update the session.
    UseDatabase(String),
    /// List of database names.
    ShowDatabases(Vec<String>),
}

/// Parse and execute a SQL statement against the OxiDB engine.
pub fn execute_sql(db: &OxiDb, sql: &str) -> Result<SqlResult> {
    execute_sql_with_dialect(db, sql, SqlDialect::default())
}

/// Parse and execute a SQL statement using a specific SQL dialect.
pub fn execute_sql_with_dialect(db: &OxiDb, sql: &str, dialect: SqlDialect) -> Result<SqlResult> {
    // Pre-parse database-level commands that sqlparser doesn't handle.
    if let Some(result) = try_database_command(sql, None)? {
        return Ok(result);
    }

    let parser_dialect = dialect.to_parser_dialect();
    let statements = Parser::parse_sql(parser_dialect.as_ref(), sql)
        .map_err(|e| Error::InvalidQuery(format!("SQL parse error: {e}")))?;

    if statements.is_empty() {
        return Err(Error::InvalidQuery("empty SQL statement".into()));
    }
    if statements.len() > 1 {
        return Err(Error::InvalidQuery(
            "only single statements are supported".into(),
        ));
    }

    let stmt = statements.into_iter().next().unwrap();
    execute_statement(db, stmt)
}

/// Parse and execute a SQL statement with access to the DatabaseManager.
/// Handles database-level commands (CREATE DATABASE, DROP DATABASE, SHOW DATABASES, USE).
pub fn execute_sql_with_db_manager(
    db: &OxiDb,
    sql: &str,
    db_manager: Option<&DatabaseManager>,
    dialect: SqlDialect,
) -> Result<SqlResult> {
    // Pre-parse database-level commands.
    if let Some(result) = try_database_command(sql, db_manager)? {
        return Ok(result);
    }

    // Fall through to standard SQL execution.
    let parser_dialect = dialect.to_parser_dialect();
    let statements = Parser::parse_sql(parser_dialect.as_ref(), sql)
        .map_err(|e| Error::InvalidQuery(format!("SQL parse error: {e}")))?;

    if statements.is_empty() {
        return Err(Error::InvalidQuery("empty SQL statement".into()));
    }
    if statements.len() > 1 {
        return Err(Error::InvalidQuery(
            "only single statements are supported".into(),
        ));
    }

    let stmt = statements.into_iter().next().unwrap();
    execute_statement(db, stmt)
}

/// Try to handle database-level SQL commands before sqlparser.
/// Returns `Ok(Some(result))` if handled, `Ok(None)` if not a database command.
fn try_database_command(
    sql: &str,
    db_manager: Option<&DatabaseManager>,
) -> Result<Option<SqlResult>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_uppercase();
    let words: Vec<&str> = upper.split_whitespace().collect();

    match words.as_slice() {
        ["CREATE", "DATABASE", _name] => {
            let name = extract_identifier(trimmed, 2);
            if let Some(mgr) = db_manager {
                mgr.create_database(&name)?;
                Ok(Some(SqlResult::Ddl(format!("database '{name}' created"))))
            } else {
                Err(Error::InvalidQuery(
                    "CREATE DATABASE not available in embedded mode".into(),
                ))
            }
        }
        ["DROP", "DATABASE", _name] => {
            let name = extract_identifier(trimmed, 2);
            if let Some(mgr) = db_manager {
                mgr.drop_database(&name)?;
                Ok(Some(SqlResult::Ddl(format!("database '{name}' dropped"))))
            } else {
                Err(Error::InvalidQuery(
                    "DROP DATABASE not available in embedded mode".into(),
                ))
            }
        }
        ["DROP", "DATABASE", "IF", "EXISTS", _name] => {
            let name = extract_identifier(trimmed, 4);
            if let Some(mgr) = db_manager {
                if mgr.database_exists(&name) {
                    mgr.drop_database(&name)?;
                    Ok(Some(SqlResult::Ddl(format!("database '{name}' dropped"))))
                } else {
                    Ok(Some(SqlResult::Ddl(format!(
                        "database '{name}' does not exist"
                    ))))
                }
            } else {
                Err(Error::InvalidQuery(
                    "DROP DATABASE not available in embedded mode".into(),
                ))
            }
        }
        ["SHOW", "DATABASES"] => {
            if let Some(mgr) = db_manager {
                let names = mgr.list_databases();
                Ok(Some(SqlResult::ShowDatabases(names)))
            } else {
                Err(Error::InvalidQuery(
                    "SHOW DATABASES not available in embedded mode".into(),
                ))
            }
        }
        ["USE", _name] => {
            let name = extract_identifier(trimmed, 1);
            Ok(Some(SqlResult::UseDatabase(name)))
        }
        // DESCRIBE / DESC table (MySQL-style)
        ["DESCRIBE", _name] | ["DESC", _name] => {
            let name = extract_identifier(trimmed, 1);
            Ok(Some(SqlResult::Ddl(format!(
                "collection '{name}' (document store — schema-free)"
            ))))
        }
        // SHOW COLUMNS FROM table
        ["SHOW", "COLUMNS", "FROM", _name] => {
            let name = extract_identifier(trimmed, 3);
            Ok(Some(SqlResult::Ddl(format!(
                "collection '{name}' (document store — schema-free)"
            ))))
        }
        // TRUNCATE TABLE
        ["TRUNCATE", "TABLE", _name] | ["TRUNCATE", _name] => {
            let idx = if words.len() == 3 { 2 } else { 1 };
            let name = extract_identifier(trimmed, idx);
            if let Some(mgr) = db_manager {
                let db = mgr.get_database("oxidb").ok();
                if let Some(db) = db {
                    let _ = db.delete(&name, &serde_json::json!({}));
                }
            }
            Ok(Some(SqlResult::Ddl(format!(
                "collection '{name}' truncated"
            ))))
        }
        // \\c or \connect (psql-style)
        _ if upper.starts_with("\\C ") || upper.starts_with("\\CONNECT ") => {
            let name = trimmed.split_whitespace().nth(1).unwrap_or("oxidb").to_string();
            Ok(Some(SqlResult::UseDatabase(name)))
        }
        _ => Ok(None),
    }
}

/// Extract the Nth whitespace-delimited word from the original (unupcased) SQL,
/// stripping quotes and backticks.
fn extract_identifier(sql: &str, word_index: usize) -> String {
    sql.split_whitespace()
        .nth(word_index)
        .unwrap_or("")
        .trim_matches('"')
        .trim_matches('\'')
        .trim_matches('`')
        .to_string()
}

fn execute_statement(db: &OxiDb, stmt: Statement) -> Result<SqlResult> {
    match stmt {
        Statement::Query(query) => execute_query(db, *query),
        Statement::Insert(insert) => execute_insert(db, insert),
        Statement::Update { table, assignments, selection, .. } => {
            execute_update(db, table, assignments, selection)
        }
        Statement::Delete(delete) => execute_delete(db, delete),
        Statement::CreateTable(create) => execute_create_table(db, create),
        Statement::Drop { object_type, names, .. } => execute_drop(db, object_type, names),
        Statement::CreateIndex(create_index) => execute_create_index(db, create_index),
        Statement::ShowTables { .. } => execute_show_tables(db),
        _ => Err(Error::InvalidQuery("unsupported SQL statement".into())),
    }
}

// ---------------------------------------------------------------------------
// SELECT
// ---------------------------------------------------------------------------

fn execute_query(db: &OxiDb, query: Query) -> Result<SqlResult> {
    // Extract order_by, limit, offset from top-level Query
    let order_by_exprs = extract_order_by_exprs(&query.order_by);
    let (limit_expr, offset_val) = extract_limit_offset(&query.limit_clause);

    let select = match *query.body {
        SetExpr::Select(sel) => *sel,
        _ => {
            return Err(Error::InvalidQuery(
                "only simple SELECT statements are supported".into(),
            ))
        }
    };

    // Determine if this is an aggregate query
    let has_group_by = !matches!(&select.group_by, GroupByExpr::Expressions(exprs, _) if exprs.is_empty());
    let has_aggregate_fn = select.projection.iter().any(|item| match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
            is_aggregate_expr(expr)
        }
        _ => false,
    });

    // Determine if this is a JOIN query
    let has_join = select
        .from
        .first()
        .is_some_and(|t| !t.joins.is_empty());

    if has_join {
        execute_join_select(db, &select, &order_by_exprs, limit_expr.as_ref(), offset_val.as_ref())
    } else if !has_group_by && has_aggregate_fn {
        // Fast path: SELECT COUNT(*) FROM table [WHERE ...] — use direct index count
        if let Some(result) = try_fast_count(db, &select)? {
            return Ok(result);
        }
        execute_aggregate_select(db, &select, &order_by_exprs, limit_expr.as_ref(), offset_val.as_ref())
    } else if has_group_by {
        execute_aggregate_select(db, &select, &order_by_exprs, limit_expr.as_ref(), offset_val.as_ref())
    } else {
        execute_simple_select(db, &select, &order_by_exprs, limit_expr.as_ref(), offset_val.as_ref())
    }
}

fn extract_order_by_exprs(order_by: &Option<ast::OrderBy>) -> Vec<OrderByExpr> {
    match order_by {
        Some(ob) => match &ob.kind {
            OrderByKind::Expressions(exprs) => exprs.clone(),
            _ => Vec::new(),
        },
        None => Vec::new(),
    }
}

fn extract_limit_offset(
    limit_clause: &Option<LimitClause>,
) -> (Option<Expr>, Option<ast::Offset>) {
    match limit_clause {
        Some(LimitClause::LimitOffset { limit, offset, .. }) => {
            (limit.clone(), offset.clone())
        }
        Some(LimitClause::OffsetCommaLimit { offset, limit }) => {
            (
                Some(limit.clone()),
                Some(ast::Offset {
                    value: offset.clone(),
                    rows: ast::OffsetRows::None,
                }),
            )
        }
        None => (None, None),
    }
}

/// Fast path for `SELECT COUNT(*) FROM table [WHERE ...]` — avoids the full
/// aggregation pipeline by calling `db.count()` which uses direct index lookups.
fn try_fast_count(db: &OxiDb, select: &ast::Select) -> Result<Option<SqlResult>> {
    // Must be a single projection item that is COUNT(...)
    if select.projection.len() != 1 {
        return Ok(None);
    }
    let expr = match &select.projection[0] {
        SelectItem::UnnamedExpr(e) => e,
        SelectItem::ExprWithAlias { expr, .. } => expr,
        _ => return Ok(None),
    };
    match expr {
        Expr::Function(f) if f.name.to_string().to_uppercase() == "COUNT" => {}
        _ => return Ok(None),
    }

    let table = extract_table_name(&select.from)?;
    let where_json = match &select.selection {
        Some(expr) => translate_expr(expr)?,
        None => json!({}),
    };

    let count = db.count(&table, &where_json)?;

    let alias = match &select.projection[0] {
        SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
        _ => "count".to_string(),
    };

    let doc = json!({ alias: count });
    Ok(Some(SqlResult::Select(vec![doc])))
}

fn is_aggregate_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Function(f) => {
            let name = f.name.to_string().to_uppercase();
            matches!(name.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX")
        }
        _ => false,
    }
}

fn execute_simple_select(
    db: &OxiDb,
    select: &ast::Select,
    order_by: &[OrderByExpr],
    limit: Option<&Expr>,
    offset: Option<&ast::Offset>,
) -> Result<SqlResult> {
    let table = extract_table_name(&select.from)?;
    let where_json = match &select.selection {
        Some(expr) => translate_expr(expr)?,
        None => json!({}),
    };

    let mut opts = FindOptions::default();

    if !order_by.is_empty() {
        let mut sort_fields = Vec::new();
        for ob in order_by {
            let field = expr_to_field_name(&ob.expr)?;
            let order = if ob.options.asc.unwrap_or(true) {
                crate::query::SortOrder::Asc
            } else {
                crate::query::SortOrder::Desc
            };
            sort_fields.push((field, order));
        }
        opts.sort = Some(sort_fields);
    }

    if let Some(lim) = limit {
        opts.limit = Some(expr_to_u64(lim)?);
    }
    if let Some(off) = offset {
        opts.skip = Some(expr_to_u64(&off.value)?);
    }

    let docs = db.find_with_options(&table, &where_json, &opts)?;

    let is_select_star = select
        .projection
        .iter()
        .any(|item| matches!(item, SelectItem::Wildcard(_)));
    if is_select_star {
        return Ok(SqlResult::Select(docs));
    }

    let projected = apply_projection(&docs, &select.projection)?;
    Ok(SqlResult::Select(projected))
}

// ---------------------------------------------------------------------------
// Aggregate SELECT (GROUP BY / aggregate functions)
// ---------------------------------------------------------------------------

fn execute_aggregate_select(
    db: &OxiDb,
    select: &ast::Select,
    order_by: &[OrderByExpr],
    limit: Option<&Expr>,
    offset: Option<&ast::Offset>,
) -> Result<SqlResult> {
    let table = extract_table_name(&select.from)?;
    let mut pipeline: Vec<Value> = Vec::new();

    // $match from WHERE
    if let Some(expr) = &select.selection {
        let where_json = translate_expr(expr)?;
        pipeline.push(json!({"$match": where_json}));
    }

    // $group
    let group_by_fields: Vec<String> = match &select.group_by {
        GroupByExpr::Expressions(exprs, _) => exprs
            .iter()
            .map(expr_to_field_name)
            .collect::<Result<Vec<_>>>()?,
        GroupByExpr::All(_) => Vec::new(),
    };

    let group_id = if group_by_fields.is_empty() {
        Value::Null
    } else if group_by_fields.len() == 1 {
        Value::String(format!("${}", group_by_fields[0]))
    } else {
        let mut obj = Map::new();
        for f in &group_by_fields {
            obj.insert(f.clone(), Value::String(format!("${f}")));
        }
        Value::Object(obj)
    };

    let mut group_obj = Map::new();
    group_obj.insert("_id".to_string(), group_id);

    // Map aggregate functions from SELECT
    for item in &select.projection {
        let (expr, alias) = match item {
            SelectItem::UnnamedExpr(e) => (e, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
            _ => continue,
        };

        match expr {
            Expr::Function(f) => {
                let func_name = f.name.to_string().to_uppercase();
                let output_name = alias.unwrap_or_else(|| func_name.to_lowercase());

                let acc = match func_name.as_str() {
                    "COUNT" => json!({"$sum": 1}),
                    "SUM" => {
                        let field = extract_function_field_arg(f)?;
                        json!({"$sum": format!("${field}")})
                    }
                    "AVG" => {
                        let field = extract_function_field_arg(f)?;
                        json!({"$avg": format!("${field}")})
                    }
                    "MIN" => {
                        let field = extract_function_field_arg(f)?;
                        json!({"$min": format!("${field}")})
                    }
                    "MAX" => {
                        let field = extract_function_field_arg(f)?;
                        json!({"$max": format!("${field}")})
                    }
                    _ => continue,
                };
                group_obj.insert(output_name, acc);
            }
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
                // GROUP BY column referenced in SELECT — skip, it's in _id
            }
            _ => {}
        }
    }

    pipeline.push(json!({"$group": group_obj}));

    // HAVING → second $match
    if let Some(having) = &select.having {
        let having_json = translate_expr(having)?;
        pipeline.push(json!({"$match": having_json}));
    }

    // ORDER BY → $sort
    if !order_by.is_empty() {
        let mut sort_obj = Map::new();
        for ob in order_by {
            let field = expr_to_field_name(&ob.expr)?;
            let dir = if ob.options.asc.unwrap_or(true) { 1 } else { -1 };
            sort_obj.insert(field, json!(dir));
        }
        pipeline.push(json!({"$sort": sort_obj}));
    }

    // OFFSET → $skip
    if let Some(off) = offset {
        pipeline.push(json!({"$skip": expr_to_u64(&off.value)?}));
    }

    // LIMIT → $limit
    if let Some(lim) = limit {
        pipeline.push(json!({"$limit": expr_to_u64(lim)?}));
    }

    let pipeline_json = Value::Array(pipeline);
    let results = db.aggregate(&table, &pipeline_json)?;

    // Flatten _id field back into the result for user-friendly output
    let flattened: Vec<Value> = results
        .into_iter()
        .map(|mut doc| {
            if let Some(id) = doc.get("_id").cloned()
                && let Some(obj) = doc.as_object_mut()
            {
                obj.remove("_id");
                match id {
                    Value::Object(id_map) => {
                        for (k, v) in id_map {
                            obj.insert(k, v);
                        }
                    }
                    Value::String(s) => {
                        if !group_by_fields.is_empty() {
                            obj.insert(group_by_fields[0].clone(), Value::String(s));
                        }
                    }
                    Value::Null => {}
                    other => {
                        if !group_by_fields.is_empty() {
                            obj.insert(group_by_fields[0].clone(), other);
                        }
                    }
                }
            }
            doc
        })
        .collect();

    Ok(SqlResult::Select(flattened))
}

// ---------------------------------------------------------------------------
// JOIN SELECT
// ---------------------------------------------------------------------------

fn execute_join_select(
    db: &OxiDb,
    select: &ast::Select,
    order_by: &[OrderByExpr],
    limit: Option<&Expr>,
    offset: Option<&ast::Offset>,
) -> Result<SqlResult> {
    let from = select
        .from
        .first()
        .ok_or_else(|| Error::InvalidQuery("missing FROM clause".into()))?;

    let left_table = extract_table_factor_name(&from.relation)?;
    let mut pipeline: Vec<Value> = Vec::new();

    // WHERE → $match (before join for efficiency)
    if let Some(expr) = &select.selection {
        let where_json = translate_expr(expr)?;
        pipeline.push(json!({"$match": where_json}));
    }

    for join in &from.joins {
        let right_table = extract_table_factor_name(&join.relation)?;

        let is_left = matches!(
            &join.join_operator,
            JoinOperator::LeftOuter(_) | JoinOperator::LeftSemi(_) | JoinOperator::LeftAnti(_)
        );

        let is_cross = matches!(&join.join_operator, JoinOperator::CrossJoin(_));

        let pairs = if is_cross {
            Vec::new()
        } else {
            let constraint = extract_join_operator_constraint(&join.join_operator)?;
            extract_join_conditions(&constraint)?
        };

        let as_field = format!("_{right_table}");

        if is_cross {
            // CROSS JOIN: lookup all docs from the right table
            pipeline.push(json!({
                "$lookup": {
                    "from": right_table,
                    "localField": "_cross_no_match_",
                    "foreignField": "_cross_no_match_",
                    "as": as_field
                }
            }));
        } else {
            // Use the first equality pair for $lookup, extra pairs for filtering
            let mut lookup_obj = json!({
                "from": right_table,
                "localField": pairs[0].local,
                "foreignField": pairs[0].foreign,
                "as": as_field
            });

            if pairs.len() > 1 {
                let extra_local: Vec<Value> = pairs[1..]
                    .iter()
                    .map(|p| Value::String(p.local.clone()))
                    .collect();
                let extra_foreign: Vec<Value> = pairs[1..]
                    .iter()
                    .map(|p| Value::String(p.foreign.clone()))
                    .collect();
                lookup_obj["localFields"] = Value::Array(extra_local);
                lookup_obj["foreignFields"] = Value::Array(extra_foreign);
            }

            pipeline.push(json!({"$lookup": lookup_obj}));
        }

        pipeline.push(json!({
            "$unwind": {
                "path": format!("${as_field}"),
                "preserveNullAndEmptyArrays": is_left
            }
        }));
    }

    // ORDER BY → $sort
    if !order_by.is_empty() {
        let mut sort_obj = Map::new();
        for ob in order_by {
            let field = expr_to_field_name(&ob.expr)?;
            let dir = if ob.options.asc.unwrap_or(true) { 1 } else { -1 };
            sort_obj.insert(field, json!(dir));
        }
        pipeline.push(json!({"$sort": sort_obj}));
    }

    // OFFSET → $skip
    if let Some(off) = offset {
        pipeline.push(json!({"$skip": expr_to_u64(&off.value)?}));
    }

    // LIMIT → $limit
    if let Some(lim) = limit {
        pipeline.push(json!({"$limit": expr_to_u64(lim)?}));
    }

    let pipeline_json = Value::Array(pipeline);
    let docs = db.aggregate(&left_table, &pipeline_json)?;

    let is_select_star = select
        .projection
        .iter()
        .any(|item| matches!(item, SelectItem::Wildcard(_)));
    if is_select_star {
        return Ok(SqlResult::Select(docs));
    }

    let projected = apply_projection(&docs, &select.projection)?;
    Ok(SqlResult::Select(projected))
}

fn extract_join_operator_constraint(op: &JoinOperator) -> Result<JoinConstraint> {
    match op {
        JoinOperator::Inner(c)
        | JoinOperator::LeftOuter(c)
        | JoinOperator::RightOuter(c)
        | JoinOperator::FullOuter(c, ..)
        | JoinOperator::LeftSemi(c)
        | JoinOperator::RightSemi(c)
        | JoinOperator::LeftAnti(c)
        | JoinOperator::RightAnti(c) => Ok(c.clone()),
        _ => Err(Error::InvalidQuery("unsupported JOIN type".into())),
    }
}

/// A single equality pair from a JOIN ON condition.
struct JoinFieldPair {
    local: String,
    foreign: String,
}

/// Extract all equality field pairs from a JOIN condition.
/// Supports: `ON a.x = b.x`, `ON a.x = b.x AND a.y = b.y`, and `USING (x, y)`.
fn extract_join_conditions(constraint: &JoinConstraint) -> Result<Vec<JoinFieldPair>> {
    match constraint {
        JoinConstraint::On(expr) => {
            let mut pairs = Vec::new();
            collect_join_equalities(expr, &mut pairs)?;
            if pairs.is_empty() {
                return Err(Error::InvalidQuery(
                    "JOIN ON requires at least one equality condition".into(),
                ));
            }
            Ok(pairs)
        }
        JoinConstraint::Using(columns) => {
            let pairs = columns
                .iter()
                .map(|col| {
                    let name = object_name_to_string(col);
                    JoinFieldPair {
                        local: name.clone(),
                        foreign: name,
                    }
                })
                .collect::<Vec<_>>();
            if pairs.is_empty() {
                return Err(Error::InvalidQuery(
                    "JOIN USING requires at least one column".into(),
                ));
            }
            Ok(pairs)
        }
        JoinConstraint::Natural => {
            Err(Error::InvalidQuery(
                "NATURAL JOIN is not supported — use explicit ON or USING".into(),
            ))
        }
        _ => Err(Error::InvalidQuery(
            "only JOIN ... ON and JOIN ... USING are supported".into(),
        )),
    }
}

/// Recursively collect `field = field` equalities from an AND tree.
fn collect_join_equalities(expr: &Expr, pairs: &mut Vec<JoinFieldPair>) -> Result<()> {
    match expr {
        Expr::BinaryOp { left, op: BinaryOperator::And, right } => {
            collect_join_equalities(left, pairs)?;
            collect_join_equalities(right, pairs)?;
        }
        Expr::BinaryOp { left, op: BinaryOperator::Eq, right } => {
            let left_field = expr_to_field_name(left)?;
            let right_field = expr_to_field_name(right)?;
            pairs.push(JoinFieldPair {
                local: left_field,
                foreign: right_field,
            });
        }
        Expr::Nested(inner) => {
            collect_join_equalities(inner, pairs)?;
        }
        _ => {
            return Err(Error::InvalidQuery(format!(
                "unsupported JOIN condition: {expr} — only AND-ed equalities are supported"
            )));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// INSERT
// ---------------------------------------------------------------------------

fn execute_insert(db: &OxiDb, insert: ast::Insert) -> Result<SqlResult> {
    let table = match &insert.table {
        TableObject::TableName(name) => object_name_to_string(name),
        _ => return Err(Error::InvalidQuery("unsupported INSERT target".into())),
    };

    let columns: Vec<String> = insert.columns.iter().map(|c| c.value.clone()).collect();

    let source = insert
        .source
        .ok_or_else(|| Error::InvalidQuery("INSERT missing VALUES".into()))?;

    let rows = match *source.body {
        SetExpr::Values(values) => values.rows,
        _ => {
            return Err(Error::InvalidQuery(
                "only INSERT ... VALUES is supported".into(),
            ))
        }
    };

    if rows.is_empty() {
        return Err(Error::InvalidQuery("INSERT with no rows".into()));
    }

    let mut docs: Vec<Value> = Vec::with_capacity(rows.len());
    for row in &rows {
        if row.len() != columns.len() {
            return Err(Error::InvalidQuery(
                "column count does not match value count".into(),
            ));
        }
        let mut obj = Map::new();
        for (col, val_expr) in columns.iter().zip(row.iter()) {
            obj.insert(col.clone(), translate_expr_to_value(val_expr)?);
        }
        docs.push(Value::Object(obj));
    }

    if docs.len() == 1 {
        let id = db.insert(&table, docs.into_iter().next().unwrap())?;
        Ok(SqlResult::Insert(vec![id]))
    } else {
        let ids = db.insert_many(&table, docs)?;
        Ok(SqlResult::Insert(ids))
    }
}

// ---------------------------------------------------------------------------
// UPDATE
// ---------------------------------------------------------------------------

fn execute_update(
    db: &OxiDb,
    table: ast::TableWithJoins,
    assignments: Vec<ast::Assignment>,
    selection: Option<Expr>,
) -> Result<SqlResult> {
    let table_name = extract_table_factor_name(&table.relation)?;

    let where_json = match selection {
        Some(expr) => translate_expr(&expr)?,
        None => json!({}),
    };

    let mut set_obj = Map::new();
    for assign in &assignments {
        let field = match &assign.target {
            AssignmentTarget::ColumnName(name) => object_name_to_string(name),
            AssignmentTarget::Tuple(names) => names
                .iter()
                .map(object_name_to_string)
                .collect::<Vec<_>>()
                .join("."),
        };
        let value = translate_expr_to_value(&assign.value)?;
        set_obj.insert(field, value);
    }

    let update_doc = json!({"$set": set_obj});
    let count = db.update(&table_name, &where_json, &update_doc)?;
    Ok(SqlResult::Update(count))
}

// ---------------------------------------------------------------------------
// DELETE
// ---------------------------------------------------------------------------

fn execute_delete(db: &OxiDb, delete: ast::Delete) -> Result<SqlResult> {
    let tables = match delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
    };
    let from = tables
        .into_iter()
        .next()
        .ok_or_else(|| Error::InvalidQuery("DELETE missing FROM".into()))?;
    let table_name = extract_table_factor_name(&from.relation)?;

    let where_json = match delete.selection {
        Some(expr) => translate_expr(&expr)?,
        None => json!({}),
    };

    let count = db.delete(&table_name, &where_json)?;
    Ok(SqlResult::Delete(count))
}

// ---------------------------------------------------------------------------
// DDL
// ---------------------------------------------------------------------------

fn execute_create_table(db: &OxiDb, create: ast::CreateTable) -> Result<SqlResult> {
    let name = object_name_to_string(&create.name);
    db.create_collection(&name)?;
    Ok(SqlResult::Ddl(format!("collection '{name}' created")))
}

fn execute_drop(
    db: &OxiDb,
    object_type: ObjectType,
    names: Vec<ObjectName>,
) -> Result<SqlResult> {
    if !matches!(object_type, ObjectType::Table) {
        return Err(Error::InvalidQuery(
            "only DROP TABLE is supported".into(),
        ));
    }
    for name in &names {
        let table = object_name_to_string(name);
        db.drop_collection(&table)?;
    }
    Ok(SqlResult::Ddl("table dropped".into()))
}

fn execute_create_index(db: &OxiDb, create: CreateIndex) -> Result<SqlResult> {
    let table = object_name_to_string(&create.table_name);

    let columns: Vec<String> = create
        .columns
        .iter()
        .map(|col| col.column.expr.to_string())
        .collect();

    if columns.is_empty() {
        return Err(Error::InvalidQuery(
            "CREATE INDEX requires at least one column".into(),
        ));
    }

    if columns.len() == 1 {
        db.create_index(&table, &columns[0])?;
        Ok(SqlResult::Ddl(format!("index on '{}' created", columns[0])))
    } else {
        let name = db.create_composite_index(&table, columns)?;
        Ok(SqlResult::Ddl(format!("composite index '{name}' created")))
    }
}

fn execute_show_tables(db: &OxiDb) -> Result<SqlResult> {
    let names = db.list_collections();
    let docs: Vec<Value> = names
        .into_iter()
        .map(|n| json!({"table_name": n}))
        .collect();
    Ok(SqlResult::Select(docs))
}

// ---------------------------------------------------------------------------
// Expression translation: SQL Expr → OxiDB query JSON
// ---------------------------------------------------------------------------

fn translate_expr(expr: &Expr) -> Result<Value> {
    match expr {
        Expr::BinaryOp { left, op, right } => translate_binary_op(left, op, right),
        Expr::Nested(inner) => translate_expr(inner),
        Expr::IsNull(expr) => {
            let field = expr_to_field_name(expr)?;
            Ok(json!({field: {"$exists": false}}))
        }
        Expr::IsNotNull(expr) => {
            let field = expr_to_field_name(expr)?;
            Ok(json!({field: {"$exists": true}}))
        }
        Expr::InList { expr, list, negated } => {
            let field = expr_to_field_name(expr)?;
            let values: Vec<Value> = list
                .iter()
                .map(translate_expr_to_value)
                .collect::<Result<_>>()?;
            if *negated {
                Ok(json!({field: {"$nin": values}}))
            } else {
                Ok(json!({field: {"$in": values}}))
            }
        }
        Expr::Between { expr, low, high, negated } => {
            let field = expr_to_field_name(expr)?;
            let low_val = translate_expr_to_value(low)?;
            let high_val = translate_expr_to_value(high)?;
            if *negated {
                Ok(json!({"$or": [{field.clone(): {"$lt": low_val}}, {field: {"$gt": high_val}}]}))
            } else {
                Ok(json!({field: {"$gte": low_val, "$lte": high_val}}))
            }
        }
        Expr::Like { expr, pattern, negated, .. } => {
            let field = expr_to_field_name(expr)?;
            let pattern_str = match pattern.as_ref() {
                Expr::Value(v) => value_with_span_to_string(v)?,
                _ => {
                    return Err(Error::InvalidQuery(
                        "LIKE pattern must be a string literal".into(),
                    ))
                }
            };
            let regex = like_to_regex(&pattern_str);
            if *negated {
                Ok(json!({field: {"$not": {"$regex": regex}}}))
            } else {
                Ok(json!({field: {"$regex": regex}}))
            }
        }
        _ => Err(Error::InvalidQuery(format!(
            "unsupported SQL expression: {expr}"
        ))),
    }
}

fn translate_binary_op(left: &Expr, op: &BinaryOperator, right: &Expr) -> Result<Value> {
    match op {
        BinaryOperator::And => {
            let lhs = translate_expr(left)?;
            let rhs = translate_expr(right)?;
            Ok(merge_and(lhs, rhs))
        }
        BinaryOperator::Or => {
            let lhs = translate_expr(left)?;
            let rhs = translate_expr(right)?;
            Ok(json!({"$or": [lhs, rhs]}))
        }
        BinaryOperator::Eq => {
            let field = expr_to_field_name(left)?;
            let val = translate_expr_to_value(right)?;
            Ok(json!({field: val}))
        }
        BinaryOperator::NotEq => {
            let field = expr_to_field_name(left)?;
            let val = translate_expr_to_value(right)?;
            Ok(json!({field: {"$ne": val}}))
        }
        BinaryOperator::Gt => {
            let field = expr_to_field_name(left)?;
            let val = translate_expr_to_value(right)?;
            Ok(json!({field: {"$gt": val}}))
        }
        BinaryOperator::GtEq => {
            let field = expr_to_field_name(left)?;
            let val = translate_expr_to_value(right)?;
            Ok(json!({field: {"$gte": val}}))
        }
        BinaryOperator::Lt => {
            let field = expr_to_field_name(left)?;
            let val = translate_expr_to_value(right)?;
            Ok(json!({field: {"$lt": val}}))
        }
        BinaryOperator::LtEq => {
            let field = expr_to_field_name(left)?;
            let val = translate_expr_to_value(right)?;
            Ok(json!({field: {"$lte": val}}))
        }
        _ => Err(Error::InvalidQuery(format!(
            "unsupported operator: {op}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Helper: merge two AND query objects
// ---------------------------------------------------------------------------

fn merge_and(left: Value, right: Value) -> Value {
    match (left, right) {
        (Value::Object(mut lm), Value::Object(rm)) => {
            let has_overlap = rm.keys().any(|key| lm.contains_key(key));
            if has_overlap {
                json!({"$and": [Value::Object(lm), Value::Object(rm)]})
            } else {
                for (k, v) in rm {
                    lm.insert(k, v);
                }
                Value::Object(lm)
            }
        }
        (l, r) => json!({"$and": [l, r]}),
    }
}

// ---------------------------------------------------------------------------
// Helper: LIKE pattern → regex
// ---------------------------------------------------------------------------

fn like_to_regex(pattern: &str) -> String {
    let mut regex = String::with_capacity(pattern.len() + 4);
    regex.push('^');
    for c in pattern.chars() {
        match c {
            '%' => regex.push_str(".*"),
            '_' => regex.push('.'),
            '.' | '^' | '$' | '*' | '+' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '\\'
            | '|' => {
                regex.push('\\');
                regex.push(c);
            }
            _ => regex.push(c),
        }
    }
    regex.push('$');
    regex
}

// ---------------------------------------------------------------------------
// Helper: extract field name from expression
// ---------------------------------------------------------------------------

fn expr_to_field_name(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => Ok(parts
            .iter()
            .map(|p| p.value.as_str())
            .collect::<Vec<_>>()
            .join(".")),
        _ => Err(Error::InvalidQuery(format!(
            "expected field name, got: {expr}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Helper: SQL literal/value → serde_json::Value
// ---------------------------------------------------------------------------

fn translate_expr_to_value(expr: &Expr) -> Result<Value> {
    match expr {
        Expr::Value(v) => sql_value_to_json(&v.value),
        Expr::UnaryOp {
            op: ast::UnaryOperator::Minus,
            expr,
        } => {
            let inner = translate_expr_to_value(expr)?;
            match inner {
                Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        Ok(json!(-i))
                    } else if let Some(f) = n.as_f64() {
                        Ok(serde_json::Number::from_f64(-f)
                            .map(Value::Number)
                            .unwrap_or(Value::Null))
                    } else {
                        Ok(Value::Null)
                    }
                }
                _ => Err(Error::InvalidQuery("cannot negate non-number".into())),
            }
        }
        Expr::Identifier(ident) => Ok(Value::String(ident.value.clone())),
        _ => Err(Error::InvalidQuery(format!(
            "unsupported value expression: {expr}"
        ))),
    }
}

fn sql_value_to_json(v: &ast::Value) -> Result<Value> {
    match v {
        ast::Value::Number(n, _) => {
            if let Ok(i) = n.parse::<i64>() {
                Ok(json!(i))
            } else if let Ok(f) = n.parse::<f64>() {
                Ok(serde_json::Number::from_f64(f)
                    .map(Value::Number)
                    .unwrap_or(Value::Null))
            } else {
                Err(Error::InvalidQuery(format!("invalid number: {n}")))
            }
        }
        ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s) => {
            Ok(Value::String(s.clone()))
        }
        ast::Value::Boolean(b) => Ok(Value::Bool(*b)),
        ast::Value::Null => Ok(Value::Null),
        _ => Err(Error::InvalidQuery(format!("unsupported SQL value: {v}"))),
    }
}

fn value_with_span_to_string(v: &ast::ValueWithSpan) -> Result<String> {
    match &v.value {
        ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s) => Ok(s.clone()),
        _ => Err(Error::InvalidQuery(format!(
            "expected string, got: {}",
            v.value
        ))),
    }
}

// ---------------------------------------------------------------------------
// Helper: extract table name from FROM clause
// ---------------------------------------------------------------------------

fn extract_table_name(from: &[TableWithJoins]) -> Result<String> {
    let table = from
        .first()
        .ok_or_else(|| Error::InvalidQuery("missing FROM clause".into()))?;
    extract_table_factor_name(&table.relation)
}

fn extract_table_factor_name(factor: &TableFactor) -> Result<String> {
    match factor {
        TableFactor::Table { name, .. } => Ok(object_name_to_string(name)),
        _ => Err(Error::InvalidQuery(
            "only simple table references are supported".into(),
        )),
    }
}

fn object_name_to_string(name: &ObjectName) -> String {
    name.0
        .iter()
        .filter_map(|p| p.as_ident().map(|i| i.value.as_str()))
        .collect::<Vec<_>>()
        .join(".")
}

// ---------------------------------------------------------------------------
// Helper: extract function argument as field name
// ---------------------------------------------------------------------------

fn extract_function_field_arg(f: &ast::Function) -> Result<String> {
    let args = match &f.args {
        ast::FunctionArguments::List(args) => &args.args,
        _ => return Err(Error::InvalidQuery("function requires arguments".into())),
    };

    if args.is_empty() {
        return Err(Error::InvalidQuery("function requires an argument".into()));
    }

    match &args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => expr_to_field_name(expr),
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
            Err(Error::InvalidQuery(
                "wildcard not valid for this function".into(),
            ))
        }
        _ => Err(Error::InvalidQuery(
            "unsupported function argument".into(),
        )),
    }
}

// ---------------------------------------------------------------------------
// Helper: extract u64 from expression (for LIMIT/OFFSET)
// ---------------------------------------------------------------------------

fn expr_to_u64(expr: &Expr) -> Result<u64> {
    match expr {
        Expr::Value(v) => match &v.value {
            ast::Value::Number(n, _) => n
                .parse::<u64>()
                .map_err(|_| Error::InvalidQuery(format!("expected non-negative integer: {n}"))),
            _ => Err(Error::InvalidQuery(format!(
                "expected integer, got: {expr}"
            ))),
        },
        _ => Err(Error::InvalidQuery(format!(
            "expected integer, got: {expr}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Projection: filter columns and apply aliases
// ---------------------------------------------------------------------------

fn apply_projection(docs: &[Value], items: &[SelectItem]) -> Result<Vec<Value>> {
    let mut result = Vec::with_capacity(docs.len());
    for doc in docs {
        let mut out = Map::new();
        // Always include _id if present
        if let Some(id) = doc.get("_id") {
            out.insert("_id".to_string(), id.clone());
        }
        for item in items {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let field = expr_to_field_name(expr)?;
                    if let Some(val) = doc.get(&field) {
                        out.insert(field, val.clone());
                    }
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let field = expr_to_field_name(expr)?;
                    if let Some(val) = doc.get(&field) {
                        out.insert(alias.value.clone(), val.clone());
                    }
                }
                SelectItem::Wildcard(_) => {
                    if let Some(obj) = doc.as_object() {
                        for (k, v) in obj {
                            out.insert(k.clone(), v.clone());
                        }
                    }
                }
                _ => {}
            }
        }
        result.push(Value::Object(out));
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::tempdir;

    fn temp_db() -> OxiDb {
        let dir = tempdir().unwrap();
        OxiDb::open(dir.path()).unwrap()
    }

    fn seed_users(db: &OxiDb) {
        db.insert("users", json!({"name": "Alice", "age": 30, "city": "NYC"}))
            .unwrap();
        db.insert("users", json!({"name": "Bob", "age": 25, "city": "LA"}))
            .unwrap();
        db.insert(
            "users",
            json!({"name": "Charlie", "age": 35, "city": "NYC"}),
        )
        .unwrap();
    }

    #[test]
    fn select_all() {
        let db = temp_db();
        seed_users(&db);
        let result = execute_sql(&db, "SELECT * FROM users").unwrap();
        match result {
            SqlResult::Select(docs) => assert_eq!(docs.len(), 3),
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn select_with_where() {
        let db = temp_db();
        seed_users(&db);
        let result = execute_sql(&db, "SELECT * FROM users WHERE age > 28").unwrap();
        match result {
            SqlResult::Select(docs) => {
                assert_eq!(docs.len(), 2);
                for doc in &docs {
                    assert!(doc["age"].as_i64().unwrap() > 28);
                }
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn select_with_order_limit_offset() {
        let db = temp_db();
        seed_users(&db);
        let result =
            execute_sql(&db, "SELECT * FROM users ORDER BY age ASC LIMIT 2 OFFSET 1").unwrap();
        match result {
            SqlResult::Select(docs) => {
                assert_eq!(docs.len(), 2);
                assert_eq!(docs[0]["name"], "Alice");
                assert_eq!(docs[1]["name"], "Charlie");
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn select_like() {
        let db = temp_db();
        seed_users(&db);
        let result = execute_sql(&db, "SELECT * FROM users WHERE name LIKE 'Al%'").unwrap();
        match result {
            SqlResult::Select(docs) => {
                assert_eq!(docs.len(), 1);
                assert_eq!(docs[0]["name"], "Alice");
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn select_in_list() {
        let db = temp_db();
        seed_users(&db);
        let result =
            execute_sql(&db, "SELECT * FROM users WHERE city IN ('NYC', 'SF')").unwrap();
        match result {
            SqlResult::Select(docs) => {
                assert_eq!(docs.len(), 2);
                for doc in &docs {
                    assert_eq!(doc["city"], "NYC");
                }
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn select_is_null() {
        let db = temp_db();
        db.insert("users", json!({"name": "Alice", "email": "alice@test.com"}))
            .unwrap();
        db.insert("users", json!({"name": "Bob"})).unwrap();
        let result = execute_sql(&db, "SELECT * FROM users WHERE email IS NULL").unwrap();
        match result {
            SqlResult::Select(docs) => {
                assert_eq!(docs.len(), 1);
                assert_eq!(docs[0]["name"], "Bob");
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn select_is_not_null() {
        let db = temp_db();
        db.insert("users", json!({"name": "Alice", "email": "alice@test.com"}))
            .unwrap();
        db.insert("users", json!({"name": "Bob"})).unwrap();
        let result = execute_sql(&db, "SELECT * FROM users WHERE email IS NOT NULL").unwrap();
        match result {
            SqlResult::Select(docs) => {
                assert_eq!(docs.len(), 1);
                assert_eq!(docs[0]["name"], "Alice");
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn select_between() {
        let db = temp_db();
        seed_users(&db);
        let result =
            execute_sql(&db, "SELECT * FROM users WHERE age BETWEEN 26 AND 34").unwrap();
        match result {
            SqlResult::Select(docs) => {
                assert_eq!(docs.len(), 1);
                assert_eq!(docs[0]["name"], "Alice");
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn select_with_and_or() {
        let db = temp_db();
        seed_users(&db);
        let result = execute_sql(
            &db,
            "SELECT * FROM users WHERE (city = 'NYC' AND age > 32) OR name = 'Bob'",
        )
        .unwrap();
        match result {
            SqlResult::Select(docs) => {
                assert_eq!(docs.len(), 2);
                let names: Vec<&str> = docs.iter().map(|d| d["name"].as_str().unwrap()).collect();
                assert!(names.contains(&"Charlie"));
                assert!(names.contains(&"Bob"));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn select_with_alias() {
        let db = temp_db();
        seed_users(&db);
        let result =
            execute_sql(&db, "SELECT name AS user_name FROM users WHERE age = 30").unwrap();
        match result {
            SqlResult::Select(docs) => {
                assert_eq!(docs.len(), 1);
                assert_eq!(docs[0]["user_name"], "Alice");
                assert!(docs[0].get("name").is_none());
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn select_count_star() {
        let db = temp_db();
        seed_users(&db);
        let result = execute_sql(&db, "SELECT COUNT(*) AS total FROM users").unwrap();
        match result {
            SqlResult::Select(docs) => {
                assert_eq!(docs.len(), 1);
                assert_eq!(docs[0]["total"], 3);
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn select_group_by_with_avg() {
        let db = temp_db();
        seed_users(&db);
        let result = execute_sql(
            &db,
            "SELECT city, AVG(age) AS avg_age FROM users GROUP BY city",
        )
        .unwrap();
        match result {
            SqlResult::Select(docs) => {
                assert_eq!(docs.len(), 2);
                for doc in &docs {
                    let city = doc["city"].as_str().unwrap();
                    match city {
                        "NYC" => {
                            let avg = doc["avg_age"].as_f64().unwrap();
                            assert!((avg - 32.5).abs() < 0.01);
                        }
                        "LA" => {
                            let avg = doc["avg_age"].as_f64().unwrap();
                            assert!((avg - 25.0).abs() < 0.01);
                        }
                        _ => panic!("unexpected city: {city}"),
                    }
                }
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn insert_single_row() {
        let db = temp_db();
        let result =
            execute_sql(&db, "INSERT INTO items (name, price) VALUES ('Widget', 9.99)").unwrap();
        match result {
            SqlResult::Insert(ids) => {
                assert_eq!(ids.len(), 1);
                let doc = db.find_one("items", &json!({})).unwrap().unwrap();
                assert_eq!(doc["name"], "Widget");
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn insert_multiple_rows() {
        let db = temp_db();
        let result = execute_sql(
            &db,
            "INSERT INTO items (name, price) VALUES ('A', 1), ('B', 2), ('C', 3)",
        )
        .unwrap();
        match result {
            SqlResult::Insert(ids) => {
                assert_eq!(ids.len(), 3);
                let docs = db.find("items", &json!({})).unwrap();
                assert_eq!(docs.len(), 3);
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn update_with_where() {
        let db = temp_db();
        seed_users(&db);
        let result =
            execute_sql(&db, "UPDATE users SET age = 31 WHERE name = 'Alice'").unwrap();
        match result {
            SqlResult::Update(count) => {
                assert_eq!(count, 1);
                let doc = db
                    .find_one("users", &json!({"name": "Alice"}))
                    .unwrap()
                    .unwrap();
                assert_eq!(doc["age"], 31);
            }
            _ => panic!("expected Update"),
        }
    }

    #[test]
    fn delete_with_where() {
        let db = temp_db();
        seed_users(&db);
        let result = execute_sql(&db, "DELETE FROM users WHERE city = 'LA'").unwrap();
        match result {
            SqlResult::Delete(count) => {
                assert_eq!(count, 1);
                let docs = db.find("users", &json!({})).unwrap();
                assert_eq!(docs.len(), 2);
            }
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn create_and_drop_table() {
        let db = temp_db();
        let result =
            execute_sql(&db, "CREATE TABLE products (id INT, name TEXT)").unwrap();
        assert!(matches!(result, SqlResult::Ddl(_)));

        let collections = db.list_collections();
        assert!(collections.contains(&"products".to_string()));

        let result = execute_sql(&db, "DROP TABLE products").unwrap();
        assert!(matches!(result, SqlResult::Ddl(_)));
    }

    #[test]
    fn show_tables() {
        let db = temp_db();
        db.create_collection("alpha").unwrap();
        db.create_collection("beta").unwrap();
        let result = execute_sql(&db, "SHOW TABLES").unwrap();
        match result {
            SqlResult::Select(docs) => {
                assert_eq!(docs.len(), 2);
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn create_index_via_sql() {
        let db = temp_db();
        seed_users(&db);
        let result =
            execute_sql(&db, "CREATE INDEX idx_name ON users (name)").unwrap();
        assert!(matches!(result, SqlResult::Ddl(_)));

        let indexes = db.list_indexes("users").unwrap();
        assert!(indexes.iter().any(|i| i.fields == vec!["name"]));
    }

    #[test]
    fn invalid_sql_returns_error() {
        let db = temp_db();
        let result = execute_sql(&db, "SELEC * FORM users");
        assert!(result.is_err());
    }

    #[test]
    fn unsupported_statement_returns_error() {
        let db = temp_db();
        let result = execute_sql(&db, "ALTER TABLE users ADD COLUMN email TEXT");
        assert!(result.is_err());
    }

    #[test]
    fn like_to_regex_conversion() {
        assert_eq!(like_to_regex("%test%"), "^.*test.*$");
        assert_eq!(like_to_regex("hello_world"), "^hello.world$");
        assert_eq!(like_to_regex("test%"), "^test.*$");
        assert_eq!(like_to_regex("%test"), "^.*test$");
        assert_eq!(like_to_regex("exact"), "^exact$");
    }
}
