//! PostgREST-compatible surface over the **SQL engine** (ADR-0019, Phase 2b/2c).
//!
//! The parent router sends `/rest/v1/{table}` here instead of to the document
//! handlers when `{table}` names a SQL table (see
//! [`sql_bridge::sql_table_exists`](crate::sql_bridge::sql_table_exists)). The
//! same PostgREST URL grammar is translated to **parameterized SQL** and run
//! through the SQL engine — so a client cannot tell whether a table is backed
//! by the document or the relational engine.
//!
//! # Safety
//!
//! - **Values are always bound as parameters** (`?` placeholders), never
//!   interpolated — no SQL injection through filter/insert values.
//! - **Identifiers** (table, column, alias, order key) are validated against
//!   `^[A-Za-z_][A-Za-z0-9_]*$` and rejected otherwise, since an identifier
//!   cannot be a bind parameter. This is the injection guard for names.
//! - Authorization is **role-based** (the parent `rest_permitted` gate: a Read
//!   token may only `GET`; writes need `ReadWrite`) plus the SQL engine's own
//!   read-only enforcement on `GET`. Unlike the document path, the SQL engine
//!   has no per-row security-rules layer — SQL tables rely on RBAC, exactly as
//!   the existing `/api/sql` endpoint does.
//!
//! Resource embedding (`select=*,related(cols)`, Phase 2c) infers the join from
//! the catalog's foreign keys via
//! [`sql_bridge::sql_foreign_keys`](crate::sql_bridge::sql_foreign_keys): a FK
//! from the current table → the target is a belongs-to (single object); a FK
//! from the target → the current table is a has-many (array). An explicit
//! `related!fk(...)` names the FK column. Each embed runs one batched secondary
//! `SELECT … WHERE fk IN (…)` and is stitched in Rust (not a JOIN — avoids
//! column-name collisions and reuses the row/projection code). Write
//! representation over SQL is still a future phase.

use std::collections::{HashMap, HashSet};

use serde_json::{Value, json};

use super::postgrest::{
    Embed, apply_select, coerce, max_rows, parse_select_plan, project_doc, split_pairs,
    split_top_commas,
};
use crate::s3::http::HttpResponse;

type PgResult<T> = Result<T, (u16, String)>;

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// `GET /rest/v1/{table}` over a SQL table — parameterized SELECT.
///
/// A `select` with resource embeds (`select=*,related(cols)`) takes the embed
/// path: fetch the base rows with `SELECT *`, resolve each embed via a batched
/// secondary query keyed on catalog foreign keys, then project in Rust. A plain
/// `select` is projected at the SQL level (more efficient).
pub(super) fn handle_get(db: &str, table: &str, query: &str) -> HttpResponse {
    let select = select_param(query);
    let plan = match parse_select_plan(select.as_deref()) {
        Ok(p) => p,
        Err((s, m)) => return err(s, &m),
    };
    let has_embed = plan.as_ref().is_some_and(|p| !p.embeds.is_empty());

    let force_star = has_embed;
    let (sql, params, offset) = match build_read(table, query, force_star) {
        Ok(x) => x,
        Err((s, m)) => return err(s, &m),
    };
    let base = match crate::sql_bridge::execute_json_in(db, &sql, Some(&Value::Array(params)), true)
    {
        Ok(v) => v,
        Err(e) => return err(400, &e),
    };
    let mut rows = rows_to_objects(&base);

    if let Some(plan) = plan.filter(|p| !p.embeds.is_empty()) {
        for embed in &plan.embeds {
            if let Err((s, m)) = resolve_embed_sql(db, table, &mut rows, embed) {
                return err(s, &m);
            }
        }
        rows = rows.into_iter().map(|r| project_doc(r, &plan)).collect();
    }

    let n = rows.len();
    let range = if n == 0 {
        "*/*".to_string()
    } else {
        format!("{offset}-{}/*", offset + n as u64 - 1)
    };
    super::json_response(200, "OK", Value::Array(rows)).with_header("Content-Range", &range)
}

/// Extract the `select` value from the query string, if present.
fn select_param(query: &str) -> Option<String> {
    split_pairs(query)
        .into_iter()
        .find(|(k, _)| k == "select")
        .map(|(_, v)| v)
}

/// `POST /rest/v1/{table}` over a SQL table — one parameterized INSERT per row.
/// Returns `201` with an empty array (minimal); write representation over SQL
/// is a future phase.
pub(super) fn handle_post(db: &str, table: &str, body: &[u8]) -> HttpResponse {
    if let Err((s, m)) = ident(table) {
        return err(s, &m);
    }
    let parsed = match serde_json::from_slice::<Value>(body) {
        Ok(v) => v,
        Err(_) => return err(400, "invalid JSON body"),
    };
    let rows: Vec<Value> = match parsed {
        Value::Array(a) => a,
        obj @ Value::Object(_) => vec![obj],
        _ => return err(400, "body must be an object or array of objects"),
    };
    if rows.is_empty() {
        return err(400, "empty insert");
    }
    for row in &rows {
        let Value::Object(map) = row else {
            return err(400, "each row must be a JSON object");
        };
        if map.is_empty() {
            return err(400, "row has no columns");
        }
        let mut cols = Vec::new();
        let mut placeholders = Vec::new();
        let mut params = Vec::new();
        for (k, v) in map {
            if let Err((s, m)) = ident(k) {
                return err(s, &m);
            }
            cols.push(k.clone());
            placeholders.push("?");
            params.push(v.clone());
        }
        let sql = format!(
            "INSERT INTO {table} ({}) VALUES ({})",
            cols.join(", "),
            placeholders.join(", ")
        );
        if let Err(e) =
            crate::sql_bridge::execute_json_in(db, &sql, Some(&Value::Array(params)), false)
        {
            return err(400, &e);
        }
    }
    super::json_response(201, "Created", json!([]))
}

/// `PATCH /rest/v1/{table}?<filters>` over a SQL table — parameterized UPDATE.
pub(super) fn handle_patch(db: &str, table: &str, query: &str, body: &[u8]) -> HttpResponse {
    if let Err((s, m)) = ident(table) {
        return err(s, &m);
    }
    let set = match serde_json::from_slice::<Value>(body) {
        Ok(Value::Object(m)) if !m.is_empty() => m,
        Ok(Value::Object(_)) => return err(400, "PATCH body has no columns"),
        Ok(_) => return err(400, "PATCH body must be a JSON object"),
        Err(_) => return err(400, "invalid JSON body"),
    };

    let mut assignments = Vec::new();
    let mut params = Vec::new();
    for (k, v) in &set {
        if let Err((s, m)) = ident(k) {
            return err(s, &m);
        }
        assignments.push(format!("{k} = ?"));
        params.push(v.clone());
    }
    let (where_sql, mut where_params) = match build_where(query) {
        Ok(x) => x,
        Err((s, m)) => return err(s, &m),
    };
    params.append(&mut where_params);

    let sql = format!("UPDATE {table} SET {}{}", assignments.join(", "), where_sql);
    match crate::sql_bridge::execute_json_in(db, &sql, Some(&Value::Array(params)), false) {
        Ok(_) => super::json_response(200, "OK", json!([])),
        Err(e) => err(400, &e),
    }
}

/// `DELETE /rest/v1/{table}?<filters>` over a SQL table — parameterized DELETE.
pub(super) fn handle_delete(db: &str, table: &str, query: &str) -> HttpResponse {
    if let Err((s, m)) = ident(table) {
        return err(s, &m);
    }
    let (where_sql, params) = match build_where(query) {
        Ok(x) => x,
        Err((s, m)) => return err(s, &m),
    };
    let sql = format!("DELETE FROM {table}{where_sql}");
    match crate::sql_bridge::execute_json_in(db, &sql, Some(&Value::Array(params)), false) {
        Ok(_) => super::json_response(204, "No Content", json!(null)),
        Err(e) => err(400, &e),
    }
}

// ---------------------------------------------------------------------------
// URL grammar → parameterized SQL
// ---------------------------------------------------------------------------

/// Build a `SELECT` and its bind parameters from the query string. Returns
/// `(sql, params, offset)` — offset is echoed into `Content-Range`.
///
/// `force_star` selects `*` regardless of the `select` param — used by the
/// embed path, which needs every column (including join keys) resident before
/// stitching, then projects down in Rust.
fn build_read(table: &str, query: &str, force_star: bool) -> PgResult<(String, Vec<Value>, u64)> {
    ident(table)?;
    let mut conds = Vec::new();
    let mut params = Vec::new();
    let mut select: Option<String> = None;
    let mut order: Option<String> = None;
    let mut limit: Option<u64> = None;
    let mut offset: u64 = 0;

    for (key, val) in split_pairs(query) {
        match key.as_str() {
            "db" => {}
            "select" => select = Some(val),
            "order" => order = Some(build_order(&val)?),
            "limit" => {
                limit = Some(
                    val.parse()
                        .map_err(|_| (400, "invalid 'limit'".to_string()))?,
                )
            }
            "offset" => {
                offset = val
                    .parse()
                    .map_err(|_| (400, "invalid 'offset'".to_string()))?
            }
            "or" => conds.push(group("OR", &val, &mut params)?),
            "and" => conds.push(group("AND", &val, &mut params)?),
            col => conds.push(col_filter(col, &val, &mut params)?),
        }
    }

    let cols = match select {
        _ if force_star => "*".to_string(),
        Some(s) => build_select_cols(&s)?,
        None => "*".to_string(),
    };
    let mut sql = format!("SELECT {cols} FROM {table}");
    if !conds.is_empty() {
        sql.push_str(&format!(" WHERE {}", conds.join(" AND ")));
    }
    if let Some(o) = order {
        sql.push_str(&format!(" ORDER BY {o}"));
    }
    let cap = max_rows();
    let lim = limit.map_or(cap, |l| l.min(cap));
    sql.push_str(&format!(" LIMIT {lim}"));
    if offset > 0 {
        sql.push_str(&format!(" OFFSET {offset}"));
    }
    Ok((sql, params, offset))
}

/// Build a leading `" WHERE ..."` clause (empty string if no filters) and its
/// bind parameters, for PATCH/DELETE. Reserved modifiers are ignored.
fn build_where(query: &str) -> PgResult<(String, Vec<Value>)> {
    let mut conds = Vec::new();
    let mut params = Vec::new();
    for (key, val) in split_pairs(query) {
        match key.as_str() {
            "db" | "select" | "order" | "limit" | "offset" => {}
            "or" => conds.push(group("OR", &val, &mut params)?),
            "and" => conds.push(group("AND", &val, &mut params)?),
            col => conds.push(col_filter(col, &val, &mut params)?),
        }
    }
    let clause = if conds.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conds.join(" AND "))
    };
    Ok((clause, params))
}

/// Translate one `col=op.value` (optionally `not.`) filter to a SQL predicate,
/// pushing bound values onto `params`.
fn col_filter(col: &str, spec: &str, params: &mut Vec<Value>) -> PgResult<String> {
    ident(col)?;
    let (negate, rest) = match spec.strip_prefix("not.") {
        Some(r) => (true, r),
        None => (false, spec),
    };
    let (op, arg) = rest
        .split_once('.')
        .ok_or((400, format!("filter must be 'op.value', got '{spec}'")))?;

    let frag = match op {
        "eq" => bind(params, coerce(arg), format!("{col} = ?")),
        "neq" => bind(params, coerce(arg), format!("{col} <> ?")),
        "gt" => bind(params, coerce(arg), format!("{col} > ?")),
        "gte" => bind(params, coerce(arg), format!("{col} >= ?")),
        "lt" => bind(params, coerce(arg), format!("{col} < ?")),
        "lte" => bind(params, coerce(arg), format!("{col} <= ?")),
        "like" => bind(params, json!(like_to_sql(arg)), format!("{col} LIKE ?")),
        "ilike" => bind(
            params,
            json!(like_to_sql(arg).to_lowercase()),
            format!("LOWER({col}) LIKE ?"),
        ),
        "in" => {
            let list = parse_in_list(arg);
            let placeholders = vec!["?"; list.len()].join(", ");
            for v in list {
                params.push(v);
            }
            format!("{col} IN ({placeholders})")
        }
        "is" => match arg {
            "null" => format!("{col} IS NULL"),
            "true" => bind(params, json!(true), format!("{col} = ?")),
            "false" => bind(params, json!(false), format!("{col} = ?")),
            _ => return Err((400, format!("'is' expects null/true/false, got '{arg}'"))),
        },
        "match" | "imatch" => {
            return Err((
                400,
                "match/imatch are not supported over the SQL engine".to_string(),
            ));
        }
        other => return Err((400, format!("unsupported operator '{other}'"))),
    };
    Ok(if negate {
        format!("NOT ({frag})")
    } else {
        frag
    })
}

/// `or=(...)`/`and=(...)` → `( c1 OR c2 )` / `( c1 AND c2 )`. Nestable.
fn group(kw: &str, raw: &str, params: &mut Vec<Value>) -> PgResult<String> {
    let inner = raw
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .ok_or((400, format!("{kw} group must be '(...)'")))?;
    let mut parts = Vec::new();
    for cond in split_top_commas(inner) {
        parts.push(dotted_condition(cond.trim(), params)?);
    }
    Ok(format!("({})", parts.join(&format!(" {kw} "))))
}

/// A condition inside a logic group: `col.op.value` or a nested `or(...)`/`and(...)`.
fn dotted_condition(cond: &str, params: &mut Vec<Value>) -> PgResult<String> {
    if let Some(rest) = cond.strip_prefix("or") {
        if rest.starts_with('(') {
            return group("OR", rest, params);
        }
    }
    if let Some(rest) = cond.strip_prefix("and") {
        if rest.starts_with('(') {
            return group("AND", rest, params);
        }
    }
    let (col, spec) = cond.split_once('.').ok_or((
        400,
        format!("condition must be 'col.op.value', got '{cond}'"),
    ))?;
    col_filter(col, spec, params)
}

/// `select=name,price` → `name, price`; `select=n:name` → `name AS n`. A `*`
/// stays `*`. An embed (`rel(...)`) is rejected — SQL embedding is a future
/// phase.
fn build_select_cols(spec: &str) -> PgResult<String> {
    let mut out = Vec::new();
    for item in split_top_commas(spec) {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        if item == "*" {
            out.push("*".to_string());
            continue;
        }
        if item.contains('(') {
            // Embeds are handled on the embed path (see `handle_get`), which
            // fetches with `SELECT *`; reaching here means a malformed select.
            return Err((400, format!("malformed select item '{item}'")));
        }
        match item.split_once(':') {
            Some((alias, col)) => {
                ident(alias)?;
                ident(col)?;
                out.push(format!("{col} AS {alias}"));
            }
            None => {
                ident(item)?;
                out.push(item.to_string());
            }
        }
    }
    if out.is_empty() {
        Ok("*".to_string())
    } else {
        Ok(out.join(", "))
    }
}

/// `order=price.desc,name` → `price DESC, name ASC`. `nullsfirst`/`nullslast`
/// are accepted and ignored.
fn build_order(spec: &str) -> PgResult<String> {
    let mut out = Vec::new();
    for term in spec.split(',').filter(|s| !s.is_empty()) {
        let mut it = term.split('.');
        let col = it
            .next()
            .filter(|c| !c.is_empty())
            .ok_or((400, "empty order column".to_string()))?;
        ident(col)?;
        let mut dir = "ASC";
        for d in it {
            match d {
                "asc" => dir = "ASC",
                "desc" => dir = "DESC",
                "nullsfirst" | "nullslast" => {}
                other => return Err((400, format!("bad order directive '{other}'"))),
            }
        }
        out.push(format!("{col} {dir}"));
    }
    Ok(out.join(", "))
}

// ---------------------------------------------------------------------------
// Resource embedding (Phase 2c) — related rows via catalog foreign keys
// ---------------------------------------------------------------------------

/// The relationship between the current table and an embed target, resolved
/// from the catalog's foreign keys.
enum Rel {
    /// The current table's `local` column references `target.parent_col`
    /// (embed a single object).
    BelongsTo { local: String, parent_col: String },
    /// The target's `child_col` references the current table's `parent_key`
    /// (embed an array).
    HasMany {
        child_col: String,
        parent_key: String,
    },
}

/// Attach one embed's related rows to every base row in place, running one
/// batched secondary query keyed on the catalog foreign key that links the
/// tables (a `$lookup`-style stitch rather than a JOIN — no column-name
/// collisions, and `rows_to_objects`/projection are reused verbatim).
fn resolve_embed_sql(db: &str, current: &str, rows: &mut [Value], embed: &Embed) -> PgResult<()> {
    ident(&embed.target)?;
    let cur_fks = crate::sql_bridge::sql_foreign_keys(db, current);
    let tgt_fks = crate::sql_bridge::sql_foreign_keys(db, &embed.target);

    let rel = match &embed.hint {
        Some(h) => cur_fks
            .iter()
            .find(|(c, pt, _)| c == h && *pt == embed.target)
            .map(|(local, _, pcol)| Rel::BelongsTo {
                local: local.clone(),
                parent_col: pcol.clone(),
            })
            .or_else(|| {
                tgt_fks
                    .iter()
                    .find(|(c, pt, _)| c == h && *pt == current)
                    .map(|(child, _, pkey)| Rel::HasMany {
                        child_col: child.clone(),
                        parent_key: pkey.clone(),
                    })
            })
            .ok_or((
                400,
                format!(
                    "no foreign key '{h}' relates '{current}' and '{}'",
                    embed.target
                ),
            ))?,
        None => cur_fks
            .iter()
            .find(|(_, pt, _)| *pt == embed.target)
            .map(|(local, _, pcol)| Rel::BelongsTo {
                local: local.clone(),
                parent_col: pcol.clone(),
            })
            .or_else(|| {
                tgt_fks
                    .iter()
                    .find(|(_, pt, _)| *pt == current)
                    .map(|(child, _, pkey)| Rel::HasMany {
                        child_col: child.clone(),
                        parent_key: pkey.clone(),
                    })
            })
            .ok_or((
                400,
                format!(
                    "no foreign key relates '{current}' and '{}'; add a hint like {}!<fk>",
                    embed.target, embed.target
                ),
            ))?,
    };

    match rel {
        Rel::BelongsTo { local, parent_col } => {
            ident(&parent_col)?;
            let ids = distinct_values(rows, &local);
            let mut by_key: HashMap<String, Value> = HashMap::new();
            if !ids.is_empty() {
                let full = fetch_in(db, &embed.target, &parent_col, ids)?;
                let projected = apply_select(full.clone(), embed.child.as_deref())?;
                for (f, p) in full.iter().zip(projected) {
                    if let Some(kv) = f.get(&parent_col) {
                        by_key.insert(key(kv), p);
                    }
                }
            }
            for r in rows.iter_mut() {
                let val = r
                    .get(&local)
                    .and_then(|lv| by_key.get(&key(lv)).cloned())
                    .unwrap_or(Value::Null);
                if let Value::Object(m) = r {
                    m.insert(embed.out.clone(), val);
                }
            }
        }
        Rel::HasMany {
            child_col,
            parent_key,
        } => {
            ident(&child_col)?;
            let keys = distinct_values(rows, &parent_key);
            let mut groups: HashMap<String, Vec<Value>> = HashMap::new();
            if !keys.is_empty() {
                let full = fetch_in(db, &embed.target, &child_col, keys)?;
                let projected = apply_select(full.clone(), embed.child.as_deref())?;
                for (f, p) in full.iter().zip(projected) {
                    if let Some(cv) = f.get(&child_col) {
                        groups.entry(key(cv)).or_default().push(p);
                    }
                }
            }
            for r in rows.iter_mut() {
                let arr = r
                    .get(&parent_key)
                    .and_then(|pv| groups.get(&key(pv)).cloned())
                    .unwrap_or_default();
                if let Value::Object(m) = r {
                    m.insert(embed.out.clone(), Value::Array(arr));
                }
            }
        }
    }
    Ok(())
}

/// Distinct non-null values of `field` across `rows`, preserving first-seen
/// order.
fn distinct_values(rows: &[Value], field: &str) -> Vec<Value> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for r in rows {
        if let Some(v) = r.get(field) {
            if !v.is_null() && seen.insert(key(v)) {
                out.push(v.clone());
            }
        }
    }
    out
}

/// `SELECT * FROM {table} WHERE {col} IN (?, …)` with `vals` bound, as row
/// objects. `table`/`col` come from the validated catalog.
fn fetch_in(db: &str, table: &str, col: &str, vals: Vec<Value>) -> PgResult<Vec<Value>> {
    let placeholders = vec!["?"; vals.len()].join(", ");
    let sql = format!("SELECT * FROM {table} WHERE {col} IN ({placeholders})");
    let res = crate::sql_bridge::execute_json_in(db, &sql, Some(&Value::Array(vals)), true)
        .map_err(|e| (400, e))?;
    Ok(rows_to_objects(&res))
}

/// A stable string key for joining on a value (numbers and strings alike).
fn key(v: &Value) -> String {
    v.to_string()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Push a bound value and return the fragment (reads better inline above).
fn bind(params: &mut Vec<Value>, v: Value, frag: String) -> String {
    params.push(v);
    frag
}

/// PostgREST `in.(a,b,c)` → coerced values.
fn parse_in_list(arg: &str) -> Vec<Value> {
    let inner = arg.trim_start_matches('(').trim_end_matches(')');
    split_top_commas(inner)
        .into_iter()
        .map(|s| coerce(s.trim()))
        .collect()
}

/// PostgREST `like`/`ilike` use `*` as the wildcard; SQL `LIKE` uses `%`.
fn like_to_sql(pattern: &str) -> String {
    pattern.replace('*', "%")
}

/// Reject anything that is not a bare SQL identifier — the injection guard for
/// names, which cannot be bind parameters.
fn ident(s: &str) -> PgResult<()> {
    let ok = !s.is_empty()
        && s.chars().enumerate().all(|(i, c)| {
            if i == 0 {
                c.is_ascii_alphabetic() || c == '_'
            } else {
                c.is_ascii_alphanumeric() || c == '_'
            }
        });
    if ok {
        Ok(())
    } else {
        Err((400, format!("invalid identifier '{s}'")))
    }
}

/// Turn the SQL engine's `{columns, rows}` result (an array of statement
/// results) into a PostgREST-style array of row objects.
fn rows_to_objects(result: &Value) -> Vec<Value> {
    let first = result.as_array().and_then(|a| a.first());
    let cols = first
        .and_then(|o| o.get("columns"))
        .and_then(|c| c.as_array());
    let rows = first.and_then(|o| o.get("rows")).and_then(|r| r.as_array());
    let (Some(cols), Some(rows)) = (cols, rows) else {
        return Vec::new();
    };
    rows.iter()
        .map(|row| {
            let mut obj = serde_json::Map::new();
            if let Some(cells) = row.as_array() {
                for (i, c) in cols.iter().enumerate() {
                    if let Some(name) = c.as_str() {
                        obj.insert(
                            name.to_string(),
                            cells.get(i).cloned().unwrap_or(Value::Null),
                        );
                    }
                }
            }
            Value::Object(obj)
        })
        .collect()
}

fn err(status: u16, message: &str) -> HttpResponse {
    let text = match status {
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        _ => "Internal Server Error",
    };
    super::json_response(status, text, json!({ "message": message }))
}

// ---------------------------------------------------------------------------
// Tests — SQL string generation (no engine required)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn read(q: &str) -> (String, Vec<Value>) {
        let (sql, params, _) = build_read("products", q, false).unwrap();
        (sql, params)
    }

    #[test]
    fn plain_select_all_has_limit_cap() {
        let (sql, params) = read("");
        assert_eq!(sql, "SELECT * FROM products LIMIT 1000");
        assert!(params.is_empty());
    }

    #[test]
    fn filters_are_parameterized() {
        let (sql, params) = read("price=gt.100&status=eq.active");
        assert_eq!(
            sql,
            "SELECT * FROM products WHERE price > ? AND status = ? LIMIT 1000"
        );
        assert_eq!(params, vec![json!(100), json!("active")]);
    }

    #[test]
    fn select_alias_and_order_and_paging() {
        let (sql, params) = read("select=n:name,price&order=price.desc,name&limit=20&offset=40");
        assert_eq!(
            sql,
            "SELECT name AS n, price FROM products ORDER BY price DESC, name ASC LIMIT 20 OFFSET 40"
        );
        assert!(params.is_empty());
    }

    #[test]
    fn limit_is_capped() {
        let (sql, _) = read("limit=99999");
        assert!(sql.ends_with("LIMIT 1000"));
    }

    #[test]
    fn in_list_and_null_and_negation() {
        let (sql, params) = read("id=in.(1,2,3)&deleted=is.null&status=not.eq.done");
        assert_eq!(
            sql,
            "SELECT * FROM products WHERE id IN (?, ?, ?) AND deleted IS NULL AND NOT (status = ?) LIMIT 1000"
        );
        assert_eq!(params, vec![json!(1), json!(2), json!(3), json!("done")]);
    }

    #[test]
    fn like_wildcard_becomes_percent() {
        let (sql, params) = read("name=like.*jo*");
        assert_eq!(sql, "SELECT * FROM products WHERE name LIKE ? LIMIT 1000");
        assert_eq!(params, vec![json!("%jo%")]);
    }

    #[test]
    fn ilike_lowers_both_sides() {
        let (sql, params) = read("name=ilike.Jo*");
        assert_eq!(
            sql,
            "SELECT * FROM products WHERE LOWER(name) LIKE ? LIMIT 1000"
        );
        assert_eq!(params, vec![json!("jo%")]);
    }

    #[test]
    fn or_group_parameterized() {
        let (sql, params) = read("or=(price.lt.5,status.eq.clearance)");
        assert_eq!(
            sql,
            "SELECT * FROM products WHERE (price < ? OR status = ?) LIMIT 1000"
        );
        assert_eq!(params, vec![json!(5), json!("clearance")]);
    }

    #[test]
    fn identifier_injection_is_rejected() {
        assert!(build_read("products; DROP TABLE users", "", false).is_err());
        assert!(build_read("products", "price;--=eq.1", false).is_err());
        assert!(build_read("products", "select=name,(evil)", false).is_err());
    }

    #[test]
    fn force_star_ignores_select_for_embed_base() {
        // The embed path fetches every column regardless of the user's select.
        let (sql, _, _) = build_read("products", "select=name,orders(id)", true).unwrap();
        assert_eq!(sql, "SELECT * FROM products LIMIT 1000");
    }

    #[test]
    fn where_builder_for_writes() {
        let (clause, params) = build_where("id=eq.7&active=is.true").unwrap();
        assert_eq!(clause, " WHERE id = ? AND active = ?");
        assert_eq!(params, vec![json!(7), json!(true)]);
    }

    #[test]
    fn empty_where_is_blank() {
        let (clause, params) = build_where("").unwrap();
        assert_eq!(clause, "");
        assert!(params.is_empty());
    }

    #[test]
    fn rows_to_objects_zips_columns() {
        let result = json!([{
            "columns": ["id", "name"],
            "rows": [[1, "a"], [2, "b"]]
        }]);
        let objs = rows_to_objects(&result);
        assert_eq!(
            objs,
            vec![json!({"id": 1, "name": "a"}), json!({"id": 2, "name": "b"})]
        );
    }
}
