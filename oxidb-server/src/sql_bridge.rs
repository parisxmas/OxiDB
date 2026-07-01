//! Bridge between the wire protocol and the standalone SQL engine (ADR-0010).
//!
//! The SQL engine is a *second* engine mounted in the same server process. It
//! owns entirely separate files (`${OXIDB_DATA}/sql` by default) and shares no
//! state with the document engine. It is **off by default** and constructed
//! lazily on first use only when `OXIDB_SQL` is truthy, so a server that never
//! uses SQL pays nothing.
//!
//! Routing (in [`crate::handler::handle_request`]): a request whose `engine`
//! field is `"sql"` — or which uses the reserved `sql` command — is served
//! here. Requests without an `engine` field default to `"doc"` and keep the
//! document path byte-for-byte, so every existing client is unaffected.
//!
//! Access control: the `sql` command is gated at the `ReadWrite` role by
//! [`crate::rbac`] (checked before this handler runs), so the SQL engine
//! requires at least write privileges — there is no read-only SQL role in v1.

use std::sync::{Arc, OnceLock};

use oxidb_sql::{QueryResult, SqlEngine, Value as SqlValue};
use serde_json::{Value, json};

use crate::handler::{err_bytes, ok_bytes};

static ENGINE: OnceLock<Option<Arc<SqlEngine>>> = OnceLock::new();

/// Whether an env var is set to a truthy value (`1`/`true`/`yes`/`on`).
fn env_truthy(key: &str) -> bool {
    matches!(
        std::env::var(key)
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// The lazily-opened SQL engine, or `None` when disabled / failed to open.
fn engine() -> Option<Arc<SqlEngine>> {
    ENGINE
        .get_or_init(|| {
            if !env_truthy("OXIDB_SQL") {
                return None;
            }
            let dir = std::env::var("OXIDB_SQL_DATA").unwrap_or_else(|_| {
                let data =
                    std::env::var("OXIDB_DATA").unwrap_or_else(|_| "./oxidb_data".to_string());
                format!("{data}/sql")
            });
            match SqlEngine::open(&dir) {
                Ok(e) => {
                    eprintln!("[oxidb] SQL engine enabled at {dir}");
                    Some(Arc::new(e))
                }
                Err(err) => {
                    eprintln!("[oxidb] failed to open SQL engine at {dir}: {err}");
                    None
                }
            }
        })
        .clone()
}

/// Handle a SQL-engine request. `cmd` must be the reserved `"sql"` command.
///
/// Request shape:
/// ```json
/// { "engine": "sql", "cmd": "sql", "sql": "SELECT ...", "params": [ ... ] }
/// ```
/// `params` is optional and binds `?` / `$N` placeholders left-to-right.
pub fn handle_sql(cmd: &str, request: &Value) -> Vec<u8> {
    if cmd != "sql" {
        return err_bytes("SQL engine requests must use cmd \"sql\"");
    }
    let Some(engine) = engine() else {
        return err_bytes("SQL engine is not enabled (set OXIDB_SQL=1)");
    };
    let Some(sql) = request.get("sql").and_then(|v| v.as_str()) else {
        return err_bytes("missing 'sql' field");
    };
    let params = match parse_params(request.get("params")) {
        Ok(p) => p,
        Err(msg) => return err_bytes(&msg),
    };

    match engine.execute_params(sql, &params) {
        Ok(results) => ok_bytes(results_to_json(results)),
        Err(e) => err_bytes(&format!("sql error: {e}")),
    }
}

/// Convert the optional `params` JSON array into typed SQL values.
fn parse_params(params: Option<&Value>) -> Result<Vec<SqlValue>, String> {
    let Some(params) = params else {
        return Ok(Vec::new());
    };
    let arr = params
        .as_array()
        .ok_or_else(|| "'params' must be an array".to_string())?;
    arr.iter().map(json_to_value).collect()
}

fn json_to_value(v: &Value) -> Result<SqlValue, String> {
    match v {
        Value::Null => Ok(SqlValue::Null),
        Value::Bool(b) => Ok(SqlValue::Bool(*b)),
        Value::String(s) => Ok(SqlValue::Text(s.clone())),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(SqlValue::Int(i))
            } else if let Some(f) = n.as_f64() {
                Ok(SqlValue::Double(f))
            } else {
                Err(format!("unsupported numeric parameter: {n}"))
            }
        }
        other => Err(format!("unsupported parameter type: {other}")),
    }
}

/// Convert one statement's result to JSON.
fn result_to_json(r: QueryResult) -> Value {
    match r {
        QueryResult::Select { columns, rows } => {
            let rows: Vec<Value> = rows
                .into_iter()
                .map(|row| Value::Array(row.iter().map(value_to_json).collect()))
                .collect();
            json!({ "columns": columns, "rows": rows })
        }
        QueryResult::Mutation { affected } => json!({ "affected": affected }),
        QueryResult::Ddl => json!({ "ddl": true }),
        QueryResult::Transaction => json!({ "transaction": true }),
    }
}

/// Convert all statement results into a JSON array (one entry per statement).
fn results_to_json(results: Vec<QueryResult>) -> Value {
    Value::Array(results.into_iter().map(result_to_json).collect())
}

fn value_to_json(v: &SqlValue) -> Value {
    match v {
        SqlValue::Null => Value::Null,
        SqlValue::Int(n) => json!(n),
        SqlValue::Double(f) => json!(f),
        SqlValue::Text(s) => json!(s),
        SqlValue::Bool(b) => json!(b),
        // Timestamps are epoch milliseconds on the wire.
        SqlValue::Timestamp(t) => json!(t),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn params_parse_all_json_scalars() {
        let p = json!([1, 2.5, "x", true, null]);
        let vals = parse_params(Some(&p)).unwrap();
        assert_eq!(
            vals,
            vec![
                SqlValue::Int(1),
                SqlValue::Double(2.5),
                SqlValue::Text("x".into()),
                SqlValue::Bool(true),
                SqlValue::Null,
            ]
        );
        assert!(parse_params(None).unwrap().is_empty());
        assert!(parse_params(Some(&json!({"a": 1}))).is_err());
        assert!(parse_params(Some(&json!([[1]]))).is_err());
    }

    #[test]
    fn result_json_shapes() {
        assert_eq!(
            result_to_json(QueryResult::Mutation { affected: 3 }),
            json!({ "affected": 3 })
        );
        assert_eq!(result_to_json(QueryResult::Ddl), json!({ "ddl": true }));
        assert_eq!(
            result_to_json(QueryResult::Transaction),
            json!({ "transaction": true })
        );
        let sel = QueryResult::Select {
            columns: vec!["id".into(), "name".into()],
            rows: vec![vec![SqlValue::Int(1), SqlValue::Text("ada".into())]],
        };
        assert_eq!(
            result_to_json(sel),
            json!({ "columns": ["id", "name"], "rows": [[1, "ada"]] })
        );
    }
}
