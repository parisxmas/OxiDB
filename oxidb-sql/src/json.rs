//! JSON bridging for hosts that speak `serde_json::Value` (the server's wire
//! protocol, the embedded FFI): bind parameters from a JSON array, execute,
//! and shape results as JSON — one entry per statement.

use serde_json::{Value as Json, json};

use crate::ast::QueryResult;
use crate::types::Value;
use crate::{SqlEngine, is_read_only};

/// Execute a SQL string with optional JSON `params` against `engine` and
/// return the results as a JSON array (one entry per statement). With
/// `readonly`, only SELECT statements are permitted.
///
/// Result shapes: `{"columns": [...], "rows": [[...], ...]}` for SELECT,
/// `{"affected": N}` for DML, `{"ddl": true}` for DDL,
/// `{"transaction": true}` for BEGIN/COMMIT/ROLLBACK.
pub fn execute_json(
    engine: &SqlEngine,
    sql: &str,
    params: Option<&Json>,
    readonly: bool,
) -> Result<Json, String> {
    if readonly {
        match is_read_only(sql) {
            Ok(true) => {}
            Ok(false) => {
                return Err(
                    "permission denied: role 'read' may only execute SELECT/SHOW statements"
                        .to_string(),
                );
            }
            Err(e) => return Err(format!("sql error: {e}")),
        }
    }
    let params = parse_params(params)?;
    engine
        .execute_params(sql, &params)
        .map(results_to_json)
        .map_err(|e| format!("sql error: {e}"))
}

/// Convert the optional `params` JSON array into typed SQL values.
pub fn parse_params(params: Option<&Json>) -> Result<Vec<Value>, String> {
    let Some(params) = params else {
        return Ok(Vec::new());
    };
    let arr = params
        .as_array()
        .ok_or_else(|| "'params' must be an array".to_string())?;
    arr.iter().map(json_to_value).collect()
}

pub fn json_to_value(v: &Json) -> Result<Value, String> {
    match v {
        Json::Null => Ok(Value::Null),
        Json::Bool(b) => Ok(Value::Bool(*b)),
        Json::String(s) => Ok(Value::Text(s.clone())),
        Json::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Int(i))
            } else if let Some(f) = n.as_f64() {
                Ok(Value::Double(f))
            } else {
                Err(format!("unsupported numeric parameter: {n}"))
            }
        }
        other => Err(format!("unsupported parameter type: {other}")),
    }
}

/// Convert one statement's result to JSON.
pub fn result_to_json(r: QueryResult) -> Json {
    match r {
        QueryResult::Select {
            columns,
            types,
            rows,
        } => {
            let rows: Vec<Json> = rows
                .into_iter()
                .map(|row| Json::Array(row.iter().map(value_to_json).collect()))
                .collect();
            let types: Vec<Json> = types
                .iter()
                .map(|t| match t {
                    Some(t) => json!(format!("{t:?}").to_uppercase()),
                    None => Json::Null,
                })
                .collect();
            json!({ "columns": columns, "types": types, "rows": rows })
        }
        QueryResult::Mutation {
            affected,
            last_insert_id,
        } => match last_insert_id {
            Some(id) => json!({ "affected": affected, "last_insert_id": id }),
            None => json!({ "affected": affected }),
        },
        QueryResult::Ddl => json!({ "ddl": true }),
        QueryResult::Transaction => json!({ "transaction": true }),
    }
}

/// Convert all statement results into a JSON array (one entry per statement).
pub fn results_to_json(results: Vec<QueryResult>) -> Json {
    Json::Array(results.into_iter().map(result_to_json).collect())
}

pub fn value_to_json(v: &Value) -> Json {
    match v {
        Value::Null => Json::Null,
        Value::Int(n) => json!(n),
        Value::Double(f) => json!(f),
        Value::Text(s) => json!(s),
        Value::Bool(b) => json!(b),
        // Timestamps are epoch milliseconds on the wire.
        Value::Timestamp(t) => json!(t),
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
                Value::Int(1),
                Value::Double(2.5),
                Value::Text("x".into()),
                Value::Bool(true),
                Value::Null,
            ]
        );
        assert!(parse_params(None).unwrap().is_empty());
        assert!(parse_params(Some(&json!({"a": 1}))).is_err());
        assert!(parse_params(Some(&json!([[1]]))).is_err());
    }

    #[test]
    fn result_json_shapes() {
        assert_eq!(
            result_to_json(QueryResult::Mutation {
                affected: 3,
                last_insert_id: None
            }),
            json!({ "affected": 3 })
        );
        assert_eq!(result_to_json(QueryResult::Ddl), json!({ "ddl": true }));
        assert_eq!(
            result_to_json(QueryResult::Transaction),
            json!({ "transaction": true })
        );
        let sel = QueryResult::Select {
            columns: vec!["id".into(), "name".into()],
            types: vec![Some(crate::SqlType::Int), None],
            rows: vec![vec![Value::Int(1), Value::Text("ada".into())]],
        };
        assert_eq!(
            result_to_json(sel),
            json!({ "columns": ["id", "name"], "types": ["INT", null], "rows": [[1, "ada"]] })
        );
    }

    #[test]
    fn execute_json_readonly_gate() {
        let dir = tempfile::tempdir().unwrap();
        let db = SqlEngine::open(dir.path()).unwrap();
        assert!(execute_json(&db, "CREATE TABLE t (a INT)", None, false).is_ok());
        assert!(execute_json(&db, "SELECT a FROM t", None, true).is_ok());
        let err = execute_json(&db, "INSERT INTO t VALUES (1)", None, true).unwrap_err();
        assert!(err.contains("permission denied"));
    }
}
