//! Shared helpers for the oxidb-sql integration tests.
#![allow(dead_code)]

use oxidb_sql::{QueryResult, SqlEngine, Value};
use tempfile::TempDir;

/// Open a fresh engine in a temp dir. Keep the returned `TempDir` alive for the
/// lifetime of the test (dropping it removes the data directory).
pub fn open() -> (TempDir, SqlEngine) {
    let dir = tempfile::tempdir().unwrap();
    let db = SqlEngine::open(dir.path()).unwrap();
    (dir, db)
}

/// Open (or reopen) an engine at an existing directory.
#[allow(dead_code)]
pub fn open_at(path: &std::path::Path) -> SqlEngine {
    SqlEngine::open(path).unwrap()
}

/// Run SQL and return the last statement's SELECT rows.
pub fn rows(db: &SqlEngine, sql: &str) -> Vec<Vec<Value>> {
    unwrap_select(db.execute(sql).unwrap()).1
}

/// Run SQL and return (columns, rows) of the last statement.
pub fn cols_rows(db: &SqlEngine, sql: &str) -> (Vec<String>, Vec<Vec<Value>>) {
    unwrap_select(db.execute(sql).unwrap())
}

/// Run parameterized SQL and return the last statement's SELECT rows.
pub fn rows_p(db: &SqlEngine, sql: &str, params: &[Value]) -> Vec<Vec<Value>> {
    unwrap_select(db.execute_params(sql, params).unwrap()).1
}

/// Run SQL expected to be a single mutation; return the affected count.
pub fn affected(db: &SqlEngine, sql: &str) -> usize {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Mutation { affected } => affected,
        other => panic!("expected Mutation, got {other:?}"),
    }
}

/// Run parameterized SQL expected to be a single mutation.
pub fn affected_p(db: &SqlEngine, sql: &str, params: &[Value]) -> usize {
    match db.execute_params(sql, params).unwrap().pop().unwrap() {
        QueryResult::Mutation { affected } => affected,
        other => panic!("expected Mutation, got {other:?}"),
    }
}

/// From a multi-statement result, return the first SELECT's rows.
pub fn first_select(results: Vec<QueryResult>) -> Vec<Vec<Value>> {
    results
        .into_iter()
        .find_map(|r| match r {
            QueryResult::Select { rows, .. } => Some(rows),
            _ => None,
        })
        .expect("no SELECT in results")
}

fn unwrap_select(mut results: Vec<QueryResult>) -> (Vec<String>, Vec<Vec<Value>>) {
    match results.pop().unwrap() {
        QueryResult::Select { columns, rows } => (columns, rows),
        other => panic!("expected Select, got {other:?}"),
    }
}

// ── value constructors ──────────────────────────────────────────────────────

pub fn i(n: i64) -> Value {
    Value::Int(n)
}
pub fn t(s: &str) -> Value {
    Value::Text(s.to_string())
}
pub fn d(f: f64) -> Value {
    Value::Double(f)
}
pub fn b(x: bool) -> Value {
    Value::Bool(x)
}
pub const NULL: Value = Value::Null;

/// Build a single-row expectation `vec![vec![...]]` succinctly.
pub fn r1(cells: Vec<Value>) -> Vec<Vec<Value>> {
    vec![cells]
}
