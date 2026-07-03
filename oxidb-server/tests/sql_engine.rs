//! Server-level integration for the second (SQL) engine behind the `engine`
//! router (ADR-0010). Its own test binary so the process-global `OXIDB_SQL`
//! env + lazy engine init are deterministic.

use std::sync::Arc;

use oxidb::OxiDb;
use serde_json::{Value, json};

fn resp(db: &Arc<OxiDb>, req: Value) -> Value {
    let mut tx = None;
    let bytes = oxidb_server::handler::handle_request(db, req, &mut tx);
    serde_json::from_slice(&bytes).unwrap()
}

/// Backward compatibility: a request with no `engine` field takes the document
/// path exactly as before.
#[test]
fn doc_path_unchanged_without_engine_field() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(OxiDb::open(dir.path()).unwrap());

    assert_eq!(resp(&db, json!({"cmd": "ping"}))["ok"], json!(true));

    let ins = resp(
        &db,
        json!({"cmd": "insert", "collection": "c", "doc": {"a": 1}}),
    );
    assert_eq!(ins["ok"], json!(true));
    let found = resp(
        &db,
        json!({"cmd": "find", "collection": "c", "query": {"a": 1}}),
    );
    assert_eq!(found["ok"], json!(true));
    assert_eq!(found["data"].as_array().unwrap().len(), 1);
}

/// An unknown engine name is a clean error, not a fall-through to the doc path.
#[test]
fn unknown_engine_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(OxiDb::open(dir.path()).unwrap());
    let r = resp(&db, json!({"engine": "weird", "cmd": "ping"}));
    assert_eq!(r["ok"], json!(false));
    assert!(r["error"].as_str().unwrap().contains("unknown engine"));
}

/// End-to-end SQL over the wire: DDL, DML, parameterized SELECT, transaction.
#[test]
fn sql_engine_roundtrip_when_enabled() {
    let sql_dir = tempfile::tempdir().unwrap();
    // Enable the SQL engine and point it at an isolated dir BEFORE the first
    // SQL request triggers lazy init. The engine is process-global (OnceLock),
    // so every SQL-touching test in this binary shares one instance — tests
    // use distinct table names.
    unsafe {
        std::env::set_var("OXIDB_SQL", "1");
        std::env::set_var("OXIDB_SQL_DATA", sql_dir.path());
    }

    let doc_dir = tempfile::tempdir().unwrap();
    let db = Arc::new(OxiDb::open(doc_dir.path()).unwrap());

    // DDL
    let r = resp(
        &db,
        json!({"engine": "sql", "cmd": "sql",
               "sql": "CREATE TABLE t (id INT PRIMARY KEY, name TEXT NOT NULL)"}),
    );
    assert_eq!(r["ok"], json!(true), "ddl failed: {r}");
    assert_eq!(r["data"], json!([{ "ddl": true }]));

    // INSERT (multi-row)
    let r = resp(
        &db,
        json!({"engine": "sql", "cmd": "sql",
               "sql": "INSERT INTO t VALUES (1,'ada'),(2,'bob')"}),
    );
    assert_eq!(r["data"], json!([{ "affected": 2 }]));

    // Parameterized SELECT
    let r = resp(
        &db,
        json!({"engine": "sql", "cmd": "sql",
               "sql": "SELECT id, name FROM t WHERE id = ?", "params": [2]}),
    );
    assert_eq!(r["ok"], json!(true), "select failed: {r}");
    assert_eq!(
        r["data"],
        json!([{ "columns": ["id", "name"], "types": ["INT", "TEXT"], "rows": [[2, "bob"]] }])
    );

    // Transaction (multi-statement, atomic)
    let r = resp(
        &db,
        json!({"engine": "sql", "cmd": "sql",
               "sql": "BEGIN; DELETE FROM t WHERE id = 1; COMMIT"}),
    );
    assert_eq!(r["ok"], json!(true), "txn failed: {r}");
    let r = resp(
        &db,
        json!({"engine": "sql", "cmd": "sql", "sql": "SELECT id FROM t"}),
    );
    assert_eq!(r["data"], json!([{ "columns": ["id"], "types": ["INT"], "rows": [[2]] }]));

    // The `cmd == "sql"` shortcut works without an explicit engine field.
    let r = resp(&db, json!({"cmd": "sql", "sql": "SELECT id FROM t"}));
    assert_eq!(r["ok"], json!(true));

    // A SQL parse error surfaces as a clean error, not a panic.
    let r = resp(&db, json!({"cmd": "sql", "sql": "SELCT nonsense"}));
    assert_eq!(r["ok"], json!(false));

    // The document engine remains completely independent in the same process.
    let ins = resp(
        &db,
        json!({"cmd": "insert", "collection": "c", "doc": {"z": 9}}),
    );
    assert_eq!(ins["ok"], json!(true));
    // The SQL table name is invisible to the document engine: a doc find on a
    // collection named "t" sees no rows (the SQL rows live in separate files).
    let found = resp(&db, json!({"cmd": "find", "collection": "t", "query": {}}));
    assert_eq!(found["ok"], json!(true));
    assert!(
        found["data"].as_array().unwrap().is_empty(),
        "doc engine must not see SQL rows: {found}"
    );
}

/// A Read-role session (flagged by the session layer) may SELECT but not
/// write through the SQL engine.
#[test]
fn readonly_flag_restricts_sql_to_selects() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(OxiDb::open(dir.path()).unwrap());
    unsafe { std::env::set_var("OXIDB_SQL", "1") };
    unsafe { std::env::set_var("OXIDB_SQL_DATA", dir.path().join("sql").to_str().unwrap()) };

    let run = |req: Value, readonly: bool| -> Value {
        let mut tx = None;
        let bytes = oxidb_server::handler::handle_request_opts(&db, req, &mut tx, readonly);
        serde_json::from_slice(&bytes).unwrap()
    };

    // Seed as a writer.
    let r = run(
        json!({"cmd": "sql", "sql": "CREATE TABLE r (id INT); INSERT INTO r VALUES (1)"}),
        false,
    );
    assert_eq!(r["ok"], json!(true), "seed failed: {r}");

    // Read-only: SELECT ok (including UNION), writes and DDL denied.
    let r = run(json!({"cmd": "sql", "sql": "SELECT id FROM r"}), true);
    assert_eq!(r["ok"], json!(true), "readonly select failed: {r}");
    let r = run(
        json!({"cmd": "sql", "sql": "SELECT id FROM r UNION SELECT id FROM r"}),
        true,
    );
    assert_eq!(r["ok"], json!(true));
    for sql in [
        "INSERT INTO r VALUES (2)",
        "UPDATE r SET id = 3",
        "DELETE FROM r",
        "DROP TABLE r",
        "SELECT id FROM r; INSERT INTO r VALUES (9)",
        "BEGIN; INSERT INTO r VALUES (9); COMMIT;",
    ] {
        let r = run(json!({"cmd": "sql", "sql": sql}), true);
        assert_eq!(r["ok"], json!(false), "should be denied: {sql}");
        assert!(
            r["error"].as_str().unwrap().contains("permission denied"),
            "unexpected error for {sql}: {r}"
        );
    }
    // Nothing leaked through.
    let r = run(
        json!({"cmd": "sql", "sql": "SELECT COUNT(*) AS n FROM r"}),
        false,
    );
    assert_eq!(
        r["data"],
        json!([{ "columns": ["n"], "types": ["INT"], "rows": [[1]] }])
    );
}
