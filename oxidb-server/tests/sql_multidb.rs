//! Multi-database SQL (ADR-0012): each database gets its own SQL engine,
//! routed by the `db` the session layer resolved. Its own test binary so the
//! process-global `OXIDB_SQL` / `OXIDB_DATA` env + lazy registry init are
//! deterministic — everything runs in ONE #[test] because the registry reads
//! the env exactly once.

use std::sync::Arc;

use oxidb::{DatabaseManager, OxiDb};
use serde_json::{Value, json};

fn resp_in(db: &Arc<OxiDb>, name: &str, req: Value) -> Value {
    let mut tx = None;
    let bytes = oxidb_server::handler::handle_request_in_db(db, name, req, &mut tx, false);
    serde_json::from_slice(&bytes).unwrap()
}

fn sql(db: &Arc<OxiDb>, name: &str, sql: &str) -> Value {
    resp_in(db, name, json!({"engine": "sql", "cmd": "sql", "sql": sql}))
}

#[test]
fn sql_engines_are_per_database() {
    let root = tempfile::tempdir().unwrap();
    unsafe {
        std::env::set_var("OXIDB_SQL", "1");
        std::env::set_var("OXIDB_DATA", root.path());
        std::env::remove_var("OXIDB_SQL_DATA");
    }

    let mgr = DatabaseManager::open(root.path(), None, false, None).unwrap();
    mgr.create_database("crm").unwrap();
    let default_db = mgr.get_default_database().unwrap();
    let crm_db = mgr.get_database("crm").unwrap();

    // Same table name in both databases, different rows.
    let r = sql(
        &default_db,
        "oxidb",
        "CREATE TABLE t (id INT PRIMARY KEY, origin TEXT); INSERT INTO t VALUES (1, 'default')",
    );
    assert_eq!(r["ok"], json!(true), "default ddl+insert: {r}");
    let r = sql(
        &crm_db,
        "crm",
        "CREATE TABLE t (id INT PRIMARY KEY, origin TEXT); INSERT INTO t VALUES (1, 'crm'), (2, 'crm')",
    );
    assert_eq!(r["ok"], json!(true), "crm ddl+insert: {r}");

    // Fully isolated: same query, different answers.
    let r = sql(&default_db, "oxidb", "SELECT origin FROM t ORDER BY id");
    assert_eq!(r["data"][0]["rows"], json!([["default"]]));
    let r = sql(&crm_db, "crm", "SELECT origin FROM t ORDER BY id");
    assert_eq!(r["data"][0]["rows"], json!([["crm"], ["crm"]]));

    // Introspection sees per-database catalogs (row counts differ).
    let r = sql(&default_db, "oxidb", "SHOW TABLES");
    assert_eq!(r["data"][0]["rows"], json!([["t", 1]]));
    let r = sql(&crm_db, "crm", "SHOW TABLES");
    assert_eq!(r["data"][0]["rows"], json!([["t", 2]]));

    // The `postgres` alias routes to the default database's engine.
    let r = sql(&default_db, "postgres", "SELECT COUNT(*) FROM t");
    assert_eq!(r["data"][0]["rows"], json!([[1]]));

    // A database that was never created is a clean error.
    let r = sql(&default_db, "ghost", "SELECT 1 FROM t");
    assert_eq!(r["ok"], json!(false));
    assert!(
        r["error"].as_str().unwrap().contains("database not found"),
        "got: {r}"
    );

    // Files land where ADR-0012 says: default at root/sql, named at root/crm/sql.
    assert!(root.path().join("sql").join("wal").is_dir());
    assert!(root.path().join("crm").join("sql").join("wal").is_dir());

    // Drop + recreate: the registry forgets the engine, so the recreated
    // database starts with a fresh (empty) SQL catalog.
    mgr.drop_database("crm").unwrap();
    oxidb_server::sql_bridge::forget_database("crm");
    mgr.create_database("crm").unwrap();
    let crm_db = mgr.get_database("crm").unwrap();
    let r = sql(&crm_db, "crm", "SHOW TABLES");
    assert_eq!(
        r["data"][0]["rows"],
        json!([]),
        "recreated db not empty: {r}"
    );

    // The legacy default-db entry points still work (REST path).
    let out = oxidb_server::sql_bridge::execute_json("SELECT origin FROM t", None, false).unwrap();
    assert_eq!(out[0]["rows"], json!([["default"]]));
}
