//! Verifies the SQL engine is OFF by default: without `OXIDB_SQL`, SQL requests
//! return a clean "not enabled" error. Its own test binary so the process never
//! has `OXIDB_SQL` set (the enabled path lives in `sql_engine.rs`).

use std::sync::Arc;

use oxidb::OxiDb;
use serde_json::{Value, json};

fn resp(db: &Arc<OxiDb>, req: Value) -> Value {
    let mut tx = None;
    let bytes = oxidb_server::handler::handle_request(db, req, &mut tx);
    serde_json::from_slice(&bytes).unwrap()
}

#[test]
fn sql_requests_error_when_disabled() {
    // Defensively ensure the env is unset for this process.
    unsafe {
        std::env::remove_var("OXIDB_SQL");
    }
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(OxiDb::open(dir.path()).unwrap());

    let r = resp(
        &db,
        json!({"engine": "sql", "cmd": "sql", "sql": "SELECT 1 FROM t"}),
    );
    assert_eq!(r["ok"], json!(false));
    assert!(
        r["error"].as_str().unwrap().contains("not enabled"),
        "unexpected error: {r}"
    );

    // The document path still works normally.
    assert_eq!(resp(&db, json!({"cmd": "ping"}))["ok"], json!(true));
}
