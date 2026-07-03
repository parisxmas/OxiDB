//! Interactive SQL transactions over the wire (ADR-0013 Phase B): the
//! session layer carries `sql_tx` across requests; the legacy entry points
//! keep batch-scoped auto-rollback. Own binary for the process-global env.

use std::sync::Arc;

use oxidb::OxiDb;
use serde_json::{Value, json};

fn sql_req(sql: &str) -> Value {
    json!({"engine": "sql", "cmd": "sql", "sql": sql})
}

#[test]
fn interactive_tx_spans_wire_requests() {
    let root = tempfile::tempdir().unwrap();
    unsafe {
        std::env::set_var("OXIDB_SQL", "1");
        std::env::set_var("OXIDB_DATA", root.path());
        std::env::remove_var("OXIDB_SQL_DATA");
    }
    let doc_dir = tempfile::tempdir().unwrap();
    let db = Arc::new(OxiDb::open(doc_dir.path()).unwrap());

    let mut doc_tx = None;
    let mut sql_tx = None;
    let mut run = |sql: &str, sql_tx: &mut Option<u64>| -> Value {
        let bytes = oxidb_server::handler::handle_request_session(
            &db,
            "oxidb",
            sql_req(sql),
            &mut doc_tx,
            sql_tx,
            false,
        );
        serde_json::from_slice(&bytes).unwrap()
    };

    let r = run(
        "CREATE TABLE w (id INT PRIMARY KEY AUTO_INCREMENT, v INT)",
        &mut sql_tx,
    );
    assert_eq!(r["ok"], json!(true), "{r}");

    // Request 1: BEGIN parks a transaction on the session.
    let r = run("BEGIN", &mut sql_tx);
    assert_eq!(r["ok"], json!(true), "{r}");
    assert!(sql_tx.is_some(), "transaction parked on the session");

    // Request 2: write inside it; request 3: read-your-writes.
    let r = run("INSERT INTO w (v) VALUES (7)", &mut sql_tx);
    assert_eq!(r["data"][0]["affected"], json!(1), "{r}");
    let r = run("SELECT COUNT(*) FROM w", &mut sql_tx);
    assert_eq!(r["data"][0]["rows"], json!([[1]]), "{r}");

    // Parallel session sees nothing until commit.
    let mut other = None;
    let r = run("SELECT COUNT(*) FROM w", &mut other);
    assert_eq!(r["data"][0]["rows"], json!([[0]]), "{r}");
    assert_eq!(other, None, "read-only batch parks nothing");

    // Request 4: COMMIT.
    let r = run("COMMIT", &mut sql_tx);
    assert_eq!(r["ok"], json!(true), "{r}");
    assert_eq!(sql_tx, None);
    let r = run("SELECT COUNT(*) FROM w", &mut other);
    assert_eq!(r["data"][0]["rows"], json!([[1]]), "{r}");

    // Legacy entry point (no session): open transaction is auto-rolled back.
    let mut doc_tx2 = None;
    let bytes = oxidb_server::handler::handle_request_in_db(
        &db,
        "oxidb",
        sql_req("BEGIN; INSERT INTO w (v) VALUES (99)"),
        &mut doc_tx2,
        false,
    );
    let r: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(r["ok"], json!(true), "{r}");
    let r = run("SELECT COUNT(*) FROM w", &mut other);
    assert_eq!(
        r["data"][0]["rows"],
        json!([[1]]),
        "99 must have been discarded: {r}"
    );

    // Disconnect-style cleanup: park a tx, then roll it back by id.
    let r = run("BEGIN; INSERT INTO w (v) VALUES (5)", &mut sql_tx);
    assert_eq!(r["ok"], json!(true), "{r}");
    let id = sql_tx.unwrap();
    oxidb_server::sql_bridge::rollback_session_tx("oxidb", id);
    let mut stale = Some(id);
    let r = run("SELECT 1 FROM w", &mut stale);
    assert_eq!(r["ok"], json!(false), "stale id must error: {r}");
}
