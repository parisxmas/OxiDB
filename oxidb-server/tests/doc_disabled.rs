//! `OXIDB_DOC=0` — the document engine off, SQL/TSDB only.
//!
//! Its own test binary so the whole process runs with `OXIDB_DOC=0` (the switch
//! is read once into a `LazyLock`), the same reason `sql_disabled.rs` is
//! separate. The enabled path is every other test in this directory.

use std::process::Command;
use std::sync::Arc;

use oxidb::OxiDb;
use serde_json::{Value, json};

fn resp(db: &Arc<OxiDb>, req: Value) -> Value {
    let mut tx = None;
    let bytes = oxidb_server::handler::handle_request(db, req, &mut tx);
    serde_json::from_slice(&bytes).unwrap()
}

#[test]
fn document_commands_are_refused_by_name() {
    unsafe {
        std::env::set_var("OXIDB_DOC", "0");
    }
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(OxiDb::open(dir.path()).unwrap());

    // A refusal has to name the switch: an operator who set it somewhere else
    // (compose file, systemd unit) is otherwise looking at a server that simply
    // says no to everything.
    for req in [
        json!({"cmd": "insert", "collection": "users", "doc": {"name": "ada"}}),
        json!({"cmd": "find", "collection": "users", "query": {}}),
        json!({"cmd": "list_collections"}),
        json!({"cmd": "create_index", "collection": "users", "field": "name"}),
    ] {
        let r = resp(&db, req.clone());
        assert_eq!(r["ok"], json!(false), "should have been refused: {req}");
        let err = r["error"].as_str().unwrap();
        assert!(err.contains("OXIDB_DOC=0"), "must name the switch: {err}");
    }
}

#[test]
fn ping_still_answers() {
    unsafe {
        std::env::set_var("OXIDB_DOC", "0");
    }
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(OxiDb::open(dir.path()).unwrap());
    // A health check asks whether the process is up, not whether it has a
    // document engine — refusing it would take a SQL-only server out of every
    // load balancer that probes this way.
    assert_eq!(resp(&db, json!({"cmd": "ping"}))["ok"], json!(true));
}

/// The startup guards. Both exit before anything binds, so these are cheap.
fn start_and_capture(envs: &[(&str, &str)]) -> (bool, String) {
    let dir = tempfile::tempdir().unwrap();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_oxidb-server"));
    cmd.env("OXIDB_DATA", dir.path()).env("OXIDB_DOC", "0");
    for (k, v) in envs {
        cmd.env(k, v);
    }
    let out = cmd.output().expect("failed to run the server");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stderr).to_string(),
    )
}

#[test]
fn refuses_to_start_with_no_engine_at_all() {
    // OXIDB_DOC=0 and no SQL/TSDB is a server that can serve nothing. Starting
    // it anyway would look healthy and answer every request with an error.
    let (ok, err) = start_and_capture(&[]);
    assert!(!ok, "should have exited non-zero; stderr: {err}");
    assert!(err.contains("OXIDB_SQL=1"), "must say what to set: {err}");
}

#[test]
fn refuses_to_start_with_a_document_backed_listener() {
    // Binding a protocol whose storage was just turned off is the silent-half-
    // service failure; name the variable and the reason instead.
    let (ok, err) = start_and_capture(&[("OXIDB_SQL", "1"), ("OXIDB_S3_PORT", "19099")]);
    assert!(!ok, "should have exited non-zero; stderr: {err}");
    assert!(
        err.contains("OXIDB_S3_PORT") && err.contains("blob"),
        "must name the listener and why: {err}"
    );
    // ...and it must not list a listener that IS compatible as an offender.
    // Checked against the bullet lines only — the closing advice names
    // OXIDB_PG_PORT on purpose, as one of the two that still work.
    let offenders: Vec<&str> = err.lines().filter(|l| l.starts_with("  OXIDB_")).collect();
    assert_eq!(offenders.len(), 1, "expected exactly one offender: {err}");
    assert!(
        offenders[0].contains("OXIDB_S3_PORT"),
        "wrong offender: {err}"
    );
}
