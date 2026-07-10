//! ADR-0014 Phase 3: pin the Raft write-classification of Cobra procedures.
//!
//! The server replicates a SQL statement through Raft iff
//! `oxidb_sql::is_read_only(sql)` says it is NOT read-only — and that
//! function parses the text, so the `LANGUAGE COBRA` pre-parse intercept
//! MUST be visible to it. If these ever regress, cobra CREATE/CALL would
//! silently stay node-local in a cluster.

use base64::Engine;

fn fixture_b64() -> String {
    let bytes = std::fs::read(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/data/cobra/add_row.cobrac"
    ))
    .expect("fixture");
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

#[test]
fn create_procedure_language_cobra_is_a_write() {
    let sql = format!(
        "CREATE PROCEDURE p(name TEXT, age INT) LANGUAGE COBRA AS '{}'",
        fixture_b64()
    );
    assert_eq!(oxidb_sql::is_read_only(&sql).unwrap(), false);
}

#[test]
fn call_is_a_write() {
    assert_eq!(oxidb_sql::is_read_only("CALL p(1, 2)").unwrap(), false);
}

#[test]
fn drop_procedure_is_a_write() {
    assert_eq!(oxidb_sql::is_read_only("DROP PROCEDURE p").unwrap(), false);
}
