//! Per-engine table cap (OxiBase per-project quota). `0` = unlimited (default);
//! a positive cap rejects a *new* table past it, never an existing one.

use oxidb_sql::SqlEngine;

#[test]
fn table_limit_caps_new_tables() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqlEngine::open(dir.path()).unwrap();
    db.set_max_tables(2);

    db.execute("CREATE TABLE a (id INT)").unwrap();
    db.execute("CREATE TABLE b (id INT)").unwrap();

    // Third table is rejected.
    let err = db.execute("CREATE TABLE c (id INT)").unwrap_err();
    assert!(
        err.to_string().contains("table limit reached"),
        "unexpected error: {err}"
    );

    // Existing tables keep working.
    db.execute("INSERT INTO a VALUES (1)").unwrap();

    // Raising the cap lets a new one through.
    db.set_max_tables(3);
    db.execute("CREATE TABLE c (id INT)").unwrap();
}

#[test]
fn table_limit_zero_is_unlimited() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqlEngine::open(dir.path()).unwrap();
    for i in 0..8 {
        db.execute(&format!("CREATE TABLE t{i} (id INT)")).unwrap();
    }
}
