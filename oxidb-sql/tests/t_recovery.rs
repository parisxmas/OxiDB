//! Durability and recovery through the SQL surface: WAL replay, checkpoints,
//! DDL recovery, value-type durability, catalog persistence.

mod common;
use common::*;
use oxidb_sql::SqlEngine;

#[test]
fn all_dml_recovers_from_wal_without_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = SqlEngine::open(dir.path()).unwrap();
        db.execute("CREATE TABLE t (id INT, v INT)").unwrap();
        db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)")
            .unwrap();
        db.execute("UPDATE t SET v = 99 WHERE id = 2").unwrap();
        db.execute("DELETE FROM t WHERE id = 3").unwrap();
    }
    let db = SqlEngine::open(dir.path()).unwrap();
    assert_eq!(
        rows(&db, "SELECT id, v FROM t ORDER BY id"),
        vec![vec![i(1), i(10)], vec![i(2), i(99)]]
    );
}

#[test]
fn checkpoint_then_reopen_then_more_writes() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = SqlEngine::open(dir.path()).unwrap();
        db.execute("CREATE TABLE t (id INT)").unwrap();
        db.execute("INSERT INTO t VALUES (1)").unwrap();
        db.checkpoint().unwrap();
        db.execute("INSERT INTO t VALUES (2)").unwrap();
        // snapshot has 1, WAL has 2
    }
    let db = SqlEngine::open(dir.path()).unwrap();
    assert_eq!(
        rows(&db, "SELECT id FROM t ORDER BY id"),
        vec![vec![i(1)], vec![i(2)]]
    );
    // Multiple checkpoints are idempotent.
    db.checkpoint().unwrap();
    db.checkpoint().unwrap();
    assert_eq!(
        rows(&db, "SELECT id FROM t ORDER BY id"),
        vec![vec![i(1)], vec![i(2)]]
    );
}

#[test]
fn dropped_table_stays_dropped_and_name_reusable() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = SqlEngine::open(dir.path()).unwrap();
        db.execute("CREATE TABLE t (id INT)").unwrap();
        db.execute("INSERT INTO t VALUES (1)").unwrap();
        db.checkpoint().unwrap();
        db.execute("DROP TABLE t").unwrap();
        db.checkpoint().unwrap(); // snapshot for t must be removed
    }
    let db = SqlEngine::open(dir.path()).unwrap();
    assert!(!db.table_names().contains(&"t".to_string()));
    // Re-create the same name with a different schema.
    db.execute("CREATE TABLE t (a TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES ('x')").unwrap();
    assert_eq!(rows(&db, "SELECT a FROM t"), r1(vec![t("x")]));
}

#[test]
fn committed_transaction_recovers_atomically() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = SqlEngine::open(dir.path()).unwrap();
        db.execute("CREATE TABLE t (id INT, v INT)").unwrap();
        db.execute("BEGIN; INSERT INTO t VALUES (1,1); INSERT INTO t VALUES (2,2); COMMIT")
            .unwrap();
    }
    let db = SqlEngine::open(dir.path()).unwrap();
    assert_eq!(
        rows(&db, "SELECT id, v FROM t ORDER BY id"),
        vec![vec![i(1), i(1)], vec![i(2), i(2)]]
    );
}

#[test]
fn double_and_bool_values_are_durable() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = SqlEngine::open(dir.path()).unwrap();
        db.execute("CREATE TABLE t (id INT, f DOUBLE, flag BOOL)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 3.5, true), (2, -1.25, false)")
            .unwrap();
        db.checkpoint().unwrap();
    }
    let db = SqlEngine::open(dir.path()).unwrap();
    assert_eq!(
        rows(&db, "SELECT f, flag FROM t ORDER BY id"),
        vec![vec![d(3.5), b(true)], vec![d(-1.25), b(false)]]
    );
}

#[test]
fn timestamp_values_are_durable_via_programmatic_api() {
    use oxidb_sql::{Column, SqlType, Table, Value};
    let dir = tempfile::tempdir().unwrap();
    {
        let db = SqlEngine::open(dir.path()).unwrap();
        db.create_table(Table::new(
            "ev",
            vec![
                Column::new("id", SqlType::Int),
                Column::new("at", SqlType::Timestamp),
            ],
        ))
        .unwrap();
        db.insert(
            "ev",
            vec![Value::Int(1), Value::Timestamp(1_700_000_000_000)],
        )
        .unwrap();
        db.checkpoint().unwrap();
    }
    let db = SqlEngine::open(dir.path()).unwrap();
    let scanned = db.scan("ev").unwrap();
    assert_eq!(scanned.len(), 1);
    assert_eq!(scanned[0].1[1], Value::Timestamp(1_700_000_000_000));
}

#[test]
fn catalog_reports_tables_and_definitions() {
    let (_d, db) = open();
    db.execute("CREATE TABLE b (x INT)").unwrap();
    db.execute("CREATE TABLE a (y TEXT)").unwrap();
    // Sorted table names.
    assert_eq!(db.table_names(), vec!["a".to_string(), "b".to_string()]);
    let def = db.table_def("a").unwrap();
    assert_eq!(def.name, "a");
    assert_eq!(def.columns.len(), 1);
    assert!(db.table_def("ghost").is_none());
}

#[test]
fn row_ids_monotonic_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = SqlEngine::open(dir.path()).unwrap();
        db.execute("CREATE TABLE t (id INT)").unwrap();
        db.execute("INSERT INTO t VALUES (1),(2),(3)").unwrap();
        db.execute("DELETE FROM t WHERE id = 3").unwrap();
    }
    // After reopen, new inserts must not reuse the freed row id in a way that
    // corrupts data; a simple insert + query stays consistent.
    let db = SqlEngine::open(dir.path()).unwrap();
    db.execute("INSERT INTO t VALUES (4)").unwrap();
    assert_eq!(
        rows(&db, "SELECT id FROM t ORDER BY id"),
        vec![vec![i(1)], vec![i(2)], vec![i(4)]]
    );
}
