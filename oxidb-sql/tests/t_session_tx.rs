//! ADR-0013 Phase B: interactive (session) transactions — BEGIN/COMMIT
//! spanning calls, read-your-writes, isolation, savepoints, error aborts.

mod common;

use common::*;
use oxidb_sql::{QueryResult, SqlEngine, Value};

fn seed(db: &SqlEngine) {
    db.execute("CREATE TABLE h (id INT PRIMARY KEY AUTO_INCREMENT, v INT)")
        .unwrap();
    db.execute("INSERT INTO h (v) VALUES (1), (2)").unwrap();
}

fn sess(db: &SqlEngine, tx: &mut Option<u64>, sql: &str) -> oxidb_sql::Result<Vec<QueryResult>> {
    db.execute_params_in_session(sql, &[], tx)
}

fn sel(r: &[QueryResult]) -> Vec<Vec<Value>> {
    match r.last().unwrap() {
        QueryResult::Select { rows, .. } => rows.clone(),
        other => panic!("expected Select, got {other:?}"),
    }
}

#[test]
fn begin_spans_calls_commit_makes_visible() {
    let (_d, db) = open();
    seed(&db);
    let mut tx = None;

    sess(&db, &mut tx, "BEGIN").unwrap();
    let id = tx.expect("transaction parked");

    // Writes across separate calls, read-your-writes in between.
    sess(&db, &mut tx, "INSERT INTO h (v) VALUES (10)").unwrap();
    let r = sess(&db, &mut tx, "SELECT COUNT(*) FROM h").unwrap();
    assert_eq!(sel(&r), vec![vec![Value::Int(3)]]);
    assert_eq!(tx, Some(id), "same transaction persists across calls");

    // Isolation: an autocommit reader doesn't see the buffered insert.
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM h"),
        vec![vec![Value::Int(2)]]
    );

    sess(&db, &mut tx, "COMMIT").unwrap();
    assert_eq!(tx, None);
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM h"),
        vec![vec![Value::Int(3)]]
    );
}

#[test]
fn rollback_and_stale_ids() {
    let (_d, db) = open();
    seed(&db);
    let mut tx = None;
    sess(&db, &mut tx, "BEGIN; DELETE FROM h").unwrap();
    let id = tx.unwrap();
    sess(&db, &mut tx, "ROLLBACK").unwrap();
    assert_eq!(tx, None);
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM h"),
        vec![vec![Value::Int(2)]]
    );
    // A stale id is a clean error.
    let mut stale = Some(id);
    assert!(sess(&db, &mut stale, "SELECT COUNT(*) FROM h").is_err());
}

#[test]
fn statement_error_aborts_the_transaction() {
    let (_d, db) = open();
    seed(&db);
    let mut tx = None;
    sess(&db, &mut tx, "BEGIN; INSERT INTO h (v) VALUES (99)").unwrap();
    // Duplicate PK -> error -> transaction aborted and cleared.
    assert!(sess(&db, &mut tx, "INSERT INTO h VALUES (1, 5)").is_err());
    assert_eq!(tx, None);
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM h"),
        vec![vec![Value::Int(2)]] // the 99 died with the transaction
    );
}

#[test]
fn savepoints_nested_partial_rollback() {
    let (_d, db) = open();
    seed(&db);
    let mut tx = None;
    sess(&db, &mut tx, "BEGIN; INSERT INTO h (v) VALUES (10)").unwrap();
    sess(&db, &mut tx, "SAVEPOINT a; INSERT INTO h (v) VALUES (20)").unwrap();
    sess(&db, &mut tx, "SAVEPOINT b; INSERT INTO h (v) VALUES (30)").unwrap();

    // Roll back to a: 20 and 30 vanish, 10 stays; savepoint a survives.
    sess(&db, &mut tx, "ROLLBACK TO SAVEPOINT a").unwrap();
    let r = sess(&db, &mut tx, "SELECT COUNT(*) FROM h").unwrap();
    assert_eq!(sel(&r), vec![vec![Value::Int(3)]]);

    // b was destroyed by the rollback; a is still usable.
    assert!(sess(&db, &mut tx, "ROLLBACK TO SAVEPOINT b").is_err());
    let mut tx2 = tx; // error above aborted the txn per our semantics
    assert_eq!(tx2, None);

    // Fresh transaction: RELEASE keeps data, forgets the savepoint.
    sess(
        &db,
        &mut tx2,
        "BEGIN; INSERT INTO h (v) VALUES (40); SAVEPOINT s",
    )
    .unwrap();
    sess(&db, &mut tx2, "RELEASE SAVEPOINT s").unwrap();
    assert!(sess(&db, &mut tx2, "ROLLBACK TO SAVEPOINT s").is_err());
}

#[test]
fn batch_scoped_execute_still_discards() {
    let (_d, db) = open();
    seed(&db);
    // The legacy entry point keeps its auto-rollback contract and leaks no
    // parked transactions.
    db.execute("BEGIN; INSERT INTO h (v) VALUES (77)").unwrap();
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM h"),
        vec![vec![Value::Int(2)]]
    );
}

#[test]
fn savepoint_outside_transaction_errors() {
    let (_d, db) = open();
    seed(&db);
    let mut tx = None;
    assert!(sess(&db, &mut tx, "SAVEPOINT s").is_err());
    assert!(sess(&db, &mut tx, "ROLLBACK TO SAVEPOINT s").is_err());
    assert!(sess(&db, &mut tx, "RELEASE SAVEPOINT s").is_err());
}
