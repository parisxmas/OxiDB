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

/// The in-transaction uniqueness check probes the engine's committed
/// PK/UNIQUE maps plus the transaction's own writes (no full-table seeding);
/// every visibility combination must still hold.
#[test]
fn txn_uniqueness_against_base_and_overlay() {
    let (_d, db) = open();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY, tag TEXT UNIQUE, v INT)")
        .unwrap();
    db.execute("INSERT INTO u VALUES (1, 'a', 0), (2, 'b', 0)")
        .unwrap();
    let mut tx = None;

    // Base-row collisions are caught (PK and UNIQUE), statement errors abort.
    sess(&db, &mut tx, "BEGIN").unwrap();
    assert!(sess(&db, &mut tx, "INSERT INTO u VALUES (1, 'x', 0)").is_err());
    assert!(tx.is_none(), "duplicate aborts the transaction");
    sess(&db, &mut tx, "BEGIN").unwrap();
    assert!(sess(&db, &mut tx, "INSERT INTO u VALUES (9, 'b', 0)").is_err());

    // Deleting a base row frees its keys for reuse within the transaction.
    let mut tx = None;
    sess(&db, &mut tx, "BEGIN").unwrap();
    sess(&db, &mut tx, "DELETE FROM u WHERE id = 1").unwrap();
    sess(&db, &mut tx, "INSERT INTO u VALUES (1, 'a', 1)").unwrap();
    // ... but the reused keys collide again inside the same transaction.
    assert!(sess(&db, &mut tx, "INSERT INTO u VALUES (1, 'z', 2)").is_err());

    // An update that moves a key frees the old one and claims the new one.
    let mut tx = None;
    sess(&db, &mut tx, "BEGIN").unwrap();
    sess(&db, &mut tx, "UPDATE u SET tag = 'c' WHERE id = 2").unwrap();
    sess(&db, &mut tx, "INSERT INTO u VALUES (3, 'b', 0)").unwrap();
    assert!(sess(&db, &mut tx, "INSERT INTO u VALUES (4, 'c', 0)").is_err());

    // A no-op key rewrite (same value onto the same row) never self-collides.
    let mut tx = None;
    sess(&db, &mut tx, "BEGIN").unwrap();
    sess(&db, &mut tx, "UPDATE u SET tag = 'b', v = 5 WHERE id = 2").unwrap();
    sess(&db, &mut tx, "COMMIT").unwrap();
    assert_eq!(
        rows(&db, "SELECT v FROM u WHERE id = 2"),
        vec![vec![Value::Int(5)]]
    );
}

/// DROP TABLE inside a transaction discards the table's constraint keys: a
/// re-created table accepts values the old one held.
#[test]
fn txn_drop_create_resets_uniqueness() {
    let (_d, db) = open();
    db.execute("CREATE TABLE u2 (id INT PRIMARY KEY)").unwrap();
    let mut tx = None;
    sess(&db, &mut tx, "BEGIN").unwrap();
    sess(&db, &mut tx, "INSERT INTO u2 VALUES (7)").unwrap();
    sess(&db, &mut tx, "DROP TABLE u2").unwrap();
    sess(&db, &mut tx, "CREATE TABLE u2 (id INT PRIMARY KEY)").unwrap();
    sess(&db, &mut tx, "INSERT INTO u2 VALUES (7)").unwrap();
    sess(&db, &mut tx, "COMMIT").unwrap();
    assert_eq!(rows(&db, "SELECT id FROM u2"), vec![vec![Value::Int(7)]]);
}

/// A transaction on a table whose column was lazily dropped before the txn
/// began: inserts/updates run in the logical schema but commit as the physical
/// layout the engine stores. Read-your-writes and the committed result agree.
#[test]
fn txn_on_dropped_column_table() {
    let (_d, db) = open();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY, a TEXT, b INT)")
        .unwrap();
    db.execute("INSERT INTO u VALUES (1, 'x', 10)").unwrap();
    db.execute("ALTER TABLE u DROP COLUMN a").unwrap(); // live schema: (id, b)

    let mut tx = None;
    sess(&db, &mut tx, "BEGIN").unwrap();
    sess(&db, &mut tx, "INSERT INTO u VALUES (2, 20)").unwrap();
    sess(&db, &mut tx, "UPDATE u SET b = 11 WHERE id = 1").unwrap();
    // Read-your-writes inside the txn sees the logical (2-column) rows.
    let r = sess(&db, &mut tx, "SELECT id, b FROM u ORDER BY id").unwrap();
    assert_eq!(
        sel(&r),
        vec![
            vec![Value::Int(1), Value::Int(11)],
            vec![Value::Int(2), Value::Int(20)],
        ]
    );
    sess(&db, &mut tx, "COMMIT").unwrap();
    assert_eq!(
        rows(&db, "SELECT id, b FROM u ORDER BY id"),
        vec![
            vec![Value::Int(1), Value::Int(11)],
            vec![Value::Int(2), Value::Int(20)],
        ]
    );
}

/// A UNIQUE column shifted by a dropped column keeps its constraint inside a
/// transaction — the engine's base-ownership probe translates the logical
/// position to the physical slot.
#[test]
fn txn_unique_after_dropped_column() {
    let (_d, db) = open();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY, junk TEXT, email TEXT UNIQUE)")
        .unwrap();
    db.execute("INSERT INTO u VALUES (1, 'j', 'a@x')").unwrap();
    db.execute("ALTER TABLE u DROP COLUMN junk").unwrap();

    let mut tx = None;
    sess(&db, &mut tx, "BEGIN").unwrap();
    // Colliding with a committed base row's email must be rejected — this
    // aborts and clears the transaction.
    assert!(sess(&db, &mut tx, "INSERT INTO u VALUES (2, 'a@x')").is_err());
    assert_eq!(tx, None);

    // A fresh transaction inserting a distinct email commits fine.
    sess(&db, &mut tx, "BEGIN").unwrap();
    sess(&db, &mut tx, "INSERT INTO u VALUES (2, 'b@x')").unwrap();
    sess(&db, &mut tx, "COMMIT").unwrap();
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM u"),
        vec![vec![Value::Int(2)]]
    );
}
