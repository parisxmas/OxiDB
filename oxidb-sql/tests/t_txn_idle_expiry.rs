//! Idle-expiry for parked interactive (session) transactions.
//!
//! Same finding as the document engine's `tests/tx_timeout.rs`: the server
//! rolls a session transaction back when its connection *closes*, but a
//! client that vanishes while the connection stays open — or an embedded
//! caller that leaks a tx id — used to park buffered state and
//! `SELECT ... FOR UPDATE` row locks in `session_txns` forever.
//! `OXIDB_TX_MAX_IDLE_SECS` (shared knob with the document engine; default
//! 300, `0` = never) now expires those, and the owner is told
//! `TxnExpired`, never the generic "no such transaction".

mod common;

use std::time::Duration;

use common::*;
use oxidb_sql::{QueryResult, SqlEngine, SqlError, Value};

fn sess(db: &SqlEngine, tx: &mut Option<u64>, sql: &str) -> oxidb_sql::Result<Vec<QueryResult>> {
    db.execute_params_in_session(sql, &[], tx)
}

fn seed(db: &SqlEngine) {
    db.execute("CREATE TABLE h (id INT PRIMARY KEY AUTO_INCREMENT, v INT)")
        .unwrap();
    db.execute("INSERT INTO h (v) VALUES (1), (2)").unwrap();
}

#[test]
fn an_abandoned_transaction_expires_and_its_writes_never_land() {
    let (_d, db) = open();
    seed(&db);
    db.set_txn_max_idle_ms(50);

    let mut tx = None;
    sess(&db, &mut tx, "BEGIN; INSERT INTO h (v) VALUES (99)").unwrap();
    let id = tx.expect("transaction parked");

    std::thread::sleep(Duration::from_millis(120));

    // The next statement reports the expiry (not "no such transaction")...
    let err = sess(&db, &mut tx, "SELECT COUNT(*) FROM h").unwrap_err();
    assert!(
        matches!(err, SqlError::TxnExpired(e) if e == id),
        "got: {err}"
    );
    // ...and the session is cleared so it can start fresh, rather than
    // repeating the error forever.
    assert_eq!(tx, None);

    // The buffered insert never became visible.
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM h"),
        vec![vec![Value::Int(2)]]
    );
}

#[test]
fn a_late_commit_through_the_stale_id_reports_expired() {
    let (_d, db) = open();
    seed(&db);
    db.set_txn_max_idle_ms(50);

    let mut tx = None;
    sess(&db, &mut tx, "BEGIN; DELETE FROM h").unwrap();
    let id = tx.unwrap();

    std::thread::sleep(Duration::from_millis(120));
    db.expire_stale_session_txns();

    // The sweeper already removed it; the remembered id keeps the error
    // honest for the returning client.
    let mut stale = Some(id);
    let err = sess(&db, &mut stale, "COMMIT").unwrap_err();
    assert!(
        matches!(err, SqlError::TxnExpired(e) if e == id),
        "got: {err}"
    );
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM h"),
        vec![vec![Value::Int(2)]]
    );
}

#[test]
fn the_sweeper_frees_for_update_row_locks_of_a_dead_client() {
    let (_d, db) = open();
    seed(&db);
    db.set_txn_max_idle_ms(50);

    let mut tx1 = None;
    sess(
        &db,
        &mut tx1,
        "BEGIN; SELECT * FROM h WHERE id = 1 FOR UPDATE",
    )
    .unwrap();

    // While tx1 is parked, its row lock excludes another transaction.
    let mut tx2 = None;
    sess(&db, &mut tx2, "BEGIN").unwrap();
    let err = sess(&db, &mut tx2, "SELECT * FROM h WHERE id = 1 FOR UPDATE").unwrap_err();
    assert!(matches!(err, SqlError::LockTimeout { .. }), "got: {err}");

    // tx1's client vanishes; the sweep alone must release the lock.
    std::thread::sleep(Duration::from_millis(120));
    db.expire_stale_session_txns();

    let mut tx3 = None;
    sess(&db, &mut tx3, "BEGIN").unwrap();
    let r = sess(&db, &mut tx3, "SELECT * FROM h WHERE id = 1 FOR UPDATE").unwrap();
    match r.last().unwrap() {
        QueryResult::Select { rows, .. } => assert_eq!(rows.len(), 1),
        other => panic!("expected Select, got {other:?}"),
    }
    sess(&db, &mut tx3, "ROLLBACK").unwrap();
}

#[test]
fn steady_activity_keeps_a_transaction_alive() {
    // Total lifetime far exceeds the idle limit, but every gap is under
    // it — the clock resets at each park, so the transaction survives.
    let (_d, db) = open();
    seed(&db);
    db.set_txn_max_idle_ms(500);

    let mut tx = None;
    sess(&db, &mut tx, "BEGIN").unwrap();
    for _ in 0..4 {
        std::thread::sleep(Duration::from_millis(200));
        sess(&db, &mut tx, "SELECT COUNT(*) FROM h").unwrap();
    }
    sess(&db, &mut tx, "INSERT INTO h (v) VALUES (3); COMMIT").unwrap();
    assert_eq!(tx, None);
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM h"),
        vec![vec![Value::Int(3)]]
    );
}

#[test]
fn zero_disables_expiry() {
    let (_d, db) = open();
    seed(&db);
    db.set_txn_max_idle_ms(0);

    let mut tx = None;
    sess(&db, &mut tx, "BEGIN; INSERT INTO h (v) VALUES (3)").unwrap();
    std::thread::sleep(Duration::from_millis(150));
    db.expire_stale_session_txns();

    sess(&db, &mut tx, "COMMIT").unwrap();
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM h"),
        vec![vec![Value::Int(3)]]
    );
}
