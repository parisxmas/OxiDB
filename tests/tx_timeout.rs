//! Idle-expiry for interactive transactions.
//!
//! The abandoned-transaction finding: the server rolls a transaction back
//! when its connection *closes*, but a client that vanishes while its
//! connection stays open (keep-alives, `OXIDB_IDLE_TIMEOUT=0`), or an
//! embedded caller that leaks a tx id, used to park state in
//! `active_transactions` — and any `find_for_update` locks — forever.
//! `OXIDB_TX_MAX_IDLE_SECS` (default 300, `0` = never) now expires those:
//! the state is dropped, the locks are released, and the owner is told
//! `TransactionExpired`, never the misleading `TransactionNotFound`.

use std::time::Duration;

use oxidb::{Error, OxiDb};
use serde_json::json;

fn db_with_idle_ms(ms: u64) -> OxiDb {
    let db = OxiDb::open_in_memory().unwrap();
    db.set_tx_max_idle_ms(ms);
    db
}

#[test]
fn an_abandoned_transaction_expires_and_its_writes_never_land() {
    let db = db_with_idle_ms(50);
    let tx = db.begin_transaction();
    db.tx_insert(tx, "accounts", json!({ "balance": 100 }))
        .unwrap();

    std::thread::sleep(Duration::from_millis(120));

    // The next touch reports the expiry (not "not found")...
    let err = db
        .tx_update(tx, "accounts", &json!({}), &json!({"$set": {"balance": 1}}))
        .unwrap_err();
    assert!(matches!(err, Error::TransactionExpired(id) if id == tx));

    // ...a late commit is refused with the same honest error...
    let err = db.commit_transaction(tx).unwrap_err();
    assert!(matches!(err, Error::TransactionExpired(id) if id == tx));

    // ...and the buffered insert never became visible.
    assert_eq!(db.find("accounts", &json!({})).unwrap().len(), 0);
}

#[test]
fn commit_of_an_expired_but_unswept_transaction_is_refused() {
    // No sweeper runs and no other op touches the tx first: commit itself
    // must notice the transaction sat idle past the limit.
    let db = db_with_idle_ms(50);
    let tx = db.begin_transaction();
    db.tx_insert(tx, "orders", json!({ "n": 1 })).unwrap();

    std::thread::sleep(Duration::from_millis(120));

    let err = db.commit_transaction(tx).unwrap_err();
    assert!(matches!(err, Error::TransactionExpired(id) if id == tx));
    assert_eq!(db.find("orders", &json!({})).unwrap().len(), 0);
}

#[test]
fn the_sweeper_frees_find_for_update_locks_of_a_dead_client() {
    let db = db_with_idle_ms(50);
    db.insert("hot", json!({ "k": 1 })).unwrap();

    let tx1 = db.begin_transaction();
    db.tx_find_for_update(tx1, "hot", &json!({"k": 1}), Duration::from_millis(500))
        .unwrap();

    // While tx1 is live its lock excludes other lockers.
    let tx2 = db.begin_transaction();
    let err = db
        .tx_find_for_update(tx2, "hot", &json!({"k": 1}), Duration::from_millis(100))
        .unwrap_err();
    assert!(matches!(err, Error::LockTimeout { .. }));
    db.rollback_transaction(tx2).unwrap();

    // tx1's client vanishes; the sweep alone must release the lock.
    std::thread::sleep(Duration::from_millis(120));
    db.expire_stale_transactions();

    let tx3 = db.begin_transaction();
    let locked = db
        .tx_find_for_update(tx3, "hot", &json!({"k": 1}), Duration::from_millis(100))
        .unwrap();
    assert_eq!(locked.len(), 1);
    db.rollback_transaction(tx3).unwrap();
}

#[test]
fn begin_transaction_sweeps_stale_ones() {
    let db = db_with_idle_ms(50);
    let tx1 = db.begin_transaction();
    std::thread::sleep(Duration::from_millis(120));

    // The sweep piggybacked on the next begin removes tx1 — a later touch
    // through the removed id still gets the accurate error.
    let _tx2 = db.begin_transaction();
    let err = db.tx_find(tx1, "c", &json!({})).unwrap_err();
    assert!(matches!(err, Error::TransactionExpired(id) if id == tx1));
}

#[test]
fn steady_activity_keeps_a_transaction_alive() {
    // Total lifetime far exceeds the idle limit, but every gap is under
    // it — the timeout is idle-based, so the transaction must survive.
    let db = db_with_idle_ms(500);
    db.insert("c", json!({ "v": 0 })).unwrap();

    let tx = db.begin_transaction();
    for _ in 0..4 {
        std::thread::sleep(Duration::from_millis(200));
        db.tx_find(tx, "c", &json!({})).unwrap();
    }
    db.tx_update(tx, "c", &json!({}), &json!({"$set": {"v": 1}}))
        .unwrap();
    db.commit_transaction(tx).unwrap();

    let docs = db.find("c", &json!({"v": 1})).unwrap();
    assert_eq!(docs.len(), 1);
}

#[test]
fn zero_disables_expiry() {
    let db = db_with_idle_ms(0);
    let tx = db.begin_transaction();
    db.tx_insert(tx, "c", json!({ "v": 1 })).unwrap();

    std::thread::sleep(Duration::from_millis(150));
    db.expire_stale_transactions();

    db.commit_transaction(tx).unwrap();
    assert_eq!(db.find("c", &json!({})).unwrap().len(), 1);
}
