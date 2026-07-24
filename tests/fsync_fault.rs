//! fsync-EIO durability — the "fsyncgate" class (Postgres, 2018): when
//! fsync fails, a database MUST NOT report the write as durable. The
//! failure mode that ate data in the wild was the opposite: fsync
//! returned an error once, the error was swallowed, a later fsync
//! returned success (the dirty pages had been dropped), and the
//! application believed data was safe when it was gone.
//!
//! OxiDB's WAL has a test-only fault seam (`oxidb::wal::fault`) that
//! forces a single fsync to return EIO. These tests prove:
//!
//!   1. a commit whose durability fsync fails returns an ERROR, never
//!      Ok — the write is not falsely acknowledged;
//!   2. after reopening the data dir, an un-acknowledged (fsync-failed)
//!      write is ABSENT — recovery discards it because it was never
//!      marked committed. Nothing is torn or half-applied;
//!   3. previously-acknowledged writes survive the failure untouched;
//!   4. the engine keeps working after a transient fsync failure.
//!
//! The seam is off by default (one relaxed atomic load per fsync), so
//! production is unaffected.
//!
//! Run with:
//!   cargo test --release --test fsync_fault -- --ignored --nocapture

use serde_json::json;
use std::sync::Mutex;
use tempfile::tempdir;

use oxidb::OxiDb;
use oxidb::wal::fault;

/// The fault seam is a PROCESS-GLOBAL static, so these tests must not
/// run concurrently — one test's armed fault would be consumed by
/// another's fsync. cargo runs tests in one binary in parallel by
/// default; this serializes them regardless of --test-threads.
static SERIAL: Mutex<()> = Mutex::new(());

fn count(db: &OxiDb, coll: &str) -> usize {
    db.count(coll, &json!({})).unwrap()
}

// ─────────────────────────────────────────────────────────────────────
// A failed fsync must surface as an error, not a false success — and
// the write it "acknowledged" must not survive recovery.
// ─────────────────────────────────────────────────────────────────────
#[test]
#[ignore]
fn fsync_failure_is_not_acknowledged_and_not_recovered() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    fault::disarm();
    let dir = tempdir().unwrap();
    {
        let db = OxiDb::open(dir.path()).unwrap();

        // Baseline: two durable inserts (real fsyncs).
        db.insert("ledger", json!({"id": 1, "v": "committed"}))
            .unwrap();
        db.insert("ledger", json!({"id": 2, "v": "committed"}))
            .unwrap();
        assert_eq!(count(&db, "ledger"), 2);

        // Arm the WAL to fail its next fsync, then attempt an insert.
        fault::fail_fsync_at(1);
        let r = db.insert("ledger", json!({"id": 3, "v": "doomed"}));
        fault::disarm();

        assert!(
            r.is_err(),
            "PINNED (fsyncgate): an insert whose WAL fsync fails MUST return \
             Err, not a false Ok. Got: {r:?}"
        );

        // The engine must keep working after a transient fsync error.
        db.insert("ledger", json!({"id": 4, "v": "committed"}))
            .unwrap();

        // Shut down cleanly — a final checkpoint fsyncs the surviving state.
        db.shutdown();
    }

    // Reopen: recovery must show a consistent world with the doomed
    // write ABSENT (never acknowledged) and every acknowledged write
    // present.
    let db = OxiDb::open(dir.path()).unwrap();
    let ids: Vec<i64> = db
        .find("ledger", &json!({}))
        .unwrap()
        .iter()
        .map(|d| d["id"].as_i64().unwrap())
        .collect();

    assert!(
        !ids.contains(&3),
        "PINNED: the fsync-failed write (id 3) must NOT survive recovery — \
         it was never acknowledged. Found ids: {ids:?}"
    );
    for must in [1, 2, 4] {
        assert!(
            ids.contains(&must),
            "acknowledged write id {must} lost after an unrelated fsync failure; ids: {ids:?}"
        );
    }
}

// ─────────────────────────────────────────────────────────────────────
// Same guarantee for a multi-op transaction: if the commit's fsync
// fails, the WHOLE transaction is rejected (all-or-nothing) — no partial
// application survives recovery.
// ─────────────────────────────────────────────────────────────────────
#[test]
#[ignore]
fn fsync_failure_rejects_whole_transaction() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    fault::disarm();
    let dir = tempdir().unwrap();
    {
        let db = OxiDb::open(dir.path()).unwrap();
        db.insert("acc", json!({"id": "a", "bal": 100})).unwrap();
        db.insert("acc", json!({"id": "b", "bal": 100})).unwrap();

        // A transfer transaction whose commit fsync will fail.
        let tx = db.begin_transaction();
        db.tx_update(
            tx,
            "acc",
            &json!({"id": "a"}),
            &json!({"$inc": {"bal": -50}}),
        )
        .unwrap();
        db.tx_update(
            tx,
            "acc",
            &json!({"id": "b"}),
            &json!({"$inc": {"bal": 50}}),
        )
        .unwrap();

        fault::fail_fsync_at(1);
        let r = db.commit_transaction(tx);
        fault::disarm();
        assert!(
            r.is_err(),
            "PINNED: a transaction commit whose fsync fails must return Err. Got {r:?}"
        );

        db.shutdown();
    }

    // Recovery: neither leg of the failed transfer may be present —
    // total must be exactly the untouched 200.
    let db = OxiDb::open(dir.path()).unwrap();
    let total: i64 = db
        .find("acc", &json!({}))
        .unwrap()
        .iter()
        .map(|d| d["bal"].as_i64().unwrap())
        .sum();
    assert_eq!(
        total, 200,
        "PINNED: a fsync-failed transfer must leave balances untouched \
         (all-or-nothing); a != 200 total means a partial commit survived"
    );
    let a = db.find_one("acc", &json!({"id": "a"})).unwrap().unwrap()["bal"]
        .as_i64()
        .unwrap();
    assert_eq!(a, 100, "account a must be untouched, not half-debited");
}

// ─────────────────────────────────────────────────────────────────────
// The seam itself is honest: disarmed = normal durability. Guards
// against a future refactor leaving the fault permanently on.
// ─────────────────────────────────────────────────────────────────────
#[test]
#[ignore]
fn seam_is_off_by_default() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    fault::disarm();
    let dir = tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();
    fault::disarm();
    for i in 0..20 {
        db.insert("t", json!({"i": i})).unwrap();
    }
    assert_eq!(count(&db, "t"), 20, "no fault armed → every insert durable");
}
