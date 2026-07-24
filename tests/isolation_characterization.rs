//! Isolation characterization — the Adya/Berenson anomaly taxonomy run
//! against OxiDB's OCC, each outcome pinned. This is the "what exactly
//! do transactions guarantee?" test an exchange (or an acquirer's due
//! diligence) asks for.
//!
//! ## The model these tests pin
//!
//! OxiDB transactions are **backward-validating OCC over item
//! read-sets**:
//!
//! - Reads inside a transaction see the latest COMMITTED state (no
//!   snapshot; read-committed-style reads) and do NOT see the
//!   transaction's own buffered writes.
//! - Every document a transaction reads (via `tx_find`/`tx_update`'s
//!   match phase) is recorded with its version. At commit, the entire
//!   read-set is validated: if any read document changed since, the
//!   commit aborts with `TransactionConflict`.
//!
//! Consequences, pinned below:
//! - Dirty write / lost update / classic write skew (item reads):
//!   **PREVENTED** — committed transactions are serializable with
//!   respect to the items they read and wrote.
//! - Read skew is OBSERVABLE mid-transaction (no snapshot), but a
//!   transaction that observed it **cannot commit a write** — commit
//!   validation catches the stale read. Read-only observers are not
//!   validated against; they may see non-repeatable reads.
//! - Phantoms: **ADMITTED** — a predicate query's read-set contains
//!   only the documents it RETURNED. A concurrent insert (or an update
//!   that makes a previously non-matching doc match) is invisible to
//!   validation, so predicate-based constraints can be violated
//!   (phantom write skew). Mitigation, also pinned: materialize the
//!   constraint into a document every writer must read-modify-write
//!   (a counter / lock doc) — then OCC serializes on it.
//!
//! Interleavings are single-threaded and deterministic: writes buffer
//! until commit, so "begin A, begin B, both read, A commits, B commits"
//! needs no barriers.
//!
//! Run with:
//!   cargo test --release --test isolation_characterization -- --ignored --nocapture

use serde_json::json;
use tempfile::tempdir;

use oxidb::{Error, OxiDb};

fn fresh_db() -> (tempfile::TempDir, OxiDb) {
    let dir = tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();
    (dir, db)
}

fn is_conflict(e: &Error) -> bool {
    matches!(e, Error::TransactionConflict { .. })
}

// ─────────────────────────────────────────────────────────────────────
// G0 — dirty write: two concurrent blind writes to the same document.
// Exactly one may win; the loser must abort, not silently interleave.
// ─────────────────────────────────────────────────────────────────────
#[test]
#[ignore]
fn g0_dirty_write_prevented() {
    let (_d, db) = fresh_db();
    db.insert("acc", json!({"id": "x", "v": 0})).unwrap();

    let t1 = db.begin_transaction();
    let t2 = db.begin_transaction();
    db.tx_update(t1, "acc", &json!({"id": "x"}), &json!({"$set": {"v": 1}}))
        .unwrap();
    db.tx_update(t2, "acc", &json!({"id": "x"}), &json!({"$set": {"v": 2}}))
        .unwrap();

    db.commit_transaction(t1).unwrap();
    let r2 = db.commit_transaction(t2);
    assert!(
        r2.as_ref().err().map(is_conflict).unwrap_or(false),
        "PINNED: second blind writer must abort with TransactionConflict, got {r2:?}"
    );
    let v = db.find_one("acc", &json!({"id": "x"})).unwrap().unwrap()["v"]
        .as_i64()
        .unwrap();
    assert_eq!(v, 1, "winner's write intact");
}

// ─────────────────────────────────────────────────────────────────────
// P4 — lost update: two read-modify-write cycles on the same document.
// The canonical "balance += x" race. One must abort.
// ─────────────────────────────────────────────────────────────────────
#[test]
#[ignore]
fn p4_lost_update_prevented() {
    let (_d, db) = fresh_db();
    db.insert("acc", json!({"id": "x", "bal": 100})).unwrap();

    let t1 = db.begin_transaction();
    let t2 = db.begin_transaction();
    // Both read the same version…
    let b1 = db.tx_find(t1, "acc", &json!({"id": "x"})).unwrap()[0]["bal"]
        .as_i64()
        .unwrap();
    let b2 = db.tx_find(t2, "acc", &json!({"id": "x"})).unwrap()[0]["bal"]
        .as_i64()
        .unwrap();
    assert_eq!((b1, b2), (100, 100));
    // …and both write a derived value.
    db.tx_update(
        t1,
        "acc",
        &json!({"id": "x"}),
        &json!({"$set": {"bal": b1 - 10}}),
    )
    .unwrap();
    db.tx_update(
        t2,
        "acc",
        &json!({"id": "x"}),
        &json!({"$set": {"bal": b2 - 20}}),
    )
    .unwrap();

    db.commit_transaction(t1).unwrap();
    let r2 = db.commit_transaction(t2);
    assert!(
        r2.as_ref().err().map(is_conflict).unwrap_or(false),
        "PINNED: lost update must be prevented — t2 read a version t1 invalidated, got {r2:?}"
    );
    let bal = db.find_one("acc", &json!({"id": "x"})).unwrap().unwrap()["bal"]
        .as_i64()
        .unwrap();
    assert_eq!(
        bal, 90,
        "only t1's update applied; t2's -20 must not be lost-update-merged"
    );
}

// ─────────────────────────────────────────────────────────────────────
// A5B — write skew over ITEM reads (the classic two-accounts case).
// Both txs read both docs, each writes a different doc. Read-set
// validation catches the cross dependency: exactly one commits, the
// cross-row constraint (total ≥ 0) survives.
// ─────────────────────────────────────────────────────────────────────
#[test]
#[ignore]
fn a5b_write_skew_item_reads_prevented() {
    let (_d, db) = fresh_db();
    db.insert("acc", json!({"id": "x", "bal": 50})).unwrap();
    db.insert("acc", json!({"id": "y", "bal": 50})).unwrap();

    let t1 = db.begin_transaction();
    let t2 = db.begin_transaction();
    let total = |tx: u64| -> i64 {
        db.tx_find(tx, "acc", &json!({}))
            .unwrap()
            .iter()
            .map(|d| d["bal"].as_i64().unwrap())
            .sum()
    };
    // Both check the constraint against the same committed state.
    assert_eq!(total(t1), 100);
    assert_eq!(total(t2), 100);
    // Each withdraws 60 from a DIFFERENT account (100 - 60 >= 0 holds
    // for each in isolation; both together would violate it).
    db.tx_update(
        t1,
        "acc",
        &json!({"id": "x"}),
        &json!({"$inc": {"bal": -60}}),
    )
    .unwrap();
    db.tx_update(
        t2,
        "acc",
        &json!({"id": "y"}),
        &json!({"$inc": {"bal": -60}}),
    )
    .unwrap();

    db.commit_transaction(t1).unwrap();
    let r2 = db.commit_transaction(t2);
    assert!(
        r2.as_ref().err().map(is_conflict).unwrap_or(false),
        "PINNED: item-read write skew must be prevented — t2's read-set \
         includes x, which t1 changed. Got {r2:?}"
    );

    let final_total: i64 = db
        .find("acc", &json!({}))
        .unwrap()
        .iter()
        .map(|d| d["bal"].as_i64().unwrap())
        .sum();
    assert_eq!(final_total, 40, "constraint survives: 100 - 60, never -20");
}

// ─────────────────────────────────────────────────────────────────────
// Phantom write skew — the one OCC over item read-sets ADMITS.
// Constraint: "at most 3 open orders". Both txs COUNT open orders via a
// predicate (sees 2), both INSERT a new one. Neither read-set contains
// the other's insert, so validation passes for both → 4 open orders.
// PINNED as admitted; the mitigation test below shows the fix.
// ─────────────────────────────────────────────────────────────────────
#[test]
#[ignore]
fn phantom_write_skew_admitted() {
    let (_d, db) = fresh_db();
    db.insert("orders", json!({"id": 1, "status": "open"}))
        .unwrap();
    db.insert("orders", json!({"id": 2, "status": "open"}))
        .unwrap();

    let t1 = db.begin_transaction();
    let t2 = db.begin_transaction();
    let open1 = db
        .tx_find(t1, "orders", &json!({"status": "open"}))
        .unwrap()
        .len();
    let open2 = db
        .tx_find(t2, "orders", &json!({"status": "open"}))
        .unwrap()
        .len();
    assert_eq!(
        (open1, open2),
        (2, 2),
        "both see 2 open — room for 1 more each"
    );
    db.tx_insert(t1, "orders", json!({"id": 3, "status": "open"}))
        .unwrap();
    db.tx_insert(t2, "orders", json!({"id": 4, "status": "open"}))
        .unwrap();

    let r1 = db.commit_transaction(t1);
    let r2 = db.commit_transaction(t2);
    let open_now = db.find("orders", &json!({"status": "open"})).unwrap().len();

    eprintln!(
        "[phantom-skew] r1={:?} r2={:?} open_now={open_now}",
        r1.is_ok(),
        r2.is_ok()
    );
    assert!(
        r1.is_ok() && r2.is_ok(),
        "PINNED: both commits succeed (phantom)"
    );
    assert_eq!(
        open_now, 4,
        "PINNED: predicate constraint violated via phantom — OxiDB's OCC \
         validates item read-sets, not predicates. If this fails, predicate \
         locking / SSI was added: update docs/isolation.md."
    );
}

// ─────────────────────────────────────────────────────────────────────
// Phantom mitigation — materialize the predicate constraint into a
// counter document every writer must read-modify-write. The counter
// collides in both txs' read/write sets, so OCC serializes on it and
// the constraint holds. This is the documented pattern for
// predicate-based invariants (order caps, position limits, risk caps).
// ─────────────────────────────────────────────────────────────────────
#[test]
#[ignore]
fn phantom_write_skew_mitigated_by_counter_doc() {
    let (_d, db) = fresh_db();
    db.insert("orders", json!({"id": 1, "status": "open"}))
        .unwrap();
    db.insert("orders", json!({"id": 2, "status": "open"}))
        .unwrap();
    db.insert("meta", json!({"id": "open_orders", "count": 2}))
        .unwrap();

    let t1 = db.begin_transaction();
    let t2 = db.begin_transaction();
    let count_of = |tx: u64| -> i64 {
        db.tx_find(tx, "meta", &json!({"id": "open_orders"}))
            .unwrap()[0]["count"]
            .as_i64()
            .unwrap()
    };
    // Both check the materialized constraint (< 3) and stake their claim
    // by bumping the counter — the shared read+write is what OCC needs.
    assert!(count_of(t1) < 3);
    db.tx_insert(t1, "orders", json!({"id": 3, "status": "open"}))
        .unwrap();
    db.tx_update(
        t1,
        "meta",
        &json!({"id": "open_orders"}),
        &json!({"$inc": {"count": 1}}),
    )
    .unwrap();
    assert!(count_of(t2) < 3);
    db.tx_insert(t2, "orders", json!({"id": 4, "status": "open"}))
        .unwrap();
    db.tx_update(
        t2,
        "meta",
        &json!({"id": "open_orders"}),
        &json!({"$inc": {"count": 1}}),
    )
    .unwrap();

    db.commit_transaction(t1).unwrap();
    let r2 = db.commit_transaction(t2);
    assert!(
        r2.as_ref().err().map(is_conflict).unwrap_or(false),
        "counter doc must serialize the two claims — t2 aborts, got {r2:?}"
    );

    let open_now = db.find("orders", &json!({"status": "open"})).unwrap().len();
    assert_eq!(
        open_now, 3,
        "constraint holds: t2's insert died with its tx"
    );
}

// ─────────────────────────────────────────────────────────────────────
// A5A — read skew: observable mid-transaction (no snapshot), but a
// transaction that acts on the stale read CANNOT commit. This is the
// exchange-relevant half: you may briefly SEE an inconsistent world,
// but you can't WRITE based on it.
// ─────────────────────────────────────────────────────────────────────
#[test]
#[ignore]
fn a5a_read_skew_observable_but_not_actionable() {
    let (_d, db) = fresh_db();
    db.insert("acc", json!({"id": "x", "bal": 50})).unwrap();
    db.insert("acc", json!({"id": "y", "bal": 50})).unwrap();

    let t1 = db.begin_transaction();
    let x1 = db.tx_find(t1, "acc", &json!({"id": "x"})).unwrap()[0]["bal"]
        .as_i64()
        .unwrap();
    assert_eq!(x1, 50);

    // Concurrent committed swap: x -30, y +30.
    let t2 = db.begin_transaction();
    db.tx_update(
        t2,
        "acc",
        &json!({"id": "x"}),
        &json!({"$inc": {"bal": -30}}),
    )
    .unwrap();
    db.tx_update(
        t2,
        "acc",
        &json!({"id": "y"}),
        &json!({"$inc": {"bal": 30}}),
    )
    .unwrap();
    db.commit_transaction(t2).unwrap();

    // PINNED (observation): t1 sees the fresh y=80 next to its stale
    // x=50 — reads are read-committed, no snapshot.
    let y1 = db.tx_find(t1, "acc", &json!({"id": "y"})).unwrap()[0]["bal"]
        .as_i64()
        .unwrap();
    assert_eq!(
        y1, 80,
        "PINNED: mid-tx reads see latest committed (no snapshot)"
    );

    // PINNED (the guarantee): t1 tries to act on its inconsistent view —
    // commit must abort, because its read-set contains x@old-version.
    db.tx_update(t1, "acc", &json!({"id": "y"}), &json!({"$set": {"bal": 0}}))
        .unwrap();
    let r1 = db.commit_transaction(t1);
    assert!(
        r1.as_ref().err().map(is_conflict).unwrap_or(false),
        "PINNED: a writer that observed read skew must fail validation, got {r1:?}"
    );
}

// ─────────────────────────────────────────────────────────────────────
// Read-your-own-writes within a transaction: PINNED as NOT provided by
// tx_find (it reads committed state; buffered writes are invisible
// until commit). The writes still COMPOSE at commit (the staged-writes
// fix) — this pins the read surface so the quirk is documented, not
// discovered in production.
// ─────────────────────────────────────────────────────────────────────
#[test]
#[ignore]
fn read_your_own_writes_pinned_not_visible_mid_tx() {
    let (_d, db) = fresh_db();
    db.insert("acc", json!({"id": "x", "bal": 100})).unwrap();

    let t1 = db.begin_transaction();
    db.tx_update(
        t1,
        "acc",
        &json!({"id": "x"}),
        &json!({"$inc": {"bal": -40}}),
    )
    .unwrap();
    let seen = db.tx_find(t1, "acc", &json!({"id": "x"})).unwrap()[0]["bal"]
        .as_i64()
        .unwrap();
    assert_eq!(
        seen, 100,
        "PINNED: tx_find does NOT see the tx's own buffered write. If this \
         fails, read-your-own-writes was added — update docs/isolation.md \
         (and celebrate)."
    );
    db.commit_transaction(t1).unwrap();
    let after = db.find_one("acc", &json!({"id": "x"})).unwrap().unwrap()["bal"]
        .as_i64()
        .unwrap();
    assert_eq!(after, 60, "the buffered write still applies at commit");
}

// ─────────────────────────────────────────────────────────────────────
// G1b — intermediate read, PINNED AS ADMITTED FOR READ-ONLY OBSERVERS.
// Plain (non-tx) reads do not take the commit lock, so a reader racing
// a commit's apply phase can observe SOME of a transaction's writes
// before the rest land (this run reliably observes it). Equivalent to
// MongoDB's "local" read concern.
//
// The load-bearing guarantee is the other half: any TRANSACTION that
// acts on such a torn view cannot commit — its read-set contains a
// stale version and validation aborts it (proven deterministically in
// a5a_read_skew_observable_but_not_actionable). Torn visibility is a
// report/monitoring concern, not a ledger-integrity one. If snapshot
// reads are ever added, flip this pin.
// ─────────────────────────────────────────────────────────────────────
#[test]
#[ignore]
fn g1b_intermediate_reads_admitted_for_observers() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;

    let dir = tempdir().unwrap();
    let db = Arc::new(OxiDb::open(dir.path()).unwrap());
    db.insert("pair", json!({"id": "a", "v": 0})).unwrap();
    db.insert("pair", json!({"id": "b", "v": 0})).unwrap();

    let stop = Arc::new(AtomicBool::new(false));

    // Writer: atomically sets a and b to the same ever-increasing value.
    let w_db = Arc::clone(&db);
    let w_stop = Arc::clone(&stop);
    let writer = thread::spawn(move || {
        let mut i: i64 = 1;
        while !w_stop.load(Ordering::Relaxed) {
            let tx = w_db.begin_transaction();
            let s1 = w_db.tx_update(tx, "pair", &json!({"id": "a"}), &json!({"$set": {"v": i}}));
            let s2 = w_db.tx_update(tx, "pair", &json!({"id": "b"}), &json!({"$set": {"v": i}}));
            if s1.is_ok() && s2.is_ok() {
                let _ = w_db.commit_transaction(tx); // conflicts fine; partial application is not
            } else {
                let _ = w_db.rollback_transaction(tx);
            }
            i += 1;
        }
    });

    // Reader: count how often a != b — each mismatch is a torn
    // (intermediate) view of a half-applied transaction.
    let mut torn = 0u64;
    let mut checks = 0u64;
    let start = std::time::Instant::now();
    while start.elapsed() < std::time::Duration::from_secs(3) {
        let docs = db.find("pair", &json!({})).unwrap();
        if docs.len() == 2 {
            let va = docs.iter().find(|d| d["id"] == "a").unwrap()["v"]
                .as_i64()
                .unwrap();
            let vb = docs.iter().find(|d| d["id"] == "b").unwrap()["v"]
                .as_i64()
                .unwrap();
            checks += 1;
            if va != vb {
                torn += 1;
            }
        }
    }
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();

    eprintln!("[G1b] {checks} checks, {torn} torn (torn > 0 is the PINNED status quo)");
    assert!(checks > 100, "reader must actually have raced the writer");
    // No assertion on `torn`: > 0 is the current, documented behavior,
    // but the race is probabilistic and a lucky zero run must not fail
    // the suite. The pin lives in the writer-protection test (a5a) and
    // docs/isolation.md; if snapshot reads land, torn becomes always-0
    // and this comment + docs get updated.
}
