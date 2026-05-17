//! CERN-grade ACID test #2 — isolation anomaly suite.
//!
//! Category 1 (ACID & isolation) in `docs/testing-roadmap.md`. PR #42
//! added the money-conservation test (catches lost-update / partial-
//! commit on the write path). This adds the three classic *isolation*
//! anomalies: dirty read, phantom read, write skew.
//!
//! Each test pins OxiDB's observed behaviour. We don't pre-assume an
//! isolation level — we DISCOVER it empirically and bake that into
//! the assertion. If the engine's behaviour ever changes (e.g. SSI
//! is added later and write-skew flips from "happens" to "blocked"),
//! the test fails loudly so the change is intentional, not silent.
//!
//! Run with:
//!   cargo test --test cern_acid_isolation -- --ignored --nocapture

use serde_json::{json, Value};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

use oxidb::OxiDb;

// ─────────────────────────────────────────────────────────────────────
// Dirty read — uncommitted writes MUST NOT be visible to other readers.
// This is the most basic ACID property. A failure here would mean any
// transaction's in-flight writes leak to concurrent queries.
// ─────────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn dirty_read_does_not_occur() {
    let dir = tempdir().unwrap();
    let db = Arc::new(OxiDb::open(dir.path()).unwrap());

    let tx = db.begin_transaction();
    db.tx_insert(tx, "secret", json!({"key": "alpha", "value": 42}))
        .unwrap();
    db.tx_insert(tx, "secret", json!({"key": "beta", "value": 99}))
        .unwrap();

    // Auto-commit (non-tx) read MUST see nothing — the writes above are
    // still buffered in tx and haven't reached the data file.
    let auto = db.find("secret", &json!({})).unwrap();
    assert_eq!(
        auto.len(), 0,
        "DIRTY READ: auto-commit find() saw {} uncommitted records",
        auto.len()
    );

    // Read from a completely different tx must also see nothing.
    let tx2 = db.begin_transaction();
    let other = db.tx_find(tx2, "secret", &json!({})).unwrap();
    assert_eq!(
        other.len(), 0,
        "DIRTY READ: a different tx saw {} uncommitted records from tx1",
        other.len()
    );
    db.commit_transaction(tx2).unwrap();

    // After commit, both reads see the records.
    db.commit_transaction(tx).unwrap();
    let visible = db.find("secret", &json!({})).unwrap();
    assert_eq!(visible.len(), 2, "after commit, records must be visible");
}

// ─────────────────────────────────────────────────────────────────────
// Phantom read — a long-running read tx re-issues the same predicate
// query. Between the two reads, a different tx commits an insert that
// matches the predicate. Does the long-running tx see the new row?
//
//   Snapshot isolation     → NO  (read returns the same set both times)
//   Read committed         → YES (phantom appears)
//
// We empirically capture which one OxiDB does and pin it. Behaviour
// change later = deliberate.
// ─────────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn phantom_read_pinned_behaviour() {
    let dir = tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();

    db.insert("items", json!({"id": "a", "price": 100})).unwrap();

    let tx1 = db.begin_transaction();
    let before = db
        .tx_find(tx1, "items", &json!({"price": {"$gte": 50}}))
        .unwrap();
    assert_eq!(before.len(), 1, "setup precondition");

    // Concurrent committed insert that matches the predicate.
    let tx2 = db.begin_transaction();
    db.tx_insert(tx2, "items", json!({"id": "b", "price": 200}))
        .unwrap();
    db.commit_transaction(tx2).unwrap();

    // tx1 re-reads with the same predicate.
    let after = db
        .tx_find(tx1, "items", &json!({"price": {"$gte": 50}}))
        .unwrap();

    eprintln!(
        "[phantom-read] tx1 saw {} before tx2 committed, {} after",
        before.len(),
        after.len()
    );

    // PINNED: OxiDB OCC currently exposes phantoms — reads inside a
    // tx see the latest committed data, not a snapshot taken at
    // begin_transaction time. If/when snapshot isolation is added,
    // this assertion flips to `after.len() == 1` and that flip is
    // intentional (not a regression).
    assert_eq!(
        after.len(),
        2,
        "PINNED isolation level: OxiDB currently exposes phantoms \
         (read-committed semantics). If this assertion fails, the \
         engine has been promoted to a stronger isolation level — \
         update the test + docs/format/tx-commit-log.md."
    );

    // tx1 didn't write anything, so it commits cleanly.
    db.commit_transaction(tx1).unwrap();
}

// ─────────────────────────────────────────────────────────────────────
// Write skew (Berenson A5B) — the canonical case OCC alone CAN'T
// detect. Two txs read overlapping data, each writes a DIFFERENT row,
// and a cross-row constraint is violated even though no single row had
// a write-write conflict.
//
// Setup: two accounts, each balance 50, constraint: total ≥ 0.
//   tx1 reads both → total = 100 → withdraws 60 from x
//   tx2 reads both → total = 100 → withdraws 60 from y   (sees stale)
// If both commit, total goes to -20 — constraint violated.
//
// Snapshot isolation: write skew CAN occur (each tx reads its own
//   snapshot, neither sees the other's write at validation).
// Serializable: write skew IS blocked.
// ─────────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn write_skew_pinned_behaviour() {
    let dir = tempdir().unwrap();
    let db = Arc::new(OxiDb::open(dir.path()).unwrap());

    db.insert("accounts", json!({"id": "x", "balance": 50i64})).unwrap();
    db.insert("accounts", json!({"id": "y", "balance": 50i64})).unwrap();

    // Coordinate the two threads so they overlap in time — both must
    // open their tx and read the world BEFORE either commits.
    let barrier_read_done = Arc::new(AtomicI64::new(0));
    let barrier_commit_ok = Arc::new(AtomicI64::new(0));
    let proceed_to_commit = Arc::new(AtomicBool::new(false));

    let total_balance = |db: &OxiDb| -> i64 {
        db.find("accounts", &json!({}))
            .unwrap()
            .iter()
            .map(|d: &Value| d["balance"].as_i64().unwrap())
            .sum()
    };
    let initial = total_balance(&db);
    assert_eq!(initial, 100);

    let worker = |from: &'static str, db: Arc<OxiDb>,
                  read_done: Arc<AtomicI64>,
                  commit_ok: Arc<AtomicI64>,
                  proceed: Arc<AtomicBool>| {
        thread::spawn(move || {
            let tx = db.begin_transaction();
            let docs = db.tx_find(tx, "accounts", &json!({})).unwrap();
            let total: i64 = docs.iter().map(|d| d["balance"].as_i64().unwrap()).sum();

            read_done.fetch_add(1, Ordering::SeqCst);
            // Spin until both threads have done their read.
            while !proceed.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_micros(50));
            }

            // Apply constraint: only withdraw if it keeps total ≥ 0.
            if total - 60 >= 0 {
                db.tx_update(
                    tx,
                    "accounts",
                    &json!({"id": from}),
                    &json!({"$inc": {"balance": -60i64}}),
                )
                .unwrap();
                if db.commit_transaction(tx).is_ok() {
                    commit_ok.fetch_add(1, Ordering::SeqCst);
                }
            } else {
                db.rollback_transaction(tx).ok();
            }
        })
    };

    let h1 = worker("x", db.clone(), barrier_read_done.clone(), barrier_commit_ok.clone(), proceed_to_commit.clone());
    let h2 = worker("y", db.clone(), barrier_read_done.clone(), barrier_commit_ok.clone(), proceed_to_commit.clone());

    // Wait for both reads to land, then release both threads to commit.
    while barrier_read_done.load(Ordering::SeqCst) < 2 {
        thread::sleep(Duration::from_micros(50));
    }
    proceed_to_commit.store(true, Ordering::SeqCst);

    h1.join().unwrap();
    h2.join().unwrap();

    let final_total = total_balance(&db);
    let committed = barrier_commit_ok.load(Ordering::SeqCst);

    eprintln!(
        "[write-skew] commits={committed} final_total={final_total} \
         (started at {initial}, each withdrew 60)"
    );

    // PINNED: OxiDB's OCC validates write-set conflicts. The two txs
    // touched DIFFERENT documents (x and y), so OCC sees no conflict
    // and lets both commit. Result: final_total = 100 - 120 = -20,
    // write skew occurred. If/when SSI lands and final_total stays
    // ≥ 40 (one tx aborted, one committed), this assertion flips and
    // the flip is the intentional promotion of the isolation level.
    assert_eq!(
        committed, 2,
        "PINNED: OCC alone admits write skew — both txs commit. \
         If this fails, isolation has been promoted; update test + docs."
    );
    assert_eq!(
        final_total, -20,
        "PINNED: write skew → total goes negative. \
         If this fails, isolation has been promoted."
    );
}
