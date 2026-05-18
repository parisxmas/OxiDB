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

// ─────────────────────────────────────────────────────────────────────
// Read skew (Berenson A5A / "monotonic reads") — a read-only tx
// observes mutually inconsistent values because its reads straddle
// another tx's commit. Distinct from phantom-read (which is about
// predicate queries returning different result sets) and write-skew
// (which involves the long tx WRITING). Here tx1 only READS — and
// still sees inconsistency.
//
// Setup: X=50, Y=50, cross-row invariant X+Y=100.
//   tx1 begins, reads X (sees 50)
//   tx2 begins, swaps 30 between X and Y, commits (X=20, Y=80)
//   tx1 reads Y
//     - Snapshot isolation: Y=50 (matches snapshot at tx1's begin)
//     - Read committed:     Y=80 (sees tx2's commit) → tx1's view
//                                  of the world is X=50 (stale)
//                                  + Y=80 (fresh) → invariant
//                                  appears violated FROM tx1's POV
// ─────────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn read_skew_pinned_behaviour() {
    let dir = tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();

    db.insert("accts", json!({"id": "x", "balance": 50i64})).unwrap();
    db.insert("accts", json!({"id": "y", "balance": 50i64})).unwrap();

    // tx1 begins. First half of a read-only consistency check.
    let tx1 = db.begin_transaction();
    let x_seen_by_tx1 = db
        .tx_find(tx1, "accts", &json!({"id": "x"}))
        .unwrap()[0]["balance"]
        .as_i64()
        .unwrap();
    assert_eq!(x_seen_by_tx1, 50, "setup precondition");

    // tx2: atomic 30-unit swap between x and y, preserving the
    // invariant from tx2's own perspective.
    let tx2 = db.begin_transaction();
    db.tx_update(
        tx2,
        "accts",
        &json!({"id": "x"}),
        &json!({"$inc": {"balance": -30i64}}),
    )
    .unwrap();
    db.tx_update(
        tx2,
        "accts",
        &json!({"id": "y"}),
        &json!({"$inc": {"balance":  30i64}}),
    )
    .unwrap();
    db.commit_transaction(tx2).unwrap();

    // tx1 reads Y. Under read-committed it'll see tx2's update;
    // under snapshot isolation it'd see the pre-tx2 value.
    let y_seen_by_tx1 = db
        .tx_find(tx1, "accts", &json!({"id": "y"}))
        .unwrap()[0]["balance"]
        .as_i64()
        .unwrap();

    let tx1_perceived_total = x_seen_by_tx1 + y_seen_by_tx1;
    eprintln!(
        "[read-skew] tx1 saw x={x_seen_by_tx1}, y={y_seen_by_tx1}, \
         tx1's perceived X+Y = {tx1_perceived_total} (real invariant = 100)"
    );

    // PINNED: OxiDB is read-committed. tx1's reads straddle tx2's
    // commit, so X is stale (50) and Y is fresh (80) — tx1 sees the
    // world as X+Y=130, which is impossible under any single point
    // in real history.
    //
    // If/when SSI lands and tx1's snapshot is preserved, y_seen_by_tx1
    // will be 50 and tx1_perceived_total will be 100. That flip is
    // the intentional documentation that isolation got stronger.
    assert_eq!(
        y_seen_by_tx1, 80,
        "PINNED: read-committed → tx1 sees tx2's committed update. \
         If this fails, isolation has been promoted to SI/SSI; \
         update this test + docs/format/tx-commit-log.md."
    );
    assert_eq!(
        tx1_perceived_total, 130,
        "PINNED: read-skew → tx1's worldview violates the invariant. \
         If this fails, isolation has been promoted."
    );

    // SECOND FINDING (orthogonal to read-skew itself):
    //
    // OxiDB's OCC validates the READ-SET at commit, even for tx1
    // which only read. Since tx2 bumped x's version between tx1's
    // begin and commit, tx1's commit_transaction returns
    // `TransactionConflict` with `expected_version: 1, actual_version: 2`.
    //
    // This is *stronger* than pure read-committed and closer to
    // OPTIMISTIC SNAPSHOT ISOLATION at commit time — the
    // application learns its reads were inconsistent and can
    // retry. (Read-committed alone would silently let tx1 commit.)
    //
    // Important nuance: the read-set validation fires here but
    // NOT in the parallel write-skew test (PR #50) where both txs
    // write to *different* docs and both commit successfully. The
    // exact rules for when validation fires deserve their own
    // empirical investigation; for now we pin what THIS test
    // observes.
    let commit_result = db.commit_transaction(tx1);
    eprintln!("[read-skew] tx1 commit result: {commit_result:?}");
    assert!(
        commit_result.is_err(),
        "PINNED: read-only tx1's commit MUST fail because OCC \
         validates the read-set and x's version bumped since tx1 \
         read it. If this passes (Ok), the engine has weakened to \
         pure read-committed (no read-set validation) — that's a \
         deliberate change, update this test + docs."
    );

    // After everything settles, the REAL state still satisfies the
    // invariant — read skew is a per-transaction-perception bug,
    // not a state-corruption one.
    let final_x = db
        .find_one("accts", &json!({"id": "x"}))
        .unwrap().unwrap()["balance"].as_i64().unwrap();
    let final_y = db
        .find_one("accts", &json!({"id": "y"}))
        .unwrap().unwrap()["balance"].as_i64().unwrap();
    assert_eq!(final_x + final_y, 100, "real state invariant must hold");
    assert_eq!(final_x, 20);
    assert_eq!(final_y, 80);
}
