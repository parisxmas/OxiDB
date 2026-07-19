//! MVCC-lite read snapshots (ADR-0017): torn multi-document reads must be
//! impossible under the default aggregate, explicit snapshots must pin
//! `find` to one commit instant, and a snapshot must never make a writer
//! wait or fail.
//!
//! The torn-sum test is the red-first proof: run against the tree one
//! commit before the feature, it fails (sums like 995/1005 appear); with
//! the feature it cannot.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use oxidb::OxiDb;
use serde_json::json;
use tempfile::tempdir;

/// Money moves between random account pairs in OCC transactions; a
/// concurrent aggregation sums every balance. Each observed sum must equal
/// the invariant total — anything else is half a transfer, the exact
/// anomaly ADR-0017 exists to kill. 400 padded accounts stretch the scan
/// long enough that, without snapshots, commits land mid-scan constantly.
#[test]
fn a_concurrent_aggregation_never_sees_half_a_transfer() {
    const N: u64 = 400;
    const TOTAL: i64 = (N as i64) * 100;
    let dir = tempdir().unwrap();
    let db = Arc::new(OxiDb::open(dir.path()).unwrap());
    let pad = "x".repeat(2000);
    let mut ids = Vec::new();
    for i in 0..N {
        ids.push(
            db.insert("accounts", json!({"i": i, "balance": 100, "pad": pad}))
                .unwrap(),
        );
    }

    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let db = Arc::clone(&db);
        let stop = Arc::clone(&stop);
        let ids = ids.clone();
        std::thread::spawn(move || {
            let mut moved = 0u64;
            let mut k = 0usize;
            while !stop.load(Ordering::Relaxed) {
                // A deterministic pseudo-random pair, far apart in id order.
                let a = ids[k % ids.len()];
                let b = ids[(k * 7 + ids.len() / 2) % ids.len()];
                k += 1;
                if a == b {
                    continue;
                }
                let tx = db.begin_transaction();
                let ok = db
                    .tx_update(
                        tx,
                        "accounts",
                        &json!({"_id": a}),
                        &json!({"$inc": {"balance": -5}}),
                    )
                    .and_then(|_| {
                        db.tx_update(
                            tx,
                            "accounts",
                            &json!({"_id": b}),
                            &json!({"$inc": {"balance": 5}}),
                        )
                    })
                    .and_then(|_| db.commit_transaction(tx));
                match ok {
                    Ok(()) => moved += 1,
                    Err(_) => {
                        let _ = db.rollback_transaction(tx);
                    }
                }
            }
            moved
        })
    };

    let pipeline = json!([{ "$group": { "_id": null, "total": { "$sum": "$balance" } } }]);
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut observed = 0u32;
    while Instant::now() < deadline {
        let out = db.aggregate("accounts", &pipeline).unwrap();
        let total = out[0]["total"].as_i64().unwrap();
        assert_eq!(
            total, TOTAL,
            "torn read: the aggregation saw half a transfer (sum {total}, expected {TOTAL})"
        );
        observed += 1;
    }
    stop.store(true, Ordering::Relaxed);
    let transfers = writer.join().unwrap();
    assert!(observed > 20, "the reader barely ran ({observed} sums)");
    assert!(
        transfers > 100,
        "the writer barely ran ({transfers} transfers)"
    );
}

/// An explicit snapshot pins reads to its instant: later updates roll back,
/// later inserts are invisible, later deletes resurrect.
#[test]
fn an_explicit_snapshot_is_one_instant() {
    let dir = tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();
    let id = db.insert("c", json!({"k": "doc", "v": 1})).unwrap();

    let s = db.begin_snapshot();

    // Update after the snapshot: the snapshot still sees v=1.
    db.update("c", &json!({"_id": id}), &json!({"$set": {"v": 2}}))
        .unwrap();
    let at_s = db.snapshot_find(s, "c", &json!({"k": "doc"})).unwrap();
    assert_eq!(at_s.len(), 1);
    assert_eq!(
        at_s[0]["v"], 1,
        "the snapshot must see the pre-update value"
    );
    assert_eq!(
        db.find("c", &json!({"k": "doc"})).unwrap()[0]["v"],
        2,
        "latest reads move on"
    );

    // Insert after the snapshot: invisible to it.
    db.insert("c", json!({"k": "late"})).unwrap();
    assert_eq!(db.snapshot_count(s, "c", &json!({"k": "late"})).unwrap(), 0);
    assert_eq!(db.count("c", &json!({"k": "late"})).unwrap(), 1);

    // Delete after the snapshot: the snapshot resurrects it.
    db.delete("c", &json!({"_id": id})).unwrap();
    let at_s = db.snapshot_find(s, "c", &json!({"k": "doc"})).unwrap();
    assert_eq!(
        at_s.len(),
        1,
        "a doc deleted after the snapshot is still visible in it"
    );
    assert_eq!(at_s[0]["v"], 1);
    assert_eq!(db.count("c", &json!({"k": "doc"})).unwrap(), 0);

    db.end_snapshot(s);
}

/// A query whose match changes across the snapshot boundary: matched-at-s
/// but updated-away docs are found; matched-now but not-at-s docs are not.
#[test]
fn snapshot_queries_match_against_the_resolved_state() {
    let dir = tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();
    let hot = db.insert("m", json!({"status": "hot"})).unwrap();
    let cold = db.insert("m", json!({"status": "cold"})).unwrap();

    let s = db.begin_snapshot();
    db.update(
        "m",
        &json!({"_id": hot}),
        &json!({"$set": {"status": "cold"}}),
    )
    .unwrap();
    db.update(
        "m",
        &json!({"_id": cold}),
        &json!({"$set": {"status": "hot"}}),
    )
    .unwrap();

    let hot_at_s = db.snapshot_find(s, "m", &json!({"status": "hot"})).unwrap();
    assert_eq!(hot_at_s.len(), 1);
    assert_eq!(
        hot_at_s[0]["_id"].as_u64().unwrap(),
        hot,
        "the snapshot's 'hot' is the doc that was hot AT the snapshot"
    );
    db.end_snapshot(s);
}

/// TTL eviction is a writer like any other: a doc it expires mid-snapshot
/// stays visible to that snapshot.
#[test]
fn ttl_eviction_does_not_reach_into_an_open_snapshot() {
    let dir = tempdir().unwrap();
    let db = Arc::new(OxiDb::open(dir.path()).unwrap());
    db.start_ttl_thread(Duration::from_millis(100));
    db.insert(
        "ephemeral",
        json!({"k": 1, "at": "2020-01-01T00:00:00Z"}), // long expired
    )
    .unwrap();

    let s = db.begin_snapshot();
    db.create_ttl_index("ephemeral", "at", 1).unwrap(); // evicts immediately

    let deadline = Instant::now() + Duration::from_secs(5);
    while db.count("ephemeral", &json!({})).unwrap() > 0 {
        assert!(Instant::now() < deadline, "TTL never evicted");
        std::thread::sleep(Duration::from_millis(50));
    }
    let at_s = db.snapshot_find(s, "ephemeral", &json!({"k": 1})).unwrap();
    assert_eq!(
        at_s.len(),
        1,
        "the snapshot must still see the TTL-evicted doc"
    );
    db.end_snapshot(s);
}

/// A snapshot never blocks or fails a writer, and repeated reads through it
/// return the same instant while the world moves on.
#[test]
fn writers_never_wait_and_snapshot_reads_repeat() {
    let dir = tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();
    let id = db.insert("r", json!({"n": 0})).unwrap();

    let s = db.begin_snapshot();
    for i in 1..=50 {
        db.update("r", &json!({"_id": id}), &json!({"$set": {"n": i}}))
            .unwrap();
        let at_s = db.snapshot_find(s, "r", &json!({"_id": id})).unwrap();
        assert_eq!(at_s[0]["n"], 0, "snapshot reads must repeat exactly");
    }
    assert_eq!(db.find("r", &json!({"_id": id})).unwrap()[0]["n"], 50);
    db.end_snapshot(s);

    // After the last snapshot ends, the gate must fully drain — nothing may
    // keep accumulating for a reader that no longer exists.
    let s2 = db.begin_snapshot();
    let fresh = db.snapshot_find(s2, "r", &json!({"_id": id})).unwrap();
    assert_eq!(
        fresh[0]["n"], 50,
        "a new snapshot starts at the new present"
    );
    db.end_snapshot(s2);
}

/// A read through an ended snapshot must error — never silently degrade to
/// latest.
#[test]
fn reading_a_dead_snapshot_is_an_error() {
    let dir = tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();
    db.insert("d", json!({"x": 1})).unwrap();
    let s = db.begin_snapshot();
    db.end_snapshot(s);
    let err = db.snapshot_find(s, "d", &json!({})).unwrap_err();
    assert!(
        err.to_string().contains("snapshot"),
        "expected a snapshot-expired error, got: {err}"
    );
}
