//! CERN-grade crash recovery test — WAL replay after a hard "crash".
//!
//! Category 2 (crash recovery) in `docs/testing-roadmap.md`. We simulate
//! a crash by `std::mem::forget`-ing the OxiDb instance — its `Drop`
//! impl calls the graceful shutdown path (final checkpoint + WAL
//! truncation), so bypassing Drop is the cheapest way to leave the
//! engine in the "killed mid-flight, WAL only" state without actually
//! running a victim subprocess.
//!
//! Two cases:
//!   - committed inserts MUST replay on reopen (the durability promise)
//!   - uncommitted inserts (inside an unrolled transaction) MUST NOT
//!     replay (the atomicity promise — _tx_commit_log gates this)
//!
//! Marked `#[ignore]` so `cargo test` stays fast; run with:
//!   cargo test --test cern_crash_recovery -- --ignored
//!
//! What this does NOT yet test:
//!   - SIGKILL at every byte offset of a write (needs a victim subprocess)
//!   - Power-loss / fsync-EIO injection (needs OS-level fault injection)
//! Those are listed as follow-ups in docs/testing-roadmap.md.

use serde_json::json;
use tempfile::tempdir;

use oxidb::OxiDb;

/// Auto-committed inserts MUST survive a crash. Each `insert` fsyncs the
/// WAL (strict-ACID-D default), so the records are durable even if the
/// graceful shutdown path never runs.
#[test]
#[ignore]
fn committed_inserts_survive_crash() {
    const N: usize = 200;

    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // Session 1 — write N records, then "crash".
    {
        let db = OxiDb::open(&path).unwrap();
        for i in 0..N {
            db.insert("recoverable", json!({"i": i, "payload": "x".repeat(50)}))
                .unwrap();
        }
        // Bypass Drop — no graceful shutdown, no final_checkpoint, no WAL
        // truncation. Mimics SIGKILL while every prior commit's WAL fsync
        // has already returned to the application.
        std::mem::forget(db);
    }

    // Session 2 — reopen and confirm every record replayed from the WAL.
    let db2 = OxiDb::open(&path).unwrap();
    let docs = db2.find("recoverable", &json!({})).unwrap();
    assert_eq!(
        docs.len(),
        N,
        "WAL replay must restore every pre-crash insert (got {}, expected {N})",
        docs.len()
    );

    // Spot-check the payloads — not just the count.
    let mut sequences: Vec<i64> = docs.iter().map(|d| d["i"].as_i64().unwrap()).collect();
    sequences.sort();
    assert_eq!(sequences, (0..N as i64).collect::<Vec<_>>());
}

/// Inserts inside a transaction that NEVER commits MUST NOT be visible
/// after recovery. _tx_commit_log gates WAL replay: any record carrying
/// a `tx_id` not in the committed set is dropped during replay.
#[test]
#[ignore]
fn uncommitted_tx_inserts_do_not_replay() {
    const N_COMMITTED: usize = 50;
    const N_UNCOMMITTED: usize = 50;

    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // Session 1 — commit some records, leave another tx hanging, then crash.
    {
        let db = OxiDb::open(&path).unwrap();

        // First batch — committed.
        let tx1 = db.begin_transaction();
        for i in 0..N_COMMITTED {
            db.tx_insert(tx1, "tx_recov", json!({"i": i, "committed": true}))
                .unwrap();
        }
        db.commit_transaction(tx1).unwrap();

        // Second batch — inserted but NEVER committed. This is the bug bait:
        // a non-tx-aware replay would resurrect these records on next open.
        let tx2 = db.begin_transaction();
        for i in N_COMMITTED..(N_COMMITTED + N_UNCOMMITTED) {
            db.tx_insert(tx2, "tx_recov", json!({"i": i, "committed": false}))
                .unwrap();
        }
        // No commit, no rollback — just "crash".
        std::mem::forget(db);
    }

    // Session 2 — reopen, confirm only the committed records are visible.
    let db2 = OxiDb::open(&path).unwrap();
    let docs = db2.find("tx_recov", &json!({})).unwrap();
    assert_eq!(
        docs.len(),
        N_COMMITTED,
        "only committed-tx inserts should replay (got {}, expected {N_COMMITTED})",
        docs.len()
    );

    // Every visible doc must carry `committed: true`.
    for d in &docs {
        assert_eq!(
            d["committed"].as_bool(),
            Some(true),
            "uncommitted record leaked through replay: {d}"
        );
    }
}
