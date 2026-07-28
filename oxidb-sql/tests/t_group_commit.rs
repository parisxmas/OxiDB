//! Group commit: concurrent writers share one fsync.
//!
//! The durability contract is unchanged — a write returns only after a flush
//! that covers it. What changes is that the flush happens *outside* the engine
//! lock, so N concurrent writers cost roughly one fsync instead of N.
//!
//! These tests pin both halves: that the batching actually happens (throughput
//! stops being flat in the number of writers), and that nothing acknowledged is
//! ever lost.

mod common;

use std::sync::Arc;
use std::time::Instant;

use common::*;
use oxidb_sql::SqlEngine;

/// Wall-clock for `writers` threads each doing `per_writer` inserts.
fn timed_writes(db: &Arc<SqlEngine>, table: &str, writers: usize, per_writer: usize) -> f64 {
    let start = Instant::now();
    std::thread::scope(|scope| {
        for w in 0..writers {
            let db = Arc::clone(db);
            let table = table.to_string();
            scope.spawn(move || {
                for i in 0..per_writer {
                    let id = w * 1_000_000 + i;
                    db.execute(&format!("INSERT INTO {table} VALUES ({id}, 'v')"))
                        .expect("insert");
                }
            });
        }
    });
    start.elapsed().as_secs_f64()
}

#[test]
fn concurrent_writers_share_a_flush() {
    // The claim: doubling the writers should not double the wall clock, because
    // their flushes merge. Before group commit this was flat — N writers took N
    // times as long, since each held the engine lock across its own fsync.
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open_at(dir.path()));
    db.execute("CREATE TABLE t1 (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    db.execute("CREATE TABLE t8 (id INT PRIMARY KEY, v TEXT)")
        .unwrap();

    let per_writer = 40;
    let solo = timed_writes(&db, "t1", 1, per_writer);
    let group = timed_writes(&db, "t8", 8, per_writer);

    // 8x the work; without batching it would take ~8x the time. Allow a lot of
    // slack — this runs on shared CI hardware and only has to show that the
    // flushes merge at all.
    let ratio = group / solo.max(f64::EPSILON);
    assert!(
        ratio < 6.0,
        "8 writers took {ratio:.1}x as long as 1 for the same per-writer work \
         ({group:.3}s vs {solo:.3}s) — flushes are not being shared"
    );

    // Every write is still there.
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM t8"),
        r1(vec![i((8 * per_writer) as i64)])
    );
}

#[test]
fn every_acknowledged_write_survives_a_reopen() {
    // Group commit moves the flush, it does not skip it: a write that returned
    // Ok must be on disk. Reopening reads only what the WAL and snapshots hold.
    let dir = tempfile::tempdir().unwrap();
    let writers = 6;
    let per_writer = 25;
    {
        let db = Arc::new(open_at(dir.path()));
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
            .unwrap();
        timed_writes(&db, "t", writers, per_writer);
    }
    let db = open_at(dir.path());
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM t"),
        r1(vec![i((writers * per_writer) as i64)]),
        "an acknowledged write went missing across a reopen"
    );
    // And the rows are the ones that were written, not just the right count.
    for w in 0..writers {
        let id = w * 1_000_000 + per_writer - 1;
        assert_eq!(
            rows(&db, &format!("SELECT v FROM t WHERE id = {id}")),
            vec![vec![t("v")]],
            "writer {w}'s last row is missing"
        );
    }
}

#[test]
fn a_checkpoint_covers_writes_that_have_not_flushed_yet() {
    // A checkpoint fsyncs the snapshot it folds records into and may then
    // truncate the WAL. Writers still waiting to flush are satisfied by it —
    // they must be, since the WAL they would have flushed is gone.
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Arc::new(open_at(dir.path()));
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
            .unwrap();
        timed_writes(&db, "t", 4, 20);
        db.checkpoint().unwrap();
        // More writes after the checkpoint, which land in a fresh WAL.
        db.execute("INSERT INTO t VALUES (999, 'after')").unwrap();
    }
    let db = open_at(dir.path());
    assert_eq!(rows(&db, "SELECT COUNT(*) FROM t"), r1(vec![i(81)]));
    assert_eq!(
        rows(&db, "SELECT v FROM t WHERE id = 999"),
        vec![vec![t("after")]]
    );
}

#[test]
fn transactions_commit_correctly_under_concurrency() {
    // The batched-commit path (`commit_batch_checked`) takes the same
    // append-then-flush split, and its uniqueness re-check still runs under the
    // engine lock — so concurrent transactions cannot both claim one key.
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open_at(dir.path()));
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .unwrap();

    let writers = 4;
    let per_writer = 15;
    std::thread::scope(|scope| {
        for w in 0..writers {
            let db = Arc::clone(&db);
            scope.spawn(move || {
                for i in 0..per_writer {
                    let id = w * 1_000_000 + i;
                    let mut tx = None;
                    db.execute_params_in_session("BEGIN", &[], &mut tx).unwrap();
                    db.execute_params_in_session(
                        &format!("INSERT INTO t VALUES ({id}, 'v')"),
                        &[],
                        &mut tx,
                    )
                    .unwrap();
                    db.execute_params_in_session("COMMIT", &[], &mut tx)
                        .unwrap();
                }
            });
        }
    });

    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM t"),
        r1(vec![i((writers * per_writer) as i64)])
    );
}
