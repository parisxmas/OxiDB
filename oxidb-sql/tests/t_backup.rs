//! Engine-aware backup / restore: a consistent `.tar.gz` of the SQL data
//! directory that restores to an identical database — including a schema left
//! mid-evolution by a lazy `ALTER` that the backup's checkpoint folds in.

mod common;

use common::*;
use oxidb_sql::{SqlEngine, Value};

#[test]
fn backup_then_restore_roundtrips() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let archive = tempfile::tempdir()
        .unwrap()
        .path()
        .join("sql-backup.tar.gz");

    {
        let db = SqlEngine::open(src.path()).unwrap();
        db.execute("CREATE TABLE u (id INT PRIMARY KEY, name TEXT, tag INT)")
            .unwrap();
        for i in 1..=50 {
            db.execute(&format!("INSERT INTO u VALUES ({i}, 'r{i}', {})", i % 3))
                .unwrap();
        }
        db.execute("CREATE INDEX i_tag ON u (tag)").unwrap();
        // A lazy ADD COLUMN that is NOT checkpointed — the backup's own
        // checkpoint must fold (and compact) it into the archive.
        db.execute("ALTER TABLE u ADD COLUMN score INT DEFAULT 7")
            .unwrap();

        let size = db.backup(&archive).unwrap();
        assert!(size > 0);
        assert!(archive.exists());
    }

    // Restore into a fresh directory and open it.
    SqlEngine::restore(&archive, dst.path()).unwrap();
    let db = SqlEngine::open(dst.path()).unwrap();

    // Data + the lazily-added column survived.
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM u"),
        vec![vec![Value::Int(50)]]
    );
    assert_eq!(
        rows(&db, "SELECT id, name, score FROM u WHERE id = 10"),
        vec![vec![Value::Int(10), t_text("r10"), Value::Int(7)]]
    );
    // The secondary index restored and still resolves.
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM u WHERE tag = 0"),
        vec![vec![Value::Int(16)]] // i in {3,6,...,48}
    );
    // The restored database is fully writable.
    db.execute("INSERT INTO u (id, name) VALUES (99, 'new')")
        .unwrap();
    assert_eq!(
        rows(&db, "SELECT score FROM u WHERE id = 99"),
        vec![vec![Value::Int(7)]]
    );
}

#[test]
fn backup_rejects_existing_target_restore_rejects_nonempty() {
    let src = tempfile::tempdir().unwrap();
    let db = SqlEngine::open(src.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();

    let archive = tempfile::tempdir().unwrap().path().join("b.tar.gz");
    db.backup(&archive).unwrap();
    // A second backup to the same path refuses to overwrite.
    assert!(db.backup(&archive).is_err());

    // Restore into a non-empty directory is refused.
    let busy = tempfile::tempdir().unwrap();
    std::fs::write(busy.path().join("stray"), b"x").unwrap();
    assert!(SqlEngine::restore(&archive, busy.path()).is_err());
}

fn t_text(s: &str) -> Value {
    Value::Text(s.to_string().into())
}

/// The low-lock backup runs its (slow) compression with the engine lock
/// released: writes proceed concurrently, and an auto-checkpoint may advance
/// the generation mid-backup — the pinned generation survives GC and the
/// archive restores to a consistent point that includes every row durable
/// before the backup began.
#[test]
fn low_lock_backup_under_concurrent_writes_and_checkpoints() {
    use oxidb_sql::SqlOptions;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

    let src = tempfile::tempdir().unwrap();
    // Small threshold so the concurrent writer triggers auto-checkpoints
    // *during* the backup, exercising pin-survives-GC.
    let db = Arc::new(
        SqlEngine::open_with_options(
            src.path(),
            SqlOptions {
                disk_first: false,
                checkpoint_bytes: 8 * 1024,
            ..SqlOptions::default()
        },
        )
        .unwrap(),
    );
    db.execute("CREATE TABLE u (id INT PRIMARY KEY, v INT)")
        .unwrap();
    for i in 1..=200 {
        db.execute(&format!("INSERT INTO u VALUES ({i}, {i})"))
            .unwrap();
    }
    db.checkpoint().unwrap(); // a committed generation exists
    for i in 201..=250 {
        db.execute(&format!("INSERT INTO u VALUES ({i}, {i})"))
            .unwrap();
    } // these live in the WAL only — the backup's WAL prefix must capture them

    // Hammer writes on another thread throughout the backup.
    let stop = Arc::new(AtomicBool::new(false));
    let written = Arc::new(AtomicI64::new(0));
    let (wdb, wstop, wcount) = (db.clone(), stop.clone(), written.clone());
    let writer = std::thread::spawn(move || {
        let mut id = 100_000;
        while !wstop.load(Ordering::Relaxed) {
            if wdb
                .execute(&format!("INSERT INTO u VALUES ({id}, {id})"))
                .is_ok()
            {
                wcount.fetch_add(1, Ordering::Relaxed);
            }
            id += 1;
        }
    });

    let archive = tempfile::tempdir().unwrap().path().join("live.tar.gz");
    let size = db.backup(&archive).unwrap();
    assert!(size > 0);
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();

    // The writer made progress *while the backup ran* — the backup did not
    // hold the engine lock across the whole compression.
    assert!(
        written.load(Ordering::Relaxed) > 0,
        "concurrent writer was blocked for the entire backup"
    );

    // Restore is a consistent point that includes every pre-backup row (1..=250).
    let dst = tempfile::tempdir().unwrap();
    SqlEngine::restore(&archive, dst.path()).unwrap();
    let rdb = SqlEngine::open(dst.path()).unwrap();
    for id in [1, 100, 200, 201, 250] {
        assert_eq!(
            rows(&rdb, &format!("SELECT v FROM u WHERE id = {id}")),
            vec![vec![Value::Int(id)]],
            "pre-backup row {id} missing from the restore"
        );
    }
    // The restored database is fully usable.
    rdb.execute("INSERT INTO u VALUES (999999, 1)").unwrap();
}
