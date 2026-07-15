//! Disk-first row storage (`SqlOptions::disk_first`) and WAL-threshold
//! auto-checkpointing. Both modes share the on-disk format, so databases are
//! exercised across mode switches too.

mod common;

use std::path::Path;

use common::*;
use oxidb_sql::{SqlEngine, SqlOptions, Value};

fn t(s: &str) -> Value {
    Value::Text(s.to_string().into())
}

fn disk_opts() -> SqlOptions {
    SqlOptions {
        disk_first: true,
        checkpoint_bytes: 0, // manual checkpoints unless a test says otherwise
    }
}

fn open_disk() -> (tempfile::TempDir, SqlEngine) {
    let dir = tempfile::tempdir().unwrap();
    let db = SqlEngine::open_with_options(dir.path(), disk_opts()).unwrap();
    (dir, db)
}

fn wal_bytes(dir: &Path) -> u64 {
    std::fs::metadata(dir.join("wal").join("live.wal"))
        .map(|m| m.len())
        .unwrap_or(0)
}

fn seed(db: &SqlEngine) {
    db.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, age INT)")
        .unwrap();
    db.execute("INSERT INTO users VALUES (1, 'ada', 36), (2, 'bob', 25), (3, 'eve', 30)")
        .unwrap();
}

// ── correctness in disk-first mode ──────────────────────────────────────────

#[test]
fn disk_first_crud_and_queries() {
    let (_d, db) = open_disk();
    seed(&db);

    assert_eq!(
        rows(&db, "SELECT name FROM users WHERE age > 26 ORDER BY name"),
        vec![vec![t("ada")], vec![t("eve")]]
    );
    assert_eq!(affected(&db, "UPDATE users SET age = 37 WHERE id = 1"), 1);
    assert_eq!(affected(&db, "DELETE FROM users WHERE id = 2"), 1);
    assert_eq!(
        rows(&db, "SELECT id, age FROM users ORDER BY id"),
        vec![
            vec![Value::Int(1), Value::Int(37)],
            vec![Value::Int(3), Value::Int(30)],
        ]
    );
    // Aggregates walk the merge iterator.
    assert_eq!(
        rows(&db, "SELECT COUNT(*), SUM(age) FROM users"),
        vec![vec![Value::Int(2), Value::Int(67)]]
    );
}

#[test]
fn disk_first_base_rows_come_from_mmap_after_checkpoint() {
    let (d, db) = open_disk();
    seed(&db);
    db.checkpoint().unwrap(); // rows now live in the mmap'd .rdat base
    assert!(d.path().join("users.rdat").exists());
    assert_eq!(wal_bytes(d.path()), 8); // header only

    // Reads hit the base.
    assert_eq!(
        rows(&db, "SELECT name FROM users WHERE id = 2"),
        vec![vec![t("bob")]]
    );

    // Mutate base rows: update shadows, delete tombstones.
    db.execute("UPDATE users SET name = 'ADA' WHERE id = 1")
        .unwrap();
    db.execute("DELETE FROM users WHERE id = 3").unwrap();
    assert_eq!(
        rows(&db, "SELECT name FROM users ORDER BY id"),
        vec![vec![t("ADA")], vec![t("bob")]]
    );
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM users"),
        vec![vec![Value::Int(2)]]
    );

    // PK uniqueness must see base rows too.
    assert!(
        db.execute("INSERT INTO users VALUES (2, 'dup', 1)")
            .is_err()
    );
    // ...and respect the tombstone: id 3 is free again.
    db.execute("INSERT INTO users VALUES (3, 'new', 1)")
        .unwrap();
}

#[test]
fn disk_first_index_lookup_fetches_from_base() {
    let (_d, db) = open_disk();
    seed(&db);
    db.execute("CREATE INDEX idx_age ON users (age)").unwrap();
    db.checkpoint().unwrap();
    // Equality seek through the index; the row itself is decoded from mmap.
    assert_eq!(
        rows(&db, "SELECT name FROM users WHERE age = 25"),
        vec![vec![t("bob")]]
    );
    // Overlay rows are visible through the same index.
    db.execute("INSERT INTO users VALUES (4, 'kim', 25)")
        .unwrap();
    assert_eq!(
        rows(&db, "SELECT name FROM users WHERE age = 25 ORDER BY name"),
        vec![vec![t("bob")], vec![t("kim")]]
    );
}

#[test]
fn disk_first_recovery_base_plus_wal() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = SqlEngine::open_with_options(dir.path(), disk_opts()).unwrap();
        seed(&db);
        db.checkpoint().unwrap();
        // Post-checkpoint changes stay in the WAL only.
        db.execute("UPDATE users SET age = 99 WHERE id = 1")
            .unwrap();
        db.execute("DELETE FROM users WHERE id = 2").unwrap();
        db.execute("INSERT INTO users VALUES (7, 'zoe', 20)")
            .unwrap();
    } // drop without checkpoint — reopen must replay the WAL over the base

    let db = SqlEngine::open_with_options(dir.path(), disk_opts()).unwrap();
    assert_eq!(
        rows(&db, "SELECT id, name, age FROM users ORDER BY id"),
        vec![
            vec![Value::Int(1), t("ada"), Value::Int(99)],
            vec![Value::Int(3), t("eve"), Value::Int(30)],
            vec![Value::Int(7), t("zoe"), Value::Int(20)],
        ]
    );
    // next_row_id / PK map seeded correctly: fresh inserts work.
    db.execute("INSERT INTO users VALUES (8, 'max', 40)")
        .unwrap();
    assert!(
        db.execute("INSERT INTO users VALUES (7, 'dup', 1)")
            .is_err()
    );
}

#[test]
fn disk_first_transactions() {
    let (_d, db) = open_disk();
    seed(&db);
    db.checkpoint().unwrap();
    let res = db
        .execute(
            "BEGIN; \
             UPDATE users SET age = 50 WHERE id = 1; \
             INSERT INTO users VALUES (9, 'tx', 1); \
             COMMIT;",
        )
        .unwrap();
    assert_eq!(res.len(), 4);
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM users WHERE age >= 50 OR id = 9"),
        vec![vec![Value::Int(2)]]
    );
    // Rollback leaves base + overlay untouched.
    db.execute("BEGIN; DELETE FROM users WHERE id = 1; ROLLBACK;")
        .unwrap();
    assert_eq!(
        rows(&db, "SELECT name FROM users WHERE id = 1"),
        vec![vec![t("ada")]]
    );
}

#[test]
fn mode_switch_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    // Write resident, with a checkpoint and a WAL tail.
    {
        let db = SqlEngine::open_with_options(
            dir.path(),
            SqlOptions {
                disk_first: false,
                checkpoint_bytes: 0,
            },
        )
        .unwrap();
        seed(&db);
        db.checkpoint().unwrap();
        db.execute("INSERT INTO users VALUES (4, 'kim', 41)")
            .unwrap();
    }
    // Reopen disk-first: same data.
    {
        let db = SqlEngine::open_with_options(dir.path(), disk_opts()).unwrap();
        assert_eq!(
            rows(&db, "SELECT COUNT(*) FROM users"),
            vec![vec![Value::Int(4)]]
        );
        db.execute("DELETE FROM users WHERE id = 1").unwrap();
        db.checkpoint().unwrap();
    }
    // And back to resident.
    let db = SqlEngine::open_with_options(
        dir.path(),
        SqlOptions {
            disk_first: false,
            checkpoint_bytes: 0,
        },
    )
    .unwrap();
    assert_eq!(
        rows(&db, "SELECT name FROM users ORDER BY id"),
        vec![vec![t("bob")], vec![t("eve")], vec![t("kim")]]
    );
}

// ── auto-checkpoint ─────────────────────────────────────────────────────────

#[test]
fn auto_checkpoint_truncates_wal_at_threshold() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqlEngine::open_with_options(
        dir.path(),
        SqlOptions {
            disk_first: false,
            checkpoint_bytes: 4096,
        },
    )
    .unwrap();
    db.execute("CREATE TABLE kv (k INT PRIMARY KEY, v TEXT)")
        .unwrap();

    let mut auto_fired = false;
    for i in 0..200 {
        db.execute(&format!("INSERT INTO kv VALUES ({i}, 'value-{i}')"))
            .unwrap();
        if dir.path().join("kv.rdat").exists() && wal_bytes(dir.path()) <= 4096 {
            auto_fired = true;
        }
    }
    assert!(
        auto_fired,
        "auto-checkpoint never fired below the threshold"
    );
    // The WAL never runs away past threshold + one batch.
    assert!(wal_bytes(dir.path()) < 8192, "WAL grew unbounded");

    // Nothing lost across the checkpoints.
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM kv"),
        vec![vec![Value::Int(200)]]
    );
    drop(db);
    let db = SqlEngine::open_with_options(
        dir.path(),
        SqlOptions {
            disk_first: false,
            checkpoint_bytes: 4096,
        },
    )
    .unwrap();
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM kv"),
        vec![vec![Value::Int(200)]]
    );
}

#[test]
fn auto_checkpoint_bounds_disk_first_overlay() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqlEngine::open_with_options(
        dir.path(),
        SqlOptions {
            disk_first: true,
            checkpoint_bytes: 4096,
        },
    )
    .unwrap();
    db.execute("CREATE TABLE kv (k INT PRIMARY KEY, v TEXT)")
        .unwrap();
    for i in 0..300 {
        db.execute(&format!("INSERT INTO kv VALUES ({i}, 'value-{i}')"))
            .unwrap();
    }
    // Rows written before the last auto-checkpoint are served from mmap;
    // everything is still queryable and correctly counted.
    assert!(dir.path().join("kv.rdat").exists());
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM kv"),
        vec![vec![Value::Int(300)]]
    );
    assert_eq!(
        rows(&db, "SELECT v FROM kv WHERE k = 0"),
        vec![vec![t("value-0")]]
    );
    assert_eq!(
        rows(&db, "SELECT v FROM kv WHERE k = 299"),
        vec![vec![t("value-299")]]
    );
}

#[test]
fn zero_threshold_disables_auto_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqlEngine::open_with_options(
        dir.path(),
        SqlOptions {
            disk_first: false,
            checkpoint_bytes: 0,
        },
    )
    .unwrap();
    db.execute("CREATE TABLE kv (k INT PRIMARY KEY)").unwrap();
    for i in 0..50 {
        db.execute(&format!("INSERT INTO kv VALUES ({i})")).unwrap();
    }
    assert!(
        !dir.path().join("kv.rdat").exists(),
        "checkpoint ran unbidden"
    );
    assert!(wal_bytes(dir.path()) > 8);
}
