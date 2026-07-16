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
