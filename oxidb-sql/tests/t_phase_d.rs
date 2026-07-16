//! ADR-0013 Phase D: ALTER TABLE, DEFAULT values, DECIMAL/BLOB types,
//! UNIQUE enforcement, FK tolerance, INSERT ... RETURNING.

mod common;

use common::*;
use oxidb_sql::{QueryResult, SqlEngine, SqlOptions, Value};

fn t(s: &str) -> Value {
    Value::Text(s.to_string().into())
}

#[test]
fn defaults_fill_omitted_columns() {
    let (_d, db) = open();
    db.execute(
        "CREATE TABLE u (id INT PRIMARY KEY AUTO_INCREMENT, ad TEXT DEFAULT 'anon', puan INT DEFAULT 0, n INT)",
    )
    .unwrap();
    db.execute("INSERT INTO u (n) VALUES (1)").unwrap();
    db.execute("INSERT INTO u (ad, n) VALUES ('ali', 2)")
        .unwrap();
    // Explicit NULL beats the default (SQL semantics).
    db.execute("INSERT INTO u (ad, puan, n) VALUES (NULL, NULL, 3)")
        .unwrap();
    assert_eq!(
        rows(&db, "SELECT ad, puan FROM u ORDER BY n"),
        vec![
            vec![t("anon"), Value::Int(0)],
            vec![t("ali"), Value::Int(0)],
            vec![Value::Null, Value::Null],
        ]
    );
}

#[test]
fn unique_enforced_autocommit_and_tx() {
    let (_d, db) = open();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY, email TEXT UNIQUE, x INT)")
        .unwrap();
    db.execute("INSERT INTO u VALUES (1, 'a@x', 0), (2, NULL, 0), (3, NULL, 0)")
        .unwrap(); // NULLs never collide
    assert!(db.execute("INSERT INTO u VALUES (4, 'a@x', 0)").is_err());
    // UPDATE into a collision is rejected too.
    db.execute("INSERT INTO u VALUES (5, 'b@x', 0)").unwrap();
    assert!(
        db.execute("UPDATE u SET email = 'a@x' WHERE id = 5")
            .is_err()
    );
    // Inside a transaction.
    assert!(
        db.execute("BEGIN; INSERT INTO u VALUES (6, 'a@x', 0); COMMIT")
            .is_err()
    );
    // Delete frees the value.
    db.execute("DELETE FROM u WHERE id = 1").unwrap();
    db.execute("INSERT INTO u VALUES (7, 'a@x', 0)").unwrap();
    // Survives reopen? (seeded from snapshot/WAL)
}

#[test]
fn unique_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = SqlEngine::open(dir.path()).unwrap();
        db.execute("CREATE TABLE u (id INT PRIMARY KEY, email TEXT UNIQUE)")
            .unwrap();
        db.execute("INSERT INTO u VALUES (1, 'a@x')").unwrap();
        db.checkpoint().unwrap();
    }
    let db = SqlEngine::open(dir.path()).unwrap();
    assert!(db.execute("INSERT INTO u VALUES (2, 'a@x')").is_err());
}

#[test]
fn decimal_and_blob_types() {
    let (_d, db) = open();
    db.execute("CREATE TABLE p (id INT PRIMARY KEY, para DECIMAL(10,2), veri BLOB)")
        .unwrap();
    // DECIMAL is exact fixed-point; BLOB accepts base64 text on the JSON-ish path.
    db.execute("INSERT INTO p VALUES (1, 12.50, 'aGVsbG8=')")
        .unwrap(); // "hello"
    let r = db
        .execute("SELECT para, veri FROM p")
        .unwrap()
        .pop()
        .unwrap();
    let QueryResult::Select { types, rows, .. } = r else {
        panic!()
    };
    assert_eq!(
        types,
        vec![
            Some(oxidb_sql::SqlType::Decimal),
            Some(oxidb_sql::SqlType::Blob)
        ]
    );
    // Exact, and the 2-digit scale of the literal is preserved on reload.
    assert_eq!(
        rows[0][0],
        Value::Decimal(Box::new(oxidb_sql::Decimal::parse("12.50").unwrap()))
    );
    assert_eq!(rows[0][1], Value::Bytes(b"hello".to_vec().into()));
}

#[test]
fn foreign_key_syntax_tolerated() {
    let (_d, db) = open();
    db.execute("CREATE TABLE a (id INT PRIMARY KEY)").unwrap();
    // Column-level and table-level FK syntax both parse (not enforced).
    db.execute("CREATE TABLE b (id INT PRIMARY KEY, a_id INT REFERENCES a(id))")
        .unwrap();
    db.execute(
        "CREATE TABLE c (id INT PRIMARY KEY, a_id INT, FOREIGN KEY (a_id) REFERENCES a(id))",
    )
    .unwrap();
    db.execute("INSERT INTO b VALUES (1, 999)").unwrap(); // no enforcement
}

#[test]
fn insert_returning() {
    let (_d, db) = open();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY AUTO_INCREMENT, ad TEXT)")
        .unwrap();
    let r = db
        .execute("INSERT INTO u (ad) VALUES ('ali'), ('ayse') RETURNING id, ad")
        .unwrap()
        .pop()
        .unwrap();
    let QueryResult::Select { columns, rows, .. } = r else {
        panic!("RETURNING must produce a result set")
    };
    assert_eq!(columns, vec!["id", "ad"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::Int(1), t("ali")],
            vec![Value::Int(2), t("ayse")]
        ]
    );
}

#[test]
fn alter_table_add_drop_rename() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqlEngine::open(dir.path()).unwrap();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY, ad TEXT)")
        .unwrap();
    db.execute("INSERT INTO u VALUES (1, 'ali')").unwrap();

    // ADD with default backfills existing rows.
    db.execute("ALTER TABLE u ADD COLUMN puan INT DEFAULT 5")
        .unwrap();
    assert_eq!(
        rows(&db, "SELECT id, ad, puan FROM u"),
        vec![vec![Value::Int(1), t("ali"), Value::Int(5)]]
    );
    db.execute("INSERT INTO u (id, ad) VALUES (2, 'ayse')")
        .unwrap();
    assert_eq!(
        rows(&db, "SELECT puan FROM u ORDER BY id"),
        vec![vec![Value::Int(5)], vec![Value::Int(5)]]
    );

    // RENAME.
    db.execute("ALTER TABLE u RENAME COLUMN puan TO skor")
        .unwrap();
    assert_eq!(
        rows(&db, "SELECT skor FROM u WHERE id = 1"),
        vec![vec![Value::Int(5)]]
    );
    assert!(db.execute("SELECT puan FROM u").is_err());

    // DROP (an index over it blocks; after dropping the index it works).
    db.execute("CREATE INDEX i_skor ON u (skor)").unwrap();
    assert!(db.execute("ALTER TABLE u DROP COLUMN skor").is_err());
    db.execute("DROP INDEX i_skor").unwrap();
    db.execute("ALTER TABLE u DROP COLUMN skor").unwrap();
    assert_eq!(
        rows(&db, "SELECT id, ad FROM u ORDER BY id"),
        vec![
            vec![Value::Int(1), t("ali")],
            vec![Value::Int(2), t("ayse")]
        ]
    );
    // PK is undroppable; NOT NULL without default on non-empty table errors.
    assert!(db.execute("ALTER TABLE u DROP COLUMN id").is_err());
    assert!(
        db.execute("ALTER TABLE u ADD COLUMN z INT NOT NULL")
            .is_err()
    );

    // Survives reopen (WAL/pending checkpoint).
    drop(db);
    let db = SqlEngine::open(dir.path()).unwrap();
    assert_eq!(
        rows(&db, "SELECT id, ad FROM u ORDER BY id"),
        vec![
            vec![Value::Int(1), t("ali")],
            vec![Value::Int(2), t("ayse")]
        ]
    );
}

#[test]
fn alter_table_disk_first() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqlEngine::open_with_options(
        dir.path(),
        SqlOptions {
            disk_first: true,
            checkpoint_bytes: 0,
        },
    )
    .unwrap();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY, ad TEXT)")
        .unwrap();
    db.execute("INSERT INTO u VALUES (1, 'ali'), (2, 'ayse')")
        .unwrap();
    db.checkpoint().unwrap(); // rows now live in the mmap'd base

    db.execute("ALTER TABLE u ADD COLUMN puan INT DEFAULT 7")
        .unwrap();
    assert_eq!(
        rows(&db, "SELECT ad, puan FROM u ORDER BY id"),
        vec![
            vec![t("ali"), Value::Int(7)],
            vec![t("ayse"), Value::Int(7)]
        ]
    );
    drop(db);
    let db = SqlEngine::open_with_options(
        dir.path(),
        SqlOptions {
            disk_first: true,
            checkpoint_bytes: 0,
        },
    )
    .unwrap();
    assert_eq!(
        rows(&db, "SELECT puan FROM u ORDER BY id"),
        vec![vec![Value::Int(7)], vec![Value::Int(7)]]
    );
}

/// ADD COLUMN is metadata-only: it does not rewrite the stored rows, yet every
/// existing row reads back the new column's default (padded on read), and the
/// column is fully usable in WHERE / aggregates / ORDER BY immediately.
#[test]
fn add_column_metadata_only_reads_back_default() {
    let (_d, db) = open();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY, ad TEXT)")
        .unwrap();
    for i in 1..=50 {
        db.execute(&format!("INSERT INTO u VALUES ({i}, 'r{i}')"))
            .unwrap();
    }

    // Instant on a populated table — no per-row rewrite.
    db.execute("ALTER TABLE u ADD COLUMN puan INT DEFAULT 5")
        .unwrap();
    // A nullable column with no default reads back NULL.
    db.execute("ALTER TABLE u ADD COLUMN note TEXT").unwrap();

    // Old rows padded with the defaults.
    assert_eq!(
        rows(&db, "SELECT id, ad, puan, note FROM u WHERE id = 1"),
        vec![vec![Value::Int(1), t("r1"), Value::Int(5), Value::Null]]
    );
    // The new column filters and aggregates.
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM u WHERE puan = 5"),
        vec![vec![Value::Int(50)]]
    );
    assert_eq!(
        rows(&db, "SELECT SUM(puan) FROM u"),
        vec![vec![Value::Int(250)]]
    );

    // Writes to the new column heal individual rows; others stay padded.
    db.execute("UPDATE u SET puan = 100, note = 'hot' WHERE id = 1")
        .unwrap();
    db.execute("INSERT INTO u (id, ad, puan) VALUES (51, 'new', 9)")
        .unwrap();
    assert_eq!(
        rows(&db, "SELECT puan, note FROM u WHERE id = 1"),
        vec![vec![Value::Int(100), t("hot")]]
    );
    assert_eq!(
        rows(&db, "SELECT puan, note FROM u WHERE id = 2"),
        vec![vec![Value::Int(5), Value::Null]]
    );
    assert_eq!(
        rows(&db, "SELECT SUM(puan) FROM u"),
        vec![vec![Value::Int(250 - 5 + 100 + 9)]]
    );
}

/// An index created over a lazily-added column is built correctly (from the
/// padded rows) and its point lookups return the default-bearing rows.
#[test]
fn index_over_lazily_added_column() {
    let (_d, db) = open();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY, ad TEXT)")
        .unwrap();
    for i in 1..=10 {
        db.execute(&format!("INSERT INTO u VALUES ({i}, 'r{i}')"))
            .unwrap();
    }
    db.execute("ALTER TABLE u ADD COLUMN tag INT DEFAULT 42")
        .unwrap();
    db.execute("CREATE INDEX i_tag ON u (tag)").unwrap();

    // Every existing row indexed under the default.
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM u WHERE tag = 42"),
        vec![vec![Value::Int(10)]]
    );
    db.execute("UPDATE u SET tag = 7 WHERE id = 3").unwrap();
    assert_eq!(
        rows(&db, "SELECT id FROM u WHERE tag = 7"),
        vec![vec![Value::Int(3)]]
    );
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM u WHERE tag = 42"),
        vec![vec![Value::Int(9)]]
    );
}

/// Two metadata-only ADDs (leaving rows physically narrow) followed by a DROP:
/// the DROP's row rewrite must widen each row first, so removing a column by
/// position is correct even for never-materialized rows.
#[test]
fn multiple_lazy_adds_then_drop() {
    let (_d, db) = open();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY, a TEXT)")
        .unwrap();
    db.execute("INSERT INTO u VALUES (1, 'x'), (2, 'y')")
        .unwrap();
    db.execute("ALTER TABLE u ADD COLUMN b INT DEFAULT 10")
        .unwrap();
    db.execute("ALTER TABLE u ADD COLUMN c INT DEFAULT 20")
        .unwrap();
    // Rows are still width 2 on disk; padded to width 4 on read.
    assert_eq!(
        rows(&db, "SELECT id, a, b, c FROM u ORDER BY id"),
        vec![
            vec![Value::Int(1), t("x"), Value::Int(10), Value::Int(20)],
            vec![Value::Int(2), t("y"), Value::Int(10), Value::Int(20)],
        ]
    );
    // DROP a middle column — rewrite must not panic on the narrow rows.
    db.execute("ALTER TABLE u DROP COLUMN a").unwrap();
    assert_eq!(
        rows(&db, "SELECT id, b, c FROM u ORDER BY id"),
        vec![
            vec![Value::Int(1), Value::Int(10), Value::Int(20)],
            vec![Value::Int(2), Value::Int(10), Value::Int(20)],
        ]
    );
    assert!(db.execute("SELECT a FROM u").is_err());
}

/// A metadata-only ADD deliberately skips the checkpoint, so its durability
/// rides entirely on the WAL record. Reopening (WAL replay over the old-arity
/// snapshot) must reconstruct the padded rows.
#[test]
fn add_column_survives_reopen_without_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqlEngine::open(dir.path()).unwrap();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY, ad TEXT)")
        .unwrap();
    db.execute("INSERT INTO u VALUES (1, 'ali'), (2, 'veli')")
        .unwrap();
    db.checkpoint().unwrap(); // snapshot at the OLD arity
    db.execute("ALTER TABLE u ADD COLUMN puan INT DEFAULT 5")
        .unwrap();
    db.execute("INSERT INTO u (id, ad, puan) VALUES (3, 'can', 9)")
        .unwrap();
    drop(db); // no checkpoint after the ALTER

    let db = SqlEngine::open(dir.path()).unwrap();
    assert_eq!(
        rows(&db, "SELECT id, ad, puan FROM u ORDER BY id"),
        vec![
            vec![Value::Int(1), t("ali"), Value::Int(5)],
            vec![Value::Int(2), t("veli"), Value::Int(5)],
            vec![Value::Int(3), t("can"), Value::Int(9)],
        ]
    );
    // A checkpoint now folds the lazy add into the snapshot; still correct.
    db.checkpoint().unwrap();
    drop(db);
    let db = SqlEngine::open(dir.path()).unwrap();
    assert_eq!(
        rows(&db, "SELECT SUM(puan) FROM u"),
        vec![vec![Value::Int(19)]]
    );
}

/// DROP COLUMN is metadata-only: the stored rows keep the column's cell (no
/// rewrite), but it's projected out of every query — invisible to SELECT *,
/// DESCRIBE, and by name — while the survivors read back correctly.
#[test]
fn drop_column_metadata_only_projects_out() {
    let (_d, db) = open();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY, a TEXT, b INT, c TEXT)")
        .unwrap();
    for i in 1..=20 {
        db.execute(&format!(
            "INSERT INTO u VALUES ({i}, 'a{i}', {}, 'c{i}')",
            i * 10
        ))
        .unwrap();
    }
    // Drop a middle column — instant, no row rewrite.
    db.execute("ALTER TABLE u DROP COLUMN b").unwrap();

    // Gone from the schema: unreferenceable, and SELECT * / DESCRIBE skip it.
    assert!(db.execute("SELECT b FROM u").is_err());
    let (cols, r) = cols_rows(&db, "SELECT * FROM u WHERE id = 3");
    assert_eq!(cols, vec!["id", "a", "c"]);
    assert_eq!(r, vec![vec![Value::Int(3), t("a3"), t("c3")]]);
    let names: Vec<Value> = rows(&db, "DESCRIBE u")
        .iter()
        .map(|r| r[0].clone())
        .collect();
    assert_eq!(names, vec![t("id"), t("a"), t("c")]);

    // The survivors still read/aggregate correctly.
    assert_eq!(
        rows(&db, "SELECT id, a, c FROM u WHERE id = 7"),
        vec![vec![Value::Int(7), t("a7"), t("c7")]]
    );
    // New INSERT binds to the live (3-column) arity.
    db.execute("INSERT INTO u VALUES (99, 'z', 'zz')").unwrap();
    assert_eq!(
        rows(&db, "SELECT a, c FROM u WHERE id = 99"),
        vec![vec![t("z"), t("zz")]]
    );
    // UPDATE a survivor by its new logical position.
    db.execute("UPDATE u SET c = 'C7' WHERE id = 7").unwrap();
    assert_eq!(
        rows(&db, "SELECT c FROM u WHERE id = 7"),
        vec![vec![t("C7")]]
    );
}

/// A lazy ADD leaves rows physically narrow; a following DROP must project a
/// row that is missing BOTH a tombstoned middle slot and a trailing slot.
#[test]
fn add_then_drop_projects_gap_and_trailing() {
    let (_d, db) = open();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY, a TEXT)")
        .unwrap();
    db.execute("INSERT INTO u VALUES (1, 'x'), (2, 'y')")
        .unwrap();
    // Rows are physically [id, a] (width 2); b lives only in the schema.
    db.execute("ALTER TABLE u ADD COLUMN b INT DEFAULT 5")
        .unwrap();
    // Drop the middle column a (slot 1); live slots become [0, 2].
    db.execute("ALTER TABLE u DROP COLUMN a").unwrap();
    assert!(db.execute("SELECT a FROM u").is_err());
    // Old narrow row [id, a]: slot 0 = id, slot 2 = absent -> b's default.
    assert_eq!(
        rows(&db, "SELECT id, b FROM u ORDER BY id"),
        vec![
            vec![Value::Int(1), Value::Int(5)],
            vec![Value::Int(2), Value::Int(5)],
        ]
    );
    // A fresh insert stores the full physical layout with a placeholder.
    db.execute("INSERT INTO u VALUES (3, 9)").unwrap();
    assert_eq!(
        rows(&db, "SELECT id, b FROM u WHERE id = 3"),
        vec![vec![Value::Int(3), Value::Int(9)]]
    );
}

/// A dropped column's name is free to reuse — the re-added column is a fresh
/// slot with its own default, independent of the tombstoned one.
#[test]
fn drop_then_readd_same_name() {
    let (_d, db) = open();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY, a INT)")
        .unwrap();
    db.execute("INSERT INTO u VALUES (1, 100)").unwrap();
    db.execute("ALTER TABLE u DROP COLUMN a").unwrap();
    db.execute("ALTER TABLE u ADD COLUMN a INT DEFAULT 7")
        .unwrap();
    // The old value is gone; the re-added column reads its default.
    assert_eq!(
        rows(&db, "SELECT id, a FROM u"),
        vec![vec![Value::Int(1), Value::Int(7)]]
    );
    db.execute("INSERT INTO u VALUES (2, 9)").unwrap();
    assert_eq!(
        rows(&db, "SELECT a FROM u ORDER BY id"),
        vec![vec![Value::Int(7)], vec![Value::Int(9)]]
    );
}

/// An index over a surviving column keeps working after a DROP (physical
/// positions are stable), including index upkeep on DELETE.
#[test]
fn drop_column_keeps_other_index_and_delete() {
    let (_d, db) = open();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY, a INT, tag INT)")
        .unwrap();
    for i in 1..=10 {
        db.execute(&format!("INSERT INTO u VALUES ({i}, {i}, {})", i % 3))
            .unwrap();
    }
    db.execute("CREATE INDEX i_tag ON u (tag)").unwrap();
    db.execute("ALTER TABLE u DROP COLUMN a").unwrap();
    // tag = 0 for i in {3, 6, 9}.
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM u WHERE tag = 0"),
        vec![vec![Value::Int(3)]]
    );
    db.execute("DELETE FROM u WHERE id = 3").unwrap();
    assert_eq!(
        rows(&db, "SELECT id FROM u WHERE tag = 0 ORDER BY id"),
        vec![vec![Value::Int(6)], vec![Value::Int(9)]]
    );
}

/// A UNIQUE column sitting *after* a dropped one keeps its constraint: the
/// engine still checks it at the shifted physical slot.
#[test]
fn unique_after_dropped_column_still_enforced() {
    let (_d, db) = open();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY, junk TEXT, email TEXT UNIQUE)")
        .unwrap();
    db.execute("INSERT INTO u VALUES (1, 'j', 'a@x')").unwrap();
    // email is physical slot 2, but logical position 1 after the drop.
    db.execute("ALTER TABLE u DROP COLUMN junk").unwrap();
    assert!(db.execute("INSERT INTO u VALUES (2, 'a@x')").is_err());
    db.execute("INSERT INTO u VALUES (2, 'b@x')").unwrap();
    assert_eq!(
        rows(&db, "SELECT id FROM u WHERE email = 'b@x'"),
        vec![vec![Value::Int(2)]]
    );
}

/// The lazy DROP skips the checkpoint, so its durability rides on the WAL
/// record; reopening (replay over the pre-drop, full-arity snapshot) must
/// tombstone the column and project it out.
#[test]
fn drop_column_survives_reopen_without_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqlEngine::open(dir.path()).unwrap();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY, a TEXT, b INT)")
        .unwrap();
    db.execute("INSERT INTO u VALUES (1, 'x', 10), (2, 'y', 20)")
        .unwrap();
    db.checkpoint().unwrap(); // snapshot at the full (pre-drop) arity
    db.execute("ALTER TABLE u DROP COLUMN a").unwrap();
    db.execute("INSERT INTO u VALUES (3, 30)").unwrap(); // live arity = (id, b)
    drop(db); // no checkpoint after the drop

    let db = SqlEngine::open(dir.path()).unwrap();
    assert!(db.execute("SELECT a FROM u").is_err());
    assert_eq!(
        rows(&db, "SELECT id, b FROM u ORDER BY id"),
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Int(20)],
            vec![Value::Int(3), Value::Int(30)],
        ]
    );
    // A checkpoint now persists the tombstoned layout; still correct on reopen.
    db.checkpoint().unwrap();
    drop(db);
    let db = SqlEngine::open(dir.path()).unwrap();
    assert_eq!(
        rows(&db, "SELECT SUM(b) FROM u"),
        vec![vec![Value::Int(60)]]
    );
    assert!(db.execute("SELECT a FROM u").is_err());
}

/// A checkpoint after a lazy DROP compacts the table — physically rewriting
/// rows to the live columns — so the dropped column's space is reclaimed on
/// disk, while query results are unchanged and the tombstone is gone.
#[test]
fn checkpoint_compacts_dropped_column_space() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqlEngine::open(dir.path()).unwrap();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY, big TEXT, keep INT)")
        .unwrap();
    let filler = "x".repeat(200);
    for i in 1..=200 {
        db.execute(&format!("INSERT INTO u VALUES ({i}, '{filler}', {i})"))
            .unwrap();
    }
    db.checkpoint().unwrap();
    let rdat = dir.path().join("u.rdat");
    let size_before = std::fs::metadata(&rdat).unwrap().len();

    // Instant drop, then a checkpoint that compacts away the big column.
    db.execute("ALTER TABLE u DROP COLUMN big").unwrap();
    db.checkpoint().unwrap();
    let size_after = std::fs::metadata(&rdat).unwrap().len();
    assert!(
        size_after * 2 < size_before,
        "expected the snapshot to shrink markedly after compaction: {size_before} -> {size_after}"
    );

    // Results are unchanged, and the column stays gone across a reopen.
    let total: i64 = (1..=200).sum();
    assert_eq!(
        rows(&db, "SELECT SUM(keep) FROM u"),
        vec![vec![Value::Int(total)]]
    );
    drop(db);
    let db = SqlEngine::open(dir.path()).unwrap();
    assert!(db.execute("SELECT big FROM u").is_err());
    assert_eq!(
        rows(&db, "SELECT id, keep FROM u WHERE id = 100"),
        vec![vec![Value::Int(100), Value::Int(100)]]
    );
    assert_eq!(
        rows(&db, "SELECT SUM(keep) FROM u"),
        vec![vec![Value::Int(total)]]
    );
}

/// Compaction preserves the PRIMARY KEY and secondary indexes: their positions
/// shift when the dropped column is physically removed, and they're rebuilt.
#[test]
fn checkpoint_compaction_preserves_indexes() {
    let (_d, db) = open();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY, junk TEXT, tag INT)")
        .unwrap();
    for i in 1..=10 {
        db.execute(&format!("INSERT INTO u VALUES ({i}, 'j{i}', {})", i % 3))
            .unwrap();
    }
    db.execute("CREATE INDEX i_tag ON u (tag)").unwrap();
    db.execute("ALTER TABLE u DROP COLUMN junk").unwrap();
    db.checkpoint().unwrap(); // compacts: tag moves from slot 2 to slot 1

    // Secondary index still resolves at the shifted position.
    assert_eq!(
        rows(&db, "SELECT id FROM u WHERE tag = 0 ORDER BY id"),
        vec![
            vec![Value::Int(3)],
            vec![Value::Int(6)],
            vec![Value::Int(9)]
        ]
    );
    // PRIMARY KEY uniqueness still enforced.
    assert!(db.execute("INSERT INTO u VALUES (3, 0)").is_err());
    // Writes and index upkeep keep working post-compaction.
    db.execute("INSERT INTO u VALUES (11, 0)").unwrap();
    db.execute("DELETE FROM u WHERE id = 3").unwrap();
    assert_eq!(
        rows(&db, "SELECT id FROM u WHERE tag = 0 ORDER BY id"),
        vec![
            vec![Value::Int(6)],
            vec![Value::Int(9)],
            vec![Value::Int(11)]
        ]
    );
}

/// DROP COLUMN in disk-first mode: base rows live in the mmap at full physical
/// arity; the drop projects the tombstoned slot out on read, across a reopen.
#[test]
fn drop_column_disk_first_reopen() {
    let opts = SqlOptions {
        disk_first: true,
        checkpoint_bytes: 0,
    };
    let dir = tempfile::tempdir().unwrap();
    let db = SqlEngine::open_with_options(dir.path(), opts.clone()).unwrap();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY, a TEXT, b INT)")
        .unwrap();
    db.execute("INSERT INTO u VALUES (1, 'x', 10), (2, 'y', 20)")
        .unwrap();
    db.checkpoint().unwrap(); // rows now in the mmap'd base at full arity
    db.execute("ALTER TABLE u DROP COLUMN a").unwrap();
    assert_eq!(
        rows(&db, "SELECT id, b FROM u ORDER BY id"),
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Int(20)],
        ]
    );
    drop(db);
    let db = SqlEngine::open_with_options(dir.path(), opts).unwrap();
    assert!(db.execute("SELECT a FROM u").is_err());
    assert_eq!(
        rows(&db, "SELECT id, b FROM u ORDER BY id"),
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Int(20)],
        ]
    );
}

/// Compaction in disk-first mode: the tombstoned base rows (mmap'd, immutable)
/// materialize projected into a fresh snapshot, which is re-attached as the new
/// base. Survives a reopen with the column reclaimed.
#[test]
fn checkpoint_compacts_disk_first() {
    let opts = SqlOptions {
        disk_first: true,
        checkpoint_bytes: 0,
    };
    let dir = tempfile::tempdir().unwrap();
    let db = SqlEngine::open_with_options(dir.path(), opts.clone()).unwrap();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY, big TEXT, keep INT)")
        .unwrap();
    let filler = "y".repeat(200);
    for i in 1..=100 {
        db.execute(&format!("INSERT INTO u VALUES ({i}, '{filler}', {i})"))
            .unwrap();
    }
    db.checkpoint().unwrap(); // base at full arity
    let rdat = dir.path().join("u.rdat");
    let size_before = std::fs::metadata(&rdat).unwrap().len();

    db.execute("ALTER TABLE u DROP COLUMN big").unwrap();
    db.checkpoint().unwrap(); // compacts base rows into a fresh snapshot
    let size_after = std::fs::metadata(&rdat).unwrap().len();
    assert!(
        size_after * 2 < size_before,
        "expected disk-first compaction to shrink the base: {size_before} -> {size_after}"
    );

    let total: i64 = (1..=100).sum();
    assert_eq!(
        rows(&db, "SELECT SUM(keep) FROM u"),
        vec![vec![Value::Int(total)]]
    );
    // The compacted base is usable for further writes, then reopen.
    db.execute("INSERT INTO u VALUES (101, 7)").unwrap();
    drop(db);
    let db = SqlEngine::open_with_options(dir.path(), opts).unwrap();
    assert!(db.execute("SELECT big FROM u").is_err());
    assert_eq!(
        rows(&db, "SELECT SUM(keep) FROM u"),
        vec![vec![Value::Int(total + 7)]]
    );
}
