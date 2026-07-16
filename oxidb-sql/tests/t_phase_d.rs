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
