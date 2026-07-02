//! Secondary indexes: creation, use, maintenance, persistence, errors.

mod common;
use common::*;

#[test]
fn index_equality_lookup_matches_full_scan() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT, tag TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'a'),(4,'c'),(5,'a')")
        .unwrap();
    db.execute("CREATE INDEX t_tag ON t(tag)").unwrap();
    assert_eq!(
        rows(&db, "SELECT id FROM t WHERE tag = 'a' ORDER BY id"),
        vec![vec![i(1)], vec![i(3)], vec![i(5)]]
    );
    // Non-matching key returns nothing.
    assert!(rows(&db, "SELECT id FROM t WHERE tag = 'z'").is_empty());
}

#[test]
fn index_on_int_column() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT, grp INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,7),(2,8),(3,7),(4,7)")
        .unwrap();
    db.execute("CREATE INDEX t_grp ON t(grp)").unwrap();
    assert_eq!(
        rows(&db, "SELECT id FROM t WHERE grp = 7 ORDER BY id"),
        vec![vec![i(1)], vec![i(3)], vec![i(4)]]
    );
}

#[test]
fn index_maintained_on_insert_update_delete() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT, tag TEXT)").unwrap();
    db.execute("CREATE INDEX t_tag ON t(tag)").unwrap();
    // Insert after index creation.
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'a')")
        .unwrap();
    assert_eq!(
        rows(&db, "SELECT id FROM t WHERE tag='a' ORDER BY id"),
        vec![vec![i(1)], vec![i(3)]]
    );
    // Update moves a row between index buckets.
    db.execute("UPDATE t SET tag='a' WHERE id=2").unwrap();
    assert_eq!(
        rows(&db, "SELECT id FROM t WHERE tag='a' ORDER BY id"),
        vec![vec![i(1)], vec![i(2)], vec![i(3)]]
    );
    db.execute("UPDATE t SET tag='c' WHERE id=1").unwrap();
    assert_eq!(
        rows(&db, "SELECT id FROM t WHERE tag='a' ORDER BY id"),
        vec![vec![i(2)], vec![i(3)]]
    );
    // Delete removes from the index.
    db.execute("DELETE FROM t WHERE id=3").unwrap();
    assert_eq!(
        rows(&db, "SELECT id FROM t WHERE tag='a' ORDER BY id"),
        r1(vec![i(2)])
    );
}

#[test]
fn dropping_index_still_returns_correct_results() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT, tag TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'a'),(3,'b')")
        .unwrap();
    db.execute("CREATE INDEX t_tag ON t(tag)").unwrap();
    db.execute("DROP INDEX t_tag").unwrap();
    // Falls back to full scan; same answer.
    assert_eq!(
        rows(&db, "SELECT id FROM t WHERE tag='a' ORDER BY id"),
        vec![vec![i(1)], vec![i(2)]]
    );
}

#[test]
fn index_persists_and_rebuilds_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = oxidb_sql::SqlEngine::open(dir.path()).unwrap();
        db.execute("CREATE TABLE t (id INT, tag TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'a')")
            .unwrap();
        db.execute("CREATE INDEX t_tag ON t(tag)").unwrap();
        db.checkpoint().unwrap();
    }
    let db = oxidb_sql::SqlEngine::open(dir.path()).unwrap();
    assert_eq!(
        rows(&db, "SELECT id FROM t WHERE tag='a' ORDER BY id"),
        vec![vec![i(1)], vec![i(3)]]
    );
}

#[test]
fn index_survives_reopen_without_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = oxidb_sql::SqlEngine::open(dir.path()).unwrap();
        db.execute("CREATE TABLE t (id INT, tag TEXT)").unwrap();
        db.execute("CREATE INDEX t_tag ON t(tag)").unwrap();
        db.execute("INSERT INTO t VALUES (1,'a'),(2,'a')").unwrap();
        // no checkpoint: index def + rows recovered from WAL replay
    }
    let db = oxidb_sql::SqlEngine::open(dir.path()).unwrap();
    assert_eq!(
        rows(&db, "SELECT id FROM t WHERE tag='a' ORDER BY id"),
        vec![vec![i(1)], vec![i(2)]]
    );
}

#[test]
fn if_not_exists_and_if_exists() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT, tag TEXT)").unwrap();
    db.execute("CREATE INDEX i ON t(tag)").unwrap();
    // Duplicate without IF NOT EXISTS -> error.
    assert!(db.execute("CREATE INDEX i ON t(tag)").is_err());
    // With IF NOT EXISTS -> ok.
    assert!(db.execute("CREATE INDEX IF NOT EXISTS i ON t(tag)").is_ok());
    // DROP IF EXISTS on missing -> ok; plain DROP -> error.
    assert!(db.execute("DROP INDEX IF EXISTS ghost").is_ok());
    assert!(db.execute("DROP INDEX ghost").is_err());
}

#[test]
fn create_index_on_unknown_column_or_table_errors() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT)").unwrap();
    assert!(db.execute("CREATE INDEX i ON t(missing)").is_err());
    assert!(db.execute("CREATE INDEX i ON ghost(x)").is_err());
}

#[test]
fn multi_column_index_serves_composite_equality() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (a INT, b INT, v TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 1, 'x'), (1, 2, 'y'), (2, 1, 'z')")
        .unwrap();
    db.execute("CREATE INDEX i ON t(a, b)").unwrap();
    assert_eq!(
        rows(&db, "SELECT v FROM t WHERE a = 1 AND b = 2"),
        vec![vec![t("y")]]
    );
    // Only part of the composite key -> falls back to a scan, same answer.
    assert_eq!(rows(&db, "SELECT v FROM t WHERE a = 2"), vec![vec![t("z")]]);
    // No match through the index.
    assert!(rows(&db, "SELECT v FROM t WHERE a = 9 AND b = 9").is_empty());
    // Writes maintain the composite index.
    db.execute("INSERT INTO t VALUES (1, 2, 'w')").unwrap();
    assert_eq!(
        rows(&db, "SELECT v FROM t WHERE a = 1 AND b = 2"),
        vec![vec![t("y")], vec![t("w")]]
    );
}

#[test]
fn index_lookup_with_null_key_returns_nothing() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT, tag TEXT)").unwrap();
    db.execute("INSERT INTO t (id) VALUES (1)").unwrap(); // tag NULL
    db.execute("INSERT INTO t VALUES (2, 'x')").unwrap();
    db.execute("CREATE INDEX t_tag ON t(tag)").unwrap();
    // tag = 'x' finds row 2; NULL rows never equal anything.
    assert_eq!(
        rows(&db, "SELECT id FROM t WHERE tag = 'x'"),
        r1(vec![i(2)])
    );
}

#[test]
fn programmatic_index_api() {
    use oxidb_sql::{Column, SqlType, Table, Value};
    let (_d, db) = open();
    db.create_table(Table::new(
        "t",
        vec![
            Column::new("id", SqlType::Int),
            Column::new("tag", SqlType::Text),
        ],
    ))
    .unwrap();
    db.insert("t", vec![Value::Int(1), Value::Text("a".into())])
        .unwrap();
    db.insert("t", vec![Value::Int(2), Value::Text("a".into())])
        .unwrap();
    db.create_index("t_tag", "t", &["tag".to_string()]).unwrap();
    // Duplicate index name errors; unknown column errors.
    assert!(db.create_index("t_tag", "t", &["tag".to_string()]).is_err());
    assert!(db.create_index("t_x", "t", &["nope".to_string()]).is_err());
    db.drop_index("t_tag").unwrap();
    assert!(db.drop_index("t_tag").is_err());
}
