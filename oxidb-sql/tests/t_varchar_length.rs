//! `VARCHAR(n)` / `CHAR(n)` length enforcement (Postgres SQLSTATE 22001,
//! "value too long for type character varying(n)"). A declared length is now
//! stored in the catalog and checked on INSERT/UPDATE; plain `TEXT` stays
//! unbounded. Length is measured in characters, not bytes.

use oxidb_sql::{QueryResult, SqlEngine, Value};

fn open() -> (tempfile::TempDir, SqlEngine) {
    let dir = tempfile::tempdir().unwrap();
    let db = SqlEngine::open(dir.path()).unwrap();
    (dir, db)
}

fn err(db: &SqlEngine, sql: &str) -> String {
    match db.execute(sql) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("expected an error for: {sql}"),
    }
}

fn rows(db: &SqlEngine, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Select { rows, .. } => rows,
        other => panic!("expected SELECT, got {other:?}"),
    }
}

fn text(v: &Value) -> String {
    match v {
        Value::Text(s) => s.to_string(),
        other => format!("{other:?}"),
    }
}

#[test]
fn varchar_enforced_on_insert() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(5))")
        .unwrap();

    // Exactly the limit is fine.
    db.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();

    // One over the limit is rejected …
    let e = err(&db, "INSERT INTO t VALUES (2, 'toolong')");
    assert!(e.to_lowercase().contains("too long"), "got: {e}");

    // … and the rejected row was not written.
    assert_eq!(rows(&db, "SELECT * FROM t").len(), 1);
}

#[test]
fn varchar_enforced_on_update() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(5))")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'ok')").unwrap();

    let e = err(&db, "UPDATE t SET name = 'way too long' WHERE id = 1");
    assert!(e.to_lowercase().contains("too long"), "got: {e}");

    // The row is unchanged.
    let r = rows(&db, "SELECT name FROM t WHERE id = 1");
    assert_eq!(text(&r[0][0]), "ok");
}

#[test]
fn char_length_enforced_too() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, code CHAR(3))")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'abc')").unwrap();
    assert!(err(&db, "INSERT INTO t VALUES (2, 'abcd')")
        .to_lowercase()
        .contains("too long"));
}

#[test]
fn length_is_characters_not_bytes() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(3))")
        .unwrap();
    // "çğü" is 3 characters but 6 UTF-8 bytes — must be allowed.
    db.execute("INSERT INTO t VALUES (1, 'çğü')").unwrap();
    // 4 characters is over the limit.
    assert!(err(&db, "INSERT INTO t VALUES (2, 'çğüx')")
        .to_lowercase()
        .contains("too long"));
}

#[test]
fn plain_text_and_unsized_varchar_are_unbounded() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b VARCHAR)")
        .unwrap();
    let big = "x".repeat(10_000);
    db.execute(&format!("INSERT INTO t VALUES (1, '{big}', '{big}')"))
        .unwrap();
    assert_eq!(rows(&db, "SELECT * FROM t").len(), 1);
}

#[test]
fn describe_reports_varchar_length() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(20), body TEXT)")
        .unwrap();
    let r = rows(&db, "DESCRIBE t");
    // columns: [column, type, nullable, primary_key, auto_increment]
    let ty = |col: &str| -> String {
        r.iter()
            .find(|row| text(&row[0]) == col)
            .map(|row| text(&row[1]))
            .unwrap_or_default()
    };
    assert_eq!(ty("name"), "VARCHAR(20)");
    assert_eq!(ty("body"), "TEXT");
    assert_eq!(ty("id"), "INT");
}

#[test]
fn alter_add_column_enforces_length() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    db.execute("ALTER TABLE t ADD COLUMN tag VARCHAR(4)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'abcd')").unwrap();
    assert!(err(&db, "INSERT INTO t VALUES (2, 'abcde')")
        .to_lowercase()
        .contains("too long"));
}
