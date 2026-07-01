//! End-to-end SQL smoke tests driving the engine purely through `execute()`.
//!
//! Covers the Phase 1 surface: DDL, INSERT (positional + column-list),
//! SELECT with WHERE / ORDER BY / LIMIT / projection, UPDATE, DELETE, and
//! durability of SQL mutations across a reopen.

use oxidb_sql::{QueryResult, SqlEngine, Value};

fn open(dir: &std::path::Path) -> SqlEngine {
    SqlEngine::open(dir).unwrap()
}

/// Helper: run one statement and unwrap a SELECT result.
fn select(db: &SqlEngine, sql: &str) -> (Vec<String>, Vec<Vec<Value>>) {
    let mut results = db.execute(sql).unwrap();
    assert_eq!(results.len(), 1, "expected exactly one statement");
    match results.pop().unwrap() {
        QueryResult::Select { columns, rows } => (columns, rows),
        other => panic!("expected Select, got {other:?}"),
    }
}

fn affected(db: &SqlEngine, sql: &str) -> usize {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Mutation { affected } => affected,
        other => panic!("expected Mutation, got {other:?}"),
    }
}

#[test]
fn ddl_and_positional_insert_and_select() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());

    db.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, age INT)")
        .unwrap();
    assert_eq!(
        affected(
            &db,
            "INSERT INTO users VALUES (1, 'ada', 36), (2, 'bob', 41)"
        ),
        2
    );

    let (cols, rows) = select(&db, "SELECT * FROM users");
    assert_eq!(cols, vec!["id", "name", "age"]);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][1], Value::Text("ada".into()));
}

#[test]
fn column_list_insert_fills_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    db.execute("CREATE TABLE t (id INT, name TEXT, note TEXT)")
        .unwrap();
    // Omit `note` -> it must be NULL.
    affected(&db, "INSERT INTO t (id, name) VALUES (1, 'x')");
    let (_cols, rows) = select(&db, "SELECT note FROM t");
    assert_eq!(rows, vec![vec![Value::Null]]);
}

#[test]
fn where_order_limit_projection() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    db.execute("CREATE TABLE nums (id INT, v INT)").unwrap();
    affected(
        &db,
        "INSERT INTO nums VALUES (1, 30), (2, 10), (3, 20), (4, 40)",
    );

    // WHERE + ORDER BY DESC + LIMIT + single-column projection.
    let (cols, rows) = select(
        &db,
        "SELECT v FROM nums WHERE v > 10 ORDER BY v DESC LIMIT 2",
    );
    assert_eq!(cols, vec!["v"]);
    assert_eq!(rows, vec![vec![Value::Int(40)], vec![Value::Int(30)]]);
}

#[test]
fn where_with_and_or_and_arithmetic() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    db.execute("CREATE TABLE p (id INT, price INT, qty INT)")
        .unwrap();
    affected(
        &db,
        "INSERT INTO p VALUES (1, 10, 5), (2, 20, 1), (3, 5, 100)",
    );

    // price * qty >= 50 AND price < 20  -> row 1 (50) and row 3 (500)
    let (_c, rows) = select(
        &db,
        "SELECT id FROM p WHERE price * qty >= 50 AND price < 20 ORDER BY id",
    );
    assert_eq!(rows, vec![vec![Value::Int(1)], vec![Value::Int(3)]]);
}

#[test]
fn is_null_predicate() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    db.execute("CREATE TABLE t (id INT, note TEXT)").unwrap();
    affected(&db, "INSERT INTO t (id) VALUES (1)");
    affected(&db, "INSERT INTO t VALUES (2, 'hi')");

    let (_c, nulls) = select(&db, "SELECT id FROM t WHERE note IS NULL");
    assert_eq!(nulls, vec![vec![Value::Int(1)]]);
    let (_c, not_nulls) = select(&db, "SELECT id FROM t WHERE note IS NOT NULL");
    assert_eq!(not_nulls, vec![vec![Value::Int(2)]]);
}

#[test]
fn update_and_delete() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    db.execute("CREATE TABLE t (id INT, v INT)").unwrap();
    affected(&db, "INSERT INTO t VALUES (1, 1), (2, 2), (3, 3)");

    assert_eq!(affected(&db, "UPDATE t SET v = v + 10 WHERE id >= 2"), 2);
    let (_c, rows) = select(&db, "SELECT v FROM t ORDER BY id");
    assert_eq!(
        rows,
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(12)],
            vec![Value::Int(13)]
        ]
    );

    assert_eq!(affected(&db, "DELETE FROM t WHERE v > 12"), 1);
    let (_c, rows) = select(&db, "SELECT id FROM t ORDER BY id");
    assert_eq!(rows, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
}

#[test]
fn sql_mutations_are_durable() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open(dir.path());
        db.execute("CREATE TABLE t (id INT, name TEXT NOT NULL)")
            .unwrap();
        affected(&db, "INSERT INTO t VALUES (1, 'ada'), (2, 'bob')");
        affected(&db, "DELETE FROM t WHERE id = 2");
        // no checkpoint -> recovery must come from the WAL
    }
    let db = open(dir.path());
    let (_c, rows) = select(&db, "SELECT name FROM t");
    assert_eq!(rows, vec![vec![Value::Text("ada".into())]]);
}

#[test]
fn errors_surface_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    db.execute("CREATE TABLE t (id INT NOT NULL)").unwrap();

    // NOT NULL violation
    assert!(db.execute("INSERT INTO t VALUES (NULL)").is_err());
    // unknown table
    assert!(db.execute("SELECT * FROM nope").is_err());
    // unknown column
    assert!(db.execute("SELECT missing FROM t").is_err());
    // unsupported feature (compound/set query) should error, not panic
    assert!(
        db.execute("SELECT id FROM t UNION SELECT id FROM t")
            .is_err()
    );
    // parse error
    assert!(db.execute("SELCT nonsense").is_err());
}

#[test]
fn if_not_exists_and_if_exists() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    db.execute("CREATE TABLE t (id INT)").unwrap();
    // second CREATE without IF NOT EXISTS -> error
    assert!(db.execute("CREATE TABLE t (id INT)").is_err());
    // with IF NOT EXISTS -> ok, no-op
    assert!(db.execute("CREATE TABLE IF NOT EXISTS t (id INT)").is_ok());
    // DROP ... IF EXISTS on a missing table -> ok
    assert!(db.execute("DROP TABLE IF EXISTS ghost").is_ok());
    // plain DROP of missing table -> error
    assert!(db.execute("DROP TABLE ghost").is_err());
}
