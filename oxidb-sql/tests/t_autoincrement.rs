//! AUTO_INCREMENT primary keys: assignment, explicit values, persistence,
//! transactions, and `last_insert_id`.

mod common;

use common::*;
use oxidb_sql::{QueryResult, SqlEngine, Value};

fn t(s: &str) -> Value {
    Value::Text(s.to_string())
}

/// Run one INSERT and return (affected, last_insert_id).
fn ins(db: &SqlEngine, sql: &str) -> (usize, Option<i64>) {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Mutation {
            affected,
            last_insert_id,
        } => (affected, last_insert_id),
        other => panic!("expected Mutation, got {other:?}"),
    }
}

#[test]
fn assigns_when_omitted_or_null() {
    let (_d, db) = open();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY AUTO_INCREMENT, name TEXT NOT NULL)")
        .unwrap();

    // Omitted via column list.
    let (n, id) = ins(&db, "INSERT INTO u (name) VALUES ('ada')");
    assert_eq!((n, id), (1, Some(1)));
    // Explicit NULL placeholder in a full-arity insert.
    let (n, id) = ins(&db, "INSERT INTO u VALUES (NULL, 'bob')");
    assert_eq!((n, id), (1, Some(2)));
    // Multi-row: sequential values, last one reported.
    let (n, id) = ins(&db, "INSERT INTO u (name) VALUES ('c'), ('d'), ('e')");
    assert_eq!((n, id), (3, Some(5)));

    assert_eq!(
        rows(&db, "SELECT id, name FROM u ORDER BY id"),
        vec![
            vec![Value::Int(1), t("ada")],
            vec![Value::Int(2), t("bob")],
            vec![Value::Int(3), t("c")],
            vec![Value::Int(4), t("d")],
            vec![Value::Int(5), t("e")],
        ]
    );
}

#[test]
fn explicit_values_bump_the_counter() {
    let (_d, db) = open();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY AUTOINCREMENT, name TEXT)")
        .unwrap();
    let (_, id) = ins(&db, "INSERT INTO u VALUES (100, 'explicit')");
    assert_eq!(id, None); // nothing generated
    let (_, id) = ins(&db, "INSERT INTO u (name) VALUES ('next')");
    assert_eq!(id, Some(101));
    // PK uniqueness still enforced on explicit duplicates.
    assert!(db.execute("INSERT INTO u VALUES (101, 'dup')").is_err());
}

#[test]
fn counter_survives_reopen_and_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = SqlEngine::open(dir.path()).unwrap();
        db.execute("CREATE TABLE u (id INT PRIMARY KEY AUTO_INCREMENT, name TEXT)")
            .unwrap();
        ins(&db, "INSERT INTO u (name) VALUES ('a'), ('b')");
        db.checkpoint().unwrap();
        ins(&db, "INSERT INTO u (name) VALUES ('c')"); // WAL-only tail
    }
    let db = SqlEngine::open(dir.path()).unwrap();
    let (_, id) = ins(&db, "INSERT INTO u (name) VALUES ('d')");
    assert_eq!(id, Some(4));
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM u"),
        vec![vec![Value::Int(4)]]
    );
}

#[test]
fn works_inside_transactions() {
    let (_d, db) = open();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY AUTO_INCREMENT, name TEXT)")
        .unwrap();
    ins(&db, "INSERT INTO u (name) VALUES ('pre')");

    let results = db
        .execute(
            "BEGIN; \
             INSERT INTO u (name) VALUES ('t1'); \
             INSERT INTO u (name) VALUES ('t2'); \
             COMMIT;",
        )
        .unwrap();
    match &results[2] {
        QueryResult::Mutation { last_insert_id, .. } => assert_eq!(*last_insert_id, Some(3)),
        other => panic!("expected Mutation, got {other:?}"),
    }
    assert_eq!(
        rows(&db, "SELECT id FROM u ORDER BY id"),
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)]
        ]
    );

    // Rolled-back values leave a gap but nothing else.
    db.execute("BEGIN; INSERT INTO u (name) VALUES ('lost'); ROLLBACK;")
        .unwrap();
    let (_, id) = ins(&db, "INSERT INTO u (name) VALUES ('after')");
    assert_eq!(id, Some(4)); // engine counter untouched by the rollback
}

#[test]
fn generated_as_identity_and_describe() {
    let (_d, db) = open();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, name TEXT)")
        .unwrap();
    let (_, id) = ins(&db, "INSERT INTO u (name) VALUES ('pg-style')");
    assert_eq!(id, Some(1));

    let r = rows(&db, "DESCRIBE u");
    assert_eq!(r[0][0], t("id"));
    assert_eq!(r[0][4], Value::Bool(true)); // auto_increment column
    assert_eq!(r[1][4], Value::Bool(false));
}

#[test]
fn rejected_on_non_int_or_non_pk() {
    let (_d, db) = open();
    let err = db
        .execute("CREATE TABLE bad (id TEXT PRIMARY KEY AUTO_INCREMENT)")
        .unwrap_err();
    assert!(err.to_string().contains("AUTO_INCREMENT"), "{err}");
    let err = db
        .execute("CREATE TABLE bad (id INT AUTO_INCREMENT, k INT PRIMARY KEY)")
        .unwrap_err();
    assert!(err.to_string().contains("AUTO_INCREMENT"), "{err}");
}
