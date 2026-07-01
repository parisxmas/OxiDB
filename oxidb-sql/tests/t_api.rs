//! The programmatic (non-SQL) engine API: create_table/insert/scan/update_row/
//! delete/row_count/table_def, and the typed value model end-to-end.

use oxidb_sql::{Column, SqlEngine, SqlType, Table, Value};

fn users() -> Table {
    Table::new(
        "users",
        vec![
            Column::new("id", SqlType::Int).primary_key(),
            Column::new("name", SqlType::Text).not_null(),
            Column::new("age", SqlType::Int),
        ],
    )
}

#[test]
fn crud_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqlEngine::open(dir.path()).unwrap();
    db.create_table(users()).unwrap();

    let a = db
        .insert(
            "users",
            vec![Value::Int(1), Value::Text("ada".into()), Value::Int(36)],
        )
        .unwrap();
    let b = db
        .insert(
            "users",
            vec![Value::Int(2), Value::Text("bob".into()), Value::Null],
        )
        .unwrap();
    assert_ne!(a, b);
    assert_eq!(db.row_count("users").unwrap(), 2);

    // update_row preserves identity.
    db.update_row(
        "users",
        a,
        vec![Value::Int(1), Value::Text("ada2".into()), Value::Int(37)],
    )
    .unwrap();
    let scanned = db.scan("users").unwrap();
    assert_eq!(
        scanned.iter().find(|(id, _)| *id == a).unwrap().1[1],
        Value::Text("ada2".into())
    );

    assert!(db.delete("users", b).unwrap());
    assert!(!db.delete("users", b).unwrap());
    assert_eq!(db.row_count("users").unwrap(), 1);
}

#[test]
fn insert_validation() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqlEngine::open(dir.path()).unwrap();
    db.create_table(users()).unwrap();
    // NOT NULL name violated.
    assert!(
        db.insert("users", vec![Value::Int(1), Value::Null, Value::Int(1)])
            .is_err()
    );
    // Wrong arity.
    assert!(db.insert("users", vec![Value::Int(1)]).is_err());
    // Wrong type (text for age).
    assert!(
        db.insert(
            "users",
            vec![
                Value::Int(1),
                Value::Text("x".into()),
                Value::Text("y".into())
            ]
        )
        .is_err()
    );
}

#[test]
fn operations_on_missing_table_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqlEngine::open(dir.path()).unwrap();
    assert!(db.insert("ghost", vec![Value::Int(1)]).is_err());
    assert!(db.scan("ghost").is_err());
    assert!(db.row_count("ghost").is_err());
    assert!(db.delete("ghost", 1).is_err());
    assert!(db.update_row("ghost", 1, vec![Value::Int(1)]).is_err());
}

#[test]
fn update_row_requires_existing_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqlEngine::open(dir.path()).unwrap();
    db.create_table(users()).unwrap();
    // No such row id.
    assert!(
        db.update_row(
            "users",
            999,
            vec![Value::Int(1), Value::Text("x".into()), Value::Null]
        )
        .is_err()
    );
}

#[test]
fn all_value_types_roundtrip_through_storage() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = SqlEngine::open(dir.path()).unwrap();
        db.create_table(Table::new(
            "v",
            vec![
                Column::new("i", SqlType::Int),
                Column::new("d", SqlType::Double),
                Column::new("s", SqlType::Text),
                Column::new("b", SqlType::Bool),
                Column::new("ts", SqlType::Timestamp),
            ],
        ))
        .unwrap();
        db.insert(
            "v",
            vec![
                Value::Int(-42),
                Value::Double(3.5),
                Value::Text("héllo 🦀".into()),
                Value::Bool(true),
                Value::Timestamp(1_234_567_890),
            ],
        )
        .unwrap();
        db.insert(
            "v",
            vec![
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
            ],
        )
        .unwrap();
        db.checkpoint().unwrap();
    }
    let db = SqlEngine::open(dir.path()).unwrap();
    let rows = db.scan("v").unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].1,
        vec![
            Value::Int(-42),
            Value::Double(3.5),
            Value::Text("héllo 🦀".into()),
            Value::Bool(true),
            Value::Timestamp(1_234_567_890),
        ]
    );
    assert!(rows[1].1.iter().all(|v| matches!(v, Value::Null)));
}

#[test]
fn dir_accessor() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqlEngine::open(dir.path()).unwrap();
    assert_eq!(db.dir(), dir.path());
}
