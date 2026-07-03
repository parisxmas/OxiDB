//! Catalog introspection: SHOW TABLES / SHOW VIEWS / SHOW INDEXES / DESCRIBE.

mod common;

use common::*;
use oxidb_sql::Value;

fn t(s: &str) -> Value {
    Value::Text(s.to_string())
}

fn setup() -> (tempfile::TempDir, oxidb_sql::SqlEngine) {
    let (d, db) = open();
    db.execute("CREATE TABLE users (id INT PRIMARY KEY, email TEXT NOT NULL, age INT)")
        .unwrap();
    db.execute("CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, total DOUBLE)")
        .unwrap();
    db.execute("INSERT INTO users VALUES (1, 'a@x.co', 30), (2, 'b@x.co', 40)")
        .unwrap();
    db.execute("CREATE INDEX idx_orders_user ON orders (user_id)")
        .unwrap();
    db.execute("CREATE INDEX idx_users_email_age ON users (email, age)")
        .unwrap();
    db.execute("CREATE VIEW adults AS SELECT email FROM users WHERE age >= 18")
        .unwrap();
    (d, db)
}

#[test]
fn show_tables_lists_tables_with_row_counts() {
    let (_d, db) = setup();
    let (cols, r) = cols_rows(&db, "SHOW TABLES");
    assert_eq!(cols, vec!["table", "rows"]);
    assert_eq!(
        r,
        vec![
            vec![t("orders"), Value::Int(0)],
            vec![t("users"), Value::Int(2)],
        ]
    );
}

#[test]
fn show_views_lists_definitions() {
    let (_d, db) = setup();
    let (cols, r) = cols_rows(&db, "SHOW VIEWS");
    assert_eq!(cols, vec!["view", "definition"]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], t("adults"));
    match &r[0][1] {
        Value::Text(sql) => assert!(sql.contains("SELECT"), "got {sql}"),
        other => panic!("expected Text, got {other:?}"),
    }
}

#[test]
fn show_indexes_all_and_per_table() {
    let (_d, db) = setup();
    let (cols, r) = cols_rows(&db, "SHOW INDEXES");
    assert_eq!(cols, vec!["index", "table", "columns"]);
    assert_eq!(
        r,
        vec![
            vec![t("idx_orders_user"), t("orders"), t("user_id")],
            vec![t("idx_users_email_age"), t("users"), t("email, age")],
        ]
    );
    let r = rows(&db, "SHOW INDEXES FROM orders");
    assert_eq!(
        r,
        vec![vec![t("idx_orders_user"), t("orders"), t("user_id")]]
    );
    // The singular form parses too.
    assert_eq!(rows(&db, "SHOW INDEX FROM orders"), r);
    assert!(db.execute("SHOW INDEXES FROM nope").is_err());
}

#[test]
fn describe_reports_columns() {
    let (_d, db) = setup();
    for sql in ["DESCRIBE users", "SHOW COLUMNS FROM users"] {
        let (cols, r) = cols_rows(&db, sql);
        assert_eq!(cols, vec!["column", "type", "nullable", "primary_key"]);
        assert_eq!(
            r,
            vec![
                vec![t("id"), t("INT"), Value::Bool(false), Value::Bool(true)],
                vec![
                    t("email"),
                    t("TEXT"),
                    Value::Bool(false),
                    Value::Bool(false)
                ],
                vec![t("age"), t("INT"), Value::Bool(true), Value::Bool(false)],
            ],
            "for {sql}"
        );
    }
    assert!(db.execute("DESCRIBE nope").is_err());
}

#[test]
fn show_is_read_only() {
    for sql in [
        "SHOW TABLES",
        "SHOW VIEWS",
        "SHOW INDEXES",
        "SHOW INDEXES FROM users",
        "DESCRIBE users",
    ] {
        assert!(oxidb_sql::is_read_only(sql).unwrap(), "{sql}");
    }
    assert!(!oxidb_sql::is_read_only("DROP TABLE users").unwrap());
}

#[test]
fn show_inside_transaction_sees_buffered_ddl() {
    let (_d, db) = setup();
    let results = db
        .execute(
            "BEGIN; \
             CREATE TABLE staged (id INT); \
             DROP TABLE orders; \
             SHOW TABLES; \
             ROLLBACK;",
        )
        .unwrap();
    // Result 3 is the SHOW inside the transaction.
    match &results[3] {
        oxidb_sql::QueryResult::Select { rows, .. } => {
            let names: Vec<_> = rows.iter().map(|r| r[0].clone()).collect();
            assert_eq!(names, vec![t("staged"), t("users")]);
        }
        other => panic!("expected Select, got {other:?}"),
    }
    // Rolled back: committed catalog unchanged.
    let names: Vec<_> = rows(&db, "SHOW TABLES")
        .into_iter()
        .map(|mut r| r.remove(0))
        .collect();
    assert_eq!(names, vec![t("orders"), t("users")]);
}

#[test]
fn database_statements_parse() {
    use oxidb_sql::{DatabaseStatement, parse_database_statement as p};
    assert_eq!(
        p("CREATE DATABASE crm"),
        Some(DatabaseStatement::Create {
            name: "crm".into(),
            if_not_exists: false
        })
    );
    assert_eq!(
        p("create database if not exists crm;"),
        Some(DatabaseStatement::Create {
            name: "crm".into(),
            if_not_exists: true
        })
    );
    assert_eq!(
        p("DROP DATABASE IF EXISTS crm"),
        Some(DatabaseStatement::Drop {
            name: "crm".into(),
            if_exists: true
        })
    );
    assert_eq!(p("SHOW DATABASES"), Some(DatabaseStatement::Show));
    assert_eq!(
        p("USE crm"),
        Some(DatabaseStatement::Use { name: "crm".into() })
    );
    assert_eq!(
        p("use crm;"),
        Some(DatabaseStatement::Use { name: "crm".into() })
    );

    // Anything else — including mixing with other statements — is not one.
    assert_eq!(p("SELECT 1 FROM t"), None);
    assert_eq!(p("CREATE TABLE crm (id INT)"), None);
    assert_eq!(p("CREATE DATABASE crm; SELECT 1 FROM t"), None);
    assert_eq!(p("SHOW TABLES"), None);
}
