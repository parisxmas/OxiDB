//! INSERT / UPDATE / DELETE behavior and edge cases.

mod common;
use common::*;

#[test]
fn multi_row_insert() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT, v INT)").unwrap();
    assert_eq!(
        affected(&db, "INSERT INTO t VALUES (1,10),(2,20),(3,30)"),
        3
    );
    assert_eq!(rows(&db, "SELECT id FROM t ORDER BY id").len(), 3);
}

#[test]
fn column_list_reorders_values() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (a INT, b INT, c INT)").unwrap();
    // Provide columns out of declared order.
    affected(&db, "INSERT INTO t (c, a, b) VALUES (30, 10, 20)");
    assert_eq!(
        rows(&db, "SELECT a, b, c FROM t"),
        r1(vec![i(10), i(20), i(30)])
    );
}

#[test]
fn omitted_columns_default_to_null() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT, x INT, y INT)").unwrap();
    affected(&db, "INSERT INTO t (id) VALUES (1)");
    assert_eq!(rows(&db, "SELECT x, y FROM t"), r1(vec![NULL, NULL]));
}

#[test]
fn insert_arity_mismatch_errors() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (a INT, b INT)").unwrap();
    assert!(db.execute("INSERT INTO t VALUES (1, 2, 3)").is_err());
    assert!(db.execute("INSERT INTO t VALUES (1)").is_err());
}

#[test]
fn insert_unknown_column_errors() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (a INT)").unwrap();
    assert!(db.execute("INSERT INTO t (nope) VALUES (1)").is_err());
}

#[test]
fn not_null_violation_errors() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT NOT NULL, v INT)")
        .unwrap();
    assert!(db.execute("INSERT INTO t VALUES (NULL, 1)").is_err());
    assert!(db.execute("INSERT INTO t (v) VALUES (1)").is_err());
}

#[test]
fn primary_key_implies_not_null() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    assert!(db.execute("INSERT INTO t VALUES (NULL, 1)").is_err());
    assert!(db.execute("INSERT INTO t VALUES (1, 1)").is_ok());
}

#[test]
fn wrong_type_insert_errors() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (n INT, s TEXT)").unwrap();
    // text into int column
    assert!(db.execute("INSERT INTO t VALUES ('x', 'y')").is_err());
    // int into double is also a mismatch (no implicit coercion)
    db.execute("CREATE TABLE f (d DOUBLE)").unwrap();
    assert!(db.execute("INSERT INTO f VALUES (5)").is_err());
    assert!(db.execute("INSERT INTO f VALUES (5.0)").is_ok());
}

#[test]
fn update_all_rows_without_where() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT, v INT)").unwrap();
    affected(&db, "INSERT INTO t VALUES (1,1),(2,2),(3,3)");
    assert_eq!(affected(&db, "UPDATE t SET v = 0"), 3);
    assert_eq!(
        rows(&db, "SELECT v FROM t"),
        vec![vec![i(0)], vec![i(0)], vec![i(0)]]
    );
}

#[test]
fn update_expression_references_other_columns() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT, a INT, b INT)").unwrap();
    affected(&db, "INSERT INTO t VALUES (1, 5, 7)");
    affected(&db, "UPDATE t SET a = a + b WHERE id = 1");
    assert_eq!(rows(&db, "SELECT a FROM t"), r1(vec![i(12)]));
}

#[test]
fn update_matching_none_affects_zero() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT, v INT)").unwrap();
    affected(&db, "INSERT INTO t VALUES (1, 1)");
    assert_eq!(affected(&db, "UPDATE t SET v = 9 WHERE id = 999"), 0);
}

#[test]
fn update_to_null_and_type_check() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT, v INT)").unwrap();
    affected(&db, "INSERT INTO t VALUES (1, 5)");
    affected(&db, "UPDATE t SET v = NULL WHERE id = 1");
    assert_eq!(rows(&db, "SELECT v FROM t"), r1(vec![NULL]));
    // UPDATE that violates NOT NULL fails.
    db.execute("CREATE TABLE u (id INT NOT NULL)").unwrap();
    affected(&db, "INSERT INTO u VALUES (1)");
    assert!(db.execute("UPDATE u SET id = NULL").is_err());
}

#[test]
fn delete_all_and_predicate_and_none() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT)").unwrap();
    affected(&db, "INSERT INTO t VALUES (1),(2),(3),(4)");
    assert_eq!(affected(&db, "DELETE FROM t WHERE id = 999"), 0);
    assert_eq!(affected(&db, "DELETE FROM t WHERE id > 2"), 2);
    assert_eq!(affected(&db, "DELETE FROM t"), 2);
    assert!(rows(&db, "SELECT id FROM t").is_empty());
}

#[test]
fn multiple_statements_in_one_execute() {
    let (_d, db) = open();
    let results = db
        .execute("CREATE TABLE t (id INT); INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)")
        .unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(rows(&db, "SELECT id FROM t ORDER BY id").len(), 2);
}

#[test]
fn empty_sql_returns_no_results() {
    let (_d, db) = open();
    assert!(db.execute("").unwrap().is_empty());
    assert!(db.execute("   ").unwrap().is_empty());
}
