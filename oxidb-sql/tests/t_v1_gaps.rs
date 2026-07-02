//! Tests for the post-Phase-4 v1 gap closure: PRIMARY KEY uniqueness,
//! implicit INT->DOUBLE/TIMESTAMP coercion, timestamp literals, OFFSET,
//! UNION [ALL], IN lists, and uncorrelated subqueries.

mod common;

use common::*;
use oxidb_sql::{SqlError, Value};

// ── PRIMARY KEY uniqueness ──────────────────────────────────────────────────

#[test]
fn pk_duplicate_insert_is_rejected() {
    let (_d, db) = open();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO u VALUES (1, 'ada')").unwrap();
    let err = db.execute("INSERT INTO u VALUES (1, 'bob')").unwrap_err();
    assert!(matches!(err, SqlError::DuplicateKey(_)), "got {err:?}");
    // The failed insert changed nothing.
    assert_eq!(rows(&db, "SELECT name FROM u"), vec![vec![t("ada")]]);
}

#[test]
fn pk_duplicate_within_one_insert_is_rejected_atomically() {
    let (_d, db) = open();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY)").unwrap();
    assert!(db.execute("INSERT INTO u VALUES (1), (2), (1)").is_err());
    // All-or-nothing: no rows landed.
    assert!(rows(&db, "SELECT id FROM u").is_empty());
}

#[test]
fn pk_update_conflicts_and_self_update_allowed() {
    let (_d, db) = open();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO u VALUES (1, 10), (2, 20)").unwrap();
    // Moving row 2 onto key 1 conflicts.
    assert!(db.execute("UPDATE u SET id = 1 WHERE id = 2").is_err());
    // Updating a row without changing its key (or to a fresh key) is fine.
    assert!(db.execute("UPDATE u SET v = 11 WHERE id = 1").is_ok());
    assert!(db.execute("UPDATE u SET id = 3 WHERE id = 2").is_ok());
    // A deleted key can be reused.
    db.execute("DELETE FROM u WHERE id = 1").unwrap();
    assert!(db.execute("INSERT INTO u VALUES (1, 100)").is_ok());
}

#[test]
fn pk_enforced_inside_transactions() {
    let (_d, db) = open();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO u VALUES (1)").unwrap();
    // Conflict with a committed row, inside a transaction.
    assert!(
        db.execute("BEGIN; INSERT INTO u VALUES (1); COMMIT;")
            .is_err()
    );
    // Conflict between two uncommitted rows of the same transaction.
    assert!(
        db.execute("BEGIN; INSERT INTO u VALUES (2); INSERT INTO u VALUES (2); COMMIT;")
            .is_err()
    );
    // Delete-then-reinsert of the same key inside a transaction is fine.
    db.execute("BEGIN; DELETE FROM u WHERE id = 1; INSERT INTO u VALUES (1); COMMIT;")
        .unwrap();
    assert_eq!(rows(&db, "SELECT id FROM u"), vec![vec![i(1)]]);
}

#[test]
fn pk_uniqueness_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open_at(dir.path());
        db.execute("CREATE TABLE u (id INT PRIMARY KEY)").unwrap();
        db.execute("INSERT INTO u VALUES (7)").unwrap();
    }
    let db = open_at(dir.path());
    assert!(db.execute("INSERT INTO u VALUES (7)").is_err());
    assert!(db.execute("INSERT INTO u VALUES (8)").is_ok());
}

#[test]
fn multiple_pk_columns_rejected() {
    let (_d, db) = open();
    assert!(
        db.execute("CREATE TABLE bad (a INT PRIMARY KEY, b INT PRIMARY KEY)")
            .is_err()
    );
}

// ── implicit coercion + timestamp literals ─────────────────────────────────

#[test]
fn int_coerces_into_double_and_timestamp_columns() {
    let (_d, db) = open();
    db.execute("CREATE TABLE m (d DOUBLE, ts TIMESTAMP)")
        .unwrap();
    db.execute("INSERT INTO m VALUES (5, 1700000000000)")
        .unwrap();
    assert_eq!(
        rows(&db, "SELECT d, ts FROM m"),
        vec![vec![d(5.0), Value::Timestamp(1_700_000_000_000)]]
    );
}

#[test]
fn coercion_applies_to_params_and_updates() {
    let (_d, db) = open();
    db.execute("CREATE TABLE m (x DOUBLE)").unwrap();
    db.execute_params("INSERT INTO m VALUES (?)", &[Value::Int(3)])
        .unwrap();
    db.execute("UPDATE m SET x = 4").unwrap();
    assert_eq!(rows(&db, "SELECT x FROM m"), vec![vec![d(4.0)]]);
}

#[test]
fn timestamp_literal_forms() {
    let (_d, db) = open();
    db.execute("CREATE TABLE e (ts TIMESTAMP)").unwrap();
    db.execute("INSERT INTO e VALUES (TIMESTAMP '1970-01-01 00:00:00')")
        .unwrap();
    db.execute("INSERT INTO e VALUES (TIMESTAMP '1970-01-02')")
        .unwrap();
    db.execute("INSERT INTO e VALUES (TIMESTAMP '2000-01-01T00:00:00.250Z')")
        .unwrap();
    db.execute("INSERT INTO e VALUES (TIMESTAMP '1970-01-01 02:00:00+02:00')")
        .unwrap();
    assert_eq!(
        rows(&db, "SELECT ts FROM e"),
        vec![
            vec![Value::Timestamp(0)],
            vec![Value::Timestamp(86_400_000)],
            vec![Value::Timestamp(946_684_800_250)],
            vec![Value::Timestamp(0)], // +02:00 offset converts back to epoch
        ]
    );
    // Comparisons against timestamp literals work in WHERE.
    assert_eq!(
        rows(
            &db,
            "SELECT COUNT(*) AS n FROM e WHERE ts < TIMESTAMP '1970-01-01 12:00:00'"
        ),
        vec![vec![i(2)]]
    );
    assert!(
        db.execute("INSERT INTO e VALUES (TIMESTAMP 'not a date')")
            .is_err()
    );
}

// ── OFFSET ──────────────────────────────────────────────────────────────────

#[test]
fn offset_pages_through_results() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3),(4),(5)")
        .unwrap();
    assert_eq!(
        rows(&db, "SELECT id FROM t ORDER BY id LIMIT 2 OFFSET 2"),
        vec![vec![i(3)], vec![i(4)]]
    );
    // OFFSET without LIMIT skips and returns the rest.
    assert_eq!(
        rows(&db, "SELECT id FROM t ORDER BY id OFFSET 4"),
        vec![vec![i(5)]]
    );
    // OFFSET past the end is empty.
    assert!(rows(&db, "SELECT id FROM t ORDER BY id OFFSET 9").is_empty());
}

// ── UNION / UNION ALL ───────────────────────────────────────────────────────

#[test]
fn union_distinct_and_all() {
    let (_d, db) = open();
    db.execute("CREATE TABLE a (x INT)").unwrap();
    db.execute("CREATE TABLE b (x INT)").unwrap();
    db.execute("INSERT INTO a VALUES (1),(2),(2)").unwrap();
    db.execute("INSERT INTO b VALUES (2),(3)").unwrap();
    // UNION dedups across (and within) arms.
    assert_eq!(
        rows(&db, "SELECT x FROM a UNION SELECT x FROM b ORDER BY x"),
        vec![vec![i(1)], vec![i(2)], vec![i(3)]]
    );
    // UNION ALL keeps duplicates.
    assert_eq!(
        rows(&db, "SELECT x FROM a UNION ALL SELECT x FROM b ORDER BY x"),
        vec![vec![i(1)], vec![i(2)], vec![i(2)], vec![i(2)], vec![i(3)]]
    );
}

#[test]
fn union_order_limit_offset_apply_to_combined_result() {
    let (_d, db) = open();
    db.execute("CREATE TABLE a (x INT)").unwrap();
    db.execute("INSERT INTO a VALUES (5),(1),(3)").unwrap();
    assert_eq!(
        rows(
            &db,
            "SELECT x FROM a UNION ALL SELECT x FROM a ORDER BY x LIMIT 3 OFFSET 1"
        ),
        vec![vec![i(1)], vec![i(3)], vec![i(3)]]
    );
    // ORDER BY by 1-based position.
    assert_eq!(
        rows(&db, "SELECT x FROM a UNION SELECT x FROM a ORDER BY 1"),
        vec![vec![i(1)], vec![i(3)], vec![i(5)]]
    );
}

#[test]
fn union_arity_mismatch_errors() {
    let (_d, db) = open();
    db.execute("CREATE TABLE a (x INT, y INT)").unwrap();
    db.execute("INSERT INTO a VALUES (1, 2)").unwrap();
    assert!(
        db.execute("SELECT x FROM a UNION SELECT x, y FROM a")
            .is_err()
    );
}

// ── IN lists + subqueries ───────────────────────────────────────────────────

#[test]
fn in_list_with_null_semantics() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, NULL)")
        .unwrap();
    assert_eq!(
        rows(&db, "SELECT id FROM t WHERE v IN (10, 30) ORDER BY id"),
        vec![vec![i(1)]]
    );
    // NULL v never matches IN; NOT IN with a NULL in the list matches nothing.
    assert_eq!(
        rows(&db, "SELECT id FROM t WHERE v NOT IN (10) ORDER BY id"),
        vec![vec![i(2)]]
    );
    assert!(rows(&db, "SELECT id FROM t WHERE v NOT IN (10, NULL)").is_empty());
}

#[test]
fn scalar_subquery_in_where_and_projection() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    assert_eq!(
        rows(&db, "SELECT id FROM t WHERE v = (SELECT MAX(v) FROM t)"),
        vec![vec![i(3)]]
    );
    assert_eq!(
        rows(&db, "SELECT (SELECT SUM(v) FROM t) AS total FROM t LIMIT 1"),
        vec![vec![i(60)]]
    );
    // Zero-row scalar subquery is NULL (matches nothing here).
    assert!(
        rows(
            &db,
            "SELECT id FROM t WHERE v = (SELECT v FROM t WHERE id = 99)"
        )
        .is_empty()
    );
    // More than one row errors.
    assert!(
        db.execute("SELECT id FROM t WHERE v = (SELECT v FROM t)")
            .is_err()
    );
}

#[test]
fn in_subquery() {
    let (_d, db) = open();
    db.execute("CREATE TABLE orders (id INT, customer_id INT)")
        .unwrap();
    db.execute("CREATE TABLE vip (customer_id INT)").unwrap();
    db.execute("INSERT INTO orders VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    db.execute("INSERT INTO vip VALUES (10), (30)").unwrap();
    assert_eq!(
        rows(
            &db,
            "SELECT id FROM orders WHERE customer_id IN (SELECT customer_id FROM vip) ORDER BY id"
        ),
        vec![vec![i(1)], vec![i(3)]]
    );
    assert_eq!(
        rows(
            &db,
            "SELECT id FROM orders WHERE customer_id NOT IN (SELECT customer_id FROM vip)"
        ),
        vec![vec![i(2)]]
    );
}

#[test]
fn subquery_works_inside_update_delete_and_transactions() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20)").unwrap();
    db.execute("UPDATE t SET v = (SELECT MAX(v) FROM t) WHERE id = 1")
        .unwrap();
    assert_eq!(
        rows(&db, "SELECT v FROM t ORDER BY id"),
        vec![vec![i(20)], vec![i(20)]]
    );
    db.execute("BEGIN; DELETE FROM t WHERE v IN (SELECT MAX(v) FROM t); COMMIT;")
        .unwrap();
    assert!(rows(&db, "SELECT id FROM t").is_empty());
}
