//! AND/OR short-circuit must skip work without changing a single result.
//!
//! `eval_scalar` returns early once the left operand decides a conjunction or
//! disjunction. That is only sound for a definitive FALSE (AND) / TRUE (OR):
//! SQL's three-valued logic makes `NULL AND FALSE` FALSE and `NULL OR TRUE`
//! TRUE, so a NULL left operand must still evaluate the right. These tests pin
//! the full truth table (TRUE/FALSE/NULL on each side, both operand orders)
//! and prove the right operand really is skipped.

mod common;

use common::*;
use oxidb_sql::Value;

/// Scalar result of `SELECT <expr>`, rendered stably.
fn scalar(db: &oxidb_sql::SqlEngine, expr: &str) -> String {
    format!("{:?}", rows(db, &format!("SELECT {expr}"))[0][0])
}

#[test]
fn and_or_truth_table_survives_short_circuit() {
    let (_d, db) = open();
    let cases = [
        // AND: FALSE dominates, NULL is unknown.
        ("TRUE AND TRUE", "Bool(true)"),
        ("TRUE AND FALSE", "Bool(false)"),
        ("FALSE AND TRUE", "Bool(false)"),
        ("FALSE AND FALSE", "Bool(false)"),
        ("TRUE AND NULL", "Null"),
        ("NULL AND TRUE", "Null"),
        // The cases that forbid short-circuiting on a NULL left: FALSE still
        // dominates from the right, so the right MUST be evaluated.
        ("NULL AND FALSE", "Bool(false)"),
        ("FALSE AND NULL", "Bool(false)"),
        ("NULL AND NULL", "Null"),
        // OR: TRUE dominates, NULL is unknown.
        ("TRUE OR TRUE", "Bool(true)"),
        ("TRUE OR FALSE", "Bool(true)"),
        ("FALSE OR TRUE", "Bool(true)"),
        ("FALSE OR FALSE", "Bool(false)"),
        ("FALSE OR NULL", "Null"),
        ("NULL OR FALSE", "Null"),
        // Mirror: TRUE dominates from the right.
        ("NULL OR TRUE", "Bool(true)"),
        ("TRUE OR NULL", "Bool(true)"),
        ("NULL OR NULL", "Null"),
    ];
    for (expr, want) in cases {
        assert_eq!(scalar(&db, expr), want, "{expr}");
    }
}

#[test]
fn short_circuit_matches_unfolded_filter_over_rows() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT, a INT, b INT)").unwrap();
    // NULLs on both sides so the unknown cases are exercised per row.
    for (id, a, b) in [
        (1, "1", "1"),
        (2, "1", "2"),
        (3, "2", "1"),
        (4, "2", "2"),
        (5, "NULL", "1"),
        (6, "NULL", "2"),
        (7, "1", "NULL"),
        (8, "2", "NULL"),
        (9, "NULL", "NULL"),
    ] {
        db.execute(&format!("INSERT INTO t VALUES ({id}, {a}, {b})"))
            .unwrap();
    }
    let ids = |sql: &str| -> Vec<i64> {
        rows(&db, sql)
            .iter()
            .map(|r| match r[0] {
                Value::Int(i) => i,
                ref v => panic!("not an int: {v:?}"),
            })
            .collect()
    };
    // A WHERE keeps only rows where the predicate is TRUE (NULL is not TRUE).
    assert_eq!(
        ids("SELECT id FROM t WHERE a = 1 AND b = 1 ORDER BY id"),
        [1]
    );
    assert_eq!(
        ids("SELECT id FROM t WHERE a = 1 OR b = 1 ORDER BY id"),
        [1, 2, 3, 5, 7]
    );
    // Left decides FALSE for every row: right skipped, result unchanged.
    let empty: [i64; 0] = [];
    assert_eq!(
        ids("SELECT id FROM t WHERE a = 99 AND b = 1 ORDER BY id"),
        empty
    );
    // Left decides TRUE for every row: right skipped, result unchanged.
    assert_eq!(
        ids("SELECT id FROM t WHERE 1 = 1 OR b = 99 ORDER BY id"),
        [1, 2, 3, 4, 5, 6, 7, 8, 9]
    );
    // `b = 99` is FALSE where b is set and NULL where b is NULL (rows 7, 9).
    // Per row: 1-2 TRUE AND FALSE = FALSE; 3-4 FALSE (left short-circuits);
    // 5-6 NULL AND FALSE = FALSE — the case that proves a NULL left still
    // evaluates the right; 7 TRUE AND NULL = NULL; 8 FALSE AND NULL = FALSE
    // (left short-circuits over a NULL right); 9 NULL AND NULL = NULL.
    assert_eq!(
        ids("SELECT id FROM t WHERE (a = 1 AND b = 99) IS NOT NULL ORDER BY id"),
        [1, 2, 3, 4, 5, 6, 8]
    );
    // Unknown survives only where a NULL meets a non-deciding partner: row 5
    // (NULL AND TRUE), 7 (TRUE AND NULL), 9 (NULL AND NULL). Row 6 is NULL AND
    // FALSE = FALSE, so it drops out — again only correct because the right
    // operand ran.
    assert_eq!(
        ids("SELECT id FROM t WHERE (a = 1 AND b = 1) IS NULL ORDER BY id"),
        [5, 7, 9]
    );
}

#[test]
fn short_circuit_skips_the_right_operand() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT, x INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 0)").unwrap();
    // `1/x` divides by zero. Guarding it with a definitively FALSE left must
    // skip it — the classic observable effect of short-circuiting, and proof
    // the right operand is not evaluated.
    let r = db.execute("SELECT id FROM t WHERE x <> 0 AND 1 / x > 0");
    assert!(r.is_ok(), "right operand ran despite a FALSE left: {r:?}");
    // Mirror for OR with a definitively TRUE left.
    let r = db.execute("SELECT id FROM t WHERE x = 0 OR 1 / x > 0");
    assert!(r.is_ok(), "right operand ran despite a TRUE left: {r:?}");
}
