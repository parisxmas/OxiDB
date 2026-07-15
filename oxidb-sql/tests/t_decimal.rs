//! Exact DECIMAL type + 2-argument ROUND: money-safe arithmetic, exact literal
//! parsing, string/int coercion, storage durability across reopen, and the
//! JSON wire form.

mod common;

use common::*;
use oxidb_sql::{Decimal, SqlEngine, Value};

fn dec(s: &str) -> Value {
    Value::Decimal(Box::new(Decimal::parse(s).unwrap()))
}

/// A one-row table so scalar expressions can be evaluated (the engine requires
/// a FROM clause).
fn seed_one(db: &SqlEngine) {
    db.execute("CREATE TABLE one (id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO one VALUES (1)").unwrap();
}

/// The headline case: SUM over 1000 two-scale values is *exact* as DECIMAL,
/// where the same data as DOUBLE drifts off the exact result.
#[test]
fn sum_of_1000_decimals_is_exact() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, p DECIMAL, pf DOUBLE)")
        .unwrap();
    // 1000 rows of 9.99 → exactly 9990.00.
    let mut sql = String::from("INSERT INTO t VALUES ");
    for i in 0..1000 {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("({i}, 9.99, 9.99)"));
    }
    db.execute(&sql).unwrap();

    // Exact DECIMAL sum.
    assert_eq!(rows(&db, "SELECT SUM(p) FROM t"), r1(vec![dec("9990.00")]));

    // The DOUBLE column drifts (classic float error): not equal to 9990.0.
    let got = rows(&db, "SELECT SUM(pf) FROM t");
    let Value::Double(f) = got[0][0] else {
        panic!("expected Double, got {got:?}");
    };
    assert!(
        (f - 9990.0).abs() > 0.0 && (f - 9990.0).abs() < 1e-6,
        "double sum should drift near but not equal 9990.0, got {f}"
    );

    // AVG over DECIMAL stays exact (9.99).
    assert_eq!(rows(&db, "SELECT AVG(p) FROM t"), r1(vec![dec("9.990000")]));
}

/// Inserting a string into a DECIMAL column now succeeds (parsed exactly);
/// this used to error with "expects Double".
#[test]
fn insert_string_into_decimal_column() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, p DECIMAL)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, '12.34')").unwrap();
    // An integer literal into a DECIMAL column widens exactly.
    db.execute("INSERT INTO t VALUES (2, 42)").unwrap();
    assert_eq!(
        rows(&db, "SELECT p FROM t ORDER BY id"),
        vec![vec![dec("12.34")], vec![dec("42")]]
    );
}

/// A fractional literal is an exact DECIMAL, so 0.1 + 0.2 is exactly 0.3 —
/// while forcing DOUBLE reproduces the IEEE-754 drift.
#[test]
fn decimal_literal_is_exact() {
    let (_d, db) = open();
    seed_one(&db);
    assert_eq!(
        rows(&db, "SELECT 0.1 + 0.2 AS x FROM one"),
        r1(vec![dec("0.3")])
    );

    let got = rows(
        &db,
        "SELECT CAST(0.1 AS DOUBLE) + CAST(0.2 AS DOUBLE) AS x FROM one",
    );
    let Value::Double(f) = got[0][0] else {
        panic!("expected Double, got {got:?}");
    };
    assert!(f != 0.3, "float path should show the drift, got {f}");
    assert!((f - 0.3).abs() < 1e-9);
}

/// ROUND: 1-arg (to integer, half-up) and 2-arg (to N places, half-up) over
/// DECIMAL, plus rounding an exact SUM.
#[test]
fn round_scalar_function() {
    let (_d, db) = open();
    seed_one(&db);
    // Half-up on a tie: 2.5 -> 3.
    assert_eq!(
        rows(&db, "SELECT ROUND(2.5) AS x FROM one"),
        r1(vec![dec("3")])
    );
    // 2-arg, half-up: 2.345 -> 2.35.
    assert_eq!(
        rows(&db, "SELECT ROUND(2.345, 2) AS x FROM one"),
        r1(vec![dec("2.35")])
    );
    // Not a tie: rounds down.
    assert_eq!(
        rows(&db, "SELECT ROUND(2.344, 2) AS x FROM one"),
        r1(vec![dec("2.34")])
    );

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, p DECIMAL)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10.005), (2, 20.004)")
        .unwrap();
    // SUM = 30.009 -> ROUND(_, 2) = 30.01 (half-up).
    assert_eq!(
        rows(&db, "SELECT ROUND(SUM(p), 2) FROM t"),
        r1(vec![dec("30.01")])
    );

    // ROUND on a DOUBLE returns a DOUBLE; on an INT returns the INT unchanged.
    assert_eq!(
        rows(&db, "SELECT ROUND(CAST(2.5 AS DOUBLE), 0) AS x FROM one"),
        r1(vec![d(3.0)])
    );
    assert_eq!(
        rows(&db, "SELECT ROUND(7, 2) AS x FROM one"),
        r1(vec![i(7)])
    );
    // NULL propagates.
    assert_eq!(
        rows(&db, "SELECT ROUND(NULL, 2) AS x FROM one"),
        r1(vec![NULL])
    );
    // A negative scale is rejected.
    assert!(db.execute("SELECT ROUND(2.5, -1) FROM one").is_err());
}

/// A DECIMAL column survives an engine close + reopen (WAL replay and the
/// checkpointed `.rdat` snapshot both round-trip the cell codec).
#[test]
fn decimal_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = SqlEngine::open(dir.path()).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, p DECIMAL)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, '19.90'), (2, '-0.010'), (3, 42)")
            .unwrap();
        db.checkpoint().unwrap(); // force the rows through the .rdat snapshot
        db.execute("INSERT INTO t VALUES (4, '3.14159')").unwrap(); // post-checkpoint, WAL only
    }
    let db = SqlEngine::open(dir.path()).unwrap();
    assert_eq!(
        rows(&db, "SELECT p FROM t ORDER BY id"),
        vec![
            vec![dec("19.90")], // trailing-zero scale preserved
            vec![dec("-0.010")],
            vec![dec("42")],
            vec![dec("3.14159")],
        ]
    );
    // And SUM over the reloaded rows is still exact (aligned to the max scale,
    // 5, of the summed values).
    assert_eq!(rows(&db, "SELECT SUM(p) FROM t"), r1(vec![dec("65.03159")]));
}

/// The JSON wire renders DECIMAL as a clean numeric value with type "DECIMAL".
#[test]
fn decimal_json_wire() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, p DECIMAL)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 9.99), (2, 9.99), (3, 9.99)")
        .unwrap();
    let out =
        oxidb_sql::json::execute_json(&db, "SELECT SUM(p) AS total FROM t", None, false).unwrap();
    let stmt = &out.as_array().unwrap()[0];
    assert_eq!(stmt["types"][0], "DECIMAL");
    // 29.97 is float-representable, so it displays cleanly as a JSON number.
    assert_eq!(stmt["rows"][0][0], serde_json::json!(29.97));
}
