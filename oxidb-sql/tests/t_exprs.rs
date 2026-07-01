//! Expressions: comparisons, arithmetic, boolean logic, NULL / three-valued
//! logic, IS NULL, ordering semantics.

mod common;
use common::*;

fn nums(db: &oxidb_sql::SqlEngine) {
    db.execute("CREATE TABLE n (id INT, v INT, f DOUBLE, s TEXT, flag BOOL)")
        .unwrap();
    db.execute(
        "INSERT INTO n VALUES \
         (1, 10, 1.5, 'apple', true), \
         (2, 20, 2.5, 'banana', false), \
         (3, 30, 3.5, 'cherry', true)",
    )
    .unwrap();
}

#[test]
fn integer_comparisons() {
    let (_d, db) = open();
    nums(&db);
    assert_eq!(rows(&db, "SELECT id FROM n WHERE v = 20"), r1(vec![i(2)]));
    assert_eq!(
        rows(&db, "SELECT id FROM n WHERE v != 20 ORDER BY id"),
        vec![vec![i(1)], vec![i(3)]]
    );
    assert_eq!(rows(&db, "SELECT id FROM n WHERE v > 25"), r1(vec![i(3)]));
    assert_eq!(
        rows(&db, "SELECT id FROM n WHERE v >= 20 ORDER BY id"),
        vec![vec![i(2)], vec![i(3)]]
    );
    assert_eq!(rows(&db, "SELECT id FROM n WHERE v < 20"), r1(vec![i(1)]));
    assert_eq!(
        rows(&db, "SELECT id FROM n WHERE v <= 20 ORDER BY id"),
        vec![vec![i(1)], vec![i(2)]]
    );
}

#[test]
fn text_comparisons_and_ordering() {
    let (_d, db) = open();
    nums(&db);
    assert_eq!(
        rows(&db, "SELECT id FROM n WHERE s = 'banana'"),
        r1(vec![i(2)])
    );
    assert_eq!(
        rows(&db, "SELECT id FROM n WHERE s < 'c' ORDER BY id"),
        vec![vec![i(1)], vec![i(2)]]
    );
    assert_eq!(
        rows(&db, "SELECT s FROM n ORDER BY s DESC"),
        vec![vec![t("cherry")], vec![t("banana")], vec![t("apple")]]
    );
}

#[test]
fn cross_numeric_comparison_int_vs_double() {
    let (_d, db) = open();
    nums(&db);
    // f is DOUBLE; compare against an integer literal.
    assert_eq!(
        rows(&db, "SELECT id FROM n WHERE f > 2"),
        vec![vec![i(2)], vec![i(3)]]
    );
    assert_eq!(
        rows(&db, "SELECT id FROM n WHERE v > 2.5 ORDER BY id").len(),
        3
    );
}

#[test]
fn integer_arithmetic() {
    let (_d, db) = open();
    nums(&db);
    assert_eq!(
        rows(&db, "SELECT v + 5 AS x FROM n WHERE id = 1"),
        r1(vec![i(15)])
    );
    assert_eq!(
        rows(&db, "SELECT v - 5 AS x FROM n WHERE id = 1"),
        r1(vec![i(5)])
    );
    assert_eq!(
        rows(&db, "SELECT v * 3 AS x FROM n WHERE id = 1"),
        r1(vec![i(30)])
    );
    assert_eq!(
        rows(&db, "SELECT v / 3 AS x FROM n WHERE id = 1"),
        r1(vec![i(3)])
    ); // integer division truncates
}

#[test]
fn float_arithmetic_and_promotion() {
    let (_d, db) = open();
    nums(&db);
    // int * double -> double
    assert_eq!(
        rows(&db, "SELECT v * 1.5 AS x FROM n WHERE id = 1"),
        r1(vec![d(15.0)])
    );
    assert_eq!(
        rows(&db, "SELECT f + f AS x FROM n WHERE id = 1"),
        r1(vec![d(3.0)])
    );
}

#[test]
fn unary_minus_and_not() {
    let (_d, db) = open();
    nums(&db);
    assert_eq!(
        rows(&db, "SELECT -v AS x FROM n WHERE id = 1"),
        r1(vec![i(-10)])
    );
    assert_eq!(
        rows(&db, "SELECT id FROM n WHERE NOT flag ORDER BY id"),
        r1(vec![i(2)])
    );
}

#[test]
fn bare_boolean_column_as_predicate() {
    let (_d, db) = open();
    nums(&db);
    assert_eq!(
        rows(&db, "SELECT id FROM n WHERE flag ORDER BY id"),
        vec![vec![i(1)], vec![i(3)]]
    );
}

#[test]
fn and_or_precedence() {
    let (_d, db) = open();
    nums(&db);
    // v > 5 AND (id = 1 OR id = 3)
    assert_eq!(
        rows(
            &db,
            "SELECT id FROM n WHERE v > 5 AND (id = 1 OR id = 3) ORDER BY id"
        ),
        vec![vec![i(1)], vec![i(3)]]
    );
    assert_eq!(
        rows(&db, "SELECT id FROM n WHERE id = 1 OR id = 2 ORDER BY id"),
        vec![vec![i(1)], vec![i(2)]]
    );
}

#[test]
fn null_comparisons_are_unknown() {
    let (_d, db) = open();
    db.execute("CREATE TABLE m (id INT, v INT)").unwrap();
    db.execute("INSERT INTO m VALUES (1, 10), (2, NULL), (3, 30)")
        .unwrap();
    // NULL compared with anything is unknown -> row excluded.
    assert_eq!(rows(&db, "SELECT id FROM m WHERE v = 10"), r1(vec![i(1)]));
    assert_eq!(
        rows(&db, "SELECT id FROM m WHERE v != 10 ORDER BY id"),
        r1(vec![i(3)])
    );
    assert_eq!(
        rows(&db, "SELECT id FROM m WHERE v > 0 ORDER BY id"),
        vec![vec![i(1)], vec![i(3)]]
    );
}

#[test]
fn is_null_and_is_not_null() {
    let (_d, db) = open();
    db.execute("CREATE TABLE m (id INT, v INT)").unwrap();
    db.execute("INSERT INTO m VALUES (1, 10), (2, NULL), (3, NULL)")
        .unwrap();
    assert_eq!(
        rows(&db, "SELECT id FROM m WHERE v IS NULL ORDER BY id"),
        vec![vec![i(2)], vec![i(3)]]
    );
    assert_eq!(
        rows(&db, "SELECT id FROM m WHERE v IS NOT NULL"),
        r1(vec![i(1)])
    );
}

#[test]
fn three_valued_and_or() {
    let (_d, db) = open();
    db.execute("CREATE TABLE m (id INT, v INT)").unwrap();
    db.execute("INSERT INTO m VALUES (1, NULL)").unwrap();
    // NULL AND false -> false ; NULL OR true -> true
    assert!(rows(&db, "SELECT id FROM m WHERE v = 1 AND id = 999").is_empty());
    assert_eq!(
        rows(&db, "SELECT id FROM m WHERE v = 1 OR id = 1"),
        r1(vec![i(1)])
    );
}

#[test]
fn arithmetic_with_null_yields_null() {
    let (_d, db) = open();
    db.execute("CREATE TABLE m (id INT, v INT)").unwrap();
    db.execute("INSERT INTO m VALUES (1, NULL)").unwrap();
    assert_eq!(rows(&db, "SELECT v + 1 AS x FROM m"), r1(vec![NULL]));
}

#[test]
fn order_by_places_nulls_first_ascending() {
    let (_d, db) = open();
    db.execute("CREATE TABLE m (id INT, v INT)").unwrap();
    db.execute("INSERT INTO m VALUES (1, 20), (2, NULL), (3, 10)")
        .unwrap();
    // NULL sorts first ascending, last descending.
    assert_eq!(
        rows(&db, "SELECT id FROM m ORDER BY v ASC"),
        vec![vec![i(2)], vec![i(3)], vec![i(1)]]
    );
    assert_eq!(
        rows(&db, "SELECT id FROM m ORDER BY v DESC"),
        vec![vec![i(1)], vec![i(3)], vec![i(2)]]
    );
}

#[test]
fn multi_key_order_by_mixed_direction() {
    let (_d, db) = open();
    db.execute("CREATE TABLE m (a INT, b INT, id INT)").unwrap();
    db.execute("INSERT INTO m VALUES (1, 2, 10), (1, 1, 11), (2, 1, 12)")
        .unwrap();
    // ORDER BY a ASC, b DESC
    assert_eq!(
        rows(&db, "SELECT id FROM m ORDER BY a ASC, b DESC"),
        vec![vec![i(10)], vec![i(11)], vec![i(12)]]
    );
}

#[test]
fn division_by_zero_is_error() {
    let (_d, db) = open();
    nums(&db);
    assert!(db.execute("SELECT v / 0 AS x FROM n").is_err());
    assert!(db.execute("UPDATE n SET v = v / 0").is_err());
}

#[test]
fn comparing_incompatible_types_is_error() {
    let (_d, db) = open();
    nums(&db);
    // text > int has no ordering -> evaluation error (rows present).
    assert!(db.execute("SELECT id FROM n WHERE s > 5").is_err());
}
