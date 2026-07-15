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
    // A fractional literal (1.5) is now an exact DECIMAL, so int * decimal ->
    // decimal (15.0), not a lossy Double.
    assert_eq!(
        rows(&db, "SELECT v * 1.5 AS x FROM n WHERE id = 1"),
        r1(vec![oxidb_sql::Value::Decimal(Box::new(
            oxidb_sql::Decimal::parse("15.0").unwrap()
        ))])
    );
    // Forcing the literal to DOUBLE keeps the old float-promotion path.
    assert_eq!(
        rows(
            &db,
            "SELECT v * CAST(1.5 AS DOUBLE) AS x FROM n WHERE id = 1"
        ),
        r1(vec![d(15.0)])
    );
    // A DOUBLE column stays on the float path.
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

// ── COALESCE / IFNULL / NULLIF ──────────────────────────────────────────────

use oxidb_sql::Value;

#[test]
fn coalesce_basics() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, s TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL, 7, NULL), (2, 5, NULL, 'x'), (3, NULL, NULL, NULL)")
        .unwrap();

    assert_eq!(
        rows(&db, "SELECT COALESCE(a, b, 0) FROM t ORDER BY id"),
        vec![
            vec![Value::Int(7)],
            vec![Value::Int(5)],
            vec![Value::Int(0)]
        ]
    );
    // IFNULL is the two-argument spelling; text works too.
    assert_eq!(
        rows(&db, "SELECT IFNULL(s, 'yok') FROM t ORDER BY id"),
        vec![
            vec![Value::Text("yok".into())],
            vec![Value::Text("x".into())],
            vec![Value::Text("yok".into())],
        ]
    );
    // In WHERE, with a bind parameter as the fallback.
    assert_eq!(
        rows_p(
            &db,
            "SELECT id FROM t WHERE COALESCE(a, ?) > 4 ORDER BY id",
            &[Value::Int(99)]
        ),
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)]
        ]
    );
}

#[test]
fn coalesce_with_aggregates_and_joins() {
    let (_d, db) = open();
    db.execute("CREATE TABLE k (id INT PRIMARY KEY, grp TEXT)")
        .unwrap();
    db.execute("CREATE TABLE v (id INT PRIMARY KEY, k_id INT, amt INT)")
        .unwrap();
    db.execute("INSERT INTO k VALUES (1, 'a'), (2, 'b')")
        .unwrap();
    db.execute("INSERT INTO v VALUES (1, 1, 10), (2, 1, 5)")
        .unwrap();

    // LEFT JOIN NULL padding is the classic COALESCE use.
    assert_eq!(
        rows(
            &db,
            "SELECT k.grp, COALESCE(SUM(v.amt), 0) AS toplam \
             FROM k LEFT JOIN v ON v.k_id = k.id \
             GROUP BY k.grp ORDER BY k.grp"
        ),
        vec![
            vec![Value::Text("a".into()), Value::Int(15)],
            vec![Value::Text("b".into()), Value::Int(0)],
        ]
    );
}

#[test]
fn nullif_basics_and_arity_errors() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 3, 3), (2, 3, 4)")
        .unwrap();

    assert_eq!(
        rows(&db, "SELECT NULLIF(a, b) FROM t ORDER BY id"),
        vec![vec![Value::Null], vec![Value::Int(3)]]
    );
    // Division-by-zero guard, the classic NULLIF use.
    assert_eq!(
        rows(&db, "SELECT a / NULLIF(a - b, 0) FROM t ORDER BY id"),
        vec![vec![Value::Null], vec![Value::Int(-3)]]
    );

    assert!(db.execute("SELECT NULLIF(a) FROM t").is_err());
    assert!(db.execute("SELECT IFNULL(a, b, 0) FROM t").is_err());
    assert!(db.execute("SELECT COALESCE() FROM t").is_err());
}

/// Sabit alt ifadeler (EF Core'un satır içine gömdüğü `LENGTH('...')`,
/// `x * 60000` gibi) bağlama zamanında bir kez katlanır — sonuç, satır başına
/// hesaplamayla aynı olmalı; kolonlu ifadeler ise değişmeden kalır.
#[test]
fn constant_folding_preserves_semantics() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, ad TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'Customer 000007'), (2, 'baska')")
        .unwrap();

    // LENGTH('Customer 00') = 11 sabiti; SUBSTRING kolon üzerinde çalışmaya
    // devam eder. Yalnızca 'Customer 00' ile başlayan ad eşleşir.
    assert_eq!(
        rows(&db, "SELECT id FROM t WHERE SUBSTRING(ad, 1, LENGTH('Customer 00')) = 'Customer 00'"),
        vec![vec![i(1)]]
    );
    // Aritmetik sabit katlama: 2 * 60000 + 1 = 120001.
    assert_eq!(
        rows(&db, "SELECT 2 * 60000 + 1 FROM t WHERE id = 1"),
        vec![vec![i(120001)]]
    );
    // Sabit NULL yayılımı korunur.
    assert_eq!(
        rows(&db, "SELECT COALESCE(NULL, LENGTH('abc'), 99) FROM t WHERE id = 1"),
        vec![vec![i(3)]]
    );
    // Kolon içeren ifade katlanmaz ama doğru sonucu verir.
    assert_eq!(
        rows(&db, "SELECT LENGTH(ad) FROM t WHERE id = 2"),
        vec![vec![i(5)]]
    );
}
