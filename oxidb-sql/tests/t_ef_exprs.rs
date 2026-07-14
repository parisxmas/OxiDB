//! ADR-0013 Phase A: CASE, LIKE, CAST, string scalars, ||, EXISTS,
//! SELECT DISTINCT, and column type metadata.

mod common;

use common::*;
use oxidb_sql::{QueryResult, SqlType, Value};

fn t(s: &str) -> Value {
    Value::Text(s.to_string())
}

fn seed(db: &oxidb_sql::SqlEngine) {
    db.execute("CREATE TABLE p (id INT PRIMARY KEY, ad TEXT, puan INT, oran DOUBLE)")
        .unwrap();
    db.execute(
        "INSERT INTO p VALUES \
         (1, 'ali', 10, 0.5), (2, 'ayse', 25, 1.5), (3, 'ali_can', NULL, 2.5), (4, 'Veli', 25, 3.5)",
    )
    .unwrap();
}

#[test]
fn case_searched_simple_and_no_else() {
    let (_d, db) = open();
    seed(&db);
    assert_eq!(
        rows(
            &db,
            "SELECT CASE WHEN puan >= 25 THEN 'yuksek' WHEN puan >= 10 THEN 'orta' ELSE 'bilinmiyor' END FROM p ORDER BY id"
        ),
        vec![
            vec![t("orta")],
            vec![t("yuksek")],
            vec![t("bilinmiyor")],
            vec![t("yuksek")]
        ]
    );
    // Simple form desugars to equality; no ELSE yields NULL.
    assert_eq!(
        rows(
            &db,
            "SELECT CASE ad WHEN 'ali' THEN 1 WHEN 'ayse' THEN 2 END FROM p ORDER BY id"
        ),
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Null],
            vec![Value::Null]
        ]
    );
    // Short-circuit: the second branch would divide by zero.
    assert_eq!(
        rows(
            &db,
            "SELECT CASE WHEN 1 = 1 THEN 9 ELSE 1 / 0 END FROM p WHERE id = 1"
        ),
        vec![vec![Value::Int(9)]]
    );
}

#[test]
fn like_patterns_escape_and_null() {
    let (_d, db) = open();
    seed(&db);
    assert_eq!(
        rows(&db, "SELECT id FROM p WHERE ad LIKE 'ali%' ORDER BY id"),
        vec![vec![Value::Int(1)], vec![Value::Int(3)]]
    );
    assert_eq!(
        rows(&db, "SELECT id FROM p WHERE ad LIKE 'a_i' ORDER BY id"),
        vec![vec![Value::Int(1)]]
    );
    assert_eq!(
        rows(&db, "SELECT id FROM p WHERE ad NOT LIKE '%a%' ORDER BY id"),
        vec![vec![Value::Int(4)]] // 'Veli' — case-sensitive
    );
    // ESCAPE: match a literal underscore.
    assert_eq!(
        rows(
            &db,
            "SELECT id FROM p WHERE ad LIKE '%!_%' ESCAPE '!' ORDER BY id"
        ),
        vec![vec![Value::Int(3)]]
    );
    // NULL operand -> NULL -> filtered out.
    assert_eq!(
        rows(
            &db,
            "SELECT id FROM p WHERE CAST(puan AS TEXT) LIKE '2%' ORDER BY id"
        ),
        vec![vec![Value::Int(2)], vec![Value::Int(4)]]
    );
}

#[test]
fn cast_matrix() {
    let (_d, db) = open();
    seed(&db);
    assert_eq!(
        rows(
            &db,
            "SELECT CAST(puan AS TEXT), CAST(oran AS INT), CAST('42' AS INT) FROM p WHERE id = 2"
        ),
        vec![vec![t("25"), Value::Int(1), Value::Int(42)]]
    );
    assert_eq!(
        rows(
            &db,
            "SELECT CAST(NULL AS INT), CAST(1 AS BOOL), CAST('true' AS BOOL) FROM p WHERE id = 1"
        ),
        vec![vec![Value::Null, Value::Bool(true), Value::Bool(true)]]
    );
    assert!(db.execute("SELECT CAST('bozuk' AS INT) FROM p").is_err());
}

#[test]
fn string_functions_and_concat() {
    let (_d, db) = open();
    seed(&db);
    assert_eq!(
        rows(
            &db,
            "SELECT UPPER(ad), LENGTH(ad), SUBSTRING(ad, 1, 3), ad || '-' || CAST(id AS TEXT) \
             FROM p WHERE id = 2"
        ),
        vec![vec![t("AYSE"), Value::Int(4), t("ays"), t("ayse-2")]]
    );
    assert_eq!(
        rows(
            &db,
            "SELECT CONCAT(ad, '/', LOWER(ad)), REPLACE(ad, 'a', 'o'), TRIM('  x  '), LTRIM('  x'), RTRIM('x  ') \
             FROM p WHERE id = 1"
        ),
        vec![vec![t("ali/ali"), t("oli"), t("x"), t("x"), t("x")]]
    );
    // NULL propagation: || and CONCAT go NULL.
    assert_eq!(
        rows(
            &db,
            "SELECT ad || CAST(puan AS TEXT), CONCAT(ad, CAST(puan AS TEXT)) FROM p WHERE id = 3"
        ),
        vec![vec![Value::Null, Value::Null]]
    );
    assert_eq!(
        rows(&db, "SELECT ABS(-5), ABS(oran - 3.0) FROM p WHERE id = 1"),
        vec![vec![Value::Int(5), Value::Double(2.5)]]
    );
}

#[test]
fn exists_plain_correlated_and_negated() {
    let (_d, db) = open();
    seed(&db);
    db.execute("CREATE TABLE emir (id INT PRIMARY KEY, p_id INT)")
        .unwrap();
    db.execute("INSERT INTO emir VALUES (1, 1), (2, 1), (3, 4)")
        .unwrap();

    // Plain EXISTS.
    assert_eq!(
        rows(
            &db,
            "SELECT COUNT(*) FROM p WHERE EXISTS (SELECT 1 FROM emir)"
        ),
        vec![vec![Value::Int(4)]]
    );
    // Correlated EXISTS — the EF workhorse.
    assert_eq!(
        rows(
            &db,
            "SELECT ad FROM p WHERE EXISTS (SELECT 1 FROM emir e WHERE e.p_id = p.id) ORDER BY id"
        ),
        vec![vec![t("ali")], vec![t("Veli")]]
    );
    assert_eq!(
        rows(
            &db,
            "SELECT COUNT(*) FROM p WHERE NOT EXISTS (SELECT 1 FROM emir e WHERE e.p_id = p.id)"
        ),
        vec![vec![Value::Int(2)]]
    );
    // Aggregated EXISTS bodies are wrapped in a derived table: the grouped
    // row count drives existence.
    assert_eq!(
        rows(
            &db,
            "SELECT COUNT(*) FROM p WHERE EXISTS (SELECT COUNT(*) FROM emir GROUP BY p_id)"
        ),
        rows(&db, "SELECT COUNT(*) FROM p")
    );
}

#[test]
fn select_distinct() {
    let (_d, db) = open();
    seed(&db);
    assert_eq!(
        rows(&db, "SELECT DISTINCT puan FROM p ORDER BY puan"),
        vec![
            vec![Value::Null],
            vec![Value::Int(10)],
            vec![Value::Int(25)]
        ]
    );
    // DISTINCT before LIMIT.
    assert_eq!(
        rows(&db, "SELECT DISTINCT puan FROM p ORDER BY puan LIMIT 2"),
        vec![vec![Value::Null], vec![Value::Int(10)]]
    );
    // Multi-column distinctness.
    assert_eq!(
        rows(
            &db,
            "SELECT DISTINCT puan, puan IS NULL FROM p ORDER BY puan"
        )
        .len(),
        3
    );
}

#[test]
fn column_type_metadata() {
    let (_d, db) = open();
    seed(&db);
    let r = db
        .execute(
            "SELECT id, ad, oran, puan >= 10, CAST(id AS TEXT), COUNT(*), AVG(oran), UPPER(ad) \
             FROM p GROUP BY id, ad, oran, puan",
        )
        .unwrap()
        .pop()
        .unwrap();
    let QueryResult::Select { types, .. } = r else {
        panic!("expected select");
    };
    assert_eq!(
        types,
        vec![
            Some(SqlType::Int),
            Some(SqlType::Text),
            Some(SqlType::Double),
            Some(SqlType::Bool),
            Some(SqlType::Text),
            Some(SqlType::Int),
            Some(SqlType::Double),
            Some(SqlType::Text),
        ]
    );
    // Params are unknown.
    let r = db
        .execute_params("SELECT ? FROM p WHERE id = 1", &[Value::Int(1)])
        .unwrap()
        .pop()
        .unwrap();
    let QueryResult::Select { types, .. } = r else {
        panic!("expected select");
    };
    assert_eq!(types, vec![None]);
}
