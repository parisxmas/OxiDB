//! Decorrelation of a correlated aggregate subquery whose body has JOINs.
//!
//! EF Core renders `GroupBy(cat).Select(g => g.Sum(x => x.Qty * x.Price))`-style
//! analytics as an outer GROUP BY with a correlated subquery per group that
//! re-runs a full join+aggregate. Decorrelating it (one grouped pass, keyed
//! lookup) turns N re-scans into one — and must produce identical results.

mod common;

use common::*;

fn seed() -> (tempfile::TempDir, oxidb_sql::SqlEngine) {
    let (dir, db) = open();
    db.execute("CREATE TABLE cat (id INT, name TEXT)").unwrap();
    db.execute("CREATE TABLE line (cat_id INT, product_id INT, qty INT)")
        .unwrap();
    db.execute("CREATE TABLE product (id INT, price DOUBLE)")
        .unwrap();
    for (id, name) in [(1, "A"), (2, "B"), (3, "C")] {
        db.execute(&format!("INSERT INTO cat VALUES ({id}, '{name}')"))
            .unwrap();
    }
    for (id, price) in [(10, 10.0), (20, 100.0)] {
        db.execute(&format!("INSERT INTO product VALUES ({id}, {price})"))
            .unwrap();
    }
    // A: 2*10 + 3*10 = 50 ; B: 1*100 = 100 ; C: none.
    for (cat, prod, qty) in [(1, 10, 2), (1, 10, 3), (2, 20, 1)] {
        db.execute(&format!(
            "INSERT INTO line VALUES ({cat}, {prod}, {qty})"
        ))
        .unwrap();
    }
    (dir, db)
}

/// Correlated SUM over a join (line ⋈ product), per category. C (no lines)
/// must yield the COALESCE default (0), matching row-by-row correlated
/// semantics.
#[test]
fn correlated_join_aggregate_decorrelates_correctly() {
    let (_d, db) = seed();
    let sql = "
        SELECT c.name,
            (SELECT COALESCE(SUM(l.qty * p.price), CAST(0 AS DOUBLE))
             FROM line l
             JOIN product p ON l.product_id = p.id
             WHERE l.cat_id = c.id) AS revenue
        FROM cat c
        ORDER BY c.name";
    let got = rows(&db, sql);
    assert_eq!(
        got,
        vec![
            vec![t("A"), d(50.0)],
            vec![t("B"), d(100.0)],
            vec![t("C"), d(0.0)],
        ],
        "per-category revenue via decorrelated joined correlated subquery"
    );
}
