//! Correlated subqueries inside a window's PARTITION BY / ORDER BY.
//!
//! EF Core renders `GroupBy(x).Select(g => g.OrderBy(k).First())`
//! (argmax-per-group) as `ROW_NUMBER() OVER(PARTITION BY x ORDER BY
//! (<correlated subquery>) DESC)`, then filters `row = 1`. The window's ORDER BY
//! key is a per-outer-row correlated aggregate, so the window evaluator must
//! resolve it against the current row — this pins that.

mod common;

use common::*;

fn seed() -> (tempfile::TempDir, oxidb_sql::SqlEngine) {
    let (dir, db) = open();
    db.execute("CREATE TABLE customers (id INT, city TEXT, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE orders (cust_id INT, amount DOUBLE)")
        .unwrap();
    for (id, city, name) in [
        (1, "NYC", "Alice"),
        (2, "NYC", "Bob"),
        (3, "LA", "Carol"),
        (4, "LA", "Dave"),
    ] {
        db.execute(&format!(
            "INSERT INTO customers VALUES ({id}, '{city}', '{name}')"
        ))
        .unwrap();
    }
    // Spend per customer: Alice 150, Bob 200, Carol 300, Dave 100.
    for (cust, amt) in [(1, 100.0), (1, 50.0), (2, 200.0), (3, 300.0), (4, 100.0)] {
        db.execute(&format!("INSERT INTO orders VALUES ({cust}, {amt})"))
            .unwrap();
    }
    (dir, db)
}

/// The window's ORDER BY is a correlated SUM subquery; per city, row 1 must be
/// the top spender: NYC → Bob (200), LA → Carol (300).
#[test]
fn correlated_subquery_in_window_order_by() {
    let (_d, db) = seed();
    let sql = "
        SELECT name, city, rn FROM (
            SELECT c.name, c.city,
                ROW_NUMBER() OVER(
                    PARTITION BY c.city
                    ORDER BY (SELECT SUM(o.amount) FROM orders o WHERE o.cust_id = c.id) DESC
                ) AS rn
            FROM customers c
        ) t
        WHERE rn = 1
        ORDER BY city";
    let got = rows(&db, sql);
    assert_eq!(
        got,
        vec![
            vec![t("Carol"), t("LA"), i(1)],
            vec![t("Bob"), t("NYC"), i(1)],
        ],
        "argmax-per-city via correlated window ORDER BY"
    );
}

/// The full EF-shaped rendering (COALESCE'd correlated subquery both projected
/// and used as the window key, wrapped in derived tables + a self-join on the
/// group key) must produce one top spender per city.
#[test]
fn ef_shaped_argmax_per_group() {
    let (_d, db) = seed();
    let sql = "
        SELECT c1.city, c3.name, c3.spend
        FROM (SELECT city FROM customers GROUP BY city) c1
        LEFT JOIN (
            SELECT c2.city, c2.name, c2.spend FROM (
                SELECT c0.city, c0.name,
                    (SELECT COALESCE(SUM(o0.amount), 0.0) FROM orders o0 WHERE c0.id = o0.cust_id) AS spend,
                    ROW_NUMBER() OVER(
                        PARTITION BY c0.city
                        ORDER BY (SELECT COALESCE(SUM(o.amount), 0.0) FROM orders o WHERE c0.id = o.cust_id) DESC
                    ) AS row
                FROM customers c0
            ) c2
            WHERE c2.row <= 1
        ) c3 ON c1.city = c3.city
        ORDER BY c1.city";
    let got = rows(&db, sql);
    assert_eq!(
        got,
        vec![
            vec![t("LA"), t("Carol"), d(300.0)],
            vec![t("NYC"), t("Bob"), d(200.0)],
        ],
        "EF argmax-per-group shape"
    );
}
