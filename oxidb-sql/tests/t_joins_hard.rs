//! Hard multi-join scenarios: 5- and 6-table join chains mixing INNER, LEFT,
//! RIGHT, and FULL joins with GROUP BY / HAVING / aggregates.
//!
//! Schema (a small e-commerce model):
//!   regions(id, name)                 — 3: North, South, West(no customers)
//!   suppliers(id, name)               — 3: Acme, Global, Orphan(no products)
//!   customers(id, name, region_id)    — ada/bob in North, cy in South, dan(no region)
//!   products(id, name, price, supplier_id)
//!                                     — Apple/Banana(Acme), Cherry/Elderberry(Global),
//!                                       Date(no supplier); Elderberry never sold
//!   orders(id, customer_id, total)    — cy has no orders; order 14 has no customer
//!   items(id, order_id, product_id, qty)
//!
//! Every expected result below is computed by hand from this fixed dataset.

mod common;
use common::*;
use oxidb_sql::SqlEngine;

fn seed(db: &SqlEngine) {
    db.execute("CREATE TABLE regions   (id INT PRIMARY KEY, name TEXT NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE suppliers (id INT PRIMARY KEY, name TEXT NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE customers (id INT PRIMARY KEY, name TEXT NOT NULL, region_id INT)")
        .unwrap();
    db.execute("CREATE TABLE products  (id INT PRIMARY KEY, name TEXT NOT NULL, price INT NOT NULL, supplier_id INT)").unwrap();
    db.execute("CREATE TABLE orders    (id INT PRIMARY KEY, customer_id INT NOT NULL, total INT)")
        .unwrap();
    db.execute(
        "CREATE TABLE items     (id INT PRIMARY KEY, order_id INT, product_id INT, qty INT)",
    )
    .unwrap();

    db.execute("INSERT INTO regions VALUES (1,'North'),(2,'South'),(3,'West')")
        .unwrap();
    db.execute("INSERT INTO suppliers VALUES (1,'Acme'),(2,'Global'),(3,'Orphan')")
        .unwrap();
    db.execute("INSERT INTO customers VALUES (1,'ada',1),(2,'bob',1),(3,'cy',2),(4,'dan',NULL)")
        .unwrap();
    db.execute(
        "INSERT INTO products VALUES \
         (100,'Apple',3,1),(101,'Banana',2,1),(102,'Cherry',5,2),(103,'Date',4,NULL),(104,'Elderberry',6,2)",
    ).unwrap();
    db.execute("INSERT INTO orders VALUES (10,1,100),(11,1,50),(12,2,200),(13,4,30),(14,99,15)")
        .unwrap();
    db.execute(
        "INSERT INTO items VALUES \
         (1000,10,100,2),(1001,10,101,1),(1002,11,100,3),(1003,12,102,5),(1004,13,101,1),(1005,14,103,1)",
    ).unwrap();
}

/// Scenario 1 — 5-table INNER chain: revenue per region.
/// Only customers with a region, their orders, items, and products participate.
/// North = ada(6+2+9=17) + bob(25) = 42; South (cy) has no orders -> absent.
#[test]
fn s1_five_table_inner_revenue_by_region() {
    let (_d, db) = open();
    seed(&db);
    let rws = rows(
        &db,
        "SELECT r.name, SUM(it.qty * p.price) AS rev \
         FROM regions r \
         JOIN customers c ON c.region_id = r.id \
         JOIN orders o    ON o.customer_id = c.id \
         JOIN items it    ON it.order_id = o.id \
         JOIN products p  ON p.id = it.product_id \
         GROUP BY r.name ORDER BY r.name",
    );
    assert_eq!(rws, r1(vec![t("North"), i(42)]));
}

/// Scenario 2 — LEFT chain region -> customers -> orders: order count per region.
/// North=3, South=0 (cy, no orders), West=0 (no customers). dan(no region) absent.
#[test]
fn s2_left_chain_order_count_per_region() {
    let (_d, db) = open();
    seed(&db);
    let rws = rows(
        &db,
        "SELECT r.name, COUNT(o.id) AS n \
         FROM regions r \
         LEFT JOIN customers c ON c.region_id = r.id \
         LEFT JOIN orders o    ON o.customer_id = c.id \
         GROUP BY r.name ORDER BY r.name",
    );
    assert_eq!(
        rws,
        vec![
            vec![t("North"), i(3)],
            vec![t("South"), i(0)],
            vec![t("West"), i(0)],
        ]
    );
}

/// Scenario 3 — 6-table LEFT chain supplier -> products -> items: qty sold per supplier.
/// Acme=7, Global=5 (Cherry sold 5; Elderberry never sold -> NULL ignored),
/// Orphan=NULL (no products at all).
#[test]
fn s3_left_chain_qty_per_supplier() {
    let (_d, db) = open();
    seed(&db);
    let rws = rows(
        &db,
        "SELECT s.name, SUM(it.qty) AS sold \
         FROM suppliers s \
         LEFT JOIN products p ON p.supplier_id = s.id \
         LEFT JOIN items it   ON it.product_id = p.id \
         GROUP BY s.name ORDER BY s.name",
    );
    assert_eq!(
        rws,
        vec![
            vec![t("Acme"), i(7)],
            vec![t("Global"), i(5)],
            vec![t("Orphan"), NULL],
        ]
    );
}

/// Scenario 4 — FULL join customers <-> orders: matched + both unmatched sides.
/// 4 matched (ada×2, bob, dan) + cy(left-only) + order14(right-only) = 6.
#[test]
fn s4_full_join_counts_and_unmatched_sides() {
    let (_d, db) = open();
    seed(&db);

    let total = rows(
        &db,
        "SELECT COUNT(*) AS n FROM customers c FULL JOIN orders o ON c.id = o.customer_id",
    );
    assert_eq!(total, r1(vec![i(6)]));

    // Right-only: an order whose customer is missing.
    let orphan_order = rows(
        &db,
        "SELECT o.id FROM customers c FULL JOIN orders o ON c.id = o.customer_id \
         WHERE c.name IS NULL",
    );
    assert_eq!(orphan_order, r1(vec![i(14)]));

    // Left-only: a customer with no orders.
    let idle_customer = rows(
        &db,
        "SELECT c.name FROM customers c FULL JOIN orders o ON c.id = o.customer_id \
         WHERE o.id IS NULL",
    );
    assert_eq!(idle_customer, r1(vec![t("cy")]));
}

/// Scenario 5 — 4-table INNER+LEFT with GROUP BY / HAVING / ORDER BY:
/// spend per customer (region-bound), keep spend>15, highest first.
/// ada=17, bob=25 -> [bob, ada].
#[test]
fn s5_inner_left_group_having_order() {
    let (_d, db) = open();
    seed(&db);
    let rws = rows(
        &db,
        "SELECT c.name, SUM(it.qty * p.price) AS spend \
         FROM regions r \
         JOIN customers c ON c.region_id = r.id \
         JOIN orders o    ON o.customer_id = c.id \
         LEFT JOIN items it   ON it.order_id = o.id \
         LEFT JOIN products p ON p.id = it.product_id \
         GROUP BY c.name HAVING SUM(it.qty * p.price) > 15 ORDER BY spend DESC",
    );
    assert_eq!(rws, vec![vec![t("bob"), i(25)], vec![t("ada"), i(17)]]);
}

/// Scenario 6 — RIGHT join items -> products keeps unsold products.
/// Apple 2, Banana 2, Cherry 1, Date 1, Elderberry 0 (never sold).
#[test]
fn s6_right_join_keeps_unsold_products() {
    let (_d, db) = open();
    seed(&db);
    let rws = rows(
        &db,
        "SELECT p.name, COUNT(it.id) AS sales \
         FROM items it RIGHT JOIN products p ON it.product_id = p.id \
         GROUP BY p.name ORDER BY p.name",
    );
    assert_eq!(
        rws,
        vec![
            vec![t("Apple"), i(2)],
            vec![t("Banana"), i(2)],
            vec![t("Cherry"), i(1)],
            vec![t("Date"), i(1)],
            vec![t("Elderberry"), i(0)],
        ]
    );
}

/// Scenario 7 — full 6-table INNER chain: revenue per supplier (region-bound sales).
/// Only ada+bob sales count: Acme(Apple 15 + Banana 2 = 17), Global(Cherry 25).
#[test]
fn s7_six_table_inner_revenue_per_supplier() {
    let (_d, db) = open();
    seed(&db);
    let rws = rows(
        &db,
        "SELECT s.name, SUM(it.qty * p.price) AS rev \
         FROM regions r \
         JOIN customers c ON c.region_id = r.id \
         JOIN orders o    ON o.customer_id = c.id \
         JOIN items it    ON it.order_id = o.id \
         JOIN products p  ON p.id = it.product_id \
         JOIN suppliers s ON s.id = p.supplier_id \
         GROUP BY s.name ORDER BY s.name",
    );
    assert_eq!(rws, vec![vec![t("Acme"), i(17)], vec![t("Global"), i(25)]]);
}

/// Scenario 7b — the same 6-table chain narrowed by a WHERE on the far end.
/// Global supplies Cherry; the only Cherry sale (region-bound) is qty 5.
#[test]
fn s7b_six_table_inner_filtered_single_path() {
    let (_d, db) = open();
    seed(&db);
    let rws = rows(
        &db,
        "SELECT p.name, it.qty \
         FROM regions r \
         JOIN customers c ON c.region_id = r.id \
         JOIN orders o    ON o.customer_id = c.id \
         JOIN items it    ON it.order_id = o.id \
         JOIN products p  ON p.id = it.product_id \
         JOIN suppliers s ON s.id = p.supplier_id \
         WHERE s.name = 'Global'",
    );
    assert_eq!(rws, r1(vec![t("Cherry"), i(5)]));
}

/// Scenario 8 — mixed RIGHT + LEFT: keep every customer (even region-less),
/// count their orders. dan has no region (NULL) but one order.
#[test]
fn s8_mixed_right_then_left() {
    let (_d, db) = open();
    seed(&db);
    let rws = rows(
        &db,
        "SELECT c.name, r.name AS region, COUNT(o.id) AS n \
         FROM regions r \
         RIGHT JOIN customers c ON c.region_id = r.id \
         LEFT JOIN orders o     ON o.customer_id = c.id \
         GROUP BY c.name, r.name ORDER BY c.name",
    );
    assert_eq!(
        rws,
        vec![
            vec![t("ada"), t("North"), i(2)],
            vec![t("bob"), t("North"), i(1)],
            vec![t("cy"), t("South"), i(0)],
            vec![t("dan"), NULL, i(1)],
        ]
    );
}

/// Scenario 9 — 5-table chain with a mid-chain FULL join, then filter to the
/// right-only branch: the order (14) whose customer is missing, joined out to
/// its item and product. Confirms NULLs from a FULL join propagate through
/// subsequent INNER joins on the non-null side.
#[test]
fn s9_full_then_inner_on_orphan_branch() {
    let (_d, db) = open();
    seed(&db);
    // customers FULL JOIN orders -> order 14 is right-only (c.* NULL);
    // it still has item 1005 (product Date) which we reach via INNER joins.
    let rws = rows(
        &db,
        "SELECT o.id, p.name, it.qty \
         FROM customers c \
         FULL JOIN orders o  ON c.id = o.customer_id \
         JOIN items it       ON it.order_id = o.id \
         JOIN products p     ON p.id = it.product_id \
         WHERE c.id IS NULL",
    );
    assert_eq!(rws, r1(vec![i(14), t("Date"), i(1)]));
}
