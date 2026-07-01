//! Joins: INNER, LEFT, RIGHT, FULL OUTER, self-join, 3-way, join + aggregate.

mod common;
use common::*;

fn seed(db: &oxidb_sql::SqlEngine) {
    db.execute("CREATE TABLE c (id INT, name TEXT)").unwrap();
    db.execute("CREATE TABLE o (id INT, cust INT, amt INT)")
        .unwrap();
    // cy(3) has no orders; order 12 (cust 9) has no matching customer.
    db.execute("INSERT INTO c VALUES (1,'ada'),(2,'bob'),(3,'cy')")
        .unwrap();
    db.execute("INSERT INTO o VALUES (10,1,100),(11,2,200),(12,9,50)")
        .unwrap();
}

#[test]
fn inner_join_matches_only() {
    let (_d, db) = open();
    seed(&db);
    let rws = rows(
        &db,
        "SELECT c.name, o.amt FROM c JOIN o ON c.id = o.cust ORDER BY o.amt",
    );
    assert_eq!(rws, vec![vec![t("ada"), i(100)], vec![t("bob"), i(200)]]);
}

#[test]
fn left_join_keeps_unmatched_left_with_nulls() {
    let (_d, db) = open();
    seed(&db);
    let rws = rows(
        &db,
        "SELECT c.name, o.amt FROM c LEFT JOIN o ON c.id = o.cust ORDER BY c.name",
    );
    // ada, bob matched; cy padded with NULL.
    assert_eq!(
        rws,
        vec![
            vec![t("ada"), i(100)],
            vec![t("bob"), i(200)],
            vec![t("cy"), NULL],
        ]
    );
}

#[test]
fn left_join_where_on_right_null_finds_unmatched() {
    let (_d, db) = open();
    seed(&db);
    // Customers with no orders.
    let rws = rows(
        &db,
        "SELECT c.name FROM c LEFT JOIN o ON c.id = o.cust WHERE o.id IS NULL",
    );
    assert_eq!(rws, r1(vec![t("cy")]));
}

#[test]
fn right_join_keeps_unmatched_right_with_nulls() {
    let (_d, db) = open();
    seed(&db);
    let rws = rows(
        &db,
        "SELECT c.name, o.id FROM c RIGHT JOIN o ON c.id = o.cust WHERE o.id = 12",
    );
    // Order 12 has no customer -> c.name NULL.
    assert_eq!(rws, r1(vec![NULL, i(12)]));
}

#[test]
fn full_join_keeps_both_sides() {
    let (_d, db) = open();
    seed(&db);
    let n = rows(&db, "SELECT c.id, o.id FROM c FULL JOIN o ON c.id = o.cust").len();
    // 2 matched + cy (left-only) + order12 (right-only) = 4 rows.
    assert_eq!(n, 4);
}

#[test]
fn self_join() {
    let (_d, db) = open();
    db.execute("CREATE TABLE n (id INT)").unwrap();
    db.execute("INSERT INTO n VALUES (1),(2),(3)").unwrap();
    let rws = rows(
        &db,
        "SELECT x.id, y.id FROM n x JOIN n y ON x.id < y.id ORDER BY x.id, y.id",
    );
    assert_eq!(
        rws,
        vec![vec![i(1), i(2)], vec![i(1), i(3)], vec![i(2), i(3)],]
    );
}

#[test]
fn three_table_join() {
    let (_d, db) = open();
    db.execute("CREATE TABLE a (id INT, x INT)").unwrap();
    db.execute("CREATE TABLE b (id INT, y INT)").unwrap();
    db.execute("CREATE TABLE d (id INT, z INT)").unwrap();
    db.execute("INSERT INTO a VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO b VALUES (1, 20)").unwrap();
    db.execute("INSERT INTO d VALUES (1, 30)").unwrap();
    let rws = rows(
        &db,
        "SELECT a.x, b.y, d.z FROM a JOIN b ON a.id = b.id JOIN d ON b.id = d.id",
    );
    assert_eq!(rws, r1(vec![i(10), i(20), i(30)]));
}

#[test]
fn join_with_group_by_and_count_ignores_null() {
    let (_d, db) = open();
    seed(&db);
    // LEFT JOIN so cy appears with COUNT(o.id) = 0.
    let rws = rows(
        &db,
        "SELECT c.name, COUNT(o.id) AS n FROM c LEFT JOIN o ON c.id = o.cust \
         GROUP BY c.name ORDER BY c.name",
    );
    assert_eq!(
        rws,
        vec![
            vec![t("ada"), i(1)],
            vec![t("bob"), i(1)],
            vec![t("cy"), i(0)],
        ]
    );
}

#[test]
fn join_with_where_filter() {
    let (_d, db) = open();
    seed(&db);
    let rws = rows(
        &db,
        "SELECT c.name FROM c JOIN o ON c.id = o.cust WHERE o.amt >= 200",
    );
    assert_eq!(rws, r1(vec![t("bob")]));
}

#[test]
fn cross_join_style_unsupported_without_on() {
    let (_d, db) = open();
    seed(&db);
    // CROSS JOIN (no ON) is not supported -> clean error, not a panic.
    assert!(db.execute("SELECT c.id FROM c CROSS JOIN o").is_err());
}
