//! Aggregation: COUNT/SUM/AVG/MIN/MAX, NULL handling, GROUP BY, HAVING.

mod common;
use common::*;

fn m(db: &oxidb_sql::SqlEngine) {
    db.execute("CREATE TABLE m (id INT, v INT)").unwrap();
    db.execute("INSERT INTO m VALUES (1,10),(2,NULL),(3,30),(4,NULL),(5,20)")
        .unwrap();
}

#[test]
fn count_star_vs_count_column_ignores_null() {
    let (_d, db) = open();
    m(&db);
    assert_eq!(rows(&db, "SELECT COUNT(*) AS c FROM m"), r1(vec![i(5)]));
    assert_eq!(rows(&db, "SELECT COUNT(v) AS c FROM m"), r1(vec![i(3)]));
}

#[test]
fn sum_min_max_ignore_null() {
    let (_d, db) = open();
    m(&db);
    assert_eq!(rows(&db, "SELECT SUM(v) AS s FROM m"), r1(vec![i(60)]));
    assert_eq!(rows(&db, "SELECT MIN(v) AS lo FROM m"), r1(vec![i(10)]));
    assert_eq!(rows(&db, "SELECT MAX(v) AS hi FROM m"), r1(vec![i(30)]));
}

#[test]
fn avg_is_double() {
    let (_d, db) = open();
    m(&db);
    assert_eq!(rows(&db, "SELECT AVG(v) AS a FROM m"), r1(vec![d(20.0)]));
}

#[test]
fn aggregates_over_empty_set() {
    let (_d, db) = open();
    m(&db);
    // No rows match -> COUNT 0, others NULL, still exactly one output row.
    let rws = rows(
        &db,
        "SELECT COUNT(*) AS c, COUNT(v) AS cv, SUM(v) AS s, AVG(v) AS a, MIN(v) AS lo, MAX(v) AS hi \
         FROM m WHERE id > 1000",
    );
    assert_eq!(rws, r1(vec![i(0), i(0), NULL, NULL, NULL, NULL]));
}

#[test]
fn sum_of_doubles() {
    let (_d, db) = open();
    db.execute("CREATE TABLE f (v DOUBLE)").unwrap();
    db.execute("INSERT INTO f VALUES (1.5),(2.5),(1.0)")
        .unwrap();
    assert_eq!(rows(&db, "SELECT SUM(v) AS s FROM f"), r1(vec![d(5.0)]));
    assert_eq!(
        rows(&db, "SELECT AVG(v) AS a FROM f"),
        r1(vec![d(5.0 / 3.0)])
    );
}

#[test]
fn min_max_over_text() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES ('banana'),('apple'),('cherry')")
        .unwrap();
    assert_eq!(
        rows(&db, "SELECT MIN(s) AS lo FROM t"),
        r1(vec![t("apple")])
    );
    assert_eq!(
        rows(&db, "SELECT MAX(s) AS hi FROM t"),
        r1(vec![t("cherry")])
    );
}

#[test]
fn group_by_single_column() {
    let (_d, db) = open();
    db.execute("CREATE TABLE o (cust INT, amt INT)").unwrap();
    db.execute("INSERT INTO o VALUES (1,100),(1,50),(2,200),(2,25),(2,75)")
        .unwrap();
    assert_eq!(
        rows(
            &db,
            "SELECT cust, SUM(amt) AS s FROM o GROUP BY cust ORDER BY cust"
        ),
        vec![vec![i(1), i(150)], vec![i(2), i(300)]]
    );
}

#[test]
fn group_by_multiple_columns() {
    let (_d, db) = open();
    db.execute("CREATE TABLE s (a INT, b INT, c INT)").unwrap();
    db.execute("INSERT INTO s VALUES (1,1,10),(1,1,20),(1,2,30),(2,1,40)")
        .unwrap();
    assert_eq!(
        rows(
            &db,
            "SELECT a, b, SUM(c) AS s FROM s GROUP BY a, b ORDER BY a, b"
        ),
        vec![
            vec![i(1), i(1), i(30)],
            vec![i(1), i(2), i(30)],
            vec![i(2), i(1), i(40)],
        ]
    );
}

#[test]
fn having_filters_groups() {
    let (_d, db) = open();
    db.execute("CREATE TABLE o (cust INT, amt INT)").unwrap();
    db.execute("INSERT INTO o VALUES (1,100),(1,50),(2,200),(2,25),(2,75)")
        .unwrap();
    assert_eq!(
        rows(
            &db,
            "SELECT cust FROM o GROUP BY cust HAVING COUNT(*) > 2 ORDER BY cust"
        ),
        r1(vec![i(2)])
    );
    assert_eq!(
        rows(
            &db,
            "SELECT cust FROM o GROUP BY cust HAVING SUM(amt) > 200 ORDER BY cust"
        ),
        r1(vec![i(2)])
    );
}

#[test]
fn aggregate_expression_arithmetic() {
    let (_d, db) = open();
    m(&db);
    // SUM(v) * 2 and COUNT(*) + 1
    assert_eq!(rows(&db, "SELECT SUM(v) * 2 AS x FROM m"), r1(vec![i(120)]));
    assert_eq!(rows(&db, "SELECT COUNT(*) + 1 AS x FROM m"), r1(vec![i(6)]));
}

#[test]
fn order_by_aggregate() {
    let (_d, db) = open();
    db.execute("CREATE TABLE o (cust INT, amt INT)").unwrap();
    db.execute("INSERT INTO o VALUES (1,100),(1,50),(2,200),(2,25),(2,75)")
        .unwrap();
    // Highest total first.
    assert_eq!(
        rows(
            &db,
            "SELECT cust FROM o GROUP BY cust ORDER BY SUM(amt) DESC"
        ),
        vec![vec![i(2)], vec![i(1)]]
    );
}

#[test]
fn group_output_includes_key_and_aggregate() {
    let (_d, db) = open();
    db.execute("CREATE TABLE o (cust INT, amt INT)").unwrap();
    db.execute("INSERT INTO o VALUES (1,100),(2,200)").unwrap();
    let (cols, _rws) = cols_rows(
        &db,
        "SELECT cust, COUNT(*) AS n, SUM(amt) AS total FROM o GROUP BY cust ORDER BY cust",
    );
    assert_eq!(cols, vec!["cust", "n", "total"]);
}
