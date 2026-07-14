//! Tests for the post-Phase-4 v1 gap closure: PRIMARY KEY uniqueness,
//! implicit INT->DOUBLE/TIMESTAMP coercion, timestamp literals, OFFSET,
//! UNION [ALL], IN lists, and uncorrelated subqueries.

mod common;

use common::*;
use oxidb_sql::{SqlError, Value};

// ── PRIMARY KEY uniqueness ──────────────────────────────────────────────────

#[test]
fn pk_duplicate_insert_is_rejected() {
    let (_d, db) = open();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO u VALUES (1, 'ada')").unwrap();
    let err = db.execute("INSERT INTO u VALUES (1, 'bob')").unwrap_err();
    assert!(matches!(err, SqlError::DuplicateKey(_)), "got {err:?}");
    // The failed insert changed nothing.
    assert_eq!(rows(&db, "SELECT name FROM u"), vec![vec![t("ada")]]);
}

#[test]
fn pk_duplicate_within_one_insert_is_rejected_atomically() {
    let (_d, db) = open();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY)").unwrap();
    assert!(db.execute("INSERT INTO u VALUES (1), (2), (1)").is_err());
    // All-or-nothing: no rows landed.
    assert!(rows(&db, "SELECT id FROM u").is_empty());
}

#[test]
fn pk_update_conflicts_and_self_update_allowed() {
    let (_d, db) = open();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO u VALUES (1, 10), (2, 20)").unwrap();
    // Moving row 2 onto key 1 conflicts.
    assert!(db.execute("UPDATE u SET id = 1 WHERE id = 2").is_err());
    // Updating a row without changing its key (or to a fresh key) is fine.
    assert!(db.execute("UPDATE u SET v = 11 WHERE id = 1").is_ok());
    assert!(db.execute("UPDATE u SET id = 3 WHERE id = 2").is_ok());
    // A deleted key can be reused.
    db.execute("DELETE FROM u WHERE id = 1").unwrap();
    assert!(db.execute("INSERT INTO u VALUES (1, 100)").is_ok());
}

#[test]
fn pk_enforced_inside_transactions() {
    let (_d, db) = open();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO u VALUES (1)").unwrap();
    // Conflict with a committed row, inside a transaction.
    assert!(
        db.execute("BEGIN; INSERT INTO u VALUES (1); COMMIT;")
            .is_err()
    );
    // Conflict between two uncommitted rows of the same transaction.
    assert!(
        db.execute("BEGIN; INSERT INTO u VALUES (2); INSERT INTO u VALUES (2); COMMIT;")
            .is_err()
    );
    // Delete-then-reinsert of the same key inside a transaction is fine.
    db.execute("BEGIN; DELETE FROM u WHERE id = 1; INSERT INTO u VALUES (1); COMMIT;")
        .unwrap();
    assert_eq!(rows(&db, "SELECT id FROM u"), vec![vec![i(1)]]);
}

#[test]
fn pk_uniqueness_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open_at(dir.path());
        db.execute("CREATE TABLE u (id INT PRIMARY KEY)").unwrap();
        db.execute("INSERT INTO u VALUES (7)").unwrap();
    }
    let db = open_at(dir.path());
    assert!(db.execute("INSERT INTO u VALUES (7)").is_err());
    assert!(db.execute("INSERT INTO u VALUES (8)").is_ok());
}

#[test]
fn multiple_pk_columns_rejected() {
    let (_d, db) = open();
    assert!(
        db.execute("CREATE TABLE bad (a INT PRIMARY KEY, b INT PRIMARY KEY)")
            .is_err()
    );
}

// ── implicit coercion + timestamp literals ─────────────────────────────────

#[test]
fn int_coerces_into_double_and_timestamp_columns() {
    let (_d, db) = open();
    db.execute("CREATE TABLE m (d DOUBLE, ts TIMESTAMP)")
        .unwrap();
    db.execute("INSERT INTO m VALUES (5, 1700000000000)")
        .unwrap();
    assert_eq!(
        rows(&db, "SELECT d, ts FROM m"),
        vec![vec![d(5.0), Value::Timestamp(1_700_000_000_000)]]
    );
}

#[test]
fn coercion_applies_to_params_and_updates() {
    let (_d, db) = open();
    db.execute("CREATE TABLE m (x DOUBLE)").unwrap();
    db.execute_params("INSERT INTO m VALUES (?)", &[Value::Int(3)])
        .unwrap();
    db.execute("UPDATE m SET x = 4").unwrap();
    assert_eq!(rows(&db, "SELECT x FROM m"), vec![vec![d(4.0)]]);
}

#[test]
fn timestamp_literal_forms() {
    let (_d, db) = open();
    db.execute("CREATE TABLE e (ts TIMESTAMP)").unwrap();
    db.execute("INSERT INTO e VALUES (TIMESTAMP '1970-01-01 00:00:00')")
        .unwrap();
    db.execute("INSERT INTO e VALUES (TIMESTAMP '1970-01-02')")
        .unwrap();
    db.execute("INSERT INTO e VALUES (TIMESTAMP '2000-01-01T00:00:00.250Z')")
        .unwrap();
    db.execute("INSERT INTO e VALUES (TIMESTAMP '1970-01-01 02:00:00+02:00')")
        .unwrap();
    assert_eq!(
        rows(&db, "SELECT ts FROM e"),
        vec![
            vec![Value::Timestamp(0)],
            vec![Value::Timestamp(86_400_000)],
            vec![Value::Timestamp(946_684_800_250)],
            vec![Value::Timestamp(0)], // +02:00 offset converts back to epoch
        ]
    );
    // Comparisons against timestamp literals work in WHERE.
    assert_eq!(
        rows(
            &db,
            "SELECT COUNT(*) AS n FROM e WHERE ts < TIMESTAMP '1970-01-01 12:00:00'"
        ),
        vec![vec![i(2)]]
    );
    assert!(
        db.execute("INSERT INTO e VALUES (TIMESTAMP 'not a date')")
            .is_err()
    );
}

// ── OFFSET ──────────────────────────────────────────────────────────────────

#[test]
fn offset_pages_through_results() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3),(4),(5)")
        .unwrap();
    assert_eq!(
        rows(&db, "SELECT id FROM t ORDER BY id LIMIT 2 OFFSET 2"),
        vec![vec![i(3)], vec![i(4)]]
    );
    // OFFSET without LIMIT skips and returns the rest.
    assert_eq!(
        rows(&db, "SELECT id FROM t ORDER BY id OFFSET 4"),
        vec![vec![i(5)]]
    );
    // OFFSET past the end is empty.
    assert!(rows(&db, "SELECT id FROM t ORDER BY id OFFSET 9").is_empty());
}

// ── UNION / UNION ALL ───────────────────────────────────────────────────────

#[test]
fn union_distinct_and_all() {
    let (_d, db) = open();
    db.execute("CREATE TABLE a (x INT)").unwrap();
    db.execute("CREATE TABLE b (x INT)").unwrap();
    db.execute("INSERT INTO a VALUES (1),(2),(2)").unwrap();
    db.execute("INSERT INTO b VALUES (2),(3)").unwrap();
    // UNION dedups across (and within) arms.
    assert_eq!(
        rows(&db, "SELECT x FROM a UNION SELECT x FROM b ORDER BY x"),
        vec![vec![i(1)], vec![i(2)], vec![i(3)]]
    );
    // UNION ALL keeps duplicates.
    assert_eq!(
        rows(&db, "SELECT x FROM a UNION ALL SELECT x FROM b ORDER BY x"),
        vec![vec![i(1)], vec![i(2)], vec![i(2)], vec![i(2)], vec![i(3)]]
    );
}

#[test]
fn union_order_limit_offset_apply_to_combined_result() {
    let (_d, db) = open();
    db.execute("CREATE TABLE a (x INT)").unwrap();
    db.execute("INSERT INTO a VALUES (5),(1),(3)").unwrap();
    assert_eq!(
        rows(
            &db,
            "SELECT x FROM a UNION ALL SELECT x FROM a ORDER BY x LIMIT 3 OFFSET 1"
        ),
        vec![vec![i(1)], vec![i(3)], vec![i(3)]]
    );
    // ORDER BY by 1-based position.
    assert_eq!(
        rows(&db, "SELECT x FROM a UNION SELECT x FROM a ORDER BY 1"),
        vec![vec![i(1)], vec![i(3)], vec![i(5)]]
    );
}

// ── FROM-less SELECT ────────────────────────────────────────────────────────

#[test]
fn from_less_select_evaluates_expressions_once() {
    let (_d, db) = open();
    assert_eq!(rows(&db, "SELECT 1"), vec![vec![i(1)]]);
    let (cols, r) = cols_rows(&db, "SELECT 1 + 2 * 3 AS x, 'hi' AS s");
    assert_eq!(cols, vec!["x", "s"]);
    assert_eq!(r, vec![vec![i(7), t("hi")]]);
    // Bind parameters work without a table.
    assert_eq!(
        rows_p(&db, "SELECT ? + 1", &[Value::Int(41)]),
        vec![vec![i(42)]]
    );
}

#[test]
fn from_less_select_where_and_aggregates() {
    let (_d, db) = open();
    // WHERE can drop the implicit row.
    assert!(rows(&db, "SELECT 1 WHERE 1 = 0").is_empty());
    assert_eq!(rows(&db, "SELECT 1 WHERE 1 = 1"), vec![vec![i(1)]]);
    // Aggregates see the single implicit row (PostgreSQL: SELECT COUNT(*) → 1).
    assert_eq!(rows(&db, "SELECT COUNT(*) AS n"), vec![vec![i(1)]]);
    // Column references still error — there is nothing to resolve against.
    assert!(db.execute("SELECT missing").is_err());
}

#[test]
fn from_less_select_in_set_operations() {
    let (_d, db) = open();
    assert_eq!(
        rows(&db, "SELECT 1 UNION ALL SELECT 2 ORDER BY 1"),
        vec![vec![i(1)], vec![i(2)]]
    );
    db.execute("CREATE TABLE a (x INT)").unwrap();
    db.execute("INSERT INTO a VALUES (1),(3)").unwrap();
    // Mixed arms: table SELECT vs FROM-less SELECT.
    assert_eq!(
        rows(&db, "SELECT x FROM a EXCEPT SELECT 1"),
        vec![vec![i(3)]]
    );
    assert_eq!(
        rows(&db, "SELECT x FROM a INTERSECT SELECT 1"),
        vec![vec![i(1)]]
    );
}

#[test]
fn except_distinct_and_all() {
    let (_d, db) = open();
    db.execute("CREATE TABLE a (x INT)").unwrap();
    db.execute("CREATE TABLE b (x INT)").unwrap();
    db.execute("INSERT INTO a VALUES (1),(2),(2),(2),(3)")
        .unwrap();
    db.execute("INSERT INTO b VALUES (2),(3),(4)").unwrap();
    // EXCEPT: distinct left rows not present in the right arm.
    assert_eq!(
        rows(&db, "SELECT x FROM a EXCEPT SELECT x FROM b ORDER BY x"),
        vec![vec![i(1)]]
    );
    // EXCEPT ALL: bag difference — each right row cancels one left copy
    // (three 2s minus one 2 leaves two).
    assert_eq!(
        rows(&db, "SELECT x FROM a EXCEPT ALL SELECT x FROM b ORDER BY x"),
        vec![vec![i(1)], vec![i(2)], vec![i(2)]]
    );
}

#[test]
fn intersect_distinct_and_all() {
    let (_d, db) = open();
    db.execute("CREATE TABLE a (x INT)").unwrap();
    db.execute("CREATE TABLE b (x INT)").unwrap();
    db.execute("INSERT INTO a VALUES (1),(2),(2),(2),(3)")
        .unwrap();
    db.execute("INSERT INTO b VALUES (2),(2),(3),(4)").unwrap();
    // INTERSECT: distinct rows present in both arms.
    assert_eq!(
        rows(&db, "SELECT x FROM a INTERSECT SELECT x FROM b ORDER BY x"),
        vec![vec![i(2)], vec![i(3)]]
    );
    // INTERSECT ALL: bag intersection — min(3, 2) copies of 2.
    assert_eq!(
        rows(
            &db,
            "SELECT x FROM a INTERSECT ALL SELECT x FROM b ORDER BY x"
        ),
        vec![vec![i(2)], vec![i(2)], vec![i(3)]]
    );
}

#[test]
fn intersect_binds_tighter_than_union() {
    let (_d, db) = open();
    db.execute("CREATE TABLE a (x INT)").unwrap();
    db.execute("CREATE TABLE b (x INT)").unwrap();
    db.execute("CREATE TABLE c (x INT)").unwrap();
    db.execute("INSERT INTO a VALUES (1)").unwrap();
    db.execute("INSERT INTO b VALUES (2)").unwrap();
    db.execute("INSERT INTO c VALUES (3)").unwrap();
    // Standard precedence: a UNION (b INTERSECT c) = {1}, not (a UNION b)
    // INTERSECT c = {}.
    assert_eq!(
        rows(
            &db,
            "SELECT x FROM a UNION SELECT x FROM b INTERSECT SELECT x FROM c"
        ),
        vec![vec![i(1)]]
    );
}

#[test]
fn union_arity_mismatch_errors() {
    let (_d, db) = open();
    db.execute("CREATE TABLE a (x INT, y INT)").unwrap();
    db.execute("INSERT INTO a VALUES (1, 2)").unwrap();
    assert!(
        db.execute("SELECT x FROM a UNION SELECT x, y FROM a")
            .is_err()
    );
}

// ── IN lists + subqueries ───────────────────────────────────────────────────

#[test]
fn in_list_with_null_semantics() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, NULL)")
        .unwrap();
    assert_eq!(
        rows(&db, "SELECT id FROM t WHERE v IN (10, 30) ORDER BY id"),
        vec![vec![i(1)]]
    );
    // NULL v never matches IN; NOT IN with a NULL in the list matches nothing.
    assert_eq!(
        rows(&db, "SELECT id FROM t WHERE v NOT IN (10) ORDER BY id"),
        vec![vec![i(2)]]
    );
    assert!(rows(&db, "SELECT id FROM t WHERE v NOT IN (10, NULL)").is_empty());
}

#[test]
fn scalar_subquery_in_where_and_projection() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    assert_eq!(
        rows(&db, "SELECT id FROM t WHERE v = (SELECT MAX(v) FROM t)"),
        vec![vec![i(3)]]
    );
    assert_eq!(
        rows(&db, "SELECT (SELECT SUM(v) FROM t) AS total FROM t LIMIT 1"),
        vec![vec![i(60)]]
    );
    // Zero-row scalar subquery is NULL (matches nothing here).
    assert!(
        rows(
            &db,
            "SELECT id FROM t WHERE v = (SELECT v FROM t WHERE id = 99)"
        )
        .is_empty()
    );
    // More than one row errors.
    assert!(
        db.execute("SELECT id FROM t WHERE v = (SELECT v FROM t)")
            .is_err()
    );
}

#[test]
fn in_subquery() {
    let (_d, db) = open();
    db.execute("CREATE TABLE orders (id INT, customer_id INT)")
        .unwrap();
    db.execute("CREATE TABLE vip (customer_id INT)").unwrap();
    db.execute("INSERT INTO orders VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    db.execute("INSERT INTO vip VALUES (10), (30)").unwrap();
    assert_eq!(
        rows(
            &db,
            "SELECT id FROM orders WHERE customer_id IN (SELECT customer_id FROM vip) ORDER BY id"
        ),
        vec![vec![i(1)], vec![i(3)]]
    );
    assert_eq!(
        rows(
            &db,
            "SELECT id FROM orders WHERE customer_id NOT IN (SELECT customer_id FROM vip)"
        ),
        vec![vec![i(2)]]
    );
}

#[test]
fn subquery_works_inside_update_delete_and_transactions() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20)").unwrap();
    db.execute("UPDATE t SET v = (SELECT MAX(v) FROM t) WHERE id = 1")
        .unwrap();
    assert_eq!(
        rows(&db, "SELECT v FROM t ORDER BY id"),
        vec![vec![i(20)], vec![i(20)]]
    );
    db.execute("BEGIN; DELETE FROM t WHERE v IN (SELECT MAX(v) FROM t); COMMIT;")
        .unwrap();
    assert!(rows(&db, "SELECT id FROM t").is_empty());
}

// ── correlated subqueries ───────────────────────────────────────────────────

#[test]
fn correlated_scalar_in_where_and_projection() {
    let (_d, db) = open();
    db.execute("CREATE TABLE emp (id INT, dept INT, salary INT)")
        .unwrap();
    db.execute("INSERT INTO emp VALUES (1, 10, 100), (2, 10, 200), (3, 20, 50), (4, 20, 80)")
        .unwrap();
    // Employees earning the max of their own department.
    assert_eq!(
        rows(
            &db,
            "SELECT id FROM emp e WHERE salary = \
             (SELECT MAX(salary) FROM emp x WHERE x.dept = e.dept) ORDER BY id"
        ),
        vec![vec![i(2)], vec![i(4)]]
    );
    // Correlated scalar in the projection.
    assert_eq!(
        rows(
            &db,
            "SELECT id, (SELECT COUNT(*) FROM emp x WHERE x.dept = e.dept) AS peers \
             FROM emp e ORDER BY id"
        ),
        vec![
            vec![i(1), i(2)],
            vec![i(2), i(2)],
            vec![i(3), i(2)],
            vec![i(4), i(2)],
        ]
    );
}

#[test]
fn correlated_in_subquery_and_update_delete() {
    let (_d, db) = open();
    db.execute("CREATE TABLE orders (id INT, cust INT, total INT)")
        .unwrap();
    db.execute("CREATE TABLE refunds (order_id INT, cust INT)")
        .unwrap();
    db.execute("INSERT INTO orders VALUES (1, 7, 50), (2, 7, 60), (3, 8, 70)")
        .unwrap();
    db.execute("INSERT INTO refunds VALUES (1, 7)").unwrap();
    // Orders that have a refund by the same customer.
    assert_eq!(
        rows(
            &db,
            "SELECT id FROM orders o WHERE id IN \
             (SELECT order_id FROM refunds r WHERE r.cust = o.cust)"
        ),
        vec![vec![i(1)]]
    );
    // Correlated in UPDATE and DELETE (outer scope = the target table).
    db.execute(
        "UPDATE orders SET total = (SELECT COUNT(*) FROM refunds r WHERE r.cust = orders.cust)",
    )
    .unwrap();
    assert_eq!(
        rows(&db, "SELECT total FROM orders ORDER BY id"),
        vec![vec![i(1)], vec![i(1)], vec![i(0)]]
    );
    db.execute(
        "DELETE FROM orders WHERE id IN (SELECT order_id FROM refunds r WHERE r.cust = orders.cust)",
    )
    .unwrap();
    assert_eq!(
        rows(&db, "SELECT id FROM orders ORDER BY id"),
        vec![vec![i(2)], vec![i(3)]]
    );
}

#[test]
fn correlated_inner_scope_shadows_outer() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2)").unwrap();
    // `v` inside the subquery resolves to the subquery's own table, not the
    // outer one — so the subquery is uncorrelated and errors on >1 row.
    assert!(
        db.execute("SELECT v FROM t o WHERE v = (SELECT v FROM t)")
            .is_err()
    );
}

#[test]
fn correlated_in_aggregated_query_uses_group_keys() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (g INT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 1), (1, 2), (2, 5)")
        .unwrap();
    // The correlated subquery references the group key; it evaluates once
    // per group (against the group's first row).
    assert_eq!(
        rows(
            &db,
            "SELECT g, SUM(v) FROM t o GROUP BY g \
             HAVING SUM(v) > (SELECT MIN(v) FROM t x WHERE x.g = o.g) ORDER BY g"
        ),
        vec![vec![i(1), i(3)]]
    );
}

// ── views ───────────────────────────────────────────────────────────────────

#[test]
fn views_select_join_and_persistence() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open_at(dir.path());
        db.execute("CREATE TABLE sales (region TEXT, amount INT)")
            .unwrap();
        db.execute("INSERT INTO sales VALUES ('n', 10), ('n', 20), ('s', 5)")
            .unwrap();
        db.execute(
            "CREATE VIEW region_totals AS \
             SELECT region, SUM(amount) AS total FROM sales GROUP BY region",
        )
        .unwrap();
        // Plain select through the view (with WHERE + ORDER BY on top).
        assert_eq!(
            rows(
                &db,
                "SELECT region, total FROM region_totals WHERE total > 6 ORDER BY region"
            ),
            vec![vec![t("n"), i(30)]]
        );
        // A view is joinable like a table (with an alias).
        assert_eq!(
            rows(
                &db,
                "SELECT s.amount FROM sales s \
                 JOIN region_totals rt ON rt.region = s.region \
                 WHERE rt.total > 6 ORDER BY s.amount"
            ),
            vec![vec![i(10)], vec![i(20)]]
        );
        // Views see fresh data on every use.
        db.execute("INSERT INTO sales VALUES ('s', 100)").unwrap();
        assert_eq!(
            rows(&db, "SELECT total FROM region_totals WHERE region = 's'"),
            vec![vec![i(105)]]
        );
    }
    // Views persist across reopen.
    let db = open_at(dir.path());
    assert_eq!(
        rows(&db, "SELECT total FROM region_totals WHERE region = 'n'"),
        vec![vec![i(30)]]
    );
}

#[test]
fn view_ddl_rules() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (a INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    db.execute("CREATE VIEW v AS SELECT a FROM t").unwrap();

    // Name collisions in both directions.
    assert!(db.execute("CREATE VIEW t AS SELECT a FROM t").is_err());
    assert!(db.execute("CREATE TABLE v (x INT)").is_err());
    assert!(db.execute("CREATE VIEW v AS SELECT a FROM t").is_err());
    // OR REPLACE swaps the body.
    db.execute("CREATE OR REPLACE VIEW v AS SELECT a + 1 AS a FROM t")
        .unwrap();
    assert_eq!(rows(&db, "SELECT a FROM v"), vec![vec![i(2)]]);

    // A view body referencing a missing table fails at creation.
    assert!(
        db.execute("CREATE VIEW bad AS SELECT x FROM ghost")
            .is_err()
    );
    // A semicolon ends the view body: the trailing SELECT is a separate
    // statement of the batch, not part of the view (PostgreSQL semantics).
    db.execute("CREATE VIEW single AS SELECT a FROM t; SELECT 1")
        .unwrap();
    assert_eq!(rows(&db, "SELECT a FROM single"), vec![vec![i(1)]]);
    db.execute("DROP VIEW single").unwrap();

    // Writes against a view fail.
    assert!(db.execute("INSERT INTO v VALUES (9)").is_err());
    // Views are per-statement read objects; a view over a view works.
    db.execute("CREATE VIEW vv AS SELECT a FROM v").unwrap();
    assert_eq!(rows(&db, "SELECT a FROM vv"), vec![vec![i(2)]]);

    db.execute("DROP VIEW vv").unwrap();
    db.execute("DROP VIEW IF EXISTS vv").unwrap();
    assert!(db.execute("DROP VIEW vv").is_err());
    // DROP VIEW does not touch same-named tables and vice versa.
    assert!(db.execute("DROP VIEW t").is_err());
}

// ── window functions ────────────────────────────────────────────────────────

#[test]
fn window_ranking_functions() {
    let (_d, db) = open();
    db.execute("CREATE TABLE s (dept TEXT, score INT)").unwrap();
    db.execute("INSERT INTO s VALUES ('a', 10), ('a', 30), ('a', 30), ('a', 40), ('b', 5)")
        .unwrap();
    // ROW_NUMBER / RANK / DENSE_RANK per partition (peers tie on 30).
    assert_eq!(
        rows(
            &db,
            "SELECT dept, score, \
             ROW_NUMBER() OVER (PARTITION BY dept ORDER BY score) AS rn, \
             RANK() OVER (PARTITION BY dept ORDER BY score) AS rk, \
             DENSE_RANK() OVER (PARTITION BY dept ORDER BY score) AS dr \
             FROM s ORDER BY dept, score, rn"
        ),
        vec![
            vec![t("a"), i(10), i(1), i(1), i(1)],
            vec![t("a"), i(30), i(2), i(2), i(2)],
            vec![t("a"), i(30), i(3), i(2), i(2)],
            vec![t("a"), i(40), i(4), i(4), i(3)],
            vec![t("b"), i(5), i(1), i(1), i(1)],
        ]
    );
}

#[test]
fn window_aggregates_whole_partition_and_running() {
    let (_d, db) = open();
    db.execute("CREATE TABLE s (g TEXT, v INT)").unwrap();
    db.execute("INSERT INTO s VALUES ('a', 1), ('a', 2), ('a', 2), ('b', 10)")
        .unwrap();
    // Whole-partition aggregate (no ORDER BY in the window).
    assert_eq!(
        rows(
            &db,
            "SELECT g, v, SUM(v) OVER (PARTITION BY g) AS total FROM s ORDER BY g, v"
        ),
        vec![
            vec![t("a"), i(1), i(5)],
            vec![t("a"), i(2), i(5)],
            vec![t("a"), i(2), i(5)],
            vec![t("b"), i(10), i(10)],
        ]
    );
    // Running aggregate: peers (the two v=2 rows) share the cumulative value.
    assert_eq!(
        rows(
            &db,
            "SELECT v, SUM(v) OVER (PARTITION BY g ORDER BY v) AS run \
             FROM s WHERE g = 'a' ORDER BY v"
        ),
        vec![vec![i(1), i(1)], vec![i(2), i(5)], vec![i(2), i(5)]]
    );
    // Running COUNT(*) and AVG.
    assert_eq!(
        rows(
            &db,
            "SELECT v, COUNT(*) OVER (ORDER BY v) AS c, AVG(v) OVER (ORDER BY v) AS a \
             FROM s WHERE g = 'a' ORDER BY v, c"
        ),
        vec![
            vec![i(1), i(1), d(1.0)],
            vec![i(2), i(3), d(5.0 / 3.0)],
            vec![i(2), i(3), d(5.0 / 3.0)],
        ]
    );
}

#[test]
fn window_rejected_outside_projection() {
    let (_d, db) = open();
    db.execute("CREATE TABLE s (v INT)").unwrap();
    db.execute("INSERT INTO s VALUES (1)").unwrap();
    assert!(
        db.execute("SELECT v FROM s WHERE ROW_NUMBER() OVER (ORDER BY v) = 1")
            .is_err()
    );
    assert!(
        db.execute("SELECT SUM(v), ROW_NUMBER() OVER (ORDER BY v) FROM s")
            .is_err()
    );
    assert!(
        db.execute("SELECT SUM(v) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM s")
            .is_err()
    );
    // A view makes window results filterable.
    db.execute("CREATE VIEW ranked AS SELECT v, ROW_NUMBER() OVER (ORDER BY v) AS rn FROM s")
        .unwrap();
    assert_eq!(
        rows(&db, "SELECT v FROM ranked WHERE rn = 1"),
        vec![vec![i(1)]]
    );
}

// ── join reordering ─────────────────────────────────────────────────────────

#[test]
fn join_reordering_preserves_results() {
    let (_d, db) = open();
    db.execute("CREATE TABLE big (id INT, small_id INT, v INT)")
        .unwrap();
    db.execute("CREATE TABLE small (id INT, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE mid (id INT, big_id INT)").unwrap();
    let mut vals = String::new();
    for i in 0..500 {
        if i > 0 {
            vals.push(',');
        }
        vals.push_str(&format!("({}, {}, {})", i, i % 5, i * 2));
    }
    db.execute(&format!("INSERT INTO big VALUES {vals}"))
        .unwrap();
    db.execute("INSERT INTO small VALUES (0,'a'),(1,'b'),(2,'c'),(3,'d'),(4,'e')")
        .unwrap();
    db.execute("INSERT INTO mid VALUES (1, 10), (2, 20), (3, 499)")
        .unwrap();
    // Written big-first: the planner may reorder (small, mid first) but the
    // result set must be identical.
    assert_eq!(
        rows(
            &db,
            "SELECT s.name, m.id, b.v FROM big b \
             JOIN small s ON s.id = b.small_id \
             JOIN mid m ON m.big_id = b.id \
             ORDER BY m.id"
        ),
        vec![
            vec![t("a"), i(1), i(20)],
            vec![t("a"), i(2), i(40)],
            vec![t("e"), i(3), i(998)],
        ]
    );
    // Outer joins keep written order (and stay correct).
    assert_eq!(
        rows(
            &db,
            "SELECT COUNT(*) AS n FROM big b \
             LEFT JOIN small s ON s.id = b.small_id \
             JOIN mid m ON m.big_id = b.id"
        ),
        vec![vec![i(3)]]
    );
}
