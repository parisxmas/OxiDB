//! WITH RECURSIVE: fixpoint-evaluated common table expressions.
//!
//! The engine materializes each self-referencing CTE before the main query
//! runs: the anchor arm seeds the row set, then the step arm is re-executed
//! with the CTE name bound to the previous iteration's rows until an
//! iteration adds nothing. UNION (distinct) drops already-seen rows — the
//! standard termination device on cyclic data; UNION ALL keeps duplicates.

mod common;

use common::*;
use oxidb_sql::Value;

// ── series generation ───────────────────────────────────────────────────────

#[test]
fn counts_to_ten() {
    let (_d, db) = open();
    let got = rows(
        &db,
        "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 10) \
         SELECT count(*), sum(n), min(n), max(n) FROM t",
    );
    assert_eq!(got, vec![vec![i(10), i(55), i(1), i(10)]]);
}

#[test]
fn ordered_series_rows() {
    let (_d, db) = open();
    let got = rows(
        &db,
        "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 4) \
         SELECT n FROM t ORDER BY n DESC",
    );
    assert_eq!(got, vec![vec![i(4)], vec![i(3)], vec![i(2)], vec![i(1)]]);
}

#[test]
fn column_names_come_from_the_alias_list() {
    let (_d, db) = open();
    let (cols, got) = cols_rows(
        &db,
        "WITH RECURSIVE seq(val) AS (SELECT 10 UNION ALL SELECT val - 1 FROM seq WHERE val > 8) \
         SELECT val FROM seq ORDER BY val",
    );
    assert_eq!(cols, vec!["val"]);
    assert_eq!(got, vec![vec![i(8)], vec![i(9)], vec![i(10)]]);
}

#[test]
fn anchor_names_used_when_no_alias_list() {
    let (_d, db) = open();
    let got = rows(
        &db,
        "WITH RECURSIVE t AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM t WHERE n < 3) \
         SELECT sum(n) FROM t",
    );
    assert_eq!(got, vec![vec![i(6)]]);
}

#[test]
fn multi_column_step_arithmetic() {
    // Fibonacci: two carried columns.
    let (_d, db) = open();
    let got = rows(
        &db,
        "WITH RECURSIVE fib(a, b) AS (SELECT 0, 1 UNION ALL SELECT b, a + b FROM fib WHERE b < 100) \
         SELECT max(b) FROM fib",
    );
    assert_eq!(got, vec![vec![i(144)]]);
}

// ── recursion over base tables ──────────────────────────────────────────────

#[test]
fn org_chart_transitive_closure() {
    let (_d, db) = open();
    db.execute("CREATE TABLE emp (id INT, boss INT, name TEXT)")
        .unwrap();
    db.execute(
        "INSERT INTO emp VALUES (1, NULL, 'ceo'), (2, 1, 'cto'), (3, 1, 'cfo'), \
         (4, 2, 'dev'), (5, 4, 'intern')",
    )
    .unwrap();
    // Everyone under the CTO, with depth.
    let got = rows(
        &db,
        "WITH RECURSIVE under(id, name, depth) AS ( \
             SELECT id, name, 0 FROM emp WHERE id = 2 \
             UNION ALL \
             SELECT e.id, e.name, u.depth + 1 FROM emp e JOIN under u ON e.boss = u.id) \
         SELECT name, depth FROM under ORDER BY depth, name",
    );
    assert_eq!(
        got,
        vec![
            vec![t("cto"), i(0)],
            vec![t("dev"), i(1)],
            vec![t("intern"), i(2)],
        ]
    );
}

#[test]
fn union_distinct_terminates_on_cycles() {
    // A cyclic graph: reachability must terminate because UNION drops rows
    // already produced.
    let (_d, db) = open();
    db.execute("CREATE TABLE edge (src INT, dst INT)").unwrap();
    db.execute("INSERT INTO edge VALUES (1, 2), (2, 3), (3, 1), (3, 4)")
        .unwrap();
    let got = rows(
        &db,
        "WITH RECURSIVE reach(node) AS ( \
             SELECT 1 \
             UNION \
             SELECT e.dst FROM edge e JOIN reach r ON e.src = r.node) \
         SELECT node FROM reach ORDER BY node",
    );
    assert_eq!(got, vec![vec![i(1)], vec![i(2)], vec![i(3)], vec![i(4)]]);
}

#[test]
fn union_distinct_dedups_anchor_rows() {
    let (_d, db) = open();
    db.execute("CREATE TABLE v (x INT)").unwrap();
    db.execute("INSERT INTO v VALUES (1), (1), (2)").unwrap();
    let got = rows(
        &db,
        "WITH RECURSIVE t(x) AS (SELECT x FROM v UNION SELECT x + 10 FROM t WHERE x < 10) \
         SELECT x FROM t ORDER BY x",
    );
    assert_eq!(got, vec![vec![i(1)], vec![i(2)], vec![i(11)], vec![i(12)]]);
}

#[test]
fn string_accumulation_builds_paths() {
    let (_d, db) = open();
    db.execute("CREATE TABLE cat (id INT, parent INT, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO cat VALUES (1, NULL, 'root'), (2, 1, 'sub'), (3, 2, 'leaf')")
        .unwrap();
    let got = rows(
        &db,
        "WITH RECURSIVE tree(id, path) AS ( \
             SELECT id, name FROM cat WHERE parent IS NULL \
             UNION ALL \
             SELECT c.id, tr.path || '/' || c.name FROM cat c JOIN tree tr ON c.parent = tr.id) \
         SELECT path FROM tree ORDER BY id",
    );
    assert_eq!(
        got,
        vec![
            vec![t("root")],
            vec![t("root/sub")],
            vec![t("root/sub/leaf")]
        ]
    );
}

// ── composition with the rest of the engine ─────────────────────────────────

#[test]
fn mixes_with_non_recursive_ctes() {
    // A non-recursive CTE before it (inlined as usual) and one after it that
    // references the recursive result.
    let (_d, db) = open();
    let got = rows(
        &db,
        "WITH RECURSIVE \
             base(s) AS (SELECT 3), \
             t(n) AS (SELECT s FROM base UNION ALL SELECT n + 1 FROM t WHERE n < 6), \
             doubled AS (SELECT n * 2 AS d FROM t) \
         SELECT sum(d) FROM doubled",
    );
    assert_eq!(got, vec![vec![i(36)]]); // (3+4+5+6)*2
}

#[test]
fn second_recursive_cte_reads_the_first() {
    let (_d, db) = open();
    let got = rows(
        &db,
        "WITH RECURSIVE \
             a(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM a WHERE n < 3), \
             b(m) AS (SELECT max(n) FROM a UNION ALL SELECT m * 2 FROM b WHERE m < 20) \
         SELECT m FROM b ORDER BY m",
    );
    assert_eq!(got, vec![vec![i(3)], vec![i(6)], vec![i(12)], vec![i(24)]]);
}

#[test]
fn referenced_twice_and_joined_with_itself() {
    let (_d, db) = open();
    let got = rows(
        &db,
        "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 3) \
         SELECT a.n, b.n FROM t a JOIN t b ON b.n = a.n + 1 ORDER BY a.n",
    );
    assert_eq!(got, vec![vec![i(1), i(2)], vec![i(2), i(3)]]);
}

#[test]
fn usable_in_expression_subqueries() {
    let (_d, db) = open();
    db.execute("CREATE TABLE x (v INT)").unwrap();
    db.execute("INSERT INTO x VALUES (2), (5), (9)").unwrap();
    let got = rows(
        &db,
        "SELECT v FROM x WHERE v IN \
         (WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 5) \
          SELECT n FROM t) \
         ORDER BY v",
    );
    assert_eq!(got, vec![vec![i(2)], vec![i(5)]]);
}

#[test]
fn works_with_parameters() {
    let (_d, db) = open();
    let got = rows_p(
        &db,
        "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < $1) \
         SELECT sum(n) FROM t",
        &[Value::Int(4)],
    );
    assert_eq!(got, vec![vec![i(10)]]);
}

#[test]
fn empty_anchor_yields_empty_result() {
    let (_d, db) = open();
    db.execute("CREATE TABLE e (v INT)").unwrap();
    let got = rows(
        &db,
        "WITH RECURSIVE t(n) AS (SELECT v FROM e UNION ALL SELECT n + 1 FROM t WHERE n < 3) \
         SELECT count(*) FROM t",
    );
    assert_eq!(got, vec![vec![i(0)]]);
}

#[test]
fn group_by_over_recursive_result() {
    let (_d, db) = open();
    let got = rows(
        &db,
        "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 10) \
         SELECT n % 3 AS r, count(*) AS c FROM t GROUP BY n % 3 ORDER BY r",
    );
    assert_eq!(
        got,
        vec![vec![i(0), i(3)], vec![i(1), i(4)], vec![i(2), i(3)]]
    );
}

// ── error shapes ────────────────────────────────────────────────────────────

#[test]
fn missing_union_is_rejected() {
    let (_d, db) = open();
    let err = db
        .execute("WITH RECURSIVE t(n) AS (SELECT n + 1 FROM t) SELECT * FROM t")
        .unwrap_err();
    assert!(err.to_string().contains("UNION"), "unexpected error: {err}");
}

#[test]
fn self_reference_in_anchor_is_rejected() {
    let (_d, db) = open();
    let err = db
        .execute("WITH RECURSIVE t(n) AS (SELECT n FROM t UNION ALL SELECT 1) SELECT * FROM t")
        .unwrap_err();
    assert!(
        err.to_string().contains("anchor"),
        "unexpected error: {err}"
    );
}

#[test]
fn column_count_mismatch_is_rejected() {
    let (_d, db) = open();
    let err = db
        .execute(
            "WITH RECURSIVE t(a, b) AS (SELECT 1 UNION ALL SELECT a, b FROM t) SELECT * FROM t",
        )
        .unwrap_err();
    assert!(
        err.to_string().contains("column"),
        "unexpected error: {err}"
    );
}

#[test]
fn runaway_union_all_recursion_errors_instead_of_hanging() {
    // No termination condition: the working set doubles every iteration, so
    // the row guard trips quickly instead of looping forever.
    let (_d, db) = open();
    let err = db
        .execute(
            "WITH RECURSIVE t(n) AS \
             (SELECT 1 UNION ALL (SELECT n FROM t UNION ALL SELECT n FROM t)) \
             SELECT count(*) FROM t",
        )
        .unwrap_err();
    assert!(
        err.to_string().contains("recursive CTE"),
        "unexpected error: {err}"
    );
}

// ── non-recursive CTE column aliases (unlocked by the same change) ──────────

#[test]
fn plain_cte_column_alias_list() {
    let (_d, db) = open();
    let (cols, got) = cols_rows(
        &db,
        "WITH pair(lo, hi) AS (SELECT 1, 9) SELECT lo, hi FROM pair",
    );
    assert_eq!(cols, vec!["lo", "hi"]);
    assert_eq!(got, vec![vec![i(1), i(9)]]);
}
