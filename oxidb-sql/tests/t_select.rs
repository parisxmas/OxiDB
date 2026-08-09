//! SELECT projection, wildcards, aliases, ORDER BY, LIMIT.

mod common;
use common::*;

fn seed(db: &oxidb_sql::SqlEngine) {
    db.execute("CREATE TABLE t (id INT, name TEXT, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',30),(2,'b',10),(3,'c',20)")
        .unwrap();
}

#[test]
fn wildcard_projects_all_columns_in_order() {
    let (_d, db) = open();
    seed(&db);
    let (cols, rws) = cols_rows(&db, "SELECT * FROM t WHERE id = 1");
    assert_eq!(cols, vec!["id", "name", "v"]);
    assert_eq!(rws, r1(vec![i(1), t("a"), i(30)]));
}

#[test]
fn explicit_column_order_and_subset() {
    let (_d, db) = open();
    seed(&db);
    let (cols, rws) = cols_rows(&db, "SELECT v, id FROM t WHERE id = 2");
    assert_eq!(cols, vec!["v", "id"]);
    assert_eq!(rws, r1(vec![i(10), i(2)]));
}

#[test]
fn duplicate_projection_columns() {
    let (_d, db) = open();
    seed(&db);
    let (cols, rws) = cols_rows(&db, "SELECT id, id FROM t WHERE id = 1");
    assert_eq!(cols, vec!["id", "id"]);
    assert_eq!(rws, r1(vec![i(1), i(1)]));
}

#[test]
fn expression_projection_with_alias() {
    let (_d, db) = open();
    seed(&db);
    let (cols, rws) = cols_rows(&db, "SELECT id * 100 + v AS score FROM t WHERE id = 1");
    assert_eq!(cols, vec!["score"]);
    assert_eq!(rws, r1(vec![i(130)]));
}

#[test]
fn order_by_column_not_in_projection() {
    let (_d, db) = open();
    seed(&db);
    // Project name, order by v.
    assert_eq!(
        rows(&db, "SELECT name FROM t ORDER BY v"),
        vec![vec![t("b")], vec![t("c")], vec![t("a")]]
    );
}

#[test]
fn order_by_expression() {
    let (_d, db) = open();
    seed(&db);
    assert_eq!(
        rows(&db, "SELECT id FROM t ORDER BY -v"),
        vec![vec![i(1)], vec![i(3)], vec![i(2)]]
    );
}

#[test]
fn limit_variants() {
    let (_d, db) = open();
    seed(&db);
    assert_eq!(rows(&db, "SELECT id FROM t ORDER BY id LIMIT 2").len(), 2);
    assert!(rows(&db, "SELECT id FROM t LIMIT 0").is_empty());
    // LIMIT larger than the row count returns everything.
    assert_eq!(rows(&db, "SELECT id FROM t LIMIT 100").len(), 3);
}

#[test]
fn empty_table_select() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT)").unwrap();
    let (cols, rws) = cols_rows(&db, "SELECT id FROM t");
    assert_eq!(cols, vec!["id"]);
    assert!(rws.is_empty());
}

#[test]
fn where_true_and_false_constant_ish() {
    let (_d, db) = open();
    seed(&db);
    // Always-true predicate keeps all; always-false keeps none.
    assert_eq!(rows(&db, "SELECT id FROM t WHERE id = id").len(), 3);
    assert!(rows(&db, "SELECT id FROM t WHERE id != id").is_empty());
}

#[test]
fn order_by_projection_alias() {
    let (_d, db) = open();
    seed(&db);
    // `id2` is an output alias, not an input column.
    assert_eq!(
        rows(&db, "SELECT id AS id2 FROM t ORDER BY id2 DESC"),
        vec![vec![i(3)], vec![i(2)], vec![i(1)]]
    );
}

#[test]
fn table_alias_qualified_columns() {
    let (_d, db) = open();
    seed(&db);
    assert_eq!(
        rows(&db, "SELECT x.id FROM t x WHERE x.v = 20"),
        r1(vec![i(3)])
    );
}

/// ORDER BY + LIMIT takes the bounded top-N path; results must be identical
/// to a full sort — including stable ties, OFFSET, parameterized limits, and
/// DISTINCT (which must NOT use it: dedup happens before LIMIT).
#[test]
fn order_by_limit_top_n_matches_full_sort() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, k INT, v TEXT)")
        .unwrap();
    // k cycles 0..5 so every key group has many ties; id is insertion order.
    let vals: Vec<String> = (1..=200)
        .map(|i| format!("({i}, {}, 'v{}')", i % 5, i % 3))
        .collect();
    db.execute(&format!("INSERT INTO t VALUES {}", vals.join(", ")))
        .unwrap();

    // Full sort (no LIMIT) as the oracle.
    let all = rows(&db, "SELECT id FROM t ORDER BY k, id DESC");
    let top7 = rows(&db, "SELECT id FROM t ORDER BY k, id DESC LIMIT 7");
    assert_eq!(top7, all[..7].to_vec());
    let off = rows(
        &db,
        "SELECT id FROM t ORDER BY k, id DESC LIMIT 5 OFFSET 10",
    );
    assert_eq!(off, all[10..15].to_vec());

    // Stable ties: equal keys keep input order.
    let ties = rows(&db, "SELECT id FROM t ORDER BY k LIMIT 3");
    assert_eq!(ties, vec![vec![i(5)], vec![i(10)], vec![i(15)]]);

    // Parameterized LIMIT/OFFSET (the EF Skip/Take shape).
    let p = rows_p(
        &db,
        "SELECT id FROM t ORDER BY k, id DESC LIMIT $1 OFFSET $2",
        &[i(4), i(2)],
    );
    assert_eq!(p, all[2..6].to_vec());

    // LIMIT 0 and LIMIT past the end.
    assert!(rows(&db, "SELECT id FROM t ORDER BY k LIMIT 0").is_empty());
    assert_eq!(
        rows(&db, "SELECT id FROM t ORDER BY k, id DESC LIMIT 9999").len(),
        200
    );

    // DISTINCT dedups BEFORE LIMIT: 3 distinct v values exist, so LIMIT 3
    // must yield all 3 (a top-3-rows-then-dedup shortcut would yield fewer).
    let d = rows(&db, "SELECT DISTINCT v FROM t ORDER BY v LIMIT 3");
    assert_eq!(d.len(), 3);
}

/// Streamed single-table scans (WHERE push-down, LIMIT early-stop, in-scan
/// top-N) must be invisible: every shape here compares against semantics a
/// full materialized scan would produce.
#[test]
fn streamed_scan_shapes_match_materialized_semantics() {
    let (_d, db) = open();
    db.execute("CREATE TABLE s (id INT PRIMARY KEY, grp INT, name TEXT)")
        .unwrap();
    let vals: Vec<String> = (1..=300)
        .map(|i| format!("({i}, {}, 'name{:03}')", i % 3, i % 50))
        .collect();
    db.execute(&format!("INSERT INTO s VALUES {}", vals.join(", ")))
        .unwrap();

    // WHERE push-down on a string predicate (evaluated on borrowed rows).
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM s WHERE name = 'name007'"),
        vec![vec![i(6)]]
    );

    // LIMIT early-stop: first N rows in scan order.
    assert_eq!(
        rows(&db, "SELECT id FROM s WHERE grp = 1 LIMIT 3"),
        vec![vec![i(1)], vec![i(4)], vec![i(7)]]
    );
    assert_eq!(
        rows(&db, "SELECT id FROM s WHERE grp = 1 LIMIT 3 OFFSET 2"),
        vec![vec![i(7)], vec![i(10)], vec![i(13)]]
    );

    // DISTINCT + LIMIT: dedup happens before LIMIT, so the scan must NOT
    // stop after 3 raw rows — all 3 distinct groups exist.
    let d = rows(&db, "SELECT DISTINCT grp FROM s LIMIT 3");
    assert_eq!(d.len(), 3);

    // GROUP BY + WHERE + LIMIT: the LIMIT counts groups, not scanned rows —
    // aggregates must still see every matching row.
    let g = rows(
        &db,
        "SELECT grp, COUNT(*) FROM s WHERE id <= 299 GROUP BY grp ORDER BY grp LIMIT 2",
    );
    assert_eq!(g, vec![vec![i(0), i(99)], vec![i(1), i(100)]]);

    // Window + WHERE + LIMIT: the window runs over all post-WHERE rows.
    let w = rows(
        &db,
        "SELECT id, COUNT(*) OVER () FROM s WHERE grp = 2 LIMIT 2",
    );
    assert_eq!(w, vec![vec![i(2), i(100)], vec![i(5), i(100)]]);

    // In-scan top-N with WHERE + OFFSET, against the no-LIMIT oracle.
    let all = rows(
        &db,
        "SELECT id, name FROM s WHERE grp = 0 ORDER BY name, id DESC",
    );
    let top = rows(
        &db,
        "SELECT id, name FROM s WHERE grp = 0 ORDER BY name, id DESC LIMIT 5 OFFSET 3",
    );
    assert_eq!(top, all[3..8].to_vec());

    // LIMIT 0 and volatile predicates (never pushed into the scan).
    assert!(rows(&db, "SELECT id FROM s ORDER BY name LIMIT 0").is_empty());
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM s WHERE random() < 2.0"),
        vec![vec![i(300)]]
    );
}

/// Streamed shapes inside an open transaction: the overlay (buffered writes)
/// must be visible to pushed-down filters and cutoffs.
#[test]
fn streamed_scan_sees_transaction_overlay() {
    let (_d, db) = open();
    db.execute("CREATE TABLE so (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO so VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    let mut tx = None;
    db.execute_params_in_session("BEGIN", &[], &mut tx).unwrap();
    db.execute_params_in_session("INSERT INTO so VALUES (4, 40)", &[], &mut tx)
        .unwrap();
    db.execute_params_in_session("UPDATE so SET v = 99 WHERE id = 2", &[], &mut tx)
        .unwrap();
    let r = db
        .execute_params_in_session(
            "SELECT id FROM so WHERE v > 25 ORDER BY v DESC LIMIT 2",
            &[],
            &mut tx,
        )
        .unwrap();
    match r.last().unwrap() {
        oxidb_sql::QueryResult::Select { rows, .. } => {
            assert_eq!(
                rows,
                &vec![
                    vec![oxidb_sql::Value::Int(2)],
                    vec![oxidb_sql::Value::Int(4)]
                ],
                "sees the in-txn update (v=99) and insert (v=40)"
            );
        }
        other => panic!("expected Select, got {other:?}"),
    }
    db.execute_params_in_session("ROLLBACK", &[], &mut tx)
        .unwrap();
}
