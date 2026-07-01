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
fn table_alias_qualified_columns() {
    let (_d, db) = open();
    seed(&db);
    assert_eq!(
        rows(&db, "SELECT x.id FROM t x WHERE x.v = 20"),
        r1(vec![i(3)])
    );
}
