//! BETWEEN — found missing by the TLP oracle on its first run.
//!
//! It is standard SQL, every ORM emits it, and the engine rejected it outright
//! with `unsupported sql` while the identical `>= AND <=` worked. It is
//! desugared in the parser, so what these really pin is that the desugaring
//! keeps SQL's three-valued logic: a NULL operand must make the whole thing
//! NULL, not FALSE — which is the difference between "excluded from the answer"
//! and "excluded from BOTH the answer and its negation".

mod common;

use common::*;
use oxidb_sql::Value;

fn setup() -> (tempfile::TempDir, oxidb_sql::SqlEngine) {
    let (dir, db) = open();
    db.execute("CREATE TABLE t (id INT, a INT, s TEXT)").unwrap();
    for (id, a) in [(1, "1"), (2, "5"), (3, "9"), (4, "NULL")] {
        db.execute(&format!("INSERT INTO t (id, a) VALUES ({id}, {a})")).unwrap();
    }
    (dir, db)
}

fn ids(db: &oxidb_sql::SqlEngine, sql: &str) -> Vec<i64> {
    let mut v: Vec<i64> = rows(db, sql)
        .iter()
        .map(|r| match r[0] {
            Value::Int(n) => n,
            ref other => panic!("expected an id, got {other:?}"),
        })
        .collect();
    v.sort();
    v
}

#[test]
fn between_is_inclusive_at_both_ends() {
    let (_d, db) = setup();
    assert_eq!(ids(&db, "SELECT id FROM t WHERE a BETWEEN 1 AND 9"), vec![1, 2, 3]);
    assert_eq!(ids(&db, "SELECT id FROM t WHERE a BETWEEN 5 AND 5"), vec![2]);
    assert_eq!(ids(&db, "SELECT id FROM t WHERE a BETWEEN 2 AND 8"), vec![2]);
}

#[test]
fn an_empty_range_matches_nothing() {
    let (_d, db) = setup();
    // low > high is not an error, it is simply unsatisfiable.
    assert_eq!(ids(&db, "SELECT id FROM t WHERE a BETWEEN 8 AND 2"), Vec::<i64>::new());
}

#[test]
fn not_between_excludes_the_range() {
    let (_d, db) = setup();
    assert_eq!(ids(&db, "SELECT id FROM t WHERE a NOT BETWEEN 2 AND 8"), vec![1, 3]);
}

#[test]
fn a_null_operand_is_null_in_both_directions() {
    let (_d, db) = setup();
    // Row 4 (a IS NULL) appears in NEITHER. If the desugaring produced FALSE
    // instead of NULL, NOT BETWEEN would wrongly include it — and that is the
    // exact class of bug the TLP oracle exists to catch.
    assert!(!ids(&db, "SELECT id FROM t WHERE a BETWEEN 1 AND 9").contains(&4));
    assert!(!ids(&db, "SELECT id FROM t WHERE a NOT BETWEEN 1 AND 9").contains(&4));

    // And it is genuinely NULL, not merely absent from both.
    assert_eq!(ids(&db, "SELECT id FROM t WHERE (a BETWEEN 1 AND 9) IS NULL"), vec![4]);
    assert_eq!(ids(&db, "SELECT id FROM t WHERE (a NOT BETWEEN 1 AND 9) IS NULL"), vec![4]);
}

#[test]
fn between_partitions_the_table_ternary() {
    let (_d, db) = setup();
    // TLP in miniature, written by hand: every row lands in exactly one bucket.
    let t = ids(&db, "SELECT id FROM t WHERE a BETWEEN 2 AND 8");
    let f = ids(&db, "SELECT id FROM t WHERE NOT (a BETWEEN 2 AND 8)");
    let n = ids(&db, "SELECT id FROM t WHERE (a BETWEEN 2 AND 8) IS NULL");
    let mut all: Vec<i64> = t.iter().chain(f.iter()).chain(n.iter()).copied().collect();
    all.sort();
    assert_eq!(all, vec![1, 2, 3, 4], "every row belongs to exactly one partition");
}

#[test]
fn between_agrees_with_the_comparison_it_desugars_to() {
    let (_d, db) = setup();
    for (lo, hi) in [(1, 9), (2, 8), (5, 5), (0, 100), (8, 2)] {
        assert_eq!(
            ids(&db, &format!("SELECT id FROM t WHERE a BETWEEN {lo} AND {hi}")),
            ids(&db, &format!("SELECT id FROM t WHERE a >= {lo} AND a <= {hi}")),
            "BETWEEN {lo} AND {hi} must equal its own definition"
        );
        assert_eq!(
            ids(&db, &format!("SELECT id FROM t WHERE a NOT BETWEEN {lo} AND {hi}")),
            ids(&db, &format!("SELECT id FROM t WHERE a < {lo} OR a > {hi}")),
            "NOT BETWEEN {lo} AND {hi} must equal its own definition"
        );
    }
}

#[test]
fn between_works_on_text_and_on_expressions() {
    let (_d, db) = setup();
    db.execute("INSERT INTO t (id, s) VALUES (10, 'bbb')").unwrap();
    db.execute("INSERT INTO t (id, s) VALUES (11, 'zzz')").unwrap();
    assert_eq!(ids(&db, "SELECT id FROM t WHERE s BETWEEN 'a' AND 'c'"), vec![10]);
    // The operand does not have to be a bare column.
    assert_eq!(ids(&db, "SELECT id FROM t WHERE a + 1 BETWEEN 2 AND 6"), vec![1, 2]);
}
