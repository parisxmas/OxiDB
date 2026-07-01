//! Parameterized queries and per-engine transactions.

mod common;
use common::*;

fn seed(db: &oxidb_sql::SqlEngine) {
    db.execute("CREATE TABLE t (id INT, cust INT, amt INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (10,1,100),(11,1,50),(12,2,200),(13,2,75)")
        .unwrap();
}

// ── parameters ──────────────────────────────────────────────────────────────

#[test]
fn question_mark_params_bind_left_to_right() {
    let (_d, db) = open();
    seed(&db);
    let rws = rows_p(
        &db,
        "SELECT id FROM t WHERE cust = ? AND amt >= ? ORDER BY id",
        &[i(2), i(100)],
    );
    assert_eq!(rws, r1(vec![i(12)]));
}

#[test]
fn dollar_params() {
    let (_d, db) = open();
    seed(&db);
    let rws = rows_p(&db, "SELECT id FROM t WHERE cust = $1 ORDER BY id", &[i(1)]);
    assert_eq!(rws, vec![vec![i(10)], vec![i(11)]]);
}

#[test]
fn dollar_param_reuse() {
    let (_d, db) = open();
    seed(&db);
    // $1 referenced twice.
    let rws = rows_p(
        &db,
        "SELECT id FROM t WHERE cust = $1 OR amt = $1 ORDER BY id",
        &[i(2)],
    );
    // cust = 2 -> ids 12,13 ; amt = 2 -> none
    assert_eq!(rws, vec![vec![i(12)], vec![i(13)]]);
}

#[test]
fn params_in_insert_and_update() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT, v INT)").unwrap();
    assert_eq!(
        affected_p(&db, "INSERT INTO t VALUES (?, ?)", &[i(1), i(10)]),
        1
    );
    assert_eq!(
        affected_p(&db, "UPDATE t SET v = ? WHERE id = ?", &[i(99), i(1)]),
        1
    );
    assert_eq!(rows(&db, "SELECT v FROM t"), r1(vec![i(99)]));
}

#[test]
fn text_param() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'ada'),(2,'bob')")
        .unwrap();
    let rws = rows_p(&db, "SELECT id FROM t WHERE s = ?", &[t("bob")]);
    assert_eq!(rws, r1(vec![i(2)]));
}

#[test]
fn missing_param_errors() {
    let (_d, db) = open();
    seed(&db);
    // Only one param supplied for two placeholders.
    assert!(
        db.execute_params("SELECT id FROM t WHERE cust = ? AND amt = ?", &[i(1)])
            .is_err()
    );
}

// ── transactions ────────────────────────────────────────────────────────────

#[test]
fn commit_is_atomic_and_durable() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = oxidb_sql::SqlEngine::open(dir.path()).unwrap();
        db.execute("CREATE TABLE acct (id INT, bal INT)").unwrap();
        db.execute("INSERT INTO acct VALUES (1,100),(2,0)").unwrap();
        db.execute(
            "BEGIN; UPDATE acct SET bal = bal - 40 WHERE id = 1; \
             UPDATE acct SET bal = bal + 40 WHERE id = 2; COMMIT",
        )
        .unwrap();
    }
    let db = oxidb_sql::SqlEngine::open(dir.path()).unwrap();
    assert_eq!(
        rows(&db, "SELECT bal FROM acct ORDER BY id"),
        vec![vec![i(60)], vec![i(40)]]
    );
}

#[test]
fn rollback_discards() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    db.execute("BEGIN; INSERT INTO t VALUES (2); INSERT INTO t VALUES (3); ROLLBACK")
        .unwrap();
    assert_eq!(rows(&db, "SELECT id FROM t ORDER BY id"), r1(vec![i(1)]));
}

#[test]
fn reads_own_writes() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    let results = db
        .execute(
            "BEGIN; INSERT INTO t VALUES (2, 20); UPDATE t SET v = 99 WHERE id = 1; \
             SELECT v FROM t ORDER BY id; COMMIT",
        )
        .unwrap();
    assert_eq!(first_select(results), vec![vec![i(99)], vec![i(20)]]);
}

#[test]
fn reads_own_deletes() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3)").unwrap();
    let results = db
        .execute("BEGIN; DELETE FROM t WHERE id = 2; SELECT id FROM t ORDER BY id; ROLLBACK")
        .unwrap();
    assert_eq!(first_select(results), vec![vec![i(1)], vec![i(3)]]);
    // After rollback the row is back.
    assert_eq!(rows(&db, "SELECT id FROM t ORDER BY id").len(), 3);
}

#[test]
fn ddl_inside_transaction_commit() {
    let (_d, db) = open();
    let results = db
        .execute(
            "BEGIN; CREATE TABLE tt (id INT); INSERT INTO tt VALUES (1); SELECT id FROM tt; COMMIT",
        )
        .unwrap();
    assert_eq!(first_select(results), r1(vec![i(1)]));
    // Table persists after commit.
    assert_eq!(rows(&db, "SELECT id FROM tt"), r1(vec![i(1)]));
}

#[test]
fn ddl_inside_transaction_rollback() {
    let (_d, db) = open();
    db.execute("BEGIN; CREATE TABLE tt (id INT); ROLLBACK")
        .unwrap();
    assert!(!db.table_names().contains(&"tt".to_string()));
}

#[test]
fn uncommitted_at_end_is_rolled_back() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT)").unwrap();
    db.execute("BEGIN; INSERT INTO t VALUES (7)").unwrap();
    assert!(rows(&db, "SELECT id FROM t").is_empty());
}

#[test]
fn nested_begin_and_orphan_commit_rollback_error() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT)").unwrap();
    assert!(db.execute("BEGIN; BEGIN").is_err());
    assert!(db.execute("COMMIT").is_err());
    assert!(db.execute("ROLLBACK").is_err());
}

#[test]
fn sequential_transactions() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT)").unwrap();
    db.execute("BEGIN; INSERT INTO t VALUES (1); COMMIT")
        .unwrap();
    db.execute("BEGIN; INSERT INTO t VALUES (2); COMMIT")
        .unwrap();
    assert_eq!(
        rows(&db, "SELECT id FROM t ORDER BY id"),
        vec![vec![i(1)], vec![i(2)]]
    );
}

#[test]
fn transaction_row_ids_do_not_collide_after_commit() {
    // Insert one row (autocommit), then insert more inside a transaction; all
    // rows must survive commit (row-id seeding continues the engine sequence).
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    db.execute("BEGIN; INSERT INTO t VALUES (2); INSERT INTO t VALUES (3); COMMIT")
        .unwrap();
    assert_eq!(
        rows(&db, "SELECT id FROM t ORDER BY id"),
        vec![vec![i(1)], vec![i(2)], vec![i(3)]]
    );
}
