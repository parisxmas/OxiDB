//! Concurrent writers vs. an open transaction.
//!
//! A transaction buffers its writes and flushes them as one batch at COMMIT,
//! so everything it allocates (row ids, auto-increment values) and everything
//! it checked (uniqueness) has to survive whatever other writers commit while
//! it is still open. The load-bearing claims: a buffered insert never takes a
//! row id another writer can also be handed, and a commit whose keys were
//! taken in the meantime is refused rather than silently duplicating them.

mod common;

use common::*;
use oxidb_sql::{QueryResult, SqlEngine, Value};

fn sess(db: &SqlEngine, tx: &mut Option<u64>, sql: &str) -> oxidb_sql::Result<Vec<QueryResult>> {
    db.execute_params_in_session(sql, &[], tx)
}

#[test]
fn an_autocommit_insert_survives_a_concurrent_transaction() {
    // The reported bug: the transaction seeded its row-id allocator at its
    // first insert and never reserved those ids, so the autocommit row took
    // the same id and the COMMIT overwrote it — the row vanished with no
    // error anywhere.
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, who TEXT)")
        .unwrap();

    let mut tx = None;
    sess(&db, &mut tx, "BEGIN").unwrap();
    sess(&db, &mut tx, "INSERT INTO t VALUES (1, 'txn')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'autocommit')")
        .unwrap();
    sess(&db, &mut tx, "COMMIT").unwrap();

    assert_eq!(
        rows(&db, "SELECT id, who FROM t ORDER BY id"),
        vec![vec![i(1), t("txn")], vec![i(2), t("autocommit")]]
    );
}

#[test]
fn two_transactions_inserting_into_one_table_both_land() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, who TEXT)")
        .unwrap();

    let (mut a, mut b) = (None, None);
    sess(&db, &mut a, "BEGIN").unwrap();
    sess(&db, &mut b, "BEGIN").unwrap();
    sess(&db, &mut a, "INSERT INTO t VALUES (1, 'a')").unwrap();
    sess(&db, &mut b, "INSERT INTO t VALUES (2, 'b')").unwrap();
    sess(&db, &mut a, "COMMIT").unwrap();
    sess(&db, &mut b, "COMMIT").unwrap();

    assert_eq!(
        rows(&db, "SELECT id, who FROM t ORDER BY id"),
        vec![vec![i(1), t("a")], vec![i(2), t("b")]]
    );
}

#[test]
fn many_rows_across_interleaved_writers() {
    // Interleave a multi-row transaction with autocommit inserts: every row
    // any writer was told it wrote must be there, exactly once.
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();

    let mut tx = None;
    sess(&db, &mut tx, "BEGIN").unwrap();
    for n in 1..=5 {
        sess(&db, &mut tx, &format!("INSERT INTO t VALUES ({n})")).unwrap();
        db.execute(&format!("INSERT INTO t VALUES ({})", n + 100))
            .unwrap();
    }
    sess(&db, &mut tx, "COMMIT").unwrap();

    let got: Vec<i64> = rows(&db, "SELECT id FROM t ORDER BY id")
        .into_iter()
        .map(|r| match r[0] {
            Value::Int(n) => n,
            _ => panic!("int expected"),
        })
        .collect();
    let want: Vec<i64> = (1..=5).chain(101..=105).collect();
    assert_eq!(got, want);
}

#[test]
fn a_rolled_back_transaction_leaves_the_other_writer_alone() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();

    let mut tx = None;
    sess(&db, &mut tx, "BEGIN").unwrap();
    sess(&db, &mut tx, "INSERT INTO t VALUES (1)").unwrap();
    db.execute("INSERT INTO t VALUES (2)").unwrap();
    sess(&db, &mut tx, "ROLLBACK").unwrap();

    // Only the committed row, and the reserved-then-abandoned id is simply a
    // gap — the next insert must not reuse it in a way that overwrites.
    assert_eq!(rows(&db, "SELECT id FROM t"), vec![vec![i(2)]]);
    db.execute("INSERT INTO t VALUES (3)").unwrap();
    assert_eq!(
        rows(&db, "SELECT id FROM t ORDER BY id"),
        vec![vec![i(2)], vec![i(3)]]
    );
}

#[test]
fn auto_increment_values_are_not_handed_out_twice() {
    // Same reservation rule for the AUTO_INCREMENT counter: a transaction that
    // draws from it must not hand a concurrent writer the same value.
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, who TEXT)")
        .unwrap();

    let mut tx = None;
    sess(&db, &mut tx, "BEGIN").unwrap();
    sess(&db, &mut tx, "INSERT INTO t (who) VALUES ('txn')").unwrap();
    db.execute("INSERT INTO t (who) VALUES ('autocommit')")
        .unwrap();
    sess(&db, &mut tx, "COMMIT").unwrap();

    let ids: Vec<Vec<Value>> = rows(&db, "SELECT id FROM t ORDER BY id");
    assert_eq!(ids.len(), 2, "both rows survive: {ids:?}");
    assert_ne!(ids[0], ids[1], "and got distinct ids: {ids:?}");
    assert_eq!(rows(&db, "SELECT COUNT(*) FROM t"), r1(vec![i(2)]));
}

#[test]
fn a_commit_whose_key_was_taken_meanwhile_is_refused() {
    // The transaction's own uniqueness check passed when it buffered the
    // write; by COMMIT another writer owns that key. Refusing is the only
    // honest answer — the alternative is two rows with one primary key.
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, who TEXT)")
        .unwrap();

    let mut tx = None;
    sess(&db, &mut tx, "BEGIN").unwrap();
    sess(&db, &mut tx, "INSERT INTO t VALUES (1, 'txn')").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'autocommit')")
        .unwrap();
    let e = sess(&db, &mut tx, "COMMIT").unwrap_err().to_string();
    assert!(e.contains("PRIMARY KEY"), "{e}");

    // The committed row stands, alone.
    assert_eq!(
        rows(&db, "SELECT id, who FROM t"),
        vec![vec![i(1), t("autocommit")]]
    );
}

#[test]
fn a_commit_whose_unique_value_was_taken_meanwhile_is_refused() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, mail TEXT UNIQUE)")
        .unwrap();

    let mut tx = None;
    sess(&db, &mut tx, "BEGIN").unwrap();
    sess(&db, &mut tx, "INSERT INTO t VALUES (1, 'a@x')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'a@x')").unwrap();
    let e = sess(&db, &mut tx, "COMMIT").unwrap_err().to_string();
    assert!(e.contains("UNIQUE"), "{e}");
    assert_eq!(rows(&db, "SELECT COUNT(*) FROM t"), r1(vec![i(1)]));
}

#[test]
fn a_key_the_batch_itself_frees_is_still_reusable() {
    // The commit-time re-check simulates the batch in order, so a row the
    // batch deletes no longer owns its key — this must not become a false
    // duplicate-key refusal.
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    let mut tx = None;
    sess(&db, &mut tx, "BEGIN").unwrap();
    sess(&db, &mut tx, "DELETE FROM t WHERE id = 1").unwrap();
    sess(&db, &mut tx, "INSERT INTO t VALUES (1, 20)").unwrap();
    sess(&db, &mut tx, "COMMIT").unwrap();
    assert_eq!(rows(&db, "SELECT id, v FROM t"), vec![vec![i(1), i(20)]]);

    // Likewise a row the batch rewrites: moving a key and reusing it.
    sess(&db, &mut tx, "BEGIN").unwrap();
    sess(&db, &mut tx, "UPDATE t SET id = 2 WHERE id = 1").unwrap();
    sess(&db, &mut tx, "INSERT INTO t VALUES (1, 30)").unwrap();
    sess(&db, &mut tx, "COMMIT").unwrap();
    assert_eq!(
        rows(&db, "SELECT id, v FROM t ORDER BY id"),
        vec![vec![i(1), i(30)], vec![i(2), i(20)]]
    );
}

#[test]
fn a_table_created_in_the_batch_commits() {
    // A table the batch creates has no committed state to re-check against;
    // the walk must skip it rather than trip over the missing maps.
    let (_d, db) = open();
    let mut tx = None;
    sess(&db, &mut tx, "BEGIN").unwrap();
    sess(
        &db,
        &mut tx,
        "CREATE TABLE fresh (id INT PRIMARY KEY, v INT)",
    )
    .unwrap();
    sess(&db, &mut tx, "INSERT INTO fresh VALUES (1, 1), (2, 2)").unwrap();
    sess(&db, &mut tx, "COMMIT").unwrap();
    assert_eq!(rows(&db, "SELECT COUNT(*) FROM fresh"), r1(vec![i(2)]));
}

#[test]
fn writes_survive_a_restart() {
    // The batch is one WAL record; reserving ids from the engine must not
    // change what replay reconstructs.
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open_at(dir.path());
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, who TEXT)")
            .unwrap();
        let mut tx = None;
        db.execute_params_in_session("BEGIN", &[], &mut tx).unwrap();
        db.execute_params_in_session("INSERT INTO t VALUES (1, 'txn')", &[], &mut tx)
            .unwrap();
        db.execute("INSERT INTO t VALUES (2, 'autocommit')")
            .unwrap();
        db.execute_params_in_session("COMMIT", &[], &mut tx)
            .unwrap();
    }
    let db = open_at(dir.path());
    assert_eq!(
        rows(&db, "SELECT id, who FROM t ORDER BY id"),
        vec![vec![i(1), t("txn")], vec![i(2), t("autocommit")]]
    );
    // And the reloaded row-id allocator still hands out fresh ids.
    db.execute("INSERT INTO t VALUES (3, 'after restart')")
        .unwrap();
    assert_eq!(rows(&db, "SELECT COUNT(*) FROM t"), r1(vec![i(3)]));
}
