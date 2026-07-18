//! `SELECT ... FOR UPDATE` — real pessimistic row locks, not accepted-and-
//! ignored syntax. The load-bearing claims: a FOR UPDATE holder excludes
//! concurrent writers until commit/rollback; plain UPDATEs exclude each
//! other (the lost-update hole is closed); contention past the lock timeout
//! is an error, not a hang; and every shape whose rows are not base-table
//! rows is refused by name instead of silently not locking.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use oxidb_sql::{SqlEngine, SqlOptions, Value};

fn engine_with_timeout(dir: &std::path::Path, lock_timeout_ms: u64) -> SqlEngine {
    let opts = SqlOptions {
        lock_timeout_ms,
        ..SqlOptions::default()
    };
    SqlEngine::open_with_options(dir, opts).unwrap()
}

fn seed(db: &SqlEngine) {
    db.execute("CREATE TABLE products (id INT PRIMARY KEY, name TEXT, stock INT)")
        .unwrap();
    db.execute(
        "INSERT INTO products (id, name, stock) VALUES (42, 'widget', 10), (43, 'gadget', 5)",
    )
    .unwrap();
}

/// Run `sql` statements one call at a time in one interactive session.
struct Session<'a> {
    db: &'a SqlEngine,
    tx: Option<u64>,
}

impl<'a> Session<'a> {
    fn new(db: &'a SqlEngine) -> Self {
        Session { db, tx: None }
    }
    fn run(&mut self, sql: &str) -> oxidb_sql::Result<Vec<oxidb_sql::QueryResult>> {
        self.db.execute_params_in_session(sql, &[], &mut self.tx)
    }
}

#[test]
fn for_update_blocks_a_concurrent_update_until_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(engine_with_timeout(dir.path(), 5_000));
    seed(&db);

    let mut t1 = Session::new(&db);
    t1.run("BEGIN").unwrap();
    t1.run("SELECT * FROM products WHERE id = 42 FOR UPDATE")
        .unwrap();

    let committed = Arc::new(AtomicBool::new(false));
    let writer = {
        let db = Arc::clone(&db);
        let committed = Arc::clone(&committed);
        std::thread::spawn(move || {
            let t0 = Instant::now();
            db.execute("UPDATE products SET name = 'stolen' WHERE id = 42")
                .unwrap();
            // The write must only have gone through after the holder
            // committed — the whole point of the lock.
            assert!(
                committed.load(Ordering::SeqCst),
                "the concurrent UPDATE went through while FOR UPDATE was held"
            );
            t0.elapsed()
        })
    };

    std::thread::sleep(Duration::from_millis(300));
    committed.store(true, Ordering::SeqCst);
    t1.run("COMMIT").unwrap();

    let waited = writer.join().unwrap();
    assert!(
        waited >= Duration::from_millis(250),
        "the writer should have blocked on the row lock, waited only {waited:?}"
    );
    let rows = db
        .execute("SELECT name FROM products WHERE id = 42")
        .unwrap();
    match &rows[0] {
        oxidb_sql::QueryResult::Select { rows, .. } => {
            assert_eq!(rows[0][0], Value::Text("stolen".into()))
        }
        other => panic!("unexpected result {other:?}"),
    }
}

#[test]
fn two_updates_on_the_same_row_serialize_no_lost_update() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(engine_with_timeout(dir.path(), 5_000));
    seed(&db);

    // T1 buffers an UPDATE in an open transaction — the row is now locked.
    let mut t1 = Session::new(&db);
    t1.run("BEGIN").unwrap();
    t1.run("UPDATE products SET stock = stock - 1 WHERE id = 42")
        .unwrap();

    // T2's autocommit decrement must WAIT for T1, then apply on top of T1's
    // committed value: 10 - 1 - 1 = 8, not the lost-update 9.
    let writer = {
        let db = Arc::clone(&db);
        std::thread::spawn(move || {
            db.execute("UPDATE products SET stock = stock - 1 WHERE id = 42")
                .unwrap();
        })
    };
    std::thread::sleep(Duration::from_millis(200));
    t1.run("COMMIT").unwrap();
    writer.join().unwrap();

    let rows = db
        .execute("SELECT stock FROM products WHERE id = 42")
        .unwrap();
    match &rows[0] {
        oxidb_sql::QueryResult::Select { rows, .. } => {
            assert_eq!(
                rows[0][0],
                Value::Int(8),
                "both decrements must land — 9 means the second read a stale stock"
            );
        }
        other => panic!("unexpected result {other:?}"),
    }
}

#[test]
fn contention_past_the_timeout_is_an_error_not_a_hang() {
    let dir = tempfile::tempdir().unwrap();
    let db = engine_with_timeout(dir.path(), 150);
    seed(&db);

    let mut t1 = Session::new(&db);
    t1.run("BEGIN").unwrap();
    t1.run("SELECT * FROM products WHERE id = 42 FOR UPDATE")
        .unwrap();

    let t0 = Instant::now();
    let err = db
        .execute("UPDATE products SET name = 'x' WHERE id = 42")
        .unwrap_err();
    let waited = t0.elapsed();
    assert!(
        err.to_string().contains("lock timeout"),
        "expected a lock timeout, got: {err}"
    );
    assert!(
        waited >= Duration::from_millis(120) && waited < Duration::from_secs(2),
        "the timeout should bite at ~150ms, took {waited:?}"
    );
    t1.run("ROLLBACK").unwrap();
    // Rollback released the lock: the same UPDATE now succeeds.
    db.execute("UPDATE products SET name = 'x' WHERE id = 42")
        .unwrap();
}

#[test]
fn autocommit_for_update_releases_at_statement_end() {
    let dir = tempfile::tempdir().unwrap();
    let db = engine_with_timeout(dir.path(), 150);
    seed(&db);
    db.execute("SELECT * FROM products WHERE id = 42 FOR UPDATE")
        .unwrap();
    // No transaction held it open — the lock died with the statement.
    db.execute("UPDATE products SET name = 'fine' WHERE id = 42")
        .unwrap();
}

#[test]
fn a_transaction_does_not_deadlock_against_itself() {
    let dir = tempfile::tempdir().unwrap();
    let db = engine_with_timeout(dir.path(), 500);
    seed(&db);
    let mut t1 = Session::new(&db);
    t1.run("BEGIN").unwrap();
    t1.run("SELECT * FROM products WHERE id = 42 FOR UPDATE")
        .unwrap();
    t1.run("SELECT * FROM products WHERE id = 42 FOR UPDATE")
        .unwrap();
    t1.run("UPDATE products SET stock = 99 WHERE id = 42")
        .unwrap();
    t1.run("DELETE FROM products WHERE id = 42").unwrap();
    t1.run("COMMIT").unwrap();
}

#[test]
fn a_failed_statement_rolls_back_and_releases_the_locks() {
    let dir = tempfile::tempdir().unwrap();
    let db = engine_with_timeout(dir.path(), 150);
    seed(&db);
    let mut t1 = Session::new(&db);
    t1.run("BEGIN").unwrap();
    t1.run("SELECT * FROM products WHERE id = 42 FOR UPDATE")
        .unwrap();
    // A statement error aborts the transaction…
    t1.run("SELECT nope FROM products").unwrap_err();
    // …and the lock must be gone with it.
    db.execute("UPDATE products SET name = 'free' WHERE id = 42")
        .unwrap();
}

#[test]
fn shapes_that_cannot_lock_base_rows_are_refused_by_name() {
    let dir = tempfile::tempdir().unwrap();
    let db = engine_with_timeout(dir.path(), 150);
    seed(&db);
    db.execute("CREATE TABLE other (id INT PRIMARY KEY)")
        .unwrap();

    for (sql, what) in [
        (
            "SELECT p.id FROM products p JOIN other o ON o.id = p.id FOR UPDATE",
            "join",
        ),
        ("SELECT COUNT(*) FROM products FOR UPDATE", "aggregate"),
        ("SELECT DISTINCT name FROM products FOR UPDATE", "DISTINCT"),
        (
            "SELECT id FROM products UNION SELECT id FROM other FOR UPDATE",
            "set operation",
        ),
        (
            "SELECT * FROM (SELECT * FROM products) d FOR UPDATE",
            "derived table",
        ),
    ] {
        let err = db.execute(sql).unwrap_err().to_string();
        assert!(
            err.contains("FOR UPDATE"),
            "{what}: the refusal must name FOR UPDATE, got: {err}"
        );
    }

    // Other locking clauses are refused at parse time, not half-honored.
    let err = db
        .execute("SELECT * FROM products FOR SHARE")
        .unwrap_err()
        .to_string();
    assert!(err.to_lowercase().contains("locking clause") || err.to_lowercase().contains("share"));
}

#[test]
fn for_update_is_not_read_only_for_the_router() {
    // oxipool routes read-only SQL to replicas, where a lock would be
    // theater: FOR UPDATE must classify as a write.
    assert!(oxidb_sql::is_read_only("SELECT * FROM t WHERE id = 1").unwrap());
    assert!(!oxidb_sql::is_read_only("SELECT * FROM t WHERE id = 1 FOR UPDATE").unwrap());
}
