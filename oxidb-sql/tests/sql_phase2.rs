//! Phase 2 end-to-end tests: inner joins, aggregation, parameterized queries,
//! secondary indexes, and per-engine transactions.

use oxidb_sql::{QueryResult, SqlEngine, Value};

fn open(dir: &std::path::Path) -> SqlEngine {
    SqlEngine::open(dir).unwrap()
}

fn select(db: &SqlEngine, sql: &str) -> (Vec<String>, Vec<Vec<Value>>) {
    let mut r = db.execute(sql).unwrap();
    assert_eq!(r.len(), 1, "expected one statement: {sql}");
    match r.pop().unwrap() {
        QueryResult::Select { columns, rows, .. } => (columns, rows),
        other => panic!("expected Select, got {other:?}"),
    }
}

fn select_p(db: &SqlEngine, sql: &str, params: &[Value]) -> Vec<Vec<Value>> {
    let mut r = db.execute_params(sql, params).unwrap();
    match r.pop().unwrap() {
        QueryResult::Select { rows, .. } => rows,
        other => panic!("expected Select, got {other:?}"),
    }
}

fn seed_orders(db: &SqlEngine) {
    db.execute("CREATE TABLE customers (id INT PRIMARY KEY, name TEXT NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE orders (id INT, cust INT, amount INT)")
        .unwrap();
    db.execute("INSERT INTO customers VALUES (1, 'ada'), (2, 'bob'), (3, 'cy')")
        .unwrap();
    db.execute(
        "INSERT INTO orders VALUES (10, 1, 100), (11, 1, 50), (12, 2, 200), (13, 2, 25), (14, 2, 75)",
    )
    .unwrap();
}

#[test]
fn inner_join_with_aliases() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    seed_orders(&db);

    let (cols, rows) = select(
        &db,
        "SELECT c.name AS who, o.amount AS amt \
         FROM customers c JOIN orders o ON c.id = o.cust \
         WHERE o.amount >= 100 ORDER BY o.amount DESC",
    );
    assert_eq!(cols, vec!["who", "amt"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::Text("bob".into()), Value::Int(200)],
            vec![Value::Text("ada".into()), Value::Int(100)],
        ]
    );
}

#[test]
fn join_produces_no_rows_for_unmatched() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    seed_orders(&db);
    // cy (id 3) has no orders -> inner join excludes them.
    let (_c, rows) = select(
        &db,
        "SELECT c.name FROM customers c JOIN orders o ON c.id = o.cust WHERE c.id = 3",
    );
    assert!(rows.is_empty());
}

#[test]
fn group_by_with_aggregates() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    seed_orders(&db);

    let (cols, rows) = select(
        &db,
        "SELECT cust, COUNT(*) AS n, SUM(amount) AS total, MIN(amount) AS lo, MAX(amount) AS hi \
         FROM orders GROUP BY cust ORDER BY cust",
    );
    assert_eq!(cols, vec!["cust", "n", "total", "lo", "hi"]);
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Int(1),
                Value::Int(2),
                Value::Int(150),
                Value::Int(50),
                Value::Int(100)
            ],
            vec![
                Value::Int(2),
                Value::Int(3),
                Value::Int(300),
                Value::Int(25),
                Value::Int(200)
            ],
        ]
    );
}

#[test]
fn aggregate_no_group_over_whole_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    seed_orders(&db);
    let (_c, rows) = select(&db, "SELECT COUNT(*) AS n, AVG(amount) AS avg FROM orders");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Int(5));
    assert_eq!(rows[0][1], Value::Double(90.0)); // (100+50+200+25+75)/5
}

#[test]
fn having_filters_groups() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    seed_orders(&db);
    // Only customers whose order count > 2 (cust 2 has 3).
    let (_c, rows) = select(
        &db,
        "SELECT cust FROM orders GROUP BY cust HAVING COUNT(*) > 2 ORDER BY cust",
    );
    assert_eq!(rows, vec![vec![Value::Int(2)]]);
}

#[test]
fn parameterized_queries() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    seed_orders(&db);

    // `?` placeholders bind left-to-right.
    let rows = select_p(
        &db,
        "SELECT id FROM orders WHERE cust = ? AND amount >= ? ORDER BY id",
        &[Value::Int(2), Value::Int(75)],
    );
    assert_eq!(rows, vec![vec![Value::Int(12)], vec![Value::Int(14)]]);

    // `$N` placeholders.
    let rows = select_p(
        &db,
        "SELECT id FROM orders WHERE cust = $1 ORDER BY id",
        &[Value::Int(1)],
    );
    assert_eq!(rows, vec![vec![Value::Int(10)], vec![Value::Int(11)]]);
}

#[test]
fn index_returns_same_results_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open(dir.path());
        db.execute("CREATE TABLE t (id INT, tag TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'a'),(4,'c'),(5,'a')")
            .unwrap();
        db.execute("CREATE INDEX t_tag ON t(tag)").unwrap();

        // Index-served equality lookup returns correct rows.
        let (_c, rows) = select(&db, "SELECT id FROM t WHERE tag = 'a' ORDER BY id");
        assert_eq!(
            rows,
            vec![
                vec![Value::Int(1)],
                vec![Value::Int(3)],
                vec![Value::Int(5)]
            ]
        );

        // Mutations keep the index consistent.
        db.execute("DELETE FROM t WHERE id = 3").unwrap();
        db.execute("UPDATE t SET tag = 'a' WHERE id = 4").unwrap();
        let (_c, rows) = select(&db, "SELECT id FROM t WHERE tag = 'a' ORDER BY id");
        assert_eq!(
            rows,
            vec![
                vec![Value::Int(1)],
                vec![Value::Int(4)],
                vec![Value::Int(5)]
            ]
        );
        db.checkpoint().unwrap();
    }
    // Index definition persists and is rebuilt on reopen.
    let db = open(dir.path());
    let (_c, rows) = select(&db, "SELECT id FROM t WHERE tag = 'a' ORDER BY id");
    assert_eq!(
        rows,
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(4)],
            vec![Value::Int(5)]
        ]
    );
}

#[test]
fn transaction_commit_is_atomic_and_durable() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open(dir.path());
        db.execute("CREATE TABLE acct (id INT, bal INT)").unwrap();
        db.execute("INSERT INTO acct VALUES (1, 100), (2, 0)")
            .unwrap();
        // Transfer 40 from acct 1 to acct 2 atomically.
        db.execute(
            "BEGIN; \
             UPDATE acct SET bal = bal - 40 WHERE id = 1; \
             UPDATE acct SET bal = bal + 40 WHERE id = 2; \
             COMMIT",
        )
        .unwrap();
        let (_c, rows) = select(&db, "SELECT bal FROM acct ORDER BY id");
        assert_eq!(rows, vec![vec![Value::Int(60)], vec![Value::Int(40)]]);
        // no checkpoint -> committed batch must be recovered from the WAL
    }
    let db = open(dir.path());
    let (_c, rows) = select(&db, "SELECT bal FROM acct ORDER BY id");
    assert_eq!(rows, vec![vec![Value::Int(60)], vec![Value::Int(40)]]);
}

#[test]
fn transaction_rollback_discards_writes() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    db.execute("CREATE TABLE t (id INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();

    db.execute("BEGIN; INSERT INTO t VALUES (2); INSERT INTO t VALUES (3); ROLLBACK")
        .unwrap();
    let (_c, rows) = select(&db, "SELECT id FROM t ORDER BY id");
    assert_eq!(rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn transaction_reads_its_own_writes() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    db.execute("CREATE TABLE t (id INT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    // Within the transaction, the SELECT must see the buffered insert+update.
    let results = db
        .execute(
            "BEGIN; \
             INSERT INTO t VALUES (2, 20); \
             UPDATE t SET v = 99 WHERE id = 1; \
             SELECT v FROM t ORDER BY id; \
             COMMIT",
        )
        .unwrap();
    // Find the SELECT result among the batch.
    let select_res = results
        .into_iter()
        .find_map(|r| match r {
            QueryResult::Select { rows, .. } => Some(rows),
            _ => None,
        })
        .unwrap();
    assert_eq!(select_res, vec![vec![Value::Int(99)], vec![Value::Int(20)]]);
}

#[test]
fn uncommitted_transaction_at_end_is_rolled_back() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    db.execute("CREATE TABLE t (id INT)").unwrap();
    // BEGIN without COMMIT: the insert must be discarded.
    db.execute("BEGIN; INSERT INTO t VALUES (7)").unwrap();
    let (_c, rows) = select(&db, "SELECT id FROM t");
    assert!(rows.is_empty());
}
