//! Savepoints inside stored procedures (SQL-text `SAVEPOINT`/`ROLLBACK TO`
//! and the Cobra `db.savepoint`/`db.rollback_to` handle methods). Interactive
//! savepoints predate this and are covered elsewhere; these pin the in-body
//! path that ADR-0014 procedures need for nested error recovery.

use oxidb_sql::SqlEngine;

fn eng() -> SqlEngine {
    let dir = tempfile::tempdir().unwrap();
    let e = SqlEngine::open(dir.path()).unwrap();
    std::mem::forget(dir);
    e
}

fn run(e: &SqlEngine, sql: &str) {
    e.execute(sql).unwrap_or_else(|err| panic!("{sql}: {err}"));
}

fn count(e: &SqlEngine) -> i64 {
    let mut r = e.execute("SELECT COUNT(*) AS n FROM t").unwrap();
    match r.remove(0) {
        oxidb_sql::QueryResult::Select { rows, .. } => match &rows[0][0] {
            oxidb_sql::Value::Int(n) => *n,
            v => panic!("count not int: {v:?}"),
        },
        other => panic!("not a select: {other:?}"),
    }
}

#[test]
fn sql_text_procedure_savepoint_rolls_back_part() {
    let e = eng();
    run(&e, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
    // Insert 5, savepoint, insert 6, roll back to the savepoint, insert 7.
    run(
        &e,
        "CREATE PROCEDURE p() AS BEGIN \
         INSERT INTO t VALUES (5, 'p'); \
         SAVEPOINT a; \
         INSERT INTO t VALUES (6, 'q'); \
         ROLLBACK TO SAVEPOINT a; \
         INSERT INTO t VALUES (7, 'r'); END",
    );
    run(&e, "CALL p()");
    assert_eq!(count(&e), 2, "6 must be rolled back, 5 and 7 kept");
}

#[test]
fn savepoint_outside_a_transaction_errors() {
    let e = eng();
    run(&e, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
    // A bare SAVEPOINT with no BEGIN and no procedure has no transaction.
    let err = e.execute("SAVEPOINT lonely").unwrap_err().to_string();
    assert!(err.contains("SAVEPOINT"), "got: {err}");
}

#[test]
fn nested_call_sql_to_sql_shares_transaction() {
    let e = eng();
    run(&e, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
    run(
        &e,
        "CREATE PROCEDURE leaf(x INT) AS BEGIN INSERT INTO t VALUES (x, 'z'); END",
    );
    run(
        &e,
        "CREATE PROCEDURE parent() AS BEGIN CALL leaf(1); CALL leaf(2); END",
    );
    run(&e, "CALL parent()");
    assert_eq!(count(&e), 2);
}

#[test]
fn nested_call_recursion_is_bounded() {
    let e = eng();
    run(&e, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
    // Self-recursive with no base case → the depth guard must stop it.
    run(
        &e,
        "CREATE PROCEDURE loop_forever() AS BEGIN CALL loop_forever(); END",
    );
    let err = e.execute("CALL loop_forever()").unwrap_err().to_string();
    assert!(err.contains("call depth"), "got: {err}");
}
