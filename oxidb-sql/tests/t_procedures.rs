//! SQL stored procedures: `CREATE [OR ALTER] PROCEDURE`, `CALL`,
//! `DROP PROCEDURE`, `SHOW PROCEDURES`. Bodies are DML/SELECT batches with
//! named parameters; a CALL is atomic (implicit transaction at top level,
//! joins the open transaction otherwise).

mod common;

use common::*;
use oxidb_sql::{QueryResult, Value};

fn t(s: &str) -> Value {
    Value::Text(s.to_string().into())
}

fn setup(db: &oxidb_sql::SqlEngine) {
    db.execute("CREATE TABLE hesap (id INT PRIMARY KEY AUTO_INCREMENT, ad TEXT, bakiye DOUBLE)")
        .unwrap();
    db.execute("INSERT INTO hesap (ad, bakiye) VALUES ('ali', 100), ('ayse', 50)")
        .unwrap();
}

#[test]
fn create_call_basics() {
    let (_d, db) = open();
    setup(&db);
    db.execute(
        "CREATE PROCEDURE yatir(kime TEXT, tutar DOUBLE) AS BEGIN
           UPDATE hesap SET bakiye = bakiye + tutar WHERE ad = kime;
           SELECT bakiye FROM hesap WHERE ad = kime;
         END",
    )
    .unwrap();
    // Result of a CALL = the last statement's result.
    assert_eq!(
        rows(&db, "CALL yatir('ali', 25)"),
        vec![vec![Value::Double(125.0)]]
    );
    // Int arg widens into a DOUBLE parameter.
    assert_eq!(
        rows(&db, "CALL yatir('ayse', 10)"),
        vec![vec![Value::Double(60.0)]]
    );
}

#[test]
fn call_with_bind_params() {
    // Wire-style: CALL arguments are the caller's own placeholders.
    let (_d, db) = open();
    setup(&db);
    db.execute(
        "CREATE PROCEDURE cek(kime TEXT, tutar DOUBLE) AS BEGIN
           UPDATE hesap SET bakiye = bakiye - tutar WHERE ad = kime;
           SELECT bakiye FROM hesap WHERE ad = kime;
         END",
    )
    .unwrap();
    assert_eq!(
        rows_p(&db, "CALL cek($1, $2)", &[t("ali"), Value::Double(30.0)]),
        vec![vec![Value::Double(70.0)]]
    );
}

#[test]
fn insert_column_list_untouched_by_param_rewrite() {
    // A parameter named like a column must not corrupt the INSERT column
    // list — only expression positions are rewritten.
    let (_d, db) = open();
    setup(&db);
    db.execute(
        "CREATE PROCEDURE ekle(ad TEXT, bakiye DOUBLE) AS BEGIN
           INSERT INTO hesap (ad, bakiye) VALUES (ad, bakiye);
           SELECT COUNT(*) FROM hesap;
         END",
    )
    .unwrap();
    assert_eq!(rows(&db, "CALL ekle('veli', 5)"), vec![vec![Value::Int(3)]]);
    assert_eq!(
        rows(&db, "SELECT bakiye FROM hesap WHERE ad = 'veli'"),
        vec![vec![Value::Double(5.0)]]
    );
}

#[test]
fn param_shadows_column_in_where() {
    // In expression position an unqualified name matching a parameter IS the
    // parameter (documented shadowing); qualify with the table name to reach
    // the column.
    let (_d, db) = open();
    setup(&db);
    db.execute(
        "CREATE PROCEDURE bul(ad TEXT) AS BEGIN
           SELECT hesap.ad FROM hesap WHERE hesap.ad = ad;
         END",
    )
    .unwrap();
    assert_eq!(rows(&db, "CALL bul('ayse')"), vec![vec![t("ayse")]]);
}

#[test]
fn call_is_atomic() {
    let (_d, db) = open();
    setup(&db);
    // Second statement violates the PK -> the first insert must roll back.
    db.execute(
        "CREATE PROCEDURE patla() AS BEGIN
           INSERT INTO hesap (ad, bakiye) VALUES ('yeni', 1);
           INSERT INTO hesap (id, ad, bakiye) VALUES (1, 'dup', 0);
         END",
    )
    .unwrap();
    assert!(db.execute("CALL patla()").is_err());
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM hesap"),
        vec![vec![Value::Int(2)]]
    );
}

#[test]
fn call_joins_open_transaction() {
    let (_d, db) = open();
    setup(&db);
    db.execute(
        "CREATE PROCEDURE arttir(kime TEXT) AS BEGIN
           UPDATE hesap SET bakiye = bakiye + 1 WHERE ad = kime;
         END",
    )
    .unwrap();
    // Rolled-back transaction discards the procedure's writes.
    db.execute("BEGIN; CALL arttir('ali'); ROLLBACK").unwrap();
    assert_eq!(
        rows(&db, "SELECT bakiye FROM hesap WHERE ad = 'ali'"),
        vec![vec![Value::Double(100.0)]]
    );
    // Committed transaction keeps them.
    db.execute("BEGIN; CALL arttir('ali'); COMMIT").unwrap();
    assert_eq!(
        rows(&db, "SELECT bakiye FROM hesap WHERE ad = 'ali'"),
        vec![vec![Value::Double(101.0)]]
    );
}

#[test]
fn or_alter_drop_and_errors() {
    let (_d, db) = open();
    setup(&db);
    db.execute("CREATE PROCEDURE p1() AS BEGIN SELECT 1 FROM hesap; END")
        .unwrap();
    // Duplicate without OR ALTER errors; with it, the body is replaced.
    assert!(
        db.execute("CREATE PROCEDURE p1() AS BEGIN SELECT 2 FROM hesap; END")
            .is_err()
    );
    db.execute("CREATE OR ALTER PROCEDURE p1() AS BEGIN SELECT COUNT(*) FROM hesap; END")
        .unwrap();
    assert_eq!(rows(&db, "CALL p1()"), vec![vec![Value::Int(2)]]);

    db.execute("DROP PROCEDURE p1").unwrap();
    assert!(db.execute("CALL p1()").is_err());
    assert!(db.execute("DROP PROCEDURE p1").is_err());
    db.execute("DROP PROCEDURE IF EXISTS p1").unwrap();
}

#[test]
fn arg_validation() {
    let (_d, db) = open();
    setup(&db);
    db.execute("CREATE PROCEDURE p(n INT) AS BEGIN SELECT n FROM hesap LIMIT 1; END")
        .unwrap();
    // Arity.
    assert!(db.execute("CALL p()").is_err());
    assert!(db.execute("CALL p(1, 2)").is_err());
    // Type mismatch names the parameter.
    let err = db.execute("CALL p('metin')").unwrap_err().to_string();
    assert!(err.contains("\"n\""), "unexpected error: {err}");
    // NULL passes.
    assert_eq!(rows(&db, "CALL p(NULL)"), vec![vec![Value::Null]]);
    // Column references are not valid arguments.
    assert!(db.execute("CALL p(bakiye)").is_err());
}

#[test]
fn body_restrictions() {
    let (_d, db) = open();
    setup(&db);
    for bad in [
        "CREATE PROCEDURE b1() AS BEGIN CREATE TABLE x (a INT); END",
        "CREATE PROCEDURE b2() AS BEGIN BEGIN; SELECT 1 FROM hesap; COMMIT; END",
        "CREATE PROCEDURE b4(n INT) AS BEGIN SELECT $1 FROM hesap; END",
        "CREATE PROCEDURE b5() AS BEGIN END",
    ] {
        assert!(db.execute(bad).is_err(), "should be rejected: {bad}");
    }
    // A nested CALL is now allowed (bounded by the runtime call-depth guard).
    db.execute("CREATE PROCEDURE b1ok() AS BEGIN SELECT 1 FROM hesap; END")
        .unwrap();
    assert!(
        db.execute("CREATE PROCEDURE b3() AS BEGIN CALL b1ok(); END")
            .is_ok(),
        "nested CALL should now be accepted"
    );
    // Bodies referencing missing tables fail at creation (trial parse only
    // covers syntax; execution of the body is deferred to CALL — but the
    // statement must at least translate).
    assert!(
        db.execute("CREATE PROCEDURE b6() AS BEGIN SELECT x FROM yok_boyle_tablo; END")
            .is_ok(),
        "missing tables surface at CALL, not creation"
    );
    assert!(db.execute("CALL b6()").is_err());
}

#[test]
fn persistence_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open_at(dir.path());
        setup(&db);
        db.execute("CREATE PROCEDURE say() AS BEGIN SELECT COUNT(*) FROM hesap; END")
            .unwrap();
        assert_eq!(rows(&db, "CALL say()"), vec![vec![Value::Int(2)]]);
    }
    // WAL replay path.
    {
        let db = open_at(dir.path());
        assert_eq!(rows(&db, "CALL say()"), vec![vec![Value::Int(2)]]);
        db.checkpoint().unwrap();
    }
    // Checkpointed-catalog path.
    {
        let db = open_at(dir.path());
        assert_eq!(rows(&db, "CALL say()"), vec![vec![Value::Int(2)]]);
    }
}

#[test]
fn show_procedures() {
    let (_d, db) = open();
    setup(&db);
    db.execute("CREATE PROCEDURE p(a INT, b TEXT) AS BEGIN SELECT a, b FROM hesap; END")
        .unwrap();
    let (cols, rws) = cols_rows(&db, "SHOW PROCEDURES");
    assert_eq!(cols, vec!["procedure", "params", "language", "definition"]);
    assert_eq!(rws.len(), 1);
    assert_eq!(rws[0][0], t("p"));
    assert_eq!(rws[0][1], t("a INT, b TEXT"));
    assert_eq!(rws[0][2], t("sql"));
    let def = match &rws[0][3] {
        Value::Text(s) => s.clone(),
        other => panic!("expected text definition, got {other:?}"),
    };
    assert!(def.contains("$1") && def.contains("$2"), "def: {def}");
}

#[test]
fn call_returns_mutation_with_generated_key() {
    let (_d, db) = open();
    setup(&db);
    db.execute(
        "CREATE PROCEDURE ekle(kim TEXT) AS BEGIN
           INSERT INTO hesap (ad, bakiye) VALUES (kim, 0);
         END",
    )
    .unwrap();
    let r = db.execute("CALL ekle('veli')").unwrap().pop().unwrap();
    match r {
        QueryResult::Mutation {
            affected,
            last_insert_id,
        } => {
            assert_eq!(affected, 1);
            assert_eq!(last_insert_id, Some(3));
        }
        other => panic!("expected Mutation, got {other:?}"),
    }
}

#[test]
fn procedures_inside_transactions_rejected_for_ddl() {
    let (_d, db) = open();
    setup(&db);
    assert!(
        db.execute("BEGIN; CREATE PROCEDURE p() AS BEGIN SELECT 1 FROM hesap; END; COMMIT")
            .is_err()
    );
    db.execute("CREATE PROCEDURE p() AS BEGIN SELECT 1 FROM hesap; END")
        .unwrap();
    assert!(db.execute("BEGIN; DROP PROCEDURE p; COMMIT").is_err());
}
