//! `CREATE UNIQUE INDEX` — a uniqueness constraint that rides the enforced
//! column-UNIQUE machinery (single column). Found by the embedded EF example:
//! EF's `IsUnique()` emits `CREATE UNIQUE INDEX`, the engine accepted it as a
//! plain index, and duplicates sailed through a constraint the application
//! believed in. Silent non-enforcement is the one wrong answer here; every
//! unsupported shape must refuse instead.

mod common;

use common::*;
use oxidb_sql::Value;

fn seed(db: &oxidb_sql::SqlEngine) {
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, note TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'ali', NULL), (2, 'ayse', NULL)")
        .unwrap();
}

#[test]
fn duplicates_are_refused_after_create_unique_index() {
    let (_d, db) = open();
    seed(&db);
    db.execute("CREATE UNIQUE INDEX ux ON t(name)").unwrap();
    // INSERT of a taken value.
    let err = db.execute("INSERT INTO t VALUES (3, 'ali', NULL)");
    assert!(err.is_err(), "duplicate insert must be refused");
    // UPDATE into a taken value.
    let err = db.execute("UPDATE t SET name = 'ayse' WHERE id = 1");
    assert!(err.is_err(), "duplicate update must be refused");
    // The rows the failed statements would have touched are unchanged.
    assert_eq!(
        rows(&db, "SELECT count(*) FROM t"),
        vec![vec![Value::Int(2)]]
    );
    // A fresh value still goes in.
    db.execute("INSERT INTO t VALUES (3, 'can', NULL)").unwrap();
}

#[test]
fn existing_duplicates_refuse_the_create() {
    let (_d, db) = open();
    seed(&db);
    db.execute("INSERT INTO t VALUES (3, 'ali', NULL)").unwrap();
    let err = db.execute("CREATE UNIQUE INDEX ux ON t(name)");
    assert!(err.is_err(), "existing duplicates must refuse the index");
    // And nothing was half-created: the name is free for a plain index.
    db.execute("CREATE INDEX ux ON t(name)").unwrap();
}

#[test]
fn nulls_are_exempt() {
    let (_d, db) = open();
    seed(&db);
    // Two NULL notes already exist; the index must accept them and more.
    db.execute("CREATE UNIQUE INDEX ux ON t(note)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'can', NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (4, 'deniz', NULL)")
        .unwrap();
}

#[test]
fn drop_index_removes_the_constraint() {
    let (_d, db) = open();
    seed(&db);
    db.execute("CREATE UNIQUE INDEX ux ON t(name)").unwrap();
    db.execute("DROP INDEX ux").unwrap();
    // The uniqueness went with the index.
    db.execute("INSERT INTO t VALUES (3, 'ali', NULL)").unwrap();
    assert_eq!(
        rows(&db, "SELECT count(*) FROM t WHERE name = 'ali'"),
        vec![vec![Value::Int(2)]]
    );
}

#[test]
fn survives_reopen_via_wal_replay_and_via_checkpoint() {
    let (dir, db) = open();
    seed(&db);
    db.execute("CREATE UNIQUE INDEX ux ON t(name)").unwrap();

    // Reopen with the CREATE still in the WAL tail: replay must re-enable
    // the constraint (the apply path, not just the live path).
    drop(db);
    let db = open_at(dir.path());
    assert!(db.execute("INSERT INTO t VALUES (3, 'ali', NULL)").is_err());

    // Fold into the catalog, reopen again: the catalog path now carries it.
    db.checkpoint().unwrap();
    drop(db);
    let db = open_at(dir.path());
    assert!(db.execute("INSERT INTO t VALUES (3, 'ali', NULL)").is_err());
    db.execute("INSERT INTO t VALUES (3, 'can', NULL)").unwrap();
}

#[test]
fn transactions_respect_it() {
    let (_d, db) = open();
    seed(&db);
    db.execute("CREATE UNIQUE INDEX ux ON t(name)").unwrap();
    // A buffered duplicate must not survive to commit.
    let r = db.execute("BEGIN; INSERT INTO t VALUES (3, 'ali', NULL); COMMIT");
    assert!(r.is_err(), "transactional duplicate must be refused");
    assert_eq!(
        rows(&db, "SELECT count(*) FROM t"),
        vec![vec![Value::Int(2)]]
    );
}

#[test]
fn unsupported_shapes_refuse_instead_of_underdelivering() {
    let (_d, db) = open();
    seed(&db);
    // Multi-column uniqueness is not enforced — so it must not be accepted.
    assert!(
        db.execute("CREATE UNIQUE INDEX ux ON t(name, note)")
            .is_err(),
        "multi-column UNIQUE INDEX must refuse"
    );
    // Inside a transaction there is no committed state to validate against.
    assert!(
        db.execute("BEGIN; CREATE UNIQUE INDEX ux ON t(name); COMMIT")
            .is_err(),
        "CREATE UNIQUE INDEX in a transaction must refuse"
    );
}

#[test]
fn declared_unique_column_is_not_hijacked() {
    let (_d, db) = open();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY, code TEXT UNIQUE)")
        .unwrap();
    db.execute("INSERT INTO u VALUES (1, 'a')").unwrap();
    // An index on a column that is already unique is just an index —
    // dropping it must not take away the declared constraint.
    db.execute("CREATE UNIQUE INDEX ux ON u(code)").unwrap();
    db.execute("DROP INDEX ux").unwrap();
    assert!(
        db.execute("INSERT INTO u VALUES (2, 'a')").is_err(),
        "declared UNIQUE must survive the index's drop"
    );
}
