//! Composite PRIMARY KEY: `CONSTRAINT pk PRIMARY KEY (a, b)`.
//!
//! Uniqueness is over the whole key tuple — two rows may share any proper
//! subset of the key columns, and only a full-tuple repeat collides. The same
//! rules must hold on the autocommit path, inside a transaction, across a
//! restart, and in disk-first mode (where the key map is seeded by decoding the
//! mmap'd snapshot rather than from resident rows).

mod common;

use common::*;
use oxidb_sql::{QueryResult, SqlEngine, SqlOptions, Value};

/// One call of an interactive (session) transaction: `BEGIN`/`COMMIT` may span
/// calls, and a failed statement aborts the transaction.
fn sess(db: &SqlEngine, tx: &mut Option<u64>, sql: &str) -> oxidb_sql::Result<Vec<QueryResult>> {
    db.execute_params_in_session(sql, &[], tx)
}

fn seed(db: &SqlEngine) {
    db.execute(
        "CREATE TABLE enrol (student INT, course TEXT, grade INT,
         CONSTRAINT pk_enrol PRIMARY KEY (student, course))",
    )
    .unwrap();
    db.execute("INSERT INTO enrol VALUES (1, 'math', 90), (1, 'physics', 80), (2, 'math', 70)")
        .unwrap();
}

#[test]
fn tuple_uniqueness_not_per_column() {
    let (_d, db) = open();
    seed(&db);
    // Sharing one key column is fine — that's the point of a composite key.
    db.execute("INSERT INTO enrol VALUES (2, 'physics', 60)")
        .unwrap();
    db.execute("INSERT INTO enrol VALUES (3, 'math', 50)")
        .unwrap();
    assert_eq!(rows(&db, "SELECT COUNT(*) FROM enrol"), r1(vec![i(5)]));

    // Repeating the whole tuple is not.
    let e = db
        .execute("INSERT INTO enrol VALUES (1, 'math', 100)")
        .unwrap_err()
        .to_string();
    assert!(e.contains("PRIMARY KEY"), "{e}");
    assert!(e.contains("math"), "the tuple is named in the error: {e}");
    assert_eq!(rows(&db, "SELECT COUNT(*) FROM enrol"), r1(vec![i(5)]));
}

#[test]
fn duplicate_within_one_insert_batch() {
    let (_d, db) = open();
    seed(&db);
    // Both rows are new to the table but collide with each other; the whole
    // statement must fail (it is one WAL batch).
    assert!(
        db.execute("INSERT INTO enrol VALUES (9, 'art', 1), (9, 'art', 2)")
            .is_err()
    );
    assert_eq!(rows(&db, "SELECT COUNT(*) FROM enrol"), r1(vec![i(3)]));
    // Differing in the second key column, the same batch succeeds.
    db.execute("INSERT INTO enrol VALUES (9, 'art', 1), (9, 'music', 2)")
        .unwrap();
    assert_eq!(rows(&db, "SELECT COUNT(*) FROM enrol"), r1(vec![i(5)]));
}

#[test]
fn update_into_an_occupied_key_is_rejected() {
    let (_d, db) = open();
    seed(&db);
    // (1,'physics') -> (1,'math') would collide with the existing row.
    assert!(
        db.execute("UPDATE enrol SET course = 'math' WHERE student = 1 AND course = 'physics'")
            .is_err()
    );
    // The row is untouched.
    assert_eq!(
        rows(&db, "SELECT grade FROM enrol WHERE course = 'physics'"),
        r1(vec![i(80)])
    );
    // Moving to a free tuple works, and frees the old one for reuse.
    affected(
        &db,
        "UPDATE enrol SET course = 'chem' WHERE student = 1 AND course = 'physics'",
    );
    db.execute("INSERT INTO enrol VALUES (1, 'physics', 55)")
        .unwrap();
    assert_eq!(
        rows(
            &db,
            "SELECT course, grade FROM enrol WHERE student = 1 ORDER BY course"
        ),
        vec![
            vec![t("chem"), i(80)],
            vec![t("math"), i(90)],
            vec![t("physics"), i(55)],
        ]
    );
}

#[test]
fn updating_a_row_in_place_keeps_its_own_key() {
    // The key check must exclude the row being rewritten, or every no-op
    // update of a keyed row would report a duplicate.
    let (_d, db) = open();
    seed(&db);
    assert_eq!(
        affected(
            &db,
            "UPDATE enrol SET grade = 95 WHERE student = 1 AND course = 'math'"
        ),
        1
    );
    assert_eq!(
        rows(
            &db,
            "SELECT grade FROM enrol WHERE student = 1 AND course = 'math'"
        ),
        r1(vec![i(95)])
    );
}

#[test]
fn delete_frees_the_key() {
    let (_d, db) = open();
    seed(&db);
    assert_eq!(
        affected(
            &db,
            "DELETE FROM enrol WHERE student = 1 AND course = 'math'"
        ),
        1
    );
    db.execute("INSERT INTO enrol VALUES (1, 'math', 10)")
        .unwrap();
    assert_eq!(
        rows(
            &db,
            "SELECT grade FROM enrol WHERE student = 1 AND course = 'math'"
        ),
        r1(vec![i(10)])
    );
}

#[test]
fn full_key_lookup_and_partial_key_scan() {
    // A full-key equality is answered from the key map; a partial key is not
    // unique, so it must fall back to a scan and return every matching row.
    let (_d, db) = open();
    seed(&db);
    assert_eq!(
        rows(
            &db,
            "SELECT grade FROM enrol WHERE student = 1 AND course = 'physics'"
        ),
        r1(vec![i(80)])
    );
    assert_eq!(
        rows(
            &db,
            "SELECT grade FROM enrol WHERE student = 1 ORDER BY grade"
        ),
        vec![vec![i(80)], vec![i(90)]]
    );
    assert_eq!(
        rows(
            &db,
            "SELECT student FROM enrol WHERE course = 'math' ORDER BY student"
        ),
        vec![vec![i(1)], vec![i(2)]]
    );
    // A full key that matches nothing.
    assert_eq!(
        rows(
            &db,
            "SELECT grade FROM enrol WHERE student = 7 AND course = 'math'"
        ),
        Vec::<Vec<Value>>::new()
    );
    // A full key plus a non-key equality: the key finds the row, the rest of
    // the predicate still has to hold.
    assert_eq!(
        rows(
            &db,
            "SELECT grade FROM enrol WHERE student = 1 AND course = 'math' AND grade = 90"
        ),
        r1(vec![i(90)])
    );
    assert_eq!(
        rows(
            &db,
            "SELECT grade FROM enrol WHERE student = 1 AND course = 'math' AND grade = 999"
        ),
        Vec::<Vec<Value>>::new()
    );
}

#[test]
fn key_columns_are_implicitly_not_null() {
    let (_d, db) = open();
    seed(&db);
    assert!(
        db.execute("INSERT INTO enrol VALUES (4, NULL, 10)")
            .is_err()
    );
    assert!(
        db.execute("INSERT INTO enrol VALUES (NULL, 'art', 10)")
            .is_err()
    );
    // DESCRIBE reports every key part (MySQL's `PRI`-on-each-part shape).
    let pri: Vec<Vec<Value>> = rows(&db, "DESCRIBE enrol")
        .into_iter()
        .filter(|r| r[3] == Value::Bool(true))
        .map(|r| vec![r[0].clone()])
        .collect();
    assert_eq!(pri, vec![vec![t("student")], vec![t("course")]]);
}

#[test]
fn enforced_inside_a_transaction() {
    let (_d, db) = open();
    seed(&db);

    // Colliding with a committed row the transaction hasn't touched (probed
    // through the engine's key map, not a snapshot of the table).
    let mut tx = None;
    sess(&db, &mut tx, "BEGIN").unwrap();
    assert!(sess(&db, &mut tx, "INSERT INTO enrol VALUES (2, 'math', 1)").is_err());
    assert_eq!(tx, None, "a failed statement aborts the transaction");
    assert_eq!(rows(&db, "SELECT COUNT(*) FROM enrol"), r1(vec![i(3)]));

    // ...and colliding with a row the same transaction just wrote — the
    // overlay's own keys are checked, not only the committed ones.
    sess(&db, &mut tx, "BEGIN").unwrap();
    sess(&db, &mut tx, "INSERT INTO enrol VALUES (5, 'bio', 1)").unwrap();
    assert!(sess(&db, &mut tx, "INSERT INTO enrol VALUES (5, 'bio', 2)").is_err());
    assert_eq!(rows(&db, "SELECT COUNT(*) FROM enrol"), r1(vec![i(3)]));

    // Rows sharing only one key column commit fine.
    sess(&db, &mut tx, "BEGIN").unwrap();
    sess(&db, &mut tx, "INSERT INTO enrol VALUES (5, 'bio', 1)").unwrap();
    sess(&db, &mut tx, "INSERT INTO enrol VALUES (5, 'geo', 3)").unwrap();
    sess(&db, &mut tx, "COMMIT").unwrap();
    assert_eq!(rows(&db, "SELECT COUNT(*) FROM enrol"), r1(vec![i(5)]));

    // A key freed inside the transaction is reusable inside it: the overlay's
    // delete supersedes the committed row's ownership.
    sess(&db, &mut tx, "BEGIN").unwrap();
    sess(
        &db,
        &mut tx,
        "DELETE FROM enrol WHERE student = 5 AND course = 'bio'",
    )
    .unwrap();
    sess(&db, &mut tx, "INSERT INTO enrol VALUES (5, 'bio', 9)").unwrap();
    sess(&db, &mut tx, "COMMIT").unwrap();
    assert_eq!(
        rows(
            &db,
            "SELECT grade FROM enrol WHERE student = 5 AND course = 'bio'"
        ),
        r1(vec![i(9)])
    );

    // A rolled-back insert releases its key.
    sess(&db, &mut tx, "BEGIN").unwrap();
    sess(&db, &mut tx, "INSERT INTO enrol VALUES (6, 'art', 1)").unwrap();
    sess(&db, &mut tx, "ROLLBACK").unwrap();
    db.execute("INSERT INTO enrol VALUES (6, 'art', 2)")
        .unwrap();
    assert_eq!(rows(&db, "SELECT COUNT(*) FROM enrol"), r1(vec![i(6)]));
}

#[test]
fn survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open_at(dir.path());
        seed(&db);
    }
    // Reopened from the WAL: the key map is reseeded, so the collision is
    // still caught and non-colliding rows still insert.
    {
        let db = open_at(dir.path());
        assert!(
            db.execute("INSERT INTO enrol VALUES (1, 'math', 1)")
                .is_err()
        );
        db.execute("INSERT INTO enrol VALUES (1, 'chem', 1)")
            .unwrap();
        db.checkpoint().unwrap();
    }
    // And again from the checkpointed snapshot.
    {
        let db = open_at(dir.path());
        assert!(
            db.execute("INSERT INTO enrol VALUES (1, 'chem', 2)")
                .is_err()
        );
        assert_eq!(rows(&db, "SELECT COUNT(*) FROM enrol"), r1(vec![i(4)]));
    }
}

#[test]
fn the_ddl_ef_core_and_pg_dump_emit() {
    let (_d, db) = open();
    // EF Core migrations: quoted identifiers, named constraint, key columns
    // already declared NOT NULL.
    db.execute(
        r#"CREATE TABLE "Enrolment" (
             "StudentId" INT NOT NULL,
             "CourseId"  INT NOT NULL,
             "Grade"     INT NULL,
             CONSTRAINT "PK_Enrolment" PRIMARY KEY ("StudentId", "CourseId")
           )"#,
    )
    .unwrap();
    db.execute(r#"INSERT INTO "Enrolment" VALUES (1, 1, 90), (1, 2, 80)"#)
        .unwrap();
    assert!(
        db.execute(r#"INSERT INTO "Enrolment" VALUES (1, 1, 70)"#)
            .is_err()
    );

    // The anonymous form (no CONSTRAINT name), three columns deep.
    db.execute("CREATE TABLE t3 (a INT, b TEXT, c INT, d INT, PRIMARY KEY (a, b, c))")
        .unwrap();
    db.execute("INSERT INTO t3 VALUES (1, 'x', 1, 0), (1, 'x', 2, 0), (1, 'y', 1, 0)")
        .unwrap();
    assert!(db.execute("INSERT INTO t3 VALUES (1, 'x', 1, 9)").is_err());
    db.execute("INSERT INTO t3 VALUES (2, 'x', 1, 9)").unwrap();
    assert_eq!(rows(&db, "SELECT COUNT(*) FROM t3"), r1(vec![i(4)]));
}

#[test]
fn survives_a_restart_disk_first() {
    // Disk-first seeds the key map by decoding the mmap'd snapshot without
    // retaining rows — a separate load path from the resident one above.
    let dir = tempfile::tempdir().unwrap();
    let opts = || SqlOptions {
        disk_first: true,
        ..SqlOptions::from_env()
    };
    {
        let db = SqlEngine::open_with_options(dir.path(), opts()).unwrap();
        seed(&db);
        db.checkpoint().unwrap();
    }
    {
        let db = SqlEngine::open_with_options(dir.path(), opts()).unwrap();
        assert!(
            db.execute("INSERT INTO enrol VALUES (2, 'math', 1)")
                .is_err()
        );
        db.execute("INSERT INTO enrol VALUES (2, 'art', 1)")
            .unwrap();
        assert_eq!(rows(&db, "SELECT COUNT(*) FROM enrol"), r1(vec![i(4)]));
    }
}

#[test]
fn a_key_column_cannot_be_dropped_or_retyped() {
    let (_d, db) = open();
    seed(&db);
    assert!(db.execute("ALTER TABLE enrol DROP COLUMN course").is_err());
    assert!(
        db.execute("ALTER TABLE enrol ALTER COLUMN student TYPE TEXT")
            .is_err()
    );
    // A non-key column is still free to go.
    db.execute("ALTER TABLE enrol DROP COLUMN grade").unwrap();
    assert!(
        db.execute("INSERT INTO enrol VALUES (1, 'math')").is_err(),
        "the key survives a dropped non-key column"
    );
    db.execute("INSERT INTO enrol VALUES (1, 'art')").unwrap();
}

#[test]
fn coexists_with_a_unique_column() {
    let (_d, db) = open();
    db.execute(
        "CREATE TABLE seat (row INT, col INT, tag TEXT UNIQUE,
         CONSTRAINT pk PRIMARY KEY (row, col))",
    )
    .unwrap();
    db.execute("INSERT INTO seat VALUES (1, 1, 'a'), (1, 2, 'b')")
        .unwrap();
    // PK tuple collision.
    assert!(db.execute("INSERT INTO seat VALUES (1, 1, 'c')").is_err());
    // UNIQUE collision on a non-key column.
    let e = db
        .execute("INSERT INTO seat VALUES (2, 2, 'a')")
        .unwrap_err()
        .to_string();
    assert!(e.contains("UNIQUE"), "{e}");
    // Both fine.
    db.execute("INSERT INTO seat VALUES (2, 2, 'c')").unwrap();
    // ...and the same rules inside a transaction (a failed statement aborts
    // it, so each case gets its own).
    let mut tx = None;
    sess(&db, &mut tx, "BEGIN").unwrap();
    assert!(sess(&db, &mut tx, "INSERT INTO seat VALUES (1, 2, 'z')").is_err());
    sess(&db, &mut tx, "BEGIN").unwrap();
    assert!(sess(&db, &mut tx, "INSERT INTO seat VALUES (3, 3, 'b')").is_err());
    sess(&db, &mut tx, "BEGIN").unwrap();
    sess(&db, &mut tx, "INSERT INTO seat VALUES (3, 3, 'z')").unwrap();
    sess(&db, &mut tx, "COMMIT").unwrap();
    assert_eq!(rows(&db, "SELECT COUNT(*) FROM seat"), r1(vec![i(4)]));
}

#[test]
fn declaration_errors() {
    let (_d, db) = open();
    // A column named twice in the key.
    assert!(
        db.execute("CREATE TABLE e1 (a INT, b INT, CONSTRAINT pk PRIMARY KEY (a, a))")
            .is_err()
    );
    // A key column that doesn't exist.
    assert!(
        db.execute("CREATE TABLE e2 (a INT, CONSTRAINT pk PRIMARY KEY (a, nope))")
            .is_err()
    );
    // Column-level and table-level primary keys together.
    assert!(
        db.execute("CREATE TABLE e3 (a INT PRIMARY KEY, b INT, CONSTRAINT pk PRIMARY KEY (a, b))")
            .is_err()
    );
    // Two column-level primary keys.
    assert!(
        db.execute("CREATE TABLE e4 (a INT PRIMARY KEY, b INT PRIMARY KEY)")
            .is_err()
    );
    // A single-column FK has no one column to resolve to on a composite-PK
    // parent, so an unnamed reference is refused rather than mis-enforced.
    db.execute("CREATE TABLE par (a INT, b INT, CONSTRAINT pk PRIMARY KEY (a, b))")
        .unwrap();
    db.execute("CREATE TABLE chi (x INT REFERENCES par)")
        .unwrap();
    let e = db
        .execute("INSERT INTO chi VALUES (1)")
        .unwrap_err()
        .to_string();
    assert!(e.contains("composite"), "{e}");
}
