//! Declared integer widths: `SMALLINT` and `INT` are enforced.
//!
//! Every integer is *stored* as an i64 whatever it was declared — the width is
//! a constraint, not a storage format. That is the trade: no space is saved by
//! declaring `SMALLINT`, but a value that does not fit is refused on write
//! instead of being silently widened, so a column's declared type stays true of
//! its contents.

mod common;

use common::*;

#[test]
fn smallint_rejects_values_outside_its_range() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, small SMALLINT)")
        .unwrap();

    // The edges fit.
    db.execute("INSERT INTO t VALUES (1, 32767), (2, -32768)")
        .unwrap();
    assert_eq!(
        rows(&db, "SELECT small FROM t ORDER BY id"),
        vec![vec![i(32767)], vec![i(-32768)]]
    );

    // One past either edge does not.
    let e = db
        .execute("INSERT INTO t VALUES (3, 32768)")
        .unwrap_err()
        .to_string();
    assert!(e.contains("out of range"), "{e}");
    assert!(e.contains("SMALLINT"), "the declared type is named: {e}");
    assert!(e.contains("small"), "the column is named: {e}");
    assert!(db.execute("INSERT INTO t VALUES (4, -32769)").is_err());

    // And nothing landed.
    assert_eq!(rows(&db, "SELECT COUNT(*) FROM t"), r1(vec![i(2)]));
}

#[test]
fn int_rejects_values_outside_32_bits() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, n INT, big BIGINT)")
        .unwrap();

    db.execute("INSERT INTO t VALUES (1, 2147483647, 9223372036854775807)")
        .unwrap();
    assert!(
        db.execute("INSERT INTO t VALUES (2, 2147483648, 1)")
            .is_err(),
        "2^31 does not fit INT"
    );
    assert!(
        db.execute("INSERT INTO t VALUES (3, -2147483649, 1)")
            .is_err()
    );

    // BIGINT keeps the full i64 range, which is what every integer is stored as.
    assert_eq!(
        rows(&db, "SELECT big FROM t"),
        vec![vec![i(9223372036854775807)]]
    );
}

#[test]
fn a_bare_integer_type_is_unrestricted() {
    // `BIGINT`, and any spelling without a declared width, keeps the i64 range
    // — including tables created before widths were recorded, whose catalogs
    // carry no width at all.
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        .unwrap();
    // (i64::MIN is not writable as a literal — the parser reads the digits as a
    // positive integer before the sign is applied — so this uses MIN + 1.)
    db.execute("INSERT INTO t VALUES (9223372036854775807, -9223372036854775807)")
        .unwrap();
    assert_eq!(rows(&db, "SELECT COUNT(*) FROM t"), r1(vec![i(1)]));
}

#[test]
fn the_range_is_enforced_on_update_and_in_transactions() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, small SMALLINT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 1)").unwrap();

    assert!(
        db.execute("UPDATE t SET small = 40000 WHERE id = 1")
            .is_err()
    );
    assert_eq!(rows(&db, "SELECT small FROM t"), vec![vec![i(1)]]);

    // Inside a transaction the write is refused at the statement, as any
    // constraint violation is.
    let mut tx = None;
    db.execute_params_in_session("BEGIN", &[], &mut tx).unwrap();
    assert!(
        db.execute_params_in_session("INSERT INTO t VALUES (2, 99999)", &[], &mut tx)
            .is_err()
    );
    assert_eq!(rows(&db, "SELECT COUNT(*) FROM t"), r1(vec![i(1)]));
}

#[test]
fn narrowing_a_column_checks_every_stored_value_first() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v BIGINT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 5), (2, 70000)")
        .unwrap();

    // 70000 does not fit, so the ALTER is refused and the column is untouched.
    let e = db
        .execute("ALTER TABLE t ALTER COLUMN v TYPE SMALLINT")
        .unwrap_err()
        .to_string();
    assert!(e.contains("out of range"), "{e}");
    db.execute("INSERT INTO t VALUES (3, 70001)").unwrap();

    // With the outsized row gone, the same ALTER succeeds — and then enforces.
    db.execute("DELETE FROM t WHERE v > 32767").unwrap();
    db.execute("ALTER TABLE t ALTER COLUMN v TYPE SMALLINT")
        .unwrap();
    assert!(db.execute("INSERT INTO t VALUES (4, 40000)").is_err());
    db.execute("INSERT INTO t VALUES (5, 300)").unwrap();
}

#[test]
fn a_widened_column_accepts_what_it_could_not_before() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v SMALLINT)")
        .unwrap();
    assert!(db.execute("INSERT INTO t VALUES (1, 50000)").is_err());
    db.execute("ALTER TABLE t ALTER COLUMN v TYPE BIGINT")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 50000)").unwrap();
    assert_eq!(rows(&db, "SELECT v FROM t"), vec![vec![i(50000)]]);
}

#[test]
fn the_declared_width_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open_at(dir.path());
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, small SMALLINT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 5)").unwrap();
    }
    let db = open_at(dir.path());
    assert!(
        db.execute("INSERT INTO t VALUES (2, 40000)").is_err(),
        "the width is in the catalog, not just in the session"
    );
    db.execute("INSERT INTO t VALUES (2, 30000)").unwrap();
}

#[test]
fn describe_reports_the_declared_type() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (a SMALLINT, b INT, c BIGINT, d INTEGER)")
        .unwrap();
    let types: Vec<String> = rows(&db, "DESCRIBE t")
        .into_iter()
        .map(|r| match &r[1] {
            oxidb_sql::Value::Text(s) => s.to_string(),
            other => panic!("expected text, got {other:?}"),
        })
        .collect();
    assert_eq!(types, vec!["SMALLINT", "INT", "BIGINT", "INT"]);
}
