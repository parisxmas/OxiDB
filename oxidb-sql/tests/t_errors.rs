//! Error handling and clean rejection of unsupported features (never a panic).

mod common;
use common::*;

fn t1(db: &oxidb_sql::SqlEngine) {
    db.execute("CREATE TABLE t (id INT, name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'b')").unwrap();
}

#[test]
fn parse_errors() {
    let (_d, db) = open();
    assert!(db.execute("SELCT 1").is_err());
    assert!(db.execute("INSERT INTO").is_err());
    assert!(db.execute("SELECT * FROM").is_err());
}

#[test]
fn unknown_table_and_column() {
    let (_d, db) = open();
    t1(&db);
    assert!(db.execute("SELECT * FROM ghost").is_err());
    assert!(db.execute("SELECT missing FROM t").is_err());
    assert!(db.execute("UPDATE t SET missing = 1").is_err());
    assert!(db.execute("DELETE FROM ghost").is_err());
    assert!(db.execute("INSERT INTO ghost VALUES (1)").is_err());
}

#[test]
fn ambiguous_column_in_join() {
    let (_d, db) = open();
    db.execute("CREATE TABLE a (id INT)").unwrap();
    db.execute("CREATE TABLE b (id INT)").unwrap();
    db.execute("INSERT INTO a VALUES (1)").unwrap();
    db.execute("INSERT INTO b VALUES (1)").unwrap();
    // `id` exists in both -> must be qualified.
    assert!(
        db.execute("SELECT id FROM a JOIN b ON a.id = b.id")
            .is_err()
    );
    // Qualified is fine.
    assert!(
        db.execute("SELECT a.id FROM a JOIN b ON a.id = b.id")
            .is_ok()
    );
}

#[test]
fn duplicate_table_create() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT)").unwrap();
    assert!(db.execute("CREATE TABLE t (id INT)").is_err());
    assert!(db.execute("CREATE TABLE IF NOT EXISTS t (id INT)").is_ok());
}

#[test]
fn drop_missing_table() {
    let (_d, db) = open();
    assert!(db.execute("DROP TABLE ghost").is_err());
    assert!(db.execute("DROP TABLE IF EXISTS ghost").is_ok());
}

#[test]
fn unsupported_select_features() {
    let (_d, db) = open();
    t1(&db);
    // UNION and OFFSET are supported now; EXCEPT/INTERSECT are not.
    assert!(
        db.execute("SELECT id FROM t EXCEPT SELECT id FROM t")
            .is_err()
    );
    assert!(
        db.execute("SELECT id FROM t INTERSECT SELECT id FROM t")
            .is_err()
    );
    // SELECT DISTINCT is supported since ADR-0013 Phase A; DISTINCT ON and
    // aggregate DISTINCT still are not.
    assert!(db.execute("SELECT DISTINCT ON (id) id FROM t").is_err());
    assert!(db.execute("SELECT COUNT(DISTINCT id) FROM t").is_err());
    assert!(db.execute("SELECT * FROM (SELECT id FROM t) x").is_err()); // derived table
    assert!(db.execute("SELECT * FROM t, t t2").is_err()); // comma join
}

#[test]
fn unsupported_join_forms() {
    let (_d, db) = open();
    t1(&db);
    assert!(
        db.execute("SELECT t.id FROM t JOIN t t2 USING (id)")
            .is_err()
    );
}

#[test]
fn unsupported_data_type() {
    let (_d, db) = open();
    // A type we don't map should be rejected at parse/translate.
    // BLOB and DECIMAL are supported since ADR-0013 Phase D; something
    // genuinely exotic still errors.
    assert!(db.execute("CREATE TABLE t (x JSONB)").is_err());
}

#[test]
fn division_by_zero_and_type_errors() {
    let (_d, db) = open();
    t1(&db);
    assert!(db.execute("SELECT id / 0 AS x FROM t").is_err());
    // text vs int comparison is not orderable.
    assert!(db.execute("SELECT id FROM t WHERE name > 5").is_err());
}

#[test]
fn aggregate_outside_group_context_in_scalar_position() {
    let (_d, db) = open();
    t1(&db);
    // WHERE cannot contain aggregates.
    assert!(db.execute("SELECT id FROM t WHERE COUNT(*) > 0").is_err());
}
