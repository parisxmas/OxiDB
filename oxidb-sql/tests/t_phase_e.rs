//! ADR-0013 Phase E engine surface: derived tables (`FROM (SELECT ...) AS x`)
//! and table-level `CONSTRAINT ... PRIMARY KEY/UNIQUE` — the DDL/queries EF
//! Core migrations and its query pipeline emit.

mod common;

use common::*;
use oxidb_sql::Value;

fn t(s: &str) -> Value {
    Value::Text(s.to_string().into())
}

fn seed(db: &oxidb_sql::SqlEngine) {
    db.execute("CREATE TABLE m (id INT PRIMARY KEY AUTO_INCREMENT, ad TEXT, puan INT)")
        .unwrap();
    db.execute("INSERT INTO m (ad, puan) VALUES ('ali', 10), ('ayse', 25), ('veli', 5)")
        .unwrap();
}

#[test]
fn derived_table_basic() {
    let (_d, db) = open();
    seed(&db);
    assert_eq!(
        rows(
            &db,
            r#"SELECT x.ad FROM (SELECT ad, puan FROM m WHERE puan >= 10) AS x ORDER BY x.ad"#
        ),
        vec![vec![t("ali")], vec![t("ayse")]]
    );
}

#[test]
fn derived_table_inner_order_limit_offset() {
    // The exact shape EF emits for Skip/Take + Single: an inner
    // ORDER BY/LIMIT/OFFSET wrapped by an outer LIMIT.
    let (_d, db) = open();
    seed(&db);
    assert_eq!(
        rows(
            &db,
            r#"SELECT e0.ad FROM (SELECT ad FROM m ORDER BY puan LIMIT 2 OFFSET 1) AS e0 LIMIT 2"#
        ),
        vec![vec![t("ali")], vec![t("ayse")]]
    );
}

#[test]
fn derived_table_with_params() {
    let (_d, db) = open();
    seed(&db);
    assert_eq!(
        rows_p(
            &db,
            r#"SELECT x.ad FROM (SELECT ad FROM m WHERE puan > $1) AS x ORDER BY x.ad"#,
            &[Value::Int(9)]
        ),
        vec![vec![t("ali")], vec![t("ayse")]]
    );
}

#[test]
fn derived_table_aggregate_then_filter() {
    let (_d, db) = open();
    seed(&db);
    assert_eq!(
        rows(&db, r#"SELECT s.n FROM (SELECT COUNT(*) AS n FROM m) AS s"#),
        vec![vec![Value::Int(3)]]
    );
}

#[test]
fn derived_table_in_join() {
    let (_d, db) = open();
    seed(&db);
    db.execute("CREATE TABLE s (id INT PRIMARY KEY AUTO_INCREMENT, mid INT, tutar DOUBLE)")
        .unwrap();
    db.execute("INSERT INTO s (mid, tutar) VALUES (1, 12.5), (1, 7.5), (2, 100)")
        .unwrap();
    assert_eq!(
        rows(
            &db,
            r#"SELECT x.ad, s.tutar FROM s
               JOIN (SELECT id, ad FROM m WHERE puan >= 10) AS x ON s.mid = x.id
               ORDER BY x.ad, s.tutar"#
        ),
        vec![
            vec![t("ali"), Value::Double(7.5)],
            vec![t("ali"), Value::Double(12.5)],
            vec![t("ayse"), Value::Double(100.0)],
        ]
    );
}

#[test]
fn derived_table_requires_alias() {
    let (_d, db) = open();
    seed(&db);
    assert!(db.execute("SELECT * FROM (SELECT ad FROM m)").is_err());
}

#[test]
fn table_level_primary_key_constraint() {
    // EF migrations emit `CONSTRAINT "PK_t" PRIMARY KEY ("Id")`.
    let (_d, db) = open();
    db.execute(
        r#"CREATE TABLE "t" ("Id" INT NOT NULL AUTO_INCREMENT, "Ad" TEXT NOT NULL,
           CONSTRAINT "PK_t" PRIMARY KEY ("Id"))"#,
    )
    .unwrap();
    db.execute(r#"INSERT INTO "t" ("Ad") VALUES ('a'), ('b')"#)
        .unwrap();
    assert_eq!(
        rows(&db, r#"SELECT "Id" FROM "t" ORDER BY "Id""#),
        vec![vec![Value::Int(1)], vec![Value::Int(2)]]
    );
    // PK uniqueness is enforced.
    assert!(
        db.execute(r#"INSERT INTO "t" ("Id", "Ad") VALUES (1, 'dup')"#)
            .is_err()
    );
}

#[test]
fn table_level_unique_constraint() {
    let (_d, db) = open();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY, mail TEXT, CONSTRAINT uq UNIQUE (mail))")
        .unwrap();
    db.execute("INSERT INTO u VALUES (1, 'a@x'), (2, 'b@x')")
        .unwrap();
    assert!(db.execute("INSERT INTO u VALUES (3, 'a@x')").is_err());
}

#[test]
fn table_level_constraint_errors() {
    let (_d, db) = open();
    // Multi-column table-level PK creates and is enforced as one key tuple
    // (see t_composite_pk.rs).
    assert!(
        db.execute("CREATE TABLE p (a INT, b INT, CONSTRAINT pk PRIMARY KEY (a, b))")
            .is_ok()
    );
    // Constraint on a column that doesn't exist.
    assert!(
        db.execute("CREATE TABLE q (a INT, CONSTRAINT pk PRIMARY KEY (nope))")
            .is_err()
    );
}

#[test]
fn update_returning() {
    let (_d, db) = open();
    seed(&db);
    // EF's affected-count shape: one row per updated row.
    assert_eq!(
        rows(&db, "UPDATE m SET puan = 99 WHERE ad = 'ali' RETURNING 1"),
        vec![vec![Value::Int(1)]]
    );
    // Post-update cell values are visible to RETURNING.
    assert_eq!(
        rows(
            &db,
            "UPDATE m SET puan = puan + 1 WHERE ad = 'ali' RETURNING ad, puan"
        ),
        vec![vec![t("ali"), Value::Int(100)]]
    );
    // No match -> empty result set (how EF detects concurrency misses).
    assert_eq!(
        rows(&db, "UPDATE m SET puan = 0 WHERE ad = 'yok' RETURNING 1"),
        Vec::<Vec<Value>>::new()
    );
}

#[test]
fn delete_returning() {
    let (_d, db) = open();
    seed(&db);
    assert_eq!(
        rows(&db, "DELETE FROM m WHERE puan < 10 RETURNING ad"),
        vec![vec![t("veli")]]
    );
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM m"),
        vec![vec![Value::Int(2)]]
    );
}

#[test]
fn update_returning_inside_transaction() {
    let (_d, db) = open();
    seed(&db);
    let results = db
        .execute(
            "BEGIN; UPDATE m SET puan = 42 WHERE ad = 'ayse' RETURNING 1; COMMIT;
             SELECT puan FROM m WHERE ad = 'ayse'",
        )
        .unwrap();
    let mut selects = results.iter().filter_map(|r| match r {
        oxidb_sql::QueryResult::Select { rows, .. } => Some(rows.clone()),
        _ => None,
    });
    assert_eq!(selects.next().unwrap(), vec![vec![Value::Int(1)]]);
    assert_eq!(selects.next().unwrap(), vec![vec![Value::Int(42)]]);
}
