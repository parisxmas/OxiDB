//! LIKE fast path (prefix/suffix/contains/exact) must match the general
//! recursive matcher — EF's StartsWith/EndsWith/Contains render as these.

mod common;

use common::*;

fn seed() -> (tempfile::TempDir, oxidb_sql::SqlEngine) {
    let (dir, db) = open();
    db.execute("CREATE TABLE t (s TEXT)").unwrap();
    for s in [
        "Customer 000007",
        "Customer 000123",
        "customer 000007", // lowercase — LIKE is ASCII case-insensitive here
        "Other 999",
        "7",
        "",
    ] {
        db.execute(&format!("INSERT INTO t VALUES ('{s}')"))
            .unwrap();
    }
    (dir, db)
}

fn count(db: &oxidb_sql::SqlEngine, pat: &str) -> i64 {
    let r = rows(db, &format!("SELECT COUNT(*) FROM t WHERE s LIKE '{pat}'"));
    match r[0][0] {
        oxidb_sql::Value::Int(n) => n,
        ref v => panic!("{v:?}"),
    }
}

#[test]
fn fastpath_prefix_suffix_contains_exact() {
    let (_d, db) = seed();
    // prefix (StartsWith) — case-insensitive: matches both "Customer" casings.
    assert_eq!(count(&db, "Customer 00%"), 3);
    // suffix (EndsWith).
    assert_eq!(count(&db, "%7"), 3); // the two ...0007 and "7"
    // contains.
    assert_eq!(count(&db, "%000%"), 3);
    // exact (no wildcard).
    assert_eq!(count(&db, "Other 999"), 1);
    // combined prefix+suffix (string_multi's shape) via AND.
    let r = rows(
        &db,
        "SELECT COUNT(*) FROM t WHERE s LIKE 'Customer 00%' AND s LIKE '%7'",
    );
    assert_eq!(r[0][0], i(2)); // the two "...0007" rows
}

#[test]
fn fastpath_edges() {
    let (_d, db) = seed();
    // Bare "%" matches everything (including empty).
    assert_eq!(count(&db, "%"), 6);
    // Empty pattern matches only the empty string.
    assert_eq!(count(&db, ""), 1);
    // A "_" wildcard falls back to the general matcher (not the fast path).
    assert_eq!(count(&db, "_"), 1); // "7"
    // Interior wildcard also uses the general path: "Customer...7" matches the
    // two "...0007" rows (both casings), not "...0123".
    assert_eq!(count(&db, "Customer%7"), 2);
}
