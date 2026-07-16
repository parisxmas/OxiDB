//! The contiguous scan cache must never serve stale rows: a write bumps the
//! store generation, so the next scan rebuilds. Uses > 1024 rows to cross the
//! cache threshold and repeats scans to cross the build-on-second-scan guard.

mod common;

use common::*;

fn sum_v(db: &oxidb_sql::SqlEngine) -> i64 {
    match rows(db, "SELECT SUM(v) FROM t")[0][0] {
        oxidb_sql::Value::Int(n) => n,
        ref x => panic!("{x:?}"),
    }
}

#[test]
fn scan_cache_reflects_writes() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT, v INT)").unwrap();
    // 2000 rows (> 1024 threshold), v = 1 each -> SUM(v) = 2000.
    for i in 0..2000 {
        db.execute(&format!("INSERT INTO t VALUES ({i}, 1)")).unwrap();
    }
    // Scan repeatedly: builds the cache on the second scan.
    assert_eq!(sum_v(&db), 2000);
    assert_eq!(sum_v(&db), 2000);
    assert_eq!(sum_v(&db), 2000); // now served from the cache

    // Update: must invalidate. One row 1 -> 1001, SUM becomes 3000.
    db.execute("UPDATE t SET v = 1001 WHERE id = 0").unwrap();
    assert_eq!(sum_v(&db), 3000, "update must not be masked by a stale cache");

    // Delete: SUM drops by that row's value.
    db.execute("DELETE FROM t WHERE id = 0").unwrap();
    assert_eq!(sum_v(&db), 1999, "delete must not be masked by a stale cache");

    // Insert after caching: reflected too.
    db.execute("INSERT INTO t VALUES (99999, 5)").unwrap();
    assert_eq!(sum_v(&db), 2004);
}
