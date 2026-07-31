//! Two regressions found by the EF Core spec suite the day disk-first became
//! the default (0.41.33) — both mode-independent, both proven red against the
//! pre-fix engine over the wire.
//!
//! 1. `IndexKey` derived `PartialEq` while its `Ord` is `Value::total_order`,
//!    which compares numerics across types. The B-tree (and the `.sidx` binary
//!    search) *found* the entry for an integer probe against a TIMESTAMP
//!    column, and the candidate verification's `!=` then rejected every row it
//!    had just found. EF Core binds every `DateTime` parameter as epoch-ms
//!    integer, so `WHERE "OrderDate" = @p` answered 0 rows the moment the
//!    column had an index.
//!
//! 2. The streamed-aggregate path pre-bound HAVING against the *input* schema
//!    and "rebound" it at emit — a no-op on already-bound `Col` nodes — so a
//!    HAVING on a group key read whatever projection slot shared the key's
//!    input position (`HAVING c = 'x'` compared the count and errored with
//!    "cannot compare Int and Text").

mod common;

use common::*;
use oxidb_sql::Value;

fn seed(db: &oxidb_sql::SqlEngine) {
    db.execute("CREATE TABLE o (id INT PRIMARY KEY, c TEXT, d TIMESTAMP)")
        .unwrap();
    db.execute("CREATE INDEX ix_d ON o(d)").unwrap();
    db.execute(
        "INSERT INTO o VALUES \
         (1, 'ALFKI', TIMESTAMP '1998-05-04 00:00:00'), \
         (2, 'ALFKI', TIMESTAMP '1998-05-04 00:00:00'), \
         (3, 'BONAP', TIMESTAMP '1998-05-04 00:00:00'), \
         (4, 'BONAP', TIMESTAMP '1997-01-01 00:00:00')",
    )
    .unwrap();
}

/// 1998-05-04T00:00:00Z in epoch milliseconds — what EF binds for a DateTime.
const MS: i64 = 894_240_000_000;

fn assert_probes(db: &oxidb_sql::SqlEngine) {
    // Parameterized, as EF sends it.
    let n = rows_p(
        db,
        "SELECT count(*) FROM o WHERE d = ?",
        &[Value::Int(MS)],
    );
    assert_eq!(n[0][0], Value::Int(3), "int param vs TIMESTAMP index");
    // Inline integer literal.
    let n = rows(db, &format!("SELECT count(*) FROM o WHERE d = {MS}"));
    assert_eq!(n[0][0], Value::Int(3), "int literal vs TIMESTAMP index");
    // The row-returning shape, not just the covered count.
    let r = rows_p(
        db,
        "SELECT id FROM o WHERE d = ? ORDER BY id",
        &[Value::Int(MS)],
    );
    assert_eq!(
        r,
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)]
        ]
    );
}

#[test]
fn int_probe_finds_timestamp_index_entries_in_overlay() {
    let (_d, db) = open();
    seed(&db);
    assert_probes(&db); // rows still in the post-checkpoint overlay
}

#[test]
fn int_probe_finds_timestamp_index_entries_in_mapped_base() {
    let (dir, db) = open();
    seed(&db);
    db.checkpoint().unwrap(); // rows + index now in mmap'd base files
    assert_probes(&db);
    drop(db);
    let db = open_at(dir.path()); // and again from a cold open
    assert_probes(&db);
}

#[test]
fn having_on_group_key_with_aggregate_projection() {
    let (_d, db) = open();
    seed(&db);
    // `c` is input column 1; in the projection the key sits at 0 and the
    // count at 1. No ORDER BY, so this stays on the streamed path.
    let r = rows(
        &db,
        "SELECT c, count(*) FROM o GROUP BY c HAVING c = 'ALFKI'",
    );
    assert_eq!(r, vec![vec![Value::Text("ALFKI".into()), Value::Int(2)]]);
    // The ORDER BY spelling takes the general path; the two must agree.
    let r = rows(
        &db,
        "SELECT c, count(*) FROM o GROUP BY c HAVING c = 'ALFKI' ORDER BY c",
    );
    assert_eq!(r, vec![vec![Value::Text("ALFKI".into()), Value::Int(2)]]);
}
