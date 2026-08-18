//! A range predicate on an indexed column reaches the index on the **read**
//! path, not only in DML.
//!
//! The gap this closes: `.sidx` is sorted by decoded key tuple, and
//! `candidates_range` had been able to seek it since streaming DML landed — but
//! the only caller was `collect_dml_matches`. Every `SELECT` whose WHERE was a
//! range walked the whole table, however narrow the range and whatever indexes
//! existed. `WHERE created >= ? AND created < ?` — the shape of essentially
//! every time-bounded query — was a full scan.
//!
//! Three claims, and the third is the one that needs the most care:
//!
//!   1. A read served by an index range answers **identically** to the same
//!      read that scanned — same rows, same order. Every differential below
//!      runs the query twice: once on a table with the index and once on the
//!      same data without it, which is the shape that cannot use the path.
//!   2. The path actually fires. A green differential proves nothing on its own
//!      here: a plan that silently declined would pass every one of these. So
//!      each case also asserts `SqlEngine::range_index_reads`, which is why
//!      that counter exists.
//!   3. It declines where a scan is the better plan — a range selecting most of
//!      the table, and a caller that can stop early — and answers identically
//!      when it does.

mod common;

use common::*;
use oxidb_sql::{SqlEngine, Value};

/// `n` rows: id 1..=n, ts = id * 1000 (an epoch-ms-shaped BIGINT), bucket =
/// id % 7 as text, amount = id % 13.
///
/// `ts` is deliberately **not** the primary key: the primary key has its own
/// lookup path, and a range over it would not exercise a secondary index at
/// all.
fn seed(db: &SqlEngine, n: i64, indexed: bool) {
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, ts BIGINT, bucket TEXT, amount INT)")
        .unwrap();
    let mut i = 1;
    while i <= n {
        let hi = (i + 499).min(n);
        let vals: Vec<String> = (i..=hi)
            .map(|k| format!("({k}, {}, 'b{}', {})", k * 1000, k % 7, k % 13))
            .collect();
        db.execute(&format!(
            "INSERT INTO t (id, ts, bucket, amount) VALUES {}",
            vals.join(", ")
        ))
        .unwrap();
        i = hi + 1;
    }
    if indexed {
        db.execute("CREATE INDEX ix_ts ON t (ts)").unwrap();
    }
}

/// Two engines over identical data: one with the index on `ts`, one without.
/// The second is the control — it cannot take the path under test, so any
/// difference in its answer is the path being wrong.
struct Pair {
    _dirs: (tempfile::TempDir, tempfile::TempDir),
    indexed: SqlEngine,
    plain: SqlEngine,
}

fn pair(n: i64) -> Pair {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let indexed = SqlEngine::open(a.path()).unwrap();
    let plain = SqlEngine::open(b.path()).unwrap();
    seed(&indexed, n, true);
    seed(&plain, n, false);
    Pair {
        _dirs: (a, b),
        indexed,
        plain,
    }
}

impl Pair {
    /// Run `sql` on both engines, assert they agree row for row **in order**,
    /// and return (rows, whether the indexed engine used a range index).
    fn differential(&self, sql: &str) -> (Vec<Vec<Value>>, bool) {
        let before = self.indexed.range_index_reads();
        let got = rows(&self.indexed, sql);
        let used = self.indexed.range_index_reads() > before;
        let want = rows(&self.plain, sql);
        assert_eq!(got, want, "index and scan disagreed on: {sql}");
        // The control must never have taken the path — otherwise "agrees with
        // the scan" is not what this assertion means.
        assert_eq!(self.plain.range_index_reads(), 0, "control used an index");
        (got, used)
    }

    /// [`differential`](Self::differential) plus the plan assertion: this query
    /// must have been served by the index.
    fn served(&self, sql: &str) -> Vec<Vec<Value>> {
        let (got, used) = self.differential(sql);
        assert!(used, "expected the range index to serve: {sql}");
        got
    }

    /// The differential, asserting the plan was **declined**.
    fn declined(&self, sql: &str) -> Vec<Vec<Value>> {
        let (got, used) = self.differential(sql);
        assert!(!used, "expected a scan, but an index served: {sql}");
        got
    }
}

fn ints(rows: &[Vec<Value>]) -> Vec<i64> {
    rows.iter()
        .map(|r| match r[0] {
            Value::Int(n) => n,
            ref other => panic!("expected Int, got {other:?}"),
        })
        .collect()
}

#[test]
fn every_bound_shape_agrees_with_the_scan_and_uses_the_index() {
    let p = pair(2_000);
    // A narrow window, written every way the grammar allows one. Each selects
    // far less than the cap, so each must be served by the index.
    for sql in [
        "SELECT id FROM t WHERE ts >= 100000 AND ts < 150000 ORDER BY id",
        "SELECT id FROM t WHERE ts > 100000 AND ts <= 150000 ORDER BY id",
        "SELECT id FROM t WHERE ts > 100000 AND ts < 150000 ORDER BY id",
        "SELECT id FROM t WHERE ts >= 100000 AND ts <= 150000 ORDER BY id",
        // Reversed operands: `literal < col` normalizes to `col > literal`.
        "SELECT id FROM t WHERE 100000 < ts AND 150000 > ts ORDER BY id",
        // One-sided, still narrow enough to be worth the index.
        "SELECT id FROM t WHERE ts > 1900000 ORDER BY id",
        "SELECT id FROM t WHERE ts <= 60000 ORDER BY id",
    ] {
        let got = p.served(sql);
        assert!(!got.is_empty(), "fixture selects nothing: {sql}");
    }
    // Spot-check the actual contents once, so a differential that agreed on
    // *nothing* (both empty, both wrong) cannot pass unnoticed.
    let got = p.served("SELECT id FROM t WHERE ts >= 100000 AND ts < 105000 ORDER BY id");
    assert_eq!(ints(&got), vec![100, 101, 102, 103, 104]);
}

#[test]
fn bounds_are_exclusive_and_inclusive_exactly_where_written() {
    let p = pair(1_000);
    assert_eq!(
        ints(&p.served("SELECT id FROM t WHERE ts >= 500000 AND ts <= 502000 ORDER BY id")),
        vec![500, 501, 502]
    );
    assert_eq!(
        ints(&p.served("SELECT id FROM t WHERE ts > 500000 AND ts < 502000 ORDER BY id")),
        vec![501]
    );
}

#[test]
fn parameters_bind_into_the_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqlEngine::open(dir.path()).unwrap();
    seed(&db, 1_000, true);
    let before = db.range_index_reads();
    let got = rows_p(
        &db,
        "SELECT id FROM t WHERE ts >= ? AND ts < ? ORDER BY id",
        &[i(300_000), i(303_000)],
    );
    assert_eq!(ints(&got), vec![300, 301, 302]);
    assert!(
        db.range_index_reads() > before,
        "a bound that arrived as a parameter must still reach the index"
    );
}

#[test]
fn nulls_in_the_indexed_column_are_excluded_as_sql_requires() {
    // A NULL sorts below every number in the index's total order, so a `< x`
    // bound admits it as a *candidate*. SQL says `NULL < x` is unknown, so the
    // row must not come back — which is the per-row predicate's job, and this
    // is what proves the index never replaces it.
    let p = pair(200);
    p.indexed
        .execute("INSERT INTO t (id, ts, bucket, amount) VALUES (9001, NULL, 'bx', 1)")
        .unwrap();
    p.plain
        .execute("INSERT INTO t (id, ts, bucket, amount) VALUES (9001, NULL, 'bx', 1)")
        .unwrap();
    let got = p.served("SELECT id FROM t WHERE ts < 5000 ORDER BY id");
    assert_eq!(ints(&got), vec![1, 2, 3, 4]);
    assert!(
        p.served("SELECT id FROM t WHERE ts > 199000 ORDER BY id")
            .len()
            == 1
    );
}

#[test]
fn a_stale_base_entry_is_verified_against_the_live_row() {
    // The `.sidx` describes the rows as of the last checkpoint. After one, move
    // a row out of the window, delete another inside it, and bring a third in:
    // the base names all three wrongly, and only the per-candidate recheck
    // against the live row makes the answer right.
    let p = pair(500);
    p.indexed.checkpoint().unwrap();
    p.plain.checkpoint().unwrap();
    for db in [&p.indexed, &p.plain] {
        db.execute("UPDATE t SET ts = 999999999 WHERE id = 101")
            .unwrap(); // out of the window
        db.execute("DELETE FROM t WHERE id = 102").unwrap();
        db.execute("UPDATE t SET ts = 103500 WHERE id = 400")
            .unwrap(); // into the window
    }
    let got = p.served("SELECT id FROM t WHERE ts >= 100000 AND ts < 105000 ORDER BY id");
    assert_eq!(ints(&got), vec![100, 103, 104, 400]);
}

#[test]
fn rows_written_after_the_checkpoint_are_found_through_the_overlay() {
    let p = pair(300);
    p.indexed.checkpoint().unwrap();
    p.plain.checkpoint().unwrap();
    for db in [&p.indexed, &p.plain] {
        db.execute("INSERT INTO t (id, ts, bucket, amount) VALUES (9001, 150500, 'bz', 2)")
            .unwrap();
    }
    let got = p.served("SELECT id FROM t WHERE ts >= 150000 AND ts < 151000 ORDER BY id");
    assert_eq!(ints(&got), vec![150, 9001]);
}

#[test]
fn a_dense_candidate_set_walks_the_snapshot_forward() {
    // The other checkpointed cases here select a narrow window, and a **sparse**
    // candidate set is located id by id — so they never reach the cursor walk
    // that a dense one takes (`visit_ids_masked` declines below one candidate
    // per 16 base rows). Without this case the whole dense path is untested and
    // every differential above still passes, which is precisely the trap.
    //
    // 2,000 rows checkpointed, ~750 of them in the window: dense enough for the
    // walk, and under the cap, which is what makes the query reach it at all.
    let p = pair(2_000);
    p.indexed.checkpoint().unwrap();
    p.plain.checkpoint().unwrap();
    let got = p.served("SELECT id, amount FROM t WHERE ts > 600000 AND ts <= 1350000 ORDER BY id");
    assert_eq!(got.len(), 750);
    assert_eq!(got[0][0], i(601));
    assert_eq!(got[749][0], i(1350));
    // Mutations after the checkpoint make the walk resolve the overlay as well
    // as the mapping, in the same pass.
    for db in [&p.indexed, &p.plain] {
        db.execute("UPDATE t SET amount = 99 WHERE id = 700")
            .unwrap();
        db.execute("DELETE FROM t WHERE id = 701").unwrap();
        db.execute("UPDATE t SET ts = 700500 WHERE id = 1999")
            .unwrap();
    }
    let got = p.served("SELECT id, amount FROM t WHERE ts > 600000 AND ts <= 1350000 ORDER BY id");
    assert_eq!(got.len(), 750); // one deleted, one moved in
    assert_eq!(got[99], vec![i(700), i(99)]);
}

#[test]
fn aggregates_over_a_window_are_served_and_agree() {
    let p = pair(2_000);
    p.served("SELECT count(*) FROM t WHERE ts >= 100000 AND ts < 150000");
    p.served("SELECT sum(amount), count(*) FROM t WHERE ts >= 100000 AND ts < 150000");
    p.served("SELECT bucket, count(*) FROM t WHERE ts >= 100000 AND ts < 150000 GROUP BY bucket");
    // A HAVING over the same window: the streamed aggregate declines HAVING
    // that mentions an aggregate, so this rides the general path — which must
    // still get the index and still agree.
    p.differential(
        "SELECT bucket, count(*) FROM t WHERE ts >= 100000 AND ts < 150000 \
         GROUP BY bucket HAVING count(*) > 2 ORDER BY bucket",
    );
}

#[test]
fn top_n_over_a_window_is_served_and_ties_break_the_same_way() {
    // `amount` is `id % 13`, so a window holds many rows of equal amount and
    // the ORDER BY is decided by arrival order. Both sources hand rows over in
    // row-id order, which is what keeps the two plans agreeing here — this is
    // the assertion that would catch an index that returned key order instead.
    let p = pair(2_000);
    let got = p.served(
        "SELECT id, amount FROM t WHERE ts >= 100000 AND ts < 150000 \
         ORDER BY amount DESC LIMIT 10",
    );
    assert_eq!(got.len(), 10);
}

#[test]
fn a_limit_with_no_ordering_keeps_the_scan() {
    // The scan can stop at the tenth match; the index would build its whole
    // candidate list before returning the first row. Declining is the plan, not
    // an accident — and the answer must be identical either way.
    let p = pair(2_000);
    let got = p.declined("SELECT id FROM t WHERE ts >= 100000 LIMIT 10");
    assert_eq!(got.len(), 10);
}

#[test]
fn a_range_over_most_of_the_table_keeps_the_scan() {
    // Past half the table the index has stopped narrowing enough to pay for the
    // row locates it costs, so the walk is capped and the plan declines. The
    // answer is the same; only the plan changes.
    //
    // The fixture must clear the cap's **floor** (4096 candidates), below which
    // every range is served because at that size neither plan is worth
    // choosing between. A 2,000-row table cannot exercise this rule at all —
    // the first version of this test used one, and the ratio it meant to assert
    // was never reached.
    let p = pair(20_000);
    let got = p.declined("SELECT id FROM t WHERE ts > 5000 ORDER BY id");
    assert_eq!(got.len(), 19_995);
    // ... and the narrow end of the same column still uses it, so the decline
    // above is about selectivity rather than the column being unusable.
    p.served("SELECT id FROM t WHERE ts <= 5000000 ORDER BY id");
}

#[test]
fn an_unindexed_column_falls_through_to_the_scan() {
    let p = pair(1_000);
    p.declined("SELECT id FROM t WHERE amount > 3 AND amount < 6 ORDER BY id");
}

#[test]
fn a_composite_index_serves_a_range_on_its_first_column_only() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqlEngine::open(dir.path()).unwrap();
    seed(&db, 1_000, false);
    db.execute("CREATE INDEX ix_ts_amount ON t (ts, amount)")
        .unwrap();

    let before = db.range_index_reads();
    let got = rows(
        &db,
        "SELECT id FROM t WHERE ts >= 10000 AND ts < 13000 ORDER BY id",
    );
    assert_eq!(ints(&got), vec![10, 11, 12]);
    assert!(
        db.range_index_reads() > before,
        "first column must be seekable"
    );

    // `amount` is the index's *second* column: a range on it does not select a
    // contiguous run of keys, so it must be declined rather than mis-served.
    let before = db.range_index_reads();
    let got = rows(&db, "SELECT id FROM t WHERE amount > 11 ORDER BY id");
    assert_eq!(
        db.range_index_reads(),
        before,
        "a second column is not a range"
    );
    assert_eq!(got.len(), 77); // id % 13 == 12, over 1..=1000
}

#[test]
fn a_dropped_column_does_not_shift_the_bound_check() {
    // After a DROP COLUMN the physical row is wider than the visible one, so
    // the bound is checked at a physical position while the caller reads
    // logical ones. Getting that wrong reads a neighbouring column.
    let p = pair(600);
    for db in [&p.indexed, &p.plain] {
        db.execute("ALTER TABLE t DROP COLUMN bucket").unwrap();
    }
    let got =
        p.served("SELECT id, ts, amount FROM t WHERE ts >= 100000 AND ts < 103000 ORDER BY id");
    assert_eq!(ints(&got), vec![100, 101, 102]);
    assert_eq!(got[0][1], i(100_000));
    // Same again after a checkpoint compacts the tombstoned column away.
    for db in [&p.indexed, &p.plain] {
        db.checkpoint().unwrap();
    }
    let got =
        p.served("SELECT id, ts, amount FROM t WHERE ts >= 100000 AND ts < 103000 ORDER BY id");
    assert_eq!(ints(&got), vec![100, 101, 102]);
}

#[test]
fn a_range_pushed_into_one_side_of_a_join_is_served_and_agrees() {
    let dirs = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let mut engines = Vec::new();
    for (dir, indexed) in [(&dirs.0, true), (&dirs.1, false)] {
        let db = SqlEngine::open(dir.path()).unwrap();
        seed(&db, 1_000, indexed);
        db.execute("CREATE TABLE u (id INT PRIMARY KEY, tag TEXT)")
            .unwrap();
        let vals: Vec<String> = (1..=1_000)
            .map(|k| format!("({k}, 'tag{}')", k % 5))
            .collect();
        db.execute(&format!(
            "INSERT INTO u (id, tag) VALUES {}",
            vals.join(", ")
        ))
        .unwrap();
        engines.push(db);
    }
    let (indexed, plain) = (&engines[0], &engines[1]);
    let sql = "SELECT t.id, u.tag FROM t JOIN u ON u.id = t.id \
               WHERE t.ts >= 100000 AND t.ts < 104000 ORDER BY t.id";
    let before = indexed.range_index_reads();
    let got = rows(indexed, sql);
    assert!(
        indexed.range_index_reads() > before,
        "a conjunct pushed into the FROM side must reach its index"
    );
    assert_eq!(rows(plain, sql), got);
    assert_eq!(ints(&got), vec![100, 101, 102, 103]);
}

#[test]
fn a_range_inside_a_transaction_still_answers_correctly() {
    // The transaction store buffers its writes in RAM and inherits the
    // materializing defaults, so it declines this path. What must hold is the
    // answer, including the transaction's own uncommitted rows.
    let dir = tempfile::tempdir().unwrap();
    let db = SqlEngine::open(dir.path()).unwrap();
    seed(&db, 500, true);
    let got = first_select(
        db.execute(
            "BEGIN; \
             INSERT INTO t (id, ts, bucket, amount) VALUES (9001, 100500, 'bt', 3); \
             DELETE FROM t WHERE id = 101; \
             SELECT id FROM t WHERE ts >= 100000 AND ts < 102000 ORDER BY id; \
             COMMIT;",
        )
        .unwrap(),
    );
    assert_eq!(ints(&got), vec![100, 9001]);
    // ... and the same window after the commit, now through the index.
    let before = db.range_index_reads();
    let got = rows(
        &db,
        "SELECT id FROM t WHERE ts >= 100000 AND ts < 102000 ORDER BY id",
    );
    assert!(db.range_index_reads() > before);
    assert_eq!(ints(&got), vec![100, 9001]);
}

#[test]
fn the_window_survives_a_reopen() {
    // The index's base is a file; reopening must not leave the plan reaching a
    // stale or absent one.
    let dir = tempfile::tempdir().unwrap();
    {
        let db = SqlEngine::open(dir.path()).unwrap();
        seed(&db, 1_000, true);
        db.checkpoint().unwrap();
        db.execute("INSERT INTO t (id, ts, bucket, amount) VALUES (9001, 100500, 'bz', 4)")
            .unwrap();
    }
    let db = SqlEngine::open(dir.path()).unwrap();
    let before = db.range_index_reads();
    let got = rows(
        &db,
        "SELECT id FROM t WHERE ts >= 100000 AND ts < 101000 ORDER BY id",
    );
    assert!(
        db.range_index_reads() > before,
        "the reopened index must serve"
    );
    assert_eq!(ints(&got), vec![100, 9001]);
}
