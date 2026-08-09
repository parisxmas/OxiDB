//! Streaming DML: `DELETE`/`UPDATE` must not materialize the table.
//!
//! The shape this exists to kill: `dml_candidates` reached an index only for
//! `column = constant`, so every other predicate — a range, an `IN`, an `OR` —
//! fell through to `store.scan()`, which returns an owned `Vec` of **every row
//! in the table**. On a table larger than memory that is not slow, it is fatal,
//! and it made a bulk purge impossible to express. Worse, it happened twice per
//! statement: once to take row locks, once to find the rows again.
//!
//! Three claims are pinned here, all measured through
//! `SqlEngine::dml_rows_examined` — the rows a DML statement walked to find its
//! matches, which is the only way from outside to tell "an index served this"
//! from "this read the table":
//!
//!   1. `DELETE ... LIMIT n` stops at the nth match (examined ≈ n, not the
//!      table), so a purge can be batched.
//!   2. A range predicate on an indexed column is served by that index
//!      (examined ≈ matches, not the table).
//!   3. Neither changes any answer: the streamed paths and the materializing
//!      fallback agree, row for row.
//!
//! A note on what these tests can and cannot prove. `dml_rows_examined` counts
//! rows *fed to the predicate*, so it catches "walked the whole table"; it does
//! not directly observe allocation. The differential tests are what guard
//! correctness, and each of the fast paths is checked against the same query
//! run through a shape that cannot use it.

mod common;

use common::*;
use oxidb_sql::{SqlEngine, Value};

/// `n` rows: id 1..=n, bucket = id % 10, ts = id, name = "row<i>".
fn seed(db: &SqlEngine, n: i64) {
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, bucket INT, ts INT, name TEXT)")
        .unwrap();
    // One multi-row INSERT per chunk keeps the fixture fast without making the
    // test about batching.
    let mut i = 1;
    while i <= n {
        let hi = (i + 499).min(n);
        let vals: Vec<String> = (i..=hi)
            .map(|k| format!("({k}, {}, {k}, 'row{k}')", k % 10))
            .collect();
        db.execute(&format!(
            "INSERT INTO t (id, bucket, ts, name) VALUES {}",
            vals.join(", ")
        ))
        .unwrap();
        i = hi + 1;
    }
}

fn count(db: &SqlEngine) -> i64 {
    match rows(db, "SELECT count(*) FROM t")[0][0] {
        Value::Int(n) => n,
        ref other => panic!("count returned {other:?}"),
    }
}

#[test]
fn delete_limit_deletes_exactly_that_many() {
    let (_d, db) = open();
    seed(&db, 1000);

    // Every row matches; LIMIT is the only thing bounding the delete.
    assert_eq!(affected(&db, "DELETE FROM t WHERE ts > 0 LIMIT 10"), 10);
    assert_eq!(count(&db), 990);

    // Repeatable — this is how a purge is batched.
    for _ in 0..3 {
        assert_eq!(affected(&db, "DELETE FROM t WHERE ts > 0 LIMIT 100"), 100);
    }
    assert_eq!(count(&db), 690);
}

#[test]
fn delete_limit_stops_at_the_nth_match_instead_of_reading_the_table() {
    // The point of the feature. Before streaming + early exit, finding 10 rows
    // to delete out of 20,000 read all 20,000 — twice.
    let (_d, db) = open();
    seed(&db, 20_000);

    assert_eq!(affected(&db, "DELETE FROM t WHERE ts > 0 LIMIT 10"), 10);
    let examined = db.dml_rows_examined();
    assert!(
        examined < 1_000,
        "a LIMIT 10 delete examined {examined} rows of 20,000 — it did not stop early"
    );
}

#[test]
fn delete_limit_larger_than_the_match_set_deletes_what_matches() {
    let (_d, db) = open();
    seed(&db, 100);
    // 10 rows have bucket = 3; the LIMIT is not reached.
    assert_eq!(
        affected(&db, "DELETE FROM t WHERE bucket = 3 LIMIT 500"),
        10
    );
    assert_eq!(count(&db), 90);
}

#[test]
fn delete_limit_zero_deletes_nothing() {
    let (_d, db) = open();
    seed(&db, 50);
    assert_eq!(affected(&db, "DELETE FROM t WHERE ts > 0 LIMIT 0"), 0);
    assert_eq!(count(&db), 50);
}

#[test]
fn delete_limit_binds_as_a_parameter() {
    // The purge-loop shape: the batch size is computed at runtime, so it must
    // bind like every other value — before this, `LIMIT ?` was refused and the
    // one number in the statement had to be pasted into the SQL text.
    let (_d, db) = open();
    seed(&db, 50);
    assert_eq!(
        affected_p(&db, "DELETE FROM t WHERE ts > 0 LIMIT ?", &[Value::Int(10)]),
        10
    );
    assert_eq!(count(&db), 40);
    // And $N spelling, as everywhere else params are accepted.
    assert_eq!(
        affected_p(&db, "DELETE FROM t WHERE ts > 0 LIMIT $1", &[Value::Int(5)]),
        5
    );
    assert_eq!(count(&db), 35);
    // A bound zero deletes nothing — same contract as the literal.
    assert_eq!(
        affected_p(&db, "DELETE FROM t WHERE ts > 0 LIMIT ?", &[Value::Int(0)]),
        0
    );
    assert_eq!(count(&db), 35);
}

#[test]
fn a_bad_delete_limit_parameter_is_a_clean_error_not_a_delete() {
    let (_d, db) = open();
    seed(&db, 20);
    for params in [
        vec![Value::Int(-1)],            // negative
        vec![Value::Text("all".into())], // wrong type
        vec![],                          // unbound
    ] {
        let err = db
            .execute_params("DELETE FROM t WHERE ts > 0 LIMIT ?", &params)
            .unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("LIMIT"),
            "the error should name the LIMIT: {msg}"
        );
    }
    // Nothing was deleted by any of the refused statements.
    assert_eq!(count(&db), 20);
}

#[test]
fn delete_limit_is_rejected_where_it_would_promise_an_order() {
    let (_d, db) = open();
    seed(&db, 10);
    // Accepting ORDER BY would promise a deletion order the executor does not
    // implement — and with LIMIT that silently changes *which* rows go.
    let err = db
        .execute("DELETE FROM t WHERE ts > 0 ORDER BY ts LIMIT 5")
        .unwrap_err();
    assert!(
        format!("{err}").contains("ORDER BY"),
        "expected the refusal to name ORDER BY, got: {err}"
    );
}

#[test]
fn a_range_delete_is_served_by_the_index_not_a_scan() {
    let (_d, db) = open();
    seed(&db, 20_000);
    db.execute("CREATE INDEX ix_ts ON t (ts)").unwrap();

    // 100 rows match. Before this change the range fell through to a full
    // table scan because only `col = const` reached an index.
    assert_eq!(
        affected(&db, "DELETE FROM t WHERE ts > 500 AND ts <= 600"),
        100
    );
    let examined = db.dml_rows_examined();
    assert!(
        examined < 1_000,
        "a 100-row range delete examined {examined} rows of 20,000 — the index did not serve it"
    );
    assert_eq!(count(&db), 19_900);
}

#[test]
fn the_range_index_path_and_a_scan_agree() {
    // Differential: the same logical deletes, once where an index can serve the
    // range and once where none exists. Sabotaging the bound handling in
    // `range_first_col`/`candidates_range` breaks exactly one side.
    let bounds = [
        "ts > 100 AND ts < 200",
        "ts >= 100 AND ts <= 200",
        "ts > 100",
        "ts < 100",
        "ts >= 9999",
        "ts <= 1",
        "ts > 50 AND ts < 50",
        "ts > 200 AND ts <= 100",
    ];
    for pred in bounds {
        let (_d1, indexed) = open();
        seed(&indexed, 500);
        indexed.execute("CREATE INDEX ix_ts ON t (ts)").unwrap();

        let (_d2, plain) = open();
        seed(&plain, 500);

        let a = affected(&indexed, &format!("DELETE FROM t WHERE {pred}"));
        let b = affected(&plain, &format!("DELETE FROM t WHERE {pred}"));
        assert_eq!(a, b, "affected differs for `{pred}`");

        let ra = rows(&indexed, "SELECT id FROM t ORDER BY id");
        let rb = rows(&plain, "SELECT id FROM t ORDER BY id");
        assert_eq!(ra, rb, "surviving rows differ for `{pred}`");
    }
}

#[test]
fn a_range_over_a_column_written_since_the_checkpoint_is_still_found() {
    // The `.sidx` base describes the last checkpoint; rows written since live
    // in the overlay. A range must merge both — reading only the base would
    // silently miss the newest rows, which is the failure mode the equality
    // path was already pinned against.
    let (_d, db) = open();
    seed(&db, 200);
    db.execute("CREATE INDEX ix_ts ON t (ts)").unwrap();
    db.checkpoint().unwrap();

    db.execute("INSERT INTO t (id, bucket, ts, name) VALUES (9001, 1, 9001, 'new')")
        .unwrap();
    db.execute("INSERT INTO t (id, bucket, ts, name) VALUES (9002, 1, 9002, 'new')")
        .unwrap();

    assert_eq!(affected(&db, "DELETE FROM t WHERE ts >= 9000"), 2);
    assert_eq!(count(&db), 200);
}

#[test]
fn a_range_over_a_row_updated_since_the_checkpoint_is_verified() {
    // The base is a hint: after a checkpoint, changing an indexed value leaves
    // a stale entry behind. Every candidate is re-checked against the live row,
    // so a row that has moved out of range must not be deleted — and one that
    // moved in must be.
    let (_d, db) = open();
    seed(&db, 200);
    db.execute("CREATE INDEX ix_ts ON t (ts)").unwrap();
    db.checkpoint().unwrap();

    db.execute("UPDATE t SET ts = 1 WHERE id = 150").unwrap(); // out of range
    db.execute("UPDATE t SET ts = 150 WHERE id = 5").unwrap(); // into range

    let n = affected(&db, "DELETE FROM t WHERE ts >= 100 AND ts <= 200");
    let survivors = rows(&db, "SELECT id FROM t WHERE id IN (5, 150) ORDER BY id");
    assert_eq!(
        survivors,
        vec![vec![Value::Int(150)]],
        "id 150 moved out of range and must survive; id 5 moved in and must go"
    );
    // 100..=200 is 101 rows; 150 left the range, 5 entered it.
    assert_eq!(n, 101);
}

#[test]
fn returning_still_reports_the_deleted_rows_under_limit() {
    // RETURNING needs the row cells, so this takes the "keep the cells" branch
    // rather than the ids-only one. Both must respect the limit.
    let (_d, db) = open();
    seed(&db, 100);
    // Clause order is the parser's: WHERE, then RETURNING, then LIMIT.
    let (_cols, out) = cols_rows(&db, "DELETE FROM t WHERE ts > 0 RETURNING id LIMIT 3");
    assert_eq!(out.len(), 3);
    assert_eq!(count(&db), 97);
}

#[test]
fn a_predicate_with_a_subquery_still_works() {
    // A predicate that calls back into the store cannot run inside a streamed
    // visitor (the store's lock is not reentrant), so it falls back to the
    // materializing path. It must still give the right answer — and with LIMIT.
    let (_d, db) = open();
    seed(&db, 50);
    db.execute("CREATE TABLE keep (id INT PRIMARY KEY)")
        .unwrap();
    db.execute("INSERT INTO keep (id) VALUES (1), (2), (3)")
        .unwrap();

    let n = affected(
        &db,
        "DELETE FROM t WHERE id NOT IN (SELECT id FROM keep) LIMIT 5",
    );
    assert_eq!(n, 5);
    assert_eq!(count(&db), 45);
    // The three protected rows are still there.
    assert_eq!(
        rows(&db, "SELECT count(*) FROM t WHERE id IN (1, 2, 3)")[0][0],
        Value::Int(3)
    );
}

#[test]
fn an_or_predicate_streams_and_still_matches_both_arms() {
    // `OR` cannot be pushed into an index; it takes the streamed scan. The
    // point is that it no longer materializes the table to do so.
    let (_d, db) = open();
    seed(&db, 300);
    let n = affected(&db, "DELETE FROM t WHERE ts < 10 OR ts > 295");
    assert_eq!(n, 9 + 5);
    assert_eq!(count(&db), 300 - 14);
}

#[test]
fn cascade_still_removes_children_when_the_parent_delete_is_limited() {
    // A referenced parent takes the "keep the cells" branch, because the
    // cascade closure is computed from the parent row. The limit applies to
    // parents, and every child of a deleted parent still goes.
    let (_d, db) = open();
    db.execute("CREATE TABLE p (id INT PRIMARY KEY)").unwrap();
    db.execute(
        "CREATE TABLE c (id INT PRIMARY KEY, pid INT, \
         CONSTRAINT fk FOREIGN KEY (pid) REFERENCES p(id) ON DELETE CASCADE)",
    )
    .unwrap();
    db.execute("INSERT INTO p (id) VALUES (1), (2), (3), (4)")
        .unwrap();
    db.execute("INSERT INTO c (id, pid) VALUES (10, 1), (11, 1), (20, 2), (30, 3)")
        .unwrap();

    assert_eq!(affected(&db, "DELETE FROM p WHERE id > 0 LIMIT 2"), 2);
    assert_eq!(rows(&db, "SELECT count(*) FROM p")[0][0], Value::Int(2));
    // Parents 1 and 2 went, taking children 10, 11 and 20 with them.
    assert_eq!(rows(&db, "SELECT count(*) FROM c")[0][0], Value::Int(1));
}

#[test]
fn update_matches_stream_and_answer_the_same() {
    // UPDATE cannot run inside the visitor (its assignments and FK checks call
    // the store), but it must no longer hold the whole table to find matches.
    let (_d, db) = open();
    seed(&db, 400);
    db.execute("CREATE INDEX ix_ts ON t (ts)").unwrap();

    let n = affected(&db, "UPDATE t SET name = 'x' WHERE ts > 100 AND ts <= 150");
    assert_eq!(n, 50);
    assert_eq!(
        rows(&db, "SELECT count(*) FROM t WHERE name = 'x'")[0][0],
        Value::Int(50)
    );
}

#[test]
fn a_limited_delete_inside_a_transaction_is_honoured() {
    // A transaction buffers its writes, so it keeps the materializing default
    // for reads. The limit is executor-level and must still apply there.
    let (_d, db) = open();
    seed(&db, 100);

    let mut tx = None;
    db.execute_params_in_session("BEGIN", &[], &mut tx).unwrap();
    db.execute_params_in_session("DELETE FROM t WHERE ts > 0 LIMIT 7", &[], &mut tx)
        .unwrap();
    db.execute_params_in_session("COMMIT", &[], &mut tx)
        .unwrap();

    assert_eq!(count(&db), 93);
}

#[test]
fn deleting_everything_without_a_filter_still_works() {
    let (_d, db) = open();
    seed(&db, 250);
    assert_eq!(affected(&db, "DELETE FROM t"), 250);
    assert_eq!(count(&db), 0);
}

#[test]
fn a_streamed_scan_after_a_checkpoint_reads_the_columns_its_predicate_needs() {
    // The masked-decode trap. A scan may skip decoding cells the query does not
    // read, and rows still in the post-checkpoint overlay are handed over whole
    // — so a mask that names the wrong columns is INVISIBLE until the rows have
    // been folded into the on-disk base by a checkpoint. Without one, every
    // other scan test here would pass with the mask completely empty.
    //
    // No index, so this is the streamed scan and nothing else.
    let (_d, db) = open();
    seed(&db, 300);
    db.checkpoint().unwrap();

    assert_eq!(
        affected(&db, "DELETE FROM t WHERE ts > 100 AND ts <= 200"),
        100
    );
    assert_eq!(count(&db), 200);
}
