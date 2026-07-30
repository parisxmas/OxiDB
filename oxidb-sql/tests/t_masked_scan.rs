//! Disk-first scans decode only the columns a query reads. Everything else in
//! the row arrives as a `Value::Null` placeholder, which is exactly the shape a
//! bug would take: a column the query *does* read, skipped by mistake, comes
//! back as NULL rather than raising anything. Results would be quietly wrong.
//!
//! So these tests are differential. The same data and the same queries are run
//! with `disk_first` on and off — resident mode never masks, since its rows are
//! already materialized — and the two must agree exactly. Anything the masking
//! gets wrong shows up as a disagreement rather than as a plausible-looking
//! number nobody checks.
//!
//! Two conditions have to hold for a query to exercise the path at all, and both
//! are set up deliberately below: the rows must be in the mapped base (so a
//! checkpoint has to have happened), and the query must leave some column out.

use oxidb_sql::{QueryResult, SqlEngine, SqlOptions, Value};

/// Wide enough that queries leave columns out, with the allocating types
/// (text, blob, decimal) in the middle so a skip has to land on them.
const SCHEMA: &[&str] = &[
    "CREATE TABLE t (
        id INT PRIMARY KEY,
        grp TEXT NOT NULL,
        note TEXT,
        payload BLOB,
        price DECIMAL(12,2),
        qty INT,
        ok BOOL,
        seen TIMESTAMP
     )",
    "CREATE INDEX t_grp ON t (grp)",
    "CREATE TABLE child (id INT PRIMARY KEY, t_id INT NOT NULL, label TEXT, amount DOUBLE)",
];

/// Queries chosen so that between them every kind of column reference is made
/// while *some* column is always skipped: filters on columns that are not
/// projected, group keys, join keys, ORDER BY keys, HAVING, and `SELECT *`
/// (which skips nothing and must still be right).
const QUERIES: &[&str] = &[
    "SELECT sum(qty) FROM t",
    "SELECT sum(qty) FROM t WHERE grp = 'b'",
    "SELECT count(*) FROM t WHERE note IS NULL",
    "SELECT grp, count(*), sum(qty), min(price), max(seen) FROM t GROUP BY grp ORDER BY grp",
    "SELECT grp, count(*) FROM t WHERE qty > 3 GROUP BY grp HAVING count(*) > 1 ORDER BY grp",
    // Filters on columns that are not in the projection.
    "SELECT id FROM t WHERE ok = TRUE ORDER BY id",
    "SELECT id FROM t WHERE seen > TIMESTAMP '2024-01-02 00:00:00' ORDER BY id",
    "SELECT id FROM t WHERE price > 10.00 ORDER BY id",
    "SELECT id FROM t WHERE payload IS NOT NULL ORDER BY id",
    "SELECT note FROM t WHERE grp = 'a' ORDER BY note",
    // ORDER BY a column that is not projected, with a LIMIT (the top-k path).
    "SELECT id FROM t ORDER BY qty DESC, id LIMIT 3",
    "SELECT grp FROM t ORDER BY seen LIMIT 4",
    // Joins: the key column is read but not projected.
    "SELECT count(*) FROM t JOIN child ON child.t_id = t.id WHERE t.grp = 'b'",
    "SELECT t.grp, sum(child.amount) FROM t JOIN child ON child.t_id = t.id
     GROUP BY t.grp ORDER BY t.grp",
    "SELECT t.id, child.label FROM t JOIN child ON child.t_id = t.id
     WHERE t.qty > 2 ORDER BY t.id, child.label",
    // Nothing skipped at all — the mask covers every column.
    "SELECT * FROM t ORDER BY id",
    "SELECT id, grp, note, payload, price, qty, ok, seen FROM t ORDER BY id",
    // Every cell of a text column, to catch a skip that corrupted the offset of
    // the cell after it.
    "SELECT id, grp, note FROM t ORDER BY id",
    // Aggregates with an equality on the indexed column and no ORDER BY: the
    // streaming path serves these from the index, fetching candidates with a
    // column mask — a mask missing a column the fold or the residual filter
    // reads would answer from NULLs.
    "SELECT count(*), min(note), max(qty) FROM t WHERE grp = 'a'",
    "SELECT count(*) FROM t WHERE grp = 'a' AND qty > 2",
    "SELECT count(*), sum(qty) FROM t WHERE grp = 'b' AND seen > TIMESTAMP '2024-01-01 12:00:00'",
];

fn rows_for(
    disk_first: bool,
    dir: &std::path::Path,
    after_checkpoint: bool,
) -> Vec<Vec<Vec<Value>>> {
    let db = SqlEngine::open_with_options(
        dir,
        SqlOptions {
            disk_first,
            ..SqlOptions::default()
        },
    )
    .expect("open");
    for stmt in SCHEMA {
        db.execute(stmt).expect(stmt);
    }
    // Rows with NULLs in every nullable column, multi-byte text, an empty
    // string, and an empty blob — the values most likely to be mis-skipped.
    // Blobs are bound as parameters: the engine has no hex-literal syntax, and a
    // blob is one of the types whose decode allocates, so it belongs in here.
    let rows: [(&str, Value); 5] = [
        (
            "INSERT INTO t VALUES (1,'a','first',?,'10.50',5,TRUE,TIMESTAMP '2024-01-01 10:00:00')",
            Value::Bytes(vec![0x00, 0xFF].into()),
        ),
        (
            "INSERT INTO t VALUES (2,'b',NULL,?,'0.01',1,FALSE,TIMESTAMP '2024-01-03 10:00:00')",
            Value::Null,
        ),
        (
            "INSERT INTO t VALUES (3,'a','çok baytlı',?,NULL,7,TRUE,NULL)",
            Value::Bytes(Vec::new().into()), // empty blob
        ),
        (
            "INSERT INTO t VALUES (4,'b','',?,'99999999.99',4,NULL,TIMESTAMP '2024-01-02 10:00:00')",
            Value::Bytes(vec![0xAA, 0xBB, 0xCC].into()),
        ),
        (
            "INSERT INTO t VALUES (5,'c',NULL,?,NULL,NULL,NULL,NULL)",
            Value::Null,
        ),
    ];
    for (stmt, blob) in rows {
        db.execute_params(stmt, &[blob]).expect(stmt);
    }
    for stmt in [
        "INSERT INTO child VALUES (1,1,'x',1.5)",
        "INSERT INTO child VALUES (2,1,'y',2.5)",
        "INSERT INTO child VALUES (3,2,NULL,3.5)",
        "INSERT INTO child VALUES (4,4,'z',4.5)",
    ] {
        db.execute(stmt).expect(stmt);
    }
    if after_checkpoint {
        // Without this the rows are all in the overlay, which is handed over
        // whole — the masked decode would never run and the test would pass
        // while proving nothing.
        db.checkpoint().expect("checkpoint");
        // A few rows *after* the checkpoint too, so scans merge a masked base
        // with an unmasked overlay.
        db.execute_params(
            "INSERT INTO t VALUES (6,'a','late',?,'7.25',2,TRUE,TIMESTAMP '2024-01-04 10:00:00')",
            &[Value::Bytes(vec![0x01].into())],
        )
        .expect("late insert");
        db.execute("UPDATE t SET note = 'changed', qty = 9 WHERE id = 2")
            .expect("late update");
        db.execute("DELETE FROM t WHERE id = 5")
            .expect("late delete");
        db.execute("INSERT INTO child VALUES (5,6,'w',5.5)")
            .expect("late child");
    }

    QUERIES
        .iter()
        .map(
            |q| match db.execute(q).unwrap_or_else(|e| panic!("{q}: {e}")).pop() {
                Some(QueryResult::Select { rows, .. }) => rows,
                other => panic!("{q} did not return rows: {other:?}"),
            },
        )
        .collect()
}

/// The masked path, against the same queries run without it.
#[test]
fn masked_scans_answer_exactly_what_unmasked_scans_answer() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let masked = rows_for(true, a.path(), true);
    let plain = rows_for(false, b.path(), true);

    for (q, (m, p)) in QUERIES.iter().zip(masked.iter().zip(plain.iter())) {
        assert_eq!(m, p, "disk-first and resident disagree on: {q}");
    }
}

/// The same, with everything still in the overlay: no checkpoint, so nothing is
/// masked. Guards the merge logic rather than the masking, and makes the
/// checkpointed case above meaningful by contrast.
#[test]
fn an_unfolded_table_answers_the_same() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let disk = rows_for(true, a.path(), false);
    let plain = rows_for(false, b.path(), false);

    for (q, (d, p)) in QUERIES.iter().zip(disk.iter().zip(plain.iter())) {
        assert_eq!(d, p, "disk-first and resident disagree on: {q}");
    }
}

/// A dropped column shifts stored positions away from query-visible ones, so
/// masking is declined there. Pinned because the decline is what keeps it
/// correct, and a later change that "optimized" it away would return values from
/// the wrong columns.
#[test]
fn a_dropped_column_still_reads_correctly() {
    let dir = tempfile::tempdir().unwrap();
    let opts = SqlOptions {
        disk_first: true,
        ..SqlOptions::default()
    };
    let db = SqlEngine::open_with_options(dir.path(), opts).unwrap();
    db.execute("CREATE TABLE d (id INT PRIMARY KEY, gone TEXT, keep TEXT, n INT)")
        .unwrap();
    for i in 1..=20 {
        db.execute(&format!(
            "INSERT INTO d VALUES ({i}, 'gone{i}', 'keep{i}', {i})"
        ))
        .unwrap();
    }
    db.checkpoint().unwrap();
    db.execute("ALTER TABLE d DROP COLUMN gone").unwrap();

    // Reads a subset (so masking would apply if it were allowed) of a table
    // whose stored layout no longer matches its visible one.
    let rows = match db
        .execute("SELECT id, keep FROM d WHERE n > 17 ORDER BY id")
        .unwrap()
        .pop()
    {
        Some(QueryResult::Select { rows, .. }) => rows,
        other => panic!("expected rows, got {other:?}"),
    };
    assert_eq!(
        rows,
        vec![
            vec![Value::Int(18), Value::Text("keep18".into())],
            vec![Value::Int(19), Value::Text("keep19".into())],
            vec![Value::Int(20), Value::Text("keep20".into())],
        ]
    );
    assert_eq!(
        match db.execute("SELECT sum(n) FROM d").unwrap().pop() {
            Some(QueryResult::Select { rows, .. }) => rows,
            other => panic!("expected rows, got {other:?}"),
        },
        vec![vec![Value::Int(210)]]
    );
}

/// Grouping reads a key cell **borrowed** from the mapping and copies it once per
/// group instead of once per row. That path is declined for a table holding a
/// `DECIMAL` (a decimal cannot be borrowed out of an owned value) — which the
/// table above has, so nothing there reaches it. This one has no decimal, so it
/// does.
///
/// Differential again: the borrowed grouping must produce exactly what the owned
/// grouping produces. The risk it guards is specific — a key that compares or
/// hashes differently from the group it belongs to silently starts a second
/// group, so a query returns two rows where one is right.
mod borrowed_grouping {
    use super::*;

    const SCHEMA: &[&str] = &["CREATE TABLE g (
            id INT PRIMARY KEY,
            grp TEXT NOT NULL,
            tag TEXT,
            n INT,
            ok BOOL,
            seen TIMESTAMP
         )"];

    /// **No `ORDER BY`, no `LIMIT`** — deliberately. The streaming aggregate that
    /// owns the borrowed key path declines any query carrying either, so an
    /// ordered query never reaches it: a first version of this test had `ORDER BY`
    /// on everything and passed happily with the path sabotaged. Group order is
    /// therefore unspecified, so the rows are sorted in the test instead.
    const QUERIES: &[&str] = &[
        // Text key, few groups (the linear compare path).
        "SELECT grp, count(*), sum(n) FROM g GROUP BY grp",
        // Filtered, so the filter's column is materialized while the key is not.
        "SELECT grp, count(*) FROM g WHERE n > 2 GROUP BY grp",
        // A nullable text key: NULL is a group of its own.
        "SELECT tag, count(*) FROM g GROUP BY tag",
        // The other key types the borrowed path admits.
        "SELECT ok, count(*) FROM g GROUP BY ok",
        "SELECT seen, count(*) FROM g GROUP BY seen",
        // Composite key mixing text and integer.
        "SELECT grp, ok, count(*), min(n), max(n) FROM g GROUP BY grp, ok",
        // Many groups, to cross into the hashed path (LINEAR_GROUPS = 32).
        "SELECT tag, count(*) FROM g GROUP BY tag",
        "SELECT grp, tag, count(*) FROM g GROUP BY grp, tag",
        // No grouping: one row, aggregates only.
        "SELECT count(*), sum(n), min(grp), max(grp) FROM g",
        // HAVING, applied to finished groups.
        "SELECT grp, count(*) FROM g GROUP BY grp HAVING count(*) > 3",
        // An aggregate over the text column itself, so it is materialized rather
        // than only compared.
        "SELECT grp, count(*), min(tag), max(tag) FROM g GROUP BY grp",
    ];

    fn results(disk_first: bool, dir: &std::path::Path) -> Vec<Vec<Vec<Value>>> {
        let db = SqlEngine::open_with_options(
            dir,
            SqlOptions {
                disk_first,
                ..SqlOptions::default()
            },
        )
        .unwrap();
        for s in SCHEMA {
            db.execute(s).unwrap();
        }
        // Enough distinct `tag` values to push past the linear-compare threshold,
        // a repeated `grp` so groups actually accumulate, and NULLs in both.
        for i in 1..=200 {
            let tag = match i % 7 {
                0 => "NULL".to_string(),
                _ => format!("'t{}'", i % 45),
            };
            let seen = match i % 5 {
                0 => "NULL".to_string(),
                _ => format!("TIMESTAMP '2024-01-{:02} 00:00:00'", (i % 27) + 1),
            };
            db.execute(&format!(
                "INSERT INTO g VALUES ({i}, 'g{}', {tag}, {}, {}, {seen})",
                i % 4,
                i % 6,
                if i % 3 == 0 { "TRUE" } else { "FALSE" }
            ))
            .unwrap();
        }
        // The rows must be in the mapped base for the borrowed path to run.
        db.checkpoint().unwrap();
        // Plus some in the overlay, whose cells are borrowed from the store.
        for i in 201..=210 {
            db.execute(&format!(
                "INSERT INTO g VALUES ({i}, 'g{}', 't{}', {}, TRUE, NULL)",
                i % 4,
                i % 45,
                i % 6
            ))
            .unwrap();
        }
        db.execute("UPDATE g SET grp = 'moved', tag = NULL WHERE id = 5")
            .unwrap();
        db.execute("DELETE FROM g WHERE id = 9").unwrap();

        QUERIES
            .iter()
            .map(
                |q| match db.execute(q).unwrap_or_else(|e| panic!("{q}: {e}")).pop() {
                    Some(QueryResult::Select { mut rows, .. }) => {
                        // Group order is unspecified without ORDER BY; sort both sides
                        // the same way so the comparison is about the grouping, not
                        // about the order groups happened to be discovered in.
                        rows.sort_by_key(|r| format!("{r:?}"));
                        rows
                    }
                    other => panic!("{q} returned {other:?}"),
                },
            )
            .collect()
    }

    #[test]
    fn borrowed_grouping_matches_owned_grouping() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        let borrowed = results(true, a.path());
        let owned = results(false, b.path());
        for (q, (x, y)) in QUERIES.iter().zip(borrowed.iter().zip(owned.iter())) {
            assert_eq!(x, y, "disk-first and resident disagree on: {q}");
        }
        // A guard on the fixture rather than the engine: if the data stopped
        // producing multiple groups, the comparisons above would still pass while
        // testing nothing.
        let first = &borrowed[0];
        assert!(
            first.len() >= 4,
            "fixture should produce several groups, got {}",
            first.len()
        );
    }
}

/// Aggregates over one INNER equi-join stream the left side instead of
/// materializing it (`streamed_join_aggregate`). A second execution of the same
/// semantics, so: differential against the general path, which any query with a
/// LIMIT takes (the streamed path declines LIMIT — and a large one changes no
/// answers here).
mod streamed_join {
    use super::*;

    const SCHEMA: &[&str] = &[
        "CREATE TABLE cust (id INT PRIMARY KEY, country TEXT NOT NULL, credit INT)",
        "CREATE INDEX cust_country ON cust (country)",
        "CREATE TABLE ord (id INT PRIMARY KEY, cust_id INT, total DOUBLE, note TEXT)",
        "CREATE INDEX ord_cust ON ord (cust_id)",
    ];

    /// Every query twice: bare (streams) and with `LIMIT 100000` (declines to
    /// the general path). Same rows either way, which is the whole assertion.
    ///
    /// The fixture is shaped so the assertions cannot pass vacuously — the
    /// first version of this test did (twice): `cust` is **larger** than `ord`,
    /// so orientation streams `cust` and indexes `ord`, whose `cust_id` is
    /// deliberately non-unique — bucket chains have real length, so a fold that
    /// stops after a chain's first match changes answers. And the `country =
    /// 'TR'` equality matches more rows than `INL_MAX_LEFT`, so the eq-driven
    /// index build engages instead of declining. Both were verified by
    /// sabotaging the implementation and watching this fail.
    const QUERIES: &[&str] = &[
        // The benchmark's shape: count over an equality-filtered join, with the
        // equality selecting too many rows for the index-nested-loop.
        "SELECT count(*) FROM ord o JOIN cust c ON c.id = o.cust_id WHERE c.country = 'TR' AND o.id > 3",
        // Aggregates reading both sides, chains exercised (index side = ord).
        "SELECT count(*), sum(o.total), min(c.credit), max(o.id) FROM cust c JOIN ord o ON o.cust_id = c.id",
        // Grouped by a right-side column.
        "SELECT c.country, count(*), sum(o.total) FROM ord o JOIN cust c ON c.id = o.cust_id GROUP BY c.country",
        // Grouped by the duplicate key itself, HAVING on the finished groups.
        "SELECT o.cust_id, count(*) FROM cust c JOIN ord o ON o.cust_id = c.id GROUP BY o.cust_id HAVING count(*) > 2",
        // Filters on both sides at once (each conjunct binds one side).
        "SELECT count(*) FROM cust c JOIN ord o ON o.cust_id = c.id WHERE c.credit > 10 AND o.total > 5.0",
        // The equality plus a second conjunct on the same side: the index
        // satisfies the equality, and the credit predicate must still be
        // re-applied to every fetched row — dropping that re-check is exactly
        // the bug this query exists to catch.
        "SELECT count(*) FROM ord o JOIN cust c ON c.id = o.cust_id WHERE c.country = 'TR' AND c.credit > 10",
        // A selective equality: declines to the general path (INL territory) —
        // the differential holds across the decline too.
        "SELECT count(*) FROM ord o JOIN cust c ON c.id = o.cust_id WHERE c.id = 7",
        // Zero matches still yields the one scalar row.
        "SELECT count(*), sum(o.total) FROM ord o JOIN cust c ON c.id = o.cust_id WHERE c.country = 'XX'",
    ];

    /// Per query: (streamed rows, general-path rows).
    type Pair = (Vec<Vec<Value>>, Vec<Vec<Value>>);

    fn results(dir: &std::path::Path, disk_first: bool) -> Vec<Pair> {
        let db = SqlEngine::open_with_options(
            dir,
            SqlOptions {
                disk_first,
                ..SqlOptions::default()
            },
        )
        .unwrap();
        for s in SCHEMA {
            db.execute(s).unwrap();
        }
        // 12,000 customers, ~92% 'TR' (well past INL_MAX_LEFT = 8192), inserted
        // in batches. Credit is NULL every fifth row.
        let mut batch: Vec<String> = Vec::new();
        for i in 1..=12_000 {
            let country = if i % 12 == 0 { "US" } else { "TR" };
            let credit = match i % 5 {
                0 => "NULL".into(),
                n => format!("{}", n * 7),
            };
            batch.push(format!("({i}, '{country}', {credit})"));
            if batch.len() == 500 {
                db.execute(&format!("INSERT INTO cust VALUES {}", batch.join(",")))
                    .unwrap();
                batch.clear();
            }
        }
        // 3,000 orders over the first 900 customers: chains of length ~3, plus
        // NULL keys (join nothing) and dangling keys (no such customer).
        for i in 1..=3_000 {
            let cust = match i % 11 {
                0 => "NULL".into(),
                _ => format!("{}", (i % 903) + 1), // 901..903 dangle
            };
            let note = match i % 4 {
                0 => "NULL".into(),
                _ => format!("'n{i}'"),
            };
            batch.push(format!("({i}, {cust}, {}.5, {note})", i % 20));
            if batch.len() == 500 {
                db.execute(&format!("INSERT INTO ord VALUES {}", batch.join(",")))
                    .unwrap();
                batch.clear();
            }
        }
        assert!(batch.is_empty(), "row counts must be batch multiples");
        if disk_first {
            db.checkpoint().unwrap();
        }
        // Post-checkpoint writes in both modes, so the fixtures stay identical
        // while disk-first merges base and overlay.
        db.execute("INSERT INTO ord VALUES (3001, 3, 9.5, 'late')")
            .unwrap();
        db.execute("DELETE FROM ord WHERE id = 7").unwrap();
        db.execute("UPDATE cust SET country = 'US' WHERE id = 40")
            .unwrap();

        QUERIES
            .iter()
            .map(|q| {
                let run = |sql: &str| -> Vec<Vec<Value>> {
                    match db
                        .execute(sql)
                        .unwrap_or_else(|e| panic!("{sql}: {e}"))
                        .pop()
                    {
                        Some(QueryResult::Select { mut rows, .. }) => {
                            rows.sort_by_key(|r| format!("{r:?}"));
                            rows
                        }
                        other => panic!("{q} returned {other:?}"),
                    }
                };
                (run(q), run(&format!("{q} LIMIT 100000")))
            })
            .collect()
    }

    #[test]
    fn streamed_join_matches_the_general_path() {
        for disk_first in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            for (q, (streamed, general)) in QUERIES.iter().zip(results(dir.path(), disk_first)) {
                assert_eq!(
                    streamed, general,
                    "streamed and general disagree (disk_first={disk_first}) on: {q}"
                );
            }
        }
    }
}
