//! What a range predicate on an indexed column costs, with and without the
//! index serving it.
//!
//! The A/B is **within one process and one session**: the same rows, the same
//! statements, two engines that differ only in whether `CREATE INDEX` ran.
//! Comparing numbers across benchmark sessions has been wrong here before —
//! there is ±5-8% of machine drift between runs — so the only comparison this
//! prints is one it measured back to back.
//!
//!   cargo run --release -p oxidb-sql --example range_bench [rows]

use std::time::Instant;

use oxidb_sql::{QueryResult, SqlEngine, Value};

fn seed(db: &SqlEngine, rows: i64, indexed: bool) {
    db.execute("CREATE TABLE events (id INT PRIMARY KEY, ts BIGINT, host TEXT, ms INT)")
        .unwrap();
    let mut i = 1;
    while i <= rows {
        let hi = (i + 999).min(rows);
        let vals: Vec<String> = (i..=hi)
            .map(|k| {
                format!(
                    "({k}, {}, 'host{}', {})",
                    1_700_000_000_000i64 + k * 1000,
                    k % 50,
                    k % 997
                )
            })
            .collect();
        db.execute(&format!(
            "INSERT INTO events (id, ts, host, ms) VALUES {}",
            vals.join(", ")
        ))
        .unwrap();
        i = hi + 1;
    }
    if indexed {
        db.execute("CREATE INDEX ix_ts ON events (ts)").unwrap();
    }
    db.checkpoint().unwrap();
}

/// Median of `n` runs, in microseconds, plus the row count the query returned
/// (printed so a "fast" plan that answered nothing is visible rather than
/// flattering).
fn time(db: &SqlEngine, sql: &str, n: usize) -> (f64, usize) {
    let mut samples = Vec::with_capacity(n);
    let mut produced = 0;
    for _ in 0..n {
        let t0 = Instant::now();
        let out = db.execute(sql).unwrap();
        samples.push(t0.elapsed().as_secs_f64() * 1e6);
        produced = match out.last() {
            Some(QueryResult::Select { rows, .. }) => rows.len(),
            _ => 0,
        };
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (samples[samples.len() / 2], produced)
}

fn main() {
    let rows: i64 = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(1_000_000);

    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let indexed = SqlEngine::open(a.path()).unwrap();
    let plain = SqlEngine::open(b.path()).unwrap();
    println!("seeding {rows} rows into two engines…");
    seed(&indexed, rows, true);
    seed(&plain, rows, false);

    let base = 1_700_000_000_000i64;
    // Windows as a fraction of the table, since selectivity is the whole story:
    // an index that narrows nothing should lose, and the cap should notice.
    let cases: Vec<(String, String)> = [
        ("0.1% window", (rows as f64 * 0.001) as i64),
        ("1% window", (rows as f64 * 0.01) as i64),
        ("10% window", (rows as f64 * 0.10) as i64),
        ("60% window (past the cap)", (rows as f64 * 0.60) as i64),
    ]
    .iter()
    .map(|(name, span)| {
        let lo = base + 1000;
        let hi = base + span * 1000;
        (
            name.to_string(),
            format!("SELECT count(*), sum(ms) FROM events WHERE ts >= {lo} AND ts < {hi}"),
        )
    })
    .collect();

    println!(
        "\n{:<28} {:>12} {:>12} {:>9} {:>10} {:>7}",
        "query", "scan µs", "index µs", "speedup", "rows read", "served"
    );
    for (name, sql) in &cases {
        let before = indexed.range_index_reads();
        let (t_idx, _) = time(&indexed, sql, 7);
        let served = indexed.range_index_reads() > before;
        let (t_scan, _) = time(&plain, sql, 7);
        // The statement produces one aggregate row; print how many rows the
        // window actually matched, so an empty window cannot look like a win.
        let matched = count_of(&plain, sql);
        println!(
            "{:<28} {:>12.0} {:>12.0} {:>8.2}x {:>10} {:>7}",
            name,
            t_scan,
            t_idx,
            t_scan / t_idx,
            matched,
            if served { "index" } else { "scan" }
        );
    }

    // Control: a range on an **unindexed** column. Neither engine can take the
    // path, so whatever this prints is the difference between the two engines
    // rather than the difference between the two plans — without it, the
    // declined row above cannot be read at all.
    let sql = format!(
        "SELECT count(*), sum(ms) FROM events WHERE ms >= 100 AND ms < {}",
        997
    );
    let (t_idx, _) = time(&indexed, &sql, 7);
    let (t_scan, _) = time(&plain, &sql, 7);
    println!(
        "{:<28} {:>12.0} {:>12.0} {:>8.2}x {:>10} {:>7}",
        "control: unindexed column",
        t_scan,
        t_idx,
        t_scan / t_idx,
        count_of(&plain, &sql),
        "scan"
    );

    // A row-returning shape too: the aggregate above folds as it goes, so it is
    // the best case for the scan. Returning rows is what most reads do.
    let lo = base + 1000;
    let hi = base + (rows as f64 * 0.01) as i64 * 1000;
    let sql = format!(
        "SELECT id, host, ms FROM events WHERE ts >= {lo} AND ts < {hi} ORDER BY ms DESC LIMIT 20"
    );
    let (t_idx, n_idx) = time(&indexed, &sql, 7);
    let (t_scan, n_scan) = time(&plain, &sql, 7);
    assert_eq!(n_idx, n_scan);
    println!(
        "{:<28} {:>12.0} {:>12.0} {:>8.2}x {:>10} {:>7}",
        "1% window, top-20",
        t_scan,
        t_idx,
        t_scan / t_idx,
        n_idx,
        "index"
    );
}

/// Rows the window actually matches, read off the scan engine.
fn count_of(db: &SqlEngine, sql: &str) -> i64 {
    let one = sql.replace("count(*), sum(ms)", "count(*)");
    match db.execute(&one).unwrap().pop() {
        Some(QueryResult::Select { rows, .. }) => match rows[0][0] {
            Value::Int(n) => n,
            _ => -1,
        },
        _ => -1,
    }
}
