//! Write-path benchmark for oxidb-sql, mirroring the PostgreSQL comparison
//! (`insert_bench_postgres.sh`): INSERT throughput into a table carrying a
//! PRIMARY KEY plus four secondary indexes (three single-column, one
//! composite), with a bare (index-free) table as contrast.
//!
//! Phases (identical SQL text and row values on both engines):
//! 1. bulk — `BATCHES` multi-row INSERTs of `BATCH_ROWS` rows each into the
//!    indexed table (one WAL fsync per statement on both engines).
//! 2. single — `SINGLES` autocommit single-row INSERTs (one fsync each).
//! 3. bare — the same bulk load into a table with no PK and no indexes
//!    (isolates index-maintenance cost).
//! 4. parity — aggregate + indexed-lookup results, for cross-engine
//!    comparison.
//!
//! Run: `cargo run --release --example insert_bench -p oxidb-sql`
//! Scale with `SCALE=5` (multiplies the bulk batch count).

use std::fmt::Write as _;
use std::time::Instant;

use oxidb_sql::{QueryResult, SqlEngine, Value};

const BATCH_ROWS: usize = 1_000;
const BASE_BATCHES: usize = 100;
const SINGLES: usize = 2_000;

fn scale() -> usize {
    std::env::var("SCALE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1)
}

/// One row's values, identical to the generator in the Postgres script:
/// id = i+1, user_id = i%1000+1, kind = 'k'||(i%8), ts = 1700000000000+i*1000,
/// amount = (i*7)%10000.
fn row_tuple(buf: &mut String, i: usize) {
    write!(
        buf,
        "({}, {}, 'k{}', {}, {})",
        i + 1,
        (i % 1000) + 1,
        i % 8,
        1_700_000_000_000u64 + (i as u64) * 1000,
        (i * 7) % 10_000
    )
    .unwrap();
}

fn batch_sql(table: &str, first: usize, rows: usize) -> String {
    let mut sql = format!("INSERT INTO {table} VALUES ");
    for j in 0..rows {
        if j > 0 {
            sql.push(',');
        }
        row_tuple(&mut sql, first + j);
    }
    sql
}

fn main() {
    let k = scale();
    let batches = BASE_BATCHES * k;
    let bulk_rows = batches * BATCH_ROWS;

    let dir = tempfile::tempdir().unwrap();
    let db = SqlEngine::open(dir.path()).unwrap();

    db.execute(
        "CREATE TABLE events (id BIGINT PRIMARY KEY, user_id INT NOT NULL, \
         kind TEXT NOT NULL, ts BIGINT NOT NULL, amount INT NOT NULL)",
    )
    .unwrap();
    db.execute("CREATE INDEX ev_user ON events (user_id)")
        .unwrap();
    db.execute("CREATE INDEX ev_kind ON events (kind)").unwrap();
    db.execute("CREATE INDEX ev_amount ON events (amount)")
        .unwrap();
    db.execute("CREATE INDEX ev_user_kind ON events (user_id, kind)")
        .unwrap();
    db.execute(
        "CREATE TABLE events_bare (id BIGINT, user_id INT, kind TEXT, ts BIGINT, amount INT)",
    )
    .unwrap();

    println!(
        "oxidb-sql insert benchmark — PK + 4 secondary indexes, \
         {batches} batches x {BATCH_ROWS} rows + {SINGLES} single inserts\n"
    );

    // 1. Bulk into the indexed table.
    let t = Instant::now();
    for b in 0..batches {
        db.execute(&batch_sql("events", b * BATCH_ROWS, BATCH_ROWS))
            .unwrap();
    }
    let secs = t.elapsed().as_secs_f64();
    println!(
        "bulk indexed    {bulk_rows:>8} rows  {:>8.2} ms  {:>9.0} rows/s",
        secs * 1000.0,
        bulk_rows as f64 / secs
    );

    // 2. Autocommit single-row inserts (continue the id sequence).
    let t = Instant::now();
    for j in 0..SINGLES {
        db.execute(&batch_sql("events", bulk_rows + j, 1)).unwrap();
    }
    let secs = t.elapsed().as_secs_f64();
    println!(
        "single indexed  {SINGLES:>8} rows  {:>8.2} ms  {:>9.0} rows/s  ({:.3} ms/insert)",
        secs * 1000.0,
        SINGLES as f64 / secs,
        secs * 1000.0 / SINGLES as f64
    );

    // 3. Bulk into the bare table (no PK, no indexes).
    let t = Instant::now();
    for b in 0..batches {
        db.execute(&batch_sql("events_bare", b * BATCH_ROWS, BATCH_ROWS))
            .unwrap();
    }
    let secs = t.elapsed().as_secs_f64();
    println!(
        "bulk bare       {bulk_rows:>8} rows  {:>8.2} ms  {:>9.0} rows/s",
        secs * 1000.0,
        bulk_rows as f64 / secs
    );

    // 4. Parity checks (identical values expected on both engines).
    for sql in [
        "SELECT COUNT(*) AS n, SUM(amount) AS total FROM events",
        "SELECT COUNT(*) AS n FROM events WHERE user_id = 42",
        "SELECT COUNT(*) AS n FROM events WHERE user_id = 42 AND kind = 'k1'",
        "SELECT COUNT(*) AS n FROM events WHERE amount = 7777",
    ] {
        let mut res = db.execute(sql).unwrap();
        if let Some(QueryResult::Select { rows, .. }) = res.pop() {
            let cells: Vec<String> = rows[0]
                .iter()
                .map(|v| match v {
                    Value::Int(n) => n.to_string(),
                    other => format!("{other:?}"),
                })
                .collect();
            println!("parity  {}  -> {}", sql, cells.join("|"));
        }
    }
}
