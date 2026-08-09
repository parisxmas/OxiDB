//! Bulk-purge benchmark: what a batched `DELETE ... LIMIT n` actually costs.
//!
//! The question this answers is the operational one — "I have a huge table and
//! must delete a fifth of it while writes continue; what batch size, and how
//! does it degrade?" — with measurements rather than reasoning. Streaming DML
//! and the range-index path make the purge *possible*; only running it says
//! what it costs.
//!
//! Four shapes, chosen because they are the ones that differ:
//!
//!   `idx-range`   an index on the purge column, `WHERE ts < cutoff`.
//!   `scan-head`   no index, same predicate. Matches sit at the FRONT of
//!                 storage order (purging the oldest rows of an append-only
//!                 table), so the streamed early exit finds its batch
//!                 immediately.
//!   `scan-tail`   no index, `WHERE ts > cutoff` — matches at the END. Every
//!                 batch must walk every surviving row to reach them. This is
//!                 the pathological case and it is here to be reported, not
//!                 hidden.
//!   `no-limit`    one unbounded `DELETE` for the whole purge, for contrast.
//!
//! Reported per batch: wall time and `dml_rows_examined()` (rows walked to find
//! the matches — the difference between "an index served this" and "this read
//! the table"). Reported overall: total time, throughput, and peak RSS.
//!
//! Run:
//!   cargo run --release --example purge_bench -p oxidb-sql -- <mode> [rows] [batch]
//! Env:
//!   OXIDB_SQL_CHECKPOINT_BYTES — the knob this benchmark exists to inform.

use std::time::{Duration, Instant};

use oxidb_sql::{QueryResult, SqlEngine, Value};

/// Peak resident set for this process. Peak, not current: `getrusage` reports a
/// high-water mark, which is the number that matters for "will this fit".
fn peak_rss_mb() -> f64 {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    let raw = ru.ru_maxrss as f64;
    if cfg!(target_os = "macos") {
        raw / (1 << 20) as f64
    } else {
        raw / 1024.0
    }
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

fn count(db: &SqlEngine) -> i64 {
    match db.execute("SELECT count(*) FROM t").unwrap().pop().unwrap() {
        QueryResult::Select { rows, .. } => match rows[0][0] {
            Value::Int(n) => n,
            ref v => panic!("count: {v:?}"),
        },
        other => panic!("count: {other:?}"),
    }
}

fn affected(db: &SqlEngine, sql: &str) -> usize {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Mutation { affected, .. } => affected,
        other => panic!("expected Mutation: {other:?}"),
    }
}

/// Load `rows` rows: id 1..=rows, ts ascending with id (append-only shape),
/// plus two columns nothing in the purge predicate reads — so the masked decode
/// has something to skip, as it would in a real table.
fn load(db: &SqlEngine, rows: usize) -> Duration {
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, ts INT, kind TEXT, payload TEXT)")
        .unwrap();
    let t0 = Instant::now();
    let mut i = 0usize;
    let mut buf = String::new();
    while i < rows {
        let hi = (i + 1_000).min(rows);
        buf.clear();
        buf.push_str("INSERT INTO t (id, ts, kind, payload) VALUES ");
        for k in i..hi {
            if k > i {
                buf.push(',');
            }
            buf.push_str(&format!(
                "({},{},'k{}','payload-{}-xxxxxxxxxxxxxxxxxxxx')",
                k + 1,
                k + 1,
                k % 8,
                k
            ));
        }
        db.execute(&buf).unwrap();
        i = hi;
    }
    t0.elapsed()
}

fn main() {
    let mode = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "idx-range".into());
    let rows: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(2_000_000);
    let batch: usize = std::env::args()
        .nth(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000);
    let purge = rows / 5; // 20%

    let dir = tempfile::tempdir().unwrap();
    let db = SqlEngine::open(dir.path()).unwrap();

    println!("mode={mode}  rows={rows}  batch={batch}  purge={purge}");
    let load_t = load(&db, rows);
    println!(
        "loaded {rows} rows in {:.1}s  (peak RSS {:.0} MB)",
        load_t.as_secs_f64(),
        peak_rss_mb()
    );

    // The index exists only where the mode is about having one.
    if mode == "idx-range" {
        let t = Instant::now();
        db.execute("CREATE INDEX ix_ts ON t (ts)").unwrap();
        println!("CREATE INDEX ix_ts: {:.1}s", t.elapsed().as_secs_f64());
    }
    // Fold the load into the on-disk base first: otherwise the purge is
    // measured against rows that all still sit in the post-checkpoint overlay,
    // which is not the state a real table is in.
    let t = Instant::now();
    db.checkpoint().unwrap();
    println!("checkpoint after load: {:.1}s", t.elapsed().as_secs_f64());
    println!("rows before purge: {}", count(&db));

    let pred = match mode.as_str() {
        // Oldest 20%: the front of storage order.
        "idx-range" | "scan-head" | "no-limit" => format!("ts <= {purge}"),
        // Newest 20%: the end of storage order.
        "scan-tail" => format!("ts > {}", rows - purge),
        other => panic!("unknown mode {other}"),
    };

    let start = Instant::now();
    let mut batches: Vec<(Duration, u64)> = Vec::new();

    if mode == "no-limit" {
        let t = Instant::now();
        let n = affected(&db, &format!("DELETE FROM t WHERE {pred}"));
        batches.push((t.elapsed(), db.dml_rows_examined()));
        println!("single unbounded DELETE removed {n}");
    } else {
        loop {
            let t = Instant::now();
            let n = affected(&db, &format!("DELETE FROM t WHERE {pred} LIMIT {batch}"));
            let el = t.elapsed();
            let ex = db.dml_rows_examined();
            if n == 0 {
                break;
            }
            batches.push((el, ex));
            // First few and then every 10th, so the shape is visible without
            // thousands of lines.
            let i = batches.len();
            if i <= 3 || i % 10 == 0 {
                println!(
                    "  batch {i:>4}: {:>8.1} ms  examined {ex:>9}  deleted {n}",
                    ms(el)
                );
            }
        }
    }
    let total = start.elapsed();

    let deleted = rows as i64 - count(&db);
    println!("\n─── {mode} ───────────────────────────────");
    println!(
        "deleted            {deleted} rows in {:.1}s",
        total.as_secs_f64()
    );
    if deleted > 0 {
        println!(
            "throughput         {:.0} rows/s",
            deleted as f64 / total.as_secs_f64()
        );
    }
    if !batches.is_empty() {
        let mut times: Vec<f64> = batches.iter().map(|(d, _)| ms(*d)).collect();
        times.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p = |q: f64| times[((times.len() as f64 * q) as usize).min(times.len() - 1)];
        let ex_total: u64 = batches.iter().map(|(_, e)| *e).sum();
        println!("batches            {}", batches.len());
        println!(
            "batch ms           p50 {:.1}  p95 {:.1}  max {:.1}",
            p(0.5),
            p(0.95),
            times[times.len() - 1]
        );
        println!(
            "rows examined      {ex_total} total  ({:.1}x the rows deleted)",
            ex_total as f64 / deleted.max(1) as f64
        );
        // Drift: is a late batch more expensive than an early one? That is the
        // overlay growing between checkpoints, and the reason to tune the
        // checkpoint interval rather than just the batch size.
        if batches.len() >= 10 {
            let first: f64 = batches[..5].iter().map(|(d, _)| ms(*d)).sum::<f64>() / 5.0;
            let last: f64 = batches[batches.len() - 5..]
                .iter()
                .map(|(d, _)| ms(*d))
                .sum::<f64>()
                / 5.0;
            println!("drift              first5 {first:.1} ms -> last5 {last:.1} ms");
        }
    }
    println!("peak RSS           {:.0} MB", peak_rss_mb());
}
