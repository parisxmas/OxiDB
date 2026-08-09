//! Bulk-purge benchmark for the **document** engine — the counterpart to
//! `oxidb-sql/examples/purge_bench.rs`, same shapes, same questions.
//!
//! What differs from the SQL engine going in: range predicates have always been
//! index-served here (field indexes are ordered maps, `Index::find_range`), the
//! scan path has always been a cursor walk rather than a materialize-everything,
//! and `limit` was already honoured inside the collection. What was missing was
//! a way to *pass* one from outside, which is what this measures the effect of.
//!
//! Modes:
//!   `idx-range`  an index on the purge field, `{ts: {$lte: cutoff}}`.
//!   `scan-head`  no index, same predicate — matches at the front of id order.
//!   `scan-tail`  no index, `{ts: {$gt: cutoff}}` — matches at the end, so every
//!                batch walks the survivors. The pathological case.
//!   `no-limit`   one unbounded delete, for contrast: this is what a client
//!                could do before, and it holds every match before it writes.
//!
//! Run: cargo run --release --example doc_purge_bench -- <mode> [docs] [batch]

use std::time::{Duration, Instant};

use oxidb::OxiDb;
use serde_json::json;

/// Current resident set, via `ps`. Current rather than peak on purpose: the
/// question here is whether a purge *accumulates* memory as it runs, and a
/// high-water mark cannot answer that — it only ever goes up, and the load
/// phase already set it.
fn rss_mb() -> f64 {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output();
    match out {
        Ok(o) => {
            String::from_utf8_lossy(&o.stdout)
                .trim()
                .parse::<f64>()
                .unwrap_or(0.0)
                / 1024.0
        }
        Err(_) => 0.0,
    }
}

/// macOS `Physical footprint`: the accounting that **excludes clean
/// file-backed pages**, so comparing it against `ps rss` separates anonymous
/// memory (heap — a real constraint) from mapped file pages (page cache —
/// evictable, and not a leak). This project has been caught by that distinction
/// before, which is why the purge benchmark reports both rather than one RSS
/// number that cannot tell them apart.
fn footprint_mb() -> f64 {
    let out = std::process::Command::new("vmmap")
        .args(["--summary", &std::process::id().to_string()])
        .output();
    let Ok(o) = out else { return 0.0 };
    for line in String::from_utf8_lossy(&o.stdout).lines() {
        if let Some(rest) = line.strip_prefix("Physical footprint:") {
            let v = rest.trim();
            let (num, mult) = match v.chars().last() {
                Some('K') => (&v[..v.len() - 1], 1.0 / 1024.0),
                Some('M') => (&v[..v.len() - 1], 1.0),
                Some('G') => (&v[..v.len() - 1], 1024.0),
                _ => (v, 1.0 / (1024.0 * 1024.0)),
            };
            return num.trim().parse::<f64>().unwrap_or(0.0) * mult;
        }
    }
    0.0
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

fn main() {
    let mode = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "idx-range".into());
    let docs: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(2_000_000);
    let batch: usize = std::env::args()
        .nth(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000);
    let purge = docs / 5;

    let dir = tempfile::tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();
    println!("mode={mode}  docs={docs}  batch={batch}  purge={purge}");

    // Load. Chunked inserts so the fixture is not itself the measurement.
    let t0 = Instant::now();
    let mut i = 0usize;
    while i < docs {
        let hi = (i + 10_000).min(docs);
        let chunk: Vec<_> = (i..hi)
            .map(|k| {
                json!({
                    "id": k + 1,
                    "ts": k + 1,
                    "kind": format!("k{}", k % 8),
                    "payload": "payload-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                })
            })
            .collect();
        db.insert_many("t", chunk).unwrap();
        i = hi;
    }
    println!(
        "loaded {docs} docs in {:.1}s  (RSS {:.0} MB, anonymous {:.0} MB)",
        t0.elapsed().as_secs_f64(),
        rss_mb(),
        footprint_mb()
    );

    if mode == "idx-range" {
        let t = Instant::now();
        db.create_index("t", "ts").unwrap();
        println!("create_index ts: {:.1}s", t.elapsed().as_secs_f64());
    }

    // Probes that isolate *where* the growth comes from. `probe-empty` makes 40
    // delete calls that match nothing through an index (no walk, no deletion):
    // anything that grows here is pure per-call overhead. `probe-scan` matches
    // nothing through a full scan: the difference between the two is the walk.
    if mode == "probe-empty" || mode == "probe-scan" {
        if mode == "probe-empty" {
            db.create_index("t", "ts").unwrap();
        }
        let q = if mode == "probe-empty" {
            json!({"ts": -1})
        } else {
            json!({"nosuchfield": -1})
        };
        println!(
            "before probes: RSS {:.0} MB, anon {:.0} MB",
            rss_mb(),
            footprint_mb()
        );
        for i in 1..=40 {
            let t = Instant::now();
            let n = db.delete_limited("t", &q, Some(batch)).unwrap();
            assert_eq!(n, 0, "probe query must match nothing");
            if i % 10 == 0 {
                println!(
                    "  probe {i:>3}: {:>8.1} ms  RSS {:.0} MB  anon {:.0} MB",
                    ms(t.elapsed()),
                    rss_mb(),
                    footprint_mb()
                );
            }
        }
        return;
    }

    let query = match mode.as_str() {
        "idx-range" | "scan-head" | "no-limit" => json!({"ts": {"$lte": purge}}),
        "scan-tail" => json!({"ts": {"$gt": docs - purge}}),
        other => panic!("unknown mode {other}"),
    };

    let start = Instant::now();
    let mut times: Vec<f64> = Vec::new();
    let mut deleted_total = 0u64;

    if mode == "no-limit" {
        let t = Instant::now();
        deleted_total = db.delete_limited("t", &query, None).unwrap();
        times.push(ms(t.elapsed()));
    } else {
        loop {
            let t = Instant::now();
            let n = db.delete_limited("t", &query, Some(batch)).unwrap();
            let el = t.elapsed();
            if n == 0 {
                break;
            }
            deleted_total += n;
            times.push(ms(el));
            let i = times.len();
            if i <= 3 || i % 10 == 0 {
                println!(
                    "  batch {i:>4}: {:>8.1} ms  deleted {n}  RSS {:.0} MB  anon {:.0} MB",
                    ms(el),
                    rss_mb(),
                    footprint_mb()
                );
            }
        }
    }
    let total = start.elapsed();

    println!("\n─── {mode} ───────────────────────────────");
    println!(
        "deleted            {deleted_total} docs in {:.1}s",
        total.as_secs_f64()
    );
    if deleted_total > 0 {
        println!(
            "throughput         {:.0} docs/s",
            deleted_total as f64 / total.as_secs_f64()
        );
    }
    if !times.is_empty() {
        let mut sorted = times.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p = |q: f64| sorted[((sorted.len() as f64 * q) as usize).min(sorted.len() - 1)];
        println!("batches            {}", times.len());
        println!(
            "batch ms           p50 {:.1}  p95 {:.1}  max {:.1}",
            p(0.5),
            p(0.95),
            sorted[sorted.len() - 1]
        );
        if times.len() >= 10 {
            let first: f64 = times[..5].iter().sum::<f64>() / 5.0;
            let last: f64 = times[times.len() - 5..].iter().sum::<f64>() / 5.0;
            println!("drift              first5 {first:.1} ms -> last5 {last:.1} ms");
        }
    }
    println!("remaining          {}", db.count("t", &json!({})).unwrap());
    println!(
        "RSS at end         {:.0} MB   (anonymous {:.0} MB)",
        rss_mb(),
        footprint_mb()
    );
    // HOLD=1 keeps the process alive so `heap`/`vmmap` can be pointed at it —
    // the only way to get the allocator to name what is holding the memory.
    if std::env::var("HOLD").is_ok() {
        println!("pid {} holding for inspection", std::process::id());
        std::thread::sleep(Duration::from_secs(120));
    }
}
