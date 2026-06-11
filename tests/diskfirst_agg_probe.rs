//! Investigation probe: why is disk-first aggregation slower?
//!
//! Localizes the cost by contrasting two group-by aggregations over the same
//! data:
//!   A) group by an *indexed* field, COUNT only  -> index-only fast path,
//!      **zero document reads** (see `try_index_only_count`).
//!   B) group by the same field, AVG(salary)     -> must read **every** doc
//!      (`aggregate_streaming` -> `for_each_value`).
//!
//! Run both modes and compare:
//!   cargo test --test diskfirst_agg_probe -- --ignored --nocapture
//!   OXIDB_DISK_FIRST=1 cargo test --test diskfirst_agg_probe -- --ignored --nocapture
//!
//! If (A) is fast in both modes but (B) is slow only in disk-first, the cost is
//! the per-document read+decode in the full scan, not the grouping itself.

use oxidb::OxiDb;
use serde_json::json;
use std::time::Instant;

fn disk_first() -> bool {
    std::env::var("OXIDB_DISK_FIRST")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

#[test]
#[ignore = "manual investigation probe; run explicitly with --nocapture"]
fn agg_cost_breakdown() {
    let n: u64 = std::env::var("PROBE_N")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200_000);

    let depts = [
        "eng", "sales", "ops", "hr", "finance", "legal", "support", "mktg",
    ];
    let cities = ["NYC", "LON", "SF", "BER", "TOK", "PAR"];

    let dir = tempfile::tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();
    db.create_index("c", "department").unwrap();

    // Insert in batches of 5000 (one commit/fsync per batch), exactly like the
    // real bench harness — a per-doc insert() loop would pay 200k separate
    // fsyncs (strict ACID-D) and dwarf everything we're trying to measure.
    let t = Instant::now();
    let batch = 5000u64;
    let mut i = 0u64;
    while i < n {
        let end = (i + batch).min(n);
        let docs: Vec<_> = (i..end)
            .map(|j| {
                let d = (j as usize) % depts.len();
                let city = (j as usize * 7) % cities.len();
                json!({
                    "emp_id": format!("E{j:07}"),
                    "name": format!("Employee Number {j}"),
                    "department": depts[d],
                    "city": cities[city],
                    "salary": 30000 + (j % 170000),
                    "age": 22 + (j % 43),
                    "rating": 1.0 + ((j % 40) as f64) / 10.0,
                })
            })
            .collect();
        db.insert_many("c", docs).unwrap();
        i = end;
    }
    let insert = t.elapsed();

    // A) index-only: group by indexed field, count only -> no doc reads.
    let count_pipe = json!([{ "$group": { "_id": "$department", "n": { "$sum": 1 } } }]);
    // warm + measure
    let _ = db.aggregate("c", &count_pipe).unwrap();
    let t = Instant::now();
    let a = db.aggregate("c", &count_pipe).unwrap();
    let count_only = t.elapsed();

    // B) full scan: group by same field, avg(salary) -> reads every doc.
    let avg_pipe = json!([{ "$group": { "_id": "$department", "avg": { "$avg": "$salary" }, "n": { "$sum": 1 } } }]);
    let _ = db.aggregate("c", &avg_pipe).unwrap();
    let t = Instant::now();
    let b = db.aggregate("c", &avg_pipe).unwrap();
    let avg_scan = t.elapsed();

    // C) for reference: an unindexed full-scan count (also reads every doc) so we
    //    can see the same per-doc read cost outside aggregation.
    let t = Instant::now();
    let cnt = db
        .count("c", &json!({ "rating": { "$gte": 0.0 } }))
        .unwrap();
    let unindexed_scan = t.elapsed();

    println!("\n=== disk_first={} | n={} ===", disk_first(), n);
    println!("insert                 : {:>9.2?}", insert);
    println!(
        "A) group count-only    : {:>9.2?}  ({} groups) [index-only, 0 doc reads]",
        count_only,
        a.len()
    );
    println!(
        "B) group avg(salary)   : {:>9.2?}  ({} groups) [reads every doc]",
        avg_scan,
        b.len()
    );
    println!(
        "C) unindexed scan count: {:>9.2?}  ({} matched) [reads every doc]",
        unindexed_scan, cnt
    );
    println!(
        "ratio B/A              : {:>6.1}x   (how much the per-doc read+decode adds)",
        avg_scan.as_secs_f64() / count_only.as_secs_f64().max(1e-9)
    );

    assert_eq!(a.len(), depts.len());
    assert_eq!(b.len(), depts.len());
    assert_eq!(cnt as u64, n);
}
