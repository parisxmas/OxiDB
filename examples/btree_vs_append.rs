use serde_json::json;
use std::sync::Arc;
use std::time::Instant;

fn main() {
    let dir = tempfile::tempdir().unwrap();
    let n = 100_000usize;
    let depts = ["eng", "sales", "hr", "ops", "marketing"];
    let regions = ["US-EAST", "US-WEST", "EU-WEST", "EU-EAST", "APAC"];

    println!("=== BTreeCollection vs Collection — {n} docs ===\n");

    let mut all_results: Vec<(&str, &str, std::time::Duration)> = Vec::new();

    for (label, use_btree) in [("Append-only", false), ("BTreeCollection", true)] {
        println!("--- {label} ---");
        let sub = dir.path().join(label);
        std::fs::create_dir_all(&sub).unwrap();

        let mut docs = Vec::with_capacity(n);
        for i in 0..n {
            let dept = depts[i % 5];
            let region = regions[i % 5];
            let salary = (30000 + (i * 7) % 170000) as u64;
            docs.push(json!({
                "emp_id": format!("E{:06}", i),
                "department": dept,
                "region": region,
                "salary": salary,
                "name": format!("Employee {}", i),
                "rating": 1.0 + (i % 50) as f64 / 10.0,
            }));
        }

        if use_btree {
            let mut col =
                oxidb::btree_collection::BTreeCollection::open("bench", &sub, None).unwrap();

            let t0 = Instant::now();
            col.insert_many(docs).unwrap();
            let d = t0.elapsed();
            println!("  Insert {n}: {:?}", d);
            all_results.push((label, "Insert", d));

            let t0 = Instant::now();
            col.create_index("salary").unwrap();
            col.create_index("region").unwrap();
            let d = t0.elapsed();
            println!("  Create 2 indexes: {:?}", d);
            all_results.push((label, "Create indexes", d));

            let t0 = Instant::now();
            for _ in 0..1000 {
                col.find_one(&json!({"region": "US-EAST"})).unwrap();
            }
            let d = t0.elapsed();
            println!(
                "  1000x find_one: {:?} ({:.1}µs/op)",
                d,
                d.as_micros() as f64 / 1000.0
            );
            all_results.push((label, "find_one", d));

            let t0 = Instant::now();
            for _ in 0..100 {
                let opts = oxidb::query::FindOptions {
                    sort: Some(vec![("salary".to_string(), oxidb::query::SortOrder::Desc)]),
                    skip: None,
                    limit: Some(10),
                };
                col.find_with_options(&json!({"region": "EU-WEST"}), &opts)
                    .unwrap();
            }
            let d = t0.elapsed();
            println!(
                "  100x sort+limit(10): {:?} ({:.1}µs/op)",
                d,
                d.as_micros() as f64 / 100.0
            );
            all_results.push((label, "sort+limit", d));

            let t0 = Instant::now();
            for _ in 0..1000 {
                col.count_matching(&json!({"region": "APAC"})).unwrap();
            }
            let d = t0.elapsed();
            println!(
                "  1000x count: {:?} ({:.1}µs/op)",
                d,
                d.as_micros() as f64 / 1000.0
            );
            all_results.push((label, "count", d));

            // Unindexed scan
            let t0 = Instant::now();
            col.find(&json!({"rating": {"$gte": 4.0}})).unwrap();
            let d = t0.elapsed();
            println!("  Unindexed scan: {:?}", d);
            all_results.push((label, "unindexed scan", d));
        } else {
            let mut col = oxidb::collection::Collection::open("bench", &sub).unwrap();

            let t0 = Instant::now();
            col.insert_many(docs).unwrap();
            let d = t0.elapsed();
            println!("  Insert {n}: {:?}", d);
            all_results.push((label, "Insert", d));

            let t0 = Instant::now();
            col.create_index("salary").unwrap();
            col.create_index("region").unwrap();
            let d = t0.elapsed();
            println!("  Create 2 indexes: {:?}", d);
            all_results.push((label, "Create indexes", d));

            let t0 = Instant::now();
            for _ in 0..1000 {
                col.find_one(&json!({"region": "US-EAST"})).unwrap();
            }
            let d = t0.elapsed();
            println!(
                "  1000x find_one: {:?} ({:.1}µs/op)",
                d,
                d.as_micros() as f64 / 1000.0
            );
            all_results.push((label, "find_one", d));

            let t0 = Instant::now();
            for _ in 0..100 {
                let opts = oxidb::query::FindOptions {
                    sort: Some(vec![("salary".to_string(), oxidb::query::SortOrder::Desc)]),
                    skip: None,
                    limit: Some(10),
                };
                col.find_with_options(&json!({"region": "EU-WEST"}), &opts)
                    .unwrap();
            }
            let d = t0.elapsed();
            println!(
                "  100x sort+limit(10): {:?} ({:.1}µs/op)",
                d,
                d.as_micros() as f64 / 100.0
            );
            all_results.push((label, "sort+limit", d));

            let t0 = Instant::now();
            for _ in 0..1000 {
                col.count_matching(&json!({"region": "APAC"})).unwrap();
            }
            let d = t0.elapsed();
            println!(
                "  1000x count: {:?} ({:.1}µs/op)",
                d,
                d.as_micros() as f64 / 1000.0
            );
            all_results.push((label, "count", d));

            // Unindexed scan
            let t0 = Instant::now();
            col.find(&json!({"rating": {"$gte": 4.0}})).unwrap();
            let d = t0.elapsed();
            println!("  Unindexed scan: {:?}", d);
            all_results.push((label, "unindexed scan", d));
        }
        println!();
    }

    // Summary
    println!("═══════════════════════════════════════════════════════════");
    println!(
        "{:<20} {:>15} {:>15} {:>10}",
        "Operation", "Append-only", "BTree", "Winner"
    );
    println!("───────────────────────────────────────────────────────────");
    let ops = [
        "Insert",
        "Create indexes",
        "find_one",
        "sort+limit",
        "count",
        "unindexed scan",
    ];
    for op in ops {
        let ao = all_results
            .iter()
            .find(|r| r.0 == "Append-only" && r.1 == op)
            .map(|r| r.2);
        let bt = all_results
            .iter()
            .find(|r| r.0 == "BTreeCollection" && r.1 == op)
            .map(|r| r.2);
        if let (Some(a), Some(b)) = (ao, bt) {
            let winner = if a < b { "Append" } else { "BTree" };
            let ratio = if a < b {
                b.as_micros() as f64 / a.as_micros() as f64
            } else {
                a.as_micros() as f64 / b.as_micros() as f64
            };
            println!(
                "{:<20} {:>15?} {:>15?} {:>6} {:.1}x",
                op, a, b, winner, ratio
            );
        }
    }
    println!("═══════════════════════════════════════════════════════════");
}
