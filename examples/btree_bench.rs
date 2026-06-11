use oxidb::collection::Collection;
use serde_json::json;
use std::time::Instant;

fn main() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path();

    let n = 100_000usize;
    let depts = ["eng", "sales", "hr", "ops", "marketing"];
    let regions = ["US-EAST", "US-WEST", "EU-WEST", "EU-EAST", "APAC"];

    println!("=== B-tree vs Append-only Benchmark ({n} docs) ===\n");

    // ── Append-only ──
    println!("--- Append-only storage ---");
    let mut col = Collection::open("bench_append", data_dir).unwrap();

    let t0 = Instant::now();
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
        }));
    }
    col.insert_many(docs).unwrap();
    println!("  Insert {n}: {:?}", t0.elapsed());

    let t0 = Instant::now();
    col.create_index("salary").unwrap();
    col.create_index("region").unwrap();
    println!("  Create 2 indexes: {:?}", t0.elapsed());

    let t0 = Instant::now();
    for _ in 0..1000 {
        col.find_one(&json!({"region": "US-EAST"})).unwrap();
    }
    println!(
        "  1000x find_one: {:?} ({:.1}µs/op)",
        t0.elapsed(),
        t0.elapsed().as_micros() as f64 / 1000.0
    );

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
    println!(
        "  100x sort+limit(10): {:?} ({:.1}µs/op)",
        t0.elapsed(),
        t0.elapsed().as_micros() as f64 / 100.0
    );

    let t0 = Instant::now();
    for _ in 0..1000 {
        col.count_matching(&json!({"region": "APAC"})).unwrap();
    }
    println!(
        "  1000x count: {:?} ({:.1}µs/op)",
        t0.elapsed(),
        t0.elapsed().as_micros() as f64 / 1000.0
    );

    drop(col);

    // ── B-tree ──
    println!("\n--- B-tree storage ---");
    let mut col_bt =
        oxidb::btree_collection::BTreeCollection::open("bench_btree", data_dir, None).unwrap();

    let t0 = Instant::now();
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
        }));
    }
    col_bt.insert_many(docs).unwrap();
    println!("  Insert {n}: {:?}", t0.elapsed());

    let t0 = Instant::now();
    col_bt.create_index("salary").unwrap();
    col_bt.create_index("region").unwrap();
    println!("  Create 2 indexes: {:?}", t0.elapsed());

    let t0 = Instant::now();
    for _ in 0..1000 {
        col_bt.find_one(&json!({"region": "US-EAST"})).unwrap();
    }
    println!(
        "  1000x find_one: {:?} ({:.1}µs/op)",
        t0.elapsed(),
        t0.elapsed().as_micros() as f64 / 1000.0
    );

    let t0 = Instant::now();
    for _ in 0..100 {
        let opts = oxidb::query::FindOptions {
            sort: Some(vec![("salary".to_string(), oxidb::query::SortOrder::Desc)]),
            skip: None,
            limit: Some(10),
        };
        col_bt
            .find_with_options(&json!({"region": "EU-WEST"}), &opts)
            .unwrap();
    }
    println!(
        "  100x sort+limit(10): {:?} ({:.1}µs/op)",
        t0.elapsed(),
        t0.elapsed().as_micros() as f64 / 100.0
    );

    let t0 = Instant::now();
    for _ in 0..1000 {
        col_bt.count_matching(&json!({"region": "APAC"})).unwrap();
    }
    println!(
        "  1000x count: {:?} ({:.1}µs/op)",
        t0.elapsed(),
        t0.elapsed().as_micros() as f64 / 1000.0
    );

    println!("\nDone.");
}
