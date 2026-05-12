//! End-to-end: load_doc_text vs load_doc_arc + serde_json::to_string.
//!
//! Validates that the cold-path bypass (skip Value materialization,
//! go RawJsonb::to_string directly) wins where the cache miss path
//! dominates.
//!
//! Run:  cargo run --release --example profile_load_doc_text

use std::hint::black_box;
use std::time::Instant;

use serde_json::json;
use tempfile::TempDir;

use oxidb::btree_collection::BTreeCollection;

fn gen_small(i: usize) -> serde_json::Value {
    json!({"_id": i as u64, "name": format!("user-{}", i), "score": 87.5, "active": true})
}
fn gen_medium(i: usize) -> serde_json::Value {
    json!({
        "_id": i as u64,
        "user": format!("user-{}", i),
        "score": 87.5,
        "tags": ["rust", "database", "embedded", "performance"],
        "address": {"city": "Istanbul", "country": "TR"},
        "stats": {"logins": 1024, "uptime_pct": 99.97},
    })
}
fn gen_large(i: usize) -> serde_json::Value {
    let events: Vec<_> = (0..50).map(|k| json!({
        "type": "click", "ts": 1715000000000_i64 + k as i64 * 1000,
        "url": format!("/page/{}", k), "duration_ms": (k as f64) * 1.7,
        "user_agent": "Mozilla/5.0",
    })).collect();
    json!({
        "_id": i as u64,
        "user": format!("user-{}", i),
        "title": "A longer document",
        "body": "Lorem ipsum dolor sit amet, ".repeat(8),
        "tags": (0..32).map(|k| format!("tag-{}", k)).collect::<Vec<_>>(),
        "events": events,
    })
}

fn run(label: &str, n_docs: usize, mut make: impl FnMut(usize) -> serde_json::Value, iters: usize) {
    let tmp = TempDir::new().unwrap();
    let col = BTreeCollection::open("bench", tmp.path(), None).unwrap();
    col.set_lazy_sync(true);
    col.set_cache_capacity(1_000); // make sure misses happen for the cold-path test

    let mut ids = Vec::with_capacity(n_docs);
    for i in 0..n_docs {
        ids.push(col.insert(make(i)).unwrap());
    }
    println!("\n=== {} ({} docs) ===", label, n_docs);

    // Strategy A: COLD — load_doc_arc + serde_json::to_string (current find→wire)
    col.doc_cache_clear();
    let t0 = Instant::now();
    let mut total_bytes = 0usize;
    for _ in 0..iters {
        for &id in &ids {
            let arc = col.load_doc_arc(id).unwrap();
            let text = serde_json::to_string(&*arc).unwrap();
            total_bytes = total_bytes.wrapping_add(text.len());
            black_box(text);
        }
        col.doc_cache_clear(); // force cold every pass
    }
    let cold_a = t0.elapsed().as_nanos() as f64 / (iters * n_docs) as f64;
    let _ = total_bytes;

    // Strategy B: COLD — load_doc_text (RawJsonb::to_string directly)
    col.doc_cache_clear();
    let t0 = Instant::now();
    for _ in 0..iters {
        for &id in &ids {
            let text = col.load_doc_text(id).unwrap();
            black_box(text);
        }
        col.doc_cache_clear();
    }
    let cold_b = t0.elapsed().as_nanos() as f64 / (iters * n_docs) as f64;

    // Strategy A-HOT: cache warm + load_doc_arc + serialize
    for &id in &ids { let _ = col.load_doc_arc(id); }
    let t0 = Instant::now();
    for _ in 0..iters {
        for &id in &ids {
            let arc = col.load_doc_arc(id).unwrap();
            let text = serde_json::to_string(&*arc).unwrap();
            black_box(text);
        }
    }
    let hot_a = t0.elapsed().as_nanos() as f64 / (iters * n_docs) as f64;

    // Strategy B-HOT: cache warm + load_doc_text
    let t0 = Instant::now();
    for _ in 0..iters {
        for &id in &ids {
            let text = col.load_doc_text(id).unwrap();
            black_box(text);
        }
    }
    let hot_b = t0.elapsed().as_nanos() as f64 / (iters * n_docs) as f64;

    println!("  {:<35} {:>10} {:>10} {:>10}", "strategy", "cold ns/op", "hot ns/op", "(cold ×)");
    println!("  {}", "-".repeat(70));
    println!("  {:<35} {:>10.0} {:>10.0} {:>10}", "A: load_doc_arc + to_string", cold_a, hot_a, "1.00x");
    println!("  {:<35} {:>10.0} {:>10.0} {:>10.2}x", "B: load_doc_text (new)", cold_b, hot_b, cold_a / cold_b);
}

fn main() {
    println!("end-to-end: load_doc_text cold-path bypass\n");
    run("small docs", 200, gen_small, 50);
    run("medium docs", 200, gen_medium, 30);
    run("LARGE docs", 100, gen_large, 10);
}
