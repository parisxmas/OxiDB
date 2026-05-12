//! Decode profile — specifically the `find → wire` path.
//!
//! When a client does a query, the server reads JSONB bytes off disk
//! and ultimately has to put JSON text on the wire. The cheapest path
//! depends on whether we materialize a `serde_json::Value` along the
//! way or skip straight from JSONB to text.
//!
//! Run:  cargo run --release --example profile_decode_wire

use std::hint::black_box;
use std::time::Instant;

use jsonb::RawJsonb;
use oxidb::codec;
use serde_json::{Value, json};

const WARMUP_ITERS: usize = 2;
const BENCH_ITERS: usize = 5;

fn gen_small() -> Value {
    json!({"_id": 42, "name": "Alice", "age": 30, "active": true})
}
fn gen_medium() -> Value {
    json!({
        "_id": 12345,
        "user": "barisakin",
        "created_at": "2026-05-12T10:30:00Z",
        "score": 87.5,
        "tags": ["rust", "database", "embedded", "performance"],
        "address": {"city": "Istanbul", "country": "TR"},
        "stats": {"logins": 1024, "uptime_pct": 99.97},
    })
}
fn gen_large() -> Value {
    let tags: Vec<String> = (0..32).map(|i| format!("tag-{}", i)).collect();
    let events: Vec<Value> = (0..50)
        .map(|i| {
            json!({
                "type": "click",
                "ts": 1715000000000_i64 + i as i64 * 1000,
                "url": format!("/page/{}", i),
                "duration_ms": (i as f64) * 1.7,
                "user_agent": "Mozilla/5.0",
            })
        })
        .collect();
    json!({
        "_id": 999_999,
        "user": "barisakin",
        "title": "A longer document",
        "body": "Lorem ipsum dolor sit amet, ".repeat(8),
        "tags": tags,
        "events": events,
    })
}

struct Sample {
    label: &'static str,
    ns_per_op: f64,
    bytes_out: usize,
}

fn bench<F: FnMut() -> usize>(label: &'static str, iters: usize, mut f: F) -> Sample {
    for _ in 0..WARMUP_ITERS {
        for _ in 0..iters / WARMUP_ITERS.max(1) {
            black_box(f());
        }
    }
    let mut samples = Vec::with_capacity(BENCH_ITERS);
    let mut bytes_out = 0;
    for _ in 0..BENCH_ITERS {
        let t0 = Instant::now();
        for _ in 0..iters {
            bytes_out = f();
        }
        samples.push(t0.elapsed().as_nanos() as f64 / iters as f64);
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    Sample {
        label,
        ns_per_op: samples[samples.len() / 2],
        bytes_out,
    }
}

fn run_shape(label: &str, val: &Value, iters: usize) {
    let jsonb_bytes = codec::encode_doc(val).unwrap();
    println!("\n=== {}  (jsonb={} B) ===", label, jsonb_bytes.len());

    let mut r: Vec<Sample> = Vec::new();

    // CURRENT find→wire path: JSONB → Value (decode_doc) → JSON text (to_vec)
    r.push(bench("CURRENT: JSONB→Value→JSON text", iters, || {
        let v = codec::decode_doc(black_box(&jsonb_bytes)).unwrap();
        let text = serde_json::to_vec(&v).unwrap();
        text.len()
    }));

    // ALT-A: JSONB → JSON text directly via RawJsonb::to_string
    r.push(bench("ALT-A: RawJsonb::to_string (skip Value)", iters, || {
        let raw = RawJsonb::new(black_box(&jsonb_bytes));
        let text = raw.to_string();
        text.len()
    }));

    // ALT-B: just full-decode to Value (no re-serialize) — for reference
    r.push(bench("REF: JSONB→Value only (no re-serialize)", iters, || {
        let v = codec::decode_doc(black_box(&jsonb_bytes)).unwrap();
        let _ = v;
        jsonb_bytes.len()
    }));

    // ALT-C: re-serialize Value→JSON text only (cost of just the back half)
    let val_owned = codec::decode_doc(&jsonb_bytes).unwrap();
    r.push(bench("REF: Value→JSON text only", iters, || {
        let text = serde_json::to_vec(black_box(&val_owned)).unwrap();
        text.len()
    }));

    // ALT-D: pre-decoded Value sitting in memory (cached doc) → JSON text
    //   This models the in-memory document cache path that doesn't pay decode.
    r.push(bench("REF: cached Value → JSON text (no decode)", iters, || {
        let text = serde_json::to_vec(black_box(&val_owned)).unwrap();
        text.len()
    }));

    let baseline = r[0].ns_per_op;
    println!("{:<48} {:>11} {:>9} {:>9}",
             "step", "ns/op (med)", "bytes", "vs CURR");
    println!("{}", "-".repeat(85));
    for s in &r {
        println!("{:<48} {:>11.0} {:>9} {:>9.2}x",
                 s.label, s.ns_per_op, s.bytes_out, s.ns_per_op / baseline);
    }
}

fn main() {
    println!("decode → wire path profile\n");
    run_shape("small (4 fields)", &gen_small(), 200_000);
    run_shape("medium nested", &gen_medium(), 100_000);
    run_shape("LARGE realistic", &gen_large(), 20_000);
}
