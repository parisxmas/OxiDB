//! Micro-benchmark: where does encode_doc actually spend time?
//!
//! Breaks the encode path apart by document shape and compares the current
//! serde-based path with the alternative `jsonb::parse_owned_jsonb` (parse
//! JSON text directly into JSONB) — useful because the serde encoder
//! allocates a fresh Serializer per value and concatenates intermediate
//! OwnedJsonb buffers, which our top-level profile showed is the dominant
//! cost.
//!
//! Run:  cargo run --release --example profile_encode

use std::hint::black_box;
use std::time::Instant;

use jsonb::{OwnedJsonb, parse_owned_jsonb};
use oxidb::codec;
use serde_json::{Value, json};

const WARMUP_ITERS: usize = 2;
const BENCH_ITERS: usize = 5;

fn gen_flat_scalars() -> Value {
    json!({
        "_id": 42,
        "active": true,
        "score": 87.5,
        "name": "Alice",
        "age": 30,
    })
}

fn gen_nested_objects() -> Value {
    json!({
        "_id": 12345,
        "user": "barisakin",
        "address": {
            "city": "Istanbul",
            "country": "TR",
            "geo": { "lat": 41.0, "lon": 28.97 },
        },
        "stats": {
            "logins": 1024,
            "uptime_pct": 99.97,
            "last_seen_ms": 1715000000000_i64,
        },
    })
}

fn gen_array_strings() -> Value {
    let arr: Vec<String> = (0..32).map(|i| format!("tag-{:02}", i)).collect();
    json!({
        "_id": 99,
        "tags": arr,
    })
}

fn gen_array_objects(n: usize) -> Value {
    let mut events = Vec::new();
    for i in 0..n {
        events.push(json!({
            "type": "click",
            "ts": 1715000000000_i64 + i as i64 * 1000,
            "url": format!("/page/{}", i),
            "duration_ms": (i as f64) * 1.7,
        }));
    }
    json!({ "_id": 1, "events": events })
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
                "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit",
            })
        })
        .collect();
    json!({
        "_id": 999_999,
        "user": "barisakin",
        "created_at": "2026-05-12T10:30:00Z",
        "title": "A longer document",
        "body": "Lorem ipsum dolor sit amet, consectetur adipiscing elit. ".repeat(8),
        "score": 1234.5678,
        "active": true,
        "tags": tags,
        "metadata": {
            "version": 17,
            "lang": "en-US",
            "geo": { "lat": 41.0082, "lon": 28.9784 },
        },
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
    let median = samples[samples.len() / 2];
    Sample {
        label,
        ns_per_op: median,
        bytes_out,
    }
}

fn run_shape(label: &str, val: &Value, iters: usize) {
    let json_text = serde_json::to_vec(val).unwrap();
    let jsonb_bytes = codec::encode_doc(val).unwrap();
    println!(
        "\n=== {}  (json_text={} B  jsonb={} B) ===",
        label,
        json_text.len(),
        jsonb_bytes.len()
    );

    let mut results: Vec<Sample> = Vec::new();

    // A) CURRENT: serde_json::Value → jsonb::to_owned_jsonb (our codec::encode_doc)
    results.push(bench(
        "CURRENT: codec::encode_doc (serde Value→JSONB)",
        iters,
        || {
            let v = codec::encode_doc(black_box(val)).unwrap();
            v.len()
        },
    ));

    // B) ALT: parse_owned_jsonb from JSON text — skips serde Serialize, builds JSONB
    //    from the raw JSON parser. Requires that we have JSON text (we'd have to
    //    serialize the Value first), so we measure two sub-flows:

    // B1) JSON text → jsonb::parse_owned_jsonb (assuming we already have text)
    results.push(bench(
        "ALT-B1: parse_owned_jsonb(json_text) — already have text",
        iters,
        || {
            let v: OwnedJsonb = parse_owned_jsonb(black_box(&json_text)).unwrap();
            v.len()
        },
    ));

    // B2) Combined: Value → JSON text → JSONB
    results.push(bench(
        "ALT-B2: Value→to_vec→parse_owned_jsonb (combined)",
        iters,
        || {
            let text = serde_json::to_vec(black_box(val)).unwrap();
            let v: OwnedJsonb = parse_owned_jsonb(&text).unwrap();
            v.len()
        },
    ));

    // C) Reference: just JSON text encode (no JSONB at all)
    results.push(bench(
        "REF: serde_json::to_vec (JSON text only)",
        iters,
        || {
            let v = serde_json::to_vec(black_box(val)).unwrap();
            v.len()
        },
    ));

    // D) The "to_vec()" step at the end of encode_doc
    results.push(bench(
        "REF: jsonb::to_owned_jsonb only (no .to_vec)",
        iters,
        || {
            let v: OwnedJsonb = jsonb::to_owned_jsonb(black_box(val)).unwrap();
            v.len()
        },
    ));

    // E) Roundtrip: parse_owned_jsonb of an existing JSONB binary (sanity / lower bound)
    //    Not what we'd actually do, but bounds parser cost.
    let pretty_json = serde_json::to_string(val).unwrap();
    let pretty_bytes = pretty_json.as_bytes().to_vec();
    results.push(bench(
        "REF: parse_owned_jsonb(pretty_json_string)",
        iters,
        || {
            let v: OwnedJsonb = parse_owned_jsonb(black_box(&pretty_bytes)).unwrap();
            v.len()
        },
    ));

    let baseline = results[0].ns_per_op;
    println!(
        "{:<55} {:>11} {:>9} {:>9}",
        "step", "ns/op (med)", "bytes", "vs CURR"
    );
    println!("{}", "-".repeat(90));
    for r in &results {
        println!(
            "{:<55} {:>11.0} {:>9} {:>9.2}x",
            r.label,
            r.ns_per_op,
            r.bytes_out,
            r.ns_per_op / baseline
        );
    }
}

fn main() {
    println!("oxidb encode profile — break down by document shape\n");

    run_shape("flat scalars (5 fields)", &gen_flat_scalars(), 200_000);
    run_shape("nested objects (2-level)", &gen_nested_objects(), 100_000);
    run_shape("array of 32 short strings", &gen_array_strings(), 100_000);
    run_shape("array of 10 small objects", &gen_array_objects(10), 50_000);
    run_shape("array of 50 small objects", &gen_array_objects(50), 20_000);
    run_shape("LARGE realistic doc", &gen_large(), 10_000);

    println!("\nNote: median of {} runs.", BENCH_ITERS);
    println!("Note: CURRENT path = serde Value → jsonb::to_owned_jsonb → .to_vec()");
    println!("      ALT-B2 path  = serde Value → serde_json::to_vec → jsonb::parse_owned_jsonb");
}
