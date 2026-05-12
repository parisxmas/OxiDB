//! Micro-benchmark: where does the decode hot path actually spend time?
//!
//! Measures each candidate step in isolation against a realistic corpus
//! (small / medium / large documents) so we can decide whether a custom
//! JSONB-native deserializer would pay for itself.
//!
//! Run:  cargo run --release --example profile_decode

use std::hint::black_box;
use std::time::Instant;

use jsonb::RawJsonb;
use oxidb::codec;
use serde_json::{Value, json};

const WARMUP_ITERS: usize = 2;
const BENCH_ITERS: usize = 5;

fn gen_small() -> Value {
    json!({
        "_id": 42,
        "name": "Alice",
        "age": 30,
        "active": true,
    })
}

fn gen_medium() -> Value {
    json!({
        "_id": 12345,
        "user": "barisakin",
        "email": "user@example.com",
        "created_at": "2026-05-12T10:30:00Z",
        "score": 87.5,
        "active": true,
        "tags": ["rust", "database", "embedded", "performance"],
        "address": {
            "city": "Istanbul",
            "country": "TR",
            "zip": "34000",
        },
        "stats": {
            "logins": 1024,
            "uptime_pct": 99.97,
            "last_seen_ms": 1715000000000_i64,
        },
    })
}

fn gen_large() -> Value {
    let mut tags = Vec::new();
    for i in 0..32 {
        tags.push(format!("tag-{}", i));
    }
    let mut events = Vec::new();
    for i in 0..50 {
        events.push(json!({
            "type": "click",
            "ts": 1715000000000_i64 + i as i64 * 1000,
            "url": format!("/page/{}", i),
            "duration_ms": (i as f64) * 1.7,
            "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit",
        }));
    }
    json!({
        "_id": 999_999,
        "user": "barisakin",
        "email": "user@example.com",
        "created_at": "2026-05-12T10:30:00Z",
        "title": "A longer document with mixed scalar, string, array, nested object fields",
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

fn build_corpus() -> Vec<(String, Value, Vec<u8>, Vec<u8>)> {
    // (label, value, jsonb_bytes, json_text_bytes)
    let docs: Vec<(&str, Value)> = vec![
        ("small", gen_small()),
        ("medium", gen_medium()),
        ("large", gen_large()),
    ];
    docs.into_iter()
        .map(|(label, v)| {
            let jsonb_bytes = codec::encode_doc(&v).expect("encode jsonb");
            let json_bytes = serde_json::to_vec(&v).expect("encode json");
            (label.to_string(), v, jsonb_bytes, json_bytes)
        })
        .collect()
}

/// Mirror of `extract_raw_index_value` in btree_collection.rs — the current
/// index hot path. JSONB → serde Value → IndexValue.
fn current_extract_indexvalue(raw: &RawJsonb, field: &str) -> Option<oxidb::value::IndexValue> {
    use jsonb::keypath::KeyPath;
    use std::borrow::Cow;
    let parts: Vec<&str> = field.split('.').collect();
    let keypath: Vec<KeyPath> = parts
        .iter()
        .map(|p| KeyPath::Name(Cow::Borrowed(p)))
        .collect();
    let owned = raw.get_by_keypath(keypath.iter()).ok()??;
    let val: Value = jsonb::from_raw_jsonb(&owned.as_raw()).ok()?;
    Some(oxidb::value::IndexValue::from_json(&val))
}

/// Hand-rolled: JSONB → IndexValue directly via RawJsonb accessors.
/// Bypasses both `jsonb::from_raw_jsonb` (serde) AND `serde_json::Value`.
fn custom_extract_indexvalue(raw: &RawJsonb, field: &str) -> Option<oxidb::value::IndexValue> {
    use jsonb::keypath::KeyPath;
    use std::borrow::Cow;
    let parts: Vec<&str> = field.split('.').collect();
    let keypath: Vec<KeyPath> = parts
        .iter()
        .map(|p| KeyPath::Name(Cow::Borrowed(p)))
        .collect();
    let owned = raw.get_by_keypath(keypath.iter()).ok()??;
    let r = owned.as_raw();

    // Tag dispatch on RawJsonb scalar accessors.
    if r.is_null().ok()? {
        return Some(oxidb::value::IndexValue::Null);
    }
    if let Ok(Some(b)) = r.as_bool() {
        return Some(oxidb::value::IndexValue::Boolean(b));
    }
    if let Ok(Some(i)) = r.as_i64() {
        return Some(oxidb::value::IndexValue::Integer(i));
    }
    if let Ok(Some(f)) = r.as_f64() {
        return Some(oxidb::value::IndexValue::Float(f));
    }
    if let Ok(Some(s)) = r.as_str() {
        return Some(oxidb::value::IndexValue::parse_string(s.as_ref()));
    }
    // Arrays/objects: fall back to full decode + to_string (matches from_json).
    let v: Value = jsonb::from_raw_jsonb(&r).ok()?;
    Some(oxidb::value::IndexValue::from_json(&v))
}

struct Result {
    label: &'static str,
    ns_per_op: f64,
    mops: f64,
}

fn bench<F: FnMut()>(label: &'static str, iters: usize, mut f: F) -> Result {
    // Warmup
    for _ in 0..WARMUP_ITERS {
        for _ in 0..iters / WARMUP_ITERS.max(1) {
            f();
        }
    }
    let mut samples = Vec::with_capacity(BENCH_ITERS);
    for _ in 0..BENCH_ITERS {
        let t0 = Instant::now();
        for _ in 0..iters {
            f();
        }
        samples.push(t0.elapsed().as_nanos() as f64 / iters as f64);
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = samples[samples.len() / 2];
    Result {
        label,
        ns_per_op: median,
        mops: 1000.0 / median,
    }
}

fn print_results(corpus_label: &str, results: &[Result]) {
    println!("\n=== {} ===", corpus_label);
    println!("{:<48} {:>14} {:>14}", "step", "ns/op (med)", "Mops/sec");
    println!("{}", "-".repeat(80));
    let baseline = results
        .iter()
        .find(|r| r.label == "JSONB decode → Value (full)")
        .map(|r| r.ns_per_op)
        .unwrap_or(1.0);
    for r in results {
        let ratio = r.ns_per_op / baseline;
        println!(
            "{:<48} {:>14.0} {:>14.3}  {:.2}x",
            r.label, r.ns_per_op, r.mops, ratio
        );
    }
}

fn main() {
    let corpus = build_corpus();
    println!("oxidb decode profile — {} corpus entries\n", corpus.len());
    for (label, val, jsonb_bytes, json_bytes) in &corpus {
        println!(
            "  {:<6}  json_text={:>5} B  jsonb={:>5} B",
            label,
            json_bytes.len(),
            jsonb_bytes.len()
        );
        let _ = val;
    }

    // Pick a target field present in all corpora.
    let field = "_id";
    // Scaled down for large docs — full-decode of large dominates wall time.
    let iters_per_strategy = 50_000;

    for (label, val, jsonb_bytes, json_bytes) in &corpus {
        let mut results = Vec::new();

        // 1) serde_json text → Value (for reference: legacy .dat path)
        results.push(bench("serde_json::from_slice → Value (legacy text)", iters_per_strategy, || {
            let v: Value = serde_json::from_slice(black_box(json_bytes)).unwrap();
            black_box(v);
        }));

        // 2) jsonb full decode → Value (current decode_doc path)
        results.push(bench("JSONB decode → Value (full)", iters_per_strategy, || {
            let v = codec::decode_doc(black_box(jsonb_bytes)).unwrap();
            black_box(v);
        }));

        // 3) Encode: serde_json::Value → JSONB bytes
        results.push(bench("encode: Value → JSONB", iters_per_strategy, || {
            let b = codec::encode_doc(black_box(val)).unwrap();
            black_box(b);
        }));

        // 4) Encode: serde_json::Value → JSON text (for ref)
        results.push(bench("encode: Value → JSON text", iters_per_strategy, || {
            let b = serde_json::to_vec(black_box(val)).unwrap();
            black_box(b);
        }));

        // 5) Partial extract → Value (extract_field)
        results.push(bench("extract_field → Value", iters_per_strategy, || {
            let v = codec::extract_field(black_box(jsonb_bytes), black_box(field));
            black_box(v);
        }));

        // 6) Current index hot path: partial → Value → IndexValue
        results.push(bench("CURRENT: extract → Value → IndexValue", iters_per_strategy, || {
            let raw = RawJsonb::new(black_box(jsonb_bytes));
            let iv = current_extract_indexvalue(&raw, black_box(field));
            black_box(iv);
        }));

        // 7) Custom: partial → IndexValue directly (no serde Value)
        results.push(bench("CUSTOM: extract → IndexValue (no serde)", iters_per_strategy, || {
            let raw = RawJsonb::new(black_box(jsonb_bytes));
            let iv = custom_extract_indexvalue(&raw, black_box(field));
            black_box(iv);
        }));

        // 8) Full decode + IndexValue::from_json (worst case — what older callers do)
        results.push(bench("FULL decode → IndexValue", iters_per_strategy, || {
            let v = codec::decode_doc(black_box(jsonb_bytes)).unwrap();
            let iv = v.get(field).map(oxidb::value::IndexValue::from_json);
            black_box(iv);
        }));

        print_results(label, &results);
    }

    println!("\nNote: median of {} runs, {} ops each, warmup={}.",
             BENCH_ITERS, iters_per_strategy, WARMUP_ITERS);
}
