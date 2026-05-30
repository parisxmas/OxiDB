//! In-process memory probe: confirm the 1M-doc-bench RSS gap vs MongoDB is
//! driven by OxiDB's in-memory caches (doc_cache + bytes_cache) rather than a
//! leak. Mirrors the comparison-mongodb dataset shape and index set, loads 1M
//! docs into the real engine, exercises reads to populate caches, and prints
//! process RSS.
//!
//! Not part of the normal suite — `#[ignore]`d, runs only when invoked
//! explicitly. The cache sizes are read from the same env vars the server
//! honors, so run it twice to compare:
//!
//!   # default caches (bytes=1M, doc=100K) — the as-shipped bench config
//!   cargo test --test mem_probe --release -- --ignored --nocapture
//!
//!   # capped caches (~MongoDB's 0.5 GB budget)
//!   OXIDB_DOC_BYTES_CACHE_SIZE=50000 OXIDB_DOC_CACHE_SIZE=20000 \
//!     cargo test --test mem_probe --release -- --ignored --nocapture

use oxidb::OxiDb;
use serde_json::{Value, json};

const FIRST: &[&str] = &["Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Heidi"];
const LAST: &[&str] = &["Smith", "Jones", "Lee", "Patel", "Kim", "Garcia", "Brown", "Khan"];
const DEPTS: &[&str] = &["Sales", "Eng", "HR", "Finance", "Ops", "Legal"];
const CITIES: &[&str] = &["Tokyo", "Paris", "Berlin", "Osaka", "Madrid", "Rome", "Oslo", "Lima"];
const COUNTRIES: &[&str] = &["JP", "FR", "DE", "ES", "IT", "NO", "PE", "US"];
const STATUSES: &[&str] = &["active", "inactive", "pending"];
const TAGS: &[&str] = &["a", "b", "c", "d", "e", "f"];

/// Deterministic LCG so the run is reproducible without an rng dependency.
struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 >> 16
    }
    fn pick<'a>(&mut self, xs: &[&'a str]) -> &'a str {
        xs[(self.next() as usize) % xs.len()]
    }
    fn range(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn gen_doc(rng: &mut Lcg, i: usize) -> Value {
    let first = rng.pick(FIRST);
    let last = rng.pick(LAST);
    let n_tags = 1 + (rng.range(3) as usize);
    let tags: Vec<Value> = (0..n_tags).map(|_| json!(rng.pick(TAGS))).collect();
    json!({
        "seq": i,
        "name": format!("{first} {last}"),
        "email": format!("{}.{}.{}@test.com", first.to_lowercase(), last.to_lowercase(), i),
        "age": 18 + rng.range(60),
        "salary": 30000.0 + rng.range(170000) as f64,
        "department": rng.pick(DEPTS),
        "city": rng.pick(CITIES),
        "country": rng.pick(COUNTRIES),
        "status": rng.pick(STATUSES),
        "score": rng.range(10000) as f64 / 100.0,
        "verified": rng.range(2) == 1,
        "rating": rng.range(5) + 1,
        "tags": tags,
        "address": { "street": format!("{} Main St", 100 + rng.range(9900)), "zip": format!("{:05}", rng.range(100000)) },
    })
}

/// Resident set size of this process, in MiB. macOS: `ps` reports KiB.
fn rss_mib() -> f64 {
    let pid = std::process::id();
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .expect("ps");
    let kib: f64 = String::from_utf8_lossy(&out.stdout).trim().parse().unwrap_or(0.0);
    kib / 1024.0
}

/// A/B: byte-level post-filter find vs the Value path, for a large unindexed
/// result. The earlier (reverted) attempt regressed 2-11x because it decoded
/// every doc; this one byte-filters (skips non-matches) and transcodes matches.
#[test]
#[ignore = "perf A/B; run explicitly with --ignored --nocapture"]
fn postfilter_vs_value_timing() {
    let total: usize = std::env::var("PROBE_DOCS").ok().and_then(|v| v.parse().ok()).unwrap_or(500_000);
    let dir = tempfile::tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();
    let mut rng = Lcg(42);
    let mut i = 0;
    while i < total {
        let end = (i + 5000).min(total);
        db.insert_many("bench", (i..end).map(|k| gen_doc(&mut rng, k)).collect()).unwrap();
        i = end;
    }
    let q = json!({"verified": true});
    let opts = oxidb::query::FindOptions::default();
    // Warm + measure the Value path.
    let t0 = std::time::Instant::now();
    let vlen = db.find("bench", &q).unwrap().len();
    let value_ms = t0.elapsed().as_millis();
    // Measure the byte post-filter path (collection-level, via a fresh handle
    // would be ideal; engine method exercises the same code).
    let t1 = std::time::Instant::now();
    let (count, buf) = db.find_oxiwire_postfilter("bench", &q, &opts).unwrap().unwrap();
    let bytes_ms = t1.elapsed().as_millis();
    println!("\n── postfilter A/B: {total} docs, query verified=true ──");
    println!("  Value path   : {value_ms} ms  ({vlen} docs materialized as Vec<Value>)");
    println!("  Byte path    : {bytes_ms} ms  ({count} docs, {} KiB encoded buffer)", buf.len() / 1024);
    println!("  (byte path matches Value count: {})", count == vlen);
}

#[test]
#[ignore = "heavy 1M-doc memory probe; run explicitly with --ignored --nocapture"]
fn one_million_doc_rss() {
    let total: usize = std::env::var("PROBE_DOCS").ok().and_then(|v| v.parse().ok()).unwrap_or(1_000_000);
    let bytes_cap = oxidb::doc_bytes_cache::default_capacity();
    let doc_cap = oxidb::doc_cache::default_capacity();

    // PROBE_DIR (persistent) lets a second process reopen the built DB to
    // measure steady-state resident memory without the build high-water that a
    // single process retains. PROBE_REOPEN=1 => open-and-measure only.
    let probe_dir = std::env::var("PROBE_DIR").ok();
    let _tmp = if probe_dir.is_none() {
        Some(tempfile::tempdir().unwrap())
    } else {
        None
    };
    let dir_path: std::path::PathBuf = match &probe_dir {
        Some(d) => std::path::PathBuf::from(d),
        None => _tmp.as_ref().unwrap().path().to_path_buf(),
    };

    if std::env::var("PROBE_REOPEN").is_ok() {
        println!("\n── mem_probe REOPEN: {dir_path:?} ──");
        println!("  RSS before open: {:.0} MiB", rss_mib());
        let db = OxiDb::open(&dir_path).unwrap();
        println!("  RSS after open : {:.0} MiB", rss_mib());
        // Touch each indexed field with a small indexed query (faults in only
        // the pages the query needs, not the whole index).
        let mut n = 0;
        n += db.find("bench", &json!({"department": "Sales"})).unwrap().len();
        n += db.find("bench", &json!({"city": "Tokyo"})).unwrap().len();
        n += db.count("bench", &json!({"age": {"$gte": 50}})).unwrap();
        println!("  RSS after indexed queries (touched {n}): {:.0} MiB", rss_mib());
        let m = db.memory_report("bench").unwrap();
        let mib = |b: usize| b as f64 / 1024.0 / 1024.0;
        println!("  field-index resident estimate: {:.1} MiB", mib(m.field_index_bytes));
        return;
    }

    let db = OxiDb::open(&dir_path).unwrap();

    println!("\n── mem_probe: {total} docs ──");
    println!("  OXIDB_DOC_BYTES_CACHE_SIZE = {bytes_cap}");
    println!("  OXIDB_DOC_CACHE_SIZE       = {doc_cap}");
    println!("  RSS before load: {:.0} MiB", rss_mib());

    // Insert (batched) — mirrors the bench's insert_many path, which also
    // populates the bytes cache (encode-once-at-write).
    let mut rng = Lcg(42);
    let batch = 5_000;
    let mut i = 0;
    while i < total {
        let end = (i + batch).min(total);
        let docs: Vec<Value> = (i..end).map(|k| gen_doc(&mut rng, k)).collect();
        db.insert_many("bench", docs).unwrap();
        i = end;
    }
    let after_insert = rss_mib();
    println!(
        "  RSS after {total} inserts: {:.0} MiB  (storage residency ~{:.0} MiB, {:.0} B/doc)",
        after_insert,
        after_insert - 7.0,
        (after_insert - 7.0) * 1024.0 * 1024.0 / total as f64
    );

    // Same index set as the bench: 4 field indexes + 1 composite. Measure RSS
    // after EACH so we can attribute per-index cost. These points have no
    // transient Value allocations, so the deltas are clean.
    let mut prev = after_insert;
    for (label, field) in [("seq", "seq"), ("age", "age"), ("department", "department"), ("city", "city")] {
        db.create_index("bench", field).unwrap();
        let now = rss_mib();
        println!("  + index {label:<11} -> {now:.0} MiB  (+{:.0} MiB, {:.0} B/doc)", now - prev, (now - prev) * 1024.0 * 1024.0 / total as f64);
        prev = now;
    }
    db.create_composite_index("bench", vec!["department".into(), "status".into()]).unwrap();
    let after_idx = rss_mib();
    println!("  + composite(dept,status) -> {after_idx:.0} MiB  (+{:.0} MiB)", after_idx - prev);
    println!("  => 5 indexes total: +{:.0} MiB", after_idx - after_insert);

    // Exercise reads that return large result sets, to populate doc_cache
    // (deserialized Values) the way query tests do.
    let mut sink = 0usize;
    for _ in 0..3 {
        for st in STATUSES {
            sink += db.find("bench", &json!({"status": st})).unwrap().len();
        }
        for d in DEPTS {
            sink += db.find("bench", &json!({"department": d})).unwrap().len();
        }
    }
    let after_reads = rss_mib();
    println!("  RSS after reads (touched {sink} docs): {after_reads:.0} MiB  (+{:.0} MiB over post-index)", after_reads - after_idx);

    // Exact, allocator-independent breakdown computed from the live structures.
    let m = db.memory_report("bench").unwrap();
    let mib = |b: usize| b as f64 / 1024.0 / 1024.0;
    println!("\n  ── EXACT resident structure bytes (allocator-independent) ──");
    println!("  primary store payload : {:>8.1} MiB  ({} docs, {:.0} B/doc encoded)", mib(m.storage_payload), m.storage_entries, m.storage_payload as f64 / m.storage_entries as f64);
    println!("  primary store overhead: {:>8.1} MiB  (per-entry container)", mib(m.storage_overhead));
    println!("  field indexes (4)     : {:>8.1} MiB", mib(m.field_index_bytes));
    println!("  composite index (1)   : {:>8.1} MiB", mib(m.composite_index_bytes));
    println!("  ───────────────────────────────────────");
    println!("  STRUCTURE TOTAL       : {:>8.1} MiB  (vs RSS {:.0} MiB)", mib(m.total()), after_reads);
    println!("  RSS − structures      : {:>8.1} MiB  (LRU caches + transient/allocator high-water)\n", after_reads - mib(m.total()));
}
