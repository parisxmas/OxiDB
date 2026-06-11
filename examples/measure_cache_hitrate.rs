//! Measure DocCache hit ratio under a Zipfian (skewed) read workload —
//! the standard model for OLTP/web doc-access patterns where a few
//! hot keys dominate.
//!
//! Used to decide whether to optimize:
//!   (A) the COLD path (cache miss → decode JSONB) — matters if hit
//!       ratio is low, or
//!   (B) the HOT path (cache hit → re-serialize Value → JSON text) —
//!       matters if hit ratio is high.
//!
//! Sweeps over (working set / cache capacity) and Zipfian skew.
//!
//! Run:  cargo run --release --example measure_cache_hitrate

use std::io::Write;
use std::time::Instant;

use serde_json::json;
use tempfile::TempDir;

use oxidb::btree_collection::BTreeCollection;

/// Inverse-CDF Zipf sampler: P(k) ∝ 1/k^s for k in [1..n].
/// Builds a CDF once, then samples in O(log n) per query via binary search.
struct Zipf {
    cdf: Vec<f64>,
    n: usize,
}

impl Zipf {
    fn new(n: usize, s: f64) -> Self {
        let mut cdf = Vec::with_capacity(n);
        let mut acc = 0.0;
        for k in 1..=n {
            acc += 1.0 / (k as f64).powf(s);
        }
        let norm = acc;
        let mut running = 0.0;
        for k in 1..=n {
            running += 1.0 / (k as f64).powf(s);
            cdf.push(running / norm);
        }
        Self { cdf, n }
    }

    /// Sample a rank in [0..n) where 0 is the hottest key.
    fn sample(&self, u: f64) -> usize {
        // Binary search for first cdf > u.
        let mut lo = 0;
        let mut hi = self.n;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.cdf[mid] < u {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo.min(self.n - 1)
    }
}

/// Tiny xorshift PRNG — deterministic, fast, no external deps.
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn next_f64(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

fn populate_collection(col: &BTreeCollection, n: usize) -> Vec<u64> {
    let mut ids = Vec::with_capacity(n);
    for i in 0..n {
        let doc = json!({
            "_id": i as u64,
            "user": format!("user-{}", i),
            "score": (i % 100) as i64,
            "active": i % 7 != 0,
            "tags": ["alpha", "beta", "gamma"],
            "address": {"city": format!("city-{}", i % 50), "country": "TR"},
        });
        let id = col.insert(doc).expect("insert");
        ids.push(id);
    }
    ids
}

fn run_workload(
    col: &BTreeCollection,
    ids: &[u64],
    skew: f64,
    queries: usize,
    seed: u64,
) -> (f64, f64) {
    let n = ids.len();
    let zipf = Zipf::new(n, skew);
    let mut rng = Rng::new(seed);

    // Warmup: enough queries to populate the cache to steady-state.
    let warmup = queries / 4;
    for _ in 0..warmup {
        let r = zipf.sample(rng.next_f64());
        let id = ids[r];
        let _ = col.load_doc_arc(id);
    }
    col.doc_cache_stats_reset();

    let t0 = Instant::now();
    for _ in 0..queries {
        let r = zipf.sample(rng.next_f64());
        let id = ids[r];
        let _ = col.load_doc_arc(id);
    }
    let elapsed_ns = t0.elapsed().as_nanos() as f64;

    let stats = col.doc_cache_stats();
    let total = stats.hits + stats.misses;
    let hit_ratio = if total == 0 {
        0.0
    } else {
        stats.hits as f64 / total as f64
    };
    let ns_per_op = elapsed_ns / queries as f64;
    (hit_ratio, ns_per_op)
}

fn main() {
    println!("DocCache hit-ratio under Zipfian workload\n");
    println!("Each row: N docs in collection, cache capacity, Zipf skew (s)");
    println!("           → measured hit ratio + ns/op (load_doc_arc)");

    // Three working-set / cache-cap regimes:
    //   - working set << cache cap (everything fits) — should be ~100% after warmup
    //   - working set ~ cache cap (boundary)
    //   - working set >> cache cap (eviction-bound) — depends on skew
    let configs: &[(usize, usize, &str)] = &[
        (1_000, 10_000, "working set << cache (1K docs, cap 10K)"),
        (10_000, 10_000, "working set ~ cache (10K docs, cap 10K)"),
        (
            100_000,
            10_000,
            "working set 10× cache (100K docs, cap 10K)",
        ),
        (
            1_000_000,
            10_000,
            "working set 100× cache (1M docs, cap 10K)",
        ),
    ];

    let skews = [0.7, 1.0, 1.2, 1.5];
    let queries = 100_000;

    for (n_docs, cache_cap, label) in configs {
        println!("\n── {} ──", label);
        std::io::stdout().flush().ok();
        let tmp = TempDir::new().unwrap();
        let col = BTreeCollection::open("cache_bench", tmp.path(), None).unwrap();
        // Lazy sync: we're measuring read-path cache, not write durability.
        // Strict fsync (the default since the ACID hardening commit) makes
        // the populate phase impractically slow for the 1M-doc config.
        col.set_lazy_sync(true);
        col.set_cache_capacity(*cache_cap);
        let t_pop = Instant::now();
        let ids = populate_collection(&col, *n_docs);
        println!(
            "  populated {} docs in {:.1}s",
            n_docs,
            t_pop.elapsed().as_secs_f64()
        );
        std::io::stdout().flush().ok();
        // Force cold cache for the actual measurement.
        col.doc_cache_clear();

        println!("  {:>10}  {:>10}  {:>10}", "skew", "hit ratio", "ns/op");
        std::io::stdout().flush().ok();
        for &s in &skews {
            let (hit, ns) = run_workload(&col, &ids, s, queries, 0xCAFE_F00D);
            println!("  {:>10.2}  {:>9.1}%  {:>10.0}", s, hit * 100.0, ns);
            std::io::stdout().flush().ok();
        }
    }

    println!("\nInterpretation:");
    println!("  - If hit ratio ≥ 80% on realistic configs: optimize HOT path");
    println!("    (cache returns Arc<Value>, then we re-serialize to JSON text on every find).");
    println!("    Switching that to RawJsonb → text is ~4-5× faster end-to-end.");
    println!("  - If hit ratio < 50%: optimize COLD path");
    println!("    (cache miss → decode JSONB → Value). Skip Value entirely for the wire path.");
}
