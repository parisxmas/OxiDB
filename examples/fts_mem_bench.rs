//! FTS memory profile at 1M documents: what the resident index costs, and
//! what a single search transiently allocates — the two OOM hypotheses from
//! the FTS report, measured instead of argued.
//!
//! Counting global allocator (the `open_mem.rs` technique): `LIVE` is bytes
//! allocated and not freed, so a number here is a real structure's size, not
//! allocator retention. Peaks are measured per phase against the phase's own
//! baseline.
//!
//! ```bash
//! N=1000000 cargo run --release --example fts_mem_bench
//! ```
//!
//! Measured history (1M docs, ~120 B text/doc, ~15 distinct terms):
//!
//! |                              | before      | after (intern + cap) |
//! |------------------------------|-------------|----------------------|
//! | resident index               | 1015 MB     | 785 MB               |
//! | common-term search peak      | 25.5 MB     | 7.3 MB               |
//! | two-common-terms search peak | 51.0 MB     | 7.3 MB (flat)        |
//!
//! (Documents themselves: 38 MB, disk-first. A measurement variant that
//! skipped `doc_terms` entirely put that structure at 558 MB — interning
//! recovers the String copies, 230 MB; the per-doc Vec + map overhead is the
//! rest and would need derive-at-removal to reclaim.)
//!
//! Vocabulary is engineered so searches hit the interesting shapes:
//! - "kahve"  appears in ~50% of documents (the common-term worst case: the
//!   scores map must hold an entry per matching doc before `limit` applies)
//! - "perde"  in ~5%
//! - "zümrüt" in ~0.1%
//! - per-doc filler terms give the index a realistic vocabulary size.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use oxidb::OxiDb;
use serde_json::json;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(l) };
        if !p.is_null() {
            let now = LIVE.fetch_add(l.size(), Ordering::Relaxed) + l.size();
            PEAK.fetch_max(now, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        LIVE.fetch_sub(l.size(), Ordering::Relaxed);
        unsafe { System.dealloc(p, l) };
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

fn mb(n: usize) -> f64 {
    n as f64 / (1024.0 * 1024.0)
}

fn live() -> usize {
    LIVE.load(Ordering::Relaxed)
}

/// Run `f`, report (live-delta after, peak-delta during) vs the call's start.
fn phase<T>(label: &str, f: impl FnOnce() -> T) -> T {
    let base = live();
    PEAK.store(base, Ordering::Relaxed);
    let t = Instant::now();
    let out = f();
    let after = live();
    let peak = PEAK.load(Ordering::Relaxed);
    println!(
        "{label:<44} live {:+9.1} MB   peak +{:8.1} MB   {:>8.2?}",
        (after as f64 - base as f64) / (1024.0 * 1024.0),
        mb(peak.saturating_sub(base)),
        t.elapsed()
    );
    out
}

fn main() {
    let n: u64 = std::env::var("N")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000_000);
    let dir = tempfile::tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();

    // Filler vocabulary: 40 words cycled per doc keep the corpus realistic
    // without making the fixture huge.
    let filler: Vec<String> = (0..50_000).map(|i| format!("kelime{i}")).collect();

    println!("=== FTS memory profile, {n} documents ===\n");

    phase("load (batched insert, no text index)", || {
        for chunk in 0..n / 5_000 {
            let docs: Vec<_> = (chunk * 5_000..(chunk + 1) * 5_000)
                .map(|i| {
                    let common = if i % 2 == 0 { "kahve" } else { "çay" };
                    let medium = if i % 20 == 0 { "perde" } else { "masa" };
                    let rare = if i % 1000 == 0 { "zümrüt" } else { "bakır" };
                    let body = format!(
                        "{common} {medium} {rare} {} {} {} sabah erken kalkan yolcu \
                         şehrin ışıklarına karşı yürüdü ve durdu",
                        filler[(i % 50_000) as usize],
                        filler[((i * 7 + 13) % 50_000) as usize],
                        filler[((i * 31 + 5) % 50_000) as usize],
                    );
                    json!({"i": i, "title": format!("belge {i}"), "body": body})
                })
                .collect();
            db.insert_many("docs", docs).unwrap();
        }
    });
    println!("{:<44} live {:9.1} MB\n", "resting after load", mb(live()));

    phase("create_text_index(title, body)", || {
        db.create_text_index("docs", vec!["title".into(), "body".into()])
            .unwrap();
    });
    println!(
        "{:<44} live {:9.1} MB\n",
        "resting with text index",
        mb(live())
    );

    // Searches. Each phase's "peak" is the transient the search allocated.
    phase("search rare term (zümrüt, ~0.1%), limit 10", || {
        db.text_search("docs", "zümrüt", 10).unwrap()
    });
    phase("search medium term (perde, ~5%), limit 10", || {
        db.text_search("docs", "perde", 10).unwrap()
    });
    phase("search common term (kahve, ~50%), limit 10", || {
        db.text_search("docs", "kahve", 10).unwrap()
    });
    phase("search two common terms, limit 10", || {
        db.text_search("docs", "kahve çay", 10).unwrap()
    });
    phase("search common, limit 500 + highlights", || {
        db.text_search_highlighted("docs", "kahve", 500, 160, 3)
            .unwrap()
    });

    println!("\n{:<44} live {:9.1} MB", "final resting", mb(live()));
}
