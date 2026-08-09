//! What the chunked scan costs and what it buys — same storage, both scan
//! variants, **same session** (numbers drift 5-8% between runs, so only an
//! in-session A/B is worth comparing).
//!
//! Three measurements per round: a full walk, a walk that stops at the first
//! document, and peak *live* allocated bytes (counting global allocator, so
//! this is a real structure's size and not the allocator's retention).
//!
//! ```bash
//! cargo run --release --example scan_chunk_bench          # 500k docs
//! N=2000000 DECODE=1 cargo run --release --example scan_chunk_bench
//! ```
//!
//! Measured on 500k / 2M documents, disk-first:
//!
//! | | unchunked | chunked | |
//! |---|---|---|---|
//! | full walk | 515 ms | 540 ms | 0.94-0.97x |
//! | stop at first doc | 7.7 ms | 1.6 ms | 5-7x |
//! | peak live bytes | 24 B/doc | 2 B/doc | 12.6x |
//!
//! The full-walk cost is the window passes over the key index; it shrinks as
//! the callback does real work (`DECODE=1` → 0.96-0.99x), and every real read
//! path decodes.

use oxidb::btree_storage::{BTreeStorage, StorageOptions};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

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

/// Peak *extra* live bytes the scan allocated above the resting baseline.
fn peak_over_baseline<F: FnOnce()>(f: F) -> f64 {
    let base = LIVE.load(Ordering::Relaxed);
    PEAK.store(base, Ordering::Relaxed);
    f();
    (PEAK.load(Ordering::Relaxed).saturating_sub(base)) as f64 / (1024.0 * 1024.0)
}

fn work(bytes: &[u8], decode: bool) -> u64 {
    if decode {
        oxidb::codec::decode_doc(bytes)
            .map(|d| d.as_object().map(|o| o.len()).unwrap_or(0) as u64)
            .unwrap_or(0)
    } else {
        bytes.len() as u64
    }
}

fn main() {
    let n: u64 = std::env::var("N")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(500_000);
    let dir = tempfile::tempdir().unwrap();
    let opts = StorageOptions {
        disk_first: true,
        ..StorageOptions::default()
    };
    let storage = BTreeStorage::open_with_options("ab", dir.path(), None, opts).unwrap();
    // Real stored documents, so the "work" round below pays what a real read
    // path pays per document (decode), not just a length add.
    let decode = std::env::var("DECODE").is_ok();
    for i in 1..=n {
        let doc = serde_json::json!({
            "_id": i,
            "name": format!("customer {i}"),
            "email": format!("user{i}@example.com"),
            "country": "TR",
            "total": (i % 997) as f64,
        });
        storage.insert(i, oxidb::codec::encode_doc(&doc).unwrap());
    }
    println!("{n} docs, decode={decode}");

    for round in 0..3 {
        let t = Instant::now();
        let mut c = 0u64;
        storage
            .scan_all_while(|_k, b| {
                c += work(b, decode);
                Ok(true)
            })
            .unwrap();
        let full_un = t.elapsed();

        let t = Instant::now();
        let mut c2 = 0u64;
        storage
            .scan_all_chunked_while(|_k, b| {
                c2 += work(b, decode);
                Ok(true)
            })
            .unwrap();
        let full_ch = t.elapsed();
        assert_eq!(c, c2);

        let t = Instant::now();
        storage.scan_all_while(|_k, _b| Ok(false)).unwrap();
        let stop_un = t.elapsed();

        let t = Instant::now();
        storage.scan_all_chunked_while(|_k, _b| Ok(false)).unwrap();
        let stop_ch = t.elapsed();

        println!(
            "round {round}: full  unchunked {:>9.2?}  chunked {:>9.2?}  ({:.2}x)",
            full_un,
            full_ch,
            full_un.as_secs_f64() / full_ch.as_secs_f64()
        );
        println!(
            "         stop-at-1 unchunked {:>9.2?}  chunked {:>9.2?}  ({:.0}x)",
            stop_un,
            stop_ch,
            stop_un.as_secs_f64() / stop_ch.as_secs_f64()
        );

        let mem_un = peak_over_baseline(|| {
            storage.scan_all_while(|_k, _b| Ok(true)).unwrap();
        });
        let mem_ch = peak_over_baseline(|| {
            storage.scan_all_chunked_while(|_k, _b| Ok(true)).unwrap();
        });
        println!(
            "         peak live  unchunked {mem_un:>7.2} MB  chunked {mem_ch:>7.2} MB  ({:.1}x)",
            mem_un / mem_ch
        );
    }
}
