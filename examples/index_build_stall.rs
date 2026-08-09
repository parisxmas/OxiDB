//! What an index build now costs a concurrent writer.
//!
//! `index_build_barrier` holds writers off for the build, so the honest number
//! is not the build's own wall time but the **worst latency a writer sees**
//! while it runs. Both are printed, per collection size.
//!
//! ```bash
//! N=1000000 cargo run --release --example index_build_stall
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use oxidb::OxiDb;
use serde_json::json;
use tempfile::tempdir;

fn main() {
    let n: u64 = std::env::var("N")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200_000);
    let dir = tempdir().unwrap();
    let db = Arc::new(OxiDb::open(dir.path()).unwrap());
    for chunk in 0..n / 5_000 {
        let docs: Vec<_> = (chunk * 5_000..(chunk + 1) * 5_000)
            .map(|i| json!({"i": i, "email": format!("user{i}@x.com"), "country": "TR"}))
            .collect();
        db.insert_many("users", docs).unwrap();
    }

    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let db = Arc::clone(&db);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut worst = Duration::ZERO;
            let mut count = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let t = Instant::now();
                db.insert("users", json!({"i": -1, "email": "w@x.com"}))
                    .unwrap();
                worst = worst.max(t.elapsed());
                count += 1;
                std::thread::sleep(Duration::from_millis(1));
            }
            (worst, count)
        })
    };

    std::thread::sleep(Duration::from_millis(50));
    let t = Instant::now();
    db.create_index("users", "email").unwrap();
    let build = t.elapsed();
    std::thread::sleep(Duration::from_millis(50));
    stop.store(true, Ordering::Relaxed);
    let (worst, count) = writer.join().unwrap();

    println!("{n} documents");
    println!("  build (writers held off) {build:.2?}");
    println!("  worst writer latency     {worst:.2?}  over {count} writes");
}
