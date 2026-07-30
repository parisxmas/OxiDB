//! Where the memory goes while opening a SQL database.
//!
//! Opening a 1.2M-row disk-first database peaks around 333 MB against the 83 MB
//! it then runs in, and three attempts to reduce it by reasoning about which
//! structure was too big each helped less than predicted. The reason is that
//! process-level metrics (`phys_footprint`, cgroup `memory.current`) cannot tell
//! *live* bytes from bytes the allocator is holding after they were freed, and
//! those two call for completely different fixes.
//!
//! So this counts allocations directly. `LIVE` is bytes currently allocated and
//! not yet freed; `PEAK` is the high-water mark of that. Comparing peak-live
//! against the process's peak footprint settles it:
//!
//! - peak live ≈ peak footprint → a real structure is that big, and shrinking
//!   it is the fix
//! - peak live ≪ peak footprint → the allocator is retaining freed pages, and
//!   no data-structure change will help
//!
//! ```bash
//! cargo run --release -p oxidb-sql --example open_mem -- <sql-data-dir>
//! ```
//!
//! The directory is the `sql/` subdirectory of a data dir the server has
//! already loaded — see `bench/pg-memory/`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

struct Counting;

/// Tracks live bytes on every path that changes them. `realloc` is left to the
/// trait's default (alloc + copy + dealloc), which routes through these two.
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

fn mb(n: usize) -> String {
    format!("{:.1} MB", n as f64 / (1024.0 * 1024.0))
}

fn report(label: &str) {
    println!(
        "  {:<34} live {:>10}   peak {:>10}",
        label,
        mb(LIVE.load(Ordering::Relaxed)),
        mb(PEAK.load(Ordering::Relaxed))
    );
}

fn main() {
    let dir = std::env::args().nth(1).unwrap_or_else(|| {
        eprintln!("usage: open_mem <sql-data-dir>");
        std::process::exit(2);
    });
    let disk_first = std::env::var("OXIDB_SQL_DISK_FIRST").as_deref() != Ok("0");

    report("before open");
    PEAK.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);

    let mut opts = oxidb_sql::SqlOptions {
        disk_first,
        ..oxidb_sql::SqlOptions::from_env()
    };
    if let Ok(v) = std::env::var("FOLD_OPS")
        && let Ok(n) = v.parse::<usize>()
    {
        opts.replay_fold_ops = n;
    }
    let t = std::time::Instant::now();
    let db = oxidb_sql::SqlEngine::open_with_options(&dir, opts).expect("open");
    let open_ms = t.elapsed().as_millis();
    report(&format!("after open ({open_ms} ms)"));

    // A scan, so the steady state includes whatever reading touches.
    let n = db
        .execute("SELECT count(*) FROM orders")
        .map(|r| format!("{r:?}").len())
        .unwrap_or(0);
    report("after one query");
    println!("  (query result size {n}, keeps the engine alive)");

    drop(db);
    report("after drop");
}
