//! p99 soak harness — sustained wire-level load with tail-latency + drift checks.
//!
//! Spawns the real server binary and drives the **document engine** from many
//! connections for a configurable duration, then reports p50/p95/p99/p999/max
//! latency for reads and writes and checks that the server stays *stable over
//! time* — the thing a short benchmark can't see.
//!
//! The workload is deliberately **steady-state**: a fixed key space is pre-seeded
//! and workers only read and update existing keys (no net inserts), so the data
//! set size is constant. That makes RSS/fd growth a clean signal — in steady
//! state a healthy server plateaus, and any sustained climb is a leak. Latency
//! drift is measured by splitting the run into an early and a late half and
//! comparing read p99: a server that degrades under sustained load shows
//! `p99(late) >> p99(early)`.
//!
//! It is `#[ignore]`d (soaks are long), so it never runs in a normal `cargo test`.
//! Run it explicitly:
//!
//! ```bash
//! cargo test -p oxidb-server --test p99_soak -- --ignored --nocapture
//! # tune via env:
//! SOAK_SECS=600 SOAK_CONNS=32 SOAK_KEYSPACE=200000 \
//!   cargo test -p oxidb-server --test p99_soak -- --ignored --nocapture
//! ```
//!
//! Env knobs (all optional):
//!   SOAK_SECS            total run seconds                 (default 20)
//!   SOAK_CONNS           concurrent worker connections     (default 16)
//!   SOAK_KEYSPACE        pre-seeded document count         (default 50_000)
//!   SOAK_WARMUP_SECS     ignore RSS before this            (default 5)
//!   SOAK_REPORT_SECS     progress print interval           (default 5)
//!   SOAK_RSS_GROWTH_PCT  fail if RSS grows more than this  (default 30)
//!   SOAK_FD_GROWTH       fail if fds grow more than this   (default 64, Linux)
//!   SOAK_DRIFT           fail if p99(late)/p99(early) over (default 3.0)
//!   SOAK_P99_MS          hard read-p99 ceiling in ms, 0=off(default 0)

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

// ---- env helpers -----------------------------------------------------------

fn env_u64(k: &str, def: u64) -> u64 {
    std::env::var(k)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(def)
}
fn env_f64(k: &str, def: f64) -> f64 {
    std::env::var(k)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(def)
}

// ---- latency histogram -----------------------------------------------------

/// 1µs-resolution histogram over [0, CAP) µs; anything slower lands in `over`
/// but still counts toward totals and `max`. Exact percentiles up to CAP.
const CAP: usize = 200_000; // 200 ms

struct Hist {
    buckets: Vec<u64>,
    over: u64,
    count: u64,
    sum_us: u128,
    max_us: u64,
}
impl Hist {
    fn new() -> Self {
        Hist {
            buckets: vec![0; CAP],
            over: 0,
            count: 0,
            sum_us: 0,
            max_us: 0,
        }
    }
    fn record(&mut self, us: u64) {
        if (us as usize) < CAP {
            self.buckets[us as usize] += 1;
        } else {
            self.over += 1;
        }
        self.count += 1;
        self.sum_us += us as u128;
        if us > self.max_us {
            self.max_us = us;
        }
    }
    fn merge(&mut self, o: &Hist) {
        for (a, b) in self.buckets.iter_mut().zip(o.buckets.iter()) {
            *a += *b;
        }
        self.over += o.over;
        self.count += o.count;
        self.sum_us += o.sum_us;
        if o.max_us > self.max_us {
            self.max_us = o.max_us;
        }
    }
    /// Percentile in **milliseconds**. Overflowing ranks report `max`.
    fn pct_ms(&self, p: f64) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        let rank = ((self.count as f64) * p).ceil().max(1.0) as u64;
        let mut acc = 0u64;
        for (i, &c) in self.buckets.iter().enumerate() {
            acc += c;
            if acc >= rank {
                return i as f64 / 1000.0;
            }
        }
        self.max_us as f64 / 1000.0 // rank fell into `over`
    }
    fn mean_ms(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            (self.sum_us as f64 / self.count as f64) / 1000.0
        }
    }
    fn max_ms(&self) -> f64 {
        self.max_us as f64 / 1000.0
    }
}

// ---- process resource sampling (Linux /proc, else `ps`) --------------------

fn sample_rss_kib(pid: u32) -> Option<u64> {
    if let Ok(s) = std::fs::read_to_string(format!("/proc/{pid}/status")) {
        for line in s.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:") {
                return rest.trim().trim_end_matches("kB").trim().parse().ok();
            }
        }
    }
    // macOS / other: `ps -o rss=` reports KiB
    let out = Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    String::from_utf8(out.stdout).ok()?.trim().parse().ok()
}

/// Open file-descriptor count. Linux only (via /proc); `None` elsewhere.
fn sample_fd(pid: u32) -> Option<u64> {
    std::fs::read_dir(format!("/proc/{pid}/fd"))
        .ok()
        .map(|rd| rd.count() as u64)
}

// ---- server lifecycle + wire ----------------------------------------------

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct Guard {
    child: Child,
    _dir: tempfile::TempDir,
    port: u16,
    pid: u32,
}
impl Drop for Guard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn(pool: u64) -> Guard {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let child = Command::new(env!("CARGO_BIN_EXE_oxidb-server"))
        .env("OXIDB_DATA", dir.path())
        .env("OXIDB_ADDR", format!("127.0.0.1:{port}"))
        .env("OXIDB_POOL_SIZE", pool.to_string())
        .env("OXIDB_IDLE_TIMEOUT", "0") // never drop an idle soak connection
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let pid = child.id();
    let g = Guard {
        child,
        _dir: dir,
        port,
        pid,
    };
    let deadline = Instant::now() + Duration::from_secs(20);
    while TcpStream::connect(("127.0.0.1", g.port)).is_err() {
        assert!(Instant::now() < deadline, "server port never opened");
        std::thread::sleep(Duration::from_millis(50));
    }
    g
}

fn connect(port: u16) -> TcpStream {
    let s = TcpStream::connect(("127.0.0.1", port)).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    s.set_nodelay(true).ok();
    s
}

/// One length-prefixed (4-byte LE) JSON request/response round trip.
fn call(s: &mut TcpStream, req: &Value) -> Value {
    let body = serde_json::to_vec(req).unwrap();
    s.write_all(&(body.len() as u32).to_le_bytes()).unwrap();
    s.write_all(&body).unwrap();
    let mut len = [0u8; 4];
    s.read_exact(&mut len).unwrap();
    let mut buf = vec![0u8; u32::from_le_bytes(len) as usize];
    s.read_exact(&mut buf).unwrap();
    serde_json::from_slice(&buf).unwrap()
}

// ---- tiny deterministic PRNG (no rand dep) ---------------------------------

struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed | 1) // must be non-zero for xorshift
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

// ---- per-worker result -----------------------------------------------------

struct WorkerOut {
    read_early: Hist,
    read_late: Hist,
    write_early: Hist,
    write_late: Hist,
    reads: u64,
    writes: u64,
    errors: u64,
}

const COLL: &str = "soak";

fn main_test() {
    let secs = env_u64("SOAK_SECS", 20);
    let conns = env_u64("SOAK_CONNS", 16).max(1);
    let keyspace = env_u64("SOAK_KEYSPACE", 50_000).max(1);
    let warmup = env_u64("SOAK_WARMUP_SECS", 5);
    let report_every = env_u64("SOAK_REPORT_SECS", 5).max(1);
    let max_rss_growth_pct = env_f64("SOAK_RSS_GROWTH_PCT", 30.0);
    let max_fd_growth = env_u64("SOAK_FD_GROWTH", 64);
    let max_drift = env_f64("SOAK_DRIFT", 3.0);
    let p99_ceiling_ms = env_f64("SOAK_P99_MS", 0.0);

    println!("\n== p99 soak ==  {secs}s · {conns} conns · {keyspace} keys · warmup {warmup}s");

    let g = spawn(conns);

    // --- seed a fixed key space (steady-state workload reads/updates these) ---
    {
        let mut s = connect(g.port);
        let r = call(
            &mut s,
            &json!({"cmd": "create_index", "collection": COLL, "field": "k"}),
        );
        assert!(
            r["ok"].as_bool().unwrap_or(false),
            "create_index failed: {r}"
        );
        let mut next = 0u64;
        while next < keyspace {
            let hi = (next + 1000).min(keyspace);
            let docs: Vec<Value> = (next..hi)
                .map(|k| json!({"k": k, "hits": 0, "pad": "xxxxxxxxxxxxxxxx"}))
                .collect();
            let r = call(
                &mut s,
                &json!({"cmd": "insert_many", "collection": COLL, "docs": docs}),
            );
            assert!(
                r["ok"].as_bool().unwrap_or(false),
                "seed insert failed: {r}"
            );
            next = hi;
        }
        println!("seeded {keyspace} docs");
    }

    let stop = Arc::new(AtomicBool::new(false));
    let ops = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    let half = Duration::from_secs(secs) / 2;

    // --- monitor: periodic progress + RSS/fd timeline -----------------------
    let mon_stop = stop.clone();
    let mon_ops = ops.clone();
    let pid = g.pid;
    let monitor = std::thread::spawn(move || {
        // (elapsed_secs, rss_kib, fd)
        let mut samples: Vec<(u64, u64, Option<u64>)> = Vec::new();
        let mut last_ops = 0u64;
        let mut last_t = Instant::now();
        while !mon_stop.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_millis(250));
            let el = start.elapsed();
            if last_t.elapsed() >= Duration::from_secs(report_every) {
                let now_ops = mon_ops.load(Ordering::Relaxed);
                let dt = last_t.elapsed().as_secs_f64();
                let rate = (now_ops - last_ops) as f64 / dt;
                let rss = sample_rss_kib(pid).unwrap_or(0);
                let fd = sample_fd(pid);
                samples.push((el.as_secs(), rss, fd));
                println!(
                    "  t={:>4}s  {:>9.0} ops/s  rss={:>6.1} MB  fd={}",
                    el.as_secs(),
                    rate,
                    rss as f64 / 1024.0,
                    fd.map(|f| f.to_string()).unwrap_or_else(|| "n/a".into()),
                );
                last_ops = now_ops;
                last_t = Instant::now();
            }
        }
        samples
    });

    // --- workers ------------------------------------------------------------
    let mut handles = Vec::new();
    for wid in 0..conns {
        let stop = stop.clone();
        let ops = ops.clone();
        let port = g.port;
        handles.push(std::thread::spawn(move || {
            let mut s = connect(port);
            let mut rng = Rng::new(0x9E37_79B9_7F4A_7C15 ^ wid.wrapping_mul(0x2545_F491_4F6C_DD1D));
            let mut out = WorkerOut {
                read_early: Hist::new(),
                read_late: Hist::new(),
                write_early: Hist::new(),
                write_late: Hist::new(),
                reads: 0,
                writes: 0,
                errors: 0,
            };
            while !stop.load(Ordering::Relaxed) {
                let k = rng.below(keyspace);
                let roll = rng.below(100);
                // 70% point read, 25% update, 5% small indexed count
                let (req, is_write) = if roll < 70 {
                    (
                        json!({"cmd": "find_one", "collection": COLL, "query": {"k": k}}),
                        false,
                    )
                } else if roll < 95 {
                    (
                        json!({"cmd": "update_one", "collection": COLL,
                               "query": {"k": k}, "update": {"$inc": {"hits": 1}}}),
                        true,
                    )
                } else {
                    (
                        json!({"cmd": "count", "collection": COLL,
                               "query": {"k": {"$lt": 100}}}),
                        false,
                    )
                };
                let t0 = Instant::now();
                let resp = call(&mut s, &req);
                let us = t0.elapsed().as_micros() as u64;
                if !resp["ok"].as_bool().unwrap_or(false) {
                    out.errors += 1;
                }
                let late = start.elapsed() >= half;
                match (is_write, late) {
                    (false, false) => {
                        out.read_early.record(us);
                        out.reads += 1;
                    }
                    (false, true) => {
                        out.read_late.record(us);
                        out.reads += 1;
                    }
                    (true, false) => {
                        out.write_early.record(us);
                        out.writes += 1;
                    }
                    (true, true) => {
                        out.write_late.record(us);
                        out.writes += 1;
                    }
                }
                ops.fetch_add(1, Ordering::Relaxed);
            }
            out
        }));
    }

    // --- run for the configured duration ------------------------------------
    std::thread::sleep(Duration::from_secs(secs));
    stop.store(true, Ordering::Relaxed);

    let mut merged = WorkerOut {
        read_early: Hist::new(),
        read_late: Hist::new(),
        write_early: Hist::new(),
        write_late: Hist::new(),
        reads: 0,
        writes: 0,
        errors: 0,
    };
    for h in handles {
        let w = h.join().unwrap();
        merged.read_early.merge(&w.read_early);
        merged.read_late.merge(&w.read_late);
        merged.write_early.merge(&w.write_early);
        merged.write_late.merge(&w.write_late);
        merged.reads += w.reads;
        merged.writes += w.writes;
        merged.errors += w.errors;
    }
    let samples = monitor.join().unwrap();
    let elapsed = start.elapsed().as_secs_f64();

    // --- cumulative read/write histograms -----------------------------------
    let mut reads = Hist::new();
    reads.merge(&merged.read_early);
    reads.merge(&merged.read_late);
    let mut writes = Hist::new();
    writes.merge(&merged.write_early);
    writes.merge(&merged.write_late);
    let total = merged.reads + merged.writes;

    // --- report -------------------------------------------------------------
    println!("\n---- results ----------------------------------------------");
    println!(
        "ops: {total}  ({:.0} ops/s over {elapsed:.1}s)   reads {}  writes {}  errors {}",
        total as f64 / elapsed,
        merged.reads,
        merged.writes,
        merged.errors
    );
    let row = |name: &str, h: &Hist| {
        println!(
            "  {name:<6} mean {:>6.3}  p50 {:>6.3}  p95 {:>6.3}  p99 {:>6.3}  p999 {:>6.3}  max {:>7.3}  (ms)  slow>{}ms {}",
            h.mean_ms(),
            h.pct_ms(0.50),
            h.pct_ms(0.95),
            h.pct_ms(0.99),
            h.pct_ms(0.999),
            h.max_ms(),
            CAP / 1000,
            h.over,
        )
    };
    println!("latency:");
    row("read", &reads);
    row("write", &writes);

    // --- drift: read p99 early vs late --------------------------------------
    let p99_early = merged.read_early.pct_ms(0.99);
    let p99_late = merged.read_late.pct_ms(0.99);
    let drift = if p99_early > 0.0 {
        p99_late / p99_early
    } else {
        1.0
    };
    println!("read-p99 drift: early {p99_early:.3} ms -> late {p99_late:.3} ms  ({drift:.2}x)");

    // --- resource drift -----------------------------------------------------
    let post_warmup: Vec<&(u64, u64, Option<u64>)> =
        samples.iter().filter(|(t, _, _)| *t >= warmup).collect();
    let mut rss_growth_pct = 0.0;
    let mut fd_ok = true;
    if let (Some(first), Some(last)) = (post_warmup.first(), post_warmup.last()) {
        let (rss0, rss1) = (first.1 as f64, last.1 as f64);
        let peak = post_warmup.iter().map(|s| s.1).max().unwrap_or(0);
        rss_growth_pct = if rss0 > 0.0 {
            (rss1 - rss0) / rss0 * 100.0
        } else {
            0.0
        };
        println!(
            "rss: baseline {:.1} MB -> final {:.1} MB  ({rss_growth_pct:+.1}%)  peak {:.1} MB",
            rss0 / 1024.0,
            rss1 / 1024.0,
            peak as f64 / 1024.0,
        );
        match (first.2, last.2) {
            (Some(fd0), Some(fd1)) => {
                let grew = fd1.saturating_sub(fd0);
                fd_ok = grew <= max_fd_growth;
                println!("fd:  baseline {fd0} -> final {fd1}  (+{grew})");
            }
            _ => println!("fd:  n/a (Linux /proc only)"),
        }
    } else {
        println!(
            "rss/fd: not enough samples past warmup ({}s) — run longer",
            warmup
        );
    }

    // --- verdict ------------------------------------------------------------
    println!("-----------------------------------------------------------");
    let mut fails: Vec<String> = Vec::new();
    if rss_growth_pct > max_rss_growth_pct {
        fails.push(format!(
            "RSS grew {rss_growth_pct:.1}% > {max_rss_growth_pct:.0}% (possible leak)"
        ));
    }
    if !fd_ok {
        fails.push(format!(
            "fd count grew more than {max_fd_growth} (possible fd leak)"
        ));
    }
    if p99_early > 0.0 && drift > max_drift {
        fails.push(format!(
            "read-p99 drifted {drift:.2}x > {max_drift:.1}x (degrades over time)"
        ));
    }
    if p99_ceiling_ms > 0.0 && reads.pct_ms(0.99) > p99_ceiling_ms {
        fails.push(format!(
            "read p99 {:.3} ms > ceiling {p99_ceiling_ms:.3} ms",
            reads.pct_ms(0.99)
        ));
    }
    if total == 0 {
        fails.push("no operations completed".into());
    }

    if fails.is_empty() {
        println!("VERDICT: PASS — stable under {secs}s of sustained load\n");
    } else {
        println!("VERDICT: FAIL");
        for f in &fails {
            println!("  - {f}");
        }
        println!();
        panic!("soak failed: {}", fails.join("; "));
    }
}

#[test]
#[ignore = "long-running soak; run with --ignored (see file header for env knobs)"]
fn p99_soak() {
    main_test();
}
