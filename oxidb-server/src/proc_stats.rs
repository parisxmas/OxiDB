// Process self-metrics for the running oxidb-server. Reads /proc/self
// directly — no external dependencies, container-friendly. CPU% is a
// delta over the time between calls, so the first poll always returns
// 0.0 and subsequent polls report the average since the previous one.

use std::fs;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::time::Instant;

use serde_json::{json, Value};

/// Process-wide singleton. Cheap to construct, no work happens until
/// `snapshot()` is first called.
pub static PROC_STATS: LazyLock<ProcStats> = LazyLock::new(ProcStats::new);

pub struct ProcStats {
    started_at: Instant,
    last: Mutex<Option<Sample>>,
}

struct Sample {
    when: Instant,
    cpu_total_ticks: u64,
}

impl ProcStats {
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),
            last: Mutex::new(None),
        }
    }

    /// Returns a snapshot suitable for serialization to clients.
    /// Shape: `{cpu_percent, mem_rss_mb, threads, uptime_s}`.
    pub fn snapshot(&self) -> Value {
        let now = Instant::now();
        let cpu_total_ticks = read_cpu_ticks().unwrap_or(0);
        let mem_rss_kb = read_vm_rss_kb().unwrap_or(0);
        let threads = read_threads().unwrap_or(0);

        let cpu_percent = {
            let mut last = self.last.lock().expect("ProcStats mutex poisoned");
            let pct = match last.as_ref() {
                Some(prev) => {
                    let dt_secs = now.duration_since(prev.when).as_secs_f64();
                    if dt_secs <= 0.0 || cpu_total_ticks < prev.cpu_total_ticks {
                        0.0
                    } else {
                        let dt_ticks = (cpu_total_ticks - prev.cpu_total_ticks) as f64;
                        (dt_ticks / clk_tck() / dt_secs) * 100.0
                    }
                }
                // First call: nothing to delta against, report 0.
                None => 0.0,
            };
            *last = Some(Sample {
                when: now,
                cpu_total_ticks,
            });
            pct
        };

        json!({
            "cpu_percent": round1(cpu_percent),
            "mem_rss_mb": round1(mem_rss_kb as f64 / 1024.0),
            "threads": threads,
            "uptime_s": now.duration_since(self.started_at).as_secs(),
        })
    }
}

fn round1(x: f64) -> f64 {
    (x * 10.0).round() / 10.0
}

#[cfg(target_os = "linux")]
fn clk_tck() -> f64 {
    // _SC_CLK_TCK is the kernel's HZ — utime/stime in /proc/<pid>/stat
    // are reported in these ticks. Universally 100 on x86_64/aarch64
    // distros, but read it anyway in case of an unusual kernel.
    let v = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    if v > 0 { v as f64 } else { 100.0 }
}

#[cfg(not(target_os = "linux"))]
fn clk_tck() -> f64 { 100.0 }

#[cfg(target_os = "linux")]
fn read_cpu_ticks() -> Option<u64> {
    let s = fs::read_to_string("/proc/self/stat").ok()?;
    // Format: "pid (comm) state ppid ... utime stime ...". The comm
    // field can contain spaces and parens, so use the LAST ')' to mark
    // its end and tokenize what follows. After comm, indexes are
    // 0-based: state=0, ppid=1, ..., utime=11, stime=12.
    let close = s.rfind(')')?;
    let after = s.get(close + 2..)?;
    let fields: Vec<&str> = after.split_whitespace().collect();
    let utime: u64 = fields.get(11)?.parse().ok()?;
    let stime: u64 = fields.get(12)?.parse().ok()?;
    Some(utime + stime)
}

#[cfg(target_os = "linux")]
fn read_vm_rss_kb() -> Option<u64> {
    let s = fs::read_to_string("/proc/self/status").ok()?;
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            return rest.trim().split_whitespace().next()?.parse().ok();
        }
    }
    None
}

#[cfg(target_os = "linux")]
fn read_threads() -> Option<u32> {
    let s = fs::read_to_string("/proc/self/status").ok()?;
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("Threads:") {
            return rest.trim().split_whitespace().next()?.parse().ok();
        }
    }
    None
}

// Non-Linux fallbacks — server is shipped as linux/amd64 only, but
// keeping these lets `cargo check` pass on macOS dev boxes.
#[cfg(not(target_os = "linux"))]
fn read_cpu_ticks() -> Option<u64> { None }

#[cfg(not(target_os = "linux"))]
fn read_vm_rss_kb() -> Option<u64> { None }

#[cfg(not(target_os = "linux"))]
fn read_threads() -> Option<u32> { None }
