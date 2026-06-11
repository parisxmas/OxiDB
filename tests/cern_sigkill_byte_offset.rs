//! CERN-grade crash recovery test #3 — byte-offset SIGKILL matrix.
//!
//! Category 2 (crash recovery) in `docs/testing-roadmap.md`. PR #43 added
//! the basic SIGKILL drill (kill after N completed ACKs). This adds the
//! follow-up tightener: instead of waiting for the engine to *complete*
//! N inserts, we sleep for a **varying short delay** (100µs → 200ms)
//! after spawn, then SIGKILL.
//!
//! Each delay in the grid lands the kill at a different point in the
//! engine's write trajectory — sometimes before the first `db.insert()`
//! returns, sometimes deep inside `Storage::append`, sometimes after
//! dozens of fsyncs. Across the matrix we exercise:
//!
//!   - DB::open being interrupted mid-initialisation (100µs delay
//!     usually hits this — the engine hasn't even written its first
//!     header byte yet)
//!   - Mid-WAL-append kills (the engine is halfway through writing the
//!     [tx_id|crc|len|payload] tuple)
//!   - Mid-fsync kills (kernel completes the syscall, then the process
//!     dies — fsync is uninterruptible, but the *next* line of Rust
//!     code never runs)
//!   - Late kills (many inserts already complete, plenty of WAL to
//!     replay)
//!
//! Same two invariants as PR #43, checked at **every** delay:
//!
//!   Invariant 1 (durability): every ACK the parent observed before
//!   the kill MUST correspond to a recovered record. Acks are only
//!   written by the victim *after* `db.insert()` returns, by which
//!   time the WAL fsync has completed.
//!
//!   Invariant 2 (no phantoms): recovered ids form a contiguous
//!   prefix [0..=max_recovered]. Replay never fabricates documents.
//!
//! Also asserts that **reopen succeeds at every delay** — even a kill
//! mid-init must leave the data dir in a recoverable state.
//!
//! Marked `#[ignore]` so `cargo test` stays fast; run with:
//!   cargo test --test cern_sigkill_byte_offset -- --ignored --nocapture

#![cfg(unix)]

use serde_json::json;
use std::collections::HashSet;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

use oxidb::OxiDb;

const ROLE_ENV: &str = "OXIDB_SIGKILL_OFFSET_VICTIM";
const DATA_ENV: &str = "OXIDB_VICTIM_DATA";

/// Delay grid in microseconds. Hand-chosen to span:
///   - tens of µs (engine likely still inside DB::open)
///   - sub-millisecond (first insert might be mid-WAL-write)
///   - low ms (a handful of inserts complete)
///   - tens of ms (dozens of inserts, several fsyncs)
///   - 200ms (well into steady state)
///
/// Geometrically spaced so we don't oversample one regime.
const DELAYS_US: &[u64] = &[100, 500, 1_000, 5_000, 10_000, 50_000, 200_000];

/// Victim loop — identical contract to PR #43's drill: insert, ack,
/// repeat. The ack is written ONLY after `insert()` returns, so any
/// ack the parent reads means that record's WAL fsync committed
/// before the victim died.
fn run_victim() -> ! {
    let path = std::env::var(DATA_ENV).expect("victim: data env");
    let db = OxiDb::open(std::path::Path::new(&path)).expect("victim: open");

    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    let mut i: u64 = 0;
    loop {
        db.insert("byteoff", json!({"i": i}))
            .expect("victim: insert");
        writeln!(out, "{i}").expect("victim: ack");
        out.flush().expect("victim: flush");
        i += 1;
    }
}

#[test]
#[ignore]
fn sigkill_at_varying_offsets_preserves_invariants() {
    // Role dispatch — when set, become the victim and never return.
    if std::env::var_os(ROLE_ENV).is_some() {
        run_victim();
    }

    let exe = std::env::current_exe().expect("locate own test binary");
    let mut summary: Vec<(u64, usize, usize, bool)> = Vec::new();

    for &delay_us in DELAYS_US {
        let dir = tempdir().expect("create temp data dir");
        let path: PathBuf = dir.path().to_path_buf();

        let mut child = Command::new(&exe)
            .env(ROLE_ENV, "1")
            .env(DATA_ENV, &path)
            .args([
                "--ignored",
                "--exact",
                "sigkill_at_varying_offsets_preserves_invariants",
                "--nocapture",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn victim");

        // Sleep the configured offset, then deliver SIGKILL. The
        // kernel completes any in-progress fsync syscall before
        // letting the process die — but the next userspace
        // instruction never runs. That's the bug surface we want.
        thread::sleep(Duration::from_micros(delay_us));
        child.kill().expect("send SIGKILL");
        let _ = child.wait();

        // Drain whatever the victim got into the pipe before dying.
        // Kernel pipe buffer is flushed-from-victim's-POV as soon as
        // `out.flush()` returned, so SIGKILL doesn't lose anything
        // already in the buffer; we just read until EOF.
        let stdout = child.stdout.take().expect("victim's stdout pipe");
        let mut acked: Vec<u64> = Vec::new();
        for line in BufReader::new(stdout).lines() {
            let line = match line {
                Ok(l) => l,
                Err(_) => break,
            };
            if let Ok(id) = line.trim().parse::<u64>() {
                acked.push(id);
            }
        }

        // Reopen — at every delay, this must succeed. A kill mid-init
        // that leaves the data dir unopenable is a real bug worth
        // surfacing, not a "soft" expected failure.
        let db = OxiDb::open(&path).unwrap_or_else(|e| {
            panic!(
                "delay={delay_us}µs: REOPEN FAILED — data dir left unrecoverable \
                 by SIGKILL during initialization. error: {e}"
            )
        });
        let docs = db
            .find("byteoff", &json!({}))
            .expect("delay={delay_us}µs: find after reopen");
        let recovered: HashSet<u64> = docs
            .iter()
            .map(|d| d["i"].as_u64().expect("doc.i is u64"))
            .collect();

        // Invariant 1: ack ⊆ recovered
        for &id in &acked {
            assert!(
                recovered.contains(&id),
                "delay={delay_us}µs: DURABILITY VIOLATION — \
                 ACKed record {id} (insert returned, WAL fsync had committed) \
                 missing from recovered set after SIGKILL"
            );
        }

        // Invariant 2: recovered is a contiguous prefix [0..=max]
        let max_recovered = recovered.iter().copied().max();
        let no_init = recovered.is_empty();
        if let Some(max) = max_recovered {
            let expected: HashSet<u64> = (0..=max).collect();
            let phantoms: Vec<u64> = recovered.difference(&expected).copied().collect();
            assert!(
                phantoms.is_empty(),
                "delay={delay_us}µs: PHANTOM DATA — recovered ids contain \
                 entries outside [0..={max}]: {phantoms:?}"
            );
        }

        summary.push((delay_us, acked.len(), recovered.len(), no_init));
    }

    // Pretty matrix summary — gets printed on the eprintln stream so
    // `--nocapture` users see it but it doesn't pollute the libtest
    // result line.
    eprintln!();
    eprintln!("[byte-offset SIGKILL matrix — all invariants held]");
    eprintln!(
        "  {:>10}  {:>10}  {:>10}  {:>10}",
        "delay_us", "acked", "recovered", "extra"
    );
    eprintln!("  {:->10}  {:->10}  {:->10}  {:->10}", "", "", "", "");
    for (d, a, r, no_init) in &summary {
        let extra = (*r as i64) - (*a as i64);
        let extra_str = if *no_init {
            "(no init)".to_string()
        } else {
            format!("{extra:+}")
        };
        eprintln!("  {d:>10}  {a:>10}  {r:>10}  {extra_str:>10}");
    }
}
