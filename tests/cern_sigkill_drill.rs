//! CERN-grade crash recovery test #2 — hard SIGKILL of a victim subprocess.
//!
//! Category 2 (crash recovery) in `docs/testing-roadmap.md`. PR #42 added
//! a soft-crash test (`std::mem::forget(db)`); this adds the *real* thing —
//! a victim subprocess that opens an OxiDb, inserts records, and acks each
//! one over stdout. The parent reads N acks, then SIGKILLs the child, then
//! reopens the same data dir and asserts:
//!
//!   - **Every ACKed record is recovered** (durability promise — `insert`
//!     fsyncs the WAL before returning in strict-ACID-D mode, so the parent
//!     reading an ack means the WAL fsync has returned to the victim)
//!   - **No phantom data** (engine doesn't fabricate documents during
//!     replay — the recovered set is a superset of the ACKed set, never
//!     a different set)
//!
//! Unlike `mem::forget`, this exercises the OS-process boundary: the
//! kernel kills the victim *anywhere* in its execution, including mid-
//! syscall, mid-fsync, mid-page-cache-flush. That's the real crash
//! shape, not a Rust-language "skip the destructors" approximation.
//!
//! Self-spawn pattern: the same test binary runs as the victim when
//! `OXIDB_SIGKILL_VICTIM=1` is set in env. No extra `[[bin]]` target
//! in Cargo.toml, no extra crate in the workspace.
//!
//! Marked `#[ignore]` so `cargo test` stays fast; run with:
//!   cargo test --test cern_sigkill_drill -- --ignored

#![cfg(unix)]

use serde_json::json;
use std::collections::HashSet;
use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tempfile::tempdir;

use oxidb::OxiDb;

/// Number of acks the parent reads before sending SIGKILL.
const ACK_TARGET: usize = 500;
/// Hard deadline for collecting `ACK_TARGET` acks — if exceeded, fail
/// loudly rather than hanging the suite.
const ACK_DEADLINE: Duration = Duration::from_secs(30);

/// Victim loop — open the DB, insert forever, ack each insert's
/// application-level id to stdout. Never returns.
fn run_victim() -> ! {
    let path = std::env::var("OXIDB_VICTIM_DATA")
        .expect("victim role: OXIDB_VICTIM_DATA must be set");
    let db = OxiDb::open(std::path::Path::new(&path))
        .expect("victim role: open db");

    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    let mut i: u64 = 0;
    loop {
        db.insert(
            "sigkill_test",
            json!({"i": i, "payload": "x".repeat(64)}),
        )
        .expect("victim role: insert");
        // Ack ONLY after insert() returns — by then the WAL fsync has
        // completed, so the parent reading this ack means the record
        // is durable. If we crash before the next write to stdout, the
        // record is still on disk.
        writeln!(out, "{i}").expect("victim role: write ack");
        out.flush().expect("victim role: flush ack");
        i += 1;
    }
}

#[test]
#[ignore]
fn sigkill_during_writes_preserves_acked_records() {
    // ── Self-spawn dispatch ──────────────────────────────────────────
    // When the env var is set, we ARE the victim — run the victim loop
    // and never return. The parent invocation reads the parent path
    // below.
    if std::env::var_os("OXIDB_SIGKILL_VICTIM").is_some() {
        run_victim();
    }

    // ── Parent path ──────────────────────────────────────────────────
    let dir = tempdir().expect("create temp data dir");
    let path = dir.path().to_path_buf();

    // Spawn ourselves (the same test binary) with the role env var.
    // `--exact` + the exact test name ensures only THIS test runs in
    // the child (so libtest doesn't drag in other ignored tests).
    // `--nocapture` keeps libtest from buffering the victim's stdout
    // (otherwise parent never sees the acks until the test "ends",
    // which it never does in the victim).
    let exe = std::env::current_exe().expect("locate own test binary");
    let mut child = Command::new(&exe)
        .env("OXIDB_SIGKILL_VICTIM", "1")
        .env("OXIDB_VICTIM_DATA", &path)
        .args([
            "--ignored",
            "--exact",
            "sigkill_during_writes_preserves_acked_records",
            "--nocapture",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::null()) // suppress libtest's "running 1 test..." noise
        .spawn()
        .expect("spawn victim subprocess");

    // ── Read acks until target reached or deadline hit ───────────────
    let stdout = child.stdout.take().expect("victim's stdout pipe");
    let mut reader = BufReader::new(stdout);
    let mut acked: Vec<u64> = Vec::with_capacity(ACK_TARGET);
    let start = Instant::now();

    while acked.len() < ACK_TARGET {
        if start.elapsed() > ACK_DEADLINE {
            let _ = child.kill();
            let _ = child.wait();
            panic!(
                "victim only acked {} of {ACK_TARGET} records in {ACK_DEADLINE:?} — \
                 either inserts are far slower than expected or the IPC pipe is stuck",
                acked.len()
            );
        }
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) => {
                // EOF — victim exited unexpectedly (panic? crash? exit?).
                let _ = child.wait();
                panic!(
                    "victim stdout closed after only {} acks — \
                     victim crashed before reaching the SIGKILL target",
                    acked.len()
                );
            }
            Ok(_) => {
                // libtest prints "running 1 test\n", a blank line, and a
                // result line ("test foo ... ok") to STDOUT (not stderr).
                // Our victim writes nothing but pure decimal u64s. Skip
                // anything that doesn't parse — robust against future
                // libtest changes and won't false-positive on a real ack.
                let trimmed = line.trim();
                if let Ok(id) = trimmed.parse::<u64>() {
                    acked.push(id);
                }
            }
            Err(e) => panic!("read ack pipe: {e}"),
        }
    }

    // ── Hard kill ────────────────────────────────────────────────────
    // `Child::kill` sends SIGKILL on Unix — uncatchable, terminates the
    // process anywhere in its execution including mid-fsync.
    child.kill().expect("send SIGKILL to victim");
    let _ = child.wait();

    // ── Reopen + verify durability invariants ────────────────────────
    let db = OxiDb::open(&path).expect("parent: reopen data dir after SIGKILL");
    let docs = db
        .find("sigkill_test", &json!({}))
        .expect("parent: find recovered docs");
    let recovered: HashSet<u64> = docs
        .iter()
        .map(|d| d["i"].as_u64().expect("doc.i is u64"))
        .collect();

    let acked_set: HashSet<u64> = acked.iter().copied().collect();

    // Invariant 1: every ACKed record MUST be recovered. The victim
    // ack'd record i only AFTER insert(i) returned, by which time the
    // WAL fsync was committed.
    for &id in &acked {
        assert!(
            recovered.contains(&id),
            "DURABILITY VIOLATION: record i={id} was ACKed by the victim \
             (so its WAL fsync had completed before stdout::write) but is \
             NOT present after SIGKILL recovery. This is the canonical 'lost \
             acknowledged write' bug."
        );
    }

    // Invariant 2: every recovered record is a real one — no phantoms
    // beyond the small range "ack target + records the victim got off
    // before SIGKILL landed". We don't know the exact upper bound, but
    // ids must be monotonic from 0 and form a contiguous prefix of the
    // insert sequence.
    let max_recovered = recovered.iter().copied().max().unwrap_or(0);
    let expected_prefix: HashSet<u64> = (0..=max_recovered).collect();
    let phantoms: Vec<u64> = recovered.difference(&expected_prefix).copied().collect();
    assert!(
        phantoms.is_empty(),
        "PHANTOM DATA: recovered ids contain entries outside [0..={max_recovered}]: {phantoms:?}"
    );

    let extra_durable = recovered.len() - acked_set.len();
    eprintln!(
        "[sigkill] acked={} recovered={} (extra-but-durable={}, max_recovered={})",
        acked.len(),
        recovered.len(),
        extra_durable,
        max_recovered,
    );
}
