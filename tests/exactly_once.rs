//! Exactly-once / idempotency — the most common REAL exchange bug
//! class: a client submits a withdrawal, times out, and retries. Did
//! the first attempt commit? Without idempotency, that's a double
//! withdrawal.
//!
//! ## The protocol under test
//!
//! OxiDB's building block is a **unique index on a request id** plus
//! transactional atomicity: every money movement inserts a receipt
//! `{request_id}` in the SAME transaction as the balance updates.
//! A retry blindly re-attempts the whole transaction:
//!
//! - original committed  → retry aborts on the unique receipt
//!   (`Error::UniqueViolation`) → treated as "already applied";
//! - original never committed → retry applies; receipt + moves land
//!   atomically.
//!
//! Either way the effect is **exactly once** — no client-side "check
//! then apply" race, no double withdrawal. These tests pin that under
//! sequential retries, concurrent retry storms, and SIGKILL-at-any-
//! point crash/recovery (self-spawn victim pattern).
//!
//! Run with:
//!   cargo test --release --test exactly_once -- --ignored --nocapture

#![cfg(unix)]

use serde_json::json;
use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

use oxidb::{Error, OxiDb};

const AMOUNT: i64 = 10;

/// Attempt one transfer exactly as an exchange would: receipt insert +
/// two balance moves in one transaction. Returns:
/// Ok(true) = applied now; Ok(false) = already applied (unique receipt);
/// Err = conflict or other failure (caller may retry).
fn attempt_transfer(db: &OxiDb, request_id: &str, from: &str, to: &str) -> Result<bool, Error> {
    let tx = db.begin_transaction();
    let steps = (|| -> Result<(), Error> {
        db.tx_insert(tx, "receipts", json!({"request_id": request_id}))?;
        db.tx_update(
            tx,
            "accounts",
            &json!({"id": from}),
            &json!({"$inc": {"bal": -AMOUNT}}),
        )?;
        db.tx_update(
            tx,
            "accounts",
            &json!({"id": to}),
            &json!({"$inc": {"bal": AMOUNT}}),
        )?;
        Ok(())
    })();
    if let Err(e) = steps {
        db.rollback_transaction(tx).ok();
        return Err(e);
    }
    match db.commit_transaction(tx) {
        Ok(()) => Ok(true),
        Err(Error::UniqueViolation { .. }) => Ok(false), // already applied
        Err(e) => Err(e),
    }
}

/// Retry loop a client would run: keep attempting until the request is
/// definitively applied (now or previously).
fn transfer_idempotent(db: &OxiDb, request_id: &str, from: &str, to: &str) -> bool {
    for _ in 0..1000 {
        match attempt_transfer(db, request_id, from, to) {
            Ok(applied_now) => return applied_now,
            Err(Error::TransactionConflict { .. }) => continue,
            Err(e) => panic!("unexpected transfer error: {e}"),
        }
    }
    panic!("transfer did not settle within 1000 retries");
}

fn setup(db: &OxiDb) {
    db.insert("accounts", json!({"id": "a", "bal": 1000}))
        .unwrap();
    db.insert("accounts", json!({"id": "b", "bal": 0})).unwrap();
    db.create_index("accounts", "id").unwrap();
    db.create_unique_index("receipts", "request_id").unwrap();
}

fn balances(db: &OxiDb) -> (i64, i64) {
    let a = db
        .find_one("accounts", &json!({"id": "a"}))
        .unwrap()
        .unwrap()["bal"]
        .as_i64()
        .unwrap();
    let b = db
        .find_one("accounts", &json!({"id": "b"}))
        .unwrap()
        .unwrap()["bal"]
        .as_i64()
        .unwrap();
    (a, b)
}

fn receipt_count(db: &OxiDb, request_id: &str) -> usize {
    db.find("receipts", &json!({"request_id": request_id}))
        .unwrap()
        .len()
}

// ─────────────────────────────────────────────────────────────────────
// Sequential retry: the same request submitted three times applies once.
// ─────────────────────────────────────────────────────────────────────
#[test]
#[ignore]
fn sequential_retries_apply_exactly_once() {
    let dir = tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();
    setup(&db);

    assert!(
        transfer_idempotent(&db, "req-1", "a", "b"),
        "first attempt applies"
    );
    assert!(
        !transfer_idempotent(&db, "req-1", "a", "b"),
        "retry is a no-op"
    );
    assert!(
        !transfer_idempotent(&db, "req-1", "a", "b"),
        "second retry is a no-op"
    );

    assert_eq!(balances(&db), (1000 - AMOUNT, AMOUNT), "moved exactly once");
    assert_eq!(receipt_count(&db, "req-1"), 1, "exactly one receipt");
}

// ─────────────────────────────────────────────────────────────────────
// Concurrent retry storm: 8 threads race the SAME request id. Exactly
// one applies; the balance moves exactly once. This is the timeout-
// then-everybody-retries scenario.
// ─────────────────────────────────────────────────────────────────────
#[test]
#[ignore]
fn concurrent_duplicate_requests_apply_exactly_once() {
    let dir = tempdir().unwrap();
    let db = Arc::new(OxiDb::open(dir.path()).unwrap());
    setup(&db);

    for round in 0..20 {
        let request_id = format!("storm-{round}");
        let applied = Arc::new(AtomicUsize::new(0));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let db = Arc::clone(&db);
                let rid = request_id.clone();
                let applied = Arc::clone(&applied);
                thread::spawn(move || {
                    if transfer_idempotent(&db, &rid, "a", "b") {
                        applied.fetch_add(1, Ordering::SeqCst);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(
            applied.load(Ordering::SeqCst),
            1,
            "round {round}: exactly one of 8 concurrent duplicates may apply"
        );
        assert_eq!(receipt_count(&db, &request_id), 1, "round {round}");
    }

    let moved = 20 * AMOUNT;
    assert_eq!(
        balances(&db),
        (1000 - moved, moved),
        "20 requests × 8 duplicate submissions each = exactly 20 applications"
    );
}

// ─────────────────────────────────────────────────────────────────────
// Crash + retry: SIGKILL the process at a random moment around the
// transfer, recover, retry the same request id. Wherever the kill
// landed — before, during, or after the commit — the retry must settle
// the request to exactly one application. The round accumulates on one
// data dir, so recovery-of-recovery is exercised too.
//
// Self-spawn pattern: this binary re-runs as the victim with
// OXIDB_XONCE_VICTIM=1.
// ─────────────────────────────────────────────────────────────────────

fn run_victim() -> ! {
    let path = std::env::var("OXIDB_VICTIM_DATA").expect("victim: OXIDB_VICTIM_DATA");
    let rid = std::env::var("OXIDB_XONCE_RID").expect("victim: OXIDB_XONCE_RID");
    let db = OxiDb::open(std::path::Path::new(&path)).expect("victim: open");
    // Signal readiness so the parent can time its kill against the
    // actual attempt, then attempt the transfer in a tight retry loop
    // (a real client's behavior) until killed or applied.
    println!("READY");
    let applied = transfer_idempotent(&db, &rid, "a", "b");
    println!("SETTLED {applied}");
    // Keep the process alive so a "late" kill also lands post-commit.
    loop {
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
#[ignore]
fn crash_retry_applies_exactly_once() {
    if std::env::var("OXIDB_XONCE_VICTIM").as_deref() == Ok("1") {
        run_victim();
    }

    let rounds: usize = std::env::var("XONCE_ROUNDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(15);

    let dir = tempdir().unwrap();
    {
        let db = OxiDb::open(dir.path()).unwrap();
        setup(&db);
    }
    let exe = std::env::current_exe().unwrap();

    // Deterministic-ish kill-delay sweep: from "certainly before the
    // commit" to "certainly after", in sub-millisecond steps around the
    // commit latency, so kills land on every phase across rounds.
    for round in 0..rounds {
        let rid = format!("crash-{round}");
        let mut child = Command::new(&exe)
            .arg("--ignored")
            .arg("--nocapture")
            .arg("crash_retry_applies_exactly_once")
            .env("OXIDB_XONCE_VICTIM", "1")
            .env("OXIDB_VICTIM_DATA", dir.path())
            .env("OXIDB_XONCE_RID", &rid)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn victim");

        // Wait for READY, then kill after a round-dependent delay.
        let stdout = child.stdout.take().unwrap();
        let mut lines = BufReader::new(stdout).lines();
        loop {
            let line = lines.next().expect("victim died before READY").unwrap();
            if line.trim() == "READY" {
                break;
            }
        }
        let delay_us = (round as u64 % 15) * 700; // 0 .. ~10ms sweep
        thread::sleep(Duration::from_micros(delay_us));
        child.kill().expect("SIGKILL victim");
        child.wait().expect("reap victim");

        // Recover + retry from the parent (the client's retry after the
        // "server" vanished). Must settle to exactly one application.
        let db = OxiDb::open(dir.path()).unwrap();
        transfer_idempotent(&db, &rid, "a", "b");
        assert_eq!(
            receipt_count(&db, &rid),
            1,
            "round {round}: exactly one receipt after crash+retry"
        );
        let moved = (round as i64 + 1) * AMOUNT;
        assert_eq!(
            balances(&db),
            (1000 - moved, moved),
            "round {round}: balance reflects exactly {} applications",
            round + 1
        );
        drop(db);
    }
    println!("crash+retry: {rounds} rounds, every request applied exactly once");
}
