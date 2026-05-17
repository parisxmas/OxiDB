//! CERN-grade soak test — sustained insert/read/update/delete loop with
//! invariant checks at the end.
//!
//! Category 3 (performance & long-tail) in `docs/testing-roadmap.md`.
//! Catches the "slow degradation" / "runaway growth" / "silent loss"
//! classes of bug that only surface after enough operations.
//!
//! Knobs (env vars):
//!   OXIDB_SOAK_SECS         total runtime in seconds (default: 30)
//!   OXIDB_SOAK_INSERT_BATCH inserts per outer iteration (default: 50)
//!
//! Marked `#[ignore]` so `cargo test` stays fast; run with:
//!   cargo test --test cern_soak -- --ignored
//!
//! What this does NOT yet test:
//!   - RSS leak (needs platform-specific /proc/self/status or mach_task_basic_info)
//!   - fd leak (needs /proc/self/fd or equivalent)
//!   - Unbounded WAL growth (needs to read the .wal file size and assert bound)
//! Each is a small follow-up; deliberately out of scope to keep the
//! harness portable across Linux/macOS without conditional compilation.

use serde_json::json;
use std::time::{Duration, Instant};
use tempfile::tempdir;

use oxidb::OxiDb;

fn env_secs(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

#[test]
#[ignore]
fn soak_keeps_doc_count_bounded() {
    let secs = env_secs("OXIDB_SOAK_SECS", 30);
    let batch = env_usize("OXIDB_SOAK_INSERT_BATCH", 50);

    eprintln!("[soak] running for {secs}s with batch={batch}");
    let dir = tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();

    // Steady-state strategy: each outer iteration inserts a batch, then
    // deletes the oldest batch — count should oscillate around `batch`,
    // never grow unbounded. Updates are also exercised so the WAL sees
    // a mix of op-types.
    let start = Instant::now();
    let deadline = start + Duration::from_secs(secs);

    let mut iter: u64 = 0;
    let mut inserted: u64 = 0;
    let mut deleted: u64 = 0;
    let mut updated: u64 = 0;
    let mut last_progress_print = Instant::now();

    while Instant::now() < deadline {
        let base = iter * batch as u64;

        // Insert a batch.
        for k in 0..batch {
            let id_field = base + k as u64;
            db.insert("soak", json!({"id": id_field, "iter": iter, "payload": "p"}))
                .unwrap();
            inserted += 1;
        }

        // Update the most recently inserted batch (every doc gets `$inc: hits++`).
        let q = json!({ "iter": iter });
        let u = json!({ "$inc": { "hits": 1 } });
        let n = db.update("soak", &q, &u).unwrap();
        updated += n;

        // After the second outer iteration, delete the oldest in-flight batch.
        // This keeps the steady-state count around `batch`.
        if iter >= 2 {
            let old_iter = iter - 2;
            let n = db.delete("soak", &json!({ "iter": old_iter })).unwrap();
            deleted += n;
        }

        iter += 1;

        if last_progress_print.elapsed() >= Duration::from_secs(5) {
            let elapsed = start.elapsed().as_secs();
            let live = db.find("soak", &json!({})).unwrap().len();
            eprintln!(
                "[soak] t={elapsed:>3}s iter={iter} inserted={inserted} updated={updated} \
                 deleted={deleted} live={live}"
            );
            last_progress_print = Instant::now();
        }
    }

    // Final invariants.
    let live = db.find("soak", &json!({})).unwrap().len();
    let expected_live_range = 0..=(batch * 3);
    assert!(
        expected_live_range.contains(&live),
        "live doc count {live} outside expected steady-state band {:?} — \
         insert/delete ratio is drifting (inserted={inserted} deleted={deleted})",
        expected_live_range
    );

    // Sanity: nontrivial amount of work actually happened (catches a
    // hung loop pretending to be done).
    assert!(
        inserted >= 10,
        "soak inserted only {inserted} records in {secs}s — engine looks stalled"
    );

    eprintln!(
        "[soak] DONE  iter={iter} inserted={inserted} updated={updated} deleted={deleted} live={live}"
    );
}
