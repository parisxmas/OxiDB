//! CERN-grade scale test (category 7 in `docs/testing-roadmap.md`).
//!
//! HEP workloads are characterised by **bursty ingest + long-range
//! scans + high-fanout point reads**. This test models all three over
//! a tunable corpus size (default 100K docs, env-var cranks it up to
//! the full 10⁹ aspiration when run on capable hardware).
//!
//! What it asserts:
//!
//!   - **Throughput floors.** Insert rate ≥ MIN_INSERT_DPS. Catches a
//!     100× regression that would otherwise hide in a "passes
//!     eventually" green run.
//!   - **Index sub-linearity.** Index-backed point queries must
//!     complete in *less than half* the time a full scan takes for
//!     the same predicate (loose O(log n) bound). Tighter bounds
//!     belong to per-index benchmarks; this just catches "index
//!     accidentally degraded to scan" bugs.
//!   - **Aggregation correctness at scale.** sum() over the full
//!     corpus matches the closed-form expected value.
//!   - **Steady-state count.** After insert + delete-batch, the live
//!     doc count is exactly the expected residual — no silent loss,
//!     no double-counting.
//!
//! Knobs (env vars):
//!   OXIDB_SCALE_DOCS         total document count (default: 100_000)
//!   OXIDB_SCALE_INSERT_BATCH per-batch insert size  (default: 1_000)
//!   OXIDB_MIN_INSERT_DPS     insert-rate floor docs/sec (default: 50_000)
//!
//! Inserts are wrapped in **one transaction per batch** — that's how
//! real bulk loaders work (and the documented 271× speedup vs auto-
//! commit per `julia-dbinterface-deferred` memory). The throughput
//! floor reflects this; if you must measure auto-commit fsync-per-
//! insert, set OXIDB_MIN_INSERT_DPS to a lower bound (~250 dps on
//! macOS APFS) and lift the OXIDB_SCALE_DOCS knob.
//!
//! Marked `#[ignore]` so default `cargo test` stays fast. Run with:
//!   cargo test --test cern_scale -- --ignored --nocapture
//!
//! Larger run:
//!   OXIDB_SCALE_DOCS=1000000 cargo test --test cern_scale -- --ignored --nocapture

use serde_json::json;
use std::time::Instant;
use tempfile::tempdir;

use oxidb::OxiDb;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

#[test]
#[ignore]
fn hep_workload_scale_invariants_hold() {
    let total_docs = env_usize("OXIDB_SCALE_DOCS", 100_000);
    let insert_batch = env_usize("OXIDB_SCALE_INSERT_BATCH", 1_000);
    let min_insert_dps = env_usize("OXIDB_MIN_INSERT_DPS", 50_000);

    eprintln!("[scale] total_docs={total_docs} insert_batch={insert_batch} min_dps={min_insert_dps}");

    let dir = tempdir().expect("data dir");
    let db = OxiDb::open(dir.path()).expect("open");

    // ── Bursty ingest ─────────────────────────────────────────────────
    // Insert in batches — models the HEP "drop a beam-fill worth of
    // events" pattern rather than steady drip. Each doc has a few
    // fields of representative shape (one int we'll index on, one
    // string, one nested object).
    let insert_t0 = Instant::now();
    for batch_start in (0..total_docs).step_by(insert_batch) {
        let batch_end = (batch_start + insert_batch).min(total_docs);
        // One transaction per batch → one fsync per batch, not per doc.
        // This is how production bulk loaders (and the documented
        // Julia 271× speedup) actually run.
        let tx = db.begin_transaction();
        for i in batch_start..batch_end {
            db.tx_insert(
                tx,
                "events",
                json!({
                    "id": i as i64,
                    "energy": (i % 1000) as i64,
                    "detector": format!("det_{}", i % 7),
                    "track": {
                        "px": (i % 100) as i64 - 50,
                        "py": (i % 73)  as i64 - 36,
                    },
                }),
            )
            .expect("tx_insert");
        }
        db.commit_transaction(tx).expect("commit batch");
    }
    let insert_elapsed = insert_t0.elapsed();
    let insert_dps = (total_docs as f64) / insert_elapsed.as_secs_f64();
    eprintln!(
        "[scale] insert: {total_docs} docs in {insert_elapsed:?} ({insert_dps:.0} docs/sec)"
    );

    assert!(
        insert_dps >= min_insert_dps as f64,
        "INSERT THROUGHPUT FLOOR — got {insert_dps:.0} docs/sec, expected ≥ {min_insert_dps} \
         (set OXIDB_MIN_INSERT_DPS to lower on slow CI runners, but a 5k-doc/sec floor \
         catches the canonical 'someone added an O(n) hot-path op' regression)"
    );

    // ── Build index after the load ────────────────────────────────────
    let idx_t0 = Instant::now();
    db.create_index("events", "energy").expect("create index");
    eprintln!("[scale] indexed `energy` in {:?}", idx_t0.elapsed());

    // ── Index sub-linearity check ────────────────────────────────────
    // Pick a narrow predicate matching ~0.1% of docs. With an index,
    // resolution should be O(log n + result_size). Without, it'd
    // walk the whole corpus.
    let narrow_query = json!({ "energy": 42 });
    let expected_count = total_docs / 1000; // rough — exact below

    let idx_query_t0 = Instant::now();
    let idx_hits = db.find("events", &narrow_query).expect("index query");
    let idx_elapsed = idx_query_t0.elapsed();
    eprintln!(
        "[scale] index query `energy=42` returned {} docs in {idx_elapsed:?}",
        idx_hits.len()
    );

    // Compare against a FULL SCAN for the same predicate to bound the
    // index advantage. We can't easily force a scan via the public
    // API, so we use a predicate the engine can't index ($mod), which
    // CLAUDE.md notes is "post-filter only (no index)".
    let scan_query = json!({ "energy": { "$mod": [1000, 42] } });
    let scan_query_t0 = Instant::now();
    let scan_hits = db.find("events", &scan_query).expect("scan query");
    let scan_elapsed = scan_query_t0.elapsed();
    eprintln!(
        "[scale] scan query `energy % 1000 == 42` returned {} docs in {scan_elapsed:?}",
        scan_hits.len()
    );

    // Both queries return the same docs (modulo logic).
    assert_eq!(idx_hits.len(), scan_hits.len(), "index vs scan disagreed on result size");

    // Sub-linearity: index must be meaningfully faster than scan.
    // We bound at 2× rather than asserting strict orders-of-magnitude
    // — at 100k docs the constant factors dominate; the real signal
    // emerges at higher OXIDB_SCALE_DOCS. The 2× floor reliably
    // catches "index disappeared from the plan" regressions.
    let idx_us = idx_elapsed.as_micros().max(1) as f64;
    let scan_us = scan_elapsed.as_micros().max(1) as f64;
    let speedup = scan_us / idx_us;
    eprintln!("[scale] index speedup over scan: {speedup:.1}×");
    assert!(
        speedup >= 2.0,
        "INDEX SUB-LINEARITY — index query was only {speedup:.1}× faster than \
         full scan; expected ≥ 2× (regression: index may have been deoptimised \
         or the predicate stopped using it)"
    );

    // ── Aggregation correctness at scale ─────────────────────────────
    let agg_t0 = Instant::now();
    let agg = db
        .aggregate(
            "events",
            &json!([{
                "$group": { "_id": null, "total": { "$sum": "$energy" } }
            }]),
        )
        .expect("aggregate");
    let agg_elapsed = agg_t0.elapsed();
    let total = agg[0]["total"].as_i64().unwrap();
    let expected_total: i64 = (0..total_docs).map(|i| (i % 1000) as i64).sum();
    eprintln!(
        "[scale] aggregation: sum(energy) = {total} in {agg_elapsed:?} (expected {expected_total})"
    );
    assert_eq!(
        total, expected_total,
        "AGGREGATION CORRECTNESS — sum diverged at scale, indicating either \
         a precision bug or a missed document"
    );

    // ── Steady-state count after delete-batch ────────────────────────
    let delete_t0 = Instant::now();
    let to_delete = total_docs / 10;
    let deleted = db
        .delete("events", &json!({ "id": { "$lt": to_delete as i64 } }))
        .expect("delete");
    let delete_elapsed = delete_t0.elapsed();
    eprintln!(
        "[scale] delete {deleted} docs in {delete_elapsed:?} ({:.0} docs/sec)",
        (deleted as f64) / delete_elapsed.as_secs_f64()
    );
    assert_eq!(
        deleted as usize, to_delete,
        "delete count must match predicate-matching docs"
    );

    let remaining = db.find("events", &json!({})).expect("final count").len();
    let expected_remaining = total_docs - to_delete;
    assert_eq!(
        remaining, expected_remaining,
        "STEADY-STATE COUNT — after deleting {to_delete} we expected {expected_remaining} \
         residual docs, got {remaining}"
    );

    eprintln!("[scale] DONE  all invariants held");
}
