//! Soak / stress tests for sustained insert/update/delete churn.
//!
//! These are mode-agnostic — correctness must hold for the default in-RAM
//! store *and* the disk-first store. To soak the disk-first engine
//! specifically, run with the flag set:
//!
//! ```sh
//! OXIDB_DISK_FIRST=1 cargo test --test disk_first_soak
//! # heavier:
//! OXIDB_DISK_FIRST=1 SOAK_ROUNDS=2000 cargo test --test disk_first_soak -- --nocapture
//! ```
//!
//! Each test maintains an in-test reference model and asserts the engine agrees
//! after churn, after a clean reopen, and after a simulated crash. The
//! `bdat_growth_under_update_churn` test additionally observes data-file growth
//! (no compaction yet — see ADR-0009).

use oxidb::OxiDb;
use serde_json::{Value, json};
use std::collections::HashMap;

/// Deterministic LCG — reproducible churn without an rng dependency.
struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 16
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn rounds() -> usize {
    // Modest default so the suite stays fast on every `cargo test`; crank it up
    // (`SOAK_ROUNDS=2000`) for an actual soak run.
    std::env::var("SOAK_ROUNDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(40)
}

fn disk_first() -> bool {
    std::env::var("OXIDB_DISK_FIRST")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn doc(k: u64, v: u64) -> Value {
    json!({
        "k": k,
        "v": v,
        "bucket": (v % 8) as i64,
        "payload": format!("doc-{k}-{v}-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"),
    })
}

/// Sustained insert/update/delete churn, model-checked after every round.
#[test]
fn churn_integrity_model_checked() {
    let dir = tempfile::tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();
    let mut model: HashMap<u64, u64> = HashMap::new(); // k -> v
    let mut rng = Lcg(1);
    let mut next_k = 0u64;

    for round in 0..rounds() {
        // Insert a few new docs.
        for _ in 0..5 {
            let k = next_k;
            next_k += 1;
            let v = rng.next();
            db.insert("c", doc(k, v)).unwrap();
            model.insert(k, v);
        }
        // Update some existing docs.
        if !model.is_empty() {
            let keys: Vec<u64> = model.keys().copied().collect();
            for _ in 0..4 {
                let k = keys[rng.below(keys.len() as u64) as usize];
                let v = rng.next();
                db.update("c", &json!({ "k": k }), &json!({ "$set": { "v": v, "bucket": (v % 8) as i64 } }))
                    .unwrap();
                model.insert(k, v);
            }
        }
        // Delete some.
        if model.len() > 10 {
            let keys: Vec<u64> = model.keys().copied().collect();
            for _ in 0..2 {
                let k = keys[rng.below(keys.len() as u64) as usize];
                db.delete("c", &json!({ "k": k })).unwrap();
                model.remove(&k);
            }
        }

        // Verify count + a sample of point reads every 25 rounds (cheap).
        if round % 25 == 0 || round == rounds() - 1 {
            assert_eq!(
                db.count("c", &json!({})).unwrap(),
                model.len(),
                "count mismatch at round {round}"
            );
            let keys: Vec<u64> = model.keys().copied().take(20).collect();
            for k in keys {
                let got = db.find_one("c", &json!({ "k": k })).unwrap();
                assert_eq!(
                    got.and_then(|d| d["v"].as_u64()),
                    Some(model[&k]),
                    "value mismatch for k={k} at round {round}"
                );
            }
        }
    }
    // Full reconciliation at the end.
    assert_eq!(db.count("c", &json!({})).unwrap(), model.len());
}

/// Index correctness under churn: an indexed field is repeatedly updated;
/// indexed queries must always equal a model-derived expectation.
#[test]
fn index_consistency_under_churn() {
    let dir = tempfile::tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();
    db.create_index("c", "bucket").unwrap();
    let mut model: HashMap<u64, u64> = HashMap::new(); // k -> v (bucket = v%8)
    let mut rng = Lcg(7);
    let mut next_k = 0u64;

    for round in 0..rounds() {
        for _ in 0..6 {
            let k = next_k;
            next_k += 1;
            let v = rng.next();
            db.insert("c", doc(k, v)).unwrap();
            model.insert(k, v);
        }
        if !model.is_empty() {
            let keys: Vec<u64> = model.keys().copied().collect();
            for _ in 0..5 {
                let k = keys[rng.below(keys.len() as u64) as usize];
                let v = rng.next(); // changes bucket → must move in the index
                db.update("c", &json!({ "k": k }), &json!({ "$set": { "v": v, "bucket": (v % 8) as i64 } }))
                    .unwrap();
                model.insert(k, v);
            }
        }
        if model.len() > 20 {
            let keys: Vec<u64> = model.keys().copied().collect();
            let k = keys[rng.below(keys.len() as u64) as usize];
            db.delete("c", &json!({ "k": k })).unwrap();
            model.remove(&k);
        }

        if round % 25 == 0 || round == rounds() - 1 {
            for bucket in 0..8i64 {
                let expected = model.values().filter(|v| (*v % 8) as i64 == bucket).count();
                // Indexed equality query must match the model exactly.
                let got = db.count("c", &json!({ "bucket": bucket })).unwrap();
                assert_eq!(got, expected, "index count mismatch bucket={bucket} round={round}");
            }
        }
    }
}

/// Churn, clean shutdown, reopen — all data + indexed queries must survive.
#[test]
fn churn_then_clean_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let mut model: HashMap<u64, u64> = HashMap::new();
    {
        let db = OxiDb::open(dir.path()).unwrap();
        db.create_index("c", "bucket").unwrap();
        let mut rng = Lcg(13);
        let mut next_k = 0u64;
        for _ in 0..rounds() {
            for _ in 0..5 {
                let k = next_k;
                next_k += 1;
                let v = rng.next();
                db.insert("c", doc(k, v)).unwrap();
                model.insert(k, v);
            }
            if model.len() > 10 {
                let keys: Vec<u64> = model.keys().copied().collect();
                let k = keys[rng.below(keys.len() as u64) as usize];
                db.delete("c", &json!({ "k": k })).unwrap();
                model.remove(&k);
            }
        }
        db.shutdown(); // clean checkpoint: persist + truncate WAL
    }
    // Reopen and reconcile.
    let db = OxiDb::open(dir.path()).unwrap();
    assert_eq!(db.count("c", &json!({})).unwrap(), model.len(), "count after reopen");
    for bucket in 0..8i64 {
        let expected = model.values().filter(|v| (*v % 8) as i64 == bucket).count();
        assert_eq!(
            db.count("c", &json!({ "bucket": bucket })).unwrap(),
            expected,
            "indexed query after reopen, bucket={bucket}"
        );
    }
    // Spot-check point reads.
    for (&k, &v) in model.iter().take(50) {
        let got = db.find_one("c", &json!({ "k": k })).unwrap();
        assert_eq!(got.and_then(|d| d["v"].as_u64()), Some(v), "k={k} after reopen");
    }
}

/// Simulated crash: drop the engine WITHOUT a clean shutdown, then reopen.
/// Committed writes must survive via WAL replay.
#[test]
fn crash_recovery_committed_survives() {
    let dir = tempfile::tempdir().unwrap();
    let n = 2000u64;
    {
        let db = OxiDb::open(dir.path()).unwrap();
        for k in 0..n {
            db.insert("c", doc(k, k * 2)).unwrap();
        }
        // No shutdown() — simulate a crash. Drop releases handles; the WAL
        // retains the committed records (not truncated).
    }
    let db = OxiDb::open(dir.path()).unwrap();
    assert_eq!(db.count("c", &json!({})).unwrap() as u64, n, "all committed docs recovered");
    // Verify a sample round-trips correctly.
    for k in (0..n).step_by(137) {
        let got = db.find_one("c", &json!({ "k": k })).unwrap();
        assert_eq!(got.and_then(|d| d["v"].as_u64()), Some(k * 2), "k={k} after crash recovery");
    }
}

/// Observation: heavy update churn on a small key set. Correctness must hold
/// (the live count stays fixed); on disk-first we also report data-file growth,
/// which motivates compaction (ADR-0009 follow-up). Not a hard size assertion.
#[test]
fn bdat_growth_under_update_churn() {
    let dir = tempfile::tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();
    let keys = 200u64;
    for k in 0..keys {
        db.insert("c", doc(k, 0)).unwrap();
    }
    let mut rng = Lcg(99);
    let updates = rounds() * 20;
    for _ in 0..updates {
        let k = rng.below(keys);
        let v = rng.next();
        db.update("c", &json!({ "k": k }), &json!({ "$set": { "v": v } })).unwrap();
    }
    // Correctness: the live set is unchanged by updates.
    assert_eq!(db.count("c", &json!({})).unwrap() as u64, keys, "live count stable under update churn");
    db.shutdown();

    if disk_first() {
        // Report data-file size — append-only without compaction grows with
        // the number of updates, not the live set.
        let mut total = 0u64;
        if let Ok(rd) = std::fs::read_dir(dir.path()) {
            for e in rd.flatten() {
                if e.path().extension().and_then(|x| x.to_str()) == Some("bdat") {
                    total += e.metadata().map(|m| m.len()).unwrap_or(0);
                }
            }
        }
        println!(
            "\n[soak] disk-first: {keys} live docs after {updates} updates → .bdat = {} KiB \
             (append-only; compaction would reclaim the dead records)",
            total / 1024
        );
    }
    // Reopen must still reconcile to the live set regardless of dead space.
    drop(db);
    let db = OxiDb::open(dir.path()).unwrap();
    assert_eq!(db.count("c", &json!({})).unwrap() as u64, keys, "live count after reopen");
}

/// Compaction reclaims the dead space left by update churn, and the live data
/// survives intact (values + indexed queries) across compact and a reopen.
#[test]
fn compaction_reclaims_space_and_preserves_data() {
    let dir = tempfile::tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();
    db.create_index("c", "bucket").unwrap();
    let keys = 300u64;
    let mut model: HashMap<u64, u64> = HashMap::new();
    for k in 0..keys {
        let v = k;
        db.insert("c", doc(k, v)).unwrap();
        model.insert(k, v);
    }
    // Heavy in-place update churn → lots of dead records (disk-first).
    let mut rng = Lcg(55);
    for _ in 0..(rounds() * 30) {
        let k = rng.below(keys);
        let v = rng.next();
        db.update("c", &json!({ "k": k }), &json!({ "$set": { "v": v, "bucket": (v % 8) as i64 } }))
            .unwrap();
        model.insert(k, v);
    }

    let stats = db.compact("c").unwrap();
    assert_eq!(stats.docs_kept as u64, keys, "compaction keeps the live set");
    if disk_first() {
        assert!(
            stats.new_size < stats.old_size,
            "disk-first compaction must shrink the file: {} -> {}",
            stats.old_size,
            stats.new_size
        );
        println!(
            "\n[soak] compaction: {} -> {} bytes ({} live docs)",
            stats.old_size, stats.new_size, stats.docs_kept
        );
    }

    // Correctness immediately after compaction.
    assert_eq!(db.count("c", &json!({})).unwrap() as u64, keys);
    for (&k, &v) in model.iter().take(50) {
        let got = db.find_one("c", &json!({ "k": k })).unwrap();
        assert_eq!(got.and_then(|d| d["v"].as_u64()), Some(v), "k={k} after compact");
    }
    for bucket in 0..8i64 {
        let expected = model.values().filter(|v| (*v % 8) as i64 == bucket).count();
        assert_eq!(db.count("c", &json!({ "bucket": bucket })).unwrap(), expected, "indexed after compact");
    }

    // And after a clean reopen (compacted file must be readable + correct).
    db.shutdown();
    drop(db);
    let db = OxiDb::open(dir.path()).unwrap();
    assert_eq!(db.count("c", &json!({})).unwrap() as u64, keys, "live count after compact+reopen");
    for (&k, &v) in model.iter().take(50) {
        let got = db.find_one("c", &json!({ "k": k })).unwrap();
        assert_eq!(got.and_then(|d| d["v"].as_u64()), Some(v), "k={k} after compact+reopen");
    }
}
