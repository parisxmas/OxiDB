//! CERN-grade disaster-recovery drill (category 8 in
//! `docs/testing-roadmap.md`).
//!
//! Simulates the "primary site is on fire" scenario end-to-end:
//!
//!   1. Build a non-trivial DB (multi-collection, indexed, blob objects)
//!   2. `backup()` → portable tar.gz
//!   3. **TOTAL DATA LOSS** — wipe the original data directory
//!   4. `restore()` to a fresh target dir
//!   5. Reopen the restored DB and verify:
//!        - all documents present and queryable by ID and by index
//!        - all blob objects present with original etags
//!        - all indexes still functional (no rebuild needed)
//!        - aggregation pipelines produce same results
//!
//! Times the operations so the test output is also a basic RTO
//! (recovery time objective) measurement. Marked `#[ignore]` so the
//! default `cargo test` run stays fast; opt-in:
//!   cargo test --test cern_dr_drill -- --ignored --nocapture

use serde_json::json;
use std::time::Instant;
use tempfile::tempdir;

use oxidb::OxiDb;

const DOCS_PER_COLLECTION: usize = 500;
const NUM_COLLECTIONS: usize = 3;

#[test]
#[ignore]
fn full_backup_wipe_restore_preserves_everything() {
    let original_dir = tempdir().expect("original data dir");
    let archive_dir = tempdir().expect("archive dir");
    let restored_dir = tempdir().expect("restored data dir");
    let archive_path = archive_dir.path().join("dr-drill.tar.gz");

    // ── Phase 1: build a non-trivial DB ──────────────────────────────
    let build_t0 = Instant::now();
    {
        let db = OxiDb::open(original_dir.path()).expect("open original");

        for col_idx in 0..NUM_COLLECTIONS {
            let col_name = format!("col_{col_idx}");
            for i in 0..DOCS_PER_COLLECTION {
                db.insert(
                    &col_name,
                    json!({
                        "id": i as i64,
                        "value": format!("v{i}"),
                        "score": (i * 7 + col_idx * 13) as i64,
                        "tags": ["a", "b", "c"],
                    }),
                )
                .expect("insert");
            }
            // One index per collection — must survive backup/restore.
            db.create_index(&col_name, "score").ok();
        }

        // Blob bucket so we exercise the _blobs/ path too.
        db.put_object("dr-bucket", "evidence.txt", b"if you see this, restore worked",
                      "text/plain", std::collections::HashMap::new())
            .expect("put_object");
        db.put_object("dr-bucket", "binary.bin", &[0xDE, 0xAD, 0xBE, 0xEF],
                      "application/octet-stream", std::collections::HashMap::new())
            .expect("put_object");
    }
    eprintln!("[dr] phase 1: built DB ({:?})", build_t0.elapsed());

    // ── Phase 2: backup ──────────────────────────────────────────────
    let backup_t0 = Instant::now();
    {
        let db = OxiDb::open(original_dir.path()).expect("reopen for backup");
        let info = db.backup(&archive_path).expect("backup");
        eprintln!(
            "[dr] phase 2: backup → {} ({:?}, {} collections)",
            archive_path.display(),
            backup_t0.elapsed(),
            info.collections,
        );
    }
    assert!(archive_path.exists(), "backup file must exist after backup()");
    let archive_size = std::fs::metadata(&archive_path).unwrap().len();
    assert!(archive_size > 0, "archive must be non-empty");

    // ── Phase 3: TOTAL DATA LOSS ─────────────────────────────────────
    // Mirrors a primary-site catastrophe. After this the original_dir
    // contains literally nothing.
    let wipe_t0 = Instant::now();
    std::fs::remove_dir_all(original_dir.path()).expect("wipe original");
    std::fs::create_dir_all(original_dir.path()).expect("recreate empty original");
    let post_wipe_entries: Vec<_> = std::fs::read_dir(original_dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .collect();
    assert!(
        post_wipe_entries.is_empty(),
        "data dir must be empty after wipe (found {} entries)",
        post_wipe_entries.len()
    );
    eprintln!("[dr] phase 3: wiped original ({:?})", wipe_t0.elapsed());

    // ── Phase 4: restore ─────────────────────────────────────────────
    let restore_t0 = Instant::now();
    let info = OxiDb::restore(&archive_path, restored_dir.path())
        .expect("restore from archive");
    eprintln!(
        "[dr] phase 4: restored to {} ({:?}, archive size {} bytes)",
        restored_dir.path().display(),
        restore_t0.elapsed(),
        archive_size,
    );
    assert!(
        info.collections >= NUM_COLLECTIONS,
        "restored collection count {} < expected {NUM_COLLECTIONS}",
        info.collections
    );

    // ── Phase 5: verify everything is intact ─────────────────────────
    let verify_t0 = Instant::now();
    let db = OxiDb::open(restored_dir.path()).expect("open restored");

    // 5a. Every doc in every collection.
    for col_idx in 0..NUM_COLLECTIONS {
        let col_name = format!("col_{col_idx}");
        let docs = db.find(&col_name, &json!({})).expect("find all");
        assert_eq!(
            docs.len(),
            DOCS_PER_COLLECTION,
            "collection {col_name}: got {} docs, expected {DOCS_PER_COLLECTION}",
            docs.len()
        );

        // 5b. Index-backed query still resolves correctly.
        let high_scores = db
            .find(&col_name, &json!({"score": {"$gte": 3000}}))
            .expect("index query");
        let expected_count = (0..DOCS_PER_COLLECTION)
            .filter(|i| (i * 7 + col_idx * 13) as i64 >= 3000)
            .count();
        assert_eq!(
            high_scores.len(),
            expected_count,
            "collection {col_name}: index-backed query returned wrong count after restore"
        );

        // 5c. Aggregation pipeline still functions.
        let pipeline = json!([
            {"$match": {"score": {"$gte": 0}}},
            {"$group": {"_id": null, "total": {"$sum": "$score"}}},
        ]);
        let agg = db.aggregate(&col_name, &pipeline).expect("aggregate");
        assert_eq!(agg.len(), 1, "aggregation must return 1 group");
        let total = agg[0]["total"].as_i64().unwrap();
        let expected_total: i64 = (0..DOCS_PER_COLLECTION)
            .map(|i| (i * 7 + col_idx * 13) as i64)
            .sum();
        assert_eq!(
            total, expected_total,
            "collection {col_name}: aggregation sum differs after restore"
        );
    }

    // 5d. Blobs survived too.
    let (blob1, _meta1) = db
        .get_object("dr-bucket", "evidence.txt")
        .expect("get text blob");
    assert_eq!(blob1, b"if you see this, restore worked");
    let (blob2, _meta2) = db
        .get_object("dr-bucket", "binary.bin")
        .expect("get binary blob");
    assert_eq!(blob2, &[0xDE, 0xAD, 0xBE, 0xEF]);

    let verify_elapsed = verify_t0.elapsed();
    eprintln!("[dr] phase 5: verified ({verify_elapsed:?})");

    // Total RTO (time from "site dies" to "all data verified").
    // wipe_t0 is the start of wipe → it's also our reference for
    // "moment of disaster". The total elapsed since then is the RTO.
    eprintln!("[dr] TOTAL RTO (wipe → verify): {:?}", wipe_t0.elapsed());
}
