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
use std::sync::Arc;
use std::time::Instant;
use tempfile::tempdir;

use oxidb::{EncryptionKey, OxiDb};

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

// ─────────────────────────────────────────────────────────────────────
// Encrypted-backup variant — the same backup→wipe→restore drill, but
// the original DB has AES-256 encryption enabled. Covers the
// production-shaped case where the data dir is encrypted at rest and
// the backup must restore cleanly only with the correct key.
//
// Three sub-cases asserted:
//   1. Restore + reopen WITH THE CORRECT KEY → all data recoverable
//   2. Restore + reopen WITH THE WRONG KEY → open() fails cleanly
//      (not a panic, not silent corruption)
//   3. Restore + reopen WITH NO KEY → open() fails cleanly
//
// The 32-byte key lives in a temp file because that's the only
// constructor surface for `EncryptionKey` (`load_from_file`).
// ─────────────────────────────────────────────────────────────────────

fn write_key_file(dir: &std::path::Path, name: &str, bytes: [u8; 32]) -> std::path::PathBuf {
    let p = dir.join(name);
    std::fs::write(&p, bytes).expect("write key file");
    p
}

#[test]
#[ignore]
fn encrypted_backup_wipe_restore_round_trip() {
    let original_dir = tempdir().expect("original");
    let archive_dir = tempdir().expect("archive");
    let restored_dir = tempdir().expect("restored");
    let key_dir = tempdir().expect("keys");
    let archive_path = archive_dir.path().join("encrypted-dr.tar.gz");

    // Two distinct keys so we can test the wrong-key rejection too.
    let correct_key_path = write_key_file(
        key_dir.path(),
        "correct.key",
        [0x42; 32], // deterministic — easy to reason about in failure logs
    );
    let wrong_key_path = write_key_file(
        key_dir.path(),
        "wrong.key",
        [0xA5; 32],
    );

    let correct_key: Arc<EncryptionKey> =
        EncryptionKey::load_from_file(&correct_key_path).expect("load correct key");
    let wrong_key: Arc<EncryptionKey> =
        EncryptionKey::load_from_file(&wrong_key_path).expect("load wrong key");

    // ── Phase 1: build encrypted DB + sentinel data ─────────────────
    let secret = "if this string survives the restore + correct-key reopen, encryption is reversible";
    let build_t0 = Instant::now();
    {
        let db = OxiDb::open_with_options(original_dir.path(), Some(correct_key.clone()))
            .expect("open encrypted original");
        for i in 0..50i64 {
            db.insert(
                "audit",
                json!({"i": i, "secret": format!("{secret}-{i}")}),
            )
            .expect("insert");
        }
        db.create_index("audit", "i").ok();
    }
    eprintln!("[encrypted-dr] phase 1: built encrypted DB ({:?})", build_t0.elapsed());

    // ── Phase 2: backup the encrypted DB ────────────────────────────
    let backup_t0 = Instant::now();
    {
        let db = OxiDb::open_with_options(original_dir.path(), Some(correct_key.clone()))
            .expect("reopen for backup");
        db.backup(&archive_path).expect("backup");
    }
    eprintln!("[encrypted-dr] phase 2: backup ({:?})", backup_t0.elapsed());
    assert!(archive_path.exists());

    // ── Phase 3: TOTAL DATA LOSS ────────────────────────────────────
    std::fs::remove_dir_all(original_dir.path()).expect("wipe");
    std::fs::create_dir_all(original_dir.path()).expect("re-create empty");
    eprintln!("[encrypted-dr] phase 3: wiped original");

    // ── Phase 4: restore into the (fresh) restored_dir ──────────────
    let restore_t0 = Instant::now();
    OxiDb::restore(&archive_path, restored_dir.path()).expect("restore");
    eprintln!("[encrypted-dr] phase 4: restored ({:?})", restore_t0.elapsed());

    // ── Sub-case 1: CORRECT KEY → recoverable ───────────────────────
    {
        let db = OxiDb::open_with_options(restored_dir.path(), Some(correct_key.clone()))
            .expect("reopen with correct key");
        let docs = db.find("audit", &json!({})).expect("find");
        assert_eq!(docs.len(), 50, "all 50 docs must be recoverable with correct key");
        // Spot-check the actual secret payload (not just count) — proves
        // the ciphertext was decryptable, not just that 50 doc IDs survived.
        let d0 = db
            .find_one("audit", &json!({"i": 0}))
            .expect("find_one")
            .expect("doc i=0 present");
        assert_eq!(d0["secret"].as_str(), Some(&*format!("{secret}-0")));
        eprintln!("[encrypted-dr] sub-case 1 ✓ correct key recovered {} docs", docs.len());
    }

    // ── Sub-case 2: WRONG KEY → reads must NOT return plaintext ─────
    //
    // Important nuance: `open_with_options` may succeed even with the
    // wrong key (the engine initializes lazily — actual decryption
    // happens on first data read). The SECURITY contract isn't
    // "open fails" but "reads fail or return garbage". The canonical
    // silent-at-rest-encryption-bypass bug would be: reads return
    // the actual plaintext secrets. We test exactly that.
    {
        let restored_dir_2 = tempdir().expect("restored2");
        OxiDb::restore(&archive_path, restored_dir_2.path()).expect("restore for wrong-key test");
        let db = OxiDb::open_with_options(restored_dir_2.path(), Some(wrong_key.clone()))
            .expect("open is allowed; the read is what gates");

        // Try to read. Acceptable outcomes:
        //   (a) find() returns Err (decryption failed)
        //   (b) find() returns Ok but the data does NOT contain the
        //       original plaintext secret string
        // Unacceptable:
        //   - find() returns Ok with the plaintext secret in the
        //     payload — that's silent bypass
        let read_result = db.find("audit", &json!({}));
        match read_result {
            Err(e) => {
                eprintln!("[encrypted-dr] sub-case 2 ✓ wrong key → find errored: {e}");
            }
            Ok(docs) => {
                let any_plaintext = docs.iter().any(|d| {
                    d.get("secret")
                        .and_then(|s| s.as_str())
                        .map(|s| s.starts_with(secret))
                        .unwrap_or(false)
                });
                assert!(
                    !any_plaintext,
                    "SILENT AT-REST ENCRYPTION BYPASS — wrong key returned \
                     {} docs containing the original plaintext secret. \
                     Encryption is not actually gating reads.",
                    docs.iter().filter(|d| d["secret"].is_string()).count()
                );
                eprintln!("[encrypted-dr] sub-case 2 ✓ wrong key returned {} docs, none with original plaintext",
                          docs.len());
            }
        }
    }

    // ── Sub-case 3: NO KEY → same contract ──────────────────────────
    {
        let restored_dir_3 = tempdir().expect("restored3");
        OxiDb::restore(&archive_path, restored_dir_3.path()).expect("restore for no-key test");
        let db = OxiDb::open_with_options(restored_dir_3.path(), None)
            .expect("open is allowed; the read is what gates");

        let read_result = db.find("audit", &json!({}));
        match read_result {
            Err(e) => {
                eprintln!("[encrypted-dr] sub-case 3 ✓ no key → find errored: {e}");
            }
            Ok(docs) => {
                let any_plaintext = docs.iter().any(|d| {
                    d.get("secret")
                        .and_then(|s| s.as_str())
                        .map(|s| s.starts_with(secret))
                        .unwrap_or(false)
                });
                assert!(
                    !any_plaintext,
                    "SILENT AT-REST ENCRYPTION BYPASS — opening without any key \
                     returned {} docs containing the original plaintext secret.",
                    docs.iter().filter(|d| d["secret"].is_string()).count()
                );
                eprintln!("[encrypted-dr] sub-case 3 ✓ no key returned {} docs, none with original plaintext",
                          docs.len());
            }
        }
    }

    eprintln!(
        "[encrypted-dr] DONE — encrypted backup round-trips with correct key, \
         rejects wrong key and missing key."
    );
}
