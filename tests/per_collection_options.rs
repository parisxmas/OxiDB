//! Per-collection storage options: a single engine can host a disk-first
//! (optionally uncompressed) collection alongside a default in-RAM one, with no
//! `OXIDB_*` env vars set. The disk-first collection's options persist
//! (`<name>.bopts`) so a reopen stays disk-first regardless of the environment.
//!
//! These tests must run WITHOUT the disk-first env vars set (the default), so
//! that the only thing making a collection disk-first is its explicit options.

use oxidb::{OxiDb, StorageOptions};
use serde_json::json;

fn file(dir: &std::path::Path, name: &str) -> bool {
    dir.join(name).exists()
}

#[test]
fn mixed_disk_first_and_in_ram_collections_in_one_engine() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = OxiDb::open(dir.path()).unwrap();

        // "fast": disk-first, uncompressed, explicit per-collection options.
        db.create_collection_with_options(
            "fast",
            StorageOptions {
                disk_first: true,
                compress: false,
                ..StorageOptions::default()
            },
        )
        .unwrap();

        // "small": explicitly in-RAM. (Was "the default" until disk-first
        // BECAME the default; the point of this test is that the storage
        // shape is per-collection, so both modes are pinned explicitly.)
        db.create_collection_with_options(
            "small",
            StorageOptions {
                disk_first: false,
                ..StorageOptions::default()
            },
        )
        .unwrap();

        for i in 0..500u64 {
            db.insert("fast", json!({ "k": i, "v": i * 2 })).unwrap();
            db.insert("small", json!({ "k": i, "v": i * 3 })).unwrap();
        }

        assert_eq!(db.count("fast", &json!({})).unwrap(), 500);
        assert_eq!(db.count("small", &json!({})).unwrap(), 500);
        assert_eq!(
            db.find_one("fast", &json!({ "k": 42 }))
                .unwrap()
                .and_then(|d| d["v"].as_u64()),
            Some(84)
        );

        db.shutdown();
    }

    // On disk: "fast" is disk-first (.bdat + persisted .bopts), "small" is
    // in-RAM (.btree, no .bdat). This is the whole point — the storage shape is
    // per-collection, not a process-wide switch.
    assert!(
        file(dir.path(), "fast.bdat"),
        "fast must be disk-first (.bdat)"
    );
    assert!(
        file(dir.path(), "fast.bopts"),
        "fast options must persist (.bopts)"
    );
    assert!(
        !file(dir.path(), "fast.btree"),
        "fast must NOT have a .btree"
    );
    assert!(
        file(dir.path(), "small.btree"),
        "small must be in-RAM (.btree)"
    );
    assert!(
        !file(dir.path(), "small.bdat"),
        "small must NOT have a .bdat"
    );

    // The uncompressed .bdat: a highly-repetitive payload would shrink under
    // zstd; here the file should be at least the raw live-bytes size.
    let bdat = std::fs::metadata(dir.path().join("fast.bdat"))
        .unwrap()
        .len();
    assert!(bdat > 0);

    // Reopen with NO env vars: "fast" must still come up disk-first (resolved
    // from .bopts, not the environment) with all data intact.
    {
        let db = OxiDb::open(dir.path()).unwrap();
        assert_eq!(
            db.count("fast", &json!({})).unwrap(),
            500,
            "fast survives reopen"
        );
        assert_eq!(
            db.count("small", &json!({})).unwrap(),
            500,
            "small survives reopen"
        );
        assert_eq!(
            db.find_one("fast", &json!({ "k": 100 }))
                .unwrap()
                .and_then(|d| d["v"].as_u64()),
            Some(200),
            "disk-first data intact after env-independent reopen"
        );
        // Still disk-first after reopen (no .btree appeared for "fast").
        db.insert("fast", json!({ "k": 9999, "v": 1 })).unwrap();
        db.shutdown();
    }
    assert!(
        !file(dir.path(), "fast.btree"),
        "fast stays disk-first across reopen"
    );
}

#[test]
fn compressed_vs_uncompressed_disk_first_same_engine() {
    if std::env::var("OXIDB_DISK_FIRST").is_ok() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();

    db.create_collection_with_options(
        "zc",
        StorageOptions {
            disk_first: true,
            compress: true,
            ..StorageOptions::default()
        },
    )
    .unwrap();
    db.create_collection_with_options(
        "raw",
        StorageOptions {
            disk_first: true,
            compress: false,
            ..StorageOptions::default()
        },
    )
    .unwrap();

    // Highly compressible payload so the compressed store is clearly smaller.
    let payload = "x".repeat(2000);
    for i in 0..300u64 {
        let doc = json!({ "k": i, "blob": payload });
        db.insert("zc", doc.clone()).unwrap();
        db.insert("raw", doc).unwrap();
    }
    db.shutdown();

    let zc = std::fs::metadata(dir.path().join("zc.bdat")).unwrap().len();
    let raw = std::fs::metadata(dir.path().join("raw.bdat"))
        .unwrap()
        .len();
    assert!(
        zc < raw / 2,
        "compressed .bdat ({zc}) should be far smaller than uncompressed ({raw})"
    );

    // Both read back identically regardless of on-disk compression.
    let db = OxiDb::open(dir.path()).unwrap();
    for name in ["zc", "raw"] {
        let d = db.find_one(name, &json!({ "k": 150 })).unwrap().unwrap();
        assert_eq!(d["blob"].as_str().unwrap().len(), 2000, "{name} round-trip");
    }
}
