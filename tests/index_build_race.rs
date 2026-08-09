//! Building an index concurrently with writes.
//!
//! `create_index` scans the collection, builds the index locally, and only
//! then registers it under `field_indexes.write()`. A writer running in that
//! window takes the same lock, sees no such index, and updates nothing — so
//! the document it wrote is in storage but not in the index that is about to
//! be published. Every later equality query on that field is served from the
//! index and answers as if the document does not exist.
//!
//! These tests are the proof, not the argument: each asserts what a user
//! observes (a document that exists but cannot be found; a UNIQUE index with
//! two copies of one value), never an internal structure.
//!
//! Three of these four were RED before `index_build_barrier` (0.42.13), on a
//! 200k-document collection in release:
//!
//! - 36 of 43 documents inserted during a build were unreachable afterwards,
//!   and still unreachable after a restart — the bad index is persisted, so
//!   this never healed on its own.
//! - 46 of 56 documents updated during a build were returned under their OLD
//!   value (a document the caller can see does not match) and could not be
//!   found under their new one.
//! - A UNIQUE index accepted 41 duplicate values written during its own build.
//! - Deletes during a build were already safe: the stale entry resolves to a
//!   document that is gone, and every read path loads before it returns.
//!
//! Run them in **release** — a debug build widens the window until the
//! numbers stop meaning anything, and the fixture takes minutes.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use oxidb::OxiDb;
use serde_json::json;
use tempfile::tempdir;

/// Big enough that the build's scan + sort is wide open (hundreds of ms),
/// which is exactly the window a real `create_index` on a real collection has.
const SEED: u64 = 200_000;

/// Batched so the fixture costs one fsync per batch instead of one per
/// document — a per-document seed of this size takes minutes.
fn seed(db: &OxiDb, collection: &str, doc: impl Fn(u64) -> serde_json::Value) {
    for chunk in 0..SEED / 5_000 {
        let docs: Vec<serde_json::Value> = (chunk * 5_000..(chunk + 1) * 5_000).map(&doc).collect();
        db.insert_many(collection, docs).unwrap();
    }
}

fn find_by_email(db: &OxiDb, email: &str) -> usize {
    db.find("users", &json!({"email": email})).unwrap().len()
}

#[test]
fn a_document_written_while_an_index_is_building_is_still_findable() {
    let dir = tempdir().unwrap();
    let db = Arc::new(OxiDb::open(dir.path()).unwrap());
    seed(
        &db,
        "users",
        |i| json!({"i": i, "email": format!("seed{i}@x.com")}),
    );

    let stop = Arc::new(AtomicBool::new(false));
    let written = Arc::new(AtomicU64::new(0));

    let writer = {
        let db = Arc::clone(&db);
        let stop = Arc::clone(&stop);
        let written = Arc::clone(&written);
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let n = written.fetch_add(1, Ordering::Relaxed);
                db.insert("users", json!({"i": -1, "email": format!("mid{n}@x.com")}))
                    .unwrap();
                std::thread::sleep(Duration::from_millis(1));
            }
        })
    };

    // Let the writer get going, then build the index underneath it.
    std::thread::sleep(Duration::from_millis(20));
    db.create_index("users", "email").unwrap();
    // Keep writing after the build too: the writes the barrier delayed land
    // here, and they must be maintained by the index that was just registered.
    std::thread::sleep(Duration::from_millis(50));
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();

    let n = written.load(Ordering::Relaxed);
    // The fix blocks writers for the build, so most of these land on either
    // side of it — but if none were attempted at all this test proves nothing.
    assert!(n >= 10, "the writer never ran ({n} writes) — vacuous");
    let missing: Vec<u64> = (0..n)
        .filter(|k| find_by_email(&db, &format!("mid{k}@x.com")) == 0)
        .collect();

    // Disk-first persists the built index and opens it verbatim next time
    // (only a crash-recovery reindex rebuilds it), so a bad build is not
    // something a restart repairs. Measure that in the same run rather than
    // asserting first — otherwise the restart evidence is never produced.
    drop(db);
    let db = OxiDb::open(dir.path()).unwrap();
    let after_restart: Vec<u64> = (0..n)
        .filter(|k| find_by_email(&db, &format!("mid{k}@x.com")) == 0)
        .collect();

    assert!(
        missing.is_empty() && after_restart.is_empty(),
        "{} of {n} documents written during the build are unreachable by the \
         index built over them ({} still unreachable after a restart, so the \
         bad index was persisted and does not heal on its own); first few: {:?}",
        missing.len(),
        after_restart.len(),
        &missing[..missing.len().min(5)]
    );
}

/// The dangerous direction is not only *missing* entries. A document updated
/// while the build is running is indexed under the value the scan saw, and the
/// writer cannot correct it (the index is not registered yet) — so the index
/// claims a value the document no longer has. A single-field equality query on
/// an indexed field is "fully indexed", so the post-filter is skipped and the
/// index's claim is returned as the answer: a document that does not match.
#[test]
fn an_index_built_over_a_concurrent_update_does_not_answer_with_the_old_value() {
    let dir = tempdir().unwrap();
    let db = Arc::new(OxiDb::open(dir.path()).unwrap());
    seed(&db, "items", |i| json!({"i": i, "tag": format!("seed{i}")}));
    // So the writer's updates are index-served point writes; without this each
    // one is a full scan and only two or three land inside the build window,
    // which makes the outcome a coin flip rather than a demonstration.
    db.create_index("items", "i").unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let updated = Arc::new(AtomicU64::new(0));
    let writer = {
        let db = Arc::clone(&db);
        let stop = Arc::clone(&stop);
        let updated = Arc::clone(&updated);
        std::thread::spawn(move || {
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) && i < SEED {
                db.update(
                    "items",
                    &json!({"i": i}),
                    &json!({"$set": {"tag": format!("moved{i}")}}),
                )
                .unwrap();
                updated.store(i + 1, Ordering::Relaxed);
                i += 1;
            }
        })
    };

    std::thread::sleep(Duration::from_millis(20));
    db.create_index("items", "tag").unwrap();
    std::thread::sleep(Duration::from_millis(50));
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();

    let n = updated.load(Ordering::Relaxed);
    assert!(n >= 10, "the writer never ran ({n} updates) — vacuous");
    let mut stale = Vec::new();
    let mut lost = Vec::new();
    for k in 0..n {
        // The old value is gone from every document — the index must not
        // report a match for it. Anything returned here is a document whose
        // `tag` is provably not what was asked for, which the caller has no
        // way to detect.
        let old = format!("seed{k}");
        for hit in db.find("items", &json!({"tag": &old})).unwrap() {
            assert_ne!(
                hit.get("tag").and_then(|v| v.as_str()),
                Some(old.as_str()),
                "sanity: the old value should not exist on any document"
            );
            stale.push(k);
        }
        // And the new value must be findable.
        if db
            .find("items", &json!({"tag": format!("moved{k}")}))
            .unwrap()
            .is_empty()
        {
            lost.push(k);
        }
    }

    assert!(
        stale.is_empty() && lost.is_empty(),
        "of {n} documents updated during the build, {} are still returned under \
         their OLD value and {} cannot be found under their new one",
        stale.len(),
        lost.len()
    );
}

/// The opposite direction is already safe, and this pins that it stays so: a
/// document deleted during the build leaves an entry in the finished index,
/// but every read path loads the document before returning it, so the entry
/// resolves to nothing. Stale entries self-correct; missing ones do not.
#[test]
fn a_document_deleted_during_a_build_is_not_resurrected_by_the_index() {
    let dir = tempdir().unwrap();
    let db = Arc::new(OxiDb::open(dir.path()).unwrap());
    seed(&db, "gone", |i| json!({"i": i, "tag": format!("seed{i}")}));
    // Index-served point deletes, for the same reason the update test needs
    // one: a delete-by-scan is slow enough that too few land in the window.
    db.create_index("gone", "i").unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let deleted = Arc::new(AtomicU64::new(0));
    let writer = {
        let db = Arc::clone(&db);
        let stop = Arc::clone(&stop);
        let deleted = Arc::clone(&deleted);
        std::thread::spawn(move || {
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) && i < SEED {
                db.delete("gone", &json!({"i": i})).unwrap();
                deleted.store(i + 1, Ordering::Relaxed);
                i += 1;
                std::thread::sleep(Duration::from_millis(1));
            }
        })
    };

    std::thread::sleep(Duration::from_millis(20));
    db.create_index("gone", "tag").unwrap();
    std::thread::sleep(Duration::from_millis(50));
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();

    let n = deleted.load(Ordering::Relaxed);
    assert!(n >= 10, "the writer never ran ({n} deletes) — vacuous");
    let resurrected: Vec<u64> = (0..n)
        .filter(|k| {
            !db.find("gone", &json!({"tag": format!("seed{k}")}))
                .unwrap()
                .is_empty()
        })
        .collect();
    assert!(
        resurrected.is_empty(),
        "{} of {n} documents deleted during the build are still returned",
        resurrected.len()
    );
}

#[test]
fn a_unique_index_built_over_a_concurrent_write_still_rejects_duplicates() {
    let dir = tempdir().unwrap();
    let db = Arc::new(OxiDb::open(dir.path()).unwrap());
    seed(
        &db,
        "accounts",
        |i| json!({"i": i, "tag": format!("seed{i}")}),
    );

    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let db = Arc::clone(&db);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let _ = db.insert("accounts", json!({"tag": format!("mid{n}")}));
                n += 1;
                std::thread::sleep(Duration::from_millis(1));
            }
            n
        })
    };

    std::thread::sleep(Duration::from_millis(20));
    db.create_unique_index("accounts", "tag").unwrap();
    std::thread::sleep(Duration::from_millis(50));
    stop.store(true, Ordering::Relaxed);
    let n = writer.join().unwrap();
    assert!(n >= 10, "the writer never ran ({n} writes) — vacuous");

    // Every value written during the build must now be enforced as unique:
    // a second copy has to be refused.
    let mut accepted_duplicates = Vec::new();
    for k in 0..n {
        let tag = format!("mid{k}");
        if db.insert("accounts", json!({"tag": &tag})).is_ok() {
            accepted_duplicates.push(tag);
        }
    }

    assert!(
        accepted_duplicates.is_empty(),
        "the UNIQUE index accepted {} duplicate values written during its own \
         build (first few: {:?})",
        accepted_duplicates.len(),
        &accepted_duplicates[..accepted_duplicates.len().min(5)]
    );
}

/// A query that arrives *while* the build is running must be answered as if
/// the index did not exist yet — from a scan, completely. The staging index is
/// partial by definition; a reader that consulted it would silently miss every
/// document the backfill has not reached. The gate is structural (the staging
/// index lives under a map key no query can name), and this pins it: a seeded
/// document from the end of the id range must be findable at every moment of
/// the build.
#[test]
fn a_query_during_the_build_sees_every_document() {
    let dir = tempdir().unwrap();
    let db = Arc::new(OxiDb::open(dir.path()).unwrap());
    seed(
        &db,
        "live",
        |i| json!({"i": i, "email": format!("seed{i}@x.com")}),
    );

    let builder = {
        let db = Arc::clone(&db);
        std::thread::spawn(move || db.create_index("live", "email").unwrap())
    };

    // Query throughout the build window. The probe is near the top of the id
    // range so early in the build it is exactly the kind of document a
    // partial index would not have yet.
    let probe = format!("seed{}@x.com", SEED - 1);
    let mut checks = 0u32;
    while !builder.is_finished() {
        assert_eq!(
            db.find("live", &json!({"email": &probe})).unwrap().len(),
            1,
            "a query during the build missed a document (after {checks} good checks)"
        );
        checks += 1;
    }
    builder.join().unwrap();
    assert!(
        checks > 0,
        "the build finished before any query ran — vacuous"
    );
    assert_eq!(db.find("live", &json!({"email": &probe})).unwrap().len(), 1);
}

/// Index metadata on DISK must not name the index until the build completes:
/// the metadata file is what reopen trusts, so a crash mid-build must find a
/// file that simply does not know the index — anything else resurrects a
/// partial index as ready. The builder itself only saves at completion, so
/// the test forces a save mid-build the way production would: `drop_index`
/// of another index (which does not serialize against builds) rewrites the
/// metadata file from the live index map, and that rewrite must exclude the
/// staging index.
#[test]
fn a_mid_build_metadata_save_does_not_name_the_building_index() {
    let dir = tempdir().unwrap();
    let db = Arc::new(OxiDb::open(dir.path()).unwrap());
    seed(
        &db,
        "meta",
        |i| json!({"i": i, "email": format!("seed{i}@x.com")}),
    );
    // A second index whose drop will force the metadata rewrite.
    db.create_index("meta", "i").unwrap();

    let builder = {
        let db = Arc::clone(&db);
        std::thread::spawn(move || db.create_index("meta", "email").unwrap())
    };
    // Let the build register and get into its backfill.
    std::thread::sleep(Duration::from_millis(30));

    db.drop_index("meta", "i").unwrap();
    let meta = std::fs::read_to_string(dir.path().join("meta.idx")).unwrap();
    let done = builder.is_finished();
    builder.join().unwrap();

    // If the build had already published before our save, "email" in the file
    // is legitimate — assert only when the save provably ran mid-build.
    assert!(
        !done,
        "the build finished before the mid-build save — widen the window (SEED too small?)"
    );
    assert!(
        !meta.contains("email"),
        "a metadata save during the build wrote the building index to disk: {meta}"
    );

    // And at completion the metadata does name it.
    let meta = std::fs::read_to_string(dir.path().join("meta.idx")).unwrap();
    assert!(meta.contains("email"));
}
