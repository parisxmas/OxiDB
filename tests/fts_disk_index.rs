//! The disk-backed collection text index (`.mtidx`), end to end through the
//! engine: build, search, restart WITHOUT a rebuild scan, mutate across the
//! persisted base, and drop.
//!
//! Before this, disk-first persisted only the index *definition* and rebuilt
//! the postings by a full collection scan at every open — the whole inverted
//! index resident (measured 785 MB per 1M docs) and the scan paid each start.

use oxidb::OxiDb;
use serde_json::json;
use tempfile::tempdir;

fn seed(db: &OxiDb, n: u64) {
    for chunk in (0..n).step_by(2_000) {
        let docs: Vec<_> = (chunk..(chunk + 2_000).min(n))
            .map(|i| {
                json!({
                    "i": i,
                    "body": format!(
                        "ortak konu {} {}",
                        if i % 100 == 0 { "zümrüt" } else { "dolgu" },
                        i
                    )
                })
            })
            .collect();
        db.insert_many("notes", docs).unwrap();
    }
}

#[test]
fn the_text_index_survives_a_restart_without_a_rebuild_scan() {
    let dir = tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();
    seed(&db, 10_000);
    db.create_text_index("notes", vec!["body".into()]).unwrap();

    let hits = db.text_search("notes", "zümrüt", 200).unwrap();
    assert_eq!(hits.len(), 100);

    // The base file exists the moment the build returns — the build persists
    // eagerly, so the resident cost is the (empty) overlay, not the corpus.
    let mtidx = dir.path().join("notes.mtidx");
    assert!(mtidx.exists(), "create_text_index must persist the base");

    drop(db);
    let db = OxiDb::open(dir.path()).unwrap();

    // Same answers after reopen. (That the open did not scan is proven by the
    // unit layer — `open_disk` never touches storage; this pins the wiring.)
    let hits = db.text_search("notes", "zümrüt", 200).unwrap();
    assert_eq!(hits.len(), 100, "reopened index lost documents");

    // The reopened index accepts writes: new doc, update, delete — all three
    // void or add base postings through the overlay + dead set.
    let new_id = db
        .insert("notes", json!({"i": -1, "body": "taze zümrüt kayıt"}))
        .unwrap();
    let hits = db.text_search("notes", "taze", 10).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0]["_id"].as_u64(), Some(new_id));

    let victim = db.text_search("notes", "zümrüt", 1).unwrap()[0]["_id"]
        .as_u64()
        .unwrap();
    db.delete("notes", &json!({"_id": victim})).unwrap();
    let hits = db.text_search("notes", "zümrüt", 300).unwrap();
    assert_eq!(hits.len(), 100, "one deleted (−1), one inserted (+1)");
    assert!(
        hits.iter().all(|h| h["_id"].as_u64() != Some(victim)),
        "a deleted doc still answers from the base"
    );

    db.update(
        "notes",
        &json!({"_id": new_id}),
        &json!({"$set": {"body": "artık bambaşka"}}),
    )
    .unwrap();
    assert!(
        db.text_search("notes", "taze", 10).unwrap().is_empty(),
        "an updated doc still answers under its OLD terms"
    );
    assert_eq!(db.text_search("notes", "bambaşka", 10).unwrap().len(), 1);

    // And all of that survives another restart (the shutdown checkpoint
    // folds the overlay).
    drop(db);
    let db = OxiDb::open(dir.path()).unwrap();
    // 100 seeded − 1 deleted + 1 inserted-with-zümrüt − that one updated
    // AWAY from zümrüt = 99.
    assert_eq!(db.text_search("notes", "zümrüt", 300).unwrap().len(), 99);
    assert!(db.text_search("notes", "taze", 10).unwrap().is_empty());
    assert_eq!(db.text_search("notes", "bambaşka", 10).unwrap().len(), 1);
}

#[test]
fn dropping_the_text_index_removes_its_base_file() {
    let dir = tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();
    seed(&db, 1_000);
    db.create_text_index("notes", vec!["body".into()]).unwrap();
    let mtidx = dir.path().join("notes.mtidx");
    assert!(mtidx.exists());

    db.drop_index("notes", "_text").unwrap();
    assert!(
        !mtidx.exists(),
        "a stale base would be adopted by a re-create"
    );
    assert!(db.text_search("notes", "ortak", 10).is_err());

    // Re-create builds fresh and works.
    db.create_text_index("notes", vec!["body".into()]).unwrap();
    assert!(!db.text_search("notes", "ortak", 10).unwrap().is_empty());
}
