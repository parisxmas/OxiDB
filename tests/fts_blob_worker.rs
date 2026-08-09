//! The blob FTS worker path after jobs stopped carrying the object's bytes:
//! the worker re-reads from the blob store, so indexing must survive the
//! object being overwritten or deleted between queueing and processing.

use oxidb::OxiDb;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tempfile::tempdir;

/// Poll until the search returns `want` hits (indexing is async) or fail.
fn wait_hits(db: &OxiDb, query: &str, want: usize) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let hits = db.search(Some("files"), query, 10).unwrap();
        if hits.len() == want {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected {want} hits for {query:?}, still at {}",
            hits.len()
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}

#[test]
fn the_worker_indexes_what_the_store_holds_now() {
    let dir = tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();

    db.put_object(
        "files",
        "a.txt",
        "eski kayıt zümrüt".as_bytes(),
        "text/plain",
        HashMap::new(),
    )
    .unwrap();
    wait_hits(&db, "zümrüt", 1);

    // Overwrite: the old terms must stop matching, the new ones start.
    db.put_object(
        "files",
        "a.txt",
        "artık bambaşka".as_bytes(),
        "text/plain",
        HashMap::new(),
    )
    .unwrap();
    wait_hits(&db, "bambaşka", 1);
    wait_hits(&db, "zümrüt", 0);

    // Delete: gone from the index, and a job racing the delete (the worker
    // re-reads and finds nothing) must skip cleanly, not wedge the worker.
    db.delete_object("files", "a.txt").unwrap();
    wait_hits(&db, "bambaşka", 0);

    db.put_object(
        "files",
        "b.txt",
        "işleyen kuyruk kanıtı".as_bytes(),
        "text/plain",
        HashMap::new(),
    )
    .unwrap();
    db.delete_object("files", "b.txt").unwrap(); // maybe before its job ran
    db.put_object(
        "files",
        "c.txt",
        "sonraki iş sağlam".as_bytes(),
        "text/plain",
        HashMap::new(),
    )
    .unwrap();
    // The worker survived whatever b.txt's job found and processed c.txt.
    wait_hits(&db, "sağlam", 1);
}
