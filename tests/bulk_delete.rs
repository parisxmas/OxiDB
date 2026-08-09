//! Batched bulk delete on the document engine.
//!
//! The gap this closes: the delete path has always honoured a `limit` — with
//! early exit on all three access paths — but nothing above the collection
//! could pass one. `OxiDb::delete` hardcoded `None` and the wire `delete`
//! command had no such field, so a client's only choices were `delete_one` and
//! delete-everything, and delete-everything holds every matched document in
//! memory before it writes anything. A collection far larger than memory could
//! not be purged in batches at all.
//!
//! Also pinned here: the two things that made an unbatched delete more
//! expensive than it needed to be — a full JSON deep copy per matched document
//! even when nothing would read it, and a TTL sweep that evicted its entire
//! backlog under one set of index write locks.

use oxidb::OxiDb;
use serde_json::json;

fn seed(db: &OxiDb, col: &str, n: i64) {
    let docs: Vec<_> = (1..=n)
        .map(|i| json!({"id": i, "ts": i, "kind": format!("k{}", i % 8), "payload": "xxxxxxxxxxxxxxxxxxxx"}))
        .collect();
    db.insert_many(col, docs).unwrap();
}

fn count(db: &OxiDb, col: &str) -> usize {
    db.count(col, &json!({})).unwrap()
}

#[test]
fn delete_limited_deletes_exactly_that_many() {
    let db = OxiDb::open_in_memory().unwrap();
    seed(&db, "t", 1000);

    assert_eq!(
        db.delete_limited("t", &json!({"ts": {"$gt": 0}}), Some(10))
            .unwrap(),
        10
    );
    assert_eq!(count(&db, "t"), 990);

    // Repeatable — this is the purge loop.
    for _ in 0..3 {
        assert_eq!(
            db.delete_limited("t", &json!({"ts": {"$gt": 0}}), Some(100))
                .unwrap(),
            100
        );
    }
    assert_eq!(count(&db, "t"), 690);
}

#[test]
fn delete_limited_none_still_deletes_everything_matching() {
    let db = OxiDb::open_in_memory().unwrap();
    seed(&db, "t", 500);
    assert_eq!(
        db.delete_limited("t", &json!({"ts": {"$lte": 200}}), None)
            .unwrap(),
        200
    );
    assert_eq!(count(&db, "t"), 300);
}

#[test]
fn delete_limited_larger_than_the_match_set_deletes_what_matches() {
    let db = OxiDb::open_in_memory().unwrap();
    seed(&db, "t", 100);
    // kind k3 is i % 8 == 3 for i in 1..=100: 3, 11, ... 99 — thirteen of them.
    let n = db
        .delete_limited("t", &json!({"kind": "k3"}), Some(500))
        .unwrap();
    assert_eq!(n, 13);
    assert_eq!(count(&db, "t"), 87);
}

#[test]
fn delete_limited_zero_deletes_nothing() {
    let db = OxiDb::open_in_memory().unwrap();
    seed(&db, "t", 50);
    assert_eq!(
        db.delete_limited("t", &json!({"ts": {"$gt": 0}}), Some(0))
            .unwrap(),
        0
    );
    assert_eq!(count(&db, "t"), 50);
}

#[test]
fn a_limited_delete_selects_in_document_id_order() {
    // Not tidiness: in cluster mode a limited delete replicates as one request
    // and each node runs it locally, so the selection has to be a function of
    // replicated state. Ids come from the replicated insert order; an
    // arbitrary sample would diverge replicas.
    let db = OxiDb::open_in_memory().unwrap();
    seed(&db, "t", 100);
    db.delete_limited("t", &json!({"ts": {"$gt": 0}}), Some(10))
        .unwrap();
    let left = db.find("t", &json!({"ts": {"$lte": 20}})).unwrap();
    let mut tss: Vec<i64> = left
        .iter()
        .map(|d| d.get("ts").unwrap().as_i64().unwrap())
        .collect();
    tss.sort();
    // The first ten inserted are gone; 11..=20 remain.
    assert_eq!(tss, (11..=20).collect::<Vec<_>>());
}

#[test]
fn a_limited_delete_is_served_by_an_index_when_one_exists() {
    let db = OxiDb::open_in_memory().unwrap();
    seed(&db, "t", 2000);
    db.create_index("t", "ts").unwrap();
    // A range on the indexed field: 100 match, the limit takes 10.
    assert_eq!(
        db.delete_limited("t", &json!({"ts": {"$gt": 500, "$lte": 600}}), Some(10))
            .unwrap(),
        10
    );
    assert_eq!(count(&db, "t"), 1990);
    // The remaining 90 of that range are still there and still findable
    // through the index — the index was maintained across the delete.
    assert_eq!(
        db.count("t", &json!({"ts": {"$gt": 500, "$lte": 600}}))
            .unwrap(),
        90
    );
}

#[test]
fn deleting_from_an_unindexed_collection_keeps_the_documents_findable() {
    // The delete path no longer carries a copy of each matched document when
    // no index and no subscriber will read it. If that gate were wrong in the
    // other direction — values dropped while an index still needed them — the
    // index would keep stale entries and a later query would return documents
    // that no longer exist. Both shapes are checked.
    let db = OxiDb::open_in_memory().unwrap();
    seed(&db, "unindexed", 200);
    assert_eq!(
        db.delete_limited("unindexed", &json!({"ts": {"$lte": 50}}), None)
            .unwrap(),
        50
    );
    assert_eq!(count(&db, "unindexed"), 150);
    assert_eq!(
        db.count("unindexed", &json!({"ts": {"$lte": 50}})).unwrap(),
        0
    );
}

#[test]
fn an_indexed_delete_leaves_no_stale_index_entry() {
    // The dangerous direction of the same gate: a document dropped from the
    // store but left in an index is a phantom — `count` (index-only) would
    // report it and `find` would not return it.
    let db = OxiDb::open_in_memory().unwrap();
    seed(&db, "t", 300);
    db.create_index("t", "kind").unwrap();
    db.create_index("t", "ts").unwrap();

    let before = db.count("t", &json!({"kind": "k1"})).unwrap();
    assert!(before > 0);
    db.delete_limited("t", &json!({"kind": "k1"}), None)
        .unwrap();

    // Index-only count and the documents themselves must agree.
    assert_eq!(db.count("t", &json!({"kind": "k1"})).unwrap(), 0);
    assert!(db.find("t", &json!({"kind": "k1"})).unwrap().is_empty());
    // The other index is intact too.
    assert_eq!(
        db.count("t", &json!({"ts": {"$lte": 300}})).unwrap(),
        300 - before
    );
}

#[test]
fn a_composite_index_survives_a_delete_that_keeps_no_pre_images() {
    // Composite indexes are the other consumer of the per-document values.
    let db = OxiDb::open_in_memory().unwrap();
    seed(&db, "t", 200);
    db.create_composite_index("t", vec!["kind".into(), "ts".into()])
        .unwrap();
    db.delete_limited("t", &json!({"kind": "k2"}), None)
        .unwrap();
    assert_eq!(db.count("t", &json!({"kind": "k2"})).unwrap(), 0);
    assert!(
        db.find("t", &json!({"kind": "k2", "ts": {"$gt": 0}}))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn a_large_ttl_backlog_is_evicted_completely() {
    // The TTL sweep now evicts in chunks, releasing the index write locks
    // between them, so a backlog cannot park the collection behind the
    // maintenance thread. Correctness requirement: chunking must not lose or
    // skip documents, including across the chunk boundary (the backlog here is
    // several chunks deep).
    let db = OxiDb::open_in_memory().unwrap();
    let n = 10_000i64;
    // All already expired: created_at is an ISO date well in the past, and the
    // TTL index is added afterwards — the "retroactive TTL on an existing
    // collection" case that produces a one-tick backlog.
    let docs: Vec<_> = (1..=n)
        .map(|i| json!({"id": i, "created_at": "2020-01-01T00:00:00Z", "kind": format!("k{}", i % 8)}))
        .collect();
    db.insert_many("sessions", docs).unwrap();
    db.create_index("sessions", "kind").unwrap();

    // `create_ttl_index` sweeps the already-expired set itself, synchronously,
    // on the caller's thread — so THIS call is the backlog path, not a later
    // maintenance tick. It runs through the same chunked `evict_ttl_indexed`.
    db.create_ttl_index("sessions", "created_at", 60).unwrap();

    assert_eq!(
        count(&db, "sessions"),
        0,
        "the whole backlog must go, not one chunk"
    );
    // And the other index must not be left holding entries for them.
    assert_eq!(db.count("sessions", &json!({"kind": "k1"})).unwrap(), 0);
    // A second sweep finds nothing and must not error.
    assert_eq!(db.evict_expired_now("sessions").unwrap(), 0);
}

#[test]
fn a_ttl_sweep_leaves_unexpired_documents_alone() {
    let db = OxiDb::open_in_memory().unwrap();
    let old: Vec<_> = (1..=5000)
        .map(|i| json!({"id": i, "created_at": "2020-01-01T00:00:00Z"}))
        .collect();
    let fresh: Vec<_> = (5001..=6000)
        .map(|i| json!({"id": i, "created_at": "2999-01-01T00:00:00Z"}))
        .collect();
    db.insert_many("sessions", old).unwrap();
    db.insert_many("sessions", fresh).unwrap();
    db.create_ttl_index("sessions", "created_at", 60).unwrap();

    // The expired 5000 went during index creation; the 1000 future-dated ones
    // must survive both that sweep and any later one.
    assert_eq!(count(&db, "sessions"), 1000);
    assert_eq!(db.evict_expired_now("sessions").unwrap(), 0);
    assert_eq!(count(&db, "sessions"), 1000);
}
