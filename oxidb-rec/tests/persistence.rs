//! ADR-0025 §6: MANIFEST + generation snapshot + WAL. Recovery must be
//! snapshot + replay with idempotent re-application, torn tails skipped,
//! and the MANIFEST as the only authority.

use oxidb_rec::{Query, Rec, RecConfig, Scoring};

const DAY: u64 = 24 * 3600;

fn cfg() -> RecConfig {
    RecConfig {
        bucket_secs: 30 * DAY,
        max_basket: 50,
        checkpoint_bytes: 0, // manual only, unless a test says otherwise
    }
}

fn count_q() -> Query {
    Query {
        scoring: Scoring::Count,
        ..Query::default()
    }
}

fn top(rec: &Rec, item: &str) -> Vec<(String, f64)> {
    rec.related("p", item, 0, &count_q())
        .unwrap()
        .into_iter()
        .map(|r| (r.item, r.score))
        .collect()
}

#[test]
fn wal_only_state_survives_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut rec = Rec::open(dir.path(), cfg()).unwrap();
        rec.track("p", 1, &["kahve", "süt"], 0);
        rec.track("p", 2, &["kahve", "süt"], 0);
        rec.track("p", 3, &["kahve", "filtre"], 0);
        // No checkpoint: everything lives in wal.0.
    }
    let rec = Rec::open(dir.path(), cfg()).unwrap();
    assert_eq!(
        top(&rec, "kahve"),
        vec![("süt".into(), 2.0), ("filtre".into(), 1.0)]
    );
    assert_eq!(rec.stats()["models"]["p"]["baskets"], 3);
}

#[test]
fn checkpoint_plus_tail_recovers_both_layers_exactly_once() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut rec = Rec::open(dir.path(), cfg()).unwrap();
        rec.track("p", 1, &["a", "b"], 0);
        rec.checkpoint(0).unwrap();
        rec.track("p", 2, &["a", "b"], 0); // tail in wal.1
    }
    let rec = Rec::open(dir.path(), cfg()).unwrap();
    assert_eq!(top(&rec, "a"), vec![("b".into(), 2.0)]);
    assert_eq!(rec.stats()["generation"], 1);

    // And a second reopen (replaying the same tail again) changes nothing:
    // the seen-set arrived inside the snapshot + replay path.
    drop(rec);
    let rec = Rec::open(dir.path(), cfg()).unwrap();
    assert_eq!(top(&rec, "a"), vec![("b".into(), 2.0)]);
    assert_eq!(rec.stats()["models"]["p"]["baskets"], 2);
}

/// The idempotence claim at the WAL level, made adversarial: duplicate the
/// entire WAL tail on disk — every record now appears twice — and recovery
/// must still count each basket once.
#[test]
fn a_doubled_wal_replays_to_the_same_state() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut rec = Rec::open(dir.path(), cfg()).unwrap();
        rec.track("p", 1, &["a", "b"], 0);
        rec.track("p", 2, &["a", "c"], 0);
    }
    let wal = dir.path().join("wal.0.log");
    let body = std::fs::read(&wal).unwrap();
    let mut doubled = body.clone();
    doubled.extend_from_slice(&body);
    std::fs::write(&wal, doubled).unwrap();

    let rec = Rec::open(dir.path(), cfg()).unwrap();
    assert_eq!(rec.stats()["models"]["p"]["baskets"], 2, "each basket once");
    assert_eq!(top(&rec, "a"), vec![("b".into(), 1.0), ("c".into(), 1.0)]);
}

#[test]
fn a_torn_wal_tail_is_skipped_not_fatal() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut rec = Rec::open(dir.path(), cfg()).unwrap();
        rec.track("p", 1, &["a", "b"], 0);
        rec.track("p", 2, &["a", "c"], 0);
    }
    let wal = dir.path().join("wal.0.log");
    let mut body = std::fs::read(&wal).unwrap();
    body.truncate(body.len() - 7); // tear the last record mid-JSON
    std::fs::write(&wal, body).unwrap();

    let rec = Rec::open(dir.path(), cfg()).unwrap();
    assert_eq!(
        rec.stats()["models"]["p"]["baskets"],
        1,
        "torn record dropped"
    );
    assert_eq!(top(&rec, "a"), vec![("b".into(), 1.0)]);
}

/// Crash between the snapshot write and the MANIFEST flip: the orphaned
/// next-generation snapshot must be ignored (manifest is the authority) and
/// swept, and the previous generation must answer.
#[test]
fn an_orphaned_next_generation_is_ignored_and_swept() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut rec = Rec::open(dir.path(), cfg()).unwrap();
        rec.track("p", 1, &["a", "b"], 0);
    }
    // Fake the crash artifact: a snap.1 that no MANIFEST ever named.
    std::fs::write(dir.path().join("snap.1.rec"), b"{ not even json").unwrap();
    std::fs::write(dir.path().join("wal.1.log"), b"").unwrap();

    let rec = Rec::open(dir.path(), cfg()).unwrap();
    assert_eq!(top(&rec, "a"), vec![("b".into(), 1.0)]);
    assert!(
        !dir.path().join("snap.1.rec").exists(),
        "the orphan must be swept, or one pair accumulates per crash"
    );
}

#[test]
fn reopening_with_a_different_bucket_width_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut rec = Rec::open(dir.path(), cfg()).unwrap();
        rec.track("p", 1, &["a", "b"], 0);
        rec.checkpoint(0).unwrap();
    }
    let narrower = RecConfig {
        bucket_secs: 7 * DAY,
        ..cfg()
    };
    let err = match Rec::open(dir.path(), narrower) {
        Ok(_) => panic!("a different bucket width must be refused"),
        Err(e) => e,
    };
    assert!(
        err.to_string().contains("bucket_secs"),
        "the refusal must name the knob: {err}"
    );
}

#[test]
fn auto_checkpoint_bounds_the_wal() {
    let dir = tempfile::tempdir().unwrap();
    let mut rec = Rec::open(
        dir.path(),
        RecConfig {
            checkpoint_bytes: 512,
            ..cfg()
        },
    )
    .unwrap();
    for i in 0..100u64 {
        rec.track("p", i, &["a", &format!("i{i}")], 0);
    }
    let generation = rec.stats()["generation"].as_u64().unwrap();
    assert!(
        generation >= 2,
        "the WAL never folded (generation {generation})"
    );
    let wal_bytes = rec.stats()["wal_bytes"].as_u64().unwrap();
    assert!(wal_bytes < 4 * 512, "replay stays bounded");

    // State intact across the folds and a reopen.
    drop(rec);
    let rec = Rec::open(
        dir.path(),
        RecConfig {
            checkpoint_bytes: 512,
            ..cfg()
        },
    )
    .unwrap();
    assert_eq!(rec.stats()["models"]["p"]["baskets"], 100);
}

/// gc runs inside checkpoint: rows expired by the lazy shift never reach the
/// snapshot, so the file shrinks as the window moves.
#[test]
fn checkpoint_drops_expired_rows_from_the_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let mut rec = Rec::open(dir.path(), cfg()).unwrap();
    for i in 0..50u64 {
        rec.track("p", i, &[&format!("eski{i}"), "moda"], 0);
    }
    rec.checkpoint(0).unwrap();
    let full = std::fs::metadata(dir.path().join("snap.1.rec"))
        .unwrap()
        .len();

    let later = (oxidb_rec::BUCKETS as u64 + 1) * 30 * DAY;
    rec.track("p", 1000, &["yeni", "moda"], later);
    rec.checkpoint(later).unwrap();
    let shrunk = std::fs::metadata(dir.path().join("snap.2.rec"))
        .unwrap()
        .len();
    assert!(
        shrunk < full / 4,
        "expired rows persisted: {shrunk} bytes vs {full} before expiry"
    );
}
