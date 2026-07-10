use oxidb_tsdb::{Agg, Point, QuerySpec, Tsdb};

fn total(db: &Tsdb, m: &str, field: &str) -> (usize, f64) {
    let spec = QuerySpec {
        measurement: m.into(),
        field: field.into(),
        tag_filters: vec![],
        start: i64::MIN / 2,
        end: i64::MAX / 2,
        group_tags: vec![],
        interval: None,
        agg: Agg::Sum,
    };
    let res = db.query(&spec);
    let sum = res.first().map(|r| r.points[0].value).unwrap_or(0.0);
    (db.point_count(), sum)
}

fn write_n(db: &mut Tsdb, base: i64, n: i64) {
    for i in 0..n {
        db.write(
            &Point::new("m", base + i * 1000)
                .tag("id", "x")
                .field("v", (i % 7) as f64),
        );
    }
}

#[test]
fn survives_reopen_after_checkpoint() {
    let dir = tempdir();
    let base = 1_700_000_000_000i64;
    {
        let mut db = Tsdb::open(&dir).unwrap().with_block_points(100);
        write_n(&mut db, base, 5000);
        let (n, s) = total(&db, "m", "v");
        assert_eq!(n, 5000);
        db.checkpoint().unwrap();
        // reference sum
        assert!(s > 0.0);
    }
    // Reopen — checkpoint snapshot must restore everything, no double count.
    let db = Tsdb::open(&dir).unwrap();
    let (n, _s) = total(&db, "m", "v");
    assert_eq!(n, 5000);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn survives_reopen_from_wal_without_checkpoint() {
    let dir = tempdir();
    let base = 1_700_000_000_000i64;
    let expected;
    {
        let mut db = Tsdb::open(&dir).unwrap().with_block_points(100);
        write_n(&mut db, base, 3333);
        expected = total(&db, "m", "v");
        // No explicit checkpoint — data lives in the WAL (+ in-memory blocks).
    }
    // Reopen replays the WAL; count + sum must match exactly (no double count).
    let db = Tsdb::open(&dir).unwrap();
    assert_eq!(total(&db, "m", "v"), expected);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn checkpoint_then_more_writes_then_reopen() {
    let dir = tempdir();
    let base = 1_700_000_000_000i64;
    {
        let mut db = Tsdb::open(&dir).unwrap().with_block_points(100);
        write_n(&mut db, base, 2000);
        db.checkpoint().unwrap(); // snapshot 2000
        write_n(&mut db, base + 2000 * 1000, 1500); // 1500 more, only in new WAL
    }
    let db = Tsdb::open(&dir).unwrap();
    assert_eq!(db.point_count(), 3500);
    std::fs::remove_dir_all(&dir).ok();
}

// Minimal unique temp dir without extra deps.
fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let mut p = std::env::temp_dir();
    p.push(format!(
        "oxidb-tsdb-test-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&p);
    p
}
