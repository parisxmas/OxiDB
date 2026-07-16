//! Engine-aware TSDB backup / restore: a consistent `.tar.gz` of the data
//! directory that restores to an identical database.

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
    let sum = db
        .query(&spec)
        .first()
        .map(|r| r.points[0].value)
        .unwrap_or(0.0);
    (db.point_count(), sum)
}

fn write_n(db: &mut Tsdb, base: i64, n: i64) {
    for i in 0..n {
        db.write(
            &Point::new("m", base + i * 1000)
                .tag("host", "a")
                .field("v", (i % 7) as f64),
        );
    }
}

fn tmp(name: &str) -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let mut p = std::env::temp_dir();
    p.push(format!(
        "oxidb-tsdb-backup-{}-{}-{name}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&p);
    p
}

#[test]
fn backup_then_restore_roundtrips() {
    let base = 1_700_000_000_000i64;
    let src = tmp("src");
    let archive = tmp("arc").join("tsdb.tar.gz");

    let (n_before, sum_before) = {
        let mut db = Tsdb::open(&src).unwrap();
        write_n(&mut db, base, 3000);
        // Some points stay in the WAL only (no explicit checkpoint) — the
        // backup's own checkpoint must fold them into the archive.
        let stats = total(&db, "m", "v");
        let size = db.backup(&archive).unwrap();
        assert!(size > 0);
        assert!(archive.exists());
        stats
    };

    let dst = tmp("dst");
    Tsdb::restore(&archive, &dst).unwrap();
    let db = Tsdb::open(&dst).unwrap();

    let (n_after, sum_after) = total(&db, "m", "v");
    assert_eq!(n_after, n_before, "point count preserved across backup");
    assert_eq!(sum_after, sum_before, "values preserved across backup");
}

#[test]
fn in_memory_backup_errors_and_target_guards() {
    // In-memory (no dir) has nothing to back up.
    let mut mem = Tsdb::new();
    assert!(mem.backup(&tmp("x").join("b.tar.gz")).is_err());

    // A real backup then guard checks.
    let src = tmp("src2");
    let archive = tmp("arc2").join("b.tar.gz");
    let mut db = Tsdb::open(&src).unwrap();
    db.write(&Point::new("m", 1).field("v", 1.0));
    db.backup(&archive).unwrap();
    // No overwrite.
    assert!(db.backup(&archive).is_err());
    // Restore into a non-empty dir is refused.
    let busy = tmp("busy");
    std::fs::create_dir_all(&busy).unwrap();
    std::fs::write(busy.join("stray"), b"x").unwrap();
    assert!(Tsdb::restore(&archive, &busy).is_err());
}
