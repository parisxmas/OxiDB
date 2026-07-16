//! Engine-aware TSDB backup / restore: a consistent `.tar.gz` of the data
//! directory that restores to an identical database.

use oxidb_tsdb::{Agg, Point, QuerySpec, TagPredicate, Tsdb};

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

/// Low-lock backup under concurrent writes: 10 threads append points while the
/// backup compresses with the engine lock released, and their writes cross the
/// auto-checkpoint threshold mid-backup — the pinned generation survives GC and
/// each restored snapshot is a consistent per-writer prefix (COUNT == MAX(v),
/// so no gaps).
#[test]
fn low_lock_backup_under_concurrent_writes() {
    use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
    use std::sync::{Arc, RwLock};

    // one-field count/max for a single writer's series
    fn agg_of(db: &Tsdb, w: usize, agg: Agg) -> i64 {
        let spec = QuerySpec {
            measurement: "m".into(),
            field: "v".into(),
            tag_filters: vec![TagPredicate {
                key: "w".into(),
                value: w.to_string(),
            }],
            start: i64::MIN / 2,
            end: i64::MAX / 2,
            group_tags: vec![],
            interval: None,
            agg,
        };
        db.query(&spec)
            .first()
            .map(|r| r.points[0].value as i64)
            .unwrap_or(0)
    }
    fn check_no_gaps(db: &Tsdb, nworkers: usize, label: &str) -> i64 {
        let mut total = 0;
        for w in 0..nworkers {
            let cnt = agg_of(db, w, Agg::Count);
            if cnt == 0 {
                continue;
            }
            let mx = agg_of(db, w, Agg::Max);
            assert_eq!(
                mx, cnt,
                "{label}: worker {w} MAX(v)={mx} != COUNT={cnt} — GAP!"
            );
            total += cnt;
        }
        total
    }

    const NW: usize = 10;
    let src = tmp("cc-src");
    // Low checkpoint threshold so the concurrent writers auto-checkpoint
    // *during* the backup, exercising pin-survives-GC.
    let engine = Arc::new(RwLock::new(
        Tsdb::open(&src).unwrap().with_checkpoint_bytes(16 * 1024),
    ));
    engine.write().unwrap().checkpoint().unwrap(); // a committed generation exists

    let stop = Arc::new(AtomicBool::new(false));
    let counts: Arc<Vec<AtomicI64>> = Arc::new((0..NW).map(|_| AtomicI64::new(0)).collect());
    let mut handles = Vec::new();
    for w in 0..NW {
        let (engine, stop, counts) = (engine.clone(), stop.clone(), counts.clone());
        handles.push(std::thread::spawn(move || {
            let mut n = 0i64;
            while !stop.load(Ordering::Relaxed) {
                n += 1;
                let ts = 1_700_000_000_000 + (w as i64) * 1_000_000 + n;
                engine.write().unwrap().write(
                    &Point::new("m", ts)
                        .tag("w", &w.to_string())
                        .field("v", n as f64),
                );
                counts[w].store(n, Ordering::Relaxed);
            }
        }));
    }

    // Take backups + restores while the load runs — pin under the write lock,
    // compress with it released, unpin under the write lock.
    std::thread::sleep(std::time::Duration::from_millis(300));
    let mut snapshots = Vec::new();
    for i in 0..3 {
        let arc = tmp(&format!("cc-arc{i}")).join(format!("bk{i}.tar.gz"));
        let plan = engine.write().unwrap().backup_begin().unwrap();
        let size = Tsdb::backup_write(&plan, &arc).unwrap(); // no lock held
        engine.write().unwrap().backup_end(&plan);
        assert!(size > 0);
        snapshots.push(arc);
        std::thread::sleep(std::time::Duration::from_millis(200));
    }

    std::thread::sleep(std::time::Duration::from_millis(200));
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
    let recorded: i64 = counts.iter().map(|a| a.load(Ordering::Relaxed)).sum();
    assert!(recorded > 0, "workers made no progress");

    // Live database: consistent + complete.
    let live = check_no_gaps(&engine.read().unwrap(), NW, "LIVE");
    assert_eq!(live, recorded, "live total {live} != recorded {recorded}");

    // Every backup taken under load restores to a consistent snapshot.
    for (i, arc) in snapshots.iter().enumerate() {
        let dst = tmp(&format!("cc-dst{i}"));
        Tsdb::restore(arc, &dst).unwrap();
        let rdb = Tsdb::open(&dst).unwrap();
        let snap = check_no_gaps(&rdb, NW, &format!("RESTORE#{i}"));
        assert!(
            snap > 0 && snap <= recorded,
            "restore#{i}: {snap} rows vs {recorded}"
        );
    }
}
