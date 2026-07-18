//! Online WAL checkpointing: bound the WAL without losing an acked write.
//!
//! The document engine used to leave its WAL to grow for the entire life of the
//! process, truncating only on a graceful shutdown. That was not caution for
//! its own sake — truncating after a snapshot really did lose acknowledged
//! writes, ~3 in 2000. A writer appends its WAL record *before* it updates the
//! B-tree, so a snapshot taken concurrently can miss a document whose record is
//! then erased. `sync_writes` carried the scar as a comment for months.
//!
//! `online_checkpoint` seals rather than truncates, and drains in-flight
//! writers for the instant of the seal, so the sealed segment is provably
//! covered by the snapshot that follows.
//!
//! **A graceful shutdown hides this bug.** `Drop` runs the final checkpoint,
//! which persists the in-memory tree — including the very write whose WAL
//! record was wrongly dropped. The loss is only observable if the process dies
//! without that flush, which is why the interesting test SIGKILLs a victim
//! (the same self-spawn pattern as `multi_collection_crash`) rather than
//! dropping the handle and reopening.
//!
//! Run: cargo test --test online_checkpoint
//!      cargo test --release --test online_checkpoint -- --ignored --nocapture

use std::collections::HashSet;
use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use oxidb::OxiDb;
use serde_json::json;
use tempfile::tempdir;

/// Size of a collection's live WAL. `OxiDb::open(dir)` puts collections
/// straight in `dir` — the `oxidb/` subdirectory only appears under the
/// server's multi-database layout. Getting this path wrong makes every
/// assertion below vacuously true, so it panics rather than defaulting to 0.
fn wal_bytes(dir: &std::path::Path, coll: &str) -> u64 {
    let p = dir.join(format!("{coll}.wal"));
    std::fs::metadata(&p)
        .unwrap_or_else(|e| panic!("no WAL at {}: {e}", p.display()))
        .len()
}

/// The WAL must come back down while the process is still running. Before this
/// existed it only ever went up.
#[test]
fn checkpoint_reclaims_the_wal_while_open() {
    unsafe { std::env::set_var("OXIDB_WAL_CHECKPOINT_BYTES", "65536") };
    let dir = tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();

    // Sample as we go: the engine's own sync thread also checkpoints, so
    // reading the size only at the end can miss the peak entirely — which is
    // itself the feature working.
    let mut peak = 0u64;
    for i in 0..3000 {
        db.insert("c", json!({"i": i, "pad": "x".repeat(200)}))
            .unwrap();
        peak = peak.max(wal_bytes(dir.path(), "c"));
    }
    assert!(
        peak > 65_536,
        "WAL never reached the threshold: {peak} bytes"
    );

    // The background thread calls exactly this; drive it directly so the test
    // isn't racing a timer.
    db.sync_all().unwrap();

    let after = wal_bytes(dir.path(), "c");
    assert!(
        after < peak / 2,
        "the WAL was not reclaimed: peaked at {peak}, still {after} bytes"
    );
    assert_eq!(
        db.count("c", &json!({})).unwrap(),
        3000,
        "data went missing"
    );
}

/// Sustained writes must not accumulate WAL. This is the whole point.
#[test]
fn wal_stays_bounded_under_sustained_writes() {
    unsafe { std::env::set_var("OXIDB_WAL_CHECKPOINT_BYTES", "65536") };
    let dir = tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();

    for round in 0..12 {
        for i in 0..500 {
            db.insert("c", json!({"r": round, "i": i, "pad": "q".repeat(200)}))
                .unwrap();
        }
        db.sync_all().unwrap();
        let wal = wal_bytes(dir.path(), "c");
        assert!(
            wal < 4 * 65_536,
            "round {round}: the WAL is running away — {wal} bytes"
        );
    }
    assert_eq!(db.count("c", &json!({})).unwrap(), 6000);
}

/// Turning it off must restore the old behaviour exactly: no seal, no bound.
///
/// Runs as its own process: the knob is read once per process into a
/// `OnceLock` — correct for a server, but it means the first test to touch it
/// fixes the value for every test in the binary.
#[test]
fn zero_disables_online_checkpointing() {
    if std::env::var("OXIDB_OC_ZERO").as_deref() == Ok("1") {
        let dir = tempdir().unwrap();
        let db = OxiDb::open(dir.path()).unwrap();
        for i in 0..2000 {
            db.insert("c", json!({"i": i, "pad": "x".repeat(200)}))
                .unwrap();
        }
        let before = wal_bytes(dir.path(), "c");
        assert!(before > 0, "nothing was written to the WAL");
        db.sync_all().unwrap();
        assert_eq!(
            wal_bytes(dir.path(), "c"),
            before,
            "with the knob at 0 the WAL must be left exactly alone"
        );
        return;
    }
    let out = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("zero_disables_online_checkpointing")
        .env("OXIDB_OC_ZERO", "1")
        .env("OXIDB_WAL_CHECKPOINT_BYTES", "0")
        .output()
        .expect("spawn");
    assert!(
        out.status.success(),
        "child failed:\n{}",
        String::from_utf8_lossy(&out.stdout)
    );
}

// ── the one that matters: acked writes vs SIGKILL, across many checkpoints ──

const VICTIM_WORKERS: usize = 4;

fn run_victim() -> ! {
    let path = std::env::var("OXIDB_OC_DATA").expect("victim: OXIDB_OC_DATA");
    unsafe { std::env::set_var("OXIDB_WAL_CHECKPOINT_BYTES", "32768") };
    let db = Arc::new(OxiDb::open(std::path::Path::new(&path)).expect("victim: open"));
    let out = Arc::new(Mutex::new(std::io::stdout()));
    let seq = Arc::new(AtomicU64::new(0));

    // Hammer the seal path against the writers — the race the design must win.
    {
        let db = Arc::clone(&db);
        thread::spawn(move || loop {
            let _ = db.sync_all();
            thread::sleep(Duration::from_millis(3));
        });
    }

    for w in 0..VICTIM_WORKERS {
        let db = Arc::clone(&db);
        let out = Arc::clone(&out);
        let seq = Arc::clone(&seq);
        thread::spawn(move || {
            loop {
                let uid = format!("w{w}-{}", seq.fetch_add(1, Ordering::Relaxed));
                // Only report an ack AFTER insert returns: that is the promise
                // recovery has to keep.
                if db
                    .insert("c", json!({"uid": uid, "pad": "y".repeat(120)}))
                    .is_ok()
                {
                    let mut o = out.lock().unwrap();
                    writeln!(o, "OK {uid}").unwrap();
                    o.flush().unwrap();
                }
            }
        });
    }
    loop {
        thread::sleep(Duration::from_secs(3600));
    }
}

/// Every write the engine acknowledged must survive a SIGKILL, no matter how
/// many online checkpoints ran underneath it. Losing 3 in 2000 is how the naive
/// version was caught, so this compares the full acked set — not a count.
#[test]
#[ignore = "spawns and SIGKILLs a victim; run with --ignored"]
#[cfg(unix)]
fn acked_writes_survive_sigkill_across_checkpoints() {
    if std::env::var("OXIDB_OC_VICTIM").as_deref() == Ok("1") {
        run_victim();
    }
    let rounds: usize = std::env::var("OC_ROUNDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5);
    let min_acks: usize = std::env::var("OC_MIN_ACKS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(400);

    let dir = tempdir().unwrap();
    let exe = std::env::current_exe().unwrap();
    let mut acked: HashSet<String> = HashSet::new();

    println!("\nonline checkpoint vs SIGKILL: {rounds} rounds, >={min_acks} acks each");

    for round in 0..rounds {
        let mut child = Command::new(&exe)
            .arg("--ignored")
            .arg("--nocapture")
            .arg("acked_writes_survive_sigkill_across_checkpoints")
            .env("OXIDB_OC_VICTIM", "1")
            .env("OXIDB_OC_DATA", dir.path())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn victim");

        let stdout = child.stdout.take().unwrap();
        let round_acks: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let reader_acks = Arc::clone(&round_acks);
        let done = Arc::new(AtomicBool::new(false));
        let reader_done = Arc::clone(&done);
        let reader = thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if let Some(uid) = line.strip_prefix("OK ") {
                    reader_acks.lock().unwrap().insert(uid.to_string());
                }
            }
            reader_done.store(true, Ordering::Relaxed);
        });

        let start = Instant::now();
        loop {
            if round_acks.lock().unwrap().len() >= min_acks {
                break;
            }
            assert!(
                start.elapsed() < Duration::from_secs(60),
                "round {round}: victim produced < {min_acks} acks in 60s"
            );
            thread::sleep(Duration::from_millis(20));
        }
        // Let a few more checkpoints land on top of the acks.
        thread::sleep(Duration::from_millis(150));

        // A graceful stop here would persist the in-memory state and hide
        // exactly the bug this test exists for.
        // Child::kill() is SIGKILL on Unix: no Drop, no final checkpoint, no
        // chance to flush the tree.
        let _ = child.kill();
        let _ = child.wait();
        let _ = done;
        let _ = reader.join();

        let round_set = round_acks.lock().unwrap().clone();
        acked.extend(round_set.iter().cloned());
        println!(
            "  round {round}: {} acks this round, {} total",
            round_set.len(),
            acked.len()
        );

        // Reopen over the crashed dir and demand every ack, ever.
        let db = OxiDb::open(dir.path()).unwrap();
        let found: HashSet<String> = db
            .find("c", &json!({}))
            .unwrap()
            .into_iter()
            .filter_map(|d| d.get("uid").and_then(|v| v.as_str()).map(str::to_string))
            .collect();
        let lost: Vec<&String> = acked.difference(&found).collect();
        assert!(
            lost.is_empty(),
            "round {round}: {} acknowledged writes lost after SIGKILL (e.g. {:?}) — \
             an online checkpoint erased a WAL record whose document was never \
             snapshotted",
            lost.len(),
            &lost[..lost.len().min(5)]
        );
        drop(db); // graceful: the next round starts from a clean checkpoint
    }
    println!("  {} acked writes, none lost", acked.len());
}

/// Prove the barrier is load-bearing, not decorative.
///
/// The window between a writer's WAL append and its B-tree apply is
/// microseconds wide, so a concurrency test never hits it by chance — remove
/// the barrier and `acked_writes_survive_sigkill_across_checkpoints` still
/// passes, which makes it evidence of nothing on this point. So widen the
/// window on purpose: stall a writer inside it, checkpoint underneath, and
/// kill the process before anything can flush the tree.
///
/// With the barrier, the checkpoint waits and the write survives. Without it,
/// the seal captures a record for a document the snapshot cannot contain, the
/// sealed segment is dropped as "covered", and the write is gone — an
/// acknowledged insert lost to a checkpoint.
#[test]
#[ignore = "spawns and SIGKILLs a victim; run with --ignored"]
#[cfg(unix)]
fn barrier_makes_the_stalled_writer_survive() {
    if std::env::var("OXIDB_OC_STALL_VICTIM").as_deref() == Ok("1") {
        let path = std::env::var("OXIDB_OC_DATA").unwrap();
        unsafe { std::env::set_var("OXIDB_WAL_CHECKPOINT_BYTES", "4096") };
        let db = Arc::new(OxiDb::open(std::path::Path::new(&path)).unwrap());

        // Enough traffic to push the WAL past the threshold so the next
        // checkpoint really seals.
        for i in 0..200 {
            db.insert(
                "c",
                json!({"uid": format!("warm-{i}"), "pad": "p".repeat(200)}),
            )
            .unwrap();
        }

        // One writer, stalled for two seconds between its WAL append and its
        // apply. Its insert is acknowledged only when it returns.
        oxidb::btree_collection::STALL_BEFORE_APPLY_MS.store(2000, Ordering::Relaxed);
        let w = {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                db.insert("c", json!({"uid": "STALLED", "pad": "s".repeat(200)}))
                    .unwrap();
                println!("OK STALLED");
                std::io::stdout().flush().unwrap();
            })
        };

        // Let it get into the window, then checkpoint on top of it.
        thread::sleep(Duration::from_millis(400));
        oxidb::btree_collection::STALL_BEFORE_APPLY_MS.store(0, Ordering::Relaxed);
        db.sync_all().unwrap();
        println!("CHECKPOINTED");
        std::io::stdout().flush().unwrap();

        w.join().unwrap();
        // Ack printed; now hang so the parent can kill us before any further
        // flush can rescue the write.
        loop {
            thread::sleep(Duration::from_secs(3600));
        }
    }

    let dir = tempdir().unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .arg("--ignored")
        .arg("--nocapture")
        .arg("barrier_makes_the_stalled_writer_survive")
        .env("OXIDB_OC_STALL_VICTIM", "1")
        .env("OXIDB_OC_DATA", dir.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn victim");

    let stdout = child.stdout.take().unwrap();
    let acked = Arc::new(AtomicBool::new(false));
    let seen = Arc::clone(&acked);
    let reader = thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            println!("    victim: {line}");
            if line.starts_with("OK STALLED") {
                seen.store(true, Ordering::Relaxed);
            }
        }
    });

    let start = Instant::now();
    while !acked.load(Ordering::Relaxed) {
        assert!(
            start.elapsed() < Duration::from_secs(60),
            "victim never acked the stalled write"
        );
        thread::sleep(Duration::from_millis(20));
    }
    // The write is acknowledged and a checkpoint has been through. Kill before
    // any later sync can quietly save it.
    let _ = child.kill();
    let _ = child.wait();
    let _ = reader.join();

    let db = OxiDb::open(dir.path()).unwrap();
    let found = db.find("c", &json!({"uid": "STALLED"})).unwrap().len();
    assert_eq!(
        found, 1,
        "the checkpoint erased an acknowledged write that was stalled between \
         its WAL append and its B-tree apply — this is exactly what apply_barrier \
         prevents, and it means the barrier is gone or ineffective"
    );
}

// ── sealed-segment lifecycle: the sentinel bug ─────────────────────────────
//
// The segment scanner's fast path probes `<wal>.0` to skip a directory scan.
// The first checkpoint used to DELETE `.0` — after which the scanner was
// blind: every later segment was never retired (unbounded growth, rediscovered
// as a mysteriously climbing "documents" disk bucket on the ColdChain demo)
// and never replayed at recovery (acked writes lost to a crash between a seal
// and its persist). `.0` is now truncated to an empty sentinel instead.

/// Segment files `<coll>.wal.N` present in the dir, sorted.
fn sealed_segments(dir: &std::path::Path, coll: &str) -> Vec<(u64, u64)> {
    let prefix = format!("{coll}.wal.");
    let mut out = Vec::new();
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        if let Some(seq) = name
            .strip_prefix(&prefix)
            .and_then(|s| s.parse::<u64>().ok())
        {
            out.push((seq, e.metadata().unwrap().len()));
        }
    }
    out.sort_unstable();
    out
}

/// Round after round, sealed segments must be retired — not only the first.
/// Before the fix this left `.1`, `.2`, `.3`, … behind forever.
#[test]
fn every_checkpoint_retires_its_sealed_segment_not_only_the_first() {
    unsafe { std::env::set_var("OXIDB_WAL_CHECKPOINT_BYTES", "65536") };
    let dir = tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();

    for round in 0..4 {
        for i in 0..600 {
            db.insert("c", json!({"r": round, "i": i, "pad": "s".repeat(200)}))
                .unwrap();
        }
        db.sync_all().unwrap();
        let segs = sealed_segments(dir.path(), "c");
        assert!(
            segs.iter().all(|&(seq, len)| seq == 0 && len == 0),
            "round {round}: sealed segments survived the checkpoint: {segs:?} \
             (only an EMPTY `.0` sentinel may remain)"
        );
    }
    // The sentinel itself must exist by now (at least one seal happened) and
    // must not confuse recovery.
    assert_eq!(sealed_segments(dir.path(), "c"), vec![(0, 0)]);
    drop(db);
    let db = OxiDb::open(dir.path()).unwrap();
    assert_eq!(
        db.count("c", &json!({})).unwrap(),
        2400,
        "data lost across reopen"
    );
}

/// An orphaned segment from a data dir the old bug left behind (first
/// surviving segment `.1`, no `.0` beside it) must be seen again — replayed
/// at open, retired at the next checkpoint.
#[test]
fn legacy_orphan_segments_are_retired_by_the_next_checkpoint() {
    unsafe { std::env::set_var("OXIDB_WAL_CHECKPOINT_BYTES", "65536") };
    let dir = tempdir().unwrap();
    {
        let db = OxiDb::open(dir.path()).unwrap();
        db.insert("c", json!({"seed": true})).unwrap();
        drop(db);
    }
    // The shape the leak produced: a (covered, empty-after-truncate-at-
    // shutdown… here simply empty) orphan at `.1` with no sentinel.
    std::fs::write(dir.path().join("c.wal.1"), b"").unwrap();

    let db = OxiDb::open(dir.path()).unwrap();
    for i in 0..600 {
        db.insert("c", json!({"i": i, "pad": "l".repeat(200)}))
            .unwrap();
    }
    db.sync_all().unwrap();
    assert_eq!(
        sealed_segments(dir.path(), "c"),
        vec![(0, 0)],
        "the legacy orphan must be retired and replaced by the `.0` sentinel"
    );
    assert_eq!(db.count("c", &json!({})).unwrap(), 601);
}

/// The crash the blindness actually loses data to: seal a segment beyond the
/// first, die before the persist, reopen. The victim builds the exact
/// on-disk state (checkpoint once → sentinel exists; write a second batch;
/// seal it file-level — the same rename `Wal::seal` does; exit with no Drop).
/// Recovery must replay the sealed batch. Before the fix it silently did not.
#[test]
fn a_sealed_segment_beyond_the_first_is_replayed_at_recovery() {
    if std::env::var("OXIDB_OC_SEAL_VICTIM").as_deref() == Ok("1") {
        let dir = std::path::PathBuf::from(std::env::var("OXIDB_OC_DIR").unwrap());
        let db = OxiDb::open(&dir).unwrap();
        for i in 0..600 {
            db.insert("c", json!({"batch": 1, "i": i, "pad": "v".repeat(200)}))
                .unwrap();
        }
        db.sync_all().unwrap(); // checkpoint #1: `.0` sentinel now exists
        for i in 0..50 {
            db.insert("c", json!({"batch": 2, "i": i})).unwrap(); // stays in live WAL
        }
        // Seal exactly as `Wal::seal` would: rename the live WAL to the next
        // segment, leave a fresh empty one. Then die with no Drop — the
        // persist that would cover the segment never happens.
        std::fs::rename(dir.join("c.wal"), dir.join("c.wal.1")).unwrap();
        std::fs::write(dir.join("c.wal"), b"").unwrap();
        std::process::exit(0);
    }

    let dir = tempdir().unwrap();
    let out = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("a_sealed_segment_beyond_the_first_is_replayed_at_recovery")
        .env("OXIDB_OC_SEAL_VICTIM", "1")
        .env("OXIDB_OC_DIR", dir.path())
        .env("OXIDB_WAL_CHECKPOINT_BYTES", "65536")
        .output()
        .expect("spawn victim");
    assert!(
        out.status.success(),
        "victim failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        dir.path().join("c.wal.1").exists(),
        "test setup broke: no sealed segment left behind"
    );

    let db = OxiDb::open(dir.path()).unwrap();
    assert_eq!(
        db.count("c", &json!({"batch": 2})).unwrap(),
        50,
        "the sealed-but-unpersisted batch was not replayed at recovery — \
         these were acknowledged writes"
    );
    assert_eq!(db.count("c", &json!({"batch": 1})).unwrap(), 600);
}
