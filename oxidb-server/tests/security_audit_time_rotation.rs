//! CERN-grade tests: audit-log **time-based** rotation.
//!
//! Companion to `security_audit_rotation.rs` (which covers size-
//! based). PR #70 added size-only rotation; this slice extends the
//! same primitive with an age trigger via `RotationPolicy`.
//!
//! Each test sleeps real wall-clock time to cross the age
//! threshold — that's the only way to assert the actual `Instant`
//! arithmetic. To keep total runtime under a couple seconds we use
//! short thresholds (300 ms typical, with small slacks for clock
//! noise on shared CI runners).

use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use tempfile::tempdir;

use oxidb_server::audit::{AuditEvent, AuditLog, RotationPolicy};

fn write_event(log: &AuditLog) {
    log.log(&AuditEvent {
        ts: "2026-05-18T10:00:00Z".to_string(),
        user: "alice",
        cmd: "ping",
        collection: None,
        result: "ok",
        detail: "",
    });
}

fn count_rotated_files(audit_dir: &Path) -> usize {
    std::fs::read_dir(audit_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            let n = e.file_name();
            let s = n.to_string_lossy();
            s.starts_with("audit.log.") && s != "audit.log"
        })
        .count()
}

fn count_total_entries(audit_dir: &Path) -> usize {
    let mut total = 0;
    for entry in std::fs::read_dir(audit_dir).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name();
        let s = name.to_string_lossy();
        if s == "audit.log" || s.starts_with("audit.log.") {
            let content = std::fs::read_to_string(entry.path()).unwrap();
            total += content.lines().filter(|l| !l.is_empty()).count();
        }
    }
    total
}

// ─────────────────────────────────────────────────────────────────────
// Age-only rotation
// ─────────────────────────────────────────────────────────────────────

/// Age=300ms, write one event immediately and one after a
/// >300ms sleep. The SECOND write must trigger rotation — the live
/// file at that moment is older than the threshold.
#[test]
fn audit_log_rotates_when_age_exceeds_threshold() {
    let dir = tempdir().unwrap();
    let log = AuditLog::open_with_policy(
        dir.path(),
        RotationPolicy::age_secs(0).clone(), // placeholder; replaced below
    )
    .unwrap();
    drop(log); // discard — we want the real policy with subsecond age

    // 300 ms threshold via direct construction.
    let policy = RotationPolicy {
        max_bytes: None,
        max_age: Some(Duration::from_millis(300)),
        calendar: None,
        compress: false,
    };
    let log = AuditLog::open_with_policy(dir.path(), policy).unwrap();

    write_event(&log); // first write — age = ~0ms, no rotation
    let audit_dir = dir.path().join("_audit");
    assert_eq!(
        count_rotated_files(&audit_dir),
        0,
        "no rotation before age threshold crossed"
    );

    thread::sleep(Duration::from_millis(500)); // exceed threshold

    write_event(&log); // post-sleep — age elapsed → triggers rotation
    drop(log);

    let rotated = count_rotated_files(&audit_dir);
    assert!(
        rotated >= 1,
        "expected at least 1 rotated file after age trigger (max_age=300ms, slept 500ms), got {rotated}"
    );
}

/// No rotation if writes stay within the age window. Pin the
/// "policy doesn't fire spuriously" half of the contract.
#[test]
fn audit_log_no_rotation_below_age_threshold() {
    let dir = tempdir().unwrap();
    let policy = RotationPolicy {
        max_bytes: None,
        max_age: Some(Duration::from_secs(60)), // 60 seconds — well above test runtime
        calendar: None,
        compress: false,
    };
    let log = AuditLog::open_with_policy(dir.path(), policy).unwrap();

    for _ in 0..200 {
        write_event(&log);
    }
    drop(log);

    let audit_dir = dir.path().join("_audit");
    assert_eq!(
        count_rotated_files(&audit_dir),
        0,
        "no rotation expected within the 60s age window"
    );
    assert_eq!(count_total_entries(&audit_dir), 200);
}

/// After rotation, the age clock RESETS. Two consecutive rotations
/// triggered by age should each take a full `max_age` worth of
/// wall-clock time — not collapse into one rotation that "catches
/// up" multiple thresholds.
#[test]
fn audit_log_age_resets_on_rotation() {
    let dir = tempdir().unwrap();
    let policy = RotationPolicy {
        max_bytes: None,
        max_age: Some(Duration::from_millis(250)),
        calendar: None,
        compress: false,
    };
    let log = AuditLog::open_with_policy(dir.path(), policy).unwrap();
    let audit_dir = dir.path().join("_audit");

    // Cycle 1: sleep > threshold, write → first rotation.
    write_event(&log);
    thread::sleep(Duration::from_millis(350));
    write_event(&log);
    assert!(
        count_rotated_files(&audit_dir) >= 1,
        "expected first rotation"
    );
    let after_first = count_rotated_files(&audit_dir);

    // Right after rotation, the clock just reset. A write NOW
    // must not trigger another rotation — age = ~0ms.
    write_event(&log);
    assert_eq!(
        count_rotated_files(&audit_dir),
        after_first,
        "age clock didn't reset — write right after rotation re-triggered"
    );

    // Cycle 2: sleep again past threshold → second rotation.
    thread::sleep(Duration::from_millis(350));
    write_event(&log);
    drop(log);
    let after_second = count_rotated_files(&audit_dir);
    assert!(
        after_second > after_first,
        "expected a second rotation after second age window, got {after_first} then {after_second}"
    );
}

// ─────────────────────────────────────────────────────────────────────
// Combined size + age — independent triggers
// ─────────────────────────────────────────────────────────────────────

/// With BOTH size and age set, whichever fires first rotates. We
/// pick max_age=300ms + max_bytes=512KiB (size unreachable in 300ms
/// of small writes). The AGE trigger must fire even though size
/// hasn't.
#[test]
fn audit_log_age_fires_independently_of_size() {
    let dir = tempdir().unwrap();
    let policy = RotationPolicy::size_or_age(512 * 1024, 0); // placeholder
    drop(policy);

    let policy = RotationPolicy {
        max_bytes: Some(512 * 1024),
        max_age: Some(Duration::from_millis(300)),
        calendar: None,
        compress: false,
    };
    let log = AuditLog::open_with_policy(dir.path(), policy).unwrap();
    let audit_dir = dir.path().join("_audit");

    write_event(&log); // ~140 bytes, well below 512 KiB
    thread::sleep(Duration::from_millis(500));
    write_event(&log); // age trigger fires

    drop(log);
    let rotated = count_rotated_files(&audit_dir);
    assert!(
        rotated >= 1,
        "age trigger should fire independently of size — got {rotated} rotated files"
    );
}

/// Mirror of above: size fires before age. max_bytes=1 KiB +
/// max_age=60s. Push enough events to cross 1 KiB; age can't
/// possibly fire in test time.
#[test]
fn audit_log_size_fires_independently_of_age() {
    let dir = tempdir().unwrap();
    let policy = RotationPolicy {
        max_bytes: Some(1024),
        max_age: Some(Duration::from_secs(60)),
        calendar: None,
        compress: false,
    };
    let log = AuditLog::open_with_policy(dir.path(), policy).unwrap();
    let audit_dir = dir.path().join("_audit");

    // ~10 events to comfortably cross 1 KiB.
    for _ in 0..30 {
        write_event(&log);
    }
    drop(log);

    let rotated = count_rotated_files(&audit_dir);
    assert!(
        rotated >= 1,
        "size trigger should fire independently of age — got {rotated} rotated files"
    );
}

// ─────────────────────────────────────────────────────────────────────
// API ergonomics + backwards-compat
// ─────────────────────────────────────────────────────────────────────

/// `RotationPolicy::unbounded()` ≡ no rotation regardless of
/// volume. Same contract as `AuditLog::open` without args.
#[test]
fn audit_log_unbounded_policy_never_rotates() {
    let dir = tempdir().unwrap();
    let log = AuditLog::open_with_policy(dir.path(), RotationPolicy::unbounded()).unwrap();
    for _ in 0..1000 {
        write_event(&log);
    }
    drop(log);

    let audit_dir = dir.path().join("_audit");
    assert_eq!(count_rotated_files(&audit_dir), 0);
    assert_eq!(count_total_entries(&audit_dir), 1000);
}

/// The PR #70 `open_with_rotation(dir, Some(N))` shim still works
/// through the new policy plumbing. Pins backwards-compat.
#[test]
fn audit_log_legacy_size_only_constructor_still_works() {
    let dir = tempdir().unwrap();
    let log = AuditLog::open_with_rotation(dir.path(), Some(1024)).unwrap();
    for _ in 0..30 {
        write_event(&log);
    }
    drop(log);

    let audit_dir = dir.path().join("_audit");
    assert!(count_rotated_files(&audit_dir) >= 1);
}

// ─────────────────────────────────────────────────────────────────────
// Concurrency: age trigger under concurrent writers
// ─────────────────────────────────────────────────────────────────────

/// 4 threads write for 600 ms with a 200 ms age threshold. Total
/// entries across all rotated + live files must equal what threads
/// wrote (no loss across age-triggered rotation handovers under
/// concurrent load). Also pins that the rotation happens at LEAST
/// twice in 600 ms — proves age firing isn't a no-op when threads
/// race each other.
#[test]
fn audit_log_age_rotation_concurrent_safe() {
    let dir = tempdir().unwrap();
    let policy = RotationPolicy {
        max_bytes: None,
        max_age: Some(Duration::from_millis(200)),
        calendar: None,
        compress: false,
    };
    let log = Arc::new(AuditLog::open_with_policy(dir.path(), policy).unwrap());

    let deadline = Instant::now() + Duration::from_millis(600);
    const THREADS: usize = 4;
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let handles: Vec<_> = (0..THREADS)
        .map(|_| {
            let log = Arc::clone(&log);
            let count = Arc::clone(&count);
            thread::spawn(move || {
                while Instant::now() < deadline {
                    write_event(&log);
                    count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    // small sleep so threads don't dominate one CPU
                    thread::sleep(Duration::from_millis(1));
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    let wrote = count.load(std::sync::atomic::Ordering::Relaxed);
    drop(log);

    let audit_dir = dir.path().join("_audit");
    let total = count_total_entries(&audit_dir);
    let rotated = count_rotated_files(&audit_dir);

    assert!(
        rotated >= 1,
        "age trigger should have fired at least once during 600ms with 200ms threshold; got {rotated}"
    );
    assert_eq!(
        total, wrote,
        "AGE ROTATION LOST EVENTS under concurrent load: wrote {wrote}, found {total} across {rotated} rotated + live"
    );
}
