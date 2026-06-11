//! CERN-grade tests: audit-log size-based rotation.
//!
//! Companion to `security_audit_evidence.rs`. That corpus pins the
//! per-vector evidence SHAPE. This one pins the operational
//! behaviour of the rotation primitive added alongside in PR #70:
//!
//!   - Default (legacy) constructor produces unbounded growth —
//!     no rotated files appear even after many writes.
//!   - Size-bounded constructor rotates atomically when the
//!     threshold is crossed.
//!   - Across many rotations, EVERY logged event is preserved
//!     (zero loss across rotated files).
//!   - Rotation is concurrency-safe — N threads writing while the
//!     rotator runs do not corrupt or lose entries.
//!
//! Why a primitive feature lands alongside its tests: PR #69's
//! footer flagged "audit-log rotation tests" as xs/pending, but
//! the feature itself was also missing — testing a non-existent
//! capability would have been a no-op. The primitive is ~80 lines
//! in `audit.rs`; these tests are the contract it commits to.

use std::path::Path;
use std::sync::Arc;
use std::thread;

use tempfile::tempdir;

use oxidb_server::audit::{AuditEvent, AuditLog};

fn write_event(log: &AuditLog, cmd: &str) {
    log.log(&AuditEvent {
        ts: "2026-05-18T10:00:00Z".to_string(),
        user: "alice",
        cmd,
        collection: Some("orders"),
        result: "denied",
        detail: "",
    });
}

/// Count files in the audit dir matching `audit.log.*` (the
/// rotated-files pattern).
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

/// Sum entries across audit.log + every audit.log.* file.
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
// Legacy unbounded behaviour — backwards-compat contract.
// ─────────────────────────────────────────────────────────────────────

/// `AuditLog::open` (no rotation arg) MUST behave exactly like the
/// pre-rotation version: a single file, unbounded growth, no
/// rotated `.N` files appear. Pins backwards compat for callers
/// in `main.rs` that don't opt into rotation.
#[test]
fn audit_log_unbounded_by_default() {
    let dir = tempdir().unwrap();
    let log = AuditLog::open(dir.path()).unwrap();
    for i in 0..5_000 {
        write_event(&log, &format!("cmd_{i}"));
    }
    drop(log);

    let audit_dir = dir.path().join("_audit");
    assert_eq!(
        count_rotated_files(&audit_dir),
        0,
        "legacy AuditLog::open must NOT rotate — caller didn't opt in"
    );
    let entries = count_total_entries(&audit_dir);
    assert_eq!(
        entries, 5_000,
        "all 5000 entries must be in the single audit.log"
    );
}

// ─────────────────────────────────────────────────────────────────────
// Size-bounded rotation — happy path.
// ─────────────────────────────────────────────────────────────────────

/// With a small max_bytes, after enough writes the live file is
/// rotated and a new fresh `audit.log` takes over. Verify a
/// rotated file appears AND the new live file is small (< max).
#[test]
fn audit_log_rotates_when_exceeds_size_threshold() {
    let dir = tempdir().unwrap();
    // 4 KiB — small enough that ~30 events at ~140 bytes each will trigger it.
    let max = 4096u64;
    let log = AuditLog::open_with_rotation(dir.path(), Some(max)).unwrap();

    for i in 0..200 {
        write_event(&log, &format!("cmd_{i}"));
    }
    drop(log);

    let audit_dir = dir.path().join("_audit");
    let rotated = count_rotated_files(&audit_dir);
    assert!(
        rotated >= 1,
        "expected at least 1 rotated file after writing past the {max}-byte threshold, got {rotated}"
    );

    // The current live `audit.log` should be small (it just started).
    let live_path = audit_dir.join("audit.log");
    let live_size = std::fs::metadata(&live_path).unwrap().len();
    assert!(
        live_size < max,
        "live audit.log size {live_size} should be < threshold {max} after rotation"
    );
}

/// Across N rotations, the SUM of entries across audit.log + all
/// rotated files MUST equal the number of events written. Catches
/// "rotation drops the last entry in the old file" or "first
/// entry in the new file" off-by-one bugs.
#[test]
fn audit_log_rotation_preserves_all_entries() {
    let dir = tempdir().unwrap();
    let max = 2048u64; // tighter — forces ~10 rotations for 1000 entries
    let log = AuditLog::open_with_rotation(dir.path(), Some(max)).unwrap();

    const N: usize = 1_000;
    for i in 0..N {
        write_event(&log, &format!("cmd_{i:04}"));
    }
    drop(log);

    let audit_dir = dir.path().join("_audit");
    let rotated = count_rotated_files(&audit_dir);
    assert!(
        rotated >= 2,
        "expected multiple rotations with max={max} and {N} entries, got {rotated}"
    );

    let total = count_total_entries(&audit_dir);
    assert_eq!(
        total, N,
        "ROTATION DROPPED ENTRIES: wrote {N}, found {total} total across {rotated} rotated + live file"
    );
}

/// With a much larger N and tight max_bytes, force many rotations
/// in a row to surface any per-rotation state-corruption bugs.
#[test]
fn audit_log_many_consecutive_rotations() {
    let dir = tempdir().unwrap();
    let max = 1024u64; // tiny — every few events
    let log = AuditLog::open_with_rotation(dir.path(), Some(max)).unwrap();

    const N: usize = 2_000;
    for i in 0..N {
        write_event(&log, &format!("c{i:04}"));
    }
    drop(log);

    let audit_dir = dir.path().join("_audit");
    let rotated = count_rotated_files(&audit_dir);
    assert!(
        rotated >= 10,
        "expected many rotations with max={max} and {N} entries, got {rotated}"
    );
    assert_eq!(
        count_total_entries(&audit_dir),
        N,
        "no loss across {rotated} rotations"
    );
}

// ─────────────────────────────────────────────────────────────────────
// Concurrency safety — multiple writer threads + rotation under load.
// ─────────────────────────────────────────────────────────────────────

/// 4 threads × 250 writes with a low rotation threshold = guaranteed
/// rotations during concurrent writes. Total entries across all
/// files MUST equal 1000 — any race that dropped entries during the
/// rename→reopen handover would lose some.
#[test]
fn audit_log_rotation_concurrent_safe() {
    let dir = tempdir().unwrap();
    let log = Arc::new(AuditLog::open_with_rotation(dir.path(), Some(2048)).unwrap());

    const THREADS: usize = 4;
    const PER_THREAD: usize = 250;
    let handles: Vec<_> = (0..THREADS)
        .map(|tid| {
            let log = Arc::clone(&log);
            thread::spawn(move || {
                for i in 0..PER_THREAD {
                    write_event(&log, &format!("t{tid}_e{i:04}"));
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    // Force release of any held file handles before reading the dir.
    drop(log);

    let audit_dir = dir.path().join("_audit");
    let total = count_total_entries(&audit_dir);
    let expected = THREADS * PER_THREAD;
    assert_eq!(
        total, expected,
        "CONCURRENT ROTATION LOST EVENTS: wrote {expected}, found {total} across rotated + live files. \
         The rotate() path holds the mutex, so any drop here is a real bug."
    );

    // Every entry must still be valid JSON (rotation didn't tear a
    // mid-line write across files).
    for entry in std::fs::read_dir(&audit_dir).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name();
        let s = name.to_string_lossy();
        if s == "audit.log" || s.starts_with("audit.log.") {
            let content = std::fs::read_to_string(entry.path()).unwrap();
            for (lineno, line) in content.lines().enumerate() {
                if line.is_empty() {
                    continue;
                }
                serde_json::from_str::<serde_json::Value>(line).unwrap_or_else(|e| {
                    panic!(
                        "ROTATION TORE A WRITE: {s} line {lineno} is not valid JSON: {e}\n  line = {line:?}"
                    )
                });
            }
        }
    }
}
