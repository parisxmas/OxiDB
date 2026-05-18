//! CERN-grade tests: gzip compression of rotated audit log files.
//!
//! Companion to the size / age / calendar rotation corpora. PR #75
//! added `RotationPolicy.compress: bool` + `OXIDB_AUDIT_COMPRESS`
//! env var. This corpus pins:
//!
//!   - `compress=false` (default) → rotated files stay
//!     uncompressed, name suffix unchanged (`audit.log.<ts>`).
//!   - `compress=true` → after rotation, the uncompressed file is
//!     gone and a `audit.log.<ts>.gz` exists in its place.
//!   - Compressed file content is a valid gzip stream that
//!     decompresses to EXACTLY the JSONL the engine wrote.
//!   - Multiple rotations under compression all produce `.gz`
//!     files; total event count across all `.gz`s + live file
//!     equals total events written (zero loss).
//!   - The OXIDB_AUDIT_COMPRESS env var parses every bool-shaped
//!     value (true/false/1/0/yes/no/on/off) case-insensitively.
//!   - `with_compress()` chainable setter composes correctly with
//!     the named constructors.
//!
//! Compression failure recovery (e.g. disk full during gzip) is
//! the trickiest property — the engine must leave audit data
//! intact even if gzip can't write its output. A full simulated-
//! failure test would need a custom Write impl that fails at a
//! specific byte offset; that's deferred to a follow-up.

use std::fs::File;
use std::io::Read;
use std::path::Path;

use flate2::read::GzDecoder;
use tempfile::tempdir;

use oxidb_server::audit::{AuditEvent, AuditLog, CalendarBoundary, RotationPolicy};

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

fn list_audit_dir(dir: &Path) -> Vec<String> {
    let mut out: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    out.sort();
    out
}

/// Count entries by reading each `audit.log` / `audit.log.<ts>`
/// (plain) and `audit.log.<ts>.gz` (gzipped) and summing line
/// counts. Returns (plain_count, gz_count, total_entries).
fn count_entries_handling_gzip(dir: &Path) -> (usize, usize, usize) {
    let mut plain = 0;
    let mut gz = 0;
    let mut total = 0;
    for entry in std::fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name();
        let s = name.to_string_lossy();
        if !(s == "audit.log" || s.starts_with("audit.log.")) {
            continue;
        }
        if s.ends_with(".gz") {
            gz += 1;
            let f = File::open(entry.path()).unwrap();
            let mut dec = GzDecoder::new(f);
            let mut buf = String::new();
            dec.read_to_string(&mut buf).unwrap();
            total += buf.lines().filter(|l| !l.is_empty()).count();
        } else {
            plain += 1;
            let buf = std::fs::read_to_string(entry.path()).unwrap();
            total += buf.lines().filter(|l| !l.is_empty()).count();
        }
    }
    (plain, gz, total)
}

// ─────────────────────────────────────────────────────────────────────
// Default off — preserves backwards-compat with PRs #70/#71/#74.
// ─────────────────────────────────────────────────────────────────────

#[test]
fn compress_field_defaults_false_on_all_constructors() {
    assert!(!RotationPolicy::unbounded().compress);
    assert!(!RotationPolicy::size(1024).compress);
    assert!(!RotationPolicy::age_secs(60).compress);
    assert!(!RotationPolicy::size_or_age(1024, 60).compress);
    assert!(!RotationPolicy::hourly_utc().compress);
    assert!(!RotationPolicy::daily_utc().compress);
}

#[test]
fn rotation_without_compress_leaves_rotated_files_uncompressed() {
    let dir = tempdir().unwrap();
    let log = AuditLog::open_with_policy(dir.path(), RotationPolicy::size(2048)).unwrap();
    for _ in 0..50 {
        write_event(&log, "drop_collection");
    }
    drop(log);

    let audit_dir = dir.path().join("_audit");
    let files = list_audit_dir(&audit_dir);
    let gz_count = files.iter().filter(|f| f.ends_with(".gz")).count();
    assert_eq!(
        gz_count, 0,
        "no .gz files expected when compress=false; got {files:?}"
    );
}

// ─────────────────────────────────────────────────────────────────────
// Compress-on rotation: produces .gz, removes plaintext, content
// round-trips correctly.
// ─────────────────────────────────────────────────────────────────────

#[test]
fn rotation_with_compress_produces_gz_files() {
    let dir = tempdir().unwrap();
    let policy = RotationPolicy::size(2048).with_compress();
    assert!(policy.compress);
    let log = AuditLog::open_with_policy(dir.path(), policy).unwrap();
    for _ in 0..100 {
        write_event(&log, "drop_collection");
    }
    drop(log);

    let audit_dir = dir.path().join("_audit");
    let files = list_audit_dir(&audit_dir);
    let gz_count = files.iter().filter(|f| f.ends_with(".gz")).count();
    assert!(
        gz_count >= 1,
        "expected at least 1 .gz file with compress=true; got {files:?}"
    );

    // No uncompressed rotated files should remain (compression
    // success path deletes the original).
    let uncompressed_rotated = files
        .iter()
        .filter(|f| f.starts_with("audit.log.") && !f.ends_with(".gz"))
        .count();
    assert_eq!(
        uncompressed_rotated, 0,
        "compress=true must remove the uncompressed original after gzip succeeds; \
         got {uncompressed_rotated} stragglers in {files:?}"
    );
}

#[test]
fn rotation_compress_preserves_all_entries_exactly() {
    let dir = tempdir().unwrap();
    let policy = RotationPolicy::size(2048).with_compress();
    let log = AuditLog::open_with_policy(dir.path(), policy).unwrap();

    const N: usize = 500;
    for i in 0..N {
        write_event(&log, &format!("cmd_{i:04}"));
    }
    drop(log);

    let audit_dir = dir.path().join("_audit");
    let (plain, gz, total) = count_entries_handling_gzip(&audit_dir);
    assert!(gz >= 1, "expected ≥ 1 .gz file, got {gz} (plain={plain})");
    assert_eq!(
        total, N,
        "COMPRESS LOST ENTRIES: wrote {N}, recovered {total} \
         across {plain} plaintext + {gz} gzipped files"
    );
}

#[test]
fn rotation_compress_gzip_stream_is_valid_and_parseable() {
    // Smaller test: one rotation, one .gz, decompress + parse
    // every line as JSON.
    let dir = tempdir().unwrap();
    let policy = RotationPolicy::size(1024).with_compress();
    let log = AuditLog::open_with_policy(dir.path(), policy).unwrap();
    for i in 0..30 {
        write_event(&log, &format!("c{i:03}"));
    }
    drop(log);

    let audit_dir = dir.path().join("_audit");
    let gz_file = list_audit_dir(&audit_dir)
        .into_iter()
        .find(|f| f.ends_with(".gz"))
        .expect("at least one .gz");
    let path = audit_dir.join(&gz_file);

    let f = File::open(&path).unwrap();
    let mut dec = GzDecoder::new(f);
    let mut buf = String::new();
    dec.read_to_string(&mut buf)
        .expect("gzip stream must be valid (decompressible)");

    // Every non-empty line must be valid JSON.
    let mut event_count = 0;
    for (lineno, line) in buf.lines().enumerate() {
        if line.is_empty() {
            continue;
        }
        serde_json::from_str::<serde_json::Value>(line).unwrap_or_else(|e| {
            panic!(
                "decompressed line {lineno} not valid JSON: {e}\n  line = {line:?}"
            )
        });
        event_count += 1;
    }
    assert!(event_count >= 1, ".gz must contain at least one event");
}

// ─────────────────────────────────────────────────────────────────────
// Combined with other triggers
// ─────────────────────────────────────────────────────────────────────

#[test]
fn compress_composes_with_calendar_constructor() {
    let p = RotationPolicy::hourly_utc().with_compress();
    assert_eq!(p.calendar, Some(CalendarBoundary::HourlyUtc));
    assert!(p.compress);
    assert_eq!(p.max_bytes, None);
    assert_eq!(p.max_age, None);
}

#[test]
fn compress_composes_with_size_or_age_constructor() {
    let p = RotationPolicy::size_or_age(1024, 60).with_compress();
    assert_eq!(p.max_bytes, Some(1024));
    assert!(p.max_age.is_some());
    assert!(p.compress);
}

// ─────────────────────────────────────────────────────────────────────
// OXIDB_AUDIT_COMPRESS env-var parsing
// ─────────────────────────────────────────────────────────────────────

#[test]
fn env_compress_truthy_values_all_enable() {
    for input in &["true", "1", "yes", "on", "TRUE", "Yes", "ON", " true "] {
        let p = RotationPolicy::from_env_strs(None, None, None, Some(input));
        assert!(p.compress, "expected compress=true for {input:?}");
    }
}

#[test]
fn env_compress_falsy_values_all_disable() {
    let cases: &[Option<&str>] = &[
        None,
        Some(""),
        Some("false"),
        Some("0"),
        Some("no"),
        Some("off"),
        Some("FALSE"),
        Some("Off"),
    ];
    for input in cases {
        let p = RotationPolicy::from_env_strs(None, None, None, *input);
        assert!(!p.compress, "expected compress=false for {input:?}");
    }
}

#[test]
#[should_panic(expected = "OXIDB_AUDIT_COMPRESS must be a bool-shaped value")]
fn env_compress_garbage_value_panics() {
    let _ = RotationPolicy::from_env_strs(None, None, None, Some("sometimes"));
}

#[test]
fn env_compress_describe_includes_compress_when_enabled() {
    let p = RotationPolicy::from_env_strs(Some("2048"), None, None, Some("true"));
    let desc = p.describe();
    assert!(
        desc.contains("compress=gzip"),
        "describe must mention compress when enabled; got {desc:?}"
    );
}

#[test]
fn env_compress_describe_omits_compress_when_disabled() {
    let p = RotationPolicy::from_env_strs(Some("2048"), None, None, None);
    let desc = p.describe();
    assert!(
        !desc.contains("compress"),
        "describe must omit compress when disabled; got {desc:?}"
    );
}
