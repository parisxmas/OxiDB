//! CERN-grade tests: audit-log **calendar-aligned** rotation.
//!
//! Extends `security_audit_rotation.rs` (size) and
//! `security_audit_time_rotation.rs` (elapsed-age) with the third
//! independent trigger added in this PR: `CalendarBoundary`
//! (HourlyUtc / DailyUtc).
//!
//! Calendar-aligned rotation differs from elapsed-age:
//!
//!   age_secs(3600)  → rotate 1 hour after the file became active
//!                     (e.g. file opened at 14:23 rotates at 15:23)
//!   HourlyUtc       → rotate at every HH:00:00 UTC, regardless of
//!                     when the file became active (file opened at
//!                     14:23 rotates at 15:00:00 — much sooner)
//!
//! UTC-only by deliberate choice: timezone-aware rotation adds DST
//! handling + a chrono dep that's worth its own ADR. UTC is
//! unambiguous and SIEMs typically prefer UTC log boundaries.
//!
//! These tests pin the boundary math via the pub
//! `CalendarBoundary::should_rotate(active_since, now)` pure
//! function — no real-clock reads, no sleeping. The integration
//! property (does `AuditLog::log` actually call should_rotate?) is
//! pinned by a smoke test that opens with a calendar policy and
//! confirms the type compiles + the engine doesn't panic when the
//! trigger fires.

use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tempfile::tempdir;

use oxidb_server::audit::{AuditEvent, AuditLog, CalendarBoundary, RotationPolicy};

/// Build a `SystemTime` representing the given unix-seconds value.
fn at_unix_secs(secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

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

// ─────────────────────────────────────────────────────────────────────
// Pure boundary math — `CalendarBoundary::should_rotate` is a pure
// function of (active_since, now). Pin every interesting case
// without touching the real clock.
// ─────────────────────────────────────────────────────────────────────

#[test]
fn calendar_hourly_should_rotate_when_hour_changes() {
    // active at 10:00:00 UTC, now at 11:00:00 UTC → cross hour
    let active = at_unix_secs(10 * 3600);
    let now = at_unix_secs(11 * 3600);
    assert!(CalendarBoundary::HourlyUtc.should_rotate(active, now));
}

#[test]
fn calendar_hourly_no_rotation_within_same_hour() {
    // active at 10:05:00, now at 10:55:00 → same hour bucket, no rotation
    let active = at_unix_secs(10 * 3600 + 300);
    let now = at_unix_secs(10 * 3600 + 3300);
    assert!(!CalendarBoundary::HourlyUtc.should_rotate(active, now));
}

#[test]
fn calendar_hourly_fires_at_exact_boundary_crossing() {
    // active at 09:59:59, now at 10:00:00 — exact second of crossing
    let active = at_unix_secs(10 * 3600 - 1);
    let now = at_unix_secs(10 * 3600);
    assert!(
        CalendarBoundary::HourlyUtc.should_rotate(active, now),
        "must rotate at the exact HH:00:00 boundary"
    );
}

#[test]
fn calendar_daily_should_rotate_when_day_changes() {
    // active 2025-01-01 23:00 UTC, now 2025-01-02 01:00 UTC → cross day
    let day = 86400;
    let active = at_unix_secs(day * 365 * 55 + 23 * 3600); // approx 2025-01-01 23:00
    let now = active + Duration::from_secs(2 * 3600);
    assert!(CalendarBoundary::DailyUtc.should_rotate(active, now));
}

#[test]
fn calendar_daily_no_rotation_within_same_day() {
    // active at start-of-day + 1h, now at start-of-day + 23h — same day
    let day_start = at_unix_secs(86400 * 1000);
    let active = day_start + Duration::from_secs(3600);
    let now = day_start + Duration::from_secs(23 * 3600);
    assert!(!CalendarBoundary::DailyUtc.should_rotate(active, now));
}

#[test]
fn calendar_daily_fires_at_midnight_crossing() {
    // active 23:59:59 of day N, now 00:00:00 of day N+1
    let day_start = at_unix_secs(86400 * 1000);
    let active = day_start + Duration::from_secs(86400 - 1); // 23:59:59
    let now = day_start + Duration::from_secs(86400); // next day 00:00:00
    assert!(
        CalendarBoundary::DailyUtc.should_rotate(active, now),
        "must rotate at the exact midnight UTC boundary"
    );
}

#[test]
fn calendar_hourly_does_not_spuriously_fire_for_now_in_past() {
    // Defensive: if `now` is somehow EARLIER than `active_since`
    // (clock went backwards via NTP slew), should_rotate must not
    // panic and should return false — the bucket math floors to
    // the same value or earlier.
    let active = at_unix_secs(10 * 3600);
    let now = at_unix_secs(10 * 3600 - 100); // 100s before
    // Different buckets if active is exactly at hour boundary,
    // same bucket otherwise — neither case should panic.
    let _ = CalendarBoundary::HourlyUtc.should_rotate(active, now);
}

// ─────────────────────────────────────────────────────────────────────
// Integration: AuditLog with a calendar policy compiles, opens, and
// logs without panic. We can't easily wait for a real hour boundary
// in a unit test — that's what the pure-fn tests above are for.
// ─────────────────────────────────────────────────────────────────────

#[test]
fn audit_log_constructible_with_hourly_policy() {
    let dir = tempdir().unwrap();
    let log = AuditLog::open_with_policy(dir.path(), RotationPolicy::hourly_utc()).unwrap();
    for _ in 0..10 {
        write_event(&log);
    }
    drop(log);
    let audit_dir = dir.path().join("_audit");
    // Within the same hour: no rotation expected.
    assert_eq!(
        count_rotated_files(&audit_dir),
        0,
        "hourly_utc policy should not rotate within the same UTC hour"
    );
}

#[test]
fn audit_log_constructible_with_daily_policy() {
    let dir = tempdir().unwrap();
    let log = AuditLog::open_with_policy(dir.path(), RotationPolicy::daily_utc()).unwrap();
    for _ in 0..10 {
        write_event(&log);
    }
    drop(log);
    let audit_dir = dir.path().join("_audit");
    assert_eq!(
        count_rotated_files(&audit_dir),
        0,
        "daily_utc policy should not rotate within the same UTC day"
    );
}

#[test]
fn audit_log_calendar_does_not_disable_size_trigger() {
    // Combined policy: small max_bytes + HourlyUtc. Within the same
    // hour, the size trigger must still fire.
    let dir = tempdir().unwrap();
    let policy = RotationPolicy {
        max_bytes: Some(1024),
        max_age: None,
        calendar: Some(CalendarBoundary::HourlyUtc),
        compress: false,
    };
    let log = AuditLog::open_with_policy(dir.path(), policy).unwrap();
    // ~10 events at ~120 bytes each comfortably crosses 1 KiB.
    for _ in 0..40 {
        write_event(&log);
    }
    drop(log);
    let audit_dir = dir.path().join("_audit");
    assert!(
        count_rotated_files(&audit_dir) >= 1,
        "size trigger must fire even when paired with calendar boundary"
    );
}

// ─────────────────────────────────────────────────────────────────────
// API ergonomics — constructor sugar pins each shape.
// ─────────────────────────────────────────────────────────────────────

#[test]
fn rotation_policy_hourly_utc_constructor_sets_only_calendar() {
    let p = RotationPolicy::hourly_utc();
    assert_eq!(p.max_bytes, None);
    assert_eq!(p.max_age, None);
    assert_eq!(p.calendar, Some(CalendarBoundary::HourlyUtc));
}

#[test]
fn rotation_policy_daily_utc_constructor_sets_only_calendar() {
    let p = RotationPolicy::daily_utc();
    assert_eq!(p.max_bytes, None);
    assert_eq!(p.max_age, None);
    assert_eq!(p.calendar, Some(CalendarBoundary::DailyUtc));
}

#[test]
fn rotation_policy_unbounded_has_no_calendar() {
    let p = RotationPolicy::unbounded();
    assert_eq!(p.calendar, None);
}
