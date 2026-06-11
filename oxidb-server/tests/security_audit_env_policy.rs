//! CERN-grade tests: `RotationPolicy::from_env_strs` parser for the
//! `OXIDB_AUDIT_*` env vars wired into `main.rs`.
//!
//! Pure-function tests — `from_env_strs` takes raw `Option<&str>` so
//! every parsing edge case is testable without `std::env::set_var`
//! (which is process-wide state and fights with parallel test
//! execution). The real env-var reader `from_env()` is a thin
//! 4-line wrapper that just shells out the three `env::var` calls
//! into `from_env_strs`; no separate test needed.
//!
//! Env vars wired:
//!   OXIDB_AUDIT             "true" / "1" → audit logging on (existing)
//!   OXIDB_AUDIT_MAX_BYTES   u64 byte threshold for size-based rotation
//!   OXIDB_AUDIT_MAX_AGE_SECS u64 seconds for elapsed-age rotation
//!   OXIDB_AUDIT_CALENDAR    "hourly" / "daily" / "none" / ""
//!
//! All three rotation vars are independent; any combination is
//! valid. All unset ⇒ `RotationPolicy::unbounded()` (matches the
//! pre-PR behavior of `AuditLog::open` without the policy arg).

use oxidb_server::audit::{CalendarBoundary, RotationPolicy};
use std::time::Duration;

// ─────────────────────────────────────────────────────────────────────
// Default / unbounded
// ─────────────────────────────────────────────────────────────────────

#[test]
fn env_policy_all_unset_is_unbounded() {
    let p = RotationPolicy::from_env_strs(None, None, None, None);
    assert_eq!(p.max_bytes, None);
    assert_eq!(p.max_age, None);
    assert_eq!(p.calendar, None);
    // Describe should call it out clearly.
    assert_eq!(p.describe(), "unbounded");
}

// ─────────────────────────────────────────────────────────────────────
// OXIDB_AUDIT_MAX_BYTES
// ─────────────────────────────────────────────────────────────────────

#[test]
fn env_policy_max_bytes_parses() {
    let p = RotationPolicy::from_env_strs(Some("1048576"), None, None, None);
    assert_eq!(p.max_bytes, Some(1_048_576));
    assert!(p.describe().contains("size=1048576B"));
}

#[test]
fn env_policy_max_bytes_zero_is_legal() {
    // Edge case: max_bytes=0 means "rotate after every write".
    // Weird but valid; parser accepts it.
    let p = RotationPolicy::from_env_strs(Some("0"), None, None, None);
    assert_eq!(p.max_bytes, Some(0));
}

#[test]
#[should_panic(expected = "OXIDB_AUDIT_MAX_BYTES must be a valid u64")]
fn env_policy_max_bytes_malformed_panics() {
    let _ = RotationPolicy::from_env_strs(Some("not-a-number"), None, None, None);
}

#[test]
#[should_panic(expected = "OXIDB_AUDIT_MAX_BYTES must be a valid u64")]
fn env_policy_max_bytes_negative_panics() {
    // u64 parse rejects negatives — message is the same.
    let _ = RotationPolicy::from_env_strs(Some("-1"), None, None, None);
}

// ─────────────────────────────────────────────────────────────────────
// OXIDB_AUDIT_MAX_AGE_SECS
// ─────────────────────────────────────────────────────────────────────

#[test]
fn env_policy_max_age_secs_parses() {
    let p = RotationPolicy::from_env_strs(None, Some("3600"), None, None);
    assert_eq!(p.max_age, Some(Duration::from_secs(3600)));
    assert!(p.describe().contains("age=3600s"));
}

#[test]
#[should_panic(expected = "OXIDB_AUDIT_MAX_AGE_SECS must be a valid u64")]
fn env_policy_max_age_malformed_panics() {
    let _ = RotationPolicy::from_env_strs(None, Some("an-hour"), None, None);
}

// ─────────────────────────────────────────────────────────────────────
// OXIDB_AUDIT_CALENDAR — string parsing
// ─────────────────────────────────────────────────────────────────────

#[test]
fn env_policy_calendar_hourly_aliases_all_parse() {
    for input in &[
        "hourly",
        "hourly-utc",
        "hourlyutc",
        "Hourly",
        "HOURLY",
        "Hourly-UTC",
    ] {
        let p = RotationPolicy::from_env_strs(None, None, Some(input), None);
        assert_eq!(
            p.calendar,
            Some(CalendarBoundary::HourlyUtc),
            "expected HourlyUtc for input {input:?}, got {:?}",
            p.calendar
        );
    }
}

#[test]
fn env_policy_calendar_daily_aliases_all_parse() {
    for input in &[
        "daily",
        "daily-utc",
        "dailyutc",
        "Daily",
        "DAILY",
        "Daily-UTC",
    ] {
        let p = RotationPolicy::from_env_strs(None, None, Some(input), None);
        assert_eq!(
            p.calendar,
            Some(CalendarBoundary::DailyUtc),
            "expected DailyUtc for input {input:?}, got {:?}",
            p.calendar
        );
    }
}

#[test]
fn env_policy_calendar_none_and_empty_disable_calendar() {
    for input in &[None, Some(""), Some("none"), Some("None"), Some("NONE")] {
        let p = RotationPolicy::from_env_strs(None, None, *input, None);
        assert_eq!(p.calendar, None, "expected None for {input:?}");
    }
}

#[test]
fn env_policy_calendar_whitespace_trimmed() {
    let p = RotationPolicy::from_env_strs(None, None, Some("  hourly  "), None);
    assert_eq!(p.calendar, Some(CalendarBoundary::HourlyUtc));
}

#[test]
#[should_panic(expected = "OXIDB_AUDIT_CALENDAR must be 'hourly' / 'daily' / 'none'")]
fn env_policy_calendar_unknown_value_panics() {
    let _ = RotationPolicy::from_env_strs(None, None, Some("weekly"), None);
}

// ─────────────────────────────────────────────────────────────────────
// All three set — combined policy
// ─────────────────────────────────────────────────────────────────────

#[test]
fn env_policy_all_three_set_compose_correctly() {
    let p = RotationPolicy::from_env_strs(Some("2048"), Some("60"), Some("hourly"), None);
    assert_eq!(p.max_bytes, Some(2048));
    assert_eq!(p.max_age, Some(Duration::from_secs(60)));
    assert_eq!(p.calendar, Some(CalendarBoundary::HourlyUtc));

    // Describe lists all three.
    let desc = p.describe();
    assert!(desc.contains("size=2048B"), "describe missing size: {desc}");
    assert!(desc.contains("age=60s"), "describe missing age: {desc}");
    assert!(
        desc.contains("calendar="),
        "describe missing calendar: {desc}"
    );
}

#[test]
fn env_policy_describe_unbounded_does_not_say_size_zero() {
    // Catch a future "let's default missing fields to 0" patch
    // that would silently change unbounded → "rotate-every-write".
    let p = RotationPolicy::from_env_strs(None, None, None, None);
    let d = p.describe();
    assert!(
        !d.contains("size=0"),
        "unbounded must NOT describe as size=0: {d}"
    );
    assert!(
        !d.contains("age=0"),
        "unbounded must NOT describe as age=0: {d}"
    );
    assert_eq!(d, "unbounded");
}

// ─────────────────────────────────────────────────────────────────────
// from_env() smoke — confirms the real env-reader wraps the
// pure parser. Uses unique var names to avoid colliding with
// any other test that might (theoretically) set them.
// ─────────────────────────────────────────────────────────────────────

/// Smoke: from_env returns unbounded when no OXIDB_AUDIT_* vars
/// are set in the test environment. Cargo test inherits the
/// developer's shell env, so we have to clear before reading.
#[test]
fn env_policy_from_env_returns_unbounded_when_unset() {
    // Defensively clear all three. Restore at end is not required
    // because we set nothing.
    // SAFETY: env::remove_var is unsafe in Rust 2024 because
    // setenv is not thread-safe. This test does not run in
    // parallel with other env-reading code in the same process;
    // cargo-test isolates between binaries by default.
    unsafe {
        std::env::remove_var("OXIDB_AUDIT_MAX_BYTES");
        std::env::remove_var("OXIDB_AUDIT_MAX_AGE_SECS");
        std::env::remove_var("OXIDB_AUDIT_CALENDAR");
    }
    let p = RotationPolicy::from_env();
    assert_eq!(p.max_bytes, None);
    assert_eq!(p.max_age, None);
    assert_eq!(p.calendar, None);
}
