use std::sync::{mpsc, Arc};
use std::time::Duration;

use serde_json::{json, Value};

use crate::engine::OxiDb;
use crate::error::{Error, Result};

// ---------------------------------------------------------------------------
// Cron expression parser
// ---------------------------------------------------------------------------

/// A parsed 5-field cron expression.
/// Each field holds the sorted set of allowed values.
pub struct CronExpr {
    pub minutes: Vec<u8>,  // 0-59
    pub hours: Vec<u8>,    // 0-23
    pub doms: Vec<u8>,     // 1-31  (day of month)
    pub months: Vec<u8>,   // 1-12
    pub dows: Vec<u8>,     // 0-6   (0 = Sunday)
}

/// Parse a 5-field cron expression string.
///
/// Fields: `MIN HOUR DOM MON DOW`
///
/// Each field supports: `*`, exact `N`, range `N-M`, step `*/N` or `N-M/S`,
/// and comma-separated lists `N,M,O`.
pub fn parse_cron(expr: &str) -> Result<CronExpr> {
    let fields: Vec<&str> = expr.split_whitespace().collect();
    if fields.len() != 5 {
        return Err(Error::ScheduleError(format!(
            "cron expression must have 5 fields, got {}",
            fields.len()
        )));
    }

    Ok(CronExpr {
        minutes: parse_field(fields[0], 0, 59)?,
        hours: parse_field(fields[1], 0, 23)?,
        doms: parse_field(fields[2], 1, 31)?,
        months: parse_field(fields[3], 1, 12)?,
        dows: parse_field(fields[4], 0, 6)?,
    })
}

fn parse_field(field: &str, min: u8, max: u8) -> Result<Vec<u8>> {
    let mut values = Vec::new();

    for part in field.split(',') {
        // Check for step: */N or N-M/N
        if let Some((range_part, step_str)) = part.split_once('/') {
            let step: u8 = step_str
                .parse()
                .map_err(|_| Error::ScheduleError(format!("invalid step value: {step_str}")))?;
            if step == 0 {
                return Err(Error::ScheduleError("step value must be > 0".into()));
            }
            let (start, end) = if range_part == "*" {
                (min, max)
            } else {
                parse_range_bounds(range_part, min, max)?
            };
            let mut v = start;
            while v <= end {
                values.push(v);
                v = match v.checked_add(step) {
                    Some(next) => next,
                    None => break,
                };
            }
        } else if part == "*" {
            for v in min..=max {
                values.push(v);
            }
        } else if let Some((lo, hi)) = part.split_once('-') {
            let lo: u8 = lo
                .parse()
                .map_err(|_| Error::ScheduleError(format!("invalid range start: {lo}")))?;
            let hi: u8 = hi
                .parse()
                .map_err(|_| Error::ScheduleError(format!("invalid range end: {hi}")))?;
            if lo < min || hi > max || lo > hi {
                return Err(Error::ScheduleError(format!(
                    "range {lo}-{hi} out of bounds ({min}-{max})"
                )));
            }
            for v in lo..=hi {
                values.push(v);
            }
        } else {
            let v: u8 = part
                .parse()
                .map_err(|_| Error::ScheduleError(format!("invalid field value: {part}")))?;
            if v < min || v > max {
                return Err(Error::ScheduleError(format!(
                    "value {v} out of bounds ({min}-{max})"
                )));
            }
            values.push(v);
        }
    }

    values.sort_unstable();
    values.dedup();
    Ok(values)
}

fn parse_range_bounds(s: &str, min: u8, max: u8) -> Result<(u8, u8)> {
    if let Some((lo, hi)) = s.split_once('-') {
        let lo: u8 = lo
            .parse()
            .map_err(|_| Error::ScheduleError(format!("invalid range start: {lo}")))?;
        let hi: u8 = hi
            .parse()
            .map_err(|_| Error::ScheduleError(format!("invalid range end: {hi}")))?;
        if lo < min || hi > max || lo > hi {
            return Err(Error::ScheduleError(format!(
                "range {lo}-{hi} out of bounds ({min}-{max})"
            )));
        }
        Ok((lo, hi))
    } else {
        let v: u8 = s
            .parse()
            .map_err(|_| Error::ScheduleError(format!("invalid value: {s}")))?;
        Ok((v, max))
    }
}

/// Check if a given minute/hour/dom/month/dow tuple matches the cron expression.
pub fn cron_matches(expr: &CronExpr, minute: u8, hour: u8, dom: u8, month: u8, dow: u8) -> bool {
    expr.minutes.contains(&minute)
        && expr.hours.contains(&hour)
        && expr.doms.contains(&dom)
        && expr.months.contains(&month)
        && expr.dows.contains(&dow)
}

// ---------------------------------------------------------------------------
// Interval parser
// ---------------------------------------------------------------------------

/// Parse an interval string like "30s", "5m", "2h" into a Duration.
pub fn parse_interval(s: &str) -> Result<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return Err(Error::ScheduleError("empty interval string".into()));
    }

    let (num_str, suffix) = if s.ends_with('s') {
        (&s[..s.len() - 1], "s")
    } else if s.ends_with('m') {
        (&s[..s.len() - 1], "m")
    } else if s.ends_with('h') {
        (&s[..s.len() - 1], "h")
    } else {
        return Err(Error::ScheduleError(format!(
            "interval must end with 's', 'm', or 'h': {s}"
        )));
    };

    let n: u64 = num_str
        .parse()
        .map_err(|_| Error::ScheduleError(format!("invalid interval number: {num_str}")))?;
    if n == 0 {
        return Err(Error::ScheduleError("interval must be > 0".into()));
    }

    Ok(match suffix {
        "s" => Duration::from_secs(n),
        "m" => Duration::from_secs(n * 60),
        "h" => Duration::from_secs(n * 3600),
        _ => unreachable!(),
    })
}

// ---------------------------------------------------------------------------
// Schedule due check
// ---------------------------------------------------------------------------

/// Determine if a schedule is due to run at the given timestamp (epoch seconds).
pub fn is_schedule_due(schedule: &Value, now_epoch: i64, now_parts: (u8, u8, u8, u8, u8)) -> bool {
    let enabled = schedule
        .get("enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    if !enabled {
        return false;
    }

    let last_run_epoch = schedule
        .get("last_run_epoch")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    // Cron mode
    if let Some(cron_str) = schedule.get("cron").and_then(|v| v.as_str()) {
        let expr = match parse_cron(cron_str) {
            Ok(e) => e,
            Err(_) => return false,
        };
        let (minute, hour, dom, month, dow) = now_parts;
        if !cron_matches(&expr, minute, hour, dom, month, dow) {
            return false;
        }
        // Don't re-run within the same minute
        let same_minute = (now_epoch - last_run_epoch).unsigned_abs() < 60;
        return !same_minute;
    }

    // Interval mode
    if let Some(every_str) = schedule.get("every").and_then(|v| v.as_str()) {
        let interval = match parse_interval(every_str) {
            Ok(d) => d,
            Err(_) => return false,
        };
        let elapsed = (now_epoch - last_run_epoch).unsigned_abs();
        return elapsed >= interval.as_secs();
    }

    false
}

// ---------------------------------------------------------------------------
// Scheduler worker loop
// ---------------------------------------------------------------------------

/// The scheduler background thread body.
///
/// Wakes every second, checks enabled schedules, and runs due procedures.
/// Exits when the shutdown channel is closed (sender dropped).
pub fn scheduler_loop(db: Arc<OxiDb>, rx: mpsc::Receiver<()>) {
    loop {
        // Sleep 1 second, checking for shutdown
        match rx.recv_timeout(Duration::from_secs(1)) {
            Ok(()) => break,   // explicit shutdown signal
            Err(mpsc::RecvTimeoutError::Disconnected) => break, // sender dropped
            Err(mpsc::RecvTimeoutError::Timeout) => {}          // normal tick
        }

        // Get current time components
        let now_epoch = epoch_now();
        let now_parts = epoch_to_parts(now_epoch);

        // Load enabled schedules from _schedules collection
        let schedules = match db.find("_schedules", &json!({"enabled": true})) {
            Ok(s) => s,
            Err(_) => continue, // collection may not exist yet
        };

        for sched in &schedules {
            if !is_schedule_due(sched, now_epoch, now_parts) {
                continue;
            }

            let name = match sched.get("name").and_then(|v| v.as_str()) {
                Some(n) => n.to_string(),
                None => continue,
            };
            let procedure = match sched.get("procedure").and_then(|v| v.as_str()) {
                Some(p) => p.to_string(),
                None => continue,
            };
            let params = sched.get("params").cloned().unwrap_or(json!({}));

            // Execute the procedure
            let (status, error) = match db.call_procedure(&procedure, params) {
                Ok(_) => ("ok".to_string(), Value::Null),
                Err(e) => {
                    let msg = e.to_string();
                    eprintln!("[scheduler] error running schedule '{name}': {msg}");
                    ("error".to_string(), Value::String(msg))
                }
            };

            // Build ISO 8601 timestamp for last_run
            let last_run_iso = epoch_to_iso(now_epoch);

            // Update the schedule record
            let run_count = sched
                .get("run_count")
                .and_then(|v| v.as_u64())
                .unwrap_or(0)
                + 1;

            let _ = db.update(
                "_schedules",
                &json!({"name": name}),
                &json!({
                    "$set": {
                        "last_run": last_run_iso,
                        "last_run_epoch": now_epoch,
                        "last_status": status,
                        "last_error": error,
                        "run_count": run_count,
                    }
                }),
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Time helpers (no chrono dependency — uses std SystemTime)
// ---------------------------------------------------------------------------

fn epoch_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

/// Convert epoch seconds to (minute, hour, dom, month, dow).
/// Uses a simple calendar calculation (no external crate).
fn epoch_to_parts(epoch: i64) -> (u8, u8, u8, u8, u8) {
    let secs = epoch;
    let days = secs.div_euclid(86400);
    let day_secs = secs.rem_euclid(86400);

    let hour = (day_secs / 3600) as u8;
    let minute = ((day_secs % 3600) / 60) as u8;

    // Day of week: Jan 1 1970 was Thursday (4)
    let dow = ((days + 4).rem_euclid(7)) as u8; // 0=Sun

    // Civil date from epoch days (algorithm from Howard Hinnant)
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let _y = if m <= 2 { y + 1 } else { y };

    (minute, hour, d as u8, m as u8, dow)
}

/// Convert epoch seconds to an ISO 8601 UTC string.
fn epoch_to_iso(epoch: i64) -> String {
    let (minute, hour, dom, month, dow) = epoch_to_parts(epoch);
    let _ = dow; // unused here

    // Re-derive year for the ISO string
    let days = epoch.div_euclid(86400);
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };

    let second = (epoch.rem_euclid(86400) % 60) as u8;

    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        year, month, dom, hour, minute, second
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Cron parser tests --

    #[test]
    fn parse_cron_star() {
        let expr = parse_cron("* * * * *").unwrap();
        assert_eq!(expr.minutes.len(), 60);
        assert_eq!(expr.hours.len(), 24);
        assert_eq!(expr.doms.len(), 31);
        assert_eq!(expr.months.len(), 12);
        assert_eq!(expr.dows.len(), 7);
    }

    #[test]
    fn parse_cron_exact() {
        let expr = parse_cron("30 3 15 6 2").unwrap();
        assert_eq!(expr.minutes, vec![30]);
        assert_eq!(expr.hours, vec![3]);
        assert_eq!(expr.doms, vec![15]);
        assert_eq!(expr.months, vec![6]);
        assert_eq!(expr.dows, vec![2]);
    }

    #[test]
    fn parse_cron_step() {
        let expr = parse_cron("*/15 */6 * * *").unwrap();
        assert_eq!(expr.minutes, vec![0, 15, 30, 45]);
        assert_eq!(expr.hours, vec![0, 6, 12, 18]);
    }

    #[test]
    fn parse_cron_range() {
        let expr = parse_cron("0 9-17 * * *").unwrap();
        assert_eq!(expr.minutes, vec![0]);
        assert_eq!(expr.hours, vec![9, 10, 11, 12, 13, 14, 15, 16, 17]);
    }

    #[test]
    fn parse_cron_list() {
        let expr = parse_cron("0 0 1,15 * *").unwrap();
        assert_eq!(expr.doms, vec![1, 15]);
    }

    #[test]
    fn parse_cron_range_step() {
        let expr = parse_cron("0-30/10 * * * *").unwrap();
        assert_eq!(expr.minutes, vec![0, 10, 20, 30]);
    }

    #[test]
    fn parse_cron_invalid_fields() {
        assert!(parse_cron("* *").is_err());
        assert!(parse_cron("* * * * * *").is_err());
    }

    #[test]
    fn parse_cron_out_of_range() {
        assert!(parse_cron("60 * * * *").is_err());
        assert!(parse_cron("* 25 * * *").is_err());
        assert!(parse_cron("* * 0 * *").is_err());
        assert!(parse_cron("* * * 13 *").is_err());
        assert!(parse_cron("* * * * 7").is_err());
    }

    // -- Cron match tests --

    #[test]
    fn cron_matches_every_minute() {
        let expr = parse_cron("* * * * *").unwrap();
        assert!(cron_matches(&expr, 0, 0, 1, 1, 0));
        assert!(cron_matches(&expr, 30, 12, 15, 6, 3));
    }

    #[test]
    fn cron_matches_specific() {
        let expr = parse_cron("0 3 * * *").unwrap();
        assert!(cron_matches(&expr, 0, 3, 1, 1, 0));
        assert!(!cron_matches(&expr, 1, 3, 1, 1, 0));
        assert!(!cron_matches(&expr, 0, 4, 1, 1, 0));
    }

    #[test]
    fn cron_matches_weekday() {
        // Only Monday (1) through Friday (5)
        let expr = parse_cron("0 9 * * 1-5").unwrap();
        assert!(cron_matches(&expr, 0, 9, 1, 1, 1));  // Monday
        assert!(cron_matches(&expr, 0, 9, 1, 1, 5));  // Friday
        assert!(!cron_matches(&expr, 0, 9, 1, 1, 0)); // Sunday
        assert!(!cron_matches(&expr, 0, 9, 1, 1, 6)); // Saturday
    }

    // -- Interval parser tests --

    #[test]
    fn parse_interval_seconds() {
        assert_eq!(parse_interval("30s").unwrap(), Duration::from_secs(30));
    }

    #[test]
    fn parse_interval_minutes() {
        assert_eq!(parse_interval("5m").unwrap(), Duration::from_secs(300));
    }

    #[test]
    fn parse_interval_hours() {
        assert_eq!(parse_interval("2h").unwrap(), Duration::from_secs(7200));
    }

    #[test]
    fn parse_interval_invalid() {
        assert!(parse_interval("").is_err());
        assert!(parse_interval("abc").is_err());
        assert!(parse_interval("0s").is_err());
        assert!(parse_interval("5d").is_err());
    }

    // -- Due check tests --

    #[test]
    fn due_cron_matches() {
        // Schedule with cron "0 3 * * *", never run before
        let sched = json!({
            "name": "test",
            "procedure": "proc",
            "cron": "0 3 * * *",
            "enabled": true,
            "last_run_epoch": 0,
        });
        // At 03:00 on any day
        assert!(is_schedule_due(&sched, 1000000, (0, 3, 15, 6, 2)));
        // At 04:00 — not matching
        assert!(!is_schedule_due(&sched, 1000000, (0, 4, 15, 6, 2)));
    }

    #[test]
    fn due_cron_no_rerun_same_minute() {
        let sched = json!({
            "name": "test",
            "procedure": "proc",
            "cron": "* * * * *",
            "enabled": true,
            "last_run_epoch": 1000000,
        });
        // Same epoch — should not re-run
        assert!(!is_schedule_due(&sched, 1000030, (0, 3, 15, 6, 2)));
        // 60+ seconds later — should run
        assert!(is_schedule_due(&sched, 1000061, (1, 3, 15, 6, 2)));
    }

    #[test]
    fn due_interval() {
        let sched = json!({
            "name": "test",
            "procedure": "proc",
            "every": "30s",
            "enabled": true,
            "last_run_epoch": 1000000,
        });
        // Only 10s elapsed — not due
        assert!(!is_schedule_due(&sched, 1000010, (0, 0, 1, 1, 0)));
        // 30s elapsed — due
        assert!(is_schedule_due(&sched, 1000030, (0, 0, 1, 1, 0)));
    }

    #[test]
    fn due_disabled() {
        let sched = json!({
            "name": "test",
            "procedure": "proc",
            "every": "1s",
            "enabled": false,
            "last_run_epoch": 0,
        });
        assert!(!is_schedule_due(&sched, 1000000, (0, 0, 1, 1, 0)));
    }

    // -- Time helper tests --

    #[test]
    fn epoch_to_parts_known_date() {
        // 2026-02-20 15:30:00 UTC = 1771601400 epoch seconds
        let parts = epoch_to_parts(1771601400);
        assert_eq!(parts.0, 30); // minute
        assert_eq!(parts.1, 15); // hour
        assert_eq!(parts.2, 20); // day
        assert_eq!(parts.3, 2);  // month
        // 2026-02-20 is a Friday => dow = 5
        assert_eq!(parts.4, 5);
    }

    #[test]
    fn epoch_to_iso_format() {
        let iso = epoch_to_iso(1771601400);
        assert_eq!(iso, "2026-02-20T15:30:00Z");
    }

    #[test]
    fn epoch_unix_epoch() {
        let parts = epoch_to_parts(0);
        assert_eq!(parts.0, 0);  // minute
        assert_eq!(parts.1, 0);  // hour
        assert_eq!(parts.2, 1);  // Jan 1
        assert_eq!(parts.3, 1);  // January
        assert_eq!(parts.4, 4);  // Thursday
    }
}
