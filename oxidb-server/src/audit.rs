use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use flate2::Compression;
use flate2::write::GzEncoder;
use serde::Serialize;

#[derive(Serialize)]
pub struct AuditEvent<'a> {
    pub ts: String,
    pub user: &'a str,
    pub cmd: &'a str,
    pub collection: Option<&'a str>,
    pub result: &'a str,
    pub detail: &'a str,
}

/// Wall-clock-aligned rotation boundary. Unlike `max_age` (which
/// counts elapsed seconds since the file became active), these
/// triggers fire when the **calendar** crosses a fixed boundary —
/// useful for SIEMs that aggregate logs by day or hour.
///
/// UTC-only by deliberate choice: timezone-aware rotation needs
/// a chrono / time crate dependency and DST handling that's worth
/// its own ADR. UTC sidesteps DST and is unambiguous for log
/// archival.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CalendarBoundary {
    /// Rotate when the wall clock crosses into the next UTC hour
    /// (boundary at HH:00:00 UTC).
    HourlyUtc,
    /// Rotate when the wall clock crosses into the next UTC day
    /// (boundary at 00:00:00 UTC).
    DailyUtc,
}

impl CalendarBoundary {
    /// Returns `true` if a rotation should fire now, given the
    /// wall-clock time the current file became active. Pure
    /// function — no real-clock reads — so unit tests can pin
    /// the boundary math without sleeping.
    pub fn should_rotate(&self, active_since: SystemTime, now: SystemTime) -> bool {
        let since_unix = active_since
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let now_unix = now
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        match self {
            // Hour bucket: floor(unix_secs / 3600). When the bucket
            // of `now` differs from the bucket of `active_since`,
            // we've crossed at least one HH:00:00 boundary.
            Self::HourlyUtc => (since_unix / 3600) != (now_unix / 3600),
            // Day bucket: floor(unix_secs / 86400). Same shape,
            // larger bucket. (No DST in UTC, so this is exact.)
            Self::DailyUtc => (since_unix / 86400) != (now_unix / 86400),
        }
    }
}

/// Combined rotation policy. Each trigger is independent — whichever
/// fires first rotates the log. All `None` ⇒ unbounded growth.
///
/// Forward-compat: when count-based rotation, weekly/monthly
/// calendar boundaries, or other triggers are added, they extend
/// this struct without breaking the `open_with_policy` API.
#[derive(Clone, Copy, Debug, Default)]
pub struct RotationPolicy {
    /// Rotate when the live file has grown to `>= max_bytes` bytes.
    pub max_bytes: Option<u64>,
    /// Rotate when the live file has been the active target for
    /// at least `max_age` of wall-clock-elapsed time since it
    /// became active (open or previous rotation, whichever was
    /// most recent).
    pub max_age: Option<Duration>,
    /// Rotate when the wall clock crosses the chosen UTC calendar
    /// boundary (hourly or daily). Independent of `max_age`:
    /// `max_age=Duration::from_secs(3600)` rotates 1 hour after
    /// file open, whereas `calendar=HourlyUtc` rotates at the
    /// next HH:00:00 UTC regardless of how long the file has
    /// already been active.
    pub calendar: Option<CalendarBoundary>,
    /// When `true`, gzip rotated files in place (rename to
    /// `audit.log.<unix_micros>.gz`, delete the uncompressed
    /// version). Cuts archived log size by 80-95% typical for
    /// JSON line-delimited text. Default `false` — preserves
    /// backwards-compat behaviour from PRs #70/#71/#74 where
    /// rotated files stayed uncompressed.
    ///
    /// Compression happens **inside** the rotation mutex (same
    /// critical section as the rename), which keeps test
    /// observability simple. For very large audit logs in
    /// production this could become a latency concern — async
    /// compression via a dedicated worker thread is a future
    /// optimisation tracked alongside the other audit ergonomics
    /// items.
    pub compress: bool,
}

impl RotationPolicy {
    /// No rotation. Equivalent to `Default::default()`.
    pub const fn unbounded() -> Self {
        Self {
            max_bytes: None,
            max_age: None,
            calendar: None,
            compress: false,
        }
    }

    /// Rotate when the live file reaches `max_bytes` bytes.
    pub const fn size(max_bytes: u64) -> Self {
        Self {
            max_bytes: Some(max_bytes),
            max_age: None,
            calendar: None,
            compress: false,
        }
    }

    /// Rotate after `secs` seconds of elapsed wall-clock time
    /// since the live file became active.
    pub const fn age_secs(secs: u64) -> Self {
        Self {
            max_bytes: None,
            max_age: Some(Duration::from_secs(secs)),
            calendar: None,
            compress: false,
        }
    }

    /// Rotate when EITHER `max_bytes` is reached OR `secs` seconds
    /// have elapsed since the live file became active.
    pub const fn size_or_age(max_bytes: u64, secs: u64) -> Self {
        Self {
            max_bytes: Some(max_bytes),
            max_age: Some(Duration::from_secs(secs)),
            calendar: None,
            compress: false,
        }
    }

    /// Rotate at the top of every UTC hour. Useful for SIEMs
    /// that index by hour-bucket.
    pub const fn hourly_utc() -> Self {
        Self {
            max_bytes: None,
            max_age: None,
            calendar: Some(CalendarBoundary::HourlyUtc),
            compress: false,
        }
    }

    /// Rotate at midnight UTC. Useful for daily log aggregation.
    pub const fn daily_utc() -> Self {
        Self {
            max_bytes: None,
            max_age: None,
            calendar: Some(CalendarBoundary::DailyUtc),
            compress: false,
        }
    }

    /// Chainable setter to enable gzip compression on rotated
    /// files. Returns `self` so it composes with the named
    /// constructors:
    ///   `RotationPolicy::daily_utc().with_compress()`
    pub const fn with_compress(mut self) -> Self {
        self.compress = true;
        self
    }

    /// Parse a `RotationPolicy` from raw string env-var values.
    /// Pure function — no `std::env::var` reads — so tests can
    /// drive every parsing edge case without touching process-
    /// wide state. Panics on malformed numeric values or
    /// unknown calendar names; matches the existing
    /// `OXIDB_POOL_SIZE` / `OXIDB_IDLE_TIMEOUT` convention of
    /// failing loudly at startup rather than silently ignoring.
    ///
    /// Calendar value parsing is case-insensitive and accepts
    /// the common shorthand:
    ///   `"hourly"` / `"hourly-utc"` → `HourlyUtc`
    ///   `"daily"` / `"daily-utc"`   → `DailyUtc`
    ///   `"none"` / `""`             → no calendar trigger
    pub fn from_env_strs(
        max_bytes: Option<&str>,
        max_age_secs: Option<&str>,
        calendar: Option<&str>,
        compress: Option<&str>,
    ) -> Self {
        let max_bytes = max_bytes.map(|s| {
            s.parse::<u64>()
                .expect("OXIDB_AUDIT_MAX_BYTES must be a valid u64")
        });
        let max_age = max_age_secs.map(|s| {
            let secs: u64 = s
                .parse()
                .expect("OXIDB_AUDIT_MAX_AGE_SECS must be a valid u64");
            Duration::from_secs(secs)
        });
        let calendar = match calendar
            .map(str::trim)
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            None | Some("") | Some("none") => None,
            Some("hourly") | Some("hourly-utc") | Some("hourlyutc") => {
                Some(CalendarBoundary::HourlyUtc)
            }
            Some("daily") | Some("daily-utc") | Some("dailyutc") => {
                Some(CalendarBoundary::DailyUtc)
            }
            Some(other) => {
                panic!("OXIDB_AUDIT_CALENDAR must be 'hourly' / 'daily' / 'none', got {other:?}")
            }
        };
        let compress = match compress
            .map(str::trim)
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            // Common bool-shaped opt-ins. Mirrors OXIDB_AUDIT itself.
            Some("true") | Some("1") | Some("yes") | Some("on") => true,
            // Explicit-off, blank, or unset all disable.
            None | Some("") | Some("false") | Some("0") | Some("no") | Some("off") => false,
            Some(other) => panic!(
                "OXIDB_AUDIT_COMPRESS must be a bool-shaped value (true/false/1/0/yes/no/on/off), got {other:?}"
            ),
        };
        Self {
            max_bytes,
            max_age,
            calendar,
            compress,
        }
    }

    /// Read the four `OXIDB_AUDIT_*` env vars and build the
    /// corresponding `RotationPolicy`. All vars are optional;
    /// unset means "no trigger of that kind" or `compress=false`.
    /// All-unset ⇒ `unbounded()`.
    pub fn from_env() -> Self {
        let max_bytes = std::env::var("OXIDB_AUDIT_MAX_BYTES").ok();
        let max_age = std::env::var("OXIDB_AUDIT_MAX_AGE_SECS").ok();
        let calendar = std::env::var("OXIDB_AUDIT_CALENDAR").ok();
        let compress = std::env::var("OXIDB_AUDIT_COMPRESS").ok();
        Self::from_env_strs(
            max_bytes.as_deref(),
            max_age.as_deref(),
            calendar.as_deref(),
            compress.as_deref(),
        )
    }

    /// Human-readable summary for the operator-visible startup
    /// stderr line. Renders `unbounded` when no trigger is set,
    /// else a compact comma-separated list.
    pub fn describe(&self) -> String {
        let mut parts: Vec<String> = Vec::new();
        if let Some(n) = self.max_bytes {
            parts.push(format!("size={n}B"));
        }
        if let Some(d) = self.max_age {
            parts.push(format!("age={}s", d.as_secs()));
        }
        if let Some(c) = self.calendar {
            parts.push(format!("calendar={c:?}"));
        }
        if self.compress {
            parts.push("compress=gzip".to_string());
        }
        if parts.is_empty() {
            "unbounded".to_string()
        } else {
            parts.join(", ")
        }
    }
}

/// Mutex-protected state inside `AuditLog` — the live file, a
/// byte counter (size-based rotation), and the timestamps the
/// current file became active. We keep BOTH a monotonic
/// `Instant` (for max_age, which must be immune to wall-clock
/// adjustments like NTP slew) AND a `SystemTime` (for calendar-
/// aligned rotation, which by definition needs the wall clock
/// even if NTP shifts it).
struct LogState {
    file: File,
    bytes_written: u64,
    active_since: Instant,
    active_since_wall: SystemTime,
}

pub struct AuditLog {
    inner: Mutex<LogState>,
    /// Path to the live `audit.log` file. Held so rotation can
    /// rename it without re-deriving the path on every call.
    path: PathBuf,
    /// Combined rotation policy (size + age + future knobs).
    policy: RotationPolicy,
}

impl AuditLog {
    /// Open or create an audit log file at `{data_dir}/_audit/audit.log`
    /// with **no rotation** (unbounded growth). Preserved for
    /// backwards compatibility with callers that don't need
    /// rotation. Use `open_with_policy` to opt into rotation.
    pub fn open(data_dir: &Path) -> Result<Self, String> {
        Self::open_with_policy(data_dir, RotationPolicy::unbounded())
    }

    /// Open or create the audit log with **size-based rotation only**
    /// (backwards-compat shim for the PR #70 API).
    pub fn open_with_rotation(data_dir: &Path, max_bytes: Option<u64>) -> Result<Self, String> {
        let policy = match max_bytes {
            Some(n) => RotationPolicy::size(n),
            None => RotationPolicy::unbounded(),
        };
        Self::open_with_policy(data_dir, policy)
    }

    /// Open or create the audit log with a full `RotationPolicy`
    /// covering size, age, and any future trigger added to the
    /// policy struct.
    ///
    /// Rotation happens INSIDE the write mutex, so concurrent
    /// writers see a single consistent transition. Rotation
    /// failures (rename / reopen errors) are swallowed — audit
    /// logging is fire-and-forget, and silently continuing to
    /// append to the existing file is preferable to dropping
    /// events.
    pub fn open_with_policy(data_dir: &Path, policy: RotationPolicy) -> Result<Self, String> {
        let audit_dir = data_dir.join("_audit");
        fs::create_dir_all(&audit_dir).map_err(|e| format!("failed to create audit dir: {e}"))?;

        let path = audit_dir.join("audit.log");
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| format!("failed to open audit log: {e}"))?;

        // If a previous run left the file partially-written, pick
        // up tracking from its current size — that's where we'll
        // start appending. Age tracking starts NOW; we don't try to
        // recover "this file was opened K hours ago" from filesystem
        // metadata (mtime is too noisy and not portable enough).
        let bytes_written = file.metadata().map(|m| m.len()).unwrap_or(0);

        Ok(Self {
            inner: Mutex::new(LogState {
                file,
                bytes_written,
                active_since: Instant::now(),
                active_since_wall: SystemTime::now(),
            }),
            path,
            policy,
        })
    }

    /// Log an audit event. Fire-and-forget (no fsync). After the
    /// write, checks every active rotation trigger and rotates
    /// if any has fired.
    pub fn log(&self, event: &AuditEvent) {
        let json = match serde_json::to_string(event) {
            Ok(s) => s,
            Err(_) => return,
        };
        let line = format!("{json}\n");

        let mut state = self.inner.lock().unwrap();
        if state.file.write_all(line.as_bytes()).is_ok() {
            state.bytes_written = state.bytes_written.saturating_add(line.len() as u64);
        }

        // Independent triggers — any fires regardless of the
        // others. All checked after the write so rotation never
        // delays the current event.
        let size_trigger = self
            .policy
            .max_bytes
            .map(|max| state.bytes_written >= max)
            .unwrap_or(false);
        let age_trigger = self
            .policy
            .max_age
            .map(|max| state.active_since.elapsed() >= max)
            .unwrap_or(false);
        let calendar_trigger = self
            .policy
            .calendar
            .map(|b| b.should_rotate(state.active_since_wall, SystemTime::now()))
            .unwrap_or(false);

        if size_trigger || age_trigger || calendar_trigger {
            // Best-effort rotate. On failure (e.g. rename collision in
            // the same microsecond, or read-only filesystem), keep
            // current file open and silently continue appending —
            // better than losing events.
            let _ = self.rotate(&mut state);
        }
    }

    /// Atomically rotate the live audit file. Caller must hold the
    /// state mutex. If `policy.compress` is set, gzip the rotated
    /// file in place before returning (also inside the mutex —
    /// see RotationPolicy.compress field-doc for rationale).
    fn rotate(&self, state: &mut LogState) -> Result<(), String> {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros())
            .unwrap_or(0);

        let rotated_name = format!("audit.log.{stamp}");
        let rotated_path = self.path.with_file_name(rotated_name);

        // Rename is atomic within the same filesystem per POSIX —
        // the open file handle in `state.file` continues to point
        // at the original inode (now reachable only via the new
        // name), but we close it next anyway.
        fs::rename(&self.path, &rotated_path).map_err(|e| format!("rotate rename failed: {e}"))?;

        // Open a fresh `audit.log` and replace the held file.
        let new_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|e| format!("rotate reopen failed: {e}"))?;
        state.file = new_file;
        state.bytes_written = 0;
        state.active_since = Instant::now();
        state.active_since_wall = SystemTime::now();

        // Best-effort gzip. If compression fails (e.g. disk full,
        // permission denied on the .gz target), leave the
        // uncompressed file in place — better than losing audit
        // data. Log to stderr so an operator can investigate.
        if self.policy.compress
            && let Err(e) = gzip_in_place(&rotated_path)
        {
            eprintln!(
                "audit: gzip of {} failed ({e}); leaving uncompressed",
                rotated_path.display()
            );
        }

        Ok(())
    }
}

/// Compress `path` into `path.gz` using deflate, then delete the
/// original on success. Atomic-ish: if compression fails part-way,
/// the partial `.gz` is removed and the original stays put. The
/// caller treats failure as "leave the uncompressed file" so audit
/// data is never lost to a compression error.
fn gzip_in_place(path: &Path) -> io::Result<()> {
    let gz_path = {
        let mut p = path.as_os_str().to_owned();
        p.push(".gz");
        PathBuf::from(p)
    };

    // If a `.gz` already exists at the target name (extremely
    // unlikely — would require two rotations in the same
    // microsecond — but treated explicitly), bail out without
    // overwriting; the uncompressed file stays as the durable
    // record.
    if gz_path.exists() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("{} already exists", gz_path.display()),
        ));
    }

    let result = (|| -> io::Result<()> {
        let mut input = File::open(path)?;
        let output = File::create(&gz_path)?;
        let mut encoder = GzEncoder::new(output, Compression::default());
        io::copy(&mut input, &mut encoder)?;
        encoder.finish()?;
        Ok(())
    })();

    match result {
        Ok(()) => {
            // Compression succeeded — drop the uncompressed
            // original. If this delete fails, the .gz is intact
            // and the original is intact too; that's "duplicate
            // data", not "lost data", so propagate the error so
            // the operator sees it but the audit state is safe.
            fs::remove_file(path)?;
            Ok(())
        }
        Err(e) => {
            // Compression failed — remove the partial .gz so the
            // uncompressed original remains the single source of
            // truth. Best-effort; ignore secondary errors.
            let _ = fs::remove_file(&gz_path);
            Err(e)
        }
    }
}

/// Get current timestamp as RFC 3339 string.
pub fn now_rfc3339() -> String {
    use std::time::SystemTime;
    let dur = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = dur.as_secs();
    let days = secs / 86400;
    let time_secs = secs % 86400;
    let hours = time_secs / 3600;
    let mins = (time_secs % 3600) / 60;
    let s = time_secs % 60;

    let mut y = 1970i64;
    let mut remaining = days as i64;
    loop {
        let days_in_year = if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 {
            366
        } else {
            365
        };
        if remaining < days_in_year {
            break;
        }
        remaining -= days_in_year;
        y += 1;
    }
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let month_days = if leap {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };
    let mut m = 0;
    for (i, &md) in month_days.iter().enumerate() {
        if remaining < md as i64 {
            m = i + 1;
            break;
        }
        remaining -= md as i64;
    }
    let d = remaining + 1;
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        y, m, d, hours, mins, s
    )
}
