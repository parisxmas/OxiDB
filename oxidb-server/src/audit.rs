use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

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

/// Combined rotation policy. Each trigger is independent — whichever
/// fires first rotates the log. Both `None` ⇒ unbounded growth.
///
/// Forward-compat: when count-based rotation, calendar-aligned
/// rotation, or other triggers are added, they extend this struct
/// without breaking the `open_with_policy` API.
#[derive(Clone, Copy, Debug, Default)]
pub struct RotationPolicy {
    /// Rotate when the live file has grown to `>= max_bytes` bytes.
    pub max_bytes: Option<u64>,
    /// Rotate when the live file has been the active target for
    /// at least `max_age` of wall-clock-elapsed time since it
    /// became active (open or previous rotation, whichever was
    /// most recent).
    pub max_age: Option<Duration>,
}

impl RotationPolicy {
    /// No rotation. Equivalent to `Default::default()`.
    pub const fn unbounded() -> Self {
        Self { max_bytes: None, max_age: None }
    }

    /// Rotate when the live file reaches `max_bytes` bytes.
    pub const fn size(max_bytes: u64) -> Self {
        Self { max_bytes: Some(max_bytes), max_age: None }
    }

    /// Rotate after `secs` seconds of elapsed wall-clock time
    /// since the live file became active.
    pub const fn age_secs(secs: u64) -> Self {
        Self {
            max_bytes: None,
            max_age: Some(Duration::from_secs(secs)),
        }
    }

    /// Rotate when EITHER `max_bytes` is reached OR `secs` seconds
    /// have elapsed since the live file became active.
    pub const fn size_or_age(max_bytes: u64, secs: u64) -> Self {
        Self {
            max_bytes: Some(max_bytes),
            max_age: Some(Duration::from_secs(secs)),
        }
    }
}

/// Mutex-protected state inside `AuditLog` — the live file, a
/// byte counter (size-based rotation), and the timestamp the
/// current file became active (age-based rotation).
struct LogState {
    file: File,
    bytes_written: u64,
    /// Set at open + each successful rotation. Used by age-based
    /// rotation to decide whether enough wall-clock time has
    /// elapsed.
    active_since: Instant,
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
    pub fn open_with_rotation(
        data_dir: &Path,
        max_bytes: Option<u64>,
    ) -> Result<Self, String> {
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
    pub fn open_with_policy(
        data_dir: &Path,
        policy: RotationPolicy,
    ) -> Result<Self, String> {
        let audit_dir = data_dir.join("_audit");
        fs::create_dir_all(&audit_dir)
            .map_err(|e| format!("failed to create audit dir: {e}"))?;

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

        // Independent triggers — either fires regardless of the
        // other. Both checked after the write so rotation never
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

        if size_trigger || age_trigger {
            // Best-effort rotate. On failure (e.g. rename collision in
            // the same microsecond, or read-only filesystem), keep
            // current file open and silently continue appending —
            // better than losing events.
            let _ = self.rotate(&mut state);
        }
    }

    /// Atomically rotate the live audit file. Caller must hold the
    /// state mutex.
    fn rotate(&self, state: &mut LogState) -> Result<(), String> {
        use std::time::{SystemTime, UNIX_EPOCH};
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
        fs::rename(&self.path, &rotated_path)
            .map_err(|e| format!("rotate rename failed: {e}"))?;

        // Open a fresh `audit.log` and replace the held file.
        let new_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|e| format!("rotate reopen failed: {e}"))?;
        state.file = new_file;
        state.bytes_written = 0;
        state.active_since = Instant::now();
        Ok(())
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
