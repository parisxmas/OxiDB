use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

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

/// Mutex-protected state inside `AuditLog` — the live file + a
/// byte counter so size-based rotation doesn't have to stat() on
/// every write.
struct LogState {
    file: File,
    bytes_written: u64,
}

pub struct AuditLog {
    inner: Mutex<LogState>,
    /// Path to the live `audit.log` file. Held so rotation can
    /// rename it without re-deriving the path on every call.
    path: PathBuf,
    /// If `Some(N)`, the log rotates when `bytes_written >= N`.
    /// `None` = unbounded growth (legacy behaviour preserved for
    /// callers that don't opt into rotation).
    max_bytes: Option<u64>,
}

impl AuditLog {
    /// Open or create an audit log file at `{data_dir}/_audit/audit.log`
    /// with **no rotation** (unbounded growth). Preserved for
    /// backwards compatibility with callers that don't need
    /// rotation. Use `open_with_rotation` to opt in.
    pub fn open(data_dir: &Path) -> Result<Self, String> {
        Self::open_with_rotation(data_dir, None)
    }

    /// Open or create the audit log with **optional size-based
    /// rotation**. When `max_bytes` is `Some(N)`, after each write
    /// that brings `bytes_written >= N`, the current `audit.log`
    /// is atomically renamed to `audit.log.<unix_micros>` and a
    /// fresh `audit.log` is opened to receive new events.
    ///
    /// Rotation happens INSIDE the write mutex, so concurrent
    /// writers see a single consistent transition. Rotation
    /// failures (e.g. rename returns an error) are swallowed —
    /// audit logging is fire-and-forget, and silently continuing
    /// to append to the existing file is preferable to dropping
    /// events.
    pub fn open_with_rotation(
        data_dir: &Path,
        max_bytes: Option<u64>,
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
        // start appending.
        let bytes_written = file.metadata().map(|m| m.len()).unwrap_or(0);

        Ok(Self {
            inner: Mutex::new(LogState {
                file,
                bytes_written,
            }),
            path,
            max_bytes,
        })
    }

    /// Log an audit event. Fire-and-forget (no fsync). If a max
    /// size is configured and this write crosses the threshold,
    /// rotates the live file (rename → reopen) before returning.
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

        if let Some(max) = self.max_bytes {
            if state.bytes_written >= max {
                // Best-effort rotate. On failure (e.g. rename fails
                // because the target name collides with an existing
                // file in the same microsecond, or filesystem is
                // read-only), keep the current file open and silently
                // continue appending — better than losing events.
                let _ = self.rotate(&mut state);
            }
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
