use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::Path;
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

pub struct AuditLog {
    file: Mutex<File>,
}

impl AuditLog {
    /// Open or create an audit log file at `{data_dir}/_audit/audit.log`.
    pub fn open(data_dir: &Path) -> Result<Self, String> {
        let audit_dir = data_dir.join("_audit");
        fs::create_dir_all(&audit_dir)
            .map_err(|e| format!("failed to create audit dir: {e}"))?;

        let path = audit_dir.join("audit.log");
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| format!("failed to open audit log: {e}"))?;

        Ok(Self {
            file: Mutex::new(file),
        })
    }

    /// Log an audit event. Fire-and-forget (no fsync).
    pub fn log(&self, event: &AuditEvent) {
        let mut file = self.file.lock().unwrap();
        if let Ok(json) = serde_json::to_string(event) {
            let _ = writeln!(file, "{}", json);
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
