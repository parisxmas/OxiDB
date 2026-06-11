//! On-disk format detection and migration scaffold.
//!
//! Phase 4 of ADR-0003. Today every shipped file is at v1, so `run` is a
//! validate-and-report no-op; the structure is in place for when a v2 lands.
//!
//! Format-version constants are duplicated from the engine here as a layering
//! choice: keeping the magics in one place at the CLI lets a future
//! `oxidb migrate` work on a data directory whose engine version is older
//! than the CLI's. Bumps must update both this file and the engine's source.

use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

// Format magics — keep in sync with engine constants.
const WAL_MAGIC: &[u8; 4] = b"OXWA";
const TX_MAGIC: &[u8; 4] = b"OXTX";
const BTREE_MAGIC: &[u8; 4] = b"OXBT";
const IDX_MAGIC: &[u8; 4] = b"OXIX";

// Current versions the engine writes.
const CURRENT_WAL_VERSION: u32 = 1;
const CURRENT_TX_VERSION: u32 = 1;
const CURRENT_BTREE_VERSION: u32 = 1;
const CURRENT_IDX_VERSION: u32 = 1;
const CURRENT_BLOB_META_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileKind {
    Wal,
    TxCommitLog,
    BTree,
    Index,
    BlobMeta,
}

impl FileKind {
    pub fn current_version(self) -> u32 {
        match self {
            Self::Wal => CURRENT_WAL_VERSION,
            Self::TxCommitLog => CURRENT_TX_VERSION,
            Self::BTree => CURRENT_BTREE_VERSION,
            Self::Index => CURRENT_IDX_VERSION,
            Self::BlobMeta => CURRENT_BLOB_META_VERSION,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Wal => "wal",
            Self::TxCommitLog => "tx-commit-log",
            Self::BTree => "btree",
            Self::Index => "index",
            Self::BlobMeta => "blob-meta",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FormatStatus {
    /// Version matches the engine's current.
    Current(u32),
    /// Older than current — would-be migration source.
    Older(u32),
    /// Newer than current — refuse-newer trigger.
    Newer(u32),
    /// Pre-magic header-less file (still readable today; engine self-migrates on next write).
    Legacy,
    /// File matched a kind by name but the header could not be parsed.
    Unreadable(String),
}

#[derive(Debug)]
pub struct FileReport {
    pub path: PathBuf,
    pub kind: FileKind,
    pub status: FormatStatus,
}

fn classify(path: &Path) -> Option<FileKind> {
    let name = path.file_name()?.to_str()?;
    if name.ends_with(".wal") || name.contains(".wal.") {
        return Some(FileKind::Wal);
    }
    if name == "_tx_commit_log" || name.starts_with("_tx_commit_log") {
        return Some(FileKind::TxCommitLog);
    }
    if name.ends_with(".btree") {
        return Some(FileKind::BTree);
    }
    if name.ends_with(".fidx") || name.ends_with(".cidx") {
        return Some(FileKind::Index);
    }
    // Blob .meta files live under `_blobs/<bucket>/<id>.meta`.
    if name.ends_with(".meta") && path.components().any(|c| c.as_os_str() == "_blobs") {
        return Some(FileKind::BlobMeta);
    }
    None
}

fn detect_status(path: &Path, kind: FileKind) -> FormatStatus {
    match kind {
        FileKind::BlobMeta => detect_blob_meta(path),
        FileKind::Wal | FileKind::TxCommitLog | FileKind::BTree | FileKind::Index => {
            detect_magic_header(path, kind)
        }
    }
}

fn detect_magic_header(path: &Path, kind: FileKind) -> FormatStatus {
    let mut f = match File::open(path) {
        Ok(f) => f,
        Err(e) => return FormatStatus::Unreadable(e.to_string()),
    };
    let mut header = [0u8; 8];
    match f.read(&mut header) {
        Ok(0) => return FormatStatus::Legacy, // empty file — pre-header writers leave these
        Ok(n) if n < 8 => return FormatStatus::Legacy,
        Ok(_) => {}
        Err(e) => return FormatStatus::Unreadable(e.to_string()),
    }

    let expected = match kind {
        FileKind::Wal => WAL_MAGIC,
        FileKind::TxCommitLog => TX_MAGIC,
        FileKind::BTree => BTREE_MAGIC,
        FileKind::Index => IDX_MAGIC,
        FileKind::BlobMeta => unreachable!(),
    };

    if &header[0..4] != expected {
        return FormatStatus::Legacy;
    }

    // OXIX uses u32 LE; OXWA/OXTX/OXBT use u16 LE. Read 4 bytes either way —
    // for the u16 formats the upper two bytes are flags (currently always 0).
    let version: u32 = match kind {
        FileKind::Index => u32::from_le_bytes([header[4], header[5], header[6], header[7]]),
        _ => u16::from_le_bytes([header[4], header[5]]) as u32,
    };

    classify_version(version, kind.current_version())
}

fn detect_blob_meta(path: &Path) -> FormatStatus {
    let bytes = match fs::read(path) {
        Ok(b) => b,
        Err(e) => return FormatStatus::Unreadable(e.to_string()),
    };
    let v: serde_json::Value = match serde_json::from_slice(&bytes) {
        Ok(v) => v,
        Err(e) => return FormatStatus::Unreadable(format!("invalid JSON: {e}")),
    };
    // Absent field → legacy (pre-Phase-1b blobs).
    let version = match v.get("format_version").and_then(|x| x.as_u64()) {
        Some(n) => n as u32,
        None => return FormatStatus::Legacy,
    };
    classify_version(version, CURRENT_BLOB_META_VERSION)
}

fn classify_version(found: u32, current: u32) -> FormatStatus {
    if found == current {
        FormatStatus::Current(found)
    } else if found < current {
        FormatStatus::Older(found)
    } else {
        FormatStatus::Newer(found)
    }
}

fn walk(dir: &Path, out: &mut Vec<FileReport>) -> Result<(), String> {
    let entries = fs::read_dir(dir).map_err(|e| format!("read_dir {}: {e}", dir.display()))?;
    for entry in entries {
        let entry = entry.map_err(|e| e.to_string())?;
        let path = entry.path();
        let ft = entry.file_type().map_err(|e| e.to_string())?;
        if ft.is_dir() {
            walk(&path, out)?;
        } else if let Some(kind) = classify(&path) {
            let status = detect_status(&path, kind);
            out.push(FileReport { path, kind, status });
        }
    }
    Ok(())
}

pub fn inspect(data_dir: &Path) -> Result<Vec<FileReport>, String> {
    if !data_dir.exists() {
        return Err(format!(
            "data directory does not exist: {}",
            data_dir.display()
        ));
    }
    let mut reports = Vec::new();
    walk(data_dir, &mut reports)?;
    reports.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(reports)
}

#[derive(Debug, Default)]
pub struct RunOptions {
    pub dry_run: bool,
    pub no_backup: bool,
    pub in_place: bool,
    pub out: Option<PathBuf>,
}

#[derive(Debug, Default)]
pub struct RunResult {
    pub current: usize,
    pub older: usize,
    pub newer: usize,
    pub legacy: usize,
    pub unreadable: usize,
}

/// Validate-and-report no-op today. When a v2 format lands, this is where the
/// per-kind upgrade dispatch goes: copy-out or in-place mutation, backup
/// first, write the new header, rewrite payloads as needed.
pub fn run(data_dir: &Path, opts: &RunOptions) -> Result<RunResult, String> {
    let reports = inspect(data_dir)?;
    let mut result = RunResult::default();
    let mut newer_files: Vec<&FileReport> = Vec::new();

    for r in &reports {
        match &r.status {
            FormatStatus::Current(_) => result.current += 1,
            FormatStatus::Older(_) => result.older += 1,
            FormatStatus::Newer(_) => {
                result.newer += 1;
                newer_files.push(r);
            }
            FormatStatus::Legacy => result.legacy += 1,
            FormatStatus::Unreadable(_) => result.unreadable += 1,
        }
    }

    if !newer_files.is_empty() {
        let list: Vec<String> = newer_files
            .iter()
            .map(|r| format!("  {} ({})", r.path.display(), r.kind.label()))
            .collect();
        return Err(format!(
            "{} file(s) use a format newer than this CLI knows how to read:\n{}",
            newer_files.len(),
            list.join("\n")
        ));
    }

    if result.older == 0 {
        return Ok(result);
    }

    // No older-format → current-format upgrade paths exist yet (everything
    // ships at v1). When the first v2 lands, dispatch goes here.
    if opts.dry_run {
        return Ok(result);
    }

    Err(format!(
        "{} older-format file(s) detected, but no migration paths are registered for the current engine. \
        Run with --dry-run to see the file list.",
        result.older
    ))
}

/// Recursively copy `src` → `dst`. Used for backup-before-mutate when run
/// dispatches real migrations. Currently exercised by tests only.
#[allow(dead_code)]
pub fn copy_dir_all(src: &Path, dst: &Path) -> std::io::Result<()> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let ft = entry.file_type()?;
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if ft.is_dir() {
            copy_dir_all(&from, &to)?;
        } else {
            fs::copy(&from, &to)?;
        }
    }
    Ok(())
}

pub fn print_inspect(
    reports: &[FileReport],
    json: bool,
    out: &mut impl Write,
) -> std::io::Result<()> {
    if json {
        let arr: Vec<serde_json::Value> = reports
            .iter()
            .map(|r| {
                serde_json::json!({
                    "path": r.path.to_string_lossy(),
                    "kind": r.kind.label(),
                    "status": match &r.status {
                        FormatStatus::Current(v) => serde_json::json!({"current": v}),
                        FormatStatus::Older(v) => serde_json::json!({"older": v}),
                        FormatStatus::Newer(v) => serde_json::json!({"newer": v}),
                        FormatStatus::Legacy => serde_json::json!("legacy"),
                        FormatStatus::Unreadable(e) => serde_json::json!({"unreadable": e}),
                    },
                })
            })
            .collect();
        writeln!(out, "{}", serde_json::to_string_pretty(&arr)?)?;
        return Ok(());
    }

    if reports.is_empty() {
        writeln!(out, "no format-versioned files found")?;
        return Ok(());
    }

    writeln!(out, "{:<14}  {:<10}  {}", "KIND", "STATUS", "PATH")?;
    for r in reports {
        let status = match &r.status {
            FormatStatus::Current(v) => format!("current v{v}"),
            FormatStatus::Older(v) => format!("older v{v}"),
            FormatStatus::Newer(v) => format!("newer v{v}"),
            FormatStatus::Legacy => "legacy".to_string(),
            FormatStatus::Unreadable(_) => "unreadable".to_string(),
        };
        writeln!(
            out,
            "{:<14}  {:<10}  {}",
            r.kind.label(),
            status,
            r.path.display()
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_known_extensions() {
        assert_eq!(classify(Path::new("/tmp/x/data.wal")), Some(FileKind::Wal));
        assert_eq!(
            classify(Path::new("/tmp/x/data.wal.000123")),
            Some(FileKind::Wal)
        );
        assert_eq!(
            classify(Path::new("/tmp/x/_tx_commit_log")),
            Some(FileKind::TxCommitLog)
        );
        assert_eq!(
            classify(Path::new("/tmp/x/users.btree")),
            Some(FileKind::BTree)
        );
        assert_eq!(
            classify(Path::new("/tmp/x/users.fidx")),
            Some(FileKind::Index)
        );
        assert_eq!(
            classify(Path::new("/tmp/x/users.cidx")),
            Some(FileKind::Index)
        );
        assert_eq!(
            classify(Path::new("/tmp/x/_blobs/audit/abc.meta")),
            Some(FileKind::BlobMeta)
        );
        assert_eq!(classify(Path::new("/tmp/x/users.json")), None);
    }

    #[test]
    fn classify_version_buckets() {
        assert_eq!(classify_version(1, 1), FormatStatus::Current(1));
        assert_eq!(classify_version(0, 1), FormatStatus::Older(0));
        assert_eq!(classify_version(2, 1), FormatStatus::Newer(2));
    }

    #[test]
    fn detect_blob_meta_handles_missing_field_as_legacy() {
        use std::io::Write as _;
        let dir = std::env::temp_dir().join(format!("oxidb-migrate-test-{}", std::process::id()));
        let _ = fs::create_dir_all(&dir);
        let p = dir.join("legacy.meta");
        let mut f = File::create(&p).unwrap();
        f.write_all(br#"{"etag":"deadbeef"}"#).unwrap();
        drop(f);
        assert_eq!(detect_blob_meta(&p), FormatStatus::Legacy);
        let _ = fs::remove_dir_all(&dir);
    }

    /// End-to-end: spin up a real OxiDB engine, write a doc so a `.wal` lands
    /// on disk, then assert `inspect` sees it at the current version.
    #[test]
    fn inspect_reports_fresh_engine_at_current_version() {
        use oxidb::OxiDb;
        use serde_json::json;

        let dir = std::env::temp_dir().join(format!(
            "oxidb-migrate-smoke-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = fs::remove_dir_all(&dir);

        {
            let db = OxiDb::open(&dir).expect("open engine");
            db.insert("events", json!({"name": "alpha", "n": 1}))
                .expect("insert");
            db.insert("events", json!({"name": "beta", "n": 2}))
                .expect("insert");
        }

        let reports = inspect(&dir).expect("inspect");
        assert!(
            !reports.is_empty(),
            "expected at least one format-versioned file in a fresh engine dir, got none"
        );
        for r in &reports {
            match &r.status {
                FormatStatus::Current(_) | FormatStatus::Legacy => {}
                other => panic!(
                    "fresh engine produced unexpected format status: {:?} for {}",
                    other,
                    r.path.display()
                ),
            }
        }

        // `run` must succeed on a fresh dir — nothing to migrate.
        let result = run(&dir, &RunOptions::default()).expect("run on fresh dir");
        assert_eq!(result.newer, 0);
        assert_eq!(result.unreadable, 0);

        let _ = fs::remove_dir_all(&dir);
    }
}
