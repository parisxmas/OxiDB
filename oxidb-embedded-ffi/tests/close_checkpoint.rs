//! Checkpoint-on-close: a cleanly closed embedded handle folds the SQL WAL
//! into a fresh generation, so the data directory a backed-up application
//! leaves behind is snapshot-only. A clean engine must NOT mint a new
//! generation per open/close cycle — that is the difference between a tidy
//! close and a directory that grows a generation every time the app starts.

use std::ffi::{CStr, CString, c_char};
use std::path::Path;

use oxidb_embedded_ffi::{oxidb_close, oxidb_execute, oxidb_free_string, oxidb_open};

fn open(dir: &Path) -> *mut std::ffi::c_void {
    let c = CString::new(dir.to_str().unwrap()).unwrap();
    let h = unsafe { oxidb_open(c.as_ptr()) };
    assert!(!h.is_null(), "open failed");
    h
}

fn sql(h: *mut std::ffi::c_void, sql: &str) -> String {
    let req = serde_json::json!({"engine": "sql", "cmd": "sql", "sql": sql});
    let c = CString::new(req.to_string()).unwrap();
    let ptr: *mut c_char = unsafe { oxidb_execute(h, c.as_ptr()) };
    assert!(!ptr.is_null(), "execute returned null");
    let out = unsafe { CStr::from_ptr(ptr) }.to_str().unwrap().to_string();
    unsafe { oxidb_free_string(ptr) };
    assert!(out.contains("\"ok\":true"), "sql failed: {out}");
    out
}

/// The WAL's size on disk. An empty (fully folded) WAL is exactly its
/// 8-byte header — never zero-length.
const WAL_HEADER: u64 = 8;

fn wal_bytes(dir: &Path) -> u64 {
    std::fs::metadata(dir.join("sql/wal/live.wal")).map_or(0, |m| m.len())
}

fn manifest(dir: &Path) -> String {
    std::fs::read_to_string(dir.join("sql/MANIFEST")).unwrap_or_default()
}

#[test]
fn close_folds_a_dirty_wal_and_spares_a_clean_one() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();

    // Write through the FFI, then close: the WAL held the rows and must be
    // folded (truncated) by the close-time checkpoint.
    let h = open(path);
    sql(h, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
    sql(h, "INSERT INTO t VALUES (1, 'a'), (2, 'b')");
    assert!(
        wal_bytes(path) > WAL_HEADER,
        "the WAL should hold the writes pre-close"
    );
    unsafe { oxidb_close(h) };
    assert_eq!(wal_bytes(path), WAL_HEADER, "close must fold the WAL");
    let folded = manifest(path);
    assert!(!folded.is_empty(), "the fold must have committed a MANIFEST");

    // The data survived the fold: a fresh open answers from the snapshot.
    let h = open(path);
    let out = sql(h, "SELECT count(*) FROM t");
    assert!(out.contains("[[2]]"), "rows lost across the fold: {out}");

    // Read-only session: close must NOT write a new generation.
    unsafe { oxidb_close(h) };
    assert_eq!(
        manifest(path),
        folded,
        "a clean close minted a new generation"
    );
}
