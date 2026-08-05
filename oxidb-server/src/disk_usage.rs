//! Per-engine on-disk footprint of the data directory.
//!
//! `{"cmd": "disk_usage"}` answers "how much space does the database take,
//! and which engine owns it" by walking `OXIDB_DATA` once and attributing
//! every file to the engine whose layout it belongs to. Attribution is by
//! path shape — the same conventions the engines themselves lay down:
//! `sql/` dirs (per-database, ADR-0012), `tsdb/` dirs, `_blobs/`, `_fts/`,
//! `_archive/` (PITR), `_oximem.snap`, `_mqtt*`/`_amqp*` collections
//! (the broker substrates), `_auth`/`_audit`/`_profile` (system), and
//! everything else is the document engine's own data.

use std::path::Path;

use serde_json::{Value, json};

#[derive(Default)]
struct Buckets {
    documents: u64,
    /// The mmap'd portion of `documents`: `.bdat`/`.bopts` files written by
    /// disk-first collections (`OXIDB_DISK_FIRST`). Counted INSIDE
    /// `documents` too — this is a breakdown, not a separate bucket — so the
    /// dashboard can say "and this much of it is not resident".
    documents_mmap: u64,
    sql: u64,
    tsdb: u64,
    blobs: u64,
    fts: u64,
    oximem: u64,
    messaging: u64,
    pitr_archive: u64,
    system: u64,
}

/// Walk the configured data directory (env `OXIDB_DATA`, the same resolution
/// the server boot uses) and report bytes per engine plus the total.
pub fn snapshot() -> Value {
    let root = std::env::var("OXIDB_DATA").unwrap_or_else(|_| "./oxidb_data".to_string());
    snapshot_at(Path::new(&root))
}

/// [`snapshot`] rooted explicitly — the testable core.
pub fn snapshot_at(root: &Path) -> Value {
    let mut b = Buckets::default();
    walk(root, root, &mut b);
    let total = b.documents
        + b.sql
        + b.tsdb
        + b.blobs
        + b.fts
        + b.oximem
        + b.messaging
        + b.pitr_archive
        + b.system;
    json!({
        "path": root.display().to_string(),
        "total_bytes": total,
        "engines": {
            "documents": b.documents,
            "documents_mmap": b.documents_mmap,
            "sql": b.sql,
            "tsdb": b.tsdb,
            "blobs": b.blobs,
            "oximem": b.oximem,
            "messaging": b.messaging,
            "fts": b.fts,
            "pitr_archive": b.pitr_archive,
            "system": b.system,
        },
    })
}

fn walk(root: &Path, dir: &Path, b: &mut Buckets) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(meta) = entry.metadata() else { continue };
        if meta.is_dir() {
            walk(root, &path, b);
        } else {
            let len = meta.len();
            let bucket = bucket_for(root, &path, b);
            *bucket += len;
            // Breakdown: disk-first document files (never a marker dir, so
            // only reachable when bucket_for chose `documents`).
            let is_docs = std::ptr::eq(bucket as *const u64, &b.documents as *const u64);
            if is_docs
                && matches!(
                    path.extension().and_then(|e| e.to_str()),
                    Some("bdat") | Some("bopts")
                )
            {
                b.documents_mmap += len;
            }
        }
    }
}

/// Which engine a file belongs to, by the layout conventions that put it
/// there. Directory markers win over file names, and the FIRST marker on the
/// path decides (`sql/` inside a named database is still the SQL engine).
fn bucket_for<'b>(root: &Path, file: &Path, b: &'b mut Buckets) -> &'b mut u64 {
    let rel = file.strip_prefix(root).unwrap_or(file);
    for comp in rel.iter() {
        let c = comp.to_string_lossy();
        match c.as_ref() {
            "sql" => return &mut b.sql,
            "tsdb" => return &mut b.tsdb,
            "_blobs" => return &mut b.blobs,
            "_fts" => return &mut b.fts,
            "_archive" => return &mut b.pitr_archive,
            _ => {}
        }
        if c.starts_with("_auth") || c.starts_with("_audit") || c.starts_with("_profile") {
            return &mut b.system;
        }
        if c.starts_with("_mqtt") || c.starts_with("_amqp") {
            return &mut b.messaging;
        }
        if c.starts_with("_oximem") {
            return &mut b.oximem;
        }
    }
    &mut b.documents
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(root: &Path, rel: &str, bytes: usize) {
        let p = root.join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, vec![0u8; bytes]).unwrap();
    }

    #[test]
    fn every_engine_layout_lands_in_its_own_bucket() {
        let dir = tempfile::tempdir().unwrap();
        let r = dir.path();
        put(r, "oxidb/orders.dat", 100); // document collection (named db)
        put(r, "oxidb/orders.wal", 10);
        put(r, "oxidb/readings.bdat", 40); // disk-first collection (mmap'd)
        put(r, "oxidb/readings.bopts", 1);
        put(r, "oxidb/sql/gen.1/products.rdat", 200); // SQL engine
        put(r, "analytics/sql/wal/live.wal", 30); // per-db SQL (ADR-0012)
        put(r, "oxidb/tsdb/blocks.1.tsb", 300); // TSDB
        put(r, "_blobs/certs/1.data", 400); // S3 blobs
        put(r, "oxidb/_fts/index.json", 50); // full-text
        put(r, "_oximem.snap", 60); // OxiMem snapshot
        put(r, "oxidb/_mqtt.dat", 70); // broker substrates
        put(r, "oxidb/_amqp.dat", 30);
        put(r, "_archive/segments/1.seg", 80); // PITR
        put(r, "_auth.dat", 90); // system
        put(r, "oxidb/_profile.dat", 5);

        let v = snapshot_at(r);
        let e = &v["engines"];
        assert_eq!(e["documents"], 151, "collection data+wal+disk-first files");
        assert_eq!(
            e["documents_mmap"], 41,
            "the mmap'd (.bdat/.bopts) breakdown"
        );
        assert_eq!(e["sql"], 230, "both databases' sql dirs");
        assert_eq!(e["tsdb"], 300);
        assert_eq!(e["blobs"], 400);
        assert_eq!(e["fts"], 50);
        assert_eq!(e["oximem"], 60);
        assert_eq!(e["messaging"], 100, "_mqtt + _amqp substrates");
        assert_eq!(e["pitr_archive"], 80);
        assert_eq!(e["system"], 95, "_auth + _profile");
        assert_eq!(
            v["total_bytes"],
            100 + 10 + 41 + 200 + 30 + 300 + 400 + 50 + 60 + 70 + 30 + 80 + 90 + 5
        );
    }
}
