//! Persistence: MANIFEST + generation snapshot + per-generation WAL
//! (ADR-0025 §6), mirroring TSDB's discipline.
//!
//! ```text
//! MANIFEST          {"generation": N} — the authoritative generation
//! snap.<N>.rec      snapshot: config guard + interner + all model counters
//! wal.<N>.log       one JSON line per tracked basket since that snapshot
//! ```
//!
//! Checkpoint writes `snap.<N+1>` (tmp + fsync + rename), atomically renames
//! MANIFEST — the single commit point — then starts `wal.<N+1>` and removes
//! generation N. A crash before the MANIFEST rename recovers from N.
//!
//! Recovery is snapshot load + WAL replay. `track` is idempotent on basket
//! id and the seen-set is inside the snapshot, so replaying records the
//! snapshot already contains is a no-op — the WAL needs no commit records
//! and no truncation discipline. A torn final line (crash mid-append) is
//! ignored, not fatal: everything before it parsed, and the torn record was
//! never acknowledged as durable.
//!
//! WAL appends are flushed per record (OS buffer, no fsync) — the durability
//! window is the process, not the disk platter, which is v1's stated trade;
//! the auto-checkpoint keeps replay bounded (`RecConfig::checkpoint_bytes`).

use std::fs;
use std::io::{self, BufRead, BufWriter, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::model::Interner;
use crate::store::Store;

#[derive(Serialize, Deserialize)]
struct Manifest {
    generation: u64,
}

/// Everything a generation snapshot holds. `bucket_secs` is a guard: periods
/// are epochs divided by it, so reopening with a different width would
/// silently re-interpret every counter — refused instead.
#[derive(Serialize, Deserialize)]
pub(crate) struct Snapshot {
    pub bucket_secs: u64,
    pub interner: Interner,
    pub store: Store,
}

/// One WAL record = one tracked basket, exactly as the caller sent it.
#[derive(Serialize, Deserialize)]
pub(crate) struct WalRecord {
    pub model: String,
    pub basket_id: u64,
    pub items: Vec<String>,
    pub ts_secs: u64,
}

pub(crate) struct Persist {
    dir: PathBuf,
    generation: u64,
    wal: BufWriter<fs::File>,
    pub wal_bytes: u64,
}

fn manifest_path(dir: &Path) -> PathBuf {
    dir.join("MANIFEST")
}
fn snap_path(dir: &Path, generation: u64) -> PathBuf {
    dir.join(format!("snap.{generation}.rec"))
}
fn wal_path(dir: &Path, generation: u64) -> PathBuf {
    dir.join(format!("wal.{generation}.log"))
}

impl Persist {
    /// Open the directory: load the authoritative generation's snapshot (if
    /// any) and hand back the WAL records to replay. The caller applies them
    /// through the normal `track` path — replay IS ingestion.
    pub fn open(dir: &Path) -> io::Result<(Self, Option<Snapshot>, Vec<WalRecord>)> {
        fs::create_dir_all(dir)?;
        let generation = match fs::read(manifest_path(dir)) {
            Ok(bytes) => {
                serde_json::from_slice::<Manifest>(&bytes)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                    .generation
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => 0,
            Err(e) => return Err(e),
        };

        let snapshot = match fs::read(snap_path(dir, generation)) {
            Ok(bytes) => Some(
                serde_json::from_slice::<Snapshot>(&bytes)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
            ),
            Err(e) if e.kind() == io::ErrorKind::NotFound => None,
            Err(e) => return Err(e),
        };

        // Replay: every parseable line. A torn final line is a crash
        // mid-append — skipped, because it was never durable.
        let mut records = Vec::new();
        match fs::File::open(wal_path(dir, generation)) {
            Ok(f) => {
                for line in io::BufReader::new(f).lines() {
                    let line = line?;
                    if let Ok(rec) = serde_json::from_str::<WalRecord>(&line) {
                        records.push(rec);
                    }
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }

        // Sweep files that are not the live generation's: a crash after the
        // MANIFEST flip but before the removes leaves the previous
        // generation behind, and a crash between snapshot write and flip
        // leaves an orphaned next generation. Neither is ever read again —
        // the MANIFEST is the sole authority — but without a sweep they
        // accumulate one pair per crash, forever.
        if let Ok(entries) = fs::read_dir(dir) {
            let keep_snap = snap_path(dir, generation);
            let keep_wal = wal_path(dir, generation);
            for e in entries.flatten() {
                let path = e.path();
                let name = e.file_name();
                let name = name.to_string_lossy();
                if (name.starts_with("snap.") || name.starts_with("wal."))
                    && path != keep_snap
                    && path != keep_wal
                {
                    let _ = fs::remove_file(&path);
                }
            }
        }

        // Continue appending to the live generation's WAL.
        let wal_file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(wal_path(dir, generation))?;
        let wal_bytes = wal_file.metadata()?.len();
        Ok((
            Self {
                dir: dir.to_path_buf(),
                generation,
                wal: BufWriter::new(wal_file),
                wal_bytes,
            },
            snapshot,
            records,
        ))
    }

    /// Append one record and flush it to the OS.
    pub fn append(&mut self, rec: &WalRecord) -> io::Result<()> {
        let mut line = serde_json::to_vec(rec).map_err(io::Error::other)?;
        line.push(b'\n');
        self.wal.write_all(&line)?;
        self.wal.flush()?;
        self.wal_bytes += line.len() as u64;
        Ok(())
    }

    /// Fold current state into generation N+1: snapshot (tmp+fsync+rename),
    /// MANIFEST rename (the commit point), fresh WAL, old generation removed.
    pub fn checkpoint(&mut self, snapshot: &Snapshot) -> io::Result<()> {
        let next = self.generation + 1;

        let snap = snap_path(&self.dir, next);
        let tmp = snap.with_extension("rec.tmp");
        {
            let mut f = fs::File::create(&tmp)?;
            f.write_all(&serde_json::to_vec(snapshot).map_err(io::Error::other)?)?;
            f.sync_all()?;
        }
        fs::rename(&tmp, &snap)?;

        // Fresh WAL BEFORE the manifest flips: if we crash between the two,
        // the manifest still names generation N, whose snapshot and WAL are
        // untouched — an orphan snap/wal for N+1 is swept on a later
        // checkpoint's remove.
        let new_wal = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(wal_path(&self.dir, next))?;

        let mpath = manifest_path(&self.dir);
        let mtmp = mpath.with_extension("tmp");
        {
            let mut f = fs::File::create(&mtmp)?;
            f.write_all(
                &serde_json::to_vec(&Manifest { generation: next }).map_err(io::Error::other)?,
            )?;
            f.sync_all()?;
        }
        fs::rename(&mtmp, &mpath)?;
        if let Ok(d) = fs::File::open(&self.dir) {
            let _ = d.sync_all();
        }

        let old = self.generation;
        self.generation = next;
        self.wal = BufWriter::new(new_wal);
        self.wal_bytes = 0;
        let _ = fs::remove_file(snap_path(&self.dir, old));
        let _ = fs::remove_file(wal_path(&self.dir, old));
        Ok(())
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }
}
