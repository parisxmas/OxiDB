//! The checkpoint commit point.
//!
//! A tiny `MANIFEST` pointer file names the *generation* of catalog + row
//! snapshots that recovery must load, plus the WAL watermark past which records
//! still need replaying. Each generation lives in its own `gen.<N>/`
//! subdirectory (`catalog.json` + `<table>.rdat`), written whole before it is
//! ever referenced.
//!
//! Committing a checkpoint is a single atomic step — a temp file, an fsync, and
//! a rename over `MANIFEST` — that promotes a freshly written generation from
//! "half-built on disk" to "the live database". A crash before the rename
//! leaves the previous MANIFEST, and thus the previous intact generation, in
//! force; the half-built generation is ignored at open and garbage-collected.
//! Because catalog and snapshots switch together, recovery never sees a catalog
//! whose arity disagrees with its snapshots.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::Result;

const MANIFEST_VERSION: u16 = 1;

/// The durable pointer to the committed generation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u16,
    /// The committed generation: its catalog + snapshots live under
    /// `gen.<generation>/`.
    pub generation: u64,
    /// The highest WAL sequence already folded into this generation's
    /// snapshots. Recovery replays only records with a strictly greater `seq`,
    /// so a not-yet-truncated WAL never double-applies a checkpointed record.
    pub wal_seq: u64,
}

impl Manifest {
    fn path(root: &Path) -> PathBuf {
        root.join("MANIFEST")
    }

    /// Read the manifest, or `None` when absent — a fresh database, or a legacy
    /// one still using the flat root-level `catalog.json` + `<table>.rdat`
    /// layout (migrated to `gen.1/` at its first checkpoint).
    pub fn load(root: &Path) -> Result<Option<Manifest>> {
        match std::fs::read(Self::path(root)) {
            Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Serialize a manifest for `(generation, wal_seq)` to JSON bytes — used to
    /// inject a synthesized MANIFEST (pointing at a pinned generation) into a
    /// low-lock backup archive without touching the live one.
    pub fn to_bytes(generation: u64, wal_seq: u64) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(&Manifest {
            version: MANIFEST_VERSION,
            generation,
            wal_seq,
        })?)
    }

    /// Atomically commit `(generation, wal_seq)`: write a temp file, fsync it,
    /// then rename it over `MANIFEST`. This rename is the checkpoint's single
    /// commit point.
    pub fn commit(root: &Path, generation: u64, wal_seq: u64) -> Result<()> {
        let manifest = Manifest {
            version: MANIFEST_VERSION,
            generation,
            wal_seq,
        };
        let path = Self::path(root);
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, serde_json::to_vec(&manifest)?)?;
        std::fs::File::open(&tmp)?.sync_all()?;
        std::fs::rename(&tmp, &path)?;
        Ok(())
    }
}

/// The directory holding generation `n`'s catalog and row snapshots.
pub fn gen_dir(root: &Path, generation: u64) -> PathBuf {
    root.join(format!("gen.{generation}"))
}
