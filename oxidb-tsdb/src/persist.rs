//! Durable storage: a versioned block snapshot + a per-generation WAL, with a
//! MANIFEST as the atomic commit point.
//!
//! ```text
//! <dir>/MANIFEST            # {"generation": N} — the authoritative generation
//! <dir>/blocks.<N>.tsb      # full compressed-block snapshot at checkpoint N
//! <dir>/wal.<N>.log         # points written after checkpoint N
//! ```
//!
//! **Checkpoint**: seal active buffers → write `blocks.<N+1>.tsb` (+fsync) →
//! atomically replace MANIFEST with generation N+1 (temp+rename) → start `wal.<N+1>`,
//! delete generation-N files. The MANIFEST rename is the single commit: a crash before
//! it recovers from generation N (old snapshot + its WAL, which still holds every
//! post-checkpoint point); a crash after it recovers from the new snapshot.
//! No point is ever double-counted. Retention is durable for free — dropped
//! blocks simply aren't in the next snapshot.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crate::model::{FieldType, SeriesKey};
use crate::store::{Block, Series};

pub struct Persist {
    dir: PathBuf,
    pub generation: u64,
    wal: BufWriter<File>,
    pub wal_bytes: u64,
}

fn manifest_path(dir: &Path) -> PathBuf {
    dir.join("MANIFEST")
}
fn blocks_path(dir: &Path, generation: u64) -> PathBuf {
    dir.join(format!("blocks.{generation}.tsb"))
}
fn wal_path(dir: &Path, generation: u64) -> PathBuf {
    dir.join(format!("wal.{generation}.log"))
}

fn read_gen(dir: &Path) -> u64 {
    fs::read(manifest_path(dir))
        .ok()
        .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok())
        .and_then(|v| v.get("generation").and_then(|g| g.as_u64()))
        .unwrap_or(0)
}

fn write_manifest(dir: &Path, generation: u64) -> io::Result<()> {
    let tmp = dir.join("MANIFEST.tmp");
    let body = serde_json::to_vec(&serde_json::json!({ "generation": generation })).unwrap();
    {
        let mut f = File::create(&tmp)?;
        f.write_all(&body)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, manifest_path(dir)) // atomic commit
}

impl Persist {
    /// Open (or create) persistence in `dir`, loading the current generation's
    /// snapshot + WAL into `series`.
    pub fn open(
        dir: &Path,
        series: &mut BTreeMap<SeriesKey, Series>,
        block_points: usize,
    ) -> io::Result<Persist> {
        fs::create_dir_all(dir)?;
        let generation = read_gen(dir);

        // Load the snapshot's sealed blocks.
        if let Ok(f) = File::open(blocks_path(dir, generation)) {
            let mut r = BufReader::new(f);
            while let Some((key, ftype, block)) = read_block(&mut r)? {
                let s = series.entry(key).or_default();
                s.set_ftype(ftype);
                s.push_block(block);
            }
        }
        // Replay the WAL (points written after the snapshot).
        if let Ok(f) = File::open(wal_path(dir, generation)) {
            let mut r = BufReader::new(f);
            while let Some((key, ftype, ts, val)) = read_wal_rec(&mut r)? {
                let s = series.entry(key).or_default();
                s.set_ftype(ftype);
                s.push(ts, val, block_points);
            }
        }

        let wal_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(wal_path(dir, generation))?;
        let wal_bytes = wal_file.metadata()?.len();
        Ok(Persist {
            dir: dir.to_path_buf(),
            generation,
            wal: BufWriter::new(wal_file),
            wal_bytes,
        })
    }

    /// Append one sample to the WAL.
    pub fn wal_append(
        &mut self,
        key: &SeriesKey,
        ftype: FieldType,
        ts: i64,
        val: f64,
    ) -> io::Result<()> {
        let mut buf = Vec::with_capacity(64);
        write_key(&mut buf, key);
        buf.push(ftype.to_u8());
        buf.extend_from_slice(&ts.to_le_bytes());
        buf.extend_from_slice(&val.to_bits().to_le_bytes());
        self.wal.write_all(&buf)?;
        self.wal_bytes += buf.len() as u64;
        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.wal.flush()
    }

    /// Write a full snapshot of `series` at generation+1, commit the MANIFEST, and
    /// rotate the WAL. Callers must seal active buffers first.
    pub fn checkpoint(&mut self, series: &BTreeMap<SeriesKey, Series>) -> io::Result<()> {
        let next = self.generation + 1;
        {
            let f = File::create(blocks_path(&self.dir, next))?;
            let mut w = BufWriter::new(f);
            for (key, s) in series {
                for b in s.sealed_blocks() {
                    write_block(&mut w, key, s.ftype(), b)?;
                }
            }
            w.flush()?;
            w.get_ref().sync_all()?;
        }
        write_manifest(&self.dir, next)?; // commit point

        // New empty WAL for the new generation, then drop the old files.
        self.wal = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(wal_path(&self.dir, next))?,
        );
        self.wal_bytes = 0;
        let _ = fs::remove_file(blocks_path(&self.dir, self.generation));
        let _ = fs::remove_file(wal_path(&self.dir, self.generation));
        self.generation = next;
        Ok(())
    }
}

// ── Encoding ────────────────────────────────────────────────────────────
fn write_str(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(&(s.len() as u16).to_le_bytes());
    buf.extend_from_slice(s.as_bytes());
}

fn write_key(buf: &mut Vec<u8>, key: &SeriesKey) {
    write_str(buf, &key.measurement);
    buf.extend_from_slice(&(key.tags.len() as u16).to_le_bytes());
    for (k, v) in &key.tags {
        write_str(buf, k);
        write_str(buf, v);
    }
    write_str(buf, &key.field);
}

fn write_block<W: Write>(
    w: &mut W,
    key: &SeriesKey,
    ftype: FieldType,
    b: &Block,
) -> io::Result<()> {
    let mut buf = Vec::with_capacity(32 + b.bytes.len());
    write_key(&mut buf, key);
    buf.push(ftype.to_u8());
    buf.extend_from_slice(&b.min_ts.to_le_bytes());
    buf.extend_from_slice(&b.max_ts.to_le_bytes());
    buf.extend_from_slice(&(b.count as u32).to_le_bytes());
    buf.extend_from_slice(&(b.bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(&b.bytes);
    w.write_all(&buf)
}

// ── Decoding (returns None at clean EOF; errors on a torn tail) ──────────
fn read_exact_opt<R: Read>(r: &mut R, buf: &mut [u8]) -> io::Result<bool> {
    let mut read = 0;
    while read < buf.len() {
        match r.read(&mut buf[read..])? {
            0 if read == 0 => return Ok(false), // clean EOF
            0 => return Ok(false),              // torn tail — stop (ignore)
            n => read += n,
        }
    }
    Ok(true)
}

fn read_u16<R: Read>(r: &mut R) -> io::Result<Option<u16>> {
    let mut b = [0u8; 2];
    if !read_exact_opt(r, &mut b)? {
        return Ok(None);
    }
    Ok(Some(u16::from_le_bytes(b)))
}

fn read_str<R: Read>(r: &mut R) -> io::Result<Option<String>> {
    let Some(len) = read_u16(r)? else {
        return Ok(None);
    };
    let mut b = vec![0u8; len as usize];
    if !read_exact_opt(r, &mut b)? {
        return Ok(None);
    }
    Ok(Some(String::from_utf8_lossy(&b).into_owned()))
}

fn read_key<R: Read>(r: &mut R) -> io::Result<Option<SeriesKey>> {
    let Some(measurement) = read_str(r)? else {
        return Ok(None);
    };
    let Some(ntags) = read_u16(r)? else {
        return Ok(None);
    };
    let mut tags = Vec::with_capacity(ntags as usize);
    for _ in 0..ntags {
        let (Some(k), Some(v)) = (read_str(r)?, read_str(r)?) else {
            return Ok(None);
        };
        tags.push((k, v));
    }
    let Some(field) = read_str(r)? else {
        return Ok(None);
    };
    // tags already sorted on write (SeriesKey::new); preserve order.
    Ok(Some(SeriesKey {
        measurement,
        tags,
        field,
    }))
}

fn read_u8<R: Read>(r: &mut R) -> io::Result<Option<u8>> {
    let mut b = [0u8; 1];
    if !read_exact_opt(r, &mut b)? {
        return Ok(None);
    }
    Ok(Some(b[0]))
}

fn read_block<R: Read>(r: &mut R) -> io::Result<Option<(SeriesKey, FieldType, Block)>> {
    let Some(key) = read_key(r)? else {
        return Ok(None);
    };
    let Some(ft) = read_u8(r)? else {
        return Ok(None);
    };
    let mut hdr = [0u8; 8 + 8 + 4 + 4];
    if !read_exact_opt(r, &mut hdr)? {
        return Ok(None);
    }
    let min_ts = i64::from_le_bytes(hdr[0..8].try_into().unwrap());
    let max_ts = i64::from_le_bytes(hdr[8..16].try_into().unwrap());
    let count = u32::from_le_bytes(hdr[16..20].try_into().unwrap()) as usize;
    let blen = u32::from_le_bytes(hdr[20..24].try_into().unwrap()) as usize;
    let mut bytes = vec![0u8; blen];
    if !read_exact_opt(r, &mut bytes)? {
        return Ok(None);
    }
    Ok(Some((
        key,
        FieldType::from_u8(ft),
        Block {
            bytes,
            min_ts,
            max_ts,
            count,
        },
    )))
}

fn read_wal_rec<R: Read>(r: &mut R) -> io::Result<Option<(SeriesKey, FieldType, i64, f64)>> {
    let Some(key) = read_key(r)? else {
        return Ok(None);
    };
    let Some(ft) = read_u8(r)? else {
        return Ok(None);
    };
    let mut b = [0u8; 16];
    if !read_exact_opt(r, &mut b)? {
        return Ok(None);
    }
    let ts = i64::from_le_bytes(b[0..8].try_into().unwrap());
    let val = f64::from_bits(u64::from_le_bytes(b[8..16].try_into().unwrap()));
    Ok(Some((key, FieldType::from_u8(ft), ts, val)))
}
