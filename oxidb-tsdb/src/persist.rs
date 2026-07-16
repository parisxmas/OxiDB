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
use crate::store::{Block, Series, StrSeries};

pub struct Persist {
    dir: PathBuf,
    pub generation: u64,
    wal: BufWriter<File>,
    pub wal_bytes: u64,
    /// Generations pinned by in-progress low-lock backups, refcounted. A
    /// checkpoint never deletes a pinned generation's files, so a backup can
    /// archive them (and a stable WAL prefix) with the engine lock released.
    pinned: BTreeMap<u64, usize>,
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
fn wm_path(dir: &Path, generation: u64) -> PathBuf {
    dir.join(format!("rollup_wm.{generation}.json"))
}
fn rollups_path(dir: &Path) -> PathBuf {
    dir.join("rollups.json")
}

/// Persist the rollup rule set (survives restart; watermark is per-generation).
pub fn save_rollups(dir: &Path, rules: &[crate::RollupSpec]) -> io::Result<()> {
    let tmp = dir.join("rollups.tmp");
    fs::write(&tmp, serde_json::to_vec(rules).unwrap_or_default())?;
    fs::rename(&tmp, rollups_path(dir))
}

pub fn load_rollups(dir: &Path) -> Vec<crate::RollupSpec> {
    fs::read(rollups_path(dir))
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or_default()
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
        str_series: &mut BTreeMap<SeriesKey, StrSeries>,
        watermark: &mut BTreeMap<String, i64>,
        block_points: usize,
    ) -> io::Result<Persist> {
        fs::create_dir_all(dir)?;
        let generation = read_gen(dir);

        // Rollup watermark sidecar for this generation.
        if let Ok(bytes) = fs::read(wm_path(dir, generation))
            && let Ok(map) = serde_json::from_slice::<BTreeMap<String, i64>>(&bytes)
        {
            *watermark = map;
        }

        // Load the snapshot: numeric block records + string-series records.
        if let Ok(f) = File::open(blocks_path(dir, generation)) {
            let mut r = BufReader::new(f);
            while let Some(rec) = read_snapshot_record(&mut r)? {
                match rec {
                    SnapRec::Num(key, ftype, block) => {
                        let s = series.entry(key).or_default();
                        s.set_ftype(ftype);
                        s.push_block(block);
                    }
                    SnapRec::Str(key, pts) => {
                        let s = str_series.entry(key).or_default();
                        for (ts, val) in pts {
                            s.push(ts, val);
                        }
                    }
                }
            }
        }
        // Replay the WAL (points written after the snapshot).
        if let Ok(f) = File::open(wal_path(dir, generation)) {
            let mut r = BufReader::new(f);
            while let Some(rec) = read_wal_rec(&mut r)? {
                match rec {
                    WalRec::Num(key, ftype, ts, val) => {
                        let s = series.entry(key).or_default();
                        s.set_ftype(ftype);
                        s.push(ts, val, block_points);
                    }
                    WalRec::Str(key, ts, val) => {
                        str_series.entry(key).or_default().push(ts, val);
                    }
                }
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
            pinned: BTreeMap::new(),
        })
    }

    /// Pin the current generation for a low-lock backup: flush the WAL so its
    /// on-disk length is up to date, bump the generation's pin refcount, and
    /// return `(generation, wal_len)`. While pinned, a checkpoint won't delete
    /// this generation's files, and its WAL prefix `[0, wal_len)` never
    /// changes (the per-generation WAL is only appended to or rotated, never
    /// truncated in place) — so the archiver can read them with the lock down.
    pub fn pin_for_backup(&mut self) -> io::Result<(u64, u64)> {
        self.flush()?;
        *self.pinned.entry(self.generation).or_insert(0) += 1;
        Ok((self.generation, self.wal_bytes))
    }

    /// Release a backup pin. If a checkpoint superseded `gen` while it was
    /// pinned (and no other backup still holds it), reclaim its files now.
    pub fn unpin_after_backup(&mut self, generation: u64) {
        if let Some(count) = self.pinned.get_mut(&generation) {
            *count -= 1;
            if *count == 0 {
                self.pinned.remove(&generation);
            }
        }
        if generation != self.generation && !self.pinned.contains_key(&generation) {
            let _ = fs::remove_file(blocks_path(&self.dir, generation));
            let _ = fs::remove_file(wal_path(&self.dir, generation));
            let _ = fs::remove_file(wm_path(&self.dir, generation));
        }
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

    /// Append one text sample to the WAL.
    pub fn wal_append_str(&mut self, key: &SeriesKey, ts: i64, val: &str) -> io::Result<()> {
        let mut buf = Vec::with_capacity(64 + val.len());
        write_key(&mut buf, key);
        buf.push(FieldType::Str.to_u8());
        buf.extend_from_slice(&ts.to_le_bytes());
        buf.extend_from_slice(&(val.len() as u32).to_le_bytes());
        buf.extend_from_slice(val.as_bytes());
        self.wal.write_all(&buf)?;
        self.wal_bytes += buf.len() as u64;
        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.wal.flush()
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Write a full snapshot of `series` at generation+1, commit the MANIFEST, and
    /// rotate the WAL. Callers must seal active buffers first.
    pub fn checkpoint(
        &mut self,
        series: &BTreeMap<SeriesKey, Series>,
        str_series: &BTreeMap<SeriesKey, StrSeries>,
        watermark: &BTreeMap<String, i64>,
    ) -> io::Result<()> {
        let next = self.generation + 1;
        {
            let f = File::create(blocks_path(&self.dir, next))?;
            let mut w = BufWriter::new(f);
            for (key, s) in series {
                for b in s.sealed_blocks() {
                    write_block(&mut w, key, s.ftype(), b)?;
                }
            }
            for (key, s) in str_series {
                write_str_series(&mut w, key, s.points())?;
            }
            w.flush()?;
            w.get_ref().sync_all()?;
        }
        // Rollup watermark rides the same generation (written before the commit).
        {
            let mut f = File::create(wm_path(&self.dir, next))?;
            f.write_all(&serde_json::to_vec(watermark).unwrap_or_default())?;
            f.sync_all()?;
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
        // Reclaim the superseded generation — unless a backup pinned it, in
        // which case its files stay until the backup unpins them.
        if !self.pinned.contains_key(&self.generation) {
            let _ = fs::remove_file(blocks_path(&self.dir, self.generation));
            let _ = fs::remove_file(wal_path(&self.dir, self.generation));
            let _ = fs::remove_file(wm_path(&self.dir, self.generation));
        }
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

/// One string-series snapshot record: `[key][ftype=Str][npoints u32][(ts, len,
/// bytes)*]`.
fn write_str_series<W: Write>(
    w: &mut W,
    key: &SeriesKey,
    points: &[(i64, String)],
) -> io::Result<()> {
    let mut buf = Vec::with_capacity(32);
    write_key(&mut buf, key);
    buf.push(FieldType::Str.to_u8());
    buf.extend_from_slice(&(points.len() as u32).to_le_bytes());
    for (ts, s) in points {
        buf.extend_from_slice(&ts.to_le_bytes());
        buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
        buf.extend_from_slice(s.as_bytes());
    }
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

fn read_u32<R: Read>(r: &mut R) -> io::Result<Option<u32>> {
    let mut b = [0u8; 4];
    if !read_exact_opt(r, &mut b)? {
        return Ok(None);
    }
    Ok(Some(u32::from_le_bytes(b)))
}

enum SnapRec {
    Num(SeriesKey, FieldType, Block),
    Str(SeriesKey, Vec<(i64, String)>),
}

fn read_snapshot_record<R: Read>(r: &mut R) -> io::Result<Option<SnapRec>> {
    let Some(key) = read_key(r)? else {
        return Ok(None);
    };
    let Some(ft) = read_u8(r)? else {
        return Ok(None);
    };
    if FieldType::from_u8(ft) == FieldType::Str {
        let Some(n) = read_u32(r)? else {
            return Ok(None);
        };
        let mut pts = Vec::with_capacity(n as usize);
        for _ in 0..n {
            let mut tsb = [0u8; 8];
            if !read_exact_opt(r, &mut tsb)? {
                return Ok(None);
            }
            let ts = i64::from_le_bytes(tsb);
            let Some(len) = read_u32(r)? else {
                return Ok(None);
            };
            let mut sb = vec![0u8; len as usize];
            if !read_exact_opt(r, &mut sb)? {
                return Ok(None);
            }
            pts.push((ts, String::from_utf8_lossy(&sb).into_owned()));
        }
        return Ok(Some(SnapRec::Str(key, pts)));
    }
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
    Ok(Some(SnapRec::Num(
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

enum WalRec {
    Num(SeriesKey, FieldType, i64, f64),
    Str(SeriesKey, i64, String),
}

fn read_wal_rec<R: Read>(r: &mut R) -> io::Result<Option<WalRec>> {
    let Some(key) = read_key(r)? else {
        return Ok(None);
    };
    let Some(ft) = read_u8(r)? else {
        return Ok(None);
    };
    let mut tsb = [0u8; 8];
    if !read_exact_opt(r, &mut tsb)? {
        return Ok(None);
    }
    let ts = i64::from_le_bytes(tsb);
    if FieldType::from_u8(ft) == FieldType::Str {
        let Some(len) = read_u32(r)? else {
            return Ok(None);
        };
        let mut sb = vec![0u8; len as usize];
        if !read_exact_opt(r, &mut sb)? {
            return Ok(None);
        }
        return Ok(Some(WalRec::Str(
            key,
            ts,
            String::from_utf8_lossy(&sb).into_owned(),
        )));
    }
    let mut vb = [0u8; 8];
    if !read_exact_opt(r, &mut vb)? {
        return Ok(None);
    }
    let val = f64::from_bits(u64::from_le_bytes(vb));
    Ok(Some(WalRec::Num(key, FieldType::from_u8(ft), ts, val)))
}
