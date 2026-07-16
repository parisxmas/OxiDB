//! # oxidb-tsdb
//!
//! A time-series engine for OxiDB — a separate storage layer (in the spirit of
//! `oxidb-sql`) aimed at metrics / ticks, InfluxDB-style. The differentiator
//! is storage: each series is a Gorilla-compressed columnar stream partitioned
//! into time blocks, so regular timestamps cost ~1 bit and smooth values a few
//! bits each.
//!
//! MVP surface: ingest points, query by measurement + tag filters + time range
//! with optional downsampling (`GROUP BY time(interval)`) and tag grouping, and
//! aggregations (mean/sum/min/max/count/first/last). Retention drops whole
//! expired blocks cheaply.

mod bits;
mod gorilla;
mod line_protocol;
mod model;
mod persist;
mod store;

pub use line_protocol::parse as parse_line_protocol;
pub use model::{FieldType, FieldValue, Point, SeriesKey};
pub use store::{
    Agg, Block, GroupPoint, QuerySpec, ResultSeries, StrGroupPoint, StrResultSeries, StrValue,
    TagPredicate,
};

use std::collections::BTreeMap;
use std::path::Path;

/// Auto-checkpoint once the WAL passes this many bytes.
const DEFAULT_CHECKPOINT_BYTES: u64 = 8 * 1024 * 1024;

/// A continuous-aggregate rule: roll every numeric series of `measurement` up
/// to `interval`-wide buckets, materializing `aggs` into a derived measurement
/// `<measurement>@<label>` with fields `<field>_<agg>`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RollupSpec {
    pub measurement: String,
    pub label: String,
    pub interval: i64,
    pub aggs: Vec<Agg>,
}

/// A pinned generation to archive, produced by
/// [`Tsdb::backup_begin`] and consumed by [`Tsdb::backup_write`] /
/// [`Tsdb::backup_end`]. Holding it keeps that generation's files pinned, so
/// the archiving in between runs with the engine lock released.
pub struct TsdbBackup {
    generation: u64,
    wal_len: u64,
    dir: std::path::PathBuf,
}

/// The time-series database: a set of compressed series streams, optionally
/// persisted to disk.
#[derive(Default)]
pub struct Tsdb {
    series: BTreeMap<SeriesKey, store::Series>,
    str_series: BTreeMap<SeriesKey, store::StrSeries>,
    rollups: Vec<RollupSpec>,
    /// Last materialized bucket start per `"<series canonical>\x1f<interval>"`.
    watermark: BTreeMap<String, i64>,
    /// Points per sealed block; smaller = finer retention/query granularity,
    /// larger = better compression. 1024 is a reasonable default.
    block_points: usize,
    persist: Option<persist::Persist>,
    checkpoint_bytes: u64,
}

impl Tsdb {
    /// An in-memory database (no disk persistence).
    pub fn new() -> Self {
        Tsdb {
            series: BTreeMap::new(),
            str_series: BTreeMap::new(),
            rollups: Vec::new(),
            watermark: BTreeMap::new(),
            block_points: 1024,
            persist: None,
            checkpoint_bytes: DEFAULT_CHECKPOINT_BYTES,
        }
    }

    /// Open (or create) a database persisted under `dir`, loading its snapshot
    /// + WAL.
    pub fn open(dir: impl AsRef<Path>) -> std::io::Result<Self> {
        let mut series = BTreeMap::new();
        let mut str_series = BTreeMap::new();
        let mut watermark = BTreeMap::new();
        let block_points = 1024;
        let p = persist::Persist::open(
            dir.as_ref(),
            &mut series,
            &mut str_series,
            &mut watermark,
            block_points,
        )?;
        let rollups = persist::load_rollups(dir.as_ref());
        Ok(Tsdb {
            series,
            str_series,
            rollups,
            watermark,
            block_points,
            persist: Some(p),
            checkpoint_bytes: DEFAULT_CHECKPOINT_BYTES,
        })
    }

    /// Registered rollup rules.
    pub fn rollups(&self) -> &[RollupSpec] {
        &self.rollups
    }

    /// Register a continuous-aggregate rule (replacing any with the same
    /// measurement+label). Persisted so it survives restarts.
    pub fn add_rollup(&mut self, spec: RollupSpec) {
        self.rollups
            .retain(|r| !(r.measurement == spec.measurement && r.label == spec.label));
        self.rollups.push(spec);
        if let Some(p) = &self.persist {
            let _ = persist::save_rollups(p.dir(), &self.rollups);
        }
    }

    /// Materialize completed rollup buckets for all registered rules up to
    /// `now` (only buckets whose window has fully closed). Incremental via a
    /// per-series watermark. Returns the number of rollup points written.
    pub fn refresh_rollups(&mut self, now: i64) -> usize {
        let specs = self.rollups.clone();
        let mut to_write: Vec<Point> = Vec::new();
        let mut wm_updates: Vec<(String, i64)> = Vec::new();
        for spec in &specs {
            if spec.interval <= 0 {
                continue;
            }
            let last_complete = now - now.rem_euclid(spec.interval) - spec.interval;
            for (key, s) in &self.series {
                if key.measurement != spec.measurement {
                    continue;
                }
                let wmk = format!("{}\u{1f}{}", key.canonical(), spec.interval);
                let last_done = self.watermark.get(&wmk).copied();
                let range_start = match last_done {
                    Some(b) => b + spec.interval,
                    None => i64::MIN / 2,
                };
                let range_end = last_complete + spec.interval; // exclusive
                if range_end <= range_start {
                    continue;
                }
                let rows =
                    store::rollup_series(s, range_start, range_end, spec.interval, &spec.aggs);
                let mut max_bucket = last_done.unwrap_or(i64::MIN / 2);
                let roll_meas = format!("{}@{}", spec.measurement, spec.label);
                for (bts, vals) in rows {
                    let mut p = Point::new(&roll_meas, bts);
                    for (tk, tv) in &key.tags {
                        p = p.tag(tk, tv);
                    }
                    for (agg, v) in spec.aggs.iter().zip(vals) {
                        p = p.field(&format!("{}_{}", key.field, store::agg_name(*agg)), v);
                    }
                    to_write.push(p);
                    if bts > max_bucket {
                        max_bucket = bts;
                    }
                }
                wm_updates.push((wmk, max_bucket));
            }
        }
        let n = to_write.len();
        for p in &to_write {
            self.write(p);
        }
        for (k, b) in wm_updates {
            self.watermark.insert(k, b);
        }
        n
    }

    pub fn with_block_points(mut self, n: usize) -> Self {
        self.block_points = n.max(1);
        self
    }

    pub fn with_checkpoint_bytes(mut self, n: u64) -> Self {
        self.checkpoint_bytes = n.max(1);
        self
    }

    /// Ingest one point (expands to one series per field). Points should
    /// generally arrive in non-decreasing time order per series for best
    /// compression, but any order is accepted.
    pub fn write(&mut self, p: &Point) {
        for (fname, fval) in &p.fields {
            let key = SeriesKey::new(&p.measurement, p.tags.clone(), fname);
            if let FieldValue::Str(s) = fval {
                // Text field — separate storage path.
                if let Some(persist) = &mut self.persist {
                    let _ = persist.wal_append_str(&key, p.ts, s);
                }
                self.str_series
                    .entry(key)
                    .or_default()
                    .push(p.ts, s.clone());
                continue;
            }
            let f = fval.as_f64();
            let ft = fval.ftype();
            if let Some(persist) = &mut self.persist {
                let _ = persist.wal_append(&key, ft, p.ts, f);
            }
            let bp = self.block_points;
            let s = self.series.entry(key).or_default();
            s.set_ftype(ft);
            s.push(p.ts, f, bp);
        }
        if let Some(persist) = &mut self.persist {
            let _ = persist.flush();
            if persist.wal_bytes >= self.checkpoint_bytes {
                let _ = self.checkpoint();
            }
        }
    }

    /// Force-persist all data: seal active buffers, write a fresh snapshot, and
    /// rotate the WAL. No-op for an in-memory database.
    pub fn checkpoint(&mut self) -> std::io::Result<()> {
        if self.persist.is_none() {
            return Ok(());
        }
        for s in self.series.values_mut() {
            s.seal_active();
        }
        let persist = self.persist.as_mut().unwrap();
        persist.checkpoint(&self.series, &self.str_series, &self.watermark)
    }

    /// The on-disk data directory, or `None` for an in-memory database.
    pub fn data_dir(&self) -> Option<&Path> {
        self.persist.as_ref().map(|p| p.dir())
    }

    /// Consistent, compressed (`.tar.gz`) backup of this engine's data to `out`.
    /// Convenience wrapper around the low-lock [`backup_begin`](Tsdb::backup_begin)
    /// / [`backup_write`](Tsdb::backup_write) / [`backup_end`](Tsdb::backup_end)
    /// phases — safe to call while holding the engine exclusively. Returns the
    /// archive size; errors for an in-memory database.
    pub fn backup(&mut self, out: &Path) -> std::io::Result<u64> {
        let plan = self.backup_begin()?;
        let result = Self::backup_write(&plan, out);
        self.backup_end(&plan);
        result
    }

    /// Phase 1 (holds the caller's exclusive lock, O(1)): pin a committed
    /// generation and snapshot the WAL length. The returned plan can then be
    /// archived with [`backup_write`](Tsdb::backup_write) — with the engine lock
    /// **released** — because the pin keeps that generation's files on disk and
    /// its WAL prefix stable while writes and checkpoints continue.
    pub fn backup_begin(&mut self) -> std::io::Result<TsdbBackup> {
        let Some(p) = self.persist.as_mut() else {
            return Err(std::io::Error::other(
                "in-memory TSDB has no on-disk data to back up",
            ));
        };
        let (generation, wal_len) = p.pin_for_backup()?;
        Ok(TsdbBackup {
            generation,
            wal_len,
            dir: p.dir().to_path_buf(),
        })
    }

    /// Phase 3 (holds the caller's exclusive lock, O(1)): release the pin taken
    /// by [`backup_begin`](Tsdb::backup_begin), reclaiming the generation if a
    /// checkpoint superseded it during the backup.
    pub fn backup_end(&mut self, plan: &TsdbBackup) {
        if let Some(p) = self.persist.as_mut() {
            p.unpin_after_backup(plan.generation);
        }
    }

    /// Phase 2 (**no lock**): compress the pinned generation into `out` — a
    /// synthesized `MANIFEST`, that generation's block snapshot, a stable prefix
    /// of its WAL, and the rollup sidecars. Reads only pinned/immutable files.
    pub fn backup_write(plan: &TsdbBackup, out: &Path) -> std::io::Result<u64> {
        use std::io::Read;
        if out.exists() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("backup target already exists: {}", out.display()),
            ));
        }
        if let Some(parent) = out.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        let (dir, g) = (&plan.dir, plan.generation);

        let file = std::fs::File::create(out)?;
        let enc = flate2::write::GzEncoder::new(file, flate2::Compression::default());
        let mut ar = tar::Builder::new(enc);

        // Synthesized MANIFEST → the pinned generation.
        let manifest = serde_json::to_vec(&serde_json::json!({ "generation": g }))?;
        let mut hdr = tar::Header::new_gnu();
        hdr.set_size(manifest.len() as u64);
        hdr.set_mode(0o644);
        hdr.set_cksum();
        ar.append_data(&mut hdr, "MANIFEST", &manifest[..])?;

        // Block snapshot (absent until the first checkpoint — recovery then
        // rebuilds from the WAL alone).
        let blocks = dir.join(format!("blocks.{g}.tsb"));
        if blocks.exists() {
            ar.append_path_with_name(&blocks, format!("blocks.{g}.tsb"))?;
        }

        // Stable prefix of the per-generation WAL.
        let wal = dir.join(format!("wal.{g}.log"));
        if plan.wal_len > 0 && wal.exists() {
            let f = std::fs::File::open(&wal)?;
            let mut hdr = tar::Header::new_gnu();
            hdr.set_size(plan.wal_len);
            hdr.set_mode(0o644);
            hdr.set_cksum();
            ar.append_data(&mut hdr, format!("wal.{g}.log"), f.take(plan.wal_len))?;
        }

        // Rollup watermark sidecar (per-generation) + rule set (global).
        let wm = dir.join(format!("rollup_wm.{g}.json"));
        if wm.exists() {
            ar.append_path_with_name(&wm, format!("rollup_wm.{g}.json"))?;
        }
        let rollups = dir.join("rollups.json");
        if rollups.exists() {
            ar.append_path_with_name(&rollups, "rollups.json")?;
        }

        ar.into_inner()?.finish()?;
        Ok(std::fs::metadata(out)?.len())
    }

    /// Extract a `.tar.gz` backup produced by [`backup`](Tsdb::backup) into
    /// `target` (which must be empty or absent). Static: open a fresh `Tsdb` on
    /// `target` afterward to use the restored database.
    pub fn restore(archive: &Path, target: &Path) -> std::io::Result<()> {
        if !archive.exists() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("backup archive not found: {}", archive.display()),
            ));
        }
        if target.exists() {
            if std::fs::read_dir(target)?.next().is_some() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    format!("restore target is not empty: {}", target.display()),
                ));
            }
        } else {
            std::fs::create_dir_all(target)?;
        }
        let file = std::fs::File::open(archive)?;
        let dec = flate2::read::GzDecoder::new(file);
        tar::Archive::new(dec).unpack(target)?;
        Ok(())
    }

    /// True when a text field with this measurement+field name exists.
    pub fn is_string_field(&self, measurement: &str, field: &str) -> bool {
        self.str_series
            .keys()
            .any(|k| k.measurement == measurement && k.field == field)
    }

    /// Query a text field. Returns one group per tag combination.
    pub fn query_str(&self, spec: &QuerySpec) -> Vec<StrResultSeries> {
        store::run_query_str(&self.str_series, spec)
    }

    /// Number of distinct series (measurement × tag-set × field).
    pub fn series_count(&self) -> usize {
        self.series.len()
    }

    /// Total stored points across all series.
    pub fn point_count(&self) -> usize {
        self.series.values().map(|s| s.len()).sum::<usize>()
            + self.str_series.values().map(|s| s.len()).sum::<usize>()
    }

    /// On-disk-equivalent compressed byte size across all sealed blocks (the
    /// active buffer is counted as raw 16 bytes/point).
    pub fn compressed_bytes(&self) -> usize {
        self.series.values().map(|s| s.compressed_bytes()).sum()
    }

    /// Drop every whole block whose newest point is older than `cutoff`. Blocks
    /// are the retention unit — no per-point rewrite. Returns points removed.
    pub fn enforce_retention(&mut self, cutoff: i64) -> usize {
        let mut removed = 0;
        for s in self.series.values_mut() {
            removed += s.drop_before(cutoff);
        }
        self.series.retain(|_, s| !s.is_empty());
        for s in self.str_series.values_mut() {
            removed += s.drop_before(cutoff);
        }
        self.str_series.retain(|_, s| !s.is_empty());
        removed
    }

    /// Run a query. Returns one [`ResultSeries`] per output group.
    pub fn query(&self, spec: &QuerySpec) -> Vec<ResultSeries> {
        store::run_query(&self.series, spec)
    }
}
