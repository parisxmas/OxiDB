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

/// The time-series database: a set of compressed series streams, optionally
/// persisted to disk.
#[derive(Default)]
pub struct Tsdb {
    series: BTreeMap<SeriesKey, store::Series>,
    str_series: BTreeMap<SeriesKey, store::StrSeries>,
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
        let block_points = 1024;
        let p = persist::Persist::open(dir.as_ref(), &mut series, &mut str_series, block_points)?;
        Ok(Tsdb {
            series,
            str_series,
            block_points,
            persist: Some(p),
            checkpoint_bytes: DEFAULT_CHECKPOINT_BYTES,
        })
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
        persist.checkpoint(&self.series, &self.str_series)
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
