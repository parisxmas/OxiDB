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
mod model;
mod store;

pub use model::{Point, SeriesKey};
pub use store::{Agg, Block, GroupPoint, QuerySpec, ResultSeries, TagPredicate};

use std::collections::BTreeMap;

/// The time-series database: a set of compressed series streams.
#[derive(Default)]
pub struct Tsdb {
    series: BTreeMap<SeriesKey, store::Series>,
    /// Points per sealed block; smaller = finer retention/query granularity,
    /// larger = better compression. 1024 is a reasonable default.
    block_points: usize,
}

impl Tsdb {
    pub fn new() -> Self {
        Tsdb {
            series: BTreeMap::new(),
            block_points: 1024,
        }
    }

    pub fn with_block_points(mut self, n: usize) -> Self {
        self.block_points = n.max(1);
        self
    }

    /// Ingest one point (expands to one series per field). Points should
    /// generally arrive in non-decreasing time order per series for best
    /// compression, but any order is accepted.
    pub fn write(&mut self, p: &Point) {
        for (fname, fval) in &p.fields {
            let key = SeriesKey::new(&p.measurement, p.tags.clone(), fname);
            let bp = self.block_points;
            self.series.entry(key).or_default().push(p.ts, *fval, bp);
        }
    }

    /// Number of distinct series (measurement × tag-set × field).
    pub fn series_count(&self) -> usize {
        self.series.len()
    }

    /// Total stored points across all series.
    pub fn point_count(&self) -> usize {
        self.series.values().map(|s| s.len()).sum()
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
        removed
    }

    /// Run a query. Returns one [`ResultSeries`] per output group.
    pub fn query(&self, spec: &QuerySpec) -> Vec<ResultSeries> {
        store::run_query(&self.series, spec)
    }
}
