//! Per-series storage (sealed Gorilla blocks + an active buffer) and the query
//! engine (tag filter → time/tag grouping → aggregation).

use std::collections::BTreeMap;

use crate::gorilla;
use crate::model::SeriesKey;

/// A sealed, compressed run of points, with its time span for pruning.
pub struct Block {
    pub bytes: Vec<u8>,
    pub min_ts: i64,
    pub max_ts: i64,
    pub count: usize,
}

/// One stored stream: sealed compressed blocks + an in-memory active buffer.
#[derive(Default)]
pub struct Series {
    sealed: Vec<Block>,
    active: Vec<(i64, f64)>,
}

impl Series {
    pub fn push(&mut self, ts: i64, val: f64, block_points: usize) {
        self.active.push((ts, val));
        if self.active.len() >= block_points {
            self.seal();
        }
    }

    /// Sealed blocks (for persistence snapshots).
    pub fn sealed_blocks(&self) -> &[Block] {
        &self.sealed
    }

    /// Adopt a block loaded from disk.
    pub fn push_block(&mut self, block: Block) {
        self.sealed.push(block);
    }

    /// Force the active buffer into a sealed block (used before a checkpoint
    /// snapshot so all data is in blocks).
    pub fn seal_active(&mut self) {
        self.seal();
    }

    fn seal(&mut self) {
        if self.active.is_empty() {
            return;
        }
        // Order within a block for compression + correct range scans.
        self.active.sort_by_key(|(t, _)| *t);
        let times: Vec<i64> = self.active.iter().map(|(t, _)| *t).collect();
        let vals: Vec<f64> = self.active.iter().map(|(_, v)| *v).collect();
        let bytes = gorilla::encode(&times, &vals);
        self.sealed.push(Block {
            bytes,
            min_ts: times[0],
            max_ts: *times.last().unwrap(),
            count: times.len(),
        });
        self.active.clear();
    }

    pub fn len(&self) -> usize {
        self.sealed.iter().map(|b| b.count).sum::<usize>() + self.active.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn compressed_bytes(&self) -> usize {
        self.sealed.iter().map(|b| b.bytes.len()).sum::<usize>() + self.active.len() * 16
    }

    /// Drop whole sealed blocks entirely older than `cutoff`, and trim the
    /// active buffer. Returns points removed.
    pub fn drop_before(&mut self, cutoff: i64) -> usize {
        let mut removed = 0;
        self.sealed.retain(|b| {
            if b.max_ts < cutoff {
                removed += b.count;
                false
            } else {
                true
            }
        });
        let before = self.active.len();
        self.active.retain(|(t, _)| *t >= cutoff);
        removed += before - self.active.len();
        removed
    }

    /// Visit every point in `[start, end)`, decoding only blocks that overlap.
    fn for_each_in<F: FnMut(i64, f64)>(&self, start: i64, end: i64, mut f: F) {
        for b in &self.sealed {
            if b.max_ts < start || b.min_ts >= end {
                continue;
            }
            let (times, vals) = gorilla::decode(&b.bytes);
            for (t, v) in times.into_iter().zip(vals) {
                if t >= start && t < end {
                    f(t, v);
                }
            }
        }
        for &(t, v) in &self.active {
            if t >= start && t < end {
                f(t, v);
            }
        }
    }
}

/// `key == value` tag filter (all predicates AND together).
#[derive(Debug, Clone)]
pub struct TagPredicate {
    pub key: String,
    pub value: String,
}

/// Aggregation applied per (tag-group, time-bucket).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Agg {
    Mean,
    Sum,
    Min,
    Max,
    Count,
    First,
    Last,
}

/// A query: what to select, filter, group and aggregate.
#[derive(Debug, Clone)]
pub struct QuerySpec {
    pub measurement: String,
    pub field: String,
    pub tag_filters: Vec<TagPredicate>,
    /// Half-open time range `[start, end)` in epoch millis.
    pub start: i64,
    pub end: i64,
    /// Group by these tag keys (in order). Empty = one group.
    pub group_tags: Vec<String>,
    /// Downsample bucket width in ms (`GROUP BY time(interval)`); `None` =
    /// a single bucket over the whole range.
    pub interval: Option<i64>,
    pub agg: Agg,
}

/// One output point: bucket start timestamp + aggregated value.
#[derive(Debug, Clone, PartialEq)]
pub struct GroupPoint {
    pub ts: i64,
    pub value: f64,
}

/// One output group (a distinct combination of the grouped tag values).
#[derive(Debug, Clone, PartialEq)]
pub struct ResultSeries {
    pub tags: Vec<(String, String)>,
    pub points: Vec<GroupPoint>,
}

#[derive(Default, Clone)]
struct Acc {
    sum: f64,
    count: u64,
    min: f64,
    max: f64,
    first_ts: i64,
    first_val: f64,
    last_ts: i64,
    last_val: f64,
}

impl Acc {
    fn add(&mut self, ts: i64, v: f64) {
        if self.count == 0 {
            self.min = v;
            self.max = v;
            self.first_ts = ts;
            self.first_val = v;
            self.last_ts = ts;
            self.last_val = v;
        } else {
            if v < self.min {
                self.min = v;
            }
            if v > self.max {
                self.max = v;
            }
            if ts < self.first_ts {
                self.first_ts = ts;
                self.first_val = v;
            }
            if ts >= self.last_ts {
                self.last_ts = ts;
                self.last_val = v;
            }
        }
        self.sum += v;
        self.count += 1;
    }
    fn value(&self, agg: Agg) -> f64 {
        match agg {
            Agg::Mean => self.sum / self.count as f64,
            Agg::Sum => self.sum,
            Agg::Min => self.min,
            Agg::Max => self.max,
            Agg::Count => self.count as f64,
            Agg::First => self.first_val,
            Agg::Last => self.last_val,
        }
    }
}

pub fn run_query(series: &BTreeMap<SeriesKey, Series>, spec: &QuerySpec) -> Vec<ResultSeries> {
    // group values (in group_tags order) → bucket ts → accumulator
    let mut groups: BTreeMap<Vec<String>, BTreeMap<i64, Acc>> = BTreeMap::new();

    for (key, s) in series {
        if key.measurement != spec.measurement || key.field != spec.field {
            continue;
        }
        if !spec
            .tag_filters
            .iter()
            .all(|p| key.tag(&p.key) == Some(p.value.as_str()))
        {
            continue;
        }
        let gvals: Vec<String> = spec
            .group_tags
            .iter()
            .map(|k| key.tag(k).unwrap_or("").to_string())
            .collect();
        let buckets = groups.entry(gvals).or_default();
        s.for_each_in(spec.start, spec.end, |t, v| {
            let bucket = match spec.interval {
                Some(iv) if iv > 0 => t - t.rem_euclid(iv),
                _ => spec.start,
            };
            buckets.entry(bucket).or_default().add(t, v);
        });
    }

    groups
        .into_iter()
        .map(|(gvals, buckets)| ResultSeries {
            tags: spec.group_tags.iter().cloned().zip(gvals).collect(),
            points: buckets
                .into_iter()
                .map(|(ts, acc)| GroupPoint {
                    ts,
                    value: acc.value(spec.agg),
                })
                .collect(),
        })
        .collect()
}
