//! Per-model counter store: ingest (`track`), the lazy bucket roll, the
//! bucketed seen-set, and the top-K queries. ADR-0025 §2, §3, §5.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::model::{BUCKETS, ModelStats, Row, Scoring, score};

/// A named event space (`purchase`, `view`, …). Signals with different base
/// rates are counted separately and blended at query time, never pooled.
#[derive(Default, Serialize, Deserialize)]
pub struct Model {
    /// Total baskets per period — one row for the whole model.
    pub(crate) baskets: Row,
    /// Baskets containing item x.
    pub(crate) item_counts: HashMap<u32, Row>,
    /// Baskets containing both x and y. Stored in BOTH directions: doubles
    /// counter memory, makes `related(x)` one row lookup — the hot path
    /// (ADR-0025 §2).
    pub(crate) pair_counts: HashMap<u32, HashMap<u32, Row>>,
    /// Basket ids seen per period, aged with the same window as the counters
    /// they guard: when a period leaves the window its ids leave with it, so
    /// the idempotence window equals the counting window and the set is
    /// bounded by one window of baskets (ADR-0025 §2). Slot i holds the ids
    /// of period `seen_epoch - i`.
    pub(crate) seen: [HashSet<u64>; BUCKETS],
    pub(crate) seen_epoch: u32,
    pub(crate) baskets_total: u64,
    pub(crate) baskets_skipped: u64,
    pub(crate) saturated: u64,
}

impl Model {
    fn new(now: u32) -> Self {
        Self {
            baskets: Row::new(now),
            seen_epoch: now,
            ..Default::default()
        }
    }

    /// Roll the seen-set to `now`, dropping the ids of expired periods.
    fn roll_seen(&mut self, now: u32) {
        debug_assert!(now >= self.seen_epoch);
        let behind = (now - self.seen_epoch) as usize;
        if behind == 0 {
            return;
        }
        if behind >= BUCKETS {
            for s in &mut self.seen {
                s.clear();
            }
        } else {
            // Shift toward the old end; the freed newest slots start empty.
            for i in (behind..BUCKETS).rev() {
                self.seen[i] = std::mem::take(&mut self.seen[i - behind]);
            }
            for s in self.seen.iter_mut().take(behind) {
                s.clear();
            }
        }
        self.seen_epoch = now;
    }

    fn seen_contains(&self, basket_id: u64) -> bool {
        self.seen.iter().any(|s| s.contains(&basket_id))
    }

    /// Ingest one deduplicated, size-checked basket of interned items.
    /// Returns false if the basket id was already counted.
    pub(crate) fn ingest(&mut self, basket_id: u64, items: &[u32], now: u32) -> bool {
        self.roll_seen(now);
        if self.seen_contains(basket_id) {
            return false;
        }
        self.seen[0].insert(basket_id);
        self.baskets_total += 1;
        if self.baskets.bump(now) {
            self.saturated += 1;
        }
        for &x in items {
            if self
                .item_counts
                .entry(x)
                .or_insert_with(|| Row::new(now))
                .bump(now)
            {
                self.saturated += 1;
            }
        }
        for (i, &x) in items.iter().enumerate() {
            for &y in &items[i + 1..] {
                for (a, b) in [(x, y), (y, x)] {
                    let row = self
                        .pair_counts
                        .entry(a)
                        .or_default()
                        .entry(b)
                        .or_insert_with(|| Row::new(now));
                    if row.bump(now) {
                        self.saturated += 1;
                    }
                }
            }
        }
        true
    }

    /// Drop rows that have fully expired as of `now` — the checkpoint-time GC
    /// the lazy shift defers to (ADR-0025 §3). Returns (items, pairs) dropped.
    pub(crate) fn gc(&mut self, now: u32) -> (usize, usize) {
        let expired = |r: &Row| now >= r.epoch && (now - r.epoch) as usize >= BUCKETS;
        let items_before = self.item_counts.len();
        self.item_counts.retain(|_, r| !expired(r));
        let mut pairs_dropped = 0;
        self.pair_counts.retain(|_, inner| {
            let before = inner.len();
            inner.retain(|_, r| !expired(r));
            pairs_dropped += before - inner.len();
            !inner.is_empty()
        });
        self.roll_seen(now);
        (items_before - self.item_counts.len(), pairs_dropped)
    }

    pub(crate) fn stats(&self) -> ModelStats {
        let pairs: usize = self.pair_counts.values().map(HashMap::len).sum();
        // Rough but honest: key + Row per entry, plus the hash-map load
        // factor; the directional doubling is already in `pairs`.
        let entry = std::mem::size_of::<Row>() + 4;
        let approx_bytes = (self.item_counts.len() + pairs) * entry * 2
            + self.seen.iter().map(HashSet::len).sum::<usize>() * 16;
        ModelStats {
            items: self.item_counts.len(),
            pairs,
            baskets: self.baskets_total,
            baskets_skipped: self.baskets_skipped,
            saturated_counters: self.saturated,
            approx_bytes,
        }
    }
}

/// Query parameters shared by `related` and `for_basket`.
#[derive(Clone, Debug)]
pub struct Query {
    pub scoring: Scoring,
    /// Exponential half-life in buckets (default 2); `0` = no decay.
    pub half_life: f64,
    /// Floor on the raw (undecayed sense: decayed) co-count.
    pub min_support: f64,
    pub limit: usize,
}

impl Default for Query {
    fn default() -> Self {
        Self {
            scoring: Scoring::default(),
            half_life: 2.0,
            min_support: 1.0,
            limit: 10,
        }
    }
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct Scored {
    pub item: u32,
    pub score: f64,
}

impl Model {
    /// Items most associated with `x`: one row lookup plus a partial sort.
    /// Cold start returns empty — no evidence is not a recommendation
    /// (ADR-0025 §5).
    pub(crate) fn related(&self, x: u32, now: u32, q: &Query) -> Vec<Scored> {
        let Some(row) = self.pair_counts.get(&x) else {
            return Vec::new();
        };
        let total = self.baskets.decayed(now, q.half_life);
        let n_x = self
            .item_counts
            .get(&x)
            .map_or(0.0, |r| r.decayed(now, q.half_life));
        let mut out: Vec<Scored> = row
            .iter()
            .filter_map(|(&y, pair)| {
                let co = pair.decayed(now, q.half_life);
                if co < q.min_support {
                    return None;
                }
                let n_y = self
                    .item_counts
                    .get(&y)
                    .map_or(0.0, |r| r.decayed(now, q.half_life));
                let s = score(q.scoring, co, n_x, n_y, total);
                (s > 0.0).then_some(Scored { item: y, score: s })
            })
            .collect();
        rank(&mut out, q.limit);
        out
    }

    /// Items most associated with a *set*: each candidate's score summed
    /// across the basket's members; the basket itself and `exclude` never
    /// appear (ADR-0025 §5).
    pub(crate) fn for_basket(
        &self,
        basket: &[u32],
        exclude: &[u32],
        now: u32,
        q: &Query,
    ) -> Vec<Scored> {
        let total = self.baskets.decayed(now, q.half_life);
        let mut acc: HashMap<u32, f64> = HashMap::new();
        for &x in basket {
            let Some(row) = self.pair_counts.get(&x) else {
                continue;
            };
            let n_x = self
                .item_counts
                .get(&x)
                .map_or(0.0, |r| r.decayed(now, q.half_life));
            for (&y, pair) in row {
                if basket.contains(&y) || exclude.contains(&y) {
                    continue;
                }
                let co = pair.decayed(now, q.half_life);
                if co < q.min_support {
                    continue;
                }
                let n_y = self
                    .item_counts
                    .get(&y)
                    .map_or(0.0, |r| r.decayed(now, q.half_life));
                let s = score(q.scoring, co, n_x, n_y, total);
                if s > 0.0 {
                    *acc.entry(y).or_insert(0.0) += s;
                }
            }
        }
        let mut out: Vec<Scored> = acc
            .into_iter()
            .map(|(item, score)| Scored { item, score })
            .collect();
        rank(&mut out, q.limit);
        out
    }
}

/// Order by score descending, ties broken by item id — the same determinism
/// lesson the FTS engine learned: map-iteration tie order breaks pagination
/// and differential tests alike.
fn rank(out: &mut Vec<Scored>, limit: usize) {
    out.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.item.cmp(&b.item))
    });
    out.truncate(limit);
}

/// All models of one database, plus the shared clock config.
#[derive(Default, Serialize, Deserialize)]
pub struct Store {
    pub(crate) models: HashMap<String, Model>,
}

impl Store {
    pub(crate) fn model_mut(&mut self, name: &str, now: u32) -> &mut Model {
        self.models
            .entry(name.to_string())
            .or_insert_with(|| Model::new(now))
    }

    pub(crate) fn model(&self, name: &str) -> Option<&Model> {
        self.models.get(name)
    }
}
