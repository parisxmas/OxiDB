//! OxiDB's fourth engine: real-time item-to-item co-occurrence
//! recommendations ("customers who bought this also bought"), per ADR-0025.
//!
//! The work is shaped like an index, not a query: the unit is a basket, the
//! update is a pair-count increment independent of catalogue size, the read
//! is a partial sort over one sparse counter row, and the scoring (Dunning's
//! log-likelihood ratio by default) has a right answer the engine implements
//! once instead of every caller re-deriving it wrong. It is not vector
//! search (that answers "what resembles this" from an embedding; this
//! answers "what is actually bought beside this" from observed behaviour)
//! and not a graph traversal (two hops from a bestseller is millions of
//! paths; the right structure is an adjacency counter).
//!
//! Phase 1: the in-memory engine — `track` / `related` / `for_basket` /
//! `stats`. Persistence (MANIFEST + snapshot + WAL) is Phase 2; the server
//! bridge is Phase 3.

mod model;
mod store;

pub use model::{BUCKETS, DEFAULT_BUCKET_SECS, DEFAULT_MAX_BASKET, ModelStats, Scoring};
pub use store::{Query, Scored};

use std::collections::BTreeSet;

use model::Interner;
use store::Store;

#[derive(Debug)]
pub enum RecError {
    /// Query-side: an unknown scoring mode, a zero limit, …
    BadRequest(String),
}

impl std::fmt::Display for RecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecError::BadRequest(m) => write!(f, "{m}"),
        }
    }
}
impl std::error::Error for RecError {}

pub type Result<T> = std::result::Result<T, RecError>;

/// Engine configuration, fixed at open.
#[derive(Clone, Debug)]
pub struct RecConfig {
    /// Bucket width in seconds; the window is `BUCKETS ×` this.
    pub bucket_secs: u64,
    /// Baskets larger than this are skipped (counted in stats).
    pub max_basket: usize,
}

impl Default for RecConfig {
    fn default() -> Self {
        Self {
            bucket_secs: DEFAULT_BUCKET_SECS,
            max_basket: DEFAULT_MAX_BASKET,
        }
    }
}

/// A scored recommendation, resolved back to the item's name.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct Recommendation {
    pub item: String,
    pub score: f64,
}

/// The engine facade. Callers pass wall-clock timestamps (epoch seconds) on
/// every operation — the engine holds no clock of its own, which keeps tests
/// exact and replay deterministic (the same discipline as the workflow
/// engine's Date-ban, for the same reason).
pub struct Rec {
    config: RecConfig,
    interner: Interner,
    store: Store,
}

impl Rec {
    pub fn new(config: RecConfig) -> Self {
        Self {
            config,
            interner: Interner::default(),
            store: Store::default(),
        }
    }

    fn period(&self, ts_secs: u64) -> u32 {
        (ts_secs / self.config.bucket_secs.max(1)) as u32
    }

    /// Ingest one basket into `model`. Idempotent on `basket_id` within the
    /// counting window; duplicate items within the basket count once; a
    /// basket over the size cap is skipped and counted, never silently
    /// dropped. Returns whether the basket was counted.
    pub fn track(&mut self, model: &str, basket_id: u64, items: &[&str], ts_secs: u64) -> bool {
        let now = self.period(ts_secs);
        // Dedup BEFORE the size cap: variants of one item are one occurrence,
        // and a 60-line order of 40 distinct items is within the cap.
        let distinct: BTreeSet<&str> = items.iter().copied().collect();
        let m = self.store.model_mut(model, now);
        if distinct.is_empty() {
            return false;
        }
        if distinct.len() > self.config.max_basket {
            m.baskets_skipped += 1;
            return false;
        }
        let interned: Vec<u32> = distinct
            .into_iter()
            .map(|s| self.interner.intern(s))
            .collect();
        self.store
            .model_mut(model, now)
            .ingest(basket_id, &interned, now)
    }

    /// Items most associated with `item` — empty when the item is unknown or
    /// has no co-occurrence evidence (the caller owns any fallback).
    pub fn related(
        &self,
        model: &str,
        item: &str,
        ts_secs: u64,
        q: &Query,
    ) -> Result<Vec<Recommendation>> {
        validate(q)?;
        let now = self.period(ts_secs);
        let (Some(m), Some(x)) = (self.store.model(model), self.interner.get(item)) else {
            return Ok(Vec::new());
        };
        Ok(self.resolve(m.related(x, now, q)))
    }

    /// Items most associated with the basket as a set — the cart page. The
    /// basket's own items and `exclude` never appear.
    pub fn for_basket(
        &self,
        model: &str,
        basket: &[&str],
        exclude: &[&str],
        ts_secs: u64,
        q: &Query,
    ) -> Result<Vec<Recommendation>> {
        validate(q)?;
        let now = self.period(ts_secs);
        let Some(m) = self.store.model(model) else {
            return Ok(Vec::new());
        };
        // Unknown items simply contribute nothing.
        let ids: Vec<u32> = basket.iter().filter_map(|s| self.interner.get(s)).collect();
        let excl: Vec<u32> = exclude
            .iter()
            .filter_map(|s| self.interner.get(s))
            .collect();
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        Ok(self.resolve(m.for_basket(&ids, &excl, now, q)))
    }

    /// Per-model statistics plus the interner size.
    pub fn stats(&self) -> serde_json::Value {
        let models: serde_json::Map<String, serde_json::Value> = self
            .store
            .models
            .iter()
            .map(|(name, m)| {
                (
                    name.clone(),
                    serde_json::to_value(m.stats()).expect("stats serialize"),
                )
            })
            .collect();
        serde_json::json!({
            "catalogue_items": self.interner.len(),
            "bucket_secs": self.config.bucket_secs,
            "buckets": BUCKETS,
            "models": models,
        })
    }

    /// Drop fully-expired counter rows (the lazy shift's deferred GC) —
    /// called by the checkpoint in Phase 2, callable directly meanwhile.
    pub fn gc(&mut self, ts_secs: u64) -> (usize, usize) {
        let now = self.period(ts_secs);
        let mut dropped = (0, 0);
        for m in self.store.models.values_mut() {
            let (i, p) = m.gc(now);
            dropped.0 += i;
            dropped.1 += p;
        }
        dropped
    }

    fn resolve(&self, scored: Vec<store::Scored>) -> Vec<Recommendation> {
        scored
            .into_iter()
            .map(|s| Recommendation {
                item: self.interner.name(s.item).to_string(),
                score: s.score,
            })
            .collect()
    }
}

fn validate(q: &Query) -> Result<()> {
    if q.limit == 0 {
        return Err(RecError::BadRequest("limit must be at least 1".into()));
    }
    if !q.half_life.is_finite() || q.half_life < 0.0 {
        return Err(RecError::BadRequest(
            "half_life must be a non-negative number".into(),
        ));
    }
    Ok(())
}
