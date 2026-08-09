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
mod persist;
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
    /// Bucket width in seconds; the window is `BUCKETS ×` this. Persisted in
    /// the snapshot and validated at open — reopening under a different
    /// width would silently re-interpret every counter's period.
    pub bucket_secs: u64,
    /// Baskets larger than this are skipped (counted in stats).
    pub max_basket: usize,
    /// Auto-checkpoint once the WAL passes this many bytes (`0` = manual
    /// only). Bounds recovery replay, like TSDB's 8 MiB default.
    pub checkpoint_bytes: u64,
}

impl Default for RecConfig {
    fn default() -> Self {
        Self {
            bucket_secs: DEFAULT_BUCKET_SECS,
            max_basket: DEFAULT_MAX_BASKET,
            checkpoint_bytes: 8 * 1024 * 1024,
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
    /// `None` = ephemeral (tests, embedding without a data dir).
    persist: Option<persist::Persist>,
}

impl Rec {
    /// An ephemeral engine — nothing touches disk.
    pub fn new(config: RecConfig) -> Self {
        Self {
            config,
            interner: Interner::default(),
            store: Store::default(),
            persist: None,
        }
    }

    /// Open (or create) a persistent engine at `dir`: load the authoritative
    /// generation's snapshot, then replay its WAL through the normal ingest
    /// path — replay IS ingestion, and the snapshotted seen-set makes
    /// re-applying already-folded records a no-op.
    pub fn open(dir: &std::path::Path, config: RecConfig) -> std::io::Result<Self> {
        let (p, snapshot, records) = persist::Persist::open(dir)?;
        let mut rec = match snapshot {
            Some(s) => {
                if s.bucket_secs != config.bucket_secs {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!(
                            "bucket_secs mismatch: snapshot has {}, config wants {} —                              periods would be silently re-interpreted",
                            s.bucket_secs, config.bucket_secs
                        ),
                    ));
                }
                Self {
                    config,
                    interner: s.interner,
                    store: s.store,
                    persist: Some(p),
                }
            }
            None => Self {
                config,
                interner: Interner::default(),
                store: Store::default(),
                persist: Some(p),
            },
        };
        for r in records {
            let items: Vec<&str> = r.items.iter().map(String::as_str).collect();
            rec.apply(&r.model, r.basket_id, &items, r.ts_secs);
        }
        Ok(rec)
    }

    /// Fold the WAL into a fresh generation snapshot; also runs the lazy
    /// shift's GC first so fully-expired rows never reach the file.
    pub fn checkpoint(&mut self, ts_secs: u64) -> std::io::Result<()> {
        self.gc(ts_secs);
        let Some(p) = &mut self.persist else {
            return Ok(());
        };
        let snap = persist::Snapshot {
            bucket_secs: self.config.bucket_secs,
            interner: std::mem::take(&mut self.interner),
            store: std::mem::take(&mut self.store),
        };
        let res = p.checkpoint(&snap);
        // Moved out only to serialize without a clone; always restored —
        // including on error.
        self.interner = snap.interner;
        self.store = snap.store;
        res.map(|_| ())
    }

    fn period(&self, ts_secs: u64) -> u32 {
        (ts_secs / self.config.bucket_secs.max(1)) as u32
    }

    /// Ingest one basket into `model`. Idempotent on `basket_id` within the
    /// counting window; duplicate items within the basket count once; a
    /// basket over the size cap is skipped and counted, never silently
    /// dropped. Returns whether the basket was counted.
    pub fn track(&mut self, model: &str, basket_id: u64, items: &[&str], ts_secs: u64) -> bool {
        let counted = self.apply(model, basket_id, items, ts_secs);
        if counted && let Some(p) = &mut self.persist {
            let rec = persist::WalRecord {
                model: model.to_string(),
                basket_id,
                items: items.iter().map(|s| s.to_string()).collect(),
                ts_secs,
            };
            if let Err(e) = p.append(&rec) {
                eprintln!("[rec] WAL append failed: {e}");
            }
            if self.config.checkpoint_bytes > 0
                && p.wal_bytes >= self.config.checkpoint_bytes
                && let Err(e) = self.checkpoint(ts_secs)
            {
                eprintln!("[rec] auto-checkpoint failed: {e}");
            }
        }
        counted
    }

    /// The pure ingest — counters only, no WAL. Recovery replays through
    /// this so a replayed record is never re-appended.
    fn apply(&mut self, model: &str, basket_id: u64, items: &[&str], ts_secs: u64) -> bool {
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
            "generation": self.persist.as_ref().map(|p| p.generation()),
            "wal_bytes": self.persist.as_ref().map(|p| p.wal_bytes),
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
