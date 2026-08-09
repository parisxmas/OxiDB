//! Data model: the item interner, bucketed counter rows, scoring modes and
//! configuration. See ADR-0025 §2–§4.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Counter slots per row. Fixed on purpose (ADR-0025 §9): a stack array is
/// most of the per-pair memory argument, and the lazy-shift epoch scheme
/// assumes a fixed rotation.
pub const BUCKETS: usize = 8;

/// Hard cap on basket size (ADR-0025 §2): a 500-line order is `|B|²/2`
/// increments of bulk-import noise, not preference signal. Skips are counted
/// in [`ModelStats::baskets_skipped`], never silent.
pub const DEFAULT_MAX_BASKET: usize = 50;

/// Default bucket width. 30 days suits retail; the open question of
/// per-model widths (ADR-0025 §9.1) is deferred — width is per-engine config
/// for now.
pub const DEFAULT_BUCKET_SECS: u64 = 30 * 24 * 3600;

/// One counter row: `counts[0]` is the bucket for period `epoch`,
/// `counts[i]` the period `epoch - i`.
///
/// The shift is LAZY (ADR-0025 §3): nobody sweeps the map at a period
/// boundary — a row rotates when next touched, by however many periods it is
/// behind. Saturating adds throughout: a count pinned at the ceiling skews a
/// score, a wrapped one inverts it.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Row {
    pub counts: [u32; BUCKETS],
    /// The period `counts[0]` belongs to.
    pub epoch: u32,
}

impl Row {
    pub fn new(epoch: u32) -> Self {
        Self {
            counts: [0; BUCKETS],
            epoch,
        }
    }

    /// Rotate forward to `now`: each period ahead of `epoch` shifts one slot
    /// off the old end. More than `BUCKETS` behind = everything expired.
    pub fn roll_to(&mut self, now: u32) {
        debug_assert!(now >= self.epoch, "periods are monotonic");
        let behind = (now - self.epoch) as usize;
        if behind == 0 {
            return;
        }
        if behind >= BUCKETS {
            self.counts = [0; BUCKETS];
        } else {
            self.counts.copy_within(0..BUCKETS - behind, behind);
            self.counts[..behind].fill(0);
        }
        self.epoch = now;
    }

    /// Increment the current-period slot, after rolling to `now`.
    pub fn bump(&mut self, now: u32) -> bool {
        self.roll_to(now);
        let saturated = self.counts[0] == u32::MAX;
        self.counts[0] = self.counts[0].saturating_add(1);
        saturated
    }

    /// The decayed count as of period `now`: slot `i` (age `now - epoch + i`
    /// periods) is weighted `0.5^(age / half_life)`. `half_life` is in
    /// periods; `0` means no decay (all slots weigh 1).
    pub fn decayed(&self, now: u32, half_life: f64) -> f64 {
        let base_age = (now - self.epoch) as usize;
        let mut sum = 0.0;
        for (i, &c) in self.counts.iter().enumerate() {
            if c == 0 {
                continue;
            }
            let age = (base_age + i) as f64;
            if base_age + i >= BUCKETS {
                continue; // outside the window as of `now`
            }
            let w = if half_life <= 0.0 {
                1.0
            } else {
                0.5f64.powf(age / half_life)
            };
            sum += c as f64 * w;
        }
        sum
    }

    /// Used by tests today and by the Phase 2 snapshot writer's row filter.
    #[allow(dead_code)]
    pub fn is_zero(&self) -> bool {
        self.counts.iter().all(|&c| c == 0)
    }
}

/// How a candidate is scored from the decayed `co`, `n_x`, `n_y`, `N`
/// (ADR-0025 §4). LLR is the default for the reason stated there: cosine
/// hands a perfect score to a single coincidence, raw counts hand the list
/// to the bestseller.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Scoring {
    #[default]
    Llr,
    Cosine,
    Jaccard,
    Lift,
    Count,
}

impl std::str::FromStr for Scoring {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "llr" => Ok(Scoring::Llr),
            "cosine" => Ok(Scoring::Cosine),
            "jaccard" => Ok(Scoring::Jaccard),
            "lift" => Ok(Scoring::Lift),
            "count" => Ok(Scoring::Count),
            other => Err(format!(
                "unknown scoring {other:?} (llr, cosine, jaccard, lift, count)"
            )),
        }
    }
}

/// `x·log(x)` with the convention `xlogx(0) = 0`.
fn xlogx(x: f64) -> f64 {
    if x <= 0.0 { 0.0 } else { x * x.ln() }
}

/// Shannon-entropy helper over raw (unnormalized) cell values:
/// `H(v…) = xlogx(Σv) − Σ xlogx(vᵢ)`.
fn entropy2(a: f64, b: f64) -> f64 {
    xlogx(a + b) - xlogx(a) - xlogx(b)
}
fn entropy4(a: f64, b: f64, c: f64, d: f64) -> f64 {
    xlogx(a + b + c + d) - xlogx(a) - xlogx(b) - xlogx(c) - xlogx(d)
}

/// Dunning's log-likelihood ratio (G²) over the 2×2 contingency table,
/// computed in f64 because decay makes every count fractional. Clamped at 0:
/// floating error near independence can dip infinitesimally negative.
pub fn llr(co: f64, n_x: f64, n_y: f64, total: f64) -> f64 {
    let k11 = co;
    let k12 = (n_x - co).max(0.0);
    let k21 = (n_y - co).max(0.0);
    let k22 = (total - n_x - n_y + co).max(0.0);
    let row_e = entropy2(k11 + k12, k21 + k22);
    let col_e = entropy2(k11 + k21, k12 + k22);
    let cell_e = entropy4(k11, k12, k21, k22);
    (2.0 * (row_e + col_e - cell_e)).max(0.0)
}

/// Score one candidate pair under `mode`.
pub fn score(mode: Scoring, co: f64, n_x: f64, n_y: f64, total: f64) -> f64 {
    if co <= 0.0 {
        return 0.0;
    }
    match mode {
        Scoring::Llr => llr(co, n_x, n_y, total),
        Scoring::Cosine => co / (n_x * n_y).sqrt(),
        Scoring::Jaccard => co / (n_x + n_y - co),
        Scoring::Lift => {
            if total <= 0.0 {
                0.0
            } else {
                (co * total) / (n_x * n_y)
            }
        }
        Scoring::Count => co,
    }
}

/// Per-model statistics, reported by `stats` (ADR-0025 §5). Memory is
/// reported prominently because it grows with catalogue *diversity*, which is
/// harder to plan for than a row count.
#[derive(Clone, Debug, Default, Serialize)]
pub struct ModelStats {
    pub items: usize,
    pub pairs: usize,
    pub baskets: u64,
    pub baskets_skipped: u64,
    pub saturated_counters: u64,
    /// Estimated resident bytes of this model's counter maps.
    pub approx_bytes: usize,
}

/// An interner: items arrive as strings, counters speak u32. One per
/// database — every model shares the same catalogue (ADR-0025 §2).
#[derive(Default, Serialize, Deserialize)]
pub struct Interner {
    ids: HashMap<String, u32>,
    names: Vec<String>,
}

impl Interner {
    pub fn intern(&mut self, name: &str) -> u32 {
        if let Some(&id) = self.ids.get(name) {
            return id;
        }
        let id = self.names.len() as u32;
        self.names.push(name.to_string());
        self.ids.insert(name.to_string(), id);
        id
    }

    /// Lookup without creating — a query for an unknown item must not grow
    /// the catalogue.
    pub fn get(&self, name: &str) -> Option<u32> {
        self.ids.get(name).copied()
    }

    pub fn name(&self, id: u32) -> &str {
        &self.names[id as usize]
    }

    pub fn len(&self) -> usize {
        self.names.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roll_shifts_and_expires() {
        let mut r = Row::new(10);
        r.counts = [1, 2, 3, 4, 5, 6, 7, 8];
        r.roll_to(12);
        assert_eq!(r.counts, [0, 0, 1, 2, 3, 4, 5, 6]);
        assert_eq!(r.epoch, 12);
        r.roll_to(12); // no-op
        assert_eq!(r.counts, [0, 0, 1, 2, 3, 4, 5, 6]);
        r.roll_to(12 + BUCKETS as u32);
        assert!(r.is_zero(), "a full window behind = fully expired");
    }

    #[test]
    fn bump_saturates_instead_of_wrapping() {
        let mut r = Row::new(0);
        r.counts[0] = u32::MAX - 1;
        assert!(!r.bump(0));
        assert_eq!(r.counts[0], u32::MAX);
        assert!(r.bump(0), "the saturating bump must report itself");
        assert_eq!(r.counts[0], u32::MAX, "saturate, never wrap");
    }

    #[test]
    fn decay_weights_by_age_and_respects_the_window() {
        let mut r = Row::new(5);
        r.counts = [4, 0, 0, 0, 0, 0, 0, 4];
        // Half-life of 1 period: age 0 → ×1, age 7 → ×1/128.
        let d = r.decayed(5, 1.0);
        assert!((d - (4.0 + 4.0 / 128.0)).abs() < 1e-9);
        // As of two periods later, the old slot has aged out of the window
        // even though the row has not been rolled yet — decay must not count
        // what a roll would have dropped.
        let d = r.decayed(7, 0.0);
        assert!((d - 4.0).abs() < 1e-9);
        // No decay: plain sum inside the window.
        assert!((r.decayed(5, 0.0) - 8.0).abs() < 1e-9);
    }

    /// The §4 argument, as arithmetic: a single coincidence between two rare
    /// items is cosine's perfect score and LLR's shrug.
    #[test]
    fn llr_shrugs_at_a_single_coincidence_where_cosine_gives_it_a_medal() {
        let single = score(Scoring::Cosine, 1.0, 1.0, 1.0, 10_000.0);
        assert!((single - 1.0).abs() < 1e-9, "cosine: perfect score");

        let coincidence = score(Scoring::Llr, 1.0, 1.0, 1.0, 10_000.0);
        // A genuinely associated pair: 40 co-occurrences, each in 50 baskets.
        let real = score(Scoring::Llr, 40.0, 50.0, 50.0, 10_000.0);
        assert!(
            real > coincidence * 10.0,
            "LLR must rank the evidenced pair far above the coincidence \
             (real={real:.2}, coincidence={coincidence:.2})"
        );
    }

    #[test]
    fn llr_is_zero_under_independence_and_never_negative() {
        // co exactly at the independence expectation: n_x*n_y/N.
        let v = llr(10.0, 100.0, 1000.0, 10_000.0);
        assert!(v.abs() < 1e-6, "independent counts must score ~0, got {v}");
        for co in [0.5, 1.0, 5.0, 9.0] {
            assert!(llr(co, 100.0, 1000.0, 10_000.0) >= 0.0);
        }
    }

    #[test]
    fn interner_round_trips_and_get_does_not_create() {
        let mut i = Interner::default();
        let a = i.intern("elma");
        assert_eq!(i.intern("elma"), a);
        assert_eq!(i.get("elma"), Some(a));
        assert_eq!(i.get("armut"), None);
        assert_eq!(i.len(), 1, "a query must not grow the catalogue");
        assert_eq!(i.name(a), "elma");
    }
}
