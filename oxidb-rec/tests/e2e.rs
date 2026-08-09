//! ADR-0025's behavioral claims, each as a test: idempotence, the ingest
//! guards, cold start, the cart page, recency, lazy expiry, and that the
//! scoring choice visibly changes the list — the engine's founding argument.

use oxidb_rec::{Query, Rec, RecConfig, Scoring};

const DAY: u64 = 24 * 3600;

fn engine() -> Rec {
    Rec::new(RecConfig {
        bucket_secs: 30 * DAY,
        max_basket: 50,
        ..RecConfig::default()
    })
}

fn q(scoring: Scoring) -> Query {
    Query {
        scoring,
        ..Query::default()
    }
}

#[test]
fn related_finds_what_is_bought_together() {
    let mut rec = engine();
    // kahve+süt in most baskets; kahve+pil once (the coincidence).
    for i in 0..40u64 {
        rec.track("purchase", i, &["kahve", "süt"], 0);
    }
    for i in 40..60u64 {
        rec.track("purchase", i, &["kahve", "filtre"], 0);
    }
    rec.track("purchase", 60, &["kahve", "pil"], 0);
    for i in 61..200u64 {
        rec.track("purchase", i, &["ekmek"], 0); // corpus mass
    }

    let r = rec
        .related("purchase", "kahve", 0, &q(Scoring::Llr))
        .unwrap();
    let names: Vec<&str> = r.iter().map(|x| x.item.as_str()).collect();
    assert_eq!(names[0], "süt");
    assert_eq!(names[1], "filtre");
    // The single coincidence may appear, but never above the evidenced pairs.
    if let Some(pos) = names.iter().position(|&n| n == "pil") {
        assert!(pos > 1);
    }
}

/// The founding claim (§4): scoring choice changes the list VISIBLY.
/// Cosine's failure is the rare-pair medal; count's is the bestseller pull.
#[test]
fn scoring_modes_produce_visibly_different_lists() {
    let mut rec = engine();
    let mut id = 0u64;
    let mut track = |items: &[&str], times: usize, rec: &mut Rec| {
        for _ in 0..times {
            rec.track("p", id, items, 0);
            id += 1;
        }
    };
    // "çanta" co-occurs with the bestseller "poşet" 30 times — but "poşet"
    // is in EVERY basket, so that association is base-rate, not signal.
    // "cüzdan" co-occurs with "çanta" 12 times out of its 15 appearances.
    track(&["çanta", "poşet", "cüzdan"], 12, &mut rec);
    track(&["çanta", "poşet"], 18, &mut rec);
    track(&["poşet", "cüzdan"], 3, &mut rec);
    track(&["poşet", "başka"], 400, &mut rec);
    // One rare coincidence for the cosine trap.
    track(&["çanta", "tekbir"], 1, &mut rec);

    let list = |scoring| -> Vec<String> {
        rec.related("p", "çanta", 0, &q(scoring))
            .unwrap()
            .into_iter()
            .map(|r| r.item)
            .collect()
    };
    assert_eq!(
        list(Scoring::Count)[0],
        "poşet",
        "raw counts crown the bestseller"
    );
    let llr = list(Scoring::Llr);
    assert_eq!(llr[0], "cüzdan", "LLR finds the actually-associated item");
    assert_ne!(
        list(Scoring::Count),
        llr,
        "the founding claim: scoring choice changes the list"
    );
    // The other modes answer and rank the bestseller off the top too.
    assert_ne!(list(Scoring::Jaccard)[0], "poşet");
    assert_ne!(list(Scoring::Lift)[0], "poşet");
}

#[test]
fn track_is_idempotent_on_basket_id() {
    let mut rec = engine();
    assert!(rec.track("p", 1, &["a", "b"], 0));
    assert!(
        !rec.track("p", 1, &["a", "b"], 0),
        "same basket, same period"
    );
    let s = rec.stats();
    assert_eq!(s["models"]["p"]["baskets"], 1);
}

#[test]
fn duplicate_items_in_one_basket_count_once() {
    let mut rec = engine();
    rec.track("p", 1, &["a", "a", "a", "b"], 0);
    rec.track("p", 2, &["a", "b"], 0);
    let r = rec.related("p", "a", 0, &q(Scoring::Count)).unwrap();
    assert_eq!(r[0].score, 2.0, "a,a,a,b must count the pair once");
}

#[test]
fn oversized_baskets_are_skipped_and_reported() {
    let mut rec = Rec::new(RecConfig {
        bucket_secs: 30 * DAY,
        max_basket: 3,
        ..RecConfig::default()
    });
    let big: Vec<String> = (0..10).map(|i| format!("i{i}")).collect();
    let refs: Vec<&str> = big.iter().map(String::as_str).collect();
    assert!(!rec.track("p", 1, &refs, 0));
    // Dedup runs first: 10 mentions of 2 distinct items is a small basket.
    assert!(rec.track("p", 2, &["x", "x", "y", "x", "y", "x", "x"], 0));
    let s = rec.stats();
    assert_eq!(s["models"]["p"]["baskets_skipped"], 1);
    assert_eq!(s["models"]["p"]["baskets"], 1);
}

#[test]
fn cold_start_returns_empty_not_bestsellers() {
    let mut rec = engine();
    for i in 0..50u64 {
        rec.track("p", i, &["bestseller", "başka"], 0);
    }
    // Known item, no co-occurrence evidence:
    rec.track("p", 100, &["yalnız"], 0);
    assert!(
        rec.related("p", "yalnız", 0, &Query::default())
            .unwrap()
            .is_empty()
    );
    // Unknown item:
    assert!(
        rec.related("p", "hiçyok", 0, &Query::default())
            .unwrap()
            .is_empty()
    );
    // Unknown model:
    assert!(
        rec.related("yok", "bestseller", 0, &Query::default())
            .unwrap()
            .is_empty()
    );
}

#[test]
fn for_basket_excludes_the_basket_and_the_exclude_list() {
    let mut rec = engine();
    for i in 0..30u64 {
        rec.track("p", i, &["makarna", "sos", "peynir"], 0);
    }
    for i in 30..45u64 {
        rec.track("p", i, &["makarna", "fesleğen"], 0);
    }
    // Corpus mass without makarna: an item present in EVERY basket carries
    // zero co-occurrence information and LLR correctly scores it 0 — the
    // first version of this fixture proved that by accident.
    for i in 100..160u64 {
        rec.track("p", i, &["ekmek"], 0);
    }
    let r = rec
        .for_basket("p", &["makarna", "sos"], &["peynir"], 0, &Query::default())
        .unwrap();
    let names: Vec<&str> = r.iter().map(|x| x.item.as_str()).collect();
    assert!(
        !names.contains(&"makarna"),
        "the basket itself never appears"
    );
    assert!(!names.contains(&"sos"));
    assert!(!names.contains(&"peynir"), "exclude list honoured");
    assert_eq!(names[0], "fesleğen");
}

/// §3: recency is a query parameter. Last season's association loses to this
/// season's under decay, with no rebuild — and wins again with decay off.
#[test]
fn decay_makes_recency_a_query_parameter() {
    let mut rec = engine();
    // Season one (period 0): kahve+kupa, heavily.
    for i in 0..30u64 {
        rec.track("p", i, &["kahve", "kupa"], 0);
    }
    // Season two (period 5): kahve+termos, lighter but current.
    let t2 = 5 * 30 * DAY;
    for i in 100..115u64 {
        rec.track("p", i, &["kahve", "termos"], t2);
    }

    let now = t2;
    let fresh = rec
        .related(
            "p",
            "kahve",
            now,
            &Query {
                half_life: 1.0,
                ..q(Scoring::Count)
            },
        )
        .unwrap();
    assert_eq!(fresh[0].item, "termos", "decay favours this season");

    let all_time = rec
        .related(
            "p",
            "kahve",
            now,
            &Query {
                half_life: 0.0,
                ..q(Scoring::Count)
            },
        )
        .unwrap();
    assert_eq!(
        all_time[0].item, "kupa",
        "no decay favours the all-time count"
    );
}

/// The lazy shift: a row nobody touched for a full window contributes
/// nothing — even though no sweep ever ran — and `gc` then drops it.
#[test]
fn expired_counters_stop_counting_without_any_sweep() {
    let mut rec = engine();
    rec.track("p", 1, &["eski", "moda"], 0);
    let much_later = (oxidb_rec::BUCKETS as u64) * 30 * DAY;
    rec.track("p", 2, &["yeni", "moda"], much_later);

    let r = rec
        .related("p", "moda", much_later, &q(Scoring::Count))
        .unwrap();
    let names: Vec<&str> = r.iter().map(|x| x.item.as_str()).collect();
    assert_eq!(names, vec!["yeni"], "the expired pair must not answer");

    let (items, pairs) = rec.gc(much_later);
    assert!(items >= 1 && pairs >= 2, "gc reclaims the expired rows");
    // And the answer is unchanged after gc.
    let r = rec
        .related("p", "moda", much_later, &q(Scoring::Count))
        .unwrap();
    assert_eq!(r.len(), 1);
}

/// Idempotence windows equal the counting window (the ADR's bounded
/// seen-set): a basket id from an expired period may count again.
#[test]
fn the_idempotence_window_is_the_counting_window() {
    let mut rec = engine();
    assert!(rec.track("p", 7, &["a", "b"], 0));
    assert!(!rec.track("p", 7, &["a", "b"], 0));
    let past_window = (oxidb_rec::BUCKETS as u64 + 1) * 30 * DAY;
    assert!(
        rec.track("p", 7, &["a", "b"], past_window),
        "an id older than every live bucket has nothing left to be idempotent against"
    );
}

#[test]
fn models_are_separate_event_spaces() {
    let mut rec = engine();
    rec.track("view", 1, &["a", "b"], 0);
    rec.track("purchase", 2, &["a", "c"], 0);
    let views = rec.related("view", "a", 0, &q(Scoring::Count)).unwrap();
    let buys = rec.related("purchase", "a", 0, &q(Scoring::Count)).unwrap();
    assert_eq!(views[0].item, "b");
    assert_eq!(buys[0].item, "c");
    assert_eq!(views.len(), 1);
    assert_eq!(buys.len(), 1);
}

#[test]
fn ties_rank_deterministically() {
    let mut rec = engine();
    rec.track("p", 1, &["x", "aaa"], 0);
    rec.track("p", 2, &["x", "bbb"], 0);
    rec.track("p", 3, &["x", "ccc"], 0);
    let a = rec.related("p", "x", 0, &q(Scoring::Count)).unwrap();
    for _ in 0..10 {
        assert_eq!(rec.related("p", "x", 0, &q(Scoring::Count)).unwrap(), a);
    }
}

#[test]
fn bad_queries_are_refused_not_guessed() {
    let rec = engine();
    assert!(
        rec.related(
            "p",
            "x",
            0,
            &Query {
                limit: 0,
                ..Query::default()
            }
        )
        .is_err()
    );
    assert!(
        rec.related(
            "p",
            "x",
            0,
            &Query {
                half_life: f64::NAN,
                ..Query::default()
            }
        )
        .is_err()
    );
}
