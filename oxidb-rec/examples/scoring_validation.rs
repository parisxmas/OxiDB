//! ADR-0025 Phase 5: the engine's founding claim — that scoring choice
//! changes the recommendation lists *visibly*, and that LLR's lists are the
//! defensible ones — demonstrated on REAL orders, not fixtures.
//!
//! Dataset: UCI "Online Retail" (Chen, 2015; UK giftware e-commerce,
//! 541k order lines, Dec 2010–Dec 2011) converted to CSV. An invoice is a
//! basket; a StockCode is an item (descriptions are shown for reading).
//!
//! ```bash
//! cargo run --release -p oxidb-rec --example scoring_validation -- online_retail.csv
//! ```
//!
//! What to look for in the output, per probe item:
//! - `count` fills with the shop's bestsellers (the same names under every
//!   probe — base rate, not signal);
//! - `cosine` surfaces low-volume exclusive pairs;
//! - `llr` agrees with neither by default: its lists hold items with
//!   co-occurrence far above what the base rates predict.
//! The overlap summary at the end quantifies it.

use std::collections::HashMap;

use oxidb_rec::{Query, Rec, RecConfig, Scoring};

fn main() {
    let path = std::env::args().nth(1).unwrap_or_else(|| {
        eprintln!("usage: scoring_validation <online_retail.csv>");
        std::process::exit(2);
    });
    let data = std::fs::read_to_string(&path).expect("read csv");

    // InvoiceNo -> (basket id, items); descriptions for display.
    let mut baskets: HashMap<String, Vec<String>> = HashMap::new();
    let mut names: HashMap<String, String> = HashMap::new();
    for line in data.lines().skip(1) {
        let cols: Vec<&str> = split_csv(line);
        if cols.len() < 5 {
            continue;
        }
        let (invoice, stock, desc) = (cols[0], cols[1], cols[2]);
        // Cancellations start with 'C'; skip them and blank codes.
        if invoice.is_empty() || invoice.starts_with('C') || stock.is_empty() {
            continue;
        }
        baskets
            .entry(invoice.to_string())
            .or_default()
            .push(stock.to_string());
        if !desc.is_empty() {
            names
                .entry(stock.to_string())
                .or_insert_with(|| desc.to_string());
        }
    }

    let mut rec = Rec::new(RecConfig::default());
    let mut id = 0u64;
    let (mut loaded, mut skipped_hint) = (0u64, 0u64);
    for items in baskets.values() {
        let refs: Vec<&str> = items.iter().map(String::as_str).collect();
        if rec.track("purchase", id, &refs, 0) {
            loaded += 1;
        } else {
            skipped_hint += 1;
        }
        id += 1;
    }
    println!(
        "baskets: {loaded} loaded, {skipped_hint} skipped (size cap / empty)\n{}",
        serde_json::to_string_pretty(&rec.stats()).unwrap()
    );

    // Probes: bestsellers first (the count-mode trap in person), plus one
    // RARE item picked from the data — cosine's single-coincidence medal
    // only manifests when the probe's own margin is small, so a validation
    // that probes only the head would never see it.
    let mut item_freq: HashMap<&str, u32> = HashMap::new();
    for items in baskets.values() {
        let mut seen: Vec<&str> = items.iter().map(String::as_str).collect();
        seen.sort_unstable();
        seen.dedup();
        for it in seen {
            *item_freq.entry(it).or_insert(0) += 1;
        }
    }
    let rare_probe = item_freq
        .iter()
        .filter(|&(_, &n)| (5..=9).contains(&n))
        .map(|(&it, _)| it)
        .min() // deterministic pick
        .unwrap_or("85123A")
        .to_string();
    println!(
        "
rare probe: {} (in {} baskets)",
        rare_probe,
        item_freq[rare_probe.as_str()]
    );
    let probes = ["85123A", "22423", "20725", rare_probe.as_str()];
    let modes = [Scoring::Llr, Scoring::Cosine, Scoring::Count];

    let label = |code: &str| -> String {
        format!(
            "{code} ({})",
            names.get(code).map(String::as_str).unwrap_or("?")
        )
    };

    let mut lists: HashMap<(String, &'static str), Vec<String>> = HashMap::new();
    for probe in probes {
        println!("\n════ related({}) ════", label(probe));
        for mode in modes {
            let mode_name = match mode {
                Scoring::Llr => "llr",
                Scoring::Cosine => "cosine",
                Scoring::Count => "count",
                _ => unreachable!(),
            };
            let q = Query {
                scoring: mode,
                half_life: 0.0, // one year of data, no recency question here
                ..Query::default()
            };
            let out = rec.related("purchase", probe, 0, &q).unwrap();
            println!("  {mode_name:>7}:");
            for r in out.iter().take(5) {
                println!("    {:>10.2}  {}", r.score, label(&r.item));
            }
            lists.insert(
                (probe.to_string(), mode_name),
                out.iter().map(|r| r.item.clone()).collect(),
            );
        }
    }

    // Overlap: |top10(llr) ∩ top10(other)| per probe — the "visibly
    // different" claim as a number.
    println!("\n════ top-10 overlap with llr ════");
    for probe in probes {
        let llr = &lists[&(probe.to_string(), "llr")];
        for other in ["cosine", "count"] {
            let o = &lists[&(probe.to_string(), other)];
            let overlap = llr.iter().filter(|i| o.contains(i)).count();
            println!("  {}  llr∩{other}: {overlap}/10", label(probe));
        }
    }
}

/// Just enough CSV: split on commas outside double quotes (descriptions
/// contain commas).
fn split_csv(line: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let (mut start, mut in_q) = (0usize, false);
    let b = line.as_bytes();
    for (i, &c) in b.iter().enumerate() {
        match c {
            b'"' => in_q = !in_q,
            b',' if !in_q => {
                out.push(line[start..i].trim_matches('"'));
                start = i + 1;
            }
            _ => {}
        }
    }
    out.push(line[start..].trim_matches('"'));
    out
}
