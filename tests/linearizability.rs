//! Serializability checker — the gold-standard ACID test, Elle-style.
//!
//! "Money is conserved" is a domain invariant; it can hold while
//! isolation is violated. This instead checks the DEFINITION of
//! serializability: that the observed concurrent history is explainable
//! by SOME serial order — by building a transaction dependency graph and
//! proving it has no cycle (a cycle is a proof of non-serializability,
//! Adya's G0/G1c/G2).
//!
//! Technique (Elle / Adya): every write APPENDS a globally-unique value
//! to a key's list; every read observes the whole list. Append-only
//! lists give, for free, a total order of writes per key that any read
//! reveals — so dependency edges are directly recoverable from the data:
//!
//!   ww  (write→write):  consecutive appends v_i, v_{i+1} in a key's
//!                       final list ⇒ writer(v_i) → writer(v_{i+1}).
//!   wr  (write→read):   a txn that observed value v ⇒ writer(v) → reader.
//!   rw  (read→write, anti-dependency): a txn that read a list ending at
//!                       v_i (missing v_{i+1}) ⇒ reader → writer(v_{i+1}).
//!
//! G0 = ww-only cycle (dirty write), G1c = ww+wr cycle (circular info
//! flow), G2 = cycle needing an rw edge (write skew / anti-dependency).
//! OxiDB's OCC over item read-sets should admit NONE for key-addressed
//! access, so the graph must be acyclic. Extra cheap invariants pinned
//! alongside: every acked append lands exactly once, and every observed
//! read is a prefix of the key's final list (append-only can't reorder
//! or lose a value mid-history).
//!
//! Run with:
//!   cargo test --release --test linearizability -- --ignored --nocapture

use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use tempfile::tempdir;

use oxidb::{Error, OxiDb};

const KEYS: u64 = 20;
const WORKERS: usize = 8;
const TXNS_PER_WORKER: usize = 250;

/// What one committed transaction did, for graph construction.
#[derive(Default, Clone)]
struct TxnRecord {
    id: u64,
    appends: Vec<(u64, u64)>,    // (key, value)
    reads: Vec<(u64, Vec<u64>)>, // (key, observed list of values)
}

fn key_id(k: u64) -> String {
    format!("k{k}")
}

/// Run one transaction: 1–3 ops, each a read or a unique append, with
/// retry on OCC conflict. Returns the committed record (None if it hit a
/// non-conflict error, which shouldn't happen).
fn run_txn(
    db: &OxiDb,
    txn_id: u64,
    rng: &mut u64,
    value_ctr: &Arc<AtomicU64>,
) -> Option<TxnRecord> {
    // xorshift
    let next = |rng: &mut u64| {
        *rng ^= *rng << 13;
        *rng ^= *rng >> 7;
        *rng ^= *rng << 17;
        *rng
    };

    loop {
        let tx = db.begin_transaction();
        let mut rec = TxnRecord {
            id: txn_id,
            ..Default::default()
        };
        let n_ops = 1 + (next(rng) % 3) as usize;
        let mut ok = true;
        for _ in 0..n_ops {
            let key = next(rng) % KEYS;
            let is_append = next(rng) % 2 == 0;
            if is_append {
                let v = value_ctr.fetch_add(1, Ordering::SeqCst);
                if db
                    .tx_update(
                        tx,
                        "reg",
                        &json!({"id": key_id(key)}),
                        &json!({"$push": {"log": v}}),
                    )
                    .is_err()
                {
                    ok = false;
                    break;
                }
                rec.appends.push((key, v));
            } else {
                match db.tx_find(tx, "reg", &json!({"id": key_id(key)})) {
                    Ok(docs) => {
                        let list: Vec<u64> = docs
                            .first()
                            .and_then(|d| d.get("log"))
                            .and_then(|l| l.as_array())
                            .map(|a| a.iter().filter_map(Value::as_u64).collect())
                            .unwrap_or_default();
                        rec.reads.push((key, list));
                    }
                    Err(_) => {
                        ok = false;
                        break;
                    }
                }
            }
        }
        if !ok {
            db.rollback_transaction(tx).ok();
            continue;
        }
        match db.commit_transaction(tx) {
            Ok(()) => return Some(rec),
            Err(Error::TransactionConflict { .. }) => continue,
            Err(_) => return None,
        }
    }
}

/// Find a cycle in a directed graph via DFS; return one cycle's node ids
/// if any, else None.
fn find_cycle(adj: &HashMap<u64, Vec<u64>>) -> Option<Vec<u64>> {
    #[derive(Clone, Copy, PartialEq)]
    enum Mark {
        White,
        Gray,
        Black,
    }
    let mut color: HashMap<u64, Mark> = adj.keys().map(|&n| (n, Mark::White)).collect();
    // Iterative DFS carrying the path, so a back-edge to a Gray node
    // yields the actual cycle.
    for &start in adj.keys() {
        if color[&start] != Mark::White {
            continue;
        }
        let mut stack: Vec<(u64, usize)> = vec![(start, 0)];
        let mut path: Vec<u64> = vec![start];
        color.insert(start, Mark::Gray);
        while let Some(&(node, idx)) = stack.last() {
            let neighbors = adj.get(&node).map(|v| v.as_slice()).unwrap_or(&[]);
            if idx < neighbors.len() {
                stack.last_mut().unwrap().1 += 1;
                let nxt = neighbors[idx];
                match color.get(&nxt).copied().unwrap_or(Mark::Black) {
                    Mark::White => {
                        color.insert(nxt, Mark::Gray);
                        stack.push((nxt, 0));
                        path.push(nxt);
                    }
                    Mark::Gray => {
                        // Back edge → cycle from nxt..=node.
                        let pos = path.iter().position(|&x| x == nxt).unwrap();
                        return Some(path[pos..].to_vec());
                    }
                    Mark::Black => {}
                }
            } else {
                color.insert(node, Mark::Black);
                stack.pop();
                path.pop();
            }
        }
    }
    None
}

// The checker is only as trustworthy as its cycle detector — prove it
// catches cycles and clears acyclic graphs. Runs in the normal suite
// (not #[ignore]): if this ever breaks, `history_is_serializable`'s
// green would be meaningless.
#[test]
fn cycle_detector_is_correct() {
    let g = |edges: &[(u64, u64)]| -> HashMap<u64, Vec<u64>> {
        let mut m: HashMap<u64, Vec<u64>> = HashMap::new();
        for &(a, b) in edges {
            m.entry(a).or_default().push(b);
            m.entry(b).or_default();
        }
        m
    };
    // acyclic DAG
    assert!(find_cycle(&g(&[(1, 2), (2, 3), (1, 3)])).is_none());
    // 3-cycle
    let c = find_cycle(&g(&[(1, 2), (2, 3), (3, 1)])).expect("must find cycle");
    assert_eq!(c.len(), 3);
    // 2-cycle (mutual dependency = G0/G1c shape)
    let c = find_cycle(&g(&[(1, 2), (2, 1)])).expect("must find cycle");
    assert_eq!(c.len(), 2);
    // cycle buried in a larger graph
    assert!(find_cycle(&g(&[(1, 2), (2, 3), (3, 4), (4, 2), (5, 1)])).is_some());
}

#[test]
#[ignore]
fn history_is_serializable() {
    let dir = tempdir().unwrap();
    let db = Arc::new(OxiDb::open(dir.path()).unwrap());
    for k in 0..KEYS {
        db.insert("reg", json!({"id": key_id(k), "log": []}))
            .unwrap();
    }
    db.create_index("reg", "id").unwrap();

    let value_ctr = Arc::new(AtomicU64::new(1));
    let txn_ctr = Arc::new(AtomicU64::new(1));
    let records: Arc<Mutex<Vec<TxnRecord>>> = Arc::new(Mutex::new(Vec::new()));

    let handles: Vec<_> = (0..WORKERS)
        .map(|w| {
            let db = Arc::clone(&db);
            let value_ctr = Arc::clone(&value_ctr);
            let txn_ctr = Arc::clone(&txn_ctr);
            let records = Arc::clone(&records);
            thread::spawn(move || {
                let mut rng = 0x9E37_79B9_7F4A_7C15u64.wrapping_mul(w as u64 + 1) | 1;
                for _ in 0..TXNS_PER_WORKER {
                    let id = txn_ctr.fetch_add(1, Ordering::SeqCst);
                    if let Some(rec) = run_txn(&db, id, &mut rng, &value_ctr) {
                        records.lock().unwrap().push(rec);
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    let records = Arc::try_unwrap(records)
        .unwrap_or_else(|_| panic!("outstanding record refs"))
        .into_inner()
        .unwrap();
    println!(
        "\nserializability check: {} committed txns, {} keys, {} workers",
        records.len(),
        KEYS,
        WORKERS
    );

    // Ground truth: each key's final append order.
    let mut final_lists: HashMap<u64, Vec<u64>> = HashMap::new();
    for k in 0..KEYS {
        let doc = db
            .find_one("reg", &json!({"id": key_id(k)}))
            .unwrap()
            .unwrap();
        let list: Vec<u64> = doc["log"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(Value::as_u64)
            .collect();
        final_lists.insert(k, list);
    }
    let writer_of: HashMap<u64, u64> = records
        .iter()
        .flat_map(|r| r.appends.iter().map(move |&(_, v)| (v, r.id)))
        .collect();
    // position of a value within its key's final list
    let mut pos_in_key: HashMap<(u64, u64), usize> = HashMap::new();
    for (&k, list) in &final_lists {
        for (i, &v) in list.iter().enumerate() {
            pos_in_key.insert((k, v), i);
        }
    }

    // ── Invariant 1: every acked append lands exactly once. ──
    let mut appended: HashMap<u64, u64> = HashMap::new(); // value -> key
    for r in &records {
        for &(k, v) in &r.appends {
            assert!(appended.insert(v, k).is_none(), "value {v} appended twice");
        }
    }
    let mut in_final: HashSet<u64> = HashSet::new();
    for list in final_lists.values() {
        for &v in list {
            assert!(
                in_final.insert(v),
                "value {v} appears twice in final lists (double-apply)"
            );
        }
    }
    for (&v, _) in &appended {
        assert!(
            in_final.contains(&v),
            "acked append {v} lost from final state"
        );
    }
    assert_eq!(
        appended.len(),
        in_final.len(),
        "phantom value in final state"
    );

    // ── Invariant 2: every observed read is a PREFIX of its key's final
    //    list — append-only histories can't reorder or drop a value. ──
    for r in &records {
        for (k, observed) in &r.reads {
            let final_list = &final_lists[k];
            assert!(
                observed.len() <= final_list.len() && final_list[..observed.len()] == observed[..],
                "read of key {k} by txn {} is not a prefix of the final list \
                 (linearizability violation): observed {observed:?} vs final {final_list:?}",
                r.id
            );
        }
    }

    // ── Invariant 3: build ww + wr + rw dependency graph, assert acyclic. ──
    let mut adj: HashMap<u64, Vec<u64>> = records.iter().map(|r| (r.id, Vec::new())).collect();
    let mut edges: HashSet<(u64, u64)> = HashSet::new();
    let mut add = |adj: &mut HashMap<u64, Vec<u64>>, a: u64, b: u64| {
        if a != b && edges.insert((a, b)) {
            adj.entry(a).or_default().push(b);
        }
    };

    // ww: consecutive appends in each key's final list.
    for list in final_lists.values() {
        for w in list.windows(2) {
            if let (Some(&t1), Some(&t2)) = (writer_of.get(&w[0]), writer_of.get(&w[1])) {
                add(&mut adj, t1, t2);
            }
        }
    }
    // wr + rw: from each read's observed prefix.
    for r in &records {
        for (k, observed) in &r.reads {
            if let Some(&last) = observed.last() {
                // wr: reader saw `last` → writer(last) → reader.
                if let Some(&tw) = writer_of.get(&last) {
                    add(&mut adj, tw, r.id);
                }
                // rw: reader missed the NEXT value in the final list →
                // reader → writer(next).
                let p = pos_in_key[&(*k, last)];
                if let Some(&next_v) = final_lists[k].get(p + 1) {
                    if let Some(&tn) = writer_of.get(&next_v) {
                        add(&mut adj, r.id, tn);
                    }
                }
            } else {
                // Read an empty list → reader precedes the FIRST writer.
                if let Some(&first) = final_lists[k].first() {
                    if let Some(&tf) = writer_of.get(&first) {
                        add(&mut adj, r.id, tf);
                    }
                }
            }
        }
    }

    let edge_count = edges.len();
    match find_cycle(&adj) {
        None => println!(
            "OK — history serializable: {} txns, {edge_count} ww/wr/rw dependency edges, \
             dependency graph ACYCLIC (no G0/G1c/G2 anomaly)",
            records.len()
        ),
        Some(cycle) => panic!(
            "NON-SERIALIZABLE: dependency cycle among committed txns {cycle:?} — \
             the concurrent history has no equivalent serial order"
        ),
    }
}
