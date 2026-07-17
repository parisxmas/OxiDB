//! Sequence monotonicity across Raft failover — trade IDs / order
//! sequence numbers must stay strictly increasing, gapless, and
//! duplicate-free even when the leader dies and a new one takes over.
//! A gap = a lost trade; a duplicate = a double-applied trade; a
//! regression = reordering. All three corrupt an exchange's audit log.
//!
//! Model (a matching engine is single-threaded): one writer appends
//! trades carrying a strictly increasing `seq` (1, 2, 3, …) plus a
//! globally-unique `uid` (never reused across attempts) through the
//! current leader. Between batches the leader is killed; the writer
//! finds the new leader and, crucially, RE-READS the committed max seq
//! before resuming — and asserts it equals the last value it was ACKed
//! for. A quorum-acked write must survive any single leader failure
//! (core Raft durability), so:
//!
//!   - max(seq) after failover == last acked seq  → no acked write lost,
//!     no phantom write beyond what was acked;
//!   - final seqs == {1..N} exactly              → gapless + monotonic;
//!   - every seq unique, every (seq,uid) acked pair present with its
//!     uid                                       → no double-apply, no
//!     silent refill of a lost trade;
//!   - all surviving replicas hold the identical set → replication
//!     consistency.
//!
//! Run with:
//!   cargo test -p oxidb-server --features cluster --test raft_sequence_monotonic -- --nocapture

#![cfg(feature = "cluster")]

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use openraft::storage::Adaptor;
use oxidb::OxiDb;
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::sleep;

use oxidb_server::async_server::{self, ServerState};
use oxidb_server::raft::log_store::OxiDbStore;
use oxidb_server::raft::network::{self, OxiDbNetworkFactory};
use oxidb_server::raft::types::OxiRaft;

// ── Client ───────────────────────────────────────────────────────────

struct AsyncClient {
    stream: TcpStream,
}
impl AsyncClient {
    async fn connect(addr: SocketAddr) -> Self {
        let stream = TcpStream::connect(addr).await.expect("connect");
        stream.set_nodelay(true).ok();
        Self { stream }
    }
    async fn send(&mut self, request: &Value) -> Value {
        let payload = serde_json::to_vec(request).unwrap();
        let len = (payload.len() as u32).to_le_bytes();
        self.stream.write_all(&len).await.unwrap();
        self.stream.write_all(&payload).await.unwrap();
        self.stream.flush().await.unwrap();
        let mut lb = [0u8; 4];
        self.stream.read_exact(&mut lb).await.unwrap();
        let n = u32::from_le_bytes(lb) as usize;
        let mut buf = vec![0u8; n];
        self.stream.read_exact(&mut buf).await.unwrap();
        serde_json::from_slice(&buf).unwrap()
    }
}

// ── Node harness ─────────────────────────────────────────────────────

struct TestNode {
    node_id: u64,
    client_addr: SocketAddr,
    raft_addr: SocketAddr,
    _dir: TempDir,
    tasks: Vec<JoinHandle<()>>,
    raft: Arc<OxiRaft>,
    alive: bool,
}
impl TestNode {
    async fn kill(&mut self) {
        let _ = self.raft.shutdown().await;
        for h in &self.tasks {
            h.abort();
        }
        self.tasks.clear();
        self.alive = false;
    }
}

fn test_openraft_config() -> Arc<openraft::Config> {
    Arc::new(
        openraft::Config {
            heartbeat_interval: 200,
            election_timeout_min: 500,
            election_timeout_max: 1000,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    )
}

async fn allocate_port() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
}

async fn start_node(
    node_id: u64,
    client_addr: SocketAddr,
    raft_addr: SocketAddr,
    dir: &std::path::Path,
) -> (Arc<OxiRaft>, Vec<JoinHandle<()>>) {
    let db = Arc::new(OxiDb::open(dir).expect("open db"));
    let store = OxiDbStore::new(Arc::clone(&db));
    let (log_store, sm) = Adaptor::new(store);
    let raft = Arc::new(
        openraft::Raft::new(
            node_id,
            test_openraft_config(),
            OxiDbNetworkFactory,
            log_store,
            sm,
        )
        .await
        .expect("raft new"),
    );
    let state = Arc::new(ServerState {
        db,
        db_manager: None,
        user_store: None,
        audit_log: None,
        auth_enabled: false,
        raft: Some(Arc::clone(&raft)),
        raft_addr: Some(raft_addr.to_string()),
    });
    let mut tasks = Vec::new();
    let rc = Arc::clone(&raft);
    let rl = TcpListener::bind(raft_addr).await.unwrap();
    tasks.push(tokio::spawn(async move {
        while let Ok((s, _)) = rl.accept().await {
            let r = Arc::clone(&rc);
            tokio::spawn(async move { network::handle_raft_rpc(s, &r).await });
        }
    }));
    let sc = Arc::clone(&state);
    let cl = TcpListener::bind(client_addr).await.unwrap();
    tasks.push(tokio::spawn(async move {
        while let Ok((s, _)) = cl.accept().await {
            let st = Arc::clone(&sc);
            tokio::spawn(
                async move { async_server::handle_connection(s, st, Duration::ZERO).await },
            );
        }
    }));
    (raft, tasks)
}

async fn form_cluster(count: u64) -> Vec<TestNode> {
    let mut nodes = Vec::new();
    for id in 1..=count {
        let client_addr = allocate_port().await;
        let raft_addr = allocate_port().await;
        let dir = TempDir::new().unwrap();
        let (raft, tasks) = start_node(id, client_addr, raft_addr, dir.path()).await;
        nodes.push(TestNode {
            node_id: id,
            client_addr,
            raft_addr,
            _dir: dir,
            tasks,
            raft,
            alive: true,
        });
    }
    sleep(Duration::from_millis(50)).await;

    let mut c0 = AsyncClient::connect(nodes[0].client_addr).await;
    assert!(
        c0.send(&json!({"cmd": "raft_init"})).await["ok"]
            .as_bool()
            .unwrap()
    );
    for id in 2..=count {
        let idx = (id - 1) as usize;
        let r = c0
            .send(&json!({"cmd": "raft_add_learner", "node_id": id, "addr": nodes[idx].raft_addr.to_string()}))
            .await;
        assert!(r["ok"].as_bool().unwrap(), "add_learner {id}: {r}");
    }
    let members: Vec<u64> = (1..=count).collect();
    let r = c0
        .send(&json!({"cmd": "raft_change_membership", "members": members}))
        .await;
    assert!(r["ok"].as_bool().unwrap(), "change_membership: {r}");
    nodes
}

/// Find a live node currently reporting Leader; reconnect fresh.
async fn find_leader(nodes: &[TestNode], timeout: Duration) -> Option<(usize, AsyncClient)> {
    let start = tokio::time::Instant::now();
    loop {
        for (i, n) in nodes.iter().enumerate() {
            if !n.alive {
                continue;
            }
            let mut c = AsyncClient::connect(n.client_addr).await;
            let m = c.send(&json!({"cmd": "raft_metrics"})).await;
            if m["ok"].as_bool().unwrap_or(false) && m["data"]["state"].as_str() == Some("Leader") {
                return Some((i, c));
            }
        }
        if start.elapsed() > timeout {
            return None;
        }
        sleep(Duration::from_millis(150)).await;
    }
}

/// All `seq` values in the trades collection on one node.
async fn seqs_on(nodes: &[TestNode], idx: usize) -> Vec<i64> {
    let mut c = AsyncClient::connect(nodes[idx].client_addr).await;
    let r = c
        .send(&json!({"cmd": "find", "collection": "trades", "query": {}}))
        .await;
    r["data"]
        .as_array()
        .map(|a| a.iter().filter_map(|d| d["seq"].as_i64()).collect())
        .unwrap_or_default()
}

// ── The test ─────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn sequence_stays_monotonic_across_failovers() {
    const N: u64 = 5;
    const BATCH: i64 = 30;
    let mut nodes = form_cluster(N).await;

    // (seq, uid) pairs the writer received an OK ack for.
    let mut acked: Vec<(i64, i64)> = Vec::new();
    let mut next_seq: i64 = 1;
    let mut uid: i64 = 0;

    // Append `count` trades through whatever node is currently leader.
    async fn append_batch(
        nodes: &[TestNode],
        count: i64,
        next_seq: &mut i64,
        uid: &mut i64,
        acked: &mut Vec<(i64, i64)>,
    ) {
        let (_li, mut leader) = find_leader(nodes, Duration::from_secs(15))
            .await
            .expect("a leader");
        let target = *next_seq + count;
        while *next_seq < target {
            *uid += 1;
            let r = leader
                .send(&json!({"cmd": "insert", "collection": "trades",
                              "doc": {"seq": *next_seq, "uid": *uid}}))
                .await;
            if r["ok"].as_bool().unwrap_or(false) {
                acked.push((*next_seq, *uid));
                *next_seq += 1;
            } else {
                // Leader lost mid-batch: re-find and retry the SAME seq
                // with a fresh uid.
                if let Some((_i, l)) = find_leader(nodes, Duration::from_secs(15)).await {
                    leader = l;
                } else {
                    break;
                }
            }
        }
    }

    // Phase 1 — steady state.
    append_batch(&nodes, BATCH, &mut next_seq, &mut uid, &mut acked).await;
    let last_acked = acked.last().unwrap().0;
    println!("phase 1: appended through seq {last_acked}");

    // Two failovers, each: kill the current leader, then verify the
    // committed max seq survived, then append another batch on the new
    // leader.
    for failover in 1..=2 {
        let (li, _c) = find_leader(&nodes, Duration::from_secs(15))
            .await
            .expect("leader before kill");
        let killed = nodes[li].node_id;
        nodes[li].kill().await;
        println!("failover {failover}: killed leader node {killed}");

        // New leader among survivors, then re-read committed max seq.
        let (survivor, mut leader) = find_leader(&nodes, Duration::from_secs(20))
            .await
            .expect("new leader after kill");
        // Give the new leader a beat to commit a no-op and expose the
        // latest applied state, then re-read.
        sleep(Duration::from_millis(500)).await;
        let max_seq = seqs_on(&nodes, survivor)
            .await
            .into_iter()
            .max()
            .unwrap_or(0);
        let last_acked = acked.last().unwrap().0;
        assert_eq!(
            max_seq, last_acked,
            "failover {failover}: committed max seq {max_seq} != last acked {last_acked} \
             — a quorum-acked trade was lost or a phantom appeared across failover"
        );
        let _ = &mut leader;

        append_batch(&nodes, BATCH, &mut next_seq, &mut uid, &mut acked).await;
        println!(
            "failover {failover}: appended through seq {}",
            acked.last().unwrap().0
        );
    }

    // Let the last writes settle on all survivors.
    sleep(Duration::from_millis(500)).await;

    // ── Invariants over the final committed state ──
    let survivors: Vec<usize> = (0..N as usize).filter(|&i| nodes[i].alive).collect();
    let reference: Vec<i64> = {
        let mut s = seqs_on(&nodes, survivors[0]).await;
        s.sort_unstable();
        s
    };
    let max = *reference.last().unwrap();

    // 1. No duplicates + gapless + monotonic: sorted == [1..=max].
    let expected: Vec<i64> = (1..=max).collect();
    assert_eq!(
        reference, expected,
        "sequence must be exactly 1..={max}: gaps = lost trades, dups = double-applied"
    );

    // 2. Every acked (seq, uid) present with matching uid (no silent
    //    refill of a lost acked trade).
    let mut present: std::collections::HashMap<i64, i64> = std::collections::HashMap::new();
    {
        let mut c = AsyncClient::connect(nodes[survivors[0]].client_addr).await;
        let r = c
            .send(&json!({"cmd": "find", "collection": "trades", "query": {}}))
            .await;
        for d in r["data"].as_array().unwrap() {
            present.insert(d["seq"].as_i64().unwrap(), d["uid"].as_i64().unwrap());
        }
    }
    for (seq, u) in &acked {
        assert_eq!(
            present.get(seq),
            Some(u),
            "acked trade seq={seq} uid={u} missing or refilled with a different uid"
        );
    }

    // 3. All survivors hold the identical set.
    for &s in &survivors[1..] {
        let mut other = seqs_on(&nodes, s).await;
        other.sort_unstable();
        assert_eq!(
            other, reference,
            "survivor node {} diverged",
            nodes[s].node_id
        );
    }

    println!(
        "OK — {} trades, seq 1..={max} gapless/unique/monotonic across 2 failovers, \
         {} survivors agree, all {} acked trades intact",
        reference.len(),
        survivors.len(),
        acked.len()
    );

    // Sanity: reference really is a set of the right size.
    assert_eq!(
        reference.len(),
        BTreeSet::from_iter(reference.iter().copied()).len()
    );

    for n in &mut nodes {
        if n.alive {
            n.kill().await;
        }
    }
}
