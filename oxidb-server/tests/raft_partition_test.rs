//! Jepsen-style network-partition test for the Raft cluster.
//!
//! The existing `raft_test.rs` injects faults by *killing* nodes
//! (`raft.shutdown()`). A crash-stop is not a partition: a killed node
//! loses its tasks, whereas a *partitioned* node stays alive, keeps its
//! state and its (now stale) leadership belief, and must reconcile when
//! the partition heals. The reconcile path — minority nodes discarding
//! uncommitted entries, catching up the leader's log, and converging —
//! is exactly where split-brain and lost-write bugs hide, and it is
//! only reachable with a real partition.
//!
//! Rather than shell out to docker + iptables (slow, flaky, not in CI),
//! we inject the partition deterministically at the transport: openraft
//! reaches every peer through `RaftNetwork`, so a wrapper factory that
//! returns `Unreachable` for cut directed edges — consulting a shared
//! matrix — is a faithful, deterministic network partition. openraft
//! sees exactly what it would see if the packets were dropped: votes and
//! AppendEntries to the far side fail, the minority can't reach quorum,
//! the majority elects/keeps a leader.
//!
//! Invariants checked each round (the Jepsen bank/register checks
//! adapted to a partition nemesis):
//!   1. **No lost acked writes** — every write the majority ACKed
//!      survives on every node after healing.
//!   2. **No split-brain** — the isolated minority (incl. the old
//!      leader, which we deliberately strand there) cannot commit: its
//!      writes fail, and none of them ever appear post-heal.
//!   3. **Availability under quorum** — the majority side keeps
//!      committing during the partition.
//!   4. **Convergence** — after heal, all N nodes hold the identical
//!      set of committed documents.
//!
//! Marked `#![cfg(feature = "cluster")]`; run with:
//!   cargo test -p oxidb-server --features cluster --test raft_partition_test -- --nocapture

#![cfg(feature = "cluster")]

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use openraft::error::{InstallSnapshotError, RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::storage::Adaptor;
use openraft::BasicNode;
use oxidb::OxiDb;
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::sleep;

use oxidb_server::async_server::{self, ServerState};
use oxidb_server::raft::log_store::OxiDbStore;
use oxidb_server::raft::network::{OxiDbNetwork, OxiDbNetworkFactory};
use oxidb_server::raft::types::{OxiRaft, TypeConfig};

// ---------------------------------------------------------------------------
// Partitioner — the shared cut matrix, consulted on every peer RPC
// ---------------------------------------------------------------------------

/// Directed-edge partition state. `blocked` holds `(from, to)` node-id
/// pairs whose RPCs must fail. Symmetric cuts insert both directions.
#[derive(Default)]
struct Partitioner {
    blocked: Mutex<HashSet<(u64, u64)>>,
}

impl Partitioner {
    /// Fully isolate the two groups from each other (both directions),
    /// leaving intra-group traffic intact.
    fn cut(&self, group_a: &[u64], group_b: &[u64]) {
        let mut b = self.blocked.lock().unwrap();
        for &x in group_a {
            for &y in group_b {
                b.insert((x, y));
                b.insert((y, x));
            }
        }
    }

    fn heal(&self) {
        self.blocked.lock().unwrap().clear();
    }

    fn is_blocked(&self, from: u64, to: u64) -> bool {
        self.blocked.lock().unwrap().contains(&(from, to))
    }
}

// ---------------------------------------------------------------------------
// Partition-aware network: wraps the real OxiDbNetwork, drops cut edges
// ---------------------------------------------------------------------------

struct PartitionedFactory {
    from: u64,
    ctrl: Arc<Partitioner>,
    inner: OxiDbNetworkFactory,
}

impl RaftNetworkFactory<TypeConfig> for PartitionedFactory {
    type Network = PartitionedNetwork;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        PartitionedNetwork {
            from: self.from,
            to: target,
            ctrl: Arc::clone(&self.ctrl),
            inner: self.inner.new_client(target, node).await,
        }
    }
}

struct PartitionedNetwork {
    from: u64,
    to: u64,
    ctrl: Arc<Partitioner>,
    inner: OxiDbNetwork,
}

fn unreachable<E: std::error::Error + 'static>(
    from: u64,
    to: u64,
) -> RPCError<u64, BasicNode, E> {
    RPCError::Unreachable(Unreachable::new(&std::io::Error::new(
        std::io::ErrorKind::Other,
        format!("partitioned: {from} -> {to}"),
    )))
}

impl RaftNetwork<TypeConfig> for PartitionedNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        if self.ctrl.is_blocked(self.from, self.to) {
            return Err(unreachable(self.from, self.to));
        }
        self.inner.append_entries(rpc, option).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        if self.ctrl.is_blocked(self.from, self.to) {
            return Err(unreachable(self.from, self.to));
        }
        self.inner.install_snapshot(rpc, option).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        if self.ctrl.is_blocked(self.from, self.to) {
            return Err(unreachable(self.from, self.to));
        }
        self.inner.vote(rpc, option).await
    }
}

// ---------------------------------------------------------------------------
// AsyncClient — length-prefixed JSON TCP client
// ---------------------------------------------------------------------------

struct AsyncClient {
    stream: TcpStream,
}

impl AsyncClient {
    async fn connect(addr: SocketAddr) -> Self {
        let stream = TcpStream::connect(addr).await.expect("connect failed");
        stream.set_nodelay(true).ok();
        Self { stream }
    }

    async fn send(&mut self, request: &Value) -> Value {
        let payload = serde_json::to_vec(request).unwrap();
        let len = (payload.len() as u32).to_le_bytes();
        self.stream.write_all(&len).await.unwrap();
        self.stream.write_all(&payload).await.unwrap();
        self.stream.flush().await.unwrap();

        let mut len_buf = [0u8; 4];
        self.stream.read_exact(&mut len_buf).await.unwrap();
        let resp_len = u32::from_le_bytes(len_buf) as usize;
        let mut buf = vec![0u8; resp_len];
        self.stream.read_exact(&mut buf).await.unwrap();
        serde_json::from_slice(&buf).unwrap()
    }
}

// ---------------------------------------------------------------------------
// Node harness
// ---------------------------------------------------------------------------

struct TestNode {
    node_id: u64,
    client_addr: SocketAddr,
    raft_addr: SocketAddr,
    _dir: TempDir,
    tasks: Vec<JoinHandle<()>>,
    raft: Arc<OxiRaft>,
}

impl TestNode {
    async fn kill(&mut self) {
        let _ = self.raft.shutdown().await;
        for h in &self.tasks {
            h.abort();
        }
        self.tasks.clear();
    }
}

fn test_openraft_config() -> Arc<openraft::Config> {
    let config = openraft::Config {
        heartbeat_interval: 200,
        election_timeout_min: 500,
        election_timeout_max: 1000,
        ..Default::default()
    };
    Arc::new(config.validate().expect("invalid raft config"))
}

async fn allocate_port() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap()
}

async fn start_node(
    node_id: u64,
    client_addr: SocketAddr,
    raft_addr: SocketAddr,
    data_dir: &std::path::Path,
    ctrl: Arc<Partitioner>,
) -> (Arc<OxiRaft>, Vec<JoinHandle<()>>) {
    let db = Arc::new(OxiDb::open(data_dir).expect("open db"));

    let store = OxiDbStore::new(Arc::clone(&db));
    let (log_store, state_machine) = Adaptor::new(store);
    // The only difference from raft_test.rs::start_node — a
    // partition-aware factory instead of the bare OxiDbNetworkFactory.
    let network_factory = PartitionedFactory {
        from: node_id,
        ctrl,
        inner: OxiDbNetworkFactory,
    };

    let raft = openraft::Raft::new(
        node_id,
        test_openraft_config(),
        network_factory,
        log_store,
        state_machine,
    )
    .await
    .expect("create raft node");
    let raft = Arc::new(raft);

    let state = Arc::new(ServerState {
        db,
        db_manager: None,
        user_store: None,
        audit_log: None,
        auth_enabled: false,
        raft: Some(Arc::clone(&raft)),
    });

    let mut tasks = Vec::new();

    let raft_clone = Arc::clone(&raft);
    let raft_listener = TcpListener::bind(raft_addr).await.expect("bind raft listener");
    tasks.push(tokio::spawn(async move {
        loop {
            match raft_listener.accept().await {
                Ok((stream, _)) => {
                    let r = Arc::clone(&raft_clone);
                    tokio::spawn(async move {
                        oxidb_server::raft::network::handle_raft_rpc(stream, &r).await;
                    });
                }
                Err(_) => break,
            }
        }
    }));

    let state_clone = Arc::clone(&state);
    let client_listener = TcpListener::bind(client_addr)
        .await
        .expect("bind client listener");
    tasks.push(tokio::spawn(async move {
        loop {
            match client_listener.accept().await {
                Ok((stream, _)) => {
                    let s = Arc::clone(&state_clone);
                    tokio::spawn(async move {
                        async_server::handle_connection(stream, s, Duration::ZERO).await;
                    });
                }
                Err(_) => break,
            }
        }
    }));

    (raft, tasks)
}

/// Start `count` nodes with the shared partitioner and form a cluster.
async fn form_cluster(count: u64, ctrl: Arc<Partitioner>) -> (Vec<TestNode>, Vec<AsyncClient>) {
    let mut nodes = Vec::new();
    for id in 1..=count {
        let client_addr = allocate_port().await;
        let raft_addr = allocate_port().await;
        let dir = TempDir::new().unwrap();
        let (raft, tasks) =
            start_node(id, client_addr, raft_addr, dir.path(), Arc::clone(&ctrl)).await;
        nodes.push(TestNode {
            node_id: id,
            client_addr,
            raft_addr,
            _dir: dir,
            tasks,
            raft,
        });
    }
    sleep(Duration::from_millis(50)).await;

    let mut clients = Vec::new();
    for node in &nodes {
        clients.push(AsyncClient::connect(node.client_addr).await);
    }

    let resp = clients[0].send(&json!({"cmd": "raft_init"})).await;
    assert!(resp["ok"].as_bool().unwrap_or(false), "raft_init: {resp}");

    for id in 2..=count {
        let idx = (id - 1) as usize;
        let resp = clients[0]
            .send(&json!({
                "cmd": "raft_add_learner",
                "node_id": id,
                "addr": nodes[idx].raft_addr.to_string(),
            }))
            .await;
        assert!(resp["ok"].as_bool().unwrap_or(false), "add_learner {id}: {resp}");
    }

    let members: Vec<u64> = (1..=count).collect();
    let resp = clients[0]
        .send(&json!({"cmd": "raft_change_membership", "members": members}))
        .await;
    assert!(
        resp["ok"].as_bool().unwrap_or(false),
        "change_membership: {resp}"
    );

    wait_for_leader_among(&mut clients, &(0..count as usize).collect::<Vec<_>>(), Duration::from_secs(15))
        .await
        .expect("initial leader");

    (nodes, clients)
}

// ---------------------------------------------------------------------------
// Cluster observation helpers
// ---------------------------------------------------------------------------

/// Poll `raft_metrics` until one of the given client indices reports
/// `Leader`. Returns that index, or None on timeout.
async fn wait_for_leader_among(
    clients: &mut [AsyncClient],
    indices: &[usize],
    timeout: Duration,
) -> Option<usize> {
    let start = tokio::time::Instant::now();
    loop {
        for &i in indices {
            let resp = clients[i].send(&json!({"cmd": "raft_metrics"})).await;
            if resp["ok"].as_bool().unwrap_or(false)
                && resp["data"]["state"].as_str() == Some("Leader")
            {
                return Some(i);
            }
        }
        if start.elapsed() > timeout {
            return None;
        }
        sleep(Duration::from_millis(150)).await;
    }
}

/// Count docs in `collection` on a single node.
async fn count_on(client: &mut AsyncClient, collection: &str) -> u64 {
    let resp = client
        .send(&json!({"cmd": "count", "collection": collection, "query": {}}))
        .await;
    resp["data"]["count"].as_u64().unwrap_or(0)
}

/// The set of `k` values present on a single node (the write key).
async fn keys_on(client: &mut AsyncClient, collection: &str) -> HashSet<i64> {
    let resp = client
        .send(&json!({"cmd": "find", "collection": collection, "query": {}}))
        .await;
    // find responses have varied historically; accept data as an array
    // or as {documents: [...]}.
    let docs = resp["data"]["documents"]
        .as_array()
        .or_else(|| resp["data"].as_array())
        .cloned()
        .unwrap_or_default();
    docs.iter().filter_map(|d| d["k"].as_i64()).collect()
}

/// Insert one keyed doc via `client`; returns true iff the node ACKed a
/// committed write.
async fn insert_key(client: &mut AsyncClient, collection: &str, k: i64) -> bool {
    let resp = client
        .send(&json!({"cmd": "insert", "collection": collection, "doc": {"k": k}}))
        .await;
    resp["ok"].as_bool().unwrap_or(false)
}

// ===========================================================================
// Shared round logic
// ===========================================================================

/// Commit `n` keyed writes through the given client, recording ACKs.
/// Returns how many committed.
async fn commit_writes(
    client: &mut AsyncClient,
    coll: &str,
    n: usize,
    next_key: &mut i64,
    acked: &mut HashSet<i64>,
) -> usize {
    let mut ok = 0;
    for _ in 0..n {
        if insert_key(client, coll, *next_key).await {
            acked.insert(*next_key);
            *next_key += 1;
            ok += 1;
        } else {
            *next_key += 1; // burn the key so it's never reused
        }
    }
    ok
}

/// Assert that a partitioned minority cannot commit — every write to a
/// minority node must be rejected or time out (no quorum). A committed
/// write here is split-brain.
async fn assert_minority_cannot_commit(
    nodes: &[TestNode],
    minority_idx: &[usize],
    coll: &str,
    round: u32,
) {
    for &mi in minority_idx {
        let addr = nodes[mi].client_addr;
        let doomed = 1_000_000 + round as i64 * 1000 + mi as i64; // never acked
        let result = tokio::time::timeout(Duration::from_secs(3), async {
            let mut c = AsyncClient::connect(addr).await;
            insert_key(&mut c, coll, doomed).await
        })
        .await;
        if let Ok(true) = result {
            panic!(
                "round {round}: SPLIT-BRAIN — minority node{} committed a write",
                nodes[mi].node_id
            );
        }
    }
}

// ===========================================================================
// Test 1 — ordinary partition: leader stays in the majority
// ===========================================================================
//
// Two followers are stranded in the minority. The majority keeps its
// leader and stays available; on heal the two followers must catch up.
// All four Jepsen invariants must hold, including full convergence.

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn raft_survives_follower_partition() {
    const N: u64 = 5;
    const COLL: &str = "ledger";
    let ctrl = Arc::new(Partitioner::default());
    let (mut nodes, mut clients) = form_cluster(N, Arc::clone(&ctrl)).await;
    let all: Vec<usize> = (0..N as usize).collect();

    let mut acked: HashSet<i64> = HashSet::new();
    let mut next_key: i64 = 0;
    {
        let leader = wait_for_leader_among(&mut clients, &all, Duration::from_secs(10))
            .await
            .expect("leader");
        commit_writes(&mut clients[leader], COLL, 10, &mut next_key, &mut acked).await;
    }
    wait_converged(&mut clients, COLL, acked.len() as u64, Duration::from_secs(10)).await;
    println!("\nfollower-partition test — {N} nodes, baseline {} docs", acked.len());

    for round in 1..=3u32 {
        let leader_idx = wait_for_leader_among(&mut clients, &all, Duration::from_secs(10))
            .await
            .expect("pre-partition leader");
        let leader_id = nodes[leader_idx].node_id;

        let followers: Vec<u64> = (1..=N).filter(|id| *id != leader_id).collect();
        let minority_ids = vec![followers[0], followers[1]];
        let majority_ids: Vec<u64> = (1..=N).filter(|id| !minority_ids.contains(id)).collect();
        let minority_idx: Vec<usize> = minority_ids.iter().map(|id| (*id - 1) as usize).collect();
        let majority_idx: Vec<usize> = majority_ids.iter().map(|id| (*id - 1) as usize).collect();
        println!("round {round}: leader=node{leader_id}; minority {minority_ids:?} | majority {majority_ids:?}");

        ctrl.cut(&minority_ids, &majority_ids);

        // (3) Majority stays available.
        let maj_leader = wait_for_leader_among(&mut clients, &majority_idx, Duration::from_secs(15))
            .await
            .expect("majority keeps a leader");
        let ok = commit_writes(&mut clients[maj_leader], COLL, 10, &mut next_key, &mut acked).await;
        assert!(ok >= 8, "round {round}: majority only committed {ok}/10");

        // (2) No split-brain.
        assert_minority_cannot_commit(&nodes, &minority_idx, COLL, round).await;

        // (4) Heal → full convergence.
        ctrl.heal();
        wait_converged(&mut clients, COLL, acked.len() as u64, Duration::from_secs(30)).await;

        // (1) Exact-set agreement on every node.
        for (i, client) in clients.iter_mut().enumerate() {
            let keys = keys_on(client, COLL).await;
            assert_eq!(
                keys, acked,
                "round {round}: node{} diverged — missing {:?}, extra {:?}",
                i + 1,
                acked.difference(&keys).collect::<Vec<_>>(),
                keys.difference(&acked).collect::<Vec<_>>(),
            );
        }
        println!("round {round}: OK — {} docs, all {N} nodes converged, minority rejected", acked.len());
    }

    for node in &mut nodes {
        node.kill().await;
    }
}

// ===========================================================================
// Test 2 — leader stranded in the minority: SAFETY only
// ===========================================================================
//
// The current leader is deliberately isolated into a 2-node minority.
// This is the hard case. SAFETY must hold: the minority (incl. the old
// leader) cannot commit, the majority elects a fresh leader and stays
// available, and every write the majority ACKs survives on the whole
// majority quorum after heal.
//
// LIVENESS caveat: openraft 0.9 has no PreVote, so an isolated ex-leader
// inflates its term and, on heal, becomes a disruptive stale-log
// candidate the quorum ignores — it does NOT catch up without a restart.
// This test therefore asserts convergence of the MAJORITY quorum only,
// and asserts the stranded node never shows uncommitted/phantom data
// (its keys stay a subset of `acked`). If openraft gains PreVote and the
// ex-leader converges too, tighten this to full-cluster convergence.

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn raft_leader_partition_safety() {
    const N: u64 = 5;
    const COLL: &str = "ledger";
    let ctrl = Arc::new(Partitioner::default());
    let (mut nodes, mut clients) = form_cluster(N, Arc::clone(&ctrl)).await;
    let all: Vec<usize> = (0..N as usize).collect();

    let mut acked: HashSet<i64> = HashSet::new();
    let mut next_key: i64 = 0;
    {
        let leader = wait_for_leader_among(&mut clients, &all, Duration::from_secs(10))
            .await
            .expect("leader");
        commit_writes(&mut clients[leader], COLL, 10, &mut next_key, &mut acked).await;
    }
    wait_converged(&mut clients, COLL, acked.len() as u64, Duration::from_secs(10)).await;
    println!("\nleader-partition safety test — {N} nodes, baseline {} docs", acked.len());

    for round in 1..=2u32 {
        let leader_idx = wait_for_leader_among(&mut clients, &all, Duration::from_secs(10))
            .await
            .expect("pre-partition leader");
        let leader_id = nodes[leader_idx].node_id;

        // Minority = the leader + one follower. Majority = the other 3.
        let minority_ids = vec![leader_id, (leader_id % N) + 1];
        let majority_ids: Vec<u64> = (1..=N).filter(|id| !minority_ids.contains(id)).collect();
        let minority_idx: Vec<usize> = minority_ids.iter().map(|id| (*id - 1) as usize).collect();
        let majority_idx: Vec<usize> = majority_ids.iter().map(|id| (*id - 1) as usize).collect();
        println!("round {round}: leader=node{leader_id} (stranded); minority {minority_ids:?} | majority {majority_ids:?}");

        ctrl.cut(&minority_ids, &majority_ids);

        // (3) Majority elects a fresh leader and stays available.
        let maj_leader = wait_for_leader_among(&mut clients, &majority_idx, Duration::from_secs(15))
            .await
            .expect("majority elects a new leader after old leader isolated");
        let ok = commit_writes(&mut clients[maj_leader], COLL, 10, &mut next_key, &mut acked).await;
        assert!(ok >= 8, "round {round}: majority only committed {ok}/10 after re-election");

        // (2) No split-brain: neither stranded node can commit.
        assert_minority_cannot_commit(&nodes, &minority_idx, COLL, round).await;

        ctrl.heal();

        // (1/4) SAFETY: the surviving majority quorum converges and holds
        // every acked write. (The stranded ex-leader may lag — see caveat.)
        wait_converged_subset(&mut clients, &majority_idx, COLL, acked.len() as u64, Duration::from_secs(30)).await;
        for &mj in &majority_idx {
            let keys = keys_on(&mut clients[mj], COLL).await;
            assert_eq!(
                keys, acked,
                "round {round}: majority node{} diverged — missing {:?}, extra {:?}",
                mj + 1,
                acked.difference(&keys).collect::<Vec<_>>(),
                keys.difference(&acked).collect::<Vec<_>>(),
            );
        }

        // The stranded ex-leader must never surface uncommitted/phantom
        // data: whatever it holds is a subset of the acked set. (It may
        // not have caught up — openraft 0.9 no-PreVote liveness caveat.)
        for &mi in &minority_idx {
            let keys = keys_on(&mut clients[mi], COLL).await;
            assert!(
                keys.is_subset(&acked),
                "round {round}: stranded node{} shows uncommitted data: {:?}",
                mi + 1,
                keys.difference(&acked).collect::<Vec<_>>(),
            );
        }
        println!("round {round}: OK — majority converged ({} docs), no split-brain, no phantom data", acked.len());
    }

    for node in &mut nodes {
        node.kill().await;
    }
}

/// Wait until the given node indices all reach `expected` count.
async fn wait_converged_subset(
    clients: &mut [AsyncClient],
    indices: &[usize],
    collection: &str,
    expected: u64,
    timeout: Duration,
) {
    let start = tokio::time::Instant::now();
    loop {
        let mut all = true;
        for &i in indices {
            if count_on(&mut clients[i], collection).await != expected {
                all = false;
                break;
            }
        }
        if all {
            return;
        }
        if start.elapsed() > timeout {
            let mut counts = Vec::new();
            for &i in indices {
                counts.push(count_on(&mut clients[i], collection).await);
            }
            panic!("majority convergence timeout: expected {expected}, got {counts:?}");
        }
        sleep(Duration::from_millis(200)).await;
    }
}

/// Wait until every node's `count` equals `expected`.
async fn wait_converged(
    clients: &mut [AsyncClient],
    collection: &str,
    expected: u64,
    timeout: Duration,
) {
    let start = tokio::time::Instant::now();
    loop {
        let mut all = true;
        for client in clients.iter_mut() {
            if count_on(client, collection).await != expected {
                all = false;
                break;
            }
        }
        if all {
            return;
        }
        if start.elapsed() > timeout {
            let mut lines = Vec::new();
            for (i, client) in clients.iter_mut().enumerate() {
                let c = count_on(client, collection).await;
                let m = client.send(&json!({"cmd": "raft_metrics"})).await;
                lines.push(format!(
                    "  node{}: count={c} state={} term={} last_log={} last_applied={} leader={}",
                    i + 1,
                    m["data"]["state"].as_str().unwrap_or("?"),
                    m["data"]["current_term"],
                    m["data"]["last_log_index"],
                    m["data"]["last_applied"].as_str().unwrap_or("?"),
                    m["data"]["current_leader"],
                ));
            }
            panic!(
                "convergence timeout: expected {expected}\n{}",
                lines.join("\n")
            );
        }
        sleep(Duration::from_millis(200)).await;
    }
}
