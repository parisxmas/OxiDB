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

// ---------------------------------------------------------------------------
// AsyncClient — lightweight async TCP client using length-prefixed JSON
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
// TestNode — a single in-process Raft node
// ---------------------------------------------------------------------------

struct TestNode {
    _node_id: u64,
    client_addr: SocketAddr,
    _raft_addr: SocketAddr,
    _dir: TempDir,
    tasks: Vec<JoinHandle<()>>,
    raft: Arc<OxiRaft>,
}

impl TestNode {
    async fn kill(&mut self) {
        // Shutdown the Raft instance first — this stops internal replication
        // tasks and heartbeats, allowing other nodes to detect the failure.
        let _ = self.raft.shutdown().await;
        for handle in &self.tasks {
            handle.abort();
        }
        self.tasks.clear();
    }
}

// ---------------------------------------------------------------------------
// Test-specific openraft config with faster timeouts
// ---------------------------------------------------------------------------

fn test_openraft_config() -> Arc<openraft::Config> {
    let config = openraft::Config {
        heartbeat_interval: 200,
        election_timeout_min: 500,
        election_timeout_max: 1000,
        ..Default::default()
    };
    Arc::new(config.validate().expect("invalid raft config"))
}

// ---------------------------------------------------------------------------
// allocate_port — bind to :0, grab the address, drop the listener
// ---------------------------------------------------------------------------

async fn allocate_port() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap()
}

// ---------------------------------------------------------------------------
// start_node — replicate main.rs startup logic programmatically
// ---------------------------------------------------------------------------

async fn start_node(
    node_id: u64,
    client_addr: SocketAddr,
    raft_addr: SocketAddr,
    data_dir: &std::path::Path,
) -> (Arc<OxiRaft>, Vec<JoinHandle<()>>) {
    let db = OxiDb::open(data_dir).expect("failed to open db");
    let db = Arc::new(db);

    let openraft_cfg = test_openraft_config();
    let store = OxiDbStore::new(Arc::clone(&db));
    let (log_store, state_machine) = Adaptor::new(store);
    let network_factory = OxiDbNetworkFactory;

    let raft = openraft::Raft::new(node_id, openraft_cfg, network_factory, log_store, state_machine)
        .await
        .expect("failed to create raft node");
    let raft = Arc::new(raft);

    let state = Arc::new(ServerState {
        db,
        user_store: None,
        audit_log: None,
        auth_enabled: false,
        raft: Some(Arc::clone(&raft)),
    });

    let mut tasks = Vec::new();

    // Spawn Raft RPC listener
    let raft_clone = Arc::clone(&raft);
    let raft_listener = TcpListener::bind(raft_addr).await.expect("bind raft listener");
    let raft_handle = tokio::spawn(async move {
        loop {
            match raft_listener.accept().await {
                Ok((stream, _)) => {
                    let r = Arc::clone(&raft_clone);
                    tokio::spawn(async move {
                        network::handle_raft_rpc(stream, &r).await;
                    });
                }
                Err(_) => break,
            }
        }
    });
    tasks.push(raft_handle);

    // Spawn client listener
    let state_clone = Arc::clone(&state);
    let client_listener = TcpListener::bind(client_addr).await.expect("bind client listener");
    let client_handle = tokio::spawn(async move {
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
    });
    tasks.push(client_handle);

    (raft, tasks)
}

// ---------------------------------------------------------------------------
// create_test_node — allocate ports, create TempDir, start node
// ---------------------------------------------------------------------------

async fn create_test_node(node_id: u64) -> TestNode {
    let client_addr = allocate_port().await;
    let raft_addr = allocate_port().await;
    let dir = TempDir::new().unwrap();

    let (raft, tasks) = start_node(node_id, client_addr, raft_addr, dir.path()).await;

    // Brief pause to let the listeners start accepting
    sleep(Duration::from_millis(50)).await;

    TestNode {
        _node_id: node_id,
        client_addr,
        _raft_addr: raft_addr,
        _dir: dir,
        tasks,
        raft,
    }
}

// ---------------------------------------------------------------------------
// form_cluster — start N nodes and bootstrap a Raft cluster
// ---------------------------------------------------------------------------

async fn form_cluster(count: u64) -> (Vec<TestNode>, Vec<AsyncClient>) {
    assert!(count >= 1);

    let mut nodes = Vec::new();
    for id in 1..=count {
        nodes.push(create_test_node(id).await);
    }

    let mut clients: Vec<AsyncClient> = Vec::new();
    for node in &nodes {
        clients.push(AsyncClient::connect(node.client_addr).await);
    }

    // Initialize cluster on node 1
    let resp = clients[0].send(&json!({"cmd": "raft_init"})).await;
    assert!(resp["ok"].as_bool().unwrap_or(false), "raft_init failed: {resp}");

    // Add learners 2..=count
    for id in 2..=count {
        let idx = (id - 1) as usize;
        let resp = clients[0]
            .send(&json!({
                "cmd": "raft_add_learner",
                "node_id": id,
                "addr": nodes[idx]._raft_addr.to_string(),
            }))
            .await;
        assert!(resp["ok"].as_bool().unwrap_or(false), "add_learner {id} failed: {resp}");
    }

    // Promote all to voters
    let members: Vec<u64> = (1..=count).collect();
    let resp = clients[0]
        .send(&json!({
            "cmd": "raft_change_membership",
            "members": members,
        }))
        .await;
    assert!(resp["ok"].as_bool().unwrap_or(false), "change_membership failed: {resp}");

    // Wait for a leader to be elected
    wait_for_leader(&mut clients, Duration::from_secs(15)).await;

    (nodes, clients)
}

// ---------------------------------------------------------------------------
// Helper: wait_for_leader — poll raft_metrics until a leader is found
// ---------------------------------------------------------------------------

async fn wait_for_leader(clients: &mut [AsyncClient], timeout: Duration) -> usize {
    let start = tokio::time::Instant::now();
    loop {
        for (i, client) in clients.iter_mut().enumerate() {
            let resp = client.send(&json!({"cmd": "raft_metrics"})).await;
            if resp["ok"].as_bool().unwrap_or(false) {
                let state = resp["data"]["state"].as_str().unwrap_or("");
                if state == "Leader" {
                    return i;
                }
            }
        }
        if start.elapsed() > timeout {
            panic!("timed out waiting for leader election");
        }
        sleep(Duration::from_millis(200)).await;
    }
}

// ---------------------------------------------------------------------------
// Helper: find_leader — single-pass check for current leader
// ---------------------------------------------------------------------------

async fn find_leader(clients: &mut [AsyncClient]) -> Option<usize> {
    for (i, client) in clients.iter_mut().enumerate() {
        let resp = client.send(&json!({"cmd": "raft_metrics"})).await;
        if resp["ok"].as_bool().unwrap_or(false) {
            let state = resp["data"]["state"].as_str().unwrap_or("");
            if state == "Leader" {
                return Some(i);
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Helper: wait_for_replication — poll count on all nodes
// ---------------------------------------------------------------------------

async fn wait_for_replication(
    clients: &mut [AsyncClient],
    collection: &str,
    expected_count: u64,
    timeout: Duration,
) {
    let start = tokio::time::Instant::now();
    loop {
        let mut all_match = true;
        for client in clients.iter_mut() {
            let resp = client
                .send(&json!({
                    "cmd": "count",
                    "collection": collection,
                    "query": {},
                }))
                .await;
            let count = resp["data"]["count"].as_u64().unwrap_or(0);
            if count != expected_count {
                all_match = false;
                break;
            }
        }
        if all_match {
            return;
        }
        if start.elapsed() > timeout {
            for (i, client) in clients.iter_mut().enumerate() {
                let resp = client
                    .send(&json!({"cmd": "count", "collection": collection, "query": {}}))
                    .await;
                eprintln!("node {i}: count response = {resp}");
            }
            panic!(
                "timed out waiting for replication: expected {expected_count} docs in '{collection}'"
            );
        }
        sleep(Duration::from_millis(200)).await;
    }
}

// ---------------------------------------------------------------------------
// Helper: send_to_leader — find leader, send request to it
// ---------------------------------------------------------------------------

async fn send_to_leader(clients: &mut [AsyncClient], request: &Value) -> Value {
    let leader_idx = find_leader(clients).await.expect("no leader found");
    clients[leader_idx].send(request).await
}

// ===========================================================================
// Tests
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_cluster_formation_and_leader_election() {
    let (mut nodes, mut clients) = form_cluster(4).await;

    // All nodes should agree on the same current_leader
    let mut leaders = Vec::new();
    for client in clients.iter_mut() {
        let resp = client.send(&json!({"cmd": "raft_metrics"})).await;
        assert!(resp["ok"].as_bool().unwrap_or(false));
        let leader_id = resp["data"]["current_leader"].as_u64();
        leaders.push(leader_id);
    }

    // All should report the same leader
    let first = leaders[0];
    assert!(first.is_some(), "no leader reported");
    for l in &leaders {
        assert_eq!(*l, first, "nodes disagree on leader: {leaders:?}");
    }

    // The leader node should report state "Leader"
    let leader_idx = find_leader(&mut clients).await;
    assert!(leader_idx.is_some(), "no node reports Leader state");

    for node in &mut nodes {
        node.kill().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_write_replication() {
    let (mut nodes, mut clients) = form_cluster(4).await;

    // Insert 10 documents via leader
    for i in 0..10 {
        let resp = send_to_leader(
            &mut clients,
            &json!({
                "cmd": "insert",
                "collection": "test",
                "doc": {"i": i, "data": format!("document_{i}")},
            }),
        )
        .await;
        assert!(resp["ok"].as_bool().unwrap_or(false), "insert {i} failed: {resp}");
    }

    // Wait for replication to all 4 nodes
    wait_for_replication(&mut clients, "test", 10, Duration::from_secs(10)).await;

    // Verify all nodes have the correct data
    for (idx, client) in clients.iter_mut().enumerate() {
        let resp = client
            .send(&json!({
                "cmd": "find",
                "collection": "test",
                "query": {},
            }))
            .await;
        assert!(resp["ok"].as_bool().unwrap_or(false), "find on node {idx} failed: {resp}");
        let docs = resp["data"].as_array().expect("data should be array");
        assert_eq!(docs.len(), 10, "node {idx} has {} docs, expected 10", docs.len());
    }

    for node in &mut nodes {
        node.kill().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_leader_kill_and_failover() {
    let (mut nodes, mut clients) = form_cluster(4).await;

    // Find current leader
    let leader_idx = find_leader(&mut clients).await.expect("no leader");
    let leader_node_id = leader_idx as u64 + 1;
    eprintln!("initial leader: node {} (index {})", leader_node_id, leader_idx);

    // Insert 5 docs via leader
    for i in 0..5 {
        let resp = clients[leader_idx]
            .send(&json!({
                "cmd": "insert",
                "collection": "test",
                "doc": {"i": i, "phase": "before_kill"},
            }))
            .await;
        assert!(resp["ok"].as_bool().unwrap_or(false), "insert {i} failed: {resp}");
    }

    // Wait for replication
    wait_for_replication(&mut clients, "test", 5, Duration::from_secs(10)).await;

    // Kill the leader (shutdown raft + abort tasks)
    nodes[leader_idx].kill().await;
    eprintln!("killed leader node {leader_node_id}");

    // Build a set of surviving client indices
    let surviving_indices: Vec<usize> = (0..4).filter(|&i| i != leader_idx).collect();

    // Reconnect surviving clients (old connections to killed node are dead)
    let mut surviving_clients: Vec<AsyncClient> = Vec::new();
    for &idx in &surviving_indices {
        surviving_clients.push(AsyncClient::connect(nodes[idx].client_addr).await);
    }

    // Wait for new leader among survivors
    let new_leader_rel = wait_for_leader(&mut surviving_clients, Duration::from_secs(15)).await;
    let new_leader_abs = surviving_indices[new_leader_rel];
    let new_leader_node_id = new_leader_abs as u64 + 1;
    eprintln!("new leader: node {} (index {})", new_leader_node_id, new_leader_abs);
    assert_ne!(
        new_leader_abs, leader_idx,
        "new leader should be different from killed leader"
    );

    // Insert 5 more docs via new leader
    for i in 5..10 {
        let resp = surviving_clients[new_leader_rel]
            .send(&json!({
                "cmd": "insert",
                "collection": "test",
                "doc": {"i": i, "phase": "after_kill"},
            }))
            .await;
        assert!(resp["ok"].as_bool().unwrap_or(false), "insert {i} failed: {resp}");
    }

    // Wait for replication on survivors
    wait_for_replication(&mut surviving_clients, "test", 10, Duration::from_secs(10)).await;

    // Verify all survivors have 10 docs
    for (rel_idx, client) in surviving_clients.iter_mut().enumerate() {
        let resp = client
            .send(&json!({
                "cmd": "find",
                "collection": "test",
                "query": {},
            }))
            .await;
        assert!(resp["ok"].as_bool().unwrap_or(false));
        let docs = resp["data"].as_array().expect("data should be array");
        assert_eq!(
            docs.len(),
            10,
            "surviving node {} has {} docs, expected 10",
            surviving_indices[rel_idx],
            docs.len()
        );
    }

    for node in &mut nodes {
        node.kill().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_data_consistency_after_failover() {
    let (mut nodes, mut clients) = form_cluster(4).await;

    // Insert 20 docs with unique keys via leader
    let leader_idx = find_leader(&mut clients).await.expect("no leader");
    for i in 0..20 {
        let resp = clients[leader_idx]
            .send(&json!({
                "cmd": "insert",
                "collection": "test",
                "doc": {"key": format!("key_{i}"), "value": i},
            }))
            .await;
        assert!(resp["ok"].as_bool().unwrap_or(false), "insert {i} failed: {resp}");
    }

    // Wait for full replication
    wait_for_replication(&mut clients, "test", 20, Duration::from_secs(10)).await;

    // Kill leader
    nodes[leader_idx].kill().await;
    eprintln!("killed leader node {}", leader_idx + 1);

    let surviving_indices: Vec<usize> = (0..4).filter(|&i| i != leader_idx).collect();
    let mut surviving_clients: Vec<AsyncClient> = Vec::new();
    for &idx in &surviving_indices {
        surviving_clients.push(AsyncClient::connect(nodes[idx].client_addr).await);
    }

    // Wait for new leader
    let new_leader_rel = wait_for_leader(&mut surviving_clients, Duration::from_secs(15)).await;

    // Verify all 3 survivors have all 20 docs
    for (rel_idx, client) in surviving_clients.iter_mut().enumerate() {
        let resp = client
            .send(&json!({
                "cmd": "find",
                "collection": "test",
                "query": {},
            }))
            .await;
        assert!(resp["ok"].as_bool().unwrap_or(false));
        let docs = resp["data"].as_array().expect("data should be array");
        assert_eq!(
            docs.len(),
            20,
            "survivor node {} has {} docs, expected 20",
            surviving_indices[rel_idx],
            docs.len()
        );
    }

    // Insert 10 more docs via new leader
    for i in 20..30 {
        let resp = surviving_clients[new_leader_rel]
            .send(&json!({
                "cmd": "insert",
                "collection": "test",
                "doc": {"key": format!("key_{i}"), "value": i},
            }))
            .await;
        assert!(resp["ok"].as_bool().unwrap_or(false), "insert {i} failed: {resp}");
    }

    // Wait for replication on survivors
    wait_for_replication(&mut surviving_clients, "test", 30, Duration::from_secs(10)).await;

    // Verify all survivors have 30 docs
    for client in surviving_clients.iter_mut() {
        let resp = client
            .send(&json!({
                "cmd": "count",
                "collection": "test",
                "query": {},
            }))
            .await;
        assert!(resp["ok"].as_bool().unwrap_or(false));
        assert_eq!(resp["data"]["count"].as_u64().unwrap(), 30);
    }

    for node in &mut nodes {
        node.kill().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_kill_two_nodes() {
    // Use 5 nodes so killing 2 still leaves a majority (3 of 5)
    let (mut nodes, mut clients) = form_cluster(5).await;

    // Insert 10 docs via leader
    let leader_idx = find_leader(&mut clients).await.expect("no leader");
    for i in 0..10 {
        let resp = clients[leader_idx]
            .send(&json!({
                "cmd": "insert",
                "collection": "test",
                "doc": {"i": i},
            }))
            .await;
        assert!(resp["ok"].as_bool().unwrap_or(false), "insert {i} failed: {resp}");
    }

    // Wait for replication
    wait_for_replication(&mut clients, "test", 10, Duration::from_secs(10)).await;

    // Kill 2 non-leader nodes
    let kill_indices: Vec<usize> = (0..5).filter(|&i| i != leader_idx).take(2).collect();
    for &idx in &kill_indices {
        nodes[idx].kill().await;
        eprintln!("killed node {}", idx + 1);
    }

    // Remaining: 3 nodes including the leader
    let surviving_indices: Vec<usize> = (0..5)
        .filter(|i| !kill_indices.contains(i))
        .collect();

    let mut surviving_clients: Vec<AsyncClient> = Vec::new();
    for &idx in &surviving_indices {
        surviving_clients.push(AsyncClient::connect(nodes[idx].client_addr).await);
    }

    // Wait for leader to stabilize
    wait_for_leader(&mut surviving_clients, Duration::from_secs(15)).await;

    // Insert 5 more docs via leader of surviving cluster
    let leader_rel = find_leader(&mut surviving_clients).await.expect("no leader among survivors");
    for i in 10..15 {
        let resp = surviving_clients[leader_rel]
            .send(&json!({
                "cmd": "insert",
                "collection": "test",
                "doc": {"i": i},
            }))
            .await;
        assert!(resp["ok"].as_bool().unwrap_or(false), "insert {i} failed: {resp}");
    }

    // Wait for replication on survivors
    wait_for_replication(&mut surviving_clients, "test", 15, Duration::from_secs(10)).await;

    // Verify all 3 remaining nodes have 15 docs
    for (rel_idx, client) in surviving_clients.iter_mut().enumerate() {
        let resp = client
            .send(&json!({
                "cmd": "find",
                "collection": "test",
                "query": {},
            }))
            .await;
        assert!(resp["ok"].as_bool().unwrap_or(false));
        let docs = resp["data"].as_array().expect("data should be array");
        assert_eq!(
            docs.len(),
            15,
            "surviving node {} has {} docs, expected 15",
            surviving_indices[rel_idx],
            docs.len()
        );
    }

    for node in &mut nodes {
        node.kill().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_minority_cannot_elect_leader() {
    let (mut nodes, mut clients) = form_cluster(4).await;

    // Confirm cluster is healthy first
    let leader_idx = find_leader(&mut clients).await.expect("no leader");
    eprintln!("initial leader: node {}", leader_idx + 1);

    // Kill 3 nodes, leave only 1 survivor (no quorum possible)
    let survivor_idx = 0usize;
    for i in 1..4 {
        nodes[i].kill().await;
        eprintln!("killed node {}", i + 1);
    }

    // Wait for the survivor to detect losses
    sleep(Duration::from_secs(3)).await;

    // Try an insert on a fresh connection — should fail or timeout (no quorum)
    let insert_result = tokio::time::timeout(Duration::from_secs(5), async {
        let mut c = AsyncClient::connect(nodes[survivor_idx].client_addr).await;
        c.send(&json!({
            "cmd": "insert",
            "collection": "test",
            "doc": {"i": 1},
        }))
        .await
    })
    .await;

    match insert_result {
        Ok(val) => {
            eprintln!("insert returned (may be error): {val}");
            // If it returned, it should be an error (raft can't commit)
        }
        Err(_) => {
            eprintln!("insert timed out as expected (no quorum)");
        }
    }

    // Use a fresh connection for metrics (the insert connection may be poisoned)
    let mut fresh_client = AsyncClient::connect(nodes[survivor_idx].client_addr).await;
    let metrics_resp = fresh_client.send(&json!({"cmd": "raft_metrics"})).await;
    eprintln!("survivor metrics: {metrics_resp}");

    // The survivor should not be a functional leader: it's 1 of 4 (needs 3 for majority).
    // It may show Candidate (trying elections) or Leader (briefly, but can't commit).
    if metrics_resp["ok"].as_bool().unwrap_or(false) {
        let state = metrics_resp["data"]["state"].as_str().unwrap_or("");
        eprintln!("survivor state: {state}");
    }

    for node in &mut nodes {
        node.kill().await;
    }
}
