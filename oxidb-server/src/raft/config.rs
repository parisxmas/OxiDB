use std::collections::BTreeMap;
use std::env;
use std::sync::Arc;

use openraft::BasicNode;

/// Raft configuration parsed from environment variables.
pub struct RaftConfig {
    pub node_id: u64,
    pub raft_addr: String,
    pub peers: BTreeMap<u64, BasicNode>,
}

impl RaftConfig {
    /// Parse Raft config from env vars. Returns `None` if OXIDB_NODE_ID is not set
    /// (standalone mode).
    pub fn from_env() -> Option<Self> {
        let node_id: u64 = env::var("OXIDB_NODE_ID").ok()?.parse().ok()?;

        let raft_addr =
            env::var("OXIDB_RAFT_ADDR").unwrap_or_else(|_| "127.0.0.1:4445".to_string());

        // Parse OXIDB_RAFT_PEERS: "1=host1:port1,2=host2:port2,..."
        let mut peers = BTreeMap::new();
        if let Ok(peers_str) = env::var("OXIDB_RAFT_PEERS") {
            for entry in peers_str.split(',') {
                let entry = entry.trim();
                if entry.is_empty() {
                    continue;
                }
                if let Some((id_str, addr)) = entry.split_once('=') {
                    if let Ok(id) = id_str.trim().parse::<u64>() {
                        peers.insert(id, BasicNode {
                            addr: addr.trim().to_string(),
                        });
                    }
                }
            }
        }

        Some(Self {
            node_id,
            raft_addr,
            peers,
        })
    }

    /// Build an openraft::Config with sensible defaults.
    pub fn openraft_config() -> Arc<openraft::Config> {
        let config = openraft::Config {
            heartbeat_interval: 500,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            ..Default::default()
        };
        Arc::new(config.validate().expect("invalid raft config"))
    }
}
