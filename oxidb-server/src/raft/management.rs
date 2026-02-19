use std::collections::BTreeMap;
use std::sync::Arc;

use openraft::BasicNode;
use serde_json::{Value, json};

use crate::handler;

use super::types::OxiRaft;

/// Handle Raft cluster management commands.
pub async fn handle_raft_command(cmd: &str, request: &Value, raft: &Arc<OxiRaft>) -> Vec<u8> {
    match cmd {
        "raft_init" => raft_init(raft).await,
        "raft_add_learner" => raft_add_learner(request, raft).await,
        "raft_change_membership" => raft_change_membership(request, raft).await,
        "raft_metrics" => raft_metrics(raft).await,
        _ => handler::err_bytes(&format!("unknown raft command: {cmd}")),
    }
}

/// Initialize a single-node Raft cluster.
async fn raft_init(raft: &OxiRaft) -> Vec<u8> {
    let mut members = BTreeMap::new();
    // Get node_id from the raft metrics
    let metrics = raft.metrics().borrow().clone();
    let node_id = metrics.id;
    members.insert(node_id, BasicNode::default());

    match raft.initialize(members).await {
        Ok(()) => handler::ok_bytes(json!("cluster initialized")),
        Err(e) => handler::err_bytes(&format!("raft init failed: {e}")),
    }
}

/// Add a learner node to the cluster.
async fn raft_add_learner(request: &Value, raft: &OxiRaft) -> Vec<u8> {
    let node_id = match request.get("node_id").and_then(|v| v.as_u64()) {
        Some(id) => id,
        None => return handler::err_bytes("missing 'node_id'"),
    };
    let addr = match request.get("addr").and_then(|v| v.as_str()) {
        Some(a) => a.to_string(),
        None => return handler::err_bytes("missing 'addr' (raft address)"),
    };

    let node = BasicNode { addr };

    match raft.add_learner(node_id, node, true).await {
        Ok(resp) => handler::ok_bytes(json!({
            "log_id": format!("{}", resp.log_id),
            "membership": format!("{:?}", resp.membership),
        })),
        Err(e) => handler::err_bytes(&format!("add learner failed: {e}")),
    }
}

/// Change the cluster membership (promote learners to voters).
async fn raft_change_membership(request: &Value, raft: &OxiRaft) -> Vec<u8> {
    let members = match request.get("members").and_then(|v| v.as_array()) {
        Some(arr) => {
            let ids: Option<Vec<u64>> = arr.iter().map(|v| v.as_u64()).collect();
            match ids {
                Some(ids) => ids.into_iter().collect::<std::collections::BTreeSet<u64>>(),
                None => return handler::err_bytes("'members' must be array of node IDs"),
            }
        }
        None => return handler::err_bytes("missing 'members' array"),
    };

    match raft.change_membership(members, false).await {
        Ok(resp) => handler::ok_bytes(json!({
            "log_id": format!("{}", resp.log_id),
            "membership": format!("{:?}", resp.membership),
        })),
        Err(e) => handler::err_bytes(&format!("change membership failed: {e}")),
    }
}

/// Return current Raft metrics.
async fn raft_metrics(raft: &OxiRaft) -> Vec<u8> {
    let metrics = raft.metrics().borrow().clone();
    handler::ok_bytes(json!({
        "id": metrics.id,
        "state": format!("{:?}", metrics.state),
        "current_term": metrics.current_term,
        "last_log_index": metrics.last_log_index,
        "last_applied": metrics.last_applied.map(|l| format!("{l}")),
        "current_leader": metrics.current_leader,
        "membership_config": format!("{:?}", metrics.membership_config),
    }))
}
