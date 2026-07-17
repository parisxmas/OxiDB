use std::collections::BTreeMap;
use std::sync::Arc;

use openraft::BasicNode;
use serde_json::{Value, json};

use crate::handler;

use super::types::OxiRaft;

/// Handle Raft cluster management commands.
pub async fn handle_raft_command(
    cmd: &str,
    request: &Value,
    raft: &Arc<OxiRaft>,
    own_addr: Option<&str>,
) -> Vec<u8> {
    match cmd {
        "raft_init" => raft_init(request, raft, own_addr).await,
        "raft_add_learner" => raft_add_learner(request, raft).await,
        "raft_change_membership" => raft_change_membership(request, raft).await,
        "raft_metrics" => raft_metrics(raft).await,
        _ => handler::err_bytes(&format!("unknown raft command: {cmd}")),
    }
}

/// Initialize a single-node Raft cluster.
///
/// The bootstrap node must publish a dialable address, just like every learner
/// does. `BasicNode::default()` has an EMPTY one, and that is invisible for as
/// long as this node stays leader — nobody dials the leader. The moment it
/// loses leadership (a partition, a restart, any election) no new leader can
/// ever reach it: replication fails with "connect to ''" forever and the node
/// silently freezes at its old log while the cluster moves on.
///
/// The address comes from the node's own `OXIDB_RAFT_ADDR` (what it actually
/// listens on); an explicit `addr` in the request overrides it. If neither is
/// known we refuse rather than bootstrap an unreachable member.
async fn raft_init(request: &Value, raft: &OxiRaft, own_addr: Option<&str>) -> Vec<u8> {
    let addr = match request
        .get("addr")
        .and_then(|v| v.as_str())
        .or(own_addr)
        .map(str::trim)
        .filter(|a| !a.is_empty())
    {
        Some(a) => a.to_string(),
        None => {
            return handler::err_bytes(
                "raft init: no Raft address known for this node — set OXIDB_RAFT_ADDR                  or pass 'addr'. Initialising with an empty address would leave this                  node unreachable to every future leader.",
            );
        }
    };

    let mut members = BTreeMap::new();
    // Get node_id from the raft metrics
    let metrics = raft.metrics().borrow().clone();
    let node_id = metrics.id;
    members.insert(node_id, BasicNode { addr });

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
