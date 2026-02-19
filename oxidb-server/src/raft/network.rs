use openraft::error::{RPCError, RaftError, InstallSnapshotError, Unreachable};
use openraft::network::{RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::BasicNode;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::types::TypeConfig;

/// Factory that creates network connections to peer Raft nodes.
pub struct OxiDbNetworkFactory;

impl RaftNetworkFactory<TypeConfig> for OxiDbNetworkFactory {
    type Network = OxiDbNetwork;

    async fn new_client(&mut self, _target: u64, node: &BasicNode) -> Self::Network {
        OxiDbNetwork {
            addr: node.addr.clone(),
        }
    }
}

/// Network client for a single peer node.
pub struct OxiDbNetwork {
    addr: String,
}

/// Tag-based envelope for Raft RPC messages over the wire.
/// We serialize openraft types to `serde_json::Value` to avoid needing
/// `TypeConfig: Serialize` (which `declare_raft_types!` doesn't provide).
#[derive(Debug, Serialize, Deserialize)]
struct RaftRpc {
    kind: String,
    payload: Value,
}

/// Tag-based envelope for Raft RPC responses.
#[derive(Debug, Serialize, Deserialize)]
struct RaftRpcResponse {
    kind: String,
    payload: Value,
}

fn make_rpc_error(msg: &str) -> RPCError<u64, BasicNode, RaftError<u64>> {
    RPCError::Unreachable(Unreachable::new(&std::io::Error::new(
        std::io::ErrorKind::Other,
        msg.to_string(),
    )))
}

fn make_snapshot_error(
    msg: &str,
) -> RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>> {
    RPCError::Unreachable(Unreachable::new(&std::io::Error::new(
        std::io::ErrorKind::Other,
        msg.to_string(),
    )))
}

/// Send a length-prefixed JSON message and read the response.
async fn rpc_call(addr: &str, rpc: &RaftRpc) -> Result<RaftRpcResponse, String> {
    let mut stream = TcpStream::connect(addr)
        .await
        .map_err(|e| format!("connect to {addr}: {e}"))?;

    let payload = serde_json::to_vec(rpc).map_err(|e| format!("serialize: {e}"))?;
    let len = (payload.len() as u32).to_le_bytes();
    stream
        .write_all(&len)
        .await
        .map_err(|e| format!("write len: {e}"))?;
    stream
        .write_all(&payload)
        .await
        .map_err(|e| format!("write payload: {e}"))?;
    stream.flush().await.map_err(|e| format!("flush: {e}"))?;

    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| format!("read resp len: {e}"))?;
    let resp_len = u32::from_le_bytes(len_buf) as usize;

    if resp_len > 16 * 1024 * 1024 {
        return Err("response too large".into());
    }

    let mut resp_buf = vec![0u8; resp_len];
    stream
        .read_exact(&mut resp_buf)
        .await
        .map_err(|e| format!("read resp: {e}"))?;

    serde_json::from_slice(&resp_buf).map_err(|e| format!("deserialize resp: {e}"))
}

impl RaftNetwork<TypeConfig> for OxiDbNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let payload = serde_json::to_value(&rpc).map_err(|e| make_rpc_error(&e.to_string()))?;
        let msg = RaftRpc {
            kind: "append_entries".to_string(),
            payload,
        };
        let resp = rpc_call(&self.addr, &msg)
            .await
            .map_err(|e| make_rpc_error(&e))?;

        if resp.kind == "error" {
            return Err(make_rpc_error(
                resp.payload.as_str().unwrap_or("unknown error"),
            ));
        }
        serde_json::from_value(resp.payload).map_err(|e| make_rpc_error(&e.to_string()))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        let payload =
            serde_json::to_value(&rpc).map_err(|e| make_snapshot_error(&e.to_string()))?;
        let msg = RaftRpc {
            kind: "install_snapshot".to_string(),
            payload,
        };
        let resp = rpc_call(&self.addr, &msg)
            .await
            .map_err(|e| make_snapshot_error(&e))?;

        if resp.kind == "error" {
            return Err(make_snapshot_error(
                resp.payload.as_str().unwrap_or("unknown error"),
            ));
        }
        serde_json::from_value(resp.payload).map_err(|e| make_snapshot_error(&e.to_string()))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: openraft::network::RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let payload = serde_json::to_value(&rpc).map_err(|e| make_rpc_error(&e.to_string()))?;
        let msg = RaftRpc {
            kind: "vote".to_string(),
            payload,
        };
        let resp = rpc_call(&self.addr, &msg)
            .await
            .map_err(|e| make_rpc_error(&e))?;

        if resp.kind == "error" {
            return Err(make_rpc_error(
                resp.payload.as_str().unwrap_or("unknown error"),
            ));
        }
        serde_json::from_value(resp.payload).map_err(|e| make_rpc_error(&e.to_string()))
    }
}

/// Handle incoming Raft RPC on the Raft listener port.
pub async fn handle_raft_rpc(mut stream: TcpStream, raft: &openraft::Raft<TypeConfig>) {
    // Read a single RPC message.
    let mut len_buf = [0u8; 4];
    if stream.read_exact(&mut len_buf).await.is_err() {
        return;
    }
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > 16 * 1024 * 1024 {
        return;
    }

    let mut buf = vec![0u8; len];
    if stream.read_exact(&mut buf).await.is_err() {
        return;
    }

    let rpc: RaftRpc = match serde_json::from_slice(&buf) {
        Ok(r) => r,
        Err(_) => return,
    };

    let resp = match rpc.kind.as_str() {
        "vote" => {
            let req: VoteRequest<u64> = match serde_json::from_value(rpc.payload) {
                Ok(r) => r,
                Err(e) => {
                    let _ = send_error(&mut stream, &e.to_string()).await;
                    return;
                }
            };
            match raft.vote(req).await {
                Ok(r) => RaftRpcResponse {
                    kind: "vote".to_string(),
                    payload: serde_json::to_value(&r).unwrap_or_default(),
                },
                Err(e) => RaftRpcResponse {
                    kind: "error".to_string(),
                    payload: Value::String(e.to_string()),
                },
            }
        }
        "append_entries" => {
            let req: AppendEntriesRequest<TypeConfig> = match serde_json::from_value(rpc.payload) {
                Ok(r) => r,
                Err(e) => {
                    let _ = send_error(&mut stream, &e.to_string()).await;
                    return;
                }
            };
            match raft.append_entries(req).await {
                Ok(r) => RaftRpcResponse {
                    kind: "append_entries".to_string(),
                    payload: serde_json::to_value(&r).unwrap_or_default(),
                },
                Err(e) => RaftRpcResponse {
                    kind: "error".to_string(),
                    payload: Value::String(e.to_string()),
                },
            }
        }
        "install_snapshot" => {
            let req: InstallSnapshotRequest<TypeConfig> = match serde_json::from_value(rpc.payload)
            {
                Ok(r) => r,
                Err(e) => {
                    let _ = send_error(&mut stream, &e.to_string()).await;
                    return;
                }
            };
            match raft.install_snapshot(req).await {
                Ok(r) => RaftRpcResponse {
                    kind: "install_snapshot".to_string(),
                    payload: serde_json::to_value(&r).unwrap_or_default(),
                },
                Err(e) => RaftRpcResponse {
                    kind: "error".to_string(),
                    payload: Value::String(e.to_string()),
                },
            }
        }
        _ => RaftRpcResponse {
            kind: "error".to_string(),
            payload: Value::String("unknown rpc kind".to_string()),
        },
    };

    let _ = send_response(&mut stream, &resp).await;
}

async fn send_error(stream: &mut TcpStream, msg: &str) -> Result<(), ()> {
    let resp = RaftRpcResponse {
        kind: "error".to_string(),
        payload: Value::String(msg.to_string()),
    };
    send_response(stream, &resp).await
}

async fn send_response(stream: &mut TcpStream, resp: &RaftRpcResponse) -> Result<(), ()> {
    let payload = serde_json::to_vec(resp).map_err(|_| ())?;
    let len = (payload.len() as u32).to_le_bytes();
    stream.write_all(&len).await.map_err(|_| ())?;
    stream.write_all(&payload).await.map_err(|_| ())?;
    stream.flush().await.map_err(|_| ())?;
    Ok(())
}
