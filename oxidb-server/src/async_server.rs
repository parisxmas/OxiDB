use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

use oxidb::OxiDb;

use crate::async_protocol::{read_message, write_message};
use crate::audit::{self, AuditEvent, AuditLog};
use crate::auth::{Role, UserStore};
use crate::handler;
use crate::raft::management;
use crate::raft::types::{OxiDbRequest, OxiDbResponse, OxiRaft};
use crate::rbac;
use crate::scram::ScramState;
use crate::session::Session;

/// Shared server state passed to each async connection handler.
pub struct ServerState {
    pub db: Arc<OxiDb>,
    pub user_store: Option<Arc<Mutex<UserStore>>>,
    pub audit_log: Option<Arc<AuditLog>>,
    pub auth_enabled: bool,
    /// Raft node — `None` in standalone mode.
    pub raft: Option<Arc<OxiRaft>>,
}

/// Handle a plain TCP connection.
pub async fn handle_connection(stream: TcpStream, state: Arc<ServerState>, idle_timeout: Duration) {
    let peer = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".into());
    eprintln!("client connected: {peer}");

    stream.set_nodelay(true).ok();

    let (reader, writer) = tokio::io::split(stream);
    handle_stream(reader, writer, state, &peer, idle_timeout).await;

    eprintln!("client disconnected: {peer}");
}

/// Handle a TLS connection.
pub async fn handle_tls_connection(
    stream: TcpStream,
    state: Arc<ServerState>,
    tls_acceptor: tokio_rustls::TlsAcceptor,
    idle_timeout: Duration,
) {
    let peer = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".into());
    eprintln!("client connected (TLS): {peer}");

    stream.set_nodelay(true).ok();

    let tls_stream = match tls_acceptor.accept(stream).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("TLS handshake error for {peer}: {e}");
            return;
        }
    };

    let (reader, writer) = tokio::io::split(tls_stream);
    handle_stream(reader, writer, state, &peer, idle_timeout).await;

    eprintln!("client disconnected: {peer}");
}

/// Generic message loop over any async reader + writer.
async fn handle_stream<R, W>(
    mut reader: R,
    mut writer: W,
    state: Arc<ServerState>,
    peer: &str,
    idle_timeout: Duration,
) where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let mut active_tx: Option<u64> = None;
    let mut session = Session::new();

    if !state.auth_enabled {
        session.set_authenticated("anonymous".to_string(), Role::Admin);
    }

    loop {
        // Apply idle timeout around the read.
        let msg = if idle_timeout.is_zero() {
            read_message(&mut reader).await
        } else {
            match tokio::time::timeout(idle_timeout, read_message(&mut reader)).await {
                Ok(result) => result,
                Err(_) => {
                    eprintln!("idle timeout for {peer}, disconnecting");
                    break;
                }
            }
        };

        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                if e.kind() != std::io::ErrorKind::UnexpectedEof {
                    eprintln!("read error from {peer}: {e}");
                }
                break;
            }
        };

        let request: Value = match serde_json::from_slice(&msg) {
            Ok(v) => v,
            Err(e) => {
                let resp = json!({"ok": false, "error": format!("invalid JSON: {e}")});
                let _ = write_message(&mut writer, resp.to_string().as_bytes()).await;
                continue;
            }
        };

        let resp_bytes =
            dispatch_request(request, &state, &mut session, &mut active_tx, peer).await;

        if let Err(e) = write_message(&mut writer, &resp_bytes).await {
            eprintln!("write error to {peer}: {e}");
            break;
        }
    }

    // Auto-rollback active transaction on disconnect.
    if let Some(tx_id) = active_tx {
        let _ = state.db.rollback_transaction(tx_id);
    }
}

/// Dispatch a single request through auth -> RBAC -> Raft routing -> handler pipeline.
async fn dispatch_request(
    request: Value,
    state: &ServerState,
    session: &mut Session,
    active_tx: &mut Option<u64>,
    _peer: &str,
) -> Vec<u8> {
    let cmd = request
        .get("cmd")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let collection = request
        .get("collection")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    // ---------------------------------------------------------------
    // HELLO handshake — pre-auth, idempotent, returns server info.
    // ---------------------------------------------------------------
    if cmd == "hello" {
        return crate::hello::handle(&request, session, state.auth_enabled);
    }

    // ---------------------------------------------------------------
    // Authentication flow (SCRAM-SHA-256)
    // ---------------------------------------------------------------
    if state.auth_enabled && !session.is_authenticated() {
        return handle_auth(&cmd, &request, state, session);
    }

    // ---------------------------------------------------------------
    // RBAC check
    // ---------------------------------------------------------------
    // Read-role sessions may use the SQL engine but only for SELECTs; the
    // flag is decided here (session layer) and enforced by the SQL bridge.
    let mut sql_readonly = false;
    if state.auth_enabled {
        if let Some(role) = session.role() {
            let is_user_cmd = matches!(
                cmd.as_str(),
                "create_user"
                    | "drop_user"
                    | "update_user"
                    | "list_users"
                    | "grant_db_role"
                    | "revoke_db_role"
            );

            // Resolve the effective role for the target database. For user
            // management and database-level commands, use the global role.
            // For everything else, honor per-database role overrides so a
            // `db_roles` downgrade is enforced in cluster mode too (matches
            // the standalone dispatch path).
            let effective_role = if is_user_cmd
                || matches!(
                    cmd.as_str(),
                    "create_database" | "drop_database" | "list_databases" | "use_db"
                ) {
                role
            } else if let Some(ref user_store) = state.user_store {
                let target_db = request
                    .get("db")
                    .and_then(|v| v.as_str())
                    .unwrap_or(&session.current_database);
                let store = user_store.lock().unwrap();
                store
                    .effective_role(session.username_str(), target_db)
                    .unwrap_or(role)
            } else {
                role
            };

            let permitted = if is_user_cmd {
                role == Role::Admin
            } else {
                rbac::is_permitted(effective_role, &cmd)
            };

            if !permitted {
                log_audit(state, session, &cmd, collection.as_deref(), "denied", "");
                return handler::err_bytes(&format!(
                    "permission denied: role '{}' cannot execute '{}'",
                    role.as_str(),
                    cmd
                ));
            }
            sql_readonly = cmd == "sql" && effective_role == Role::Read;
        }
    }

    // ---------------------------------------------------------------
    // Raft management commands (cluster mode only)
    // ---------------------------------------------------------------
    if let Some(raft) = &state.raft {
        match cmd.as_str() {
            "raft_init" | "raft_add_learner" | "raft_change_membership" | "raft_metrics" => {
                let resp = management::handle_raft_command(&cmd, &request, raft).await;
                log_audit(state, session, &cmd, None, "ok", "");
                return resp;
            }
            _ => {}
        }
    }

    // ---------------------------------------------------------------
    // Handle user management commands
    // ---------------------------------------------------------------
    if let Some(user_store) = &state.user_store {
        if let Some(resp_bytes) = handler::handle_user_command(&cmd, &request, user_store) {
            log_audit(state, session, &cmd, None, "ok", "");
            return resp_bytes;
        }
    }

    // ---------------------------------------------------------------
    // Transaction commit through Raft (cluster mode)
    // ---------------------------------------------------------------
    if let Some(raft) = &state.raft {
        if cmd == "commit_tx" {
            if let Some(tx_id) = active_tx.take() {
                // Extract buffered writes from the transaction and send as one Raft entry
                match state.db.extract_transaction_writes(tx_id) {
                    Ok(write_ops) => {
                        // Convert core WriteOp to Raft TransactionWriteOp
                        let raft_ops: Vec<crate::raft::types::TransactionWriteOp> = write_ops
                            .into_iter()
                            .map(|op| match op {
                                oxidb::transaction::WriteOp::Insert {
                                    collection,
                                    data,
                                    id: _,
                                } => {
                                    // Raft replication path: the leader-side id reservation
                                    // doesn't apply on followers (they reserve from their own
                                    // counter when replaying). Drop it on the wire.
                                    crate::raft::types::TransactionWriteOp::Insert {
                                        collection,
                                        document: data,
                                    }
                                }
                                oxidb::transaction::WriteOp::Update {
                                    collection,
                                    query,
                                    update,
                                } => crate::raft::types::TransactionWriteOp::Update {
                                    collection,
                                    query,
                                    update,
                                },
                                oxidb::transaction::WriteOp::Delete { collection, query } => {
                                    crate::raft::types::TransactionWriteOp::Delete {
                                        collection,
                                        query,
                                    }
                                }
                            })
                            .collect();
                        let raft_req = crate::raft::types::OxiDbRequest::CommitTransaction {
                            write_ops: raft_ops,
                        };
                        let result = raft.client_write(raft_req).await;
                        log_audit(state, session, &cmd, None, "ok", "");
                        return match result {
                            Ok(resp) => {
                                let raft_resp: crate::raft::types::OxiDbResponse = resp.data;
                                match raft_resp {
                                    crate::raft::types::OxiDbResponse::Ok { data } => {
                                        handler::ok_bytes(data)
                                    }
                                    crate::raft::types::OxiDbResponse::Error { message } => {
                                        handler::err_bytes(&message)
                                    }
                                }
                            }
                            Err(e) => handler::err_bytes(&format!("raft error: {e}")),
                        };
                    }
                    Err(e) => {
                        return handler::err_bytes(&format!("transaction error: {e}"));
                    }
                }
            } else {
                return handler::err_bytes("no active transaction");
            }
        }
    }

    // ---------------------------------------------------------------
    // Write routing through Raft (cluster mode)
    // ---------------------------------------------------------------
    if let Some(raft) = &state.raft {
        if is_write_command(&cmd) && active_tx.is_none() {
            let raft_req = match build_raft_request(&cmd, &request) {
                Some(req) => req,
                None => {
                    // Fall through to local handler if we can't build a raft request
                    return dispatch_local(
                        state,
                        request,
                        active_tx,
                        session,
                        &cmd,
                        collection.as_deref(),
                        sql_readonly,
                    )
                    .await;
                }
            };
            let result = raft.client_write(raft_req).await;
            log_audit(state, session, &cmd, collection.as_deref(), "ok", "");
            return match result {
                Ok(resp) => {
                    let raft_resp: OxiDbResponse = resp.data;
                    match raft_resp {
                        OxiDbResponse::Ok { data } => handler::ok_bytes(data),
                        OxiDbResponse::Error { message } => handler::err_bytes(&message),
                    }
                }
                Err(e) => handler::err_bytes(&format!("raft error: {e}")),
            };
        }
    }

    // ---------------------------------------------------------------
    // Local execution (standalone mode, reads, or transactions)
    // ---------------------------------------------------------------
    dispatch_local(
        state,
        request,
        active_tx,
        session,
        &cmd,
        collection.as_deref(),
        sql_readonly,
    )
    .await
}

/// Execute a request locally via spawn_blocking.
#[allow(clippy::too_many_arguments)]
async fn dispatch_local(
    state: &ServerState,
    request: Value,
    active_tx: &mut Option<u64>,
    session: &Session,
    cmd: &str,
    collection: Option<&str>,
    sql_readonly: bool,
) -> Vec<u8> {
    let db = Arc::clone(&state.db);

    // Transaction commands must be handled in the current task (they modify active_tx).
    match cmd {
        "begin_tx" | "commit_tx" | "rollback_tx" => {
            let resp_bytes = handler::handle_request(&db, request, active_tx);
            log_audit(state, session, cmd, collection, "ok", "");
            return resp_bytes;
        }
        _ => {}
    }

    // All other commands: run handler in a blocking thread.
    let mut tx = active_tx.take();
    let resp_bytes = tokio::task::spawn_blocking(move || {
        let resp = handler::handle_request_opts(&db, request, &mut tx, sql_readonly);
        (resp, tx)
    })
    .await
    .unwrap_or_else(|e| (handler::err_bytes(&format!("internal error: {e}")), None));
    *active_tx = resp_bytes.1;
    let bytes = resp_bytes.0;

    log_audit(state, session, cmd, collection, "ok", "");
    bytes
}

/// Returns true if the command is a write operation that should go through Raft.
fn is_write_command(cmd: &str) -> bool {
    matches!(
        cmd,
        "insert"
            | "insert_many"
            | "update"
            | "update_one"
            | "delete"
            | "delete_one"
            | "create_collection"
            | "create_collection_with_options"
            | "drop_collection"
            | "compact"
            | "create_index"
            | "create_unique_index"
            | "create_composite_index"
            | "create_text_index"
            | "drop_index"
            | "create_bucket"
            | "delete_bucket"
            | "put_object"
            | "delete_object"
    )
}

/// Build an `OxiDbRequest` from the JSON request for Raft replication.
fn build_raft_request(cmd: &str, request: &Value) -> Option<OxiDbRequest> {
    let collection = request
        .get("collection")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    match cmd {
        "insert" => Some(OxiDbRequest::Insert {
            collection: collection?,
            document: request.get("doc")?.clone(),
        }),
        "insert_many" => Some(OxiDbRequest::InsertMany {
            collection: collection?,
            documents: request.get("docs").and_then(|v| v.as_array()).cloned()?,
        }),
        "update" => Some(OxiDbRequest::Update {
            collection: collection?,
            query: request.get("query")?.clone(),
            update: request.get("update")?.clone(),
        }),
        "update_one" => Some(OxiDbRequest::UpdateOne {
            collection: collection?,
            query: request.get("query")?.clone(),
            update: request.get("update")?.clone(),
        }),
        "delete" => Some(OxiDbRequest::Delete {
            collection: collection?,
            query: request.get("query")?.clone(),
        }),
        "delete_one" => Some(OxiDbRequest::DeleteOne {
            collection: collection?,
            query: request.get("query")?.clone(),
        }),
        "create_collection" => Some(OxiDbRequest::CreateCollection { name: collection? }),
        "create_collection_with_options" => {
            // Parse the options up front so the leader validates before
            // replicating. On a parse error return `None` → the caller falls
            // through to local execution, which reports the invalid-options
            // error to the client (rather than replicating a doomed entry).
            let options = match request.get("options") {
                Some(v) => serde_json::from_value::<oxidb::StorageOptions>(v.clone()).ok()?,
                None => oxidb::StorageOptions::default(),
            };
            Some(OxiDbRequest::CreateCollectionWithOptions {
                name: collection?,
                options,
            })
        }
        "drop_collection" => Some(OxiDbRequest::DropCollection { name: collection? }),
        "compact" => Some(OxiDbRequest::Compact {
            collection: collection?,
        }),
        "create_index" => Some(OxiDbRequest::CreateIndex {
            collection: collection?,
            field: request.get("field")?.as_str()?.to_string(),
        }),
        "create_unique_index" => Some(OxiDbRequest::CreateUniqueIndex {
            collection: collection?,
            field: request.get("field")?.as_str()?.to_string(),
        }),
        "create_composite_index" => {
            let fields: Option<Vec<String>> =
                request.get("fields").and_then(|v| v.as_array()).map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                });
            Some(OxiDbRequest::CreateCompositeIndex {
                collection: collection?,
                fields: fields?,
            })
        }
        "create_text_index" => {
            let fields: Option<Vec<String>> =
                request.get("fields").and_then(|v| v.as_array()).map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                });
            Some(OxiDbRequest::CreateTextIndex {
                collection: collection?,
                fields: fields?,
            })
        }
        "drop_index" => Some(OxiDbRequest::DropIndex {
            collection: collection?,
            index: request.get("index")?.as_str()?.to_string(),
        }),
        "create_bucket" => Some(OxiDbRequest::CreateBucket {
            bucket: request.get("bucket")?.as_str()?.to_string(),
        }),
        "delete_bucket" => Some(OxiDbRequest::DeleteBucket {
            bucket: request.get("bucket")?.as_str()?.to_string(),
        }),
        "put_object" => Some(OxiDbRequest::PutObject {
            bucket: request.get("bucket")?.as_str()?.to_string(),
            key: request.get("key")?.as_str()?.to_string(),
            data_b64: request.get("data")?.as_str()?.to_string(),
            content_type: request
                .get("content_type")
                .and_then(|v| v.as_str())
                .unwrap_or("application/octet-stream")
                .to_string(),
            metadata: request.get("metadata").cloned().unwrap_or(json!({})),
        }),
        "delete_object" => Some(OxiDbRequest::DeleteObject {
            bucket: request.get("bucket")?.as_str()?.to_string(),
            key: request.get("key")?.as_str()?.to_string(),
        }),
        _ => None,
    }
}

/// Handle authentication commands. Mirrors the sync main.rs logic exactly.
fn handle_auth(cmd: &str, request: &Value, state: &ServerState, session: &mut Session) -> Vec<u8> {
    match cmd {
        "ping" => handler::ok_bytes(json!("pong")),

        "authenticate" => {
            let client_first = request
                .get("payload")
                .and_then(|v| v.as_str())
                .unwrap_or("");

            let user_store_guard = state.user_store.as_ref().unwrap().lock().unwrap();
            match ScramState::process_client_first(client_first, &user_store_guard) {
                Ok((server_first, scram_state)) => {
                    drop(user_store_guard);
                    session.scram_state = Some(scram_state);
                    handler::ok_bytes(json!({
                        "payload": server_first,
                        "done": false,
                    }))
                }
                Err(e) => handler::err_bytes(&e),
            }
        }

        "authenticate_continue" => {
            let client_final = request
                .get("payload")
                .and_then(|v| v.as_str())
                .unwrap_or("");

            if let Some(scram_state) = session.scram_state.take() {
                let user_store_guard = state.user_store.as_ref().unwrap().lock().unwrap();
                match scram_state.process_client_final(client_final, &user_store_guard) {
                    Ok((server_final, role)) => {
                        let username = scram_state.username().to_string();
                        drop(user_store_guard);
                        session.set_authenticated(username, role);
                        handler::ok_bytes(json!({
                            "payload": server_final,
                            "done": true,
                        }))
                    }
                    Err(e) => handler::err_bytes(&e),
                }
            } else {
                handler::err_bytes("no SCRAM state; send 'authenticate' first")
            }
        }

        "auth_simple" => {
            let username = request
                .get("username")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let password = request
                .get("password")
                .and_then(|v| v.as_str())
                .unwrap_or("");

            let user_store_guard = state.user_store.as_ref().unwrap().lock().unwrap();
            match user_store_guard.authenticate(username, password) {
                Some(role) => {
                    drop(user_store_guard);
                    session.set_authenticated(username.to_string(), role);
                    handler::ok_bytes(json!({
                        "role": role.as_str(),
                        "done": true,
                    }))
                }
                None => handler::err_bytes("authentication failed"),
            }
        }

        _ => handler::err_bytes("authentication required"),
    }
}

fn log_audit(
    state: &ServerState,
    session: &Session,
    cmd: &str,
    collection: Option<&str>,
    result: &str,
    detail: &str,
) {
    if let Some(audit) = &state.audit_log {
        audit.log(&AuditEvent {
            ts: audit::now_rfc3339(),
            user: session.username_str(),
            cmd,
            collection,
            result,
            detail,
        });
    }
}

#[cfg(test)]
mod create_with_options_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn build_raft_request_parses_options() {
        assert!(is_write_command("create_collection_with_options"));

        let req = build_raft_request(
            "create_collection_with_options",
            &json!({"collection": "c", "options": {"disk_first": true, "compress": false}}),
        )
        .expect("should build a raft request");
        match req {
            OxiDbRequest::CreateCollectionWithOptions { name, options } => {
                assert_eq!(name, "c");
                assert!(options.disk_first);
                assert!(!options.compress);
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn build_raft_request_defaults_when_options_absent() {
        let req = build_raft_request(
            "create_collection_with_options",
            &json!({"collection": "c"}),
        )
        .expect("should build with default options");
        match req {
            OxiDbRequest::CreateCollectionWithOptions { options, .. } => {
                // Defaults: in-RAM, compressed.
                assert!(!options.disk_first);
                assert!(options.compress);
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn build_raft_request_invalid_options_falls_through() {
        // Wrong type for a field → parse fails → None (caller runs locally and
        // returns the error to the client instead of replicating a bad entry).
        let req = build_raft_request(
            "create_collection_with_options",
            &json!({"collection": "c", "options": {"disk_first": "yes"}}),
        );
        assert!(req.is_none());
    }
}
