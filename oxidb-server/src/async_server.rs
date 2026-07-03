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
    /// The default database — Raft applies and disconnect rollback target it
    /// directly; per-request routing goes through `db_manager`.
    pub db: Arc<OxiDb>,
    /// Multi-database registry (ADR-0012). `None` keeps every request on the
    /// default database (e.g. minimal test setups).
    pub db_manager: Option<Arc<oxidb::DatabaseManager>>,
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

    // Auto-rollback active transactions on disconnect.
    if let Some(tx_id) = active_tx {
        let _ = state.db.rollback_transaction(tx_id);
    }
    if let Some(sql_tx) = session.sql_tx {
        let db_name = session
            .tx_db
            .as_deref()
            .unwrap_or(&session.current_database);
        crate::sql_bridge::rollback_session_tx(db_name, sql_tx);
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
    // Database management (ADR-0012) — wire commands and SQL text parse
    // into one intent, share the permission gate, and (in cluster mode)
    // replicate create/drop through Raft so every node converges.
    // ---------------------------------------------------------------
    if let Some(db_manager) = &state.db_manager {
        if let Some(intent) = crate::db_admin::parse_intent(&cmd, &request) {
            if let Some(denied) =
                crate::db_admin::permission_error(&intent, session, state.auth_enabled)
            {
                log_audit(state, session, intent.audit_cmd(), None, "denied", "");
                return denied;
            }
            let raft_req = match (&state.raft, &intent) {
                (
                    Some(_),
                    crate::db_admin::DbIntent::Create {
                        name,
                        tolerate_exists,
                        ..
                    },
                ) => Some(OxiDbRequest::CreateDatabase {
                    name: name.clone(),
                    if_not_exists: *tolerate_exists,
                }),
                (
                    Some(_),
                    crate::db_admin::DbIntent::Drop {
                        name,
                        tolerate_missing,
                        ..
                    },
                ) => Some(OxiDbRequest::DropDatabase {
                    name: name.clone(),
                    if_exists: *tolerate_missing,
                }),
                _ => None,
            };
            let resp = if let (Some(raft), Some(raft_req)) = (&state.raft, raft_req) {
                match raft.client_write(raft_req).await {
                    Ok(resp) => match resp.data {
                        OxiDbResponse::Ok { .. } => crate::db_admin::replicated_response(&intent),
                        OxiDbResponse::Error { message } => handler::err_bytes(&message),
                    },
                    Err(e) => handler::err_bytes(&format!("raft error: {e}")),
                }
            } else {
                crate::db_admin::execute_local(&intent, db_manager, session)
            };
            log_audit(state, session, intent.audit_cmd(), None, "ok", "");
            return resp;
        }
    }

    // User management as SQL text (CREATE/ALTER/DROP USER, SHOW USERS,
    // GRANT/REVOKE ... ON DATABASE) — same Admin gate as the wire commands;
    // node-local, like the wire user commands.
    if let Some((audit_cmd, resp)) = crate::db_admin::handle_sql_user_statement(
        &cmd,
        &request,
        state.user_store.as_ref(),
        session,
        state.auth_enabled,
    ) {
        log_audit(state, session, audit_cmd, None, "ok", "");
        return resp;
    }

    // ---------------------------------------------------------------
    // Target database for this request + transaction binding: an open
    // transaction's buffered writes belong to the database it began on.
    // ---------------------------------------------------------------
    let target_db_name = request
        .get("db")
        .and_then(|v| v.as_str())
        .unwrap_or(&session.current_database)
        .to_string();
    if (active_tx.is_some() || session.sql_tx.is_some())
        && let Some(tx_db) = &session.tx_db
        && tx_db != &target_db_name
    {
        return handler::err_bytes(&format!(
            "active transaction is bound to database '{tx_db}'; commit or roll it back before using '{target_db_name}'"
        ));
    }

    // Interactive SQL transactions in cluster mode (ADR-0013 Phase B):
    // statements execute locally on this node (writes buffer in the parked
    // session transaction); the COMMIT is intercepted here and the buffered
    // ops replicate through Raft as one atomic batch. To keep the protocol
    // unambiguous, BEGIN and COMMIT must each be their own request.
    if let Some(raft) = &state.raft {
        let sql_text = request.get("sql").and_then(|v| v.as_str());
        let is_sql = cmd == "sql" || request.get("engine").and_then(|v| v.as_str()) == Some("sql");
        if is_sql && let Some(sql) = sql_text {
            if session.sql_tx.is_none()
                && oxidb_sql::leaves_transaction_open(sql).unwrap_or(false)
                && !oxidb_sql::is_lone_begin(sql)
            {
                return handler::err_bytes(
                    "in cluster mode, start an interactive transaction with a lone BEGIN \
                     (or send a self-contained BEGIN..COMMIT batch)",
                );
            }
            if let Some(txn_id) = session.sql_tx
                && oxidb_sql::is_lone_commit(sql)
            {
                let ops = match crate::sql_bridge::take_session_ops(&target_db_name, txn_id) {
                    Ok(ops) => ops,
                    Err(e) => {
                        session.sql_tx = None;
                        return handler::err_bytes(&e);
                    }
                };
                session.sql_tx = None;
                let raft_req = scope_to_db(OxiDbRequest::SqlTxnCommit { ops }, &target_db_name);
                log_audit(state, session, &cmd, None, "ok", "");
                return match raft.client_write(raft_req).await {
                    Ok(resp) => match resp.data {
                        OxiDbResponse::Ok { .. } => {
                            handler::ok_bytes(serde_json::json!([{ "transaction": true }]))
                        }
                        OxiDbResponse::Error { message } => handler::err_bytes(&message),
                    },
                    Err(e) => handler::err_bytes(&format!("raft error: {e}")),
                };
            }
        }
    }

    // ---------------------------------------------------------------
    // Transaction commit through Raft (cluster mode)
    // ---------------------------------------------------------------
    if let Some(raft) = &state.raft {
        if cmd == "commit_tx" {
            if let Some(tx_id) = active_tx.take() {
                // The transaction was begun on its bound database's engine;
                // extract the buffered writes from that same engine.
                let tx_engine = match (&state.db_manager, &session.tx_db) {
                    (Some(mgr), Some(tx_db)) => match mgr.get_database(tx_db) {
                        Ok(db) => db,
                        Err(e) => return handler::err_bytes(&e.to_string()),
                    },
                    _ => Arc::clone(&state.db),
                };
                // Extract buffered writes from the transaction and send as one Raft entry
                match tx_engine.extract_transaction_writes(tx_id) {
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
                        let raft_req = scope_to_db(
                            crate::raft::types::OxiDbRequest::CommitTransaction {
                                write_ops: raft_ops,
                            },
                            session.tx_db.as_deref().unwrap_or(&target_db_name),
                        );
                        session.tx_db = None;
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
        if (is_write_command(&cmd) || sql_is_write(&cmd, &request))
            && active_tx.is_none()
            && session.sql_tx.is_none()
        {
            let raft_req = match build_raft_request(&cmd, &request) {
                Some(req) => scope_to_db(req, &target_db_name),
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
    let resp = dispatch_local(
        state,
        request,
        active_tx,
        session,
        &cmd,
        collection.as_deref(),
        sql_readonly,
    )
    .await;

    // Keep the transaction's database binding in step with its lifecycle:
    // set when a transaction appears, cleared when both kinds end.
    let any_tx = active_tx.is_some() || session.sql_tx.is_some();
    match (any_tx, &session.tx_db) {
        (true, None) => session.tx_db = Some(target_db_name),
        (false, Some(_)) => session.tx_db = None,
        _ => {}
    }
    resp
}

/// Wrap a replicated write so it applies to `db_name` on every node. The
/// default database stays unwrapped — byte-identical to pre-multi-database
/// log entries.
fn scope_to_db(req: OxiDbRequest, db_name: &str) -> OxiDbRequest {
    if db_name == oxidb::database_manager::DEFAULT_DATABASE || db_name == "postgres" {
        req
    } else {
        OxiDbRequest::Scoped {
            db: db_name.to_string(),
            inner: Box::new(req),
        }
    }
}

/// Execute a request locally via spawn_blocking.
#[allow(clippy::too_many_arguments)]
async fn dispatch_local(
    state: &ServerState,
    request: Value,
    active_tx: &mut Option<u64>,
    session: &mut Session,
    cmd: &str,
    collection: Option<&str>,
    sql_readonly: bool,
) -> Vec<u8> {
    // Resolve the target database: explicit `db` field, else the session's
    // current database (set by `use_db`, default `oxidb`).
    let db_name = request
        .get("db")
        .and_then(|v| v.as_str())
        .unwrap_or(&session.current_database)
        .to_string();
    let db = match &state.db_manager {
        Some(mgr) => match mgr.get_database(&db_name) {
            Ok(db) => db,
            Err(e) => return handler::err_bytes(&e.to_string()),
        },
        None => Arc::clone(&state.db),
    };

    // Transaction commands must be handled in the current task (they modify active_tx).
    match cmd {
        "begin_tx" | "commit_tx" | "rollback_tx" => {
            let mut sql_tx = session.sql_tx;
            let resp_bytes = handler::handle_request_session(
                &db,
                &db_name,
                request,
                active_tx,
                &mut sql_tx,
                sql_readonly,
            );
            session.sql_tx = sql_tx;
            log_audit(state, session, cmd, collection, "ok", "");
            return resp_bytes;
        }
        _ => {}
    }

    // All other commands: run handler in a blocking thread.
    let mut tx = active_tx.take();
    let mut sql_tx = session.sql_tx.take();
    let resp_bytes = tokio::task::spawn_blocking(move || {
        let resp = handler::handle_request_session(
            &db,
            &db_name,
            request,
            &mut tx,
            &mut sql_tx,
            sql_readonly,
        );
        (resp, tx, sql_tx)
    })
    .await
    .unwrap_or_else(|e| {
        (
            handler::err_bytes(&format!("internal error: {e}")),
            None,
            None,
        )
    });
    *active_tx = resp_bytes.1;
    session.sql_tx = resp_bytes.2;
    let bytes = resp_bytes.0;

    log_audit(state, session, cmd, collection, "ok", "");
    bytes
}

/// A `sql` request is a write (Raft-replicated in cluster mode) iff it parses
/// and contains any non-SELECT statement. SELECT-only SQL — and SQL that fails
/// to parse, so the local handler produces the real error message — runs
/// node-locally.
fn sql_is_write(cmd: &str, request: &Value) -> bool {
    cmd == "sql"
        && request
            .get("sql")
            .and_then(|v| v.as_str())
            .is_some_and(|s| !oxidb_sql::is_read_only(s).unwrap_or(true))
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
        // SQL engine writes (ADR-0010): the SQL string + params replicate
        // verbatim and re-execute on every node.
        "sql" => Some(OxiDbRequest::Sql {
            sql: request.get("sql")?.as_str()?.to_string(),
            params: request.get("params").cloned().unwrap_or(Value::Null),
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
    fn sql_write_classification() {
        // SELECT-only SQL runs locally; any write statement replicates.
        let read = json!({"cmd": "sql", "sql": "SELECT a FROM t UNION SELECT b FROM u"});
        assert!(!sql_is_write("sql", &read));
        for sql in [
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET a = 1",
            "DELETE FROM t",
            "CREATE TABLE t (a INT)",
            "DROP TABLE t",
            "CREATE INDEX i ON t (a)",
            "SELECT a FROM t; INSERT INTO t VALUES (1)",
            "BEGIN; UPDATE t SET a = 1; COMMIT;",
        ] {
            assert!(
                sql_is_write("sql", &json!({"cmd": "sql", "sql": sql})),
                "should classify as write: {sql}"
            );
        }
        // Unparseable SQL runs locally so the error message reaches the client.
        assert!(!sql_is_write(
            "sql",
            &json!({"cmd": "sql", "sql": "SELEKT"})
        ));
        // Only the sql command is affected.
        assert!(!sql_is_write("insert", &json!({"cmd": "insert"})));
    }

    #[test]
    fn build_raft_request_sql_carries_text_and_params() {
        let req = build_raft_request(
            "sql",
            &json!({"cmd": "sql", "sql": "INSERT INTO t VALUES (?)", "params": [7]}),
        )
        .expect("should build a raft request");
        match req {
            OxiDbRequest::Sql { sql, params } => {
                assert_eq!(sql, "INSERT INTO t VALUES (?)");
                assert_eq!(params, json!([7]));
            }
            other => panic!("wrong variant: {other:?}"),
        }
        // No params -> Null marker.
        match build_raft_request("sql", &json!({"cmd": "sql", "sql": "DROP TABLE t"})).unwrap() {
            OxiDbRequest::Sql { params, .. } => assert_eq!(params, Value::Null),
            other => panic!("wrong variant: {other:?}"),
        }
        // Missing sql field -> falls through to local execution.
        assert!(build_raft_request("sql", &json!({"cmd": "sql"})).is_none());
    }

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
