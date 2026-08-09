//! Database-management intents (ADR-0012), shared by both dispatchers.
//!
//! A database operation can arrive two ways — as a wire command
//! (`create_database` / `drop_database` / `list_databases` / `use_db`) or as
//! SQL text (`CREATE DATABASE x` / `DROP DATABASE x` / `SHOW DATABASES` /
//! `USE x`). Both forms parse into one [`DbIntent`] here, get the same
//! permission gate, and execute through the same code, so the two surfaces
//! can never drift. Responses keep each surface's native shape: wire
//! commands answer with strings/arrays as before; SQL text answers with
//! SQL-result-shaped JSON.

use oxidb::DatabaseManager;
use serde_json::{Value, json};

use crate::auth::Role;
use crate::handler::{err_bytes, ok_bytes};
use crate::session::Session;

/// One database-management operation, plus which surface it arrived on
/// (`via_sql` selects the response shape).
pub enum DbIntent {
    Create {
        name: String,
        tolerate_exists: bool,
        via_sql: bool,
    },
    Drop {
        name: String,
        tolerate_missing: bool,
        via_sql: bool,
    },
    List {
        via_sql: bool,
    },
    Use {
        name: String,
        via_sql: bool,
    },
}

impl DbIntent {
    /// Creating and dropping databases is Admin-only (matches the RBAC table
    /// for the wire commands; enforced here so the SQL-text form can't slip
    /// through the `sql` command's ReadWrite gate).
    pub fn requires_admin(&self) -> bool {
        matches!(self, DbIntent::Create { .. } | DbIntent::Drop { .. })
    }

    /// The wire-command name this intent corresponds to (for audit logs).
    pub fn audit_cmd(&self) -> &'static str {
        match self {
            DbIntent::Create { .. } => "create_database",
            DbIntent::Drop { .. } => "drop_database",
            DbIntent::List { .. } => "list_databases",
            DbIntent::Use { .. } => "use_db",
        }
    }
}

/// Parse a request into a database-management intent, from either surface.
/// `None` = not a database operation (dispatch continues normally).
pub fn parse_intent(cmd: &str, request: &Value) -> Option<DbIntent> {
    // Wire commands.
    let name_field = || {
        request
            .get("name")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    };
    match cmd {
        "create_database" => {
            return Some(DbIntent::Create {
                name: name_field()?,
                tolerate_exists: false,
                via_sql: false,
            });
        }
        "drop_database" => {
            return Some(DbIntent::Drop {
                name: name_field()?,
                tolerate_missing: false,
                via_sql: false,
            });
        }
        "list_databases" => return Some(DbIntent::List { via_sql: false }),
        "use_db" => {
            return Some(DbIntent::Use {
                name: name_field()?,
                via_sql: false,
            });
        }
        _ => {}
    }

    // SQL text.
    let is_sql = cmd == "sql" || request.get("engine").and_then(|v| v.as_str()) == Some("sql");
    if !is_sql {
        return None;
    }
    let sql = request.get("sql").and_then(|v| v.as_str())?;
    Some(match oxidb_sql::parse_database_statement(sql)? {
        oxidb_sql::DatabaseStatement::Create {
            name,
            if_not_exists,
        } => DbIntent::Create {
            name,
            tolerate_exists: if_not_exists,
            via_sql: true,
        },
        oxidb_sql::DatabaseStatement::Drop { name, if_exists } => DbIntent::Drop {
            name,
            tolerate_missing: if_exists,
            via_sql: true,
        },
        oxidb_sql::DatabaseStatement::Show => DbIntent::List { via_sql: true },
        oxidb_sql::DatabaseStatement::Use { name } => DbIntent::Use {
            name,
            via_sql: true,
        },
    })
}

/// Check the permission gate for an intent. `None` = permitted.
pub fn permission_error(
    intent: &DbIntent,
    session: &Session,
    auth_enabled: bool,
) -> Option<Vec<u8>> {
    if auth_enabled && intent.requires_admin() && session.role() != Some(Role::Admin) {
        return Some(err_bytes(&format!(
            "permission denied: '{}' requires the admin role",
            intent.audit_cmd()
        )));
    }
    None
}

/// Execute an intent against this node's database registry (and session, for
/// `USE`). Raft routing, when applicable, happens before this in the cluster
/// dispatcher.
pub fn execute_local(
    intent: &DbIntent,
    db_manager: &DatabaseManager,
    session: &mut Session,
) -> Vec<u8> {
    match intent {
        DbIntent::Create {
            name,
            tolerate_exists,
            via_sql,
        } => match db_manager.create_database(name) {
            Ok(()) => created_ok(name, *via_sql),
            Err(oxidb::Error::DatabaseAlreadyExists(_)) if *tolerate_exists => {
                created_ok(name, *via_sql)
            }
            Err(e) => err_bytes(&e.to_string()),
        },
        DbIntent::Drop {
            name,
            tolerate_missing,
            via_sql,
        } => match db_manager.drop_database(name) {
            Ok(()) => {
                crate::sql_bridge::forget_database(name);
                crate::tsdb_bridge::forget_database(name);
                crate::rec_bridge::forget_database(name);
                dropped_ok(name, *via_sql)
            }
            Err(oxidb::Error::DatabaseNotFound(_)) if *tolerate_missing => {
                dropped_ok(name, *via_sql)
            }
            Err(e) => err_bytes(&e.to_string()),
        },
        DbIntent::List { via_sql } => {
            let names = db_manager.list_databases();
            if *via_sql {
                let rows: Vec<Value> = names.into_iter().map(|n| json!([n])).collect();
                ok_bytes(json!([{ "columns": ["database"], "rows": rows }]))
            } else {
                ok_bytes(json!(names))
            }
        }
        DbIntent::Use { name, via_sql } => {
            if !db_manager.database_exists(name) {
                return err_bytes(&format!("database not found: {name}"));
            }
            session.set_database(name.to_string());
            if *via_sql {
                ok_bytes(json!([{ "use": name }]))
            } else {
                ok_bytes(json!(format!("switched to database '{name}'")))
            }
        }
    }
}

/// Shape the success response for a create/drop that was applied through
/// Raft (the state machine returns plain strings; each surface keeps its
/// native shape, same as `execute_local`).
pub fn replicated_response(intent: &DbIntent) -> Vec<u8> {
    match intent {
        DbIntent::Create { name, via_sql, .. } => created_ok(name, *via_sql),
        DbIntent::Drop { name, via_sql, .. } => dropped_ok(name, *via_sql),
        // List/Use never route through Raft.
        DbIntent::List { .. } | DbIntent::Use { .. } => ok_bytes(json!(null)),
    }
}

/// Handle a user-management statement arriving as SQL text (`CREATE USER` /
/// `ALTER USER` / `DROP USER` / `SHOW USERS` / `GRANT role ON DATABASE` /
/// `REVOKE ... ON DATABASE`). Returns `None` when the request is not one.
/// All user statements are Admin-only, matching the wire commands' gate; the
/// user store itself is node-local (like the wire commands — not
/// Raft-replicated). On success, mutations answer `[{"ddl": true}]` and
/// `SHOW USERS` answers a SELECT-shaped result.
///
/// The returned tuple carries the equivalent wire-command name for audit
/// logging.
pub fn handle_sql_user_statement(
    cmd: &str,
    request: &Value,
    user_store: Option<&std::sync::Arc<std::sync::Mutex<crate::auth::UserStore>>>,
    session: &Session,
    auth_enabled: bool,
) -> Option<(&'static str, Vec<u8>)> {
    let is_sql = cmd == "sql" || request.get("engine").and_then(|v| v.as_str()) == Some("sql");
    if !is_sql {
        return None;
    }
    let sql = request.get("sql").and_then(|v| v.as_str())?;
    let stmt = oxidb_sql::parse_user_statement(sql)?;

    use oxidb_sql::UserStatement as Us;
    let audit = match &stmt {
        Us::Create { .. } => "create_user",
        Us::Alter { .. } => "update_user",
        Us::Drop { .. } => "drop_user",
        Us::Show => "list_users",
        Us::Grant { .. } => "grant_db_role",
        Us::Revoke { .. } => "revoke_db_role",
    };

    if auth_enabled && session.role() != Some(Role::Admin) {
        return Some((
            audit,
            err_bytes(&format!(
                "permission denied: '{audit}' requires the admin role"
            )),
        ));
    }
    let Some(store) = user_store else {
        return Some((
            audit,
            err_bytes("user management requires authentication (set OXIDB_AUTH=1)"),
        ));
    };

    let parse_role = |r: &str| {
        Role::from_str(r).ok_or_else(|| format!("invalid role: {r} (admin/readwrite/read)"))
    };
    let ddl_ok = || ok_bytes(json!([{ "ddl": true }]));

    let mut store = store.lock().unwrap();
    let resp = match stmt {
        Us::Create {
            name,
            password,
            role,
        } => {
            let role = match role.as_deref().map(parse_role).transpose() {
                Ok(r) => r.unwrap_or(Role::Read),
                Err(e) => return Some((audit, err_bytes(&e))),
            };
            match store.create_user(&name, &password, role) {
                Ok(()) => ddl_ok(),
                Err(e) => err_bytes(&e),
            }
        }
        Us::Alter {
            name,
            password,
            role,
        } => {
            let role = match role.as_deref().map(parse_role).transpose() {
                Ok(r) => r,
                Err(e) => return Some((audit, err_bytes(&e))),
            };
            match store.update_user(&name, password.as_deref(), role) {
                Ok(()) => ddl_ok(),
                Err(e) => err_bytes(&e),
            }
        }
        Us::Drop { name, if_exists } => match store.drop_user(&name) {
            Ok(()) => ddl_ok(),
            Err(_) if if_exists => ddl_ok(),
            Err(e) => err_bytes(&e),
        },
        Us::Show => {
            let rows: Vec<Value> = store
                .list_users()
                .into_iter()
                .map(|u| {
                    let db_roles = u.get("db_roles").map(|d| d.to_string()).unwrap_or_default();
                    json!([u["username"], u["role"], db_roles])
                })
                .collect();
            ok_bytes(json!([{ "columns": ["user", "role", "db_roles"], "rows": rows }]))
        }
        Us::Grant {
            role,
            database,
            user,
        } => {
            let role = match parse_role(&role) {
                Ok(r) => r,
                Err(e) => return Some((audit, err_bytes(&e))),
            };
            match store.grant_db_role(&user, &database, role) {
                Ok(()) => ddl_ok(),
                Err(e) => err_bytes(&e),
            }
        }
        Us::Revoke { database, user } => match store.revoke_db_role(&user, &database) {
            Ok(()) => ddl_ok(),
            Err(e) => err_bytes(&e),
        },
    };
    Some((audit, resp))
}

fn created_ok(name: &str, via_sql: bool) -> Vec<u8> {
    if via_sql {
        ok_bytes(json!([{ "ddl": true }]))
    } else {
        ok_bytes(json!(format!("database '{name}' created")))
    }
}

fn dropped_ok(name: &str, via_sql: bool) -> Vec<u8> {
    if via_sql {
        ok_bytes(json!([{ "ddl": true }]))
    } else {
        ok_bytes(json!(format!("database '{name}' dropped")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn intent(cmd: &str, req: serde_json::Value) -> Option<DbIntent> {
        parse_intent(cmd, &req)
    }

    #[test]
    fn wire_and_sql_forms_parse_to_the_same_intents() {
        // Wire command.
        let a = intent("create_database", json!({"name": "crm"})).unwrap();
        assert!(matches!(&a, DbIntent::Create { name, via_sql: false, .. } if name == "crm"));
        // SQL text via the `sql` command…
        let b = intent("sql", json!({"sql": "CREATE DATABASE crm"})).unwrap();
        assert!(matches!(&b, DbIntent::Create { name, via_sql: true, .. } if name == "crm"));
        // …and via engine routing.
        let c = intent("sql", json!({"engine": "sql", "sql": "USE crm"})).unwrap();
        assert!(matches!(&c, DbIntent::Use { name, via_sql: true } if name == "crm"));
        // Ordinary SQL is not an intent.
        assert!(intent("sql", json!({"sql": "SELECT 1 FROM t"})).is_none());
        // Ordinary commands are not intents.
        assert!(intent("find", json!({"collection": "x"})).is_none());
    }

    #[test]
    fn create_and_drop_require_admin_when_auth_enabled() {
        use crate::auth::Role;
        let mut s = Session::new();
        s.set_authenticated("bob".into(), Role::ReadWrite);
        let create = intent("create_database", json!({"name": "x"})).unwrap();
        assert!(permission_error(&create, &s, true).is_some());
        assert!(permission_error(&create, &s, false).is_none()); // no auth = open
        let list = intent("list_databases", json!({})).unwrap();
        assert!(permission_error(&list, &s, true).is_none());

        s.set_authenticated("root".into(), Role::Admin);
        assert!(permission_error(&create, &s, true).is_none());
    }

    #[test]
    fn execute_local_full_cycle_with_sql_shapes() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = oxidb::DatabaseManager::open(dir.path(), None, false, None).unwrap();
        let mut session = Session::new();

        let create = intent("sql", json!({"sql": "CREATE DATABASE crm"})).unwrap();
        let resp: Value =
            serde_json::from_slice(&execute_local(&create, &mgr, &mut session)).unwrap();
        assert_eq!(resp["ok"], json!(true));
        assert_eq!(resp["data"], json!([{ "ddl": true }]));

        // IF NOT EXISTS tolerates the duplicate; plain CREATE errors.
        let again = intent("sql", json!({"sql": "CREATE DATABASE IF NOT EXISTS crm"})).unwrap();
        let resp: Value =
            serde_json::from_slice(&execute_local(&again, &mgr, &mut session)).unwrap();
        assert_eq!(resp["ok"], json!(true));
        let dup = intent("sql", json!({"sql": "CREATE DATABASE crm"})).unwrap();
        let resp: Value = serde_json::from_slice(&execute_local(&dup, &mgr, &mut session)).unwrap();
        assert_eq!(resp["ok"], json!(false));

        // SHOW DATABASES comes back SELECT-shaped.
        let show = intent("sql", json!({"sql": "SHOW DATABASES"})).unwrap();
        let resp: Value =
            serde_json::from_slice(&execute_local(&show, &mgr, &mut session)).unwrap();
        assert_eq!(resp["data"][0]["columns"], json!(["database"]));
        let rows = resp["data"][0]["rows"].as_array().unwrap();
        assert!(rows.contains(&json!(["crm"])));

        // USE switches the session.
        let use_stmt = intent("sql", json!({"sql": "USE crm"})).unwrap();
        let resp: Value =
            serde_json::from_slice(&execute_local(&use_stmt, &mgr, &mut session)).unwrap();
        assert_eq!(resp["ok"], json!(true));
        assert_eq!(session.current_database, "crm");

        // DROP via wire shape.
        let drop = intent("drop_database", json!({"name": "crm"})).unwrap();
        let resp: Value =
            serde_json::from_slice(&execute_local(&drop, &mgr, &mut session)).unwrap();
        assert_eq!(resp["ok"], json!(true));
        assert_eq!(resp["data"], json!("database 'crm' dropped"));
    }
}

#[cfg(test)]
mod sql_user_tests {
    use super::*;
    use crate::auth::{Role, UserStore};
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    fn run(
        sql: &str,
        store: &Arc<Mutex<UserStore>>,
        session: &Session,
        auth: bool,
    ) -> Option<(&'static str, serde_json::Value)> {
        handle_sql_user_statement("sql", &json!({ "sql": sql }), Some(store), session, auth)
            .map(|(a, b)| (a, serde_json::from_slice(&b).unwrap()))
    }

    #[test]
    fn sql_user_lifecycle() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Mutex::new(UserStore::open(dir.path()).unwrap()));
        let mut admin = Session::new();
        admin.set_authenticated("root".into(), Role::Admin);

        let (audit, r) = run(
            "CREATE USER ali WITH PASSWORD 'gizli' ROLE readwrite",
            &store,
            &admin,
            true,
        )
        .unwrap();
        assert_eq!((audit, r["ok"].clone()), ("create_user", json!(true)));

        let (_, r) = run("GRANT read ON DATABASE crm TO ali", &store, &admin, true).unwrap();
        assert_eq!(r["ok"], json!(true));

        let (_, r) = run("SHOW USERS", &store, &admin, true).unwrap();
        assert_eq!(r["data"][0]["columns"], json!(["user", "role", "db_roles"]));
        let rows = r["data"][0]["rows"].as_array().unwrap();
        assert!(
            rows.iter()
                .any(|row| row[0] == "ali" && row[1] == "readWrite")
        );

        let (_, r) = run("ALTER USER ali ROLE read", &store, &admin, true).unwrap();
        assert_eq!(r["ok"], json!(true));
        let (_, r) = run("REVOKE ALL ON DATABASE crm FROM ali", &store, &admin, true).unwrap();
        assert_eq!(r["ok"], json!(true));
        let (_, r) = run("DROP USER ali", &store, &admin, true).unwrap();
        assert_eq!(r["ok"], json!(true));
        // IF EXISTS tolerates the missing user; plain DROP errors.
        let (_, r) = run("DROP USER IF EXISTS ali", &store, &admin, true).unwrap();
        assert_eq!(r["ok"], json!(true));
        let (_, r) = run("DROP USER ali", &store, &admin, true).unwrap();
        assert_eq!(r["ok"], json!(false));

        // Non-admin is refused; ordinary SQL is not intercepted.
        let mut rw = Session::new();
        rw.set_authenticated("bob".into(), Role::ReadWrite);
        let (_, r) = run("CREATE USER eve WITH PASSWORD 'x'", &store, &rw, true).unwrap();
        assert_eq!(r["ok"], json!(false));
        assert!(run("SELECT 1 FROM t", &store, &admin, true).is_none());
    }
}
