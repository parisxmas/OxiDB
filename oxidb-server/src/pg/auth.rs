//! The PostgreSQL authentication handshake, over the server's existing user
//! store.
//!
//! PostgreSQL carries SCRAM-SHA-256 inside its own SASL envelope, but the
//! messages *inside* that envelope are the ones RFC 5802 specifies — the same
//! ones [`crate::scram::ScramState`] already speaks for the OxiWire port. So
//! this module is an envelope, not a second SCRAM implementation: unwrap,
//! delegate, wrap the reply.
//!
//! Which mechanism is offered depends on the account. A user created before
//! the SCRAM-verifier migration has no verifier to check a proof against, so
//! it is offered cleartext instead — over TLS if the connection has it, and
//! the operator can promote the account by resetting its password.

use std::io::{Read, Write};
use std::sync::Mutex;

use super::errors::{PgError, SQLSTATE_INVALID_AUTHORIZATION, SQLSTATE_INVALID_PASSWORD};
use super::wire::{self, Conn, F_PASSWORD, Reader};
use crate::auth::{Role, UserStore};
use crate::scram::ScramState;

const SCRAM_SHA_256: &str = "SCRAM-SHA-256";

/// Run the handshake. On success the client has been sent `AuthenticationOk`
/// and the caller may proceed to the startup parameters.
pub fn authenticate<S: Read + Write>(
    conn: &mut Conn<S>,
    user: &str,
    database: &str,
    auth_enabled: bool,
    user_store: Option<&Mutex<UserStore>>,
) -> Result<Role, PgError> {
    if !auth_enabled {
        // Matches the OxiWire path with auth off: the connection is trusted
        // and gets Admin (`async_server.rs`, `set_authenticated("anonymous")`).
        wire::auth_ok(conn.w()).map_err(io_err)?;
        return Ok(Role::Admin);
    }
    let Some(store) = user_store else {
        return Err(PgError::new(
            SQLSTATE_INVALID_AUTHORIZATION,
            "authentication is enabled but this server has no user store",
        ));
    };

    let has_verifier = {
        let guard = store.lock().unwrap();
        match guard.get_user(user) {
            Some(rec) => rec.scram_salt.is_some(),
            // Do not leak whether the account exists: offer SCRAM and let the
            // proof fail exactly as a wrong password would.
            None => true,
        }
    };

    if has_verifier {
        sasl_scram(conn, user, store)?;
    } else {
        cleartext(conn, user, store)?;
    }

    let role = store
        .lock()
        .unwrap()
        .effective_role(user, database)
        .ok_or_else(|| {
            PgError::new(
                SQLSTATE_INVALID_AUTHORIZATION,
                format!("role \"{user}\" does not exist"),
            )
        })?;
    wire::auth_ok(conn.w()).map_err(io_err)?;
    Ok(role)
}

fn sasl_scram<S: Read + Write>(
    conn: &mut Conn<S>,
    user: &str,
    store: &Mutex<UserStore>,
) -> Result<(), PgError> {
    wire::auth_sasl(conn.w(), &[SCRAM_SHA_256]).map_err(io_err)?;
    conn.flush().map_err(io_err)?;

    // SASLInitialResponse: mechanism name, then a length-prefixed blob.
    let msg = conn.read().map_err(io_err)?;
    if msg.tag != F_PASSWORD {
        return Err(PgError::protocol(format!(
            "expected a SASL response, got message '{}'",
            msg.tag as char
        )));
    }
    let mut r = Reader::new(&msg.body);
    let mechanism = r.cstring().map_err(io_err)?;
    if mechanism != SCRAM_SHA_256 {
        return Err(PgError::new(
            SQLSTATE_INVALID_AUTHORIZATION,
            format!("unsupported SASL mechanism {mechanism:?}; this server offers {SCRAM_SHA_256}"),
        ));
    }
    let client_first = match r.nullable_bytes().map_err(io_err)? {
        Some(b) => String::from_utf8_lossy(b).into_owned(),
        None => return Err(PgError::protocol("SASL initial response carried no data")),
    };

    let (server_first, state) = {
        let guard = store.lock().unwrap();
        ScramState::process_client_first(&client_first, &guard).map_err(auth_failed)?
    };
    // The username inside the SCRAM exchange must be the one the startup
    // packet claimed, or a proof for account A would authenticate session B.
    if state.username() != user {
        return Err(PgError::new(
            SQLSTATE_INVALID_AUTHORIZATION,
            "SCRAM username does not match the startup packet",
        ));
    }
    wire::auth_sasl_continue(conn.w(), &server_first).map_err(io_err)?;
    conn.flush().map_err(io_err)?;

    let msg = conn.read().map_err(io_err)?;
    if msg.tag != F_PASSWORD {
        return Err(PgError::protocol(format!(
            "expected the SASL final response, got message '{}'",
            msg.tag as char
        )));
    }
    let client_final = String::from_utf8_lossy(&msg.body).into_owned();
    let server_final = {
        let guard = store.lock().unwrap();
        state
            .process_client_final(&client_final, &guard)
            .map_err(auth_failed)?
            .0
    };
    wire::auth_sasl_final(conn.w(), &server_final).map_err(io_err)?;
    Ok(())
}

fn cleartext<S: Read + Write>(
    conn: &mut Conn<S>,
    user: &str,
    store: &Mutex<UserStore>,
) -> Result<(), PgError> {
    wire::auth_cleartext(conn.w()).map_err(io_err)?;
    conn.flush().map_err(io_err)?;
    let msg = conn.read().map_err(io_err)?;
    if msg.tag != F_PASSWORD {
        return Err(PgError::protocol(format!(
            "expected a password, got message '{}'",
            msg.tag as char
        )));
    }
    let password = Reader::new(&msg.body).cstring().map_err(io_err)?;
    let ok = store
        .lock()
        .unwrap()
        .authenticate(user, &password)
        .is_some();
    if !ok {
        return Err(auth_failed(format!(
            "password authentication failed for user \"{user}\""
        )));
    }
    Ok(())
}

/// Every credential failure answers the same way, whatever went wrong inside —
/// a distinct "no such user" would be an account-enumeration oracle. The
/// detail goes to the server log instead.
fn auth_failed(detail: String) -> PgError {
    eprintln!("[pg] authentication failed: {detail}");
    PgError::new(
        SQLSTATE_INVALID_PASSWORD,
        "password authentication failed".to_string(),
    )
}

fn io_err(e: std::io::Error) -> PgError {
    PgError::protocol(format!("connection error during authentication: {e}"))
}
