//! OxiWire `HELLO` handshake — ADR-0003 Phase 2.
//!
//! Optional first message a client sends to negotiate wire version and learn
//! which engine features the server has compiled in. Backward-compatible:
//! older clients that don't send `hello` still work; the connection just
//! defaults to wire v1.
//!
//! HELLO is allowed **pre-auth** (it's pure server-info, no data access) and
//! is **idempotent** (can be sent at any point in the connection). It does
//! NOT change session auth state.

use serde_json::{json, Value};

use crate::handler;
use crate::session::Session;

/// Wire protocol versions this server can speak. The negotiation picks the
/// highest mutually-supported version; absence of overlap is a hard error.
pub const SUPPORTED_WIRE_VERSIONS: &[u32] = &[1];

/// Engine version (matches the binary's `CARGO_PKG_VERSION`).
pub const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Stable 1.0 surface version this build targets. See `docs/STABILITY.md`.
pub const STABLE_SURFACE_VERSION: &str = "1.0";

/// Features in the 1.0 stable surface (see `docs/STABILITY.md`).
const STABLE_FEATURES: &[&str] = &[
    "fts",
    "blobs",
    "txn",
    "rbac",
    "tls",
    "encryption_at_rest",
    "audit",
    "scram_sha_256",
    "indexes",
    "aggregation",
];

/// Features explicitly outside the 1.0 surface (see `docs/STABILITY.md`).
/// Listed here so clients can detect availability without hitting commands
/// blindly.
const EXPERIMENTAL_FEATURES: &[&str] = &[
    "raft",
    "pitr",
    "vector_search",
    "fdw",
    "stored_procedures",
    "ttl_indexes",
    "change_streams",
    "rest_http",
    "websocket",
    "oximem",
    "mqtt",
    "s3",
    "pg_wire",
    "gelf",
];

/// Returns true if the request's `cmd` is `"hello"`.
pub fn is_hello(request: &Value) -> bool {
    request.get("cmd").and_then(|v| v.as_str()) == Some("hello")
}

/// Handle a HELLO request. Returns the wire-format-agnostic JSON response
/// bytes the caller serializes back to the client.
///
/// `auth_enabled` reflects the server's `OXIDB_AUTH` setting at startup —
/// the client uses this to know which auth methods are available.
pub fn handle(request: &Value, session: &mut Session, auth_enabled: bool) -> Vec<u8> {
    // Pick the wire version. Default to v1 if the client doesn't say.
    let client_versions: Vec<u32> = request
        .get("wire_versions")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|x| x.as_u64().map(|n| n as u32))
                .collect()
        })
        .unwrap_or_else(|| vec![1]);

    let chosen = SUPPORTED_WIRE_VERSIONS
        .iter()
        .rev()
        .copied()
        .find(|v| client_versions.contains(v));

    let chosen = match chosen {
        Some(v) => v,
        None => {
            return handler::err_bytes(&format!(
                "no compatible wire version (server supports {:?}, client offered {:?})",
                SUPPORTED_WIRE_VERSIONS, client_versions
            ));
        }
    };

    session.wire_version = chosen;

    let auth_methods: Vec<&str> = if auth_enabled {
        vec!["scram-sha-256"]
    } else {
        vec!["anonymous"]
    };

    // HELLO returns the server info at top level (not wrapped in
    // `{"ok": true, "data": …}` like normal commands) so clients can read
    // negotiated state without an extra indirection.
    let resp = json!({
        "ok": true,
        "server": {
            "name": "oxidb-server",
            "version": SERVER_VERSION,
            "wire_version": chosen,
            "supported_wire_versions": SUPPORTED_WIRE_VERSIONS,
            "stable_surface_version": STABLE_SURFACE_VERSION,
            "features": STABLE_FEATURES,
            "experimental_features": EXPERIMENTAL_FEATURES,
            "auth_methods": auth_methods,
        },
    });
    serde_json::to_vec(&resp)
        .unwrap_or_else(|_| handler::err_bytes("hello serialization failed"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_hello_cmd() {
        assert!(is_hello(&json!({"cmd": "hello"})));
        assert!(!is_hello(&json!({"cmd": "find"})));
        assert!(!is_hello(&json!({})));
    }

    #[test]
    fn responds_with_server_info() {
        let mut session = Session::new();
        let resp_bytes = handle(&json!({"cmd": "hello"}), &mut session, false);
        let resp: Value = serde_json::from_slice(&resp_bytes).expect("valid JSON");
        assert_eq!(resp["ok"], json!(true));
        assert_eq!(resp["server"]["name"], json!("oxidb-server"));
        assert_eq!(resp["server"]["wire_version"], json!(1));
        assert_eq!(resp["server"]["stable_surface_version"], json!("1.0"));
        assert_eq!(resp["server"]["auth_methods"], json!(["anonymous"]));
        assert!(resp["server"]["features"].as_array().unwrap().contains(&json!("fts")));
        assert!(resp["server"]["experimental_features"]
            .as_array()
            .unwrap()
            .contains(&json!("raft")));
        assert_eq!(session.wire_version, 1);
    }

    #[test]
    fn picks_highest_mutually_supported_version() {
        let mut session = Session::new();
        let resp_bytes = handle(
            &json!({"cmd": "hello", "wire_versions": [1, 2, 3]}),
            &mut session,
            false,
        );
        let resp: Value = serde_json::from_slice(&resp_bytes).expect("valid JSON");
        assert_eq!(resp["server"]["wire_version"], json!(1));
    }

    #[test]
    fn rejects_no_compatible_version() {
        let mut session = Session::new();
        let resp_bytes = handle(
            &json!({"cmd": "hello", "wire_versions": [2, 3]}),
            &mut session,
            false,
        );
        let resp: Value = serde_json::from_slice(&resp_bytes).expect("valid JSON");
        assert_eq!(resp["ok"], json!(false));
        assert!(resp["error"].as_str().unwrap().contains("no compatible wire version"));
    }

    #[test]
    fn advertises_scram_when_auth_enabled() {
        let mut session = Session::new();
        let resp_bytes = handle(&json!({"cmd": "hello"}), &mut session, true);
        let resp: Value = serde_json::from_slice(&resp_bytes).expect("valid JSON");
        assert_eq!(resp["server"]["auth_methods"], json!(["scram-sha-256"]));
    }
}
