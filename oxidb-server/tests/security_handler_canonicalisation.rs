//! CERN-grade tests: pin the wire handler's command-name handling
//! contract.
//!
//! Companion to `security_authn_authz.rs` (JWT/RBAC) and
//! `security_scram_stateful.rs` (SCRAM). Surfaces an open question
//! left in PR #66's rbac_uppercase_commands_fail_closed_for_non_admin:
//! does the **handler** ALSO treat case-mismatched commands
//! consistently, or could a wire-level case manipulation produce a
//! split-brain where one layer accepts and another denies?
//!
//! After reading `oxidb-server/src/async_server.rs:dispatch_request`
//! and `oxidb-server/src/handler.rs:handle_request`, the actual
//! contract is:
//!
//!   - Both layers are **case-sensitive** (literal lowercase
//!     matches!). No canonicalization happens at either layer.
//!   - Uppercase command from a non-Admin client: RBAC denies it
//!     (matches! doesn't match) → "permission denied" error.
//!   - Uppercase command from an Admin client: RBAC allows
//!     (Admin == everything) → handler dispatch doesn't match →
//!     "unknown command: INSERT" error.
//!   - In NEITHER path does case-mismatch succeed in invoking the
//!     intended-but-mis-cased operation.
//!
//! These tests pin the handler-side property: the wire handler
//! NEVER silently executes a case-mismatched command, regardless of
//! which layer rejects it. If a future "developer convenience"
//! patch adds case-insensitive routing, one or more of these tests
//! flips and the change becomes intentional rather than silent.
//!
//! Scope: `handler::handle_request` only (sync path, no auth/RBAC
//! involved). The RBAC layer is already covered in
//! `security_authn_authz.rs::rbac_uppercase_commands_fail_closed_for_non_admin`.

use std::sync::Arc;

use serde_json::{json, Value};
use tempfile::tempdir;

use oxidb::OxiDb;
use oxidb_server::handler;

/// Build a fresh in-memory DB + the mutable-active-tx state the
/// handler expects.
fn make_db() -> (tempfile::TempDir, Arc<OxiDb>) {
    let dir = tempdir().unwrap();
    let db = Arc::new(OxiDb::open(dir.path()).unwrap());
    (dir, db)
}

/// Parse a handler response (length-prefixed JSON byte vec) into a
/// `serde_json::Value`. Asserts the response is parseable — silent
/// returns of malformed bytes would break every client.
fn parse_resp(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes)
        .unwrap_or_else(|e| panic!("handler returned unparseable bytes: {e}\nbytes = {bytes:?}"))
}

/// Send a request and assert the response indicates an error.
/// The "ok" field is the standard top-level success/failure flag.
fn expect_err(resp: Value, hint: &str) {
    let ok = resp.get("ok").and_then(|v| v.as_bool());
    assert_eq!(
        ok, Some(false),
        "{hint}: expected ok=false, got resp = {resp}",
    );
    let err = resp.get("error").and_then(|v| v.as_str());
    assert!(
        err.is_some(),
        "{hint}: expected an 'error' field with explanation, got resp = {resp}",
    );
}

// ─────────────────────────────────────────────────────────────────────
// Case-mismatch consistency — the handler MUST NOT silently execute
// a case-mismatched command. Each of these used to be "I assume it
// errors" — now it's pinned.
// ─────────────────────────────────────────────────────────────────────

/// All-uppercase commands MUST be unknown to the handler. The
/// dispatch table at `handler.rs:116` only contains literal
/// lowercase strings. Any future addition of case-insensitive
/// routing would flip this test.
#[test]
fn handler_uppercase_command_is_unknown() {
    let (_dir, db) = make_db();
    let mut tx: Option<u64> = None;
    for cmd in &["INSERT", "FIND", "DELETE", "UPDATE", "BEGIN_TX"] {
        let req = json!({
            "cmd": cmd,
            "collection": "test",
            "doc": {"x": 1},
        });
        let resp = parse_resp(&handler::handle_request(&db, req, &mut tx));
        expect_err(resp, &format!("uppercase {cmd:?}"));
    }

    // And the canonical "no row inserted by accident": the
    // collection should not exist (or be empty) after attempting
    // INSERT with uppercase.
    let count_req = json!({"cmd": "count", "collection": "test"});
    let count_resp = parse_resp(&handler::handle_request(&db, count_req, &mut tx));
    // count response shape: {"ok": true, "data": {"count": N}}
    let count = count_resp["data"]["count"].as_u64().unwrap_or(0);
    assert_eq!(
        count, 0,
        "uppercase INSERT must NOT have inserted anything — engine handled it as unknown cmd"
    );
}

/// Mixed-case variants (`Insert`, `iNsErT`) MUST also be unknown.
#[test]
fn handler_mixed_case_command_is_unknown() {
    let (_dir, db) = make_db();
    let mut tx: Option<u64> = None;
    for cmd in &["Insert", "iNsErT", "INserT", "FiNd"] {
        let req = json!({"cmd": cmd, "collection": "test", "doc": {"x": 1}});
        let resp = parse_resp(&handler::handle_request(&db, req, &mut tx));
        expect_err(resp, &format!("mixed-case {cmd:?}"));
    }
}

/// Whitespace-padded command names MUST be unknown — no implicit
/// trim that could let `" insert "` execute. Catches a hypothetical
/// "let's be friendly to clients" patch from breaking the
/// audit-log's name match.
#[test]
fn handler_whitespace_padded_command_is_unknown() {
    let (_dir, db) = make_db();
    let mut tx: Option<u64> = None;
    for cmd in &[" insert", "insert ", "  insert  ", "\tinsert", "insert\n"] {
        let req = json!({"cmd": cmd, "collection": "test", "doc": {"x": 1}});
        let resp = parse_resp(&handler::handle_request(&db, req, &mut tx));
        expect_err(resp, &format!("whitespace-padded {cmd:?}"));
    }
}

// ─────────────────────────────────────────────────────────────────────
// `cmd` field-shape robustness — non-string, missing, null, etc.
// All must return well-formed error responses, never panic.
// ─────────────────────────────────────────────────────────────────────

/// Missing `cmd` field → must err. (Existing handler returns
/// "missing or invalid 'cmd' field"; pin that contract.)
#[test]
fn handler_missing_cmd_field_rejected() {
    let (_dir, db) = make_db();
    let mut tx: Option<u64> = None;
    let req = json!({"collection": "test"});
    let resp = parse_resp(&handler::handle_request(&db, req, &mut tx));
    expect_err(resp, "missing cmd field");
}

/// `cmd: null` → must err.
#[test]
fn handler_null_cmd_value_rejected() {
    let (_dir, db) = make_db();
    let mut tx: Option<u64> = None;
    let req = json!({"cmd": null, "collection": "test"});
    let resp = parse_resp(&handler::handle_request(&db, req, &mut tx));
    expect_err(resp, "null cmd");
}

/// `cmd: <number>` and other non-string types → must err. Catches
/// any naive `.to_string()` on `Value` that would silently produce
/// e.g. "42" and dispatch on it.
#[test]
fn handler_non_string_cmd_value_rejected() {
    let (_dir, db) = make_db();
    let mut tx: Option<u64> = None;
    for bad in &[
        json!({"cmd": 42, "collection": "test"}),
        json!({"cmd": true, "collection": "test"}),
        json!({"cmd": [], "collection": "test"}),
        json!({"cmd": {}, "collection": "test"}),
    ] {
        let resp = parse_resp(&handler::handle_request(&db, bad.clone(), &mut tx));
        expect_err(resp, &format!("non-string cmd: {bad}"));
    }
}

/// `cmd: ""` → empty string. Must err (not unknown-but-no-op
/// silently). The empty string IS distinct from missing — the
/// field exists but holds nothing.
#[test]
fn handler_empty_string_cmd_rejected() {
    let (_dir, db) = make_db();
    let mut tx: Option<u64> = None;
    let req = json!({"cmd": "", "collection": "test"});
    let resp = parse_resp(&handler::handle_request(&db, req, &mut tx));
    expect_err(resp, "empty-string cmd");
}

/// Pathological command names — embedded control chars, very long
/// strings, NULL bytes. None should panic, all should err.
#[test]
fn handler_pathological_cmd_names_do_not_panic() {
    let (_dir, db) = make_db();
    let mut tx: Option<u64> = None;
    let long_cmd: String = "a".repeat(10_000);
    let cases = [
        "insert\0".to_string(),
        "insert\r\nDROP TABLE users".to_string(), // log-injection attempt
        "\u{202e}drop".to_string(),               // unicode RTL override
        long_cmd,
    ];
    for cmd in &cases {
        let req = json!({"cmd": cmd, "collection": "test"});
        // Must not panic; result must be well-formed (parseable).
        let resp = parse_resp(&handler::handle_request(&db, req, &mut tx));
        expect_err(resp, &format!("pathological cmd len={}", cmd.len()));
    }
}

// ─────────────────────────────────────────────────────────────────────
// Sanity baseline — pin that lowercase commands DO work, so the
// "rejected" assertions above can be trusted to reject for the
// right reason.
// ─────────────────────────────────────────────────────────────────────

/// Lowercase `insert` → succeeds. This is the happy-path baseline.
#[test]
fn handler_lowercase_insert_works_baseline() {
    let (_dir, db) = make_db();
    let mut tx: Option<u64> = None;
    let req = json!({"cmd": "insert", "collection": "test", "doc": {"x": 1}});
    let resp = parse_resp(&handler::handle_request(&db, req, &mut tx));
    let ok = resp.get("ok").and_then(|v| v.as_bool());
    assert_eq!(
        ok, Some(true),
        "lowercase insert MUST succeed (this baseline test failing means \
         the whole corpus is suspect): resp = {resp}"
    );
    // Confirm it actually inserted (count = 1 in the collection).
    let count_resp = parse_resp(&handler::handle_request(
        &db,
        json!({"cmd": "count", "collection": "test"}),
        &mut tx,
    ));
    assert_eq!(count_resp["data"]["count"].as_u64(), Some(1));
}
