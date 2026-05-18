//! CERN-grade tests: per-attack-vector audit-log evidence shape.
//!
//! Companion to `security_authn_authz.rs`, `security_scram_stateful.rs`,
//! and `security_handler_canonicalisation.rs`. Those corpora pin
//! that the engine REJECTS each named attack. This corpus pins that
//! the audit log preserves enough EVIDENCE about each rejection
//! for a downstream consumer (SIEM, log aggregator, forensic
//! investigator) to reconstruct what happened.
//!
//! Scope: shape and content of `AuditEvent` JSON for each attack
//! class. The actual dispatch-path integration (`dispatch_request`
//! is private in `async_server.rs`) would need TCP-level tests in
//! a separate slice — flagged in the PR follow-up. What this slice
//! delivers:
//!
//!   - The JSON shape per attack vector — every field a SIEM might
//!     filter on (`result`, `user`, `cmd`, `collection`) is present
//!     and parseable.
//!   - Adversarial input safety — a `cmd` field containing JSON-
//!     unsafe chars (quotes, backslash, CRLF) does NOT corrupt the
//!     log file's line-delimited-JSON structure.
//!   - Volume invariant — N rejection attempts produce N audit
//!     entries (no silent rate-limit / dedup that would hide an
//!     attack-in-progress from a defender).

use serde_json::Value;
use tempfile::tempdir;

use oxidb_server::audit::{AuditEvent, AuditLog};

/// Read the audit log file back as a Vec of parsed JSON values,
/// asserting every line is independently parseable (line-delimited
/// JSON — JSONL).
fn read_audit_log(dir: &std::path::Path) -> Vec<Value> {
    let path = dir.join("_audit/audit.log");
    let content = std::fs::read_to_string(&path).expect("audit log readable");
    content
        .lines()
        .enumerate()
        .map(|(i, line)| {
            serde_json::from_str(line).unwrap_or_else(|e| {
                panic!(
                    "audit log line {i} is not parseable JSON: {e}\n  line = {line:?}"
                )
            })
        })
        .collect()
}

/// Helper that logs one event and returns it parsed back. Removes
/// repetition from the per-vector tests below.
fn log_and_read_one(log: &AuditLog, event: &AuditEvent, dir: &std::path::Path) -> Value {
    log.log(event);
    let entries = read_audit_log(dir);
    assert_eq!(entries.len(), 1, "expected exactly one entry");
    entries.into_iter().next().unwrap()
}

// ─────────────────────────────────────────────────────────────────────
// Per-vector evidence — for each named attack class, pin what the
// audit log records.
// ─────────────────────────────────────────────────────────────────────

/// RBAC denial: the canonical `dispatch_request` denial path emits
/// `result="denied"` + the cmd that was denied. Forensic value:
/// a defender filtering for `result:"denied"` MUST see this event
/// or the attack is invisible.
#[test]
fn audit_evidence_rbac_denial_shape() {
    let dir = tempdir().unwrap();
    let log = AuditLog::open(dir.path()).unwrap();
    let entry = log_and_read_one(
        &log,
        &AuditEvent {
            ts: "2026-05-18T10:00:00Z".to_string(),
            user: "alice",
            cmd: "drop_collection",
            collection: Some("orders"),
            result: "denied",
            detail: "",
        },
        dir.path(),
    );
    assert_eq!(entry["result"], "denied", "RBAC denial must record result=denied");
    assert_eq!(entry["user"], "alice", "must preserve attacking user");
    assert_eq!(entry["cmd"], "drop_collection", "must preserve attempted command");
    assert_eq!(entry["collection"], "orders", "must preserve target collection");
    assert!(entry["ts"].is_string(), "must have a timestamp");
}

/// JWT verification failure (wrong secret / tampered payload).
/// The dispatch path treats this as auth failure — the cmd never
/// gets a chance to run. The log entry should still record what
/// was attempted.
#[test]
fn audit_evidence_jwt_failure_shape() {
    let dir = tempdir().unwrap();
    let log = AuditLog::open(dir.path()).unwrap();
    let entry = log_and_read_one(
        &log,
        &AuditEvent {
            ts: "2026-05-18T10:00:01Z".to_string(),
            user: "anonymous", // pre-auth — no real user yet
            cmd: "verify_jwt",
            collection: None,
            result: "denied",
            detail: "invalid signature",
        },
        dir.path(),
    );
    assert_eq!(entry["result"], "denied");
    assert_eq!(entry["user"], "anonymous");
    assert_eq!(entry["collection"], Value::Null, "no collection on auth failure");
    assert_eq!(entry["detail"], "invalid signature");
}

/// SCRAM auth failure — pinned analogously to JWT.
#[test]
fn audit_evidence_scram_failure_shape() {
    let dir = tempdir().unwrap();
    let log = AuditLog::open(dir.path()).unwrap();
    let entry = log_and_read_one(
        &log,
        &AuditEvent {
            ts: "2026-05-18T10:00:02Z".to_string(),
            user: "alice", // username was claimed but proof failed
            cmd: "scram_client_final",
            collection: None,
            result: "denied",
            detail: "authentication failed",
        },
        dir.path(),
    );
    assert_eq!(entry["result"], "denied");
    assert_eq!(entry["user"], "alice");
    assert_eq!(entry["cmd"], "scram_client_final");
}

/// Unknown command (handler.rs unknown-cmd arm). Less severe than
/// a denial but still worth logging — a flood of unknown commands
/// from one peer is a recon signal.
#[test]
fn audit_evidence_unknown_command_shape() {
    let dir = tempdir().unwrap();
    let log = AuditLog::open(dir.path()).unwrap();
    let entry = log_and_read_one(
        &log,
        &AuditEvent {
            ts: "2026-05-18T10:00:03Z".to_string(),
            user: "anonymous",
            cmd: "INSERT", // uppercase — handler rejects as unknown
            collection: Some("victims"),
            result: "error",
            detail: "unknown command: INSERT",
        },
        dir.path(),
    );
    assert_eq!(entry["cmd"], "INSERT", "must preserve the EXACT attempted cmd (incl case)");
    assert!(entry["detail"].as_str().unwrap().contains("INSERT"));
}

// ─────────────────────────────────────────────────────────────────────
// Adversarial-input safety — special chars in cmd / user / detail
// MUST NOT corrupt the JSONL file format.
// ─────────────────────────────────────────────────────────────────────

/// `cmd` containing JSON-unsafe chars (quote, backslash, CRLF) must
/// NOT corrupt the log file. JSONL relies on `\n` as line separator;
/// if the serializer doesn't escape the embedded `\n`, downstream
/// parsers split mid-record and EVERYTHING after that entry is
/// misaligned.
#[test]
fn audit_evidence_adversarial_chars_in_cmd_does_not_corrupt_jsonl() {
    let dir = tempdir().unwrap();
    let log = AuditLog::open(dir.path()).unwrap();

    let adversarial = [
        "drop\"; rm -rf /;",        // quote-injection
        "delete\\everything",       // backslash
        "INSERT\r\nSECOND COMMAND", // CRLF — log-line injection attempt
        "cmd\nwith\nnewlines",      // bare LF
        "cmd\x00with\x00nulls",     // NUL bytes
        "\u{202e}DROP",             // RTL override
    ];
    for cmd in &adversarial {
        log.log(&AuditEvent {
            ts: "2026-05-18T10:00:00Z".to_string(),
            user: "attacker",
            cmd,
            collection: None,
            result: "denied",
            detail: "",
        });
    }

    // EVERY entry must parse independently — that's the test.
    let entries = read_audit_log(dir.path());
    assert_eq!(
        entries.len(),
        adversarial.len(),
        "expected exactly {} entries (one per adversarial cmd) — \
         a JSONL-corruption attack would have collapsed entries together",
        adversarial.len()
    );

    // Each entry's cmd must match exactly (no silent stripping of
    // dangerous chars, no truncation at the first \n).
    for (entry, expected) in entries.iter().zip(adversarial.iter()) {
        assert_eq!(
            entry["cmd"].as_str(),
            Some(*expected),
            "cmd round-trip lost data: {:?} → {:?}",
            expected,
            entry["cmd"].as_str()
        );
    }
}

/// Same property for the `user` field — username SCRAM-bound to the
/// session may contain any UTF-8; an attacker controlling the
/// account name (via signup) could try to embed JSONL-corrupting
/// chars there.
#[test]
fn audit_evidence_adversarial_chars_in_user_does_not_corrupt_jsonl() {
    let dir = tempdir().unwrap();
    let log = AuditLog::open(dir.path()).unwrap();

    let names = [
        "alice\nadmin",
        "bob\"hacker",
        "charlie\\evil",
        "user\r\n{\"injected\":true}",
    ];
    for u in &names {
        log.log(&AuditEvent {
            ts: "2026-05-18T10:00:00Z".to_string(),
            user: u,
            cmd: "ping",
            collection: None,
            result: "ok",
            detail: "",
        });
    }
    let entries = read_audit_log(dir.path());
    assert_eq!(entries.len(), names.len(), "JSONL must stay aligned");
    for (entry, expected) in entries.iter().zip(names.iter()) {
        assert_eq!(entry["user"].as_str(), Some(*expected));
    }
}

// ─────────────────────────────────────────────────────────────────────
// Volume invariant — every attack attempt is logged, no silent dedup.
// ─────────────────────────────────────────────────────────────────────

/// 1000 identical RBAC denials produce 1000 log entries. Catches a
/// future "let's rate-limit duplicate denials to spare disk" patch
/// that would silently hide an attack-in-progress.
#[test]
fn audit_evidence_no_silent_dedup_on_repeated_denials() {
    let dir = tempdir().unwrap();
    let log = AuditLog::open(dir.path()).unwrap();

    const N: usize = 1000;
    for _ in 0..N {
        log.log(&AuditEvent {
            ts: "2026-05-18T10:00:00Z".to_string(),
            user: "attacker",
            cmd: "drop_collection",
            collection: Some("victims"),
            result: "denied",
            detail: "",
        });
    }

    let entries = read_audit_log(dir.path());
    assert_eq!(
        entries.len(),
        N,
        "AUDIT DEDUP: only {} of {N} repeated denials logged. \
         An attacker hammering the engine should see EVERY attempt logged.",
        entries.len()
    );
}

// ─────────────────────────────────────────────────────────────────────
// Schema invariant — every audit event has the SIEM-required fields.
// ─────────────────────────────────────────────────────────────────────

/// Pin the AuditEvent JSON shape. Downstream SIEMs parse on these
/// field names; reshaping would break log pipelines silently.
/// The full set: ts, user, cmd, collection, result, detail.
#[test]
fn audit_evidence_event_has_all_required_fields() {
    let dir = tempdir().unwrap();
    let log = AuditLog::open(dir.path()).unwrap();
    let entry = log_and_read_one(
        &log,
        &AuditEvent {
            ts: "2026-05-18T10:00:00Z".to_string(),
            user: "alice",
            cmd: "insert",
            collection: Some("orders"),
            result: "ok",
            detail: "doc_id=42",
        },
        dir.path(),
    );
    for field in &["ts", "user", "cmd", "collection", "result", "detail"] {
        assert!(
            entry.get(*field).is_some(),
            "audit event missing required field {field:?}: {entry}"
        );
    }
}
