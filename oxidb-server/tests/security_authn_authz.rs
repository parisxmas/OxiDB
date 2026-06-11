//! CERN-grade authn/authz bypass test corpus (category 5 in
//! `docs/testing-roadmap.md`, ADR-0006 §5 row).
//!
//! Companion to `security_test.rs`. The existing file covers
//! happy-path encryption + the basic role-permission table; this
//! file covers **attack patterns** against the JWT decode path and
//! RBAC's fail-closed defaults.
//!
//! Each #[test] is one named attack vector. Every assertion is a
//! security CONTRACT that, if it ever passes (i.e. the test fails),
//! is an unauthenticated bypass that an external auditor would flag
//! at P0. Run as part of `cargo test -p oxidb-server`.

use base64::Engine as _;
use serde_json::{Value, json};

use oxidb_server::auth::Role;
use oxidb_server::jwt::{Claims, decode_jwt, encode_jwt};
use oxidb_server::rbac;

const TEST_SECRET: &str = "test-secret-NOT-FOR-PRODUCTION";

fn b64url(data: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(data)
}

fn make_valid_claims(role: &str) -> Claims {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    Claims {
        sub: "alice".into(),
        role: role.into(),
        iat: now,
        exp: now + 3600,
    }
}

// ─────────────────────────────────────────────────────────────────────
// JWT — tampering & forgery attempts
// ─────────────────────────────────────────────────────────────────────

/// Modifying the payload (e.g. role escalation) without re-signing
/// with the secret MUST cause verification to fail. This is the
/// canonical "store a JWT, edit it client-side, replay it" attack.
#[test]
fn jwt_role_escalation_payload_tamper_rejected() {
    let claims = make_valid_claims("read");
    let token = encode_jwt(&claims, TEST_SECRET);
    let parts: Vec<&str> = token.split('.').collect();
    assert_eq!(parts.len(), 3);

    // Tamper: decode payload, change role to admin, re-encode.
    let payload_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(parts[1])
        .unwrap();
    let mut payload: Value = serde_json::from_slice(&payload_bytes).unwrap();
    payload["role"] = json!("admin");
    let tampered_payload = b64url(&serde_json::to_vec(&payload).unwrap());

    // Reassemble: same header + tampered payload + original signature.
    let tampered = format!("{}.{}.{}", parts[0], tampered_payload, parts[2]);

    let result = decode_jwt(&tampered, TEST_SECRET);
    assert!(
        result.is_err(),
        "ROLE-ESCALATION TAMPER: decode_jwt accepted a payload-modified token \
         without re-signing. result = {result:?}"
    );
}

/// A token signed with secret A MUST NOT verify with secret B.
/// Catches "deployment used different secret in dev / prod" silent
/// trust patterns.
#[test]
fn jwt_wrong_secret_rejected() {
    let claims = make_valid_claims("admin");
    let token = encode_jwt(&claims, TEST_SECRET);
    let result = decode_jwt(&token, "a-completely-different-secret");
    assert!(
        result.is_err(),
        "WRONG-SECRET ACCEPTED: decode_jwt verified a token with a different \
         secret. result = {result:?}"
    );
}

/// Expired tokens MUST be rejected even if the signature is valid.
#[test]
fn jwt_expired_token_rejected() {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let claims = Claims {
        sub: "alice".into(),
        role: "admin".into(),
        iat: now - 7200,
        exp: now - 1, // expired 1 second ago
    };
    let token = encode_jwt(&claims, TEST_SECRET);
    let result = decode_jwt(&token, TEST_SECRET);
    assert!(
        result.is_err(),
        "EXPIRED TOKEN ACCEPTED: decode_jwt admitted a token with exp in the \
         past. result = {result:?}"
    );
}

/// alg: none attack — pretend signature is irrelevant by setting
/// the header's algorithm to "none" and providing an empty (or
/// guessed) signature. OxiDB's decode_jwt MUST require a valid
/// HMAC-SHA256 signature regardless of what the header claims.
#[test]
fn jwt_alg_none_header_does_not_bypass_signature() {
    let none_header = json!({"alg": "none", "typ": "JWT"});
    let payload = json!({
        "sub": "attacker",
        "role": "admin",
        "iat": 0u64,
        "exp": u64::MAX,
    });
    let header_b64 = b64url(&serde_json::to_vec(&none_header).unwrap());
    let payload_b64 = b64url(&serde_json::to_vec(&payload).unwrap());

    // Try three variations of the missing-signature attack:
    //   (a) empty signature field        "header.payload."
    //   (b) literal "none" as signature  "header.payload.none"
    //   (c) base64-empty signature       "header.payload.="
    for (variant, token) in [
        ("empty sig", format!("{header_b64}.{payload_b64}.")),
        ("literal 'none'", format!("{header_b64}.{payload_b64}.none")),
        ("base64-empty", format!("{header_b64}.{payload_b64}.=")),
    ] {
        let result = decode_jwt(&token, TEST_SECRET);
        assert!(
            result.is_err(),
            "ALG=NONE BYPASS ({variant}): decode_jwt accepted a token with no \
             valid signature. result = {result:?}"
        );
    }
}

/// Signature replaced with garbage MUST be rejected. Mirror of the
/// "wrong secret" case but with arbitrary attacker bytes rather than
/// a real HMAC value.
#[test]
fn jwt_garbage_signature_rejected() {
    let claims = make_valid_claims("admin");
    let token = encode_jwt(&claims, TEST_SECRET);
    let parts: Vec<&str> = token.split('.').collect();
    let garbage = format!("{}.{}.aGVsbG93b3JsZA", parts[0], parts[1]); // "helloworld" base64
    assert!(
        decode_jwt(&garbage, TEST_SECRET).is_err(),
        "GARBAGE SIGNATURE ACCEPTED"
    );
}

/// Missing `exp` claim MUST be rejected — without an expiry the
/// token effectively never expires, which is itself a security
/// regression that should fail closed.
#[test]
fn jwt_missing_exp_claim_rejected() {
    // Forge a token without exp. We can't use encode_jwt since it
    // always includes exp from the Claims struct; build by hand.
    let header = json!({"alg": "HS256", "typ": "JWT"});
    let payload = json!({"sub": "alice", "role": "admin", "iat": 0u64}); // no exp
    let header_b64 = b64url(&serde_json::to_vec(&header).unwrap());
    let payload_b64 = b64url(&serde_json::to_vec(&payload).unwrap());

    let signing_input = format!("{header_b64}.{payload_b64}");
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let mut mac = Hmac::<Sha256>::new_from_slice(TEST_SECRET.as_bytes()).unwrap();
    mac.update(signing_input.as_bytes());
    let sig = b64url(&mac.finalize().into_bytes());
    let token = format!("{signing_input}.{sig}");

    assert!(
        decode_jwt(&token, TEST_SECRET).is_err(),
        "MISSING EXP ACCEPTED: token without exp claim should fail closed"
    );
}

/// Malformed tokens (wrong number of parts) MUST be rejected
/// cleanly — no panic, no info leak.
#[test]
fn jwt_malformed_structure_rejected() {
    let cases = [
        "",                // empty
        "x",               // 1 part
        "x.y",             // 2 parts
        "x.y.z.w",         // 4 parts
        "...",             // 3 empty parts
        "header.payload.", // signature missing
        ".payload.sig",    // header missing
        "header..sig",     // payload missing
    ];
    for token in &cases {
        let result = decode_jwt(token, TEST_SECRET);
        assert!(
            result.is_err(),
            "MALFORMED TOKEN ACCEPTED: {token:?} → {result:?}"
        );
    }
}

/// A claims-shaped token signed with NULL secret string MUST NOT
/// be accepted for verification with any non-null secret. Catches
/// "developer left secret empty for testing" footguns.
#[test]
fn jwt_empty_string_secret_does_not_universally_unlock() {
    let claims = make_valid_claims("admin");
    let token_with_empty_secret = encode_jwt(&claims, "");
    assert!(
        decode_jwt(&token_with_empty_secret, TEST_SECRET).is_err(),
        "EMPTY-SECRET TOKEN ACCEPTED with real secret — empty-secret tokens \
         must not be universally trusted"
    );
}

// ─────────────────────────────────────────────────────────────────────
// RBAC — fail-closed defaults & edge cases
// ─────────────────────────────────────────────────────────────────────

/// Unknown commands MUST be denied by default (fail-closed). A
/// future engine that grows a new admin-class command must NOT
/// automatically be accessible to lower roles just because the
/// rbac table doesn't list it.
#[test]
fn rbac_unknown_command_denied_for_non_admin() {
    let novel_commands = [
        "RANDOM_NEW_ADMIN_THING",
        "drop_database", // not in current vocab
        "grant_role",    // hypothetical
        "set_password",
        "configure_replication",
        "", // empty string — degenerate input
    ];
    for cmd in &novel_commands {
        assert!(
            !rbac::is_permitted(Role::Read, cmd),
            "FAIL-OPEN: Role::Read was permitted to execute unknown command {cmd:?}"
        );
        assert!(
            !rbac::is_permitted(Role::ReadWrite, cmd),
            "FAIL-OPEN: Role::ReadWrite was permitted to execute unknown command {cmd:?}"
        );
    }
}

/// RBAC is intentionally case-sensitive (literal `matches!` against
/// lowercase command names in `rbac.rs`). The CONTRACT is "uppercase
/// = unknown = denied" — fail-closed in the case-mismatch direction.
/// This pins that direction: uppercased command names for any role
/// other than Admin MUST never escalate, regardless of what the
/// lowercase form would permit.
///
/// IMPORTANT CONTRACT NOTE for the handler layer: the wire handler
/// MUST canonicalize incoming command strings to lowercase before
/// calling `is_permitted`, otherwise a client sending `INSERT`
/// instead of `insert` is denied where it should be allowed —
/// a DoS, not an escalation. Verifying that the handler does so is
/// in `handler_test.rs`; this test just pins the rbac primitive.
#[test]
fn rbac_uppercase_commands_fail_closed_for_non_admin() {
    let uppercased = [
        "INSERT",
        "UPDATE",
        "DELETE",
        "FIND",
        "CREATE_COLLECTION",
        "DROP_COLLECTION",
        "CREATE_USER",
        "DELETE_USER",
    ];
    for cmd in &uppercased {
        assert!(
            !rbac::is_permitted(Role::Read, cmd),
            "Read got uppercase {cmd:?} permitted — case-mismatch must fail closed"
        );
        assert!(
            !rbac::is_permitted(Role::ReadWrite, cmd),
            "ReadWrite got uppercase {cmd:?} permitted — case-mismatch must fail closed"
        );
    }
}

/// The Read role explicitly MUST NOT have any write or admin
/// permissions. Companion to the existing `rbac_permissions` happy-
/// path test but adds attempts at NAMED admin-class commands that
/// have appeared in adjacent issues.
#[test]
fn rbac_read_role_locked_down() {
    let forbidden_for_read = [
        "insert",
        "update",
        "delete",
        "upsert",
        "drop_collection",
        "create_collection",
        "rename_collection",
        "create_user",
        "delete_user",
        "set_role",
        "create_index",
        "drop_index",
        "begin_tx",
        "commit_tx",
        "rollback_tx",
        "snapshot",
        "restore",
        "compact",
    ];
    for cmd in &forbidden_for_read {
        assert!(
            !rbac::is_permitted(Role::Read, cmd),
            "READ ROLE ESCAPE: Read was permitted {cmd:?}"
        );
    }
}

/// ReadWrite MUST NOT have admin-class permissions. List is
/// derived from what's NOT in `rbac::is_permitted`'s ReadWrite arm
/// (see `oxidb-server/src/rbac.rs`). Note: `create_collection` IS
/// legitimately ReadWrite-permitted (collections auto-create on
/// first insert per CLAUDE.md, so making this allowed-explicit is
/// consistent), so it's not in the forbidden list.
#[test]
fn rbac_readwrite_cannot_admin() {
    let admin_only = [
        "create_user",
        "delete_user",
        "set_role",
        "grant_role",
        "drop_collection", // explicitly NOT in the ReadWrite arm
    ];
    for cmd in &admin_only {
        assert!(
            !rbac::is_permitted(Role::ReadWrite, cmd),
            "RW ROLE ESCAPE: ReadWrite was permitted admin-class {cmd:?}"
        );
    }
}
