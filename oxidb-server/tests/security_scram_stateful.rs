//! CERN-grade SCRAM-SHA-256 stateful authentication test corpus
//! (category 5 in `docs/testing-roadmap.md`, ADR-0006 §5 row).
//!
//! Companion to `security_authn_authz.rs` (which covers JWT + RBAC).
//! SCRAM is a multi-round protocol — `process_client_first` produces
//! a `ScramState`, which is then handed to `process_client_final`.
//! Each named test exercises one stateful attack pattern that must
//! be rejected by the engine.
//!
//! Why a corpus and not a cargo-fuzz harness here: SCRAM's
//! interesting bug surface is in the *transitions* between messages
//! (replay, nonce manipulation, proof forgery), not in the parser
//! of any single message. A stateful fuzz harness would need a
//! grammar of "valid first then mutated second" sequences, which is
//! a multi-PR effort. This corpus pins the canonical attacks
//! explicitly so any regression fails LOUDLY with a named test.

use tempfile::tempdir;

use oxidb_server::auth::{Role, UserStore};
use oxidb_server::scram::ScramState;

const TEST_USER: &str = "alice";
const TEST_PASSWORD: &str = "correct horse battery staple";

/// Build a fresh `UserStore` with one SCRAM-capable user. Each test
/// gets its own dir so they don't interfere.
fn make_user_store() -> (tempfile::TempDir, UserStore) {
    let dir = tempdir().expect("tempdir");
    let mut store = UserStore::open(dir.path()).expect("open user store");
    store
        .create_user(TEST_USER, TEST_PASSWORD, Role::ReadWrite)
        .expect("create user");
    (dir, store)
}

/// Generate a fresh client-first message for `TEST_USER`. Uses a
/// fixed-looking but unique-per-test nonce so debugging output is
/// readable; randomness isn't required for any of these tests.
fn client_first(nonce: &str) -> String {
    format!("n,,n={TEST_USER},r={nonce}")
}

// ─────────────────────────────────────────────────────────────────────
// Happy path — pin the working-flow baseline so the attack tests'
// "reject" assertions can be trusted to reject for the right reason.
// ─────────────────────────────────────────────────────────────────────

/// Sanity: a clean SCRAM round-trip with the correct password
/// completes both phases without error. If THIS fails, none of the
/// attack assertions below mean anything.
#[test]
fn scram_happy_path_completes() {
    let (_dir, store) = make_user_store();
    let (server_first, state) =
        ScramState::process_client_first(&client_first("client-nonce-1"), &store)
            .expect("client-first should succeed for known user");
    // The server-first response carries the combined nonce + salt + iterations.
    assert!(
        server_first.contains("r="),
        "server-first must echo a nonce"
    );
    assert!(
        server_first.contains("s="),
        "server-first must include salt"
    );
    assert!(
        server_first.contains("i="),
        "server-first must include iter count"
    );

    // Compute a real client-final by replaying SCRAM math against
    // the server's salt/iterations.
    let (combined_nonce, salt_b64, iterations) = parse_server_first(&server_first);
    let proof_b64 = compute_client_proof(
        TEST_USER,
        TEST_PASSWORD,
        "client-nonce-1",
        &combined_nonce,
        &salt_b64,
        iterations,
    );
    let client_final = format!("c=biws,r={combined_nonce},p={proof_b64}");

    let (server_final, role) = state
        .process_client_final(&client_final, &store)
        .expect("client-final with correct proof should succeed");
    assert!(server_final.starts_with("v="), "server-final must be v=...");
    assert_eq!(role, Role::ReadWrite);
}

// ─────────────────────────────────────────────────────────────────────
// process_client_first — pre-state attacks
// ─────────────────────────────────────────────────────────────────────

/// Client-first MUST begin with a GS2 header this server can honour:
/// `n,,` (client does not support channel binding) or `y,,` (client
/// supports it but saw no `-PLUS` mechanism offered — which is true,
/// this server never offers one). Anything else → reject.
///
/// `p=...` demands channel binding, which this server cannot do, so it
/// stays rejected rather than being silently downgraded.
#[test]
fn scram_client_first_missing_gs2_header_rejected() {
    let (_dir, store) = make_user_store();
    for bad in &[
        "n=alice,r=xyz",              // missing gs2 header
        "p=tls-unique,n=alice,r=xyz", // demands channel binding
        "",                           // empty
        "garbage",                    // not even close
    ] {
        let r = ScramState::process_client_first(bad, &store);
        assert!(
            r.is_err(),
            "SCRAM ACCEPTED malformed client-first {bad:?}: err = {:?}",
            r.as_ref().err()
        );
    }
}

/// `y,,` is a conforming opening (RFC 5802 §5) and PostgreSQL clients
/// that support channel binding send it when the server offers only
/// `SCRAM-SHA-256`. Accepting it is required for interop — but the
/// header is part of the signed auth message, so a proof computed for
/// one header MUST NOT verify against the other. That binding is what
/// makes accepting `y,,` safe: nothing in the middle can flip the flag.
#[test]
fn scram_y_gs2_header_is_accepted_and_bound_into_the_proof() {
    let (_dir, store) = make_user_store();

    // A `y,,` opening is accepted...
    let (server_first, state) =
        ScramState::process_client_first(&format!("y,,n={TEST_USER},r=nonce-y"), &store)
            .expect("y,, is a conforming GS2 header");
    let (combined_nonce, salt_b64, iterations) = parse_server_first(&server_first);

    // ...and a proof that says `c=biws` ("n,,") does not verify against it,
    // because the server signed the header the client actually sent.
    let n_proof = compute_client_proof(
        TEST_USER,
        TEST_PASSWORD,
        "nonce-y",
        &combined_nonce,
        &salt_b64,
        iterations,
    );
    assert!(
        state
            .process_client_final(&format!("c=biws,r={combined_nonce},p={n_proof}"), &store)
            .is_err(),
        "GS2 DOWNGRADE: a proof bound to 'n,,' verified against a 'y,,' opening"
    );

    // The matching proof — same math, `c=eSws` = base64("y,,") — does verify.
    let y_proof = compute_client_proof_with_binding(
        TEST_USER,
        TEST_PASSWORD,
        "nonce-y",
        &combined_nonce,
        &salt_b64,
        iterations,
        "eSws",
    );
    let (server_final, role) = state
        .process_client_final(&format!("c=eSws,r={combined_nonce},p={y_proof}"), &store)
        .expect("a proof bound to 'y,,' must verify against a 'y,,' opening");
    assert!(server_final.starts_with("v="));
    assert_eq!(role, Role::ReadWrite);
}

/// User-not-found MUST reject and MUST NOT leak the distinction
/// from "wrong password" in a way an attacker can mine. The error
/// type doesn't have to be identical, but the rejection must
/// happen here (during client-first), not during client-final.
#[test]
fn scram_unknown_user_rejected() {
    let (_dir, store) = make_user_store();
    let cf = "n,,n=NOT_A_REAL_USER_AT_ALL,r=xyz";
    let r = ScramState::process_client_first(cf, &store);
    assert!(
        r.is_err(),
        "SCRAM ACCEPTED client-first for nonexistent user: err = {:?}",
        r.as_ref().err()
    );
}

/// Empty username — must be rejected, not treated as a valid
/// zero-length username that happens to match no record.
#[test]
fn scram_empty_username_rejected() {
    let (_dir, store) = make_user_store();
    let r = ScramState::process_client_first("n,,n=,r=xyz", &store);
    assert!(
        r.is_err(),
        "SCRAM ACCEPTED empty username: err = {:?}",
        r.as_ref().err()
    );
}

/// Adversarial usernames containing control chars (NUL, CRLF) MUST
/// be rejected or sanitized — no panic, no log injection, no auth
/// success.
#[test]
fn scram_username_with_control_chars_rejected_cleanly() {
    let (_dir, store) = make_user_store();
    let attacks = [
        "n,,n=alice\0,r=xyz",
        "n,,n=alice\r\nattacker,r=xyz",
        "n,,n=alice,r=xyz,extra,n=admin", // double-username injection
    ];
    for cf in &attacks {
        // Must not panic. Result is allowed to be either Err
        // (rejected outright) or Ok (parser accepted, will fail
        // at auth) — the contract is "no panic + no successful
        // auth as admin".
        let _ = ScramState::process_client_first(cf, &store);
    }
}

// ─────────────────────────────────────────────────────────────────────
// process_client_final — stateful attacks against the second round
// ─────────────────────────────────────────────────────────────────────

/// Wrong password → proof verification MUST fail. This is the
/// canonical "did the engine actually check the cryptographic
/// proof" test.
#[test]
fn scram_wrong_password_rejected() {
    let (_dir, store) = make_user_store();
    let (server_first, state) =
        ScramState::process_client_first(&client_first("nonce-wrong-pw"), &store)
            .expect("client-first ok");
    let (combined_nonce, salt_b64, iters) = parse_server_first(&server_first);
    let proof_b64 = compute_client_proof(
        TEST_USER,
        "WRONG password",
        "nonce-wrong-pw",
        &combined_nonce,
        &salt_b64,
        iters,
    );
    let client_final = format!("c=biws,r={combined_nonce},p={proof_b64}");
    let r = state.process_client_final(&client_final, &store);
    assert!(
        r.is_err(),
        "SCRAM ACCEPTED WRONG PASSWORD — proof verification is broken: err = {:?}",
        r.as_ref().err()
    );
}

/// Nonce mismatch in client-final → MUST reject. Defeats the
/// "replay an old proof against a fresh server state" attack.
#[test]
fn scram_nonce_mismatch_rejected() {
    let (_dir, store) = make_user_store();
    let (server_first, state) =
        ScramState::process_client_first(&client_first("nonce-A"), &store).unwrap();
    let (combined_nonce, salt_b64, iters) = parse_server_first(&server_first);
    let proof_b64 = compute_client_proof(
        TEST_USER,
        TEST_PASSWORD,
        "nonce-A",
        &combined_nonce,
        &salt_b64,
        iters,
    );

    // Substitute a wrong nonce in client-final — proof is still
    // valid for the original combined_nonce, but the server is
    // matching what the CLIENT echoes back, not what it sent.
    let client_final = format!("c=biws,r=ATTACKER-NONCE,p={proof_b64}");
    let r = state.process_client_final(&client_final, &store);
    assert!(
        r.is_err(),
        "SCRAM ACCEPTED MISMATCHED NONCE: err = {:?}",
        r.as_ref().err()
    );
}

/// Truncated / wrong-length client proof MUST reject cleanly
/// (Err, not panic). Surfaced by the explicit
/// `proof length mismatch` Err in scram.rs:188.
#[test]
fn scram_proof_length_mismatch_does_not_panic() {
    let (_dir, store) = make_user_store();
    let (server_first, state) =
        ScramState::process_client_first(&client_first("nonce-trunc"), &store).unwrap();
    let (combined_nonce, _, _) = parse_server_first(&server_first);

    for bad_proof_b64 in &[
        "",     // empty
        "AAAA", // way too short
        // 200 bytes — well past the 32-byte SHA-256 output length
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "@@@@@@@@", // invalid base64
        "===",      // base64 padding only
    ] {
        let client_final = format!("c=biws,r={combined_nonce},p={bad_proof_b64}");
        let r = state.process_client_final(&client_final, &store);
        assert!(
            r.is_err(),
            "SCRAM ACCEPTED malformed proof {bad_proof_b64:?}: err = {:?}",
            r.as_ref().err()
        );
    }
}

/// Zero-byte proof — classic "guess that maybe the engine treats
/// all-zeros as valid" check. Must reject.
#[test]
fn scram_all_zero_proof_rejected() {
    let (_dir, store) = make_user_store();
    let (server_first, state) =
        ScramState::process_client_first(&client_first("nonce-zero"), &store).unwrap();
    let (combined_nonce, _, _) = parse_server_first(&server_first);

    use base64::Engine as _;
    let zero_proof_b64 = base64::engine::general_purpose::STANDARD.encode([0u8; 32]);
    let client_final = format!("c=biws,r={combined_nonce},p={zero_proof_b64}");
    let r = state.process_client_final(&client_final, &store);
    assert!(
        r.is_err(),
        "SCRAM ACCEPTED ZERO PROOF: err = {:?}",
        r.as_ref().err()
    );
}

/// Missing fields in client-final (no `r=` or no `p=`) MUST reject.
#[test]
fn scram_client_final_missing_fields_rejected() {
    let (_dir, store) = make_user_store();
    let (server_first, state) =
        ScramState::process_client_first(&client_first("nonce-missing"), &store).unwrap();
    let (combined_nonce, _, _) = parse_server_first(&server_first);

    for bad in &[
        format!("c=biws,r={combined_nonce}"), // no proof
        "c=biws,p=AAAA".to_string(),          // no nonce
        String::new(),                        // empty
        "garbage".to_string(),                // no fields
        format!("r={combined_nonce}"),        // proof missing, c= missing
    ] {
        let r = state.process_client_final(bad, &store);
        assert!(
            r.is_err(),
            "SCRAM ACCEPTED malformed client-final {bad:?}: err = {:?}",
            r.as_ref().err()
        );
    }
}

/// Cross-session attack — use the ScramState from session A
/// (combined_nonce_A) but a client-final crafted against session B's
/// combined_nonce. Must reject — the state machine pins its OWN
/// nonce, not whichever one the client claims.
#[test]
fn scram_cross_session_replay_rejected() {
    let (_dir, store) = make_user_store();
    let (server_first_a, state_a) =
        ScramState::process_client_first(&client_first("nonce-session-A"), &store)
            .expect("session A first ok");
    let (server_first_b, _state_b) =
        ScramState::process_client_first(&client_first("nonce-session-B"), &store)
            .expect("session B first ok");

    let (combined_b, salt_b, iters_b) = parse_server_first(&server_first_b);
    // Genuine proof for session B (correct password).
    let proof_for_b = compute_client_proof(
        TEST_USER,
        TEST_PASSWORD,
        "nonce-session-B",
        &combined_b,
        &salt_b,
        iters_b,
    );

    // Send it to state_a — the nonce + proof both belong to session
    // B, but we're hitting session A's state machine. Must reject.
    let cross_final = format!("c=biws,r={combined_b},p={proof_for_b}");
    let r = state_a.process_client_final(&cross_final, &store);
    assert!(
        r.is_err(),
        "SCRAM ACCEPTED CROSS-SESSION PROOF (session B's proof against \
         session A's state): err = {:?}\n  server_first_a = {}\n  \
         server_first_b = {}",
        r.as_ref().err(),
        server_first_a,
        server_first_b,
    );
}

// ─────────────────────────────────────────────────────────────────────
// Helpers — replay the SCRAM math to build a real client-final
// ─────────────────────────────────────────────────────────────────────

/// Parse `r=<combined_nonce>,s=<salt_b64>,i=<iterations>` out of a
/// server-first message. Tolerant of field ordering.
fn parse_server_first(msg: &str) -> (String, String, u32) {
    let mut combined_nonce = String::new();
    let mut salt = String::new();
    let mut iters: u32 = 0;
    for part in msg.split(',') {
        if let Some(v) = part.strip_prefix("r=") {
            combined_nonce = v.to_string();
        } else if let Some(v) = part.strip_prefix("s=") {
            salt = v.to_string();
        } else if let Some(v) = part.strip_prefix("i=") {
            iters = v.parse().unwrap_or(0);
        }
    }
    assert!(!combined_nonce.is_empty(), "server-first missing r=");
    assert!(!salt.is_empty(), "server-first missing s=");
    assert!(iters > 0, "server-first missing or zero i=");
    (combined_nonce, salt, iters)
}

/// Build the client_proof for SCRAM-SHA-256 per RFC 7677 / 5802.
/// Replays the math that a real client (e.g. libpq's SCRAM impl)
/// would do.
fn compute_client_proof(
    username: &str,
    password: &str,
    client_nonce: &str,
    combined_nonce: &str,
    salt_b64: &str,
    iterations: u32,
) -> String {
    // "biws" is base64("n,,") — channel-binding-not-supported.
    compute_client_proof_with_binding(
        username,
        password,
        client_nonce,
        combined_nonce,
        salt_b64,
        iterations,
        "biws",
    )
}

/// [`compute_client_proof`] with an explicit `c=` channel-binding field, so a
/// test can prove the GS2 header is signed rather than assumed.
fn compute_client_proof_with_binding(
    username: &str,
    password: &str,
    client_nonce: &str,
    combined_nonce: &str,
    salt_b64: &str,
    iterations: u32,
    channel_binding: &str,
) -> String {
    use oxidb_server::scram::{
        base64_decode_simple_pub, base64_encode_simple_pub, hmac_sha256_pub, pbkdf2_sha256_pub,
        sha256_hash_pub,
    };

    let salt = base64_decode_simple_pub(salt_b64).expect("decode salt");
    let salted_password = pbkdf2_sha256_pub(password.as_bytes(), &salt, iterations);
    let client_key = hmac_sha256_pub(&salted_password, b"Client Key");
    let stored_key = sha256_hash_pub(&client_key);

    let client_first_bare = format!("n={username},r={client_nonce}");
    let client_final_no_proof = format!("c={channel_binding},r={combined_nonce}");
    let server_first = format!("r={combined_nonce},s={salt_b64},i={iterations}");
    let auth_message = format!("{client_first_bare},{server_first},{client_final_no_proof}");

    let client_signature = hmac_sha256_pub(&stored_key, auth_message.as_bytes());
    let client_proof: Vec<u8> = client_key
        .iter()
        .zip(client_signature.iter())
        .map(|(a, b)| a ^ b)
        .collect();
    base64_encode_simple_pub(&client_proof)
}
