//! Client-side SCRAM-SHA-256 (RFC 7677). The dual of `ScramState`:
//! whereas the server holds the verifier and validates a client's
//! proof, this state machine holds the plaintext password and emits
//! the proof, then verifies the server's signature on the back-end.
//!
//! Used by the FDW proxy (`remote_client::authenticate`) when a
//! linked-collection URL carries userinfo. Lives in `oxidb-server`
//! (not the engine crate) so it can share the existing
//! crypto-primitive helpers in `crate::scram`.
//!
//! The state machine is two steps:
//!
//!   ScramClient::new(user, password) -> Self
//!     .client_first()                -> String   // "n,,n=user,r=<nonce>"
//!     .client_final(server_first)    -> Result<(String, Vec<u8>), String>
//!                                                 // (client_final_msg, expected server_signature)
//!     verify_server_final(server_final, expected_sig) -> Result<(), String>
//!
//! Both sides hold a transcript hash implicitly via `auth_message`;
//! a tampered server-first or client-final breaks the proof, which
//! is exactly what RFC 7677 wants.

use crate::scram::{
    base64_decode_simple_pub as b64dec, base64_encode_simple_pub as b64enc,
    generate_salt_pub as random_bytes, hmac_sha256_pub as hmac256, pbkdf2_sha256_pub as pbkdf2,
    sha256_hash_pub as sha256,
};

/// ScramClient owns the plaintext password until consumed in
/// `client_final`. After that it holds only derived material (salted
/// password is dropped; only `server_key` survives for the server-
/// final verification step).
pub struct ScramClient {
    username: String,
    password: String,
    client_nonce: String,
    client_first_bare: Option<String>,
}

impl ScramClient {
    /// Construct a fresh state machine for one authentication
    /// exchange. The client_nonce is freshly random; reusing one
    /// across exchanges is a SCRAM protocol error.
    pub fn new(username: &str, password: &str) -> Self {
        // 16 random bytes, base64-encoded → ~22 ASCII chars. Plenty
        // for collision-resistance over the lifetime of one auth
        // exchange.
        let nonce_bytes = random_bytes();
        Self {
            username: username.to_string(),
            password: password.to_string(),
            client_nonce: b64enc(&nonce_bytes),
            client_first_bare: None,
        }
    }

    /// Emit client-first-message: `n,,n=<user>,r=<client_nonce>`.
    /// Records the bare form (without the `n,,` GS2 header) for the
    /// auth_message transcript later — the server expects that exact
    /// substring inside the signed message.
    pub fn client_first(&mut self) -> String {
        let bare = format!("n={},r={}", self.username, self.client_nonce);
        self.client_first_bare = Some(bare.clone());
        format!("n,,{}", bare)
    }

    /// Process server-first-message and emit client-final-message.
    /// Returns the wire message AND the server_signature the caller
    /// should expect to see echoed back in server-final — pass that
    /// to `verify_server_final` once the next response arrives.
    ///
    /// Errors when:
    ///   - server-first is malformed
    ///   - the combined nonce doesn't extend our client_nonce (a
    ///     server-side MITM swapped the nonce; abort the exchange)
    pub fn client_final(&mut self, server_first: &str) -> Result<(String, Vec<u8>), String> {
        let bare = self
            .client_first_bare
            .as_ref()
            .ok_or("client_final called before client_first")?
            .clone();

        // Parse server-first: r=<combined_nonce>,s=<salt_b64>,i=<iters>
        let mut combined_nonce = String::new();
        let mut salt_b64 = String::new();
        let mut iter_count: u32 = 0;
        for part in server_first.split(',') {
            if let Some(r) = part.strip_prefix("r=") {
                combined_nonce = r.to_string();
            } else if let Some(s) = part.strip_prefix("s=") {
                salt_b64 = s.to_string();
            } else if let Some(i) = part.strip_prefix("i=") {
                iter_count = i
                    .parse()
                    .map_err(|e| format!("bad i= in server-first: {}", e))?;
            }
        }
        if combined_nonce.is_empty() || salt_b64.is_empty() || iter_count == 0 {
            return Err(format!(
                "server-first missing one of r=/s=/i=: {:?}",
                server_first
            ));
        }
        if !combined_nonce.starts_with(&self.client_nonce) {
            return Err(format!(
                "server combined nonce does not extend client_nonce — possible MITM. \
                 client_nonce={:?} combined_nonce={:?}",
                self.client_nonce, combined_nonce
            ));
        }

        let salt = b64dec(&salt_b64).map_err(|e| format!("decode salt: {}", e))?;

        // Derive the SCRAM keys from plaintext + server salt. This
        // is the step the old (pre-RFC-7677) server-side
        // implementation could not authenticate against — it ran
        // PBKDF2 over the stored Argon2 hash instead of plaintext.
        let salted = pbkdf2(self.password.as_bytes(), &salt, iter_count);
        let client_key = hmac256(&salted, b"Client Key");
        let stored_key = sha256(&client_key);

        // The channel-binding indicator "biws" is base64 of "n,,",
        // i.e. "no channel binding" — matches what the server
        // accepts in process_client_final (no TLS-binding).
        let cb_b64 = "biws";
        let client_final_no_proof = format!("c={},r={}", cb_b64, combined_nonce);
        let auth_message = format!("{},{},{}", bare, server_first, client_final_no_proof);

        let client_signature = hmac256(&stored_key, auth_message.as_bytes());
        let mut proof = vec![0u8; client_key.len()];
        for i in 0..client_key.len() {
            proof[i] = client_key[i] ^ client_signature[i];
        }

        let client_final = format!("{},p={}", client_final_no_proof, b64enc(&proof));

        // Pre-compute the server_signature we expect to receive in
        // server-final, so the caller can verify it. Doing this here
        // (rather than asking the caller to re-derive in
        // verify_server_final) means the plaintext only needs to
        // touch the salted_password derivation once.
        let server_key = hmac256(&salted, b"Server Key");
        let expected_server_signature = hmac256(&server_key, auth_message.as_bytes());

        // Plaintext can be dropped now — we have everything we need.
        self.password.clear();

        Ok((client_final, expected_server_signature))
    }
}

/// Verify the server's `v=<server_signature>` reply against what we
/// pre-computed in `client_final`. Constant-time comparison so a
/// mismatch never depends on which byte was wrong.
pub fn verify_server_final(server_final: &str, expected_signature: &[u8]) -> Result<(), String> {
    let v_b64 = server_final
        .strip_prefix("v=")
        .ok_or("server-final must start with v=")?;
    let got = b64dec(v_b64).map_err(|e| format!("decode server signature: {}", e))?;
    if got.len() != expected_signature.len() {
        return Err("server signature length mismatch".into());
    }
    let mut diff: u8 = 0;
    for i in 0..got.len() {
        diff |= got[i] ^ expected_signature[i];
    }
    if diff != 0 {
        return Err("server signature mismatch — possible MITM or wrong password on server".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    //! Cross-test against the SERVER side of SCRAM (ScramState).
    //! Both halves talk to each other in-process; if either drifts
    //! from RFC 7677 the test fails immediately.

    use super::*;
    use crate::auth::{Role, UserStore};
    use crate::scram::ScramState;

    #[test]
    fn client_and_server_complete_a_full_exchange() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = UserStore::open(dir.path()).unwrap();
        store
            .create_user("alice", "wonderland", Role::Admin)
            .unwrap();

        let mut client = ScramClient::new("alice", "wonderland");
        let client_first_msg = client.client_first();

        let (server_first, scram_state) =
            ScramState::process_client_first(&client_first_msg, &store).expect("server-first");
        let (client_final_msg, expected_sig) =
            client.client_final(&server_first).expect("client-final");

        let (server_final, role) = scram_state
            .process_client_final(&client_final_msg, &store)
            .expect("server-final");
        assert_eq!(role, Role::Admin);

        verify_server_final(&server_final, &expected_sig).expect("server signature verifies");
    }

    #[test]
    fn wrong_password_makes_server_reject_proof() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = UserStore::open(dir.path()).unwrap();
        store
            .create_user("bob", "right-password", Role::Read)
            .unwrap();

        let mut client = ScramClient::new("bob", "WRONG-password");
        let client_first_msg = client.client_first();
        let (server_first, scram_state) =
            ScramState::process_client_first(&client_first_msg, &store).unwrap();
        let (client_final_msg, _expected_sig) = client.client_final(&server_first).unwrap();

        let err = match scram_state.process_client_final(&client_final_msg, &store) {
            Ok(_) => panic!("must fail"),
            Err(e) => e,
        };
        assert!(err.contains("authentication failed"), "got: {}", err);
    }

    #[test]
    fn nonce_mismatch_is_caught_client_side() {
        let mut client = ScramClient::new("user", "pw");
        client.client_first();
        // Server returns a combined nonce that does NOT start with
        // our client_nonce. Either a MITM or a buggy server; we
        // refuse without sending the proof.
        let server_first = "r=COMPLETELY-DIFFERENT,s=ZGVhZGJlZWY=,i=4096";
        let err = client
            .client_final(server_first)
            .expect_err("nonce mismatch must error");
        assert!(err.contains("does not extend client_nonce"), "got: {}", err);
    }

    #[test]
    fn verify_server_final_rejects_tampered_signature() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = UserStore::open(dir.path()).unwrap();
        store.create_user("carol", "pw", Role::Read).unwrap();

        let mut client = ScramClient::new("carol", "pw");
        let cf = client.client_first();
        let (server_first, scram_state) = ScramState::process_client_first(&cf, &store).unwrap();
        let (cf2, expected) = client.client_final(&server_first).unwrap();
        let (server_final, _) = scram_state.process_client_final(&cf2, &store).unwrap();

        // Tamper: change one byte of the expected signature. Real
        // server-final still parses but the proof check fails.
        let mut tampered = expected.clone();
        tampered[0] ^= 0xff;
        let err = verify_server_final(&server_final, &tampered).expect_err("tamper must fail");
        assert!(err.contains("server signature mismatch"), "got: {}", err);
    }
}
