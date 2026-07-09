use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;

use oxidb::OxiDb;
use serde_json::Value;

pub enum DbBackend {
    Embedded {
        db: Arc<OxiDb>,
        active_tx: Option<u64>,
        data_path: String,
    },
    Client {
        stream: TcpStream,
        host: String,
        port: u16,
        /// Credentials kept so a dropped socket can silently re-authenticate
        /// on reconnect. `None` user = anonymous connection.
        user: Option<String>,
        password: Option<String>,
    },
    Disconnected,
}

impl DbBackend {
    fn try_send(stream: &mut TcpStream, request: &Value) -> Result<Value, String> {
        let payload = request.to_string();
        let payload_bytes = payload.as_bytes();

        let len = (payload_bytes.len() as u32).to_le_bytes();
        stream
            .write_all(&len)
            .map_err(|e| format!("write error: {e}"))?;
        stream
            .write_all(payload_bytes)
            .map_err(|e| format!("write error: {e}"))?;
        stream
            .flush()
            .map_err(|e| format!("flush error: {e}"))?;

        let mut len_buf = [0u8; 4];
        stream
            .read_exact(&mut len_buf)
            .map_err(|e| format!("read error: {e}"))?;
        let resp_len = u32::from_le_bytes(len_buf) as usize;

        let mut buf = vec![0u8; resp_len];
        stream
            .read_exact(&mut buf)
            .map_err(|e| format!("read error: {e}"))?;

        serde_json::from_slice(&buf).map_err(|e| format!("invalid response JSON: {e}"))
    }

    /// Run one SCRAM-SHA-256 exchange on the stream, leaving it in an
    /// authenticated session on the server. No-op framing errors surface
    /// as the returned Err.
    pub fn authenticate(
        stream: &mut TcpStream,
        username: &str,
        password: &str,
    ) -> Result<(), String> {
        use crate::scram::{verify_server_final, ScramClient};
        let mut client = ScramClient::new(username, password);
        let first = client.client_first();
        let r1 = Self::try_send(stream, &serde_json::json!({
            "cmd": "authenticate",
            "payload": first,
        }))?;
        let server_first = Self::auth_payload(&r1, "authenticate")?;
        let (final_msg, expected) = client.client_final(&server_first)?;
        let r2 = Self::try_send(stream, &serde_json::json!({
            "cmd": "authenticate_continue",
            "payload": final_msg,
        }))?;
        let server_final = Self::auth_payload(&r2, "authenticate_continue")?;
        verify_server_final(&server_final, &expected)
    }

    fn auth_payload(resp: &Value, step: &str) -> Result<String, String> {
        if resp.get("ok").and_then(|v| v.as_bool()) == Some(false) {
            let msg = resp.get("error").and_then(|v| v.as_str()).unwrap_or("rejected");
            return Err(format!("{step} rejected: {msg}"));
        }
        resp.get("data")
            .and_then(|d| d.get("payload"))
            .and_then(|p| p.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| format!("{step}: response missing data.payload"))
    }

    /// Reconnect using explicit host/port (called from Client variant),
    /// re-authenticating with stored credentials if the connection is
    /// user-bound.
    pub fn send_or_reconnect(
        stream: &mut TcpStream,
        host: &str,
        port: u16,
        user: Option<&str>,
        password: Option<&str>,
        request: &Value,
    ) -> Result<Value, String> {
        match Self::try_send(stream, request) {
            Ok(v) => Ok(v),
            Err(_) => {
                let new_stream = TcpStream::connect((host, port))
                    .map_err(|e| format!("reconnect failed: {e}"))?;
                new_stream
                    .set_read_timeout(Some(std::time::Duration::from_secs(30)))
                    .ok();
                *stream = new_stream;
                if let (Some(u), Some(p)) = (user, password) {
                    Self::authenticate(stream, u, p)?;
                }
                Self::try_send(stream, request)
            }
        }
    }
}

#[cfg(test)]
mod auth_smoke {
    //! Live end-to-end SCRAM smoke test. Ignored by default; run against a
    //! server started with OXIDB_AUTH=1 by passing address + credentials:
    //!   OXIDB_TEST_ADDR=127.0.0.1:4488 OXIDB_TEST_USER=admin \
    //!   OXIDB_TEST_PASS=<pw> cargo test --lib auth_smoke -- --ignored --nocapture
    use super::*;
    use std::net::TcpStream;

    #[test]
    #[ignore]
    fn scram_handshake_then_authed_ping() {
        let addr = std::env::var("OXIDB_TEST_ADDR").unwrap();
        let user = std::env::var("OXIDB_TEST_USER").unwrap();
        let pass = std::env::var("OXIDB_TEST_PASS").unwrap();
        let mut s = TcpStream::connect(&addr).unwrap();
        s.set_read_timeout(Some(std::time::Duration::from_secs(10))).ok();
        DbBackend::authenticate(&mut s, &user, &pass).expect("SCRAM handshake");
        // A command that requires auth should now succeed on this session.
        let resp = DbBackend::try_send(&mut s, &serde_json::json!({"cmd": "ping"})).unwrap();
        assert_eq!(resp.get("ok").and_then(|v| v.as_bool()), Some(true), "resp={resp}");
        // Wrong password must be rejected.
        let mut s2 = TcpStream::connect(&addr).unwrap();
        assert!(DbBackend::authenticate(&mut s2, &user, "definitely-wrong").is_err());
    }
}
