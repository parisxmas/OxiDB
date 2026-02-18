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

    /// Reconnect using explicit host/port (called from Client variant).
    pub fn send_or_reconnect(
        stream: &mut TcpStream,
        host: &str,
        port: u16,
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
                Self::try_send(stream, request)
            }
        }
    }
}
