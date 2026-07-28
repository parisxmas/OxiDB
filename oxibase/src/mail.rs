//! Outbound email over SMTP submission with implicit TLS (RFC 8314, port 465)
//! — sized for the two transactional messages the control plane sends
//! (address verification, password reset), not for bulk mail.
//!
//! The deployment points this at its own OxiMail server, which handles DKIM
//! signing and onward delivery. Config (all required to enable email; absent →
//! email flows are disabled and endpoints report that):
//!
//!   OXIBASE_SMTP_HOST      e.g. mail.example.com
//!   OXIBASE_SMTP_PORT      default 465 (implicit TLS)
//!   OXIBASE_SMTP_USER      submission account (AUTH PLAIN)
//!   OXIBASE_SMTP_PASSWORD
//!   OXIBASE_MAIL_FROM      e.g. "OxiBase <noreply@example.com>" (default: user)
//!
//! For local development and tests there is a second transport:
//!
//!   OXIBASE_MAIL_SINK      append messages to this file instead of sending
//!
//! It takes precedence over the SMTP settings and enables the email flows with
//! no mail server, so a test can read back the link it just triggered. Never
//! set it in a deployment that sends real mail — every message, including
//! single-use sign-in links, lands in that file in the clear.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;

#[derive(Clone)]
pub struct Mailer {
    transport: Transport,
}

#[derive(Clone)]
enum Transport {
    Smtp(Smtp),
    /// Dev/test: one JSON object per message, appended to a file.
    File(std::path::PathBuf),
}

#[derive(Clone)]
struct Smtp {
    host: String,
    port: u16,
    user: String,
    password: String,
    from: String,
}

impl Mailer {
    /// Build from environment; `None` disables email flows.
    pub fn from_env() -> Option<Mailer> {
        if let Some(path) = std::env::var("OXIBASE_MAIL_SINK")
            .ok()
            .filter(|s| !s.is_empty())
        {
            eprintln!("[mail] SINK MODE — messages are written to {path}, not sent");
            return Some(Mailer {
                transport: Transport::File(path.into()),
            });
        }
        let host = std::env::var("OXIBASE_SMTP_HOST")
            .ok()
            .filter(|s| !s.is_empty())?;
        let user = std::env::var("OXIBASE_SMTP_USER")
            .ok()
            .filter(|s| !s.is_empty())?;
        let password = std::env::var("OXIBASE_SMTP_PASSWORD")
            .ok()
            .filter(|s| !s.is_empty())?;
        let port = std::env::var("OXIBASE_SMTP_PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(465);
        let from = std::env::var("OXIBASE_MAIL_FROM")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| user.clone());
        Some(Mailer {
            transport: Transport::Smtp(Smtp {
                host,
                port,
                user,
                password,
                from,
            }),
        })
    }

    /// Send a plain-text message. Blocking (a few round-trips to the local
    /// mail server); callers spawn a thread when latency matters.
    pub fn send(&self, to: &str, subject: &str, body: &str) -> Result<(), String> {
        // Recipient goes into SMTP verbatim — refuse anything that could
        // smuggle commands or extra recipients.
        if to.is_empty()
            || to
                .chars()
                .any(|c| c.is_control() || c == '<' || c == '>' || c == ',')
        {
            return Err("invalid recipient address".into());
        }
        match &self.transport {
            Transport::File(path) => Self::write_to_file(path, to, subject, body),
            Transport::Smtp(smtp) => smtp.send(to, subject, body),
        }
    }

    fn write_to_file(
        path: &std::path::Path,
        to: &str,
        subject: &str,
        body: &str,
    ) -> Result<(), String> {
        let line = serde_json::json!({ "to": to, "subject": subject, "body": body }).to_string();
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| format!("mail sink: {e}"))?;
        writeln!(f, "{line}").map_err(|e| format!("mail sink: {e}"))
    }

    /// `send` on a background thread — request handlers never wait on SMTP.
    pub fn send_async(&self, to: String, subject: String, body: String) {
        let mailer = self.clone();
        std::thread::spawn(move || {
            if let Err(e) = mailer.send(&to, &subject, &body) {
                eprintln!("[mail] send to {to} failed: {e}");
            }
        });
    }
}

impl Smtp {
    // Builds the message's From: header from this address — `from` is the header,
    // not a conversion.
    #[allow(clippy::wrong_self_convention)]
    /// The bare address inside `From:` (display names stripped).
    fn from_addr(&self) -> &str {
        match (self.from.find('<'), self.from.find('>')) {
            (Some(a), Some(b)) if b > a => &self.from[a + 1..b],
            _ => self.from.trim(),
        }
    }

    fn send(&self, to: &str, subject: &str, body: &str) -> Result<(), String> {
        let err = |stage: &str, e: String| format!("smtp {stage}: {e}");

        // TCP + implicit TLS.
        let tcp = TcpStream::connect((self.host.as_str(), self.port))
            .map_err(|e| err("connect", e.to_string()))?;
        tcp.set_read_timeout(Some(Duration::from_secs(15))).ok();
        tcp.set_write_timeout(Some(Duration::from_secs(15))).ok();
        let roots = rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        };
        // Explicit provider: with both `ring` (ours) and ureq's rustls features
        // in the dependency graph, rustls cannot auto-pick one and panics.
        let config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|e| err("tls-config", e.to_string()))?
        .with_root_certificates(roots)
        .with_no_client_auth();
        let server_name = rustls::pki_types::ServerName::try_from(self.host.clone())
            .map_err(|e| err("tls-name", e.to_string()))?;
        let conn = rustls::ClientConnection::new(Arc::new(config), server_name)
            .map_err(|e| err("tls", e.to_string()))?;
        let mut stream = rustls::StreamOwned::new(conn, tcp);

        let mut reader = BufReader::new(&mut stream);
        // One SMTP reply (all continuation lines); returns the status code.
        fn reply<R: BufRead>(r: &mut R) -> Result<u16, String> {
            let mut code;
            loop {
                let mut line = String::new();
                r.read_line(&mut line).map_err(|e| e.to_string())?;
                if line.len() < 4 {
                    return Err(format!("short reply: {line:?}"));
                }
                code = line[..3]
                    .parse()
                    .map_err(|_| format!("bad reply: {line:?}"))?;
                if line.as_bytes()[3] != b'-' {
                    return Ok(code);
                }
            }
            #[allow(unreachable_code)]
            Ok(code)
        }
        // rustls::StreamOwned can't be split; do the dialogue with a tiny
        // write-then-read helper over the same stream via interior reborrows.
        macro_rules! cmd {
            ($stage:expr, $expect:expr, $($arg:tt)*) => {{
                reader
                    .get_mut()
                    .write_all(format!($($arg)*).as_bytes())
                    .map_err(|e| err($stage, e.to_string()))?;
                let code = reply(&mut reader).map_err(|e| err($stage, e))?;
                if code != $expect {
                    return Err(err($stage, format!("unexpected status {code}")));
                }
            }};
        }

        let greeting = reply(&mut reader).map_err(|e| err("greeting", e))?;
        if greeting != 220 {
            return Err(err("greeting", format!("unexpected status {greeting}")));
        }
        cmd!("ehlo", 250, "EHLO oxibase\r\n");
        let auth = base64::engine::general_purpose::STANDARD
            .encode(format!("\0{}\0{}", self.user, self.password));
        cmd!("auth", 235, "AUTH PLAIN {auth}\r\n");
        cmd!("mail-from", 250, "MAIL FROM:<{}>\r\n", self.from_addr());
        cmd!("rcpt-to", 250, "RCPT TO:<{to}>\r\n");
        cmd!("data", 354, "DATA\r\n");

        // Dot-stuff body lines starting with '.' (RFC 5321 §4.5.2).
        let stuffed: String = body
            .lines()
            .map(|l| {
                if l.starts_with('.') {
                    format!(".{l}\r\n")
                } else {
                    format!("{l}\r\n")
                }
            })
            .collect();
        let msg_id: u64 = rand::random();
        let message = format!(
            "From: {}\r\nTo: {to}\r\nSubject: {subject}\r\nMessage-ID: <{msg_id:016x}@oxibase>\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n{stuffed}",
            self.from,
        );
        cmd!("body", 250, "{message}.\r\n");
        // Best-effort close.
        let _ = reader.get_mut().write_all(b"QUIT\r\n");
        Ok(())
    }
}
