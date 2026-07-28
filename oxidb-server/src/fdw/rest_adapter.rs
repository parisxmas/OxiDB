//! REST/HTTP foreign-collection adapter. Treats an HTTP resource
//! collection as a flat document collection following the most
//! common REST convention:
//!
//!   GET    /resource          → list (array of docs)
//!   GET    /resource/<id>     → one doc by id
//!   POST   /resource          → create one doc
//!   PATCH  /resource/<id>     → update one doc (partial)
//!   DELETE /resource/<id>     → delete one doc
//!
//! Mapping to OxiDB CRUD:
//!   find / find_one with empty query / `{}`  → GET /resource (then filter
//!                                                client-side if needed)
//!   find_one with `{<id_field>: X}`          → GET /resource/X
//!   count                                    → GET /resource then count
//!   insert (doc)                             → POST /resource body=doc
//!   update_one with `{<id_field>: X}`        → PATCH /resource/X
//!                              body = update.$set (other ops rejected)
//!   delete_one with `{<id_field>: X}`        → DELETE /resource/X
//!
//! Scope:
//!   - HTTP/1.1 over plain TCP (`http://`) or TLS (`https://`, via
//!     rustls + the Mozilla CA bundle from webpki-roots).
//!   - No chunked transfer-encoding; servers must send Content-Length.
//!   - No redirects, no compression, no HTTP/2.
//!   - id field defaults to `"id"`. Override via URL fragment:
//!     `http://api.example.com/users#id_field=user_id`.
//!
//! TLS uses the system's process-wide rustls crypto provider. We don't
//! validate hostnames against a custom set; standard webpki-roots is
//! the source of truth, which matches what browsers + every Rust HTTP
//! client does. Pinning / custom roots is out of scope here — a
//! follow-up could add it if some operator needs it.

use serde_json::{Value, json};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use crate::fdw::Adapter;

/// Default field name a REST resource uses for its primary key.
/// Overridable per-link via the URL fragment `#id_field=<name>`.
const DEFAULT_ID_FIELD: &str = "id";

/// Hard cap on response body size — prevents a misbehaving remote
/// from exhausting memory by sending an unbounded stream. 16 MiB
/// covers any realistic single-collection JSON response.
const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

/// Connect + read + write timeout. Matches `remote_client::PROXY_TIMEOUT`
/// — a REST link to a sluggish API should fail fast, same as a wedged
/// OxiDB peer.
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);

/// Scheme: http (plain TCP) or https (TLS via rustls + webpki-roots).
/// Stored on the adapter so http_request can decide whether to do a
/// TLS handshake after the TCP connect.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Scheme {
    Http,
    Https,
}

impl Scheme {
    fn default_port(self) -> u16 {
        match self {
            Scheme::Http => 80,
            Scheme::Https => 443,
        }
    }
}

/// RestAdapter owns a parsed http(s):// endpoint and emits HTTP/1.1
/// requests on demand. No persistent connection — every request opens
/// a fresh TCP socket (vs OxiDB's pooled conns) because most public
/// REST APIs ignore Connection: keep-alive anyway and the per-request
/// dial cost is acceptable for a generic adapter. HTTPS pays for a
/// TLS handshake per request; if that ever shows up as a bottleneck
/// we can add a per-(scheme, host, port) connection pool the same way
/// the OxiDB adapter does.
pub struct RestAdapter {
    scheme: Scheme,
    host: String,
    port: u16,
    /// Path on the remote, with leading slash. e.g. "/users".
    base_path: String,
    /// Configurable primary-key field name; defaults to "id".
    id_field: String,
    /// Original URL kept around for error messages.
    url: String,
    /// Per-adapter rustls config override. Production code never sets
    /// this; the global webpki-roots config is used. Integration
    /// tests inject a config with a test-only root so a self-signed
    /// localhost cert verifies. `Arc` because rustls holds the
    /// config across the connection's lifetime.
    #[cfg(test)]
    tls_config_override: Option<Arc<rustls::ClientConfig>>,
}

impl RestAdapter {
    pub fn from_url(url: &str) -> Result<Self, String> {
        let (scheme, rest) = if let Some(r) = url.strip_prefix("https://") {
            (Scheme::Https, r)
        } else if let Some(r) = url.strip_prefix("http://") {
            (Scheme::Http, r)
        } else {
            return Err(format!(
                "REST link URL must start with http:// or https:// — got {:?}",
                url
            ));
        };

        // Split off the optional URL fragment first (#id_field=...).
        let (rest, fragment) = match rest.find('#') {
            Some(i) => (&rest[..i], Some(&rest[i + 1..])),
            None => (rest, None),
        };

        let (hostport, path) = match rest.find('/') {
            Some(i) => (&rest[..i], &rest[i..]),
            None => (rest, "/"),
        };
        if path.is_empty() || path == "/" {
            return Err(format!(
                "REST link URL must include a resource path — got {:?}",
                url
            ));
        }

        let (host, port) = match hostport.rsplit_once(':') {
            Some((h, p)) => {
                let port: u16 = p
                    .parse()
                    .map_err(|_| format!("REST link URL port must be a number — got {:?}", p))?;
                (h.to_string(), port)
            }
            None => (hostport.to_string(), scheme.default_port()),
        };
        if host.is_empty() {
            return Err("REST link URL host is empty".to_string());
        }

        let mut id_field = DEFAULT_ID_FIELD.to_string();
        if let Some(frag) = fragment {
            for pair in frag.split('&') {
                if let Some(v) = pair.strip_prefix("id_field=")
                    && !v.is_empty()
                {
                    id_field = v.to_string();
                }
            }
        }

        Ok(Self {
            scheme,
            host,
            port,
            base_path: path.to_string(),
            id_field,
            url: url.to_string(),
            #[cfg(test)]
            tls_config_override: None,
        })
    }

    /// id_from_query pulls the primary-key value from a query like
    /// `{<id_field>: X}`. Returns Ok(Some(id_as_path_segment)) when
    /// the query selects exactly that one field, Ok(None) when the
    /// query is empty (caller should issue a list request), or Err
    /// when the query uses unsupported fields / operators.
    fn id_from_query(&self, query: &Value) -> Result<Option<String>, String> {
        let obj = match query.as_object() {
            Some(o) => o,
            None => return Ok(None),
        };
        if obj.is_empty() {
            return Ok(None);
        }
        if obj.len() != 1 {
            return Err(format!(
                "REST FDW supports only single-field equality on the id field {:?} \
                — got query with {} fields",
                self.id_field,
                obj.len()
            ));
        }
        let (k, v) = obj.iter().next().unwrap();
        if k != &self.id_field {
            return Err(format!(
                "REST FDW: query field {:?} must be the configured id field {:?} \
                (override via URL fragment #id_field=<name>)",
                k, self.id_field
            ));
        }
        // Stringify the value for the URL path segment. Plain string
        // values land as-is; numbers / bools get their JSON form
        // (no quotes). That matches how most REST APIs spell IDs.
        let segment = match v {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        Ok(Some(segment))
    }
}

impl Adapter for RestAdapter {
    fn execute(&self, cmd: &str, request: &Value) -> Result<Value, String> {
        let query = request.get("query").cloned().unwrap_or_else(|| json!({}));

        match cmd {
            "find" => {
                // GET /resource (with optional id) → array. If the query
                // names an id we still issue the list endpoint and
                // client-filter, because GET /resource/<id> returns
                // a single object, not an array — find always returns
                // an array.
                let body = self.http_get(&self.base_path)?;
                let parsed = parse_json_body(&body)?;
                let arr = into_array(parsed);
                let filtered = filter_array(&arr, &query);
                Ok(ok_envelope(Value::Array(filtered)))
            }
            "find_one" => {
                // Empty query → first item from the list.
                // {id: X} → direct GET /resource/<id>.
                match self.id_from_query(&query)? {
                    None => {
                        let body = self.http_get(&self.base_path)?;
                        let parsed = parse_json_body(&body)?;
                        let arr = into_array(parsed);
                        let first = arr.into_iter().next().unwrap_or(Value::Null);
                        Ok(ok_envelope(first))
                    }
                    Some(id) => {
                        let path = format!("{}/{}", self.base_path, id);
                        let body = self.http_get(&path)?;
                        let parsed = parse_json_body(&body)?;
                        Ok(ok_envelope(parsed))
                    }
                }
            }
            "count" => {
                let body = self.http_get(&self.base_path)?;
                let parsed = parse_json_body(&body)?;
                let arr = into_array(parsed);
                let n = filter_array(&arr, &query).len();
                Ok(ok_envelope(json!({ "count": n })))
            }
            "insert" => {
                let doc = request.get("doc").ok_or("missing 'doc'")?.clone();
                let body = serde_json::to_vec(&doc).map_err(|e| format!("encode doc: {}", e))?;
                let resp = self.http_request("POST", &self.base_path, Some(&body))?;
                let parsed = parse_json_body(&resp)?;
                // Echo back what the server returned in the standard
                // `{"ok": true, "data": ...}` envelope; many APIs
                // return the created resource (including the new id),
                // which is exactly what callers want.
                Ok(ok_envelope(parsed))
            }
            "update_one" => {
                let id = self
                    .id_from_query(&query)?
                    .ok_or("REST FDW update_one requires a query selecting by the id field")?;
                let update = request.get("update").ok_or("missing 'update'")?;
                let set = update
                    .get("$set")
                    .ok_or("REST FDW supports only the $set update operator")?;
                let body = serde_json::to_vec(set).map_err(|e| format!("encode $set: {}", e))?;
                let path = format!("{}/{}", self.base_path, id);
                let resp = self.http_request("PATCH", &path, Some(&body))?;
                let parsed = parse_json_body(&resp).unwrap_or(Value::Null);
                Ok(ok_envelope(json!({ "modified": 1, "doc": parsed })))
            }
            "delete_one" => {
                let id = self
                    .id_from_query(&query)?
                    .ok_or("REST FDW delete_one requires a query selecting by the id field")?;
                let path = format!("{}/{}", self.base_path, id);
                // Many APIs return empty body on DELETE — that's fine,
                // we don't try to parse the response.
                self.http_request("DELETE", &path, None)?;
                Ok(ok_envelope(json!({ "deleted": 1 })))
            }
            other => Err(format!(
                "REST FDW adapter does not implement command {:?} (link {})",
                other, self.url
            )),
        }
    }
}

impl RestAdapter {
    /// http_get is the verb-specific convenience wrapper most call
    /// sites want. Equivalent to http_request("GET", path, None).
    fn http_get(&self, path: &str) -> Result<Vec<u8>, String> {
        self.http_request("GET", path, None)
    }

    /// http_request issues one HTTP/1.1 request — over plain TCP for
    /// http:// links or over a rustls TLS session for https:// links
    /// — and returns the response body as bytes. Connection is closed
    /// after the request via `Connection: close`: no keep-alive, no
    /// pooling, fresh socket per call. That's pessimistic but safe;
    /// REST endpoints rarely care.
    ///
    /// Returns Err on any of:
    ///   - TCP connect / TLS handshake / timeout failure
    ///   - response not parseable as HTTP/1.x
    ///   - 4xx / 5xx response (the message includes the status line)
    ///   - body exceeds MAX_RESPONSE_BYTES
    fn http_request(
        &self,
        method: &str,
        path: &str,
        body: Option<&[u8]>,
    ) -> Result<Vec<u8>, String> {
        let addr = format!("{}:{}", self.host, self.port);
        let tcp = TcpStream::connect_timeout(&addr.to_socket_addrs_first()?, HTTP_TIMEOUT)
            .map_err(|e| format!("connect {}: {}", addr, e))?;
        tcp.set_read_timeout(Some(HTTP_TIMEOUT))
            .map_err(|e| format!("set read timeout: {}", e))?;
        tcp.set_write_timeout(Some(HTTP_TIMEOUT))
            .map_err(|e| format!("set write timeout: {}", e))?;

        // Wrap TCP in TLS if the link URL was https://. The trait
        // object lets the wire code below stay scheme-agnostic.
        let mut conn: Box<dyn ReadWrite> = match self.scheme {
            Scheme::Http => Box::new(tcp),
            Scheme::Https => {
                #[cfg(test)]
                let cfg = self.tls_config_override.clone().unwrap_or_else(tls_config);
                #[cfg(not(test))]
                let cfg = tls_config();
                Box::new(open_tls_stream(&self.host, cfg, tcp)?)
            }
        };

        do_http_round_trip(&mut *conn, &self.host, method, path, body)
    }
}

/// do_http_round_trip writes one HTTP/1.1 request and reads the
/// response off the given connection (which may be plain TCP or TLS).
/// Pulled out of http_request so the same wire-format code runs for
/// both schemes — the only difference is what the underlying stream
/// does with the bytes.
fn do_http_round_trip(
    conn: &mut dyn ReadWrite,
    host: &str,
    method: &str,
    path: &str,
    body: Option<&[u8]>,
) -> Result<Vec<u8>, String> {
    let mut req = format!(
        "{method} {path} HTTP/1.1\r\n\
         Host: {host}\r\n\
         User-Agent: oxidb-fdw/3b\r\n\
         Accept: application/json\r\n\
         Connection: close\r\n"
    );
    if let Some(b) = body {
        req.push_str(&format!(
            "Content-Type: application/json\r\nContent-Length: {}\r\n\r\n",
            b.len()
        ));
    } else {
        req.push_str("\r\n");
    }
    conn.write_all(req.as_bytes())
        .map_err(|e| format!("write request line/headers: {}", e))?;
    if let Some(b) = body {
        conn.write_all(b)
            .map_err(|e| format!("write request body: {}", e))?;
    }

    let mut raw = Vec::with_capacity(4096);
    let mut buf = [0u8; 4096];
    loop {
        match conn.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                if raw.len() + n > MAX_RESPONSE_BYTES {
                    return Err(format!(
                        "REST response exceeds {} bytes — aborting to avoid OOM",
                        MAX_RESPONSE_BYTES
                    ));
                }
                raw.extend_from_slice(&buf[..n]);
            }
            // rustls returns UnexpectedEof when the TLS peer closes
            // the TCP socket without sending a close_notify alert.
            // In HTTP/1.1 with Connection: close, that's the normal
            // termination path — close_notify is theoretically
            // required but rarely sent in practice. Treat it as
            // graceful EOF so HTTPS to any real-world server works.
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof && !raw.is_empty() => break,
            Err(e) => return Err(format!("read response: {}", e)),
        }
    }

    parse_http_response(&raw)
}

/// ReadWrite is the trait-object surface http_request needs — Read +
/// Write both. Rust doesn't let us write `dyn Read + Write` directly
/// because each is its own trait; the blanket impl below lets any
/// type that satisfies both (TcpStream, rustls::StreamOwned, …) be
/// boxed as `Box<dyn ReadWrite>`.
trait ReadWrite: Read + Write {}
impl<T: Read + Write + ?Sized> ReadWrite for T {}

/// open_tls_stream performs the rustls handshake against `host` on an
/// already-connected TCP socket and returns the wrapped stream. The
/// returned stream owns the rustls ClientConnection alongside the
/// TcpStream so the caller can just read/write into it normally.
fn open_tls_stream(
    host: &str,
    cfg: Arc<rustls::ClientConfig>,
    tcp: TcpStream,
) -> Result<rustls::StreamOwned<rustls::ClientConnection, TcpStream>, String> {
    let server_name = rustls::pki_types::ServerName::try_from(host.to_string())
        .map_err(|e| format!("invalid TLS server name {:?}: {}", host, e))?;
    let conn = rustls::ClientConnection::new(cfg, server_name)
        .map_err(|e| format!("rustls client init: {}", e))?;
    Ok(rustls::StreamOwned::new(conn, tcp))
}

/// tls_config builds the rustls ClientConfig used by every HTTPS
/// request. Cached for the lifetime of the process because building
/// it is non-trivial (parses ~150 root certs) and the result is
/// stateless / shareable across threads.
fn tls_config() -> Arc<rustls::ClientConfig> {
    static CONFIG: OnceLock<Arc<rustls::ClientConfig>> = OnceLock::new();
    CONFIG
        .get_or_init(|| {
            // Install the default rustls crypto provider if no one
            // else has done it yet. Idempotent across crates — only
            // the first install in the process wins, and we don't
            // care which one as long as one exists. We use aws-lc-rs
            // because that's the provider rustls already pulls into
            // the workspace (FIPS-friendlier than ring, same surface).
            let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

            let mut roots = rustls::RootCertStore::empty();
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            let config = rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth();
            Arc::new(config)
        })
        .clone()
}

/// parse_http_response splits headers from body and returns the body
/// bytes on a 2xx status. Non-2xx surfaces as Err with the status
/// line + a snippet of the body for debugging.
fn parse_http_response(raw: &[u8]) -> Result<Vec<u8>, String> {
    let sep = b"\r\n\r\n";
    let split = raw
        .windows(4)
        .position(|w| w == sep)
        .ok_or("response missing header/body separator (no \\r\\n\\r\\n)")?;
    let head = std::str::from_utf8(&raw[..split])
        .map_err(|e| format!("response head not UTF-8: {}", e))?;
    let body = &raw[split + 4..];

    let status_line = head.lines().next().ok_or("response has no status line")?;
    let mut parts = status_line.splitn(3, ' ');
    let _version = parts.next();
    let code: u16 = parts
        .next()
        .ok_or("status line missing code")?
        .parse()
        .map_err(|e| format!("status code not a number: {}", e))?;
    if !(200..300).contains(&code) {
        let snippet_len = body.len().min(256);
        let snippet = String::from_utf8_lossy(&body[..snippet_len]);
        return Err(format!(
            "REST remote returned non-2xx status: {} ({}); body snippet: {}",
            status_line, code, snippet
        ));
    }
    Ok(body.to_vec())
}

/// parse_json_body decodes the response body as JSON; surfaces a
/// snippet of what arrived if the parse fails so an operator can see
/// "ah, the API actually returned HTML" without grep'ing logs.
fn parse_json_body(body: &[u8]) -> Result<Value, String> {
    if body.is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_slice::<Value>(body).map_err(|e| {
        let snippet_len = body.len().min(200);
        let snippet = String::from_utf8_lossy(&body[..snippet_len]);
        format!("REST response not JSON: {} — body snippet: {}", e, snippet)
    })
}

/// into_array normalises the parsed body to a `Vec<Value>`. Some
/// endpoints return a bare array (`[{...}]`), others wrap it
/// (`{"data": [...]}` or `{"items": [...]}`). We try the obvious
/// shapes before giving up and treating the value as a single doc.
fn into_array(v: Value) -> Vec<Value> {
    match v {
        Value::Array(a) => a,
        Value::Object(mut o) => {
            for key in ["data", "items", "results"] {
                if let Some(Value::Array(a)) = o.remove(key) {
                    return a;
                }
            }
            // Single-object response (e.g. /resource/<id>) — treat as
            // a one-element collection so find() callers don't have
            // to special-case.
            vec![Value::Object(o)]
        }
        Value::Null => Vec::new(),
        other => vec![other],
    }
}

/// filter_array runs the same simple-equality predicate as the CSV
/// adapter. The REST API may have already filtered server-side (we
/// always issue GET /resource without query params in v3b), so this
/// is the post-fetch narrowing layer. Future PRs can add query-param
/// pass-through to push predicates down to the remote.
fn filter_array(rows: &[Value], query: &Value) -> Vec<Value> {
    let pred = match query.as_object() {
        Some(o) if !o.is_empty() => o,
        _ => return rows.to_vec(),
    };
    rows.iter()
        .filter(|row| {
            let obj = match row.as_object() {
                Some(o) => o,
                None => return false,
            };
            pred.iter().all(|(k, expected)| {
                let actual = obj.get(k).unwrap_or(&Value::Null);
                actual == expected
            })
        })
        .cloned()
        .collect()
}

fn ok_envelope(data: Value) -> Value {
    json!({ "ok": true, "data": data })
}

/// Tiny trait so we don't need to import std::net::ToSocketAddrs at
/// the call site. Same pattern as remote_client::resolve.
trait ToSocketAddrsFirst {
    fn to_socket_addrs_first(&self) -> Result<std::net::SocketAddr, String>;
}
impl ToSocketAddrsFirst for String {
    fn to_socket_addrs_first(&self) -> Result<std::net::SocketAddr, String> {
        use std::net::ToSocketAddrs;
        self.to_socket_addrs()
            .map_err(|e| format!("resolve {}: {}", self, e))?
            .next()
            .ok_or_else(|| format!("resolve {}: no addresses returned", self))
    }
}

#[cfg(test)]
mod tests {
    //! Unit tests against an in-process mock HTTP server. We can't
    //! talk to a real public API in CI; the mock implements just
    //! enough HTTP/1.1 to roundtrip our adapter against it.

    use super::*;
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};

    /// MockHttpServer remembers every request it served plus a
    /// scripted response per (method, path). Lives for the duration
    /// of one test; killed when dropped.
    struct MockHttpServer {
        port: u16,
        // The adapter's parsed response shape, used only here.
        #[allow(clippy::type_complexity)]
        requests: Arc<Mutex<Vec<(String, String, Vec<u8>)>>>,
    }

    impl MockHttpServer {
        fn start(responses: Vec<(&'static str, &'static str, u16, &'static str)>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let port = listener.local_addr().unwrap().port();
            let requests = Arc::new(Mutex::new(Vec::new()));
            let log = Arc::clone(&requests);

            std::thread::spawn(move || {
                for stream in listener.incoming().flatten() {
                    let log = Arc::clone(&log);
                    let resps = responses.clone();
                    std::thread::spawn(move || {
                        Self::handle(stream, log, resps);
                    });
                }
            });

            Self { port, requests }
        }

        // `log` records (method, path, body) per request; the tuple is used only
        // in this test adapter.
        #[allow(clippy::type_complexity)]
        fn handle(
            mut stream: TcpStream,
            log: Arc<Mutex<Vec<(String, String, Vec<u8>)>>>,
            responses: Vec<(&'static str, &'static str, u16, &'static str)>,
        ) {
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut buf = vec![0u8; 8192];
            let mut total = Vec::new();
            // Read until we see the full headers + body. A real HTTP
            // server would parse Content-Length and read exactly that
            // many bytes — we cheat: do one read, parse headers,
            // then read the declared content-length.
            loop {
                let n = stream.read(&mut buf).unwrap_or(0);
                if n == 0 {
                    break;
                }
                total.extend_from_slice(&buf[..n]);
                if let Some(idx) = total.windows(4).position(|w| w == b"\r\n\r\n") {
                    let head = std::str::from_utf8(&total[..idx]).unwrap();
                    let cl = head
                        .lines()
                        .find_map(|l| {
                            let l = l.trim_end();
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                        })
                        .unwrap_or(0);
                    if total.len() >= idx + 4 + cl {
                        break;
                    }
                }
            }

            let head_end = total
                .windows(4)
                .position(|w| w == b"\r\n\r\n")
                .unwrap_or(total.len());
            let head_str = std::str::from_utf8(&total[..head_end]).unwrap();
            let body = if head_end + 4 <= total.len() {
                total[head_end + 4..].to_vec()
            } else {
                Vec::new()
            };
            let request_line = head_str.lines().next().unwrap_or("");
            let mut parts = request_line.split(' ');
            let method = parts.next().unwrap_or("").to_string();
            let path = parts.next().unwrap_or("").to_string();

            log.lock()
                .unwrap()
                .push((method.clone(), path.clone(), body));

            let (status, payload) = responses
                .iter()
                .find(|(m, p, _, _)| *m == method && *p == path)
                .map(|(_, _, s, p)| (*s, *p))
                .unwrap_or((404, ""));

            let body_bytes = payload.as_bytes();
            let resp = format!(
                "HTTP/1.1 {} OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                status,
                body_bytes.len()
            );
            stream.write_all(resp.as_bytes()).unwrap();
            stream.write_all(body_bytes).unwrap();
        }
    }

    fn adapter(port: u16, path: &str) -> RestAdapter {
        RestAdapter::from_url(&format!("http://127.0.0.1:{}{}", port, path)).unwrap()
    }

    // RestAdapter isn't Debug so we hand-roll error-path assertions
    // (same pattern as fdw::tests::expect_err).
    fn expect_url_err(url: &str) -> String {
        match RestAdapter::from_url(url) {
            Ok(_) => panic!("RestAdapter::from_url({:?}) must reject this URL", url),
            Err(e) => e,
        }
    }

    #[test]
    fn https_url_parses_with_default_port_443() {
        // HTTPS was rejected in the original v3b; that restriction is
        // gone. URL parsing must accept https://, default the port to
        // 443 when one isn't given, and store scheme=Https so the
        // request path takes the TLS branch.
        let a = RestAdapter::from_url("https://api.example.com/users").unwrap();
        assert_eq!(a.scheme, Scheme::Https);
        assert_eq!(a.port, 443);
        assert_eq!(a.host, "api.example.com");

        // Explicit port overrides the default.
        let a = RestAdapter::from_url("https://api.example.com:8443/users").unwrap();
        assert_eq!(a.port, 8443);
    }

    #[test]
    fn http_url_still_picks_port_80_by_default() {
        // Regression-guard: the scheme refactor didn't break the
        // http:// default-port path.
        let a = RestAdapter::from_url("http://api.example.com/users").unwrap();
        assert_eq!(a.scheme, Scheme::Http);
        assert_eq!(a.port, 80);
    }

    #[test]
    fn unknown_scheme_is_rejected() {
        // Anything that's not http:// or https:// must error — the
        // dispatcher routes by prefix, but RestAdapter::from_url is
        // also called directly in some places (tests, future code)
        // so its own gate matters.
        let err = expect_url_err("ftp://api.example.com/users");
        assert!(err.contains("http://") && err.contains("https://"), "{err}");
    }

    #[test]
    fn missing_resource_path_is_rejected() {
        let err = expect_url_err("http://api.example.com");
        assert!(err.contains("resource path"));
    }

    #[test]
    fn find_returns_array_from_endpoint() {
        let srv = MockHttpServer::start(vec![(
            "GET",
            "/users",
            200,
            r#"[{"id":1,"name":"alice"},{"id":2,"name":"bob"}]"#,
        )]);
        let a = adapter(srv.port, "/users");
        let resp = a.execute("find", &json!({"query": {}})).unwrap();
        let rows = resp["data"].as_array().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1]["name"], "bob");
    }

    #[test]
    fn find_filters_array_client_side_with_equality_predicate() {
        let srv = MockHttpServer::start(vec![(
            "GET",
            "/users",
            200,
            r#"[{"id":1,"role":"admin"},{"id":2,"role":"user"}]"#,
        )]);
        let a = adapter(srv.port, "/users");
        let resp = a
            .execute("find", &json!({"query": {"role": "admin"}}))
            .unwrap();
        let rows = resp["data"].as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["id"], 1);
    }

    #[test]
    fn find_one_with_id_query_uses_path_segment() {
        let srv = MockHttpServer::start(vec![("GET", "/users/42", 200, r#"{"id":42,"name":"x"}"#)]);
        let a = adapter(srv.port, "/users");
        let resp = a
            .execute("find_one", &json!({"query": {"id": 42}}))
            .unwrap();
        assert_eq!(resp["data"]["name"], "x");
        // Confirm we hit /users/42, not /users.
        let log = srv.requests.lock().unwrap();
        assert_eq!(log[0].0, "GET");
        assert_eq!(log[0].1, "/users/42");
    }

    #[test]
    fn count_uses_filtered_array_length() {
        let srv = MockHttpServer::start(vec![(
            "GET",
            "/things",
            200,
            r#"[{"x":1},{"x":1},{"x":2}]"#,
        )]);
        let a = adapter(srv.port, "/things");
        let resp = a.execute("count", &json!({"query": {"x": 1}})).unwrap();
        assert_eq!(resp["data"]["count"], 2);
    }

    #[test]
    fn unwraps_data_envelope_from_apis_that_wrap() {
        // Many REST APIs return `{"data": [...]}` instead of a bare
        // array. The adapter must accept that shape transparently.
        let srv = MockHttpServer::start(vec![(
            "GET",
            "/items",
            200,
            r#"{"data":[{"x":1},{"x":2}]}"#,
        )]);
        let a = adapter(srv.port, "/items");
        let resp = a.execute("find", &json!({"query": {}})).unwrap();
        assert_eq!(resp["data"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn insert_posts_doc_and_returns_server_payload() {
        let srv = MockHttpServer::start(vec![("POST", "/users", 201, r#"{"id":99,"name":"new"}"#)]);
        let a = adapter(srv.port, "/users");
        let resp = a
            .execute("insert", &json!({"doc": {"name": "new"}}))
            .unwrap();
        assert_eq!(resp["data"]["id"], 99);

        let log = srv.requests.lock().unwrap();
        assert_eq!(log[0].0, "POST");
        assert_eq!(log[0].1, "/users");
        // Body was the JSON-encoded doc.
        let sent: Value = serde_json::from_slice(&log[0].2).unwrap();
        assert_eq!(sent["name"], "new");
    }

    #[test]
    fn update_one_patches_by_id_and_sends_only_set_body() {
        let srv = MockHttpServer::start(vec![(
            "PATCH",
            "/users/7",
            200,
            r#"{"id":7,"name":"updated"}"#,
        )]);
        let a = adapter(srv.port, "/users");
        let resp = a
            .execute(
                "update_one",
                &json!({
                    "query": {"id": 7},
                    "update": {"$set": {"name": "updated"}},
                }),
            )
            .unwrap();
        assert_eq!(resp["data"]["modified"], 1);

        let log = srv.requests.lock().unwrap();
        assert_eq!(log[0].0, "PATCH");
        assert_eq!(log[0].1, "/users/7");
        let sent: Value = serde_json::from_slice(&log[0].2).unwrap();
        // ONLY the $set contents go over the wire — not the wrapper.
        assert_eq!(sent, json!({"name": "updated"}));
    }

    #[test]
    fn update_one_without_id_query_is_rejected() {
        let srv = MockHttpServer::start(vec![]);
        let a = adapter(srv.port, "/users");
        let err = a
            .execute(
                "update_one",
                &json!({
                    "query": {},
                    "update": {"$set": {"name": "x"}},
                }),
            )
            .unwrap_err();
        assert!(err.contains("id field"));
    }

    #[test]
    fn update_one_without_set_operator_is_rejected() {
        let srv = MockHttpServer::start(vec![]);
        let a = adapter(srv.port, "/users");
        let err = a
            .execute(
                "update_one",
                &json!({
                    "query": {"id": 1},
                    "update": {"$inc": {"x": 1}},
                }),
            )
            .unwrap_err();
        assert!(err.contains("$set"));
    }

    #[test]
    fn delete_one_deletes_by_id() {
        let srv = MockHttpServer::start(vec![("DELETE", "/items/abc", 204, "")]);
        let a = adapter(srv.port, "/items");
        let resp = a
            .execute("delete_one", &json!({"query": {"id": "abc"}}))
            .unwrap();
        assert_eq!(resp["data"]["deleted"], 1);
    }

    #[test]
    fn id_field_override_via_url_fragment() {
        let srv = MockHttpServer::start(vec![("GET", "/items/sku-9", 200, r#"{"sku":"sku-9"}"#)]);
        let a = RestAdapter::from_url(&format!("http://127.0.0.1:{}/items#id_field=sku", srv.port))
            .unwrap();
        let resp = a
            .execute("find_one", &json!({"query": {"sku": "sku-9"}}))
            .unwrap();
        assert_eq!(resp["data"]["sku"], "sku-9");
    }

    #[test]
    fn non_2xx_response_surfaces_as_error_with_status() {
        let srv = MockHttpServer::start(vec![("GET", "/oops", 500, r#"{"error":"boom"}"#)]);
        let a = adapter(srv.port, "/oops");
        let err = a.execute("find", &json!({"query": {}})).unwrap_err();
        assert!(err.contains("500"));
        assert!(err.contains("boom"), "body snippet must surface: {}", err);
    }

    #[test]
    fn non_json_response_surfaces_with_body_snippet() {
        let srv = MockHttpServer::start(vec![(
            "GET",
            "/html",
            200,
            "<html><body>not json</body></html>",
        )]);
        let a = adapter(srv.port, "/html");
        let err = a.execute("find", &json!({"query": {}})).unwrap_err();
        assert!(err.contains("not JSON"));
        assert!(err.contains("<html>"), "{}", err);
    }

    // -------------------------------------------------------------------
    // HTTPS — end-to-end against an in-process rustls server using a
    // self-signed cert generated with rcgen at test time. The adapter
    // gets a tls_config_override that trusts ONLY the test cert, so
    // the handshake exercises real cert validation without depending
    // on the public webpki-roots store.
    // -------------------------------------------------------------------

    /// Spin up a one-shot HTTPS server. Returns (port, root_cert_der)
    /// so the test can build a client config that trusts this cert.
    /// One scripted (method, path) → (status, body) response per
    /// request; the connection is closed after each.
    fn start_tls_mock(
        responses: Vec<(&'static str, &'static str, u16, &'static str)>,
    ) -> (u16, rustls::pki_types::CertificateDer<'static>) {
        use rustls::pki_types::PrivatePkcs8KeyDer;
        use std::net::TcpListener;

        // rustls needs SOME crypto provider installed before any
        // server config can be built. Same install used by the
        // production tls_config(); idempotent across tests.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        // Generate a self-signed cert covering both "localhost" and
        // 127.0.0.1 — different systems resolve "localhost" differently
        // (sometimes ::1 only) and we always bind to 127.0.0.1, so
        // either name in the URL should validate.
        let ck = rcgen::generate_simple_self_signed(vec![
            "localhost".to_string(),
            "127.0.0.1".to_string(),
        ])
        .expect("generate self-signed cert");
        let cert_der = rustls::pki_types::CertificateDer::from(ck.cert.der().to_vec());
        let key_der = PrivatePkcs8KeyDer::from(ck.key_pair.serialize_der());

        let server_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![cert_der.clone()],
                rustls::pki_types::PrivateKeyDer::Pkcs8(key_der),
            )
            .expect("build server config");
        let server_config = Arc::new(server_config);

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();

        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let cfg = Arc::clone(&server_config);
                let resps = responses.clone();
                std::thread::spawn(move || {
                    let mut tls = match rustls::ServerConnection::new(cfg) {
                        Ok(c) => c,
                        Err(_) => return,
                    };
                    let mut sock = stream;
                    let mut s = rustls::Stream::new(&mut tls, &mut sock);

                    // Read request: Connection: close means we wait
                    // until the peer half-closes after sending body.
                    // Same trick as MockHttpServer above (parse
                    // Content-Length, then read exactly that much).
                    let mut total = Vec::new();
                    let mut buf = [0u8; 4096];
                    loop {
                        match s.read(&mut buf) {
                            Ok(0) => break,
                            Ok(n) => total.extend_from_slice(&buf[..n]),
                            Err(_) => break,
                        }
                        if let Some(idx) = total.windows(4).position(|w| w == b"\r\n\r\n") {
                            let head = std::str::from_utf8(&total[..idx]).unwrap_or("");
                            let cl = head
                                .lines()
                                .find_map(|l| {
                                    l.to_ascii_lowercase()
                                        .strip_prefix("content-length:")
                                        .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                                })
                                .unwrap_or(0);
                            if total.len() >= idx + 4 + cl {
                                break;
                            }
                        }
                    }
                    let head_end = total
                        .windows(4)
                        .position(|w| w == b"\r\n\r\n")
                        .unwrap_or(total.len());
                    let head_str = std::str::from_utf8(&total[..head_end]).unwrap_or("");
                    let line = head_str.lines().next().unwrap_or("");
                    let mut parts = line.split(' ');
                    let method = parts.next().unwrap_or("");
                    let path = parts.next().unwrap_or("");

                    let (status, payload) = resps
                        .iter()
                        .find(|(m, p, _, _)| *m == method && *p == path)
                        .map(|(_, _, st, b)| (*st, *b))
                        .unwrap_or((404, ""));
                    let body_bytes = payload.as_bytes();
                    let resp = format!(
                        "HTTP/1.1 {} OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        status,
                        body_bytes.len()
                    );
                    let _ = s.write_all(resp.as_bytes());
                    let _ = s.write_all(body_bytes);
                    let _ = s.flush();
                });
            }
        });

        (port, cert_der)
    }

    /// build_trusting_config makes a rustls ClientConfig that trusts
    /// EXACTLY the cert handed in. Used by tests to talk to the local
    /// TLS mock — production code never goes through this path.
    fn build_trusting_config(
        cert: rustls::pki_types::CertificateDer<'static>,
    ) -> Arc<rustls::ClientConfig> {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert).expect("add test cert to root store");
        Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        )
    }

    #[test]
    fn https_round_trip_against_self_signed_local_server() {
        let (port, cert) = start_tls_mock(vec![
            ("GET", "/users", 200, r#"[{"id":1,"name":"alice"}]"#),
            ("POST", "/users", 201, r#"{"id":2,"name":"bob"}"#),
        ]);

        // Build adapter pointing at https://localhost:<port>/users,
        // with the test cert injected as the trusted root.
        let mut a = RestAdapter::from_url(&format!("https://127.0.0.1:{}/users", port)).unwrap();
        a.tls_config_override = Some(build_trusting_config(cert));

        // find — exercises TLS handshake + GET + JSON parse.
        let resp = a.execute("find", &json!({"query": {}})).unwrap();
        assert_eq!(resp["ok"], true, "{resp}");
        let rows = resp["data"].as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["name"], "alice");

        // insert — exercises TLS handshake + POST with body.
        let resp = a
            .execute("insert", &json!({"doc": {"name": "bob"}}))
            .unwrap();
        assert_eq!(resp["data"]["id"], 2);
    }

    #[test]
    fn https_handshake_against_untrusted_cert_errors_cleanly() {
        // Use the DEFAULT tls_config (webpki-roots) against a server
        // presenting a self-signed cert — the handshake must fail,
        // and the failure must surface as an error rather than a panic.
        let (port, _untrusted_cert) = start_tls_mock(vec![("GET", "/users", 200, r#"[]"#)]);
        let a = RestAdapter::from_url(&format!("https://127.0.0.1:{}/users", port)).unwrap();
        // tls_config_override stays None → production trust path.
        let err = a.execute("find", &json!({"query": {}})).unwrap_err();
        // The exact error message comes from rustls and may vary by
        // version; the point is it doesn't panic and it mentions
        // either "unknown" CA, "certificate", or "trust" in the message.
        let lower = err.to_lowercase();
        assert!(
            lower.contains("certificate") || lower.contains("unknown") || lower.contains("trust"),
            "rustls error should mention cert/trust failure: {err}"
        );
    }
}
