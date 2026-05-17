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
//! Scope (v3b):
//!   - HTTP/1.1 plain, no TLS. `https://` URLs are rejected with a
//!     clear "not supported in v3b" message.
//!   - No chunked transfer-encoding; servers must send Content-Length.
//!   - No redirects, no compression, no HTTP/2.
//!   - id field defaults to `"id"`. Override via URL fragment:
//!     `http://api.example.com/users#id_field=user_id`.
//!
//! These limits are real but covered by tests, and a follow-up PR
//! can add TLS + chunked + redirects without touching the public
//! adapter trait.

use serde_json::{json, Value};
use std::io::{Read, Write};
use std::net::TcpStream;
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

/// RestAdapter owns a parsed http:// endpoint and emits HTTP/1.1
/// requests on demand. No persistent connection — every request opens
/// a fresh TCP socket (vs OxiDB's pooled conns) because most public
/// REST APIs ignore Connection: keep-alive anyway and the per-request
/// dial cost is acceptable for a generic adapter.
pub struct RestAdapter {
    host: String,
    port: u16,
    /// Path on the remote, with leading slash. e.g. "/users".
    base_path: String,
    /// Configurable primary-key field name; defaults to "id".
    id_field: String,
    /// Original URL kept around for error messages.
    url: String,
}

impl RestAdapter {
    pub fn from_url(url: &str) -> Result<Self, String> {
        if url.starts_with("https://") {
            return Err(format!(
                "REST FDW adapter does not support HTTPS yet (v3b) — got {:?}; \
                use http:// or run a local TLS-terminating proxy in front of the API",
                url
            ));
        }
        let rest = url
            .strip_prefix("http://")
            .ok_or_else(|| format!("REST link URL must start with http:// — got {:?}", url))?;

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
            None => (hostport.to_string(), 80),
        };
        if host.is_empty() {
            return Err("REST link URL host is empty".to_string());
        }

        let mut id_field = DEFAULT_ID_FIELD.to_string();
        if let Some(frag) = fragment {
            for pair in frag.split('&') {
                if let Some(v) = pair.strip_prefix("id_field=") {
                    if !v.is_empty() {
                        id_field = v.to_string();
                    }
                }
            }
        }

        Ok(Self {
            host,
            port,
            base_path: path.to_string(),
            id_field,
            url: url.to_string(),
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
                let body = http_get(&self.host, self.port, &self.base_path)?;
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
                        let body = http_get(&self.host, self.port, &self.base_path)?;
                        let parsed = parse_json_body(&body)?;
                        let arr = into_array(parsed);
                        let first = arr.into_iter().next().unwrap_or(Value::Null);
                        Ok(ok_envelope(first))
                    }
                    Some(id) => {
                        let path = format!("{}/{}", self.base_path, id);
                        let body = http_get(&self.host, self.port, &path)?;
                        let parsed = parse_json_body(&body)?;
                        Ok(ok_envelope(parsed))
                    }
                }
            }
            "count" => {
                let body = http_get(&self.host, self.port, &self.base_path)?;
                let parsed = parse_json_body(&body)?;
                let arr = into_array(parsed);
                let n = filter_array(&arr, &query).len();
                Ok(ok_envelope(json!({ "count": n })))
            }
            "insert" => {
                let doc = request
                    .get("doc")
                    .ok_or("missing 'doc'")?
                    .clone();
                let body = serde_json::to_vec(&doc)
                    .map_err(|e| format!("encode doc: {}", e))?;
                let resp = http_request(
                    &self.host,
                    self.port,
                    "POST",
                    &self.base_path,
                    Some(&body),
                )?;
                let parsed = parse_json_body(&resp)?;
                // Echo back what the server returned in the standard
                // `{"ok": true, "data": ...}` envelope; many APIs
                // return the created resource (including the new id),
                // which is exactly what callers want.
                Ok(ok_envelope(parsed))
            }
            "update_one" => {
                let id = self.id_from_query(&query)?.ok_or(
                    "REST FDW update_one requires a query selecting by the id field",
                )?;
                let update = request.get("update").ok_or("missing 'update'")?;
                let set = update
                    .get("$set")
                    .ok_or("REST FDW supports only the $set update operator")?;
                let body = serde_json::to_vec(set)
                    .map_err(|e| format!("encode $set: {}", e))?;
                let path = format!("{}/{}", self.base_path, id);
                let resp = http_request(&self.host, self.port, "PATCH", &path, Some(&body))?;
                let parsed = parse_json_body(&resp).unwrap_or(Value::Null);
                Ok(ok_envelope(json!({ "modified": 1, "doc": parsed })))
            }
            "delete_one" => {
                let id = self.id_from_query(&query)?.ok_or(
                    "REST FDW delete_one requires a query selecting by the id field",
                )?;
                let path = format!("{}/{}", self.base_path, id);
                // Many APIs return empty body on DELETE — that's fine,
                // we don't try to parse the response.
                http_request(&self.host, self.port, "DELETE", &path, None)?;
                Ok(ok_envelope(json!({ "deleted": 1 })))
            }
            other => Err(format!(
                "REST FDW adapter does not implement command {:?} (link {})",
                other, self.url
            )),
        }
    }
}

/// http_get is the verb-specific convenience wrapper most call sites
/// want. Equivalent to http_request("GET", path, None).
fn http_get(host: &str, port: u16, path: &str) -> Result<Vec<u8>, String> {
    http_request(host, port, "GET", path, None)
}

/// http_request issues one HTTP/1.1 request and returns the response
/// body as bytes. Connection is closed after the request via
/// `Connection: close` — no keep-alive, no pooling, fresh socket per
/// call. That's pessimistic but safe; REST endpoints rarely care.
///
/// Returns Err on any of:
///   - TCP connect / timeout failure
///   - response not parseable as HTTP/1.x
///   - 4xx / 5xx response (the message includes the status line)
///   - body exceeds MAX_RESPONSE_BYTES
fn http_request(
    host: &str,
    port: u16,
    method: &str,
    path: &str,
    body: Option<&[u8]>,
) -> Result<Vec<u8>, String> {
    let addr = format!("{}:{}", host, port);
    let mut stream = TcpStream::connect_timeout(
        &addr
            .to_socket_addrs_first()?,
        HTTP_TIMEOUT,
    )
    .map_err(|e| format!("connect {}: {}", addr, e))?;
    stream
        .set_read_timeout(Some(HTTP_TIMEOUT))
        .map_err(|e| format!("set read timeout: {}", e))?;
    stream
        .set_write_timeout(Some(HTTP_TIMEOUT))
        .map_err(|e| format!("set write timeout: {}", e))?;

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
    stream
        .write_all(req.as_bytes())
        .map_err(|e| format!("write request line/headers: {}", e))?;
    if let Some(b) = body {
        stream
            .write_all(b)
            .map_err(|e| format!("write request body: {}", e))?;
    }

    let mut raw = Vec::with_capacity(4096);
    let mut buf = [0u8; 4096];
    loop {
        let n = stream
            .read(&mut buf)
            .map_err(|e| format!("read response: {}", e))?;
        if n == 0 {
            break;
        }
        if raw.len() + n > MAX_RESPONSE_BYTES {
            return Err(format!(
                "REST response exceeds {} bytes — aborting to avoid OOM",
                MAX_RESPONSE_BYTES
            ));
        }
        raw.extend_from_slice(&buf[..n]);
    }

    parse_http_response(&raw)
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

    let status_line = head
        .lines()
        .next()
        .ok_or("response has no status line")?;
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

        fn handle(
            mut stream: TcpStream,
            log: Arc<Mutex<Vec<(String, String, Vec<u8>)>>>,
            responses: Vec<(&'static str, &'static str, u16, &'static str)>,
        ) {
            stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
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

            let head_end = total.windows(4).position(|w| w == b"\r\n\r\n").unwrap_or(total.len());
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

            log.lock().unwrap().push((method.clone(), path.clone(), body));

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
    fn https_url_is_rejected_with_clear_message() {
        let err = expect_url_err("https://api.example.com/users");
        assert!(err.contains("HTTPS"));
        assert!(err.contains("v3b"));
    }

    #[test]
    fn missing_resource_path_is_rejected() {
        let err = expect_url_err("http://api.example.com");
        assert!(err.contains("resource path"));
    }

    #[test]
    fn find_returns_array_from_endpoint() {
        let srv = MockHttpServer::start(vec![
            ("GET", "/users", 200, r#"[{"id":1,"name":"alice"},{"id":2,"name":"bob"}]"#),
        ]);
        let a = adapter(srv.port, "/users");
        let resp = a.execute("find", &json!({"query": {}})).unwrap();
        let rows = resp["data"].as_array().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1]["name"], "bob");
    }

    #[test]
    fn find_filters_array_client_side_with_equality_predicate() {
        let srv = MockHttpServer::start(vec![
            ("GET", "/users", 200, r#"[{"id":1,"role":"admin"},{"id":2,"role":"user"}]"#),
        ]);
        let a = adapter(srv.port, "/users");
        let resp = a.execute("find", &json!({"query": {"role": "admin"}})).unwrap();
        let rows = resp["data"].as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["id"], 1);
    }

    #[test]
    fn find_one_with_id_query_uses_path_segment() {
        let srv = MockHttpServer::start(vec![
            ("GET", "/users/42", 200, r#"{"id":42,"name":"x"}"#),
        ]);
        let a = adapter(srv.port, "/users");
        let resp = a.execute("find_one", &json!({"query": {"id": 42}})).unwrap();
        assert_eq!(resp["data"]["name"], "x");
        // Confirm we hit /users/42, not /users.
        let log = srv.requests.lock().unwrap();
        assert_eq!(log[0].0, "GET");
        assert_eq!(log[0].1, "/users/42");
    }

    #[test]
    fn count_uses_filtered_array_length() {
        let srv = MockHttpServer::start(vec![
            ("GET", "/things", 200, r#"[{"x":1},{"x":1},{"x":2}]"#),
        ]);
        let a = adapter(srv.port, "/things");
        let resp = a.execute("count", &json!({"query": {"x": 1}})).unwrap();
        assert_eq!(resp["data"]["count"], 2);
    }

    #[test]
    fn unwraps_data_envelope_from_apis_that_wrap() {
        // Many REST APIs return `{"data": [...]}` instead of a bare
        // array. The adapter must accept that shape transparently.
        let srv = MockHttpServer::start(vec![
            ("GET", "/items", 200, r#"{"data":[{"x":1},{"x":2}]}"#),
        ]);
        let a = adapter(srv.port, "/items");
        let resp = a.execute("find", &json!({"query": {}})).unwrap();
        assert_eq!(resp["data"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn insert_posts_doc_and_returns_server_payload() {
        let srv = MockHttpServer::start(vec![
            ("POST", "/users", 201, r#"{"id":99,"name":"new"}"#),
        ]);
        let a = adapter(srv.port, "/users");
        let resp = a.execute("insert", &json!({"doc": {"name": "new"}})).unwrap();
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
        let srv = MockHttpServer::start(vec![
            ("PATCH", "/users/7", 200, r#"{"id":7,"name":"updated"}"#),
        ]);
        let a = adapter(srv.port, "/users");
        let resp = a.execute("update_one", &json!({
            "query": {"id": 7},
            "update": {"$set": {"name": "updated"}},
        })).unwrap();
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
        let err = a.execute("update_one", &json!({
            "query": {},
            "update": {"$set": {"name": "x"}},
        })).unwrap_err();
        assert!(err.contains("id field"));
    }

    #[test]
    fn update_one_without_set_operator_is_rejected() {
        let srv = MockHttpServer::start(vec![]);
        let a = adapter(srv.port, "/users");
        let err = a.execute("update_one", &json!({
            "query": {"id": 1},
            "update": {"$inc": {"x": 1}},
        })).unwrap_err();
        assert!(err.contains("$set"));
    }

    #[test]
    fn delete_one_deletes_by_id() {
        let srv = MockHttpServer::start(vec![
            ("DELETE", "/items/abc", 204, ""),
        ]);
        let a = adapter(srv.port, "/items");
        let resp = a.execute("delete_one", &json!({"query": {"id": "abc"}})).unwrap();
        assert_eq!(resp["data"]["deleted"], 1);
    }

    #[test]
    fn id_field_override_via_url_fragment() {
        let srv = MockHttpServer::start(vec![
            ("GET", "/items/sku-9", 200, r#"{"sku":"sku-9"}"#),
        ]);
        let a = RestAdapter::from_url(
            &format!("http://127.0.0.1:{}/items#id_field=sku", srv.port)
        ).unwrap();
        let resp = a.execute("find_one", &json!({"query": {"sku": "sku-9"}})).unwrap();
        assert_eq!(resp["data"]["sku"], "sku-9");
    }

    #[test]
    fn non_2xx_response_surfaces_as_error_with_status() {
        let srv = MockHttpServer::start(vec![
            ("GET", "/oops", 500, r#"{"error":"boom"}"#),
        ]);
        let a = adapter(srv.port, "/oops");
        let err = a.execute("find", &json!({"query": {}})).unwrap_err();
        assert!(err.contains("500"));
        assert!(err.contains("boom"), "body snippet must surface: {}", err);
    }

    #[test]
    fn non_json_response_surfaces_with_body_snippet() {
        let srv = MockHttpServer::start(vec![
            ("GET", "/html", 200, "<html><body>not json</body></html>"),
        ]);
        let a = adapter(srv.port, "/html");
        let err = a.execute("find", &json!({"query": {}})).unwrap_err();
        assert!(err.contains("not JSON"));
        assert!(err.contains("<html>"), "{}", err);
    }
}
