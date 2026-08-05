//! A [`Wire`] backed by the server's **REST surface** instead of OxiWire —
//! the hosted half of ADR-0024.
//!
//! Why REST and not the native protocol: an OxiBase project authenticates with
//! a per-project JWT (`anon_key` / `service_role_key`), and the surface that
//! verifies those keys, applies the project's row-level rules, and enforces
//! its per-project rate limit is the REST listener. Reaching the engine over
//! OxiWire instead would mean re-implementing all three here, with a copy of
//! the seal key — so the hosted mode **forwards the caller's own key** and
//! inherits the gate that already exists. Nothing in this file decides who may
//! read what; it only translates shapes.
//!
//! The translation target is the *native* `/api` surface (not PostgREST),
//! because its request and response shapes are already the wire's: `find`
//! returns a document array, `count` returns `{"count": n}`. Time-series is
//! the exception — it is only reachable through the PostgREST profile route.

use serde_json::{Value, json};

use crate::Wire;

pub struct RestWire {
    base_url: String,
    /// The caller's project key, forwarded verbatim. Never minted here.
    bearer: Option<String>,
    /// Project ref = database name (ADR-0012 `?db=`).
    db: Option<String>,
}

impl RestWire {
    pub fn new(base_url: String, bearer: Option<String>, db: Option<String>) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            bearer,
            db,
        }
    }

    /// Build `<base><path>?<params>&db=<ref>`, percent-encoding every value.
    fn url(&self, path: &str, params: &[(&str, String)]) -> String {
        let mut url = format!("{}{}", self.base_url, path);
        let mut sep = '?';
        for (k, v) in params {
            url.push(sep);
            url.push_str(k);
            url.push('=');
            url.push_str(&percent_encode(v));
            sep = '&';
        }
        if let Some(db) = &self.db {
            url.push(sep);
            url.push_str("db=");
            url.push_str(&percent_encode(db));
        }
        url
    }

    fn get(&self, path: &str, params: &[(&str, String)]) -> Result<Value, String> {
        let url = self.url(path, params);
        let resp = oxidb_http::client::get(&url, self.bearer.as_deref())
            .map_err(|e| format!("request to {path} failed: {e}"))?;
        interpret(resp)
    }

    fn post(&self, path: &str, body: &Value) -> Result<Value, String> {
        let url = self.url(path, &[]);
        let bytes = serde_json::to_vec(body).map_err(|e| e.to_string())?;
        let resp = oxidb_http::client::post_json(&url, self.bearer.as_deref(), &bytes)
            .map_err(|e| format!("request to {path} failed: {e}"))?;
        interpret(resp)
    }

    /// Time-series lives only behind the PostgREST schema profile, so it needs
    /// a hand-built request rather than the two helpers above.
    fn tsdb(&self, request: &Value) -> Result<Value, String> {
        let m = str_field(request, "measurement")?;
        let field = str_field(request, "field")?;
        let mut params: Vec<(&str, String)> = vec![("select", field.to_string())];
        if let Some(start) = request.get("start").and_then(|v| v.as_i64()) {
            params.push(("ts", format!("gte.{start}")));
        }
        if let Some(end) = request.get("end").and_then(|v| v.as_i64()) {
            params.push(("ts", format!("lt.{end}")));
        }
        // Tag equality filters become PostgREST `col=eq.value` pairs. The
        // borrow has to outlive the request, so the names are owned here.
        let tag_params: Vec<(String, String)> = request
            .get("tags")
            .and_then(|t| t.as_object())
            .map(|o| {
                o.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), format!("eq.{s}"))))
                    .collect()
            })
            .unwrap_or_default();
        for (k, v) in &tag_params {
            params.push((k.as_str(), v.clone()));
        }
        for key in ["agg", "interval", "p"] {
            if let Some(v) = request.get(key) {
                params.push((key, scalar_to_string(v)));
            }
        }
        if let Some(g) = request.get("group_by").and_then(|v| v.as_array()) {
            let names: Vec<&str> = g.iter().filter_map(|v| v.as_str()).collect();
            if !names.is_empty() {
                params.push(("group_by", names.join(",")));
            }
        }

        let url = self.url(&format!("/rest/v1/{m}"), &params);
        let auth = self.bearer.as_ref().map(|t| format!("Bearer {t}"));
        let mut headers: Vec<(&str, &str)> = vec![("Accept-Profile", "tsdb")];
        if let Some(a) = &auth {
            headers.push(("Authorization", a));
        }
        let resp = oxidb_http::client::request("GET", &url, &headers, &[])
            .map_err(|e| format!("tsdb request failed: {e}"))?;
        interpret(resp)
    }
}

impl Wire for RestWire {
    fn call(&self, request: &Value) -> Result<Value, String> {
        // The SQL and TSDB engines are selected by `engine`, as on the wire.
        match request.get("engine").and_then(|v| v.as_str()) {
            Some("sql") => {
                let sql = str_field(request, "sql")?;
                let mut body = json!({ "sql": sql });
                if let Some(p) = request.get("params") {
                    body["params"] = p.clone();
                }
                return self.post("/api/sql", &body);
            }
            Some("tsdb") => return self.tsdb(request),
            _ => {}
        }

        let cmd = request.get("cmd").and_then(|v| v.as_str()).unwrap_or("");
        match cmd {
            // A project *is* one database, so there is no list to serve and
            // no endpoint that would serve it. Answer what is true here
            // rather than failing: the caller is scoped to this one.
            "list_databases" => Ok(json!(
                self.db.as_deref().map(|d| vec![d]).unwrap_or_default()
            )),

            "list_collections" => {
                let v = self.get("/api/collections", &[])?;
                // REST wraps it; the wire returns a bare array.
                Ok(v.get("collections").cloned().unwrap_or(v))
            }

            "find" => {
                let col = str_field(request, "collection")?;
                let mut params = vec![("q", json_param(request.get("query")))];
                for key in ["sort"] {
                    if let Some(v) = request.get(key) {
                        params.push((key, v.to_string()));
                    }
                }
                for key in ["skip", "limit"] {
                    if let Some(v) = request.get(key).and_then(|v| v.as_u64()) {
                        params.push((key, v.to_string()));
                    }
                }
                self.get(&format!("/api/{col}/documents"), &params)
            }

            "count" => {
                let col = str_field(request, "collection")?;
                self.get(
                    &format!("/api/{col}/count"),
                    &[("q", json_param(request.get("query")))],
                )
            }

            "aggregate" => {
                let col = str_field(request, "collection")?;
                let pipeline = request.get("pipeline").ok_or("missing 'pipeline'")?;
                self.post(
                    &format!("/api/{col}/aggregate"),
                    &json!({ "pipeline": pipeline }),
                )
            }

            "text_search" => {
                let col = str_field(request, "collection")?;
                let mut body = json!({ "query": str_field(request, "query")? });
                for key in ["limit", "highlight"] {
                    if let Some(v) = request.get(key) {
                        body[key] = v.clone();
                    }
                }
                self.post(&format!("/api/{col}/text_search"), &body)
            }

            "list_indexes" => {
                let col = str_field(request, "collection")?;
                self.get(&format!("/api/{col}/indexes"), &[])
            }

            "insert" => {
                let col = str_field(request, "collection")?;
                let doc = request.get("doc").ok_or("missing 'doc'")?;
                self.post(&format!("/api/{col}/documents"), &json!({ "doc": doc }))
            }

            "insert_many" => {
                let col = str_field(request, "collection")?;
                let docs = request.get("docs").ok_or("missing 'docs'")?;
                self.post(&format!("/api/{col}/documents"), &json!({ "docs": docs }))
            }

            // PATCH/DELETE carry a JSON body, which neither helper sends.
            "update" | "delete" => {
                let col = str_field(request, "collection")?;
                let method = if cmd == "update" { "PATCH" } else { "DELETE" };
                let mut body =
                    json!({ "query": request.get("query").cloned().unwrap_or(json!({})) });
                if cmd == "update" {
                    body["update"] = request.get("update").ok_or("missing 'update'")?.clone();
                }
                let url = self.url(&format!("/api/{col}/documents"), &[]);
                let auth = self.bearer.as_ref().map(|t| format!("Bearer {t}"));
                let mut headers: Vec<(&str, &str)> = vec![("Content-Type", "application/json")];
                if let Some(a) = &auth {
                    headers.push(("Authorization", a));
                }
                let bytes = serde_json::to_vec(&body).map_err(|e| e.to_string())?;
                let resp = oxidb_http::client::request(method, &url, &headers, &bytes)
                    .map_err(|e| format!("{method} failed: {e}"))?;
                interpret(resp)
            }

            // `explain` has no REST endpoint. Saying so is better than a
            // confusing 404 from a path that was never going to exist.
            "explain" => Err(
                "explain is not available over the hosted (HTTP) endpoint — it is a \
                 wire-protocol diagnostic; run oxidb-mcp in stdio mode against the server"
                    .into(),
            ),

            other => Err(format!("command not available over REST: {other}")),
        }
    }
}

/// Map an HTTP response to the wire's success/error split. The REST surface
/// carries the reason in `{"error": …}`; a rules refusal is a 403 and a
/// per-project rate limit a 429, and both are worth naming as such because a
/// model can act on the difference.
fn interpret(resp: oxidb_http::client::Response) -> Result<Value, String> {
    let text = resp.text();
    let parsed: Option<Value> = serde_json::from_str(&text).ok();
    if resp.is_success() {
        return Ok(parsed.unwrap_or(Value::Null));
    }
    let detail = parsed
        .as_ref()
        .and_then(|v| v.get("error"))
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .unwrap_or_else(|| text.chars().take(300).collect());
    Err(match resp.status {
        401 | 403 => format!("access denied ({}): {detail}", resp.status),
        429 => format!("rate limited (429): {detail}"),
        404 => format!("not found (404): {detail}"),
        s => format!("request failed ({s}): {detail}"),
    })
}

fn str_field<'a>(v: &'a Value, key: &str) -> Result<&'a str, String> {
    v.get(key)
        .and_then(|x| x.as_str())
        .ok_or_else(|| format!("missing '{key}'"))
}

fn json_param(v: Option<&Value>) -> String {
    v.cloned().unwrap_or(json!({})).to_string()
}

fn scalar_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Percent-encode a query-parameter value (RFC 3986 unreserved set kept).
fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percent_encoding_covers_json_punctuation() {
        assert_eq!(percent_encode("{\"a\":1}"), "%7B%22a%22%3A1%7D");
        assert_eq!(percent_encode("a b&c=d"), "a%20b%26c%3Dd");
        assert_eq!(percent_encode("safe-_.~"), "safe-_.~");
        // Non-ASCII goes out as UTF-8 bytes, not as replacement characters.
        assert_eq!(percent_encode("é"), "%C3%A9");
    }

    #[test]
    fn url_appends_db_and_encodes_params() {
        let w = RestWire::new("http://h:1/".into(), None, Some("proj1".into()));
        let url = w.url("/api/c/documents", &[("q", "{\"a\":1}".into())]);
        assert_eq!(
            url,
            "http://h:1/api/c/documents?q=%7B%22a%22%3A1%7D&db=proj1"
        );
        // No params: db still starts the query string.
        assert_eq!(
            w.url("/api/collections", &[]),
            "http://h:1/api/collections?db=proj1"
        );
    }

    #[test]
    fn url_without_a_db_omits_the_param() {
        let w = RestWire::new("http://h:1".into(), None, None);
        assert_eq!(w.url("/api/collections", &[]), "http://h:1/api/collections");
    }

    #[test]
    fn list_databases_reports_the_one_project_without_a_request() {
        let w = RestWire::new("http://127.0.0.1:1".into(), None, Some("proj1".into()));
        // Would fail loudly if it tried to reach the (dead) address.
        assert_eq!(
            w.call(&json!({"cmd": "list_databases"})).unwrap(),
            json!(["proj1"])
        );
    }

    #[test]
    fn explain_is_refused_by_name_not_attempted() {
        let w = RestWire::new("http://127.0.0.1:1".into(), None, None);
        let err = w.call(&json!({"cmd": "explain", "inner": {}})).unwrap_err();
        assert!(err.contains("stdio mode"), "{err}");
    }

    #[test]
    fn unknown_commands_are_named_in_the_error() {
        let w = RestWire::new("http://127.0.0.1:1".into(), None, None);
        let err = w.call(&json!({"cmd": "snapshot_begin"})).unwrap_err();
        assert!(err.contains("snapshot_begin"), "{err}");
    }
}
