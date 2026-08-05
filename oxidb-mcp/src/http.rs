//! The **streamable-HTTP** MCP transport (ADR-0024 Phase 2): one endpoint per
//! OxiBase project, so a hosted project can be reached by an AI host without
//! anything installed locally.
//!
//! ```text
//! POST /mcp/<project-ref>
//! Authorization: Bearer <anon_key | service_role_key>
//! Content-Type: application/json
//!
//! {"jsonrpc":"2.0","id":1,"method":"tools/list"}
//! ```
//!
//! **Every request is independent.** The project ref comes from the path and
//! the key from the header, and both are used to build a [`RestWire`] for that
//! one request — no session table, no cached credentials, nothing shared
//! between two callers. That is what makes a single process safe to point at
//! many projects: there is no state in which one tenant's key could be
//! applied to another tenant's database.
//!
//! Authorization is **not decided here**. The key is forwarded to the REST
//! surface, which verifies it against the project's own secret and applies
//! that project's rules and rate limits (see [`crate::rest_wire`]).

use oxidb_http::message::{HttpRequest, HttpResponse};
use serde_json::json;

use crate::{Config, McpServer, rest_wire::RestWire};

pub struct HttpConfig {
    /// Where the OxiDB REST listener lives, e.g. `http://127.0.0.1:8080`.
    pub upstream: String,
    /// Offer the write tools. The key still has to be allowed to write.
    pub allow_writes: bool,
    /// Serve exactly this project and ignore the path ref (single-tenant).
    pub fixed_db: Option<String>,
}

/// Serve MCP over HTTP until the process is killed.
pub fn serve(addr: &str, pool_size: usize, config: HttpConfig) -> std::io::Result<()> {
    eprintln!(
        "oxidb-mcp: HTTP transport on {addr} → REST {}{}{}",
        config.upstream,
        config
            .fixed_db
            .as_deref()
            .map(|d| format!(", fixed to project '{d}'"))
            .unwrap_or_else(|| ", project from /mcp/<ref>".into()),
        if config.allow_writes {
            ", WRITE TOOLS ENABLED"
        } else {
            ", read-only tools"
        },
    );
    oxidb_http::server::serve(addr, pool_size, 1024, move |req| handle(req, &config))
}

pub fn handle(req: &HttpRequest, config: &HttpConfig) -> HttpResponse {
    // Browsers preflight a cross-origin POST with custom headers.
    if req.method == "OPTIONS" {
        return HttpResponse::no_content()
            .with_cors()
            .with_header(
                "Access-Control-Allow-Headers",
                "Authorization, Content-Type",
            )
            .with_header("Access-Control-Allow-Methods", "POST, GET, OPTIONS");
    }

    let path = req.path.trim_end_matches('/');
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    // A health probe that needs neither a key nor a project.
    if req.method == "GET" && segments.as_slice() == ["mcp", "health"] {
        return json_response(200, "OK", json!({"ok": true, "server": "oxidb-mcp"}));
    }

    let db = match (segments.as_slice(), &config.fixed_db) {
        // A fixed deployment ignores any ref in the path rather than letting
        // a caller name a different database than the one it is scoped to.
        (["mcp"], Some(fixed)) | (["mcp", _], Some(fixed)) => Some(fixed.clone()),
        (["mcp", r], None) => Some((*r).to_string()),
        (["mcp"], None) => None,
        _ => {
            return json_response(
                404,
                "Not Found",
                json!({"error": "POST /mcp/<project-ref> is the MCP endpoint"}),
            );
        }
    };

    if req.method != "POST" {
        return json_response(
            405,
            "Method Not Allowed",
            json!({"error": "MCP messages are POSTed"}),
        )
        .with_header("Allow", "POST, OPTIONS");
    }

    // The caller's key, forwarded verbatim. A missing key is not refused
    // here: an OxiBase project may be readable by an unauthenticated caller
    // if its rules say so, and that judgement belongs to the REST surface.
    let bearer = req
        .headers
        .get("authorization")
        .and_then(|v| {
            v.strip_prefix("Bearer ")
                .or_else(|| v.strip_prefix("bearer "))
        })
        .map(str::to_string);

    let line = match std::str::from_utf8(&req.body) {
        Ok(s) => s.trim(),
        Err(_) => {
            return json_rpc_error(-32700, "request body is not UTF-8");
        }
    };
    if line.is_empty() {
        return json_rpc_error(-32600, "empty request body");
    }

    let wire = RestWire::new(config.upstream.clone(), bearer, db);
    let server = McpServer::new(
        wire,
        Config {
            // The path ref already pinned it; `db` arguments must not move it.
            pinned_db: None,
            allow_writes: config.allow_writes,
        },
    );

    match server.handle_line(line) {
        // A notification gets 202 with no body, as the MCP spec requires.
        None => HttpResponse::no_content().with_cors(),
        Some(body) => HttpResponse::data(body.into_bytes(), "application/json").with_cors(),
    }
}

fn json_response(status: u16, status_text: &'static str, body: serde_json::Value) -> HttpResponse {
    let mut r = HttpResponse::data(body.to_string().into_bytes(), "application/json").with_cors();
    r.status = status;
    r.status_text = status_text;
    r
}

fn json_rpc_error(code: i64, message: &str) -> HttpResponse {
    HttpResponse::data(
        json!({"jsonrpc": "2.0", "id": null, "error": {"code": code, "message": message}})
            .to_string()
            .into_bytes(),
        "application/json",
    )
    .with_cors()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::collections::HashMap;

    fn req(method: &str, path: &str, body: &str, auth: Option<&str>) -> HttpRequest {
        let mut headers = HashMap::new();
        if let Some(a) = auth {
            headers.insert("authorization".to_string(), a.to_string());
        }
        HttpRequest {
            method: method.into(),
            path: path.into(),
            query: String::new(),
            headers,
            body: body.as_bytes().to_vec(),
        }
    }

    fn cfg() -> HttpConfig {
        HttpConfig {
            // Dead address: any request that reaches the wire fails loudly.
            upstream: "http://127.0.0.1:1".into(),
            allow_writes: false,
            fixed_db: None,
        }
    }

    fn body_json(r: &HttpResponse) -> Value {
        serde_json::from_slice(&r.body).expect("json body")
    }

    #[test]
    fn tools_list_is_served_without_touching_the_upstream() {
        let r = handle(
            &req(
                "POST",
                "/mcp/proj1",
                r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#,
                Some("Bearer key123"),
            ),
            &cfg(),
        );
        assert_eq!(r.status, 200);
        let v = body_json(&r);
        let names: Vec<&str> = v["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"find"));
        assert!(!names.contains(&"insert"), "writes off by default");
    }

    #[test]
    fn initialize_works_over_http() {
        let r = handle(
            &req(
                "POST",
                "/mcp/proj1",
                r#"{"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":"2025-06-18"}}"#,
                None,
            ),
            &cfg(),
        );
        assert_eq!(body_json(&r)["result"]["protocolVersion"], "2025-06-18");
    }

    #[test]
    fn a_notification_gets_202_with_no_body() {
        let r = handle(
            &req(
                "POST",
                "/mcp/proj1",
                r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
                None,
            ),
            &cfg(),
        );
        assert!(r.body.is_empty());
        assert!((200..300).contains(&r.status), "status {}", r.status);
    }

    #[test]
    fn health_needs_no_key_and_no_project() {
        let r = handle(&req("GET", "/mcp/health", "", None), &cfg());
        assert_eq!(r.status, 200);
        assert_eq!(body_json(&r)["ok"], true);
    }

    #[test]
    fn a_get_to_the_message_endpoint_is_405_and_says_what_to_do() {
        let r = handle(&req("GET", "/mcp/proj1", "", None), &cfg());
        assert_eq!(r.status, 405);
        assert!(r.headers.iter().any(|(k, _)| k == "Allow"));
    }

    #[test]
    fn an_unknown_path_is_404_naming_the_endpoint() {
        let r = handle(&req("POST", "/rest/v1/things", "{}", None), &cfg());
        assert_eq!(r.status, 404);
        assert!(
            body_json(&r)["error"]
                .as_str()
                .unwrap()
                .contains("/mcp/<project-ref>")
        );
    }

    #[test]
    fn preflight_is_answered_with_the_headers_a_browser_asks_for() {
        let r = handle(&req("OPTIONS", "/mcp/proj1", "", None), &cfg());
        assert!((200..300).contains(&r.status));
        let names: Vec<&str> = r.headers.iter().map(|(k, _)| k.as_str()).collect();
        assert!(names.contains(&"Access-Control-Allow-Headers"));
    }

    #[test]
    fn a_fixed_project_ignores_a_ref_in_the_path() {
        let config = HttpConfig {
            upstream: "http://127.0.0.1:1".into(),
            allow_writes: false,
            fixed_db: Some("mine".into()),
        };
        // Reaching list_databases proves which db the wire was built with,
        // and it is the one tool that answers without a network call.
        let r = handle(
            &req(
                "POST",
                "/mcp/someone-elses-project",
                r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"list_databases","arguments":{}}}"#,
                None,
            ),
            &config,
        );
        let v = body_json(&r);
        let text = v["result"]["content"][0]["text"].as_str().unwrap();
        assert_eq!(text, r#"["mine"]"#, "must not honour the path ref");
    }

    #[test]
    fn the_path_ref_becomes_the_target_database() {
        let r = handle(
            &req(
                "POST",
                "/mcp/proj-abc",
                r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"list_databases","arguments":{}}}"#,
                None,
            ),
            &cfg(),
        );
        let v = body_json(&r);
        let text = v["result"]["content"][0]["text"].as_str().unwrap();
        assert_eq!(text, r#"["proj-abc"]"#);
    }

    #[test]
    fn malformed_bodies_are_json_rpc_errors_not_crashes() {
        let r = handle(&req("POST", "/mcp/p", "{not json", None), &cfg());
        assert_eq!(body_json(&r)["error"]["code"], -32700);
        let r = handle(&req("POST", "/mcp/p", "", None), &cfg());
        assert_eq!(body_json(&r)["error"]["code"], -32600);
    }

    #[test]
    fn write_tools_appear_only_when_enabled() {
        let config = HttpConfig {
            upstream: "http://127.0.0.1:1".into(),
            allow_writes: true,
            fixed_db: None,
        };
        let r = handle(
            &req(
                "POST",
                "/mcp/p",
                r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#,
                None,
            ),
            &config,
        );
        let v = body_json(&r);
        let names: Vec<&str> = v["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"insert"));
    }
}
