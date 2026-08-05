//! The offline acceptance test from ADR-0024: spawn the real binary and speak
//! newline-delimited JSON-RPC over its stdio — the exact transport an MCP host
//! uses. No OxiDB server is needed: connections are dialed lazily, so the
//! handshake, the tool list and the write-gating are all observable offline.

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, Command, Stdio};

use serde_json::{Value, json};

struct Mcp {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<std::process::ChildStdout>,
}

impl Mcp {
    fn spawn(envs: &[(&str, &str)]) -> Self {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_oxidb-mcp"));
        // Point at a dead address: any accidental wire call fails loudly.
        cmd.env("OXIDB_ADDR", "127.0.0.1:1")
            .env_remove("OXIDB_MCP_WRITES")
            .env_remove("OXIDB_MCP_DB")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        for (k, v) in envs {
            cmd.env(k, v);
        }
        let mut child = cmd.spawn().expect("spawn oxidb-mcp");
        let stdin = child.stdin.take().unwrap();
        let stdout = BufReader::new(child.stdout.take().unwrap());
        Self {
            child,
            stdin,
            stdout,
        }
    }

    fn send(&mut self, msg: &Value) {
        writeln!(self.stdin, "{msg}").unwrap();
        self.stdin.flush().unwrap();
    }

    fn recv(&mut self) -> Value {
        let mut line = String::new();
        self.stdout.read_line(&mut line).unwrap();
        assert!(!line.is_empty(), "server closed stdout unexpectedly");
        serde_json::from_str(&line).expect("response is one JSON line")
    }

    fn call(&mut self, msg: Value) -> Value {
        self.send(&msg);
        self.recv()
    }
}

impl Drop for Mcp {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The canonical host startup: initialize → initialized → tools/list → ping.
#[test]
fn handshake_tool_list_and_ping_work_offline() {
    let mut mcp = Mcp::spawn(&[]);

    let resp = mcp.call(json!({
        "jsonrpc": "2.0", "id": 0, "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": { "name": "test-host", "version": "0" }
        }
    }));
    assert_eq!(resp["jsonrpc"], "2.0");
    assert_eq!(resp["id"], 0);
    assert_eq!(resp["result"]["protocolVersion"], "2025-06-18");
    assert_eq!(resp["result"]["serverInfo"]["name"], "oxidb-mcp");
    assert!(resp["result"]["capabilities"]["tools"].is_object());

    // A notification produces no response — proven by the next request
    // answering with its own id, not a stray line.
    mcp.send(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }));

    let resp = mcp.call(json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }));
    assert_eq!(resp["id"], 1);
    let names: Vec<&str> = resp["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    for expected in [
        "list_databases",
        "list_collections",
        "list_tables",
        "describe_table",
        "list_indexes",
        "find",
        "count",
        "aggregate",
        "explain",
        "sql_query",
        "tsdb_query",
        "text_search",
    ] {
        assert!(
            names.contains(&expected),
            "missing tool {expected}: {names:?}"
        );
    }
    for write in ["insert", "update", "delete", "sql_execute"] {
        assert!(
            !names.contains(&write),
            "{write} offered without OXIDB_MCP_WRITES"
        );
    }

    let resp = mcp.call(json!({ "jsonrpc": "2.0", "id": 2, "method": "ping" }));
    assert_eq!(resp["result"], json!({}));
}

#[test]
fn write_tools_appear_only_with_the_flag() {
    let mut mcp = Mcp::spawn(&[("OXIDB_MCP_WRITES", "1")]);
    let resp = mcp.call(json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }));
    let names: Vec<&str> = resp["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    for write in ["insert", "update", "delete", "sql_execute"] {
        assert!(names.contains(&write), "missing write tool {write}");
    }
}

#[test]
fn unknown_methods_are_refused_by_name_and_the_server_keeps_serving() {
    let mut mcp = Mcp::spawn(&[]);
    let resp = mcp.call(json!({ "jsonrpc": "2.0", "id": 1, "method": "resources/list" }));
    assert_eq!(resp["error"]["code"], -32601);
    assert!(
        resp["error"]["message"]
            .as_str()
            .unwrap()
            .contains("resources/list")
    );

    // Still alive after the refusal.
    let resp = mcp.call(json!({ "jsonrpc": "2.0", "id": 2, "method": "ping" }));
    assert_eq!(resp["result"], json!({}));
}

/// A tool that needs the database reports the connection failure as a tool
/// error (isError: true) the model can read — not a crash, not a hang.
#[test]
fn wire_failure_is_a_readable_tool_error() {
    let mut mcp = Mcp::spawn(&[]);
    let resp = mcp.call(json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": "list_databases", "arguments": {} }
    }));
    assert_eq!(resp["result"]["isError"], true);
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    assert!(!text.is_empty());
}
