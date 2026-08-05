//! `oxidb-mcp` — MCP stdio server for OxiDB (ADR-0024).
//!
//! Spawned by an MCP host (Claude Code, Claude Desktop, Cursor, …):
//!
//! ```json
//! {
//!   "mcpServers": {
//!     "oxidb": {
//!       "command": "oxidb-mcp",
//!       "env": { "OXIDB_ADDR": "127.0.0.1:4444", "OXIDB_USER": "assistant", "OXIDB_PASSWORD": "…" }
//!     }
//!   }
//! }
//! ```
//!
//! Env: `OXIDB_ADDR` (default `127.0.0.1:4444`), `OXIDB_USER`/`OXIDB_PASSWORD`
//! (SCRAM; omit both only against a no-auth server), `OXIDB_MCP_DB` (pin every
//! call to one database), `OXIDB_MCP_WRITES=1` (register the write tools).
//!
//! The recommended account is a **Read-role** one: read-only is then enforced
//! by the server's RBAC, not by this process.

use std::io::{BufRead, Write};

use oxidb_mcp::{Config, McpServer, PoolWire, SERVER_VERSION};

fn env_flag(name: &str) -> bool {
    matches!(
        std::env::var(name)
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

const HELP: &str = "\
oxidb-mcp — MCP (Model Context Protocol) server for OxiDB

An MCP host (Claude Code, Claude Desktop, Cursor, …) launches this and speaks
JSON-RPC over stdin/stdout. Running it by hand does nothing visible: it waits
for a host to talk to it.

  claude mcp add oxidb -e OXIDB_ADDR=127.0.0.1:4444 -- oxidb-mcp

Environment:
  OXIDB_ADDR         server to connect to (default 127.0.0.1:4444)
  OXIDB_USER         SCRAM username — a Read-role account keeps it read-only
  OXIDB_PASSWORD     SCRAM password
  OXIDB_MCP_DB       pin every call to one database
  OXIDB_MCP_WRITES   1 to offer the write tools (off by default)

Options:
  -V, --version      print the version and exit
  -h, --help         print this help and exit";

fn main() {
    // A release binary people download: answer the two flags they will try
    // before the stdin loop, which otherwise looks like a hang. Everything
    // else is configured by environment, so one argument is the whole grammar.
    if let Some(arg) = std::env::args().nth(1) {
        match arg.as_str() {
            "-V" | "--version" => println!("oxidb-mcp {SERVER_VERSION}"),
            "-h" | "--help" => println!("{HELP}"),
            other => {
                eprintln!("oxidb-mcp: unknown argument {other:?} (try --help)");
                std::process::exit(2);
            }
        }
        return;
    }

    // HTTP mode (ADR-0024 Phase 2): serve many callers over one port instead
    // of being spawned per host. Selected by the port being set, because the
    // two modes cannot share a process — stdio owns stdin/stdout.
    if let Some(port) = std::env::var("OXIDB_MCP_HTTP_PORT")
        .ok()
        .filter(|s| !s.is_empty())
    {
        let bind = if port.contains(':') {
            port
        } else {
            format!("0.0.0.0:{port}")
        };
        let config = oxidb_mcp::http::HttpConfig {
            upstream: std::env::var("OXIDB_MCP_UPSTREAM")
                .unwrap_or_else(|_| "http://127.0.0.1:8080".into()),
            allow_writes: env_flag("OXIDB_MCP_WRITES"),
            fixed_db: std::env::var("OXIDB_MCP_DB").ok().filter(|s| !s.is_empty()),
        };
        let pool = std::env::var("OXIDB_MCP_POOL")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8);
        if let Err(e) = oxidb_mcp::http::serve(&bind, pool, config) {
            eprintln!("oxidb-mcp: cannot serve on {bind}: {e}");
            std::process::exit(1);
        }
        return;
    }

    let addr = std::env::var("OXIDB_ADDR").unwrap_or_else(|_| "127.0.0.1:4444".into());
    let user = std::env::var("OXIDB_USER").ok().filter(|s| !s.is_empty());
    let password = std::env::var("OXIDB_PASSWORD")
        .ok()
        .filter(|s| !s.is_empty());
    let config = Config {
        pinned_db: std::env::var("OXIDB_MCP_DB").ok().filter(|s| !s.is_empty()),
        allow_writes: env_flag("OXIDB_MCP_WRITES"),
    };

    // stderr is the host's log pane; stdout carries only protocol messages.
    eprintln!(
        "oxidb-mcp {SERVER_VERSION}: OxiDB at {addr}{}{}{}",
        if user.is_some() {
            " (SCRAM)"
        } else {
            " (no auth)"
        },
        config
            .pinned_db
            .as_deref()
            .map(|d| format!(", pinned to db '{d}'"))
            .unwrap_or_default(),
        if config.allow_writes {
            ", WRITE TOOLS ENABLED"
        } else {
            ", read-only tools"
        },
    );

    let server = McpServer::new(PoolWire::new(addr, user, password), config);

    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break, // stdin closed: the host is gone
        };
        if line.trim().is_empty() {
            continue;
        }
        if let Some(response) = server.handle_line(&line) {
            // Newline-delimited framing; serde_json never emits raw newlines.
            if writeln!(stdout, "{response}")
                .and_then(|_| stdout.flush())
                .is_err()
            {
                break; // stdout closed: nothing left to serve
            }
        }
    }
}
