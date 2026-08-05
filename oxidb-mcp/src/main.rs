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

fn main() {
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
