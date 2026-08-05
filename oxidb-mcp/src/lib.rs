//! MCP (Model Context Protocol) server for OxiDB — ADR-0024.
//!
//! A standalone stdio front end: MCP JSON-RPC on one side, OxiWire (via
//! `oxidb-client`) on the other. Nothing in `oxidb-server` is involved; every
//! tool call is a wire request the server already answers, so authorization is
//! the engine's (SCRAM + RBAC), not this process's good manners.
//!
//! The protocol subset is deliberately small — `initialize`, `ping`,
//! `tools/list`, `tools/call` — and anything outside it is answered with
//! JSON-RPC `method not found`, never silently swallowed.
//!
//! Read results are budgeted for a model's context window: finds default to
//! [`DEFAULT_LIMIT`] rows and cap at [`MAX_LIMIT`], and a trimmed result says
//! so and reports the true total. "No more" and "no more that I'll show you"
//! are different answers.

pub mod http;
pub mod rest_wire;

use serde_json::{Value, json};

/// Spec revisions this server implements, newest first. `initialize` echoes
/// the client's revision when it is one of these, else answers with the
/// newest — per spec, the client then decides whether to proceed.
pub const SUPPORTED_PROTOCOL_VERSIONS: &[&str] = &["2025-06-18", "2025-03-26", "2024-11-05"];

/// Rows returned by a read tool when the model did not ask for a limit.
pub const DEFAULT_LIMIT: u64 = 50;
/// Hard cap on rows in any tool result, whatever the model asked for.
pub const MAX_LIMIT: u64 = 500;
/// `list_collections` reports per-collection counts (one index-only `count`
/// each) only up to this many collections; past it, names only — stated.
const COUNTS_MAX_COLLECTIONS: usize = 100;

pub const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

// ---------------------------------------------------------------------------
// Wire abstraction — the real one is a `Pool`; tests substitute a fake.
// ---------------------------------------------------------------------------

pub trait Wire: Send + Sync {
    fn call(&self, request: &Value) -> Result<Value, String>;
}

pub struct PoolWire {
    pool: oxidb_client::Pool,
}

impl PoolWire {
    /// Connections are dialed lazily by the pool, so constructing this does
    /// not touch the network — `initialize`/`tools/list` work offline.
    pub fn new(addr: String, user: Option<String>, password: Option<String>) -> Self {
        Self {
            pool: oxidb_client::Pool::new(addr, user, password, 4),
        }
    }
}

impl Wire for PoolWire {
    fn call(&self, request: &Value) -> Result<Value, String> {
        self.pool
            .with(|c| c.call(request))
            .map_err(|e| e.to_string())
    }
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

pub struct Config {
    /// Pin every tool call to this database; a conflicting `db` argument is
    /// refused loudly (an agent scoped to one project must not wander).
    pub pinned_db: Option<String>,
    /// Register the write tools (`insert`/`update`/`delete`/`sql_execute`).
    /// Off by default — a model cannot call a tool it was never offered.
    pub allow_writes: bool,
}

pub struct McpServer<W: Wire> {
    wire: W,
    config: Config,
}

impl<W: Wire> McpServer<W> {
    pub fn new(wire: W, config: Config) -> Self {
        Self { wire, config }
    }

    /// Handle one newline-delimited JSON-RPC message (the stdio transport's
    /// framing). Returns the response line, or `None` for notifications.
    pub fn handle_line(&self, line: &str) -> Option<String> {
        let msg: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(e) => {
                return Some(
                    rpc_error(Value::Null, -32700, &format!("parse error: {e}")).to_string(),
                );
            }
        };
        // Pre-2025-06-18 revisions allow JSON-RPC batches.
        if let Value::Array(msgs) = msg {
            let responses: Vec<Value> = msgs
                .into_iter()
                .filter_map(|m| self.handle_message(m))
                .collect();
            if responses.is_empty() {
                return None;
            }
            return Some(Value::Array(responses).to_string());
        }
        self.handle_message(msg).map(|v| v.to_string())
    }

    fn handle_message(&self, msg: Value) -> Option<Value> {
        let id = msg.get("id").cloned();
        let method = msg.get("method").and_then(|m| m.as_str());
        let is_notification = id.is_none();

        let Some(method) = method else {
            // A message with neither method nor id is unanswerable.
            let id = id.unwrap_or(Value::Null);
            return Some(rpc_error(id, -32600, "invalid request: no method"));
        };

        // Notifications never get a response, not even an error.
        if is_notification {
            return None;
        }
        let id = id.unwrap();

        match method {
            "initialize" => {
                let requested = msg
                    .get("params")
                    .and_then(|p| p.get("protocolVersion"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let version = if SUPPORTED_PROTOCOL_VERSIONS.contains(&requested) {
                    requested
                } else {
                    SUPPORTED_PROTOCOL_VERSIONS[0]
                };
                Some(rpc_result(
                    id,
                    json!({
                        "protocolVersion": version,
                        "capabilities": { "tools": {} },
                        "serverInfo": {
                            "name": "oxidb-mcp",
                            "version": SERVER_VERSION,
                        },
                    }),
                ))
            }
            "ping" => Some(rpc_result(id, json!({}))),
            "tools/list" => Some(rpc_result(id, json!({ "tools": self.tool_defs() }))),
            "tools/call" => {
                let params = msg.get("params").cloned().unwrap_or_else(|| json!({}));
                let Some(name) = params.get("name").and_then(|n| n.as_str()) else {
                    return Some(rpc_error(id, -32602, "tools/call: missing 'name'"));
                };
                let args = params
                    .get("arguments")
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                if !self.tool_exists(name) {
                    // Per spec an unknown tool is a *protocol* error, not a
                    // tool result — and an unregistered write tool is exactly
                    // as unknown as a misspelled one.
                    return Some(rpc_error(id, -32602, &format!("unknown tool: {name}")));
                }
                match self.call_tool(name, &args) {
                    Ok(text) => Some(rpc_result(
                        id,
                        json!({ "content": [{ "type": "text", "text": text }], "isError": false }),
                    )),
                    // Execution errors go back as a tool result the model can
                    // read and correct — the server's message is the report.
                    Err(text) => Some(rpc_result(
                        id,
                        json!({ "content": [{ "type": "text", "text": text }], "isError": true }),
                    )),
                }
            }
            other => Some(rpc_error(id, -32601, &format!("method not found: {other}"))),
        }
    }

    // -----------------------------------------------------------------------
    // Tool registry
    // -----------------------------------------------------------------------

    fn tool_exists(&self, name: &str) -> bool {
        let read = matches!(
            name,
            "list_databases"
                | "list_collections"
                | "list_tables"
                | "describe_table"
                | "list_indexes"
                | "find"
                | "count"
                | "aggregate"
                | "explain"
                | "sql_query"
                | "tsdb_query"
                | "text_search"
        );
        let write = matches!(name, "insert" | "update" | "delete" | "sql_execute");
        read || (self.config.allow_writes && write)
    }

    fn tool_defs(&self) -> Vec<Value> {
        let db_prop = json!({
            "type": "string",
            "description": "Target database (optional; defaults to the session/default database)"
        });
        let mut tools = vec![
            tool(
                "list_databases",
                "List the databases on this OxiDB server.",
                json!({ "type": "object", "properties": {} }),
                true,
                false,
            ),
            tool(
                "list_collections",
                "List document collections (with document counts when few enough to count cheaply).",
                json!({ "type": "object", "properties": { "db": db_prop } }),
                true,
                false,
            ),
            tool(
                "list_tables",
                "List SQL tables (SQL engine; errors if the SQL engine is not enabled).",
                json!({ "type": "object", "properties": { "db": db_prop } }),
                true,
                false,
            ),
            tool(
                "describe_table",
                "Describe a SQL table's columns and types.",
                json!({
                    "type": "object",
                    "properties": { "table": { "type": "string" }, "db": db_prop },
                    "required": ["table"]
                }),
                true,
                false,
            ),
            tool(
                "list_indexes",
                "List indexes on a document collection OR a SQL table (pass exactly one of 'collection' / 'table').",
                json!({
                    "type": "object",
                    "properties": {
                        "collection": { "type": "string" },
                        "table": { "type": "string" },
                        "db": db_prop
                    }
                }),
                true,
                false,
            ),
            tool(
                "find",
                format!(
                    "Query a document collection with a MongoDB-style filter. Returns at most {MAX_LIMIT} documents (default {DEFAULT_LIMIT}); a trimmed result says so and reports the true total."
                ),
                json!({
                    "type": "object",
                    "properties": {
                        "collection": { "type": "string" },
                        "query": { "type": "object", "description": "Filter, e.g. {\"age\": {\"$gt\": 30}} (default: all)" },
                        "sort": { "type": "object", "description": "e.g. {\"created\": -1} (1 asc, -1 desc)" },
                        "skip": { "type": "integer" },
                        "limit": { "type": "integer", "description": format!("Max documents (default {DEFAULT_LIMIT}, cap {MAX_LIMIT})") },
                        "db": db_prop
                    },
                    "required": ["collection"]
                }),
                true,
                false,
            ),
            tool(
                "count",
                "Count documents matching a filter (index-only when possible — cheap even on large collections).",
                json!({
                    "type": "object",
                    "properties": {
                        "collection": { "type": "string" },
                        "query": { "type": "object" },
                        "db": db_prop
                    },
                    "required": ["collection"]
                }),
                true,
                false,
            ),
            tool(
                "aggregate",
                format!(
                    "Run an aggregation pipeline ($match, $group, $sort, $lookup, $unwind, $facet, window functions, …). Output rows are capped at {MAX_LIMIT}, stated when trimmed."
                ),
                json!({
                    "type": "object",
                    "properties": {
                        "collection": { "type": "string" },
                        "pipeline": { "type": "array", "description": "Aggregation stages, e.g. [{\"$match\": …}, {\"$group\": …}]" },
                        "db": db_prop
                    },
                    "required": ["collection", "pipeline"]
                }),
                true,
                false,
            ),
            tool(
                "explain",
                "Explain a find/count/aggregate: the query plan (strategy, index used, examined/returned, post-filter operators) plus real run timing. Use it to diagnose slow queries before reaching for an index.",
                json!({
                    "type": "object",
                    "properties": {
                        "collection": { "type": "string" },
                        "command": { "type": "string", "enum": ["find", "count", "aggregate"] },
                        "query": { "type": "object" },
                        "pipeline": { "type": "array" },
                        "sort": { "type": "object" },
                        "skip": { "type": "integer" },
                        "limit": { "type": "integer" },
                        "db": db_prop
                    },
                    "required": ["collection", "command"]
                }),
                true,
                false,
            ),
            tool(
                "sql_query",
                "Run a read-only SQL statement (SELECT / SHOW / DESCRIBE / EXPLAIN / WITH / VALUES) against the SQL engine. Bind values with ? placeholders and 'params'.",
                json!({
                    "type": "object",
                    "properties": {
                        "sql": { "type": "string" },
                        "params": { "type": "array", "description": "Binds ? / $N placeholders left-to-right" },
                        "db": db_prop
                    },
                    "required": ["sql"]
                }),
                true,
                false,
            ),
            tool(
                "tsdb_query",
                "Query the time-series engine: a measurement's field over [start,end) ms, optionally downsampled (interval ms + agg: mean/sum/min/max/count/first/last/rate/percentile) and grouped by tags.",
                json!({
                    "type": "object",
                    "properties": {
                        "measurement": { "type": "string" },
                        "field": { "type": "string" },
                        "tags": { "type": "object", "description": "Tag equality filters, e.g. {\"host\": \"a\"}" },
                        "start": { "type": "integer", "description": "Epoch ms, inclusive" },
                        "end": { "type": "integer", "description": "Epoch ms, exclusive" },
                        "interval": { "type": "integer", "description": "Downsample bucket width in ms" },
                        "group_by": { "type": "array", "items": { "type": "string" } },
                        "agg": { "type": "string" },
                        "p": { "type": "number", "description": "Percentile (with agg: \"percentile\")" },
                        "db": db_prop
                    },
                    "required": ["measurement", "field"]
                }),
                true,
                false,
            ),
            tool(
                "text_search",
                "BM25 full-text search over a collection's text index. Errors (rather than returning empty) if the collection has no text index.",
                json!({
                    "type": "object",
                    "properties": {
                        "collection": { "type": "string" },
                        "query": { "type": "string" },
                        "limit": { "type": "integer", "description": format!("Max results (default 10, cap {MAX_LIMIT})") },
                        "highlight": { "type": "boolean", "description": "Include <mark> snippets" },
                        "db": db_prop
                    },
                    "required": ["collection", "query"]
                }),
                true,
                false,
            ),
        ];

        if self.config.allow_writes {
            tools.push(tool(
                "insert",
                "Insert one document ('doc') or many ('docs') into a collection (auto-created on first insert).",
                json!({
                    "type": "object",
                    "properties": {
                        "collection": { "type": "string" },
                        "doc": { "type": "object" },
                        "docs": { "type": "array" },
                        "db": db_prop
                    },
                    "required": ["collection"]
                }),
                false, false,
            ));
            tools.push(tool(
                "update",
                "Update documents matching 'query' with update operators ($set, $inc, $push, …).",
                json!({
                    "type": "object",
                    "properties": {
                        "collection": { "type": "string" },
                        "query": { "type": "object" },
                        "update": { "type": "object" },
                        "db": db_prop
                    },
                    "required": ["collection", "query", "update"]
                }),
                false,
                true,
            ));
            tools.push(tool(
                "delete",
                "Delete documents matching 'query'. The query must be non-empty — deleting a whole collection is not a tool call away.",
                json!({
                    "type": "object",
                    "properties": {
                        "collection": { "type": "string" },
                        "query": { "type": "object" },
                        "db": db_prop
                    },
                    "required": ["collection", "query"]
                }),
                false, true,
            ));
            tools.push(tool(
                "sql_execute",
                "Run a SQL statement that writes (INSERT / UPDATE / DELETE / DDL). Bind values with ? placeholders and 'params'.",
                json!({
                    "type": "object",
                    "properties": {
                        "sql": { "type": "string" },
                        "params": { "type": "array" },
                        "db": db_prop
                    },
                    "required": ["sql"]
                }),
                false, true,
            ));
        }
        tools
    }

    // -----------------------------------------------------------------------
    // Tool dispatch
    // -----------------------------------------------------------------------

    fn call_tool(&self, name: &str, args: &Value) -> Result<String, String> {
        match name {
            "list_databases" => self.list_databases(),
            "list_collections" => self.list_collections(args),
            "list_tables" => self.sql_passthrough(args, "SHOW TABLES", None),
            "describe_table" => {
                let table = req_ident(args, "table")?;
                self.sql_passthrough(args, &format!("DESCRIBE {table}"), None)
            }
            "list_indexes" => self.list_indexes(args),
            "find" => self.find(args),
            "count" => self.count(args),
            "aggregate" => self.aggregate(args),
            "explain" => self.explain(args),
            "sql_query" => {
                let sql = req_str(args, "sql")?;
                ensure_read_only_sql(sql)?;
                self.sql_passthrough(args, sql, args.get("params"))
            }
            "tsdb_query" => self.tsdb_query(args),
            "text_search" => self.text_search(args),
            "insert" => self.insert(args),
            "update" => self.update(args),
            "delete" => self.delete(args),
            "sql_execute" => {
                let sql = req_str(args, "sql")?;
                self.sql_passthrough(args, sql, args.get("params"))
            }
            other => Err(format!("unknown tool: {other}")),
        }
    }

    /// Resolve the request's database: the pinned one wins, and a conflicting
    /// explicit `db` is refused loudly rather than silently overridden.
    fn resolve_db(&self, args: &Value) -> Result<Option<String>, String> {
        let asked = args.get("db").and_then(|v| v.as_str());
        match (&self.config.pinned_db, asked) {
            (Some(pinned), Some(db)) if pinned != db => Err(format!(
                "this MCP server is pinned to database '{pinned}' (OXIDB_MCP_DB); omit 'db'"
            )),
            (Some(pinned), _) => Ok(Some(pinned.clone())),
            (None, Some(db)) => Ok(Some(db.to_string())),
            (None, None) => Ok(None),
        }
    }

    /// Build a wire request with the resolved `db` attached.
    fn wire_request(&self, args: &Value, mut req: Value) -> Result<Value, String> {
        if let Some(db) = self.resolve_db(args)? {
            req["db"] = json!(db);
        }
        Ok(req)
    }

    fn list_databases(&self) -> Result<String, String> {
        let data = self.wire.call(&json!({ "cmd": "list_databases" }))?;
        Ok(compact(&data))
    }

    fn list_collections(&self, args: &Value) -> Result<String, String> {
        let req = self.wire_request(args, json!({ "cmd": "list_collections" }))?;
        let data = self.wire.call(&req)?;
        let names: Vec<String> = data
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        if names.len() > COUNTS_MAX_COLLECTIONS {
            return Ok(compact(&json!({
                "collections": names,
                "note": format!(
                    "{} collections — counts skipped (would be {} count calls); use the count tool per collection",
                    names.len(), names.len()
                ),
            })));
        }
        let mut out = Vec::with_capacity(names.len());
        for name in &names {
            let creq = self.wire_request(
                args,
                json!({ "cmd": "count", "collection": name, "query": {} }),
            )?;
            let count = self
                .wire
                .call(&creq)
                .ok()
                .and_then(|d| d.get("count").cloned());
            out.push(json!({ "name": name, "count": count }));
        }
        Ok(compact(&json!({ "collections": out })))
    }

    fn list_indexes(&self, args: &Value) -> Result<String, String> {
        let collection = args.get("collection").and_then(|v| v.as_str());
        let table = args.get("table").and_then(|v| v.as_str());
        match (collection, table) {
            (Some(col), None) => {
                let req =
                    self.wire_request(args, json!({ "cmd": "list_indexes", "collection": col }))?;
                let data = self.wire.call(&req)?;
                Ok(compact(
                    &json!({ "engine": "document", "collection": col, "indexes": data }),
                ))
            }
            (None, Some(_)) => {
                let table = req_ident(args, "table")?;
                self.sql_passthrough(args, &format!("SHOW INDEXES FROM {table}"), None)
            }
            _ => Err(
                "pass exactly one of 'collection' (document engine) or 'table' (SQL engine)".into(),
            ),
        }
    }

    fn find(&self, args: &Value) -> Result<String, String> {
        let col = req_str(args, "collection")?;
        let query = args.get("query").cloned().unwrap_or_else(|| json!({}));
        let limit = effective_limit(args)?;
        let mut req = json!({ "cmd": "find", "collection": col, "query": query, "limit": limit });
        for key in ["sort", "skip"] {
            if let Some(v) = args.get(key) {
                req[key] = v.clone();
            }
        }
        let req = self.wire_request(args, req)?;
        let data = self.wire.call(&req)?;
        let docs = data.as_array().cloned().unwrap_or_default();
        let returned = docs.len() as u64;
        let mut out = json!({ "documents": docs, "returned": returned });
        // A full page means there may be more. The total is one index-only
        // count away, so a trimmed result names it instead of implying
        // "that was everything".
        if returned >= limit {
            let creq = self
                .wire_request(args, json!({ "cmd": "count", "collection": col, "query": args.get("query").cloned().unwrap_or_else(|| json!({})) }))?;
            let total = self
                .wire
                .call(&creq)?
                .get("count")
                .cloned()
                .unwrap_or(Value::Null);
            out["truncated"] = json!(true);
            out["total"] = total;
            out["note"] = json!(format!(
                "showing {returned} of the total; refine the query, or raise 'limit' (cap {MAX_LIMIT}) / use 'skip' to page"
            ));
        }
        Ok(compact(&out))
    }

    fn count(&self, args: &Value) -> Result<String, String> {
        let col = req_str(args, "collection")?;
        let query = args.get("query").cloned().unwrap_or_else(|| json!({}));
        let req = self.wire_request(
            args,
            json!({ "cmd": "count", "collection": col, "query": query }),
        )?;
        Ok(compact(&self.wire.call(&req)?))
    }

    fn aggregate(&self, args: &Value) -> Result<String, String> {
        let col = req_str(args, "collection")?;
        let pipeline = args.get("pipeline").ok_or("missing 'pipeline'")?.clone();
        let req = self.wire_request(
            args,
            json!({ "cmd": "aggregate", "collection": col, "pipeline": pipeline }),
        )?;
        let data = self.wire.call(&req)?;
        let rows = data.as_array().cloned().unwrap_or_default();
        let total = rows.len() as u64;
        if total > MAX_LIMIT {
            let shown: Vec<Value> = rows.into_iter().take(MAX_LIMIT as usize).collect();
            return Ok(compact(&json!({
                "rows": shown,
                "returned": MAX_LIMIT,
                "truncated": true,
                "total": total,
                "note": format!("showing {MAX_LIMIT} of {total} result rows; add a $limit / tighter $match stage"),
            })));
        }
        Ok(compact(&json!({ "rows": rows, "returned": total })))
    }

    fn explain(&self, args: &Value) -> Result<String, String> {
        let col = req_str(args, "collection")?;
        let command = req_str(args, "command")?;
        let mut inner = json!({ "cmd": command, "collection": col });
        match command {
            "find" | "count" => {
                inner["query"] = args.get("query").cloned().unwrap_or_else(|| json!({}));
                for key in ["sort", "skip", "limit"] {
                    if let Some(v) = args.get(key) {
                        inner[key] = v.clone();
                    }
                }
            }
            "aggregate" => {
                inner["pipeline"] = args.get("pipeline").ok_or("missing 'pipeline'")?.clone();
            }
            other => {
                return Err(format!(
                    "command must be find|count|aggregate, got '{other}'"
                ));
            }
        }
        let req = self.wire_request(args, json!({ "cmd": "explain", "inner": inner }))?;
        Ok(compact(&self.wire.call(&req)?))
    }

    fn sql_passthrough(
        &self,
        args: &Value,
        sql: &str,
        params: Option<&Value>,
    ) -> Result<String, String> {
        let mut req = json!({ "engine": "sql", "cmd": "sql", "sql": sql });
        if let Some(p) = params {
            req["params"] = p.clone();
        }
        let req = self.wire_request(args, req)?;
        Ok(compact(&self.wire.call(&req)?))
    }

    fn tsdb_query(&self, args: &Value) -> Result<String, String> {
        let measurement = req_str(args, "measurement")?;
        let field = req_str(args, "field")?;
        let mut req = json!({
            "engine": "tsdb", "cmd": "tsdb", "op": "query",
            "measurement": measurement, "field": field,
        });
        for key in ["tags", "start", "end", "interval", "group_by", "agg", "p"] {
            if let Some(v) = args.get(key) {
                req[key] = v.clone();
            }
        }
        let req = self.wire_request(args, req)?;
        Ok(compact(&self.wire.call(&req)?))
    }

    fn text_search(&self, args: &Value) -> Result<String, String> {
        let col = req_str(args, "collection")?;
        let query = req_str(args, "query")?;
        let limit = args
            .get("limit")
            .and_then(|v| v.as_u64())
            .unwrap_or(10)
            .min(MAX_LIMIT);
        let mut req =
            json!({ "cmd": "text_search", "collection": col, "query": query, "limit": limit });
        if args.get("highlight").and_then(|v| v.as_bool()) == Some(true) {
            req["highlight"] = json!(true);
        }
        let req = self.wire_request(args, req)?;
        Ok(compact(&self.wire.call(&req)?))
    }

    fn insert(&self, args: &Value) -> Result<String, String> {
        let col = req_str(args, "collection")?;
        let req = match (args.get("doc"), args.get("docs")) {
            (Some(doc), None) => json!({ "cmd": "insert", "collection": col, "doc": doc }),
            (None, Some(docs)) => json!({ "cmd": "insert_many", "collection": col, "docs": docs }),
            _ => return Err("pass exactly one of 'doc' or 'docs'".into()),
        };
        let req = self.wire_request(args, req)?;
        Ok(compact(&self.wire.call(&req)?))
    }

    fn update(&self, args: &Value) -> Result<String, String> {
        let col = req_str(args, "collection")?;
        let query = args.get("query").ok_or("missing 'query'")?.clone();
        let update = args.get("update").ok_or("missing 'update'")?.clone();
        let req = self.wire_request(
            args,
            json!({ "cmd": "update", "collection": col, "query": query, "update": update }),
        )?;
        Ok(compact(&self.wire.call(&req)?))
    }

    fn delete(&self, args: &Value) -> Result<String, String> {
        let col = req_str(args, "collection")?;
        let query = args.get("query").ok_or("missing 'query'")?.clone();
        // An empty filter is "delete everything" — that stays a deliberate
        // act done through a real client, not a tool call a model reaches.
        if query.as_object().is_none_or(|o| o.is_empty()) {
            return Err(
                "'query' must be a non-empty filter; deleting all documents is refused".into(),
            );
        }
        let req = self.wire_request(
            args,
            json!({ "cmd": "delete", "collection": col, "query": query }),
        )?;
        Ok(compact(&self.wire.call(&req)?))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn tool(
    name: &str,
    description: impl Into<String>,
    input_schema: Value,
    read_only: bool,
    destructive: bool,
) -> Value {
    json!({
        "name": name,
        "description": description.into(),
        "inputSchema": input_schema,
        "annotations": { "readOnlyHint": read_only, "destructiveHint": destructive },
    })
}

fn rpc_result(id: Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

fn rpc_error(id: Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

fn compact(v: &Value) -> String {
    serde_json::to_string(v).unwrap_or_else(|_| "null".into())
}

fn req_str<'a>(args: &'a Value, key: &str) -> Result<&'a str, String> {
    args.get(key)
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("missing '{key}'"))
}

/// A SQL identifier this process will splice into statement text. Same guard
/// as the PostgREST surface: names are validated, values are always bound.
fn req_ident<'a>(args: &'a Value, key: &str) -> Result<&'a str, String> {
    let s = req_str(args, key)?;
    let mut chars = s.chars();
    let head_ok = chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_');
    if head_ok && chars.all(|c| c.is_ascii_alphanumeric() || c == '_') {
        Ok(s)
    } else {
        Err(format!("'{key}' is not a valid identifier: {s:?}"))
    }
}

fn effective_limit(args: &Value) -> Result<u64, String> {
    match args.get("limit") {
        None => Ok(DEFAULT_LIMIT),
        Some(v) => {
            let n = v.as_u64().ok_or("'limit' must be a non-negative integer")?;
            if n == 0 {
                return Err("'limit' must be at least 1".into());
            }
            Ok(n.min(MAX_LIMIT))
        }
    }
}

/// Advisory read-only gate for `sql_query` — defense in depth on top of the
/// engine's role gate (a Read account is refused writes server-side no matter
/// what this passes through). One statement, leading keyword allowlisted.
fn ensure_read_only_sql(sql: &str) -> Result<(), String> {
    if has_statement_separator(sql) {
        return Err("sql_query takes a single statement (no ';' between statements)".into());
    }
    let first = sql
        .trim_start()
        .split(|c: char| c.is_whitespace() || c == '(')
        .next()
        .unwrap_or("")
        .to_ascii_uppercase();
    match first.as_str() {
        "SELECT" | "SHOW" | "DESCRIBE" | "DESC" | "EXPLAIN" | "WITH" | "VALUES" | "TABLE" => Ok(()),
        other => Err(format!(
            "sql_query is read-only ('{other}' refused); writes go through sql_execute, which requires OXIDB_MCP_WRITES=1"
        )),
    }
}

/// Quote-aware scan for a `;` that separates statements (a `;` inside a
/// string literal is data). A trailing `;` is tolerated.
fn has_statement_separator(sql: &str) -> bool {
    let mut in_single = false;
    let mut in_double = false;
    let mut chars = sql.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        match c {
            '\'' if !in_double => {
                // '' inside a single-quoted string is an escaped quote.
                if in_single && chars.peek().is_some_and(|&(_, n)| n == '\'') {
                    chars.next();
                } else {
                    in_single = !in_single;
                }
            }
            '"' if !in_single => in_double = !in_double,
            ';' if !in_single && !in_double => {
                if !sql[i + 1..].trim().is_empty() {
                    return true;
                }
            }
            _ => {}
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Records requests; answers from a scripted queue (or a fixed default).
    struct FakeWire {
        responses: Mutex<Vec<Result<Value, String>>>,
        requests: Mutex<Vec<Value>>,
    }

    impl FakeWire {
        fn scripted(responses: Vec<Result<Value, String>>) -> Self {
            // Popped from the back; store reversed so scripts read in order.
            let mut r = responses;
            r.reverse();
            Self {
                responses: Mutex::new(r),
                requests: Mutex::new(Vec::new()),
            }
        }
        fn requests(&self) -> Vec<Value> {
            self.requests.lock().unwrap().clone()
        }
    }

    impl Wire for FakeWire {
        fn call(&self, request: &Value) -> Result<Value, String> {
            self.requests.lock().unwrap().push(request.clone());
            self.responses
                .lock()
                .unwrap()
                .pop()
                .unwrap_or(Ok(Value::Null))
        }
    }

    fn server(responses: Vec<Result<Value, String>>, config: Config) -> McpServer<FakeWire> {
        McpServer::new(FakeWire::scripted(responses), config)
    }

    fn read_only() -> Config {
        Config {
            pinned_db: None,
            allow_writes: false,
        }
    }

    fn rpc(server: &McpServer<FakeWire>, msg: Value) -> Value {
        serde_json::from_str(
            &server
                .handle_line(&msg.to_string())
                .expect("expected a response"),
        )
        .unwrap()
    }

    fn call_tool(server: &McpServer<FakeWire>, name: &str, args: Value) -> Value {
        rpc(
            server,
            json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/call",
                    "params": { "name": name, "arguments": args } }),
        )
    }

    /// Parse the JSON text out of a tool result.
    fn tool_json(resp: &Value) -> (Value, bool) {
        let result = &resp["result"];
        let text = result["content"][0]["text"].as_str().unwrap();
        let is_error = result["isError"].as_bool().unwrap();
        (
            serde_json::from_str(text).unwrap_or(Value::String(text.into())),
            is_error,
        )
    }

    // ── Protocol ───────────────────────────────────────────────────────────

    #[test]
    fn initialize_echoes_a_known_version_and_offers_latest_for_unknown() {
        let s = server(vec![], read_only());
        let resp = rpc(
            &s,
            json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": { "protocolVersion": "2025-03-26" } }),
        );
        assert_eq!(resp["result"]["protocolVersion"], "2025-03-26");
        assert_eq!(resp["result"]["serverInfo"]["name"], "oxidb-mcp");

        let resp = rpc(
            &s,
            json!({ "jsonrpc": "2.0", "id": 2, "method": "initialize",
            "params": { "protocolVersion": "2099-01-01" } }),
        );
        assert_eq!(
            resp["result"]["protocolVersion"],
            SUPPORTED_PROTOCOL_VERSIONS[0]
        );
    }

    #[test]
    fn notifications_get_no_response_even_unknown_ones() {
        let s = server(vec![], read_only());
        assert!(
            s.handle_line(
                &json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }).to_string()
            )
            .is_none()
        );
        assert!(
            s.handle_line(
                &json!({ "jsonrpc": "2.0", "method": "notifications/cancelled" }).to_string()
            )
            .is_none()
        );
    }

    #[test]
    fn unknown_method_is_32601_and_parse_error_is_32700() {
        let s = server(vec![], read_only());
        let resp = rpc(
            &s,
            json!({ "jsonrpc": "2.0", "id": 1, "method": "resources/list" }),
        );
        assert_eq!(resp["error"]["code"], -32601);
        let resp: Value = serde_json::from_str(&s.handle_line("{not json").unwrap()).unwrap();
        assert_eq!(resp["error"]["code"], -32700);
    }

    #[test]
    fn ping_answers_empty_object() {
        let s = server(vec![], read_only());
        let resp = rpc(&s, json!({ "jsonrpc": "2.0", "id": 7, "method": "ping" }));
        assert_eq!(resp["result"], json!({}));
    }

    #[test]
    fn batch_requests_are_answered_as_a_batch() {
        let s = server(vec![], read_only());
        let line = json!([
            { "jsonrpc": "2.0", "id": 1, "method": "ping" },
            { "jsonrpc": "2.0", "method": "notifications/initialized" },
            { "jsonrpc": "2.0", "id": 2, "method": "ping" },
        ]);
        let resp: Value = serde_json::from_str(&s.handle_line(&line.to_string()).unwrap()).unwrap();
        let arr = resp.as_array().unwrap();
        assert_eq!(arr.len(), 2, "notification contributes no response");
    }

    // ── Tool registry / write gating ───────────────────────────────────────

    fn tool_names(s: &McpServer<FakeWire>) -> Vec<String> {
        let resp = rpc(
            s,
            json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }),
        );
        resp["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap().to_string())
            .collect()
    }

    #[test]
    fn write_tools_are_absent_without_the_flag_and_present_with_it() {
        let names = tool_names(&server(vec![], read_only()));
        for w in ["insert", "update", "delete", "sql_execute"] {
            assert!(
                !names.contains(&w.to_string()),
                "{w} must not be offered read-only"
            );
        }
        assert!(names.contains(&"find".to_string()));
        assert!(names.contains(&"explain".to_string()));

        let names = tool_names(&server(
            vec![],
            Config {
                pinned_db: None,
                allow_writes: true,
            },
        ));
        for w in ["insert", "update", "delete", "sql_execute"] {
            assert!(names.contains(&w.to_string()));
        }
    }

    #[test]
    fn calling_an_unregistered_write_tool_is_a_protocol_error_not_a_tool_result() {
        let s = server(vec![], read_only());
        let resp = call_tool(
            &s,
            "delete",
            json!({ "collection": "c", "query": { "a": 1 } }),
        );
        assert_eq!(resp["error"]["code"], -32602);
        assert!(s.wire.requests().is_empty(), "nothing may reach the wire");
    }

    #[test]
    fn every_tool_schema_is_an_object_schema_with_annotations() {
        let s = server(
            vec![],
            Config {
                pinned_db: None,
                allow_writes: true,
            },
        );
        let resp = rpc(
            &s,
            json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }),
        );
        for t in resp["result"]["tools"].as_array().unwrap() {
            assert_eq!(t["inputSchema"]["type"], "object", "{}", t["name"]);
            assert!(t["annotations"]["readOnlyHint"].is_boolean());
            assert!(!t["description"].as_str().unwrap().is_empty());
        }
    }

    // ── Read tools ─────────────────────────────────────────────────────────

    #[test]
    fn find_defaults_the_limit_and_reports_the_total_when_the_page_is_full() {
        let docs: Vec<Value> = (0..DEFAULT_LIMIT).map(|i| json!({ "i": i })).collect();
        let s = server(
            vec![Ok(json!(docs)), Ok(json!({ "count": 1234 }))],
            read_only(),
        );
        let resp = call_tool(&s, "find", json!({ "collection": "c" }));
        let (body, is_error) = tool_json(&resp);
        assert!(!is_error);
        assert_eq!(body["returned"], DEFAULT_LIMIT);
        assert_eq!(body["truncated"], true);
        assert_eq!(body["total"], 1234);
        let reqs = s.wire.requests();
        assert_eq!(
            reqs[0]["limit"], DEFAULT_LIMIT,
            "default limit goes to the wire"
        );
        assert_eq!(
            reqs[1]["cmd"], "count",
            "total comes from an index-only count"
        );
    }

    #[test]
    fn find_below_the_limit_reports_no_truncation_and_skips_the_count() {
        let s = server(vec![Ok(json!([{ "a": 1 }]))], read_only());
        let resp = call_tool(&s, "find", json!({ "collection": "c" }));
        let (body, _) = tool_json(&resp);
        assert_eq!(body["returned"], 1);
        assert!(body.get("truncated").is_none());
        assert_eq!(s.wire.requests().len(), 1, "no count call");
    }

    #[test]
    fn find_caps_an_oversized_limit_at_max() {
        let s = server(vec![Ok(json!([]))], read_only());
        call_tool(&s, "find", json!({ "collection": "c", "limit": 100000 }));
        assert_eq!(s.wire.requests()[0]["limit"], MAX_LIMIT);
    }

    #[test]
    fn aggregate_truncates_oversized_results_and_says_so() {
        let rows: Vec<Value> = (0..MAX_LIMIT + 200).map(|i| json!({ "i": i })).collect();
        let s = server(vec![Ok(json!(rows))], read_only());
        let (body, is_error) = tool_json(&call_tool(
            &s,
            "aggregate",
            json!({ "collection": "c", "pipeline": [{ "$match": {} }] }),
        ));
        assert!(!is_error);
        assert_eq!(body["returned"], MAX_LIMIT);
        assert_eq!(body["truncated"], true);
        assert_eq!(body["total"], MAX_LIMIT + 200);
        assert_eq!(body["rows"].as_array().unwrap().len(), MAX_LIMIT as usize);
    }

    #[test]
    fn explain_builds_the_inner_request() {
        let s = server(vec![Ok(json!({ "strategy": "index" }))], read_only());
        call_tool(
            &s,
            "explain",
            json!({
            "collection": "c", "command": "find", "query": { "a": 1 }, "limit": 5 }),
        );
        let req = &s.wire.requests()[0];
        assert_eq!(req["cmd"], "explain");
        assert_eq!(req["inner"]["cmd"], "find");
        assert_eq!(req["inner"]["query"], json!({ "a": 1 }));
        assert_eq!(req["inner"]["limit"], 5);
    }

    #[test]
    fn wire_errors_come_back_as_tool_errors_the_model_can_read() {
        let s = server(
            vec![Err("no text index on collection 'c'".into())],
            read_only(),
        );
        let resp = call_tool(
            &s,
            "text_search",
            json!({ "collection": "c", "query": "q" }),
        );
        let (body, is_error) = tool_json(&resp);
        assert!(is_error);
        assert!(body.as_str().unwrap().contains("no text index"));
    }

    #[test]
    fn list_collections_attaches_counts() {
        let s = server(
            vec![
                Ok(json!(["users", "orders"])),
                Ok(json!({ "count": 3 })),
                Ok(json!({ "count": 7 })),
            ],
            read_only(),
        );
        let (body, _) = tool_json(&call_tool(&s, "list_collections", json!({})));
        assert_eq!(
            body["collections"][0],
            json!({ "name": "users", "count": 3 })
        );
        assert_eq!(
            body["collections"][1],
            json!({ "name": "orders", "count": 7 })
        );
    }

    #[test]
    fn list_indexes_requires_exactly_one_target() {
        let s = server(vec![], read_only());
        let (_, is_error) = tool_json(&call_tool(&s, "list_indexes", json!({})));
        assert!(is_error);
        let (_, is_error) = tool_json(&call_tool(
            &s,
            "list_indexes",
            json!({ "collection": "c", "table": "t" }),
        ));
        assert!(is_error);
    }

    #[test]
    fn tsdb_query_builds_the_op_request() {
        let s = server(vec![Ok(json!({ "series": [] }))], read_only());
        call_tool(
            &s,
            "tsdb_query",
            json!({
            "measurement": "cpu", "field": "usage", "interval": 60000, "agg": "mean" }),
        );
        let req = &s.wire.requests()[0];
        assert_eq!(req["engine"], "tsdb");
        assert_eq!(req["op"], "query");
        assert_eq!(req["interval"], 60000);
    }

    // ── SQL guards ─────────────────────────────────────────────────────────

    #[test]
    fn sql_query_refuses_writes_and_multi_statements_but_allows_literal_semicolons() {
        assert!(ensure_read_only_sql("SELECT * FROM t").is_ok());
        assert!(ensure_read_only_sql("  with x as (select 1) select * from x").is_ok());
        assert!(
            ensure_read_only_sql("SELECT 1;").is_ok(),
            "trailing ; is fine"
        );
        assert!(ensure_read_only_sql("SELECT * FROM t WHERE name = 'a;b'").is_ok());
        assert!(ensure_read_only_sql("SELECT * FROM t WHERE note = 'it''s; fine'").is_ok());

        let err = ensure_read_only_sql("INSERT INTO t VALUES (1)").unwrap_err();
        assert!(
            err.contains("sql_execute"),
            "refusal names the way to do it: {err}"
        );
        assert!(ensure_read_only_sql("DROP TABLE t").is_err());
        assert!(ensure_read_only_sql("SELECT 1; DROP TABLE t").is_err());
    }

    #[test]
    fn identifiers_are_validated_before_splicing_into_sql() {
        let s = server(vec![], read_only());
        let (body, is_error) = tool_json(&call_tool(
            &s,
            "describe_table",
            json!({ "table": "users; DROP TABLE users" }),
        ));
        assert!(is_error);
        assert!(body.as_str().unwrap().contains("not a valid identifier"));
        assert!(s.wire.requests().is_empty());
    }

    // ── db pinning / writes ────────────────────────────────────────────────

    #[test]
    fn pinned_db_is_attached_and_a_conflicting_db_is_refused() {
        let cfg = Config {
            pinned_db: Some("proj1".into()),
            allow_writes: false,
        };
        let s = server(vec![Ok(json!([]))], cfg);
        call_tool(&s, "find", json!({ "collection": "c" }));
        assert_eq!(s.wire.requests()[0]["db"], "proj1");

        let (body, is_error) = tool_json(&call_tool(
            &s,
            "find",
            json!({ "collection": "c", "db": "other" }),
        ));
        assert!(is_error);
        assert!(body.as_str().unwrap().contains("pinned"));
    }

    #[test]
    fn delete_refuses_an_empty_filter() {
        let cfg = Config {
            pinned_db: None,
            allow_writes: true,
        };
        let s = server(vec![], cfg);
        let (body, is_error) = tool_json(&call_tool(
            &s,
            "delete",
            json!({ "collection": "c", "query": {} }),
        ));
        assert!(is_error);
        assert!(body.as_str().unwrap().contains("non-empty"));
        assert!(s.wire.requests().is_empty());
    }

    #[test]
    fn insert_takes_doc_or_docs_but_not_both() {
        let cfg = Config {
            pinned_db: None,
            allow_writes: true,
        };
        let s = server(vec![Ok(json!({ "id": 1 })), Ok(json!([1, 2]))], cfg);
        call_tool(
            &s,
            "insert",
            json!({ "collection": "c", "doc": { "a": 1 } }),
        );
        call_tool(
            &s,
            "insert",
            json!({ "collection": "c", "docs": [{ "a": 1 }, { "b": 2 }] }),
        );
        let reqs = s.wire.requests();
        assert_eq!(reqs[0]["cmd"], "insert");
        assert_eq!(reqs[1]["cmd"], "insert_many");
        let (_, is_error) = tool_json(&call_tool(
            &s,
            "insert",
            json!({ "collection": "c", "doc": {}, "docs": [] }),
        ));
        assert!(is_error);
    }
}
