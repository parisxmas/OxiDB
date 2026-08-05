#!/usr/bin/env python3
"""Live e2e for the HTTP (hosted) MCP transport — ADR-0024 Phase 2.

A real oxidb-server with its REST listener on, a real oxidb-mcp in HTTP mode
in front of it, driven with plain HTTP the way an MCP host would. Not run by
`cargo test` — opt-in:

    cargo build -p oxidb-server -p oxidb-mcp
    OXIDB_ADDR=127.0.0.1:14481 OXIDB_HTTP_PORT=14482 OXIDB_DATA=/tmp/mcp-http \
        OXIDB_SQL=1 OXIDB_TSDB=1 target/debug/oxidb-server &
    OXIDB_MCP_HTTP_PORT=14490 OXIDB_MCP_UPSTREAM=http://127.0.0.1:14482 \
        target/debug/oxidb-mcp &
    python3 oxidb-mcp/tests/http_e2e.py
"""
import json, socket, struct, sys, time, urllib.request, urllib.error

WIRE = ("127.0.0.1", 14481)
MCP = "http://127.0.0.1:14490"
DB = "oxidb"

def wire_call(req):
    s = socket.create_connection(WIRE, timeout=10)
    p = json.dumps(req).encode()
    s.sendall(struct.pack("<I", len(p)) + p)
    hdr = b""
    while len(hdr) < 4:
        hdr += s.recv(4 - len(hdr))
    n = struct.unpack("<I", hdr)[0]
    buf = b""
    while len(buf) < n:
        buf += s.recv(n - len(buf))
    s.close()
    return json.loads(buf)

def rpc(method, params=None, path=f"/mcp/{DB}", token=None, raw=None):
    body = raw if raw is not None else json.dumps(
        {"jsonrpc": "2.0", "id": 1, "method": method, **({"params": params} if params else {})})
    req = urllib.request.Request(MCP + path, data=body.encode(),
                                 headers={"Content-Type": "application/json",
                                          **({"Authorization": f"Bearer {token}"} if token else {})},
                                 method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            raw_body = r.read()
            return r.status, (json.loads(raw_body) if raw_body else None)
    except urllib.error.HTTPError as e:
        raw_body = e.read()
        return e.code, (json.loads(raw_body) if raw_body else None)

def tool(name, args=None):
    st, r = rpc("tools/call", {"name": name, "arguments": args or {}})
    if r and "error" in r:
        return "PROTOCOL_ERROR", r["error"]
    res = r["result"]
    text = res["content"][0]["text"]
    try:
        body = json.loads(text)
    except json.JSONDecodeError:
        body = text
    return ("ERR" if res["isError"] else "OK"), body

failures = []
def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" — {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(name)

# wait for both
for _ in range(120):
    try:
        wire_call({"cmd": "ping"}); break
    except OSError:
        time.sleep(0.25)
else:
    sys.exit("server never came up")
for _ in range(120):
    try:
        urllib.request.urlopen(MCP + "/mcp/health", timeout=5); break
    except Exception:
        time.sleep(0.25)
else:
    sys.exit("mcp http never came up")

# Seed through the wire (independent of the path under test).
wire_call({"cmd": "delete", "collection": "widgets", "query": {}})
for i in range(12):
    wire_call({"cmd": "insert", "collection": "widgets",
               "doc": {"name": f"w{i}", "price": 10 + i, "kind": "bolt" if i % 2 else "nut"}})
wire_call({"engine": "sql", "cmd": "sql", "sql": "DROP TABLE IF EXISTS parts"})
wire_call({"engine": "sql", "cmd": "sql",
           "sql": "CREATE TABLE parts (id INT PRIMARY KEY, label VARCHAR(30), qty INT)"})
wire_call({"engine": "sql", "cmd": "sql",
           "sql": "INSERT INTO parts VALUES (1,'hex',5),(2,'flat',9)"})

print("== HTTP transport ==")
st, r = rpc("initialize", {"protocolVersion": "2025-06-18", "capabilities": {},
                           "clientInfo": {"name": "http-e2e", "version": "0"}})
check("initialize over HTTP", st == 200 and r["result"]["protocolVersion"] == "2025-06-18", r)

st, r = rpc("tools/list")
names = [t["name"] for t in r["result"]["tools"]]
check("tools/list", st == 200 and "find" in names and "insert" not in names, names)

st, r = rpc(None, raw='{"jsonrpc":"2.0","method":"notifications/initialized"}')
check("notification → 2xx, no body", 200 <= st < 300 and r is None, (st, r))

st, r = rpc("resources/list")
check("unknown method → -32601", r["error"]["code"] == -32601, r)

print("== tools through REST ==")
st, body = tool("list_collections")
cols = {c["name"]: c["count"] for c in body.get("collections", [])} if st == "OK" else {}
check("list_collections (+counts)", st == "OK" and cols.get("widgets") == 12, body)

st, body = tool("find", {"collection": "widgets", "query": {"kind": "nut"}})
check("find with filter", st == "OK" and body["returned"] == 6
      and all(d["kind"] == "nut" for d in body["documents"]), body)

st, body = tool("find", {"collection": "widgets", "limit": 5})
check("find truncates + reports true total", st == "OK" and body["returned"] == 5
      and body.get("truncated") is True and body.get("total") == 12, body)

st, body = tool("count", {"collection": "widgets", "query": {"price": {"$gte": 16}}})
check("count with filter", st == "OK" and body.get("count") == 6, body)

st, body = tool("aggregate", {"collection": "widgets", "pipeline":
    [{"$group": {"_id": "$kind", "n": {"$sum": 1}, "avg": {"$avg": "$price"}}}]})
check("aggregate $group", st == "OK" and body["returned"] == 2, body)

st, body = tool("list_indexes", {"collection": "widgets"})
check("list_indexes (document)", st == "OK" and body.get("engine") == "document", body)

st, body = tool("sql_query", {"sql": "SELECT label, qty FROM parts ORDER BY qty DESC"})
check("sql_query", st == "OK" and "flat" in json.dumps(body), body)

st, body = tool("sql_query", {"sql": "SELECT * FROM parts WHERE label = ?", "params": ["hex"]})
check("sql_query with params", st == "OK" and "hex" in json.dumps(body), body)

st, body = tool("list_tables")
check("list_tables", st == "OK" and "parts" in json.dumps(body), body)

st, body = tool("describe_table", {"table": "parts"})
check("describe_table", st == "OK" and "label" in json.dumps(body), body)

st, body = tool("explain", {"collection": "widgets", "command": "find", "query": {"kind": "nut"}})
check("explain refused by name (not a 404)", st == "ERR" and "stdio" in str(body), body)

st, body = tool("text_search", {"collection": "widgets", "query": "w1"})
check("text_search without index → explicit error", st == "ERR", body)

print("== routing / isolation ==")
st, body = tool("list_databases")
check("project ref from path is the target db", st == "OK" and body == [DB], body)

st, r = rpc("tools/list", path="/mcp/other-project")
check("a different ref is served (per-request wire)", st == 200, st)

st, r = rpc("tools/list", path="/nope")
check("unknown path → 404", st == 404, st)

import urllib.request as u
try:
    req = u.Request(MCP + f"/mcp/{DB}", method="GET")
    u.urlopen(req, timeout=10); code = 200
except urllib.error.HTTPError as e:
    code = e.code
check("GET on the message endpoint → 405", code == 405, code)

print()
if failures:
    print(f"{len(failures)} FAILURES:", ", ".join(failures)); sys.exit(1)
print("ALL CHECKS PASSED")
