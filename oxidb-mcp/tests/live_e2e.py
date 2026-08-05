#!/usr/bin/env python3
"""Live e2e for oxidb-mcp (ADR-0024): drives the real binary over stdio
against a real oxidb-server (SQL + TSDB enabled, no auth). Seeds TSDB via the
raw wire (there is no MCP write tool for tsdb by design); everything else is
done through MCP tools themselves. Not run by `cargo test` — opt-in:

    cargo build -p oxidb-server -p oxidb-mcp
    OXIDB_ADDR=127.0.0.1:14471 OXIDB_DATA=/tmp/mcp-e2e OXIDB_SQL=1 OXIDB_TSDB=1 \
        target/debug/oxidb-server &
    python3 oxidb-mcp/tests/live_e2e.py target/debug/oxidb-mcp
"""
import json, os, socket, struct, subprocess, sys, time

ADDR = "127.0.0.1:14471"
MCP_BIN = sys.argv[1]

# ── raw OxiWire helper (length-prefixed LE u32 + JSON) ──────────────────────
def wire_call(req):
    host, port = ADDR.split(":")
    s = socket.create_connection((host, int(port)), timeout=10)
    payload = json.dumps(req).encode()
    s.sendall(struct.pack("<I", len(payload)) + payload)
    hdr = b""
    while len(hdr) < 4:
        hdr += s.recv(4 - len(hdr))
    n = struct.unpack("<I", hdr)[0]
    buf = b""
    while len(buf) < n:
        buf += s.recv(n - len(buf))
    s.close()
    return json.loads(buf)

# ── MCP client over stdio ───────────────────────────────────────────────────
class Mcp:
    def __init__(self, extra_env=None):
        env = dict(os.environ, OXIDB_ADDR=ADDR)
        env.pop("OXIDB_MCP_WRITES", None)
        env.pop("OXIDB_MCP_DB", None)
        if extra_env:
            env.update(extra_env)
        self.p = subprocess.Popen([MCP_BIN], stdin=subprocess.PIPE,
                                  stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                  env=env, text=True)
        self.next_id = 0

    def rpc(self, method, params=None):
        self.next_id += 1
        msg = {"jsonrpc": "2.0", "id": self.next_id, "method": method}
        if params is not None:
            msg["params"] = params
        self.p.stdin.write(json.dumps(msg) + "\n")
        self.p.stdin.flush()
        return json.loads(self.p.stdout.readline())

    def notify(self, method):
        self.p.stdin.write(json.dumps({"jsonrpc": "2.0", "method": method}) + "\n")
        self.p.stdin.flush()

    def tool(self, name, args=None):
        r = self.rpc("tools/call", {"name": name, "arguments": args or {}})
        if "error" in r:
            return ("PROTOCOL_ERROR", r["error"])
        res = r["result"]
        text = res["content"][0]["text"]
        try:
            body = json.loads(text)
        except json.JSONDecodeError:
            body = text
        return ("ERR" if res["isError"] else "OK", body)

    def close(self):
        self.p.stdin.close(); self.p.wait(timeout=10)

failures = []
def check(name, cond, detail=""):
    status = "PASS" if cond else "FAIL"
    print(f"  [{status}] {name}" + (f" — {detail}" if detail and not cond else ""))
    if not cond:
        failures.append((name, detail))

# ── wait for server ─────────────────────────────────────────────────────────
for _ in range(100):
    try:
        wire_call({"cmd": "ping"}); break
    except OSError:
        time.sleep(0.2)
else:
    sys.exit("server never came up")

# Seed TSDB over the raw wire (no MCP write tool for tsdb, by design).
now = 1754300000000
pts = [{"measurement": "cpu", "tags": {"host": h}, "fields": {"usage": 0.1 * i + (0.5 if h == "b" else 0)},
        "ts": now + i * 1000} for i in range(10) for h in ("a", "b")]
r = wire_call({"engine": "tsdb", "cmd": "tsdb", "op": "write", "points": pts})
assert r.get("ok"), r

print("== read-write session (OXIDB_MCP_WRITES=1) ==")
m = Mcp({"OXIDB_MCP_WRITES": "1"})
init = m.rpc("initialize", {"protocolVersion": "2025-06-18", "capabilities": {},
                            "clientInfo": {"name": "e2e", "version": "0"}})
check("initialize", init["result"]["protocolVersion"] == "2025-06-18", init)
m.notify("notifications/initialized")

st, body = m.tool("insert", {"collection": "people", "docs":
    [{"name": f"user{i}", "age": 20 + i % 40, "city": "izmir" if i % 3 else "ankara"} for i in range(60)]})
check("insert 60 docs", st == "OK", body)

st, body = m.tool("find", {"collection": "people"})
check("find truncates at default 50 and reports total 60",
      st == "OK" and body.get("returned") == 50 and body.get("truncated") is True
      and body.get("total") == 60, body)

st, body = m.tool("find", {"collection": "people", "query": {"age": {"$gt": 55}}, "sort": {"age": -1}})
ages = [d["age"] for d in body.get("documents", [])] if st == "OK" else []
check("find with filter+sort", st == "OK" and ages == sorted(ages, reverse=True) and
      body.get("truncated") is None and all(a > 55 for a in ages), body)

st, body = m.tool("count", {"collection": "people", "query": {"city": "ankara"}})
check("count with filter", st == "OK" and body.get("count") == 20, body)

st, body = m.tool("aggregate", {"collection": "people", "pipeline":
    [{"$group": {"_id": "$city", "n": {"$sum": 1}, "avg_age": {"$avg": "$age"}}}, {"$sort": {"n": -1}}]})
check("aggregate $group", st == "OK" and body.get("returned") == 2, body)

st, body = m.tool("explain", {"collection": "people", "command": "find", "query": {"age": {"$gt": 55}}})
check("explain returns a plan", st == "OK" and isinstance(body, dict) and body, body)

st, body = m.tool("list_collections", {})
cols = {c["name"]: c.get("count") for c in body.get("collections", [])} if st == "OK" else {}
check("list_collections with counts", st == "OK" and cols.get("people") == 60, body)

st, body = m.tool("list_databases", {})
check("list_databases", st == "OK", body)

# SQL
st, body = m.tool("sql_execute", {"sql": "CREATE TABLE runs (id INT PRIMARY KEY, athlete VARCHAR(50), km DOUBLE)"})
check("sql_execute DDL", st == "OK", body)
st, body = m.tool("sql_execute", {"sql": "INSERT INTO runs VALUES (1,'ali',5.2),(2,'veli',10.1),(3,'ali',7.3)"})
check("sql_execute INSERT", st == "OK", body)
st, body = m.tool("sql_query", {"sql": "SELECT athlete, SUM(km) AS total FROM runs GROUP BY athlete ORDER BY total DESC"})
check("sql_query aggregate", st == "OK" and "ali" in json.dumps(body), body)
st, body = m.tool("sql_query", {"sql": "SELECT * FROM runs WHERE athlete = ?", "params": ["ali"]})
check("sql_query with params", st == "OK" and json.dumps(body).count("ali") >= 2, body)
st, body = m.tool("list_tables", {})
check("list_tables", st == "OK" and "runs" in json.dumps(body), body)
st, body = m.tool("describe_table", {"table": "runs"})
check("describe_table", st == "OK" and "athlete" in json.dumps(body), body)
st, body = m.tool("list_indexes", {"table": "runs"})
check("list_indexes (sql)", st == "OK", body)
st, body = m.tool("list_indexes", {"collection": "people"})
check("list_indexes (document)", st == "OK" and body.get("engine") == "document", body)

st, body = m.tool("sql_query", {"sql": "DELETE FROM runs WHERE id = 1"})
check("sql_query refuses a write even with writes enabled", st == "ERR" and "read-only" in str(body), body)

# TSDB
st, body = m.tool("tsdb_query", {"measurement": "cpu", "field": "usage",
                                 "start": now, "end": now + 60000, "interval": 5000,
                                 "agg": "mean", "group_by": ["host"]})
hosts = sorted(s["tags"]["host"] for s in body) if st == "OK" and isinstance(body, list) else []
means_a = [p["value"] for s in body for p in s["points"] if s["tags"]["host"] == "a"] if hosts else []
check("tsdb_query downsampled+grouped", hosts == ["a", "b"] and means_a
      and abs(means_a[0] - 0.2) < 1e-9, body)

# FTS: unindexed collection must error, not answer empty
st, body = m.tool("text_search", {"collection": "people", "query": "user1"})
check("text_search without index is an explicit error", st == "ERR", body)

# update/delete
st, body = m.tool("update", {"collection": "people", "query": {"name": "user1"},
                             "update": {"$set": {"vip": True}}})
check("update", st == "OK", body)
st, body = m.tool("delete", {"collection": "people", "query": {}})
check("delete refuses empty filter", st == "ERR" and "non-empty" in str(body), body)
st, body = m.tool("delete", {"collection": "people", "query": {"name": "user2"}})
check("delete with filter", st == "OK", body)
m.close()

print("== read-only session (default) ==")
m = Mcp()
m.rpc("initialize", {"protocolVersion": "2025-06-18", "capabilities": {},
                     "clientInfo": {"name": "e2e", "version": "0"}})
m.notify("notifications/initialized")
tools = m.rpc("tools/list")["result"]["tools"]
names = [t["name"] for t in tools]
check("no write tools offered", not any(w in names for w in ("insert", "update", "delete", "sql_execute")), names)
st, body = m.tool("insert", {"collection": "people", "doc": {"x": 1}})
check("calling insert is a protocol error", st == "PROTOCOL_ERROR" and body.get("code") == -32602, body)
st, body = m.tool("find", {"collection": "people", "query": {"name": "user3"}})
check("reads still work", st == "OK" and body.get("returned") == 1, body)
m.close()

print()
if failures:
    print(f"{len(failures)} FAILURES");  [print(" -", n, d) for n, d in failures]
    sys.exit(1)
print("ALL CHECKS PASSED")
