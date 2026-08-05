#!/usr/bin/env python3
"""The load-bearing claim of the hosted MCP transport (ADR-0024 Phase 2):
it forwards the caller's key and inherits the REST surface's authorization,
rather than deciding anything itself. This proves it — a security rule set on
a collection must refuse the MCP tool call, with no change to oxidb-mcp.

Writing this found two real holes in the REST surface (both fixed in 0.42.8):
`GET /api/{col}/count` never consulted the read rule, making it an
arbitrary-predicate disclosure oracle; and numeric rule comparisons compared
JSON representations, so `doc.hidden != 1` published the rows it meant to hide.

Same setup as http_e2e.py (see its docstring)."""
import json, socket, struct, sys, time, urllib.request, urllib.error

WIRE = ("127.0.0.1", 14481)
REST = "http://127.0.0.1:14482"
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

def http(method, url, body=None):
    req = urllib.request.Request(url, data=(json.dumps(body).encode() if body is not None else None),
                                 headers={"Content-Type": "application/json"}, method=method)
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, raw.decode(errors="replace")

def tool(name, args=None):
    st, r = http("POST", f"{MCP}/mcp/{DB}",
                 {"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                  "params": {"name": name, "arguments": args or {}}})
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

for _ in range(120):
    try:
        wire_call({"cmd": "ping"}); break
    except OSError:
        time.sleep(0.25)
for _ in range(120):
    try:
        urllib.request.urlopen(MCP + "/mcp/health", timeout=5); break
    except Exception:
        time.sleep(0.25)

# Seed two collections: one public, one we will lock.
for col in ("public_notes", "secret_notes"):
    wire_call({"cmd": "delete", "collection": col, "query": {}})
    for i in range(3):
        wire_call({"cmd": "insert", "collection": col, "doc": {"n": i, "col": col}})
http("DELETE", f"{REST}/api/rules/secret_notes")

print("== before any rule ==")
st, body = tool("find", {"collection": "secret_notes"})
check("secret_notes readable with no rule", st == "OK" and body["returned"] == 3, body)

print("== rule: read=false on secret_notes ==")
code, _ = http("POST", f"{REST}/api/rules/secret_notes", {"read": "false"})
check("rule stored", code == 200, code)

st, body = tool("find", {"collection": "secret_notes"})
check("find is REFUSED by the rule", st == "ERR" and "denied" in str(body).lower(), body)

st, body = tool("count", {"collection": "secret_notes", "query": {}})
check("count is refused too", st == "ERR", body)

st, body = tool("aggregate", {"collection": "secret_notes",
                              "pipeline": [{"$group": {"_id": None, "n": {"$sum": 1}}}]})
check("aggregate is refused (cannot be filtered after the fact)", st == "ERR", body)

st, body = tool("find", {"collection": "public_notes"})
check("the unruled collection still reads", st == "OK" and body["returned"] == 3, body)

print("== row-level rule ==")
http("POST", f"{REST}/api/rules/secret_notes", {"read": "doc.n == 0"})
st, body = tool("find", {"collection": "secret_notes"})
# The document engine CAN honour a row-level read rule on find: it filters.
check("row-level rule filters find to the allowed rows",
      st == "OK" and body["returned"] == 1 and body["documents"][0]["n"] == 0, body)
st, body = tool("aggregate", {"collection": "secret_notes",
                              "pipeline": [{"$group": {"_id": None, "n": {"$sum": 1}}}]})
check("but aggregate is refused rather than answered from hidden rows",
      st == "ERR" and "row-level" in str(body).lower(), body)

# Leave it clean.
http("DELETE", f"{REST}/api/rules/secret_notes")

print()
if failures:
    print(f"{len(failures)} FAILURES:", ", ".join(failures)); sys.exit(1)
print("ALL CHECKS PASSED")
