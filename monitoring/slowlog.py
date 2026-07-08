#!/usr/bin/env python3
"""Slow-query inspector — the "why" behind the oxidb_slow_queries_total
metric. Reads the _profile collection the profiler writes
(OXIDB_SLOW_QUERY_MS must be set on the server), groups the slow
operations by command and by query shape, and auto-runs `explain` on
the query-shaped ones so you see the plan (COLLSCAN vs index, examined
vs returned, post-filter operators) that made them slow.

Usage:  python3 slowlog.py [host] [port]   (default 127.0.0.1 4444)
"""
import json
import socket
import struct
import sys
from collections import Counter, defaultdict

HOST = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 4444


def rpc(sock, obj):
    b = json.dumps(obj).encode()
    sock.sendall(struct.pack("<I", len(b)) + b)
    n = struct.unpack("<I", sock.recv(4))[0]
    data = b""
    while len(data) < n:
        data += sock.recv(n - len(data))
    return json.loads(data)


def main():
    s = socket.create_connection((HOST, PORT))
    prof = rpc(s, {"cmd": "find", "collection": "_profile", "query": {}}).get("data", [])
    if not prof:
        print("_profile is empty — is OXIDB_SLOW_QUERY_MS set on the server, "
              "and has anything been slow yet?")
        return

    print(f"═══ {len(prof)} slow operations captured "
          f"(threshold {prof[0].get('threshold_ms','?')}ms) ═══\n")

    # By command: which operation TYPE is slow?
    by_cmd = defaultdict(list)
    for p in prof:
        by_cmd[p["cmd"]].append(p.get("duration_ms", 0))
    print("By command:")
    print(f"  {'cmd':14} {'count':>6}  {'avg':>8}  {'p50':>8}  {'max':>8}")
    for cmd, durs in sorted(by_cmd.items(), key=lambda x: -sum(x[1])):
        durs.sort()
        p50 = durs[len(durs) // 2]
        print(f"  {cmd:14} {len(durs):6}  {sum(durs)/len(durs):7.1f}ms "
              f"{p50:7.1f}ms {max(durs):7.1f}ms")

    # By query shape (find/count/aggregate), with explain.
    shaped = defaultdict(list)  # (cmd, collection, shape_json) -> durations
    for p in prof:
        req = p.get("request", {})
        key_part = req.get("query") or req.get("pipeline") or req.get("sql")
        if key_part is not None and p.get("collection"):
            shaped[(p["cmd"], p["collection"], json.dumps(key_part, sort_keys=True))].append(
                p.get("duration_ms", 0)
            )

    if shaped:
        print("\nBy query shape (slowest first), with plan:")
        ranked = sorted(shaped.items(), key=lambda x: -max(x[1]))
        for (cmd, coll, shape), durs in ranked[:10]:
            print(f"\n  {len(durs)}x  {cmd} on {coll}  max {max(durs):.1f}ms")
            print(f"      shape: {shape}")
            try:
                inner = {"cmd": cmd, "collection": coll}
                parsed = json.loads(shape)
                if cmd in ("find", "count"):
                    inner["query"] = parsed
                elif cmd == "aggregate":
                    inner["pipeline"] = parsed
                plan = rpc(s, {"cmd": "explain", "inner": inner}).get("data", {})
                strat = plan.get("strategy") or plan.get("first_match", {}).get("strategy")
                print(f"      plan : strategy={strat} "
                      f"examined={plan.get('examined')} returned={plan.get('returned')} "
                      f"post_filter_ops={plan.get('post_filter_ops')}")
            except Exception as e:
                print(f"      (explain failed: {e})")
    else:
        print("\nNo query-shaped slow ops — the slow operations are writes "
              "(commit_tx/insert), i.e. contention / fsync latency, not slow reads.")
    s.close()


if __name__ == "__main__":
    main()
