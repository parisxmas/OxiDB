#!/usr/bin/env python3
"""Create a database over OxiWire: `mkdb.py <port> <name>`.

Length-prefixed JSON, u32 little-endian — note the endianness, which is the
opposite of the PostgreSQL wire this benchmark otherwise speaks.
"""
import json
import socket
import struct
import sys

port, name = int(sys.argv[1]), sys.argv[2]
body = json.dumps({"cmd": "create_database", "name": name}).encode()
s = socket.create_connection(("127.0.0.1", port), timeout=30)
s.sendall(struct.pack("<I", len(body)) + body)
n = struct.unpack("<I", s.recv(4))[0]
buf = b""
while len(buf) < n:
    buf += s.recv(n - len(buf))
resp = json.loads(buf)
if not resp.get("ok"):
    sys.exit(f"create_database {name!r} failed: {resp}")
