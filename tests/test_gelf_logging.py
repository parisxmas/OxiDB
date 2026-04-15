#!/usr/bin/env python3
"""Comprehensive integration tests for OxiDB GELF log ingestion platform.

Tests:
  - GELF UDP ingestion (single & batch)
  - Auto-indexing of all fields
  - GELF chunked message reassembly
  - Structured field mapping (_Foo → Foo)
  - Retention policies (set/get/delete/list)
  - Alerting (create/test/list/delete/history)
  - Full-text search with stemming
  - Query performance on log data
  - Aggregation on log data
  - Unicode / Turkish character handling
  - Edge cases (malformed GELF, missing fields, oversized messages)

Requires: oxidb-server built with GELF support.
Usage:  python3 test_gelf_logging.py
"""

import json
import math
import os
import random
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time
import threading

PORT = 14999
GELF_PORT = 12299
COLLECTION = "_gelf_logs"

# ─── TCP helpers ───────────────────────────────────────────────

def send_recv(sock, payload):
    data = json.dumps(payload).encode()
    sock.sendall(struct.pack("<I", len(data)) + data)
    lb = b""
    while len(lb) < 4:
        lb += sock.recv(4 - len(lb))
    length = struct.unpack("<I", lb)[0]
    rb = b""
    while len(rb) < length:
        rb += sock.recv(length - len(rb))
    return json.loads(rb)


def connect(port, retries=30, delay=0.3):
    for _ in range(retries):
        try:
            sock = socket.create_connection(("127.0.0.1", port), timeout=5)
            r = send_recv(sock, {"cmd": "ping"})
            if r.get("ok"):
                return sock
            sock.close()
        except (ConnectionRefusedError, ConnectionError, OSError):
            pass
        time.sleep(delay)
    raise RuntimeError(f"cannot connect to port {port}")


def get_data(resp):
    return resp.get("data")


def get_count(resp):
    """Extract count from response — handles both int and {"count": N} formats."""
    data = resp.get("data")
    if isinstance(data, dict) and "count" in data:
        return data["count"]
    if isinstance(data, (int, float)):
        return int(data)
    return data


def timed(sock, payload):
    t0 = time.perf_counter()
    resp = send_recv(sock, payload)
    elapsed = (time.perf_counter() - t0) * 1000
    return resp, elapsed


# ─── GELF UDP helpers ─────────────────────────────────────────

def send_gelf(msg, port=GELF_PORT):
    """Send a single GELF message via UDP."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    data = json.dumps(msg).encode()
    sock.sendto(data, ("127.0.0.1", port))
    sock.close()


def send_gelf_batch(messages, port=GELF_PORT):
    """Send multiple GELF messages via UDP."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    for msg in messages:
        data = json.dumps(msg).encode()
        sock.sendto(data, ("127.0.0.1", port))
    sock.close()


def send_gelf_chunked(msg, chunk_size=1024, port=GELF_PORT):
    """Send a GELF message as chunked UDP packets."""
    data = json.dumps(msg).encode()
    message_id = random.getrandbits(64)
    chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
    seq_count = len(chunks)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    for seq_num, chunk in enumerate(chunks):
        header = b"\x1e\x0f"  # magic
        header += message_id.to_bytes(8, "big")
        header += bytes([seq_num, seq_count])
        sock.sendto(header + chunk, ("127.0.0.1", port))
    sock.close()


def make_gelf(short_message, level=6, extra=None):
    """Create a GELF v1.1 message."""
    msg = {
        "version": "1.1",
        "host": "test-host",
        "short_message": short_message,
        "timestamp": time.time(),
        "level": level,
        "facility": "TestSuite",
    }
    if extra:
        for k, v in extra.items():
            msg[f"_{k}"] = v
    return msg


# ─── Test framework ───────────────────────────────────────────

passed = 0
failed = 0
errors = []


def test(name):
    def decorator(fn):
        def wrapper(*args, **kwargs):
            global passed, failed
            try:
                fn(*args, **kwargs)
                passed += 1
                print(f"  ✓ {name}")
            except AssertionError as e:
                failed += 1
                errors.append((name, str(e)))
                print(f"  ✗ {name}: {e}")
            except Exception as e:
                failed += 1
                errors.append((name, str(e)))
                print(f"  ✗ {name}: EXCEPTION: {e}")
        wrapper.__test_name__ = name
        return wrapper
    return decorator


# ═══════════════════════════════════════════════════════════════
# TEST DEFINITIONS
# ═══════════════════════════════════════════════════════════════

# ─── 1. GELF Ingestion ────────────────────────────────────────

@test("GELF: single message ingestion")
def test_single_gelf(sock):
    send_recv(sock, {"cmd": "drop_collection", "collection": COLLECTION})
    time.sleep(0.3)

    send_gelf(make_gelf("test single message", extra={"TestId": "single_001"}))
    time.sleep(1)

    resp = send_recv(sock, {"cmd": "find", "collection": COLLECTION, "query": {"TestId": "single_001"}})
    docs = get_data(resp)
    assert len(docs) == 1, f"expected 1 doc, got {len(docs)}"
    assert docs[0]["short_message"] == "test single message"
    assert docs[0]["host"] == "test-host"
    assert docs[0]["facility"] == "TestSuite"


@test("GELF: batch ingestion (100 messages)")
def test_batch_gelf(sock):
    send_recv(sock, {"cmd": "drop_collection", "collection": COLLECTION})
    time.sleep(0.3)

    messages = [make_gelf(f"batch msg {i}", extra={"BatchId": "batch_100", "Seq": str(i)}) for i in range(100)]
    send_gelf_batch(messages)
    time.sleep(2)

    resp = send_recv(sock, {"cmd": "count", "collection": COLLECTION, "query": {"BatchId": "batch_100"}})
    count = get_count(resp)
    assert count == 100, f"expected 100, got {count}"


@test("GELF: structured field mapping (_Foo → Foo)")
def test_field_mapping(sock):
    send_recv(sock, {"cmd": "drop_collection", "collection": COLLECTION})
    time.sleep(0.3)

    send_gelf(make_gelf("field mapping test", extra={
        "Username": "johndoe",
        "ClientIp": "192.0.2.42",
        "SourceContext": "Example.API.Endpoints.LoginEndpoint",
        "MobileClient": "iOS",
        "ProcessId": "1",
    }))
    time.sleep(1)

    resp = send_recv(sock, {"cmd": "find", "collection": COLLECTION, "query": {"Username": "johndoe"}})
    docs = get_data(resp)
    assert len(docs) == 1, f"expected 1, got {len(docs)}"
    doc = docs[0]
    assert doc["Username"] == "johndoe"
    assert doc["ClientIp"] == "192.0.2.42"
    assert doc["SourceContext"] == "Example.API.Endpoints.LoginEndpoint"
    assert doc["MobileClient"] == "iOS"
    assert "version" not in doc, "GELF version field should be stripped"
    assert "_ts" in doc, "_ts timestamp should be added"


@test("GELF: version field is stripped")
def test_version_stripped(sock):
    send_recv(sock, {"cmd": "drop_collection", "collection": COLLECTION})
    time.sleep(0.3)

    send_gelf(make_gelf("version strip test", extra={"Tag": "version_test"}))
    time.sleep(1)

    resp = send_recv(sock, {"cmd": "find", "collection": COLLECTION, "query": {"Tag": "version_test"}})
    docs = get_data(resp)
    assert len(docs) == 1
    assert "version" not in docs[0]


@test("GELF: numeric extra fields preserved as numbers")
def test_numeric_fields(sock):
    send_recv(sock, {"cmd": "drop_collection", "collection": COLLECTION})
    time.sleep(0.3)

    send_gelf(make_gelf("numeric test", extra={
        "ResponseTimeMs": 42,
        "StatusCode": 200,
        "ProcessId": 1,
    }))
    time.sleep(1)

    resp = send_recv(sock, {"cmd": "find", "collection": COLLECTION, "query": {"ResponseTimeMs": 42}})
    docs = get_data(resp)
    assert len(docs) == 1
    assert docs[0]["StatusCode"] == 200
    assert docs[0]["ProcessId"] == 1


@test("GELF: level field queryable")
def test_level_query(sock):
    send_recv(sock, {"cmd": "drop_collection", "collection": COLLECTION})
    time.sleep(0.3)

    send_gelf(make_gelf("info message", level=6, extra={"LevelTest": "yes"}))
    send_gelf(make_gelf("error message", level=3, extra={"LevelTest": "yes"}))
    send_gelf(make_gelf("debug message", level=7, extra={"LevelTest": "yes"}))
    time.sleep(1)

    # Query errors only (level <= 3)
    resp = send_recv(sock, {"cmd": "find", "collection": COLLECTION, "query": {
        "LevelTest": "yes", "level": {"$lte": 3}
    }})
    docs = get_data(resp)
    assert len(docs) == 1
    assert docs[0]["short_message"] == "error message"


@test("GELF: full_message field preserved")
def test_full_message(sock):
    send_recv(sock, {"cmd": "drop_collection", "collection": COLLECTION})
    time.sleep(0.3)

    msg = make_gelf("error occurred", level=3, extra={"FullMsgTest": "yes"})
    msg["full_message"] = "stack trace:\n  at Foo.Bar()\n  at Main()"
    send_gelf(msg)
    time.sleep(1)

    resp = send_recv(sock, {"cmd": "find", "collection": COLLECTION, "query": {"FullMsgTest": "yes"}})
    docs = get_data(resp)
    assert len(docs) == 1
    assert "stack trace" in docs[0]["full_message"]


@test("GELF: malformed JSON is silently dropped")
def test_malformed_json(sock):
    send_recv(sock, {"cmd": "drop_collection", "collection": COLLECTION})
    time.sleep(0.3)

    # Send invalid JSON — these should be silently dropped
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.sendto(b"this is not json at all", ("127.0.0.1", GELF_PORT))
    s.sendto(b"{broken json", ("127.0.0.1", GELF_PORT))
    # Valid JSON but with unique tag to count
    s.sendto(json.dumps({"version": "1.1", "host": "x", "short_message": "valid", "_MalformedTag": "yes"}).encode(), ("127.0.0.1", GELF_PORT))
    s.close()
    time.sleep(1)

    # Only the valid one should be stored
    resp = send_recv(sock, {"cmd": "count", "collection": COLLECTION, "query": {"MalformedTag": "yes"}})
    count = get_count(resp)
    assert count == 1, f"only the valid message should be stored, got {count}"


@test("GELF: missing short_message is dropped")
def test_missing_short_message(sock):
    # GELF without short_message — should be silently dropped
    msg = {"version": "1.1", "host": "test", "level": 6, "_NoShortMsgTag": "yes"}
    send_gelf(msg)
    time.sleep(0.5)

    resp = send_recv(sock, {"cmd": "count", "collection": COLLECTION, "query": {"NoShortMsgTag": "yes"}})
    count = get_count(resp)
    assert count == 0, f"message without short_message should be dropped, got {count}"


# ─── 2. Auto-Indexing ─────────────────────────────────────────

@test("Auto-index: new fields are automatically indexed")
def test_auto_index(sock):
    send_recv(sock, {"cmd": "drop_collection", "collection": COLLECTION})
    time.sleep(0.3)

    # Send messages with various fields
    send_gelf(make_gelf("auto index test", extra={
        "CustomField1": "value1",
        "CustomField2": "value2",
        "RequestPath": "/api/test",
    }))
    time.sleep(1)

    # Query on auto-indexed field should work
    resp = send_recv(sock, {"cmd": "find", "collection": COLLECTION, "query": {"CustomField1": "value1"}})
    docs = get_data(resp)
    assert len(docs) == 1

    # Verify indexes exist
    resp = send_recv(sock, {"cmd": "list_indexes", "collection": COLLECTION})
    indexes = get_data(resp)
    index_fields = [idx.get("field", idx.get("name", "")) for idx in indexes]
    assert any("CustomField1" in f for f in index_fields), f"CustomField1 not indexed: {index_fields}"


# ─── 3. Chunked Messages ─────────────────────────────────────

@test("GELF chunked: large message reassembly")
def test_chunked_reassembly(sock):
    send_recv(sock, {"cmd": "drop_collection", "collection": COLLECTION})
    time.sleep(0.3)

    # Create a message larger than typical UDP MTU
    large_text = "A" * 5000
    msg = make_gelf(f"chunked test: {large_text}", extra={"ChunkedTest": "large_msg"})

    send_gelf_chunked(msg, chunk_size=512)
    time.sleep(2)

    resp = send_recv(sock, {"cmd": "find", "collection": COLLECTION, "query": {"ChunkedTest": "large_msg"}})
    docs = get_data(resp)
    assert len(docs) == 1, f"expected 1 chunked doc, got {len(docs)}"
    assert "AAAAA" in docs[0]["short_message"]


@test("GELF chunked: multiple chunked messages")
def test_multiple_chunked(sock):
    send_recv(sock, {"cmd": "drop_collection", "collection": COLLECTION})
    time.sleep(0.3)

    for i in range(5):
        big_payload = f"chunked message {i} " + ("X" * 3000)
        msg = make_gelf(big_payload, extra={"MultiChunk": "yes", "ChunkSeq": str(i)})
        send_gelf_chunked(msg, chunk_size=512)

    time.sleep(3)

    resp = send_recv(sock, {"cmd": "count", "collection": COLLECTION, "query": {"MultiChunk": "yes"}})
    count = get_count(resp)
    assert count == 5, f"expected 5 chunked messages, got {count}"


# ─── 4. Queries on Log Data ──────────────────────────────────

@test("Query: find by exact field match")
def test_query_exact(sock):
    send_recv(sock, {"cmd": "drop_collection", "collection": COLLECTION})
    time.sleep(0.3)

    for user in ["alice", "bob", "charlie"]:
        send_gelf(make_gelf(f"login {user}", extra={"Username": user, "QueryTest": "exact"}))
    time.sleep(1)

    resp = send_recv(sock, {"cmd": "find", "collection": COLLECTION, "query": {"Username": "bob", "QueryTest": "exact"}})
    docs = get_data(resp)
    assert len(docs) == 1
    assert docs[0]["Username"] == "bob"


@test("Query: regex on SourceContext")
def test_query_regex(sock):
    send_recv(sock, {"cmd": "drop_collection", "collection": COLLECTION})
    time.sleep(0.3)

    contexts = [
        "Example.API.Endpoints.LoginEndpoint",
        "Example.API.Endpoints.RegisterEndpoint",
        "Example.API.Middleware.AuthMiddleware",
    ]
    for ctx in contexts:
        send_gelf(make_gelf(f"ctx: {ctx}", extra={"SourceContext": ctx, "RegexTest": "yes"}))
    time.sleep(1)

    resp = send_recv(sock, {"cmd": "find", "collection": COLLECTION, "query": {
        "SourceContext": {"$regex": "Endpoint"}, "RegexTest": "yes"
    }})
    docs = get_data(resp)
    assert len(docs) == 2, f"expected 2 Endpoint matches, got {len(docs)}"


@test("Query: range query on numeric field")
def test_query_range(sock):
    send_recv(sock, {"cmd": "drop_collection", "collection": COLLECTION})
    time.sleep(0.3)

    for ms in [10, 50, 100, 500, 1000, 5000]:
        send_gelf(make_gelf(f"request {ms}ms", extra={"ResponseTimeMs": ms, "RangeTest": "yes"}))
    time.sleep(1)

    # Slow requests (>= 500ms)
    resp = send_recv(sock, {"cmd": "find", "collection": COLLECTION, "query": {
        "ResponseTimeMs": {"$gte": 500}, "RangeTest": "yes"
    }})
    docs = get_data(resp)
    assert len(docs) == 3, f"expected 3 slow requests, got {len(docs)}"


@test("Query: compound query (AND)")
def test_query_compound(sock):
    send_recv(sock, {"cmd": "drop_collection", "collection": COLLECTION})
    time.sleep(0.3)

    send_gelf(make_gelf("ios login", extra={"MobileClient": "iOS", "RequestPath": "/Login", "CompoundTest": "yes"}))
    send_gelf(make_gelf("android login", extra={"MobileClient": "Android", "RequestPath": "/Login", "CompoundTest": "yes"}))
    send_gelf(make_gelf("ios dashboard", extra={"MobileClient": "iOS", "RequestPath": "/Dashboard", "CompoundTest": "yes"}))
    time.sleep(1)

    resp = send_recv(sock, {"cmd": "find", "collection": COLLECTION, "query": {
        "MobileClient": "iOS", "RequestPath": "/Login", "CompoundTest": "yes"
    }})
    docs = get_data(resp)
    assert len(docs) == 1
    assert docs[0]["short_message"] == "ios login"


@test("Query: $or query")
def test_query_or(sock):
    send_recv(sock, {"cmd": "drop_collection", "collection": COLLECTION})
    time.sleep(0.3)

    for level in [3, 4, 5, 6, 7]:
        send_gelf(make_gelf(f"level {level}", level=level, extra={"OrTest": "yes"}))
    time.sleep(1)

    resp = send_recv(sock, {"cmd": "find", "collection": COLLECTION, "query": {
        "$or": [{"level": 3}, {"level": 4}], "OrTest": "yes"
    }})
    docs = get_data(resp)
    assert len(docs) == 2, f"expected 2 (error+warning), got {len(docs)}"


@test("Query: sort and limit")
def test_query_sort_limit(sock):
    send_recv(sock, {"cmd": "drop_collection", "collection": COLLECTION})
    time.sleep(0.3)

    for i in range(20):
        send_gelf(make_gelf(f"sorted msg {i}", extra={"SortSeq": i, "SortTest": "yes"}))
    time.sleep(1)

    resp = send_recv(sock, {"cmd": "find", "collection": COLLECTION,
        "query": {"SortTest": "yes"}, "sort": {"SortSeq": -1}, "limit": 5})
    docs = get_data(resp)
    assert len(docs) == 5
    assert docs[0]["SortSeq"] == 19


# ─── 5. Aggregation on Log Data ──────────────────────────────

@test("Aggregation: count by level")
def test_agg_count_by_level(sock):
    send_recv(sock, {"cmd": "drop_collection", "collection": COLLECTION})
    time.sleep(0.3)

    levels = [3, 3, 4, 6, 6, 6, 7, 7]
    for i, lvl in enumerate(levels):
        send_gelf(make_gelf(f"agg msg {i}", level=lvl, extra={"AggTest": "level_count"}))
    time.sleep(1)

    resp = send_recv(sock, {"cmd": "aggregate", "collection": COLLECTION, "pipeline": [
        {"$match": {"AggTest": "level_count"}},
        {"$group": {"_id": "$level", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]})
    groups = get_data(resp)
    assert len(groups) >= 3, f"expected at least 3 level groups, got {len(groups)}"
    # level 6 should have 3
    level6 = [g for g in groups if g["_id"] == 6]
    assert len(level6) == 1 and level6[0]["count"] == 3


@test("Aggregation: top users by request count")
def test_agg_top_users(sock):
    send_recv(sock, {"cmd": "drop_collection", "collection": COLLECTION})
    time.sleep(0.3)

    users = ["alice"] * 5 + ["bob"] * 3 + ["charlie"] * 1
    for u in users:
        send_gelf(make_gelf(f"request from {u}", extra={"Username": u, "AggUserTest": "yes"}))
    time.sleep(1)

    resp = send_recv(sock, {"cmd": "aggregate", "collection": COLLECTION, "pipeline": [
        {"$match": {"AggUserTest": "yes"}},
        {"$group": {"_id": "$Username", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]})
    groups = get_data(resp)
    assert groups[0]["_id"] == "alice"
    assert groups[0]["count"] == 5


@test("Aggregation: average response time by endpoint")
def test_agg_avg_response(sock):
    send_recv(sock, {"cmd": "drop_collection", "collection": COLLECTION})
    time.sleep(0.3)

    data = [
        ("/Login", 100), ("/Login", 200), ("/Login", 300),
        ("/Dashboard", 50), ("/Dashboard", 150),
    ]
    for path, ms in data:
        send_gelf(make_gelf(f"req {path}", extra={"RequestPath": path, "ResponseTimeMs": ms, "AvgTest": "yes"}))
    time.sleep(1)

    resp = send_recv(sock, {"cmd": "aggregate", "collection": COLLECTION, "pipeline": [
        {"$match": {"AvgTest": "yes"}},
        {"$group": {"_id": "$RequestPath", "avg_ms": {"$avg": "$ResponseTimeMs"}}},
        {"$sort": {"avg_ms": -1}},
    ]})
    groups = get_data(resp)
    login_group = [g for g in groups if g["_id"] == "/Login"]
    assert len(login_group) == 1
    assert abs(login_group[0]["avg_ms"] - 200.0) < 1.0


# ─── 6. Retention Policies ────────────────────────────────────

@test("Retention: set and get policy")
def test_retention_set_get(sock):
    resp = send_recv(sock, {"cmd": "set_retention", "collection": "test_retention_col", "days": 30})
    assert resp.get("ok"), f"set_retention failed: {resp}"

    resp = send_recv(sock, {"cmd": "get_retention", "collection": "test_retention_col"})
    assert resp.get("ok"), f"get_retention failed: {resp}"
    policy = get_data(resp)
    assert policy["retain_days"] == 30
    assert policy["expire_after_seconds"] == 30 * 86400


@test("Retention: list policies")
def test_retention_list(sock):
    send_recv(sock, {"cmd": "set_retention", "collection": "ret_list_1", "days": 7})
    send_recv(sock, {"cmd": "set_retention", "collection": "ret_list_2", "days": 90})

    resp = send_recv(sock, {"cmd": "list_retentions"})
    assert resp.get("ok")
    policies = get_data(resp)
    names = [p["collection"] for p in policies]
    assert "ret_list_1" in names
    assert "ret_list_2" in names


@test("Retention: delete policy")
def test_retention_delete(sock):
    send_recv(sock, {"cmd": "set_retention", "collection": "ret_delete_col", "days": 14})
    resp = send_recv(sock, {"cmd": "delete_retention", "collection": "ret_delete_col"})
    assert resp.get("ok"), f"delete_retention failed: {resp}"

    resp = send_recv(sock, {"cmd": "get_retention", "collection": "ret_delete_col"})
    assert not resp.get("ok"), "should fail after deletion"


@test("Retention: update existing policy")
def test_retention_update(sock):
    send_recv(sock, {"cmd": "set_retention", "collection": "ret_update_col", "days": 7})
    send_recv(sock, {"cmd": "set_retention", "collection": "ret_update_col", "days": 30})

    resp = send_recv(sock, {"cmd": "get_retention", "collection": "ret_update_col"})
    policy = get_data(resp)
    assert policy["retain_days"] == 30


# ─── 7. Alerting ─────────────────────────────────────────────

@test("Alert: create and get alert")
def test_alert_create(sock):
    resp = send_recv(sock, {"cmd": "create_alert",
        "name": "test_high_errors",
        "collection": COLLECTION,
        "condition": {
            "type": "count_threshold",
            "query": {"level": {"$lte": 3}},
            "window": "5m",
            "threshold": 10,
            "operator": "gte",
        },
        "actions": [{"type": "stderr"}, {"type": "log"}],
        "cooldown_seconds": 60,
    })
    assert resp.get("ok"), f"create_alert failed: {resp}"

    resp = send_recv(sock, {"cmd": "get_alert", "name": "test_high_errors"})
    assert resp.get("ok")
    alert = get_data(resp)
    assert alert["name"] == "test_high_errors"
    assert alert["enabled"] is True
    assert alert["condition"]["threshold"] == 10


@test("Alert: list alerts")
def test_alert_list(sock):
    send_recv(sock, {"cmd": "create_alert",
        "name": "alert_list_1", "collection": COLLECTION,
        "condition": {"type": "count_threshold", "query": {}, "window": "1m", "threshold": 1, "operator": "gte"},
        "actions": [{"type": "log"}],
    })
    resp = send_recv(sock, {"cmd": "list_alerts"})
    assert resp.get("ok")
    alerts = get_data(resp)
    names = [a["name"] for a in alerts]
    assert "alert_list_1" in names


@test("Alert: test alert evaluation")
def test_alert_test(sock):
    # Insert some error logs
    send_recv(sock, {"cmd": "drop_collection", "collection": COLLECTION})
    time.sleep(0.3)

    for i in range(15):
        send_gelf(make_gelf(f"error {i}", level=3))
    time.sleep(1)

    send_recv(sock, {"cmd": "create_alert",
        "name": "test_eval_alert", "collection": COLLECTION,
        "condition": {"type": "count_threshold", "query": {"level": {"$lte": 3}}, "window": "5m", "threshold": 10, "operator": "gte"},
        "actions": [{"type": "log"}],
    })

    resp = send_recv(sock, {"cmd": "test_alert", "name": "test_eval_alert"})
    assert resp.get("ok"), f"test_alert failed: {resp}"
    result = get_data(resp)
    assert result["would_fire"] is True, f"alert should fire, got: {result}"
    assert result["current_value"] >= 15


@test("Alert: test alert does NOT fire below threshold")
def test_alert_below_threshold(sock):
    send_recv(sock, {"cmd": "drop_collection", "collection": COLLECTION})
    time.sleep(0.3)

    send_gelf(make_gelf("just one error", level=3))
    time.sleep(1)

    send_recv(sock, {"cmd": "create_alert",
        "name": "test_no_fire", "collection": COLLECTION,
        "condition": {"type": "count_threshold", "query": {"level": {"$lte": 3}}, "window": "5m", "threshold": 100, "operator": "gte"},
        "actions": [{"type": "log"}],
    })

    resp = send_recv(sock, {"cmd": "test_alert", "name": "test_no_fire"})
    result = get_data(resp)
    assert result["would_fire"] is False


@test("Alert: delete alert")
def test_alert_delete(sock):
    send_recv(sock, {"cmd": "create_alert",
        "name": "to_delete", "collection": COLLECTION,
        "condition": {"type": "count_threshold", "query": {}, "window": "1m", "threshold": 1, "operator": "gte"},
        "actions": [{"type": "log"}],
    })

    resp = send_recv(sock, {"cmd": "delete_alert", "name": "to_delete"})
    assert resp.get("ok")

    resp = send_recv(sock, {"cmd": "get_alert", "name": "to_delete"})
    assert not resp.get("ok"), "should fail after deletion"


# ─── 8. Unicode / Turkish ─────────────────────────────────────

@test("Unicode: Turkish characters in GELF fields")
def test_turkish_chars(sock):
    send_recv(sock, {"cmd": "drop_collection", "collection": COLLECTION})
    time.sleep(0.3)

    send_gelf(make_gelf("Türkçe mesaj: güneş çiçek şehir", extra={
        "Kullanici": "Barış",
        "Sehir": "İstanbul",
        "UnicodeTest": "yes",
    }))
    time.sleep(1)

    resp = send_recv(sock, {"cmd": "find", "collection": COLLECTION, "query": {"UnicodeTest": "yes"}})
    docs = get_data(resp)
    assert len(docs) == 1
    assert docs[0]["Kullanici"] == "Barış"
    assert docs[0]["Sehir"] == "İstanbul"
    assert "güneş" in docs[0]["short_message"]


@test("Unicode: accented characters in queries")
def test_accented_query(sock):
    send_recv(sock, {"cmd": "drop_collection", "collection": COLLECTION})
    time.sleep(0.3)

    send_gelf(make_gelf("café résumé", extra={"AccentTest": "yes", "Location": "café"}))
    time.sleep(1)

    resp = send_recv(sock, {"cmd": "find", "collection": COLLECTION, "query": {"Location": "café"}})
    docs = get_data(resp)
    assert len(docs) == 1


# ─── 9. Edge Cases ───────────────────────────────────────────

@test("Edge: empty GELF message body")
def test_empty_body(sock):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.sendto(b"", ("127.0.0.1", GELF_PORT))
    s.close()
    # Should not crash the server
    resp = send_recv(sock, {"cmd": "ping"})
    assert resp.get("ok")


@test("Edge: GELF with no extra fields")
def test_no_extra_fields(sock):
    send_recv(sock, {"cmd": "drop_collection", "collection": COLLECTION})
    time.sleep(0.3)

    msg = {"version": "1.1", "host": "minimal", "short_message": "bare minimum", "level": 6, "timestamp": time.time()}
    send_gelf(msg)
    time.sleep(1)

    resp = send_recv(sock, {"cmd": "find", "collection": COLLECTION, "query": {"host": "minimal"}})
    docs = get_data(resp)
    assert len(docs) == 1
    assert docs[0]["short_message"] == "bare minimum"


@test("Edge: very long field values")
def test_long_values(sock):
    send_recv(sock, {"cmd": "drop_collection", "collection": COLLECTION})
    time.sleep(0.3)

    long_value = "x" * 4000
    send_gelf(make_gelf("long value test", extra={"LongField": long_value, "LongTest": "yes"}))
    time.sleep(1)

    resp = send_recv(sock, {"cmd": "find", "collection": COLLECTION, "query": {"LongTest": "yes"}})
    docs = get_data(resp)
    assert len(docs) == 1
    assert len(docs[0]["LongField"]) == 4000


@test("Edge: concurrent writers (4 threads × 25 messages)")
def test_concurrent_writers(sock):
    send_recv(sock, {"cmd": "drop_collection", "collection": COLLECTION})
    time.sleep(0.3)

    def writer(thread_id):
        for i in range(25):
            send_gelf(make_gelf(f"thread {thread_id} msg {i}", extra={
                "ThreadId": str(thread_id), "ConcurrentTest": "yes"
            }))

    threads = [threading.Thread(target=writer, args=(t,)) for t in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    time.sleep(2)

    resp = send_recv(sock, {"cmd": "count", "collection": COLLECTION, "query": {"ConcurrentTest": "yes"}})
    count = get_count(resp)
    assert count == 100, f"expected 100 from 4 threads × 25, got {count}"


@test("Edge: rapid fire (1000 messages)")
def test_rapid_fire(sock):
    send_recv(sock, {"cmd": "drop_collection", "collection": COLLECTION})
    time.sleep(0.3)

    messages = [make_gelf(f"rapid {i}", extra={"RapidTest": "yes"}) for i in range(1000)]
    send_gelf_batch(messages)
    time.sleep(3)

    resp = send_recv(sock, {"cmd": "count", "collection": COLLECTION, "query": {"RapidTest": "yes"}})
    count = get_count(resp)
    assert count == 1000, f"expected 1000, got {count}"


# ─── 10. Performance ─────────────────────────────────────────

@test("Perf: count query latency on 1000 docs")
def test_perf_count(sock):
    # Reuse rapid fire data
    resp, ms = timed(sock, {"cmd": "count", "collection": COLLECTION, "query": {"RapidTest": "yes"}})
    count = get_count(resp)
    print(f"      count={count} in {ms:.1f}ms")
    assert ms < 500, f"count too slow: {ms:.1f}ms"


@test("Perf: find with limit on 1000 docs")
def test_perf_find(sock):
    resp, ms = timed(sock, {"cmd": "find", "collection": COLLECTION,
        "query": {"RapidTest": "yes"}, "limit": 10})
    docs = get_data(resp)
    print(f"      found={len(docs)} in {ms:.1f}ms")
    assert ms < 500, f"find too slow: {ms:.1f}ms"


@test("Perf: aggregation on 1000 docs")
def test_perf_aggregation(sock):
    resp, ms = timed(sock, {"cmd": "aggregate", "collection": COLLECTION, "pipeline": [
        {"$match": {"RapidTest": "yes"}},
        {"$group": {"_id": "$host", "count": {"$sum": 1}}},
    ]})
    print(f"      aggregation in {ms:.1f}ms")
    assert ms < 1000, f"aggregation too slow: {ms:.1f}ms"


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║       OxiDB GELF Logging Platform — Integration Tests      ║")
    print("╚══════════════════════════════════════════════════════════════╝")
    print()

    # Start server
    data_dir = tempfile.mkdtemp(prefix="oxidb_gelf_test_")
    server_bin = os.path.join(os.path.dirname(__file__), "..", "target-local", "release", "oxidb-server")
    if not os.path.exists(server_bin):
        print(f"ERROR: {server_bin} not found. Build with: cargo build --release -p oxidb-server")
        sys.exit(1)

    env = os.environ.copy()
    env.update({
        "OXIDB_ADDR": f"127.0.0.1:{PORT}",
        "OXIDB_DATA": data_dir,
        "OXIDB_IDLE_TIMEOUT": "0",
        "OXIDB_GELF_PORT": str(GELF_PORT),
        "OXIDB_GELF_COLLECTION": COLLECTION,
        "OXIDB_ALERT_INTERVAL": "5",
    })

    proc = subprocess.Popen([server_bin], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    print(f"  Server PID: {proc.pid} (port={PORT}, gelf={GELF_PORT})")

    try:
        sock = connect(PORT)
        print(f"  Connected.\n")

        # Collect all test functions
        tests = [v for v in globals().values() if callable(v) and hasattr(v, "__test_name__")]

        for t in tests:
            t(sock)

        sock.close()

    finally:
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=5)
        import shutil
        shutil.rmtree(data_dir, ignore_errors=True)

    print()
    print(f"═══════════════════════════════════════════════════════════════")
    print(f"  Results: {passed} passed, {failed} failed ({passed + failed} total)")
    if errors:
        print(f"\n  Failures:")
        for name, msg in errors:
            print(f"    ✗ {name}: {msg}")
    print()

    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    main()
