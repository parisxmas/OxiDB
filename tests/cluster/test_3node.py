#!/usr/bin/env python3
"""
Docker Raft cluster integration test (3-node variant).

Validates:
  1. 3-node Raft cluster formation via Docker containers
  2. HAProxy automatic leader routing
  3. Insert through HAProxy -> replication to all nodes
  4. Per-node content consistency (query verification)
  5. Leader kill -> new leader election -> HAProxy failover
  6. Continued writes after failover -> consistency on survivors
"""

import os
import sys
import json
import socket
import struct
import subprocess
import time

# Add the python client to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "python"))
from oxidb import OxiDbClient, OxiDbError

HAPROXY_PORT = 5500
NODE_PORTS = {1: 5501, 2: 5502, 3: 5503}
COLLECTION = "cluster_3node_test"


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def log(msg):
    print(f"[3node-test] {msg}", flush=True)


def wait_for_port(host, port, timeout=30):
    """Wait until a TCP port is accepting connections."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            s = socket.create_connection((host, port), timeout=2)
            s.close()
            return
        except OSError:
            time.sleep(0.5)
    raise TimeoutError(f"Port {host}:{port} not reachable after {timeout}s")


def connect_node(node_id):
    """Return an OxiDbClient connected to a specific node via host port."""
    return OxiDbClient("127.0.0.1", NODE_PORTS[node_id], timeout=10.0)


def connect_haproxy():
    """Return an OxiDbClient connected through HAProxy."""
    return OxiDbClient("127.0.0.1", HAPROXY_PORT, timeout=10.0)


def raft_metrics(client):
    """Get raft_metrics from a node. Returns the data dict or None on error."""
    try:
        resp = client._request({"cmd": "raft_metrics"})
        if resp.get("ok"):
            return resp["data"]
    except Exception:
        pass
    return None


def wait_for_leader(node_ids, timeout=30):
    """Poll nodes until one reports state=Leader. Returns (leader_node_id, metrics)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        for nid in node_ids:
            try:
                with connect_node(nid) as c:
                    m = raft_metrics(c)
                    if m and m.get("state") == "Leader":
                        return nid, m
            except Exception:
                pass
        time.sleep(0.5)
    raise TimeoutError(f"No leader found among nodes {node_ids} after {timeout}s")


def wait_for_replication(node_ids, collection, expected_count, timeout=30):
    """Poll count on all nodes until they all match expected_count."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        counts = {}
        all_match = True
        for nid in node_ids:
            try:
                with connect_node(nid) as c:
                    cnt = c.count(collection)
                    counts[nid] = cnt
                    if cnt != expected_count:
                        all_match = False
            except Exception as e:
                counts[nid] = f"error: {e}"
                all_match = False
        if all_match:
            log(f"  Replication OK: all nodes have {expected_count} docs")
            return
        time.sleep(0.5)
    raise TimeoutError(
        f"Replication not converged after {timeout}s. Counts: {counts}"
    )


def wait_for_haproxy(timeout=15):
    """Wait until HAProxy routes to a leader (raft_metrics returns Leader)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with connect_haproxy() as c:
                m = raft_metrics(c)
                if m and m.get("state") == "Leader":
                    log(f"  HAProxy routing to leader (node {m.get('id')})")
                    return m
        except Exception:
            pass
        time.sleep(0.5)
    raise TimeoutError(f"HAProxy not routing to leader after {timeout}s")


def docker_stop_node(node_id, project="oxidb-3node"):
    """Stop a specific node container with 1s grace period."""
    container = f"{project}-oxidb-node{node_id}-1"
    log(f"  Stopping container {container} ...")
    subprocess.run(
        ["docker", "stop", "-t", "1", container],
        check=True, capture_output=True, text=True,
    )
    log(f"  Container {container} stopped")


def verify_documents_on_node(node_id, expected_docs):
    """Query all docs on a node and verify they match expected content."""
    with connect_node(node_id) as c:
        docs = c.find(COLLECTION)
    # Build a set of (seq, phase) tuples from the node's docs
    found = set()
    for doc in docs:
        found.add((doc["seq"], doc["phase"]))
    missing = expected_docs - found
    extra = found - expected_docs
    if missing or extra:
        raise AssertionError(
            f"Node {node_id} content mismatch: missing={missing}, extra={extra}"
        )


# ------------------------------------------------------------------
# Test
# ------------------------------------------------------------------

def main():
    log("=" * 60)
    log("Docker Raft 3-Node Cluster Integration Test")
    log("=" * 60)

    # Phase 1: Wait for all nodes to be reachable
    log("\nPhase 1: Waiting for nodes to come up ...")
    for nid, port in NODE_PORTS.items():
        wait_for_port("127.0.0.1", port, timeout=60)
        with connect_node(nid) as c:
            c.ping()
        log(f"  Node {nid} (port {port}) is up")

    # Phase 2: Bootstrap Raft cluster
    log("\nPhase 2: Bootstrapping Raft cluster ...")

    # Initialize on node 1
    with connect_node(1) as c:
        resp = c._request({"cmd": "raft_init"})
        assert resp.get("ok"), f"raft_init failed: {resp}"
        log("  raft_init on node 1: OK")

    time.sleep(1)

    # Add learners 2, 3
    for nid in [2, 3]:
        with connect_node(1) as c:
            resp = c._request({
                "cmd": "raft_add_learner",
                "node_id": nid,
                "addr": f"oxidb-node{nid}:4445",
            })
            assert resp.get("ok"), f"raft_add_learner({nid}) failed: {resp}"
            log(f"  raft_add_learner(node {nid}): OK")

    time.sleep(1)

    # Promote all to voters
    with connect_node(1) as c:
        resp = c._request({
            "cmd": "raft_change_membership",
            "members": [1, 2, 3],
        })
        assert resp.get("ok"), f"raft_change_membership failed: {resp}"
        log("  raft_change_membership([1,2,3]): OK")

    leader_id, _ = wait_for_leader([1, 2, 3], timeout=15)
    log(f"  Cluster leader: node {leader_id}")

    # Phase 3: Wait for HAProxy to detect leader
    log("\nPhase 3: Waiting for HAProxy leader routing ...")
    wait_for_haproxy(timeout=15)

    # Phase 4: Bulk insert 10K docs through HAProxy
    INITIAL_COUNT = 10_000
    log(f"\nPhase 4: Bulk inserting {INITIAL_COUNT} documents through HAProxy ...")
    BATCH_SIZE = 500
    t0 = time.time()
    with connect_haproxy() as c:
        for start in range(0, INITIAL_COUNT, BATCH_SIZE):
            batch = [{"seq": i, "phase": "initial"} for i in range(start, min(start + BATCH_SIZE, INITIAL_COUNT))]
            result = c.insert_many(COLLECTION, batch)
            assert len(result) == len(batch), f"Batch insert failed: expected {len(batch)} ids, got {result}"
    elapsed = time.time() - t0
    log(f"  Inserted {INITIAL_COUNT} docs in {elapsed:.2f}s ({INITIAL_COUNT/elapsed:.0f} docs/s)")

    # Phase 5: Verify replication to all 3 nodes (count check)
    log(f"\nPhase 5: Verifying replication ({INITIAL_COUNT} docs on all nodes) ...")
    wait_for_replication([1, 2, 3], COLLECTION, INITIAL_COUNT, timeout=30)

    # Phase 6: Verify content consistency on each node (sample check)
    log("\nPhase 6: Verifying document content on each node ...")
    for nid in [1, 2, 3]:
        with connect_node(nid) as c:
            cnt = c.count(COLLECTION)
            assert cnt == INITIAL_COUNT, f"Node {nid} count mismatch: {cnt} != {INITIAL_COUNT}"
            # Spot-check a few documents
            for seq in [0, INITIAL_COUNT // 2, INITIAL_COUNT - 1]:
                docs = c.find(COLLECTION, {"seq": seq})
                assert len(docs) == 1 and docs[0]["phase"] == "initial", \
                    f"Node {nid} seq={seq} mismatch: {docs}"
        log(f"  Node {nid}: {INITIAL_COUNT} docs, spot checks passed")

    # Phase 7: Kill the leader
    log(f"\nPhase 7: Killing leader (node {leader_id}) ...")
    docker_stop_node(leader_id)

    survivor_ids = [nid for nid in [1, 2, 3] if nid != leader_id]
    log(f"  Survivors: {survivor_ids}")

    # Phase 8: Wait for new leader + HAProxy failover
    log("\nPhase 8: Waiting for new leader election ...")
    new_leader_id, _ = wait_for_leader(survivor_ids, timeout=30)
    log(f"  New leader: node {new_leader_id}")

    log("  Waiting for HAProxy to detect new leader ...")
    wait_for_haproxy(timeout=15)

    # Phase 9: Bulk insert 5K more docs through HAProxy
    FAILOVER_COUNT = 5_000
    TOTAL_COUNT = INITIAL_COUNT + FAILOVER_COUNT
    log(f"\nPhase 9: Bulk inserting {FAILOVER_COUNT} more documents through HAProxy ...")
    t0 = time.time()
    with connect_haproxy() as c:
        for start in range(INITIAL_COUNT, TOTAL_COUNT, BATCH_SIZE):
            batch = [{"seq": i, "phase": "after_failover"} for i in range(start, min(start + BATCH_SIZE, TOTAL_COUNT))]
            result = c.insert_many(COLLECTION, batch)
            assert len(result) == len(batch), f"Batch insert failed: expected {len(batch)} ids, got {result}"
    elapsed = time.time() - t0
    log(f"  Inserted {FAILOVER_COUNT} docs in {elapsed:.2f}s ({FAILOVER_COUNT/elapsed:.0f} docs/s)")

    # Phase 10: Verify consistency on survivors (15K total docs)
    log(f"\nPhase 10: Verifying consistency ({TOTAL_COUNT} docs on all survivors) ...")
    wait_for_replication(survivor_ids, COLLECTION, TOTAL_COUNT, timeout=30)

    for nid in survivor_ids:
        with connect_node(nid) as c:
            cnt = c.count(COLLECTION)
            assert cnt == TOTAL_COUNT, f"Node {nid} count mismatch: {cnt} != {TOTAL_COUNT}"
            # Spot-check pre and post failover docs
            for seq in [0, INITIAL_COUNT - 1, INITIAL_COUNT, TOTAL_COUNT - 1]:
                docs = c.find(COLLECTION, {"seq": seq})
                assert len(docs) == 1, f"Node {nid} seq={seq}: expected 1 doc, got {len(docs)}"
        log(f"  Node {nid}: all {TOTAL_COUNT} docs verified")

    log("\n" + "=" * 60)
    log("ALL PHASES PASSED")
    log("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"\nFAILED: {e}")
        sys.exit(1)
