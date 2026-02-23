#!/usr/bin/env python3
"""
Docker Raft cluster integration test.

Validates:
  1. 4-node Raft cluster formation via Docker containers
  2. HAProxy automatic leader routing
  3. Insert through HAProxy -> replication to all nodes
  4. Leader kill -> new leader election -> HAProxy failover
  5. Continued writes after failover -> consistency on survivors
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
NODE_PORTS = {1: 5501, 2: 5502, 3: 5503, 4: 5504}
COLLECTION = "cluster_test"


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def log(msg):
    print(f"[cluster-test] {msg}", flush=True)


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


def docker_stop_node(node_id, project="oxidb-cluster-test"):
    """Stop a specific node container with 1s grace period."""
    container = f"{project}-oxidb-node{node_id}-1"
    log(f"  Stopping container {container} ...")
    subprocess.run(
        ["docker", "stop", "-t", "1", container],
        check=True, capture_output=True, text=True,
    )
    log(f"  Container {container} stopped")


# ------------------------------------------------------------------
# Test
# ------------------------------------------------------------------

def main():
    log("=" * 60)
    log("Docker Raft Cluster Integration Test")
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

    # Add learners 2, 3, 4
    for nid in [2, 3, 4]:
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
            "members": [1, 2, 3, 4],
        })
        assert resp.get("ok"), f"raft_change_membership failed: {resp}"
        log("  raft_change_membership([1,2,3,4]): OK")

    leader_id, _ = wait_for_leader([1, 2, 3, 4], timeout=15)
    log(f"  Cluster leader: node {leader_id}")

    # Phase 3: Wait for HAProxy to detect leader
    log("\nPhase 3: Waiting for HAProxy leader routing ...")
    wait_for_haproxy(timeout=15)

    # Phase 4: Insert 10 docs through HAProxy
    log("\nPhase 4: Inserting 10 documents through HAProxy ...")
    for i in range(10):
        with connect_haproxy() as c:
            result = c.insert(COLLECTION, {"seq": i, "phase": "initial"})
            assert "id" in result, f"Insert failed: {result}"
    log("  Inserted 10 docs through HAProxy")

    # Phase 5: Verify replication to all 4 nodes
    log("\nPhase 5: Verifying replication (10 docs on all nodes) ...")
    wait_for_replication([1, 2, 3, 4], COLLECTION, 10, timeout=15)

    # Phase 6: Kill the leader
    log(f"\nPhase 6: Killing leader (node {leader_id}) ...")
    docker_stop_node(leader_id)

    survivor_ids = [nid for nid in [1, 2, 3, 4] if nid != leader_id]
    log(f"  Survivors: {survivor_ids}")

    # Phase 7: Wait for new leader + HAProxy failover
    log("\nPhase 7: Waiting for new leader election ...")
    new_leader_id, _ = wait_for_leader(survivor_ids, timeout=30)
    log(f"  New leader: node {new_leader_id}")

    log("  Waiting for HAProxy to detect new leader ...")
    wait_for_haproxy(timeout=15)

    # Phase 8: Insert 10 more docs through HAProxy
    log("\nPhase 8: Inserting 10 more documents through HAProxy ...")
    for i in range(10, 20):
        with connect_haproxy() as c:
            result = c.insert(COLLECTION, {"seq": i, "phase": "after_failover"})
            assert "id" in result, f"Insert failed: {result}"
    log("  Inserted 10 more docs through HAProxy")

    # Phase 9: Verify consistency on survivors
    log("\nPhase 9: Verifying consistency (20 docs on all survivors) ...")
    wait_for_replication(survivor_ids, COLLECTION, 20, timeout=15)

    log("\n" + "=" * 60)
    log("ALL PHASES PASSED")
    log("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"\nFAILED: {e}")
        sys.exit(1)
