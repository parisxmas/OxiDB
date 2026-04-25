#!/usr/bin/env python3
"""
ShopEdge cluster — failover scenarios.

Uses `docker compose` to take nodes offline (stop / kill / pause / network
disconnect), observes how writes and reads behave, then brings the nodes back.

Run from the host:

    cd ShardReplicaRealWorldTest
    python tests/test_failover.py

Or run individual scenarios:

    python tests/test_failover.py follower_down
    python tests/test_failover.py leader_down
    python tests/test_failover.py two_followers_down
    python tests/test_failover.py recovery_catchup
    python tests/test_failover.py network_partition

Notes:
  - oxipool's `OXIPOOL_MASTER` is a **static** address (db-X0:4444 by config).
    Killing the leader exposes a real gap: oxipool keeps trying the dead
    address even after Raft elects a new leader. This is documented behavior;
    the leader_down scenario surfaces it explicitly.
  - Quorum for a 3-node Raft group = 2/3. Killing one follower leaves the
    cluster writable; killing two followers blocks writes (no quorum).
"""

import json
import os
import subprocess
import sys
import time
import urllib.request
import zlib

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.normpath(os.path.join(HERE, "..", "..", "python")))

from oxidb import OxiDbClient, OxiDbError  # noqa: E402

ROUTER_HOST = os.getenv("ROUTER_HOST", "127.0.0.1")
ROUTER_PORT = int(os.getenv("ROUTER_PORT", "4445"))
COMPOSE_DIR = os.path.normpath(os.path.join(HERE, ".."))

GREEN, RED, YELLOW, DIM, END = "\033[32m", "\033[31m", "\033[33m", "\033[2m", "\033[0m"


def shard_of(customer_id: int) -> str:
    h = zlib.crc32(str(customer_id).encode())
    return ["A", "B", "C"][(h % 256) % 3]


def cid_for_shard(target: str, start: int = 1) -> int:
    """Return the smallest customer_id whose CRC32 hashes to the given shard."""
    cid = start
    while shard_of(cid) != target:
        cid += 1
        if cid > 100_000:
            raise RuntimeError(f"could not find cid for shard {target}")
    return cid


def compose(*args, capture=True, check=True):
    """Run `docker compose <args>` from the compose directory."""
    return subprocess.run(
        ["docker", "compose", *args],
        cwd=COMPOSE_DIR,
        check=check,
        capture_output=capture,
        text=True,
    )


def stop(name):
    print(f"     {YELLOW}⏹  docker compose stop {name}{END}")
    compose("stop", name)


def start(name):
    print(f"     {GREEN}▶  docker compose start {name}{END}")
    compose("start", name)


def kill(name):
    print(f"     {RED}☠  docker compose kill {name}{END}")
    compose("kill", name, check=False)


def pause(name):
    print(f"     {YELLOW}⏸  docker compose pause {name}{END}")
    compose("pause", name)


def unpause(name):
    print(f"     {GREEN}▶  docker compose unpause {name}{END}")
    compose("unpause", name)


def disconnect(container):
    """Drop the container from the shopedge network — simulates a partition."""
    print(f"     {YELLOW}⛔ docker network disconnect shopedge_shopedge {container}{END}")
    subprocess.run(
        ["docker", "network", "disconnect", "shopedge_shopedge", container],
        check=True,
    )


def reconnect(container):
    print(f"     {GREEN}🔌 docker network connect shopedge_shopedge {container}{END}")
    subprocess.run(
        ["docker", "network", "connect", "shopedge_shopedge", container],
        check=True,
    )


def wait_raft_in_sync(timeout=30):
    """Poll /api/raft/metrics until every shard's followers match their leader's
    last_log_index. Returns True if all caught up, False on timeout."""
    api_host = os.getenv("API_HOST", "127.0.0.1")
    api_port = int(os.getenv("API_PORT", "8080"))
    url = f"http://{api_host}:{api_port}/api/raft/metrics"
    deadline = time.time() + timeout
    last_state = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=3) as resp:
                data = json.loads(resp.read())
            by_shard = {}  # shard letter → list of (state, log_index)
            for r in data["results"]:
                if not r.get("ok"):
                    continue
                m = r["metrics"]
                shard_letter = r["node"].split(":")[0][3]  # "db-a0" → "a"
                by_shard.setdefault(shard_letter, []).append(m["last_log_index"])
            in_sync = all(len(set(idxs)) == 1 for idxs in by_shard.values()) and len(by_shard) == 3
            last_state = {s: list(idxs) for s, idxs in by_shard.items()}
            if in_sync:
                return True
        except Exception:
            pass
        time.sleep(0.5)
    print(f"     {YELLOW}timeout waiting for Raft sync; last state: {last_state}{END}")
    return False


TRANSIENT_MARKERS = ("broken pipe", "early eof", "connection reset",
                     "forward request to", "timed out", "connection refused")


def is_transient(err) -> bool:
    msg = str(err).lower()
    return any(m in msg for m in TRANSIENT_MARKERS)


def warmup_pool():
    """oxipool keeps cached backend connections that go stale when a node's
    state changes. The first few requests after a stop/start typically fail
    with `Broken pipe` / `early eof` until oxipool's `spawn_replace` swaps
    them out. We hammer ping until 2 in a row succeed so subsequent test
    traffic uses fresh conns."""
    consecutive_ok = 0
    for _ in range(20):
        try:
            with OxiDbClient(ROUTER_HOST, ROUTER_PORT, timeout=4) as db:
                db.ping()
            consecutive_ok += 1
            if consecutive_ok >= 2:
                return
        except (OxiDbError, OSError, TimeoutError):
            consecutive_ok = 0
        time.sleep(0.3)


def reset_pools():
    """Restart the oxipool tier — guarantees a clean slate between scenarios.
    The data is preserved (oxidb-server volumes persist); only oxipool's
    connection cache is rebuilt."""
    print(f"     {DIM}↻ flushing oxipool tier (restart pools){END}")
    compose("restart", "pool-shard-a", "pool-shard-b", "pool-shard-c", "pool-router", check=False)
    time.sleep(4)
    warmup_pool()


def write_and_read(tag, cid, expect_ok=True, timeout=8, attempts=4):
    """Try a single insert+find via the router. Returns (write_ok, find_count).
    Retries on transient pool errors (broken pipe, early eof, etc.); returns
    False on a real failure (e.g. master gone)."""
    last_err = None
    for attempt in range(attempts):
        try:
            with OxiDbClient(ROUTER_HOST, ROUTER_PORT, timeout=timeout) as db:
                db.insert("orders", {"customer_id": cid, "_smoke": tag, "status": "test"})
                time.sleep(0.6)
                docs = db.find("orders", {"customer_id": cid, "_smoke": tag})
                return True, len(docs)
        except (OxiDbError, OSError, TimeoutError) as e:
            last_err = e
            if is_transient(e) and attempt < attempts - 1:
                time.sleep(0.6)
                continue
            return False, str(e)
    return False, str(last_err)


# ─── Scenarios ──────────────────────────────────────────────────────


def scenario_follower_down():
    """One follower in shard A is stopped. Quorum = 2/3 → writes still succeed.

    Note: openraft can briefly return 'forward to None' right after a stop —
    we tolerate that with a short settle window before the test write."""
    print(f"\n{DIM}── scenario: 1 follower down (writes survive — quorum 2/3) ──{END}")
    cid = cid_for_shard("A")
    tag = f"fl-{int(time.time() * 1000)}"
    stop("db-a1")
    try:
        time.sleep(5)  # let openraft settle after the stop
        warmup_pool()
        ok, result = write_and_read(tag, cid, attempts=6)
        if ok:
            print(f"     {GREEN}✓ shard A still writable with 1/3 nodes down (cid={cid}, found {result} row(s)){END}")
            return True
        print(f"     {RED}✗ write blocked unexpectedly: {result}{END}")
        return False
    finally:
        start("db-a1")
        time.sleep(8)


def scenario_two_followers_down():
    """Two followers down → no Raft quorum → writes blocked."""
    print(f"\n{DIM}── scenario: 2 followers down (writes blocked — no quorum) ──{END}")
    cid = cid_for_shard("A")
    tag = f"2fl-{int(time.time() * 1000)}"
    stop("db-a1")
    stop("db-a2")
    try:
        time.sleep(3)
        ok, result = write_and_read(tag, cid, timeout=6)
        if not ok:
            print(f"     {GREEN}✓ shard A writes blocked as expected: {str(result)[:120]}{END}")
            return True
        # If write somehow succeeded, that's bad — Raft should reject without quorum
        print(f"     {YELLOW}! write returned ok={ok} result={result} — quorum check may be soft{END}")
        return False
    finally:
        start("db-a1")
        start("db-a2")
        time.sleep(10)


def scenario_leader_down():
    """Leader dies: oxipool's STATIC OXIPOOL_MASTER points at the dead node →
    shard A unreachable through the router until db-a0 is back. Other shards
    untouched."""
    print(f"\n{DIM}── scenario: leader down (surfaces oxipool static-master gap) ──{END}")
    cid_a = cid_for_shard("A")
    cid_b = cid_for_shard("B")
    tag = f"ld-{int(time.time() * 1000)}"
    stop("db-a0")
    try:
        time.sleep(5)  # raft election would happen among a1/a2

        # Shard A: oxipool keeps dialing db-a0:4444 (down) → fails
        ok_a, result_a = write_and_read(tag, cid_a, timeout=8)
        print(f"     shard A (cid={cid_a}): write_ok={ok_a} result={str(result_a)[:80]}")
        if ok_a:
            print(f"     {YELLOW}! unexpected — leader_down was supposed to break shard A writes{END}")
        else:
            print(f"     {GREEN}✓ shard A writes blocked (oxipool can't reach static master){END}")

        # Shard B: untouched
        ok_b, result_b = write_and_read(tag, cid_b, attempts=4)
        if ok_b:
            print(f"     {GREEN}✓ shard B unaffected — failure isolated to one shard (cid={cid_b}, found {result_b} row(s)){END}")
            return not ok_a and ok_b
        print(f"     {RED}✗ shard B should still work: ok={ok_b} result={result_b}{END}")
        return False
    finally:
        start("db-a0")
        time.sleep(10)


def scenario_recovery_catchup():
    """Stop a follower, write data, restart it, verify it catches up via Raft.
    (We can only confirm the write succeeded and the follower came back; direct
    verification of the follower's contents would need its port published.)"""
    print(f"\n{DIM}── scenario: follower restart + Raft log catch-up ──{END}")
    cid = cid_for_shard("A")
    tag = f"rec-{int(time.time() * 1000)}"
    stop("db-a2")
    try:
        time.sleep(2)
        warmup_pool()  # flush oxipool stale-conn cache
        # Retry-tolerant insert loop — oxipool's spawn_replace may fire on first miss
        inserted = 0
        for i in range(20):
            for attempt in range(3):
                try:
                    with OxiDbClient(ROUTER_HOST, ROUTER_PORT, timeout=8) as db:
                        db.insert("orders", {"customer_id": cid, "_smoke": tag, "i": i})
                    inserted += 1
                    break
                except (OxiDbError, OSError, TimeoutError):
                    time.sleep(0.3)
        print(f"     inserted {inserted}/20 rows while db-a2 was down")
        if inserted < 20:
            return False
    finally:
        start("db-a2")

    # Flush oxipool's stale-conn cache before checking the count
    warmup_pool()

    # Poll Raft metrics until db-a2's log_index matches the leader's.
    print(f"     waiting for Raft to replay log to db-a2...")
    if not wait_raft_in_sync(timeout=45):
        print(f"     {RED}✗ db-a2 didn't catch up within 45s{END}")
        return False
    print(f"     {GREEN}✓ db-a2 in sync via /api/raft/metrics{END}")

    # Allow oxipool a moment to refresh its replica pool
    time.sleep(2)

    with OxiDbClient(ROUTER_HOST, ROUTER_PORT) as db:
        n = db.count("orders", {"_smoke": tag})
        if n == 20:
            print(f"     {GREEN}✓ count via router: {n} (data fully replicated){END}")
            return True
        print(f"     {RED}✗ expected 20, got {n} (count may have hit a stale replica){END}")
        return False


def scenario_network_partition():
    """Disconnect a follower from the docker network instead of stopping it.
    Process is alive but unreachable — closer to a real-world partition."""
    print(f"\n{DIM}── scenario: network partition (follower isolated) ──{END}")
    cid = cid_for_shard("A")
    tag = f"net-{int(time.time() * 1000)}"
    try:
        disconnect("shopedge-db-a1")
        time.sleep(3)
        ok, result = write_and_read(tag, cid, attempts=4)
        if ok:
            print(f"     {GREEN}✓ writes survive a follower partition (quorum 2/3, found {result} row(s)){END}")
            return True
        print(f"     {RED}✗ writes blocked unexpectedly: {result}{END}")
        return False
    finally:
        try:
            reconnect("shopedge-db-a1")
        except subprocess.CalledProcessError:
            pass
        time.sleep(8)


# ─── Driver ─────────────────────────────────────────────────────────


# Scenario order matters when running the full suite: scenarios that don't
# disturb leadership run first; `leader_down` runs LAST because after stopping
# the leader and restarting it, db-a0 may come back as a follower (Raft elected
# someone else during its outage) — which breaks subsequent scenarios that
# assume oxipool's static OXIPOOL_MASTER=db-a0 is still the actual Raft leader.
SCENARIOS = {
    "network_partition":   scenario_network_partition,
    "follower_down":       scenario_follower_down,
    "recovery_catchup":    scenario_recovery_catchup,
    "two_followers_down":  scenario_two_followers_down,
    "leader_down":         scenario_leader_down,
}


def main():
    selected = sys.argv[1:] if len(sys.argv) > 1 else list(SCENARIOS.keys())
    unknown = [s for s in selected if s not in SCENARIOS]
    if unknown:
        print(f"unknown scenario(s): {unknown}")
        print(f"available: {list(SCENARIOS.keys())}")
        sys.exit(2)

    print()
    print("═" * 70)
    print(f"  ShopEdge cluster — failover scenarios")
    print(f"  router: {ROUTER_HOST}:{ROUTER_PORT}")
    print(f"  scenarios: {selected}")
    print("═" * 70)

    # Make sure the cluster is in a stable starting state. Without this, a
    # previous run that left leadership unstable (e.g. after `leader_down`)
    # poisons the first scenario.
    print(f"  {DIM}warming up oxipool's connection cache...{END}")
    warmup_pool()
    print()

    results = {}
    for i, name in enumerate(selected):
        try:
            results[name] = SCENARIOS[name]()
        except Exception as e:
            print(f"     {RED}exception: {type(e).__name__}: {e}{END}")
            results[name] = False
        # Brief warmup between scenarios to flush any poisoned conns from
        # the just-completed test (don't restart the pool — that closes good
        # connections too).
        if i < len(selected) - 1:
            warmup_pool()

    print()
    print("═" * 70)
    print("  Summary")
    print("═" * 70)
    for name in selected:
        mark = f"{GREEN}PASS{END}" if results[name] else f"{RED}FAIL{END}"
        print(f"  {mark}  {name}")
    print()
    sys.exit(0 if all(results.values()) else 1)


if __name__ == "__main__":
    main()
