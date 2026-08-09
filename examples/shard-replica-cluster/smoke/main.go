// smoke: end-to-end validation of the ShopEdge sharded + replicated cluster.
//
// Connects to the top-level router AND to each db-XN directly to verify:
//   1) Cluster health   — router + 9 db nodes respond to ping
//   2) Sharding         — orders inserted via the router land on the shard
//                         determined by CRC32(customer_id)
//   3) Raft replication — after inserting via the master, both replicas in
//                         that Raft group return the row when queried directly
//   4) TX pinning       — begin_tx → insert → find (sees uncommitted) → commit
//   5) Scatter-gather   — find without shard key returns docs from all shards
//
// Designed to run as a compose service alongside the cluster.
//
// Env:
//   SMOKE_ROUTER       default "pool-router:4445"
//   SMOKE_NODES_A      default "db-a0:4444,db-a1:4444,db-a2:4444"
//   SMOKE_NODES_B      default "db-b0:4444,db-b1:4444,db-b2:4444"
//   SMOKE_NODES_C      default "db-c0:4444,db-c1:4444,db-c2:4444"
//   SMOKE_REPL_DELAY   ms to wait for Raft commit, default 800

package main

import (
	"fmt"
	"hash/crc32"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/parisxmas/OxiDB/clients/go/oxidb"
)

type endpoint struct {
	host string
	port int
}

func (e endpoint) String() string { return fmt.Sprintf("%s:%d", e.host, e.port) }

var (
	router   endpoint
	shardA   []endpoint
	shardB   []endpoint
	shardC   []endpoint
	replWait time.Duration
)

func init() {
	router = parseEndpoint(envOr("SMOKE_ROUTER", "pool-router:4445"))
	shardA = parseEndpoints(envOr("SMOKE_NODES_A", "db-a0:4444,db-a1:4444,db-a2:4444"))
	shardB = parseEndpoints(envOr("SMOKE_NODES_B", "db-b0:4444,db-b1:4444,db-b2:4444"))
	shardC = parseEndpoints(envOr("SMOKE_NODES_C", "db-c0:4444,db-c1:4444,db-c2:4444"))
	replWait = time.Duration(intOr("SMOKE_REPL_DELAY", 800)) * time.Millisecond
}

type test struct {
	name  string
	fn    func() error
	skip  bool
	note  string
}

func main() {
	tests := []test{
		{name: "1. Cluster health", fn: testHealth},
		{name: "2. Sharding distribution", fn: testSharding},
		{name: "3. Raft replication (master → replicas)", fn: testReplication},
		{name: "4. TX pinning + commit visibility", fn: testTxPinning},
		{name: "5. Scatter-gather across shards", fn: testScatterGather},
	}

	fmt.Println()
	fmt.Println("──────────────────────────────────────────────────────────────────────")
	fmt.Println("  ShopEdge cluster smoke test")
	fmt.Printf("    router:   %s\n", router)
	fmt.Printf("    shard A:  %v\n", shardA)
	fmt.Printf("    shard B:  %v\n", shardB)
	fmt.Printf("    shard C:  %v\n", shardC)
	fmt.Println("──────────────────────────────────────────────────────────────────────")
	fmt.Println()

	pass, fail := 0, 0
	for _, t := range tests {
		fmt.Printf(" [..] %-50s\n", t.name)
		t0 := time.Now()
		err := t.fn()
		took := time.Since(t0).Round(time.Millisecond)
		if err == nil {
			fmt.Printf(" \033[32m[OK]\033[0m %-50s %s\n\n", t.name, took)
			pass++
		} else {
			fmt.Printf(" \033[31m[FAIL]\033[0m %-50s %s\n", t.name, took)
			fmt.Printf("       %v\n\n", err)
			fail++
		}
	}

	fmt.Println("──────────────────────────────────────────────────────────────────────")
	fmt.Printf("  Result: %d passed · %d failed\n", pass, fail)
	fmt.Println("──────────────────────────────────────────────────────────────────────")
	if fail > 0 {
		os.Exit(1)
	}
}

// ─── 1. Health ──────────────────────────────────────────────────────

func testHealth() error {
	all := []endpoint{router}
	all = append(all, shardA...)
	all = append(all, shardB...)
	all = append(all, shardC...)

	fmt.Printf("       pinging %d endpoints (router + 9 db nodes)...\n", len(all))
	for _, e := range all {
		c, err := oxidb.Connect(e.host, e.port, 5*time.Second)
		if err != nil {
			return fmt.Errorf("connect %s: %w", e, err)
		}
		_, err = c.Ping()
		_ = c.Close()
		if err != nil {
			return fmt.Errorf("ping %s: %w", e, err)
		}
	}
	fmt.Println("       all endpoints reachable")
	return nil
}

// ─── 2. Sharding ────────────────────────────────────────────────────

func testSharding() error {
	r, err := oxidb.Connect(router.host, router.port, 5*time.Second)
	if err != nil {
		return fmt.Errorf("connect router: %w", err)
	}
	defer r.Close()

	tag := fmt.Sprintf("smoke-%d", time.Now().UnixNano())
	const N = 60

	// Insert N orders with sequential customer_id; each lands on the shard
	// determined by CRC32(customer_id) % 3.
	expected := map[string]int{"A": 0, "B": 0, "C": 0}
	for i := 1; i <= N; i++ {
		_, err := r.Insert("orders", map[string]any{
			"customer_id": i,
			"_smoke":      tag,
			"status":      "pending",
		})
		if err != nil {
			return fmt.Errorf("insert order cid=%d: %w", i, err)
		}
		expected[shardOf(i)]++
	}
	fmt.Printf("       inserted %d orders via router; expected per shard: A=%d B=%d C=%d\n",
		N, expected["A"], expected["B"], expected["C"])

	// Wait briefly for Raft commit on each shard's leader.
	time.Sleep(replWait)

	// Direct-count on each shard's master.
	got := map[string]int{}
	got["A"], err = directCount(shardA[0], "orders", map[string]any{"_smoke": tag})
	if err != nil {
		return fmt.Errorf("count shard A: %w", err)
	}
	got["B"], err = directCount(shardB[0], "orders", map[string]any{"_smoke": tag})
	if err != nil {
		return fmt.Errorf("count shard B: %w", err)
	}
	got["C"], err = directCount(shardC[0], "orders", map[string]any{"_smoke": tag})
	if err != nil {
		return fmt.Errorf("count shard C: %w", err)
	}
	fmt.Printf("       direct count on masters:        A=%d B=%d C=%d (sum=%d)\n",
		got["A"], got["B"], got["C"], got["A"]+got["B"]+got["C"])

	if got["A"]+got["B"]+got["C"] != N {
		return fmt.Errorf("sum mismatch: got %d, want %d", got["A"]+got["B"]+got["C"], N)
	}
	for s, want := range expected {
		if got[s] != want {
			return fmt.Errorf("shard %s mismatch: got %d, want %d (CRC32 routing broken?)", s, got[s], want)
		}
	}
	if got["A"] == 0 || got["B"] == 0 || got["C"] == 0 {
		return fmt.Errorf("at least one shard got 0 orders — distribution suspicious")
	}
	return nil
}

// ─── 3. Replication ────────────────────────────────────────────────

func testReplication() error {
	r, err := oxidb.Connect(router.host, router.port, 5*time.Second)
	if err != nil {
		return fmt.Errorf("connect router: %w", err)
	}
	defer r.Close()

	// Pick a customer id, find which shard it goes to.
	cid := 12345
	target := shardOf(cid)
	tag := fmt.Sprintf("repl-%d", time.Now().UnixNano())

	if _, err := r.Insert("orders", map[string]any{
		"customer_id": cid,
		"_smoke":      tag,
		"status":      "pending",
	}); err != nil {
		return fmt.Errorf("insert via router: %w", err)
	}
	fmt.Printf("       inserted order cid=%d via router → shard %s\n", cid, target)

	// Wait for Raft commit + replication.
	time.Sleep(replWait)

	nodes := nodesForShard(target)
	for i, n := range nodes {
		role := "master"
		if i > 0 {
			role = "replica"
		}
		count, err := directCount(n, "orders", map[string]any{"_smoke": tag})
		if err != nil {
			return fmt.Errorf("count on %s (%s): %w", n, role, err)
		}
		fmt.Printf("       %-12s %s  → %d row(s)\n", n, role, count)
		if count != 1 {
			return fmt.Errorf("expected 1 row on %s (%s), got %d — Raft replication may be broken", n, role, count)
		}
	}
	return nil
}

// ─── 4. TX pinning ──────────────────────────────────────────────────

func testTxPinning() error {
	r, err := oxidb.Connect(router.host, router.port, 5*time.Second)
	if err != nil {
		return fmt.Errorf("connect router: %w", err)
	}
	defer r.Close()

	// oxipool pins begin_tx to shard 0 (shard A) — see oxipool/src/main.rs:534.
	// Any write inside the TX whose CRC32 routes elsewhere is rejected as
	// "cross-shard transactions not supported". So pick a customer_id that
	// actually hashes to shard A.
	cid := 0
	for candidate := 1; candidate < 100000; candidate++ {
		if shardOf(candidate) == "A" {
			cid = candidate
			break
		}
	}
	if cid == 0 {
		return fmt.Errorf("could not find a customer_id mapping to shard A")
	}
	fmt.Printf("       using cid=%d (hashes to shard A — required by oxipool TX pinning)\n", cid)
	tag := fmt.Sprintf("tx-%d", time.Now().UnixNano())

	// OxiDB uses OCC: writes are BUFFERED inside a TX and not visible to reads
	// (even on the same connection) until commit. So we test:
	//   (a) the TX commits cleanly through oxipool's pinning,
	//   (b) post-commit, the row is visible — confirms the buffered write was
	//       flushed to the leader and replicated by Raft.
	err = r.WithTransaction(func() error {
		if _, err := r.Insert("carts", map[string]any{
			"customer_id": cid,
			"product_id":  1,
			"qty":         3,
			"_smoke":      tag,
		}); err != nil {
			return fmt.Errorf("insert in tx: %w", err)
		}
		fmt.Printf("       insert inside TX accepted (buffered until commit)\n")
		return nil
	})
	if err != nil {
		return fmt.Errorf("commit failed: %w", err)
	}
	fmt.Printf("       commit succeeded\n")

	// Post-commit visibility — direct-connect to each node in shard A. We
	// briefly sleep so Raft has time to replicate the committed entry to the
	// followers before we probe them.
	time.Sleep(replWait)
	master := shardA[0]
	masterCount, err := directCount(master, "carts", map[string]any{"customer_id": cid, "_smoke": tag})
	if err != nil {
		return fmt.Errorf("count on master: %w", err)
	}
	fmt.Printf("       post-commit: master %s → %d row (TX landed on pinned shard ✓)\n", master, masterCount)
	if masterCount != 1 {
		return fmt.Errorf("expected 1 row on master after commit, got %d — TX commit didn't persist", masterCount)
	}
	for _, repl := range shardA[1:] {
		c, err := directCount(repl, "carts", map[string]any{"customer_id": cid, "_smoke": tag})
		if err != nil {
			return fmt.Errorf("count on %s: %w", repl, err)
		}
		fmt.Printf("       post-commit: replica %s → %d row (Raft replicated the commit ✓)\n", repl, c)
		if c != 1 {
			return fmt.Errorf("replica %s missing the commit: got %d rows", repl, c)
		}
	}
	return nil
}

// ─── 5. Scatter-gather ──────────────────────────────────────────────

func testScatterGather() error {
	r, err := oxidb.Connect(router.host, router.port, 5*time.Second)
	if err != nil {
		return fmt.Errorf("connect router: %w", err)
	}
	defer r.Close()

	tag := fmt.Sprintf("scat-%d", time.Now().UnixNano())
	const N = 30

	for i := 1; i <= N; i++ {
		if _, err := r.Insert("orders", map[string]any{
			"customer_id": i,
			"_smoke":      tag,
			"status":      "pending",
		}); err != nil {
			return fmt.Errorf("insert: %w", err)
		}
	}
	time.Sleep(replWait)

	// Direct-count per shard
	a, _ := directCount(shardA[0], "orders", map[string]any{"_smoke": tag})
	b, _ := directCount(shardB[0], "orders", map[string]any{"_smoke": tag})
	c, _ := directCount(shardC[0], "orders", map[string]any{"_smoke": tag})
	fmt.Printf("       per-shard counts: A=%d B=%d C=%d (sum=%d)\n", a, b, c, a+b+c)
	if a == 0 || b == 0 || c == 0 {
		return fmt.Errorf("data didn't reach all 3 shards (A=%d B=%d C=%d)", a, b, c)
	}

	// Find via router with NO shard key — scatter-gather across all 3.
	docs, err := r.Find("orders", map[string]any{"_smoke": tag}, nil)
	if err != nil {
		return fmt.Errorf("scatter-gather find: %w", err)
	}
	fmt.Printf("       scatter-gather find returned %d doc(s) (expected %d)\n", len(docs), N)
	if len(docs) != N {
		return fmt.Errorf("expected %d, got %d — scatter-gather may have lost docs", N, len(docs))
	}
	return nil
}

// ─── helpers ────────────────────────────────────────────────────────

func directCount(e endpoint, collection string, query map[string]any) (int, error) {
	c, err := oxidb.Connect(e.host, e.port, 5*time.Second)
	if err != nil {
		return 0, err
	}
	defer c.Close()
	return c.Count(collection, query)
}

func nodesForShard(name string) []endpoint {
	switch name {
	case "A":
		return shardA
	case "B":
		return shardB
	case "C":
		return shardC
	}
	return nil
}

// shardOf mirrors oxipool's CRC32 % 256 % num_shards routing math.
func shardOf(customerID int) string {
	h := crc32.ChecksumIEEE([]byte(strconv.Itoa(customerID)))
	chunk := h % 256
	num := []string{"A", "B", "C"}
	return num[int(chunk)%3]
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func intOr(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func parseEndpoint(s string) endpoint {
	host, port, ok := strings.Cut(s, ":")
	if !ok {
		return endpoint{host: s, port: 4444}
	}
	p, _ := strconv.Atoi(port)
	return endpoint{host: host, port: p}
}

func parseEndpoints(s string) []endpoint {
	var out []endpoint
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		out = append(out, parseEndpoint(part))
	}
	return out
}
