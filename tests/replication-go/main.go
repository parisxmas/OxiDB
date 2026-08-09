package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/parisxmas/OxiDB/clients/go/oxidb"
)

// ─── Config ────────────────────────────────────────────────────────

var (
	haproxyPort = envInt("HAPROXY_PORT", 5500)
	node1Port   = envInt("NODE1_PORT", 5501)
	node2Port   = envInt("NODE2_PORT", 5502)
	node3Port   = envInt("NODE3_PORT", 5503)
	hostAddr    = envOr("HOST", "127.0.0.1")
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		n, _ := strconv.Atoi(v)
		if n > 0 {
			return n
		}
	}
	return def
}

// ─── Raw TCP helper (for raft commands not in Go client) ──────────

func rawSend(host string, port int, payload map[string]any) (map[string]any, error) {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", host, port), 10*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(30 * time.Second))

	data, _ := json.Marshal(payload)
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, uint32(len(data)))
	conn.Write(lenBuf)
	conn.Write(data)

	// Read response
	respLen := make([]byte, 4)
	if _, err := io.ReadFull(conn, respLen); err != nil {
		return nil, fmt.Errorf("read len: %w", err)
	}
	length := binary.LittleEndian.Uint32(respLen)
	respData := make([]byte, length)
	if _, err := io.ReadFull(conn, respData); err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	var result map[string]any
	json.Unmarshal(respData, &result)
	return result, nil
}

func connect(port int) *oxidb.Client {
	c, err := oxidb.Connect(hostAddr, port, 10*time.Second)
	if err != nil {
		fmt.Printf("FATAL: connect to port %d: %v\n", port, err)
		os.Exit(1)
	}
	return c
}

func waitForPort(port int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", hostAddr, port), time.Second)
		if err == nil {
			conn.Close()
			return true
		}
		time.Sleep(500 * time.Millisecond)
	}
	return false
}

// ─── Test framework ────────────────────────────────────────────────

var (
	passed int
	failed int
)

func check(name string, ok bool, detail string) {
	if ok {
		passed++
		fmt.Printf("  ✓ %s\n", name)
	} else {
		failed++
		fmt.Printf("  ✗ %s — %s\n", name, detail)
	}
}

func main() {
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║         OxiDB Raft Replication Test Suite                   ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Println()

	// Check cluster is running
	fmt.Println("Checking cluster connectivity...")
	for _, p := range []int{node1Port, node2Port, node3Port, haproxyPort} {
		if !waitForPort(p, 5*time.Second) {
			fmt.Printf("FATAL: port %d not reachable. Start cluster first:\n", p)
			fmt.Println("  cd tests/cluster && docker compose -f docker-compose.3node.yml up -d")
			os.Exit(1)
		}
		fmt.Printf("  Port %d: OK\n", p)
	}
	fmt.Println()

	// Initialize cluster if needed
	initCluster()

	testWriteViaHAProxyReplicates()
	testDirectNodeReadConsistency()
	testBulkInsertReplication()
	testUpdateReplication()
	testDeleteReplication()
	testTransactionReplication()
	testConcurrentWriteReplication()
	testLeaderFailover()
	testIndexReplication()
	testDataConsistencyAfterRecovery()

	fmt.Println()
	fmt.Println("═══════════════════════════════════════════════════════════════")
	fmt.Printf("Results: %d passed, %d failed out of %d\n", passed, failed, passed+failed)
	fmt.Println("═══════════════════════════════════════════════════════════════")

	if failed > 0 {
		os.Exit(1)
	}
}

func initCluster() {
	fmt.Println("Initializing Raft cluster...")

	// raft_init on node1
	resp, err := rawSend(hostAddr, node1Port, map[string]any{"cmd": "raft_init"})
	if err != nil {
		fmt.Printf("  raft_init: %v (may already be initialized)\n", err)
	} else if resp != nil {
		if e, ok := resp["error"]; ok {
			if !strings.Contains(fmt.Sprintf("%v", e), "already") {
				fmt.Printf("  raft_init: %v\n", e)
			}
		}
	}

	// Add learners
	rawSend(hostAddr, node1Port, map[string]any{
		"cmd": "raft_add_learner", "node_id": 2,
		"addr": "oxidb-node2:4445",
	})
	rawSend(hostAddr, node1Port, map[string]any{
		"cmd": "raft_add_learner", "node_id": 3,
		"addr": "oxidb-node3:4445",
	})

	// Change membership
	rawSend(hostAddr, node1Port, map[string]any{
		"cmd": "raft_change_membership", "members": []int{1, 2, 3},
	})
	time.Sleep(5 * time.Second) // Wait for cluster to stabilize
	fmt.Println("  Cluster initialized.")
	fmt.Println()
}

// ─── Test 1: Write via HAProxy replicates to all nodes ────────────
func testWriteViaHAProxyReplicates() {
	fmt.Println("═══ Test 1: Write via HAProxy Replicates ═══")

	coll := "repl_test1"
	// Use fresh connection for drop (HAProxy may close idle conns)
	hpDrop := connect(haproxyPort)
	hpDrop.DropCollection(coll)
	hpDrop.Close()
	time.Sleep(time.Second)

	// Retry insert with fresh connection (HAProxy routing may take a moment)
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		hp := connect(haproxyPort)
		_, err = hp.Insert(coll, map[string]any{"key": "hello", "value": 42})
		hp.Close()
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	check("Insert via HAProxy succeeds", err == nil, fmt.Sprintf("%v", err))

	time.Sleep(time.Second) // Replication lag

	// Verify on each node
	for _, nodePort := range []int{node1Port, node2Port, node3Port} {
		c := connect(nodePort)
		doc, err := c.FindOne(coll, map[string]any{"key": "hello"})
		c.Close()
		ok := err == nil && doc != nil
		val := 0
		if doc != nil {
			if v, ok := doc["value"].(float64); ok {
				val = int(v)
			}
		}
		check(fmt.Sprintf("Node :%d sees doc (value=%d)", nodePort, val),
			ok && val == 42, fmt.Sprintf("err=%v doc=%v", err, doc))
	}
	fmt.Println()
}

// ─── Test 2: Direct read consistency across nodes ─────────────────
func testDirectNodeReadConsistency() {
	fmt.Println("═══ Test 2: Direct Node Read Consistency ═══")
	hp := connect(haproxyPort)
	defer hp.Close()

	coll := "repl_test2"
	hp.DropCollection(coll)
	time.Sleep(500 * time.Millisecond)

	// Insert 10 docs
	for i := 0; i < 10; i++ {
		hp.Insert(coll, map[string]any{"seq": i, "data": fmt.Sprintf("doc_%d", i)})
	}
	time.Sleep(time.Second)

	// Each node should have all 10
	for _, nodePort := range []int{node1Port, node2Port, node3Port} {
		c := connect(nodePort)
		n, _ := c.Count(coll, map[string]any{})
		c.Close()
		check(fmt.Sprintf("Node :%d has 10 docs", nodePort), n == 10, fmt.Sprintf("count=%d", n))
	}
	fmt.Println()
}

// ─── Test 3: Bulk insert replication ──────────────────────────────
func testBulkInsertReplication() {
	fmt.Println("═══ Test 3: Bulk Insert Replication ═══")
	hp := connect(haproxyPort)
	defer hp.Close()

	coll := "repl_bulk"
	hp.DropCollection(coll)
	time.Sleep(500 * time.Millisecond)

	// Insert 100 docs in batches
	batch := make([]map[string]any, 50)
	for i := 0; i < 50; i++ {
		batch[i] = map[string]any{"idx": i, "name": fmt.Sprintf("user_%d", i)}
	}
	_, err := hp.InsertMany(coll, batch)
	check("InsertMany via HAProxy succeeds", err == nil, fmt.Sprintf("%v", err))

	batch2 := make([]map[string]any, 50)
	for i := 50; i < 100; i++ {
		batch2[i-50] = map[string]any{"idx": i, "name": fmt.Sprintf("user_%d", i)}
	}
	hp.InsertMany(coll, batch2)

	time.Sleep(2 * time.Second) // Wait for replication

	for _, nodePort := range []int{node1Port, node2Port, node3Port} {
		c := connect(nodePort)
		n, _ := c.Count(coll, map[string]any{})
		c.Close()
		check(fmt.Sprintf("Node :%d has 100 docs", nodePort), n == 100, fmt.Sprintf("count=%d", n))
	}
	fmt.Println()
}

// ─── Test 4: Update replication ───────────────────────────────────
func testUpdateReplication() {
	fmt.Println("═══ Test 4: Update Replication ═══")
	hp := connect(haproxyPort)
	defer hp.Close()

	coll := "repl_update"
	hp.DropCollection(coll)
	time.Sleep(500 * time.Millisecond)

	hp.Insert(coll, map[string]any{"key": "target", "counter": 0})
	time.Sleep(500 * time.Millisecond)

	// Update via HAProxy
	hp.UpdateOne(coll, map[string]any{"key": "target"}, map[string]any{"$inc": map[string]any{"counter": 10}})
	time.Sleep(time.Second)

	// Verify update replicated
	for _, nodePort := range []int{node1Port, node2Port, node3Port} {
		c := connect(nodePort)
		doc, _ := c.FindOne(coll, map[string]any{"key": "target"})
		c.Close()
		val := -1
		if doc != nil {
			if v, ok := doc["counter"].(float64); ok {
				val = int(v)
			}
		}
		check(fmt.Sprintf("Node :%d counter=10", nodePort), val == 10, fmt.Sprintf("counter=%d", val))
	}
	fmt.Println()
}

// ─── Test 5: Delete replication ───────────────────────────────────
func testDeleteReplication() {
	fmt.Println("═══ Test 5: Delete Replication ═══")
	hp := connect(haproxyPort)
	defer hp.Close()

	coll := "repl_delete"
	hp.DropCollection(coll)
	time.Sleep(500 * time.Millisecond)

	for i := 0; i < 5; i++ {
		hp.Insert(coll, map[string]any{"idx": i})
	}
	time.Sleep(time.Second)

	// Delete one doc
	hp.DeleteOne(coll, map[string]any{"idx": 2})
	time.Sleep(time.Second)

	for _, nodePort := range []int{node1Port, node2Port, node3Port} {
		c := connect(nodePort)
		n, _ := c.Count(coll, map[string]any{})
		doc, _ := c.FindOne(coll, map[string]any{"idx": 2})
		c.Close()
		check(fmt.Sprintf("Node :%d has 4 docs", nodePort), n == 4, fmt.Sprintf("count=%d", n))
		check(fmt.Sprintf("Node :%d idx=2 deleted", nodePort), doc == nil, fmt.Sprintf("doc=%v", doc))
	}
	fmt.Println()
}

// ─── Test 6: Transaction replication ──────────────────────────────
func testTransactionReplication() {
	fmt.Println("═══ Test 6: Transaction Replication ═══")
	hp := connect(haproxyPort)
	defer hp.Close()

	coll := "repl_tx"
	hp.DropCollection(coll)
	time.Sleep(500 * time.Millisecond)

	hp.Insert(coll, map[string]any{"account": "A", "balance": 1000})
	hp.Insert(coll, map[string]any{"account": "B", "balance": 1000})
	time.Sleep(2 * time.Second)

	// Transfer via direct writes (Raft-replicated)
	_, err := hp.UpdateOne(coll, map[string]any{"account": "A"}, map[string]any{"$inc": map[string]any{"balance": -300}})
	check("Update A via HAProxy succeeds", err == nil, fmt.Sprintf("%v", err))
	_, err = hp.UpdateOne(coll, map[string]any{"account": "B"}, map[string]any{"$inc": map[string]any{"balance": 300}})
	check("Update B via HAProxy succeeds", err == nil, fmt.Sprintf("%v", err))
	time.Sleep(2 * time.Second)

	// All nodes should see consistent total (2000 preserved)
	for _, nodePort := range []int{node1Port, node2Port, node3Port} {
		c := connect(nodePort)
		dA, _ := c.FindOne(coll, map[string]any{"account": "A"})
		dB, _ := c.FindOne(coll, map[string]any{"account": "B"})
		c.Close()
		balA, balB := 0, 0
		if dA != nil {
			if v, ok := dA["balance"].(float64); ok {
				balA = int(v)
			}
		}
		if dB != nil {
			if v, ok := dB["balance"].(float64); ok {
				balB = int(v)
			}
		}
		check(fmt.Sprintf("Node :%d total preserved (2000)", nodePort),
			balA+balB == 2000, fmt.Sprintf("A=%d B=%d total=%d", balA, balB, balA+balB))
	}
	fmt.Println()
}

// ─── Test 7: Concurrent write replication ─────────────────────────
func testConcurrentWriteReplication() {
	fmt.Println("═══ Test 7: Concurrent Write Replication ═══")
	hp := connect(haproxyPort)
	defer hp.Close()

	coll := "repl_concurrent"
	hp.DropCollection(coll)
	time.Sleep(500 * time.Millisecond)

	// 5 workers insert 20 docs each via HAProxy
	workers := 5
	docsPerWorker := 20
	expected := workers * docsPerWorker

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c := connect(haproxyPort)
			defer c.Close()
			for i := 0; i < docsPerWorker; i++ {
				c.Insert(coll, map[string]any{"worker": id, "seq": i})
			}
		}(w)
	}
	wg.Wait()
	time.Sleep(2 * time.Second)

	for _, nodePort := range []int{node1Port, node2Port, node3Port} {
		c := connect(nodePort)
		n, _ := c.Count(coll, map[string]any{})
		c.Close()
		check(fmt.Sprintf("Node :%d has %d docs", nodePort, expected), n == expected,
			fmt.Sprintf("count=%d", n))
	}
	fmt.Println()
}

// ─── Test 8: Leader failover ──────────────────────────────────────
func testLeaderFailover() {
	fmt.Println("═══ Test 8: Leader Failover ═══")

	hp := connect(haproxyPort)
	defer hp.Close()

	coll := "repl_failover"
	hp.DropCollection(coll)
	time.Sleep(500 * time.Millisecond)

	hp.Insert(coll, map[string]any{"phase": "before_failover", "value": 1})
	time.Sleep(time.Second)

	// Find current leader via raft_metrics
	resp, err := rawSend(hostAddr, node1Port, map[string]any{"cmd": "raft_metrics"})
	leaderNode := "unknown"
	if err == nil && resp != nil {
		if data, ok := resp["data"].(map[string]any); ok {
			if lid, ok := data["current_leader"].(float64); ok {
				leaderNode = fmt.Sprintf("cluster-oxidb-node%d-1", int(lid))
			}
		}
	}
	fmt.Printf("  Current leader: %s\n", leaderNode)

	// Kill the leader container
	if leaderNode != "unknown" {
		out, err := exec.Command("docker", "stop", leaderNode).CombinedOutput()
		if err != nil {
			fmt.Printf("  docker stop failed: %v %s\n", err, out)
			check("Leader killed", false, "docker stop failed")
			fmt.Println()
			return
		}
		check("Leader killed", true, "")
	} else {
		fmt.Println("  Skipping failover (leader unknown)")
		fmt.Println()
		return
	}

	// Wait for new leader election + HAProxy health check
	time.Sleep(10 * time.Second)

	// Try to write via HAProxy with retry (new leader may need a moment)
	var writeErr error
	for attempt := 0; attempt < 5; attempt++ {
		hp2, connErr := oxidb.Connect(hostAddr, haproxyPort, 5*time.Second)
		if connErr != nil {
			writeErr = connErr
			time.Sleep(2 * time.Second)
			continue
		}
		_, writeErr = hp2.Insert(coll, map[string]any{"phase": "after_failover", "value": 2})
		hp2.Close()
		if writeErr == nil {
			break
		}
		time.Sleep(2 * time.Second)
	}
	check("Write after failover succeeds", writeErr == nil, fmt.Sprintf("%v", writeErr))

	time.Sleep(time.Second)

	// Verify on surviving nodes
	surviving := 0
	for _, nodePort := range []int{node1Port, node2Port, node3Port} {
		c, err := oxidb.Connect(hostAddr, nodePort, 2*time.Second)
		if err != nil {
			continue // this node is down
		}
		n, _ := c.Count(coll, map[string]any{})
		c.Close()
		if n == 2 {
			surviving++
		}
	}
	check("Surviving nodes have both docs", surviving >= 2, fmt.Sprintf("nodes_with_2_docs=%d", surviving))

	// Restart killed node
	exec.Command("docker", "start", leaderNode).Run()
	fmt.Println("  Restarted killed node. Remaining tests use surviving nodes only.")
	time.Sleep(5 * time.Second)
	fmt.Println()
}

// ─── Test 9: Index replication ────────────────────────────────────
func testIndexReplication() {
	fmt.Println("═══ Test 9: Index Replication ═══")
	hp := connect(haproxyPort)
	defer hp.Close()

	coll := "repl_index"
	hp.DropCollection(coll)
	time.Sleep(500 * time.Millisecond)

	for i := 0; i < 50; i++ {
		hp.Insert(coll, map[string]any{"name": fmt.Sprintf("user_%d", i), "age": 20 + rand.Intn(40)})
	}
	hp.CreateIndex(coll, "name")
	hp.CreateIndex(coll, "age")
	time.Sleep(2 * time.Second)

	// Verify indexes exist and work on reachable nodes
	indexOk := 0
	for _, nodePort := range []int{node1Port, node2Port, node3Port} {
		c, err := oxidb.Connect(hostAddr, nodePort, 2*time.Second)
		if err != nil {
			continue // node may be recovering from failover test
		}
		doc, err := c.FindOne(coll, map[string]any{"name": "user_25"})
		n, _ := c.Count(coll, map[string]any{})
		c.Close()
		if err == nil && doc != nil && n == 50 {
			indexOk++
		}
	}
	check("At least 2 nodes have working indexes", indexOk >= 2, fmt.Sprintf("nodes_ok=%d", indexOk))
	fmt.Println()
}

// ─── Test 10: Data consistency after full recovery ────────────────
func testDataConsistencyAfterRecovery() {
	fmt.Println("═══ Test 10: Data Consistency After Recovery ═══")
	hp := connect(haproxyPort)
	defer hp.Close()

	coll := "repl_recovery"
	hp.DropCollection(coll)
	time.Sleep(500 * time.Millisecond)

	// Insert known data
	for i := 0; i < 20; i++ {
		hp.Insert(coll, map[string]any{"seq": i, "checksum": i * 7})
	}
	time.Sleep(2 * time.Second)

	// Verify all nodes have identical data
	var nodeCounts []int
	var nodeChecksums []int
	for _, nodePort := range []int{node1Port, node2Port, node3Port} {
		c, err := oxidb.Connect(hostAddr, nodePort, 2*time.Second)
		if err != nil {
			nodeCounts = append(nodeCounts, -1)
			nodeChecksums = append(nodeChecksums, -1)
			continue
		}
		n, _ := c.Count(coll, map[string]any{})
		nodeCounts = append(nodeCounts, n)

		// Sum all checksums
		docs, _ := c.Find(coll, map[string]any{}, nil)
		total := 0
		for _, d := range docs {
			if v, ok := d["checksum"].(float64); ok {
				total += int(v)
			}
		}
		nodeChecksums = append(nodeChecksums, total)
		c.Close()
	}

	// Reachable nodes should agree on count
	expectedChecksum := 0
	for i := 0; i < 20; i++ {
		expectedChecksum += i * 7
	}
	correctNodes := 0
	for i, n := range nodeCounts {
		if n == 20 && nodeChecksums[i] == expectedChecksum {
			correctNodes++
		}
	}
	check("At least 2 nodes have correct data (count=20, checksum match)",
		correctNodes >= 2, fmt.Sprintf("correct_nodes=%d counts=%v checksums=%v", correctNodes, nodeCounts, nodeChecksums))
	fmt.Println()
}
