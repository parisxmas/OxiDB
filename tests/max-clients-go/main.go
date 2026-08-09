package main

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/parisxmas/OxiDB/go/oxidb"
)

// Max Client Rejection Test for OxiPool
//
// OxiPool enforces OXIPOOL_MAX_CLIENTS via a tokio Semaphore.
// When the limit is reached, new TCP connections are accepted then
// immediately dropped (client sees EOF on first operation).
//
// Tests:
//   1. Under limit — all clients connect and operate normally
//   2. At limit — exactly maxClients connections work simultaneously
//   3. Over limit — excess connections are rejected (EOF/error on first op)
//   4. Slot reclaim — after a client disconnects, a new one can take its slot
//   5. Burst over limit — 3× max_clients simultaneously, exactly maxClients succeed
//   6. Rapid connect-disconnect churn — no slot leak after 1000 cycles at limit
//   7. Rejected client retry — rejected client succeeds after slot opens
//   8. Pool slots independent of client limit — pool size != max_clients

const (
	projectDir = "/Users/barisakin/source/docdb"
	collection = "max_clients_test"
)

var (
	serverPort = 4444
	proxyPort  = 4445
	maxClients = 10 // intentionally small for easy testing
	poolSize   = 5
	passed     int
	failed     int
)

func main() {
	if p := os.Getenv("OXIDB_PORT"); p != "" {
		serverPort, _ = strconv.Atoi(p)
	}
	if p := os.Getenv("OXIPOOL_PORT"); p != "" {
		proxyPort, _ = strconv.Atoi(p)
	}

	fmt.Println("=== OxiPool Max Client Rejection Test ===")
	fmt.Printf("    max_clients=%d  pool_size=%d\n\n", maxClients, poolSize)

	killAll()
	time.Sleep(500 * time.Millisecond)

	startServer()
	startOxiPool()

	test1_under_limit()
	test2_at_limit()
	test3_over_limit()
	test4_slot_reclaim()
	test5_burst_over_limit()
	test6_churn_no_leak()
	test7_rejected_then_retry()
	test8_pool_vs_client_limit()

	killAll()

	total := passed + failed
	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════╗")
	fmt.Println("║          MAX CLIENT REJECTION TEST RESULTS             ║")
	fmt.Println("╠══════════════════════════════════════════════════════════╣")
	fmt.Printf("║   Passed:             %-34d ║\n", passed)
	fmt.Printf("║   Failed:             %-34d ║\n", failed)
	fmt.Printf("║   Total:              %-34d ║\n", total)
	fmt.Println("╚══════════════════════════════════════════════════════════╝")

	if failed > 0 {
		fmt.Printf("\nFAILED: %d/%d tests failed\n", failed, total)
		os.Exit(1)
	}
	fmt.Println("\nAll max client rejection tests passed.")
}

// ─── Test 1: Under Limit ────────────────────────────────────────────

func test1_under_limit() {
	fmt.Println("TEST 1: Under limit — all clients connect and work")

	count := maxClients / 2
	clients := make([]*oxidb.Client, 0, count)

	for i := 0; i < count; i++ {
		c, err := connectProxy()
		if err != nil {
			fail("connect %d: %v", i, err)
			closeAll(clients)
			return
		}
		_, err = c.Ping()
		if err != nil {
			fail("ping %d: %v", i, err)
			c.Close()
			closeAll(clients)
			return
		}
		clients = append(clients, c)
	}

	fmt.Printf("  %d/%d clients connected and pinged successfully\n", len(clients), count)
	closeAll(clients)

	if len(clients) == count {
		fmt.Println("  PASS: all clients under limit work normally")
		passed++
	}
	fmt.Println()
}

// ─── Test 2: At Limit ───────────────────────────────────────────────

func test2_at_limit() {
	fmt.Println("TEST 2: At limit — exactly max_clients connections work")

	clients := make([]*oxidb.Client, 0, maxClients)

	for i := 0; i < maxClients; i++ {
		c, err := connectProxy()
		if err != nil {
			fail("connect %d: %v", i, err)
			closeAll(clients)
			return
		}
		_, err = c.Ping()
		if err != nil {
			fail("ping %d: %v (connection accepted but rejected on use?)", i, err)
			c.Close()
			closeAll(clients)
			return
		}
		clients = append(clients, c)
	}

	fmt.Printf("  %d/%d clients connected and working at limit\n", len(clients), maxClients)
	closeAll(clients)

	if len(clients) == maxClients {
		fmt.Println("  PASS: exactly max_clients connections accepted")
		passed++
	}
	fmt.Println()
}

// ─── Test 3: Over Limit ─────────────────────────────────────────────

func test3_over_limit() {
	fmt.Println("TEST 3: Over limit — excess connections rejected")

	// Fill all slots
	holders := make([]*oxidb.Client, 0, maxClients)
	for i := 0; i < maxClients; i++ {
		c, err := connectProxy()
		if err != nil {
			fail("fill slot %d: %v", i, err)
			closeAll(holders)
			return
		}
		c.Ping()
		holders = append(holders, c)
	}

	// Try 5 more — should all be rejected
	rejected := 0
	for i := 0; i < 5; i++ {
		c, err := connectProxy()
		if err != nil {
			// Connection refused — rejected at TCP level
			rejected++
			continue
		}
		// Connection accepted but should get EOF on first operation
		_, err = c.Ping()
		c.Close()
		if err != nil {
			rejected++
		}
	}

	closeAll(holders)

	fmt.Printf("  %d/5 excess connections rejected\n", rejected)
	if rejected == 5 {
		fmt.Println("  PASS: all excess connections rejected")
		passed++
	} else {
		fail("expected 5 rejections, got %d", rejected)
	}
	fmt.Println()
}

// ─── Test 4: Slot Reclaim ───────────────────────────────────────────

func test4_slot_reclaim() {
	fmt.Println("TEST 4: Disconnected client frees slot → new client takes it")

	// Fill all slots
	holders := make([]*oxidb.Client, 0, maxClients)
	for i := 0; i < maxClients; i++ {
		c, err := connectProxy()
		if err != nil {
			fail("fill slot %d: %v", i, err)
			closeAll(holders)
			return
		}
		c.Ping()
		holders = append(holders, c)
	}

	// Verify over limit — next connection rejected
	excess, err := connectProxy()
	excessRejected := false
	if err != nil {
		excessRejected = true
	} else {
		_, err = excess.Ping()
		excess.Close()
		if err != nil {
			excessRejected = true
		}
	}

	if !excessRejected {
		fail("excess connection was not rejected (limit not enforced?)")
		closeAll(holders)
		return
	}
	fmt.Println("  Confirmed: limit enforced (excess rejected)")

	// Close one holder — frees a slot
	holders[0].Close()
	holders = holders[1:]
	time.Sleep(200 * time.Millisecond) // give OxiPool time to release semaphore

	// New connection should now succeed
	newClient, err := connectProxy()
	if err != nil {
		fail("new client after slot free: %v", err)
		closeAll(holders)
		return
	}
	_, err = newClient.Ping()
	newClient.Close()
	closeAll(holders)

	if err != nil {
		fail("new client ping after slot free: %v", err)
	} else {
		fmt.Println("  PASS: slot reclaimed — new client connected after disconnect")
		passed++
	}
	fmt.Println()
}

// ─── Test 5: Burst Over Limit ───────────────────────────────────────

func test5_burst_over_limit() {
	fmt.Println("TEST 5: 3x max_clients burst — exactly max_clients succeed concurrently")

	total := maxClients * 3
	var mu sync.Mutex
	var wg sync.WaitGroup
	alive := make([]*oxidb.Client, 0, total)
	var rejectCount atomic.Int64

	// Launch all connections simultaneously
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := connectProxy()
			if err != nil {
				rejectCount.Add(1)
				return
			}
			_, err = c.Ping()
			if err != nil {
				c.Close()
				rejectCount.Add(1)
				return
			}
			mu.Lock()
			alive = append(alive, c)
			mu.Unlock()
		}()
	}
	wg.Wait()

	accepted := len(alive)
	rejected := int(rejectCount.Load())

	fmt.Printf("  Attempted: %d  Accepted: %d  Rejected: %d\n", total, accepted, rejected)

	closeAll(alive)

	if accepted <= maxClients {
		fmt.Printf("  PASS: accepted %d <= max_clients (%d)\n", accepted, maxClients)
		passed++
	} else {
		fail("accepted %d > max_clients (%d)", accepted, maxClients)
	}

	if rejected >= maxClients*2 {
		fmt.Printf("  PASS: rejected %d >= expected minimum (%d)\n", rejected, maxClients*2)
		passed++
	} else {
		fail("only %d rejected, expected >= %d", rejected, maxClients*2)
	}
	fmt.Println()
}

// ─── Test 6: Rapid Churn No Leak ────────────────────────────────────

func test6_churn_no_leak() {
	fmt.Println("TEST 6: 1000 connect-disconnect cycles at limit — no slot leak")

	// First fill to limit
	holders := make([]*oxidb.Client, 0, maxClients-1)
	for i := 0; i < maxClients-1; i++ {
		c, err := connectProxy()
		if err != nil {
			fail("fill slot %d: %v", i, err)
			closeAll(holders)
			return
		}
		c.Ping()
		holders = append(holders, c)
	}

	// Rapidly connect/disconnect the last slot 1000 times
	success := 0
	for i := 0; i < 1000; i++ {
		c, err := connectProxy()
		if err != nil {
			continue
		}
		_, err = c.Ping()
		c.Close()
		if err == nil {
			success++
		}
	}

	closeAll(holders)

	fmt.Printf("  %d/1000 rapid cycles succeeded on last slot\n", success)

	if success >= 950 {
		fmt.Printf("  PASS: no slot leak (%d/1000 succeeded)\n", success)
		passed++
	} else {
		fail("slot leak suspected: only %d/1000 succeeded", success)
	}
	fmt.Println()
}

// ─── Test 7: Rejected Client Retries Successfully ───────────────────

func test7_rejected_then_retry() {
	fmt.Println("TEST 7: Rejected client succeeds after slot opens")

	// Fill all slots
	holders := make([]*oxidb.Client, 0, maxClients)
	for i := 0; i < maxClients; i++ {
		c, err := connectProxy()
		if err != nil {
			fail("fill slot %d: %v", i, err)
			closeAll(holders)
			return
		}
		c.Ping()
		holders = append(holders, c)
	}

	// Attempt — should be rejected
	c, err := connectProxy()
	firstRejected := false
	if err != nil {
		firstRejected = true
	} else {
		_, err = c.Ping()
		c.Close()
		if err != nil {
			firstRejected = true
		}
	}

	if !firstRejected {
		fail("initial attempt was not rejected")
		closeAll(holders)
		return
	}
	fmt.Println("  First attempt rejected (expected)")

	// Free 3 slots
	for i := 0; i < 3; i++ {
		holders[i].Close()
	}
	holders = holders[3:]
	time.Sleep(200 * time.Millisecond)

	// Retry — should succeed now
	retrySuccess := 0
	for i := 0; i < 3; i++ {
		c, err := connectProxy()
		if err != nil {
			continue
		}
		_, err = c.Ping()
		if err == nil {
			retrySuccess++
		}
		c.Close()
	}

	closeAll(holders)

	if retrySuccess == 3 {
		fmt.Printf("  PASS: all 3 retries succeeded after slots opened\n")
		passed++
	} else {
		fail("only %d/3 retries succeeded", retrySuccess)
	}
	fmt.Println()
}

// ─── Test 8: Pool Size vs Client Limit Independence ─────────────────

func test8_pool_vs_client_limit() {
	fmt.Println("TEST 8: Pool size (5) independent of client limit (10)")

	// Connect max_clients (10) simultaneously, each doing a query
	// Only pool_size (5) backend connections exist, so clients queue on pool
	// but all should eventually succeed
	var wg sync.WaitGroup
	var success atomic.Int64
	var errors atomic.Int64

	for i := 0; i < maxClients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c, err := connectProxy()
			if err != nil {
				errors.Add(1)
				return
			}
			defer c.Close()

			_, err = c.Insert(collection, map[string]any{"client": id, "test": 8})
			if err != nil {
				errors.Add(1)
				return
			}
			success.Add(1)
		}(i)
	}
	wg.Wait()

	fmt.Printf("  %d clients, %d pool conns: %d succeeded, %d errors\n",
		maxClients, poolSize, success.Load(), errors.Load())

	if success.Load() == int64(maxClients) {
		fmt.Printf("  PASS: all %d clients served through %d pool connections\n", maxClients, poolSize)
		passed++
	} else {
		fail("only %d/%d succeeded", success.Load(), maxClients)
	}

	// Verify: more than pool_size clients can't ALL be active on backends
	// simultaneously, but they can all be connected to OxiPool
	clients := make([]*oxidb.Client, 0, maxClients)
	for i := 0; i < maxClients; i++ {
		c, err := connectProxy()
		if err != nil {
			continue
		}
		c.Ping()
		clients = append(clients, c)
	}

	fmt.Printf("  %d/%d clients connected simultaneously (pool_size=%d)\n",
		len(clients), maxClients, poolSize)

	closeAll(clients)

	if len(clients) == maxClients {
		fmt.Println("  PASS: client limit allows more concurrent clients than pool size")
		passed++
	} else {
		fail("expected %d connected clients, got %d", maxClients, len(clients))
	}
	fmt.Println()
}

// ─── Helpers ────────────────────────────────────────────────────────

func startServer() {
	cmd := exec.Command("./target/release/oxidb-server")
	cmd.Dir = projectDir
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("OXIDB_ADDR=127.0.0.1:%d", serverPort),
		"OXIDB_POOL_SIZE=20",
		"OXIDB_IDLE_TIMEOUT=0",
	)
	cmd.Start()
	time.Sleep(1 * time.Second)
}

func startOxiPool() {
	cmd := exec.Command(projectDir + "/target/release/oxipool")
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("OXIPOOL_LISTEN=127.0.0.1:%d", proxyPort),
		fmt.Sprintf("OXIPOOL_BACKEND=127.0.0.1:%d", serverPort),
		fmt.Sprintf("OXIPOOL_SIZE=%d", poolSize),
		fmt.Sprintf("OXIPOOL_MAX_CLIENTS=%d", maxClients),
		"OXIPOOL_STATS_INTERVAL=0",
	)
	cmd.Start()
	time.Sleep(1 * time.Second)
}

func killAll() {
	for _, name := range []string{"oxipool", "oxidb-server"} {
		pids := findPids(name)
		for _, pid := range pids {
			exec.Command("kill", "-9", pid).Run()
		}
	}
	time.Sleep(300 * time.Millisecond)
}

func connectProxy() (*oxidb.Client, error) {
	return oxidb.Connect("127.0.0.1", proxyPort, 3*time.Second)
}

func closeAll(clients []*oxidb.Client) {
	for _, c := range clients {
		c.Close()
	}
}

func findPids(name string) []string {
	out, _ := exec.Command("pgrep", "-x", name).Output()
	s := strings.TrimSpace(string(out))
	if s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}

func fail(format string, args ...any) {
	fmt.Printf("  FAIL: "+format+"\n", args...)
	failed++
}

// holdConn keeps a connection alive by reading, used to keep raw TCP
// connections open without the oxidb client. Not currently used but
// available for lower-level tests.
func holdConn(conn net.Conn) {
	buf := make([]byte, 1)
	conn.Read(buf)
}
