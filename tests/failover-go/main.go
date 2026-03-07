package main

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/parisxmas/OxiDB/go/oxidb"
)

// Backend Failover Test for OxiPool
//
// Tests:
//   1. Kill server while clients are active → clients get errors, pool replaces conns on restart
//   2. Kill server during transaction → transaction fails cleanly, no hang
//   3. Server restart → OxiPool auto-reconnects, clients resume without restart
//   4. Rapid server restart (flap) → pool recovers stability
//   5. Server kill under sustained load → load resumes after restart
//   6. All pool connections die at once → pool rebuilds fully

const (
	projectDir = "/Users/barisakin/source/docdb"
	serverBin  = "./target/release/oxidb-server"
	collection = "failover_test"
)

var (
	serverPort = 4444
	proxyPort  = 4445
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

	fmt.Println("=== OxiPool Backend Failover Test ===")
	fmt.Println()

	// Make sure we start clean
	killAll()
	time.Sleep(500 * time.Millisecond)

	test1_kill_server_clients_get_errors()
	test2_kill_server_during_transaction()
	test3_server_restart_auto_reconnect()
	test4_rapid_server_flap()
	test5_kill_under_load()
	test6_all_pool_conns_die()

	// Cleanup
	killAll()

	// Report
	total := passed + failed
	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════╗")
	fmt.Println("║              FAILOVER TEST RESULTS                      ║")
	fmt.Println("╠══════════════════════════════════════════════════════════╣")
	fmt.Printf("║   Passed:             %-34d ║\n", passed)
	fmt.Printf("║   Failed:             %-34d ║\n", failed)
	fmt.Printf("║   Total:              %-34d ║\n", total)
	fmt.Println("╚══════════════════════════════════════════════════════════╝")

	if failed > 0 {
		fmt.Printf("\nFAILED: %d/%d tests failed\n", failed, total)
		os.Exit(1)
	}
	fmt.Println("\nAll failover tests passed.")
}

// ─── Test 1: Kill server → clients get clean errors ─────────────────

func test1_kill_server_clients_get_errors() {
	fmt.Println("TEST 1: Kill server → clients get clean errors from OxiPool")

	startServer()
	startOxiPool()
	defer killOxiPool()
	defer killServer()

	// Insert some data through proxy
	proxy, err := connectProxy()
	if err != nil {
		fail("connect proxy: %v", err)
		return
	}
	_, err = proxy.Insert(collection, map[string]any{"test": 1, "data": "before_kill"})
	if err != nil {
		fail("insert: %v", err)
		proxy.Close()
		return
	}
	proxy.Close()

	// Kill server
	fmt.Println("  Killing oxidb-server...")
	killServer()
	time.Sleep(500 * time.Millisecond)

	// Try operation through proxy — should get a clean error, not hang
	proxy2, err := connectProxy()
	if err != nil {
		// Can't even connect through proxy — that's acceptable
		fmt.Println("  PASS: proxy correctly refused connection (server down)")
		passed++
		return
	}

	done := make(chan error, 1)
	go func() {
		_, err := proxy2.Insert(collection, map[string]any{"test": 1, "data": "during_kill"})
		done <- err
	}()

	select {
	case err := <-done:
		proxy2.Close()
		if err != nil {
			fmt.Printf("  PASS: got clean error (no hang): %v\n", truncate(err.Error(), 60))
			passed++
		} else {
			fail("insert succeeded after server kill — should have failed")
		}
	case <-time.After(10 * time.Second):
		proxy2.Close()
		fail("operation hung for 10s after server kill")
	}
	fmt.Println()
}

// ─── Test 2: Kill server during transaction → clean failure ─────────

func test2_kill_server_during_transaction() {
	fmt.Println("TEST 2: Kill server during active transaction → clean failure")

	startServer()
	startOxiPool()
	defer killOxiPool()
	defer killServer()

	proxy, err := connectProxy()
	if err != nil {
		fail("connect: %v", err)
		return
	}

	// Begin transaction
	_, err = proxy.BeginTx()
	if err != nil {
		fail("begin_tx: %v", err)
		proxy.Close()
		return
	}

	_, err = proxy.Insert(collection, map[string]any{"test": 2, "data": "in_tx"})
	if err != nil {
		fail("insert in tx: %v", err)
		proxy.Close()
		return
	}

	// Kill server while transaction is open
	fmt.Println("  Killing server with open transaction...")
	killServer()
	time.Sleep(500 * time.Millisecond)

	// Try to commit — should fail cleanly
	done := make(chan error, 1)
	go func() {
		done <- proxy.CommitTx()
	}()

	select {
	case err := <-done:
		proxy.Close()
		if err != nil {
			fmt.Printf("  PASS: commit failed cleanly: %v\n", truncate(err.Error(), 60))
			passed++
		} else {
			fail("commit succeeded after server kill")
		}
	case <-time.After(10 * time.Second):
		proxy.Close()
		fail("commit hung for 10s")
	}

	// Restart server and verify no data leaked
	startServer()
	time.Sleep(500 * time.Millisecond)

	direct, err := connectDirect()
	if err != nil {
		fail("reconnect direct: %v", err)
		return
	}
	count, _ := direct.Count(collection, map[string]any{"test": 2})
	direct.Close()

	if count == 0 {
		fmt.Println("  PASS: 0 docs from killed transaction (no data leak)")
		passed++
	} else {
		fail("found %d docs from killed transaction, expected 0", count)
	}
	fmt.Println()
}

// ─── Test 3: Server restart → OxiPool auto-reconnects ───────────────

func test3_server_restart_auto_reconnect() {
	fmt.Println("TEST 3: Server restart → OxiPool auto-reconnects pool")

	startServer()
	startOxiPool()
	defer killOxiPool()
	defer killServer()

	// Insert data before restart
	proxy, err := connectProxy()
	if err != nil {
		fail("connect: %v", err)
		return
	}
	_, err = proxy.Insert(collection, map[string]any{"test": 3, "phase": "before"})
	proxy.Close()
	if err != nil {
		fail("insert before: %v", err)
		return
	}

	// Kill and restart server
	fmt.Println("  Restarting server...")
	killServer()
	time.Sleep(1 * time.Second)
	startServer()
	time.Sleep(1 * time.Second)

	// Try operations through proxy — pool should auto-reconnect
	var lastErr error
	for attempt := 1; attempt <= 5; attempt++ {
		proxy2, err := connectProxy()
		if err != nil {
			lastErr = err
			time.Sleep(500 * time.Millisecond)
			continue
		}
		_, lastErr = proxy2.Insert(collection, map[string]any{"test": 3, "phase": "after", "attempt": attempt})
		proxy2.Close()
		if lastErr == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	if lastErr != nil {
		fail("could not resume after server restart: %v", lastErr)
	} else {
		fmt.Println("  PASS: operations resumed after server restart")
		passed++
	}

	// Verify data from before restart is gone (fresh data dir)
	// and new data exists
	direct, err := connectDirect()
	if err == nil {
		count, _ := direct.Count(collection, map[string]any{"test": 3, "phase": "after"})
		direct.Close()
		if count >= 1 {
			fmt.Printf("  PASS: %d new docs found after restart\n", count)
			passed++
		} else {
			fail("no new docs after restart")
		}
	} else {
		fail("cannot verify: %v", err)
	}
	fmt.Println()
}

// ─── Test 4: Rapid server flap → pool recovers stability ────────────

func test4_rapid_server_flap() {
	fmt.Println("TEST 4: Rapid server restart (flap) → pool recovers")

	startServer()
	startOxiPool()
	defer killOxiPool()
	defer killServer()

	// Flap the server 3 times rapidly
	for i := 1; i <= 3; i++ {
		fmt.Printf("  Flap %d/3: kill → restart\n", i)
		killServer()
		time.Sleep(300 * time.Millisecond)
		startServer()
		time.Sleep(500 * time.Millisecond)
	}

	// Wait for pool to stabilize
	time.Sleep(2 * time.Second)

	// Verify pool recovered: do 10 operations
	success := 0
	for i := 0; i < 10; i++ {
		proxy, err := connectProxy()
		if err != nil {
			continue
		}
		_, err = proxy.Insert(collection, map[string]any{"test": 4, "op": i})
		proxy.Close()
		if err == nil {
			success++
		}
	}

	if success >= 8 {
		fmt.Printf("  PASS: %d/10 ops succeeded after 3 flaps\n", success)
		passed++
	} else {
		fail("%d/10 ops succeeded, expected >= 8", success)
	}
	fmt.Println()
}

// ─── Test 5: Kill under sustained load → load resumes ───────────────

func test5_kill_under_load() {
	fmt.Println("TEST 5: Kill server under sustained load → load resumes after restart")

	startServer()
	startOxiPool()
	defer killOxiPool()
	defer killServer()

	// Seed some data
	proxy, _ := connectProxy()
	if proxy != nil {
		_ = proxy.CreateCollection(collection)
		for i := 0; i < 50; i++ {
			proxy.Insert(collection, map[string]any{"test": 5, "i": i, "status": "active"})
		}
		proxy.Close()
	}

	var opsBeforeKill atomic.Int64
	var opsAfterRestart atomic.Int64
	var errsDuringKill atomic.Int64
	stopLoad := make(chan struct{})

	// Start 5 concurrent workers doing continuous ops through proxy
	var wg sync.WaitGroup
	for w := 0; w < 5; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stopLoad:
					return
				default:
				}
				c, err := connectProxy()
				if err != nil {
					errsDuringKill.Add(1)
					time.Sleep(1 * time.Second)
					continue
				}
				_, err = c.FindOne(collection, map[string]any{"test": 5})
				c.Close()
				if err != nil {
					errsDuringKill.Add(1)
					time.Sleep(500 * time.Millisecond)
				} else {
					opsBeforeKill.Add(1)
					opsAfterRestart.Add(1)
				}
				time.Sleep(100 * time.Millisecond)
			}
		}(w)
	}

	// Let load run for 2 seconds
	time.Sleep(2 * time.Second)
	beforeKillOps := opsBeforeKill.Load()
	fmt.Printf("  Ops before kill: %d\n", beforeKillOps)

	// Kill server
	fmt.Println("  Killing server under load...")
	killServer()
	time.Sleep(2 * time.Second)
	errCount := errsDuringKill.Load()
	fmt.Printf("  Errors during downtime: %d\n", errCount)

	// Restart server and give pool time to rebuild
	fmt.Println("  Restarting server...")
	startServer()
	time.Sleep(5 * time.Second) // pool needs time to rebuild dead connections

	// Reset counter to measure post-restart ops
	snapshot := opsAfterRestart.Load()
	time.Sleep(3 * time.Second)
	afterOps := opsAfterRestart.Load() - snapshot

	close(stopLoad)
	wg.Wait()

	fmt.Printf("  Ops after restart (2s window): %d\n", afterOps)

	if errCount > 0 && afterOps > 0 {
		fmt.Println("  PASS: errors during downtime, load resumed after restart")
		passed++
	} else if afterOps == 0 {
		fail("no successful ops after server restart")
	} else {
		fail("unexpected: no errors during server downtime")
	}
	fmt.Println()
}

// ─── Test 6: All pool connections die → pool rebuilds ────────────────

func test6_all_pool_conns_die() {
	fmt.Println("TEST 6: All pool connections die at once → pool rebuilds")

	startServer()
	startOxiPool()
	defer killOxiPool()
	defer killServer()

	// Verify pool works
	proxy, err := connectProxy()
	if err != nil {
		fail("initial connect: %v", err)
		return
	}
	_, err = proxy.Insert(collection, map[string]any{"test": 6, "phase": "initial"})
	proxy.Close()
	if err != nil {
		fail("initial insert: %v", err)
		return
	}

	// Kill server — all pool connections die
	fmt.Println("  Killing server (all pool conns die)...")
	killServer()
	time.Sleep(1 * time.Second)

	// Restart server
	fmt.Println("  Restarting server...")
	startServer()
	time.Sleep(2 * time.Second)

	// Try multiple operations — pool should rebuild connections on demand
	success := 0
	for attempt := 1; attempt <= 10; attempt++ {
		proxy2, err := connectProxy()
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		_, err = proxy2.Insert(collection, map[string]any{"test": 6, "attempt": attempt})
		proxy2.Close()
		if err == nil {
			success++
		} else {
			time.Sleep(500 * time.Millisecond)
		}
	}

	if success >= 5 {
		fmt.Printf("  PASS: pool rebuilt — %d/10 ops succeeded\n", success)
		passed++
	} else {
		fail("pool failed to rebuild — only %d/10 ops succeeded", success)
	}
	fmt.Println()
}

// ─── Helpers ─────────────────────────────────────────────────────────

func startServer() {
	cmd := exec.Command(serverBin)
	cmd.Dir = projectDir
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("OXIDB_ADDR=127.0.0.1:%d", serverPort),
		"OXIDB_POOL_SIZE=20",
		"OXIDB_IDLE_TIMEOUT=0",
	)
	cmd.Start()
	time.Sleep(1 * time.Second)
}

func killServer() {
	pids := findPids("oxidb-server")
	for _, pid := range pids {
		exec.Command("kill", "-9", pid).Run()
	}
	time.Sleep(300 * time.Millisecond)
}

func startOxiPool() {
	cmd := exec.Command(projectDir + "/target/release/oxipool")
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("OXIPOOL_LISTEN=127.0.0.1:%d", proxyPort),
		fmt.Sprintf("OXIPOOL_BACKEND=127.0.0.1:%d", serverPort),
		"OXIPOOL_SIZE=5",
		"OXIPOOL_STATS_INTERVAL=0",
	)
	cmd.Start()
	time.Sleep(1 * time.Second)
}

func killOxiPool() {
	pids := findPids("oxipool")
	for _, pid := range pids {
		exec.Command("kill", "-9", pid).Run()
	}
	time.Sleep(300 * time.Millisecond)
}

func killAll() {
	killOxiPool()
	killServer()
}

func connectProxy() (*oxidb.Client, error) {
	return oxidb.Connect("127.0.0.1", proxyPort, 3*time.Second)
}

func connectDirect() (*oxidb.Client, error) {
	return oxidb.Connect("127.0.0.1", serverPort, 3*time.Second)
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

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
