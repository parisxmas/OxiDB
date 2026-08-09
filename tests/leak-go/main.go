package main

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/parisxmas/OxiDB/go/oxidb"
)

// Connection Leak Test for OxiPool
//
// Tests:
//   1. Rapid connect/disconnect (1000 cycles) → FD count stays stable
//   2. Concurrent connect/disconnect storm → no leaked connections
//   3. Abandon connection without Close() → OxiPool cleans up
//   4. Transaction abandon (begin_tx, never commit/rollback, disconnect) → pinned conn returns to pool
//   5. Mixed operations with connect/disconnect → pool size stays at configured value
//   6. Server FD count stays stable through all tests

const (
	projectDir = "/Users/barisakin/source/docdb"
	collection = "leak_test"
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

	fmt.Println("=== OxiPool Connection Leak Test ===")
	fmt.Println()

	killAll()
	time.Sleep(500 * time.Millisecond)

	startServer()
	startOxiPool()

	// Baseline FD counts
	time.Sleep(1 * time.Second)
	baseServerFDs := countFDs("oxidb-server")
	basePoolFDs := countFDs("oxipool")
	fmt.Printf("Baseline FDs — server: %d, oxipool: %d\n\n", baseServerFDs, basePoolFDs)

	test1_rapid_connect_disconnect(basePoolFDs)
	test2_concurrent_storm(basePoolFDs)
	test3_abandon_without_close(basePoolFDs)
	test4_transaction_abandon(basePoolFDs)
	test5_mixed_ops_pool_stability()
	test6_server_fd_stability(baseServerFDs)

	killAll()

	total := passed + failed
	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════╗")
	fmt.Println("║              CONNECTION LEAK TEST RESULTS               ║")
	fmt.Println("╠══════════════════════════════════════════════════════════╣")
	fmt.Printf("║   Passed:             %-34d ║\n", passed)
	fmt.Printf("║   Failed:             %-34d ║\n", failed)
	fmt.Printf("║   Total:              %-34d ║\n", total)
	fmt.Println("╚══════════════════════════════════════════════════════════╝")

	if failed > 0 {
		fmt.Printf("\nFAILED: %d/%d tests failed\n", failed, total)
		os.Exit(1)
	}
	fmt.Println("\nAll connection leak tests passed.")
}

// ─── Test 1: Rapid connect/disconnect ───────────────────────────────

func test1_rapid_connect_disconnect(baseFDs int) {
	fmt.Println("TEST 1: 1000 rapid connect/disconnect cycles → FD count stable")

	for i := 0; i < 1000; i++ {
		c, err := connectProxy()
		if err != nil {
			if i < 5 {
				fmt.Printf("  connect %d failed: %v\n", i, err)
			}
			continue
		}
		c.Ping()
		c.Close()
	}

	time.Sleep(1 * time.Second)
	afterFDs := countFDs("oxipool")
	drift := afterFDs - baseFDs

	fmt.Printf("  FDs: baseline=%d, after=%d, drift=%d\n", baseFDs, afterFDs, drift)
	if drift <= 5 {
		fmt.Println("  PASS: FD count stable (drift <= 5)")
		passed++
	} else {
		fail("FD leak detected: drift=%d", drift)
	}
	fmt.Println()
}

// ─── Test 2: Concurrent connect/disconnect storm ────────────────────

func test2_concurrent_storm(baseFDs int) {
	fmt.Println("TEST 2: 100 concurrent clients × 50 connect/disconnect cycles")

	var wg sync.WaitGroup
	var totalConns atomic.Int64
	var totalErrors atomic.Int64

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				c, err := connectProxy()
				if err != nil {
					totalErrors.Add(1)
					continue
				}
				c.Ping()
				c.Close()
				totalConns.Add(1)
			}
		}()
	}
	wg.Wait()

	time.Sleep(2 * time.Second)
	afterFDs := countFDs("oxipool")
	drift := afterFDs - baseFDs

	fmt.Printf("  Connections: %d successful, %d errors\n", totalConns.Load(), totalErrors.Load())
	fmt.Printf("  FDs: baseline=%d, after=%d, drift=%d\n", baseFDs, afterFDs, drift)
	if drift <= 5 {
		fmt.Println("  PASS: no FD leak after 5000 concurrent connect/disconnect cycles")
		passed++
	} else {
		fail("FD leak: drift=%d after storm", drift)
	}
	fmt.Println()
}

// ─── Test 3: Abandon connection without Close() ─────────────────────

func test3_abandon_without_close(baseFDs int) {
	fmt.Println("TEST 3: 200 abandoned connections (no Close()) → OxiPool cleans up")

	for i := 0; i < 200; i++ {
		c, err := connectProxy()
		if err != nil {
			continue
		}
		c.Ping()
		// Intentionally NOT calling c.Close()
		// Go's GC will eventually close the socket, but we're testing
		// that OxiPool handles the client-side TCP close properly
		_ = c
	}

	// Force GC to finalize abandoned sockets
	for i := 0; i < 5; i++ {
		runtime.GC()
		time.Sleep(1 * time.Second)
	}

	afterFDs := countFDs("oxipool")
	drift := afterFDs - baseFDs

	fmt.Printf("  FDs: baseline=%d, after=%d, drift=%d\n", baseFDs, afterFDs, drift)
	// Larger tolerance here since GC timing is non-deterministic
	if drift <= 20 {
		fmt.Println("  PASS: OxiPool cleaned up abandoned connections")
		passed++
	} else {
		fail("possible leak: drift=%d after abandoned connections", drift)
	}
	fmt.Println()
}

// ─── Test 4: Transaction abandon ────────────────────────────────────

func test4_transaction_abandon(baseFDs int) {
	fmt.Println("TEST 4: 50 abandoned transactions → pinned conns return to pool")

	for i := 0; i < 50; i++ {
		c, err := connectProxy()
		if err != nil {
			continue
		}
		// Begin transaction but never commit/rollback
		c.BeginTx()
		c.Insert(collection, map[string]any{"test": "leak_tx", "i": i})
		// Close without commit — OxiPool should rollback and return backend to pool
		c.Close()
	}

	time.Sleep(2 * time.Second)

	// Verify: pool backend connections should all be available
	// Test by doing 5 sequential operations (if pool leaked, we'd run out)
	success := 0
	for i := 0; i < 10; i++ {
		c, err := connectProxy()
		if err != nil {
			continue
		}
		_, err = c.Insert(collection, map[string]any{"test": "after_leak_tx", "i": i})
		c.Close()
		if err == nil {
			success++
		}
	}

	afterFDs := countFDs("oxipool")
	drift := afterFDs - baseFDs

	fmt.Printf("  Post-abandon ops: %d/10 succeeded\n", success)
	fmt.Printf("  FDs: baseline=%d, after=%d, drift=%d\n", baseFDs, afterFDs, drift)

	// Verify no leaked transaction data
	direct, err := connectDirect()
	txCount := 0
	if err == nil {
		txCount, _ = direct.Count(collection, map[string]any{"test": "leak_tx"})
		direct.Close()
	}

	if success >= 8 && txCount == 0 {
		fmt.Printf("  PASS: pool stable, 0 leaked tx docs (all rolled back)\n")
		passed++
	} else if success < 8 {
		fail("pool exhausted: only %d/10 ops succeeded", success)
	} else {
		fail("found %d leaked tx docs, expected 0", txCount)
	}
	fmt.Println()
}

// ─── Test 5: Mixed ops with connect/disconnect ──────────────────────

func test5_mixed_ops_pool_stability() {
	fmt.Println("TEST 5: Mixed operations with rapid connect/disconnect → pool stable")

	// Get pool size by checking OxiPool backend connections to server
	poolConnsStart := countServerConns()

	var wg sync.WaitGroup
	var ops atomic.Int64

	for w := 0; w < 20; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				c, err := connectProxy()
				if err != nil {
					continue
				}

				switch i % 5 {
				case 0:
					c.Insert(collection, map[string]any{"worker": id, "op": i})
				case 1:
					c.FindOne(collection, map[string]any{"worker": id})
				case 2:
					c.Count(collection, map[string]any{})
				case 3:
					c.UpdateOne(collection,
						map[string]any{"worker": id},
						map[string]any{"$set": map[string]any{"updated": true}})
				case 4:
					c.SQL("SELECT * FROM leak_test LIMIT 1")
				}
				c.Close()
				ops.Add(1)
			}
		}(w)
	}
	wg.Wait()

	time.Sleep(2 * time.Second)
	poolConnsEnd := countServerConns()

	fmt.Printf("  Ops completed: %d\n", ops.Load())
	fmt.Printf("  Server connections: start=%d, end=%d\n", poolConnsStart, poolConnsEnd)

	// Pool connections to server should stay roughly the same
	connDrift := abs(poolConnsEnd - poolConnsStart)
	if connDrift <= 3 {
		fmt.Printf("  PASS: server connection count stable (drift=%d)\n", connDrift)
		passed++
	} else {
		fail("server connection drift=%d (leaked backend connections?)", connDrift)
	}
	fmt.Println()
}

// ─── Test 6: Server FD stability ────────────────────────────────────

func test6_server_fd_stability(baseServerFDs int) {
	fmt.Println("TEST 6: Server FD count stable after all tests")

	afterFDs := countFDs("oxidb-server")
	drift := afterFDs - baseServerFDs

	fmt.Printf("  Server FDs: baseline=%d, after=%d, drift=%d\n", baseServerFDs, afterFDs, drift)
	if drift <= 10 {
		fmt.Println("  PASS: server FD count stable")
		passed++
	} else {
		fail("server FD leak: drift=%d", drift)
	}
	fmt.Println()
}

// ─── Helpers ─────────────────────────────────────────────────────────

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
		"OXIPOOL_SIZE=5",
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

func countFDs(processName string) int {
	pids := findPids(processName)
	if len(pids) == 0 {
		return 0
	}
	// Count open file descriptors via lsof
	out, err := exec.Command("lsof", "-p", pids[0]).Output()
	if err != nil {
		return 0
	}
	lines := strings.Split(string(out), "\n")
	return len(lines) - 1 // subtract header
}

func countServerConns() int {
	// Count TCP connections to the server port
	out, _ := exec.Command("lsof", "-i", fmt.Sprintf(":%d", serverPort), "-sTCP:ESTABLISHED").Output()
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) == 1 && lines[0] == "" {
		return 0
	}
	return len(lines) - 1 // subtract header
}

func fail(format string, args ...any) {
	fmt.Printf("  FAIL: "+format+"\n", args...)
	failed++
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}
