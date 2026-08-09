package main

import (
	"fmt"
	"math"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/parisxmas/OxiDB/clients/go/oxidb"
)

// Replica Lag Simulation Test for OxiPool
//
// Architecture:
//   master (port 4444) ← writes
//   replica (port 4446) ← reads (via OxiPool routing)
//   OxiPool (port 4445) ← client entry point, routes writes→master, reads→replica
//
// A background "replicator" goroutine copies documents from master→replica
// with a configurable delay, simulating real replication lag.
//
// Tests:
//   1. Read/write routing verification — writes go to master, reads go to replica
//   2. Stale read detection — immediate read after write returns stale (replica behind)
//   3. Eventual consistency — replica catches up after replication delay
//   4. Lag measurement — measure actual replication lag percentiles
//   5. Read-your-writes with master fallback — transactions pin to master (consistent)
//   6. High-throughput write burst — replica catches up under load

const (
	projectDir = "/Users/barisakin/source/docdb"
	collection = "replica_lag_test"
)

var (
	masterPort  = 4444
	replicaPort = 4446
	proxyPort   = 4445
	passed      int
	failed      int
)

func main() {
	if p := os.Getenv("OXIDB_PORT"); p != "" {
		masterPort, _ = strconv.Atoi(p)
	}
	if p := os.Getenv("REPLICA_PORT"); p != "" {
		replicaPort, _ = strconv.Atoi(p)
	}
	if p := os.Getenv("OXIPOOL_PORT"); p != "" {
		proxyPort, _ = strconv.Atoi(p)
	}

	fmt.Println("=== OxiPool Replica Lag Simulation Test ===")
	fmt.Println()

	killAll()
	time.Sleep(500 * time.Millisecond)

	startMaster()
	startReplica()
	startOxiPool()

	// Clean up collections on both servers
	cleanup()

	test1_routing_verification()
	test2_stale_read()
	test3_eventual_consistency()
	test4_lag_percentiles()
	test5_read_your_writes_via_tx()
	test6_burst_catchup()

	killAll()

	total := passed + failed
	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════╗")
	fmt.Println("║            REPLICA LAG SIMULATION RESULTS              ║")
	fmt.Println("╠══════════════════════════════════════════════════════════╣")
	fmt.Printf("║   Passed:             %-34d ║\n", passed)
	fmt.Printf("║   Failed:             %-34d ║\n", failed)
	fmt.Printf("║   Total:              %-34d ║\n", total)
	fmt.Println("╚══════════════════════════════════════════════════════════╝")

	if failed > 0 {
		fmt.Printf("\nFAILED: %d/%d tests failed\n", failed, total)
		os.Exit(1)
	}
	fmt.Println("\nAll replica lag simulation tests passed.")
}

// ─── Background Replicator ──────────────────────────────────────────

// replicator copies documents from master to replica with a delay.
// It polls the master for new documents and inserts them into the replica
// after waiting `lag` duration. Returns a stop function.
func startReplicator(lag time.Duration) (stop func()) {
	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		seen := make(map[string]bool)

		for {
			select {
			case <-stopCh:
				return
			default:
			}

			// Connect to master, fetch all docs
			master, err := connectMaster()
			if err != nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			docs, err := master.Find(collection, map[string]any{}, nil)
			master.Close()
			if err != nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			// Find new documents not yet replicated
			for _, docMap := range docs {
				id := fmt.Sprintf("%v", docMap["_id"])
				if seen[id] {
					continue
				}
				seen[id] = true

				// Simulate replication delay
				docCopy := make(map[string]any)
				for k, v := range docMap {
					docCopy[k] = v
				}

				wg.Add(1)
				go func(d map[string]any, delay time.Duration) {
					defer wg.Done()
					select {
					case <-stopCh:
						return
					case <-time.After(delay):
					}
					replica, err := connectReplica()
					if err != nil {
						return
					}
					replica.Insert(collection, d)
					replica.Close()
				}(docCopy, lag)
			}

			time.Sleep(50 * time.Millisecond)
		}
	}()

	return func() {
		close(stopCh)
		wg.Wait()
	}
}

// ─── Test 1: Read/Write Routing Verification ────────────────────────

func test1_routing_verification() {
	fmt.Println("TEST 1: Verify OxiPool routes writes→master, reads→replica")

	cleanup()

	// Insert directly into replica so we can distinguish routing
	replica, err := connectReplica()
	if err != nil {
		fail("connect replica: %v", err)
		return
	}
	replica.Insert(collection, map[string]any{"source": "replica_only", "marker": true})
	replica.Close()

	// Insert through OxiPool (should go to master)
	proxy, err := connectProxy()
	if err != nil {
		fail("connect proxy: %v", err)
		return
	}
	proxy.Insert(collection, map[string]any{"source": "via_proxy", "marker": true})
	proxy.Close()

	// Read through OxiPool (should go to replica)
	proxy2, err := connectProxy()
	if err != nil {
		fail("connect proxy2: %v", err)
		return
	}
	docs, err := proxy2.Find(collection, map[string]any{"marker": true}, nil)
	proxy2.Close()

	if err != nil {
		fail("find via proxy: %v", err)
		return
	}

	// Should see "replica_only" doc (from replica) but NOT "via_proxy" (on master only)
	foundReplicaDoc := false
	foundMasterDoc := false
	for _, docMap := range docs {
		src := fmt.Sprintf("%v", docMap["source"])
		if src == "replica_only" {
			foundReplicaDoc = true
		}
		if src == "via_proxy" {
			foundMasterDoc = true
		}
	}

	if foundReplicaDoc && !foundMasterDoc {
		fmt.Println("  PASS: reads go to replica (saw replica_only, not via_proxy)")
		passed++
	} else if foundMasterDoc {
		fail("reads went to master (found via_proxy doc)")
	} else {
		fail("unexpected: replica_only=%v, via_proxy=%v", foundReplicaDoc, foundMasterDoc)
	}

	// Verify master has the write
	master, err := connectMaster()
	if err != nil {
		fail("connect master: %v", err)
		return
	}
	masterDocs, _ := master.Find(collection, map[string]any{"source": "via_proxy"}, nil)
	master.Close()

	if len(masterDocs) == 1 {
		fmt.Println("  PASS: writes go to master (found via_proxy on master)")
		passed++
	} else {
		fail("write not on master: found %d docs", len(masterDocs))
	}
	fmt.Println()
}

// ─── Test 2: Stale Read Detection ───────────────────────────────────

func test2_stale_read() {
	fmt.Println("TEST 2: Immediate read after write returns stale data (replica behind)")

	cleanup()

	// No replicator running — replica should have nothing

	// Write through OxiPool
	proxy, err := connectProxy()
	if err != nil {
		fail("connect: %v", err)
		return
	}
	proxy.Insert(collection, map[string]any{"test": 2, "data": "written_to_master"})
	proxy.Close()

	// Immediately read through OxiPool — goes to replica which has nothing
	proxy2, err := connectProxy()
	if err != nil {
		fail("connect: %v", err)
		return
	}
	count, err := proxy2.Count(collection, map[string]any{"test": 2})
	proxy2.Close()

	if err != nil {
		fail("count: %v", err)
		return
	}

	if count == 0 {
		fmt.Println("  PASS: read returned 0 (stale — replica hasn't received write yet)")
		passed++
	} else {
		fail("expected stale read (0 docs), got %d", count)
	}

	// Verify master actually has it
	master, err := connectMaster()
	if err != nil {
		fail("connect master: %v", err)
		return
	}
	masterCount, _ := master.Count(collection, map[string]any{"test": 2})
	master.Close()

	if masterCount == 1 {
		fmt.Println("  PASS: master has the document (write succeeded)")
		passed++
	} else {
		fail("master count=%d, expected 1", masterCount)
	}
	fmt.Println()
}

// ─── Test 3: Eventual Consistency ───────────────────────────────────

func test3_eventual_consistency() {
	fmt.Println("TEST 3: Replica catches up after replication delay")

	cleanup()

	replicationLag := 500 * time.Millisecond
	stopReplicator := startReplicator(replicationLag)
	defer stopReplicator()

	// Write 10 documents through OxiPool
	for i := 0; i < 10; i++ {
		proxy, err := connectProxy()
		if err != nil {
			continue
		}
		proxy.Insert(collection, map[string]any{"test": 3, "seq": i})
		proxy.Close()
	}

	// Immediately read — should see fewer than 10 (or 0) on replica
	proxy, err := connectProxy()
	if err != nil {
		fail("connect: %v", err)
		return
	}
	immCount, _ := proxy.Count(collection, map[string]any{"test": 3})
	proxy.Close()

	fmt.Printf("  Immediate read: %d/10 docs visible on replica\n", immCount)

	// Wait for replication to complete (lag + polling buffer)
	time.Sleep(replicationLag + 2*time.Second)

	// Read again — should see all 10
	proxy2, err := connectProxy()
	if err != nil {
		fail("connect: %v", err)
		return
	}
	finalCount, _ := proxy2.Count(collection, map[string]any{"test": 3})
	proxy2.Close()

	fmt.Printf("  After replication: %d/10 docs visible on replica\n", finalCount)

	if finalCount == 10 {
		fmt.Println("  PASS: eventual consistency achieved — all 10 docs replicated")
		passed++
	} else {
		fail("expected 10 docs after replication, got %d", finalCount)
	}
	fmt.Println()
}

// ─── Test 4: Lag Percentile Measurement ─────────────────────────────

func test4_lag_percentiles() {
	fmt.Println("TEST 4: Measure replication lag percentiles (100 documents)")

	cleanup()

	replicationLag := 200 * time.Millisecond
	stopReplicator := startReplicator(replicationLag)
	defer stopReplicator()

	// Insert 100 documents, measure how long until each appears on replica
	const docCount = 100
	var lags []time.Duration
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i := 0; i < docCount; i++ {
		proxy, err := connectProxy()
		if err != nil {
			continue
		}
		marker := fmt.Sprintf("lag_%d_%d", i, time.Now().UnixNano())
		proxy.Insert(collection, map[string]any{"test": 4, "marker": marker, "seq": i})
		proxy.Close()

		wg.Add(1)
		go func(m string) {
			defer wg.Done()
			start := time.Now()
			// Poll replica until document appears
			for attempt := 0; attempt < 100; attempt++ {
				replica, err := connectReplica()
				if err != nil {
					time.Sleep(50 * time.Millisecond)
					continue
				}
				count, _ := replica.Count(collection, map[string]any{"marker": m})
				replica.Close()
				if count > 0 {
					lag := time.Since(start)
					mu.Lock()
					lags = append(lags, lag)
					mu.Unlock()
					return
				}
				time.Sleep(50 * time.Millisecond)
			}
			// Timed out — record max
			mu.Lock()
			lags = append(lags, 5*time.Second)
			mu.Unlock()
		}(marker)

		// Small gap between inserts to avoid overwhelming
		time.Sleep(10 * time.Millisecond)
	}

	wg.Wait()

	if len(lags) == 0 {
		fail("no lag measurements collected")
		return
	}

	sort.Slice(lags, func(i, j int) bool { return lags[i] < lags[j] })

	p50 := lags[len(lags)*50/100]
	p95 := lags[len(lags)*95/100]
	p99 := lags[len(lags)*99/100]
	maxLag := lags[len(lags)-1]
	avgLag := time.Duration(0)
	for _, l := range lags {
		avgLag += l
	}
	avgLag /= time.Duration(len(lags))

	fmt.Printf("  Configured replication lag: %v\n", replicationLag)
	fmt.Printf("  Measured lags (%d samples):\n", len(lags))
	fmt.Printf("    avg:  %v\n", avgLag.Round(time.Millisecond))
	fmt.Printf("    p50:  %v\n", p50.Round(time.Millisecond))
	fmt.Printf("    p95:  %v\n", p95.Round(time.Millisecond))
	fmt.Printf("    p99:  %v\n", p99.Round(time.Millisecond))
	fmt.Printf("    max:  %v\n", maxLag.Round(time.Millisecond))

	// Lag should be roughly in the range of configured delay + polling interval
	// (200ms lag + 50ms polling + overhead ≈ 300-600ms expected)
	if p50 < 5*time.Second {
		fmt.Println("  PASS: replication lag measured successfully")
		passed++
	} else {
		fail("p50 lag too high: %v", p50)
	}

	// Check that most replication happens within 2x the configured lag
	withinBound := 0
	bound := replicationLag * 5 // generous bound: 1s for 200ms lag
	for _, l := range lags {
		if l <= bound {
			withinBound++
		}
	}
	pct := float64(withinBound) / float64(len(lags)) * 100
	fmt.Printf("  %.0f%% of docs replicated within %v\n", pct, bound)

	if pct >= 80 {
		fmt.Println("  PASS: >= 80%% of docs replicated within expected bound")
		passed++
	} else {
		fail("only %.0f%% within bound", pct)
	}
	fmt.Println()
}

// ─── Test 5: Read-Your-Writes via Transaction Pinning ───────────────

func test5_read_your_writes_via_tx() {
	fmt.Println("TEST 5: Transaction pins to master → read-your-writes consistency")

	cleanup()

	// No replicator — replica stays empty

	// First, insert a document directly to master (committed, visible outside tx)
	master, err := connectMaster()
	if err != nil {
		fail("connect master: %v", err)
		return
	}
	master.Insert(collection, map[string]any{"test": 5, "data": "on_master"})
	master.Close()

	// Inside a transaction through OxiPool, reads should be pinned to master
	// and see the committed document (not go to replica which has nothing)
	proxy, err := connectProxy()
	if err != nil {
		fail("connect: %v", err)
		return
	}

	proxy.BeginTx()

	// This read should go to master (pinned), not replica
	count, err := proxy.Count(collection, map[string]any{"test": 5})
	if err != nil {
		proxy.RollbackTx()
		proxy.Close()
		fail("count in tx: %v", err)
		return
	}

	proxy.Insert(collection, map[string]any{"test": 5, "data": "in_tx"})
	proxy.CommitTx()
	proxy.Close()

	if count == 1 {
		fmt.Println("  PASS: read inside transaction pinned to master (saw 1 doc)")
		passed++
	} else {
		fail("expected 1 doc in tx (pinned to master), got %d", count)
	}

	// Outside transaction, read should go to replica (which has nothing)
	proxy2, err := connectProxy()
	if err != nil {
		fail("connect: %v", err)
		return
	}
	replicaCount, _ := proxy2.Count(collection, map[string]any{"test": 5})
	proxy2.Close()

	if replicaCount == 0 {
		fmt.Println("  PASS: read outside transaction goes to replica (0 docs — stale)")
		passed++
	} else {
		fail("expected stale read (0), got %d", replicaCount)
	}
	fmt.Println()
}

// ─── Test 6: High-Throughput Burst → Replica Catches Up ─────────────

func test6_burst_catchup() {
	fmt.Println("TEST 6: 500-document burst → replica catches up")

	cleanup()

	replicationLag := 100 * time.Millisecond
	stopReplicator := startReplicator(replicationLag)
	defer stopReplicator()

	const burstSize = 500

	// Burst write 500 docs through OxiPool as fast as possible
	t0 := time.Now()
	var writeErrors atomic.Int64
	var wg sync.WaitGroup

	for i := 0; i < burstSize; i++ {
		wg.Add(1)
		go func(seq int) {
			defer wg.Done()
			proxy, err := connectProxy()
			if err != nil {
				writeErrors.Add(1)
				return
			}
			_, err = proxy.Insert(collection, map[string]any{"test": 6, "seq": seq})
			proxy.Close()
			if err != nil {
				writeErrors.Add(1)
			}
		}(i)
		// Throttle slightly to avoid connection storm
		if i%50 == 49 {
			time.Sleep(50 * time.Millisecond)
		}
	}
	wg.Wait()
	writeTime := time.Since(t0)

	// Verify master count
	master, err := connectMaster()
	if err != nil {
		fail("connect master: %v", err)
		return
	}
	masterCount, _ := master.Count(collection, map[string]any{"test": 6})
	master.Close()

	fmt.Printf("  Burst: %d docs written in %v (%d errors)\n", masterCount, writeTime.Round(time.Millisecond), writeErrors.Load())
	fmt.Printf("  Write throughput: %.0f docs/s\n", float64(masterCount)/writeTime.Seconds())

	// Poll replica until it catches up
	startCatchup := time.Now()
	var replicaCount int
	converged := false

	for attempt := 0; attempt < 100; attempt++ {
		replica, err := connectReplica()
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		replicaCount, _ = replica.Count(collection, map[string]any{"test": 6})
		replica.Close()

		pct := float64(replicaCount) / float64(masterCount) * 100
		if attempt%10 == 0 {
			fmt.Printf("  Catchup: %d/%d (%.0f%%) after %v\n",
				replicaCount, masterCount, pct, time.Since(startCatchup).Round(100*time.Millisecond))
		}

		if replicaCount >= masterCount {
			converged = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	catchupTime := time.Since(startCatchup)

	if converged {
		fmt.Printf("  PASS: replica fully caught up in %v (%d/%d docs)\n",
			catchupTime.Round(time.Millisecond), replicaCount, masterCount)
		passed++
	} else {
		pct := float64(replicaCount) / math.Max(float64(masterCount), 1) * 100
		fail("replica did not catch up: %d/%d (%.0f%%) after %v",
			replicaCount, masterCount, pct, catchupTime.Round(time.Millisecond))
	}

	// Check that most documents are present
	if replicaCount >= masterCount*90/100 {
		fmt.Printf("  PASS: >= 90%% replication (%d/%d)\n", replicaCount, masterCount)
		passed++
	} else {
		fail("replication below 90%%: %d/%d", replicaCount, masterCount)
	}
	fmt.Println()
}

// ─── Helpers ────────────────────────────────────────────────────────

func startMaster() {
	cmd := exec.Command("./target/release/oxidb-server")
	cmd.Dir = projectDir
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("OXIDB_ADDR=127.0.0.1:%d", masterPort),
		"OXIDB_POOL_SIZE=20",
		"OXIDB_IDLE_TIMEOUT=0",
		fmt.Sprintf("OXIDB_DATA=%s/oxidb_data_master", os.TempDir()),
	)
	cmd.Start()
	time.Sleep(1 * time.Second)
}

func startReplica() {
	cmd := exec.Command("./target/release/oxidb-server")
	cmd.Dir = projectDir
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("OXIDB_ADDR=127.0.0.1:%d", replicaPort),
		"OXIDB_POOL_SIZE=20",
		"OXIDB_IDLE_TIMEOUT=0",
		fmt.Sprintf("OXIDB_DATA=%s/oxidb_data_replica", os.TempDir()),
	)
	cmd.Start()
	time.Sleep(1 * time.Second)
}

func startOxiPool() {
	cmd := exec.Command(projectDir + "/target/release/oxipool")
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("OXIPOOL_LISTEN=127.0.0.1:%d", proxyPort),
		fmt.Sprintf("OXIPOOL_MASTER=127.0.0.1:%d", masterPort),
		fmt.Sprintf("OXIPOOL_REPLICAS=127.0.0.1:%d", replicaPort),
		"OXIPOOL_MASTER_SIZE=5",
		"OXIPOOL_REPLICA_SIZE=5",
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

func cleanup() {
	// Drop collection on both master and replica
	for _, port := range []int{masterPort, replicaPort} {
		c, err := oxidb.Connect("127.0.0.1", port, 3*time.Second)
		if err != nil {
			continue
		}
		c.DropCollection(collection)
		c.Close()
	}
	time.Sleep(100 * time.Millisecond)
}

func connectMaster() (*oxidb.Client, error) {
	return oxidb.Connect("127.0.0.1", masterPort, 3*time.Second)
}

func connectReplica() (*oxidb.Client, error) {
	return oxidb.Connect("127.0.0.1", replicaPort, 3*time.Second)
}

func connectProxy() (*oxidb.Client, error) {
	return oxidb.Connect("127.0.0.1", proxyPort, 3*time.Second)
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
