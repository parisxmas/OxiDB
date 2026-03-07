package main

import (
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/parisxmas/OxiDB/go/oxidb"
)

// Long-Running Query Saturation Test for OxiPool
//
// Simulates heavy queries that hold pool connections for extended periods,
// then verifies OxiPool behavior under saturation:
//
// Tests:
//   1. Seed & baseline — insert 50K docs, measure fast query latency
//   2. Pool saturation — launch slow queries to consume all pool slots,
//      verify new requests queue (not rejected) and complete once slots free
//   3. Fast vs slow fairness — concurrent mix of fast pings and slow scans,
//      measure whether fast queries are starved
//   4. Transaction holds under saturation — long-held transactions don't
//      permanently consume pool slots after commit
//   5. Aggregation pipeline storm — heavy $group/$sort pipelines on 50K docs
//      verify pool throughput and recovery
//   6. Recovery after saturation — after all slow queries finish, latency
//      returns to baseline

const (
	projectDir = "/Users/barisakin/source/docdb"
	collection = "saturation_test"
	heavyColl  = "saturation_heavy"
	docCount   = 50000
)

var (
	serverPort = 4444
	proxyPort  = 4445
	poolSize   = 5 // small pool to make saturation easy to trigger
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

	fmt.Println("=== OxiPool Long-Running Query Saturation Test ===")
	fmt.Println()

	killAll()
	time.Sleep(500 * time.Millisecond)

	startServer()
	startOxiPool()

	baselineLatency := test1_seed_and_baseline()
	test2_pool_saturation()
	test3_fast_vs_slow_fairness()
	test4_transaction_holds()
	test5_aggregation_storm()
	test6_recovery(baselineLatency)

	killAll()

	total := passed + failed
	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════╗")
	fmt.Println("║          QUERY SATURATION TEST RESULTS                 ║")
	fmt.Println("╠══════════════════════════════════════════════════════════╣")
	fmt.Printf("║   Passed:             %-34d ║\n", passed)
	fmt.Printf("║   Failed:             %-34d ║\n", failed)
	fmt.Printf("║   Total:              %-34d ║\n", total)
	fmt.Println("╚══════════════════════════════════════════════════════════╝")

	if failed > 0 {
		fmt.Printf("\nFAILED: %d/%d tests failed\n", failed, total)
		os.Exit(1)
	}
	fmt.Println("\nAll query saturation tests passed.")
}

// ─── Test 1: Seed Data & Measure Baseline ───────────────────────────

func test1_seed_and_baseline() time.Duration {
	fmt.Println("TEST 1: Seed 50K documents and measure baseline latency")

	// Seed data directly to server (bypass pool for speed)
	direct, err := connectDirect()
	if err != nil {
		fail("connect direct: %v", err)
		return 0
	}

	direct.DropCollection(collection)
	direct.DropCollection(heavyColl)

	// Bulk insert in batches of 500
	t0 := time.Now()
	for start := 0; start < docCount; start += 500 {
		batch := make([]map[string]any, 0, 500)
		end := start + 500
		if end > docCount {
			end = docCount
		}
		for i := start; i < end; i++ {
			batch = append(batch, map[string]any{
				"seq":      i,
				"category": fmt.Sprintf("cat_%d", i%100),
				"value":    i * 7 % 1000,
				"status":   []string{"active", "pending", "archived"}[i%3],
				"region":   []string{"us", "eu", "ap", "sa"}[i%4],
				"payload":  strings.Repeat("x", 200), // ~200B per doc
			})
		}
		_, err := direct.InsertMany(collection, batch)
		if err != nil {
			fail("insert batch at %d: %v", start, err)
			direct.Close()
			return 0
		}
	}
	seedTime := time.Since(t0)
	direct.Close()

	fmt.Printf("  Seeded %d docs in %v (%.0f docs/s)\n", docCount, seedTime.Round(time.Millisecond), float64(docCount)/seedTime.Seconds())

	// Measure baseline latency through OxiPool (10 pings)
	var latencies []time.Duration
	for i := 0; i < 20; i++ {
		proxy, err := connectProxy()
		if err != nil {
			continue
		}
		start := time.Now()
		proxy.Ping()
		latencies = append(latencies, time.Since(start))
		proxy.Close()
	}

	if len(latencies) == 0 {
		fail("no baseline latencies collected")
		return 0
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	baseline := latencies[len(latencies)/2] // median

	fmt.Printf("  Baseline ping latency (median): %v\n", baseline.Round(time.Microsecond))
	fmt.Println("  PASS: data seeded and baseline measured")
	passed++
	fmt.Println()
	return baseline
}

// ─── Test 2: Pool Saturation ────────────────────────────────────────

func test2_pool_saturation() {
	fmt.Println("TEST 2: Saturate pool with slow queries → queued requests complete")

	// Launch poolSize slow queries to consume all pool connections
	// Each does a full collection scan (find all 50K docs)
	var slowWg sync.WaitGroup
	slowStarted := make(chan struct{}, poolSize)

	for i := 0; i < poolSize; i++ {
		slowWg.Add(1)
		go func(id int) {
			defer slowWg.Done()
			proxy, err := connectProxy()
			if err != nil {
				return
			}
			defer proxy.Close()
			slowStarted <- struct{}{}
			// Full scan of 50K docs — holds the pool connection
			proxy.Find(collection, map[string]any{}, nil)
		}(i)
	}

	// Wait for all slow queries to start
	for i := 0; i < poolSize; i++ {
		<-slowStarted
	}
	// Small delay to ensure they're mid-query
	time.Sleep(100 * time.Millisecond)

	// Now try a fast query — it should queue and eventually complete
	// (once a slow query finishes and releases a pool slot)
	done := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		proxy, err := connectProxy()
		if err != nil {
			done <- -1
			return
		}
		proxy.Ping()
		proxy.Close()
		done <- time.Since(start)
	}()

	// Wait for slow queries to finish
	slowWg.Wait()

	select {
	case d := <-done:
		if d < 0 {
			fail("queued request failed to connect")
		} else {
			fmt.Printf("  Queued ping completed in %v (waited for pool slot)\n", d.Round(time.Millisecond))
			fmt.Println("  PASS: queued request completed after pool freed up")
			passed++
		}
	case <-time.After(30 * time.Second):
		fail("queued request hung for 30s — pool deadlocked?")
	}
	fmt.Println()
}

// ─── Test 3: Fast vs Slow Fairness ──────────────────────────────────

func test3_fast_vs_slow_fairness() {
	fmt.Println("TEST 3: Mixed fast/slow queries → fast queries not permanently starved")

	var fastLatencies []time.Duration
	var fastMu sync.Mutex
	var fastCount atomic.Int64
	var slowCount atomic.Int64
	var wg sync.WaitGroup

	// 5 slow workers: each does 3 full collection scans (50K docs each)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 3; j++ {
				proxy, err := connectProxy()
				if err != nil {
					continue
				}
				proxy.Find(collection, map[string]any{}, nil)
				proxy.Close()
				slowCount.Add(1)
			}
		}()
	}

	// 10 fast workers: each does 20 pings
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				proxy, err := connectProxy()
				if err != nil {
					continue
				}
				start := time.Now()
				proxy.Ping()
				lat := time.Since(start)
				proxy.Close()

				fastMu.Lock()
				fastLatencies = append(fastLatencies, lat)
				fastMu.Unlock()
				fastCount.Add(1)
			}
		}()
	}

	wg.Wait()

	if len(fastLatencies) == 0 {
		fail("no fast latencies collected")
		return
	}

	sort.Slice(fastLatencies, func(i, j int) bool { return fastLatencies[i] < fastLatencies[j] })
	p50 := fastLatencies[len(fastLatencies)*50/100]
	p95 := fastLatencies[len(fastLatencies)*95/100]
	p99 := fastLatencies[len(fastLatencies)*99/100]

	fmt.Printf("  Slow scans completed: %d\n", slowCount.Load())
	fmt.Printf("  Fast pings completed: %d\n", fastCount.Load())
	fmt.Printf("  Fast ping latencies: p50=%v  p95=%v  p99=%v\n",
		p50.Round(time.Millisecond), p95.Round(time.Millisecond), p99.Round(time.Millisecond))

	// All fast pings should complete (not starved)
	if fastCount.Load() == 200 {
		fmt.Println("  PASS: all 200 fast pings completed (no starvation)")
		passed++
	} else {
		fail("only %d/200 fast pings completed", fastCount.Load())
	}

	// p50 should be under 5s (even with pool contention)
	if p50 < 5*time.Second {
		fmt.Printf("  PASS: fast query p50 under 5s (%v)\n", p50.Round(time.Millisecond))
		passed++
	} else {
		fail("fast query p50 too high: %v", p50)
	}
	fmt.Println()
}

// ─── Test 4: Transaction Holds Under Saturation ─────────────────────

func test4_transaction_holds() {
	fmt.Println("TEST 4: Long-held transactions release pool slots after commit")

	// Open poolSize transactions that each hold a pinned connection
	txClients := make([]*oxidb.Client, 0, poolSize)
	for i := 0; i < poolSize; i++ {
		proxy, err := connectProxy()
		if err != nil {
			continue
		}
		_, err = proxy.BeginTx()
		if err != nil {
			proxy.Close()
			continue
		}
		// Do some work inside the transaction
		proxy.Insert(heavyColl, map[string]any{"tx_id": i, "phase": "holding"})
		txClients = append(txClients, proxy)
	}

	fmt.Printf("  Opened %d transactions (pinning %d pool connections)\n", len(txClients), len(txClients))

	if len(txClients) < poolSize {
		fail("could only open %d/%d transactions", len(txClients), poolSize)
		for _, c := range txClients {
			c.RollbackTx()
			c.Close()
		}
		return
	}

	// Pool is now fully saturated with pinned tx connections
	// Try a new operation — it should queue
	queuedDone := make(chan bool, 1)
	go func() {
		proxy, err := connectProxy()
		if err != nil {
			queuedDone <- false
			return
		}
		proxy.Ping()
		proxy.Close()
		queuedDone <- true
	}()

	// Wait briefly — the queued request should NOT complete yet
	select {
	case <-queuedDone:
		// It completed immediately — pool might have spare connections
		fmt.Println("  (queued request completed immediately — pool not fully pinned)")
	case <-time.After(500 * time.Millisecond):
		fmt.Println("  Confirmed: new request blocked (pool fully pinned by transactions)")
	}

	// Now commit all transactions — this should release pool connections
	for i, c := range txClients {
		err := c.CommitTx()
		if err != nil {
			fmt.Printf("  commit tx %d error: %v\n", i, err)
		}
		c.Close()
	}

	// The queued request should now complete
	select {
	case ok := <-queuedDone:
		if ok {
			fmt.Println("  PASS: queued request completed after transactions committed")
			passed++
		} else {
			fail("queued request failed")
		}
	case <-time.After(10 * time.Second):
		fail("queued request still blocked 10s after transactions committed")
	}

	// Verify pool is fully usable — do poolSize*2 operations
	success := 0
	for i := 0; i < poolSize*2; i++ {
		proxy, err := connectProxy()
		if err != nil {
			continue
		}
		_, err = proxy.Insert(heavyColl, map[string]any{"post_tx": true, "i": i})
		proxy.Close()
		if err == nil {
			success++
		}
	}

	if success >= poolSize*2-1 {
		fmt.Printf("  PASS: pool fully recovered — %d/%d post-tx ops succeeded\n", success, poolSize*2)
		passed++
	} else {
		fail("pool not recovered: %d/%d ops succeeded", success, poolSize*2)
	}
	fmt.Println()
}

// ─── Test 5: Aggregation Pipeline Storm ─────────────────────────────

func test5_aggregation_storm() {
	fmt.Println("TEST 5: Heavy aggregation pipelines on 50K docs → pool throughput")

	var wg sync.WaitGroup
	var completed atomic.Int64
	var errors atomic.Int64
	var latencies []time.Duration
	var mu sync.Mutex

	// 20 concurrent workers, each runs 5 heavy aggregations
	workers := 20
	opsPerWorker := 5

	t0 := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				proxy, err := connectProxy()
				if err != nil {
					errors.Add(1)
					continue
				}

				start := time.Now()

				// Heavy pipeline: match → group by category → sort by count
				var opErr error
				switch i % 3 {
				case 0:
					// Group all 50K docs by category (100 groups)
					_, opErr = proxy.Aggregate(collection, []map[string]any{
						{"$group": map[string]any{
							"_id":       "$category",
							"count":     map[string]any{"$sum": 1},
							"avg_value": map[string]any{"$avg": "$value"},
						}},
						{"$sort": map[string]any{"count": -1}},
					})
				case 1:
					// Group by status + region (12 groups) with filter
					_, opErr = proxy.Aggregate(collection, []map[string]any{
						{"$match": map[string]any{"value": map[string]any{"$gt": 500}}},
						{"$group": map[string]any{
							"_id":       map[string]any{"status": "$status", "region": "$region"},
							"total":     map[string]any{"$sum": "$value"},
							"doc_count": map[string]any{"$sum": 1},
						}},
						{"$sort": map[string]any{"total": -1}},
						{"$limit": 10},
					})
				case 2:
					// Full collection scan with sort (no index)
					_, opErr = proxy.Find(collection, map[string]any{
						"value": map[string]any{"$gt": 900},
					}, &oxidb.FindOptions{
						Sort: map[string]any{"seq": -1},
					})
				}

				lat := time.Since(start)
				proxy.Close()

				if opErr != nil {
					errors.Add(1)
				} else {
					completed.Add(1)
					mu.Lock()
					latencies = append(latencies, lat)
					mu.Unlock()
				}
			}
		}(w)
	}

	wg.Wait()
	totalTime := time.Since(t0)

	if len(latencies) == 0 {
		fail("no aggregation latencies collected")
		return
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p50 := latencies[len(latencies)*50/100]
	p95 := latencies[len(latencies)*95/100]
	p99 := latencies[len(latencies)*99/100]

	totalOps := workers * opsPerWorker
	throughput := float64(completed.Load()) / totalTime.Seconds()

	fmt.Printf("  Workers: %d, ops/worker: %d, total: %d\n", workers, opsPerWorker, totalOps)
	fmt.Printf("  Completed: %d, errors: %d\n", completed.Load(), errors.Load())
	fmt.Printf("  Total time: %v\n", totalTime.Round(time.Millisecond))
	fmt.Printf("  Throughput: %.1f ops/s\n", throughput)
	fmt.Printf("  Latencies: p50=%v  p95=%v  p99=%v\n",
		p50.Round(time.Millisecond), p95.Round(time.Millisecond), p99.Round(time.Millisecond))

	// At least 90% of operations should succeed
	successRate := float64(completed.Load()) / float64(totalOps) * 100
	if successRate >= 90 {
		fmt.Printf("  PASS: %.0f%% success rate under heavy aggregation load\n", successRate)
		passed++
	} else {
		fail("only %.0f%% success rate", successRate)
	}

	// Pool should handle at least 5 ops/s even under heavy load
	if throughput >= 5 {
		fmt.Printf("  PASS: throughput %.1f ops/s (>= 5 ops/s)\n", throughput)
		passed++
	} else {
		fail("throughput too low: %.1f ops/s", throughput)
	}
	fmt.Println()
}

// ─── Test 6: Recovery After Saturation ──────────────────────────────

func test6_recovery(baseline time.Duration) {
	fmt.Println("TEST 6: Latency recovers to baseline after saturation ends")

	// Wait a moment for pool to settle
	time.Sleep(1 * time.Second)

	// Measure post-saturation latency
	var latencies []time.Duration
	for i := 0; i < 30; i++ {
		proxy, err := connectProxy()
		if err != nil {
			continue
		}
		start := time.Now()
		proxy.Ping()
		latencies = append(latencies, time.Since(start))
		proxy.Close()
	}

	if len(latencies) == 0 {
		fail("no recovery latencies collected")
		return
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	postMedian := latencies[len(latencies)/2]

	fmt.Printf("  Baseline ping median: %v\n", baseline.Round(time.Microsecond))
	fmt.Printf("  Post-saturation ping median: %v\n", postMedian.Round(time.Microsecond))

	// Recovery: post-saturation latency should be within 10x of baseline
	// (generous bound — we just want to confirm no permanent degradation)
	if baseline == 0 {
		if postMedian < 50*time.Millisecond {
			fmt.Println("  PASS: post-saturation latency reasonable (< 50ms)")
			passed++
		} else {
			fail("post-saturation latency too high: %v", postMedian)
		}
	} else {
		ratio := float64(postMedian) / float64(baseline)
		if ratio <= 10 {
			fmt.Printf("  PASS: latency recovered (%.1fx baseline)\n", ratio)
			passed++
		} else {
			fail("latency still degraded: %.1fx baseline (%v vs %v)", ratio, postMedian, baseline)
		}
	}

	// Verify all pool connections are usable (do poolSize concurrent ops)
	var wg sync.WaitGroup
	var success atomic.Int64
	for i := 0; i < poolSize; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			proxy, err := connectProxy()
			if err != nil {
				return
			}
			_, err = proxy.Insert(heavyColl, map[string]any{"recovery": true})
			proxy.Close()
			if err == nil {
				success.Add(1)
			}
		}()
	}
	wg.Wait()

	if success.Load() == int64(poolSize) {
		fmt.Printf("  PASS: all %d pool slots functional\n", poolSize)
		passed++
	} else {
		fail("only %d/%d pool slots functional", success.Load(), poolSize)
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
	return oxidb.Connect("127.0.0.1", proxyPort, 5*time.Second)
}

func connectDirect() (*oxidb.Client, error) {
	return oxidb.Connect("127.0.0.1", serverPort, 5*time.Second)
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
