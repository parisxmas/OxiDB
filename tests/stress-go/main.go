package main

import (
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/parisxmas/OxiDB/go/oxidb"
)

// --- Config ---

const (
	numWorkers   = 100
	opsPerWorker = 100
	poolSize     = 10
	collection   = "stress_test"
)

// --- Counters ---

var (
	totalOps      atomic.Int64
	insertOps     atomic.Int64
	findOps       atomic.Int64
	findOneOps    atomic.Int64
	updateOps     atomic.Int64
	deleteOps     atomic.Int64
	countOps      atomic.Int64
	sqlOps        atomic.Int64
	aggregateOps  atomic.Int64
	txOps         atomic.Int64
	errors        atomic.Int64
)

// --- Latency tracker ---

type latencyTracker struct {
	mu      sync.Mutex
	samples []time.Duration
}

func (lt *latencyTracker) record(d time.Duration) {
	lt.mu.Lock()
	lt.samples = append(lt.samples, d)
	lt.mu.Unlock()
}

func (lt *latencyTracker) stats() (min, max, avg, p50, p95, p99 time.Duration) {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	if len(lt.samples) == 0 {
		return
	}
	sorted := make([]time.Duration, len(lt.samples))
	copy(sorted, lt.samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	min = sorted[0]
	max = sorted[len(sorted)-1]
	var total time.Duration
	for _, d := range sorted {
		total += d
	}
	avg = total / time.Duration(len(sorted))
	p50 = sorted[len(sorted)*50/100]
	p95 = sorted[len(sorted)*95/100]
	p99 = sorted[len(sorted)*99/100]
	return
}

var latencies = &latencyTracker{}

// --- Memory snapshot ---

type memSnapshot struct {
	alloc      uint64
	totalAlloc uint64
	sys        uint64
	numGC      uint32
}

func memStats() memSnapshot {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return memSnapshot{
		alloc:      m.Alloc,
		totalAlloc: m.TotalAlloc,
		sys:        m.Sys,
		numGC:      m.NumGC,
	}
}

func fmtBytes(b uint64) string {
	if b < 1024 {
		return fmt.Sprintf("%d B", b)
	}
	if b < 1024*1024 {
		return fmt.Sprintf("%.1f KB", float64(b)/1024)
	}
	return fmt.Sprintf("%.1f MB", float64(b)/(1024*1024))
}

// --- Worker (pool-based) ---

func worker(id int, pool *oxidb.Pool, wg *sync.WaitGroup) {
	defer wg.Done()
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))

	for op := 0; op < opsPerWorker; op++ {
		err := pool.WithConn(func(c *oxidb.Client) error {
			start := time.Now()
			opErr := doRandomOp(c, id, rng)
			latencies.record(time.Since(start))
			totalOps.Add(1)
			return opErr
		})
		if err != nil {
			errors.Add(1)
			if errors.Load() <= 20 {
				fmt.Printf("[worker %3d] op %d error: %v\n", id, op, err)
			}
		}
	}
}

func doRandomOp(c *oxidb.Client, workerID int, rng *rand.Rand) error {
	r := rng.Intn(100)
	switch {
	case r < 25:
		return doInsert(c, workerID, rng)
	case r < 45:
		return doFind(c, rng)
	case r < 60:
		return doFindOne(c, rng)
	case r < 72:
		return doUpdate(c, workerID, rng)
	case r < 80:
		return doDelete(c, rng)
	case r < 88:
		return doCount(c)
	case r < 94:
		return doSQL(c, rng)
	case r < 98:
		return doAggregate(c)
	default:
		return doTransaction(c, workerID, rng)
	}
}

func doInsert(c *oxidb.Client, workerID int, rng *rand.Rand) error {
	insertOps.Add(1)
	categories := []string{"electronics", "books", "clothing", "food", "sports", "music", "toys", "tools"}
	statuses := []string{"active", "inactive", "pending", "archived"}
	doc := map[string]any{
		"worker":    workerID,
		"name":      fmt.Sprintf("item-%d-%d", workerID, rng.Intn(100000)),
		"price":     rng.Float64() * 1000,
		"quantity":  rng.Intn(500),
		"category":  categories[rng.Intn(len(categories))],
		"status":    statuses[rng.Intn(len(statuses))],
		"score":     rng.Float64() * 100,
		"tags":      []string{fmt.Sprintf("tag%d", rng.Intn(20)), fmt.Sprintf("tag%d", rng.Intn(20))},
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}
	_, err := c.Insert(collection, doc)
	return err
}

func doFind(c *oxidb.Client, rng *rand.Rand) error {
	findOps.Add(1)
	queries := []map[string]any{
		{"category": "electronics"},
		{"status": "active"},
		{"quantity": map[string]any{"$gte": rng.Intn(100)}},
		{"price": map[string]any{"$lt": rng.Float64() * 500}},
		{"worker": rng.Intn(numWorkers)},
		{"category": map[string]any{"$in": []string{"books", "music", "toys"}}},
		{"status": map[string]any{"$nin": []string{"archived"}}},
	}
	q := queries[rng.Intn(len(queries))]
	limit := 10
	_, err := c.Find(collection, q, &oxidb.FindOptions{Limit: &limit})
	return err
}

func doFindOne(c *oxidb.Client, rng *rand.Rand) error {
	findOneOps.Add(1)
	queries := []map[string]any{
		{"worker": rng.Intn(numWorkers)},
		{"category": "food"},
		{"status": "pending"},
	}
	q := queries[rng.Intn(len(queries))]
	_, err := c.FindOne(collection, q)
	return err
}

func doUpdate(c *oxidb.Client, workerID int, rng *rand.Rand) error {
	updateOps.Add(1)
	query := map[string]any{"worker": workerID, "status": "active"}
	update := map[string]any{
		"$set": map[string]any{
			"score":      rng.Float64() * 100,
			"updated_at": time.Now().UTC().Format(time.RFC3339),
		},
		"$inc": map[string]any{"quantity": rng.Intn(10) - 5},
	}
	_, err := c.UpdateOne(collection, query, update)
	return err
}

func doDelete(c *oxidb.Client, rng *rand.Rand) error {
	deleteOps.Add(1)
	query := map[string]any{"worker": rng.Intn(numWorkers), "status": "archived"}
	_, err := c.DeleteOne(collection, query)
	return err
}

func doCount(c *oxidb.Client) error {
	countOps.Add(1)
	_, err := c.Count(collection, map[string]any{})
	return err
}

func doSQL(c *oxidb.Client, rng *rand.Rand) error {
	sqlOps.Add(1)
	queries := []string{
		"SELECT * FROM stress_test WHERE category = 'electronics' LIMIT 5",
		"SELECT * FROM stress_test WHERE price > 500 LIMIT 10",
		"SELECT * FROM stress_test WHERE status = 'active' AND quantity > 100 LIMIT 5",
		fmt.Sprintf("SELECT * FROM stress_test WHERE worker = %d LIMIT 5", rng.Intn(numWorkers)),
		"SELECT * FROM stress_test WHERE category NOT IN ('archived') LIMIT 5",
	}
	_, err := c.SQL(queries[rng.Intn(len(queries))])
	return err
}

func doAggregate(c *oxidb.Client) error {
	aggregateOps.Add(1)
	pipeline := []map[string]any{
		{"$match": map[string]any{"status": "active"}},
		{"$group": map[string]any{
			"_id":       "$category",
			"total":     map[string]any{"$sum": "$price"},
			"avg_score": map[string]any{"$avg": "$score"},
			"count":     map[string]any{"$sum": 1},
		}},
		{"$sort": map[string]any{"total": -1}},
		{"$limit": 5},
	}
	_, err := c.Aggregate(collection, pipeline)
	return err
}

func doTransaction(c *oxidb.Client, workerID int, rng *rand.Rand) error {
	txOps.Add(1)
	return c.WithTransaction(func() error {
		_, err := c.Insert(collection, map[string]any{
			"worker":   workerID,
			"name":     fmt.Sprintf("tx-item-%d", rng.Intn(100000)),
			"price":    rng.Float64() * 100,
			"quantity": rng.Intn(50),
			"category": "tx_test",
			"status":   "active",
		})
		if err != nil {
			return err
		}
		_, err = c.UpdateOne(collection,
			map[string]any{"worker": workerID, "category": "tx_test"},
			map[string]any{"$inc": map[string]any{"quantity": 1}},
		)
		return err
	})
}

// --- Main ---

func main() {
	fmt.Println("=== OxiDB Stress Test (Connection Pool) ===")
	fmt.Printf("Workers: %d | Ops/worker: %d | Pool size: %d\n", numWorkers, opsPerWorker, poolSize)
	fmt.Printf("Total planned ops: %d\n\n", numWorkers*opsPerWorker)

	host := "127.0.0.1"
	port := 4444
	if h := os.Getenv("OXIDB_HOST"); h != "" {
		host = h
	}
	if p := os.Getenv("OXIDB_PORT"); p != "" {
		port, _ = strconv.Atoi(p)
	}

	// Setup: create collection and indexes
	setup, err := oxidb.Connect(host, port, 5*time.Second)
	if err != nil {
		fmt.Printf("FATAL: cannot connect for setup: %v\n", err)
		os.Exit(1)
	}
	_ = setup.DropCollection(collection)
	if err := setup.CreateCollection(collection); err != nil {
		fmt.Printf("FATAL: create collection: %v\n", err)
		os.Exit(1)
	}
	_ = setup.CreateIndex(collection, "worker")
	_ = setup.CreateIndex(collection, "category")
	_ = setup.CreateIndex(collection, "status")
	_ = setup.CreateIndex(collection, "price")
	setup.Close()
	fmt.Println("Setup complete: collection + indexes created")

	// Create connection pool
	pool, err := oxidb.NewPool(host, port, poolSize, 5*time.Second)
	if err != nil {
		fmt.Printf("FATAL: create pool: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()
	fmt.Printf("Connection pool created: %d connections\n", poolSize)

	// Snapshot memory before
	memBefore := memStats()

	// Run
	start := time.Now()
	var wg sync.WaitGroup

	// Progress ticker
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				fmt.Printf("  ... %d/%d ops completed (%d errors, pool: %d/%d idle)\n",
					totalOps.Load(), int64(numWorkers)*int64(opsPerWorker),
					errors.Load(), pool.Available(), poolSize)
			case <-done:
				return
			}
		}
	}()

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(i, pool, &wg)
	}
	wg.Wait()
	close(done)
	elapsed := time.Since(start)

	// Snapshot memory after
	memAfter := memStats()

	// Final stats
	total := totalOps.Load()
	errCount := errors.Load()
	opsPerSec := float64(total) / elapsed.Seconds()

	lMin, lMax, lAvg, lP50, lP95, lP99 := latencies.stats()

	// Final doc count
	var docCount int
	pool.WithConn(func(c *oxidb.Client) error {
		docCount, _ = c.Count(collection, map[string]any{})
		return nil
	})

	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════╗")
	fmt.Println("║              STRESS TEST REPORT (Pool Mode)             ║")
	fmt.Println("╠══════════════════════════════════════════════════════════╣")
	fmt.Println("║ CONFIGURATION                                          ║")
	fmt.Printf("║   Workers:            %-34d ║\n", numWorkers)
	fmt.Printf("║   Ops per worker:     %-34d ║\n", opsPerWorker)
	fmt.Printf("║   Pool size:          %-34d ║\n", poolSize)
	fmt.Printf("║   Total planned:      %-34d ║\n", numWorkers*opsPerWorker)
	fmt.Println("╠══════════════════════════════════════════════════════════╣")
	fmt.Println("║ RESULTS                                                ║")
	fmt.Printf("║   Total ops:          %-34d ║\n", total)
	fmt.Printf("║   Total time:         %-34s ║\n", elapsed.Round(time.Millisecond))
	fmt.Printf("║   Throughput:         %-30.0f ops/s ║\n", opsPerSec)
	fmt.Printf("║   Errors:             %-34d ║\n", errCount)
	fmt.Printf("║   Error rate:         %-33.2f%% ║\n", float64(errCount)/float64(total)*100)
	fmt.Printf("║   Final doc count:    %-34d ║\n", docCount)
	fmt.Println("╠══════════════════════════════════════════════════════════╣")
	fmt.Println("║ OPERATION BREAKDOWN                                     ║")
	fmt.Printf("║   Insert:             %-34d ║\n", insertOps.Load())
	fmt.Printf("║   Find:               %-34d ║\n", findOps.Load())
	fmt.Printf("║   FindOne:            %-34d ║\n", findOneOps.Load())
	fmt.Printf("║   Update:             %-34d ║\n", updateOps.Load())
	fmt.Printf("║   Delete:             %-34d ║\n", deleteOps.Load())
	fmt.Printf("║   Count:              %-34d ║\n", countOps.Load())
	fmt.Printf("║   SQL:                %-34d ║\n", sqlOps.Load())
	fmt.Printf("║   Aggregate:          %-34d ║\n", aggregateOps.Load())
	fmt.Printf("║   Transaction:        %-34d ║\n", txOps.Load())
	fmt.Println("╠══════════════════════════════════════════════════════════╣")
	fmt.Println("║ LATENCY (per operation)                                 ║")
	fmt.Printf("║   Min:                %-34s ║\n", lMin.Round(time.Microsecond))
	fmt.Printf("║   Max:                %-34s ║\n", lMax.Round(time.Microsecond))
	fmt.Printf("║   Avg:                %-34s ║\n", lAvg.Round(time.Microsecond))
	fmt.Printf("║   P50:                %-34s ║\n", lP50.Round(time.Microsecond))
	fmt.Printf("║   P95:                %-34s ║\n", lP95.Round(time.Microsecond))
	fmt.Printf("║   P99:                %-34s ║\n", lP99.Round(time.Microsecond))
	fmt.Println("╠══════════════════════════════════════════════════════════╣")
	fmt.Println("║ MEMORY (Go client process)                              ║")
	fmt.Printf("║   Heap alloc before:  %-34s ║\n", fmtBytes(memBefore.alloc))
	fmt.Printf("║   Heap alloc after:   %-34s ║\n", fmtBytes(memAfter.alloc))
	fmt.Printf("║   Total allocated:    %-34s ║\n", fmtBytes(memAfter.totalAlloc))
	fmt.Printf("║   System memory:      %-34s ║\n", fmtBytes(memAfter.sys))
	fmt.Printf("║   GC cycles:          %-34d ║\n", memAfter.numGC-memBefore.numGC)
	fmt.Println("╠══════════════════════════════════════════════════════════╣")
	fmt.Println("║ SERVER MEMORY (oxidb-server)                            ║")
	serverMem, serverErr := getServerMemory()
	if serverErr == nil {
		fmt.Printf("║   RSS:                %-34s ║\n", serverMem)
	} else {
		fmt.Printf("║   (could not read: %-36s) ║\n", serverErr)
	}
	fmt.Println("╚══════════════════════════════════════════════════════════╝")

	if errCount > 0 {
		fmt.Printf("\nWARNING: %d errors occurred (%.2f%%)\n", errCount, float64(errCount)/float64(total)*100)
	} else {
		fmt.Println("\nAll operations completed successfully.")
	}
}

func getServerMemory() (string, error) {
	pids, err := execCmd("pgrep", "-x", "oxidb-server")
	if err != nil || pids == "" {
		pids, err = execCmd("pgrep", "-f", "oxidb-server")
		if err != nil || pids == "" {
			return "", fmt.Errorf("server not found")
		}
	}
	var maxRSS uint64
	for _, pid := range strings.Split(pids, "\n") {
		pid = strings.TrimSpace(pid)
		if pid == "" {
			continue
		}
		out, err := execCmd("ps", "-o", "rss=", "-p", pid)
		if err != nil || out == "" {
			continue
		}
		kb, _ := strconv.ParseUint(strings.TrimSpace(out), 10, 64)
		if kb > maxRSS {
			maxRSS = kb
		}
	}
	if maxRSS == 0 {
		return "", fmt.Errorf("could not read RSS")
	}
	return fmtBytes(maxRSS * 1024), nil
}

func execCmd(name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}
