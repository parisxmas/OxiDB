package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/parisxmas/OxiDB/go/oxidb"
)

var (
	oxidbHost = envOr("OXIDB_HOST", "127.0.0.1")
	oxidbPort = envInt("OXIDB_PORT", 14888)
	udpPort   = envInt("UDP_PORT", 5140)
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

// GELF log levels
const (
	GelfEmergency = 0
	GelfAlert     = 1
	GelfCritical  = 2
	GelfError     = 3
	GelfWarning   = 4
	GelfNotice    = 5
	GelfInfo      = 6
	GelfDebug     = 7
)

var (
	hosts    = []string{"web-01", "web-02", "api-01", "api-02", "db-01", "worker-01", "worker-02", "proxy-01"}
	services = []string{"nginx", "app-server", "postgres", "redis", "queue-worker", "cron", "auth-service"}
	levels   = []int{GelfError, GelfWarning, GelfNotice, GelfInfo, GelfInfo, GelfInfo, GelfDebug, GelfDebug}
	messages = []string{
		"Request completed successfully",
		"Connection timeout after 30s",
		"Disk usage at 85%",
		"User login failed: invalid password",
		"Cache miss for key user:12345",
		"Database query took 250ms",
		"Worker process restarted",
		"SSL certificate expires in 7 days",
		"Memory usage: 2.4GB / 4.0GB",
		"Rate limit exceeded for IP 192.0.2.1000",
		"Background job completed: send_emails batch_id=4521",
		"Health check passed",
		"New deployment v2.3.1 rolling out",
		"Slow query detected: SELECT * FROM orders WHERE status='pending'",
		"Backup completed: 2.1GB compressed",
	}
)

func makeGELF(seq int) []byte {
	rng := rand.New(rand.NewSource(int64(seq)))
	msg := map[string]any{
		"version":       "1.1",
		"host":          hosts[rng.Intn(len(hosts))],
		"short_message": messages[rng.Intn(len(messages))],
		"timestamp":     float64(time.Now().UnixMilli()) / 1000.0,
		"level":         levels[rng.Intn(len(levels))],
		"_service":      services[rng.Intn(len(services))],
		"_request_id":   fmt.Sprintf("req-%08x", rng.Uint32()),
		"_duration_ms":  rng.Intn(5000),
		"_seq":          seq,
	}
	data, _ := json.Marshal(msg)
	return data
}

func main() {
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║          OxiDB UDP GELF Logging Benchmark                   ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Println()

	udpAddr := fmt.Sprintf("%s:%d", oxidbHost, udpPort)
	tcpClient, err := oxidb.Connect(oxidbHost, oxidbPort, 10*time.Second)
	if err != nil {
		fmt.Printf("FATAL: TCP connect: %v\n", err)
		os.Exit(1)
	}
	defer tcpClient.Close()
	fmt.Printf("  TCP: connected to %s:%d\n", oxidbHost, oxidbPort)
	fmt.Printf("  UDP: targeting %s\n", udpAddr)
	fmt.Println()

	// Clean up
	tcpClient.DropCollection("_udp_logs")
	time.Sleep(500 * time.Millisecond)

	// ─── Test 1: Single-thread UDP burst ─────────────────────────
	fmt.Println("═══ Test 1: Single-thread UDP burst (10K messages) ═══")
	{
		conn, err := net.Dial("udp", udpAddr)
		if err != nil {
			fmt.Printf("  UDP dial error: %v\n", err)
			os.Exit(1)
		}

		count := 10000
		t0 := time.Now()
		for i := 0; i < count; i++ {
			msg := makeGELF(i)
			conn.Write(msg)
		}
		sendDur := time.Since(t0)
		conn.Close()

		sendRate := float64(count) / sendDur.Seconds()
		fmt.Printf("  Sent: %d messages in %v (%.0f msg/sec send rate)\n", count, sendDur.Round(time.Millisecond), sendRate)

		// Wait for OxiDB to process
		time.Sleep(3 * time.Second)

		n, _ := tcpClient.Count("_udp_logs", map[string]any{})
		lossRate := float64(count-n) / float64(count) * 100
		fmt.Printf("  Stored: %d / %d (%.1f%% loss)\n", n, count, lossRate)
		fmt.Printf("  Effective ingest: %.0f msg/sec\n", float64(n)/sendDur.Seconds())
		fmt.Println()
	}

	// ─── Test 2: Multi-thread UDP burst ──────────────────────────
	fmt.Println("═══ Test 2: Multi-thread UDP burst (8 workers × 10K) ═══")
	tcpClient.DropCollection("_udp_logs")
	time.Sleep(500 * time.Millisecond)
	{
		workers := 8
		perWorker := 10000
		total := workers * perWorker
		var sent int64

		t0 := time.Now()
		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				conn, err := net.Dial("udp", udpAddr)
				if err != nil {
					return
				}
				defer conn.Close()
				base := id * perWorker
				for i := 0; i < perWorker; i++ {
					msg := makeGELF(base + i)
					conn.Write(msg)
					atomic.AddInt64(&sent, 1)
				}
			}(w)
		}
		wg.Wait()
		sendDur := time.Since(t0)

		sendRate := float64(sent) / sendDur.Seconds()
		fmt.Printf("  Sent: %d messages in %v (%.0f msg/sec send rate)\n", sent, sendDur.Round(time.Millisecond), sendRate)

		time.Sleep(5 * time.Second)

		n, _ := tcpClient.Count("_udp_logs", map[string]any{})
		lossRate := float64(total-n) / float64(total) * 100
		fmt.Printf("  Stored: %d / %d (%.1f%% loss)\n", n, total, lossRate)
		if sendDur.Seconds() > 0 {
			fmt.Printf("  Effective ingest: %.0f msg/sec\n", float64(n)/sendDur.Seconds())
		}
		fmt.Println()
	}

	// ─── Test 3: Sustained throughput (5 seconds) ────────────────
	fmt.Println("═══ Test 3: Sustained throughput (8 workers, 5 seconds) ═══")
	tcpClient.DropCollection("_udp_logs")
	time.Sleep(500 * time.Millisecond)
	{
		workers := 8
		duration := 5 * time.Second
		var sent int64
		var stop int64

		go func() {
			time.Sleep(duration)
			atomic.StoreInt64(&stop, 1)
		}()

		t0 := time.Now()
		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				conn, err := net.Dial("udp", udpAddr)
				if err != nil {
					return
				}
				defer conn.Close()
				seq := id * 10000000
				for atomic.LoadInt64(&stop) == 0 {
					msg := makeGELF(seq)
					conn.Write(msg)
					seq++
					atomic.AddInt64(&sent, 1)
				}
			}(w)
		}
		wg.Wait()
		sendDur := time.Since(t0)

		sendRate := float64(sent) / sendDur.Seconds()
		fmt.Printf("  Sent: %d messages in %v (%.0f msg/sec)\n", sent, sendDur.Round(time.Millisecond), sendRate)

		time.Sleep(5 * time.Second)

		n, _ := tcpClient.Count("_udp_logs", map[string]any{})
		lossRate := float64(int64(n)-sent) / float64(sent) * 100
		if lossRate < 0 {
			lossRate = -lossRate
		}
		ingestRate := float64(n) / sendDur.Seconds()
		fmt.Printf("  Stored: %d / %d (%.1f%% loss)\n", n, sent, lossRate)
		fmt.Printf("  Sustained ingest: %.0f msg/sec\n", ingestRate)
		fmt.Println()
	}

	// ─── Test 4: Query stored logs ───────────────────────────────
	fmt.Println("═══ Test 4: Query stored logs ═══")
	{
		// Count by level
		t0 := time.Now()
		n, _ := tcpClient.Count("_udp_logs", map[string]any{})
		countDur := time.Since(t0)
		fmt.Printf("  Total logs: %d (count in %v)\n", n, countDur.Round(time.Microsecond))

		// Find errors
		t0 = time.Now()
		errors, _ := tcpClient.Find("_udp_logs", map[string]any{"level": GelfError},
			&oxidb.FindOptions{Limit: intPtr(10)})
		findDur := time.Since(t0)
		fmt.Printf("  Error logs (limit 10): %d found in %v\n", len(errors), findDur.Round(time.Microsecond))

		// Find by host
		t0 = time.Now()
		hostLogs, _ := tcpClient.Find("_udp_logs", map[string]any{"host": "web-01"},
			&oxidb.FindOptions{Limit: intPtr(10)})
		hostDur := time.Since(t0)
		fmt.Printf("  web-01 logs (limit 10): %d found in %v\n", len(hostLogs), hostDur.Round(time.Microsecond))

		// Create index and query again
		tcpClient.CreateIndex("_udp_logs", "level")
		tcpClient.CreateIndex("_udp_logs", "host")
		tcpClient.CreateIndex("_udp_logs", "_service")
		time.Sleep(500 * time.Millisecond)

		t0 = time.Now()
		errors2, _ := tcpClient.Find("_udp_logs", map[string]any{"level": GelfError},
			&oxidb.FindOptions{Limit: intPtr(10)})
		findIdxDur := time.Since(t0)
		fmt.Printf("  Error logs (indexed, limit 10): %d found in %v\n", len(errors2), findIdxDur.Round(time.Microsecond))

		// Aggregate: count by service
		t0 = time.Now()
		agg, _ := tcpClient.Aggregate("_udp_logs", []map[string]any{
			{"$group": map[string]any{"_id": "$_service", "count": map[string]any{"$sum": 1}}},
			{"$sort": map[string]any{"count": -1}},
		})
		aggDur := time.Since(t0)
		fmt.Printf("  Aggregate by service: %d groups in %v\n", len(agg), aggDur.Round(time.Microsecond))
	}

	fmt.Println()
	fmt.Println("═══════════════════════════════════════════════════════════════")
	fmt.Println("Done.")
}

func intPtr(n int) *int { return &n }
