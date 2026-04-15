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
	gelfPort  = envInt("GELF_PORT", 12201)
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

// Realistic GELF message pools — modeled after KarteX .NET Serilog output
var (
	hosts = []string{
		"kartex-api-01", "kartex-api-02", "kartex-api-03",
		"kartex-worker-01", "kartex-worker-02",
		"kartex-web-01", "kartex-web-02",
		"kartex-scheduler-01",
	}
	applications = []string{"KarteX.API", "KarteX.Worker", "KarteX.Web", "KarteX.Scheduler"}
	environments = []string{"Docker", "Kubernetes", "Staging"}
	sourceContexts = []string{
		"KarteX.API.Endpoints.User.LoginEndpoint",
		"KarteX.API.Endpoints.User.RegisterEndpoint",
		"KarteX.API.Endpoints.IpLogPreProcessor",
		"KarteX.API.Endpoints.Order.CreateOrderEndpoint",
		"KarteX.API.Endpoints.Order.GetOrderEndpoint",
		"KarteX.API.Middleware.AuthenticationMiddleware",
		"KarteX.API.Middleware.RateLimitMiddleware",
		"KarteX.Worker.Jobs.SendEmailJob",
		"KarteX.Worker.Jobs.ProcessPaymentJob",
		"KarteX.Web.Controllers.DashboardController",
	}
	usernames = []string{
		"johndoe", "janedoe", "admin", "baris", "mehmet",
		"ayse", "fatma", "ali", "veli", "zeynep",
		"emre", "selin", "can", "deniz", "elif",
	}
	requestMethods = []string{"GET", "POST", "PUT", "DELETE", "PATCH"}
	requestPaths   = []string{
		"/Login", "/Register", "/api/orders", "/api/users",
		"/api/products", "/api/dashboard", "/api/reports",
		"/api/notifications", "/health", "/api/payments",
	}
	mobileClients  = []string{"iOS", "Android", "-"}
	mobileVersions = []string{"2.1.0", "2.0.5", "1.9.3", "2.2.0-beta"}
	userAgents     = []string{
		"KarteX/2.1.0 (iPhone; iOS 18.3)",
		"KarteX/2.0.5 (Samsung Galaxy S24; Android 15)",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/131.0",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/605.1",
		"KarteX/2.2.0-beta (Pixel 9; Android 15)",
	}
	messages = []string{
		"Login user: %s %s",
		"IpLog %s %s from %s user:%s",
		"Request completed successfully",
		"Authentication failed for user: %s",
		"Rate limit exceeded for IP %s",
		"Order created: order_id=%d",
		"Payment processed: amount=%.2f TRY",
		"Email sent to %s",
		"Database query took %dms",
		"Cache miss for key user:%s",
		"Health check passed",
		"Session expired for user: %s",
		"File uploaded: %d bytes",
		"Notification sent to %d recipients",
		"Background job completed in %dms",
	}
	levels = []int{3, 4, 5, 6, 6, 6, 6, 6, 7, 7} // weighted toward Info/Debug
)

func randomIP(rng *rand.Rand) string {
	// Turkish IP ranges
	prefixes := []string{"85.105", "78.186", "88.243", "176.236", "95.70", "46.196"}
	p := prefixes[rng.Intn(len(prefixes))]
	return fmt.Sprintf("%s.%d.%d", p, rng.Intn(256), rng.Intn(256))
}

func makeGELF(seq int) []byte {
	rng := rand.New(rand.NewSource(int64(seq) + time.Now().UnixNano()))

	ip := randomIP(rng)
	user := usernames[rng.Intn(len(usernames))]
	method := requestMethods[rng.Intn(len(requestMethods))]
	path := requestPaths[rng.Intn(len(requestPaths))]
	mobile := mobileClients[rng.Intn(len(mobileClients))]
	level := levels[rng.Intn(len(levels))]

	// Build short_message based on context
	var shortMsg string
	switch rng.Intn(5) {
	case 0:
		shortMsg = fmt.Sprintf("Login user: %s %s", user, ip)
	case 1:
		shortMsg = fmt.Sprintf("IpLog %s %s from %s user:%s", method, path, ip, user)
	case 2:
		shortMsg = fmt.Sprintf("Request %s %s completed in %dms", method, path, rng.Intn(2000))
	case 3:
		shortMsg = fmt.Sprintf("Order created: order_id=%d amount=%.2f", rng.Intn(100000), rng.Float64()*10000)
	case 4:
		shortMsg = fmt.Sprintf("Payment processed for %s: %.2f TRY", user, rng.Float64()*5000)
	}

	msg := map[string]any{
		"version":          "1.1",
		"host":             hosts[rng.Intn(len(hosts))],
		"short_message":    shortMsg,
		"timestamp":        float64(time.Now().UnixMilli()) / 1000.0,
		"level":            level,
		"facility":         applications[rng.Intn(len(applications))],
		"_Username":        user,
		"_ClientIp":        ip,
		"_RequestMethod":   method,
		"_RequestPath":     path,
		"_RequestUser":     user,
		"_MobileClient":    mobile,
		"_MobileVersion":   mobileVersions[rng.Intn(len(mobileVersions))],
		"_UserAgent":       userAgents[rng.Intn(len(userAgents))],
		"_SourceContext":   sourceContexts[rng.Intn(len(sourceContexts))],
		"_MachineName":     hosts[rng.Intn(len(hosts))],
		"_ProcessId":       rng.Intn(100) + 1,
		"_EnvironmentName": environments[rng.Intn(len(environments))],
		"_Application":     applications[rng.Intn(len(applications))],
		"_ResponseTimeMs":  rng.Intn(3000),
		"_StatusCode":      []int{200, 200, 200, 201, 204, 301, 400, 401, 403, 404, 500}[rng.Intn(11)],
		"_seq":             seq,
	}
	data, _ := json.Marshal(msg)
	return data
}

func main() {
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║        OxiDB GELF Ingestion Benchmark (auto-indexing)       ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Println()

	gelfAddr := fmt.Sprintf("%s:%d", oxidbHost, gelfPort)
	tcpClient, err := oxidb.Connect(oxidbHost, oxidbPort, 10*time.Second)
	if err != nil {
		fmt.Printf("FATAL: TCP connect: %v\n", err)
		os.Exit(1)
	}
	defer tcpClient.Close()
	fmt.Printf("  TCP:  connected to %s:%d\n", oxidbHost, oxidbPort)
	fmt.Printf("  GELF: targeting %s (UDP)\n", gelfAddr)
	fmt.Println()

	collection := "_gelf_logs"

	// ─── Test 1: Single-thread burst (10K) ───────────────────────
	fmt.Println("═══ Test 1: Single-thread GELF burst (10K messages) ═══")
	tcpClient.DropCollection(collection)
	time.Sleep(500 * time.Millisecond)
	{
		conn, err := net.Dial("udp", gelfAddr)
		if err != nil {
			fmt.Printf("  UDP dial error: %v\n", err)
			os.Exit(1)
		}

		count := 10_000
		t0 := time.Now()
		for i := 0; i < count; i++ {
			msg := makeGELF(i)
			conn.Write(msg)
		}
		sendDur := time.Since(t0)
		conn.Close()

		sendRate := float64(count) / sendDur.Seconds()
		fmt.Printf("  Sent:     %d messages in %v (%.0f msg/sec send rate)\n", count, sendDur.Round(time.Millisecond), sendRate)

		time.Sleep(3 * time.Second)

		n, _ := tcpClient.Count(collection, map[string]any{})
		lossRate := float64(count-n) / float64(count) * 100
		fmt.Printf("  Stored:   %d / %d (%.1f%% loss)\n", n, count, lossRate)
		fmt.Printf("  Ingest:   %.0f msg/sec\n", float64(n)/sendDur.Seconds())
		fmt.Println()
	}

	// ─── Test 2: Multi-thread burst (8 × 10K) ───────────────────
	fmt.Println("═══ Test 2: Multi-thread GELF burst (8 workers × 10K) ═══")
	tcpClient.DropCollection(collection)
	time.Sleep(2 * time.Second)
	{
		workers := 8
		perWorker := 10_000
		total := workers * perWorker
		var sent int64

		t0 := time.Now()
		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				conn, err := net.Dial("udp", gelfAddr)
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
		fmt.Printf("  Sent:     %d messages in %v (%.0f msg/sec send rate)\n", sent, sendDur.Round(time.Millisecond), sendRate)

		time.Sleep(5 * time.Second)

		n, _ := tcpClient.Count(collection, map[string]any{})
		lossRate := float64(total-n) / float64(total) * 100
		fmt.Printf("  Stored:   %d / %d (%.1f%% loss)\n", n, total, lossRate)
		fmt.Printf("  Ingest:   %.0f msg/sec\n", float64(n)/sendDur.Seconds())
		fmt.Println()
	}

	// ─── Test 3: Sustained throughput (5 seconds) ────────────────
	fmt.Println("═══ Test 3: Sustained throughput (8 workers, 5 seconds) ═══")
	tcpClient.DropCollection(collection)
	time.Sleep(2 * time.Second)
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
				conn, err := net.Dial("udp", gelfAddr)
				if err != nil {
					return
				}
				defer conn.Close()
				seq := id * 10_000_000
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
		fmt.Printf("  Sent:     %d messages in %v (%.0f msg/sec)\n", sent, sendDur.Round(time.Millisecond), sendRate)

		time.Sleep(5 * time.Second)

		n, _ := tcpClient.Count(collection, map[string]any{})
		lossRate := float64(int64(n)-sent) / float64(sent) * 100
		if lossRate < 0 {
			lossRate = -lossRate
		}
		fmt.Printf("  Stored:   %d / %d (%.1f%% loss)\n", n, sent, lossRate)
		fmt.Printf("  Sustained: %.0f msg/sec\n", float64(n)/sendDur.Seconds())
		fmt.Println()
	}

	// ─── Test 4: Query performance (auto-indexed fields) ─────────
	fmt.Println("═══ Test 4: Query performance (all fields auto-indexed) ═══")
	{
		// Count total
		t0 := time.Now()
		n, _ := tcpClient.Count(collection, map[string]any{})
		dur := time.Since(t0)
		fmt.Printf("  Total logs: %d (count in %v)\n", n, dur.Round(time.Microsecond))

		// Query by Username (auto-indexed)
		t0 = time.Now()
		docs, _ := tcpClient.Find(collection, map[string]any{"Username": "johndoe"},
			&oxidb.FindOptions{Limit: intPtr(10)})
		dur = time.Since(t0)
		fmt.Printf("  Username='johndoe' (limit 10): %d found in %v\n", len(docs), dur.Round(time.Microsecond))

		// Query by ClientIp (auto-indexed)
		t0 = time.Now()
		docs, _ = tcpClient.Find(collection, map[string]any{"ClientIp": map[string]any{"$regex": "^85\\.105"}},
			&oxidb.FindOptions{Limit: intPtr(10)})
		dur = time.Since(t0)
		fmt.Printf("  ClientIp ^85.105 (limit 10): %d found in %v\n", len(docs), dur.Round(time.Microsecond))

		// Query by level (auto-indexed)
		t0 = time.Now()
		docs, _ = tcpClient.Find(collection, map[string]any{"level": map[string]any{"$lte": 3}},
			&oxidb.FindOptions{Limit: intPtr(10)})
		dur = time.Since(t0)
		fmt.Printf("  level<=3 errors (limit 10): %d found in %v\n", len(docs), dur.Round(time.Microsecond))

		// Query by SourceContext (auto-indexed)
		t0 = time.Now()
		docs, _ = tcpClient.Find(collection, map[string]any{"SourceContext": "KarteX.API.Endpoints.User.LoginEndpoint"},
			&oxidb.FindOptions{Limit: intPtr(10)})
		dur = time.Since(t0)
		fmt.Printf("  SourceContext=LoginEndpoint (limit 10): %d found in %v\n", len(docs), dur.Round(time.Microsecond))

		// Query by MobileClient (auto-indexed)
		t0 = time.Now()
		docs, _ = tcpClient.Find(collection, map[string]any{"MobileClient": "iOS"},
			&oxidb.FindOptions{Limit: intPtr(10)})
		dur = time.Since(t0)
		fmt.Printf("  MobileClient='iOS' (limit 10): %d found in %v\n", len(docs), dur.Round(time.Microsecond))

		// Compound query
		t0 = time.Now()
		docs, _ = tcpClient.Find(collection, map[string]any{
			"RequestPath": "/Login",
			"MobileClient": "iOS",
		}, &oxidb.FindOptions{Limit: intPtr(10)})
		dur = time.Since(t0)
		fmt.Printf("  /Login + iOS (limit 10): %d found in %v\n", len(docs), dur.Round(time.Microsecond))

		fmt.Println()

		// Aggregation: top 5 users by request count
		t0 = time.Now()
		agg, _ := tcpClient.Aggregate(collection, []map[string]any{
			{"$group": map[string]any{"_id": "$Username", "count": map[string]any{"$sum": 1}}},
			{"$sort": map[string]any{"count": -1}},
			{"$limit": 5},
		})
		dur = time.Since(t0)
		fmt.Printf("  Top 5 users by request count (%v):\n", dur.Round(time.Microsecond))
		for _, row := range agg {
			fmt.Printf("    %-12s %v requests\n", row["_id"], row["count"])
		}

		// Aggregation: error count by Application
		t0 = time.Now()
		agg, _ = tcpClient.Aggregate(collection, []map[string]any{
			{"$match": map[string]any{"level": map[string]any{"$lte": 3}}},
			{"$group": map[string]any{"_id": "$Application", "errors": map[string]any{"$sum": 1}}},
			{"$sort": map[string]any{"errors": -1}},
		})
		dur = time.Since(t0)
		fmt.Printf("  Errors by Application (%v):\n", dur.Round(time.Microsecond))
		for _, row := range agg {
			fmt.Printf("    %-20s %v errors\n", row["_id"], row["errors"])
		}
	}

	fmt.Println()
	fmt.Println("═══════════════════════════════════════════════════════════════")
	fmt.Println("Done.")
}

func intPtr(n int) *int { return &n }
