// Package comparison_postgresql benchmarks OxiDB against PostgreSQL 16 JSONB
// with 100K documents. Both run in Docker containers with tmpfs storage.
//
// PostgreSQL stores documents as JSONB in a single column with GIN indexes.
// Queries use native JSONB operators (@>, ->>).
package comparison_postgresql

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/parisxmas/OxiDB/go/oxidb"
)

// ═══════════════════════════════════════════════════════════════════════════
// Constants & globals
// ═══════════════════════════════════════════════════════════════════════════

const (
	oxidbHost  = "127.0.0.1"
	oxidbPort  = 4444
	pgDSN      = "postgres://bench:bench@127.0.0.1:5432/bench?sslmode=disable"
	totalDocs  = 100_000
	batchSize  = 1000
	collection = "bench_employees"
)

var (
	oxiClient *oxidb.Client
	pgPool    *pgxpool.Pool
	ctx       = context.Background()

	timings     []TimingEntry
	timingMutex sync.Mutex
)

// ═══════════════════════════════════════════════════════════════════════════
// Timing
// ═══════════════════════════════════════════════════════════════════════════

type TimingEntry struct {
	Category  string
	Name      string
	OxiDur    time.Duration
	PgDur     time.Duration
	OxiCount  int
	PgCount   int
	OxiErr    string
	PgErr     string
	OxiDetail string
	PgDetail  string
}

func recordTiming(cat, name string, oxiDur, pgDur time.Duration, oxiCount, pgCount int, oxiErr, pgErr error) {
	timingMutex.Lock()
	defer timingMutex.Unlock()
	e := TimingEntry{
		Category: cat,
		Name:     name,
		OxiDur:   oxiDur,
		PgDur:    pgDur,
		OxiCount: oxiCount,
		PgCount:  pgCount,
	}
	if oxiErr != nil {
		e.OxiErr = oxiErr.Error()
	}
	if pgErr != nil {
		e.PgErr = pgErr.Error()
	}
	timings = append(timings, e)
}

func recordTimingDetailed(cat, name string, oxiDur, pgDur time.Duration, oxiCount, pgCount int, oxiErr, pgErr error, oxiDetail, pgDetail string) {
	timingMutex.Lock()
	defer timingMutex.Unlock()
	e := TimingEntry{
		Category:  cat,
		Name:      name,
		OxiDur:    oxiDur,
		PgDur:     pgDur,
		OxiCount:  oxiCount,
		PgCount:   pgCount,
		OxiDetail: oxiDetail,
		PgDetail:  pgDetail,
	}
	if oxiErr != nil {
		e.OxiErr = oxiErr.Error()
	}
	if pgErr != nil {
		e.PgErr = pgErr.Error()
	}
	timings = append(timings, e)
}

// ═══════════════════════════════════════════════════════════════════════════
// TestMain — global setup & teardown
// ═══════════════════════════════════════════════════════════════════════════

func TestMain(m *testing.M) {
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║   OxiDB vs PostgreSQL 16 JSONB — 100K Document Benchmark   ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")

	// Wait for OxiDB
	if err := waitFor("OxiDB", fmt.Sprintf("%s:%d", oxidbHost, oxidbPort), 60*time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: %v\n", err)
		os.Exit(1)
	}

	// Wait for PostgreSQL
	if err := waitFor("PostgreSQL", "127.0.0.1:5432", 60*time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: %v\n", err)
		os.Exit(1)
	}

	// Connect OxiDB
	var err error
	oxiClient, err = oxidb.Connect(oxidbHost, oxidbPort, 30*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: OxiDB connect: %v\n", err)
		os.Exit(1)
	}
	oxiClient.UseOxiWire()
	fmt.Println("  OxiDB connected (OxiWire binary protocol).")

	// Connect PostgreSQL
	pgPool, err = pgxpool.New(ctx, pgDSN)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: PostgreSQL connect: %v\n", err)
		os.Exit(1)
	}
	if err := pgPool.Ping(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: PostgreSQL ping: %v\n", err)
		os.Exit(1)
	}

	// Get PG version
	var pgVer string
	pgPool.QueryRow(ctx, "SHOW server_version").Scan(&pgVer)
	fmt.Printf("  PostgreSQL %s connected (tmpfs, fsync=off).\n\n", pgVer)

	code := m.Run()

	oxiClient.Close()
	pgPool.Close()

	generateHTMLReport()

	os.Exit(code)
}

func waitFor(name, addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err == nil {
			conn.Close()
			fmt.Printf("  %s ready at %s\n", name, addr)
			return nil
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("%s not reachable at %s after %s", name, addr, timeout)
}

// ═══════════════════════════════════════════════════════════════════════════
// Data generation (same as MongoDB benchmark)
// ═══════════════════════════════════════════════════════════════════════════

var (
	firstNames = []string{"Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hank", "Ivy", "Jack"}
	lastNames  = []string{"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"}
	cities     = []string{"New York", "London", "Tokyo", "Paris", "Berlin", "Sydney", "Toronto", "Mumbai", "Dubai", "Singapore"}
	countries  = []string{"US", "UK", "JP", "FR", "DE", "AU", "CA", "IN", "AE", "SG"}
	depts      = []string{"Engineering", "Sales", "Marketing", "Finance", "HR"}
	statuses   = []string{"active", "inactive", "suspended", "pending"}
)

func genDoc(rng *rand.Rand, i int) map[string]any {
	first := firstNames[rng.Intn(len(firstNames))]
	last := lastNames[rng.Intn(len(lastNames))]
	return map[string]any{
		"seq":        i,
		"name":       first + " " + last,
		"email":      fmt.Sprintf("%s.%s.%d@test.com", strings.ToLower(first), strings.ToLower(last), i),
		"age":        18 + rng.Intn(60),
		"salary":     30000.0 + float64(rng.Intn(170000)),
		"department": depts[rng.Intn(len(depts))],
		"city":       cities[rng.Intn(len(cities))],
		"country":    countries[rng.Intn(len(countries))],
		"status":     statuses[rng.Intn(len(statuses))],
		"score":      float64(rng.Intn(10000)) / 100.0,
		"verified":   rng.Intn(2) == 1,
		"rating":     rng.Intn(5) + 1,
		"address": map[string]any{
			"street": fmt.Sprintf("%d Main St", 100+rng.Intn(9900)),
			"zip":    fmt.Sprintf("%05d", rng.Intn(100000)),
		},
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Test: Bulk Insert 100K documents
// ═══════════════════════════════════════════════════════════════════════════

func TestBulkInsert(t *testing.T) {
	// Cleanup
	_ = oxiClient.DropCollection(collection)
	pgPool.Exec(ctx, "DROP TABLE IF EXISTS docs")
	pgPool.Exec(ctx, "CREATE TABLE docs (id SERIAL PRIMARY KEY, data JSONB NOT NULL)")

	rng := rand.New(rand.NewSource(42))

	// Pre-generate all documents
	allDocs := make([]map[string]any, totalDocs)
	for i := 0; i < totalDocs; i++ {
		allDocs[i] = genDoc(rng, i)
	}

	// ── OxiDB insert (pipelined) ──
	const pipelineSize = 10
	oxiStart := time.Now()
	oxiInserted := 0
	for offset := 0; offset < totalDocs; offset += batchSize * pipelineSize {
		var pipeBatches [][]map[string]any
		for b := 0; b < pipelineSize; b++ {
			start := offset + b*batchSize
			if start >= totalDocs {
				break
			}
			end := start + batchSize
			if end > totalDocs {
				end = totalDocs
			}
			pipeBatches = append(pipeBatches, allDocs[start:end])
		}
		n, err := oxiClient.PipelineInsertMany(collection, pipeBatches)
		if err != nil {
			t.Fatalf("OxiDB pipeline insert at %d: %v", offset, err)
		}
		oxiInserted += n
	}
	oxiDur := time.Since(oxiStart)

	// ── PostgreSQL insert (COPY protocol for max throughput) ──
	pgStart := time.Now()
	for offset := 0; offset < totalDocs; offset += batchSize {
		end := offset + batchSize
		if end > totalDocs {
			end = totalDocs
		}
		batch := allDocs[offset:end]

		tx, err := pgPool.Begin(ctx)
		if err != nil {
			t.Fatalf("PG begin: %v", err)
		}
		for _, doc := range batch {
			jsonBytes, _ := json.Marshal(doc)
			_, err := tx.Exec(ctx, "INSERT INTO docs (data) VALUES ($1)", jsonBytes)
			if err != nil {
				tx.Rollback(ctx)
				t.Fatalf("PG insert: %v", err)
			}
		}
		tx.Commit(ctx)
	}
	pgDur := time.Since(pgStart)

	// Verify counts
	oxiCount, err := oxiClient.Count(collection, map[string]any{})
	if err != nil {
		t.Fatalf("OxiDB count: %v", err)
	}
	var pgCount int
	pgPool.QueryRow(ctx, "SELECT COUNT(*) FROM docs").Scan(&pgCount)

	oxiRate := float64(totalDocs) / oxiDur.Seconds()
	pgRate := float64(totalDocs) / pgDur.Seconds()

	fmt.Printf("\n  ┌─ Bulk Insert %d documents ─────────────────────────────\n", totalDocs)
	fmt.Printf("  │  OxiDB:      %v  (%.0f docs/s)\n", oxiDur.Round(time.Millisecond), oxiRate)
	fmt.Printf("  │  PostgreSQL: %v  (%.0f docs/s)\n", pgDur.Round(time.Millisecond), pgRate)
	fmt.Printf("  │  Counts:     OxiDB=%d  PostgreSQL=%d\n", oxiCount, pgCount)
	fmt.Printf("  └───────────────────────────────────────────────────────\n\n")

	recordTimingDetailed("Bulk Insert", fmt.Sprintf("Insert %d docs (batch %d)", totalDocs, batchSize),
		oxiDur, pgDur, oxiCount, pgCount, nil, nil,
		fmt.Sprintf("%.0f docs/s", oxiRate), fmt.Sprintf("%.0f docs/s", pgRate))

	if oxiCount != totalDocs {
		t.Errorf("OxiDB count mismatch: got %d, want %d", oxiCount, totalDocs)
	}
	if pgCount != totalDocs {
		t.Errorf("PostgreSQL count mismatch: got %d, want %d", pgCount, totalDocs)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Test: Queries (unindexed)
// ═══════════════════════════════════════════════════════════════════════════

func TestQueries(t *testing.T) {
	oxiCount, err := oxiClient.Count(collection, map[string]any{})
	if err != nil || oxiCount == 0 {
		t.Skip("No data — run TestBulkInsert first")
	}

	tests := []struct {
		name     string
		oxiQuery map[string]any
		pgSQL    string
	}{
		{
			name:     "Exact match (department=Engineering)",
			oxiQuery: map[string]any{"department": "Engineering"},
			pgSQL:    "SELECT data FROM docs WHERE data->>'department' = 'Engineering'",
		},
		{
			name:     "Range query (age >= 50)",
			oxiQuery: map[string]any{"age": map[string]any{"$gte": 50}},
			pgSQL:    "SELECT data FROM docs WHERE (data->>'age')::int >= 50",
		},
		{
			name:     "Compound (dept=Sales AND status=active)",
			oxiQuery: map[string]any{"department": "Sales", "status": "active"},
			pgSQL:    "SELECT data FROM docs WHERE data->>'department' = 'Sales' AND data->>'status' = 'active'",
		},
		{
			name: "$or query (city=Tokyo OR city=Paris)",
			oxiQuery: map[string]any{"$or": []any{
				map[string]any{"city": "Tokyo"},
				map[string]any{"city": "Paris"},
			}},
			pgSQL: "SELECT data FROM docs WHERE data->>'city' IN ('Tokyo', 'Paris')",
		},
		{
			name:     "$in query (country in [US, UK, JP])",
			oxiQuery: map[string]any{"country": map[string]any{"$in": []any{"US", "UK", "JP"}}},
			pgSQL:    "SELECT data FROM docs WHERE data->>'country' IN ('US', 'UK', 'JP')",
		},
		{
			name:     "Range (salary 50000-100000)",
			oxiQuery: map[string]any{"salary": map[string]any{"$gte": 50000, "$lte": 100000}},
			pgSQL:    "SELECT data FROM docs WHERE (data->>'salary')::numeric >= 50000 AND (data->>'salary')::numeric <= 100000",
		},
		{
			name:     "Boolean (verified=true)",
			oxiQuery: map[string]any{"verified": true},
			pgSQL:    "SELECT data FROM docs WHERE (data->>'verified')::boolean = true",
		},
		{
			name:     "Nested field (address.zip starts with 0)",
			oxiQuery: map[string]any{"address.zip": map[string]any{"$gte": "00000", "$lt": "10000"}},
			pgSQL:    "SELECT data FROM docs WHERE data->'address'->>'zip' >= '00000' AND data->'address'->>'zip' < '10000'",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// OxiDB
			oxiStart := time.Now()
			oxiDocs, oxiErr := oxiClient.Find(collection, tc.oxiQuery, nil)
			oxiDur := time.Since(oxiStart)

			// PostgreSQL
			pgStart := time.Now()
			rows, pgErr := pgPool.Query(ctx, tc.pgSQL)
			pgN := 0
			if pgErr == nil {
				for rows.Next() {
					var data []byte
					rows.Scan(&data)
					pgN++
				}
				rows.Close()
			}
			pgDur := time.Since(pgStart)

			oxiN := len(oxiDocs)

			fmt.Printf("  %-45s  OxiDB: %6d docs in %v  |  PG: %6d docs in %v\n",
				tc.name, oxiN, oxiDur.Round(100*time.Microsecond), pgN, pgDur.Round(100*time.Microsecond))

			recordTiming("Queries", tc.name, oxiDur, pgDur, oxiN, pgN, oxiErr, pgErr)
		})
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Test: Queries with indexes
// ═══════════════════════════════════════════════════════════════════════════

func TestIndexedQueries(t *testing.T) {
	oxiCount, err := oxiClient.Count(collection, map[string]any{})
	if err != nil || oxiCount == 0 {
		t.Skip("No data — run TestBulkInsert first")
	}

	// Create indexes on both
	fmt.Println("\n  Creating indexes...")
	oxiStart := time.Now()
	_ = oxiClient.CreateIndex(collection, "department")
	_ = oxiClient.CreateIndex(collection, "age")
	_ = oxiClient.CreateIndex(collection, "city")
	_ = oxiClient.CreateIndex(collection, "salary")
	oxiIdxDur := time.Since(oxiStart)

	pgStart := time.Now()
	pgPool.Exec(ctx, "CREATE INDEX idx_dept ON docs ((data->>'department'))")
	pgPool.Exec(ctx, "CREATE INDEX idx_age ON docs (((data->>'age')::int))")
	pgPool.Exec(ctx, "CREATE INDEX idx_city ON docs ((data->>'city'))")
	pgPool.Exec(ctx, "CREATE INDEX idx_salary ON docs (((data->>'salary')::numeric))")
	pgPool.Exec(ctx, "ANALYZE docs")
	pgIdxDur := time.Since(pgStart)

	fmt.Printf("  Index creation:  OxiDB: %v  |  PostgreSQL: %v\n\n",
		oxiIdxDur.Round(time.Millisecond), pgIdxDur.Round(time.Millisecond))
	recordTiming("Indexes", "Create 4 indexes on 100K docs", oxiIdxDur, pgIdxDur, 4, 4, nil, nil)

	tests := []struct {
		name     string
		oxiQuery map[string]any
		pgSQL    string
	}{
		{
			name:     "Indexed: department=Engineering",
			oxiQuery: map[string]any{"department": "Engineering"},
			pgSQL:    "SELECT data FROM docs WHERE data->>'department' = 'Engineering'",
		},
		{
			name:     "Indexed: age >= 60",
			oxiQuery: map[string]any{"age": map[string]any{"$gte": 60}},
			pgSQL:    "SELECT data FROM docs WHERE (data->>'age')::int >= 60",
		},
		{
			name:     "Indexed: city=Tokyo",
			oxiQuery: map[string]any{"city": "Tokyo"},
			pgSQL:    "SELECT data FROM docs WHERE data->>'city' = 'Tokyo'",
		},
		{
			name:     "Indexed: salary 80000-120000",
			oxiQuery: map[string]any{"salary": map[string]any{"$gte": 80000, "$lte": 120000}},
			pgSQL:    "SELECT data FROM docs WHERE (data->>'salary')::numeric >= 80000 AND (data->>'salary')::numeric <= 120000",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			oxiStart := time.Now()
			oxiDocs, oxiErr := oxiClient.Find(collection, tc.oxiQuery, nil)
			oxiDur := time.Since(oxiStart)

			pgStart := time.Now()
			rows, pgErr := pgPool.Query(ctx, tc.pgSQL)
			pgN := 0
			if pgErr == nil {
				for rows.Next() {
					var data []byte
					rows.Scan(&data)
					pgN++
				}
				rows.Close()
			}
			pgDur := time.Since(pgStart)

			fmt.Printf("  %-45s  OxiDB: %6d docs in %v  |  PG: %6d docs in %v\n",
				tc.name, len(oxiDocs), oxiDur.Round(100*time.Microsecond), pgN, pgDur.Round(100*time.Microsecond))

			recordTiming("Indexed Queries", tc.name, oxiDur, pgDur, len(oxiDocs), pgN, oxiErr, pgErr)
		})
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Test: Count queries
// ═══════════════════════════════════════════════════════════════════════════

func TestCountQueries(t *testing.T) {
	oxiCount, err := oxiClient.Count(collection, map[string]any{})
	if err != nil || oxiCount == 0 {
		t.Skip("No data — run TestBulkInsert first")
	}

	tests := []struct {
		name     string
		oxiQuery map[string]any
		pgSQL    string
	}{
		{
			name:     "Count all",
			oxiQuery: map[string]any{},
			pgSQL:    "SELECT COUNT(*) FROM docs",
		},
		{
			name:     "Count dept=Engineering",
			oxiQuery: map[string]any{"department": "Engineering"},
			pgSQL:    "SELECT COUNT(*) FROM docs WHERE data->>'department' = 'Engineering'",
		},
		{
			name:     "Count age >= 50",
			oxiQuery: map[string]any{"age": map[string]any{"$gte": 50}},
			pgSQL:    "SELECT COUNT(*) FROM docs WHERE (data->>'age')::int >= 50",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			oxiStart := time.Now()
			oxiN, oxiErr := oxiClient.Count(collection, tc.oxiQuery)
			oxiDur := time.Since(oxiStart)

			pgStart := time.Now()
			var pgN int
			pgErr := pgPool.QueryRow(ctx, tc.pgSQL).Scan(&pgN)
			pgDur := time.Since(pgStart)

			fmt.Printf("  %-45s  OxiDB: %6d in %v  |  PG: %6d in %v\n",
				tc.name, oxiN, oxiDur.Round(100*time.Microsecond), pgN, pgDur.Round(100*time.Microsecond))

			recordTiming("Count", tc.name, oxiDur, pgDur, oxiN, pgN, oxiErr, pgErr)
		})
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Test: Aggregation
// ═══════════════════════════════════════════════════════════════════════════

func TestAggregation(t *testing.T) {
	oxiCount, err := oxiClient.Count(collection, map[string]any{})
	if err != nil || oxiCount == 0 {
		t.Skip("No data — run TestBulkInsert first")
	}

	t.Run("Group by department, avg salary", func(t *testing.T) {
		oxiPipeline := []map[string]any{
			{"$group": map[string]any{
				"_id":        "$department",
				"avg_salary": map[string]any{"$avg": "$salary"},
				"count":      map[string]any{"$sum": 1},
			}},
			{"$sort": map[string]any{"_id": 1}},
		}

		pgSQL := `SELECT data->>'department' AS dept, AVG((data->>'salary')::numeric), COUNT(*)
		          FROM docs GROUP BY data->>'department' ORDER BY data->>'department'`

		oxiStart := time.Now()
		oxiRes, oxiErr := oxiClient.Aggregate(collection, oxiPipeline)
		oxiDur := time.Since(oxiStart)

		pgStart := time.Now()
		rows, pgErr := pgPool.Query(ctx, pgSQL)
		pgN := 0
		if pgErr == nil {
			for rows.Next() {
				var dept string
				var avg float64
				var count int
				rows.Scan(&dept, &avg, &count)
				pgN++
			}
			rows.Close()
		}
		pgDur := time.Since(pgStart)

		fmt.Printf("  Group by dept (avg salary):  OxiDB: %d groups in %v  |  PG: %d groups in %v\n",
			len(oxiRes), oxiDur.Round(100*time.Microsecond), pgN, pgDur.Round(100*time.Microsecond))

		recordTiming("Aggregation", "Group by department, avg salary", oxiDur, pgDur, len(oxiRes), pgN, oxiErr, pgErr)
	})

	t.Run("Top 5 cities by count", func(t *testing.T) {
		oxiPipeline := []map[string]any{
			{"$group": map[string]any{
				"_id":   "$city",
				"count": map[string]any{"$sum": 1},
			}},
			{"$sort": map[string]any{"count": -1}},
			{"$limit": 5},
		}

		pgSQL := `SELECT data->>'city', COUNT(*) AS cnt FROM docs
		          GROUP BY data->>'city' ORDER BY cnt DESC LIMIT 5`

		oxiStart := time.Now()
		oxiRes, oxiErr := oxiClient.Aggregate(collection, oxiPipeline)
		oxiDur := time.Since(oxiStart)

		pgStart := time.Now()
		rows, pgErr := pgPool.Query(ctx, pgSQL)
		pgN := 0
		if pgErr == nil {
			for rows.Next() {
				var city string
				var cnt int
				rows.Scan(&city, &cnt)
				pgN++
			}
			rows.Close()
		}
		pgDur := time.Since(pgStart)

		fmt.Printf("  Top 5 cities by count:       OxiDB: %d in %v  |  PG: %d in %v\n",
			len(oxiRes), oxiDur.Round(100*time.Microsecond), pgN, pgDur.Round(100*time.Microsecond))

		recordTiming("Aggregation", "Top 5 cities by count", oxiDur, pgDur, len(oxiRes), pgN, oxiErr, pgErr)
	})

	t.Run("Match + Group (active engineers avg score)", func(t *testing.T) {
		oxiPipeline := []map[string]any{
			{"$match": map[string]any{"status": "active", "department": "Engineering"}},
			{"$group": map[string]any{
				"_id":       nil,
				"avg_score": map[string]any{"$avg": "$score"},
				"count":     map[string]any{"$sum": 1},
			}},
		}

		pgSQL := `SELECT AVG((data->>'score')::numeric), COUNT(*)
		          FROM docs WHERE data->>'status' = 'active' AND data->>'department' = 'Engineering'`

		oxiStart := time.Now()
		oxiRes, oxiErr := oxiClient.Aggregate(collection, oxiPipeline)
		oxiDur := time.Since(oxiStart)

		pgStart := time.Now()
		var pgAvg float64
		var pgCnt int
		pgErr := pgPool.QueryRow(ctx, pgSQL).Scan(&pgAvg, &pgCnt)
		pgDur := time.Since(pgStart)
		pgN := 0
		if pgErr == nil {
			pgN = 1
		}

		fmt.Printf("  Active engineers avg score:   OxiDB: %d in %v  |  PG: %d in %v\n",
			len(oxiRes), oxiDur.Round(100*time.Microsecond), pgN, pgDur.Round(100*time.Microsecond))

		recordTiming("Aggregation", "Match + Group (active engineers)", oxiDur, pgDur, len(oxiRes), pgN, oxiErr, pgErr)
	})
}

// ═══════════════════════════════════════════════════════════════════════════
// Test: Resource Usage
// ═══════════════════════════════════════════════════════════════════════════

func TestResourceUsage(t *testing.T) {
	oxiCount, err := oxiClient.Count(collection, map[string]any{})
	if err != nil || oxiCount == 0 {
		t.Skip("No data — run TestBulkInsert first")
	}

	fmt.Println("\n  ┌─ Resource Usage ──────────────────────────────────────────")

	oxiDisk, oxiMem := getContainerStats("oxidb")
	pgDisk, pgMem := getContainerStats("postgres")

	fmt.Printf("  │  OxiDB      — Disk: %s  Memory: %s\n", oxiDisk, oxiMem)
	fmt.Printf("  │  PostgreSQL — Disk: %s  Memory: %s\n", pgDisk, pgMem)
	fmt.Printf("  └───────────────────────────────────────────────────────────\n\n")

	recordTimingDetailed("Resources", "Disk usage (100K docs)", 0, 0, 0, 0, nil, nil, oxiDisk, pgDisk)
	recordTimingDetailed("Resources", "Memory usage (100K docs)", 0, 0, 0, 0, nil, nil, oxiMem, pgMem)
}

func getContainerStats(serviceName string) (disk string, mem string) {
	disk = "N/A"
	mem = "N/A"

	containerName := getContainerName(serviceName)
	if containerName == "" {
		return
	}

	mem = dockerExec("docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", containerName)

	switch serviceName {
	case "oxidb":
		disk = dockerExec("docker", "exec", containerName, "du", "-sh", "/data")
	case "postgres":
		disk = dockerExec("docker", "exec", containerName, "du", "-sh", "/var/lib/postgresql/data")
	}

	return strings.TrimSpace(disk), strings.TrimSpace(mem)
}

func getContainerName(service string) string {
	out := dockerExec("docker", "compose", "-f", "docker-compose.yml", "ps", "-q", service)
	return strings.TrimSpace(out)
}

func dockerExec(args ...string) string {
	cmd := execCommand(args[0], args[1:]...)
	out, err := cmd.Output()
	if err != nil {
		return "N/A"
	}
	return strings.TrimSpace(string(out))
}

// ═══════════════════════════════════════════════════════════════════════════
// Benchmarks (go test -bench=.)
// ═══════════════════════════════════════════════════════════════════════════

func BenchmarkOxiDBInsert(b *testing.B) {
	col := fmt.Sprintf("bench_insert_%d", time.Now().UnixNano()%100000)
	defer oxiClient.DropCollection(col)
	rng := rand.New(rand.NewSource(99))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		doc := genDoc(rng, i)
		_, err := oxiClient.Insert(col, doc)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "docs/s")
}

func BenchmarkPgInsert(b *testing.B) {
	pgPool.Exec(ctx, "DROP TABLE IF EXISTS bench_insert_pg")
	pgPool.Exec(ctx, "CREATE TABLE bench_insert_pg (id SERIAL PRIMARY KEY, data JSONB NOT NULL)")
	defer pgPool.Exec(ctx, "DROP TABLE IF EXISTS bench_insert_pg")
	rng := rand.New(rand.NewSource(99))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		doc := genDoc(rng, i)
		jsonBytes, _ := json.Marshal(doc)
		_, err := pgPool.Exec(ctx, "INSERT INTO bench_insert_pg (data) VALUES ($1)", jsonBytes)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "docs/s")
}

func BenchmarkOxiDBFind(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := oxiClient.Find(collection, map[string]any{"department": "Engineering"}, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "queries/s")
}

func BenchmarkPgFind(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := pgPool.Query(ctx, "SELECT data FROM docs WHERE data->>'department' = 'Engineering'")
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
			var data []byte
			rows.Scan(&data)
		}
		rows.Close()
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "queries/s")
}

// ═══════════════════════════════════════════════════════════════════════════
// System Info
// ═══════════════════════════════════════════════════════════════════════════

type SystemInfo struct {
	OS     string
	Arch   string
	CPU    string
	Cores  int
	RAM    string
	GoVer  string
	Docker string
	PgVer  string
}

func getSystemInfo() SystemInfo {
	info := SystemInfo{
		OS:    runtime.GOOS + "/" + runtime.GOARCH,
		Arch:  runtime.GOARCH,
		Cores: runtime.NumCPU(),
		GoVer: runtime.Version(),
	}

	switch runtime.GOOS {
	case "darwin":
		if out, err := exec.Command("sysctl", "-n", "machdep.cpu.brand_string").Output(); err == nil {
			info.CPU = strings.TrimSpace(string(out))
		}
		if out, err := exec.Command("sysctl", "-n", "hw.memsize").Output(); err == nil {
			var memBytes uint64
			fmt.Sscanf(strings.TrimSpace(string(out)), "%d", &memBytes)
			info.RAM = fmt.Sprintf("%.0f GB", float64(memBytes)/(1024*1024*1024))
		}
	case "linux":
		if out, err := exec.Command("bash", "-c", `grep -m1 'model name' /proc/cpuinfo | cut -d: -f2`).Output(); err == nil {
			info.CPU = strings.TrimSpace(string(out))
		}
		if out, err := exec.Command("bash", "-c", `grep MemTotal /proc/meminfo | awk '{printf "%.0f GB", $2/1024/1024}'`).Output(); err == nil {
			info.RAM = strings.TrimSpace(string(out))
		}
	}
	if info.CPU == "" {
		info.CPU = fmt.Sprintf("%s (%d cores)", runtime.GOARCH, runtime.NumCPU())
	}
	if info.RAM == "" {
		info.RAM = "unknown"
	}

	if out, err := exec.Command("docker", "version", "--format", "{{.Server.Version}}").Output(); err == nil {
		info.Docker = strings.TrimSpace(string(out))
	}

	// PostgreSQL version
	if pgPool != nil {
		var ver string
		if err := pgPool.QueryRow(ctx, "SHOW server_version").Scan(&ver); err == nil {
			info.PgVer = ver
		}
	}

	return info
}

// ═══════════════════════════════════════════════════════════════════════════
// HTML Report
// ═══════════════════════════════════════════════════════════════════════════

func fmtDur(d time.Duration) string {
	if d == 0 {
		return "N/A"
	}
	if d < time.Millisecond {
		return fmt.Sprintf("%.0f&micro;s", float64(d.Microseconds()))
	}
	if d < time.Second {
		return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000.0)
	}
	return fmt.Sprintf("%.2fs", d.Seconds())
}

func countResourceEntries(entries []TimingEntry) int {
	n := 0
	for _, e := range entries {
		if e.OxiDur == 0 && e.PgDur == 0 {
			n++
		}
	}
	return n
}

func generateHTMLReport() {
	timingMutex.Lock()
	defer timingMutex.Unlock()

	if len(timings) == 0 {
		return
	}

	sysInfo := getSystemInfo()

	type catGroup struct {
		Name    string
		Entries []TimingEntry
	}
	catMap := map[string]*catGroup{}
	catOrder := []string{}
	for _, e := range timings {
		if _, ok := catMap[e.Category]; !ok {
			catMap[e.Category] = &catGroup{Name: e.Category}
			catOrder = append(catOrder, e.Category)
		}
		catMap[e.Category].Entries = append(catMap[e.Category].Entries, e)
	}

	oxiWins, pgWins := 0, 0
	for _, e := range timings {
		if e.PgDur == 0 || e.OxiDur == 0 {
			continue
		}
		if e.OxiDur < e.PgDur {
			oxiWins++
		} else if e.PgDur < e.OxiDur {
			pgWins++
		}
	}

	catIcon := map[string]string{
		"Bulk Insert":     "&#9654;",
		"Queries":         "&#128269;",
		"Indexed Queries": "&#9889;",
		"Indexes":         "&#9889;",
		"Count":           "&#35;",
		"Aggregation":     "&#8721;",
		"Resources":       "&#9881;",
	}

	var sb strings.Builder
	sb.WriteString(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>OxiDB vs PostgreSQL — Benchmark</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700;800&family=Outfit:wght@300;400;500;600;700;800;900&display=swap');

*{margin:0;padding:0;box-sizing:border-box}

:root{
  --bg:#06080c;--surface:#0c1018;--card:#111820;--border:#1a2332;
  --text:#c8d6e5;--dim:#5a6a7e;--bright:#e8f0f8;
  --oxi:#22d666;--oxi-dim:#1a4a2e;--oxi-glow:rgba(34,214,102,0.12);
  --pg:#336791;--pg-dim:#1a2a4a;--pg-glow:rgba(51,103,145,0.12);
  --accent:#f0c040;--red:#e84057;
}

body{
  font-family:'Outfit',system-ui,sans-serif;
  background:var(--bg);color:var(--text);
  min-height:100vh;overflow-x:hidden;
}

body::before{
  content:'';position:fixed;inset:0;z-index:9999;pointer-events:none;
  opacity:0.025;
  background-image:url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)'/%3E%3C/svg%3E");
}

body::after{
  content:'';position:fixed;top:0;left:0;right:0;height:2px;z-index:100;
  background:linear-gradient(90deg,transparent,var(--oxi),var(--accent),var(--pg),transparent);
}

.wrap{max-width:1100px;margin:0 auto;padding:48px 24px 80px}

header{margin-bottom:32px}
header h1{
  font-family:'JetBrains Mono',monospace;font-size:13px;font-weight:500;
  color:var(--dim);letter-spacing:3px;text-transform:uppercase;margin-bottom:12px;
}
header .title{
  font-size:42px;font-weight:800;letter-spacing:-1px;
  background:linear-gradient(135deg,var(--oxi) 0%,#40e8a0 40%,#5b9bd5 100%);
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;
  line-height:1.1;margin-bottom:16px;
}
header .meta{
  font-family:'JetBrains Mono',monospace;font-size:12px;color:var(--dim);
  display:flex;gap:24px;flex-wrap:wrap;
}
header .meta span::before{content:'› ';color:var(--border)}

.methodology{
  font-family:'JetBrains Mono',monospace;font-size:12px;line-height:1.7;
  color:var(--dim);margin-bottom:32px;padding:20px 24px;
  background:var(--surface);border:1px solid var(--border);border-radius:12px;
}
.methodology strong{color:var(--text);font-weight:600}
.methodology .hl-oxi{color:var(--oxi)}
.methodology .hl-pg{color:#5b9bd5}

.env-panel{
  background:var(--surface);border:1px solid var(--border);border-radius:12px;
  padding:24px 28px;margin-bottom:36px;position:relative;overflow:hidden;
}
.env-panel::before{
  content:'';position:absolute;top:0;left:0;right:0;height:2px;
  background:linear-gradient(90deg,var(--oxi),var(--border),var(--pg));
}
.env-title{
  font-family:'JetBrains Mono',monospace;font-size:11px;font-weight:600;
  color:var(--dim);letter-spacing:2px;text-transform:uppercase;margin-bottom:16px;
  display:flex;align-items:center;gap:8px;
}
.env-grid{
  display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:12px 24px;
}
.env-item{display:flex;flex-direction:column;gap:2px}
.env-label{font-family:'JetBrains Mono',monospace;font-size:10px;font-weight:600;color:var(--dim);letter-spacing:1px;text-transform:uppercase}
.env-value{font-family:'JetBrains Mono',monospace;font-size:13px;font-weight:500;color:var(--bright)}

.score-strip{
  display:grid;grid-template-columns:1fr auto 1fr;align-items:center;gap:0;
  background:var(--surface);border:1px solid var(--border);border-radius:16px;
  padding:32px 40px;margin-bottom:48px;position:relative;overflow:hidden;
}
.score-strip::before{
  content:'';position:absolute;inset:0;
  background:linear-gradient(135deg,var(--oxi-glow),transparent 50%,var(--pg-glow));
  pointer-events:none;
}
.score-side{text-align:center;position:relative}
.score-side .db-label{
  font-family:'JetBrains Mono',monospace;font-size:11px;font-weight:600;
  letter-spacing:2px;text-transform:uppercase;margin-bottom:12px;
}
.score-side .db-label.oxi{color:var(--oxi)}
.score-side .db-label.pg{color:#5b9bd5}
.score-num{font-size:72px;font-weight:900;line-height:1;letter-spacing:-3px}
.score-num.oxi{color:var(--oxi)}
.score-num.pg{color:#5b9bd5}
.score-sub{font-size:13px;color:var(--dim);margin-top:6px;font-weight:500}
.score-vs{
  font-family:'JetBrains Mono',monospace;font-size:14px;font-weight:700;
  color:var(--border);padding:0 24px;
  display:flex;flex-direction:column;align-items:center;gap:4px;
}
.score-vs::before,.score-vs::after{content:'';width:1px;height:32px;background:var(--border)}

.cat{margin-bottom:36px}
.cat-head{
  display:flex;align-items:center;gap:10px;
  padding:14px 0;margin-bottom:2px;border-bottom:1px solid var(--border);
}
.cat-icon{width:28px;height:28px;display:flex;align-items:center;justify-content:center;background:var(--card);border:1px solid var(--border);border-radius:8px;font-size:14px;flex-shrink:0}
.cat-name{font-size:16px;font-weight:700;color:var(--bright)}
.cat-count{font-family:'JetBrains Mono',monospace;font-size:11px;color:var(--dim);margin-left:auto}

.row{
  display:grid;grid-template-columns:1fr 300px 100px;align-items:center;gap:16px;
  padding:14px 0;border-bottom:1px solid rgba(26,35,50,0.6);transition:background 0.15s;
}
.row:last-child{border-bottom:none}
.row:hover{background:rgba(34,214,102,0.02)}
.row-label{font-size:14px;font-weight:500;color:var(--text)}
.row-label .row-detail{font-family:'JetBrains Mono',monospace;font-size:11px;color:var(--dim);display:block;margin-top:3px}
.row-label .row-counts{font-family:'JetBrains Mono',monospace;font-size:11px;color:var(--dim)}

.duel{display:flex;flex-direction:column;gap:5px}
.duel-row{display:flex;align-items:center;gap:8px}
.duel-label{font-family:'JetBrains Mono',monospace;font-size:10px;font-weight:600;width:50px;text-align:right;flex-shrink:0}
.duel-label.oxi{color:var(--oxi)}
.duel-label.pg{color:#5b9bd5}
.duel-track{flex:1;height:18px;background:var(--bg);border-radius:3px;overflow:hidden;position:relative}
.duel-fill{height:100%;border-radius:3px;position:relative;transition:width 0.8s cubic-bezier(0.16,1,0.3,1)}
.duel-fill.oxi{background:linear-gradient(90deg,var(--oxi-dim),var(--oxi))}
.duel-fill.pg{background:linear-gradient(90deg,var(--pg-dim),#5b9bd5)}
.duel-fill.winner{box-shadow:0 0 12px rgba(34,214,102,0.3)}
.duel-fill.winner.pg{box-shadow:0 0 12px rgba(91,155,213,0.3)}
.duel-time{font-family:'JetBrains Mono',monospace;font-size:11px;font-weight:500;color:var(--dim);width:70px;flex-shrink:0}

.result{text-align:right}
.badge{display:inline-block;font-family:'JetBrains Mono',monospace;font-size:11px;font-weight:700;padding:3px 10px;border-radius:4px;letter-spacing:0.5px}
.badge-oxi{background:var(--oxi-dim);color:var(--oxi)}
.badge-pg{background:var(--pg-dim);color:#5b9bd5}
.badge-tie{background:var(--card);color:var(--dim)}
.speedup{font-family:'JetBrains Mono',monospace;font-size:11px;color:var(--accent);display:block;margin-top:3px}

.res-grid{display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-top:8px}
.res-card{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:24px;position:relative;overflow:hidden}
.res-card::before{content:'';position:absolute;top:0;left:0;right:0;height:2px}
.res-card.disk::before{background:linear-gradient(90deg,var(--oxi),#5b9bd5)}
.res-card.mem::before{background:linear-gradient(90deg,var(--accent),#5b9bd5)}
.res-title{font-family:'JetBrains Mono',monospace;font-size:11px;font-weight:600;color:var(--dim);letter-spacing:1.5px;text-transform:uppercase;margin-bottom:20px}
.res-bars{display:flex;flex-direction:column;gap:12px}
.res-row{display:flex;align-items:center;gap:12px}
.res-db{font-family:'JetBrains Mono',monospace;font-size:11px;font-weight:600;width:60px;flex-shrink:0}
.res-db.oxi{color:var(--oxi)}
.res-db.pg{color:#5b9bd5}
.res-track{flex:1;height:24px;background:var(--bg);border-radius:4px;overflow:hidden}
.res-fill{height:100%;border-radius:4px;display:flex;align-items:center;padding:0 10px;min-width:fit-content}
.res-fill.oxi{background:linear-gradient(90deg,var(--oxi-dim),rgba(34,214,102,0.35))}
.res-fill.pg{background:linear-gradient(90deg,var(--pg-dim),rgba(91,155,213,0.35))}
.res-val{font-family:'JetBrains Mono',monospace;font-size:11px;font-weight:600;color:var(--bright);white-space:nowrap}

footer{
  margin-top:56px;padding-top:24px;border-top:1px solid var(--border);
  display:flex;justify-content:space-between;align-items:center;
  font-family:'JetBrains Mono',monospace;font-size:11px;color:var(--dim);
}
footer .oxi-tag{color:var(--oxi)}

.err{color:var(--red);font-family:'JetBrains Mono',monospace;font-size:11px}

@keyframes grow{from{width:0}to{width:var(--w)}}
.duel-fill,.res-fill{animation:grow 0.8s cubic-bezier(0.16,1,0.3,1) forwards;width:0}

@media(max-width:768px){
  .score-strip{grid-template-columns:1fr;gap:16px;padding:24px}
  .score-vs{flex-direction:row;padding:8px 0}
  .score-vs::before,.score-vs::after{width:32px;height:1px}
  .row{grid-template-columns:1fr;gap:8px}
  .res-grid{grid-template-columns:1fr}
  header .title{font-size:28px}
  .score-num{font-size:48px}
}
</style>
</head>
<body>
<div class="wrap">

<header>
  <h1>benchmark report</h1>
  <div class="title">OxiDB vs PostgreSQL</div>
  <div class="meta">
    <span>` + fmt.Sprintf("%d", totalDocs/1000) + `K documents</span>
    <span>` + time.Now().Format("2006-01-02 15:04") + `</span>
    <span>` + fmt.Sprintf("%d tests", len(timings)) + `</span>
    <span>OxiWire binary protocol</span>
  </div>
</header>

<div class="methodology">
  A Go test binary (<strong>go test</strong>) drives both databases through their official client libraries.
  <span class="hl-oxi">OxiDB</span> and <span class="hl-pg">PostgreSQL 16</span> each run in isolated
  Docker containers with <strong>tmpfs</strong> storage (pure RAM — no disk I/O variance).
  PostgreSQL stores documents as <strong>JSONB</strong> in a single column with expression indexes
  and native JSONB operators (<strong>-&gt;&gt;</strong>, <strong>@&gt;</strong>).
  OxiDB uses its custom <strong>OxiWire</strong> binary protocol (fixed-size encoding, zero-copy strings); PostgreSQL uses its native wire protocol via <strong>pgx</strong>.
  Both have relaxed durability (fsync=off / lazy sync). Each test is a single cold run.
</div>

<div class="env-panel">
  <div class="env-title">&#9881; Test Environment</div>
  <div class="env-grid">
    <div class="env-item"><div class="env-label">CPU</div><div class="env-value">` + sysInfo.CPU + `</div></div>
    <div class="env-item"><div class="env-label">Cores</div><div class="env-value">` + fmt.Sprintf("%d", sysInfo.Cores) + `</div></div>
    <div class="env-item"><div class="env-label">Memory</div><div class="env-value">` + sysInfo.RAM + `</div></div>
    <div class="env-item"><div class="env-label">OS / Arch</div><div class="env-value">` + sysInfo.OS + `</div></div>
    <div class="env-item"><div class="env-label">Go</div><div class="env-value">` + sysInfo.GoVer + `</div></div>
    <div class="env-item"><div class="env-label">Docker</div><div class="env-value">` + sysInfo.Docker + `</div></div>
    <div class="env-item"><div class="env-label">PostgreSQL</div><div class="env-value">` + sysInfo.PgVer + `</div></div>
    <div class="env-item"><div class="env-label">Storage</div><div class="env-value">tmpfs (RAM-backed)</div></div>
  </div>
</div>

<div class="score-strip">
  <div class="score-side">
    <div class="db-label oxi">OxiDB</div>
    <div class="score-num oxi">` + fmt.Sprintf("%d", oxiWins) + `</div>
    <div class="score-sub">wins</div>
  </div>
  <div class="score-vs">VS</div>
  <div class="score-side">
    <div class="db-label pg">PostgreSQL</div>
    <div class="score-num pg">` + fmt.Sprintf("%d", pgWins) + `</div>
    <div class="score-sub">wins</div>
  </div>
</div>
`)

	for _, catName := range catOrder {
		cg := catMap[catName]
		icon := catIcon[catName]
		if icon == "" {
			icon = "&#9679;"
		}

		if catName == "Resources" {
			sb.WriteString(`<div class="cat">
<div class="cat-head">
  <div class="cat-icon">` + icon + `</div>
  <div class="cat-name">` + catName + `</div>
</div>
<div class="res-grid">
`)
			for _, e := range cg.Entries {
				cardClass := "disk"
				if strings.Contains(e.Name, "Memory") {
					cardClass = "mem"
				}
				oxiVal := e.OxiDetail
				pgVal := e.PgDetail
				if oxiVal == "" { oxiVal = "N/A" }
				if pgVal == "" { pgVal = "N/A" }
				oxiNum := parseSize(oxiVal)
				pgNum := parseSize(pgVal)
				maxNum := oxiNum
				if pgNum > maxNum { maxNum = pgNum }
				oxiPct, pgPct := 10.0, 10.0
				if maxNum > 0 {
					oxiPct = oxiNum / maxNum * 100
					pgPct = pgNum / maxNum * 100
				}

				sb.WriteString(fmt.Sprintf(`<div class="res-card %s">
  <div class="res-title">%s</div>
  <div class="res-bars">
    <div class="res-row">
      <div class="res-db oxi">OxiDB</div>
      <div class="res-track"><div class="res-fill oxi" style="--w:%.0f%%;width:0"><span class="res-val">%s</span></div></div>
    </div>
    <div class="res-row">
      <div class="res-db pg">PG</div>
      <div class="res-track"><div class="res-fill pg" style="--w:%.0f%%;width:0"><span class="res-val">%s</span></div></div>
    </div>
  </div>
</div>
`, cardClass, e.Name, oxiPct, oxiVal, pgPct, pgVal))
			}
			sb.WriteString("</div>\n</div>\n")
			continue
		}

		sb.WriteString(`<div class="cat">
<div class="cat-head">
  <div class="cat-icon">` + icon + `</div>
  <div class="cat-name">` + catName + `</div>
  <div class="cat-count">` + fmt.Sprintf("%d tests", len(cg.Entries)) + `</div>
</div>
`)

		for _, e := range cg.Entries {
			oxiStr := fmtDur(e.OxiDur)
			pgStr := fmtDur(e.PgDur)

			maxDur := e.OxiDur
			if e.PgDur > maxDur { maxDur = e.PgDur }
			oxiPct, pgPct := 0.0, 0.0
			if maxDur > 0 {
				oxiPct = float64(e.OxiDur) / float64(maxDur) * 100
				pgPct = float64(e.PgDur) / float64(maxDur) * 100
			}

			winBadge := `<span class="badge badge-tie">TIE</span>`
			speedupHTML := ""
			oxiWinner, pgWinner := "", ""
			if e.PgDur > 0 && e.OxiDur > 0 {
				if e.OxiDur < e.PgDur {
					winBadge = `<span class="badge badge-oxi">OxiDB</span>`
					factor := float64(e.PgDur) / float64(e.OxiDur)
					speedupHTML = fmt.Sprintf(`<span class="speedup">%.1fx faster</span>`, factor)
					oxiWinner = " winner"
				} else if e.PgDur < e.OxiDur {
					winBadge = `<span class="badge badge-pg">PostgreSQL</span>`
					factor := float64(e.OxiDur) / float64(e.PgDur)
					speedupHTML = fmt.Sprintf(`<span class="speedup">%.1fx faster</span>`, factor)
					pgWinner = " winner"
				}
			}

			if e.OxiErr != "" {
				winBadge = `<span class="err">` + e.OxiErr + `</span>`
			}

			detailHTML := ""
			if e.OxiDetail != "" || e.PgDetail != "" {
				detailHTML = fmt.Sprintf(`<span class="row-detail">%s vs %s</span>`, e.OxiDetail, e.PgDetail)
			}
			if e.OxiCount > 0 || e.PgCount > 0 {
				detailHTML += fmt.Sprintf(` <span class="row-counts">%d / %d docs</span>`, e.OxiCount, e.PgCount)
			}

			sb.WriteString(fmt.Sprintf(`<div class="row">
  <div class="row-label">%s%s</div>
  <div class="duel">
    <div class="duel-row">
      <div class="duel-label oxi">OxiDB</div>
      <div class="duel-track"><div class="duel-fill oxi%s" style="--w:%.0f%%;width:0"></div></div>
      <div class="duel-time">%s</div>
    </div>
    <div class="duel-row">
      <div class="duel-label pg">PG</div>
      <div class="duel-track"><div class="duel-fill pg%s" style="--w:%.0f%%;width:0"></div></div>
      <div class="duel-time">%s</div>
    </div>
  </div>
  <div class="result">%s%s</div>
</div>
`, e.Name, detailHTML, oxiWinner, oxiPct, oxiStr, pgWinner, pgPct, pgStr, winBadge, speedupHTML))
		}
		sb.WriteString("</div>\n")
	}

	sb.WriteString(fmt.Sprintf(`
<footer>
  <span><span class="oxi-tag">OxiDB</span> benchmark suite</span>
  <span>%d tests &middot; %d categories</span>
</footer>

</div>
</body>
</html>`, len(timings), len(catOrder)))

	reportPath := "report.html"
	if err := os.WriteFile(reportPath, []byte(sb.String()), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to write HTML report: %v\n", err)
		return
	}
	fmt.Printf("\n══════════════════════════════════════════════════════════════\n")
	fmt.Printf("  HTML Report: %s\n", reportPath)
	fmt.Printf("  OxiDB wins: %d | PostgreSQL wins: %d | Ties/N-A: %d\n", oxiWins, pgWins, countResourceEntries(timings))
	fmt.Printf("══════════════════════════════════════════════════════════════\n")

	// JSON summary
	summary := map[string]any{
		"total":    len(timings),
		"oxi_wins": oxiWins,
		"pg_wins":  pgWins,
		"timings":  timings,
	}
	jsonBytes, _ := json.MarshalIndent(summary, "", "  ")
	os.WriteFile("report.json", jsonBytes, 0644)
}

func parseSize(s string) float64 {
	s = strings.TrimSpace(s)
	for _, part := range strings.Fields(s) {
		num := ""
		for _, c := range part {
			if (c >= '0' && c <= '9') || c == '.' {
				num += string(c)
			} else if len(num) > 0 {
				break
			}
		}
		if num != "" {
			var f float64
			fmt.Sscanf(num, "%f", &f)
			return f
		}
	}
	return 0
}
