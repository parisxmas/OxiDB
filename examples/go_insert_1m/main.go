package main

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/parisxmas/OxiDB/go/oxidb"
)


const (
	host       = "127.0.0.1"
	port       = 4444
	collection = "bench_1m"
	totalDocs  = 1_000_000
	batchSize  = 5000
)

var (
	firstNames = []string{"Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hank", "Ivy", "Jack", "Karen", "Leo", "Mona", "Nick", "Olivia", "Paul", "Quinn", "Rosa", "Sam", "Tina"}
	lastNames  = []string{"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez", "Wilson", "Anderson", "Taylor", "Thomas", "Moore", "Jackson", "Martin", "Lee", "Harris", "Clark"}
	cities     = []string{"New York", "London", "Tokyo", "Paris", "Berlin", "Sydney", "Toronto", "Mumbai", "Dubai", "Singapore", "Amsterdam", "Seoul", "Istanbul", "Bangkok", "Moscow", "Lagos", "Cairo", "Lima", "Rome", "Vienna"}
	countries  = []string{"US", "UK", "JP", "FR", "DE", "AU", "CA", "IN", "AE", "SG", "NL", "KR", "TR", "TH", "RU", "NG", "EG", "PE", "IT", "AT"}
	depts      = []string{"Engineering", "Sales", "Marketing", "Finance", "HR", "Legal", "Operations", "Support", "Research", "Design"}
	statuses   = []string{"active", "inactive", "suspended", "pending", "archived"}
	allTags    = []string{"vip", "premium", "trial", "enterprise", "startup", "partner", "internal", "external", "beta", "legacy"}
)

func randomDate(rng *rand.Rand, minYear, maxYear int) string {
	year := minYear + rng.Intn(maxYear-minYear+1)
	month := 1 + rng.Intn(12)
	day := 1 + rng.Intn(28)
	return fmt.Sprintf("%04d-%02d-%02d", year, month, day)
}

func randomTags(rng *rand.Rand) []any {
	n := 1 + rng.Intn(4)
	picked := make([]any, n)
	for i := 0; i < n; i++ {
		picked[i] = allTags[rng.Intn(len(allTags))]
	}
	return picked
}

func generateDoc(rng *rand.Rand, i int) map[string]any {
	first := firstNames[rng.Intn(len(firstNames))]
	last := lastNames[rng.Intn(len(lastNames))]
	city := cities[rng.Intn(len(cities))]
	country := countries[rng.Intn(len(countries))]

	return map[string]any{
		"seq":        i,
		"name":       first + " " + last,
		"email":      fmt.Sprintf("%s.%s.%d@example.com", strings.ToLower(first), strings.ToLower(last), i),
		"age":        18 + rng.Intn(62),                   // int 18-79
		"salary":     30000.0 + rng.Float64()*170000.0,    // float 30k-200k
		"department": depts[rng.Intn(len(depts))],         // string enum
		"city":       city,                                 // string enum
		"country":    country,                              // string enum
		"status":     statuses[rng.Intn(len(statuses))],   // string enum
		"score":      rng.Float64() * 100.0,               // float 0-100
		"verified":   rng.Intn(2) == 1,                    // bool
		"birthDate":  randomDate(rng, 1960, 2005),         // date string
		"hireDate":   randomDate(rng, 2010, 2025),         // date string
		"tags":       randomTags(rng),                     // array of strings
		"rating":     rng.Intn(5) + 1,                    // int 1-5
		"address": map[string]any{                         // nested object
			"street": fmt.Sprintf("%d %s St", 100+rng.Intn(9900), lastNames[rng.Intn(len(lastNames))]),
			"zip":    fmt.Sprintf("%05d", rng.Intn(100000)),
		},
	}
}

func main() {
	fmt.Println("╔══════════════════════════════════════════╗")
	fmt.Println("║     OxiDB 1M Document Benchmark          ║")
	fmt.Println("╚══════════════════════════════════════════╝")
	fmt.Printf("Server:     %s:%d\n", host, port)
	fmt.Printf("Collection: %s\n", collection)
	fmt.Printf("Documents:  %dk\n", totalDocs/1000)
	fmt.Printf("Batch size: %d\n\n", batchSize)

	client, err := oxidb.Connect(host, port, 30*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: connect failed: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	if _, err := client.Ping(); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: ping failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Connected. Ping OK.")

	// Drop old data
	_ = client.DropCollection(collection)
	fmt.Println("Old collection dropped.\n")

	// ==================================================================
	// PHASE 1: INSERT
	// ==================================================================
	fmt.Println("━━━ PHASE 1: INSERT ━━━━━━━━━━━━━━━━━━━━━━")
	rng := rand.New(rand.NewSource(42))

	insertStart := time.Now()
	inserted := 0
	lastReport := time.Now()

	for inserted < totalDocs {
		remaining := totalDocs - inserted
		n := batchSize
		if remaining < n {
			n = remaining
		}

		batch := make([]map[string]any, n)
		for j := 0; j < n; j++ {
			batch[j] = generateDoc(rng, inserted+j)
		}

		_, err := client.InsertMany(collection, batch)
		if err != nil {
			fmt.Fprintf(os.Stderr, "FATAL: insert_many at %d: %v\n", inserted, err)
			os.Exit(1)
		}
		inserted += n

		if time.Since(lastReport) >= 3*time.Second || inserted == totalDocs {
			elapsed := time.Since(insertStart)
			rate := float64(inserted) / elapsed.Seconds()
			pct := float64(inserted) / float64(totalDocs) * 100
			fmt.Printf("  %7d / %d  (%5.1f%%)  %8.0f docs/s  %s\n",
				inserted, totalDocs, pct, rate, elapsed.Round(time.Millisecond))
			lastReport = time.Now()
		}
	}

	insertElapsed := time.Since(insertStart)
	insertRate := float64(totalDocs) / insertElapsed.Seconds()

	fmt.Println()
	fmt.Println("┌─────────────────────────────────────────┐")
	fmt.Println("│           INSERT REPORT                 │")
	fmt.Println("├─────────────────────────────────────────┤")
	fmt.Printf("│  Documents:  %-27d│\n", totalDocs)
	fmt.Printf("│  Time:       %-27s│\n", insertElapsed.Round(time.Millisecond))
	fmt.Printf("│  Throughput: %-23.0f docs/s│\n", insertRate)
	fmt.Println("└─────────────────────────────────────────┘")

	// ==================================================================
	// PHASE 2: CREATE INDEXES
	// ==================================================================
	fmt.Println("\n━━━ PHASE 2: CREATE INDEXES ━━━━━━━━━━━━━━")

	type idxDef struct {
		label string
		fn    func() error
	}

	indexDefs := []idxDef{
		{"email (unique)", func() error { return client.CreateUniqueIndex(collection, "email") }},
		{"age", func() error { return client.CreateIndex(collection, "age") }},
		{"department", func() error { return client.CreateIndex(collection, "department") }},
		{"city", func() error { return client.CreateIndex(collection, "city") }},
		{"country", func() error { return client.CreateIndex(collection, "country") }},
		{"status", func() error { return client.CreateIndex(collection, "status") }},
		{"salary", func() error { return client.CreateIndex(collection, "salary") }},
		{"score", func() error { return client.CreateIndex(collection, "score") }},
		{"verified", func() error { return client.CreateIndex(collection, "verified") }},
		{"rating", func() error { return client.CreateIndex(collection, "rating") }},
		{"birthDate", func() error { return client.CreateIndex(collection, "birthDate") }},
		{"hireDate", func() error { return client.CreateIndex(collection, "hireDate") }},
		{"[department,city] composite", func() error {
			return client.CreateCompositeIndex(collection, []string{"department", "city"})
		}},
		{"[country,status] composite", func() error {
			return client.CreateCompositeIndex(collection, []string{"country", "status"})
		}},
	}

	indexStart := time.Now()
	allOK := true
	for _, idx := range indexDefs {
		t0 := time.Now()
		if err := idx.fn(); err != nil {
			fmt.Printf("  ✗ %-35s FAILED  %v\n", idx.label, err)
			allOK = false
		} else {
			fmt.Printf("  ✓ %-35s %s\n", idx.label, time.Since(t0).Round(time.Millisecond))
		}
	}
	indexElapsed := time.Since(indexStart)
	fmt.Printf("\n  Total: %d indexes in %s\n", len(indexDefs), indexElapsed.Round(time.Millisecond))

	// Verify
	fmt.Println("\n━━━ PHASE 2b: VERIFY INDEXES ━━━━━━━━━━━━━")
	idxList, err := client.ListIndexes(collection)
	if err != nil {
		fmt.Printf("  ERROR: %v\n", err)
	} else {
		fmt.Printf("  Server reports %d indexes:\n", len(idxList))
		for _, ix := range idxList {
			tp, _ := ix["type"].(string)
			fields := ix["fields"]
			if fields == nil {
				fields = ix["field"]
			}
			unique, _ := ix["unique"].(bool)
			u := ""
			if unique {
				u = " (unique)"
			}
			fmt.Printf("    %-12s %v%s\n", tp, fields, u)
		}
		if allOK && len(idxList) >= len(indexDefs) {
			fmt.Println("  All indexes verified OK.")
		}
	}

	// ==================================================================
	// PHASE 3: QUERIES
	// ==================================================================
	fmt.Println("\n━━━ PHASE 3: QUERIES ━━━━━━━━━━━━━━━━━━━━━")

	limit10 := 10
	limit50 := 50

	type queryCase struct {
		label string
		run   func() (int, error)
	}

	queryTests := []queryCase{
		{"Exact: department=Engineering", func() (int, error) {
			docs, err := client.Find(collection, map[string]any{"department": "Engineering"}, &oxidb.FindOptions{Limit: &limit10})
			return len(docs), err
		}},
		{"Exact: status=active AND country=US", func() (int, error) {
			docs, err := client.Find(collection, map[string]any{"status": "active", "country": "US"}, &oxidb.FindOptions{Limit: &limit10})
			return len(docs), err
		}},
		{"Range: age 25-35", func() (int, error) {
			docs, err := client.Find(collection, map[string]any{"age": map[string]any{"$gte": 25, "$lte": 35}}, &oxidb.FindOptions{Limit: &limit10})
			return len(docs), err
		}},
		{"Range: salary > 150000", func() (int, error) {
			docs, err := client.Find(collection, map[string]any{"salary": map[string]any{"$gt": 150000}}, &oxidb.FindOptions{Limit: &limit10})
			return len(docs), err
		}},
		{"Bool: verified=true (limit 50)", func() (int, error) {
			docs, err := client.Find(collection, map[string]any{"verified": true}, &oxidb.FindOptions{Limit: &limit50})
			return len(docs), err
		}},
		{"Sort: score DESC, limit 10", func() (int, error) {
			docs, err := client.Find(collection, map[string]any{}, &oxidb.FindOptions{Sort: map[string]any{"score": -1}, Limit: &limit10})
			return len(docs), err
		}},
		{"Sort: salary ASC, limit 10", func() (int, error) {
			docs, err := client.Find(collection, map[string]any{}, &oxidb.FindOptions{Sort: map[string]any{"salary": 1}, Limit: &limit10})
			return len(docs), err
		}},
		{"Date range: birthDate 1990-2000", func() (int, error) {
			docs, err := client.Find(collection, map[string]any{"birthDate": map[string]any{"$gte": "1990-01-01", "$lte": "2000-12-31"}}, &oxidb.FindOptions{Limit: &limit10})
			return len(docs), err
		}},
		{"$or: city=Tokyo OR city=Paris", func() (int, error) {
			docs, err := client.Find(collection, map[string]any{"$or": []any{map[string]any{"city": "Tokyo"}, map[string]any{"city": "Paris"}}}, &oxidb.FindOptions{Limit: &limit10})
			return len(docs), err
		}},
		{"$in: rating in [4,5]", func() (int, error) {
			docs, err := client.Find(collection, map[string]any{"rating": map[string]any{"$in": []any{4, 5}}}, &oxidb.FindOptions{Limit: &limit10})
			return len(docs), err
		}},
		{"Nested: address.zip starts with 0", func() (int, error) {
			docs, err := client.Find(collection, map[string]any{"address.zip": map[string]any{"$regex": "^0"}}, &oxidb.FindOptions{Limit: &limit10})
			return len(docs), err
		}},
		{"$regex: name starts with A", func() (int, error) {
			docs, err := client.Find(collection, map[string]any{"name": map[string]any{"$regex": "^A"}}, &oxidb.FindOptions{Limit: &limit10})
			return len(docs), err
		}},
		{"$exists: tags field exists", func() (int, error) {
			docs, err := client.Find(collection, map[string]any{"tags": map[string]any{"$exists": true}}, &oxidb.FindOptions{Limit: &limit10})
			return len(docs), err
		}},
		{"$ne: status != active", func() (int, error) {
			docs, err := client.Find(collection, map[string]any{"status": map[string]any{"$ne": "active"}}, &oxidb.FindOptions{Limit: &limit10})
			return len(docs), err
		}},
		{"Count: department=Sales", func() (int, error) {
			n, err := client.Count(collection, map[string]any{"department": "Sales"})
			return n, err
		}},
		{"Count: verified=true", func() (int, error) {
			n, err := client.Count(collection, map[string]any{"verified": true})
			return n, err
		}},
		{"FindOne: seq=500000", func() (int, error) {
			doc, err := client.FindOne(collection, map[string]any{"seq": 500000})
			if doc != nil {
				return 1, err
			}
			return 0, err
		}},
		{"FindOne: seq=999999", func() (int, error) {
			doc, err := client.FindOne(collection, map[string]any{"seq": 999999})
			if doc != nil {
				return 1, err
			}
			return 0, err
		}},
	}

	fmt.Printf("\n  %-45s %12s %8s\n", "Query", "Time", "Results")
	fmt.Println("  " + strings.Repeat("─", 68))

	var totalQueryTime time.Duration
	queryErrors := 0

	for _, q := range queryTests {
		t0 := time.Now()
		count, err := q.run()
		elapsed := time.Since(t0)
		totalQueryTime += elapsed
		if err != nil {
			fmt.Printf("  %-45s %12s  ERROR: %v\n", q.label, elapsed.Round(time.Microsecond), err)
			queryErrors++
		} else {
			fmt.Printf("  %-45s %12s %8d\n", q.label, elapsed.Round(time.Microsecond), count)
		}
	}

	fmt.Println("  " + strings.Repeat("─", 68))
	fmt.Printf("  %-45s %12s\n", "Total", totalQueryTime.Round(time.Microsecond))
	fmt.Printf("  %-45s %12s\n", "Average", (totalQueryTime/time.Duration(len(queryTests))).Round(time.Microsecond))

	// ==================================================================
	// PHASE 4: AGGREGATION
	// ==================================================================
	fmt.Println("\n━━━ PHASE 4: AGGREGATION ━━━━━━━━━━━━━━━━━")

	type aggCase struct {
		label    string
		pipeline []map[string]any
	}

	aggTests := []aggCase{
		{"Count by department", []map[string]any{
			{"$group": map[string]any{"_id": "$department", "count": map[string]any{"$sum": 1}}},
			{"$sort": map[string]any{"count": -1}},
		}},
		{"Avg salary by department", []map[string]any{
			{"$group": map[string]any{"_id": "$department", "avg": map[string]any{"$avg": "$salary"}}},
			{"$sort": map[string]any{"avg": -1}},
		}},
		{"Count by country, top 5", []map[string]any{
			{"$group": map[string]any{"_id": "$country", "count": map[string]any{"$sum": 1}}},
			{"$sort": map[string]any{"count": -1}},
			{"$limit": 5},
		}},
		{"Active users, avg score by city top 5", []map[string]any{
			{"$match": map[string]any{"status": "active"}},
			{"$group": map[string]any{"_id": "$city", "avgScore": map[string]any{"$avg": "$score"}, "count": map[string]any{"$sum": 1}}},
			{"$sort": map[string]any{"avgScore": -1}},
			{"$limit": 5},
		}},
		{"Min/Max salary by department", []map[string]any{
			{"$group": map[string]any{"_id": "$department", "min": map[string]any{"$min": "$salary"}, "max": map[string]any{"$max": "$salary"}}},
			{"$sort": map[string]any{"max": -1}},
		}},
		{"Count by rating", []map[string]any{
			{"$group": map[string]any{"_id": "$rating", "count": map[string]any{"$sum": 1}}},
			{"$sort": map[string]any{"_id": 1}},
		}},
		{"Count by status", []map[string]any{
			{"$group": map[string]any{"_id": "$status", "count": map[string]any{"$sum": 1}}},
			{"$sort": map[string]any{"count": -1}},
		}},
	}

	fmt.Printf("\n  %-45s %12s %8s\n", "Pipeline", "Time", "Results")
	fmt.Println("  " + strings.Repeat("─", 68))

	var totalAggTime time.Duration

	for _, a := range aggTests {
		t0 := time.Now()
		docs, err := client.Aggregate(collection, a.pipeline)
		elapsed := time.Since(t0)
		totalAggTime += elapsed
		if err != nil {
			fmt.Printf("  %-45s %12s  ERROR: %v\n", a.label, elapsed.Round(time.Microsecond), err)
		} else {
			fmt.Printf("  %-45s %12s %8d\n", a.label, elapsed.Round(time.Microsecond), len(docs))
		}
	}

	fmt.Println("  " + strings.Repeat("─", 68))
	fmt.Printf("  %-45s %12s\n", "Total", totalAggTime.Round(time.Microsecond))
	fmt.Printf("  %-45s %12s\n", "Average", (totalAggTime/time.Duration(len(aggTests))).Round(time.Microsecond))

	// ==================================================================
	// FINAL REPORT
	// ==================================================================
	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════╗")
	fmt.Println("║           FINAL REPORT                   ║")
	fmt.Println("╠══════════════════════════════════════════╣")
	fmt.Printf("║  Insert: %d docs in %-18s ║\n", totalDocs, insertElapsed.Round(time.Millisecond))
	fmt.Printf("║  Insert throughput: %-17.0f docs/s║\n", insertRate)
	fmt.Printf("║  Indexes: %-12d in %-15s ║\n", len(indexDefs), indexElapsed.Round(time.Millisecond))
	fmt.Printf("║  Queries: %-3d        total %-13s ║\n", len(queryTests), totalQueryTime.Round(time.Microsecond))
	fmt.Printf("║  Query avg:          %-18s ║\n", (totalQueryTime/time.Duration(len(queryTests))).Round(time.Microsecond))
	fmt.Printf("║  Aggregations: %-3d   total %-13s ║\n", len(aggTests), totalAggTime.Round(time.Microsecond))
	fmt.Printf("║  Agg avg:            %-18s ║\n", (totalAggTime/time.Duration(len(aggTests))).Round(time.Microsecond))
	if queryErrors > 0 {
		fmt.Printf("║  Query errors: %-24d ║\n", queryErrors)
	}
	fmt.Println("╚══════════════════════════════════════════╝")
}
