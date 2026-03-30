package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/parisxmas/OxiDB/go/oxidb"
)

var (
	host      = envOr("OXIDB_HOST", "127.0.0.1")
	port      = envInt("OXIDB_PORT", 14888)
	docCount  = envInt("DOC_COUNT", 1000000)
	batchSize = envInt("BATCH_SIZE", 5000)
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

var (
	regions  = []string{"US-EAST", "US-WEST", "EU-WEST", "EU-EAST", "APAC"}
	depts    = []string{"engineering", "sales", "marketing", "support", "hr", "finance", "ops", "legal"}
	statuses = []string{"active", "inactive", "onleave"}
	cities   = []string{"New York", "San Francisco", "London", "Berlin", "Tokyo", "Sydney", "Toronto", "Mumbai"}
)

func makeDoc(i int) map[string]any {
	rng := rand.New(rand.NewSource(int64(i)))
	return map[string]any{
		"emp_id":     fmt.Sprintf("E%07d", i),
		"name":       fmt.Sprintf("Employee %d", i),
		"email":      fmt.Sprintf("emp%d@company.com", i),
		"department": depts[rng.Intn(len(depts))],
		"region":     regions[rng.Intn(len(regions))],
		"city":       cities[rng.Intn(len(cities))],
		"status":     statuses[rng.Intn(len(statuses))],
		"salary":     30000 + rng.Intn(170000),
		"age":        22 + rng.Intn(43),
		"rating":     1.0 + rng.Float64()*4.0,
		"projects":   rng.Intn(20),
		"hired_year": 2010 + rng.Intn(16),
	}
}

func connect() *oxidb.Client {
	c, err := oxidb.Connect(host, port, 30*time.Second)
	if err != nil {
		fmt.Printf("FATAL: connect: %v\n", err)
		os.Exit(1)
	}
	return c
}

func formatDur(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%dµs", d.Microseconds())
	}
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	return d.Round(time.Millisecond).String()
}

func main() {
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Printf("║   OxiDB 1M Scale Test (%dk documents)                     ║\n", docCount/1000)
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Println()

	c := connect()
	defer c.Close()

	coll := "scale_1m"
	c.DropCollection(coll)
	time.Sleep(time.Second)

	// ═══ Phase 1: Insert 1M docs ═══
	fmt.Printf("═══ Phase 1: Insert %dk documents (batch %d) ═══\n", docCount/1000, batchSize)
	t0 := time.Now()
	for offset := 0; offset < docCount; offset += batchSize {
		end := offset + batchSize
		if end > docCount {
			end = docCount
		}
		batch := make([]map[string]any, 0, end-offset)
		for i := offset; i < end; i++ {
			batch = append(batch, makeDoc(i))
		}
		_, err := c.InsertMany(coll, batch)
		if err != nil {
			fmt.Printf("\n  Insert error at %d: %v\n", offset, err)
			break
		}
		if (offset/batchSize+1)%20 == 0 || end == docCount {
			fmt.Printf("\r  Inserting... %dk/%dk (%.1fs)", end/1000, docCount/1000, time.Since(t0).Seconds())
		}
	}
	insertDur := time.Since(t0)
	fmt.Printf("\r  Insert: %dk docs in %s (%.0f docs/sec)                    \n\n", docCount/1000, formatDur(insertDur), float64(docCount)/insertDur.Seconds())

	// ═══ Phase 2: Queries WITHOUT indexes ═══
	fmt.Println("═══ Phase 2: Queries WITHOUT indexes ═══")

	runQueries(c, coll, "no-index")

	// ═══ Phase 3: Create indexes ═══
	fmt.Println("═══ Phase 3: Create indexes ═══")
	indexFields := []string{"emp_id", "department", "region", "status", "salary", "age", "hired_year", "city"}
	t0 = time.Now()
	for _, f := range indexFields {
		ft := time.Now()
		c.CreateIndex(coll, f)
		fmt.Printf("  Index '%s': %s\n", f, formatDur(time.Since(ft)))
	}
	indexDur := time.Since(t0)
	fmt.Printf("  Total: %d indexes in %s\n\n", len(indexFields), formatDur(indexDur))

	// ═══ Phase 4: Queries WITH indexes ═══
	fmt.Println("═══ Phase 4: Queries WITH indexes ═══")

	runQueries(c, coll, "indexed")

	// ═══ Phase 5: Aggregation ═══
	fmt.Println("═══ Phase 5: Aggregation (1M docs) ═══")

	runAggregation(c, coll)

	// ═══ Phase 6: Updates & Deletes ═══
	fmt.Println("═══ Phase 6: Updates & Deletes ═══")

	runUpdates(c, coll)

	// ═══ Summary ═══
	fmt.Println()
	fmt.Println("═══════════════════════════════════════════════════════════════")
	n, _ := c.Count(coll, map[string]any{})
	fmt.Printf("Final document count: %d\n", n)
	fmt.Println("═══════════════════════════════════════════════════════════════")
}

func runQueries(c *oxidb.Client, coll, label string) {
	queries := []struct {
		name  string
		query map[string]any
		limit *int
	}{
		{"Exact match (emp_id)", map[string]any{"emp_id": "E0500000"}, nil},
		{"Equality (department=engineering)", map[string]any{"department": "engineering"}, intPtr(100)},
		{"Range (salary 100K-150K)", map[string]any{"salary": map[string]any{"$gte": 100000, "$lte": 150000}}, intPtr(100)},
		{"Multi-AND (dept+status+region)", map[string]any{"department": "hr", "status": "active", "region": "EU-WEST"}, nil},
		{"Unindexed scan (rating > 4.5)", map[string]any{"rating": map[string]any{"$gt": 4.5}}, intPtr(100)},
		{"find_one (emp_id)", map[string]any{"emp_id": "E0750000"}, intPtr(1)},
		{"Count (status=active)", map[string]any{"status": "active"}, nil},
		{"Sort+limit 10 (salary desc)", map[string]any{"department": "sales"}, intPtr(10)},
	}

	for _, q := range queries {
		t0 := time.Now()
		if q.name == "Count (status=active)" {
			n, _ := c.Count(coll, q.query)
			dur := time.Since(t0)
			fmt.Printf("  [%s] %-40s %8s  (count=%d)\n", label, q.name, formatDur(dur), n)
		} else if q.name == "find_one (emp_id)" {
			doc, _ := c.FindOne(coll, q.query)
			dur := time.Since(t0)
			found := doc != nil
			fmt.Printf("  [%s] %-40s %8s  (found=%v)\n", label, q.name, formatDur(dur), found)
		} else if q.name == "Sort+limit 10 (salary desc)" {
			opts := &oxidb.FindOptions{Sort: map[string]any{"salary": -1}, Limit: q.limit}
			res, _ := c.Find(coll, q.query, opts)
			dur := time.Since(t0)
			fmt.Printf("  [%s] %-40s %8s  (rows=%d)\n", label, q.name, formatDur(dur), len(res))
		} else {
			opts := &oxidb.FindOptions{}
			if q.limit != nil {
				opts.Limit = q.limit
			}
			res, _ := c.Find(coll, q.query, opts)
			dur := time.Since(t0)
			fmt.Printf("  [%s] %-40s %8s  (rows=%d)\n", label, q.name, formatDur(dur), len(res))
		}
	}
	fmt.Println()
}

func runAggregation(c *oxidb.Client, coll string) {
	// Group by department
	{
		pipeline := []map[string]any{
			{"$group": map[string]any{"_id": "$department", "count": map[string]any{"$sum": 1}, "avg_salary": map[string]any{"$avg": "$salary"}}},
			{"$sort": map[string]any{"count": -1}},
		}
		t0 := time.Now()
		res, _ := c.Aggregate(coll, pipeline)
		fmt.Printf("  Group by dept (count+avg):      %8s  (groups=%d)\n", formatDur(time.Since(t0)), len(res))
	}

	// Group by city with full stats
	{
		pipeline := []map[string]any{
			{"$group": map[string]any{
				"_id":        "$city",
				"count":      map[string]any{"$sum": 1},
				"avg_salary": map[string]any{"$avg": "$salary"},
				"max_salary": map[string]any{"$max": "$salary"},
				"min_salary": map[string]any{"$min": "$salary"},
			}},
			{"$sort": map[string]any{"avg_salary": -1}},
		}
		t0 := time.Now()
		res, _ := c.Aggregate(coll, pipeline)
		fmt.Printf("  Group by city (5 accum):        %8s  (groups=%d)\n", formatDur(time.Since(t0)), len(res))
	}

	// Match + group
	{
		pipeline := []map[string]any{
			{"$match": map[string]any{"region": "US-EAST"}},
			{"$group": map[string]any{"_id": "$department", "total": map[string]any{"$sum": "$salary"}, "headcount": map[string]any{"$sum": 1}}},
			{"$sort": map[string]any{"total": -1}},
		}
		t0 := time.Now()
		res, _ := c.Aggregate(coll, pipeline)
		fmt.Printf("  Match US-EAST + group dept:     %8s  (groups=%d)\n", formatDur(time.Since(t0)), len(res))
	}

	// Count pipeline
	{
		pipeline := []map[string]any{
			{"$match": map[string]any{"department": "engineering", "status": "active"}},
			{"$count": "total"},
		}
		t0 := time.Now()
		res, _ := c.Aggregate(coll, pipeline)
		count := 0
		if len(res) > 0 {
			if v, ok := res[0]["total"].(float64); ok {
				count = int(v)
			}
		}
		fmt.Printf("  Count pipeline (eng+active):    %8s  (count=%d)\n", formatDur(time.Since(t0)), count)
	}
	fmt.Println()
}

func runUpdates(c *oxidb.Client, coll string) {
	// UpdateOne
	{
		t0 := time.Now()
		c.UpdateOne(coll, map[string]any{"emp_id": "E0000001"}, map[string]any{"$set": map[string]any{"status": "onleave"}})
		fmt.Printf("  UpdateOne (indexed):             %8s\n", formatDur(time.Since(t0)))
	}

	// UpdateMany
	{
		t0 := time.Now()
		res, _ := c.Update(coll, map[string]any{"department": "hr", "status": "inactive"}, map[string]any{"$set": map[string]any{"status": "active"}})
		n := 0
		if res != nil {
			if v, ok := res["modified"].(float64); ok {
				n = int(v)
			}
		}
		fmt.Printf("  UpdateMany (hr+inactive):       %8s  (modified=%d)\n", formatDur(time.Since(t0)), n)
	}

	// DeleteMany
	{
		t0 := time.Now()
		res, _ := c.Delete(coll, map[string]any{"status": "onleave"})
		n := 0
		if res != nil {
			if v, ok := res["deleted"].(float64); ok {
				n = int(v)
			}
		}
		fmt.Printf("  DeleteMany (onleave):           %8s  (deleted=%d)\n", formatDur(time.Since(t0)), n)
	}
	fmt.Println()
}

func intPtr(n int) *int { return &n }
