package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/parisxmas/OxiDB/go/oxidb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// getContainerMemory returns RSS memory of a container in MB
func getContainerMemory(containerName string) string {
	out, err := exec.Command("sh", "-c",
		fmt.Sprintf("cat /sys/fs/cgroup/memory.current 2>/dev/null || cat /sys/fs/cgroup/memory/memory.usage_in_bytes 2>/dev/null || echo 0")).Output()
	if err != nil {
		// Try docker stats from outside
		return "?"
	}
	bytes, _ := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if bytes > 0 {
		mb := float64(bytes) / 1024 / 1024
		return fmt.Sprintf("%.1f MB", mb)
	}
	return "?"
}

// getOxiDBMemory gets OxiDB memory via /proc or ps
func getProcessMemory(name string, host string, port int) string {
	// Use docker stats API — bench container can't see other containers' /proc
	// Instead, call a simple endpoint or estimate from connection
	return "?"
}

// ─── Config ────────────────────────────────────────────────────────

var (
	oxidbHost = envOr("OXIDB_HOST", "127.0.0.1")
	oxidbPort = envInt("OXIDB_PORT", 4444)
	mongoURI  = envOr("MONGO_URI", "mongodb://127.0.0.1:27017")
	docCount  = envInt("DOC_COUNT", 1_000_000)
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

// ─── Result tracking ───────────────────────────────────────────────

type Result struct {
	Category string        `json:"category"`
	Name     string        `json:"name"`
	OxiDB    time.Duration `json:"oxidb_ms"`
	MongoDB  time.Duration `json:"mongodb_ms"`
	OxiCount int           `json:"oxidb_count"`
	MonCount int           `json:"mongodb_count"`
	OxiErr   string        `json:"oxidb_error,omitempty"`
	MonErr   string        `json:"mongodb_error,omitempty"`
}

var (
	results []Result
	resMu   sync.Mutex
)

func record(cat, name string, oxiDur, monDur time.Duration, oxiN, monN int, oxiErr, monErr error) {
	r := Result{
		Category: cat,
		Name:     name,
		OxiDB:    oxiDur,
		MongoDB:  monDur,
		OxiCount: oxiN,
		MonCount: monN,
	}
	if oxiErr != nil {
		r.OxiErr = oxiErr.Error()
	}
	if monErr != nil {
		r.MonErr = monErr.Error()
	}
	resMu.Lock()
	results = append(results, r)
	resMu.Unlock()

	ratio := ""
	if monDur > 0 && oxiDur > 0 {
		r := float64(oxiDur) / float64(monDur)
		if r < 1 {
			ratio = fmt.Sprintf("  OxiDB %.1fx faster", 1/r)
		} else {
			ratio = fmt.Sprintf("  MongoDB %.1fx faster", r)
		}
	}
	fmt.Printf("  %-35s OxiDB: %8s  MongoDB: %8s%s\n",
		name,
		formatDur(oxiDur),
		formatDur(monDur),
		ratio)
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

// ─── Data generators ───────────────────────────────────────────────

var (
	regions    = []string{"US-EAST", "US-WEST", "EU-WEST", "EU-EAST", "APAC"}
	depts      = []string{"engineering", "sales", "marketing", "support", "hr", "finance", "ops", "legal"}
	statuses   = []string{"active", "inactive", "onleave"}
	cities     = []string{"New York", "San Francisco", "London", "Berlin", "Tokyo", "Sydney", "Toronto", "Mumbai"}
)

func makeDoc(i int) map[string]any {
	return map[string]any{
		"emp_id":     fmt.Sprintf("E%06d", i),
		"name":       fmt.Sprintf("Employee %d", i),
		"email":      fmt.Sprintf("emp%d@company.com", i),
		"department": depts[rand.Intn(len(depts))],
		"region":     regions[rand.Intn(len(regions))],
		"city":       cities[rand.Intn(len(cities))],
		"status":     statuses[rand.Intn(len(statuses))],
		"salary":     30000 + rand.Intn(170000),
		"age":        22 + rand.Intn(43),
		"rating":     1.0 + rand.Float64()*4.0,
		"projects":   rand.Intn(20),
		"hired_year": 2010 + rand.Intn(16),
	}
}

func makeBson(i int) bson.M {
	d := makeDoc(i)
	b := bson.M{}
	for k, v := range d {
		b[k] = v
	}
	return b
}

// ─── Main ──────────────────────────────────────────────────────────

func main() {
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║        OxiDB vs MongoDB Benchmark — 1M Documents           ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Println()

	ctx := context.Background()

	// Connect to OxiDB
	fmt.Printf("Connecting to OxiDB (%s:%d)...\n", oxidbHost, oxidbPort)
	oxi, err := oxidb.Connect(oxidbHost, oxidbPort, 30*time.Second)
	if err != nil {
		fmt.Printf("FATAL: OxiDB connect: %v\n", err)
		os.Exit(1)
	}
	defer oxi.Close()
	oxi.Ping()
	fmt.Println("  OxiDB connected.")

	// Connect to MongoDB
	fmt.Printf("Connecting to MongoDB (%s)...\n", mongoURI)
	mopts := options.Client().ApplyURI(mongoURI)
	mc, err := mongo.Connect(ctx, mopts)
	if err != nil {
		fmt.Printf("FATAL: MongoDB connect: %v\n", err)
		os.Exit(1)
	}
	defer mc.Disconnect(ctx)
	mc.Ping(ctx, nil)
	fmt.Println("  MongoDB connected.")
	fmt.Println()

	mdb := mc.Database("bench")
	coll := "employees"
	mcoll := mdb.Collection(coll)

	// ════════════════════════════════════════════════════════════════
	// Phase 1: Insert
	// ════════════════════════════════════════════════════════════════

	fmt.Printf("═══ Phase 1: Insert %d documents (batch %d) ═══\n", docCount, batchSize)

	oxi.DropCollection(coll)
	mdb.Collection(coll).Drop(ctx)

	// OxiDB insert
	fmt.Printf("  OxiDB inserting...")
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
		if _, err := oxi.InsertMany(coll, batch); err != nil {
			fmt.Printf("\n  OxiDB insert error at offset %d: %v\n", offset, err)
			break
		}
		if (offset/batchSize+1)%20 == 0 || end == docCount {
			fmt.Printf("\r  OxiDB inserting... %dk/%dk (%.1fs)", end/1000, docCount/1000, time.Since(t0).Seconds())
		}
	}
	oxiInsert := time.Since(t0)
	fmt.Printf("\r  OxiDB inserted %dk in %s                    \n", docCount/1000, oxiInsert.Round(time.Millisecond))

	// MongoDB insert
	fmt.Printf("  MongoDB inserting...")
	t0 = time.Now()
	for offset := 0; offset < docCount; offset += batchSize {
		end := offset + batchSize
		if end > docCount {
			end = docCount
		}
		batch := make([]interface{}, 0, end-offset)
		for i := offset; i < end; i++ {
			batch = append(batch, makeBson(i))
		}
		if _, err := mcoll.InsertMany(ctx, batch); err != nil {
			fmt.Printf("\n  MongoDB insert error at offset %d: %v\n", offset, err)
			break
		}
		if (offset/batchSize+1)%20 == 0 || end == docCount {
			fmt.Printf("\r  MongoDB inserting... %dk/%dk (%.1fs)", end/1000, docCount/1000, time.Since(t0).Seconds())
		}
	}
	monInsert := time.Since(t0)
	fmt.Printf("\r  MongoDB inserted %dk in %s                    \n", docCount/1000, monInsert.Round(time.Millisecond))

	record("INSERT", fmt.Sprintf("%d docs (batch %d)", docCount, batchSize), oxiInsert, monInsert, docCount, docCount, nil, nil)
	fmt.Println()

	// ════════════════════════════════════════════════════════════════
	// Phase 2: Create indexes
	// ════════════════════════════════════════════════════════════════

	fmt.Println("═══ Phase 2: Create indexes ═══")

	indexFields := []string{"emp_id", "department", "region", "status", "salary", "age", "hired_year", "city"}

	t0 = time.Now()
	for _, f := range indexFields {
		oxi.CreateIndex(coll, f)
	}
	oxiIdx := time.Since(t0)

	t0 = time.Now()
	for _, f := range indexFields {
		mcoll.Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys: bson.D{{Key: f, Value: 1}},
		})
	}
	monIdx := time.Since(t0)

	record("INDEX", fmt.Sprintf("%d indexes", len(indexFields)), oxiIdx, monIdx, len(indexFields), len(indexFields), nil, nil)
	fmt.Println()

	// ════════════════════════════════════════════════════════════════
	// Phase 3: Queries
	// ════════════════════════════════════════════════════════════════

	fmt.Println("═══ Phase 3: Queries ═══")

	// 3a. Exact match (indexed)
	runQuery := func(name string, oxiQ map[string]any, monQ bson.M) {
		t0 = time.Now()
		oxiRes, oxiErr := oxi.Find(coll, oxiQ, &oxidb.FindOptions{})
		oxiDur := time.Since(t0)

		t0 = time.Now()
		cur, monErr := mcoll.Find(ctx, monQ)
		var monRes []bson.M
		if monErr == nil {
			monErr = cur.All(ctx, &monRes)
		}
		monDur := time.Since(t0)

		record("QUERY", name, oxiDur, monDur, len(oxiRes), len(monRes), oxiErr, monErr)
	}

	runQuery("Exact match (indexed)",
		map[string]any{"emp_id": "E000042"},
		bson.M{"emp_id": "E000042"})

	runQuery("Equality (indexed)",
		map[string]any{"department": "engineering"},
		bson.M{"department": "engineering"})

	runQuery("Range (indexed)",
		map[string]any{"salary": map[string]any{"$gte": 100000, "$lte": 150000}},
		bson.M{"salary": bson.M{"$gte": 100000, "$lte": 150000}})

	runQuery("Range + equality",
		map[string]any{"department": "sales", "age": map[string]any{"$gte": 30, "$lte": 40}},
		bson.M{"department": "sales", "age": bson.M{"$gte": 30, "$lte": 40}})

	runQuery("Multi-condition AND",
		map[string]any{"region": "US-EAST", "status": "active", "hired_year": map[string]any{"$gte": 2020}},
		bson.M{"region": "US-EAST", "status": "active", "hired_year": bson.M{"$gte": 2020}})

	runQuery("Unindexed scan (rating)",
		map[string]any{"rating": map[string]any{"$gte": 4.5}},
		bson.M{"rating": bson.M{"$gte": 4.5}})

	// 3b. find_one
	{
		t0 = time.Now()
		_, oxiErr := oxi.FindOne(coll, map[string]any{"emp_id": "E500000"})
		oxiDur := time.Since(t0)

		t0 = time.Now()
		var monDoc bson.M
		monErr := mcoll.FindOne(ctx, bson.M{"emp_id": "E500000"}).Decode(&monDoc)
		monDur := time.Since(t0)
		record("QUERY", "find_one (indexed)", oxiDur, monDur, 1, 1, oxiErr, monErr)
	}

	// 3c. Count
	{
		t0 = time.Now()
		oxiN, oxiErr := oxi.Count(coll, map[string]any{"department": "engineering"})
		oxiDur := time.Since(t0)

		t0 = time.Now()
		monN, monErr := mcoll.CountDocuments(ctx, bson.M{"department": "engineering"})
		monDur := time.Since(t0)
		record("QUERY", "Count (indexed)", oxiDur, monDur, oxiN, int(monN), oxiErr, monErr)
	}

	// 3d. Sort + limit
	{
		limit := 10
		t0 = time.Now()
		oxiRes, oxiErr := oxi.Find(coll, map[string]any{"region": "EU-WEST"}, &oxidb.FindOptions{
			Sort: map[string]any{"salary": -1}, Limit: &limit,
		})
		oxiDur := time.Since(t0)

		t0 = time.Now()
		cur, monErr := mcoll.Find(ctx, bson.M{"region": "EU-WEST"},
			options.Find().SetSort(bson.D{{Key: "salary", Value: -1}}).SetLimit(int64(limit)))
		var monRes []bson.M
		if monErr == nil {
			monErr = cur.All(ctx, &monRes)
		}
		monDur := time.Since(t0)
		record("QUERY", "Sort + limit 10 (indexed)", oxiDur, monDur, len(oxiRes), len(monRes), oxiErr, monErr)
	}
	fmt.Println()

	// ════════════════════════════════════════════════════════════════
	// Phase 4: Updates
	// ════════════════════════════════════════════════════════════════

	fmt.Println("═══ Phase 4: Updates ═══")

	// Single update
	{
		t0 = time.Now()
		_, oxiErr := oxi.UpdateOne(coll,
			map[string]any{"emp_id": "E000001"},
			map[string]any{"$set": map[string]any{"status": "onleave"}, "$inc": map[string]any{"projects": 1}})
		oxiDur := time.Since(t0)

		t0 = time.Now()
		_, monErr := mcoll.UpdateOne(ctx,
			bson.M{"emp_id": "E000001"},
			bson.M{"$set": bson.M{"status": "onleave"}, "$inc": bson.M{"projects": 1}})
		monDur := time.Since(t0)
		record("UPDATE", "UpdateOne (indexed)", oxiDur, monDur, 1, 1, oxiErr, monErr)
	}

	// Bulk update
	{
		t0 = time.Now()
		oxiRes, oxiErr := oxi.Update(coll,
			map[string]any{"department": "hr", "status": "inactive"},
			map[string]any{"$set": map[string]any{"status": "active"}})
		oxiDur := time.Since(t0)
		oxiN := 0
		if oxiRes != nil {
			if n, ok := oxiRes["modified"]; ok {
				if nf, ok := n.(float64); ok {
					oxiN = int(nf)
				}
			}
		}

		t0 = time.Now()
		monRes, monErr := mcoll.UpdateMany(ctx,
			bson.M{"department": "hr", "status": "inactive"},
			bson.M{"$set": bson.M{"status": "active"}})
		monDur := time.Since(t0)
		monN := 0
		if monRes != nil {
			monN = int(monRes.ModifiedCount)
		}
		record("UPDATE", "UpdateMany (bulk)", oxiDur, monDur, oxiN, monN, oxiErr, monErr)
	}
	fmt.Println()

	// ════════════════════════════════════════════════════════════════
	// Phase 5: Aggregation
	// ════════════════════════════════════════════════════════════════

	fmt.Println("═══ Phase 5: Aggregation ═══")

	// Group by department
	{
		oxiPipeline := []map[string]any{
			{"$group": map[string]any{"_id": "$department", "count": map[string]any{"$sum": 1}, "avg_salary": map[string]any{"$avg": "$salary"}}},
			{"$sort": map[string]any{"count": -1}},
		}
		t0 = time.Now()
		oxiRes, oxiErr := oxi.Aggregate(coll, oxiPipeline)
		oxiDur := time.Since(t0)

		monPipeline := mongo.Pipeline{
			{{Key: "$group", Value: bson.M{"_id": "$department", "count": bson.M{"$sum": 1}, "avg_salary": bson.M{"$avg": "$salary"}}}},
			{{Key: "$sort", Value: bson.D{{Key: "count", Value: -1}}}},
		}
		t0 = time.Now()
		cur, monErr := mcoll.Aggregate(ctx, monPipeline)
		var monRes []bson.M
		if monErr == nil {
			monErr = cur.All(ctx, &monRes)
		}
		monDur := time.Since(t0)
		record("AGGREGATE", "Group by dept + avg salary", oxiDur, monDur, len(oxiRes), len(monRes), oxiErr, monErr)
	}

	// Match + group
	{
		oxiPipeline := []map[string]any{
			{"$match": map[string]any{"region": "US-EAST"}},
			{"$group": map[string]any{"_id": "$department", "total_salary": map[string]any{"$sum": "$salary"}, "headcount": map[string]any{"$sum": 1}}},
			{"$sort": map[string]any{"total_salary": -1}},
		}
		t0 = time.Now()
		oxiRes, oxiErr := oxi.Aggregate(coll, oxiPipeline)
		oxiDur := time.Since(t0)

		monPipeline := mongo.Pipeline{
			{{Key: "$match", Value: bson.M{"region": "US-EAST"}}},
			{{Key: "$group", Value: bson.M{"_id": "$department", "total_salary": bson.M{"$sum": "$salary"}, "headcount": bson.M{"$sum": 1}}}},
			{{Key: "$sort", Value: bson.D{{Key: "total_salary", Value: -1}}}},
		}
		t0 = time.Now()
		cur, monErr := mcoll.Aggregate(ctx, monPipeline)
		var monRes []bson.M
		if monErr == nil {
			monErr = cur.All(ctx, &monRes)
		}
		monDur := time.Since(t0)
		record("AGGREGATE", "Match region + group dept", oxiDur, monDur, len(oxiRes), len(monRes), oxiErr, monErr)
	}

	// Heavy: group by city + stats
	{
		oxiPipeline := []map[string]any{
			{"$group": map[string]any{
				"_id":        "$city",
				"count":      map[string]any{"$sum": 1},
				"avg_salary": map[string]any{"$avg": "$salary"},
				"max_salary": map[string]any{"$max": "$salary"},
				"min_salary": map[string]any{"$min": "$salary"},
			}},
			{"$sort": map[string]any{"avg_salary": -1}},
		}
		t0 = time.Now()
		oxiRes, oxiErr := oxi.Aggregate(coll, oxiPipeline)
		oxiDur := time.Since(t0)

		monPipeline := mongo.Pipeline{
			{{Key: "$group", Value: bson.M{
				"_id":        "$city",
				"count":      bson.M{"$sum": 1},
				"avg_salary": bson.M{"$avg": "$salary"},
				"max_salary": bson.M{"$max": "$salary"},
				"min_salary": bson.M{"$min": "$salary"},
			}}},
			{{Key: "$sort", Value: bson.D{{Key: "avg_salary", Value: -1}}}},
		}
		t0 = time.Now()
		cur, monErr := mcoll.Aggregate(ctx, monPipeline)
		var monRes []bson.M
		if monErr == nil {
			monErr = cur.All(ctx, &monRes)
		}
		monDur := time.Since(t0)
		record("AGGREGATE", "Group by city + full stats", oxiDur, monDur, len(oxiRes), len(monRes), oxiErr, monErr)
	}
	fmt.Println()

	// ════════════════════════════════════════════════════════════════
	// Phase 6: Concurrent throughput
	// ════════════════════════════════════════════════════════════════

	fmt.Println("═══ Phase 6: Concurrent throughput (10 workers x 100 ops) ═══")

	workers := 10
	opsPerWorker := 100
	totalOps := workers * opsPerWorker

	// OxiDB concurrent
	t0 = time.Now()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := oxidb.Connect(oxidbHost, oxidbPort, 5*time.Second)
			if err != nil {
				return
			}
			defer c.Close()
			for i := 0; i < opsPerWorker; i++ {
				empID := fmt.Sprintf("E%06d", rand.Intn(docCount))
				c.FindOne(coll, map[string]any{"emp_id": empID})
			}
		}()
	}
	wg.Wait()
	oxiConc := time.Since(t0)

	// MongoDB concurrent
	t0 = time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				empID := fmt.Sprintf("E%06d", rand.Intn(docCount))
				var doc bson.M
				mcoll.FindOne(ctx, bson.M{"emp_id": empID}).Decode(&doc)
			}
		}()
	}
	wg.Wait()
	monConc := time.Since(t0)

	record("CONCURRENT", fmt.Sprintf("find_one (%d ops, %d workers)", totalOps, workers),
		oxiConc, monConc, totalOps, totalOps, nil, nil)

	oxiOps := float64(totalOps) / oxiConc.Seconds()
	monOps := float64(totalOps) / monConc.Seconds()
	fmt.Printf("  Throughput: OxiDB %.0f ops/sec  MongoDB %.0f ops/sec\n", oxiOps, monOps)
	fmt.Println()

	// ════════════════════════════════════════════════════════════════
	// Phase 7: Delete
	// ════════════════════════════════════════════════════════════════

	fmt.Println("═══ Phase 7: Delete ═══")
	{
		t0 = time.Now()
		oxiRes, oxiErr := oxi.Delete(coll, map[string]any{"status": "onleave"})
		oxiDur := time.Since(t0)
		oxiN := 0
		if oxiRes != nil {
			if n, ok := oxiRes["deleted"]; ok {
				if nf, ok := n.(float64); ok {
					oxiN = int(nf)
				}
			}
		}

		t0 = time.Now()
		monRes, monErr := mcoll.DeleteMany(ctx, bson.M{"status": "onleave"})
		monDur := time.Since(t0)
		monN := 0
		if monRes != nil {
			monN = int(monRes.DeletedCount)
		}
		record("DELETE", "DeleteMany (status=onleave)", oxiDur, monDur, oxiN, monN, oxiErr, monErr)
	}
	fmt.Println()

	// ════════════════════════════════════════════════════════════════
	// Summary
	// ════════════════════════════════════════════════════════════════

	fmt.Println(strings.Repeat("═", 75))
	fmt.Printf("%-12s %-35s %10s %10s %8s\n", "Category", "Operation", "OxiDB", "MongoDB", "Ratio")
	fmt.Println(strings.Repeat("─", 75))

	oxiWins := 0
	monWins := 0
	for _, r := range results {
		ratio := ""
		if r.MongoDB > 0 && r.OxiDB > 0 {
			rv := float64(r.OxiDB) / float64(r.MongoDB)
			if rv < 1 {
				ratio = fmt.Sprintf("%.1fx", 1/rv)
				oxiWins++
			} else {
				ratio = fmt.Sprintf("%.1fx", rv)
				monWins++
			}
		}
		fmt.Printf("%-12s %-35s %10s %10s %8s\n",
			r.Category, r.Name,
			formatDur(r.OxiDB),
			formatDur(r.MongoDB),
			ratio)
	}
	fmt.Println(strings.Repeat("─", 75))
	fmt.Printf("OxiDB faster in %d/%d tests\n", oxiWins, oxiWins+monWins)
	fmt.Println(strings.Repeat("═", 75))

	// Save JSON report
	reportData, _ := json.MarshalIndent(results, "", "  ")
	os.WriteFile("report.json", reportData, 0644)
	fmt.Println("Report saved to report.json")
}
