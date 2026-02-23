package main

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"

	"github.com/parisxmas/OxiDB/go/oxidb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const (
	totalDocs = 1_000_000
	batchSize = 5000
	collName  = "bench_1m"
	oxidbAddr = "127.0.0.1:4444"
	oxidbHost = "127.0.0.1"
	oxidbPort = 4444
	mongoURI  = "mongodb://127.0.0.1:27017"
	mongoDBN  = "benchmark"
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

// Query/agg labels used for both engines (must match 1:1).
var queryLabels = []string{
	"Exact: department=Engineering",
	"Exact: status=active AND country=US",
	"Range: age 25-35",
	"Range: salary > 150000",
	"Bool: verified=true (limit 50)",
	"Sort: score DESC, limit 10",
	"Sort: salary ASC, limit 10",
	"Date range: birthDate 1990-2000",
	"$or: city=Tokyo OR city=Paris",
	"$in: rating in [4,5]",
	"Nested: address.zip ^0",
	"$regex: name ^A",
	"$exists: tags",
	"$ne: status != active",
	"Count: department=Sales",
	"Count: verified=true",
	"FindOne: seq=500000",
	"FindOne: seq=999999",
}

var aggLabels = []string{
	"Count by department",
	"Avg salary by department",
	"Count by country (top 5)",
	"Active users avg score/city top5",
	"Min/Max salary by department",
	"Count by rating",
	"Count by status",
}

// ---------------------------------------------------------------------------
// Result types
// ---------------------------------------------------------------------------

type OpResult struct {
	Time  time.Duration
	Count int
	Err   error
}

type BenchResult struct {
	Engine     string
	InsertTime time.Duration
	InsertRate float64
	IndexTime  time.Duration
	Indexes    []OpResult // 14
	Queries    []OpResult // 18
	Aggs       []OpResult // 7
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func randomDate(rng *rand.Rand, minYear, maxYear int) string {
	y := minYear + rng.Intn(maxYear-minYear+1)
	m := 1 + rng.Intn(12)
	d := 1 + rng.Intn(28)
	return fmt.Sprintf("%04d-%02d-%02d", y, m, d)
}

func randomTags(rng *rand.Rand) []any {
	n := 1 + rng.Intn(4)
	out := make([]any, n)
	for i := range out {
		out[i] = allTags[rng.Intn(len(allTags))]
	}
	return out
}

func generateDoc(rng *rand.Rand, i int) map[string]any {
	first := firstNames[rng.Intn(len(firstNames))]
	last := lastNames[rng.Intn(len(lastNames))]
	return map[string]any{
		"seq":        i,
		"name":       first + " " + last,
		"email":      fmt.Sprintf("%s.%s.%d@example.com", strings.ToLower(first), strings.ToLower(last), i),
		"age":        18 + rng.Intn(62),
		"salary":     30000.0 + rng.Float64()*170000.0,
		"department": depts[rng.Intn(len(depts))],
		"city":       cities[rng.Intn(len(cities))],
		"country":    countries[rng.Intn(len(countries))],
		"status":     statuses[rng.Intn(len(statuses))],
		"score":      rng.Float64() * 100.0,
		"verified":   rng.Intn(2) == 1,
		"birthDate":  randomDate(rng, 1960, 2005),
		"hireDate":   randomDate(rng, 2010, 2025),
		"tags":       randomTags(rng),
		"rating":     rng.Intn(5) + 1,
		"address": map[string]any{
			"street": fmt.Sprintf("%d %s St", 100+rng.Intn(9900), lastNames[rng.Intn(len(lastNames))]),
			"zip":    fmt.Sprintf("%05d", rng.Intn(100000)),
		},
	}
}

func waitForService(name, addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("%s not reachable at %s after %s", name, addr, timeout)
}

func fmtDur(d time.Duration) string {
	if d < time.Millisecond {
		return d.Round(time.Microsecond).String()
	}
	if d < time.Second {
		return d.Round(100 * time.Microsecond).String()
	}
	return d.Round(time.Millisecond).String()
}

func winnerStr(a, b time.Duration) string {
	if a < b {
		return "OxiDB"
	}
	if b < a {
		return "MongoDB"
	}
	return "Tie"
}

// ---------------------------------------------------------------------------
// OxiDB benchmark
// ---------------------------------------------------------------------------

func benchmarkOxiDB() *BenchResult {
	res := &BenchResult{Engine: "OxiDB"}

	client, err := oxidb.Connect(oxidbHost, oxidbPort, 30*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "OxiDB connect: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()
	if _, err := client.Ping(); err != nil {
		fmt.Fprintf(os.Stderr, "OxiDB ping: %v\n", err)
		os.Exit(1)
	}
	_ = client.DropCollection(collName)
	fmt.Println("[OxiDB] Connected. Old collection dropped.")

	// ── Phase 1: INSERT ──────────────────────────────────────────────
	fmt.Println("[OxiDB] Phase 1: INSERT")
	rng := rand.New(rand.NewSource(42))
	start := time.Now()
	inserted := 0
	lastReport := time.Now()
	for inserted < totalDocs {
		n := batchSize
		if totalDocs-inserted < n {
			n = totalDocs - inserted
		}
		batch := make([]map[string]any, n)
		for j := 0; j < n; j++ {
			batch[j] = generateDoc(rng, inserted+j)
		}
		if _, err := client.InsertMany(collName, batch); err != nil {
			fmt.Fprintf(os.Stderr, "OxiDB insert at %d: %v\n", inserted, err)
			os.Exit(1)
		}
		inserted += n
		if time.Since(lastReport) >= 3*time.Second || inserted == totalDocs {
			pct := float64(inserted) / float64(totalDocs) * 100
			rate := float64(inserted) / time.Since(start).Seconds()
			fmt.Printf("  [OxiDB]  %7d / %d  (%5.1f%%)  %8.0f docs/s\n", inserted, totalDocs, pct, rate)
			lastReport = time.Now()
		}
	}
	res.InsertTime = time.Since(start)
	res.InsertRate = float64(totalDocs) / res.InsertTime.Seconds()
	fmt.Printf("[OxiDB] Insert done: %s (%.0f docs/s)\n\n", fmtDur(res.InsertTime), res.InsertRate)

	// ── Phase 2: INDEXES ─────────────────────────────────────────────
	fmt.Println("[OxiDB] Phase 2: INDEXES")
	type idxDef struct {
		label string
		fn    func() error
	}
	idxDefs := []idxDef{
		{"email (unique)", func() error { return client.CreateUniqueIndex(collName, "email") }},
		{"age", func() error { return client.CreateIndex(collName, "age") }},
		{"department", func() error { return client.CreateIndex(collName, "department") }},
		{"city", func() error { return client.CreateIndex(collName, "city") }},
		{"country", func() error { return client.CreateIndex(collName, "country") }},
		{"status", func() error { return client.CreateIndex(collName, "status") }},
		{"salary", func() error { return client.CreateIndex(collName, "salary") }},
		{"score", func() error { return client.CreateIndex(collName, "score") }},
		{"verified", func() error { return client.CreateIndex(collName, "verified") }},
		{"rating", func() error { return client.CreateIndex(collName, "rating") }},
		{"birthDate", func() error { return client.CreateIndex(collName, "birthDate") }},
		{"hireDate", func() error { return client.CreateIndex(collName, "hireDate") }},
		{"[department,city]", func() error {
			return client.CreateCompositeIndex(collName, []string{"department", "city"})
		}},
		{"[country,status]", func() error {
			return client.CreateCompositeIndex(collName, []string{"country", "status"})
		}},
	}
	idxStart := time.Now()
	for _, idx := range idxDefs {
		t0 := time.Now()
		e := idx.fn()
		res.Indexes = append(res.Indexes, OpResult{Time: time.Since(t0), Err: e})
		mark := "ok"
		if e != nil {
			mark = "FAIL " + e.Error()
		}
		fmt.Printf("  [OxiDB] %-25s %s  %s\n", idx.label, fmtDur(time.Since(t0)), mark)
	}
	res.IndexTime = time.Since(idxStart)
	fmt.Printf("[OxiDB] Indexes done: %s\n\n", fmtDur(res.IndexTime))

	// ── Phase 3: QUERIES ─────────────────────────────────────────────
	fmt.Println("[OxiDB] Phase 3: QUERIES")
	limit10 := 10
	limit50 := 50

	type qDef struct {
		fn func() (int, error)
	}
	qDefs := []qDef{
		{func() (int, error) {
			d, e := client.Find(collName, map[string]any{"department": "Engineering"}, &oxidb.FindOptions{Limit: &limit10})
			return len(d), e
		}},
		{func() (int, error) {
			d, e := client.Find(collName, map[string]any{"status": "active", "country": "US"}, &oxidb.FindOptions{Limit: &limit10})
			return len(d), e
		}},
		{func() (int, error) {
			d, e := client.Find(collName, map[string]any{"age": map[string]any{"$gte": 25, "$lte": 35}}, &oxidb.FindOptions{Limit: &limit10})
			return len(d), e
		}},
		{func() (int, error) {
			d, e := client.Find(collName, map[string]any{"salary": map[string]any{"$gt": 150000}}, &oxidb.FindOptions{Limit: &limit10})
			return len(d), e
		}},
		{func() (int, error) {
			d, e := client.Find(collName, map[string]any{"verified": true}, &oxidb.FindOptions{Limit: &limit50})
			return len(d), e
		}},
		{func() (int, error) {
			d, e := client.Find(collName, map[string]any{}, &oxidb.FindOptions{Sort: map[string]any{"score": -1}, Limit: &limit10})
			return len(d), e
		}},
		{func() (int, error) {
			d, e := client.Find(collName, map[string]any{}, &oxidb.FindOptions{Sort: map[string]any{"salary": 1}, Limit: &limit10})
			return len(d), e
		}},
		{func() (int, error) {
			d, e := client.Find(collName, map[string]any{"birthDate": map[string]any{"$gte": "1990-01-01", "$lte": "2000-12-31"}}, &oxidb.FindOptions{Limit: &limit10})
			return len(d), e
		}},
		{func() (int, error) {
			d, e := client.Find(collName, map[string]any{"$or": []any{map[string]any{"city": "Tokyo"}, map[string]any{"city": "Paris"}}}, &oxidb.FindOptions{Limit: &limit10})
			return len(d), e
		}},
		{func() (int, error) {
			d, e := client.Find(collName, map[string]any{"rating": map[string]any{"$in": []any{4, 5}}}, &oxidb.FindOptions{Limit: &limit10})
			return len(d), e
		}},
		{func() (int, error) {
			d, e := client.Find(collName, map[string]any{"address.zip": map[string]any{"$regex": "^0"}}, &oxidb.FindOptions{Limit: &limit10})
			return len(d), e
		}},
		{func() (int, error) {
			d, e := client.Find(collName, map[string]any{"name": map[string]any{"$regex": "^A"}}, &oxidb.FindOptions{Limit: &limit10})
			return len(d), e
		}},
		{func() (int, error) {
			d, e := client.Find(collName, map[string]any{"tags": map[string]any{"$exists": true}}, &oxidb.FindOptions{Limit: &limit10})
			return len(d), e
		}},
		{func() (int, error) {
			d, e := client.Find(collName, map[string]any{"status": map[string]any{"$ne": "active"}}, &oxidb.FindOptions{Limit: &limit10})
			return len(d), e
		}},
		{func() (int, error) { return client.Count(collName, map[string]any{"department": "Sales"}) }},
		{func() (int, error) { return client.Count(collName, map[string]any{"verified": true}) }},
		{func() (int, error) {
			d, e := client.FindOne(collName, map[string]any{"seq": 500000})
			if d != nil {
				return 1, e
			}
			return 0, e
		}},
		{func() (int, error) {
			d, e := client.FindOne(collName, map[string]any{"seq": 999999})
			if d != nil {
				return 1, e
			}
			return 0, e
		}},
	}

	for i, q := range qDefs {
		t0 := time.Now()
		cnt, e := q.fn()
		res.Queries = append(res.Queries, OpResult{Time: time.Since(t0), Count: cnt, Err: e})
		status := fmt.Sprintf("%d results", cnt)
		if e != nil {
			status = "ERR " + e.Error()
		}
		fmt.Printf("  [OxiDB] Q%02d %-34s %12s  %s\n", i+1, queryLabels[i], fmtDur(time.Since(t0)), status)
	}
	fmt.Println()

	// ── Phase 4: AGGREGATION ─────────────────────────────────────────
	fmt.Println("[OxiDB] Phase 4: AGGREGATION")
	aggPipelines := [][]map[string]any{
		{{"$group": map[string]any{"_id": "$department", "count": map[string]any{"$sum": 1}}}, {"$sort": map[string]any{"count": -1}}},
		{{"$group": map[string]any{"_id": "$department", "avg": map[string]any{"$avg": "$salary"}}}, {"$sort": map[string]any{"avg": -1}}},
		{{"$group": map[string]any{"_id": "$country", "count": map[string]any{"$sum": 1}}}, {"$sort": map[string]any{"count": -1}}, {"$limit": 5}},
		{{"$match": map[string]any{"status": "active"}}, {"$group": map[string]any{"_id": "$city", "avgScore": map[string]any{"$avg": "$score"}, "count": map[string]any{"$sum": 1}}}, {"$sort": map[string]any{"avgScore": -1}}, {"$limit": 5}},
		{{"$group": map[string]any{"_id": "$department", "min": map[string]any{"$min": "$salary"}, "max": map[string]any{"$max": "$salary"}}}, {"$sort": map[string]any{"max": -1}}},
		{{"$group": map[string]any{"_id": "$rating", "count": map[string]any{"$sum": 1}}}, {"$sort": map[string]any{"_id": 1}}},
		{{"$group": map[string]any{"_id": "$status", "count": map[string]any{"$sum": 1}}}, {"$sort": map[string]any{"count": -1}}},
	}
	for i, p := range aggPipelines {
		t0 := time.Now()
		docs, e := client.Aggregate(collName, p)
		cnt := len(docs)
		res.Aggs = append(res.Aggs, OpResult{Time: time.Since(t0), Count: cnt, Err: e})
		status := fmt.Sprintf("%d groups", cnt)
		if e != nil {
			status = "ERR " + e.Error()
		}
		fmt.Printf("  [OxiDB] A%d %-35s %12s  %s\n", i+1, aggLabels[i], fmtDur(time.Since(t0)), status)
	}
	fmt.Println()
	return res
}

// ---------------------------------------------------------------------------
// MongoDB benchmark
// ---------------------------------------------------------------------------

func benchmarkMongoDB() *BenchResult {
	res := &BenchResult{Engine: "MongoDB"}
	ctx := context.Background()

	client, err := mongo.Connect(options.Client().ApplyURI(mongoURI))
	if err != nil {
		fmt.Fprintf(os.Stderr, "MongoDB connect: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = client.Disconnect(ctx) }()

	if err := client.Ping(ctx, nil); err != nil {
		fmt.Fprintf(os.Stderr, "MongoDB ping: %v\n", err)
		os.Exit(1)
	}

	coll := client.Database(mongoDBN).Collection(collName)
	_ = coll.Drop(ctx)
	fmt.Println("[MongoDB] Connected. Old collection dropped.")

	// ── Phase 1: INSERT ──────────────────────────────────────────────
	fmt.Println("[MongoDB] Phase 1: INSERT")
	rng := rand.New(rand.NewSource(42))
	insertOpts := options.InsertMany().SetOrdered(false)
	start := time.Now()
	inserted := 0
	lastReport := time.Now()
	for inserted < totalDocs {
		n := batchSize
		if totalDocs-inserted < n {
			n = totalDocs - inserted
		}
		batch := make([]any, n)
		for j := 0; j < n; j++ {
			batch[j] = generateDoc(rng, inserted+j)
		}
		if _, err := coll.InsertMany(ctx, batch, insertOpts); err != nil {
			fmt.Fprintf(os.Stderr, "MongoDB insert at %d: %v\n", inserted, err)
			os.Exit(1)
		}
		inserted += n
		if time.Since(lastReport) >= 3*time.Second || inserted == totalDocs {
			pct := float64(inserted) / float64(totalDocs) * 100
			rate := float64(inserted) / time.Since(start).Seconds()
			fmt.Printf("  [MongoDB] %7d / %d  (%5.1f%%)  %8.0f docs/s\n", inserted, totalDocs, pct, rate)
			lastReport = time.Now()
		}
	}
	res.InsertTime = time.Since(start)
	res.InsertRate = float64(totalDocs) / res.InsertTime.Seconds()
	fmt.Printf("[MongoDB] Insert done: %s (%.0f docs/s)\n\n", fmtDur(res.InsertTime), res.InsertRate)

	// Verify count
	cnt, _ := coll.CountDocuments(ctx, bson.M{})
	fmt.Printf("[MongoDB] Document count: %d\n\n", cnt)

	// ── Phase 2: INDEXES ─────────────────────────────────────────────
	fmt.Println("[MongoDB] Phase 2: INDEXES")
	type idxDef struct {
		label string
		model mongo.IndexModel
	}
	idxDefs := []idxDef{
		{"email (unique)", mongo.IndexModel{Keys: bson.D{{"email", 1}}, Options: options.Index().SetUnique(true)}},
		{"age", mongo.IndexModel{Keys: bson.D{{"age", 1}}}},
		{"department", mongo.IndexModel{Keys: bson.D{{"department", 1}}}},
		{"city", mongo.IndexModel{Keys: bson.D{{"city", 1}}}},
		{"country", mongo.IndexModel{Keys: bson.D{{"country", 1}}}},
		{"status", mongo.IndexModel{Keys: bson.D{{"status", 1}}}},
		{"salary", mongo.IndexModel{Keys: bson.D{{"salary", 1}}}},
		{"score", mongo.IndexModel{Keys: bson.D{{"score", 1}}}},
		{"verified", mongo.IndexModel{Keys: bson.D{{"verified", 1}}}},
		{"rating", mongo.IndexModel{Keys: bson.D{{"rating", 1}}}},
		{"birthDate", mongo.IndexModel{Keys: bson.D{{"birthDate", 1}}}},
		{"hireDate", mongo.IndexModel{Keys: bson.D{{"hireDate", 1}}}},
		{"[department,city]", mongo.IndexModel{Keys: bson.D{{"department", 1}, {"city", 1}}}},
		{"[country,status]", mongo.IndexModel{Keys: bson.D{{"country", 1}, {"status", 1}}}},
	}
	idxStart := time.Now()
	for _, idx := range idxDefs {
		t0 := time.Now()
		_, e := coll.Indexes().CreateOne(ctx, idx.model)
		res.Indexes = append(res.Indexes, OpResult{Time: time.Since(t0), Err: e})
		mark := "ok"
		if e != nil {
			mark = "FAIL " + e.Error()
		}
		fmt.Printf("  [MongoDB] %-25s %s  %s\n", idx.label, fmtDur(time.Since(t0)), mark)
	}
	res.IndexTime = time.Since(idxStart)
	fmt.Printf("[MongoDB] Indexes done: %s\n\n", fmtDur(res.IndexTime))

	// ── Phase 3: QUERIES ─────────────────────────────────────────────
	fmt.Println("[MongoDB] Phase 3: QUERIES")

	// helper: find with filter + opts, return count
	mFind := func(filter bson.M, limit int64, sort bson.D) (int, error) {
		fopts := options.Find().SetLimit(limit)
		if sort != nil {
			fopts = fopts.SetSort(sort)
		}
		cur, e := coll.Find(ctx, filter, fopts)
		if e != nil {
			return 0, e
		}
		var docs []bson.M
		e = cur.All(ctx, &docs)
		return len(docs), e
	}

	type qDef struct {
		fn func() (int, error)
	}
	qDefs := []qDef{
		{func() (int, error) { return mFind(bson.M{"department": "Engineering"}, 10, nil) }},
		{func() (int, error) { return mFind(bson.M{"status": "active", "country": "US"}, 10, nil) }},
		{func() (int, error) {
			return mFind(bson.M{"age": bson.M{"$gte": 25, "$lte": 35}}, 10, nil)
		}},
		{func() (int, error) {
			return mFind(bson.M{"salary": bson.M{"$gt": 150000}}, 10, nil)
		}},
		{func() (int, error) { return mFind(bson.M{"verified": true}, 50, nil) }},
		{func() (int, error) {
			return mFind(bson.M{}, 10, bson.D{{"score", -1}})
		}},
		{func() (int, error) {
			return mFind(bson.M{}, 10, bson.D{{"salary", 1}})
		}},
		{func() (int, error) {
			return mFind(bson.M{"birthDate": bson.M{"$gte": "1990-01-01", "$lte": "2000-12-31"}}, 10, nil)
		}},
		{func() (int, error) {
			return mFind(bson.M{"$or": bson.A{bson.M{"city": "Tokyo"}, bson.M{"city": "Paris"}}}, 10, nil)
		}},
		{func() (int, error) {
			return mFind(bson.M{"rating": bson.M{"$in": bson.A{4, 5}}}, 10, nil)
		}},
		{func() (int, error) {
			return mFind(bson.M{"address.zip": bson.M{"$regex": "^0"}}, 10, nil)
		}},
		{func() (int, error) {
			return mFind(bson.M{"name": bson.M{"$regex": "^A"}}, 10, nil)
		}},
		{func() (int, error) {
			return mFind(bson.M{"tags": bson.M{"$exists": true}}, 10, nil)
		}},
		{func() (int, error) {
			return mFind(bson.M{"status": bson.M{"$ne": "active"}}, 10, nil)
		}},
		// Count queries
		{func() (int, error) {
			n, e := coll.CountDocuments(ctx, bson.M{"department": "Sales"})
			return int(n), e
		}},
		{func() (int, error) {
			n, e := coll.CountDocuments(ctx, bson.M{"verified": true})
			return int(n), e
		}},
		// FindOne queries
		{func() (int, error) {
			var doc bson.M
			e := coll.FindOne(ctx, bson.M{"seq": 500000}).Decode(&doc)
			if e == mongo.ErrNoDocuments {
				return 0, nil
			}
			if e != nil {
				return 0, e
			}
			return 1, nil
		}},
		{func() (int, error) {
			var doc bson.M
			e := coll.FindOne(ctx, bson.M{"seq": 999999}).Decode(&doc)
			if e == mongo.ErrNoDocuments {
				return 0, nil
			}
			if e != nil {
				return 0, e
			}
			return 1, nil
		}},
	}

	for i, q := range qDefs {
		t0 := time.Now()
		c, e := q.fn()
		res.Queries = append(res.Queries, OpResult{Time: time.Since(t0), Count: c, Err: e})
		status := fmt.Sprintf("%d results", c)
		if e != nil {
			status = "ERR " + e.Error()
		}
		fmt.Printf("  [MongoDB] Q%02d %-34s %12s  %s\n", i+1, queryLabels[i], fmtDur(time.Since(t0)), status)
	}
	fmt.Println()

	// ── Phase 4: AGGREGATION ─────────────────────────────────────────
	fmt.Println("[MongoDB] Phase 4: AGGREGATION")

	mAgg := func(pipeline bson.A) (int, error) {
		cur, e := coll.Aggregate(ctx, pipeline)
		if e != nil {
			return 0, e
		}
		var docs []bson.M
		e = cur.All(ctx, &docs)
		return len(docs), e
	}

	aggPipelines := []bson.A{
		{bson.M{"$group": bson.M{"_id": "$department", "count": bson.M{"$sum": 1}}}, bson.M{"$sort": bson.D{{"count", -1}}}},
		{bson.M{"$group": bson.M{"_id": "$department", "avg": bson.M{"$avg": "$salary"}}}, bson.M{"$sort": bson.D{{"avg", -1}}}},
		{bson.M{"$group": bson.M{"_id": "$country", "count": bson.M{"$sum": 1}}}, bson.M{"$sort": bson.D{{"count", -1}}}, bson.M{"$limit": 5}},
		{bson.M{"$match": bson.M{"status": "active"}}, bson.M{"$group": bson.M{"_id": "$city", "avgScore": bson.M{"$avg": "$score"}, "count": bson.M{"$sum": 1}}}, bson.M{"$sort": bson.D{{"avgScore", -1}}}, bson.M{"$limit": 5}},
		{bson.M{"$group": bson.M{"_id": "$department", "min": bson.M{"$min": "$salary"}, "max": bson.M{"$max": "$salary"}}}, bson.M{"$sort": bson.D{{"max", -1}}}},
		{bson.M{"$group": bson.M{"_id": "$rating", "count": bson.M{"$sum": 1}}}, bson.M{"$sort": bson.D{{"_id", 1}}}},
		{bson.M{"$group": bson.M{"_id": "$status", "count": bson.M{"$sum": 1}}}, bson.M{"$sort": bson.D{{"count", -1}}}},
	}
	for i, p := range aggPipelines {
		t0 := time.Now()
		c, e := mAgg(p)
		res.Aggs = append(res.Aggs, OpResult{Time: time.Since(t0), Count: c, Err: e})
		status := fmt.Sprintf("%d groups", c)
		if e != nil {
			status = "ERR " + e.Error()
		}
		fmt.Printf("  [MongoDB] A%d %-35s %12s  %s\n", i+1, aggLabels[i], fmtDur(time.Since(t0)), status)
	}
	fmt.Println()
	return res
}

// ---------------------------------------------------------------------------
// Comparison table
// ---------------------------------------------------------------------------

const (
	colOp  = 42
	colVal = 14
	colWin = 9
)

func tLine(l, m, r, h string) {
	fmt.Printf("%s%s%s%s%s%s%s%s%s\n",
		l, strings.Repeat(h, colOp+2),
		m, strings.Repeat(h, colVal+2),
		m, strings.Repeat(h, colVal+2),
		m, strings.Repeat(h, colWin+2), r)
}

func tRow(op, a, b, w string) {
	fmt.Printf("│ %-*s │ %*s │ %*s │ %-*s │\n", colOp, op, colVal, a, colVal, b, colWin, w)
}

func printComparison(oxi, mgo *BenchResult) {
	oxiWins, mgoWins := 0, 0
	win := func(a, b time.Duration) string {
		w := winnerStr(a, b)
		if w == "OxiDB" {
			oxiWins++
		} else if w == "MongoDB" {
			mgoWins++
		}
		return w
	}

	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════════════════════════════╗")
	fmt.Println("║                    OxiDB vs MongoDB  —  1 000 000 Document Benchmark                ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════════════════════════════╝")
	fmt.Println()

	tLine("┌", "┬", "┐", "─")
	tRow("Operation", "OxiDB", "MongoDB", "Winner")
	tLine("├", "┼", "┤", "─")

	// Insert
	tRow("INSERT 1M docs",
		fmtDur(oxi.InsertTime), fmtDur(mgo.InsertTime),
		win(oxi.InsertTime, mgo.InsertTime))
	tRow("  throughput",
		fmt.Sprintf("%.0f doc/s", oxi.InsertRate),
		fmt.Sprintf("%.0f doc/s", mgo.InsertRate), "")

	tLine("├", "┼", "┤", "─")

	// Indexes
	tRow(fmt.Sprintf("INDEXES total (%d)", len(oxi.Indexes)),
		fmtDur(oxi.IndexTime), fmtDur(mgo.IndexTime),
		win(oxi.IndexTime, mgo.IndexTime))

	tLine("├", "┼", "┤", "─")

	// Queries
	var oxiQTotal, mgoQTotal time.Duration
	for i, label := range queryLabels {
		ot := oxi.Queries[i].Time
		mt := mgo.Queries[i].Time
		oxiQTotal += ot
		mgoQTotal += mt
		tRow(fmt.Sprintf("Q%02d %s", i+1, label), fmtDur(ot), fmtDur(mt), win(ot, mt))
	}
	tLine("├", "┼", "┤", "─")
	tRow("QUERY total", fmtDur(oxiQTotal), fmtDur(mgoQTotal), winnerStr(oxiQTotal, mgoQTotal))

	tLine("├", "┼", "┤", "─")

	// Aggregations
	var oxiATotal, mgoATotal time.Duration
	for i, label := range aggLabels {
		ot := oxi.Aggs[i].Time
		mt := mgo.Aggs[i].Time
		oxiATotal += ot
		mgoATotal += mt
		tRow(fmt.Sprintf("A%d  %s", i+1, label), fmtDur(ot), fmtDur(mt), win(ot, mt))
	}
	tLine("├", "┼", "┤", "─")
	tRow("AGG total", fmtDur(oxiATotal), fmtDur(mgoATotal), winnerStr(oxiATotal, mgoATotal))

	tLine("├", "┼", "┤", "─")

	// Score
	total := oxiWins + mgoWins
	tRow("SCORE",
		fmt.Sprintf("%d / %d wins", oxiWins, total),
		fmt.Sprintf("%d / %d wins", mgoWins, total),
		winnerStr(time.Duration(mgoWins), time.Duration(oxiWins))) // more wins = better
	tLine("└", "┴", "┘", "─")
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

func main() {
	fmt.Println("╔══════════════════════════════════════════════════════════╗")
	fmt.Println("║     OxiDB vs MongoDB — Docker 1M Document Benchmark     ║")
	fmt.Println("╚══════════════════════════════════════════════════════════╝")
	fmt.Printf("Documents:  %dk   Batch: %d\n", totalDocs/1000, batchSize)
	fmt.Printf("OxiDB:      %s\n", oxidbAddr)
	fmt.Printf("MongoDB:    %s\n\n", mongoURI)

	fmt.Println("Waiting for services...")
	if err := waitForService("OxiDB", oxidbAddr, 60*time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	fmt.Println("  OxiDB ready.")

	if err := waitForService("MongoDB", "127.0.0.1:27017", 60*time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	fmt.Println("  MongoDB ready.")
	fmt.Println()

	// Run benchmarks sequentially to avoid resource contention.
	oxiResult := benchmarkOxiDB()
	mgoResult := benchmarkMongoDB()

	printComparison(oxiResult, mgoResult)
}
