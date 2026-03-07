// Package comparison_mongodb benchmarks OxiDB against MongoDB 7 with 100K
// documents. Both run in Docker containers with tmpfs storage.
//
// Metrics: insert time, query time, disk size, memory usage.
package comparison_mongodb

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

	"github.com/parisxmas/OxiDB/go/oxidb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ═══════════════════════════════════════════════════════════════════════════
// Constants & globals
// ═══════════════════════════════════════════════════════════════════════════

const (
	oxidbHost  = "127.0.0.1"
	oxidbPort  = 4444
	mongoURI   = "mongodb://127.0.0.1:27017"
	totalDocs  = 100_000
	batchSize  = 1000
	collection = "bench_employees"
)

var (
	oxiClient   *oxidb.Client
	mongoClient *mongo.Client
	mongoDB     *mongo.Database
	mongoColl   *mongo.Collection
	ctx         = context.Background()

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
	MongoDur  time.Duration
	OxiCount  int
	MongoCount int
	OxiErr    string
	MongoErr  string
	OxiDetail string
	MongoDetail string
}

func recordTiming(cat, name string, oxiDur, mongoDur time.Duration, oxiCount, mongoCount int, oxiErr, mongoErr error) {
	timingMutex.Lock()
	defer timingMutex.Unlock()
	e := TimingEntry{
		Category:   cat,
		Name:       name,
		OxiDur:     oxiDur,
		MongoDur:   mongoDur,
		OxiCount:   oxiCount,
		MongoCount: mongoCount,
	}
	if oxiErr != nil {
		e.OxiErr = oxiErr.Error()
	}
	if mongoErr != nil {
		e.MongoErr = mongoErr.Error()
	}
	timings = append(timings, e)
}

func recordTimingDetailed(cat, name string, oxiDur, mongoDur time.Duration, oxiCount, mongoCount int, oxiErr, mongoErr error, oxiDetail, mongoDetail string) {
	timingMutex.Lock()
	defer timingMutex.Unlock()
	e := TimingEntry{
		Category:    cat,
		Name:        name,
		OxiDur:      oxiDur,
		MongoDur:    mongoDur,
		OxiCount:    oxiCount,
		MongoCount:  mongoCount,
		OxiDetail:   oxiDetail,
		MongoDetail: mongoDetail,
	}
	if oxiErr != nil {
		e.OxiErr = oxiErr.Error()
	}
	if mongoErr != nil {
		e.MongoErr = mongoErr.Error()
	}
	timings = append(timings, e)
}

// ═══════════════════════════════════════════════════════════════════════════
// TestMain — global setup & teardown
// ═══════════════════════════════════════════════════════════════════════════

func TestMain(m *testing.M) {
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║   OxiDB vs MongoDB — 100K Document Benchmark               ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")

	// Wait for OxiDB
	if err := waitFor("OxiDB", fmt.Sprintf("%s:%d", oxidbHost, oxidbPort), 60*time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: %v\n", err)
		os.Exit(1)
	}

	// Wait for MongoDB
	if err := waitFor("MongoDB", "127.0.0.1:27017", 60*time.Second); err != nil {
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
	fmt.Println("  OxiDB connected (OxiWire wire format).")

	// Connect MongoDB
	mongoClient, err = mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: MongoDB connect: %v\n", err)
		os.Exit(1)
	}
	if err := mongoClient.Ping(ctx, nil); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: MongoDB ping: %v\n", err)
		os.Exit(1)
	}
	mongoDB = mongoClient.Database("bench")
	mongoColl = mongoDB.Collection(collection)
	fmt.Println("  MongoDB 7 connected (tmpfs, WiredTiger).")
	fmt.Println()

	code := m.Run()

	oxiClient.Close()
	mongoClient.Disconnect(ctx)

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
// Data generation
// ═══════════════════════════════════════════════════════════════════════════

var (
	firstNames = []string{"Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hank", "Ivy", "Jack"}
	lastNames  = []string{"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"}
	cities     = []string{"New York", "London", "Tokyo", "Paris", "Berlin", "Sydney", "Toronto", "Mumbai", "Dubai", "Singapore"}
	countries  = []string{"US", "UK", "JP", "FR", "DE", "AU", "CA", "IN", "AE", "SG"}
	depts      = []string{"Engineering", "Sales", "Marketing", "Finance", "HR"}
	statuses   = []string{"active", "inactive", "suspended", "pending"}
	tags       = []string{"vip", "premium", "trial", "enterprise", "beta"}
)

func genDoc(rng *rand.Rand, i int) map[string]any {
	first := firstNames[rng.Intn(len(firstNames))]
	last := lastNames[rng.Intn(len(lastNames))]
	nTags := 1 + rng.Intn(3)
	docTags := make([]any, nTags)
	for j := range docTags {
		docTags[j] = tags[rng.Intn(len(tags))]
	}
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
		"tags":       docTags,
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
	mongoColl.Drop(ctx)

	rng := rand.New(rand.NewSource(42))

	// Pre-generate all documents
	allDocs := make([]map[string]any, totalDocs)
	for i := 0; i < totalDocs; i++ {
		allDocs[i] = genDoc(rng, i)
	}

	// ── OxiDB insert (pipelined: 10 batches of 1000 per roundtrip = 10 roundtrips) ──
	const pipelineSize = 10 // batches per pipeline call
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
	if oxiInserted != totalDocs {
		t.Fatalf("OxiDB pipeline inserted %d, want %d", oxiInserted, totalDocs)
	}

	// ── MongoDB insert ──
	mongoStart := time.Now()
	for offset := 0; offset < totalDocs; offset += batchSize {
		end := offset + batchSize
		if end > totalDocs {
			end = totalDocs
		}
		batch := allDocs[offset:end]
		mongoDocs := make([]any, len(batch))
		for i, doc := range batch {
			mongoDocs[i] = doc
		}
		_, err := mongoColl.InsertMany(ctx, mongoDocs)
		if err != nil {
			t.Fatalf("MongoDB insert batch at %d: %v", offset, err)
		}
	}
	mongoDur := time.Since(mongoStart)

	// Verify counts
	oxiCount, err := oxiClient.Count(collection, map[string]any{})
	if err != nil {
		t.Fatalf("OxiDB count: %v", err)
	}
	mongoCount, err := mongoColl.CountDocuments(ctx, bson.M{})
	if err != nil {
		t.Fatalf("MongoDB count: %v", err)
	}

	oxiRate := float64(totalDocs) / oxiDur.Seconds()
	mongoRate := float64(totalDocs) / mongoDur.Seconds()

	fmt.Printf("\n  ┌─ Bulk Insert %d documents ─────────────────────────────\n", totalDocs)
	fmt.Printf("  │  OxiDB:   %v  (%.0f docs/s)\n", oxiDur.Round(time.Millisecond), oxiRate)
	fmt.Printf("  │  MongoDB: %v  (%.0f docs/s)\n", mongoDur.Round(time.Millisecond), mongoRate)
	fmt.Printf("  │  Counts:  OxiDB=%d  MongoDB=%d\n", oxiCount, mongoCount)
	fmt.Printf("  └───────────────────────────────────────────────────────\n\n")

	recordTimingDetailed("Bulk Insert", fmt.Sprintf("Insert %d docs (batch %d)", totalDocs, batchSize),
		oxiDur, mongoDur, oxiCount, int(mongoCount), nil, nil,
		fmt.Sprintf("%.0f docs/s", oxiRate), fmt.Sprintf("%.0f docs/s", mongoRate))

	if oxiCount != totalDocs {
		t.Errorf("OxiDB count mismatch: got %d, want %d", oxiCount, totalDocs)
	}
	if mongoCount != int64(totalDocs) {
		t.Errorf("MongoDB count mismatch: got %d, want %d", mongoCount, totalDocs)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Test: Queries (run after insert)
// ═══════════════════════════════════════════════════════════════════════════

func TestQueries(t *testing.T) {
	// Ensure data exists
	oxiCount, err := oxiClient.Count(collection, map[string]any{})
	if err != nil || oxiCount == 0 {
		t.Skip("No data — run TestBulkInsert first")
	}

	tests := []struct {
		name       string
		oxiQuery   map[string]any
		mongoQuery bson.M
	}{
		{
			name:       "Exact match (department=Engineering)",
			oxiQuery:   map[string]any{"department": "Engineering"},
			mongoQuery: bson.M{"department": "Engineering"},
		},
		{
			name:       "Range query (age >= 50)",
			oxiQuery:   map[string]any{"age": map[string]any{"$gte": 50}},
			mongoQuery: bson.M{"age": bson.M{"$gte": 50}},
		},
		{
			name:       "Compound (dept=Sales AND status=active)",
			oxiQuery:   map[string]any{"department": "Sales", "status": "active"},
			mongoQuery: bson.M{"department": "Sales", "status": "active"},
		},
		{
			name: "$or query (city=Tokyo OR city=Paris)",
			oxiQuery: map[string]any{"$or": []any{
				map[string]any{"city": "Tokyo"},
				map[string]any{"city": "Paris"},
			}},
			mongoQuery: bson.M{"$or": bson.A{
				bson.M{"city": "Tokyo"},
				bson.M{"city": "Paris"},
			}},
		},
		{
			name:       "$in query (country in [US, UK, JP])",
			oxiQuery:   map[string]any{"country": map[string]any{"$in": []any{"US", "UK", "JP"}}},
			mongoQuery: bson.M{"country": bson.M{"$in": bson.A{"US", "UK", "JP"}}},
		},
		{
			name:       "Range (salary 50000-100000)",
			oxiQuery:   map[string]any{"salary": map[string]any{"$gte": 50000, "$lte": 100000}},
			mongoQuery: bson.M{"salary": bson.M{"$gte": 50000, "$lte": 100000}},
		},
		{
			name:       "Boolean (verified=true)",
			oxiQuery:   map[string]any{"verified": true},
			mongoQuery: bson.M{"verified": true},
		},
		{
			name:       "Nested field (address.zip starts with 0)",
			oxiQuery:   map[string]any{"address.zip": map[string]any{"$gte": "00000", "$lt": "10000"}},
			mongoQuery: bson.M{"address.zip": bson.M{"$gte": "00000", "$lt": "10000"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// OxiDB
			oxiStart := time.Now()
			oxiDocs, oxiErr := oxiClient.Find(collection, tc.oxiQuery, nil)
			oxiDur := time.Since(oxiStart)

			// MongoDB
			mongoStart := time.Now()
			cursor, mongoErr := mongoColl.Find(ctx, tc.mongoQuery)
			var mongoDocs []bson.M
			if mongoErr == nil {
				mongoErr = cursor.All(ctx, &mongoDocs)
			}
			mongoDur := time.Since(mongoStart)

			oxiN := len(oxiDocs)
			mongoN := len(mongoDocs)

			fmt.Printf("  %-45s  OxiDB: %6d docs in %v  |  MongoDB: %6d docs in %v\n",
				tc.name, oxiN, oxiDur.Round(100*time.Microsecond), mongoN, mongoDur.Round(100*time.Microsecond))

			recordTiming("Queries", tc.name, oxiDur, mongoDur, oxiN, mongoN, oxiErr, mongoErr)
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

	mongoStart := time.Now()
	mongoColl.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "department", Value: 1}}},
		{Keys: bson.D{{Key: "age", Value: 1}}},
		{Keys: bson.D{{Key: "city", Value: 1}}},
		{Keys: bson.D{{Key: "salary", Value: 1}}},
	})
	mongoIdxDur := time.Since(mongoStart)

	fmt.Printf("  Index creation:  OxiDB: %v  |  MongoDB: %v\n\n", oxiIdxDur.Round(time.Millisecond), mongoIdxDur.Round(time.Millisecond))
	recordTiming("Indexes", "Create 4 indexes on 100K docs", oxiIdxDur, mongoIdxDur, 4, 4, nil, nil)

	tests := []struct {
		name       string
		oxiQuery   map[string]any
		mongoQuery bson.M
	}{
		{
			name:       "Indexed: department=Engineering",
			oxiQuery:   map[string]any{"department": "Engineering"},
			mongoQuery: bson.M{"department": "Engineering"},
		},
		{
			name:       "Indexed: age >= 60",
			oxiQuery:   map[string]any{"age": map[string]any{"$gte": 60}},
			mongoQuery: bson.M{"age": bson.M{"$gte": 60}},
		},
		{
			name:       "Indexed: city=Tokyo",
			oxiQuery:   map[string]any{"city": "Tokyo"},
			mongoQuery: bson.M{"city": "Tokyo"},
		},
		{
			name:       "Indexed: salary 80000-120000",
			oxiQuery:   map[string]any{"salary": map[string]any{"$gte": 80000, "$lte": 120000}},
			mongoQuery: bson.M{"salary": bson.M{"$gte": 80000, "$lte": 120000}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			oxiStart := time.Now()
			oxiDocs, oxiErr := oxiClient.Find(collection, tc.oxiQuery, nil)
			oxiDur := time.Since(oxiStart)

			mongoStart := time.Now()
			cursor, mongoErr := mongoColl.Find(ctx, tc.mongoQuery)
			var mongoDocs []bson.M
			if mongoErr == nil {
				mongoErr = cursor.All(ctx, &mongoDocs)
			}
			mongoDur := time.Since(mongoStart)

			fmt.Printf("  %-45s  OxiDB: %6d docs in %v  |  MongoDB: %6d docs in %v\n",
				tc.name, len(oxiDocs), oxiDur.Round(100*time.Microsecond), len(mongoDocs), mongoDur.Round(100*time.Microsecond))

			recordTiming("Indexed Queries", tc.name, oxiDur, mongoDur, len(oxiDocs), len(mongoDocs), oxiErr, mongoErr)
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
		name       string
		oxiQuery   map[string]any
		mongoQuery bson.M
	}{
		{
			name:       "Count all",
			oxiQuery:   map[string]any{},
			mongoQuery: bson.M{},
		},
		{
			name:       "Count dept=Engineering",
			oxiQuery:   map[string]any{"department": "Engineering"},
			mongoQuery: bson.M{"department": "Engineering"},
		},
		{
			name:       "Count age >= 50",
			oxiQuery:   map[string]any{"age": map[string]any{"$gte": 50}},
			mongoQuery: bson.M{"age": bson.M{"$gte": 50}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			oxiStart := time.Now()
			oxiN, oxiErr := oxiClient.Count(collection, tc.oxiQuery)
			oxiDur := time.Since(oxiStart)

			mongoStart := time.Now()
			mongoN, mongoErr := mongoColl.CountDocuments(ctx, tc.mongoQuery)
			mongoDur := time.Since(mongoStart)

			fmt.Printf("  %-45s  OxiDB: %6d in %v  |  MongoDB: %6d in %v\n",
				tc.name, oxiN, oxiDur.Round(100*time.Microsecond), mongoN, mongoDur.Round(100*time.Microsecond))

			recordTiming("Count", tc.name, oxiDur, mongoDur, oxiN, int(mongoN), oxiErr, mongoErr)
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

		mongoPipeline := mongo.Pipeline{
			{{Key: "$group", Value: bson.D{
				{Key: "_id", Value: "$department"},
				{Key: "avg_salary", Value: bson.D{{Key: "$avg", Value: "$salary"}}},
				{Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}},
			}}},
			{{Key: "$sort", Value: bson.D{{Key: "_id", Value: 1}}}},
		}

		oxiStart := time.Now()
		oxiRes, oxiErr := oxiClient.Aggregate(collection, oxiPipeline)
		oxiDur := time.Since(oxiStart)

		mongoStart := time.Now()
		cursor, mongoErr := mongoColl.Aggregate(ctx, mongoPipeline)
		var mongoRes []bson.M
		if mongoErr == nil {
			mongoErr = cursor.All(ctx, &mongoRes)
		}
		mongoDur := time.Since(mongoStart)

		fmt.Printf("  Group by dept (avg salary):  OxiDB: %d groups in %v  |  MongoDB: %d groups in %v\n",
			len(oxiRes), oxiDur.Round(100*time.Microsecond), len(mongoRes), mongoDur.Round(100*time.Microsecond))

		recordTiming("Aggregation", "Group by department, avg salary", oxiDur, mongoDur, len(oxiRes), len(mongoRes), oxiErr, mongoErr)
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

		mongoPipeline := mongo.Pipeline{
			{{Key: "$group", Value: bson.D{
				{Key: "_id", Value: "$city"},
				{Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}},
			}}},
			{{Key: "$sort", Value: bson.D{{Key: "count", Value: -1}}}},
			{{Key: "$limit", Value: 5}},
		}

		oxiStart := time.Now()
		oxiRes, oxiErr := oxiClient.Aggregate(collection, oxiPipeline)
		oxiDur := time.Since(oxiStart)

		mongoStart := time.Now()
		cursor, mongoErr := mongoColl.Aggregate(ctx, mongoPipeline)
		var mongoRes []bson.M
		if mongoErr == nil {
			mongoErr = cursor.All(ctx, &mongoRes)
		}
		mongoDur := time.Since(mongoStart)

		fmt.Printf("  Top 5 cities by count:       OxiDB: %d in %v  |  MongoDB: %d in %v\n",
			len(oxiRes), oxiDur.Round(100*time.Microsecond), len(mongoRes), mongoDur.Round(100*time.Microsecond))

		recordTiming("Aggregation", "Top 5 cities by count", oxiDur, mongoDur, len(oxiRes), len(mongoRes), oxiErr, mongoErr)
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

		mongoPipeline := mongo.Pipeline{
			{{Key: "$match", Value: bson.D{
				{Key: "status", Value: "active"},
				{Key: "department", Value: "Engineering"},
			}}},
			{{Key: "$group", Value: bson.D{
				{Key: "_id", Value: nil},
				{Key: "avg_score", Value: bson.D{{Key: "$avg", Value: "$score"}}},
				{Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}},
			}}},
		}

		oxiStart := time.Now()
		oxiRes, oxiErr := oxiClient.Aggregate(collection, oxiPipeline)
		oxiDur := time.Since(oxiStart)

		mongoStart := time.Now()
		cursor, mongoErr := mongoColl.Aggregate(ctx, mongoPipeline)
		var mongoRes []bson.M
		if mongoErr == nil {
			mongoErr = cursor.All(ctx, &mongoRes)
		}
		mongoDur := time.Since(mongoStart)

		fmt.Printf("  Active engineers avg score:   OxiDB: %v in %v  |  MongoDB: %v in %v\n",
			len(oxiRes), oxiDur.Round(100*time.Microsecond), len(mongoRes), mongoDur.Round(100*time.Microsecond))

		recordTiming("Aggregation", "Match + Group (active engineers)", oxiDur, mongoDur, len(oxiRes), len(mongoRes), oxiErr, mongoErr)
	})
}

// ═══════════════════════════════════════════════════════════════════════════
// Test: Disk Size & Memory Usage
// ═══════════════════════════════════════════════════════════════════════════

func TestResourceUsage(t *testing.T) {
	oxiCount, err := oxiClient.Count(collection, map[string]any{})
	if err != nil || oxiCount == 0 {
		t.Skip("No data — run TestBulkInsert first")
	}

	fmt.Println("\n  ┌─ Resource Usage ──────────────────────────────────────────")

	// ── OxiDB: get container stats via docker ──
	oxiDisk, oxiMem := getContainerStats("oxidb")
	mongoDisk, mongoMem := getContainerStats("mongodb")

	fmt.Printf("  │  OxiDB   — Disk: %s  Memory: %s\n", oxiDisk, oxiMem)
	fmt.Printf("  │  MongoDB — Disk: %s  Memory: %s\n", mongoDisk, mongoMem)
	fmt.Printf("  └───────────────────────────────────────────────────────────\n\n")

	recordTimingDetailed("Resources", "Disk usage (100K docs)", 0, 0, 0, 0, nil, nil, oxiDisk, mongoDisk)
	recordTimingDetailed("Resources", "Memory usage (100K docs)", 0, 0, 0, 0, nil, nil, oxiMem, mongoMem)
}

func getContainerStats(serviceName string) (disk string, mem string) {
	// Use docker compose to get container ID, then inspect
	// Disk: exec into container and du the data dir
	// Memory: docker stats --no-stream

	disk = "N/A"
	mem = "N/A"

	// Get container name
	containerName := getContainerName(serviceName)
	if containerName == "" {
		return
	}

	// Memory from docker stats
	mem = dockerExec("docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", containerName)

	// Disk usage
	switch serviceName {
	case "oxidb":
		disk = dockerExec("docker", "exec", containerName, "du", "-sh", "/data")
	case "mongodb":
		disk = dockerExec("docker", "exec", containerName, "du", "-sh", "/data/db")
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

func BenchmarkMongoDBInsert(b *testing.B) {
	col := mongoDB.Collection(fmt.Sprintf("bench_insert_%d", time.Now().UnixNano()%100000))
	defer col.Drop(ctx)
	rng := rand.New(rand.NewSource(99))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		doc := genDoc(rng, i)
		_, err := col.InsertOne(ctx, doc)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "docs/s")
}

func BenchmarkOxiDBFind(b *testing.B) {
	// Uses the 100K seeded collection
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := oxiClient.Find(collection, map[string]any{"department": "Engineering"}, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "queries/s")
}

func BenchmarkMongoDBFind(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cursor, err := mongoColl.Find(ctx, bson.M{"department": "Engineering"})
		if err != nil {
			b.Fatal(err)
		}
		var results []bson.M
		if err := cursor.All(ctx, &results); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "queries/s")
}

// ═══════════════════════════════════════════════════════════════════════════
// HTML Report
// ═══════════════════════════════════════════════════════════════════════════

type SystemInfo struct {
	OS       string
	Arch     string
	CPU      string
	Cores    int
	RAM      string
	GoVer    string
	Docker   string
	MongoVer string
}

func getSystemInfo() SystemInfo {
	info := SystemInfo{
		OS:    runtime.GOOS + "/" + runtime.GOARCH,
		Arch:  runtime.GOARCH,
		Cores: runtime.NumCPU(),
		GoVer: runtime.Version(),
	}

	// CPU model
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

	// Docker version
	if out, err := exec.Command("docker", "version", "--format", "{{.Server.Version}}").Output(); err == nil {
		info.Docker = strings.TrimSpace(string(out))
	}

	// MongoDB version from running container
	if mongoDB != nil {
		var result bson.M
		if err := mongoDB.RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&result); err == nil {
			if ver, ok := result["version"].(string); ok {
				info.MongoVer = ver
			}
		}
	}

	return info
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

	oxiWins, mongoWins := 0, 0
	for _, e := range timings {
		if e.MongoDur == 0 || e.OxiDur == 0 {
			continue
		}
		if e.OxiDur < e.MongoDur {
			oxiWins++
		} else if e.MongoDur < e.OxiDur {
			mongoWins++
		}
	}

	// Category icons
	catIcon := map[string]string{
		"Bulk Insert":     "&#9654;",
		"Queries":         "&#128269;",
		"Indexed Queries": "&#9889;",
		"Count Queries":   "&#35;",
		"Aggregation":     "&#8721;",
		"Resources":       "&#9881;",
	}

	var sb strings.Builder
	sb.WriteString(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>OxiDB vs MongoDB — Benchmark</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700;800&family=Outfit:wght@300;400;500;600;700;800;900&display=swap');

*{margin:0;padding:0;box-sizing:border-box}

:root{
  --bg:#06080c;--surface:#0c1018;--card:#111820;--border:#1a2332;
  --text:#c8d6e5;--dim:#5a6a7e;--bright:#e8f0f8;
  --oxi:#22d666;--oxi-dim:#1a4a2e;--oxi-glow:rgba(34,214,102,0.12);
  --mongo:#00a86b;--mongo-dim:#0a3024;--mongo-glow:rgba(0,168,107,0.12);
  --accent:#f0c040;--red:#e84057;
}

body{
  font-family:'Outfit',system-ui,sans-serif;
  background:var(--bg);color:var(--text);
  min-height:100vh;overflow-x:hidden;
}

/* Grain overlay */
body::before{
  content:'';position:fixed;inset:0;z-index:9999;pointer-events:none;
  opacity:0.025;
  background-image:url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)'/%3E%3C/svg%3E");
}

/* Top accent line */
body::after{
  content:'';position:fixed;top:0;left:0;right:0;height:2px;z-index:100;
  background:linear-gradient(90deg,transparent,var(--oxi),var(--accent),var(--mongo),transparent);
}

.wrap{max-width:1100px;margin:0 auto;padding:48px 24px 80px}

/* Header */
header{margin-bottom:56px;position:relative}
header h1{
  font-family:'JetBrains Mono',monospace;font-size:13px;font-weight:500;
  color:var(--dim);letter-spacing:3px;text-transform:uppercase;
  margin-bottom:12px;
}
header .title{
  font-size:42px;font-weight:800;letter-spacing:-1px;
  background:linear-gradient(135deg,var(--oxi) 0%,#40e8a0 40%,var(--accent) 100%);
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;
  background-clip:text;
  line-height:1.1;margin-bottom:16px;
}
header .meta{
  font-family:'JetBrains Mono',monospace;font-size:12px;color:var(--dim);
  display:flex;gap:24px;flex-wrap:wrap;
}
header .meta span::before{content:'› ';color:var(--border)}

/* Score strip */
.score-strip{
  display:grid;grid-template-columns:1fr auto 1fr;align-items:center;gap:0;
  background:var(--surface);border:1px solid var(--border);border-radius:16px;
  padding:32px 40px;margin-bottom:48px;position:relative;overflow:hidden;
}
.score-strip::before{
  content:'';position:absolute;inset:0;
  background:linear-gradient(135deg,var(--oxi-glow),transparent 50%,var(--mongo-glow));
  pointer-events:none;
}
.score-side{text-align:center;position:relative}
.score-side .db-label{
  font-family:'JetBrains Mono',monospace;font-size:11px;font-weight:600;
  letter-spacing:2px;text-transform:uppercase;margin-bottom:12px;
}
.score-side .db-label.oxi{color:var(--oxi)}
.score-side .db-label.mongo{color:var(--mongo)}
.score-num{font-size:72px;font-weight:900;line-height:1;letter-spacing:-3px}
.score-num.oxi{color:var(--oxi)}
.score-num.mongo{color:var(--mongo)}
.score-sub{font-size:13px;color:var(--dim);margin-top:6px;font-weight:500}
.score-vs{
  font-family:'JetBrains Mono',monospace;font-size:14px;font-weight:700;
  color:var(--border);padding:0 24px;
  display:flex;flex-direction:column;align-items:center;gap:4px;
}
.score-vs::before,.score-vs::after{
  content:'';width:1px;height:32px;background:var(--border);
}

/* Category sections */
.cat{margin-bottom:36px}
.cat-head{
  display:flex;align-items:center;gap:10px;
  padding:14px 0;margin-bottom:2px;
  border-bottom:1px solid var(--border);
}
.cat-icon{
  width:28px;height:28px;display:flex;align-items:center;justify-content:center;
  background:var(--card);border:1px solid var(--border);border-radius:8px;
  font-size:14px;flex-shrink:0;
}
.cat-name{font-size:16px;font-weight:700;color:var(--bright)}
.cat-count{
  font-family:'JetBrains Mono',monospace;font-size:11px;
  color:var(--dim);margin-left:auto;
}

/* Rows */
.row{
  display:grid;grid-template-columns:1fr 300px 100px;align-items:center;gap:16px;
  padding:14px 0;border-bottom:1px solid rgba(26,35,50,0.6);
  transition:background 0.15s;
}
.row:last-child{border-bottom:none}
.row:hover{background:rgba(34,214,102,0.02)}

.row-label{font-size:14px;font-weight:500;color:var(--text)}
.row-label .row-detail{
  font-family:'JetBrains Mono',monospace;font-size:11px;
  color:var(--dim);display:block;margin-top:3px;
}
.row-label .row-counts{
  font-family:'JetBrains Mono',monospace;font-size:11px;
  color:var(--dim);
}

/* Duel bar */
.duel{display:flex;flex-direction:column;gap:5px}
.duel-row{display:flex;align-items:center;gap:8px}
.duel-label{
  font-family:'JetBrains Mono',monospace;font-size:10px;font-weight:600;
  width:50px;text-align:right;flex-shrink:0;
}
.duel-label.oxi{color:var(--oxi)}
.duel-label.mongo{color:var(--mongo)}
.duel-track{flex:1;height:18px;background:var(--bg);border-radius:3px;overflow:hidden;position:relative}
.duel-fill{
  height:100%;border-radius:3px;position:relative;
  transition:width 0.8s cubic-bezier(0.16,1,0.3,1);
}
.duel-fill.oxi{background:linear-gradient(90deg,var(--oxi-dim),var(--oxi))}
.duel-fill.mongo{background:linear-gradient(90deg,var(--mongo-dim),var(--mongo))}
.duel-fill.winner{box-shadow:0 0 12px rgba(34,214,102,0.3)}
.duel-fill.winner.mongo{box-shadow:0 0 12px rgba(0,168,107,0.3)}
.duel-time{
  font-family:'JetBrains Mono',monospace;font-size:11px;font-weight:500;
  color:var(--dim);width:70px;flex-shrink:0;
}

/* Result badge */
.result{text-align:right}
.badge{
  display:inline-block;font-family:'JetBrains Mono',monospace;
  font-size:11px;font-weight:700;padding:3px 10px;border-radius:4px;
  letter-spacing:0.5px;
}
.badge-oxi{background:var(--oxi-dim);color:var(--oxi)}
.badge-mongo{background:var(--mongo-dim);color:var(--mongo)}
.badge-tie{background:var(--card);color:var(--dim)}
.speedup{
  font-family:'JetBrains Mono',monospace;font-size:11px;
  color:var(--accent);display:block;margin-top:3px;
}

/* Resource section */
.res-grid{display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-top:8px}
.res-card{
  background:var(--surface);border:1px solid var(--border);border-radius:12px;
  padding:24px;position:relative;overflow:hidden;
}
.res-card::before{
  content:'';position:absolute;top:0;left:0;right:0;height:2px;
}
.res-card.disk::before{background:linear-gradient(90deg,var(--oxi),var(--mongo))}
.res-card.mem::before{background:linear-gradient(90deg,var(--accent),var(--mongo))}
.res-title{
  font-family:'JetBrains Mono',monospace;font-size:11px;font-weight:600;
  color:var(--dim);letter-spacing:1.5px;text-transform:uppercase;margin-bottom:20px;
}
.res-bars{display:flex;flex-direction:column;gap:12px}
.res-row{display:flex;align-items:center;gap:12px}
.res-db{
  font-family:'JetBrains Mono',monospace;font-size:11px;font-weight:600;
  width:60px;flex-shrink:0;
}
.res-db.oxi{color:var(--oxi)}
.res-db.mongo{color:var(--mongo)}
.res-track{flex:1;height:24px;background:var(--bg);border-radius:4px;overflow:hidden}
.res-fill{height:100%;border-radius:4px;display:flex;align-items:center;padding:0 10px;min-width:fit-content}
.res-fill.oxi{background:linear-gradient(90deg,var(--oxi-dim),rgba(34,214,102,0.35))}
.res-fill.mongo{background:linear-gradient(90deg,var(--mongo-dim),rgba(0,168,107,0.35))}
.res-val{
  font-family:'JetBrains Mono',monospace;font-size:11px;font-weight:600;
  color:var(--bright);white-space:nowrap;
}

/* Methodology */
.methodology{
  font-family:'JetBrains Mono',monospace;font-size:12px;line-height:1.7;
  color:var(--dim);margin-bottom:32px;padding:20px 24px;
  background:var(--surface);border:1px solid var(--border);border-radius:12px;
}
.methodology p{margin-bottom:8px}
.methodology p:last-child{margin-bottom:0}
.methodology strong{color:var(--text);font-weight:600}
.methodology .hl-oxi{color:var(--oxi)}
.methodology .hl-mongo{color:var(--mongo)}

/* Environment panel */
.env-panel{
  background:var(--surface);border:1px solid var(--border);border-radius:12px;
  padding:24px 28px;margin-bottom:36px;position:relative;overflow:hidden;
}
.env-panel::before{
  content:'';position:absolute;top:0;left:0;right:0;height:2px;
  background:linear-gradient(90deg,var(--oxi),var(--border),var(--mongo));
}
.env-title{
  font-family:'JetBrains Mono',monospace;font-size:11px;font-weight:600;
  color:var(--dim);letter-spacing:2px;text-transform:uppercase;margin-bottom:16px;
  display:flex;align-items:center;gap:8px;
}
.env-title::before{content:'&#9881;';font-size:13px}
.env-grid{
  display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:12px 24px;
}
.env-item{display:flex;flex-direction:column;gap:2px}
.env-label{
  font-family:'JetBrains Mono',monospace;font-size:10px;font-weight:600;
  color:var(--dim);letter-spacing:1px;text-transform:uppercase;
}
.env-value{
  font-family:'JetBrains Mono',monospace;font-size:13px;font-weight:500;
  color:var(--bright);
}

/* Footer */
footer{
  margin-top:56px;padding-top:24px;border-top:1px solid var(--border);
  display:flex;justify-content:space-between;align-items:center;
  font-family:'JetBrains Mono',monospace;font-size:11px;color:var(--dim);
}
footer .oxi-tag{color:var(--oxi)}

/* Error state */
.err{color:var(--red);font-family:'JetBrains Mono',monospace;font-size:11px}

/* Animate bars on load */
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
  <div class="title">OxiDB vs MongoDB</div>
  <div class="meta">
    <span>` + fmt.Sprintf("%d", totalDocs/1000) + `K documents</span>
    <span>` + time.Now().Format("2006-01-02 15:04") + `</span>
    <span>` + fmt.Sprintf("%d tests", len(timings)) + `</span>
    <span>MsgPack wire protocol</span>
  </div>
</header>

<div class="methodology">
  <p>A Go test binary (<strong>go test</strong>) drives both databases through their official client libraries.
  <span class="hl-oxi">OxiDB</span> and <span class="hl-mongo">MongoDB 7</span> each run in isolated
  Docker containers with <strong>tmpfs</strong> storage (pure RAM — no disk I/O variance).
  The test program connects to both over <strong>localhost TCP</strong>, inserts <strong>` + fmt.Sprintf("%dK", totalDocs/1000) + ` identical documents</strong>
  into each, then runs the same queries sequentially — first OxiDB, then MongoDB — measuring wall-clock time for each.
  OxiDB uses the <strong>MsgPack</strong> binary wire protocol; MongoDB uses its native BSON/Wire protocol.
  Indexes are created after insertion and timed separately. Each test is a single cold run (no warm-up iterations).</p>
</div>

<div class="env-panel">
  <div class="env-title">Test Environment</div>
  <div class="env-grid">
    <div class="env-item"><div class="env-label">CPU</div><div class="env-value">` + sysInfo.CPU + `</div></div>
    <div class="env-item"><div class="env-label">Cores</div><div class="env-value">` + fmt.Sprintf("%d", sysInfo.Cores) + `</div></div>
    <div class="env-item"><div class="env-label">Memory</div><div class="env-value">` + sysInfo.RAM + `</div></div>
    <div class="env-item"><div class="env-label">OS / Arch</div><div class="env-value">` + sysInfo.OS + `</div></div>
    <div class="env-item"><div class="env-label">Go</div><div class="env-value">` + sysInfo.GoVer + `</div></div>
    <div class="env-item"><div class="env-label">Docker</div><div class="env-value">` + sysInfo.Docker + `</div></div>
    <div class="env-item"><div class="env-label">MongoDB</div><div class="env-value">` + sysInfo.MongoVer + `</div></div>
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
    <div class="db-label mongo">MongoDB</div>
    <div class="score-num mongo">` + fmt.Sprintf("%d", mongoWins) + `</div>
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
			// Special resource section
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
				mongoVal := e.MongoDetail
				if oxiVal == "" {
					oxiVal = "N/A"
				}
				if mongoVal == "" {
					mongoVal = "N/A"
				}
				// Parse numeric portion for bar widths
				oxiNum := parseSize(oxiVal)
				mongoNum := parseSize(mongoVal)
				maxNum := oxiNum
				if mongoNum > maxNum {
					maxNum = mongoNum
				}
				oxiPct, mongoPct := 10.0, 10.0
				if maxNum > 0 {
					oxiPct = oxiNum / maxNum * 100
					mongoPct = mongoNum / maxNum * 100
				}

				sb.WriteString(fmt.Sprintf(`<div class="res-card %s">
  <div class="res-title">%s</div>
  <div class="res-bars">
    <div class="res-row">
      <div class="res-db oxi">OxiDB</div>
      <div class="res-track"><div class="res-fill oxi" style="--w:%.0f%%;width:0"><span class="res-val">%s</span></div></div>
    </div>
    <div class="res-row">
      <div class="res-db mongo">Mongo</div>
      <div class="res-track"><div class="res-fill mongo" style="--w:%.0f%%;width:0"><span class="res-val">%s</span></div></div>
    </div>
  </div>
</div>
`, cardClass, e.Name, oxiPct, oxiVal, mongoPct, mongoVal))
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
			mongoStr := fmtDur(e.MongoDur)

			// Compute bar widths
			maxDur := e.OxiDur
			if e.MongoDur > maxDur {
				maxDur = e.MongoDur
			}
			oxiPct, mongoPct := 0.0, 0.0
			if maxDur > 0 {
				oxiPct = float64(e.OxiDur) / float64(maxDur) * 100
				mongoPct = float64(e.MongoDur) / float64(maxDur) * 100
			}

			winBadge := `<span class="badge badge-tie">TIE</span>`
			speedupHTML := ""
			oxiWinner, mongoWinner := "", ""
			if e.MongoDur > 0 && e.OxiDur > 0 {
				if e.OxiDur < e.MongoDur {
					winBadge = `<span class="badge badge-oxi">OxiDB</span>`
					factor := float64(e.MongoDur) / float64(e.OxiDur)
					speedupHTML = fmt.Sprintf(`<span class="speedup">%.1fx faster</span>`, factor)
					oxiWinner = " winner"
				} else if e.MongoDur < e.OxiDur {
					winBadge = `<span class="badge badge-mongo">MongoDB</span>`
					factor := float64(e.OxiDur) / float64(e.MongoDur)
					speedupHTML = fmt.Sprintf(`<span class="speedup">%.1fx faster</span>`, factor)
					mongoWinner = " winner"
				}
			}

			if e.OxiErr != "" {
				winBadge = `<span class="err">` + e.OxiErr + `</span>`
			}

			detailHTML := ""
			if e.OxiDetail != "" || e.MongoDetail != "" {
				detailHTML = fmt.Sprintf(`<span class="row-detail">%s vs %s</span>`, e.OxiDetail, e.MongoDetail)
			}
			if e.OxiCount > 0 || e.MongoCount > 0 {
				detailHTML += fmt.Sprintf(` <span class="row-counts">%d / %d docs</span>`, e.OxiCount, e.MongoCount)
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
      <div class="duel-label mongo">Mongo</div>
      <div class="duel-track"><div class="duel-fill mongo%s" style="--w:%.0f%%;width:0"></div></div>
      <div class="duel-time">%s</div>
    </div>
  </div>
  <div class="result">%s%s</div>
</div>
`, e.Name, detailHTML, oxiWinner, oxiPct, oxiStr, mongoWinner, mongoPct, mongoStr, winBadge, speedupHTML))
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
	fmt.Printf("  OxiDB wins: %d | MongoDB wins: %d | Ties/N-A: %d\n", oxiWins, mongoWins, countResourceEntries(timings))
	fmt.Printf("══════════════════════════════════════════════════════════════\n")

	// JSON summary
	summary := map[string]any{
		"total":      len(timings),
		"oxi_wins":   oxiWins,
		"mongo_wins": mongoWins,
		"timings":    timings,
	}
	jsonBytes, _ := json.MarshalIndent(summary, "", "  ")
	os.WriteFile("report.json", jsonBytes, 0644)
}

func parseSize(s string) float64 {
	s = strings.TrimSpace(s)
	// Extract first numeric-like token: "38M\t/data" → 38, "674.1MiB / 8.2GiB" → 674.1
	for _, part := range strings.Fields(s) {
		num := ""
		for _, c := range part {
			if (c >= '0' && c <= '9') || c == '.' {
				num += string(c)
			} else {
				break
			}
		}
		if num != "" {
			v := 0.0
			fmt.Sscanf(num, "%f", &v)
			return v
		}
	}
	return 0
}

func countResourceEntries(entries []TimingEntry) int {
	n := 0
	for _, e := range entries {
		if e.MongoDur == 0 && e.OxiDur == 0 {
			n++
		}
	}
	return n
}

func fmtDur(d time.Duration) string {
	if d == 0 {
		return "—"
	}
	if d < time.Millisecond {
		return d.Round(time.Microsecond).String()
	}
	if d < time.Second {
		return d.Round(100 * time.Microsecond).String()
	}
	return d.Round(time.Millisecond).String()
}

func (e TimingEntry) MarshalJSON() ([]byte, error) {
	type alias struct {
		Category    string  `json:"category"`
		Name        string  `json:"name"`
		OxiMs       float64 `json:"oxi_ms"`
		MongoMs     float64 `json:"mongo_ms"`
		Speedup     string  `json:"speedup"`
		OxiCount    int     `json:"oxi_count"`
		MongoCount  int     `json:"mongo_count"`
		Winner      string  `json:"winner"`
		OxiErr      string  `json:"oxi_err,omitempty"`
		MongoErr    string  `json:"mongo_err,omitempty"`
		OxiDetail   string  `json:"oxi_detail,omitempty"`
		MongoDetail string  `json:"mongo_detail,omitempty"`
	}
	winner := "tie"
	speedup := "1.0x"
	if e.MongoDur == 0 && e.OxiDur == 0 {
		winner = "n/a"
		speedup = "n/a"
	} else if e.MongoDur == 0 {
		winner = "n/a"
		speedup = "n/a"
	} else if e.OxiDur < e.MongoDur {
		winner = "oxidb"
		speedup = fmt.Sprintf("%.1fx", float64(e.MongoDur)/float64(e.OxiDur))
	} else if e.MongoDur < e.OxiDur {
		winner = "mongodb"
		speedup = fmt.Sprintf("%.1fx", float64(e.OxiDur)/float64(e.MongoDur))
	}
	return json.Marshal(alias{
		Category:    e.Category,
		Name:        e.Name,
		OxiMs:       float64(e.OxiDur.Microseconds()) / 1000.0,
		MongoMs:     float64(e.MongoDur.Microseconds()) / 1000.0,
		Speedup:     speedup,
		OxiCount:    e.OxiCount,
		MongoCount:  e.MongoCount,
		Winner:      winner,
		OxiErr:      e.OxiErr,
		MongoErr:    e.MongoErr,
		OxiDetail:   e.OxiDetail,
		MongoDetail: e.MongoDetail,
	})
}
