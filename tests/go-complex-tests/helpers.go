package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/parisxmas/OxiDB/clients/go/oxidb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ─── Config ────────────────────────────────────────────────────────

var (
	oxidbHost = envOr("OXIDB_HOST", "127.0.0.1")
	oxidbPort = envInt("OXIDB_PORT", 14888)
	mongoURI  = envOr("MONGO_URI", "mongodb://127.0.0.1:27099")
	docCount  = envInt("DOC_COUNT", 100000)
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
		rv := float64(oxiDur) / float64(monDur)
		if rv < 1 {
			ratio = fmt.Sprintf("  OxiDB %.1fx faster", 1/rv)
		} else {
			ratio = fmt.Sprintf("  MongoDB %.1fx faster", rv)
		}
	}
	fmt.Printf("  %-40s OxiDB: %8s  MongoDB: %8s%s\n", name, formatDur(oxiDur), formatDur(monDur), ratio)
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

func printReport() {
	fmt.Println()
	fmt.Println("═══════════════════════════════════════════════════════════════════════════════")
	fmt.Printf("%-12s %-42s %8s %8s %8s\n", "Category", "Operation", "OxiDB", "MongoDB", "Ratio")
	fmt.Println("───────────────────────────────────────────────────────────────────────────────")
	oxiWins := 0
	for _, r := range results {
		ratio := ""
		if r.OxiDB > 0 && r.MongoDB > 0 {
			rv := float64(r.MongoDB) / float64(r.OxiDB)
			if rv >= 1.0 {
				ratio = fmt.Sprintf("%.1fx", rv)
				oxiWins++
			} else {
				ratio = fmt.Sprintf("%.1fx", 1/rv)
			}
		}
		fmt.Printf("%-12s %-42s %8s %8s %8s\n", r.Category, r.Name, formatDur(r.OxiDB), formatDur(r.MongoDB), ratio)
	}
	fmt.Println("───────────────────────────────────────────────────────────────────────────────")
	fmt.Printf("OxiDB faster in %d/%d tests\n", oxiWins, len(results))
	fmt.Println("═══════════════════════════════════════════════════════════════════════════════")

	// Save JSON report
	data, _ := json.MarshalIndent(results, "", "  ")
	os.WriteFile("report.json", data, 0644)
	fmt.Println("Report saved to report.json")
}

// ─── Data generators ───────────────────────────────────────────────

var (
	regions  = []string{"US-EAST", "US-WEST", "EU-WEST", "EU-EAST", "APAC"}
	depts    = []string{"engineering", "sales", "marketing", "support", "hr", "finance", "ops", "legal"}
	statuses = []string{"active", "inactive", "onleave"}
	cities   = []string{"New York", "San Francisco", "London", "Berlin", "Tokyo", "Sydney", "Toronto", "Mumbai"}
	tags     = []string{"urgent", "review", "approved", "pending", "archived", "flagged"}
)

func makeDoc(i int) map[string]any {
	nTags := 1 + rand.Intn(4)
	docTags := make([]string, nTags)
	for t := 0; t < nTags; t++ {
		docTags[t] = tags[rand.Intn(len(tags))]
	}
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
		"tags":       docTags,
		"address": map[string]any{
			"city":    cities[rand.Intn(len(cities))],
			"country": regions[rand.Intn(len(regions))],
			"zip":     fmt.Sprintf("%05d", rand.Intn(100000)),
		},
	}
}

func makeBson(i int) bson.M {
	d := makeDoc(i)
	return toBsonM(d)
}

func toBsonM(m map[string]any) bson.M {
	b := bson.M{}
	for k, v := range m {
		if nested, ok := v.(map[string]any); ok {
			b[k] = toBsonM(nested)
		} else {
			b[k] = v
		}
	}
	return b
}

// ─── Connection setup ──────────────────────────────────────────────

type Env struct {
	Oxi   *oxidb.Client
	Mcoll *mongo.Collection
	Mdb   *mongo.Database
	Ctx   context.Context
}

func setup(collName string) *Env {
	ctx := context.Background()

	oxi, err := oxidb.Connect(oxidbHost, oxidbPort, 30*time.Second)
	if err != nil {
		fmt.Printf("FATAL: OxiDB connect: %v\n", err)
		os.Exit(1)
	}
	oxi.Ping()

	mopts := options.Client().ApplyURI(mongoURI)
	mc, err := mongo.Connect(ctx, mopts)
	if err != nil {
		fmt.Printf("FATAL: MongoDB connect: %v\n", err)
		os.Exit(1)
	}
	mc.Ping(ctx, nil)

	mdb := mc.Database("bench")

	return &Env{
		Oxi:   oxi,
		Mcoll: mdb.Collection(collName),
		Mdb:   mdb,
		Ctx:   ctx,
	}
}

// newOxiClient creates a fresh OxiDB client connection.
func newOxiClient() *oxidb.Client {
	c, err := oxidb.Connect(oxidbHost, oxidbPort, 10*time.Second)
	if err != nil {
		fmt.Printf("FATAL: OxiDB connect: %v\n", err)
		os.Exit(1)
	}
	return c
}

func seedData(env *Env, coll string, count int) {
	env.Oxi.DropCollection(coll)
	env.Mcoll.Drop(env.Ctx)

	fmt.Printf("  Seeding %dk documents...", count/1000)
	t0 := time.Now()
	for offset := 0; offset < count; offset += batchSize {
		end := offset + batchSize
		if end > count {
			end = count
		}
		oxiBatch := make([]map[string]any, 0, end-offset)
		monBatch := make([]interface{}, 0, end-offset)
		for i := offset; i < end; i++ {
			doc := makeDoc(i)
			oxiBatch = append(oxiBatch, doc)
			monBatch = append(monBatch, makeBson(i))
		}
		env.Oxi.InsertMany(coll, oxiBatch)
		env.Mcoll.InsertMany(env.Ctx, monBatch)
		if (offset/batchSize+1)%10 == 0 || end == count {
			fmt.Printf("\r  Seeding %dk documents... %dk/%dk (%.1fs)", count/1000, end/1000, count/1000, time.Since(t0).Seconds())
		}
	}
	fmt.Println()

	// Create indexes
	fmt.Printf("  Creating indexes...")
	indexFields := []string{"emp_id", "department", "region", "status", "salary", "age", "hired_year", "city"}
	for _, f := range indexFields {
		env.Oxi.CreateIndex(coll, f)
		env.Mcoll.Indexes().CreateOne(env.Ctx, mongo.IndexModel{Keys: bson.D{{Key: f, Value: 1}}})
	}
	fmt.Printf(" done (%.1fs)\n", time.Since(t0).Seconds())
}

func intPtr(n int) *int { return &n }
