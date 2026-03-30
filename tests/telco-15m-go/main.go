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
	custCount = envInt("CUST_COUNT", 15000000)
	batchSize = envInt("BATCH_SIZE", 10000)
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" { return v }
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		n, _ := strconv.Atoi(v)
		if n > 0 { return n }
	}
	return def
}

// Telco data generators
var (
	plans     = []string{"Starter", "Basic", "Standard", "Premium", "Unlimited", "Business", "Enterprise"}
	cities    = []string{"Istanbul", "Ankara", "Izmir", "Bursa", "Antalya", "Adana", "Konya", "Gaziantep", "Mersin", "Kayseri", "Eskisehir", "Trabzon", "Samsun", "Diyarbakir", "Malatya"}
	regions   = []string{"Marmara", "Ege", "Akdeniz", "Ic Anadolu", "Karadeniz", "Dogu Anadolu", "Guneydogu"}
	statuses  = []string{"active", "suspended", "churned", "pending"}
	devices   = []string{"iPhone 15", "iPhone 14", "Samsung S24", "Samsung S23", "Samsung A54", "Xiaomi 14", "Xiaomi Redmi Note 13", "Oppo A79", "Google Pixel 8", "Huawei P60", "OnePlus 12"}
	channels  = []string{"online", "store", "dealer", "callcenter", "app"}
	segments  = []string{"prepaid", "postpaid", "hybrid"}
)

func makeCustomer(i int) map[string]any {
	rng := rand.New(rand.NewSource(int64(i)))
	city := cities[rng.Intn(len(cities))]

	// Assign region based on city
	region := regions[rng.Intn(len(regions))]
	switch city {
	case "Istanbul", "Bursa":
		region = "Marmara"
	case "Izmir":
		region = "Ege"
	case "Antalya", "Mersin", "Adana":
		region = "Akdeniz"
	case "Ankara", "Eskisehir", "Konya", "Kayseri":
		region = "Ic Anadolu"
	case "Trabzon", "Samsun":
		region = "Karadeniz"
	case "Diyarbakir", "Malatya":
		region = "Dogu Anadolu"
	case "Gaziantep":
		region = "Guneydogu"
	}

	monthlyBill := 50 + rng.Intn(950)       // 50-999 TL
	dataUsageGB := rng.Float64() * 100       // 0-100 GB
	callMinutes := rng.Intn(3000)            // 0-3000 min
	smsCount := rng.Intn(500)                // 0-500

	return map[string]any{
		"msisdn":        fmt.Sprintf("905%09d", 100000000+i),
		"tc_kimlik":     fmt.Sprintf("%011d", 10000000000+int64(i)),
		"name":          fmt.Sprintf("Musteri %d", i),
		"city":          city,
		"region":        region,
		"plan":          plans[rng.Intn(len(plans))],
		"segment":       segments[rng.Intn(len(segments))],
		"status":        statuses[rng.Intn(len(statuses))],
		"device":        devices[rng.Intn(len(devices))],
		"channel":       channels[rng.Intn(len(channels))],
		"monthly_bill":  monthlyBill,
		"data_usage_gb": fmt.Sprintf("%.1f", dataUsageGB),
		"call_minutes":  callMinutes,
		"sms_count":     smsCount,
		"loyalty_years": rng.Intn(20),
		"contract_end":  fmt.Sprintf("2025-%02d-%02d", 1+rng.Intn(12), 1+rng.Intn(28)),
		"nps_score":     rng.Intn(11), // 0-10
		"has_fiber":     rng.Intn(4) == 0, // 25% have fiber
	}
}

func connect() *oxidb.Client {
	c, err := oxidb.Connect(host, port, 60*time.Second)
	if err != nil {
		fmt.Printf("FATAL: connect: %v\n", err)
		os.Exit(1)
	}
	return c
}

func formatDur(d time.Duration) string {
	if d < time.Millisecond { return fmt.Sprintf("%dµs", d.Microseconds()) }
	if d < time.Second { return fmt.Sprintf("%dms", d.Milliseconds()) }
	return d.Round(time.Millisecond).String()
}

func main() {
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Printf("║   Telco Scenario: %dM Customers                            ║\n", custCount/1000000)
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Println()

	c := connect()
	defer c.Close()

	coll := "customers"
	c.DropCollection(coll)
	time.Sleep(time.Second)

	// ═══ Phase 1: Seed 15M customers ═══
	fmt.Printf("═══ Phase 1: Insert %dM customers (batch %d) ═══\n", custCount/1000000, batchSize)
	t0 := time.Now()
	for offset := 0; offset < custCount; offset += batchSize {
		end := offset + batchSize
		if end > custCount { end = custCount }
		batch := make([]map[string]any, 0, end-offset)
		for i := offset; i < end; i++ {
			batch = append(batch, makeCustomer(i))
		}
		_, err := c.InsertMany(coll, batch)
		if err != nil {
			fmt.Printf("\n  Insert error at %d: %v\n", offset, err)
			break
		}
		if (offset/batchSize+1)%50 == 0 || end == custCount {
			elapsed := time.Since(t0).Seconds()
			rate := float64(end) / elapsed
			eta := float64(custCount-end) / rate
			fmt.Printf("\r  Inserting... %dM/%dM (%.0fs, %.0f/sec, ETA %.0fs)    ",
				end/1000000, custCount/1000000, elapsed, rate, eta)
		}
	}
	insertDur := time.Since(t0)
	fmt.Printf("\r  Insert: %dM docs in %s (%.0f docs/sec)                              \n\n",
		custCount/1000000, formatDur(insertDur), float64(custCount)/insertDur.Seconds())

	// ═══ Phase 2: Create indexes ═══
	fmt.Println("═══ Phase 2: Create indexes ═══")
	indexes := []string{"msisdn", "tc_kimlik", "city", "region", "plan", "segment", "status", "monthly_bill", "loyalty_years", "nps_score"}
	t0 = time.Now()
	for _, f := range indexes {
		ft := time.Now()
		c.CreateIndex(coll, f)
		fmt.Printf("  %-20s %s\n", f, formatDur(time.Since(ft)))
	}
	fmt.Printf("  Total: %d indexes in %s\n\n", len(indexes), formatDur(time.Since(t0)))

	// ═══ Phase 3: Real-world telco queries ═══
	fmt.Println("═══ Phase 3: Telco Queries ═══")

	query := func(name string, q map[string]any, opts *oxidb.FindOptions) {
		t0 := time.Now()
		res, _ := c.Find(coll, q, opts)
		fmt.Printf("  %-55s %8s  (%d rows)\n", name, formatDur(time.Since(t0)), len(res))
	}

	queryCount := func(name string, q map[string]any) {
		t0 := time.Now()
		n, _ := c.Count(coll, q)
		fmt.Printf("  %-55s %8s  (count=%d)\n", name, formatDur(time.Since(t0)), n)
	}

	queryOne := func(name string, q map[string]any) {
		t0 := time.Now()
		doc, _ := c.FindOne(coll, q)
		found := doc != nil
		fmt.Printf("  %-55s %8s  (found=%v)\n", name, formatDur(time.Since(t0)), found)
	}

	queryAgg := func(name string, pipeline []map[string]any) {
		t0 := time.Now()
		res, _ := c.Aggregate(coll, pipeline)
		fmt.Printf("  %-55s %8s  (%d groups)\n", name, formatDur(time.Since(t0)), len(res))
	}

	fmt.Println("  — Customer Lookup —")
	queryOne("Find by MSISDN (exact)", map[string]any{"msisdn": "905107500000"})
	queryOne("Find by TC Kimlik", map[string]any{"tc_kimlik": "10005000000"})

	fmt.Println("\n  — Segment Analysis —")
	queryCount("Active customers", map[string]any{"status": "active"})
	queryCount("Churned customers", map[string]any{"status": "churned"})
	queryCount("Premium plan users", map[string]any{"plan": "Premium"})
	queryCount("Postpaid in Istanbul", map[string]any{"segment": "postpaid", "city": "Istanbul"})

	fmt.Println("\n  — Revenue Queries —")
	query("Top 20 highest bills", map[string]any{}, &oxidb.FindOptions{
		Sort: map[string]any{"monthly_bill": -1}, Limit: intPtr(20)})
	query("Bills > 800 TL (limit 50)", map[string]any{"monthly_bill": map[string]any{"$gt": 800}},
		&oxidb.FindOptions{Limit: intPtr(50)})
	queryCount("Customers paying > 500 TL", map[string]any{"monthly_bill": map[string]any{"$gt": 500}})

	fmt.Println("\n  — Churn Risk —")
	query("Low NPS + active (churn risk), limit 100",
		map[string]any{"nps_score": map[string]any{"$lte": 3}, "status": "active"},
		&oxidb.FindOptions{Limit: intPtr(100)})
	queryCount("NPS <= 3 count", map[string]any{"nps_score": map[string]any{"$lte": 3}})

	fmt.Println("\n  — Network Analysis —")
	query("Samsung users in Ankara, limit 50",
		map[string]any{"city": "Ankara", "device": map[string]any{"$regex": "Samsung"}},
		&oxidb.FindOptions{Limit: intPtr(50)})
	queryCount("Fiber subscribers", map[string]any{"has_fiber": true})
	query("Loyal customers (10+ years), top 20 by bill",
		map[string]any{"loyalty_years": map[string]any{"$gte": 10}},
		&oxidb.FindOptions{Sort: map[string]any{"monthly_bill": -1}, Limit: intPtr(20)})

	// ═══ Phase 4: Aggregation ═══
	fmt.Println("\n═══ Phase 4: Aggregation ═══")

	queryAgg("Revenue by region", []map[string]any{
		{"$group": map[string]any{"_id": "$region", "total_revenue": map[string]any{"$sum": "$monthly_bill"}, "count": map[string]any{"$sum": 1}}},
		{"$sort": map[string]any{"total_revenue": -1}},
	})

	queryAgg("Avg bill by plan", []map[string]any{
		{"$group": map[string]any{"_id": "$plan", "avg_bill": map[string]any{"$avg": "$monthly_bill"}, "count": map[string]any{"$sum": 1}}},
		{"$sort": map[string]any{"avg_bill": -1}},
	})

	queryAgg("Churn by city (top 10)", []map[string]any{
		{"$match": map[string]any{"status": "churned"}},
		{"$group": map[string]any{"_id": "$city", "churned": map[string]any{"$sum": 1}}},
		{"$sort": map[string]any{"churned": -1}},
		{"$limit": 10},
	})

	queryAgg("Device distribution", []map[string]any{
		{"$group": map[string]any{"_id": "$device", "count": map[string]any{"$sum": 1}}},
		{"$sort": map[string]any{"count": -1}},
	})

	queryAgg("NPS by segment", []map[string]any{
		{"$group": map[string]any{"_id": "$segment", "avg_nps": map[string]any{"$avg": "$nps_score"}, "count": map[string]any{"$sum": 1}}},
		{"$sort": map[string]any{"avg_nps": -1}},
	})

	queryAgg("Active users by channel", []map[string]any{
		{"$match": map[string]any{"status": "active"}},
		{"$group": map[string]any{"_id": "$channel", "count": map[string]any{"$sum": 1}}},
		{"$sort": map[string]any{"count": -1}},
	})

	// ═══ Phase 5: Update (campaign) ═══
	fmt.Println("\n═══ Phase 5: Campaign Update ═══")
	{
		t0 := time.Now()
		res, _ := c.Update(coll,
			map[string]any{"plan": "Starter", "loyalty_years": map[string]any{"$gte": 5}},
			map[string]any{"$set": map[string]any{"plan": "Basic", "upgrade_campaign": "loyalty_2025"}})
		n := 0
		if res != nil { if v, ok := res["modified"].(float64); ok { n = int(v) } }
		fmt.Printf("  Upgrade Starter→Basic (loyalty 5+yr): %s (%d modified)\n", formatDur(time.Since(t0)), n)
	}

	// ═══ Summary ═══
	fmt.Println()
	fmt.Println("═══════════════════════════════════════════════════════════════")
	n, _ := c.Count(coll, map[string]any{})
	fmt.Printf("Total customers: %d\n", n)

	// Memory
	fmt.Println("═══════════════════════════════════════════════════════════════")
}

func intPtr(n int) *int { return &n }
