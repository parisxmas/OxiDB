package main

import (
	"fmt"
	"time"

	"github.com/parisxmas/OxiDB/clients/go/oxidb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// testLargeResult tests fetching large result sets (1K, 5K, 10K documents).
func testLargeResult() {
	coll := "large_bench"
	env := setup(coll)
	seedData(env, coll, docCount)
	ctx := env.Ctx

	fmt.Println("  Fetching large result sets\n")

	sizes := []int{100, 1000, 5000, 10000}

	for _, size := range sizes {
		// 1. Raw fetch (no filter, just limit)
		{
			name := fmt.Sprintf("Fetch %d docs (no filter)", size)
			limit := size

			t0 := time.Now()
			oxiRes, oxiErr := env.Oxi.Find(coll, map[string]any{},
				&oxidb.FindOptions{Limit: &limit})
			oxiDur := time.Since(t0)

			t0 = time.Now()
			mopts := options.Find().SetLimit(int64(limit))
			cur, monErr := env.Mcoll.Find(ctx, bson.M{}, mopts)
			var monRes []bson.M
			if monErr == nil {
				monErr = cur.All(ctx, &monRes)
			}
			monDur := time.Since(t0)
			record("LARGE_RESULT", name, oxiDur, monDur, len(oxiRes), len(monRes), oxiErr, monErr)
		}
	}

	// 2. Filtered fetch (indexed field, large result)
	{
		name := "Fetch all engineering dept (~12.5K)"
		t0 := time.Now()
		oxiRes, oxiErr := env.Oxi.Find(coll, map[string]any{"department": "engineering"}, nil)
		oxiDur := time.Since(t0)

		t0 = time.Now()
		cur, monErr := env.Mcoll.Find(ctx, bson.M{"department": "engineering"})
		var monRes []bson.M
		if monErr == nil {
			monErr = cur.All(ctx, &monRes)
		}
		monDur := time.Since(t0)
		record("LARGE_RESULT", name, oxiDur, monDur, len(oxiRes), len(monRes), oxiErr, monErr)
	}

	// 3. Sorted large result
	{
		name := "Fetch 5K sorted by salary desc"
		limit := 5000
		t0 := time.Now()
		oxiRes, oxiErr := env.Oxi.Find(coll, map[string]any{},
			&oxidb.FindOptions{Sort: map[string]any{"salary": -1}, Limit: &limit})
		oxiDur := time.Since(t0)

		t0 = time.Now()
		mopts := options.Find().SetSort(bson.D{{Key: "salary", Value: -1}}).SetLimit(int64(limit))
		cur, monErr := env.Mcoll.Find(ctx, bson.M{}, mopts)
		var monRes []bson.M
		if monErr == nil {
			monErr = cur.All(ctx, &monRes)
		}
		monDur := time.Since(t0)
		record("LARGE_RESULT", name, oxiDur, monDur, len(oxiRes), len(monRes), oxiErr, monErr)
	}

	printReport()
}
