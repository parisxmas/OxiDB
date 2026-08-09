package main

import (
	"fmt"
	"time"

	"github.com/parisxmas/OxiDB/go/oxidb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// testPagination tests skip+limit pagination at various page depths.
func testPagination() {
	coll := "page_bench"
	env := setup(coll)
	seedData(env, coll, docCount)
	ctx := env.Ctx

	pageSize := 20
	pages := []int{1, 10, 50, 100, 500}

	fmt.Printf("  Page size: %d, testing pages: %v\n\n", pageSize, pages)

	for _, page := range pages {
		skip := (page - 1) * pageSize
		name := fmt.Sprintf("Page %d (skip=%d, limit=%d)", page, skip, pageSize)

		// OxiDB
		t0 := time.Now()
		oxiRes, oxiErr := env.Oxi.Find(coll, map[string]any{"department": "engineering"},
			&oxidb.FindOptions{
				Sort:  map[string]any{"salary": -1},
				Skip:  intPtr(skip),
				Limit: intPtr(pageSize),
			})
		oxiDur := time.Since(t0)

		// MongoDB
		t0 = time.Now()
		mopts := options.Find().SetSort(bson.D{{Key: "salary", Value: -1}}).SetSkip(int64(skip)).SetLimit(int64(pageSize))
		cur, monErr := env.Mcoll.Find(ctx, bson.M{"department": "engineering"}, mopts)
		var monRes []bson.M
		if monErr == nil {
			monErr = cur.All(ctx, &monRes)
		}
		monDur := time.Since(t0)

		record("PAGINATION", name, oxiDur, monDur, len(oxiRes), len(monRes), oxiErr, monErr)
	}

	printReport()
}
