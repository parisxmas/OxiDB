package main

import (
	"fmt"
	"time"

	"github.com/parisxmas/OxiDB/go/oxidb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// testNestedQuery tests dot-notation queries on nested document fields.
func testNestedQuery() {
	coll := "nested_bench"
	env := setup(coll)
	seedData(env, coll, docCount)
	ctx := env.Ctx

	// Create indexes on nested fields (dot-notation)
	fmt.Println("  Creating nested field indexes...")
	nestedFields := []string{"address.city", "address.country", "address.zip"}
	for _, f := range nestedFields {
		env.Oxi.CreateIndex(coll, f)
		env.Mcoll.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: f, Value: 1}}})
	}
	fmt.Println("  Done.\n")

	fmt.Println("  Testing dot-notation queries on nested fields\n")

	// 1. Exact match on nested field: address.city
	{
		name := "Nested exact: address.city"
		t0 := time.Now()
		oxiRes, oxiErr := env.Oxi.Find(coll, map[string]any{"address.city": "Tokyo"}, nil)
		oxiDur := time.Since(t0)

		t0 = time.Now()
		cur, monErr := env.Mcoll.Find(ctx, bson.M{"address.city": "Tokyo"})
		var monRes []bson.M
		if monErr == nil {
			monErr = cur.All(ctx, &monRes)
		}
		monDur := time.Since(t0)
		record("NESTED", name, oxiDur, monDur, len(oxiRes), len(monRes), oxiErr, monErr)
	}

	// 2. Nested field + top-level field
	{
		name := "Nested + top-level: address.country + status"
		t0 := time.Now()
		oxiRes, oxiErr := env.Oxi.Find(coll,
			map[string]any{"address.country": "EU-WEST", "status": "active"}, nil)
		oxiDur := time.Since(t0)

		t0 = time.Now()
		cur, monErr := env.Mcoll.Find(ctx, bson.M{"address.country": "EU-WEST", "status": "active"})
		var monRes []bson.M
		if monErr == nil {
			monErr = cur.All(ctx, &monRes)
		}
		monDur := time.Since(t0)
		record("NESTED", name, oxiDur, monDur, len(oxiRes), len(monRes), oxiErr, monErr)
	}

	// 3. Nested range query: address.zip > "050000"
	{
		name := "Nested range: address.zip > 050000"
		limit := 100
		t0 := time.Now()
		oxiRes, oxiErr := env.Oxi.Find(coll,
			map[string]any{"address.zip": map[string]any{"$gt": "050000"}},
			&oxidb.FindOptions{Limit: &limit})
		oxiDur := time.Since(t0)

		t0 = time.Now()
		mopts := options.Find().SetLimit(int64(limit))
		cur, monErr := env.Mcoll.Find(ctx, bson.M{"address.zip": bson.M{"$gt": "050000"}}, mopts)
		var monRes []bson.M
		if monErr == nil {
			monErr = cur.All(ctx, &monRes)
		}
		monDur := time.Since(t0)
		record("NESTED", name, oxiDur, monDur, len(oxiRes), len(monRes), oxiErr, monErr)
	}

	// 4. Count on nested field
	{
		name := "Nested count: address.city = London"
		t0 := time.Now()
		oxiN, oxiErr := env.Oxi.Count(coll, map[string]any{"address.city": "London"})
		oxiDur := time.Since(t0)

		t0 = time.Now()
		monN, monErr := env.Mcoll.CountDocuments(ctx, bson.M{"address.city": "London"})
		monDur := time.Since(t0)
		record("NESTED", name, oxiDur, monDur, oxiN, int(monN), oxiErr, monErr)
	}

	// 5. FindOne on nested field
	{
		name := "Nested findOne: address.zip = 00042"
		t0 := time.Now()
		oxiDoc, oxiErr := env.Oxi.FindOne(coll, map[string]any{"address.zip": "00042"})
		oxiDur := time.Since(t0)
		oxiN := 0
		if oxiDoc != nil {
			oxiN = 1
		}

		t0 = time.Now()
		var monDoc bson.M
		monErr := env.Mcoll.FindOne(ctx, bson.M{"address.zip": "00042"}).Decode(&monDoc)
		monDur := time.Since(t0)
		monN := 0
		if monDoc != nil {
			monN = 1
		}
		record("NESTED", name, oxiDur, monDur, oxiN, monN, oxiErr, monErr)
	}

	printReport()
}
