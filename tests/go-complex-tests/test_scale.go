package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/parisxmas/OxiDB/go/oxidb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// testScaleTest runs the same queries at different data sizes to measure scaling.
func testScaleTest() {
	scales := []int{10000, 50000, 100000}
	// Add 500K only if DOC_COUNT >= 500000
	if docCount >= 500000 {
		scales = append(scales, 500000)
	}

	fmt.Printf("  Testing scaling at: %v documents\n\n", scales)

	for _, n := range scales {
		coll := fmt.Sprintf("scale_%dk", n/1000)
		env := setup(coll)
		ctx := env.Ctx
		mcoll := env.Mdb.Collection(coll)

		// Seed
		env.Oxi.DropCollection(coll)
		mcoll.Drop(ctx)
		fmt.Printf("  ── Scale: %dk ──\n", n/1000)
		fmt.Printf("  Seeding...")
		for offset := 0; offset < n; offset += batchSize {
			end := offset + batchSize
			if end > n {
				end = n
			}
			oxiBatch := make([]map[string]any, 0, end-offset)
			monBatch := make([]interface{}, 0, end-offset)
			for i := offset; i < end; i++ {
				doc := makeDoc(i)
				oxiBatch = append(oxiBatch, doc)
				monBatch = append(monBatch, makeBson(i))
			}
			env.Oxi.InsertMany(coll, oxiBatch)
			mcoll.InsertMany(ctx, monBatch)
		}
		indexFields := []string{"emp_id", "department", "salary", "city"}
		for _, f := range indexFields {
			env.Oxi.CreateIndex(coll, f)
			mcoll.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: f, Value: 1}}})
		}
		fmt.Println(" done")

		prefix := fmt.Sprintf("[%dk]", n/1000)

		// 1. find_one indexed
		{
			empID := fmt.Sprintf("E%06d", rand.Intn(n))
			t0 := time.Now()
			_, oxiErr := env.Oxi.FindOne(coll, map[string]any{"emp_id": empID})
			oxiDur := time.Since(t0)

			t0 = time.Now()
			monErr := mcoll.FindOne(ctx, bson.M{"emp_id": empID}).Err()
			monDur := time.Since(t0)
			record("SCALE", prefix+" find_one (indexed)", oxiDur, monDur, 1, 1, oxiErr, monErr)
		}

		// 2. Equality query (indexed)
		{
			t0 := time.Now()
			oxiRes, oxiErr := env.Oxi.Find(coll, map[string]any{"department": "engineering"}, nil)
			oxiDur := time.Since(t0)

			t0 = time.Now()
			cur, monErr := mcoll.Find(ctx, bson.M{"department": "engineering"})
			var monRes []bson.M
			if monErr == nil {
				monErr = cur.All(ctx, &monRes)
			}
			monDur := time.Since(t0)
			record("SCALE", prefix+" equality (indexed)", oxiDur, monDur, len(oxiRes), len(monRes), oxiErr, monErr)
		}

		// 3. Range query
		{
			t0 := time.Now()
			oxiRes, oxiErr := env.Oxi.Find(coll,
				map[string]any{"salary": map[string]any{"$gte": 100000, "$lte": 150000}}, nil)
			oxiDur := time.Since(t0)

			t0 = time.Now()
			cur, monErr := mcoll.Find(ctx, bson.M{"salary": bson.M{"$gte": 100000, "$lte": 150000}})
			var monRes []bson.M
			if monErr == nil {
				monErr = cur.All(ctx, &monRes)
			}
			monDur := time.Since(t0)
			record("SCALE", prefix+" range query", oxiDur, monDur, len(oxiRes), len(monRes), oxiErr, monErr)
		}

		// 4. Sort + limit
		{
			t0 := time.Now()
			limit := 10
			oxiRes, oxiErr := env.Oxi.Find(coll, map[string]any{"department": "sales"},
				&oxidb.FindOptions{Sort: map[string]any{"salary": -1}, Limit: &limit})
			oxiDur := time.Since(t0)

			t0 = time.Now()
			mopts := options.Find().SetSort(bson.D{{Key: "salary", Value: -1}}).SetLimit(10)
			cur, monErr := mcoll.Find(ctx, bson.M{"department": "sales"}, mopts)
			var monRes []bson.M
			if monErr == nil {
				monErr = cur.All(ctx, &monRes)
			}
			monDur := time.Since(t0)
			record("SCALE", prefix+" sort+limit 10", oxiDur, monDur, len(oxiRes), len(monRes), oxiErr, monErr)
		}

		// 5. Aggregation
		{
			oxiPipeline := []map[string]any{
				{"$group": map[string]any{"_id": "$city", "avg_sal": map[string]any{"$avg": "$salary"}, "count": map[string]any{"$sum": 1}}},
				{"$sort": map[string]any{"avg_sal": -1}},
			}
			t0 := time.Now()
			oxiRes, oxiErr := env.Oxi.Aggregate(coll, oxiPipeline)
			oxiDur := time.Since(t0)

			monPipeline := mongo.Pipeline{
				{{Key: "$group", Value: bson.M{"_id": "$city", "avg_sal": bson.M{"$avg": "$salary"}, "count": bson.M{"$sum": 1}}}},
				{{Key: "$sort", Value: bson.D{{Key: "avg_sal", Value: -1}}}},
			}
			t0 = time.Now()
			cur, monErr := mcoll.Aggregate(ctx, monPipeline)
			var monRes []bson.M
			if monErr == nil {
				monErr = cur.All(ctx, &monRes)
			}
			monDur := time.Since(t0)
			record("SCALE", prefix+" group by city", oxiDur, monDur, len(oxiRes), len(monRes), oxiErr, monErr)
		}

		// 6. UpdateOne
		{
			empID := fmt.Sprintf("E%06d", rand.Intn(n))
			t0 := time.Now()
			_, oxiErr := env.Oxi.UpdateOne(coll, map[string]any{"emp_id": empID},
				map[string]any{"$set": map[string]any{"status": "onleave"}})
			oxiDur := time.Since(t0)

			t0 = time.Now()
			_, monErr := mcoll.UpdateOne(ctx, bson.M{"emp_id": empID},
				bson.M{"$set": bson.M{"status": "onleave"}})
			monDur := time.Since(t0)
			record("SCALE", prefix+" update_one", oxiDur, monDur, 1, 1, oxiErr, monErr)
		}

		fmt.Println()
	}

	printReport()
}
