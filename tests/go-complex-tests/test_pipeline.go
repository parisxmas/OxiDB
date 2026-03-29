package main

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// testPipeline tests multi-stage aggregation pipelines.
func testPipeline() {
	coll := "pipe_bench"
	env := setup(coll)
	seedData(env, coll, docCount)
	ctx := env.Ctx

	fmt.Println("  Multi-stage aggregation pipelines\n")

	// 1. $match → $group → $sort → $limit
	{
		name := "match→group→sort→limit (top 5 depts)"

		oxiPipeline := []map[string]any{
			{"$match": map[string]any{"region": "US-EAST"}},
			{"$group": map[string]any{"_id": "$department", "total": map[string]any{"$sum": "$salary"}, "count": map[string]any{"$sum": 1}}},
			{"$sort": map[string]any{"total": -1}},
			{"$limit": 5},
		}
		t0 := time.Now()
		oxiRes, oxiErr := env.Oxi.Aggregate(coll, oxiPipeline)
		oxiDur := time.Since(t0)

		monPipeline := mongo.Pipeline{
			{{Key: "$match", Value: bson.M{"region": "US-EAST"}}},
			{{Key: "$group", Value: bson.M{"_id": "$department", "total": bson.M{"$sum": "$salary"}, "count": bson.M{"$sum": 1}}}},
			{{Key: "$sort", Value: bson.D{{Key: "total", Value: -1}}}},
			{{Key: "$limit", Value: 5}},
		}
		t0 = time.Now()
		cur, monErr := env.Mcoll.Aggregate(ctx, monPipeline)
		var monRes []bson.M
		if monErr == nil {
			monErr = cur.All(ctx, &monRes)
		}
		monDur := time.Since(t0)
		record("PIPELINE", name, oxiDur, monDur, len(oxiRes), len(monRes), oxiErr, monErr)
	}

	// 2. $match → $group → $match (having) → $sort
	{
		name := "match→group→match(having)→sort"

		oxiPipeline := []map[string]any{
			{"$match": map[string]any{"status": "active"}},
			{"$group": map[string]any{"_id": "$city", "avg_sal": map[string]any{"$avg": "$salary"}, "count": map[string]any{"$sum": 1}}},
			{"$match": map[string]any{"count": map[string]any{"$gt": 500}}},
			{"$sort": map[string]any{"avg_sal": -1}},
		}
		t0 := time.Now()
		oxiRes, oxiErr := env.Oxi.Aggregate(coll, oxiPipeline)
		oxiDur := time.Since(t0)

		monPipeline := mongo.Pipeline{
			{{Key: "$match", Value: bson.M{"status": "active"}}},
			{{Key: "$group", Value: bson.M{"_id": "$city", "avg_sal": bson.M{"$avg": "$salary"}, "count": bson.M{"$sum": 1}}}},
			{{Key: "$match", Value: bson.M{"count": bson.M{"$gt": 500}}}},
			{{Key: "$sort", Value: bson.D{{Key: "avg_sal", Value: -1}}}},
		}
		t0 = time.Now()
		cur, monErr := env.Mcoll.Aggregate(ctx, monPipeline)
		var monRes []bson.M
		if monErr == nil {
			monErr = cur.All(ctx, &monRes)
		}
		monDur := time.Since(t0)
		record("PIPELINE", name, oxiDur, monDur, len(oxiRes), len(monRes), oxiErr, monErr)
	}

	// 3. $group with multiple accumulators (heavy computation)
	{
		name := "group all — 5 accumulators per city"

		oxiPipeline := []map[string]any{
			{"$group": map[string]any{
				"_id":        "$city",
				"count":      map[string]any{"$sum": 1},
				"avg_salary": map[string]any{"$avg": "$salary"},
				"max_salary": map[string]any{"$max": "$salary"},
				"min_salary": map[string]any{"$min": "$salary"},
				"avg_age":    map[string]any{"$avg": "$age"},
			}},
			{"$sort": map[string]any{"count": -1}},
		}
		t0 := time.Now()
		oxiRes, oxiErr := env.Oxi.Aggregate(coll, oxiPipeline)
		oxiDur := time.Since(t0)

		monPipeline := mongo.Pipeline{
			{{Key: "$group", Value: bson.M{
				"_id":        "$city",
				"count":      bson.M{"$sum": 1},
				"avg_salary": bson.M{"$avg": "$salary"},
				"max_salary": bson.M{"$max": "$salary"},
				"min_salary": bson.M{"$min": "$salary"},
				"avg_age":    bson.M{"$avg": "$age"},
			}}},
			{{Key: "$sort", Value: bson.D{{Key: "count", Value: -1}}}},
		}
		t0 = time.Now()
		cur, monErr := env.Mcoll.Aggregate(ctx, monPipeline)
		var monRes []bson.M
		if monErr == nil {
			monErr = cur.All(ctx, &monRes)
		}
		monDur := time.Since(t0)
		record("PIPELINE", name, oxiDur, monDur, len(oxiRes), len(monRes), oxiErr, monErr)
	}

	// 4. $match → $group → $sort → $skip → $limit (deep pagination of aggregation)
	{
		name := "match→group→sort→skip 3→limit 3"

		oxiPipeline := []map[string]any{
			{"$match": map[string]any{"salary": map[string]any{"$gt": 100000}}},
			{"$group": map[string]any{"_id": "$department", "headcount": map[string]any{"$sum": 1}, "total_sal": map[string]any{"$sum": "$salary"}}},
			{"$sort": map[string]any{"headcount": -1}},
			{"$skip": 3},
			{"$limit": 3},
		}
		t0 := time.Now()
		oxiRes, oxiErr := env.Oxi.Aggregate(coll, oxiPipeline)
		oxiDur := time.Since(t0)

		monPipeline := mongo.Pipeline{
			{{Key: "$match", Value: bson.M{"salary": bson.M{"$gt": 100000}}}},
			{{Key: "$group", Value: bson.M{"_id": "$department", "headcount": bson.M{"$sum": 1}, "total_sal": bson.M{"$sum": "$salary"}}}},
			{{Key: "$sort", Value: bson.D{{Key: "headcount", Value: -1}}}},
			{{Key: "$skip", Value: 3}},
			{{Key: "$limit", Value: 3}},
		}
		t0 = time.Now()
		cur, monErr := env.Mcoll.Aggregate(ctx, monPipeline)
		var monRes []bson.M
		if monErr == nil {
			monErr = cur.All(ctx, &monRes)
		}
		monDur := time.Since(t0)
		record("PIPELINE", name, oxiDur, monDur, len(oxiRes), len(monRes), oxiErr, monErr)
	}

	// 5. $count
	{
		name := "count pipeline (active engineers)"

		oxiPipeline := []map[string]any{
			{"$match": map[string]any{"department": "engineering", "status": "active"}},
			{"$count": "total"},
		}
		t0 := time.Now()
		oxiRes, oxiErr := env.Oxi.Aggregate(coll, oxiPipeline)
		oxiDur := time.Since(t0)

		monPipeline := mongo.Pipeline{
			{{Key: "$match", Value: bson.M{"department": "engineering", "status": "active"}}},
			{{Key: "$count", Value: "total"}},
		}
		t0 = time.Now()
		cur, monErr := env.Mcoll.Aggregate(ctx, monPipeline)
		var monRes []bson.M
		if monErr == nil {
			monErr = cur.All(ctx, &monRes)
		}
		monDur := time.Since(t0)
		record("PIPELINE", name, oxiDur, monDur, len(oxiRes), len(monRes), oxiErr, monErr)
	}

	printReport()
}
