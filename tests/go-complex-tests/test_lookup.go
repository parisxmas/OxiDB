package main

import (
	"fmt"
	"math/rand"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// testLookup tests cross-collection $lookup (join) operations.
func testLookup() {
	env := setup("orders")
	ctx := env.Ctx

	// ── Seed two collections: customers + orders ──
	custColl := "customers"
	ordersColl := "orders"
	numCustomers := 10000
	numOrders := 50000

	env.Oxi.DropCollection(custColl)
	env.Oxi.DropCollection(ordersColl)
	env.Mdb.Collection(custColl).Drop(ctx)
	env.Mdb.Collection(ordersColl).Drop(ctx)

	fmt.Printf("  Seeding %d customers + %d orders...\n", numCustomers, numOrders)

	// Customers
	oxiCusts := make([]map[string]any, numCustomers)
	monCusts := make([]interface{}, numCustomers)
	for i := 0; i < numCustomers; i++ {
		c := map[string]any{
			"customer_id": fmt.Sprintf("C%06d", i),
			"name":        fmt.Sprintf("Customer %d", i),
			"region":      regions[rand.Intn(len(regions))],
			"tier":        []string{"bronze", "silver", "gold", "platinum"}[rand.Intn(4)],
		}
		oxiCusts[i] = c
		monCusts[i] = toBsonM(c)
	}
	env.Oxi.InsertMany(custColl, oxiCusts)
	env.Mdb.Collection(custColl).InsertMany(ctx, monCusts)

	// Orders
	products := []string{"Widget", "Gadget", "Sprocket", "Bracket", "Module"}
	for offset := 0; offset < numOrders; offset += batchSize {
		end := offset + batchSize
		if end > numOrders {
			end = numOrders
		}
		oxiBatch := make([]map[string]any, 0, end-offset)
		monBatch := make([]interface{}, 0, end-offset)
		for i := offset; i < end; i++ {
			o := map[string]any{
				"order_id":    fmt.Sprintf("O%06d", i),
				"customer_id": fmt.Sprintf("C%06d", rand.Intn(numCustomers)),
				"product":     products[rand.Intn(len(products))],
				"amount":      10 + rand.Intn(990),
				"quantity":    1 + rand.Intn(10),
				"status":      []string{"pending", "shipped", "delivered", "cancelled"}[rand.Intn(4)],
			}
			oxiBatch = append(oxiBatch, o)
			monBatch = append(monBatch, toBsonM(o))
		}
		env.Oxi.InsertMany(ordersColl, oxiBatch)
		env.Mdb.Collection(ordersColl).InsertMany(ctx, monBatch)
	}

	// Create indexes
	env.Oxi.CreateIndex(custColl, "customer_id")
	env.Oxi.CreateIndex(ordersColl, "customer_id")
	env.Oxi.CreateIndex(ordersColl, "product")
	env.Oxi.CreateIndex(ordersColl, "status")
	env.Mdb.Collection(custColl).Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "customer_id", Value: 1}}})
	env.Mdb.Collection(ordersColl).Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "customer_id", Value: 1}}})
	env.Mdb.Collection(ordersColl).Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "product", Value: 1}}})
	env.Mdb.Collection(ordersColl).Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "status", Value: 1}}})

	fmt.Println("  Seed complete.\n")

	// 1. $lookup: orders → customers (small result, filtered)
	{
		name := "lookup orders→customers (limit 20)"

		oxiPipeline := []map[string]any{
			{"$match": map[string]any{"product": "Widget", "status": "delivered"}},
			{"$lookup": map[string]any{
				"from":         custColl,
				"localField":   "customer_id",
				"foreignField": "customer_id",
				"as":           "customer",
			}},
			{"$limit": 20},
		}
		t0 := time.Now()
		oxiRes, oxiErr := env.Oxi.Aggregate(ordersColl, oxiPipeline)
		oxiDur := time.Since(t0)

		monPipeline := mongo.Pipeline{
			{{Key: "$match", Value: bson.M{"product": "Widget", "status": "delivered"}}},
			{{Key: "$lookup", Value: bson.M{
				"from":         custColl,
				"localField":   "customer_id",
				"foreignField": "customer_id",
				"as":           "customer",
			}}},
			{{Key: "$limit", Value: 20}},
		}
		t0 = time.Now()
		cur, monErr := env.Mdb.Collection(ordersColl).Aggregate(ctx, monPipeline)
		var monRes []bson.M
		if monErr == nil {
			monErr = cur.All(ctx, &monRes)
		}
		monDur := time.Since(t0)
		record("LOOKUP", name, oxiDur, monDur, len(oxiRes), len(monRes), oxiErr, monErr)
	}

	// 2. $lookup + $group: total spend per customer tier
	{
		name := "lookup + group by customer tier"

		oxiPipeline := []map[string]any{
			{"$match": map[string]any{"status": "delivered"}},
			{"$lookup": map[string]any{
				"from":         custColl,
				"localField":   "customer_id",
				"foreignField": "customer_id",
				"as":           "cust",
			}},
			{"$group": map[string]any{
				"_id":         "$cust.tier",
				"total_spent": map[string]any{"$sum": "$amount"},
				"order_count": map[string]any{"$sum": 1},
			}},
			{"$sort": map[string]any{"total_spent": -1}},
		}
		t0 := time.Now()
		oxiRes, oxiErr := env.Oxi.Aggregate(ordersColl, oxiPipeline)
		oxiDur := time.Since(t0)

		monPipeline := mongo.Pipeline{
			{{Key: "$match", Value: bson.M{"status": "delivered"}}},
			{{Key: "$lookup", Value: bson.M{
				"from":         custColl,
				"localField":   "customer_id",
				"foreignField": "customer_id",
				"as":           "cust",
			}}},
			{{Key: "$group", Value: bson.M{
				"_id":         "$cust.tier",
				"total_spent": bson.M{"$sum": "$amount"},
				"order_count": bson.M{"$sum": 1},
			}}},
			{{Key: "$sort", Value: bson.D{{Key: "total_spent", Value: -1}}}},
		}
		t0 = time.Now()
		cur, monErr := env.Mdb.Collection(ordersColl).Aggregate(ctx, monPipeline)
		var monRes []bson.M
		if monErr == nil {
			monErr = cur.All(ctx, &monRes)
		}
		monDur := time.Since(t0)
		record("LOOKUP", name, oxiDur, monDur, len(oxiRes), len(monRes), oxiErr, monErr)
	}

	// 3. $lookup (reverse): customers → their order count
	{
		name := "lookup customers→orders count (limit 10)"

		oxiPipeline := []map[string]any{
			{"$match": map[string]any{"tier": "platinum"}},
			{"$lookup": map[string]any{
				"from":         ordersColl,
				"localField":   "customer_id",
				"foreignField": "customer_id",
				"as":           "orders",
			}},
			{"$addFields": map[string]any{"order_count": map[string]any{"$size": "$orders"}}},
			{"$sort": map[string]any{"order_count": -1}},
			{"$limit": 10},
		}
		t0 := time.Now()
		oxiRes, oxiErr := env.Oxi.Aggregate(custColl, oxiPipeline)
		oxiDur := time.Since(t0)

		monPipeline := mongo.Pipeline{
			{{Key: "$match", Value: bson.M{"tier": "platinum"}}},
			{{Key: "$lookup", Value: bson.M{
				"from":         ordersColl,
				"localField":   "customer_id",
				"foreignField": "customer_id",
				"as":           "orders",
			}}},
			{{Key: "$addFields", Value: bson.M{"order_count": bson.M{"$size": "$orders"}}}},
			{{Key: "$sort", Value: bson.D{{Key: "order_count", Value: -1}}}},
			{{Key: "$limit", Value: 10}},
		}
		t0 = time.Now()
		cur, monErr := env.Mdb.Collection(custColl).Aggregate(ctx, monPipeline)
		var monRes []bson.M
		if monErr == nil {
			monErr = cur.All(ctx, &monRes)
		}
		monDur := time.Since(t0)
		record("LOOKUP", name, oxiDur, monDur, len(oxiRes), len(monRes), oxiErr, monErr)
	}

	printReport()
}
