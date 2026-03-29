package main

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

// testBulkOps tests bulk update and delete operations on large document sets.
func testBulkOps() {
	coll := "bulk_bench"
	env := setup(coll)
	seedData(env, coll, docCount)
	ctx := env.Ctx

	fmt.Println("  Bulk update and delete operations\n")

	// 1. Bulk update: set all engineers to salary+5000
	{
		name := "Bulk update: all engineering +5000"
		t0 := time.Now()
		oxiRes, oxiErr := env.Oxi.Update(coll,
			map[string]any{"department": "engineering"},
			map[string]any{"$inc": map[string]any{"salary": 5000}})
		oxiDur := time.Since(t0)
		oxiN := 0
		if oxiRes != nil {
			if n, ok := oxiRes["modified"]; ok {
				if nf, ok := n.(float64); ok {
					oxiN = int(nf)
				}
			}
		}

		t0 = time.Now()
		monRes, monErr := env.Mcoll.UpdateMany(ctx,
			bson.M{"department": "engineering"},
			bson.M{"$inc": bson.M{"salary": 5000}})
		monDur := time.Since(t0)
		monN := 0
		if monRes != nil {
			monN = int(monRes.ModifiedCount)
		}
		record("BULK", name, oxiDur, monDur, oxiN, monN, oxiErr, monErr)
	}

	// 2. Bulk update: multi-condition update
	{
		name := "Bulk update: region+status → set rating"
		t0 := time.Now()
		oxiRes, oxiErr := env.Oxi.Update(coll,
			map[string]any{"region": "EU-WEST", "status": "active"},
			map[string]any{"$set": map[string]any{"rating": 4.5}})
		oxiDur := time.Since(t0)
		oxiN := 0
		if oxiRes != nil {
			if n, ok := oxiRes["modified"]; ok {
				if nf, ok := n.(float64); ok {
					oxiN = int(nf)
				}
			}
		}

		t0 = time.Now()
		monRes, monErr := env.Mcoll.UpdateMany(ctx,
			bson.M{"region": "EU-WEST", "status": "active"},
			bson.M{"$set": bson.M{"rating": 4.5}})
		monDur := time.Since(t0)
		monN := 0
		if monRes != nil {
			monN = int(monRes.ModifiedCount)
		}
		record("BULK", name, oxiDur, monDur, oxiN, monN, oxiErr, monErr)
	}

	// 3. Bulk delete: remove all inactive employees
	{
		name := "Bulk delete: all inactive"
		t0 := time.Now()
		oxiRes, oxiErr := env.Oxi.Delete(coll, map[string]any{"status": "inactive"})
		oxiDur := time.Since(t0)
		oxiN := 0
		if oxiRes != nil {
			if n, ok := oxiRes["deleted"]; ok {
				if nf, ok := n.(float64); ok {
					oxiN = int(nf)
				}
			}
		}

		t0 = time.Now()
		monRes, monErr := env.Mcoll.DeleteMany(ctx, bson.M{"status": "inactive"})
		monDur := time.Since(t0)
		monN := 0
		if monRes != nil {
			monN = int(monRes.DeletedCount)
		}
		record("BULK", name, oxiDur, monDur, oxiN, monN, oxiErr, monErr)
	}

	// 4. Bulk delete: range condition
	{
		name := "Bulk delete: salary < 50000"
		t0 := time.Now()
		oxiRes, oxiErr := env.Oxi.Delete(coll, map[string]any{"salary": map[string]any{"$lt": 50000}})
		oxiDur := time.Since(t0)
		oxiN := 0
		if oxiRes != nil {
			if n, ok := oxiRes["deleted"]; ok {
				if nf, ok := n.(float64); ok {
					oxiN = int(nf)
				}
			}
		}

		t0 = time.Now()
		monRes, monErr := env.Mcoll.DeleteMany(ctx, bson.M{"salary": bson.M{"$lt": 50000}})
		monDur := time.Since(t0)
		monN := 0
		if monRes != nil {
			monN = int(monRes.DeletedCount)
		}
		record("BULK", name, oxiDur, monDur, oxiN, monN, oxiErr, monErr)
	}

	// 5. Count remaining
	{
		name := "Count remaining after bulk ops"
		t0 := time.Now()
		oxiN, oxiErr := env.Oxi.Count(coll, map[string]any{})
		oxiDur := time.Since(t0)

		t0 = time.Now()
		monN, monErr := env.Mcoll.CountDocuments(ctx, bson.M{})
		monDur := time.Since(t0)
		record("BULK", name, oxiDur, monDur, oxiN, int(monN), oxiErr, monErr)
	}

	printReport()
}
