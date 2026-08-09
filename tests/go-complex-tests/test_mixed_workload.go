package main

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/parisxmas/OxiDB/go/oxidb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// testMixedWorkload runs a concurrent mixed workload:
// 10 workers, each doing 1000 operations: 70% read, 20% update, 10% insert
func testMixedWorkload() {
	coll := "mixed_bench"
	env := setup(coll)
	seedData(env, coll, docCount)
	env.Oxi.Close() // free connection slot for workers

	workers := 10
	opsPerWorker := 1000
	totalOps := workers * opsPerWorker

	fmt.Printf("  %d workers × %d ops (70%% read, 20%% update, 10%% insert)\n\n", workers, opsPerWorker)

	// ── OxiDB (dedicated connection per worker) ──
	var oxiReads, oxiUpdates, oxiInserts, oxiErrors int64
	oxiClients := make([]*oxidb.Client, workers)
	for i := 0; i < workers; i++ {
		oxiClients[i] = newOxiClient()
	}

	t0 := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(client *oxidb.Client, seed int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(seed)))
			nextInsert := docCount + seed*opsPerWorker
			for i := 0; i < opsPerWorker; i++ {
				roll := rng.Intn(100)
				if roll < 70 {
					empID := fmt.Sprintf("E%06d", rng.Intn(docCount))
					_, err := client.FindOne(coll, map[string]any{"emp_id": empID})
					if err != nil {
						atomic.AddInt64(&oxiErrors, 1)
					}
					atomic.AddInt64(&oxiReads, 1)
				} else if roll < 90 {
					empID := fmt.Sprintf("E%06d", rng.Intn(docCount))
					_, err := client.UpdateOne(coll, map[string]any{"emp_id": empID},
						map[string]any{"$set": map[string]any{"salary": 30000 + rng.Intn(170000)}})
					if err != nil {
						atomic.AddInt64(&oxiErrors, 1)
					}
					atomic.AddInt64(&oxiUpdates, 1)
				} else {
					doc := makeDoc(nextInsert)
					nextInsert++
					_, err := client.Insert(coll, doc)
					if err != nil {
						atomic.AddInt64(&oxiErrors, 1)
					}
					atomic.AddInt64(&oxiInserts, 1)
				}
			}
		}(oxiClients[w], w)
	}
	wg.Wait()
	oxiDur := time.Since(t0)
	for _, c := range oxiClients {
		c.Close()
	}

	oxiOps := float64(totalOps) / oxiDur.Seconds()
	fmt.Printf("  OxiDB:   %s (%d reads, %d updates, %d inserts, %d errors) — %.0f ops/sec\n",
		formatDur(oxiDur), oxiReads, oxiUpdates, oxiInserts, oxiErrors, oxiOps)

	// ── MongoDB ──
	var monReads, monUpdates, monInserts, monErrors int64
	ctx := env.Ctx

	t0 = time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(seed + 1000)))
			mcoll := env.Mdb.Collection(coll)
			nextInsert := docCount + seed*opsPerWorker + totalOps
			for i := 0; i < opsPerWorker; i++ {
				roll := rng.Intn(100)
				if roll < 70 {
					empID := fmt.Sprintf("E%06d", rng.Intn(docCount))
					err := mcoll.FindOne(ctx, bson.M{"emp_id": empID}).Err()
					if err != nil && err != mongo.ErrNoDocuments {
						atomic.AddInt64(&monErrors, 1)
					}
					atomic.AddInt64(&monReads, 1)
				} else if roll < 90 {
					empID := fmt.Sprintf("E%06d", rng.Intn(docCount))
					_, err := mcoll.UpdateOne(ctx, bson.M{"emp_id": empID},
						bson.M{"$set": bson.M{"salary": 30000 + rng.Intn(170000)}})
					if err != nil {
						atomic.AddInt64(&monErrors, 1)
					}
					atomic.AddInt64(&monUpdates, 1)
				} else {
					doc := makeBson(nextInsert)
					nextInsert++
					_, err := mcoll.InsertOne(ctx, doc)
					if err != nil {
						atomic.AddInt64(&monErrors, 1)
					}
					atomic.AddInt64(&monInserts, 1)
				}
			}
		}(w)
	}
	wg.Wait()
	monDur := time.Since(t0)

	monOps := float64(totalOps) / monDur.Seconds()
	fmt.Printf("  MongoDB: %s (%d reads, %d updates, %d inserts, %d errors) — %.0f ops/sec\n",
		formatDur(monDur), monReads, monUpdates, monInserts, monErrors, monOps)

	record("MIXED", fmt.Sprintf("Mixed workload (%d ops, %d workers)", totalOps, workers), oxiDur, monDur, totalOps, totalOps, nil, nil)
	fmt.Printf("\n  Throughput: OxiDB %.0f ops/sec  MongoDB %.0f ops/sec\n", oxiOps, monOps)

	printReport()
}
