package main

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/parisxmas/OxiDB/go/oxidb"
	"go.mongodb.org/mongo-driver/bson"
)

// testHotKey tests concurrent updates to the same document (contention).
func testHotKey() {
	coll := "hotkey_bench"
	env := setup(coll)
	seedData(env, coll, docCount)
	env.Oxi.Close()
	ctx := env.Ctx

	workers := 10
	opsPerWorker := 200
	totalOps := workers * opsPerWorker
	targetEmpID := "E000001"

	fmt.Printf("  %d workers × %d updates on same doc (%s)\n\n", workers, opsPerWorker, targetEmpID)

	// ── OxiDB ──
	var oxiErrors int64
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
			for i := 0; i < opsPerWorker; i++ {
				_, err := client.UpdateOne(coll,
					map[string]any{"emp_id": targetEmpID},
					map[string]any{"$inc": map[string]any{"projects": 1}, "$set": map[string]any{"salary": 50000 + rng.Intn(100000)}})
				if err != nil {
					atomic.AddInt64(&oxiErrors, 1)
				}
			}
		}(oxiClients[w], w)
	}
	wg.Wait()
	oxiDur := time.Since(t0)
	for _, c := range oxiClients {
		c.Close()
	}
	fmt.Printf("  OxiDB:   %s (%d ops, %d errors)\n", formatDur(oxiDur), totalOps, oxiErrors)

	// ── MongoDB ──
	var monErrors int64

	t0 = time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(seed + 1000)))
			mcoll := env.Mdb.Collection(coll)
			for i := 0; i < opsPerWorker; i++ {
				_, err := mcoll.UpdateOne(ctx,
					bson.M{"emp_id": targetEmpID},
					bson.M{"$inc": bson.M{"projects": 1}, "$set": bson.M{"salary": 50000 + rng.Intn(100000)}})
				if err != nil {
					atomic.AddInt64(&monErrors, 1)
				}
			}
		}(w)
	}
	wg.Wait()
	monDur := time.Since(t0)
	fmt.Printf("  MongoDB: %s (%d ops, %d errors)\n", formatDur(monDur), totalOps, monErrors)

	record("HOT_KEY", fmt.Sprintf("Hot key %d updates (%d workers)", totalOps, workers), oxiDur, monDur, totalOps, totalOps, nil, nil)

	verifyClient := newOxiClient()
	oxiDoc, _ := verifyClient.FindOne(coll, map[string]any{"emp_id": targetEmpID})
	verifyClient.Close()
	if oxiDoc != nil {
		if proj, ok := oxiDoc["projects"]; ok {
			fmt.Printf("\n  OxiDB final projects count: %v\n", proj)
		}
	}

	printReport()
}
