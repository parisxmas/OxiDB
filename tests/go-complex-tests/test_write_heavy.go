package main

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/parisxmas/OxiDB/clients/go/oxidb"
	"go.mongodb.org/mongo-driver/bson"
)

// testWriteHeavy measures sustained write throughput: insert + update ops/sec.
func testWriteHeavy() {
	coll := "write_bench"
	env := setup(coll)
	ctx := env.Ctx

	seedCount := 10000
	seedData(env, coll, seedCount)
	env.Oxi.Close()

	workers := 8
	duration := 5 * time.Second

	fmt.Printf("  %d workers, %s sustained write (50%% insert, 50%% update)\n\n", workers, duration)

	// ── OxiDB ──
	var oxiOps int64
	var oxiErrors int64
	oxiClients := make([]*oxidb.Client, workers)
	for i := 0; i < workers; i++ {
		oxiClients[i] = newOxiClient()
	}

	var stop int64
	var wg sync.WaitGroup

	go func() {
		time.Sleep(duration)
		atomic.StoreInt64(&stop, 1)
	}()

	t0 := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(client *oxidb.Client, seed int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(seed)))
			nextID := seedCount + seed*1000000
			for atomic.LoadInt64(&stop) == 0 {
				if rng.Intn(2) == 0 {
					doc := makeDoc(nextID)
					nextID++
					_, err := client.Insert(coll, doc)
					if err != nil {
						atomic.AddInt64(&oxiErrors, 1)
					}
				} else {
					empID := fmt.Sprintf("E%06d", rng.Intn(seedCount))
					_, err := client.UpdateOne(coll, map[string]any{"emp_id": empID},
						map[string]any{"$set": map[string]any{"salary": 30000 + rng.Intn(170000)}})
					if err != nil {
						atomic.AddInt64(&oxiErrors, 1)
					}
				}
				atomic.AddInt64(&oxiOps, 1)
			}
		}(oxiClients[w], w)
	}
	wg.Wait()
	oxiDur := time.Since(t0)
	for _, c := range oxiClients {
		c.Close()
	}
	oxiOpsPerSec := float64(oxiOps) / oxiDur.Seconds()
	fmt.Printf("  OxiDB:   %d ops in %s — %.0f ops/sec (%d errors)\n", oxiOps, formatDur(oxiDur), oxiOpsPerSec, oxiErrors)

	// ── MongoDB ──
	atomic.StoreInt64(&stop, 0)
	var monOps int64
	var monErrors int64

	go func() {
		time.Sleep(duration)
		atomic.StoreInt64(&stop, 1)
	}()

	t0 = time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(seed + 1000)))
			mcoll := env.Mdb.Collection(coll)
			nextID := seedCount + seed*1000000 + 500000
			for atomic.LoadInt64(&stop) == 0 {
				if rng.Intn(2) == 0 {
					doc := makeBson(nextID)
					nextID++
					_, err := mcoll.InsertOne(ctx, doc)
					if err != nil {
						atomic.AddInt64(&monErrors, 1)
					}
				} else {
					empID := fmt.Sprintf("E%06d", rng.Intn(seedCount))
					_, err := mcoll.UpdateOne(ctx, bson.M{"emp_id": empID},
						bson.M{"$set": bson.M{"salary": 30000 + rng.Intn(170000)}})
					if err != nil {
						atomic.AddInt64(&monErrors, 1)
					}
				}
				atomic.AddInt64(&monOps, 1)
			}
		}(w)
	}
	wg.Wait()
	monDur := time.Since(t0)
	monOpsPerSec := float64(monOps) / monDur.Seconds()
	fmt.Printf("  MongoDB: %d ops in %s — %.0f ops/sec (%d errors)\n", monOps, formatDur(monDur), monOpsPerSec, monErrors)

	oxiTime := time.Duration(float64(time.Second) / oxiOpsPerSec)
	monTime := time.Duration(float64(time.Second) / monOpsPerSec)
	record("WRITE_HEAVY", fmt.Sprintf("Sustained write %ds (%d workers)", int(duration.Seconds()), workers),
		oxiTime, monTime, int(oxiOps), int(monOps), nil, nil)

	fmt.Printf("\n  Throughput: OxiDB %.0f ops/sec  MongoDB %.0f ops/sec\n", oxiOpsPerSec, monOpsPerSec)

	printReport()
}
