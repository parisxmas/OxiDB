// Diagnostic: measures MongoDB InsertOne + ping latency, so it can be run
// from the macOS host (port-forward) and from inside the Docker network
// for an apples-to-apples comparison with the OxiDB diag.
package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func uri() string {
	if u := os.Getenv("MONGO_URI"); u != "" {
		return u
	}
	return "mongodb://127.0.0.1:27017"
}

func measure(name string, n int, fn func() error) {
	ds := make([]time.Duration, 0, n)
	var total time.Duration
	for i := 0; i < n; i++ {
		t := time.Now()
		if err := fn(); err != nil {
			panic(fmt.Sprintf("%s: %v", name, err))
		}
		d := time.Since(t)
		ds = append(ds, d)
		total += d
	}
	sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
	fmt.Printf("  %-28s  p50 %8s   p99 %9s   mean %8s   min %7s\n",
		name, ds[n/2], ds[(n*99)/100], total/time.Duration(n), ds[0])
}

func main() {
	u := uri()
	fmt.Printf("connecting to %s\n", u)
	ctx := context.Background()
	cli, err := mongo.Connect(ctx, options.Client().ApplyURI(u))
	if err != nil {
		panic(err)
	}
	defer cli.Disconnect(ctx)
	if err := cli.Ping(ctx, nil); err != nil {
		panic(err)
	}
	db := cli.Database("diag")
	coll := db.Collection(fmt.Sprintf("c%d", time.Now().UnixNano()%100000))
	defer coll.Drop(ctx)
	admin := cli.Database("admin")

	const N = 20000
	const warmup = 2000
	for i := 0; i < warmup; i++ {
		_ = admin.RunCommand(ctx, bson.D{{Key: "ping", Value: 1}}).Err()
	}

	fmt.Printf("\n── %d iterations each ──\n\n", N)

	measure("ping (admin command)", N, func() error {
		return admin.RunCommand(ctx, bson.D{{Key: "ping", Value: 1}}).Err()
	})

	i := 0
	measure("InsertOne", N, func() error {
		i++
		_, err := coll.InsertOne(ctx, bson.M{
			"seq": i, "name": fmt.Sprintf("User %d", i),
			"email": fmt.Sprintf("user.%d@test.com", i),
			"age":   30, "salary": 50000.0, "department": "Engineering",
			"city": "Tokyo", "country": "JP", "status": "active",
			"score": 88.5, "verified": true, "rating": 4,
			"tags":    []string{"a", "b"},
			"address": bson.M{"street": "100 Main St", "zip": "01234"},
		})
		return err
	})
}
