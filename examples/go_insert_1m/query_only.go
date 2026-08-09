//go:build ignore

package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/parisxmas/OxiDB/go/oxidb"
)

func main() {
	client, err := oxidb.Connect("127.0.0.1", 4444, 30*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	collection := "bench_1m"
	limit10 := 10
	limit50 := 50

	fmt.Println("━━━ QUERIES (data already loaded) ━━━━━━━━")

	type queryCase struct {
		label string
		run   func() (int, error)
	}

	queryTests := []queryCase{
		{"Exact: department=Engineering", func() (int, error) {
			docs, err := client.Find(collection, map[string]any{"department": "Engineering"}, &oxidb.FindOptions{Limit: &limit10})
			return len(docs), err
		}},
		{"Exact: status=active AND country=US", func() (int, error) {
			docs, err := client.Find(collection, map[string]any{"status": "active", "country": "US"}, &oxidb.FindOptions{Limit: &limit10})
			return len(docs), err
		}},
		{"Range: age 25-35", func() (int, error) {
			docs, err := client.Find(collection, map[string]any{"age": map[string]any{"$gte": 25, "$lte": 35}}, &oxidb.FindOptions{Limit: &limit10})
			return len(docs), err
		}},
		{"Range: salary > 150000", func() (int, error) {
			docs, err := client.Find(collection, map[string]any{"salary": map[string]any{"$gt": 150000}}, &oxidb.FindOptions{Limit: &limit10})
			return len(docs), err
		}},
		{"Bool: verified=true (limit 50)", func() (int, error) {
			docs, err := client.Find(collection, map[string]any{"verified": true}, &oxidb.FindOptions{Limit: &limit50})
			return len(docs), err
		}},
		{"Sort: score DESC, limit 10", func() (int, error) {
			docs, err := client.Find(collection, map[string]any{}, &oxidb.FindOptions{Sort: map[string]any{"score": -1}, Limit: &limit10})
			return len(docs), err
		}},
		{"Sort: salary ASC, limit 10", func() (int, error) {
			docs, err := client.Find(collection, map[string]any{}, &oxidb.FindOptions{Sort: map[string]any{"salary": 1}, Limit: &limit10})
			return len(docs), err
		}},
		{"Date range: birthDate 1990-2000", func() (int, error) {
			docs, err := client.Find(collection, map[string]any{"birthDate": map[string]any{"$gte": "1990-01-01", "$lte": "2000-12-31"}}, &oxidb.FindOptions{Limit: &limit10})
			return len(docs), err
		}},
		{"$or: city=Tokyo OR city=Paris", func() (int, error) {
			docs, err := client.Find(collection, map[string]any{"$or": []any{map[string]any{"city": "Tokyo"}, map[string]any{"city": "Paris"}}}, &oxidb.FindOptions{Limit: &limit10})
			return len(docs), err
		}},
		{"$in: rating in [4,5]", func() (int, error) {
			docs, err := client.Find(collection, map[string]any{"rating": map[string]any{"$in": []any{4, 5}}}, &oxidb.FindOptions{Limit: &limit10})
			return len(docs), err
		}},
		{"Nested: address.zip starts with 0", func() (int, error) {
			docs, err := client.Find(collection, map[string]any{"address.zip": map[string]any{"$regex": "^0"}}, &oxidb.FindOptions{Limit: &limit10})
			return len(docs), err
		}},
		{"$regex: name starts with A", func() (int, error) {
			docs, err := client.Find(collection, map[string]any{"name": map[string]any{"$regex": "^A"}}, &oxidb.FindOptions{Limit: &limit10})
			return len(docs), err
		}},
		{"$exists: tags field exists", func() (int, error) {
			docs, err := client.Find(collection, map[string]any{"tags": map[string]any{"$exists": true}}, &oxidb.FindOptions{Limit: &limit10})
			return len(docs), err
		}},
		{"$ne: status != active", func() (int, error) {
			docs, err := client.Find(collection, map[string]any{"status": map[string]any{"$ne": "active"}}, &oxidb.FindOptions{Limit: &limit10})
			return len(docs), err
		}},
		{"Count: department=Sales", func() (int, error) {
			return client.Count(collection, map[string]any{"department": "Sales"})
		}},
		{"Count: verified=true", func() (int, error) {
			return client.Count(collection, map[string]any{"verified": true})
		}},
		{"FindOne: seq=500000", func() (int, error) {
			doc, err := client.FindOne(collection, map[string]any{"seq": 500000})
			if doc != nil { return 1, err }
			return 0, err
		}},
		{"FindOne: seq=999999", func() (int, error) {
			doc, err := client.FindOne(collection, map[string]any{"seq": 999999})
			if doc != nil { return 1, err }
			return 0, err
		}},
	}

	fmt.Printf("\n  %-45s %12s %8s\n", "Query", "Time", "Results")
	fmt.Println("  " + strings.Repeat("─", 68))

	var totalQueryTime time.Duration
	for _, q := range queryTests {
		t0 := time.Now()
		count, err := q.run()
		elapsed := time.Since(t0)
		totalQueryTime += elapsed
		if err != nil {
			fmt.Printf("  %-45s %12s  ERROR: %v\n", q.label, elapsed.Round(time.Microsecond), err)
		} else {
			fmt.Printf("  %-45s %12s %8d\n", q.label, elapsed.Round(time.Microsecond), count)
		}
	}
	fmt.Println("  " + strings.Repeat("─", 68))
	fmt.Printf("  %-45s %12s\n", "Total", totalQueryTime.Round(time.Microsecond))
	fmt.Printf("  %-45s %12s\n", "Average", (totalQueryTime/time.Duration(len(queryTests))).Round(time.Microsecond))

	// Aggregation
	fmt.Println("\n━━━ AGGREGATION ━━━━━━━━━━━━━━━━━━━━━━━━━━")

	type aggCase struct {
		label    string
		pipeline []map[string]any
	}
	aggTests := []aggCase{
		{"Count by department", []map[string]any{
			{"$group": map[string]any{"_id": "$department", "count": map[string]any{"$sum": 1}}},
			{"$sort": map[string]any{"count": -1}},
		}},
		{"Avg salary by department", []map[string]any{
			{"$group": map[string]any{"_id": "$department", "avg": map[string]any{"$avg": "$salary"}}},
			{"$sort": map[string]any{"avg": -1}},
		}},
		{"Count by country, top 5", []map[string]any{
			{"$group": map[string]any{"_id": "$country", "count": map[string]any{"$sum": 1}}},
			{"$sort": map[string]any{"count": -1}},
			{"$limit": 5},
		}},
		{"Active users, avg score by city top 5", []map[string]any{
			{"$match": map[string]any{"status": "active"}},
			{"$group": map[string]any{"_id": "$city", "avgScore": map[string]any{"$avg": "$score"}, "count": map[string]any{"$sum": 1}}},
			{"$sort": map[string]any{"avgScore": -1}},
			{"$limit": 5},
		}},
		{"Min/Max salary by department", []map[string]any{
			{"$group": map[string]any{"_id": "$department", "min": map[string]any{"$min": "$salary"}, "max": map[string]any{"$max": "$salary"}}},
			{"$sort": map[string]any{"max": -1}},
		}},
		{"Count by rating", []map[string]any{
			{"$group": map[string]any{"_id": "$rating", "count": map[string]any{"$sum": 1}}},
			{"$sort": map[string]any{"_id": 1}},
		}},
		{"Count by status", []map[string]any{
			{"$group": map[string]any{"_id": "$status", "count": map[string]any{"$sum": 1}}},
			{"$sort": map[string]any{"count": -1}},
		}},
	}

	fmt.Printf("\n  %-45s %12s %8s\n", "Pipeline", "Time", "Results")
	fmt.Println("  " + strings.Repeat("─", 68))

	var totalAggTime time.Duration
	for _, a := range aggTests {
		t0 := time.Now()
		docs, err := client.Aggregate(collection, a.pipeline)
		elapsed := time.Since(t0)
		totalAggTime += elapsed
		if err != nil {
			fmt.Printf("  %-45s %12s  ERROR: %v\n", a.label, elapsed.Round(time.Microsecond), err)
		} else {
			fmt.Printf("  %-45s %12s %8d\n", a.label, elapsed.Round(time.Microsecond), len(docs))
		}
	}
	fmt.Println("  " + strings.Repeat("─", 68))
	fmt.Printf("  %-45s %12s\n", "Total", totalAggTime.Round(time.Microsecond))
	fmt.Printf("  %-45s %12s\n", "Average", (totalAggTime/time.Duration(len(aggTests))).Round(time.Microsecond))

	fmt.Println("\n━━━ SUMMARY ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Printf("  Queries: %d, total %s, avg %s\n", len(queryTests), totalQueryTime.Round(time.Microsecond), (totalQueryTime/time.Duration(len(queryTests))).Round(time.Microsecond))
	fmt.Printf("  Aggregations: %d, total %s, avg %s\n", len(aggTests), totalAggTime.Round(time.Microsecond), (totalAggTime/time.Duration(len(aggTests))).Round(time.Microsecond))
}
