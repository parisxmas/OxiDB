// Full API showcase for OxiDB Go client.
//
// Demonstrates every major feature: CRUD, updateOne/deleteOne, indexes
// (unique, composite, text), full-text search, aggregation, transactions,
// blob storage, compaction, and collection management.
//
// Usage:
//
//	go run main.go                     # connect to localhost:4444
//	go run main.go -host 10.0.0.5 -port 5500
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/parisxmas/OxiDB/go/oxidb"
)

func main() {
	host := flag.String("host", "127.0.0.1", "OxiDB server host")
	port := flag.Int("port", 4444, "OxiDB server port")
	flag.Parse()

	client, err := oxidb.Connect(*host, *port, 10*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	header("OxiDB Go Client — Full API Demo")

	// ------------------------------------------------------------------
	// 1. Ping
	// ------------------------------------------------------------------
	section("1. Ping")
	pong, _ := client.Ping()
	fmt.Printf("  Server says: %s\n", pong)

	// ------------------------------------------------------------------
	// 2. Collection management
	// ------------------------------------------------------------------
	section("2. Collection Management")

	// Clean up from previous runs
	_ = client.DropCollection("orders")
	_ = client.DropCollection("articles")

	must(client.CreateCollection("orders"))
	fmt.Println("  Created collection: orders")

	cols, _ := client.ListCollections()
	fmt.Printf("  Collections: %v\n", cols)

	// ------------------------------------------------------------------
	// 3. Insert & InsertMany
	// ------------------------------------------------------------------
	section("3. Insert & InsertMany")

	res, _ := client.Insert("orders", map[string]any{
		"order_id": 1, "customer": "Alice", "amount": 250.00,
		"status": "completed", "category": "electronics",
		"created_at": "2024-03-15T10:30:00Z",
	})
	fmt.Printf("  Inserted 1 doc: id=%v\n", res["id"])

	sampleOrders := []map[string]any{
		{"order_id": 2, "customer": "Bob", "amount": 89.99, "status": "pending", "category": "books", "created_at": "2024-04-01T08:00:00Z"},
		{"order_id": 3, "customer": "Charlie", "amount": 1200.00, "status": "completed", "category": "electronics", "created_at": "2024-04-10T14:22:00Z"},
		{"order_id": 4, "customer": "Diana", "amount": 45.50, "status": "cancelled", "category": "clothing", "created_at": "2024-05-01T09:15:00Z"},
		{"order_id": 5, "customer": "Eve", "amount": 320.00, "status": "shipped", "category": "electronics", "created_at": "2024-05-20T16:00:00Z"},
		{"order_id": 6, "customer": "Frank", "amount": 15.00, "status": "pending", "category": "books", "created_at": "2024-06-01T11:30:00Z"},
		{"order_id": 7, "customer": "Grace", "amount": 550.00, "status": "completed", "category": "home", "created_at": "2024-06-15T13:45:00Z"},
		{"order_id": 8, "customer": "Hank", "amount": 99.99, "status": "shipped", "category": "clothing", "created_at": "2024-07-01T07:00:00Z"},
		{"order_id": 9, "customer": "Ivy", "amount": 2100.00, "status": "completed", "category": "electronics", "created_at": "2024-07-20T20:00:00Z"},
		{"order_id": 10, "customer": "Jack", "amount": 35.00, "status": "pending", "category": "books", "created_at": "2024-08-05T15:30:00Z"},
	}
	_, err = client.InsertMany("orders", sampleOrders)
	must(err)
	fmt.Printf("  Inserted %d docs via InsertMany\n", len(sampleOrders))

	cnt, _ := client.Count("orders", nil)
	fmt.Printf("  Total count: %d\n", cnt)

	// ------------------------------------------------------------------
	// 4. Indexes: field, unique, composite, text
	// ------------------------------------------------------------------
	section("4. Index Creation")

	must(client.CreateIndex("orders", "status"))
	fmt.Println("  Field index: status")

	must(client.CreateIndex("orders", "category"))
	fmt.Println("  Field index: category")

	must(client.CreateIndex("orders", "amount"))
	fmt.Println("  Field index: amount")

	must(client.CreateUniqueIndex("orders", "order_id"))
	fmt.Println("  Unique index: order_id")

	must(client.CreateCompositeIndex("orders", []string{"status", "category"}))
	fmt.Println("  Composite index: status+category")

	indexes, _ := client.ListIndexes("orders")
	fmt.Printf("  Indexes on 'orders': %d total\n", len(indexes))
	for _, idx := range indexes {
		pretty, _ := json.Marshal(idx)
		fmt.Printf("    %s\n", pretty)
	}

	// ------------------------------------------------------------------
	// 5. Find, FindOne, sort, skip, limit
	// ------------------------------------------------------------------
	section("5. Queries")

	// Simple equality
	docs, _ := client.Find("orders", map[string]any{"status": "completed"}, nil)
	fmt.Printf("  status=completed: %d docs\n", len(docs))

	// Range query
	docs, _ = client.Find("orders", map[string]any{
		"$and": []any{
			map[string]any{"amount": map[string]any{"$gte": 100}},
			map[string]any{"amount": map[string]any{"$lte": 1000}},
		},
	}, nil)
	fmt.Printf("  amount 100-1000: %d docs\n", len(docs))

	// Sort + limit
	limit3 := 3
	docs, _ = client.Find("orders", map[string]any{}, &oxidb.FindOptions{
		Sort:  map[string]any{"amount": -1},
		Limit: &limit3,
	})
	fmt.Println("  Top 3 by amount (desc):")
	for _, d := range docs {
		fmt.Printf("    order_id=%v  amount=%v  customer=%v\n",
			d["order_id"], d["amount"], d["customer"])
	}

	// FindOne
	one, _ := client.FindOne("orders", map[string]any{"order_id": 5})
	fmt.Printf("  FindOne(order_id=5): customer=%v\n", one["customer"])

	// ------------------------------------------------------------------
	// 6. UpdateOne & Update
	// ------------------------------------------------------------------
	section("6. UpdateOne & Update")

	upRes, _ := client.UpdateOne("orders",
		map[string]any{"order_id": 6},
		map[string]any{"$set": map[string]any{"status": "shipped", "tracking": "TRACK-006"}})
	fmt.Printf("  UpdateOne(order_id=6 -> shipped): modified=%v\n", upRes["modified"])

	upRes, _ = client.Update("orders",
		map[string]any{"status": "pending"},
		map[string]any{"$set": map[string]any{"priority": "high"}})
	fmt.Printf("  Update(all pending -> priority=high): modified=%v\n", upRes["modified"])

	// Verify
	one, _ = client.FindOne("orders", map[string]any{"order_id": 6})
	fmt.Printf("  Verify order_id=6: status=%v, tracking=%v\n", one["status"], one["tracking"])

	// ------------------------------------------------------------------
	// 7. DeleteOne & Delete
	// ------------------------------------------------------------------
	section("7. DeleteOne & Delete")

	delRes, _ := client.DeleteOne("orders", map[string]any{"order_id": 4})
	fmt.Printf("  DeleteOne(order_id=4): deleted=%v\n", delRes["deleted"])

	cnt, _ = client.Count("orders", nil)
	fmt.Printf("  Count after delete: %d\n", cnt)

	// ------------------------------------------------------------------
	// 8. Aggregation
	// ------------------------------------------------------------------
	section("8. Aggregation")

	// Group by status
	agg, _ := client.Aggregate("orders", []map[string]any{
		{"$group": map[string]any{"_id": "$status", "count": map[string]any{"$sum": 1}}},
		{"$sort": map[string]any{"count": -1}},
	})
	fmt.Println("  Group by status:")
	for _, row := range agg {
		fmt.Printf("    %v: %v orders\n", row["_id"], row["count"])
	}

	// Revenue by category
	agg, _ = client.Aggregate("orders", []map[string]any{
		{"$group": map[string]any{"_id": "$category", "revenue": map[string]any{"$sum": "$amount"}}},
		{"$sort": map[string]any{"revenue": -1}},
	})
	fmt.Println("  Revenue by category:")
	for _, row := range agg {
		fmt.Printf("    %v: $%.2f\n", row["_id"], row["revenue"])
	}

	// Match + group
	agg, _ = client.Aggregate("orders", []map[string]any{
		{"$match": map[string]any{"status": "completed"}},
		{"$group": map[string]any{"_id": "$category", "avg": map[string]any{"$avg": "$amount"}, "n": map[string]any{"$sum": 1}}},
		{"$sort": map[string]any{"avg": -1}},
	})
	fmt.Println("  Completed orders — avg amount by category:")
	for _, row := range agg {
		fmt.Printf("    %v: avg=$%.2f (%v orders)\n", row["_id"], row["avg"], row["n"])
	}

	// ------------------------------------------------------------------
	// 9. Transactions
	// ------------------------------------------------------------------
	section("9. Transactions")

	err = client.WithTransaction(func() error {
		_, err := client.Insert("orders", map[string]any{
			"order_id": 100, "customer": "TxAlice", "amount": 500.0,
			"status": "completed", "category": "home", "created_at": "2024-09-01T00:00:00Z",
		})
		if err != nil {
			return err
		}
		_, err = client.Insert("orders", map[string]any{
			"order_id": 101, "customer": "TxBob", "amount": 300.0,
			"status": "pending", "category": "books", "created_at": "2024-09-01T00:00:00Z",
		})
		return err
	})
	must(err)
	fmt.Println("  Transaction committed (2 inserts)")

	cnt, _ = client.Count("orders", nil)
	fmt.Printf("  Count after tx: %d\n", cnt)

	// ------------------------------------------------------------------
	// 10. Full-Text Search (collection-level)
	// ------------------------------------------------------------------
	section("10. Full-Text Search")

	_ = client.DropCollection("articles")
	client.InsertMany("articles", []map[string]any{
		{"title": "Getting Started with Rust", "body": "Rust is a systems programming language focused on safety, speed, and concurrency."},
		{"title": "Go for Backend Services", "body": "Go excels at building fast, concurrent backend services and APIs."},
		{"title": "Rust and WebAssembly", "body": "Rust compiles to WebAssembly for fast and safe web applications."},
		{"title": "Database Design Patterns", "body": "Document databases store data as JSON documents, offering flexibility."},
		{"title": "Building with Go and gRPC", "body": "gRPC and Go make a powerful combination for microservices."},
	})
	fmt.Println("  Inserted 5 articles")

	must(client.CreateTextIndex("articles", []string{"title", "body"}))
	fmt.Println("  Created text index on [title, body]")

	results, _ := client.TextSearch("articles", "Rust", 10)
	fmt.Printf("  TextSearch('Rust'): %d results\n", len(results))
	for _, r := range results {
		fmt.Printf("    %v (score: %v)\n", r["title"], r["_score"])
	}

	results, _ = client.TextSearch("articles", "Go backend", 10)
	fmt.Printf("  TextSearch('Go backend'): %d results\n", len(results))
	for _, r := range results {
		fmt.Printf("    %v (score: %v)\n", r["title"], r["_score"])
	}

	// ------------------------------------------------------------------
	// 11. Blob Storage
	// ------------------------------------------------------------------
	section("11. Blob Storage")

	_ = client.DeleteBucket("assets")
	must(client.CreateBucket("assets"))
	fmt.Println("  Created bucket: assets")

	testData := []byte("Hello from OxiDB Go client!")
	meta, err := client.PutObject("assets", "greeting.txt", testData, "text/plain",
		map[string]string{"author": "go-demo"})
	must(err)
	fmt.Printf("  PutObject: etag=%v, size=%v\n", meta["etag"], meta["size"])

	data, objMeta, err := client.GetObject("assets", "greeting.txt")
	must(err)
	fmt.Printf("  GetObject: %q, content_type=%v\n", string(data), objMeta["content_type"])

	headMeta, _ := client.HeadObject("assets", "greeting.txt")
	fmt.Printf("  HeadObject: size=%v, content_type=%v\n", headMeta["size"], headMeta["content_type"])

	objs, _ := client.ListObjects("assets", nil, nil)
	fmt.Printf("  ListObjects: %d objects\n", len(objs))

	must(client.DeleteObject("assets", "greeting.txt"))
	fmt.Println("  Deleted greeting.txt")

	buckets, _ := client.ListBuckets()
	fmt.Printf("  Buckets: %v\n", buckets)

	must(client.DeleteBucket("assets"))
	fmt.Println("  Deleted bucket: assets")

	// ------------------------------------------------------------------
	// 12. Compact
	// ------------------------------------------------------------------
	section("12. Compact")

	// Delete some docs first so compaction has something to reclaim
	client.Delete("orders", map[string]any{"status": "cancelled"})
	stats, _ := client.Compact("orders")
	fmt.Printf("  Compact: old_size=%v, new_size=%v, docs_kept=%v\n",
		stats["old_size"], stats["new_size"], stats["docs_kept"])

	// ------------------------------------------------------------------
	// 13. Drop Index
	// ------------------------------------------------------------------
	section("13. Drop Index")

	must(client.DropIndex("orders", "amount"))
	fmt.Println("  Dropped index: amount")

	indexes, _ = client.ListIndexes("orders")
	fmt.Printf("  Remaining indexes: %d\n", len(indexes))

	// ------------------------------------------------------------------
	// Cleanup
	// ------------------------------------------------------------------
	section("Cleanup")
	_ = client.DropCollection("orders")
	_ = client.DropCollection("articles")
	fmt.Println("  Dropped collections: orders, articles")

	cnt, _ = client.Count("orders", nil)
	fmt.Printf("  Final count: %d\n", cnt)

	header("Done!")
}

func header(title string) {
	w := 60
	fmt.Println()
	fmt.Printf("  %s\n", strings.Repeat("=", w))
	fmt.Printf("  %-*s\n", w, title)
	fmt.Printf("  %s\n", strings.Repeat("=", w))
	fmt.Println()
}

func section(title string) {
	fmt.Printf("\n  --- %s ---\n\n", title)
}

func must(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: %v\n", err)
		os.Exit(1)
	}
}
