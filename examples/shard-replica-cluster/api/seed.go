package main

import (
	"fmt"
	"math/rand"
	"net/http"
	"time"
)

// handleSeed populates the cluster with a small synthetic dataset.
// Idempotent-ish: it creates collections + indexes (no-op if they exist) and
// appends fresh data each time it's called.
//
//   POST /api/seed?customers=200&products=50&orders=1000&events=5000
func (s *Server) handleSeed(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, 405, "POST only")
		return
	}
	customers := intQuery(r, "customers", 200)
	products := intQuery(r, "products", 50)
	orders := intQuery(r, "orders", 1000)
	events := intQuery(r, "events", 5000)

	c, err := s.connect()
	if err != nil {
		writeError(w, 502, err.Error())
		return
	}
	defer c.Close()

	report := map[string]any{}
	t0 := time.Now()

	// Indexes — safe to call repeatedly (oxidb returns OK on existing index)
	_ = c.CreateIndex("customers", "_id")
	_ = c.CreateIndex("products", "_id")
	_ = c.CreateIndex("orders", "customer_id")
	_ = c.CreateIndex("orders", "status")
	_ = c.CreateIndex("carts", "customer_id")
	_ = c.CreateIndex("events", "customer_id")
	_ = c.CreateIndex("events", "_ts")

	// Customers — unsharded, all land on shard A
	{
		batch := make([]map[string]any, customers)
		for i := 0; i < customers; i++ {
			id := i + 1
			batch[i] = map[string]any{
				"_id":   id,
				"name":  fmt.Sprintf("Customer %03d", id),
				"email": fmt.Sprintf("cust%03d@shopedge.local", id),
			}
		}
		if _, err := c.InsertMany("customers", batch); err != nil {
			writeError(w, 502, "insert customers: "+err.Error())
			return
		}
		report["customers"] = customers
	}

	// Products — unsharded, all land on shard A
	{
		batch := make([]map[string]any, products)
		categories := []string{"books", "kitchen", "audio", "outdoor", "stationery"}
		for i := 0; i < products; i++ {
			id := i + 1
			batch[i] = map[string]any{
				"_id":      id,
				"name":     fmt.Sprintf("Product %03d", id),
				"price":    9.99 + float64(rand.Intn(900))/10.0,
				"category": categories[i%len(categories)],
				"stock":    rand.Intn(500) + 50,
			}
		}
		if _, err := c.InsertMany("products", batch); err != nil {
			writeError(w, 502, "insert products: "+err.Error())
			return
		}
		report["products"] = products
	}

	// Orders — sharded by customer_id, distributed across A/B/C
	{
		statuses := []string{"pending", "paid", "shipped", "delivered", "cancelled"}
		batch := make([]map[string]any, orders)
		for i := 0; i < orders; i++ {
			cid := rand.Intn(customers) + 1
			batch[i] = map[string]any{
				"customer_id": cid,
				"total":       float64(rand.Intn(20000)) / 100.0,
				"status":      statuses[rand.Intn(len(statuses))],
				"_ts":         time.Now().Add(-time.Duration(rand.Intn(30*86400)) * time.Second).UTC().Format(time.RFC3339),
			}
		}
		if _, err := c.InsertMany("orders", batch); err != nil {
			writeError(w, 502, "insert orders: "+err.Error())
			return
		}
		report["orders"] = orders
	}

	// Events — sharded by customer_id, time-series (last 7d)
	{
		eventTypes := []string{"page_view", "search", "add_to_cart", "remove_from_cart", "checkout_start"}
		batch := make([]map[string]any, events)
		for i := 0; i < events; i++ {
			cid := rand.Intn(customers) + 1
			ago := time.Duration(rand.Intn(7*86400)) * time.Second
			batch[i] = map[string]any{
				"customer_id": cid,
				"type":        eventTypes[rand.Intn(len(eventTypes))],
				"_ts":         time.Now().Add(-ago).UTC().Format(time.RFC3339),
			}
		}
		if _, err := c.InsertMany("events", batch); err != nil {
			writeError(w, 502, "insert events: "+err.Error())
			return
		}
		report["events"] = events
	}

	report["ok"] = true
	report["took"] = time.Since(t0).String()
	report["note"] = "customers/products are unsharded (shard A); orders/carts/events sharded by customer_id"
	writeJSON(w, 200, report)
}

func intQuery(r *http.Request, key string, def int) int {
	v := r.URL.Query().Get(key)
	if v == "" {
		return def
	}
	var n int
	_, err := fmt.Sscanf(v, "%d", &n)
	if err != nil || n <= 0 {
		return def
	}
	return n
}
