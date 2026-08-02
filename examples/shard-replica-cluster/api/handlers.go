package main

import (
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// ─── Index / landing ─────────────────────────────────────────────────

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = fmt.Fprintf(w, `<!doctype html>
<html><head><title>ShopEdge API</title>
<style>
  body{background:#0b0d11;color:#e7e9ed;font-family:ui-monospace,Menlo,monospace;
       padding:48px;max-width:880px;margin:auto;line-height:1.55}
  h1{font-weight:500;letter-spacing:-.02em;color:#e2784a}
  h2{font-size:14px;letter-spacing:.18em;text-transform:uppercase;color:#7c9eb4;margin-top:36px}
  a{color:#e2784a;text-decoration:none;border-bottom:1px dashed #b04a1a}
  a:hover{color:#fff}
  code{background:#181d26;padding:2px 8px;border-radius:3px;color:#e7e9ed;border:1px solid #262d39}
  ul{padding-left:18px}
  li{margin:6px 0}
</style></head><body>
<h1>ShopEdge // OxiDB sharded + replicated cluster</h1>
<p>Three shards · nine Raft-replicated nodes · two-tier oxipool routing.</p>
<h2>Diagnostics</h2>
<ul>
  <li><a href="/api/health">/api/health</a> — ping every node + every pool</li>
  <li><a href="/api/topology">/api/topology</a> — configured topology</li>
  <li><a href="/api/raft/metrics">/api/raft/metrics</a> — Raft state of all 9 nodes</li>
</ul>
<h2>Data</h2>
<ul>
  <li><code>POST /api/seed</code> — populate customers / products / orders / events</li>
  <li><code>GET  /api/products</code> — catalog (unsharded → shard A replicas)</li>
  <li><code>POST /api/cart</code> body: <code>{customer_id, product_id, qty}</code> (sharded write)</li>
  <li><code>GET  /api/cart/:customer_id</code> (sharded read)</li>
  <li><code>POST /api/checkout</code> body: <code>{customer_id}</code> (TX-pinned to master)</li>
  <li><code>GET  /api/orders/:customer_id</code> (sharded read)</li>
  <li><code>GET  /api/orders?status=pending</code> (scatter-gather)</li>
</ul>
</body></html>`)
}

// ─── Topology ────────────────────────────────────────────────────────

func (s *Server) handleTopology(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, 200, map[string]any{
		"ok": true,
		"topology": map[string]any{
			"router": fmt.Sprintf("%s:%d", s.RouterHost, s.RouterPort),
			"shards": []map[string]any{
				{"name": "A", "pool": "pool-shard-a:4446", "master": "db-a0:4444", "replicas": []string{"db-a1:4444", "db-a2:4444"}},
				{"name": "B", "pool": "pool-shard-b:4446", "master": "db-b0:4444", "replicas": []string{"db-b1:4444", "db-b2:4444"}},
				{"name": "C", "pool": "pool-shard-c:4446", "master": "db-c0:4444", "replicas": []string{"db-c1:4444", "db-c2:4444"}},
			},
			"shard_keys": map[string]string{
				"orders": "customer_id",
				"carts":  "customer_id",
				"events": "customer_id",
			},
			"unsharded": []string{"products", "categories", "customers"},
			"chunks":    256,
		},
	})
}

// ─── Health ──────────────────────────────────────────────────────────

type healthResult struct {
	Target string `json:"target"`
	OK     bool   `json:"ok"`
	Error  string `json:"error,omitempty"`
	Took   string `json:"took"`
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	results := make([]healthResult, 0, len(s.DirectNodes)+1)

	results = append(results, pingTarget(fmt.Sprintf("%s:%d (router)", s.RouterHost, s.RouterPort), func() error {
		c, err := s.connect()
		if err != nil {
			return err
		}
		defer c.Close()
		_, err = c.Ping()
		return err
	}))

	for _, hp := range s.DirectNodes {
		hp := hp
		results = append(results, pingTarget(hp.String(), func() error {
			c, err := s.connectDirect(hp)
			if err != nil {
				return err
			}
			defer c.Close()
			_, err = c.Ping()
			return err
		}))
	}

	healthy := 0
	for _, r := range results {
		if r.OK {
			healthy++
		}
	}
	writeJSON(w, 200, map[string]any{
		"ok":      healthy == len(results),
		"healthy": healthy,
		"total":   len(results),
		"results": results,
	})
}

func pingTarget(name string, fn func() error) healthResult {
	r := healthResult{Target: name}
	start := time.Now()
	if err := fn(); err != nil {
		r.OK = false
		r.Error = err.Error()
	} else {
		r.OK = true
	}
	r.Took = time.Since(start).String()
	return r
}

// ─── Raft metrics — directly query each node ─────────────────────────

func (s *Server) handleRaftMetrics(w http.ResponseWriter, _ *http.Request) {
	type entry struct {
		Node    string         `json:"node"`
		OK      bool           `json:"ok"`
		Metrics map[string]any `json:"metrics,omitempty"`
		Error   string         `json:"error,omitempty"`
	}

	out := make([]entry, len(s.DirectNodes))
	var wg sync.WaitGroup
	for i, hp := range s.DirectNodes {
		i, hp := i, hp
		wg.Add(1)
		go func() {
			defer wg.Done()
			e := entry{Node: hp.String()}
			resp, err := rawCommand(hp, map[string]any{"cmd": "raft_metrics"}, 3*time.Second)
			if err != nil {
				e.Error = err.Error()
				out[i] = e
				return
			}
			e.OK = true
			if data, ok := resp["data"].(map[string]any); ok {
				e.Metrics = data
			}
			out[i] = e
		}()
	}
	wg.Wait()

	writeJSON(w, 200, map[string]any{
		"ok":      true,
		"results": out,
	})
}

// ─── Catalog ─────────────────────────────────────────────────────────

func (s *Server) handleProducts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, 405, "GET only")
		return
	}
	c, err := s.connect()
	if err != nil {
		writeError(w, 502, err.Error())
		return
	}
	defer c.Close()

	docs, err := c.Find("products", map[string]any{}, nil)
	if err != nil {
		writeError(w, 502, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{
		"ok":       true,
		"products": docs,
		"count":    len(docs),
		"note":     "products is unsharded → routed to shard A; reads served by replicas",
	})
}

// ─── Cart ────────────────────────────────────────────────────────────

func (s *Server) handleCart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, 405, "POST only")
		return
	}
	var body struct {
		CustomerID int    `json:"customer_id"`
		ProductID  int    `json:"product_id"`
		Qty        int    `json:"qty"`
		Note       string `json:"note,omitempty"`
	}
	if err := readJSON(r, &body); err != nil {
		writeError(w, 400, "invalid JSON: "+err.Error())
		return
	}
	if body.CustomerID == 0 || body.ProductID == 0 {
		writeError(w, 400, "customer_id and product_id are required")
		return
	}
	if body.Qty <= 0 {
		body.Qty = 1
	}

	c, err := s.connect()
	if err != nil {
		writeError(w, 502, err.Error())
		return
	}
	defer c.Close()

	doc := map[string]any{
		"customer_id": body.CustomerID,
		"product_id":  body.ProductID,
		"qty":         body.Qty,
		"_ts":         time.Now().UTC().Format(time.RFC3339),
	}
	if body.Note != "" {
		doc["note"] = body.Note
	}
	resp, err := c.Insert("carts", doc)
	if err != nil {
		writeError(w, 502, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{
		"ok":       true,
		"inserted": resp,
		"routed":   fmt.Sprintf("CRC32(customer_id=%d) → shard %s", body.CustomerID, shardOf(body.CustomerID)),
	})
}

func (s *Server) handleCartByCustomer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, 405, "GET only")
		return
	}
	id, err := strconv.Atoi(pathTail(r.URL.Path, "/api/cart/"))
	if err != nil {
		writeError(w, 400, "customer_id must be an integer")
		return
	}
	c, err := s.connect()
	if err != nil {
		writeError(w, 502, err.Error())
		return
	}
	defer c.Close()

	docs, err := c.Find("carts", map[string]any{"customer_id": id}, nil)
	if err != nil {
		writeError(w, 502, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{
		"ok":          true,
		"customer_id": id,
		"items":       docs,
		"count":       len(docs),
		"routed":      fmt.Sprintf("CRC32(customer_id=%d) → shard %s (replica)", id, shardOf(id)),
	})
}

// ─── Checkout (TX-pinned) ────────────────────────────────────────────

func (s *Server) handleCheckout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, 405, "POST only")
		return
	}
	var body struct {
		CustomerID int `json:"customer_id"`
	}
	if err := readJSON(r, &body); err != nil {
		writeError(w, 400, "invalid JSON: "+err.Error())
		return
	}
	if body.CustomerID == 0 {
		writeError(w, 400, "customer_id is required")
		return
	}

	c, err := s.connect()
	if err != nil {
		writeError(w, 502, err.Error())
		return
	}
	defer c.Close()

	var orderID any
	var total float64
	var lineCount int

	err = c.WithTransaction(func() error {
		// Read cart inside the TX — sees uncommitted master state
		lines, err := c.Find("carts", map[string]any{"customer_id": body.CustomerID}, nil)
		if err != nil {
			return fmt.Errorf("find cart: %w", err)
		}
		if len(lines) == 0 {
			return fmt.Errorf("cart for customer %d is empty", body.CustomerID)
		}
		lineCount = len(lines)

		for _, ln := range lines {
			qty, _ := numberOf(ln["qty"])
			total += qty * 10.0
		}

		order := map[string]any{
			"customer_id": body.CustomerID,
			"items":       lines,
			"total":       total,
			"status":      "pending",
			"_ts":         time.Now().UTC().Format(time.RFC3339),
		}
		resp, err := c.Insert("orders", order)
		if err != nil {
			return fmt.Errorf("insert order: %w", err)
		}
		if resp != nil {
			orderID = resp["_id"]
		}

		if _, err := c.Delete("carts", map[string]any{"customer_id": body.CustomerID}); err != nil {
			return fmt.Errorf("clear cart: %w", err)
		}
		return nil
	})

	if err != nil {
		writeError(w, 400, err.Error())
		return
	}

	writeJSON(w, 200, map[string]any{
		"ok":          true,
		"order_id":    orderID,
		"customer_id": body.CustomerID,
		"line_count":  lineCount,
		"total":       total,
		"routed":      fmt.Sprintf("TX pinned to shard %s master", shardOf(body.CustomerID)),
	})
}

// ─── Orders ──────────────────────────────────────────────────────────

func (s *Server) handleOrdersByCustomer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, 405, "GET only")
		return
	}
	id, err := strconv.Atoi(pathTail(r.URL.Path, "/api/orders/"))
	if err != nil {
		writeError(w, 400, "customer_id must be an integer")
		return
	}
	c, err := s.connect()
	if err != nil {
		writeError(w, 502, err.Error())
		return
	}
	defer c.Close()

	docs, err := c.Find("orders", map[string]any{"customer_id": id}, nil)
	if err != nil {
		writeError(w, 502, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{
		"ok":          true,
		"customer_id": id,
		"orders":      docs,
		"count":       len(docs),
		"routed":      fmt.Sprintf("CRC32(customer_id=%d) → shard %s (replica)", id, shardOf(id)),
	})
}

func (s *Server) handleOrdersScatter(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, 405, "GET only")
		return
	}
	status := r.URL.Query().Get("status")
	q := map[string]any{}
	if status != "" {
		q["status"] = status
	}

	c, err := s.connect()
	if err != nil {
		writeError(w, 502, err.Error())
		return
	}
	defer c.Close()

	docs, err := c.Find("orders", q, nil)
	if err != nil {
		writeError(w, 502, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{
		"ok":     true,
		"query":  q,
		"orders": docs,
		"count":  len(docs),
		"routed": "no shard key in query → scatter-gather across all 3 shards",
	})
}

// ─── Helpers ─────────────────────────────────────────────────────────

// shardOf is a hint for human-readable responses. The router does the real
// CRC32 routing in oxipool/src/shard.rs; this mirrors its math: CRC32(key)
// → 256 chunks → chunk_id % num_shards.
func shardOf(customerID int) string {
	h := crc32IEEE([]byte(strconv.Itoa(customerID)))
	chunk := h % 256
	num := []string{"A", "B", "C"}
	return num[int(chunk)%3]
}

func numberOf(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	}
	return 0, false
}
