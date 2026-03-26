package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

type conn struct {
	c net.Conn
}

func connect(addr string) *conn {
	for i := 0; i < 40; i++ {
		c, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err == nil {
			cl := &conn{c: c}
			r, _ := cl.do(map[string]any{"cmd": "ping"})
			if r != nil {
				return cl
			}
			c.Close()
		}
		time.Sleep(250 * time.Millisecond)
	}
	fmt.Println("FATAL: cannot connect to", addr)
	os.Exit(1)
	return nil
}

func (c *conn) do(payload map[string]any) (map[string]any, error) {
	data, _ := json.Marshal(payload)
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(len(data)))
	c.c.Write(buf)
	c.c.Write(data)
	if _, err := io.ReadFull(c.c, buf); err != nil {
		return nil, err
	}
	resp := make([]byte, binary.LittleEndian.Uint32(buf))
	if _, err := io.ReadFull(c.c, resp); err != nil {
		return nil, err
	}
	var r map[string]any
	json.Unmarshal(resp, &r)
	return r, nil
}

func (c *conn) must(payload map[string]any) map[string]any {
	r, err := c.do(payload)
	if err != nil {
		panic(err)
	}
	if ok, _ := r["ok"].(bool); !ok {
		e, _ := r["error"].(string)
		panic(fmt.Sprintf("error: %s", e))
	}
	return r
}

func d(r map[string]any) any   { return r["data"] }
func m(v any) map[string]any   { r, _ := v.(map[string]any); return r }
func f(v any) float64          { r, _ := v.(float64); return r }
func arr(v any) []any          { r, _ := v.([]any); return r }

func main() {
	addr := "127.0.0.1:14999"
	dataDir := filepath.Join("..", "..", "test_data", "oxiscript_bench")
	serverBin := filepath.Join("..", "..", "target", "release", "oxidb-server")

	if _, err := os.Stat(serverBin); err != nil {
		fmt.Println("FATAL: oxidb-server not found")
		os.Exit(1)
	}

	cmd := exec.Command(serverBin)
	cmd.Env = append(os.Environ(), "OXIDB_ADDR="+addr, "OXIDB_DATA="+dataDir, "OXIDB_IDLE_TIMEOUT=0")
	cmd.Start()
	defer func() { cmd.Process.Signal(syscall.SIGTERM); cmd.Wait() }()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sig; cmd.Process.Signal(syscall.SIGTERM); cmd.Wait(); os.Exit(0) }()

	c := connect(addr)
	fmt.Println("Server ready.")

	// Check data
	r := c.must(map[string]any{"cmd": "count", "collection": "orders"})
	orderCount := f(m(d(r))["count"])
	r = c.must(map[string]any{"cmd": "count", "collection": "customers"})
	custCount := f(m(d(r))["count"])
	r = c.must(map[string]any{"cmd": "count", "collection": "products"})
	prodCount := f(m(d(r))["count"])
	fmt.Printf("Data: %.0f orders, %.0f customers, %.0f products\n\n", orderCount, custCount, prodCount)

	if orderCount == 0 {
		fmt.Println("FATAL: no data. Run ./oxiscript-bench first to seed.")
		os.Exit(1)
	}

	// Recreate procedures
	fmt.Println("Creating stored procedures...")
	scripts := []struct{ name, script string }{
		{"get_balance", `proc get_balance(customer_id) {
			let cust = find_one("customers", {customer_id: customer_id})
			if cust == null { abort "not found" }
			return cust.balance
		}`},
		{"get_customer_profile", `proc get_customer_profile(customer_id) {
			let cust = find_one("customers", {customer_id: customer_id})
			if cust == null { abort "not found" }
			let cnt = count("orders", {customer_id: customer_id})
			let recent = find("orders", {customer_id: customer_id}, {_id: -1}, 5)
			return {name: cust.name, region: cust.region, tier: cust.tier, balance: cust.balance, total_orders: cnt, recent: recent}
		}`},
		{"check_stock", `proc check_stock(sku) {
			let p = find_one("products", {sku: sku})
			if p == null { abort "not found" }
			return {sku: p.sku, stock: p.stock, price: p.price}
		}`},
		{"place_order", `proc place_order(customer_id, sku, qty, total) {
			let stock = check_stock({sku: sku})
			if stock.stock < qty { abort "out of stock" }
			let cust = find_one("customers", {customer_id: customer_id})
			if cust == null { abort "not found" }
			if cust.balance < total { abort "insufficient funds" }
			update("products", {sku: sku}, {$inc: {stock: -qty, sold: qty}})
			update("customers", {customer_id: customer_id}, {$inc: {balance: -total, total_spent: total, order_count: 1}})
			insert("orders", {customer_id: customer_id, sku: sku, qty: qty, total: total, status: "completed", region: cust.region})
			return {status: "placed", total: total}
		}`},
		{"process_refund", `proc process_refund(order_id) {
			let order = find_one("orders", {order_id: order_id})
			if order == null { abort "not found" }
			if order.status != "completed" { abort "not refundable" }
			update("orders", {order_id: order_id}, {$set: {status: "refunded"}})
			update("customers", {customer_id: order.customer_id}, {$inc: {balance: order.total}})
			insert("transactions", {type: "refund", order_id: order_id, customer_id: order.customer_id, amount: order.total})
			return {refunded: order.total}
		}`},
		{"generate_report", `proc generate_report(region) {
			let summary = aggregate("orders", [
				{$match: {region: region, status: "completed"}},
				{$group: {_id: "$sku", revenue: {$sum: "$total"}, count: {$sum: 1}}},
				{$sort: {revenue: -1}},
				{$limit: 10}
			])
			let totals = aggregate("orders", [
				{$match: {region: region, status: "completed"}},
				{$group: {_id: null, total_revenue: {$sum: "$total"}, total_orders: {$sum: 1}}}
			])
			delete("report_cache", {region: region})
			insert("report_cache", {region: region, top_products: summary, totals: totals})
			return {region: region, top_products: summary, totals: totals}
		}`},
		{"get_report", `proc get_report(region) {
			let cached = find_one("report_cache", {region: region})
			if cached == null { abort "no cached report" }
			return {region: cached.region, top_products: cached.top_products, totals: cached.totals}
		}`},
	}
	for _, s := range scripts {
		c.must(map[string]any{"cmd": "create_procedure", "script": s.script})
	}
	fmt.Println("Done.\n")

	regions := []string{"US-EAST", "US-WEST", "EU-WEST", "EU-EAST", "APAC"}
	type result struct {
		name string
		dur  time.Duration
		detail string
	}
	var results []result

	// ─── 1. get_balance (indexed find_one) ─────────────────────────
	fmt.Println("─── 1. get_balance (indexed find_one + return) ───")
	for i := 0; i < 5; i++ {
		cid := fmt.Sprintf("C%04d", rand.Intn(10000))
		t0 := time.Now()
		r := c.must(map[string]any{"cmd": "call_procedure", "name": "get_balance", "params": map[string]any{"customer_id": cid}})
		dur := time.Since(t0)
		bal := f(d(r))
		fmt.Printf("  %s balance=%.0f  %s\n", cid, bal, dur.Round(time.Microsecond))
		results = append(results, result{"get_balance", dur, cid})
	}
	fmt.Println()

	// ─── 2. get_customer_profile (find + count + find top 5) ───────
	fmt.Println("─── 2. get_customer_profile (find_one + count + find top 5) ───")
	for i := 0; i < 5; i++ {
		cid := fmt.Sprintf("C%04d", rand.Intn(10000))
		t0 := time.Now()
		r := c.must(map[string]any{"cmd": "call_procedure", "name": "get_customer_profile", "params": map[string]any{"customer_id": cid}})
		dur := time.Since(t0)
		p := m(d(r))
		fmt.Printf("  %s tier=%s orders=%.0f balance=%.0f  %s\n", cid, p["tier"], f(p["total_orders"]), f(p["balance"]), dur.Round(time.Microsecond))
		results = append(results, result{"get_customer_profile", dur, cid})
	}
	fmt.Println()

	// ─── 3. check_stock (indexed find_one) ─────────────────────────
	fmt.Println("─── 3. check_stock (indexed find_one) ───")
	for i := 0; i < 5; i++ {
		sku := fmt.Sprintf("P%03d", rand.Intn(1000))
		t0 := time.Now()
		r := c.must(map[string]any{"cmd": "call_procedure", "name": "check_stock", "params": map[string]any{"sku": sku}})
		dur := time.Since(t0)
		p := m(d(r))
		fmt.Printf("  %s stock=%.0f price=%.0f  %s\n", sku, f(p["stock"]), f(p["price"]), dur.Round(time.Microsecond))
		results = append(results, result{"check_stock", dur, sku})
	}
	fmt.Println()

	// ─── 4. place_order (proc→proc + 3 writes) ────────────────────
	fmt.Println("─── 4. place_order (calls check_stock + 3 writes) ───")
	for i := 0; i < 5; i++ {
		cid := fmt.Sprintf("C%04d", rand.Intn(10000))
		sku := fmt.Sprintf("P%03d", rand.Intn(1000))
		qty := 1 + rand.Intn(3)
		total := float64(qty) * float64(10+rand.Intn(200))
		t0 := time.Now()
		r, _ := c.do(map[string]any{"cmd": "call_procedure", "name": "place_order", "params": map[string]any{
			"customer_id": cid, "sku": sku, "qty": qty, "total": total,
		}})
		dur := time.Since(t0)
		ok, _ := r["ok"].(bool)
		if ok {
			fmt.Printf("  %s → %s qty=%d total=%.0f  %s  OK\n", cid, sku, qty, total, dur.Round(time.Microsecond))
		} else {
			e, _ := r["error"].(string)
			fmt.Printf("  %s → %s qty=%d total=%.0f  %s  FAIL: %s\n", cid, sku, qty, total, dur.Round(time.Microsecond), e)
		}
		results = append(results, result{"place_order", dur, ""})
	}
	fmt.Println()

	// ─── 5. process_refund (find + validate + 3 writes) ────────────
	fmt.Println("─── 5. process_refund (find + validate + 3 writes) ───")
	for i := 0; i < 5; i++ {
		oid := fmt.Sprintf("H%06d", rand.Intn(1000000))
		t0 := time.Now()
		r, _ := c.do(map[string]any{"cmd": "call_procedure", "name": "process_refund", "params": map[string]any{"order_id": oid}})
		dur := time.Since(t0)
		ok, _ := r["ok"].(bool)
		if ok {
			p := m(d(r))
			fmt.Printf("  %s refunded=%.0f  %s  OK\n", oid, f(p["refunded"]), dur.Round(time.Microsecond))
		} else {
			e, _ := r["error"].(string)
			fmt.Printf("  %s  %s  %s\n", oid, dur.Round(time.Microsecond), e)
		}
		results = append(results, result{"process_refund", dur, ""})
	}
	fmt.Println()

	// ─── 6. generate_report (2x aggregation over 1M → cache) ──────
	fmt.Println("─── 6. generate_report (heavy aggregation → cache) ───")
	c.must(map[string]any{"cmd": "create_index", "collection": "report_cache", "field": "region"})
	for _, region := range regions {
		t0 := time.Now()
		r := c.must(map[string]any{"cmd": "call_procedure", "name": "generate_report", "params": map[string]any{"region": region}})
		dur := time.Since(t0)
		p := m(d(r))
		totals := arr(p["totals"])
		orders := 0.0
		if len(totals) > 0 {
			orders = f(m(totals[0])["total_orders"])
		}
		fmt.Printf("  %s: %.0f orders  %s (cached)\n", region, orders, dur.Round(time.Microsecond))
		results = append(results, result{"generate_report", dur, region})
	}
	fmt.Println()

	// ─── 6b. get_report (read from cache — fast) ──────────────────
	fmt.Println("─── 6b. get_report (read from cache) ───")
	for _, region := range regions {
		t0 := time.Now()
		c.must(map[string]any{"cmd": "call_procedure", "name": "get_report", "params": map[string]any{"region": region}})
		dur := time.Since(t0)
		fmt.Printf("  %s  %s\n", region, dur.Round(time.Microsecond))
		results = append(results, result{"get_report", dur, region})
	}
	fmt.Println()

	// ─── 7. Throughput: 100x get_balance (sequential) ──────────────
	fmt.Println("─── 7. Throughput: 100x get_balance (sequential) ───")
	t0 := time.Now()
	for i := 0; i < 100; i++ {
		cid := fmt.Sprintf("C%04d", rand.Intn(10000))
		c.must(map[string]any{"cmd": "call_procedure", "name": "get_balance", "params": map[string]any{"customer_id": cid}})
	}
	dur100 := time.Since(t0)
	fmt.Printf("  100 calls in %s  (avg %s, %.0f/sec)\n\n",
		dur100.Round(time.Millisecond),
		(dur100 / 100).Round(time.Microsecond),
		100.0/dur100.Seconds())

	// ─── 8. Concurrent: 10 workers x 50 mixed ops ──────────────────
	fmt.Println("─── 8. Concurrent workload (10 workers x 50 ops) ───")
	fmt.Println("  Mix: 40% place_order, 15% refund, 30% profile, 15% get_report (cached)")
	fmt.Println()

	var (
		mu          sync.Mutex
		concResults []result
		wg          sync.WaitGroup
		okCount     int
		failCount   int
	)

	wallStart := time.Now()
	for w := 0; w < 10; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			wc := connect(addr)
			defer wc.c.Close()
			rng := rand.New(rand.NewSource(int64(wid*1000) + time.Now().UnixNano()))

			for op := 0; op < 50; op++ {
				roll := rng.Intn(100)
				var name string
				var payload map[string]any

				switch {
				case roll < 40: // place_order
					name = "place_order"
					cid := fmt.Sprintf("C%04d", rng.Intn(10000))
					sku := fmt.Sprintf("P%03d", rng.Intn(1000))
					qty := 1 + rng.Intn(3)
					total := float64(qty) * float64(10+rng.Intn(200))
					payload = map[string]any{"cmd": "call_procedure", "name": "place_order", "params": map[string]any{
						"customer_id": cid, "sku": sku, "qty": qty, "total": total,
					}}
				case roll < 55: // refund
					name = "process_refund"
					oid := fmt.Sprintf("H%06d", rng.Intn(1000000))
					payload = map[string]any{"cmd": "call_procedure", "name": "process_refund", "params": map[string]any{"order_id": oid}}
				case roll < 85: // profile
					name = "get_customer_profile"
					cid := fmt.Sprintf("C%04d", rng.Intn(10000))
					payload = map[string]any{"cmd": "call_procedure", "name": "get_customer_profile", "params": map[string]any{"customer_id": cid}}
				default: // cached report
					name = "get_report"
					payload = map[string]any{"cmd": "call_procedure", "name": "get_report", "params": map[string]any{"region": regions[rng.Intn(5)]}}
				}

				t := time.Now()
				r, err := wc.do(payload)
				dur := time.Since(t)

				mu.Lock()
				concResults = append(concResults, result{name, dur, ""})
				if err == nil {
					if ok, _ := r["ok"].(bool); ok {
						okCount++
					} else {
						failCount++
					}
				} else {
					failCount++
				}
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	wallDur := time.Since(wallStart)
	totalOps := 10 * 50

	fmt.Printf("  Wall time: %s\n", wallDur.Round(time.Millisecond))
	fmt.Printf("  Total ops: %d  OK: %d  Fail: %d\n", totalOps, okCount, failCount)
	fmt.Printf("  Throughput: %.0f ops/sec\n\n", float64(totalOps)/wallDur.Seconds())

	// Concurrent per-operation breakdown
	type agg struct {
		total time.Duration
		count int
	}
	concGroups := map[string]*agg{}
	concOrder := []string{}
	for _, r := range concResults {
		if _, ok := concGroups[r.name]; !ok {
			concGroups[r.name] = &agg{}
			concOrder = append(concOrder, r.name)
		}
		concGroups[r.name].total += r.dur
		concGroups[r.name].count++
	}
	for _, name := range concOrder {
		a := concGroups[name]
		avg := a.total / time.Duration(a.count)
		fmt.Printf("  %-35s %4d calls  avg %s\n", name, a.count, avg.Round(time.Microsecond))
	}
	fmt.Println()

	// ─── Summary ───────────────────────────────────────────────────
	fmt.Println(strings.Repeat("═", 65))
	fmt.Printf("%-45s %15s\n", "Operation", "Time")
	fmt.Println(strings.Repeat("─", 65))

	seqGroups := map[string]*agg{}
	seqOrder := []string{}
	for _, r := range results {
		if _, ok := seqGroups[r.name]; !ok {
			seqGroups[r.name] = &agg{}
			seqOrder = append(seqOrder, r.name)
		}
		seqGroups[r.name].total += r.dur
		seqGroups[r.name].count++
	}

	fmt.Println("  Sequential:")
	for _, name := range seqOrder {
		a := seqGroups[name]
		avg := a.total / time.Duration(a.count)
		fmt.Printf("    %-43s %13s\n", fmt.Sprintf("%s (avg of %d)", name, a.count), avg.Round(time.Microsecond))
	}
	fmt.Printf("    %-43s %13s\n", "get_balance throughput (100x)", (dur100/100).Round(time.Microsecond))

	fmt.Println("  Concurrent (10 workers):")
	for _, name := range concOrder {
		a := concGroups[name]
		avg := a.total / time.Duration(a.count)
		fmt.Printf("    %-43s %13s\n", fmt.Sprintf("%s (avg of %d)", name, a.count), avg.Round(time.Microsecond))
	}
	fmt.Printf("    %-43s %13s\n",
		fmt.Sprintf("total throughput (%d ops)", totalOps),
		fmt.Sprintf("%.0f ops/sec", float64(totalOps)/wallDur.Seconds()))
	fmt.Println(strings.Repeat("═", 65))
}
