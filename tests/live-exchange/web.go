package main

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// web serves a live price dashboard. The browser connects over WebSocket
// to /ws; this process polls the exchange's `symbols` and `trades`
// collections from OxiDB and pushes updates. Minimal RFC-6455 server
// (server→client text frames only) so the harness needs no Go deps.
func web() {
	port := os.Getenv("WEB_PORT")
	if port == "" {
		port = "8090"
	}
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(dashboardHTML))
	})
	http.HandleFunc("/ws", serveWS)
	http.HandleFunc("/candles", serveCandles)
	http.HandleFunc("/allcandles", serveAllCandles)
	http.HandleFunc("/candles24", serveCandles24)
	http.HandleFunc("/metrics-json", serveMetricsJSON)
	fmt.Printf("[web] dashboard on http://localhost:%s  (WS /ws)\n", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		fmt.Println("[web] error:", err)
	}
}

// serveCandles returns a symbol's OHLCV history (oldest→newest) as JSON for
// the candlestick chart. GET /candles?sym=BTC&n=120
func serveCandles(w http.ResponseWriter, r *http.Request) {
	sym := r.URL.Query().Get("sym")
	n := 150
	if v := r.URL.Query().Get("n"); v != "" {
		if k, err := strconv.Atoi(v); err == nil && k > 0 {
			n = k
		}
	}
	db, err := Dial()
	if err != nil {
		http.Error(w, "db", 500)
		return
	}
	// Newest n by ts, then reversed to chronological order for plotting.
	rows, _ := db.Find("candles", map[string]any{"sym": sym}, map[string]any{"ts": -1}, n)
	out := make([]map[string]any, 0, len(rows))
	for i := len(rows) - 1; i >= 0; i-- {
		r := rows[i]
		out = append(out, map[string]any{
			"ts": getF(r, "ts"), "o": getF(r, "o"), "h": getF(r, "h"),
			"l": getF(r, "l"), "c": getF(r, "c"), "v": getF(r, "v"),
		})
	}
	w.Header().Set("Content-Type", "application/json")
	b, _ := json.Marshal(map[string]any{"sym": sym, "candles": out})
	w.Write(b)
}

// serveMetricsJSON scrapes OxiDB's Prometheus endpoint server-side (no CORS
// for the browser) and returns the counters the dashboard graphs. The client
// turns the counter deltas into per-second rates. GET /metrics-json
func serveMetricsJSON(w http.ResponseWriter, r *http.Request) {
	url := os.Getenv("METRICS_URL")
	if url == "" {
		url = "http://127.0.0.1:14580/metrics"
	}
	out := map[string]float64{"at": float64(time.Now().UnixMilli())}
	resp, err := http.Get(url)
	if err == nil {
		defer resp.Body.Close()
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			ln := sc.Text()
			if strings.HasPrefix(ln, "#") {
				continue
			}
			f := strings.Fields(ln)
			if len(f) < 2 {
				continue
			}
			v, e := strconv.ParseFloat(f[len(f)-1], 64)
			if e != nil {
				continue
			}
			name := f[0]
			switch {
			case name == "oxidb_tx_commits_total":
				out["commits"] = v
			case name == "oxidb_tx_conflicts_total":
				out["conflicts"] = v
			case strings.HasPrefix(name, "oxidb_commands_total{"):
				for _, cls := range []string{"insert", "find", "update", "delete", "tx"} {
					if strings.Contains(name, "class=\""+cls+"\"") {
						out[cls] = v
					}
				}
			}
		}
	}
	// OxiDB's own process memory + CPU. Its Prometheus process gauges are
	// Linux-only, so read them cross-platform via `ps` on the server PID
	// (passed in by run.sh). RSS in MB, CPU as a percent of one core.
	if rss, cpu, ok := procStats(); ok {
		out["rss_mb"] = rss
		out["cpu_pct"] = cpu
	}
	// Trader count + cumulative trade counter from OxiMem.
	if mem, err := DialResp(); err == nil {
		if v, err := mem.Do("KEYS", "usd:*"); err == nil {
			if arr, ok := v.([]any); ok {
				out["traders"] = float64(len(arr))
			}
		}
		if v, err := mem.Do("GET", "trades:count"); err == nil {
			if n, err := strconv.ParseFloat(str(v), 64); err == nil {
				out["trades_count"] = n
			}
		}
		mem.Close()
	}
	w.Header().Set("Content-Type", "application/json")
	b, _ := json.Marshal(out)
	w.Write(b)
}

func procStats() (float64, float64, bool) {
	pid := os.Getenv("SERVER_PID")
	if pid == "" {
		return 0, 0, false
	}
	out, err := exec.Command("ps", "-o", "rss=,%cpu=", "-p", pid).Output()
	if err != nil {
		return 0, 0, false
	}
	f := strings.Fields(string(out))
	if len(f) < 2 {
		return 0, 0, false
	}
	rssKB, _ := strconv.ParseFloat(f[0], 64)
	cpu, _ := strconv.ParseFloat(f[1], 64)
	return rssKB / 1024.0, cpu, true
}

// serveCandles24 returns a symbol's candles at a chosen timeframe for the big
// chart. GET /candles24?sym=BTC&tf=15  (tf = minutes). Sub-5-minute frames are
// rolled up from the fine 2s series; 5-minute-and-up from the 24h base series,
// aggregated on the fly into tf-minute buckets. Newest window, chronological.
func serveCandles24(w http.ResponseWriter, r *http.Request) {
	sym := r.URL.Query().Get("sym")
	tf := 15
	if v := r.URL.Query().Get("tf"); v != "" {
		if k, err := strconv.Atoi(v); err == nil && k > 0 {
			tf = k
		}
	}
	db, err := Dial()
	if err != nil {
		http.Error(w, "db", 500)
		return
	}
	src := "hcandles" // 5-min base (24h)
	if tf < 5 {
		src = "candles" // 2s base (last ~15 min)
	}
	// Pull the whole base series (oldest→newest) and bucket by tf minutes.
	rows, _ := db.Find(src, map[string]any{"sym": sym}, map[string]any{"ts": 1}, 0)
	bucket := int64(tf) * 60
	type cndl struct{ ts int64; o, h, l, c, v float64 }
	var out []cndl
	for _, r := range rows {
		ts := int64(getF(r, "ts"))
		bs := ts - ts%bucket
		o, h, l, c, v := getF(r, "o"), getF(r, "h"), getF(r, "l"), getF(r, "c"), getF(r, "v")
		if len(out) > 0 && out[len(out)-1].ts == bs {
			k := &out[len(out)-1]
			if h > k.h {
				k.h = h
			}
			if l < k.l {
				k.l = l
			}
			k.c = c
			k.v += v
		} else {
			out = append(out, cndl{bs, o, h, l, c, v})
		}
	}
	const max = 220
	if len(out) > max {
		out = out[len(out)-max:]
	}
	arr := make([]map[string]any, len(out))
	for i, k := range out {
		arr[i] = map[string]any{"ts": k.ts, "o": k.o, "h": k.h, "l": k.l, "c": k.c, "v": k.v}
	}
	w.Header().Set("Content-Type", "application/json")
	b, _ := json.Marshal(map[string]any{"sym": sym, "tf": tf, "candles": arr})
	w.Write(b)
}

// serveAllCandles returns recent candles for EVERY symbol in one request,
// feeding the per-card mini charts without a fetch storm. GET /allcandles?n=40
func serveAllCandles(w http.ResponseWriter, r *http.Request) {
	n := 40
	if v := r.URL.Query().Get("n"); v != "" {
		if k, err := strconv.Atoi(v); err == nil && k > 0 {
			n = k
		}
	}
	db, err := Dial()
	if err != nil {
		http.Error(w, "db", 500)
		return
	}
	out := map[string][]map[string]any{}
	for _, s := range symbols {
		rows, _ := db.Find("candles", map[string]any{"sym": s}, map[string]any{"ts": -1}, n)
		cs := make([]map[string]any, 0, len(rows))
		for i := len(rows) - 1; i >= 0; i-- { // chronological
			r := rows[i]
			cs = append(cs, map[string]any{
				"o": getF(r, "o"), "h": getF(r, "h"),
				"l": getF(r, "l"), "c": getF(r, "c"),
			})
		}
		out[s] = cs
	}
	w.Header().Set("Content-Type", "application/json")
	b, _ := json.Marshal(out)
	w.Write(b)
}

func serveWS(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		http.Error(w, "not a websocket request", 400)
		return
	}
	h := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	accept := base64.StdEncoding.EncodeToString(h[:])

	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "no hijack", 500)
		return
	}
	conn, buf, err := hj.Hijack()
	if err != nil {
		return
	}
	defer conn.Close()
	fmt.Fprintf(buf, "HTTP/1.1 101 Switching Protocols\r\n"+
		"Upgrade: websocket\r\nConnection: Upgrade\r\n"+
		"Sec-WebSocket-Accept: %s\r\n\r\n", accept)
	buf.Flush()

	db, err := Dial()
	if err != nil {
		return
	}
	// Reader goroutine: drain client frames so a browser close is noticed.
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		tmp := make([]byte, 512)
		for {
			if _, err := conn.Read(tmp); err != nil {
				return
			}
		}
	}()

	// Snapshot push cadence. Books/prices come from OxiMem now (microsecond
	// reads), so a fast tick is cheap — 250ms default; WS_TICK_MS overrides
	// (the 4-core shared server deploy sets it higher).
	tickMs := 250
	if v := os.Getenv("WS_TICK_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 50 {
			tickMs = n
		}
	}
	tick := time.NewTicker(time.Duration(tickMs) * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-closed:
			return
		case <-tick.C:
			payload := snapshot(db)
			if err := writeTextFrame(buf, payload); err != nil {
				return
			}
			if err := buf.Flush(); err != nil {
				return
			}
		}
	}
}

// level is one aggregated price level in the order book.
type level struct {
	P float64 `json:"p"`
	Q float64 `json:"q"`
}

// memLevels reads a book side from an OxiMem ZSET and aggregates it into at
// most n price levels: bids ≤ mid (best/highest first), asks ≥ mid (best/
// lowest first). Marketable orders beyond the mid are in flight for the
// matcher and are hidden, keeping the displayed book uncrossed.
func memLevels(mem *Resp, key string, bids bool, mid float64, n int) []level {
	if mem == nil {
		return nil
	}
	var raw any
	var err error
	if bids {
		raw, err = mem.Do("ZREVRANGE", key, "0", "79", "WITHSCORES")
	} else {
		raw, err = mem.Do("ZRANGE", key, "0", "79", "WITHSCORES")
	}
	if err != nil {
		return nil
	}
	arr, _ := raw.([]any)
	var out []level
	for i := 0; i+1 < len(arr); i += 2 {
		member := str(arr[i])
		price, _ := strconv.ParseFloat(str(arr[i+1]), 64)
		if bids && price > mid {
			continue // marketable bid in flight
		}
		if !bids && price < mid {
			continue
		}
		qty := 0.0
		if parts := strings.SplitN(member, "|", 3); len(parts) == 3 {
			qty, _ = strconv.ParseFloat(parts[2], 64)
		}
		if len(out) > 0 && out[len(out)-1].P == price {
			out[len(out)-1].Q += qty
			continue
		}
		if len(out) >= n {
			break
		}
		out = append(out, level{price, qty})
	}
	return out
}

// levels collapses same-price orders (already price-sorted) into at most
// n depth levels with summed quantity.
func levels(orders []map[string]any, n int) []level {
	var out []level
	for _, o := range orders {
		p, q := getF(o, "price"), getF(o, "remaining")
		if len(out) > 0 && out[len(out)-1].P == p {
			out[len(out)-1].Q += q
			continue
		}
		if len(out) >= n {
			break
		}
		out = append(out, level{p, q})
	}
	return out
}

// snapshot builds the JSON pushed to the browser: for each symbol its
// last price, order-book depth (bids/asks), and recent trades.
func snapshot(db *Client) []byte {
	type symOut struct {
		Sym    string           `json:"sym"`
		Price  float64          `json:"price"`
		Bids   []level          `json:"bids"` // best (highest) first
		Asks   []level          `json:"asks"` // best (lowest) first
		Trades []map[string]any `json:"trades"`
	}
	out := struct {
		Symbols []symOut `json:"symbols"`
		Total   int      `json:"total"`
		At      int64    `json:"at"`
	}{At: time.Now().UnixMilli()}

	mem, _ := DialResp()
	if mem != nil {
		defer mem.Close()
	}
	if mem != nil {
		if v, err := mem.Do("GET", "trades:count"); err == nil {
			if n, err := strconv.Atoi(str(v)); err == nil {
				out.Total = n
			}
		}
	}
	for _, s := range symbols {
		price := seedPrice[s]
		if mem != nil {
			if v, err := mem.Do("GET", "px:"+s); err == nil {
				if f, err := strconv.ParseFloat(str(v), 64); err == nil && f > 0 {
					price = f
				}
			}
		}
		so := symOut{Sym: s, Price: price}
		// The RESTING (maker) book a real venue shows: resting bids sit below
		// the fair mid, resting asks above it (marketable orders in flight are
		// filtered out client-side). Books live in OxiMem ZSETs now.
		mid := 0.7*price + 0.3*seedPrice[s]
		so.Bids = memLevels(mem, "book:"+s+":b", true, mid, 6)
		so.Asks = memLevels(mem, "book:"+s+":a", false, mid, 6)
		// A couple of most-recent prints.
		recent, _ := db.Find("trades", map[string]any{"sym": s}, map[string]any{"_id": -1}, 4)
		for _, t := range recent {
			so.Trades = append(so.Trades, map[string]any{
				"price": getF(t, "price"), "qty": getF(t, "qty"),
				"buyer": getS(t, "buyer"), "seller": getS(t, "seller"),
			})
		}
		out.Symbols = append(out.Symbols, so)
	}
	b, _ := json.Marshal(out)
	return b
}

// writeTextFrame writes an unmasked server→client text frame (opcode 0x1).
func writeTextFrame(w *bufio.ReadWriter, payload []byte) error {
	n := len(payload)
	hdr := []byte{0x81}
	switch {
	case n < 126:
		hdr = append(hdr, byte(n))
	case n < 65536:
		hdr = append(hdr, 126, byte(n>>8), byte(n))
	default:
		hdr = append(hdr, 127, 0, 0, 0, 0, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	}
	if _, err := w.Write(hdr); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

var _ = net.Dial // keep net imported for clarity of the hijacked conn type
