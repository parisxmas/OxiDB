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
	fmt.Printf("[web] dashboard on http://localhost:%s  (WS /ws)\n", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		fmt.Println("[web] error:", err)
	}
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

	tick := time.NewTicker(400 * time.Millisecond)
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

// snapshot builds the JSON pushed to the browser: for each symbol its
// current price AND its own most-recent trades, plus the total count.
func snapshot(db *Client) []byte {
	type symOut struct {
		Sym    string           `json:"sym"`
		Price  float64          `json:"price"`
		Trades []map[string]any `json:"trades"`
	}
	out := struct {
		Symbols []symOut `json:"symbols"`
		Total   int      `json:"total"`
		At      int64    `json:"at"`
	}{At: time.Now().UnixMilli()}

	for _, s := range symbols {
		row, _ := db.FindOne("symbols", map[string]any{"sym": s})
		price := seedPrice[s]
		if row != nil {
			price = getF(row, "price")
		}
		so := symOut{Sym: s, Price: price}
		// This symbol's last few trades (newest first).
		recent, _ := db.Find("trades", map[string]any{"sym": s}, map[string]any{"_id": -1}, 6)
		for _, t := range recent {
			so.Trades = append(so.Trades, map[string]any{
				"price": getF(t, "price"), "qty": getF(t, "qty"),
				"buyer": getS(t, "buyer"), "seller": getS(t, "seller"),
			})
		}
		out.Symbols = append(out.Symbols, so)
	}
	out.Total = db.Count("trades", map[string]any{})
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
