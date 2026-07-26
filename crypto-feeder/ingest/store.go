package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

// OxiDBStore buffers order book snapshots and flushes them in bulk.
type OxiDBStore struct {
	addr     string
	mu       sync.Mutex
	conn     net.Conn
	buf      []map[string]interface{}
	maxBatch int
	interval time.Duration
	lastTop  map[string][2]float64 // key: "exchange/pair" → [bid_top, ask_top]
}

func NewOxiDBStore(addr string) *OxiDBStore {
	s := &OxiDBStore{
		addr:     addr,
		buf:      make([]map[string]interface{}, 0, 64),
		maxBatch: 50,
		interval: 5 * time.Second,
		lastTop:  make(map[string][2]float64),
	}
	go s.flushLoop()
	return s
}

func (s *OxiDBStore) connect() error {
	conn, err := net.DialTimeout("tcp", s.addr, 5*time.Second)
	if err != nil {
		return err
	}
	s.conn = conn
	return nil
}

// Store buffers a snapshot. Skips duplicates for non-Binance exchanges.
func (s *OxiDBStore) Store(ob OrderBook) {
	bid := topPrice(ob.Bids)
	ask := topPrice(ob.Asks)

	// Dedup only for non-Binance (Binance has meaningful tick-level data)
	if ob.Exchange != "binance" {
		key := ob.Exchange + "/" + ob.Pair
		s.mu.Lock()
		last, exists := s.lastTop[key]
		if exists && last[0] == bid && last[1] == ask {
			s.mu.Unlock()
			return
		}
		s.lastTop[key] = [2]float64{bid, ask}
		s.mu.Unlock()
	}

	doc := map[string]interface{}{
		"exchange":  ob.Exchange,
		"pair":      ob.Pair,
		"timestamp": ob.Timestamp,
		"bid_top":   topPrice(ob.Bids),
		"ask_top":   topPrice(ob.Asks),
		"bid_depth": len(ob.Bids),
		"ask_depth": len(ob.Asks),
		"spread":    spread(ob),
		"_ttl":      86400, // auto-expire after 24h
	}

	s.mu.Lock()
	s.buf = append(s.buf, doc)
	shouldFlush := len(s.buf) >= s.maxBatch
	s.mu.Unlock()

	if shouldFlush {
		s.Flush()
	}
}

func (s *OxiDBStore) flushLoop() {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for range ticker.C {
		s.Flush()
	}
}

// Flush sends all buffered documents as a single insert_many.
func (s *OxiDBStore) Flush() {
	s.mu.Lock()
	if len(s.buf) == 0 {
		s.mu.Unlock()
		return
	}
	docs := s.buf
	s.buf = make([]map[string]interface{}, 0, 64)
	s.mu.Unlock()

	if s.conn == nil {
		if err := s.connect(); err != nil {
			log.Printf("[store] connect error: %v", err)
			return
		}
	}

	cmd := map[string]interface{}{
		"cmd":        "insert_many",
		"collection": "orderbooks",
		"docs":       docs,
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return
	}

	header := make([]byte, 4)
	binary.LittleEndian.PutUint32(header, uint32(len(data)))

	s.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := s.conn.Write(header); err != nil {
		s.conn.Close()
		s.conn = nil
		return
	}
	if _, err := s.conn.Write(data); err != nil {
		s.conn.Close()
		s.conn = nil
		return
	}

	// Read response
	s.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var respLen [4]byte
	if _, err := s.conn.Read(respLen[:]); err != nil {
		s.conn.Close()
		s.conn = nil
		return
	}
	rLen := binary.LittleEndian.Uint32(respLen[:])
	if rLen > 0 && rLen < 1<<20 {
		discard := make([]byte, rLen)
		s.conn.Read(discard)
	}

	log.Printf("[store] bulk inserted %d documents", len(docs))
}

// sendCommand sends a JSON command to OxiDB and returns the response.
func sendCommand(addr string, cmd map[string]interface{}) (map[string]interface{}, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	data, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}

	header := make([]byte, 4)
	binary.LittleEndian.PutUint32(header, uint32(len(data)))

	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write(header); err != nil {
		return nil, err
	}
	if _, err := conn.Write(data); err != nil {
		return nil, err
	}

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	var respLen [4]byte
	if _, err := conn.Read(respLen[:]); err != nil {
		return nil, err
	}
	rLen := binary.LittleEndian.Uint32(respLen[:])
	if rLen == 0 || rLen > 4<<20 {
		return nil, fmt.Errorf("bad response length: %d", rLen)
	}

	buf := make([]byte, rLen)
	n := 0
	for n < int(rLen) {
		nn, err := conn.Read(buf[n:])
		if err != nil {
			return nil, err
		}
		n += nn
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(buf[:n], &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// RunDownsampler periodically aggregates raw orderbook data into 5-minute OHLC
// summaries stored in orderbooks_history (kept forever).
func RunDownsampler(addr string, exchanges, pairs []string, done <-chan struct{}) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			downsample(addr, exchanges, pairs)
		}
	}
}

func downsample(addr string, exchanges, pairs []string) {
	// Query last 5 minutes of raw data per exchange/pair
	now := time.Now().UnixMilli()
	fiveMinAgo := now - 5*60*1000
	periodStart := fiveMinAgo - (fiveMinAgo % (5 * 60 * 1000)) // align to 5-min boundary

	for _, exchange := range exchanges {
		for _, pair := range pairs {
			resp, err := sendCommand(addr, map[string]interface{}{
				"cmd":        "find",
				"collection": "orderbooks",
				"query": map[string]interface{}{
					"exchange": exchange,
					"pair":     pair,
					"timestamp": map[string]interface{}{
						"$gte": fiveMinAgo,
						"$lt":  now,
					},
				},
			})
			if err != nil {
				log.Printf("[downsample] query error %s/%s: %v", exchange, pair, err)
				continue
			}

			docs, ok := resp["data"].([]interface{})
			if !ok || len(docs) == 0 {
				continue
			}

			var bidHigh, askHigh float64
			bidLow, askLow := 1e18, 1e18
			var bidFirst, askFirst, bidLast, askLast float64
			first := true

			for _, d := range docs {
				doc, ok := d.(map[string]interface{})
				if !ok {
					continue
				}
				bid, _ := doc["bid_top"].(float64)
				ask, _ := doc["ask_top"].(float64)
				if bid == 0 || ask == 0 {
					continue
				}

				if first {
					bidFirst, askFirst = bid, ask
					first = false
				}
				bidLast, askLast = bid, ask

				if bid > bidHigh {
					bidHigh = bid
				}
				if bid < bidLow {
					bidLow = bid
				}
				if ask > askHigh {
					askHigh = ask
				}
				if ask < askLow {
					askLow = ask
				}
			}

			if first {
				continue // no valid docs
			}

			summary := map[string]interface{}{
				"exchange":     exchange,
				"pair":         pair,
				"period_start": periodStart,
				"bid_open":     bidFirst,
				"bid_close":    bidLast,
				"bid_high":     bidHigh,
				"bid_low":      bidLow,
				"ask_open":     askFirst,
				"ask_close":    askLast,
				"ask_high":     askHigh,
				"ask_low":      askLow,
				"samples":      len(docs),
			}

			_, err = sendCommand(addr, map[string]interface{}{
				"cmd":        "insert",
				"collection": "orderbooks_history",
				"doc":        summary,
			})
			if err != nil {
				log.Printf("[downsample] insert error %s/%s: %v", exchange, pair, err)
			}
		}
	}
	log.Printf("[downsample] aggregated %d exchange/pair combos", len(exchanges)*len(pairs))
}

func topPrice(levels []Level) float64 {
	if len(levels) > 0 {
		return levels[0].Price
	}
	return 0
}

func spread(ob OrderBook) string {
	if len(ob.Bids) > 0 && len(ob.Asks) > 0 {
		s := ob.Asks[0].Price - ob.Bids[0].Price
		return fmt.Sprintf("%.8f", s)
	}
	return "0"
}
