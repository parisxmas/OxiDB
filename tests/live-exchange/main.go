// A self-contained exchange over OxiDB. NO external market data: prices
// are formed entirely by the traders' order flow, matched by a single
// matching engine, exactly like a real venue.
//
//	setup            seed symbols, accounts (cash + initial holdings),
//	                 indexes, and a TTL on the resting order book
//	matcher          the exchange core: one process, single-threaded
//	                 per-symbol matching; the last trade sets the price
//	trader <id> <s>  a user: places limit orders around the current
//	                 price; some cross the book and cause trades
//	verify           post-run ledger consistency checks
package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	nUsers      = 10
	startCash   = 1_000_000.0
	initHolding = 1000.0 // each user's opening position in every symbol
)

var symbols = []string{"BTC", "ETH", "SOL", "BNB", "XRP", "ADA",
	"DOGE", "AVAX", "LINK", "DOT", "ATOM", "LTC"}

var seedPrice = map[string]float64{
	"BTC": 60000, "ETH": 3000, "SOL": 150, "BNB": 550, "XRP": 0.5, "ADA": 0.4,
	"DOGE": 0.1, "AVAX": 30, "LINK": 15, "DOT": 6, "ATOM": 8, "LTC": 80,
}

func getF(m map[string]any, k string) float64 {
	f, _ := m[k].(float64)
	return f
}
func getS(m map[string]any, k string) string {
	s, _ := m[k].(string)
	return s
}
func nowISO() string { return time.Now().UTC().Format("2006-01-02T15:04:05Z") }

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: exchange setup|matcher|trader <id> <secs>|web|verify")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "setup":
		setup()
	case "matcher":
		matcher()
	case "trader":
		id, _ := strconv.Atoi(os.Args[2])
		secs, _ := strconv.Atoi(os.Args[3])
		trader(id, secs)
	case "web":
		web()
	case "verify":
		os.Exit(verify())
	default:
		fmt.Println("unknown mode:", os.Args[1])
		os.Exit(2)
	}
}

// ---- setup ----------------------------------------------------------------

func setup() {
	c, err := Dial()
	if err != nil {
		panic(err)
	}
	for _, s := range symbols {
		c.Insert("symbols", map[string]any{"sym": s, "price": seedPrice[s], "ts": 0})
	}
	c.CreateIndex("symbols", "sym")

	for u := 0; u < nUsers; u++ {
		owner := fmt.Sprintf("user-%d", u)
		c.Insert("accounts", map[string]any{"owner": owner, "asset": "USD", "bal": startCash})
		for _, s := range symbols {
			c.Insert("accounts", map[string]any{"owner": owner, "asset": s, "bal": initHolding})
		}
	}
	c.CreateIndex("accounts", "owner")

	// Resting order book: indexed for best-bid/best-ask scans, and TTL'd so
	// unfilled orders expire and the book can't grow without bound.
	c.CreateIndex("open_orders", "sym")
	c.CreateIndex("open_orders", "uid")
	ttl := 20
	if v := os.Getenv("ORDER_TTL_SECS"); v != "" {
		ttl, _ = strconv.Atoi(v)
	}
	if err := c.CreateTTL("open_orders", "created_at", ttl); err != nil {
		fmt.Println("WARN: ttl:", err)
	}

	c.CreateUniqueIndex("receipts", "uid")
	c.CreateIndex("trades", "uid")
	c.CreateIndex("trades", "sym") // per-symbol recent-trade lookups for the dashboard
	c.CreateIndex("journal", "uid")

	fmt.Printf("seeded %d symbols, %d users (cash %.0f + %.0f/symbol), order TTL=%ds\n",
		len(symbols), nUsers, startCash, initHolding, ttl)
}

// ---- trader ---------------------------------------------------------------

func trader(id, secs int) {
	c, err := Dial()
	if err != nil {
		panic(err)
	}
	owner := fmt.Sprintf("user-%d", id)
	rng := rand.New(rand.NewSource(int64(id)*7919 + time.Now().UnixNano()))
	deadline := time.Now().Add(time.Duration(secs) * time.Second)
	placed, skipped := 0, 0
	seq := 0

	balance := func(asset string) float64 {
		row, _ := c.FindOne("accounts", map[string]any{"owner": owner, "asset": asset})
		if row == nil {
			return 0
		}
		return getF(row, "bal")
	}

	for time.Now().Before(deadline) {
		sym := symbols[rng.Intn(len(symbols))]
		srow, _ := c.FindOne("symbols", map[string]any{"sym": sym})
		if srow == nil {
			continue
		}
		p := getF(srow, "price")
		if p <= 0 {
			continue
		}
		buy := rng.Intn(2) == 0
		aggressive := rng.Intn(10) < 7 // ~70% takers cross the spread → feed the matcher
		spread := 0.001 + rng.Float64()*0.004
		var price float64
		if buy {
			if aggressive {
				price = p * (1 + spread)
			} else {
				price = p * (1 - spread)
			}
		} else {
			if aggressive {
				price = p * (1 - spread)
			} else {
				price = p * (1 + spread)
			}
		}
		price = float64(int(price*1e6)) / 1e6
		qty := 1 + rng.Float64()*19 // 1..20 units

		// Only place what the (last-known) balance can back — the matcher
		// re-checks and cancels anything that can't settle.
		if buy && balance("USD") < price*qty {
			skipped++
			continue
		}
		if !buy && balance(sym) < qty {
			skipped++
			continue
		}
		seq++
		side := "sell"
		if buy {
			side = "buy"
		}
		uid := fmt.Sprintf("%s-%d", owner, seq)
		if err := c.Insert("open_orders", map[string]any{
			"uid": uid, "sym": sym, "side": side, "price": price,
			"qty": qty, "remaining": qty, "owner": owner, "created_at": nowISO(),
		}); err == nil {
			placed++
		}
	}
	fmt.Printf("[%s] placed=%d skipped=%d\n", owner, placed, skipped)
}

// ---- matcher (the exchange core) -----------------------------------------

// The matching engine is SHARDED BY SYMBOL: one goroutine (its own
// connection) per symbol, matching that symbol's book independently — as
// real venues shard matching across cores. Concurrent commits let group
// commit batch their fsyncs, lifting throughput far above a single
// serial matcher. Symbols are independent, but a user's cash account is
// shared across symbols, so a fill locks all four touched accounts in a
// global order (see executeTrade) to queue rather than conflict-storm.
func matcher() {
	var trades, cancels int64
	start := time.Now()
	var wg sync.WaitGroup

	for _, sym := range symbols {
		wg.Add(1)
		go func(sym string) {
			defer wg.Done()
			c, err := Dial()
			if err != nil {
				return
			}
			seq := 0
			for {
				bids, _ := c.Find("open_orders", map[string]any{"sym": sym, "side": "buy"},
					map[string]any{"price": -1}, 1)
				asks, _ := c.Find("open_orders", map[string]any{"sym": sym, "side": "sell"},
					map[string]any{"price": 1}, 1)
				if len(bids) == 0 || len(asks) == 0 {
					time.Sleep(8 * time.Millisecond)
					continue
				}
				bid, ask := bids[0], asks[0]
				if getF(bid, "price") < getF(ask, "price") {
					time.Sleep(8 * time.Millisecond)
					continue // spread not crossed
				}
				if getS(bid, "owner") == getS(ask, "owner") {
					c.Delete("open_orders", map[string]any{"uid": getS(ask, "uid")})
					continue
				}
				seq++
				switch executeTrade(c, sym, bid, ask, fmt.Sprintf("%s-%d", sym, seq)) {
				case "ok":
					atomic.AddInt64(&trades, 1)
				case "cancel":
					atomic.AddInt64(&cancels, 1)
				}
			}
		}(sym)
	}

	go func() {
		last := int64(0)
		lastT := start
		for range time.Tick(15 * time.Second) {
			t := atomic.LoadInt64(&trades)
			rate := float64(t-last) / time.Since(lastT).Seconds()
			fmt.Printf("[matcher] trades=%d cancels=%d (%.0f/s)\n",
				t, atomic.LoadInt64(&cancels), rate)
			last, lastT = t, time.Now()
		}
	}()
	wg.Wait()
}

// executeTrade settles one match atomically. Returns "ok", "cancel"
// (insufficient funds — offending order removed), or "" (retry/gone).
func executeTrade(c *Client, sym string, bid, ask map[string]any, uid string) string {
	tradePrice := getF(ask, "price") // maker's price
	qty := getF(bid, "remaining")
	if a := getF(ask, "remaining"); a < qty {
		qty = a
	}
	if qty <= 0 {
		return ""
	}
	buyer, seller := getS(bid, "owner"), getS(ask, "owner")
	bidUID, askUID := getS(bid, "uid"), getS(ask, "uid")

	for retry := 0; retry < 8; retry++ {
		if err := c.Begin(); err != nil {
			return ""
		}
		// Re-read inside the tx (a plain `find` in an open tx records the
		// read-set, so this is optimistic — no blocking locks, which keeps
		// commits concurrent and lets group commit batch their fsyncs. A
		// concurrent change to any doc read here aborts the commit and we
		// retry). Orders may have been filled or TTL-expired since the scan.
		lb, _ := c.Find("open_orders", map[string]any{"uid": bidUID}, nil, 1)
		la, _ := c.Find("open_orders", map[string]any{"uid": askUID}, nil, 1)
		if len(lb) == 0 || len(la) == 0 {
			c.Rollback()
			return ""
		}
		bidRem, askRem := getF(lb[0], "remaining"), getF(la[0], "remaining")
		fillQty := qty
		if bidRem < fillQty {
			fillQty = bidRem
		}
		if askRem < fillQty {
			fillQty = askRem
		}
		if fillQty <= 0 {
			c.Rollback()
			return ""
		}
		fillCost := tradePrice * fillQty

		// Sufficiency: buyer cash, seller holding (read into the read-set).
		buUSD, _ := c.Find("accounts", map[string]any{"owner": buyer, "asset": "USD"}, nil, 1)
		seAsset, _ := c.Find("accounts", map[string]any{"owner": seller, "asset": sym}, nil, 1)
		if len(buUSD) == 0 || getF(buUSD[0], "bal") < fillCost-1e-9 {
			c.Rollback()
			c.Delete("open_orders", map[string]any{"uid": bidUID}) // can't afford → cancel
			return "cancel"
		}
		if len(seAsset) == 0 || getF(seAsset[0], "bal") < fillQty-1e-9 {
			c.Rollback()
			c.Delete("open_orders", map[string]any{"uid": askUID})
			return "cancel"
		}

		err := func() error {
			if e := c.TxInsert("receipts", map[string]any{"uid": uid}); e != nil {
				return e
			}
			// Cash + asset moves (matcher is the sole writer of accounts).
			if e := c.TxUpdate("accounts", map[string]any{"owner": buyer, "asset": "USD"},
				map[string]any{"$inc": map[string]any{"bal": -fillCost}}); e != nil {
				return e
			}
			if e := c.TxUpdate("accounts", map[string]any{"owner": seller, "asset": "USD"},
				map[string]any{"$inc": map[string]any{"bal": fillCost}}); e != nil {
				return e
			}
			if e := c.TxUpdate("accounts", map[string]any{"owner": buyer, "asset": sym},
				map[string]any{"$inc": map[string]any{"bal": fillQty}}); e != nil {
				return e
			}
			if e := c.TxUpdate("accounts", map[string]any{"owner": seller, "asset": sym},
				map[string]any{"$inc": map[string]any{"bal": -fillQty}}); e != nil {
				return e
			}
			// Trade + double-entry journal.
			if e := c.TxInsert("trades", map[string]any{"uid": uid, "sym": sym,
				"price": tradePrice, "qty": fillQty, "buyer": buyer, "seller": seller}); e != nil {
				return e
			}
			for _, leg := range []map[string]any{
				{"uid": uid, "owner": buyer, "acct": "USD", "delta": -fillCost},
				{"uid": uid, "owner": seller, "acct": "USD", "delta": fillCost},
				{"uid": uid, "owner": buyer, "acct": sym, "delta_asset": fillQty},
				{"uid": uid, "owner": seller, "acct": sym, "delta_asset": -fillQty},
			} {
				if e := c.TxInsert("journal", leg); e != nil {
					return e
				}
			}
			// THE PRICE: last trade sets the market price for the symbol.
			if e := c.TxUpdate("symbols", map[string]any{"sym": sym},
				map[string]any{"$set": map[string]any{"price": tradePrice, "ts": time.Now().Unix()}}); e != nil {
				return e
			}
			// Consume the orders.
			if bidRem-fillQty <= 1e-9 {
				if e := c.TxDelete("open_orders", map[string]any{"uid": bidUID}); e != nil {
					return e
				}
			} else if e := c.TxUpdate("open_orders", map[string]any{"uid": bidUID},
				map[string]any{"$inc": map[string]any{"remaining": -fillQty}}); e != nil {
				return e
			}
			if askRem-fillQty <= 1e-9 {
				if e := c.TxDelete("open_orders", map[string]any{"uid": askUID}); e != nil {
					return e
				}
			} else if e := c.TxUpdate("open_orders", map[string]any{"uid": askUID},
				map[string]any{"$inc": map[string]any{"remaining": -fillQty}}); e != nil {
				return e
			}
			return nil
		}()
		if err != nil {
			c.Rollback()
			return ""
		}
		committed, conflict, _ := c.Commit()
		if committed {
			return "ok"
		}
		if conflict {
			continue // retry the whole settlement
		}
		return "" // unique receipt (already settled) or other — drop it
	}
	return ""
}
