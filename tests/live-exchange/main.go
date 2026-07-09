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
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	startCash   = 1_000_000.0
	initHolding = 1000.0 // each user's opening position in every symbol
)

// nUsers is configurable (NUSERS): every trade locks the buyer's and
// seller's USD accounts, so more users = more disjoint account pairs that
// can settle concurrently = higher trade throughput before the hot-account
// serialization wall.
var nUsers = envInt("NUSERS", 10)

func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

var symbols = []string{"BTC", "ETH", "SOL", "BNB", "XRP", "ADA",
	"DOGE", "AVAX", "LINK", "DOT", "ATOM", "LTC", "MATIC", "UNI", "XLM"}

var seedPrice = map[string]float64{
	"BTC": 60000, "ETH": 3000, "SOL": 150, "BNB": 550, "XRP": 0.5, "ADA": 0.4,
	"DOGE": 0.1, "AVAX": 30, "LINK": 15, "DOT": 6, "ATOM": 8, "LTC": 80,
	"MATIC": 0.85, "UNI": 10.5, "XLM": 0.11,
}

func getF(m map[string]any, k string) float64 {
	f, _ := m[k].(float64)
	return f
}
func getS(m map[string]any, k string) string {
	s, _ := m[k].(string)
	return s
}
func nowISO() string        { return time.Now().UTC().Format("2006-01-02T15:04:05Z") }
func isoAt(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05Z") }

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: exchange setup|matcher|trader <id> <secs>|web|verify")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "setup":
		setup()
	case "matcher":
		hybridMatcher()
	case "trader":
		id, _ := strconv.Atoi(os.Args[2])
		secs, _ := strconv.Atoi(os.Args[3])
		hybridTrader(id, secs)
	case "traders": // all N traders as goroutines in one process
		n, _ := strconv.Atoi(os.Args[2])
		secs, _ := strconv.Atoi(os.Args[3])
		tradersMode(n, secs)
	case "matcher-doc": // legacy doc-engine matcher (kept for comparison)
		matcher()
	case "trader-doc":
		id, _ := strconv.Atoi(os.Args[2])
		secs, _ := strconv.Atoi(os.Args[3])
		trader(id, secs)
	case "web":
		web()
	case "verify":
		os.Exit(verify())
	case "hybrid":
		secs, _ := strconv.Atoi(os.Args[2])
		hybrid(secs)
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
	// Durable ledger + chart collections live in the doc engine; the order
	// books, balances and prices live in OxiMem (see hybrid.go).
	c.CreateUniqueIndex("trades", "uid") // exactly-once ledger writes
	c.CreateIndex("trades", "sym")

	lttl := 120
	if v := os.Getenv("LEDGER_TTL_SECS"); v != "" {
		lttl, _ = strconv.Atoi(v)
	}
	if lttl > 0 {
		c.CreateTTL("trades", "created_at", lttl)
	}

	// OHLCV candles for the charts (built from the trades ledger).
	c.CreateIndex("candles", "sym")
	c.CreateTTL("candles", "created_at", 900)
	c.CreateIndex("hcandles", "sym")
	c.CreateTTL("hcandles", "created_at", 90000)
	backfill24h(c)

	// Market state (balances, prices, empty books) in OxiMem.
	seedMem()

	fmt.Printf("seeded %d symbols, %d users (cash %.0f + %.0f/symbol), ledger TTL=%ds\n",
		len(symbols), nUsers, startCash, initHolding, lttl)
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

	// Cache prices and this user's balances, refreshing on a timer instead
	// of per-order. A per-order price+balance lookup was two server
	// round-trips PER placed order — with ~900 orders/s across 10 traders
	// that load starved the matcher's share of the worker pool. Now each
	// order costs the server a single insert; the matcher gets the freed
	// capacity. Balances are refreshed every refreshMs (fills land there);
	// between refreshes we optimistically debit locally so a trader can't
	// over-commit, and the matcher still re-checks and cancels the rare
	// order that can't actually settle.
	prices := map[string]float64{}
	bals := map[string]float64{}
	var lastRefresh time.Time
	refresh := func() {
		if rows, _ := c.Find("symbols", map[string]any{}, nil, 0); rows != nil {
			for _, r := range rows {
				prices[getS(r, "sym")] = getF(r, "price")
			}
		}
		if rows, _ := c.Find("accounts", map[string]any{"owner": owner}, nil, 0); rows != nil {
			for _, r := range rows {
				bals[getS(r, "asset")] = getF(r, "bal")
			}
		}
		lastRefresh = time.Now()
	}
	refresh()

	// Every write — trader order inserts AND the matcher's trade legs — goes
	// through the one WAL, so they share a fixed write budget. Un-paced, the
	// traders flood ~1800 orders/s (most expire unmatched, TTL) and starve
	// the matcher's share, capping trades. Pacing the order flow hands that
	// WAL bandwidth back to settlement. ORDER_RATE_EACH = orders/s per
	// trader (0 = unlimited).
	var minGap time.Duration
	if r := envInt("ORDER_RATE_EACH", 0); r > 0 {
		minGap = time.Second / time.Duration(r)
	}
	takerPct := envInt("TAKER_PCT", 25)
	var lastPlace time.Time

	for time.Now().Before(deadline) {
		if minGap > 0 {
			if d := minGap - time.Since(lastPlace); d > 0 {
				time.Sleep(d)
			}
			lastPlace = time.Now()
		}
		if time.Since(lastRefresh) > 200*time.Millisecond {
			refresh()
		}
		sym := symbols[rng.Intn(len(symbols))]
		p := prices[sym]
		if p <= 0 {
			continue
		}
		// Anchor orders to a fair value that mean-reverts toward the seed,
		// so the last-trade price can't random-walk away and drag the book
		// into a wide, crossed range. This is the restoring force a real
		// market gets from arbitrage; here it keeps prices bounded.
		fair := 0.7*p + 0.3*seedPrice[sym]
		// Inventory management: mean-revert this trader's cash toward its
		// starting level. Cash above start → lean BUY (spend it down); cash
		// below start → lean SELL (rebuild it). Without this, a closed
		// zero-sum market lets USD random-walk apart until poor traders can't
		// afford orders and drop out, and the trade rate decays over a long
		// run. Keeps everyone solvent and the market lively.
		buyBias := 0.5 + 0.5*(bals["USD"]-startCash)/startCash
		if buyBias < 0.05 {
			buyBias = 0.05
		} else if buyBias > 0.95 {
			buyBias = 0.95
		}
		buy := rng.Float64() < buyBias
		// ~18% takers cross the top of the book (→ a trade); the ~82% makers
		// rest across a depth ladder on their own side, building a deep,
		// two-sided book. Few takers → the matcher only nibbles the top, so
		// depth persists and the book stays uncrossed like a real venue.
		aggressive := rng.Intn(100) < takerPct
		var price float64
		if aggressive {
			cross := 0.0002 + rng.Float64()*0.0010 // just past the best opposite
			if buy {
				price = fair * (1 + cross)
			} else {
				price = fair * (1 - cross)
			}
		} else {
			off := 0.0004 + rng.Float64()*0.0120 // 0.04%..1.24% → many levels
			if buy {
				price = fair * (1 - off)
			} else {
				price = fair * (1 + off)
			}
		}
		price = float64(int(price*1e6)) / 1e6
		qty := 1 + rng.Float64()*19 // 1..20 units

		// Only place what the (cached) balance can back — the matcher
		// re-checks and cancels anything that can't settle.
		if buy && bals["USD"] < price*qty {
			skipped++
			continue
		}
		if !buy && bals[sym] < qty {
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
			// Optimistically debit locally until the next refresh re-syncs.
			if buy {
				bals["USD"] -= price * qty
			} else {
				bals[sym] -= qty
			}
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

	go candleBuilder()
	go hcandleBuilder()
	wg.Wait()
}

// hcandleSec is the big-chart bucket (default 5 min → 288 buckets = 24h).
func hcandleSec() int64 { return int64(envInt("HCANDLE_SEC", 300)) }

// backfill24h seeds several days of coarse candles per symbol so every
// timeframe (5m…4h) has plenty of history to show, not a sparse handful. A
// mean-reverting random walk that lands on the seed, connecting smoothly to
// where live trading opens.
func backfill24h(c *Client) {
	hsec := hcandleSec()
	n := envInt("BACKFILL_DAYS", 5) * 288 // 288 five-min buckets per day
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	nowSec := time.Now().Unix()
	cur := nowSec - nowSec%hsec // current bucket start; backfill everything before it
	for _, s := range symbols {
		seed := seedPrice[s]
		price := seed * (0.97 + rng.Float64()*0.06) // open the day within ±3%
		for i := n; i >= 1; i-- {
			o := price
			// Gentle mean-reverting walk with small per-5-min steps, so that
			// when 5-min candles roll up into 15m/1h/4h the aggregated bodies
			// stay realistic (a few %), not giant blocks.
			drift := (seed-price)/seed*0.02 + (rng.Float64()-0.5)*0.005 // ±0.25%/step
			cls := o * (1 + drift)
			if cls <= 0 {
				cls = o
			}
			hi := math.Max(o, cls) * (1 + rng.Float64()*0.0025)
			lo := math.Min(o, cls) * (1 - rng.Float64()*0.0025)
			c.Insert("hcandles", map[string]any{
				"sym": s, "ts": cur - int64(i)*hsec,
				"o": o, "h": hi, "l": lo, "c": cls, "v": 300 + rng.Float64()*4000,
				"created_at": nowISO(),
			})
			price = cls
		}
	}
}

// hcandleBuilder keeps the current 24h-series bucket live by rolling up the
// fine (2s) candles that fall inside it, upserting the bucket every few
// seconds. Aggregating the 2s candles (15-min TTL) rather than raw trades
// (60s TTL) lets it see the whole 5-min bucket.
func hcandleBuilder() {
	cl, err := Dial()
	if err != nil {
		return
	}
	hsec := hcandleSec()
	lastClose := map[string]float64{}
	for _, s := range symbols {
		lastClose[s] = seedPrice[s]
	}
	for now := range time.Tick(3 * time.Second) {
		bstart := now.Unix() - now.Unix()%hsec
		for _, s := range symbols {
			cs, _ := cl.Find("candles",
				map[string]any{"sym": s, "ts": map[string]any{"$gte": bstart}},
				map[string]any{"ts": 1}, 0)
			o, h, l, cc, v := 0.0, 0.0, 0.0, 0.0, 0.0
			if len(cs) > 0 {
				o, h, l = getF(cs[0], "o"), getF(cs[0], "h"), getF(cs[0], "l")
				for _, k := range cs {
					if getF(k, "h") > h {
						h = getF(k, "h")
					}
					if getF(k, "l") < l {
						l = getF(k, "l")
					}
					v += getF(k, "v")
				}
				cc = getF(cs[len(cs)-1], "c")
			} else {
				o, h, l, cc = lastClose[s], lastClose[s], lastClose[s], lastClose[s]
			}
			lastClose[s] = cc
			cl.Delete("hcandles", map[string]any{"sym": s, "ts": bstart}) // upsert
			cl.Insert("hcandles", map[string]any{
				"sym": s, "ts": bstart, "o": o, "h": h, "l": l, "c": cc, "v": v,
				"created_at": nowISO(),
			})
		}
	}
}

// candleBuilder rolls recent trades into fixed-interval OHLCV candles, one
// per symbol per bucket, and appends them to the `candles` collection. Runs
// off the trades' own rolling window, so empty buckets carry the last close
// forward (a flat candle) to keep every symbol's chart continuous.
func candleBuilder() {
	c, err := Dial()
	if err != nil {
		return
	}
	bucket := time.Duration(envInt("CANDLE_SEC", 2)) * time.Second
	lastClose := map[string]float64{}
	for _, s := range symbols {
		lastClose[s] = seedPrice[s]
	}
	for now := range time.Tick(bucket) {
		winStart := now.Add(-bucket)
		since := isoAt(winStart)
		for _, s := range symbols {
			rows, _ := c.Find("trades",
				map[string]any{"sym": s, "created_at": map[string]any{"$gte": since}},
				map[string]any{"_id": 1}, 0)
			o, h, l, cl, v := 0.0, 0.0, 0.0, 0.0, 0.0
			if len(rows) > 0 {
				o = getF(rows[0], "price")
				h, l = o, o
				for _, r := range rows {
					p := getF(r, "price")
					if p > h {
						h = p
					}
					if p < l {
						l = p
					}
					v += getF(r, "qty")
				}
				cl = getF(rows[len(rows)-1], "price")
			} else {
				// No trades this bucket → flat candle at the last close.
				o, h, l, cl = lastClose[s], lastClose[s], lastClose[s], lastClose[s]
			}
			lastClose[s] = cl
			c.Insert("candles", map[string]any{
				"sym": s, "ts": winStart.Unix(),
				"o": o, "h": h, "l": l, "c": cl, "v": v,
				"created_at": nowISO(),
			})
		}
	}
}

// executeTrade settles one match atomically. Returns "ok", "cancel"
// (insufficient funds — offending order removed), or "" (retry/gone).
func executeTrade(c *Client, sym string, bid, ask map[string]any, uid string) string {
	// Execute at the MIDPOINT of the crossing bid/ask, not the maker's price.
	// At the maker price the taker systematically pays the half-spread to the
	// maker; over hundreds of thousands of fills that bleeds total wealth from
	// frequent takers until enough go broke that they can't place orders and
	// the market freezes. Midpoint makes each fill value-neutral in
	// expectation, so wealth only random-walks (bounded by the trader's
	// mean-reversion) and the market stays liquid indefinitely.
	tradePrice := (getF(bid, "price") + getF(ask, "price")) / 2
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

		// Lock ONLY the two USD accounts — they are the hot cross-symbol
		// contention point (every symbol shard moves the same users' cash).
		// Locking makes contenders QUEUE instead of OCC-conflict-storming,
		// and a fixed (sorted) lock order rules out deadlock. The asset (sym)
		// accounts are touched solely by THIS symbol's shard — single writer,
		// so an optimistic read of the seller's holding can't conflict and
		// needs no lock. Two locks, not four → shorter hold, less
		// serialization on the bottleneck.
		usdOwners := []string{buyer, seller}
		sort.Strings(usdOwners)
		usd := map[string]float64{}
		locked := true
		for _, o := range usdOwners {
			rows, err := c.TxFindForUpdate("accounts", map[string]any{"owner": o, "asset": "USD"})
			if err != nil || len(rows) == 0 {
				locked = false
				break
			}
			usd[o] = getF(rows[0], "bal")
		}
		if !locked {
			c.Rollback()
			return "" // lock timeout or missing account → re-scan
		}
		if usd[buyer] < fillCost-1e-9 {
			c.Rollback()
			c.Delete("open_orders", map[string]any{"uid": bidUID}) // can't afford → cancel
			return "cancel"
		}
		seAsset, _ := c.Find("accounts", map[string]any{"owner": seller, "asset": sym}, nil, 1)
		if len(seAsset) == 0 || getF(seAsset[0], "bal") < fillQty-1e-9 {
			c.Rollback()
			c.Delete("open_orders", map[string]any{"uid": askUID})
			return "cancel"
		}

		ca := nowISO() // TTL stamp — bounds the ledger for a long demo
		err := func() error {
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
			// Trade record — its uid carries a UNIQUE index, so the insert
			// itself is the exactly-once idempotency guard (a duplicate uid
			// aborts the whole settlement). No separate receipts write.
			if e := c.TxInsert("trades", map[string]any{"uid": uid, "sym": sym,
				"price": tradePrice, "qty": fillQty, "buyer": buyer, "seller": seller,
				"created_at": ca}); e != nil {
				return e
			}
			// Double-entry journal as ONE doc holding all four legs (was four
			// separate inserts — a big chunk of the per-trade round-trips).
			if e := c.TxInsert("journal", map[string]any{
				"uid": uid, "sym": sym, "created_at": ca,
				"legs": []map[string]any{
					{"owner": buyer, "acct": "USD", "delta": -fillCost},
					{"owner": seller, "acct": "USD", "delta": fillCost},
					{"owner": buyer, "acct": sym, "delta_asset": fillQty},
					{"owner": seller, "acct": sym, "delta_asset": -fillQty},
				},
			}); e != nil {
				return e
			}
			// THE PRICE: last trade sets the market price for the symbol.
			// `traded` is a per-symbol cumulative fill counter — single
			// writer (this symbol's shard), so no added contention, and it
			// survives ledger TTL (the trades collection is a rolling window)
			// to give the dashboard a true all-time total.
			if e := c.TxUpdate("symbols", map[string]any{"sym": sym},
				map[string]any{
					"$set": map[string]any{"price": tradePrice, "ts": time.Now().Unix()},
					"$inc": map[string]any{"traded": 1},
				}); e != nil {
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
