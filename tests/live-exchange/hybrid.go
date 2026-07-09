package main

// hybrid — the real-exchange architecture on one process: the ORDER BOOK and
// BALANCES live in OxiMem (ZSETs + strings, matched at memory speed with
// WATCH/MULTI/EXEC atomic settlement), while every fill is appended to the
// OxiDB document engine as the durable trade ledger. This is the pattern real
// venues use: match in RAM, persist events.
//
//	usd:{owner}        string balance (INCRBYFLOAT)
//	ast:{sym}:{owner}  string position
//	book:{sym}:b/a     ZSET score=price member="uid|owner|qty"
//	px:{sym}           last trade price

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

func hybrid(secs int) {
	nUsersH := envInt("HY_USERS", 10)
	rateEach := envInt("HY_RATE_EACH", 150) // orders/s per trader
	takerPct := envInt("HY_TAKER_PCT", 50)

	// Seed balances + prices in OxiMem.
	seedC, err := DialResp()
	if err != nil {
		panic(fmt.Sprintf("OxiMem dial: %v (set OXIDB_OXIMEM_PORT on the server, OXIMEM_PORT here)", err))
	}
	seedC.Do("FLUSHALL")
	for u := 0; u < nUsersH; u++ {
		owner := fmt.Sprintf("user-%d", u)
		seedC.Do("SET", "usd:"+owner, fmt.Sprintf("%f", startCash))
		for _, s := range symbols {
			seedC.Do("SET", "ast:"+s+":"+owner, fmt.Sprintf("%f", initHolding))
		}
	}
	for _, s := range symbols {
		seedC.Do("SET", "px:"+s, fmt.Sprintf("%f", seedPrice[s]))
	}
	fmt.Printf("[hybrid] seeded %d users × %d symbols in OxiMem\n", nUsersH, len(symbols))

	// Durable ledger writers: fills stream to OxiDB without stalling matchers.
	ledger := make(chan map[string]any, 4096)
	for w := 0; w < 2; w++ {
		go func() {
			dc, err := Dial()
			if err != nil {
				return
			}
			for doc := range ledger {
				dc.Insert("trades", doc)
			}
		}()
	}

	var trades, aborts, cancels, placed, skipped, scans int64
	stop := make(chan struct{})

	// One matcher goroutine per symbol — single-threaded per book.
	for _, sym := range symbols {
		go func(sym string) {
			c, err := DialResp()
			if err != nil {
				return
			}
			bkey, akey := "book:"+sym+":b", "book:"+sym+":a"
			seq := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				bb, err1 := c.Do("ZREVRANGE", bkey, "0", "0", "WITHSCORES")
				ba, err2 := c.Do("ZRANGE", akey, "0", "0", "WITHSCORES")
				if err1 != nil || err2 != nil {
					fmt.Printf("[hybrid] %s matcher error: %v %v — reconnecting\n", sym, err1, err2)
					c.Close()
					time.Sleep(100 * time.Millisecond)
					if nc, e := DialResp(); e == nil {
						c = nc
					}
					continue
				}
				bidArr, _ := bb.([]any)
				askArr, _ := ba.([]any)
				if len(bidArr) < 2 || len(askArr) < 2 {
					time.Sleep(2 * time.Millisecond)
					continue
				}
				bidM, _ := bidArr[0].(string)
				bidP, _ := strconv.ParseFloat(bidArr[1].(string), 64)
				askM, _ := askArr[0].(string)
				askP, _ := strconv.ParseFloat(askArr[1].(string), 64)
				atomic.AddInt64(&scans, 1)
				if bidP < askP {
					time.Sleep(2 * time.Millisecond)
					continue
				}
				bf := strings.SplitN(bidM, "|", 3)
				af := strings.SplitN(askM, "|", 3)
				if len(bf) != 3 || len(af) != 3 {
					c.Do("ZREM", bkey, bidM)
					c.Do("ZREM", akey, askM)
					continue
				}
				buyer, seller := bf[1], af[1]
				if buyer == seller { // self-cross: drop the ask, rescan
					c.Do("ZREM", akey, askM)
					continue
				}
				bq, _ := strconv.ParseFloat(bf[2], 64)
				aq, _ := strconv.ParseFloat(af[2], 64)
				qty := bq
				if aq < qty {
					qty = aq
				}
				price := (bidP + askP) / 2 // midpoint — value-neutral fills
				cost := price * qty

				// Optimistic settlement: WATCH the two hot cash keys, verify
				// sufficiency, then move everything in one atomic EXEC.
				ok := false
				for retry := 0; retry < 5; retry++ {
					c.Do("WATCH", "usd:"+buyer, "usd:"+seller)
					bv, _ := c.Do("GET", "usd:"+buyer)
					sv, _ := c.Do("GET", "ast:"+sym+":"+seller)
					bbal, _ := strconv.ParseFloat(str(bv), 64)
					sbal, _ := strconv.ParseFloat(str(sv), 64)
					if bbal < cost {
						c.Do("UNWATCH")
						c.Do("ZREM", bkey, bidM) // can't afford → cancel bid
						atomic.AddInt64(&cancels, 1)
						break
					}
					if sbal < qty {
						c.Do("UNWATCH")
						c.Do("ZREM", akey, askM)
						atomic.AddInt64(&cancels, 1)
						break
					}
					c.Do("MULTI")
					c.Do("ZREM", bkey, bidM)
					c.Do("ZREM", akey, askM)
					// PARTIAL FILLS: the untraded remainder of the larger
					// order goes straight back on the book, same price.
					if bq > qty {
						rem := fmt.Sprintf("%s|%s|%.4f", bf[0], buyer, bq-qty)
						c.Do("ZADD", bkey, fmt.Sprintf("%f", bidP), rem)
					}
					if aq > qty {
						rem := fmt.Sprintf("%s|%s|%.4f", af[0], seller, aq-qty)
						c.Do("ZADD", akey, fmt.Sprintf("%f", askP), rem)
					}
					c.Do("INCRBYFLOAT", "usd:"+buyer, fmt.Sprintf("%f", -cost))
					c.Do("INCRBYFLOAT", "usd:"+seller, fmt.Sprintf("%f", cost))
					c.Do("INCRBYFLOAT", "ast:"+sym+":"+buyer, fmt.Sprintf("%f", qty))
					c.Do("INCRBYFLOAT", "ast:"+sym+":"+seller, fmt.Sprintf("%f", -qty))
					c.Do("SET", "px:"+sym, fmt.Sprintf("%f", price))
					r, err := c.Do("EXEC")
					if err == nil && r != nil { // nil = WATCH abort
						ok = true
						break
					}
					atomic.AddInt64(&aborts, 1)
				}
				if ok {
					seq++
					atomic.AddInt64(&trades, 1)
					select { // never block the matcher on the ledger
					case ledger <- map[string]any{
						"uid": fmt.Sprintf("%s-h%d", sym, seq), "sym": sym,
						"price": price, "qty": qty, "buyer": buyer, "seller": seller,
						"created_at": nowISO(),
					}:
					default:
					}
				}
			}
		}(sym)
	}

	// Book pruner: cap each side at 2000 resting orders using the new
	// ZREMRANGEBYRANK — drops the worst-priced tail (lowest bids / highest
	// asks) so an unmatched backlog can't grow without bound.
	go func() {
		c, err := DialResp()
		if err != nil {
			return
		}
		for {
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Second):
			}
			for _, s := range symbols {
				c.Do("ZREMRANGEBYRANK", "book:"+s+":b", "0", "-2001") // keep top bids
				c.Do("ZREMRANGEBYRANK", "book:"+s+":a", "2000", "-1") // keep low asks
			}
		}
	}()

	// Trader goroutines: place limit orders around px into the ZSET book.
	for u := 0; u < nUsersH; u++ {
		go func(u int) {
			c, err := DialResp()
			if err != nil {
				return
			}
			owner := fmt.Sprintf("user-%d", u)
			rng := rand.New(rand.NewSource(int64(u)*7919 + time.Now().UnixNano()))
			gap := time.Second / time.Duration(rateEach)
			seq := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				time.Sleep(gap)
				sym := symbols[rng.Intn(len(symbols))]
				pv, _ := c.Do("GET", "px:"+sym)
				p, _ := strconv.ParseFloat(str(pv), 64)
				if p <= 0 {
					atomic.AddInt64(&skipped, 1)
					continue
				}
				fair := 0.7*p + 0.3*seedPrice[sym]
				buy := rng.Intn(2) == 0
				aggressive := rng.Intn(100) < takerPct
				var price float64
				off := 0.0004 + rng.Float64()*0.004
				cross := 0.0002 + rng.Float64()*0.001
				side := "a"
				if buy {
					side = "b"
					if aggressive {
						price = fair * (1 + cross)
					} else {
						price = fair * (1 - off)
					}
				} else if aggressive {
					price = fair * (1 - cross)
				} else {
					price = fair * (1 + off)
				}
				qty := 1 + rng.Float64()*9
				seq++
				member := fmt.Sprintf("%s-%d|%s|%.4f", owner, seq, owner, qty)
				if _, err := c.Do("ZADD", "book:"+sym+":"+side, fmt.Sprintf("%f", price), member); err == nil {
					atomic.AddInt64(&placed, 1)
				} else {
					atomic.AddInt64(&skipped, 1)
				}
			}
		}(u)
	}

	// Report + conservation check.
	start := time.Now()
	last := int64(0)
	for time.Since(start) < time.Duration(secs)*time.Second {
		time.Sleep(5 * time.Second)
		t := atomic.LoadInt64(&trades)
		fmt.Printf("[hybrid] trades=%d (%.0f/s) aborts=%d cancels=%d placed=%d skipped=%d scans=%d\n",
			t, float64(t-last)/5, atomic.LoadInt64(&aborts), atomic.LoadInt64(&cancels),
			atomic.LoadInt64(&placed), atomic.LoadInt64(&skipped), atomic.LoadInt64(&scans))
		last = t
	}
	close(stop)
	time.Sleep(300 * time.Millisecond)

	fmt.Println("[hybrid] verifying conservation from OxiMem…")
	vc, _ := DialResp()
	totUSD := 0.0
	for u := 0; u < nUsersH; u++ {
		v, _ := vc.Do("GET", fmt.Sprintf("usd:user-%d", u))
		f, _ := strconv.ParseFloat(str(v), 64)
		totUSD += f
	}
	wantUSD := float64(nUsersH) * startCash
	fmt.Printf("  USD total: %.2f (want %.0f) %s\n", totUSD, wantUSD, okMark(abs(totUSD-wantUSD) < 1.0))
	allOk := abs(totUSD-wantUSD) < 1.0
	for _, s := range symbols {
		tot := 0.0
		for u := 0; u < nUsersH; u++ {
			v, _ := vc.Do("GET", fmt.Sprintf("ast:%s:user-%d", s, u))
			f, _ := strconv.ParseFloat(str(v), 64)
			tot += f
		}
		if abs(tot-float64(nUsersH)*initHolding) > 0.01 {
			fmt.Printf("  %s holdings %.4f != %.0f FAIL\n", s, tot, float64(nUsersH)*initHolding)
			allOk = false
		}
	}
	if allOk {
		fmt.Printf("[hybrid] ALL CONSERVED — %d trades settled atomically in OxiMem\n",
			atomic.LoadInt64(&trades))
	} else {
		fmt.Println("[hybrid] CONSERVATION FAILED")
	}
}

func str(v any) string {
	s, _ := v.(string)
	return s
}
func abs(f float64) float64 {
	if f < 0 {
		return -f
	}
	return f
}
func okMark(b bool) string {
	if b {
		return "OK"
	}
	return "FAIL"
}
