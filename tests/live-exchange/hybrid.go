package main

// The exchange core, v2 — built on the full OxiMem stack:
//
//   order books   ZSETs  book:{sym}:b / book:{sym}:a  (member "uid|owner|qty")
//   balances      strings usd:{owner} / ast:{sym}:{owner}
//   last price    string  px:{sym}
//   settlement    ONE EVALSHA — a Lua script that checks funds, consumes the
//                 orders (re-listing partial-fill remainders), moves cash and
//                 assets, sets the price, bumps the cumulative counter and
//                 queues the trade event — atomically, in a single round
//                 trip. No WATCH, no aborts, no retries.
//   durability    ledger writers BLPOP trades:q and insert each fill into
//                 the OxiDB `trades` collection (uid unique) — the durable
//                 event log that also feeds the candle builders.
//
// This is the "match in RAM, persist events" architecture real venues use.

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// settleLua is the atomic settlement script.
// KEYS: 1 bkey 2 akey 3 usd:buyer 4 usd:seller 5 ast:buyer 6 ast:seller 7 px 8 counter 9 queue
// ARGV: 1 bidM 2 askM 3 qty 4 price 5 cost 6 bidRemMember 7 bidPrice 8 askRemMember 9 askPrice 10 tradeJson
// Returns 1 = settled, 0 = buyer can't pay (bid cancelled), -1 = seller short (ask cancelled).
const settleLua = `local ub = tonumber(redis.call('GET', KEYS[3]) or '0')
if ub < tonumber(ARGV[5]) then redis.call('ZREM', KEYS[1], ARGV[1]) return 0 end
local sa = tonumber(redis.call('GET', KEYS[6]) or '0')
if sa < tonumber(ARGV[3]) then redis.call('ZREM', KEYS[2], ARGV[2]) return -1 end
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[2])
if ARGV[6] ~= '' then redis.call('ZADD', KEYS[1], ARGV[7], ARGV[6]) end
if ARGV[8] ~= '' then redis.call('ZADD', KEYS[2], ARGV[9], ARGV[8]) end
redis.call('INCRBYFLOAT', KEYS[3], '-' .. ARGV[5])
redis.call('INCRBYFLOAT', KEYS[4], ARGV[5])
redis.call('INCRBYFLOAT', KEYS[5], ARGV[3])
redis.call('INCRBYFLOAT', KEYS[6], '-' .. ARGV[3])
redis.call('SET', KEYS[7], ARGV[4])
redis.call('INCR', KEYS[8])
redis.call('RPUSH', KEYS[9], ARGV[10])
return 1`

// seedMem seeds balances + prices in OxiMem (fresh market).
func seedMem() {
	c, err := DialResp()
	if err != nil {
		panic(fmt.Sprintf("OxiMem dial: %v (is OXIDB_OXIMEM_PORT set on the server?)", err))
	}
	defer c.Close()
	c.Do("FLUSHALL")
	for u := 0; u < nUsers; u++ {
		owner := fmt.Sprintf("user-%d", u)
		c.Do("SET", "usd:"+owner, fmt.Sprintf("%f", startCash))
		for _, s := range symbols {
			c.Do("SET", "ast:"+s+":"+owner, fmt.Sprintf("%f", initHolding))
		}
	}
	for _, s := range symbols {
		c.Do("SET", "px:"+s, fmt.Sprintf("%f", seedPrice[s]))
	}
	c.Do("SET", "trades:count", "0")
	fmt.Printf("[mem] seeded %d users × %d symbols in OxiMem\n", nUsers, len(symbols))
}

// hybridMatcher runs the matching engine: one goroutine per symbol scanning
// its book and settling crossings via EVALSHA, plus ledger writers draining
// trades:q into OxiDB, plus a book pruner. Blocks forever.
func hybridMatcher() {
	var trades, cancels int64

	// Durable ledger writers: BLPOP the fill queue into OxiDB.
	for w := 0; w < 2; w++ {
		go func() {
			mem, err := DialResp()
			if err != nil {
				return
			}
			doc, err := Dial()
			if err != nil {
				return
			}
			for {
				r, err := mem.Do("BLPOP", "trades:q", "1")
				if err != nil {
					time.Sleep(200 * time.Millisecond)
					continue
				}
				arr, ok := r.([]any)
				if !ok || len(arr) < 2 {
					continue
				}
				var t map[string]any
				if json.Unmarshal([]byte(str(arr[1])), &t) == nil {
					doc.Insert("trades", t)
				}
			}
		}()
	}

	// Book pruner: cap resting depth per side (drops worst-priced tail).
	go func() {
		c, err := DialResp()
		if err != nil {
			return
		}
		for range time.Tick(5 * time.Second) {
			for _, s := range symbols {
				c.Do("ZREMRANGEBYRANK", "book:"+s+":b", "0", "-2001")
				c.Do("ZREMRANGEBYRANK", "book:"+s+":a", "2000", "-1")
			}
		}
	}()

	for _, sym := range symbols {
		go func(sym string) {
			c, err := DialResp()
			if err != nil {
				return
			}
			shaV, err := c.Do("SCRIPT", "LOAD", settleLua)
			if err != nil {
				fmt.Printf("[matcher] %s SCRIPT LOAD failed: %v\n", sym, err)
				return
			}
			sha := str(shaV)
			bkey, akey := "book:"+sym+":b", "book:"+sym+":a"
			seq := 0
			for {
				bb, e1 := c.Do("ZREVRANGE", bkey, "0", "0", "WITHSCORES")
				ba, e2 := c.Do("ZRANGE", akey, "0", "0", "WITHSCORES")
				if e1 != nil || e2 != nil {
					c.Close()
					time.Sleep(100 * time.Millisecond)
					if nc, e := DialResp(); e == nil {
						c = nc
						if sv, e := c.Do("SCRIPT", "LOAD", settleLua); e == nil {
							sha = str(sv)
						}
					}
					continue
				}
				bidArr, _ := bb.([]any)
				askArr, _ := ba.([]any)
				if len(bidArr) < 2 || len(askArr) < 2 {
					time.Sleep(2 * time.Millisecond)
					continue
				}
				bidM := str(bidArr[0])
				bidP, _ := strconv.ParseFloat(str(bidArr[1]), 64)
				askM := str(askArr[0])
				askP, _ := strconv.ParseFloat(str(askArr[1]), 64)
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
				bidRem, askRem := "", ""
				if bq > qty {
					bidRem = fmt.Sprintf("%s|%s|%.4f", bf[0], buyer, bq-qty)
				}
				if aq > qty {
					askRem = fmt.Sprintf("%s|%s|%.4f", af[0], seller, aq-qty)
				}
				seq++
				ev, _ := json.Marshal(map[string]any{
					"uid": fmt.Sprintf("%s-h%d", sym, seq), "sym": sym,
					"price": price, "qty": qty, "buyer": buyer, "seller": seller,
					"created_at": nowISO(),
				})
				r, err := c.Do("EVALSHA", sha, "9",
					bkey, akey, "usd:"+buyer, "usd:"+seller,
					"ast:"+sym+":"+buyer, "ast:"+sym+":"+seller,
					"px:"+sym, "trades:count", "trades:q",
					bidM, askM,
					fmt.Sprintf("%f", qty), fmt.Sprintf("%f", price), fmt.Sprintf("%f", cost),
					bidRem, fmt.Sprintf("%f", bidP), askRem, fmt.Sprintf("%f", askP),
					string(ev))
				if err != nil {
					continue
				}
				switch v, _ := r.(int64); v {
				case 1:
					atomic.AddInt64(&trades, 1)
				default:
					atomic.AddInt64(&cancels, 1)
				}
			}
		}(sym)
	}

	go candleBuilder()
	go hcandleBuilder()

	last := int64(0)
	for range time.Tick(15 * time.Second) {
		t := atomic.LoadInt64(&trades)
		fmt.Printf("[matcher] trades=%d cancels=%d (%.0f/s)\n",
			t, atomic.LoadInt64(&cancels), float64(t-last)/15)
		last = t
	}
}

// hybridTrader places limit orders into the OxiMem books around px.
// Every trader gets a PERSONALITY derived from its id: a staggered start, an
// on/off activity cycle (bursts of trading separated by idle spells), its own
// order rate, aggressiveness, size preference and a favourite subset of
// symbols — so 100 traders behave like 100 different market participants
// acting at different times, not one clone army.
func hybridTrader(id, secs int) {
	c, err := DialResp()
	if err != nil {
		panic(err)
	}
	owner := fmt.Sprintf("user-%d", id)
	rng := rand.New(rand.NewSource(int64(id)*7919 + time.Now().UnixNano()))
	deadline := time.Now().Add(time.Duration(secs) * time.Second)

	// --- personality ---
	baseRate := envInt("ORDER_RATE_EACH", 0)
	if baseRate == 0 {
		baseRate = 5 + rng.Intn(36) // 5..40 orders/s while active
	}
	takerPct := envInt("TAKER_PCT", 0)
	if takerPct == 0 {
		takerPct = 20 + rng.Intn(51) // 20..70 %
	}
	sizeMax := 2.0 + rng.Float64()*13.0 // biggest order this trader places
	// Favourite symbols: a random subset (4..all), traded most of the time.
	favs := append([]string(nil), symbols...)
	rng.Shuffle(len(favs), func(i, j int) { favs[i], favs[j] = favs[j], favs[i] })
	favs = favs[:4+rng.Intn(len(favs)-3)]
	// Duty cycle: active bursts and idle spells of personal length.
	activeFor := time.Duration(10+rng.Intn(50)) * time.Second
	idleFor := time.Duration(5+rng.Intn(35)) * time.Second
	// Staggered start: traders wake up over the first ~20s.
	time.Sleep(time.Duration(rng.Intn(20000)) * time.Millisecond)

	gap := time.Second / time.Duration(baseRate)
	placed, seq := 0, 0
	phaseEnd := time.Now().Add(activeFor)
	active := true
	for time.Now().Before(deadline) {
		// Flip between active bursts and idle spells.
		if time.Now().After(phaseEnd) {
			active = !active
			if active {
				phaseEnd = time.Now().Add(activeFor)
			} else {
				phaseEnd = time.Now().Add(idleFor)
			}
		}
		if !active {
			time.Sleep(250 * time.Millisecond)
			continue
		}
		time.Sleep(gap)
		// Mostly favourites, occasionally anything.
		var sym string
		if rng.Intn(100) < 80 {
			sym = favs[rng.Intn(len(favs))]
		} else {
			sym = symbols[rng.Intn(len(symbols))]
		}
		pv, _ := c.Do("GET", "px:"+sym)
		p, _ := strconv.ParseFloat(str(pv), 64)
		if p <= 0 {
			continue
		}
		fair := 0.7*p + 0.3*seedPrice[sym]
		buy := rng.Intn(2) == 0
		aggressive := rng.Intn(100) < takerPct
		off := 0.0004 + rng.Float64()*0.004
		cross := 0.0002 + rng.Float64()*0.001
		side := "a"
		var price float64
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
		qty := 1 + rng.Float64()*sizeMax
		seq++
		member := fmt.Sprintf("%s-%d|%s|%.4f", owner, seq, owner, qty)
		if _, err := c.Do("ZADD", "book:"+sym+":"+side, fmt.Sprintf("%f", price), member); err == nil {
			placed++
		}
	}
	fmt.Printf("[%s] placed=%d rate=%d taker=%d%% favs=%d\n", owner, placed, baseRate, takerPct, len(favs))
}

// traders runs N personality traders as goroutines in ONE process (spawning
// 100 OS processes is pointless — each trader is just a connection + a loop).
func tradersMode(n, secs int) {
	done := make(chan int, n)
	for u := 0; u < n; u++ {
		go func(u int) {
			hybridTrader(u, secs)
			done <- u
		}(u)
	}
	for range n {
		<-done
	}
}

// hybrid keeps the original all-in-one benchmark mode (seed + matcher +
// traders in one process for N seconds, then a conservation check).
func hybrid(secs int) {
	seedMem()
	go hybridMatcher()
	for u := 0; u < nUsers; u++ {
		go hybridTrader(u, secs)
	}
	time.Sleep(time.Duration(secs) * time.Second)
	time.Sleep(500 * time.Millisecond)
	vc, _ := DialResp()
	tot := 0.0
	for u := 0; u < nUsers; u++ {
		v, _ := vc.Do("GET", fmt.Sprintf("usd:user-%d", u))
		f, _ := strconv.ParseFloat(str(v), 64)
		tot += f
	}
	cnt, _ := vc.Do("GET", "trades:count")
	fmt.Printf("[hybrid] trades=%s USD total=%.2f (want %.0f) %s\n",
		str(cnt), tot, float64(nUsers)*startCash,
		okMark(abs(tot-float64(nUsers)*startCash) < 1.0))
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
