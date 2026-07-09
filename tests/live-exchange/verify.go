package main

// verify — post-run consistency for the hybrid exchange: conservation is
// checked from the OxiMem balances (never expired) and the durable OxiDB
// ledger is cross-checked against the cumulative trade counter.

import (
	"fmt"
	"math"
	"os"
	"strconv"
)

func verify() int {
	mem, err := DialResp()
	if err != nil {
		panic(err)
	}
	doc, err := Dial()
	if err != nil {
		panic(err)
	}
	ledgerTTL := 120
	if v := os.Getenv("LEDGER_TTL_SECS"); v != "" {
		ledgerTTL, _ = strconv.Atoi(v)
	}
	fails := 0
	check := func(cond bool, msg string) {
		if cond {
			fmt.Println("  OK  " + msg)
		} else {
			fmt.Println("FAIL  " + msg)
			fails++
		}
	}

	fmt.Printf("\n=== exchange verification (hybrid: OxiMem market + OxiDB ledger) ===\n")

	// Cumulative fills settled atomically in OxiMem.
	cntV, _ := mem.Do("GET", "trades:count")
	counter, _ := strconv.Atoi(str(cntV))
	nLedger := doc.Count("trades", map[string]any{})
	qlenV, _ := mem.Do("LLEN", "trades:q")
	qlen, _ := qlenV.(int64)
	fmt.Printf("settled=%d ledger=%d queue_backlog=%d\n", counter, nLedger, qlen)

	check(counter > 0, "trades were executed (traders formed a market)")

	// Ledger completeness: with no TTL every settled fill must be in OxiDB
	// (minus whatever is still queued); with a TTL the ledger is a rolling
	// window, so only require it to be non-empty and bounded by the counter.
	if ledgerTTL == 0 {
		check(nLedger+int(qlen) == counter,
			fmt.Sprintf("ledger complete (%d + %d queued == %d settled)", nLedger, qlen, counter))
	} else {
		check(nLedger > 0 && nLedger <= counter,
			fmt.Sprintf("ledger is a live rolling window (%d of %d, ttl=%ds)", nLedger, counter, ledgerTTL))
	}

	// Distinct uids in the ledger (exactly-once writes; unique index backs it).
	d := doc.Aggregate("trades", []any{
		map[string]any{"$group": map[string]any{"_id": "$uid"}},
		map[string]any{"$count": "n"},
	})
	nDistinct := 0
	if len(d) > 0 {
		nDistinct = int(getF(d[0], "n"))
	}
	check(nDistinct == nLedger, fmt.Sprintf("ledger uids all distinct (%d == %d)", nDistinct, nLedger))

	// Conservation from OxiMem balances (the source of truth).
	totalUSD := 0.0
	negCash := 0
	for u := 0; u < nUsers; u++ {
		v, _ := mem.Do("GET", fmt.Sprintf("usd:user-%d", u))
		f, _ := strconv.ParseFloat(str(v), 64)
		totalUSD += f
		if f < -1e-6 {
			negCash++
		}
	}
	wantUSD := float64(nUsers) * startCash
	check(math.Abs(totalUSD-wantUSD) < 1.0,
		fmt.Sprintf("total USD conserved (%.2f == %.0f) — no money created/destroyed", totalUSD, wantUSD))
	check(negCash == 0, fmt.Sprintf("no negative USD (overdrafts: %d)", negCash))

	symOK, negHold := true, 0
	wantHold := float64(nUsers) * initHolding
	for _, s := range symbols {
		tot := 0.0
		for u := 0; u < nUsers; u++ {
			v, _ := mem.Do("GET", fmt.Sprintf("ast:%s:user-%d", s, u))
			f, _ := strconv.ParseFloat(str(v), 64)
			tot += f
			if f < -1e-6 {
				negHold++
			}
		}
		if math.Abs(tot-wantHold) > 0.01 {
			symOK = false
			fmt.Printf("      %s holdings %.4f != %.0f\n", s, tot, wantHold)
		}
	}
	check(symOK, "each symbol's total holdings conserved (closed system)")
	check(negHold == 0, fmt.Sprintf("no negative holdings / no naked shorts (%d)", negHold))

	// Prices moved from seed → the market was formed by traders.
	moved := 0
	for _, s := range symbols {
		v, _ := mem.Do("GET", "px:"+s)
		f, _ := strconv.ParseFloat(str(v), 64)
		if math.Abs(f-seedPrice[s]) > 1e-9 {
			moved++
		}
	}
	check(moved > 0, fmt.Sprintf("prices set by trading (%d/%d symbols moved from seed)", moved, len(symbols)))

	fmt.Println()
	if fails > 0 {
		fmt.Printf("RESULT: %d CHECK(S) FAILED\n", fails)
		return 1
	}
	fmt.Println("RESULT: ALL CHECKS PASSED — RAM-matched market, durable ledger consistent")
	return 0
}
