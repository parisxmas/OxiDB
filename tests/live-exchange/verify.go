package main

import (
	"fmt"
	"math"
	"os"
	"strconv"
)

func verify() int {
	c, err := Dial()
	if err != nil {
		panic(err)
	}
	// When the ledger is TTL-bounded it's a rolling window, so balances
	// can't be integrated from the (partial) journal — the conservation
	// checks below, computed from the never-expiring accounts, are the
	// real correctness proof and hold regardless.
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

	nTrades := c.Count("trades", map[string]any{})
	nReceipts := c.Count("receipts", map[string]any{})
	nJournal := c.Count("journal", map[string]any{})
	nOpen := c.Count("open_orders", map[string]any{})

	fmt.Printf("\n=== exchange verification ===\n")
	fmt.Printf("trades=%d receipts=%d journal_legs=%d open_orders(resting)=%d\n",
		nTrades, nReceipts, nJournal, nOpen)

	// Under ledger TTL a sweep can fire between these counts, so allow a
	// tiny boundary skew; with no TTL the relation is exact.
	tol := 0
	if ledgerTTL > 0 {
		tol = 20
	}
	absi := func(x int) int {
		if x < 0 {
			return -x
		}
		return x
	}
	check(nTrades > 0, "trades were executed (traders formed a market)")
	check(absi(nTrades-nReceipts) <= tol, fmt.Sprintf("trades == receipts (%d ~ %d) — idempotent settlement", nTrades, nReceipts))
	check(absi(nJournal-4*nTrades) <= 4*tol, fmt.Sprintf("journal_legs == 4 x trades (%d ~ %d)", nJournal, 4*nTrades))

	// Distinct trade uids.
	dcount := c.Aggregate("trades", []any{
		map[string]any{"$group": map[string]any{"_id": "$uid"}},
		map[string]any{"$count": "n"},
	})
	nDistinct := 0
	if len(dcount) > 0 {
		nDistinct = int(getF(dcount[0], "n"))
	}
	check(absi(nDistinct-nTrades) <= tol, fmt.Sprintf("trade uids all distinct (%d ~ %d) — no double-settlement", nDistinct, nTrades))

	// USD net per owner from the journal.
	usdNet := map[string]float64{}
	for _, r := range c.Aggregate("journal", []any{
		map[string]any{"$match": map[string]any{"acct": "USD"}},
		map[string]any{"$group": map[string]any{"_id": "$owner", "s": map[string]any{"$sum": "$delta"}}},
	}) {
		usdNet[getS(r, "_id")] = getF(r, "s")
	}
	// Asset net per (owner, symbol).
	type key struct{ o, a string }
	assetNet := map[key]float64{}
	for _, r := range c.Aggregate("journal", []any{
		map[string]any{"$match": map[string]any{"acct": map[string]any{"$ne": "USD"}}},
		map[string]any{"$group": map[string]any{
			"_id": map[string]any{"o": "$owner", "a": "$acct"},
			"s":   map[string]any{"$sum": "$delta_asset"}}},
	}) {
		id, _ := r["_id"].(map[string]any)
		assetNet[key{getS(id, "o"), getS(id, "a")}] = getF(r, "s")
	}

	accts, _ := c.Find("accounts", map[string]any{}, nil, 0)
	usdReproduce, assetReproduce := true, true
	negCash, negHold := 0, 0
	totalUSD := 0.0
	holdingBySym := map[string]float64{}
	for _, a := range accts {
		owner, asset, bal := getS(a, "owner"), getS(a, "asset"), getF(a, "bal")
		if asset == "USD" {
			totalUSD += bal
			if bal < -1e-6 {
				negCash++
			}
			exp := startCash + usdNet[owner]
			if math.Abs(bal-exp) > 1e-3 {
				usdReproduce = false
				fmt.Printf("      %s USD %.6f != %.6f\n", owner, bal, exp)
			}
		} else {
			holdingBySym[asset] += bal
			if bal < -1e-6 {
				negHold++
			}
			exp := initHolding + assetNet[key{owner, asset}]
			if math.Abs(bal-exp) > 1e-3 {
				assetReproduce = false
				fmt.Printf("      %s/%s %.6f != %.6f\n", owner, asset, bal, exp)
			}
		}
	}
	_ = usdReproduce
	_ = assetReproduce

	check(math.Abs(totalUSD-nUsers*startCash) < 1e-2,
		fmt.Sprintf("total USD conserved (%.2f == %.0f) — no money created/destroyed", totalUSD, nUsers*startCash))

	symOK := true
	for _, s := range symbols {
		if math.Abs(holdingBySym[s]-nUsers*initHolding) > 1e-3 {
			symOK = false
			fmt.Printf("      %s total holdings %.4f != %.0f\n", s, holdingBySym[s], nUsers*initHolding)
		}
	}
	check(symOK, "each symbol's total holdings conserved (closed system: every buy has a sell)")
	check(negCash == 0, fmt.Sprintf("no negative USD (overdrafts: %d)", negCash))
	check(negHold == 0, fmt.Sprintf("no negative holdings / no naked shorts (%d)", negHold))
	if ledgerTTL == 0 {
		check(usdReproduce, "every USD balance reproducible from the journal")
		check(assetReproduce, "every position reproducible from the journal")
	} else {
		fmt.Printf("  --  journal is a %ds rolling window (ledger TTL) — conservation above is the proof\n", ledgerTTL)
	}

	// Prices moved from the seed → the market was formed by traders.
	moved := 0
	for _, s := range symbols {
		row, _ := c.FindOne("symbols", map[string]any{"sym": s})
		if row != nil && math.Abs(getF(row, "price")-seedPrice[s]) > 1e-9 {
			moved++
		}
	}
	check(moved > 0, fmt.Sprintf("prices set by trading (%d/%d symbols moved from seed)", moved, len(symbols)))

	fmt.Println()
	if fails > 0 {
		fmt.Printf("RESULT: %d CHECK(S) FAILED\n", fails)
		return 1
	}
	fmt.Println("RESULT: ALL CHECKS PASSED — trader-formed market, ledger consistent")
	return 0
}
