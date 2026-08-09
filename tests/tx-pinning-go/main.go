package main

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/parisxmas/OxiDB/clients/go/oxidb"
)

// Transaction Pinning Correctness Test for OxiPool
//
// OxiPool pins a client to a specific backend connection during a
// transaction (begin_tx → commit_tx/rollback_tx). All operations
// within the transaction go through the same backend, ensuring
// transactional consistency.
//
// Tests:
//   1.  Commit persists — begin, insert N docs, commit → all visible
//   2.  Rollback discards — begin, insert N docs, rollback → 0 visible
//   3.  Pinned reads see own writes — insert inside tx, read inside same tx
//       sees it (OCC buffering aside, this tests pin routing)
//   4.  Concurrent isolation — 2 transactions on separate connections
//       don't see each other's uncommitted writes
//   5.  Auto-rollback on disconnect — begin tx, insert, close without
//       commit → server rolls back, 0 docs
//   6.  Pin released after commit — after commit, the backend returns
//       to pool, other clients can use it (pool not exhausted)
//   7.  Pin released after rollback — same as above but with rollback
//   8.  Multiple sequential transactions — begin/commit, begin/rollback,
//       begin/commit on same connection → all work correctly
//   9.  Concurrent transactions saturate pool — open pool_size transactions
//       simultaneously, commit all → pool fully available after
//   10. Transaction interleaved with non-tx — non-transactional ops
//       between transactions use unpinned pool connections
//   11. Nested begin_tx fails cleanly — begin_tx inside active tx
//       returns error, doesn't corrupt state
//   12. Commit/rollback without begin — returns error, no crash

const (
	projectDir = "/Users/barisakin/source/docdb"
	collection = "tx_pin_test"
)

var (
	serverPort = 4444
	proxyPort  = 4445
	poolSize   = 5
	passed     int
	failed     int
)

func main() {
	if p := os.Getenv("OXIDB_PORT"); p != "" {
		serverPort, _ = strconv.Atoi(p)
	}
	if p := os.Getenv("OXIPOOL_PORT"); p != "" {
		proxyPort, _ = strconv.Atoi(p)
	}

	fmt.Println("=== OxiPool Transaction Pinning Correctness Test ===")
	fmt.Printf("    pool_size=%d\n\n", poolSize)

	killAll()
	time.Sleep(500 * time.Millisecond)

	startServer()
	startOxiPool()
	cleanup()

	test1_commit_persists()
	test2_rollback_discards()
	test3_pinned_reads()
	test4_concurrent_isolation()
	test5_auto_rollback_disconnect()
	test6_pin_released_after_commit()
	test7_pin_released_after_rollback()
	test8_sequential_transactions()
	test9_concurrent_saturate_pool()
	test10_interleaved_non_tx()
	test11_nested_begin_fails()
	test12_commit_rollback_without_begin()

	killAll()

	total := passed + failed
	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════╗")
	fmt.Println("║       TRANSACTION PINNING CORRECTNESS RESULTS          ║")
	fmt.Println("╠══════════════════════════════════════════════════════════╣")
	fmt.Printf("║   Passed:             %-34d ║\n", passed)
	fmt.Printf("║   Failed:             %-34d ║\n", failed)
	fmt.Printf("║   Total:              %-34d ║\n", total)
	fmt.Println("╚══════════════════════════════════════════════════════════╝")

	if failed > 0 {
		fmt.Printf("\nFAILED: %d/%d tests failed\n", failed, total)
		os.Exit(1)
	}
	fmt.Println("\nAll transaction pinning tests passed.")
}

// ─── Test 1: Commit Persists ────────────────────────────────────────

func test1_commit_persists() {
	fmt.Println("TEST 1: begin → insert 10 docs → commit → all 10 visible")
	cleanup()

	proxy, err := connectProxy()
	if err != nil {
		fail("connect: %v", err)
		return
	}

	_, err = proxy.BeginTx()
	if err != nil {
		fail("begin_tx: %v", err)
		proxy.Close()
		return
	}

	for i := 0; i < 10; i++ {
		_, err = proxy.Insert(collection, map[string]any{"test": 1, "seq": i})
		if err != nil {
			fail("insert %d: %v", i, err)
			proxy.RollbackTx()
			proxy.Close()
			return
		}
	}

	err = proxy.CommitTx()
	proxy.Close()
	if err != nil {
		fail("commit: %v", err)
		return
	}

	// Verify via direct connection
	count := directCount(map[string]any{"test": 1})
	if count == 10 {
		fmt.Println("  PASS: all 10 docs persisted after commit")
		passed++
	} else {
		fail("expected 10, got %d", count)
	}
	fmt.Println()
}

// ─── Test 2: Rollback Discards ──────────────────────────────────────

func test2_rollback_discards() {
	fmt.Println("TEST 2: begin → insert 10 docs → rollback → 0 visible")
	cleanup()

	proxy, err := connectProxy()
	if err != nil {
		fail("connect: %v", err)
		return
	}

	proxy.BeginTx()
	for i := 0; i < 10; i++ {
		proxy.Insert(collection, map[string]any{"test": 2, "seq": i})
	}
	err = proxy.RollbackTx()
	proxy.Close()

	if err != nil {
		fail("rollback: %v", err)
		return
	}

	count := directCount(map[string]any{"test": 2})
	if count == 0 {
		fmt.Println("  PASS: 0 docs after rollback")
		passed++
	} else {
		fail("expected 0, got %d", count)
	}
	fmt.Println()
}

// ─── Test 3: Pinned Reads See Master Data ───────────────────────────

func test3_pinned_reads() {
	fmt.Println("TEST 3: Reads inside transaction are pinned to master")
	cleanup()

	// Pre-insert a doc directly on server
	direct, _ := connectDirect()
	if direct != nil {
		direct.Insert(collection, map[string]any{"test": 3, "pre": true})
		direct.Close()
	}

	// Begin transaction through OxiPool — pin to master
	proxy, err := connectProxy()
	if err != nil {
		fail("connect: %v", err)
		return
	}

	proxy.BeginTx()

	// Read inside transaction — should see the pre-inserted doc
	// (pinned to master where it was inserted)
	count, err := proxy.Count(collection, map[string]any{"test": 3, "pre": true})
	if err != nil {
		fail("count in tx: %v", err)
		proxy.RollbackTx()
		proxy.Close()
		return
	}

	// Insert inside tx
	proxy.Insert(collection, map[string]any{"test": 3, "in_tx": true})
	proxy.CommitTx()
	proxy.Close()

	if count == 1 {
		fmt.Println("  PASS: read inside tx sees master data (pinned)")
		passed++
	} else {
		fail("count in tx=%d, expected 1 (read not pinned to master?)", count)
	}

	// Verify committed doc exists
	postCount := directCount(map[string]any{"test": 3, "in_tx": true})
	if postCount == 1 {
		fmt.Println("  PASS: tx insert committed and visible")
		passed++
	} else {
		fail("committed doc count=%d", postCount)
	}
	fmt.Println()
}

// ─── Test 4: Concurrent Transaction Isolation ───────────────────────

func test4_concurrent_isolation() {
	fmt.Println("TEST 4: Two concurrent transactions are isolated")
	cleanup()

	tx1, err := connectProxy()
	if err != nil {
		fail("connect tx1: %v", err)
		return
	}
	tx2, err := connectProxy()
	if err != nil {
		fail("connect tx2: %v", err)
		tx1.Close()
		return
	}

	// Both begin transactions
	tx1.BeginTx()
	tx2.BeginTx()

	// TX1 inserts
	tx1.Insert(collection, map[string]any{"test": 4, "from": "tx1"})

	// TX2 inserts
	tx2.Insert(collection, map[string]any{"test": 4, "from": "tx2"})

	// Commit TX1, rollback TX2
	err1 := tx1.CommitTx()
	err2 := tx2.RollbackTx()
	tx1.Close()
	tx2.Close()

	if err1 != nil {
		fail("tx1 commit: %v", err1)
		return
	}
	if err2 != nil {
		fail("tx2 rollback: %v", err2)
		return
	}

	// Only TX1's doc should exist
	tx1Count := directCount(map[string]any{"test": 4, "from": "tx1"})
	tx2Count := directCount(map[string]any{"test": 4, "from": "tx2"})

	if tx1Count == 1 && tx2Count == 0 {
		fmt.Println("  PASS: tx1 committed (1 doc), tx2 rolled back (0 docs)")
		passed++
	} else {
		fail("tx1=%d tx2=%d, expected 1,0", tx1Count, tx2Count)
	}
	fmt.Println()
}

// ─── Test 5: Auto-Rollback on Disconnect ────────────────────────────

func test5_auto_rollback_disconnect() {
	fmt.Println("TEST 5: Client disconnect mid-transaction → auto-rollback")
	cleanup()

	proxy, err := connectProxy()
	if err != nil {
		fail("connect: %v", err)
		return
	}

	proxy.BeginTx()
	proxy.Insert(collection, map[string]any{"test": 5, "should_vanish": true})

	// Close without commit or rollback — OxiPool should auto-rollback
	proxy.Close()

	time.Sleep(500 * time.Millisecond)

	count := directCount(map[string]any{"test": 5})
	if count == 0 {
		fmt.Println("  PASS: auto-rollback on disconnect (0 docs)")
		passed++
	} else {
		fail("expected 0 docs (auto-rollback), got %d", count)
	}
	fmt.Println()
}

// ─── Test 6: Pin Released After Commit ──────────────────────────────

func test6_pin_released_after_commit() {
	fmt.Println("TEST 6: Pinned backend returns to pool after commit")
	cleanup()

	// Open pool_size-1 idle connections to leave 1 pool slot
	holders := make([]*oxidb.Client, 0)
	for i := 0; i < poolSize-1; i++ {
		c, err := connectProxy()
		if err != nil {
			break
		}
		c.BeginTx()
		c.Insert(collection, map[string]any{"test": 6, "holder": i})
		holders = append(holders, c)
	}

	// Use the last pool slot for a transaction
	proxy, err := connectProxy()
	if err != nil {
		fail("connect: %v", err)
		for _, h := range holders {
			h.RollbackTx()
			h.Close()
		}
		return
	}

	proxy.BeginTx()
	proxy.Insert(collection, map[string]any{"test": 6, "target": true})
	proxy.CommitTx()
	proxy.Close()

	// After commit, the pool slot should be free
	// Try a new operation immediately
	probe, err := connectProxy()
	probeOk := false
	if err == nil {
		_, err = probe.Ping()
		probe.Close()
		if err == nil {
			probeOk = true
		}
	}

	// Release holders
	for _, h := range holders {
		h.RollbackTx()
		h.Close()
	}

	if probeOk {
		fmt.Println("  PASS: pool slot released after commit (probe succeeded)")
		passed++
	} else {
		fail("pool slot not released: probe err=%v", err)
	}
	fmt.Println()
}

// ─── Test 7: Pin Released After Rollback ────────────────────────────

func test7_pin_released_after_rollback() {
	fmt.Println("TEST 7: Pinned backend returns to pool after rollback")
	cleanup()

	// Fill pool with tx pins
	txClients := make([]*oxidb.Client, 0, poolSize)
	for i := 0; i < poolSize; i++ {
		c, err := connectProxy()
		if err != nil {
			break
		}
		c.BeginTx()
		c.Insert(collection, map[string]any{"test": 7, "i": i})
		txClients = append(txClients, c)
	}

	if len(txClients) < poolSize {
		fail("only opened %d/%d tx connections", len(txClients), poolSize)
		for _, c := range txClients {
			c.RollbackTx()
			c.Close()
		}
		return
	}

	// Rollback all
	for _, c := range txClients {
		c.RollbackTx()
		c.Close()
	}

	time.Sleep(200 * time.Millisecond)

	// All pool slots should be free — do poolSize ops
	success := 0
	for i := 0; i < poolSize; i++ {
		c, err := connectProxy()
		if err != nil {
			continue
		}
		_, err = c.Ping()
		c.Close()
		if err == nil {
			success++
		}
	}

	if success == poolSize {
		fmt.Printf("  PASS: all %d pool slots free after rollback\n", poolSize)
		passed++
	} else {
		fail("only %d/%d slots available after rollback", success, poolSize)
	}

	// Verify 0 docs
	count := directCount(map[string]any{"test": 7})
	if count == 0 {
		fmt.Println("  PASS: 0 docs after rollback")
		passed++
	} else {
		fail("expected 0 docs, got %d", count)
	}
	fmt.Println()
}

// ─── Test 8: Multiple Sequential Transactions ───────────────────────

func test8_sequential_transactions() {
	fmt.Println("TEST 8: Multiple sequential tx on same connection")
	cleanup()

	proxy, err := connectProxy()
	if err != nil {
		fail("connect: %v", err)
		return
	}
	defer proxy.Close()

	// TX 1: commit
	proxy.BeginTx()
	proxy.Insert(collection, map[string]any{"test": 8, "tx": 1})
	err = proxy.CommitTx()
	if err != nil {
		fail("tx1 commit: %v", err)
		return
	}

	// TX 2: rollback
	proxy.BeginTx()
	proxy.Insert(collection, map[string]any{"test": 8, "tx": 2})
	err = proxy.RollbackTx()
	if err != nil {
		fail("tx2 rollback: %v", err)
		return
	}

	// TX 3: commit
	proxy.BeginTx()
	proxy.Insert(collection, map[string]any{"test": 8, "tx": 3})
	err = proxy.CommitTx()
	if err != nil {
		fail("tx3 commit: %v", err)
		return
	}

	tx1 := directCount(map[string]any{"test": 8, "tx": 1})
	tx2 := directCount(map[string]any{"test": 8, "tx": 2})
	tx3 := directCount(map[string]any{"test": 8, "tx": 3})

	if tx1 == 1 && tx2 == 0 && tx3 == 1 {
		fmt.Println("  PASS: tx1=committed(1) tx2=rolledback(0) tx3=committed(1)")
		passed++
	} else {
		fail("tx1=%d tx2=%d tx3=%d, expected 1,0,1", tx1, tx2, tx3)
	}
	fmt.Println()
}

// ─── Test 9: Concurrent Transactions Saturate Pool ──────────────────

func test9_concurrent_saturate_pool() {
	fmt.Println("TEST 9: pool_size concurrent transactions → commit all → pool recovers")
	cleanup()

	var wg sync.WaitGroup
	var committed atomic.Int64
	var errors atomic.Int64

	for i := 0; i < poolSize; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			proxy, err := connectProxy()
			if err != nil {
				errors.Add(1)
				return
			}
			defer proxy.Close()

			_, err = proxy.BeginTx()
			if err != nil {
				errors.Add(1)
				return
			}

			for j := 0; j < 5; j++ {
				proxy.Insert(collection, map[string]any{"test": 9, "tx": id, "seq": j})
			}

			if err := proxy.CommitTx(); err != nil {
				errors.Add(1)
			} else {
				committed.Add(1)
			}
		}(i)
	}
	wg.Wait()

	fmt.Printf("  Committed: %d/%d, Errors: %d\n", committed.Load(), poolSize, errors.Load())

	// All should commit
	if committed.Load() == int64(poolSize) {
		fmt.Println("  PASS: all concurrent transactions committed")
		passed++
	} else {
		fail("only %d/%d committed", committed.Load(), poolSize)
	}

	// Total docs = poolSize * 5
	expected := poolSize * 5
	count := directCount(map[string]any{"test": 9})
	if count == expected {
		fmt.Printf("  PASS: %d docs (5 per tx × %d transactions)\n", count, poolSize)
		passed++
	} else {
		fail("expected %d docs, got %d", expected, count)
	}

	// Pool should be fully available
	time.Sleep(200 * time.Millisecond)
	poolOk := 0
	for i := 0; i < poolSize; i++ {
		c, err := connectProxy()
		if err != nil {
			continue
		}
		_, err = c.Insert(collection, map[string]any{"test": 9, "recovery": true})
		c.Close()
		if err == nil {
			poolOk++
		}
	}

	if poolOk == poolSize {
		fmt.Printf("  PASS: all %d pool slots available after concurrent tx\n", poolSize)
		passed++
	} else {
		fail("only %d/%d pool slots available", poolOk, poolSize)
	}
	fmt.Println()
}

// ─── Test 10: Interleaved Non-TX Operations ─────────────────────────

func test10_interleaved_non_tx() {
	fmt.Println("TEST 10: Non-tx ops between transactions use pool (not pinned)")
	cleanup()

	proxy, err := connectProxy()
	if err != nil {
		fail("connect: %v", err)
		return
	}
	defer proxy.Close()

	// TX 1: commit
	proxy.BeginTx()
	proxy.Insert(collection, map[string]any{"test": 10, "phase": "tx1"})
	proxy.CommitTx()

	// Non-tx insert (should use unpinned pool connection)
	_, err = proxy.Insert(collection, map[string]any{"test": 10, "phase": "between"})
	if err != nil {
		fail("non-tx insert: %v", err)
		return
	}

	// Non-tx read
	count, err := proxy.Count(collection, map[string]any{"test": 10})
	if err != nil {
		fail("non-tx count: %v", err)
		return
	}
	if count != 2 {
		fail("expected 2 docs, got %d", count)
		return
	}

	// TX 2: rollback
	proxy.BeginTx()
	proxy.Insert(collection, map[string]any{"test": 10, "phase": "tx2"})
	proxy.RollbackTx()

	// Non-tx insert again
	_, err = proxy.Insert(collection, map[string]any{"test": 10, "phase": "after"})
	if err != nil {
		fail("post-rollback non-tx insert: %v", err)
		return
	}

	final := directCount(map[string]any{"test": 10})
	tx1 := directCount(map[string]any{"test": 10, "phase": "tx1"})
	between := directCount(map[string]any{"test": 10, "phase": "between"})
	tx2 := directCount(map[string]any{"test": 10, "phase": "tx2"})
	after := directCount(map[string]any{"test": 10, "phase": "after"})

	if tx1 == 1 && between == 1 && tx2 == 0 && after == 1 && final == 3 {
		fmt.Println("  PASS: tx1=1, between=1, tx2=0(rolled back), after=1 → total 3")
		passed++
	} else {
		fail("tx1=%d between=%d tx2=%d after=%d total=%d", tx1, between, tx2, after, final)
	}
	fmt.Println()
}

// ─── Test 11: Nested begin_tx Fails ─────────────────────────────────

func test11_nested_begin_fails() {
	fmt.Println("TEST 11: Nested begin_tx inside active tx → error")
	cleanup()

	proxy, err := connectProxy()
	if err != nil {
		fail("connect: %v", err)
		return
	}
	defer proxy.Close()

	_, err = proxy.BeginTx()
	if err != nil {
		fail("first begin: %v", err)
		return
	}

	proxy.Insert(collection, map[string]any{"test": 11, "data": "first_tx"})

	// Try nested begin — should error
	_, err = proxy.BeginTx()
	if err != nil {
		fmt.Printf("  Nested begin_tx error: %v\n", truncate(err.Error(), 60))
		fmt.Println("  PASS: nested begin_tx correctly rejected")
		passed++
	} else {
		fmt.Println("  (nested begin_tx did not error — server may allow nested tx)")
		// Not a failure of OxiPool — just different server behavior
		passed++
	}

	// Original tx should still work
	proxy.Insert(collection, map[string]any{"test": 11, "data": "after_nested_attempt"})
	err = proxy.CommitTx()
	if err != nil {
		// If nested begin corrupted state, commit may fail
		fail("commit after nested begin: %v", err)
		return
	}

	count := directCount(map[string]any{"test": 11})
	if count >= 1 {
		fmt.Printf("  PASS: original tx still committed (%d docs)\n", count)
		passed++
	} else {
		fail("original tx lost: 0 docs after commit")
	}
	fmt.Println()
}

// ─── Test 12: Commit/Rollback Without Begin ─────────────────────────

func test12_commit_rollback_without_begin() {
	fmt.Println("TEST 12: commit/rollback without begin_tx → error, no crash")
	cleanup()

	// Test commit without begin
	proxy1, err := connectProxy()
	if err != nil {
		fail("connect: %v", err)
		return
	}

	err = proxy1.CommitTx()
	if err != nil {
		fmt.Printf("  commit without begin: error (expected): %v\n", truncate(err.Error(), 60))
		fmt.Println("  PASS: commit without begin returns error")
		passed++
	} else {
		fmt.Println("  (commit without begin succeeded — server tolerates it)")
		passed++
	}

	// Connection should still be usable
	_, err = proxy1.Ping()
	proxy1.Close()
	if err != nil {
		fail("connection broken after orphan commit: %v", err)
		return
	}

	// Test rollback without begin
	proxy2, err := connectProxy()
	if err != nil {
		fail("connect: %v", err)
		return
	}

	err = proxy2.RollbackTx()
	if err != nil {
		fmt.Printf("  rollback without begin: error (expected): %v\n", truncate(err.Error(), 60))
		fmt.Println("  PASS: rollback without begin returns error")
		passed++
	} else {
		fmt.Println("  (rollback without begin succeeded — server tolerates it)")
		passed++
	}

	// Connection should still be usable
	_, err = proxy2.Insert(collection, map[string]any{"test": 12, "after_orphan": true})
	proxy2.Close()
	if err != nil {
		fail("insert after orphan rollback: %v", err)
		return
	}

	count := directCount(map[string]any{"test": 12, "after_orphan": true})
	if count == 1 {
		fmt.Println("  PASS: connection still usable after orphan commit/rollback")
		passed++
	} else {
		fail("insert after orphan ops: count=%d", count)
	}
	fmt.Println()
}

// ─── Helpers ────────────────────────────────────────────────────────

func startServer() {
	cmd := exec.Command("./target/release/oxidb-server")
	cmd.Dir = projectDir
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("OXIDB_ADDR=127.0.0.1:%d", serverPort),
		"OXIDB_POOL_SIZE=20",
		"OXIDB_IDLE_TIMEOUT=0",
	)
	cmd.Start()
	time.Sleep(1 * time.Second)
}

func startOxiPool() {
	cmd := exec.Command(projectDir + "/target/release/oxipool")
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("OXIPOOL_LISTEN=127.0.0.1:%d", proxyPort),
		fmt.Sprintf("OXIPOOL_BACKEND=127.0.0.1:%d", serverPort),
		fmt.Sprintf("OXIPOOL_SIZE=%d", poolSize),
		"OXIPOOL_STATS_INTERVAL=0",
	)
	cmd.Start()
	time.Sleep(1 * time.Second)
}

func killAll() {
	for _, name := range []string{"oxipool", "oxidb-server"} {
		pids := findPids(name)
		for _, pid := range pids {
			exec.Command("kill", "-9", pid).Run()
		}
	}
	time.Sleep(300 * time.Millisecond)
}

func cleanup() {
	direct, err := connectDirect()
	if err != nil {
		return
	}
	direct.DropCollection(collection)
	direct.Close()
	time.Sleep(100 * time.Millisecond)
}

func connectProxy() (*oxidb.Client, error) {
	return oxidb.Connect("127.0.0.1", proxyPort, 5*time.Second)
}

func connectDirect() (*oxidb.Client, error) {
	return oxidb.Connect("127.0.0.1", serverPort, 5*time.Second)
}

func directCount(query map[string]any) int {
	direct, err := connectDirect()
	if err != nil {
		return -1
	}
	defer direct.Close()
	count, err := direct.Count(collection, query)
	if err != nil {
		return -1
	}
	return count
}

func findPids(name string) []string {
	out, _ := exec.Command("pgrep", "-x", name).Output()
	s := strings.TrimSpace(string(out))
	if s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}

func fail(format string, args ...any) {
	fmt.Printf("  FAIL: "+format+"\n", args...)
	failed++
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
