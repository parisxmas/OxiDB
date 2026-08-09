package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/parisxmas/OxiDB/go/oxidb"
)

var (
	host = envOr("OXIDB_HOST", "127.0.0.1")
	port = envInt("OXIDB_PORT", 14888)
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		n, _ := strconv.Atoi(v)
		if n > 0 {
			return n
		}
	}
	return def
}

func connect() *oxidb.Client {
	c, err := oxidb.Connect(host, port, 10*time.Second)
	if err != nil {
		fmt.Printf("FATAL: connect: %v\n", err)
		os.Exit(1)
	}
	return c
}

var (
	passed int
	failed int
)

func check(name string, ok bool, detail string) {
	if ok {
		passed++
		fmt.Printf("  ✓ %s\n", name)
	} else {
		failed++
		fmt.Printf("  ✗ %s — %s\n", name, detail)
	}
}

func main() {
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║           OxiDB ACID Consistency Test Suite                 ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Println()

	testCommitPersists()
	testRollbackReverts()
	testIsolationReadUncommitted()
	testAtomicMultiDocUpdate()
	testConcurrentTransferConsistency()
	testUniqueConstraintInTx()
	testVersionConflictDetection()
	testConcurrentInsertCount()
	testDeleteConsistency()
	testLargeTransactionAtomicity()

	fmt.Println()
	fmt.Println("═══════════════════════════════════════════════════════════════")
	fmt.Printf("Results: %d passed, %d failed out of %d\n", passed, failed, passed+failed)
	fmt.Println("═══════════════════════════════════════════════════════════════")

	if failed > 0 {
		os.Exit(1)
	}
}

// ─── Test 1: Committed transaction data persists ──────────────────
func testCommitPersists() {
	fmt.Println("═══ Test 1: Commit Persists ═══")
	c := connect()
	defer c.Close()

	coll := "acid_commit"
	c.DropCollection(coll)

	c.BeginTx()
	c.Insert(coll, map[string]any{"key": "a", "value": 100})
	c.Insert(coll, map[string]any{"key": "b", "value": 200})
	err := c.CommitTx()
	check("Commit succeeds", err == nil, fmt.Sprintf("%v", err))

	// Verify via separate connection
	c2 := connect()
	defer c2.Close()
	n, _ := c2.Count(coll, map[string]any{})
	check("Both docs visible after commit", n == 2, fmt.Sprintf("count=%d", n))

	doc, _ := c2.FindOne(coll, map[string]any{"key": "a"})
	val := 0
	if doc != nil {
		if v, ok := doc["value"].(float64); ok {
			val = int(v)
		}
	}
	check("Correct value after commit", val == 100, fmt.Sprintf("value=%d", val))
	fmt.Println()
}

// ─── Test 2: Rollback reverts all changes ─────────────────────────
func testRollbackReverts() {
	fmt.Println("═══ Test 2: Rollback Reverts ═══")
	c := connect()
	defer c.Close()

	coll := "acid_rollback"
	c.DropCollection(coll)
	c.Insert(coll, map[string]any{"key": "pre", "value": 1})

	c.BeginTx()
	c.Insert(coll, map[string]any{"key": "tx1", "value": 2})
	c.Insert(coll, map[string]any{"key": "tx2", "value": 3})
	c.UpdateOne(coll, map[string]any{"key": "pre"}, map[string]any{"$set": map[string]any{"value": 999}})
	err := c.RollbackTx()
	check("Rollback succeeds", err == nil, fmt.Sprintf("%v", err))

	c2 := connect()
	defer c2.Close()
	n, _ := c2.Count(coll, map[string]any{})
	check("Only pre-tx doc remains", n == 1, fmt.Sprintf("count=%d", n))

	doc, _ := c2.FindOne(coll, map[string]any{"key": "pre"})
	val := 0
	if doc != nil {
		if v, ok := doc["value"].(float64); ok {
			val = int(v)
		}
	}
	check("Pre-tx doc unchanged", val == 1, fmt.Sprintf("value=%d", val))
	fmt.Println()
}

// ─── Test 3: Isolation — uncommitted data not visible to others ───
func testIsolationReadUncommitted() {
	fmt.Println("═══ Test 3: Isolation ═══")
	c1 := connect()
	defer c1.Close()
	c2 := connect()
	defer c2.Close()

	coll := "acid_isolation"
	c1.DropCollection(coll)

	// c1 starts a transaction and inserts
	c1.BeginTx()
	c1.Insert(coll, map[string]any{"key": "secret", "value": 42})

	// c2 should NOT see uncommitted data
	n, _ := c2.Count(coll, map[string]any{})
	check("Uncommitted insert invisible to other conn", n == 0, fmt.Sprintf("count=%d", n))

	// Now commit
	c1.CommitTx()

	// c2 should now see it
	n, _ = c2.Count(coll, map[string]any{})
	check("Committed insert visible to other conn", n == 1, fmt.Sprintf("count=%d", n))
	fmt.Println()
}

// ─── Test 4: Atomic multi-doc update ──────────────────────────────
func testAtomicMultiDocUpdate() {
	fmt.Println("═══ Test 4: Atomic Multi-Doc Update ═══")
	c := connect()
	defer c.Close()

	coll := "acid_atomic"
	c.DropCollection(coll)

	// Seed accounts
	c.Insert(coll, map[string]any{"account": "A", "balance": 1000})
	c.Insert(coll, map[string]any{"account": "B", "balance": 1000})

	// Transfer 300 from A to B in a transaction
	c.BeginTx()
	c.UpdateOne(coll, map[string]any{"account": "A"}, map[string]any{"$inc": map[string]any{"balance": -300}})
	c.UpdateOne(coll, map[string]any{"account": "B"}, map[string]any{"$inc": map[string]any{"balance": 300}})
	err := c.CommitTx()
	check("Transfer commits", err == nil, fmt.Sprintf("%v", err))

	// Verify balances
	c2 := connect()
	defer c2.Close()
	docA, _ := c2.FindOne(coll, map[string]any{"account": "A"})
	docB, _ := c2.FindOne(coll, map[string]any{"account": "B"})
	balA, balB := 0, 0
	if docA != nil {
		if v, ok := docA["balance"].(float64); ok {
			balA = int(v)
		}
	}
	if docB != nil {
		if v, ok := docB["balance"].(float64); ok {
			balB = int(v)
		}
	}
	check("A balance = 700", balA == 700, fmt.Sprintf("balance=%d", balA))
	check("B balance = 1300", balB == 1300, fmt.Sprintf("balance=%d", balB))
	check("Total preserved (2000)", balA+balB == 2000, fmt.Sprintf("total=%d", balA+balB))
	fmt.Println()
}

// ─── Test 5: Concurrent transfers — total balance preserved ───────
func testConcurrentTransferConsistency() {
	fmt.Println("═══ Test 5: Concurrent Transfer Consistency ═══")
	c := connect()
	defer c.Close()

	coll := "acid_transfers"
	c.DropCollection(coll)

	numAccounts := 10
	initialBalance := 10000
	totalExpected := numAccounts * initialBalance

	for i := 0; i < numAccounts; i++ {
		c.Insert(coll, map[string]any{"account": fmt.Sprintf("ACC%03d", i), "balance": initialBalance})
	}

	// 10 workers do 100 random transfers each
	workers := 10
	transfers := 100
	var successCount int64
	var failCount int64
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(seed)))
			tc := connect()
			defer tc.Close()

			for i := 0; i < transfers; i++ {
				from := fmt.Sprintf("ACC%03d", rng.Intn(numAccounts))
				to := fmt.Sprintf("ACC%03d", rng.Intn(numAccounts))
				if from == to {
					continue
				}
				amount := 1 + rng.Intn(100)

				err := tc.WithTransaction(func() error {
					tc.UpdateOne(coll, map[string]any{"account": from}, map[string]any{"$inc": map[string]any{"balance": -amount}})
					tc.UpdateOne(coll, map[string]any{"account": to}, map[string]any{"$inc": map[string]any{"balance": amount}})
					return nil
				})
				if err != nil {
					atomic.AddInt64(&failCount, 1)
				} else {
					atomic.AddInt64(&successCount, 1)
				}
			}
		}(w)
	}
	wg.Wait()

	fmt.Printf("  Transfers: %d success, %d failed (OCC conflicts expected)\n", successCount, failCount)

	// Verify total balance is preserved
	c2 := connect()
	defer c2.Close()
	docs, _ := c2.Find(coll, map[string]any{}, nil)
	total := 0
	for _, d := range docs {
		if v, ok := d["balance"].(float64); ok {
			total += int(v)
		}
	}
	check("Total balance preserved after concurrent transfers", total == totalExpected,
		fmt.Sprintf("expected=%d got=%d", totalExpected, total))
	fmt.Println()
}

// ─── Test 6: Unique constraint respected inside transaction ───────
func testUniqueConstraintInTx() {
	fmt.Println("═══ Test 6: Unique Constraint in Transaction ═══")
	c := connect()
	defer c.Close()

	coll := "acid_unique"
	c.DropCollection(coll)
	c.CreateUniqueIndex(coll, "email")
	c.Insert(coll, map[string]any{"email": "alice@test.com", "name": "Alice"})

	// Try to insert duplicate in transaction
	c.BeginTx()
	_, err := c.Insert(coll, map[string]any{"email": "alice@test.com", "name": "Fake Alice"})
	hasDupErr := err != nil
	if hasDupErr {
		c.RollbackTx()
	} else {
		// Even if insert didn't error, commit should fail or the constraint should hold
		c.CommitTx()
	}

	c2 := connect()
	defer c2.Close()
	n, _ := c2.Count(coll, map[string]any{})
	check("Unique constraint prevents duplicate", n == 1, fmt.Sprintf("count=%d", n))

	doc, _ := c2.FindOne(coll, map[string]any{"email": "alice@test.com"})
	name := ""
	if doc != nil {
		if v, ok := doc["name"].(string); ok {
			name = v
		}
	}
	check("Original doc unchanged", name == "Alice", fmt.Sprintf("name=%s", name))
	fmt.Println()
}

// ─── Test 7: OCC version conflict detection ──────────────────────
func testVersionConflictDetection() {
	fmt.Println("═══ Test 7: OCC Version Conflict Detection ═══")
	c1 := connect()
	defer c1.Close()
	c2 := connect()
	defer c2.Close()

	coll := "acid_occ"
	c1.DropCollection(coll)
	c1.Insert(coll, map[string]any{"key": "contested", "value": 0})

	// Both start transactions
	c1.BeginTx()
	c2.BeginTx()

	// Both read and update the same doc
	c1.UpdateOne(coll, map[string]any{"key": "contested"}, map[string]any{"$set": map[string]any{"value": 1}})
	c2.UpdateOne(coll, map[string]any{"key": "contested"}, map[string]any{"$set": map[string]any{"value": 2}})

	// First commit should succeed
	err1 := c1.CommitTx()
	// Second should fail (OCC conflict)
	err2 := c2.CommitTx()

	check("First committer wins", err1 == nil, fmt.Sprintf("%v", err1))
	check("Second committer gets conflict", err2 != nil, "no error returned")

	// Value should be from first committer
	c3 := connect()
	defer c3.Close()
	doc, _ := c3.FindOne(coll, map[string]any{"key": "contested"})
	val := -1
	if doc != nil {
		if v, ok := doc["value"].(float64); ok {
			val = int(v)
		}
	}
	check("Value from first committer (1)", val == 1, fmt.Sprintf("value=%d", val))
	fmt.Println()
}

// ─── Test 8: Concurrent insert count consistency ──────────────────
func testConcurrentInsertCount() {
	fmt.Println("═══ Test 8: Concurrent Insert Count ═══")
	c := connect()
	defer c.Close()

	coll := "acid_count"
	c.DropCollection(coll)

	workers := 10
	insertsPerWorker := 100
	expected := workers * insertsPerWorker

	var wg sync.WaitGroup
	var errors int64
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			tc := connect()
			defer tc.Close()
			for i := 0; i < insertsPerWorker; i++ {
				_, err := tc.Insert(coll, map[string]any{
					"worker": id,
					"seq":    i,
					"value":  fmt.Sprintf("w%d_s%d", id, i),
				})
				if err != nil {
					atomic.AddInt64(&errors, 1)
				}
			}
		}(w)
	}
	wg.Wait()

	c2 := connect()
	defer c2.Close()
	n, _ := c2.Count(coll, map[string]any{})
	check(fmt.Sprintf("All %d inserts counted", expected), n == expected,
		fmt.Sprintf("expected=%d got=%d errors=%d", expected, n, errors))
	fmt.Println()
}

// ─── Test 9: Delete consistency ───────────────────────────────────
func testDeleteConsistency() {
	fmt.Println("═══ Test 9: Delete Consistency ═══")
	c := connect()
	defer c.Close()

	coll := "acid_delete"
	c.DropCollection(coll)

	// Insert 100 docs
	for i := 0; i < 100; i++ {
		c.Insert(coll, map[string]any{"idx": i, "group": i % 5})
	}

	n, _ := c.Count(coll, map[string]any{})
	check("100 docs inserted", n == 100, fmt.Sprintf("count=%d", n))

	// Delete group=0 (20 docs) in transaction
	c.BeginTx()
	c.Delete(coll, map[string]any{"group": 0})
	c.CommitTx()

	n, _ = c.Count(coll, map[string]any{})
	check("80 docs remain after delete group=0", n == 80, fmt.Sprintf("count=%d", n))

	// Delete group=1 and rollback
	c.BeginTx()
	c.Delete(coll, map[string]any{"group": 1})
	c.RollbackTx()

	n, _ = c.Count(coll, map[string]any{})
	check("Still 80 docs after rollback", n == 80, fmt.Sprintf("count=%d", n))

	// Verify group=1 docs still exist
	n1, _ := c.Count(coll, map[string]any{"group": 1})
	check("Group=1 docs survive rollback (20)", n1 == 20, fmt.Sprintf("count=%d", n1))
	fmt.Println()
}

// ─── Test 10: Large transaction atomicity ─────────────────────────
func testLargeTransactionAtomicity() {
	fmt.Println("═══ Test 10: Large Transaction Atomicity ═══")
	c := connect()
	defer c.Close()

	coll := "acid_large_tx"
	c.DropCollection(coll)

	// Insert 500 docs in a single transaction
	c.BeginTx()
	for i := 0; i < 500; i++ {
		c.Insert(coll, map[string]any{"idx": i, "data": fmt.Sprintf("doc_%d", i)})
	}
	err := c.CommitTx()
	check("Large tx (500 inserts) commits", err == nil, fmt.Sprintf("%v", err))

	c2 := connect()
	defer c2.Close()
	n, _ := c2.Count(coll, map[string]any{})
	check("All 500 docs present", n == 500, fmt.Sprintf("count=%d", n))

	// Verify first and last
	d1, _ := c2.FindOne(coll, map[string]any{"idx": 0})
	d500, _ := c2.FindOne(coll, map[string]any{"idx": 499})
	check("First doc exists (idx=0)", d1 != nil, "nil")
	check("Last doc exists (idx=499)", d500 != nil, "nil")

	// Large rollback
	c.BeginTx()
	for i := 500; i < 1000; i++ {
		c.Insert(coll, map[string]any{"idx": i, "data": fmt.Sprintf("doc_%d", i)})
	}
	c.RollbackTx()

	n, _ = c2.Count(coll, map[string]any{})
	check("Count still 500 after large rollback", n == 500, fmt.Sprintf("count=%d", n))
	fmt.Println()
}
