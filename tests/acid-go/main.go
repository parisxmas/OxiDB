package main

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/parisxmas/OxiDB/clients/go/oxidb"
)

// ACID test for OxiPool
// Tests that uncommitted transactions are rolled back when:
//   1. Client disconnects mid-transaction (OxiPool auto-rollback)
//   2. OxiPool crashes mid-transaction (server cleans up dead connection)
//   3. Committed transactions survive OxiPool restart

const acidCollection = "acid_test"

func main() {
	proxyPort := 4445
	directPort := 4444
	if p := os.Getenv("OXIDB_PORT"); p != "" {
		directPort, _ = strconv.Atoi(p)
	}
	if p := os.Getenv("OXIPOOL_PORT"); p != "" {
		proxyPort, _ = strconv.Atoi(p)
	}

	fmt.Println("=== OxiPool ACID Test ===")
	fmt.Println()

	// Setup: direct connection to server for verification
	direct, err := oxidb.Connect("127.0.0.1", directPort, 5*time.Second)
	if err != nil {
		fmt.Printf("FATAL: cannot connect directly to server on :%d: %v\n", directPort, err)
		os.Exit(1)
	}
	defer direct.Close()

	_ = direct.DropCollection(acidCollection)
	_ = direct.CreateCollection(acidCollection)
	fmt.Printf("Setup: collection '%s' created (direct on :%d)\n\n", acidCollection, directPort)

	passed := 0
	failed := 0

	// ─── Test 1: Committed transaction persists ─────────────────────
	fmt.Println("TEST 1: Committed transaction persists through OxiPool")
	{
		proxy, err := oxidb.Connect("127.0.0.1", proxyPort, 5*time.Second)
		if err != nil {
			fmt.Printf("  FAIL: connect to proxy: %v\n", err)
			failed++
		} else {
			err = proxy.WithTransaction(func() error {
				_, err := proxy.Insert(acidCollection, map[string]any{
					"test": "committed", "value": 1,
				})
				if err != nil {
					return err
				}
				_, err = proxy.Insert(acidCollection, map[string]any{
					"test": "committed", "value": 2,
				})
				return err
			})
			proxy.Close()

			if err != nil {
				fmt.Printf("  FAIL: transaction error: %v\n", err)
				failed++
			} else {
				time.Sleep(100 * time.Millisecond)
				count, _ := direct.Count(acidCollection, map[string]any{"test": "committed"})
				if count == 2 {
					fmt.Printf("  PASS: 2 committed docs found (expected 2)\n")
					passed++
				} else {
					fmt.Printf("  FAIL: found %d docs, expected 2\n", count)
					failed++
				}
			}
		}
	}
	fmt.Println()

	// ─── Test 2: Client disconnect → auto-rollback ──────────────────
	fmt.Println("TEST 2: Client disconnect mid-transaction → auto-rollback")
	{
		proxy, err := oxidb.Connect("127.0.0.1", proxyPort, 5*time.Second)
		if err != nil {
			fmt.Printf("  FAIL: connect to proxy: %v\n", err)
			failed++
		} else {
			// Begin transaction and insert, but DON'T commit
			_, err = proxy.BeginTx()
			if err != nil {
				fmt.Printf("  FAIL: begin_tx: %v\n", err)
				failed++
			} else {
				_, err = proxy.Insert(acidCollection, map[string]any{
					"test": "should_rollback", "value": 999,
				})
				if err != nil {
					fmt.Printf("  FAIL: insert in tx: %v\n", err)
					failed++
				} else {
					// Close connection WITHOUT committing
					// OxiPool should detect disconnect and send rollback_tx to server
					proxy.Close()
					fmt.Println("  Client disconnected without commit")

					time.Sleep(500 * time.Millisecond) // give OxiPool time to rollback

					count, _ := direct.Count(acidCollection, map[string]any{"test": "should_rollback"})
					if count == 0 {
						fmt.Println("  PASS: 0 uncommitted docs found (rolled back)")
						passed++
					} else {
						fmt.Printf("  FAIL: found %d docs, expected 0 (rollback failed)\n", count)
						failed++
					}
				}
			}
		}
	}
	fmt.Println()

	// ─── Test 3: OxiPool crash mid-transaction → server rollback ────
	fmt.Println("TEST 3: OxiPool crash mid-transaction → server auto-cleanup")
	{
		// We need to start our own OxiPool instance on a different port for this test
		crashPort := 4446
		fmt.Printf("  Starting temporary OxiPool on :%d\n", crashPort)

		cmd := exec.Command("./target/release/oxipool")
		cmd.Dir = "/Users/barisakin/source/docdb"
		cmd.Env = append(os.Environ(),
			fmt.Sprintf("OXIPOOL_LISTEN=127.0.0.1:%d", crashPort),
			fmt.Sprintf("OXIPOOL_BACKEND=127.0.0.1:%d", directPort),
			"OXIPOOL_SIZE=2",
			"OXIPOOL_STATS_INTERVAL=0",
		)
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			fmt.Printf("  FAIL: start oxipool: %v\n", err)
			failed++
		} else {
			time.Sleep(1 * time.Second) // wait for startup

			proxy, err := oxidb.Connect("127.0.0.1", crashPort, 5*time.Second)
			if err != nil {
				fmt.Printf("  FAIL: connect to crash proxy: %v\n", err)
				cmd.Process.Kill()
				cmd.Wait()
				failed++
			} else {
				// Begin transaction and insert
				_, err = proxy.BeginTx()
				if err != nil {
					fmt.Printf("  FAIL: begin_tx: %v\n", err)
					proxy.Close()
					cmd.Process.Kill()
					cmd.Wait()
					failed++
				} else {
					_, err = proxy.Insert(acidCollection, map[string]any{
						"test": "crash_rollback", "value": 777,
					})
					if err != nil {
						fmt.Printf("  FAIL: insert in tx: %v\n", err)
						proxy.Close()
						cmd.Process.Kill()
						cmd.Wait()
						failed++
					} else {
						// KILL OxiPool (simulating crash) — don't close client gracefully
						fmt.Println("  Killing OxiPool process (SIGKILL)")
						cmd.Process.Kill()
						cmd.Wait()

						time.Sleep(1 * time.Second) // give server time to detect dead connection

						count, _ := direct.Count(acidCollection, map[string]any{"test": "crash_rollback"})
						if count == 0 {
							fmt.Println("  PASS: 0 uncommitted docs found (server rolled back on connection loss)")
							passed++
						} else {
							fmt.Printf("  FAIL: found %d docs, expected 0\n", count)
							failed++
						}
					}
				}
			}
		}
	}
	fmt.Println()

	// ─── Test 4: Concurrent transactions isolation ──────────────────
	fmt.Println("TEST 4: Concurrent transaction isolation through OxiPool")
	{
		proxy1, err1 := oxidb.Connect("127.0.0.1", proxyPort, 5*time.Second)
		proxy2, err2 := oxidb.Connect("127.0.0.1", proxyPort, 5*time.Second)
		if err1 != nil || err2 != nil {
			fmt.Printf("  FAIL: connect: %v / %v\n", err1, err2)
			failed++
		} else {
			// TX1: insert doc A
			_, _ = proxy1.BeginTx()
			proxy1.Insert(acidCollection, map[string]any{
				"test": "isolation", "tx": "tx1", "value": 100,
			})

			// TX2: insert doc B
			_, _ = proxy2.BeginTx()
			proxy2.Insert(acidCollection, map[string]any{
				"test": "isolation", "tx": "tx2", "value": 200,
			})

			// Commit TX1 only
			err := proxy1.CommitTx()
			if err != nil {
				fmt.Printf("  FAIL: commit tx1: %v\n", err)
				failed++
			} else {
				// Rollback TX2
				proxy2.RollbackTx()

				proxy1.Close()
				proxy2.Close()

				time.Sleep(200 * time.Millisecond)

				// Verify: tx1 doc should exist, tx2 doc should not
				countTx1, _ := direct.Count(acidCollection, map[string]any{"test": "isolation", "tx": "tx1"})
				countTx2, _ := direct.Count(acidCollection, map[string]any{"test": "isolation", "tx": "tx2"})

				if countTx1 == 1 && countTx2 == 0 {
					fmt.Println("  PASS: tx1 committed (1 doc), tx2 rolled back (0 docs)")
					passed++
				} else {
					fmt.Printf("  FAIL: tx1=%d (expected 1), tx2=%d (expected 0)\n", countTx1, countTx2)
					failed++
				}
			}
		}
	}
	fmt.Println()

	// ─── Test 5: Data integrity after OxiPool restart ───────────────
	fmt.Println("TEST 5: Data persists after OxiPool restart")
	{
		// Insert through proxy
		proxy, err := oxidb.Connect("127.0.0.1", proxyPort, 5*time.Second)
		if err != nil {
			fmt.Printf("  FAIL: connect: %v\n", err)
			failed++
		} else {
			_, err = proxy.Insert(acidCollection, map[string]any{
				"test": "persist", "value": 42,
			})
			proxy.Close()
			if err != nil {
				fmt.Printf("  FAIL: insert: %v\n", err)
				failed++
			} else {
				// Count before restart
				countBefore, _ := direct.Count(acidCollection, map[string]any{})

				// Kill and restart OxiPool
				fmt.Println("  Restarting OxiPool...")
				killOxiPool(proxyPort)
				time.Sleep(500 * time.Millisecond)

				cmd := exec.Command("./target/release/oxipool")
				cmd.Dir = "/Users/barisakin/source/docdb"
				cmd.Env = append(os.Environ(),
					fmt.Sprintf("OXIPOOL_LISTEN=127.0.0.1:%d", proxyPort),
					fmt.Sprintf("OXIPOOL_BACKEND=127.0.0.1:%d", directPort),
					"OXIPOOL_SIZE=10",
					"OXIPOOL_STATS_INTERVAL=0",
				)
				cmd.Stderr = os.Stderr
				cmd.Start()
				time.Sleep(1 * time.Second)

				// Count after restart — verify through direct connection
				countAfter, _ := direct.Count(acidCollection, map[string]any{})

				if countAfter == countBefore {
					fmt.Printf("  PASS: %d docs before restart = %d docs after\n", countBefore, countAfter)
					passed++
				} else {
					fmt.Printf("  FAIL: %d before, %d after\n", countBefore, countAfter)
					failed++
				}
			}
		}
	}
	fmt.Println()

	// ─── Report ─────────────────────────────────────────────────────
	total := passed + failed
	fmt.Println("╔══════════════════════════════════════════════════════════╗")
	fmt.Println("║                  ACID TEST RESULTS                      ║")
	fmt.Println("╠══════════════════════════════════════════════════════════╣")
	fmt.Printf("║   Passed:             %-34d ║\n", passed)
	fmt.Printf("║   Failed:             %-34d ║\n", failed)
	fmt.Printf("║   Total:              %-34d ║\n", total)
	fmt.Println("╚══════════════════════════════════════════════════════════╝")

	if failed > 0 {
		fmt.Printf("\nFAILED: %d/%d tests failed\n", failed, total)
		os.Exit(1)
	} else {
		fmt.Println("\nAll ACID tests passed.")
	}
}

func killOxiPool(port int) {
	out, _ := exec.Command("lsof", "-ti", fmt.Sprintf(":%d", port)).Output()
	for _, pid := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if pid != "" {
			exec.Command("kill", "-9", pid).Run()
		}
	}
}
