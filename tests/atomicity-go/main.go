// atomicity-go — cross-collection transaction atomicity regression.
//
// Companion to crash-recovery-go. That one proves single-collection
// durability ("ack'd writes survive SIGKILL"); this one proves
// cross-collection atomicity ("a tx that touches multiple collections
// commits all-or-nothing across crashes").
//
// What this protects against:
//
//   The DMS upload path commits a Document row and a Version row in
//   ONE oxidb transaction. If oxidb crashes between the two writes
//   landing on disk, recovery must NOT leave us with the Document
//   present and the Version missing — that would corrupt the version
//   history (the doc has no v1) without anyone noticing.
//
// Two scenarios, each independent:
//
//   Scenario A — crash BEFORE commit:
//     Begin tx, insert doc, insert version, SIGKILL (no commit_tx
//     ever sent). Restart. Expect: neither row landed. The tx_log
//     never recorded this tx as committed, so WAL replay drops both
//     entries.
//
//   Scenario B — crash AFTER commit:
//     Begin tx, insert doc, insert version, commit_tx (returns OK),
//     THEN SIGKILL. Restart. Expect: both rows present. Commit point
//     was the tx_log fsync; everything after that is recovery's job.
//
// Each scenario uses its own tempdir so they're independent and can
// run in parallel if we ever choose to.
package main

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"github.com/parisxmas/OxiDB/clients/go/oxidb"
)

const (
	docColl  = "tx_docs"
	versColl = "tx_versions"
	// Number of {doc + version} pairs per scenario. Higher numbers
	// give more chances to catch a partial-tx bug if one exists,
	// but inflate the test runtime under strict-fsync mode (~3ms
	// per write on macOS APFS).
	pairs = 50
)

func main() {
	bin := envOr("OXIDB_BIN", "../../target/release/oxidb-server")
	if !filepath.IsAbs(bin) {
		abs, err := filepath.Abs(bin)
		if err == nil {
			bin = abs
		}
	}
	if _, err := os.Stat(bin); err != nil {
		fatalf("oxidb-server binary not found at %s — set OXIDB_BIN or build it", bin)
	}

	fmt.Println("=== OxiDB cross-collection atomicity test ===")
	fmt.Printf("  binary: %s\n", bin)
	fmt.Printf("  pairs : %d (per scenario)\n\n", pairs)

	failures := 0
	if !runScenarioA(bin) {
		failures++
	}
	fmt.Println()
	if !runScenarioB(bin) {
		failures++
	}
	fmt.Println()
	if !runScenarioC(bin) {
		failures++
	}

	fmt.Println()
	if failures > 0 {
		fmt.Printf("=== ATOMICITY: %d FAILURE(S) ===\n", failures)
		os.Exit(1)
	}
	fmt.Println("=== ATOMICITY: OK ===")
}

// Scenario A: SIGKILL after the tx writes are buffered server-side
// but BEFORE commit_tx completes. Every successful commit fsyncs the
// tx_log; without that fsync the tx is invisible to recovery and all
// its writes get dropped on replay.
func runScenarioA(bin string) bool {
	tmpDir, port := setup("atomicity-A")
	defer os.RemoveAll(tmpDir)

	fmt.Println("Scenario A: kill BEFORE commit → expect 0 rows in each collection")
	srv := startServer(bin, tmpDir, port)
	waitReady(port)

	c, err := oxidb.Connect("127.0.0.1", port, 5*time.Second)
	if err != nil {
		killAndWait(srv)
		fmt.Printf("  FAIL: connect: %v\n", err)
		return false
	}
	if err := c.CreateCollection(docColl); err != nil {
		killAndWait(srv)
		fmt.Printf("  FAIL: create %s: %v\n", docColl, err)
		return false
	}
	if err := c.CreateCollection(versColl); err != nil {
		killAndWait(srv)
		fmt.Printf("  FAIL: create %s: %v\n", versColl, err)
		return false
	}

	// Begin tx and stuff it with doc/version pairs. We rely on
	// tx_insert returning the assigned id so the version row carries
	// the correct foreign key — this is the exact pattern the DMS
	// upload path uses.
	if _, err := c.BeginTx(); err != nil {
		killAndWait(srv)
		fmt.Printf("  FAIL: begin_tx: %v\n", err)
		return false
	}
	for i := 0; i < pairs; i++ {
		docResp, err := c.Insert(docColl, map[string]any{"seq": i, "title": fmt.Sprintf("doc-%d", i)})
		if err != nil {
			killAndWait(srv)
			fmt.Printf("  FAIL: tx insert doc[%d]: %v\n", i, err)
			return false
		}
		docID := docResp["id"]
		_, err = c.Insert(versColl, map[string]any{"seq": i, "document_id": docID, "version_no": 1})
		if err != nil {
			killAndWait(srv)
			fmt.Printf("  FAIL: tx insert ver[%d]: %v\n", i, err)
			return false
		}
	}

	// All writes are buffered server-side. The tx_log has NOT yet
	// recorded this tx as committed — there's been no commit_tx call.
	// Now: hard kill. No graceful shutdown, no commit fsync.
	fmt.Printf("  buffered %d×2 inserts; SIGKILL...\n", pairs)
	if err := srv.Process.Signal(syscall.SIGKILL); err != nil {
		fmt.Printf("  FAIL: SIGKILL: %v\n", err)
		return false
	}
	_ = srv.Wait()

	// Restart and verify the abandoned tx left zero traces.
	srv2 := startServer(bin, tmpDir, port)
	defer func() { _ = srv2.Process.Signal(syscall.SIGTERM); srv2.Wait() }()
	waitReady(port)

	c2, err := oxidb.Connect("127.0.0.1", port, 5*time.Second)
	if err != nil {
		fmt.Printf("  FAIL: post-restart connect: %v\n", err)
		return false
	}
	defer c2.Close()

	docCount, _ := c2.Count(docColl, map[string]any{})
	verCount, _ := c2.Count(versColl, map[string]any{})
	fmt.Printf("  recovered: docs=%d versions=%d (expected 0/0)\n", docCount, verCount)

	if docCount == 0 && verCount == 0 {
		fmt.Println("  PASS: pre-commit SIGKILL left no half-tx state")
		return true
	}
	fmt.Println("  FAIL: pre-commit SIGKILL leaked tx writes — atomicity broken")
	return false
}

// Scenario B: tx commits cleanly, then SIGKILL. The commit_tx ack
// from the server means tx_log fsync is done. Everything after that
// is just background bookkeeping; recovery must restore both writes
// from the WAL.
func runScenarioB(bin string) bool {
	tmpDir, port := setup("atomicity-B")
	defer os.RemoveAll(tmpDir)

	fmt.Println("Scenario B: kill AFTER commit → expect both collections fully populated")
	srv := startServer(bin, tmpDir, port)
	waitReady(port)

	c, err := oxidb.Connect("127.0.0.1", port, 5*time.Second)
	if err != nil {
		killAndWait(srv)
		fmt.Printf("  FAIL: connect: %v\n", err)
		return false
	}
	if err := c.CreateCollection(docColl); err != nil {
		killAndWait(srv)
		fmt.Printf("  FAIL: create %s: %v\n", docColl, err)
		return false
	}
	if err := c.CreateCollection(versColl); err != nil {
		killAndWait(srv)
		fmt.Printf("  FAIL: create %s: %v\n", versColl, err)
		return false
	}

	// Same buffered pattern, then commit. After CommitTx returns OK,
	// the server has fsync'd the tx_log entry — that's the moment
	// the writes are durable.
	if _, err := c.BeginTx(); err != nil {
		killAndWait(srv)
		fmt.Printf("  FAIL: begin_tx: %v\n", err)
		return false
	}
	for i := 0; i < pairs; i++ {
		docResp, err := c.Insert(docColl, map[string]any{"seq": i, "title": fmt.Sprintf("doc-%d", i)})
		if err != nil {
			killAndWait(srv)
			fmt.Printf("  FAIL: tx insert doc[%d]: %v\n", i, err)
			return false
		}
		docID := docResp["id"]
		_, err = c.Insert(versColl, map[string]any{"seq": i, "document_id": docID, "version_no": 1})
		if err != nil {
			killAndWait(srv)
			fmt.Printf("  FAIL: tx insert ver[%d]: %v\n", i, err)
			return false
		}
	}
	if err := c.CommitTx(); err != nil {
		killAndWait(srv)
		fmt.Printf("  FAIL: commit_tx: %v\n", err)
		return false
	}
	c.Close()
	fmt.Printf("  committed %d×2 writes; SIGKILL...\n", pairs)
	if err := srv.Process.Signal(syscall.SIGKILL); err != nil {
		fmt.Printf("  FAIL: SIGKILL: %v\n", err)
		return false
	}
	_ = srv.Wait()

	// Cold restart and confirm everything came back.
	srv2 := startServer(bin, tmpDir, port)
	defer func() { _ = srv2.Process.Signal(syscall.SIGTERM); srv2.Wait() }()
	waitReady(port)

	c2, err := oxidb.Connect("127.0.0.1", port, 5*time.Second)
	if err != nil {
		fmt.Printf("  FAIL: post-restart connect: %v\n", err)
		return false
	}
	defer c2.Close()

	docCount, _ := c2.Count(docColl, map[string]any{})
	verCount, _ := c2.Count(versColl, map[string]any{})
	fmt.Printf("  recovered: docs=%d versions=%d (expected %d/%d)\n",
		docCount, verCount, pairs, pairs)

	ok := int(docCount) == pairs && int(verCount) == pairs

	// FK integrity spot-check — a version's document_id should
	// resolve to a real doc. Catches a hypothetical bug where both
	// counts match but the link got mangled in WAL replay.
	if ok {
		vers, _ := c2.Find(versColl, map[string]any{"seq": pairs / 2}, nil)
		if len(vers) != 1 {
			fmt.Printf("  FAIL: spot version seq=%d returned %d rows\n", pairs/2, len(vers))
			ok = false
		} else {
			docID := vers[0]["document_id"]
			docs, _ := c2.Find(docColl, map[string]any{"_id": docID}, nil)
			if len(docs) != 1 {
				fmt.Printf("  FAIL: version's document_id=%v has no matching doc\n", docID)
				ok = false
			}
		}
	}

	if ok {
		fmt.Println("  PASS: post-commit SIGKILL preserved both collections + FK link")
		return true
	}
	fmt.Println("  FAIL: post-commit SIGKILL lost or corrupted committed tx")
	return false
}

// Scenario C: SIGTERM (graceful shutdown) mid-tx, no commit. The
// graceful-shutdown handler flushes the lazy-sync backlog and the
// btree pages — but the in-tx writes were never marked committed in
// the tx_log, so on restart they're still discarded by replay. The
// difference vs. Scenario A is that here the engine had a clean
// chance to drain everything before exiting; the test verifies that
// "clean shutdown" doesn't accidentally promote uncommitted tx
// writes.
func runScenarioC(bin string) bool {
	tmpDir, port := setup("atomicity-C")
	defer os.RemoveAll(tmpDir)

	fmt.Println("Scenario C: SIGTERM (graceful) mid-tx → expect 0 rows in each collection")
	srv := startServer(bin, tmpDir, port)
	waitReady(port)

	c, err := oxidb.Connect("127.0.0.1", port, 5*time.Second)
	if err != nil {
		killAndWait(srv)
		fmt.Printf("  FAIL: connect: %v\n", err)
		return false
	}
	if err := c.CreateCollection(docColl); err != nil {
		killAndWait(srv)
		fmt.Printf("  FAIL: create %s: %v\n", docColl, err)
		return false
	}
	if err := c.CreateCollection(versColl); err != nil {
		killAndWait(srv)
		fmt.Printf("  FAIL: create %s: %v\n", versColl, err)
		return false
	}

	if _, err := c.BeginTx(); err != nil {
		killAndWait(srv)
		fmt.Printf("  FAIL: begin_tx: %v\n", err)
		return false
	}
	for i := 0; i < pairs; i++ {
		docResp, err := c.Insert(docColl, map[string]any{"seq": i, "title": fmt.Sprintf("doc-%d", i)})
		if err != nil {
			killAndWait(srv)
			fmt.Printf("  FAIL: tx insert doc[%d]: %v\n", i, err)
			return false
		}
		docID := docResp["id"]
		_, err = c.Insert(versColl, map[string]any{"seq": i, "document_id": docID, "version_no": 1})
		if err != nil {
			killAndWait(srv)
			fmt.Printf("  FAIL: tx insert ver[%d]: %v\n", i, err)
			return false
		}
	}
	c.Close() // drop the conn — server's tx state goes with it

	// SIGTERM (graceful), not SIGKILL. The shutdown handler should
	// flush + exit cleanly without preserving the abandoned tx.
	fmt.Printf("  buffered %d×2 inserts; SIGTERM (graceful)...\n", pairs)
	if err := srv.Process.Signal(syscall.SIGTERM); err != nil {
		fmt.Printf("  FAIL: SIGTERM: %v\n", err)
		return false
	}
	state, err := srv.Process.Wait()
	if err != nil {
		fmt.Printf("  FAIL: wait after SIGTERM: %v\n", err)
		return false
	}
	if state.ExitCode() != 0 {
		fmt.Printf("  WARN: graceful shutdown exit code %d (expected 0)\n", state.ExitCode())
		// not a hard fail — atomicity is what matters here
	}

	srv2 := startServer(bin, tmpDir, port)
	defer func() { _ = srv2.Process.Signal(syscall.SIGTERM); srv2.Wait() }()
	waitReady(port)

	c2, err := oxidb.Connect("127.0.0.1", port, 5*time.Second)
	if err != nil {
		fmt.Printf("  FAIL: post-restart connect: %v\n", err)
		return false
	}
	defer c2.Close()

	docCount, _ := c2.Count(docColl, map[string]any{})
	verCount, _ := c2.Count(versColl, map[string]any{})
	fmt.Printf("  recovered: docs=%d versions=%d (expected 0/0)\n", docCount, verCount)

	if docCount == 0 && verCount == 0 {
		fmt.Println("  PASS: graceful shutdown didn't promote uncommitted tx writes")
		return true
	}
	fmt.Println("  FAIL: graceful shutdown leaked uncommitted tx writes — atomicity broken")
	return false
}

// ── helpers ────────────────────────────────────────────────────────

func setup(prefix string) (string, int) {
	tmpDir, err := os.MkdirTemp("", "oxidb-"+prefix+"-*")
	if err != nil {
		fatalf("MkdirTemp: %v", err)
	}
	return tmpDir, pickFreePort()
}

func startServer(bin, dataDir string, port int) *exec.Cmd {
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(),
		"OXIDB_ADDR="+fmt.Sprintf("127.0.0.1:%d", port),
		"OXIDB_DATA="+dataDir,
		"OXIDB_LAZY_SYNC=false",
		"OXIDB_IDLE_TIMEOUT=60",
		"OXIDB_POOL_SIZE=8",
		"OXIDB_S3_PORT=0",
	)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		fatalf("start oxidb-server: %v", err)
	}
	return cmd
}

func waitReady(port int) {
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	fatalf("oxidb-server didn't become ready on :%d within 10s", port)
}

func killAndWait(cmd *exec.Cmd) {
	_ = cmd.Process.Signal(syscall.SIGKILL)
	_, _ = cmd.Process.Wait()
}

func pickFreePort() int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fatalf("listen :0: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func envOr(k, dflt string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return dflt
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "FATAL: "+format+"\n", args...)
	os.Exit(2)
}
