// pitr-go — end-to-end Point-In-Time Recovery test for OxiDB.
//
// The headline scenario: take a base backup, do more work, then a
// destructive bulk delete — and prove `restore_to_point` can rewind the
// database to the moment just before the delete, with every document
// back and the delete undone.
//
// Flow:
//
//  1. Boot oxidb-server with OXIDB_PITR=true and small WAL segments, so
//     segments seal and reach the archive during the run.
//  2. Insert batch A (N docs), take a base backup.
//  3. Insert batch B (N more docs) — these go WAL -> seal -> archive.
//  4. Record `goodMicros`; sleep; then a "bad" bulk delete wipes all 2N.
//  5. SIGTERM the server: the graceful shutdown seals every live WAL tail
//     and runs a final archive pass, so the archive holds the full
//     history. (SIGTERM, not SIGKILL: we WANT the clean-shutdown flush.)
//  6. Restart a server and call restore_to_point(base, archive, restored,
//     at_micros = goodMicros).
//  7. Boot a server over the restored database and assert all 2N docs
//     are back and the bulk delete did not happen — batch A came from
//     the base, batch B was replayed from the archive.
//
// The server is multi-database: `OXIDB_DATA` is the manager root and each
// subdirectory is a database; the default one is "oxidb". So a per-
// database backup/restore lands in `<root>/oxidb/`.
//
// Scope note: this proves the PITR pipeline end to end — archive, base
// watermark, timestamp resolution, replay, and the destructive-op
// rewind. It does NOT prove fsync durability under power loss (SIGTERM
// on a healthy machine preserves the page cache); that needs block-layer
// fault injection, tracked separately.
//
// Tunables via env:
//
//	OXIDB_BIN     path to oxidb-server (default: ../../target/release/oxidb-server)
//	N             docs per batch (default 1000; the test inserts 2*N total)
//	KEEP_TMPDIR   if set, don't rm -rf the work dir on exit (debugging)
package main

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/parisxmas/OxiDB/clients/go/oxidb"
)

const (
	collection = "docs"
	// The server's default database — `OXIDB_DATA` is the manager root,
	// and the native client lands in `<root>/oxidb/` without a use_db.
	defaultDB = "oxidb"
)

// Every server we spawn, so the exit path can guarantee none are
// orphaned — an orphaned server keeps stdio pipes open and wedges
// whatever is reading the test's output.
var allServers []*exec.Cmd

func main() {
	os.Exit(run())
}

func run() int {
	bin := envOr("OXIDB_BIN", "../../target/release/oxidb-server")
	if !filepath.IsAbs(bin) {
		if abs, err := filepath.Abs(bin); err == nil {
			bin = abs
		}
	}
	if _, err := os.Stat(bin); err != nil {
		return fail("oxidb-server binary not found at %s — set OXIDB_BIN or build it (cargo build --release -p oxidb-server)", bin)
	}
	n := atoiOr(os.Getenv("N"), 1000)
	total := 2 * n

	root, err := os.MkdirTemp("", "oxidb-pitr-test-*")
	if err != nil {
		return fail("MkdirTemp: %v", err)
	}
	if os.Getenv("KEEP_TMPDIR") == "" {
		defer os.RemoveAll(root)
	} else {
		fmt.Printf("(keeping work dir: %s)\n", root)
	}
	// killAllServers runs on every return path — including the failure
	// ones — so no server is left holding the test's output pipe.
	defer killAllServers()

	dataDir := filepath.Join(root, "data")
	baseBackup := filepath.Join(root, "base.tar.gz")
	// The collections / archive live one level down, under the database.
	archiveDir := filepath.Join(dataDir, defaultDB, "_archive")
	verifyRoot := filepath.Join(root, "verify")           // a fresh manager root
	restoredDB := filepath.Join(verifyRoot, defaultDB)     // the restored database
	port := pickFreePort()

	fmt.Printf("=== OxiDB Point-In-Time Recovery test ===\n")
	fmt.Printf("  binary  : %s\n", bin)
	fmt.Printf("  data dir: %s\n", dataDir)
	fmt.Printf("  port    : %d\n", port)
	fmt.Printf("  docs    : 2 batches of %d (= %d total)\n\n", n, total)

	// ── Phase 1: boot with PITR, insert batch A, take a base backup ──
	srv := startServer(bin, dataDir, port, true)
	waitReady(port)

	client, err := oxidb.Connect("127.0.0.1", port, 5*time.Second)
	if err != nil {
		return fail("connect: %v", err)
	}
	if err := client.CreateCollection(collection); err != nil {
		client.Close()
		return fail("create collection: %v", err)
	}

	fmt.Printf("Phase 1: inserting batch A (%d docs), then a base backup...\n", n)
	if err := insertRange(client, 0, n); err != nil {
		client.Close()
		return fail("%v", err)
	}
	if _, err := client.Backup(baseBackup); err != nil {
		client.Close()
		return fail("backup: %v", err)
	}
	fmt.Printf("  base backup written — %d docs captured in the base\n\n", n)

	// ── Phase 2: insert batch B, mark the good point, bad delete ─────
	fmt.Printf("Phase 2: inserting batch B (%d docs)...\n", n)
	if err := insertRange(client, n, total); err != nil {
		client.Close()
		return fail("%v", err)
	}

	goodMicros := time.Now().UnixMicro()
	time.Sleep(250 * time.Millisecond) // separate the good point from the delete
	fmt.Printf("  good point recorded: %d (all %d docs present here)\n", goodMicros, total)

	fmt.Println("Phase 2: BAD bulk delete — wiping every document...")
	if _, err := client.Delete(collection, map[string]any{}); err != nil {
		client.Close()
		return fail("bulk delete: %v", err)
	}
	if cnt, err := client.Count(collection, map[string]any{}); err != nil || cnt != 0 {
		client.Close()
		return fail("post-delete count = %d, err = %v (expected 0)", cnt, err)
	}
	client.Close()

	// SIGTERM — the graceful shutdown seals every live WAL tail and runs
	// a final archive pass, so the archive ends up holding the full
	// history. (SIGKILL would skip that flush; here we want it.)
	fmt.Println("Phase 2: SIGTERM — graceful shutdown seals + archives the tail...")
	if err := srv.Process.Signal(syscall.SIGTERM); err != nil {
		return fail("SIGTERM: %v", err)
	}
	_ = srv.Wait()
	fmt.Println()

	// ── Phase 3: restore_to_point, back to just before the delete ────
	fmt.Println("Phase 3: restore_to_point to the good point...")
	srv2 := startServer(bin, dataDir, port, false)
	waitReady(port)
	admin, err := oxidb.Connect("127.0.0.1", port, 5*time.Second)
	if err != nil {
		return fail("reconnect: %v", err)
	}

	status, err := admin.ArchiveStatus()
	if err != nil {
		admin.Close()
		return fail("archive_status: %v", err)
	}
	fmt.Printf("  archive: %v segment(s), max_gsn %v\n", status["segment_count"], status["max_gsn"])

	info, err := admin.RestoreToPoint(baseBackup, archiveDir, restoredDB, map[string]any{
		"at_micros": goodMicros,
	})
	if err != nil {
		admin.Close()
		return fail("restore_to_point: %v", err)
	}
	fmt.Printf("  restored to GSN %v, %v record(s) applied -> %s\n",
		info["target_gsn"], info["records_applied"], restoredDB)
	admin.Close()
	_ = srv2.Process.Signal(syscall.SIGTERM)
	_ = srv2.Wait()
	fmt.Println()

	// ── Phase 4: verify the restored database ────────────────────────
	fmt.Println("Phase 4: opening the restored database...")
	startServer(bin, verifyRoot, port, false)
	waitReady(port)
	verify, err := oxidb.Connect("127.0.0.1", port, 5*time.Second)
	if err != nil {
		return fail("verify connect: %v", err)
	}
	defer verify.Close()

	failed := 0

	// Assertion 1: every document is back — the bad delete was rewound.
	count, err := verify.Count(collection, map[string]any{})
	if err != nil {
		return fail("count: %v", err)
	}
	fmt.Printf("  recovered count: %d (expected %d)\n", count, total)
	if count != total {
		failed++
		fmt.Printf("  FAIL: lost %d doc(s) — the bulk delete was not fully rewound\n", total-count)
	} else {
		fmt.Println("  PASS: all docs recovered — the bulk delete was rewound")
	}

	// Assertion 2: spot-check docs from batch A (came from the base) and
	// batch B (replayed from the archive) by their seq number.
	probes := []int{0, n - 1, n, total - 1}
	probeFails := 0
	for _, seq := range probes {
		docs, err := verify.Find(collection, map[string]any{"seq": seq}, nil)
		if err != nil || len(docs) != 1 {
			probeFails++
			fmt.Printf("  FAIL: find seq=%d returned %d doc(s), err=%v\n", seq, len(docs), err)
			continue
		}
		want := fmt.Sprintf("doc-%06d", seq)
		if got, _ := docs[0]["payload"].(string); got != want {
			probeFails++
			fmt.Printf("  FAIL: seq=%d payload=%q (expected %q)\n", seq, got, want)
		}
	}
	if probeFails == 0 {
		fmt.Printf("  PASS: spot-check %v all present and intact (base + archive-replayed)\n", probes)
	} else {
		failed++
	}

	fmt.Println()
	if failed > 0 {
		fmt.Printf("=== PITR: %d FAILURE(S) ===\n", failed)
		return 1
	}
	fmt.Println("=== PITR: OK ===")
	return 0
}

// insertRange inserts docs with `seq` in [lo, hi).
func insertRange(client *oxidb.Client, lo, hi int) error {
	for i := lo; i < hi; i++ {
		if _, err := client.Insert(collection, map[string]any{
			"seq":     i,
			"payload": fmt.Sprintf("doc-%06d", i),
		}); err != nil {
			return fmt.Errorf("insert #%d: %w", i, err)
		}
	}
	return nil
}

// startServer spawns oxidb-server pointed at dataDir on the given port.
// With `pitr` set it enables PITR and small WAL segments so the archive
// is actually exercised; otherwise it boots a plain server (used to
// issue admin commands and to verify a restored directory). The handle
// is tracked in allServers so the exit path can reap it. Does NOT wait
// for ready — pair with waitReady.
func startServer(bin, dataDir string, port int, pitr bool) *exec.Cmd {
	env := append(os.Environ(),
		"OXIDB_ADDR="+fmt.Sprintf("127.0.0.1:%d", port),
		"OXIDB_DATA="+dataDir,
		"OXIDB_IDLE_TIMEOUT=60",
		"OXIDB_POOL_SIZE=8",
		// Disable S3 to keep the boot footprint small.
		"OXIDB_S3_PORT=0",
	)
	if pitr {
		env = append(env,
			"OXIDB_PITR=true",
			// Small segments so the WAL rotates and segments reach the
			// archive within the test instead of only at shutdown.
			"OXIDB_WAL_SEGMENT_BYTES=8192",
			"OXIDB_ARCHIVE_INTERVAL=1",
		)
	}
	cmd := exec.Command(bin)
	cmd.Env = env
	cmd.Stdout = os.Stderr // surface server logs to the test output
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: start oxidb-server: %v\n", err)
		killAllServers()
		os.Exit(2)
	}
	allServers = append(allServers, cmd)
	return cmd
}

// killAllServers SIGKILLs and reaps every server we ever started. Safe
// to call on already-exited handles (errors are ignored). Runs on every
// exit path so nothing is left holding the test's stdio pipe.
func killAllServers() {
	for _, s := range allServers {
		if s != nil && s.Process != nil {
			_ = s.Process.Signal(syscall.SIGKILL)
			_, _ = s.Process.Wait()
		}
	}
}

// waitReady polls a TCP connect until the server accepts, up to ~10s.
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
	fmt.Fprintf(os.Stderr, "FATAL: oxidb-server didn't become ready on :%d within 10s\n", port)
	killAllServers()
	os.Exit(2)
}

// pickFreePort opens an ephemeral TCP socket, reads the assigned port,
// and closes it. There's a TOCTOU window before the next bind, but for a
// localhost test that's a non-issue in practice.
func pickFreePort() int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: listen :0: %v\n", err)
		os.Exit(2)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// fail prints a FATAL line and returns exit code 2 — the caller returns
// it up to run()'s return, so the deferred cleanup still runs.
func fail(format string, args ...any) int {
	fmt.Fprintf(os.Stderr, "FATAL: "+format+"\n", args...)
	return 2
}

func envOr(k, dflt string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return dflt
}

func atoiOr(s string, dflt int) int {
	if s == "" {
		return dflt
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return dflt
	}
	return v
}
