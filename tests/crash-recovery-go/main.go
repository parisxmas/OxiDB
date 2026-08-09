// crash-recovery-go — durability regression harness for OxiDB.
//
// What it tests, in plain words:
//
//   "If the OS hard-kills oxidb-server (SIGKILL — power loss, OOM kill,
//   panic in a non-engine thread), every insert that the server already
//   acknowledged must still be present after the next clean boot, and
//   the server must boot cleanly without any manual recovery step."
//
// The flow:
//
//   1. Spin up oxidb-server in a tempdir on a free port. Wait for ready.
//   2. Connect a client, create collection, insert N documents in serial.
//      Each successful Insert is treated as a committed write — the
//      durability promise is that all of them survive.
//   3. SIGKILL the server. (Not SIGTERM — that triggers our graceful
//      shutdown which would flush pending writes; we want to test the
//      hostile case where no shutdown hook gets to run.)
//   4. Restart oxidb-server on the same data dir. Boot must succeed.
//   5. Reconnect, count documents. Must equal N exactly. Anything
//      less means we lost an ack'd write — that's the bug we're
//      regressing against.
//   6. Sanity check the data dir for stray .btree.tmp files; one would
//      indicate a partial atomic-rename, which our persist() should
//      have prevented.
//
// Tunables via env:
//
//   OXIDB_BIN     path to oxidb-server (default: ../../target/release/oxidb-server)
//   N             insert count (default 2000)
//   PORT          server port (default: pick a free one)
//   KEEP_TMPDIR   if set, don't rm -rf the data dir on exit (debugging)
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
	collection = "crash_test"
)

func main() {
	bin := envOr("OXIDB_BIN", "../../target/release/oxidb-server")
	if !filepath.IsAbs(bin) {
		// Resolve relative to the test dir for sane behavior under run.sh.
		abs, err := filepath.Abs(bin)
		if err == nil {
			bin = abs
		}
	}
	if _, err := os.Stat(bin); err != nil {
		fatalf("oxidb-server binary not found at %s — set OXIDB_BIN or build it (cargo build --release -p oxidb-server)", bin)
	}

	n := atoiOr(os.Getenv("N"), 2000)
	port := atoiOr(os.Getenv("PORT"), 0)
	if port == 0 {
		port = pickFreePort()
	}

	tmpDir, err := os.MkdirTemp("", "oxidb-crash-test-*")
	if err != nil {
		fatalf("MkdirTemp: %v", err)
	}
	if os.Getenv("KEEP_TMPDIR") == "" {
		defer os.RemoveAll(tmpDir)
	} else {
		fmt.Printf("(keeping tmpdir: %s)\n", tmpDir)
	}

	fmt.Printf("=== OxiDB crash recovery test ===\n")
	fmt.Printf("  binary  : %s\n", bin)
	fmt.Printf("  data dir: %s\n", tmpDir)
	fmt.Printf("  port    : %d\n", port)
	fmt.Printf("  inserts : %d\n\n", n)

	// ── Phase 1: boot, insert N, SIGKILL ────────────────────────────
	srv1 := startServer(bin, tmpDir, port)
	waitReady(port)

	client, err := oxidb.Connect("127.0.0.1", port, 5*time.Second)
	if err != nil {
		killAndWait(srv1)
		fatalf("connect: %v", err)
	}
	if err := client.CreateCollection(collection); err != nil {
		killAndWait(srv1)
		fatalf("create collection: %v", err)
	}

	fmt.Printf("Phase 1: inserting %d documents...\n", n)
	t0 := time.Now()
	for i := 0; i < n; i++ {
		_, err := client.Insert(collection, map[string]any{
			"seq":     i,
			"payload": fmt.Sprintf("doc-%06d", i),
		})
		if err != nil {
			client.Close()
			killAndWait(srv1)
			fatalf("insert #%d failed: %v", i, err)
		}
	}
	insertDuration := time.Since(t0)
	fmt.Printf("  %d acks in %v (%.0f ops/s)\n",
		n, insertDuration.Round(time.Millisecond),
		float64(n)/insertDuration.Seconds())
	client.Close()

	// SIGKILL — no graceful shutdown, no flush hook, no chance for
	// the engine to clean up. Same brutality as `kill -9` or a power cut.
	fmt.Println("Phase 1: SIGKILL'ing server...")
	if err := srv1.Process.Signal(syscall.SIGKILL); err != nil {
		fatalf("SIGKILL: %v", err)
	}
	_ = srv1.Wait() // reap
	fmt.Println("  server killed (no graceful shutdown ran)")
	fmt.Println()

	// ── Phase 2: cold start same data dir, verify survival ──────────
	fmt.Println("Phase 2: cold start on same data dir...")
	srv2 := startServer(bin, tmpDir, port)
	defer func() { _ = srv2.Process.Signal(syscall.SIGTERM); srv2.Wait() }()
	waitReady(port)
	fmt.Println("  boot succeeded")

	client2, err := oxidb.Connect("127.0.0.1", port, 5*time.Second)
	if err != nil {
		fatalf("post-restart connect: %v", err)
	}
	defer client2.Close()

	count, err := client2.Count(collection, map[string]any{})
	if err != nil {
		fatalf("count: %v", err)
	}
	fmt.Printf("  recovered count: %d (expected %d)\n", count, n)

	failed := 0

	// Assertion 1: every committed write must survive.
	if int(count) != n {
		failed++
		fmt.Printf("  FAIL: lost %d ack'd write(s) across crash\n", n-int(count))
	} else {
		fmt.Println("  PASS: all ack'd writes survived")
	}

	// Assertion 2: no atomic-rename leftovers — those would mean a
	// persist() fell apart between tmp-write and rename.
	tmpFiles := globAll(filepath.Join(tmpDir, "**", "*.btree.tmp"))
	if len(tmpFiles) > 0 {
		failed++
		fmt.Printf("  FAIL: found %d stray .btree.tmp file(s):\n", len(tmpFiles))
		for _, f := range tmpFiles {
			fmt.Printf("    %s\n", f)
		}
	} else {
		fmt.Println("  PASS: no stray .btree.tmp files in data dir")
	}

	// Assertion 3: spot-check a few specific docs by their seq number,
	// not just count — guards against a bug where N docs are present
	// but with corrupted contents.
	probes := []int{0, n / 2, n - 1}
	for _, seq := range probes {
		docs, err := client2.Find(collection, map[string]any{"seq": seq}, nil)
		if err != nil {
			failed++
			fmt.Printf("  FAIL: find seq=%d: %v\n", seq, err)
			continue
		}
		if len(docs) != 1 {
			failed++
			fmt.Printf("  FAIL: find seq=%d returned %d docs (expected 1)\n", seq, len(docs))
			continue
		}
		want := fmt.Sprintf("doc-%06d", seq)
		got, _ := docs[0]["payload"].(string)
		if got != want {
			failed++
			fmt.Printf("  FAIL: seq=%d payload=%q (expected %q)\n", seq, got, want)
		}
	}
	if failed == 0 {
		fmt.Printf("  PASS: spot-check %v all match\n", probes)
	}

	fmt.Println()
	if failed > 0 {
		fmt.Printf("=== CRASH RECOVERY: %d FAILURE(S) ===\n", failed)
		os.Exit(1)
	}
	fmt.Println("=== CRASH RECOVERY: OK ===")
}

// startServer spawns oxidb-server pointed at dataDir on the given
// port. It does NOT wait for ready — pair with waitReady.
func startServer(bin, dataDir string, port int) *exec.Cmd {
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(),
		"OXIDB_ADDR="+fmt.Sprintf("127.0.0.1:%d", port),
		"OXIDB_DATA="+dataDir,
		// Strict ACID-D — what we're actually testing. If someone
		// flips OXIDB_LAZY_SYNC=true via the environment this test
		// would let acked-but-unflushed writes evaporate, which is
		// the documented behavior of lazy mode.
		"OXIDB_LAZY_SYNC=false",
		"OXIDB_IDLE_TIMEOUT=60",
		"OXIDB_POOL_SIZE=8",
		// Disable S3 to keep the boot footprint small.
		"OXIDB_S3_PORT=0",
	)
	cmd.Stdout = os.Stderr // surface server logs to the test output
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		fatalf("start oxidb-server: %v", err)
	}
	return cmd
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
	fatalf("oxidb-server didn't become ready on :%d within 10s", port)
}

func killAndWait(cmd *exec.Cmd) {
	_ = cmd.Process.Signal(syscall.SIGKILL)
	_, _ = cmd.Process.Wait()
}

// pickFreePort opens an ephemeral TCP socket, reads the assigned port,
// and closes it. There's a TOCTOU window between close and the next
// bind, but for a localhost test that's a non-issue in practice.
func pickFreePort() int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fatalf("listen :0: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// globAll walks the dir tree under root looking for files matching
// the basename of `pattern` (the standard library's glob doesn't
// recurse). Cheap to do manually since data dirs are small.
func globAll(pattern string) []string {
	root, base := filepath.Split(pattern)
	root = filepath.Clean(root)
	// Pattern was "<root>/**/*.btree.tmp" — strip the "**/" sentinel.
	base = filepath.Base(base)
	var matches []string
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		ok, _ := filepath.Match(base, filepath.Base(path))
		if ok {
			matches = append(matches, path)
		}
		return nil
	})
	return matches
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

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "FATAL: "+format+"\n", args...)
	os.Exit(2)
}
