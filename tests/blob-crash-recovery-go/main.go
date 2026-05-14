// blob-crash-recovery-go — crash-CONSISTENCY harness for OxiDB's blob store.
//
// Scope, stated honestly:
//
//	This verifies that after the OS hard-kills oxidb-server (SIGKILL),
//	the blob store comes back CONSISTENT on the next boot:
//
//	  - every object whose put was ack'd is present, with intact content
//	  - every object whose delete was ack'd stays deleted (no ghost
//	    resurrection from a leftover .meta)
//	  - an interrupted put leaves no stray .data.tmp / .meta.tmp behind
//	  - every object scan_bucket reports as existing is readable (a
//	    .meta with no .data would list fine but error on get — a ghost)
//	  - the server boots with no manual recovery step
//
//	What it does NOT prove: fsync durability. SIGKILL does not drop the
//	kernel page cache — ack'd-but-unflushed writes survive a process
//	kill regardless of whether fsync ran. Proving the fsync path
//	(OXIDB_BLOB_SYNC) genuinely needs power-loss / block-layer fault
//	injection (e.g. dm-log-writes), which is separate CI infrastructure.
//	The server is still run with OXIDB_BLOB_SYNC=true here so the
//	durable code path is at least exercised under the crash.
//
// Tunables via env:
//
//	OXIDB_BIN     path to oxidb-server (default: ../../target/release/oxidb-server)
//	N             object count (default 500)
//	DELETE_EVERY  delete every Nth object before the crash (default 7)
//	PORT          server port (default: pick a free one)
//	KEEP_TMPDIR   if set, don't rm -rf the data dir on exit (debugging)
package main

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/parisxmas/OxiDB/go/oxidb"
)

const bucket = "crash_blobs"

func payload(seq int) []byte {
	return []byte(fmt.Sprintf("blob-payload-%06d", seq))
}

func main() {
	bin := envOr("OXIDB_BIN", "../../target/release/oxidb-server")
	if !filepath.IsAbs(bin) {
		if abs, err := filepath.Abs(bin); err == nil {
			bin = abs
		}
	}
	if _, err := os.Stat(bin); err != nil {
		fatalf("oxidb-server binary not found at %s — set OXIDB_BIN or build it (cargo build --release -p oxidb-server)", bin)
	}

	n := atoiOr(os.Getenv("N"), 500)
	deleteEvery := atoiOr(os.Getenv("DELETE_EVERY"), 7)
	port := atoiOr(os.Getenv("PORT"), 0)
	if port == 0 {
		port = pickFreePort()
	}

	tmpDir, err := os.MkdirTemp("", "oxidb-blob-crash-test-*")
	if err != nil {
		fatalf("MkdirTemp: %v", err)
	}
	if os.Getenv("KEEP_TMPDIR") == "" {
		defer os.RemoveAll(tmpDir)
	} else {
		fmt.Printf("(keeping tmpdir: %s)\n", tmpDir)
	}

	fmt.Printf("=== OxiDB blob crash-consistency test ===\n")
	fmt.Printf("  binary  : %s\n", bin)
	fmt.Printf("  data dir: %s\n", tmpDir)
	fmt.Printf("  port    : %d\n", port)
	fmt.Printf("  objects : %d (deleting every %dth before crash)\n\n", n, deleteEvery)

	// ── Phase 1: boot, put N, delete a subset, churn, SIGKILL ───────
	srv1 := startServer(bin, tmpDir, port)
	waitReady(port)

	client, err := oxidb.Connect("127.0.0.1", port, 5*time.Second)
	if err != nil {
		killAndWait(srv1)
		fatalf("connect: %v", err)
	}
	if err := client.CreateBucket(bucket); err != nil {
		client.Close()
		killAndWait(srv1)
		fatalf("create bucket: %v", err)
	}

	fmt.Printf("Phase 1: putting %d objects...\n", n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("obj-%06d", i)
		if _, err := client.PutObject(bucket, key, payload(i), "application/octet-stream", nil); err != nil {
			client.Close()
			killAndWait(srv1)
			fatalf("put #%d failed: %v", i, err)
		}
	}

	// Delete a deterministic subset. Each ack'd delete is a committed
	// "this object is gone" — it must not come back after the crash.
	deleted := map[int]bool{}
	for i := 0; i < n; i += deleteEvery {
		key := fmt.Sprintf("obj-%06d", i)
		if err := client.DeleteObject(bucket, key); err != nil {
			client.Close()
			killAndWait(srv1)
			fatalf("delete #%d failed: %v", i, err)
		}
		deleted[i] = true
	}
	fmt.Printf("  %d puts ack'd, %d deletes ack'd\n", n, len(deleted))
	client.Close()

	// Churn goroutine: keep writing to a second bucket so the SIGKILL
	// lands while a put_object is genuinely mid-flight. This is what
	// exercises the interrupted-write → .tmp cleanup path on recovery.
	started := make(chan struct{}, 1)
	var churnWG sync.WaitGroup
	churnWG.Add(1)
	go func() {
		defer churnWG.Done()
		cc, err := oxidb.Connect("127.0.0.1", port, 5*time.Second)
		if err != nil {
			return
		}
		defer cc.Close()
		_ = cc.CreateBucket("churn")
		for i := 0; ; i++ {
			key := fmt.Sprintf("churn-%08d", i)
			if _, err := cc.PutObject("churn", key, payload(i), "application/octet-stream", nil); err != nil {
				return // server gone
			}
			if i == 0 {
				started <- struct{}{}
			}
		}
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		// Churn never got going; not fatal — the core assertions don't depend on it.
	}
	time.Sleep(150 * time.Millisecond) // let churn build up in-flight writes

	fmt.Println("Phase 1: SIGKILL'ing server mid-churn...")
	if err := srv1.Process.Signal(syscall.SIGKILL); err != nil {
		fatalf("SIGKILL: %v", err)
	}
	_ = srv1.Wait() // reap
	churnWG.Wait()
	fmt.Println("  server killed (no graceful shutdown ran)")
	fmt.Println()

	// ── Phase 2: cold start same data dir, verify consistency ───────
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

	failed := 0

	// Assertion 1: every ack'd put that wasn't deleted must be present
	// with byte-identical content; every ack'd delete must stay gone.
	missing, resurrected, corrupt := 0, 0, 0
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("obj-%06d", i)
		data, _, err := client2.GetObject(bucket, key)
		if deleted[i] {
			if err == nil {
				resurrected++
			}
			continue
		}
		if err != nil {
			missing++
			continue
		}
		if !bytes.Equal(data, payload(i)) {
			corrupt++
		}
	}
	if missing == 0 && resurrected == 0 && corrupt == 0 {
		fmt.Printf("  PASS: all %d surviving objects intact, all %d deletes held\n", n-len(deleted), len(deleted))
	} else {
		failed++
		fmt.Printf("  FAIL: %d missing, %d resurrected (ghost delete), %d corrupt\n", missing, resurrected, corrupt)
	}

	// Assertion 2: no stray temp files from an interrupted put — those
	// would mean a put_object fell apart between write and rename.
	tmpFiles := findFiles(filepath.Join(tmpDir, "_blobs"), func(name string) bool {
		return strings.HasSuffix(name, ".tmp")
	})
	if len(tmpFiles) == 0 {
		fmt.Println("  PASS: no stray .tmp files under _blobs/")
	} else {
		failed++
		fmt.Printf("  FAIL: found %d stray .tmp file(s):\n", len(tmpFiles))
		for _, f := range tmpFiles {
			fmt.Printf("    %s\n", f)
		}
	}

	// Assertion 3: no ghost reads. Every object the store lists as
	// existing — in either bucket — must actually be readable. A .meta
	// with no .data would list fine but error on get.
	ghosts := 0
	for _, b := range []string{bucket, "churn"} {
		objs, err := client2.ListObjects(b, nil, nil)
		if err != nil {
			continue // "churn" may not exist if the goroutine never ran — tolerate it
		}
		for _, o := range objs {
			key, _ := o["key"].(string)
			if key == "" {
				continue
			}
			if _, _, err := client2.GetObject(b, key); err != nil {
				ghosts++
				fmt.Printf("  FAIL: listed object %s/%s is not readable: %v\n", b, key, err)
			}
		}
	}
	if ghosts == 0 {
		fmt.Println("  PASS: every listed object is readable (no ghost .meta entries)")
	} else {
		failed++
	}

	fmt.Println()
	if failed > 0 {
		fmt.Printf("=== BLOB CRASH CONSISTENCY: %d FAILURE(S) ===\n", failed)
		os.Exit(1)
	}
	fmt.Println("=== BLOB CRASH CONSISTENCY: OK ===")
}

// startServer spawns oxidb-server pointed at dataDir on the given port.
// It does NOT wait for ready — pair with waitReady.
func startServer(bin, dataDir string, port int) *exec.Cmd {
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(),
		"OXIDB_ADDR="+fmt.Sprintf("127.0.0.1:%d", port),
		"OXIDB_DATA="+dataDir,
		// Exercise the durable blob path. Note: SIGKILL alone can't
		// prove this knob works (the page cache survives a process
		// kill) — but running with it on keeps the durable code path
		// covered by the crash harness.
		"OXIDB_BLOB_SYNC=true",
		"OXIDB_IDLE_TIMEOUT=60",
		"OXIDB_POOL_SIZE=8",
		// Disable S3 to keep the boot footprint small; we drive blobs
		// through the native protocol via the Go client.
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

// findFiles walks the tree under root and returns every file whose
// basename satisfies match.
func findFiles(root string, match func(name string) bool) []string {
	var out []string
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		if match(filepath.Base(path)) {
			out = append(out, path)
		}
		return nil
	})
	return out
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
