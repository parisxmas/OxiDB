// OxiDB's AMQP listener vs real RabbitMQ, measured with the same client
// (rabbitmq/amqp091-go), the same code path, the same scenarios — the only
// variable is the broker behind the socket.
//
// OxiDB is spawned by the benchmark itself (target/debug|release, or
// OXIDB_SERVER_BIN — build --release for honest numbers). RabbitMQ is
// expected to be running already (default amqp://guest:guest@127.0.0.1:5672/,
// override with -rabbit); if it is not reachable, its column reads "n/a".
//
// Scenarios:
//   publish confirms, pipelined   — N transient msgs, confirm mode, publish
//                                   all then drain acks: broker ack throughput
//   publish confirm, sequential   — per-message Wait: confirm round-trip
//                                   latency p50/p99
//   durable publish, pipelined    — durable queue + persistent msgs, confirm
//                                   mode: the write-before-confirm disk path
//   end-to-end throughput         — publisher + autoack consumer on separate
//                                   connections: queue transit rate
//   end-to-end latency            — publish, wait for the delivery: transit
//                                   round-trip p50/p99
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	rabbitURL = flag.String("rabbit", "amqp://guest:guest@127.0.0.1:5672/", "RabbitMQ URL (unreachable = skipped)")
	oxidbURL  = flag.String("oxidb", "", "OxiDB AMQP URL (empty = spawn oxidb-server ourselves)")
	msgs      = flag.Int("msgs", 20000, "messages for throughput scenarios")
	durMsgs   = flag.Int("durable-msgs", 1000, "messages for the durable (disk-bound) scenario")
	latIters  = flag.Int("lat-iters", 300, "iterations for latency scenarios")
	size      = flag.Int("size", 100, "message body size in bytes")
)

type result struct {
	rate float64 // msgs/s (throughput scenarios)
	p50  float64 // ms (latency scenarios)
	p99  float64 // ms
}

type scenario struct {
	name       string
	fn         func(url string) (result, error)
	throughput bool
}

func main() {
	flag.Parse()
	body := make([]byte, *size)
	for i := range body {
		body[i] = byte('a' + i%26)
	}

	scenarios := []scenario{
		{"publish confirms, pipelined", func(u string) (result, error) { return benchPipelined(u, *msgs, body, false) }, true},
		{"publish confirm, sequential", func(u string) (result, error) { return benchSequentialConfirm(u, *latIters, body) }, false},
		{"durable publish, pipelined", func(u string) (result, error) { return benchPipelined(u, *durMsgs, body, true) }, true},
		{"end-to-end throughput", func(u string) (result, error) { return benchE2EThroughput(u, *msgs, body) }, true},
		{"end-to-end latency", func(u string) (result, error) { return benchE2ELatency(u, *latIters, body) }, false},
	}

	// OxiDB: spawn unless a URL was given.
	oxURL := *oxidbURL
	var srv *server
	if oxURL == "" {
		var err error
		srv, err = startServer()
		if err != nil {
			fmt.Fprintf(os.Stderr, "cannot start oxidb-server: %v\n", err)
			os.Exit(1)
		}
		defer srv.stop()
		oxURL = fmt.Sprintf("amqp://127.0.0.1:%d/", srv.port)
	}

	rabbitOK := reachable(*rabbitURL)
	if !rabbitOK {
		fmt.Fprintf(os.Stderr, "note: RabbitMQ not reachable at %s — its column is n/a\n\n", *rabbitURL)
	}

	fmt.Printf("body %dB · throughput over %d msgs (durable %d) · latency over %d iters\n\n",
		*size, *msgs, *durMsgs, *latIters)
	fmt.Printf("%-30s  %18s  %18s  %8s\n", "scenario", "OxiDB", "RabbitMQ", "ratio")

	for _, sc := range scenarios {
		ox, err := sc.fn(oxURL)
		if err != nil {
			fmt.Printf("%-30s  OxiDB FAILED: %v\n", sc.name, err)
			continue
		}
		var cell string
		ratio := ""
		if rabbitOK {
			rb, err := sc.fn(*rabbitURL)
			if err != nil {
				cell = fmt.Sprintf("FAILED: %v", err)
			} else if sc.throughput {
				cell = fmt.Sprintf("%11.0f msg/s", rb.rate)
				ratio = fmt.Sprintf("%.2fx", ox.rate/rb.rate)
			} else {
				cell = fmt.Sprintf("%5.2f/%5.2f ms", rb.p50, rb.p99)
				ratio = fmt.Sprintf("%.2fx", rb.p50/ox.p50)
			}
		} else {
			cell = "n/a"
		}
		if sc.throughput {
			fmt.Printf("%-30s  %11.0f msg/s  %18s  %8s\n", sc.name, ox.rate, cell, ratio)
		} else {
			fmt.Printf("%-30s  %8s p50/p99  %18s  %8s\n", sc.name,
				fmt.Sprintf("%.2f/%.2f ms", ox.p50, ox.p99), cell, ratio)
		}
	}
	fmt.Println("\nratio > 1.00x = OxiDB faster (throughput: rate ratio; latency: p50 ratio)")
}

// ── Scenarios ───────────────────────────────────────────────────────────

// benchPipelined publishes n messages in confirm mode without waiting per
// message, then drains the ack stream — broker-side confirm throughput. With
// durable=true the queue is durable and the messages persistent, so every
// confirm carries the write-before-confirm disk promise.
func benchPipelined(url string, n int, body []byte, durable bool) (result, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return result{}, err
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		return result{}, err
	}
	q := fmt.Sprintf("bench-pipe-%d", time.Now().UnixNano())
	if _, err := ch.QueueDeclare(q, durable, false, !durable /*RabbitMQ 4.x rejects transient non-exclusive queues*/, false, nil); err != nil {
		return result{}, err
	}
	defer ch.QueueDelete(q, false, false, false)
	if err := ch.Confirm(false); err != nil {
		return result{}, err
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, n))

	pub := amqp.Publishing{Body: body}
	if durable {
		pub.DeliveryMode = amqp.Persistent
	}
	ctx := context.Background()
	start := time.Now()
	for i := 0; i < n; i++ {
		if err := ch.PublishWithContext(ctx, "", q, false, false, pub); err != nil {
			return result{}, err
		}
	}
	for i := 0; i < n; i++ {
		select {
		case c := <-confirms:
			if !c.Ack {
				return result{}, fmt.Errorf("broker nacked message %d", c.DeliveryTag)
			}
		case <-time.After(120 * time.Second):
			return result{}, fmt.Errorf("confirm stream stalled at %d/%d", i, n)
		}
	}
	elapsed := time.Since(start)
	return result{rate: float64(n) / elapsed.Seconds()}, nil
}

// benchSequentialConfirm waits for each publish's confirm before sending the
// next — the per-message confirm round trip an RPC-style producer sees.
func benchSequentialConfirm(url string, iters int, body []byte) (result, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return result{}, err
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		return result{}, err
	}
	q := fmt.Sprintf("bench-seq-%d", time.Now().UnixNano())
	if _, err := ch.QueueDeclare(q, false, false, true /*RabbitMQ 4.x rejects transient non-exclusive queues*/, false, nil); err != nil {
		return result{}, err
	}
	defer ch.QueueDelete(q, false, false, false)
	if err := ch.Confirm(false); err != nil {
		return result{}, err
	}

	lat := make([]time.Duration, 0, iters)
	ctx := context.Background()
	for i := 0; i < iters; i++ {
		t0 := time.Now()
		dc, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", q, false, false, amqp.Publishing{Body: body})
		if err != nil {
			return result{}, err
		}
		wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		acked, err := dc.WaitContext(wctx)
		cancel()
		if err != nil || !acked {
			return result{}, fmt.Errorf("confirm %d failed: %v", i, err)
		}
		lat = append(lat, time.Since(t0))
	}
	p50, p99 := percentiles(lat)
	return result{p50: p50, p99: p99}, nil
}

// benchE2EThroughput measures queue transit: publisher on one connection,
// autoack consumer on another, clock stops when the last message arrives.
func benchE2EThroughput(url string, n int, body []byte) (result, error) {
	q := fmt.Sprintf("bench-e2e-%d", time.Now().UnixNano())

	cconn, err := amqp.Dial(url)
	if err != nil {
		return result{}, err
	}
	defer cconn.Close()
	cch, err := cconn.Channel()
	if err != nil {
		return result{}, err
	}
	if _, err := cch.QueueDeclare(q, false, false, true /*RabbitMQ 4.x rejects transient non-exclusive queues*/, false, nil); err != nil {
		return result{}, err
	}
	defer cch.QueueDelete(q, false, false, false)
	deliveries, err := cch.Consume(q, "", true /*autoAck*/, false, false, false, nil)
	if err != nil {
		return result{}, err
	}
	done := make(chan struct{})
	go func() {
		for i := 0; i < n; i++ {
			<-deliveries
		}
		close(done)
	}()

	pconn, err := amqp.Dial(url)
	if err != nil {
		return result{}, err
	}
	defer pconn.Close()
	pch, err := pconn.Channel()
	if err != nil {
		return result{}, err
	}

	ctx := context.Background()
	start := time.Now()
	for i := 0; i < n; i++ {
		if err := pch.PublishWithContext(ctx, "", q, false, false, amqp.Publishing{Body: body}); err != nil {
			return result{}, err
		}
	}
	select {
	case <-done:
	case <-time.After(120 * time.Second):
		return result{}, fmt.Errorf("consumer never received all %d messages", n)
	}
	elapsed := time.Since(start)
	return result{rate: float64(n) / elapsed.Seconds()}, nil
}

// benchE2ELatency publishes one message and waits for its delivery, each
// iteration — the transit round trip a request/worker pair sees.
func benchE2ELatency(url string, iters int, body []byte) (result, error) {
	q := fmt.Sprintf("bench-lat-%d", time.Now().UnixNano())

	cconn, err := amqp.Dial(url)
	if err != nil {
		return result{}, err
	}
	defer cconn.Close()
	cch, err := cconn.Channel()
	if err != nil {
		return result{}, err
	}
	if _, err := cch.QueueDeclare(q, false, false, true /*RabbitMQ 4.x rejects transient non-exclusive queues*/, false, nil); err != nil {
		return result{}, err
	}
	defer cch.QueueDelete(q, false, false, false)
	deliveries, err := cch.Consume(q, "", true, false, false, false, nil)
	if err != nil {
		return result{}, err
	}

	pconn, err := amqp.Dial(url)
	if err != nil {
		return result{}, err
	}
	defer pconn.Close()
	pch, err := pconn.Channel()
	if err != nil {
		return result{}, err
	}

	ctx := context.Background()
	lat := make([]time.Duration, 0, iters)
	for i := 0; i < iters; i++ {
		t0 := time.Now()
		if err := pch.PublishWithContext(ctx, "", q, false, false, amqp.Publishing{Body: body}); err != nil {
			return result{}, err
		}
		select {
		case <-deliveries:
			lat = append(lat, time.Since(t0))
		case <-time.After(30 * time.Second):
			return result{}, fmt.Errorf("delivery %d never arrived", i)
		}
	}
	p50, p99 := percentiles(lat)
	return result{p50: p50, p99: p99}, nil
}

// ── Helpers ─────────────────────────────────────────────────────────────

func percentiles(lat []time.Duration) (p50, p99 float64) {
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	ms := func(d time.Duration) float64 { return float64(d.Microseconds()) / 1000.0 }
	return ms(lat[len(lat)/2]), ms(lat[len(lat)*99/100])
}

func reachable(url string) bool {
	conn, err := amqp.Dial(url)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

type server struct {
	port int
	data string
	proc *exec.Cmd
}

func startServer() (*server, error) {
	bin, err := findServerBinary()
	if err != nil {
		return nil, err
	}
	dataDir, err := os.MkdirTemp("", "oxidb-amqp-bench")
	if err != nil {
		return nil, err
	}
	port, err := freePort()
	if err != nil {
		return nil, err
	}
	docPort, err := freePort()
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("OXIDB_AMQP_PORT=%d", port),
		fmt.Sprintf("OXIDB_ADDR=127.0.0.1:%d", docPort),
		"OXIDB_DATA="+dataDir,
	)
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	srv := &server{port: port, data: dataDir, proc: cmd}
	deadline := time.Now().Add(30 * time.Second)
	for {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Second)
		if err == nil {
			c.Close()
			return srv, nil
		}
		if time.Now().After(deadline) {
			srv.stop()
			return nil, fmt.Errorf("AMQP listener never came up on %d", port)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (s *server) stop() {
	_ = s.proc.Process.Kill()
	_, _ = s.proc.Process.Wait()
	os.RemoveAll(s.data)
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func findServerBinary() (string, error) {
	if env := os.Getenv("OXIDB_SERVER_BIN"); env != "" {
		if _, err := os.Stat(env); err == nil {
			return env, nil
		}
	}
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		// Prefer release for honest numbers; fall back to debug.
		for _, profile := range []string{"release", "debug"} {
			p := filepath.Join(dir, "target", profile, "oxidb-server")
			if _, err := os.Stat(p); err == nil {
				return p, nil
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("oxidb-server binary not found — `cargo build --release -p oxidb-server` first, or set OXIDB_SERVER_BIN")
		}
		dir = parent
	}
}
