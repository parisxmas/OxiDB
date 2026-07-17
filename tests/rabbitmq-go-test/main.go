// The official RabbitMQ Go client (rabbitmq/amqp091-go, the maintained
// successor of streadway/amqp), unmodified, against OxiDB's AMQP listener —
// ADR-0016's claim from the Go ecosystem (pika covers Python, RabbitMQ.Client
// covers .NET in tests/rabbitmq-dotnet-test).
//
// Self-contained: spawns its own oxidb-server (target/debug, or
// OXIDB_SERVER_BIN), runs every scenario, exits nonzero on any failure. The
// durability scenario kills the server hard (SIGKILL, no graceful shutdown)
// and restarts it — the publisher confirm is the only promise being tested.
package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	tests := []struct {
		name string
		fn   func() error
	}{
		{"hello world roundtrip with publisher confirms", helloWorld},
		{"competing consumers split the work exactly once", competingConsumers},
		{"prefetch caps a slow consumer, the rest flows on", prefetch},
		{"topic exchange routes on wildcards", topicExchange},
		{"fanout copies to every bound queue", fanout},
		{"mandatory unroutable publish comes back as Basic.Return", mandatoryReturn},
		{"nack with requeue redelivers, flagged", nackRequeue},
		{"durable persistent messages survive a hard kill", durableSurvivesKill},
	}
	failed := 0
	for _, t := range tests {
		if err := t.fn(); err != nil {
			failed++
			fmt.Printf("FAIL  %s\n      %v\n", t.name, err)
		} else {
			fmt.Printf("PASS  %s\n", t.name)
		}
	}
	fmt.Printf("\n%d passed, %d failed\n", len(tests)-failed, failed)
	if failed > 0 {
		os.Exit(1)
	}
}

// ── Scenarios ───────────────────────────────────────────────────────────

func helloWorld() error {
	srv, err := startServer()
	if err != nil {
		return err
	}
	defer srv.stop()

	ch, cleanup, err := srv.channel(true)
	if err != nil {
		return err
	}
	defer cleanup()

	if _, err := ch.QueueDeclare("hello", false, false, false, false, nil); err != nil {
		return err
	}
	if err := publishConfirmed(ch, "", "hello", amqp.Publishing{Body: []byte("Hello OxiDB!")}); err != nil {
		return err
	}
	msg, ok, err := ch.Get("hello", true)
	if err != nil || !ok {
		return fmt.Errorf("queue was empty after a confirmed publish: %v", err)
	}
	if string(msg.Body) != "Hello OxiDB!" {
		return fmt.Errorf("body mismatch: %q", msg.Body)
	}
	if _, ok, _ := ch.Get("hello", true); ok {
		return fmt.Errorf("queue must be drained")
	}
	return nil
}

func competingConsumers() error {
	srv, err := startServer()
	if err != nil {
		return err
	}
	defer srv.stop()

	got1, stop1, err := srv.ackingConsumer("work", true)
	if err != nil {
		return err
	}
	defer stop1()
	got2, stop2, err := srv.ackingConsumer("work", false)
	if err != nil {
		return err
	}
	defer stop2()

	pub, cleanup, err := srv.channel(true)
	if err != nil {
		return err
	}
	defer cleanup()
	for i := 0; i < 10; i++ {
		if err := publishConfirmed(pub, "", "work", amqp.Publishing{Body: []byte(fmt.Sprint(i))}); err != nil {
			return err
		}
	}

	if err := until(func() bool { return got1.len()+got2.len() >= 10 }, "10 deliveries"); err != nil {
		return err
	}
	all := append(got1.snapshot(), got2.snapshot()...)
	seen := map[string]bool{}
	for _, b := range all {
		if seen[b] {
			return fmt.Errorf("message %q delivered twice", b)
		}
		seen[b] = true
	}
	if got1.len() != 5 || got2.len() != 5 {
		return fmt.Errorf("round-robin must split evenly, got %d/%d", got1.len(), got2.len())
	}
	return nil
}

func prefetch() error {
	srv, err := startServer()
	if err != nil {
		return err
	}
	defer srv.stop()

	// c1 never acks and has prefetch 1: it must hold exactly one delivery
	// while its skipped turns pass to c2 — the work-queue pattern Basic.Qos
	// exists for.
	c1, err := srv.connect()
	if err != nil {
		return err
	}
	defer c1.Close()
	ch1, err := c1.Channel()
	if err != nil {
		return err
	}
	if _, err := ch1.QueueDeclare("work", false, false, false, false, nil); err != nil {
		return err
	}
	if err := ch1.Qos(1, 0, false); err != nil {
		return err
	}
	stuck := &safeList{}
	deliveries, err := ch1.Consume("work", "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	go func() {
		for d := range deliveries {
			stuck.add(string(d.Body)) // no ack, ever
		}
	}()

	flowed, stop2, err := srv.ackingConsumer("work", false)
	if err != nil {
		return err
	}
	defer stop2()

	pub, cleanup, err := srv.channel(true)
	if err != nil {
		return err
	}
	defer cleanup()
	for i := 0; i < 6; i++ {
		if err := publishConfirmed(pub, "", "work", amqp.Publishing{Body: []byte(fmt.Sprint(i))}); err != nil {
			return err
		}
	}

	if err := until(func() bool { return stuck.len()+flowed.len() >= 6 }, "6 deliveries"); err != nil {
		return err
	}
	if stuck.len() != 1 {
		return fmt.Errorf("prefetch=1 with no ack must hold at 1, held %d", stuck.len())
	}
	if flowed.len() != 5 {
		return fmt.Errorf("the capped consumer's turns must flow on, got %d", flowed.len())
	}
	return nil
}

func topicExchange() error {
	srv, err := startServer()
	if err != nil {
		return err
	}
	defer srv.stop()

	ch, cleanup, err := srv.channel(true)
	if err != nil {
		return err
	}
	defer cleanup()

	if err := ch.ExchangeDeclare("logs", "topic", false, false, false, false, nil); err != nil {
		return err
	}
	for _, q := range []string{"kern", "all"} {
		if _, err := ch.QueueDeclare(q, false, false, false, false, nil); err != nil {
			return err
		}
	}
	if err := ch.QueueBind("kern", "kern.*", "logs", false, nil); err != nil {
		return err
	}
	if err := ch.QueueBind("all", "#", "logs", false, nil); err != nil {
		return err
	}

	if err := publishConfirmed(ch, "logs", "kern.crit", amqp.Publishing{Body: []byte("kc")}); err != nil {
		return err
	}
	if err := publishConfirmed(ch, "logs", "app.info", amqp.Publishing{Body: []byte("ai")}); err != nil {
		return err
	}

	msg, ok, _ := ch.Get("kern", true)
	if !ok || string(msg.Body) != "kc" {
		return fmt.Errorf("kern.* must match kern.crit")
	}
	if _, ok, _ := ch.Get("kern", true); ok {
		return fmt.Errorf("kern.* must not match app.info")
	}
	for i := 0; i < 2; i++ {
		if _, ok, _ := ch.Get("all", true); !ok {
			return fmt.Errorf("# must match everything (missing delivery %d)", i)
		}
	}
	return nil
}

func fanout() error {
	srv, err := startServer()
	if err != nil {
		return err
	}
	defer srv.stop()

	ch, cleanup, err := srv.channel(true)
	if err != nil {
		return err
	}
	defer cleanup()

	if err := ch.ExchangeDeclare("bcast", "fanout", false, false, false, false, nil); err != nil {
		return err
	}
	for _, q := range []string{"f1", "f2"} {
		if _, err := ch.QueueDeclare(q, false, false, false, false, nil); err != nil {
			return err
		}
		if err := ch.QueueBind(q, "", "bcast", false, nil); err != nil {
			return err
		}
	}
	if err := publishConfirmed(ch, "bcast", "ignored-key", amqp.Publishing{Body: []byte("copy")}); err != nil {
		return err
	}
	for _, q := range []string{"f1", "f2"} {
		msg, ok, _ := ch.Get(q, true)
		if !ok || string(msg.Body) != "copy" {
			return fmt.Errorf("fanout must copy to %s", q)
		}
	}
	return nil
}

func mandatoryReturn() error {
	srv, err := startServer()
	if err != nil {
		return err
	}
	defer srv.stop()

	// No confirm mode on this channel: the Basic.Return itself is what is
	// under test (the flush bug the .NET client found lived exactly here).
	ch, cleanup, err := srv.channel(false)
	if err != nil {
		return err
	}
	defer cleanup()

	returns := ch.NotifyReturn(make(chan amqp.Return, 1))
	if err := ch.PublishWithContext(context.Background(),
		"", "no-such-queue", true /*mandatory*/, false, amqp.Publishing{Body: []byte("boomerang")}); err != nil {
		return err
	}
	select {
	case r := <-returns:
		if r.ReplyCode != 312 {
			return fmt.Errorf("reply code must be 312 NO_ROUTE, got %d", r.ReplyCode)
		}
		if string(r.Body) != "boomerang" {
			return fmt.Errorf("the returned body must be the published one, got %q", r.Body)
		}
	case <-time.After(8 * time.Second):
		return fmt.Errorf("no Basic.Return arrived for an unroutable mandatory publish")
	}

	// A routable mandatory publish must NOT return.
	if _, err := ch.QueueDeclare("exists", false, false, false, false, nil); err != nil {
		return err
	}
	if err := ch.PublishWithContext(context.Background(),
		"", "exists", true, false, amqp.Publishing{Body: []byte("lands")}); err != nil {
		return err
	}
	return until(func() bool {
		_, ok, _ := ch.Get("exists", true)
		return ok
	}, "the routable mandatory publish to land")
}

func nackRequeue() error {
	srv, err := startServer()
	if err != nil {
		return err
	}
	defer srv.stop()

	ch, cleanup, err := srv.channel(true)
	if err != nil {
		return err
	}
	defer cleanup()

	if _, err := ch.QueueDeclare("q", false, false, false, false, nil); err != nil {
		return err
	}
	if err := publishConfirmed(ch, "", "q", amqp.Publishing{Body: []byte("precious")}); err != nil {
		return err
	}

	first, ok, err := ch.Get("q", false)
	if err != nil || !ok {
		return fmt.Errorf("first get failed: %v", err)
	}
	if first.Redelivered {
		return fmt.Errorf("first delivery must not be flagged redelivered")
	}
	if err := first.Nack(false, true /*requeue*/); err != nil {
		return err
	}

	second, ok, err := ch.Get("q", false)
	if err != nil || !ok {
		return fmt.Errorf("the nacked message must come back: %v", err)
	}
	if !second.Redelivered {
		return fmt.Errorf("the requeued delivery must be flagged redelivered")
	}
	if string(second.Body) != "precious" {
		return fmt.Errorf("body mismatch: %q", second.Body)
	}

	// Nack WITHOUT requeue: gone for good.
	if err := second.Nack(false, false); err != nil {
		return err
	}
	if _, ok, _ := ch.Get("q", true); ok {
		return fmt.Errorf("a discarded message must not return")
	}
	return nil
}

func durableSurvivesKill() error {
	dataDir, err := os.MkdirTemp("", "oxidb-amqp-go")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dataDir)
	port, err := freePort()
	if err != nil {
		return err
	}

	srv, err := startServerAt(port, dataDir)
	if err != nil {
		return err
	}
	defer func() { srv.stop() }()

	ch, cleanup, err := srv.channel(true)
	if err != nil {
		return err
	}
	if _, err := ch.QueueDeclare("dq", true /*durable*/, false, false, false, nil); err != nil {
		return err
	}
	for i := 0; i < 3; i++ {
		if err := publishConfirmed(ch, "", "dq", amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Body:         []byte(fmt.Sprintf("m%d", i)),
		}); err != nil {
			return err
		}
		// The confirm has arrived: the broker promises the message is on disk.
	}
	cleanup()

	// No graceful shutdown — the confirm is the only promise being tested.
	srv.kill()
	srv, err = startServerAt(port, dataDir)
	if err != nil {
		return err
	}

	ch, cleanup, err = srv.channel(false)
	if err != nil {
		return err
	}
	if _, err := ch.QueueDeclare("dq", true, false, false, false, nil); err != nil {
		return err
	}
	for i := 0; i < 3; i++ {
		msg, ok, _ := ch.Get("dq", true)
		if !ok {
			return fmt.Errorf("message m%d must survive the kill", i)
		}
		if string(msg.Body) != fmt.Sprintf("m%d", i) {
			return fmt.Errorf("order/body mismatch: %q", msg.Body)
		}
		if !msg.Redelivered {
			return fmt.Errorf("a recovered message must be flagged redelivered")
		}
	}
	if _, ok, _ := ch.Get("dq", true); ok {
		return fmt.Errorf("exactly three messages — no resurrection")
	}
	cleanup()

	// The drain deleted the durable records; a second kill must not bring
	// anything back (at-least-once must not become
	// at-least-twice-after-every-crash).
	srv.kill()
	srv, err = startServerAt(port, dataDir)
	if err != nil {
		return err
	}

	ch, cleanup, err = srv.channel(false)
	if err != nil {
		return err
	}
	defer cleanup()
	if _, err := ch.QueueDeclare("dq", true, false, false, false, nil); err != nil {
		return err
	}
	if _, ok, _ := ch.Get("dq", true); ok {
		return fmt.Errorf("consumed messages must stay consumed across a crash")
	}
	return nil
}

// ── Helpers ─────────────────────────────────────────────────────────────

// publishConfirmed publishes and waits for the broker's ack — the
// write-before-confirm promise, awaited per message.
func publishConfirmed(ch *amqp.Channel, exchange, key string, msg amqp.Publishing) error {
	dc, err := ch.PublishWithDeferredConfirmWithContext(
		context.Background(), exchange, key, false, false, msg)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	acked, err := dc.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("confirm never arrived: %w", err)
	}
	if !acked {
		return fmt.Errorf("broker nacked the publish")
	}
	return nil
}

func until(cond func() bool, what string) error {
	deadline := time.Now().Add(8 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

type safeList struct {
	mu    sync.Mutex
	items []string
}

func (l *safeList) add(s string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.items = append(l.items, s)
}

func (l *safeList) len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.items)
}

func (l *safeList) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.items...)
}

// server is an oxidb-server that dies with its guard, AMQP listener up.
type server struct {
	port  int
	data  string
	proc  *exec.Cmd
	conns []*amqp.Connection
}

func startServer() (*server, error) {
	dataDir, err := os.MkdirTemp("", "oxidb-amqp-go")
	if err != nil {
		return nil, err
	}
	port, err := freePort()
	if err != nil {
		return nil, err
	}
	srv, err := startServerAt(port, dataDir)
	if err != nil {
		return nil, err
	}
	return srv, nil
}

func startServerAt(port int, dataDir string) (*server, error) {
	bin, err := findServerBinary()
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

func (s *server) connect() (*amqp.Connection, error) {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://127.0.0.1:%d/", s.port))
	if err != nil {
		return nil, err
	}
	s.conns = append(s.conns, conn)
	return conn, nil
}

// channel opens a fresh connection + channel, optionally in confirm mode.
func (s *server) channel(confirms bool) (*amqp.Channel, func(), error) {
	conn, err := s.connect()
	if err != nil {
		return nil, nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, err
	}
	if confirms {
		if err := ch.Confirm(false); err != nil {
			return nil, nil, err
		}
	}
	return ch, func() { conn.Close() }, nil
}

// ackingConsumer consumes from `queue` on its own connection, acking every
// delivery and recording its body. `declare` creates the queue first.
func (s *server) ackingConsumer(queue string, declare bool) (*safeList, func(), error) {
	conn, err := s.connect()
	if err != nil {
		return nil, nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, err
	}
	if declare {
		if _, err := ch.QueueDeclare(queue, false, false, false, false, nil); err != nil {
			return nil, nil, err
		}
	}
	deliveries, err := ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		return nil, nil, err
	}
	got := &safeList{}
	go func() {
		for d := range deliveries {
			got.add(string(d.Body))
			_ = d.Ack(false)
		}
	}()
	return got, func() { conn.Close() }, nil
}

// kill is SIGKILL — no graceful shutdown, that is the point.
func (s *server) kill() {
	for _, c := range s.conns {
		_ = c.Close()
	}
	s.conns = nil
	_ = s.proc.Process.Kill()
	_, _ = s.proc.Process.Wait()
}

func (s *server) stop() {
	s.kill()
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
		for _, profile := range []string{"debug", "release"} {
			p := filepath.Join(dir, "target", profile, "oxidb-server")
			if _, err := os.Stat(p); err == nil {
				return p, nil
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("oxidb-server binary not found — `cargo build -p oxidb-server` first, or set OXIDB_SERVER_BIN")
		}
		dir = parent
	}
}
