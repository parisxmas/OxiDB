// Diagnostic: isolates where single-doc insert latency goes.
//
// Compares ping vs insert round-trips, and coalesced (1 write) vs split
// (2 writes, as the oxidb client's sendRaw does) framing — over the same
// TCP path the benchmark uses.
package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"sort"
	"time"

	"github.com/parisxmas/OxiDB/go/oxiwire"
)

func serverAddr() string {
	if a := os.Getenv("OXIDB_ADDR"); a != "" {
		return a
	}
	return "127.0.0.1:4444"
}

func genDoc(rng *rand.Rand, i int) map[string]any {
	depts := []string{"Engineering", "Sales", "Marketing", "Support", "HR"}
	return map[string]any{
		"seq": i, "name": fmt.Sprintf("User %d", i),
		"email": fmt.Sprintf("user.%d@test.com", i),
		"age":   18 + rng.Intn(60), "salary": 30000.0 + float64(rng.Intn(170000)),
		"department": depts[rng.Intn(len(depts))], "city": "Tokyo",
		"country": "JP", "status": "active", "score": float64(rng.Intn(10000)) / 100.0,
		"verified": true, "rating": rng.Intn(5) + 1,
		"tags":    []any{"a", "b"},
		"address": map[string]any{"street": "100 Main St", "zip": "01234"},
	}
}

// sendCoalesced writes [len][payload] in ONE syscall.
func sendCoalesced(conn net.Conn, payload []byte) error {
	buf := make([]byte, 4+len(payload))
	binary.LittleEndian.PutUint32(buf, uint32(len(payload)))
	copy(buf[4:], payload)
	_, err := conn.Write(buf)
	return err
}

// sendSplit writes [len] then [payload] in TWO syscalls — mirrors oxidb client sendRaw.
func sendSplit(conn net.Conn, payload []byte) error {
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, uint32(len(payload)))
	if _, err := conn.Write(lenBuf); err != nil {
		return err
	}
	_, err := conn.Write(payload)
	return err
}

func recv(conn net.Conn) ([]byte, error) {
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, lenBuf); err != nil {
		return nil, err
	}
	n := binary.LittleEndian.Uint32(lenBuf)
	p := make([]byte, n)
	_, err := io.ReadFull(conn, p)
	return p, err
}

type stats struct{ p50, p99, mean, min, max time.Duration }

func measure(name string, n int, fn func() error) stats {
	ds := make([]time.Duration, 0, n)
	var total time.Duration
	for i := 0; i < n; i++ {
		t := time.Now()
		if err := fn(); err != nil {
			panic(fmt.Sprintf("%s: %v", name, err))
		}
		d := time.Since(t)
		ds = append(ds, d)
		total += d
	}
	sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
	s := stats{
		p50:  ds[n/2],
		p99:  ds[(n*99)/100],
		mean: total / time.Duration(n),
		min:  ds[0],
		max:  ds[n-1],
	}
	fmt.Printf("  %-32s  p50 %7s   p99 %8s   mean %7s   min %6s   max %8s\n",
		name, s.p50, s.p99, s.mean, s.min, s.max)
	return s
}

func main() {
	addr := serverAddr()
	fmt.Printf("connecting to %s\n", addr)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	if tcp, ok := conn.(*net.TCPConn); ok {
		fmt.Println("TCP_NODELAY default on dial; leaving as-is (Go default = true)")
		_ = tcp
	}
	rng := rand.New(rand.NewSource(99))

	const N = 20000
	const warmup = 2000

	pingPayload := oxiwire.Marshal(map[string]any{"cmd": "ping"})

	// round-trip helpers
	rtCoalesced := func(payload []byte) error {
		if err := sendCoalesced(conn, payload); err != nil {
			return err
		}
		_, err := recv(conn)
		return err
	}
	rtSplit := func(payload []byte) error {
		if err := sendSplit(conn, payload); err != nil {
			return err
		}
		_, err := recv(conn)
		return err
	}

	// warmup
	for i := 0; i < warmup; i++ {
		_ = rtCoalesced(pingPayload)
	}

	fmt.Printf("\n── %d iterations each ──\n\n", N)

	// 1. Pure round-trip: ping, coalesced vs split
	measure("ping (1 write)", N, func() error { return rtCoalesced(pingPayload) })
	measure("ping (2 writes, client-style)", N, func() error { return rtSplit(pingPayload) })

	fmt.Println()

	// 2. Marshal cost alone (no network)
	{
		ds := make([]time.Duration, 0, N)
		var total time.Duration
		for i := 0; i < N; i++ {
			doc := genDoc(rng, i)
			req := map[string]any{"cmd": "insert", "collection": "diag", "doc": doc}
			t := time.Now()
			_ = oxiwire.Marshal(req)
			d := time.Since(t)
			ds = append(ds, d)
			total += d
		}
		sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
		fmt.Printf("  %-32s  p50 %7s   p99 %8s   mean %7s\n",
			"oxiwire.Marshal(insert req)", ds[N/2], ds[(N*99)/100], total/time.Duration(N))
	}

	fmt.Println()

	// 3. Insert round-trip: coalesced vs split
	measure("insert (1 write)", N, func() error {
		req := oxiwire.Marshal(map[string]any{"cmd": "insert", "collection": "diag", "doc": genDoc(rng, rng.Int())})
		return rtCoalesced(req)
	})
	measure("insert (2 writes, client-style)", N, func() error {
		req := oxiwire.Marshal(map[string]any{"cmd": "insert", "collection": "diag", "doc": genDoc(rng, rng.Int())})
		return rtSplit(req)
	})

	fmt.Println()

	// 4. Pre-marshalled insert (excludes marshal + genDoc from the timed section)
	preReq := oxiwire.Marshal(map[string]any{"cmd": "insert", "collection": "diag", "doc": genDoc(rng, 1)})
	measure("insert pre-marshalled (1 write)", N, func() error { return rtCoalesced(preReq) })
	measure("insert pre-marshalled (2 writes)", N, func() error { return rtSplit(preReq) })

	fmt.Println()

	// 5. recv variant: single buffered read instead of two io.ReadFull calls.
	//    Tests whether the proxy splits the response across two reads.
	br := bufio.NewReaderSize(conn, 64*1024)
	recvBuffered := func() error {
		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(br, lenBuf); err != nil {
			return err
		}
		n := binary.LittleEndian.Uint32(lenBuf)
		p := make([]byte, n)
		_, err := io.ReadFull(br, p)
		return err
	}
	rtBufferedRecv := func(payload []byte) error {
		if err := sendCoalesced(conn, payload); err != nil {
			return err
		}
		return recvBuffered()
	}
	measure("ping (1 write, buffered recv)", N, func() error { return rtBufferedRecv(pingPayload) })
	measure("insert (1 write, buffered recv)", N, func() error { return rtBufferedRecv(preReq) })
}
