// cluster-init: one-shot bootstrapper for the ShopEdge Raft groups.
//
// For each of the 3 shards, connects to db-X0:4444 and runs:
//   1. raft_init           — elects db-X0 as the single-member leader
//   2. raft_add_learner ×2 — adds db-X1 and db-X2 as learners
//   3. raft_change_membership — promotes all 3 to voters
//
// Idempotent: if a shard is already initialized, the raft_init call returns
// an error which we log and continue. Wire protocol is length-prefixed JSON.

package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"time"
)

const (
	clientPort = 4444
	raftPort   = 5000
)

type shard struct {
	name  string // "A", "B", "C"
	nodes []node // db-a0, db-a1, db-a2 (etc.)
}

type node struct {
	id   uint64
	host string // db-a0
}

var shards = []shard{
	{name: "A", nodes: []node{{1, "db-a0"}, {2, "db-a1"}, {3, "db-a2"}}},
	{name: "B", nodes: []node{{1, "db-b0"}, {2, "db-b1"}, {3, "db-b2"}}},
	{name: "C", nodes: []node{{1, "db-c0"}, {2, "db-c1"}, {3, "db-c2"}}},
}

func main() {
	wait := envSeconds("WAIT_SECONDS", 8)
	log.Printf("cluster-init: waiting %ds for oxidb-server nodes to come up...", wait)
	time.Sleep(time.Duration(wait) * time.Second)

	for _, s := range shards {
		log.Printf("─── shard %s ───────────────────────────────", s.name)
		if err := initShard(s); err != nil {
			log.Printf("shard %s: %v (continuing — may already be initialized)", s.name, err)
			continue
		}
		log.Printf("shard %s: cluster initialized with members %v", s.name, memberIDs(s))
	}
	log.Println("cluster-init: done.")
}

// initShard bootstraps one Raft group via the leader candidate (node id=1).
func initShard(s shard) error {
	leader := s.nodes[0]

	// Establish a connection to the leader candidate, with retries — the
	// container may take a moment to bind its listener after our 8s sleep.
	conn, err := dialWithRetry(leader.host, clientPort, 30*time.Second)
	if err != nil {
		return fmt.Errorf("dial %s: %w", leader.host, err)
	}
	defer conn.Close()

	// 1. raft_init — single-member cluster
	if resp, err := request(conn, map[string]any{"cmd": "raft_init"}); err != nil {
		return fmt.Errorf("raft_init: %w", err)
	} else {
		log.Printf("  raft_init  → %s", brief(resp))
	}

	// 2. add_learner for nodes 2 and 3
	for _, n := range s.nodes[1:] {
		addr := fmt.Sprintf("%s:%d", n.host, raftPort)
		resp, err := request(conn, map[string]any{
			"cmd":     "raft_add_learner",
			"node_id": n.id,
			"addr":    addr,
		})
		if err != nil {
			return fmt.Errorf("add_learner node %d (%s): %w", n.id, addr, err)
		}
		log.Printf("  add_learner node=%d addr=%s → %s", n.id, addr, brief(resp))
	}

	// 3. change_membership to {1, 2, 3}
	members := memberIDs(s)
	resp, err := request(conn, map[string]any{
		"cmd":     "raft_change_membership",
		"members": members,
	})
	if err != nil {
		return fmt.Errorf("change_membership %v: %w", members, err)
	}
	log.Printf("  change_membership %v → %s", members, brief(resp))

	// Final: print metrics so we can see leader + state in compose logs
	if resp, err := request(conn, map[string]any{"cmd": "raft_metrics"}); err == nil {
		log.Printf("  metrics → %s", brief(resp))
	}
	return nil
}

func memberIDs(s shard) []uint64 {
	ids := make([]uint64, len(s.nodes))
	for i, n := range s.nodes {
		ids[i] = n.id
	}
	return ids
}

// ─── Wire ────────────────────────────────────────────────────────────

func dialWithRetry(host string, port int, totalTimeout time.Duration) (net.Conn, error) {
	deadline := time.Now().Add(totalTimeout)
	addr := fmt.Sprintf("%s:%d", host, port)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err == nil {
			return conn, nil
		}
		lastErr = err
		time.Sleep(1 * time.Second)
	}
	return nil, fmt.Errorf("after %s: %v", totalTimeout, lastErr)
}

func request(conn net.Conn, payload map[string]any) (map[string]any, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	if err := writeFrame(conn, body); err != nil {
		return nil, err
	}
	respBytes, err := readFrame(conn)
	if err != nil {
		return nil, err
	}
	var resp map[string]any
	if err := json.Unmarshal(respBytes, &resp); err != nil {
		return nil, fmt.Errorf("decode response: %w (body=%q)", err, string(respBytes))
	}
	if ok, _ := resp["ok"].(bool); !ok {
		errMsg, _ := resp["error"].(string)
		return resp, fmt.Errorf("server: %s", errMsg)
	}
	return resp, nil
}

func writeFrame(w io.Writer, payload []byte) error {
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(payload)))
	if _, err := w.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

func readFrame(r io.Reader) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	n := binary.LittleEndian.Uint32(lenBuf[:])
	if n > 16*1024*1024 {
		return nil, fmt.Errorf("frame too large: %d", n)
	}
	payload := make([]byte, n)
	_, err := io.ReadFull(r, payload)
	return payload, err
}

// ─── Helpers ────────────────────────────────────────────────────────

func envSeconds(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// brief returns a single-line summary of a response map for logging.
func brief(resp map[string]any) string {
	b, _ := json.Marshal(resp)
	if len(b) > 200 {
		return string(b[:200]) + "..."
	}
	return string(b)
}
