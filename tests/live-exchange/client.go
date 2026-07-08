package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
)

// Client is a minimal OxiDB wire client: length-prefixed JSON over TCP.
type Client struct {
	conn net.Conn
	addr string
}

func Dial() (*Client, error) {
	port := os.Getenv("OXIDB_PORT")
	if port == "" {
		port = "4444"
	}
	addr := "127.0.0.1:" + port
	c := &Client{addr: addr}
	return c, c.connect()
}

func (c *Client) connect() error {
	conn, err := net.Dial("tcp", c.addr)
	if err != nil {
		return err
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		tcp.SetNoDelay(true)
	}
	c.conn = conn
	return nil
}

// Call sends one request and returns the decoded response. On a transport
// error it reconnects and returns the error so the caller can retry.
func (c *Client) Call(req map[string]any) (map[string]any, error) {
	b, _ := json.Marshal(req)
	hdr := make([]byte, 4)
	binary.LittleEndian.PutUint32(hdr, uint32(len(b)))
	if _, err := c.conn.Write(hdr); err != nil {
		c.connect()
		return nil, err
	}
	if _, err := c.conn.Write(b); err != nil {
		c.connect()
		return nil, err
	}
	if _, err := io.ReadFull(c.conn, hdr); err != nil {
		c.connect()
		return nil, err
	}
	n := binary.LittleEndian.Uint32(hdr)
	buf := make([]byte, n)
	if _, err := io.ReadFull(c.conn, buf); err != nil {
		c.connect()
		return nil, err
	}
	var resp map[string]any
	if err := json.Unmarshal(buf, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// ok reports whether a response was successful.
func ok(resp map[string]any) bool {
	b, _ := resp["ok"].(bool)
	return b
}

func errStr(resp map[string]any) string {
	s, _ := resp["error"].(string)
	return s
}

// --- convenience wrappers ---

func (c *Client) Insert(coll string, doc map[string]any) error {
	r, err := c.Call(map[string]any{"cmd": "insert", "collection": coll, "doc": doc})
	if err != nil {
		return err
	}
	if !ok(r) {
		return fmt.Errorf("insert: %s", errStr(r))
	}
	return nil
}

func (c *Client) Find(coll string, query map[string]any, sort map[string]any, limit int) ([]map[string]any, error) {
	req := map[string]any{"cmd": "find", "collection": coll, "query": query}
	if sort != nil {
		req["sort"] = sort
	}
	if limit > 0 {
		req["limit"] = limit
	}
	r, err := c.Call(req)
	if err != nil {
		return nil, err
	}
	data, _ := r["data"].([]any)
	out := make([]map[string]any, 0, len(data))
	for _, d := range data {
		if m, ok := d.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out, nil
}

func (c *Client) FindOne(coll string, query map[string]any) (map[string]any, error) {
	rows, err := c.Find(coll, query, nil, 1)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	return rows[0], nil
}

func (c *Client) Count(coll string, query map[string]any) int {
	r, err := c.Call(map[string]any{"cmd": "count", "collection": coll, "query": query})
	if err != nil || !ok(r) {
		return -1
	}
	d, _ := r["data"].(map[string]any)
	n, _ := d["count"].(float64)
	return int(n)
}

// Delete removes matching docs outside a transaction (auto-commit).
func (c *Client) Delete(coll string, query map[string]any) {
	c.Call(map[string]any{"cmd": "delete", "collection": coll, "query": query})
}

func (c *Client) CreateIndex(coll, field string) {
	c.Call(map[string]any{"cmd": "create_index", "collection": coll, "field": field})
}

func (c *Client) CreateUniqueIndex(coll, field string) {
	c.Call(map[string]any{"cmd": "create_unique_index", "collection": coll, "field": field})
}

func (c *Client) CreateTTL(coll, field string, secs int) error {
	r, err := c.Call(map[string]any{"cmd": "create_ttl_index", "collection": coll,
		"field": field, "expireAfterSeconds": secs})
	if err != nil {
		return err
	}
	if !ok(r) {
		return fmt.Errorf("ttl: %s", errStr(r))
	}
	return nil
}

func (c *Client) Aggregate(coll string, pipeline []any) []map[string]any {
	r, err := c.Call(map[string]any{"cmd": "aggregate", "collection": coll, "pipeline": pipeline})
	if err != nil || !ok(r) {
		return nil
	}
	data, _ := r["data"].([]any)
	out := make([]map[string]any, 0, len(data))
	for _, d := range data {
		if m, ok := d.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

// --- transaction (per-connection session) ---

func (c *Client) Begin() error {
	r, err := c.Call(map[string]any{"cmd": "begin_tx"})
	if err != nil {
		return err
	}
	if !ok(r) {
		return fmt.Errorf("begin: %s", errStr(r))
	}
	return nil
}

func (c *Client) TxUpdate(coll string, query, update map[string]any) error {
	r, err := c.Call(map[string]any{"cmd": "update", "collection": coll, "query": query, "update": update})
	if err != nil {
		return err
	}
	if !ok(r) {
		return fmt.Errorf("update: %s", errStr(r))
	}
	return nil
}

func (c *Client) TxInsert(coll string, doc map[string]any) error {
	r, err := c.Call(map[string]any{"cmd": "insert", "collection": coll, "doc": doc})
	if err != nil {
		return err
	}
	if !ok(r) {
		return fmt.Errorf("tx insert: %s", errStr(r))
	}
	return nil
}

func (c *Client) TxDelete(coll string, query map[string]any) error {
	r, err := c.Call(map[string]any{"cmd": "delete", "collection": coll, "query": query})
	if err != nil {
		return err
	}
	if !ok(r) {
		return fmt.Errorf("delete: %s", errStr(r))
	}
	return nil
}

// TxFindForUpdate locks matched docs until commit/rollback (SELECT FOR UPDATE).
func (c *Client) TxFindForUpdate(coll string, query map[string]any) ([]map[string]any, error) {
	r, err := c.Call(map[string]any{"cmd": "find_for_update", "collection": coll,
		"query": query, "lock_timeout_ms": 5000})
	if err != nil {
		return nil, err
	}
	if !ok(r) {
		return nil, fmt.Errorf("for_update: %s", errStr(r))
	}
	data, _ := r["data"].([]any)
	out := make([]map[string]any, 0, len(data))
	for _, d := range data {
		if m, ok := d.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out, nil
}

// Commit returns (committed, isConflict, err). Conflict lets the caller retry.
func (c *Client) Commit() (bool, bool, error) {
	r, err := c.Call(map[string]any{"cmd": "commit_tx"})
	if err != nil {
		return false, false, err
	}
	if ok(r) {
		return true, false, nil
	}
	e := errStr(r)
	isConflict := containsAny(e, "conflict") || containsAny(e, "lock timeout")
	return false, isConflict, fmt.Errorf("%s", e)
}

func (c *Client) Rollback() {
	c.Call(map[string]any{"cmd": "rollback_tx"})
}

func containsAny(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
