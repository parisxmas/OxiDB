// Package oxidb provides a TCP client for oxidb-server.
//
// Protocol: each message is [4-byte little-endian length][JSON payload].
// Server responds with {"ok": true, "data": ...} or {"ok": false, "error": "..."}.
//
// Zero external dependencies — uses only the Go standard library.
package oxidb

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

// Client is a TCP client for oxidb-server. Thread-safe via mutex.
type Client struct {
	conn net.Conn
	mu   sync.Mutex
}

// Connect creates a new client connected to oxidb-server.
func Connect(host string, port int, timeout time.Duration) (*Client, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, fmt.Errorf("oxidb: connect to %s: %w", addr, err)
	}
	conn.SetDeadline(time.Time{})
	return &Client{conn: conn}, nil
}

// ConnectDefault connects to localhost:4444 with a 5-second timeout.
func ConnectDefault() (*Client, error) {
	return Connect("127.0.0.1", 4444, 5*time.Second)
}

// Close closes the TCP connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// ------------------------------------------------------------------
// Low-level protocol
// ------------------------------------------------------------------

func (c *Client) sendRaw(data []byte) error {
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, uint32(len(data)))
	if _, err := c.conn.Write(lenBuf); err != nil {
		return err
	}
	_, err := c.conn.Write(data)
	return err
}

func (c *Client) recvRaw() ([]byte, error) {
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(c.conn, lenBuf); err != nil {
		return nil, fmt.Errorf("oxidb: read length: %w", err)
	}
	length := binary.LittleEndian.Uint32(lenBuf)
	payload := make([]byte, length)
	if _, err := io.ReadFull(c.conn, payload); err != nil {
		return nil, fmt.Errorf("oxidb: read payload: %w", err)
	}
	return payload, nil
}

func (c *Client) request(payload map[string]any) (map[string]any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("oxidb: marshal request: %w", err)
	}
	if err := c.sendRaw(jsonBytes); err != nil {
		return nil, fmt.Errorf("oxidb: send: %w", err)
	}
	respBytes, err := c.recvRaw()
	if err != nil {
		return nil, err
	}
	var resp map[string]any
	if err := json.Unmarshal(respBytes, &resp); err != nil {
		return nil, fmt.Errorf("oxidb: unmarshal response: %w", err)
	}
	return resp, nil
}

func (c *Client) checked(payload map[string]any) (any, error) {
	resp, err := c.request(payload)
	if err != nil {
		return nil, err
	}
	ok, _ := resp["ok"].(bool)
	if !ok {
		errMsg, _ := resp["error"].(string)
		if errMsg == "" {
			errMsg = "unknown error"
		}
		if strings.Contains(strings.ToLower(errMsg), "conflict") {
			return nil, &TransactionConflictError{Msg: errMsg}
		}
		return nil, &Error{Msg: errMsg}
	}
	return resp["data"], nil
}

// ------------------------------------------------------------------
// Utility
// ------------------------------------------------------------------

// Ping sends a ping to the server. Returns "pong".
func (c *Client) Ping() (string, error) {
	data, err := c.checked(map[string]any{"cmd": "ping"})
	if err != nil {
		return "", err
	}
	s, _ := data.(string)
	return s, nil
}

// ------------------------------------------------------------------
// Collection management
// ------------------------------------------------------------------

// CreateCollection explicitly creates a collection.
func (c *Client) CreateCollection(name string) error {
	_, err := c.checked(map[string]any{"cmd": "create_collection", "collection": name})
	return err
}

// ListCollections returns a list of collection names.
func (c *Client) ListCollections() ([]string, error) {
	data, err := c.checked(map[string]any{"cmd": "list_collections"})
	if err != nil {
		return nil, err
	}
	arr, _ := data.([]any)
	result := make([]string, len(arr))
	for i, v := range arr {
		result[i], _ = v.(string)
	}
	return result, nil
}

// DropCollection drops a collection and its data.
func (c *Client) DropCollection(name string) error {
	_, err := c.checked(map[string]any{"cmd": "drop_collection", "collection": name})
	return err
}

// ------------------------------------------------------------------
// CRUD
// ------------------------------------------------------------------

// Insert inserts a single document. Returns the raw response data.
func (c *Client) Insert(collection string, doc map[string]any) (map[string]any, error) {
	data, err := c.checked(map[string]any{"cmd": "insert", "collection": collection, "doc": doc})
	if err != nil {
		return nil, err
	}
	if m, ok := data.(map[string]any); ok {
		return m, nil
	}
	// Inside tx, returns "buffered"
	return map[string]any{"status": data}, nil
}

// InsertMany inserts multiple documents.
func (c *Client) InsertMany(collection string, docs []map[string]any) (any, error) {
	return c.checked(map[string]any{"cmd": "insert_many", "collection": collection, "docs": docs})
}

// FindOptions holds optional parameters for Find.
type FindOptions struct {
	Sort  map[string]any
	Skip  *int
	Limit *int
}

// Find returns documents matching a query.
func (c *Client) Find(collection string, query map[string]any, opts *FindOptions) ([]map[string]any, error) {
	payload := map[string]any{"cmd": "find", "collection": collection, "query": query}
	if opts != nil {
		if opts.Sort != nil {
			payload["sort"] = opts.Sort
		}
		if opts.Skip != nil {
			payload["skip"] = *opts.Skip
		}
		if opts.Limit != nil {
			payload["limit"] = *opts.Limit
		}
	}
	data, err := c.checked(payload)
	if err != nil {
		return nil, err
	}
	return toMapSlice(data), nil
}

// FindOne returns a single document matching a query, or nil.
func (c *Client) FindOne(collection string, query map[string]any) (map[string]any, error) {
	data, err := c.checked(map[string]any{"cmd": "find_one", "collection": collection, "query": query})
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}
	m, _ := data.(map[string]any)
	return m, nil
}

// Update updates documents matching a query.
func (c *Client) Update(collection string, query, update map[string]any) (map[string]any, error) {
	data, err := c.checked(map[string]any{
		"cmd": "update", "collection": collection,
		"query": query, "update": update,
	})
	if err != nil {
		return nil, err
	}
	if m, ok := data.(map[string]any); ok {
		return m, nil
	}
	return map[string]any{"status": data}, nil
}

// UpdateOne updates at most one document matching a query.
func (c *Client) UpdateOne(collection string, query, update map[string]any) (map[string]any, error) {
	data, err := c.checked(map[string]any{
		"cmd": "update_one", "collection": collection,
		"query": query, "update": update,
	})
	if err != nil {
		return nil, err
	}
	if m, ok := data.(map[string]any); ok {
		return m, nil
	}
	return map[string]any{"status": data}, nil
}

// Delete deletes documents matching a query.
func (c *Client) Delete(collection string, query map[string]any) (map[string]any, error) {
	data, err := c.checked(map[string]any{
		"cmd": "delete", "collection": collection, "query": query,
	})
	if err != nil {
		return nil, err
	}
	if m, ok := data.(map[string]any); ok {
		return m, nil
	}
	return map[string]any{"status": data}, nil
}

// DeleteOne deletes at most one document matching a query.
func (c *Client) DeleteOne(collection string, query map[string]any) (map[string]any, error) {
	data, err := c.checked(map[string]any{
		"cmd": "delete_one", "collection": collection, "query": query,
	})
	if err != nil {
		return nil, err
	}
	if m, ok := data.(map[string]any); ok {
		return m, nil
	}
	return map[string]any{"status": data}, nil
}

// Count returns the number of documents matching a query.
func (c *Client) Count(collection string, query map[string]any) (int, error) {
	data, err := c.checked(map[string]any{
		"cmd": "count", "collection": collection, "query": query,
	})
	if err != nil {
		return 0, err
	}
	m, _ := data.(map[string]any)
	count, _ := m["count"].(float64)
	return int(count), nil
}

// ------------------------------------------------------------------
// Indexes
// ------------------------------------------------------------------

// CreateIndex creates a non-unique index on a field.
func (c *Client) CreateIndex(collection, field string) error {
	_, err := c.checked(map[string]any{"cmd": "create_index", "collection": collection, "field": field})
	return err
}

// CreateUniqueIndex creates a unique index on a field.
func (c *Client) CreateUniqueIndex(collection, field string) error {
	_, err := c.checked(map[string]any{"cmd": "create_unique_index", "collection": collection, "field": field})
	return err
}

// CreateCompositeIndex creates a composite index on multiple fields.
func (c *Client) CreateCompositeIndex(collection string, fields []string) error {
	_, err := c.checked(map[string]any{"cmd": "create_composite_index", "collection": collection, "fields": fields})
	return err
}

// CreateTextIndex creates a full-text search index on the specified fields.
func (c *Client) CreateTextIndex(collection string, fields []string) error {
	_, err := c.checked(map[string]any{
		"cmd": "create_text_index", "collection": collection, "fields": fields,
	})
	return err
}

// ListIndexes returns metadata for all indexes on a collection.
func (c *Client) ListIndexes(collection string) ([]map[string]any, error) {
	data, err := c.checked(map[string]any{"cmd": "list_indexes", "collection": collection})
	if err != nil {
		return nil, err
	}
	return toMapSlice(data), nil
}

// DropIndex drops an index by name.
func (c *Client) DropIndex(collection, index string) error {
	_, err := c.checked(map[string]any{
		"cmd": "drop_index", "collection": collection, "index": index,
	})
	return err
}

// TextSearch performs full-text search on a collection's text index.
func (c *Client) TextSearch(collection, query string, limit int) ([]map[string]any, error) {
	data, err := c.checked(map[string]any{
		"cmd": "text_search", "collection": collection, "query": query, "limit": limit,
	})
	if err != nil {
		return nil, err
	}
	return toMapSlice(data), nil
}

// ------------------------------------------------------------------
// Aggregation
// ------------------------------------------------------------------

// Aggregate runs an aggregation pipeline.
func (c *Client) Aggregate(collection string, pipeline []map[string]any) ([]map[string]any, error) {
	data, err := c.checked(map[string]any{
		"cmd": "aggregate", "collection": collection, "pipeline": pipeline,
	})
	if err != nil {
		return nil, err
	}
	return toMapSlice(data), nil
}

// ------------------------------------------------------------------
// Compaction
// ------------------------------------------------------------------

// Compact compacts a collection. Returns stats with old_size, new_size, docs_kept.
func (c *Client) Compact(collection string) (map[string]any, error) {
	data, err := c.checked(map[string]any{"cmd": "compact", "collection": collection})
	if err != nil {
		return nil, err
	}
	m, _ := data.(map[string]any)
	return m, nil
}

// ------------------------------------------------------------------
// Transactions
// ------------------------------------------------------------------

// BeginTx starts a transaction on this connection.
func (c *Client) BeginTx() (map[string]any, error) {
	data, err := c.checked(map[string]any{"cmd": "begin_tx"})
	if err != nil {
		return nil, err
	}
	m, _ := data.(map[string]any)
	return m, nil
}

// CommitTx commits the active transaction.
func (c *Client) CommitTx() error {
	_, err := c.checked(map[string]any{"cmd": "commit_tx"})
	return err
}

// RollbackTx rolls back the active transaction.
func (c *Client) RollbackTx() error {
	_, err := c.checked(map[string]any{"cmd": "rollback_tx"})
	return err
}

// WithTransaction executes fn within a transaction.
// Auto-commits on success, auto-rolls back on error.
func (c *Client) WithTransaction(fn func() error) error {
	if _, err := c.BeginTx(); err != nil {
		return err
	}
	if err := fn(); err != nil {
		_ = c.RollbackTx()
		return err
	}
	return c.CommitTx()
}

// ------------------------------------------------------------------
// Blob storage
// ------------------------------------------------------------------

// CreateBucket creates a blob storage bucket.
func (c *Client) CreateBucket(bucket string) error {
	_, err := c.checked(map[string]any{"cmd": "create_bucket", "bucket": bucket})
	return err
}

// ListBuckets lists all blob storage buckets.
func (c *Client) ListBuckets() ([]string, error) {
	data, err := c.checked(map[string]any{"cmd": "list_buckets"})
	if err != nil {
		return nil, err
	}
	arr, _ := data.([]any)
	result := make([]string, len(arr))
	for i, v := range arr {
		result[i], _ = v.(string)
	}
	return result, nil
}

// DeleteBucket deletes a blob storage bucket.
func (c *Client) DeleteBucket(bucket string) error {
	_, err := c.checked(map[string]any{"cmd": "delete_bucket", "bucket": bucket})
	return err
}

// PutObject uploads a blob object. Data is base64-encoded automatically.
func (c *Client) PutObject(bucket, key string, data []byte, contentType string, metadata map[string]string) (map[string]any, error) {
	payload := map[string]any{
		"cmd":          "put_object",
		"bucket":       bucket,
		"key":          key,
		"data":         base64.StdEncoding.EncodeToString(data),
		"content_type": contentType,
	}
	if contentType == "" {
		payload["content_type"] = "application/octet-stream"
	}
	if len(metadata) > 0 {
		payload["metadata"] = metadata
	}
	result, err := c.checked(payload)
	if err != nil {
		return nil, err
	}
	m, _ := result.(map[string]any)
	return m, nil
}

// GetObject downloads a blob object. Returns (data, metadata).
func (c *Client) GetObject(bucket, key string) ([]byte, map[string]any, error) {
	result, err := c.checked(map[string]any{"cmd": "get_object", "bucket": bucket, "key": key})
	if err != nil {
		return nil, nil, err
	}
	m, _ := result.(map[string]any)
	content, _ := m["content"].(string)
	decoded, err := base64.StdEncoding.DecodeString(content)
	if err != nil {
		return nil, nil, fmt.Errorf("oxidb: decode base64: %w", err)
	}
	meta, _ := m["metadata"].(map[string]any)
	return decoded, meta, nil
}

// HeadObject gets blob object metadata without downloading content.
func (c *Client) HeadObject(bucket, key string) (map[string]any, error) {
	data, err := c.checked(map[string]any{"cmd": "head_object", "bucket": bucket, "key": key})
	if err != nil {
		return nil, err
	}
	m, _ := data.(map[string]any)
	return m, nil
}

// DeleteObject deletes a blob object.
func (c *Client) DeleteObject(bucket, key string) error {
	_, err := c.checked(map[string]any{"cmd": "delete_object", "bucket": bucket, "key": key})
	return err
}

// ListObjects lists objects in a bucket.
func (c *Client) ListObjects(bucket string, prefix *string, limit *int) ([]map[string]any, error) {
	payload := map[string]any{"cmd": "list_objects", "bucket": bucket}
	if prefix != nil {
		payload["prefix"] = *prefix
	}
	if limit != nil {
		payload["limit"] = *limit
	}
	data, err := c.checked(payload)
	if err != nil {
		return nil, err
	}
	return toMapSlice(data), nil
}

// ------------------------------------------------------------------
// Full-text search
// ------------------------------------------------------------------

// Search performs full-text search across blobs.
func (c *Client) Search(query string, bucket *string, limit int) ([]map[string]any, error) {
	payload := map[string]any{"cmd": "search", "query": query, "limit": limit}
	if bucket != nil {
		payload["bucket"] = *bucket
	}
	data, err := c.checked(payload)
	if err != nil {
		return nil, err
	}
	return toMapSlice(data), nil
}

// ------------------------------------------------------------------
// SQL
// ------------------------------------------------------------------

// SQL executes a SQL query. Supports SELECT, INSERT, UPDATE, DELETE,
// CREATE/DROP TABLE, CREATE INDEX, and SHOW TABLES.
func (c *Client) SQL(query string) (any, error) {
	return c.checked(map[string]any{"cmd": "sql", "query": query})
}

// ------------------------------------------------------------------
// Cron schedules
// ------------------------------------------------------------------

// CreateSchedule creates or replaces a named schedule.
// Pass a cron expression (e.g. "0 3 * * *") or an interval (e.g. "5m").
func (c *Client) CreateSchedule(name, procedure string, opts map[string]any) (map[string]any, error) {
	payload := map[string]any{"cmd": "create_schedule", "name": name, "procedure": procedure}
	for k, v := range opts {
		payload[k] = v
	}
	data, err := c.checked(payload)
	if err != nil {
		return nil, err
	}
	m, _ := data.(map[string]any)
	return m, nil
}

// ListSchedules lists all schedules with status.
func (c *Client) ListSchedules() ([]map[string]any, error) {
	data, err := c.checked(map[string]any{"cmd": "list_schedules"})
	if err != nil {
		return nil, err
	}
	return toMapSlice(data), nil
}

// GetSchedule gets a schedule by name.
func (c *Client) GetSchedule(name string) (map[string]any, error) {
	data, err := c.checked(map[string]any{"cmd": "get_schedule", "name": name})
	if err != nil {
		return nil, err
	}
	m, _ := data.(map[string]any)
	return m, nil
}

// DeleteSchedule deletes a schedule.
func (c *Client) DeleteSchedule(name string) error {
	_, err := c.checked(map[string]any{"cmd": "delete_schedule", "name": name})
	return err
}

// EnableSchedule enables a paused schedule.
func (c *Client) EnableSchedule(name string) error {
	_, err := c.checked(map[string]any{"cmd": "enable_schedule", "name": name})
	return err
}

// DisableSchedule pauses a schedule.
func (c *Client) DisableSchedule(name string) error {
	_, err := c.checked(map[string]any{"cmd": "disable_schedule", "name": name})
	return err
}

// ------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------

func toMapSlice(data any) []map[string]any {
	arr, _ := data.([]any)
	result := make([]map[string]any, 0, len(arr))
	for _, v := range arr {
		if m, ok := v.(map[string]any); ok {
			result = append(result, m)
		}
	}
	return result
}
