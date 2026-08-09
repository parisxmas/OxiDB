// Package oxidb provides a TCP client for oxidb-server.
//
// Protocol: each message is [4-byte little-endian length][payload].
// Server responds with OxiWire binary format or JSON.
package oxidb

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/parisxmas/OxiDB/clients/go/oxiwire"
)

// Client is a TCP client for oxidb-server. Thread-safe via mutex.
type Client struct {
	conn    net.Conn
	mu      sync.Mutex
	oxiwire bool // use OxiWire binary format (fastest)
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

// SetDeadline sets the read/write deadline on the underlying connection.
func (c *Client) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

// UseOxiWire enables OxiDB's custom binary wire protocol (fastest).
func (c *Client) UseOxiWire() {
	c.oxiwire = true
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

	var reqBytes []byte
	var err error
	if c.oxiwire {
		reqBytes = oxiwire.Marshal(payload)
	} else {
		reqBytes, err = json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("oxidb: marshal request: %w", err)
		}
	}
	if err := c.sendRaw(reqBytes); err != nil {
		return nil, fmt.Errorf("oxidb: send: %w", err)
	}
	respBytes, err := c.recvRaw()
	if err != nil {
		return nil, err
	}

	var resp map[string]any
	if c.oxiwire && oxiwire.IsOxiWire(respBytes) {
		ok, data, decErr := oxiwire.DecodeResponse(respBytes)
		if decErr != nil {
			return nil, fmt.Errorf("oxidb: decode oxiwire response: %w", decErr)
		}
		resp = map[string]any{"ok": ok, "data": data}
		if !ok {
			if errStr, isStr := data.(string); isStr {
				resp["error"] = errStr
			}
		}
	} else {
		err = json.Unmarshal(respBytes, &resp)
		if err != nil {
			return nil, fmt.Errorf("oxidb: unmarshal response: %w", err)
		}
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

// ServerInfo is the negotiated state returned by HELLO. See
// oxidb-server's hello.rs (ADR-0003 Phase 2) for the response shape.
type ServerInfo struct {
	Name                  string
	Version               string
	WireVersion           uint32
	SupportedWireVersions []uint32
	StableSurfaceVersion  string
	Features              []string
	ExperimentalFeatures  []string
	AuthMethods           []string
}

// Hello performs the OxiWire HELLO handshake: negotiates the wire
// version and learns which engine features the server has compiled in.
// Pre-auth, idempotent — safe to call at any point in the connection,
// does not affect auth state.
//
// Supported by oxidb-server v0.28.13+. Older servers will reject "hello"
// as an unknown command; the caller can treat that as "wire v1, unknown
// feature surface".
func (c *Client) Hello() (*ServerInfo, error) {
	resp, err := c.request(map[string]any{
		"cmd":           "hello",
		"wire_versions": []uint32{1},
	})
	if err != nil {
		return nil, err
	}
	ok, _ := resp["ok"].(bool)
	if !ok {
		errMsg, _ := resp["error"].(string)
		if errMsg == "" {
			errMsg = "hello failed"
		}
		return nil, &Error{Msg: errMsg}
	}
	server, _ := resp["server"].(map[string]any)
	if server == nil {
		return nil, fmt.Errorf("oxidb: hello: missing 'server' field in response")
	}
	info := &ServerInfo{}
	if v, ok := server["name"].(string); ok {
		info.Name = v
	}
	if v, ok := server["version"].(string); ok {
		info.Version = v
	}
	if v, ok := server["wire_version"].(float64); ok {
		info.WireVersion = uint32(v)
	}
	if v, ok := server["stable_surface_version"].(string); ok {
		info.StableSurfaceVersion = v
	}
	info.SupportedWireVersions = readU32Array(server["supported_wire_versions"])
	info.Features = readStringArray(server["features"])
	info.ExperimentalFeatures = readStringArray(server["experimental_features"])
	info.AuthMethods = readStringArray(server["auth_methods"])
	return info, nil
}

func readU32Array(v any) []uint32 {
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]uint32, 0, len(arr))
	for _, x := range arr {
		if n, ok := x.(float64); ok {
			out = append(out, uint32(n))
		}
	}
	return out
}

func readStringArray(v any) []string {
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, x := range arr {
		if s, ok := x.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// BucketFTSSize returns the bytes of indexed text attributable to a single
// blob bucket. Useful for per-tenant FTS storage accounting (DMS quota).
// Result is approximate for indexes written before the per-doc text_bytes
// field existed (server-side falls back to total_terms × estimate).
func (c *Client) BucketFTSSize(bucket string) (uint64, error) {
	data, err := c.checked(map[string]any{
		"cmd":    "bucket_fts_size",
		"bucket": bucket,
	})
	if err != nil {
		return 0, err
	}
	m, ok := data.(map[string]any)
	if !ok {
		return 0, fmt.Errorf("oxidb: bucket_fts_size: unexpected response shape: %T", data)
	}
	switch v := m["bytes"].(type) {
	case float64:
		return uint64(v), nil
	case int64:
		return uint64(v), nil
	case int:
		return uint64(v), nil
	}
	return 0, fmt.Errorf("oxidb: bucket_fts_size: missing 'bytes' field in %v", m)
}

// ProcStatus returns process self-metrics for the running oxidb-server:
// {cpu_percent, mem_rss_mb, threads, uptime_s}. cpu_percent is the
// average over the time since the previous call; the first call always
// returns 0.0. Cheap to invoke — server-side just samples kernel
// counters and updates an internal moving average.
func (c *Client) ProcStatus() (map[string]any, error) {
	data, err := c.checked(map[string]any{"cmd": "proc_status"})
	if err != nil {
		return nil, err
	}
	m, ok := data.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("oxidb: proc_status: unexpected response shape: %T", data)
	}
	return m, nil
}

// FtsStatus returns a snapshot of the FTS pipeline: queue depth,
// per-worker in-flight jobs, and a ring of recently completed/failed
// jobs. Locks the index for read while assembling.
func (c *Client) FtsStatus() (map[string]any, error) {
	data, err := c.checked(map[string]any{"cmd": "fts_status"})
	if err != nil {
		return nil, err
	}
	m, ok := data.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("oxidb: fts_status: unexpected response shape: %T", data)
	}
	return m, nil
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

// StorageOption configures a collection's storage shape for
// CreateCollectionWithOptions. Any option left unset falls back to the server
// default (in-RAM, compressed, auto-compaction on).
type StorageOption func(map[string]any)

// DiskFirst stores documents on disk (an mmap'd .bdat) keeping only the offset
// index resident, instead of the default in-RAM store.
func DiskFirst(v bool) StorageOption { return func(o map[string]any) { o["disk_first"] = v } }

// Compress zstd-compresses on-disk records. Ignored unless DiskFirst is set.
func Compress(v bool) StorageOption { return func(o map[string]any) { o["compress"] = v } }

// AutoCompact reclaims dead space automatically (disk-first only).
func AutoCompact(v bool) StorageOption { return func(o map[string]any) { o["auto_compact"] = v } }

// CompactMinBytes sets the floor below which a data file is never auto-compacted.
func CompactMinBytes(v uint64) StorageOption {
	return func(o map[string]any) { o["compact_min_bytes"] = v }
}

// CompactDeadRatio sets the dead-space fraction (0..1) that triggers compaction.
func CompactDeadRatio(v float64) StorageOption {
	return func(o map[string]any) { o["compact_dead_ratio"] = v }
}

// CreateCollectionWithOptions creates a collection with explicit per-collection
// storage options. The chosen shape is persisted, so the collection reopens the
// same way regardless of the server's environment. Unset options use the server
// defaults.
//
//	db.CreateCollectionWithOptions("events", oxidb.DiskFirst(true), oxidb.Compress(false))
func (c *Client) CreateCollectionWithOptions(name string, opts ...StorageOption) error {
	options := map[string]any{}
	for _, o := range opts {
		o(options)
	}
	_, err := c.checked(map[string]any{
		"cmd":        "create_collection_with_options",
		"collection": name,
		"options":    options,
	})
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

	// Fast path: OxiWire decodes directly to []map[string]any
	if c.oxiwire {
		return c.findOxiWire(payload)
	}

	data, err := c.checked(payload)
	if err != nil {
		return nil, err
	}
	return toMapSlice(data), nil
}

// findOxiWire is the fast path for Find using the OxiWire protocol.
func (c *Client) findOxiWire(payload map[string]any) ([]map[string]any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	reqBytes := oxiwire.Marshal(payload)
	if err := c.sendRaw(reqBytes); err != nil {
		return nil, fmt.Errorf("oxidb: send: %w", err)
	}
	respBytes, err := c.recvRaw()
	if err != nil {
		return nil, err
	}

	if !oxiwire.IsOxiWire(respBytes) {
		return nil, fmt.Errorf("oxidb: expected OxiWire response")
	}

	// Check for error response (status byte = 1)
	if len(respBytes) >= 2 && respBytes[1] != 0 {
		_, data, _ := oxiwire.DecodeResponse(respBytes)
		errMsg, _ := data.(string)
		if errMsg == "" {
			errMsg = "unknown error"
		}
		return nil, &Error{Msg: errMsg}
	}

	return oxiwire.DecodeDocArray(respBytes)
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

// FindAndModify atomically finds one document matching query, applies
// update to it, and returns the modified document (with its bumped
// _version) — or nil if nothing matched. Unlike Update + $inc, this is
// safe under concurrency: the find and the write are contiguous, so it
// is the correct primitive for counters such as a mailbox's IMAP
// UIDNEXT. A caller mutating a counter must always use FindAndModify,
// never plain Update.
func (c *Client) FindAndModify(collection string, query, update map[string]any) (map[string]any, error) {
	data, err := c.checked(map[string]any{
		"cmd": "find_and_modify", "collection": collection,
		"query": query, "update": update,
	})
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil // no document matched
	}
	if m, ok := data.(map[string]any); ok {
		return m, nil
	}
	return nil, &Error{Msg: fmt.Sprintf("find_and_modify: unexpected response %T", data)}
}

// ------------------------------------------------------------------
// WORM phase 2 (engine-level immutability)
// ------------------------------------------------------------------

// WormIndefinite is the sentinel for "no time expiry" — used as
// lockedUntilMicros when the operator wants the lock to outlive any
// wall-clock retention window (legal hold, indefinite WORM).
const WormIndefinite uint64 = 0xFFFFFFFFFFFFFFFF

// WormLock pins a single document at the engine layer. Subsequent
// Update / Delete / FindAndModify on (collection, docID) return an
// error from the server (Error{Msg: "document is WORM-locked ..."}).
// Idempotent on equal-value locks; refuses to LOWER an existing
// lock.
func (c *Client) WormLock(collection string, docID uint64, lockedUntilMicros uint64) error {
	_, err := c.checked(map[string]any{
		"cmd": "worm_lock", "collection": collection,
		"doc_id": docID, "locked_until_micros": lockedUntilMicros,
	})
	return err
}

// WormRelease clears the engine-level lock on (collection, docID).
// Admin-only operation at the wire layer.
func (c *Client) WormRelease(collection string, docID uint64) error {
	_, err := c.checked(map[string]any{
		"cmd": "worm_release", "collection": collection, "doc_id": docID,
	})
	return err
}

// WormStatus returns the locked_until_micros for a doc, or 0 if the
// doc is not currently locked. Read-only.
func (c *Client) WormStatus(collection string, docID uint64) (uint64, error) {
	data, err := c.checked(map[string]any{
		"cmd": "worm_status", "collection": collection, "doc_id": docID,
	})
	if err != nil {
		return 0, err
	}
	m, ok := data.(map[string]any)
	if !ok {
		return 0, &Error{Msg: fmt.Sprintf("worm_status: unexpected response %T", data)}
	}
	if v, ok := m["locked_until_micros"].(float64); ok {
		return uint64(v), nil
	}
	return 0, nil
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

// CreateGeoIndex creates a geospatial index on a field holding a point.
//
// The field may be GeoJSON ({"type":"Point","coordinates":[lon,lat]}),
// [lon, lat], or {"lat":..,"lon":..}. Anything else is skipped rather than
// rejected, so a collection where only some documents carry a location
// indexes cleanly. Needed for $near and $geoWithin.
func (c *Client) CreateGeoIndex(collection, field string) error {
	_, err := c.checked(map[string]any{"cmd": "create_geo_index", "collection": collection, "field": field})
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
// SQL engine (second engine, ADR-0010)
// ------------------------------------------------------------------

// SqlResult is the outcome of one SQL statement.
//
// Exactly one shape is populated, mirroring the wire response:
//   - SELECT:               Columns + Rows
//   - INSERT/UPDATE/DELETE: Affected
//   - CREATE/DROP:          Ddl = true
//   - BEGIN/COMMIT/ROLLBACK: Transaction = true
type SqlResult struct {
	Columns     []string `json:"columns,omitempty"`
	Rows        [][]any  `json:"rows,omitempty"`
	Affected    int64    `json:"affected,omitempty"`
	Ddl         bool     `json:"ddl,omitempty"`
	Transaction bool     `json:"transaction,omitempty"`
}

// Sql executes SQL against the server's standalone SQL engine and returns one
// result per statement.
//
// The SQL engine is separate from document collections (own tables, own
// files) and must be enabled on the server with OXIDB_SQL=1. `params`
// optionally binds `?` / `$N` placeholders left-to-right.
//
//	_, err := c.Sql("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
//	_, err = c.Sql("INSERT INTO users VALUES (?, ?)", 1, "ada")
//	res, err := c.Sql("SELECT name FROM users WHERE id = $1", 1)
//	// res[0].Columns == ["name"]; res[0].Rows == [["ada"]]
func (c *Client) Sql(query string, params ...any) ([]SqlResult, error) {
	payload := map[string]any{"engine": "sql", "cmd": "sql", "sql": query}
	if len(params) > 0 {
		payload["params"] = params
	}
	data, err := c.checked(payload)
	if err != nil {
		return nil, err
	}
	items, ok := data.([]any)
	if !ok {
		return nil, &Error{Msg: "unexpected sql response shape"}
	}
	results := make([]SqlResult, 0, len(items))
	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			return nil, &Error{Msg: "unexpected sql result shape"}
		}
		var r SqlResult
		if cols, ok := m["columns"].([]any); ok {
			for _, col := range cols {
				if s, ok := col.(string); ok {
					r.Columns = append(r.Columns, s)
				}
			}
			if rows, ok := m["rows"].([]any); ok {
				r.Rows = make([][]any, 0, len(rows))
				for _, row := range rows {
					if cells, ok := row.([]any); ok {
						r.Rows = append(r.Rows, cells)
					}
				}
			}
		}
		if n, ok := m["affected"].(float64); ok {
			r.Affected = int64(n)
		}
		if b, ok := m["ddl"].(bool); ok {
			r.Ddl = b
		}
		if b, ok := m["transaction"].(bool); ok {
			r.Transaction = b
		}
		results = append(results, r)
	}
	return results, nil
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
// Vector search
// ------------------------------------------------------------------

// CreateVectorIndex creates a vector similarity search index on a field.
// Metric can be "cosine", "euclidean", or "dot_product".
func (c *Client) CreateVectorIndex(collection, field string, dimension int, metric string) error {
	if metric == "" {
		metric = "cosine"
	}
	_, err := c.checked(map[string]any{
		"cmd": "create_vector_index", "collection": collection,
		"field": field, "dimension": dimension, "metric": metric,
	})
	return err
}

// VectorSearch finds the k nearest neighbors by vector similarity.
// Returns documents with _similarity and _distance fields.
func (c *Client) VectorSearch(collection, field string, vector []float64, limit int) ([]map[string]any, error) {
	if limit <= 0 {
		limit = 10
	}
	data, err := c.checked(map[string]any{
		"cmd": "vector_search", "collection": collection,
		"field": field, "vector": vector, "limit": limit,
	})
	if err != nil {
		return nil, err
	}
	return toMapSlice(data), nil
}

// ------------------------------------------------------------------
// Cron schedules
// ------------------------------------------------------------------

// CreateSchedule creates or replaces a named schedule.
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
// Database management
// ------------------------------------------------------------------

// CreateDatabase creates a new database.
func (c *Client) CreateDatabase(name string) error {
	_, err := c.checked(map[string]any{"cmd": "create_database", "name": name})
	return err
}

// DropDatabase drops a database. Cannot drop the default 'oxidb' database.
func (c *Client) DropDatabase(name string) error {
	_, err := c.checked(map[string]any{"cmd": "drop_database", "name": name})
	return err
}

// ListDatabases returns a list of database names.
func (c *Client) ListDatabases() ([]string, error) {
	data, err := c.checked(map[string]any{"cmd": "list_databases"})
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

// UseDatabase switches the current session to a different database.
func (c *Client) UseDatabase(name string) error {
	_, err := c.checked(map[string]any{"cmd": "use_db", "name": name})
	return err
}

// ------------------------------------------------------------------
// Authentication
// ------------------------------------------------------------------

// AuthSimple authenticates with username and password (simple auth).
func (c *Client) AuthSimple(username, password string) (string, error) {
	data, err := c.checked(map[string]any{
		"cmd": "auth_simple", "username": username, "password": password,
	})
	if err != nil {
		return "", err
	}
	m, _ := data.(map[string]any)
	role, _ := m["role"].(string)
	return role, nil
}

// ------------------------------------------------------------------
// User management (requires Admin)
// ------------------------------------------------------------------

// CreateUser creates a new user with the given role.
func (c *Client) CreateUser(username, password, role string) error {
	_, err := c.checked(map[string]any{
		"cmd": "create_user", "username": username,
		"password": password, "role": role,
	})
	return err
}

// DropUser removes a user.
func (c *Client) DropUser(username string) error {
	_, err := c.checked(map[string]any{"cmd": "drop_user", "username": username})
	return err
}

// UpdateUser updates a user's password and/or role.
func (c *Client) UpdateUser(username string, password *string, role *string) error {
	payload := map[string]any{"cmd": "update_user", "username": username}
	if password != nil {
		payload["password"] = *password
	}
	if role != nil {
		payload["role"] = *role
	}
	_, err := c.checked(payload)
	return err
}

// ListUsers returns all users (without password hashes).
func (c *Client) ListUsers() ([]map[string]any, error) {
	data, err := c.checked(map[string]any{"cmd": "list_users"})
	if err != nil {
		return nil, err
	}
	return toMapSlice(data), nil
}

// GrantDbRole grants a per-database role override for a user.
func (c *Client) GrantDbRole(username, database, role string) error {
	_, err := c.checked(map[string]any{
		"cmd": "grant_db_role", "username": username,
		"database": database, "role": role,
	})
	return err
}

// RevokeDbRole revokes a per-database role override from a user.
func (c *Client) RevokeDbRole(username, database string) error {
	_, err := c.checked(map[string]any{
		"cmd": "revoke_db_role", "username": username,
		"database": database,
	})
	return err
}

// ------------------------------------------------------------------
// Pipeline — execute multiple commands in one roundtrip
// ------------------------------------------------------------------

// Pipeline sends multiple commands to the server in a single roundtrip.
func (c *Client) Pipeline(commands []map[string]any) ([]any, error) {
	data, err := c.checked(map[string]any{"cmd": "pipeline", "commands": commands})
	if err != nil {
		return nil, err
	}
	arr, _ := data.([]any)
	return arr, nil
}

// PipelineInsertMany sends multiple insert_many commands in a single roundtrip.
func (c *Client) PipelineInsertMany(collection string, batches [][]map[string]any) (int, error) {
	commands := make([]map[string]any, len(batches))
	for i, batch := range batches {
		commands[i] = map[string]any{
			"cmd":        "insert_many",
			"collection": collection,
			"docs":       batch,
		}
	}
	results, err := c.Pipeline(commands)
	if err != nil {
		return 0, err
	}
	total := 0
	for _, r := range results {
		m, ok := r.(map[string]any)
		if !ok {
			continue
		}
		if okFlag, _ := m["ok"].(bool); !okFlag {
			errMsg, _ := m["error"].(string)
			return total, &Error{Msg: errMsg}
		}
		if data, ok := m["data"].([]any); ok {
			total += len(data)
		}
	}
	return total, nil
}

// ------------------------------------------------------------------
// Stored procedures
// ------------------------------------------------------------------

// CreateProcedure creates a stored procedure from a JSON definition.
// The definition should include "name", "body" (JavaScript), and optionally "params".
func (c *Client) CreateProcedure(name string, definition map[string]any) error {
	payload := map[string]any{"cmd": "create_procedure", "name": name}
	for k, v := range definition {
		payload[k] = v
	}
	_, err := c.checked(payload)
	return err
}

// CreateProcedureFromScript creates a stored procedure from OxiScript source.
func (c *Client) CreateProcedureFromScript(script string) error {
	_, err := c.checked(map[string]any{"cmd": "create_procedure", "script": script})
	return err
}

// CallProcedure executes a stored procedure by name with optional parameters.
func (c *Client) CallProcedure(name string, params map[string]any) (any, error) {
	payload := map[string]any{"cmd": "call_procedure", "name": name}
	if params != nil {
		payload["params"] = params
	}
	return c.checked(payload)
}

// ListProcedures returns a list of stored procedure names.
func (c *Client) ListProcedures() ([]string, error) {
	data, err := c.checked(map[string]any{"cmd": "list_procedures"})
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

// GetProcedure returns the definition of a stored procedure.
func (c *Client) GetProcedure(name string) (map[string]any, error) {
	data, err := c.checked(map[string]any{"cmd": "get_procedure", "name": name})
	if err != nil {
		return nil, err
	}
	m, _ := data.(map[string]any)
	return m, nil
}

// DeleteProcedure deletes a stored procedure by name.
func (c *Client) DeleteProcedure(name string) error {
	_, err := c.checked(map[string]any{"cmd": "delete_procedure", "name": name})
	return err
}

// CompileOxiScript compiles OxiScript source and returns the compiled definition.
func (c *Client) CompileOxiScript(script string) (map[string]any, error) {
	data, err := c.checked(map[string]any{"cmd": "compile_oxiscript", "script": script})
	if err != nil {
		return nil, err
	}
	m, _ := data.(map[string]any)
	return m, nil
}

// ------------------------------------------------------------------
// TTL indexes
// ------------------------------------------------------------------

// CreateTTLIndex creates a TTL index that automatically expires documents.
// The field should be a datetime field; documents are deleted expireAfterSeconds
// after the field value.
func (c *Client) CreateTTLIndex(collection, field string, expireAfterSeconds int) error {
	_, err := c.checked(map[string]any{
		"cmd": "create_ttl_index", "collection": collection,
		"field": field, "expireAfterSeconds": expireAfterSeconds,
	})
	return err
}

// ------------------------------------------------------------------
// Retention policies
// ------------------------------------------------------------------

// SetRetention sets a retention policy — documents older than days are auto-deleted.
// Creates a TTL index on the _ts field.
func (c *Client) SetRetention(collection string, days int) error {
	_, err := c.checked(map[string]any{
		"cmd": "set_retention", "collection": collection, "days": days,
	})
	return err
}

// GetRetention returns the retention policy for a collection.
func (c *Client) GetRetention(collection string) (map[string]any, error) {
	data, err := c.checked(map[string]any{"cmd": "get_retention", "collection": collection})
	if err != nil {
		return nil, err
	}
	m, _ := data.(map[string]any)
	return m, nil
}

// DeleteRetention removes the retention policy for a collection.
func (c *Client) DeleteRetention(collection string) error {
	_, err := c.checked(map[string]any{"cmd": "delete_retention", "collection": collection})
	return err
}

// ListRetentions returns all retention policies.
func (c *Client) ListRetentions() ([]map[string]any, error) {
	data, err := c.checked(map[string]any{"cmd": "list_retentions"})
	if err != nil {
		return nil, err
	}
	return toMapSlice(data), nil
}

// ------------------------------------------------------------------
// Alerting
// ------------------------------------------------------------------

// CreateAlert creates an alert rule that fires when a condition is met.
// Condition example: {"type": "count_threshold", "query": {"level": {"$lte": 3}},
//
//	"window": "5m", "threshold": 100, "operator": "gte"}
//
// Actions example: [{"type": "webhook", "url": "..."}, {"type": "stderr"}]
func (c *Client) CreateAlert(name, collection string, condition map[string]any, actions []map[string]any, cooldownSeconds int) error {
	if cooldownSeconds <= 0 {
		cooldownSeconds = 300
	}
	_, err := c.checked(map[string]any{
		"cmd": "create_alert", "name": name, "collection": collection,
		"condition": condition, "actions": actions,
		"cooldown_seconds": cooldownSeconds,
	})
	return err
}

// GetAlert returns an alert rule by name.
func (c *Client) GetAlert(name string) (map[string]any, error) {
	data, err := c.checked(map[string]any{"cmd": "get_alert", "name": name})
	if err != nil {
		return nil, err
	}
	m, _ := data.(map[string]any)
	return m, nil
}

// DeleteAlert deletes an alert rule by name.
func (c *Client) DeleteAlert(name string) error {
	_, err := c.checked(map[string]any{"cmd": "delete_alert", "name": name})
	return err
}

// ListAlerts returns all alert rules.
func (c *Client) ListAlerts() ([]map[string]any, error) {
	data, err := c.checked(map[string]any{"cmd": "list_alerts"})
	if err != nil {
		return nil, err
	}
	return toMapSlice(data), nil
}

// TestAlert evaluates an alert's condition immediately without firing actions.
// Returns the current value, threshold, and whether it would fire.
func (c *Client) TestAlert(name string) (map[string]any, error) {
	data, err := c.checked(map[string]any{"cmd": "test_alert", "name": name})
	if err != nil {
		return nil, err
	}
	m, _ := data.(map[string]any)
	return m, nil
}

// ListAlertHistory returns all fired alert events from _alert_history.
func (c *Client) ListAlertHistory() ([]map[string]any, error) {
	data, err := c.checked(map[string]any{"cmd": "list_alert_history"})
	if err != nil {
		return nil, err
	}
	return toMapSlice(data), nil
}

// ------------------------------------------------------------------
// Text extraction
// ------------------------------------------------------------------

// ExtractText extracts text from a blob object using OxiDB's built-in
// extractors (PDF, DOCX, HTML, etc.).
func (c *Client) ExtractText(bucket, key string) (string, error) {
	data, err := c.checked(map[string]any{"cmd": "extract_text", "bucket": bucket, "key": key})
	if err != nil {
		return "", err
	}
	m, _ := data.(map[string]any)
	text, _ := m["text"].(string)
	return text, nil
}

// ------------------------------------------------------------------
// Backup & Restore
// ------------------------------------------------------------------

// Backup creates a full database backup at the given path.
// Returns backup info with path, size_bytes, and collections.
func (c *Client) Backup(path string) (map[string]any, error) {
	data, err := c.checked(map[string]any{"cmd": "backup", "path": path})
	if err != nil {
		return nil, err
	}
	m, _ := data.(map[string]any)
	return m, nil
}

// Restore restores a database from a backup archive to a target directory.
// Returns restore info with path, collections, and a message.
func (c *Client) Restore(archive, target string) (map[string]any, error) {
	data, err := c.checked(map[string]any{"cmd": "restore", "archive": archive, "target": target})
	if err != nil {
		return nil, err
	}
	m, _ := data.(map[string]any)
	return m, nil
}

// RestoreToPoint performs a Point-In-Time Recovery: it extracts the base
// backup at baseBackup, then replays the PITR archive at archiveDir on top
// of it into targetDir, up to a chosen point. The point is selected via
// opts: {"gsn": <u64>} for an exact GSN, {"at_micros": <u64>} for a
// wall-clock cutoff (micros since the Unix epoch), or nil for the latest
// archived record. Returns info with path, collections, target_gsn,
// records_applied, and a message. Restart a server on targetDir to use it.
func (c *Client) RestoreToPoint(baseBackup, archiveDir, targetDir string, opts map[string]any) (map[string]any, error) {
	payload := map[string]any{
		"cmd":         "restore_to_point",
		"base_backup": baseBackup,
		"archive":     archiveDir,
		"target":      targetDir,
	}
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

// ArchiveStatus reports the PITR archive's segment count and GSN /
// wall-clock coverage.
func (c *Client) ArchiveStatus() (map[string]any, error) {
	data, err := c.checked(map[string]any{"cmd": "archive_status"})
	if err != nil {
		return nil, err
	}
	m, _ := data.(map[string]any)
	return m, nil
}

// ------------------------------------------------------------------
// SQL dialect
// ------------------------------------------------------------------

// SetDialect sets the SQL dialect for this session.
// Valid dialects: "mysql", "postgresql", "mssql", "generic".
func (c *Client) SetDialect(dialect string) error {
	_, err := c.checked(map[string]any{"cmd": "set_dialect", "dialect": dialect})
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

// ------------------------------------------------------------------
// Change streams
// ------------------------------------------------------------------

// ChangeEvent is one document change reported by [Client.Watch].
type ChangeEvent struct {
	// Token orders events and is what a later Watch passes as resumeAfter to
	// pick up where this one stopped.
	Token      uint64         `json:"token"`
	Operation  string         `json:"operation"` // Insert | Update | Delete
	Collection string         `json:"collection"`
	DocID      uint64         `json:"doc_id"`
	Document   map[string]any `json:"document"`
}

// WatchOverflow reports events the server dropped because the consumer fell
// behind. It is delivered in-band rather than silently: a gap in a change
// stream is the kind of thing a caller must decide about (usually by
// resynchronising), not something a library should hide.
type WatchOverflow struct {
	Dropped uint64 `json:"dropped"`
}

// Watch turns this connection into a change stream and blocks, calling onEvent
// for every change to collection (empty for all collections).
//
// The connection is dedicated for the duration: watch is a mode, not a
// request, so the client must not be shared with ordinary commands while it
// runs. Pass resumeAfter to continue after a known token, or 0 to start from
// now. Returns when ctx is cancelled, the server closes, or onEvent fails.
//
// Requires the Admin role when the server has auth enabled.
func (c *Client) Watch(
	ctx context.Context,
	collection string,
	resumeAfter uint64,
	onEvent func(ChangeEvent) error,
	onOverflow func(WatchOverflow),
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	req := map[string]any{"cmd": "watch"}
	if collection != "" {
		req["collection"] = collection
	}
	if resumeAfter > 0 {
		req["resume_after"] = resumeAfter
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return err
	}
	if err := c.sendRaw(payload); err != nil {
		return fmt.Errorf("oxidb: watch: %w", err)
	}

	// Cancellation closes the socket: the read below is blocking, and there is
	// no in-band way to interrupt it.
	if ctx != nil {
		stop := make(chan struct{})
		defer close(stop)
		go func() {
			select {
			case <-ctx.Done():
				_ = c.conn.Close()
			case <-stop:
			}
		}()
	}

	for {
		raw, err := c.recvRaw()
		if err != nil {
			if ctx != nil && ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		var msg struct {
			Event string          `json:"event"`
			Data  json.RawMessage `json:"data"`
			OK    *bool           `json:"ok"`
			Error string          `json:"error"`
		}
		if err := json.Unmarshal(raw, &msg); err != nil {
			return fmt.Errorf("oxidb: watch decode: %w", err)
		}
		switch {
		case msg.Error != "":
			return fmt.Errorf("oxidb: watch: %s", msg.Error)
		case msg.Event == "change":
			var ev ChangeEvent
			if err := json.Unmarshal(msg.Data, &ev); err != nil {
				return fmt.Errorf("oxidb: watch event: %w", err)
			}
			if err := onEvent(ev); err != nil {
				return err
			}
		case msg.Event == "overflow":
			var ov WatchOverflow
			if err := json.Unmarshal(msg.Data, &ov); err == nil && onOverflow != nil {
				onOverflow(ov)
			}
		}
	}
}
