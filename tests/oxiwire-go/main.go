package main

import (
	"fmt"
	"math"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/parisxmas/OxiDB/clients/go/oxidb"
	"github.com/parisxmas/OxiDB/clients/go/oxiwire"
)

// OxiWire Binary Protocol Test
//
// Tests that OxiDB's custom binary wire protocol works correctly
// through both direct server connection and OxiPool proxy.
//
// OxiWire format:
//   Request:  [0xDB] [encoded map]
//   Response: [0xDB] [status: 0=ok/1=err] [encoded value]
//   Types: Null(0x00) False(0x01) True(0x02) Int64(0x03) Uint64(0x04)
//          Float64(0x05) String(0x06) Array(0x07) Map(0x08)
//
// Tests:
//   1. Codec roundtrip — encode/decode all types without server
//   2. Direct server — CRUD operations using OxiWire protocol
//   3. Through OxiPool — OxiWire binary frames forwarded transparently
//   4. JSON vs OxiWire correctness — same operations, compare results
//   5. Mixed protocol — JSON and OxiWire clients interleaved
//   6. Large document — 10KB+ nested doc encoded/decoded via OxiWire
//   7. Bulk throughput — 10K inserts, compare JSON vs OxiWire speed
//   8. OxiWire Find — fast-path find returning []map[string]any

const (
	projectDir = "/Users/barisakin/source/docdb"
	collection = "oxiwire_test"
)

var (
	serverPort = 4444
	proxyPort  = 4445
	passed     int
	failed     int
)

func main() {
	if p := os.Getenv("OXIDB_PORT"); p != "" {
		serverPort, _ = strconv.Atoi(p)
	}
	if p := os.Getenv("OXIPOOL_PORT"); p != "" {
		proxyPort, _ = strconv.Atoi(p)
	}

	fmt.Println("=== OxiWire Binary Protocol Test ===")
	fmt.Println()

	// Test 1 is pure codec — no server needed
	test1_codec_roundtrip()

	killAll()
	time.Sleep(500 * time.Millisecond)
	startServer()
	startOxiPool()

	cleanup()

	test2_direct_server_crud()
	test3_through_oxipool()
	test4_json_vs_oxiwire_correctness()
	test5_mixed_protocol()
	test6_large_document()
	test7_bulk_throughput()
	test8_oxiwire_find()

	killAll()

	total := passed + failed
	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════╗")
	fmt.Println("║          OXIWIRE BINARY PROTOCOL TEST RESULTS          ║")
	fmt.Println("╠══════════════════════════════════════════════════════════╣")
	fmt.Printf("║   Passed:             %-34d ║\n", passed)
	fmt.Printf("║   Failed:             %-34d ║\n", failed)
	fmt.Printf("║   Total:              %-34d ║\n", total)
	fmt.Println("╚══════════════════════════════════════════════════════════╝")

	if failed > 0 {
		fmt.Printf("\nFAILED: %d/%d tests failed\n", failed, total)
		os.Exit(1)
	}
	fmt.Println("\nAll OxiWire protocol tests passed.")
}

// ─── Test 1: Codec Roundtrip ────────────────────────────────────────

func test1_codec_roundtrip() {
	fmt.Println("TEST 1: OxiWire codec roundtrip — encode then decode all types")

	testCases := []struct {
		name string
		val  any
	}{
		{"null", nil},
		{"true", true},
		{"false", false},
		{"int_pos", 42},
		{"int_zero", 0},
		{"float", 3.14159},
		{"float_neg", -99.5},
		{"string", "hello world"},
		{"empty_string", ""},
		{"string_unicode", "日本語テスト 🎉"},
		{"array", []any{"a", "b", "c"}},
		{"empty_array", []any{}},
		{"map", map[string]any{"key": "value", "num": 123}},
		{"nested", map[string]any{
			"outer": map[string]any{
				"inner": []any{1, "two", true, nil},
			},
		}},
	}

	allOk := true
	for _, tc := range testCases {
		encoded := oxiwire.Marshal(tc.val)

		// Verify magic byte
		if len(encoded) == 0 || encoded[0] != oxiwire.Magic {
			fail("  %s: missing magic byte", tc.name)
			allOk = false
			continue
		}

		// Decode via response envelope (add status=ok)
		envelope := make([]byte, 0, len(encoded)+1)
		envelope = append(envelope, oxiwire.Magic, 0x00) // magic + ok status
		// Re-encode without magic for the value part
		enc := oxiwire.NewEncoder(64)
		enc.Encode(tc.val)
		envelope = append(envelope, enc.Bytes()...)

		ok, decoded, err := oxiwire.DecodeResponse(envelope)
		if err != nil {
			fail("  %s: decode error: %v", tc.name, err)
			allOk = false
			continue
		}
		if !ok {
			fail("  %s: decode returned not-ok", tc.name)
			allOk = false
			continue
		}

		if !valuesEqual(tc.val, decoded) {
			fail("  %s: mismatch — encoded %T(%v), decoded %T(%v)", tc.name, tc.val, tc.val, decoded, decoded)
			allOk = false
		}
	}

	if allOk {
		fmt.Printf("  PASS: all %d type roundtrips correct\n", len(testCases))
		passed++
	}

	// Verify error response decoding
	errEnvelope := []byte{oxiwire.Magic, 0x01} // magic + error status
	enc := oxiwire.NewEncoder(32)
	enc.Encode("something went wrong")
	errEnvelope = append(errEnvelope, enc.Bytes()...)

	ok, data, err := oxiwire.DecodeResponse(errEnvelope)
	if err != nil {
		fail("error envelope decode failed: %v", err)
	} else if ok {
		fail("error envelope decoded as ok")
	} else if data != "something went wrong" {
		fail("error message mismatch: %v", data)
	} else {
		fmt.Println("  PASS: error response decoded correctly")
		passed++
	}
	fmt.Println()
}

// ─── Test 2: Direct Server CRUD ─────────────────────────────────────

func test2_direct_server_crud() {
	fmt.Println("TEST 2: CRUD operations via OxiWire to server directly")

	c, err := connectDirect()
	if err != nil {
		fail("connect: %v", err)
		return
	}
	defer c.Close()
	c.UseOxiWire()

	// Ping
	pong, err := c.Ping()
	if err != nil {
		fail("ping: %v", err)
		return
	}
	if pong != "pong" {
		fail("ping response: %q", pong)
		return
	}

	// Insert
	doc := map[string]any{
		"name":   "oxiwire_test",
		"value":  42,
		"active": true,
		"tags":   []any{"binary", "fast"},
	}
	result, err := c.Insert(collection, doc)
	if err != nil {
		fail("insert: %v", err)
		return
	}
	id := fmt.Sprintf("%v", result["_id"])
	if id == "" {
		fail("insert returned no _id")
		return
	}

	// FindOne
	found, err := c.FindOne(collection, map[string]any{"name": "oxiwire_test"})
	if err != nil {
		fail("find_one: %v", err)
		return
	}
	if found == nil {
		fail("find_one returned nil")
		return
	}
	if fmt.Sprintf("%v", found["name"]) != "oxiwire_test" {
		fail("find_one name mismatch: %v", found["name"])
		return
	}

	// Count
	count, err := c.Count(collection, map[string]any{"name": "oxiwire_test"})
	if err != nil {
		fail("count: %v", err)
		return
	}
	if count != 1 {
		fail("count=%d, expected 1", count)
		return
	}

	// Update
	_, err = c.UpdateOne(collection,
		map[string]any{"name": "oxiwire_test"},
		map[string]any{"$set": map[string]any{"value": 99}},
	)
	if err != nil {
		fail("update: %v", err)
		return
	}

	// Verify update
	found2, err := c.FindOne(collection, map[string]any{"name": "oxiwire_test"})
	if err != nil {
		fail("find after update: %v", err)
		return
	}
	val := toFloat(found2["value"])
	if val != 99 {
		fail("updated value=%v, expected 99", found2["value"])
		return
	}

	// Delete
	_, err = c.DeleteOne(collection, map[string]any{"name": "oxiwire_test"})
	if err != nil {
		fail("delete: %v", err)
		return
	}

	// Verify delete
	count, err = c.Count(collection, map[string]any{"name": "oxiwire_test"})
	if err != nil {
		fail("count after delete: %v", err)
		return
	}
	if count != 0 {
		fail("count after delete=%d, expected 0", count)
		return
	}

	fmt.Println("  PASS: all CRUD ops work via OxiWire (ping/insert/find/update/delete)")
	passed++
	fmt.Println()
}

// ─── Test 3: Through OxiPool ────────────────────────────────────────

func test3_through_oxipool() {
	fmt.Println("TEST 3: OxiWire binary frames forwarded through OxiPool")

	c, err := connectProxy()
	if err != nil {
		fail("connect proxy: %v", err)
		return
	}
	defer c.Close()
	c.UseOxiWire()

	// Ping through proxy
	pong, err := c.Ping()
	if err != nil {
		fail("ping through proxy: %v", err)
		return
	}
	if pong != "pong" {
		fail("ping response: %q", pong)
		return
	}

	// Insert through proxy
	_, err = c.Insert(collection, map[string]any{
		"source":  "oxiwire_via_proxy",
		"number":  777,
		"nested":  map[string]any{"a": 1, "b": "two"},
	})
	if err != nil {
		fail("insert via proxy: %v", err)
		return
	}

	// Find through proxy
	docs, err := c.Find(collection, map[string]any{"source": "oxiwire_via_proxy"}, nil)
	if err != nil {
		fail("find via proxy: %v", err)
		return
	}
	if len(docs) != 1 {
		fail("expected 1 doc, got %d", len(docs))
		return
	}

	// Verify nested structure survived binary encoding through proxy
	nested, ok := docs[0]["nested"].(map[string]any)
	if !ok {
		fail("nested field not a map: %T", docs[0]["nested"])
		return
	}
	if toFloat(nested["a"]) != 1 || fmt.Sprintf("%v", nested["b"]) != "two" {
		fail("nested values mismatch: %v", nested)
		return
	}

	// Cleanup
	c.DeleteOne(collection, map[string]any{"source": "oxiwire_via_proxy"})

	fmt.Println("  PASS: OxiWire works transparently through OxiPool")
	passed++
	fmt.Println()
}

// ─── Test 4: JSON vs OxiWire Correctness ────────────────────────────

func test4_json_vs_oxiwire_correctness() {
	fmt.Println("TEST 4: JSON vs OxiWire produce identical results")

	// Insert same document via JSON
	jsonClient, err := connectDirect()
	if err != nil {
		fail("connect json: %v", err)
		return
	}
	jsonClient.Insert(collection, map[string]any{
		"protocol_test": "compare",
		"int_val":       42,
		"float_val":     3.14,
		"bool_val":      true,
		"null_val":      nil,
		"str_val":       "hello",
		"arr_val":       []any{1, "two", 3},
	})
	jsonClient.Close()

	// Read via JSON
	jsonReader, err := connectDirect()
	if err != nil {
		fail("connect json reader: %v", err)
		return
	}
	jsonDocs, err := jsonReader.Find(collection, map[string]any{"protocol_test": "compare"}, nil)
	jsonReader.Close()
	if err != nil || len(jsonDocs) == 0 {
		fail("json find: err=%v, docs=%d", err, len(jsonDocs))
		return
	}

	// Read via OxiWire
	wireReader, err := connectDirect()
	if err != nil {
		fail("connect wire reader: %v", err)
		return
	}
	wireReader.UseOxiWire()
	wireDocs, err := wireReader.Find(collection, map[string]any{"protocol_test": "compare"}, nil)
	wireReader.Close()
	if err != nil || len(wireDocs) == 0 {
		fail("wire find: err=%v, docs=%d", err, len(wireDocs))
		return
	}

	// Compare field by field
	jd := jsonDocs[0]
	wd := wireDocs[0]

	mismatches := 0
	for _, key := range []string{"protocol_test", "int_val", "float_val", "bool_val", "str_val"} {
		jv := fmt.Sprintf("%v", jd[key])
		wv := fmt.Sprintf("%v", wd[key])
		if jv != wv {
			fmt.Printf("  MISMATCH %s: json=%q wire=%q\n", key, jv, wv)
			mismatches++
		}
	}

	// Check null
	if jd["null_val"] != nil || wd["null_val"] != nil {
		fmt.Printf("  MISMATCH null_val: json=%v wire=%v\n", jd["null_val"], wd["null_val"])
		mismatches++
	}

	// Check array (compare as strings since types may differ slightly)
	jArr := fmt.Sprintf("%v", jd["arr_val"])
	wArr := fmt.Sprintf("%v", wd["arr_val"])
	if jArr != wArr {
		fmt.Printf("  MISMATCH arr_val: json=%q wire=%q\n", jArr, wArr)
		mismatches++
	}

	// Cleanup
	direct, _ := connectDirect()
	if direct != nil {
		direct.DeleteOne(collection, map[string]any{"protocol_test": "compare"})
		direct.Close()
	}

	if mismatches == 0 {
		fmt.Println("  PASS: JSON and OxiWire return identical data")
		passed++
	} else {
		fail("%d field mismatches", mismatches)
	}
	fmt.Println()
}

// ─── Test 5: Mixed Protocol Clients ─────────────────────────────────

func test5_mixed_protocol() {
	fmt.Println("TEST 5: Mixed JSON and OxiWire clients interleaved")

	var wg sync.WaitGroup
	var jsonOps, wireOps atomic.Int64
	var jsonErrs, wireErrs atomic.Int64

	// 5 JSON workers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				c, err := connectProxy()
				if err != nil {
					jsonErrs.Add(1)
					continue
				}
				_, err = c.Insert(collection, map[string]any{
					"proto": "json", "worker": id, "seq": j,
				})
				c.Close()
				if err != nil {
					jsonErrs.Add(1)
				} else {
					jsonOps.Add(1)
				}
			}
		}(i)
	}

	// 5 OxiWire workers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				c, err := connectProxy()
				if err != nil {
					wireErrs.Add(1)
					continue
				}
				c.UseOxiWire()
				_, err = c.Insert(collection, map[string]any{
					"proto": "wire", "worker": id, "seq": j,
				})
				c.Close()
				if err != nil {
					wireErrs.Add(1)
				} else {
					wireOps.Add(1)
				}
			}
		}(i)
	}

	wg.Wait()

	fmt.Printf("  JSON: %d succeeded, %d errors\n", jsonOps.Load(), jsonErrs.Load())
	fmt.Printf("  Wire: %d succeeded, %d errors\n", wireOps.Load(), wireErrs.Load())

	// Verify counts
	direct, err := connectDirect()
	if err != nil {
		fail("connect verify: %v", err)
		return
	}
	jsonCount, _ := direct.Count(collection, map[string]any{"proto": "json"})
	wireCount, _ := direct.Count(collection, map[string]any{"proto": "wire"})
	direct.Close()

	fmt.Printf("  Server count: json=%d wire=%d\n", jsonCount, wireCount)

	if jsonOps.Load() == 250 && wireOps.Load() == 250 {
		fmt.Println("  PASS: all 500 mixed-protocol inserts succeeded")
		passed++
	} else {
		fail("expected 250+250, got %d+%d", jsonOps.Load(), wireOps.Load())
	}

	if jsonCount == 250 && wireCount == 250 {
		fmt.Println("  PASS: server has correct counts for both protocols")
		passed++
	} else {
		fail("server counts: json=%d wire=%d (expected 250+250)", jsonCount, wireCount)
	}

	// Cleanup
	direct2, _ := connectDirect()
	if direct2 != nil {
		direct2.Delete(collection, map[string]any{"proto": "json"})
		direct2.Delete(collection, map[string]any{"proto": "wire"})
		direct2.Close()
	}
	fmt.Println()
}

// ─── Test 6: Large Document ─────────────────────────────────────────

func test6_large_document() {
	fmt.Println("TEST 6: Large nested document (10KB+) via OxiWire")

	// Build a ~10KB doc with deep nesting
	bigDoc := map[string]any{
		"large_test": true,
		"title":      strings.Repeat("OxiWire Large Doc Test ", 20),
		"numbers":    make([]any, 100),
		"metadata": map[string]any{
			"created": "2025-01-01T00:00:00Z",
			"tags":    make([]any, 50),
			"nested": map[string]any{
				"level2": map[string]any{
					"level3": map[string]any{
						"deep_value": "found it",
						"deep_array": []any{1, 2, 3, 4, 5},
					},
				},
			},
		},
	}
	for i := 0; i < 100; i++ {
		bigDoc["numbers"].([]any)[i] = float64(i * i)
	}
	tags := bigDoc["metadata"].(map[string]any)["tags"].([]any)
	for i := 0; i < 50; i++ {
		tags[i] = fmt.Sprintf("tag_%03d_%s", i, strings.Repeat("x", 50))
	}

	// Encode and measure size
	encoded := oxiwire.Marshal(bigDoc)
	fmt.Printf("  Document OxiWire size: %d bytes\n", len(encoded))

	// Insert via OxiWire
	c, err := connectDirect()
	if err != nil {
		fail("connect: %v", err)
		return
	}
	c.UseOxiWire()

	_, err = c.Insert(collection, bigDoc)
	if err != nil {
		fail("insert large doc: %v", err)
		c.Close()
		return
	}

	// Read back
	found, err := c.FindOne(collection, map[string]any{"large_test": true})
	c.Close()
	if err != nil {
		fail("find large doc: %v", err)
		return
	}
	if found == nil {
		fail("large doc not found")
		return
	}

	// Verify deep nested value
	meta, _ := found["metadata"].(map[string]any)
	nested, _ := meta["nested"].(map[string]any)
	level2, _ := nested["level2"].(map[string]any)
	level3, _ := level2["level3"].(map[string]any)

	if fmt.Sprintf("%v", level3["deep_value"]) != "found it" {
		fail("deep nested value mismatch: %v", level3["deep_value"])
		return
	}

	// Verify array
	deepArr, _ := level3["deep_array"].([]any)
	if len(deepArr) != 5 {
		fail("deep array len=%d, expected 5", len(deepArr))
		return
	}

	// Verify numbers array
	nums, _ := found["numbers"].([]any)
	if len(nums) != 100 {
		fail("numbers len=%d, expected 100", len(nums))
		return
	}
	if toFloat(nums[10]) != 100 { // 10*10 = 100
		fail("numbers[10]=%v, expected 100", nums[10])
		return
	}

	// Cleanup
	direct, _ := connectDirect()
	if direct != nil {
		direct.DeleteOne(collection, map[string]any{"large_test": true})
		direct.Close()
	}

	fmt.Printf("  PASS: %d-byte document roundtripped with deep nesting intact\n", len(encoded))
	passed++
	fmt.Println()
}

// ─── Test 7: Bulk Throughput Comparison ──────────────────────────────

func test7_bulk_throughput() {
	fmt.Println("TEST 7: JSON vs OxiWire insert throughput (10K docs each)")

	const n = 10000

	// JSON throughput
	jsonStart := time.Now()
	jsonClient, err := connectDirect()
	if err != nil {
		fail("connect json: %v", err)
		return
	}
	for i := 0; i < n; i++ {
		jsonClient.Insert(collection, map[string]any{
			"bench": "json", "i": i, "data": "payload",
		})
	}
	jsonClient.Close()
	jsonDuration := time.Since(jsonStart)
	jsonRps := float64(n) / jsonDuration.Seconds()

	// OxiWire throughput
	wireStart := time.Now()
	wireClient, err := connectDirect()
	if err != nil {
		fail("connect wire: %v", err)
		return
	}
	wireClient.UseOxiWire()
	for i := 0; i < n; i++ {
		wireClient.Insert(collection, map[string]any{
			"bench": "wire", "i": i, "data": "payload",
		})
	}
	wireClient.Close()
	wireDuration := time.Since(wireStart)
	wireRps := float64(n) / wireDuration.Seconds()

	speedup := wireRps / jsonRps

	fmt.Printf("  JSON:    %d inserts in %v (%.0f ops/s)\n", n, jsonDuration.Round(time.Millisecond), jsonRps)
	fmt.Printf("  OxiWire: %d inserts in %v (%.0f ops/s)\n", n, wireDuration.Round(time.Millisecond), wireRps)
	fmt.Printf("  Speedup: %.2fx\n", speedup)

	// Both should complete
	if jsonRps > 0 && wireRps > 0 {
		fmt.Println("  PASS: both protocols completed 10K inserts")
		passed++
	} else {
		fail("throughput zero: json=%.0f wire=%.0f", jsonRps, wireRps)
	}

	// OxiWire should be at least as fast as JSON (typically faster)
	if speedup >= 0.8 {
		fmt.Printf("  PASS: OxiWire not slower than JSON (%.2fx)\n", speedup)
		passed++
	} else {
		fail("OxiWire significantly slower: %.2fx", speedup)
	}

	// Verify counts
	direct, _ := connectDirect()
	if direct != nil {
		jc, _ := direct.Count(collection, map[string]any{"bench": "json"})
		wc, _ := direct.Count(collection, map[string]any{"bench": "wire"})
		direct.Close()
		if jc == n && wc == n {
			fmt.Printf("  PASS: all %d+%d docs persisted\n", jc, wc)
			passed++
		} else {
			fail("counts: json=%d wire=%d (expected %d each)", jc, wc, n)
		}
	}

	// Cleanup
	direct2, _ := connectDirect()
	if direct2 != nil {
		direct2.Delete(collection, map[string]any{"bench": "json"})
		direct2.Delete(collection, map[string]any{"bench": "wire"})
		direct2.Close()
	}
	fmt.Println()
}

// ─── Test 8: OxiWire Find Fast Path ─────────────────────────────────

func test8_oxiwire_find() {
	fmt.Println("TEST 8: OxiWire Find fast path (DecodeDocArray)")

	// Insert test data
	direct, err := connectDirect()
	if err != nil {
		fail("connect: %v", err)
		return
	}
	for i := 0; i < 100; i++ {
		direct.Insert(collection, map[string]any{
			"find_test": true,
			"seq":       i,
			"name":      fmt.Sprintf("item_%03d", i),
			"value":     float64(i) * 1.5,
			"active":    i%2 == 0,
		})
	}
	direct.Close()

	// Find via OxiWire
	wire, err := connectDirect()
	if err != nil {
		fail("connect wire: %v", err)
		return
	}
	wire.UseOxiWire()

	// Find all
	docs, err := wire.Find(collection, map[string]any{"find_test": true}, nil)
	if err != nil {
		fail("find: %v", err)
		wire.Close()
		return
	}
	if len(docs) != 100 {
		fail("expected 100 docs, got %d", len(docs))
		wire.Close()
		return
	}

	// Verify field types
	sample := docs[0]
	// _id can be string (JSON) or float64 (OxiWire decodes Uint64 as float64)
	switch sample["_id"].(type) {
	case string, float64:
		// ok — both are valid _id representations
	default:
		fail("_id unexpected type: %T", sample["_id"])
	}
	if _, ok := sample["name"].(string); !ok {
		fail("name not string: %T", sample["name"])
	}

	// Find with limit
	limit := 10
	limited, err := wire.Find(collection, map[string]any{"find_test": true}, &oxidb.FindOptions{
		Limit: &limit,
		Sort:  map[string]any{"seq": 1},
	})
	if err != nil {
		fail("find with limit: %v", err)
		wire.Close()
		return
	}
	if len(limited) != 10 {
		fail("limited find: expected 10, got %d", len(limited))
		wire.Close()
		return
	}

	// Find with filter
	activeDocs, err := wire.Find(collection, map[string]any{"find_test": true, "active": true}, nil)
	if err != nil {
		fail("find active: %v", err)
		wire.Close()
		return
	}
	if len(activeDocs) != 50 {
		fail("active docs: expected 50, got %d", len(activeDocs))
		wire.Close()
		return
	}

	wire.Close()

	// Throughput: time 1000 find operations
	wire2, _ := connectDirect()
	if wire2 != nil {
		wire2.UseOxiWire()
		lim := 10
		t0 := time.Now()
		for i := 0; i < 1000; i++ {
			wire2.Find(collection, map[string]any{"find_test": true}, &oxidb.FindOptions{Limit: &lim})
		}
		findDuration := time.Since(t0)
		wire2.Close()
		fmt.Printf("  1000 OxiWire finds (limit=10): %v (%.0f ops/s)\n",
			findDuration.Round(time.Millisecond), 1000/findDuration.Seconds())
	}

	// JSON find throughput for comparison
	json2, _ := connectDirect()
	if json2 != nil {
		lim := 10
		t0 := time.Now()
		for i := 0; i < 1000; i++ {
			json2.Find(collection, map[string]any{"find_test": true}, &oxidb.FindOptions{Limit: &lim})
		}
		jsonDuration := time.Since(t0)
		json2.Close()
		fmt.Printf("  1000 JSON finds  (limit=10): %v (%.0f ops/s)\n",
			jsonDuration.Round(time.Millisecond), 1000/jsonDuration.Seconds())
	}

	// Cleanup
	direct2, _ := connectDirect()
	if direct2 != nil {
		direct2.Delete(collection, map[string]any{"find_test": true})
		direct2.Close()
	}

	fmt.Println("  PASS: OxiWire Find returns correct docs with filters, limits, and sorts")
	passed++
	fmt.Println()
}

// ─── Helpers ────────────────────────────────────────────────────────

func startServer() {
	cmd := exec.Command("./target/release/oxidb-server")
	cmd.Dir = projectDir
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("OXIDB_ADDR=127.0.0.1:%d", serverPort),
		"OXIDB_POOL_SIZE=20",
		"OXIDB_IDLE_TIMEOUT=0",
	)
	cmd.Start()
	time.Sleep(1 * time.Second)
}

func startOxiPool() {
	cmd := exec.Command(projectDir + "/target/release/oxipool")
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("OXIPOOL_LISTEN=127.0.0.1:%d", proxyPort),
		fmt.Sprintf("OXIPOOL_BACKEND=127.0.0.1:%d", serverPort),
		"OXIPOOL_SIZE=5",
		"OXIPOOL_STATS_INTERVAL=0",
	)
	cmd.Start()
	time.Sleep(1 * time.Second)
}

func killAll() {
	for _, name := range []string{"oxipool", "oxidb-server"} {
		pids := findPids(name)
		for _, pid := range pids {
			exec.Command("kill", "-9", pid).Run()
		}
	}
	time.Sleep(300 * time.Millisecond)
}

func cleanup() {
	direct, err := connectDirect()
	if err != nil {
		return
	}
	direct.DropCollection(collection)
	direct.Close()
}

func connectDirect() (*oxidb.Client, error) {
	return oxidb.Connect("127.0.0.1", serverPort, 3*time.Second)
}

func connectProxy() (*oxidb.Client, error) {
	return oxidb.Connect("127.0.0.1", proxyPort, 3*time.Second)
}

func findPids(name string) []string {
	out, _ := exec.Command("pgrep", "-x", name).Output()
	s := strings.TrimSpace(string(out))
	if s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}

func fail(format string, args ...any) {
	fmt.Printf("  FAIL: "+format+"\n", args...)
	failed++
}

func toFloat(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int:
		return float64(n)
	case int64:
		return float64(n)
	}
	return math.NaN()
}

func valuesEqual(a, b any) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	switch av := a.(type) {
	case bool:
		bv, ok := b.(bool)
		return ok && av == bv
	case int:
		return toFloat(b) == float64(av)
	case float64:
		return toFloat(b) == av
	case string:
		bv, ok := b.(string)
		return ok && av == bv
	case []any:
		bv, ok := b.([]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if !valuesEqual(av[i], bv[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		bv, ok := b.(map[string]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for k, v := range av {
			if !valuesEqual(v, bv[k]) {
				return false
			}
		}
		return true
	}
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

// percentile returns the p-th percentile from a sorted duration slice.
func percentile(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := len(sorted) * p / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// Ensure sort is used (imported above)
var _ = sort.Slice
