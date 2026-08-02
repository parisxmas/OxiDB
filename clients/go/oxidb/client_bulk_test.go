package oxidb_test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/parisxmas/OxiDB/go/oxidb"
)

// ------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------

const bulkCollection = "go_bulk_test"

func setupBulk(t *testing.T) *oxidb.Client {
	t.Helper()
	c := getClient(t)
	_ = c.DropCollection(bulkCollection)
	return c
}

func teardownBulk(t *testing.T, c *oxidb.Client) {
	t.Helper()
	_ = c.DropCollection(bulkCollection)
	c.Close()
}

// ------------------------------------------------------------------
// Bulk insert tests
// ------------------------------------------------------------------

func TestBulkInsert_Single(t *testing.T) {
	c := setupBulk(t)
	defer teardownBulk(t, c)

	const N = 500
	docs := make([]map[string]any, N)
	for i := range docs {
		docs[i] = map[string]any{
			"idx":    i,
			"name":   fmt.Sprintf("user_%04d", i),
			"email":  fmt.Sprintf("user%04d@test.com", i),
			"age":    20 + (i % 50),
			"active": i%2 == 0,
		}
	}

	result, err := c.InsertMany(bulkCollection, docs)
	if err != nil {
		t.Fatalf("insert_many: %v", err)
	}

	// Should return array of IDs
	ids, ok := result.([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", result)
	}
	if len(ids) != N {
		t.Fatalf("expected %d ids, got %d", N, len(ids))
	}

	count, err := c.Count(bulkCollection, map[string]any{})
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != N {
		t.Fatalf("expected count=%d, got %d", N, count)
	}
}

func TestBulkInsert_MultipleBatches(t *testing.T) {
	c := setupBulk(t)
	defer teardownBulk(t, c)

	const batchSize = 200
	const batches = 5
	total := 0

	for b := 0; b < batches; b++ {
		docs := make([]map[string]any, batchSize)
		for i := range docs {
			docs[i] = map[string]any{
				"batch": b,
				"idx":   i,
				"value": rand.Float64() * 1000,
			}
		}
		_, err := c.InsertMany(bulkCollection, docs)
		if err != nil {
			t.Fatalf("batch %d: %v", b, err)
		}
		total += batchSize
	}

	count, err := c.Count(bulkCollection, map[string]any{})
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != total {
		t.Fatalf("expected %d, got %d", total, count)
	}
}

func TestBulkInsert_LargeDocuments(t *testing.T) {
	c := setupBulk(t)
	defer teardownBulk(t, c)

	const N = 50
	payload := make([]byte, 4096)
	for i := range payload {
		payload[i] = byte('A' + i%26)
	}
	bigStr := string(payload)

	docs := make([]map[string]any, N)
	for i := range docs {
		docs[i] = map[string]any{
			"idx":     i,
			"payload": bigStr,
			"nested": map[string]any{
				"a": i * 10,
				"b": fmt.Sprintf("nested_%d", i),
				"tags": []any{
					fmt.Sprintf("tag_%d", i%5),
					fmt.Sprintf("cat_%d", i%3),
				},
			},
		}
	}

	_, err := c.InsertMany(bulkCollection, docs)
	if err != nil {
		t.Fatalf("insert large docs: %v", err)
	}

	count, err := c.Count(bulkCollection, map[string]any{})
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != N {
		t.Fatalf("expected %d, got %d", N, count)
	}
}

func TestBulkInsert_VerifyData(t *testing.T) {
	c := setupBulk(t)
	defer teardownBulk(t, c)

	docs := make([]map[string]any, 100)
	for i := range docs {
		docs[i] = map[string]any{
			"seq":    i,
			"label":  fmt.Sprintf("item-%d", i),
			"amount": float64(i) * 1.5,
		}
	}

	_, err := c.InsertMany(bulkCollection, docs)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Verify a specific document
	doc, err := c.FindOne(bulkCollection, map[string]any{"seq": 42})
	if err != nil {
		t.Fatalf("find_one: %v", err)
	}
	if doc == nil {
		t.Fatal("expected doc with seq=42")
	}
	label, _ := doc["label"].(string)
	if label != "item-42" {
		t.Fatalf("expected label=item-42, got %q", label)
	}
	amount, _ := doc["amount"].(float64)
	if amount != 63.0 {
		t.Fatalf("expected amount=63.0, got %v", amount)
	}
}

func TestBulkInsert_Timing(t *testing.T) {
	c := setupBulk(t)
	defer teardownBulk(t, c)

	const N = 5000
	docs := make([]map[string]any, N)
	for i := range docs {
		docs[i] = map[string]any{
			"idx":       i,
			"name":      fmt.Sprintf("user_%d", i),
			"age":       20 + i%60,
			"dept":      fmt.Sprintf("dept_%d", i%10),
			"salary":    30000 + rand.Intn(70000),
			"joined":    fmt.Sprintf("2020-%02d-%02d", 1+i%12, 1+i%28),
			"active":    i%3 != 0,
		}
	}

	start := time.Now()
	_, err := c.InsertMany(bulkCollection, docs)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("insert_many: %v", err)
	}

	count, _ := c.Count(bulkCollection, map[string]any{})
	t.Logf("Inserted %d docs in %v (%.0f docs/sec)", count, elapsed, float64(count)/elapsed.Seconds())
}

// ------------------------------------------------------------------
// Query tests (on bulk data)
// ------------------------------------------------------------------

// setupQueryData inserts test data and returns client + cleanup
func setupQueryData(t *testing.T) *oxidb.Client {
	t.Helper()
	c := setupBulk(t)

	const N = 500
	docs := make([]map[string]any, N)
	for i := range docs {
		docs[i] = map[string]any{
			"idx":    i,
			"name":   fmt.Sprintf("user_%04d", i),
			"age":    20 + (i % 50),
			"dept":   fmt.Sprintf("dept_%d", i%5),
			"salary": 30000 + (i%10)*5000,
			"active": i%2 == 0,
			"score":  float64(i%100) / 10.0,
		}
	}

	_, err := c.InsertMany(bulkCollection, docs)
	if err != nil {
		t.Fatalf("setup data: %v", err)
	}
	return c
}

func TestQuery_ExactMatch(t *testing.T) {
	c := setupQueryData(t)
	defer teardownBulk(t, c)

	docs, err := c.Find(bulkCollection, map[string]any{"name": "user_0042"}, nil)
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(docs))
	}
	idx, _ := docs[0]["idx"].(float64)
	if int(idx) != 42 {
		t.Fatalf("expected idx=42, got %v", idx)
	}
}

func TestQuery_RangeFilter(t *testing.T) {
	c := setupQueryData(t)
	defer teardownBulk(t, c)

	// age 60..69 means (i % 50) in [40..49] → 10 values, each appearing 10 times in 500 docs
	docs, err := c.Find(bulkCollection, map[string]any{
		"age": map[string]any{"$gte": 60, "$lt": 70},
	}, nil)
	if err != nil {
		t.Fatalf("find range: %v", err)
	}
	if len(docs) != 100 {
		t.Fatalf("expected 100 docs (ages 60-69), got %d", len(docs))
	}
	for _, doc := range docs {
		age, _ := doc["age"].(float64)
		if int(age) < 60 || int(age) >= 70 {
			t.Fatalf("doc outside range: age=%v", age)
		}
	}
}

func TestQuery_BooleanFilter(t *testing.T) {
	c := setupQueryData(t)
	defer teardownBulk(t, c)

	docs, err := c.Find(bulkCollection, map[string]any{"active": true}, nil)
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if len(docs) != 250 {
		t.Fatalf("expected 250 active docs, got %d", len(docs))
	}
}

func TestQuery_MultiCondition(t *testing.T) {
	c := setupQueryData(t)
	defer teardownBulk(t, c)

	// dept_0 AND age >= 50
	docs, err := c.Find(bulkCollection, map[string]any{
		"dept": "dept_0",
		"age":  map[string]any{"$gte": 50},
	}, nil)
	if err != nil {
		t.Fatalf("find: %v", err)
	}

	for _, doc := range docs {
		dept, _ := doc["dept"].(string)
		age, _ := doc["age"].(float64)
		if dept != "dept_0" {
			t.Fatalf("expected dept_0, got %s", dept)
		}
		if int(age) < 50 {
			t.Fatalf("expected age >= 50, got %v", age)
		}
	}
}

func TestQuery_SortAsc(t *testing.T) {
	c := setupQueryData(t)
	defer teardownBulk(t, c)

	limit := 10
	docs, err := c.Find(bulkCollection, map[string]any{}, &oxidb.FindOptions{
		Sort:  map[string]any{"age": 1},
		Limit: &limit,
	})
	if err != nil {
		t.Fatalf("find sorted: %v", err)
	}
	if len(docs) != 10 {
		t.Fatalf("expected 10 docs, got %d", len(docs))
	}
	// All should have age=20 (the minimum)
	for _, doc := range docs {
		age, _ := doc["age"].(float64)
		if int(age) != 20 {
			t.Fatalf("expected age=20 for first page asc, got %v", age)
		}
	}
}

func TestQuery_SortDesc(t *testing.T) {
	c := setupQueryData(t)
	defer teardownBulk(t, c)

	limit := 10
	docs, err := c.Find(bulkCollection, map[string]any{}, &oxidb.FindOptions{
		Sort:  map[string]any{"age": -1},
		Limit: &limit,
	})
	if err != nil {
		t.Fatalf("find sorted desc: %v", err)
	}
	if len(docs) != 10 {
		t.Fatalf("expected 10 docs, got %d", len(docs))
	}
	// All should have age=69 (the maximum: 20+49)
	for _, doc := range docs {
		age, _ := doc["age"].(float64)
		if int(age) != 69 {
			t.Fatalf("expected age=69 for first page desc, got %v", age)
		}
	}
}

func TestQuery_SkipLimit(t *testing.T) {
	c := setupQueryData(t)
	defer teardownBulk(t, c)

	skip := 10
	limit := 5
	docs, err := c.Find(bulkCollection, map[string]any{}, &oxidb.FindOptions{
		Sort:  map[string]any{"idx": 1},
		Skip:  &skip,
		Limit: &limit,
	})
	if err != nil {
		t.Fatalf("find skip/limit: %v", err)
	}
	if len(docs) != 5 {
		t.Fatalf("expected 5 docs, got %d", len(docs))
	}
	first, _ := docs[0]["idx"].(float64)
	if int(first) != 10 {
		t.Fatalf("expected first idx=10, got %v", first)
	}
	last, _ := docs[4]["idx"].(float64)
	if int(last) != 14 {
		t.Fatalf("expected last idx=14, got %v", last)
	}
}

func TestQuery_CountWithFilter(t *testing.T) {
	c := setupQueryData(t)
	defer teardownBulk(t, c)

	n, err := c.Count(bulkCollection, map[string]any{"dept": "dept_0"})
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 100 {
		t.Fatalf("expected 100 in dept_0, got %d", n)
	}
}

func TestQuery_FindOneNotFound(t *testing.T) {
	c := setupQueryData(t)
	defer teardownBulk(t, c)

	doc, err := c.FindOne(bulkCollection, map[string]any{"name": "nonexistent"})
	if err != nil {
		t.Fatalf("find_one: %v", err)
	}
	if doc != nil {
		t.Fatalf("expected nil, got %v", doc)
	}
}

func TestQuery_IndexAccelerated(t *testing.T) {
	c := setupQueryData(t)
	defer teardownBulk(t, c)

	if err := c.CreateIndex(bulkCollection, "dept"); err != nil {
		t.Fatalf("create_index: %v", err)
	}

	// Indexed query
	docs, err := c.Find(bulkCollection, map[string]any{"dept": "dept_2"}, nil)
	if err != nil {
		t.Fatalf("find indexed: %v", err)
	}
	if len(docs) != 100 {
		t.Fatalf("expected 100, got %d", len(docs))
	}
}

func TestQuery_UpdateOne(t *testing.T) {
	c := setupQueryData(t)
	defer teardownBulk(t, c)

	result, err := c.UpdateOne(bulkCollection,
		map[string]any{"idx": 0},
		map[string]any{"$set": map[string]any{"name": "updated_user"}},
	)
	if err != nil {
		t.Fatalf("update_one: %v", err)
	}
	mod, _ := result["modified"].(float64)
	if int(mod) != 1 {
		t.Fatalf("expected 1 modified, got %v", mod)
	}

	doc, _ := c.FindOne(bulkCollection, map[string]any{"idx": 0})
	name, _ := doc["name"].(string)
	if name != "updated_user" {
		t.Fatalf("expected updated_user, got %s", name)
	}
}

func TestQuery_DeleteOne(t *testing.T) {
	c := setupQueryData(t)
	defer teardownBulk(t, c)

	result, err := c.DeleteOne(bulkCollection, map[string]any{"idx": 0})
	if err != nil {
		t.Fatalf("delete_one: %v", err)
	}
	del, _ := result["deleted"].(float64)
	if int(del) != 1 {
		t.Fatalf("expected 1 deleted, got %v", del)
	}

	count, _ := c.Count(bulkCollection, map[string]any{})
	if count != 499 {
		t.Fatalf("expected 499, got %d", count)
	}
}

func TestQuery_AggregateOnBulk(t *testing.T) {
	c := setupQueryData(t)
	defer teardownBulk(t, c)

	results, err := c.Aggregate(bulkCollection, []map[string]any{
		{"$group": map[string]any{
			"_id":       "$dept",
			"count":     map[string]any{"$sum": 1},
			"avg_age":   map[string]any{"$avg": "$age"},
			"max_score": map[string]any{"$max": "$score"},
		}},
		{"$sort": map[string]any{"_id": 1}},
	})
	if err != nil {
		t.Fatalf("aggregate: %v", err)
	}
	if len(results) != 5 {
		t.Fatalf("expected 5 groups (dept_0..dept_4), got %d", len(results))
	}
	for _, r := range results {
		cnt, _ := r["count"].(float64)
		if int(cnt) != 100 {
			t.Fatalf("expected 100 per dept, got %v", cnt)
		}
	}
}

func TestQuery_TextSearch(t *testing.T) {
	c := setupBulk(t)
	defer teardownBulk(t, c)

	docs := []map[string]any{
		{"title": "Introduction to Go programming", "body": "Go is a compiled language designed at Google"},
		{"title": "Rust systems programming", "body": "Rust guarantees memory safety without garbage collection"},
		{"title": "Advanced Go concurrency", "body": "Goroutines and channels are the core of Go concurrency"},
	}
	_, err := c.InsertMany(bulkCollection, docs)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	if err := c.CreateTextIndex(bulkCollection, []string{"title", "body"}); err != nil {
		t.Fatalf("create_text_index: %v", err)
	}

	results, err := c.TextSearch(bulkCollection, "Go programming", 10)
	if err != nil {
		t.Fatalf("text_search: %v", err)
	}
	if len(results) < 1 {
		t.Fatal("expected at least 1 text search result")
	}
}

func TestQuery_ListAndDropIndex(t *testing.T) {
	c := setupBulk(t)
	defer teardownBulk(t, c)

	_, _ = c.InsertMany(bulkCollection, []map[string]any{
		{"x": 1, "y": 2},
	})

	if err := c.CreateIndex(bulkCollection, "x"); err != nil {
		t.Fatalf("create_index: %v", err)
	}

	indexes, err := c.ListIndexes(bulkCollection)
	if err != nil {
		t.Fatalf("list_indexes: %v", err)
	}
	if len(indexes) < 1 {
		t.Fatal("expected at least 1 index")
	}

	found := false
	for _, idx := range indexes {
		name, _ := idx["name"].(string)
		if name == "x" {
			found = true
		}
	}
	if !found {
		t.Fatal("index 'x' not in list")
	}

	if err := c.DropIndex(bulkCollection, "x"); err != nil {
		t.Fatalf("drop_index: %v", err)
	}

	indexes, err = c.ListIndexes(bulkCollection)
	if err != nil {
		t.Fatalf("list_indexes after drop: %v", err)
	}
	if len(indexes) != 0 {
		t.Fatalf("expected 0 indexes after drop, got %d", len(indexes))
	}
}

func TestQuery_CompactAfterBulkDelete(t *testing.T) {
	c := setupBulk(t)
	defer teardownBulk(t, c)

	const N = 200
	docs := make([]map[string]any, N)
	for i := range docs {
		docs[i] = map[string]any{"idx": i, "data": "test"}
	}
	_, err := c.InsertMany(bulkCollection, docs)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Delete half
	_, err = c.Delete(bulkCollection, map[string]any{
		"idx": map[string]any{"$lt": 100},
	})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}

	stats, err := c.Compact(bulkCollection)
	if err != nil {
		t.Fatalf("compact: %v", err)
	}

	kept, _ := stats["docs_kept"].(float64)
	if int(kept) != 100 {
		t.Fatalf("expected 100 kept, got %v", kept)
	}

	oldSize, _ := stats["old_size"].(float64)
	newSize, _ := stats["new_size"].(float64)
	if newSize >= oldSize {
		t.Fatalf("expected new_size < old_size, got %v >= %v", newSize, oldSize)
	}

	// Verify remaining docs are accessible
	count, _ := c.Count(bulkCollection, map[string]any{})
	if count != 100 {
		t.Fatalf("expected 100 after compact, got %d", count)
	}
}
