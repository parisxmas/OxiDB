package oxidb_test

import (
	"os"
	"strconv"
	"testing"

	"github.com/parisxmas/OxiDB/go/oxidb"
)

func getClient(t *testing.T) *oxidb.Client {
	t.Helper()
	host := "127.0.0.1"
	port := 4444
	if h := os.Getenv("OXIDB_HOST"); h != "" {
		host = h
	}
	if p := os.Getenv("OXIDB_PORT"); p != "" {
		port, _ = strconv.Atoi(p)
	}
	c, err := oxidb.Connect(host, port, 5e9)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	return c
}

func TestPing(t *testing.T) {
	c := getClient(t)
	defer c.Close()

	pong, err := c.Ping()
	if err != nil {
		t.Fatalf("ping: %v", err)
	}
	if pong != "pong" {
		t.Fatalf("expected pong, got %q", pong)
	}
}

func TestCollections(t *testing.T) {
	c := getClient(t)
	defer c.Close()

	if err := c.CreateCollection("go_test"); err != nil {
		t.Fatalf("create collection: %v", err)
	}

	cols, err := c.ListCollections()
	if err != nil {
		t.Fatalf("list collections: %v", err)
	}
	found := false
	for _, col := range cols {
		if col == "go_test" {
			found = true
		}
	}
	if !found {
		t.Fatal("go_test not in collection list")
	}
}

func TestInsertAndFind(t *testing.T) {
	c := getClient(t)
	defer c.Close()

	result, err := c.Insert("go_test", map[string]any{"name": "Alice", "age": 30})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if result["id"] == nil {
		t.Fatal("insert did not return id")
	}

	docs, err := c.Find("go_test", map[string]any{"name": "Alice"}, nil)
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if len(docs) < 1 {
		t.Fatal("find returned no docs")
	}
	if docs[0]["name"] != "Alice" {
		t.Fatalf("expected Alice, got %v", docs[0]["name"])
	}
}

func TestInsertMany(t *testing.T) {
	c := getClient(t)
	defer c.Close()

	_, err := c.InsertMany("go_test", []map[string]any{
		{"name": "Bob", "age": 25},
		{"name": "Charlie", "age": 35},
	})
	if err != nil {
		t.Fatalf("insert_many: %v", err)
	}
}

func TestFindWithOptions(t *testing.T) {
	c := getClient(t)
	defer c.Close()

	limit := 1
	docs, err := c.Find("go_test", map[string]any{}, &oxidb.FindOptions{Limit: &limit})
	if err != nil {
		t.Fatalf("find with limit: %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(docs))
	}
}

func TestFindOne(t *testing.T) {
	c := getClient(t)
	defer c.Close()

	doc, err := c.FindOne("go_test", map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatalf("find_one: %v", err)
	}
	if doc["name"] != "Bob" {
		t.Fatalf("expected Bob, got %v", doc["name"])
	}
}

func TestCount(t *testing.T) {
	c := getClient(t)
	defer c.Close()

	n, err := c.Count("go_test", map[string]any{})
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if n < 3 {
		t.Fatalf("expected >= 3, got %d", n)
	}
}

func TestUpdate(t *testing.T) {
	c := getClient(t)
	defer c.Close()

	result, err := c.Update("go_test",
		map[string]any{"name": "Alice"},
		map[string]any{"$set": map[string]any{"age": 31}},
	)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	mod, _ := result["modified"].(float64)
	if int(mod) != 1 {
		t.Fatalf("expected 1 modified, got %v", result["modified"])
	}

	doc, _ := c.FindOne("go_test", map[string]any{"name": "Alice"})
	age, _ := doc["age"].(float64)
	if int(age) != 31 {
		t.Fatalf("expected age 31, got %v", doc["age"])
	}
}

func TestDelete(t *testing.T) {
	c := getClient(t)
	defer c.Close()

	result, err := c.Delete("go_test", map[string]any{"name": "Charlie"})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	del, _ := result["deleted"].(float64)
	if int(del) != 1 {
		t.Fatalf("expected 1 deleted, got %v", result["deleted"])
	}
}

func TestIndexes(t *testing.T) {
	c := getClient(t)
	defer c.Close()

	if err := c.CreateIndex("go_test", "name"); err != nil {
		t.Fatalf("create_index: %v", err)
	}
	if err := c.CreateUniqueIndex("go_test", "age"); err != nil {
		t.Fatalf("create_unique_index: %v", err)
	}
	if err := c.CreateCompositeIndex("go_test", []string{"name", "age"}); err != nil {
		t.Fatalf("create_composite_index: %v", err)
	}
}

func TestCompositeIndex(t *testing.T) {
	c := getClient(t)
	defer c.Close()

	col := "go_composite_test"
	_ = c.DropCollection(col)

	// Insert test data with department + city + age fields
	docs := []map[string]any{
		{"name": "Alice", "department": "Engineering", "city": "Tokyo", "age": 30},
		{"name": "Bob", "department": "Engineering", "city": "Paris", "age": 25},
		{"name": "Charlie", "department": "Sales", "city": "Tokyo", "age": 35},
		{"name": "Diana", "department": "Engineering", "city": "Tokyo", "age": 28},
		{"name": "Eve", "department": "Sales", "city": "Paris", "age": 40},
		{"name": "Frank", "department": "HR", "city": "London", "age": 33},
		{"name": "Grace", "department": "Engineering", "city": "London", "age": 45},
		{"name": "Hank", "department": "HR", "city": "Tokyo", "age": 29},
	}
	_, err := c.InsertMany(col, docs)
	if err != nil {
		t.Fatalf("insert_many: %v", err)
	}

	// Create composite index on (department, city)
	if err := c.CreateCompositeIndex(col, []string{"department", "city"}); err != nil {
		t.Fatalf("create composite index: %v", err)
	}

	// List indexes and verify composite index exists
	indexes, err := c.ListIndexes(col)
	if err != nil {
		t.Fatalf("list_indexes: %v", err)
	}
	foundComposite := false
	for _, idx := range indexes {
		if name, _ := idx["name"].(string); name == "department_city" {
			foundComposite = true
		}
	}
	if !foundComposite {
		t.Fatal("composite index 'department_city' not found in index list")
	}

	// Query using both fields of the composite index
	t.Run("ExactMatchBothFields", func(t *testing.T) {
		results, err := c.Find(col, map[string]any{
			"department": "Engineering", "city": "Tokyo",
		}, nil)
		if err != nil {
			t.Fatalf("find: %v", err)
		}
		if len(results) != 2 {
			t.Fatalf("expected 2 docs (Alice, Diana), got %d", len(results))
		}
		for _, doc := range results {
			if doc["department"] != "Engineering" || doc["city"] != "Tokyo" {
				t.Fatalf("unexpected doc: %v", doc)
			}
		}
	})

	// Query using prefix of composite index (department only)
	t.Run("PrefixMatch", func(t *testing.T) {
		results, err := c.Find(col, map[string]any{
			"department": "Engineering",
		}, nil)
		if err != nil {
			t.Fatalf("find: %v", err)
		}
		if len(results) != 4 {
			t.Fatalf("expected 4 Engineering docs, got %d", len(results))
		}
	})

	// Count with composite index
	t.Run("CountWithComposite", func(t *testing.T) {
		n, err := c.Count(col, map[string]any{
			"department": "Sales", "city": "Tokyo",
		})
		if err != nil {
			t.Fatalf("count: %v", err)
		}
		if n != 1 {
			t.Fatalf("expected 1 (Charlie), got %d", n)
		}
	})

	// Sorted query using composite index
	t.Run("SortedByCompositeFields", func(t *testing.T) {
		results, err := c.Find(col, map[string]any{}, &oxidb.FindOptions{
			Sort: map[string]any{"department": 1, "city": 1},
		})
		if err != nil {
			t.Fatalf("find sorted: %v", err)
		}
		if len(results) != 8 {
			t.Fatalf("expected 8 docs, got %d", len(results))
		}
		// First doc should be Engineering/London (alphabetical)
		dept, _ := results[0]["department"].(string)
		if dept != "Engineering" {
			t.Fatalf("expected first department=Engineering, got %q", dept)
		}
	})

	// Update using composite-indexed fields
	t.Run("UpdateWithComposite", func(t *testing.T) {
		result, err := c.Update(col,
			map[string]any{"department": "HR", "city": "London"},
			map[string]any{"$set": map[string]any{"age": 34}},
		)
		if err != nil {
			t.Fatalf("update: %v", err)
		}
		mod, _ := result["modified"].(float64)
		if int(mod) != 1 {
			t.Fatalf("expected 1 modified, got %v", result["modified"])
		}
		doc, _ := c.FindOne(col, map[string]any{"name": "Frank"})
		age, _ := doc["age"].(float64)
		if int(age) != 34 {
			t.Fatalf("expected age 34, got %v", doc["age"])
		}
	})

	// Delete using composite-indexed fields
	t.Run("DeleteWithComposite", func(t *testing.T) {
		result, err := c.Delete(col, map[string]any{
			"department": "Sales", "city": "Paris",
		})
		if err != nil {
			t.Fatalf("delete: %v", err)
		}
		del, _ := result["deleted"].(float64)
		if int(del) != 1 {
			t.Fatalf("expected 1 deleted (Eve), got %v", result["deleted"])
		}
		n, _ := c.Count(col, map[string]any{})
		if n != 7 {
			t.Fatalf("expected 7 remaining, got %d", n)
		}
	})

	// Aggregation using composite-indexed match
	t.Run("AggregateWithComposite", func(t *testing.T) {
		results, err := c.Aggregate(col, []map[string]any{
			{"$match": map[string]any{"department": "Engineering"}},
			{"$group": map[string]any{
				"_id":     "$city",
				"avg_age": map[string]any{"$avg": "$age"},
				"count":   map[string]any{"$sum": 1},
			}},
			{"$sort": map[string]any{"_id": 1}},
		})
		if err != nil {
			t.Fatalf("aggregate: %v", err)
		}
		if len(results) != 3 {
			t.Fatalf("expected 3 cities (London, Paris, Tokyo), got %d", len(results))
		}
	})

	// Drop composite index
	t.Run("DropCompositeIndex", func(t *testing.T) {
		if err := c.DropIndex(col, "department_city"); err != nil {
			t.Fatalf("drop_index: %v", err)
		}
		indexes, err := c.ListIndexes(col)
		if err != nil {
			t.Fatalf("list_indexes: %v", err)
		}
		for _, idx := range indexes {
			if name, _ := idx["name"].(string); name == "department_city" {
				t.Fatal("composite index still exists after drop")
			}
		}
	})

	// Triple composite index
	t.Run("TripleCompositeIndex", func(t *testing.T) {
		if err := c.CreateCompositeIndex(col, []string{"department", "city", "age"}); err != nil {
			t.Fatalf("create triple composite: %v", err)
		}
		results, err := c.Find(col, map[string]any{
			"department": "Engineering", "city": "Tokyo",
		}, nil)
		if err != nil {
			t.Fatalf("find: %v", err)
		}
		if len(results) != 2 {
			t.Fatalf("expected 2, got %d", len(results))
		}
	})

	// Cleanup
	_ = c.DropCollection(col)
}

func TestAggregation(t *testing.T) {
	c := getClient(t)
	defer c.Close()

	results, err := c.Aggregate("go_test", []map[string]any{
		{"$group": map[string]any{"_id": nil, "avg_age": map[string]any{"$avg": "$age"}}},
	})
	if err != nil {
		t.Fatalf("aggregate: %v", err)
	}
	if len(results) < 1 {
		t.Fatal("aggregate returned no results")
	}
}

func TestTransaction(t *testing.T) {
	c := getClient(t)
	defer c.Close()

	err := c.WithTransaction(func() error {
		if _, err := c.Insert("go_tx", map[string]any{"action": "debit", "amount": 100}); err != nil {
			return err
		}
		if _, err := c.Insert("go_tx", map[string]any{"action": "credit", "amount": 100}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("transaction: %v", err)
	}

	docs, err := c.Find("go_tx", map[string]any{}, nil)
	if err != nil {
		t.Fatalf("find tx docs: %v", err)
	}
	if len(docs) != 2 {
		t.Fatalf("expected 2 tx docs, got %d", len(docs))
	}
}

func TestBlobStorage(t *testing.T) {
	c := getClient(t)
	defer c.Close()

	if err := c.CreateBucket("go-bucket"); err != nil {
		t.Fatalf("create_bucket: %v", err)
	}

	buckets, err := c.ListBuckets()
	if err != nil {
		t.Fatalf("list_buckets: %v", err)
	}
	found := false
	for _, b := range buckets {
		if b == "go-bucket" {
			found = true
		}
	}
	if !found {
		t.Fatal("go-bucket not in bucket list")
	}

	_, err = c.PutObject("go-bucket", "hello.txt", []byte("Hello from Go!"), "text/plain", nil)
	if err != nil {
		t.Fatalf("put_object: %v", err)
	}

	data, _, err := c.GetObject("go-bucket", "hello.txt")
	if err != nil {
		t.Fatalf("get_object: %v", err)
	}
	if string(data) != "Hello from Go!" {
		t.Fatalf("expected 'Hello from Go!', got %q", string(data))
	}

	head, err := c.HeadObject("go-bucket", "hello.txt")
	if err != nil {
		t.Fatalf("head_object: %v", err)
	}
	if head["size"] == nil {
		t.Fatal("head_object missing size")
	}

	objs, err := c.ListObjects("go-bucket", nil, nil)
	if err != nil {
		t.Fatalf("list_objects: %v", err)
	}
	if len(objs) < 1 {
		t.Fatal("list_objects returned empty")
	}

	if err := c.DeleteObject("go-bucket", "hello.txt"); err != nil {
		t.Fatalf("delete_object: %v", err)
	}
}

func TestSearch(t *testing.T) {
	c := getClient(t)
	defer c.Close()

	results, err := c.Search("hello", nil, 10)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	_ = results // may be empty, just checking no error
}

func TestCompact(t *testing.T) {
	c := getClient(t)
	defer c.Close()

	stats, err := c.Compact("go_test")
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if stats["docs_kept"] == nil {
		t.Fatal("compact missing docs_kept")
	}
}

func TestCleanup(t *testing.T) {
	c := getClient(t)
	defer c.Close()

	_ = c.DropCollection("go_test")
	_ = c.DropCollection("go_tx")
	_ = c.DeleteBucket("go-bucket")
}

func TestCreateCollectionWithOptions(t *testing.T) {
	c := getClient(t)
	defer c.Close()

	const col = "go_opts"
	_ = c.DropCollection(col) // start clean; ignore "not found"

	if err := c.CreateCollectionWithOptions(col, oxidb.DiskFirst(true), oxidb.Compress(false)); err != nil {
		t.Fatalf("create with options: %v", err)
	}
	for i := 0; i < 25; i++ {
		if _, err := c.Insert(col, map[string]any{"k": i, "v": i * 2}); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	n, err := c.Count(col, map[string]any{})
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 25 {
		t.Fatalf("expected 25 docs, got %d", n)
	}
	doc, err := c.FindOne(col, map[string]any{"k": 7})
	if err != nil {
		t.Fatalf("find_one: %v", err)
	}
	if doc["v"] != float64(14) {
		t.Fatalf("expected v=14, got %v (%T)", doc["v"], doc["v"])
	}
	_ = c.DropCollection(col)
}
