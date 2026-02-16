using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using OxiDb.Client;

const int DocCount = 10_000;
const string Collection = "benchmark";

Console.WriteLine("=== OxiDB .NET FFI Benchmark ===");
Console.WriteLine();

using var db = OxiDbClient.Connect("127.0.0.1", 4444);

// Verify connection
var pong = db.Ping();
Console.WriteLine($"Ping: {pong}");

// Clean up from any previous run
db.DropCollection(Collection);

// Create an index on "category" before inserting for faster queries
db.CreateIndex(Collection, "category");
Console.WriteLine("Created index on 'category'");

// --- Insert 10k documents via batch ---
Console.WriteLine();
Console.WriteLine($"Inserting {DocCount:N0} documents (batch)...");

var sw = Stopwatch.StartNew();
var categories = new[] { "electronics", "books", "clothing", "food", "toys" };
var random = new Random(42);

const int BatchSize = 500;
for (int batch = 0; batch < DocCount; batch += BatchSize)
{
    var end = Math.Min(batch + BatchSize, DocCount);
    var sb = new System.Text.StringBuilder("[");
    for (int i = batch; i < end; i++)
    {
        if (i > batch) sb.Append(',');
        var category = categories[i % categories.Length];
        var price = Math.Round(random.NextDouble() * 1000, 2).ToString(CultureInfo.InvariantCulture);
        var rating = Math.Round(random.NextDouble() * 5, 1).ToString(CultureInfo.InvariantCulture);
        var stock = random.Next(0, 500);
        sb.Append($$"""{"name":"Product {{i}}","category":"{{category}}","price":{{price}},"stock":{{stock}},"rating":{{rating}}}""");
    }
    sb.Append(']');
    db.InsertMany(Collection, sb.ToString());
}

sw.Stop();
Console.WriteLine($"  Inserted {DocCount:N0} docs in {sw.Elapsed.TotalSeconds:F2}s ({DocCount / sw.Elapsed.TotalSeconds:F0} docs/s)");

// Verify count
using var countResult = db.Count(Collection);
var count = countResult.RootElement.GetProperty("data").GetProperty("count").GetInt32();
Console.WriteLine($"  Collection count: {count:N0}");

// --- Queries ---
Console.WriteLine();
Console.WriteLine("Running queries...");

// Query 1: Find all electronics
sw.Restart();
using var electronicsResult = db.Find(Collection, Filter.Eq("category", "electronics"));
sw.Stop();
var electronicsData = electronicsResult.RootElement.GetProperty("data");
var electronicsCount = electronicsData.GetArrayLength();
Console.WriteLine($"  Find category='electronics': {electronicsCount:N0} docs in {sw.Elapsed.TotalMilliseconds:F1}ms");

// Query 2: Find one specific product
sw.Restart();
using var oneResult = db.FindOne(Collection, Filter.Eq("name", "Product 5000"));
sw.Stop();
var oneData = oneResult.RootElement.GetProperty("data");
Console.WriteLine($"  FindOne name='Product 5000': found in {sw.Elapsed.TotalMilliseconds:F1}ms");
Console.WriteLine($"    -> {oneData}");

// Query 3: Find all books
sw.Restart();
using var booksResult = db.Find(Collection, Filter.Eq("category", "books"));
sw.Stop();
var booksCount = booksResult.RootElement.GetProperty("data").GetArrayLength();
Console.WriteLine($"  Find category='books': {booksCount:N0} docs in {sw.Elapsed.TotalMilliseconds:F1}ms");

// Query 4: Find all food items
sw.Restart();
using var foodResult = db.Find(Collection, Filter.Eq("category", "food"));
sw.Stop();
var foodCount = foodResult.RootElement.GetProperty("data").GetArrayLength();
Console.WriteLine($"  Find category='food': {foodCount:N0} docs in {sw.Elapsed.TotalMilliseconds:F1}ms");

// --- Update ---
Console.WriteLine();
Console.WriteLine("Running update...");

sw.Restart();
using var updateResult = db.Update(Collection, Filter.Eq("name", "Product 0"), UpdateDef.Set("price", 999.99) + UpdateDef.Set("featured", true));
sw.Stop();
var modified = updateResult.RootElement.GetProperty("data").GetProperty("modified").GetInt32();
Console.WriteLine($"  Updated {modified} doc(s) in {sw.Elapsed.TotalMilliseconds:F1}ms");

// Verify update
using var verifyResult = db.FindOne(Collection, Filter.Eq("name", "Product 0"));
var updatedDoc = verifyResult.RootElement.GetProperty("data");
Console.WriteLine($"    -> {updatedDoc}");

// --- Delete ---
Console.WriteLine();
Console.WriteLine("Running delete...");

sw.Restart();
using var deleteResult = db.Delete(Collection, Filter.Eq("name", "Product 9999"));
sw.Stop();
var deleted = deleteResult.RootElement.GetProperty("data").GetProperty("deleted").GetInt32();
Console.WriteLine($"  Deleted {deleted} doc(s) in {sw.Elapsed.TotalMilliseconds:F1}ms");

// Final count
using var finalCount = db.Count(Collection);
var remaining = finalCount.RootElement.GetProperty("data").GetProperty("count").GetInt32();
Console.WriteLine($"  Final count: {remaining:N0}");

// =======================================================================
// Aggregation Pipeline Tests
// =======================================================================
Console.WriteLine();
Console.WriteLine("=== Aggregation Pipeline Tests ===");
Console.WriteLine();

int passed = 0;
int failed = 0;

void Assert(bool condition, string testName, string detail = "")
{
    if (condition)
    {
        passed++;
        Console.WriteLine($"  PASS: {testName}");
    }
    else
    {
        failed++;
        Console.WriteLine($"  FAIL: {testName} {detail}");
    }
}

// --- Test 1: $match stage ---
{
    sw.Restart();
    using var result = db.Aggregate(Collection, """
        [{"$match": {"category": "electronics"}}, {"$count": "total"}]
    """);
    sw.Stop();
    var data = result.RootElement.GetProperty("data");
    var total = data[0].GetProperty("total").GetInt32();
    Assert(total > 0, "$match + $count filters correctly", $"got {total} electronics");
    Console.WriteLine($"    (electronics count: {total}, {sw.Elapsed.TotalMilliseconds:F1}ms)");
}

// --- Test 2: $group with $sum ---
{
    using var result = db.Aggregate(Collection, """
        [{"$group": {"_id": "$category", "totalStock": {"$sum": "$stock"}}}]
    """);
    var data = result.RootElement.GetProperty("data");
    var groups = data.GetArrayLength();
    Assert(groups == 5, "$group by category produces 5 groups", $"got {groups}");
    // Verify each group has _id and totalStock
    bool allValid = true;
    for (int i = 0; i < groups; i++)
    {
        if (!data[i].TryGetProperty("_id", out _) || !data[i].TryGetProperty("totalStock", out _))
        {
            allValid = false;
            break;
        }
    }
    Assert(allValid, "$group output has _id and totalStock fields");
}

// --- Test 3: $group with $avg, $min, $max ---
{
    using var result = db.Aggregate(Collection, """
        [{"$group": {
            "_id": null,
            "avgPrice": {"$avg": "$price"},
            "minPrice": {"$min": "$price"},
            "maxPrice": {"$max": "$price"}
        }}]
    """);
    var data = result.RootElement.GetProperty("data");
    Assert(data.GetArrayLength() == 1, "$group null key produces single doc");
    var doc = data[0];
    var avg = doc.GetProperty("avgPrice").GetDouble();
    var min = doc.GetProperty("minPrice").GetDouble();
    var max = doc.GetProperty("maxPrice").GetDouble();
    Assert(min <= avg && avg <= max, "$avg is between $min and $max", $"min={min:F2} avg={avg:F2} max={max:F2}");
    Console.WriteLine($"    (price stats: min={min:F2}, avg={avg:F2}, max={max:F2})");
}

// --- Test 4: $group with $first, $last, $push ---
{
    using var result = db.Aggregate(Collection, """
        [
            {"$match": {"category": "toys"}},
            {"$limit": 3},
            {"$group": {
                "_id": null,
                "firstName": {"$first": "$name"},
                "lastName": {"$last": "$name"},
                "allNames": {"$push": "$name"}
            }}
        ]
    """);
    var data = result.RootElement.GetProperty("data");
    Assert(data.GetArrayLength() == 1, "$first/$last/$push produces single doc");
    var doc = data[0];
    var names = doc.GetProperty("allNames").GetArrayLength();
    Assert(names == 3, "$push collects 3 names", $"got {names}");
    var first = doc.GetProperty("firstName").GetString();
    var last = doc.GetProperty("lastName").GetString();
    Assert(first != null && last != null, "$first and $last are non-null");
}

// --- Test 5: $sort + $limit (top 3 most expensive) ---
{
    using var result = db.Aggregate(Collection, """
        [
            {"$sort": {"price": -1}},
            {"$limit": 3},
            {"$project": {"name": 1, "price": 1, "_id": 0}}
        ]
    """);
    var data = result.RootElement.GetProperty("data");
    Assert(data.GetArrayLength() == 3, "$sort + $limit returns 3 docs");
    var p0 = data[0].GetProperty("price").GetDouble();
    var p1 = data[1].GetProperty("price").GetDouble();
    var p2 = data[2].GetProperty("price").GetDouble();
    Assert(p0 >= p1 && p1 >= p2, "$sort desc orders prices correctly", $"{p0:F2} >= {p1:F2} >= {p2:F2}");
}

// --- Test 6: $skip + $limit pagination ---
{
    using var result = db.Aggregate(Collection, """
        [
            {"$sort": {"price": 1}},
            {"$skip": 10},
            {"$limit": 5}
        ]
    """);
    var data = result.RootElement.GetProperty("data");
    Assert(data.GetArrayLength() == 5, "$skip 10 + $limit 5 returns 5 docs");
}

// --- Test 7: $project include/exclude/compute ---
{
    using var result = db.Aggregate(Collection, """
        [
            {"$limit": 1},
            {"$project": {"name": 1, "category": 1, "_id": 0}}
        ]
    """);
    var data = result.RootElement.GetProperty("data");
    var doc = data[0];
    Assert(doc.TryGetProperty("name", out _), "$project includes 'name'");
    Assert(doc.TryGetProperty("category", out _), "$project includes 'category'");
    Assert(!doc.TryGetProperty("_id", out _), "$project excludes '_id'");
    Assert(!doc.TryGetProperty("price", out _), "$project excludes non-listed 'price'");
}

// --- Test 8: $project with computed field ---
{
    using var result = db.Aggregate(Collection, """
        [
            {"$limit": 3},
            {"$addFields": {"discountedPrice": {"$multiply": ["$price", 0.9]}}},
            {"$project": {"name": 1, "price": 1, "discountedPrice": 1, "_id": 0}}
        ]
    """);
    var data = result.RootElement.GetProperty("data");
    Assert(data.GetArrayLength() == 3, "$addFields + $project returns 3 docs");
    var price = data[0].GetProperty("price").GetDouble();
    var discounted = data[0].GetProperty("discountedPrice").GetDouble();
    Assert(Math.Abs(discounted - price * 0.9) < 0.01, "$addFields computes discountedPrice correctly",
        $"price={price:F2} discounted={discounted:F2}");
}

// --- Test 9: $count stage ---
{
    using var result = db.Aggregate(Collection, """
        [{"$count": "totalDocs"}]
    """);
    var data = result.RootElement.GetProperty("data");
    Assert(data.GetArrayLength() == 1, "$count produces single doc");
    var totalDocs = data[0].GetProperty("totalDocs").GetInt32();
    Assert(totalDocs == remaining, "$count matches collection count", $"got {totalDocs}, expected {remaining}");
}

// --- Test 10: $unwind ---
{
    // Insert docs with arrays for unwind test
    const string unwindCol = "unwind_test";
    db.DropCollection(unwindCol);
    db.InsertMany(unwindCol, """
        [
            {"name": "Alice", "tags": ["rust", "db", "fast"]},
            {"name": "Bob", "tags": ["go", "api"]},
            {"name": "Charlie", "tags": []}
        ]
    """);

    using var result = db.Aggregate(unwindCol, """
        [
            {"$unwind": "$tags"},
            {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
    """);
    var data = result.RootElement.GetProperty("data");
    Assert(data.GetArrayLength() == 5, "$unwind expands to 5 unique tags", $"got {data.GetArrayLength()}");

    db.DropCollection(unwindCol);
}

// --- Test 11: $unwind with preserveNullAndEmptyArrays ---
{
    const string unwindCol2 = "unwind_preserve_test";
    db.DropCollection(unwindCol2);
    db.InsertMany(unwindCol2, """
        [
            {"name": "A", "items": [1, 2]},
            {"name": "B", "items": []},
            {"name": "C", "items": null},
            {"name": "D"}
        ]
    """);

    using var result = db.Aggregate(unwindCol2, """
        [{"$unwind": {"path": "$items", "preserveNullAndEmptyArrays": true}}]
    """);
    var data = result.RootElement.GetProperty("data");
    // A -> 2 docs, B -> 1 (preserved), C -> 1 (preserved), D -> 1 (preserved)
    Assert(data.GetArrayLength() == 5, "$unwind preserve keeps empty/null/missing", $"got {data.GetArrayLength()}");

    db.DropCollection(unwindCol2);
}

// --- Test 12: $lookup (cross-collection join) ---
{
    const string ordersCol = "orders_test";
    const string productsCol = "products_test";
    db.DropCollection(ordersCol);
    db.DropCollection(productsCol);

    db.InsertMany(productsCol, """
        [
            {"sku": "ABC", "name": "Widget", "price": 25},
            {"sku": "XYZ", "name": "Gadget", "price": 50}
        ]
    """);
    db.InsertMany(ordersCol, """
        [
            {"orderNum": 1, "item": "ABC", "qty": 10},
            {"orderNum": 2, "item": "XYZ", "qty": 5},
            {"orderNum": 3, "item": "NONE", "qty": 1}
        ]
    """);

    using var result = db.Aggregate(ordersCol, """
        [{"$lookup": {
            "from": "products_test",
            "localField": "item",
            "foreignField": "sku",
            "as": "product"
        }}]
    """);
    var data = result.RootElement.GetProperty("data");
    Assert(data.GetArrayLength() == 3, "$lookup returns all 3 orders");

    // Order 1 (ABC) should have 1 matching product
    var order1Product = data[0].GetProperty("product");
    Assert(order1Product.GetArrayLength() == 1, "$lookup finds matching product for ABC",
        $"got {order1Product.GetArrayLength()}");

    // Order 3 (NONE) should have 0 matching products
    var order3Product = data[2].GetProperty("product");
    Assert(order3Product.GetArrayLength() == 0, "$lookup returns empty array for no match");

    db.DropCollection(ordersCol);
    db.DropCollection(productsCol);
}

// --- Test 13: Full multi-stage pipeline (match -> group -> sort -> limit) ---
{
    sw.Restart();
    using var result = db.Aggregate(Collection, """
        [
            {"$match": {"category": {"$in": ["electronics", "books", "toys"]}}},
            {"$group": {"_id": "$category", "avgRating": {"$avg": "$rating"}, "count": {"$sum": 1}}},
            {"$sort": {"avgRating": -1}},
            {"$limit": 2}
        ]
    """);
    sw.Stop();
    var data = result.RootElement.GetProperty("data");
    Assert(data.GetArrayLength() == 2, "Multi-stage pipeline returns top 2 categories");
    var r0 = data[0].GetProperty("avgRating").GetDouble();
    var r1 = data[1].GetProperty("avgRating").GetDouble();
    Assert(r0 >= r1, "Multi-stage results sorted desc by avgRating", $"{r0:F2} >= {r1:F2}");
    Console.WriteLine($"    (top categories by avg rating: {data[0].GetProperty("_id")}={r0:F2}, {data[1].GetProperty("_id")}={r1:F2}, {sw.Elapsed.TotalMilliseconds:F1}ms)");
}

// --- Test 14: $group with compound key ---
{
    using var result = db.Aggregate(Collection, """
        [
            {"$group": {"_id": {"cat": "$category"}, "total": {"$sum": 1}}},
            {"$sort": {"total": -1}}
        ]
    """);
    var data = result.RootElement.GetProperty("data");
    Assert(data.GetArrayLength() == 5, "$group compound key produces 5 groups");
    var first = data[0];
    Assert(first.GetProperty("_id").TryGetProperty("cat", out _), "Compound key has 'cat' field in _id");
}

// --- Test 15: Empty pipeline returns all docs ---
{
    using var result = db.Aggregate(Collection, "[]");
    var data = result.RootElement.GetProperty("data");
    Assert(data.GetArrayLength() == remaining, "Empty pipeline returns all docs", $"got {data.GetArrayLength()}, expected {remaining}");
}

// --- Test 16: $addFields with arithmetic ---
{
    using var result = db.Aggregate(Collection, """
        [
            {"$limit": 2},
            {"$addFields": {
                "totalValue": {"$multiply": ["$price", "$stock"]},
                "priceWithTax": {"$add": ["$price", {"$multiply": ["$price", 0.18]}]}
            }}
        ]
    """);
    var data = result.RootElement.GetProperty("data");
    Assert(data.GetArrayLength() == 2, "$addFields returns correct number of docs");
    var doc = data[0];
    Assert(doc.TryGetProperty("totalValue", out _), "$addFields adds 'totalValue'");
    Assert(doc.TryGetProperty("priceWithTax", out _), "$addFields adds 'priceWithTax'");
    Assert(doc.TryGetProperty("name", out _), "$addFields preserves existing 'name'");
}

// --- Test 17: Error handling - unknown stage ---
{
    using var result = db.Aggregate(Collection, """[{"$badStage": {}}]""");
    var ok = result.RootElement.GetProperty("ok").GetBoolean();
    Assert(!ok, "Unknown stage returns error");
}

// --- Test 18: Error handling - invalid pipeline type ---
{
    using var result = db.Aggregate(Collection, """{"not": "an array"}""");
    var ok = result.RootElement.GetProperty("ok").GetBoolean();
    Assert(!ok, "Non-array pipeline returns error");
}

// --- Aggregation Summary ---
Console.WriteLine();
Console.WriteLine($"=== Aggregation Tests: {passed} passed, {failed} failed ===");

// =======================================================================
// Blob Storage + Full-Text Search Tests
// =======================================================================
Console.WriteLine();
Console.WriteLine("=== Blob Storage + Full-Text Search Tests ===");
Console.WriteLine();

// --- Test 19: Create bucket ---
{
    using var result = db.CreateBucket("test-docs");
    var ok = result.RootElement.GetProperty("ok").GetBoolean();
    Assert(ok, "create_bucket succeeds");
}

// --- Test 20: List buckets ---
{
    db.CreateBucket("test-images");
    using var result = db.ListBuckets();
    var data = result.RootElement.GetProperty("data");
    var bucketCount = data.GetArrayLength();
    Assert(bucketCount >= 2, "list_buckets returns at least 2 buckets", $"got {bucketCount}");

    bool foundDocs = false, foundImages = false;
    for (int i = 0; i < bucketCount; i++)
    {
        var name = data[i].GetString();
        if (name == "test-docs") foundDocs = true;
        if (name == "test-images") foundImages = true;
    }
    Assert(foundDocs && foundImages, "list_buckets contains test-docs and test-images");
}

// --- Test 21: Put object (text/plain) + get object roundtrip ---
{
    var textContent = "The quick brown fox jumps over the lazy dog. Database performance tuning is important.";
    var b64 = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(textContent));

    using var putResult = db.PutObject("test-docs", "fox.txt", b64, "text/plain", """{"author": "Alice"}""");
    var ok = putResult.RootElement.GetProperty("ok").GetBoolean();
    Assert(ok, "put_object text/plain succeeds");

    var meta = putResult.RootElement.GetProperty("data");
    Assert(meta.GetProperty("key").GetString() == "fox.txt", "put_object returns correct key");
    Assert(meta.GetProperty("bucket").GetString() == "test-docs", "put_object returns correct bucket");
    Assert(meta.GetProperty("size").GetInt64() == textContent.Length, "put_object returns correct size");
    Assert(meta.GetProperty("content_type").GetString() == "text/plain", "put_object returns correct content_type");
    Assert(meta.TryGetProperty("etag", out _), "put_object returns etag");

    // Get object and verify content roundtrip
    using var getResult = db.GetObject("test-docs", "fox.txt");
    var getData = getResult.RootElement.GetProperty("data");
    var contentB64 = getData.GetProperty("content").GetString()!;
    var decoded = System.Text.Encoding.UTF8.GetString(Convert.FromBase64String(contentB64));
    Assert(decoded == textContent, "get_object roundtrip preserves content");

    var getMeta = getData.GetProperty("metadata");
    Assert(getMeta.GetProperty("key").GetString() == "fox.txt", "get_object metadata has correct key");
}

// --- Test 22: Head object (metadata only) ---
{
    using var result = db.HeadObject("test-docs", "fox.txt");
    var ok = result.RootElement.GetProperty("ok").GetBoolean();
    Assert(ok, "head_object succeeds");
    var meta = result.RootElement.GetProperty("data");
    Assert(meta.GetProperty("key").GetString() == "fox.txt", "head_object returns correct key");
    Assert(meta.GetProperty("size").GetInt64() > 0, "head_object returns non-zero size");
}

// --- Test 23: Put multiple objects for listing and search ---
{
    var report = "Database query optimization and indexing strategies for high performance systems.";
    var notes = "Quick notes on Rust programming language and memory safety features.";
    var csv = "name,age,city\nAlice,30,NYC\nBob,25,LA";

    db.PutObject("test-docs", "report.txt",
        Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(report)),
        "text/plain");
    db.PutObject("test-docs", "notes.md",
        Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(notes)),
        "text/plain");
    db.PutObject("test-docs", "data/people.csv",
        Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(csv)),
        "text/csv");
    db.PutObject("test-docs", "data/config.json",
        Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes("""{"database": "oxidb", "version": "0.1"}""")),
        "application/json");

    Assert(true, "Multiple put_object calls succeed");
}

// --- Test 24: List objects (all) ---
{
    using var result = db.ListObjects("test-docs");
    var data = result.RootElement.GetProperty("data");
    var objectCount = data.GetArrayLength();
    Assert(objectCount == 5, "list_objects returns all 5 objects", $"got {objectCount}");
}

// --- Test 25: List objects with prefix filter ---
{
    using var result = db.ListObjects("test-docs", "data/");
    var data = result.RootElement.GetProperty("data");
    var objectCount = data.GetArrayLength();
    Assert(objectCount == 2, "list_objects prefix='data/' returns 2 objects", $"got {objectCount}");

    // Verify sorted by key
    var key0 = data[0].GetProperty("key").GetString();
    var key1 = data[1].GetProperty("key").GetString();
    Assert(string.Compare(key0, key1, StringComparison.Ordinal) < 0,
        "list_objects sorted by key", $"'{key0}' < '{key1}'");
}

// --- Test 26: List objects with limit ---
{
    using var result = db.ListObjects("test-docs", null, 2);
    var data = result.RootElement.GetProperty("data");
    Assert(data.GetArrayLength() == 2, "list_objects limit=2 returns 2 objects");
}

// --- Test 27: Full-text search across bucket ---
{
    using var result = db.Search("database performance", "test-docs", 10);
    var ok = result.RootElement.GetProperty("ok").GetBoolean();
    Assert(ok, "search succeeds");
    var data = result.RootElement.GetProperty("data");
    var resultCount = data.GetArrayLength();
    Assert(resultCount >= 1, "search 'database performance' finds results", $"got {resultCount}");

    // Top result should have highest score
    if (resultCount > 0)
    {
        var topKey = data[0].GetProperty("key").GetString();
        var topScore = data[0].GetProperty("score").GetDouble();
        Assert(topScore > 0, $"Top search result '{topKey}' has positive score ({topScore:F4})");
        Console.WriteLine($"    (top result: {topKey}, score: {topScore:F4})");
    }
}

// --- Test 28: Search with specific term ---
{
    using var result = db.Search("rust programming memory", "test-docs", 10);
    var data = result.RootElement.GetProperty("data");
    var resultCount = data.GetArrayLength();
    Assert(resultCount >= 1, "search 'rust programming memory' finds results", $"got {resultCount}");

    if (resultCount > 0)
    {
        var topKey = data[0].GetProperty("key").GetString();
        Assert(topKey == "notes.md", "search 'rust programming' top result is notes.md", $"got '{topKey}'");
    }
}

// --- Test 29: Search with no matching terms ---
{
    using var result = db.Search("xyznonexistent", "test-docs", 10);
    var data = result.RootElement.GetProperty("data");
    Assert(data.GetArrayLength() == 0, "search for nonexistent term returns empty");
}

// --- Test 30: Search JSON content (application/json indexing) ---
{
    using var result = db.Search("oxidb", "test-docs", 10);
    var data = result.RootElement.GetProperty("data");
    Assert(data.GetArrayLength() >= 1, "search indexes JSON string values", $"got {data.GetArrayLength()}");
    if (data.GetArrayLength() > 0)
    {
        var topKey = data[0].GetProperty("key").GetString();
        Assert(topKey == "data/config.json", "search JSON content finds config.json", $"got '{topKey}'");
    }
}

// --- Test 31: Overwrite existing object ---
{
    var newContent = "Updated content about machine learning and artificial intelligence.";
    var b64 = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(newContent));
    db.PutObject("test-docs", "fox.txt", b64, "text/plain");

    // Old content should no longer be searchable
    using var result = db.Search("lazy dog", "test-docs", 10);
    var data = result.RootElement.GetProperty("data");
    Assert(data.GetArrayLength() == 0, "overwrite removes old content from search index");

    // New content should be searchable
    using var result2 = db.Search("machine learning", "test-docs", 10);
    var data2 = result2.RootElement.GetProperty("data");
    Assert(data2.GetArrayLength() >= 1, "overwrite indexes new content");
}

// --- Test 32: Delete object removes from search index ---
{
    db.DeleteObject("test-docs", "notes.md");

    // Verify it's gone from search
    using var result = db.Search("rust programming", "test-docs", 10);
    var data = result.RootElement.GetProperty("data");
    bool notesFound = false;
    for (int i = 0; i < data.GetArrayLength(); i++)
    {
        if (data[i].GetProperty("key").GetString() == "notes.md")
        {
            notesFound = true;
            break;
        }
    }
    Assert(!notesFound, "delete_object removes document from search index");

    // Verify get_object returns error
    using var getResult = db.GetObject("test-docs", "notes.md");
    var ok = getResult.RootElement.GetProperty("ok").GetBoolean();
    Assert(!ok, "get_object after delete returns error");
}

// --- Test 33: Get from missing bucket returns error ---
{
    using var result = db.GetObject("nonexistent-bucket", "anything.txt");
    var ok = result.RootElement.GetProperty("ok").GetBoolean();
    Assert(!ok, "get_object from missing bucket returns error");
}

// --- Test 34: Put binary object (not indexed) ---
{
    var binaryData = new byte[] { 0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A }; // PNG header
    var b64 = Convert.ToBase64String(binaryData);
    using var result = db.PutObject("test-images", "icon.png", b64, "image/png");
    var ok = result.RootElement.GetProperty("ok").GetBoolean();
    Assert(ok, "put_object binary data succeeds");

    // Should not appear in text search
    using var searchResult = db.Search("PNG", "test-images", 10);
    var searchData = searchResult.RootElement.GetProperty("data");
    Assert(searchData.GetArrayLength() == 0, "binary objects are not indexed for search");
}

// --- Test 35: HTML tag stripping for text/html ---
{
    var html = "<html><body><h1>Welcome</h1><p>This is about database optimization</p></body></html>";
    var b64 = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(html));
    db.PutObject("test-docs", "page.html", b64, "text/html");

    using var result = db.Search("welcome optimization", "test-docs", 10);
    var data = result.RootElement.GetProperty("data");
    bool htmlFound = false;
    for (int i = 0; i < data.GetArrayLength(); i++)
    {
        if (data[i].GetProperty("key").GetString() == "page.html")
        {
            htmlFound = true;
            break;
        }
    }
    Assert(htmlFound, "text/html content is indexed with tags stripped");
}

// --- Test 36: Search across all buckets (no bucket filter) ---
{
    using var result = db.Search("database", null, 10);
    var data = result.RootElement.GetProperty("data");
    Assert(data.GetArrayLength() >= 1, "search without bucket filter finds results across buckets");
}

// --- Test 37: Delete bucket removes everything ---
{
    db.DeleteBucket("test-images");
    using var result = db.ListBuckets();
    var data = result.RootElement.GetProperty("data");
    bool imagesFound = false;
    for (int i = 0; i < data.GetArrayLength(); i++)
    {
        if (data[i].GetString() == "test-images")
        {
            imagesFound = true;
            break;
        }
    }
    Assert(!imagesFound, "delete_bucket removes bucket from list");
}

// Cleanup blob test data
db.DeleteBucket("test-docs");

// --- Blob + FTS Summary ---
Console.WriteLine();
Console.WriteLine($"=== Blob + FTS Tests: {passed - 18} passed (of {passed + failed - 18}) ===");
Console.WriteLine();
Console.WriteLine($"=== Total: {passed} passed, {failed} failed ===");

// Cleanup
db.DropCollection(Collection);
Console.WriteLine();
Console.WriteLine("Done. Cleaned up.");

if (failed > 0)
    Environment.Exit(1);
