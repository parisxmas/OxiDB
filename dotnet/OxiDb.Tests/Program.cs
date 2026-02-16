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
using var electronicsResult = db.Find(Collection, "{\"category\":\"electronics\"}");
sw.Stop();
var electronicsData = electronicsResult.RootElement.GetProperty("data");
var electronicsCount = electronicsData.GetArrayLength();
Console.WriteLine($"  Find category='electronics': {electronicsCount:N0} docs in {sw.Elapsed.TotalMilliseconds:F1}ms");

// Query 2: Find one specific product
sw.Restart();
using var oneResult = db.FindOne(Collection, "{\"name\":\"Product 5000\"}");
sw.Stop();
var oneData = oneResult.RootElement.GetProperty("data");
Console.WriteLine($"  FindOne name='Product 5000': found in {sw.Elapsed.TotalMilliseconds:F1}ms");
Console.WriteLine($"    -> {oneData}");

// Query 3: Find all books
sw.Restart();
using var booksResult = db.Find(Collection, "{\"category\":\"books\"}");
sw.Stop();
var booksCount = booksResult.RootElement.GetProperty("data").GetArrayLength();
Console.WriteLine($"  Find category='books': {booksCount:N0} docs in {sw.Elapsed.TotalMilliseconds:F1}ms");

// Query 4: Find all food items
sw.Restart();
using var foodResult = db.Find(Collection, "{\"category\":\"food\"}");
sw.Stop();
var foodCount = foodResult.RootElement.GetProperty("data").GetArrayLength();
Console.WriteLine($"  Find category='food': {foodCount:N0} docs in {sw.Elapsed.TotalMilliseconds:F1}ms");

// --- Update ---
Console.WriteLine();
Console.WriteLine("Running update...");

sw.Restart();
using var updateResult = db.Update(Collection, "{\"name\":\"Product 0\"}", "{\"$set\":{\"price\":999.99,\"featured\":true}}");
sw.Stop();
var modified = updateResult.RootElement.GetProperty("data").GetProperty("modified").GetInt32();
Console.WriteLine($"  Updated {modified} doc(s) in {sw.Elapsed.TotalMilliseconds:F1}ms");

// Verify update
using var verifyResult = db.FindOne(Collection, "{\"name\":\"Product 0\"}");
var updatedDoc = verifyResult.RootElement.GetProperty("data");
Console.WriteLine($"    -> {updatedDoc}");

// --- Delete ---
Console.WriteLine();
Console.WriteLine("Running delete...");

sw.Restart();
using var deleteResult = db.Delete(Collection, "{\"name\":\"Product 9999\"}");
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

// --- Summary ---
Console.WriteLine();
Console.WriteLine($"=== Aggregation Tests: {passed} passed, {failed} failed ===");

// Cleanup
db.DropCollection(Collection);
Console.WriteLine();
Console.WriteLine("Done. Collection dropped.");

if (failed > 0)
    Environment.Exit(1);
