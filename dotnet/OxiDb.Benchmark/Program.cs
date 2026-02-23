using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json;
using OxiDb.Client;

const string Collection = "bench_15m";
const int TotalRecords = 15_000_000;
const int BatchSize = 1_000;
const int TotalBatches = TotalRecords / BatchSize;

// ─── Synthetic data generators ─────────────────────────────
string[] categories = ["electronics", "clothing", "books", "food", "automotive", "healthcare", "sports", "finance", "education", "technology"];
string[] cities = ["New York", "London", "Tokyo", "Berlin", "Paris", "Istanbul", "Sydney", "Toronto", "Mumbai", "Seoul"];
string[] statuses = ["active", "inactive", "pending", "archived"];
string[] tags = ["premium", "sale", "new", "featured", "limited", "popular", "trending", "classic", "budget", "luxury"];

var sw = new Stopwatch();

Console.WriteLine("╔══════════════════════════════════════════════════════════╗");
Console.WriteLine("║     OxiDB Benchmark — 15 Million Records                ║");
Console.WriteLine("╚══════════════════════════════════════════════════════════╝");
Console.WriteLine();

// ─── Connect ───────────────────────────────────────────────
Console.Write("Connecting to OxiDB... ");
using var db = OxiDbClient.Connect("127.0.0.1", 4444);
Console.WriteLine(db.Ping());

// ─── Check if data already exists ──────────────────────────
long existingCount = 0;
try
{
    using var cr = db.Count(Collection);
    existingCount = cr.RootElement.GetProperty("data").GetProperty("count").GetInt64();
}
catch { }

if (existingCount >= TotalRecords)
{
    Console.WriteLine($"Collection already has {existingCount:N0} records — skipping insert.");
}
else
{
    if (existingCount == 0)
    {
        Console.Write("Creating collection + indexes... ");
        sw.Restart();
        db.Insert(Collection, """{"_setup": true}""");
        db.Delete(Collection, """{"_setup": true}""");
        db.CreateIndex(Collection, "category");
        db.CreateIndex(Collection, "city");
        db.CreateIndex(Collection, "status");
        db.CreateIndex(Collection, "price");
        db.CreateIndex(Collection, "rating");
        db.CreateIndex(Collection, "year");
        db.CreateCompositeIndex(Collection, """["category", "city"]""");
        db.CreateCompositeIndex(Collection, """["status", "year"]""");
        sw.Stop();
        Console.WriteLine($"done ({sw.ElapsedMilliseconds} ms — 6 single + 2 composite)");
    }
    else
    {
        Console.WriteLine($"Resuming insert — {existingCount:N0} records already present.");
    }

    long startFrom = existingCount;
    int startBatch = (int)(startFrom / BatchSize);
    Console.WriteLine($"Inserting {TotalRecords - startFrom:N0} remaining records (batch size {BatchSize:N0})...");
    Console.WriteLine();

    var rng = new Random(42);
    // Fast-forward RNG to match the batch we're resuming from
    for (long skip = 0; skip < startFrom; skip++)
    {
        rng.Next(categories.Length); rng.Next(cities.Length); rng.Next(statuses.Length);
        rng.NextDouble(); rng.NextDouble(); rng.Next(11); rng.Next(10000);
        rng.Next(tags.Length); rng.Next(tags.Length);
    }

    var totalInsertSw = Stopwatch.StartNew();
    long insertedCount = 0;
    var lastReport = Stopwatch.StartNew();

    for (int batch = startBatch; batch < TotalBatches; batch++)
    {
        var sb = new StringBuilder(BatchSize * 220);
        sb.Append('[');
        for (int i = 0; i < BatchSize; i++)
        {
            if (i > 0) sb.Append(',');
            long id = (long)batch * BatchSize + i;
            var category = categories[rng.Next(categories.Length)];
            var city = cities[rng.Next(cities.Length)];
            var status = statuses[rng.Next(statuses.Length)];
            var price = Math.Round(rng.NextDouble() * 10000, 2).ToString(CultureInfo.InvariantCulture);
            var rating = Math.Round(rng.NextDouble() * 5, 1).ToString(CultureInfo.InvariantCulture);
            var year = 2015 + rng.Next(11);
            var stock = rng.Next(10000);
            var tag1 = tags[rng.Next(tags.Length)];
            var tag2 = tags[rng.Next(tags.Length)];

            sb.Append($$"""{"name":"Product {{id}}","category":"{{category}}","city":"{{city}}","status":"{{status}}","price":{{price}},"rating":{{rating}},"year":{{year}},"stock":{{stock}},"tags":["{{tag1}}","{{tag2}}"],"description":"Product {{id}} in {{category}} from {{city}}"}""");
        }
        sb.Append(']');

        using var result = db.InsertMany(Collection, sb.ToString());
        insertedCount += BatchSize;

        if (lastReport.ElapsedMilliseconds >= 5000 || batch == TotalBatches - 1)
        {
            var total = startFrom + insertedCount;
            var elapsed = totalInsertSw.Elapsed;
            var rate = insertedCount / elapsed.TotalSeconds;
            var pct = (double)total / TotalRecords * 100;
            var remaining = TotalRecords - total;
            var eta = TimeSpan.FromSeconds(remaining / Math.Max(rate, 1));
            Console.WriteLine($"  [{pct,6:F1}%] {total,12:N0} / {TotalRecords:N0}  |  {rate,10:N0} docs/sec  |  elapsed {elapsed:mm\\:ss}  |  ETA {eta:mm\\:ss}");
            lastReport.Restart();
        }
    }

    totalInsertSw.Stop();
    var totalRate = insertedCount / totalInsertSw.Elapsed.TotalSeconds;
    Console.WriteLine();
    Console.WriteLine($"  *** INSERT COMPLETE: {insertedCount:N0} new records in {totalInsertSw.Elapsed:mm\\:ss\\.ff}  ({totalRate:N0} docs/sec) ***");
}
Console.WriteLine();

// ─── Verify count ──────────────────────────────────────────
Console.Write("Verifying count... ");
sw.Restart();
using (var countResult = db.Count(Collection))
{
    sw.Stop();
    var count = countResult.RootElement.GetProperty("data").GetProperty("count").GetInt64();
    Console.WriteLine($"{count:N0} records ({sw.ElapsedMilliseconds} ms)");
}
Console.WriteLine();

// ─── Search benchmarks ─────────────────────────────────────
Console.WriteLine("╔══════════════════════════════════════════════════════════╗");
Console.WriteLine("║     Search Benchmarks on 15M Records                    ║");
Console.WriteLine("╚══════════════════════════════════════════════════════════╝");
Console.WriteLine();

// Warm up
db.FindOne(Collection, Filter.Eq("category", "electronics"));

void BenchFind(string label, string queryJson)
{
    sw.Restart();
    using var result = db.Find(Collection, queryJson);
    sw.Stop();
    var arr = result.RootElement.GetProperty("data");
    var count = arr.GetArrayLength();
    Console.WriteLine($"  {label,-55} → {count,8:N0} hits  in {sw.ElapsedMilliseconds,7:N0} ms");
}

void BenchFindOne(string label, Filter filter)
{
    sw.Restart();
    using var result = db.FindOne(Collection, filter);
    sw.Stop();
    var data = result.RootElement.GetProperty("data");
    var found = data.ValueKind != JsonValueKind.Null;
    Console.WriteLine($"  {label,-55} → {(found ? "FOUND" : "NULL "),-8}      in {sw.ElapsedMilliseconds,7:N0} ms");
}

void BenchAggregate(string label, string pipeline)
{
    sw.Restart();
    using var result = db.Aggregate(Collection, pipeline);
    sw.Stop();
    var data = result.RootElement.GetProperty("data");
    var info = "";
    if (data.ValueKind == JsonValueKind.Array && data.GetArrayLength() > 0)
    {
        var first = data[0];
        info = first.ToString();
        if (info.Length > 60) info = info[..60] + "...";
    }
    Console.WriteLine($"  {label,-55} → {info,-20} in {sw.ElapsedMilliseconds,7:N0} ms");
}

// ── 1. Exact match on indexed field ────────────────────────
Console.WriteLine("── 1. Indexed exact match (~1.5M per category) ──────────");
BenchFind("category = 'electronics'",
    """{"category": "electronics"}""");
BenchFind("city = 'Istanbul'",
    """{"city": "Istanbul"}""");
BenchFind("status = 'active'",
    """{"status": "active"}""");
Console.WriteLine();

// ── 2. Range queries ───────────────────────────────────────
Console.WriteLine("── 2. Range queries on indexed fields ──────────────────");
BenchFind("price > 9500 (top ~5%)",
    """{"price": {"$gt": 9500}}""");
BenchFind("price 100-200 (narrow band)",
    """{"$and": [{"price": {"$gte": 100}}, {"price": {"$lte": 200}}]}""");
BenchFind("rating >= 4.5 (top ~10%)",
    """{"rating": {"$gte": 4.5}}""");
BenchFind("year = 2024 (1 of 11 years)",
    """{"year": 2024}""");
Console.WriteLine();

// ── 3. Compound queries ────────────────────────────────────
Console.WriteLine("── 3. Compound queries (AND on multiple indexes) ────────");
BenchFind("category=electronics AND city=Tokyo",
    """{"$and": [{"category": "electronics"}, {"city": "Tokyo"}]}""");
BenchFind("status=active AND year=2023",
    """{"$and": [{"status": "active"}, {"year": 2023}]}""");
BenchFind("category=books AND price>5000 AND rating>=4",
    """{"$and": [{"category": "books"}, {"price": {"$gt": 5000}}, {"rating": {"$gte": 4}}]}""");
Console.WriteLine();

// ── 4. FindOne (point lookup) ──────────────────────────────
Console.WriteLine("── 4. FindOne (point lookups, no index on name) ─────────");
BenchFindOne("name = 'Product 0' (first doc)",
    Filter.Eq("name", "Product 0"));
BenchFindOne("name = 'Product 7500000' (middle)",
    Filter.Eq("name", "Product 7500000"));
BenchFindOne("name = 'Product 14999999' (last doc)",
    Filter.Eq("name", "Product 14999999"));
BenchFindOne("name = 'NONEXISTENT'",
    Filter.Eq("name", "NONEXISTENT"));
Console.WriteLine();

// ── 5. $in queries ─────────────────────────────────────────
Console.WriteLine("── 5. $in queries ──────────────────────────────────────");
BenchFind("category in [electronics, books, sports]",
    """{"category": {"$in": ["electronics", "books", "sports"]}}""");
BenchFind("city in [Istanbul, Tokyo]",
    """{"city": {"$in": ["Istanbul", "Tokyo"]}}""");
Console.WriteLine();

// ── 6. $or queries ─────────────────────────────────────────
Console.WriteLine("── 6. $or queries ──────────────────────────────────────");
BenchFind("category=food OR city=Berlin",
    """{"$or": [{"category": "food"}, {"city": "Berlin"}]}""");
Console.WriteLine();

// ── 7. Aggregation ─────────────────────────────────────────
Console.WriteLine("── 7. Aggregation pipelines ────────────────────────────");
BenchAggregate("COUNT where category=electronics",
    """[{"$match": {"category": "electronics"}}, {"$count": "total"}]""");
BenchAggregate("GROUP BY category → sum(stock)",
    """[{"$group": {"_id": "$category", "totalStock": {"$sum": "$stock"}}}]""");
BenchAggregate("GROUP BY city → avg(price), count",
    """[{"$group": {"_id": "$city", "avgPrice": {"$avg": "$price"}, "n": {"$sum": 1}}}]""");
BenchAggregate("TOP 5 most expensive (sort+limit)",
    """[{"$sort": {"price": -1}}, {"$limit": 5}, {"$project": {"name": 1, "price": 1, "_id": 0}}]""");
BenchAggregate("Match electronics → GROUP BY city → sort",
    """[{"$match": {"category": "electronics"}}, {"$group": {"_id": "$city", "total": {"$sum": "$stock"}}}, {"$sort": {"total": -1}}]""");
Console.WriteLine();

// ── 8. Repeated query (hot path) ───────────────────────────
Console.WriteLine("── 8. Same query 5x (warm path) ────────────────────────");
for (int i = 0; i < 5; i++)
{
    sw.Restart();
    using var r = db.Find(Collection, """{"$and": [{"category": "electronics"}, {"city": "Tokyo"}, {"price": {"$gt": 5000}}]}""");
    sw.Stop();
    var n = r.RootElement.GetProperty("data").GetArrayLength();
    Console.WriteLine($"  Run {i + 1}: electronics+Tokyo+price>5000              → {n,8:N0} hits  in {sw.ElapsedMilliseconds,7:N0} ms");
}
Console.WriteLine();

// ── Summary ────────────────────────────────────────────────
Console.WriteLine("╔══════════════════════════════════════════════════════════╗");
Console.WriteLine("║     Benchmark Complete                                  ║");
Console.WriteLine("╚══════════════════════════════════════════════════════════╝");
