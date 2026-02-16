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

// Cleanup
db.DropCollection(Collection);
Console.WriteLine();
Console.WriteLine("Done. Collection dropped.");
