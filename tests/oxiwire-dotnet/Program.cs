using System.Diagnostics;
using OxiDb.Client.Tcp;

Console.WriteLine("=== OxiWire .NET Protocol Test ===\n");

var port = args.Length > 0 ? int.Parse(args[0]) : 14444;
int passed = 0, failed = 0;

void Assert(bool cond, string name, string detail = "")
{
    if (cond) { passed++; Console.WriteLine($"  PASS: {name}"); }
    else { failed++; Console.WriteLine($"  FAIL: {name} {detail}"); }
}

await using var client = await OxiDbTcpClient.ConnectAsync("127.0.0.1", port);
client.UseOxiWire();

// 1. Ping
var pong = await client.PingAsync();
Assert(pong == "pong", "Ping returns pong", $"got '{pong}'");

// 2. Insert + Find
var col = $"oxiwire_{Guid.NewGuid():N}";
var insertResult = await client.InsertAsync(col, new Dictionary<string, object?>
{
    ["name"] = "Alice",
    ["age"] = 30,
    ["active"] = true,
    ["score"] = 99.5
});
Assert(insertResult.TryGetProperty("id", out _), "Insert returns id");

var docs = await client.FindAsync(col);
Assert(docs.GetArrayLength() == 1, "Find returns 1 doc");
var doc = docs[0];
Assert(doc.GetProperty("name").GetString() == "Alice", "String field roundtrip");
Assert(doc.GetProperty("age").GetUInt64() == 30, "Integer field roundtrip");
Assert(doc.GetProperty("active").GetBoolean() == true, "Boolean field roundtrip");
Assert(Math.Abs(doc.GetProperty("score").GetDouble() - 99.5) < 0.001, "Float field roundtrip");

// 3. InsertMany + Count
var batch = Enumerable.Range(0, 50).Select(i => (object)new Dictionary<string, object?>
{
    ["n"] = i, ["label"] = $"item_{i}"
}).ToArray();
await client.InsertManyAsync(col, batch);
var count = await client.CountAsync(col);
Assert(count == 51, "InsertMany + Count", $"expected 51, got {count}");

// 4. Query with filter
var filtered = await client.FindAsync(col,
    query: new Dictionary<string, object?> { ["n"] = new Dictionary<string, object?> { ["$gte"] = 40 } });
Assert(filtered.GetArrayLength() == 10, "Range query $gte=40", $"got {filtered.GetArrayLength()}");

// 5. Sort + limit
var sorted = await client.FindAsync(col,
    query: new Dictionary<string, object?> { ["n"] = new Dictionary<string, object?> { ["$gte"] = 0 } },
    sort: new Dictionary<string, object?> { ["n"] = -1 },
    limit: 3);
Assert(sorted.GetArrayLength() == 3, "Sort desc + limit 3");
Assert(sorted[0].GetProperty("n").GetUInt64() == 49, "Sort desc first is 49");

// 6. Update
var updateResult = await client.UpdateAsync(col,
    new Dictionary<string, object?> { ["name"] = "Alice" },
    new Dictionary<string, object?> { ["$set"] = new Dictionary<string, object?> { ["age"] = 31 } });
Assert(updateResult.GetProperty("modified").GetInt32() == 1, "Update modified 1");

var updated = await client.FindOneAsync(col, new Dictionary<string, object?> { ["name"] = "Alice" });
Assert(updated.GetProperty("age").GetUInt64() == 31, "Update value persisted");

// 7. Delete
var deleteResult = await client.DeleteAsync(col,
    new Dictionary<string, object?> { ["n"] = new Dictionary<string, object?> { ["$lt"] = 5 } });
Assert(deleteResult.GetProperty("deleted").GetInt32() == 5, "Delete removed 5 docs");

// 8. Transaction commit
await client.BeginTransactionAsync();
await client.InsertAsync(col, new Dictionary<string, object?> { ["tx"] = "committed" });
await client.CommitTransactionAsync();
var txDoc = await client.FindOneAsync(col, new Dictionary<string, object?> { ["tx"] = "committed" });
Assert(txDoc.GetProperty("tx").GetString() == "committed", "Tx commit visible");

// 9. Transaction rollback
var countBefore = await client.CountAsync(col);
await client.BeginTransactionAsync();
await client.InsertAsync(col, new Dictionary<string, object?> { ["tx"] = "rolled_back" });
await client.RollbackTransactionAsync();
var countAfter = await client.CountAsync(col);
Assert(countAfter == countBefore, "Tx rollback discards insert");

// 10. Error handling
try
{
    await client.CreateCollectionAsync(col);
    await client.CreateCollectionAsync(col);
    Assert(false, "Duplicate collection throws");
}
catch (OxiDbTcpException ex)
{
    Assert(ex.Message.Contains("already exists"), "Error message decoded", ex.Message);
}

// 11. Negative numbers
await client.InsertAsync(col, new Dictionary<string, object?> { ["balance"] = -100, ["tag"] = "negative" });
var negDoc = await client.FindOneAsync(col, new Dictionary<string, object?> { ["tag"] = "negative" });
Assert(negDoc.GetProperty("balance").GetInt64() == -100, "Negative int64 roundtrip");

// 12. Null values
await client.InsertAsync(col, new Dictionary<string, object?> { ["nullable"] = (object?)null, ["tag"] = "nulltest" });
var nullDoc = await client.FindOneAsync(col, new Dictionary<string, object?> { ["tag"] = "nulltest" });
Assert(nullDoc.GetProperty("nullable").ValueKind == System.Text.Json.JsonValueKind.Null, "Null roundtrip");

// 13. Index operations
await client.CreateIndexAsync(col, "n");
await client.CreateUniqueIndexAsync(col, "tag");

// Verify unique constraint works through OxiWire
try
{
    await client.InsertAsync(col, new Dictionary<string, object?> { ["tag"] = "negative" });
    Assert(false, "Unique index violation throws");
}
catch (OxiDbTcpException)
{
    Assert(true, "Unique index violation throws");
}

// 14. Aggregation
var aggResult = await client.AggregateAsync(col, new object[]
{
    new Dictionary<string, object?> { ["$match"] = new Dictionary<string, object?> { ["n"] = new Dictionary<string, object?> { ["$gte"] = 0 } } },
    new Dictionary<string, object?> { ["$count"] = "total" }
});
Assert(aggResult.GetArrayLength() == 1, "Aggregation returns result");
Assert(aggResult[0].GetProperty("total").GetInt32() > 0, "Aggregation count > 0");

// 15. Performance comparison
Console.WriteLine("\n--- Performance Benchmark ---");
var perfCol = $"perf_{Guid.NewGuid():N}";
var perfDocs = Enumerable.Range(0, 1000).Select(i => (object)new Dictionary<string, object?>
{
    ["n"] = i, ["name"] = $"user_{i}", ["score"] = i * 1.5, ["active"] = i % 2 == 0
}).ToArray();

// JSON baseline
await using var jsonClient = await OxiDbTcpClient.ConnectAsync("127.0.0.1", port);
await jsonClient.InsertManyAsync(perfCol, perfDocs);

// Warmup
for (int i = 0; i < 5; i++)
    await jsonClient.FindAsync(perfCol);

var sw = Stopwatch.StartNew();
for (int i = 0; i < 50; i++)
    await jsonClient.FindAsync(perfCol);
sw.Stop();
var jsonMs = sw.Elapsed.TotalMilliseconds;

// OxiWire
await using var wireClient = await OxiDbTcpClient.ConnectAsync("127.0.0.1", port);
wireClient.UseOxiWire();

// Warmup
for (int i = 0; i < 5; i++)
    await wireClient.FindAsync(perfCol);

sw.Restart();
for (int i = 0; i < 50; i++)
    await wireClient.FindAsync(perfCol);
sw.Stop();
var wireMs = sw.Elapsed.TotalMilliseconds;

Console.WriteLine($"  50x find 1000 docs:");
Console.WriteLine($"    JSON:    {jsonMs:F1}ms ({jsonMs / 50:F1}ms/op)");
Console.WriteLine($"    OxiWire: {wireMs:F1}ms ({wireMs / 50:F1}ms/op)");
Console.WriteLine($"    Speedup: {jsonMs / wireMs:F2}x");

// Cleanup
await client.DropCollectionAsync(col);
await wireClient.DropCollectionAsync(perfCol);

Console.WriteLine($"\n=== Results: {passed} passed, {failed} failed ===");
if (failed > 0) Environment.Exit(1);
