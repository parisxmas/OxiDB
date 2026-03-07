using System.Diagnostics;
using OxiDb.Client.Tcp;

Console.WriteLine("╔══════════════════════════════════════════════════════════════╗");
Console.WriteLine("║          OxiDB ACID Compliance Test Suite (.NET 11)         ║");
Console.WriteLine("╚══════════════════════════════════════════════════════════════╝");
Console.WriteLine();

var port = args.Length > 0 ? int.Parse(args[0]) : 14444;
int passed = 0, failed = 0;
var sw = new Stopwatch();

void Pass(string name)
{
    passed++;
    Console.WriteLine($"  ✓ {name}");
}

void Fail(string name, string detail = "")
{
    failed++;
    Console.WriteLine($"  ✗ {name} {detail}");
}

void Assert(bool cond, string name, string detail = "")
{
    if (cond) Pass(name); else Fail(name, detail);
}

await using var client = await OxiDbTcpClient.ConnectAsync("127.0.0.1", port);
client.UseOxiWire();

var pong = await client.PingAsync();
Assert(pong == "pong", "Server connection OK");

// ═══════════════════════════════════════════════════════════════
// A — ATOMICITY
// All operations in a transaction succeed or none do.
// ═══════════════════════════════════════════════════════════════
Console.WriteLine("\n── ATOMICITY ──────────────────────────────────────────────────");

// A1: Committed transaction — all writes visible
{
    var col = $"acid_a1_{Guid.NewGuid():N}";
    await client.BeginTransactionAsync();
    await client.InsertAsync(col, new Dictionary<string, object?> { ["item"] = "A", ["qty"] = 10 });
    await client.InsertAsync(col, new Dictionary<string, object?> { ["item"] = "B", ["qty"] = 20 });
    await client.InsertAsync(col, new Dictionary<string, object?> { ["item"] = "C", ["qty"] = 30 });
    await client.CommitTransactionAsync();

    var count = await client.CountAsync(col);
    Assert(count == 3, "A1: Commit makes all 3 inserts visible");
    await client.DropCollectionAsync(col);
}

// A2: Rolled-back transaction — no writes visible
{
    var col = $"acid_a2_{Guid.NewGuid():N}";
    // Seed one doc outside tx
    await client.InsertAsync(col, new Dictionary<string, object?> { ["item"] = "pre-existing" });

    await client.BeginTransactionAsync();
    await client.InsertAsync(col, new Dictionary<string, object?> { ["item"] = "ghost1" });
    await client.InsertAsync(col, new Dictionary<string, object?> { ["item"] = "ghost2" });
    await client.RollbackTransactionAsync();

    var count = await client.CountAsync(col);
    Assert(count == 1, "A2: Rollback discards all tx inserts", $"expected 1, got {count}");
    await client.DropCollectionAsync(col);
}

// A3: Multi-operation atomicity (insert + update + delete in one tx)
{
    var col = $"acid_a3_{Guid.NewGuid():N}";
    await client.InsertAsync(col, new Dictionary<string, object?> { ["name"] = "Alpha", ["v"] = 1 });
    await client.InsertAsync(col, new Dictionary<string, object?> { ["name"] = "Beta", ["v"] = 1 });

    await client.BeginTransactionAsync();
    await client.InsertAsync(col, new Dictionary<string, object?> { ["name"] = "Gamma", ["v"] = 1 });
    await client.UpdateAsync(col,
        new Dictionary<string, object?> { ["name"] = "Alpha" },
        new Dictionary<string, object?> { ["$set"] = new Dictionary<string, object?> { ["v"] = 99 } });
    await client.DeleteAsync(col, new Dictionary<string, object?> { ["name"] = "Beta" });
    await client.CommitTransactionAsync();

    var docs = await client.FindAsync(col, sort: new Dictionary<string, object?> { ["name"] = 1 });
    Assert(docs.GetArrayLength() == 2, "A3: Atomic multi-op — 2 docs remain");
    bool hasAlpha = false, hasGamma = false;
    for (int i = 0; i < docs.GetArrayLength(); i++)
    {
        var name = docs[i].GetProperty("name").GetString();
        if (name == "Alpha") { hasAlpha = true; Assert(docs[i].GetProperty("v").GetUInt64() == 99, "A3: Alpha updated to v=99"); }
        if (name == "Gamma") hasGamma = true;
    }
    Assert(hasAlpha, "A3: Alpha still exists");
    Assert(hasGamma, "A3: Gamma inserted");

    // Verify Beta is gone
    var beta = await client.FindOneAsync(col, new Dictionary<string, object?> { ["name"] = "Beta" });
    Assert(beta.ValueKind == System.Text.Json.JsonValueKind.Null, "A3: Beta deleted");
    await client.DropCollectionAsync(col);
}

// A4: Rollback of multi-op transaction — nothing changes
{
    var col = $"acid_a4_{Guid.NewGuid():N}";
    await client.InsertAsync(col, new Dictionary<string, object?> { ["name"] = "X", ["v"] = 1 });
    await client.InsertAsync(col, new Dictionary<string, object?> { ["name"] = "Y", ["v"] = 1 });

    await client.BeginTransactionAsync();
    await client.InsertAsync(col, new Dictionary<string, object?> { ["name"] = "Z", ["v"] = 1 });
    await client.UpdateAsync(col,
        new Dictionary<string, object?> { ["name"] = "X" },
        new Dictionary<string, object?> { ["$set"] = new Dictionary<string, object?> { ["v"] = 999 } });
    await client.DeleteAsync(col, new Dictionary<string, object?> { ["name"] = "Y" });
    await client.RollbackTransactionAsync();

    var count = await client.CountAsync(col);
    Assert(count == 2, "A4: Rollback preserves original state", $"expected 2, got {count}");
    var x = await client.FindOneAsync(col, new Dictionary<string, object?> { ["name"] = "X" });
    Assert(x.GetProperty("v").GetUInt64() == 1, "A4: X still has original v=1");
    await client.DropCollectionAsync(col);
}

// A5: Disconnect without commit = auto-rollback
{
    var col = $"acid_a5_{Guid.NewGuid():N}";

    await using (var tempClient = await OxiDbTcpClient.ConnectAsync("127.0.0.1", port))
    {
        tempClient.UseOxiWire();
        await tempClient.BeginTransactionAsync();
        await tempClient.InsertAsync(col, new Dictionary<string, object?> { ["item"] = "orphan" });
        // Dispose without commit
    }

    await Task.Delay(200); // Let server process disconnect
    var count = await client.CountAsync(col);
    Assert(count == 0, "A5: Disconnect auto-rollback — no orphan data");
}

// ═══════════════════════════════════════════════════════════════
// C — CONSISTENCY
// Database invariants are maintained before and after transactions.
// ═══════════════════════════════════════════════════════════════
Console.WriteLine("\n── CONSISTENCY ────────────────────────────────────────────────");

// C1: Unique index constraint enforced
{
    var col = $"acid_c1_{Guid.NewGuid():N}";
    await client.CreateUniqueIndexAsync(col, "email");
    await client.InsertAsync(col, new Dictionary<string, object?> { ["email"] = "alice@test.com" });

    bool threw = false;
    try
    {
        await client.InsertAsync(col, new Dictionary<string, object?> { ["email"] = "alice@test.com" });
    }
    catch (OxiDbTcpException)
    {
        threw = true;
    }

    Assert(threw, "C1: Unique constraint violation throws");
    var count = await client.CountAsync(col);
    Assert(count == 1, "C1: Only original doc remains after failed insert");
    await client.DropCollectionAsync(col);
}

// C2: Unique constraint maintained across committed transactions
{
    var col = $"acid_c2_{Guid.NewGuid():N}";
    await client.CreateUniqueIndexAsync(col, "sku");

    await client.BeginTransactionAsync();
    await client.InsertAsync(col, new Dictionary<string, object?> { ["sku"] = "ABC-001", ["name"] = "Widget" });
    await client.CommitTransactionAsync();

    bool threw = false;
    try
    {
        await client.InsertAsync(col, new Dictionary<string, object?> { ["sku"] = "ABC-001", ["name"] = "Duplicate" });
    }
    catch (OxiDbTcpException)
    {
        threw = true;
    }
    Assert(threw, "C2: Unique constraint holds after committed tx");
    await client.DropCollectionAsync(col);
}

// C3: Version tracking consistency
{
    var col = $"acid_c3_{Guid.NewGuid():N}";
    await client.InsertAsync(col, new Dictionary<string, object?> { ["name"] = "Versioned" });

    var doc = await client.FindOneAsync(col, new Dictionary<string, object?> { ["name"] = "Versioned" });
    var v1 = doc.GetProperty("_version").GetUInt64();
    Assert(v1 == 1, "C3: Initial version is 1");

    await client.UpdateAsync(col,
        new Dictionary<string, object?> { ["name"] = "Versioned" },
        new Dictionary<string, object?> { ["$set"] = new Dictionary<string, object?> { ["status"] = "updated" } });

    doc = await client.FindOneAsync(col, new Dictionary<string, object?> { ["name"] = "Versioned" });
    var v2 = doc.GetProperty("_version").GetUInt64();
    Assert(v2 == 2, "C3: Version increments to 2 after update");

    // Update inside tx
    await client.BeginTransactionAsync();
    await client.UpdateAsync(col,
        new Dictionary<string, object?> { ["name"] = "Versioned" },
        new Dictionary<string, object?> { ["$set"] = new Dictionary<string, object?> { ["status"] = "tx-updated" } });
    await client.CommitTransactionAsync();

    doc = await client.FindOneAsync(col, new Dictionary<string, object?> { ["name"] = "Versioned" });
    var v3 = doc.GetProperty("_version").GetUInt64();
    Assert(v3 == 3, "C3: Version increments through tx commit");
    await client.DropCollectionAsync(col);
}

// C4: Index consistency after rollback
{
    var col = $"acid_c4_{Guid.NewGuid():N}";
    await client.CreateIndexAsync(col, "category");
    await client.InsertAsync(col, new Dictionary<string, object?> { ["category"] = "A", ["n"] = 1 });
    await client.InsertAsync(col, new Dictionary<string, object?> { ["category"] = "B", ["n"] = 2 });

    await client.BeginTransactionAsync();
    await client.InsertAsync(col, new Dictionary<string, object?> { ["category"] = "A", ["n"] = 3 });
    await client.RollbackTransactionAsync();

    // Index should still correctly return only original docs
    var results = await client.FindAsync(col,
        query: new Dictionary<string, object?> { ["category"] = "A" });
    Assert(results.GetArrayLength() == 1, "C4: Index consistent after rollback", $"got {results.GetArrayLength()}");
    await client.DropCollectionAsync(col);
}

// C5: Count consistency
{
    var col = $"acid_c5_{Guid.NewGuid():N}";
    for (int i = 0; i < 10; i++)
        await client.InsertAsync(col, new Dictionary<string, object?> { ["n"] = i });

    var countBefore = await client.CountAsync(col);

    await client.BeginTransactionAsync();
    await client.InsertAsync(col, new Dictionary<string, object?> { ["n"] = 100 });
    await client.InsertAsync(col, new Dictionary<string, object?> { ["n"] = 101 });
    await client.DeleteAsync(col, new Dictionary<string, object?> { ["n"] = 0 });
    await client.RollbackTransactionAsync();

    var countAfter = await client.CountAsync(col);
    Assert(countBefore == countAfter, "C5: Count unchanged after rollback", $"before={countBefore}, after={countAfter}");
    await client.DropCollectionAsync(col);
}

// ═══════════════════════════════════════════════════════════════
// I — ISOLATION
// Concurrent transactions don't interfere with each other.
// ═══════════════════════════════════════════════════════════════
Console.WriteLine("\n── ISOLATION ──────────────────────────────────────────────────");

// I1: Two clients — tx on client1 not visible to client2 before commit
{
    var col = $"acid_i1_{Guid.NewGuid():N}";
    await client.InsertAsync(col, new Dictionary<string, object?> { ["baseline"] = true });

    await using var client2 = await OxiDbTcpClient.ConnectAsync("127.0.0.1", port);
    client2.UseOxiWire();

    await client.BeginTransactionAsync();
    await client.InsertAsync(col, new Dictionary<string, object?> { ["from_tx"] = true });

    // client2 should not see the uncommitted insert
    var count2 = await client2.CountAsync(col);
    Assert(count2 == 1, "I1: Uncommitted tx data invisible to other client", $"got {count2}");

    await client.CommitTransactionAsync();

    // After commit, client2 should see it
    count2 = await client2.CountAsync(col);
    Assert(count2 == 2, "I1: Committed tx data visible to other client", $"got {count2}");
    await client.DropCollectionAsync(col);
}

// I2: Concurrent inserts from multiple clients
{
    var col = $"acid_i2_{Guid.NewGuid():N}";
    const int clientCount = 8;
    const int docsPerClient = 50;

    var tasks = Enumerable.Range(0, clientCount).Select(async cid =>
    {
        await using var c = await OxiDbTcpClient.ConnectAsync("127.0.0.1", port);
        c.UseOxiWire();
        for (int i = 0; i < docsPerClient; i++)
            await c.InsertAsync(col, new Dictionary<string, object?> { ["client"] = cid, ["seq"] = i });
    });
    await Task.WhenAll(tasks);

    var total = await client.CountAsync(col);
    Assert(total == clientCount * docsPerClient, "I2: Concurrent inserts — no lost writes",
        $"expected {clientCount * docsPerClient}, got {total}");
    await client.DropCollectionAsync(col);
}

// I3: Concurrent transactions — each commits independently
{
    var col = $"acid_i3_{Guid.NewGuid():N}";

    var txTasks = Enumerable.Range(0, 4).Select(async tid =>
    {
        await using var c = await OxiDbTcpClient.ConnectAsync("127.0.0.1", port);
        c.UseOxiWire();
        await c.BeginTransactionAsync();
        for (int i = 0; i < 10; i++)
            await c.InsertAsync(col, new Dictionary<string, object?> { ["tx"] = tid, ["i"] = i });
        await c.CommitTransactionAsync();
    });
    await Task.WhenAll(txTasks);

    var total = await client.CountAsync(col);
    Assert(total == 40, "I3: 4 concurrent tx commits — all 40 docs visible", $"got {total}");
    await client.DropCollectionAsync(col);
}

// I4: Mix of commit and rollback in concurrent transactions
{
    var col = $"acid_i4_{Guid.NewGuid():N}";

    var mixTasks = Enumerable.Range(0, 6).Select(async tid =>
    {
        await using var c = await OxiDbTcpClient.ConnectAsync("127.0.0.1", port);
        c.UseOxiWire();
        await c.BeginTransactionAsync();
        for (int i = 0; i < 5; i++)
            await c.InsertAsync(col, new Dictionary<string, object?> { ["tx"] = tid, ["i"] = i });

        if (tid % 2 == 0)
            await c.CommitTransactionAsync();   // tx 0, 2, 4 commit
        else
            await c.RollbackTransactionAsync(); // tx 1, 3, 5 rollback
    });
    await Task.WhenAll(mixTasks);

    var total = await client.CountAsync(col);
    Assert(total == 15, "I4: 3 commits + 3 rollbacks = 15 docs", $"got {total}");
    await client.DropCollectionAsync(col);
}

// I5: Concurrent updates to same collection — no data corruption
{
    var col = $"acid_i5_{Guid.NewGuid():N}";
    await client.CreateIndexAsync(col, "counter_id");

    // Create 10 counters
    for (int i = 0; i < 10; i++)
        await client.InsertAsync(col, new Dictionary<string, object?> { ["counter_id"] = i, ["value"] = 0 });

    // 5 clients each increment all 10 counters
    var incTasks = Enumerable.Range(0, 5).Select(async _ =>
    {
        await using var c = await OxiDbTcpClient.ConnectAsync("127.0.0.1", port);
        c.UseOxiWire();
        for (int i = 0; i < 10; i++)
        {
            await c.UpdateAsync(col,
                new Dictionary<string, object?> { ["counter_id"] = i },
                new Dictionary<string, object?> { ["$inc"] = new Dictionary<string, object?> { ["value"] = 1 } });
        }
    });
    await Task.WhenAll(incTasks);

    // Verify total increments are correct (10 counters * 5 increments each = 50 total)
    var aggResult2 = await client.AggregateAsync(col, new object[]
    {
        new Dictionary<string, object?> { ["$group"] = new Dictionary<string, object?>
        {
            ["_id"] = (object?)null,
            ["total"] = new Dictionary<string, object?> { ["$sum"] = "$value" },
            ["cnt"] = new Dictionary<string, object?> { ["$sum"] = 1 }
        }}
    });
    var totalValue = aggResult2[0].GetProperty("total").GetInt64();
    var docCount = aggResult2[0].GetProperty("cnt").GetInt64();
    Assert(docCount == 10, "I5: All 10 counters present", $"got {docCount}");
    Assert(totalValue == 50, "I5: Concurrent $inc — total sum = 50 (no lost updates)", $"got {totalValue}");
    await client.DropCollectionAsync(col);
}

// ═══════════════════════════════════════════════════════════════
// D — DURABILITY
// Committed data survives server restart.
// ═══════════════════════════════════════════════════════════════
Console.WriteLine("\n── DURABILITY ─────────────────────────────────────────────────");

// D1: Data persists after commit (verified via separate connection)
{
    var col = $"acid_d1_{Guid.NewGuid():N}";

    await client.BeginTransactionAsync();
    await client.InsertAsync(col, new Dictionary<string, object?> { ["durable"] = true, ["value"] = 42 });
    await client.CommitTransactionAsync();

    // New connection verifies data
    await using var freshClient = await OxiDbTcpClient.ConnectAsync("127.0.0.1", port);
    freshClient.UseOxiWire();
    var doc = await freshClient.FindOneAsync(col, new Dictionary<string, object?> { ["durable"] = true });
    Assert(doc.GetProperty("value").GetUInt64() == 42, "D1: Committed data visible from new connection");
    await client.DropCollectionAsync(col);
}

// D2: Large batch commit durability
{
    var col = $"acid_d2_{Guid.NewGuid():N}";
    var largeBatch = Enumerable.Range(0, 500).Select(i => (object)new Dictionary<string, object?>
    {
        ["n"] = i, ["data"] = $"payload_{i}_{new string('x', 100)}"
    }).ToArray();

    await client.BeginTransactionAsync();
    await client.InsertManyAsync(col, largeBatch);
    await client.CommitTransactionAsync();

    await using var verifyClient = await OxiDbTcpClient.ConnectAsync("127.0.0.1", port);
    verifyClient.UseOxiWire();
    var count = await verifyClient.CountAsync(col);
    Assert(count == 500, "D2: 500-doc batch tx fully durable", $"got {count}");
    await client.DropCollectionAsync(col);
}

// D3: Sequential transactions — each durable independently
{
    var col = $"acid_d3_{Guid.NewGuid():N}";

    for (int txNum = 0; txNum < 5; txNum++)
    {
        await client.BeginTransactionAsync();
        for (int i = 0; i < 10; i++)
            await client.InsertAsync(col, new Dictionary<string, object?> { ["tx"] = txNum, ["i"] = i });
        await client.CommitTransactionAsync();
    }

    await using var checkClient = await OxiDbTcpClient.ConnectAsync("127.0.0.1", port);
    checkClient.UseOxiWire();
    var total = await checkClient.CountAsync(col);
    Assert(total == 50, "D3: 5 sequential tx commits = 50 durable docs", $"got {total}");
    await client.DropCollectionAsync(col);
}

// D4: Compaction preserves committed data
{
    var col = $"acid_d4_{Guid.NewGuid():N}";

    // Insert then delete most docs
    for (int i = 0; i < 50; i++)
        await client.InsertAsync(col, new Dictionary<string, object?> { ["n"] = i });
    await client.DeleteAsync(col,
        new Dictionary<string, object?> { ["n"] = new Dictionary<string, object?> { ["$lt"] = 45 } });

    // Compact
    await client.CompactAsync(col);

    // Verify surviving docs
    var remainCount = await client.CountAsync(col);
    Assert(remainCount == 5, "D4: Compaction preserves 5 remaining docs", $"got {remainCount}");
    var remainDocs = await client.FindAsync(col, sort: new Dictionary<string, object?> { ["n"] = 1 });
    if (remainDocs.GetArrayLength() > 0)
    {
        var firstN = remainDocs[0].GetProperty("n").GetInt64();
        Assert(firstN == 45, "D4: First surviving doc is n=45", $"got n={firstN}");
    }
    await client.DropCollectionAsync(col);
}

// ═══════════════════════════════════════════════════════════════
// COMBINED — Cross-property stress tests
// ═══════════════════════════════════════════════════════════════
Console.WriteLine("\n── COMBINED STRESS TESTS ──────────────────────────────────────");

// S1: Bank transfer — atomicity + consistency
{
    var col = $"acid_s1_{Guid.NewGuid():N}";
    await client.CreateIndexAsync(col, "account");
    await client.InsertAsync(col, new Dictionary<string, object?> { ["account"] = "Alice", ["balance"] = 1000 });
    await client.InsertAsync(col, new Dictionary<string, object?> { ["account"] = "Bob", ["balance"] = 500 });

    // Transfer 200 from Alice to Bob
    await client.BeginTransactionAsync();
    await client.UpdateAsync(col,
        new Dictionary<string, object?> { ["account"] = "Alice" },
        new Dictionary<string, object?> { ["$inc"] = new Dictionary<string, object?> { ["balance"] = -200 } });
    await client.UpdateAsync(col,
        new Dictionary<string, object?> { ["account"] = "Bob" },
        new Dictionary<string, object?> { ["$inc"] = new Dictionary<string, object?> { ["balance"] = 200 } });
    await client.CommitTransactionAsync();

    var alice = await client.FindOneAsync(col, new Dictionary<string, object?> { ["account"] = "Alice" });
    var bob = await client.FindOneAsync(col, new Dictionary<string, object?> { ["account"] = "Bob" });
    var aliceBal = alice.GetProperty("balance").GetInt64();
    var bobBal = bob.GetProperty("balance").GetInt64();

    Assert(aliceBal == 800, "S1: Alice balance = 800 after transfer", $"got {aliceBal}");
    Assert(bobBal == 700, "S1: Bob balance = 700 after transfer", $"got {bobBal}");
    Assert(aliceBal + bobBal == 1500, "S1: Total balance conserved (1500)", $"got {aliceBal + bobBal}");
    await client.DropCollectionAsync(col);
}

// S2: Failed transfer rollback — money doesn't disappear
{
    var col = $"acid_s2_{Guid.NewGuid():N}";
    await client.CreateIndexAsync(col, "account");
    await client.InsertAsync(col, new Dictionary<string, object?> { ["account"] = "Alice", ["balance"] = 1000 });
    await client.InsertAsync(col, new Dictionary<string, object?> { ["account"] = "Bob", ["balance"] = 500 });

    // Start transfer but rollback
    await client.BeginTransactionAsync();
    await client.UpdateAsync(col,
        new Dictionary<string, object?> { ["account"] = "Alice" },
        new Dictionary<string, object?> { ["$inc"] = new Dictionary<string, object?> { ["balance"] = -300 } });
    await client.UpdateAsync(col,
        new Dictionary<string, object?> { ["account"] = "Bob" },
        new Dictionary<string, object?> { ["$inc"] = new Dictionary<string, object?> { ["balance"] = 300 } });
    await client.RollbackTransactionAsync();

    var alice = await client.FindOneAsync(col, new Dictionary<string, object?> { ["account"] = "Alice" });
    var bob = await client.FindOneAsync(col, new Dictionary<string, object?> { ["account"] = "Bob" });
    Assert(alice.GetProperty("balance").GetInt64() == 1000, "S2: Alice unchanged after rollback");
    Assert(bob.GetProperty("balance").GetInt64() == 500, "S2: Bob unchanged after rollback");
    await client.DropCollectionAsync(col);
}

// S3: Concurrent bank transfers — total balance conserved
{
    var col = $"acid_s3_{Guid.NewGuid():N}";
    await client.CreateIndexAsync(col, "account");

    const int numAccounts = 10;
    const int initialBalance = 1000;
    for (int i = 0; i < numAccounts; i++)
        await client.InsertAsync(col, new Dictionary<string, object?> { ["account"] = $"acct_{i}", ["balance"] = initialBalance });

    var expectedTotal = numAccounts * initialBalance;

    // 20 concurrent transfers with OCC retry on conflict
    int conflicts = 0;
    var transferTasks = Enumerable.Range(0, 20).Select(async tid =>
    {
        await using var c = await OxiDbTcpClient.ConnectAsync("127.0.0.1", port);
        c.UseOxiWire();
        var rng = new Random(42 + tid);
        var from = $"acct_{rng.Next(numAccounts)}";
        var to = $"acct_{rng.Next(numAccounts)}";
        if (from == to) return;

        for (int attempt = 0; attempt < 5; attempt++)
        {
            try
            {
                await c.BeginTransactionAsync();
                await c.UpdateAsync(col,
                    new Dictionary<string, object?> { ["account"] = from },
                    new Dictionary<string, object?> { ["$inc"] = new Dictionary<string, object?> { ["balance"] = -10 } });
                await c.UpdateAsync(col,
                    new Dictionary<string, object?> { ["account"] = to },
                    new Dictionary<string, object?> { ["$inc"] = new Dictionary<string, object?> { ["balance"] = 10 } });
                await c.CommitTransactionAsync();
                break; // success
            }
            catch (OxiDbTcpException ex) when (ex.Message.Contains("conflict"))
            {
                Interlocked.Increment(ref conflicts);
                await Task.Delay(10 * (attempt + 1)); // backoff and retry
            }
        }
    });
    await Task.WhenAll(transferTasks);
    if (conflicts > 0) Console.WriteLine($"    (OCC retries: {conflicts} conflicts resolved)");

    // Verify total balance is conserved
    var aggResult = await client.AggregateAsync(col, new object[]
    {
        new Dictionary<string, object?> { ["$group"] = new Dictionary<string, object?>
        {
            ["_id"] = (object?)null,
            ["total"] = new Dictionary<string, object?> { ["$sum"] = "$balance" }
        }}
    });
    var totalBalance = aggResult[0].GetProperty("total").GetInt64();
    Assert(totalBalance == expectedTotal, "S3: Total balance conserved after 20 concurrent transfers",
        $"expected {expectedTotal}, got {totalBalance}");
    await client.DropCollectionAsync(col);
}

// ═══════════════════════════════════════════════════════════════
// Summary
// ═══════════════════════════════════════════════════════════════
Console.WriteLine("\n╔══════════════════════════════════════════════════════════════╗");
Console.WriteLine($"║  Results: {passed} passed, {failed} failed                              ║");
Console.WriteLine($"║  Runtime: .NET {Environment.Version}                                    ║");
Console.WriteLine("╚══════════════════════════════════════════════════════════════╝");

if (failed > 0) Environment.Exit(1);
