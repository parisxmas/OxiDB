using System.Text;
using System.Text.Json.Nodes;

namespace OxiDb.IntegrationTests;

[Collection("OxiDb")]
public class FullIntegrationTests : IDisposable
{
    private readonly TestServerFixture _fixture;
    private readonly OxiDbClient _db;

    public FullIntegrationTests(TestServerFixture fixture)
    {
        _fixture = fixture;
        _db = fixture.CreateClient();
    }

    public void Dispose() => _db.Dispose();

    // ===================================================================
    // 1. Basic connectivity
    // ===================================================================

    [Fact]
    public void Ping_ReturnssPong()
    {
        Assert.Equal("pong", _db.Ping());
    }

    // ===================================================================
    // 2. Collection management
    // ===================================================================

    [Fact]
    public void CreateListDropCollection()
    {
        var col = $"col_mgmt_{Guid.NewGuid():N}";
        _db.CreateCollection(col);

        var list = _db.ListCollections();
        Assert.Contains(list, n => n!.GetValue<string>() == col);

        _db.DropCollection(col);
    }

    [Fact]
    public void CreateDuplicateCollection_Fails()
    {
        var col = $"dup_{Guid.NewGuid():N}";
        _db.CreateCollection(col);
        var ex = Assert.Throws<OxiDbException>(() => _db.CreateCollection(col));
        Assert.Contains("already exists", ex.Message);
    }

    // ===================================================================
    // 3. CRUD operations
    // ===================================================================

    [Fact]
    public void Insert_And_FindById()
    {
        var col = $"crud_{Guid.NewGuid():N}";
        var result = _db.Insert(col, new JsonObject
        {
            ["name"] = "Alice",
            ["age"] = 30,
            ["email"] = "alice@example.com"
        });
        var id = result["id"]!.GetValue<long>();
        Assert.True(id >= 1);

        var doc = _db.FindOne(col, new JsonObject { ["_id"] = id });
        Assert.NotNull(doc);
        Assert.Equal("Alice", doc!["name"]!.GetValue<string>());
        Assert.Equal(30, doc["age"]!.GetValue<int>());
    }

    [Fact]
    public void InsertMany_And_Count()
    {
        var col = $"batch_{Guid.NewGuid():N}";
        var docs = new JsonArray
        {
            new JsonObject { ["city"] = "London", ["pop"] = 9_000_000 },
            new JsonObject { ["city"] = "Paris", ["pop"] = 2_100_000 },
            new JsonObject { ["city"] = "Berlin", ["pop"] = 3_600_000 },
            new JsonObject { ["city"] = "Rome", ["pop"] = 2_800_000 },
            new JsonObject { ["city"] = "Madrid", ["pop"] = 3_200_000 },
        };
        var ids = _db.InsertMany(col, docs);
        Assert.Equal(5, ids.AsArray().Count);

        Assert.Equal(5, _db.Count(col));
    }

    [Fact]
    public void Update_ModifiesDocument()
    {
        var col = $"upd_{Guid.NewGuid():N}";
        _db.Insert(col, new JsonObject { ["name"] = "Bob", ["score"] = 50 });

        var result = _db.Update(col,
            new JsonObject { ["name"] = "Bob" },
            new JsonObject { ["$set"] = new JsonObject { ["score"] = 95 } });
        Assert.Equal(1, result["modified"]!.GetValue<int>());

        var doc = _db.FindOne(col, new JsonObject { ["name"] = "Bob" });
        Assert.Equal(95, doc!["score"]!.GetValue<int>());
    }

    [Fact]
    public void Update_IncrementOperator()
    {
        var col = $"inc_{Guid.NewGuid():N}";
        _db.Insert(col, new JsonObject { ["name"] = "Counter", ["value"] = 10 });

        _db.Update(col,
            new JsonObject { ["name"] = "Counter" },
            new JsonObject { ["$inc"] = new JsonObject { ["value"] = 5 } });

        var doc = _db.FindOne(col, new JsonObject { ["name"] = "Counter" });
        Assert.Equal(15, doc!["value"]!.GetValue<int>());
    }

    [Fact]
    public void Delete_RemovesMatchingDocs()
    {
        var col = $"del_{Guid.NewGuid():N}";
        _db.Insert(col, new JsonObject { ["status"] = "active", ["name"] = "Keep" });
        _db.Insert(col, new JsonObject { ["status"] = "inactive", ["name"] = "Remove1" });
        _db.Insert(col, new JsonObject { ["status"] = "inactive", ["name"] = "Remove2" });

        var result = _db.Delete(col, new JsonObject { ["status"] = "inactive" });
        Assert.Equal(2, result["deleted"]!.GetValue<int>());

        Assert.Equal(1, _db.Count(col));
        var remaining = _db.Find(col);
        Assert.Equal("Keep", remaining[0]!["name"]!.GetValue<string>());
    }

    [Fact]
    public void Find_WithQueryOperators()
    {
        var col = $"ops_{Guid.NewGuid():N}";
        for (int i = 0; i < 20; i++)
            _db.Insert(col, new JsonObject { ["n"] = i, ["label"] = $"item_{i}" });

        // Range query: n >= 5 AND n < 10
        var results = _db.Find(col, new JsonObject
        {
            ["n"] = new JsonObject { ["$gte"] = 5, ["$lt"] = 10 }
        });
        Assert.Equal(5, results.Count);
        foreach (var doc in results)
        {
            var n = doc!["n"]!.GetValue<int>();
            Assert.InRange(n, 5, 9);
        }
    }

    [Fact]
    public void FindOne_ReturnsNull_WhenNoMatch()
    {
        var col = $"none_{Guid.NewGuid():N}";
        _db.Insert(col, new JsonObject { ["x"] = 1 });
        var result = _db.FindOne(col, new JsonObject { ["x"] = 999 });
        // The server returns null as the data value
        Assert.True(result is null || result.ToString() == "null" || result.GetValueKind() == System.Text.Json.JsonValueKind.Null);
    }

    // ===================================================================
    // 4. Indexing
    // ===================================================================

    [Fact]
    public void CreateIndex_AcceleratesQueries()
    {
        var col = $"idx_{Guid.NewGuid():N}";

        // Insert docs first
        var docs = new JsonArray();
        for (int i = 0; i < 100; i++)
            docs.Add(new JsonObject { ["category"] = $"cat_{i % 10}", ["value"] = i });
        _db.InsertMany(col, docs);

        // Create index on category
        _db.CreateIndex(col, "category");

        // Query using the indexed field
        var results = _db.Find(col, new JsonObject { ["category"] = "cat_3" });
        Assert.Equal(10, results.Count);
        foreach (var doc in results)
            Assert.Equal("cat_3", doc!["category"]!.GetValue<string>());
    }

    [Fact]
    public void UniqueIndex_PreventssDuplicates()
    {
        var col = $"uniq_{Guid.NewGuid():N}";
        _db.CreateUniqueIndex(col, "email");

        _db.Insert(col, new JsonObject { ["email"] = "alice@test.com", ["name"] = "Alice" });

        var ex = Assert.Throws<OxiDbException>(() =>
            _db.Insert(col, new JsonObject { ["email"] = "alice@test.com", ["name"] = "NotAlice" }));
        Assert.Contains("unique", ex.Message, StringComparison.OrdinalIgnoreCase);

        // Different email should work
        _db.Insert(col, new JsonObject { ["email"] = "bob@test.com", ["name"] = "Bob" });
        Assert.Equal(2, _db.Count(col));
    }

    [Fact]
    public void CompositeIndex_MultiFieldLookup()
    {
        var col = $"comp_{Guid.NewGuid():N}";
        _db.InsertMany(col, new JsonArray
        {
            new JsonObject { ["dept"] = "eng", ["level"] = "senior", ["name"] = "Alice" },
            new JsonObject { ["dept"] = "eng", ["level"] = "junior", ["name"] = "Bob" },
            new JsonObject { ["dept"] = "sales", ["level"] = "senior", ["name"] = "Charlie" },
            new JsonObject { ["dept"] = "eng", ["level"] = "senior", ["name"] = "Diana" },
        });

        _db.CreateCompositeIndex(col, new JsonArray { "dept", "level" });

        var results = _db.Find(col, new JsonObject { ["dept"] = "eng", ["level"] = "senior" });
        Assert.Equal(2, results.Count);
    }

    [Fact]
    public void IndexedSort_WithSkipLimit()
    {
        var col = $"isort_{Guid.NewGuid():N}";
        for (int i = 0; i < 50; i++)
            _db.Insert(col, new JsonObject { ["rank"] = i, ["name"] = $"player_{i}" });

        _db.CreateIndex(col, "rank");

        // Sort by rank descending, skip 5, limit 10
        var results = _db.Find(col,
            query: new JsonObject(),
            sort: new JsonObject { ["rank"] = -1 },
            skip: 5,
            limit: 10);

        Assert.Equal(10, results.Count);
        Assert.Equal(44, results[0]!["rank"]!.GetValue<int>());  // 49 - 5 = 44
        Assert.Equal(35, results[9]!["rank"]!.GetValue<int>());  // 44 - 9 = 35
    }

    // ===================================================================
    // 5. Sort / Skip / Limit
    // ===================================================================

    [Fact]
    public void Sort_Ascending()
    {
        var col = $"sasc_{Guid.NewGuid():N}";
        _db.Insert(col, new JsonObject { ["n"] = 30, ["label"] = "C" });
        _db.Insert(col, new JsonObject { ["n"] = 10, ["label"] = "A" });
        _db.Insert(col, new JsonObject { ["n"] = 20, ["label"] = "B" });

        var results = _db.Find(col, sort: new JsonObject { ["n"] = 1 });
        Assert.Equal(3, results.Count);
        Assert.Equal("A", results[0]!["label"]!.GetValue<string>());
        Assert.Equal("B", results[1]!["label"]!.GetValue<string>());
        Assert.Equal("C", results[2]!["label"]!.GetValue<string>());
    }

    [Fact]
    public void Sort_Descending()
    {
        var col = $"sdesc_{Guid.NewGuid():N}";
        _db.Insert(col, new JsonObject { ["n"] = 30, ["label"] = "C" });
        _db.Insert(col, new JsonObject { ["n"] = 10, ["label"] = "A" });
        _db.Insert(col, new JsonObject { ["n"] = 20, ["label"] = "B" });

        var results = _db.Find(col, sort: new JsonObject { ["n"] = -1 });
        Assert.Equal(3, results.Count);
        Assert.Equal("C", results[0]!["label"]!.GetValue<string>());
        Assert.Equal("B", results[1]!["label"]!.GetValue<string>());
        Assert.Equal("A", results[2]!["label"]!.GetValue<string>());
    }

    [Fact]
    public void SkipAndLimit_Pagination()
    {
        var col = $"page_{Guid.NewGuid():N}";
        for (int i = 0; i < 25; i++)
            _db.Insert(col, new JsonObject { ["page_n"] = i });

        // Page 1: skip 0, limit 10
        var page1 = _db.Find(col, sort: new JsonObject { ["page_n"] = 1 }, limit: 10);
        Assert.Equal(10, page1.Count);
        Assert.Equal(0, page1[0]!["page_n"]!.GetValue<int>());

        // Page 2: skip 10, limit 10
        var page2 = _db.Find(col, sort: new JsonObject { ["page_n"] = 1 }, skip: 10, limit: 10);
        Assert.Equal(10, page2.Count);
        Assert.Equal(10, page2[0]!["page_n"]!.GetValue<int>());

        // Page 3: skip 20, limit 10 (only 5 remaining)
        var page3 = _db.Find(col, sort: new JsonObject { ["page_n"] = 1 }, skip: 20, limit: 10);
        Assert.Equal(5, page3.Count);
        Assert.Equal(20, page3[0]!["page_n"]!.GetValue<int>());
    }

    // ===================================================================
    // 6. Aggregation pipeline
    // ===================================================================

    [Fact]
    public void Aggregate_GroupByWithSum()
    {
        var col = $"agg_{Guid.NewGuid():N}";
        _db.InsertMany(col, new JsonArray
        {
            new JsonObject { ["dept"] = "eng", ["salary"] = 100_000 },
            new JsonObject { ["dept"] = "eng", ["salary"] = 120_000 },
            new JsonObject { ["dept"] = "sales", ["salary"] = 80_000 },
            new JsonObject { ["dept"] = "sales", ["salary"] = 90_000 },
            new JsonObject { ["dept"] = "eng", ["salary"] = 110_000 },
        });

        var pipeline = new JsonArray
        {
            new JsonObject
            {
                ["$group"] = new JsonObject
                {
                    ["_id"] = "$dept",
                    ["total_salary"] = new JsonObject { ["$sum"] = "$salary" },
                    ["count"] = new JsonObject { ["$sum"] = 1 },
                }
            },
            new JsonObject { ["$sort"] = new JsonObject { ["_id"] = 1 } }
        };

        var results = _db.Aggregate(col, pipeline);
        Assert.Equal(2, results.Count);

        var eng = results.First(r => r!["_id"]!.GetValue<string>() == "eng");
        Assert.Equal(330_000, eng!["total_salary"]!.GetValue<int>());
        Assert.Equal(3, eng["count"]!.GetValue<int>());

        var sales = results.First(r => r!["_id"]!.GetValue<string>() == "sales");
        Assert.Equal(170_000, sales!["total_salary"]!.GetValue<int>());
        Assert.Equal(2, sales["count"]!.GetValue<int>());
    }

    [Fact]
    public void Aggregate_MatchGroupSortLimit()
    {
        var col = $"agg2_{Guid.NewGuid():N}";
        _db.InsertMany(col, new JsonArray
        {
            new JsonObject { ["product"] = "Widget", ["amount"] = 10, ["region"] = "NA" },
            new JsonObject { ["product"] = "Widget", ["amount"] = 20, ["region"] = "EU" },
            new JsonObject { ["product"] = "Gadget", ["amount"] = 30, ["region"] = "NA" },
            new JsonObject { ["product"] = "Gadget", ["amount"] = 40, ["region"] = "EU" },
            new JsonObject { ["product"] = "Gizmo", ["amount"] = 5, ["region"] = "NA" },
        });

        var pipeline = new JsonArray
        {
            new JsonObject { ["$match"] = new JsonObject { ["region"] = "NA" } },
            new JsonObject
            {
                ["$group"] = new JsonObject
                {
                    ["_id"] = "$product",
                    ["total"] = new JsonObject { ["$sum"] = "$amount" }
                }
            },
            new JsonObject { ["$sort"] = new JsonObject { ["total"] = -1 } },
            new JsonObject { ["$limit"] = 2 }
        };

        var results = _db.Aggregate(col, pipeline);
        Assert.Equal(2, results.Count);
        Assert.Equal("Gadget", results[0]!["_id"]!.GetValue<string>());
        Assert.Equal(30, results[0]!["total"]!.GetValue<int>());
    }

    [Fact]
    public void Aggregate_CountStage()
    {
        var col = $"cnt_{Guid.NewGuid():N}";
        for (int i = 0; i < 15; i++)
            _db.Insert(col, new JsonObject { ["active"] = i % 3 == 0 });

        var pipeline = new JsonArray
        {
            new JsonObject { ["$match"] = new JsonObject { ["active"] = true } },
            new JsonObject { ["$count"] = "active_count" }
        };

        var results = _db.Aggregate(col, pipeline);
        Assert.Single(results);
        Assert.Equal(5, results[0]!["active_count"]!.GetValue<int>());
    }

    // ===================================================================
    // 7. Transactions
    // ===================================================================

    [Fact]
    public void Transaction_CommitMakesDataVisible()
    {
        var col = $"tx_commit_{Guid.NewGuid():N}";

        _db.BeginTx();
        _db.Insert(col, new JsonObject { ["item"] = "TxItem" });
        _db.CommitTx();

        using var reader = _fixture.CreateClient();
        var docs = reader.Find(col);
        Assert.Single(docs);
        Assert.Equal("TxItem", docs[0]!["item"]!.GetValue<string>());
    }

    [Fact]
    public void Transaction_RollbackDiscardsData()
    {
        var col = $"tx_rb_{Guid.NewGuid():N}";

        _db.BeginTx();
        _db.Insert(col, new JsonObject { ["item"] = "Ghost" });
        _db.RollbackTx();

        var docs = _db.Find(col);
        Assert.Empty(docs);
    }

    [Fact]
    public void Transaction_InsertUpdateDeleteCommit()
    {
        var col = $"tx_multi_{Guid.NewGuid():N}";

        // Seed docs outside tx
        _db.Insert(col, new JsonObject { ["name"] = "Alpha", ["v"] = 1 });
        _db.Insert(col, new JsonObject { ["name"] = "Beta", ["v"] = 1 });

        // Transaction: insert Gamma, update Alpha, delete Beta
        _db.BeginTx();
        _db.Insert(col, new JsonObject { ["name"] = "Gamma", ["v"] = 1 });
        _db.Update(col,
            new JsonObject { ["name"] = "Alpha" },
            new JsonObject { ["$set"] = new JsonObject { ["v"] = 2 } });
        _db.Delete(col, new JsonObject { ["name"] = "Beta" });
        _db.CommitTx();

        var docs = _db.Find(col, sort: new JsonObject { ["name"] = 1 });
        Assert.Equal(2, docs.Count);

        var alpha = docs.First(d => d!["name"]!.GetValue<string>() == "Alpha");
        Assert.Equal(2, alpha!["v"]!.GetValue<int>());

        Assert.Contains(docs, d => d!["name"]!.GetValue<string>() == "Gamma");
        Assert.DoesNotContain(docs, d => d!["name"]!.GetValue<string>() == "Beta");
    }

    [Fact]
    public void Transaction_DisconnectAutoRollback()
    {
        var col = $"tx_dc_{Guid.NewGuid():N}";

        // Start a tx, insert, then disconnect (dispose) without commit
        using (var tempClient = _fixture.CreateClient())
        {
            tempClient.BeginTx();
            tempClient.Insert(col, new JsonObject { ["item"] = "LostItem" });
            // Dispose triggers disconnect → server auto-rollback
        }

        Thread.Sleep(200); // give server a moment

        var docs = _db.Find(col);
        Assert.Empty(docs);
    }

    // ===================================================================
    // 8. Blob storage
    // ===================================================================

    [Fact]
    public void Blob_PutGetHeadDelete()
    {
        var bucket = $"bkt_{Guid.NewGuid():N}";
        _db.CreateBucket(bucket);

        var data = Encoding.UTF8.GetBytes("Hello, OxiDB blob storage!");
        var meta = _db.PutObject(bucket, "greeting.txt", data, "text/plain",
            new JsonObject { ["author"] = "test" });

        Assert.Equal("greeting.txt", meta["key"]!.GetValue<string>());
        Assert.Equal(26, meta["size"]!.GetValue<int>());

        // Get
        var (retrieved, getMeta) = _db.GetObject(bucket, "greeting.txt");
        Assert.Equal(data, retrieved);
        Assert.Equal("text/plain", getMeta["content_type"]!.GetValue<string>());

        // Head (metadata only)
        var headMeta = _db.HeadObject(bucket, "greeting.txt");
        Assert.Equal(26, headMeta["size"]!.GetValue<int>());

        // Delete
        _db.DeleteObject(bucket, "greeting.txt");
        var ex = Assert.Throws<OxiDbException>(() => _db.GetObject(bucket, "greeting.txt"));
        Assert.Contains("not found", ex.Message);
    }

    [Fact]
    public void Blob_ListObjectsWithPrefix()
    {
        var bucket = $"lst_{Guid.NewGuid():N}";
        _db.CreateBucket(bucket);

        _db.PutObject(bucket, "images/photo1.jpg", new byte[] { 1, 2, 3 }, "image/jpeg");
        _db.PutObject(bucket, "images/photo2.jpg", new byte[] { 4, 5, 6 }, "image/jpeg");
        _db.PutObject(bucket, "docs/report.pdf", new byte[] { 7, 8, 9 }, "application/pdf");

        var images = _db.ListObjects(bucket, prefix: "images/");
        Assert.Equal(2, images.Count);

        var allObjects = _db.ListObjects(bucket);
        Assert.Equal(3, allObjects.Count);
    }

    [Fact]
    public void Blob_BucketManagement()
    {
        var b1 = $"bm1_{Guid.NewGuid():N}";
        var b2 = $"bm2_{Guid.NewGuid():N}";
        _db.CreateBucket(b1);
        _db.CreateBucket(b2);

        var buckets = _db.ListBuckets();
        Assert.Contains(buckets, b => b!.GetValue<string>() == b1);
        Assert.Contains(buckets, b => b!.GetValue<string>() == b2);

        _db.DeleteBucket(b1);
        buckets = _db.ListBuckets();
        Assert.DoesNotContain(buckets, b => b!.GetValue<string>() == b1);
    }

    [Fact]
    public void Blob_OverwriteExistingKey()
    {
        var bucket = $"ow_{Guid.NewGuid():N}";
        _db.PutObject(bucket, "file.txt", Encoding.UTF8.GetBytes("version 1"), "text/plain");
        _db.PutObject(bucket, "file.txt", Encoding.UTF8.GetBytes("version 2"), "text/plain");

        var (data, _) = _db.GetObject(bucket, "file.txt");
        Assert.Equal("version 2", Encoding.UTF8.GetString(data));

        // Only one object should exist
        var list = _db.ListObjects(bucket);
        Assert.Single(list);
    }

    // ===================================================================
    // 9. Document upload and full-text search indexing
    // ===================================================================

    [Fact]
    public void FullTextSearch_UploadTextAndSearch()
    {
        var bucket = $"fts_{Guid.NewGuid():N}";

        // Upload a text document
        var textContent = "The quick brown fox jumps over the lazy dog. " +
                          "This document discusses machine learning and artificial intelligence.";
        _db.PutObject(bucket, "article.txt", Encoding.UTF8.GetBytes(textContent), "text/plain");

        // Upload another document with different content
        var textContent2 = "Database systems provide efficient storage and retrieval mechanisms. " +
                           "SQL and NoSQL are common paradigms.";
        _db.PutObject(bucket, "db_article.txt", Encoding.UTF8.GetBytes(textContent2), "text/plain");

        // Give the async FTS indexer a moment to process
        Thread.Sleep(500);

        // Search for terms in the first document
        var results = _db.Search("machine learning", bucket: bucket);
        Assert.NotEmpty(results);
        Assert.Contains(results, r => r!["key"]!.GetValue<string>() == "article.txt");

        // Search for terms in the second document
        var results2 = _db.Search("database storage", bucket: bucket);
        Assert.NotEmpty(results2);
        Assert.Contains(results2, r => r!["key"]!.GetValue<string>() == "db_article.txt");
    }

    [Fact]
    public void FullTextSearch_UploadHtmlAndSearch()
    {
        var bucket = $"html_{Guid.NewGuid():N}";

        var html = "<html><body><h1>Rust Programming</h1>" +
                   "<p>Rust is a systems programming language focused on safety and performance.</p>" +
                   "<p>Memory safety without garbage collection is a key feature.</p></body></html>";
        _db.PutObject(bucket, "rust_intro.html", Encoding.UTF8.GetBytes(html), "text/html");

        Thread.Sleep(500);

        var results = _db.Search("rust safety performance", bucket: bucket);
        Assert.NotEmpty(results);
        Assert.Contains(results, r => r!["key"]!.GetValue<string>() == "rust_intro.html");
    }

    [Fact]
    public void FullTextSearch_UploadJsonAndSearch()
    {
        var bucket = $"json_fts_{Guid.NewGuid():N}";

        var jsonDoc = """
        {
            "title": "Quantum Computing Primer",
            "abstract": "Quantum computers use qubits to perform calculations exponentially faster.",
            "keywords": ["quantum", "qubits", "superposition", "entanglement"]
        }
        """;
        _db.PutObject(bucket, "quantum.json", Encoding.UTF8.GetBytes(jsonDoc), "application/json");

        Thread.Sleep(500);

        var results = _db.Search("quantum qubits", bucket: bucket);
        Assert.NotEmpty(results);
        Assert.Contains(results, r => r!["key"]!.GetValue<string>() == "quantum.json");
    }

    [Fact]
    public void FullTextSearch_CrossBucketSearch()
    {
        var b1 = $"xb1_{Guid.NewGuid():N}";
        var b2 = $"xb2_{Guid.NewGuid():N}";

        _db.PutObject(b1, "doc1.txt",
            Encoding.UTF8.GetBytes("Kubernetes container orchestration platform"), "text/plain");
        _db.PutObject(b2, "doc2.txt",
            Encoding.UTF8.GetBytes("Kubernetes cluster management and deployment"), "text/plain");

        Thread.Sleep(500);

        // Search across all buckets (no bucket filter)
        var results = _db.Search("kubernetes");
        Assert.True(results.Count >= 2, $"Expected at least 2 results, got {results.Count}");
    }

    [Fact]
    public void FullTextSearch_DeleteObjectRemovesFromIndex()
    {
        var bucket = $"ftsdel_{Guid.NewGuid():N}";

        _db.PutObject(bucket, "ephemeral.txt",
            Encoding.UTF8.GetBytes("Ephemeral content about blockchain distributed ledger"), "text/plain");

        Thread.Sleep(500);

        var before = _db.Search("blockchain distributed", bucket: bucket);
        Assert.NotEmpty(before);

        _db.DeleteObject(bucket, "ephemeral.txt");
        Thread.Sleep(500);

        var after = _db.Search("blockchain distributed", bucket: bucket);
        Assert.Empty(after);
    }

    // ===================================================================
    // 10. Compaction
    // ===================================================================

    [Fact]
    public void Compact_ReclaimsSpace()
    {
        var col = $"compact_{Guid.NewGuid():N}";

        // Insert 20 docs
        for (int i = 0; i < 20; i++)
            _db.Insert(col, new JsonObject { ["n"] = i, ["payload"] = new string('x', 200) });

        // Delete 15
        _db.Delete(col, new JsonObject { ["n"] = new JsonObject { ["$lt"] = 15 } });
        Assert.Equal(5, _db.Count(col));

        // Compact
        var stats = _db.Compact(col);
        Assert.Equal(5, stats["docs_kept"]!.GetValue<int>());
        Assert.True(stats["new_size"]!.GetValue<long>() < stats["old_size"]!.GetValue<long>());

        // Remaining docs still accessible
        var docs = _db.Find(col, sort: new JsonObject { ["n"] = 1 });
        Assert.Equal(5, docs.Count);
        Assert.Equal(15, docs[0]!["n"]!.GetValue<int>());
    }

    // ===================================================================
    // 11. Complex real-world scenario: E-commerce
    // ===================================================================

    [Fact]
    public void Scenario_ECommerce_FullWorkflow()
    {
        var products = $"products_{Guid.NewGuid():N}";
        var orders = $"orders_{Guid.NewGuid():N}";

        // Create indexes
        _db.CreateIndex(products, "category");
        _db.CreateUniqueIndex(products, "sku");
        _db.CreateIndex(orders, "customer_id");
        _db.CreateIndex(orders, "status");

        // Insert products
        _db.InsertMany(products, new JsonArray
        {
            new JsonObject { ["sku"] = "LAPTOP-001", ["name"] = "Pro Laptop", ["category"] = "electronics", ["price"] = 1299.99 },
            new JsonObject { ["sku"] = "MOUSE-001", ["name"] = "Wireless Mouse", ["category"] = "electronics", ["price"] = 29.99 },
            new JsonObject { ["sku"] = "DESK-001", ["name"] = "Standing Desk", ["category"] = "furniture", ["price"] = 499.99 },
            new JsonObject { ["sku"] = "CHAIR-001", ["name"] = "Ergonomic Chair", ["category"] = "furniture", ["price"] = 349.99 },
            new JsonObject { ["sku"] = "BOOK-001", ["name"] = "Rust Programming", ["category"] = "books", ["price"] = 39.99 },
        });

        // Insert orders
        _db.InsertMany(orders, new JsonArray
        {
            new JsonObject { ["customer_id"] = 1, ["sku"] = "LAPTOP-001", ["qty"] = 1, ["status"] = "shipped" },
            new JsonObject { ["customer_id"] = 1, ["sku"] = "MOUSE-001", ["qty"] = 2, ["status"] = "delivered" },
            new JsonObject { ["customer_id"] = 2, ["sku"] = "DESK-001", ["qty"] = 1, ["status"] = "shipped" },
            new JsonObject { ["customer_id"] = 2, ["sku"] = "CHAIR-001", ["qty"] = 1, ["status"] = "pending" },
            new JsonObject { ["customer_id"] = 3, ["sku"] = "BOOK-001", ["qty"] = 3, ["status"] = "delivered" },
            new JsonObject { ["customer_id"] = 3, ["sku"] = "LAPTOP-001", ["qty"] = 1, ["status"] = "delivered" },
        });

        // Query: electronics products
        var electronics = _db.Find(products, new JsonObject { ["category"] = "electronics" });
        Assert.Equal(2, electronics.Count);

        // Query: customer 1's orders
        var custOrders = _db.Find(orders, new JsonObject { ["customer_id"] = 1 });
        Assert.Equal(2, custOrders.Count);

        // Aggregate: total orders by status
        var statusAgg = _db.Aggregate(orders, new JsonArray
        {
            new JsonObject
            {
                ["$group"] = new JsonObject
                {
                    ["_id"] = "$status",
                    ["count"] = new JsonObject { ["$sum"] = 1 }
                }
            },
            new JsonObject { ["$sort"] = new JsonObject { ["count"] = -1 } }
        });

        Assert.Equal(3, statusAgg.Count); // shipped, delivered, pending
        var delivered = statusAgg.First(r => r!["_id"]!.GetValue<string>() == "delivered");
        Assert.Equal(3, delivered!["count"]!.GetValue<int>());

        // Unique constraint: duplicate SKU should fail
        var ex = Assert.Throws<OxiDbException>(() =>
            _db.Insert(products, new JsonObject { ["sku"] = "LAPTOP-001", ["name"] = "Dup" }));
        Assert.Contains("unique", ex.Message, StringComparison.OrdinalIgnoreCase);

        // Update: mark pending orders as shipped
        var updated = _db.Update(orders,
            new JsonObject { ["status"] = "pending" },
            new JsonObject { ["$set"] = new JsonObject { ["status"] = "shipped" } });
        Assert.Equal(1, updated["modified"]!.GetValue<int>());

        // Verify no more pending
        Assert.Equal(0, _db.Count(orders, new JsonObject { ["status"] = "pending" }));
    }

    // ===================================================================
    // 12. Complex real-world scenario: Document management with FTS
    // ===================================================================

    [Fact]
    public void Scenario_DocumentManagement_UploadSearchRetrieve()
    {
        var metaCol = $"doc_meta_{Guid.NewGuid():N}";
        var bucket = $"doc_store_{Guid.NewGuid():N}";

        _db.CreateIndex(metaCol, "content_type");
        _db.CreateIndex(metaCol, "tags");

        // Upload documents to blob store and track metadata in collection
        var docs = new[]
        {
            ("report_q1.txt", "text/plain",
             "Q1 Financial Report: Revenue increased by 15% compared to last quarter. " +
             "Operating expenses remained stable. Net profit margin improved significantly.",
             new[] { "finance", "quarterly" }),
            ("architecture.txt", "text/plain",
             "System Architecture Document: The microservices architecture uses gRPC for " +
             "inter-service communication. Each service has its own database following the " +
             "database-per-service pattern.",
             new[] { "engineering", "architecture" }),
            ("onboarding.txt", "text/plain",
             "Employee Onboarding Guide: New employees should complete security training " +
             "within the first week. Access to production systems requires manager approval.",
             new[] { "hr", "onboarding" }),
        };

        foreach (var (key, ct, content, tags) in docs)
        {
            _db.PutObject(bucket, key, Encoding.UTF8.GetBytes(content), ct);
            _db.Insert(metaCol, new JsonObject
            {
                ["key"] = key,
                ["bucket"] = bucket,
                ["content_type"] = ct,
                ["size"] = content.Length,
                ["tags"] = new JsonArray(tags.Select(t => (JsonNode)JsonValue.Create(t)!).ToArray()),
            });
        }

        // Wait for FTS indexing
        Thread.Sleep(500);

        // Search for financial content
        var financeResults = _db.Search("revenue profit financial", bucket: bucket);
        Assert.NotEmpty(financeResults);
        Assert.Contains(financeResults, r => r!["key"]!.GetValue<string>() == "report_q1.txt");

        // Search for architecture content
        var archResults = _db.Search("microservices database", bucket: bucket);
        Assert.NotEmpty(archResults);
        Assert.Contains(archResults, r => r!["key"]!.GetValue<string>() == "architecture.txt");

        // Query metadata collection
        Assert.Equal(3, _db.Count(metaCol));

        // Retrieve the actual document content
        var (reportData, _) = _db.GetObject(bucket, "report_q1.txt");
        var reportText = Encoding.UTF8.GetString(reportData);
        Assert.Contains("Revenue increased", reportText);
    }

    // ===================================================================
    // 13. Date range queries
    // ===================================================================

    [Fact]
    public void DateRange_QueryWithIndex()
    {
        var col = $"dates_{Guid.NewGuid():N}";
        _db.CreateIndex(col, "created_at");

        _db.InsertMany(col, new JsonArray
        {
            new JsonObject { ["title"] = "Old", ["created_at"] = "2023-01-15T10:00:00Z" },
            new JsonObject { ["title"] = "Mid", ["created_at"] = "2024-06-15T10:00:00Z" },
            new JsonObject { ["title"] = "New", ["created_at"] = "2025-01-15T10:00:00Z" },
        });

        var results = _db.Find(col, new JsonObject
        {
            ["created_at"] = new JsonObject
            {
                ["$gte"] = "2024-01-01",
                ["$lt"] = "2025-01-01"
            }
        });
        Assert.Single(results);
        Assert.Equal("Mid", results[0]!["title"]!.GetValue<string>());
    }

    // ===================================================================
    // 14. Nested document operations
    // ===================================================================

    [Fact]
    public void NestedFields_SetAndQuery()
    {
        var col = $"nested_{Guid.NewGuid():N}";
        _db.Insert(col, new JsonObject
        {
            ["user"] = new JsonObject
            {
                ["name"] = "Alice",
                ["address"] = new JsonObject
                {
                    ["city"] = "London",
                    ["country"] = "UK"
                }
            }
        });

        // Update nested field using dot notation
        _db.Update(col,
            new JsonObject { ["user.name"] = "Alice" },
            new JsonObject { ["$set"] = new JsonObject { ["user.address.city"] = "Manchester" } });

        var doc = _db.FindOne(col, new JsonObject { ["user.name"] = "Alice" });
        Assert.Equal("Manchester", doc!["user"]!["address"]!["city"]!.GetValue<string>());
    }

    // ===================================================================
    // 15. Array operations
    // ===================================================================

    [Fact]
    public void ArrayOperations_PushPull()
    {
        var col = $"arr_{Guid.NewGuid():N}";
        _db.Insert(col, new JsonObject
        {
            ["name"] = "TaggedItem",
            ["tags"] = new JsonArray { "initial" }
        });

        // Push a new tag
        _db.Update(col,
            new JsonObject { ["name"] = "TaggedItem" },
            new JsonObject { ["$push"] = new JsonObject { ["tags"] = "added" } });

        var doc = _db.FindOne(col, new JsonObject { ["name"] = "TaggedItem" });
        var tags = doc!["tags"]!.AsArray();
        Assert.Equal(2, tags.Count);
        Assert.Contains(tags, t => t!.GetValue<string>() == "added");

        // Pull the initial tag
        _db.Update(col,
            new JsonObject { ["name"] = "TaggedItem" },
            new JsonObject { ["$pull"] = new JsonObject { ["tags"] = "initial" } });

        doc = _db.FindOne(col, new JsonObject { ["name"] = "TaggedItem" });
        tags = doc!["tags"]!.AsArray();
        Assert.Single(tags);
        Assert.Equal("added", tags[0]!.GetValue<string>());
    }

    // ===================================================================
    // 16. Concurrent operations
    // ===================================================================

    [Fact]
    public void Concurrent_MultipleClientsInsert()
    {
        var col = $"conc_{Guid.NewGuid():N}";
        const int clientCount = 4;
        const int docsPerClient = 25;

        var tasks = Enumerable.Range(0, clientCount).Select(clientId =>
            Task.Run(() =>
            {
                using var client = _fixture.CreateClient();
                for (int i = 0; i < docsPerClient; i++)
                    client.Insert(col, new JsonObject
                    {
                        ["client"] = clientId,
                        ["seq"] = i
                    });
            }));

        Task.WaitAll(tasks.ToArray());

        Assert.Equal(clientCount * docsPerClient, _db.Count(col));
    }

    // ===================================================================
    // 17. Error handling
    // ===================================================================

    [Fact]
    public void Error_UpdateWithoutOperator()
    {
        var col = $"err_{Guid.NewGuid():N}";
        _db.Insert(col, new JsonObject { ["x"] = 1 });

        var ex = Assert.Throws<OxiDbException>(() =>
            _db.Update(col, new JsonObject { ["x"] = 1 }, new JsonObject()));
        Assert.Contains("operator", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Error_InsertNonObject()
    {
        var col = $"errobj_{Guid.NewGuid():N}";
        // Send raw request with doc as a string instead of object
        var resp = _db.Send(new JsonObject
        {
            ["cmd"] = "insert",
            ["collection"] = col,
            ["doc"] = "not an object"
        });
        Assert.False(resp["ok"]!.GetValue<bool>());
    }

    [Fact]
    public void Error_UnknownCommand()
    {
        var resp = _db.Send(new JsonObject { ["cmd"] = "nonexistent_cmd" });
        Assert.False(resp["ok"]!.GetValue<bool>());
        Assert.Contains("unknown command", resp["error"]!.GetValue<string>());
    }

    // ===================================================================
    // 18. Aggregation pipeline - Unwind
    // ===================================================================

    [Fact]
    public void Aggregate_Unwind()
    {
        var col = $"unwind_{Guid.NewGuid():N}";
        _db.InsertMany(col, new JsonArray
        {
            new JsonObject { ["name"] = "Alice", ["skills"] = new JsonArray { "rust", "python", "go" } },
            new JsonObject { ["name"] = "Bob", ["skills"] = new JsonArray { "rust", "java" } },
        });

        var pipeline = new JsonArray
        {
            new JsonObject { ["$unwind"] = "$skills" },
            new JsonObject
            {
                ["$group"] = new JsonObject
                {
                    ["_id"] = "$skills",
                    ["count"] = new JsonObject { ["$sum"] = 1 }
                }
            },
            new JsonObject { ["$sort"] = new JsonObject { ["count"] = -1 } }
        };

        var results = _db.Aggregate(col, pipeline);
        // "rust" appears in both docs
        Assert.Equal("rust", results[0]!["_id"]!.GetValue<string>());
        Assert.Equal(2, results[0]!["count"]!.GetValue<int>());
    }

    // ===================================================================
    // 19. Version tracking
    // ===================================================================

    [Fact]
    public void VersionTracking_AutoIncrement()
    {
        var col = $"ver_{Guid.NewGuid():N}";
        _db.Insert(col, new JsonObject { ["name"] = "Versioned" });

        var doc = _db.FindOne(col, new JsonObject { ["name"] = "Versioned" });
        Assert.Equal(1, doc!["_version"]!.GetValue<int>());

        _db.Update(col,
            new JsonObject { ["name"] = "Versioned" },
            new JsonObject { ["$set"] = new JsonObject { ["status"] = "updated" } });

        doc = _db.FindOne(col, new JsonObject { ["name"] = "Versioned" });
        Assert.Equal(2, doc!["_version"]!.GetValue<int>());
    }

    // ===================================================================
    // 20. Large batch operations
    // ===================================================================

    [Fact]
    public void LargeBatch_InsertAndQueryThousandDocs()
    {
        var col = $"large_{Guid.NewGuid():N}";
        const int batchSize = 500;
        const int batches = 2;

        for (int b = 0; b < batches; b++)
        {
            var docs = new JsonArray();
            for (int i = 0; i < batchSize; i++)
            {
                docs.Add(new JsonObject
                {
                    ["batch"] = b,
                    ["seq"] = i,
                    ["value"] = b * batchSize + i,
                    ["label"] = $"doc_{b}_{i}"
                });
            }
            _db.InsertMany(col, docs);
        }

        Assert.Equal(batchSize * batches, _db.Count(col));

        // Create index and query
        _db.CreateIndex(col, "value");

        var results = _db.Find(col, new JsonObject
        {
            ["value"] = new JsonObject { ["$gte"] = 400, ["$lt"] = 410 }
        });
        Assert.Equal(10, results.Count);

        // Sort + limit on large dataset
        var top5 = _db.Find(col, sort: new JsonObject { ["value"] = -1 }, limit: 5);
        Assert.Equal(5, top5.Count);
        Assert.Equal(999, top5[0]!["value"]!.GetValue<int>());
    }
}
