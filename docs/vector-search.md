# Vector Search

OxiDB supports vector similarity search for AI/ML applications that store embeddings. Create a vector index on any field containing numeric arrays, then query for the k-nearest neighbors using cosine, Euclidean, or dot product distance.

## Creating a Vector Index

```json
{
  "command": "create_vector_index",
  "collection": "articles",
  "field": "embedding",
  "dimension": 384,
  "metric": "cosine"
}
```

- **collection**: The collection to index
- **field**: The document field containing the vector (supports dot notation, e.g. `"meta.embedding"`)
- **dimension**: The expected length of each vector (all vectors must match)
- **metric** (optional): Distance metric, one of `"cosine"` (default), `"euclidean"`, or `"dot_product"`

Existing documents in the collection are backfilled into the index on creation. Documents inserted or updated afterward are automatically indexed.

## Inserting Documents with Vectors

Insert documents with a numeric array field matching the indexed field name:

```json
{
  "command": "insert",
  "collection": "articles",
  "doc": {
    "title": "Introduction to Rust",
    "embedding": [0.12, -0.34, 0.56, ...]
  }
}
```

The vector must have exactly `dimension` elements. Documents with missing or malformed vectors are silently skipped during indexing.

## Searching

```json
{
  "command": "vector_search",
  "collection": "articles",
  "field": "embedding",
  "vector": [0.15, -0.30, 0.50, ...],
  "limit": 10
}
```

- **vector**: The query vector (must match the index dimension)
- **limit** (optional): Number of results to return (default: 10)
- **ef_search** (optional): HNSW search beam width for tuning recall vs speed (default: 50)

### Response

Results are sorted by similarity (most similar first) and include the full document plus two extra fields:

```json
{
  "ok": true,
  "data": [
    {
      "_id": 42,
      "title": "Introduction to Rust",
      "embedding": [0.12, -0.34, 0.56, ...],
      "_similarity": 0.95,
      "_distance": 0.05
    },
    ...
  ]
}
```

- **`_similarity`**: A 0-1 score where 1.0 means identical (higher is better)
- **`_distance`**: Raw distance value (lower is better, scale depends on metric)

## Distance Metrics

| Metric | Formula | Best For |
|--------|---------|----------|
| `cosine` | `1 - cos(a, b)` | Text embeddings, normalized vectors |
| `euclidean` | `sqrt(sum((a_i - b_i)^2))` | Spatial data, unnormalized vectors |
| `dot_product` | `-sum(a_i * b_i)` | Pre-normalized vectors, maximum inner product |

All metrics are converted to a 0-1 similarity score:
- **Cosine**: `1 - distance/2`
- **Euclidean**: `1 / (1 + distance)`
- **Dot product**: `(1 + distance_negated) / 2`, clamped to [0, 1]

## Search Algorithms

OxiDB automatically selects the optimal algorithm based on collection size:

- **Flat (exact) search**: Used when the collection has fewer than 1,000 vectors. Computes distances to all vectors and returns exact results.
- **HNSW (approximate) search**: Used for 1,000+ vectors. Builds a Hierarchical Navigable Small World graph for fast approximate nearest neighbor search with >95% recall.

### HNSW Tuning

The HNSW index uses these defaults:

| Parameter | Default | Description |
|-----------|---------|-------------|
| M | 16 | Max connections per node per layer |
| ef_construction | 200 | Beam width during index building |
| ef_search | 50 | Beam width during search (overridable per query) |

Increase `ef_search` for higher recall at the cost of latency. Pass it in the query:

```json
{"command": "vector_search", "collection": "articles", "field": "embedding", "vector": [...], "limit": 10, "ef_search": 200}
```

## Index Lifecycle

- **Insert**: New vectors are added to the index automatically
- **Update**: Old vector is removed, new vector is inserted
- **Delete**: Vector is removed from the index
- **Compact**: Index is rebuilt from surviving documents
- **Persistence**: Saved as `.vidx` binary files and restored on startup

## Client Examples

### Python

```python
from oxidb import OxiDbClient

with OxiDbClient() as db:
    # Create a vector index
    db.create_vector_index("articles", "embedding", 384, metric="cosine")

    # Insert documents with embeddings
    db.insert("articles", {
        "title": "Introduction to Rust",
        "embedding": [0.12, -0.34, 0.56, ...]  # 384-dim vector
    })

    # Search for similar documents
    query_vector = [0.15, -0.30, 0.50, ...]  # 384-dim query
    results = db.vector_search("articles", "embedding", query_vector, limit=5)
    for doc in results:
        print(f"{doc['title']}: similarity={doc['_similarity']:.3f}")
```

### Go

```go
client, _ := oxidb.ConnectDefault()
defer client.Close()

// Create a vector index
client.CreateVectorIndex("articles", "embedding", 384, "cosine")

// Insert documents with embeddings
client.Insert("articles", map[string]any{
    "title":     "Introduction to Rust",
    "embedding": []float64{0.12, -0.34, 0.56},  // 384-dim vector
})

// Search for similar documents
results, _ := client.VectorSearch("articles", "embedding",
    []float64{0.15, -0.30, 0.50}, 5)
for _, doc := range results {
    fmt.Printf("%s: similarity=%.3f\n", doc["title"], doc["_similarity"])
}
```

### Java

```java
OxiDbClient db = new OxiDbClient("127.0.0.1", 4444, 5000);

// Create a vector index
db.createVectorIndex("articles", "embedding", 384, "cosine");

// Insert documents with embeddings
db.insert("articles", Map.of(
    "title", "Introduction to Rust",
    "embedding", new double[]{0.12, -0.34, 0.56}  // 384-dim vector
));

// Search for similar documents
JsonNode results = db.vectorSearch("articles", "embedding",
    new double[]{0.15, -0.30, 0.50}, 5);
```

### Julia

```julia
using OxiDb

client = connect_oxidb()

# Create a vector index
create_vector_index(client, "articles", "embedding", 384; metric="cosine")

# Insert documents with embeddings
insert(client, "articles", Dict(
    "title" => "Introduction to Rust",
    "embedding" => [0.12, -0.34, 0.56]  # 384-dim vector
))

# Search for similar documents
results = vector_search(client, "articles", "embedding",
    [0.15, -0.30, 0.50]; limit=5)
```

### .NET

```csharp
using var db = OxiDbClient.Connect();

// Create a vector index
db.CreateVectorIndex("articles", "embedding", 384, "cosine");

// Insert documents with embeddings
db.Insert("articles", """{"title": "Introduction to Rust", "embedding": [0.12, -0.34, 0.56]}""");

// Search for similar documents
var results = db.VectorSearch("articles", "embedding", "[0.15, -0.30, 0.50]", 5);
```

### Swift

```swift
let db = try OxiDBClient.connect()

// Create a vector index
try db.createVectorIndex(collection: "articles", field: "embedding",
                         dimension: 384, metric: "cosine")

// Insert documents with embeddings
try db.insert(collection: "articles", document: [
    "title": "Introduction to Rust",
    "embedding": [0.12, -0.34, 0.56]  // 384-dim vector
])

// Search for similar documents
let results = try db.vectorSearch(collection: "articles", field: "embedding",
                                  vector: [0.15, -0.30, 0.50], limit: 5)
```

## See Also

- [Indexes](indexes.md) -- all index types including vector
- [Protocol Reference](protocol-reference.md) -- raw command details
- [Client Libraries](client-libraries.md) -- full API tables per language
