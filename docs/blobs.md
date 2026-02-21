# Blob Storage

OxiDB includes S3-style blob storage for binary objects. Blobs are organized into buckets and accessed by key. Each object has associated metadata, a content type, and a CRC32 etag.

## Concepts

- **Bucket**: A namespace for objects (like an S3 bucket or a directory)
- **Object**: A binary blob identified by a key within a bucket
- **Metadata**: Custom key-value pairs attached to an object
- **ETag**: CRC32 checksum of the object data

Objects are stored on disk as `_blobs/<bucket>/<id>.data` (content) and `<id>.meta` (metadata). When [encryption at rest](server.md#encryption-at-rest) is enabled, both files are encrypted with AES-256-GCM.

## Bucket Operations

### Create a Bucket

```json
{"command": "create_bucket", "bucket": "images"}
```

Buckets are also auto-created when you `put_object` into a non-existent bucket.

### List Buckets

```json
{"command": "list_buckets"}
```

### Delete a Bucket

```json
{"command": "delete_bucket", "bucket": "images"}
```

## Object Operations

### Put Object

Store a binary object. The `data` field must be base64-encoded.

```json
{
  "command": "put_object",
  "bucket": "images",
  "key": "photo.jpg",
  "data": "<base64-encoded data>",
  "content_type": "image/jpeg",
  "metadata": {"author": "Alice", "resolution": "1920x1080"}
}
```

`content_type` defaults to `"application/octet-stream"` if not specified. `metadata` is optional.

### Get Object

```json
{"command": "get_object", "bucket": "images", "key": "photo.jpg"}
```

Response:

```json
{
  "ok": true,
  "data": {
    "key": "photo.jpg",
    "bucket": "images",
    "content": "<base64-encoded data>",
    "content_type": "image/jpeg",
    "size": 204800,
    "etag": "a1b2c3d4",
    "created_at": "2025-03-15T10:30:00Z",
    "metadata": {"author": "Alice", "resolution": "1920x1080"}
  }
}
```

### Head Object

Retrieve metadata without downloading the content:

```json
{"command": "head_object", "bucket": "images", "key": "photo.jpg"}
```

Returns the same fields as `get_object` but without the `content` field.

### Delete Object

```json
{"command": "delete_object", "bucket": "images", "key": "photo.jpg"}
```

### List Objects

```json
{"command": "list_objects", "bucket": "images"}
```

With optional prefix filter and limit:

```json
{"command": "list_objects", "bucket": "images", "prefix": "2025/", "limit": 100}
```

Default limit is 1000. Results are sorted alphabetically by key.

## Full-Text Search on Blobs

OxiDB can extract text from stored blobs and index them for full-text search. The search uses TF-IDF ranking.

### Search

```json
{"command": "search", "query": "quarterly report", "limit": 10}
```

With bucket filter:

```json
{"command": "search", "query": "quarterly report", "bucket": "documents", "limit": 5}
```

Response:

```json
{
  "ok": true,
  "data": [
    {"bucket": "documents", "key": "q3-report.pdf", "score": 0.92},
    {"bucket": "documents", "key": "q2-report.pdf", "score": 0.78}
  ]
}
```

### Supported Formats

Text extraction is automatic based on content type:

| Content Type | Format | Notes |
|-------------|--------|-------|
| `text/plain`, `text/csv` | Plain text | Direct indexing |
| `text/html` | HTML | Tags stripped, text extracted |
| `text/xml`, `application/xml` | XML | Tags stripped, text extracted |
| `application/json` | JSON | All string values extracted recursively |
| `application/pdf` | PDF | Text extracted via pdf_extract |
| `application/vnd.openxmlformats-officedocument.wordprocessingml.document` | DOCX | Text from word/document.xml |
| `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet` | XLSX | Text from xl/sharedStrings.xml |
| `image/png`, `image/jpeg`, `image/tiff`, `image/bmp` | Images | OCR via Tesseract (requires `ocr` feature) |

The FTS index is persisted at `_fts/index.json`.

## Client Examples

### Python

```python
# Bucket operations
client.create_bucket("documents")
buckets = client.list_buckets()
client.delete_bucket("old-bucket")

# Put object (bytes auto-encoded to base64)
with open("report.pdf", "rb") as f:
    client.put_object("documents", "report.pdf", f.read(),
                      content_type="application/pdf",
                      metadata={"department": "finance"})

# Get object (returns bytes, auto-decoded from base64)
data, meta = client.get_object("documents", "report.pdf")
with open("downloaded.pdf", "wb") as f:
    f.write(data)

# Head object
meta = client.head_object("documents", "report.pdf")

# List objects
objects = client.list_objects("documents", prefix="2025/", limit=50)

# Delete object
client.delete_object("documents", "report.pdf")

# Full-text search
results = client.search("quarterly report", bucket="documents", limit=5)
```

### Go

```go
// Bucket operations
client.CreateBucket("documents")
buckets, _ := client.ListBuckets()
client.DeleteBucket("old-bucket")

// Put object (bytes auto-encoded to base64)
data, _ := os.ReadFile("report.pdf")
client.PutObject("documents", "report.pdf", data, "application/pdf",
    map[string]string{"department": "finance"})

// Get object (returns bytes, auto-decoded from base64)
content, meta, _ := client.GetObject("documents", "report.pdf")
os.WriteFile("downloaded.pdf", content, 0644)

// Head object
meta, _ := client.HeadObject("documents", "report.pdf")

// List objects with prefix
objects, _ := client.ListObjects("documents", strPtr("2025/"), intPtr(50))

// Full-text search
results, _ := client.Search("quarterly report", strPtr("documents"), 5)
```

### Java

```java
// Bucket operations
db.createBucket("documents");
JsonNode buckets = db.listBuckets();

// Put object
byte[] data = Files.readAllBytes(Path.of("report.pdf"));
db.putObject("documents", "report.pdf", data, "application/pdf",
    Map.of("department", "finance"));

// Get object (content is base64 in response)
JsonNode obj = db.getObject("documents", "report.pdf");
byte[] content = db.decodeObjectContent(obj);

// List objects
JsonNode objects = db.listObjects("documents", "2025/", 50);

// Full-text search
JsonNode results = db.search("quarterly report", "documents", 5);
```

### Julia

```julia
# Bucket operations
create_bucket(client, "documents")
buckets = list_buckets(client)

# Put object (bytes auto-encoded)
data = read("report.pdf")
put_object(client, "documents", "report.pdf", data;
           content_type="application/pdf",
           metadata=Dict("department" => "finance"))

# Get object (returns bytes, metadata)
content, meta = get_object(client, "documents", "report.pdf")
write("downloaded.pdf", content)

# List objects
objects = list_objects(client, "documents"; prefix="2025/", limit=50)

# Full-text search
results = search(client, "quarterly report"; bucket="documents", limit=5)
```

### .NET

```csharp
// Bucket operations
db.CreateBucket("documents");
var buckets = db.ListBuckets();

// Put object (data as base64 string)
var data = Convert.ToBase64String(File.ReadAllBytes("report.pdf"));
db.PutObject("documents", "report.pdf", data, "application/pdf",
    """{"department": "finance"}""");

// Get object
var obj = db.GetObject("documents", "report.pdf");

// List objects
var objects = db.ListObjects("documents", "2025/", 50);

// Full-text search
var results = db.Search("quarterly report", "documents", 5);
```

### Swift

```swift
// Bucket operations
try db.createBucket("documents")
let buckets = try db.listBuckets()

// Put object (data as base64 string)
let fileData = try Data(contentsOf: URL(fileURLWithPath: "report.pdf"))
let base64 = fileData.base64EncodedString()
try db.putObject(bucket: "documents", key: "report.pdf",
                 dataBase64: base64, contentType: "application/pdf",
                 metadata: ["department": "finance"])

// Get object
let obj = try db.getObject(bucket: "documents", key: "report.pdf")

// List objects
let objects = try db.listObjects(bucket: "documents", prefix: "2025/", limit: 50)

// Full-text search
let results = try db.search(query: "quarterly report", bucket: "documents", limit: 5)
```

## See Also

- [Server Configuration](server.md#encryption-at-rest) -- encrypting blob data at rest
- [Indexes](indexes.md) -- text indexes for collection-level full-text search
