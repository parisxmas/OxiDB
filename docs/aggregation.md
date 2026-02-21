# Aggregation Pipeline

OxiDB supports an aggregation pipeline for data analytics. A pipeline is an array of stages that process documents sequentially -- each stage transforms the output of the previous stage.

## Running a Pipeline

```json
{
  "command": "aggregate",
  "collection": "orders",
  "pipeline": [
    {"$match": {"status": "completed"}},
    {"$group": {"_id": "$customer_id", "total": {"$sum": "$amount"}}},
    {"$sort": {"total": -1}},
    {"$limit": 10}
  ]
}
```

## Pipeline Stages

### $match

Filters documents using the same [query syntax](queries.md) as `find`. Place `$match` early in the pipeline to reduce the number of documents processed by later stages.

```json
{"$match": {"status": "active", "age": {"$gte": 18}}}
```

When `$match` is the first stage, it can use [indexes](indexes.md).

### $group

Groups documents by a key and applies accumulator expressions.

**Group by a single field:**

```json
{"$group": {"_id": "$category", "count": {"$count": {}}, "avg_price": {"$avg": "$price"}}}
```

**Group by multiple fields (compound key):**

```json
{"$group": {"_id": {"year": "$year", "month": "$month"}, "revenue": {"$sum": "$amount"}}}
```

**Group all documents (single group):**

```json
{"$group": {"_id": null, "total": {"$sum": "$amount"}}}
```

#### Accumulators

| Accumulator | Description | Example |
|-------------|-------------|---------|
| `$sum` | Sum of values (or count with `1`) | `{"$sum": "$amount"}` or `{"$sum": 1}` |
| `$avg` | Average of values | `{"$avg": "$score"}` |
| `$min` | Minimum value | `{"$min": "$price"}` |
| `$max` | Maximum value | `{"$max": "$price"}` |
| `$count` | Count of documents | `{"$count": {}}` |
| `$first` | First value in group | `{"$first": "$name"}` |
| `$last` | Last value in group | `{"$last": "$name"}` |
| `$push` | Collect all values into array | `{"$push": "$tag"}` |

### $sort

Sorts documents by one or more fields. Use `1` for ascending, `-1` for descending.

```json
{"$sort": {"total": -1, "name": 1}}
```

Sorting is type-aware, following the [value ordering](indexes.md#value-ordering): Null < Bool < Number < DateTime < String.

### $skip

Skips the first N documents.

```json
{"$skip": 20}
```

### $limit

Limits output to N documents.

```json
{"$limit": 10}
```

### $project

Includes, excludes, or computes fields. Use `1` to include, `0` to exclude, or an expression to compute.

```json
{
  "$project": {
    "name": 1,
    "total": {"$multiply": ["$price", "$quantity"]},
    "internal_notes": 0
  }
}
```

### $count

Replaces the input with a single document containing the count.

```json
{"$count": "total_orders"}
```

Output: `{"total_orders": 42}`

### $unwind

Deconstructs an array field, creating one document per array element.

```json
{"$unwind": "$tags"}
```

With options:

```json
{"$unwind": {"path": "$tags", "preserveNullAndEmptyArrays": true}}
```

When `preserveNullAndEmptyArrays` is `true`, documents where the field is missing, null, or an empty array are preserved (with the field removed or set to null).

### $addFields

Adds new fields or overwrites existing ones using expressions.

```json
{"$addFields": {"total": {"$multiply": ["$price", "$quantity"]}}}
```

### $lookup

Performs a left join with another collection.

```json
{
  "$lookup": {
    "from": "customers",
    "localField": "customer_id",
    "foreignField": "_id",
    "as": "customer_info"
  }
}
```

This adds a `customer_info` array field containing all matching documents from the `customers` collection.

## Expressions

Expressions can be used in `$project`, `$addFields`, and accumulator values.

### Field References

Prefix a field name with `$` to reference its value:

```json
"$price"
"$address.city"
```

### Arithmetic

| Expression | Description | Example |
|------------|-------------|---------|
| `$add` | Add values | `{"$add": ["$price", "$tax"]}` |
| `$subtract` | Subtract second from first | `{"$subtract": ["$total", "$discount"]}` |
| `$multiply` | Multiply values | `{"$multiply": ["$price", "$quantity"]}` |
| `$divide` | Divide first by second | `{"$divide": ["$total", "$count"]}` |

Division by zero returns `null`.

### Literals

Numbers, strings, booleans, and null can be used directly as expression values.

## Practical Examples

### Revenue by Category

```json
[
  {"$match": {"status": "completed"}},
  {"$group": {"_id": "$category", "revenue": {"$sum": "$amount"}, "orders": {"$sum": 1}}},
  {"$sort": {"revenue": -1}}
]
```

### Top 5 Customers

```json
[
  {"$group": {"_id": "$customer_id", "total_spent": {"$sum": "$amount"}}},
  {"$sort": {"total_spent": -1}},
  {"$limit": 5}
]
```

### Tag Frequency Analysis

```json
[
  {"$unwind": "$tags"},
  {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
  {"$sort": {"count": -1}},
  {"$limit": 20}
]
```

### Join Orders with Customers

```json
[
  {"$lookup": {"from": "customers", "localField": "customer_id", "foreignField": "_id", "as": "customer"}},
  {"$unwind": "$customer"},
  {"$project": {"order_id": 1, "amount": 1, "customer_name": "$customer.name"}}
]
```

## Client Examples

### Python

```python
pipeline = [
    {"$match": {"status": "completed"}},
    {"$group": {"_id": "$category", "total": {"$sum": "$amount"}}},
    {"$sort": {"total": -1}},
    {"$limit": 10}
]
results = client.aggregate("orders", pipeline)
```

### Go

```go
pipeline := []map[string]any{
    {"$match": map[string]any{"status": "completed"}},
    {"$group": map[string]any{"_id": "$category", "total": map[string]any{"$sum": "$amount"}}},
    {"$sort": map[string]any{"total": -1}},
    {"$limit": 10},
}
results, _ := client.Aggregate("orders", pipeline)
```

### Java

```java
List<Map<String, Object>> pipeline = List.of(
    Map.of("$match", Map.of("status", "completed")),
    Map.of("$group", Map.of("_id", "$category", "total", Map.of("$sum", "$amount"))),
    Map.of("$sort", Map.of("total", -1)),
    Map.of("$limit", 10)
);
JsonNode results = db.aggregate("orders", pipeline);
```

### Julia

```julia
pipeline = [
    Dict("\$match" => Dict("status" => "completed")),
    Dict("\$group" => Dict("_id" => "\$category", "total" => Dict("\$sum" => "\$amount"))),
    Dict("\$sort" => Dict("total" => -1)),
    Dict("\$limit" => 10)
]
results = aggregate(client, "orders", pipeline)
```

### .NET

```csharp
var results = db.Aggregate("orders", """[
    {"$match": {"status": "completed"}},
    {"$group": {"_id": "$category", "total": {"$sum": "$amount"}}},
    {"$sort": {"total": -1}},
    {"$limit": 10}
]""");
```

### Swift

```swift
let pipeline: [[String: Any]] = [
    ["$match": ["status": "completed"]],
    ["$group": ["_id": "$category", "total": ["$sum": "$amount"]]],
    ["$sort": ["total": -1]],
    ["$limit": 10]
]
let results = try db.aggregate(collection: "orders", pipeline: pipeline)
```

## See Also

- [Querying Documents](queries.md) -- query syntax used in `$match`
- [SQL](sql.md) -- SQL aggregate functions (`COUNT`, `SUM`, `AVG`, etc.) map to this pipeline
- [Indexes](indexes.md) -- indexes used by `$match` when it is the first stage
