# SQL Query Language

OxiDB supports a SQL interface that is translated internally to MongoDB-style queries and aggregation pipelines. This lets you work with familiar SQL syntax while using OxiDB's document storage.

## Running SQL Queries

```json
{"command": "sql", "query": "SELECT * FROM users WHERE age >= 18 ORDER BY name LIMIT 10"}
```

## SELECT

### Basic SELECT

```sql
SELECT * FROM users
SELECT name, email FROM users
SELECT name AS user_name, age FROM users
```

### WHERE Clause

```sql
SELECT * FROM users WHERE age >= 18
SELECT * FROM users WHERE status = 'active' AND role = 'admin'
SELECT * FROM users WHERE role = 'admin' OR role = 'moderator'
```

### ORDER BY

```sql
SELECT * FROM users ORDER BY name ASC
SELECT * FROM users ORDER BY created_at DESC, name ASC
```

### LIMIT and OFFSET

```sql
SELECT * FROM users ORDER BY name LIMIT 10 OFFSET 20
```

## WHERE Operators

| SQL Operator | Maps To | Example |
|-------------|---------|---------|
| `=` | `$eq` | `WHERE name = 'Alice'` |
| `!=` or `<>` | `$ne` | `WHERE status != 'inactive'` |
| `>` | `$gt` | `WHERE age > 18` |
| `>=` | `$gte` | `WHERE score >= 90` |
| `<` | `$lt` | `WHERE price < 100` |
| `<=` | `$lte` | `WHERE quantity <= 0` |
| `IN` | `$in` | `WHERE status IN ('active', 'pending')` |
| `BETWEEN` | `$gte` + `$lte` | `WHERE age BETWEEN 18 AND 65` |
| `LIKE` | `$regex` | `WHERE name LIKE 'Al%'` |
| `IS NULL` | `$exists: false` | `WHERE email IS NULL` |
| `IS NOT NULL` | `$exists: true` | `WHERE email IS NOT NULL` |
| `AND` | `$and` | `WHERE a = 1 AND b = 2` |
| `OR` | `$or` | `WHERE a = 1 OR b = 2` |

### LIKE Patterns

SQL `LIKE` patterns are converted to regular expressions:

- `%` matches any sequence of characters
- `_` matches any single character

```sql
SELECT * FROM users WHERE name LIKE 'Al%'     -- starts with "Al"
SELECT * FROM users WHERE code LIKE 'A_B'     -- "A" + any char + "B"
```

## Aggregate Functions

```sql
SELECT COUNT(*) FROM orders
SELECT COUNT(*) AS total, AVG(amount) AS avg_amount FROM orders WHERE status = 'completed'
SELECT category, SUM(amount) AS revenue FROM orders GROUP BY category
SELECT category, COUNT(*) AS cnt FROM orders GROUP BY category HAVING cnt > 10
```

| Function | Maps To | Description |
|----------|---------|-------------|
| `COUNT(*)` | `$sum: 1` | Count documents |
| `COUNT(field)` | `$sum: 1` | Count documents (same as `COUNT(*)`) |
| `SUM(field)` | `$sum: "$field"` | Sum of field values |
| `AVG(field)` | `$avg: "$field"` | Average of field values |
| `MIN(field)` | `$min: "$field"` | Minimum value |
| `MAX(field)` | `$max: "$field"` | Maximum value |

### GROUP BY

```sql
SELECT category, COUNT(*) AS count, AVG(price) AS avg_price
FROM products
GROUP BY category
ORDER BY count DESC
```

### HAVING

Filter groups after aggregation:

```sql
SELECT category, SUM(amount) AS total
FROM orders
GROUP BY category
HAVING total > 1000
```

## JOINs

JOINs are converted to `$lookup` + `$unwind` pipeline stages.

### INNER JOIN

```sql
SELECT o.*, c.name AS customer_name
FROM orders o
INNER JOIN customers c ON o.customer_id = c._id
```

### LEFT JOIN

```sql
SELECT u.name, p.title
FROM users u
LEFT JOIN posts p ON u._id = p.author_id
```

### RIGHT JOIN

```sql
SELECT o.order_id, c.name
FROM orders o
RIGHT JOIN customers c ON o.customer_id = c._id
```

### FULL OUTER JOIN

```sql
SELECT *
FROM table_a a
FULL OUTER JOIN table_b b ON a.key = b.key
```

## INSERT

### Single Row

```sql
INSERT INTO users (name, age, email) VALUES ('Alice', 30, 'alice@example.com')
```

### Multiple Rows

```sql
INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', 25), ('Charlie', 35)
```

## UPDATE

```sql
UPDATE users SET status = 'inactive' WHERE last_login < '2024-01-01'
UPDATE products SET price = price * 1.1 WHERE category = 'electronics'
```

## DELETE

```sql
DELETE FROM users WHERE status = 'inactive'
DELETE FROM sessions WHERE expires_at < '2025-01-01'
```

## DDL Statements

### CREATE TABLE

```sql
CREATE TABLE users
```

Creates an empty collection. (Collections are also auto-created on first insert.)

### DROP TABLE

```sql
DROP TABLE temp_data
```

### CREATE INDEX

```sql
CREATE INDEX ON users (email)
CREATE INDEX ON orders (customer_id, status)
```

Single-field and composite indexes are supported.

### SHOW TABLES

```sql
SHOW TABLES
```

Lists all collections.

## Client Examples

### Python

```python
# SELECT
users = client.sql("SELECT * FROM users WHERE age >= 18 ORDER BY name LIMIT 10")

# Aggregate
stats = client.sql("SELECT category, COUNT(*) as cnt, AVG(price) as avg FROM products GROUP BY category")

# JOIN
result = client.sql("""
    SELECT o.*, c.name AS customer_name
    FROM orders o
    INNER JOIN customers c ON o.customer_id = c._id
""")

# INSERT
client.sql("INSERT INTO users (name, age) VALUES ('Alice', 30)")

# UPDATE
client.sql("UPDATE users SET status = 'inactive' WHERE last_login < '2024-01-01'")

# DELETE
client.sql("DELETE FROM sessions WHERE expires_at < '2025-01-01'")

# DDL
client.sql("CREATE INDEX ON users (email)")
client.sql("SHOW TABLES")
```

### Go

```go
users, _ := client.SQL("SELECT * FROM users WHERE age >= 18 ORDER BY name LIMIT 10")
stats, _ := client.SQL("SELECT category, COUNT(*) as cnt FROM products GROUP BY category")
client.SQL("INSERT INTO users (name, age) VALUES ('Alice', 30)")
client.SQL("UPDATE users SET status = 'inactive' WHERE last_login < '2024-01-01'")
```

### Java

```java
JsonNode users = db.sql("SELECT * FROM users WHERE age >= 18 ORDER BY name LIMIT 10");
db.sql("INSERT INTO users (name, age) VALUES ('Alice', 30)");
db.sql("UPDATE users SET status = 'inactive' WHERE last_login < '2024-01-01'");
db.sql("CREATE INDEX ON users (email)");
```

### Julia

```julia
users = sql(client, "SELECT * FROM users WHERE age >= 18 ORDER BY name LIMIT 10")
sql(client, "INSERT INTO users (name, age) VALUES ('Alice', 30)")
sql(client, "CREATE INDEX ON users (email)")
```

### .NET

```csharp
var users = db.Sql("SELECT * FROM users WHERE age >= 18 ORDER BY name LIMIT 10");
db.Sql("INSERT INTO users (name, age) VALUES ('Alice', 30)");
db.Sql("CREATE INDEX ON users (email)");
```

### Swift

```swift
let users = try db.sql(query: "SELECT * FROM users WHERE age >= 18 ORDER BY name LIMIT 10")
try db.sql(query: "INSERT INTO users (name, age) VALUES ('Alice', 30)")
try db.sql(query: "CREATE INDEX ON users (email)")
```

## See Also

- [Querying Documents](queries.md) -- MongoDB-style query syntax
- [Aggregation](aggregation.md) -- pipeline stages that SQL maps to
- [Indexes](indexes.md) -- indexes improve SQL query performance
