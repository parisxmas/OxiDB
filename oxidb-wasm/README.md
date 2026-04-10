# OxiDB WebAssembly

Run OxiDB document database entirely in the browser. No server needed — all data lives in memory within the WASM module.

## Quick Start

### Option 1: Use Pre-built Release

Download from GitHub releases:

```html
<script type="module">
  import init, * as oxidb from 'https://github.com/parisxmas/oxidb/releases/latest/download/oxidb-wasm/oxidb_wasm.js';

  await init();
  oxidb.init();

  // Insert a document
  oxidb.insert('users', JSON.stringify({ name: 'Alice', age: 30 }));

  // Query
  const users = JSON.parse(oxidb.find('users', '{}'));
  console.log(users);
</script>
```

### Option 2: Self-host

1. Download and extract `oxidb-wasm.tar.gz` from [releases](https://github.com/parisxmas/oxidb/releases)
2. Place the files in your project (e.g. `/wasm/`)
3. Import from local path:

```
your-project/
  wasm/
    oxidb_wasm.js
    oxidb_wasm.d.ts
    oxidb_wasm_bg.wasm
    oxidb_wasm_bg.wasm.d.ts
  index.html
```

```html
<script type="module">
  import init, * as oxidb from './wasm/oxidb_wasm.js';

  await init();
  oxidb.init();
  // ready to use
</script>
```

### Option 3: Build from Source

```bash
# Prerequisites: Rust, wasm-pack
cargo install wasm-pack

# Build
cd oxidb-wasm
./build.sh

# Output in oxidb-wasm/pkg/
```

## API Reference

All functions work with JSON strings. Call `init()` before anything else.

### Initialize

```js
import init, * as oxidb from './oxidb_wasm.js';

await init();   // load WASM binary
oxidb.init();   // create in-memory database
```

### Insert

```js
// Single document — returns document ID (string)
const id = oxidb.insert('users', JSON.stringify({ name: 'Alice', age: 30 }));

// Multiple documents — returns JSON array of IDs
const ids = JSON.parse(oxidb.insert_many('users', JSON.stringify([
  { name: 'Bob', age: 25 },
  { name: 'Charlie', age: 35 }
])));
```

### Find

```js
// All documents
const all = JSON.parse(oxidb.find('users', '{}'));

// With filter
const result = JSON.parse(oxidb.find('users', JSON.stringify({ city: 'Berlin' })));

// Operators: $gt, $gte, $lt, $lte, $ne, $in, $exists, $regex
const older = JSON.parse(oxidb.find('users', JSON.stringify({ age: { $gt: 25 } })));

// Single document
const alice = JSON.parse(oxidb.find_one('users', JSON.stringify({ name: 'Alice' })));
```

### Update

```js
// Returns number of modified documents
const count = oxidb.update(
  'users',
  JSON.stringify({ name: 'Alice' }),                // filter
  JSON.stringify({ $set: { age: 31, city: 'Paris' } }) // update
);

// Operators: $set, $unset, $inc, $mul, $min, $max, $push, $pull, $addToSet
oxidb.update(
  'users',
  JSON.stringify({ name: 'Bob' }),
  JSON.stringify({ $inc: { age: 1 } })
);
```

### Delete

```js
// Returns number of deleted documents
const count = oxidb.delete('users', JSON.stringify({ age: { $lt: 20 } }));
```

### Count

```js
const total = oxidb.count('users', '{}');
const berliners = oxidb.count('users', JSON.stringify({ city: 'Berlin' }));
```

### SQL

```js
const result = JSON.parse(oxidb.sql('SELECT name, age FROM users WHERE age > 25 ORDER BY age'));
// result.rows = [{ name: "Alice", age: 30 }, ...]

oxidb.sql("INSERT INTO products (name, price) VALUES ('Laptop', 999)");
oxidb.sql("UPDATE users SET age = age + 1 WHERE city = 'Berlin'");
oxidb.sql("DELETE FROM users WHERE age < 20");
oxidb.sql("SELECT city, COUNT(*) as n FROM users GROUP BY city");
```

### Indexes

```js
oxidb.create_index('users', 'age');    // speeds up queries on age
oxidb.create_index('users', 'city');
```

### Aggregation

```js
const pipeline = JSON.stringify([
  { $match: { age: { $gte: 20 } } },
  { $group: { _id: '$city', count: { $sum: 1 }, avg_age: { $avg: '$age' } } },
  { $sort: { count: -1 } }
]);
const stats = JSON.parse(oxidb.aggregate('users', pipeline));
```

### Collections

```js
const names = JSON.parse(oxidb.list_collections());  // ["users", "products"]
oxidb.drop_collection('products');
```

## Complete Example

```html
<!DOCTYPE html>
<html>
<head><title>OxiDB WASM</title></head>
<body>
  <pre id="out"></pre>
  <script type="module">
    import init, * as oxidb from './wasm/oxidb_wasm.js';

    const log = msg => document.getElementById('out').textContent += msg + '\n';

    await init();
    oxidb.init();
    log('Database ready');

    // Create collection with data
    oxidb.insert_many('tasks', JSON.stringify([
      { title: 'Buy groceries', done: false, priority: 'high' },
      { title: 'Write docs',    done: true,  priority: 'medium' },
      { title: 'Fix bug #42',   done: false, priority: 'high' },
      { title: 'Clean desk',    done: false, priority: 'low' }
    ]));

    // Index for fast queries
    oxidb.create_index('tasks', 'priority');
    oxidb.create_index('tasks', 'done');

    // JSON query
    const urgent = JSON.parse(oxidb.find('tasks', JSON.stringify({
      priority: 'high', done: false
    })));
    log('Urgent tasks: ' + urgent.length);

    // SQL query
    const sql_result = JSON.parse(oxidb.sql(
      "SELECT priority, COUNT(*) as n FROM tasks WHERE done = false GROUP BY priority"
    ));
    log('Pending by priority: ' + JSON.stringify(sql_result.rows));

    // Update
    oxidb.update('tasks',
      JSON.stringify({ title: 'Fix bug #42' }),
      JSON.stringify({ $set: { done: true } })
    );

    // Aggregation
    const stats = JSON.parse(oxidb.aggregate('tasks', JSON.stringify([
      { $group: { _id: '$done', count: { $sum: 1 } } }
    ])));
    log('Stats: ' + JSON.stringify(stats));
  </script>
</body>
</html>
```

## Notes

- All data is **in-memory only** — it does not persist across page reloads
- The WASM binary is ~1.5 MB gzipped (~4.7 MB uncompressed)
- All queries run synchronously on the main thread
- Supports the same query operators, SQL dialect, and aggregation pipeline as native OxiDB
- TypeScript types included (`oxidb_wasm.d.ts`)
