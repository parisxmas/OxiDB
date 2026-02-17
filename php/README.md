# OxiDB PHP Client

PHP client for [OxiDB](https://github.com/parisxmas/OxiDB) document database.

Zero dependencies — uses only built-in PHP sockets and json. Communicates with `oxidb-server` over TCP using the length-prefixed JSON protocol.

## Requirements

- PHP 8.1+
- A running `oxidb-server` instance (see [main README](../README.md#installation))

## Installation

Copy the source files into your project:

```bash
cp src/OxiDbException.php src/TransactionConflictException.php src/OxiDbClient.php your_project/
```

Or use Composer autoloading with the provided `composer.json`:

```bash
composer install
```

## Quick Start

```php
require_once 'src/OxiDbException.php';
require_once 'src/TransactionConflictException.php';
require_once 'src/OxiDbClient.php';

$db = new \OxiDb\OxiDbClient('127.0.0.1', 4444);

$db->insert('users', ['name' => 'Alice', 'age' => 30]);
$docs = $db->find('users', ['name' => 'Alice']);
print_r($docs);
// [['_id' => 1, '_version' => 1, 'name' => 'Alice', 'age' => 30]]

$db->close();
```

## API Reference

### Connection

```php
$db = new \OxiDb\OxiDbClient(
    host: '127.0.0.1',  // default
    port: 4444,          // default
    timeout: 5.0         // seconds, default
);
$db->close();
```

### CRUD

| Method | Description |
|--------|-------------|
| `insert($collection, $doc)` | Insert a document, returns `['id' => ...]` |
| `insertMany($collection, $docs)` | Insert multiple documents |
| `find($collection, $query, $sort, $skip, $limit)` | Find matching documents |
| `findOne($collection, $query)` | Find first matching document or `null` |
| `update($collection, $query, $update)` | Update matching documents |
| `delete($collection, $query)` | Delete matching documents |
| `count($collection, $query)` | Count matching documents |

```php
// Insert
$db->insert('users', ['name' => 'Alice', 'age' => 30]);
$db->insertMany('users', [
    ['name' => 'Bob', 'age' => 25],
    ['name' => 'Charlie', 'age' => 35],
]);

// Find with options
$docs = $db->find('users', ['age' => ['$gte' => 18]]);
$docs = $db->find('users', [], ['age' => 1], 0, 10); // sort, skip, limit
$doc  = $db->findOne('users', ['name' => 'Alice']);

// Update
$db->update('users', ['name' => 'Alice'], ['$set' => ['age' => 31]]);

// Delete
$db->delete('users', ['name' => 'Charlie']);

// Count
$n = $db->count('users');
```

### Collections & Indexes

```php
$db->createCollection('orders');
$db->listCollections();
$db->dropCollection('orders');

$db->createIndex('users', 'name');
$db->createUniqueIndex('users', 'email');
$db->createCompositeIndex('users', ['name', 'age']);
```

### Aggregation

```php
$results = $db->aggregate('orders', [
    ['$match' => ['status' => 'completed']],
    ['$group' => ['_id' => '$category', 'total' => ['$sum' => '$amount']]],
    ['$sort'  => ['total' => -1]],
    ['$limit' => 10],
]);
```

**Supported stages:** `$match`, `$group`, `$sort`, `$skip`, `$limit`, `$project`, `$count`, `$unwind`, `$addFields`, `$lookup`

### Transactions

```php
// Auto-commit on success, auto-rollback on exception
$db->transaction(function () use ($db) {
    $db->insert('ledger', ['action' => 'debit',  'amount' => 100]);
    $db->insert('ledger', ['action' => 'credit', 'amount' => 100]);
});

// Manual control
$db->beginTx();
$db->insert('ledger', ['action' => 'refund', 'amount' => 50]);
$db->commitTx();   // or $db->rollbackTx()
```

### Blob Storage

```php
// Buckets
$db->createBucket('files');
$db->listBuckets();
$db->deleteBucket('files');

// Objects
$db->putObject('files', 'hello.txt', 'Hello!', 'text/plain', ['author' => 'php']);
[$data, $meta] = $db->getObject('files', 'hello.txt');
$head = $db->headObject('files', 'hello.txt');
$objs = $db->listObjects('files', 'hello', 10);
$db->deleteObject('files', 'hello.txt');
```

### Full-Text Search

```php
$results = $db->search('hello world', 'files', 10);
// [['bucket' => 'files', 'key' => 'doc.txt', 'score' => 2.45], ...]
```

### Compaction

```php
$stats = $db->compact('users');
// ['old_size' => 4096, 'new_size' => 2048, 'docs_kept' => 10]
```

## Error Handling

```php
use OxiDb\OxiDbException;
use OxiDb\TransactionConflictException;

try {
    $db->insert('users', ['email' => 'duplicate@test.com']);
} catch (TransactionConflictException $e) {
    echo "OCC conflict: " . $e->getMessage();
} catch (OxiDbException $e) {
    echo "Database error: " . $e->getMessage();
}
```

## Running Tests

```bash
# Start the server
./oxidb-server

# Run tests
cd php
php tests/OxiDbClientTest.php
```

## License

See [LICENSE](../LICENSE) for details.
