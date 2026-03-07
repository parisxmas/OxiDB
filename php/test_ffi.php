<?php
/**
 * OxiDB FFI Client Test & Benchmark
 *
 * Compares pure PHP OxiWire client vs FFI native client.
 * Requires: OxiDB server running on 127.0.0.1:4444
 */

error_reporting(E_ALL);
ini_set('display_errors', 1);

require_once __DIR__ . '/OxiDb.php';
require_once __DIR__ . '/OxiDbFFI.php';

// Point to the built library
OxiDbFFIClient::setLibraryPath(__DIR__ . '/../target/release/liboxidb_client_ffi.dylib');

$passed = 0;
$failed = 0;

function test(string $name, callable $fn): void
{
    global $passed, $failed;
    try {
        $fn();
        echo "  PASS  $name\n";
        $passed++;
    } catch (\Throwable $e) {
        echo "  FAIL  $name — {$e->getMessage()}\n";
        $failed++;
    }
}

function assert_eq($expected, $actual, string $msg = ''): void
{
    if ($expected !== $actual) {
        throw new \RuntimeException(
            ($msg ? "$msg: " : '') . "expected " . var_export($expected, true) .
            ", got " . var_export($actual, true)
        );
    }
}

// ── 1. Functional Tests ─────────────────────────────────────────

echo "=== OxiDB FFI Client — Functional Tests ===\n\n";

$ffi = new OxiDbFFIClient('127.0.0.1', 4444);

test('ping', function () use ($ffi) {
    assert_eq('pong', $ffi->ping());
});

// Clean up test collection
try { $ffi->dropCollection('ffi_test'); } catch (OxiDbException $_) {}

test('create collection', function () use ($ffi) {
    $ffi->createCollection('ffi_test');
    $cols = $ffi->listCollections();
    assert_eq(true, in_array('ffi_test', $cols));
});

test('insert + find', function () use ($ffi) {
    $result = $ffi->insert('ffi_test', ['name' => 'Alice', 'age' => 30]);
    $id = $result['id'] ?? null;
    assert_eq(true, $id !== null, 'insert should return id');

    $docs = $ffi->find('ffi_test', ['name' => 'Alice']);
    assert_eq(1, count($docs));
    assert_eq('Alice', $docs[0]['name']);
    assert_eq(30, $docs[0]['age']);
});

test('insert many', function () use ($ffi) {
    $docs = [];
    for ($i = 0; $i < 10; $i++) {
        $docs[] = ['name' => "User$i", 'score' => $i * 10];
    }
    $ids = $ffi->insertMany('ffi_test', $docs);
    assert_eq(10, count($ids));
});

test('find with options (sort, limit, skip)', function () use ($ffi) {
    $docs = $ffi->find('ffi_test', [], ['sort' => ['score' => -1], 'limit' => 3, 'skip' => 1]);
    assert_eq(3, count($docs));
});

test('find one', function () use ($ffi) {
    $doc = $ffi->findOne('ffi_test', ['name' => 'User5']);
    assert_eq('User5', $doc['name']);
    assert_eq(50, $doc['score']);
});

test('update', function () use ($ffi) {
    $result = $ffi->update('ffi_test', ['name' => 'Alice'], ['$set' => ['age' => 31]]);
    assert_eq(1, $result['modified']);
    $doc = $ffi->findOne('ffi_test', ['name' => 'Alice']);
    assert_eq(31, $doc['age']);
});

test('update one', function () use ($ffi) {
    $result = $ffi->updateOne('ffi_test', ['name' => 'User0'], ['$set' => ['score' => 999]]);
    assert_eq(1, $result['modified']);
});

test('count', function () use ($ffi) {
    $n = $ffi->count('ffi_test');
    assert_eq(11, $n); // 1 Alice + 10 User*
});

test('delete one', function () use ($ffi) {
    $result = $ffi->deleteOne('ffi_test', ['name' => 'User9']);
    assert_eq(1, $result['deleted']);
});

test('delete', function () use ($ffi) {
    $result = $ffi->delete('ffi_test', ['name' => ['$regex' => '^User']]);
    assert_eq(true, $result['deleted'] >= 1);
});

test('create index', function () use ($ffi) {
    $ffi->createIndex('ffi_test', 'name');
});

test('create unique index', function () use ($ffi) {
    $ffi->createUniqueIndex('ffi_test', 'email');
});

test('list indexes', function () use ($ffi) {
    $indexes = $ffi->listIndexes('ffi_test');
    assert_eq(true, count($indexes) >= 2);
});

test('aggregate', function () use ($ffi) {
    $ffi->insert('ffi_test', ['type' => 'A', 'val' => 10]);
    $ffi->insert('ffi_test', ['type' => 'B', 'val' => 20]);
    $ffi->insert('ffi_test', ['type' => 'A', 'val' => 30]);

    $result = $ffi->aggregate('ffi_test', [
        ['$match' => ['type' => ['$exists' => true]]],
        ['$group' => ['_id' => '$type', 'total' => ['$sum' => '$val']]],
        ['$sort' => ['_id' => 1]],
    ]);
    assert_eq(2, count($result));
    assert_eq('A', $result[0]['_id']);
    assert_eq(40, $result[0]['total']);
});

test('SQL query', function () use ($ffi) {
    $result = $ffi->sql("SELECT * FROM ffi_test WHERE name = 'Alice'");
    assert_eq(true, is_array($result));
    assert_eq(true, count($result) >= 1);
});

test('transaction commit', function () use ($ffi) {
    $ffi->beginTransaction();
    $ffi->insert('ffi_test', ['tx_test' => true, 'status' => 'committed']);
    $ffi->commit();
    $doc = $ffi->findOne('ffi_test', ['tx_test' => true]);
    assert_eq('committed', $doc['status']);
});

test('transaction rollback', function () use ($ffi) {
    $ffi->beginTransaction();
    $ffi->insert('ffi_test', ['rollback_test' => true]);
    $ffi->rollback();
    $doc = $ffi->findOne('ffi_test', ['rollback_test' => true]);
    assert_eq(null, $doc);
});

test('transaction helper', function () use ($ffi) {
    $ffi->transaction(function ($db) {
        $db->insert('ffi_test', ['helper_test' => true]);
    });
    $doc = $ffi->findOne('ffi_test', ['helper_test' => true]);
    assert_eq(true, $doc['helper_test']);
});

test('compact', function () use ($ffi) {
    $result = $ffi->compact('ffi_test');
    assert_eq(true, isset($result['docs_kept']));
});

test('use database', function () use ($ffi) {
    $ffi->useDatabase('oxidb');
});

test('drop collection', function () use ($ffi) {
    $ffi->dropCollection('ffi_test');
    $cols = $ffi->listCollections();
    assert_eq(false, in_array('ffi_test', $cols));
});

echo "\n";

// ── 2. Benchmark: FFI vs Pure PHP ───────────────────────────────

echo "=== Benchmark: FFI vs Pure PHP OxiWire ===\n\n";

function benchmark(string $label, callable $fn, int $iterations = 1000): float
{
    // Warmup
    for ($i = 0; $i < 10; $i++) $fn($i);

    $start = hrtime(true);
    for ($i = 0; $i < $iterations; $i++) {
        $fn($i);
    }
    $elapsed = (hrtime(true) - $start) / 1e6; // ms
    $perOp = $elapsed / $iterations;
    printf("  %-40s %8.1f ms total  |  %.3f ms/op  |  %d ops/sec\n",
        $label, $elapsed, $perOp, (int)(1000 / $perOp));
    return $elapsed;
}

// Create test collections
$php = new OxiDbClient('127.0.0.1', 4444);
try { $php->dropCollection('bench_php'); } catch (OxiDbException $_) {}
try { $ffi->dropCollection('bench_ffi'); } catch (OxiDbException $_) {}
$php->createCollection('bench_php');
$ffi->createCollection('bench_ffi');

$N = 1000;

echo "--- Ping ($N iterations) ---\n";
$t_php = benchmark('Pure PHP (OxiWire)', fn($i) => $php->ping(), $N);
$t_ffi = benchmark('FFI (Rust native)',  fn($i) => $ffi->ping(), $N);
printf("  Speedup: %.1fx\n\n", $t_php / $t_ffi);

echo "--- Insert ($N iterations) ---\n";
$t_php = benchmark('Pure PHP (OxiWire)', fn($i) => $php->insert('bench_php', ['i' => $i, 'data' => str_repeat('x', 100)]), $N);
$t_ffi = benchmark('FFI (Rust native)',  fn($i) => $ffi->insert('bench_ffi', ['i' => $i, 'data' => str_repeat('x', 100)]), $N);
printf("  Speedup: %.1fx\n\n", $t_php / $t_ffi);

// Create indexes for find benchmark
$php->createIndex('bench_php', 'i');
$ffi->createIndex('bench_ffi', 'i');

echo "--- Find by index ($N iterations) ---\n";
$t_php = benchmark('Pure PHP (OxiWire)', fn($i) => $php->find('bench_php', ['i' => $i % $N]), $N);
$t_ffi = benchmark('FFI (Rust native)',  fn($i) => $ffi->find('bench_ffi', ['i' => $i % $N]), $N);
printf("  Speedup: %.1fx\n\n", $t_php / $t_ffi);

echo "--- Find One ($N iterations) ---\n";
$t_php = benchmark('Pure PHP (OxiWire)', fn($i) => $php->findOne('bench_php', ['i' => $i % $N]), $N);
$t_ffi = benchmark('FFI (Rust native)',  fn($i) => $ffi->findOne('bench_ffi', ['i' => $i % $N]), $N);
printf("  Speedup: %.1fx\n\n", $t_php / $t_ffi);

echo "--- Count ($N iterations) ---\n";
$t_php = benchmark('Pure PHP (OxiWire)', fn($i) => $php->count('bench_php'), $N);
$t_ffi = benchmark('FFI (Rust native)',  fn($i) => $ffi->count('bench_ffi'), $N);
printf("  Speedup: %.1fx\n\n", $t_php / $t_ffi);

echo "--- SQL SELECT ($N iterations) ---\n";
$t_php = benchmark('Pure PHP (OxiWire)', fn($i) => $php->sql("SELECT * FROM bench_php WHERE i = " . ($i % $N)), $N);
$t_ffi = benchmark('FFI (Rust native)',  fn($i) => $ffi->sql("SELECT * FROM bench_ffi WHERE i = " . ($i % $N)), $N);
printf("  Speedup: %.1fx\n\n", $t_php / $t_ffi);

// Bulk operations
echo "--- Insert Many (100 docs x 100 batches) ---\n";
$batch = [];
for ($i = 0; $i < 100; $i++) {
    $batch[] = ['bulk' => true, 'idx' => $i, 'payload' => str_repeat('y', 200)];
}
$t_php = benchmark('Pure PHP (OxiWire)', fn($i) => $php->insertMany('bench_php', $batch), 100);
$t_ffi = benchmark('FFI (Rust native)',  fn($i) => $ffi->insertMany('bench_ffi', $batch), 100);
printf("  Speedup: %.1fx\n\n", $t_php / $t_ffi);

// Cleanup
$php->dropCollection('bench_php');
$ffi->dropCollection('bench_ffi');
$php->close();
$ffi->close();

echo "=== Results ===\n";
echo "Functional: $passed passed, $failed failed\n";
