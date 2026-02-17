<?php

declare(strict_types=1);

require_once __DIR__ . '/../src/OxiDbException.php';
require_once __DIR__ . '/../src/TransactionConflictException.php';
require_once __DIR__ . '/../src/OxiDbClient.php';

use OxiDb\OxiDbClient;

/**
 * Integration tests for OxiDbClient.
 * Run with: php tests/OxiDbClientTest.php
 * Requires oxidb-server running on 127.0.0.1:4444.
 */

$host = getenv('OXIDB_HOST') ?: '127.0.0.1';
$port = (int)(getenv('OXIDB_PORT') ?: 4444);

$passed = 0;
$failed = 0;

function test(string $name, callable $fn): void
{
    global $passed, $failed;
    try {
        $fn();
        echo "  PASS  {$name}\n";
        $passed++;
    } catch (\Throwable $e) {
        echo "  FAIL  {$name}: {$e->getMessage()}\n";
        $failed++;
    }
}

function assert_eq($expected, $actual, string $msg = ''): void
{
    if ($expected !== $actual) {
        throw new \RuntimeException(
            $msg ?: "Expected " . var_export($expected, true) . ", got " . var_export($actual, true)
        );
    }
}

function assert_true($value, string $msg = ''): void
{
    if (!$value) {
        throw new \RuntimeException($msg ?: "Expected truthy value");
    }
}

echo "OxiDB PHP Client Tests\n";
echo "======================\n\n";

$db = new OxiDbClient($host, $port);

test('ping', function () use ($db) {
    assert_eq('pong', $db->ping());
});

test('create_collection', function () use ($db) {
    $db->createCollection('php_test');
    $cols = $db->listCollections();
    assert_true(in_array('php_test', $cols), 'php_test not in collections');
});

test('insert & find', function () use ($db) {
    $result = $db->insert('php_test', ['name' => 'Alice', 'age' => 30]);
    assert_true(isset($result['id']), 'insert did not return id');

    $docs = $db->find('php_test', ['name' => 'Alice']);
    assert_true(count($docs) >= 1, 'find returned no docs');
    assert_eq('Alice', $docs[0]['name']);
    assert_eq(30, $docs[0]['age']);
});

test('insert_many', function () use ($db) {
    $result = $db->insertMany('php_test', [
        ['name' => 'Bob', 'age' => 25],
        ['name' => 'Charlie', 'age' => 35],
    ]);
    assert_true(is_array($result) && count($result) === 2);
});

test('find with options', function () use ($db) {
    $docs = $db->find('php_test', [], ['age' => 1]);
    assert_true(count($docs) >= 3);

    $docs = $db->find('php_test', [], null, null, 1);
    assert_eq(1, count($docs));
});

test('find_one', function () use ($db) {
    $doc = $db->findOne('php_test', ['name' => 'Bob']);
    assert_eq('Bob', $doc['name']);
});

test('count', function () use ($db) {
    $n = $db->count('php_test');
    assert_true($n >= 3, "expected >= 3, got {$n}");
});

test('update', function () use ($db) {
    $result = $db->update('php_test', ['name' => 'Alice'], ['$set' => ['age' => 31]]);
    assert_eq(1, $result['modified']);

    $doc = $db->findOne('php_test', ['name' => 'Alice']);
    assert_eq(31, $doc['age']);
});

test('delete', function () use ($db) {
    $result = $db->delete('php_test', ['name' => 'Charlie']);
    assert_eq(1, $result['deleted']);
});

test('indexes', function () use ($db) {
    $db->createIndex('php_test', 'name');
    $db->createUniqueIndex('php_test', 'age');
    $db->createCompositeIndex('php_test', ['name', 'age']);
});

test('aggregation', function () use ($db) {
    $results = $db->aggregate('php_test', [
        ['$group' => ['_id' => null, 'avg_age' => ['$avg' => '$age']]],
    ]);
    assert_true(count($results) >= 1);
});

test('transaction', function () use ($db) {
    $db->transaction(function () use ($db) {
        $db->insert('php_tx', ['action' => 'debit', 'amount' => 100]);
        $db->insert('php_tx', ['action' => 'credit', 'amount' => 100]);
    });

    $docs = $db->find('php_tx');
    assert_eq(2, count($docs));
});

test('blob storage', function () use ($db) {
    $db->createBucket('php-bucket');

    $buckets = $db->listBuckets();
    assert_true(in_array('php-bucket', $buckets));

    $db->putObject('php-bucket', 'hello.txt', 'Hello from PHP!');

    [$data, $meta] = $db->getObject('php-bucket', 'hello.txt');
    assert_eq('Hello from PHP!', $data);

    $head = $db->headObject('php-bucket', 'hello.txt');
    assert_true(isset($head['size']));

    $objs = $db->listObjects('php-bucket');
    assert_true(count($objs) >= 1);

    $db->deleteObject('php-bucket', 'hello.txt');
});

test('search', function () use ($db) {
    $results = $db->search('hello');
    assert_true(is_array($results));
});

test('compact', function () use ($db) {
    $stats = $db->compact('php_test');
    assert_true(isset($stats['docs_kept']));
});

test('cleanup', function () use ($db) {
    $db->dropCollection('php_test');
    $db->dropCollection('php_tx');
    $db->deleteBucket('php-bucket');
});

$db->close();

echo "\n{$passed} passed, {$failed} failed\n";
exit($failed > 0 ? 1 : 0);
