<?php
/**
 * OxiDB PHP Client — Integration Test Suite
 *
 * Usage: php test_oxidb.php [port]
 * Requires oxidb-server running on the specified port (default: 4444).
 */

require_once __DIR__ . '/OxiDb.php';

$port = isset($argv[1]) ? (int)$argv[1] : 4444;
$passed = 0;
$failed = 0;

function assert_test(bool $cond, string $name, string $detail = ''): void
{
    global $passed, $failed;
    if ($cond) {
        $passed++;
        echo "  PASS: $name\n";
    } else {
        $failed++;
        echo "  FAIL: $name $detail\n";
    }
}

echo "=== OxiDB PHP Client Test (OxiWire protocol) ===\n\n";

$db = new OxiDbClient('127.0.0.1', $port);
$col = 'php_test_' . bin2hex(random_bytes(8));

// 1. Ping
$pong = $db->ping();
assert_test($pong === 'pong', 'Ping returns pong', "got '$pong'");

// 2. Insert + Find
$result = $db->insert($col, ['name' => 'Alice', 'age' => 30, 'active' => true, 'score' => 99.5]);
assert_test(isset($result['id']), 'Insert returns id');

$docs = $db->find($col);
assert_test(count($docs) === 1, 'Find returns 1 doc', 'got ' . count($docs));
$doc = $docs[0];
assert_test($doc['name'] === 'Alice', 'String field roundtrip');
assert_test($doc['age'] === 30, 'Integer field roundtrip');
assert_test($doc['active'] === true, 'Boolean field roundtrip');
assert_test(abs($doc['score'] - 99.5) < 0.001, 'Float field roundtrip');

// 3. InsertMany + Count
$batch = [];
for ($i = 0; $i < 50; $i++) {
    $batch[] = ['n' => $i, 'label' => "item_$i"];
}
$db->insertMany($col, $batch);
$count = $db->count($col);
assert_test($count === 51, 'InsertMany + Count', "expected 51, got $count");

// 4. Query with filter
$filtered = $db->find($col, ['n' => ['$gte' => 40]]);
assert_test(count($filtered) === 10, 'Range query $gte=40', 'got ' . count($filtered));

// 5. Sort + limit
$sorted = $db->find($col,
    ['n' => ['$gte' => 0]],
    ['sort' => ['n' => -1], 'limit' => 3]
);
assert_test(count($sorted) === 3, 'Sort desc + limit 3');
assert_test($sorted[0]['n'] === 49, 'Sort desc first is 49', 'got ' . $sorted[0]['n']);

// 6. Update
$updateResult = $db->update($col,
    ['name' => 'Alice'],
    ['$set' => ['age' => 31]]
);
assert_test($updateResult['modified'] === 1, 'Update modified 1');

$updated = $db->findOne($col, ['name' => 'Alice']);
assert_test($updated['age'] === 31, 'Update value persisted');

// 7. Delete
$deleteResult = $db->delete($col, ['n' => ['$lt' => 5]]);
assert_test($deleteResult['deleted'] === 5, 'Delete removed 5 docs', 'got ' . $deleteResult['deleted']);

// 8. Transaction commit
$db->beginTransaction();
$db->insert($col, ['tx' => 'committed']);
$db->commit();
$txDoc = $db->findOne($col, ['tx' => 'committed']);
assert_test($txDoc['tx'] === 'committed', 'Tx commit visible');

// 9. Transaction rollback
$countBefore = $db->count($col);
$db->beginTransaction();
$db->insert($col, ['tx' => 'rolled_back']);
$db->rollback();
$countAfter = $db->count($col);
assert_test($countAfter === $countBefore, 'Tx rollback discards insert');

// 10. Error handling
try {
    $db->createCollection($col);
    $db->createCollection($col);
    assert_test(false, 'Duplicate collection throws');
} catch (OxiDbException $e) {
    assert_test(strpos($e->getMessage(), 'already exists') !== false, 'Error message decoded', $e->getMessage());
}

// 11. Negative numbers
$db->insert($col, ['balance' => -100, 'tag' => 'negative']);
$negDoc = $db->findOne($col, ['tag' => 'negative']);
assert_test($negDoc['balance'] === -100, 'Negative int roundtrip', 'got ' . ($negDoc['balance'] ?? 'null'));

// 12. Null values
$db->insert($col, ['nullable' => null, 'tag' => 'nulltest']);
$nullDoc = $db->findOne($col, ['tag' => 'nulltest']);
assert_test($nullDoc['nullable'] === null, 'Null roundtrip');

// 13. Index operations
$db->createIndex($col, 'n');
$db->createUniqueIndex($col, 'tag');

try {
    $db->insert($col, ['tag' => 'negative']);
    assert_test(false, 'Unique index violation throws');
} catch (OxiDbException $e) {
    assert_test(true, 'Unique index violation throws');
}

// 14. Aggregation
$aggResult = $db->aggregate($col, [
    ['$match' => ['n' => ['$gte' => 0]]],
    ['$count' => 'total'],
]);
assert_test(count($aggResult) === 1, 'Aggregation returns result');
assert_test($aggResult[0]['total'] > 0, 'Aggregation count > 0');

// 15. SQL with MySQL dialect
$sqlResult = $db->sql("SELECT * FROM `$col` WHERE age = 31", 'mysql');
assert_test(is_array($sqlResult) && count($sqlResult) === 1, 'SQL query with MySQL dialect');

// 16. Transaction closure
$db->transaction(function ($client) use ($col) {
    $client->insert($col, ['tx_closure' => 'works']);
});
$closureDoc = $db->findOne($col, ['tx_closure' => 'works']);
assert_test($closureDoc !== null && $closureDoc['tx_closure'] === 'works', 'Transaction closure');

// 17. Text index
$db->createTextIndex($col, ['name', 'tag']);
assert_test(true, 'Create text index');

// 18. Text search
$db->insert($col, ['name' => 'searchable document', 'tag' => 'findme']);
$textResults = $db->textSearch($col, 'searchable');
assert_test(count($textResults) >= 1, 'Text search returns results');

// 19. Composite index
$db->createCompositeIndex($col, ['name', 'age']);
$indexes = $db->listIndexes($col);
assert_test(count($indexes) >= 3, 'List indexes includes composite', 'got ' . count($indexes));

// 20. Stored procedures
$db->createProcedure('test_proc', [
    'steps' => [
        ['step' => 'count', 'collection' => $col, 'query' => new \stdClass(), 'as' => 'n'],
        ['step' => 'return', 'value' => ['total' => '$n']],
    ],
]);
assert_test(true, 'Create procedure');

$procs = $db->listProcedures();
assert_test(in_array('test_proc', $procs), 'List procedures includes test_proc');

$procDef = $db->getProcedure('test_proc');
assert_test(isset($procDef['steps']), 'Get procedure returns definition');

$procResult = $db->callProcedure('test_proc');
assert_test(isset($procResult['total']), 'Call procedure returns result');

$db->deleteProcedure('test_proc');
$procsAfter = $db->listProcedures();
assert_test(!in_array('test_proc', $procsAfter), 'Delete procedure');

// 21. Cron schedules
$db->createSchedule('test_sched', [
    'cron' => '*/5 * * * *',
    'procedure' => 'nonexistent',
]);
assert_test(true, 'Create schedule');

$scheds = $db->listSchedules();
assert_test(count($scheds) >= 1, 'List schedules');

$sched = $db->getSchedule('test_sched');
assert_test(isset($sched['cron']), 'Get schedule returns definition');

$db->disableSchedule('test_sched');
assert_test(true, 'Disable schedule');

$db->enableSchedule('test_sched');
assert_test(true, 'Enable schedule');

$db->deleteSchedule('test_sched');
assert_test(true, 'Delete schedule');

// 22. Blob storage
$db->createBucket('test_bucket');
$db->putObject('test_bucket', 'hello.txt', 'Hello, World!', 'text/plain');
[$content, $meta] = $db->getObject('test_bucket', 'hello.txt');
assert_test($content === 'Hello, World!', 'Blob roundtrip');

$head = $db->headObject('test_bucket', 'hello.txt');
assert_test(isset($head['size']), 'Head object returns metadata');

$objects = $db->listObjects('test_bucket');
assert_test(count($objects) >= 1, 'List objects');

$db->deleteObject('test_bucket', 'hello.txt');
$db->deleteBucket('test_bucket');
assert_test(true, 'Delete blob + bucket');

// 23. Backup
$backupResult = $db->backup('/tmp/oxidb_php_test_backup.tar.gz');
assert_test(isset($backupResult['size_bytes']), 'Backup returns info');

// 24. Database management
$db->createDatabase('php_test_db');
$dbs = $db->listDatabases();
assert_test(in_array('php_test_db', $dbs), 'Create + list databases');

$db->useDatabase('php_test_db');
$db->insert('test_in_db', ['x' => 1]);
$found = $db->findOne('test_in_db', ['x' => 1]);
assert_test($found !== null && $found['x'] === 1, 'Use database + insert/find');

$db->useDatabase('default');
$db->dropDatabase('php_test_db');
assert_test(true, 'Drop database');

// Cleanup
$db->dropCollection($col);

echo "\n=== Results: $passed passed, $failed failed ===\n";
if ($failed > 0) exit(1);
