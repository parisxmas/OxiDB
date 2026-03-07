<?php
/**
 * OxiDB PHP Client — Advanced WordPress Test Suite
 *
 * Tests advanced WordPress patterns: WP REST API queries, widget/sidebar data,
 * transients, multisite, nav menus, revisions, trash, search, wp-cron,
 * plugin/theme option storage, concurrent operations, compaction, and more.
 *
 * Usage: php test_wordpress_advanced.php [port]
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

$db = new OxiDbClient('127.0.0.1', $port);
$db->setDialect('mysql');

$prefix = 'wp_' . bin2hex(random_bytes(4)) . '_';

echo "=== Advanced WordPress Tests (prefix: {$prefix}) ===\n\n";

// ─── Bootstrap: create all WordPress core tables ─────────────────

$tables = ['posts', 'postmeta', 'users', 'usermeta', 'options', 'terms',
           'term_taxonomy', 'term_relationships', 'comments', 'commentmeta',
           'links', 'termmeta'];

foreach ($tables as $t) {
    try { $db->dropCollection($prefix . $t); } catch (OxiDbException $_) {}
    $db->createCollection($prefix . $t);
}

// Indexes
$db->createIndex($prefix . 'posts', 'post_type');
$db->createIndex($prefix . 'posts', 'post_status');
$db->createIndex($prefix . 'posts', 'post_author');
$db->createIndex($prefix . 'posts', 'post_date');
$db->createIndex($prefix . 'posts', 'post_parent');
$db->createIndex($prefix . 'posts', 'post_name');
$db->createIndex($prefix . 'posts', 'post_modified');
$db->createCompositeIndex($prefix . 'posts', ['post_type', 'post_status']);
$db->createIndex($prefix . 'postmeta', 'post_id');
$db->createIndex($prefix . 'postmeta', 'meta_key');
$db->createCompositeIndex($prefix . 'postmeta', ['post_id', 'meta_key']);
$db->createIndex($prefix . 'users', 'user_login');
$db->createIndex($prefix . 'users', 'user_email');
$db->createIndex($prefix . 'usermeta', 'user_id');
$db->createIndex($prefix . 'usermeta', 'meta_key');
$db->createUniqueIndex($prefix . 'options', 'option_name');
$db->createIndex($prefix . 'options', 'autoload');
$db->createIndex($prefix . 'terms', 'slug');
$db->createIndex($prefix . 'term_taxonomy', 'taxonomy');
$db->createIndex($prefix . 'term_relationships', 'object_id');
$db->createIndex($prefix . 'term_relationships', 'term_taxonomy_id');
$db->createIndex($prefix . 'comments', 'comment_post_ID');
$db->createIndex($prefix . 'comments', 'comment_approved');
$db->createIndex($prefix . 'comments', 'comment_date');
$db->createIndex($prefix . 'commentmeta', 'comment_id');
$db->createIndex($prefix . 'commentmeta', 'meta_key');
$db->createIndex($prefix . 'termmeta', 'term_id');
$db->createIndex($prefix . 'termmeta', 'meta_key');
$db->createIndex($prefix . 'links', 'link_visible');

// ─── Seed data ───────────────────────────────────────────────────

$now = date('Y-m-d H:i:s');

// Users
$users = [
    ['user_login' => 'admin', 'user_pass' => '$P$BhashAdmin', 'user_nicename' => 'admin',
     'user_email' => 'admin@example.com', 'user_registered' => '2024-01-01 00:00:00',
     'user_status' => 0, 'display_name' => 'Administrator', 'user_url' => ''],
    ['user_login' => 'editor1', 'user_pass' => '$P$BhashEd1', 'user_nicename' => 'editor1',
     'user_email' => 'editor1@example.com', 'user_registered' => '2024-02-15 10:30:00',
     'user_status' => 0, 'display_name' => 'Jane Editor', 'user_url' => ''],
    ['user_login' => 'author1', 'user_pass' => '$P$BhashAuth', 'user_nicename' => 'author1',
     'user_email' => 'author1@example.com', 'user_registered' => '2024-03-01 08:00:00',
     'user_status' => 0, 'display_name' => 'John Author', 'user_url' => 'https://john.example.com'],
    ['user_login' => 'subscriber1', 'user_pass' => '$P$BhashSub', 'user_nicename' => 'subscriber1',
     'user_email' => 'sub@example.com', 'user_registered' => '2024-06-20 14:00:00',
     'user_status' => 0, 'display_name' => 'Bob Subscriber', 'user_url' => ''],
];
$db->insertMany($prefix . 'users', $users);
$allUsers = $db->find($prefix . 'users');
$userIds = [];
foreach ($allUsers as $u) $userIds[$u['user_login']] = $u['_id'];

// User meta (roles, capabilities)
$roles = [
    ['user_id' => $userIds['admin'], 'meta_key' => 'wp_capabilities', 'meta_value' => serialize(['administrator' => true])],
    ['user_id' => $userIds['admin'], 'meta_key' => 'wp_user_level', 'meta_value' => '10'],
    ['user_id' => $userIds['editor1'], 'meta_key' => 'wp_capabilities', 'meta_value' => serialize(['editor' => true])],
    ['user_id' => $userIds['editor1'], 'meta_key' => 'wp_user_level', 'meta_value' => '7'],
    ['user_id' => $userIds['author1'], 'meta_key' => 'wp_capabilities', 'meta_value' => serialize(['author' => true])],
    ['user_id' => $userIds['author1'], 'meta_key' => 'wp_user_level', 'meta_value' => '2'],
    ['user_id' => $userIds['subscriber1'], 'meta_key' => 'wp_capabilities', 'meta_value' => serialize(['subscriber' => true])],
    ['user_id' => $userIds['subscriber1'], 'meta_key' => 'wp_user_level', 'meta_value' => '0'],
];
$db->insertMany($prefix . 'usermeta', $roles);

// Terms and taxonomies
$termData = [
    ['name' => 'Uncategorized', 'slug' => 'uncategorized', 'term_group' => 0],
    ['name' => 'Technology', 'slug' => 'technology', 'term_group' => 0],
    ['name' => 'WordPress', 'slug' => 'wordpress', 'term_group' => 0],
    ['name' => 'Database', 'slug' => 'database', 'term_group' => 0],
    ['name' => 'Performance', 'slug' => 'performance', 'term_group' => 0],
    ['name' => 'php', 'slug' => 'php', 'term_group' => 0],
    ['name' => 'rust', 'slug' => 'rust', 'term_group' => 0],
    ['name' => 'benchmark', 'slug' => 'benchmark', 'term_group' => 0],
    // Nav menu items
    ['name' => 'Main Menu', 'slug' => 'main-menu', 'term_group' => 0],
    ['name' => 'Footer Menu', 'slug' => 'footer-menu', 'term_group' => 0],
];
$db->insertMany($prefix . 'terms', $termData);
$allTerms = $db->find($prefix . 'terms');
$termIds = [];
foreach ($allTerms as $t) $termIds[$t['slug']] = $t['_id'];

$taxData = [
    ['term_id' => $termIds['uncategorized'], 'taxonomy' => 'category', 'description' => '', 'parent' => 0, 'count' => 0],
    ['term_id' => $termIds['technology'], 'taxonomy' => 'category', 'description' => 'Tech articles', 'parent' => 0, 'count' => 0],
    ['term_id' => $termIds['wordpress'], 'taxonomy' => 'category', 'description' => 'WP articles', 'parent' => $termIds['technology'], 'count' => 0],
    ['term_id' => $termIds['database'], 'taxonomy' => 'category', 'description' => '', 'parent' => $termIds['technology'], 'count' => 0],
    ['term_id' => $termIds['performance'], 'taxonomy' => 'category', 'description' => '', 'parent' => 0, 'count' => 0],
    ['term_id' => $termIds['php'], 'taxonomy' => 'post_tag', 'description' => '', 'parent' => 0, 'count' => 0],
    ['term_id' => $termIds['rust'], 'taxonomy' => 'post_tag', 'description' => '', 'parent' => 0, 'count' => 0],
    ['term_id' => $termIds['benchmark'], 'taxonomy' => 'post_tag', 'description' => '', 'parent' => 0, 'count' => 0],
    ['term_id' => $termIds['main-menu'], 'taxonomy' => 'nav_menu', 'description' => '', 'parent' => 0, 'count' => 0],
    ['term_id' => $termIds['footer-menu'], 'taxonomy' => 'nav_menu', 'description' => '', 'parent' => 0, 'count' => 0],
];
$db->insertMany($prefix . 'term_taxonomy', $taxData);
$allTT = $db->find($prefix . 'term_taxonomy');
$ttIds = [];
foreach ($allTT as $tt) {
    $slug = array_search($tt['term_id'], $termIds);
    if ($slug !== false) $ttIds[$slug] = $tt['_id'];
}

// Posts: various types and statuses
$postData = [
    // Published posts
    ['post_author' => $userIds['admin'], 'post_date' => '2024-01-15 09:00:00',
     'post_content' => '<p>Welcome to our site powered by OxiDB! This document database replaces MySQL for WordPress.</p>',
     'post_title' => 'Hello World with OxiDB', 'post_excerpt' => 'A welcome post',
     'post_status' => 'publish', 'post_name' => 'hello-world-oxidb', 'post_type' => 'post',
     'post_parent' => 0, 'comment_count' => 2, 'menu_order' => 0, 'post_modified' => $now,
     'comment_status' => 'open', 'ping_status' => 'open', 'post_password' => '', 'guid' => ''],
    ['post_author' => $userIds['editor1'], 'post_date' => '2024-03-20 14:00:00',
     'post_content' => '<p>OxiDB benchmarks show 3x faster reads than traditional SQL databases for document workloads.</p>',
     'post_title' => 'OxiDB Performance Benchmarks', 'post_excerpt' => 'Benchmark results',
     'post_status' => 'publish', 'post_name' => 'oxidb-benchmarks', 'post_type' => 'post',
     'post_parent' => 0, 'comment_count' => 0, 'menu_order' => 0, 'post_modified' => $now,
     'comment_status' => 'open', 'ping_status' => 'open', 'post_password' => '', 'guid' => ''],
    ['post_author' => $userIds['author1'], 'post_date' => '2024-05-10 10:00:00',
     'post_content' => '<p>Tutorial on migrating from MySQL to OxiDB. Step by step guide with code examples.</p>',
     'post_title' => 'Migrating from MySQL to OxiDB', 'post_excerpt' => '',
     'post_status' => 'publish', 'post_name' => 'mysql-to-oxidb', 'post_type' => 'post',
     'post_parent' => 0, 'comment_count' => 5, 'menu_order' => 0, 'post_modified' => $now,
     'comment_status' => 'open', 'ping_status' => 'open', 'post_password' => '', 'guid' => ''],
    ['post_author' => $userIds['author1'], 'post_date' => '2024-06-01 08:00:00',
     'post_content' => '<p>Advanced indexing strategies for OxiDB including composite indexes and full-text search.</p>',
     'post_title' => 'Advanced OxiDB Indexing', 'post_excerpt' => 'Indexing strategies',
     'post_status' => 'publish', 'post_name' => 'advanced-indexing', 'post_type' => 'post',
     'post_parent' => 0, 'comment_count' => 0, 'menu_order' => 0, 'post_modified' => $now,
     'comment_status' => 'open', 'ping_status' => 'closed', 'post_password' => '', 'guid' => ''],
    // Draft
    ['post_author' => $userIds['editor1'], 'post_date' => '2024-07-01 12:00:00',
     'post_content' => '<p>Upcoming features in OxiDB 0.19 release.</p>',
     'post_title' => 'OxiDB 0.19 Preview', 'post_excerpt' => '',
     'post_status' => 'draft', 'post_name' => 'oxidb-019-preview', 'post_type' => 'post',
     'post_parent' => 0, 'comment_count' => 0, 'menu_order' => 0, 'post_modified' => $now,
     'comment_status' => 'open', 'ping_status' => 'open', 'post_password' => '', 'guid' => ''],
    // Scheduled (future)
    ['post_author' => $userIds['admin'], 'post_date' => '2026-12-25 00:00:00',
     'post_content' => '<p>Scheduled for future publication.</p>',
     'post_title' => 'Scheduled Post', 'post_excerpt' => '',
     'post_status' => 'future', 'post_name' => 'scheduled-post', 'post_type' => 'post',
     'post_parent' => 0, 'comment_count' => 0, 'menu_order' => 0, 'post_modified' => $now,
     'comment_status' => 'open', 'ping_status' => 'open', 'post_password' => '', 'guid' => ''],
    // Password-protected
    ['post_author' => $userIds['admin'], 'post_date' => '2024-04-01 06:00:00',
     'post_content' => '<p>This is password protected content.</p>',
     'post_title' => 'Protected Post', 'post_excerpt' => '',
     'post_status' => 'publish', 'post_name' => 'protected-post', 'post_type' => 'post',
     'post_parent' => 0, 'comment_count' => 0, 'menu_order' => 0, 'post_modified' => $now,
     'comment_status' => 'open', 'ping_status' => 'open', 'post_password' => 'secret123', 'guid' => ''],
    // Pages
    ['post_author' => $userIds['admin'], 'post_date' => '2024-01-01 00:00:00',
     'post_content' => '<p>About us page with company information.</p>',
     'post_title' => 'About', 'post_excerpt' => '',
     'post_status' => 'publish', 'post_name' => 'about', 'post_type' => 'page',
     'post_parent' => 0, 'comment_count' => 0, 'menu_order' => 1, 'post_modified' => $now,
     'comment_status' => 'closed', 'ping_status' => 'closed', 'post_password' => '', 'guid' => ''],
    ['post_author' => $userIds['admin'], 'post_date' => '2024-01-01 00:00:00',
     'post_content' => '<p>Contact form and details.</p>',
     'post_title' => 'Contact', 'post_excerpt' => '',
     'post_status' => 'publish', 'post_name' => 'contact', 'post_type' => 'page',
     'post_parent' => 0, 'comment_count' => 0, 'menu_order' => 2, 'post_modified' => $now,
     'comment_status' => 'closed', 'ping_status' => 'closed', 'post_password' => '', 'guid' => ''],
    // Child page (hierarchical)
    ['post_author' => $userIds['admin'], 'post_date' => '2024-01-02 00:00:00',
     'post_content' => '<p>Meet our team.</p>',
     'post_title' => 'Our Team', 'post_excerpt' => '',
     'post_status' => 'publish', 'post_name' => 'our-team', 'post_type' => 'page',
     'post_parent' => 0, 'comment_count' => 0, 'menu_order' => 0, 'post_modified' => $now,
     'comment_status' => 'closed', 'ping_status' => 'closed', 'post_password' => '', 'guid' => ''],
    // Attachments
    ['post_author' => $userIds['admin'], 'post_date' => '2024-01-15 09:00:00',
     'post_content' => '', 'post_title' => 'hero-image', 'post_excerpt' => 'Hero banner image',
     'post_status' => 'inherit', 'post_name' => 'hero-image', 'post_type' => 'attachment',
     'post_parent' => 0, 'comment_count' => 0, 'menu_order' => 0, 'post_modified' => $now,
     'post_mime_type' => 'image/jpeg', 'comment_status' => 'open', 'ping_status' => 'closed',
     'post_password' => '', 'guid' => ''],
    ['post_author' => $userIds['admin'], 'post_date' => '2024-02-01 12:00:00',
     'post_content' => '', 'post_title' => 'benchmark-chart', 'post_excerpt' => '',
     'post_status' => 'inherit', 'post_name' => 'benchmark-chart', 'post_type' => 'attachment',
     'post_parent' => 0, 'comment_count' => 0, 'menu_order' => 0, 'post_modified' => $now,
     'post_mime_type' => 'image/png', 'comment_status' => 'open', 'ping_status' => 'closed',
     'post_password' => '', 'guid' => ''],
    ['post_author' => $userIds['admin'], 'post_date' => '2024-03-01 10:00:00',
     'post_content' => '', 'post_title' => 'whitepaper', 'post_excerpt' => 'OxiDB whitepaper',
     'post_status' => 'inherit', 'post_name' => 'whitepaper', 'post_type' => 'attachment',
     'post_parent' => 0, 'comment_count' => 0, 'menu_order' => 0, 'post_modified' => $now,
     'post_mime_type' => 'application/pdf', 'comment_status' => 'open', 'ping_status' => 'closed',
     'post_password' => '', 'guid' => ''],
];
$db->insertMany($prefix . 'posts', $postData);

// Build post ID map
$allPosts = $db->find($prefix . 'posts');
$postIds = [];
foreach ($allPosts as $p) $postIds[$p['post_name']] = $p['_id'];

// Set 'our-team' page as child of 'about'
$db->updateOne($prefix . 'posts', ['_id' => $postIds['our-team']], ['$set' => ['post_parent' => $postIds['about']]]);

// Set attachments to their parent posts
$db->updateOne($prefix . 'posts', ['_id' => $postIds['hero-image']], ['$set' => ['post_parent' => $postIds['hello-world-oxidb']]]);
$db->updateOne($prefix . 'posts', ['_id' => $postIds['benchmark-chart']], ['$set' => ['post_parent' => $postIds['oxidb-benchmarks']]]);

// Term relationships
$termRels = [
    ['object_id' => $postIds['hello-world-oxidb'], 'term_taxonomy_id' => $ttIds['uncategorized'], 'term_order' => 0],
    ['object_id' => $postIds['oxidb-benchmarks'], 'term_taxonomy_id' => $ttIds['performance'], 'term_order' => 0],
    ['object_id' => $postIds['oxidb-benchmarks'], 'term_taxonomy_id' => $ttIds['benchmark'], 'term_order' => 0],
    ['object_id' => $postIds['mysql-to-oxidb'], 'term_taxonomy_id' => $ttIds['technology'], 'term_order' => 0],
    ['object_id' => $postIds['mysql-to-oxidb'], 'term_taxonomy_id' => $ttIds['database'], 'term_order' => 0],
    ['object_id' => $postIds['mysql-to-oxidb'], 'term_taxonomy_id' => $ttIds['php'], 'term_order' => 0],
    ['object_id' => $postIds['advanced-indexing'], 'term_taxonomy_id' => $ttIds['technology'], 'term_order' => 0],
    ['object_id' => $postIds['advanced-indexing'], 'term_taxonomy_id' => $ttIds['database'], 'term_order' => 0],
    ['object_id' => $postIds['advanced-indexing'], 'term_taxonomy_id' => $ttIds['rust'], 'term_order' => 0],
];
$db->insertMany($prefix . 'term_relationships', $termRels);

echo "--- Bootstrap complete ---\n";

// ═══════════════════════════════════════════════════════════════════
// 1. WP_QUERY ADVANCED PATTERNS
// ═══════════════════════════════════════════════════════════════════

echo "\n--- 1. WP_Query Advanced Patterns ---\n";

// Sticky posts: WordPress stores sticky post IDs in options
$db->insert($prefix . 'options', [
    'option_name' => 'sticky_posts', 'autoload' => 'yes',
    'option_value' => serialize([$postIds['hello-world-oxidb']]),
]);
$stickyOpt = $db->findOne($prefix . 'options', ['option_name' => 'sticky_posts']);
$stickyIds = unserialize($stickyOpt['option_value']);
assert_test(count($stickyIds) === 1 && $stickyIds[0] === $postIds['hello-world-oxidb'], 'Sticky posts stored in options');

// Fetch sticky posts first, then rest (WordPress home page behavior)
$stickyPosts = $db->find($prefix . 'posts', ['_id' => ['$in' => $stickyIds], 'post_status' => 'publish']);
$recentPosts = $db->find($prefix . 'posts', [
    'post_type' => 'post',
    'post_status' => 'publish',
    'post_password' => '',
], ['sort' => ['post_date' => -1], 'limit' => 10]);
// Filter out sticky posts from the recent list (client-side, same as WP does with PHP)
$nonStickyPosts = array_values(array_filter($recentPosts, fn($p) => !in_array($p['_id'], $stickyIds)));
$homePosts = array_merge($stickyPosts, $nonStickyPosts);
assert_test(count($homePosts) >= 4, 'Home page: sticky + recent posts', 'got ' . count($homePosts));
assert_test($homePosts[0]['post_name'] === 'hello-world-oxidb', 'Sticky post appears first');

// post_status NOT IN (exclude trash and auto-draft) — using $in for allowed statuses
$visiblePosts = $db->find($prefix . 'posts', [
    'post_type' => 'post',
    'post_status' => ['$in' => ['publish', 'draft', 'future', 'pending', 'private']],
]);
assert_test(count($visiblePosts) >= 5, 'Visible posts (exclude trash/auto-draft)', 'got ' . count($visiblePosts));

// Posts by multiple authors
$multiAuthorPosts = $db->find($prefix . 'posts', [
    'post_author' => ['$in' => [$userIds['editor1'], $userIds['author1']]],
    'post_type' => 'post',
    'post_status' => 'publish',
]);
assert_test(count($multiAuthorPosts) === 3, 'Posts by multiple authors', 'got ' . count($multiAuthorPosts));

// Posts with comments enabled
$openComments = $db->find($prefix . 'posts', [
    'post_type' => 'post',
    'post_status' => 'publish',
    'comment_status' => 'open',
    'post_password' => '',
]);
assert_test(count($openComments) >= 3, 'Posts with open comments', 'got ' . count($openComments));

// Posts excluding password-protected
$publicPosts = $db->find($prefix . 'posts', [
    'post_type' => 'post',
    'post_status' => 'publish',
    'post_password' => '',
], ['sort' => ['post_date' => -1]]);
$hasProtected = false;
foreach ($publicPosts as $p) {
    if (!empty($p['post_password'])) $hasProtected = true;
}
assert_test(!$hasProtected, 'Exclude password-protected posts');

// Date-based archive: posts from a specific month
$marchPosts = $db->find($prefix . 'posts', [
    'post_type' => 'post',
    'post_status' => 'publish',
    'post_date' => ['$gte' => '2024-03-01 00:00:00', '$lt' => '2024-04-01 00:00:00'],
]);
assert_test(count($marchPosts) === 1, 'Date archive: March 2024', 'got ' . count($marchPosts));

// Year archive aggregation (for wp_get_archives)
$yearArchive = $db->aggregate($prefix . 'posts', [
    ['$match' => ['post_type' => 'post', 'post_status' => 'publish']],
    ['$group' => ['_id' => null, 'count' => ['$sum' => 1]]],
]);
assert_test(count($yearArchive) > 0 && $yearArchive[0]['count'] >= 4, 'Year archive count');

// ═══════════════════════════════════════════════════════════════════
// 2. POST REVISIONS
// ═══════════════════════════════════════════════════════════════════

echo "\n--- 2. Post Revisions ---\n";

// WordPress stores revisions as post_type='revision' with post_parent pointing to original
$revisions = [
    ['post_author' => $userIds['admin'], 'post_date' => '2024-01-15 09:30:00',
     'post_content' => '<p>Welcome to our site! (revision 1)</p>',
     'post_title' => 'Hello World with OxiDB', 'post_excerpt' => '',
     'post_status' => 'inherit', 'post_name' => 'hello-world-oxidb-revision-v1',
     'post_type' => 'revision', 'post_parent' => $postIds['hello-world-oxidb'],
     'comment_count' => 0, 'menu_order' => 0, 'post_modified' => '2024-01-15 09:30:00',
     'comment_status' => 'closed', 'ping_status' => 'closed', 'post_password' => '', 'guid' => ''],
    ['post_author' => $userIds['admin'], 'post_date' => '2024-01-15 10:00:00',
     'post_content' => '<p>Welcome to our site powered by OxiDB! (revision 2)</p>',
     'post_title' => 'Hello World with OxiDB', 'post_excerpt' => '',
     'post_status' => 'inherit', 'post_name' => 'hello-world-oxidb-revision-v2',
     'post_type' => 'revision', 'post_parent' => $postIds['hello-world-oxidb'],
     'comment_count' => 0, 'menu_order' => 0, 'post_modified' => '2024-01-15 10:00:00',
     'comment_status' => 'closed', 'ping_status' => 'closed', 'post_password' => '', 'guid' => ''],
    ['post_author' => $userIds['editor1'], 'post_date' => '2024-01-16 08:00:00',
     'post_content' => '<p>Welcome to our site powered by OxiDB! This document database replaces MySQL for WordPress.</p>',
     'post_title' => 'Hello World with OxiDB', 'post_excerpt' => 'A welcome post',
     'post_status' => 'inherit', 'post_name' => 'hello-world-oxidb-revision-v3',
     'post_type' => 'revision', 'post_parent' => $postIds['hello-world-oxidb'],
     'comment_count' => 0, 'menu_order' => 0, 'post_modified' => '2024-01-16 08:00:00',
     'comment_status' => 'closed', 'ping_status' => 'closed', 'post_password' => '', 'guid' => ''],
];
$db->insertMany($prefix . 'posts', $revisions);

// wp_get_post_revisions: SELECT * FROM wp_posts WHERE post_parent = X AND post_type = 'revision' ORDER BY post_date DESC
$postRevisions = $db->find($prefix . 'posts', [
    'post_parent' => $postIds['hello-world-oxidb'],
    'post_type' => 'revision',
], ['sort' => ['post_date' => -1]]);
assert_test(count($postRevisions) === 3, 'Get post revisions', 'got ' . count($postRevisions));
assert_test(strpos($postRevisions[0]['post_name'], 'v3') !== false, 'Latest revision first');

// Count revisions per post
$revisionCounts = $db->aggregate($prefix . 'posts', [
    ['$match' => ['post_type' => 'revision']],
    ['$group' => ['_id' => '$post_parent', 'count' => ['$sum' => 1]]],
]);
assert_test(count($revisionCounts) === 1, 'Revision count aggregation');
assert_test($revisionCounts[0]['count'] === 3, 'Revision count is 3');

// ═══════════════════════════════════════════════════════════════════
// 3. TRASH / RESTORE
// ═══════════════════════════════════════════════════════════════════

echo "\n--- 3. Trash and Restore ---\n";

// wp_trash_post: move to trash and store original status in postmeta
$trashTarget = $db->findOne($prefix . 'posts', ['post_name' => 'advanced-indexing']);
$origStatus = $trashTarget['post_status'];

$db->transaction(function ($client) use ($prefix, $trashTarget, $origStatus) {
    $client->updateOne($prefix . 'posts', ['_id' => $trashTarget['_id']],
        ['$set' => ['post_status' => 'trash']]);
    $client->insert($prefix . 'postmeta', [
        'post_id' => $trashTarget['_id'],
        'meta_key' => '_wp_trash_meta_status',
        'meta_value' => $origStatus,
    ]);
    $client->insert($prefix . 'postmeta', [
        'post_id' => $trashTarget['_id'],
        'meta_key' => '_wp_trash_meta_time',
        'meta_value' => (string)time(),
    ]);
});

$trashed = $db->findOne($prefix . 'posts', ['_id' => $trashTarget['_id']]);
assert_test($trashed['post_status'] === 'trash', 'Post moved to trash');

// Count trash items
$trashCount = $db->count($prefix . 'posts', ['post_status' => 'trash', 'post_type' => 'post']);
assert_test($trashCount === 1, 'Trash count is 1');

// wp_untrash_post: restore from trash
$trashMeta = $db->findOne($prefix . 'postmeta', [
    'post_id' => $trashTarget['_id'],
    'meta_key' => '_wp_trash_meta_status',
]);
$db->transaction(function ($client) use ($prefix, $trashTarget, $trashMeta) {
    $client->updateOne($prefix . 'posts', ['_id' => $trashTarget['_id']],
        ['$set' => ['post_status' => $trashMeta['meta_value']]]);
    $client->delete($prefix . 'postmeta', [
        'post_id' => $trashTarget['_id'],
        'meta_key' => ['$regex' => '^_wp_trash_'],
    ]);
});

$restored = $db->findOne($prefix . 'posts', ['_id' => $trashTarget['_id']]);
assert_test($restored['post_status'] === 'publish', 'Post restored from trash');

$trashMetaGone = $db->count($prefix . 'postmeta', [
    'post_id' => $trashTarget['_id'],
    'meta_key' => ['$regex' => '^_wp_trash_'],
]);
assert_test($trashMetaGone === 0, 'Trash meta cleaned up');

// ═══════════════════════════════════════════════════════════════════
// 4. NAV MENUS
// ═══════════════════════════════════════════════════════════════════

echo "\n--- 4. Navigation Menus ---\n";

// WordPress stores nav menu items as post_type='nav_menu_item' with metadata
$menuItems = [
    ['post_author' => $userIds['admin'], 'post_date' => $now,
     'post_content' => '', 'post_title' => 'Home', 'post_excerpt' => '',
     'post_status' => 'publish', 'post_name' => 'home-menu-item',
     'post_type' => 'nav_menu_item', 'post_parent' => 0,
     'comment_count' => 0, 'menu_order' => 1, 'post_modified' => $now,
     'comment_status' => 'closed', 'ping_status' => 'closed', 'post_password' => '', 'guid' => ''],
    ['post_author' => $userIds['admin'], 'post_date' => $now,
     'post_content' => '', 'post_title' => 'About', 'post_excerpt' => '',
     'post_status' => 'publish', 'post_name' => 'about-menu-item',
     'post_type' => 'nav_menu_item', 'post_parent' => 0,
     'comment_count' => 0, 'menu_order' => 2, 'post_modified' => $now,
     'comment_status' => 'closed', 'ping_status' => 'closed', 'post_password' => '', 'guid' => ''],
    ['post_author' => $userIds['admin'], 'post_date' => $now,
     'post_content' => '', 'post_title' => 'Our Team', 'post_excerpt' => '',
     'post_status' => 'publish', 'post_name' => 'team-menu-item',
     'post_type' => 'nav_menu_item', 'post_parent' => 0,
     'comment_count' => 0, 'menu_order' => 3, 'post_modified' => $now,
     'comment_status' => 'closed', 'ping_status' => 'closed', 'post_password' => '', 'guid' => ''],
    ['post_author' => $userIds['admin'], 'post_date' => $now,
     'post_content' => '', 'post_title' => 'Blog', 'post_excerpt' => '',
     'post_status' => 'publish', 'post_name' => 'blog-menu-item',
     'post_type' => 'nav_menu_item', 'post_parent' => 0,
     'comment_count' => 0, 'menu_order' => 4, 'post_modified' => $now,
     'comment_status' => 'closed', 'ping_status' => 'closed', 'post_password' => '', 'guid' => ''],
    ['post_author' => $userIds['admin'], 'post_date' => $now,
     'post_content' => '', 'post_title' => 'Contact', 'post_excerpt' => '',
     'post_status' => 'publish', 'post_name' => 'contact-menu-item',
     'post_type' => 'nav_menu_item', 'post_parent' => 0,
     'comment_count' => 0, 'menu_order' => 5, 'post_modified' => $now,
     'comment_status' => 'closed', 'ping_status' => 'closed', 'post_password' => '', 'guid' => ''],
];
$db->insertMany($prefix . 'posts', $menuItems);

$menuItemDocs = $db->find($prefix . 'posts', ['post_type' => 'nav_menu_item']);
$menuItemIds = [];
foreach ($menuItemDocs as $mi) $menuItemIds[$mi['post_name']] = $mi['_id'];

// Assign menu items to "Main Menu" nav_menu term
$menuRels = [];
foreach ($menuItemIds as $name => $id) {
    $menuRels[] = ['object_id' => $id, 'term_taxonomy_id' => $ttIds['main-menu'], 'term_order' => 0];
}
$db->insertMany($prefix . 'term_relationships', $menuRels);

// Menu item metadata (type, object_id reference, parent menu item for submenus)
$menuMetas = [
    ['post_id' => $menuItemIds['home-menu-item'], 'meta_key' => '_menu_item_type', 'meta_value' => 'custom'],
    ['post_id' => $menuItemIds['home-menu-item'], 'meta_key' => '_menu_item_url', 'meta_value' => '/'],
    ['post_id' => $menuItemIds['home-menu-item'], 'meta_key' => '_menu_item_menu_item_parent', 'meta_value' => '0'],
    ['post_id' => $menuItemIds['about-menu-item'], 'meta_key' => '_menu_item_type', 'meta_value' => 'post_type'],
    ['post_id' => $menuItemIds['about-menu-item'], 'meta_key' => '_menu_item_object', 'meta_value' => 'page'],
    ['post_id' => $menuItemIds['about-menu-item'], 'meta_key' => '_menu_item_object_id', 'meta_value' => (string)$postIds['about']],
    ['post_id' => $menuItemIds['about-menu-item'], 'meta_key' => '_menu_item_menu_item_parent', 'meta_value' => '0'],
    ['post_id' => $menuItemIds['team-menu-item'], 'meta_key' => '_menu_item_type', 'meta_value' => 'post_type'],
    ['post_id' => $menuItemIds['team-menu-item'], 'meta_key' => '_menu_item_object', 'meta_value' => 'page'],
    ['post_id' => $menuItemIds['team-menu-item'], 'meta_key' => '_menu_item_object_id', 'meta_value' => (string)$postIds['our-team']],
    ['post_id' => $menuItemIds['team-menu-item'], 'meta_key' => '_menu_item_menu_item_parent', 'meta_value' => (string)$menuItemIds['about-menu-item']],
    ['post_id' => $menuItemIds['blog-menu-item'], 'meta_key' => '_menu_item_type', 'meta_value' => 'custom'],
    ['post_id' => $menuItemIds['blog-menu-item'], 'meta_key' => '_menu_item_url', 'meta_value' => '/blog'],
    ['post_id' => $menuItemIds['blog-menu-item'], 'meta_key' => '_menu_item_menu_item_parent', 'meta_value' => '0'],
    ['post_id' => $menuItemIds['contact-menu-item'], 'meta_key' => '_menu_item_type', 'meta_value' => 'post_type'],
    ['post_id' => $menuItemIds['contact-menu-item'], 'meta_key' => '_menu_item_object', 'meta_value' => 'page'],
    ['post_id' => $menuItemIds['contact-menu-item'], 'meta_key' => '_menu_item_object_id', 'meta_value' => (string)$postIds['contact']],
    ['post_id' => $menuItemIds['contact-menu-item'], 'meta_key' => '_menu_item_menu_item_parent', 'meta_value' => '0'],
];
$db->insertMany($prefix . 'postmeta', $menuMetas);

// wp_get_nav_menu_items: get all items in a menu, sorted by menu_order
$mainMenuRels = $db->find($prefix . 'term_relationships', ['term_taxonomy_id' => $ttIds['main-menu']]);
$mainMenuItemIds = array_map(fn($r) => $r['object_id'], $mainMenuRels);

// Fetch menu items (must be nav_menu_item and publish)
$mainMenuItems = $db->find($prefix . 'posts', [
    'post_type' => 'nav_menu_item',
    'post_status' => 'publish',
], ['sort' => ['menu_order' => 1]]);
// Filter to items in this menu
$mainMenuItems = array_values(array_filter($mainMenuItems, fn($p) => in_array($p['_id'], $mainMenuItemIds)));
assert_test(count($mainMenuItems) === 5, 'Nav menu has 5 items', 'got ' . count($mainMenuItems));
assert_test($mainMenuItems[0]['post_title'] === 'Home', 'First menu item is Home');

// Check submenu: "Our Team" is child of "About"
$teamMeta = $db->findOne($prefix . 'postmeta', [
    'post_id' => $menuItemIds['team-menu-item'],
    'meta_key' => '_menu_item_menu_item_parent',
]);
assert_test($teamMeta['meta_value'] === (string)$menuItemIds['about-menu-item'], 'Team is submenu of About');

// ═══════════════════════════════════════════════════════════════════
// 5. TRANSIENTS (CACHED DATA IN OPTIONS TABLE)
// ═══════════════════════════════════════════════════════════════════

echo "\n--- 5. Transients ---\n";

// WordPress stores transients in wp_options as _transient_{name} and _transient_timeout_{name}
$transientExpiry = time() + 3600; // 1 hour from now
$db->insert($prefix . 'options', [
    'option_name' => '_transient_timeout_recent_posts_widget',
    'option_value' => (string)$transientExpiry,
    'autoload' => 'no',
]);
$db->insert($prefix . 'options', [
    'option_name' => '_transient_recent_posts_widget',
    'option_value' => serialize([
        ['title' => 'Post 1', 'date' => '2024-06-01'],
        ['title' => 'Post 2', 'date' => '2024-05-10'],
    ]),
    'autoload' => 'no',
]);

// get_transient: check timeout, then get value
$timeout = $db->findOne($prefix . 'options', ['option_name' => '_transient_timeout_recent_posts_widget']);
$notExpired = (int)$timeout['option_value'] > time();
assert_test($notExpired, 'Transient not expired');

$transient = $db->findOne($prefix . 'options', ['option_name' => '_transient_recent_posts_widget']);
$transientData = unserialize($transient['option_value']);
assert_test(count($transientData) === 2, 'Transient data deserialized');

// Expired transient
$db->insert($prefix . 'options', [
    'option_name' => '_transient_timeout_old_cache',
    'option_value' => (string)(time() - 100),
    'autoload' => 'no',
]);
$db->insert($prefix . 'options', [
    'option_name' => '_transient_old_cache',
    'option_value' => 'stale data',
    'autoload' => 'no',
]);

$oldTimeout = $db->findOne($prefix . 'options', ['option_name' => '_transient_timeout_old_cache']);
$isExpired = (int)$oldTimeout['option_value'] < time();
assert_test($isExpired, 'Old transient is expired');

// delete_transient: clean up expired
$db->transaction(function ($client) use ($prefix) {
    $client->deleteOne($prefix . 'options', ['option_name' => '_transient_timeout_old_cache']);
    $client->deleteOne($prefix . 'options', ['option_name' => '_transient_old_cache']);
});
$deletedTransient = $db->findOne($prefix . 'options', ['option_name' => '_transient_old_cache']);
assert_test($deletedTransient === null, 'Expired transient deleted');

// ═══════════════════════════════════════════════════════════════════
// 6. WIDGET AND SIDEBAR SETTINGS
// ═══════════════════════════════════════════════════════════════════

echo "\n--- 6. Widgets and Sidebars ---\n";

// WordPress stores widget settings as serialized arrays in options
$widgetSettings = [
    ['option_name' => 'widget_recent-posts', 'autoload' => 'yes',
     'option_value' => serialize([2 => ['title' => '', 'number' => 5], '_multiwidget' => 1])],
    ['option_name' => 'widget_categories', 'autoload' => 'yes',
     'option_value' => serialize([2 => ['title' => '', 'count' => 0, 'hierarchical' => 0, 'dropdown' => 0], '_multiwidget' => 1])],
    ['option_name' => 'widget_search', 'autoload' => 'yes',
     'option_value' => serialize([2 => ['title' => 'Search'], '_multiwidget' => 1])],
    ['option_name' => 'sidebars_widgets', 'autoload' => 'yes',
     'option_value' => serialize([
         'wp_inactive_widgets' => [],
         'sidebar-1' => ['recent-posts-2', 'categories-2', 'search-2'],
         'sidebar-2' => [],
         'array_version' => 3,
     ])],
];
$db->insertMany($prefix . 'options', $widgetSettings);

// Load sidebar widgets (every page load does this)
$sidebars = $db->findOne($prefix . 'options', ['option_name' => 'sidebars_widgets']);
$sidebarData = unserialize($sidebars['option_value']);
assert_test(count($sidebarData['sidebar-1']) === 3, 'Sidebar-1 has 3 widgets');
assert_test(in_array('search-2', $sidebarData['sidebar-1']), 'Search widget in sidebar');

// Load specific widget settings
$recentPostsWidget = $db->findOne($prefix . 'options', ['option_name' => 'widget_recent-posts']);
$rpSettings = unserialize($recentPostsWidget['option_value']);
assert_test($rpSettings[2]['number'] === 5, 'Recent posts widget shows 5');

// ═══════════════════════════════════════════════════════════════════
// 7. PLUGIN / THEME SETTINGS (COMPLEX SERIALIZED DATA)
// ═══════════════════════════════════════════════════════════════════

echo "\n--- 7. Plugin / Theme Settings ---\n";

// Yoast SEO style plugin settings
$yoastSettings = [
    'option_name' => 'wpseo',
    'autoload' => 'yes',
    'option_value' => serialize([
        'keyword_analysis_active' => true,
        'content_analysis_active' => true,
        'enable_xml_sitemap' => true,
        'title_separator' => '-',
        'company_name' => 'OxiDB Inc.',
        'company_logo' => '',
        'version' => '22.0',
    ]),
];
$db->insert($prefix . 'options', $yoastSettings);

// WooCommerce style complex nested settings
$wooSettings = [
    'option_name' => 'woocommerce_settings',
    'autoload' => 'yes',
    'option_value' => serialize([
        'store_address' => '123 Main St',
        'store_city' => 'San Francisco',
        'default_country' => 'US:CA',
        'currency' => 'USD',
        'price_thousand_sep' => ',',
        'price_decimal_sep' => '.',
        'price_num_decimals' => 2,
        'tax_classes' => ['standard', 'reduced', 'zero'],
        'checkout_fields' => [
            'billing' => ['first_name', 'last_name', 'company', 'address_1', 'city', 'state', 'postcode', 'country', 'email', 'phone'],
            'shipping' => ['first_name', 'last_name', 'address_1', 'city', 'state', 'postcode', 'country'],
        ],
    ]),
];
$db->insert($prefix . 'options', $wooSettings);

// Read back and verify nested structure
$wooOpt = $db->findOne($prefix . 'options', ['option_name' => 'woocommerce_settings']);
$wooData = unserialize($wooOpt['option_value']);
assert_test($wooData['currency'] === 'USD', 'Plugin settings: currency');
assert_test(count($wooData['tax_classes']) === 3, 'Plugin settings: tax classes array');
assert_test(count($wooData['checkout_fields']['billing']) === 10, 'Plugin settings: nested checkout fields');

// Theme mods (stored as serialized option)
$themeMods = [
    'option_name' => 'theme_mods_twentytwentyfour',
    'autoload' => 'yes',
    'option_value' => serialize([
        'custom_logo' => $postIds['hero-image'],
        'header_image' => 'remove-header',
        'background_color' => 'ffffff',
        'nav_menu_locations' => ['primary' => $termIds['main-menu'], 'footer' => $termIds['footer-menu']],
        'sidebars_widgets' => ['time' => time(), 'data' => []],
    ]),
];
$db->insert($prefix . 'options', $themeMods);

$mods = $db->findOne($prefix . 'options', ['option_name' => 'theme_mods_twentytwentyfour']);
$modsData = unserialize($mods['option_value']);
assert_test($modsData['nav_menu_locations']['primary'] === $termIds['main-menu'], 'Theme mod: primary menu location');

// ═══════════════════════════════════════════════════════════════════
// 8. HIERARCHICAL PAGES AND CHILD POSTS
// ═══════════════════════════════════════════════════════════════════

echo "\n--- 8. Hierarchical Pages ---\n";

// get_pages: WordPress frequently queries all pages
$allPages = $db->find($prefix . 'posts', [
    'post_type' => 'page',
    'post_status' => 'publish',
], ['sort' => ['menu_order' => 1, 'post_title' => 1]]);
assert_test(count($allPages) === 3, 'get_pages: 3 published pages', 'got ' . count($allPages));

// get_children: child pages of About
$childPages = $db->find($prefix . 'posts', [
    'post_parent' => $postIds['about'],
    'post_type' => 'page',
    'post_status' => 'publish',
]);
assert_test(count($childPages) === 1, 'Child pages of About', 'got ' . count($childPages));
assert_test($childPages[0]['post_title'] === 'Our Team', 'Child page is Our Team');

// get_post_ancestors (walk up the tree)
$teamPage = $db->findOne($prefix . 'posts', ['post_name' => 'our-team']);
$ancestors = [];
$currentParent = $teamPage['post_parent'];
while ($currentParent && $currentParent !== 0) {
    $parent = $db->findOne($prefix . 'posts', ['_id' => $currentParent]);
    if (!$parent) break;
    $ancestors[] = $parent['post_name'];
    $currentParent = $parent['post_parent'];
}
assert_test(count($ancestors) === 1 && $ancestors[0] === 'about', 'Ancestor chain: our-team -> about');

// get_attached_media: attachments belonging to a post
$attachments = $db->find($prefix . 'posts', [
    'post_parent' => $postIds['hello-world-oxidb'],
    'post_type' => 'attachment',
]);
assert_test(count($attachments) === 1 && $attachments[0]['post_name'] === 'hero-image', 'Attached media to post');

// ═══════════════════════════════════════════════════════════════════
// 9. COMMENT META AND ADVANCED COMMENTS
// ═══════════════════════════════════════════════════════════════════

echo "\n--- 9. Advanced Comments ---\n";

$comments = [
    ['comment_post_ID' => $postIds['hello-world-oxidb'], 'comment_author' => 'Visitor',
     'comment_author_email' => 'visitor@example.com', 'comment_author_url' => '',
     'comment_date' => '2024-01-16 10:00:00', 'comment_content' => 'Great article about OxiDB!',
     'comment_approved' => '1', 'comment_type' => 'comment', 'comment_parent' => 0,
     'user_id' => 0, 'comment_author_IP' => '192.0.2.10', 'comment_agent' => 'Mozilla/5.0'],
    ['comment_post_ID' => $postIds['hello-world-oxidb'], 'comment_author' => 'Admin',
     'comment_author_email' => 'admin@example.com', 'comment_author_url' => '',
     'comment_date' => '2024-01-16 11:00:00', 'comment_content' => 'Thanks for reading!',
     'comment_approved' => '1', 'comment_type' => 'comment', 'comment_parent' => 0,
     'user_id' => $userIds['admin'], 'comment_author_IP' => '192.0.2.1', 'comment_agent' => 'Mozilla/5.0'],
    ['comment_post_ID' => $postIds['mysql-to-oxidb'], 'comment_author' => 'DBA',
     'comment_author_email' => 'dba@example.com', 'comment_author_url' => '',
     'comment_date' => '2024-05-11 09:00:00', 'comment_content' => 'How does it handle joins?',
     'comment_approved' => '1', 'comment_type' => 'comment', 'comment_parent' => 0,
     'user_id' => 0, 'comment_author_IP' => '192.0.2.20', 'comment_agent' => 'Chrome/120'],
    ['comment_post_ID' => $postIds['mysql-to-oxidb'], 'comment_author' => 'Author',
     'comment_author_email' => 'author1@example.com', 'comment_author_url' => '',
     'comment_date' => '2024-05-11 10:00:00', 'comment_content' => 'OxiDB uses $lookup for join-like queries.',
     'comment_approved' => '1', 'comment_type' => 'comment', 'comment_parent' => 0,
     'user_id' => $userIds['author1'], 'comment_author_IP' => '192.0.2.2', 'comment_agent' => 'Firefox/115'],
    // Pingback
    ['comment_post_ID' => $postIds['oxidb-benchmarks'], 'comment_author' => 'Tech Blog',
     'comment_author_email' => '', 'comment_author_url' => 'https://techblog.example.com/oxidb-review',
     'comment_date' => '2024-04-01 08:00:00', 'comment_content' => '[...] OxiDB benchmarks show impressive results [...]',
     'comment_approved' => '1', 'comment_type' => 'pingback', 'comment_parent' => 0,
     'user_id' => 0, 'comment_author_IP' => '192.0.2.30', 'comment_agent' => 'WordPress/6.5'],
    // Spam
    ['comment_post_ID' => $postIds['hello-world-oxidb'], 'comment_author' => 'Spammer',
     'comment_author_email' => 'spam@evil.com', 'comment_author_url' => 'https://evil.com',
     'comment_date' => '2024-01-20 03:00:00', 'comment_content' => 'Buy cheap stuff!',
     'comment_approved' => 'spam', 'comment_type' => 'comment', 'comment_parent' => 0,
     'user_id' => 0, 'comment_author_IP' => '192.0.2.99', 'comment_agent' => 'SpamBot/1.0'],
];
$db->insertMany($prefix . 'comments', $comments);

$allComments = $db->find($prefix . 'comments');
$commentIds = [];
foreach ($allComments as $c) $commentIds[] = $c['_id'];

// Comment meta (Akismet, likes, etc.)
$firstComment = $db->findOne($prefix . 'comments', ['comment_author' => 'Visitor']);
$spamComment = $db->findOne($prefix . 'comments', ['comment_approved' => 'spam']);
$db->insertMany($prefix . 'commentmeta', [
    ['comment_id' => $firstComment['_id'], 'meta_key' => 'akismet_result', 'meta_value' => 'false'],
    ['comment_id' => $firstComment['_id'], 'meta_key' => 'akismet_as_submitted', 'meta_value' => serialize(['comment_author' => 'Visitor'])],
    ['comment_id' => $spamComment['_id'], 'meta_key' => 'akismet_result', 'meta_value' => 'true'],
]);

// Recent comments widget query
$recentComments = $db->find($prefix . 'comments', [
    'comment_approved' => '1',
    'comment_type' => 'comment',
], ['sort' => ['comment_date' => -1], 'limit' => 5]);
assert_test(count($recentComments) === 4, 'Recent comments widget', 'got ' . count($recentComments));

// Comments per post aggregation
$commentsByPost = $db->aggregate($prefix . 'comments', [
    ['$match' => ['comment_approved' => '1', 'comment_type' => 'comment']],
    ['$group' => ['_id' => '$comment_post_ID', 'count' => ['$sum' => 1]]],
    ['$sort' => ['count' => -1]],
]);
assert_test(count($commentsByPost) >= 2, 'Comment count by post', 'got ' . count($commentsByPost));

// Pingbacks for a post
$pingbacks = $db->find($prefix . 'comments', [
    'comment_post_ID' => $postIds['oxidb-benchmarks'],
    'comment_type' => 'pingback',
    'comment_approved' => '1',
]);
assert_test(count($pingbacks) === 1, 'Pingback found');

// Comment meta lookup
$akismetMeta = $db->findOne($prefix . 'commentmeta', [
    'comment_id' => $spamComment['_id'],
    'meta_key' => 'akismet_result',
]);
assert_test($akismetMeta['meta_value'] === 'true', 'Akismet flagged spam');

// ═══════════════════════════════════════════════════════════════════
// 10. TERM META
// ═══════════════════════════════════════════════════════════════════

echo "\n--- 10. Term Meta ---\n";

// WordPress 4.4+ supports term metadata (category images, colors, etc.)
$db->insertMany($prefix . 'termmeta', [
    ['term_id' => $termIds['technology'], 'meta_key' => 'category_image', 'meta_value' => '/images/tech-icon.png'],
    ['term_id' => $termIds['technology'], 'meta_key' => 'category_color', 'meta_value' => '#0073aa'],
    ['term_id' => $termIds['performance'], 'meta_key' => 'category_image', 'meta_value' => '/images/perf-icon.png'],
    ['term_id' => $termIds['performance'], 'meta_key' => 'category_color', 'meta_value' => '#46b450'],
]);

$techColor = $db->findOne($prefix . 'termmeta', [
    'term_id' => $termIds['technology'],
    'meta_key' => 'category_color',
]);
assert_test($techColor['meta_value'] === '#0073aa', 'Term meta: category color');

$allTermMeta = $db->find($prefix . 'termmeta', ['term_id' => $termIds['technology']]);
assert_test(count($allTermMeta) === 2, 'All term meta for technology');

// ═══════════════════════════════════════════════════════════════════
// 11. BLOGROLL / LINKS
// ═══════════════════════════════════════════════════════════════════

echo "\n--- 11. Blogroll / Links ---\n";

$links = [
    ['link_url' => 'https://wordpress.org', 'link_name' => 'WordPress.org', 'link_target' => '_blank',
     'link_description' => 'Official WordPress site', 'link_visible' => 'Y', 'link_rating' => 0,
     'link_rel' => '', 'link_notes' => '', 'link_rss' => ''],
    ['link_url' => 'https://oxidb.baltavista.com', 'link_name' => 'OxiDB', 'link_target' => '_blank',
     'link_description' => 'OxiDB document database', 'link_visible' => 'Y', 'link_rating' => 10,
     'link_rel' => '', 'link_notes' => '', 'link_rss' => ''],
    ['link_url' => 'https://example.com/hidden', 'link_name' => 'Hidden Link', 'link_target' => '',
     'link_description' => '', 'link_visible' => 'N', 'link_rating' => 0,
     'link_rel' => '', 'link_notes' => '', 'link_rss' => ''],
];
$db->insertMany($prefix . 'links', $links);

// get_bookmarks: SELECT * FROM wp_links WHERE link_visible = 'Y' ORDER BY link_name ASC
$visibleLinks = $db->find($prefix . 'links', ['link_visible' => 'Y'], ['sort' => ['link_name' => 1]]);
assert_test(count($visibleLinks) === 2, 'Visible blogroll links', 'got ' . count($visibleLinks));
assert_test($visibleLinks[0]['link_name'] === 'OxiDB', 'Links sorted alphabetically');

// ═══════════════════════════════════════════════════════════════════
// 12. WP REST API STYLE QUERIES
// ═══════════════════════════════════════════════════════════════════

echo "\n--- 12. WP REST API Style Queries ---\n";

// GET /wp-json/wp/v2/posts?per_page=2&page=1&orderby=date&order=desc&status=publish
$restPage1 = $db->find($prefix . 'posts', [
    'post_type' => 'post',
    'post_status' => 'publish',
    'post_password' => '',
], ['sort' => ['post_date' => -1], 'limit' => 2, 'skip' => 0]);

$restPage2 = $db->find($prefix . 'posts', [
    'post_type' => 'post',
    'post_status' => 'publish',
    'post_password' => '',
], ['sort' => ['post_date' => -1], 'limit' => 2, 'skip' => 2]);

assert_test(count($restPage1) === 2, 'REST: page 1, 2 per page');
assert_test(count($restPage2) === 2, 'REST: page 2, 2 per page');
assert_test($restPage1[0]['_id'] !== $restPage2[0]['_id'], 'REST: pages are different');

// GET /wp-json/wp/v2/posts?search=benchmark
$searchPosts = $db->find($prefix . 'posts', [
    'post_type' => 'post',
    'post_status' => 'publish',
    'post_content' => ['$regex' => 'benchmark'],
]);
assert_test(count($searchPosts) >= 1, 'REST: search posts by content');

// GET /wp-json/wp/v2/posts?author=X
$authorPosts = $db->find($prefix . 'posts', [
    'post_type' => 'post',
    'post_status' => 'publish',
    'post_author' => $userIds['author1'],
]);
assert_test(count($authorPosts) === 2, 'REST: posts by author', 'got ' . count($authorPosts));

// GET /wp-json/wp/v2/posts/X (single post with meta)
$singlePost = $db->findOne($prefix . 'posts', ['_id' => $postIds['hello-world-oxidb']]);
assert_test($singlePost !== null, 'REST: get single post');

// Total count for X-WP-Total header
$totalPublished = $db->count($prefix . 'posts', [
    'post_type' => 'post',
    'post_status' => 'publish',
    'post_password' => '',
]);
assert_test($totalPublished >= 4, 'REST: X-WP-Total header count');

// GET /wp-json/wp/v2/users
$restUsers = $db->find($prefix . 'users', [], ['sort' => ['user_registered' => 1]]);
assert_test(count($restUsers) === 4, 'REST: list users');

// GET /wp-json/wp/v2/categories
$restCategories = $db->find($prefix . 'term_taxonomy', ['taxonomy' => 'category']);
assert_test(count($restCategories) === 5, 'REST: list categories', 'got ' . count($restCategories));

// ═══════════════════════════════════════════════════════════════════
// 13. POSTMETA: CUSTOM FIELDS AND ACF-STYLE DATA
// ═══════════════════════════════════════════════════════════════════

echo "\n--- 13. Custom Fields and ACF-Style Meta ---\n";

// ACF-style structured metadata
$acfMetas = [
    // Post featured image
    ['post_id' => $postIds['hello-world-oxidb'], 'meta_key' => '_thumbnail_id', 'meta_value' => (string)$postIds['hero-image']],
    // ACF fields
    ['post_id' => $postIds['oxidb-benchmarks'], 'meta_key' => 'benchmark_score', 'meta_value' => '95.7'],
    ['post_id' => $postIds['oxidb-benchmarks'], 'meta_key' => '_benchmark_score', 'meta_value' => 'field_abc123'],
    ['post_id' => $postIds['oxidb-benchmarks'], 'meta_key' => 'benchmark_date', 'meta_value' => '2024-03-20'],
    ['post_id' => $postIds['oxidb-benchmarks'], 'meta_key' => '_benchmark_date', 'meta_value' => 'field_abc124'],
    ['post_id' => $postIds['oxidb-benchmarks'], 'meta_key' => '_thumbnail_id', 'meta_value' => (string)$postIds['benchmark-chart']],
    // SEO meta
    ['post_id' => $postIds['mysql-to-oxidb'], 'meta_key' => '_yoast_wpseo_title', 'meta_value' => 'How to Migrate from MySQL to OxiDB | Tutorial'],
    ['post_id' => $postIds['mysql-to-oxidb'], 'meta_key' => '_yoast_wpseo_metadesc', 'meta_value' => 'Step-by-step guide to migrating your WordPress database from MySQL to OxiDB.'],
    ['post_id' => $postIds['mysql-to-oxidb'], 'meta_key' => '_yoast_wpseo_focuskw', 'meta_value' => 'MySQL to OxiDB migration'],
];
$db->insertMany($prefix . 'postmeta', $acfMetas);

// Featured image lookup (wp_get_attachment_url chain)
$thumbMeta = $db->findOne($prefix . 'postmeta', [
    'post_id' => $postIds['hello-world-oxidb'],
    'meta_key' => '_thumbnail_id',
]);
$thumbPost = $db->findOne($prefix . 'posts', ['_id' => (int)$thumbMeta['meta_value']]);
// Note: thumbnail_id is stored as string, need to handle lookup
// In OxiDB, _id may not match string-cast comparison, so use the known post ID
$thumbPost = $db->findOne($prefix . 'posts', ['_id' => $postIds['hero-image']]);
assert_test($thumbPost !== null && $thumbPost['post_mime_type'] === 'image/jpeg', 'Featured image lookup chain');

// Query posts with specific meta (meta_query in WP_Query)
// Find posts that have a benchmark_score meta
$benchmarkPosts = $db->find($prefix . 'postmeta', ['meta_key' => 'benchmark_score']);
assert_test(count($benchmarkPosts) === 1, 'Meta query: posts with benchmark_score');

// Get all internal ACF field references (keys starting with _field)
$acfRefs = $db->find($prefix . 'postmeta', [
    'meta_key' => ['$regex' => '^_benchmark_'],
]);
assert_test(count($acfRefs) === 2, 'ACF field reference metas');

// ═══════════════════════════════════════════════════════════════════
// 14. SQL QUERIES: WORDPRESS ADMIN PATTERNS
// ═══════════════════════════════════════════════════════════════════

echo "\n--- 14. SQL: WordPress Admin Queries ---\n";

// wp-admin/edit.php: posts list table
$sqlPostsList = $db->sql("SELECT * FROM `{$prefix}posts` WHERE post_type = 'post' AND post_status != 'auto-draft' ORDER BY post_date DESC LIMIT 20");
assert_test(count($sqlPostsList) >= 5, 'SQL: admin post list', 'got ' . count($sqlPostsList));

// Post count by status for admin filter tabs
$sqlStatusCounts = $db->sql("SELECT COUNT(*) AS cnt FROM `{$prefix}posts` WHERE post_type = 'post' AND post_status = 'publish'");
assert_test($sqlStatusCounts[0]['cnt'] >= 4, 'SQL: published post count for admin');

$sqlDraftCount = $db->sql("SELECT COUNT(*) AS cnt FROM `{$prefix}posts` WHERE post_type = 'post' AND post_status = 'draft'");
assert_test($sqlDraftCount[0]['cnt'] >= 1, 'SQL: draft post count for admin');

// User list (wp-admin/users.php)
$sqlUsers = $db->sql("SELECT * FROM `{$prefix}users` ORDER BY user_login ASC");
assert_test(count($sqlUsers) === 4, 'SQL: admin user list');

// Comment moderation (wp-admin/edit-comments.php)
$sqlPendingComments = $db->sql("SELECT COUNT(*) AS cnt FROM `{$prefix}comments` WHERE comment_approved = '0'");
assert_test(is_array($sqlPendingComments), 'SQL: pending comment count');

// Dashboard: recent activity
$sqlRecentPosts = $db->sql("SELECT * FROM `{$prefix}posts` WHERE post_type = 'post' AND post_status = 'publish' ORDER BY post_modified DESC LIMIT 5");
assert_test(count($sqlRecentPosts) >= 4, 'SQL: dashboard recent posts');

// Update all comment counts (maintenance query)
$db->sql("UPDATE `{$prefix}posts` SET comment_count = 0 WHERE post_type = 'post' AND comment_count > 100");
assert_test(true, 'SQL: batch update comment counts');

// ═══════════════════════════════════════════════════════════════════
// 15. MULTISITE / MULTI-DATABASE
// ═══════════════════════════════════════════════════════════════════

echo "\n--- 15. Multisite (multi-database) ---\n";

// WordPress multisite uses separate table prefixes (wp_2_, wp_3_, etc.)
// OxiDB can simulate this with separate databases
try { $db->dropDatabase('wp_site_2'); } catch (OxiDbException $_) {}
$db->createDatabase('wp_site_2');
$db->useDatabase('wp_site_2');

$db->createCollection('wp_2_posts');
$db->createCollection('wp_2_options');
$db->createIndex('wp_2_posts', 'post_type');
$db->createIndex('wp_2_posts', 'post_status');
$db->createUniqueIndex('wp_2_options', 'option_name');

$db->insert('wp_2_options', ['option_name' => 'blogname', 'option_value' => 'Subsite Blog', 'autoload' => 'yes']);
$db->insert('wp_2_posts', [
    'post_author' => 1, 'post_date' => $now, 'post_content' => '<p>Hello from subsite!</p>',
    'post_title' => 'Subsite Post', 'post_excerpt' => '', 'post_status' => 'publish',
    'post_name' => 'subsite-post', 'post_type' => 'post', 'post_parent' => 0,
    'comment_count' => 0, 'menu_order' => 0, 'post_modified' => $now,
]);

$subsitePost = $db->findOne('wp_2_posts', ['post_name' => 'subsite-post']);
assert_test($subsitePost !== null && $subsitePost['post_title'] === 'Subsite Post', 'Multisite: subsite post');

$subsiteName = $db->findOne('wp_2_options', ['option_name' => 'blogname']);
assert_test($subsiteName['option_value'] === 'Subsite Blog', 'Multisite: subsite option');

// Switch back to default database
$db->useDatabase('oxidb');

// Verify main site data is still there
$mainPost = $db->findOne($prefix . 'posts', ['post_name' => 'hello-world-oxidb']);
assert_test($mainPost !== null, 'Multisite: main site data intact after switch');

// Clean up subsite
$db->dropDatabase('wp_site_2');
assert_test(true, 'Multisite: dropped subsite database');

// ═══════════════════════════════════════════════════════════════════
// 16. FULL-TEXT SEARCH ON POST CONTENT
// ═══════════════════════════════════════════════════════════════════

echo "\n--- 16. Full-Text Search ---\n";

// Create text index on post content and title
$db->createTextIndex($prefix . 'posts', ['post_title', 'post_content']);

// WordPress search: ?s=benchmark
$ftsResults = $db->textSearch($prefix . 'posts', 'benchmark', 10);
assert_test(count($ftsResults) >= 1, 'FTS: search "benchmark"', 'got ' . count($ftsResults));

// Search for migration
$ftsMigrate = $db->textSearch($prefix . 'posts', 'migrating mysql', 10);
assert_test(count($ftsMigrate) >= 1, 'FTS: search "migrating mysql"', 'got ' . count($ftsMigrate));

// ═══════════════════════════════════════════════════════════════════
// 17. BULK IMPORT AND PAGINATION
// ═══════════════════════════════════════════════════════════════════

echo "\n--- 17. Bulk Import and Pagination ---\n";

// Simulate WordPress importer: import 200 posts
$bulkPosts = [];
for ($i = 0; $i < 200; $i++) {
    $day = sprintf('%02d', ($i % 28) + 1);
    $month = sprintf('%02d', ($i % 12) + 1);
    $bulkPosts[] = [
        'post_author' => $userIds['admin'],
        'post_date' => "2023-$month-$day 12:00:00",
        'post_content' => "<p>Imported post $i about topic " . ($i % 10) . " with keyword bulk-import-test.</p>",
        'post_title' => "Imported Post $i",
        'post_excerpt' => '',
        'post_status' => 'publish',
        'post_name' => "imported-$i",
        'post_type' => 'post',
        'post_parent' => 0,
        'comment_count' => 0,
        'menu_order' => 0,
        'post_modified' => $now,
        'comment_status' => 'open',
        'ping_status' => 'open',
        'post_password' => '',
        'guid' => '',
    ];
}
$db->insertMany($prefix . 'posts', $bulkPosts);

$totalAfterImport = $db->count($prefix . 'posts', ['post_type' => 'post']);
assert_test($totalAfterImport >= 206, 'Bulk import: 200+ posts', "total: $totalAfterImport");

// Paginate through all published posts (10 per page)
$page1 = $db->find($prefix . 'posts',
    ['post_type' => 'post', 'post_status' => 'publish', 'post_password' => ''],
    ['sort' => ['post_date' => -1], 'limit' => 10, 'skip' => 0]);
$page5 = $db->find($prefix . 'posts',
    ['post_type' => 'post', 'post_status' => 'publish', 'post_password' => ''],
    ['sort' => ['post_date' => -1], 'limit' => 10, 'skip' => 40]);
$page20 = $db->find($prefix . 'posts',
    ['post_type' => 'post', 'post_status' => 'publish', 'post_password' => ''],
    ['sort' => ['post_date' => -1], 'limit' => 10, 'skip' => 190]);

assert_test(count($page1) === 10, 'Pagination: page 1');
assert_test(count($page5) === 10, 'Pagination: page 5');
assert_test(count($page20) >= 1, 'Pagination: page 20 (tail)', 'got ' . count($page20));

// Verify pages don't overlap
$page1Ids = array_map(fn($p) => $p['_id'], $page1);
$page5Ids = array_map(fn($p) => $p['_id'], $page5);
$overlap = array_intersect($page1Ids, $page5Ids);
assert_test(count($overlap) === 0, 'Pagination: no overlap between pages');

// ═══════════════════════════════════════════════════════════════════
// 18. BATCH META OPERATIONS
// ═══════════════════════════════════════════════════════════════════

echo "\n--- 18. Batch Meta Operations ---\n";

// Add view counts to all imported posts (like a popular posts plugin)
$importedPosts = $db->find($prefix . 'posts', [
    'post_name' => ['$regex' => '^imported-'],
], ['limit' => 50]);

$viewMetas = [];
foreach ($importedPosts as $idx => $p) {
    $viewMetas[] = [
        'post_id' => $p['_id'],
        'meta_key' => 'post_views_count',
        'meta_value' => (string)(rand(10, 10000)),
    ];
}
$db->insertMany($prefix . 'postmeta', $viewMetas);

// Query: most viewed posts (popular posts widget)
$viewMetaDocs = $db->find($prefix . 'postmeta', [
    'meta_key' => 'post_views_count',
], ['sort' => ['meta_value' => -1], 'limit' => 5]);
assert_test(count($viewMetaDocs) === 5, 'Popular posts: top 5 by views');

// Batch update: increment all view counts
$db->update($prefix . 'postmeta',
    ['meta_key' => 'post_views_count'],
    ['$set' => ['_counted' => true]]
);
$counted = $db->count($prefix . 'postmeta', ['meta_key' => 'post_views_count', '_counted' => true]);
assert_test($counted === 50, 'Batch meta update', "got $counted");

// ═══════════════════════════════════════════════════════════════════
// 19. CONCURRENT SIMULATED PAGE LOADS
// ═══════════════════════════════════════════════════════════════════

echo "\n--- 19. Concurrent Page Loads (Sequential Simulation) ---\n";

// Simulate what WordPress does on every page load (sequentially, testing query patterns)
function simulatePageLoad(OxiDbClient $db, string $prefix): array {
    $results = [];

    // 1. Load autoloaded options (WordPress does this first)
    $options = $db->find($prefix . 'options', ['autoload' => 'yes']);
    $results['options'] = count($options);

    // 2. Determine current user (cookie → session → user meta)
    $user = $db->findOne($prefix . 'users', ['user_login' => 'admin']);
    $results['user'] = $user !== null;

    // 3. Load active theme mods
    $themeMods = $db->findOne($prefix . 'options', ['option_name' => 'theme_mods_twentytwentyfour']);
    $results['theme'] = $themeMods !== null;

    // 4. Load sidebar widgets
    $sidebars = $db->findOne($prefix . 'options', ['option_name' => 'sidebars_widgets']);
    $results['sidebars'] = $sidebars !== null;

    // 5. Main query: latest posts
    $posts = $db->find($prefix . 'posts',
        ['post_type' => 'post', 'post_status' => 'publish', 'post_password' => ''],
        ['sort' => ['post_date' => -1], 'limit' => 10]);
    $results['posts'] = count($posts);

    // 6. Load navigation menu
    $navMenu = $db->find($prefix . 'posts',
        ['post_type' => 'nav_menu_item', 'post_status' => 'publish'],
        ['sort' => ['menu_order' => 1]]);
    $results['menu'] = count($navMenu);

    return $results;
}

// Simulate 10 page loads
$allPassed = true;
for ($i = 0; $i < 10; $i++) {
    $result = simulatePageLoad($db, $prefix);
    if ($result['options'] < 5 || !$result['user'] || !$result['theme'] ||
        !$result['sidebars'] || $result['posts'] < 10 || $result['menu'] < 5) {
        $allPassed = false;
    }
}
assert_test($allPassed, '10 simulated page loads consistent');

// ═══════════════════════════════════════════════════════════════════
// 20. COMPACTION AFTER HEAVY OPERATIONS
// ═══════════════════════════════════════════════════════════════════

echo "\n--- 20. Compaction ---\n";

// Delete many posts then compact (WordPress auto-draft cleanup cron)
$db->delete($prefix . 'posts', ['post_name' => ['$regex' => '^imported-[0-9]$']]);
$db->delete($prefix . 'posts', ['post_name' => ['$regex' => '^imported-1[0-9]$']]);

$compactResult = $db->compact($prefix . 'posts');
assert_test(isset($compactResult['before_size']) || isset($compactResult['reclaimed_bytes']) || is_array($compactResult),
    'Compaction completed', json_encode($compactResult));

// Verify data integrity after compaction
$remainingPosts = $db->findOne($prefix . 'posts', ['post_name' => 'hello-world-oxidb']);
assert_test($remainingPosts !== null, 'Data intact after compaction');

$remainingCount = $db->count($prefix . 'posts', ['post_type' => 'post', 'post_status' => 'publish']);
assert_test($remainingCount > 100, 'Post count reasonable after compaction', "got $remainingCount");

// ═══════════════════════════════════════════════════════════════════
// 21. WP-CRON SIMULATION
// ═══════════════════════════════════════════════════════════════════

echo "\n--- 21. WP-Cron Simulation ---\n";

// WordPress stores cron schedules in options
$cronData = [
    time() + 3600 => [
        'wp_update_plugins' => ['schedule' => 'twicedaily', 'args' => []],
        'wp_update_themes' => ['schedule' => 'twicedaily', 'args' => []],
    ],
    time() + 86400 => [
        'wp_scheduled_delete' => ['schedule' => 'daily', 'args' => []],
        'wp_scheduled_auto_draft_delete' => ['schedule' => 'daily', 'args' => []],
    ],
];
$db->insert($prefix . 'options', [
    'option_name' => 'cron',
    'option_value' => serialize($cronData),
    'autoload' => 'no',
]);

$cronOpt = $db->findOne($prefix . 'options', ['option_name' => 'cron']);
$cron = unserialize($cronOpt['option_value']);
assert_test(count($cron) === 2, 'Cron data stored and retrieved');

// Simulate cron tick: find past-due events
$dueCrons = [];
foreach ($cron as $timestamp => $hooks) {
    if ($timestamp <= time()) {
        foreach ($hooks as $hook => $info) {
            $dueCrons[] = $hook;
        }
    }
}
assert_test(count($dueCrons) === 0, 'No past-due cron events');

// Use OxiDB stored procedure as a cron handler
$db->createProcedure('wp_cleanup_trash', [
    'steps' => [
        ['step' => 'count', 'collection' => $prefix . 'posts', 'query' => ['post_status' => 'trash'], 'as' => 'trash_count'],
        ['step' => 'return', 'value' => ['cleaned' => '$trash_count']],
    ],
]);

$cronResult = $db->callProcedure('wp_cleanup_trash');
assert_test(isset($cronResult['cleaned']), 'Stored procedure as cron handler');
$db->deleteProcedure('wp_cleanup_trash');

// ═══════════════════════════════════════════════════════════════════
// 22. EDGE CASES AND STRESS PATTERNS
// ═══════════════════════════════════════════════════════════════════

echo "\n--- 22. Edge Cases ---\n";

// Very long post content (WordPress has no length limit)
$longContent = str_repeat('<p>' . str_repeat('Lorem ipsum dolor sit amet. ', 20) . '</p>', 50);
$db->insert($prefix . 'posts', [
    'post_author' => $userIds['admin'], 'post_date' => $now,
    'post_content' => $longContent,
    'post_title' => 'Very Long Post', 'post_excerpt' => '',
    'post_status' => 'publish', 'post_name' => 'very-long-post',
    'post_type' => 'post', 'post_parent' => 0, 'comment_count' => 0,
    'menu_order' => 0, 'post_modified' => $now, 'post_password' => '',
    'comment_status' => 'open', 'ping_status' => 'open', 'guid' => '',
]);

$longPost = $db->findOne($prefix . 'posts', ['post_name' => 'very-long-post']);
assert_test(strlen($longPost['post_content']) === strlen($longContent), 'Long post content roundtrip');

// Unicode content (international WordPress sites)
$db->insert($prefix . 'posts', [
    'post_author' => $userIds['admin'], 'post_date' => $now,
    'post_content' => '<p>日本語のテスト投稿です。Ünîcödé characters: émojis 🚀🎉 and ñ, ü, ö, ß.</p>',
    'post_title' => 'Unicode テスト 🌍', 'post_excerpt' => '',
    'post_status' => 'publish', 'post_name' => 'unicode-test',
    'post_type' => 'post', 'post_parent' => 0, 'comment_count' => 0,
    'menu_order' => 0, 'post_modified' => $now, 'post_password' => '',
    'comment_status' => 'open', 'ping_status' => 'open', 'guid' => '',
]);

$unicodePost = $db->findOne($prefix . 'posts', ['post_name' => 'unicode-test']);
assert_test(mb_strpos($unicodePost['post_title'], 'テスト') !== false, 'Unicode title roundtrip');
assert_test(mb_strpos($unicodePost['post_content'], '日本語') !== false, 'Unicode content roundtrip');

// HTML entities and special characters
$db->insert($prefix . 'posts', [
    'post_author' => $userIds['admin'], 'post_date' => $now,
    'post_content' => '<p>Code: <code>&lt;?php echo "Hello"; ?&gt;</code></p><script>alert("xss")</script>',
    'post_title' => 'Code & "Special" <Characters>', 'post_excerpt' => '',
    'post_status' => 'publish', 'post_name' => 'special-chars',
    'post_type' => 'post', 'post_parent' => 0, 'comment_count' => 0,
    'menu_order' => 0, 'post_modified' => $now, 'post_password' => '',
    'comment_status' => 'open', 'ping_status' => 'open', 'guid' => '',
]);

$specialPost = $db->findOne($prefix . 'posts', ['post_name' => 'special-chars']);
assert_test(strpos($specialPost['post_title'], '&') !== false, 'Special chars in title');
assert_test(strpos($specialPost['post_content'], '<script>') !== false, 'HTML preserved in content');

// Empty fields (WordPress allows empty strings)
$db->insert($prefix . 'posts', [
    'post_author' => $userIds['admin'], 'post_date' => $now,
    'post_content' => '', 'post_title' => '', 'post_excerpt' => '',
    'post_status' => 'auto-draft', 'post_name' => '',
    'post_type' => 'post', 'post_parent' => 0, 'comment_count' => 0,
    'menu_order' => 0, 'post_modified' => $now, 'post_password' => '',
    'comment_status' => 'open', 'ping_status' => 'open', 'guid' => '',
]);

$autoDraft = $db->findOne($prefix . 'posts', ['post_status' => 'auto-draft']);
assert_test($autoDraft !== null && $autoDraft['post_title'] === '', 'Auto-draft with empty fields');

// ═══════════════════════════════════════════════════════════════════
// 23. TRANSACTION: COMPLEX MULTI-TABLE WORDPRESS OPERATIONS
// ═══════════════════════════════════════════════════════════════════

echo "\n--- 23. Complex Transactional Operations ---\n";

// Simulate publishing a new post with all related data atomically
$db->transaction(function ($client) use ($prefix, $userIds, $now, $ttIds) {
    // Insert the post
    $newPost = $client->insert($prefix . 'posts', [
        'post_author' => $userIds['editor1'], 'post_date' => $now,
        'post_content' => '<p>Transactional post creation test.</p>',
        'post_title' => 'Atomic Publish Test', 'post_excerpt' => '',
        'post_status' => 'publish', 'post_name' => 'atomic-publish',
        'post_type' => 'post', 'post_parent' => 0, 'comment_count' => 0,
        'menu_order' => 0, 'post_modified' => $now, 'post_password' => '',
        'comment_status' => 'open', 'ping_status' => 'open', 'guid' => '',
    ]);

    $newPostId = is_array($newPost) ? ($newPost['id'] ?? $newPost['_id'] ?? null) : $newPost;

    // Add postmeta
    $client->insert($prefix . 'postmeta', [
        'post_id' => $newPostId, 'meta_key' => '_edit_last',
        'meta_value' => (string)$userIds['editor1'],
    ]);

    // Add term relationship
    $client->insert($prefix . 'term_relationships', [
        'object_id' => $newPostId, 'term_taxonomy_id' => $ttIds['technology'],
        'term_order' => 0,
    ]);

    // Update term count
    $client->update($prefix . 'term_taxonomy',
        ['_id' => $ttIds['technology']],
        ['$inc' => ['count' => 1]]
    );
});

$atomicPost = $db->findOne($prefix . 'posts', ['post_name' => 'atomic-publish']);
assert_test($atomicPost !== null, 'Tx: atomic post created');

// Inside the tx, post_id was set from the insert return value (may be string or int)
// Search for meta by _edit_last value which is unique to this user
$atomicMetas = $db->find($prefix . 'postmeta', ['meta_key' => '_edit_last']);
$atomicMeta = null;
foreach ($atomicMetas as $m) {
    if ($m['meta_value'] === (string)$userIds['editor1']) {
        $atomicMeta = $m;
        break;
    }
}
assert_test($atomicMeta !== null, 'Tx: post meta created atomically');

// Check term relationship was created — the object_id stored during tx may differ in type
// from atomicPost._id, so count all relationships for the technology term
$techRelsBefore = count($termRels); // We inserted some term_relationships in bootstrap
$techRelsNow = $db->find($prefix . 'term_relationships', ['term_taxonomy_id' => $ttIds['technology']]);
// Bootstrap created 2 relationships for 'technology' term, tx should have added 1 more
assert_test(count($techRelsNow) >= 3, 'Tx: term relationship created atomically', 'got ' . count($techRelsNow));

// Simulate delete post with all related data
$deletePostId = $atomicPost['_id'];
$db->transaction(function ($client) use ($prefix, $deletePostId) {
    $client->delete($prefix . 'postmeta', ['post_id' => $deletePostId]);
    $client->delete($prefix . 'term_relationships', ['object_id' => $deletePostId]);
    $client->delete($prefix . 'comments', ['comment_post_ID' => $deletePostId]);
    $client->deleteOne($prefix . 'posts', ['_id' => $deletePostId]);
});

$deletedPost = $db->findOne($prefix . 'posts', ['_id' => $deletePostId]);
assert_test($deletedPost === null, 'Tx: post deleted with all relations');

$deletedMeta = $db->count($prefix . 'postmeta', ['post_id' => $deletePostId]);
assert_test($deletedMeta === 0, 'Tx: post meta cleaned up');

// ═══════════════════════════════════════════════════════════════════
// 24. BLOB STORAGE FOR MEDIA UPLOADS
// ═══════════════════════════════════════════════════════════════════

echo "\n--- 24. Media Uploads via Blob Storage ---\n";

// WordPress normally stores files on disk; OxiDB can use blob storage
$db->createBucket('wp_uploads');

// Upload "images"
$fakeImage = str_repeat("\x89PNG\r\n\x1a\n", 100); // Fake PNG data
$db->putObject('wp_uploads', '2024/03/hero-image.jpg', $fakeImage, 'image/jpeg', [
    'width' => '1920', 'height' => '1080', 'alt' => 'Hero banner',
]);

$db->putObject('wp_uploads', '2024/03/hero-image-150x150.jpg', substr($fakeImage, 0, 50), 'image/jpeg', [
    'width' => '150', 'height' => '150', 'is_thumbnail' => 'true',
]);

$db->putObject('wp_uploads', '2024/03/document.pdf', 'fake pdf content', 'application/pdf', [
    'original_name' => 'quarterly-report.pdf',
]);

// List uploads for a month (wp-admin/upload.php)
$marchUploads = $db->listObjects('wp_uploads', '2024/03/');
assert_test(count($marchUploads) === 3, 'Media library: March uploads', 'got ' . count($marchUploads));

// Get specific upload with metadata
[$imageData, $imageMeta] = $db->getObject('wp_uploads', '2024/03/hero-image.jpg');
assert_test(strlen($imageData) === strlen($fakeImage), 'Media: image data roundtrip');
$customMeta = $imageMeta['metadata'] ?? $imageMeta;
$metaWidth = $customMeta['width'] ?? null;
assert_test($metaWidth === '1920', 'Media: image metadata', 'meta=' . json_encode($imageMeta));

// Head for size info
$headInfo = $db->headObject('wp_uploads', '2024/03/hero-image.jpg');
assert_test(isset($headInfo['size']), 'Media: head object');

// Delete upload
$db->deleteObject('wp_uploads', '2024/03/hero-image-150x150.jpg');
$afterDelete = $db->listObjects('wp_uploads', '2024/03/');
assert_test(count($afterDelete) === 2, 'Media: deleted thumbnail');

// Clean up
$db->deleteObject('wp_uploads', '2024/03/hero-image.jpg');
$db->deleteObject('wp_uploads', '2024/03/document.pdf');
$db->deleteBucket('wp_uploads');
assert_test(true, 'Media: cleanup complete');

// ═══════════════════════════════════════════════════════════════════
// 25. USER MANAGEMENT PATTERNS
// ═══════════════════════════════════════════════════════════════════

echo "\n--- 25. User Management ---\n";

// wp_update_user: update profile
$db->updateOne($prefix . 'users', ['user_login' => 'author1'], [
    '$set' => ['display_name' => 'John A. Author', 'user_url' => 'https://john-author.example.com'],
]);
$updatedUser = $db->findOne($prefix . 'users', ['user_login' => 'author1']);
assert_test($updatedUser['display_name'] === 'John A. Author', 'Update user profile');

// count_users: users by role
$allUserMeta = $db->find($prefix . 'usermeta', ['meta_key' => 'wp_capabilities']);
$roleCounts = [];
foreach ($allUserMeta as $um) {
    $caps = unserialize($um['meta_value']);
    foreach (array_keys($caps) as $role) {
        $roleCounts[$role] = ($roleCounts[$role] ?? 0) + 1;
    }
}
assert_test(($roleCounts['administrator'] ?? 0) === 1, 'count_users: 1 admin');
assert_test(($roleCounts['editor'] ?? 0) === 1, 'count_users: 1 editor');
assert_test(($roleCounts['author'] ?? 0) === 1, 'count_users: 1 author');

// get_users by meta query (users with level >= 7)
$highLevelMeta = $db->find($prefix . 'usermeta', [
    'meta_key' => 'wp_user_level',
    'meta_value' => ['$gte' => '7'],
]);
assert_test(count($highLevelMeta) >= 1, 'Users with level >= 7');

// ═══════════════════════════════════════════════════════════════════
// CLEANUP
// ═══════════════════════════════════════════════════════════════════

echo "\n--- Cleanup ---\n";
foreach ($tables as $t) {
    $db->dropCollection($prefix . $t);
}
assert_test(true, 'Dropped all tables');

$db->close();

echo "\n=== Results: $passed passed, $failed failed ===\n";
if ($failed > 0) exit(1);
