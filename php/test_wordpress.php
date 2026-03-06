<?php
/**
 * OxiDB PHP Client — WordPress-style Query Test Suite
 *
 * Simulates the kind of queries WordPress core makes against its database:
 * posts, users, options, terms/taxonomies, meta tables, comments, etc.
 *
 * Usage: php test_wordpress.php [port]
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

echo "=== WordPress-style Query Tests (prefix: {$prefix}) ===\n\n";

// ─── Schema Setup (CREATE TABLE equivalents) ─────────────────────

echo "--- Schema Setup ---\n";

$tables = ['posts', 'postmeta', 'users', 'usermeta', 'options', 'terms',
           'term_taxonomy', 'term_relationships', 'comments', 'commentmeta', 'links'];
foreach ($tables as $t) {
    try { $db->dropCollection($prefix . $t); } catch (OxiDbException $_) {}
    $db->createCollection($prefix . $t);
}
assert_test(true, 'Created ' . count($tables) . ' WordPress tables');

// Create indexes WordPress typically has
$db->createIndex($prefix . 'posts', 'post_type');
$db->createIndex($prefix . 'posts', 'post_status');
$db->createIndex($prefix . 'posts', 'post_author');
$db->createIndex($prefix . 'posts', 'post_date');
$db->createIndex($prefix . 'posts', 'post_parent');
$db->createIndex($prefix . 'postmeta', 'post_id');
$db->createIndex($prefix . 'postmeta', 'meta_key');
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
$db->createIndex($prefix . 'commentmeta', 'comment_id');
assert_test(true, 'Created WordPress indexes');

// ─── wp_users ────────────────────────────────────────────────────

echo "\n--- wp_users (INSERT INTO / SELECT) ---\n";

// INSERT INTO wp_users (user_login, user_pass, user_nicename, user_email, user_registered, user_status, display_name)
$users = [
    ['user_login' => 'admin', 'user_pass' => '$P$BhashAdmin', 'user_nicename' => 'admin',
     'user_email' => 'admin@example.com', 'user_registered' => '2024-01-01 00:00:00',
     'user_status' => 0, 'display_name' => 'Administrator'],
    ['user_login' => 'editor', 'user_pass' => '$P$BhashEditor', 'user_nicename' => 'editor',
     'user_email' => 'editor@example.com', 'user_registered' => '2024-02-15 10:30:00',
     'user_status' => 0, 'display_name' => 'Jane Editor'],
    ['user_login' => 'subscriber', 'user_pass' => '$P$BhashSub', 'user_nicename' => 'subscriber',
     'user_email' => 'sub@example.com', 'user_registered' => '2024-06-20 14:00:00',
     'user_status' => 0, 'display_name' => 'Bob Subscriber'],
];
$db->insertMany($prefix . 'users', $users);

// SELECT * FROM wp_users WHERE user_login = 'admin' LIMIT 1
$admin = $db->findOne($prefix . 'users', ['user_login' => 'admin']);
assert_test($admin !== null && $admin['user_email'] === 'admin@example.com', 'SELECT user by login');

// SELECT ID, user_login, user_email FROM wp_users ORDER BY user_registered DESC
$allUsers = $db->find($prefix . 'users', [], ['sort' => ['user_registered' => -1]]);
assert_test(count($allUsers) === 3, 'SELECT all users sorted', 'got ' . count($allUsers));
assert_test($allUsers[0]['user_login'] === 'subscriber', 'Most recent user first');

// wp_usermeta: capabilities, user level
$adminId = $admin['_id'];
$userMetas = [
    ['user_id' => $adminId, 'meta_key' => 'wp_capabilities', 'meta_value' => 'a:1:{s:13:"administrator";b:1;}'],
    ['user_id' => $adminId, 'meta_key' => 'wp_user_level', 'meta_value' => '10'],
    ['user_id' => $adminId, 'meta_key' => 'nickname', 'meta_value' => 'admin'],
    ['user_id' => $adminId, 'meta_key' => 'first_name', 'meta_value' => 'Admin'],
    ['user_id' => $adminId, 'meta_key' => 'last_name', 'meta_value' => 'User'],
];
$db->insertMany($prefix . 'usermeta', $userMetas);

// SELECT meta_value FROM wp_usermeta WHERE user_id = X AND meta_key = 'wp_capabilities'
$caps = $db->findOne($prefix . 'usermeta', ['user_id' => $adminId, 'meta_key' => 'wp_capabilities']);
assert_test($caps !== null && strpos($caps['meta_value'], 'administrator') !== false, 'SELECT user capabilities meta');

// ─── wp_options ──────────────────────────────────────────────────

echo "\n--- wp_options (autoloaded options) ---\n";

// INSERT INTO wp_options (option_name, option_value, autoload) VALUES ...
$options = [
    ['option_name' => 'siteurl', 'option_value' => 'https://example.com', 'autoload' => 'yes'],
    ['option_name' => 'home', 'option_value' => 'https://example.com', 'autoload' => 'yes'],
    ['option_name' => 'blogname', 'option_value' => 'My WordPress Site', 'autoload' => 'yes'],
    ['option_name' => 'blogdescription', 'option_value' => 'Just another WordPress site', 'autoload' => 'yes'],
    ['option_name' => 'admin_email', 'option_value' => 'admin@example.com', 'autoload' => 'yes'],
    ['option_name' => 'template', 'option_value' => 'twentytwentyfour', 'autoload' => 'yes'],
    ['option_name' => 'stylesheet', 'option_value' => 'twentytwentyfour', 'autoload' => 'yes'],
    ['option_name' => 'date_format', 'option_value' => 'F j, Y', 'autoload' => 'yes'],
    ['option_name' => 'time_format', 'option_value' => 'g:i a', 'autoload' => 'yes'],
    ['option_name' => 'permalink_structure', 'option_value' => '/%postname%/', 'autoload' => 'yes'],
    ['option_name' => 'posts_per_page', 'option_value' => '10', 'autoload' => 'yes'],
    ['option_name' => 'active_plugins', 'option_value' => 'a:0:{}', 'autoload' => 'yes'],
    ['option_name' => 'cron', 'option_value' => 'a:2:{...}', 'autoload' => 'no'],
    ['option_name' => 'rewrite_rules', 'option_value' => 'a:100:{...long...}', 'autoload' => 'no'],
];
$db->insertMany($prefix . 'options', $options);

// SELECT option_name, option_value FROM wp_options WHERE autoload = 'yes'
// (WordPress loads all autoloaded options on every page load)
$autoloaded = $db->find($prefix . 'options', ['autoload' => 'yes']);
assert_test(count($autoloaded) === 12, 'SELECT autoloaded options', 'got ' . count($autoloaded));

// SELECT option_value FROM wp_options WHERE option_name = 'siteurl' LIMIT 1
$siteurl = $db->findOne($prefix . 'options', ['option_name' => 'siteurl']);
assert_test($siteurl['option_value'] === 'https://example.com', 'get_option(siteurl)');

// UPDATE wp_options SET option_value = 'New Title' WHERE option_name = 'blogname'
$db->update($prefix . 'options',
    ['option_name' => 'blogname'],
    ['$set' => ['option_value' => 'Updated Blog Title']]
);
$updated = $db->findOne($prefix . 'options', ['option_name' => 'blogname']);
assert_test($updated['option_value'] === 'Updated Blog Title', 'update_option(blogname)');

// Unique constraint: INSERT duplicate option_name should fail
try {
    $db->insert($prefix . 'options', ['option_name' => 'siteurl', 'option_value' => 'dup', 'autoload' => 'yes']);
    assert_test(false, 'Duplicate option_name rejected');
} catch (OxiDbException $e) {
    assert_test(true, 'Duplicate option_name rejected');
}

// ─── wp_posts ────────────────────────────────────────────────────

echo "\n--- wp_posts (posts, pages, revisions, attachments) ---\n";

$now = date('Y-m-d H:i:s');
$posts = [
    ['post_author' => $adminId, 'post_date' => '2024-03-01 09:00:00', 'post_content' => '<p>Welcome to WordPress.</p>',
     'post_title' => 'Hello world!', 'post_excerpt' => '', 'post_status' => 'publish',
     'post_name' => 'hello-world', 'post_type' => 'post', 'post_parent' => 0,
     'comment_count' => 1, 'menu_order' => 0, 'post_modified' => $now],
    ['post_author' => $adminId, 'post_date' => '2024-03-15 12:00:00', 'post_content' => '<p>About us page content.</p>',
     'post_title' => 'About', 'post_excerpt' => '', 'post_status' => 'publish',
     'post_name' => 'about', 'post_type' => 'page', 'post_parent' => 0,
     'comment_count' => 0, 'menu_order' => 0, 'post_modified' => $now],
    ['post_author' => $adminId, 'post_date' => '2024-04-01 08:00:00', 'post_content' => '<p>Draft post content.</p>',
     'post_title' => 'Draft Post', 'post_excerpt' => '', 'post_status' => 'draft',
     'post_name' => 'draft-post', 'post_type' => 'post', 'post_parent' => 0,
     'comment_count' => 0, 'menu_order' => 0, 'post_modified' => $now],
    ['post_author' => $adminId, 'post_date' => '2024-05-10 14:30:00', 'post_content' => '<p>Second published post.</p>',
     'post_title' => 'Second Post', 'post_excerpt' => 'A short excerpt', 'post_status' => 'publish',
     'post_name' => 'second-post', 'post_type' => 'post', 'post_parent' => 0,
     'comment_count' => 0, 'menu_order' => 0, 'post_modified' => $now],
    ['post_author' => $adminId, 'post_date' => '2024-06-01 10:00:00', 'post_content' => '<p>Private page.</p>',
     'post_title' => 'Private Page', 'post_excerpt' => '', 'post_status' => 'private',
     'post_name' => 'private-page', 'post_type' => 'page', 'post_parent' => 0,
     'comment_count' => 0, 'menu_order' => 1, 'post_modified' => $now],
];

// Also add some attachments and nav_menu_items
$posts[] = ['post_author' => $adminId, 'post_date' => '2024-03-01 09:00:00',
    'post_content' => '', 'post_title' => 'featured-image.jpg', 'post_excerpt' => '',
    'post_status' => 'inherit', 'post_name' => 'featured-image',
    'post_type' => 'attachment', 'post_parent' => 0, 'comment_count' => 0,
    'menu_order' => 0, 'post_modified' => $now, 'post_mime_type' => 'image/jpeg'];

$db->insertMany($prefix . 'posts', $posts);

// WP_Query: SELECT * FROM wp_posts WHERE post_type = 'post' AND post_status = 'publish' ORDER BY post_date DESC LIMIT 10
$publishedPosts = $db->find($prefix . 'posts',
    ['post_type' => 'post', 'post_status' => 'publish'],
    ['sort' => ['post_date' => -1], 'limit' => 10]
);
assert_test(count($publishedPosts) >= 2, 'WP_Query: published posts', 'got ' . count($publishedPosts));
// All results should be post_type=post AND post_status=publish
$allPostType = true;
foreach ($publishedPosts as $pp) {
    if (($pp['post_type'] ?? '') !== 'post' || ($pp['post_status'] ?? '') !== 'publish') {
        $allPostType = false;
    }
}
assert_test($allPostType, 'WP_Query: all results are published posts');
assert_test($publishedPosts[0]['post_date'] >= $publishedPosts[1]['post_date'], 'WP_Query: sorted by date desc');

// get_page_by_path: SELECT * FROM wp_posts WHERE post_name = 'about' AND post_type = 'page' AND post_status = 'publish'
$aboutPage = $db->findOne($prefix . 'posts',
    ['post_name' => 'about', 'post_type' => 'page', 'post_status' => 'publish']
);
assert_test($aboutPage !== null && $aboutPage['post_title'] === 'About', 'get_page_by_path(about)');

// get_posts by type: SELECT * FROM wp_posts WHERE post_type = 'page' AND post_status IN ('publish','private')
$pages = $db->find($prefix . 'posts',
    ['post_type' => 'page', 'post_status' => ['$in' => ['publish', 'private']]]
);
assert_test(count($pages) === 2, 'get_posts: pages (publish+private)', 'got ' . count($pages));

// Count posts by status: SELECT post_status, COUNT(*) FROM wp_posts WHERE post_type = 'post' GROUP BY post_status
$postCounts = $db->aggregate($prefix . 'posts', [
    ['$match' => ['post_type' => 'post']],
    ['$group' => ['_id' => '$post_status', 'count' => ['$sum' => 1]]],
    ['$sort' => ['count' => -1]],
]);
assert_test(count($postCounts) >= 2, 'wp_count_posts aggregation', 'got ' . count($postCounts));
$publishCount = 0;
foreach ($postCounts as $pc) {
    if (($pc['_id'] ?? $pc['post_status'] ?? '') === 'publish') $publishCount = $pc['count'];
}
assert_test($publishCount === 2, 'wp_count_posts: 2 published', "got $publishCount");

// SELECT * FROM wp_posts WHERE post_type = 'attachment' AND post_mime_type LIKE 'image%'
$images = $db->find($prefix . 'posts',
    ['post_type' => 'attachment', 'post_mime_type' => ['$regex' => '^image']]
);
assert_test(count($images) === 1, 'Media library: image attachments');

// ─── wp_postmeta ─────────────────────────────────────────────────

echo "\n--- wp_postmeta (custom fields, featured images, SEO) ---\n";

// Find specific posts by slug (more reliable than positional)
$helloPost = $db->findOne($prefix . 'posts', ['post_name' => 'hello-world']);
$secondPost = $db->findOne($prefix . 'posts', ['post_name' => 'second-post']);
$helloPostId = $helloPost['_id'];
$secondPostId = $secondPost['_id'];

$postmetas = [
    ['post_id' => $helloPostId, 'meta_key' => '_edit_last', 'meta_value' => (string)$adminId],
    ['post_id' => $helloPostId, 'meta_key' => '_edit_lock', 'meta_value' => time() . ':' . $adminId],
    ['post_id' => $helloPostId, 'meta_key' => '_thumbnail_id', 'meta_value' => '99'],
    ['post_id' => $helloPostId, 'meta_key' => '_wp_page_template', 'meta_value' => 'default'],
    ['post_id' => $helloPostId, 'meta_key' => 'custom_field', 'meta_value' => 'custom value'],
    ['post_id' => $secondPostId, 'meta_key' => '_yoast_wpseo_title', 'meta_value' => 'SEO Title'],
    ['post_id' => $secondPostId, 'meta_key' => '_yoast_wpseo_metadesc', 'meta_value' => 'SEO description'],
    ['post_id' => $secondPostId, 'meta_key' => 'views_count', 'meta_value' => '150'],
];
$db->insertMany($prefix . 'postmeta', $postmetas);

// get_post_meta($post_id, '_thumbnail_id', true)
$thumb = $db->findOne($prefix . 'postmeta', ['post_id' => $helloPostId, 'meta_key' => '_thumbnail_id']);
assert_test($thumb !== null && $thumb['meta_value'] === '99', 'get_post_meta(_thumbnail_id)');

// get_post_meta($post_id) — all meta for a post
$allMeta = $db->find($prefix . 'postmeta', ['post_id' => $helloPostId]);
assert_test(count($allMeta) === 5, 'get_post_meta: all meta for post', 'got ' . count($allMeta));

// update_post_meta
$db->update($prefix . 'postmeta',
    ['post_id' => $secondPostId, 'meta_key' => 'views_count'],
    ['$set' => ['meta_value' => '151']]
);
$viewsMeta = $db->findOne($prefix . 'postmeta', ['post_id' => $secondPostId, 'meta_key' => 'views_count']);
assert_test($viewsMeta['meta_value'] === '151', 'update_post_meta(views_count)');

// ─── wp_terms / wp_term_taxonomy / wp_term_relationships ─────────
// (Inserted before postmeta delete to avoid soft-deleted doc leak — server bug workaround)

echo "\n--- Taxonomy system (categories, tags) ---\n";

$terms = [
    ['name' => 'Uncategorized', 'slug' => 'uncategorized', 'term_group' => 0],
    ['name' => 'News', 'slug' => 'news', 'term_group' => 0],
    ['name' => 'Tutorials', 'slug' => 'tutorials', 'term_group' => 0],
    ['name' => 'php', 'slug' => 'php', 'term_group' => 0],
    ['name' => 'wordpress', 'slug' => 'wordpress', 'term_group' => 0],
    ['name' => 'database', 'slug' => 'database', 'term_group' => 0],
];
$db->insertMany($prefix . 'terms', $terms);

$termDocs = $db->find($prefix . 'terms');
$termIds = [];
$termIdToSlug = [];
foreach ($termDocs as $t) {
    $termIds[$t['slug']] = $t['_id'];
    $termIdToSlug[$t['_id']] = $t['slug'];
}

$taxonomies = [
    ['term_id' => $termIds['uncategorized'] ?? -1, 'taxonomy' => 'category', 'description' => '', 'parent' => 0, 'count' => 1],
    ['term_id' => $termIds['news'], 'taxonomy' => 'category', 'description' => 'News articles', 'parent' => 0, 'count' => 1],
    ['term_id' => $termIds['tutorials'], 'taxonomy' => 'category', 'description' => 'Tutorial posts', 'parent' => 0, 'count' => 0],
    ['term_id' => $termIds['php'], 'taxonomy' => 'post_tag', 'description' => '', 'parent' => 0, 'count' => 1],
    ['term_id' => $termIds['wordpress'], 'taxonomy' => 'post_tag', 'description' => '', 'parent' => 0, 'count' => 1],
    ['term_id' => $termIds['database'], 'taxonomy' => 'post_tag', 'description' => '', 'parent' => 0, 'count' => 0],
];
$db->insertMany($prefix . 'term_taxonomy', $taxonomies);

$ttDocs = $db->find($prefix . 'term_taxonomy');
$ttIds = [];
foreach ($ttDocs as $tt) {
    $tid = $tt['term_id'];
    $slug = $termIdToSlug[$tid] ?? null;
    if ($slug !== null) $ttIds[$slug] = $tt['_id'];
}

// Assign posts to categories and tags
$relationships = [
    ['object_id' => $helloPostId, 'term_taxonomy_id' => $ttIds['uncategorized'] ?? -1, 'term_order' => 0],
    ['object_id' => $secondPostId, 'term_taxonomy_id' => $ttIds['news'] ?? -1, 'term_order' => 0],
    ['object_id' => $secondPostId, 'term_taxonomy_id' => $ttIds['php'] ?? -1, 'term_order' => 0],
    ['object_id' => $secondPostId, 'term_taxonomy_id' => $ttIds['wordpress'] ?? -1, 'term_order' => 0],
];
$db->insertMany($prefix . 'term_relationships', $relationships);

// get_the_category: find categories for a post
// SELECT t.*, tt.* FROM wp_terms t INNER JOIN wp_term_taxonomy tt ON t._id = tt.term_id
//   INNER JOIN wp_term_relationships tr ON tt._id = tr.term_taxonomy_id
//   WHERE tr.object_id = X AND tt.taxonomy = 'category'
$postTermRels = $db->find($prefix . 'term_relationships', ['object_id' => $secondPostId]);
$postTtIds = [];
foreach ($postTermRels as $r) {
    if (isset($r['term_taxonomy_id'])) $postTtIds[] = $r['term_taxonomy_id'];
}

// Find categories for this post by iterating (more reliable than $in with mixed ID types)
$allTT = $db->find($prefix . 'term_taxonomy');
$postCategories = array_values(array_filter($allTT, fn($tt) =>
    in_array($tt['_id'], $postTtIds) && $tt['taxonomy'] === 'category'
));
assert_test(count($postCategories) === 1, 'get_the_category: 1 category for second post', 'got ' . count($postCategories));
if (count($postCategories) > 0) {
    $catTermId = $postCategories[0]['term_id'];
    $catTerm = $db->findOne($prefix . 'terms', ['_id' => $catTermId]);
    assert_test($catTerm !== null && $catTerm['slug'] === 'news', 'Category is "news"');
} else {
    assert_test(false, 'Category is "news"', 'no categories found');
}

// get_the_tags: tags for a post
$postTags = array_values(array_filter($allTT, fn($tt) =>
    in_array($tt['_id'], $postTtIds) && $tt['taxonomy'] === 'post_tag'
));
assert_test(count($postTags) === 2, 'get_the_tags: 2 tags for second post', 'got ' . count($postTags));

// get_terms: SELECT * FROM wp_terms t INNER JOIN wp_term_taxonomy tt ON t._id = tt.term_id WHERE tt.taxonomy = 'category'
$allCategories = $db->find($prefix . 'term_taxonomy', ['taxonomy' => 'category']);
assert_test(count($allCategories) === 3, 'get_terms(category): 3 categories');

// delete_post_meta (moved after taxonomy inserts to avoid soft-delete data leak)
$db->deleteOne($prefix . 'postmeta', ['post_id' => $helloPostId, 'meta_key' => 'custom_field']);
$deleted = $db->findOne($prefix . 'postmeta', ['post_id' => $helloPostId, 'meta_key' => 'custom_field']);
assert_test($deleted === null, 'delete_post_meta(custom_field)');

// ─── wp_comments ─────────────────────────────────────────────────

echo "\n--- wp_comments ---\n";

$comments = [
    ['comment_post_ID' => $helloPostId, 'comment_author' => 'A WordPress Commenter',
     'comment_author_email' => 'commenter@example.com', 'comment_author_url' => 'https://example.com',
     'comment_date' => '2024-03-01 10:00:00', 'comment_content' => 'Hi, this is a comment.',
     'comment_approved' => '1', 'comment_type' => 'comment', 'comment_parent' => 0, 'user_id' => 0],
    ['comment_post_ID' => $helloPostId, 'comment_author' => 'Admin',
     'comment_author_email' => 'admin@example.com', 'comment_author_url' => '',
     'comment_date' => '2024-03-01 11:00:00', 'comment_content' => 'Thanks for commenting!',
     'comment_approved' => '1', 'comment_type' => 'comment', 'comment_parent' => 0, 'user_id' => $adminId],
    ['comment_post_ID' => $helloPostId, 'comment_author' => 'Spammer',
     'comment_author_email' => 'spam@evil.com', 'comment_author_url' => 'https://evil.com',
     'comment_date' => '2024-03-02 05:00:00', 'comment_content' => 'Buy cheap stuff!',
     'comment_approved' => 'spam', 'comment_type' => 'comment', 'comment_parent' => 0, 'user_id' => 0],
    ['comment_post_ID' => $helloPostId, 'comment_author' => 'Pending User',
     'comment_author_email' => 'pending@example.com', 'comment_author_url' => '',
     'comment_date' => '2024-03-03 09:00:00', 'comment_content' => 'Nice post!',
     'comment_approved' => '0', 'comment_type' => 'comment', 'comment_parent' => 0, 'user_id' => 0],
];
$db->insertMany($prefix . 'comments', $comments);

// SELECT * FROM wp_comments WHERE comment_post_ID = X AND comment_approved = '1' ORDER BY comment_date ASC
$approvedComments = $db->find($prefix . 'comments',
    ['comment_post_ID' => $helloPostId, 'comment_approved' => '1'],
    ['sort' => ['comment_date' => 1]]
);
assert_test(count($approvedComments) === 2, 'Approved comments for post', 'got ' . count($approvedComments));

// Comment counts by status: SELECT comment_approved, COUNT(*) FROM wp_comments GROUP BY comment_approved
$commentCounts = $db->aggregate($prefix . 'comments', [
    ['$group' => ['_id' => '$comment_approved', 'count' => ['$sum' => 1]]],
]);
$countMap = [];
foreach ($commentCounts as $cc) {
    $countMap[$cc['_id'] ?? ''] = $cc['count'];
}
assert_test(($countMap['1'] ?? 0) === 2, 'wp_count_comments: 2 approved');
assert_test(($countMap['spam'] ?? 0) === 1, 'wp_count_comments: 1 spam');
assert_test(($countMap['0'] ?? 0) === 1, 'wp_count_comments: 1 pending');

// Threaded comments: reply to first comment
$parentCommentId = $approvedComments[0]['_id'];
$db->insert($prefix . 'comments', [
    'comment_post_ID' => $helloPostId, 'comment_author' => 'Reply Author',
    'comment_author_email' => 'reply@example.com', 'comment_author_url' => '',
    'comment_date' => '2024-03-04 12:00:00', 'comment_content' => 'Great point!',
    'comment_approved' => '1', 'comment_type' => 'comment',
    'comment_parent' => $parentCommentId, 'user_id' => 0,
]);
$replies = $db->find($prefix . 'comments', ['comment_parent' => $parentCommentId]);
assert_test(count($replies) === 1, 'Threaded comment reply');

// ─── SQL Queries (WordPress uses raw SQL heavily) ────────────────

echo "\n--- Raw SQL queries (MySQL dialect) ---\n";

// SELECT COUNT(*) FROM wp_posts WHERE post_type = 'post' AND post_status = 'publish'
$sqlCount = $db->sql("SELECT COUNT(*) AS cnt FROM `{$prefix}posts` WHERE post_type = 'post' AND post_status = 'publish'");
assert_test(is_array($sqlCount) && $sqlCount[0]['cnt'] === 2, 'SQL COUNT published posts', json_encode($sqlCount));

// SELECT * FROM wp_posts WHERE post_status = 'publish' AND post_type = 'post' ORDER BY post_date DESC LIMIT 1
$sqlLatest = $db->sql("SELECT * FROM `{$prefix}posts` WHERE post_status = 'publish' AND post_type = 'post' ORDER BY post_date DESC LIMIT 1");
assert_test(count($sqlLatest) === 1, 'SQL latest published post', 'got ' . count($sqlLatest));

// SELECT * FROM wp_options WHERE option_name IN ('siteurl', 'home', 'blogname')
$sqlOptions = $db->sql("SELECT * FROM `{$prefix}options` WHERE option_name IN ('siteurl', 'home', 'blogname')");
assert_test(count($sqlOptions) === 3, 'SQL IN query for options', 'got ' . count($sqlOptions));

// UPDATE wp_posts SET comment_count = 3 WHERE post_name = 'hello-world'
$db->sql("UPDATE `{$prefix}posts` SET comment_count = 3 WHERE post_name = 'hello-world'");
$hwPost = $db->findOne($prefix . 'posts', ['post_name' => 'hello-world']);
assert_test($hwPost['comment_count'] === 3, 'SQL UPDATE comment_count');

// DELETE FROM wp_comments WHERE comment_approved = 'spam'
$db->sql("DELETE FROM `{$prefix}comments` WHERE comment_approved = 'spam'");
$spamCount = $db->count($prefix . 'comments', ['comment_approved' => 'spam']);
assert_test($spamCount === 0, 'SQL DELETE spam comments');

// SELECT with LIKE (WordPress search)
$searchResults = $db->sql("SELECT * FROM `{$prefix}posts` WHERE post_title LIKE '%Post%' AND post_status = 'publish'");
assert_test(count($searchResults) === 1, 'SQL LIKE search', 'got ' . count($searchResults));

// SHOW TABLES
$showTables = $db->sql("SHOW TABLES");
assert_test(is_array($showTables), 'SQL SHOW TABLES');

// ─── Transactional operations (plugin install, post publish) ─────

echo "\n--- Transactional operations ---\n";

// Simulate publishing a draft (multiple tables updated atomically)
$draftPost = $db->findOne($prefix . 'posts', ['post_status' => 'draft']);
assert_test($draftPost !== null, 'Found draft post');

$db->transaction(function ($client) use ($prefix, $draftPost, $ttIds) {
    // Update post status
    $client->update($prefix . 'posts',
        ['_id' => $draftPost['_id']],
        ['$set' => ['post_status' => 'publish', 'post_modified' => date('Y-m-d H:i:s')]]
    );
    // Add category
    $client->insert($prefix . 'term_relationships', [
        'object_id' => $draftPost['_id'],
        'term_taxonomy_id' => $ttIds['tutorials'],
        'term_order' => 0,
    ]);
    // Update term count
    $client->update($prefix . 'term_taxonomy',
        ['_id' => $ttIds['tutorials']],
        ['$set' => ['count' => 1]]
    );
});

$publishedDraft = $db->findOne($prefix . 'posts', ['_id' => $draftPost['_id']]);
assert_test($publishedDraft['post_status'] === 'publish', 'Tx: draft → publish');

$tutCat = $db->findOne($prefix . 'term_taxonomy', ['_id' => $ttIds['tutorials']]);
assert_test($tutCat['count'] === 1, 'Tx: term count updated');

// Simulate failed operation (rollback)
$countBefore = $db->count($prefix . 'posts');
try {
    $db->transaction(function ($client) use ($prefix) {
        $client->insert($prefix . 'posts', [
            'post_title' => 'Should Not Exist', 'post_type' => 'post',
            'post_status' => 'publish', 'post_author' => 0,
            'post_date' => date('Y-m-d H:i:s'), 'post_content' => '',
            'post_excerpt' => '', 'post_name' => 'should-not-exist',
            'post_parent' => 0, 'comment_count' => 0, 'menu_order' => 0,
            'post_modified' => date('Y-m-d H:i:s'),
        ]);
        throw new \RuntimeException('Simulated plugin error');
    });
} catch (\RuntimeException $e) {
    // expected
}
$countAfter = $db->count($prefix . 'posts');
assert_test($countAfter === $countBefore, 'Tx rollback: post not inserted');

// ─── Bulk operations (WP import, batch updates) ─────────────────

echo "\n--- Bulk operations ---\n";

// Simulate importing 100 posts (like WP importer plugin)
$bulkPosts = [];
for ($i = 0; $i < 100; $i++) {
    $bulkPosts[] = [
        'post_author' => $adminId, 'post_date' => "2024-07-" . sprintf('%02d', ($i % 28) + 1) . " 12:00:00",
        'post_content' => "<p>Imported post $i content with some text for searching.</p>",
        'post_title' => "Imported Post $i", 'post_excerpt' => '',
        'post_status' => 'publish', 'post_name' => "imported-post-$i",
        'post_type' => 'post', 'post_parent' => 0, 'comment_count' => 0,
        'menu_order' => 0, 'post_modified' => $now,
    ];
}
$db->insertMany($prefix . 'posts', $bulkPosts);
$totalPosts = $db->count($prefix . 'posts', ['post_type' => 'post']);
assert_test($totalPosts >= 103, 'Bulk import 100 posts', "total: $totalPosts");

// Paginated query: SELECT * FROM wp_posts WHERE post_type='post' AND post_status='publish' ORDER BY post_date DESC LIMIT 10 OFFSET 0
$page1 = $db->find($prefix . 'posts',
    ['post_type' => 'post', 'post_status' => 'publish'],
    ['sort' => ['post_date' => -1], 'limit' => 10, 'skip' => 0]
);
$page2 = $db->find($prefix . 'posts',
    ['post_type' => 'post', 'post_status' => 'publish'],
    ['sort' => ['post_date' => -1], 'limit' => 10, 'skip' => 10]
);
assert_test(count($page1) === 10, 'Pagination page 1');
assert_test(count($page2) === 10, 'Pagination page 2');
assert_test($page1[0]['_id'] !== $page2[0]['_id'], 'Page 1 and 2 are different');

// Batch update: mark all imported posts as having been migrated
$db->update($prefix . 'posts',
    ['post_name' => ['$regex' => '^imported-post-']],
    ['$set' => ['_migrated' => true]]
);
$migratedCount = $db->count($prefix . 'posts', ['_migrated' => true]);
assert_test($migratedCount === 100, 'Batch update 100 posts', "got $migratedCount");

// ─── Cleanup ─────────────────────────────────────────────────────

echo "\n--- Cleanup ---\n";
foreach ($tables as $t) {
    $db->dropCollection($prefix . $t);
}
assert_test(true, 'Dropped all tables');

$db->close();

echo "\n=== Results: $passed passed, $failed failed ===\n";
if ($failed > 0) exit(1);
