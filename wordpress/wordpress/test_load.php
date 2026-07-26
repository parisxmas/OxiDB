<?php
error_reporting(E_ALL);
ini_set('display_errors', 1);

$_SERVER['HTTP_HOST'] = 'localhost:8080';
$_SERVER['REQUEST_URI'] = '/wp-admin/install.php';
$_SERVER['SERVER_NAME'] = 'localhost';
$_SERVER['SERVER_PORT'] = '8080';

define('WP_INSTALLING', true);

// Capture output
ob_start();
require_once __DIR__ . '/wp-load.php';
$output = ob_get_clean();

global $wpdb;
echo "wpdb class: " . get_class($wpdb) . "\n";
echo "Ready: " . var_export($wpdb->ready, true) . "\n";
echo "Last error: " . var_export($wpdb->last_error, true) . "\n";
echo "Last query: " . var_export($wpdb->last_query, true) . "\n";
echo "Num queries: " . $wpdb->num_queries . "\n";

if (strpos($output, 'error') !== false || strpos($output, 'Error') !== false) {
    preg_match('/wp-die-message[^>]*>(.*?)</', $output, $m);
    echo "\nError: " . ($m[1] ?? substr(strip_tags($output), 0, 500)) . "\n";
} else {
    echo "\nOK - no error\n";
}
