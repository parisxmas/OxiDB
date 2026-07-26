<?php
/**
 * WordPress + OxiDB Configuration
 */

// Database settings (used by db.php drop-in, not for MySQL)
define( 'DB_NAME', 'wordpress' );
define( 'DB_USER', 'oxidb' );
define( 'DB_PASSWORD', '' );
define( 'DB_HOST', '127.0.0.1' );
define( 'DB_CHARSET', 'utf8mb4' );
define( 'DB_COLLATE', '' );

// OxiDB connection settings
define( 'OXIDB_HOST', '127.0.0.1' );
define( 'OXIDB_PORT', 4444 );

// Force site URL (OxiDB doesn't auto-detect)
define( 'WP_SITEURL', 'http://127.0.0.1:8080' );
define( 'WP_HOME', 'http://127.0.0.1:8080' );

// Authentication keys and salts
define( 'AUTH_KEY',         'oxidb-wp-auth-key-change-me-1' );
define( 'SECURE_AUTH_KEY',  'oxidb-wp-secure-auth-key-change-me-2' );
define( 'LOGGED_IN_KEY',    'oxidb-wp-logged-in-key-change-me-3' );
define( 'NONCE_KEY',        'oxidb-wp-nonce-key-change-me-4' );
define( 'AUTH_SALT',        'oxidb-wp-auth-salt-change-me-5' );
define( 'SECURE_AUTH_SALT', 'oxidb-wp-secure-auth-salt-change-me-6' );
define( 'LOGGED_IN_SALT',   'oxidb-wp-logged-in-salt-change-me-7' );
define( 'NONCE_SALT',       'oxidb-wp-nonce-salt-change-me-8' );

$table_prefix = 'wp_';

// Debug
define( 'WP_DEBUG', true );
define( 'WP_DEBUG_LOG', true );
define( 'WP_DEBUG_DISPLAY', false );
define( 'SAVEQUERIES', true );

// Suppress OxiDB errors we handle gracefully
define( 'OXIDB_SUPPRESS_ERRORS', false );

// Disable WP-Cron (prevents loopback HTTP requests that hang PHP built-in server)
define( 'DISABLE_WP_CRON', true );

/* That's all, stop editing! Happy publishing. */

if ( ! defined( 'ABSPATH' ) ) {
    define( 'ABSPATH', __DIR__ . '/' );
}

require_once ABSPATH . 'wp-settings.php';
