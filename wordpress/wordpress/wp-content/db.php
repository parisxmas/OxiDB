<?php
/**
 * OxiDB Drop-in Database Driver for WordPress
 *
 * Replaces the default MySQL-backed wpdb with OxiDB, a fast document database.
 * All SQL queries are routed through OxiDB's MySQL-dialect SQL engine.
 *
 * Place this file at wp-content/db.php and configure wp-config.php:
 *   define('OXIDB_HOST', '127.0.0.1');
 *   define('OXIDB_PORT', 4444);
 */

require_once dirname(__DIR__) . '/../../php/OxiDb.php';
require_once dirname(__DIR__) . '/../../php/OxiDbFFI.php';

// Point FFI to the native library
OxiDbFFIClient::setLibraryPath(dirname(__DIR__) . '/../../target/release/liboxidb_client_ffi.dylib');

class OxiDB_wpdb extends wpdb {

    /** @var OxiDbFFIClient */
    private $oxidb;

    /** Auto-increment counters per table */
    private array $auto_inc = [];

    /** Column schema cache from CREATE TABLE parsing */
    private array $table_schemas = [];

    /** Track created tables */
    private array $created_tables = [];

    /** Found rows from last SQL_CALC_FOUND_ROWS query */
    private int $found_rows_count = 0;

    public function __construct($dbuser, $dbpassword, $dbname, $dbhost) {
        // Set basic properties before connecting.
        $this->dbuser     = $dbuser;
        $this->dbpassword = $dbpassword;
        $this->dbname     = $dbname;
        $this->dbhost     = $dbhost;

        if (WP_DEBUG && WP_DEBUG_DISPLAY) {
            $this->show_errors();
        }

        if (defined('WP_SETUP_CONFIG')) {
            return;
        }

        $this->db_connect();
    }

    public function db_connect($allow_bail = true) {
        $host = defined('OXIDB_HOST') ? OXIDB_HOST : '127.0.0.1';
        $port = defined('OXIDB_PORT') ? OXIDB_PORT : 4444;

        try {
            $this->oxidb = new OxiDbFFIClient($host, $port);
            $this->oxidb->setDialect('mysql');
            file_put_contents('/tmp/oxidb_wp_debug.log', "Connected to OxiDB at $host:$port via FFI\n", FILE_APPEND);
        } catch (OxiDbException $e) {
            file_put_contents('/tmp/oxidb_wp_debug.log', "Connection FAILED: " . $e->getMessage() . "\n", FILE_APPEND);
            if ($allow_bail) {
                $this->bail(
                    '<h1>Error connecting to OxiDB</h1>' .
                    '<p>Could not connect to OxiDB at ' . htmlspecialchars("$host:$port") . '</p>' .
                    '<p>' . htmlspecialchars($e->getMessage()) . '</p>',
                    'db_connect_fail'
                );
                return false;
            }
            return false;
        }

        $this->dbh           = true; // Fake handle — not null so WP thinks we're connected
        $this->is_mysql      = true;
        $this->has_connected = true;
        $this->ready         = true;

        $this->init_charset();
        $this->set_prefix($this->base_prefix ?? 'wp_');

        return true;
    }

    public function check_connection($allow_bail = true) {
        if ($this->oxidb) {
            try {
                $this->oxidb->ping();
                return true;
            } catch (OxiDbException $e) {
                // Try reconnect
                return $this->db_connect($allow_bail);
            }
        }
        return $this->db_connect($allow_bail);
    }

    public function select($db, $dbh = null) {
        // No-op: OxiDB uses the default database.
        return;
    }

    public function close() {
        if ($this->oxidb) {
            $this->oxidb->close();
            $this->oxidb = null;
        }
        $this->dbh           = null;
        $this->ready         = false;
        $this->has_connected = false;
        return true;
    }

    public function set_charset($dbh, $charset = null, $collate = null) {
        // No-op.
    }

    public function determine_charset($charset, $collate) {
        return compact('charset', 'collate');
    }

    public function set_sql_mode($modes = []) {
        // No-op.
    }

    public function db_version() {
        return '8.0.36'; // Fake MySQL 8.0
    }

    public function db_server_info() {
        return '8.0.36-OxiDB';
    }

    public function has_cap($db_cap) {
        switch (strtolower($db_cap)) {
            case 'collation':
            case 'group_concat':
            case 'subqueries':
            case 'set_charset':
            case 'utf8mb4':
            case 'utf8mb4_520':
            case 'identifier_placeholders':
                return true;
        }
        return false;
    }

    public function flush() {
        $this->last_result   = [];
        $this->col_info      = null;
        $this->last_query    = null;
        $this->rows_affected = 0;
        $this->num_rows      = 0;
        $this->last_error    = '';
        $this->result        = null;
    }

    // ── Core query dispatch ──────────────────────────────────────────

    public function query($query) {
        if (!$this->ready) {
            $this->check_current_query = true;
            return false;
        }

        // Log INSERT queries for debugging
        if (preg_match('/^\s*INSERT/i', trim($query))) {
            file_put_contents('/tmp/oxidb_wp_debug.log', "QUERY INSERT: " . substr(trim($query), 0, 200) . "\n", FILE_APPEND);
        }

        if (function_exists('apply_filters')) {
            $query = apply_filters('query', $query);
        }

        if (!$query) {
            $this->insert_id = 0;
            return false;
        }

        $this->flush();
        $this->func_call  = "\$db->query(\"$query\")";
        $this->last_query = $query;
        $this->check_current_query = true;

        if (defined('SAVEQUERIES') && SAVEQUERIES) {
            $this->timer_start();
        }

        ++$this->num_queries;

        $result = $this->execute_oxidb_query($query);

        if (defined('SAVEQUERIES') && SAVEQUERIES) {
            $this->log_query(
                $query,
                $this->timer_stop(),
                $this->get_caller(),
                $this->time_start,
                []
            );
        }

        return $result;
    }

    /**
     * Route a SQL query to OxiDB or handle it locally.
     */
    private function execute_oxidb_query(string $sql) {
        $trimmed = trim($sql);

        // ── Statements we handle locally ────────────────────────
        if ($this->handle_set_statement($trimmed))         return true;
        if ($this->handle_show_tables_like($trimmed))      return $this->num_rows;
        if (($r = $this->handle_describe($trimmed)) !== null) return $r;
        if (($r = $this->handle_show_full_columns($trimmed)) !== null) return $r;
        if (($r = $this->handle_show_table_status($trimmed)) !== null) return $r;
        if (($r = $this->handle_show_index($trimmed)) !== null) return $r;
        if ($this->handle_alter_table($trimmed))           return true;
        if (($r = $this->handle_show_variables($trimmed)) !== null) return $r;

        // ── Rewrite MySQL-isms for OxiDB ─────────────────────────
        $trimmed = $this->rewrite_sql($trimmed);

        // ── FOUND_ROWS() was already handled in rewrite ─────────
        if ($trimmed === '__FOUND_ROWS_HANDLED__') {
            return $this->num_rows;
        }

        // ── Route through OxiDB SQL engine ──────────────────────
        file_put_contents('/tmp/oxidb_wp_debug.log', "SQL: " . substr($trimmed, 0, 300) . "\n", FILE_APPEND);
        // Handle SELECT from non-existent collection gracefully
        if (preg_match('/^\s*SELECT\s/i', $trimmed)) {
            // Pre-check: if collection doesn't exist, return empty result
            if (preg_match('/\bFROM\s+[`]?(\w+)[`]?/i', $trimmed, $fromMatch)) {
                try {
                    $cols = $this->oxidb->listCollections();
                    file_put_contents('/tmp/oxidb_wp_debug.log', "  -> collections: " . json_encode($cols) . ", checking: {$fromMatch[1]}\n", FILE_APPEND);
                    if (!in_array($fromMatch[1], $cols, true)) {
                        $this->last_result = [];
                        $this->num_rows = 0;
                        $this->last_error = '';
                        file_put_contents('/tmp/oxidb_wp_debug.log', "  -> collection not found, returning empty\n", FILE_APPEND);
                        return 0;
                    }
                } catch (OxiDbException $e) {
                    file_put_contents('/tmp/oxidb_wp_debug.log', "  -> listCollections error: " . $e->getMessage() . "\n", FILE_APPEND);
                }
            }
        }
        try {
            $result = $this->oxidb->sql($trimmed);
        } catch (OxiDbException $e) {
            $msg = $e->getMessage();

            // CREATE TABLE on already existing collection: ignore
            if (preg_match('/already exists/i', $msg) &&
                preg_match('/^\s*CREATE\s+TABLE/i', $trimmed)) {
                $this->result = true;
                return true;
            }

            // Many MySQL-specific queries (REGEXP, CONCAT, multi-table DELETE, subqueries) are expected to fail.
            // WordPress handles these gracefully, so we suppress non-critical errors.
            $non_critical = preg_match('/REGEXP|CONCAT|unsupported|expected field|parse error|subquer|HAVING|GROUP|UNION|__no_match__/i', $msg);

            file_put_contents('/tmp/oxidb_wp_debug.log', "ERROR: $msg | SQL: " . substr($trimmed, 0, 150) . "\n", FILE_APPEND);

            $this->last_error = $msg;
            if (!$non_critical) {
                $this->print_error();
            }
            return false;
        }

        // ── Process the result based on query type ──────────────

        // DDL: CREATE TABLE, DROP TABLE, etc.
        if (preg_match('/^\s*(CREATE|ALTER|TRUNCATE|DROP)\s/i', $trimmed)) {
            // If CREATE TABLE, parse schema for DESCRIBE support
            if (preg_match('/^\s*CREATE\s+TABLE/i', $trimmed)) {
                $this->parse_and_cache_schema($trimmed);
            }
            $this->result = true;
            return true;
        }

        // INSERT / REPLACE
        if (preg_match('/^\s*(INSERT|REPLACE)\s/i', $trimmed)) {
            $this->rows_affected = 1;
            if (is_array($result) && isset($result['ids'])) {
                $ids = $result['ids'];
                $this->insert_id = is_array($ids) && count($ids) > 0 ? $ids[0] : 0;
                $this->rows_affected = count($ids);
            } elseif (is_array($result) && isset($result['id'])) {
                $this->insert_id = $result['id'];
            } elseif (is_numeric($result)) {
                $this->insert_id = (int)$result;
            }
            file_put_contents('/tmp/oxidb_wp_debug.log', "INSERT result: " . json_encode($result) . " insert_id={$this->insert_id}\n", FILE_APPEND);
            return $this->rows_affected;
        }

        // UPDATE
        if (preg_match('/^\s*UPDATE\s/i', $trimmed)) {
            if (is_array($result) && isset($result['modified'])) {
                $this->rows_affected = (int)$result['modified'];
            } elseif (is_numeric($result)) {
                $this->rows_affected = (int)$result;
            }
            return $this->rows_affected;
        }

        // DELETE
        if (preg_match('/^\s*DELETE\s/i', $trimmed)) {
            if (is_array($result) && isset($result['deleted'])) {
                $this->rows_affected = (int)$result['deleted'];
            } elseif (is_numeric($result)) {
                $this->rows_affected = (int)$result;
            }
            return $this->rows_affected;
        }

        // SELECT / SHOW — expect array of rows
        if (is_array($result)) {
            // Detect which table this query is from, to map _id → correct PK column
            $pk_col = null;
            if (preg_match('/\bFROM\s+[`]?(\w+)[`]?/i', $trimmed, $tblMatch)) {
                $pk_col = $this->get_pk_column($tblMatch[1]);
            }

            $num_rows = 0;
            foreach ($result as $row) {
                if (is_array($row)) {
                    $obj = new stdClass();
                    foreach ($row as $k => $v) {
                        // Map _id to the table's primary key column
                        if ($k === '_id' && $pk_col !== null) {
                            $obj->$pk_col = $v;
                            continue;
                        }
                        // Skip _id if no mapping and not explicitly selected
                        if ($k === '_id' && !preg_match('/\b_id\b/i', $trimmed)) {
                            continue;
                        }
                        $obj->$k = $v;
                    }
                    $this->last_result[$num_rows] = $obj;
                } elseif (is_object($row)) {
                    $this->last_result[$num_rows] = $row;
                } else {
                    // Scalar result (e.g., SHOW TABLES returns strings)
                    $obj = new stdClass();
                    $obj->{'Tables_in_db'} = $row;
                    $this->last_result[$num_rows] = $obj;
                }
                $num_rows++;
            }
            $this->num_rows = $num_rows;
            // Track for FOUND_ROWS()
            if (preg_match('/^\s*SELECT\s/i', $trimmed)) {
                $this->found_rows_count = $num_rows;
            }
            return $num_rows;
        }

        // String result (DDL message, etc.)
        if (is_string($result)) {
            $this->result = true;
            return true;
        }

        return true;
    }

    // ── Primary key mapping ────────────────────────────────────

    /** Map WordPress table names to their primary key column */
    private static array $pk_map = [
        'wp_users'              => 'ID',
        'wp_posts'              => 'ID',
        'wp_comments'           => 'comment_ID',
        'wp_links'              => 'link_id',
        'wp_options'            => 'option_id',
        'wp_postmeta'           => 'meta_id',
        'wp_usermeta'           => 'umeta_id',
        'wp_terms'              => 'term_id',
        'wp_term_taxonomy'      => 'term_taxonomy_id',
        'wp_term_relationships' => null, // composite key
        'wp_termmeta'           => 'meta_id',
        'wp_commentmeta'        => 'meta_id',
    ];

    private function get_pk_column(string $table): ?string {
        return self::$pk_map[$table] ?? null;
    }

    /**
     * Apply a regex replacement only to parts of SQL outside single-quoted strings.
     * This prevents mangling values like password hashes that contain periods.
     */
    private function replace_outside_strings(string $sql, string $pattern, string $replacement): string {
        // Split SQL into quoted and unquoted segments
        $parts = preg_split("/(\'[^\']*\')/", $sql, -1, PREG_SPLIT_DELIM_CAPTURE);
        $result = '';
        foreach ($parts as $i => $part) {
            if ($i % 2 === 0) {
                // Unquoted segment — apply regex
                $result .= preg_replace($pattern, $replacement, $part);
            } else {
                // Quoted string — leave untouched
                $result .= $part;
            }
        }
        return $result;
    }

    // ── SQL rewriting ──────────────────────────────────────────

    /**
     * Rewrite MySQL-specific SQL constructs into OxiDB-compatible SQL.
     */
    private function rewrite_sql(string $sql): string {
        // Remove SQL_CALC_FOUND_ROWS (WordPress uses this for pagination)
        $sql = preg_replace('/\bSQL_CALC_FOUND_ROWS\s+/i', '', $sql);

        // Handle FOUND_ROWS() — replace with a count query marker
        if (preg_match('/^\s*SELECT\s+FOUND_ROWS\(\)/i', $sql)) {
            // Return the count from last query — we stored it
            $this->last_result = [];
            $obj = new stdClass();
            $obj->{'FOUND_ROWS()'} = $this->found_rows_count ?? 0;
            $this->last_result[0] = $obj;
            $this->num_rows = 1;
            return '__FOUND_ROWS_HANDLED__';
        }

        // Strip table-qualified wildcards: wp_posts.* → *
        $sql = $this->replace_outside_strings($sql, '/\b`?(\w+)`?\s*\.\s*\*/', '*');

        // Strip table-qualified column references: wp_posts.ID → ID, wp_posts.post_date → post_date
        // Must handle: table.col, `table`.`col`, `table`.col
        // IMPORTANT: only replace outside of quoted strings to avoid mangling values like password hashes
        $sql = $this->replace_outside_strings($sql, '/\b`?(\w+)`?\s*\.\s*`?(\w+)`?/', '$2');

        // Map WordPress PK columns to OxiDB's _id
        // Detect table from FROM/INTO/UPDATE clause
        $pk_col = null;
        if (preg_match('/\b(?:FROM|INTO|UPDATE)\s+[`]?(\w+)[`]?/i', $sql, $tblM)) {
            $pk_col = $this->get_pk_column($tblM[1]);
        }
        if ($pk_col) {
            // Replace PK column name with _id in WHERE/SET/ORDER BY contexts
            // Only outside quoted strings to avoid mangling values
            $escaped = preg_quote($pk_col, '/');
            $sql = $this->replace_outside_strings($sql, '/\b`?' . $escaped . '`?\b/', '_id');
        }

        // Remove WHERE 1=1 AND → WHERE, or standalone WHERE 1=1
        $sql = preg_replace('/\bWHERE\s+1\s*=\s*1\s+AND\b/i', 'WHERE', $sql);
        $sql = preg_replace('/\bWHERE\s+1\s*=\s*1\s*$/i', '', $sql);
        // Handle WHERE ... AND 1=1
        $sql = preg_replace('/\bAND\s+1\s*=\s*1\b/i', '', $sql);

        // Remove 0 = 1 conditions (WordPress uses this to force empty results in some queries)
        // Turn "WHERE ... AND ( \n  0 = 1\n)" into a no-match condition
        if (preg_match('/\b0\s*=\s*1\b/', $sql)) {
            // This query is designed to return no results
            // Replace "0 = 1" with "_id = -99999" which matches nothing
            $sql = preg_replace('/\b0\s*=\s*1\b/', "_id = '__no_match__'", $sql);
        }

        // Remove GROUP BY (not supported in OxiDB SQL)
        $sql = preg_replace('/\s+GROUP\s+BY\s+\S+/i', '', $sql);

        // Remove NOT IN (0) — WordPress uses this sometimes
        $sql = preg_replace('/\bAND\s+\w+\s+NOT\s+IN\s*\(\s*0\s*\)/i', '', $sql);

        // Remove LIMIT that has offset syntax already handled: keep LIMIT N or LIMIT offset, N
        // OxiDB supports LIMIT and OFFSET separately
        if (preg_match('/LIMIT\s+(\d+)\s*,\s*(\d+)/i', $sql, $lm)) {
            $sql = preg_replace('/LIMIT\s+\d+\s*,\s*\d+/i', "LIMIT {$lm[2]} OFFSET {$lm[1]}", $sql);
        }

        // Remove charset/collation specifications
        $sql = preg_replace('/\s+CHARACTER\s+SET\s+\w+/i', '', $sql);
        $sql = preg_replace('/\s+COLLATE\s+\w+/i', '', $sql);
        $sql = preg_replace('/\s+DEFAULT\s+CHARSET\s*=?\s*\w+/i', '', $sql);

        // Remove ENGINE= specifications
        $sql = preg_replace('/\s+ENGINE\s*=\s*\w+/i', '', $sql);

        // Remove IF NOT EXISTS for CREATE TABLE (OxiDB handles duplicates via error)
        // Actually keep it — just note it

        // CAST(... AS CHAR) → just the expression (OxiDB doesn't support CAST)
        $sql = preg_replace('/CAST\s*\(\s*(\w+)\s+AS\s+\w+\s*\)/i', '$1', $sql);

        // Remove /*!... */ MySQL version-conditional comments
        $sql = preg_replace('/\/\*!\d+\s+(.*?)\s*\*\//', '$1', $sql);

        // Convert IFNULL(a, b) → remove it or use COALESCE (not supported, simplify)
        // For now strip it to just the first argument
        $sql = preg_replace_callback('/IFNULL\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)/i', function($m) {
            return trim($m[1]);
        }, $sql);

        return $sql;
    }

    // ── SET statements ──────────────────────────────────────────

    private function handle_set_statement(string $sql): bool {
        if (preg_match('/^\s*SET\s/i', $sql)) {
            return true; // Silently ignore all SET statements
        }
        return false;
    }

    // ── SHOW TABLES LIKE 'pattern' ──────────────────────────────

    private function handle_show_tables_like(string $sql): bool {
        if (!preg_match('/^\s*SHOW\s+TABLES\s+LIKE\s+[\'"]([^\'"]+)[\'"]/i', $sql, $m)) {
            return false;
        }

        $pattern = $m[1];
        // Convert SQL LIKE pattern to a check
        $table = str_replace(['%', '_'], ['', ''], $pattern);

        try {
            $collections = $this->oxidb->listCollections();
            $matched = [];
            foreach ($collections as $col) {
                if ($this->sql_like_match($col, $pattern)) {
                    $matched[] = $col;
                }
            }

            $this->last_result = [];
            $num = 0;
            foreach ($matched as $tbl) {
                $obj = new stdClass();
                $obj->{"Tables_in_{$this->dbname} ({$pattern})"} = $tbl;
                $this->last_result[$num++] = $obj;
            }
            $this->num_rows = $num;
            return true;
        } catch (OxiDbException $e) {
            return true;
        }
    }

    private function sql_like_match(string $value, string $pattern): bool {
        $regex = '/^' . str_replace(
            ['%', '_'],
            ['.*', '.'],
            preg_quote($pattern, '/')
        ) . '$/i';
        return (bool)preg_match($regex, $value);
    }

    // ── DESCRIBE / DESC table ────────────────────────────────────

    private function handle_describe(string $sql): ?int {
        if (!preg_match('/^\s*(?:DESCRIBE|DESC)\s+[`]?(\S+?)[`]?\s*;?\s*$/i', $sql, $m)) {
            return null;
        }

        $table = trim($m[1], '`');
        file_put_contents('/tmp/oxidb_wp_debug.log', "DESCRIBE: $table\n", FILE_APPEND);

        // Check if collection exists
        try {
            $collections = $this->oxidb->listCollections();
            if (!in_array($table, $collections, true)) {
                // Keep last_error EMPTY so WP's is_blog_installed() sees "doesn't exist"
                $this->last_error = '';
                $this->num_rows = 0;
                return 0;
            }
        } catch (OxiDbException $e) {
            $this->last_error = '';
            return 0;
        }

        // If we have cached schema from CREATE TABLE, use it
        if (isset($this->table_schemas[$table])) {
            $this->last_result = $this->table_schemas[$table]['describe'];
            $this->num_rows = count($this->last_result);
            return $this->num_rows;
        }

        // Infer schema from a sample document
        $fields = $this->infer_fields_from_data($table);
        $this->last_result = $fields;
        $this->num_rows = count($fields);
        return $this->num_rows;
    }

    // ── SHOW FULL COLUMNS FROM table ─────────────────────────────

    private function handle_show_full_columns(string $sql): ?int {
        if (!preg_match('/^\s*SHOW\s+(?:FULL\s+)?COLUMNS\s+FROM\s+[`]?(\S+?)[`]?\s*;?\s*$/i', $sql, $m)) {
            return null;
        }

        $table = trim($m[1], '`');

        // Check if collection exists
        try {
            $collections = $this->oxidb->listCollections();
            if (!in_array($table, $collections, true)) {
                $this->last_error = "Table '$table' doesn't exist";
                return 0;
            }
        } catch (OxiDbException $e) {
            return 0;
        }

        // Use cached schema or infer
        if (isset($this->table_schemas[$table])) {
            $this->last_result = $this->table_schemas[$table]['full_columns'];
        } else {
            $this->last_result = $this->infer_full_columns($table);
        }

        $this->num_rows = count($this->last_result);
        return $this->num_rows;
    }

    // ── SHOW TABLE STATUS LIKE 'table' ────────────────────────────

    private function handle_show_table_status(string $sql): ?int {
        if (!preg_match('/^\s*SHOW\s+TABLE\s+STATUS\s+LIKE\s+[\'"]([^\'"]+)[\'"]/i', $sql, $m)) {
            return null;
        }

        $table = str_replace(['%', '_'], ['', ''], $m[1]);

        $obj = new stdClass();
        $obj->Name          = $table;
        $obj->Engine        = 'OxiDB';
        $obj->Version       = 10;
        $obj->Row_format    = 'Dynamic';
        $obj->Rows          = 0;
        $obj->Avg_row_length = 0;
        $obj->Data_length   = 0;
        $obj->Max_data_length = 0;
        $obj->Index_length  = 0;
        $obj->Data_free     = 0;
        $obj->Auto_increment = null;
        $obj->Create_time   = date('Y-m-d H:i:s');
        $obj->Update_time   = null;
        $obj->Check_time    = null;
        $obj->Collation     = 'utf8mb4_unicode_ci';
        $obj->Checksum      = null;
        $obj->Create_options = '';
        $obj->Comment       = '';

        $this->last_result = [$obj];
        $this->num_rows = 1;
        return 1;
    }

    // ── SHOW INDEX FROM table ─────────────────────────────────────

    private function handle_show_index(string $sql): ?int {
        if (!preg_match('/^\s*SHOW\s+(?:INDEX|INDEXES|KEYS)\s+FROM\s+[`]?(\S+?)[`]?\s*(?:WHERE\s+.*)?\s*;?\s*$/i', $sql, $m)) {
            return null;
        }

        $table = trim($m[1], '`');

        try {
            $indexes = $this->oxidb->listIndexes($table);
        } catch (OxiDbException $e) {
            $this->last_result = [];
            $this->num_rows = 0;
            return 0;
        }

        $this->last_result = [];
        $seq = 0;

        // Always add primary key index on _id
        $pk = new stdClass();
        $pk->Table        = $table;
        $pk->Non_unique   = '0';
        $pk->Key_name     = 'PRIMARY';
        $pk->Seq_in_index = '1';
        $pk->Column_name  = '_id';
        $pk->Collation    = 'A';
        $pk->Cardinality  = '0';
        $pk->Sub_part     = null;
        $pk->Packed       = null;
        $pk->Null         = '';
        $pk->Index_type   = 'BTREE';
        $pk->Comment      = '';
        $pk->Index_comment = '';
        $this->last_result[$seq++] = $pk;

        if (is_array($indexes)) {
            foreach ($indexes as $idx) {
                $name = is_array($idx) ? ($idx['name'] ?? ($idx['field'] ?? "idx_$seq")) : (string)$idx;
                $field = is_array($idx) ? ($idx['field'] ?? $name) : (string)$idx;

                $obj = new stdClass();
                $obj->Table        = $table;
                $obj->Non_unique   = '1';
                $obj->Key_name     = $name;
                $obj->Seq_in_index = '1';
                $obj->Column_name  = $field;
                $obj->Collation    = 'A';
                $obj->Cardinality  = '0';
                $obj->Sub_part     = null;
                $obj->Packed       = null;
                $obj->Null         = 'YES';
                $obj->Index_type   = 'BTREE';
                $obj->Comment      = '';
                $obj->Index_comment = '';
                $this->last_result[$seq++] = $obj;
            }
        }

        $this->num_rows = $seq;
        return $seq;
    }

    // ── ALTER TABLE ─────────────────────────────────────────────

    private function handle_alter_table(string $sql): bool {
        if (!preg_match('/^\s*ALTER\s+TABLE\s/i', $sql)) {
            return false;
        }
        // Silently ignore ALTER TABLE — OxiDB is schema-free
        $this->result = true;
        return true;
    }

    // ── SHOW VARIABLES ──────────────────────────────────────────

    private function handle_show_variables(string $sql): ?int {
        if (!preg_match('/^\s*SHOW\s+(?:GLOBAL\s+)?VARIABLES\s/i', $sql)) {
            return null;
        }

        $this->last_result = [];
        $this->num_rows = 0;
        return 0;
    }

    // ── Schema parsing and inference ─────────────────────────────

    /**
     * Parse a CREATE TABLE statement and cache the column schema.
     */
    private function parse_and_cache_schema(string $sql): void {
        if (!preg_match('/CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[`]?(\S+?)[`]?\s*\((.*)\)/si', $sql, $m)) {
            return;
        }

        $table = trim($m[1], '`');
        $body  = $m[2];

        $lines = explode("\n", $body);
        $describe = [];
        $full_columns = [];
        $i = 0;

        foreach ($lines as $line) {
            $line = trim($line, " \t\r\n,");
            if (empty($line)) continue;

            // Skip KEY/INDEX/PRIMARY/UNIQUE/CONSTRAINT lines
            if (preg_match('/^\s*(PRIMARY|UNIQUE|KEY|INDEX|CONSTRAINT|FULLTEXT|SPATIAL)\s/i', $line)) {
                continue;
            }

            // Parse: `column_name` type [(length)] [NOT NULL] [DEFAULT value] [AUTO_INCREMENT]
            if (!preg_match('/^[`]?(\w+)[`]?\s+(\w+)(?:\(([^)]*)\))?\s*(.*)/i', $line, $cm)) {
                continue;
            }

            $field   = $cm[1];
            $type    = strtolower($cm[2]);
            $length  = $cm[3] ?? '';
            $rest    = strtolower($cm[4] ?? '');

            $full_type = $type;
            if ($length) $full_type .= "($length)";
            if (str_contains($rest, 'unsigned')) $full_type .= ' unsigned';

            $nullable = !str_contains($rest, 'not null');
            $default  = null;
            if (preg_match("/default\s+'([^']*)'/i", $cm[4] ?? '', $dm)) {
                $default = $dm[1];
            } elseif (preg_match('/default\s+(\S+)/i', $cm[4] ?? '', $dm)) {
                $val = strtolower($dm[1]);
                if ($val !== 'null') $default = $dm[1];
            }

            $auto_inc = str_contains($rest, 'auto_increment');

            // DESCRIBE row
            $d = new stdClass();
            $d->Field   = $field;
            $d->Type    = $full_type;
            $d->Null    = $nullable ? 'YES' : 'NO';
            $d->Key     = $auto_inc ? 'PRI' : '';
            $d->Default = $default;
            $d->Extra   = $auto_inc ? 'auto_increment' : '';
            $describe[$i] = $d;

            // SHOW FULL COLUMNS row
            $fc = new stdClass();
            $fc->Field      = $field;
            $fc->Type       = $full_type;
            $fc->Collation  = in_array($type, ['varchar', 'text', 'longtext', 'mediumtext', 'tinytext', 'char', 'enum'])
                              ? 'utf8mb4_unicode_ci' : null;
            $fc->Null       = $nullable ? 'YES' : 'NO';
            $fc->Key        = $auto_inc ? 'PRI' : '';
            $fc->Default    = $default;
            $fc->Extra      = $auto_inc ? 'auto_increment' : '';
            $fc->Privileges = 'select,insert,update,references';
            $fc->Comment    = '';
            $full_columns[$i] = $fc;

            $i++;
        }

        if (!empty($describe)) {
            $this->table_schemas[$table] = [
                'describe'     => $describe,
                'full_columns' => $full_columns,
            ];
        }
    }

    /**
     * Infer DESCRIBE output from a sample document in the collection.
     */
    private function infer_fields_from_data(string $table): array {
        try {
            $doc = $this->oxidb->findOne($table);
        } catch (OxiDbException $e) {
            $doc = null;
        }

        $fields = [];
        if ($doc && is_array($doc)) {
            $i = 0;
            foreach ($doc as $key => $value) {
                $d = new stdClass();
                $d->Field   = $key;
                $d->Type    = $this->infer_mysql_type($value);
                $d->Null    = 'YES';
                $d->Key     = ($key === '_id') ? 'PRI' : '';
                $d->Default = null;
                $d->Extra   = ($key === '_id') ? 'auto_increment' : '';
                $fields[$i++] = $d;
            }
        } else {
            // Return at least _id
            $d = new stdClass();
            $d->Field   = '_id';
            $d->Type    = 'bigint(20) unsigned';
            $d->Null    = 'NO';
            $d->Key     = 'PRI';
            $d->Default = null;
            $d->Extra   = 'auto_increment';
            $fields[0] = $d;
        }

        return $fields;
    }

    private function infer_full_columns(string $table): array {
        $describe = $this->infer_fields_from_data($table);
        $full = [];
        foreach ($describe as $i => $d) {
            $fc = new stdClass();
            $fc->Field      = $d->Field;
            $fc->Type       = $d->Type;
            $fc->Collation  = str_contains($d->Type, 'text') || str_contains($d->Type, 'varchar')
                              ? 'utf8mb4_unicode_ci' : null;
            $fc->Null       = $d->Null;
            $fc->Key        = $d->Key;
            $fc->Default    = $d->Default;
            $fc->Extra      = $d->Extra;
            $fc->Privileges = 'select,insert,update,references';
            $fc->Comment    = '';
            $full[$i] = $fc;
        }
        return $full;
    }

    private function infer_mysql_type($value): string {
        if (is_null($value))   return 'text';
        if (is_bool($value))   return 'tinyint(1)';
        if (is_int($value))    return 'bigint(20)';
        if (is_float($value))  return 'double';
        if (is_string($value)) return strlen($value) > 255 ? 'longtext' : 'varchar(255)';
        if (is_array($value))  return 'longtext';
        return 'text';
    }

    // ── Override process_fields to skip charset/length checks ─────

    protected function process_fields($table, $data, $format) {
        $data = $this->process_field_formats($data, $format);
        if (false === $data) {
            return false;
        }

        // Set charset and length for all fields without querying the DB
        foreach ($data as $field => &$value) {
            if ('%d' === $value['format'] || '%f' === $value['format']) {
                $value['charset'] = false;
            } else {
                $value['charset'] = 'utf8mb4';
            }
            $value['length'] = false; // No length limit
        }
        unset($value);

        return $data;
    }

    // ── Escape ───────────────────────────────────────────────────

    public function _real_escape($data) {
        if (!is_string($data)) {
            return $data;
        }
        return addslashes($data);
    }

    // ── Column charset/length: short-circuit ─────────────────────

    public function get_col_charset($table, $column) {
        return 'utf8mb4';
    }

    public function get_col_length($table, $column) {
        return ['type' => 'byte', 'length' => 0];
    }

    protected function get_table_charset($table) {
        $this->table_charset[strtolower($table)] = 'utf8mb4';
        return 'utf8mb4';
    }

    protected function strip_invalid_text_from_query($query) {
        return $query;
    }

    // ── Column info from last query ──────────────────────────────

    protected function load_col_info() {
        if ($this->col_info) {
            return;
        }

        $this->col_info = [];
        if (!empty($this->last_result[0])) {
            $i = 0;
            foreach (get_object_vars($this->last_result[0]) as $name => $val) {
                $info = new stdClass();
                $info->name       = $name;
                $info->table      = '';
                $info->def        = '';
                $info->max_length = 0;
                $info->not_null   = 0;
                $info->primary_key = ($name === '_id') ? 1 : 0;
                $info->multiple_key = 0;
                $info->unique_key = 0;
                $info->numeric    = is_numeric($val) ? 1 : 0;
                $info->blob       = 0;
                $info->type       = is_numeric($val) ? 'int' : 'string';
                $info->unsigned   = 0;
                $info->zerofill   = 0;
                $this->col_info[$i++] = $info;
            }
        }
    }

    // ── Print error ──────────────────────────────────────────────

    public function print_error($str = '') {
        if (!$str) {
            $str = $this->last_error;
        }

        if (defined('OXIDB_SUPPRESS_ERRORS') && OXIDB_SUPPRESS_ERRORS) {
            return false;
        }

        // Log to error_log but don't die
        if ($str) {
            error_log("OxiDB wpdb error: $str | Query: {$this->last_query}");
        }

        if ($this->show_errors && !$this->suppress_errors) {
            $error_str = htmlspecialchars($str, ENT_QUOTES);
            $query_str = htmlspecialchars($this->last_query, ENT_QUOTES);
            echo "<div style='border:1px solid #c00;padding:10px;margin:5px;background:#fff0f0;font-family:monospace'>" .
                 "<b>OxiDB error:</b> $error_str<br><b>Query:</b> <code>$query_str</code></div>";
        }

        return false;
    }
}

// Replace global $wpdb
$wpdb = new OxiDB_wpdb(DB_USER, DB_PASSWORD, DB_NAME, DB_HOST);
