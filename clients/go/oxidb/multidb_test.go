package oxidb_test

import (
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/parisxmas/OxiDB/go/oxidb"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func connectTo(t *testing.T, host string, port int) *oxidb.Client {
	t.Helper()
	c, err := oxidb.Connect(host, port, 5*time.Second)
	if err != nil {
		t.Fatalf("connect %s:%d: %v", host, port, err)
	}
	return c
}

func getAuthClient(t *testing.T) *oxidb.Client {
	t.Helper()
	host := "127.0.0.1"
	port := 4445 // auth-enabled server
	if h := os.Getenv("OXIDB_AUTH_HOST"); h != "" {
		host = h
	}
	if p := os.Getenv("OXIDB_AUTH_PORT"); p != "" {
		port, _ = strconv.Atoi(p)
	}
	return connectTo(t, host, port)
}

func getNoAuthClient(t *testing.T) *oxidb.Client {
	t.Helper()
	host := "127.0.0.1"
	port := 4444
	if h := os.Getenv("OXIDB_HOST"); h != "" {
		host = h
	}
	if p := os.Getenv("OXIDB_PORT"); p != "" {
		port, _ = strconv.Atoi(p)
	}
	return connectTo(t, host, port)
}

// ===========================================================================
// Part 1: Multi-database tests (no auth required)
// ===========================================================================

func TestMultiDB_CreateAndListDatabases(t *testing.T) {
	c := getNoAuthClient(t)
	defer c.Close()

	// Cleanup from previous runs
	_ = c.DropDatabase("go_testdb1")
	_ = c.DropDatabase("go_testdb2")

	// List should include the default database
	dbs, err := c.ListDatabases()
	if err != nil {
		t.Fatalf("list_databases: %v", err)
	}
	found := false
	for _, db := range dbs {
		if db == "oxidb" {
			found = true
		}
	}
	if !found {
		t.Fatal("default 'oxidb' database not in list")
	}

	// Create two databases
	if err := c.CreateDatabase("go_testdb1"); err != nil {
		t.Fatalf("create_database go_testdb1: %v", err)
	}
	if err := c.CreateDatabase("go_testdb2"); err != nil {
		t.Fatalf("create_database go_testdb2: %v", err)
	}

	// Verify they appear in the list
	dbs, err = c.ListDatabases()
	if err != nil {
		t.Fatalf("list_databases: %v", err)
	}
	found1, found2 := false, false
	for _, db := range dbs {
		if db == "go_testdb1" {
			found1 = true
		}
		if db == "go_testdb2" {
			found2 = true
		}
	}
	if !found1 || !found2 {
		t.Fatalf("created databases not in list: %v", dbs)
	}
}

func TestMultiDB_CreateDuplicate(t *testing.T) {
	c := getNoAuthClient(t)
	defer c.Close()

	// go_testdb1 already exists from the previous test
	err := c.CreateDatabase("go_testdb1")
	if err == nil {
		t.Fatal("expected error creating duplicate database")
	}
}

func TestMultiDB_DatabaseIsolation(t *testing.T) {
	c := getNoAuthClient(t)
	defer c.Close()

	// Insert into go_testdb1
	if err := c.UseDatabase("go_testdb1"); err != nil {
		t.Fatalf("use_db go_testdb1: %v", err)
	}
	_, err := c.Insert("isolation_test", map[string]any{"source": "db1", "value": 100})
	if err != nil {
		t.Fatalf("insert into db1: %v", err)
	}

	// Insert into go_testdb2
	if err := c.UseDatabase("go_testdb2"); err != nil {
		t.Fatalf("use_db go_testdb2: %v", err)
	}
	_, err = c.Insert("isolation_test", map[string]any{"source": "db2", "value": 200})
	if err != nil {
		t.Fatalf("insert into db2: %v", err)
	}

	// Query go_testdb1 — should only see db1 data
	if err := c.UseDatabase("go_testdb1"); err != nil {
		t.Fatalf("use_db go_testdb1: %v", err)
	}
	docs, err := c.Find("isolation_test", map[string]any{}, nil)
	if err != nil {
		t.Fatalf("find in db1: %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("expected 1 doc in db1, got %d", len(docs))
	}
	if docs[0]["source"] != "db1" {
		t.Fatalf("expected source=db1, got %v", docs[0]["source"])
	}

	// Query go_testdb2 — should only see db2 data
	if err := c.UseDatabase("go_testdb2"); err != nil {
		t.Fatalf("use_db go_testdb2: %v", err)
	}
	docs, err = c.Find("isolation_test", map[string]any{}, nil)
	if err != nil {
		t.Fatalf("find in db2: %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("expected 1 doc in db2, got %d", len(docs))
	}
	if docs[0]["source"] != "db2" {
		t.Fatalf("expected source=db2, got %v", docs[0]["source"])
	}
}

func TestMultiDB_CollectionsArePerDatabase(t *testing.T) {
	c := getNoAuthClient(t)
	defer c.Close()

	// Create a collection only in go_testdb1
	if err := c.UseDatabase("go_testdb1"); err != nil {
		t.Fatalf("use_db: %v", err)
	}
	if err := c.CreateCollection("db1_only_col"); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	cols1, err := c.ListCollections()
	if err != nil {
		t.Fatalf("list collections db1: %v", err)
	}

	// Switch to go_testdb2 — should NOT have db1_only_col
	if err := c.UseDatabase("go_testdb2"); err != nil {
		t.Fatalf("use_db: %v", err)
	}
	cols2, err := c.ListCollections()
	if err != nil {
		t.Fatalf("list collections db2: %v", err)
	}

	foundInDb1 := false
	for _, col := range cols1 {
		if col == "db1_only_col" {
			foundInDb1 = true
		}
	}
	foundInDb2 := false
	for _, col := range cols2 {
		if col == "db1_only_col" {
			foundInDb2 = true
		}
	}

	if !foundInDb1 {
		t.Fatal("db1_only_col should exist in go_testdb1")
	}
	if foundInDb2 {
		t.Fatal("db1_only_col should NOT exist in go_testdb2")
	}
}

func TestMultiDB_IndexesArePerDatabase(t *testing.T) {
	c := getNoAuthClient(t)
	defer c.Close()

	// Create an index in go_testdb1
	if err := c.UseDatabase("go_testdb1"); err != nil {
		t.Fatalf("use_db: %v", err)
	}
	_, err := c.Insert("idx_test", map[string]any{"field_a": "hello"})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := c.CreateIndex("idx_test", "field_a"); err != nil {
		t.Fatalf("create_index: %v", err)
	}
	idxs1, err := c.ListIndexes("idx_test")
	if err != nil {
		t.Fatalf("list_indexes db1: %v", err)
	}

	// go_testdb2 should not have idx_test collection or its indexes
	if err := c.UseDatabase("go_testdb2"); err != nil {
		t.Fatalf("use_db: %v", err)
	}
	// Insert so collection exists in db2
	_, err = c.Insert("idx_test", map[string]any{"field_a": "world"})
	if err != nil {
		t.Fatalf("insert db2: %v", err)
	}
	idxs2, err := c.ListIndexes("idx_test")
	if err != nil {
		t.Fatalf("list_indexes db2: %v", err)
	}

	if len(idxs1) == 0 {
		t.Fatal("expected indexes in db1")
	}
	// db2 should have fewer indexes (no manually created index)
	if len(idxs2) >= len(idxs1) {
		t.Fatalf("expected db2 to have fewer indexes than db1: db1=%d db2=%d", len(idxs1), len(idxs2))
	}
}

func TestMultiDB_TransactionsArePerDatabase(t *testing.T) {
	c := getNoAuthClient(t)
	defer c.Close()

	if err := c.UseDatabase("go_testdb1"); err != nil {
		t.Fatalf("use_db: %v", err)
	}

	err := c.WithTransaction(func() error {
		_, err := c.Insert("tx_test", map[string]any{"tx": true, "val": 42})
		return err
	})
	if err != nil {
		t.Fatalf("transaction: %v", err)
	}

	// Verify data is in db1
	docs, err := c.Find("tx_test", map[string]any{"tx": true}, nil)
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(docs))
	}

	// Verify data is NOT in db2
	if err := c.UseDatabase("go_testdb2"); err != nil {
		t.Fatalf("use_db: %v", err)
	}
	docs, err = c.Find("tx_test", map[string]any{"tx": true}, nil)
	if err != nil {
		t.Fatalf("find db2: %v", err)
	}
	if len(docs) != 0 {
		t.Fatalf("expected 0 docs in db2, got %d", len(docs))
	}
}

func TestMultiDB_SQLCreateDropDatabase(t *testing.T) {
	c := getNoAuthClient(t)
	defer c.Close()

	// Create database via SQL
	_, err := c.SQL("CREATE DATABASE go_sqldb")
	if err != nil {
		t.Fatalf("CREATE DATABASE: %v", err)
	}

	// Verify it exists
	dbs, err := c.ListDatabases()
	if err != nil {
		t.Fatalf("list_databases: %v", err)
	}
	found := false
	for _, db := range dbs {
		if db == "go_sqldb" {
			found = true
		}
	}
	if !found {
		t.Fatal("go_sqldb not found after CREATE DATABASE")
	}

	// Drop via SQL
	_, err = c.SQL("DROP DATABASE go_sqldb")
	if err != nil {
		t.Fatalf("DROP DATABASE: %v", err)
	}

	// Verify it's gone
	dbs, err = c.ListDatabases()
	if err != nil {
		t.Fatalf("list_databases: %v", err)
	}
	for _, db := range dbs {
		if db == "go_sqldb" {
			t.Fatal("go_sqldb still exists after DROP DATABASE")
		}
	}
}

func TestMultiDB_SQLShowDatabases(t *testing.T) {
	c := getNoAuthClient(t)
	defer c.Close()

	result, err := c.SQL("SHOW DATABASES")
	if err != nil {
		t.Fatalf("SHOW DATABASES: %v", err)
	}
	// Result should be a list of objects with database_name
	arr, ok := result.([]any)
	if !ok {
		t.Fatalf("expected array result, got %T", result)
	}
	if len(arr) < 1 {
		t.Fatal("SHOW DATABASES returned empty")
	}
	// Check at least one entry has database_name
	first, _ := arr[0].(map[string]any)
	if first["database_name"] == nil {
		t.Fatalf("expected database_name field, got %v", first)
	}
}

func TestMultiDB_UseNonexistentDatabase(t *testing.T) {
	c := getNoAuthClient(t)
	defer c.Close()

	err := c.UseDatabase("this_db_does_not_exist")
	if err == nil {
		t.Fatal("expected error using nonexistent database")
	}
}

func TestMultiDB_DropDefaultDatabaseFails(t *testing.T) {
	c := getNoAuthClient(t)
	defer c.Close()

	err := c.DropDatabase("oxidb")
	if err == nil {
		t.Fatal("expected error dropping default database")
	}
}

func TestMultiDB_DropDatabase(t *testing.T) {
	c := getNoAuthClient(t)
	defer c.Close()

	// Switch back to default first
	if err := c.UseDatabase("oxidb"); err != nil {
		t.Fatalf("use_db oxidb: %v", err)
	}

	// Drop test databases
	if err := c.DropDatabase("go_testdb1"); err != nil {
		t.Fatalf("drop go_testdb1: %v", err)
	}
	if err := c.DropDatabase("go_testdb2"); err != nil {
		t.Fatalf("drop go_testdb2: %v", err)
	}

	// Verify they're gone
	dbs, err := c.ListDatabases()
	if err != nil {
		t.Fatalf("list_databases: %v", err)
	}
	for _, db := range dbs {
		if db == "go_testdb1" || db == "go_testdb2" {
			t.Fatalf("database %s still exists after drop", db)
		}
	}
}

// ===========================================================================
// Part 2: Per-database auth tests (requires auth-enabled server)
//
// Start the server with:
//   OXIDB_AUTH=true OXIDB_ADDR=127.0.0.1:4445 OXIDB_DATA=./test_auth_data oxidb-server
//
// Set OXIDB_AUTH_PORT=4445 (or the port you used).
// The default admin password is printed on first startup.
// Set OXIDB_ADMIN_PASS to that password.
// ===========================================================================

func getAdminPassword(t *testing.T) string {
	t.Helper()
	pw := os.Getenv("OXIDB_ADMIN_PASS")
	if pw == "" {
		t.Skip("OXIDB_ADMIN_PASS not set; skipping auth tests")
	}
	return pw
}

func authAsAdmin(t *testing.T) *oxidb.Client {
	t.Helper()
	pw := getAdminPassword(t)
	c := getAuthClient(t)
	role, err := c.AuthSimple("admin", pw)
	if err != nil {
		c.Close()
		t.Fatalf("admin auth: %v", err)
	}
	if role != "admin" {
		c.Close()
		t.Fatalf("expected admin role, got %q", role)
	}
	return c
}

func authAs(t *testing.T, username, password string) *oxidb.Client {
	t.Helper()
	c := getAuthClient(t)
	_, err := c.AuthSimple(username, password)
	if err != nil {
		c.Close()
		t.Fatalf("auth as %s: %v", username, err)
	}
	return c
}

func TestAuth_Setup(t *testing.T) {
	admin := authAsAdmin(t)
	defer admin.Close()

	// Cleanup from previous runs
	_ = admin.DropUser("go_reader")
	_ = admin.DropUser("go_writer")
	_ = admin.DropDatabase("go_authdb1")
	_ = admin.DropDatabase("go_authdb2")

	// Create test databases
	if err := admin.CreateDatabase("go_authdb1"); err != nil {
		t.Fatalf("create go_authdb1: %v", err)
	}
	if err := admin.CreateDatabase("go_authdb2"); err != nil {
		t.Fatalf("create go_authdb2: %v", err)
	}

	// Seed some data
	if err := admin.UseDatabase("go_authdb1"); err != nil {
		t.Fatalf("use_db: %v", err)
	}
	_, err := admin.Insert("docs", map[string]any{"name": "secret1"})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	if err := admin.UseDatabase("go_authdb2"); err != nil {
		t.Fatalf("use_db: %v", err)
	}
	_, err = admin.Insert("docs", map[string]any{"name": "secret2"})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Create users with global roles
	if err := admin.CreateUser("go_reader", "readerpass", "read"); err != nil {
		t.Fatalf("create go_reader: %v", err)
	}
	if err := admin.CreateUser("go_writer", "writerpass", "read"); err != nil {
		t.Fatalf("create go_writer: %v", err)
	}
}

func TestAuth_GlobalReadOnlyCannotWrite(t *testing.T) {
	getAdminPassword(t) // skip if no auth

	c := authAs(t, "go_reader", "readerpass")
	defer c.Close()

	if err := c.UseDatabase("go_authdb1"); err != nil {
		t.Fatalf("use_db: %v", err)
	}

	// Should be able to read
	docs, err := c.Find("docs", map[string]any{}, nil)
	if err != nil {
		t.Fatalf("read should succeed: %v", err)
	}
	if len(docs) < 1 {
		t.Fatal("expected at least 1 doc")
	}

	// Should NOT be able to write
	_, err = c.Insert("docs", map[string]any{"name": "hacker"})
	if err == nil {
		t.Fatal("expected permission denied for insert")
	}
}

func TestAuth_GrantDbRoleReadWrite(t *testing.T) {
	admin := authAsAdmin(t)
	defer admin.Close()

	// Grant readWrite on go_authdb1 to go_writer (who has global read role)
	if err := admin.GrantDbRole("go_writer", "go_authdb1", "readWrite"); err != nil {
		t.Fatalf("grant_db_role: %v", err)
	}
}

func TestAuth_PerDbWriteAllowed(t *testing.T) {
	getAdminPassword(t)

	c := authAs(t, "go_writer", "writerpass")
	defer c.Close()

	// go_writer has readWrite on go_authdb1 (per-db override)
	if err := c.UseDatabase("go_authdb1"); err != nil {
		t.Fatalf("use_db: %v", err)
	}

	// Should be able to insert in go_authdb1
	_, err := c.Insert("docs", map[string]any{"name": "go_writer_doc"})
	if err != nil {
		t.Fatalf("insert should succeed with db-level readWrite: %v", err)
	}

	// Verify the insert
	docs, err := c.Find("docs", map[string]any{"name": "go_writer_doc"}, nil)
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(docs))
	}
}

func TestAuth_PerDbWriteDeniedOnOtherDb(t *testing.T) {
	getAdminPassword(t)

	c := authAs(t, "go_writer", "writerpass")
	defer c.Close()

	// go_writer has global "read" role, no override on go_authdb2
	if err := c.UseDatabase("go_authdb2"); err != nil {
		t.Fatalf("use_db: %v", err)
	}

	// Should NOT be able to insert in go_authdb2
	_, err := c.Insert("docs", map[string]any{"name": "should_fail"})
	if err == nil {
		t.Fatal("expected permission denied for insert on go_authdb2")
	}

	// But reading should work (global read role)
	docs, err := c.Find("docs", map[string]any{}, nil)
	if err != nil {
		t.Fatalf("read should succeed: %v", err)
	}
	if len(docs) < 1 {
		t.Fatal("expected at least 1 doc")
	}
}

func TestAuth_GrantAdminOnSpecificDb(t *testing.T) {
	admin := authAsAdmin(t)
	defer admin.Close()

	// Grant admin on go_authdb2 to go_reader
	if err := admin.GrantDbRole("go_reader", "go_authdb2", "admin"); err != nil {
		t.Fatalf("grant_db_role: %v", err)
	}
}

func TestAuth_PerDbAdminCanCreateIndex(t *testing.T) {
	getAdminPassword(t)

	c := authAs(t, "go_reader", "readerpass")
	defer c.Close()

	// go_reader has admin on go_authdb2 (per-db override)
	if err := c.UseDatabase("go_authdb2"); err != nil {
		t.Fatalf("use_db: %v", err)
	}

	// Should be able to create index (admin-level)
	_, err := c.Insert("admin_test", map[string]any{"x": 1})
	if err != nil {
		t.Fatalf("insert should succeed: %v", err)
	}
	if err := c.CreateIndex("admin_test", "x"); err != nil {
		t.Fatalf("create_index should succeed with db-level admin: %v", err)
	}

	// But still read-only on go_authdb1 (no override, global read)
	if err := c.UseDatabase("go_authdb1"); err != nil {
		t.Fatalf("use_db: %v", err)
	}
	_, err = c.Insert("docs", map[string]any{"name": "nope"})
	if err == nil {
		t.Fatal("expected permission denied on go_authdb1")
	}
}

func TestAuth_ListUsersShowsDbRoles(t *testing.T) {
	admin := authAsAdmin(t)
	defer admin.Close()

	users, err := admin.ListUsers()
	if err != nil {
		t.Fatalf("list_users: %v", err)
	}

	// Find go_writer and check db_roles
	for _, u := range users {
		if u["username"] == "go_writer" {
			dbRoles, ok := u["db_roles"].(map[string]any)
			if !ok {
				t.Fatal("go_writer should have db_roles")
			}
			role, ok := dbRoles["go_authdb1"].(string)
			if !ok || role != "readWrite" {
				t.Fatalf("expected readWrite on go_authdb1, got %v", dbRoles)
			}
			return
		}
	}
	t.Fatal("go_writer not found in user list")
}

func TestAuth_RevokeDbRole(t *testing.T) {
	admin := authAsAdmin(t)
	defer admin.Close()

	// Revoke go_writer's readWrite on go_authdb1
	if err := admin.RevokeDbRole("go_writer", "go_authdb1"); err != nil {
		t.Fatalf("revoke_db_role: %v", err)
	}

	// Now go_writer should be back to global "read" on go_authdb1
	c := authAs(t, "go_writer", "writerpass")
	defer c.Close()

	if err := c.UseDatabase("go_authdb1"); err != nil {
		t.Fatalf("use_db: %v", err)
	}

	_, err := c.Insert("docs", map[string]any{"name": "after_revoke"})
	if err == nil {
		t.Fatal("expected permission denied after revoke")
	}

	// But reading should still work
	docs, err := c.Find("docs", map[string]any{}, nil)
	if err != nil {
		t.Fatalf("read should still work: %v", err)
	}
	if len(docs) < 1 {
		t.Fatal("expected docs to be readable")
	}
}

func TestAuth_RevokeNonexistentOverrideFails(t *testing.T) {
	admin := authAsAdmin(t)
	defer admin.Close()

	// go_writer no longer has a db override on go_authdb1
	err := admin.RevokeDbRole("go_writer", "go_authdb1")
	if err == nil {
		t.Fatal("expected error revoking non-existent override")
	}
}

func TestAuth_MultipleDbRolesOnSameUser(t *testing.T) {
	admin := authAsAdmin(t)
	defer admin.Close()

	// Grant different roles on different databases
	if err := admin.GrantDbRole("go_reader", "go_authdb1", "readWrite"); err != nil {
		t.Fatalf("grant readWrite on db1: %v", err)
	}
	// go_reader already has admin on go_authdb2 from previous test

	c := authAs(t, "go_reader", "readerpass")
	defer c.Close()

	// go_authdb1: readWrite — can insert
	if err := c.UseDatabase("go_authdb1"); err != nil {
		t.Fatalf("use_db: %v", err)
	}
	_, err := c.Insert("multi_role_test", map[string]any{"db": "db1"})
	if err != nil {
		t.Fatalf("insert in db1 should succeed: %v", err)
	}

	// go_authdb2: admin — can insert and create indexes
	if err := c.UseDatabase("go_authdb2"); err != nil {
		t.Fatalf("use_db: %v", err)
	}
	_, err = c.Insert("multi_role_test", map[string]any{"db": "db2"})
	if err != nil {
		t.Fatalf("insert in db2 should succeed: %v", err)
	}

	// default oxidb: global read — cannot insert
	if err := c.UseDatabase("oxidb"); err != nil {
		t.Fatalf("use_db: %v", err)
	}
	_, err = c.Insert("multi_role_test", map[string]any{"db": "default"})
	if err == nil {
		t.Fatal("expected permission denied on default db (global read role)")
	}
}

func TestAuth_UnauthenticatedCannotManageDb(t *testing.T) {
	getAdminPassword(t)

	// Connect without authenticating
	c := getAuthClient(t)
	defer c.Close()

	// Should be denied
	err := c.CreateDatabase("hacker_db")
	if err == nil {
		t.Fatal("expected auth error for unauthenticated create_database")
	}

	err = c.UseDatabase("go_authdb1")
	if err == nil {
		t.Fatal("expected auth error for unauthenticated use_db")
	}
}

func TestAuth_NonAdminCannotGrantDbRole(t *testing.T) {
	getAdminPassword(t)

	c := authAs(t, "go_writer", "writerpass")
	defer c.Close()

	err := c.GrantDbRole("go_reader", "go_authdb1", "admin")
	if err == nil {
		t.Fatal("expected permission denied for non-admin grant_db_role")
	}
}

func TestAuth_Cleanup(t *testing.T) {
	admin := authAsAdmin(t)
	defer admin.Close()

	// Revoke remaining overrides
	_ = admin.RevokeDbRole("go_reader", "go_authdb1")
	_ = admin.RevokeDbRole("go_reader", "go_authdb2")

	// Drop users
	_ = admin.DropUser("go_reader")
	_ = admin.DropUser("go_writer")

	// Drop test databases
	_ = admin.DropDatabase("go_authdb1")
	_ = admin.DropDatabase("go_authdb2")

	// Verify cleanup
	dbs, err := admin.ListDatabases()
	if err != nil {
		t.Fatalf("list_databases: %v", err)
	}
	for _, db := range dbs {
		if db == "go_authdb1" || db == "go_authdb2" {
			t.Fatalf("test database %s still exists after cleanup", db)
		}
	}

	fmt.Println("auth integration test cleanup complete")
}
