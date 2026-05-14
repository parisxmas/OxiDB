package com.oxidb.jdbc;

import java.sql.*;
import java.util.*;

/**
 * JDBC DatabaseMetaData for OxiDB. Provides catalog/schema/table info
 * by querying OxiDB's list_collections command.
 */
public class OxiDbDatabaseMetaData implements DatabaseMetaData {

    private final OxiDbConnection connection;

    OxiDbDatabaseMetaData(OxiDbConnection connection) {
        this.connection = connection;
    }

    @Override
    public String getDatabaseProductName() { return "OxiDB"; }

    @Override
    public String getDatabaseProductVersion() { return "0.18.0"; }

    @Override
    public String getDriverName() { return "OxiDB JDBC Driver"; }

    @Override
    public String getDriverVersion() { return "0.1.0"; }

    @Override
    public int getDriverMajorVersion() { return 0; }

    @Override
    public int getDriverMinorVersion() { return 1; }

    @Override
    public int getDatabaseMajorVersion() { return 0; }

    @Override
    public int getDatabaseMinorVersion() { return 18; }

    @Override
    public int getJDBCMajorVersion() { return 4; }

    @Override
    public int getJDBCMinorVersion() { return 3; }

    @Override
    public Connection getConnection() { return connection; }

    @Override
    public String getURL() { return "jdbc:oxidb://"; }

    @Override
    public String getUserName() { return ""; }

    @Override
    public boolean isReadOnly() { return false; }

    @Override
    public String getIdentifierQuoteString() { return "\""; }

    @Override
    public String getCatalogSeparator() { return "."; }

    @Override
    public String getCatalogTerm() { return "database"; }

    @Override
    public String getSchemaTerm() { return "schema"; }

    @Override
    public String getProcedureTerm() { return "procedure"; }

    @Override
    @SuppressWarnings("unchecked")
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        // Query OxiDB for collections
        var resp = connection.sendCommand(Map.of("cmd", "list_collections"));
        List<Map<String, Object>> tableRows = new ArrayList<>();

        if (resp.ok() && resp.value() instanceof List<?> list) {
            for (Object item : list) {
                String name = item.toString();
                if (tableNamePattern != null && !tableNamePattern.equals("%")) {
                    String pattern = tableNamePattern.replace("%", ".*").replace("_", ".");
                    if (!name.matches(pattern)) continue;
                }
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("TABLE_CAT", "oxidb");
                row.put("TABLE_SCHEM", null);
                row.put("TABLE_NAME", name);
                row.put("TABLE_TYPE", "TABLE");
                row.put("REMARKS", "OxiDB collection");
                row.put("TYPE_CAT", null);
                row.put("TYPE_SCHEM", null);
                row.put("TYPE_NAME", null);
                row.put("SELF_REFERENCING_COL_NAME", null);
                row.put("REF_GENERATION", null);
                tableRows.add(row);
            }
        }

        return new OxiDbResultSet(tableRows, null);
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("TABLE_CAT", "oxidb");
        return new OxiDbResultSet(List.of(row), null);
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return new OxiDbResultSet(Collections.emptyList(), null);
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        return getSchemas();
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("TABLE_TYPE", "TABLE");
        return new OxiDbResultSet(List.of(row), null);
    }

    @Override
    @SuppressWarnings("unchecked")
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        // For document databases, columns are dynamic. Query a sample doc.
        List<Map<String, Object>> colRows = new ArrayList<>();

        // Get tables that match
        var tablesRs = getTables(catalog, schemaPattern, tableNamePattern, null);
        while (tablesRs.next()) {
            String tableName = tablesRs.getString("TABLE_NAME");
            // Sample one document to get field names
            try {
                Map<String, Object> cmd = new LinkedHashMap<>();
                cmd.put("cmd", "sql");
                cmd.put("query", "SELECT * FROM " + tableName + " LIMIT 1");
                var resp = connection.sendCommand(cmd);

                if (resp.ok() && resp.value() instanceof List<?> list && !list.isEmpty()) {
                    Object first = list.get(0);
                    if (first instanceof Map<?, ?> doc) {
                        int ordinal = 1;
                        for (var entry : doc.entrySet()) {
                            String colName = entry.getKey().toString();
                            if (columnNamePattern != null && !columnNamePattern.equals("%")) {
                                String pattern = columnNamePattern.replace("%", ".*").replace("_", ".");
                                if (!colName.matches(pattern)) continue;
                            }
                            Map<String, Object> colRow = new LinkedHashMap<>();
                            colRow.put("TABLE_CAT", "oxidb");
                            colRow.put("TABLE_SCHEM", null);
                            colRow.put("TABLE_NAME", tableName);
                            colRow.put("COLUMN_NAME", colName);
                            colRow.put("DATA_TYPE", inferSqlType(entry.getValue()));
                            colRow.put("TYPE_NAME", inferTypeName(entry.getValue()));
                            colRow.put("COLUMN_SIZE", 256);
                            colRow.put("BUFFER_LENGTH", null);
                            colRow.put("DECIMAL_DIGITS", 0);
                            colRow.put("NUM_PREC_RADIX", 10);
                            colRow.put("NULLABLE", (long) DatabaseMetaData.columnNullable);
                            colRow.put("REMARKS", null);
                            colRow.put("COLUMN_DEF", null);
                            colRow.put("SQL_DATA_TYPE", null);
                            colRow.put("SQL_DATETIME_SUB", null);
                            colRow.put("CHAR_OCTET_LENGTH", 256);
                            colRow.put("ORDINAL_POSITION", (long) ordinal++);
                            colRow.put("IS_NULLABLE", "YES");
                            colRow.put("SCOPE_CATALOG", null);
                            colRow.put("SCOPE_SCHEMA", null);
                            colRow.put("SCOPE_TABLE", null);
                            colRow.put("SOURCE_DATA_TYPE", null);
                            colRow.put("IS_AUTOINCREMENT", "NO");
                            colRow.put("IS_GENERATEDCOLUMN", "NO");
                            colRows.add(colRow);
                        }
                    }
                }
            } catch (SQLException e) {
                // Skip if we can't sample this collection
            }
        }

        return new OxiDbResultSet(colRows, null);
    }

    private long inferSqlType(Object value) {
        if (value == null) return Types.VARCHAR;
        if (value instanceof Boolean) return Types.BOOLEAN;
        if (value instanceof Long) return Types.BIGINT;
        if (value instanceof Integer) return Types.INTEGER;
        if (value instanceof Double || value instanceof Float) return Types.DOUBLE;
        if (value instanceof List) return Types.ARRAY;
        if (value instanceof Map) return Types.JAVA_OBJECT;
        return Types.VARCHAR;
    }

    private String inferTypeName(Object value) {
        if (value == null) return "VARCHAR";
        if (value instanceof Boolean) return "BOOLEAN";
        if (value instanceof Long) return "BIGINT";
        if (value instanceof Integer) return "INTEGER";
        if (value instanceof Double || value instanceof Float) return "DOUBLE";
        if (value instanceof List) return "ARRAY";
        if (value instanceof Map) return "JSON";
        return "VARCHAR";
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        // OxiDB uses _id as the primary key for every collection
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("TABLE_CAT", "oxidb");
        row.put("TABLE_SCHEM", null);
        row.put("TABLE_NAME", table);
        row.put("COLUMN_NAME", "_id");
        row.put("KEY_SEQ", 1L);
        row.put("PK_NAME", "PRIMARY");
        return new OxiDbResultSet(List.of(row), null);
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        List<Map<String, Object>> types = new ArrayList<>();
        addTypeInfo(types, "VARCHAR", Types.VARCHAR, 65535, "'", "'");
        addTypeInfo(types, "BOOLEAN", Types.BOOLEAN, 1, null, null);
        addTypeInfo(types, "BIGINT", Types.BIGINT, 19, null, null);
        addTypeInfo(types, "DOUBLE", Types.DOUBLE, 15, null, null);
        addTypeInfo(types, "JSON", Types.JAVA_OBJECT, 0, null, null);
        return new OxiDbResultSet(types, null);
    }

    private void addTypeInfo(List<Map<String, Object>> types, String name, int sqlType, int precision, String prefix, String suffix) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("TYPE_NAME", name);
        row.put("DATA_TYPE", (long) sqlType);
        row.put("PRECISION", (long) precision);
        row.put("LITERAL_PREFIX", prefix);
        row.put("LITERAL_SUFFIX", suffix);
        row.put("CREATE_PARAMS", null);
        row.put("NULLABLE", (long) DatabaseMetaData.typeNullable);
        row.put("CASE_SENSITIVE", true);
        row.put("SEARCHABLE", (long) DatabaseMetaData.typeSearchable);
        row.put("UNSIGNED_ATTRIBUTE", false);
        row.put("FIXED_PREC_SCALE", false);
        row.put("AUTO_INCREMENT", false);
        row.put("LOCAL_TYPE_NAME", name);
        row.put("MINIMUM_SCALE", 0L);
        row.put("MAXIMUM_SCALE", 0L);
        row.put("SQL_DATA_TYPE", null);
        row.put("SQL_DATETIME_SUB", null);
        row.put("NUM_PREC_RADIX", 10L);
        types.add(row);
    }

    // ── Feature support flags ─────────────────────────────────

    @Override public boolean allProceduresAreCallable() { return false; }
    @Override public boolean allTablesAreSelectable() { return true; }
    @Override public boolean nullsAreSortedHigh() { return false; }
    @Override public boolean nullsAreSortedLow() { return true; }
    @Override public boolean nullsAreSortedAtStart() { return false; }
    @Override public boolean nullsAreSortedAtEnd() { return false; }
    @Override public boolean usesLocalFiles() { return true; }
    @Override public boolean usesLocalFilePerTable() { return true; }
    @Override public boolean supportsMixedCaseIdentifiers() { return true; }
    @Override public boolean storesUpperCaseIdentifiers() { return false; }
    @Override public boolean storesLowerCaseIdentifiers() { return false; }
    @Override public boolean storesMixedCaseIdentifiers() { return true; }
    @Override public boolean supportsMixedCaseQuotedIdentifiers() { return true; }
    @Override public boolean storesUpperCaseQuotedIdentifiers() { return false; }
    @Override public boolean storesLowerCaseQuotedIdentifiers() { return false; }
    @Override public boolean storesMixedCaseQuotedIdentifiers() { return true; }
    @Override public String getSQLKeywords() { return ""; }
    @Override public String getNumericFunctions() { return ""; }
    @Override public String getStringFunctions() { return ""; }
    @Override public String getSystemFunctions() { return ""; }
    @Override public String getTimeDateFunctions() { return ""; }
    @Override public String getSearchStringEscape() { return "\\"; }
    @Override public String getExtraNameCharacters() { return ""; }
    @Override public boolean supportsAlterTableWithAddColumn() { return false; }
    @Override public boolean supportsAlterTableWithDropColumn() { return false; }
    @Override public boolean supportsColumnAliasing() { return true; }
    @Override public boolean nullPlusNonNullIsNull() { return true; }
    @Override public boolean supportsConvert() { return false; }
    @Override public boolean supportsConvert(int fromType, int toType) { return false; }
    @Override public boolean supportsTableCorrelationNames() { return false; }
    @Override public boolean supportsDifferentTableCorrelationNames() { return false; }
    @Override public boolean supportsExpressionsInOrderBy() { return true; }
    @Override public boolean supportsOrderByUnrelated() { return true; }
    @Override public boolean supportsGroupBy() { return true; }
    @Override public boolean supportsGroupByUnrelated() { return true; }
    @Override public boolean supportsGroupByBeyondSelect() { return true; }
    @Override public boolean supportsLikeEscapeClause() { return true; }
    @Override public boolean supportsMultipleResultSets() { return false; }
    @Override public boolean supportsMultipleTransactions() { return false; }
    @Override public boolean supportsNonNullableColumns() { return false; }
    @Override public boolean supportsMinimumSQLGrammar() { return true; }
    @Override public boolean supportsCoreSQLGrammar() { return false; }
    @Override public boolean supportsExtendedSQLGrammar() { return false; }
    @Override public boolean supportsANSI92EntryLevelSQL() { return false; }
    @Override public boolean supportsANSI92IntermediateSQL() { return false; }
    @Override public boolean supportsANSI92FullSQL() { return false; }
    @Override public boolean supportsIntegrityEnhancementFacility() { return false; }
    @Override public boolean supportsOuterJoins() { return false; }
    @Override public boolean supportsFullOuterJoins() { return false; }
    @Override public boolean supportsLimitedOuterJoins() { return false; }
    @Override public boolean isCatalogAtStart() { return true; }
    @Override public boolean supportsSchemasInDataManipulation() { return false; }
    @Override public boolean supportsSchemasInProcedureCalls() { return false; }
    @Override public boolean supportsSchemasInTableDefinitions() { return false; }
    @Override public boolean supportsSchemasInIndexDefinitions() { return false; }
    @Override public boolean supportsSchemasInPrivilegeDefinitions() { return false; }
    @Override public boolean supportsCatalogsInDataManipulation() { return false; }
    @Override public boolean supportsCatalogsInProcedureCalls() { return false; }
    @Override public boolean supportsCatalogsInTableDefinitions() { return false; }
    @Override public boolean supportsCatalogsInIndexDefinitions() { return false; }
    @Override public boolean supportsCatalogsInPrivilegeDefinitions() { return false; }
    @Override public boolean supportsPositionedDelete() { return false; }
    @Override public boolean supportsPositionedUpdate() { return false; }
    @Override public boolean supportsSelectForUpdate() { return false; }
    @Override public boolean supportsStoredProcedures() { return false; }
    @Override public boolean supportsSubqueriesInComparisons() { return false; }
    @Override public boolean supportsSubqueriesInExists() { return false; }
    @Override public boolean supportsSubqueriesInIns() { return false; }
    @Override public boolean supportsSubqueriesInQuantifieds() { return false; }
    @Override public boolean supportsCorrelatedSubqueries() { return false; }
    @Override public boolean supportsUnion() { return false; }
    @Override public boolean supportsUnionAll() { return false; }
    @Override public boolean supportsOpenCursorsAcrossCommit() { return false; }
    @Override public boolean supportsOpenCursorsAcrossRollback() { return false; }
    @Override public boolean supportsOpenStatementsAcrossCommit() { return true; }
    @Override public boolean supportsOpenStatementsAcrossRollback() { return true; }
    @Override public int getMaxBinaryLiteralLength() { return 0; }
    @Override public int getMaxCharLiteralLength() { return 0; }
    @Override public int getMaxColumnNameLength() { return 256; }
    @Override public int getMaxColumnsInGroupBy() { return 0; }
    @Override public int getMaxColumnsInIndex() { return 0; }
    @Override public int getMaxColumnsInOrderBy() { return 0; }
    @Override public int getMaxColumnsInSelect() { return 0; }
    @Override public int getMaxColumnsInTable() { return 0; }
    @Override public int getMaxConnections() { return 0; }
    @Override public int getMaxCursorNameLength() { return 0; }
    @Override public int getMaxIndexLength() { return 0; }
    @Override public int getMaxSchemaNameLength() { return 0; }
    @Override public int getMaxProcedureNameLength() { return 0; }
    @Override public int getMaxCatalogNameLength() { return 256; }
    @Override public int getMaxRowSize() { return 16 * 1024 * 1024; }
    @Override public boolean doesMaxRowSizeIncludeBlobs() { return true; }
    @Override public int getMaxStatementLength() { return 16 * 1024 * 1024; }
    @Override public int getMaxStatements() { return 0; }
    @Override public int getMaxTableNameLength() { return 256; }
    @Override public int getMaxTablesInSelect() { return 1; }
    @Override public int getMaxUserNameLength() { return 256; }
    @Override public int getDefaultTransactionIsolation() { return Connection.TRANSACTION_READ_COMMITTED; }
    @Override public boolean supportsTransactions() { return true; }
    @Override public boolean supportsTransactionIsolationLevel(int level) { return level == Connection.TRANSACTION_READ_COMMITTED; }
    @Override public boolean supportsDataDefinitionAndDataManipulationTransactions() { return false; }
    @Override public boolean supportsDataManipulationTransactionsOnly() { return false; }
    @Override public boolean dataDefinitionCausesTransactionCommit() { return false; }
    @Override public boolean dataDefinitionIgnoredInTransactions() { return false; }
    @Override public boolean supportsResultSetType(int type) { return type == ResultSet.TYPE_FORWARD_ONLY; }
    @Override public boolean supportsResultSetConcurrency(int type, int concurrency) { return concurrency == ResultSet.CONCUR_READ_ONLY; }
    @Override public boolean ownUpdatesAreVisible(int type) { return false; }
    @Override public boolean ownDeletesAreVisible(int type) { return false; }
    @Override public boolean ownInsertsAreVisible(int type) { return false; }
    @Override public boolean othersUpdatesAreVisible(int type) { return false; }
    @Override public boolean othersDeletesAreVisible(int type) { return false; }
    @Override public boolean othersInsertsAreVisible(int type) { return false; }
    @Override public boolean updatesAreDetected(int type) { return false; }
    @Override public boolean deletesAreDetected(int type) { return false; }
    @Override public boolean insertsAreDetected(int type) { return false; }
    @Override public boolean supportsBatchUpdates() { return false; }
    @Override public boolean supportsSavepoints() { return false; }
    @Override public boolean supportsNamedParameters() { return false; }
    @Override public boolean supportsMultipleOpenResults() { return false; }
    @Override public boolean supportsGetGeneratedKeys() { return false; }
    @Override public boolean supportsResultSetHoldability(int holdability) { return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT; }
    @Override public int getResultSetHoldability() { return ResultSet.HOLD_CURSORS_OVER_COMMIT; }
    @Override public int getSQLStateType() { return DatabaseMetaData.sqlStateSQL; }
    @Override public boolean locatorsUpdateCopy() { return false; }
    @Override public boolean supportsStatementPooling() { return false; }
    @Override public RowIdLifetime getRowIdLifetime() { return RowIdLifetime.ROWID_UNSUPPORTED; }
    @Override public boolean supportsStoredFunctionsUsingCallSyntax() { return false; }
    @Override public boolean autoCommitFailureClosesAllResultSets() { return false; }
    @Override public boolean generatedKeyAlwaysReturned() { return false; }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        return new OxiDbResultSet(Collections.emptyList(), null);
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        return new OxiDbResultSet(Collections.emptyList(), null);
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        return new OxiDbResultSet(Collections.emptyList(), null);
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return new OxiDbResultSet(Collections.emptyList(), null);
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        return new OxiDbResultSet(Collections.emptyList(), null);
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return new OxiDbResultSet(Collections.emptyList(), null);
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return new OxiDbResultSet(Collections.emptyList(), null);
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return new OxiDbResultSet(Collections.emptyList(), null);
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        return new OxiDbResultSet(Collections.emptyList(), null);
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        return new OxiDbResultSet(Collections.emptyList(), null);
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        return new OxiDbResultSet(Collections.emptyList(), null);
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return new OxiDbResultSet(Collections.emptyList(), null);
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return new OxiDbResultSet(Collections.emptyList(), null);
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        return new OxiDbResultSet(Collections.emptyList(), null);
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return new OxiDbResultSet(Collections.emptyList(), null);
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        return new OxiDbResultSet(Collections.emptyList(), null);
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        return new OxiDbResultSet(Collections.emptyList(), null);
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return new OxiDbResultSet(Collections.emptyList(), null);
    }

    @Override
    public long getMaxLogicalLobSize() { return 0; }

    @Override
    public boolean supportsRefCursors() { return false; }

    @Override
    public boolean isWrapperFor(Class<?> iface) { return false; }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException { throw new SQLException("Not a wrapper"); }
}
