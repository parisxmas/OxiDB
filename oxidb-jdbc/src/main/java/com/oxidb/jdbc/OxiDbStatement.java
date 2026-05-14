package com.oxidb.jdbc;

import java.sql.*;
import java.util.*;

/**
 * JDBC Statement that sends SQL to OxiDB via OxiWire.
 */
public class OxiDbStatement implements Statement {

    protected final OxiDbConnection connection;
    private ResultSet currentResultSet;
    private int updateCount = -1;
    private boolean closed;
    private int maxRows;

    OxiDbStatement(OxiDbConnection connection) {
        this.connection = connection;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        checkClosed();
        execute(sql);
        if (currentResultSet == null) {
            throw new SQLException("Query did not return a result set");
        }
        return currentResultSet;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkClosed();
        execute(sql);
        return Math.max(updateCount, 0);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean execute(String sql) throws SQLException {
        checkClosed();
        currentResultSet = null;
        updateCount = -1;

        Map<String, Object> cmd = new LinkedHashMap<>();
        cmd.put("cmd", "sql");
        cmd.put("query", sql);

        var resp = connection.sendCommand(cmd);
        if (!resp.ok()) {
            throw new SQLException("OxiDB error: " + resp.errorMessage());
        }

        Object value = resp.value();

        if (value instanceof List<?> list) {
            // SELECT result — array of documents (maps)
            List<Map<String, Object>> rows = new ArrayList<>();
            for (Object item : list) {
                if (item instanceof Map<?, ?> m) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    for (var e : m.entrySet()) {
                        row.put(e.getKey().toString(), e.getValue());
                    }
                    rows.add(row);
                }
            }
            if (maxRows > 0 && rows.size() > maxRows) {
                rows = rows.subList(0, maxRows);
            }
            currentResultSet = new OxiDbResultSet(rows, this);
            return true;
        } else if (value instanceof Map<?, ?> map) {
            // Could be a single document result or status
            Object data = map.get("data");
            if (data instanceof List<?> list) {
                List<Map<String, Object>> rows = new ArrayList<>();
                for (Object item : list) {
                    if (item instanceof Map<?, ?> m) {
                        Map<String, Object> row = new LinkedHashMap<>();
                        for (var e : m.entrySet()) {
                            row.put(e.getKey().toString(), e.getValue());
                        }
                        rows.add(row);
                    }
                }
                if (maxRows > 0 && rows.size() > maxRows) {
                    rows = rows.subList(0, maxRows);
                }
                currentResultSet = new OxiDbResultSet(rows, this);
                return true;
            }
            // Check for count result
            Object count = map.get("count");
            if (count instanceof Number n) {
                updateCount = n.intValue();
                return false;
            }
            // Check for affected/modified
            Object affected = map.get("affected");
            if (affected instanceof Number n) {
                updateCount = n.intValue();
                return false;
            }
            // DDL message
            Object msg = map.get("message");
            if (msg != null) {
                updateCount = 0;
                return false;
            }
            // Single doc — wrap in list
            Map<String, Object> row = new LinkedHashMap<>();
            for (var e : map.entrySet()) {
                row.put(e.getKey().toString(), e.getValue());
            }
            currentResultSet = new OxiDbResultSet(List.of(row), this);
            return true;
        } else if (value instanceof Number n) {
            updateCount = n.intValue();
            return false;
        } else if (value instanceof String s) {
            // DDL result like "Table created"
            updateCount = 0;
            return false;
        } else {
            updateCount = 0;
            return false;
        }
    }

    protected void checkClosed() throws SQLException {
        if (closed) throw new SQLException("Statement is closed");
    }

    @Override
    public ResultSet getResultSet() { return currentResultSet; }

    @Override
    public int getUpdateCount() { return updateCount; }

    @Override
    public boolean getMoreResults() { return false; }

    @Override
    public boolean getMoreResults(int current) { return false; }

    @Override
    public void close() { closed = true; }

    @Override
    public boolean isClosed() { return closed; }

    @Override
    public Connection getConnection() { return connection; }

    @Override
    public void setMaxRows(int max) { this.maxRows = max; }

    @Override
    public int getMaxRows() { return maxRows; }

    @Override
    public void setQueryTimeout(int seconds) {}

    @Override
    public int getQueryTimeout() { return 0; }

    @Override
    public void cancel() {}

    @Override
    public SQLWarning getWarnings() { return null; }

    @Override
    public void clearWarnings() {}

    @Override
    public void setCursorName(String name) {}

    @Override
    public void setFetchDirection(int direction) {}

    @Override
    public int getFetchDirection() { return ResultSet.FETCH_FORWARD; }

    @Override
    public void setFetchSize(int rows) {}

    @Override
    public int getFetchSize() { return 0; }

    @Override
    public int getResultSetConcurrency() { return ResultSet.CONCUR_READ_ONLY; }

    @Override
    public int getResultSetType() { return ResultSet.TYPE_FORWARD_ONLY; }

    @Override
    public void addBatch(String sql) throws SQLException { throw new SQLFeatureNotSupportedException(); }

    @Override
    public void clearBatch() {}

    @Override
    public int[] executeBatch() throws SQLException { throw new SQLFeatureNotSupportedException(); }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return new OxiDbResultSet(Collections.emptyList(), this);
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException { return executeUpdate(sql); }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException { return executeUpdate(sql); }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException { return executeUpdate(sql); }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException { return execute(sql); }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException { return execute(sql); }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException { return execute(sql); }

    @Override
    public int getResultSetHoldability() { return ResultSet.HOLD_CURSORS_OVER_COMMIT; }

    @Override
    public void setPoolable(boolean poolable) {}

    @Override
    public boolean isPoolable() { return false; }

    @Override
    public void closeOnCompletion() {}

    @Override
    public boolean isCloseOnCompletion() { return false; }

    @Override
    public void setMaxFieldSize(int max) {}

    @Override
    public int getMaxFieldSize() { return 0; }

    @Override
    public void setEscapeProcessing(boolean enable) {}

    @Override
    public boolean isWrapperFor(Class<?> iface) { return false; }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException { throw new SQLException("Not a wrapper"); }
}
