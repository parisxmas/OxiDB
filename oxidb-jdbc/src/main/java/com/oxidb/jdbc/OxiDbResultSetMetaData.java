package com.oxidb.jdbc;

import java.sql.*;
import java.util.*;

/**
 * JDBC ResultSetMetaData that infers column types from OxiDB document values.
 */
public class OxiDbResultSetMetaData implements ResultSetMetaData {

    private final List<String> columns;
    private final List<Map<String, Object>> rows;

    OxiDbResultSetMetaData(List<String> columns, List<Map<String, Object>> rows) {
        this.columns = columns;
        this.rows = rows;
    }

    @Override
    public int getColumnCount() {
        return columns.size();
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        checkColumn(column);
        return columns.get(column - 1);
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumnName(column);
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        checkColumn(column);
        Object sample = findSample(column);
        if (sample == null) return Types.VARCHAR;
        if (sample instanceof Boolean) return Types.BOOLEAN;
        if (sample instanceof Long) return Types.BIGINT;
        if (sample instanceof Integer) return Types.INTEGER;
        if (sample instanceof Double || sample instanceof Float) return Types.DOUBLE;
        if (sample instanceof Number) return Types.NUMERIC;
        if (sample instanceof List) return Types.ARRAY;
        if (sample instanceof Map) return Types.JAVA_OBJECT;
        return Types.VARCHAR;
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        int type = getColumnType(column);
        return switch (type) {
            case Types.BOOLEAN -> "BOOLEAN";
            case Types.BIGINT -> "BIGINT";
            case Types.INTEGER -> "INTEGER";
            case Types.DOUBLE -> "DOUBLE";
            case Types.NUMERIC -> "NUMERIC";
            case Types.ARRAY -> "ARRAY";
            case Types.JAVA_OBJECT -> "JSON";
            default -> "VARCHAR";
        };
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        int type = getColumnType(column);
        return switch (type) {
            case Types.BOOLEAN -> "java.lang.Boolean";
            case Types.BIGINT -> "java.lang.Long";
            case Types.INTEGER -> "java.lang.Integer";
            case Types.DOUBLE -> "java.lang.Double";
            case Types.NUMERIC -> "java.lang.Number";
            case Types.ARRAY -> "java.util.List";
            case Types.JAVA_OBJECT -> "java.util.Map";
            default -> "java.lang.String";
        };
    }

    private Object findSample(int column) {
        String colName = columns.get(column - 1);
        for (Map<String, Object> row : rows) {
            Object v = row.get(colName);
            if (v != null) return v;
        }
        return null;
    }

    private void checkColumn(int column) throws SQLException {
        if (column < 1 || column > columns.size())
            throw new SQLException("Invalid column index: " + column);
    }

    @Override public boolean isAutoIncrement(int column) { return false; }
    @Override public boolean isCaseSensitive(int column) { return true; }
    @Override public boolean isSearchable(int column) { return true; }
    @Override public boolean isCurrency(int column) { return false; }
    @Override public int isNullable(int column) { return ResultSetMetaData.columnNullable; }
    @Override public boolean isSigned(int column) { return true; }
    @Override public int getColumnDisplaySize(int column) { return 256; }
    @Override public String getSchemaName(int column) { return ""; }
    @Override public int getPrecision(int column) { return 0; }
    @Override public int getScale(int column) { return 0; }
    @Override public String getTableName(int column) { return ""; }
    @Override public String getCatalogName(int column) { return "oxidb"; }
    @Override public boolean isReadOnly(int column) { return true; }
    @Override public boolean isWritable(int column) { return false; }
    @Override public boolean isDefinitelyWritable(int column) { return false; }

    @Override
    public boolean isWrapperFor(Class<?> iface) { return false; }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException { throw new SQLException("Not a wrapper"); }
}
