package com.oxidb.jdbc;

import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.Calendar;

/**
 * JDBC PreparedStatement for OxiDB. Since OxiDB SQL doesn't support
 * parameterized queries natively, we substitute parameters into the SQL string.
 */
public class OxiDbPreparedStatement extends OxiDbStatement implements PreparedStatement {

    private final String sql;
    private final Map<Integer, Object> parameters = new TreeMap<>();

    OxiDbPreparedStatement(OxiDbConnection connection, String sql) {
        super(connection);
        this.sql = sql;
    }

    private String buildSql() {
        StringBuilder sb = new StringBuilder();
        int paramIdx = 1;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '?' && !inQuote(sql, i)) {
                Object val = parameters.get(paramIdx++);
                if (val == null) {
                    sb.append("NULL");
                } else if (val instanceof Number) {
                    sb.append(val);
                } else if (val instanceof Boolean b) {
                    sb.append(b ? "TRUE" : "FALSE");
                } else {
                    // Escape single quotes
                    String s = val.toString().replace("'", "''");
                    sb.append('\'').append(s).append('\'');
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private static boolean inQuote(String sql, int pos) {
        boolean inSingle = false;
        for (int i = 0; i < pos; i++) {
            if (sql.charAt(i) == '\'' && (i == 0 || sql.charAt(i - 1) != '\\')) {
                inSingle = !inSingle;
            }
        }
        return inSingle;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return executeQuery(buildSql());
    }

    @Override
    public int executeUpdate() throws SQLException {
        return executeUpdate(buildSql());
    }

    @Override
    public boolean execute() throws SQLException {
        return execute(buildSql());
    }

    @Override
    public void clearParameters() {
        parameters.clear();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return null; // Not available before execution
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void setNull(int parameterIndex, int sqlType) { parameters.put(parameterIndex, null); }
    @Override public void setBoolean(int parameterIndex, boolean x) { parameters.put(parameterIndex, x); }
    @Override public void setByte(int parameterIndex, byte x) { parameters.put(parameterIndex, (int) x); }
    @Override public void setShort(int parameterIndex, short x) { parameters.put(parameterIndex, (int) x); }
    @Override public void setInt(int parameterIndex, int x) { parameters.put(parameterIndex, x); }
    @Override public void setLong(int parameterIndex, long x) { parameters.put(parameterIndex, x); }
    @Override public void setFloat(int parameterIndex, float x) { parameters.put(parameterIndex, x); }
    @Override public void setDouble(int parameterIndex, double x) { parameters.put(parameterIndex, x); }
    @Override public void setBigDecimal(int parameterIndex, BigDecimal x) { parameters.put(parameterIndex, x); }
    @Override public void setString(int parameterIndex, String x) { parameters.put(parameterIndex, x); }
    @Override public void setBytes(int parameterIndex, byte[] x) { parameters.put(parameterIndex, x != null ? new String(x) : null); }
    @Override public void setDate(int parameterIndex, Date x) { parameters.put(parameterIndex, x != null ? x.toString() : null); }
    @Override public void setTime(int parameterIndex, Time x) { parameters.put(parameterIndex, x != null ? x.toString() : null); }
    @Override public void setTimestamp(int parameterIndex, Timestamp x) { parameters.put(parameterIndex, x != null ? x.toString() : null); }
    @Override public void setDate(int parameterIndex, Date x, Calendar cal) { setDate(parameterIndex, x); }
    @Override public void setTime(int parameterIndex, Time x, Calendar cal) { setTime(parameterIndex, x); }
    @Override public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) { setTimestamp(parameterIndex, x); }
    @Override public void setObject(int parameterIndex, Object x) { parameters.put(parameterIndex, x); }
    @Override public void setObject(int parameterIndex, Object x, int targetSqlType) { setObject(parameterIndex, x); }
    @Override public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) { setObject(parameterIndex, x); }
    @Override public void setNull(int parameterIndex, int sqlType, String typeName) { setNull(parameterIndex, sqlType); }

    @Override public void setAsciiStream(int pi, InputStream x, int l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override @Deprecated public void setUnicodeStream(int pi, InputStream x, int l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setBinaryStream(int pi, InputStream x, int l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setCharacterStream(int pi, Reader x, int l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setRef(int pi, Ref x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setBlob(int pi, Blob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setClob(int pi, Clob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setArray(int pi, Array x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setURL(int pi, URL x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setRowId(int pi, RowId x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setNString(int pi, String x) { setString(pi, x); }
    @Override public void setNCharacterStream(int pi, Reader x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setNClob(int pi, NClob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setClob(int pi, Reader x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setBlob(int pi, InputStream x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setNClob(int pi, Reader x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setSQLXML(int pi, SQLXML x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setAsciiStream(int pi, InputStream x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setBinaryStream(int pi, InputStream x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setCharacterStream(int pi, Reader x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setAsciiStream(int pi, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setBinaryStream(int pi, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setCharacterStream(int pi, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setNCharacterStream(int pi, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setClob(int pi, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setBlob(int pi, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setNClob(int pi, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }

    @Override
    public void addBatch() throws SQLException { throw new SQLFeatureNotSupportedException(); }
}
