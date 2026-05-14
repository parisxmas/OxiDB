package com.oxidb.jdbc;

import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.Calendar;

/**
 * JDBC ResultSet backed by a list of document maps from OxiDB.
 */
public class OxiDbResultSet implements ResultSet {

    private final List<Map<String, Object>> rows;
    private final List<String> columns;
    private final Statement statement;
    private int cursor = -1; // before first row
    private boolean closed;
    private boolean wasNull;

    OxiDbResultSet(List<Map<String, Object>> rows, Statement statement) {
        this.rows = rows;
        this.statement = statement;
        // Collect all unique column names preserving order
        LinkedHashSet<String> cols = new LinkedHashSet<>();
        for (Map<String, Object> row : rows) {
            cols.addAll(row.keySet());
        }
        this.columns = new ArrayList<>(cols);
    }

    private Map<String, Object> currentRow() throws SQLException {
        if (closed) throw new SQLException("ResultSet is closed");
        if (cursor < 0 || cursor >= rows.size()) throw new SQLException("No current row");
        return rows.get(cursor);
    }

    private Object getColumn(int columnIndex) throws SQLException {
        if (columnIndex < 1 || columnIndex > columns.size())
            throw new SQLException("Invalid column index: " + columnIndex);
        Object val = currentRow().get(columns.get(columnIndex - 1));
        wasNull = (val == null);
        return val;
    }

    private Object getColumn(String columnLabel) throws SQLException {
        Object val = currentRow().get(columnLabel);
        wasNull = (val == null);
        return val;
    }

    // ── Navigation ────────────────────────────────────────────

    @Override
    public boolean next() throws SQLException {
        if (closed) throw new SQLException("ResultSet is closed");
        cursor++;
        return cursor < rows.size();
    }

    @Override
    public boolean isBeforeFirst() { return cursor < 0; }

    @Override
    public boolean isAfterLast() { return cursor >= rows.size(); }

    @Override
    public boolean isFirst() { return cursor == 0 && !rows.isEmpty(); }

    @Override
    public boolean isLast() { return cursor == rows.size() - 1 && !rows.isEmpty(); }

    @Override
    public void beforeFirst() { cursor = -1; }

    @Override
    public void afterLast() { cursor = rows.size(); }

    @Override
    public boolean first() {
        if (rows.isEmpty()) return false;
        cursor = 0;
        return true;
    }

    @Override
    public boolean last() {
        if (rows.isEmpty()) return false;
        cursor = rows.size() - 1;
        return true;
    }

    @Override
    public int getRow() { return cursor >= 0 && cursor < rows.size() ? cursor + 1 : 0; }

    @Override
    public boolean absolute(int row) {
        if (row > 0) { cursor = row - 1; }
        else if (row < 0) { cursor = rows.size() + row; }
        else { cursor = -1; return false; }
        return cursor >= 0 && cursor < rows.size();
    }

    @Override
    public boolean relative(int rowOffset) {
        return absolute(cursor + 1 + rowOffset);
    }

    @Override
    public boolean previous() {
        cursor--;
        return cursor >= 0;
    }

    // ── Getters ───────────────────────────────────────────────

    @Override
    public String getString(int columnIndex) throws SQLException {
        Object v = getColumn(columnIndex);
        return v == null ? null : v.toString();
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        Object v = getColumn(columnLabel);
        return v == null ? null : v.toString();
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        Object v = getColumn(columnIndex);
        if (v == null) return false;
        if (v instanceof Boolean b) return b;
        return Boolean.parseBoolean(v.toString());
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        Object v = getColumn(columnLabel);
        if (v == null) return false;
        if (v instanceof Boolean b) return b;
        return Boolean.parseBoolean(v.toString());
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        Object v = getColumn(columnIndex);
        if (v == null) return 0;
        if (v instanceof Number n) return n.intValue();
        return Integer.parseInt(v.toString());
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        Object v = getColumn(columnLabel);
        if (v == null) return 0;
        if (v instanceof Number n) return n.intValue();
        return Integer.parseInt(v.toString());
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        Object v = getColumn(columnIndex);
        if (v == null) return 0;
        if (v instanceof Number n) return n.longValue();
        return Long.parseLong(v.toString());
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        Object v = getColumn(columnLabel);
        if (v == null) return 0;
        if (v instanceof Number n) return n.longValue();
        return Long.parseLong(v.toString());
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        Object v = getColumn(columnIndex);
        if (v == null) return 0;
        if (v instanceof Number n) return n.floatValue();
        return Float.parseFloat(v.toString());
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        Object v = getColumn(columnLabel);
        if (v == null) return 0;
        if (v instanceof Number n) return n.floatValue();
        return Float.parseFloat(v.toString());
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        Object v = getColumn(columnIndex);
        if (v == null) return 0;
        if (v instanceof Number n) return n.doubleValue();
        return Double.parseDouble(v.toString());
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        Object v = getColumn(columnLabel);
        if (v == null) return 0;
        if (v instanceof Number n) return n.doubleValue();
        return Double.parseDouble(v.toString());
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        Object v = getColumn(columnIndex);
        if (v == null) return null;
        if (v instanceof Number n) return BigDecimal.valueOf(n.doubleValue());
        return new BigDecimal(v.toString());
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        Object v = getColumn(columnLabel);
        if (v == null) return null;
        if (v instanceof Number n) return BigDecimal.valueOf(n.doubleValue());
        return new BigDecimal(v.toString());
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        BigDecimal bd = getBigDecimal(columnIndex);
        return bd == null ? null : bd.setScale(scale, java.math.RoundingMode.HALF_UP);
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        BigDecimal bd = getBigDecimal(columnLabel);
        return bd == null ? null : bd.setScale(scale, java.math.RoundingMode.HALF_UP);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return getColumn(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getColumn(columnLabel);
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return getObject(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return getObject(columnLabel);
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return type.cast(getObject(columnIndex));
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return type.cast(getObject(columnLabel));
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return (byte) getInt(columnIndex);
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return (byte) getInt(columnLabel);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return (short) getInt(columnIndex);
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return (short) getInt(columnLabel);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        String s = getString(columnIndex);
        return s == null ? null : s.getBytes();
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        String s = getString(columnLabel);
        return s == null ? null : s.getBytes();
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        String s = getString(columnIndex);
        if (s == null) return null;
        try { return Date.valueOf(s.substring(0, 10)); } catch (Exception e) { return null; }
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        String s = getString(columnLabel);
        if (s == null) return null;
        try { return Date.valueOf(s.substring(0, 10)); } catch (Exception e) { return null; }
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException { return getDate(columnIndex); }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException { return getDate(columnLabel); }

    @Override
    public Time getTime(int columnIndex) throws SQLException { return null; }

    @Override
    public Time getTime(String columnLabel) throws SQLException { return null; }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException { return null; }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException { return null; }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        Object v = getColumn(columnIndex);
        if (v == null) return null;
        if (v instanceof Number n) return new Timestamp(n.longValue());
        try { return Timestamp.valueOf(v.toString()); } catch (Exception e) { return null; }
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        Object v = getColumn(columnLabel);
        if (v == null) return null;
        if (v instanceof Number n) return new Timestamp(n.longValue());
        try { return Timestamp.valueOf(v.toString()); } catch (Exception e) { return null; }
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException { return getTimestamp(columnIndex); }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException { return getTimestamp(columnLabel); }

    @Override
    public boolean wasNull() { return wasNull; }

    // ── Metadata ──────────────────────────────────────────────

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new OxiDbResultSetMetaData(columns, rows);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        int idx = columns.indexOf(columnLabel);
        if (idx < 0) throw new SQLException("Column not found: " + columnLabel);
        return idx + 1;
    }

    // ── ResultSet properties ──────────────────────────────────

    @Override
    public void close() { closed = true; }

    @Override
    public boolean isClosed() { return closed; }

    @Override
    public int getType() { return ResultSet.TYPE_SCROLL_INSENSITIVE; }

    @Override
    public int getConcurrency() { return ResultSet.CONCUR_READ_ONLY; }

    @Override
    public int getHoldability() { return ResultSet.HOLD_CURSORS_OVER_COMMIT; }

    @Override
    public Statement getStatement() { return statement; }

    @Override
    public void setFetchDirection(int direction) {}

    @Override
    public int getFetchDirection() { return ResultSet.FETCH_FORWARD; }

    @Override
    public void setFetchSize(int rows) {}

    @Override
    public int getFetchSize() { return 0; }

    @Override
    public String getCursorName() { return null; }

    @Override
    public SQLWarning getWarnings() { return null; }

    @Override
    public void clearWarnings() {}

    // ── Unsupported update operations ─────────────────────────

    @Override public void insertRow() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateRow() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void deleteRow() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void refreshRow() {}
    @Override public void cancelRowUpdates() {}
    @Override public void moveToInsertRow() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void moveToCurrentRow() {}
    @Override public boolean rowUpdated() { return false; }
    @Override public boolean rowInserted() { return false; }
    @Override public boolean rowDeleted() { return false; }

    @Override public void updateNull(int ci) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBoolean(int ci, boolean x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateByte(int ci, byte x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateShort(int ci, short x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateInt(int ci, int x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateLong(int ci, long x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateFloat(int ci, float x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateDouble(int ci, double x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBigDecimal(int ci, BigDecimal x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateString(int ci, String x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBytes(int ci, byte[] x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateDate(int ci, Date x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateTime(int ci, Time x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateTimestamp(int ci, Timestamp x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateAsciiStream(int ci, InputStream x, int l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBinaryStream(int ci, InputStream x, int l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateCharacterStream(int ci, Reader x, int l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateObject(int ci, Object x, int s) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateObject(int ci, Object x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNull(String cl) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBoolean(String cl, boolean x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateByte(String cl, byte x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateShort(String cl, short x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateInt(String cl, int x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateLong(String cl, long x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateFloat(String cl, float x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateDouble(String cl, double x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBigDecimal(String cl, BigDecimal x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateString(String cl, String x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBytes(String cl, byte[] x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateDate(String cl, Date x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateTime(String cl, Time x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateTimestamp(String cl, Timestamp x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateAsciiStream(String cl, InputStream x, int l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBinaryStream(String cl, InputStream x, int l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateCharacterStream(String cl, Reader x, int l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateObject(String cl, Object x, int s) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateObject(String cl, Object x) throws SQLException { throw new SQLFeatureNotSupportedException(); }

    // ── Stream getters (minimal support) ──────────────────────

    @Override public InputStream getAsciiStream(int ci) throws SQLException { return null; }
    @Override public InputStream getAsciiStream(String cl) throws SQLException { return null; }
    @Override @Deprecated public InputStream getUnicodeStream(int ci) throws SQLException { return null; }
    @Override @Deprecated public InputStream getUnicodeStream(String cl) throws SQLException { return null; }
    @Override public InputStream getBinaryStream(int ci) throws SQLException { return null; }
    @Override public InputStream getBinaryStream(String cl) throws SQLException { return null; }
    @Override public Reader getCharacterStream(int ci) throws SQLException { return null; }
    @Override public Reader getCharacterStream(String cl) throws SQLException { return null; }

    // ── LOB types (not supported) ─────────────────────────────

    @Override public Ref getRef(int ci) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Ref getRef(String cl) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Blob getBlob(int ci) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Blob getBlob(String cl) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Clob getClob(int ci) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Clob getClob(String cl) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Array getArray(int ci) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Array getArray(String cl) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public URL getURL(int ci) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public URL getURL(String cl) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public NClob getNClob(int ci) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public NClob getNClob(String cl) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public SQLXML getSQLXML(int ci) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public SQLXML getSQLXML(String cl) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public String getNString(int ci) throws SQLException { return getString(ci); }
    @Override public String getNString(String cl) throws SQLException { return getString(cl); }
    @Override public Reader getNCharacterStream(int ci) throws SQLException { return null; }
    @Override public Reader getNCharacterStream(String cl) throws SQLException { return null; }
    @Override public RowId getRowId(int ci) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public RowId getRowId(String cl) throws SQLException { throw new SQLFeatureNotSupportedException(); }

    // ── Update streams (not supported) ────────────────────────
    @Override public void updateRef(int ci, Ref x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateRef(String cl, Ref x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBlob(int ci, Blob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBlob(String cl, Blob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateClob(int ci, Clob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateClob(String cl, Clob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateArray(int ci, Array x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateArray(String cl, Array x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateRowId(int ci, RowId x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateRowId(String cl, RowId x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNString(int ci, String x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNString(String cl, String x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNClob(int ci, NClob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNClob(String cl, NClob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateSQLXML(int ci, SQLXML x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateSQLXML(String cl, SQLXML x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNCharacterStream(int ci, Reader x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNCharacterStream(String cl, Reader x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateAsciiStream(int ci, InputStream x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateAsciiStream(String cl, InputStream x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBinaryStream(int ci, InputStream x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBinaryStream(String cl, InputStream x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateCharacterStream(int ci, Reader x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateCharacterStream(String cl, Reader x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBlob(int ci, InputStream x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBlob(String cl, InputStream x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateClob(int ci, Reader x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateClob(String cl, Reader x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNClob(int ci, Reader x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNClob(String cl, Reader x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNCharacterStream(int ci, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateAsciiStream(int ci, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBinaryStream(int ci, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateCharacterStream(int ci, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNCharacterStream(String cl, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateAsciiStream(String cl, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBinaryStream(String cl, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateCharacterStream(String cl, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBlob(int ci, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBlob(String cl, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateClob(int ci, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateClob(String cl, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNClob(int ci, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNClob(String cl, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }

    @Override
    public boolean isWrapperFor(Class<?> iface) { return false; }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException { throw new SQLException("Not a wrapper"); }
}
