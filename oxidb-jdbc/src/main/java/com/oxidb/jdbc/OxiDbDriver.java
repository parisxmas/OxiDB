package com.oxidb.jdbc;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * JDBC Driver for OxiDB using the OxiWire binary protocol.
 * URL format: jdbc:oxidb://host:port
 */
public class OxiDbDriver implements Driver {

    private static final String URL_PREFIX = "jdbc:oxidb://";

    static {
        try {
            DriverManager.registerDriver(new OxiDbDriver());
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) return null;

        String hostPort = url.substring(URL_PREFIX.length());
        // Strip trailing slash or path
        int slashIdx = hostPort.indexOf('/');
        if (slashIdx >= 0) hostPort = hostPort.substring(0, slashIdx);

        String host;
        int port;
        int colonIdx = hostPort.lastIndexOf(':');
        if (colonIdx >= 0) {
            host = hostPort.substring(0, colonIdx);
            try {
                port = Integer.parseInt(hostPort.substring(colonIdx + 1));
            } catch (NumberFormatException e) {
                throw new SQLException("Invalid port in URL: " + url);
            }
        } else {
            host = hostPort;
            port = 4444; // OxiDB default port
        }

        if (host.isEmpty()) host = "127.0.0.1";

        return new OxiDbConnection(host, port);
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() { return 0; }

    @Override
    public int getMinorVersion() { return 1; }

    @Override
    public boolean jdbcCompliant() { return false; }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }
}
