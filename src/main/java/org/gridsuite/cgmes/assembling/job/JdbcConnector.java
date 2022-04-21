package org.gridsuite.cgmes.assembling.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JdbcConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcConnector.class);

    private final String url;
    private final String username;
    private final String password;
    private Connection conn;

    public JdbcConnector(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

    public Connection connect() {
        try {
            conn = DriverManager.getConnection(url, username, password);
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }

        return conn;
    }

    public Connection getConnection() {
        return conn;
    }

    public void close() {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                LOGGER.error("Error closing connection", e);
            }
        }
    }
}
