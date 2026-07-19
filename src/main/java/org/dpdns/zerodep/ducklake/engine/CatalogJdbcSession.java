package org.dpdns.zerodep.ducklake.engine;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/** Single-reader catalog session with prepared-statement reuse and one reconnect on connection loss. */
final class CatalogJdbcSession implements AutoCloseable {

    private final String jdbcUrl;
    private final String user;
    private final String password;
    private final Map<String, PreparedStatement> statements = new HashMap<>();
    private Connection connection;
    private boolean closed;

    CatalogJdbcSession(String jdbcUrl, String user, String password) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
    }

    synchronized <T> T withConnection(SqlAction<Connection, T> action) throws SQLException {
        return execute(action);
    }

    synchronized <T> T withPrepared(String sql, SqlAction<PreparedStatement, T> action) throws SQLException {
        return execute(connection -> {
            PreparedStatement statement = statements.get(sql);
            if (statement == null || statement.isClosed()) {
                statement = connection.prepareStatement(sql);
                statements.put(sql, statement);
            }
            statement.clearParameters();
            return action.run(statement);
        });
    }

    private <T> T execute(SqlAction<Connection, T> action) throws SQLException {
        if (closed) {
            throw new SQLException("Catalog JDBC session is closed", "08003");
        }
        try {
            return action.run(connection());
        } catch (SQLException first) {
            if (!isConnectionFailure(first)) throw first;
            reset();
            try {
                return action.run(connection());
            } catch (SQLException retry) {
                retry.addSuppressed(first);
                reset();
                throw retry;
            }
        }
    }

    private Connection connection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            connection = DriverManager.getConnection(jdbcUrl, user, password);
            connection.setAutoCommit(true);
        }
        return connection;
    }

    private boolean isConnectionFailure(SQLException error) {
        for (SQLException current = error; current != null; current = current.getNextException()) {
            String state = current.getSQLState();
            if (state != null && state.startsWith("08")) return true;
        }
        try {
            return connection == null || connection.isClosed() || !connection.isValid(1);
        } catch (SQLException ignored) {
            return true;
        }
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        reset();
    }

    private void reset() {
        for (PreparedStatement statement : statements.values()) {
            try {
                statement.close();
            } catch (SQLException ignored) {
            }
        }
        statements.clear();
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException ignored) {
            }
            connection = null;
        }
    }

    @FunctionalInterface
    interface SqlAction<I, O> {
        O run(I input) throws SQLException;
    }
}
