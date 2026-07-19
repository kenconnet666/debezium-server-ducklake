package org.dpdns.zerodep.ducklake.engine;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CatalogJdbcSessionTest {

    @Test
    void reusesConnectionAndPreparedStatementUntilClosed() throws Exception {
        CatalogJdbcSession session = new CatalogJdbcSession("jdbc:duckdb:", null, null);

        Connection firstConnection = session.withConnection(connection -> connection);
        Connection secondConnection = session.withConnection(connection -> connection);
        PreparedStatement firstStatement = session.withPrepared("SELECT ?::INTEGER", statement -> statement);
        PreparedStatement secondStatement = session.withPrepared("SELECT ?::INTEGER", statement -> statement);

        assertThat(secondConnection).isSameAs(firstConnection);
        assertThat(secondStatement).isSameAs(firstStatement);

        session.close();
        session.close();

        assertThat(firstStatement.isClosed()).isTrue();
        assertThat(firstConnection.isClosed()).isTrue();
        assertThatThrownBy(() -> session.withConnection(connection -> connection))
                .isInstanceOfSatisfying(SQLException.class,
                        error -> assertThat(error.getSQLState()).isEqualTo("08003"));
    }

    @Test
    void reconnectsOnceForConnectionFailureOnly() throws Exception {
        try (CatalogJdbcSession session = new CatalogJdbcSession("jdbc:duckdb:", null, null)) {
            AtomicInteger attempts = new AtomicInteger();
            AtomicReference<Connection> failedConnection = new AtomicReference<>();
            AtomicReference<Connection> recoveredConnection = new AtomicReference<>();

            int value = session.withConnection(connection -> {
                if (attempts.getAndIncrement() == 0) {
                    failedConnection.set(connection);
                    connection.close();
                    throw new SQLException("connection lost", "08006");
                }
                recoveredConnection.set(connection);
                try (var statement = connection.createStatement();
                     var result = statement.executeQuery("SELECT 42")) {
                    assertThat(result.next()).isTrue();
                    return result.getInt(1);
                }
            });

            assertThat(value).isEqualTo(42);
            assertThat(attempts.get()).isEqualTo(2);
            assertThat(recoveredConnection.get()).isNotSameAs(failedConnection.get());

            AtomicInteger sqlErrorAttempts = new AtomicInteger();
            assertThatThrownBy(() -> session.withConnection(connection -> {
                sqlErrorAttempts.incrementAndGet();
                throw new SQLException("invalid statement", "42000");
            })).isInstanceOfSatisfying(SQLException.class,
                    error -> assertThat(error.getSQLState()).isEqualTo("42000"));
            assertThat(sqlErrorAttempts.get()).isEqualTo(1);
        }
    }
}
