package org.dpdns.zerodep.ducklake;

import org.dpdns.zerodep.ducklake.metrics.SyncState;
import org.dpdns.zerodep.ducklake.sink.DuckLakeEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * MySQL 原生 binlog → DuckLake 生产链吞吐基准。
 * <p>
 * 默认 5 万行；用 {@code -Dmysql.bench.rows=N} 调整。覆盖单热表与四表混合两种形态，
 * 输出 source INSERT、reader drain、湖批次数及最近一批 stage/lake transaction 分段耗时。
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class MySqlThroughputTest {

    private static final int ROWS = Integer.getInteger("mysql.bench.rows", 50_000);

    @Container
    static final MySQLContainer MYSQL = new MySQLContainer("mysql:8.4")
            .withDatabaseName("bench").withUsername("test").withPassword("test")
            .withCommand("--server-id=1", "--gtid-mode=ON", "--enforce-gtid-consistency=ON",
                    "--binlog-row-image=FULL", "--binlog-row-metadata=FULL");

    @Container
    static final PostgreSQLContainer CATALOG = new PostgreSQLContainer("postgres:18-alpine")
            .withDatabaseName("ducklake_catalog").withUsername("lake_admin").withPassword("test");

    @TempDir
    static Path lakeDir;

    @Autowired
    DuckLakeEngine engine;
    @Autowired
    SyncState syncState;

    @BeforeAll
    static void provision() throws Exception {
        try (Connection connection = DriverManager.getConnection(MYSQL.getJdbcUrl(), "root", "test");
             Statement statement = connection.createStatement()) {
            statement.execute("SET SESSION cte_max_recursion_depth = " + (ROWS + 1));
            statement.execute("CREATE USER 'dbuser_cdc'@'%' IDENTIFIED BY 'test'");
            statement.execute("GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, "
                    + "REPLICATION CLIENT ON *.* TO 'dbuser_cdc'@'%'");
            statement.execute("CREATE TABLE bench.stream "
                    + "(id bigint PRIMARY KEY, name varchar(32), val decimal(12,2))");
            for (int table = 1; table <= 4; table++) {
                statement.execute("CREATE TABLE bench.mt" + table
                        + " (id bigint PRIMARY KEY, value varchar(16))");
            }
        }
    }

    @DynamicPropertySource
    static void wire(DynamicPropertyRegistry registry) {
        registry.add("ducklake.source.type", () -> "mysql");
        registry.add("ducklake.source.name", () -> "mysql-bench");
        registry.add("ducklake.source.hostname", MYSQL::getHost);
        registry.add("ducklake.source.port", MYSQL::getFirstMappedPort);
        registry.add("ducklake.source.user", () -> "dbuser_cdc");
        registry.add("ducklake.source.password", () -> "test");
        registry.add("ducklake.source.dbname", () -> "bench");
        registry.add("ducklake.source.server-id", () -> "6402");
        registry.add("ducklake.source.schema-include-list", () -> "bench");

        registry.add("ducklake.lake.catalog-host", CATALOG::getHost);
        registry.add("ducklake.lake.catalog-port", CATALOG::getFirstMappedPort);
        registry.add("ducklake.lake.catalog-db", () -> "ducklake_catalog");
        registry.add("ducklake.lake.catalog-user", () -> "lake_admin");
        registry.add("ducklake.lake.catalog-password", () -> "test");
        registry.add("ducklake.lake.data-path", () -> lakeDir.toString().replace('\\', '/') + "/");
        registry.add("ducklake.lake.s3-endpoint", () -> "127.0.0.1:1");
        registry.add("ducklake.lake.s3-access-key", () -> "dummy");
        registry.add("ducklake.lake.s3-secret-key", () -> "dummy");

        registry.add("ducklake.maintenance.enabled", () -> "false");
        registry.add("ducklake.maintenance.scanner-bootstrap", () -> "false");
        registry.add("ducklake.maintenance.scanner-refill", () -> "false");
        registry.add("ducklake.engine.max-batch-size", () -> "32768");
    }

    @Test
    @Order(1)
    void singleTableStreamingThroughput() throws Exception {
        awaitBinlogClient();
        double eventsBefore = syncState.getEvents().count();
        double batchesBefore = syncState.getBatches().count();

        long startedAt = System.currentTimeMillis();
        insertRange("bench.stream", ROWS,
                "i, CONCAT('row-', i), i * 0.01");
        long insertedAt = System.currentTimeMillis();
        awaitEvents(eventsBefore + ROWS);
        long finishedAt = System.currentTimeMillis();

        assertThat(lakeCount("SELECT count(*) FROM bench.stream")).isEqualTo(ROWS);
        report("mysql native single-table", ROWS, insertedAt - startedAt,
                finishedAt - insertedAt, finishedAt - startedAt,
                Math.round(syncState.getBatches().count() - batchesBefore));
        assertThat(rate(ROWS, finishedAt - startedAt)).isGreaterThan(10_000);
    }

    @Test
    @Order(2)
    void fourTableStreamingThroughput() throws Exception {
        int rowsPerTable = Math.max(1, ROWS / 4);
        int totalRows = rowsPerTable * 4;
        double eventsBefore = syncState.getEvents().count();
        double batchesBefore = syncState.getBatches().count();

        long startedAt = System.currentTimeMillis();
        for (int table = 1; table <= 4; table++) {
            insertRange("bench.mt" + table, rowsPerTable, "i, CONCAT('v-', i)");
        }
        long insertedAt = System.currentTimeMillis();
        awaitEvents(eventsBefore + totalRows);
        long finishedAt = System.currentTimeMillis();

        for (int table = 1; table <= 4; table++) {
            assertThat(lakeCount("SELECT count(*) FROM bench.mt" + table)).isEqualTo(rowsPerTable);
        }
        report("mysql native four-table", totalRows, insertedAt - startedAt,
                finishedAt - insertedAt, finishedAt - startedAt,
                Math.round(syncState.getBatches().count() - batchesBefore));
        assertThat(rate(totalRows, finishedAt - startedAt)).isGreaterThan(10_000);
    }

    private static void insertRange(String table, int rows, String projection) throws Exception {
        try (Connection connection = DriverManager.getConnection(MYSQL.getJdbcUrl(), "root", "test");
             Statement statement = connection.createStatement()) {
            statement.execute("SET SESSION cte_max_recursion_depth = " + (rows + 1));
            statement.execute("INSERT INTO " + table
                    + " WITH RECURSIVE n (i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < "
                    + rows + ") SELECT " + projection + " FROM n");
        }
    }

    private void awaitBinlogClient() {
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> {
                    assertThat(syncState.getEngineRunning().get()).isEqualTo(1);
                    assertThat(binlogClientConnected()).isTrue();
                });
    }

    private static boolean binlogClientConnected() {
        try (Connection connection = DriverManager.getConnection(MYSQL.getJdbcUrl(), "root", "test");
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT count(*) FROM information_schema.processlist "
                     + "WHERE user='dbuser_cdc' AND command LIKE 'Binlog Dump%'")) {
            return rs.next() && rs.getLong(1) > 0;
        } catch (Exception e) {
            return false;
        }
    }

    private void awaitEvents(double expected) {
        await().atMost(Duration.ofMinutes(3)).pollInterval(Duration.ofMillis(25))
                .until(() -> syncState.getEvents().count() >= expected);
    }

    private long lakeCount(String sql) {
        try {
            Long count = engine.queryScalar(sql, Long.class);
            return count == null ? 0 : count;
        } catch (Exception e) {
            return 0;
        }
    }

    private void report(String label, int rows, long insertMs, long drainMs,
                        long totalMs, long batches) {
        System.out.printf("[THROUGHPUT] %-28s %,7d rows  insert=%dms  drain=%dms  "
                        + "total=%dms  rate=%,d rows/s  batches=%d  stage=%dms  lakeTx=%dms%n",
                label, rows, insertMs, drainMs, totalMs, rate(rows, totalMs), batches,
                syncState.getLastStageMs().get(), syncState.getLastLakeTxMs().get());
    }

    private static long rate(int rows, long elapsedMs) {
        return elapsedMs == 0 ? Long.MAX_VALUE : rows * 1000L / elapsedMs;
    }
}
