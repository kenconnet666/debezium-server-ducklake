package org.dpdns.zerodep.ducklake;

import io.micrometer.core.instrument.MeterRegistry;
import org.dpdns.zerodep.ducklake.metrics.SyncState;
import org.dpdns.zerodep.ducklake.engine.RawMySqlRunner;
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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * MySQL 原生 binlog → DuckLake 生产链吞吐基准。
 * <p>
 * 默认 5 万行；用 {@code -Dmysql.bench.rows=N} 调整。覆盖单热表 I/U/D、小事务和四表混合，
 * 输出 source INSERT、reader drain、湖批次数及最近一批 stage/lake transaction 分段耗时。
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class MySqlThroughputTest {

    private static final int ROWS = Integer.getInteger("mysql.bench.rows", 50_000);
    private static final int TRANSACTION_ROWS = Math.max(1, Integer.getInteger(
            "mysql.bench.transaction-rows", Math.min(ROWS, 50_000)));
    private static final int SMALL_TRANSACTIONS = Integer.getInteger("mysql.bench.small-transactions", 500);
    private static final int ROWS_PER_SMALL_TRANSACTION = 10;
    private static final Duration BENCH_TIMEOUT = Duration.ofMinutes(
            Long.getLong("mysql.bench.timeout-minutes", ROWS > 1_000_000 ? 30L : 3L));

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
    @Autowired
    RawMySqlRunner rawRunner;
    @Autowired
    MeterRegistry meterRegistry;

    @BeforeAll
    static void provision() throws Exception {
        try (Connection connection = DriverManager.getConnection(MYSQL.getJdbcUrl(), "root", "test");
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE USER 'dbuser_cdc'@'%' IDENTIFIED BY 'test'");
            statement.execute("GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, "
                    + "REPLICATION CLIENT ON *.* TO 'dbuser_cdc'@'%'");
            statement.execute("CREATE TABLE bench.stream "
                    + "(id bigint PRIMARY KEY, name varchar(32), val decimal(12,2))");
            statement.execute("CREATE TABLE bench.small_tx "
                    + "(id bigint PRIMARY KEY, value varchar(32))");
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
        ThroughputMetrics.assertSourceTiming(syncState, startedAt, finishedAt);
    }

    @Test
    @Order(2)
    void warmUpdateThroughput() throws Exception {
        double eventsBefore = syncState.getEvents().count();
        double batchesBefore = syncState.getBatches().count();

        long startedAt = System.currentTimeMillis();
        executeRanges("UPDATE bench.stream SET name=CONCAT('updated-', id), val=val+1 "
                + "WHERE id BETWEEN %d AND %d");
        long sourceDoneAt = System.currentTimeMillis();
        awaitEvents(eventsBefore + ROWS);
        long finishedAt = System.currentTimeMillis();

        assertThat(lakeCount("SELECT count(*) FROM bench.stream WHERE name LIKE 'updated-%'"))
                .isEqualTo(ROWS);
        report("mysql native warm-update", ROWS, sourceDoneAt - startedAt,
                finishedAt - sourceDoneAt, finishedAt - startedAt,
                Math.round(syncState.getBatches().count() - batchesBefore));
        assertThat(rate(ROWS, finishedAt - startedAt)).isGreaterThan(1_000);
    }

    @Test
    @Order(3)
    void warmDeleteThroughput() throws Exception {
        int deletedRows = ROWS / 2;
        int remainingRows = ROWS - deletedRows;
        double eventsBefore = syncState.getEvents().count();
        double batchesBefore = syncState.getBatches().count();

        long startedAt = System.currentTimeMillis();
        executeRanges("DELETE FROM bench.stream WHERE id BETWEEN %d AND %d AND MOD(id, 2)=0");
        long sourceDoneAt = System.currentTimeMillis();
        awaitEvents(eventsBefore + deletedRows);
        long finishedAt = System.currentTimeMillis();

        assertThat(lakeCount("SELECT count(*) FROM bench.stream")).isEqualTo(remainingRows);
        report("mysql native warm-delete", deletedRows, sourceDoneAt - startedAt,
                finishedAt - sourceDoneAt, finishedAt - startedAt,
                Math.round(syncState.getBatches().count() - batchesBefore));
        assertThat(rate(deletedRows, finishedAt - startedAt)).isGreaterThan(1_000);
    }

    @Test
    @Order(4)
    void smallTransactionStreamingThroughput() throws Exception {
        int totalRows = SMALL_TRANSACTIONS * ROWS_PER_SMALL_TRANSACTION;
        rawRunner.stop();
        double eventsBefore = syncState.getEvents().count();
        double batchesBefore = syncState.getBatches().count();

        long startedAt = System.currentTimeMillis();
        insertSmallTransactions();
        long insertedAt = System.currentTimeMillis();
        rawRunner.start();
        awaitEvents(eventsBefore + totalRows);
        long finishedAt = System.currentTimeMillis();

        assertThat(lakeCount("SELECT count(*) FROM bench.small_tx")).isEqualTo(totalRows);
        reportBacklog(totalRows, insertedAt - startedAt,
                finishedAt - insertedAt,
                Math.round(syncState.getBatches().count() - batchesBefore));
        assertThat(rate(totalRows, finishedAt - insertedAt)).isGreaterThan(1_000);
    }

    @Test
    @Order(5)
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
        ThroughputMetrics.printAndAssert(meterRegistry, SyncState.Reader.MYSQL);
    }

    private static void insertRange(String table, int rows, String projection) throws Exception {
        try (Connection connection = DriverManager.getConnection(MYSQL.getJdbcUrl(), "root", "test");
             Statement statement = connection.createStatement()) {
            statement.execute("SET SESSION cte_max_recursion_depth = " + (TRANSACTION_ROWS + 1));
            for (int first = 1; first <= rows; first += TRANSACTION_ROWS) {
                int last = Math.min(rows, first + TRANSACTION_ROWS - 1);
                statement.execute("INSERT INTO " + table
                        + " WITH RECURSIVE n (i) AS (SELECT " + first
                        + " UNION ALL SELECT i+1 FROM n WHERE i < " + last
                        + ") SELECT " + projection + " FROM n");
            }
        }
    }

    private static void executeRanges(String sqlTemplate) throws Exception {
        try (Connection connection = DriverManager.getConnection(MYSQL.getJdbcUrl(), "root", "test");
             Statement statement = connection.createStatement()) {
            for (int first = 1; first <= ROWS; first += TRANSACTION_ROWS) {
                int last = Math.min(ROWS, first + TRANSACTION_ROWS - 1);
                statement.execute(sqlTemplate.formatted(first, last));
            }
        }
    }

    private static void insertSmallTransactions() throws Exception {
        try (Connection connection = DriverManager.getConnection(MYSQL.getJdbcUrl(), "root", "test");
             PreparedStatement statement = connection.prepareStatement(
                     "INSERT INTO bench.small_tx(id, value) VALUES (?, ?)")) {
            connection.setAutoCommit(false);
            long id = 1;
            for (int transaction = 0; transaction < SMALL_TRANSACTIONS; transaction++) {
                for (int row = 0; row < ROWS_PER_SMALL_TRANSACTION; row++, id++) {
                    statement.setLong(1, id);
                    statement.setString(2, "value-" + id);
                    statement.addBatch();
                }
                statement.executeBatch();
                connection.commit();
            }
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
        await().atMost(BENCH_TIMEOUT).pollInterval(Duration.ofMillis(25))
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

    private void reportBacklog(int rows, long sourceMs, long drainMs, long batches) {
        System.out.printf("[THROUGHPUT] %-28s %,7d rows  source=%dms  sourceRate=%,d rows/s  "
                        + "backlogDrain=%dms  drainRate=%,d rows/s  batches=%d  stage=%dms  lakeTx=%dms%n",
                "mysql native 500x10 tx", rows, sourceMs, rate(rows, sourceMs), drainMs, rate(rows, drainMs), batches,
                syncState.getLastStageMs().get(), syncState.getLastLakeTxMs().get());
    }

    private static long rate(int rows, long elapsedMs) {
        return elapsedMs == 0 ? Long.MAX_VALUE : rows * 1000L / elapsedMs;
    }
}
