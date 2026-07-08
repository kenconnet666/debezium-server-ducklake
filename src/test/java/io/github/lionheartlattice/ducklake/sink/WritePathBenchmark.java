package io.github.lionheartlattice.ducklake.sink;

import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 写入路径 microbench（类名不以 Test 结尾，常规 mvn test 不执行；手动 -Dtest=WritePathBenchmark 跑）：
 * 真 DuckLake（Testcontainers PG catalog + 本地 DATA_PATH）上对比三种落湖写法的行成本——
 * ① prepared-batch（现状 flushRun 的写法）② 大 VALUES 拼接单条 SQL ③ Appender 灌本地临时表 + INSERT SELECT 中转。
 * 口径：每方法 warmup 1 批后计时 5 批 × 8192 行，批间独立事务（与生产一致）。
 */
@Testcontainers(disabledWithoutDocker = true)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class WritePathBenchmark {

    private static final int BATCH_ROWS = 8192;
    private static final int BATCHES = 5;
    private static final int COLS = 9;

    @Container
    static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:18-alpine")
            .withDatabaseName("ducklake_catalog").withUsername("postgres").withPassword("bench");

    @TempDir
    static Path lakeDir;

    private static Connection duck;

    @BeforeAll
    static void setup() throws SQLException {
        duck = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement s = duck.createStatement()) {
            for (String ext : new String[]{"ducklake", "postgres"}) {
                s.execute("INSTALL " + ext);
                s.execute("LOAD " + ext);
            }
            s.execute(("ATTACH 'ducklake:postgres:dbname=ducklake_catalog host=%s port=%d user=postgres password=bench' "
                    + "AS lake (DATA_PATH '%s/')")
                    .formatted(PG.getHost(), PG.getFirstMappedPort(), lakeDir.toString().replace('\\', '/')));
            s.execute("CREATE SCHEMA IF NOT EXISTS lake.cdc");
        }
        duck.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws SQLException {
        duck.close();
    }

    private static void createTable(String name) throws SQLException {
        try (Statement s = duck.createStatement()) {
            s.execute(("CREATE TABLE lake.cdc.%s (id BIGINT, name VARCHAR, payload VARCHAR, tag VARCHAR, "
                    + "val DOUBLE, ok BOOLEAN, __op VARCHAR, __lsn BIGINT, __source_ts_ms BIGINT)").formatted(name));
        }
    }

    private static String payload(long i) {
        return "payload-中文负载-" + i + "-0123456789012345678901234567890123456789012345678901234567890123456789";
    }

    private static long rowCount(String table) throws SQLException {
        try (Statement s = duck.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*) FROM lake.cdc." + table)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static void report(String label, long nanos, int rows) {
        double ms = nanos / 1e6;
        System.out.printf("[BENCH] %-22s %,d 行  %,.0f ms  %,.0f 行/秒  %.3f ms/行%n",
                label, rows, ms, rows / (ms / 1000), ms / rows);
    }

    // ---------- ① prepared-batch(现状写法) ----------
    @Test
    @Order(1)
    void preparedBatch() throws SQLException {
        createTable("bench_prep");
        String sql = "INSERT INTO lake.cdc.bench_prep VALUES (?,?,?,?,?,?,?,?,?)";
        runPrepared(sql, 0, false);                       // warmup 1 批
        long t0 = System.nanoTime();
        for (int b = 1; b <= BATCHES; b++) {
            runPrepared(sql, b, true);
        }
        report("prepared-batch(现状)", System.nanoTime() - t0, BATCHES * BATCH_ROWS);
        assertThat(rowCount("bench_prep")).isEqualTo((BATCHES + 1) * BATCH_ROWS);
    }

    private void runPrepared(String sql, int batchNo, boolean tx) throws SQLException {
        duck.setAutoCommit(false);
        try (PreparedStatement ps = duck.prepareStatement(sql)) {
            for (int i = 0; i < BATCH_ROWS; i++) {
                long id = (long) batchNo * BATCH_ROWS + i;
                ps.setLong(1, id);
                ps.setString(2, "name-" + id);
                ps.setString(3, payload(id));
                ps.setString(4, "tag" + (id % 7));
                ps.setDouble(5, id * 0.01);
                ps.setBoolean(6, (id & 1) == 0);
                ps.setString(7, "c");
                ps.setLong(8, id);
                ps.setLong(9, 1_783_000_000_000L + id);
                ps.addBatch();
            }
            ps.executeBatch();
        }
        duck.commit();
        duck.setAutoCommit(true);
    }

    // ---------- ② 大 VALUES 拼接(单条 SQL/批,含字符串构造成本) ----------
    @Test
    @Order(2)
    void bigValuesSql() throws SQLException {
        createTable("bench_values");
        runValues(0);                                     // warmup
        long t0 = System.nanoTime();
        for (int b = 1; b <= BATCHES; b++) {
            runValues(b);
        }
        report("大 VALUES 拼接", System.nanoTime() - t0, BATCHES * BATCH_ROWS);
        assertThat(rowCount("bench_values")).isEqualTo((BATCHES + 1) * BATCH_ROWS);
    }

    private void runValues(int batchNo) throws SQLException {
        StringBuilder sb = new StringBuilder(BATCH_ROWS * 160)
                .append("INSERT INTO lake.cdc.bench_values VALUES ");
        for (int i = 0; i < BATCH_ROWS; i++) {
            long id = (long) batchNo * BATCH_ROWS + i;
            if (i > 0) {
                sb.append(',');
            }
            sb.append('(').append(id)
                    .append(",'name-").append(id).append('\'')
                    .append(",'").append(payload(id)).append('\'')
                    .append(",'tag").append(id % 7).append('\'')
                    .append(',').append(id * 0.01)
                    .append(',').append((id & 1) == 0)
                    .append(",'c',").append(id)
                    .append(',').append(1_783_000_000_000L + id).append(')');
        }
        duck.setAutoCommit(false);
        try (Statement s = duck.createStatement()) {
            s.execute(sb.toString());
        }
        duck.commit();
        duck.setAutoCommit(true);
    }

    // ---------- ③ Appender 灌本地临时表 + INSERT SELECT 中转 ----------
    @Test
    @Order(3)
    void appenderStaging() throws SQLException {
        createTable("bench_appender");
        try (Statement s = duck.createStatement()) {
            s.execute("CREATE TABLE staging (id BIGINT, name VARCHAR, payload VARCHAR, tag VARCHAR, "
                    + "val DOUBLE, ok BOOLEAN, __op VARCHAR, __lsn BIGINT, __source_ts_ms BIGINT)");
        }
        runAppender(0);                                   // warmup
        long t0 = System.nanoTime();
        for (int b = 1; b <= BATCHES; b++) {
            runAppender(b);
        }
        report("Appender+临时表中转", System.nanoTime() - t0, BATCHES * BATCH_ROWS);
        assertThat(rowCount("bench_appender")).isEqualTo((BATCHES + 1) * BATCH_ROWS);
    }

    private void runAppender(int batchNo) throws SQLException {
        DuckDBConnection dc = duck.unwrap(DuckDBConnection.class);
        try (DuckDBAppender ap = dc.createAppender("main", "staging")) {
            for (int i = 0; i < BATCH_ROWS; i++) {
                long id = (long) batchNo * BATCH_ROWS + i;
                ap.beginRow();
                ap.append(id);
                ap.append("name-" + id);
                ap.append(payload(id));
                ap.append("tag" + (id % 7));
                ap.append(id * 0.01);
                ap.append((id & 1) == 0);
                ap.append("c");
                ap.append(id);
                ap.append(1_783_000_000_000L + id);
                ap.endRow();
            }
        }
        // DuckDB 限制:单事务只能写一个 attached 库——湖事务只做 INSERT SELECT,
        // staging 清理放湖事务提交后单独 autocommit(写 memory 库)
        duck.setAutoCommit(false);
        try (Statement s = duck.createStatement()) {
            s.execute("INSERT INTO lake.cdc.bench_appender SELECT * FROM staging");
        }
        duck.commit();
        duck.setAutoCommit(true);
        try (Statement s = duck.createStatement()) {
            s.execute("DELETE FROM staging");
        }
    }
}
