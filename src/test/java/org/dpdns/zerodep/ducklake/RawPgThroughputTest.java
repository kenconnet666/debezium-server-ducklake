package org.dpdns.zerodep.ducklake;

import io.micrometer.core.instrument.MeterRegistry;
import org.dpdns.zerodep.ducklake.metrics.SyncState;
import org.dpdns.zerodep.ducklake.engine.RawPgRunner;
import org.dpdns.zerodep.ducklake.sink.DuckLakeEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
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
 * RawPgReader 吞吐量基准测试（Testcontainers，Docker 不可用时整类跳过）。
 * <p>
 * 两阶段测量：
 * <ul>
 *   <li><b>追赶吞吐</b>：暂停 reader 后预写 N 行，再从已确认 LSN 恢复并追赶到全部落湖。</li>
 *   <li><b>流式吞吐</b>：引擎在线后 INSERT N 行，测增量路径端到端速率。</li>
 * </ul>
 * 单表测试使用相同的简单表（id bigint PK, name text, val numeric(12,2)），
 * 以 {@code generate_series} 分块批量灌入；默认 100k/事务，放大到百万级时避免单个超大源事务。
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RawPgThroughputTest {

    static final int ROWS = Integer.getInteger("pg.bench.rows", 100_000);
    static final int TRANSACTION_ROWS = Math.max(1, Integer.getInteger(
            "pg.bench.transaction-rows", Math.min(ROWS, 100_000)));
    static final Duration BENCH_TIMEOUT = Duration.ofMinutes(
            Long.getLong("pg.bench.timeout-minutes", ROWS > 1_000_000 ? 30L : 3L));

    @Container
    static final PostgreSQLContainer PG = new PostgreSQLContainer("postgres:18-alpine")
            .withDatabaseName("postgres").withUsername("postgres").withPassword("test")
            .withCommand("postgres", "-c", "wal_level=logical");

    @TempDir
    static Path lakeDir;

    @Autowired DuckLakeEngine engine;
    @Autowired SyncState syncState;
    @Autowired RawPgRunner rawRunner;
    @Autowired MeterRegistry meterRegistry;

    // ──────────── 源库初始化 ────────────

    @BeforeAll
    static void provision() throws Exception {
        try (Connection c = DriverManager.getConnection(PG.getJdbcUrl(), "postgres", "test");
             Statement s = c.createStatement()) {
            // CDC 角色 + 发布
            s.execute("CREATE ROLE dbuser_cdc REPLICATION LOGIN PASSWORD 'test'");
            s.execute("GRANT pg_read_all_data TO dbuser_cdc");
            s.execute("CREATE ROLE dbuser_zadmin LOGIN PASSWORD 'test'");
            s.execute("GRANT CREATE ON SCHEMA public TO dbuser_zadmin");
            s.execute("CREATE PUBLICATION dbz_publication FOR ALL TABLES");
            s.execute("CREATE DATABASE ducklake_catalog OWNER dbuser_zadmin");

            // ① 先建 replication slot，再插追赶行
            //    slot 从此位置开始，后续 INSERT 都在 slot 窗口内——引擎启动后立即可追赶
            s.execute("SELECT pg_create_logical_replication_slot('dbz_ducklake', 'pgoutput')");

            // ② 追赶表：测试中暂停 reader 后写入，避免测试方法开始前已经消费导致计时虚高。
            s.execute("CREATE TABLE catchup (id bigint PRIMARY KEY, name text, val numeric(12,2))");

            // ③ 流式表：引擎启动后再插行，测增量路径
            s.execute("CREATE TABLE stream (id bigint PRIMARY KEY, name text, val numeric(12,2))");
        }
    }

    @DynamicPropertySource
    static void wire(DynamicPropertyRegistry r) {
        r.add("ducklake.source.hostname", PG::getHost);
        r.add("ducklake.source.port", PG::getFirstMappedPort);
        r.add("ducklake.source.user", () -> "dbuser_cdc");
        r.add("ducklake.source.password", () -> "test");
        r.add("ducklake.source.dbname", () -> "postgres");

        r.add("ducklake.lake.catalog-host", PG::getHost);
        r.add("ducklake.lake.catalog-port", PG::getFirstMappedPort);
        r.add("ducklake.lake.catalog-db", () -> "ducklake_catalog");
        r.add("ducklake.lake.catalog-user", () -> "dbuser_zadmin");
        r.add("ducklake.lake.catalog-password", () -> "test");
        r.add("ducklake.lake.data-path", () -> lakeDir.toString().replace('\\', '/') + "/");
        r.add("ducklake.lake.s3-endpoint", () -> "127.0.0.1:1");
        r.add("ducklake.lake.s3-access-key", () -> "dummy");
        r.add("ducklake.lake.s3-secret-key", () -> "dummy");

        r.add("ducklake.maintenance.scanner-bootstrap", () -> "false");
        r.add("ducklake.maintenance.scanner-refill", () -> "false");
        r.add("ducklake.maintenance.enabled", () -> "false");
        // 大批阈值：追赶场景下允许一次性提交更多行，减少 withLock 次数
        r.add("ducklake.engine.max-batch-size", () -> "32768");
    }

    // ──────────── 测试 ────────────

    /**
     * 追赶吞吐：暂停 reader 造出 {@code ROWS} 行 backlog，再计量 reader 恢复到全部落湖。
     */
    @Test
    @Order(1)
    void catchupThroughput() throws Exception {
        assertThat(syncState.getEngineRunning().get()).isEqualTo(1);
        rawRunner.stop();

        long sourceStarted = System.currentTimeMillis();
        insertRows("catchup");
        long sourceMs = System.currentTimeMillis() - sourceStarted;
        double batchesBefore = syncState.getBatches().count();

        long t0 = System.currentTimeMillis();
        rawRunner.start();
        await().atMost(BENCH_TIMEOUT).pollInterval(Duration.ofMillis(100))
                .until(() -> lakeCount("SELECT count(*) FROM public.catchup") == ROWS);
        long elapsed = System.currentTimeMillis() - t0;

        long rowsPerSec = elapsed > 0 ? ROWS * 1000L / elapsed : Long.MAX_VALUE;
        printResult("catchup", ROWS, elapsed, rowsPerSec,
                Math.round(syncState.getBatches().count() - batchesBefore));
        System.out.printf("[THROUGHPUT] catchup source=%dms sourceRate=%,d rows/s%n",
                sourceMs, rate(ROWS, sourceMs));
        assertThat(rowsPerSec).as("追赶吞吐应 > 5,000 rows/s").isGreaterThan(5_000);
        ThroughputMetrics.assertSourceTiming(syncState, sourceStarted, System.currentTimeMillis());
    }

    /**
     * 流式吞吐：引擎已在线，INSERT {@code ROWS} 行，测端到端（INSERT 开始 → 全部落湖）速率。
     */
    @Test
    @Order(2)
    void streamingThroughput() throws Exception {
        double batchesBefore = syncState.getBatches().count();
        long t0 = System.currentTimeMillis();
        insertRows("stream");
        long insertDone = System.currentTimeMillis();

        await().atMost(BENCH_TIMEOUT).pollInterval(Duration.ofMillis(100))
                .until(() -> lakeCount("SELECT count(*) FROM public.stream") == ROWS);
        long totalElapsed = System.currentTimeMillis() - t0;
        long landElapsed  = System.currentTimeMillis() - insertDone;

        long rowsPerSec = totalElapsed > 0 ? ROWS * 1000L / totalElapsed : Long.MAX_VALUE;
        printResult("streaming", ROWS, totalElapsed, rowsPerSec,
                Math.round(syncState.getBatches().count() - batchesBefore));
        System.out.printf("[THROUGHPUT] streaming  insert=%dms  land=%dms%n",
                insertDone - t0, landElapsed);
        assertThat(rowsPerSec).as("流式吞吐应 > 5,000 rows/s").isGreaterThan(5_000);
    }

    /**
     * warm UPDATE：复用 streaming 已落湖的表，避免把首次建表/冷 catalog 混入更新吞吐。
     */
    @Test
    @Order(3)
    void warmUpdateThroughput() throws Exception {
        double batchesBefore = syncState.getBatches().count();
        long startedAt = System.currentTimeMillis();
        executeRanges("UPDATE stream SET name='updated-'||id, val=val+1 "
                + "WHERE id BETWEEN %d AND %d");
        long sourceDoneAt = System.currentTimeMillis();

        await().atMost(BENCH_TIMEOUT).pollInterval(Duration.ofMillis(100))
                .until(() -> lakeCount("SELECT count(*) FROM public.stream "
                        + "WHERE name LIKE 'updated-%'") == ROWS);
        long finishedAt = System.currentTimeMillis();

        printMutationResult("warm-update", ROWS, sourceDoneAt - startedAt,
                finishedAt - sourceDoneAt, finishedAt - startedAt,
                Math.round(syncState.getBatches().count() - batchesBefore));
        assertThat(rate(ROWS, finishedAt - startedAt))
                .as("warm UPDATE 吞吐应 > 1,000 rows/s").isGreaterThan(1_000);
    }

    /** warm DELETE：删除偶数 key，并校验湖中只剩奇数 key。 */
    @Test
    @Order(4)
    void warmDeleteThroughput() throws Exception {
        int deletedRows = ROWS / 2;
        int remainingRows = ROWS - deletedRows;
        double batchesBefore = syncState.getBatches().count();
        long startedAt = System.currentTimeMillis();
        executeRanges("DELETE FROM stream WHERE id BETWEEN %d AND %d AND id %% 2 = 0");
        long sourceDoneAt = System.currentTimeMillis();

        await().atMost(BENCH_TIMEOUT).pollInterval(Duration.ofMillis(100))
                .until(() -> lakeCount("SELECT count(*) FROM public.stream") == remainingRows);
        long finishedAt = System.currentTimeMillis();

        printMutationResult("warm-delete", deletedRows, sourceDoneAt - startedAt,
                finishedAt - sourceDoneAt, finishedAt - startedAt,
                Math.round(syncState.getBatches().count() - batchesBefore));
        assertThat(rate(deletedRows, finishedAt - startedAt))
                .as("warm DELETE 吞吐应 > 1,000 rows/s").isGreaterThan(1_000);
    }

    /**
     * 多表混合：同时对4张表写入，测多表混合下的聚合吞吐。
     */
    @Test
    @Order(5)
    void multiTableThroughput() throws Exception {
        int perTable = Math.max(1, ROWS / 4);
        for (int t = 1; t <= 4; t++) {
            exec("CREATE TABLE mt" + t + " (id bigint PRIMARY KEY, v text)");
        }
        long t0 = System.currentTimeMillis();
        double batchesBefore = syncState.getBatches().count();
        for (int t = 1; t <= 4; t++) {
            exec("INSERT INTO mt" + t + " SELECT g, 'v'||g FROM generate_series(1," + perTable + ") g");
        }

        int total = 4 * perTable;
        await().atMost(BENCH_TIMEOUT).pollInterval(Duration.ofMillis(200))
                .until(() -> {
                    long cnt = 0;
                    for (int t = 1; t <= 4; t++) cnt += lakeCount("SELECT count(*) FROM public.mt" + t);
                    return cnt == total;
                });
        long elapsed = System.currentTimeMillis() - t0;

        long rowsPerSec = elapsed > 0 ? total * 1000L / elapsed : Long.MAX_VALUE;
        printResult("multi-table(4×" + perTable + ")", total, elapsed, rowsPerSec,
                Math.round(syncState.getBatches().count() - batchesBefore));
        assertThat(rowsPerSec).as("多表聚合吞吐应 > 5,000 rows/s").isGreaterThan(5_000);
        ThroughputMetrics.printAndAssert(meterRegistry, SyncState.Reader.POSTGRES);
    }

    // ──────────── 工具 ────────────

    private void exec(String sql) throws Exception {
        try (Connection c = DriverManager.getConnection(PG.getJdbcUrl(), "postgres", "test");
             Statement s = c.createStatement()) {
            s.execute(sql);
        }
    }

    private void insertRows(String table) throws Exception {
        executeRanges("INSERT INTO " + table + " SELECT g, 'row-'||g, g*0.01 "
                + "FROM generate_series(%d,%d) g");
    }

    private void executeRanges(String sqlTemplate) throws Exception {
        for (int first = 1; first <= ROWS; first += TRANSACTION_ROWS) {
            int last = Math.min(ROWS, first + TRANSACTION_ROWS - 1);
            exec(sqlTemplate.formatted(first, last));
        }
    }

    private Long lakeCount(String sql) {
        try {
            return engine.queryScalar(sql, Long.class);
        } catch (Exception e) {
            return 0L;
        }
    }

    /** 获取 PG 源库 replication slot 的已消费 WAL 量（诊断用） */
    @SuppressWarnings("unused")
    private String slotLag() throws Exception {
        try (Connection c = DriverManager.getConnection(PG.getJdbcUrl(), "postgres", "test");
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT pg_size_pretty(pg_current_wal_lsn() - confirmed_flush_lsn) "
                     + "FROM pg_replication_slots WHERE slot_name='dbz_ducklake'")) {
            return rs.next() ? rs.getString(1) : "unknown";
        }
    }

    private void printResult(String label, long rows, long elapsedMs, long rowsPerSec, long batches) {
        System.out.printf("[THROUGHPUT] %-28s %,6d rows  %,5d ms  =  %,7d rows/s  "
                        + "batches=%d  stage=%dms  lakeTx=%dms%n",
                label, rows, elapsedMs, rowsPerSec, batches,
                syncState.getLastStageMs().get(), syncState.getLastLakeTxMs().get());
    }

    private void printMutationResult(String label, long rows, long sourceMs, long drainMs,
                                     long totalMs, long batches) {
        System.out.printf("[THROUGHPUT] %-28s %,6d rows  source=%dms  drain=%dms  "
                        + "total=%dms  rate=%,d rows/s  batches=%d  stage=%dms  lakeTx=%dms%n",
                label, rows, sourceMs, drainMs, totalMs, rate(rows, totalMs), batches,
                syncState.getLastStageMs().get(), syncState.getLastLakeTxMs().get());
    }

    private static long rate(long rows, long elapsedMs) {
        return elapsedMs == 0 ? Long.MAX_VALUE : rows * 1000L / elapsedMs;
    }
}
