package io.github.lionheartlattice.ducklake.sink;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
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

    // ---------- ④ 会话开关对阶段二(CAST 投影 + 湖事务)的影响 ----------
    /**
     * 贴近生产两阶段路径(全 VARCHAR staging + CAST 投影)对比 threads × preserve_insertion_order：
     * 语义上当前态由 __lsn 窗口函数决定、不依赖物理行序,preserve_insertion_order 可关;
     * 本地 DATA_PATH 只反映 CAST/压缩的并行差异,S3 上传侧收益需服务器验证。
     */
    @Test
    @Order(4)
    void sessionKnobs() throws SQLException {
        final int knobBatches = 10;
        try (Statement s = duck.createStatement()) {
            s.execute("CREATE TABLE stg_knobs (id VARCHAR, name VARCHAR, payload VARCHAR, tag VARCHAR, "
                    + "val VARCHAR, ok VARCHAR, __op VARCHAR, __lsn VARCHAR, __source_ts_ms VARCHAR)");
        }
        String[][] combos = {{"2", "true"}, {"2", "false"}, {"4", "true"}, {"4", "false"}};
        for (String[] c : combos) {
            String table = "bench_knob_t" + c[0] + ("true".equals(c[1]) ? "_ord" : "_noord");
            createTable(table);
            try (Statement s = duck.createStatement()) {
                s.execute("SET threads=" + c[0]);
                s.execute("SET preserve_insertion_order=" + c[1]);
            }
            runKnob(table, 0);                            // warmup
            long t0 = System.nanoTime();
            for (int b = 1; b <= knobBatches; b++) {
                runKnob(table, b);
            }
            report("t=%s 保序=%s".formatted(c[0], c[1]), System.nanoTime() - t0, knobBatches * BATCH_ROWS);
            assertThat(rowCount(table)).isEqualTo((knobBatches + 1) * BATCH_ROWS);
        }
    }

    // ---------- ⑤ 单行批固定成本：staging 路径 vs prepared 直插(hybrid 决策依据) ----------
    /**
     * 零星/小流场景每批只有 1~几行,批耗时由固定成本主导。对比两条路径的每批固定成本
     * (staging=CREATE OR REPLACE+Appender+INSERT SELECT+DROP 共 5 语句 vs prepared 直插 1 语句),
     * 差值即 hybrid(小段回落直插)在本地 DuckDB 侧的可回收延迟;生产另有 catalog PG 往返放大批成本。
     */
    @Test
    @Order(5)
    void singleRowBatchFixedCost() throws SQLException {
        final int rounds = 50;
        createTable("bench_one_stg");
        createTable("bench_one_prep");

        // staging 全流程(与生产 stageSegment/insertFromStaging/dropStagings 同构)
        singleRowViaStaging("bench_one_stg", -1);         // warmup
        long t0 = System.nanoTime();
        for (int b = 0; b < rounds; b++) {
            singleRowViaStaging("bench_one_stg", b);
        }
        long stagingNs = System.nanoTime() - t0;

        // prepared 直插(hybrid 小段回落路径)
        singleRowViaPrepared("bench_one_prep", -1);       // warmup
        t0 = System.nanoTime();
        for (int b = 0; b < rounds; b++) {
            singleRowViaPrepared("bench_one_prep", b);
        }
        long preparedNs = System.nanoTime() - t0;

        System.out.printf("[BENCH] 单行批固定成本: staging=%.1f ms/批  prepared直插=%.1f ms/批  差=%.1f ms/批%n",
                stagingNs / 1e6 / rounds, preparedNs / 1e6 / rounds, (stagingNs - preparedNs) / 1e6 / rounds);
        assertThat(rowCount("bench_one_stg")).isEqualTo(rounds + 1);
        assertThat(rowCount("bench_one_prep")).isEqualTo(rounds + 1);
    }

    private void singleRowViaStaging(String table, int batchNo) throws SQLException {
        try (Statement s = duck.createStatement()) {
            s.execute("CREATE OR REPLACE TABLE main.stg_one (id VARCHAR, name VARCHAR, payload VARCHAR, tag VARCHAR, "
                    + "val VARCHAR, ok VARCHAR, __op VARCHAR, __lsn VARCHAR, __source_ts_ms VARCHAR)");
        }
        DuckDBConnection dc = duck.unwrap(DuckDBConnection.class);
        try (DuckDBAppender ap = dc.createAppender("main", "stg_one")) {
            ap.beginRow();
            ap.append(String.valueOf(batchNo));
            ap.append("name-" + batchNo);
            ap.append(payload(batchNo));
            ap.append("tag1");
            ap.append("0.5");
            ap.append("true");
            ap.append("c");
            ap.append(String.valueOf(batchNo));
            ap.append(String.valueOf(1_783_000_000_000L + batchNo));
            ap.endRow();
        }
        duck.setAutoCommit(false);
        try (Statement s = duck.createStatement()) {
            s.execute("INSERT INTO lake.cdc." + table + " SELECT CAST(id AS BIGINT), name, payload, tag, "
                    + "CAST(val AS DOUBLE), CAST(ok AS BOOLEAN), __op, CAST(__lsn AS BIGINT), "
                    + "CAST(__source_ts_ms AS BIGINT) FROM stg_one");
        }
        duck.commit();
        duck.setAutoCommit(true);
        try (Statement s = duck.createStatement()) {
            s.execute("DROP TABLE IF EXISTS main.stg_one");
        }
    }

    private void singleRowViaPrepared(String table, int batchNo) throws SQLException {
        duck.setAutoCommit(false);
        try (PreparedStatement ps = duck.prepareStatement(
                "INSERT INTO lake.cdc." + table + " VALUES (?,?,?,?,?,?,?,?,?)")) {
            ps.setLong(1, batchNo);
            ps.setString(2, "name-" + batchNo);
            ps.setString(3, payload(batchNo));
            ps.setString(4, "tag1");
            ps.setDouble(5, 0.5);
            ps.setBoolean(6, true);
            ps.setString(7, "c");
            ps.setLong(8, batchNo);
            ps.setLong(9, 1_783_000_000_000L + batchNo);
            ps.addBatch();
            ps.executeBatch();
        }
        duck.commit();
        duck.setAutoCommit(true);
    }

    private void runKnob(String table, int batchNo) throws SQLException {
        DuckDBConnection dc = duck.unwrap(DuckDBConnection.class);
        try (DuckDBAppender ap = dc.createAppender("main", "stg_knobs")) {
            for (int i = 0; i < BATCH_ROWS; i++) {
                long id = (long) batchNo * BATCH_ROWS + i;
                ap.beginRow();
                ap.append(String.valueOf(id));
                ap.append("name-" + id);
                ap.append(payload(id));
                ap.append("tag" + (id % 7));
                ap.append(String.valueOf(id * 0.01));
                ap.append(String.valueOf((id & 1) == 0));
                ap.append("c");
                ap.append(String.valueOf(id));
                ap.append(String.valueOf(1_783_000_000_000L + id));
                ap.endRow();
            }
        }
        duck.setAutoCommit(false);
        try (Statement s = duck.createStatement()) {
            s.execute("INSERT INTO lake.cdc." + table + " SELECT CAST(id AS BIGINT), name, payload, tag, "
                    + "CAST(val AS DOUBLE), CAST(ok AS BOOLEAN), __op, CAST(__lsn AS BIGINT), "
                    + "CAST(__source_ts_ms AS BIGINT) FROM stg_knobs");
        }
        duck.commit();
        duck.setAutoCommit(true);
        try (Statement s = duck.createStatement()) {
            s.execute("DELETE FROM stg_knobs");
        }
    }

    // ---------- ⑦ Data Inlining 提交成本:parquet 直写 vs inlined(写 catalog) + flush 代价 ----------
    /**
     * 同一 Appender+staging 路径、同批行数(8192),对比 inlining 开/关的批提交耗时——
     * 回答"内联阈值设多大合适":inlined 提交免 parquet 物化(生产更免 S3 PUT),
     * 代价是行进 catalog PG 与之后 flush_inlined_data 的二次落盘(此处一并计时)。
     */
    @Test
    @Order(7)
    void inliningCommitCost() throws SQLException {
        try (Statement s = duck.createStatement()) {
            s.execute("CREATE TABLE IF NOT EXISTS stg_knobs (id VARCHAR, name VARCHAR, payload VARCHAR, tag VARCHAR, "
                    + "val VARCHAR, ok VARCHAR, __op VARCHAR, __lsn VARCHAR, __source_ts_ms VARCHAR)");
        }
        createTable("bench_inl_off");
        runKnob("bench_inl_off", 0);                      // warmup
        long t0 = System.nanoTime();
        for (int b = 1; b <= BATCHES; b++) {
            runKnob("bench_inl_off", b);
        }
        report("inlining关(parquet直写)", System.nanoTime() - t0, BATCHES * BATCH_ROWS);

        try (Statement s = duck.createStatement()) {
            s.execute("SET ducklake_default_data_inlining_row_limit=16384");
        }
        createTable("bench_inl_on");
        runKnob("bench_inl_on", 0);                       // warmup
        t0 = System.nanoTime();
        for (int b = 1; b <= BATCHES; b++) {
            runKnob("bench_inl_on", b);
        }
        report("inlining开(16384)", System.nanoTime() - t0, BATCHES * BATCH_ROWS);

        long tf = System.nanoTime();
        try (Statement s = duck.createStatement()) {
            s.execute("CALL ducklake_flush_inlined_data('lake')");
        }
        System.out.printf("[BENCH] flush_inlined_data(≤%,d 行): %,.0f ms%n",
                (BATCHES + 1) * BATCH_ROWS, (System.nanoTime() - tf) / 1e6);
        assertThat(rowCount("bench_inl_off")).isEqualTo((BATCHES + 1) * BATCH_ROWS);
        assertThat(rowCount("bench_inl_on")).isEqualTo((BATCHES + 1) * BATCH_ROWS);
    }

    // ---------- ⑥ Arrow C Data 流式注册插入(ADBC/Arrow 路线对照) ----------
    /**
     * 构造 Arrow VectorSchemaRoot(列式,零拷贝进 DuckDB)→ registerArrowStream → INSERT SELECT。
     * 与 ③ 的差异:数据在 Java 侧就已是列式向量,DuckDB 经 C Data 接口直读,免 Appender 逐行 JNI;
     * 代价是上游需要攒批构造 RecordBatch(生产接入=架构级改动,此处仅量化收益上限)。
     */
    @Test
    @Order(6)
    void arrowStreamInsert() throws Exception {
        createTable("bench_arrow");
        try (BufferAllocator alloc = new RootAllocator()) {
            runArrow(alloc, 0);                           // warmup
            long t0 = System.nanoTime();
            for (int b = 1; b <= BATCHES; b++) {
                runArrow(alloc, b);
            }
            report("Arrow 流式注册插入", System.nanoTime() - t0, BATCHES * BATCH_ROWS);
        }
        assertThat(rowCount("bench_arrow")).isEqualTo((BATCHES + 1) * BATCH_ROWS);
    }

    private void runArrow(BufferAllocator alloc, int batchNo) throws Exception {
        Schema schema = new Schema(java.util.List.of(
                Field.nullable("id", new ArrowType.Int(64, true)),
                Field.nullable("name", ArrowType.Utf8.INSTANCE),
                Field.nullable("payload", ArrowType.Utf8.INSTANCE),
                Field.nullable("tag", ArrowType.Utf8.INSTANCE),
                Field.nullable("val", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                Field.nullable("ok", ArrowType.Bool.INSTANCE),
                Field.nullable("__op", ArrowType.Utf8.INSTANCE),
                Field.nullable("__lsn", new ArrowType.Int(64, true)),
                Field.nullable("__source_ts_ms", new ArrowType.Int(64, true))));
        // root 生命周期交给 reader→stream 链(stream 释放时经 reader.close 关 root),外层不重复 close
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc);
        root.allocateNew();
        BigIntVector id = (BigIntVector) root.getVector("id");
        VarCharVector name = (VarCharVector) root.getVector("name");
        VarCharVector payloadV = (VarCharVector) root.getVector("payload");
        VarCharVector tag = (VarCharVector) root.getVector("tag");
        Float8Vector val = (Float8Vector) root.getVector("val");
        BitVector ok = (BitVector) root.getVector("ok");
        VarCharVector op = (VarCharVector) root.getVector("__op");
        BigIntVector lsn = (BigIntVector) root.getVector("__lsn");
        BigIntVector ts = (BigIntVector) root.getVector("__source_ts_ms");
        for (int i = 0; i < BATCH_ROWS; i++) {
            long v = (long) batchNo * BATCH_ROWS + i;
            id.setSafe(i, v);
            name.setSafe(i, ("name-" + v).getBytes(java.nio.charset.StandardCharsets.UTF_8));
            payloadV.setSafe(i, payload(v).getBytes(java.nio.charset.StandardCharsets.UTF_8));
            tag.setSafe(i, ("tag" + (v % 7)).getBytes(java.nio.charset.StandardCharsets.UTF_8));
            val.setSafe(i, v * 0.01);
            ok.setSafe(i, (v & 1) == 0 ? 1 : 0);
            op.setSafe(i, new byte[]{'c'});
            lsn.setSafe(i, v);
            ts.setSafe(i, 1_783_000_000_000L + v);
        }
        root.setRowCount(BATCH_ROWS);

        DuckDBConnection dc = duck.unwrap(DuckDBConnection.class);
        String view = "arrow_in_" + batchNo;
        try (ArrowArrayStream stream = ArrowArrayStream.allocateNew(alloc)) {
            Data.exportArrayStream(alloc, new OneShotReader(alloc, root), stream);
            dc.registerArrowStream(view, stream);
            duck.setAutoCommit(false);
            try (Statement s = duck.createStatement()) {
                s.execute("INSERT INTO lake.cdc.bench_arrow SELECT * FROM " + view);
            }
            duck.commit();
            duck.setAutoCommit(true);
        }
    }

    /** 单 RecordBatch 的 ArrowReader:loadNextBatch 首次 true 交出 root,其后流结束 */
    private static final class OneShotReader extends ArrowReader {
        private final VectorSchemaRoot root;
        private boolean served;

        OneShotReader(BufferAllocator alloc, VectorSchemaRoot root) {
            super(alloc);
            this.root = root;
        }

        @Override
        public VectorSchemaRoot getVectorSchemaRoot() {
            return root;
        }

        @Override
        public boolean loadNextBatch() {
            if (served) {
                return false;
            }
            served = true;
            return true;
        }

        @Override
        public long bytesRead() {
            return 0;
        }

        @Override
        protected void closeReadSource() {
            root.close();
        }

        @Override
        protected Schema readSchema() {
            return root.getSchema();
        }
    }
}
