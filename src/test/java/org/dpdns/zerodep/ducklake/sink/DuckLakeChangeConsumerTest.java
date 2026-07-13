package org.dpdns.zerodep.ducklake.sink;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import org.dpdns.zerodep.ducklake.config.DucklakeProperties;
import org.dpdns.zerodep.ducklake.ddl.DdlApplier;
import org.dpdns.zerodep.ducklake.metrics.SyncState;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 消费者单测：对真 DuckDB(ATTACH 内存库伪装 lake catalog)跑完整 handleBatch——
 * 覆盖首见建表、schema 演进加列、墓碑链、DDL 审计流路由、失败自愈与持续失败抛出。
 * <p>
 * DuckLakeEngine 以匿名子类桩替(withLock 直通测试连接):不触发 @PostConstruct,
 * 不需要真 DuckLake/PG catalog——被测对象是消费者逻辑与它产出的 SQL,引擎另有集成测试。
 */
class DuckLakeChangeConsumerTest {

    private static Connection conn;
    private static DucklakeProperties props;

    private SyncState syncState;
    private DuckLakeChangeConsumer consumer;

    @BeforeAll
    static void openDuckDb() throws SQLException {
        // 命名内存 instance 复刻生产结构(主 catalog 恒为 memory,:memory:<name> 只是共享键)。
        // 名字刻意不用 ducklake:集成测试的 Spring 缓存上下文持有同名 instance 且已 ATTACH lake,
        // 同 JVM 复用会撞 "lake already attached"(surefire 单 fork 实测)。
        // 并复刻 USE lake 会话——曾因会话差异漏掉"staging 落湖"缺陷(两段名 main.x
        // 在 USE lake 下解析成 lake.main.x),bench/单测环境必须与生产会话状态一致
        conn = DriverManager.getConnection("jdbc:duckdb::memory:consumer_test");
        try (Statement s = conn.createStatement()) {
            // 消费者的 SQL 全部以 lake.<schema>.<table> 三段名引用;ATTACH 一个内存库顶名即可
            s.execute("ATTACH ':memory:' AS " + DuckLakeEngine.LAKE);
            s.execute("CREATE SCHEMA lake.cdc");
            s.execute("CREATE SCHEMA lake.meta");
            s.execute("USE " + DuckLakeEngine.LAKE);
        }
        props = new DucklakeProperties();
        props.getEngine().setRetrySleepBaseMs(1); // 测试不真等退避
    }

    @AfterAll
    static void close() throws SQLException {
        conn.close();
    }

    @BeforeEach
    void newConsumer() {
        syncState = new SyncState(new SimpleMeterRegistry());
        DuckLakeEngine stub = new DuckLakeEngine(props) {
            @Override
            public <T> T withLock(LakeAction<T> action) throws SQLException {
                return action.run(conn);
            }
        };
        consumer = new DuckLakeChangeConsumer(stub, props, new DdlApplier(props, syncState), syncState);
    }

    // ---------- 事件构造 ----------

    private static final Schema ROW_SCHEMA = SchemaBuilder.struct().name("t1.Value")
            .field("id", Schema.INT64_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .field("__op", Schema.OPTIONAL_STRING_SCHEMA)
            .field("__deleted", Schema.OPTIONAL_STRING_SCHEMA)
            .field("__lsn", Schema.OPTIONAL_INT64_SCHEMA)
            .field("__source_ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
            .build();

    private static final Schema KEY_SCHEMA = SchemaBuilder.struct().name("t1.Key")
            .field("id", Schema.INT64_SCHEMA)
            .build();

    /** 带主键 key 的常规事件（源表有 PK 的正常形态） */
    private static ChangeEvent<SourceRecord, SourceRecord> rowEvent(String topic, long id, String name,
                                                                    String op, long lsn) {
        Struct v = new Struct(ROW_SCHEMA)
                .put("id", id).put("name", name).put("__op", op)
                .put("__deleted", "d".equals(op) ? "true" : "false")
                .put("__lsn", lsn).put("__source_ts_ms", 1_783_000_000_000L + lsn);
        Struct k = new Struct(KEY_SCHEMA).put("id", id);
        return event(new SourceRecord(Map.of(), Map.of(), topic, KEY_SCHEMA, k, ROW_SCHEMA, v));
    }

    /** 无 key 事件（源表无 PK）——湖侧应降级 insert-only */
    private static ChangeEvent<SourceRecord, SourceRecord> rowEventNoKey(String topic, long id, String name,
                                                                         String op, long lsn) {
        Struct v = new Struct(ROW_SCHEMA)
                .put("id", id).put("name", name).put("__op", op)
                .put("__deleted", "d".equals(op) ? "true" : "false")
                .put("__lsn", lsn).put("__source_ts_ms", 1_783_000_000_000L + lsn);
        return event(new SourceRecord(Map.of(), Map.of(), topic, ROW_SCHEMA, v));
    }

    private static ChangeEvent<SourceRecord, SourceRecord> event(SourceRecord rec) {
        return new ChangeEvent<>() {
            public SourceRecord key() { return null; }
            public SourceRecord value() { return rec; }
            public String destination() { return rec.topic(); }
            public Integer partition() { return null; }
        };
    }

    /** no-op committer(单测不校验 offset 推进,那是引擎职责) */
    private static DebeziumEngine.RecordCommitter<ChangeEvent<SourceRecord, SourceRecord>> committer() {
        return new DebeziumEngine.RecordCommitter<>() {
            public void markProcessed(ChangeEvent<SourceRecord, SourceRecord> record) { }
            public void markBatchFinished() { }
            public void markProcessed(ChangeEvent<SourceRecord, SourceRecord> record, DebeziumEngine.Offsets offsets) { }
            public DebeziumEngine.Offsets buildOffsets() { return null; }
        };
    }

    // ---------- 用例 ----------

    @Test
    void mirrorsInsertUpdateDeleteToCurrentState() throws Exception {
        // 批内 I→U→D 链:镜像语义下终态=行不存在(批内按 __lsn 取最后版本,墓碑不插)
        consumer.handleBatch(List.of(
                rowEvent("zadmin.public.t1", 1, "a", "c", 100),
                rowEvent("zadmin.public.t1", 1, "a2", "u", 101),
                rowEvent("zadmin.public.t1", 1, "a2", "d", 102)
        ), committer());

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*) FROM lake.cdc.public_t1")) {
            rs.next();
            assertThat(rs.getLong(1)).as("I-U-D 链终态应为空表").isZero();
        }
        // 湖表列=源表列一一对应,不含任何 __* 元列
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*) FROM information_schema.columns "
                     + "WHERE table_catalog='lake' AND table_name='public_t1' AND column_name LIKE '\\_\\_%' ESCAPE '\\'")) {
            rs.next();
            assertThat(rs.getLong(1)).as("湖表不应含元列").isZero();
        }
        // 跨批 UPDATE:值就地更新,行数不变
        consumer.handleBatch(List.of(rowEvent("zadmin.public.t1", 2, "b", "c", 103)), committer());
        consumer.handleBatch(List.of(rowEvent("zadmin.public.t1", 2, "b9", "u", 104)), committer());
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*), max(name) FROM lake.cdc.public_t1")) {
            rs.next();
            assertThat(rs.getLong(1)).isEqualTo(1);
            assertThat(rs.getString(2)).isEqualTo("b9");
        }
        // 水位线推进到最大源提交时刻
        assertThat(syncState.getLastSourceTsMs().get()).isEqualTo(1_783_000_000_000L + 104);
    }

    /** at-least-once 重放:同一批再处理一遍,结果与处理一遍完全一致(upsert 幂等) */
    @Test
    void replayingSameBatchIsIdempotent() throws Exception {
        List<ChangeEvent<SourceRecord, SourceRecord>> batch = List.of(
                rowEvent("zadmin.public.t10", 1, "a", "c", 100),
                rowEvent("zadmin.public.t10", 2, "b", "c", 101),
                rowEvent("zadmin.public.t10", 1, "a2", "u", 102)
        );
        consumer.handleBatch(batch, committer());
        consumer.handleBatch(batch, committer()); // 重放

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT count(*), min(name), max(name) FROM lake.cdc.public_t10")) {
            rs.next();
            assertThat(rs.getLong(1)).isEqualTo(2);
            assertThat(rs.getString(2)).isEqualTo("a2");
            assertThat(rs.getString(3)).isEqualTo("b");
        }
    }

    /** 源表无主键(事件无 key):降级 insert-only,UPDATE/DELETE 不跟随(墓碑丢弃) */
    @Test
    void noKeyTableDegradesToInsertOnly() throws Exception {
        consumer.handleBatch(List.of(
                rowEventNoKey("zadmin.public.t11", 1, "a", "c", 100),
                rowEventNoKey("zadmin.public.t11", 1, "a", "d", 101)  // 无 key 无从定位,墓碑丢弃
        ), committer());

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*) FROM lake.cdc.public_t11")) {
            rs.next();
            assertThat(rs.getLong(1)).as("insert-only:c 行保留,d 墓碑丢弃").isEqualTo(1);
        }
    }

    @Test
    void schemaEvolutionAddsColumnMidBatch() throws Exception {
        Schema evolved = SchemaBuilder.struct().name("t2.Value")
                .field("id", Schema.INT64_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("extra", Schema.OPTIONAL_INT32_SCHEMA)   // 新列
                .field("__op", Schema.OPTIONAL_STRING_SCHEMA)
                .field("__deleted", Schema.OPTIONAL_STRING_SCHEMA)
                .field("__lsn", Schema.OPTIONAL_INT64_SCHEMA)
                .field("__source_ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
                .build();
        Struct evolvedRow = new Struct(evolved)
                .put("id", 2L).put("name", "b").put("extra", 7)
                .put("__op", "c").put("__deleted", "false").put("__lsn", 201L).put("__source_ts_ms", 1L);

        // 同一批内旧结构 → 新结构:段边界切开,湖表自动 ADD COLUMN
        consumer.handleBatch(List.of(
                rowEvent("zadmin.public.t2", 1, "a", "c", 200),
                event(new SourceRecord(Map.of(), Map.of(), "zadmin.public.t2", evolved, evolvedRow))
        ), committer());

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT extra FROM lake.cdc.public_t2 ORDER BY id")) {
            rs.next();
            assertThat((Object) rs.getObject(1)).isNull();   // 旧结构行:新列补 NULL
            rs.next();
            assertThat(rs.getInt(1)).isEqualTo(7);
        }
    }

    @Test
    void ddlAuditTopicRoutesToApplierNotDataTable() throws Exception {
        Schema ddlSchema = SchemaBuilder.struct().name("sys_ddl_log.Value")
                .field("id", Schema.INT64_SCHEMA)
                .field("ev", Schema.STRING_SCHEMA)
                .field("tag", Schema.OPTIONAL_STRING_SCHEMA)
                .field("object_type", Schema.OPTIONAL_STRING_SCHEMA)
                .field("object_identity", Schema.OPTIONAL_STRING_SCHEMA)
                .field("query_text", Schema.OPTIONAL_STRING_SCHEMA)
                .field("xid", Schema.OPTIONAL_INT64_SCHEMA)
                .field("occurred_at", Schema.OPTIONAL_STRING_SCHEMA)
                .field("__lsn", Schema.OPTIONAL_INT64_SCHEMA)
                .build();
        Struct ddlRow = new Struct(ddlSchema)
                .put("id", 1L).put("ev", "ddl_command_end").put("tag", "CREATE TABLE")
                .put("object_type", "table").put("object_identity", "public.t9")
                .put("query_text", "CREATE TABLE t9(id bigint)").put("xid", 900L)
                .put("occurred_at", "2026-07-07T10:00:00+08:00").put("__lsn", 300L);

        consumer.handleBatch(List.of(
                event(new SourceRecord(Map.of(), Map.of(), "zadmin.public.sys_ddl_log", ddlSchema, ddlRow))
        ), committer());

        // 信号被 DdlApplier 消费(计数推进)
        assertThat(syncState.getDdlAudited().count()).isEqualTo(1);
        try (Statement s = conn.createStatement()) {
            // 没有被当业务表建出 cdc.public_sys_ddl_log
            try (ResultSet rs = s.executeQuery(
                    "SELECT count(*) FROM information_schema.tables WHERE table_catalog='lake' AND table_name='public_sys_ddl_log'")) {
                rs.next();
                assertThat(rs.getLong(1)).isZero();
            }
            // 纯跟随不留档:湖里不存在 ddl_history 留档表(2026-07-08 裁撤)
            try (ResultSet rs = s.executeQuery(
                    "SELECT count(*) FROM information_schema.tables WHERE table_catalog='lake' AND table_name='ddl_history'")) {
                rs.next();
                assertThat(rs.getLong(1)).isZero();
            }
        }
    }

    /** 湖表意外消失(≈事务回滚把建表回掉):首次失败清缓存,重试重建自愈 */
    @Test
    void staleColumnCacheSelfHealsByRetry() throws Exception {
        consumer.handleBatch(List.of(rowEvent("zadmin.public.t3", 1, "a", "c", 400)), committer());
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE lake.cdc.public_t3"); // 制造"缓存认为存在、湖里已没有"
        }
        consumer.handleBatch(List.of(rowEvent("zadmin.public.t3", 2, "b", "c", 401)), committer());

        assertThat(syncState.getBatchFailures().count()).isEqualTo(1); // 失败一次后自愈
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*) FROM lake.cdc.public_t3")) {
            rs.next();
            assertThat(rs.getLong(1)).isEqualTo(1); // 表重建,重试批的行在
        }
    }

    /** 类型漂移(湖列被收窄)由严格跟随自愈:漂移检测 → ALTER 回事件类型 → 大值正常落湖 */
    @Test
    void typeDriftSelfHealsByStrictFollow() throws Exception {
        consumer.handleBatch(List.of(rowEvent("zadmin.public.t4", 1, "a", "c", 500)), committer());
        try (Statement s = conn.createStatement()) {
            s.execute("ALTER TABLE lake.cdc.public_t4 ALTER COLUMN id TYPE SMALLINT"); // 制造湖列收窄漂移
        }
        // 缓存里 id 仍是 BIGINT,首批直写溢出失败 → 清缓存重试 → 漂移被发现 → ALTER 跟随 → 自愈
        consumer.handleBatch(List.of(rowEvent("zadmin.public.t4", 4_000_000_000L, "big", "c", 501)), committer());

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*) FROM lake.cdc.public_t4 WHERE id=4000000000")) {
            rs.next();
            assertThat(rs.getLong(1)).isEqualTo(1);
        }
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT data_type FROM information_schema.columns "
                     + "WHERE table_catalog='lake' AND table_name='public_t4' AND column_name='id'")) {
            rs.next();
            assertThat(rs.getString(1)).isEqualTo("BIGINT"); // 列型已严格跟回事件类型
        }
    }

    /** 类型无法就地转换(湖列 VARCHAR 存非数值,事件要 BIGINT):标记重建+重试批干净事务里 DROP 重建 */
    @Test
    void unconvertibleTypeDriftRebuildsTableOnRetry() throws Exception {
        try (Statement s = conn.createStatement()) {
            // 预置"湖列与事件类型不可转"的表:id 是 VARCHAR 且存着非数值(ALTER/CAST 必失败)
            s.execute("CREATE TABLE lake.cdc.public_t7 (id VARCHAR, name VARCHAR)");
            s.execute("INSERT INTO lake.cdc.public_t7 VALUES ('abc', 'old')");
        }
        // 事件 id 为 BIGINT → 漂移 → ALTER 失败 → CAST 重写失败('abc') → 标记重建+signal(源库不可达仅 log)
        // → 本批重试 → 干净事务 DROP 重建 → 当批行按新类型写入
        consumer.handleBatch(List.of(rowEvent("zadmin.public.t7", 42, "new", "c", 700)), committer());

        assertThat(syncState.getBatchFailures().count()).isEqualTo(1); // 首次失败,重试即成功
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*), max(id) FROM lake.cdc.public_t7")) {
            rs.next();
            assertThat(rs.getLong(1)).isEqualTo(1);   // 旧数据已让位(由增量快照重灌,单测不覆盖)
            assertThat(rs.getLong(2)).isEqualTo(42L); // 当批行按 BIGINT 写入
        }
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT data_type FROM information_schema.columns "
                     + "WHERE table_catalog='lake' AND table_name='public_t7' AND column_name='id'")) {
            rs.next();
            assertThat(rs.getString(1)).isEqualTo("BIGINT");
        }
    }

    /** 不可自愈的 SQL 错误(湖对象被换成视图等异常形态):3 次重试耗尽,抛出交给进程级重启 */
    @Test
    void persistentSqlFailureGivesUpAfterThreeAttempts() throws Exception {
        consumer.handleBatch(List.of(rowEvent("zadmin.public.t5", 1, "a", "c", 600)), committer());
        try (Statement s = conn.createStatement()) {
            // 表被换成同名视图:列集合/类型与事件完全一致(不触发任何跟随),但写入永远失败
            s.execute("DROP TABLE lake.cdc.public_t5");
            s.execute("CREATE VIEW lake.cdc.public_t5 AS SELECT 1::BIGINT AS id, 'x' AS name");
        }
        assertThatThrownBy(() -> consumer.handleBatch(
                List.of(rowEvent("zadmin.public.t5", 2, "boom", "c", 601)), committer()))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("落湖连续 3 次失败");
        assertThat(syncState.getBatchFailures().count()).isEqualTo(3);
    }

    /** null 字段经 Appender-staging 路径(append null String)落湖必须是真 NULL,非空串 */
    @Test
    void nullFieldSurvivesStagingPathAsSqlNull() throws Exception {
        consumer.handleBatch(List.of(rowEvent("zadmin.public.t8", 1, null, "c", 800)), committer());

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT name, name IS NULL FROM lake.cdc.public_t8")) {
            rs.next();
            assertThat(rs.getString(1)).isNull();
            assertThat(rs.getBoolean(2)).isTrue();
        }
    }

    @Test
    void stagingStaysInMemoryCatalogNeverLeaksIntoLake() throws Exception {
        // 回归:USE lake 会话下 staging 若用两段名会静默建进湖(每批多 3 次 catalog 提交)。
        // 断言写批后湖 catalog 里没有任何 stg_seg_* 残留(含事务内周期)
        consumer.handleBatch(List.of(rowEvent("zadmin.public.t9", 1, "a", "c", 900)), committer());

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*) FROM information_schema.tables "
                     + "WHERE table_catalog = '" + DuckLakeEngine.LAKE + "' AND table_name LIKE 'stg_seg_%'")) {
            rs.next();
            assertThat(rs.getLong(1)).as("staging 表泄漏进湖 catalog").isZero();
        }
    }

    @Test
    void topicParsingHandlesTwoAndThreeSegments() {
        assertThat(DuckLakeChangeConsumer.TableRef.parse("zadmin.public.sys_user"))
                .isEqualTo(new DuckLakeChangeConsumer.TableRef("public", "sys_user"));
        assertThat(DuckLakeChangeConsumer.TableRef.parse("prefix.only"))
                .isEqualTo(new DuckLakeChangeConsumer.TableRef("public", "only"));
    }
}
