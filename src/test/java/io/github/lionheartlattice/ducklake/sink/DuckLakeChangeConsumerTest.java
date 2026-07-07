package io.github.lionheartlattice.ducklake.sink;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.github.lionheartlattice.ducklake.config.DucklakeProperties;
import io.github.lionheartlattice.ducklake.ddl.DdlApplier;
import io.github.lionheartlattice.ducklake.metrics.SyncState;
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
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement s = conn.createStatement()) {
            // 消费者的 SQL 全部以 lake.<schema>.<table> 三段名引用;ATTACH 一个内存库顶名即可
            s.execute("ATTACH ':memory:' AS " + DuckLakeEngine.LAKE);
            s.execute("CREATE SCHEMA lake.cdc");
            s.execute("CREATE SCHEMA lake.meta");
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

    private static ChangeEvent<SourceRecord, SourceRecord> rowEvent(String topic, long id, String name,
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
    void createsTableOnFirstSightAndAppendsTombstoneChain() throws Exception {
        consumer.handleBatch(List.of(
                rowEvent("zadmin.public.t1", 1, "a", "c", 100),
                rowEvent("zadmin.public.t1", 1, "a2", "u", 101),
                rowEvent("zadmin.public.t1", 1, "a2", "d", 102)
        ), committer());

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT __op, __deleted, name FROM lake.cdc.public_t1 ORDER BY __lsn")) {
            rs.next();
            assertThat(rs.getString(1)).isEqualTo("c");
            rs.next();
            assertThat(rs.getString(1)).isEqualTo("u");
            rs.next();
            assertThat(List.of(rs.getString(1), rs.getString(2), rs.getString(3)))
                    .containsExactly("d", "true", "a2"); // 墓碑保留整行旧值
            assertThat(rs.next()).isFalse();
        }
        // 水位线推进到本批最大源提交时刻
        assertThat(syncState.getLastSourceTsMs().get()).isEqualTo(1_783_000_000_000L + 102);
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
             ResultSet rs = s.executeQuery("SELECT extra FROM lake.cdc.public_t2 ORDER BY __lsn")) {
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

        try (Statement s = conn.createStatement()) {
            // 进了审计表
            try (ResultSet rs = s.executeQuery(
                    "SELECT count(*) FROM lake.meta.ddl_history WHERE object_identity='public.t9'")) {
                rs.next();
                assertThat(rs.getLong(1)).isEqualTo(1);
            }
            // 没有被当业务表建出 cdc.public_sys_ddl_log
            try (ResultSet rs = s.executeQuery(
                    "SELECT count(*) FROM information_schema.tables WHERE table_catalog='lake' AND table_name='public_sys_ddl_log'")) {
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

    /** 不可自愈的 SQL 错误(列类型收窄后溢出):3 次重试耗尽,抛出交给进程级重启 */
    @Test
    void persistentSqlFailureGivesUpAfterThreeAttempts() throws Exception {
        consumer.handleBatch(List.of(rowEvent("zadmin.public.t4", 1, "a", "c", 500)), committer());
        try (Statement s = conn.createStatement()) {
            s.execute("ALTER TABLE lake.cdc.public_t4 ALTER COLUMN id TYPE SMALLINT");
        }
        assertThatThrownBy(() -> consumer.handleBatch(
                List.of(rowEvent("zadmin.public.t4", 4_000_000_000L, "boom", "c", 501)), committer()))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("落湖连续 3 次失败");
        assertThat(syncState.getBatchFailures().count()).isEqualTo(3);
    }

    @Test
    void topicParsingHandlesTwoAndThreeSegments() {
        assertThat(DuckLakeChangeConsumer.TableRef.parse("zadmin.public.sys_user"))
                .isEqualTo(new DuckLakeChangeConsumer.TableRef("public", "sys_user"));
        assertThat(DuckLakeChangeConsumer.TableRef.parse("prefix.only"))
                .isEqualTo(new DuckLakeChangeConsumer.TableRef("public", "only"));
    }
}
