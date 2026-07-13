package org.dpdns.zerodep.ducklake.ddl;

import org.dpdns.zerodep.ducklake.config.DucklakeProperties;
import org.dpdns.zerodep.ducklake.metrics.SyncState;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * DdlApplier 单测(真 DuckDB 内存库):真 rename 判定(同事务无 sql_drop)、DROP+ADD 不误判且
 * 默认跟随真删、followDropColumn=false 保留历史列、墓碑事件过滤(阅后即焚兜底)、幂等重放、
 * 缓存失效回调。2026-07-08 起纯跟随不留档(meta.ddl_history 已裁撤)。
 */
class DdlApplierTest {

    private static Connection conn;

    private DucklakeProperties props;
    private SyncState syncState;
    private DdlApplier applier;
    private List<String> invalidated;

    private static final Schema DDL_SCHEMA = SchemaBuilder.struct().name("sys_ddl_log.Value")
            .field("id", Schema.OPTIONAL_INT64_SCHEMA)
            .field("ev", Schema.STRING_SCHEMA)
            .field("tag", Schema.OPTIONAL_STRING_SCHEMA)
            .field("object_type", Schema.OPTIONAL_STRING_SCHEMA)
            .field("object_identity", Schema.OPTIONAL_STRING_SCHEMA)
            .field("query_text", Schema.OPTIONAL_STRING_SCHEMA)
            .field("xid", Schema.OPTIONAL_INT64_SCHEMA)
            .field("occurred_at", Schema.OPTIONAL_STRING_SCHEMA)
            .field("__lsn", Schema.OPTIONAL_INT64_SCHEMA)
            .field("__op", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    @BeforeAll
    static void openDuckDb() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement s = conn.createStatement()) {
            s.execute("ATTACH ':memory:' AS lake");
            s.execute("CREATE SCHEMA lake.public");
        }
    }

    @AfterAll
    static void close() throws SQLException {
        conn.close();
    }

    @BeforeEach
    void newApplier() {
        props = new DucklakeProperties();
        syncState = new SyncState(new SimpleMeterRegistry());
        applier = new DdlApplier(props, syncState);
        invalidated = new ArrayList<>();
    }

    private static long idSeq = 0;

    private static Struct row(String ev, String tag, String objectType, String identity, String query, long xid) {
        return rowWithOp(ev, tag, objectType, identity, query, xid, "c");
    }

    private static Struct rowWithOp(String ev, String tag, String objectType, String identity,
                                    String query, long xid, String op) {
        return new Struct(DDL_SCHEMA)
                .put("id", ++idSeq).put("ev", ev).put("tag", tag)
                .put("object_type", objectType).put("object_identity", identity)
                .put("query_text", query).put("xid", xid)
                .put("occurred_at", "2026-07-07T10:00:00+08:00").put("__lsn", idSeq)
                .put("__op", op);
    }

    private void apply(List<Struct> run) throws SQLException {
        applier.apply(conn, run, invalidated::add);
    }

    // ---------- 用例 ----------

    @Test
    void trueRenameIsAppliedToLakeTable() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE lake.public.r1 (id BIGINT, note VARCHAR)");
        }
        // ALTER TABLE + 同事务无 sql_drop + RENAME COLUMN 语句 = 真 rename
        apply(List.of(row("ddl_command_end", "ALTER TABLE", "table", "public.r1",
                "ALTER TABLE r1 RENAME COLUMN note TO remark", 100)));

        assertThat(columns("public", "r1")).containsExactly("id", "remark");
        assertThat(invalidated).containsExactly("public.r1");  // 消费者缓存失效回调
        assertThat(syncState.getDdlApplied().count()).isEqualTo(1);
    }

    @Test
    void renameReplayIsIdempotent() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE lake.public.r2 (id BIGINT, note VARCHAR)");
        }
        List<Struct> run = List.of(row("ddl_command_end", "ALTER TABLE", "table", "public.r2",
                "ALTER TABLE r2 RENAME COLUMN note TO remark", 110));
        apply(run);
        apply(run); // 快照重放同一 DDL:旧列已不存在 → 安全跳过

        assertThat(columns("public", "r2")).containsExactly("id", "remark");
        assertThat(syncState.getDdlApplied().count()).isEqualTo(1); // 只应用了一次
    }

    @Test
    void dropAddIsNotMisjudgedAsRenameAndFollowsDropByDefault() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE lake.public.r3 (id BIGINT, note VARCHAR)");
        }
        // DROP COLUMN + ADD COLUMN(同 xid 有 sql_drop):不做 rename;followDropColumn 默认 true → 跟随真删
        apply(List.of(
                row("ddl_command_end", "ALTER TABLE", "table", "public.r3",
                        "ALTER TABLE r3 DROP COLUMN note, ADD COLUMN remark text", 120),
                row("sql_drop", "ALTER TABLE", "table column", "public.r3.note",
                        "ALTER TABLE r3 DROP COLUMN note, ADD COLUMN remark text", 120)));

        assertThat(columns("public", "r3")).containsExactly("id"); // note 已跟删;remark 由数据驱动 ensureTable 建
        assertThat(invalidated).containsExactly("public.r3");
        assertThat(syncState.getDdlApplied().count()).isEqualTo(1);
    }

    @Test
    void followDropColumnDisabledKeepsHistoricalColumn() throws Exception {
        props.getMaintenance().setFollowDropColumn(false);
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE lake.public.r4 (id BIGINT, legacy VARCHAR)");
        }
        apply(List.of(
                row("ddl_command_end", "ALTER TABLE", "table", "public.r4",
                        "ALTER TABLE r4 DROP COLUMN legacy", 130),
                row("sql_drop", "ALTER TABLE", "table column", "public.r4.legacy",
                        "ALTER TABLE r4 DROP COLUMN legacy", 130)));

        assertThat(columns("public", "r4")).containsExactly("id", "legacy"); // 历史列保留,新行该列 NULL
        assertThat(syncState.getDdlApplied().count()).isZero();
    }

    @Test
    void tombstoneEventsAreIgnored() throws Exception {
        // 信号表被 DELETE 清理产生的墓碑(__op=d)不得当作 DDL 信号重放(常规清理走 TRUNCATE 无事件)
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE lake.public.r5 (id BIGINT, note VARCHAR)");
        }
        apply(List.of(rowWithOp("ddl_command_end", "ALTER TABLE", "table", "public.r5",
                "ALTER TABLE r5 RENAME COLUMN note TO remark", 140, "d")));

        assertThat(columns("public", "r5")).containsExactly("id", "note"); // 未被重放
        assertThat(syncState.getDdlApplied().count()).isZero();
        assertThat(syncState.getDdlAudited().count()).isZero(); // 墓碑不计入信号消费数
    }

    @Test
    void renameOnAbsentLakeTableIsSkipped() throws Exception {
        // 湖表尚未由数据驱动建出(建表延迟到首批数据):rename 安全跳过,建表时直接新列名
        apply(List.of(row("ddl_command_end", "ALTER TABLE", "table", "public.ghost",
                "ALTER TABLE ghost RENAME COLUMN a TO b", 150)));
        assertThat(syncState.getDdlApplied().count()).isZero();
        assertThat(invalidated).isEmpty();
    }

    @Test
    void dropTableFollowsToLakeByDefault() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE lake.public.r6 (id BIGINT)");
        }
        // 真实 PG 行为:DROP 命令不出现在 pg_event_trigger_ddl_commands(),审计流只有 sql_drop 行
        apply(List.of(row("sql_drop", "DROP TABLE", "table", "public.r6", "DROP TABLE r6", 160)));

        assertThat(tableExists("public", "r6")).isFalse();
        assertThat(invalidated).containsExactly("public.r6");
        assertThat(syncState.getDdlApplied().count()).isEqualTo(1);
    }

    @Test
    void dropSchemaCascadeFollowsEveryTable() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE SCHEMA IF NOT EXISTS lake.app");
            s.execute("CREATE TABLE lake.app.o1 (id BIGINT)");
            s.execute("CREATE TABLE lake.app.o2 (id BIGINT)");
        }
        // DROP SCHEMA app CASCADE:级联的每张表各有一条 sql_drop table 行(tg_tag='DROP SCHEMA')
        apply(List.of(
                row("sql_drop", "DROP SCHEMA", "table", "app.o1", "DROP SCHEMA app CASCADE", 165),
                row("sql_drop", "DROP SCHEMA", "table", "app.o2", "DROP SCHEMA app CASCADE", 165)));

        assertThat(tableExists("app", "o1")).isFalse();
        assertThat(tableExists("app", "o2")).isFalse();
        assertThat(syncState.getDdlApplied().count()).isEqualTo(2);
    }

    @Test
    void dropTableDisabledKeepsLakeTable() throws Exception {
        props.getMaintenance().setFollowDropTable(false);
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE lake.public.r7 (id BIGINT)");
        }
        apply(List.of(row("sql_drop", "DROP TABLE", "table", "public.r7", "DROP TABLE r7", 170)));

        assertThat(tableExists("public", "r7")).isTrue(); // followDropTable=false:湖表保留
        assertThat(syncState.getDdlApplied().count()).isZero();
    }

    @Test
    void snapshotReplayedDropTableIsSkipped() throws Exception {
        // 快照重放(op='r')的历史 DROP 不得删活表:快照=当前态,表存在说明后来又被重建
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE lake.public.r8 (id BIGINT)");
        }
        apply(List.of(rowWithOp("sql_drop", "DROP TABLE", "table", "public.r8", "DROP TABLE r8", 180, "r")));

        assertThat(tableExists("public", "r8")).isTrue();
        assertThat(syncState.getDdlApplied().count()).isZero();
    }

    @Test
    void commentOnTableAndColumnFollowsToLake() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE lake.public.r9 (id BIGINT, note VARCHAR)");
        }
        apply(List.of(
                row("ddl_command_end", "COMMENT", "table", "public.r9",
                        "COMMENT ON TABLE r9 IS '测试表'", 190),
                row("ddl_command_end", "COMMENT", "table column", "public.r9.note",
                        "COMMENT ON COLUMN r9.note IS '备注''引号'", 191)));

        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(
                "SELECT comment FROM duckdb_tables() WHERE database_name='lake' AND schema_name='public' AND table_name='r9'")) {
            rs.next();
            assertThat(rs.getString(1)).isEqualTo("测试表");
        }
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(
                "SELECT comment FROM duckdb_columns() WHERE database_name='lake' "
                        + "AND schema_name='public' AND table_name='r9' AND column_name='note'")) {
            rs.next();
            assertThat(rs.getString(1)).isEqualTo("备注'引号"); // '' 转义原样搬运
        }
        assertThat(syncState.getDdlApplied().count()).isEqualTo(2);
    }

    @Test
    void commentOnAbsentTableOrUnparsableIsSkipped() throws Exception {
        apply(List.of(
                row("ddl_command_end", "COMMENT", "table", "public.ghost9",
                        "COMMENT ON TABLE ghost9 IS 'x'", 195),      // 湖表不存在:执行失败仅告警
                row("ddl_command_end", "COMMENT", "table", "public.ghost9",
                        "COMMENT ON TABLE ghost9 IS E'pg\\n转义'", 196))); // PG E'' 形式:解析不上跳过
        // 两条都不落地也不抛出
        assertThat(syncState.getDdlApplied().count()).isZero();
    }

    // ---------- 工具 ----------

    private static List<String> columns(String schema, String table) throws SQLException {
        List<String> cols = new ArrayList<>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(
                "SELECT column_name FROM information_schema.columns " +
                        "WHERE table_catalog='lake' AND table_schema='" + schema + "' AND table_name='" + table + "' ORDER BY ordinal_position")) {
            while (rs.next()) {
                cols.add(rs.getString(1));
            }
        }
        return cols;
    }

    private static boolean tableExists(String schema, String table) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(
                "SELECT count(*) FROM information_schema.tables " +
                        "WHERE table_catalog='lake' AND table_schema='" + schema + "' AND table_name='" + table + "'")) {
            rs.next();
            return rs.getLong(1) > 0;
        }
    }
}
