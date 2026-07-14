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
    private List<String> rebuilds;

    private static final Schema DDL_SCHEMA = SchemaBuilder.struct().name("dbz_ddl_log.Value")
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
        rebuilds = new ArrayList<>();
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
        applier.apply(conn, run, invalidated::add, rebuilds::add);
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

    @Test
    void primaryKeyChangeTriggersRebuildResnapshot() throws Exception {
        // 存量表补主键:旧行主键回填无 CDC 事件,湖旧行主键恒 NULL——必须重建+重灌
        apply(List.of(row("ddl_command_end", "ALTER TABLE", "table", "public.p1",
                "ALTER TABLE p1 ADD COLUMN id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY", 200)));
        assertThat(rebuilds).containsExactly("public.p1");

        // 对已有列直接 ADD PRIMARY KEY 同样触发
        apply(List.of(row("ddl_command_end", "ALTER TABLE", "table", "public.p2",
                "ALTER TABLE p2 ADD PRIMARY KEY (code)", 201)));
        assertThat(rebuilds).containsExactly("public.p1", "public.p2");
    }

    @Test
    void primaryKeyChangeSkippedForSnapshotReplayAndUnrelatedAlter() throws Exception {
        // 快照重放(op='r')的历史主键 DDL 不触发(当前态本就完整,重灌无意义)
        apply(List.of(rowWithOp("ddl_command_end", "ALTER TABLE", "table", "public.p3",
                "ALTER TABLE p3 ADD PRIMARY KEY (id)", 210, "r")));
        // 与主键无关的 ALTER(如 DROP CONSTRAINT xxx_pkey 语句不含 PRIMARY KEY 字样)不触发
        apply(List.of(row("ddl_command_end", "ALTER TABLE", "table", "public.p4",
                "ALTER TABLE p4 DROP CONSTRAINT p4_pkey", 211)));
        assertThat(rebuilds).isEmpty();
    }

    // ---------- MySQL schema change 前端（applySchemaChange） ----------

    private static final Schema CHANGE_TABLE_COLUMN = SchemaBuilder.struct().name("Column")
            .field("name", Schema.STRING_SCHEMA)
            .field("comment", Schema.OPTIONAL_STRING_SCHEMA)
            .optional().build();
    private static final Schema CHANGE_TABLE = SchemaBuilder.struct().name("Table")
            .field("comment", Schema.OPTIONAL_STRING_SCHEMA)
            .field("columns", SchemaBuilder.array(CHANGE_TABLE_COLUMN).optional().build())
            .optional().build();
    private static final Schema TABLE_CHANGE = SchemaBuilder.struct().name("Change")
            .field("type", Schema.STRING_SCHEMA)
            .field("id", Schema.STRING_SCHEMA)
            .field("table", CHANGE_TABLE)
            .optional().build();
    private static final Schema CHANGE_SOURCE = SchemaBuilder.struct().name("Source")
            .field("snapshot", Schema.OPTIONAL_STRING_SCHEMA)
            .optional().build();
    private static final Schema SCHEMA_CHANGE = SchemaBuilder.struct().name("SchemaChangeValue")
            .field("source", CHANGE_SOURCE)
            .field("databaseName", Schema.OPTIONAL_STRING_SCHEMA)
            .field("ddl", Schema.OPTIONAL_STRING_SCHEMA)
            .field("tableChanges", SchemaBuilder.array(TABLE_CHANGE).optional().build())
            .build();

    /** 构造 MySQL schema change 事件（snapshot=false 的实时 DDL） */
    private static Struct changeEvent(String db, String ddl, Struct... tableChanges) {
        return changeEventWithSnapshot(db, ddl, "false", tableChanges);
    }

    private static Struct changeEventWithSnapshot(String db, String ddl, String snapshot, Struct... tableChanges) {
        return new Struct(SCHEMA_CHANGE)
                .put("source", new Struct(CHANGE_SOURCE).put("snapshot", snapshot))
                .put("databaseName", db)
                .put("ddl", ddl)
                .put("tableChanges", List.of(tableChanges));
    }

    private static Struct tableChange(String type, String id) {
        return new Struct(TABLE_CHANGE).put("type", type).put("id", id);
    }

    private void applyChange(Struct... events) throws SQLException {
        applier.applySchemaChange(conn, List.of(events), invalidated::add, rebuilds::add);
    }

    @Test
    void mysqlRenameColumnBothSyntaxesFollow() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE SCHEMA IF NOT EXISTS lake.shop");
            s.execute("CREATE TABLE lake.shop.m1 (id BIGINT, note VARCHAR, alias VARCHAR)");
        }
        // 8.0 RENAME COLUMN 语法（反引号标识符）
        applyChange(changeEvent("shop", "ALTER TABLE `shop`.`m1` RENAME COLUMN `note` TO `remark`",
                tableChange("ALTER", "\"shop\".\"m1\"")));
        // 5.x CHANGE 语法改名（old≠new）
        applyChange(changeEvent("shop", "ALTER TABLE m1 CHANGE alias nickname varchar(64)",
                tableChange("ALTER", "\"shop\".\"m1\"")));

        assertThat(columns("shop", "m1")).containsExactly("id", "remark", "nickname");
        // CHANGE 同名（纯类型变更）不触发 rename
        applyChange(changeEvent("shop", "ALTER TABLE m1 CHANGE nickname nickname varchar(255)",
                tableChange("ALTER", "\"shop\".\"m1\"")));
        assertThat(columns("shop", "m1")).containsExactly("id", "remark", "nickname");
    }

    @Test
    void mysqlDropColumnAndDropTableFollow() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE SCHEMA IF NOT EXISTS lake.shop");
            s.execute("CREATE TABLE lake.shop.m2 (id BIGINT, legacy VARCHAR)");
            s.execute("CREATE TABLE lake.shop.m3 (id BIGINT)");
        }
        applyChange(changeEvent("shop", "ALTER TABLE m2 DROP COLUMN legacy",
                tableChange("ALTER", "\"shop\".\"m2\"")));
        assertThat(columns("shop", "m2")).containsExactly("id");

        // DROP TABLE 走结构化 tableChanges type=DROP（ddl 原文无关紧要）
        applyChange(changeEvent("shop", "DROP TABLE `m3`", tableChange("DROP", "\"shop\".\"m3\"")));
        assertThat(tableExists("shop", "m3")).isFalse();
        // DROP PRIMARY KEY 不得误判为删列
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE lake.shop.m4 (id BIGINT, name VARCHAR)");
        }
        applyChange(changeEvent("shop", "ALTER TABLE m4 DROP PRIMARY KEY",
                tableChange("ALTER", "\"shop\".\"m4\"")));
        assertThat(columns("shop", "m4")).containsExactly("id", "name");
        assertThat(rebuilds).contains("shop.m4"); // 但主键变更触发重建
    }

    @Test
    void mysqlRenameTableAndTruncateFollow() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE SCHEMA IF NOT EXISTS lake.shop");
            s.execute("CREATE TABLE lake.shop.m5 (id BIGINT)");
            s.execute("INSERT INTO lake.shop.m5 VALUES (1), (2)");
        }
        // 表重命名:tableChanges id 为 "<old>,<new>" 拼接形态
        applyChange(changeEvent("shop", "ALTER TABLE m5 RENAME TO m5_new",
                tableChange("ALTER", "\"shop\".\"m5\",\"shop\".\"m5_new\"")));
        assertThat(tableExists("shop", "m5")).isFalse();
        assertThat(tableExists("shop", "m5_new")).isTrue();

        // TRUNCATE(schema change 流形态兜底)
        applyChange(changeEvent("shop", "TRUNCATE TABLE m5_new"));
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(
                "SELECT count(*) FROM lake.shop.m5_new")) {
            rs.next();
            assertThat(rs.getLong(1)).isZero();
        }
    }

    @Test
    void mysqlDropDatabaseFollowsAsSchemaCascade() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE SCHEMA IF NOT EXISTS lake.olddb");
            s.execute("CREATE TABLE lake.olddb.x1 (id BIGINT)");
        }
        applyChange(changeEvent("olddb", "DROP DATABASE `olddb`"));
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(
                "SELECT count(*) FROM information_schema.schemata WHERE catalog_name='lake' AND schema_name='olddb'")) {
            rs.next();
            assertThat(rs.getLong(1)).as("湖 schema 应随 DROP DATABASE 级联删除").isZero();
        }
    }

    @Test
    void mysqlSnapshotPhaseDdlOnlyFollowsComments() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE SCHEMA IF NOT EXISTS lake.shop");
            s.execute("CREATE TABLE lake.shop.m6 (id BIGINT, note VARCHAR)");
        }
        // 快照期(snapshot=true)的历史 DDL:结构变更一律跳过(活表不能被历史 DDL 误删/误改)
        Struct dropInSnapshot = changeEventWithSnapshot("shop", "DROP TABLE m6", "true",
                tableChange("DROP", "\"shop\".\"m6\""));
        applyChange(dropInSnapshot);
        assertThat(tableExists("shop", "m6")).isTrue();

        // 但快照期 CREATE TABLE 携带的注释照样跟随(存量表注释借初始快照落湖)
        Struct col = new Struct(CHANGE_TABLE_COLUMN).put("name", "note").put("comment", "备注列");
        Struct table = new Struct(CHANGE_TABLE).put("comment", "商品表")
                .put("columns", List.of(col));
        Struct create = new Struct(TABLE_CHANGE).put("type", "CREATE")
                .put("id", "\"shop\".\"m6\"").put("table", table);
        applyChange(changeEventWithSnapshot("shop",
                "CREATE TABLE m6 (id bigint, note varchar(64) COMMENT '备注列') COMMENT='商品表'", "true", create));
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(
                "SELECT comment FROM duckdb_tables() WHERE database_name='lake' AND schema_name='shop' AND table_name='m6'")) {
            rs.next();
            assertThat(rs.getString(1)).isEqualTo("商品表");
        }
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(
                "SELECT comment FROM duckdb_columns() WHERE database_name='lake' "
                        + "AND schema_name='shop' AND table_name='m6' AND column_name='note'")) {
            rs.next();
            assertThat(rs.getString(1)).isEqualTo("备注列");
        }
    }

    @Test
    void mysqlPrimaryKeyAddTriggersRebuild() throws Exception {
        applyChange(changeEvent("shop", "ALTER TABLE m7 ADD PRIMARY KEY (id)",
                tableChange("ALTER", "\"shop\".\"m7\"")));
        assertThat(rebuilds).containsExactly("shop.m7");
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
