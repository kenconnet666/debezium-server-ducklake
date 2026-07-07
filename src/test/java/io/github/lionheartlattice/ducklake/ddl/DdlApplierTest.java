package io.github.lionheartlattice.ducklake.ddl;

import io.github.lionheartlattice.ducklake.config.DucklakeProperties;
import io.github.lionheartlattice.ducklake.metrics.SyncState;
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
 * DdlApplier 单测(真 DuckDB 内存库):真 rename 判定(同事务无 sql_drop)、
 * DROP+ADD 不误判、幂等重放、followDropColumn 开关、全量审计落表、缓存失效回调。
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
            .build();

    @BeforeAll
    static void openDuckDb() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement s = conn.createStatement()) {
            s.execute("ATTACH ':memory:' AS lake");
            s.execute("CREATE SCHEMA lake.cdc");
            s.execute("CREATE SCHEMA lake.meta");
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
        return new Struct(DDL_SCHEMA)
                .put("id", ++idSeq).put("ev", ev).put("tag", tag)
                .put("object_type", objectType).put("object_identity", identity)
                .put("query_text", query).put("xid", xid)
                .put("occurred_at", "2026-07-07T10:00:00+08:00").put("__lsn", idSeq);
    }

    private void apply(List<Struct> run) throws SQLException {
        applier.apply(conn, run, invalidated::add);
    }

    // ---------- 用例 ----------

    @Test
    void trueRenameIsAppliedToLakeTable() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE lake.cdc.public_r1 (id BIGINT, note VARCHAR)");
        }
        // ALTER TABLE + 同事务无 sql_drop + RENAME COLUMN 语句 = 真 rename
        apply(List.of(row("ddl_command_end", "ALTER TABLE", "table", "public.r1",
                "ALTER TABLE r1 RENAME COLUMN note TO remark", 100)));

        assertThat(columns("public_r1")).containsExactly("id", "remark");
        assertThat(invalidated).containsExactly("cdc.public_r1");  // 消费者缓存失效回调
        assertThat(syncState.getDdlApplied().count()).isEqualTo(1);
    }

    @Test
    void renameReplayIsIdempotent() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE lake.cdc.public_r2 (id BIGINT, note VARCHAR)");
        }
        List<Struct> run = List.of(row("ddl_command_end", "ALTER TABLE", "table", "public.r2",
                "ALTER TABLE r2 RENAME COLUMN note TO remark", 110));
        apply(run);
        apply(run); // 快照重放同一 DDL:旧列已不存在 → 安全跳过

        assertThat(columns("public_r2")).containsExactly("id", "remark");
        assertThat(syncState.getDdlApplied().count()).isEqualTo(1); // 只应用了一次
    }

    @Test
    void dropAddInSameXidIsNotMisjudgedAsRename() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE lake.cdc.public_r3 (id BIGINT, note VARCHAR)");
        }
        // DROP COLUMN + ADD COLUMN(同 xid 有 sql_drop):默认 followDropColumn=false → 湖列保守不动
        apply(List.of(
                row("ddl_command_end", "ALTER TABLE", "table", "public.r3",
                        "ALTER TABLE r3 DROP COLUMN note, ADD COLUMN remark text", 120),
                row("sql_drop", "ALTER TABLE", "table column", "public.r3.note",
                        "ALTER TABLE r3 DROP COLUMN note, ADD COLUMN remark text", 120)));

        assertThat(columns("public_r3")).containsExactly("id", "note"); // 未 rename、未删列
        assertThat(syncState.getDdlApplied().count()).isZero();
    }

    @Test
    void followDropColumnDeletesLakeColumnWhenEnabled() throws Exception {
        props.getMaintenance().setFollowDropColumn(true);
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE lake.cdc.public_r4 (id BIGINT, legacy VARCHAR)");
        }
        apply(List.of(
                row("ddl_command_end", "ALTER TABLE", "table", "public.r4",
                        "ALTER TABLE r4 DROP COLUMN legacy", 130),
                row("sql_drop", "ALTER TABLE", "table column", "public.r4.legacy",
                        "ALTER TABLE r4 DROP COLUMN legacy", 130)));

        assertThat(columns("public_r4")).containsExactly("id");
        assertThat(invalidated).containsExactly("cdc.public_r4");
    }

    @Test
    void everyEventIsAuditedVerbatim() throws Exception {
        long before = countHistory();
        apply(List.of(
                row("ddl_command_end", "CREATE TABLE", "table", "public.audit_t", "CREATE TABLE audit_t(id bigint)", 140),
                row("ddl_command_end", "CREATE INDEX", "index", "public.audit_t_pkey", "CREATE TABLE audit_t(id bigint)", 140)));

        assertThat(countHistory() - before).isEqualTo(2);
        assertThat(syncState.getDdlAudited().count()).isEqualTo(2);
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(
                "SELECT query_text, xid FROM lake.meta.ddl_history WHERE object_identity='public.audit_t'")) {
            rs.next();
            assertThat(rs.getString(1)).isEqualTo("CREATE TABLE audit_t(id bigint)");
            assertThat(rs.getLong(2)).isEqualTo(140);
        }
    }

    @Test
    void renameOnAbsentLakeTableIsSkipped() throws Exception {
        // 湖表尚未由数据驱动建出(建表延迟到首批数据):rename 安全跳过,建表时直接新列名
        apply(List.of(row("ddl_command_end", "ALTER TABLE", "table", "public.ghost",
                "ALTER TABLE ghost RENAME COLUMN a TO b", 150)));
        assertThat(syncState.getDdlApplied().count()).isZero();
        assertThat(invalidated).isEmpty();
    }

    // ---------- 工具 ----------

    private static List<String> columns(String table) throws SQLException {
        List<String> cols = new ArrayList<>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(
                "SELECT column_name FROM information_schema.columns " +
                        "WHERE table_catalog='lake' AND table_schema='cdc' AND table_name='" + table + "' ORDER BY ordinal_position")) {
            while (rs.next()) {
                cols.add(rs.getString(1));
            }
        }
        return cols;
    }

    private static long countHistory() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*) FROM lake.meta.ddl_history")) {
            rs.next();
            return rs.getLong(1);
        }
    }
}
