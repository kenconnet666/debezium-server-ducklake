package org.dpdns.zerodep.ducklake.ddl;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.dpdns.zerodep.ducklake.config.DucklakeProperties;
import org.dpdns.zerodep.ducklake.metrics.SyncState;
import org.dpdns.zerodep.ducklake.sink.DuckLakeEngine;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** PostgreSQL 原生 DDL 审计行到 DuckLake 的跟随语义（真 DuckDB 内存库）。 */
class DdlApplierTest {

    private static Connection connection;
    private static long idSequence;

    private DucklakeProperties props;
    private SyncState syncState;
    private DdlApplier applier;
    private List<String> invalidated;
    private List<String> rebuilds;

    @BeforeAll
    static void openDuckDb() throws SQLException {
        connection = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement statement = connection.createStatement()) {
            statement.execute("ATTACH ':memory:' AS lake");
            statement.execute("CREATE SCHEMA lake.public");
        }
    }

    @AfterAll
    static void close() throws SQLException {
        connection.close();
    }

    @BeforeEach
    void newApplier() {
        props = new DucklakeProperties();
        syncState = new SyncState(new SimpleMeterRegistry());
        applier = new DdlApplier(props, syncState, new DuckLakeEngine(props));
        invalidated = new ArrayList<>();
        rebuilds = new ArrayList<>();
    }

    private static Map<String, String> row(String event, String tag, String objectType,
                                           String identity, String query, long xid) {
        Map<String, String> row = new LinkedHashMap<>();
        row.put("id", Long.toString(++idSequence));
        row.put("ev", event);
        row.put("tag", tag);
        row.put("object_type", objectType);
        row.put("object_identity", identity);
        row.put("query_text", query);
        row.put("xid", Long.toString(xid));
        return row;
    }

    private void apply(List<Map<String, String>> rows) throws SQLException {
        applier.apply(connection, rows, invalidated::add, rebuilds::add);
    }

    @Test
    void pgArrayTypeKeepsNumericPrecisionDomainsAndDimensions() {
        assertThat(applier.pgTypeToDuck("numeric(12,2)", 1)).isEqualTo("DECIMAL(12,2)[]");
        assertThat(applier.pgTypeToDuck("numeric(10,3)", 2)).isEqualTo("DECIMAL(10,3)[][]");
        assertThat(applier.pgTypeToDuck("numeric", 1)).isEqualTo("DECIMAL(38,18)[]");
        assertThat(applier.pgTypeToDuck("numeric(7,3)", 1)).isEqualTo("DECIMAL(7,3)[]");
        assertThat(applier.pgTypeToDuck("timestamp(3)", 1)).isEqualTo("TIMESTAMP[]");
        assertThat(applier.pgTypeToDuck("character varying(32)", 1)).isEqualTo("VARCHAR[]");
        assertThat(applier.pgTypeToDuck("timestamp with time zone", 2)).isEqualTo("TIMESTAMPTZ[][]");
        assertThat(applier.pgTypeToDuck("numeric(40,2)", 1)).isEqualTo("VARCHAR");
        assertThat(applier.pgTypeToDuck("public.composite_amount", 1)).isEqualTo("VARCHAR");
    }

    @Test
    void trueRenameIsAppliedToLakeTable() throws Exception {
        execute("CREATE TABLE lake.public.r1 (id BIGINT, note VARCHAR)");
        apply(List.of(row("ddl_command_end", "ALTER TABLE", "table", "public.r1",
                "ALTER TABLE r1 RENAME COLUMN note TO remark", 100)));

        assertThat(columns("public", "r1")).containsExactly("id", "remark");
        assertThat(invalidated).containsExactly("public.r1");
        assertThat(syncState.getDdlApplied().count()).isEqualTo(1);
    }

    @Test
    void renameReplayIsIdempotent() throws Exception {
        execute("CREATE TABLE lake.public.r2 (id BIGINT, note VARCHAR)");
        List<Map<String, String>> rows = List.of(row("ddl_command_end", "ALTER TABLE", "table",
                "public.r2", "ALTER TABLE r2 RENAME COLUMN note TO remark", 110));
        apply(rows);
        apply(rows);

        assertThat(columns("public", "r2")).containsExactly("id", "remark");
        assertThat(syncState.getDdlApplied().count()).isEqualTo(1);
    }

    @Test
    void dropAddIsNotMisjudgedAsRenameAndFollowsDropByDefault() throws Exception {
        execute("CREATE TABLE lake.public.r3 (id BIGINT, note VARCHAR)");
        apply(List.of(
                row("ddl_command_end", "ALTER TABLE", "table", "public.r3",
                        "ALTER TABLE r3 DROP COLUMN note, ADD COLUMN remark text", 120),
                row("sql_drop", "ALTER TABLE", "table column", "public.r3.note",
                        "ALTER TABLE r3 DROP COLUMN note, ADD COLUMN remark text", 120)));

        assertThat(columns("public", "r3")).containsExactly("id");
        assertThat(syncState.getDdlApplied().count()).isEqualTo(1);
    }

    @Test
    void followDropColumnDisabledKeepsHistoricalColumn() throws Exception {
        props.getMaintenance().setFollowDropColumn(false);
        execute("CREATE TABLE lake.public.r4 (id BIGINT, legacy VARCHAR)");
        apply(List.of(
                row("ddl_command_end", "ALTER TABLE", "table", "public.r4",
                        "ALTER TABLE r4 DROP COLUMN legacy", 130),
                row("sql_drop", "ALTER TABLE", "table column", "public.r4.legacy",
                        "ALTER TABLE r4 DROP COLUMN legacy", 130)));

        assertThat(columns("public", "r4")).containsExactly("id", "legacy");
    }

    @Test
    void renameOnAbsentLakeTableIsSkipped() throws Exception {
        apply(List.of(row("ddl_command_end", "ALTER TABLE", "table", "public.ghost",
                "ALTER TABLE ghost RENAME COLUMN a TO b", 150)));
        assertThat(syncState.getDdlApplied().count()).isZero();
    }

    @Test
    void dropTableFollowsToLakeByDefault() throws Exception {
        execute("CREATE TABLE lake.public.r6 (id BIGINT)");
        apply(List.of(row("sql_drop", "DROP TABLE", "table", "public.r6", "DROP TABLE r6", 160)));

        assertThat(tableExists("public", "r6")).isFalse();
        assertThat(invalidated).containsExactly("public.r6");
    }

    @Test
    void dropSchemaCascadeFollowsEveryTable() throws Exception {
        execute("CREATE SCHEMA IF NOT EXISTS lake.app");
        execute("CREATE TABLE lake.app.o1 (id BIGINT)");
        execute("CREATE TABLE lake.app.o2 (id BIGINT)");
        apply(List.of(
                row("sql_drop", "DROP SCHEMA", "table", "app.o1", "DROP SCHEMA app CASCADE", 165),
                row("sql_drop", "DROP SCHEMA", "table", "app.o2", "DROP SCHEMA app CASCADE", 165)));

        assertThat(tableExists("app", "o1")).isFalse();
        assertThat(tableExists("app", "o2")).isFalse();
    }

    @Test
    void dropTableDisabledKeepsLakeTable() throws Exception {
        props.getMaintenance().setFollowDropTable(false);
        execute("CREATE TABLE lake.public.r7 (id BIGINT)");
        apply(List.of(row("sql_drop", "DROP TABLE", "table", "public.r7", "DROP TABLE r7", 170)));

        assertThat(tableExists("public", "r7")).isTrue();
    }

    @Test
    void commentsFollowToLake() throws Exception {
        execute("CREATE TABLE lake.public.r9 (id BIGINT, note VARCHAR)");
        apply(List.of(
                row("ddl_command_end", "COMMENT", "table", "public.r9",
                        "COMMENT ON TABLE r9 IS '测试表'", 190),
                row("ddl_command_end", "COMMENT", "table column", "public.r9.note",
                        "COMMENT ON COLUMN r9.note IS '备注''引号'", 191)));

        assertThat(tableComment("public", "r9")).isEqualTo("测试表");
        assertThat(columnComment("public", "r9", "note")).isEqualTo("备注'引号");
    }

    @Test
    void primaryKeyChangeRequestsRebuild() throws Exception {
        apply(List.of(row("ddl_command_end", "ALTER TABLE", "table", "public.p1",
                "ALTER TABLE p1 ADD COLUMN id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY", 200)));
        apply(List.of(row("ddl_command_end", "ALTER TABLE", "table", "public.p2",
                "ALTER TABLE p2 ADD PRIMARY KEY (code)", 201)));

        assertThat(rebuilds).containsExactly("public.p1", "public.p2");
    }

    @Test
    void unrelatedAlterDoesNotRequestRebuild() throws Exception {
        apply(List.of(row("ddl_command_end", "ALTER TABLE", "table", "public.p4",
                "ALTER TABLE p4 DROP CONSTRAINT p4_pkey", 211)));

        assertThat(rebuilds).isEmpty();
    }

    @Test
    void renameTableAndBareRenameColumnFollow() throws Exception {
        execute("CREATE TABLE lake.public.rt1 (id BIGINT, note VARCHAR)");
        apply(List.of(row("ddl_command_end", "ALTER TABLE", "table", "public.rt2",
                "ALTER TABLE rt1 RENAME TO rt2", 400)));
        apply(List.of(row("ddl_command_end", "ALTER TABLE", "table", "public.rt2",
                "ALTER TABLE rt2 RENAME note TO remark", 401)));

        assertThat(tableExists("public", "rt1")).isFalse();
        assertThat(columns("public", "rt2")).containsExactly("id", "remark");
    }

    private static void execute(String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static List<String> columns(String schema, String table) throws SQLException {
        List<String> result = new ArrayList<>();
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(
                     "SELECT column_name FROM information_schema.columns "
                             + "WHERE table_catalog='lake' AND table_schema='" + schema
                             + "' AND table_name='" + table + "' ORDER BY ordinal_position")) {
            while (rs.next()) {
                result.add(rs.getString(1));
            }
        }
        return result;
    }

    private static boolean tableExists(String schema, String table) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(
                     "SELECT count(*) FROM information_schema.tables "
                             + "WHERE table_catalog='lake' AND table_schema='" + schema
                             + "' AND table_name='" + table + "'")) {
            rs.next();
            return rs.getLong(1) > 0;
        }
    }

    private static String tableComment(String schema, String table) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(
                     "SELECT comment FROM duckdb_tables() WHERE database_name='lake' "
                             + "AND schema_name='" + schema + "' AND table_name='" + table + "'")) {
            rs.next();
            return rs.getString(1);
        }
    }

    private static String columnComment(String schema, String table, String column) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(
                     "SELECT comment FROM duckdb_columns() WHERE database_name='lake' "
                             + "AND schema_name='" + schema + "' AND table_name='" + table
                             + "' AND column_name='" + column + "'")) {
            rs.next();
            return rs.getString(1);
        }
    }
}
