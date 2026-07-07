package io.github.lionheartlattice.ducklake.sink;

import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * TypeMapper 单测：映射断言 + 对<b>真 DuckDB 内存库</b>做 bind→回读闭环
 * （不 mock JDBC——列类型与绑定值的兼容性只有真引擎说了算）。
 */
class TypeMapperTest {

    private static Connection conn;

    @BeforeAll
    static void openDuckDb() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
    }

    @AfterAll
    static void close() throws SQLException {
        conn.close();
    }

    // ---------- duckType 映射 ----------

    @Test
    void decimalMapsWithScale() {
        assertThat(TypeMapper.duckType(Decimal.schema(2))).isEqualTo("DECIMAL(38,2)");
        // scale 上限保护:DECIMAL(38,38) 非法,压到 37
        assertThat(TypeMapper.duckType(Decimal.schema(38))).isEqualTo("DECIMAL(38,37)");
    }

    @Test
    void isoStringTimeLogicalTypesMapToNativeColumns() {
        assertThat(TypeMapper.duckType(logical("io.debezium.time.IsoDate"))).isEqualTo("DATE");
        assertThat(TypeMapper.duckType(logical("io.debezium.time.IsoTime"))).isEqualTo("TIME");
        assertThat(TypeMapper.duckType(logical("io.debezium.time.IsoTimestamp"))).isEqualTo("TIMESTAMP");
        assertThat(TypeMapper.duckType(logical("io.debezium.time.ZonedTimestamp"))).isEqualTo("TIMESTAMPTZ");
        assertThat(TypeMapper.duckType(logical("io.debezium.data.Json"))).isEqualTo("JSON");
        assertThat(TypeMapper.duckType(logical("io.debezium.data.Uuid"))).isEqualTo("UUID");
    }

    @Test
    void physicalTypesAndFallback() {
        assertThat(TypeMapper.duckType(Schema.INT16_SCHEMA)).isEqualTo("SMALLINT");
        assertThat(TypeMapper.duckType(Schema.INT32_SCHEMA)).isEqualTo("INTEGER");
        assertThat(TypeMapper.duckType(Schema.INT64_SCHEMA)).isEqualTo("BIGINT");
        assertThat(TypeMapper.duckType(Schema.FLOAT64_SCHEMA)).isEqualTo("DOUBLE");
        assertThat(TypeMapper.duckType(Schema.BOOLEAN_SCHEMA)).isEqualTo("BOOLEAN");
        assertThat(TypeMapper.duckType(Schema.BYTES_SCHEMA)).isEqualTo("BLOB");
        assertThat(TypeMapper.duckType(Schema.STRING_SCHEMA)).isEqualTo("VARCHAR");
        // 裸 numeric(无固定 scale)→ 字符串保精度
        assertThat(TypeMapper.duckType(VariableScaleDecimal.schema())).isEqualTo("VARCHAR");
    }

    // ---------- bind → DuckDB 回读闭环 ----------

    @Test
    void bindRoundTripThroughRealDuckDb() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE t_bind (d DECIMAL(38,2), dt DATE, ts TIMESTAMP, tstz TIMESTAMPTZ, i BIGINT, str VARCHAR, vd VARCHAR)");
        }
        Schema decimalSchema = Decimal.schema(2);
        Struct variable = VariableScaleDecimal.fromLogical(VariableScaleDecimal.schema(),
                new SpecialValueDecimal(new BigDecimal("12345.6789")));

        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO t_bind VALUES (?,?,?,?,?,?,?)")) {
            TypeMapper.bind(ps, 1, decimalSchema, new BigDecimal("99999999999999999999.55")); // 20 位整数部分不丢精度
            TypeMapper.bind(ps, 2, logical("io.debezium.time.IsoDate"), "2026-07-07");
            TypeMapper.bind(ps, 3, logical("io.debezium.time.IsoTimestamp"), "2026-07-07T12:34:56");
            TypeMapper.bind(ps, 4, logical("io.debezium.time.ZonedTimestamp"), "2026-07-07T12:34:56+08:00");
            TypeMapper.bind(ps, 5, Schema.INT64_SCHEMA, 42L);
            TypeMapper.bind(ps, 6, Schema.STRING_SCHEMA, "中文");
            TypeMapper.bind(ps, 7, VariableScaleDecimal.schema(), variable);
            ps.execute();
        }
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("SELECT * FROM t_bind")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getBigDecimal(1)).isEqualByComparingTo("99999999999999999999.55");
            assertThat(rs.getObject(2, LocalDate.class)).isEqualTo(LocalDate.of(2026, 7, 7));
            assertThat(rs.getObject(3, LocalDateTime.class)).isEqualTo(LocalDateTime.of(2026, 7, 7, 12, 34, 56));
            assertThat(rs.getObject(4, OffsetDateTime.class).toInstant())
                    .isEqualTo(OffsetDateTime.parse("2026-07-07T12:34:56+08:00").toInstant());
            assertThat(rs.getLong(5)).isEqualTo(42L);
            assertThat(rs.getString(6)).isEqualTo("中文");
            assertThat(rs.getString(7)).isEqualTo("12345.6789");
        }
    }

    @Test
    void bindNullSetsSqlNull() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE t_null (v VARCHAR)");
        }
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO t_null VALUES (?)")) {
            TypeMapper.bind(ps, 1, Schema.OPTIONAL_STRING_SCHEMA, null);
            ps.execute();
        }
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("SELECT v FROM t_null")) {
            rs.next();
            assertThat(rs.getString(1)).isNull();
        }
    }

    private static Schema logical(String name) {
        return SchemaBuilder.string().name(name).optional().build();
    }
}
