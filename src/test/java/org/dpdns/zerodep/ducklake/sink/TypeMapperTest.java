package org.dpdns.zerodep.ducklake.sink;

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
        // 无 precision 参数(仅 scale)默认按 38 宽
        assertThat(TypeMapper.duckType(Decimal.schema(2))).isEqualTo("DECIMAL(38,2)");
        // s=p=38 合法(DuckDB 1.5.4 实测 DECIMAL(38,38) 可建)
        assertThat(TypeMapper.duckType(Decimal.schema(38))).isEqualTo("DECIMAL(38,38)");
        // 源列声明了精度 → 忠实对应(元数据保真;width≤18 走 int64 存储更省)
        Schema n12_2 = Decimal.builder(2).parameter("connect.decimal.precision", "12").optional().build();
        assertThat(TypeMapper.duckType(n12_2)).isEqualTo("DECIMAL(12,2)");
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
        // 裸 numeric(无精度声明,scale 逐值可变)→ DuckDB 上限形态:20 位整数+18 位小数,可直接聚合
        assertThat(TypeMapper.duckType(VariableScaleDecimal.schema())).isEqualTo("DECIMAL(38,18)");
    }

    // ---------- bind → DuckDB 回读闭环 ----------

    @Test
    void bindRoundTripThroughRealDuckDb() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE t_bind (d DECIMAL(38,2), dt DATE, ts TIMESTAMP, tstz TIMESTAMPTZ, i BIGINT, str VARCHAR, vd DECIMAL(38,18))");
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
            assertThat(rs.getBigDecimal(7)).isEqualByComparingTo("12345.6789"); // 裸 numeric → DECIMAL(38,18)
        }
    }

    /** normalize 假设钉死在真引擎上:各类型建表回读 information_schema,归一后须与 duckType 产出一致 */
    @Test
    void normalizeDuckTypeRoundTripsThroughInformationSchema() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE t_norm (a SMALLINT, b INTEGER, c BIGINT, d FLOAT, e DOUBLE, f BOOLEAN, "
                    + "g BLOB, h VARCHAR, i DECIMAL(38,6), j DATE, k TIME, l TIMESTAMP, m TIMESTAMPTZ, n JSON, o UUID)");
        }
        java.util.Map<String, String> expect = java.util.Map.ofEntries(
                java.util.Map.entry("a", "SMALLINT"), java.util.Map.entry("b", "INTEGER"),
                java.util.Map.entry("c", "BIGINT"), java.util.Map.entry("d", "FLOAT"),
                java.util.Map.entry("e", "DOUBLE"), java.util.Map.entry("f", "BOOLEAN"),
                java.util.Map.entry("g", "BLOB"), java.util.Map.entry("h", "VARCHAR"),
                java.util.Map.entry("i", "DECIMAL(38,6)"), java.util.Map.entry("j", "DATE"),
                java.util.Map.entry("k", "TIME"), java.util.Map.entry("l", "TIMESTAMP"),
                java.util.Map.entry("m", "TIMESTAMPTZ"), java.util.Map.entry("n", "JSON"),
                java.util.Map.entry("o", "UUID"));
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(
                "SELECT column_name, data_type FROM information_schema.columns WHERE table_name='t_norm'")) {
            int seen = 0;
            while (rs.next()) {
                String col = rs.getString(1);
                assertThat(TypeMapper.normalizeDuckType(rs.getString(2)))
                        .as("列 %s 的 information_schema 形态归一", col)
                        .isEqualTo(expect.get(col));
                seen++;
            }
            assertThat(seen).isEqualTo(expect.size());
        }
    }

    /**
     * Debezium isostring 模式的真实输出:无时区族一律带 'Z' 后缀(实测 IsoDate='2024-02-29Z')。
     * 回归防线:2026-07-08 该形态曾致 DateTimeParseException → 批重放 → 引擎 crash loop(26 次重启)。
     */
    @Test
    void isoStringWithTrailingZoneSuffixBindsCorrectly() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE t_zsuffix (dt DATE, tm TIME, ts TIMESTAMP)");
        }
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO t_zsuffix VALUES (?,?,?)")) {
            TypeMapper.bind(ps, 1, logical("io.debezium.time.IsoDate"), "2024-02-29Z");
            TypeMapper.bind(ps, 2, logical("io.debezium.time.IsoTime"), "23:59:59.999999Z");
            TypeMapper.bind(ps, 3, logical("io.debezium.time.IsoTimestamp"), "2026-01-02T03:04:05.678901Z");
            ps.execute();
        }
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("SELECT * FROM t_zsuffix")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getObject(1, LocalDate.class)).isEqualTo(LocalDate.of(2024, 2, 29));
            assertThat(rs.getObject(2, java.time.LocalTime.class)).isEqualTo(java.time.LocalTime.of(23, 59, 59, 999_999_000));
            assertThat(rs.getObject(3, LocalDateTime.class)).isEqualTo(LocalDateTime.of(2026, 1, 2, 3, 4, 5, 678_901_000));
        }
    }

    /** MySQL 空间族(Geometry/Point):Struct{wkb,srid} 取 WKB 落 BLOB,hex-staging 往返无损 */
    @Test
    void geometryStructMapsToBlobViaWkb() throws SQLException {
        Schema geo = SchemaBuilder.struct().name("io.debezium.data.geometry.Geometry")
                .field("wkb", Schema.BYTES_SCHEMA)
                .field("srid", Schema.OPTIONAL_INT32_SCHEMA)
                .optional().build();
        assertThat(TypeMapper.duckType(geo)).isEqualTo("BLOB");

        byte[] wkb = {0x01, 0x01, 0x00, 0x00, 0x00, 0x2A}; // 任意 WKB 字节
        Struct point = new Struct(geo).put("wkb", wkb).put("srid", 4326);
        // staging 文本=hex,castExpr(BLOB)=unhex——真 DuckDB 回读闭环
        String staged = TypeMapper.stagingText(geo, point);
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE t_geo AS SELECT " + TypeMapper.castExpr("v", "BLOB")
                    + " AS g FROM (SELECT '" + staged + "' AS v)");
        }
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("SELECT g FROM t_geo")) {
            rs.next();
            assertThat(rs.getBytes(1)).isEqualTo(wkb);
        }
    }

    /** MySQL 特有逻辑类型按物理类型兜底：Year→INTEGER、Enum/EnumSet→VARCHAR、Bits→BLOB */
    @Test
    void mysqlLogicalTypesFallBackToPhysical() {
        assertThat(TypeMapper.duckType(SchemaBuilder.int32().name("io.debezium.time.Year").build()))
                .isEqualTo("INTEGER");
        assertThat(TypeMapper.duckType(SchemaBuilder.string().name("io.debezium.data.Enum").build()))
                .isEqualTo("VARCHAR");
        assertThat(TypeMapper.duckType(SchemaBuilder.string().name("io.debezium.data.EnumSet").build()))
                .isEqualTo("VARCHAR");
        assertThat(TypeMapper.duckType(SchemaBuilder.bytes().name("io.debezium.data.Bits").build()))
                .isEqualTo("BLOB");
    }

    /** 时间族毒丸防护:TRY_CAST 让 MySQL TIME>24h/zero-date 个别值置 NULL 而非整批 crash loop */
    @Test
    void timeFamilyCastExprUsesTryCastAgainstPoisonValues() throws SQLException {
        assertThat(TypeMapper.castExpr("c", "TIME")).isEqualTo("TRY_CAST(\"c\" AS TIME)");
        assertThat(TypeMapper.castExpr("c", "DATE")).isEqualTo("TRY_CAST(\"c\" AS DATE)");
        assertThat(TypeMapper.castExpr("c", "TIMESTAMP")).isEqualTo("TRY_CAST(\"c\" AS TIMESTAMP)");
        assertThat(TypeMapper.castExpr("c", "TIMESTAMPTZ")).isEqualTo("TRY_CAST(\"c\" AS TIMESTAMPTZ)");
        assertThat(TypeMapper.castExpr("c", "BIGINT")).isEqualTo("CAST(\"c\" AS BIGINT)"); // 非时间族仍严格 CAST

        // 真 DuckDB 验证:毒丸值(MySQL TIME 合法值域 ±838h / zero-date)不炸、正常值无损
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(
                "SELECT TRY_CAST('838:59:59' AS TIME), TRY_CAST('0000-00-00' AS DATE), TRY_CAST('12:34:56' AS TIME)")) {
            rs.next();
            assertThat(rs.getObject(1)).isNull();
            assertThat(rs.getObject(2)).isNull();
            assertThat(rs.getObject(3, java.time.LocalTime.class)).isEqualTo(java.time.LocalTime.of(12, 34, 56));
        }
    }

    /** DECIMAL 超过 DuckDB 上限(p>38 或 s>37)落 VARCHAR 保全精度(无损优先,不阻塞链路) */
    @Test
    void oversizedDecimalFallsBackToVarchar() {
        Schema p45 = Decimal.builder(2).parameter("connect.decimal.precision", "45").optional().build();
        assertThat(TypeMapper.duckType(p45)).isEqualTo("VARCHAR");
        assertThat(TypeMapper.duckType(Decimal.schema(40))).isEqualTo("VARCHAR");  // scale>37
        // 边界内正常映射(无 precision 参数默认按 38 处理)
        assertThat(TypeMapper.duckType(Decimal.schema(2))).isEqualTo("DECIMAL(38,2)");
    }

    /** PG 数组 → DuckDB LIST:类型推导 + staging list 字面量编码 + TRY_CAST 真引擎往返 */
    @Test
    void pgArrayMapsToDuckListWithRoundTrip() throws SQLException {
        Schema longArr = SchemaBuilder.array(Schema.OPTIONAL_INT64_SCHEMA).optional().build();
        Schema strArr = SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build();
        assertThat(TypeMapper.duckType(longArr)).isEqualTo("BIGINT[]");
        assertThat(TypeMapper.duckType(strArr)).isEqualTo("VARCHAR[]");
        // 嵌套数组 / BYTES 元素:staging 文本表达不了,整列 VARCHAR 兜底
        assertThat(TypeMapper.duckType(SchemaBuilder.array(longArr).build())).isEqualTo("VARCHAR");
        assertThat(TypeMapper.duckType(SchemaBuilder.array(Schema.BYTES_SCHEMA).build())).isEqualTo("VARCHAR");
        assertThat(TypeMapper.castExpr("c", "BIGINT[]")).isEqualTo("TRY_CAST(\"c\" AS BIGINT[])");

        // 毒元素全覆盖往返:含逗号/含双引号/NULL 元素经 staging 编码 → TRY_CAST 还原
        String staged = TypeMapper.stagingText(strArr, java.util.Arrays.asList("a,b", "c\"d", null));
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(
                "SELECT t[1], t[2], t[3] FROM (SELECT TRY_CAST('" + staged + "' AS VARCHAR[]) AS t)")) {
            rs.next();
            assertThat(rs.getString(1)).isEqualTo("a,b");
            assertThat(rs.getString(2)).isEqualTo("c\"d");
            assertThat(rs.getString(3)).isNull();
        }
        String nums = TypeMapper.stagingText(longArr, java.util.List.of(1L, 2L, 3L));
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(
                "SELECT list_sum(TRY_CAST('" + nums + "' AS BIGINT[]))")) {
            rs.next();
            assertThat(rs.getLong(1)).isEqualTo(6L);
        }
    }

    /** PG interval(ISO8601 文本出流) → INTERVAL;MySQL BIGINT UNSIGNED(无符号文本) → UBIGINT——
     *  真 DuckDB 往返验证(interval 语义保全可运算;uint64 最大值无损) */
    @Test
    void intervalAndUnsignedBigintRoundTrip() throws SQLException {
        assertThat(TypeMapper.duckType(logical("io.debezium.time.Interval"))).isEqualTo("INTERVAL");
        assertThat(TypeMapper.duckType(logical("ducklake.UBigInt"))).isEqualTo("UBIGINT");
        assertThat(TypeMapper.castExpr("c", "INTERVAL")).isEqualTo("TRY_CAST(\"c\" AS INTERVAL)");
        assertThat(TypeMapper.castExpr("c", "UBIGINT")).isEqualTo("CAST(\"c\" AS UBIGINT)");

        // Debezium ISO8601 → DuckDB 认的 PG 风格(1.5.4 实测不解析 ISO 形态,必须转换)
        Schema iv = logical("io.debezium.time.Interval");
        assertThat(TypeMapper.stagingText(iv, "P1Y2M3DT4H5M6.7S"))
                .isEqualTo("1 years 2 months 3 days 4 hours 5 minutes 6.7 seconds");
        assertThat(TypeMapper.stagingText(iv, "PT4H")).isEqualTo("4 hours");
        assertThat(TypeMapper.stagingText(iv, "P-1Y")).isEqualTo("-1 years");
        assertThat(TypeMapper.stagingText(iv, "garbage")).isEqualTo("garbage"); // 解析不上退原文

        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(
                "SELECT date_part('month', TRY_CAST('"
                        + TypeMapper.stagingText(iv, "P1Y2M3DT4H5M6.7S") + "' AS INTERVAL)), "
                        + "TRY_CAST('garbage' AS INTERVAL), "
                        + "CAST('18446744073709551615' AS UBIGINT)")) {
            rs.next();
            assertThat(rs.getLong(1)).as("转换后 interval 可解析可运算").isEqualTo(2L);
            assertThat(rs.getObject(2)).as("毒丸 interval 置 NULL 不断链").isNull();
            assertThat(rs.getString(3)).isEqualTo("18446744073709551615");
        }
    }

    /** LIST 列的 information_schema 形态归一(否则 TIMESTAMPTZ[] 恒判漂移,每批误触类型跟随) */
    @Test
    void normalizeHandlesListSuffix() {
        assertThat(TypeMapper.normalizeDuckType("TIMESTAMP WITH TIME ZONE[]")).isEqualTo("TIMESTAMPTZ[]");
        assertThat(TypeMapper.normalizeDuckType("BIGINT[]")).isEqualTo("BIGINT[]");
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
