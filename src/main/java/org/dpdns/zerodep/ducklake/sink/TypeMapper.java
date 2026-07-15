package org.dpdns.zerodep.ducklake.sink;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

/**
 * Kafka Connect Schema（Debezium 事件字段）→ DuckDB 列类型与值绑定。
 * <p>
 * 与引擎配置的两个类型模式配套（见 DebeziumEngineRunner）：
 * <ul>
 *   <li>{@code decimal.handling.mode=precise}：PG numeric → Connect {@link Decimal} 逻辑类型（BigDecimal）。
 *       zadmin 主键是 numeric(28) 雪花 ID，double 只有 15-16 位有效数字会静默丢精度——precise 是硬要求。</li>
 *   <li>{@code time.precision.mode=isostring}：全部时间类型 → ISO-8601 字符串，按逻辑名映射回
 *       DuckDB 原生时间类型（分析友好），解析失败退回 VARCHAR 原文。</li>
 * </ul>
 * 未识别类型一律 VARCHAR + toString 兜底（打日志），保证不丢数据。
 */
final class TypeMapper {

    private TypeMapper() {
    }

    /** Debezium isostring 时间逻辑类型名 */
    private static final String ISO_DATE = "io.debezium.time.IsoDate";
    private static final String ISO_TIME = "io.debezium.time.IsoTime";
    private static final String ISO_TIMESTAMP = "io.debezium.time.IsoTimestamp";
    private static final String ZONED_TIMESTAMP = "io.debezium.time.ZonedTimestamp";
    private static final String ZONED_TIME = "io.debezium.time.ZonedTime";
    private static final String PG_JSON = "io.debezium.data.Json";
    private static final String PG_UUID = "io.debezium.data.Uuid";
    /** 裸 numeric（无 (p,s) 精度声明）在 precise 模式下的逻辑类型：scale 逐值可变的 Struct */
    private static final String VARIABLE_DECIMAL = "io.debezium.data.VariableScaleDecimal";
    /** 空间类型（MySQL GEOMETRY 族 / PG postgis）：Struct{wkb BYTES, srid INT32}——
     *  取 WKB 落 BLOB（DuckDB spatial 扩展可 ST_GeomFromWKB 解析），srid 不单列保存 */
    private static final String GEOMETRY = "io.debezium.data.geometry.Geometry";
    private static final String GEO_POINT = "io.debezium.data.geometry.Point";
    private static final String GEOGRAPHY = "io.debezium.data.geometry.Geography";
    /** PG interval（interval.handling.mode=string 下 ISO8601 文本出流）→ DuckDB 原生 INTERVAL */
    private static final String INTERVAL = "io.debezium.time.Interval";
    /** MySQL BIGINT UNSIGNED（UnsignedBigintConverter 以无符号文本出流）→ DuckDB UBIGINT */
    private static final String UBIGINT = "ducklake.UBigInt";

    /** Connect 字段 schema → DuckDB 列类型 DDL 片段 */
    static String duckType(Schema schema) {
        String logical = schema.name();
        if (logical != null) {
            switch (logical) {
                case Decimal.LOGICAL_NAME -> {
                    // 忠实对应源精度:numeric(p,s) → DECIMAL(p,s)(元数据保真;width≤18 走 int64
                    // 存储更省更快)。DuckDB 合法域 1≤p≤38、0≤s≤p(38,38 亦合法,1.5.4 实测);
                    // 域外(p>38 / s>p / 负 s)落 VARCHAR 保全精度(无损优先,不有损压缩)
                    int scale = Integer.parseInt(schema.parameters().getOrDefault(Decimal.SCALE_FIELD, "0"));
                    int precision = Integer.parseInt(
                            schema.parameters().getOrDefault("connect.decimal.precision", "38"));
                    if (precision < 1 || precision > 38 || scale < 0 || scale > precision) {
                        return "VARCHAR";
                    }
                    return "DECIMAL(" + precision + "," + scale + ")";
                }
                case ISO_DATE -> {
                    return "DATE";
                }
                case ISO_TIME -> {
                    return "TIME";
                }
                case ZONED_TIME -> {
                    // PG timetz:保全时区偏移(DuckLake 支持 TIMETZ,1.5.4 实测;旧映射 TIME 丢偏移)
                    return "TIMETZ";
                }
                case ISO_TIMESTAMP -> {
                    return "TIMESTAMP";
                }
                case ZONED_TIMESTAMP -> {
                    return "TIMESTAMPTZ";
                }
                case PG_JSON -> {
                    return "JSON";
                }
                case PG_UUID -> {
                    return "UUID";
                }
                case VARIABLE_DECIMAL -> {
                    // 裸 numeric(无精度声明,scale 逐值可变)→ DuckDB 上限形态 DECIMAL(38,18):
                    // 整数位 20 位(int64 雪花 ID 19 位无忧)+小数 18 位,可直接聚合运算;
                    // 超 18 位小数 CAST 四舍五入(1.5.4 实测),超 20 位整数溢出阻塞保完整性——
                    // 值域更大的列请在源侧声明 numeric(p,s) 获得忠实对应
                    return "DECIMAL(38,18)";
                }
                case GEOMETRY, GEO_POINT, GEOGRAPHY -> {
                    return "BLOB";
                }
                case INTERVAL -> {
                    return "INTERVAL";
                }
                case UBIGINT -> {
                    return "UBIGINT";
                }
                default -> {
                    // 其他逻辑类型（Enum/EnumSet/Year/Bits/Ltree/Xml…）按物理类型走下方 switch
                    // （MySQL ENUM/SET→STRING→VARCHAR、YEAR→INT32→INTEGER、BIT(n>1)→BYTES→BLOB）
                }
            }
        }
        return switch (schema.type()) {
            case INT8, INT16 -> "SMALLINT";
            case INT32 -> "INTEGER";
            case INT64 -> "BIGINT";
            case FLOAT32 -> "FLOAT";
            case FLOAT64 -> "DOUBLE";
            case BOOLEAN -> "BOOLEAN";
            case BYTES -> "BLOB";
            // PG 一维数组 → DuckDB LIST(元素类型递归推导);嵌套数组/BYTES 元素等
            // staging 文本编码表达不了的形态退 VARCHAR 兜底
            case ARRAY -> {
                String elem = duckType(schema.valueSchema());
                yield switch (elem) {
                    case "BLOB", "VARCHAR" -> schema.valueSchema().type() == Schema.Type.STRING
                            ? "VARCHAR[]" : "VARCHAR";
                    default -> elem.endsWith("[]") ? "VARCHAR" : elem + "[]";
                };
            }
            default -> "VARCHAR";
        };
    }

    /** 绑定一个字段值到 PreparedStatement（与 {@link #duckType} 的列类型一一对应） */
    static void bind(PreparedStatement ps, int idx, Schema schema, Object value) throws SQLException {
        if (value == null) {
            ps.setNull(idx, Types.NULL);
            return;
        }
        String logical = schema.name();
        if (logical != null) {
            switch (logical) {
                case Decimal.LOGICAL_NAME -> {
                    ps.setBigDecimal(idx, (BigDecimal) value);
                    return;
                }
                case ISO_DATE -> {
                    ps.setObject(idx, LocalDate.parse(stripTrailingZ(value)));
                    return;
                }
                case ISO_TIME -> {
                    ps.setObject(idx, LocalTime.parse(stripTrailingZ(value)));
                    return;
                }
                case ISO_TIMESTAMP -> {
                    ps.setObject(idx, LocalDateTime.parse(stripTrailingZ(value)));
                    return;
                }
                case ZONED_TIMESTAMP -> {
                    ps.setObject(idx, OffsetDateTime.parse(value.toString()));
                    return;
                }
                case ZONED_TIME, PG_JSON, PG_UUID -> {
                    ps.setString(idx, value.toString());
                    return;
                }
                case VARIABLE_DECIMAL -> {
                    // Struct{scale, value(unscaled bytes)} → BigDecimal(列型 DECIMAL(38,18))
                    ps.setBigDecimal(idx, io.debezium.data.VariableScaleDecimal
                            .toLogical((org.apache.kafka.connect.data.Struct) value)
                            .getDecimalValue().orElse(null));
                    return;
                }
                case GEOMETRY, GEO_POINT, GEOGRAPHY -> {
                    ps.setBytes(idx, geometryWkb(value));
                    return;
                }
                default -> {
                    // 落到物理类型分支
                }
            }
        }
        switch (schema.type()) {
            case INT8, INT16 -> ps.setShort(idx, ((Number) value).shortValue());
            case INT32 -> ps.setInt(idx, ((Number) value).intValue());
            case INT64 -> ps.setLong(idx, ((Number) value).longValue());
            case FLOAT32 -> ps.setFloat(idx, ((Number) value).floatValue());
            case FLOAT64 -> ps.setDouble(idx, ((Number) value).doubleValue());
            case BOOLEAN -> ps.setBoolean(idx, (Boolean) value);
            case BYTES -> ps.setBytes(idx, value instanceof ByteBuffer bb ? toBytes(bb) : (byte[]) value);
            default -> ps.setString(idx, value.toString());
        }
    }

    /**
     * information_schema.columns.data_type 的形态归一到 {@link #duckType} 的产出形态,
     * 供湖列现型与事件类型比对(差异仅时区时间戳一处;LIST 列递归归一元素类型,
     * 否则 "TIMESTAMP WITH TIME ZONE[]" 与事件 "TIMESTAMPTZ[]" 恒不等,每批误触类型跟随)。
     */
    static String normalizeDuckType(String infoSchemaType) {
        String t = infoSchemaType == null ? "" : infoSchemaType.toUpperCase();
        if (t.endsWith("[]")) {
            return normalizeDuckType(t.substring(0, t.length() - 2)) + "[]";
        }
        return switch (t) {
            case "TIMESTAMP WITH TIME ZONE" -> "TIMESTAMPTZ";
            case "TIME WITH TIME ZONE" -> "TIMETZ";
            default -> t;
        };
    }

    /**
     * Appender-staging 高速写入路径的文本编码：字段值 → staging 表(全 VARCHAR 列)的字符串形态。
     * 与 {@link #castExpr} 的投影表达式配对构成无损往返——是 {@link #bind} 的文本化等价物
     * (microbench:prepared-batch 0.85ms/行 vs Appender+staging 0.004ms/行,行成本降两个数量级)。
     */
    static String stagingText(Schema schema, Object value) {
        if (value == null) {
            return null;
        }
        String logical = schema.name();
        if (logical != null) {
            switch (logical) {
                case Decimal.LOGICAL_NAME -> {
                    return ((BigDecimal) value).toPlainString();
                }
                case ISO_DATE, ISO_TIME, ISO_TIMESTAMP -> {
                    return stripTrailingZ(value);
                }
                case ZONED_TIMESTAMP, ZONED_TIME, PG_JSON, PG_UUID -> {
                    return value.toString();
                }
                case INTERVAL -> {
                    return isoIntervalToDuck(value.toString());
                }
                case VARIABLE_DECIMAL -> {
                    return io.debezium.data.VariableScaleDecimal
                            .toLogical((org.apache.kafka.connect.data.Struct) value)
                            .getDecimalValue()
                            .map(BigDecimal::toPlainString).orElse(null);
                }
                case GEOMETRY, GEO_POINT, GEOGRAPHY -> {
                    // 空间 Struct 取 WKB 走 hex staging（与 BLOB 物理路径同编码，unhex 还原）
                    byte[] wkb = geometryWkb(value);
                    return wkb == null ? null : java.util.HexFormat.of().formatHex(wkb);
                }
                default -> {
                    // 落到物理类型分支
                }
            }
        }
        return switch (schema.type()) {
            // BLOB 经 hex 编码过 VARCHAR staging,投影侧 unhex 还原(见 castExpr)
            case BYTES -> java.util.HexFormat.of()
                    .formatHex(value instanceof ByteBuffer bb ? toBytes(bb) : (byte[]) value);
            // 数组 → DuckDB list 字面量文本(与 castExpr 的 TRY_CAST ... [] 配对):
            // 元素文本递归复用本方法(时间/decimal 元素编码一致),统一双引号包裹+转义,
            // NULL 元素用裸 NULL 字面量
            case ARRAY -> {
                StringBuilder sb = new StringBuilder("[");
                java.util.List<?> list = (java.util.List<?>) value;
                for (int i = 0; i < list.size(); i++) {
                    Object elem = list.get(i);
                    sb.append(i > 0 ? ", " : "");
                    if (elem == null) {
                        sb.append("NULL");
                    } else {
                        String text = stagingText(schema.valueSchema(), elem);
                        sb.append('"').append(text == null ? "" : text.replace("\"", "\\\"")).append('"');
                    }
                }
                yield sb.append(']').toString();
            }
            default -> value.toString();
        };
    }

    /** 空间逻辑类型 Struct{wkb,srid[,x,y]} → WKB 字节。⚠️ PG 内建 point（非 PostGIS）只给
     *  x/y、wkb 为空——从坐标手工构造 21 字节 WKB point（1 字节序+4 类型+8x+8y），
     *  否则该列落湖恒 NULL（值丢失，实排查发现的缺口） */
    private static byte[] geometryWkb(Object value) {
        if (!(value instanceof org.apache.kafka.connect.data.Struct s)) {
            return null;
        }
        if (s.schema().field("wkb") != null) {
            Object wkb = s.get("wkb");
            if (wkb instanceof ByteBuffer bb) {
                return toBytes(bb);
            }
            if (wkb instanceof byte[] b && b.length > 0) {
                return b;
            }
        }
        if (s.schema().field("x") != null && s.schema().field("y") != null
                && s.get("x") instanceof Double x && s.get("y") instanceof Double y) {
            java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(21).order(java.nio.ByteOrder.LITTLE_ENDIAN);
            buf.put((byte) 1).putInt(1).putDouble(x).putDouble(y); // little-endian WKB point
            return buf.array();
        }
        return null;
    }

    /**
     * staging(VARCHAR) 列 → 湖列类型的 SELECT 投影片段(向量化 CAST,成本可忽略)。
     * 与 {@link #stagingText} 的编码规则配对。
     * <p>
     * 时间族用 TRY_CAST：MySQL 的 TIME 合法值域 ±838h 超出 DuckDB TIME(24h)、历史数据可能残留
     * zero-date('0000-00-00')——CAST 会让整批失败进 crash loop（毒丸），TRY_CAST 把无法表达的
     * 个别值置 NULL 保数据链不断（引擎侧另有 event.converting.failure=warn 第一道防线）。
     * 正常值两者语义一致、成本相同。
     */
    static String castExpr(String column, String duckType) {
        String quoted = '"' + column + '"';
        if ("VARCHAR".equals(duckType)) {
            return quoted;
        }
        if ("BLOB".equals(duckType)) {
            return "unhex(" + quoted + ")";
        }
        // LIST 与时间族同属"文本解析型 cast":个别毒丸值置 NULL 保数据链不断
        if (duckType.endsWith("[]")) {
            return "TRY_CAST(" + quoted + " AS " + duckType + ")";
        }
        return switch (duckType) {
            case "DATE", "TIME", "TIMETZ", "TIMESTAMP", "TIMESTAMPTZ", "INTERVAL" ->
                    "TRY_CAST(" + quoted + " AS " + duckType + ")";
            default -> "CAST(" + quoted + " AS " + duckType + ")";
        };
    }

    /** Debezium ISO8601 期间格式（P1Y2M3DT4H5M6.7S） */
    private static final java.util.regex.Pattern ISO_INTERVAL = java.util.regex.Pattern.compile(
            "^P(?:(-?\\d+)Y)?(?:(-?\\d+)M)?(?:(-?\\d+)D)?(?:T(?:(-?\\d+)H)?(?:(-?\\d+)M)?(?:(-?[\\d.]+)S)?)?$");

    /**
     * ISO8601 期间 → DuckDB/PG 风格 interval 文本（"1 years 2 months 3 days 4 hours ..."）。
     * DuckDB 1.5.4 实测完全不解析 ISO8601 形态（TRY_CAST 全 NULL），只认 PG 风格——
     * Debezium interval.handling.mode=string 的输出必须在此转换；解析不上退原文
     * （投影侧 TRY_CAST 兜底置 NULL，不断链）。
     */
    private static String isoIntervalToDuck(String iso) {
        java.util.regex.Matcher m = ISO_INTERVAL.matcher(iso.trim());
        if (!m.matches()) {
            return iso;
        }
        StringBuilder sb = new StringBuilder();
        append(sb, m.group(1), "years");
        append(sb, m.group(2), "months");
        append(sb, m.group(3), "days");
        append(sb, m.group(4), "hours");
        append(sb, m.group(5), "minutes");
        append(sb, m.group(6), "seconds");
        return sb.isEmpty() ? "0 seconds" : sb.toString();
    }

    private static void append(StringBuilder sb, String value, String unit) {
        if (value != null) {
            sb.append(sb.isEmpty() ? "" : " ").append(value).append(' ').append(unit);
        }
    }

    /**
     * isostring 模式下无时区族(IsoDate/IsoTime/IsoTimestamp)的输出带 'Z' 后缀(如 {@code 2024-02-29Z}),
     * 而 LocalDate/LocalTime/LocalDateTime.parse 不接受尾部 zone 标记——解析前剥掉。
     * 全类型矩阵实测毒丸:不剥则 DateTimeParseException → 批失败重放 → 引擎 crash loop。
     * 带时区族(ZonedTimestamp)走 OffsetDateTime.parse 原生支持 'Z',不经此函数。
     */
    private static String stripTrailingZ(Object value) {
        String s = value.toString();
        return s.endsWith("Z") ? s.substring(0, s.length() - 1) : s;
    }

    private static byte[] toBytes(ByteBuffer buffer) {
        ByteBuffer ro = buffer.asReadOnlyBuffer();
        byte[] bytes = new byte[ro.remaining()];
        ro.get(bytes);
        return bytes;
    }
}
