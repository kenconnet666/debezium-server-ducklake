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

    /** Connect 字段 schema → DuckDB 列类型 DDL 片段 */
    static String duckType(Schema schema) {
        String logical = schema.name();
        if (logical != null) {
            switch (logical) {
                case Decimal.LOGICAL_NAME -> {
                    // scale 在 schema 参数里,源列精度在 Debezium 附加参数里;DuckDB DECIMAL 上限 38 位——
                    // 源列 p>38(或 s>37)时 CAST 必溢出,落 VARCHAR 保全精度(与裸 numeric 同策略,无损优先)
                    int scale = Integer.parseInt(schema.parameters().getOrDefault(Decimal.SCALE_FIELD, "0"));
                    int precision = Integer.parseInt(
                            schema.parameters().getOrDefault("connect.decimal.precision", "38"));
                    if (precision > 38 || scale > 37 || scale < 0) {
                        return "VARCHAR";
                    }
                    return "DECIMAL(38," + scale + ")";
                }
                case ISO_DATE -> {
                    return "DATE";
                }
                case ISO_TIME, ZONED_TIME -> {
                    return "TIME";
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
                    // 裸 numeric 无固定 scale，DuckDB DECIMAL 必须定长——以字符串保全精度
                    // （业务表应尽量声明 numeric(p,s)；此兜底防丢数据不防难看）
                    return "VARCHAR";
                }
                case GEOMETRY, GEO_POINT, GEOGRAPHY -> {
                    return "BLOB";
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
                    // Struct{scale, value(unscaled bytes)} → BigDecimal 明文字符串
                    ps.setString(idx, io.debezium.data.VariableScaleDecimal
                            .toLogical((org.apache.kafka.connect.data.Struct) value)
                            .getDecimalValue()
                            .map(BigDecimal::toPlainString).orElse(null));
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
        return "TIMESTAMP WITH TIME ZONE".equals(t) ? "TIMESTAMPTZ" : t;
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

    /** 空间逻辑类型 Struct{wkb,srid} → WKB 字节（wkb 字段缺失/空值返回 null） */
    private static byte[] geometryWkb(Object value) {
        if (!(value instanceof org.apache.kafka.connect.data.Struct s) || s.schema().field("wkb") == null) {
            return null;
        }
        Object wkb = s.get("wkb");
        return switch (wkb) {
            case null -> null;
            case ByteBuffer bb -> toBytes(bb);
            default -> (byte[]) wkb;
        };
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
            case "DATE", "TIME", "TIMESTAMP", "TIMESTAMPTZ" ->
                    "TRY_CAST(" + quoted + " AS " + duckType + ")";
            default -> "CAST(" + quoted + " AS " + duckType + ")";
        };
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
