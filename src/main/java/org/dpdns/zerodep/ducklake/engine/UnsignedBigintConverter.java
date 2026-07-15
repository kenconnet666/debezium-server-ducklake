package org.dpdns.zerodep.ducklake.engine;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.Properties;

/**
 * MySQL {@code BIGINT UNSIGNED} 列的自定义转换器（CustomConverter SPI）：
 * 以无符号十进制文本出流（自定义逻辑名 {@code ducklake.UBigInt}），TypeMapper 据此映射
 * DuckDB 原生 {@code UBIGINT}（8 字节整数运算），替代 bigint.unsigned.handling.mode=precise
 * 的 Decimal(20,0)（16 字节 decimal 路径）——值域同为 0~2^64-1，无损且更省更快。
 * <p>
 * 两阶段值形态防御（converter 在快照与流式都回调，输入类型不同）：
 * 快照 JDBC 给 BigInteger/BigDecimal；流式 binlog 解码给原始 8 字节的有符号 long
 * （高位值表现为负数），用 {@link Long#toUnsignedString} 还原无符号语义。
 */
public class UnsignedBigintConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    /** TypeMapper 映射 UBIGINT 的分发锚点 */
    public static final String LOGICAL_NAME = "ducklake.UBigInt";

    @Override
    public void configure(Properties props) {
        // 无配置项
    }

    @Override
    public void converterFor(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        String typeName = column.typeName();
        if (typeName == null || !typeName.toUpperCase().contains("BIGINT")
                || !typeName.toUpperCase().contains("UNSIGNED")) {
            return; // 其余列走默认转换
        }
        registration.register(SchemaBuilder.string().name(LOGICAL_NAME).optional(), value -> switch (value) {
            case null -> null;
            case Long l -> Long.toUnsignedString(l);        // 流式:binlog 原始 8 字节位
            case Number n -> n.toString();                   // 快照:JDBC BigInteger/BigDecimal
            default -> value.toString();
        });
    }
}
