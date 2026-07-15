package org.dpdns.zerodep.ducklake.engine;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * UnsignedBigintConverter 单测：仅接管 BIGINT UNSIGNED 列；两阶段值形态
 * （快照 BigInteger/BigDecimal、流式原始 long 位）均还原无符号十进制文本。
 */
class UnsignedBigintConverterTest {

    private final UnsignedBigintConverter converter = new UnsignedBigintConverter();

    private record Col(String typeName) implements RelationalColumn {
        public String name() { return "c"; }
        public String dataCollection() { return "shop.t"; }
        public String typeExpression() { return typeName; }
        public int jdbcType() { return 0; }
        public int nativeType() { return 0; }
        public OptionalInt length() { return OptionalInt.empty(); }
        public OptionalInt scale() { return OptionalInt.empty(); }
        public boolean isOptional() { return true; }
        public Object defaultValue() { return null; }
        public boolean hasDefaultValue() { return false; }
    }

    /** 捕获注册结果的桩 */
    private CustomConverter.ConverterRegistration<SchemaBuilder> capture(
            AtomicReference<SchemaBuilder> schema, AtomicReference<CustomConverter.Converter> fn) {
        return (fieldSchema, converterFn) -> {
            schema.set(fieldSchema);
            fn.set(converterFn);
        };
    }

    @Test
    void takesOverOnlyUnsignedBigintWithUnsignedTextOutput() {
        converter.configure(new Properties());
        AtomicReference<SchemaBuilder> schema = new AtomicReference<>();
        AtomicReference<CustomConverter.Converter> fn = new AtomicReference<>();

        // 非目标列不接管
        converter.converterFor(new Col("BIGINT"), capture(schema, fn));
        converter.converterFor(new Col("INT UNSIGNED"), capture(schema, fn));
        converter.converterFor(new Col(null), capture(schema, fn));
        assertThat(fn.get()).isNull();

        // BIGINT UNSIGNED 接管:自定义逻辑名 + STRING 物理型
        converter.converterFor(new Col("BIGINT UNSIGNED"), capture(schema, fn));
        assertThat(fn.get()).isNotNull();
        Schema built = schema.get().build();
        assertThat(built.name()).isEqualTo(UnsignedBigintConverter.LOGICAL_NAME);
        assertThat(built.type()).isEqualTo(Schema.Type.STRING);

        // 两阶段值形态:流式原始 long 位(uint64 最大值=位表示 -1)/快照 BigInteger/BigDecimal/null
        assertThat(fn.get().convert(-1L)).isEqualTo("18446744073709551615");
        assertThat(fn.get().convert(42L)).isEqualTo("42");
        assertThat(fn.get().convert(new BigInteger("18446744073709551615"))).isEqualTo("18446744073709551615");
        assertThat(fn.get().convert(new BigDecimal("9223372036854775808"))).isEqualTo("9223372036854775808");
        assertThat(fn.get().convert(null)).isNull();
    }
}
