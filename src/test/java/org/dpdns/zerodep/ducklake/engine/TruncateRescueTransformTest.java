package org.dpdns.zerodep.ducklake.engine;

import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * TruncateRescueTransform 单测：op=t 的 envelope 改装成 {__op:'t'} 标记行
 * （绕开 unwrap 对 truncate 的硬编码丢弃），其余记录（含非 envelope 的心跳/schema change）
 * 零改动直通。
 */
class TruncateRescueTransformTest {

    private final TruncateRescueTransform transform = new TruncateRescueTransform();

    private static final Schema SOURCE = SchemaBuilder.struct().name("io.debezium.connector.mysql.Source")
            .field("db", Schema.OPTIONAL_STRING_SCHEMA).build();

    /** 标准 Debezium envelope（含 op/before/after/source） */
    private static Schema envelopeSchema() {
        Schema row = SchemaBuilder.struct().name("shop.t.Value")
                .field("id", Schema.OPTIONAL_INT64_SCHEMA).optional().build();
        return Envelope.defineSchema()
                .withName("shop.t.Envelope")
                .withRecord(row)
                .withSource(SOURCE)
                .build().schema();
    }

    @Test
    void truncateEnvelopeBecomesMarkerRow() {
        Schema env = envelopeSchema();
        Struct value = new Struct(env)
                .put(Envelope.FieldName.OPERATION, Envelope.Operation.TRUNCATE.code())
                .put(Envelope.FieldName.SOURCE, new Struct(SOURCE).put("db", "shop"));
        SourceRecord rec = new SourceRecord(Map.of("p", 1), Map.of("o", 2), "ducklake.shop.t", env, value);

        SourceRecord out = transform.apply(rec);

        assertThat(out.topic()).isEqualTo("ducklake.shop.t");
        assertThat(out.key()).isNull();
        Struct marker = (Struct) out.value();
        assertThat(marker.getString("__op")).isEqualTo("t");
        assertThat(marker.schema().fields()).hasSize(1); // 纯标记行,不带 envelope 结构
        assertThat(out.sourceOffset()).isEqualTo(Map.of("o", 2)); // offset 保留,照常推进
    }

    @Test
    void nonTruncateAndNonEnvelopePassThroughUntouched() {
        // 普通 c 事件:原样直通
        Schema env = envelopeSchema();
        Struct create = new Struct(env)
                .put(Envelope.FieldName.OPERATION, Envelope.Operation.CREATE.code())
                .put(Envelope.FieldName.SOURCE, new Struct(SOURCE).put("db", "shop"));
        SourceRecord cRec = new SourceRecord(Map.of(), Map.of(), "ducklake.shop.t", env, create);
        assertThat(transform.apply(cRec)).isSameAs(cRec);

        // 非 envelope(心跳/schema change 形态):原样直通
        Schema hb = SchemaBuilder.struct().name("Heartbeat").field("ts_ms", Schema.INT64_SCHEMA).build();
        SourceRecord hbRec = new SourceRecord(Map.of(), Map.of(), "__debezium-heartbeat.ducklake",
                hb, new Struct(hb).put("ts_ms", 1L));
        assertThat(transform.apply(hbRec)).isSameAs(hbRec);

        // tombstone(value=null):原样直通
        SourceRecord tomb = new SourceRecord(Map.of(), Map.of(), "ducklake.shop.t", null, null);
        assertThat(transform.apply(tomb)).isSameAs(tomb);
    }
}
