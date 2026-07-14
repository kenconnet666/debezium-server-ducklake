package org.dpdns.zerodep.ducklake.engine;

import io.debezium.data.Envelope;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

/**
 * TRUNCATE 事件抢救 SMT（挂在 unwrap 之前）：把 op=t 的 envelope 记录改装成
 * 非 envelope 的扁平标记行 {__op:'t'}，随后的 ExtractNewRecordState 对非 envelope
 * 记录原样透传（与心跳消息同路径）——绕开其对 truncate 记录的硬编码丢弃
 * （AbstractExtractRecordStrategy.handleTruncateRecord 恒 return null，无配置可改，
 * 3.3/3.6 反编译实测）。消费者按 __op='t' 识别为 TRUNCATE 段执行湖表清空。
 * <p>
 * 非 truncate 记录零改动直通；PG 源下 pgoutput 的 truncate 消息默认已被连接器
 * skipped.operations=t 跳过，本 SMT 天然空转——两源共用无需分支。
 */
public class TruncateRescueTransform implements Transformation<SourceRecord> {

    /** 改装后的标记行 schema：仅 __op 一列（消费者 isTruncateEvent 的识别锚点） */
    private static final Schema MARKER_SCHEMA = SchemaBuilder.struct()
            .name("org.dpdns.zerodep.ducklake.TruncateMarker")
            .field("__op", Schema.STRING_SCHEMA)
            .build();

    @Override
    public SourceRecord apply(SourceRecord record) {
        if (!(record.value() instanceof Struct value) || !Envelope.isEnvelopeSchema(value.schema())) {
            return record;
        }
        Object op = value.schema().field(Envelope.FieldName.OPERATION) != null
                ? value.get(Envelope.FieldName.OPERATION) : null;
        if (!Envelope.Operation.TRUNCATE.code().equals(op)) {
            return record;
        }
        Struct marker = new Struct(MARKER_SCHEMA).put("__op", Envelope.Operation.TRUNCATE.code());
        // key 置 null(truncate 事件本就无 key);sourcePartition/offset 随 newRecord 保留,offset 照常推进
        return record.newRecord(record.topic(), record.kafkaPartition(),
                null, null, MARKER_SCHEMA, marker, record.timestamp());
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // 无配置项
    }

    @Override
    public void close() {
        // 无资源
    }
}
