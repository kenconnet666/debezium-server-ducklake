package org.dpdns.zerodep.ducklake.sink;

import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.ChangeEvent;
import org.dpdns.zerodep.ducklake.config.DucklakeProperties;
import org.dpdns.zerodep.ducklake.ddl.DdlApplier;
import org.dpdns.zerodep.ducklake.metrics.SyncState;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBConnection;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * CDC 批消费者：Debezium 事件 → DuckLake 当前态镜像（批量 upsert/delete）。
 * <p>
 * 核心约定：
 * <ul>
 *   <li><b>有序段合并（ordered run coalescing）</b>：严格按事件到达顺序处理，只把"连续同表同结构"
 *       的事件合并成一个应用批——不做全批按表分组，从而保证 DDL 与前后 DML 的先后语义
 *       （单槽复制流按事务提交序全局有序）。</li>
 *   <li><b>单批一个湖事务</b>：整个 handleBatch 在一个 DuckDB 事务里，失败整体回滚并抛出，
 *       引擎按 at-least-once 重投同一批；重放天然幂等（同一批再 upsert 一遍结果一致）。</li>
 *   <li><b>镜像语义</b>：湖表 = 主库当前态，列与源表一一对应（不落任何 __* 元列）。
 *       每段两步 SQL：先按主键 DELETE 本段涉及的行（merge-on-read position delete），
 *       再 INSERT 段内每个主键的最后版本（墓碑除外）——UPDATE/DELETE 物理跟随，
 *       查询层直接 SELECT 即当前态，无需窗口函数去重。时间旅行由 DuckLake snapshot
 *       独立提供（保留期内可 AT (TIMESTAMP ...) 回看，不影响默认查询）。</li>
 *   <li><b>无主键表降级 insert-only</b>：Debezium 事件无 key 时（源表无 PK）湖侧无法定位行，
 *       UPDATE/DELETE 不跟随（每表告警一次）——PG 逻辑复制本就要求有 PK 的表才有完整语义。</li>
 *   <li><b>建表/加列由事件 schema 驱动</b>（unwrap 元列剥离后）；rename 语义由 DDL 审计流
 *       （{@link DdlApplier}）补齐——两者幂等互补。</li>
 * </ul>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DuckLakeChangeConsumer
        implements DebeziumEngine.ChangeConsumer<ChangeEvent<SourceRecord, SourceRecord>> {

    private final DuckLakeEngine engine;
    private final DucklakeProperties props;
    private final DdlApplier ddlApplier;
    private final SyncState syncState;

    /** unwrap SMT 追加的元列（staging 工作列，不进湖表；见 DebeziumEngineRunner 的 add.fields） */
    static final Set<String> META_COLS = Set.of("__op", "__table", "__lsn", "__db", "__source_ts_ms", "__deleted");

    /** 已确保存在的湖表 → 已知列及其类型（避免每批 DESCRIBE；类型用于漂移检测与安全放宽） */
    private final Map<String, Map<String, String>> knownColumns = new HashMap<>();

    /** 无主键已告警的湖表（降级 insert-only 每表只提示一次） */
    private final Set<String> noKeyWarned = new HashSet<>();

    /** 类型漂移已告警的 表.列（followTypeChange=false 时每列只告警一次，防高频批刷屏） */
    private final Set<String> typeDriftWarned = new HashSet<>();

    /** 待重建的湖表（类型无法就地转换时标记；重试批在干净事务开头 DROP 重建，历史由增量快照重灌）。
     *  刻意不随批回滚清空：标记要跨重试存活才能兑现重建 */
    private final Set<String> pendingRebuild = new HashSet<>();

    @Override
    public void handleBatch(List<ChangeEvent<SourceRecord, SourceRecord>> records,
                            DebeziumEngine.RecordCommitter<ChangeEvent<SourceRecord, SourceRecord>> committer)
            throws InterruptedException {
        if (records.isEmpty()) {
            committer.markBatchFinished();
            return;
        }
        // 心跳记录仅确认 offset+推水位,不进湖:空闲期靠它周期确认 slot——否则 publication 内表
        // 空闲时 confirmed_flush_lsn 冻结,实例级 WAL(含维护任务写 catalog 自产的)被无限扣留
        long heartbeatTs = -1;
        List<ChangeEvent<SourceRecord, SourceRecord>> data = new ArrayList<>(records.size());
        for (ChangeEvent<SourceRecord, SourceRecord> r : records) {
            String topic = r.value() != null ? r.value().topic() : null;
            if (topic != null && topic.startsWith("__debezium-heartbeat")) {
                Object v = r.value().value();
                if (v instanceof Struct hb) {
                    Long ts = fieldAsLong(hb, "ts_ms");
                    if (ts != null && ts > heartbeatTs) {
                        heartbeatTs = ts;
                    }
                }
            } else {
                data.add(r);
            }
        }
        if (data.isEmpty()) {
            for (ChangeEvent<SourceRecord, SourceRecord> r : records) {
                committer.markProcessed(r);
            }
            committer.markBatchFinished();
            if (heartbeatTs > 0) {
                syncState.heartbeat(heartbeatTs);
            }
            return;
        }
        // 有限重试吸收湖端瞬时故障（rustfs 抖动/catalog PG 切主）；写入已整批回滚，重做安全。
        // 全部失败才抛出 → 引擎致命退出 → Runner 回调让进程退出 → 容器重启后从上个 offset 重放
        // （at-least-once，镜像 upsert 天然幂等：重放同批=先删后插同样结果）。data=剥离心跳后的数据事件
        long deliveredAt = System.currentTimeMillis();
        // 诊断:空转模式只确认不写湖,测纯解码+交付吞吐——定位吞吐天花板是解码瓶颈还是写侧瓶颈
        if (props.getEngine().isDryRun()) {
            long maxTs = 0;
            for (ChangeEvent<SourceRecord, SourceRecord> event : records) {
                SourceRecord rec = event.value();
                if (rec != null && rec.value() instanceof Struct v) {
                    Long ts = fieldAsLong(v, "__source_ts_ms");
                    if (ts != null && ts > maxTs) {
                        maxTs = ts;
                    }
                }
            }
            for (ChangeEvent<SourceRecord, SourceRecord> r : records) {
                committer.markProcessed(r);
            }
            committer.markBatchFinished();
            syncState.batchCommitted(records.size(), maxTs,
                    maxTs > 0 ? deliveredAt - maxTs : -1, 0, 0);
            return;
        }
        WriteStats stats = null;
        for (int attempt = 1; stats == null; attempt++) {
            try {
                stats = engine.withLock(conn -> writeBatchInTx(conn, data));
            } catch (SQLException e) {
                syncState.getBatchFailures().increment();
                if (attempt >= 3) {
                    throw new RuntimeException("落湖连续 %d 次失败，放弃本批交给进程级重启: %s"
                            .formatted(attempt, e.getMessage()), e);
                }
                long sleepMs = props.getEngine().getRetrySleepBaseMs() << attempt;
                log.warn("落湖失败(第 {} 次)，{}ms 后重试: {}", attempt, sleepMs, e.getMessage());
                Thread.sleep(sleepMs);
            }
        }
        // 湖事务已提交 → 才允许引擎推进 offset（含本批剥离出的心跳记录,一并 mark）
        for (ChangeEvent<SourceRecord, SourceRecord> r : records) {
            committer.markProcessed(r);
        }
        committer.markBatchFinished();
        long ackTs = Math.max(stats.maxSourceTs(), heartbeatTs); // 心跳与数据取较新者推进水位
        syncState.batchCommitted(data.size(), ackTs,
                ackTs > 0 ? deliveredAt - ackTs : -1,
                stats.stageMs(), stats.lakeTxMs());
    }

    /** 一个连续段：同表同结构的事件序列（段边界 = 换表或换结构）；keyColumns=Debezium key
     *  的列名（源表主键，同表恒定，取段首事件即可；空=无主键表，降级 insert-only） */
    private record Segment(String topic, Schema schema, List<String> keyColumns, List<Struct> rows) {
    }

    /** 单次成功写批的水位与分段耗时（deliver 滞后由调用方以交付时刻减 maxSourceTs 得出） */
    private record WriteStats(long maxSourceTs, long stageMs, long lakeTxMs) {
    }

    /**
     * 两阶段写入整批；返回本批最大源提交时刻(ms)。
     * <p>
     * <b>阶段一（湖事务外）</b>：数据段经 DuckDB Appender 物化到本地 main 库的 staging 表
     * （全 VARCHAR 列，microbench 实测行成本 0.004ms vs prepared-batch 0.85ms，降两个数量级）。
     * DuckDB 单事务只能写一个 attached 库，staging(memory) 写入必须在湖事务外——
     * 也因此湖事务开始于 staging 物化之后，快照隔离下对 staging 数据可见。
     * <b>阶段二（单湖事务）</b>：按段序 INSERT INTO lake ... SELECT 带 CAST 投影 FROM staging；
     * DDL 信号段在同一事务内按原序应用——批原子性与 DDL/DML 先后语义与旧路径完全一致。
     * 失败回滚重试时阶段一 CREATE OR REPLACE 覆盖残留，天然幂等。
     * <p>
     * 事务用 JDBC API 控制（DuckDB JDBC 对 SQL 字面 BEGIN 与 autoCommit 状态机的交互有坑，
     * 实测手写 BEGIN 会报 "cannot start a transaction within a transaction"）。
     */
    private WriteStats writeBatchInTx(Connection conn, List<ChangeEvent<SourceRecord, SourceRecord>> records) throws SQLException {
        long t0 = System.currentTimeMillis();
        // ---- 切段（连续同表同结构），顺带收集水位与主键列 ----
        List<Segment> segments = new ArrayList<>();
        long maxSourceTs = 0;
        List<Struct> run = new ArrayList<>();
        String runTopic = null;
        Schema runSchema = null;
        List<String> runKeys = List.of();
        for (ChangeEvent<SourceRecord, SourceRecord> event : records) {
            SourceRecord record = event.value();
            if (record.value() == null) {
                continue; // tombstone 兜底跳过（rewrite 模式下正常不出现）
            }
            Struct value = (Struct) record.value();
            String topic = record.topic();
            if (runTopic != null && (!runTopic.equals(topic) || runSchema != value.schema())) {
                segments.add(new Segment(runTopic, runSchema, runKeys, List.copyOf(run)));
                run.clear();
            }
            if (run.isEmpty()) {
                runKeys = keyColumnsOf(record);
            }
            runTopic = topic;
            runSchema = value.schema();
            run.add(value);

            Long ts = fieldAsLong(value, "__source_ts_ms");
            if (ts != null && ts > maxSourceTs) {
                maxSourceTs = ts;
            }
        }
        if (!run.isEmpty()) {
            segments.add(new Segment(runTopic, runSchema, runKeys, List.copyOf(run)));
        }

        // ---- 阶段一：湖事务外，数据段物化到 main.stg_seg_<i> ----
        for (int i = 0; i < segments.size(); i++) {
            if (!isDdlSignal(segments.get(i).topic())) {
                stageSegment(conn, segments.get(i), i);
            }
        }
        long t1 = System.currentTimeMillis();

        // ---- 阶段二：单湖事务内按段序应用 ----
        conn.setAutoCommit(false);
        try {
            for (int i = 0; i < segments.size(); i++) {
                Segment seg = segments.get(i);
                if (isDdlSignal(seg.topic())) {
                    ddlApplier.apply(conn, seg.rows(), this::invalidateColumns, this::rebuildWithResnapshot);
                } else {
                    insertFromStaging(conn, seg, i);
                }
            }
            conn.commit();
            return new WriteStats(maxSourceTs, t1 - t0, System.currentTimeMillis() - t1);
        } catch (SQLException | RuntimeException e) {
            try {
                conn.rollback();
            } catch (SQLException ignored) {
            }
            // 事务里的 CREATE TABLE/ADD COLUMN 已随回滚消失，列缓存必须一并作废，
            // 否则重试批会向"缓存认为存在"的表插入（部署实测踩坑）
            knownColumns.clear();
            throw e;
        } finally {
            try {
                conn.setAutoCommit(true);
            } catch (SQLException ignored) {
            }
            dropStagings(conn, segments.size());
        }
    }

    private boolean isDdlSignal(String topic) {
        return props.getMaintenance().getDdlAuditTables().contains(TableRef.parse(topic).table());
    }

    /** 阶段一：段数据经 Appender 灌入本地内存库 stg_seg_<idx>（全 VARCHAR 列，autocommit 下执行）。
     *  ⚠️ 一律三段名/三参 appender 显式指到内存 catalog——worker 会话 USE lake 下
     *  两段名 main.x 解析为 lake.main.x，staging 会静默落湖（功能正确但每批多 3 次 catalog 提交） */
    private void stageSegment(Connection conn, Segment seg, int idx) throws SQLException {
        List<Field> fields = seg.schema().fields();
        StringBuilder ddl = new StringBuilder("CREATE OR REPLACE TABLE ")
                .append(DuckLakeEngine.MEM).append(".main.stg_seg_").append(idx).append(" (");
        for (int i = 0; i < fields.size(); i++) {
            ddl.append(i > 0 ? ", " : "").append('"').append(fields.get(i).name()).append("\" VARCHAR");
        }
        ddl.append(')');
        try (Statement s = conn.createStatement()) {
            s.execute(ddl.toString());
        }
        DuckDBConnection dc = conn.unwrap(DuckDBConnection.class);
        try (DuckDBAppender ap = dc.createAppender(DuckLakeEngine.MEM, "main", "stg_seg_" + idx)) {
            for (Struct row : seg.rows()) {
                ap.beginRow();
                for (Field f : fields) {
                    ap.append(TypeMapper.stagingText(f.schema(), row.get(f)));
                }
                ap.endRow();
            }
        }
    }

    /**
     * 阶段二：staging → 湖表镜像应用（湖事务内，两步 SQL）：
     * ① 按主键 DELETE 本段涉及的全部行（c 事件空命中幂等；重放=先删旧版本再插，upsert 语义）
     * ② INSERT 段内每个主键的最后版本（QUALIFY 按 __lsn 取批内终态；__deleted 墓碑不插）
     * 无主键段（keyColumns 空）降级 insert-only：跳过①，②不去重不滤墓碑无从谈起——仅插非墓碑行。
     */
    private void insertFromStaging(Connection conn, Segment seg, int idx) throws SQLException {
        TableRef ref = TableRef.parse(seg.topic());
        // 镜像命名:湖 schema = <前缀><pg_schema>,表名原样(lake."my-public".demo / lake.public.demo)
        String lakeTable = props.getMaintenance().getSchemaPrefix() + ref.pgSchema() + "." + ref.table();
        ensureTable(conn, lakeTable, seg.schema());

        List<Field> fields = seg.schema().fields().stream()
                .filter(f -> !META_COLS.contains(f.name())).toList();
        String cols = String.join(", ", fields.stream().map(f -> '"' + f.name() + '"').toList());
        String proj = String.join(", ", fields.stream()
                .map(f -> TypeMapper.castExpr(f.name(), TypeMapper.duckType(f.schema()))).toList());
        String stg = DuckLakeEngine.MEM + ".main.stg_seg_" + idx;
        String lake = DuckLakeEngine.LAKE + "." + DuckLakeEngine.quoted(lakeTable);

        if (seg.keyColumns().isEmpty()) {
            if (noKeyWarned.add(lakeTable)) {
                log.warn("源表无主键,湖侧降级 insert-only(UPDATE/DELETE 不跟随): {}", lakeTable);
            }
            try (Statement s = conn.createStatement()) {
                s.execute("INSERT INTO " + lake + " (" + cols + ") SELECT " + proj + " FROM " + stg
                        + " WHERE COALESCE(\"__deleted\", 'false') <> 'true'");
            }
            return;
        }

        // key 匹配条件:staging 全 VARCHAR,按湖列类型 CAST 后与湖行比较(编码由 TypeMapper 配对保真)
        String keyMatch = String.join(" AND ", seg.keyColumns().stream()
                .map(k -> "t.\"" + k + "\" = " + castKey("s", k, seg.schema())).toList());
        String partition = String.join(", ", seg.keyColumns().stream().map(k -> '"' + k + '"').toList());
        try (Statement s = conn.createStatement()) {
            s.execute("DELETE FROM " + lake + " t WHERE EXISTS (SELECT 1 FROM " + stg + " s WHERE " + keyMatch + ")");
            s.execute("INSERT INTO " + lake + " (" + cols + ") SELECT " + proj + " FROM ("
                    + "SELECT * FROM " + stg
                    + " QUALIFY row_number() OVER (PARTITION BY " + partition
                    + " ORDER BY CAST(\"__lsn\" AS BIGINT) DESC NULLS LAST) = 1"
                    + ") WHERE COALESCE(\"__deleted\", 'false') <> 'true'");
        }
        log.debug("mirror {} rows -> {}", seg.rows().size(), lakeTable);
    }

    /** staging 的 key 列按湖类型 CAST（类型取自 value schema 同名字段） */
    private static String castKey(String alias, String col, Schema valueSchema) {
        Field f = valueSchema.field(col);
        String type = f != null ? TypeMapper.duckType(f.schema()) : "VARCHAR";
        return "CAST(" + alias + ".\"" + col + "\" AS " + type + ")";
    }

    /** Debezium key struct 的列名（源表主键）；无 key（无 PK 表）返回空 */
    private static List<String> keyColumnsOf(SourceRecord record) {
        if (record.key() instanceof Struct k) {
            return k.schema().fields().stream().map(Field::name).toList();
        }
        return List.of();
    }

    /** staging 清理（事务外 best-effort；内存 instance 的表，进程重启自然消失） */
    private void dropStagings(Connection conn, int count) {
        try (Statement s = conn.createStatement()) {
            for (int i = 0; i < count; i++) {
                s.execute("DROP TABLE IF EXISTS " + DuckLakeEngine.MEM + ".main.stg_seg_" + i);
            }
        } catch (SQLException e) {
            log.debug("staging 清理失败(无碍,下批 CREATE OR REPLACE 覆盖): {}", e.getMessage());
        }
    }

    /** 首见建表（湖 schema 一并按需建）；schema 出现新字段则 ALTER ADD COLUMN，
     *  已有字段类型漂移则按白名单安全放宽（与 DDL 信号流的 rename/删列应用幂等互补） */
    private void ensureTable(Connection conn, String lakeTable, Schema schema) throws SQLException {
        // 类型无法就地转换的表:上一批已标记重建并触发增量快照,本批(干净事务)开头先 DROP,
        // 随后走建表分支按事件 schema 的新类型重建,当批数据正常写入
        if (pendingRebuild.remove(lakeTable)) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP TABLE IF EXISTS " + DuckLakeEngine.LAKE + "." + DuckLakeEngine.quoted(lakeTable));
            }
            knownColumns.remove(lakeTable);
            syncState.getDdlApplied().increment();
            log.warn("按新类型重建湖表(历史当前态由增量快照重灌): {}", lakeTable);
        }
        Map<String, String> cols = knownColumns.get(lakeTable);
        if (cols == null) {
            String lakeSchema = lakeTable.substring(0, lakeTable.indexOf('.'));
            StringBuilder ddl = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                    .append(DuckLakeEngine.LAKE).append('.').append(DuckLakeEngine.quoted(lakeTable)).append(" (");
            // 镜像语义:湖表列=源表列一一对应,unwrap 元列(__op/__lsn/...)只留在 staging 不进湖
            List<Field> fields = schema.fields().stream()
                    .filter(f -> !META_COLS.contains(f.name())).toList();
            for (int i = 0; i < fields.size(); i++) {
                Field f = fields.get(i);
                ddl.append(i > 0 ? ", " : "").append('"').append(f.name()).append("\" ").append(TypeMapper.duckType(f.schema()));
            }
            ddl.append(')');
            try (Statement s = conn.createStatement()) {
                // 湖 schema 首见按需建(镜像 pg schema,可带前缀;事务回滚会连带回滚,缓存同 knownColumns 一并清)
                s.execute("CREATE SCHEMA IF NOT EXISTS " + DuckLakeEngine.LAKE + ".\"" + lakeSchema + '"');
                s.execute(ddl.toString());
            }
            cols = loadColumns(conn, lakeTable);
            knownColumns.put(lakeTable, cols);
            log.info("湖表就绪: {} ({} 列)", lakeTable, cols.size());
        }
        for (Field f : schema.fields()) {
            if (META_COLS.contains(f.name())) {
                continue;
            }
            String want = TypeMapper.duckType(f.schema());
            String have = cols.get(f.name());
            if (have == null) {
                try (Statement s = conn.createStatement()) {
                    s.execute("ALTER TABLE " + DuckLakeEngine.LAKE + "." + DuckLakeEngine.quoted(lakeTable)
                            + " ADD COLUMN IF NOT EXISTS \"" + f.name() + "\" " + want);
                }
                cols.put(f.name(), want);
                log.warn("湖表加列: {}.{} ({})", lakeTable, f.name(), want);
            } else if (!have.equals(want)) {
                followTypeChange(conn, lakeTable, schema, f.name(), have, want, cols);
            }
        }
    }

    /**
     * 类型严格跟随（数据驱动：源库 ALTER COLUMN TYPE 后事件 schema 即带新类型），逐级执行：
     * ① ALTER COLUMN SET DATA TYPE（DuckLake type promotion 支持的免重写变更，零成本）
     * ② 失败则湖内整表 CAST 重写（历史数据保留并严格转为新类型；溢出即失败进入 ③）
     * ③ 删湖表按新 schema 重建，并向源库 signal 表触发 Debezium 增量快照重拉该表
     *    （历史墓碑链让位于当前态重灌——"必要时旧数据重新拉取"的兜底）
     * followTypeChange=false 时仅告警一次，湖列保守不动。
     */
    private void followTypeChange(Connection conn, String lakeTable, Schema schema,
                                  String col, String have, String want,
                                  Map<String, String> cols) throws SQLException {
        if (!props.getMaintenance().isFollowTypeChange()) {
            if (typeDriftWarned.add(lakeTable + "." + col)) {
                log.warn("湖列类型与事件漂移(followTypeChange=false 仅告警): {}.{} 湖={} 事件={}",
                        lakeTable, col, have, want);
            }
            return;
        }
        try (Statement s = conn.createStatement()) {
            s.execute("ALTER TABLE " + DuckLakeEngine.LAKE + "." + DuckLakeEngine.quoted(lakeTable)
                    + " ALTER COLUMN \"" + col + "\" SET DATA TYPE " + want);
            cols.put(col, want);
            syncState.getDdlApplied().increment();
            log.warn("湖列类型跟随(ALTER): {}.{} {} -> {}", lakeTable, col, have, want);
            return;
        } catch (SQLException e) {
            log.info("类型跟随 ALTER 不支持,转湖内整表重写: {}.{} {} -> {} ({})",
                    lakeTable, col, have, want, e.getMessage());
        }
        try {
            rewriteTableWithCasts(conn, lakeTable, schema, cols);
            return;
        } catch (SQLException e) {
            log.warn("湖内 CAST 重写失败(历史值超新类型范围等),转重建+增量快照重拉: {} ({})",
                    lakeTable, e.getMessage());
        }
        // ③ 不能在当前(已有失败语句的)湖事务里继续 DROP/重建——标记重建 + 触发增量快照,
        //    抛错让本批走既有重试:重试批在干净事务开头完成 DROP 重建(见 ensureTable),
        //    当批数据按新类型写入,历史当前态由增量快照重灌
        if (pendingRebuild.add(lakeTable)) {
            requestSnapshot(lakeTable, "incremental");
        }
        throw new SQLException("湖列类型无法就地转换,已安排重建+增量快照重拉,本批将重试: "
                + lakeTable + " (" + col + " " + have + " -> " + want + ")");
    }

    /**
     * 湖内整表 CAST 重写：以湖现列为基（列序保持），事件 schema 给出目标类型的列做严格 CAST，
     * 湖独有列（rename 复活列/历史列）原样保留；tmp 表交换完成。与当批数据写入同湖事务，原子；
     * 大表有全量重写 IO 代价（类型变更本就是低频运维动作）。
     */
    private void rewriteTableWithCasts(Connection conn, String lakeTable, Schema schema,
                                       Map<String, String> cols) throws SQLException {
        Map<String, String> want = new LinkedHashMap<>();
        for (Field f : schema.fields()) {
            if (!META_COLS.contains(f.name())) {
                want.put(f.name(), TypeMapper.duckType(f.schema()));
            }
        }
        StringBuilder proj = new StringBuilder();
        for (Map.Entry<String, String> c : cols.entrySet()) {
            String target = want.get(c.getKey());
            proj.append(proj.isEmpty() ? "" : ", ");
            if (target != null && !target.equals(c.getValue())) {
                proj.append("CAST(\"").append(c.getKey()).append("\" AS ").append(target)
                        .append(") AS \"").append(c.getKey()).append('"');
            } else {
                proj.append('"').append(c.getKey()).append('"');
            }
        }
        String schemaName = lakeTable.substring(0, lakeTable.indexOf('.'));
        String tableName = lakeTable.substring(lakeTable.indexOf('.') + 1);
        String qTmp = DuckLakeEngine.LAKE + ".\"" + schemaName + "\".\"" + tableName + "__typemig\"";
        String qTable = DuckLakeEngine.LAKE + "." + DuckLakeEngine.quoted(lakeTable);
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS " + qTmp);
            s.execute("CREATE TABLE " + qTmp + " AS SELECT " + proj + " FROM " + qTable);
            s.execute("DROP TABLE " + qTable);
            s.execute("ALTER TABLE " + qTmp + " RENAME TO \"" + tableName + '"');
        }
        Map<String, String> fresh = loadColumns(conn, lakeTable);
        cols.clear();
        cols.putAll(fresh);
        syncState.getDdlApplied().increment();
        log.warn("湖表类型跟随(整表 CAST 重写): {} 列型={}", lakeTable, fresh);
    }

    /**
     * 向源库 signal 表写 execute-snapshot，触发 Debezium 对该表的快照重灌（READ 事件重灌当前态）。
     * type=incremental（分块、不停流，要求 connector 已知主键）或 blocking（同 initial 流程、
     * 重读表结构、该表快照期间流式短暂停顿）。signal 行由连接器内部消费不进变更流；
     * 表由维护任务随 DDL 信号表一并 TRUNCATE。
     */
    private void requestSnapshot(String lakeTable, String type) {
        // 镜像命名反向:<前缀><pg_schema>.<表> → <pg_schema>.<表>(剥掉配置前缀)
        String prefix = props.getMaintenance().getSchemaPrefix();
        String pgTable = lakeTable.startsWith(prefix) ? lakeTable.substring(prefix.length()) : lakeTable;
        DucklakeProperties.Source src = props.getSource();
        String url = "jdbc:postgresql://%s:%d/%s".formatted(src.getHostname(), src.getPort(), src.getDbname());
        String sql = "INSERT INTO " + src.getSignalTable() + " (id, type, data) VALUES (?, 'execute-snapshot', ?)";
        try (Connection c = java.sql.DriverManager.getConnection(url, src.getUser(), src.getPassword());
             PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, java.util.UUID.randomUUID().toString());
            ps.setString(2, "{\"data-collections\":[\"" + pgTable + "\"],\"type\":\"" + type + "\"}");
            ps.executeUpdate();
            log.warn("已触发 {} 快照重拉: {} (湖表将重建,当前态将重灌)", type, pgTable);
        } catch (SQLException e) {
            log.error("快照 signal 写入失败,需人工重拉 {}: {}", pgTable, e.getMessage());
        }
    }

    /** DDL 流 rename 后由 DdlApplier 调用，失效列缓存 */
    public void invalidateColumns(String lakeTable) {
        knownColumns.remove(lakeTable);
    }

    /** 主键变更（存量表补主键等）由 DdlApplier 回调：湖表标记重建 + 触发 blocking 快照重灌。
     *  存量行的主键回填不产生 CDC 事件、湖旧行主键恒 NULL——重建后快照 READ 事件按新主键
     *  重灌当前态（兑现点在 ensureTable：首个 READ 批到达时干净事务里 DROP 重建）。
     *  ⚠️ 必须用 blocking 而非 incremental：pgoutput 只在该表下一条 DML 时才刷新 connector
     *  内部 schema，增量快照此刻仍按旧 schema 判"无主键"直接 skip（集成测试实测踩坑）；
     *  blocking 快照走 initial 同款流程、从 JDBC metadata 重读表结构，能拿到新主键
     *  （代价=该表快照期间流式短暂停顿，低频运维操作可接受）。
     *  标记是进程内存态：崩溃丢失由 at-least-once 兜底（重放审计事件会重新标记，
     *  signal 重复写只是多触发一轮幂等快照） */
    public void rebuildWithResnapshot(String lakeTable) {
        if (pendingRebuild.add(lakeTable)) {
            requestSnapshot(lakeTable, "blocking");
            log.warn("主键变更,湖表将重建并由 blocking 快照重灌当前态: {}", lakeTable);
        }
    }

    private Map<String, String> loadColumns(Connection conn, String lakeTable) throws SQLException {
        String[] parts = lakeTable.split("\\.", 2);
        // LinkedHashMap + ordinal_position:保留列序,整表 CAST 重写按原列序投影
        Map<String, String> cols = new LinkedHashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT column_name, data_type FROM information_schema.columns WHERE table_catalog = ? AND table_schema = ? AND table_name = ? ORDER BY ordinal_position")) {
            ps.setString(1, DuckLakeEngine.LAKE);
            ps.setString(2, parts[0]);
            ps.setString(3, parts[1]);
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    cols.put(rs.getString(1), TypeMapper.normalizeDuckType(rs.getString(2)));
                }
            }
        }
        return cols;
    }

    private static Long fieldAsLong(Struct value, String field) {
        if (value.schema().field(field) == null) {
            return null;
        }
        Object v = value.get(field);
        return v instanceof Number n ? n.longValue() : null;
    }

    /** topic "ducklake.public.cdc_test" → (public, cdc_test)；防御性处理仅两段的极端情况 */
    record TableRef(String pgSchema, String table) {
        static TableRef parse(String topic) {
            String[] parts = topic.split("\\.");
            return parts.length >= 3
                    ? new TableRef(parts[parts.length - 2], parts[parts.length - 1])
                    : new TableRef("public", parts[parts.length - 1]);
        }
    }
}
