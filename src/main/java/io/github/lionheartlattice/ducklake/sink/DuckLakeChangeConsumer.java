package io.github.lionheartlattice.ducklake.sink;

import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.ChangeEvent;
import io.github.lionheartlattice.ducklake.config.DucklakeProperties;
import io.github.lionheartlattice.ducklake.ddl.DdlApplier;
import io.github.lionheartlattice.ducklake.metrics.SyncState;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
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
 * CDC 批消费者：Debezium 事件 → DuckLake append。
 * <p>
 * 核心约定：
 * <ul>
 *   <li><b>有序段合并（ordered run coalescing）</b>：严格按事件到达顺序处理，只把"连续同表同结构"
 *       的事件合并成一个 INSERT 批——不做全批按表分组，从而保证 DDL 与前后 DML 的先后语义
 *       （单槽复制流按事务提交序全局有序）。</li>
 *   <li><b>单批一个湖事务</b>：整个 handleBatch 在一个 DuckDB 事务里，失败整体回滚并抛出，
 *       引擎按 at-least-once 重投同一批；重复投递靠 __lsn 幂等（查询层窗口函数按 __lsn 取最新）。</li>
 *   <li><b>append 变更流</b>：不做 MERGE（DuckLake 无主键约束、delete file 有读放大），
 *       DELETE 以 __deleted='true' 的整行墓碑落湖；当前态由查询层
 *       row_number() OVER (PARTITION BY id ORDER BY __lsn DESC) 取。</li>
 *   <li><b>建表/加列由事件 schema 驱动</b>（含 unwrap 追加的 __op/__deleted/__lsn/__source_ts_ms/__db/__table 列）；
 *       rename 语义由 DDL 审计流（{@link DdlApplier}）补齐——两者幂等互补。</li>
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

    /** 已确保存在的湖表 → 已知列及其类型（避免每批 DESCRIBE；类型用于漂移检测与安全放宽） */
    private final Map<String, Map<String, String>> knownColumns = new HashMap<>();

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
        // 有限重试吸收湖端瞬时故障（rustfs 抖动/catalog PG 切主）；写入已整批回滚，重做安全。
        // 全部失败才抛出 → 引擎致命退出 → Runner 回调让进程退出 → 容器重启后从上个 offset 重放
        // （at-least-once，查询层按 __lsn 幂等去重）。
        long maxSourceTs = -1;
        for (int attempt = 1; maxSourceTs < 0; attempt++) {
            try {
                maxSourceTs = engine.withLock(conn -> writeBatchInTx(conn, records));
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
        // 湖事务已提交 → 才允许引擎推进 offset
        for (ChangeEvent<SourceRecord, SourceRecord> r : records) {
            committer.markProcessed(r);
        }
        committer.markBatchFinished();
        syncState.batchCommitted(records.size(), maxSourceTs);
    }

    /**
     * 在单个 DuckDB 事务内按序写入整批；返回本批最大源提交时刻(ms)。
     * 事务用 JDBC API 控制（DuckDB JDBC 对 SQL 字面 BEGIN 与 autoCommit 状态机的交互有坑，
     * 实测手写 BEGIN 会报 "cannot start a transaction within a transaction"）。
     */
    private long writeBatchInTx(Connection conn, List<ChangeEvent<SourceRecord, SourceRecord>> records) throws SQLException {
        long maxSourceTs = 0;
        conn.setAutoCommit(false);
        try {
            List<Struct> run = new ArrayList<>();
            String runTopic = null;
            Schema runSchema = null;

            for (ChangeEvent<SourceRecord, SourceRecord> event : records) {
                SourceRecord record = event.value();
                if (record.value() == null) {
                    continue; // tombstone 兜底跳过（rewrite 模式下正常不出现）
                }
                Struct value = (Struct) record.value();
                String topic = record.topic();

                // 段边界：换表或换结构（同表 DDL 前后的新旧结构不混批）
                if (runTopic != null && (!runTopic.equals(topic) || runSchema != value.schema())) {
                    flushRun(conn, runTopic, runSchema, run);
                    run.clear();
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
                flushRun(conn, runTopic, runSchema, run);
            }
            conn.commit();
            return maxSourceTs;
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
        }
    }

    /** 写出一个连续段：DDL 审计流走 DdlApplier，业务流 append 进 cdc.<schema>_<table> */
    private void flushRun(Connection conn, String topic, Schema schema, List<Struct> run) throws SQLException {
        TableRef ref = TableRef.parse(topic);
        if (props.getMaintenance().getDdlAuditTables().contains(ref.table())) {
            ddlApplier.apply(conn, run, this::invalidateColumns);
            return;
        }
        String lakeTable = props.getMaintenance().getCdcSchema() + "." + ref.pgSchema() + "_" + ref.table();
        ensureTable(conn, lakeTable, schema);

        List<Field> fields = schema.fields();
        String cols = String.join(", ", fields.stream().map(f -> '"' + f.name() + '"').toList());
        String marks = String.join(", ", fields.stream().map(f -> "?").toList());
        String sql = "INSERT INTO " + DuckLakeEngine.LAKE + "." + lakeTable + " (" + cols + ") VALUES (" + marks + ")";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (Struct row : run) {
                for (int i = 0; i < fields.size(); i++) {
                    Field f = fields.get(i);
                    TypeMapper.bind(ps, i + 1, f.schema(), row.get(f));
                }
                ps.addBatch();
            }
            ps.executeBatch();
        }
        log.debug("append {} rows -> {}", run.size(), lakeTable);
    }

    /** 首见建表；schema 出现新字段则 ALTER ADD COLUMN，已有字段类型漂移则按白名单安全放宽
     *  （与 DDL 信号流的 rename/删列应用幂等互补） */
    private void ensureTable(Connection conn, String lakeTable, Schema schema) throws SQLException {
        // 类型无法就地转换的表:上一批已标记重建并触发增量快照,本批(干净事务)开头先 DROP,
        // 随后走建表分支按事件 schema 的新类型重建,当批数据正常写入
        if (pendingRebuild.remove(lakeTable)) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP TABLE IF EXISTS " + DuckLakeEngine.LAKE + "." + lakeTable);
            }
            knownColumns.remove(lakeTable);
            syncState.getDdlApplied().increment();
            log.warn("按新类型重建湖表(历史当前态由增量快照重灌): {}", lakeTable);
        }
        Map<String, String> cols = knownColumns.get(lakeTable);
        if (cols == null) {
            StringBuilder ddl = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                    .append(DuckLakeEngine.LAKE).append('.').append(lakeTable).append(" (");
            List<Field> fields = schema.fields();
            for (int i = 0; i < fields.size(); i++) {
                Field f = fields.get(i);
                ddl.append(i > 0 ? ", " : "").append('"').append(f.name()).append("\" ").append(TypeMapper.duckType(f.schema()));
            }
            ddl.append(')');
            try (Statement s = conn.createStatement()) {
                s.execute(ddl.toString());
            }
            cols = loadColumns(conn, lakeTable);
            knownColumns.put(lakeTable, cols);
            log.info("湖表就绪: {} ({} 列)", lakeTable, cols.size());
        }
        for (Field f : schema.fields()) {
            String want = TypeMapper.duckType(f.schema());
            String have = cols.get(f.name());
            if (have == null) {
                try (Statement s = conn.createStatement()) {
                    s.execute("ALTER TABLE " + DuckLakeEngine.LAKE + "." + lakeTable
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
            s.execute("ALTER TABLE " + DuckLakeEngine.LAKE + "." + lakeTable
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
            requestIncrementalSnapshot(lakeTable);
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
            want.put(f.name(), TypeMapper.duckType(f.schema()));
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
        String tmp = tableName + "__typemig";
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS " + DuckLakeEngine.LAKE + "." + schemaName + "." + tmp);
            s.execute("CREATE TABLE " + DuckLakeEngine.LAKE + "." + schemaName + "." + tmp
                    + " AS SELECT " + proj + " FROM " + DuckLakeEngine.LAKE + "." + lakeTable);
            s.execute("DROP TABLE " + DuckLakeEngine.LAKE + "." + lakeTable);
            s.execute("ALTER TABLE " + DuckLakeEngine.LAKE + "." + schemaName + "." + tmp
                    + " RENAME TO " + tableName);
        }
        Map<String, String> fresh = loadColumns(conn, lakeTable);
        cols.clear();
        cols.putAll(fresh);
        syncState.getDdlApplied().increment();
        log.warn("湖表类型跟随(整表 CAST 重写): {} 列型={}", lakeTable, fresh);
    }

    /**
     * 向源库 signal 表写 execute-snapshot，触发 Debezium 对该表的增量快照（READ 事件重灌当前态）。
     * signal 行由连接器内部消费不进变更流；表由维护任务随 DDL 信号表一并 TRUNCATE。
     */
    private void requestIncrementalSnapshot(String lakeTable) {
        // cdc.public_sys_user → public.sys_user（cdcSchema 前缀去掉后,首个 '_' 即 pg schema 分隔）
        String pgTable = lakeTable.substring(lakeTable.indexOf('.') + 1).replaceFirst("_", ".");
        DucklakeProperties.Source src = props.getSource();
        String url = "jdbc:postgresql://%s:%d/%s".formatted(src.getHostname(), src.getPort(), src.getDbname());
        String sql = "INSERT INTO " + src.getSignalTable() + " (id, type, data) VALUES (?, 'execute-snapshot', ?)";
        try (Connection c = java.sql.DriverManager.getConnection(url, src.getUser(), src.getPassword());
             PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, java.util.UUID.randomUUID().toString());
            ps.setString(2, "{\"data-collections\":[\"" + pgTable + "\"],\"type\":\"incremental\"}");
            ps.executeUpdate();
            log.warn("已触发增量快照重拉: {} (湖表已按新类型重建,当前态将重灌)", pgTable);
        } catch (SQLException e) {
            log.error("增量快照 signal 写入失败,需人工重拉 {}: {}", pgTable, e.getMessage());
        }
    }

    /** DDL 流 rename 后由 DdlApplier 调用，失效列缓存 */
    public void invalidateColumns(String lakeTable) {
        knownColumns.remove(lakeTable);
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

    /** topic "zadmin.public.cdc_test" → (public, cdc_test)；防御性处理仅两段的极端情况 */
    record TableRef(String pgSchema, String table) {
        static TableRef parse(String topic) {
            String[] parts = topic.split("\\.");
            return parts.length >= 3
                    ? new TableRef(parts[parts.length - 2], parts[parts.length - 1])
                    : new TableRef("public", parts[parts.length - 1]);
        }
    }
}
