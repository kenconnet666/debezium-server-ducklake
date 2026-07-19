package org.dpdns.zerodep.ducklake.engine;

import lombok.extern.slf4j.Slf4j;
import org.dpdns.zerodep.ducklake.config.DucklakeProperties;
import org.dpdns.zerodep.ducklake.ddl.DdlApplier;
import org.dpdns.zerodep.ducklake.metrics.SyncState;
import org.dpdns.zerodep.ducklake.sink.DuckLakeEngine;
import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBConnection;
import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

/**
 * PostgreSQL 原生读取器：通过 pgjdbc replication API 直读 pgoutput。
 * <p>
 * 写湖使用 DuckLakeEngine.withLock + DuckDBAppender 的全 VARCHAR staging，再以
 * DELETE/QUALIFY INSERT 投影提交；类型名通过 {@link DdlApplier#pgColumnTypes} 获取。
 * <p>
 * DDL 跟随两路并行：① Relation 消息自愈（加列即刻感知）；
 * ② DDL 审计表 Insert 行经 {@link DdlApplier#applyRaw} 复用 PG 前端处理 rename/删列/删表。
 */
@Slf4j
class RawPgReader {

    private static final long PG_EPOCH_MILLIS = 946_684_800_000L;

    private final DucklakeProperties props;
    private final DuckLakeEngine engine;
    private final DdlApplier ddlApplier;
    private final SyncState syncState;
    private final RawPgOffset offset;
    private final DucklakeProperties.Source src;
    private final DucklakeProperties.Engine eng;

    /** pgoutput Relation 缓存（OID → 列元信息，首次见到时查 pg_catalog） */
    private record ColDef(String name, String duckType, boolean isKey) {}
    private record RelInfo(String schema, String table, List<ColDef> cols) {
        List<String> keyColumns() { return cols.stream().filter(ColDef::isKey).map(ColDef::name).toList(); }
    }
    private final Map<Integer, RelInfo> relations = new HashMap<>();

    /** DDL 审计表名集合（小写）——这些表的 Insert 事件走 DdlApplier 而非落湖 */
    private final Set<String> ddlAuditTables;

    /** 批次内事件保持源顺序；TRUNCATE 必须与前后 DML 在同一湖事务中按序执行。 */
    private sealed interface PendingEvent permits PendingRow, PendingTruncate {
        RelInfo rel();
    }
    private enum RowOp { INSERT, UPDATE, DELETE }
    private record TupleData(String[] values, boolean[] unchanged) {
        boolean hasUnchanged() {
            if (unchanged == null) return false;
            for (boolean value : unchanged) if (value) return true;
            return false;
        }

        boolean isUnchanged(int index) {
            return unchanged != null && unchanged[index];
        }
    }
    /** 事件携带解码时的 Relation、旧/新 tuple 与 unchanged-TOAST 快照。 */
    private record PendingRow(RelInfo rel, RowOp op, TupleData oldTuple, TupleData newTuple)
            implements PendingEvent {
        boolean deleted() { return op == RowOp.DELETE; }
        String[] values() { return deleted() ? oldTuple.values() : newTuple.values(); }
        boolean requiresPatch() {
            if (op != RowOp.UPDATE) return false;
            if (newTuple.hasUnchanged()) return true;
            if (oldTuple == null) return false;
            for (int i = 0; i < rel.cols().size(); i++) {
                if (rel.cols().get(i).isKey()
                        && !Objects.equals(oldTuple.values()[i], newTuple.values()[i])) return true;
            }
            return false;
        }
    }
    private record PendingTruncate(RelInfo rel) implements PendingEvent {}
    private enum SegmentKind { DATA, PATCH, DDL, TRUNCATE }
    private record Segment(SegmentKind kind, RelInfo rel, List<PendingRow> rows,
                           Set<List<String>> patchKeys) {}
    private record StagingTiming(long ddlNanos, long appenderNanos) {}
    private record WriteTiming(long stageMs, long lakeTxMs, long planNanos, long ensureNanos,
                               long stagingDdlNanos, long appenderNanos, long mirrorDmlNanos,
                               long lakeCommitNanos, long cleanupNanos) {}
    private final List<PendingEvent> pending = new ArrayList<>();
    /** 最近一个 Commit 消息的 end LSN（向 PG 确认后 slot 才推进） */
    private long batchEndLsn = 0;
    /** 已同步写入 catalog offset 且反馈给 slot 的 LSN，避免空闲期重复写 offset。 */
    private long persistedLsn = 0;
    /** 当前待刷批中最大的源事务提交时刻（epoch ms）。 */
    private long batchMaxSourceTsMs;
    /** pgoutput Begin/Commit 边界；未见 Commit 时绝不把半个源事务暴露到湖。 */
    private boolean inTransaction = false;
    /** DDL/TRUNCATE 事务在 Commit 立即刷出，阻断后续 Relation 自愈越过 schema 边界。 */
    private boolean flushAtCommit = false;
    private long lastFlushMs = System.currentTimeMillis();
    private long decodeNanos;

    /** 湖表已知列缓存（避免每批 DESCRIBE） */
    private final Map<String, Map<String, String>> knownColumns = new HashMap<>();
    /** 无主键已告警的湖表 */
    private final Set<String> noKeyWarned = new HashSet<>();
    /** followTypeChange=false 时避免同一列重复告警 */
    private final Set<String> typeDriftWarned = new HashSet<>();

    /**
     * per-Relation 快照 SQL 片段缓存：stageRows/applyStaging 所需字符串在首次构造后复用。
     * schema 变更（parseRelation 写入新 RelInfo / ensureTable 加列）时失效对应条目。
     * <p>
     * 收益：对单表连续高吞吐流，每批省去 stream().map().toList() + StringBuilder 的重复构造；
     * 压测场景（100k 行，maxBatchSize=8192，约12批）减少约 11 次完整 SQL 重建。
     */
    private record TableSqlParts(
            String stageCols,   // "\"c1\" VARCHAR, \"c2\" VARCHAR, ..., \"__deleted\" VARCHAR, \"__seq\" BIGINT"
            String cols,        // "\"c1\", \"c2\", ..."
            String proj,        // "CAST(\"c1\" AS ...), \"c2\", ..."
            String keyMatch,    // "t.\"id\" = CAST(s.\"id\" AS BIGINT) AND ..."
            String partition    // "\"id\""（无主键时空串）
    ) {}
    private final Map<RelInfo, TableSqlParts> sqlCache = new HashMap<>();

    private volatile boolean stopped = false;
    RawPgReader(DucklakeProperties props, DuckLakeEngine engine, DdlApplier ddlApplier,
                SyncState syncState, RawPgOffset offset) {
        this.props = props;
        this.engine = engine;
        this.ddlApplier = ddlApplier;
        this.syncState = syncState;
        this.offset = offset;
        this.src = props.getSource();
        this.eng = props.getEngine();
        this.ddlAuditTables = new HashSet<>();
        for (String table : props.getMaintenance().getDdlAuditTables()) {
            ddlAuditTables.add(table.toLowerCase(Locale.ROOT));
        }
    }

    void stop() {
        stopped = true;
    }

    boolean isStopped() {
        return stopped;
    }

    /** 主循环，由 RawPgRunner 的专用线程调用，阻塞直到 stopped 或 interrupted。 */
    void run(long startLsn, Runnable onStreamReady) throws Exception {
        persistedLsn = startLsn;
        try (Connection repl = openReplicationConn()) {
            PGReplicationStream stream = openStream(repl, startLsn);
            log.info("pgoutput 流已就绪: slot={} startLsn={}", src.getSlotName(),
                    startLsn == 0 ? "HEAD" : "0x" + Long.toHexString(startLsn));
            onStreamReady.run();
            while (!stopped && !Thread.currentThread().isInterrupted()) {
                ByteBuffer msg = stream.readPending();
                if (msg == null) {
                    maybeFlush(stream, false);
                    //noinspection BusyWait
                    Thread.sleep(eng.getPollIntervalMs());
                    continue;
                }
                handleMessage(msg, stream);
            }
        }
    }

    private void handleMessage(ByteBuffer msg, PGReplicationStream stream) throws Exception {
        byte type = msg.get();
        switch (type) {
            case 'R' -> {
                long started = System.nanoTime();
                parseRelation(msg);
                decodeNanos += System.nanoTime() - started;
            }
            case 'B' -> {
                inTransaction = true; // payload 无需解析，边界必须记录
                flushAtCommit = false;
            }
            case 'I' -> {
                long started = System.nanoTime();
                parseInsert(msg);
                decodeNanos += System.nanoTime() - started;
            }
            case 'U' -> {
                long started = System.nanoTime();
                parseUpdate(msg);
                decodeNanos += System.nanoTime() - started;
            }
            case 'D' -> {
                long started = System.nanoTime();
                parseDelete(msg);
                decodeNanos += System.nanoTime() - started;
            }
            case 'T' -> {
                long started = System.nanoTime();
                parseTruncate(msg);
                decodeNanos += System.nanoTime() - started;
            }
            case 'C' -> {
                msg.get();        // flags (unused)
                msg.getLong();    // commit lsn
                batchEndLsn = msg.getLong(); // end lsn = slot 确认位点
                batchMaxSourceTsMs = Math.max(batchMaxSourceTsMs, pgTimestampMs(msg.getLong()));
                inTransaction = false;
                maybeFlush(stream, flushAtCommit || pending.size() >= eng.getMaxBatchSize());
                flushAtCommit = false;
            }
            // Origin/Type/Message → 忽略
        }
    }
    // ──────────── pgoutput 消息解析 ────────────

    private void parseRelation(ByteBuffer msg) {
        int oid = msg.getInt();
        String schema = readCStr(msg);
        String table  = readCStr(msg);
        msg.get(); // replica identity
        int colCount = msg.getShort() & 0xFFFF;

        // 从 pgoutput 读取列名和 key 标志
        String[] names = new String[colCount];
        boolean[] isKey = new boolean[colCount];
        for (int i = 0; i < colCount; i++) {
            isKey[i] = (msg.get() & 1) != 0;
            names[i] = readCStr(msg);
            msg.getInt(); // type OID（用 pg_catalog 连同 typmod/维度解析，见下）
            msg.getInt(); // atttypmod
        }
        // 从 pg_catalog 取精确的 DuckDB 列类型
        Map<String, String> typeMap = ddlApplier.pgColumnTypes(schema, table);
        List<ColDef> cols = new ArrayList<>(colCount);
        for (int i = 0; i < colCount; i++) {
            String t = typeMap.getOrDefault(names[i], "VARCHAR");
            cols.add(new ColDef(names[i], t, isKey[i]));
        }
        RelInfo old = relations.put(oid, new RelInfo(schema, table, cols));
        if (old == null) {
            log.info("RawPg 感知表: {}.{} ({}列, key={})",
                    schema, table, colCount, cols.stream().filter(ColDef::isKey).map(ColDef::name).toList());
        } else {
            // 新旧事件各自携带 Relation；旧 SQL 片段可丢弃，需要时按快照重建。
            sqlCache.remove(old);
        }
    }

    private void parseInsert(ByteBuffer msg) {
        int oid = msg.getInt();
        msg.get(); // 'N'
        RelInfo rel = relations.get(oid);
        TupleData tuple = readTuple(msg, rel);
        if (rel != null && tuple != null) {
            pending.add(new PendingRow(rel, RowOp.INSERT, null, tuple));
            if (ddlAuditTables.contains(rel.table().toLowerCase(Locale.ROOT))) flushAtCommit = true;
        }
    }

    private void parseUpdate(ByteBuffer msg) {
        int oid = msg.getInt();
        // pgoutput Update: [K|O 旧 tuple]? N 新 tuple
        RelInfo rel = relations.get(oid);
        byte indicator = msg.get(); // 先无条件消费，可能是 K/O/N
        TupleData oldTuple = null;
        if (indicator == 'K' || indicator == 'O') {
            oldTuple = readTuple(msg, rel);
            msg.get();      // 消费后续的 'N'
        }
        // indicator == 'N' 时已在新 tuple 列数前，直接读取
        TupleData newTuple = readTuple(msg, rel);
        if (rel != null && newTuple != null) {
            pending.add(new PendingRow(rel, RowOp.UPDATE, oldTuple, newTuple));
        }
    }

    private void parseDelete(ByteBuffer msg) {
        int oid = msg.getInt();
        msg.get(); // 'K' or 'O'
        RelInfo rel = relations.get(oid);
        TupleData oldTuple = readTuple(msg, rel); // null rel 时 readTuple 内部 skipTuple 安全处理
        if (rel == null || oldTuple == null) return;
        pending.add(new PendingRow(rel, RowOp.DELETE, oldTuple, null));
    }

    /** pgoutput Truncate：Int32 relation count + Int8 options + relation OID[]。 */
    private void parseTruncate(ByteBuffer msg) {
        int relationCount = msg.getInt();
        msg.get(); // options: CASCADE / RESTART IDENTITY；湖侧只需镜像清空
        boolean follow = props.getMaintenance().isFollowTruncate();
        if (follow && relationCount > 0) flushAtCommit = true;
        for (int i = 0; i < relationCount; i++) {
            int oid = msg.getInt();
            RelInfo rel = relations.get(oid);
            // pgoutput 保证在 T 之前发送最新 R；缺失说明流不完整，不能猜测目标表。
            if (follow && rel != null) pending.add(new PendingTruncate(rel));
            else if (follow) log.warn("RawPg TRUNCATE 缺少 Relation，跳过未知 OID={}", oid);
        }
    }
    // ──────────── 批次刷湖 ────────────

    private void maybeFlush(PGReplicationStream stream, boolean force) throws Exception {
        long now = System.currentTimeMillis();
        boolean timeout = (now - lastFlushMs) >= Math.max(eng.getPollIntervalMs() * 50L, 500L);
        if (pending.isEmpty()) {
            // 即使事务全部被过滤，也要先持久化 offset 再反馈 PG，防 slot WAL 扣留。
            if (!inTransaction && Long.compareUnsigned(batchEndLsn, persistedLsn) > 0 && timeout) {
                long offsetStarted = System.nanoTime();
                try {
                    persistAndAck(stream, batchEndLsn);
                } catch (Exception e) {
                    syncState.batchFailed();
                    throw e;
                }
                recordStage(SyncState.Stage.OFFSET_ACK, System.nanoTime() - offsetStarted);
                recordStage(SyncState.Stage.DECODE, decodeNanos);
                decodeNanos = 0;
                syncState.sourceReflectedThrough(batchMaxSourceTsMs);
                batchMaxSourceTsMs = 0;
                lastFlushMs = now;
            }
            return;
        }
        // readPending 可能在 Begin 与 Commit 之间短暂无消息；不能因批超时提交半个源事务。
        if (inTransaction) return;
        if (!force && !timeout) return;

        long flushLsn = batchEndLsn;
        long maxSourceTsMs = batchMaxSourceTsMs;
        List<PendingEvent> batch = pending;
        int eventCount = batch.size();
        long t0 = System.currentTimeMillis();

        DuckLakeEngine.LockedResult<WriteTiming> locked;
        long offsetNanos = 0;
        try {
            locked = engine.withLockTimed(conn -> writeBatch(conn, batch));
            if (flushLsn > 0) {
                long offsetStarted = System.nanoTime();
                persistAndAck(stream, flushLsn);
                offsetNanos = System.nanoTime() - offsetStarted;
            }
        } catch (Exception e) {
            syncState.batchFailed();
            throw e;
        }
        WriteTiming timing = locked.value();
        pending.clear();
        batchMaxSourceTsMs = 0;
        long elapsed = System.currentTimeMillis() - t0;
        lastFlushMs = System.currentTimeMillis();
        syncState.batchCommitted(eventCount, maxSourceTsMs, deliverLag(t0, maxSourceTsMs),
                timing.stageMs(), timing.lakeTxMs());
        recordBatchTiming(timing, locked.waitNanos(), offsetNanos);
        decodeNanos = 0;
        log.debug("RawPg 落湖 {} 行 {}ms lsn=0x{}", eventCount, elapsed, Long.toHexString(flushLsn));
    }

    private void ackLsn(PGReplicationStream stream, long lsn) throws SQLException {
        LogSequenceNumber l = LogSequenceNumber.valueOf(lsn);
        stream.setAppliedLSN(l);
        stream.setFlushedLSN(l);
        stream.forceUpdateStatus();
    }

    /** Debezium 同序语义：消费者 offset 先持久化，成功后 replication slot 才能确认。 */
    private void persistAndAck(PGReplicationStream stream, long lsn) throws SQLException {
        offset.save(src.getSlotName(), lsn);
        ackLsn(stream, lsn);
        persistedLsn = lsn;
    }

    /** 两阶段写：staging（湖事务外）→ DELETE+INSERT（单湖事务）。 */
    private WriteTiming writeBatch(Connection conn, List<PendingEvent> batch) throws SQLException {
        long stageStarted = System.nanoTime();
        long planStarted = System.nanoTime();
        List<Segment> segments = segmentsOf(batch);
        long planNanos = System.nanoTime() - planStarted;
        int[] stagingIndexes = new int[segments.size()];
        Arrays.fill(stagingIndexes, -1);

        // 阶段一：staging（湖事务外）
        int stagingCount = 0;
        long ensureNanos = 0;
        long stagingDdlNanos = 0;
        long appenderNanos = 0;
        for (int i = 0; i < segments.size(); i++) {
            Segment segment = segments.get(i);
            if (segment.kind() == SegmentKind.DATA || segment.kind() == SegmentKind.PATCH) {
                RelInfo rel = segment.rel();
                // Relation 已携带当前目标类型。先在湖事务外完成建表/加列/类型迁移，避免失败的
                // ALTER 使后续 DELETE+INSERT 事务进入 aborted 状态。
                long ensureStarted = System.nanoTime();
                ensureTable(conn, lakeTableName(rel), rel);
                ensureNanos += System.nanoTime() - ensureStarted;
                stagingIndexes[i] = stagingCount;
                StagingTiming timing;
                if (segment.kind() == SegmentKind.PATCH) {
                    timing = stagePatch(conn, rel, segment.rows(), stagingCount++);
                } else {
                    timing = stageRows(conn, rel, segment.rows(), stagingCount++);
                }
                stagingDdlNanos += timing.ddlNanos();
                appenderNanos += timing.appenderNanos();
            }
        }
        long stageMs = (System.nanoTime() - stageStarted) / 1_000_000L;

        // 阶段二：单湖事务，按源事件段顺序执行 DDL / DML / TRUNCATE。
        long lakeStarted = System.nanoTime();
        long lakeTxMs;
        long mirrorDmlNanos;
        long lakeCommitNanos;
        long cleanupNanos;
        conn.setAutoCommit(false);
        try {
            long mirrorStarted = System.nanoTime();
            List<Map<String, String>> ddlRows = new ArrayList<>();
            for (int i = 0; i < segments.size(); i++) {
                Segment segment = segments.get(i);
                RelInfo rel = segment.rel();
                if (segment.kind() == SegmentKind.DDL) {
                    for (PendingRow r : segment.rows()) {
                        // 审计表只消费 INSERT；运维误用 DELETE 清理时不能把旧值当 DDL 再执行。
                        if (r.op() == RowOp.INSERT) ddlRows.add(toRowMap(rel, r));
                    }
                    continue;
                }
                applyDdlRows(conn, ddlRows);
                switch (segment.kind()) {
                    case DATA -> applyStaging(conn, rel, stagingIndexes[i]);
                    case PATCH -> applyPatch(conn, rel, segment.rows(), stagingIndexes[i]);
                    case TRUNCATE -> ddlApplier.applyRawPgTruncate(conn, lakeTableName(rel));
                    case DDL -> throw new IllegalStateException("DDL 段不应进入执行分支");
                }
            }
            applyDdlRows(conn, ddlRows);
            mirrorDmlNanos = System.nanoTime() - mirrorStarted;
            long commitStarted = System.nanoTime();
            conn.commit();
            lakeCommitNanos = System.nanoTime() - commitStarted;
            lakeTxMs = (System.nanoTime() - lakeStarted) / 1_000_000L;
        } catch (SQLException | RuntimeException ex) {
            conn.rollback();
            knownColumns.clear();
            sqlCache.clear();   // schema 可能已变，整批 SQL 缓存全部失效
            throw ex;
        } finally {
            long cleanupStarted = System.nanoTime();
            conn.setAutoCommit(true);
            dropStagings(conn, stagingCount);
            cleanupNanos = System.nanoTime() - cleanupStarted;
        }
        return new WriteTiming(stageMs, lakeTxMs, planNanos, ensureNanos, stagingDdlNanos,
                appenderNanos, mirrorDmlNanos, lakeCommitNanos, cleanupNanos);
    }

    /** 保持源顺序分段；可证明互不依赖的 unchanged-TOAST 更新才合并。 */
    private List<Segment> segmentsOf(List<PendingEvent> batch) {
        List<Segment> segments = new ArrayList<>();
        for (PendingEvent event : batch) {
            RelInfo rel = event.rel();
            if (event instanceof PendingTruncate) {
                segments.add(new Segment(SegmentKind.TRUNCATE, rel, List.of(), null));
                continue;
            }
            PendingRow row = (PendingRow) event;
            SegmentKind kind = ddlAuditTables.contains(rel.table().toLowerCase(Locale.ROOT))
                    ? SegmentKind.DDL : row.requiresPatch() ? SegmentKind.PATCH : SegmentKind.DATA;
            Segment last = segments.isEmpty() ? null : segments.getLast();
            if (kind == SegmentKind.PATCH && last != null && last.kind() == kind
                    && last.rel().equals(rel) && appendPatch(last, row)) {
                continue;
            }
            if (kind != SegmentKind.PATCH && last != null && last.kind() == kind && last.rel().equals(rel)) {
                last.rows().add(row);
            } else {
                List<PendingRow> rows = new ArrayList<>();
                rows.add(row);
                Set<List<String>> patchKeys = null;
                if (kind == SegmentKind.PATCH && batchablePatch(row)) {
                    patchKeys = new HashSet<>();
                    patchKeys.add(patchKey(row));
                }
                segments.add(new Segment(kind, rel, rows, patchKeys));
            }
        }
        return segments;
    }

    private static boolean appendPatch(Segment segment, PendingRow row) {
        if (segment.patchKeys() == null || !batchablePatch(row)) return false;
        PendingRow first = segment.rows().getFirst();
        if (!Arrays.equals(first.newTuple().unchanged(), row.newTuple().unchanged())) return false;
        if (!segment.patchKeys().add(patchKey(row))) return false;
        segment.rows().add(row);
        return true;
    }

    /** 主键不变、mask 相同且 key 唯一时，各目标行之间没有读后写依赖。 */
    private static boolean batchablePatch(PendingRow row) {
        if (!row.newTuple().hasUnchanged()) return false;
        TupleData oldTuple = row.oldTuple() == null ? row.newTuple() : row.oldTuple();
        boolean hasKey = false;
        for (int i = 0; i < row.rel().cols().size(); i++) {
            if (!row.rel().cols().get(i).isKey()) continue;
            hasKey = true;
            if (!Objects.equals(oldTuple.values()[i], row.newTuple().values()[i])) return false;
        }
        return hasKey;
    }

    private static List<String> patchKey(PendingRow row) {
        TupleData oldTuple = row.oldTuple() == null ? row.newTuple() : row.oldTuple();
        List<String> values = new ArrayList<>();
        for (int i = 0; i < row.rel().cols().size(); i++) {
            if (row.rel().cols().get(i).isKey()) values.add(oldTuple.values()[i]);
        }
        return values;
    }

    private void applyDdlRows(Connection conn, List<Map<String, String>> ddlRows) throws SQLException {
        if (ddlRows.isEmpty()) return;
        ddlApplier.applyRaw(conn, ddlRows, this::invalidate, lakeTable -> {
            invalidate(lakeTable);
            throw new IllegalStateException("RawPg 重建需要 scanner 通道，当前不可用: " + lakeTable);
        });
        ddlRows.clear();
    }

    private void invalidate(String lakeTable) {
        knownColumns.remove(lakeTable);
        invalidateSql(lakeTable);
    }
    // ──────────── Staging + Lake 写入原语 ────────────

    private StagingTiming stageRows(Connection conn, RelInfo rel, List<PendingRow> rows, int idx)
            throws SQLException {
        TableSqlParts parts = sqlCache.computeIfAbsent(rel, this::buildSqlParts);
        String stgName = DuckLakeEngine.MEM + ".main.stg_raw_" + idx;
        long ddlStarted = System.nanoTime();
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE OR REPLACE TABLE " + stgName + " (" + parts.stageCols() + ")");
        }
        long ddlNanos = System.nanoTime() - ddlStarted;
        long appenderStarted = System.nanoTime();
        DuckDBConnection dc = conn.unwrap(DuckDBConnection.class);
        try (DuckDBAppender ap = dc.createAppender(DuckLakeEngine.MEM, "main", "stg_raw_" + idx)) {
            long seq = 0;
            List<ColDef> cols = rel.cols();
            for (PendingRow row : rows) {
                ap.beginRow();
                for (int i = 0; i < cols.size(); i++) {
                    ap.append(i < row.values().length ? row.values()[i] : null);
                }
                ap.append(Boolean.toString(row.deleted()));
                ap.append(seq++);
                ap.endRow();
            }
        }
        return new StagingTiming(ddlNanos, System.nanoTime() - appenderStarted);
    }

    /** 语义更新 staging：新 tuple 全列 + 独立旧主键列。 */
    private StagingTiming stagePatch(Connection conn, RelInfo rel, List<PendingRow> rows, int idx)
            throws SQLException {
        String stgName = DuckLakeEngine.MEM + ".main.stg_raw_" + idx;
        StringBuilder ddl = new StringBuilder("CREATE OR REPLACE TABLE ").append(stgName).append(" (");
        for (int i = 0; i < rel.cols().size(); i++) {
            if (i > 0) ddl.append(", ");
            ddl.append('"').append(rel.cols().get(i).name()).append("\" VARCHAR");
        }
        List<Integer> keyIndexes = keyIndexes(rel);
        for (int i = 0; i < keyIndexes.size(); i++) {
            ddl.append(", \"").append(oldKeyStageColumn(rel, i)).append("\" VARCHAR");
        }
        ddl.append(')');
        long ddlStarted = System.nanoTime();
        try (Statement s = conn.createStatement()) {
            s.execute(ddl.toString());
        }
        long ddlNanos = System.nanoTime() - ddlStarted;

        long appenderStarted = System.nanoTime();
        DuckDBConnection dc = conn.unwrap(DuckDBConnection.class);
        try (DuckDBAppender ap = dc.createAppender(DuckLakeEngine.MEM, "main", "stg_raw_" + idx)) {
            for (PendingRow row : rows) {
                TupleData oldTuple = row.oldTuple() == null ? row.newTuple() : row.oldTuple();
                ap.beginRow();
                for (String value : row.newTuple().values()) ap.append(value);
                for (int keyIndex : keyIndexes) ap.append(oldTuple.values()[keyIndex]);
                ap.endRow();
            }
        }
        return new StagingTiming(ddlNanos, System.nanoTime() - appenderStarted);
    }

    /** DELETE + QUALIFY INSERT，实现镜像 upsert/delete 语义。 */
    private void applyStaging(Connection conn, RelInfo rel, int idx) throws SQLException {
        String lakeTable = lakeTableName(rel);
        TableSqlParts parts = sqlCache.computeIfAbsent(rel, this::buildSqlParts);
        String stg  = DuckLakeEngine.MEM + ".main.stg_raw_" + idx;
        String lake = DuckLakeEngine.LAKE + "." + DuckLakeEngine.quoted(lakeTable);

        if (parts.keyMatch().isEmpty()) {
            if (noKeyWarned.add(lakeTable)) log.warn("源表无主键，降级 insert-only: {}", lakeTable);
            try (Statement s = conn.createStatement()) {
                s.execute("INSERT INTO " + lake + " (" + parts.cols() + ") SELECT " + parts.proj()
                        + " FROM " + stg + " WHERE COALESCE(\"__deleted\", 'false') <> 'true'");
            }
            return;
        }
        try (Statement s = conn.createStatement()) {
            s.execute("DELETE FROM " + lake + " t WHERE EXISTS (SELECT 1 FROM " + stg
                    + " s WHERE " + parts.keyMatch() + ")");
            s.execute("INSERT INTO " + lake + " (" + parts.cols() + ") SELECT " + parts.proj() + " FROM ("
                    + "SELECT * FROM " + stg
                    + " QUALIFY row_number() OVER (PARTITION BY " + parts.partition()
                    + " ORDER BY \"__seq\" DESC) = 1"
                    + ") latest WHERE COALESCE(\"__deleted\", 'false') <> 'true'");
        }
    }

    /**
     * UPDATE 的语义路径：主键变化按旧 key 定位；unchanged TOAST 列不进入 SET，保留湖中旧值。
     * 同 key 连续更新仍被 planner 拆成独立段，只有互不依赖的目标行会共享一次 UPDATE。
     */
    private void applyPatch(Connection conn, RelInfo rel, List<PendingRow> rows, int idx) throws SQLException {
        PendingRow row = rows.getFirst();
        String lakeTable = lakeTableName(rel);
        List<Integer> keyIndexes = keyIndexes(rel);
        if (keyIndexes.isEmpty()) {
            if (noKeyWarned.add(lakeTable)) {
                log.warn("源表无可定位 key，unchanged-TOAST/主键更新无法镜像，跳过 UPDATE: {}", lakeTable);
            }
            return;
        }

        String lake = DuckLakeEngine.LAKE + "." + DuckLakeEngine.quoted(lakeTable);
        String stg = DuckLakeEngine.MEM + ".main.stg_raw_" + idx;
        StringBuilder set = new StringBuilder();
        for (int i = 0; i < rel.cols().size(); i++) {
            if (row.newTuple().isUnchanged(i)) continue;
            ColDef col = rel.cols().get(i);
            set.append(set.isEmpty() ? "" : ", ").append('"').append(col.name()).append("\" = ")
                    .append(castExprRaw("s.\"" + col.name() + '"', col.duckType()));
        }
        if (set.isEmpty()) return;

        StringBuilder oldKeyMatch = new StringBuilder();
        for (int i = 0; i < keyIndexes.size(); i++) {
            ColDef key = rel.cols().get(keyIndexes.get(i));
            oldKeyMatch.append(oldKeyMatch.isEmpty() ? "" : " AND ")
                    .append("t.\"").append(key.name()).append("\" IS NOT DISTINCT FROM ")
                    .append(castExprRaw("s.\"" + oldKeyStageColumn(rel, i) + '"', key.duckType()));
        }

        int updated;
        try (Statement s = conn.createStatement()) {
            updated = s.executeUpdate("UPDATE " + lake + " AS t SET " + set
                    + " FROM " + stg + " AS s WHERE " + oldKeyMatch);
        }
        if (updated < 0 || updated >= rows.size()) return;

        if (row.newTuple().hasUnchanged()) {
            // 首次异步 bootstrap 可能尚未插入基线行；不写 NULL 占位，让后续 anti-join 补全真值。
            log.warn("unchanged-TOAST UPDATE 有 {} 行未命中湖基线，跳过并等待 bootstrap: {}",
                    rows.size() - updated, lakeTable);
            return;
        }

        // 无 TOAST 缺口时可安全补插完整新 tuple（典型为 bootstrap 竞态下的主键更新）。
        if (rows.size() != 1) {
            throw new IllegalStateException("主键变化 PATCH 不应被合并: " + lakeTable);
        }
        StringBuilder cols = new StringBuilder();
        StringBuilder proj = new StringBuilder();
        for (ColDef col : rel.cols()) {
            cols.append(cols.isEmpty() ? "" : ", ").append('"').append(col.name()).append('"');
            proj.append(proj.isEmpty() ? "" : ", ")
                    .append(castExprRaw("s.\"" + col.name() + '"', col.duckType()));
        }
        StringBuilder newKeyMatch = new StringBuilder();
        for (int keyIndex : keyIndexes) {
            ColDef key = rel.cols().get(keyIndex);
            newKeyMatch.append(newKeyMatch.isEmpty() ? "" : " AND ")
                    .append("t.\"").append(key.name()).append("\" IS NOT DISTINCT FROM ")
                    .append(castExprRaw("s.\"" + key.name() + '"', key.duckType()));
        }
        try (Statement s = conn.createStatement()) {
            s.execute("INSERT INTO " + lake + " (" + cols + ") SELECT " + proj + " FROM " + stg
                    + " s WHERE NOT EXISTS (SELECT 1 FROM " + lake + " t WHERE " + newKeyMatch + ")");
        }
    }

    private static List<Integer> keyIndexes(RelInfo rel) {
        List<Integer> indexes = new ArrayList<>();
        for (int i = 0; i < rel.cols().size(); i++) {
            if (rel.cols().get(i).isKey()) indexes.add(i);
        }
        return indexes;
    }

    /** 避开用户源列名，生成稳定的 staging 内部旧 key 列名。 */
    private static String oldKeyStageColumn(RelInfo rel, int ordinal) {
        Set<String> names = new HashSet<>();
        for (ColDef col : rel.cols()) names.add(col.name());
        String name = "__ducklake_old_key_" + ordinal;
        while (names.contains(name)) name = '_' + name;
        return name;
    }

    private TableSqlParts buildSqlParts(RelInfo rel) {
        List<ColDef> defs = rel.cols();
        // stageCols: "\"c1\" VARCHAR, \"c2\" VARCHAR, ..., \"__deleted\" VARCHAR, \"__seq\" BIGINT"
        StringBuilder sc = new StringBuilder();
        for (ColDef c : defs) {
            if (!sc.isEmpty()) sc.append(", ");
            sc.append('"').append(c.name()).append("\" VARCHAR");
        }
        sc.append(", \"__deleted\" VARCHAR, \"__seq\" BIGINT");

        // cols: "\"c1\", \"c2\", ..."
        StringBuilder cols = new StringBuilder();
        // proj: CAST expressions for each column
        StringBuilder proj = new StringBuilder();
        for (ColDef c : defs) {
            if (!cols.isEmpty()) { cols.append(", "); proj.append(", "); }
            cols.append('"').append(c.name()).append('"');
            proj.append(castExprRaw('"' + c.name() + '"', c.duckType()));
        }

        // keyMatch / partition（无主键时均为空串）
        List<String> keyCols = rel.keyColumns();
        StringBuilder keyMatch = new StringBuilder();
        StringBuilder partition = new StringBuilder();
        for (String k : keyCols) {
            String duckType = rel.cols().stream().filter(c -> c.name().equals(k))
                    .findFirst().map(ColDef::duckType).orElse("VARCHAR");
            if (!keyMatch.isEmpty()) { keyMatch.append(" AND "); partition.append(", "); }
            // DELETE 语句里的 keyMatch 用到 stg 的别名，运行时替换 %%STG%% 不需要
            // 实际 DELETE FROM lake t WHERE EXISTS (SELECT 1 FROM stg s WHERE t.k = CAST(s.k AS type))
            keyMatch.append("t.\"").append(k).append("\" = ")
                    .append(castExprRaw("s.\"" + k + '"', duckType));
            partition.append('"').append(k).append('"');
        }
        return new TableSqlParts(sc.toString(), cols.toString(), proj.toString(),
                keyMatch.toString(), partition.toString());
    }
    // ──────────── ensureTable（Relation 消息自驱 DDL 演化） ────────────

    private void ensureTable(Connection conn, String lakeTable, RelInfo rel) throws SQLException {
        Map<String, String> cols = knownColumns.get(lakeTable);
        if (cols == null) {
            // 先查湖表是否已存在（cache 被清后重入时表已在，不重建也不重设 SORTED BY；
            // 对已有数据的表再次 ALTER SET SORTED BY 会被 DuckLake 拒绝并 abort 整个事务）
            cols = loadLakeCols(conn, lakeTable);
            if (cols.isEmpty()) {
                // 真正首次建表
                String lakeSchema = lakeTable.substring(0, lakeTable.indexOf('.'));
                StringBuilder ddl = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                        .append(DuckLakeEngine.LAKE).append('.').append(DuckLakeEngine.quoted(lakeTable))
                        .append(" (");
                List<ColDef> defs = rel.cols();
                for (int i = 0; i < defs.size(); i++) {
                    ddl.append(i > 0 ? ", " : "")
                            .append('"').append(defs.get(i).name()).append("\" ")
                            .append(defs.get(i).duckType());
                }
                ddl.append(')');
                try (Statement s = conn.createStatement()) {
                    s.execute("CREATE SCHEMA IF NOT EXISTS " + DuckLakeEngine.LAKE + ".\"" + lakeSchema + '"');
                    s.execute(ddl.toString());
                }
                ddlApplier.applySortedByPk(conn, lakeTable, rel.keyColumns());
                cols = loadLakeCols(conn, lakeTable);
            }
            knownColumns.put(lakeTable, cols);
            log.info("RawPg 湖表就绪: {} ({} 列)", lakeTable, cols.size());
        }
        // Relation 消息感知到新列/新类型：在当批新值写入前完成湖表演化。
        for (ColDef col : rel.cols()) {
            String have = cols.get(col.name());
            if (have == null) {
                try (Statement s = conn.createStatement()) {
                    s.execute("ALTER TABLE " + DuckLakeEngine.LAKE + "." + DuckLakeEngine.quoted(lakeTable)
                            + " ADD COLUMN IF NOT EXISTS \"" + col.name() + "\" " + col.duckType());
                }
                cols.put(col.name(), col.duckType());
                log.warn("RawPg 加列（Relation 自愈）: {}.{} ({})", lakeTable, col.name(), col.duckType());
            } else if (!have.equals(col.duckType())) {
                followTypeChange(conn, lakeTable, rel, col, have, cols);
            }
        }
    }

    /**
     * Relation 类型与湖表类型不一致时，优先使用 DuckLake 原生 ALTER；不支持的转换再用
     * CAST 临时表原子换表。该方法只从 writeBatch 的事务外阶段调用。
     */
    private void followTypeChange(Connection conn, String lakeTable, RelInfo rel, ColDef col,
                                  String have, Map<String, String> cols) throws SQLException {
        String driftKey = lakeTable + "." + col.name();
        if (!props.getMaintenance().isFollowTypeChange()) {
            if (typeDriftWarned.add(driftKey)) {
                log.warn("RawPg 类型漂移未跟随(followTypeChange=false): {} {} -> {}",
                        driftKey, have, col.duckType());
            }
            return;
        }

        SQLException alterFailure;
        try (Statement s = conn.createStatement()) {
            s.execute("ALTER TABLE " + DuckLakeEngine.LAKE + "." + DuckLakeEngine.quoted(lakeTable)
                    + " ALTER COLUMN \"" + col.name() + "\" SET DATA TYPE " + col.duckType());
            cols.put(col.name(), col.duckType());
            invalidateSql(lakeTable);
            typeDriftWarned.remove(driftKey);
            syncState.getDdlApplied().increment();
            log.warn("RawPg 类型跟随(ALTER): {} {} -> {}", driftKey, have, col.duckType());
            return;
        } catch (SQLException e) {
            alterFailure = e;
            log.info("RawPg 原生 ALTER 类型失败，尝试 CAST 换表: {} {} -> {} ({})",
                    driftKey, have, col.duckType(), e.getMessage());
        }

        try {
            rewriteTableWithCasts(conn, lakeTable, rel, cols);
            invalidateSql(lakeTable);
            typeDriftWarned.remove(driftKey);
        } catch (SQLException rewriteFailure) {
            rewriteFailure.addSuppressed(alterFailure);
            throw new SQLException("RawPg 类型迁移失败: " + driftKey + " " + have + " -> "
                    + col.duckType(), rewriteFailure);
        }
    }

    /** 使用 Relation 的当前目标类型重写湖表；显式事务保证 DROP/RENAME 对读者原子可见。 */
    private void rewriteTableWithCasts(Connection conn, String lakeTable, RelInfo rel,
                                       Map<String, String> cols) throws SQLException {
        if (!conn.getAutoCommit()) {
            throw new SQLException("RawPg CAST 换表必须在湖数据事务外执行: " + lakeTable);
        }
        Map<String, String> wanted = new LinkedHashMap<>();
        for (ColDef col : rel.cols()) wanted.put(col.name(), col.duckType());

        StringBuilder projection = new StringBuilder();
        for (Map.Entry<String, String> current : cols.entrySet()) {
            String target = wanted.get(current.getKey());
            projection.append(projection.isEmpty() ? "" : ", ");
            if (target != null && !target.equals(current.getValue())) {
                projection.append(typeMigrationCastExpr("\"" + current.getKey() + '"',
                                current.getValue(), target))
                        .append(" AS \"").append(current.getKey()).append('"');
            } else {
                projection.append('"').append(current.getKey()).append('"');
            }
        }

        String schemaName = lakeTable.substring(0, lakeTable.indexOf('.'));
        String tableName = lakeTable.substring(lakeTable.indexOf('.') + 1);
        String tmpName = tableName + "__typemig";
        String qTmp = DuckLakeEngine.LAKE + ".\"" + schemaName + "\".\"" + tmpName + "\"";
        String qTable = DuckLakeEngine.LAKE + "." + DuckLakeEngine.quoted(lakeTable);

        conn.setAutoCommit(false);
        try {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP TABLE IF EXISTS " + qTmp);
                s.execute("CREATE TABLE " + qTmp + " AS SELECT " + projection + " FROM " + qTable + " LIMIT 0");
            }
            ddlApplier.applySortedByPk(conn, schemaName + "." + tmpName, rel.keyColumns());
            try (Statement s = conn.createStatement()) {
                s.execute("INSERT INTO " + qTmp + " SELECT " + projection + " FROM " + qTable);
                s.execute("DROP TABLE " + qTable);
                s.execute("ALTER TABLE " + qTmp + " RENAME TO \"" + tableName + '"');
            }
            conn.commit();
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        } finally {
            conn.setAutoCommit(true);
        }

        Map<String, String> fresh = loadLakeCols(conn, lakeTable);
        cols.clear();
        cols.putAll(fresh);
        syncState.getDdlApplied().increment();
        log.warn("RawPg 类型跟随(CAST 换表): {} -> {}", lakeTable, fresh);
    }

    /** 旧版本 PG 数组曾以 VARCHAR 保存花括号文本，迁移到 LIST 前先归一化为 DuckDB 列表文本。 */
    private static String typeMigrationCastExpr(String column, String currentType, String targetType) {
        if ("VARCHAR".equals(currentType) && targetType.endsWith("[]")) {
            return "CAST(replace(replace(" + column + ", '{', '['), '}', ']') AS " + targetType + ')';
        }
        return "CAST(" + column + " AS " + targetType + ')';
    }

    private void invalidateSql(String lakeTable) {
        sqlCache.keySet().removeIf(rel -> lakeTableName(rel).equals(lakeTable));
    }

    private Map<String, String> loadLakeCols(Connection conn, String lakeTable) throws SQLException {
        String[] parts = lakeTable.split("\\.", 2);
        Map<String, String> cols = new LinkedHashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT column_name, data_type FROM information_schema.columns "
                        + "WHERE table_catalog=? AND table_schema=? AND table_name=? ORDER BY ordinal_position")) {
            ps.setString(1, DuckLakeEngine.LAKE);
            ps.setString(2, parts[0]);
            ps.setString(3, parts[1]);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    cols.put(rs.getString(1),
                            org.dpdns.zerodep.ducklake.sink.DuckType.normalize(rs.getString(2)));
                }
            }
        }
        return cols;
    }
    // ──────────── pgjdbc 连接 ────────────

    private Connection openReplicationConn() throws SQLException {
        Properties p = new Properties();
        p.setProperty("user", src.getUser());
        p.setProperty("password", src.getPassword());
        p.setProperty("replication", "database");
        p.setProperty("preferQueryMode", "simple");
        p.setProperty("assumeMinServerVersion", "10");
        String url = "jdbc:postgresql://%s:%d/%s"
                .formatted(src.getHostname(), src.getPort(), src.getDbname());
        return DriverManager.getConnection(url, p);
    }

    private PGReplicationStream openStream(Connection conn, long startLsn) throws SQLException {
        PGConnection pg = conn.unwrap(PGConnection.class);
        ensureSlotExists(pg, startLsn);
        var builder = pg.getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName(src.getSlotName())
                .withSlotOption("proto_version", "1")
                .withSlotOption("publication_names", src.getPublicationName())
                .withStatusInterval(10, java.util.concurrent.TimeUnit.SECONDS);
        if (startLsn > 0) {
            builder = builder.withStartPosition(LogSequenceNumber.valueOf(startLsn));
        }
        return builder.start();
    }

    /**
     * 确保 replication slot 存在：用独立普通连接查询 pg_replication_slots，
     * 不存在则在 replication 连接上创建。
     * 生产环境 initdb 脚本会预建槽；此处是自愈兜底（槽被意外删除、测试环境首次启动）。
     * 新建槽后 startLsn 无意义（槽从当前 WAL 位置开始），历史数据由 ScannerBootstrap 补灌。
     * <p>
     * ⚠️ slot 存在性检查刻意用普通连接而非 replication 连接：replication 连接虽在 simple query
     * 协议下也能跑 SELECT，但 PG 对 replication 连接的普通 SQL 支持未在文档中明确保证，
     * 且无法使用扩展查询协议（PreparedStatement）—— 独立连接更安全。
     */
    private void ensureSlotExists(PGConnection pg, long startLsn) throws SQLException {
        boolean exists;
        String url = "jdbc:postgresql://%s:%d/%s".formatted(src.getHostname(), src.getPort(), src.getDbname());
        try (java.sql.Connection plain = DriverManager.getConnection(url, src.getUser(), src.getPassword());
             java.sql.PreparedStatement ps = plain.prepareStatement(
                     "SELECT 1 FROM pg_replication_slots WHERE slot_name = ?")) {
            ps.setString(1, src.getSlotName());
            try (java.sql.ResultSet rs = ps.executeQuery()) {
                exists = rs.next();
            }
        }
        if (exists) return;
        if (startLsn != 0) {
            throw new SQLException("replication slot '" + src.getSlotName()
                    + "' 已丢失，但 catalog 仍有非零 offset 0x" + Long.toHexString(startLsn)
                    + "；自动新建会静默跳过 WAL，拒绝继续。请执行受控全量重建并清理对应 offset 后重启");
        }
        log.info("RawPg replication slot '{}' 不存在，自动创建（pgoutput）", src.getSlotName());
        pg.getReplicationAPI()
                .createReplicationSlot()
                .logical()
                .withSlotName(src.getSlotName())
                .withOutputPlugin("pgoutput")
                .make();
    }

    // ──────────── pgoutput Tuple 解析 ────────────

    /** 读取 TupleData，显式保留 {@code u}（unchanged TOAST），不能与 SQL NULL 混为一谈。 */
    private TupleData readTuple(ByteBuffer msg, RelInfo rel) {
        if (rel == null) { skipTuple(msg); return null; }
        int colCount = msg.getShort() & 0xFFFF;
        String[] vals = new String[rel.cols().size()];
        boolean[] unchanged = null;
        for (int i = 0; i < colCount; i++) {
            byte kind = msg.get();
            if (kind == 'n') { /* null */ }
            else if (kind == 'u') {
                if (i < vals.length) {
                    if (unchanged == null) unchanged = new boolean[vals.length];
                    unchanged[i] = true;
                }
            }
            else if (kind == 't') {
                int len = msg.getInt();
                if (i < vals.length) vals[i] = readUtf8(msg, len);
                else msg.position(msg.position() + len);
            }
        }
        return new TupleData(vals, unchanged);
    }

    private void skipTuple(ByteBuffer msg) {
        int colCount = msg.getShort() & 0xFFFF;
        for (int i = 0; i < colCount; i++) {
            byte kind = msg.get();
            if (kind == 't') {
                int len = msg.getInt();
                msg.position(msg.position() + len);
            }
        }
    }

    /** 读 null 结尾的 C 字符串 */
    private static String readCStr(ByteBuffer msg) {
        int start = msg.position();
        while (msg.get() != 0) { /* skip */ }
        int end = msg.position() - 1;
        msg.position(start);
        String value = readUtf8(msg, end - start);
        msg.get(); // consume null terminator
        return value;
    }

    /** Heap ByteBuffer 直接解码，避免为每个 pgoutput 文本列再分配一份临时 byte[]。 */
    private static String readUtf8(ByteBuffer msg, int length) {
        int start = msg.position();
        String value;
        if (msg.hasArray()) {
            value = new String(msg.array(), msg.arrayOffset() + start, length, StandardCharsets.UTF_8);
            msg.position(start + length);
        } else {
            byte[] bytes = new byte[length];
            msg.get(bytes);
            value = new String(bytes, StandardCharsets.UTF_8);
        }
        return value;
    }
    // ──────────── 工具方法 ────────────

    /**
     * pgoutput 文本值的 CAST 表达式，特别处理 bytea 的 \x 前缀。
     * 参数 col 已含引号（如 "\"col\"" 或 "s.\"col\""）。
     */
    private static String castExprRaw(String col, String duckType) {
        if ("VARCHAR".equals(duckType)) return col;
        if ("BLOB".equals(duckType)) {
            // pgoutput bytea = \x<hex>；跳过前缀再 unhex
            return "unhex(CASE WHEN " + col + " LIKE '\\x%' THEN substring(" + col + ", 3) ELSE " + col + " END)";
        }
        // PG 数组文本格式 {a,b} → DuckDB 需要 [a,b]；花括号替换后 TRY_CAST 可直接解析
        if (duckType.endsWith("[]")) return "TRY_CAST(replace(replace(" + col + ", '{', '['), '}', ']') AS " + duckType + ")";
        return switch (duckType) {
            case "DATE","TIME","TIMETZ","TIMESTAMP","TIMESTAMPTZ","INTERVAL","VARIANT" ->
                    "TRY_CAST(" + col + " AS " + duckType + ")";
            default -> "CAST(" + col + " AS " + duckType + ")";
        };
    }

    private String lakeTableName(RelInfo rel) {
        return props.getMaintenance().getSchemaPrefix() + rel.schema() + "." + rel.table();
    }

    private Map<String, String> toRowMap(RelInfo rel, PendingRow row) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i < rel.cols().size(); i++) {
            m.put(rel.cols().get(i).name(), i < row.values().length ? row.values()[i] : null);
        }
        return m;
    }

    private void dropStagings(Connection conn, int count) {
        try (Statement s = conn.createStatement()) {
            for (int i = 0; i < count; i++) {
                s.execute("DROP TABLE IF EXISTS " + DuckLakeEngine.MEM + ".main.stg_raw_" + i);
            }
        } catch (SQLException e) {
            log.debug("stg_raw 清理失败（下批 CREATE OR REPLACE 覆盖）: {}", e.getMessage());
        }
    }

    private void recordBatchTiming(WriteTiming timing, long lockWaitNanos, long offsetNanos) {
        recordStage(SyncState.Stage.DECODE, decodeNanos);
        recordStage(SyncState.Stage.PLAN, timing.planNanos());
        recordStage(SyncState.Stage.LOCK_WAIT, lockWaitNanos);
        recordStage(SyncState.Stage.ENSURE, timing.ensureNanos());
        recordStage(SyncState.Stage.STAGING_DDL, timing.stagingDdlNanos());
        recordStage(SyncState.Stage.APPENDER, timing.appenderNanos());
        recordStage(SyncState.Stage.MIRROR_DML, timing.mirrorDmlNanos());
        recordStage(SyncState.Stage.LAKE_COMMIT, timing.lakeCommitNanos());
        recordStage(SyncState.Stage.CLEANUP, timing.cleanupNanos());
        recordStage(SyncState.Stage.OFFSET_ACK, offsetNanos);
    }

    private void recordStage(SyncState.Stage stage, long nanos) {
        syncState.recordStage(SyncState.Reader.POSTGRES, stage, nanos);
    }

    static long pgTimestampMs(long pgMicros) {
        return PG_EPOCH_MILLIS + Math.floorDiv(pgMicros, 1_000L);
    }

    private static long deliverLag(long deliveredAtMs, long sourceTsMs) {
        return sourceTsMs <= 0 ? 0 : Math.max(0, deliveredAtMs - sourceTsMs);
    }
}
