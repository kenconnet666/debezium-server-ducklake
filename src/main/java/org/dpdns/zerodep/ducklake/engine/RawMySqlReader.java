package org.dpdns.zerodep.ducklake.engine;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventMetadata;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.event.XidEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.json.JsonBinary;
import lombok.extern.slf4j.Slf4j;
import org.dpdns.zerodep.ducklake.config.DucklakeProperties;
import org.dpdns.zerodep.ducklake.ddl.DdlApplier;
import org.dpdns.zerodep.ducklake.metrics.SyncState;
import org.dpdns.zerodep.ducklake.sink.DuckLakeEngine;
import org.dpdns.zerodep.ducklake.sink.DuckType;
import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBConnection;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

/**
 * MySQL ROW binlog 原生直通 reader。TableMap(FULL metadata) 提供列名/主键/物理类型，行值直接写
 * 全 VARCHAR staging，再在单个 DuckLake 事务内做镜像 upsert；不经过 Kafka Connect Struct、SMT
 * 或 ChangeEventQueue。offset 只在源事务完整落湖后推进，崩溃时按主键幂等重放。
 */
@Slf4j
class RawMySqlReader {

    private record PhysicalCol(String name, ColumnType mysqlType, int metadata, boolean key,
                               boolean unsigned, List<String> enumValues, List<String> setValues) {
    }

    private record TableShape(String database, String table, List<PhysicalCol> cols) {
    }

    private record ColDef(String name, ColumnType mysqlType, String duckType, boolean key,
                          boolean unsigned, List<String> enumValues, List<String> setValues) {
    }

    private record TableInfo(String database, String table, String lakeTable, List<ColDef> cols) {
        List<String> keyColumns() {
            return cols.stream().filter(ColDef::key).map(ColDef::name).toList();
        }
    }

    private record CachedTable(TableShape shape, TableInfo table) {
    }

    private record PendingRow(TableInfo table, boolean deleted, String[] values) {
    }

    private record TableSqlParts(String stageCols, String cols, String projection,
                                 String keyMatch, String partition) {
    }

    private record StagingTiming(long ddlNanos, long appenderNanos) {
    }

    private record WriteTiming(long stageMs, long lakeTxMs, long planNanos, long ensureNanos,
                               long stagingDdlNanos, long appenderNanos, long mirrorDmlNanos,
                               long lakeCommitNanos, long cleanupNanos) {
    }

    private final DucklakeProperties props;
    private final DuckLakeEngine engine;
    private final DdlApplier ddlApplier;
    private final SyncState syncState;
    private final RawMySqlOffset offset;
    private final String sourceName;
    private final DucklakeProperties.Source src;
    private final DucklakeProperties.Engine eng;
    private final List<Pattern> includes;
    private final List<Pattern> excludes;
    private final Set<String> infrastructureTables;

    /** table-id 只在当前 binlog 文件内绑定；结构缓存按源表保留并由 DDL/shape 变化失效。 */
    private final Map<Long, TableInfo> tables = new HashMap<>();
    private final Map<String, CachedTable> tableDefinitions = new HashMap<>();
    /** 当前源事务中的行；只有 XID/COMMIT 到达后才移入 committed。 */
    private List<PendingRow> transactionRows = new ArrayList<>();
    /** 已提交但为攒批尚未落湖的行。 */
    private List<PendingRow> committed = new ArrayList<>();
    private final Map<String, Map<String, String>> knownColumns = new HashMap<>();
    private final Map<TableInfo, TableSqlParts> sqlCache = new HashMap<>();
    private final Set<String> noKeyWarned = new HashSet<>();
    private final AtomicReference<Throwable> fatal = new AtomicReference<>();

    private volatile BinaryLogClient client;
    private volatile boolean stopped;
    private String currentFilename;
    private RawMySqlOffset.Position committedPosition;
    private long committedMaxSourceTsMs;
    private long lastFlushMs = System.currentTimeMillis();
    private long decodeNanos;

    RawMySqlReader(DucklakeProperties props, DuckLakeEngine engine, DdlApplier ddlApplier,
                   SyncState syncState, RawMySqlOffset offset, String sourceName) {
        this.props = props;
        this.engine = engine;
        this.ddlApplier = ddlApplier;
        this.syncState = syncState;
        this.offset = offset;
        this.sourceName = sourceName;
        this.src = props.getSource();
        this.eng = props.getEngine();
        this.includes = compileList(src.getSchemaIncludeList());
        this.excludes = compileList(src.getTableExcludeList());
        this.infrastructureTables = new HashSet<>();
        // 兼容旧部署遗留表；原生链路本身不再创建或使用 signal/heartbeat。
        infrastructureTables.add((src.getDbname() + ".dbz_signal").toLowerCase(Locale.ROOT));
        infrastructureTables.addAll(props.getMaintenance().getDdlAuditTables().stream()
                .map(table -> table.toLowerCase(Locale.ROOT)).toList());
        infrastructureTables.add("dbz_heartbeat");
    }

    /** 校验 raw 契约；首次接入捕获并先持久化 HEAD，返回实际启动位点。 */
    RawMySqlOffset.Position prepare(RawMySqlOffset.Position saved) {
        SourceStatus status = sourceStatus();
        if (saved != null) {
            return saved;
        }
        RawMySqlOffset.Position start = new RawMySqlOffset.Position(
                status.filename(), status.position(), status.gtidSet());
        offset.initialize(sourceName, start);
        log.info("RawMySQL 首次位点已固化: {}:{} gtid={}", start.filename(), start.position(),
                start.gtidSet().isBlank() ? "disabled" : "enabled");
        return start;
    }

    void run(RawMySqlOffset.Position start) throws Exception {
        currentFilename = start.filename();
        BinaryLogClient c = new BinaryLogClient(
                src.getHostname(), src.getPort(), src.getDbname(), src.getUser(), src.getPassword());
        client = c;
        c.setServerId(src.getServerId());
        c.setBinlogFilename(start.filename());
        c.setBinlogPosition(start.position());
        if (!start.gtidSet().isBlank()) {
            c.setGtidSet(start.gtidSet());
            c.setUseBinlogFilenamePositionInGtidMode(false);
        }
        c.setKeepAlive(false); // 断链交给容器重启，不在库内部静默跳位重连
        c.setHeartbeatInterval(Math.max(500L, eng.getPollIntervalMs() * 50L));
        c.setConnectTimeout(15_000L);

        EventDeserializer deserializer = new EventDeserializer();
        deserializer.setCompatibilityMode(
                EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG_MICRO,
                EventDeserializer.CompatibilityMode.INVALID_DATE_AND_TIME_AS_MIN_VALUE,
                EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY);
        c.setEventDeserializer(deserializer);
        c.registerEventListener(this::onEvent);
        c.registerLifecycleListener(new BinaryLogClient.AbstractLifecycleListener() {
            @Override
            public void onCommunicationFailure(BinaryLogClient ignored, Exception ex) {
                if (!stopped) fatal.compareAndSet(null, ex);
            }

            @Override
            public void onEventDeserializationFailure(BinaryLogClient ignored, Exception ex) {
                fail(ex);
            }
        });

        try {
            c.connect();
            Throwable failure = fatal.get();
            if (failure != null) throw new IllegalStateException("RAW_MYSQL binlog 读取失败", failure);
            if (!stopped) throw new IllegalStateException("BinaryLogClient 非预期断开");
            flushCommitted(true);
        } finally {
            client = null;
        }
    }

    void stop() {
        stopped = true;
        BinaryLogClient c = client;
        if (c != null && c.isConnected()) {
            try {
                c.disconnect();
            } catch (IOException e) {
                log.debug("RawMySQL disconnect: {}", e.getMessage());
            }
        }
    }

    boolean isStopped() {
        return stopped;
    }

    private void onEvent(Event event) {
        if (stopped || fatal.get() != null) return;
        try {
            handleEvent(event);
        } catch (Throwable e) {
            fail(e);
        }
    }

    private void fail(Throwable e) {
        if (!fatal.compareAndSet(null, e)) return;
        log.error("RawMySQL 事件处理失败，将断链并从已提交 offset 重放: {}", e.getMessage(), e);
        BinaryLogClient c = client;
        if (c != null && c.isConnected()) {
            try {
                c.disconnect();
            } catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
        }
    }

    private void handleEvent(Event event) throws Exception {
        EventData data = event.getData();
        if (data instanceof RotateEventData rotate) {
            flushCommitted(true);
            if (!transactionRows.isEmpty()) {
                throw new IllegalStateException("binlog rotate 出现在未提交源事务中");
            }
            // MySQL table-id 的作用域不跨 binlog 文件；保留按源表的结构缓存即可。
            tables.clear();
            currentFilename = rotate.getBinlogFilename();
            RawMySqlOffset.Position p = new RawMySqlOffset.Position(
                    currentFilename, rotate.getBinlogPosition(), currentGtidSet());
            saveStandaloneOffset(p);
            return;
        }
        if (data instanceof TableMapEventData map) {
            long started = System.nanoTime();
            parseTableMap(map);
            decodeNanos += System.nanoTime() - started;
        } else if (data instanceof WriteRowsEventData rows) {
            long started = System.nanoTime();
            parseWrite(rows);
            decodeNanos += System.nanoTime() - started;
        } else if (data instanceof UpdateRowsEventData rows) {
            long started = System.nanoTime();
            parseUpdate(rows);
            decodeNanos += System.nanoTime() - started;
        } else if (data instanceof DeleteRowsEventData rows) {
            long started = System.nanoTime();
            parseDelete(rows);
            decodeNanos += System.nanoTime() - started;
        } else if (data instanceof XidEventData) {
            commitTransaction(positionOf(event), event.getHeader().getTimestamp());
        } else if (data instanceof QueryEventData query) {
            handleQuery(event, query);
        }
        maybeFlush();
    }

    private void handleQuery(Event event, QueryEventData query) throws Exception {
        String sql = query.getSql() == null ? "" : query.getSql().strip();
        String normalized = sql.replaceFirst(";\\s*$", "").strip().toUpperCase(Locale.ROOT);
        if ("BEGIN".equals(normalized)) {
            if (!transactionRows.isEmpty()) {
                throw new IllegalStateException("BEGIN 前仍有未完成行事件");
            }
            return;
        }
        if ("ROLLBACK".equals(normalized)) {
            transactionRows.clear();
            return;
        }
        if ("COMMIT".equals(normalized)) {
            commitTransaction(positionOf(event), event.getHeader().getTimestamp());
            return;
        }

        // ROW 模式下其余 QueryEvent 是自动提交 DDL/管理语句。DDL 前先刷完已提交 DML，
        // 保证 rename/drop/truncate 与前后数据严格按 binlog 顺序应用。
        if (!transactionRows.isEmpty()) {
            throw new IllegalStateException("自动提交 QueryEvent 前存在未完成行事务: " + sql);
        }
        flushCommitted(true);
        boolean appliedDdl = isTableDdl(normalized) && databaseIncluded(query.getDatabase());
        if (appliedDdl) {
            log.debug("RawMySQL DDL: db={} sql={}", query.getDatabase(), sql);
            applyDdl(query.getDatabase(), sql, event.getHeader().getTimestamp());
        }
        RawMySqlOffset.Position p = positionOf(event);
        if (p != null) {
            saveStandaloneOffset(p);
            if (!appliedDdl) {
                syncState.sourceReflectedThrough(event.getHeader().getTimestamp());
            }
        }
    }

    private void commitTransaction(RawMySqlOffset.Position position, long sourceTsMs) throws Exception {
        if (!transactionRows.isEmpty()) {
            if (committed.isEmpty()) {
                List<PendingRow> reusable = committed;
                committed = transactionRows;
                transactionRows = reusable;
            } else {
                committed.addAll(transactionRows);
                transactionRows.clear();
            }
        }
        if (position != null) committedPosition = position;
        committedMaxSourceTsMs = Math.max(committedMaxSourceTsMs, sourceTsMs);
        if (committed.size() >= eng.getMaxBatchSize()) flushCommitted(true);
    }

    // ───────────────────────── TableMap / row decode ─────────────────────────

    private void parseTableMap(TableMapEventData map) {
        String full = map.getDatabase() + "." + map.getTable();
        if (!captured(map.getDatabase(), map.getTable())) {
            tables.remove(map.getTableId());
            return;
        }
        TableMapEventMetadata metadata = map.getEventMetadata();
        List<String> names = metadata == null ? null : metadata.getColumnNames();
        if (names == null || names.size() != map.getColumnTypes().length) {
            throw new IllegalStateException("TableMap 缺少完整列名: " + full
                    + "；源库必须设置 binlog_row_metadata=FULL");
        }
        Set<Integer> keyIndexes = new HashSet<>();
        if (metadata.getSimplePrimaryKeys() != null) keyIndexes.addAll(metadata.getSimplePrimaryKeys());
        if (metadata.getPrimaryKeysWithPrefix() != null) keyIndexes.addAll(metadata.getPrimaryKeysWithPrefix().keySet());

        BitSet signedness = metadata.getSignedness();
        List<String[]> enumMetadata = metadata.getEnumStrValues();
        List<String[]> setMetadata = metadata.getSetStrValues();
        int enumIndex = 0;
        int setIndex = 0;
        List<PhysicalCol> physicalCols = new ArrayList<>(names.size());
        for (int i = 0; i < names.size(); i++) {
            ColumnType type = effectiveType(map.getColumnTypes()[i], map.getColumnMetadata()[i]);
            boolean unsigned = signedness != null && signedness.get(i);
            List<String> enumValues = type == ColumnType.ENUM && enumMetadata != null
                    && enumIndex < enumMetadata.size() ? immutableValues(enumMetadata.get(enumIndex++)) : null;
            List<String> setValues = type == ColumnType.SET && setMetadata != null
                    && setIndex < setMetadata.size() ? immutableValues(setMetadata.get(setIndex++)) : null;
            physicalCols.add(new PhysicalCol(names.get(i), type, map.getColumnMetadata()[i],
                    keyIndexes.contains(i), unsigned, enumValues, setValues));
        }

        TableShape shape = new TableShape(map.getDatabase(), map.getTable(), List.copyOf(physicalCols));
        CachedTable cached = tableDefinitions.get(full);
        TableInfo info;
        if (cached != null && cached.shape().equals(shape)) {
            info = cached.table();
        } else {
            Map<String, String> liveTypes = ddlApplier.mysqlColumnTypes(map.getDatabase(), map.getTable());
            boolean liveMatches = new ArrayList<>(liveTypes.keySet()).equals(names);
            List<ColDef> cols = new ArrayList<>(physicalCols.size());
            for (PhysicalCol col : physicalCols) {
                String rawType = rawDuckType(col.mysqlType(), col.metadata(), col.unsigned());
                String duckType = liveMatches ? liveTypes.getOrDefault(col.name(), rawType) : rawType;
                cols.add(new ColDef(col.name(), col.mysqlType(), DuckType.normalize(duckType),
                        col.key(), col.unsigned(), col.enumValues(), col.setValues()));
            }
            info = new TableInfo(map.getDatabase(), map.getTable(),
                    lakeTableName(map.getDatabase(), map.getTable()), List.copyOf(cols));
            CachedTable replaced = tableDefinitions.put(full, new CachedTable(shape, info));
            if (replaced != null) sqlCache.remove(replaced.table());
        }

        TableInfo old = tables.put(map.getTableId(), info);
        if (!info.equals(old)) {
            sqlCache.remove(old);
            log.info("RawMySQL 感知表: {} ({}列, key={})", full, info.cols().size(), info.keyColumns());
        }
    }

    private static List<String> immutableValues(String[] values) {
        return values == null ? null : List.of(values.clone());
    }

    private void parseWrite(WriteRowsEventData rows) throws IOException {
        TableInfo table = tables.get(rows.getTableId());
        if (table == null) return;
        for (Serializable[] packed : rows.getRows()) {
            transactionRows.add(new PendingRow(table, false,
                    serializeRow(table, expand(packed, rows.getIncludedColumns(), table.cols().size()))));
        }
    }

    private void parseUpdate(UpdateRowsEventData rows) throws IOException {
        TableInfo table = tables.get(rows.getTableId());
        if (table == null) return;
        for (Map.Entry<Serializable[], Serializable[]> change : rows.getRows()) {
            String[] before = serializeRow(table,
                    expand(change.getKey(), rows.getIncludedColumnsBeforeUpdate(), table.cols().size()));
            String[] after = serializeRow(table,
                    expand(change.getValue(), rows.getIncludedColumns(), table.cols().size()));
            if (primaryKeyChanged(table, before, after)) {
                transactionRows.add(new PendingRow(table, true, before));
            }
            transactionRows.add(new PendingRow(table, false, after));
        }
    }

    private void parseDelete(DeleteRowsEventData rows) throws IOException {
        TableInfo table = tables.get(rows.getTableId());
        if (table == null) return;
        for (Serializable[] packed : rows.getRows()) {
            transactionRows.add(new PendingRow(table, true,
                    serializeRow(table, expand(packed, rows.getIncludedColumns(), table.cols().size()))));
        }
    }

    private static Serializable[] expand(Serializable[] packed, BitSet included, int columnCount) {
        if (packed.length == columnCount) return packed;
        Serializable[] full = new Serializable[columnCount];
        int source = 0;
        for (int i = 0; i < columnCount; i++) {
            if (included.get(i)) full[i] = packed[source++];
        }
        return full;
    }

    private String[] serializeRow(TableInfo table, Serializable[] values) throws IOException {
        String[] out = new String[table.cols().size()];
        for (int i = 0; i < out.length; i++) {
            out[i] = serialize(table.cols().get(i), values[i]);
        }
        return out;
    }

    private static boolean primaryKeyChanged(TableInfo table, String[] before, String[] after) {
        for (int i = 0; i < table.cols().size(); i++) {
            if (table.cols().get(i).key()) {
                if (!Objects.equals(before[i], after[i])) return true;
            }
        }
        return false;
    }

    private String serialize(ColDef col, Serializable value) throws IOException {
        if (value == null || value instanceof Long n && n == Long.MIN_VALUE
                && isTemporal(col.mysqlType())) return null;
        if (col.mysqlType() == ColumnType.JSON && value instanceof byte[] bytes) {
            return JsonBinary.parseAsString(bytes);
        }
        if (col.mysqlType() == ColumnType.ENUM && value instanceof Number n) {
            int index = n.intValue();
            return col.enumValues() != null && index > 0 && index <= col.enumValues().size()
                    ? col.enumValues().get(index - 1) : String.valueOf(index);
        }
        if (col.mysqlType() == ColumnType.SET && value instanceof Number n) {
            return decodeSet(n.longValue(), col.setValues());
        }
        if (value instanceof BitSet bits) {
            if ("BOOLEAN".equals(col.duckType())) return Boolean.toString(bits.get(0));
            return HexFormat.of().formatHex(bits.toByteArray());
        }
        if (value instanceof byte[] bytes) {
            return "BLOB".equals(col.duckType()) ? HexFormat.of().formatHex(bytes)
                    : new String(bytes, StandardCharsets.UTF_8);
        }
        if (value instanceof BigDecimal decimal) return decimal.toPlainString();
        if (value instanceof Number number) {
            if (isTemporal(col.mysqlType())) return temporalText(col.mysqlType(), number.longValue());
            return numericText(col, number);
        }
        return value.toString();
    }

    private static String numericText(ColDef col, Number value) {
        if (!col.unsigned()) return value.toString();
        return switch (col.mysqlType()) {
            case TINY -> Integer.toString(value.intValue() & 0xff);
            case SHORT -> Integer.toString(value.intValue() & 0xffff);
            case INT24 -> Integer.toString(value.intValue() & 0x00ff_ffff);
            case LONG -> Long.toString(Integer.toUnsignedLong(value.intValue()));
            case LONGLONG -> Long.toUnsignedString(value.longValue());
            default -> value.toString();
        };
    }

    private static String temporalText(ColumnType type, long micros) {
        long seconds = Math.floorDiv(micros, 1_000_000L);
        int nanos = (int) Math.floorMod(micros, 1_000_000L) * 1_000;
        Instant instant = Instant.ofEpochSecond(seconds, nanos);
        return switch (type) {
            case TIMESTAMP, TIMESTAMP_V2 -> instant.toString();
            case DATE -> instant.atOffset(ZoneOffset.UTC).toLocalDate().toString();
            case DATETIME, DATETIME_V2 -> DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(
                    LocalDateTime.ofInstant(instant, ZoneOffset.UTC));
            case TIME, TIME_V2 -> {
                boolean negative = micros < 0;
                long abs = Math.abs(micros);
                long hours = abs / 3_600_000_000L;
                long minutes = abs / 60_000_000L % 60;
                long secs = abs / 1_000_000L % 60;
                long fraction = abs % 1_000_000L;
                String base = "%s%02d:%02d:%02d".formatted(negative ? "-" : "", hours, minutes, secs);
                yield fraction == 0 ? base : base + "." + String.format(Locale.ROOT, "%06d", fraction)
                        .replaceFirst("0+$", "");
            }
            default -> Long.toString(micros);
        };
    }

    private static String decodeSet(long mask, List<String> values) {
        if (values == null) return Long.toUnsignedString(mask);
        List<String> selected = new ArrayList<>();
        for (int i = 0; i < values.size(); i++) {
            if ((mask & (1L << i)) != 0) selected.add(values.get(i));
        }
        return String.join(",", selected);
    }

    // ───────────────────────── batch → staging → lake ─────────────────────────

    private void maybeFlush() throws Exception {
        long timeout = Math.max(500L, eng.getPollIntervalMs() * 50L);
        if (System.currentTimeMillis() - lastFlushMs < timeout) return;
        flushCommitted(false);
    }

    private void flushCommitted(boolean force) throws Exception {
        long timeout = Math.max(500L, eng.getPollIntervalMs() * 50L);
        if (committed.isEmpty()) {
            // 只有被过滤表/空事务时也要推进持久化位点；否则长时间无目标表流量后重启，
            // 可能从已经过期的旧 binlog 开始。这里只保存最近完整 COMMIT，不越过源事务边界。
            if (committedPosition != null
                    && (force || System.currentTimeMillis() - lastFlushMs >= timeout)) {
                saveStandaloneOffset(committedPosition);
                recordStage(SyncState.Stage.DECODE, decodeNanos);
                decodeNanos = 0;
                syncState.sourceReflectedThrough(committedMaxSourceTsMs);
                committedMaxSourceTsMs = 0;
                committedPosition = null;
                lastFlushMs = System.currentTimeMillis();
            } else if (force) {
                lastFlushMs = System.currentTimeMillis();
            }
            return;
        }
        if (!force && committed.size() < eng.getMaxBatchSize()
                && System.currentTimeMillis() - lastFlushMs < timeout) return;

        List<PendingRow> batch = committed;
        int rowCount = batch.size();
        RawMySqlOffset.Position position = committedPosition;
        long maxSourceTsMs = committedMaxSourceTsMs;
        long t0 = System.currentTimeMillis();
        DuckLakeEngine.LockedResult<WriteTiming> locked;
        long offsetNanos = 0;
        try {
            locked = engine.withLockTimed(conn -> writeBatch(conn, batch));
            if (position != null) {
                long offsetStarted = System.nanoTime();
                offset.save(sourceName, position);
                offsetNanos = System.nanoTime() - offsetStarted;
            }
        } catch (Exception e) {
            syncState.batchFailed();
            throw e;
        }
        WriteTiming timing = locked.value();
        committed.clear();
        committedPosition = null;
        committedMaxSourceTsMs = 0;
        long elapsed = System.currentTimeMillis() - t0;
        lastFlushMs = System.currentTimeMillis();
        syncState.batchCommitted(rowCount, maxSourceTsMs, deliverLag(t0, maxSourceTsMs),
                timing.stageMs(), timing.lakeTxMs());
        recordBatchTiming(timing, locked.waitNanos(), offsetNanos);
        decodeNanos = 0;
        log.debug("RawMySQL 落湖 {} 行 {}ms offset={}:{}", rowCount, elapsed,
                position == null ? "?" : position.filename(), position == null ? 0 : position.position());
    }

    private WriteTiming writeBatch(Connection conn, List<PendingRow> batch) throws SQLException {
        long stageStarted = System.nanoTime();
        long planStarted = System.nanoTime();
        Map<String, List<PendingRow>> byTable = new LinkedHashMap<>();
        Map<String, TableInfo> tableInfo = new LinkedHashMap<>();
        for (PendingRow row : batch) {
            String key = row.table().lakeTable();
            byTable.computeIfAbsent(key, ignored -> new ArrayList<>()).add(row);
            tableInfo.putIfAbsent(key, row.table());
        }
        long planNanos = System.nanoTime() - planStarted;

        int index = 0;
        long stagingDdlNanos = 0;
        long appenderNanos = 0;
        for (Map.Entry<String, List<PendingRow>> entry : byTable.entrySet()) {
            StagingTiming timing = stageRows(conn, tableInfo.get(entry.getKey()), entry.getValue(), index++);
            stagingDdlNanos += timing.ddlNanos();
            appenderNanos += timing.appenderNanos();
        }
        long stageMs = (System.nanoTime() - stageStarted) / 1_000_000L;
        long lakeStarted = System.nanoTime();
        long lakeTxMs;
        long ensureNanos = 0;
        long mirrorDmlNanos = 0;
        long lakeCommitNanos;
        long cleanupNanos;
        conn.setAutoCommit(false);
        try {
            index = 0;
            for (Map.Entry<String, List<PendingRow>> entry : byTable.entrySet()) {
                TableInfo table = tableInfo.get(entry.getKey());
                long ensureStarted = System.nanoTime();
                ensureTable(conn, table.lakeTable(), table);
                ensureNanos += System.nanoTime() - ensureStarted;
                long mirrorStarted = System.nanoTime();
                applyStaging(conn, table, index++);
                mirrorDmlNanos += System.nanoTime() - mirrorStarted;
            }
            long commitStarted = System.nanoTime();
            conn.commit();
            lakeCommitNanos = System.nanoTime() - commitStarted;
            lakeTxMs = (System.nanoTime() - lakeStarted) / 1_000_000L;
        } catch (SQLException e) {
            conn.rollback();
            knownColumns.clear();
            sqlCache.clear();
            throw e;
        } finally {
            long cleanupStarted = System.nanoTime();
            conn.setAutoCommit(true);
            dropStagings(conn, byTable.size());
            cleanupNanos = System.nanoTime() - cleanupStarted;
        }
        return new WriteTiming(stageMs, lakeTxMs, planNanos, ensureNanos, stagingDdlNanos,
                appenderNanos, mirrorDmlNanos, lakeCommitNanos, cleanupNanos);
    }

    private StagingTiming stageRows(Connection conn, TableInfo table, List<PendingRow> rows, int index)
            throws SQLException {
        TableSqlParts parts = sqlCache.computeIfAbsent(table, this::buildSqlParts);
        String name = "stg_raw_mysql_" + index;
        long ddlStarted = System.nanoTime();
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE OR REPLACE TABLE " + DuckLakeEngine.MEM + ".main." + name
                    + " (" + parts.stageCols() + ")");
        }
        long ddlNanos = System.nanoTime() - ddlStarted;
        long appenderStarted = System.nanoTime();
        DuckDBConnection duck = conn.unwrap(DuckDBConnection.class);
        try (DuckDBAppender appender = duck.createAppender(DuckLakeEngine.MEM, "main", name)) {
            long seq = 0;
            for (PendingRow row : rows) {
                appender.beginRow();
                for (String value : row.values()) {
                    if (value == null) appender.appendNull(); else appender.append(value);
                }
                appender.append(Boolean.toString(row.deleted()));
                appender.append(seq++);
                appender.endRow();
            }
        }
        return new StagingTiming(ddlNanos, System.nanoTime() - appenderStarted);
    }

    private void applyStaging(Connection conn, TableInfo table, int index) throws SQLException {
        String lakeTable = table.lakeTable();
        TableSqlParts parts = sqlCache.computeIfAbsent(table, this::buildSqlParts);
        String staging = DuckLakeEngine.MEM + ".main.stg_raw_mysql_" + index;
        String lake = DuckLakeEngine.LAKE + "." + DuckLakeEngine.quoted(lakeTable);
        if (parts.keyMatch().isEmpty()) {
            if (noKeyWarned.add(lakeTable)) {
                log.warn("RAW_MYSQL 源表无主键，降级 insert-only（UPDATE/DELETE 无法镜像）: {}", lakeTable);
            }
            try (Statement s = conn.createStatement()) {
                s.execute("INSERT INTO " + lake + " (" + parts.cols() + ") SELECT " + parts.projection()
                        + " FROM " + staging + " WHERE COALESCE(\"__deleted\", 'false') <> 'true'");
            }
            return;
        }
        try (Statement s = conn.createStatement()) {
            s.execute("DELETE FROM " + lake + " t WHERE EXISTS (SELECT 1 FROM " + staging
                    + " s WHERE " + parts.keyMatch() + ")");
            s.execute("INSERT INTO " + lake + " (" + parts.cols() + ") SELECT " + parts.projection()
                    + " FROM (SELECT * FROM " + staging
                    + " QUALIFY row_number() OVER (PARTITION BY " + parts.partition()
                    + " ORDER BY \"__seq\" DESC) = 1) latest"
                    + " WHERE COALESCE(\"__deleted\", 'false') <> 'true'");
        }
    }

    private TableSqlParts buildSqlParts(TableInfo table) {
        StringBuilder stage = new StringBuilder();
        StringBuilder cols = new StringBuilder();
        StringBuilder projection = new StringBuilder();
        StringBuilder keyMatch = new StringBuilder();
        StringBuilder partition = new StringBuilder();
        for (ColDef col : table.cols()) {
            if (!stage.isEmpty()) {
                stage.append(", ");
                cols.append(", ");
                projection.append(", ");
            }
            stage.append('"').append(col.name()).append("\" VARCHAR");
            cols.append('"').append(col.name()).append('"');
            projection.append(castExpr('"' + col.name() + '"', col.duckType()));
            if (col.key()) {
                if (!keyMatch.isEmpty()) {
                    keyMatch.append(" AND ");
                    partition.append(", ");
                }
                keyMatch.append("t.\"").append(col.name()).append("\" = ")
                        .append(castExpr("s.\"" + col.name() + '"', col.duckType()));
                partition.append('"').append(col.name()).append('"');
            }
        }
        stage.append(", \"__deleted\" VARCHAR, \"__seq\" BIGINT");
        return new TableSqlParts(stage.toString(), cols.toString(), projection.toString(),
                keyMatch.toString(), partition.toString());
    }

    private void ensureTable(Connection conn, String lakeTable, TableInfo table) throws SQLException {
        Map<String, String> current = knownColumns.get(lakeTable);
        if (current == null) {
            current = loadLakeColumns(conn, lakeTable);
            if (current.isEmpty()) {
                String schema = lakeTable.substring(0, lakeTable.indexOf('.'));
                StringBuilder ddl = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                        .append(DuckLakeEngine.LAKE).append('.').append(DuckLakeEngine.quoted(lakeTable)).append(" (");
                for (int i = 0; i < table.cols().size(); i++) {
                    ColDef col = table.cols().get(i);
                    ddl.append(i == 0 ? "" : ", ").append('"').append(col.name()).append("\" ")
                            .append(col.duckType());
                }
                ddl.append(')');
                try (Statement s = conn.createStatement()) {
                    s.execute("CREATE SCHEMA IF NOT EXISTS " + DuckLakeEngine.LAKE + ".\"" + schema + '"');
                    s.execute(ddl.toString());
                }
                ddlApplier.applySortedByPk(conn, lakeTable, table.keyColumns());
                current = loadLakeColumns(conn, lakeTable);
            }
            knownColumns.put(lakeTable, current);
            log.info("RawMySQL 湖表就绪: {} ({}列)", lakeTable, current.size());
        }
        for (ColDef col : table.cols()) {
            String have = current.get(col.name());
            if (have == null) {
                try (Statement s = conn.createStatement()) {
                    s.execute("ALTER TABLE " + DuckLakeEngine.LAKE + "." + DuckLakeEngine.quoted(lakeTable)
                            + " ADD COLUMN IF NOT EXISTS \"" + col.name() + "\" " + col.duckType());
                }
                current.put(col.name(), col.duckType());
                log.warn("RawMySQL 加列（TableMap 自愈）: {}.{} ({})", lakeTable, col.name(), col.duckType());
            } else if (!have.equals(col.duckType()) && props.getMaintenance().isFollowTypeChange()) {
                try (Statement s = conn.createStatement()) {
                    s.execute("ALTER TABLE " + DuckLakeEngine.LAKE + "." + DuckLakeEngine.quoted(lakeTable)
                            + " ALTER COLUMN \"" + col.name() + "\" SET DATA TYPE " + col.duckType());
                }
                current.put(col.name(), col.duckType());
                syncState.getDdlApplied().increment();
                log.warn("RawMySQL 类型自愈: {}.{} {} -> {}", lakeTable, col.name(), have, col.duckType());
            }
        }
    }

    private Map<String, String> loadLakeColumns(Connection conn, String lakeTable) throws SQLException {
        String[] parts = lakeTable.split("\\.", 2);
        Map<String, String> out = new LinkedHashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT column_name, data_type FROM information_schema.columns "
                        + "WHERE table_catalog=? AND table_schema=? AND table_name=? ORDER BY ordinal_position")) {
            ps.setString(1, DuckLakeEngine.LAKE);
            ps.setString(2, parts[0]);
            ps.setString(3, parts[1]);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.put(rs.getString(1), DuckType.normalize(rs.getString(2)));
            }
        }
        return out;
    }

    private void applyDdl(String database, String ddl, long sourceTsMs) throws SQLException {
        long t0 = System.currentTimeMillis();
        engine.withLock(conn -> {
            conn.setAutoCommit(false);
            try {
                ddlApplier.applyRawMySql(conn, database, ddl, this::invalidate,
                        table -> { throw new IllegalStateException(
                                "RAW_MYSQL 重建需要 scanner 通道，当前不可用: " + table); });
                conn.commit();
            } catch (RuntimeException | SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
            return null;
        });
        syncState.batchCommitted(0, sourceTsMs, deliverLag(t0, sourceTsMs),
                0, System.currentTimeMillis() - t0);
    }

    private void invalidate(String lakeTable) {
        boolean wildcard = lakeTable.endsWith(".*");
        String target = wildcard ? lakeTable.substring(0, lakeTable.length() - 1) : lakeTable;
        knownColumns.keySet().removeIf(name -> matchesLakeTable(name, target, wildcard));
        noKeyWarned.removeIf(name -> matchesLakeTable(name, target, wildcard));
        sqlCache.keySet().removeIf(table -> matchesLakeTable(table.lakeTable(), target, wildcard));
        tables.entrySet().removeIf(entry -> matchesLakeTable(entry.getValue().lakeTable(), target, wildcard));
        tableDefinitions.entrySet().removeIf(entry ->
                matchesLakeTable(entry.getValue().table().lakeTable(), target, wildcard));
    }

    private static boolean matchesLakeTable(String candidate, String target, boolean wildcard) {
        return wildcard ? candidate.startsWith(target) : candidate.equals(target);
    }

    private void dropStagings(Connection conn, int count) {
        try (Statement s = conn.createStatement()) {
            for (int i = 0; i < count; i++) {
                s.execute("DROP TABLE IF EXISTS " + DuckLakeEngine.MEM + ".main.stg_raw_mysql_" + i);
            }
        } catch (SQLException e) {
            log.debug("RawMySQL staging 清理失败（下批覆盖）: {}", e.getMessage());
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
        syncState.recordStage(SyncState.Reader.MYSQL, stage, nanos);
    }

    private void saveStandaloneOffset(RawMySqlOffset.Position position) {
        long started = System.nanoTime();
        try {
            offset.save(sourceName, position);
        } catch (RuntimeException e) {
            syncState.batchFailed();
            throw e;
        }
        recordStage(SyncState.Stage.OFFSET_ACK, System.nanoTime() - started);
    }

    private static long deliverLag(long deliveredAtMs, long sourceTsMs) {
        return sourceTsMs <= 0 ? 0 : Math.max(0, deliveredAtMs - sourceTsMs);
    }

    // ───────────────────────── source contract / helpers ─────────────────────────

    private record SourceStatus(String filename, long position, String gtidSet) {
    }

    private SourceStatus sourceStatus() {
        try (Connection c = DriverManager.getConnection(src.jdbcUrl(), src.getUser(), src.getPassword());
             Statement s = c.createStatement()) {
            String format = scalar(s, "SELECT @@GLOBAL.binlog_format");
            String image = scalar(s, "SELECT @@GLOBAL.binlog_row_image");
            String metadata = scalar(s, "SELECT @@GLOBAL.binlog_row_metadata");
            String compression = scalar(s, "SELECT @@GLOBAL.binlog_transaction_compression");
            if (!"ROW".equalsIgnoreCase(format)) {
                throw new IllegalStateException("RAW_MYSQL 要求 binlog_format=ROW，当前=" + format);
            }
            if (!"FULL".equalsIgnoreCase(image)) {
                throw new IllegalStateException("RAW_MYSQL 要求 binlog_row_image=FULL，当前=" + image);
            }
            if (!"FULL".equalsIgnoreCase(metadata)) {
                throw new IllegalStateException("RAW_MYSQL 要求 binlog_row_metadata=FULL，当前=" + metadata);
            }
            if ("ON".equalsIgnoreCase(compression)) {
                throw new IllegalStateException("RAW_MYSQL 当前要求 binlog_transaction_compression=OFF");
            }
            String gtidMode = scalar(s, "SELECT @@GLOBAL.gtid_mode");
            String gtid = "ON".equalsIgnoreCase(gtidMode)
                    ? scalar(s, "SELECT @@GLOBAL.gtid_executed") : "";
            if (gtid.isBlank()) {
                log.warn("MySQL GTID 未启用：RAW_MYSQL 可按 file/position 续传，但切主不能自动续位");
            }
            try (ResultSet rs = s.executeQuery("SHOW BINARY LOG STATUS")) {
                if (!rs.next()) throw new IllegalStateException("SHOW BINARY LOG STATUS 无结果");
                return new SourceStatus(rs.getString(1), rs.getLong(2), gtid);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("RAW_MYSQL 源端契约检查失败: " + e.getMessage(), e);
        }
    }

    private static String scalar(Statement s, String sql) throws SQLException {
        try (ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? Objects.toString(rs.getObject(1), "") : "";
        }
    }

    private RawMySqlOffset.Position positionOf(Event event) {
        if (!(event.getHeader() instanceof EventHeaderV4 header) || header.getNextPosition() <= 0) return null;
        return new RawMySqlOffset.Position(currentFilename, header.getNextPosition(), currentGtidSet());
    }

    private String currentGtidSet() {
        BinaryLogClient c = client;
        return c == null || c.getGtidSet() == null ? "" : c.getGtidSet();
    }

    private boolean captured(String database, String table) {
        if (!databaseIncluded(database)) return false;
        String full = database + "." + table;
        if (infrastructureTables.contains(full.toLowerCase(Locale.ROOT))
                || infrastructureTables.contains(table.toLowerCase(Locale.ROOT))) return false;
        return excludes.stream().noneMatch(p -> p.matcher(full).matches());
    }

    private boolean databaseIncluded(String database) {
        if (database == null || database.isBlank()) return includes.isEmpty();
        if (Set.of("mysql", "sys", "information_schema", "performance_schema")
                .contains(database.toLowerCase(Locale.ROOT))) return false;
        return includes.isEmpty() || includes.stream().anyMatch(p -> p.matcher(database).matches());
    }

    private static List<Pattern> compileList(String csv) {
        if (csv == null || csv.isBlank()) return List.of();
        List<Pattern> out = new ArrayList<>();
        for (String item : csv.split(",")) {
            if (!item.isBlank()) out.add(Pattern.compile(item.strip()));
        }
        return out;
    }

    private static boolean isTableDdl(String sql) {
        return sql.startsWith("CREATE TABLE") || sql.startsWith("CREATE TEMPORARY TABLE")
                || sql.startsWith("ALTER TABLE") || sql.startsWith("DROP TABLE")
                || sql.startsWith("DROP TEMPORARY TABLE") || sql.startsWith("RENAME TABLE")
                || sql.startsWith("TRUNCATE") || sql.startsWith("DROP DATABASE")
                || sql.startsWith("DROP SCHEMA");
    }

    private static ColumnType effectiveType(byte code, int metadata) {
        ColumnType type = ColumnType.byCode(code & 0xff);
        if (type != ColumnType.STRING || metadata < 256) return type;
        int high = metadata >> 8;
        if ((high & 0x30) == 0x30) {
            if (high == ColumnType.ENUM.getCode()) return ColumnType.ENUM;
            if (high == ColumnType.SET.getCode()) return ColumnType.SET;
        }
        return ColumnType.byCode(high | 0x30);
    }

    private String rawDuckType(ColumnType type, int metadata, boolean unsigned) {
        return switch (type) {
            case TINY -> "SMALLINT";
            case SHORT -> unsigned ? "INTEGER" : "SMALLINT";
            case INT24 -> "INTEGER";
            case LONG -> unsigned ? "BIGINT" : "INTEGER";
            case LONGLONG -> unsigned ? "UBIGINT" : "BIGINT";
            case FLOAT -> "FLOAT";
            case DOUBLE -> "DOUBLE";
            case NEWDECIMAL -> {
                int precision = metadata & 0xff;
                int scale = metadata >> 8;
                yield precision > 0 && precision <= 38 && scale <= precision
                        ? "DECIMAL(" + precision + ',' + scale + ')' : "VARCHAR";
            }
            case BIT -> ((metadata >> 8) * 8 + (metadata & 0xff)) == 1 ? "BOOLEAN" : "BLOB";
            case DATE -> "DATE";
            case TIME, TIME_V2 -> "TIME";
            case DATETIME, DATETIME_V2 -> "TIMESTAMP";
            case TIMESTAMP, TIMESTAMP_V2 -> "TIMESTAMPTZ";
            case YEAR -> "INTEGER";
            case JSON -> props.getMaintenance().isJsonAsVariant() ? "VARIANT" : "JSON";
            case BLOB, TINY_BLOB, MEDIUM_BLOB, LONG_BLOB, GEOMETRY, VECTOR -> "BLOB";
            default -> "VARCHAR";
        };
    }

    private static boolean isTemporal(ColumnType type) {
        return switch (type) {
            case DATE, TIME, TIME_V2, DATETIME, DATETIME_V2, TIMESTAMP, TIMESTAMP_V2 -> true;
            default -> false;
        };
    }

    private static String castExpr(String column, String duckType) {
        if ("VARCHAR".equals(duckType)) return column;
        if ("BLOB".equals(duckType)) return "unhex(" + column + ')';
        return switch (duckType) {
            case "DATE", "TIME", "TIMETZ", "TIMESTAMP", "TIMESTAMPTZ", "VARIANT", "JSON", "BOOLEAN" ->
                    "TRY_CAST(" + column + " AS " + duckType + ')';
            default -> "CAST(" + column + " AS " + duckType + ')';
        };
    }

    private String lakeTableName(String database, String table) {
        return props.getMaintenance().getSchemaPrefix() + database + "." + table;
    }
}
