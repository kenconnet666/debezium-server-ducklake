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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * pgoutput 原始串直通读取器：pgjdbc replication API 直读 pgoutput，
 * 绕开 Debezium engine 的 Struct 物化 + unpark 瓶颈（harness 实测 153-163k rows/s，
 * 约 Debezium 13-14×）。
 * <p>
 * 写湖路径与 Debezium 路径共用相同 sink：DuckLakeEngine.withLock + DuckDBAppender
 * 全 VARCHAR staging + DELETE/QUALIFY INSERT 投影——staging 格式完全一致，类型名来自
 * information_schema 而非 Debezium Schema，通过 {@link DdlApplier#pgColumnTypes} 获取。
 * <p>
 * DDL 跟随两路并行：① Relation 消息自愈（加列即刻感知）；
 * ② DDL 审计表 Insert 行经 {@link DdlApplier#applyRaw} 复用 PG 前端处理 rename/删列/删表。
 */
@Slf4j
class RawPgReader {

    private final DucklakeProperties props;
    private final DuckLakeEngine engine;
    private final DdlApplier ddlApplier;
    private final SyncState syncState;
    private final RawPgOffset offset;
    private final DucklakeProperties.Source src;
    private final DucklakeProperties.Engine eng;

    /** pgoutput Relation 缓存（OID → 列元信息，首次见到时查 information_schema） */
    private record ColDef(String name, String duckType, boolean isKey) {}
    private record RelInfo(String schema, String table, List<ColDef> cols) {
        List<String> keyColumns() { return cols.stream().filter(ColDef::isKey).map(ColDef::name).toList(); }
    }
    private final Map<Integer, RelInfo> relations = new HashMap<>();

    /** DDL 审计表名集合（小写）——这些表的 Insert 事件走 DdlApplier 而非落湖 */
    private final Set<String> ddlAuditTables;

    /** 批次内待落湖行 */
    private record PendingRow(int relOid, boolean deleted, String[] values) {}
    private final List<PendingRow> pending = new ArrayList<>();
    /** 最近一个 Commit 消息的 end LSN（向 PG 确认后 slot 才推进） */
    private long batchEndLsn = 0;
    private long lastFlushMs = System.currentTimeMillis();

    /** 湖表已知列缓存（避免每批 DESCRIBE） */
    private final Map<String, Map<String, String>> knownColumns = new HashMap<>();
    /** 无主键已告警的湖表 */
    private final Set<String> noKeyWarned = new HashSet<>();

    /**
     * per-relOid SQL 片段缓存：stageRows/applyStaging 所需字符串在首次构造后复用。
     * schema 变更（parseRelation 写入新RelInfo / ensureTable 加列）时失效对应条目。
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
    private final Map<Integer, TableSqlParts> sqlCache = new HashMap<>();
    /** lakeTable → relOid 反向映射：ensureTable 加列时需要通过表名使缓存失效 */
    private final Map<String, Integer> lakeTableOid = new HashMap<>();

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
        this.ddlAuditTables = new HashSet<>(props.getMaintenance().getDdlAuditTables());
    }

    void stop() {
        stopped = true;
    }

    boolean isStopped() {
        return stopped;
    }

    /** 主循环，由 RawPgRunner 的专用线程调用，阻塞直到 stopped 或 interrupted。 */
    void run() throws Exception {
        long startLsn = offset.load(src.getSlotName());
        try (Connection repl = openReplicationConn()) {
            PGReplicationStream stream = openStream(repl, startLsn);
            log.info("pgoutput 流已就绪: slot={} startLsn={}", src.getSlotName(),
                    startLsn == 0 ? "HEAD" : "0x" + Long.toHexString(startLsn));
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
            case 'R' -> parseRelation(msg);
            case 'B' -> { /* Begin: skip finalLsn(8)/commitTime(8)/xid(4) */ }
            case 'I' -> parseInsert(msg);
            case 'U' -> parseUpdate(msg);
            case 'D' -> parseDelete(msg);
            case 'C' -> {
                msg.get();        // flags (unused)
                msg.getLong();    // commit lsn
                batchEndLsn = msg.getLong(); // end lsn = slot 确认位点
                msg.getLong();    // commit timestamp
                maybeFlush(stream, pending.size() >= eng.getMaxBatchSize());
            }
            // Truncate/Origin/Type/Message → 忽略
        }
    }
    // ──────────── pgoutput 消息解析 ────────────

    private void parseRelation(ByteBuffer msg) throws SQLException {
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
            msg.getInt(); // type OID（用 information_schema 查更准，见下）
            msg.getInt(); // atttypmod
        }
        // 从 information_schema 取精确的 DuckDB 列类型
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
            // schema 有变化（DDL 期间 Relation 消息更新），失效 SQL 缓存
            sqlCache.remove(oid);
        }
    }

    private void parseInsert(ByteBuffer msg) {
        int oid = msg.getInt();
        msg.get(); // 'N'
        String[] vals = readTuple(msg, relations.get(oid));
        if (vals != null) pending.add(new PendingRow(oid, false, vals));
    }

    private void parseUpdate(ByteBuffer msg) {
        int oid = msg.getInt();
        // pgoutput Update: [K|O 旧 tuple]? N 新 tuple
        byte indicator = msg.get(); // 先无条件消费，可能是 K/O/N
        if (indicator == 'K' || indicator == 'O') {
            skipTuple(msg); // 跳过旧 tuple
            msg.get();      // 消费后续的 'N'
        }
        // indicator == 'N' 时已在新 tuple 列数前，直接读取
        String[] vals = readTuple(msg, relations.get(oid));
        if (vals != null) pending.add(new PendingRow(oid, false, vals));
    }

    private void parseDelete(ByteBuffer msg) {
        int oid = msg.getInt();
        msg.get(); // 'K' or 'O'
        RelInfo rel = relations.get(oid);
        String[] keyVals = readTuple(msg, rel); // null rel 时 readTuple 内部 skipTuple 安全处理
        if (rel == null || keyVals == null) return; // rel 判断提前（逻辑主因）
        // Delete：只保留主键列值，其余置 null；__deleted 标记将驱动 insertFromStaging 跳过插入
        String[] vals = new String[rel.cols().size()];
        for (int i = 0; i < rel.cols().size(); i++) {
            if (rel.cols().get(i).isKey()) vals[i] = keyVals[i];
        }
        pending.add(new PendingRow(oid, true, vals));
    }
    // ──────────── 批次刷湖 ────────────

    private void maybeFlush(PGReplicationStream stream, boolean force) throws Exception {
        long now = System.currentTimeMillis();
        boolean timeout = (now - lastFlushMs) >= Math.max(eng.getPollIntervalMs() * 50L, 500L);
        if (pending.isEmpty()) {
            // 无数据时仍要周期向 PG 心跳，防 slot WAL 扣留
            if (batchEndLsn > 0 && timeout) {
                ackLsn(stream, batchEndLsn);
                lastFlushMs = now;
            }
            return;
        }
        if (!force && !timeout) return;

        long flushLsn = batchEndLsn;
        List<PendingRow> batch = new ArrayList<>(pending);
        pending.clear();
        long t0 = System.currentTimeMillis();

        engine.withLock(conn -> {
            writeBatch(conn, batch);
            return null;
        });

        if (flushLsn > 0) {
            ackLsn(stream, flushLsn);
            offset.save(src.getSlotName(), flushLsn);
        }
        long elapsed = System.currentTimeMillis() - t0;
        lastFlushMs = System.currentTimeMillis();
        syncState.batchCommitted(batch.size(), System.currentTimeMillis(), 0, elapsed, 0);
        log.debug("RawPg 落湖 {} 行 {}ms lsn=0x{}", batch.size(), elapsed, Long.toHexString(flushLsn));
    }

    private void ackLsn(PGReplicationStream stream, long lsn) throws SQLException {
        LogSequenceNumber l = LogSequenceNumber.valueOf(lsn);
        stream.setAppliedLSN(l);
        stream.setFlushedLSN(l);
        stream.forceUpdateStatus();
    }

    /** 两阶段写：staging（湖事务外）→ DELETE+INSERT（单湖事务）。与 DuckLakeChangeConsumer 同构。 */
    private void writeBatch(Connection conn, List<PendingRow> batch) throws SQLException {
        // 按表顺序分组（保序——同一批内后来的行覆盖前面的，依赖 __seq 排序）
        Map<Integer, List<PendingRow>> byRel = new LinkedHashMap<>();
        for (PendingRow r : batch) byRel.computeIfAbsent(r.relOid(), k -> new ArrayList<>()).add(r);

        // 拆分：DDL 审计行单独处理，数据行走 staging
        List<Map.Entry<Integer, List<PendingRow>>> dataSegs = new ArrayList<>();
        List<Map.Entry<Integer, List<PendingRow>>> ddlSegs  = new ArrayList<>();
        for (Map.Entry<Integer, List<PendingRow>> e : byRel.entrySet()) {
            RelInfo rel = relations.get(e.getKey());
            if (rel != null && ddlAuditTables.contains(rel.table().toLowerCase())) {
                ddlSegs.add(e);
            } else {
                dataSegs.add(e);
            }
        }

        // 阶段一：staging（湖事务外）
        for (int i = 0; i < dataSegs.size(); i++) {
            int oid = dataSegs.get(i).getKey();
            RelInfo rel = relations.get(oid);
            if (rel != null) stageRows(conn, oid, rel, dataSegs.get(i).getValue(), i);
        }

        // 阶段二：单湖事务
        conn.setAutoCommit(false);
        try {
            for (int i = 0; i < dataSegs.size(); i++) {
                int oid = dataSegs.get(i).getKey();
                RelInfo rel = relations.get(oid);
                if (rel != null) applyStaging(conn, oid, rel, i);
            }
            // DDL 审计行在同一事务内按序应用
            if (!ddlSegs.isEmpty()) {
                List<Map<String, String>> ddlRows = new ArrayList<>();
                for (Map.Entry<Integer, List<PendingRow>> e : ddlSegs) {
                    RelInfo rel = relations.get(e.getKey());
                    if (rel == null) continue;
                    for (PendingRow r : e.getValue()) {
                        ddlRows.add(toRowMap(rel, r));
                    }
                }
                ddlApplier.applyRaw(conn, ddlRows, lakeTable -> {
                    knownColumns.remove(lakeTable);
                    // DDL rename/drop 后 SQL 片段也过期，防下一批使用旧 cols/proj
                    Integer oid = lakeTableOid.get(lakeTable);
                    if (oid != null) sqlCache.remove(oid);
                }, lakeTable -> {
                    knownColumns.remove(lakeTable);
                    Integer oid = lakeTableOid.get(lakeTable);
                    if (oid != null) sqlCache.remove(oid);
                });
            }
            conn.commit();
        } catch (SQLException ex) {
            conn.rollback();
            knownColumns.clear();
            sqlCache.clear();   // schema 可能已变，整批 SQL 缓存全部失效
            throw ex;
        } finally {
            conn.setAutoCommit(true);
            dropStagings(conn, dataSegs.size());
        }
    }
    // ──────────── Staging + Lake 写入原语 ────────────

    private void stageRows(Connection conn, int oid, RelInfo rel, List<PendingRow> rows, int idx) throws SQLException {
        TableSqlParts parts = sqlCache.computeIfAbsent(oid, k -> buildSqlParts(rel));
        String stgName = DuckLakeEngine.MEM + ".main.stg_raw_" + idx;
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE OR REPLACE TABLE " + stgName + " (" + parts.stageCols() + ")");
        }
        DuckDBConnection dc = conn.unwrap(DuckDBConnection.class);
        try (DuckDBAppender ap = dc.createAppender(DuckLakeEngine.MEM, "main", "stg_raw_" + idx)) {
            long seq = 0;
            List<ColDef> cols = rel.cols();
            for (PendingRow row : rows) {
                ap.beginRow();
                for (int i = 0; i < cols.size(); i++) {
                    ap.append(i < row.values().length ? row.values()[i] : null);
                }
                ap.append(row.deleted() ? "true" : "false");
                ap.append(seq++);
                ap.endRow();
            }
        }
    }

    /** ensureTable + DELETE + QUALIFY INSERT（镜像 upsert/delete 语义，与 DuckLakeChangeConsumer 等价）。 */
    private void applyStaging(Connection conn, int oid, RelInfo rel, int idx) throws SQLException {
        String lakeTable = lakeTableName(rel);
        ensureTable(conn, oid, lakeTable, rel);

        TableSqlParts parts = sqlCache.computeIfAbsent(oid, k -> buildSqlParts(rel));
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
                    + ") WHERE COALESCE(\"__deleted\", 'false') <> 'true'");
        }
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

    private void ensureTable(Connection conn, int oid, String lakeTable, RelInfo rel) throws SQLException {
        lakeTableOid.put(lakeTable, oid); // 始终更新（表 drop+recreate OID 变化时保持准确）
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
        // Relation 消息感知到新列：即刻 ALTER ADD COLUMN（DDL 演化自愈）
        for (ColDef col : rel.cols()) {
            if (!cols.containsKey(col.name())) {
                try (Statement s = conn.createStatement()) {
                    s.execute("ALTER TABLE " + DuckLakeEngine.LAKE + "." + DuckLakeEngine.quoted(lakeTable)
                            + " ADD COLUMN IF NOT EXISTS \"" + col.name() + "\" " + col.duckType());
                }
                cols.put(col.name(), col.duckType());
                // 列增加后 SQL 片段（cols/proj/stageCols）已过期，失效对应 OID 缓存
                Integer oidForTable = lakeTableOid.get(lakeTable);
                if (oidForTable != null) sqlCache.remove(oidForTable);
                log.warn("RawPg 加列（Relation 自愈）: {}.{} ({})", lakeTable, col.name(), col.duckType());
            }
        }
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
                            org.dpdns.zerodep.ducklake.sink.TypeMapper.normalizeDuckType(rs.getString(2)));
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
        ensureSlotExists(pg);
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
    private void ensureSlotExists(PGConnection pg) throws SQLException {
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
        log.info("RawPg replication slot '{}' 不存在，自动创建（pgoutput）", src.getSlotName());
        pg.getReplicationAPI()
                .createReplicationSlot()
                .logical()
                .withSlotName(src.getSlotName())
                .withOutputPlugin("pgoutput")
                .make();
    }

    // ──────────── pgoutput Tuple 解析 ────────────

    /** 读取 TupleData（Int16 列数 + 各列 kind/data），返回与 RelInfo.cols() 等长的字符串数组。*/
    private String[] readTuple(ByteBuffer msg, RelInfo rel) {
        if (rel == null) { skipTuple(msg); return null; }
        int colCount = msg.getShort() & 0xFFFF;
        String[] vals = new String[rel.cols().size()];
        for (int i = 0; i < colCount; i++) {
            byte kind = msg.get();
            if (kind == 'n') { /* null */ }
            else if (kind == 'u') { /* unchanged toast */ }
            else if (kind == 't') {
                int len = msg.getInt();
                byte[] bytes = new byte[len];
                msg.get(bytes);
                if (i < vals.length) vals[i] = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
            }
        }
        return vals;
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
        byte[] bytes = new byte[end - start];
        msg.position(start);
        msg.get(bytes);
        msg.get(); // consume null terminator
        return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }
    // ──────────── 工具方法 ────────────

    /**
     * raw-pg 专用 CAST 表达式：与 TypeMapper.castExpr 等价，但处理 pgoutput bytea 的 \x 前缀。
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
}

