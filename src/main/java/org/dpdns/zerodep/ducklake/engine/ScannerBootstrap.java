package org.dpdns.zerodep.ducklake.engine;

import org.dpdns.zerodep.ducklake.config.DucklakeProperties;
import org.dpdns.zerodep.ducklake.ddl.DdlApplier;
import org.dpdns.zerodep.ducklake.sink.DuckLakeEngine;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * 首次接入的存量数据 scanner 化（maintenance.scanner-bootstrap，默认 true）：
 * <p>
 * 传统路径是 Debezium initial 快照（实测 22.4k 行/秒，1 亿行 ≈ 75 分钟，期间流式未开始）。
 * 本组件接管后：快照模式改 {@code no_data}（秒级拿一致位点+结构，流式从第一秒就在追），
 * 同时异步枚举捕获范围内的源表 → 建湖表（含注释/排序聚簇）→ scanner 直拉存量
 * （复用重建重灌原语，实测 81.5 万/25.2 万 行/秒）。
 * <p>
 * <b>一致性</b>（DBLog 思路的简化版）：直灌按主键 anti-join——增量流先落的行（其值 ≥ 流式
 * 起点，比快照读更新）不被覆盖；直灌之后的变更由流式 upsert 正常覆盖；快照读之前已删的行
 * scanner 天然读不到，之后删的由流式 DELETE 事件清掉——任意交错下最终收敛到当前态。
 * <b>兜底</b>：无主键表（无 anti-join 依据）与 MySQL 跨库表（scanner 只读 attach 绑定单库）
 * 收集后一次性写 signal 触发 blocking 快照（表已建好的建好，数据走 Debezium 老路）。
 * <b>断点续跑</b>：进度存 catalog PG 的 {@code ducklake_bootstrap} 表——中途崩溃重启后
 * 只补 pending 表，不重拉已完成的；全部完成后不再触发。
 * <b>降级</b>：scanner 通道未就绪（离线/源不可达）或用户显式改过 snapshot-mode 时完全让位，
 * 走 Debezium initial 快照原路。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ScannerBootstrap {

    /** 基建表（信号/心跳/DDL 审计）不参与 bootstrap：signal 由连接器内部消费，
     *  心跳表分钟级自产事件流式自然落湖，审计表阅后即焚——直灌无意义 */
    private static final Set<String> INFRA_TABLES = Set.of("dbz_heartbeat");

    private final DucklakeProperties props;
    private final DuckLakeEngine engine;
    private final DdlApplier ddlApplier;

    private volatile boolean takeOver;

    /** 是否接管首次快照（Runner buildProps 据此把 snapshot.mode 改为 no_data） */
    public boolean shouldTakeOver() {
        return takeOver;
    }

    /**
     * Runner 启动前评估（engine attach 已在 @PostConstruct 完成，状态可用）。
     * 仅在「开关开 + 用户没改快照模式 + scanner 通道就绪 + （首次接入或有未完成清单）」时接管。
     */
    public void evaluate() {
        DucklakeProperties.Maintenance m = props.getMaintenance();
        takeOver = m.isScannerRefill() && m.isScannerBootstrap()
                && "initial".equals(props.getEngine().getSnapshotMode())
                && engine.isScannerSrcAttached()
                && needsBootstrap();
        if (takeOver) {
            log.info("scanner bootstrap 接管首次存量：snapshot.mode → no_data，流式即刻开始，存量由 scanner 直拉");
        }
    }

    /** 引擎启动后异步执行（虚拟线程；与流式批写共用单写者锁，一表一锁段交错，互不停顿） */
    public void runAsync() {
        if (!takeOver) {
            return;
        }
        Thread.ofVirtual().name("scanner-bootstrap").start(this::run);
    }

    /** 首次接入 = offset 为空；或上次 bootstrap 有 pending 残留（崩溃续跑，此时 offset 可能非空） */
    private boolean needsBootstrap() {
        try (Connection c = catalog()) {
            try (Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery("SELECT count(*) FROM ducklake_bootstrap WHERE status='pending'")) {
                if (rs.next() && rs.getLong(1) > 0) {
                    return true;
                }
            } catch (SQLException e) {
                // 状态表不存在 = 从未 bootstrap 过，看 offset
            }
            try (Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery("SELECT count(*) FROM debezium_offset_storage")) {
                return rs.next() && rs.getLong(1) == 0;
            } catch (SQLException e) {
                return true; // offset 表不存在 = 首次启动
            }
        } catch (SQLException e) {
            log.warn("bootstrap 评估失败(回退 Debezium initial 快照): {}", e.getMessage());
            return false;
        }
    }

    private void run() {
        long t0 = System.currentTimeMillis();
        try (Connection c = catalog()) {
            try (Statement s = c.createStatement()) {
                s.execute("CREATE TABLE IF NOT EXISTS ducklake_bootstrap ("
                        + "table_name text PRIMARY KEY, status text NOT NULL, "
                        + "rows_loaded bigint, done_at timestamptz)");
            }
            seedChecklistIfEmpty(c);
            List<String> pending = new ArrayList<>();
            try (Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery("SELECT table_name FROM ducklake_bootstrap WHERE status='pending' ORDER BY table_name")) {
                while (rs.next()) {
                    pending.add(rs.getString(1));
                }
            }
            if (pending.isEmpty()) {
                log.info("scanner bootstrap 无待处理表");
                return;
            }
            log.info("scanner bootstrap 开始：{} 张表待直灌", pending.size());
            String prefix = props.getMaintenance().getSchemaPrefix();
            List<String> fallback = new ArrayList<>();
            long total = 0;
            int viaScanner = 0;
            for (String srcTable : pending) {
                String lakeTable = prefix + srcTable;
                try {
                    int n = engine.withLock(conn -> ddlApplier.bootstrapTable(conn, lakeTable, srcTable));
                    if (n < 0) {
                        fallback.add(srcTable); // 表已建，无主键/跨库的数据走 signal
                        continue;
                    }
                    total += n;
                    viaScanner++;
                    markDone(c, srcTable, "done", n);
                } catch (Exception e) {
                    log.warn("bootstrap 直灌失败(转 signal 兜底): {} ({})", srcTable, e.getMessage());
                    fallback.add(srcTable);
                }
            }
            if (!fallback.isEmpty()) {
                requestBlockingSnapshot(c, fallback);
            }
            log.info("scanner bootstrap 完成：{} 表直灌 {} 行 / {} 表转 signal，耗时 {}s",
                    viaScanner, total, fallback.size(), (System.currentTimeMillis() - t0) / 1000);
        } catch (Exception e) {
            // 状态表保留 pending —— 下次启动续跑；不影响已在跑的流式链路
            log.error("scanner bootstrap 异常中止(pending 保留,重启续跑;流式不受影响): {}", e.getMessage(), e);
        }
    }

    /** 首次运行：枚举捕获范围内源表写入清单（重启续跑时清单已在，跳过） */
    private void seedChecklistIfEmpty(Connection c) throws SQLException {
        try (Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*) FROM ducklake_bootstrap")) {
            if (rs.next() && rs.getLong(1) > 0) {
                return;
            }
        }
        List<String> tables = listSourceTables();
        try (PreparedStatement ps = c.prepareStatement(
                "INSERT INTO ducklake_bootstrap (table_name, status) VALUES (?, 'pending') ON CONFLICT DO NOTHING")) {
            for (String t : tables) {
                ps.setString(1, t);
                ps.addBatch();
            }
            ps.executeBatch();
        }
        log.info("scanner bootstrap 清单就绪：{} 张表", tables.size());
    }

    /** 枚举源库用户表（"schema.table"/"db.table" 全名），按 include/exclude 与基建表过滤 */
    private List<String> listSourceTables() throws SQLException {
        DucklakeProperties.Source src = props.getSource();
        String sql = switch (src.getType()) {
            case POSTGRES -> "SELECT schemaname, tablename FROM pg_tables "
                    + "WHERE schemaname NOT IN ('pg_catalog','information_schema') ORDER BY 1, 2";
            case MYSQL -> "SELECT table_schema, table_name FROM information_schema.tables "
                    + "WHERE table_type='BASE TABLE' AND table_schema NOT IN "
                    + "('mysql','sys','information_schema','performance_schema') ORDER BY 1, 2";
        };
        List<Pattern> includes = compileList(src.getSchemaIncludeList());
        List<Pattern> excludes = compileList(src.getTableExcludeList());
        Set<String> auditTables = Set.copyOf(props.getMaintenance().getDdlAuditTables());
        String signalTable = src.resolvedSignalTable();
        List<String> out = new ArrayList<>();
        try (Connection c = DriverManager.getConnection(src.jdbcUrl(), src.getUser(), src.getPassword());
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) {
                String schema = rs.getString(1);
                String table = rs.getString(2);
                String full = schema + "." + table;
                if (INFRA_TABLES.contains(table) || auditTables.contains(table) || full.equals(signalTable)) {
                    continue;
                }
                if (!includes.isEmpty() && includes.stream().noneMatch(p -> p.matcher(schema).matches())) {
                    continue;
                }
                if (excludes.stream().anyMatch(p -> p.matcher(full).matches())) {
                    continue;
                }
                out.add(full);
            }
        }
        return out;
    }

    /** Debezium include/exclude 语义：逗号分隔的 anchored 正则；空串=空列表 */
    private static List<Pattern> compileList(String csv) {
        if (csv == null || csv.isBlank()) {
            return List.of();
        }
        List<Pattern> out = new ArrayList<>();
        for (String part : csv.split(",")) {
            if (!part.isBlank()) {
                out.add(Pattern.compile(part.trim()));
            }
        }
        return out;
    }

    /** 兜底表一次性 blocking 快照（无主键/跨库/直灌失败）：一轮流式停顿换全部补齐；
     *  signal 写入失败保留 pending（重启续跑再试）。
     *  ⚠️ 必须先等复制位点确立：bootstrap 与引擎启动并发，位点确立前写入的 signal 行
     *  其 WAL/binlog 事件在位点之前、流式看不到 = signal 静默丢失（集成测试实测踩坑） */
    private void requestBlockingSnapshot(Connection catalog, List<String> tables) {
        if (!waitForStreamingPosition(catalog)) {
            log.error("复制位点未就绪,兜底 signal 暂缓(表保持 pending,重启续跑): {}", tables);
            return;
        }
        DucklakeProperties.Source src = props.getSource();
        StringBuilder collections = new StringBuilder();
        for (String t : tables) {
            collections.append(collections.isEmpty() ? "" : ",").append('"').append(t).append('"');
        }
        String sql = "INSERT INTO " + src.resolvedSignalTable() + " (id, type, data) VALUES (?, 'execute-snapshot', ?)";
        try (Connection c = DriverManager.getConnection(src.jdbcUrl(), src.getUser(), src.getPassword());
             PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, UUID.randomUUID().toString());
            ps.setString(2, "{\"data-collections\":[" + collections + "],\"type\":\"blocking\"}");
            ps.executeUpdate();
            for (String t : tables) {
                markDone(catalog, t, "signal", null);
            }
            log.warn("bootstrap 兜底表已托付 blocking 快照({} 张): {}", tables.size(), tables);
        } catch (SQLException e) {
            log.error("bootstrap 兜底 signal 写入失败(表保持 pending,重启续跑): {}", e.getMessage());
        }
    }

    /**
     * 等 Debezium 首次落盘 offset（= 复制位点已确立）：此后写入的 signal 行必在位点之后、
     * 必被流式捕获。no_data 模式位点秒级确立，offset 随 flush 周期（默认 10s）落盘，
     * 常规 30s 内出现；上限 3 分钟，超时返回 false（表保持 pending 重启续跑）。
     */
    private boolean waitForStreamingPosition(Connection catalog) {
        for (int i = 0; i < 90; i++) {
            try (Statement s = catalog.createStatement();
                 ResultSet rs = s.executeQuery("SELECT count(*) FROM debezium_offset_storage")) {
                if (rs.next() && rs.getLong(1) > 0) {
                    return true;
                }
            } catch (SQLException e) {
                // offset 表尚未建 = 引擎更早阶段,继续等
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    private void markDone(Connection c, String table, String status, Integer rows) {
        try (PreparedStatement ps = c.prepareStatement(
                "UPDATE ducklake_bootstrap SET status=?, rows_loaded=?, done_at=now() WHERE table_name=?")) {
            ps.setString(1, status);
            if (rows == null) {
                ps.setNull(2, java.sql.Types.BIGINT);
            } else {
                ps.setLong(2, rows);
            }
            ps.setString(3, table);
            ps.executeUpdate();
        } catch (SQLException e) {
            log.warn("bootstrap 进度标记失败(无碍,最多重启后重灌一次幂等): {} ({})", table, e.getMessage());
        }
    }

    /** catalog PG 直连（与 offset/schema history 同库，bootstrap 状态表也放这里） */
    private Connection catalog() throws SQLException {
        DucklakeProperties.Lake lake = props.getLake();
        String url = "jdbc:postgresql://%s:%d/%s"
                .formatted(lake.getCatalogHost(), lake.getCatalogPort(), lake.getCatalogDb());
        return DriverManager.getConnection(url, lake.getCatalogUser(), lake.getCatalogPassword());
    }
}
