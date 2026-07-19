package org.dpdns.zerodep.ducklake.engine;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.dpdns.zerodep.ducklake.config.DucklakeProperties;
import org.dpdns.zerodep.ducklake.ddl.DdlApplier;
import org.dpdns.zerodep.ducklake.sink.DuckLakeEngine;
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
import java.util.regex.Pattern;

/**
 * 首次接入的存量数据 scanner 化（maintenance.scanner-bootstrap）。
 * <p>
 * 原生 reader 先从当前复制位点启动，本组件随后异步枚举源表、建湖表，并通过 DuckDB
 * postgres/mysql scanner 直拉存量。直灌按主键 anti-join：流式先落的更新不会被存量覆盖，
 * 直灌后的变更则继续由流式 upsert/delete 收敛到当前态。
 * <p>
 * 无主键表无法安全 anti-join，MySQL 非默认库也可能超出 scanner attach 范围；这些表记录为
 * {@code no-pk-skip}，不补历史，后续流式按 insert-only。进度存于 catalog 的
 * {@code ducklake_bootstrap}，异常中止时 pending 表会在下次启动续跑。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ScannerBootstrap {

    /** 兼容旧部署遗留的信号/心跳表；DDL 审计表由配置单独排除。 */
    private static final Set<String> INFRA_TABLES = Set.of("dbz_heartbeat", "dbz_signal");

    private final DucklakeProperties props;
    private final DuckLakeEngine engine;
    private final DdlApplier ddlApplier;

    private volatile boolean takeOver;

    /** @param startLsn raw_pg_offset 中已确认的 LSN；0 表示首次接入。 */
    public void evaluateForRawPg(long startLsn) {
        evaluate(startLsn == 0, "PostgreSQL");
    }

    /** @param firstStart raw_mysql_offset 不存在时为 true。 */
    public void evaluateForRawMySql(boolean firstStart) {
        evaluate(firstStart, "MySQL");
    }

    private void evaluate(boolean firstStart, String sourceKind) {
        DucklakeProperties.Maintenance maintenance = props.getMaintenance();
        takeOver = maintenance.isScannerRefill()
                && maintenance.isScannerBootstrap()
                && engine.isScannerSourceReady()
                && needsBootstrap(firstStart);
        if (takeOver) {
            log.info("scanner bootstrap 接管 {} 首次存量：复制流就绪后由 scanner 异步直拉", sourceKind);
        }
    }

    private boolean needsBootstrap(boolean firstStart) {
        try (Connection connection = catalog()) {
            try (Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery(
                         "SELECT count(*) FROM ducklake_bootstrap WHERE status='pending'")) {
                if (rs.next() && rs.getLong(1) > 0) {
                    return true;
                }
            } catch (SQLException e) {
                // 状态表不存在即从未 bootstrap，继续用 offset 判断。
            }
            return firstStart;
        } catch (SQLException e) {
            log.warn("bootstrap 评估失败（跳过存量直灌）: {}", e.getMessage());
            return false;
        }
    }

    /** 流式启动后异步执行；与流式批写共用 DuckLake 单写者锁。 */
    public void runAsync() {
        if (takeOver) {
            Thread.ofVirtual().name("scanner-bootstrap").start(this::run);
        }
    }

    private void run() {
        long startedAt = System.currentTimeMillis();
        try (Connection catalog = catalog()) {
            try (Statement statement = catalog.createStatement()) {
                statement.execute("CREATE TABLE IF NOT EXISTS ducklake_bootstrap ("
                        + "table_name text PRIMARY KEY, status text NOT NULL, "
                        + "rows_loaded bigint, done_at timestamptz)");
            }
            seedChecklistIfEmpty(catalog);
            List<String> pending = new ArrayList<>();
            try (Statement statement = catalog.createStatement();
                 ResultSet rs = statement.executeQuery(
                         "SELECT table_name FROM ducklake_bootstrap WHERE status='pending' ORDER BY table_name")) {
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
            List<String> skipped = new ArrayList<>();
            long totalRows = 0;
            int completed = 0;
            int failures = 0;
            for (String sourceTable : pending) {
                String lakeTable = prefix + sourceTable;
                try {
                    int rows = engine.withLock(
                            connection -> ddlApplier.bootstrapTable(connection, lakeTable, sourceTable));
                    if (rows < 0) {
                        skipped.add(sourceTable);
                        continue;
                    }
                    totalRows += rows;
                    completed++;
                    markDone(catalog, sourceTable, "done", rows);
                } catch (Exception e) {
                    failures++;
                    log.warn("bootstrap 直灌失败（保留 pending，重启续跑）: {} ({})",
                            sourceTable, e.getMessage());
                }
            }
            markSkipped(catalog, skipped);
            log.info("scanner bootstrap 本轮完成：{} 表直灌 {} 行 / {} 表跳过存量 / {} 表待重试，耗时 {}s",
                    completed, totalRows, skipped.size(), failures,
                    (System.currentTimeMillis() - startedAt) / 1000);
        } catch (Exception e) {
            log.error("scanner bootstrap 异常中止（pending 保留，流式不受影响）: {}", e.getMessage(), e);
        }
    }

    private void seedChecklistIfEmpty(Connection catalog) throws SQLException {
        try (Statement statement = catalog.createStatement();
             ResultSet rs = statement.executeQuery("SELECT count(*) FROM ducklake_bootstrap")) {
            if (rs.next() && rs.getLong(1) > 0) {
                return;
            }
        }
        List<String> tables = listSourceTables();
        try (PreparedStatement statement = catalog.prepareStatement(
                "INSERT INTO ducklake_bootstrap (table_name, status) "
                        + "VALUES (?, 'pending') ON CONFLICT DO NOTHING")) {
            for (String table : tables) {
                statement.setString(1, table);
                statement.addBatch();
            }
            statement.executeBatch();
        }
        log.info("scanner bootstrap 清单就绪：{} 张表", tables.size());
    }

    /** 枚举源库用户表（schema.table/db.table），并应用 include/exclude 规则。 */
    private List<String> listSourceTables() throws SQLException {
        DucklakeProperties.Source source = props.getSource();
        String sql = switch (source.getType()) {
            case POSTGRES -> "SELECT schemaname, tablename FROM pg_tables "
                    + "WHERE schemaname NOT IN ('pg_catalog','information_schema') ORDER BY 1, 2";
            case MYSQL -> "SELECT table_schema, table_name FROM information_schema.tables "
                    + "WHERE table_type='BASE TABLE' AND table_schema NOT IN "
                    + "('mysql','sys','information_schema','performance_schema') ORDER BY 1, 2";
        };
        List<Pattern> includes = compileList(source.getSchemaIncludeList());
        List<Pattern> excludes = compileList(source.getTableExcludeList());
        Set<String> auditTables = Set.copyOf(props.getMaintenance().getDdlAuditTables());
        List<String> result = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(
                source.jdbcUrl(), source.getUser(), source.getPassword());
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                String schema = rs.getString(1);
                String table = rs.getString(2);
                String fullName = schema + "." + table;
                if (INFRA_TABLES.contains(table) || auditTables.contains(table)) {
                    continue;
                }
                if (!includes.isEmpty()
                        && includes.stream().noneMatch(pattern -> pattern.matcher(schema).matches())) {
                    continue;
                }
                if (excludes.stream().anyMatch(pattern -> pattern.matcher(fullName).matches())) {
                    continue;
                }
                result.add(fullName);
            }
        }
        return result;
    }

    /** 逗号分隔的 anchored 正则；空串表示不限制。 */
    private static List<Pattern> compileList(String csv) {
        if (csv == null || csv.isBlank()) {
            return List.of();
        }
        List<Pattern> result = new ArrayList<>();
        for (String part : csv.split(",")) {
            if (!part.isBlank()) {
                result.add(Pattern.compile(part.trim()));
            }
        }
        return result;
    }

    private void markSkipped(Connection catalog, List<String> tables) {
        if (tables.isEmpty()) {
            return;
        }
        log.warn("以下表跳过存量快照（无主键或 scanner 无法覆盖；后续流式 insert-only）: {}", tables);
        for (String table : tables) {
            markDone(catalog, table, "no-pk-skip", null);
        }
    }

    private void markDone(Connection connection, String table, String status, Integer rows) {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE ducklake_bootstrap SET status=?, rows_loaded=?, done_at=now() WHERE table_name=?")) {
            statement.setString(1, status);
            if (rows == null) {
                statement.setNull(2, java.sql.Types.BIGINT);
            } else {
                statement.setLong(2, rows);
            }
            statement.setString(3, table);
            statement.executeUpdate();
        } catch (SQLException e) {
            log.warn("bootstrap 进度标记失败（最多重启后幂等重灌）: {} ({})", table, e.getMessage());
        }
    }

    private Connection catalog() throws SQLException {
        DucklakeProperties.Lake lake = props.getLake();
        String url = "jdbc:postgresql://%s:%d/%s"
                .formatted(lake.getCatalogHost(), lake.getCatalogPort(), lake.getCatalogDb());
        return DriverManager.getConnection(url, lake.getCatalogUser(), lake.getCatalogPassword());
    }
}
