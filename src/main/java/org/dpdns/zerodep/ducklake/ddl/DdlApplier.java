package org.dpdns.zerodep.ducklake.ddl;

import org.dpdns.zerodep.ducklake.config.DucklakeProperties;
import org.dpdns.zerodep.ducklake.metrics.SyncState;
import org.dpdns.zerodep.ducklake.sink.DuckLakeEngine;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Struct;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * DDL 信号流消费：PG event trigger（ddl_command_end + sql_drop）写入 sys_ddl_log，
 * 该表随 publication 被 Debezium 当普通表抓走，到这里翻译应用到湖侧——**纯跟随，不留档**
 * （2026-07-08 起湖侧 meta.ddl_history 审计已裁撤；信号表本身由维护任务每日 TRUNCATE 阅后即焚）。
 * <p>
 * 判定原理（PG 目录语义，确定性非启发式）：RENAME COLUMN 是 pg_attribute 原地改名、
 * <b>不触发 sql_drop</b>；DROP+ADD 必触发。故按事务号 xid 分组后，
 * "ALTER TABLE 且同事务无 sql_drop 且语句含 RENAME COLUMN" ⇒ 真 rename，湖侧同步
 * ALTER TABLE ... RENAME COLUMN（这是数据驱动 schema 演进做不到、必须靠 DDL 信号补齐的唯一动作）。
 * <p>
 * 其余跟随分工：加列与类型安全放宽交给数据驱动 ensureTable 幂等处理；删列默认跟随真删
 * （followDropColumn=false 时保留湖列留历史）；DROP TABLE 默认跟随真删湖表
 * （followDropTable=false 时湖表保留；快照重放的历史 DROP 一律跳过——快照给的是当前态，
 * 活表不能被历史 DDL 误删）；建表延迟到首批数据；快照重放旧 DDL 时靠"列存在性检查"天然幂等。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DdlApplier {

    private static final Pattern RENAME_COLUMN = Pattern.compile(
            "RENAME\\s+COLUMN\\s+\"?([\\w$]+)\"?\\s+TO\\s+\"?([\\w$]+)\"?", Pattern.CASE_INSENSITIVE);

    /** COMMENT ON TABLE/COLUMN ... IS <值>——取 IS 之后原样搬运（'文本'/NULL 的字面量
     *  语法 PG 与 DuckDB 兼容，含 '' 转义;E'...' 等 PG 特有形式匹配不上则跳过） */
    private static final Pattern COMMENT_IS = Pattern.compile(
            "COMMENT\\s+ON\\s+(?:TABLE|COLUMN)\\s+\\S+\\s+IS\\s+('(?:[^']|'')*'|NULL)\\s*;?\\s*$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private final DucklakeProperties props;
    private final SyncState syncState;

    /**
     * 处理一段连续的 sys_ddl_log 事件（由消费者在湖事务内调用，与数据写入同序同事务）。
     *
     * @param cacheInvalidator 湖表结构被本方法改动后回调（消费者失效其 knownColumns 缓存）；
     *                         以回调而非直引消费者，避免 consumer↔applier 循环依赖
     */
    public void apply(Connection conn, List<Struct> run, Consumer<String> cacheInvalidator) throws SQLException {
        // 只认插入(c)与快照读(r)——信号表若被 DELETE 清理会产生墓碑(d)事件,不得当作信号重放
        // (常规清理走 TRUNCATE 不产生事件,此过滤是对误用 DELETE 的兜底)
        List<Struct> signals = run.stream()
                .filter(r -> {
                    String op = str(r, "__op");
                    return op == null || "c".equals(op) || "r".equals(op);
                })
                .toList();
        if (signals.isEmpty()) {
            return;
        }
        syncState.getDdlAudited().increment(signals.size());

        // 按源事务 xid 分组（同一条 ALTER 产生的 ddl_command_end 与 sql_drop 共享 xid）。
        // 已知边界:分组只在单批内做,同事务两行被批边界劈开时(截断事务中间,低概率)
        // hasDrop 误判为 false→删列跟随静默跳过(集成测试偶发复现);湖列多留无数据
        // 正确性影响,下一次同表 DDL 或人工 ALTER 可补齐,故不为此引入跨批配对缓存
        Map<Long, List<Struct>> byXid = new LinkedHashMap<>();
        for (Struct row : signals) {
            byXid.computeIfAbsent(asLong(row, "xid"), k -> new ArrayList<>()).add(row);
        }

        for (Map.Entry<Long, List<Struct>> group : byXid.entrySet()) {
            // 镜像语义:源 DROP TABLE/DROP SCHEMA CASCADE → 湖表真删(时间旅行窗口内旧 snapshot
            // 仍可回看)。⚠️ DROP 命令不出现在 pg_event_trigger_ddl_commands() 里(PG 语义,
            // 集成测试实测踩坑)——审计流只有 sql_drop 行,跟随必须由 sql_drop 行自身触发,
            // 不能挂在下面的 ddl_command_end 循环
            if (props.getMaintenance().isFollowDropTable()) {
                applyDropTables(conn, group.getValue(), cacheInvalidator);
            }
            boolean hasDrop = group.getValue().stream()
                    .anyMatch(r -> "sql_drop".equals(str(r, "ev")));
            for (Struct row : group.getValue()) {
                String tag = str(row, "tag");
                String query = str(row, "query_text");
                if (!"ddl_command_end".equals(str(row, "ev")) || tag == null || query == null) {
                    continue;
                }
                if ("ALTER TABLE".equals(tag) && !hasDrop) {
                    applyRenameIfAny(conn, str(row, "object_identity"), query, cacheInvalidator);
                } else if ("ALTER TABLE".equals(tag) && hasDrop && props.getMaintenance().isFollowDropColumn()) {
                    applyDropColumns(conn, group.getValue(), cacheInvalidator);
                } else if ("COMMENT".equals(tag)) {
                    applyComment(conn, str(row, "object_type"), str(row, "object_identity"), query);
                }
                // 其余（CREATE TABLE 等）：建表延迟到首批数据到达;
                // 类型变更由数据驱动的 ensureTable 安全放宽跟随（TypeMapper.isSafeWidening 白名单）
            }
        }
    }

    /**
     * COMMENT ON TABLE/COLUMN 跟随：对象名取自 object_identity（规范名，免解析原句里的写法），
     * 注释值从 query_text 的 IS 之后原样搬运。重复应用=同值覆盖，天然幂等（快照重放同样无害）。
     * 失败仅告警不抛出——注释是元数据锦上添花，不值得让 CDC 批进入重试。
     */
    private void applyComment(Connection conn, String objectType, String identity, String query) {
        if (identity == null || query == null
                || !("table".equals(objectType) || "table column".equals(objectType))) {
            return; // index/view/function 等对象的 comment 不在湖跟随范围
        }
        Matcher m = COMMENT_IS.matcher(query.trim());
        if (!m.find()) {
            log.info("COMMENT 语句无法解析,跳过: {}", query);
            return;
        }
        String value = m.group(1);
        String target;
        if ("table".equals(objectType)) {
            String lakeTable = lakeTableOf(identity);
            if (lakeTable == null) {
                return;
            }
            target = "TABLE " + DuckLakeEngine.LAKE + "." + lakeTable;
        } else {
            int lastDot = identity.lastIndexOf('.');
            String col = identity.substring(lastDot + 1);
            String lakeTable = lakeTableOf(identity.substring(0, lastDot));
            if (lakeTable == null) {
                return;
            }
            target = "COLUMN " + DuckLakeEngine.LAKE + "." + lakeTable + ".\"" + col + "\"";
        }
        try (Statement s = conn.createStatement()) {
            s.execute("COMMENT ON " + target + " IS " + value);
            syncState.getDdlApplied().increment();
            log.info("湖侧注释跟随: {} IS {}", target, value.length() > 40 ? value.substring(0, 40) + "…" : value);
        } catch (SQLException e) {
            log.warn("湖侧注释应用失败(跳过,不影响数据链): {} ({})", target, e.getMessage());
        }
    }

    /**
     * followDropTable=true（默认）时跟随删湖表；sql_drop 的 object_type='table' 行给出被删表
     * （DROP TABLE 每表一行;DROP SCHEMA CASCADE 级联的每张表也各有一行,天然覆盖）。
     * 快照重放（op='r'）的历史 DROP 跳过：快照=当前态，活表不能被历史 DDL 误删。
     */
    private void applyDropTables(Connection conn, List<Struct> group,
                                 Consumer<String> cacheInvalidator) throws SQLException {
        for (Struct row : group) {
            if (!"sql_drop".equals(str(row, "ev")) || !"table".equals(str(row, "object_type"))
                    || "r".equals(str(row, "__op"))) {
                continue;
            }
            String identity = str(row, "object_identity");
            String lakeTable = identity == null ? null : lakeTableOf(identity);
            if (lakeTable == null) {
                continue;
            }
            try (Statement s = conn.createStatement()) {
                s.execute("DROP TABLE IF EXISTS " + DuckLakeEngine.LAKE + "." + lakeTable);
            }
            cacheInvalidator.accept(lakeTable);
            syncState.getDdlApplied().increment();
            log.warn("湖侧跟随删表: {}", lakeTable);
        }
    }

    /** rename 应用：列存在性检查保证幂等（快照重放旧 DDL 时安全跳过） */
    private void applyRenameIfAny(Connection conn, String objectIdentity, String query,
                                  Consumer<String> cacheInvalidator) throws SQLException {
        Matcher m = RENAME_COLUMN.matcher(query);
        if (!m.find() || objectIdentity == null) {
            return;
        }
        String oldCol = m.group(1);
        String newCol = m.group(2);
        String lakeTable = lakeTableOf(objectIdentity);
        if (lakeTable == null) {
            return;
        }
        if (columnExists(conn, lakeTable, oldCol) && !columnExists(conn, lakeTable, newCol)) {
            try (Statement s = conn.createStatement()) {
                s.execute("ALTER TABLE " + DuckLakeEngine.LAKE + "." + lakeTable
                        + " RENAME COLUMN \"" + oldCol + "\" TO \"" + newCol + "\"");
            }
            cacheInvalidator.accept(lakeTable);
            syncState.getDdlApplied().increment();
            log.warn("湖侧真 rename 应用: {} {} -> {}", lakeTable, oldCol, newCol);
        } else {
            log.info("rename 跳过(已应用或列不存在): {} {} -> {}", lakeTable, oldCol, newCol);
        }
    }

    /** followDropColumn=true（默认）时跟随删列；false 时湖保留历史列 */
    private void applyDropColumns(Connection conn, List<Struct> group,
                                  Consumer<String> cacheInvalidator) throws SQLException {
        for (Struct row : group) {
            if (!"sql_drop".equals(str(row, "ev")) || !"table column".equals(str(row, "object_type"))) {
                continue;
            }
            // object_identity 形如 public.sys_user.nickname
            String identity = str(row, "object_identity");
            if (identity == null) {
                continue;
            }
            int lastDot = identity.lastIndexOf('.');
            String col = identity.substring(lastDot + 1);
            String lakeTable = lakeTableOf(identity.substring(0, lastDot));
            if (lakeTable != null && columnExists(conn, lakeTable, col)) {
                try (Statement s = conn.createStatement()) {
                    s.execute("ALTER TABLE " + DuckLakeEngine.LAKE + "." + lakeTable + " DROP COLUMN \"" + col + "\"");
                }
                cacheInvalidator.accept(lakeTable);
                syncState.getDdlApplied().increment();
                log.warn("湖侧跟随删列: {}.{}", lakeTable, col);
            }
        }
    }

    /** public.sys_user → cdc.public_sys_user（与消费者的表命名规则一致） */
    private String lakeTableOf(String pgIdentity) {
        String[] parts = pgIdentity.split("\\.");
        if (parts.length < 2) {
            return null;
        }
        return props.getMaintenance().getCdcSchema() + "." + parts[0] + "_" + parts[1];
    }

    private boolean columnExists(Connection conn, String lakeTable, String column) throws SQLException {
        String[] parts = lakeTable.split("\\.", 2);
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT 1 FROM information_schema.columns WHERE table_catalog=? AND table_schema=? AND table_name=? AND column_name=?")) {
            ps.setString(1, DuckLakeEngine.LAKE);
            ps.setString(2, parts[0]);
            ps.setString(3, parts[1]);
            ps.setString(4, column);
            try (var rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private static String str(Struct row, String field) {
        if (row.schema().field(field) == null) {
            return null;
        }
        Object v = row.get(field);
        return v == null ? null : v.toString();
    }

    private static Long asLong(Struct row, String field) {
        if (row.schema().field(field) == null) {
            return null;
        }
        Object v = row.get(field);
        return switch (v) {
            case null -> null;
            case Number n -> n.longValue();
            default -> Long.parseLong(v.toString());
        };
    }
}
