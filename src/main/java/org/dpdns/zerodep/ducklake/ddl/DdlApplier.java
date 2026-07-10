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
 * （followDropColumn=false 时保留湖列留历史）；建表延迟到首批数据；快照重放旧 DDL 时
 * 靠"列存在性检查"天然幂等。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DdlApplier {

    private static final Pattern RENAME_COLUMN = Pattern.compile(
            "RENAME\\s+COLUMN\\s+\"?([\\w$]+)\"?\\s+TO\\s+\"?([\\w$]+)\"?", Pattern.CASE_INSENSITIVE);

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

        // 按源事务 xid 分组（同一条 ALTER 产生的 ddl_command_end 与 sql_drop 共享 xid）
        Map<Long, List<Struct>> byXid = new LinkedHashMap<>();
        for (Struct row : signals) {
            byXid.computeIfAbsent(asLong(row, "xid"), k -> new ArrayList<>()).add(row);
        }

        for (Map.Entry<Long, List<Struct>> group : byXid.entrySet()) {
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
                }
                // 其余（CREATE/DROP TABLE 等）：建表延迟到首批数据到达；DROP TABLE 湖表保留历史快照;
                // 类型变更由数据驱动的 ensureTable 安全放宽跟随（TypeMapper.isSafeWidening 白名单）
            }
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
