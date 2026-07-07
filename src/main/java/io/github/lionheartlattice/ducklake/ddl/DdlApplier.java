package io.github.lionheartlattice.ducklake.ddl;

import io.github.lionheartlattice.ducklake.config.DucklakeProperties;
import io.github.lionheartlattice.ducklake.metrics.SyncState;
import io.github.lionheartlattice.ducklake.sink.DuckLakeEngine;
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
 * DDL 审计流消费：PG event trigger（ddl_command_end + sql_drop）写入 sys_ddl_log，
 * 该表随 publication 被 Debezium 当普通表抓走，到这里翻译应用到湖侧。
 * <p>
 * 判定原理（PG 目录语义，确定性非启发式）：RENAME COLUMN 是 pg_attribute 原地改名、
 * <b>不触发 sql_drop</b>；DROP+ADD 必触发。故按事务号 xid 分组后，
 * "ALTER TABLE 且同事务无 sql_drop 且语句含 RENAME COLUMN" ⇒ 真 rename，湖侧同步
 * ALTER TABLE ... RENAME COLUMN（这是数据驱动 schema 演进做不到、必须靠 DDL 流补齐的唯一动作）。
 * <p>
 * 其余 DDL 一律只进湖侧审计表（meta.ddl_history）：加列交给数据驱动 ensureTable 幂等处理；
 * 删列默认保留湖列（历史可查，followDropColumn=true 才跟删）；建表延迟到首批数据；
 * 快照重放旧 DDL 时靠"列存在性检查"天然幂等。
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
        ensureHistoryTable(conn);
        audit(conn, run);

        // 按源事务 xid 分组（同一条 ALTER 产生的 ddl_command_end 与 sql_drop 共享 xid）
        Map<Long, List<Struct>> byXid = new LinkedHashMap<>();
        for (Struct row : run) {
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
                // 其余（CREATE/DROP TABLE/类型变更等）：仅审计。建表延迟到首批数据到达;
                // 类型收窄等破坏性变更需人工处理（湖列保守不动，审计表可追溯）
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

    /** followDropColumn=true 时跟随删列（默认关闭：湖保留历史列） */
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

    /** 全部 DDL 事件原样落湖审计表（含 query_text 原文，可追溯/可重放） */
    private void audit(Connection conn, List<Struct> run) throws SQLException {
        String sql = "INSERT INTO " + DuckLakeEngine.LAKE + "." + props.getMaintenance().getMetaSchema()
                + ".ddl_history (id, ev, tag, object_type, object_identity, query_text, xid, occurred_at, lsn) "
                + "VALUES (?,?,?,?,?,?,?,?,?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (Struct row : run) {
                ps.setObject(1, asLong(row, "id"));
                ps.setString(2, str(row, "ev"));
                ps.setString(3, str(row, "tag"));
                ps.setString(4, str(row, "object_type"));
                ps.setString(5, str(row, "object_identity"));
                ps.setString(6, str(row, "query_text"));
                ps.setObject(7, asLong(row, "xid"));
                ps.setString(8, str(row, "occurred_at"));
                ps.setObject(9, asLong(row, "__lsn"));
                ps.addBatch();
            }
            ps.executeBatch();
        }
        syncState.getDdlAudited().increment(run.size());
    }

    /** 每次调用都 IF NOT EXISTS(不做"已建"缓存:批回滚会把建表一并回掉,缓存标志会脏——与消费者列缓存同教训) */
    private void ensureHistoryTable(Connection conn) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE IF NOT EXISTS " + DuckLakeEngine.LAKE + "."
                    + props.getMaintenance().getMetaSchema() + ".ddl_history ("
                    + "id BIGINT, ev VARCHAR, tag VARCHAR, object_type VARCHAR, object_identity VARCHAR, "
                    + "query_text VARCHAR, xid BIGINT, occurred_at VARCHAR, lsn BIGINT)");
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
