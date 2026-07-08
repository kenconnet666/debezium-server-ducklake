package io.github.lionheartlattice.ducklake.maintain;

import io.github.lionheartlattice.ducklake.config.DucklakeProperties;
import io.github.lionheartlattice.ducklake.sink.DuckLakeEngine;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 湖维护定时任务——全部是进程内 SQL CALL，零 Spark、零外挂容器。
 * <p>
 * 与 CDC 写入共用 {@link DuckLakeEngine} 的单写者锁，机制上杜绝维护与写入并发提交。
 * <p>
 * <b>分层压实（2026-07-08 换代，替代旧的"每 5 分钟无参 merge"）</b>：旧策略每 5 分钟把当天累积
 * 数据反复重写（一天最多 288 次，30 天保留窗口下中间代文件放大数千倍）。新策略按官方
 * Tiered Compaction 分四级，每个碎片一生只被重写 O(层数) 次：
 * <ul>
 *   <li>每 5 分钟：flush 内联 + <b>条件式 Tier0</b>（&lt;1MB 碎片攒够阈值才压，低流量自动 no-op，
 *       高流量保持 5 分钟级收敛）</li>
 *   <li>每小时：Tier1（1–10MB → 32MB）</li>
 *   <li>每日 04:40：Tier2（10–64MB → 128MB）+ 快照过期/物理清理/信号表 TRUNCATE</li>
 *   <li>每月 1 日 05:00：全量归并（target/max=100GB，整表收敛到极少文件）——merge 是流式
 *       读写（无 sort order 时无阻塞算子），内存峰值 ≈ row group 缓冲，与文件大小无关；
 *       即便超 memory_limit 也只是本次 CALL 报错被 catch，不伤数据链路</li>
 * </ul>
 * merge 产物是 partial data file（内嵌 _ducklake_internal_snapshot_id），时间旅行与
 * change feed 完全无损；不同 schema 版本的文件永不互并（DDL 跟随的世代分组是预期行为）。
 * <p>
 * ⚠️ 孤儿文件清理铁律：{@code ducklake_delete_orphaned_files} 出过误删活跃文件的数据丢失
 * bug（duckdb/ducklake#815），且"失败事务残留文件清不掉"仍是 open issue（#300）——
 * <b>永远 dry_run，结果只记日志供人工确认，绝不自动物理删除</b>。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class LakeMaintenanceJobs {

    private static final long MB = 1024L * 1024;
    /** Tier0 触发阈值：全湖 <1MB 活跃碎片达到该数量才压实（防低流量下产物被反复重写） */
    private static final int TIER0_MIN_FILES = 6;

    private final DuckLakeEngine engine;
    private final DucklakeProperties props;

    /** 每 5 分钟：内联数据落盘成 Parquet；碎片攒够阈值时做 Tier0 压实（阈值不满自动 no-op） */
    @Scheduled(fixedDelay = 300_000, initialDelay = 120_000)
    public void quick() {
        if (!props.getMaintenance().isEnabled()) {
            return;
        }
        call("CALL ducklake_flush_inlined_data('%s')".formatted(DuckLakeEngine.LAKE), "flush_inlined_data");
        long smallFiles = countActiveFilesUnder(MB);
        if (smallFiles >= TIER0_MIN_FILES) {
            mergeTier("Tier0(碎片→5MB)", "5MB", null, MB, 20);
        }
    }

    /** 每小时：Tier1 小文件 → 中文件 */
    @Scheduled(fixedDelay = 3_600_000, initialDelay = 900_000)
    public void hourly() {
        if (!props.getMaintenance().isEnabled()) {
            return;
        }
        mergeTier("Tier1(1-10MB→32MB)", "32MB", MB, 10 * MB, 20);
    }

    /** 每日 04:40：Tier2 压实 + 过期旧快照（保留窗口 = time travel 窗口）+ 物理清理 + 信号表防堆积 */
    @Scheduled(cron = "0 40 4 * * *")
    public void daily() {
        if (!props.getMaintenance().isEnabled()) {
            return;
        }
        mergeTier("Tier2(10-64MB→128MB)", "128MB", 10 * MB, 64 * MB, 10);
        int days = props.getMaintenance().getSnapshotRetainDays();
        call("CALL ducklake_expire_snapshots('%s', older_than => now() - INTERVAL %d DAYS)"
                .formatted(DuckLakeEngine.LAKE, days), "expire_snapshots");
        call("CALL ducklake_cleanup_old_files('%s', older_than => now() - INTERVAL 2 HOURS)"
                .formatted(DuckLakeEngine.LAKE), "cleanup_old_files");
        truncateDdlSignals();
    }

    /** 每月 1 日 05:00：全量归并——所有(含已很大的)文件收敛到极少数大文件 */
    @Scheduled(cron = "0 0 5 1 * *")
    public void monthly() {
        if (!props.getMaintenance().isEnabled()) {
            return;
        }
        monthlyFullMerge();
    }

    /** 全量归并本体（public 供集成测试跨包直接驱动）：target/max=100GB，不限分批 */
    public void monthlyFullMerge() {
        mergeTier("Tier3-monthly(全量归并→100GB)", "100GB", null, 100L * 1024 * MB, null);
    }

    /**
     * 一级压实：SET target_file_size + 参数化 merge_adjacent_files，同一锁段内顺序执行。
     * set_option 持久化在 catalog，故每级调用前都显式设置，互不残留。
     */
    private void mergeTier(String label, String targetSize, Long minBytes, Long maxBytes, Integer maxCompacted) {
        StringBuilder merge = new StringBuilder("CALL ducklake_merge_adjacent_files('")
                .append(DuckLakeEngine.LAKE).append('\'');
        if (minBytes != null) {
            merge.append(", min_file_size => ").append(minBytes);
        }
        if (maxBytes != null) {
            merge.append(", max_file_size => ").append(maxBytes);
        }
        if (maxCompacted != null) {
            merge.append(", max_compacted_files => ").append(maxCompacted);
        }
        merge.append(')');
        try {
            engine.withLock(conn -> {
                try (Statement s = conn.createStatement()) {
                    s.execute("CALL ducklake_set_option('%s', 'target_file_size', '%s')"
                            .formatted(DuckLakeEngine.LAKE, targetSize));
                    s.execute(merge.toString());
                }
                return null;
            });
            log.info("湖压实 {} 完成", label);
        } catch (SQLException e) {
            // 单级失败不影响其他维护项与数据链路(流式合并超 memory_limit 也只会走到这里)
            log.error("湖压实 {} 失败: {}", label, e.getMessage());
        }
    }

    /** 统计 catalog 中活跃(未过期)且小于给定字节数的数据文件数——Tier0 的触发条件；查询失败返回 -1(保守不触发) */
    private long countActiveFilesUnder(long maxBytes) {
        DucklakeProperties.Lake lake = props.getLake();
        String url = "jdbc:postgresql://%s:%d/%s"
                .formatted(lake.getCatalogHost(), lake.getCatalogPort(), lake.getCatalogDb());
        try (Connection c = DriverManager.getConnection(url, lake.getCatalogUser(), lake.getCatalogPassword());
             PreparedStatement ps = c.prepareStatement(
                     "SELECT count(*) FROM ducklake_data_file WHERE end_snapshot IS NULL AND file_size_bytes < ?")) {
            ps.setLong(1, maxBytes);
            try (var rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : -1;
            }
        } catch (SQLException e) {
            log.warn("碎片计数查询失败(本轮跳过 Tier0): {}", e.getMessage());
            return -1;
        }
    }

    /** 每周一 05:20：孤儿文件 dry_run 巡检（只记日志，人工确认后才手动清） */
    @Scheduled(cron = "0 20 5 * * 1")
    public void orphanDryRun() {
        if (!props.getMaintenance().isEnabled()) {
            return;
        }
        try {
            int candidates = engine.withLock(conn -> {
                int n = 0;
                try (Statement s = conn.createStatement();
                     var rs = s.executeQuery("SELECT * FROM ducklake_delete_orphaned_files('%s', dry_run => true)"
                             .formatted(DuckLakeEngine.LAKE))) {
                    while (rs.next()) {
                        log.warn("[orphan dry_run 候选] {}", rs.getString(1));
                        n++;
                    }
                }
                return n;
            });
            log.info("orphan dry_run 完成: {} 个候选文件（如需清理请人工确认后手动执行）", candidates);
        } catch (SQLException e) {
            log.error("orphan dry_run 失败: {}", e.getMessage());
        }
    }

    /**
     * 源库信号表阅后即焚：TRUNCATE 不产生复制事件（Debezium 默认 skipped.operations=t），
     * 已提交信号早在 WAL/复制流内、消费与表内容无关，任意时刻清空都安全；重快照后无历史信号
     * 也安全（rename/删列应用有列存在性幂等兜底）。⚠️ 不能用 DELETE 清——会产生墓碑事件流进
     * DdlApplier（其 __op 过滤只是兜底）。需要 GRANT TRUNCATE ON sys_ddl_log TO dbuser_cdc。
     */
    private void truncateDdlSignals() {
        DucklakeProperties.Source src = props.getSource();
        String url = "jdbc:postgresql://%s:%d/%s".formatted(src.getHostname(), src.getPort(), src.getDbname());
        try (Connection c = DriverManager.getConnection(url, src.getUser(), src.getPassword());
             Statement s = c.createStatement()) {
            for (String table : props.getMaintenance().getDdlAuditTables()) {
                s.execute("TRUNCATE TABLE " + table);
            }
            // 增量快照 signal 表同样阅后即焚(已消费的 execute-snapshot 与水位标记行无保留价值)
            s.execute("TRUNCATE TABLE " + src.getSignalTable());
            log.info("信号表已清空(防堆积): {} + {}",
                    props.getMaintenance().getDdlAuditTables(), src.getSignalTable());
        } catch (SQLException e) {
            // 单项失败不影响主链;常见原因是 TRUNCATE 未授权(见 init 脚本 GRANT)或源库瞬时不可达
            log.warn("信号表清理失败: {}", e.getMessage());
        }
    }

    private void call(String sql, String label) {
        try {
            engine.execute(sql);
            log.info("湖维护 {} 完成", label);
        } catch (SQLException e) {
            // 单项失败不影响其他维护项；持续失败会体现在日志与湖文件数上
            log.error("湖维护 {} 失败: {}", label, e.getMessage());
        }
    }
}
