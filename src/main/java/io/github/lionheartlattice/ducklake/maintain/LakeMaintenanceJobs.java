package io.github.lionheartlattice.ducklake.maintain;

import io.github.lionheartlattice.ducklake.config.DucklakeProperties;
import io.github.lionheartlattice.ducklake.sink.DuckLakeEngine;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * 湖维护定时任务——全部是进程内 SQL CALL，零 Spark、零外挂容器
 * （替代 Iceberg 时代的 spark-compact 容器 + 青龙四级 compaction）。
 * <p>
 * 与 CDC 写入共用 {@link DuckLakeEngine} 的单写者锁，机制上杜绝维护与写入并发提交。
 * <p>
 * ⚠️ 孤儿文件清理铁律：{@code ducklake_delete_orphaned_files} 出过误删活跃文件的数据丢失
 * bug（duckdb/ducklake#815），且"失败事务残留文件清不掉"仍是 open issue（#300）——
 * <b>永远 dry_run，结果只记日志供人工确认，绝不自动物理删除</b>。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class LakeMaintenanceJobs {

    private final DuckLakeEngine engine;
    private final DucklakeProperties props;

    /** 每 5 分钟：内联数据落盘成 Parquet + 相邻小文件合并（阈值不满自动 no-op） */
    @Scheduled(fixedDelay = 300_000, initialDelay = 120_000)
    public void quick() {
        if (!props.getMaintenance().isEnabled()) {
            return;
        }
        call("CALL ducklake_flush_inlined_data('%s')".formatted(DuckLakeEngine.LAKE), "flush_inlined_data");
        call("CALL ducklake_merge_adjacent_files('%s')".formatted(DuckLakeEngine.LAKE), "merge_adjacent_files");
    }

    /** 每日 04:40：过期旧快照（保留窗口 = time travel 窗口）+ 物理清理被标记文件 */
    @Scheduled(cron = "0 40 4 * * *")
    public void daily() {
        if (!props.getMaintenance().isEnabled()) {
            return;
        }
        int days = props.getMaintenance().getSnapshotRetainDays();
        call("CALL ducklake_expire_snapshots('%s', older_than => now() - INTERVAL %d DAYS)"
                .formatted(DuckLakeEngine.LAKE, days), "expire_snapshots");
        call("CALL ducklake_cleanup_old_files('%s', older_than => now() - INTERVAL 2 HOURS)"
                .formatted(DuckLakeEngine.LAKE), "cleanup_old_files");
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
