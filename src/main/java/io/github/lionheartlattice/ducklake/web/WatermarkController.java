package io.github.lionheartlattice.ducklake.web;

import io.github.lionheartlattice.core.entity.parent.ApiResult;
import io.github.lionheartlattice.ducklake.metrics.SyncState;
import io.github.lionheartlattice.ducklake.sink.DuckLakeEngine;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 同步水位线接口（需求："提供上次全部同步成功的时间"）。
 * <p>
 * 三个互补口径：
 * <ul>
 *   <li><b>lastSyncedAt</b>：DuckLake catalog 最新 snapshot 提交时刻——湖上一次成功提交的权威时间
 *       （DuckLake 每次提交即一个带时间戳的 snapshot，一条 SQL 可查，这是选 DuckLake 的红利）</li>
 *   <li><b>lastSourceEventTs</b>：已落湖事件的最大源库提交时刻——"湖已反映源库截至此刻的全部变更"</li>
 *   <li><b>lastBatchAt</b>：本进程最近一次批落湖墙钟（进程视角，重启归零）</li>
 * </ul>
 * 本模块无鉴权过滤链（core 已瘦身），该接口仅容器网内可达（compose 不发布宿主端口）。
 */
@Slf4j
@RestController
@RequestMapping("/watermark")
@RequiredArgsConstructor
public class WatermarkController {

    private final DuckLakeEngine engine;
    private final SyncState syncState;

    @GetMapping
    public ApiResult<Map<String, Object>> watermark() {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("lastSyncedAt", querySnapshotTime());
        data.put("lastSourceEventTs", toIso(syncState.getLastSourceTsMs().get()));
        data.put("lastBatchAt", toIso(syncState.getLastBatchAtMs().get()));
        data.put("engineRunning", syncState.getEngineRunning().get() == 1);
        data.put("eventsTotal", (long) syncState.getEvents().count());
        data.put("batchesTotal", (long) syncState.getBatches().count());
        data.put("ddlAuditedTotal", (long) syncState.getDdlAudited().count());
        return ApiResult.success(data);
    }

    /** DuckLake catalog 最新 snapshot 时刻（湖侧权威水位线） */
    private String querySnapshotTime() {
        try {
            OffsetDateTime ts = engine.queryScalar(
                    "SELECT max(snapshot_time) FROM %s.snapshots()".formatted(DuckLakeEngine.LAKE),
                    OffsetDateTime.class);
            return ts == null ? null : ts.toString();
        } catch (Exception e) {
            log.warn("查询 snapshot 水位线失败: {}", e.getMessage());
            return null;
        }
    }

    private static String toIso(long epochMs) {
        return epochMs <= 0 ? null : Instant.ofEpochMilli(epochMs).toString();
    }
}
