package org.dpdns.zerodep.ducklake.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.Getter;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 同步状态（水位线 + 计数），同时是 Micrometer 指标源与 /watermark 接口的数据源。
 * <p>
 * 指标命名 ducklake_*，经 actuator prometheus 端点被 VictoriaMetrics 抓取（job=ducklake），
 * Grafana 面板与告警（DucklakeWatermarkStale 等）按这些名字引用。
 */
@Getter
@Component
public class SyncState {

    /** 最近一批中最大的源库提交时刻（epoch ms）——"湖已反映源库截至此刻的全部变更" */
    private final AtomicLong lastSourceTsMs = new AtomicLong(0);
    /** 最近一次批成功落湖的墙钟（epoch ms）——"上次全部同步成功的时间"（进程视角） */
    private final AtomicLong lastBatchAtMs = new AtomicLong(0);
    /** 引擎是否在运行（1/0，SmartLifecycle 维护） */
    private final AtomicLong engineRunning = new AtomicLong(0);

    // ---- 分段延迟（最近一批,ms）:端到端滞后 = deliver(源→交付) + stage + lakeTx + 残差(锁等待/重试) ----
    /** 源提交 → 事件交付进 handleBatch（Debezium 解码/unwrap/队列/poll 全链） */
    private final AtomicLong lastDeliverLagMs = new AtomicLong(-1);
    /** 阶段一:切段 + Appender 物化 staging */
    private final AtomicLong lastStageMs = new AtomicLong(-1);
    /** 阶段二:湖事务(INSERT SELECT + DDL 应用 + COMMIT 到 catalog) */
    private final AtomicLong lastLakeTxMs = new AtomicLong(-1);
    /** 源提交 → 湖提交完成（该批端到端滞后上界,服务端真口径,替代外部探针） */
    private final AtomicLong lastBatchLagMs = new AtomicLong(-1);

    private final Counter events;
    private final Counter batches;
    private final Counter batchFailures;
    private final Counter ddlApplied;
    private final Counter ddlAudited;

    public SyncState(MeterRegistry registry) {
        Gauge.builder("ducklake_last_source_event_ts_ms", lastSourceTsMs, AtomicLong::get)
                .description("最近事件的源库提交时刻(epoch ms)").register(registry);
        Gauge.builder("ducklake_last_batch_at_ms", lastBatchAtMs, AtomicLong::get)
                .description("最近一批成功落湖的墙钟(epoch ms)").register(registry);
        Gauge.builder("ducklake_engine_running", engineRunning, AtomicLong::get)
                .description("Debezium 引擎运行状态(1/0)").register(registry);
        Gauge.builder("ducklake_last_deliver_lag_ms", lastDeliverLagMs, AtomicLong::get)
                .description("最近批:源提交→交付进 handleBatch 的滞后(ms)").register(registry);
        Gauge.builder("ducklake_last_stage_ms", lastStageMs, AtomicLong::get)
                .description("最近批:阶段一 staging 物化耗时(ms)").register(registry);
        Gauge.builder("ducklake_last_lake_tx_ms", lastLakeTxMs, AtomicLong::get)
                .description("最近批:阶段二湖事务耗时(ms)").register(registry);
        Gauge.builder("ducklake_last_batch_lag_ms", lastBatchLagMs, AtomicLong::get)
                .description("最近批:源提交→湖提交完成的端到端滞后(ms)").register(registry);
        events = Counter.builder("ducklake_events_total").description("已落湖事件数").register(registry);
        batches = Counter.builder("ducklake_batches_total").description("已提交批次数").register(registry);
        batchFailures = Counter.builder("ducklake_batch_failures_total").description("批失败次数(引擎会重试)").register(registry);
        ddlApplied = Counter.builder("ducklake_ddl_applied_total").description("已应用到湖侧的 DDL 数(rename 等)").register(registry);
        ddlAudited = Counter.builder("ducklake_ddl_audited_total").description("已入湖审计的 DDL 事件数").register(registry);
    }

    /** 心跳推进"已反映时刻"：空闲期无数据批,靠心跳声明"源库截至此刻无变更"——
     *  watermark 的 lastSourceEventTs 不被空闲拖旧;不计 events/batches */
    public void heartbeat(long heartbeatTsMs) {
        lastSourceTsMs.accumulateAndGet(heartbeatTsMs, Math::max);
    }

    /** 批成功后推进水位线并记录分段延迟（快照/无数据批的 maxSourceTsMs≤0,分段值只在有源时刻时有意义） */
    public void batchCommitted(int eventCount, long maxSourceTsMs, long deliverLagMs, long stageMs, long lakeTxMs) {
        events.increment(eventCount);
        batches.increment();
        long now = System.currentTimeMillis();
        if (maxSourceTsMs > 0) {
            lastSourceTsMs.accumulateAndGet(maxSourceTsMs, Math::max);
            lastDeliverLagMs.set(deliverLagMs);
            lastBatchLagMs.set(now - maxSourceTsMs);
        }
        lastStageMs.set(stageMs);
        lastLakeTxMs.set(lakeTxMs);
        lastBatchAtMs.set(now);
    }
}
