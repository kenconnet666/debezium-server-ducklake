package org.dpdns.zerodep.ducklake.engine;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.dpdns.zerodep.ducklake.config.DucklakeProperties;
import org.dpdns.zerodep.ducklake.ddl.DdlApplier;
import org.dpdns.zerodep.ducklake.metrics.SyncState;
import org.dpdns.zerodep.ducklake.sink.DuckLakeEngine;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * raw-pg 引擎生命周期（随 Spring 容器启停，与 {@link DebeziumEngineRunner} 互斥）。
 * <p>
 * source.engine=RAW_PG 时启动；DEBEZIUM 时静默跳过（DebeziumEngineRunner 接管）。
 * 致命退出模型与 Debezium 路径一致：异常 → 进程自杀 → 容器重启 → 从上个 LSN offset 续传。
 * <p>
 * 首次接入存量：评估 {@link ScannerBootstrap}（开关 scanner-bootstrap 控制），
 * 流式启动后异步直拉历史行，与增量 anti-join 收敛（与 DebeziumEngineRunner 同机制）。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RawPgRunner implements SmartLifecycle {

    private final DucklakeProperties props;
    private final DuckLakeEngine engine;
    private final DdlApplier ddlApplier;
    private final SyncState syncState;
    private final ScannerBootstrap bootstrap;

    private ExecutorService executor;
    private RawPgReader reader;
    private volatile boolean running = false;

    @Override
    public void start() {
        DucklakeProperties.Source src = props.getSource();
        if (src.getEngine() != DucklakeProperties.CdcEngine.RAW_PG) {
            return;
        }
        if (src.getType() != DucklakeProperties.SourceType.POSTGRES) {
            log.warn("engine=raw-pg 仅支持 source.type=POSTGRES（当前 {}），跳过启动", src.getType());
            return;
        }
        DucklakeProperties.Lake lake = props.getLake();
        String catalogUrl = "jdbc:postgresql://%s:%d/%s"
                .formatted(lake.getCatalogHost(), lake.getCatalogPort(), lake.getCatalogDb());
        RawPgOffset offset = new RawPgOffset(catalogUrl, lake.getCatalogUser(), lake.getCatalogPassword());

        // 评估首次接入的存量 scanner 化（首次 lsn=0 或崩溃续跑有 pending 行时接管）
        long startLsn = offset.load(src.getSlotName());
        bootstrap.evaluateForRawPg(startLsn);

        reader = new RawPgReader(props, engine, ddlApplier, syncState, offset);

        // running=true 在 executor 启动前设置，防止线程启动后立即报错时 if(running) 误判
        running = true;
        syncState.getEngineRunning().set(1);
        executor = Executors.newSingleThreadExecutor(r -> new Thread(r, "raw-pg-reader"));
        executor.execute(() -> {
            try {
                reader.run();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.info("RawPgReader 已停止（interrupted）");
            } catch (Exception e) {
                syncState.getEngineRunning().set(0);
                if (running && !reader.isStopped()) {
                    log.error("RawPgReader 致命退出，进程自杀交给容器重启: {}", e.getMessage(), e);
                    // 短暂等待：给 Spring stop() 有机会先到（测试场景容器先停导致的误触）
                    Thread.ofPlatform().name("raw-pg-fatal-exit").start(() -> {
                        try { Thread.sleep(500); } catch (InterruptedException ignored) {}
                        if (!reader.isStopped()) System.exit(1);
                    });
                }
            }
        });
        bootstrap.runAsync(); // 流式已开始，异步直拉存量（与增量 anti-join 收敛）
        log.info("RawPgReader 已启动: slot={} publication={} -> DuckLake",
                src.getSlotName(), src.getPublicationName());
    }

    @Override
    public void stop() {
        running = false;
        if (reader != null) {
            reader.stop();
        }
        if (executor != null) {
            executor.shutdownNow();
            try {
                executor.awaitTermination(15, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        syncState.getEngineRunning().set(0);
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
