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
 * PostgreSQL 原生 CDC 生命周期（随 Spring 容器启停）。
 * <p>
 * source.type=POSTGRES 时启动；异常 → 进程退出 → 容器重启 → 从上个已提交 LSN 续传。
 * <p>
 * 首次接入存量：评估 {@link ScannerBootstrap}（开关 scanner-bootstrap 控制），
 * 流式启动后异步直拉历史行，与增量 anti-join 收敛。
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
    private RawPgOffset offset;
    private volatile boolean running = false;

    @Override
    public void start() {
        DucklakeProperties.Source src = props.getSource();
        if (src.getType() != DucklakeProperties.SourceType.POSTGRES) {
            return;
        }
        DucklakeProperties.Lake lake = props.getLake();
        String catalogUrl = "jdbc:postgresql://%s:%d/%s"
                .formatted(lake.getCatalogHost(), lake.getCatalogPort(), lake.getCatalogDb());
        offset = new RawPgOffset(catalogUrl, lake.getCatalogUser(), lake.getCatalogPassword());

        long startLsn;
        try {
            // 评估首次接入的存量 scanner 化（首次 lsn=0 或崩溃续跑有 pending 行时接管）
            startLsn = offset.load(src.getSlotName());
            bootstrap.evaluateForRawPg(startLsn);
        } catch (RuntimeException | Error e) {
            closeOffset();
            throw e;
        }

        RawPgReader activeReader = new RawPgReader(props, engine, ddlApplier, syncState, offset);
        reader = activeReader;
        long preparedStartLsn = startLsn;

        // running=true 在 executor 启动前设置，防止线程启动后立即报错时 if(running) 误判
        running = true;
        syncState.getEngineRunning().set(1);
        executor = Executors.newSingleThreadExecutor(r -> new Thread(r, "raw-pg-reader"));
        executor.execute(() -> {
            try {
                // 必须先建立 slot/replication stream，再允许 scanner 读取源快照；否则两者之间
                // 提交的变更既不在快照里，也可能早于 slot 起点，形成不可恢复的首启缺口。
                activeReader.run(preparedStartLsn, bootstrap::runAsync);
                if (running && !activeReader.isStopped()) {
                    throw new IllegalStateException("pgoutput replication stream 非预期退出");
                }
            } catch (Throwable e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                syncState.getEngineRunning().set(0);
                if (running && !activeReader.isStopped()) {
                    log.error("RawPgReader 致命退出，进程自杀交给容器重启: {}", e.getMessage(), e);
                    // 短暂等待：给 Spring stop() 有机会先到（测试场景容器先停导致的误触）
                    Thread.ofPlatform().name("raw-pg-fatal-exit").start(() -> {
                        try { Thread.sleep(500); } catch (InterruptedException ignored) {}
                        if (!activeReader.isStopped()) System.exit(1);
                    });
                } else if (e instanceof InterruptedException) {
                    log.info("RawPgReader 已停止（interrupted）");
                }
            }
        });
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
                if (!executor.awaitTermination(15, TimeUnit.SECONDS)) {
                    log.warn("RawPgReader 停止超时，线程仍未退出");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        closeOffset();
        syncState.getEngineRunning().set(0);
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    private void closeOffset() {
        if (offset != null) {
            offset.close();
            offset = null;
        }
    }
}
