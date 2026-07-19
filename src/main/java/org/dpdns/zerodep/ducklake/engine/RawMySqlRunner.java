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

/** MySQL 原生 CDC 生命周期：BinaryLogClient 直读 ROW binlog。 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RawMySqlRunner implements SmartLifecycle {

    private final DucklakeProperties props;
    private final DuckLakeEngine engine;
    private final DdlApplier ddlApplier;
    private final SyncState syncState;
    private final ScannerBootstrap bootstrap;

    private ExecutorService executor;
    private RawMySqlReader reader;
    private RawMySqlOffset offset;
    private volatile boolean running;

    @Override
    public void start() {
        DucklakeProperties.Source src = props.getSource();
        if (src.getType() != DucklakeProperties.SourceType.MYSQL) {
            return;
        }
        DucklakeProperties.Lake lake = props.getLake();
        String catalogUrl = "jdbc:postgresql://%s:%d/%s"
                .formatted(lake.getCatalogHost(), lake.getCatalogPort(), lake.getCatalogDb());
        offset = new RawMySqlOffset(
                catalogUrl, lake.getCatalogUser(), lake.getCatalogPassword());
        String sourceName = src.getName();
        RawMySqlOffset.Position start;
        try {
            start = offset.load(sourceName);
            bootstrap.evaluateForRawMySql(start == null);
            RawMySqlReader activeReader = new RawMySqlReader(
                    props, engine, ddlApplier, syncState, offset, sourceName);
            reader = activeReader;
            start = activeReader.prepare(start);
        } catch (RuntimeException | Error e) {
            closeOffset();
            throw e;
        }
        RawMySqlOffset.Position preparedStart = start;
        RawMySqlReader activeReader = reader;

        running = true;
        syncState.getEngineRunning().set(1);
        executor = Executors.newSingleThreadExecutor(r -> new Thread(r, "raw-mysql-reader"));
        executor.execute(() -> {
            try {
                activeReader.run(preparedStart);
                if (running && !activeReader.isStopped()) {
                    throw new IllegalStateException("BinaryLogClient 非预期断开");
                }
            } catch (Throwable e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                syncState.getEngineRunning().set(0);
                if (running && !activeReader.isStopped()) {
                    log.error("RawMySqlReader 致命退出，进程自杀交给容器重启: {}", e.getMessage(), e);
                    Thread.ofPlatform().name("raw-mysql-fatal-exit").start(() -> {
                        // Testcontainers/优雅停机可能先断源连接、随后才关闭 Spring context；给 stop()
                        // 足够时间把 reader 标成 stopped，避免正常退出被误判成生产断链。
                        try { Thread.sleep(3000); } catch (InterruptedException ignored) { }
                        if (!activeReader.isStopped()) System.exit(1);
                    });
                } else if (e instanceof InterruptedException) {
                    log.info("RawMySqlReader 已停止（interrupted）");
                }
            }
        });
        bootstrap.runAsync();
        log.info("RawMySQL 已启动: serverId={} start={}:{} gtid={} -> DuckLake",
                src.getServerId(), start.filename(), start.position(),
                start.gtidSet().isBlank() ? "disabled" : "enabled");
    }

    @Override
    public void stop() {
        running = false;
        if (reader != null) reader.stop();
        if (executor != null) {
            executor.shutdownNow();
            try {
                if (!executor.awaitTermination(15, TimeUnit.SECONDS)) {
                    log.warn("RawMySqlReader 停止超时，线程仍未退出");
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
