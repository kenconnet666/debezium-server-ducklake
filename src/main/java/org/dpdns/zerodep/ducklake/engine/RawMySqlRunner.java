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
        RawMySqlOffset offset = new RawMySqlOffset(
                catalogUrl, lake.getCatalogUser(), lake.getCatalogPassword());
        String sourceName = src.getName();
        RawMySqlOffset.Position start = offset.load(sourceName);
        bootstrap.evaluateForRawMySql(start == null);

        reader = new RawMySqlReader(props, engine, ddlApplier, syncState, offset, sourceName);
        start = reader.prepare(start);
        RawMySqlOffset.Position preparedStart = start;

        running = true;
        syncState.getEngineRunning().set(1);
        executor = Executors.newSingleThreadExecutor(r -> new Thread(r, "raw-mysql-reader"));
        executor.execute(() -> {
            try {
                reader.run(preparedStart);
                if (running && !reader.isStopped()) {
                    throw new IllegalStateException("BinaryLogClient 非预期断开");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.info("RawMySqlReader 已停止（interrupted）");
            } catch (Exception e) {
                syncState.getEngineRunning().set(0);
                if (running && !reader.isStopped()) {
                    log.error("RawMySqlReader 致命退出，进程自杀交给容器重启: {}", e.getMessage(), e);
                    Thread.ofPlatform().name("raw-mysql-fatal-exit").start(() -> {
                        // Testcontainers/优雅停机可能先断源连接、随后才关闭 Spring context；给 stop()
                        // 足够时间把 reader 标成 stopped，避免正常退出被误判成生产断链。
                        try { Thread.sleep(3000); } catch (InterruptedException ignored) { }
                        if (!reader.isStopped()) System.exit(1);
                    });
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
