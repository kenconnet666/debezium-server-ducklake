package org.dpdns.zerodep.ducklake.engine;

import io.debezium.embedded.Connect;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import org.dpdns.zerodep.ducklake.config.DucklakeProperties;
import org.dpdns.zerodep.ducklake.metrics.SyncState;
import org.dpdns.zerodep.ducklake.sink.DuckLakeChangeConsumer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Debezium Embedded Engine 生命周期（随 Spring 容器启停）。
 * <p>
 * 要点：
 * <ul>
 *   <li>Debezium 3.2 起 AsyncEmbeddedEngine 是唯一实现，API 不变；record.processing.* 保持默认：
 *       processing 线程数由引擎按核数/负载动态分配（SMT 转换可并行），record.processing.order=ORDERED
 *       保证按原序交付 handleBatch——转换并行 + 交付有序，与单写者铁律兼容，无需显式调参。</li>
 *   <li>offset 经 debezium-storage-jdbc 存 PG（ducklake_catalog 库，与 DuckLake catalog 同库不同表），
 *       不落本地文件；⚠️ 配置 key 是 3.2+ 的 offset.storage.jdbc.connection.* 新形式。</li>
 *   <li>PG 连接器不需要 schema history（结构随流内 Relation message 刷新）。</li>
 *   <li>unwrap SMT 在引擎侧完成扁平化：__op/__table/__lsn/__source_ts_ms/__db 追加为普通列,
 *       DELETE 以 rewrite 模式产出 __deleted='true' 的整行墓碑。</li>
 *   <li>decimal.handling.mode=precise（numeric(28) 雪花主键不容许 double 丢精度）、
 *       time.precision.mode=isostring（时间以 ISO 字符串出流，TypeMapper 映射回原生时间列）。</li>
 * </ul>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DebeziumEngineRunner implements SmartLifecycle {

    private final DucklakeProperties props;
    private final DuckLakeChangeConsumer consumer;
    private final SyncState syncState;

    private DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> engine;
    private ExecutorService executor;
    private volatile boolean running = false;

    @Override
    public void start() {
        engine = DebeziumEngine.create(Connect.class)
                .using(buildProps())
                .notifying(consumer)
                .using((success, message, error) -> {
                    syncState.getEngineRunning().set(0);
                    if (error != null && running) {
                        // 引擎致命退出而进程还活着 = 僵尸容器（restart:always 只救进程退出）。
                        // 独立线程触发 JVM 退出（引擎回调线程里直接 exit 会与 shutdown hook 等待互相卡死），
                        // Boot 优雅停机随 exit 走完，容器非零退出 → docker 重启 → 从上个 offset 续传。
                        log.error("Debezium 引擎致命退出，进程自杀交给容器重启: {}", message, error);
                        Thread.ofPlatform().name("engine-fatal-exit").start(() -> System.exit(1));
                    } else {
                        log.info("Debezium 引擎停止: {}", message);
                    }
                })
                .build();
        executor = Executors.newSingleThreadExecutor(r -> new Thread(r, "debezium-engine"));
        executor.execute(engine);
        running = true;
        syncState.getEngineRunning().set(1);
        log.info("Debezium 引擎已启动: slot={} publication={} -> DuckLake",
                props.getSource().getSlotName(), props.getSource().getPublicationName());
    }

    @Override
    public void stop() {
        running = false;
        try {
            if (engine != null) {
                engine.close(); // 优雅停：等当前批处理完并落盘 offset
            }
        } catch (IOException e) {
            log.warn("引擎关闭异常", e);
        }
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        syncState.getEngineRunning().set(0);
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    private Properties buildProps() {
        DucklakeProperties.Source src = props.getSource();
        DucklakeProperties.Lake lake = props.getLake();
        DucklakeProperties.Engine eng = props.getEngine();

        Properties p = new Properties();
        p.setProperty("name", "ducklake");
        p.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");

        // --- offset 存 PG（3.2+ 的 .connection.* 新 key；老教程的 offset.storage.jdbc.url 已弃用） ---
        p.setProperty("offset.storage", "io.debezium.storage.jdbc.offset.JdbcOffsetBackingStore");
        p.setProperty("offset.storage.jdbc.connection.url",
                "jdbc:postgresql://%s:%d/%s".formatted(lake.getCatalogHost(), lake.getCatalogPort(), lake.getCatalogDb()));
        p.setProperty("offset.storage.jdbc.connection.user", lake.getCatalogUser());
        p.setProperty("offset.storage.jdbc.connection.password", lake.getCatalogPassword());
        p.setProperty("offset.storage.jdbc.table.name", "debezium_offset_storage");
        p.setProperty("offset.flush.interval.ms", String.valueOf(eng.getOffsetFlushIntervalMs()));

        // --- PG 源（集群形态此处是 HAProxy 读写口，故障转移自动跟随） ---
        p.setProperty("database.hostname", src.getHostname());
        p.setProperty("database.port", String.valueOf(src.getPort()));
        p.setProperty("database.user", src.getUser());
        p.setProperty("database.password", src.getPassword());
        p.setProperty("database.dbname", src.getDbname());
        p.setProperty("topic.prefix", "ducklake");
        p.setProperty("plugin.name", "pgoutput");
        p.setProperty("slot.name", src.getSlotName());
        p.setProperty("publication.name", src.getPublicationName());
        p.setProperty("publication.autocreate.mode", "disabled");
        p.setProperty("schema.include.list", src.getSchemaIncludeList());
        if (!src.getTableExcludeList().isBlank()) {
            p.setProperty("table.exclude.list", src.getTableExcludeList());
        }
        p.setProperty("snapshot.mode", eng.getSnapshotMode());

        // --- 增量快照 signal(source channel):类型严格跟随的"重建+重拉"兜底经此触发,
        //     连接器也在该表写快照水位标记;signal 行内部消费不进变更流 ---
        p.setProperty("signal.enabled.channels", "source");
        p.setProperty("signal.data.collection", src.getSignalTable());

        // --- 心跳:空闲期周期确认 slot(防实例级 WAL 被冻结的 confirmed_flush_lsn 无限扣留,
        //     含本模块维护任务写 catalog 自产的 WAL);消费者按前缀识别,只确认+推水位不入湖。
        //     action query 是零流量场景的必要闭环:主动 UPSERT 心跳表造真实 WAL 事件触发 LSN flush
        //     (LSN flush mode 'connector' 只在事件处理时确认,pgoutput 服务端过滤下零事件=零确认) ---
        if (eng.getHeartbeatIntervalMs() > 0) {
            p.setProperty("heartbeat.interval.ms", String.valueOf(eng.getHeartbeatIntervalMs()));
            if (!eng.getHeartbeatActionQuery().isBlank()) {
                p.setProperty("heartbeat.action.query", eng.getHeartbeatActionQuery());
            }
        }

        // --- 攒批（Data Inlining 下小批无小文件代价，间隔可以比 Iceberg 时代更激进） ---
        p.setProperty("max.batch.size", String.valueOf(eng.getMaxBatchSize()));
        p.setProperty("max.queue.size", String.valueOf(eng.getMaxQueueSize()));
        p.setProperty("poll.interval.ms", String.valueOf(eng.getPollIntervalMs()));

        // --- SMT 并行(ChangeConsumer 模式=ParallelSmtBatchProcessor,按序收集保序):
        //     引擎默认的"弹性池"因无界队列实际恒 1 线程(3.6.0 源码实测),显式设值才真并行 ---
        int rpt = eng.getRecordProcessingThreads();
        if (rpt != 0) {
            p.setProperty("record.processing.threads", rpt < 0 ? "AVAILABLE_CORES" : String.valueOf(rpt));
        }

        // --- 类型模式（与 TypeMapper 配套，见类注释） ---
        p.setProperty("decimal.handling.mode", "precise");
        p.setProperty("time.precision.mode", "isostring");

        // --- 扁平化：__op/__table/__lsn/__source_ts_ms/__db 追加为普通列；DELETE rewrite 成 __deleted 墓碑行 ---
        p.setProperty("transforms", "unwrap");
        p.setProperty("transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState");
        p.setProperty("transforms.unwrap.add.fields", "op,table,lsn,db,source.ts_ms");
        p.setProperty("transforms.unwrap.delete.tombstone.handling.mode", "rewrite");

        return p;
    }
}
