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
 *   <li>源类型双轨（source.type）：PG 连接器不需要 schema history（结构随流内 Relation message
 *       刷新）；MySQL binlog ROW 事件只有列值数组无列名/类型，<b>必须</b>配 schema history 重放
 *       DDL 重建结构——经 JdbcSchemaHistory 存 catalog PG，与 offset 同库不同表，容器保持无状态。</li>
 *   <li>unwrap SMT 在引擎侧完成扁平化：__op/__table/__source_ts_ms/__db 追加为普通列,
 *       DELETE 以 rewrite 模式产出 __deleted='true' 的整行墓碑；批内终态序由消费者 staging 侧
 *       的到达序号 __seq 承担（交付序=复制流序，PG/MySQL 通用，不再依赖 PG 特有的 lsn）。
 *       经 TopicNameMatches predicate 只对三段式数据 topic 应用——schema change（一段式）、
 *       心跳、transaction 元消息原样透传（官方建议的选择性应用）。</li>
 *   <li>decimal.handling.mode=precise（numeric(28) 雪花主键不容许 double 丢精度）、
 *       time.precision.mode=isostring（时间以 ISO 字符串出流，TypeMapper 映射回原生时间列；
 *       两连接器共享该模式枚举）。</li>
 * </ul>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DebeziumEngineRunner implements SmartLifecycle {

    private final DucklakeProperties props;
    private final DuckLakeChangeConsumer consumer;
    private final SyncState syncState;
    private final ScannerBootstrap bootstrap;

    private DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> engine;
    private ExecutorService executor;
    private volatile boolean running = false;

    @Override
    public void start() {
        // 首次接入的存量 scanner 化评估(须在 buildProps 前:决定 snapshot.mode 是否改 no_data)
        bootstrap.evaluate();
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
        bootstrap.runAsync(); // 接管时异步直拉存量(流式已在跑,一表一锁段交错)
        DucklakeProperties.Source src = props.getSource();
        switch (src.getType()) {
            case POSTGRES -> log.info("Debezium 引擎已启动: postgres slot={} publication={} -> DuckLake",
                    src.getSlotName(), src.getPublicationName());
            case MYSQL -> log.info("Debezium 引擎已启动: mysql serverId={} databases={} -> DuckLake",
                    src.getServerId(), src.getSchemaIncludeList().isBlank() ? "<all>" : src.getSchemaIncludeList());
        }
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

        // --- offset 存 PG（3.2+ 的 .connection.* 新 key；老教程的 offset.storage.jdbc.url 已弃用）。
        //     catalog 恒为 PG，与源库类型无关 ---
        String catalogUrl = "jdbc:postgresql://%s:%d/%s"
                .formatted(lake.getCatalogHost(), lake.getCatalogPort(), lake.getCatalogDb());
        p.setProperty("offset.storage", "io.debezium.storage.jdbc.offset.JdbcOffsetBackingStore");
        p.setProperty("offset.storage.jdbc.connection.url", catalogUrl);
        p.setProperty("offset.storage.jdbc.connection.user", lake.getCatalogUser());
        p.setProperty("offset.storage.jdbc.connection.password", lake.getCatalogPassword());
        p.setProperty("offset.storage.jdbc.table.name", "debezium_offset_storage");
        p.setProperty("offset.flush.interval.ms", String.valueOf(eng.getOffsetFlushIntervalMs()));

        // --- 源连接（集群形态此处是代理读写口，故障转移自动跟随） ---
        p.setProperty("database.hostname", src.getHostname());
        p.setProperty("database.port", String.valueOf(src.getPort()));
        p.setProperty("database.user", src.getUser());
        p.setProperty("database.password", src.getPassword());
        p.setProperty("topic.prefix", "ducklake");
        switch (src.getType()) {
            case POSTGRES -> {
                p.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
                p.setProperty("database.dbname", src.getDbname());
                p.setProperty("plugin.name", "pgoutput");
                p.setProperty("slot.name", src.getSlotName());
                p.setProperty("publication.name", src.getPublicationName());
                p.setProperty("publication.autocreate.mode", "disabled");
                // interval 列以 ISO8601 字符串出流(默认是微秒数,单位语义丢失)——
                // TypeMapper 按 Interval 逻辑名映射回 DuckDB 原生 INTERVAL
                p.setProperty("interval.handling.mode", "string");
                // 空=不设 include(整库全部用户 schema);空串直接下发会被当成"空列表"反而全排除
                if (!src.getSchemaIncludeList().isBlank()) {
                    p.setProperty("schema.include.list", src.getSchemaIncludeList());
                }
            }
            case MYSQL -> {
                p.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector");
                // binlog 客户端以"另一台 replica"身份接入,集群内必须唯一
                p.setProperty("database.server.id", String.valueOf(src.getServerId()));
                // 湖 schema 镜像 MySQL database(与 PG schema 同位),include 语义一一对应
                if (!src.getSchemaIncludeList().isBlank()) {
                    p.setProperty("database.include.list", src.getSchemaIncludeList());
                }
                // schema history:binlog ROW 事件无列名/类型,必须重放 DDL 重建结构——存 catalog PG
                // (与 offset 同库不同表);store.only.captured.* 保持默认 false(官方推荐:
                // 日后扩大捕获范围无需重建 history)
                p.setProperty("schema.history.internal", "io.debezium.storage.jdbc.history.JdbcSchemaHistory");
                p.setProperty("schema.history.internal.jdbc.connection.url", catalogUrl);
                p.setProperty("schema.history.internal.jdbc.connection.user", lake.getCatalogUser());
                p.setProperty("schema.history.internal.jdbc.connection.password", lake.getCatalogPassword());
                // BIGINT UNSIGNED 上限 1.8e19 超 Long.MAX(9.2e18),默认 long 静默溢出为负——
                // precise 为未被 ubig 转换器覆盖场景的兜底;主路径由 ubig 以无符号文本出流,
                // TypeMapper 映射 DuckDB 原生 UBIGINT(8 字节整数,替代 16 字节 Decimal(20,0))
                p.setProperty("bigint.unsigned.handling.mode", "precise");
                // TINYINT(1) 快照期(SHOW CREATE TABLE)与流式期(DDL 解析)schema 不一致的官方修正:
                // 两阶段统一为 BOOLEAN,否则同列两形态会误触本项目的类型漂移重建路径
                p.setProperty("converters", "t1b,ubig");
                p.setProperty("t1b.type", "io.debezium.connector.binlog.converters.TinyIntOneToBooleanConverter");
                p.setProperty("ubig.type", "org.dpdns.zerodep.ducklake.engine.UnsignedBigintConverter");
                // zero-date('0000-00-00')等脏值转换失败降级告警置 NULL,不让引擎 crash loop
                p.setProperty("event.converting.failure.handling.mode", "warn");
                // 列/表 COMMENT 进 schema change 事件(湖侧注释跟随的前提)
                p.setProperty("include.schema.comments", "true");
                // 会话时区透传(空=Debezium 自动查询服务端时区,官方默认行为)
                if (!src.getConnectionTimeZone().isBlank()) {
                    p.setProperty("driver.connectionTimeZone", src.getConnectionTimeZone());
                }
            }
        }
        if (!src.getTableExcludeList().isBlank()) {
            p.setProperty("table.exclude.list", src.getTableExcludeList());
        }
        // 源 TRUNCATE 跟随(两源通用):默认 skipped.operations=t 会吞 op=t 事件,显式放行——
        // PG 的 FOR ALL TABLES publication 默认就 publish truncate,MySQL binlog 原生支持;
        // trescue SMT 把 op=t 改装为标记行穿过 unwrap(见下),消费者按段清空湖表
        if (props.getMaintenance().isFollowTruncate()) {
            p.setProperty("skipped.operations", "none");
        }
        // bootstrap 接管首次存量时降为 no_data:秒级拿一致位点+结构,流式即刻开始,
        // 存量由 scanner 直拉(见 ScannerBootstrap;非首次/通道未就绪时维持用户配置)
        p.setProperty("snapshot.mode", bootstrap.shouldTakeOver() ? "no_data" : eng.getSnapshotMode());

        // --- 增量快照 signal(source channel):类型严格跟随的"重建+重拉"兜底经此触发,
        //     连接器也在该表写快照水位标记;signal 行内部消费不进变更流 ---
        p.setProperty("signal.enabled.channels", "source");
        p.setProperty("signal.data.collection", src.resolvedSignalTable());

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

        // --- 扁平化：__op/__table/__db/__source_ts_ms 追加为普通列；DELETE rewrite 成 __deleted 墓碑行。
        //     批内终态序由消费者 staging 的到达序号 __seq 承担(交付序=复制流序,两源通用),
        //     不再附加 PG 特有的 lsn。trescue 在 unwrap 前把 op=t 改装成非 envelope 标记行
        //     (unwrap 对 truncate 硬编码丢弃、对非 envelope 透传——MySQL TRUNCATE 跟随的通路)。
        //     predicate 限定 unwrap 只对三段式数据 topic 应用(官方建议):
        //     schema change(一段式,MySQL DDL 跟随的输入)、心跳、transaction 元消息原样透传 ---
        p.setProperty("transforms", "trescue,unwrap");
        p.setProperty("transforms.trescue.type",
                "org.dpdns.zerodep.ducklake.engine.TruncateRescueTransform");
        p.setProperty("transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState");
        p.setProperty("transforms.unwrap.add.fields", "op,table,db,source.ts_ms");
        p.setProperty("transforms.unwrap.delete.tombstone.handling.mode", "rewrite");
        p.setProperty("transforms.unwrap.predicate", "isDataTopic");
        p.setProperty("predicates", "isDataTopic");
        p.setProperty("predicates.isDataTopic.type",
                "org.apache.kafka.connect.transforms.predicates.TopicNameMatches");
        p.setProperty("predicates.isDataTopic.pattern", "ducklake\\.[^.]+\\.[^.]+");

        return p;
    }
}
