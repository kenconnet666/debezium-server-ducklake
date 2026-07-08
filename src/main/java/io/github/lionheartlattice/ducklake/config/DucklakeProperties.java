package io.github.lionheartlattice.ducklake.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * ducklake 模块配置（前缀 zadmin.ducklake，各 profile 的 ducklake.yml 提供实际值）。
 * 分四段：source=PG 逻辑复制源；lake=DuckLake 目标；engine=Debezium 引擎调参；maintenance=湖维护开关。
 */
@Data
@ConfigurationProperties(prefix = "zadmin.ducklake")
public class DucklakeProperties {

    private Source source = new Source();
    private Lake lake = new Lake();
    private Engine engine = new Engine();
    private Maintenance maintenance = new Maintenance();

    /** PG 逻辑复制源（集群形态填 HAProxy 读写口，故障转移自动跟随；单机直连） */
    @Data
    public static class Source {
        private String hostname;
        private int port = 5432;
        private String user = "dbuser_cdc";
        private String password;
        /** 逻辑复制所在库（业务库） */
        private String dbname = "postgres";
        /** 复制槽名（与现役 iceberg 链的 dbz_iceberg 槽相互独立，可并行影子期） */
        private String slotName = "dbz_ducklake";
        /** 发布名（复用既有 FOR TABLES IN SCHEMA public 发布，新表自动纳入） */
        private String publicationName = "dbz_publication";
        /** 捕获的 schema */
        private String schemaIncludeList = "public";
        /** 排除表（正则，schema.table 形式），如内部状态表 */
        private String tableExcludeList = "";
        /** Debezium 增量快照 signal 表（source channel；类型严格跟随的"重建+重拉"兜底经由它触发，
         *  连接器的快照水位标记也写在此表；行由连接器内部消费不进变更流，维护任务定期 TRUNCATE） */
        private String signalTable = "public.dbz_signal";
    }

    /** DuckLake 目标（catalog=PG 库，数据=S3 Parquet） */
    @Data
    public static class Lake {
        /** DuckLake catalog 的 PG 连接（同时承载 Debezium offset 表） */
        private String catalogHost;
        private int catalogPort = 5432;
        private String catalogDb = "ducklake_catalog";
        private String catalogUser;
        private String catalogPassword;
        /** 数据文件根路径（S3） */
        private String dataPath = "s3://lake/ducklake/";
        /** S3（rustfs）端点：容器网内 zrustfs:9000；path-style 必须 */
        private String s3Endpoint;
        private String s3AccessKey;
        private String s3SecretKey;
        private boolean s3Ssl = false;
        /** 内嵌 DuckDB 资源上限（与容器内存限额联动：容器 3072MB = JVM ~700 + 本值 + 余量。
         *  ⚠️ 512/768MB 均实测 OOM crash loop（大批重放 + DuckDB 常驻占用顶格,反复
         *  could not allocate 76.5MiB;76.5MiB≈120 万行 inlined 存量单列物化尺寸,
         *  常驻 ~700MB 成因仍在观察——本值须给足"常驻 + 批工作集"两份） */
        private String memoryLimit = "1536MB";
        /** DuckDB 引擎并行线程数;<=0 = 不 SET,用引擎默认(自动=可用核数)。
         *  ⚠️ 并行度×每线程写出缓冲(per_thread_output 下 row group 列缓冲+S3 上传 buffer)是乘法:
         *  8 线程(自动)在 1.75G 容器实测把 768MB 也吃满(732.4/732.4)持续 OOM——自动核数需
         *  容器内存预算 ≥2.5G 才可用;默认 2 = 已验证稳定的并行度 */
        private int threads = 2;
        /** Data Inlining 行数阈值。⚠️ 只对**小批**有利：bench 实测 8192 行批 inlined 提交比
         *  parquet 直写慢 60×（0.355 vs 0.006 ms/行——inlined 是 catalog PG 行式写），
         *  官方"插入 5×"仅指单行/几十行免小文件的场景；平衡点 ≈ 每批固定成本差(约 40ms)/行成本差
         *  ≈ 百行级。512：零星/小流免小文件（碎文件由 Tier0 压实兜底），大批走 parquet 向量化直写。
         *  0=不启用 */
        private int dataInliningRowLimit = 512;
        /** DuckLake 提交冲突内建重试（单写者下冲突罕见，防维护任务与写入偶发交叠） */
        private int maxRetryCount = 20;
    }

    /** Debezium Embedded Engine 调参 */
    @Data
    public static class Engine {
        /** 连续消费模型：批串行处理，上一批写湖期间到达的事件在队列自然堆积成下一批——
         *  批大小自动 = 单批写入耗时内的流量(负反馈自稳,自动抗大流量)。上限放大让高流量时
         *  "自然批"能长到真实尺寸(更少湖事务/snapshot);毒丸批重放粒度随之变大,可接受 */
        private int maxBatchSize = 8192;
        private int maxQueueSize = 32768;
        /** 队列空后发现新事件的唤醒间隔——连续消费模型的唯一延迟参数,仅空闲时燃烧
         *  (持续有流时 poll 立即返回,不经过此参数)。空闲首条端到端延迟 ≈ 本值 + 单批写入耗时;
         *  空闲期仅一个 condition await,调低近乎零成本 */
        private long pollIntervalMs = 10;
        private long offsetFlushIntervalMs = 10_000;
        /** initial=首启全量快照后转流式；no_data=只流式不快照 */
        private String snapshotMode = "initial";
        /** 批写失败重试的退避基数(实际等待 = 基数 << 重试次数;测试可调小) */
        private long retrySleepBaseMs = 2000;
    }

    /** 湖维护（全部为进程内 SQL CALL；孤儿清理永远 dry_run，人工确认后才手动清） */
    @Data
    public static class Maintenance {
        private boolean enabled = true;
        /** 快照保留窗口（time travel 窗口），过期后物理清理 */
        private int snapshotRetainDays = 30;
        /** DDL 信号流里跟随删除湖列（默认 true=跟随真删；false=保留历史列，新行 NULL） */
        private boolean followDropColumn = true;
        /** 数据驱动的类型严格跟随（源库 ALTER COLUMN TYPE 后湖列类型严格对齐，逐级执行：
         *  ALTER 直改 → 湖内整表 CAST 重写旧数据 → 删表重建并经 signal 增量快照重拉；
         *  关闭则类型漂移仅告警，湖列保守不动） */
        private boolean followTypeChange = true;
        /** 湖内 CDC 数据 schema */
        private String cdcSchema = "cdc";
        /** DDL 信号源表（PG 侧 event trigger 写入，随 publication 复制过来；阅后即焚，
         *  由维护任务每日 TRUNCATE 防堆积——TRUNCATE 不产生复制事件） */
        private List<String> ddlAuditTables = List.of("sys_ddl_log");
    }
}
