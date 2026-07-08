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
        /** 内嵌 DuckDB 资源上限（与容器内存限额联动） */
        private String memoryLimit = "512MB";
        private int threads = 2;
        /** Data Inlining 行数阈值：≥ 引擎单批上限时高频小批直接落 catalog，零小文件（0=不启用） */
        private int dataInliningRowLimit = 4096;
        /** DuckLake 提交冲突内建重试（单写者下冲突罕见，防维护任务与写入偶发交叠） */
        private int maxRetryCount = 20;
    }

    /** Debezium Embedded Engine 调参 */
    @Data
    public static class Engine {
        private int maxBatchSize = 2048;
        private int maxQueueSize = 16000;
        private long pollIntervalMs = 500;
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
