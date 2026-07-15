package org.dpdns.zerodep.ducklake.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * ducklake 模块配置（前缀 ducklake，各 profile 的 ducklake.yml 提供实际值）。
 * 分四段：source=CDC 源库（PostgreSQL / MySQL）；lake=DuckLake 目标；engine=Debezium 引擎调参；
 * maintenance=湖维护开关。
 */
@Data
@ConfigurationProperties(prefix = "ducklake")
public class DucklakeProperties {

    private Source source = new Source();
    private Lake lake = new Lake();
    private Engine engine = new Engine();
    private Maintenance maintenance = new Maintenance();

    /** 源库类型：解码机制与 DDL 跟随路线随之切换（见 DebeziumEngineRunner / DdlApplier） */
    public enum SourceType { POSTGRES, MYSQL }

    /** CDC 源库。PG=逻辑复制（slot+publication）；MySQL 8.0+=binlog（ROW/FULL，8.0 起默认即是）。
     *  集群形态填代理读写口（HAProxy 等），故障转移自动跟随；单机直连 */
    @Data
    public static class Source {
        /** 源库类型：postgres（默认，全向后兼容）/ mysql（8.0+，不支持 5.7） */
        private SourceType type = SourceType.POSTGRES;
        private String hostname;
        /** 端口。⚠️ 默认 5432 是 PG 习惯值——type=mysql 时务必显式配 3306（或实际端口） */
        private int port = 5432;
        private String user = "dbuser_cdc";
        private String password;
        /** 业务库：PG=逻辑复制所在库；MySQL=signal/心跳表所在的主业务库（binlog 是实例级，
         *  捕获范围由 schema-include-list 决定，此值不限定捕获） */
        private String dbname = "postgres";
        /** [仅 PG] 复制槽名（一个实例一个槽；与源库上其他消费链的槽相互独立，可并行共存） */
        private String slotName = "dbz_ducklake";
        /** [仅 PG] 发布名（init 脚本建 FOR ALL TABLES 发布：整库所有 schema 的表、含新建表自动纳入） */
        private String publicationName = "dbz_publication";
        /** [仅 MySQL] binlog 客户端 ID（database.server.id）：以"另一台 replica"身份接入，
         *  必须在整个 MySQL 集群的 server_id/replica id 空间内唯一；多实例部署各配不同值 */
        private long serverId = 6400;
        /** [仅 MySQL] 会话时区透传（driver.connectionTimeZone）。空=Debezium 自动从服务端查询
         *  （默认行为，通常无需配置）；服务端 time_zone=SYSTEM 且 JVM 时区不一致时显式指定 */
        private String connectionTimeZone = "";
        /** 捕获范围（PG 下发 schema.include.list；MySQL 下发 database.include.list——湖 schema
         *  一一镜像 PG schema / MySQL database，语义完全对应）。默认空 = 不限制：整库全部用户
         *  schema/库自动捕获（系统 schema/库由 Debezium 内建排除），首启 initial 快照拉全部存量
         *  + 之后流式增量。需收窄时填逗号分隔列表（正则） */
        private String schemaIncludeList = "";
        /** 排除表（正则，PG schema.table / MySQL db.table 形式），如内部状态表 */
        private String tableExcludeList = "";
        /** Debezium 增量快照 signal 表（source channel；类型严格跟随的"重建+重拉"兜底经由它触发，
         *  连接器的快照水位标记也写在此表；行由连接器内部消费不进变更流，维护任务定期 TRUNCATE）。
         *  空 = 按类型推导默认：PG "public.dbz_signal"；MySQL "<dbname>.dbz_signal" */
        private String signalTable = "";

        /** 直连源库的 JDBC URL（signal 写入 / 信号表维护清理用；按 type 出方言） */
        public String jdbcUrl() {
            return switch (type) {
                case POSTGRES -> "jdbc:postgresql://%s:%d/%s".formatted(hostname, port, dbname);
                case MYSQL -> "jdbc:mysql://%s:%d/%s".formatted(hostname, port, dbname);
            };
        }

        /** signal 表全名（显式配置优先；空则按类型推导，PG 兼容旧默认 public.dbz_signal） */
        public String resolvedSignalTable() {
            if (!signalTable.isBlank()) {
                return signalTable;
            }
            return type == SourceType.MYSQL ? dbname + ".dbz_signal" : "public.dbz_signal";
        }
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
        /** S3 端点（rustfs/MinIO 等，容器网内如 rustfs:9000）；path-style 必须 */
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
        /** 心跳间隔(ms)：空闲期让连接器周期生成心跳事件（topic 前缀 __debezium-heartbeat），
         *  借此向 PG 确认 slot 位置。没有它，publication 内表空闲时 confirmed_flush_lsn 冻结，
         *  而 WAL 是实例级——其他库(含本模块维护任务写 catalog 自产的)的 WAL 被 slot 无限扣留
         *  （2026-07-10 实测：业务表空闲数小时即扣 2.6MB，无上限累积）。
         *  消费者按前缀识别心跳，只确认 offset+推水位、不写湖。0=禁用。
         *  ⚠️ 单独配本值只覆盖"低流量"：pgoutput 服务端过滤下，publication 零事件时 walsender
         *  不发任何消息、心跳无从挂靠（LSN flush mode 'connector' 只在事件处理时确认，实测
         *  confirmed_flush_lsn 仍冻结）——零流量场景必须配 heartbeatActionQuery */
        private long heartbeatIntervalMs = 60_000;
        /** 心跳动作 SQL（Debezium 官方防"零流量 WAL 无限扣留"的完整方案）：连接器每个心跳周期
         *  以 source.user 身份对源库执行本 SQL——UPSERT 心跳表产生真实 WAL 事件流回连接器，
         *  触发 LSN flush。空=禁用。前置：心跳表存在且在 publication 内、source.user 有写权限
         *  （见 test yml 配置与建表 SQL）；心跳表变更作为普通表落湖（单行 upsert，每天 1440 行
         *  append，量可忽略且让 lastBatchAt 监控口径"永远有活"不误报） */
        private String heartbeatActionQuery = "";
        /** initial=首启全量快照后转流式；no_data=只流式不快照 */
        private String snapshotMode = "initial";
        /** 批写失败重试的退避基数(实际等待 = 基数 << 重试次数;测试可调小) */
        private long retrySleepBaseMs = 2000;
        /** 诊断开关:handleBatch 空转(只确认 offset、不写湖)——测 Debezium 纯解码+交付吞吐天花板,
         *  区分"吞吐上限是解码瓶颈(no-op 也就这么快)还是写侧瓶颈(no-op 快很多)"。
         *  ⚠️ 生产必须 false:开启则数据不落湖、offset 照推进=永久丢数据,仅压测诊断用 */
        private boolean dryRun = false;
        /** SMT 并行线程数(AsyncEmbeddedEngine record.processing.threads):ChangeConsumer 模式下
         *  引擎用 ParallelSmtBatchProcessor 把每条记录的 SMT(unwrap 扁平化/墓碑 rewrite)转换
         *  提交线程池并行执行,再按提交序收集——<b>交付顺序不变,保序语义零影响</b>。
         *  ⚠️ Debezium 3.6 默认(不设)= ThreadPoolExecutor(core=0, max=核数, 无界 LinkedBlockingQueue)
         *  ——无界队列下超 core 的线程永不创建,<b>实际恒 1 线程</b>(官方 javadoc 称
         *  newCachedThreadPool,与实现不符,3.6.0 源码实测);显式设置才走 newFixedThreadPool 真并行。
         *  -1=AVAILABLE_CORES(默认,真并行到核数);>0=固定 N 线程;0=沿用引擎默认(事实单线程)。
         *  注:并行的只是 SMT 段,连接器 WAL 解码(pgoutput 解析→Struct 构建)固有单线程,是另一半地板 */
        private int recordProcessingThreads = -1;
    }

    /** 湖维护（全部为进程内 SQL CALL；孤儿清理永远 dry_run，人工确认后才手动清） */
    @Data
    public static class Maintenance {
        private boolean enabled = true;
        /** 每日维护链(全量归并→快照过期→物理清理→信号表清空)的 cron,默认凌晨 04:40。
         *  被 LakeMaintenanceJobs 的 @Scheduled 以 placeholder 直接引用;每日全量归并的
         *  写放大账见该类注释,存量大后可调稀(如每周)并恢复分层 */
        private String dailyCron = "0 40 4 * * *";
        /** 快照保留窗口（time travel 窗口），过期后物理清理 */
        private int snapshotRetainDays = 30;
        /** DDL 信号流里跟随删除湖列（默认 true=跟随真删；false=保留历史列，新行 NULL） */
        private boolean followDropColumn = true;
        /** 源 TRUNCATE TABLE 跟随清空湖表（两源通用，默认 true=镜像语义）：放行 op=t 事件
         *  （PG 的 FOR ALL TABLES publication 默认 publish truncate；MySQL binlog 原生支持），
         *  经 TruncateRescueTransform 穿过 unwrap 的硬编码丢弃后按段应用 */
        private boolean followTruncate = true;
        /** 新建湖表自动 SET SORTED BY (主键) + sort_on_insert=false（DuckLake 无索引，
         *  排序聚簇是查询加速正道，见 README 查询优化节）：写入热路径不排序零成本，
         *  分层压实时自动按主键重排——min/max 统计变紧，主键/范围过滤的文件剪枝立竿见影。
         *  仅对启用后新建的湖表生效；存量表可手动 ALTER TABLE ... SET SORTED BY 补挂 */
        private boolean sortedByPk = true;
        /** 源 JSON/jsonb 列映射 DuckLake VARIANT（默认 true）：子字段 shredding 统计参与
         *  文件剪枝、查询免运行时 JSON 解析（payload->>'k' 直取）。代价：PG catalog 下
         *  VARIANT 表暂不参与 Data Inlining（DuckLake 1.0 限制，v1.1 拟解除）——小批直落
         *  Parquet 小文件，由分层压实吸收。false=沿用 JSON（物理文本）列型；
         *  存量湖表 JSON 列由类型漂移检测自动 CAST 重写迁移 */
        private boolean jsonAsVariant = true;
        /** DDL 信号流里跟随源库 DROP TABLE 真删湖表（默认 true=镜像语义；false=湖表保留。
         *  真删后时间旅行窗口内旧 snapshot 仍可 AT (TIMESTAMP ...) 回看该表） */
        private boolean followDropTable = true;
        /** 数据驱动的类型严格跟随（源库 ALTER COLUMN TYPE 后湖列类型严格对齐，逐级执行：
         *  ALTER 直改 → 湖内整表 CAST 重写旧数据 → 删表重建并经 signal 增量快照重拉；
         *  关闭则类型漂移仅告警，湖列保守不动） */
        private boolean followTypeChange = true;
        /** 重建重灌走 scanner 直拉（默认 true）：湖表重建后历史当前态由 DuckDB 的
         *  postgres/mysql scanner 扩展直连源库 SELECT 灌入（引擎启动时按 source.type 自动
         *  INSTALL/ATTACH 只读源；服务器实测 100 万行 PG 1.2s / MySQL 4.0s，比 Debezium
         *  快照重灌快 36×/11×，且不停流）。源不可达/扩展装不上时自动降级 signal 快照老路
         *  （blocking/incremental）。false=一律走 signal 快照 */
        private boolean scannerRefill = true;
        /** 首次接入的存量数据也走 scanner 直拉（默认 true，需 scannerRefill 同时开启）：
         *  首启(offset 为空)把快照降为 no_data（秒级拿位点，流式即刻开始），存量由
         *  ScannerBootstrap 异步逐表建表+直灌（anti-join 与并行增量收敛）；无主键/跨库表
         *  自动转 signal blocking 快照兜底；进度存 catalog 的 ducklake_bootstrap 表，
         *  崩溃重启续跑。false=首次接入走 Debezium initial 快照老路 */
        private boolean scannerBootstrap = true;
        /** 湖 schema 前缀：湖 schema = 前缀 + 源 PG schema（表名原样），默认空 = 纯镜像
         *  （源 public.demo → 湖 lake.public.demo，查询语感与主库一致）。
         *  配置前缀（如 "my_"）→ 湖 lake.my_public.demo——多个源库实例共享同一湖 catalog 时
         *  以不同前缀隔离命名空间（单写者约束是表级的，schema 不同天然不冲突）。
         *  仅允许小写字母/数字/下划线（启动时 fail-fast 校验）——保证湖 schema 是普通
         *  合法标识符，查询无需引号 */
        private String schemaPrefix = "";
        /** DDL 信号源表（PG 侧 event trigger 写入，随 publication 复制过来；阅后即焚，
         *  由维护任务每日 TRUNCATE 防堆积——TRUNCATE 不产生复制事件） */
        private List<String> ddlAuditTables = List.of("dbz_ddl_log");
    }
}
