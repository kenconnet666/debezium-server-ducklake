package org.dpdns.zerodep.ducklake.sink;

import org.dpdns.zerodep.ducklake.config.DucklakeProperties;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 内嵌 DuckDB 引擎（DuckLake 唯一写入通道）。
 * <p>
 * 生命周期设计（v2 架构计划 §6.3 实测经验）：
 * <ul>
 *   <li><b>命名内存 URL</b>（jdbc:duckdb::memory:ducklake）：同 JVM 内同名共享同一 instance;
 *       数据都在 DuckLake（S3+PG catalog），本地实例只是计算载体，重启无损。</li>
 *   <li><b>锚连接（anchor）</b>：持有 instance、承载一次性 instance 级初始化
 *       （INSTALL/LOAD 扩展、S3 SECRET、memory_limit/threads），进程存活期间永不关闭——
 *       命名内存实例在全部连接关闭时被销毁，锚连接就是"别销毁"的那根钉子。</li>
 *   <li><b>工作连接（worker）</b>：唯一做实际写入的连接（ATTACH ducklake + USE lake + 重试参数）。
 *       CDC 消费线程与 @Scheduled 维护任务共用它，由 {@link #lock} 串行化——
 *       这就是"单写者铁律"的落地（规避 DuckLake 并发提交缺陷 duckdb/ducklake#233/#376）。</li>
 *   <li><b>只读连接（reader）</b>：watermark 等只读查询专用；与写连接共享全局锁，规避
 *       DuckDB 1.5.4 postgres catalog 并发失效的原生 use-after-free。</li>
 * </ul>
 * 不引入连接池：本模块查询面只有低频水位线，写入专线 + 只读单线 + 显式锁比池更简单
 * 且从机制上杜绝并发写。外部分析进程各自只读 ATTACH（DuckLake 多进程
 * 经 PG catalog 乐观并发协调，跨 JVM 安全）。
 */
@Slf4j
@Component
public class DuckLakeEngine {

    private static final String DUCKDB_URL = "jdbc:duckdb::memory:ducklake";
    /** ATTACH 后的湖别名，全模块 SQL 统一用 lake.<schema>.<table> 引用 */
    public static final String LAKE = "lake";

    /** 湖内两段名 schema.table → "schema"."table"。前缀已被启动校验约束为合法标识符
     *  （查询侧因此无需引号），此处引号化是对 PG 侧任意命名对象（引号建的大写/特殊字符
     *  表名）的防御——对普通标识符加引号语义等价、零副作用 */
    public static String quoted(String lakeTable) {
        int dot = lakeTable.indexOf('.');
        return '"' + lakeTable.substring(0, dot) + "\".\"" + lakeTable.substring(dot + 1) + '"';
    }
    /** 本地内存主 catalog。URL :memory:ducklake 的 "ducklake" 只是同 JVM 共享 instance 的
     *  缓存键，主 catalog 名恒为 memory（集成测试实测踩坑）。staging 表必须显式挂它下面：
     *  worker 连接 USE lake 后，两段名 main.x 会解析成 lake.main.x——staging 落进湖 =
     *  每批多付 3 次 catalog PG 提交 + 3 个多余 snapshot（分段指标实测 stage≈130ms 抓获） */
    public static final String MEM = "memory";

    /** MySQL scanner 直拉重灌的源库 attach 别名（READ_ONLY）。 */
    public static final String SRC = "cdc_src";

    private final DucklakeProperties props;

    /** scanner 源库直拉是否就绪（init 探活；失败时原生增量仍可运行，需重建的 DDL 会明确失败） */
    private volatile boolean scannerSourceReady;
    /** 单写者串行锁：CDC 写入、DDL 应用、维护任务全部经它 */
    private final ReentrantLock lock = new ReentrantLock(true);
    private Connection anchor;
    private Connection worker;
    /** 只读查询连接（watermark 等）。ATTACH 是 instance 级，worker 挂好的 lake 对本连接直接可见。
     *  DuckDB 1.5.4 postgres catalog scan 存在并发 cache invalidation use-after-free
     *  （duckdb-postgres#502/#506），因此本连接也必须经过 {@link #lock}。 */
    private Connection reader;

    public DuckLakeEngine(DucklakeProperties props) {
        this.props = props;
    }

    @PostConstruct
    public void init() throws SQLException {
        DucklakeProperties.Lake lake = props.getLake();

        // 湖 schema 前缀 fail-fast:限定小写字母/数字/下划线,保证湖 schema 是普通合法标识符
        // (用户查询 lake.my_public.demo 无需引号;连字符等会迫使全生态带引号引用,体验差)
        String prefix = props.getMaintenance().getSchemaPrefix();
        if (!prefix.matches("[a-z0-9_]*")) {
            throw new IllegalStateException(
                    "ducklake.maintenance.schema-prefix 仅允许小写字母/数字/下划线(如 \"my_\"),当前值: \"" + prefix + '"');
        }

        // ① 锚连接：instance 级初始化，一次生效、全 instance 共享。
        // 扩展三件套：ducklake(湖格式) + postgres(DuckLake 的 PG catalog 走它) + httpfs(S3)。
        // INSTALL 首次需访问 extensions.duckdb.org 下载,容器挂载 /root/.duckdb 持久卷后仅首次;
        // 逐条 execute(不用分号拼接,规避 JDBC 多语句边界差异)。
        anchor = DriverManager.getConnection(DUCKDB_URL);
        try (Statement s = anchor.createStatement()) {
            for (String ext : new String[]{"ducklake", "postgres", "httpfs"}) {
                s.execute("INSTALL " + ext);
                s.execute("LOAD " + ext);
            }
            s.execute("CREATE OR REPLACE SECRET lake_s3 (TYPE s3, KEY_ID '%s', SECRET '%s', ENDPOINT '%s', URL_STYLE 'path', USE_SSL %b)"
                    .formatted(lake.getS3AccessKey(), lake.getS3SecretKey(), lake.getS3Endpoint(), lake.isS3Ssl()));
            s.execute("SET memory_limit='%s'".formatted(lake.getMemoryLimit()));
            if (lake.getThreads() > 0) {
                s.execute("SET threads=%d".formatted(lake.getThreads()));
            } // <=0 不 SET:用引擎默认(自动=可用核数)
            // 不保物理插入序,降大批写出的内存重排开销(官方 OOM 三建议之一);
            // 当前态语义由 __lsn 窗口函数决定,与物理行序无关,bench 实测对吞吐中性
            s.execute("SET preserve_insertion_order=false");
        }
        log.info("DuckDB instance 初始化完成（扩展/SECRET/资源限额）");

        // ② 工作连接：连接级 ATTACH + 会话参数
        worker = DriverManager.getConnection(DUCKDB_URL);
        try (Statement s = worker.createStatement()) {
            // Data Inlining：小批直接写进 catalog（PG），零 Parquet 小文件；flush_inlined_data 定时落盘。
            // 必须连 0 也显式 SET：DuckLake 1.0 默认值非零，跳过 SET 会让“0=禁用”的配置失效。
            int inliningRowLimit = lake.getDataInliningRowLimit();
            if (inliningRowLimit < 0) {
                throw new IllegalStateException("ducklake.lake.data-inlining-row-limit 不能小于 0: "
                        + inliningRowLimit);
            }
            try {
                s.execute("SET ducklake_default_data_inlining_row_limit=" + inliningRowLimit);
            } catch (SQLException e) {
                log.warn("Data Inlining 阈值设置失败（0 也无法保证禁用；请核对当前 ducklake 版本）: {}",
                        e.getMessage());
            }
            s.execute(("ATTACH IF NOT EXISTS 'ducklake:postgres:dbname=%s host=%s port=%d user=%s password=%s' AS %s (DATA_PATH '%s')")
                    .formatted(lake.getCatalogDb(), lake.getCatalogHost(), lake.getCatalogPort(),
                            lake.getCatalogUser(), lake.getCatalogPassword(), LAKE, lake.getDataPath()));
            s.execute("USE " + LAKE);
            // DuckLake 提交冲突内建重试（PG catalog 乐观并发）；单写者下应罕见
            s.execute("SET ducklake_max_retry_count=%d".formatted(lake.getMaxRetryCount()));
            s.execute("SET ducklake_retry_wait_ms=100");
            s.execute("SET ducklake_retry_backoff=1.5");
            // ⚠️ per_thread_output 显式关闭（catalog 持久选项，曾设 true 必须显式设回）：
            // 每线程 parquet writer 的列缓冲按 row group 容量(122,880 行)预分配——宽行表
            // (TOAST/长 payload)下单 writer 即数百 MB，与 CDC 高频提交组合 = 内存无界增长，
            // 512/768/1536MB 三档 memory_limit 全部实测 OOM crash loop(2026-07-08 全案)；
            // 官方 PR#397 的 4× 收益仅针对大批扫描导入，不适配"高频小提交+宽行"的 CDC 形态
            try {
                s.execute("CALL ducklake_set_option('%s', 'per_thread_output', 'false')".formatted(LAKE));
            } catch (SQLException e) {
                log.warn("per_thread_output 关闭失败（当前 ducklake 版本无该选项则本就未启用）: {}", e.getMessage());
            }
            // ⚠️ parquet writer 的 row group 列缓冲按行数容量**预分配**（默认 122,880 行），
            // 与实际批行数无关——宽行表(TOAST 6KB)单 writer 即 ~737MB,是三档 memory_limit
            // 全炸的最底层主犯(per_thread_output 只是 ×threads 放大器)。16384 行 → 宽行场景
            // 缓冲 ~96MB 可控;CDC 批 ≤8192 行本就一文件一两个 row group,扫描粒度影响微小
            try {
                s.execute("CALL ducklake_set_option('%s', 'parquet_row_group_size', '16384')".formatted(LAKE));
            } catch (SQLException e) {
                log.warn("parquet_row_group_size 设置失败（当前版本不支持该选项,宽行大批有 OOM 风险）: {}", e.getMessage());
            }
            // 湖内 schema 不再预建:镜像模式下湖 schema = <前缀><pg_schema>,由消费者首见按需建
        }
        // ③ 只读查询连接:同 instance 第三连接,lake 已由 worker ATTACH(instance 级)直接可见。
        //    USE lake 与 worker 会话对齐——queryScalar 的调用方习惯两段名(cdc.<t>),
        //    不对齐会解析进 memory catalog(集成测试实测,与 staging 落湖是同类会话漂移坑的镜像)
        reader = DriverManager.getConnection(DUCKDB_URL);
        try (Statement s = reader.createStatement()) {
            s.execute("USE " + LAKE);
        }

        // ⚠️ DuckDB JDBC 默认 autoCommit=false(违背 JDBC 惯例,部署实测):连接会隐式带着
        // 打开的事务,后续显式事务控制全乱。统一显式置 true,批写入用 JDBC 事务 API 临时关闭。
        anchor.setAutoCommit(true);
        worker.setAutoCommit(true);
        reader.setAutoCommit(true);
        initializeScannerSource();
        log.info("DuckLake 已 ATTACH：catalog={}@{}:{} dataPath={}",
                lake.getCatalogDb(), lake.getCatalogHost(), lake.getCatalogPort(), lake.getDataPath());
    }

    /**
     * scanner-refill 开启时探活源库。PostgreSQL 使用独立 table function，不注册 attached
     * catalog；MySQL 仍需启动期只读 ATTACH。失败仅告警且不阻断原生增量读取，但需要重建
     * 重灌的 DDL 会回滚并终止 reader，避免静默产生不完整湖表。
     */
    private void initializeScannerSource() {
        if (!props.getMaintenance().isScannerRefill()) {
            return;
        }
        DucklakeProperties.Source src = props.getSource();
        try {
            if (src.getType() == DucklakeProperties.SourceType.POSTGRES) {
                // postgres_scan 是完全独立的 table function，不进入全局 catalog/cache。
                // DSN 参数化，避免凭据出现在 SQL 文本和日志中。
                try (PreparedStatement ps = anchor.prepareStatement(
                        "SELECT 1 FROM postgres_scan(?, 'pg_catalog', 'pg_class') LIMIT 1")) {
                    ps.setString(1, postgresDsn());
                    try (ResultSet rs = ps.executeQuery()) {
                        if (!rs.next()) {
                            throw new SQLException("postgres_scan 探活未返回行");
                        }
                    }
                }
            } else {
                String pwd = String.valueOf(src.getPassword()).replace("'", "''");
                try (Statement s = anchor.createStatement()) {
                    s.execute("INSTALL mysql");
                    s.execute("LOAD mysql");
                    String attach = ("ATTACH IF NOT EXISTS 'host=%s port=%d user=%s password=%s database=%s' "
                            + "AS %s (TYPE mysql, READ_ONLY)")
                            .formatted(src.getHostname(), src.getPort(), src.getUser(), pwd, src.getDbname(), SRC);
                    s.execute(attach);
                    s.execute("SELECT 1");
                }
            }
            scannerSourceReady = true;
            log.info("scanner 重灌源已就绪: {} {}:{} ({})",
                    src.getType(), src.getHostname(), src.getPort(),
                    src.getType() == DucklakeProperties.SourceType.POSTGRES
                            ? "独立 postgres_scan" : "只读 attach " + SRC);
        } catch (SQLException e) {
            scannerSourceReady = false;
            log.warn("scanner 重灌源初始化失败(原生增量仍可运行；需重建的 DDL 将失败；可关闭 ducklake.maintenance.scanner-refill 消除本告警): {}",
                    e.getMessage());
        }
    }

    /** scanner 直拉重灌通道是否就绪。 */
    public boolean isScannerSourceReady() {
        return scannerSourceReady;
    }

    /**
     * 重灌 SELECT 的安全源描述。PostgreSQL 使用 {@code postgres_scan(?, ?, ?)}，参数化
     * DSN/标识符且不注册 attached catalog；
     * MySQL：attach 绑定单 database（{@code source.dbname}）→ 同库表用两段
     * {@code cdc_src."table"}，其他库的表本通道够不到时返回 null。
     */
    public ScannerSource scannerSource(String schemaOrDb, String table) {
        if (!scannerSourceReady) {
            return null;
        }
        DucklakeProperties.Source src = props.getSource();
        return switch (src.getType()) {
            case POSTGRES -> new ScannerSource("postgres_scan(?, ?, ?)",
                    List.of(postgresDsn(), schemaOrDb, table), schemaOrDb + "." + table);
            case MYSQL -> schemaOrDb.equals(src.getDbname())
                    ? new ScannerSource(SRC + ".\"" + table + '"', List.of(), schemaOrDb + "." + table)
                    : null;
        };
    }

    private String postgresDsn() {
        DucklakeProperties.Source src = props.getSource();
        return "dbname=%s host=%s port=%d user=%s password=%s connect_timeout=5"
                .formatted(src.getDbname(), src.getHostname(), src.getPort(), src.getUser(), src.getPassword());
    }

    /** SQL 片段与绑定参数分离；{@link #toString()} 只暴露无凭据的源表名。 */
    public static final class ScannerSource {
        private final String fromSql;
        private final List<String> parameters;
        private final String displayName;

        private ScannerSource(String fromSql, List<String> parameters, String displayName) {
            this.fromSql = fromSql;
            this.parameters = List.copyOf(parameters);
            this.displayName = displayName;
        }

        public String fromSql() {
            return fromSql;
        }

        public void bind(PreparedStatement statement) throws SQLException {
            for (int i = 0; i < parameters.size(); i++) {
                statement.setString(i + 1, parameters.get(i));
            }
        }

        @Override
        public String toString() {
            return displayName;
        }
    }

    /**
     * MySQL scanner 会缓存远端表结构；源 DDL 后重灌前必须清缓存，否则 RENAME/ADD/DROP 后的
     * SELECT 仍按启动时列定义绑定。postgres scanner 不需要此步骤。
     */
    public void refreshScannerMetadata(Connection conn) throws SQLException {
        if (!scannerSourceReady || props.getSource().getType() != DucklakeProperties.SourceType.MYSQL) {
            return;
        }
        try (Statement s = conn.createStatement()) {
            s.execute("SELECT * FROM mysql_clear_cache()");
        }
        log.info("MySQL scanner 元数据缓存已刷新");
    }

    /** 在单写者锁内执行一段湖操作（CDC 批写入 / DDL 应用 / 维护 SQL 共用此入口） */
    public <T> T withLock(LakeAction<T> action) throws SQLException {
        lock.lock();
        try {
            return action.run(worker);
        } finally {
            lock.unlock();
        }
    }

    /** 与 {@link #withLock(LakeAction)} 相同，但把公平锁等待时间返回给 CDC 分层指标。 */
    public <T> LockedResult<T> withLockTimed(LakeAction<T> action) throws SQLException {
        long started = System.nanoTime();
        lock.lock();
        long waitNanos = System.nanoTime() - started;
        try {
            return new LockedResult<>(action.run(worker), waitNanos);
        } finally {
            lock.unlock();
        }
    }

    /** 便捷执行（维护类单条 SQL） */
    public void execute(String sql) throws SQLException {
        withLock(conn -> {
            try (Statement s = conn.createStatement()) {
                s.execute(sql);
            }
            return null;
        });
    }

    /** 便捷单值只读查询（水位线等）。与写路径共用全局锁：上游修复进入稳定版前，
     * 短暂排队优先于 postgres extension catalog scan 的 JVM 原生崩溃。 */
    public <T> T queryScalar(String sql, Class<T> type) throws SQLException {
        lock.lock();
        try (Statement s = reader.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getObject(1, type) : null;
        } finally {
            lock.unlock();
        }
    }

    @PreDestroy
    public void close() {
        closeQuietly(reader);
        closeQuietly(worker);
        closeQuietly(anchor);
        log.info("DuckDB 连接已关闭");
    }

    private static void closeQuietly(Connection c) {
        if (c != null) {
            try {
                c.close();
            } catch (SQLException ignored) {
            }
        }
    }

    /** 湖操作函数式接口（受检 SQLException 透传） */
    @FunctionalInterface
    public interface LakeAction<T> {
        T run(Connection conn) throws SQLException;
    }

    public record LockedResult<T>(T value, long waitNanos) {
    }
}
