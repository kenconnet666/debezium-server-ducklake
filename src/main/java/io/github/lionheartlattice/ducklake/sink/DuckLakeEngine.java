package io.github.lionheartlattice.ducklake.sink;

import io.github.lionheartlattice.ducklake.config.DucklakeProperties;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
 *   <li><b>工作连接（worker）</b>：唯一做实际读写的连接（ATTACH ducklake + USE lake + 重试参数）。
 *       CDC 消费线程与 @Scheduled 维护任务共用它，由 {@link #lock} 串行化——
 *       这就是"单写者铁律"的落地（规避 DuckLake 并发提交缺陷 duckdb/ducklake#233/#376）。</li>
 * </ul>
 * 不引入连接池：本模块只有一个写路径 + 低频维护/水位线查询，一条工作连接 + 显式锁
 * 比池更简单且从机制上杜绝并发写。将来 a_start 的联邦分析各自 ATTACH（DuckLake 多进程
 * 经 PG catalog 乐观并发协调，跨 JVM 安全）。
 */
@Slf4j
@Component
public class DuckLakeEngine {

    private static final String DUCKDB_URL = "jdbc:duckdb::memory:ducklake";
    /** ATTACH 后的湖别名，全模块 SQL 统一用 lake.<schema>.<table> 引用 */
    public static final String LAKE = "lake";

    private final DucklakeProperties props;
    /** 单写者串行锁：CDC 写入、DDL 应用、维护任务全部经它 */
    private final ReentrantLock lock = new ReentrantLock(true);

    private Connection anchor;
    private Connection worker;

    public DuckLakeEngine(DucklakeProperties props) {
        this.props = props;
    }

    @PostConstruct
    public void init() throws SQLException {
        DucklakeProperties.Lake lake = props.getLake();

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
            s.execute("SET threads=%d".formatted(lake.getThreads()));
        }
        log.info("DuckDB instance 初始化完成（扩展/SECRET/资源限额）");

        // ② 工作连接：连接级 ATTACH + 会话参数
        worker = DriverManager.getConnection(DUCKDB_URL);
        try (Statement s = worker.createStatement()) {
            // Data Inlining：小批直接写进 catalog（PG），零 Parquet 小文件；flush_inlined_data 定时落盘。
            // 该参数在部分版本以 ATTACH 选项/表属性形式提供，SET 失败仅降级为普通写盘（优化项非正确性），warn 后继续。
            if (lake.getDataInliningRowLimit() > 0) {
                try {
                    s.execute("SET ducklake_default_data_inlining_row_limit=" + lake.getDataInliningRowLimit());
                } catch (SQLException e) {
                    log.warn("Data Inlining 未启用（当前 ducklake 版本不支持该全局参数，退化为常规写盘）: {}", e.getMessage());
                }
            }
            s.execute(("ATTACH IF NOT EXISTS 'ducklake:postgres:dbname=%s host=%s port=%d user=%s password=%s' AS %s (DATA_PATH '%s')")
                    .formatted(lake.getCatalogDb(), lake.getCatalogHost(), lake.getCatalogPort(),
                            lake.getCatalogUser(), lake.getCatalogPassword(), LAKE, lake.getDataPath()));
            s.execute("USE " + LAKE);
            // DuckLake 提交冲突内建重试（PG catalog 乐观并发）；单写者下应罕见
            s.execute("SET ducklake_max_retry_count=%d".formatted(lake.getMaxRetryCount()));
            s.execute("SET ducklake_retry_wait_ms=100");
            s.execute("SET ducklake_retry_backoff=1.5");
            // 湖内 schema 就位(2026-07-08 起仅 cdc:meta.ddl_history 留档已随"纯跟随"改造裁撤)
            s.execute("CREATE SCHEMA IF NOT EXISTS " + props.getMaintenance().getCdcSchema());
        }
        // ⚠️ DuckDB JDBC 默认 autoCommit=false(违背 JDBC 惯例,部署实测):连接会隐式带着
        // 打开的事务,后续显式事务控制全乱。统一显式置 true,批写入用 JDBC 事务 API 临时关闭。
        anchor.setAutoCommit(true);
        worker.setAutoCommit(true);
        log.info("DuckLake 已 ATTACH：catalog={}@{}:{} dataPath={}",
                lake.getCatalogDb(), lake.getCatalogHost(), lake.getCatalogPort(), lake.getDataPath());
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

    /** 便捷执行（维护类单条 SQL） */
    public void execute(String sql) throws SQLException {
        withLock(conn -> {
            try (Statement s = conn.createStatement()) {
                s.execute(sql);
            }
            return null;
        });
    }

    /** 便捷单值查询（水位线等；同样走锁，避免与写事务交叠） */
    public <T> T queryScalar(String sql, Class<T> type) throws SQLException {
        return withLock(conn -> {
            try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
                return rs.next() ? rs.getObject(1, type) : null;
            }
        });
    }

    @PreDestroy
    public void close() {
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
}
