package org.dpdns.zerodep.ducklake;

import org.dpdns.zerodep.ducklake.metrics.SyncState;
import org.dpdns.zerodep.ducklake.sink.DuckLakeEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.RestClient;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * {@link DucklakeApplication} 全链集成测试(Testcontainers,Docker 不可用时整类跳过)：
 * 真 PG18(wal_level=logical)+ 真 DuckLake——湖数据走本地 DATA_PATH(DuckLake 原生支持
 * 文件系统路径,免 S3 容器;CREATE SECRET 是惰性的,假 S3 端点无害)。
 * <p>
 * 验证与服务器 E2E 同口径：完整 Spring 上下文启动 → scanner 存量首灌 → 流式增量(INSERT)→
 * DDL 审计流(RENAME COLUMN 湖侧真 rename)→ /watermark 接口。
 */
@Testcontainers(disabledWithoutDocker = true)
// redis 栈已整体归属 sys(2026-07-08),本模块依赖链无 redis——无需任何排除
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DucklakeApplicationIntegrationTest {

    @Container
    static final PostgreSQLContainer PG = new PostgreSQLContainer("postgres:18-alpine")
            .withDatabaseName("postgres").withUsername("postgres").withPassword("test")
            .withCommand("postgres", "-c", "wal_level=logical");

    @TempDir
    static Path lakeDir;

    @Autowired
    DuckLakeEngine engine;
    @Autowired
    SyncState syncState;
    @Autowired
    org.dpdns.zerodep.ducklake.maintain.LakeMaintenanceJobs maintenanceJobs;
    @Autowired
    org.dpdns.zerodep.ducklake.config.DucklakeProperties props;
    @LocalServerPort
    int port;

    /** 等价于 initdb 03-cdc.sh:角色/publication/catalog 库/DDL 审计/冒烟表(在 Spring 上下文启动前跑) */
    @BeforeAll
    static void provisionSourceDatabase() throws Exception {
        try (Connection c = DriverManager.getConnection(PG.getJdbcUrl(), "postgres", "test");
             Statement s = c.createStatement()) {
            s.execute("CREATE ROLE dbuser_cdc REPLICATION LOGIN PASSWORD 'test'");
            s.execute("GRANT pg_read_all_data TO dbuser_cdc");
            s.execute("CREATE ROLE dbuser_zadmin LOGIN PASSWORD 'test'");
            s.execute("GRANT CREATE ON SCHEMA public TO dbuser_zadmin");
            s.execute("CREATE PUBLICATION dbz_publication FOR ALL TABLES");
            s.execute("CREATE DATABASE ducklake_catalog OWNER dbuser_zadmin");

            s.execute("""
                    CREATE TABLE dbz_ddl_log (
                        id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        ev text NOT NULL, tag text, object_type text, object_identity text, query_text text,
                        xid bigint NOT NULL DEFAULT (pg_current_xact_id()::text::bigint),
                        occurred_at timestamptz NOT NULL DEFAULT now())""");
            // 与 initdb/01-cdc.sh 对齐:审计全部用户 schema(排除法),不再限定 public
            s.execute("""
                    CREATE OR REPLACE FUNCTION fn_capture_ddl() RETURNS event_trigger
                    LANGUAGE plpgsql SECURITY DEFINER AS $fn$
                    DECLARE r record;
                    BEGIN
                      FOR r IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
                        IF r.schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                           AND r.schema_name NOT LIKE 'pg_temp%'
                           AND r.object_identity <> 'public.dbz_ddl_log' THEN
                          INSERT INTO public.dbz_ddl_log(ev, tag, object_type, object_identity, query_text)
                          VALUES ('ddl_command_end', r.command_tag, r.object_type, r.object_identity, current_query());
                        END IF;
                      END LOOP;
                    END $fn$""");
            s.execute("""
                    CREATE OR REPLACE FUNCTION fn_capture_drop() RETURNS event_trigger
                    LANGUAGE plpgsql SECURITY DEFINER AS $fn$
                    DECLARE r record;
                    BEGIN
                      FOR r IN SELECT * FROM pg_event_trigger_dropped_objects() LOOP
                        IF r.schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                           AND r.schema_name NOT LIKE 'pg_temp%'
                           AND r.object_type IN ('table', 'table column')
                           AND r.object_identity NOT LIKE 'public.dbz_ddl_log%' THEN
                          INSERT INTO public.dbz_ddl_log(ev, tag, object_type, object_identity, query_text)
                          VALUES ('sql_drop', tg_tag, r.object_type, r.object_identity, current_query());
                        END IF;
                      END LOOP;
                    END $fn$""");
            s.execute("CREATE EVENT TRIGGER trg_capture_ddl ON ddl_command_end "
                    + "WHEN TAG IN ('CREATE TABLE', 'ALTER TABLE', 'DROP TABLE', 'COMMENT') EXECUTE FUNCTION fn_capture_ddl()");
            s.execute("CREATE EVENT TRIGGER trg_capture_drop ON sql_drop "
                    + "WHEN TAG IN ('ALTER TABLE', 'DROP TABLE', 'DROP SCHEMA') EXECUTE FUNCTION fn_capture_drop()");

            s.execute("CREATE TABLE cdc_test (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, "
                    + "name text NOT NULL, big_content text, amount numeric(12,2) DEFAULT 0, tags text[], dur interval)");
            s.execute("INSERT INTO cdc_test (name, amount, tags, dur) VALUES "
                    + "('alpha', 10.50, ARRAY['a,b','c'], interval '1 year 2 months 3 days'), "
                    + "('beta', 20.00, NULL, NULL), ('gamma', 99.99, ARRAY[]::text[], interval '4 hours')");

            // 非 public schema:默认整库同步下应自动捕获并映射为湖表 app.orders(快照+增量)
            s.execute("CREATE SCHEMA app");
            s.execute("CREATE TABLE app.orders (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, "
                    + "sku text NOT NULL, qty int DEFAULT 1)");
            s.execute("INSERT INTO app.orders (sku, qty) VALUES ('sku-a', 2), ('sku-b', 3)");

            s.execute("GRANT TRUNCATE ON public.dbz_ddl_log TO dbuser_cdc");
        }
    }

    /** 全部连接指向 PG 容器;湖数据落 @TempDir(覆盖 dev profile 的本机地址) */
    @DynamicPropertySource
    static void wire(DynamicPropertyRegistry r) {
        r.add("ducklake.source.hostname", PG::getHost);
        r.add("ducklake.source.port", PG::getFirstMappedPort);
        r.add("ducklake.source.user", () -> "dbuser_cdc");
        r.add("ducklake.source.password", () -> "test");
        r.add("ducklake.source.dbname", () -> "postgres");

        r.add("ducklake.lake.catalog-host", PG::getHost);
        r.add("ducklake.lake.catalog-port", PG::getFirstMappedPort);
        r.add("ducklake.lake.catalog-user", () -> "dbuser_zadmin");
        r.add("ducklake.lake.catalog-password", () -> "test");
        r.add("ducklake.lake.data-path", () -> lakeDir.toString().replace('\\', '/') + "/");
        // SECRET 创建是惰性的(不连接);本测试全程 Data Inlining + 本地 DATA_PATH,S3 永不触达
        r.add("ducklake.lake.s3-endpoint", () -> "127.0.0.1:1");
        r.add("ducklake.lake.s3-access-key", () -> "dummy");
        r.add("ducklake.lake.s3-secret-key", () -> "dummy");
        r.add("ducklake.maintenance.enabled", () -> "false");
    }

    // ---------- 用例(有序:快照 → 增量 → DDL → 接口) ----------

    @Test
    @Order(1)
    void snapshotLandsInLake() {
        // scanner bootstrap 异步建 schema/表；首次轮询可能早于建表完成，此时应继续等待。
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofSeconds(1)).ignoreExceptions().untilAsserted(() -> {
            assertThat(lakeCount("SELECT count(*) FROM public.cdc_test")).isEqualTo(3L);
            // 默认整库:非 public schema 的存量随 initial 快照自动落湖,湖表名 <schema>_<表>
            assertThat(lakeCount("SELECT count(*) FROM app.orders")).isEqualTo(2L);
        });
        assertThat(syncState.getEngineRunning().get()).isEqualTo(1);
    }

    @Test
    @Order(2)
    void streamingInsertUpdateDeleteMirrorsCurrentState() throws Exception {
        // 镜像语义:UPDATE 就地更新值,DELETE 物理跟随——湖=主库当前态,无墓碑无多版本
        try (Connection c = DriverManager.getConnection(PG.getJdbcUrl(), "postgres", "test");
             Statement s = c.createStatement()) {
            s.execute("INSERT INTO cdc_test (name, amount) VALUES ('it_insert', 42.42)");
            s.execute("UPDATE cdc_test SET amount = 43.42 WHERE name = 'it_insert'");
        }
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500)).untilAsserted(() -> {
            assertThat(lakeCount("SELECT count(*) FROM public.cdc_test WHERE name='it_insert'")).isEqualTo(1L);
            assertThat(lakeCount("SELECT count(*) FROM public.cdc_test WHERE name='it_insert' AND amount=43.42")).isEqualTo(1L);
        });
        try (Connection c = DriverManager.getConnection(PG.getJdbcUrl(), "postgres", "test");
             Statement s = c.createStatement()) {
            s.execute("DELETE FROM cdc_test WHERE name = 'it_insert'");
        }
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500)).untilAsserted(() ->
                assertThat(lakeCount("SELECT count(*) FROM public.cdc_test WHERE name='it_insert'")).isZero());
        // 湖表列与源表一一对应,不含 __* 元列
        assertThat(lakeColumnType("__op")).isNull();
        assertThat(lakeColumnType("__deleted")).isNull();
    }

    @Test
    @Order(3)
    void ddlFollowsRenameDropAndStrictTypeChange() throws Exception {
        // ① RENAME COLUMN → 湖侧真 rename(rename 前旧行在新列名下可读)
        try (Connection c = DriverManager.getConnection(PG.getJdbcUrl(), "postgres", "test");
             Statement s = c.createStatement()) {
            s.execute("ALTER TABLE cdc_test RENAME COLUMN amount TO price");
            s.execute("INSERT INTO cdc_test (name, price) VALUES ('after_rename', 3.14)");
        }
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500)).untilAsserted(() -> {
            assertThat(lakeColumnType("price")).isNotNull();
            assertThat(lakeCount("SELECT count(*) FROM public.cdc_test WHERE name='after_rename'")).isEqualTo(1L);
        });

        // ② DROP COLUMN → 湖列跟随真删(followDropColumn 默认 true)
        try (Connection c = DriverManager.getConnection(PG.getJdbcUrl(), "postgres", "test");
             Statement s = c.createStatement()) {
            s.execute("ALTER TABLE cdc_test DROP COLUMN big_content");
            s.execute("INSERT INTO cdc_test (name, price) VALUES ('after_dropcol', 1.00)");
        }
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500)).untilAsserted(() -> {
            assertThat(lakeCount("SELECT count(*) FROM public.cdc_test WHERE name='after_dropcol'")).isEqualTo(1L);
            assertThat(lakeColumnType("big_content")).isNull();
        });

        // ③ 类型严格跟随:加 int 列 → 湖 INTEGER;源 ALTER 成 bigint → 湖 BIGINT,
        //    并插入超 INT32 的值(4000000000)——跟随未生效则绑定溢出批失败,测试必超时
        try (Connection c = DriverManager.getConnection(PG.getJdbcUrl(), "postgres", "test");
             Statement s = c.createStatement()) {
            s.execute("ALTER TABLE cdc_test ADD COLUMN hits int");
            s.execute("INSERT INTO cdc_test (name, price, hits) VALUES ('with_hits', 1.00, 100)");
        }
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500)).untilAsserted(() ->
                assertThat(lakeColumnType("hits")).isEqualTo("INTEGER"));
        try (Connection c = DriverManager.getConnection(PG.getJdbcUrl(), "postgres", "test");
             Statement s = c.createStatement()) {
            s.execute("ALTER TABLE cdc_test ALTER COLUMN hits TYPE bigint");
            s.execute("INSERT INTO cdc_test (name, price, hits) VALUES ('after_widen', 1.00, 4000000000)");
        }
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500)).untilAsserted(() -> {
            assertThat(lakeColumnType("hits")).isEqualTo("BIGINT");
            assertThat(lakeCount("SELECT count(*) FROM public.cdc_test WHERE hits=4000000000")).isEqualTo(1L);
        });

        // ④ COMMENT 跟随:表/列注释同步到湖(DuckLake catalog 持久化)
        try (Connection c = DriverManager.getConnection(PG.getJdbcUrl(), "postgres", "test");
             Statement s = c.createStatement()) {
            s.execute("COMMENT ON TABLE cdc_test IS '集成测试表'");
            s.execute("COMMENT ON COLUMN cdc_test.name IS '名称'");
        }
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500)).untilAsserted(() -> {
            assertThat(engine.queryScalar("SELECT comment FROM duckdb_tables() "
                    + "WHERE database_name='lake' AND schema_name='public' AND table_name='cdc_test'", String.class))
                    .isEqualTo("集成测试表");
            assertThat(engine.queryScalar("SELECT comment FROM duckdb_columns() "
                    + "WHERE database_name='lake' AND schema_name='public' AND table_name='cdc_test' AND column_name='name'", String.class))
                    .isEqualTo("名称");
        });
    }

    @Test
    @Order(4)
    void dailyFullMergeConvergesFilesWithoutDataLoss() throws Exception {
        maintenanceJobs.getClass(); // 显式引用,防未用告警
        boolean prev = propsEnabled(true);
        try {
            // 两轮 flush 制造多个 parquet 文件,再全量归并
            long rows0 = lakeCount("SELECT count(*) FROM public.cdc_test");
            maintenanceJobs.quick();
            try (Connection c = DriverManager.getConnection(PG.getJdbcUrl(), "postgres", "test");
                 Statement s = c.createStatement()) {
                s.execute("INSERT INTO cdc_test (name, price) VALUES ('merge_probe', 9.99)");
            }
            await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500)).untilAsserted(() ->
                    assertThat(lakeCount("SELECT count(*) FROM public.cdc_test WHERE name='merge_probe'")).isEqualTo(1L));
            maintenanceJobs.quick();

            long filesBefore = catalogFileCount();
            maintenanceJobs.fullMerge();
            long filesAfter = catalogFileCount();
            long rowsAfter = lakeCount("SELECT count(*) FROM public.cdc_test");

            assertThat(rowsAfter).isEqualTo(rows0 + 1);          // 行数无损
            assertThat(filesAfter).isLessThanOrEqualTo(filesBefore); // 文件收敛(或已最优)
        } finally {
            propsEnabled(prev);
        }
    }

    /** 切换 maintenance.enabled 并返回旧值(集成测试默认 false,归并用例需临时打开) */
    private boolean propsEnabled(boolean value) {
        boolean prev = props.getMaintenance().isEnabled();
        props.getMaintenance().setEnabled(value);
        return prev;
    }

    /** catalog PG 里活跃数据文件数 */
    private long catalogFileCount() throws Exception {
        try (Connection c = DriverManager.getConnection(
                PG.getJdbcUrl().replace("/postgres", "/ducklake_catalog"), "dbuser_zadmin", "test");
             Statement s = c.createStatement();
             var rs = s.executeQuery("SELECT count(*) FROM ducklake_data_file WHERE end_snapshot IS NULL")) {
            rs.next();
            return rs.getLong(1);
        }
    }

    @Test
    @Order(5)
    void watermarkEndpointReportsHealthyPipeline() {
        String body = RestClient.create("http://127.0.0.1:" + port + "/api/ducklake")
                .get().uri("/watermark").retrieve().body(String.class);
        assertThat(body)
                .contains("\"code\":\"0000\"")
                .contains("\"engineRunning\":true")
                .contains("\"lastSyncedAt\":\"2026-");
    }

    @Test
    @Order(6)
    void nonPublicSchemaStreamsAutomatically() throws Exception {
        try (Connection c = DriverManager.getConnection(PG.getJdbcUrl(), "postgres", "test");
             Statement s = c.createStatement()) {
            s.execute("INSERT INTO app.orders (sku, qty) VALUES ('sku-live', 5)");
        }
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() ->
                assertThat(lakeCount("SELECT count(*) FROM app.orders WHERE sku='sku-live'")).isEqualTo(1L));
    }

    @Test
    @Order(7)
    void addingPrimaryKeyToExistingTableRebuildsAndRefillsLake() throws Exception {
        // 用户实测场景:无主键表先积累了数据(湖 insert-only 镜像),后补主键——
        // 旧行主键回填无 CDC 事件,湖旧行主键恒 NULL;应自动重建+增量快照重灌恢复完整镜像
        try (Connection c = DriverManager.getConnection(PG.getJdbcUrl(), "postgres", "test");
             Statement s = c.createStatement()) {
            s.execute("CREATE TABLE pkfix (name text NOT NULL)");
            s.execute("INSERT INTO pkfix VALUES ('a'), ('b'), ('c')");
        }
        // reader 使用独立连接，首轮查询可能早于异步建表；该窗口只代表尚未就绪。
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500))
                .ignoreExceptions().untilAsserted(() ->
                        assertThat(lakeCount("SELECT count(*) FROM public.pkfix")).isEqualTo(3L));

        try (Connection c = DriverManager.getConnection(PG.getJdbcUrl(), "postgres", "test");
             Statement s = c.createStatement()) {
            s.execute("ALTER TABLE pkfix ADD COLUMN id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY");
        }
        // 重建+重灌后:行数不变且全部行的主键非空(旧 NULL 行已让位)。
        // ignoreExceptions:重建完成前 id 列不存在会抛 Binder Error,应视作"未就绪"继续等
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(500))
                .ignoreExceptions().untilAsserted(() -> {
            assertThat(lakeCount("SELECT count(*) FROM public.pkfix WHERE id IS NOT NULL")).isEqualTo(3L);
            assertThat(lakeCount("SELECT count(*) FROM public.pkfix")).isEqualTo(3L);
        });
        // 补主键后 DELETE 应完整跟随(修复前:湖旧行 NULL 主键永久失配,删不掉)
        try (Connection c = DriverManager.getConnection(PG.getJdbcUrl(), "postgres", "test");
             Statement s = c.createStatement()) {
            s.execute("DELETE FROM pkfix WHERE name = 'b'");
        }
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500)).untilAsserted(() ->
                assertThat(lakeCount("SELECT count(*) FROM public.pkfix")).isEqualTo(2L));
    }

    /** PG 数组 → DuckDB LIST:列型 VARCHAR[]、值往返(含逗号元素)、NULL/空数组保真 */
    @Test
    @Order(71)
    void pgArrayLandsAsDuckList() throws Exception {
        assertThat(lakeColumnType("tags")).isEqualTo("VARCHAR[]");
        assertThat(engine.queryScalar(
                "SELECT tags[1] FROM public.cdc_test WHERE name='alpha'", String.class)).isEqualTo("a,b");
        assertThat(engine.queryScalar(
                "SELECT len(tags) FROM public.cdc_test WHERE name='gamma'", Long.class)).isZero();
        assertThat(engine.queryScalar(
                "SELECT count(*) FROM public.cdc_test WHERE name='beta' AND tags IS NULL", Long.class)).isEqualTo(1L);
    }

    /** PG interval → DuckDB INTERVAL:ISO8601 文本出流,湖列原生 INTERVAL 语义可运算 */
    @Test
    @Order(74)
    void pgIntervalLandsAsNativeInterval() throws Exception {
        assertThat(lakeColumnType("dur")).isEqualTo("INTERVAL");
        assertThat(engine.queryScalar(
                "SELECT date_part('month', dur) FROM public.cdc_test WHERE name='alpha'", Long.class)).isEqualTo(2L);
        assertThat(engine.queryScalar(
                "SELECT date_part('hour', dur) FROM public.cdc_test WHERE name='gamma'", Long.class)).isEqualTo(4L);
    }

    /** DDL 驱动建空表 + 表改名跟随(PG 增量 DDL 生效;存量空表 PG 无事件覆盖不到,见 README) */
    @Test
    @Order(72)
    void emptyTableCreateAndRenameFollow() throws Exception {
        try (Connection c = DriverManager.getConnection(PG.getJdbcUrl(), "postgres", "test");
             Statement s = c.createStatement()) {
            s.execute("CREATE TABLE empty_probe (id bigint PRIMARY KEY, v text)");
        }
        // 不插任何数据:湖空表应由 DDL 审计流直接建出
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500))
                .ignoreExceptions().untilAsserted(() ->
                assertThat(engine.queryScalar("SELECT count(*) FROM public.empty_probe", Long.class)).isZero());

        try (Connection c = DriverManager.getConnection(PG.getJdbcUrl(), "postgres", "test");
             Statement s = c.createStatement()) {
            s.execute("ALTER TABLE empty_probe RENAME TO renamed_probe");
        }
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500))
                .ignoreExceptions().untilAsserted(() -> {
            assertThat(engine.queryScalar("SELECT count(*) FROM public.renamed_probe", Long.class)).isZero();
            assertThat(engine.queryScalar("SELECT count(*) FROM information_schema.tables "
                    + "WHERE table_catalog='lake' AND table_schema='public' AND table_name='empty_probe'",
                    Long.class)).isZero();
        });

        // ADD COLUMN DDL 驱动:不插任何数据,湖列应即刻出现(连源库读目标态对齐)
        try (Connection c = DriverManager.getConnection(PG.getJdbcUrl(), "postgres", "test");
             Statement s = c.createStatement()) {
            s.execute("ALTER TABLE renamed_probe ADD COLUMN note text");
        }
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500))
                .ignoreExceptions().untilAsserted(() ->
                assertThat(engine.queryScalar("SELECT count(*) FROM information_schema.columns "
                        + "WHERE table_catalog='lake' AND table_schema='public' "
                        + "AND table_name='renamed_probe' AND column_name='note'", Long.class)).isEqualTo(1L));
    }

    /** 源 TRUNCATE 跟随：PG 直读 pgoutput Truncate，MySQL 直读 QueryEvent。 */
    @Test
    @Order(73)
    void truncateFollowsToLake() throws Exception {
        try (Connection c = DriverManager.getConnection(PG.getJdbcUrl(), "postgres", "test");
             Statement s = c.createStatement()) {
            s.execute("CREATE TABLE trunc_probe (id bigint PRIMARY KEY, v text)");
            s.execute("INSERT INTO trunc_probe VALUES (1,'a'), (2,'b'), (3,'c')");
        }
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500))
                .ignoreExceptions().untilAsserted(() ->
                assertThat(engine.queryScalar("SELECT count(*) FROM public.trunc_probe", Long.class)).isEqualTo(3L));

        try (Connection c = DriverManager.getConnection(PG.getJdbcUrl(), "postgres", "test");
             Statement s = c.createStatement()) {
            s.execute("TRUNCATE trunc_probe");
        }
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500)).untilAsserted(() ->
                assertThat(engine.queryScalar("SELECT count(*) FROM public.trunc_probe", Long.class)).isZero());
        // TRUNCATE 后新增照常镜像(段序语义)
        try (Connection c = DriverManager.getConnection(PG.getJdbcUrl(), "postgres", "test");
             Statement s = c.createStatement()) {
            s.execute("INSERT INTO trunc_probe VALUES (10,'fresh')");
        }
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500)).untilAsserted(() ->
                assertThat(engine.queryScalar("SELECT count(*) FROM public.trunc_probe", Long.class)).isEqualTo(1L));
    }

    @Test
    @Order(8)
    void dropTableFollowsToLake() throws Exception {
        // 镜像语义:源 DROP TABLE → 湖表真删(followDropTable 默认 true)
        try (Connection c = DriverManager.getConnection(PG.getJdbcUrl(), "postgres", "test");
             Statement s = c.createStatement()) {
            s.execute("DROP TABLE app.orders");
        }
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() ->
                assertThat(engine.queryScalar(
                        "SELECT count(*) FROM information_schema.tables "
                                + "WHERE table_catalog='lake' AND table_schema='app' AND table_name='orders'",
                        Long.class)).isZero());
    }

    /**
     * 闭环兜底(最后执行)：把本类在源库建的所有用户表(CDC 基建 dbz_* 保留)全部 DROP,
     * 经 CDC 跟随后源库与湖两侧都不再残留任何测试数据——"新建的数据最终都删掉",
     * 显式验证,不依赖 Testcontainers 销毁容器来兜底清理。
     */
    @Test
    @Order(1000)
    void teardownDropsEveryCreatedTableSoSourceAndLakeEndEmpty() throws Exception {
        // ① 源侧：动态枚举所有用户表并 DROP（排除 DDL 审计表）。
        try (Connection c = DriverManager.getConnection(PG.getJdbcUrl(), "postgres", "test");
             Statement s = c.createStatement()) {
            var toDrop = new java.util.ArrayList<String>();
            try (var rs = s.executeQuery(
                    "SELECT table_schema, table_name FROM information_schema.tables "
                    + "WHERE table_type='BASE TABLE' AND table_schema NOT IN ('pg_catalog','information_schema') "
                    + "AND table_name NOT LIKE 'dbz%'")) {
                while (rs.next()) toDrop.add("\"" + rs.getString(1) + "\".\"" + rs.getString(2) + "\"");
            }
            for (String fq : toDrop) {
                s.execute("DROP TABLE IF EXISTS " + fq + " CASCADE");
            }
            // 源侧用户表已清零(基建表不算)
            try (var rs = s.executeQuery(
                    "SELECT count(*) FROM information_schema.tables WHERE table_type='BASE TABLE' "
                    + "AND table_schema NOT IN ('pg_catalog','information_schema') AND table_name NOT LIKE 'dbz%'")) {
                rs.next();
                assertThat(rs.getLong(1)).isZero();
            }
        }
        // ② 湖侧收敛:源 DROP 经 CDC 跟随后,public/app 下不再有任何用户表
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500)).untilAsserted(() ->
                assertThat(engine.queryScalar(
                        "SELECT count(*) FROM information_schema.tables "
                                + "WHERE table_catalog='lake' AND table_schema IN ('public','app')", Long.class)).isZero());
    }

    private Long lakeCount(String sql) throws Exception {
        return engine.queryScalar(sql, Long.class);
    }

    /** 湖表 public.cdc_test 指定列的 data_type(列不存在返回 null) */
    private String lakeColumnType(String column) throws Exception {
        return engine.queryScalar("SELECT data_type FROM information_schema.columns "
                + "WHERE table_catalog='lake' AND table_schema='public' AND table_name='cdc_test' AND column_name='" + column + "'",
                String.class);
    }
}
