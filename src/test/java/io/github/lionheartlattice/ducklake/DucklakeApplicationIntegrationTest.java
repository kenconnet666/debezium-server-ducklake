package io.github.lionheartlattice.ducklake;

import io.github.lionheartlattice.ducklake.metrics.SyncState;
import io.github.lionheartlattice.ducklake.sink.DuckLakeEngine;
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
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

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
 * 验证与服务器 E2E 同口径：完整 Spring 上下文启动 → 引擎快照 → 流式增量(INSERT)→
 * DDL 审计流(RENAME COLUMN 湖侧真 rename)→ /watermark 接口。
 */
@Testcontainers(disabledWithoutDocker = true)
// Redis 是备用连接(暂无业务逻辑依赖),集成测试不起 Redis 容器,按类排除其装配
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "spring.autoconfigure.exclude="
                + "org.redisson.spring.starter.RedissonAutoConfigurationV2,"
                + "org.redisson.spring.starter.RedissonAutoConfigurationV4,"
                + "org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration,"
                + "org.springframework.boot.data.redis.autoconfigure.DataRedisReactiveAutoConfiguration,"
                + "org.springframework.boot.data.redis.autoconfigure.DataRedisRepositoriesAutoConfiguration,"
                + "org.springframework.boot.data.redis.autoconfigure.health.DataRedisHealthContributorAutoConfiguration,"
                + "org.springframework.boot.data.redis.autoconfigure.health.DataRedisReactiveHealthContributorAutoConfiguration,"
                + "org.springframework.boot.data.redis.autoconfigure.observation.LettuceObservationAutoConfiguration")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DucklakeApplicationIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:18-alpine")
            .withDatabaseName("postgres").withUsername("postgres").withPassword("test")
            .withCommand("postgres", "-c", "wal_level=logical");

    @TempDir
    static Path lakeDir;

    @Autowired
    DuckLakeEngine engine;
    @Autowired
    SyncState syncState;
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
            s.execute("CREATE PUBLICATION dbz_publication FOR TABLES IN SCHEMA public");
            s.execute("CREATE DATABASE ducklake_catalog OWNER dbuser_zadmin");

            s.execute("""
                    CREATE TABLE sys_ddl_log (
                        id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        ev text NOT NULL, tag text, object_type text, object_identity text, query_text text,
                        xid bigint NOT NULL DEFAULT (pg_current_xact_id()::text::bigint),
                        occurred_at timestamptz NOT NULL DEFAULT now())""");
            s.execute("""
                    CREATE OR REPLACE FUNCTION fn_capture_ddl() RETURNS event_trigger
                    LANGUAGE plpgsql SECURITY DEFINER AS $fn$
                    DECLARE r record;
                    BEGIN
                      FOR r IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
                        IF r.schema_name = 'public' AND r.object_identity <> 'public.sys_ddl_log' THEN
                          INSERT INTO public.sys_ddl_log(ev, tag, object_type, object_identity, query_text)
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
                        IF r.schema_name = 'public' AND r.object_type IN ('table', 'table column')
                           AND r.object_identity NOT LIKE 'public.sys_ddl_log%' THEN
                          INSERT INTO public.sys_ddl_log(ev, tag, object_type, object_identity, query_text)
                          VALUES ('sql_drop', tg_tag, r.object_type, r.object_identity, current_query());
                        END IF;
                      END LOOP;
                    END $fn$""");
            s.execute("CREATE EVENT TRIGGER trg_capture_ddl ON ddl_command_end "
                    + "WHEN TAG IN ('CREATE TABLE', 'ALTER TABLE', 'DROP TABLE') EXECUTE FUNCTION fn_capture_ddl()");
            s.execute("CREATE EVENT TRIGGER trg_capture_drop ON sql_drop "
                    + "WHEN TAG IN ('ALTER TABLE', 'DROP TABLE') EXECUTE FUNCTION fn_capture_drop()");

            s.execute("CREATE TABLE cdc_test (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, "
                    + "name text NOT NULL, amount numeric(12,2) DEFAULT 0)");
            s.execute("ALTER TABLE cdc_test REPLICA IDENTITY FULL");
            s.execute("INSERT INTO cdc_test (name, amount) VALUES ('alpha', 10.50), ('beta', 20.00), ('gamma', 99.99)");
        }
    }

    /** 全部连接指向 PG 容器;湖数据落 @TempDir(覆盖 dev profile 的本机地址) */
    @DynamicPropertySource
    static void wire(DynamicPropertyRegistry r) {
        r.add("spring.datasource.dynamic.primary.url", PG::getJdbcUrl);
        r.add("spring.datasource.dynamic.primary.username", () -> "postgres");
        r.add("spring.datasource.dynamic.primary.password", () -> "test");

        r.add("zadmin.ducklake.source.hostname", PG::getHost);
        r.add("zadmin.ducklake.source.port", PG::getFirstMappedPort);
        r.add("zadmin.ducklake.source.user", () -> "dbuser_cdc");
        r.add("zadmin.ducklake.source.password", () -> "test");
        r.add("zadmin.ducklake.source.dbname", () -> "postgres");

        r.add("zadmin.ducklake.lake.catalog-host", PG::getHost);
        r.add("zadmin.ducklake.lake.catalog-port", PG::getFirstMappedPort);
        r.add("zadmin.ducklake.lake.catalog-user", () -> "dbuser_zadmin");
        r.add("zadmin.ducklake.lake.catalog-password", () -> "test");
        r.add("zadmin.ducklake.lake.data-path", () -> lakeDir.toString().replace('\\', '/') + "/");
        // SECRET 创建是惰性的(不连接);本测试全程 Data Inlining + 本地 DATA_PATH,S3 永不触达
        r.add("zadmin.ducklake.lake.s3-endpoint", () -> "127.0.0.1:1");
        r.add("zadmin.ducklake.lake.s3-access-key", () -> "dummy");
        r.add("zadmin.ducklake.lake.s3-secret-key", () -> "dummy");
        r.add("zadmin.ducklake.maintenance.enabled", () -> "false");

        // oss.* 是 core S3Config 的必填配置(无条件装配);同样惰性,不触达
        r.add("oss.endpoint", () -> "127.0.0.1:1");
        r.add("oss.access-key", () -> "dummy");
        r.add("oss.secret-key", () -> "dummy");
        r.add("oss.bucket-name", () -> "lake");
        r.add("oss.region", () -> "us-east-1");
        r.add("oss.path-style-access", () -> "true");
    }

    // ---------- 用例(有序:快照 → 增量 → DDL → 接口) ----------

    @Test
    @Order(1)
    void snapshotLandsInLake() {
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() ->
                assertThat(lakeCount("SELECT count(*) FROM cdc.public_cdc_test")).isEqualTo(3L));
        assertThat(syncState.getEngineRunning().get()).isEqualTo(1);
    }

    @Test
    @Order(2)
    void streamingInsertUpdateDeleteProduceTombstoneChain() throws Exception {
        try (Connection c = DriverManager.getConnection(PG.getJdbcUrl(), "postgres", "test");
             Statement s = c.createStatement()) {
            s.execute("INSERT INTO cdc_test (name, amount) VALUES ('it_insert', 42.42)");
            s.execute("UPDATE cdc_test SET amount = 43.42 WHERE name = 'it_insert'");
            s.execute("DELETE FROM cdc_test WHERE name = 'it_insert'");
        }
        // c → u → d 三事件全落湖,DELETE 是 __deleted='true' 的整行墓碑
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500)).untilAsserted(() ->
                assertThat(lakeCount("SELECT count(*) FROM cdc.public_cdc_test WHERE name='it_insert'")).isEqualTo(3L));
        assertThat(lakeCount("SELECT count(*) FROM cdc.public_cdc_test WHERE name='it_insert' AND __deleted='true' AND __op='d'"))
                .isEqualTo(1L);
    }

    @Test
    @Order(3)
    void renameColumnIsAppliedToLakeViaDdlAuditStream() throws Exception {
        try (Connection c = DriverManager.getConnection(PG.getJdbcUrl(), "postgres", "test");
             Statement s = c.createStatement()) {
            s.execute("ALTER TABLE cdc_test RENAME COLUMN amount TO price");
            s.execute("INSERT INTO cdc_test (name, price) VALUES ('after_rename', 3.14)");
        }
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500)).untilAsserted(() -> {
            // 湖列已改名(rename 前的旧行在新列名下可读——DuckLake 元数据级 rename)
            assertThat(lakeCount("SELECT count(*) FROM information_schema.columns "
                    + "WHERE table_catalog='lake' AND table_name='public_cdc_test' AND column_name='price'")).isEqualTo(1L);
            assertThat(lakeCount("SELECT count(*) FROM cdc.public_cdc_test WHERE name='after_rename'")).isEqualTo(1L);
        });
        // DDL 全量审计落 meta.ddl_history
        assertThat(lakeCount("SELECT count(*) FROM meta.ddl_history WHERE query_text LIKE '%RENAME COLUMN%'"))
                .isGreaterThanOrEqualTo(1L);
    }

    @Test
    @Order(4)
    void watermarkEndpointReportsHealthyPipeline() {
        String body = RestClient.create("http://127.0.0.1:" + port + "/api/ducklake")
                .get().uri("/watermark").retrieve().body(String.class);
        assertThat(body)
                .contains("\"code\":\"0000\"")
                .contains("\"engineRunning\":true")
                .contains("\"lastSyncedAt\":\"2026-");
    }

    private Long lakeCount(String sql) throws Exception {
        return engine.queryScalar(sql, Long.class);
    }
}
