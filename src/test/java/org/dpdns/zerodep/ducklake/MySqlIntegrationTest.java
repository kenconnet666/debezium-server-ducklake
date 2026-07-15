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
import org.testcontainers.containers.MySQLContainer;
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
 * MySQL 8.0+ 源全链集成测试(Testcontainers,Docker 不可用时整类跳过)：
 * 真 MySQL 8.4(binlog ROW/FULL 默认开) + 真 PG(DuckLake catalog + offset + schema history)
 * + 真 DuckLake(湖数据落本地 @TempDir)。
 * <p>
 * 与 PG 版集成测试同口径，另覆盖 MySQL 专属面：JdbcSchemaHistory 落 catalog PG、
 * binlog schema change 事件驱动的 DDL 跟随(RENAME/DROP COLUMN/COMMENT/DROP TABLE)、
 * TRUNCATE 跟随(op=t)、TINYINT(1) 两阶段一致(TinyIntOneToBooleanConverter)、
 * MySQL 类型族落湖(DATETIME/TIMESTAMP/DECIMAL/JSON/ENUM)。
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MySqlIntegrationTest {

    /** 源库：8.4 LTS 默认 log_bin=ON / binlog_format=ROW / binlog_row_image=FULL，零参数调整 */
    @Container
    static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("shop").withUsername("test").withPassword("test");

    /** 湖 catalog（恒为 PG，与源库类型无关）：offset + schema history + DuckLake 元数据同库 */
    @Container
    static final PostgreSQLContainer<?> CATALOG = new PostgreSQLContainer<>("postgres:18-alpine")
            .withDatabaseName("ducklake_catalog").withUsername("lake_admin").withPassword("test");

    @TempDir
    static Path lakeDir;

    @Autowired
    DuckLakeEngine engine;
    @Autowired
    SyncState syncState;
    @LocalServerPort
    int port;

    /** 等价于 docker/mysql/initdb：CDC 账号(官方权限清单)/signal 表/心跳表/冒烟表 */
    @BeforeAll
    static void provisionSourceDatabase() throws Exception {
        try (Connection c = DriverManager.getConnection(MYSQL.getJdbcUrl(), "root", "test");
             Statement s = c.createStatement()) {
            // 官方 GRANT 原文(caching_sha2_password 为 8.0+ 默认认证插件,顺带验证其连通性)
            s.execute("CREATE USER 'dbuser_cdc'@'%' IDENTIFIED BY 'test'");
            s.execute("GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'dbuser_cdc'@'%'");
            // signal 表(增量/blocking 快照触发;连接器要写水位,CDC 账号要 INSERT/DELETE)
            s.execute("CREATE TABLE shop.dbz_signal (id varchar(42) PRIMARY KEY, type varchar(32) NOT NULL, data varchar(2048))");
            s.execute("GRANT INSERT, UPDATE, DELETE, DROP ON shop.dbz_signal TO 'dbuser_cdc'@'%'");

            // 冒烟表:覆盖 MySQL 类型族(BOOLEAN=TINYINT(1)/DATETIME/TIMESTAMP/DECIMAL/JSON/ENUM)
            s.execute("""
                    CREATE TABLE shop.cdc_test (
                        id bigint AUTO_INCREMENT PRIMARY KEY,
                        name varchar(64) NOT NULL,
                        big_content text,
                        amount decimal(12,2) DEFAULT 0,
                        flag boolean DEFAULT false,
                        created datetime(6) DEFAULT CURRENT_TIMESTAMP(6),
                        updated timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6),
                        payload json NULL,
                        tier enum('gold','silver') DEFAULT 'silver')""");
            s.execute("INSERT INTO shop.cdc_test (name, amount, flag, payload, tier) VALUES "
                    + "('alpha', 10.50, true, '{\"k\": 1}', 'gold'), ('beta', 20.00, false, NULL, 'silver'), "
                    + "('gamma', 99.99, true, '{\"k\": 3}', 'silver')");
            // 存量空表:无任何数据事件,湖表应由快照期 schema change(历史 CREATE)直接建出
            s.execute("CREATE TABLE shop.empty_seed (id bigint PRIMARY KEY, note varchar(64) COMMENT '备注') COMMENT='存量空表'");
        }
    }

    /** 源指向 MySQL 容器、catalog 指向 PG 容器；湖数据落 @TempDir */
    @DynamicPropertySource
    static void wire(DynamicPropertyRegistry r) {
        r.add("ducklake.source.type", () -> "mysql");
        r.add("ducklake.source.hostname", MYSQL::getHost);
        r.add("ducklake.source.port", MYSQL::getFirstMappedPort);
        r.add("ducklake.source.user", () -> "dbuser_cdc");
        r.add("ducklake.source.password", () -> "test");
        r.add("ducklake.source.dbname", () -> "shop");
        r.add("ducklake.source.server-id", () -> "6401");

        r.add("ducklake.lake.catalog-host", CATALOG::getHost);
        r.add("ducklake.lake.catalog-port", CATALOG::getFirstMappedPort);
        r.add("ducklake.lake.catalog-db", () -> "ducklake_catalog");
        r.add("ducklake.lake.catalog-user", () -> "lake_admin");
        r.add("ducklake.lake.catalog-password", () -> "test");
        r.add("ducklake.lake.data-path", () -> lakeDir.toString().replace('\\', '/') + "/");
        r.add("ducklake.lake.s3-endpoint", () -> "127.0.0.1:1");
        r.add("ducklake.lake.s3-access-key", () -> "dummy");
        r.add("ducklake.lake.s3-secret-key", () -> "dummy");
        r.add("ducklake.maintenance.enabled", () -> "false");
    }

    private static Connection mysql() throws Exception {
        return DriverManager.getConnection(MYSQL.getJdbcUrl(), "root", "test");
    }

    // ---------- 用例(有序:快照 → 增量 → DDL → TRUNCATE → 接口) ----------

    @Test
    @Order(1)
    void snapshotLandsInLakeWithMysqlTypes() {
        // MySQL 首启快照较慢(容器初始化+FLUSH TABLES WITH READ LOCK 流程),放宽到 120s。
        // ignoreExceptions:湖 schema/表由快照首批数据建出,建成前 SELECT 抛 Catalog Error,
        // 应视作"未就绪"继续等(对齐 PG 版 pkfix 用例的写法)
        await().atMost(Duration.ofSeconds(120)).pollInterval(Duration.ofSeconds(1))
                .ignoreExceptions().untilAsserted(() ->
                assertThat(lakeCount("SELECT count(*) FROM shop.cdc_test")).isEqualTo(3L));
        assertThat(syncState.getEngineRunning().get()).isEqualTo(1);
    }

    @Test
    @Order(2)
    void mysqlTypeFamilyMapsToNativeLakeColumns() throws Exception {
        // TINYINT(1)→BOOLEAN(TinyIntOneToBooleanConverter,快照/流式两阶段一致)
        assertThat(lakeColumnType("flag")).isEqualTo("BOOLEAN");
        // DATETIME(无时区墙钟)→TIMESTAMP;TIMESTAMP(UTC 规范化)→TIMESTAMPTZ
        // (information_schema 的原始形态是 "TIMESTAMP WITH TIME ZONE",此处按裸查口径断言)
        assertThat(lakeColumnType("created")).isEqualTo("TIMESTAMP");
        assertThat(lakeColumnType("updated")).isEqualTo("TIMESTAMP WITH TIME ZONE");
        assertThat(lakeColumnType("amount")).startsWith("DECIMAL(38,");
        assertThat(lakeColumnType("payload")).isEqualTo("JSON");
        assertThat(lakeColumnType("tier")).isEqualTo("VARCHAR"); // ENUM→VARCHAR
        // 值正确性抽查
        assertThat(engine.queryScalar(
                "SELECT tier FROM shop.cdc_test WHERE name='alpha'", String.class)).isEqualTo("gold");
        assertThat(lakeCount("SELECT count(*) FROM shop.cdc_test WHERE flag")).isEqualTo(2L);
    }

    @Test
    @Order(3)
    void streamingInsertUpdateDeleteMirrorsCurrentState() throws Exception {
        try (Connection c = mysql(); Statement s = c.createStatement()) {
            s.execute("INSERT INTO shop.cdc_test (name, amount, flag) VALUES ('it_insert', 42.42, true)");
            s.execute("UPDATE shop.cdc_test SET amount = 43.42 WHERE name = 'it_insert'");
        }
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500)).untilAsserted(() -> {
            assertThat(lakeCount("SELECT count(*) FROM shop.cdc_test WHERE name='it_insert'")).isEqualTo(1L);
            assertThat(lakeCount("SELECT count(*) FROM shop.cdc_test WHERE name='it_insert' AND amount=43.42")).isEqualTo(1L);
        });
        try (Connection c = mysql(); Statement s = c.createStatement()) {
            s.execute("DELETE FROM shop.cdc_test WHERE name = 'it_insert'");
        }
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500)).untilAsserted(() ->
                assertThat(lakeCount("SELECT count(*) FROM shop.cdc_test WHERE name='it_insert'")).isZero());
        // 湖表列与源表一一对应,不含 __* 元列
        assertThat(lakeColumnType("__op")).isNull();
        assertThat(lakeColumnType("__seq")).isNull();
    }

    @Test
    @Order(4)
    void binlogDdlFollowsRenameDropCommentAndTypeChange() throws Exception {
        // ① RENAME COLUMN(8.0 语法)→ 湖侧真 rename(binlog schema change 事件驱动,零审计表基建)
        try (Connection c = mysql(); Statement s = c.createStatement()) {
            s.execute("ALTER TABLE shop.cdc_test RENAME COLUMN amount TO price");
            s.execute("INSERT INTO shop.cdc_test (name, price) VALUES ('after_rename', 3.14)");
        }
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500)).untilAsserted(() -> {
            assertThat(lakeColumnType("price")).isNotNull();
            assertThat(lakeCount("SELECT count(*) FROM shop.cdc_test WHERE name='after_rename'")).isEqualTo(1L);
        });

        // ② DROP COLUMN → 湖列跟随真删
        try (Connection c = mysql(); Statement s = c.createStatement()) {
            s.execute("ALTER TABLE shop.cdc_test DROP COLUMN big_content");
            s.execute("INSERT INTO shop.cdc_test (name, price) VALUES ('after_dropcol', 1.00)");
        }
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500)).untilAsserted(() -> {
            assertThat(lakeCount("SELECT count(*) FROM shop.cdc_test WHERE name='after_dropcol'")).isEqualTo(1L);
            assertThat(lakeColumnType("big_content")).isNull();
        });

        // ③ 类型严格跟随(数据驱动):int 列 → MODIFY bigint 宽化,插超 INT32 值验证
        try (Connection c = mysql(); Statement s = c.createStatement()) {
            s.execute("ALTER TABLE shop.cdc_test ADD COLUMN hits int");
            s.execute("INSERT INTO shop.cdc_test (name, price, hits) VALUES ('with_hits', 1.00, 100)");
        }
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500)).untilAsserted(() ->
                assertThat(lakeColumnType("hits")).isEqualTo("INTEGER"));
        try (Connection c = mysql(); Statement s = c.createStatement()) {
            s.execute("ALTER TABLE shop.cdc_test MODIFY COLUMN hits bigint");
            s.execute("INSERT INTO shop.cdc_test (name, price, hits) VALUES ('after_widen', 1.00, 4000000000)");
        }
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500)).untilAsserted(() -> {
            assertThat(lakeColumnType("hits")).isEqualTo("BIGINT");
            assertThat(lakeCount("SELECT count(*) FROM shop.cdc_test WHERE hits=4000000000")).isEqualTo(1L);
        });

        // ④ COMMENT 跟随:MySQL 注释内嵌在 ALTER 里(tableChanges 携带,include.schema.comments=true)
        try (Connection c = mysql(); Statement s = c.createStatement()) {
            s.execute("ALTER TABLE shop.cdc_test COMMENT='集成测试表'");
            s.execute("ALTER TABLE shop.cdc_test MODIFY COLUMN name varchar(64) NOT NULL COMMENT '名称'");
        }
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500)).untilAsserted(() -> {
            assertThat(engine.queryScalar("SELECT comment FROM duckdb_tables() "
                    + "WHERE database_name='lake' AND schema_name='shop' AND table_name='cdc_test'", String.class))
                    .isEqualTo("集成测试表");
            assertThat(engine.queryScalar("SELECT comment FROM duckdb_columns() "
                    + "WHERE database_name='lake' AND schema_name='shop' AND table_name='cdc_test' AND column_name='name'", String.class))
                    .isEqualTo("名称");
        });
    }

    @Test
    @Order(5)
    void truncateFollowsToLake() throws Exception {
        // TRUNCATE 跟随是 MySQL 路线的增强(PG 不支持):op=t 数据事件 + schema change 双路兜底
        try (Connection c = mysql(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE shop.trunc_probe (id bigint PRIMARY KEY, v varchar(16))");
            s.execute("INSERT INTO shop.trunc_probe VALUES (1,'a'), (2,'b'), (3,'c')");
        }
        // ignoreExceptions:湖表由首批数据建出,建成前 SELECT 抛 Catalog Error 视作未就绪
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500))
                .ignoreExceptions().untilAsserted(() ->
                assertThat(lakeCount("SELECT count(*) FROM shop.trunc_probe")).isEqualTo(3L));

        try (Connection c = mysql(); Statement s = c.createStatement()) {
            s.execute("TRUNCATE TABLE shop.trunc_probe");
        }
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500)).untilAsserted(() ->
                assertThat(lakeCount("SELECT count(*) FROM shop.trunc_probe")).isZero());

        // TRUNCATE 后新增照常镜像(段序语义:清空 → 新行)
        try (Connection c = mysql(); Statement s = c.createStatement()) {
            s.execute("INSERT INTO shop.trunc_probe VALUES (10,'fresh')");
        }
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500)).untilAsserted(() ->
                assertThat(lakeCount("SELECT count(*) FROM shop.trunc_probe")).isEqualTo(1L));
    }

    @Test
    @Order(6)
    void dropTableFollowsToLake() throws Exception {
        try (Connection c = mysql(); Statement s = c.createStatement()) {
            s.execute("DROP TABLE shop.trunc_probe");
        }
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() ->
                assertThat(engine.queryScalar(
                        "SELECT count(*) FROM information_schema.tables "
                                + "WHERE table_catalog='lake' AND table_schema='shop' AND table_name='trunc_probe'",
                        Long.class)).isZero());
    }

    @Test
    @Order(7)
    void schemaHistoryPersistedInCatalogPg() throws Exception {
        // MySQL 硬前提:schema history 经 JdbcSchemaHistory 落 catalog PG(与 offset 同库),
        // 容器无本地状态——重启后结构从此表重放恢复
        try (Connection c = DriverManager.getConnection(CATALOG.getJdbcUrl(), "lake_admin", "test");
             Statement s = c.createStatement();
             var rs = s.executeQuery("SELECT count(*) FROM debezium_database_history")) {
            rs.next();
            assertThat(rs.getLong(1)).as("schema history 应已写入 catalog PG").isGreaterThan(0);
        }
        // offset 表同库(复用 PG 版机制)
        try (Connection c = DriverManager.getConnection(CATALOG.getJdbcUrl(), "lake_admin", "test");
             Statement s = c.createStatement();
             var rs = s.executeQuery("SELECT count(*) FROM debezium_offset_storage")) {
            rs.next();
            assertThat(rs.getLong(1)).isGreaterThan(0);
        }
    }

    /** DDL 驱动建空表:存量空表借快照期历史 CREATE 补齐(含表/列注释),流式 CREATE 即刻建 */
    @Test
    @Order(9)
    void emptyTablesCreatedByDdlWithComments() throws Exception {
        // 存量空表(快照期已建):0 行 + 注释落湖
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500))
                .ignoreExceptions().untilAsserted(() ->
                assertThat(lakeCount("SELECT count(*) FROM shop.empty_seed")).isZero());
        assertThat(engine.queryScalar("SELECT comment FROM duckdb_tables() "
                + "WHERE database_name='lake' AND schema_name='shop' AND table_name='empty_seed'", String.class))
                .isEqualTo("存量空表");
        assertThat(engine.queryScalar("SELECT comment FROM duckdb_columns() WHERE database_name='lake' "
                + "AND schema_name='shop' AND table_name='empty_seed' AND column_name='note'", String.class))
                .isEqualTo("备注");

        // 流式 CREATE 空表:不插数据即建
        try (Connection c = mysql(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE shop.empty_live (id bigint PRIMARY KEY, v varchar(16))");
        }
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500))
                .ignoreExceptions().untilAsserted(() ->
                assertThat(lakeCount("SELECT count(*) FROM shop.empty_live")).isZero());
    }

    /** MySQL TIME 毒丸(合法值域 ±838h 超湖 TIME 24h):行不丢、链路不断(TRY_CAST 置 NULL 或
     *  Debezium 侧钳制,两者皆可——钉死"不 crash loop"这一行为) */
    @Test
    @Order(10)
    void poisonTimeValuesDoNotBreakPipeline() throws Exception {
        try (Connection c = mysql(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE shop.poison_t (id bigint PRIMARY KEY, dur time)");
            s.execute("INSERT INTO shop.poison_t VALUES (1, '838:59:59'), (2, '-100:00:00'), (3, '12:34:56')");
        }
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500))
                .ignoreExceptions().untilAsserted(() ->
                assertThat(lakeCount("SELECT count(*) FROM shop.poison_t")).isEqualTo(3L));
        // 正常值保真;毒丸行存在(值 NULL 或钳制值均可)
        assertThat(lakeCount("SELECT count(*) FROM shop.poison_t WHERE id=3 AND dur='12:34:56'")).isEqualTo(1L);
    }

    @Test
    @Order(8)
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

    /** 湖表 shop.cdc_test 指定列的 data_type(列不存在返回 null) */
    private String lakeColumnType(String column) throws Exception {
        return engine.queryScalar("SELECT data_type FROM information_schema.columns "
                        + "WHERE table_catalog='lake' AND table_schema='shop' AND table_name='cdc_test' AND column_name='" + column + "'",
                String.class);
    }
}
