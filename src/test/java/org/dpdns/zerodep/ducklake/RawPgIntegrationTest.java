package org.dpdns.zerodep.ducklake;

import org.dpdns.zerodep.ducklake.config.DucklakeProperties;
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
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
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
 * PostgreSQL 原生 CDC 全链集成测试（Testcontainers，Docker 不可用时整类跳过）：
 * 真 PG18（wal_level=logical）+ 真 DuckLake。
 * <p>
 * 验证范围：
 * <ul>
 *   <li>流式 INSERT / UPDATE / DELETE（pgoutput 切帧+Relation 缓存）</li>
 *   <li>unchanged TOAST 保值、同事务连续 UPDATE、主键变更按旧 tuple 定位</li>
 *   <li>类型映射矩阵（bool/int/numeric/text/date/time/ts/tstz/interval/uuid/bytea/array）</li>
 *   <li>ADD COLUMN：Relation 消息自愈（不需 DDL 审计，ensureTable 即刻感知）</li>
 *   <li>RENAME COLUMN：DDL 审计表 → DdlApplier.applyRaw</li>
 *   <li>多 schema 自动对应</li>
 *   <li>无主键表降级 insert-only</li>
 * </ul>
 * scanner-bootstrap/scanner-refill 均关闭（测试环境无 postgres_scanner 扩展）：
 * 无快照路径，全部数据经流式写入，引擎启动后插入的行均可验证。
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RawPgIntegrationTest {

    @Container
    static final PostgreSQLContainer PG = new PostgreSQLContainer("postgres:18-alpine")
            .withDatabaseName("postgres").withUsername("postgres").withPassword("test")
            .withCommand("postgres", "-c", "wal_level=logical");

    @TempDir
    static Path lakeDir;

    @Autowired DuckLakeEngine engine;
    @Autowired DucklakeProperties props;
    @Autowired SyncState syncState;

    // ──────────── 源库初始化（Spring 上下文启动前完成） ────────────

    @BeforeAll
    static void provision() throws Exception {
        try (Connection c = DriverManager.getConnection(PG.getJdbcUrl(), "postgres", "test");
             Statement s = c.createStatement()) {

            // CDC 角色 + 发布
            s.execute("CREATE ROLE dbuser_cdc REPLICATION LOGIN PASSWORD 'test'");
            s.execute("GRANT pg_read_all_data TO dbuser_cdc");
            s.execute("CREATE ROLE dbuser_zadmin LOGIN PASSWORD 'test'");
            s.execute("GRANT CREATE ON SCHEMA public TO dbuser_zadmin");
            s.execute("CREATE PUBLICATION dbz_publication FOR ALL TABLES");
            s.execute("CREATE DATABASE ducklake_catalog OWNER dbuser_zadmin");

            // DDL 审计表 + event trigger（RENAME COLUMN / DROP COLUMN / DROP TABLE 跟随所需）
            s.execute("""
                    CREATE TABLE dbz_ddl_log (
                        id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        ev text NOT NULL, tag text, object_type text, object_identity text,
                        query_text text,
                        xid bigint NOT NULL DEFAULT (pg_current_xact_id()::text::bigint),
                        occurred_at timestamptz NOT NULL DEFAULT now())""");
            s.execute("""
                    CREATE OR REPLACE FUNCTION fn_capture_ddl() RETURNS event_trigger
                    LANGUAGE plpgsql SECURITY DEFINER AS $fn$
                    DECLARE r record;
                    BEGIN
                      FOR r IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
                        IF r.schema_name NOT IN ('pg_catalog','information_schema','pg_toast')
                           AND r.schema_name NOT LIKE 'pg_temp%'
                           AND r.object_identity <> 'public.dbz_ddl_log' THEN
                          INSERT INTO public.dbz_ddl_log(ev,tag,object_type,object_identity,query_text)
                          VALUES ('ddl_command_end',r.command_tag,r.object_type,r.object_identity,current_query());
                        END IF;
                      END LOOP;
                    END $fn$""");
            s.execute("""
                    CREATE OR REPLACE FUNCTION fn_capture_drop() RETURNS event_trigger
                    LANGUAGE plpgsql SECURITY DEFINER AS $fn$
                    DECLARE r record;
                    BEGIN
                      FOR r IN SELECT * FROM pg_event_trigger_dropped_objects() LOOP
                        IF r.schema_name NOT IN ('pg_catalog','information_schema','pg_toast')
                           AND r.schema_name NOT LIKE 'pg_temp%'
                           AND r.object_type IN ('table','table column')
                           AND r.object_identity NOT LIKE 'public.dbz_ddl_log%' THEN
                          INSERT INTO public.dbz_ddl_log(ev,tag,object_type,object_identity,query_text)
                          VALUES ('sql_drop',tg_tag,r.object_type,r.object_identity,current_query());
                        END IF;
                      END LOOP;
                    END $fn$""");
            s.execute("CREATE EVENT TRIGGER trg_capture_ddl ON ddl_command_end "
                    + "WHEN TAG IN ('CREATE TABLE','ALTER TABLE','DROP TABLE','COMMENT') "
                    + "EXECUTE FUNCTION fn_capture_ddl()");
            s.execute("CREATE EVENT TRIGGER trg_capture_drop ON sql_drop "
                    + "WHEN TAG IN ('ALTER TABLE','DROP TABLE','DROP SCHEMA') "
                    + "EXECUTE FUNCTION fn_capture_drop()");

            // 主测试表（丰富类型矩阵）
            s.execute("CREATE DOMAIN amount_7_3 AS numeric(7,3)");
            s.execute("""
                    CREATE TABLE cdc_raw (
                        id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        name     text     NOT NULL,
                        flag     boolean,
                        score    smallint,
                        cnt      integer,
                        big      bigint,
                        price    numeric(12,2),
                        ratio    float4,
                        rate     float8,
                        uid      uuid,
                        evt_date date,
                        evt_time time,
                        evt_tstz timestamptz,
                        evt_ts   timestamp,
                        dur      interval,
                        raw_data bytea,
                        tags     text[],
                        prices   numeric(12,2)[],
                        matrix   numeric(10,3)[][],
                        domain_prices amount_7_3[])""");

            // 非 public schema
            s.execute("CREATE SCHEMA app");
            s.execute("CREATE TABLE app.items (id bigint PRIMARY KEY, label text)");

            // 大字段更新语义：marker-only UPDATE 会在 pgoutput 中把 payload 编码为 'u'
            s.execute("CREATE TABLE toast_probe (id bigint PRIMARY KEY, payload text, marker integer)");

            // 无主键表（降级 insert-only）
            s.execute("CREATE TABLE nopk (v text)");
        }
    }

    @DynamicPropertySource
    static void wire(DynamicPropertyRegistry r) {
        r.add("ducklake.source.hostname", PG::getHost);
        r.add("ducklake.source.port", PG::getFirstMappedPort);
        r.add("ducklake.source.user", () -> "dbuser_cdc");
        r.add("ducklake.source.password", () -> "test");
        r.add("ducklake.source.dbname", () -> "postgres");

        r.add("ducklake.lake.catalog-host", PG::getHost);
        r.add("ducklake.lake.catalog-port", PG::getFirstMappedPort);
        r.add("ducklake.lake.catalog-db", () -> "ducklake_catalog");
        r.add("ducklake.lake.catalog-user", () -> "dbuser_zadmin");
        r.add("ducklake.lake.catalog-password", () -> "test");
        r.add("ducklake.lake.data-path",
                () -> lakeDir.toString().replace('\\', '/') + "/");
        r.add("ducklake.lake.s3-endpoint", () -> "127.0.0.1:1");
        r.add("ducklake.lake.s3-access-key", () -> "dummy");
        r.add("ducklake.lake.s3-secret-key", () -> "dummy");

        // scanner 路径关闭（测试环境无 postgres_scanner 扩展）
        r.add("ducklake.maintenance.scanner-bootstrap", () -> "false");
        r.add("ducklake.maintenance.scanner-refill", () -> "false");
        r.add("ducklake.maintenance.enabled", () -> "false");
    }

    // ──────────── 用例（有序，共享同一 Spring 上下文） ────────────

    @Test
    @Order(1)
    void engineStartsWithRawPg() throws Exception {
        assertThat(syncState.getEngineRunning().get())
                .as("PostgreSQL 原生 reader 应已启动").isEqualTo(1);
        assertThat(engine.queryScalar(
                "SELECT CAST(current_setting('ducklake_default_data_inlining_row_limit') AS INTEGER)",
                Integer.class))
                .as("Data Inlining 阈值必须显式下发，尤其是 0=禁用")
                .isEqualTo(props.getLake().getDataInliningRowLimit());
    }

    @Test
    @Order(2)
    void insertLandsInLake() throws Exception {
        exec("""
                INSERT INTO cdc_raw(name,flag,score,cnt,big,price,ratio,rate,
                    uid,evt_date,evt_time,evt_tstz,evt_ts,dur,raw_data,tags,prices,matrix,domain_prices)
                VALUES (
                    'row1', true, 32767, 2147483647, 9223372036854775807,
                    9999999999.99, 3.14, 2.718281828,
                    'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
                    '2024-02-29', '12:34:56.123456',
                    '2024-02-29 12:00:00.123456+08', '2024-02-29 12:00:00.123456',
                    interval '1 year 2 months 3 days 04:05:06.7',
                    decode('48656c6c6f','hex'),
                    ARRAY['a,b','c'],
                    ARRAY[1.23,9999999999.99]::numeric(12,2)[],
                    ARRAY[[1.234,2.345],[3.456,4.567]]::numeric(10,3)[][],
                    ARRAY[1234.567]::amount_7_3[])
                """);
        await30s(() -> assertThat(lakeCount("SELECT count(*) FROM public.cdc_raw")).isEqualTo(1L));
    }

    @Test
    @Order(3)
    void typeMappingIsCorrect() throws Exception {
        // 类型矩阵断言：pgoutput 文本 → DuckDB 原生类型。
        assertThat(lakeColumnType("cdc_raw", "flag")).isEqualTo("BOOLEAN");
        assertThat(lakeColumnType("cdc_raw", "score")).isEqualTo("SMALLINT");
        assertThat(lakeColumnType("cdc_raw", "cnt")).isEqualTo("INTEGER");
        assertThat(lakeColumnType("cdc_raw", "big")).isEqualTo("BIGINT");
        assertThat(lakeColumnType("cdc_raw", "price")).isEqualTo("DECIMAL(12,2)");
        assertThat(lakeColumnType("cdc_raw", "ratio")).isEqualTo("FLOAT");
        assertThat(lakeColumnType("cdc_raw", "rate")).isEqualTo("DOUBLE");
        assertThat(lakeColumnType("cdc_raw", "uid")).isEqualTo("UUID");
        assertThat(lakeColumnType("cdc_raw", "evt_date")).isEqualTo("DATE");
        assertThat(lakeColumnType("cdc_raw", "evt_time")).isEqualTo("TIME");
        assertThat(lakeColumnType("cdc_raw", "evt_tstz")).isEqualTo("TIMESTAMP WITH TIME ZONE");
        assertThat(lakeColumnType("cdc_raw", "evt_ts")).isEqualTo("TIMESTAMP");
        assertThat(lakeColumnType("cdc_raw", "dur")).isEqualTo("INTERVAL");
        assertThat(lakeColumnType("cdc_raw", "raw_data")).isEqualTo("BLOB");
        assertThat(lakeColumnType("cdc_raw", "tags")).isEqualTo("VARCHAR[]");
        assertThat(lakeColumnType("cdc_raw", "prices")).isEqualTo("DECIMAL(12,2)[]");
        assertThat(lakeColumnType("cdc_raw", "matrix")).isEqualTo("DECIMAL(10,3)[][]");
        assertThat(lakeColumnType("cdc_raw", "domain_prices")).isEqualTo("DECIMAL(7,3)[]");

        // 值精度断言
        assertThat(engine.queryScalar(
                "SELECT price FROM public.cdc_raw WHERE name='row1'", java.math.BigDecimal.class))
                .isEqualByComparingTo("9999999999.99");
        assertThat(engine.queryScalar(
                "SELECT big FROM public.cdc_raw WHERE name='row1'", Long.class))
                .isEqualTo(Long.MAX_VALUE);
        assertThat(engine.queryScalar(
                "SELECT flag FROM public.cdc_raw WHERE name='row1'", Boolean.class))
                .isTrue();
        // UUID 无损往返
        assertThat(engine.queryScalar(
                "SELECT uid::text FROM public.cdc_raw WHERE name='row1'", String.class))
                .isEqualTo("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
        assertThat(engine.queryScalar(
                "SELECT prices[2] FROM public.cdc_raw WHERE name='row1'", java.math.BigDecimal.class))
                .isEqualByComparingTo("9999999999.99");
        assertThat(engine.queryScalar(
                "SELECT matrix[2][2] FROM public.cdc_raw WHERE name='row1'", java.math.BigDecimal.class))
                .isEqualByComparingTo("4.567");
        assertThat(engine.queryScalar(
                "SELECT domain_prices[1] FROM public.cdc_raw WHERE name='row1'", java.math.BigDecimal.class))
                .isEqualByComparingTo("1234.567");
        // interval 可做日期运算
        assertThat(engine.queryScalar(
                "SELECT date_part('month', dur) FROM public.cdc_raw WHERE name='row1'", Long.class))
                .isEqualTo(2L);
        // bytea 正确存储验证：BLOB 与 unhex('Hello') 相等
        assertThat(engine.queryScalar(
                "SELECT raw_data = unhex('48656c6c6f') FROM public.cdc_raw WHERE name='row1'", Boolean.class))
                .isTrue();
        // text[] 第一个元素含逗号保真
        assertThat(engine.queryScalar(
                "SELECT tags[1] FROM public.cdc_raw WHERE name='row1'", String.class))
                .isEqualTo("a,b");
    }

    @Test
    @Order(4)
    void updateMirrorsCurrentState() throws Exception {
        exec("INSERT INTO cdc_raw(name,cnt) VALUES ('upd_target',100)");
        await30s(() -> assertThat(lakeCount(
                "SELECT count(*) FROM public.cdc_raw WHERE name='upd_target' AND cnt=100")).isEqualTo(1L));

        exec("UPDATE cdc_raw SET cnt=999 WHERE name='upd_target'");
        await30s(() -> {
            assertThat(lakeCount(
                    "SELECT count(*) FROM public.cdc_raw WHERE name='upd_target' AND cnt=999")).isEqualTo(1L);
            // UPDATE 就地更新：旧值行消失，总行只有一行
            assertThat(lakeCount(
                    "SELECT count(*) FROM public.cdc_raw WHERE name='upd_target'")).isEqualTo(1L);
        });
    }

    @Test
    @Order(5)
    void unchangedToastAndPrimaryKeyUpdatePreserveCurrentState() throws Exception {
        exec("""
                INSERT INTO toast_probe
                SELECT 9001, string_agg(md5(g::text), ''), 1
                FROM generate_series(1, 10000) g
                """);
        await30s(() -> assertThat(lakeCount(
                "SELECT length(payload) FROM public.toast_probe WHERE id=9001")).isEqualTo(320000L));

        // 同一源事务的后一条 'u' 必须读取前一条 UPDATE 后的湖值，不能被批内折叠成 NULL。
        try (Connection c = DriverManager.getConnection(PG.getJdbcUrl(), "postgres", "test");
             Statement s = c.createStatement()) {
            c.setAutoCommit(false);
            s.execute("UPDATE toast_probe SET marker=2 WHERE id=9001");
            s.execute("UPDATE toast_probe SET marker=3 WHERE id=9001");
            c.commit();
        }
        await30s(() -> {
            assertThat(lakeCount("SELECT marker FROM public.toast_probe WHERE id=9001")).isEqualTo(3L);
            assertThat(lakeCount("SELECT length(payload) FROM public.toast_probe WHERE id=9001")).isEqualTo(320000L);
        });

        // UPDATE 主键时 pgoutput 携带旧 K tuple；旧 identity 必须消失，TOAST 值仍保持。
        exec("UPDATE toast_probe SET id=9002, marker=4 WHERE id=9001");
        await30s(() -> {
            assertThat(lakeCount("SELECT count(*) FROM public.toast_probe WHERE id=9001")).isZero();
            assertThat(lakeCount("SELECT length(payload) FROM public.toast_probe WHERE id=9002 AND marker=4"))
                    .isEqualTo(320000L);
        });

        exec("INSERT INTO toast_probe SELECT g, repeat(md5(g::text), 10000), 0 "
                + "FROM generate_series(9100, 9107) g");
        await30s(() -> {
            assertThat(lakeCount("SELECT count(*) FROM public.toast_probe WHERE id BETWEEN 9100 AND 9107"))
                    .isEqualTo(8L);
            assertThat(lakeCount("SELECT count(*) FROM public.toast_probe "
                    + "WHERE id BETWEEN 9100 AND 9107 AND length(payload)=320000")).isEqualTo(8L);
        });
        exec("UPDATE toast_probe SET marker=5 WHERE id BETWEEN 9100 AND 9107");
        await30s(() -> {
            assertThat(lakeCount("SELECT count(*) FROM public.toast_probe "
                    + "WHERE id BETWEEN 9100 AND 9107 AND marker=5")).isEqualTo(8L);
            assertThat(lakeCount("SELECT count(*) FROM public.toast_probe "
                    + "WHERE id BETWEEN 9100 AND 9107 AND length(payload)=320000")).isEqualTo(8L);
        });
    }

    @Test
    @Order(6)
    void deletePhysicallyFollows() throws Exception {
        exec("INSERT INTO cdc_raw(name,cnt) VALUES ('del_target',1)");
        await30s(() -> assertThat(lakeCount(
                "SELECT count(*) FROM public.cdc_raw WHERE name='del_target'")).isEqualTo(1L));

        exec("DELETE FROM cdc_raw WHERE name='del_target'");
        await30s(() -> assertThat(lakeCount(
                "SELECT count(*) FROM public.cdc_raw WHERE name='del_target'")).isZero());
        // 无元数据列（湖 = 源表当前态镜像）
        assertThat(lakeColumnType("cdc_raw", "__op")).isNull();
        assertThat(lakeColumnType("cdc_raw", "__deleted")).isNull();
    }

    @Test
    @Order(7)
    void addColumnViaRelationMessageHealing() throws Exception {
        // Relation 消息自愈：源加列后下一条事件触发新 Relation 消息，ensureTable 即刻补列
        exec("ALTER TABLE cdc_raw ADD COLUMN note text");
        exec("INSERT INTO cdc_raw(name,note) VALUES ('with_note','hello')");
        await30s(() -> {
            assertThat(lakeColumnType("cdc_raw", "note")).isNotNull();
            assertThat(lakeCount(
                    "SELECT count(*) FROM public.cdc_raw WHERE note='hello'")).isEqualTo(1L);
        });
    }

    @Test
    @Order(8)
    void renameColumnViaAuditTable() throws Exception {
        // RENAME COLUMN：DDL 审计表 INSERT → DdlApplier.applyRaw → 湖侧真 rename
        exec("ALTER TABLE cdc_raw RENAME COLUMN cnt TO count_val");
        exec("INSERT INTO cdc_raw(name,count_val) VALUES ('after_rename',77)");
        await30s(() -> {
            // 新列名可查
            assertThat(lakeColumnType("cdc_raw", "count_val")).isNotNull();
            // 旧列名消失
            assertThat(lakeColumnType("cdc_raw", "cnt")).isNull();
            assertThat(lakeCount(
                    "SELECT count(*) FROM public.cdc_raw WHERE count_val=77")).isEqualTo(1L);
        });
    }

    @Test
    @Order(9)
    void dropColumnFollows() throws Exception {
        exec("ALTER TABLE cdc_raw DROP COLUMN score");
        exec("INSERT INTO cdc_raw(name) VALUES ('after_dropcol')");
        await30s(() -> {
            assertThat(lakeColumnType("cdc_raw", "score")).isNull();
            assertThat(lakeCount(
                    "SELECT count(*) FROM public.cdc_raw WHERE name='after_dropcol'")).isEqualTo(1L);
        });
    }

    @Test
    @Order(10)
    void multiSchemaStreamsAutomatically() throws Exception {
        exec("INSERT INTO app.items VALUES (1,'item-a'), (2,'item-b')");
        await30s(() -> assertThat(lakeCount("SELECT count(*) FROM app.items")).isEqualTo(2L));

        exec("UPDATE app.items SET label='item-a-upd' WHERE id=1");
        await30s(() -> assertThat(lakeCount(
                "SELECT count(*) FROM app.items WHERE label='item-a-upd'")).isEqualTo(1L));
    }

    @Test
    @Order(11)
    void noKeyTableInsertOnly() throws Exception {
        // 无主键表：INSERT 落湖（insert-only 语义）
        // DELETE 不测——对 FOR ALL TABLES publication，无 replica identity 表的 DELETE
        // 在 pgoutput 协议层不被复制（replication stream 不含该事件），
        // 且在 PG 会话层可能报错，不属于本引擎的职责范围
        exec("INSERT INTO nopk VALUES ('x'), ('y')");
        await30s(() -> assertThat(lakeCount("SELECT count(*) FROM public.nopk")).isEqualTo(2L));
        exec("INSERT INTO nopk VALUES ('z')");
        await30s(() -> assertThat(lakeCount("SELECT count(*) FROM public.nopk")).isEqualTo(3L));
    }

    @Test
    @Order(12)
    void nullValuesRoundtrip() throws Exception {
        exec("INSERT INTO cdc_raw(name,flag,uid,evt_date,dur,raw_data,tags) VALUES ('nulls',NULL,NULL,NULL,NULL,NULL,NULL)");
        await30s(() -> assertThat(lakeCount(
                "SELECT count(*) FROM public.cdc_raw WHERE name='nulls' AND flag IS NULL AND uid IS NULL")).isEqualTo(1L));
    }

    // ──────────── 工具方法 ────────────

    private void exec(String sql) throws Exception {
        try (Connection c = DriverManager.getConnection(PG.getJdbcUrl(), "postgres", "test");
             Statement s = c.createStatement()) {
            s.execute(sql);
        }
    }

    private void await30s(org.awaitility.core.ThrowingRunnable assertion) {
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(300))
                .untilAsserted(assertion);
    }

    /**
     * 安全版行数查询：Catalog Error（湖表尚未建出）返回 0L 而非抛异常。
     * 避免 DuckDB JDBC 连接因异常进入脏状态，确保 Awaitility 可以正常重试。
     */
    private Long lakeCount(String sql) {
        try {
            Long v = engine.queryScalar(sql, Long.class);
            return v == null ? 0L : v;
        } catch (Exception e) {
            return 0L;
        }
    }

    /** 湖表指定列的 data_type（列不存在返回 null） */
    private String lakeColumnType(String table, String column) throws Exception {
        return engine.queryScalar(
                "SELECT data_type FROM information_schema.columns "
                        + "WHERE table_catalog='lake' AND table_schema='public' "
                        + "AND table_name='" + table + "' AND column_name='" + column + "'",
                String.class);
    }
}
