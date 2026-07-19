-- ============================================================================
-- debezium-server-ducklake 源库一次性初始化（以超级用户在业务库执行）
--   psql -h <host> -U postgres -d <业务库> -f init-source-db.sql
--
-- 前置：postgresql.conf 需 wal_level=logical（重启生效），
--       max_replication_slots / max_wal_senders 留足额度（默认 10 通常够单实例）。
-- 本脚本幂等可重跑。密码请替换为你自己的。
-- ============================================================================

-- ① 角色：CDC 复制账号（logical replication + 全库只读）
DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'dbuser_cdc') THEN
    CREATE ROLE dbuser_cdc REPLICATION LOGIN PASSWORD 'changeme';
  END IF;
END $$;
GRANT pg_read_all_data TO dbuser_cdc;

-- ② publication：FOR ALL TABLES 整库发布——所有 schema 的表、含新建表自动纳入捕获
--    （服务端默认整库同步：scanner 存量首灌 + WAL 增量，湖表按 schema/table 一一对应；
--     需收窄范围时改用 FOR TABLES IN SCHEMA ... 并配 DUCKLAKE_SCHEMA_INCLUDE）
DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'dbz_publication') THEN
    CREATE PUBLICATION dbz_publication FOR ALL TABLES;
  END IF;
END $$;

-- ② 方案B 按 schema 分片 publication（多实例横向扩展，聚合吞吐近线性，harness 实测 2× 线性）。
-- 按需取消注释，每实例对应一个独立 slot + publication：
--   实例A：slot=dbz_ducklake_a  publication=pub_schema_a  schema-include-list=schema_a
--   实例B：slot=dbz_ducklake_b  publication=pub_schema_b  schema-include-list=schema_b
-- 注意：每路 walsender 全量解码 WAL 再按 publication 过滤，N 个分片 = N× WAL 读放大；
--       按表拆 publication（每路只解码自己 schema 的 WAL 再过滤）优于按行 hash 分片同一热表。
-- DO $$ BEGIN
--   IF NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'pub_schema_a') THEN
--     CREATE PUBLICATION pub_schema_a FOR TABLES IN SCHEMA schema_a;
--   END IF;
--   IF NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'pub_schema_b') THEN
--     CREATE PUBLICATION pub_schema_b FOR TABLES IN SCHEMA schema_b;
--   END IF;
-- END $$;

-- ③ DuckLake catalog（湖元数据 + 原生 reader offset）——**推荐独立 PG 实例承载**
--    （.docker/postgres/docker-compose.yml 即此形态：catalog-pg 容器由 POSTGRES_USER=lake_admin /
--     POSTGRES_DB=ducklake_catalog 环境变量直接建好，无需任何脚本）。
--    仅当与源库共用同一实例时才取消下面两行注释：
-- CREATE ROLE lake_admin LOGIN PASSWORD 'changeme';
-- CREATE DATABASE ducklake_catalog OWNER lake_admin;   -- 重跑报 already exists 可忽略

-- ④ DDL 审计流：event trigger 把 DDL 写进 dbz_ddl_log，
--    该表在 publication 内随流复制，服务端 DdlApplier 据此跟随 rename/删列
CREATE TABLE IF NOT EXISTS public.dbz_ddl_log (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ev text NOT NULL, tag text, object_type text, object_identity text, query_text text,
    xid bigint NOT NULL DEFAULT (pg_current_xact_id()::text::bigint),
    occurred_at timestamptz NOT NULL DEFAULT now());

-- 审计范围与整库捕获对齐:全部用户 schema(排系统 schema 与临时 schema),审计表自身除外
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
END $fn$;

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
END $fn$;

DROP EVENT TRIGGER IF EXISTS trg_capture_ddl;
CREATE EVENT TRIGGER trg_capture_ddl ON ddl_command_end
    WHEN TAG IN ('CREATE TABLE', 'ALTER TABLE', 'DROP TABLE', 'COMMENT') EXECUTE FUNCTION fn_capture_ddl();
DROP EVENT TRIGGER IF EXISTS trg_capture_drop;
CREATE EVENT TRIGGER trg_capture_drop ON sql_drop
    WHEN TAG IN ('ALTER TABLE', 'DROP TABLE', 'DROP SCHEMA') EXECUTE FUNCTION fn_capture_drop();

GRANT TRUNCATE ON public.dbz_ddl_log TO dbuser_cdc;  -- 维护任务定期清空防堆积

-- ⑤ 业务表要求：**必须有主键**（无主键的表在 FOR ALL TABLES publication 下
--    连源库自己的 DELETE/UPDATE 都会被 PG 拒绝(55000)，湖侧也只能降级 insert-only）。
--    无需 REPLICA IDENTITY FULL——镜像 upsert 只按主键定位行，DEFAULT（主键旧值）
--    足够且更省 WAL（FULL 会把整行旧值写入 WAL，宽表高频更新时膨胀明显）。
