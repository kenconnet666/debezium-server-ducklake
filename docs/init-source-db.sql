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

-- ② publication：FOR TABLES IN SCHEMA 让新建表自动纳入捕获
DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'dbz_publication') THEN
    CREATE PUBLICATION dbz_publication FOR TABLES IN SCHEMA public;
  END IF;
END $$;

-- ③ DuckLake catalog（湖元数据 + Debezium offset 存储）——**推荐独立 PG 实例承载**
--    （docker/docker-compose.yml 即此形态：catalog-pg 容器由 POSTGRES_USER=lake_admin /
--     POSTGRES_DB=ducklake_catalog 环境变量直接建好，无需任何脚本）。
--    仅当与源库共用同一实例时才取消下面两行注释：
-- CREATE ROLE lake_admin LOGIN PASSWORD 'changeme';
-- CREATE DATABASE ducklake_catalog OWNER lake_admin;   -- 重跑报 already exists 可忽略

-- ④ DDL 审计流：event trigger 把 DDL 写进 sys_ddl_log，
--    该表在 publication 内随流复制，服务端 DdlApplier 据此跟随 rename/删列
CREATE TABLE IF NOT EXISTS public.sys_ddl_log (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ev text NOT NULL, tag text, object_type text, object_identity text, query_text text,
    xid bigint NOT NULL DEFAULT (pg_current_xact_id()::text::bigint),
    occurred_at timestamptz NOT NULL DEFAULT now());

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
END $fn$;

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
END $fn$;

DROP EVENT TRIGGER IF EXISTS trg_capture_ddl;
CREATE EVENT TRIGGER trg_capture_ddl ON ddl_command_end
    WHEN TAG IN ('CREATE TABLE', 'ALTER TABLE', 'DROP TABLE') EXECUTE FUNCTION fn_capture_ddl();
DROP EVENT TRIGGER IF EXISTS trg_capture_drop;
CREATE EVENT TRIGGER trg_capture_drop ON sql_drop
    WHEN TAG IN ('ALTER TABLE', 'DROP TABLE') EXECUTE FUNCTION fn_capture_drop();

GRANT TRUNCATE ON public.sys_ddl_log TO dbuser_cdc;  -- 维护任务定期清空防堆积

-- ⑤ Debezium 增量快照 signal 表（source channel）：
--    类型严格跟随的"重建+重拉"兜底经它触发；连接器的快照水位标记也写在此表
CREATE TABLE IF NOT EXISTS public.dbz_signal (
    id varchar(42) PRIMARY KEY, type varchar(32) NOT NULL, data varchar(2048));
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON public.dbz_signal TO dbuser_cdc;

-- ⑥ 心跳表：heartbeat.action.query 周期 UPSERT，防"零流量时 slot 无限扣留实例级 WAL"
--    （LSN flush mode 'connector' 只在事件处理时确认；纯空闲必须造真实事件闭环）
CREATE TABLE IF NOT EXISTS public.dbz_heartbeat (
    id int PRIMARY KEY, ts timestamptz NOT NULL DEFAULT now());
ALTER TABLE public.dbz_heartbeat REPLICA IDENTITY FULL;
GRANT SELECT, INSERT, UPDATE ON public.dbz_heartbeat TO dbuser_cdc;

-- ⑦ 业务表建议：REPLICA IDENTITY FULL 让 UPDATE/DELETE 事件携带整行旧值
--    （DELETE 墓碑行才能带全列；按表执行）
-- ALTER TABLE <你的表> REPLICA IDENTITY FULL;
