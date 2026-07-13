#!/usr/bin/env bash
# 源库 CDC 全套基建:首次 initdb 自动执行(docker-entrypoint-initdb.d),up 即可用。
# 与 docs/init-source-db.sql 等价(catalog 库除外——元空间由独立 catalog-pg 容器承载)。
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- ── CDC 账号:逻辑复制 + 全库只读 ──
    CREATE ROLE dbuser_cdc REPLICATION LOGIN PASSWORD '${POSTGRES_PASSWORD}' CONNECTION LIMIT 8;
    GRANT pg_read_all_data TO dbuser_cdc;
    COMMENT ON ROLE dbuser_cdc IS 'CDC user for debezium-server-ducklake';

    -- ── publication:public schema 整库发布(新表自动纳入)──
    CREATE PUBLICATION dbz_publication FOR TABLES IN SCHEMA public;

    -- ── 观测扩展(shared_preload 由 compose command 行配好;pigsty 预装见 Dockerfile)──
    CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
    CREATE EXTENSION IF NOT EXISTS pg_stat_kcache;
    CREATE EXTENSION IF NOT EXISTS pg_wait_sampling;
    CREATE EXTENSION IF NOT EXISTS pg_bigm;
    CREATE EXTENSION IF NOT EXISTS hll;
EOSQL

# 官方镜像 entrypoint 只给 pg_hba 追加 "host all all all scram-sha-256"(不含 replication 伪库),
# 逻辑复制连接必须显式放行
cat >> "$PGDATA/pg_hba.conf" <<-EOF
    host replication dbuser_cdc 0.0.0.0/0 scram-sha-256
EOF

# ── DDL 审计流 + signal 表 + 心跳表(湖侧 DDL 跟随/类型重建兜底/防空闲 WAL 扣留)──
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-'EOSQL'
    CREATE TABLE public.sys_ddl_log (
        id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        ev              text NOT NULL,
        tag             text,
        object_type     text,
        object_identity text,
        query_text      text,
        xid             bigint NOT NULL DEFAULT (pg_current_xact_id()::text::bigint),
        occurred_at     timestamptz NOT NULL DEFAULT now());

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

    CREATE EVENT TRIGGER trg_capture_ddl ON ddl_command_end
        WHEN TAG IN ('CREATE TABLE', 'ALTER TABLE', 'DROP TABLE') EXECUTE FUNCTION fn_capture_ddl();
    CREATE EVENT TRIGGER trg_capture_drop ON sql_drop
        WHEN TAG IN ('ALTER TABLE', 'DROP TABLE') EXECUTE FUNCTION fn_capture_drop();

    GRANT TRUNCATE ON public.sys_ddl_log TO dbuser_cdc;

    CREATE TABLE public.dbz_signal (
        id varchar(42) PRIMARY KEY, type varchar(32) NOT NULL, data varchar(2048));
    GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON public.dbz_signal TO dbuser_cdc;

    CREATE TABLE public.dbz_heartbeat (
        id int PRIMARY KEY, ts timestamptz NOT NULL DEFAULT now());
    ALTER TABLE public.dbz_heartbeat REPLICA IDENTITY FULL;
    GRANT SELECT, INSERT, UPDATE ON public.dbz_heartbeat TO dbuser_cdc;
EOSQL
