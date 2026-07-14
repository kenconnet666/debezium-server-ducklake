-- ============================================================================
-- debezium-server-ducklake：MySQL 8.0+ 源库初始化（幂等，可重复执行）
-- 用法：mysql -uroot -p < init-source-mysql.sql
--   （或逐段复制执行；密码占位 'changeme' 请先替换）
--
-- 与 PG 版(init-source-db.sql)对比，MySQL 侧基建面小得多：
--   · 无需 publication —— binlog 是实例级，捕获范围由连接器 database.include.list 决定
--   · 无需 DDL 审计表/event trigger —— binlog 自带 DDL，Debezium schema change 事件
--     驱动湖侧跟随(RENAME/DROP COLUMN/DROP TABLE/COMMENT/TRUNCATE)
--   · 无需 REPLICA IDENTITY —— binlog_row_image=FULL(8.0 默认)天然带整行前镜像
--
-- 服务端前提（8.0+ 出厂默认即满足，逐项核实命令）：
--   SHOW VARIABLES LIKE 'log_bin';            -- 必须 ON      (8.0+ 默认 ON)
--   SHOW VARIABLES LIKE 'binlog_format';      -- 必须 ROW     (8.0+ 默认 ROW)
--   SHOW VARIABLES LIKE 'binlog_row_image';   -- 必须 FULL    (默认 FULL)
--   SHOW VARIABLES LIKE 'server_id';          -- 非 0
--   SHOW VARIABLES LIKE 'binlog_expire_logs_seconds'; -- 默认 2592000(30 天)
--     ⚠️ 消费者停机超过保留期会丢 binlog 位点(重启需重新全量快照),按停机容忍度调
--   建议开启 GTID（故障转移位点可续；接入存量库非强制）：
--   gtid_mode=ON + enforce_gtid_consistency=ON  (my.cnf,需重启;云托管改参数组)
-- ============================================================================

-- ① CDC 账号（Debezium 官方权限清单；8.0 默认 caching_sha2_password 认证）
--    RDS/Aurora 等禁全局读锁的托管库需额外 GRANT LOCK TABLES ON *.*（表级锁快照回退路径）
CREATE USER IF NOT EXISTS 'dbuser_cdc'@'%' IDENTIFIED BY 'changeme';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'dbuser_cdc'@'%';

-- ② Debezium 增量快照 signal 表（source channel；类型重建兜底的"重建+重灌"经它触发，
--    连接器的快照水位标记也写此表）。⚠️ 建在业务主库内（与 ducklake.source.dbname 对应，
--    默认推导 <dbname>.dbz_signal）；替换 your_db 为实际库名
-- CREATE DATABASE IF NOT EXISTS your_db;
CREATE TABLE IF NOT EXISTS your_db.dbz_signal (
    id   varchar(42) PRIMARY KEY,
    type varchar(32) NOT NULL,
    data varchar(2048) NULL);
-- DROP 权限承载 TRUNCATE（维护任务每日阅后即焚）
GRANT INSERT, UPDATE, DELETE, DROP ON your_db.dbz_signal TO 'dbuser_cdc'@'%';

-- ③ 心跳表（可选但建议）：空闲期 heartbeat action query 周期 UPSERT——
--    MySQL 无 PG 的"slot 扣留 WAL"问题，心跳价值 = 持续推进 offset，
--    规避"长期全闲后 binlog 过期、重启位点丢失"的边界；也让 watermark 监控口径永远有活。
--    配套 ducklake 配置(prod 环境变量)：
--    DUCKLAKE_HEARTBEAT_QUERY=INSERT INTO your_db.dbz_heartbeat (id, ts) VALUES (1, NOW(6)) ON DUPLICATE KEY UPDATE ts = NOW(6)
CREATE TABLE IF NOT EXISTS your_db.dbz_heartbeat (
    id int PRIMARY KEY,
    ts datetime(6) NOT NULL);
GRANT SELECT, INSERT, UPDATE ON your_db.dbz_heartbeat TO 'dbuser_cdc'@'%';

-- ④ 业务表要求（与 PG 版一致）：**必须有主键**——镜像 upsert 按主键定位湖行，
--    无主键表降级 insert-only(UPDATE/DELETE 不跟随)。MySQL 无"无主键表拒绝写入"的
--    服务端约束(与 PG FOR ALL TABLES 不同)，源库写入照常、湖侧仅追加。
