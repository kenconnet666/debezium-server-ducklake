-- ============================================================================
-- MySQL 8.0+ 原生 CDC 源库初始化（幂等）
-- 用法：mysql -uroot -p < init-source-mysql.sql
--
-- my.cnf / 云参数组必须满足：
--   log_bin=ON
--   binlog_format=ROW
--   binlog_row_image=FULL
--   binlog_row_metadata=FULL
--   binlog_transaction_compression=OFF
--   server_id 非 0
-- 推荐：gtid_mode=ON + enforce_gtid_consistency=ON，以便切主后按 GTID 续传。
-- binlog_expire_logs_seconds 必须覆盖最长停机窗口。
-- ============================================================================

CREATE USER IF NOT EXISTS 'dbuser_cdc'@'%' IDENTIFIED BY 'changeme';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT
    ON *.* TO 'dbuser_cdc'@'%';

-- MySQL DDL 直接来自 binlog QueryEvent，无需 publication、signal、心跳或 DDL 审计表。
-- 业务表应有主键：有主键时 INSERT/UPDATE/DELETE 都能镜像；无主键表仅 insert-only。
