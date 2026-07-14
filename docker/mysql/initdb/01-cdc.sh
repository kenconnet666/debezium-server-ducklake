#!/bin/bash
# MySQL 源库 CDC 基建（首次初始化自动执行；与 docs/init-source-mysql.sql 等价）：
# CDC 账号（Debezium 官方权限清单）+ signal 表 + 心跳表。
# 对比 PG 侧 initdb：无需 publication（binlog 是实例级）、无需 DDL 审计 event trigger
# （binlog 自带 DDL，Debezium schema change 事件驱动湖侧跟随）——基建面小得多。
set -euo pipefail

mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" <<SQL
-- ① CDC 账号（8.0 默认 caching_sha2_password 认证；权限为 Debezium 官方 GRANT 原文）
CREATE USER IF NOT EXISTS 'dbuser_cdc'@'%' IDENTIFIED BY '${MYSQL_CDC_PASSWORD:-${MYSQL_ROOT_PASSWORD}}';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'dbuser_cdc'@'%';

-- ② Debezium 增量快照 signal 表（source channel；类型重建兜底经它触发，连接器写快照水位也在此表）
CREATE TABLE IF NOT EXISTS ${MYSQL_DATABASE}.dbz_signal (
    id   varchar(42) PRIMARY KEY,
    type varchar(32) NOT NULL,
    data varchar(2048) NULL);
-- DROP 权限承载 TRUNCATE（维护任务阅后即焚）
GRANT INSERT, UPDATE, DELETE, DROP ON ${MYSQL_DATABASE}.dbz_signal TO 'dbuser_cdc'@'%';

-- ③ 心跳表（可选但建议：空闲期 heartbeat action query 周期 UPSERT，持续推进 offset，
--    规避"长期空闲后 binlog 过期、重启位点丢失"的边界；也让 watermark 监控口径永远有活）
CREATE TABLE IF NOT EXISTS ${MYSQL_DATABASE}.dbz_heartbeat (
    id int PRIMARY KEY,
    ts datetime(6) NOT NULL);
GRANT SELECT, INSERT, UPDATE ON ${MYSQL_DATABASE}.dbz_heartbeat TO 'dbuser_cdc'@'%';
SQL

echo "[01-cdc.sh] CDC 基建完成: dbuser_cdc + ${MYSQL_DATABASE}.dbz_signal + ${MYSQL_DATABASE}.dbz_heartbeat"
