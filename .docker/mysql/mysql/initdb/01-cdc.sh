#!/bin/bash
# MySQL 原生 CDC 基建：只需复制账号；DDL 直接来自 binlog QueryEvent。
set -euo pipefail

mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" <<SQL
CREATE USER IF NOT EXISTS 'dbuser_cdc'@'%'
    IDENTIFIED BY '${MYSQL_CDC_PASSWORD:-${MYSQL_ROOT_PASSWORD}}';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT
    ON *.* TO 'dbuser_cdc'@'%';
SQL

echo "[01-cdc.sh] MySQL 原生 CDC 复制账号已就绪: dbuser_cdc"
