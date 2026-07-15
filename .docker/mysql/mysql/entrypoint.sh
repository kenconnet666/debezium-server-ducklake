#!/bin/bash
# 精简版 MySQL 容器 entrypoint(兼容官方镜像初始化协定,覆盖本栈所需子集):
#   MYSQL_ROOT_PASSWORD(必填) / MYSQL_DATABASE(可选) /
#   /docker-entrypoint-initdb.d 下 *.sh 执行、*.sql 灌入(仅数据目录首次初始化时)
# 初始化流程:initialize-insecure → 临时实例(仅 socket)设 root 密码/建库/跑 initdb → 正式启动。
set -euo pipefail

DATADIR=/var/lib/mysql
SOCKET=/var/run/mysqld/mysqld.sock

: "${MYSQL_ROOT_PASSWORD:?必须设置 MYSQL_ROOT_PASSWORD}"

mkdir -p /var/run/mysqld && chown mysql:mysql /var/run/mysqld "$DATADIR"

if [ ! -d "$DATADIR/mysql" ]; then
    echo "[entrypoint] 首次启动:初始化数据目录 ..."
    mysqld --initialize-insecure --user=mysql --datadir="$DATADIR"

    echo "[entrypoint] 临时实例(仅 socket)做首次配置 ..."
    mysqld --user=mysql --datadir="$DATADIR" --skip-networking --socket="$SOCKET" &
    tmp_pid=$!
    for _ in $(seq 1 60); do
        mysqladmin --socket="$SOCKET" ping --silent 2>/dev/null && break
        sleep 1
    done

    mysql --socket="$SOCKET" -uroot <<SQL
ALTER USER 'root'@'localhost' IDENTIFIED BY '${MYSQL_ROOT_PASSWORD}';
CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED BY '${MYSQL_ROOT_PASSWORD}';
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;
SQL
    if [ -n "${MYSQL_DATABASE:-}" ]; then
        mysql --socket="$SOCKET" -uroot -p"${MYSQL_ROOT_PASSWORD}" \
            -e "CREATE DATABASE IF NOT EXISTS \`${MYSQL_DATABASE}\`"
    fi

    # initdb 脚本(*.sh 内的 mysql 客户端默认走同一 socket,与临时实例天然连通)
    export MYSQL_ROOT_PASSWORD MYSQL_DATABASE
    for f in /docker-entrypoint-initdb.d/*; do
        [ -e "$f" ] || continue
        case "$f" in
            *.sh)  echo "[entrypoint] 执行 $f"; bash "$f" ;;
            *.sql) echo "[entrypoint] 灌入 $f"
                   mysql --socket="$SOCKET" -uroot -p"${MYSQL_ROOT_PASSWORD}" \
                       ${MYSQL_DATABASE:+"$MYSQL_DATABASE"} < "$f" ;;
            *)     echo "[entrypoint] 忽略 $f" ;;
        esac
    done

    mysqladmin --socket="$SOCKET" -uroot -p"${MYSQL_ROOT_PASSWORD}" shutdown
    wait "$tmp_pid" 2>/dev/null || true
    echo "[entrypoint] 初始化完成"
fi

echo "[entrypoint] 启动 mysqld ..."
exec mysqld --user=mysql --datadir="$DATADIR"
