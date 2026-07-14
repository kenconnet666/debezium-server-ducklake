#!/usr/bin/env bash
# MySQL 栈端到端冒烟验证:在部署机 docker/ 目录内执行 bash e2e-verify-mysql.sh
# 断言链:全容器健康 → 建表插数落湖 → UPDATE/DELETE 镜像 → DDL 加列/RENAME 跟随 →
#         TRUNCATE 跟随(MySQL 增强,PG 栈不支持) → DROP TABLE 跟随 → 心跳 → 日志零 ERROR
# 自建唯一名测试表(t_e2e_<时分秒>),不碰业务对象;可重复执行。
# 断言口径与 PG 版一致 = 湖侧当前态行数(Σ活跃 data - Σ活跃 delete + 活跃 inlined)。
set -uo pipefail
cd "$(dirname "$0")"

DC="docker compose -f docker-compose.mysql.yml"
PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); echo "  ✓ $1"; }
bad()  { FAIL=$((FAIL+1)); echo "  ✗ $1"; }
# MySQL 源库执行(-N 免表头 -B batch 模式,输出等价 psql -qtA)
msrc() { $DC exec -T mysql mysql -N -B -uroot -p"${MYSQL_PASSWORD:-changeme}" shop -e "$1" 2>/dev/null; }
pcat() { $DC exec -T catalog-pg psql -qtA -U lake_admin -d ducklake_catalog -c "$1"; }
TBL="t_e2e_$(date +%H%M%S)"

# 湖侧当前态行数(表未注册时输出 <none>):$1=湖schema $2=表名
lakecount_in() {
  local sch=$1 tbl=$2 tid parquet inlined itbl
  tid=$(pcat "SELECT t.table_id FROM ducklake_table t JOIN ducklake_schema s ON s.schema_id=t.schema_id
              WHERE s.schema_name='$sch' AND t.table_name='$tbl' AND t.end_snapshot IS NULL")
  [ -z "$tid" ] && { echo "<none>"; return; }
  parquet=$(pcat "SELECT COALESCE((SELECT sum(record_count) FROM ducklake_data_file WHERE table_id=$tid AND end_snapshot IS NULL),0)
                       - COALESCE((SELECT sum(delete_count) FROM ducklake_delete_file WHERE table_id=$tid AND end_snapshot IS NULL),0)")
  inlined=0
  for itbl in $(pcat "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name LIKE 'ducklake_inlined_data_${tid}\\_%'"); do
    inlined=$((inlined + $(pcat "SELECT count(*) FROM \"$itbl\" WHERE end_snapshot IS NULL")))
  done
  echo $((parquet + inlined))
}
# 主测试表(湖 schema=shop,镜像 MySQL database)的当前态行数
lakecount() { lakecount_in "shop" "$TBL"; }
hascol() { # $1=列名
  pcat "SELECT count(*) FROM ducklake_column c JOIN ducklake_table t ON t.table_id=c.table_id
        JOIN ducklake_schema s ON s.schema_id=t.schema_id
        WHERE s.schema_name='shop' AND t.table_name='$TBL' AND t.end_snapshot IS NULL
          AND c.column_name='$1' AND c.end_snapshot IS NULL"
}
hasnote()   { hascol "note"; }
hasremark() { hascol "remark"; }
wait_for() { # $1=描述 $2=秒数 $3=期望 $4=函数名
  local desc="$1" secs="$2" want="$3" fn="$4" got=""
  for _ in $(seq 1 "$secs"); do
    got="$($fn 2>/dev/null)" && [ "$got" = "$want" ] && { ok "$desc"; return 0; }
    sleep 1
  done
  bad "$desc (期望=$want 实际=${got:-<empty>})"; return 1
}

echo "── 1. 容器健康 ──"
for _ in $(seq 1 60); do
  starting=$($DC ps --format '{{.Health}}' | grep -c 'starting' || true)
  [ "$starting" = "0" ] && break; sleep 5
done
$DC ps -a --format 'table {{.Name}}\t{{.Status}}'
if $DC ps --format '{{.Name}}:{{.Health}}' | grep -v ':healthy' | grep -q .; then
  bad "存在非 healthy 容器"
else
  ok "mysql/catalog-pg/rustfs/ducklake 全部 healthy"
fi
bi=$($DC ps -a --format '{{.Service}} {{.ExitCode}}' | grep '^bucket-init ')
[ "$bi" = "bucket-init 0" ] && ok "bucket-init 建桶完成(exit 0)" || bad "bucket-init 异常: ${bi:-未找到}"

echo "── 2. 引擎在线 ──"
engine_up() {
  $DC exec -T ducklake sh -c \
    "curl -sf -m 5 http://127.0.0.1:19992/api/ducklake/watermark | jq -r '.message.engineRunning'"
}
wait_for "watermark API 可达且引擎 running" 120 "true" engine_up

echo "── 3. INSERT 落湖(测试表 shop.$TBL) ──"
msrc "CREATE TABLE $TBL(id int PRIMARY KEY, name varchar(64), val decimal(12,2), created datetime(6) DEFAULT CURRENT_TIMESTAMP(6))"
# MySQL 无 generate_series:递归 CTE 灌 1000 行(8.0 CTE 默认深度上限 1000 恰好够用)
msrc "SET SESSION cte_max_recursion_depth=1001;
      INSERT INTO $TBL(id,name,val) WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM g WHERE n<1000)
      SELECT n, CONCAT('row-',n), n*1.5 FROM g"
wait_for "1000 行 INSERT 落湖(当前态 1000)" 120 "1000" lakecount

echo "── 4. UPDATE/DELETE 镜像跟随 ──"
msrc "UPDATE $TBL SET val = val + 100 WHERE id <= 100"
msrc "DELETE FROM $TBL WHERE id > 950"
wait_for "UPDATE 就地更新+DELETE 物理跟随(当前态 950)" 120 "950" lakecount

echo "── 5. DDL 跟随(binlog schema change,零审计表基建) ──"
msrc "ALTER TABLE $TBL ADD COLUMN note varchar(64)"
msrc "INSERT INTO $TBL(id,name,val,note) VALUES (2001,'ddl-probe',1.0,'new-col')"
wait_for "湖表出现 note 列(加列跟随)" 120 "1" hasnote
wait_for "新列行落湖(当前态 951)" 60 "951" lakecount
msrc "ALTER TABLE $TBL RENAME COLUMN note TO remark"
wait_for "RENAME COLUMN 湖侧真 rename(remark 列)" 60 "1" hasremark

echo "── 6. TRUNCATE 跟随(MySQL 增强) ──"
msrc "TRUNCATE TABLE $TBL"
wait_for "源 TRUNCATE → 湖表清空(当前态 0)" 60 "0" lakecount
msrc "INSERT INTO $TBL(id,name,val) VALUES (1,'fresh',1.0)"
wait_for "TRUNCATE 后新增照常镜像(当前态 1)" 60 "1" lakecount

echo "── 7. 心跳闭环(offset 持续推进) ──"
hb=$(msrc "SELECT count(*) FROM dbz_heartbeat")
if [ "${hb:-0}" -ge 1 ]; then ok "dbz_heartbeat 已有心跳行(action query 生效)"; else
  echo "  … 等待一个心跳周期(65s)"; sleep 65
  hb=$(msrc "SELECT count(*) FROM dbz_heartbeat")
  [ "${hb:-0}" -ge 1 ] && ok "dbz_heartbeat 心跳行出现" || bad "心跳表仍为空(interval 60s 应已触发)"
fi
# binlog 客户端在线(Debezium 以 replica 身份注册,server_id=6400)
replica=$(msrc "SELECT count(*) FROM information_schema.processlist WHERE command LIKE 'Binlog Dump%'")
[ "${replica:-0}" -ge 1 ] && ok "binlog 客户端在线(Binlog Dump 线程存在)" || bad "无 Binlog Dump 线程(连接器未在读流)"

echo "── 8. DROP TABLE 跟随 + 日志 ERROR 扫描 ──"
msrc "DROP TABLE $TBL"
droptbl() {
  pcat "SELECT count(*) FROM ducklake_table t JOIN ducklake_schema s ON s.schema_id=t.schema_id
        WHERE s.schema_name='shop' AND t.table_name='$TBL' AND t.end_snapshot IS NULL"
}
wait_for "源 DROP TABLE → 湖表跟随删除" 60 "0" droptbl
errs=$($DC logs ducklake 2>/dev/null | grep -c ' ERROR ' || true)
if [ "${errs:-0}" = "0" ]; then ok "ducklake 日志无 ERROR"; else
  bad "ducklake 日志有 $errs 条 ERROR(样例如下)"
  $DC logs ducklake 2>/dev/null | grep ' ERROR ' | head -3
fi

echo
echo "════ 结果: PASS=$PASS FAIL=$FAIL ════"
[ "$FAIL" = "0" ]
