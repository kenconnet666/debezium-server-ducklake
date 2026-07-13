#!/usr/bin/env bash
# 栈端到端冒烟验证:在部署机 docker/ 目录内执行 bash e2e-verify.sh
# 断言链:全容器健康 → 建表插数落湖 → UPDATE/DELETE 流转 → DDL 加列跟随 → 心跳闭环
# 自建唯一名测试表(t_e2e_<时分秒>),不碰业务对象;可重复执行(每轮全新表;尾部 DROP
# 源表后湖表随 DROP 跟随一并消失)。
# 断言口径 = 湖侧当前态行数(镜像语义:UPDATE 就地更新,DELETE 物理跟随,湖=主库当前态):
#   当前行数 = Σ活跃 data_file.record_count − Σ活跃 delete_file.delete_count + 活跃 inlined 行
# 本脚本聚合前两分量于 data_file 净值(delete file 由 DuckLake merge-on-read 语义计入),
# 实现细节:直接数活跃 data 行减 delete 行,加 inlined 活跃行。
# (flush_inlined_data 每 5 分钟把 inlined 转 parquet,分量互补守恒;
#  ducklake_table_stats.record_count 是近似统计,不作断言口径)
set -uo pipefail
cd "$(dirname "$0")"

PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); echo "  ✓ $1"; }
bad()  { FAIL=$((FAIL+1)); echo "  ✗ $1"; }
psrc() { docker compose exec -T postgres psql -qtA -U postgres -c "$1"; }
pcat() { docker compose exec -T catalog-pg psql -qtA -U lake_admin -d ducklake_catalog -c "$1"; }
# 测试表名唯一化;湖表命名规则:湖 schema=cdc,表名=<pg_schema>_<表名>
TBL="t_e2e_$(date +%H%M%S)"
LAKE_TBL="public_${TBL}"

# 湖侧事件行数(见文件头公式;表未注册时输出 <none>)
lakecount() {
  local tid parquet inlined itbl
  tid=$(pcat "SELECT table_id FROM ducklake_table WHERE table_name='$LAKE_TBL' AND end_snapshot IS NULL")
  [ -z "$tid" ] && { echo "<none>"; return; }
  parquet=$(pcat "SELECT COALESCE((SELECT sum(record_count) FROM ducklake_data_file WHERE table_id=$tid AND end_snapshot IS NULL),0)
                       - COALESCE((SELECT sum(delete_count) FROM ducklake_delete_file WHERE table_id=$tid AND end_snapshot IS NULL),0)")
  # inlined 表按 schema 版本分代(ducklake_inlined_data_<tid>_<版本>),DDL 变更开新代——全代求和
  inlined=0
  for itbl in $(pcat "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name LIKE 'ducklake_inlined_data_${tid}\\_%'"); do
    inlined=$((inlined + $(pcat "SELECT count(*) FROM \"$itbl\" WHERE end_snapshot IS NULL")))
  done
  echo $((parquet + inlined))
}
# note 列在湖表活跃 schema 中存在?(DDL 跟随断言)
hascol() {
  pcat "SELECT count(*) FROM ducklake_column c JOIN ducklake_table t ON t.table_id=c.table_id
        WHERE t.table_name='$LAKE_TBL' AND t.end_snapshot IS NULL
          AND c.column_name='note' AND c.end_snapshot IS NULL"
}
# 轮询直到命令输出=期望值,最多 N 秒
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
  starting=$(docker compose ps --format '{{.Health}}' | grep -c 'starting' || true)
  [ "$starting" = "0" ] && break; sleep 5
done
docker compose ps -a --format 'table {{.Name}}\t{{.Status}}'
if docker compose ps --format '{{.Name}}:{{.Health}}' | grep -v ':healthy' | grep -q .; then
  bad "存在非 healthy 容器"
else
  ok "postgres/catalog-pg/rustfs/ducklake 全部 healthy"
fi
bi=$(docker compose ps -a --format '{{.Service}} {{.ExitCode}}' | grep '^bucket-init ')
[ "$bi" = "bucket-init 0" ] && ok "bucket-init 建桶完成(exit 0)" || bad "bucket-init 异常: ${bi:-未找到}"

echo "── 2. 引擎在线 ──"
# 走 ducklake 容器内的 curl+jq(镜像内置排障工具),不依赖宿主装了什么;
# 响应包装字段为 message(ApiResult 约定)
engine_up() {
  docker compose exec -T ducklake sh -c \
    "curl -sf -m 5 http://127.0.0.1:19992/api/ducklake/watermark | jq -r '.message.engineRunning'"
}
wait_for "watermark API 可达且引擎 running" 90 "true" engine_up

echo "── 3. INSERT 落湖(测试表 $TBL) ──"
psrc "CREATE TABLE $TBL(id int PRIMARY KEY, name text, val numeric(12,2), created timestamptz DEFAULT now())" >/dev/null
psrc "ALTER TABLE $TBL REPLICA IDENTITY FULL" >/dev/null
psrc "INSERT INTO $TBL(id,name,val) SELECT g, 'row-'||g, g*1.5 FROM generate_series(1,1000) g" >/dev/null
wait_for "1000 行 INSERT 落湖(当前态 1000)" 120 "1000" lakecount

echo "── 4. UPDATE/DELETE 镜像跟随 ──"
psrc "UPDATE $TBL SET val = val + 100 WHERE id <= 100" >/dev/null
psrc "DELETE FROM $TBL WHERE id > 950" >/dev/null
wait_for "UPDATE 就地更新+DELETE 物理跟随(当前态 950)" 120 "950" lakecount

echo "── 5. DDL 加列跟随 ──"
psrc "ALTER TABLE $TBL ADD COLUMN note text" >/dev/null
psrc "INSERT INTO $TBL(id,name,val,note) VALUES (2001,'ddl-probe',1.0,'new-col')" >/dev/null
wait_for "湖表出现 note 列(DDL 审计流跟随)" 120 "1" hascol
wait_for "新列行落湖(当前态 951)" 60 "951" lakecount

echo "── 6. 非 public schema 自动对应(默认整库同步) ──"
SCHEMA_TBL="app_e2e.orders_$(date +%H%M%S)"
LAKE_SCHEMA_TBL="app_e2e_${SCHEMA_TBL#app_e2e.}"
schemacount() {
  local tid
  tid=$(pcat "SELECT table_id FROM ducklake_table WHERE table_name='$LAKE_SCHEMA_TBL' AND end_snapshot IS NULL")
  [ -z "$tid" ] && { echo "<none>"; return; }
  local parquet inlined itbl
  parquet=$(pcat "SELECT COALESCE((SELECT sum(record_count) FROM ducklake_data_file WHERE table_id=$tid AND end_snapshot IS NULL),0)
                       - COALESCE((SELECT sum(delete_count) FROM ducklake_delete_file WHERE table_id=$tid AND end_snapshot IS NULL),0)")
  inlined=0
  for itbl in $(pcat "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name LIKE 'ducklake_inlined_data_${tid}\\_%'"); do
    inlined=$((inlined + $(pcat "SELECT count(*) FROM \"$itbl\" WHERE end_snapshot IS NULL")))
  done
  echo $((parquet + inlined))
}
psrc "CREATE SCHEMA IF NOT EXISTS app_e2e" >/dev/null
psrc "CREATE TABLE $SCHEMA_TBL(id int PRIMARY KEY, sku text)" >/dev/null
psrc "INSERT INTO $SCHEMA_TBL SELECT g, 'sku-'||g FROM generate_series(1,100) g" >/dev/null
wait_for "app_e2e schema 表自动落湖(cdc.$LAKE_SCHEMA_TBL,100 行)" 120 "100" schemacount
psrc "DROP TABLE IF EXISTS $SCHEMA_TBL" >/dev/null

echo "── 7. 心跳闭环(空闲 WAL 确认) ──"
hb=$(psrc "SELECT count(*) FROM dbz_heartbeat")
if [ "${hb:-0}" -ge 1 ]; then ok "dbz_heartbeat 已有心跳行(action query 生效)"; else
  echo "  … 等待一个心跳周期(65s)"; sleep 65
  hb=$(psrc "SELECT count(*) FROM dbz_heartbeat")
  [ "${hb:-0}" -ge 1 ] && ok "dbz_heartbeat 心跳行出现" || bad "心跳表仍为空(interval 60s 应已触发)"
fi
slot=$(psrc "SELECT active FROM pg_replication_slots WHERE slot_name='dbz_ducklake'")
[ "$slot" = "t" ] && ok "复制槽 dbz_ducklake active" || bad "复制槽状态异常: ${slot:-不存在}"

echo "── 8. 应用日志 ERROR 扫描 ──"
errs=$(docker compose logs ducklake 2>/dev/null | grep -c ' ERROR ' || true)
if [ "${errs:-0}" = "0" ]; then ok "ducklake 日志无 ERROR"; else
  bad "ducklake 日志有 $errs 条 ERROR(样例如下)"
  docker compose logs ducklake 2>/dev/null | grep ' ERROR ' | head -3
fi

# 清理源库测试表(镜像语义:湖侧对应表随 DROP 跟随一并删除)
psrc "DROP TABLE IF EXISTS $TBL" >/dev/null

echo
echo "════ 结果: PASS=$PASS FAIL=$FAIL ════"
[ "$FAIL" = "0" ]
